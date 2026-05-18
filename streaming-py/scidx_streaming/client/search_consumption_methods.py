import logging
import json
from typing import List, Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

from scidx_streaming.method_types import MethodType, SUPPORTED_METHOD_TYPES, URL_BACKED_METHOD_TYPES


class StreamingDataSourceSearch:
    """
    Mixin class extending StreamingClient with search functionality for consumption methods.

    Returns dataset-grouped results:
    [
      {
        "name": "<dataset_name>",
        "description": "<dataset_notes>",
        "resources": [
           {"id": "<resource_id>", ...<parsed method json>...},
           ...
        ]
      },
      ...
    ]
    """

    # --- Validation helpers -------------------------------------------------

    _URL_TYPES = URL_BACKED_METHOD_TYPES

    @staticmethod
    def _is_nonempty_str(v: Any) -> bool:
        return isinstance(v, str) and bool(v.strip())

    @classmethod
    def _validate_method_payload(cls, fmt: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a parsed method payload according to the registration spec.

        Returns (is_valid, reason_if_invalid)
        """
        fmt = (fmt or "").lower()

        # Basic shape & types
        if not isinstance(payload, dict):
            return False, "method payload is not a dict"

        # Optional sections must be dicts when present
        for key in ("mapping", "processing"):
            if key in payload and payload[key] is not None and not isinstance(payload[key], dict):
                return False, f"'{key}' must be a dict when present"

        # Config is required for all types in this system
        config = payload.get("config")
        if not isinstance(config, dict):
            return False, "'config' must be a dict"

        # Per-type requireds
        if fmt in cls._URL_TYPES:
            url = config.get("url")
            if not cls._is_nonempty_str(url):
                return False, f"'config.url' is required for type '{fmt}'"
            # conservative URL sanity check
            if not (url.startswith("http://") or url.startswith("https://") or url.startswith("ws://") or url.startswith("wss://")):
                    # Allow s3:// and gs:// for NetCDF or others if you support them; adjust as needed
                if not (
                    fmt == MethodType.NETCDF.value
                    and (url.startswith("s3://") or url.startswith("gs://"))
                    ):
                    return False, f"'config.url' must be http(s), ws(s) (or allowed scheme) for type '{fmt}'"

        elif fmt == MethodType.KAFKA.value:
            host = config.get("host")
            port = config.get("port")
            topic = config.get("topic")
            if not cls._is_nonempty_str(host):
                return False, "'config.host' required for kafka"
            # accept int or numeric string for port
            if not (isinstance(port, int) or (isinstance(port, str) and port.isdigit())):
                return False, "'config.port' must be int or numeric string for kafka"
            if not cls._is_nonempty_str(topic):
                return False, "'config.topic' required for kafka"
        else:
            # Unknown format: reject (shouldn't happen due to enum check)
            return False, f"unsupported method type '{fmt}'"

        # Names & type fields consistency (helpful for downstream)
        if "type" in payload and str(payload["type"]).lower() != fmt:
            return False, f"payload['type'] '{payload['type']}' mismatches resource format '{fmt}'"
        if not cls._is_nonempty_str(payload.get("name", "")):
            return False, "'name' is required in method payload"

        return True, ""

    # --- Search API ---------------------------------------------------------

    def search_consumption_methods(
        self,
        terms: List[str],
        types: Optional[List[str]] = None,
        *,
        server: str = "local",
    ) -> List[Dict[str, Any]]:
        """
        Search across datasets and return matching consumption methods grouped by dataset.
        Only returns methods that pass strict validation for their type.
        """
        supported = SUPPORTED_METHOD_TYPES
        
        def _resolve_allowed_types(types_opt: Optional[List[str]]) -> set:
            # None or empty => all supported types
            if not types_opt:
                return supported
            # wildcard => all
            if any(t == "*" for t in types_opt):
                return supported
            # normalize and keep only supported; warn if user passed unknowns
            normalized = {str(t).lower() for t in types_opt}
            unknown = normalized - supported
            if unknown:
                logger.debug("Ignoring unknown method types: %s", sorted(unknown))
            return normalized & supported

        allowed_types = _resolve_allowed_types(types)

        # Search datasets
        datasets = self.search_datasets(terms=terms, server=server) or []

        # Optional fallback: if nothing came back, try exact-name lookups per term
        if not datasets and terms:
            acc: List[Dict[str, Any]] = []
            for t in terms:
                if not t:  # skip empty strings
                    continue
                try:
                    ds = self._get_dataset(t, server=server)
                except Exception:
                    ds = None
                if ds:
                    acc.append(ds)
            if acc:
                logger.debug("Exact-name fallback returned %d dataset(s).", len(acc))
                datasets = acc

        if not datasets:
            return []


        grouped: List[Dict[str, Any]] = []
        term_matches = {str(t).strip().lower() for t in terms or [] if str(t).strip()}

        for ds in datasets:
            current_ds = ds
            ds_name = (current_ds.get("name") or "")
            ds_notes = (current_ds.get("notes") or "")
            matched_resources: List[Dict[str, Any]] = []
            attempted_refresh = False

            while True:
                matched_resources.clear()
                ds_resources = current_ds.get("resources") or []

                for res in ds_resources:
                    fmt = (res.get("format") or "").lower()
                    if not fmt or fmt not in allowed_types:
                        continue

                    try:
                        MethodType(fmt)
                    except ValueError:
                        logger.debug("Skipping unknown method type '%s' in dataset '%s'", fmt, ds_name)
                        continue

                    desc_text = res.get("description") or ""
                    if desc_text:
                        try:
                            parsed = json.loads(desc_text)
                            if not isinstance(parsed, dict):
                                parsed = {"value": parsed}
                        except Exception as ex:
                            logger.debug(
                                "Failed to parse description JSON for resource '%s' (dataset '%s'): %s",
                                res.get("name", ""), ds_name, ex
                            )
                            # If we can't parse, we cannot validate; skip
                            continue
                    else:
                        logger.debug(
                            "Resource '%s' in dataset '%s' has empty description; skipping",
                            res.get("name", ""), ds_name
                        )
                        continue

                    parsed_type = (parsed.get("type") or fmt)
                    parsed["type"] = str(parsed_type).lower()
                    parsed["name"] = parsed.get("name") or res.get("name", "")

                    is_valid, reason = self._validate_method_payload(fmt, parsed)
                    if not is_valid:
                        logger.info(
                            "Filtered out invalid method in dataset '%s' (resource '%s'): %s",
                            ds_name, res.get("name", ""), reason
                        )
                        continue

                    entry = {"id": res.get("id", "")}
                    entry.update(parsed)
                    entry["type"] = (entry.get("type") or fmt).lower()
                    matched_resources.append(entry)

                if matched_resources or attempted_refresh or not ds_name:
                    break

                attempted_refresh = True
                try:
                    refreshed = self._get_dataset(ds_name, server=server)
                except Exception:
                    refreshed = None
                if not refreshed or refreshed is current_ds:
                    break
                current_ds = refreshed
                ds_name = (current_ds.get("name") or ds_name)
                ds_notes = (current_ds.get("notes") or ds_notes)

            if matched_resources:
                grouped.append(
                    {
                        "name": ds_name,
                        "description": ds_notes,
                        "resources": matched_resources,
                    }
                )
            elif ds_name and ds_name.strip().lower() in term_matches:
                grouped.append(
                    {
                        "name": ds_name,
                        "description": ds_notes,
                        "resources": [],
                    }
                )

        return grouped
