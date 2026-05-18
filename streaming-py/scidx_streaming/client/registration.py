import logging
import json
from typing import List, Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

from scidx_streaming.method_types import MethodType, SUPPORTED_METHOD_TYPES, URL_BACKED_METHOD_TYPES


class StreamingDataSourceRegistration:
    """
    Mixin class extending StreamingClient with dataset registration and consumption method management.
    """
    @classmethod
    def _validate_method_payload(cls, fmt: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a parsed method payload according to the registration spec.

        Returns (is_valid, reason_if_invalid)
        """
        fmt = (fmt or "").lower()

        if fmt not in SUPPORTED_METHOD_TYPES:
            return False, f"unsupported method type '{fmt}'"

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
        if fmt in URL_BACKED_METHOD_TYPES:
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
                    return False, f"'config.url' must be http(s) (or allowed scheme) for type '{fmt}'"

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
            # Defensive, should be unreachable due to earlier guard.
            return False, f"unsupported method type '{fmt}'"

        # Names & type fields consistency (helpful for downstream)
        if "type" in payload and str(payload["type"]).lower() != fmt:
            return False, f"payload['type'] '{payload['type']}' mismatches resource format '{fmt}'"
        if not cls._is_nonempty_str(payload.get("name", "")):
            return False, "'name' is required in method payload"

        return True, ""
    
    def register_data_source(self, dataset_metadata: Dict[str, Any], methods: Optional[List[Dict[str, Any]]] = None, server: str = "local") -> Dict[str, Any]:
        """
        Register a new dataset (data source) with the given metadata and associated consumption methods.
        If the dataset already exists, ensure the provided methods are present (idempotent for same input).

        Validation notes
        ----------------
        * `dataset_metadata` must provide `name`, `title`, `notes`, and `owner_org`.
        * Every entry in `methods` must be a dict containing `type`, `name`, `description`, and a
          nested `config` dict; do not collapse config into a string.
        * Optional `mapping` and `processing` sections must be dicts when supplied. Flattened strings
          such as ``"x=coor[0]"`` must be converted to JSON keys inside the dict (``{"x": "coor[0]"}``).
        * Kafka configs must include `host`, `port` (int or numeric string), and `topic`. Additional
          fields such as security protocol, SASL mechanism, offsets, or timing go inside the same
          config dict using the correct scalar types (e.g., numeric intervals stay numeric).
        
        Parameters
        ----------
        dataset_metadata : dict
            Metadata for the dataset. Must include at least:
                - name: Unique identifier for the dataset (lowercase, no spaces).
                - title: Human-readable title of the dataset.
                - notes: Description or notes about the dataset.
                - owner_org: Organization ID that owns this dataset.
            Additional optional fields (tags, groups, extras, etc.) may be included as needed.
        methods : list of dict, optional
            A list of consumption method definitions, each a dict containing:
                - type: Type of the consumption method (e.g., "csv", "kafka", "rss", etc.).
                - name: Name of the method.
                - description: Description of the method.
                - config: Configuration details (dict) specific to this method type.
                - mapping: (optional) Field mapping dict for data translation (if applicable).
                - processing: (optional) Processing instructions dict (if applicable).
            Each method dict will be stored as a dataset resource with the JSON serialized into the description field.
        server : str, optional
            Server context to register the dataset on. Defaults to "local".

        Returns
        -------
        dict
            The created dataset's information (or existing dataset info if it already existed).

        Raises
        ------
        ValueError
            If required metadata is missing or if dataset creation fails for reasons other than already exists.
        """
        methods = methods or []
        # Ensure required fields in metadata
        required_fields = {"name", "title", "notes", "owner_org"}
        missing = required_fields - set(dataset_metadata.keys())
        if missing:
            raise ValueError(f"Missing required dataset metadata fields: {', '.join(missing)}")
        # Prepare dataset payload
        data = dataset_metadata.copy()
        # Build resource entries from methods list
        resources = []
        for method in methods:
            # ---- NEW: strict validation BEFORE building resource ----
            fmt = str(method.get("type") or "").lower()
            try:
                is_valid, reason = self._validate_method_payload(fmt, method)
            except AttributeError:
                # Safety: if mixin isn't present for some reason
                raise ValueError("Validator not available on client (StreamingDataSourceSearch missing).")
            if not is_valid:
                raise ValueError(f"Invalid method payload for type='{fmt}' name='{method.get('name','')}'. Reason: {reason}")
            # ---------------------------------------------------------
            try:
                resource_entry = self._build_resource_entry(method)
            except Exception as e:
                raise ValueError(f"Invalid method definition: {e}")
            resources.append(resource_entry)
        if resources:
            data["resources"] = resources
        try:
            # Attempt to create the dataset via base client method
            result = self.register_general_dataset(data, server=server)
            logger.info(f"Dataset '{data['name']}' created successfully with {len(resources)} resource(s).")
            return result
        except ValueError as e:
            error_msg = str(e)
            # Handle dataset already exists as idempotent case
            if "already exists" in error_msg.lower():
                # Dataset likely already present
                dataset_name = dataset_metadata["name"]
                print(f"Info: Dataset '{dataset_name}' already exists. Skipping creation.")
                # Retrieve existing dataset details
                existing = self._get_dataset(dataset_name, server=server)
                if existing is None:
                    # If we cannot retrieve, raise original exception
                    raise ValueError(f"Dataset '{dataset_name}' already exists, but could not retrieve its details.") from e
                # Ensure each provided method is present as a resource
                for method in methods:
                    # If a resource with this method name is missing, add it
                    if not any(res.get("name") == method.get("name") for res in existing.get("resources", [])):
                        try:
                            self.add_consumption_method(dataset_id=dataset_name, method_payload=method, server=server)
                            logger.info(f"Added missing method '{method.get('name')}' to existing dataset '{dataset_name}'.")
                        except Exception as add_err:
                            logger.error(f"Failed to add method '{method.get('name')}' to existing dataset: {add_err}")
                            raise
                # Return existing dataset info
                refreshed = self._get_dataset(dataset_name, server=server)
                if refreshed is not None:
                    return refreshed
                return existing
            else:
                # Propagate other errors (e.g., network issues, invalid data, etc.)
                raise

    def add_consumption_method(self, dataset_id: str, method_payload: Dict[str, Any], server: str = "local") -> Dict[str, Any]:
        """
        Append a new consumption method to an existing dataset.

        This will add a new resource entry to the dataset with the given method details.

        The payload must match the same structure enforced during dataset registration: include
        `type`, `name`, `description`, and a dict-based `config`. Optional `mapping`/`processing`
        sections must remain dictionaries. Kafka configs require at minimum `host`, `port`, and
        `topic`, with any auth/timing parameters added as keys on the config dict.

        Parameters
        ----------
        dataset_id : str
            The dataset identifier (name or ID) to which the method will be added.
        method_payload : dict
            The consumption method definition (see `register_data_source` for fields required).
        server : str, optional
            Server context where the dataset resides. Defaults to "local".

        Returns
        -------
        dict
            The updated dataset information after adding the new method.

        Raises
        ------
        ValueError
            If the dataset is not found, or if a method with the same name already exists, 
            or if adding the method fails due to other errors.
        """
        # Retrieve the current dataset to get existing resources
        dataset = self._get_dataset(dataset_id, server=server)
        if dataset is None:
            raise ValueError(f"Dataset '{dataset_id}' not found. Cannot add consumption method.")
        # Check for duplicate resource name
        new_name = method_payload.get("name")
        if any(res.get("name") == new_name for res in dataset.get("resources", [])):
            raise ValueError(f"A consumption method with name '{new_name}' already exists in dataset '{dataset_id}'.")
        
        # ---- NEW: strict validation BEFORE building resource ----
        fmt = str(method_payload.get("type") or "").lower()
        try:
            is_valid, reason = self._validate_method_payload(fmt, method_payload)
        except AttributeError:
            raise ValueError("Validator not available on client (StreamingDataSourceSearch missing).")
        if not is_valid:
            raise ValueError(f"Invalid method payload for type='{fmt}' name='{new_name}'. Reason: {reason}")
        # ---------------------------------------------------------

        # Build resource entry for the new method
        try:
            new_resource = self._build_resource_entry(method_payload)
        except Exception as e:
            raise ValueError(f"Invalid method payload: {e}")
        updated_resources = dataset.get("resources", []) + [new_resource]
        # Use partial update (PATCH) to add the new resource without affecting other fields
        result = self.patch_general_dataset(dataset_id, {"resources": updated_resources}, server=server)
        logger.info(f"Added new consumption method '{new_name}' to dataset '{dataset_id}'.")
        return result

    def _build_resource_entry(self, method: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal helper to construct a dataset resource entry from a method definition.
        """
        # Validate required keys in method definition
        if "type" not in method:
            raise ValueError("Method definition must include 'type'.")
        method = dict(method)
        method.setdefault("name", f"{method['type']} method")
        method.setdefault("description", method["name"])
        # Prepare base resource fields
        resource = {
            "name": method["name"],
            "description": json.dumps(method),
            "format": method["type"]
        }
        # If a URL is provided in config (for file-based or feed methods), use it as resource URL
        config = method.get("config", {})
        if config.get("url"):
            resource["url"] = config["url"]
        else:
            # For Kafka or other non-URL methods, construct a placeholder URL if possible
            method_type = method.get("type", "").lower()
            if method_type == "kafka":
                host = config.get("host", "")
                port = config.get("port", "")
                topic = config.get("topic", "")
                if host or port:
                    # Construct kafka://host:port/topic
                    addr = host
                    if port:
                        addr += f":{port}"
                    resource["url"] = f"kafka://{addr}/{topic}" if topic else f"kafka://{addr}"
                elif topic:
                    resource["url"] = f"kafka://{topic}"
                else:
                    resource["url"] = "kafka://"
            else:
                # Generic placeholder URL for other types without a direct URL
                resource["url"] = f"{method_type}://"
        return resource

    def _get_dataset(self, dataset_name: str, server: str = "local") -> Optional[Dict[str, Any]]:
        """
        Internal helper to fetch a dataset by name.
        Returns the dataset dict if found, otherwise None.
        """
        cache = getattr(self, "_mcp_dataset_cache", None)
        cache_key = None
        if isinstance(cache, dict):
            cache_key = (dataset_name.strip().lower(), server.strip().lower())
            cached = cache.get(cache_key)
            if isinstance(cached, dict):
                return cached

        try:
            # Use advanced_search for a direct name match search
            search_data = {"dataset_name": dataset_name, "server": server}
            result_list = self.advanced_search(search_data)
        except Exception as e:
            logger.error(f"Failed to retrieve dataset '{dataset_name}': {e}")
            return None
        if not result_list:
            return None
        # Find exact match by name (just in case search returns partial matches)
        for ds in result_list:
            if ds.get("name") == dataset_name:
                if cache_key and isinstance(cache, dict):
                    cache[cache_key] = ds
                return ds
        return None
