# scidx_streaming/client/delete_resource.py
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from scidx_streaming.method_types import SUPPORTED_METHOD_TYPES

logger = logging.getLogger(__name__)

_SUPPORTED_TYPES = SUPPORTED_METHOD_TYPES


class StreamingResourceDeletion:
    """
    Mixin extending StreamingClient with resource-level (per-method) deletion.

    Notes
    -----
    - This removes a *single CKAN resource* from the dataset by PATCHing the dataset's
      'resources' list (same mechanism you use to add/patch methods).
    - Verifies removal and retries once on concurrent modification.
    - By default, it only deletes resources that look like SciDX 'consumption methods'
      (JSON description with a supported 'type'). Pass force=True to override.
    """

    # ---------- Public API --------------------------------------------------

    def delete_consumption_method_by_id(
        self,
        resource_id: str,
        *,
        server: str = "local",
        force: bool = False,
        retries: int = 1,
    ) -> Dict[str, Any]:
        """
        Delete one CKAN dataset resource (consumption method) by resource ID.

        Strategy:
        A) Attempt hard-remove by excluding the resource from the dataset's resources list.
        B) If backend ignores removals (merge-only), fallback to soft-delete by tombstoning:
            - set resource.format = "__deleted__"
            - mutate description JSON to {"type": "__deleted__", "deleted": true, ...}
        C) Verify via discovery (search_consumption_methods) that the resource is gone.
        """
        if not resource_id:
            raise ValueError("resource_id is required")

        attempts = 0
        last_err: Optional[Exception] = None

        while attempts <= max(0, retries):
            attempts += 1

            ds, res = self._locate_dataset_by_resource_id(resource_id, server=server)
            if not ds or not res:
                raise ValueError(f"Resource '{resource_id}' not found on server='{server}'")

            if not force and not self._looks_like_streaming_method(res):
                raise ValueError(
                    f"Resource '{resource_id}' does not look like a SciDX streaming consumption method. "
                    f"Use force=True to delete anyway."
                )

            ds_name = ds.get("name") or ""
            current_resources = ds.get("resources") or []

            # ---------- A) Try HARD remove (exclude from list) ----------
            new_resources = [r for r in current_resources if r.get("id") != resource_id]
            try:
                result = self.patch_general_dataset(ds_name, {"resources": new_resources}, server=server)
            except Exception as e:
                last_err = e
                logger.warning(
                    "patch_general_dataset (hard-remove) failed (attempt %d/%d): %s",
                    attempts, max(1, retries + 1), e
                )
                # re-loop to re-fetch and retry
                continue

            # Verify hard remove by a fresh dataset fetch
            try:
                verify_ds = self._get_dataset(ds_name, server=server) or {}
            except Exception as e:
                logger.warning("Post-delete hard-verify failed for dataset '%s': %s", ds_name, e)
                return result

            still_in_dataset = any(r.get("id") == resource_id for r in (verify_ds.get("resources") or []))
            if not still_in_dataset:
                logger.info("Deleted resource '%s' from dataset '%s' (hard remove).", resource_id, ds_name)
                return result

            # ---------- B) Fallback: SOFT delete (tombstone) ----------
            # Keep all resources, but mutate the target to an unsupported format + tombstone description
            mutated: List[Dict[str, Any]] = []
            for r in (verify_ds.get("resources") or []):
                if r.get("id") != resource_id:
                    mutated.append(r)
                    continue

                r2 = dict(r)
                r2["format"] = "__deleted__"
                # mutate description json
                desc_text = r2.get("description") or ""
                try:
                    desc_obj = json.loads(desc_text) if desc_text else {}
                    if not isinstance(desc_obj, dict):
                        desc_obj = {"value": desc_obj}
                except Exception:
                    desc_obj = {}
                desc_obj["type"] = "__deleted__"
                desc_obj["deleted"] = True
                r2["description"] = json.dumps(desc_obj)
                mutated.append(r2)

            try:
                result = self.patch_general_dataset(ds_name, {"resources": mutated}, server=server)
            except Exception as e:
                last_err = e
                logger.warning(
                    "patch_general_dataset (soft-delete) failed (attempt %d/%d): %s",
                    attempts, max(1, retries + 1), e
                )
                continue

            # ---------- C) Verify via discovery (search filters out unsupported types) ----------
            try:
                hits = self.search_consumption_methods(terms=[ds_name], types=["*"], server=server) or []
            except Exception as e:
                logger.warning("Discovery verify failed for dataset '%s': %s", ds_name, e)
                return result

            present_in_search = any(
                any((r.get("id") == resource_id) for r in (ds_entry.get("resources") or []))
                for ds_entry in hits
            )
            if not present_in_search:
                logger.info("Deleted resource '%s' from dataset '%s' (soft tombstone).", resource_id, ds_name)
                return result

            # Still present in search; loop and try again
            last_err = RuntimeError(f"Resource '{resource_id}' still present after patch")
            logger.warning("%s; retrying (%d/%d)", last_err, attempts, max(1, retries + 1))

        raise ValueError(f"Failed to delete resource '{resource_id}' after {attempts} attempt(s): {last_err}")


    def delete_consumption_methods_by_ids(
        self,
        resource_ids: List[str],
        *,
        server: str = "local",
        force: bool = False,
        stop_on_error: bool = False,
        retries: int = 1,
    ) -> Dict[str, Any]:
        """
        Bulk delete wrapper.

        Returns
        -------
        dict
            {"ok": [ids...], "failed": {id: "error", ...}}
        """
        summary: Dict[str, Any] = {"ok": [], "failed": {}}
        for rid in (resource_ids or []):
            try:
                self.delete_consumption_method_by_id(rid, server=server, force=force, retries=retries)
                summary["ok"].append(rid)
            except Exception as e:
                if stop_on_error:
                    raise
                summary["failed"][rid] = str(e)
        return summary

    # ---------- Internals ---------------------------------------------------

    def _locate_dataset_by_resource_id(
        self, resource_id: str, *, server: str = "local"
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Scan datasets to find the one that contains the given CKAN resource ID.
        """
        try:
            datasets = self.advanced_search({"server": server}) or []
        except Exception as e:
            logger.error("advanced_search(server=%s) failed: %s", server, e)
            return None, None

        for ds in datasets:
            for res in (ds.get("resources") or []):
                if res.get("id") == resource_id:
                    return ds, res
        return None, None

    @staticmethod
    def _looks_like_streaming_method(res: Dict[str, Any]) -> bool:
        """
        Heuristic: a streaming 'consumption method' has JSON description with a supported 'type'.
        """
        try:
            desc = res.get("description") or ""
            if not desc:
                return False
            parsed = json.loads(desc)
            if not isinstance(parsed, dict):
                return False
            t = str(parsed.get("type") or "").lower()
            return t in _SUPPORTED_TYPES
        except Exception:
            return False
