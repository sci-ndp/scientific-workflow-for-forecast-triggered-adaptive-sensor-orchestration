# scidx_streaming/client/list_data_sources.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class StreamingDataSourceLister:
    """
    Mixin extending StreamingClient with a simple dataset listing utility.

    Purpose
    -------
    NDP EP does not yet support server-side filtering on dataset extras.
    This helper performs:
      1) a broad dataset search via search_datasets(...)
      2) local filtering using dataset extras (assumed to be dicts)

    Example
    -------
    streaming.list_data_sources(
        extras_filter={"dataset_kind": "hotspots", "region": "utah"}
    )
    """

    @staticmethod
    def _extras_to_dict(ds: Dict[str, Any]) -> Dict[str, str]:
        """
        Return dataset extras as a dict[str, str].

        Assumption:
        -----------
        extras are already stored as a dictionary, e.g.
        {
            "dataset_kind": "sensor",
            "region": "utah"
        }
        """
        extras = ds.get("extras")
        if isinstance(extras, dict):
            return {str(k): str(v) for k, v in extras.items()}
        return {}

    @staticmethod
    def _matches_extras(
        extras: Dict[str, str],
        extras_filter: Dict[str, Any],
    ) -> bool:
        """
        Check whether all key/value pairs in extras_filter
        exactly match the dataset extras.
        """
        for k, v in (extras_filter or {}).items():
            if v is None:
                continue
            if extras.get(str(k)) != str(v):
                return False
        return True

    def list_data_sources(
        self,
        *,
        extras_filter: Optional[Dict[str, Any]] = None,
        terms: Optional[List[str]] = None,
        limit: int = 50,
        server: str = "local",
    ) -> List[Dict[str, Any]]:
        """
        List datasets with optional local extras filtering.
        """
        extras_filter = extras_filter or {}

        # Build search terms from extras_filter if not provided
        if terms is None:
            terms = [
                str(v).strip()
                for v in extras_filter.values()
                if v is not None and str(v).strip()
            ]

        datasets = self.search_datasets(terms=terms, server=server) or []

        filtered: List[Dict[str, Any]] = []

        for ds in datasets:
            if not isinstance(ds, dict):
                continue

            extras = self._extras_to_dict(ds)

            if self._matches_extras(extras, extras_filter):
                ds_out = dict(ds)
                ds_out["extras"] = extras
                filtered.append(ds_out)

        if limit and limit > 0:
            filtered = filtered[:limit]

        return filtered
