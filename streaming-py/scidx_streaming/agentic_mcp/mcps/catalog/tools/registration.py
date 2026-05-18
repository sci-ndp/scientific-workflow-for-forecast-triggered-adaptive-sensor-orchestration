"""Dataset registration and discovery tools for the Catalog MCP module."""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from beeai_framework.tools import AnyTool, tool as bee_tool

from scidx_streaming.agentic_mcp.core import MCPStreamState
from scidx_streaming.agentic_mcp.utils import format_tool_output

if TYPE_CHECKING:  # pragma: no cover
    from scidx_streaming.agentic_mcp.mcps.catalog.server import CatalogMCPModule


logger = logging.getLogger(__name__)


def register_dataset_tools(
    module: "CatalogMCPModule",
    tool_supplier: Callable[[], List[str]] | None = None,
) -> List[AnyTool]:
    """Attach dataset registration, method registration, and search tools."""

    del tool_supplier  # retained for signature parity in case future tools need it

    client = module.client
    state = module.state
    tools: List[AnyTool] = []

    @bee_tool()
    def register_dataset(
        dataset_metadata: Dict[str, Any],
        methods: Optional[List[Dict[str, Any]]] = None,
        server: str = "local",
    ) -> str:
        """Create a CKAN dataset with optional consumption methods."""

        logger.info(
            "register_dataset tool invoked name=%s methods=%s server=%s",
            dataset_metadata.get("name"),
            len(methods or []),
            server,
        )
        try:
            result = client.register_data_source(
                dataset_metadata=dataset_metadata,
                methods=methods,
                server=server,
            )
            dataset_payload: Dict[str, Any] = result
            dataset_name = dataset_metadata.get("name")
            if isinstance(result, dict) and result.get("name"):
                dataset_name = result.get("name")

            if dataset_name:
                try:
                    refreshed = client._get_dataset(dataset_name, server=server)  # noqa: SLF001
                    if isinstance(refreshed, dict):
                        dataset_payload = refreshed
                except Exception:
                    dataset_payload = result

            state.record_dataset(dataset_payload)
            resource_ids = MCPStreamState._extract_resource_ids([dataset_payload])
            summary = [
                "Status: success",
                f"Dataset: {dataset_payload.get('name') or 'unknown'}",
                f"CKAN ID: {dataset_payload.get('id') or 'unknown'}",
                f"Resources tracked: {len(dataset_payload.get('resources') or [])}",
            ]
            payload = format_tool_output(
                summary,
                {
                    "status": "success",
                    "dataset": dataset_payload,
                    "dataset_name": dataset_payload.get("name"),
                    "resource_ids": resource_ids,
                    "context_hint": "These resource IDs can be reused in later tool calls (e.g., create_kafka_stream).",
                },
            )
            logger.info(
                "register_dataset succeeded dataset_name=%s resources=%s",
                dataset_payload.get("name"),
                len(resource_ids),
            )
            return payload
        except Exception as exc:  # noqa: BLE001
            logger.exception("register_dataset failed dataset_name=%s", dataset_metadata.get("name"))
            return format_tool_output(
                ["Status: error", f"Reason: {exc}"],
                {"status": "error", "message": str(exc)},
            )

    tools.append(register_dataset)

    @bee_tool()
    def register_consumption_method(
        dataset_id: str,
        method_payload: Dict[str, Any],
        server: str = "local",
    ) -> str:
        """Attach a consumption method to an existing dataset."""

        logger.info(
            "register_consumption_method invoked dataset_id=%s method=%s server=%s",
            dataset_id,
            method_payload.get("name") or method_payload.get("type"),
            server,
        )
        try:
            dataset_identifier = dataset_id or ""
            resolved_name = state.resolve_dataset_name(dataset_identifier)
            if not resolved_name:
                resolved_name = dataset_identifier or state.last_registered_dataset

            if not resolved_name:
                return format_tool_output(
                    [
                        "Status: error",
                        "Dataset identifier not provided or unknown. Try registering or referencing a dataset first.",
                    ],
                    {
                        "status": "error",
                        "message": "Dataset identifier not provided or unknown. Try registering or referencing a dataset first.",
                    },
                )

            result = client.add_consumption_method(
                dataset_id=resolved_name,
                method_payload=method_payload,
                server=server,
            )
            dataset_payload: Dict[str, Any] = result
            dataset_name = None
            if isinstance(result, dict):
                dataset_name = result.get("name") or result.get("id")
            if not dataset_name and resolved_name:
                dataset_name = resolved_name
            if dataset_name:
                try:
                    refreshed = client._get_dataset(dataset_name, server=server)  # noqa: SLF001
                    if isinstance(refreshed, dict):
                        dataset_payload = refreshed
                except Exception:
                    dataset_payload = result
            if isinstance(dataset_payload, dict) and dataset_name and not dataset_payload.get("name"):
                dataset_payload = {**dataset_payload, "name": dataset_name}

            state.record_dataset(dataset_payload)
            resource_ids = MCPStreamState._extract_resource_ids([dataset_payload])
            summary = [
                "Status: success",
                f"Dataset: {dataset_payload.get('name') or resolved_name or 'unknown'}",
                f"CKAN ID: {dataset_payload.get('id') or 'unknown'}",
                f"Methods now tracked: {len(dataset_payload.get('resources') or [])}",
                f"Method added: {method_payload.get('name') or method_payload.get('type')}",
            ]
            payload = format_tool_output(
                summary,
                {
                    "status": "success",
                    "dataset": dataset_payload,
                    "dataset_name": dataset_payload.get("name"),
                    "resource_ids": resource_ids,
                    "context_hint": "Use the resource_ids field when creating streams or exploring methods.",
                },
            )
            logger.info(
                "register_consumption_method succeeded dataset_name=%s method=%s",
                dataset_payload.get("name") or resolved_name,
                method_payload.get("name") or method_payload.get("type"),
            )
            return payload
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "register_consumption_method failed dataset_id=%s method=%s",
                dataset_id,
                method_payload.get("name") or method_payload.get("type"),
            )
            return format_tool_output(
                ["Status: error", f"Reason: {exc}"],
                {"status": "error", "message": str(exc)},
            )

    tools.append(register_consumption_method)

    @bee_tool()
    def search_consumption_methods(
        keywords: List[str],
        method_types: Optional[List[str]] = None,
        server: str = "local",
    ) -> str:
        """Search datasets and cache consumption methods grouped by dataset."""

        logger.info(
            "search_consumption_methods invoked keywords=%s method_types=%s server=%s",
            keywords,
            method_types,
            server,
        )
        try:
            results = client.search_consumption_methods(
                terms=keywords,
                types=method_types,
                server=server,
            )
            state.record_search(keywords, results)
            resource_ids = state.get_resource_ids_from_search(keywords)
            summary = [
                "Status: success",
                f"Datasets matched: {len(results)}",
                f"Keywords: {', '.join(keywords) if keywords else 'none'}",
                f"Resource IDs cached: {len(resource_ids)}",
            ]
            payload = format_tool_output(
                summary,
                {
                    "status": "success",
                    "results": results,
                    "resource_ids": resource_ids,
                    "context_hint": "Pass the resource_ids directly to create_kafka_stream or omit them to reuse automatically.",
                },
            )
            logger.info(
                "search_consumption_methods completed matches=%s cached_resources=%s",
                len(results),
                len(resource_ids),
            )
            return payload
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "search_consumption_methods failed keywords=%s method_types=%s",
                keywords,
                method_types,
            )
            return format_tool_output(
                ["Status: error", f"Reason: {exc}"],
                {"status": "error", "message": str(exc)},
            )

    tools.append(search_consumption_methods)
    return tools


__all__ = ["register_dataset_tools"]
