"""Management tools registered by the Catalog MCP module."""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from beeai_framework.tools import AnyTool, tool as bee_tool

from scidx_streaming.agentic_mcp.utils import format_tool_output

if TYPE_CHECKING:  # pragma: no cover
    from scidx_streaming.agentic_mcp.mcps.catalog.server import CatalogMCPModule


logger = logging.getLogger(__name__)


def register_management_tools(
    module: "CatalogMCPModule",
    tool_supplier: Callable[[], List[str]],
) -> List[AnyTool]:
    """Attach management-focused tools to the catalog module."""

    client = module.client
    state = module.state
    tools: List[AnyTool] = []

    @bee_tool()
    def library_information() -> str:
        """Describe the SciDX streaming client, configuration, and active capabilities."""

        logger.info("library_information tool requested")
        service_catalog: List[Dict[str, Any]] = []
        describe_services = getattr(client, "describe_mcp_services", None)
        if callable(describe_services):
            try:
                service_catalog = describe_services()
            except Exception as exc:  # noqa: BLE001
                logger.debug("describe_mcp_services failed: %s", exc)

        info = {
            "client_base_url": getattr(client, "base_url", None),
            "user_id": getattr(client, "user_id", None),
            "kafka": {
                "host": getattr(client, "KAFKA_HOST", None),
                "port": getattr(client, "KAFKA_PORT", None),
                "prefix": getattr(client, "KAFKA_PREFIX", None),
                "max_streams": getattr(client, "MAX_STREAMS", None),
            },
            "available_tools": tool_supplier(),
            "context": state.snapshot(),
            "services": service_catalog,
            "usage_hints": {
                "create_kafka_stream": "You may omit resource_ids to reuse results from the latest search or registration.",
                "consume_stream": "If topic is omitted the most recent stream topic will be used.",
                "visualize_stream": "Provides a JSON preview of the consumer dataframe for quick inspection.",
                "registration": "Call scidx://registration-guide to view the full consumption method schema.",
                "delete_dataset": "Use delete_dataset to remove the most recent or specified CKAN dataset.",
            },
        }
        summary = [
            "Status: success",
            f"Tools available: {len(info['available_tools'])}",
            f"Services: {len(service_catalog)}",
            f"Active streams: {len(state.producers)}",
            f"Active consumers: {len(state.consumers)}",
        ]
        payload = format_tool_output(summary, info)
        logger.debug("library_information response summary=%s", summary)
        return payload

    tools.append(library_information)

    @bee_tool()
    def delete_dataset(
        dataset_identifier: Optional[str] = None,
        server: str = "local",
    ) -> str:
        """Delete a CKAN dataset registered through the streaming tools."""

        logger.info(
            "delete_dataset tool invoked dataset_identifier=%s server=%s",
            dataset_identifier,
            server,
        )
        candidates: List[str] = []
        if dataset_identifier and dataset_identifier.strip():
            candidates.append(dataset_identifier.strip())
        if state.last_registered_dataset and state.last_registered_dataset not in candidates:
            candidates.append(state.last_registered_dataset)

        if not candidates:
            return format_tool_output(
                [
                    "Status: error",
                    "No dataset identifier provided and no recent dataset is available in context.",
                ],
                {
                    "status": "error",
                    "message": "No dataset identifier provided and no recent dataset is available in context.",
                },
            )

        dataset_payload: Optional[Dict[str, Any]] = None
        dataset_name: Optional[str] = None
        dataset_id: Optional[str] = None
        errors: Dict[str, str] = {}

        def _load_dataset(candidate: str) -> Optional[Dict[str, Any]]:
            try:
                loader = getattr(client, "_get_dataset")
            except AttributeError as exc:  # pragma: no cover - defensive
                raise RuntimeError("_get_dataset helper missing on client") from exc
            try:
                return loader(candidate, server=server)
            except Exception as exc:  # noqa: BLE001
                errors[candidate] = str(exc)
                return None

        for candidate in candidates:
            name_candidate = state.resolve_dataset_name(candidate)
            if name_candidate and name_candidate in state.registered_datasets:
                dataset_payload = state.registered_datasets[name_candidate]
                dataset_name = name_candidate
                dataset_id = str(dataset_payload.get("id")) if dataset_payload.get("id") else None

            if dataset_payload is None and name_candidate:
                dataset_payload = _load_dataset(name_candidate)
                if dataset_payload:
                    dataset_name = dataset_payload.get("name") or name_candidate
                    dataset_id = str(dataset_payload.get("id")) if dataset_payload.get("id") else dataset_id

            if dataset_payload is None:
                id_candidate = state.resolve_dataset_id(candidate)
                if id_candidate and id_candidate in state.registered_datasets_by_id:
                    dataset_payload = state.registered_datasets_by_id[id_candidate]
                    dataset_name = dataset_payload.get("name") or dataset_name
                    dataset_id = id_candidate

            if dataset_payload is None:
                dataset_payload = _load_dataset(candidate)
                if dataset_payload:
                    dataset_name = dataset_payload.get("name") or dataset_name
                    dataset_id = str(dataset_payload.get("id")) if dataset_payload.get("id") else dataset_id

            if dataset_payload or dataset_id:
                break

        if not dataset_id and dataset_payload and dataset_payload.get("id"):
            dataset_id = str(dataset_payload.get("id"))

        if not dataset_id:
            dataset_id = candidates[-1]

        if not dataset_id:
            return format_tool_output(
                [
                    "Status: error",
                    "Unable to resolve a dataset ID for deletion.",
                ],
                {
                    "status": "error",
                    "message": "Unable to resolve a dataset ID for deletion.",
                    "attempted": candidates,
                    "errors": errors,
                },
            )

        try:
            delete_result = client.delete_resource_by_id(dataset_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "delete_dataset failed dataset_id=%s dataset_name=%s", dataset_id, dataset_name
            )
            return format_tool_output(
                ["Status: error", f"Reason: {exc}"],
                {
                    "status": "error",
                    "message": str(exc),
                    "dataset_id": dataset_id,
                    "dataset_name": dataset_name,
                },
            )

        state.forget_dataset(dataset_name or dataset_id)
        summary = [
            "Status: success",
            f"Dataset removed: {dataset_name or dataset_id}",
        ]
        payload = format_tool_output(
            summary,
            {
                "status": "success",
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "result": delete_result,
            },
        )
        logger.info(
            "delete_dataset succeeded dataset_id=%s dataset_name=%s",
            dataset_id,
            dataset_name,
        )
        return payload

    tools.append(delete_dataset)
    return tools


__all__ = ["register_management_tools"]
