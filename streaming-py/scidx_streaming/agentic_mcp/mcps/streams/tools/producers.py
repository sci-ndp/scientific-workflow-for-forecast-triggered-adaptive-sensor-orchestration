"""Producer and stream creation tools for the Streams MCP module."""

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

from beeai_framework.tools import AnyTool, tool as bee_tool

from scidx_streaming.agentic_mcp.core import MCPStreamState
from scidx_streaming.agentic_mcp.utils import format_tool_output

if TYPE_CHECKING:  # pragma: no cover
    from scidx_streaming.agentic_mcp.mcps.streams.server import StreamsMCPModule


logger = logging.getLogger(__name__)


def _normalise_filters(
    raw: Optional[Any],
) -> Tuple[Optional[List[str]], List[str], List[Dict[str, Any]]]:
    """Normalize filter semantics into plain string expressions."""

    if raw is None:
        return None, [], []

    entries: Sequence[Any]
    if isinstance(raw, list):
        entries = raw
    else:
        entries = [raw]

    backend_filters: List[str] = []
    display_filters: List[str] = []

    for entry in entries:
        if entry is None:
            continue
        if isinstance(entry, str):
            value = entry.strip()
            if value:
                backend_filters.append(value)
                display_filters.append(value)
            continue
        if isinstance(entry, dict):
            serialized = json.dumps(entry, sort_keys=True)
            backend_filters.append(serialized)
            display_filters.append(serialized)
            continue
        raise ValueError(
            f"Unsupported filter_semantics entry type: {type(entry).__name__}. Provide strings or dictionaries."
        )

    if not backend_filters:
        return None, [], []
    return backend_filters, display_filters, []


def register_producer_tools(module: "StreamsMCPModule") -> List[AnyTool]:
    """Attach Kafka stream creation tools to the module."""

    client = module.client
    state: MCPStreamState = module.state
    tools: List[AnyTool] = []

    @bee_tool()
    async def create_kafka_stream(
        resource_ids: Optional[List[str]] = None,
        filter_semantics: Optional[Any] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        server: str = "local",
        use_last_search: bool = True,
        dataset_names: Optional[List[str]] = None,
    ) -> str:
        """Create and start a Kafka producer using cached context when IDs are omitted."""

        logger.info(
            "create_kafka_stream invoked resource_ids=%s dataset_names=%s use_last_search=%s server=%s",
            resource_ids,
            dataset_names,
            use_last_search,
            server,
        )
        effective_ids = [str(rid) for rid in (resource_ids or []) if str(rid).strip()]
        if not effective_ids and dataset_names:
            effective_ids = state.get_resource_ids_from_datasets(dataset_names)
        if not effective_ids and use_last_search:
            effective_ids = state.get_resource_ids_from_search()

        if effective_ids:
            effective_ids = list(dict.fromkeys(effective_ids))

        if not effective_ids:
            return format_tool_output(
                [
                    "Status: error",
                    "No resource IDs provided or discoverable from context. Run search_consumption_methods or register_dataset first.",
                ],
                {
                    "status": "error",
                    "message": "No resource IDs provided or discoverable from context. Run search_consumption_methods or register_dataset first.",
                },
            )

        try:
            backend_filters, display_filters, filter_details = _normalise_filters(filter_semantics)
        except ValueError as exc:
            return format_tool_output(
                ["Status: error", str(exc)],
                {"status": "error", "message": str(exc)},
            )

        try:
            producer = await client.create_kafka_stream(
                consumption_method_ids=effective_ids,
                filter_semantics=backend_filters,
                username=username,
                password=password,
                server=server,
            )
        except Exception as exc:  # noqa: BLE001
            return format_tool_output(
                ["Status: error", f"Reason: {exc}"],
                {"status": "error", "message": str(exc)},
            )

        if isinstance(producer, dict):
            if "error" in producer:
                return format_tool_output(
                    ["Status: error", f"Reason: {producer.get('error')}"],
                    {"status": "error", "message": producer.get("error")},
                )
            topic = str(producer.get("data_stream_id")) if producer.get("data_stream_id") else None
        else:
            topic = getattr(producer, "data_stream_id", None)

        if not topic:
            return format_tool_output(
                ["Status: error", "Producer did not supply a topic identifier."],
                {"status": "error", "message": "Producer did not supply a topic identifier."},
            )

        if not isinstance(producer, dict):
            state.record_producer(
                topic,
                producer,
                resource_ids=effective_ids,
                filter_semantics=backend_filters or [],
                filter_details=filter_details,
            )
        summary = [
            "Status: success",
            f"Topic: {topic}",
            f"Resource IDs used: {len(effective_ids)}",
            f"Filter semantics applied: {len(backend_filters or [])}",
        ]
        payload = format_tool_output(
            summary,
            {
                "status": "success",
                "topic": topic,
                "used_resource_ids": effective_ids,
                "filter_semantics": display_filters or [],
                "filter_details": filter_details,
                "tracked_producers": list(state.producers.keys()),
                "context_hint": "You can now call consume_stream without parameters to attach to this topic.",
            },
        )
        logger.info(
            "create_kafka_stream succeeded topic=%s resources=%s filters=%s",
            topic,
            len(effective_ids),
            len(backend_filters or []),
        )
        return payload

    tools.append(create_kafka_stream)
    return tools


__all__ = ["register_producer_tools"]
