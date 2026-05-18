"""Streaming and filtering tools exposed by the Filtering MCP module."""

import json
import logging
from typing import TYPE_CHECKING, Callable, List, Optional

from beeai_framework.tools import AnyTool, tool as bee_tool

from scidx_streaming.agentic_mcp.utils import format_tool_output

if TYPE_CHECKING:  # pragma: no cover
    from scidx_streaming.agentic_mcp.mcps.filtering.server import FilteringMCPModule


logger = logging.getLogger(__name__)


def register_streaming_tools(
    module: "FilteringMCPModule",
    tool_supplier: Callable[[], List[str]] | None = None,
) -> List[AnyTool]:
    """Attach streaming management tools for Kafka consumers and producers."""

    del tool_supplier

    client = module.client
    state = module.state
    tools: List[AnyTool] = []

    @bee_tool()
    def consume_stream(
        topic: Optional[str] = None,
        use_last_topic: bool = True,
    ) -> str:
        """Start a threaded consumer for the selected Kafka topic."""

        logger.info(
            "consume_stream invoked topic=%s use_last_topic=%s",
            topic,
            use_last_topic,
        )
        selected_topic = (topic or "").strip() if topic else None
        if not selected_topic and use_last_topic:
            selected_topic = state.last_created_topic or state.last_consumed_topic
        if not selected_topic and state.producers:
            selected_topic = next(reversed(list(state.producers.keys())))

        if not selected_topic:
            return format_tool_output(
                [
                    "Status: error",
                    "No topic provided and no recent stream available. Create a stream first.",
                ],
                {
                    "status": "error",
                    "message": "No topic provided and no recent stream available. Create a stream first.",
                },
            )

        if selected_topic in state.consumers:
            return format_tool_output(
                [
                    "Status: success",
                    f"Consumer for '{selected_topic}' is already active.",
                ],
                {
                    "status": "success",
                    "message": f"Consumer for '{selected_topic}' is already active.",
                    "tracked_consumers": list(state.consumers.keys()),
                },
            )

        host = getattr(client, "KAFKA_HOST", None)
        port = getattr(client, "KAFKA_PORT", None)
        consumer = client.consume_kafka_messages(topic=selected_topic, host=host, port=port)
        state.record_consumer_start(selected_topic, consumer)
        summary = [
            "Status: success",
            f"Consumer started for '{selected_topic}'",
            f"Active consumers: {len(state.consumers)}",
        ]
        payload = format_tool_output(
            summary,
            {
                "status": "success",
                "message": f"Consumer started for '{selected_topic}'.",
                "tracked_consumers": list(state.consumers.keys()),
                "context_hint": "You can visualize or stop this consumer without specifying the topic explicitly.",
            },
        )
        logger.info("consume_stream started consumer topic=%s", selected_topic)
        return payload

    tools.append(consume_stream)

    @bee_tool()
    def visualize_stream(
        topic: Optional[str] = None,
        max_rows: int = 5,
        use_last_topic: bool = True,
    ) -> str:
        """Display a sample of the buffered dataframe for the active consumer."""

        logger.info(
            "visualize_stream invoked topic=%s max_rows=%s use_last_topic=%s",
            topic,
            max_rows,
            use_last_topic,
        )
        selected_topic = (topic or "").strip() if topic else None
        if not selected_topic and use_last_topic:
            selected_topic = state.last_consumed_topic or state.last_created_topic

        consumer = state.consumers.get(selected_topic) if selected_topic else None
        if consumer is None:
            available = list(state.consumers.keys())
            message = (
                f"No consumer is registered for topic '{selected_topic}'."
                if selected_topic
                else "No active consumer found."
            )
            return format_tool_output(
                ["Status: error", message],
                {
                    "status": "error",
                    "message": message,
                    "active_consumers": available,
                },
            )

        df = consumer.dataframe
        if df is None or df.empty:
            return format_tool_output(
                [
                    "Status: warning",
                    "Consumer has not received data yet.",
                ],
                {
                    "status": "warning",
                    "message": "Consumer has not received data yet.",
                    "topic": selected_topic,
                },
            )

        preview = df.head(max_rows)
        rows = json.loads(preview.to_json(orient="records"))
        summary = [
            "Status: success",
            f"Topic: {selected_topic}",
            f"Total rows buffered: {len(df)}",
            f"Previewing: {min(max_rows, len(df))} rows",
        ]
        payload = format_tool_output(
            summary,
            {
                "status": "success",
                "topic": selected_topic,
                "row_count": int(len(df)),
                "sample": rows,
            },
        )
        logger.info(
            "visualize_stream preview topic=%s rows=%s preview_rows=%s",
            selected_topic,
            len(df),
            min(max_rows, len(df)),
        )
        return payload

    tools.append(visualize_stream)

    @bee_tool()
    def stop_consumer(
        topic: Optional[str] = None,
        use_last_topic: bool = True,
    ) -> str:
        """Stop an active consumer and remove it from the state cache."""

        logger.info(
            "stop_consumer invoked topic=%s use_last_topic=%s",
            topic,
            use_last_topic,
        )
        selected_topic = (topic or "").strip() if topic else None
        if not selected_topic and use_last_topic:
            selected_topic = state.last_consumed_topic

        if not selected_topic:
            return format_tool_output(
                [
                    "Status: warning",
                    "No topic specified and no recent consumer found.",
                ],
                {
                    "status": "warning",
                    "message": "No topic specified and no recent consumer found.",
                    "active_consumers": list(state.consumers.keys()),
                },
            )

        consumer = state.consumers.get(selected_topic)
        if consumer is None:
            return format_tool_output(
                [
                    "Status: warning",
                    f"No consumer was registered for '{selected_topic}'.",
                ],
                {
                    "status": "warning",
                    "message": f"No consumer was registered for '{selected_topic}'.",
                    "active_consumers": list(state.consumers.keys()),
                },
            )

        try:
            consumer.stop()
        except Exception as exc:  # noqa: BLE001
            return format_tool_output(
                [
                    "Status: error",
                    f"Consumer for '{selected_topic}' raised while stopping: {exc}",
                ],
                {
                    "status": "error",
                    "message": f"Consumer for '{selected_topic}' raised while stopping: {exc}",
                },
            )

        state.record_consumer_stop(selected_topic)
        summary = [
            "Status: success",
            f"Consumer for '{selected_topic}' stopped.",
            f"Active consumers remaining: {len(state.consumers)}",
        ]
        payload = format_tool_output(
            summary,
            {
                "status": "success",
                "message": f"Consumer for '{selected_topic}' stopped.",
                "active_consumers": list(state.consumers.keys()),
            },
        )
        logger.info("stop_consumer completed topic=%s", selected_topic)
        return payload

    tools.append(stop_consumer)

    @bee_tool()
    async def delete_stream(
        topic: Optional[str] = None,
        use_last_topic: bool = True,
    ) -> str:
        """Delete a Kafka stream and clean up associated producers/consumers."""

        logger.info(
            "delete_stream invoked topic=%s use_last_topic=%s",
            topic,
            use_last_topic,
        )
        selected_topic = (topic or "").strip() if topic else None
        if not selected_topic and use_last_topic:
            selected_topic = state.last_created_topic or state.last_consumed_topic

        if not selected_topic:
            return format_tool_output(
                ["Status: error", "No topic provided and no recent stream found to delete."],
                {
                    "status": "error",
                    "message": "No topic provided and no recent stream found to delete.",
                },
            )

        producer = state.producers.get(selected_topic)
        if producer is not None:
            try:
                await producer.stop()
            except Exception:
                pass
            finally:
                state.forget_producer(selected_topic)

        consumer = state.consumers.get(selected_topic)
        if consumer is not None:
            try:
                consumer.stop()
            except Exception:
                pass
            finally:
                state.record_consumer_stop(selected_topic)

        try:
            result = await client.delete_stream(selected_topic)
        except Exception as exc:  # noqa: BLE001
            logger.exception("delete_stream failed topic=%s", selected_topic)
            return format_tool_output(
                ["Status: error", f"Reason: {exc}"],
                {"status": "error", "message": str(exc)},
            )

        summary = [
            "Status: success",
            f"Stream '{selected_topic}' deleted.",
            f"Remaining streams: {len(state.producers)}",
        ]
        payload = format_tool_output(
            summary,
            {
                "status": "success",
                "topic": selected_topic,
                "result": result,
                "active_streams": list(state.producers.keys()),
                "active_consumers": list(state.consumers.keys()),
            },
        )
        logger.info("delete_stream succeeded topic=%s", selected_topic)
        return payload
    tools.append(delete_stream)
    return tools


__all__ = ["register_streaming_tools"]
