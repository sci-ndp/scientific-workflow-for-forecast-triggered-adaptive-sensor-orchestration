"""Application integration with streaming v2, including legacy Kafka payloads.

WebSocket ingestion is intentionally left to the upstream streaming library.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from functools import partial
from queue import Empty, Full, Queue
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from scidx_streaming_v2 import StreamingClient


def resource_selectors(
    client: Any,
    resource_ids: Sequence[str],
    *,
    server: str,
    dataset_names: Sequence[str] | None = None,
) -> list[tuple[str, str]]:
    """Translate resource UUIDs, with a search fallback for older NDP servers."""
    identifiers = list(dict.fromkeys(resource_ids))
    if not identifiers:
        raise ValueError("At least one resource ID is required.")
    selectors: dict[str, tuple[str, str]] = {}
    for resource_id in identifiers:
        try:
            resource = client.get_resource(resource_id, server=server)
        except (ValueError, AttributeError):
            continue
        dataset = resource.get("dataset_name") or resource.get("package_id") or resource.get("dataset_id")
        if dataset and resource.get("name"):
            selectors[resource_id] = (str(dataset), str(resource["name"]))

    if len(selectors) != len(identifiers):
        # Some deployed endpoints support dataset search but not the newer
        # resource-by-ID route. Match exact UUIDs within their dataset records.
        # Use the same preferred/local/global scope order as v2 discovery.
        for scope in dict.fromkeys((server, "local", "global")):
            try:
                datasets = client.search_datasets(terms=list(dataset_names or [""]), server=scope) or []
            except ValueError:
                continue
            for dataset in datasets:
                dataset_ref = dataset.get("name") or dataset.get("id")
                for resource in dataset.get("resources") or []:
                    resource_id = resource.get("id")
                    if resource_id in identifiers and resource_id not in selectors and dataset_ref and resource.get("name"):
                        selectors[resource_id] = (str(dataset_ref), str(resource["name"]))
            if len(selectors) == len(identifiers):
                break
    missing = [resource_id for resource_id in identifiers if resource_id not in selectors]
    if missing:
        raise ValueError(f"Resource ID(s) not found or missing dataset/name metadata: {', '.join(missing)}")
    return [selectors[resource_id] for resource_id in identifiers]


def register_dataset_resources(
    client: Any,
    streaming: StreamingClient,
    metadata: dict[str, Any],
    resources: Sequence[dict[str, Any]],
    *,
    server: str,
) -> dict[str, Any]:
    """Create/find a dataset, then attach resources with v2's resource API."""
    name = metadata["name"]
    matches = client.search_datasets(terms=[name], server=server) or []
    dataset = next((ds for ds in matches if ds.get("name") == name), None)
    if dataset is None:
        dataset = client.register_general_dataset(dict(metadata), server=server)
    dataset_id = dataset.get("id") or name
    registered = [streaming.register_resource(dataset_id, resource, server=server) for resource in resources]
    return {**metadata, **dataset, "name": name, "resources": registered}


def decode_kafka_message(raw: Any) -> dict[str, Any]:
    """Decode v2 JSON and the old library's Blosc/MessagePack envelope."""
    if isinstance(raw, Mapping):
        return dict(raw)
    if raw is None:
        return {}
    if isinstance(raw, (bytes, bytearray, memoryview)):
        raw = bytes(raw)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            import blosc
            import msgpack

            try:
                value = msgpack.unpackb(blosc.decompress(raw), raw=False)
            except Exception as exc:
                raise ValueError("Kafka message is neither JSON nor legacy Blosc/MessagePack.") from exc
    elif isinstance(raw, str):
        value = json.loads(raw)
    else:
        value = raw
    return dict(value) if isinstance(value, Mapping) else {"value": value}


def message_payloads(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Extract sensor messages from v2 records and legacy columnar batches."""
    values = record.get("values")
    if isinstance(values, Mapping):
        lengths = [len(value) for value in values.values() if isinstance(value, list)]
        if lengths:
            rows = [
                {key: (value[i] if i < len(value) else None) if isinstance(value, list) else value
                 for key, value in values.items()}
                for i in range(max(lengths))
            ]
        else:
            rows = [dict(values)]
    else:
        rows = [dict(record)]
    messages = []
    for row in rows:
        payload = row.get("payload", row.get("value", row))
        if isinstance(payload, Mapping):
            messages.append(dict(payload))
        elif isinstance(payload, list):
            messages.extend(dict(item) for item in payload if isinstance(item, Mapping))
        else:
            messages.append(row)
    return messages


class MessageInbox:
    """Receive every consumer callback, independently of preview-buffer eviction.

    Overflow fails explicitly instead of silently dropping sensor messages.
    """

    def __init__(self, max_pending: int = 10000) -> None:
        self._messages: Queue[dict[str, Any]] = Queue(maxsize=max_pending)
        self._overflow = False

    def __call__(self, record: dict[str, Any], timestamp: float) -> None:
        for message in message_payloads(record):
            try:
                self._messages.put_nowait(message)
            except Full:
                self._overflow = True
                break

    def take(self, limit: int = 1000) -> list[dict[str, Any]]:
        if self._overflow:
            raise RuntimeError("Stream processing cannot keep up: the pending-message queue is full.")
        messages = []
        for _ in range(limit):
            try:
                messages.append(self._messages.get_nowait())
            except Empty:
                break
        return messages


def _assigned_consumer(topic: str, **kwargs: Any):
    """Assign this preview's partitions before v2 primes its starting offsets.

    V2's subscription-assignment wait polls Kafka and discards that poll's
    records. Explicit assignment ensures its initial seek happens after
    assignment, so an existing backlog is read from the requested offset.
    These preview consumers have no consumer group or committed offsets.
    """
    consumer = KafkaConsumer(**kwargs)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise ValueError(f"Kafka topic has no available partitions: {topic}")
        consumer.assign([TopicPartition(topic, partition) for partition in sorted(partitions)])
        return consumer
    except BaseException:
        consumer.close()
        raise


def prepare_consumer(streaming: StreamingClient, topic: str, inbox: MessageInbox):
    """Configure a v2 handle; callers explicitly start and stop consumption."""
    handle = streaming.consume_stream(topic, from_beginning=True)
    # StreamHandle accepts an injected consumer factory. Kafka applies the
    # deserializer before v2 buffers records and invokes callbacks.
    handle.consumer_factory = partial(_assigned_consumer, value_deserializer=decode_kafka_message)
    handle.add_callback(inbox)
    return handle
