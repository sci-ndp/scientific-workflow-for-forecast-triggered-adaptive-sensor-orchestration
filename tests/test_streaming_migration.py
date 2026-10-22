import asyncio
import json

import blosc
import msgpack
import pytest

from src.orchestration_model import SciDxStreamOrchestrator, SensorCandidate
from src.streaming_support import (
    MessageInbox, decode_kafka_message, message_payloads, prepare_consumer,
    register_dataset_resources, resource_selectors,
)


def legacy_message(messages):
    envelope = {"values": {"timestamp": ["2025-01-16"] * len(messages), "payload": messages}, "stream_info": {}}
    return blosc.compress(msgpack.packb(envelope, use_bin_type=True), cname="zstd")


def test_resource_uuid_resolves_to_v2_selector_and_creates_stream(streaming, catalog, source):
    selectors = resource_selectors(catalog, [source["id"], source["id"]], server="local")
    assert selectors == [("sensor-one", "observations")]
    result = asyncio.run(streaming.acreate_stream(resources=selectors, filters=[], server="local"))
    assert result.topic
    assert result.sources[0].id == source["id"]
    assert result.dataset_id == "sensor-one"
    assert result.resource["format"] == "kafka"


@pytest.mark.parametrize("ids", [[], ["missing-resource"]])
def test_invalid_resource_ids_fail_before_creation(catalog, ids):
    with pytest.raises(ValueError):
        resource_selectors(catalog, ids, server="local")


def test_register_prediction_and_hotspot_csvs_is_idempotent(streaming, catalog):
    for kind in ("prediction", "hotspot"):
        metadata = {"name": f"{kind}_utah", "title": kind, "owner_org": kind, "extras": {"region": "utah"}}
        methods = [{"type": "csv", "name": f"{kind}_csv", "description": "Hosted CSV",
                    "config": {"url": f"https://example.test/{kind}.csv"},
                    "processing": {"delimiter": ",", "header_line": 0}}]
        first = register_dataset_resources(catalog, streaming, metadata, methods, server="local")
        second = register_dataset_resources(catalog, streaming, metadata, methods, server="local")
        assert first["name"] == second["name"] == metadata["name"]
        datasets = catalog.search_datasets([metadata["name"]])
        assert len(datasets) == 1
        assert len(datasets[0]["resources"]) == 1
        found = streaming.search_resources(terms=[metadata["name"]], server="local")
        assert found[0].metadata["url"] == methods[0]["config"]["url"]


def test_decoder_reads_legacy_columnar_batches_and_v2_json():
    messages = [{"stid": "QLN", "value": 5}, {"stid": "QLN", "value": 6}]
    assert message_payloads(decode_kafka_message(legacy_message(messages))) == messages
    assert message_payloads(decode_kafka_message(json.dumps(messages[0]).encode())) == [messages[0]]
    assert message_payloads(decode_kafka_message(json.dumps(messages).encode())) == messages
    assert message_payloads({"payload": messages}) == messages
    assert message_payloads({"values": {"timestamp": [1, 2], "value": [5, 6]}}) == [
        {"timestamp": 1, "value": 5}, {"timestamp": 2, "value": 6},
    ]


def test_decoder_rejects_corrupt_messages():
    with pytest.raises(ValueError, match="neither JSON nor legacy"):
        decode_kafka_message(b"\xff\x00\x01")


def test_inbox_tracks_messages_after_bounded_dataframe_eviction(streaming):
    inbox = MessageInbox()
    handle = prepare_consumer(streaming, "existing-topic", inbox)
    assert handle._default_from_beginning is True
    assert handle.consumer_factory.keywords["value_deserializer"] is decode_kafka_message
    handle.set_retention(max_records=2)
    messages = [{"value": i} for i in range(10)]
    handle.ingest(messages)
    assert handle.dataframe()["value"].tolist() == [8, 9]
    assert inbox.take(limit=3) == messages[:3]
    assert inbox.take() == messages[3:]
    assert inbox.take() == []
    handle.stop()


def test_inbox_overflow_reports_data_loss():
    inbox = MessageInbox(max_pending=1)
    inbox({"value": 1}, 0)
    inbox({"value": 2}, 0)
    with pytest.raises(RuntimeError, match="queue is full"):
        inbox.take()


def test_consumer_delivers_backlog_present_during_partition_assignment(monkeypatch, streaming):
    from threading import Event
    from types import SimpleNamespace
    from kafka import TopicPartition
    from src import streaming_support

    delivered = Event()
    messages = [{"payload": {"sequence": i}} for i in range(5)]

    class AssigningKafka:
        def __init__(self, *topics, value_deserializer=None, **kwargs):
            self.assigned = set()
            self.position = 0
            self.decoder = value_deserializer
            self.closed = False

        def partitions_for_topic(self, topic):
            return {0}

        def assignment(self):
            return self.assigned

        def assign(self, partitions):
            self.assigned = set(partitions)

        def seek_to_beginning(self, *partitions):
            self.position = 0

        def poll(self, timeout_ms=0):
            if not self.assigned:
                if timeout_ms == 0:
                    return {}
                self.assigned = {TopicPartition("backlog", 0)}
            if self.position:
                delivered.wait(0.01)
                return {}
            self.position = len(messages)
            return {next(iter(self.assigned)): [
                SimpleNamespace(value=self.decoder(json.dumps(message).encode())) for message in messages
            ]}

        def close(self):
            self.closed = True

    monkeypatch.setattr(streaming_support, "KafkaConsumer", AssigningKafka)
    inbox = MessageInbox()
    handle = prepare_consumer(streaming, "backlog", inbox)
    handle.add_callback(lambda record, timestamp: delivered.set() if record == messages[-1] else None)
    try:
        handle.start(poll_interval=0.01)
        assert delivered.wait(2), "Messages fetched during startup must still reach callbacks"
        assert inbox.take() == [message["payload"] for message in messages]
        assert handle.summary()["total_consumed"] == 5
    finally:
        handle.stop()


def test_orchestrator_uses_v2_sync_creation_inside_an_event_loop(streaming, source):
    sensor = SensorCandidate(sensor_id="QLN", name="sensor-one", lat=40, lon=-111,
                             resource_id=source["id"], metadata={"server": "local"})
    async def activate():
        return SciDxStreamOrchestrator().observe([sensor], streaming)
    streams = asyncio.run(activate())
    assert len(streams) == 1
    assert streams[0].topic == streams[0].stream_id
    assert streams[0].topic
    assert streams[0].metadata["resource_id"] == source["id"]


def test_v2_filter_comparisons_work(streaming):
    from scidx_streaming_v2.filters import apply_filters
    filters = streaming.compile_filters(["value > 10"])
    assert apply_filters({"value": 12}, filters).to_dict("records") == [{"value": 12}]
    assert apply_filters({"value": 8}, filters).empty


def test_uuid_selection_falls_back_when_resource_route_and_local_catalog_are_unavailable(monkeypatch, catalog, source):
    original_search = catalog.search_datasets
    def unavailable(*args, **kwargs):
        raise ValueError("Resource route not available on older endpoint")
    def search(terms, server="local", **kwargs):
        if server == "local":
            raise ValueError("Local CKAN unavailable")
        return original_search(terms, server=server)
    monkeypatch.setattr(catalog, "get_resource", unavailable)
    monkeypatch.setattr(catalog, "search_datasets", search)
    assert resource_selectors(catalog, [source["id"]], server="local") == [("sensor-one", "observations")]
