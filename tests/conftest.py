"""Offline fixtures shared with the pinned streaming v2 contract suite."""
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("v2_contract_fixtures", REPO_ROOT / "streaming_v2/tests/conftest.py")
_upstream = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_upstream)


class OfflineCatalog(_upstream.OfflineAPIClient):
    def search_datasets(self, terms, keys=None, server="local"):
        return super().search_datasets(terms, server=server)

    def register_general_dataset(self, data, server="local"):
        return super().register_general_dataset(data, server=server)

    def get_resource(self, resource_id, server="local"):
        dataset, _, index = self._locate_resource(resource_id)
        if dataset is None:
            raise ValueError(f"Resource '{resource_id}' not found")
        return {**dataset["resources"][index], "package_id": dataset["id"], "dataset_name": dataset["name"]}


@pytest.fixture
def catalog():
    return OfflineCatalog()


@pytest.fixture
def streaming(monkeypatch, catalog):
    from scidx_streaming_v2.streams.creation import common
    from scidx_streaming_v2.streams.creation.types import kafka

    client = _upstream.v2_client.__wrapped__(monkeypatch, catalog)
    # Keep real v2 source resolution, registration, and result wrapping;
    # replace only network topic creation and the live producer startup.
    monkeypatch.setattr(kafka, "_ensure_topic", lambda *args, **kwargs: True)
    monkeypatch.setattr(common, "_start_producer", lambda producer: None)
    return client


@pytest.fixture
def source(streaming, catalog):
    catalog.register_general_dataset({"name": "sensor-one", "title": "Sensor one", "owner_org": "sensors"})
    return streaming.register_resource(
        "sensor-one",
        {"type": "kafka", "name": "observations", "description": "Sensor feed",
         "config": {"host": "offline-kafka", "port": 19092, "topic": "raw.sensor"}},
        server="local",
    )
