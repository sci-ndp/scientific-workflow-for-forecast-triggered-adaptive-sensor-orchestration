"""Exercise the full forecast application with offline catalog and Kafka IO."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src import forecast_api as api
from src.streaming_support import prepare_consumer


@pytest.fixture
def forecast_api(monkeypatch, catalog, streaming):
    monkeypatch.setattr(api, "_create_clients", lambda server=None: (server or "local", catalog, streaming))
    monkeypatch.setattr(api, "APIClient", lambda **kwargs: catalog)
    monkeypatch.setattr(api, "StreamingClient", lambda client: streaming)
    monkeypatch.setattr(api, "load_dotenv", lambda **kwargs: None)
    monkeypatch.setattr(api, "STREAM_CONSUMERS", {})
    return api


def test_full_service_startup_health_dashboard_and_openapi(forecast_api):
    with TestClient(forecast_api.app) as http:
        assert http.get("/health").json() == {"ok": True}
        for path in ("/", "/openapi.json", "/predictions", "/hotspots"):
            assert http.get(path).status_code == 200
        schema = http.get("/openapi.json").json()
        assert {"/run-model", "/status/{job_id}", "/streams/create", "/streams/{topic}/consume/start"} <= schema["paths"].keys()
        assert "ToolInvocationRequest" not in schema["components"]["schemas"]
        assert not any(path.startswith(("/exact/", "/saved-results/")) or path == "/tools/invoke" for path in schema["paths"])
        assert http.get("/exact/hourly").status_code == 404
        assert http.get("/saved-results/runs").status_code == 404
        assert http.post("/tools/invoke", json={"tool": "unused"}).status_code == 404


def test_http_resource_discovery_and_stream_creation(forecast_api, source):
    with TestClient(forecast_api.app) as http:
        found = http.get("/sensors/sensor-one/consumption-method")
        assert found.status_code == 200
        assert found.json()["consumption_methods"] == [{"id": source["id"], "name": "observations", "type": "kafka"}]
        response = http.post("/streams/create", params={"consumption_method_ids": source["id"], "filter_semantics": "value > 10"})
        assert response.status_code == 200, response.text
        assert response.json()["topic"] == response.json()["data_stream_id"]
        assert response.json()["consumption_method_ids"] == [source["id"]]


def test_http_validation_and_not_found(forecast_api, source):
    with TestClient(forecast_api.app) as http:
        assert http.get("/sensors/missing/consumption-method").status_code == 404
        assert http.post("/streams/create", params={"consumption_method_ids": " "}).status_code == 422
        assert http.post("/streams/create", params={"consumption_method_ids": "missing"}).status_code == 422
        assert http.post("/streams/create", params={"consumption_method_ids": source["id"], "filter_semantics": "unparseable natural language"}).status_code == 422


def test_http_existing_websocket_resource_returns_501(forecast_api, catalog):
    definition = {"type": "websocket", "name": "feed", "description": "Synoptic feed", "config": {"url": "wss://example.test/feed"}}
    catalog.register_general_dataset({"name": "ws-sensor", "resources": [
        {"id": "ws-source", "name": "feed", "format": "websocket", "description": json.dumps(definition)},
    ]})
    with TestClient(forecast_api.app) as http:
        response = http.post("/streams/create", params={"consumption_method_ids": "ws-source"})
        assert response.status_code == 501
        assert "websocket" in response.json()["detail"]


@pytest.mark.parametrize("kind", ["prediction", "hotspot"])
def test_api_csv_registration_uses_general_dataset_and_v2_resources(monkeypatch, forecast_api, catalog, kind):
    monkeypatch.setenv("TOKEN", "offline-token")
    monkeypatch.setenv("API_URL", "https://offline.local")
    monkeypatch.setenv("SERVER", "local")
    monkeypatch.setattr(forecast_api, "build_public_prediction_url", lambda filename: f"https://example.test/predictions/{filename}")
    monkeypatch.setattr(forecast_api, "build_public_hotspot_url", lambda filename: f"https://example.test/hotspots/{filename}")
    paths = [Path("2025-01-16_utah.csv")]
    if kind == "prediction":
        result = forecast_api.register_prediction_files_ndp(paths, "utah")
    else:
        result = forecast_api.register_hotspot_files_ndp(paths, threshold_field="pm2p5_ugm3", threshold=10, persistence_k=6)
    assert len(result["registered"]) == 1
    assert "error" not in result["registered"][0]
    dataset = catalog.search_datasets([result["registered"][0]["dataset_name"]])[0]
    assert len(dataset["resources"]) == 1
    assert dataset["resources"][0]["format"] == "csv"


@pytest.mark.parametrize("stop_mode", ["limit", "manual", "timed", "start_error"])
def test_consumer_worker_limit_stop_and_failure_cleanup(monkeypatch, forecast_api, stop_mode):
    state = {"status": "running", "message_count": 0, "stop_requested": stop_mode == "manual"}
    handles = []
    def prepare(client, topic, inbox):
        handle = prepare_consumer(client, topic, inbox)
        handle.set_retention(max_records=1)
        handle.stopped = False
        def start():
            if stop_mode == "start_error":
                raise RuntimeError("offline start failure")
            handle.ingest([{"payload": {"value": i}} for i in range(3)])
        def stop():
            handle.stopped = True
        handle.start, handle.stop = start, stop
        handles.append(handle)
        return handle
    monkeypatch.setattr(forecast_api, "prepare_consumer", prepare)
    forecast_api.STREAM_CONSUMERS["topic"] = state
    asyncio.run(forecast_api._consume_topic_worker("topic", server="local", mode="timed" if stop_mode == "timed" else "manual",
                                                  auto_close_minutes=0, poll_interval=0, max_messages=2))
    assert handles[0].stopped
    assert state["ended_at"]
    if stop_mode == "limit":
        assert state["message_count"] == 2
        assert state["stop_reason"] == "max_messages_reached"
        assert len(handles[0].dataframe()) == 1
        assert state["recent_messages"] == [{"value": 0}, {"value": 1}]
    elif stop_mode == "start_error":
        assert state["status"] == "failed"
        assert state["error"] == "offline start failure"
    else:
        assert state["message_count"] == 0
        assert state["stop_reason"] == ("manual_stop" if stop_mode == "manual" else "auto_close_timeout")


def test_http_uses_dataset_hints_when_uuid_endpoint_is_unavailable(monkeypatch, forecast_api, catalog, source):
    original_search = catalog.search_datasets
    def unavailable(*args, **kwargs):
        raise ValueError("Resource route unavailable")
    def search(terms, **kwargs):
        assert terms != [""], "Known datasets should avoid a full catalog scan"
        return original_search(terms, **kwargs)
    monkeypatch.setattr(catalog, "get_resource", unavailable)
    monkeypatch.setattr(catalog, "search_datasets", search)
    with TestClient(forecast_api.app) as http:
        response = http.post("/streams/create", params={"consumption_method_ids": source["id"], "dataset_names": "sensor-one"})
        assert response.status_code == 200, response.text


@pytest.mark.parametrize("mode", ["generated", "catalog_reuse", "failure"])
def test_prediction_job_without_saved_results_preserves_progress_results_and_cleanup(monkeypatch, forecast_api, tmp_path, mode):
    cams_dir = tmp_path / "data/raw/cams_raw"
    cams_dir.mkdir(parents=True)
    preexisting = cams_dir / "preexisting.nc"
    preexisting.write_text("keep")
    static = cams_dir / "aurora-0.4-air-pollution-static.pickle"
    static.write_text("keep static")
    job = SimpleNamespace(id="offline-job", meta={"request_key": "offline-request"}, save_meta=lambda: None)
    cleanup = []
    monkeypatch.setattr(forecast_api, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(forecast_api, "get_current_job", lambda **kwargs: job)
    monkeypatch.setattr(forecast_api, "_clear_request_mapping", lambda key, job_id: cleanup.append((key, job_id)))
    expected = {"predictions": ["prediction.csv"], "prediction_urls": ["https://example.test/prediction.csv"]}
    existing = {"result": expected, "dataset_name": "existing_predictions"} if mode == "catalog_reuse" else None
    monkeypatch.setattr(forecast_api, "_find_matching_prediction_dataset", lambda req: existing)
    def generate(**kwargs):
        assert mode != "catalog_reuse", "Catalog reuse should skip model execution"
        kwargs["on_progress"]({"step": "predict", "current_date": "2025-01-16"})
        (cams_dir / "download.nc").write_text("temporary")
        if mode == "failure":
            raise RuntimeError("offline model failure")
        return expected
    monkeypatch.setattr(forecast_api.WORKFLOW_SERVICE, "run_model_batch", generate)
    payload = {"region": {"name": "utah", "lat_min": 37, "lat_max": 42.1, "lon_min": 245.9, "lon_max": 251},
               "dates": {"start": "2025-01-16", "end": "2025-01-16"}, "overwrite": False}
    if mode == "failure":
        with pytest.raises(RuntimeError, match="offline model failure"):
            forecast_api.run_prediction_job(payload)
        assert job.meta["step"] == "failed"
        assert job.meta["error"] == "offline model failure"
    else:
        result = forecast_api.run_prediction_job(payload)
        assert result["predictions"] == expected["predictions"]
        assert result["result_source"] == mode
        assert "saved_run_id" not in result
        assert "daily_summary_path" not in result
        assert job.meta["step"] == "complete"
    assert preexisting.read_text() == "keep"
    assert static.exists()
    assert not (cams_dir / "download.nc").exists()
    assert cleanup == [("offline-request", "offline-job")]
