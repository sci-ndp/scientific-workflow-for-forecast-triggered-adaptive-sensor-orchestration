"""Execute changed notebook cells against real v2 with offline IO fixtures."""
import ast
import asyncio
import inspect
from pathlib import Path
from types import SimpleNamespace

import nbformat
import pandas as pd
import pytest

from scidx_streaming_v2.models.resources import ResourceRecord
from scidx_streaming_v2.streams.consumer import StreamHandle
from test_streaming_migration import legacy_message

REPO_ROOT = Path(__file__).resolve().parents[1]


def notebook():
    return nbformat.read(REPO_ROOT / "notebooks/aq.ipynb", as_version=4)


def run_cells(indices, namespace):
    cells = notebook().cells
    async def run():
        for index in indices:
            code = compile(cells[index].source, f"aq.ipynb cell {index}", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
            result = eval(code, namespace)
            if inspect.isawaitable(result):
                await result
    asyncio.run(run())


def test_notebook_schema_and_all_code_cells_compile():
    nb = notebook()
    nbformat.validate(nb)
    for index, cell in enumerate(nb.cells):
        if cell.cell_type == "code":
            compile(cell.source, f"cell {index}", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
            assert cell.outputs == []
            assert cell.execution_count is None


def test_notebook_v2_cells_execute_through_consumption(monkeypatch, streaming, catalog, source, capsys):
    import dotenv
    import ndp_ep

    monkeypatch.chdir(REPO_ROOT / "notebooks")
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: False)
    for name, value in {"API_URL": "https://offline.local", "TOKEN": catalog.token, "SERVER": "local",
                        "SERVICE_NAME": "forecast_service", "SERVICE_BASE_URL": "", "AQ_EXISTING_TOPIC": ""}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(ndp_ep, "APIClient", lambda **kwargs: catalog)
    messages = [{"stid": "QLN", "value": i} for i in range(5)]
    def start(handle, **kwargs):
        handle.set_retention(max_records=1)
        decoder = handle.consumer_factory.keywords["value_deserializer"]
        for message in messages:
            handle.ingest([decoder(legacy_message([message]))])
        return handle
    monkeypatch.setattr(StreamHandle, "start", start)
    namespace = {"sensor_df": pd.DataFrame({"sensor_name": ["sensor-one"]})}
    run_cells([3, 6, 33, 34, 36, 37, 39, 43], namespace)
    assert namespace["resource_selector"] == ("sensor-one", "observations")
    assert namespace["topic"] == namespace["stream"].topic
    assert namespace["consumer"].summary()["total_consumed"] == 5
    assert len(namespace["consumer"].dataframe()) == 1
    assert "messages_read: 5" in capsys.readouterr().out


def test_notebook_websocket_limitation_is_explicit_and_clears_stale_topic():
    resource = ResourceRecord(id="ws-source", dataset_id="sensor", dataset_name="sensor",
                              type="websocket", name="feed", description="WebSocket", state="active", metadata={}, raw={})
    namespace = {"selected_resource": resource, "topic": "stale-topic"}
    with pytest.raises(NotImplementedError, match="does not ingest WebSocket"):
        run_cells([39], namespace)
    assert namespace["topic"] is None


def test_prediction_url_loads_fresh_data_without_catalog_lookup(monkeypatch):
    import os

    def unavailable(*args, **kwargs):
        raise AssertionError("A workflow URL should not depend on a second catalog lookup")

    current = pd.DataFrame({"pm2p5_ugm3": [12.0]})
    monkeypatch.setattr(pd, "read_csv", lambda url: current)
    namespace = {
        "os": os, "pd": pd, "display": lambda value: None,
        "prediction_url": "https://example.test/new.csv",
        "prediction_df": pd.DataFrame({"pm2p5_ugm3": [1.0]}),
        "client": SimpleNamespace(search_datasets=unavailable), "SERVER": "local",
    }
    run_cells([22], namespace)
    assert namespace["prediction_df"] is current


def test_sensor_selection_loads_full_mapping_instead_of_preview(monkeypatch):
    response = {
        "map_url": "https://example.test/mapping.csv", "mapping_count": 25,
        "preview": [{"sensor_name": "preview-only", "distance_km": 3}],
    }
    full_mapping = pd.DataFrame({"sensor_name": ["full-result"] * 25, "distance_km": range(25)})
    monkeypatch.setattr(pd, "read_csv", lambda url: full_mapping)
    namespace = {
        "pd": pd, "display": lambda value: None, "hotspot_input": "hotspots.csv",
        "API_URL": "https://offline.test", "SERVICE_NAME": "forecast_service", "HEADERS": {},
        "requests": SimpleNamespace(post=lambda *args, **kwargs: SimpleNamespace(
            status_code=200, raise_for_status=lambda: None, json=lambda: response)),
    }
    run_cells([30], namespace)
    assert len(namespace["sensor_df"]) == response["mapping_count"]
    assert namespace["sensor_df"]["sensor_name"].unique().tolist() == ["full-result"]


def test_hotspots_load_full_public_csv_instead_of_preview(monkeypatch):
    response = {
        "hotspots_csv": "/app/data/processed/hotspots/output.csv",
        "hotspots_url": "https://example.test/hotspots.csv", "hotspot_count": 67,
        "preview": [{"lat": 40.0, "lon": -111.0}] * 10,
    }
    complete = pd.DataFrame({"lat": [40.0] * 67, "lon": [-111.0] * 67})
    monkeypatch.setattr(pd, "read_csv", lambda url: complete)
    namespace = {
        "pd": pd, "Path": Path, "display": lambda value: None,
        "prediction_url": "https://example.test/prediction.csv",
        "API_URL": "https://offline.test", "SERVICE_NAME": "forecast_service", "HEADERS": {},
        "requests": SimpleNamespace(post=lambda *args, **kwargs: SimpleNamespace(
            status_code=200, raise_for_status=lambda: None, json=lambda: response)),
    }
    run_cells([25, 27], namespace)
    assert namespace["hotspot_input"] == response["hotspots_url"]
    assert len(namespace["hotspot_df"]) == response["hotspot_count"]
