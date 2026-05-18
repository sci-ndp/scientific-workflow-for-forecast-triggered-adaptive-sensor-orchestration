"""EarthScope demo tools."""

import base64
import json as _json
import logging
import os
import socket
import threading
import time
import math
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence, Tuple
from html import escape as _escape

import requests

try:  # pragma: no cover - optional dependency
    import matplotlib

    matplotlib.use("Agg")  # type: ignore[attr-defined]
    import matplotlib.pyplot as plt  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - matplotlib not required
    plt = None  # type: ignore[assignment]

from io import BytesIO

try:  # pragma: no cover - optional import
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse
    import uvicorn
except Exception:  # pragma: no cover
    FastAPI = None  # type: ignore[assignment]
    JSONResponse = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]
    uvicorn = None  # type: ignore[assignment]

from beeai_framework.tools import AnyTool, tool as bee_tool

from scidx_streaming.agentic_mcp.core import MCPModuleConfig, MCPStreamState
from scidx_streaming.agentic_mcp.utils import ToolOutput, format_tool_output


logger = logging.getLogger(__name__)


_visualization_service: Optional["EarthScopeVisualizationService"] = None


_preview_service: Optional["EarthScopeVisualizationService"] = None


def _find_free_port(
    preferred_port: Optional[int] = None,
    *,
    fallback_port: int = 55555,
    require_preferred: bool = False,
) -> int:
    env_port = os.getenv("EARTHSCOPE_VIZ_PORT")
    candidates: List[int] = []

    def _append_candidate(value: Optional[object], *, label: str) -> None:
        if value is None:
            return
        try:
            port_value = int(value)
        except (TypeError, ValueError):
            logger.warning("Invalid %s=%s; ignoring.", label, value)
            return
        if port_value <= 0 or port_value in candidates:
            return
        candidates.append(port_value)

    if preferred_port is not None:
        _append_candidate(preferred_port, label="preferred visualization port")

    if not require_preferred:
        if env_port:
            _append_candidate(env_port, label="EARTHSCOPE_VIZ_PORT")

        _append_candidate(fallback_port, label="default visualization port")

    seen: set[int] = set()
    for port in candidates:
        if port in seen or port <= 0:
            continue
        seen.add(port)
        if _wait_for_port_release(port, timeout=5.0):
            logger.debug("Visualization port %s available, selecting.", port)
            return port
        logger.info("Visualization port %s still in use after grace period.", port)

    if require_preferred and preferred_port is not None:
        raise RuntimeError(f"Failed to bind visualization port; preferred port {preferred_port} unavailable")

    raise RuntimeError(f"Failed to bind visualization port; port {fallback_port} unavailable")


def _wait_for_port_release(port: int, timeout: float = 5.0, attempt_shutdown: bool = True) -> bool:
    if port <= 0:
        return False
    deadline = time.time() + max(timeout, 0.0)
    shutdown_attempted = False
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            if attempt_shutdown and not shutdown_attempted:
                try:
                    requests.post(
                        f"http://127.0.0.1:{port}/__shutdown__",
                        timeout=1.0,
                    )
                except Exception:
                    pass
                shutdown_attempted = True
            if time.time() >= deadline:
                return False
            time.sleep(0.1)


def _scalar(value: object) -> object:
    if isinstance(value, list) and value:
        return value[-1]
    return value

def _normalize_records(values: Dict[str, object]) -> List[Dict[str, object]]:
    if not isinstance(values, dict) or not values:
        return []
    list_lengths = [len(item) for item in values.values() if isinstance(item, list)]
    if not list_lengths:
        return [values]
    record_length = max(list_lengths)
    records: List[Dict[str, object]] = []
    for idx in range(record_length):
        entry: Dict[str, object] = {}
        for key, item in values.items():
            if isinstance(item, list):
                entry[key] = item[idx] if idx < len(item) else None
            else:
                entry[key] = item
        records.append(entry)
    return records



def _truncate_points(points, max_points=20):
    if not points:
        return []
    buckets = {}
    for entry in points:
        try:
            timestamp = float(entry.get("time"))
        except (TypeError, ValueError):
            continue
        buckets.setdefault(timestamp, []).append(entry)
    sorted_keys = sorted(buckets.keys())[-max_points:]
    result = []
    for key in sorted_keys:
        result.extend(buckets[key])
    return result[-max_points:]

class EarthScopeVisualizationService:
    """Background FastAPI server that serves the live EarthScope map."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        preferred_port: Optional[int] = None,
        fallback_port: int = 55555,
        require_preferred_port: bool = False,
        mode: str = "full",
    ) -> None:
        if FastAPI is None or uvicorn is None:
            raise RuntimeError(
                "FastAPI and uvicorn are required for visualization. Install them to enable this feature."
            )

        self.consumer = None
        self.station_positions: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()
        self.public_host = host
        self.mode = mode if mode in {"full", "preview"} else "full"
        self.port = _find_free_port(
            preferred_port,
            fallback_port=fallback_port,
            require_preferred=require_preferred_port,
        )
        self.app = FastAPI(title="EarthScope Visualization", docs_url=None, redoc_url=None)
        self._configure_routes()
        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.port, log_level="warning")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        logger.info("EarthScope visualization service binding on %s:%s", self.public_host, self.port)
        # Wait until the ASGI server reports itself ready.
        if hasattr(self._server, "started") and isinstance(self._server.started, threading.Event):
            self._server.started.wait(timeout=5)

    @property
    def url(self) -> str:
        return f"http://{self.public_host}:{self.port}"

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def update(self, consumer, station_positions: Dict[str, Dict[str, float]]) -> None:
        with self._lock:
            self.consumer = consumer
            self.station_positions = dict(station_positions)

    def stop(self, timeout: float = 5.0) -> None:
        with self._lock:
            server = self._server
            thread = self._thread
            self._server = None
            self._thread = None
        if server is None or thread is None:
            return
        try:
            server.should_exit = True
            if hasattr(server, "force_exit"):
                server.force_exit = True  # type: ignore[attr-defined]
        except Exception:
            pass
        if thread.is_alive():
            thread.join(timeout=timeout)
        logger.info("EarthScope visualization service on %s:%s stopped.", self.public_host, self.port)


    def _collect_snapshot(self) -> Dict[str, object]:
        with self._lock:
            consumer = self.consumer
            station_positions = dict(self.station_positions)

        points: List[Dict[str, object]] = []
        raw_records: List[Dict[str, object]] = []
        raw_messages_count = 0
        topic = None
        dataframe = None

        if consumer is not None:
            topic = getattr(consumer, "topic", None) or getattr(consumer, "data_stream_id", None)
            data_list = getattr(consumer, "data_list", None)
            if isinstance(data_list, list):
                raw_messages_count = len(data_list)
                for payload in data_list[-200:]:
                    if not isinstance(payload, dict):
                        continue
                    values = payload.get("values") if isinstance(payload.get("values"), dict) else payload
                    for record in _normalize_records(values):
                        raw_records.append(record)
                        station = record.get("station")
                        timestamp = record.get("time")
                        position = station_positions.get(station) if station else None
                        try:
                            timestamp_ms = float(timestamp)
                        except (TypeError, ValueError):
                            timestamp_ms = None
                        cutoff_ms = (time.time() * 1000) - 60_000
                        if timestamp_ms is not None and timestamp_ms < cutoff_ms:
                            continue
                        points.append(
                            {
                                "station": station,
                                "time": timestamp,
                                "x": record.get("x"),
                                "y": record.get("y"),
                                "z": record.get("z"),
                                "lat": position.get("lat") if position else None,
                                "lon": position.get("lon") if position else None,
                            }
                        )

            dataframe = getattr(consumer, "dataframe", None)
            if dataframe is not None and not getattr(dataframe, "empty", True):
                try:
                    sample = dataframe.tail(500).to_dict(orient="records")  # type: ignore[arg-type]
                except Exception:
                    sample = []
                cutoff_ms = (time.time() * 1000) - 60_000
                for row in sample:
                    if not isinstance(row, dict):
                        continue
                    raw_records.append(row)
                    station = str(_scalar(row.get("station"))) if row.get("station") is not None else None
                    timestamp = _scalar(row.get("time"))
                    try:
                        timestamp_ms = float(timestamp)
                    except (TypeError, ValueError):
                        timestamp_ms = None
                    if timestamp_ms is not None and timestamp_ms < cutoff_ms:
                        continue
                    position = station_positions.get(station) if station else None
                    points.append(
                        {
                            "station": station,
                            "time": timestamp,
                            "x": _scalar(row.get("x")),
                            "y": _scalar(row.get("y")),
                            "z": _scalar(row.get("z")),
                            "lat": position.get("lat") if position else None,
                            "lon": position.get("lon") if position else None,
                        }
                    )

        dataframe_rows = int(dataframe.shape[0]) if dataframe is not None else 0

        if not points and (raw_messages_count or dataframe_rows):
            logger.debug(
                "Visualization has %s raw messages and %s dataframe rows but no geo points. "
                "Check station labels against station_positions.",
                raw_messages_count,
                dataframe_rows,
            )

        station_coords: Dict[str, Dict[str, float]] = {}
        for entry in points:
            name = entry.get("station")
            lat = entry.get("lat")
            lon = entry.get("lon")
            if name and isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                station_coords.setdefault(name, {"lat": float(lat), "lon": float(lon)})

        for name, pos in station_positions.items():
            if not isinstance(pos, dict):
                continue
            lat = pos.get("lat")
            lon = pos.get("lon")
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                station_coords.setdefault(name, {"lat": float(lat), "lon": float(lon)})

        return {
            "points": points,
            "raw": raw_records,
            "raw_messages_count": raw_messages_count,
            "dataframe_rows": dataframe_rows,
            "topic": topic,
            "station_positions": station_positions,
            "station_coords": station_coords,
            "generated_at": time.time(),
        }


    def _collect_snapshot(self) -> Dict[str, object]:
        with self._lock:
            consumer = self.consumer
            station_positions = dict(self.station_positions)

        points: List[Dict[str, object]] = []
        raw_records: List[Dict[str, object]] = []
        raw_messages_count = 0
        topic = None
        dataframe = None

        if consumer is not None:
            topic = getattr(consumer, "topic", None) or getattr(consumer, "data_stream_id", None)
            data_list = getattr(consumer, "data_list", None)
            if isinstance(data_list, list):
                raw_messages_count = len(data_list)
                for payload in data_list[-200:]:
                    if not isinstance(payload, dict):
                        continue
                    values = payload.get("values") if isinstance(payload.get("values"), dict) else payload
                    for record in _normalize_records(values):
                        raw_records.append(record)
                        station = record.get("station")
                        timestamp = record.get("time")
                        position = station_positions.get(station) if station else None
                        try:
                            timestamp_ms = float(timestamp)
                        except (TypeError, ValueError):
                            timestamp_ms = None
                        cutoff_ms = (time.time() * 1000) - 60_000
                        if timestamp_ms is not None and timestamp_ms < cutoff_ms:
                            continue
                        points.append(
                            {
                                "station": station,
                                "time": timestamp,
                                "x": record.get("x"),
                                "y": record.get("y"),
                                "z": record.get("z"),
                                "lat": position.get("lat") if position else None,
                                "lon": position.get("lon") if position else None,
                            }
                        )

            dataframe = getattr(consumer, "dataframe", None)
            if dataframe is not None and not getattr(dataframe, "empty", True):
                try:
                    sample = dataframe.tail(500).to_dict(orient="records")  # type: ignore[arg-type]
                except Exception:
                    sample = []
                cutoff_ms = (time.time() * 1000) - 60_000
                for row in sample:
                    if not isinstance(row, dict):
                        continue
                    raw_records.append(row)
                    station = str(_scalar(row.get("station"))) if row.get("station") is not None else None
                    timestamp = _scalar(row.get("time"))
                    try:
                        timestamp_ms = float(timestamp)
                    except (TypeError, ValueError):
                        timestamp_ms = None
                    if timestamp_ms is not None and timestamp_ms < cutoff_ms:
                        continue
                    position = station_positions.get(station) if station else None
                    points.append(
                        {
                            "station": station,
                            "time": timestamp,
                            "x": _scalar(row.get("x")),
                            "y": _scalar(row.get("y")),
                            "z": _scalar(row.get("z")),
                            "lat": position.get("lat") if position else None,
                            "lon": position.get("lon") if position else None,
                        }
                    )

        dataframe_rows = int(dataframe.shape[0]) if dataframe is not None else 0

        if not points and (raw_messages_count or dataframe_rows):
            logger.debug(
                "Visualization has %s raw messages and %s dataframe rows but no geo points. "
                "Check station labels against station_positions.",
                raw_messages_count,
                dataframe_rows,
            )

        return {
            "points": points,
            "raw": raw_records,
            "raw_messages_count": raw_messages_count,
            "dataframe_rows": dataframe_rows,
            "topic": topic,
            "station_positions": station_positions,
            "generated_at": time.time(),
        }

    def _configure_routes(self) -> None:
        @self.app.get("/", response_class=HTMLResponse)
        def index():  # pragma: no cover - HTTP endpoint
            snapshot = self._collect_snapshot()
            if self.mode == "preview":
                return HTMLResponse(self._render_preview(snapshot))
            return HTMLResponse(self._render_index(snapshot))

        @self.app.post("/__shutdown__", response_class=JSONResponse)
        def shutdown():  # pragma: no cover - HTTP endpoint
            def _async_stop() -> None:
                try:
                    self.stop()
                except Exception:
                    logger.exception("Failed to stop visualization service on shutdown request")

            threading.Thread(target=_async_stop, daemon=True).start()
            return JSONResponse({"status": "shutting_down"})

        @self.app.get("/data", response_class=JSONResponse)
        def get_data():  # pragma: no cover - HTTP endpoint
            snapshot = self._collect_snapshot()
            return JSONResponse(snapshot)

    def _render_preview(self, snapshot: Dict[str, object]) -> str:
        station_positions = snapshot.get("station_positions") or {}
        stations: List[Dict[str, object]] = []
        for name, position in sorted(station_positions.items()):
            if not isinstance(position, dict):
                continue
            lat = position.get("lat")
            lon = position.get("lon")
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                continue
            stations.append({"name": str(name), "lat": float(lat), "lon": float(lon)})

        stations_json = _json.dumps(stations, separators=(",", ":"))
        stations_json_safe = stations_json.replace("</", "<\\/")
        generated_at = snapshot.get("generated_at")
        if generated_at is not None:
            try:
                generated_display = _escape(
                    datetime.fromtimestamp(float(generated_at)).strftime("%Y-%m-%d %H:%M:%S")
                )
            except (TypeError, ValueError):
                generated_display = _escape(str(generated_at))
        else:
            generated_display = "—"
        station_count = len(stations)

        return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>EarthScope Station Preview</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link
      rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
      crossorigin=""
    />
    <style>
      html, body {{
        margin: 0;
        padding: 0;
        height: 100%;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background: #e2e8f0;
        color: #0f172a;
      }}
      header {{
        background: #0f172a;
        color: #f8fafc;
        padding: 18px 22px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.35);
        display: flex;
        flex-direction: column;
        gap: 6px;
      }}
      header h1 {{
        font-size: 22px;
        margin: 0;
        font-weight: 600;
      }}
      header span {{
        font-size: 15px;
        color: #cbd5f5;
      }}
      main {{
        padding: 20px;
        height: calc(100% - 88px);
      }}
      #map {{
        width: 100%;
        height: 100%;
        min-height: 420px;
        border-radius: 16px;
        box-shadow: 0 16px 32px rgba(15, 23, 42, 0.18);
        background: #1e293b;
        color: #f8fafc;
        display: flex;
        align-items: center;
        justify-content: center;
      }}
    </style>
  </head>
  <body>
    <header>
      <h1>Station Preview</h1>
      <span>Stations plotted: {station_count} · Generated at: {generated_display}</span>
    </header>
    <main>
      <div id="map">Loading map…</div>
    </main>
    <script
      src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
      integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
      crossorigin=""
    ></script>
    <script type="application/json" id="stations-data">{stations_json_safe}</script>
    <script>
      window.addEventListener('load', function () {{
        var mapContainer = document.getElementById('map');
        if (typeof L === 'undefined') {{
          mapContainer.innerHTML = '<p>Map library failed to load.</p>';
          return;
        }}
        var dataNode = document.getElementById('stations-data');
        var stations = [];
        if (dataNode) {{
          try {{
            stations = JSON.parse(dataNode.textContent);
          }} catch (err) {{
            console.warn('Failed to parse stations payload', err);
          }}
        }}
        var map = L.map('map', {{ preferCanvas: true }}).setView([39, -98], 4);
        L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
          maxZoom: 18,
          attribution: '&copy; OpenStreetMap contributors'
        }}).addTo(map);
        var bounds = [];
        var overlap = {{}};
        stations.forEach(function (station) {{
          var lat = Number(station.lat);
          var lon = Number(station.lon);
          if (!isFinite(lat) || !isFinite(lon)) {{
            return;
          }}
          var key = lat.toFixed(5) + ',' + lon.toFixed(5);
          var count = overlap[key] || 0;
          overlap[key] = count + 1;
          var jitterLat = lat;
          var jitterLon = lon;
          if (count > 0) {{
            var offset = 0.0004 * count;
            jitterLat = lat + offset;
            jitterLon = lon + (offset * 0.6);
          }}
          var marker = L.marker([jitterLat, jitterLon]).addTo(map);
          marker.bindPopup(station.name);
          bounds.push([jitterLat, jitterLon]);
        }});
        if (bounds.length) {{
          map.fitBounds(bounds, {{ padding: [30, 30], maxZoom: 10 }});
        }} else {{
          map.setView([39, -98], 4);
        }}
        setTimeout(function () {{
          map.invalidateSize();
        }}, 200);
      }});
    </script>
  </body>
</html>
"""

    def _render_index(self, snapshot: Dict[str, object]) -> str:
        def fmt_number(value: object, digits: int = 3) -> str:
            try:
                num = float(value)
            except (TypeError, ValueError):
                return "—"
            if math.isfinite(num):
                return f"{num:.{digits}f}"
            return "—"

        def fmt_time(value: object) -> str:
            try:
                return datetime.fromtimestamp(float(value) / 1000.0).strftime("%H:%M:%S")
            except (TypeError, ValueError):
                if value is None:
                    return "—"
                return str(value)

        def sanitize(obj: object) -> object:
            if isinstance(obj, dict):
                return {str(k): sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [sanitize(v) for v in obj]
            if isinstance(obj, float):
                if math.isfinite(obj):
                    return obj
                return None
            if isinstance(obj, (int, str, bool)) or obj is None:
                return obj
            return str(obj)

        points_all = snapshot.get("points") or []
        station_positions = snapshot.get("station_positions") or {}
        station_coords = snapshot.get("station_coords") or {}
        raw_count = int(snapshot.get("raw_messages_count") or 0)
        row_count = int(snapshot.get("dataframe_rows") or 0)
        topic_value = snapshot.get("topic")
        topic_display = _escape(str(topic_value)) if topic_value else "—"
        generated_at = snapshot.get("generated_at")
        if generated_at is not None:
            try:
                generated_display = _escape(datetime.fromtimestamp(float(generated_at)).strftime("%Y-%m-%d %H:%M:%S"))
            except (TypeError, ValueError):
                generated_display = _escape(str(generated_at))
        else:
            generated_display = "—"
        if points_all:
            status_text = "Live data streaming"
        elif raw_count or row_count:
            status_text = "Data available (no mapped coordinates yet)"
        else:
            status_text = "Waiting for data…"
        status_display = _escape(status_text)
        raw_display = str(raw_count)
        row_display = str(row_count)
        station_count_display = str(len(station_coords) or len(station_positions))

        station_items: List[str] = []
        if station_positions:
            for name in sorted(station_positions):
                position = station_positions.get(name) or {}
                lat_str = fmt_number(position.get("lat"), 4)
                lon_str = fmt_number(position.get("lon"), 4)
                station_items.append(
                    "          <li><strong>{}</strong><span>lat: {}</span><span>lon: {}</span></li>".format(
                        _escape(str(name)), lat_str, lon_str
                    )
                )
        station_list_html = '\n'.join(station_items) if station_items else '          <li class="empty">No station metadata available.</li>'

        recent_points = list(points_all[-20:])
        recent_points.reverse()
        if not recent_points:
            table_rows_html = "            <tr><td colspan=\"7\">No samples yet.</td></tr>"
        else:
            row_fragments: List[str] = []
            for entry in recent_points:
                station_raw = entry.get("station")
                station_display = _escape(str(station_raw or "—"))
                time_text = _escape(fmt_time(entry.get("time")))
                lat_value = entry.get("lat")
                lon_value = entry.get("lon")
                if lat_value is None and station_raw in station_positions:
                    lat_value = station_positions[station_raw].get("lat")
                if lon_value is None and station_raw in station_positions:
                    lon_value = station_positions[station_raw].get("lon")
                row_fragments.append(
                    "            <tr><td>{station}</td><td>{time}</td><td>{x}</td><td>{y}</td><td>{z}</td><td>{lat}</td><td>{lon}</td></tr>".format(
                        station=station_display,
                        time=time_text,
                        x=fmt_number(entry.get("x")),
                        y=fmt_number(entry.get("y")),
                        z=fmt_number(entry.get("z")),
                        lat=fmt_number(lat_value, 4),
                        lon=fmt_number(lon_value, 4),
                    )
                )
            table_rows_html = '\n'.join(row_fragments)

        chart_points = (snapshot.get("points") or [])[-200:]
        chart_html = '<p class="empty">Matplotlib is unavailable; install matplotlib to enable plots.</p>'
        if plt is not None and chart_points:
            try:
                buckets: Dict[int, Dict[str, List[float]]] = {}
                for entry in chart_points:
                    try:
                        timestamp_ms = float(entry.get("time"))
                    except (TypeError, ValueError):
                        continue
                    bucket_second = int(timestamp_ms // 1000)
                    bucket = buckets.setdefault(bucket_second, {"x": [], "y": [], "z": []})
                    for axis in ("x", "y", "z"):
                        try:
                            bucket[axis].append(float(entry.get(axis)))
                        except (TypeError, ValueError):
                            continue

                if buckets:
                    sorted_seconds = sorted(buckets.keys())[-20:]
                    if sorted_seconds:
                        from matplotlib.dates import DateFormatter  # type: ignore

                        times = [datetime.fromtimestamp(sec) for sec in sorted_seconds]
                        axis_series: Dict[str, List[Optional[float]]] = {axis: [] for axis in ("x", "y", "z")}
                        for sec in sorted_seconds:
                            bucket = buckets[sec]
                            for axis in ("x", "y", "z"):
                                values = bucket.get(axis) or []
                                axis_series[axis].append(sum(values) / len(values) if values else None)

                        fig, ax = plt.subplots(figsize=(8, 3.6))
                        labels = {"x": "X", "y": "Y", "z": "Z"}
                        colors = {"x": "#1d4ed8", "y": "#059669", "z": "#dc2626"}
                        plotted = False
                        for axis, series_values in axis_series.items():
                            plot_times: List[datetime] = []
                            plot_values: List[float] = []
                            for moment, value in zip(times, series_values):
                                if value is None:
                                    continue
                                plot_times.append(moment)
                                plot_values.append(value)
                            if plot_times:
                                ax.plot(plot_times, plot_values, marker="o", label=labels[axis], color=colors.get(axis))
                                plotted = True

                        if plotted:
                            ax.set_xlabel("Time")
                            ax.set_ylabel("Average movement (mm)")
                            ax.grid(True, alpha=0.3, linestyle="--", linewidth=0.6)
                            ax.legend(
                                loc="upper left",
                                bbox_to_anchor=(0, 1.15),
                                fontsize=10,
                                frameon=False,
                                ncol=3,
                                handlelength=2.2,
                                columnspacing=1.0,
                            )
                            ax.set_facecolor("#f8fafc")
                            fig.patch.set_facecolor("#f8fafc")
                            ax.spines["top"].set_visible(False)
                            ax.spines["right"].set_visible(False)
                            ax.spines["left"].set_color("#94a3b8")
                            ax.spines["bottom"].set_color("#94a3b8")
                            ax.tick_params(axis="both", colors="#475569", labelsize=9)
                            ax.set_ylim(-0.5, 0.5)
                            ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
                            fig.autofmt_xdate()
                            fig.tight_layout()
                            buffer = BytesIO()
                            fig.savefig(buffer, format="png", bbox_inches="tight", dpi=160)
                            plt.close(fig)
                            chart_html = '<img src="data:image/png;base64,{}" alt="Average movement over time" />'.format(
                                base64.b64encode(buffer.getvalue()).decode("ascii")
                            )
                        else:
                            chart_html = '<p class="empty">Insufficient numeric data to plot.</p>'
                else:
                    chart_html = '<p class="empty">Insufficient numeric data to plot.</p>'
            except Exception:
                logger.exception("Failed to render matplotlib chart for EarthScope visualization")
                chart_html = '<p class="empty">Failed to render chart.</p>'
        else:
            chart_html = '<p class="empty">Matplotlib is unavailable; install matplotlib to enable plots.</p>'
        snapshot_json = _json.dumps(sanitize(snapshot), ensure_ascii=False, separators=(",", ":"))
        snapshot_json_escaped = snapshot_json.replace("</", "<\\/")

        template = """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="1" />
    <title>EarthScope Stream Viewer</title>
    <style>
      body {{
        margin: 0;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background: #e2e8f0;
        color: #0f172a;
      }}
      header {{
        background: #0f172a;
        color: #f8fafc;
        padding: 26px 32px;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.45);
      }}
      header h1 {{
        margin: 0;
        font-size: 30px;
        font-weight: 600;
      }}
      header p {{
        margin: 10px 0 0;
        font-size: 16px;
        color: #dbeafe;
      }}
      main {{
        max-width: 2000px;
        margin: 0 auto;
        padding: 32px;
        display: flex;
        flex-direction: column;
        gap: 20px;
      }}
      .chart-panel img {{
        width: 100%;
        max-width: 100%;
        border-radius: 12px;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.2);
      }}
      section {{
        background: #ffffff;
        border-radius: 16px;
        padding: 26px;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.1);
      }}
      section h2 {{
        margin: 0 0 12px;
        font-size: 20px;
        font-weight: 600;
        color: #0f172a;
      }}
      #meta-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 12px;
      }}
      .metric {{
        background: #f8fafc;
        border-radius: 12px;
        padding: 16px 18px;
        box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.2);
        display: flex;
        flex-direction: column;
        gap: 4px;
      }}
      .metric span.label {{
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #64748b;
      }}
      .metric span.value {{
        font-size: 24px;
        font-weight: 600;
        color: #0f172a;
      }}
      #station-list {{
        list-style: none;
        margin: 0;
        padding: 0;
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 14px;
      }}
      #station-list li {{
        background: #f8fafc;
        border-radius: 12px;
        padding: 14px 16px;
        box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.2);
        font-size: 15px;
        color: #0f172a;
      }}
      #station-list li span {{
        display: block;
        font-size: 13px;
        color: #475569;
      }}
      #station-list li.empty {{
        text-align: center;
        color: #64748b;
        font-style: italic;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 15px;
        background: #ffffff;
        border-radius: 12px;
        overflow: hidden;
      }}
      th {{
        text-align: left;
        padding: 12px 14px;
        background: #f1f5f9;
        color: #334155;
        text-transform: uppercase;
        font-size: 13px;
        letter-spacing: 0.08em;
      }}
      td {{
        padding: 12px 14px;
        color: #0f172a;
        border-bottom: 1px solid #e2e8f0;
      }}
      tbody tr:nth-child(even) {{
        background: #f8fafc;
      }}
      tbody tr:last-child td {{
        border-bottom: none;
      }}
      @media (max-width: 900px) {{
        main {{
          padding: 16px;
        }}
      }}
    </style>
  </head>
  <body>
    <header>
      <h1>EarthScope Stream Viewer</h1>
      <p class="status">{status_display}</p>
    </header>
    <main>
      <section class="stations-panel">
        <h2>Stations</h2>
        <ul id="station-list">
{station_list_html}
        </ul>
      </section>
      <section class="summary-panel">
        <h2>Stream Summary</h2>
        <div id="meta-grid">
          <div class="metric">
            <span class="label">Topic</span>
            <span class="value">{topic_display}</span>
          </div>
          <div class="metric">
            <span class="label">Kafka Messages</span>
            <span class="value">{raw_display}</span>
          </div>
          <div class="metric">
            <span class="label">Dataframe Rows</span>
            <span class="value">{row_display}</span>
          </div>
          <div class="metric">
            <span class="label">Stations</span>
            <span class="value">{station_count_display}</span>
          </div>
          <div class="metric">
            <span class="label">Last Update</span>
            <span class="value">{generated_display}</span>
          </div>
        </div>
      </section>
      <section class="samples-panel">
        <h2>Latest Samples</h2>
        <table>
          <thead>
            <tr>
              <th>Station</th>
              <th>Time</th>
              <th>X</th>
              <th>Y</th>
              <th>Z</th>
              <th>Latitude</th>
              <th>Longitude</th>
            </tr>
          </thead>
          <tbody>
{table_rows_html}
          </tbody>
        </table>
      </section>
      <section class="chart-panel">
        <h2>Moving Average of All Stations</h2>
        {chart_html}
      </section>
    </main>
  </body>
</html>
"""

        return template.format(
            status_display=status_display,
            topic_display=topic_display,
            raw_display=raw_display,
            row_display=row_display,
            station_count_display=station_count_display,
            generated_display=generated_display,
            station_list_html=station_list_html,
            table_rows_html=table_rows_html,
            chart_html=chart_html,
            snapshot_json=snapshot_json_escaped,
        )


def _ensure_visualization_service(
    consumer,
    station_positions: Dict[str, Dict[str, float]],
    *,
    host: str = "127.0.0.1",
    preferred_port: Optional[int] = None,
) -> str:
    global _visualization_service

    if FastAPI is None or uvicorn is None:
        raise RuntimeError(
            "The visualization feature requires 'fastapi' and 'uvicorn'. Install them to enable live maps."
        )

    service = _visualization_service
    if preferred_port is not None:
        target_port = preferred_port
    else:
        env_port = os.getenv("EARTHSCOPE_VIZ_PORT")
        try:
            target_port = int(env_port) if env_port else 55555
        except (TypeError, ValueError):
            target_port = 55555

    if service is not None:
        alive = service.is_alive()
        host_changed = service.public_host != host
        port_changed = preferred_port is not None and service.port != preferred_port
        mode_changed = getattr(service, "mode", "full") != "full"
        if not alive or host_changed or port_changed:
            try:
                service.stop(timeout=5.0)
            finally:
                _visualization_service = None
                service = None
            if not _wait_for_port_release(target_port, timeout=5.0):
                raise RuntimeError(
                    f"Visualization port {target_port} is still busy after stopping the previous server."
                )
        elif mode_changed:
            try:
                service.stop(timeout=5.0)
            finally:
                _visualization_service = None
                service = None
            if not _wait_for_port_release(target_port, timeout=5.0):
                raise RuntimeError(
                    f"Visualization port {target_port} is still busy after stopping the previous server."
                )
        else:
            service.update(consumer, station_positions)
            return service.url

    if not _wait_for_port_release(target_port, timeout=5.0):
        raise RuntimeError(f"Visualization port {target_port} is unavailable. Stop the existing service or choose another port.")

    _visualization_service = EarthScopeVisualizationService(
        host=host,
        preferred_port=preferred_port,
        fallback_port=target_port,
        mode="full",
    )
    _visualization_service.update(consumer, station_positions)
    return _visualization_service.url




def _ensure_preview_service(
    station_positions: Dict[str, Dict[str, float]],
    *,
    host: str = "127.0.0.1",
    preferred_port: Optional[int] = None,
) -> str:
    global _preview_service

    if FastAPI is None or uvicorn is None:
        raise RuntimeError(
            "The visualization feature requires 'fastapi' and 'uvicorn'. Install them to enable live maps."
        )

    env_port = os.getenv("EARTHSCOPE_VIZ_PREVIEW_PORT")
    target_port = preferred_port
    if target_port is None:
        try:
            target_port = int(env_port) if env_port else 55556
        except (TypeError, ValueError):
            target_port = 55556

    service = _preview_service
    if service is not None:
        alive = service.is_alive()
        host_changed = service.public_host != host
        port_changed = service.port != target_port
        mode_changed = getattr(service, "mode", "full") != "preview"
        if not alive or host_changed or port_changed or mode_changed:
            try:
                service.stop(timeout=5.0)
            finally:
                _preview_service = None
                service = None
            if not _wait_for_port_release(target_port, timeout=5.0):
                raise RuntimeError(
                    f"Preview visualization port {target_port} is still busy after stopping the previous server."
                )
        else:
            service.update(None, station_positions)
            return service.url

    if not _wait_for_port_release(target_port, timeout=5.0):
        raise RuntimeError(
            f"Preview visualization port {target_port} is unavailable. Stop the existing service or choose another port."
        )

    _preview_service = EarthScopeVisualizationService(
        host=host,
        preferred_port=target_port,
        fallback_port=target_port,
        require_preferred_port=True,
        mode="preview",
    )
    _preview_service.update(None, station_positions)
    return _preview_service.url


def register_earthscope_tools(
    module: "EarthScopeMCPModule",
    tool_supplier: Callable[[], List[str]],
) -> List[AnyTool]:
    """Register the EarthScope demo tools."""

    client = module.client
    state: MCPStreamState = module.state
    config: Optional[MCPModuleConfig] = getattr(module, "config", None)
    extras = config.extras if config else {}
    if "extras" in extras and isinstance(extras["extras"], dict):
        nested = extras.pop("extras")
        extras = {**extras, **nested}

    default_method_ids: List[str] = list(extras.get("consumption_method_ids") or [])
    default_semantics: List[str] = list(extras.get("filter_semantics") or [])
    default_username: Optional[str] = extras.get("username")
    default_password: Optional[str] = extras.get("password")
    default_server: str = extras.get("search_server") or "local"
    viz_host = str(extras.get("visualization_host") or os.getenv("EARTHSCOPE_VIZ_HOST") or "127.0.0.1")
    viz_port_raw = extras.get("visualization_port")
    viz_port: Optional[int]
    try:
        viz_port = int(viz_port_raw) if viz_port_raw is not None else None
    except (TypeError, ValueError):
        logger.warning("Invalid visualization_port=%s in EarthScope config; falling back to auto.", viz_port_raw)
        viz_port = None
    preview_host = str(extras.get("preview_visualization_host") or os.getenv("EARTHSCOPE_VIZ_PREVIEW_HOST") or "127.0.0.1")
    preview_port_raw = extras.get("preview_visualization_port")
    preview_port: Optional[int]
    try:
        preview_port = int(preview_port_raw) if preview_port_raw is not None else 55556
    except (TypeError, ValueError):
        logger.warning(
            "Invalid preview_visualization_port=%s in EarthScope config; using default 55556.",
            preview_port_raw,
        )
        preview_port = 55556

    tools: List[AnyTool] = []

    @bee_tool()
    async def start_earthscope_stream(
        filter_semantics: Optional[str] = None,
    ) -> str:
        """Start the pre-configured EarthScope stream using canned credentials."""

        effective_ids = list(default_method_ids)
        if not effective_ids:
            return format_tool_output(
                ["Status: error", "No resource IDs were provided in parameters or module config."],
                {
                    "status": "error",
                    "message": "Supply resource_ids or configure consumption_method_ids in the EarthScope MCP config.",
                },
            )

        semantics_value: Optional[str] = None
        if isinstance(filter_semantics, str) and filter_semantics.strip():
            semantics_value = filter_semantics.strip()
        elif isinstance(filter_semantics, list) and filter_semantics:
            semantics_value = str(filter_semantics[0]).strip()

        if semantics_value is None:
            latest = state.producer_metadata.get("earthscope_latest_filter")
            if isinstance(latest, dict):
                value = latest.get("semantics")
                if isinstance(value, str) and value.strip():
                    semantics_value = value.strip()

        if semantics_value is None and default_semantics:
            for item in default_semantics:
                text = str(item).strip()
                if text:
                    semantics_value = text
                    break

        if semantics_value is None:
            return format_tool_output(
                [
                    "Status: error",
                    "No filter semantics supplied. Run build_station_filter first.",
                ],
                {
                    "status": "error",
                    "message": "Call build_station_filter before starting the stream or pass filter_semantics explicitly.",
                },
            )

        credentials = {
            "username": default_username,
            "password": default_password,
        }
        resolved_server = default_server

        applied_semantics = [semantics_value]
        logger.info(
            "EarthScope start: creating Kafka stream ids=%s server=%s semantics=%s",
            effective_ids,
            resolved_server,
            applied_semantics,
        )

        try:
            producer = await client.create_kafka_stream(
                consumption_method_ids=effective_ids,
                filter_semantics=applied_semantics,
                username=credentials["username"],
                password=credentials["password"],
                server=resolved_server,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("EarthScope stream creation failed")
            return format_tool_output(
                ["Status: error", f"Reason: {exc}"],
                {"status": "error", "message": str(exc)},
            )

        if isinstance(producer, dict) and "error" in producer:
            return format_tool_output(
                ["Status: error", producer.get("error", "Unknown error")],
                {"status": "error", "message": producer.get("error")},
            )

        topic = getattr(producer, "data_stream_id", None)
        if not topic:
            return format_tool_output(
                ["Status: error", "Producer did not provide a topic identifier."],
                {"status": "error", "message": "Producer did not provide a topic identifier."},
            )

        state.record_producer(
            topic,
            producer,
            resource_ids=effective_ids,
            filter_semantics=applied_semantics,
            filter_details=[
                {
                    "type": "earthscope",
                    "description": "EarthScope demo filters",
                    "values": list(applied_semantics),
                }
            ],
        )
        state.last_created_topic = topic
        state.producer_metadata.setdefault("earthscope_latest_filter", {}).update(
            {
                "applied_semantics": applied_semantics,
                "topic": topic,
            }
        )

        summary = [
            "Status: success",
            f"Topic: {topic}",
            f"Resource IDs used: {', '.join(effective_ids)}",
            f"Filter semantics applied: {len(applied_semantics)}",
        ]
        payload = {
            "status": "success",
            "topic": topic,
            "resource_ids": effective_ids,
            "applied_semantics": applied_semantics,
            "tracked_producers": list(state.producers.keys()),
            "context_hint": "Use launch_earthscope_visualization to inspect the stream.",
        }
        return format_tool_output(summary, payload)

    tools.append(start_earthscope_stream)

    @bee_tool()
    def build_station_filter(
        area: str,
        limit: Optional[int] = None,
        server: Optional[str] = None,
    ) -> str:
        """Derive station-based filter semantics for a geographical area."""

        effective_server = (server or default_server or "local").strip()

        if not area or not area.strip():
            return format_tool_output(
                ["Status: error", "Area parameter is required."],
                {"status": "error", "message": "Provide a non-empty area string."},
            )

        query = f"{area.strip()}, United States"
        nominatim_url = "https://nominatim.openstreetmap.org/search"
        params = {"q": query, "format": "json"}
        headers = {
            "User-Agent": "scidx-streaming-demo/1.0 (+https://github.com/sci-ndp/streaming-py)",
        }
        station_limit = (
            limit if isinstance(limit, int) and limit > 0 else int(extras.get("station_search_limit", 50))
        )

        try:
            response = requests.get(nominatim_url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            results = response.json()
        except Exception as exc:  # noqa: BLE001
            logger.exception("Nominatim search failed for area=%s", area)
            return format_tool_output(
                ["Status: error", f"Failed to resolve area '{area}'."],
                {
                    "status": "error",
                    "message": str(exc),
                    "query": query,
                    "endpoint": nominatim_url,
                },
            )

        if not results:
            return format_tool_output(
                ["Status: warning", f"No geocoding results for '{area}'."],
                {
                    "status": "warning",
                    "message": "Geocoder returned no matches.",
                    "query": query,
                    "endpoint": nominatim_url,
                },
            )

        bounding = results[0].get("boundingbox")
        if not (isinstance(bounding, list) and len(bounding) == 4):
            return format_tool_output(
                ["Status: error", "Bounding box unavailable from geocoder."],
                {
                    "status": "error",
                    "message": "First geocoder result did not include a bounding box.",
                    "query": query,
                },
            )

        lat_min, lat_max = float(bounding[0]), float(bounding[1])
        lon_min, lon_max = float(bounding[2]), float(bounding[3])
        bounds = ((lat_max, lon_min), (lat_min, lon_max))

        try:
            stations, station_positions, total_candidates = _search_stations_within_bounds(
                client,
                bounds=bounds,
                limit=station_limit,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Station search failed for area=%s", area)
            return format_tool_output(
                ["Status: error", f"Failed to search stations for '{area}'."],
                {
                    "status": "error",
                    "message": str(exc),
                },
            )

        if not stations:
            return format_tool_output(
                ["Status: warning", "No stations discovered within bounding box."],
                {
                    "status": "warning",
                    "area": area,
                    "query": query,
                    "bounding_box": bounding,
                    "total_candidates": total_candidates,
                },
            )

        semantics = f"station IN [{', '.join(stations)}]"
        cache_entry = {
            "area": area,
            "stations": stations,
            "semantics": semantics,
            "bounding_box": bounding,
            "query": query,
            "station_positions": station_positions,
            "server": effective_server,
        }
        preview_url: Optional[str] = None
        try:
            preview_url = _ensure_preview_service(
                station_positions,
                host=preview_host,
                preferred_port=preview_port,
            )
        except Exception as exc:  # pragma: no cover - preview is best effort
            logger.exception("Failed to start preview visualization for area=%s", area)

        if preview_url:
            cache_entry["preview_url"] = preview_url
        state.producer_metadata.setdefault("earthscope_station_filters", {})[area] = cache_entry
        state.producer_metadata["earthscope_latest_filter"] = cache_entry
        state.producer_metadata["earthscope_station_positions"] = station_positions
        if preview_url:
            state.producer_metadata.setdefault("earthscope_preview", {})[area] = preview_url

        summary = [
            "Status: success",
            f"Stations discovered: {len(stations)}",
            "Filter semantics generated.",
            f"Server: {effective_server}",
        ]
        if preview_url:
            summary.append(f"Preview map: {preview_url}")
        payload = {
            "status": "success",
            "area": area,
            "query": query,
            "bounding_box": bounding,
            "stations": stations,
            "station_count": len(stations),
            "station_limit": station_limit,
            "total_candidates": total_candidates,
            "station_positions": station_positions,
            "filter_semantics": semantics,
            "server": effective_server,
        }
        if preview_url:
            payload["preview_url"] = preview_url
        return format_tool_output(summary, payload)

    tools.append(build_station_filter)

    @bee_tool()
    def consume_earthscope_stream(
        topic: Optional[str] = None,
        use_last_topic: bool = True,
    ) -> str:
        """Start a consumer for the active EarthScope topic."""

        selected_topic = (topic or "").strip() if topic else None
        if not selected_topic and use_last_topic:
            selected_topic = state.last_created_topic or state.last_consumed_topic
        if not selected_topic:
            return format_tool_output(
                ["Status: error", "No stream topic available. Start a stream first."],
                {
                    "status": "error",
                    "message": "Start the EarthScope stream before consuming data.",
                },
            )

        if selected_topic in state.consumers:
            return format_tool_output(
                ["Status: success", f"Consumer for '{selected_topic}' is already active."],
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
        state.producer_metadata.setdefault("earthscope_visualization", {})["topic"] = selected_topic
        summary = [
            "Status: success",
            f"Consumer started for '{selected_topic}'",
            f"Active consumers: {len(state.consumers)}",
        ]
        payload = {
            "status": "success",
            "topic": selected_topic,
            "tracked_consumers": list(state.consumers.keys()),
            "context_hint": "Use launch_earthscope_visualization to inspect the stream.",
        }
        return format_tool_output(summary, payload)

    tools.append(consume_earthscope_stream)

    @bee_tool()
    def launch_earthscope_visualization(
        topic: Optional[str] = None,
        *,
        use_last_topic: bool = True,
    ) -> str:
        """Render a live view of the latest EarthScope stream."""

        selected_topic = (topic or "").strip() if topic else None
        if not selected_topic and use_last_topic:
            selected_topic = state.last_created_topic or state.last_consumed_topic

        if not selected_topic:
            return format_tool_output(
                ["Status: error", "No topic provided and no recent stream available."],
                {"status": "error", "message": "Start a stream before launching the visualization."},
            )

        consumer = state.consumers.get(selected_topic)
        if consumer is None:
            host = getattr(client, "KAFKA_HOST", None)
            port = getattr(client, "KAFKA_PORT", None)
            consumer = client.consume_kafka_messages(topic=selected_topic, host=host, port=port)
            state.record_consumer_start(selected_topic, consumer)

        filter_entry = state.producer_metadata.get("earthscope_latest_filter") or {}
        station_positions = filter_entry.get("station_positions")
        if not isinstance(station_positions, dict) or not station_positions:
            return format_tool_output(
                ["Status: error", "Station positions unavailable."],
                {
                    "status": "error",
                    "message": "Run build_station_filter before launching the visualization so station coordinates are known.",
                },
            )

        try:
            url = _ensure_visualization_service(
                consumer,
                station_positions,
                host=viz_host,
                preferred_port=viz_port,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to start EarthScope visualization service")
            return format_tool_output(
                ["Status: error", "Failed to start visualization server."],
                {"status": "error", "message": str(exc)},
            )

        state.producer_metadata.setdefault("earthscope_visualization", {})["url"] = url
        summary = [
            "Status: success",
            f"Visualization available for '{selected_topic}'",
            f"Stations plotted: {len(station_positions)}",
        ]
        payload = {
            "status": "success",
            "topic": selected_topic,
            "url": url,
            "station_positions": station_positions,
        }
        return format_tool_output(summary, payload)

    tools.append(launch_earthscope_visualization)

    return tools


def _extract_stations(results: Sequence[Dict[str, object]], *, limit: int) -> List[str]:
    """Best-effort station extraction from dataset results."""

    stations: List[str] = []

    def _append_candidate(value: Optional[str]) -> None:
        if not value:
            return
        normalized = str(value).strip()
        if not normalized or normalized in stations:
            return
        stations.append(normalized)

    for dataset in results or []:
        resources = dataset.get("resources") if isinstance(dataset, dict) else None
        for resource in resources or []:
            if not isinstance(resource, dict):
                continue
            station_fields = [
                resource.get("station"),
                resource.get("station_id"),
                resource.get("station_code"),
            ]
            config = resource.get("config") if isinstance(resource.get("config"), dict) else {}
            station_fields.extend(
                [
                    config.get("station"),
                    config.get("station_id"),
                    config.get("station_code"),
                ]
            )
            metadata = resource.get("metadata") if isinstance(resource.get("metadata"), dict) else {}
            station_fields.append(metadata.get("station"))

            for value in station_fields:
                if isinstance(value, list):
                    for item in value:
                        _append_candidate(item)
                else:
                    _append_candidate(value)

            if len(stations) >= limit:
                break
        if len(stations) >= limit:
            break

    return stations[:limit]


def _search_stations_within_bounds(
    client,
    *,
    bounds: Tuple[Tuple[float, float], Tuple[float, float]],
    limit: int,
) -> Tuple[List[str], Dict[str, Dict[str, float]], int]:
    (lat_max, lon_min), (lat_min, lon_max) = bounds

    api_base: str = getattr(client, "base_url", "") or ""
    if not api_base.endswith("/"):
        api_base += "/"
    search_url = f"{api_base}search"

    try:
        response = requests.get(
            search_url,
            params={"terms": "GNSS", "server": "global"},
            headers={"accept": "application/json"},
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"GNSS station search failed: {exc}") from exc

    candidates: List[Tuple[str, float, float]] = []
    for entry in payload or []:
        if not isinstance(entry, dict):
            continue
        extras = entry.get("extras")
        if not isinstance(extras, dict):
            continue
        lat = extras.get("latitude")
        lon = extras.get("longitude")
        name = entry.get("title")
        if lat is None or lon is None or not name:
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            continue
        candidates.append((str(name), lat_f, lon_f))

    total_candidates = len(candidates)
    stations: List[str] = []
    positions: Dict[str, Dict[str, float]] = {}
    for name, lat_f, lon_f in candidates:
        if lat_min <= lat_f <= lat_max and lon_min <= lon_f <= lon_max:
            stations.append(name)
            positions.setdefault(name, {"lat": lat_f, "lon": lon_f})
            if len(stations) >= limit:
                break

    return stations, positions, total_candidates


def _render_stream_snapshot(topic: str, consumer, *, max_rows: int) -> str:
    """Render a snapshot of the consumer data, optionally opening a Matplotlib window."""

    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.debug("Matplotlib not available: %s", exc)
        dataframe = getattr(consumer, "dataframe", None)
        if dataframe is None or dataframe.empty:
            return format_tool_output(
                ["Status: warning", "Consumer has no data yet and matplotlib is unavailable."],
                {
                    "status": "warning",
                    "message": "Install matplotlib for interactive visualization or wait for data.",
                    "topic": topic,
                },
            )
        preview = dataframe.head(max_rows).to_dict(orient="records")  # type: ignore[arg-type]
        return format_tool_output(
            ["Status: success", "Returned raw sample because matplotlib is unavailable."],
            {
                "status": "success",
                "topic": topic,
                "sample": preview,
                "note": "Install matplotlib to enable the interactive plot.",
            },
        )

    dataframe = getattr(consumer, "dataframe", None)
    if dataframe is None or dataframe.empty:
        return format_tool_output(
            ["Status: warning", "Consumer has not received data yet."],
            {"status": "warning", "message": "No data available to plot.", "topic": topic},
        )

    sample = dataframe.head(max_rows)

    numeric_columns = sample.select_dtypes(include=["number"]).columns.tolist()
    if not numeric_columns:
        preview = sample.to_dict(orient="records")  # type: ignore[arg-type]
        return format_tool_output(
            ["Status: warning", "No numeric columns to plot; returning raw sample."],
            {
                "status": "warning",
                "topic": topic,
                "sample": preview,
            },
        )

    x_axis = sample.index.to_list()
    y_axis = sample[numeric_columns[0]].to_list()

    plt.figure()
    plt.plot(x_axis, y_axis, marker="o")
    plt.title(f"EarthScope Stream: {topic}")
    plt.xlabel("Event Index")
    plt.ylabel(numeric_columns[0])
    plt.tight_layout()

    try:
        plt.show(block=False)
    except Exception as exc:  # pragma: no cover - headless environments
        logger.debug("Matplotlib show() failed: %s", exc)
        preview = sample.to_dict(orient="records")  # type: ignore[arg-type]
        return format_tool_output(
            ["Status: warning", "Unable to open visualization window; returning sample data."],
            {
                "status": "warning",
                "topic": topic,
                "sample": preview,
                "error": str(exc),
            },
        )

    preview_records = sample.to_dict(orient="records")  # type: ignore[arg-type]
    return format_tool_output(
        [
            "Status: success",
            f"Visualization opened for topic '{topic}'.",
            f"Plotted column: {numeric_columns[0]}",
        ],
        {
            "status": "success",
            "topic": topic,
            "plotted_column": numeric_columns[0],
            "plotted_points": len(y_axis),
            "sample": preview_records,
            "context_hint": "The Matplotlib window updates when new data arrives.",
        },
    )
