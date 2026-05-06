from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.orchestration_model import (
    AuroraIngestor,
    Event,
    ForecastTriggeredWorkflow,
    HaversineSelector,
    NDPCatalogDiscovery,
    NumericFieldPersistenceDetector,
    PredictionArtifact,
    SciDxStreamOrchestrator,
    SensorMapping,
    WorkflowMode,
)


class AirQualityWorkflowService:
    """
    Compatibility layer between the current air-quality API/UI and the
    generalized forecast-triggered workflow.
    """

    def __init__(
        self,
        *,
        project_root: Path,
        predictions_dir: Path,
        hotspots_dir: Path,
        map_dir: Path,
        build_public_prediction_url: Callable[[str], str],
        register_prediction_files_ndp: Callable[..., Dict[str, Any]],
        register_hotspot_files_ndp: Callable[..., Dict[str, Any]],
        create_clients: Callable[[Optional[str]], tuple[str, Any, Any]],
        logger: Any,
    ):
        self.project_root = Path(project_root)
        self.predictions_dir = Path(predictions_dir)
        self.hotspots_dir = Path(hotspots_dir)
        self.map_dir = Path(map_dir)
        self.build_public_prediction_url = build_public_prediction_url
        self.register_prediction_files_ndp = register_prediction_files_ndp
        self.register_hotspot_files_ndp = register_hotspot_files_ndp
        self.create_clients = create_clients
        self.logger = logger
        self.workflow = ForecastTriggeredWorkflow(
            ingestor=AuroraIngestor(),
            detector=NumericFieldPersistenceDetector(),
            discovery=NDPCatalogDiscovery(),
            selector=HaversineSelector(),
            orchestrator=SciDxStreamOrchestrator(),
        )
        # Backward-compatible attribute while callers migrate.
        self.pipeline = self.workflow

    @staticmethod
    def build_date_list(start: str, end: str) -> List[str]:
        start_ts = pd.to_datetime(start)
        end_ts = pd.to_datetime(end)
        if end_ts < start_ts:
            raise ValueError("dates.end must be >= dates.start")
        return [
            ts.strftime("%Y-%m-%d")
            for ts in pd.date_range(start_ts, end_ts, freq="D")
        ]

    @staticmethod
    def _prediction_artifact_from_path(pred_path: Path) -> PredictionArtifact:
        stem = pred_path.stem
        match = re.match(r"(?P<date>\d{4}-\d{2}-\d{2})_.+_(?P<region>[^_]+)$", stem)
        date = match.group("date") if match else ""
        region = match.group("region") if match else ""
        return PredictionArtifact(path=str(pred_path), region=region, date=date)

    @staticmethod
    def _normalize_threshold_runs(detection_request: Dict[str, Any]) -> List[Dict[str, Any]]:
        threshold_runs: List[Dict[str, Any]] = []
        raw_rules = detection_request.get("field_thresholds") or []
        for entry in raw_rules:
            if not isinstance(entry, dict):
                continue
            field_name = str(entry.get("field", "")).strip()
            if not field_name:
                continue
            threshold_runs.append(
                {
                    "field": field_name,
                    "threshold": float(entry.get("threshold")),
                }
            )

        if not threshold_runs:
            threshold_runs.append(
                {
                    "field": str(
                        detection_request.get("threshold_field", "pm2p5_ugm3")
                    ).strip(),
                    "threshold": float(detection_request.get("threshold", 10.0)),
                }
            )

        return threshold_runs

    @staticmethod
    def _events_to_hotspots_dataframe(events: List[Event]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for event in events:
            threshold_field = str(
                event.config.get("threshold_field") or event.variable or ""
            )
            row = {
                "timestamp": event.timestamp,
                "lat": float(event.lat),
                "lon": float(event.lon),
                "value": float(event.value),
                "variable": threshold_field,
                "threshold_field": threshold_field,
                "threshold_value": event.config.get("threshold"),
            }
            if threshold_field == "pm2p5_ugm3":
                row["pm25"] = float(event.value)
            rows.append(row)

        if not rows:
            return pd.DataFrame(
                columns=[
                    "timestamp",
                    "lat",
                    "lon",
                    "value",
                    "variable",
                    "threshold_field",
                    "threshold_value",
                    "pm25",
                ]
            )

        return pd.DataFrame(rows)

    @staticmethod
    def _hotspots_dataframe_to_events(hotspots_df: pd.DataFrame) -> List[Event]:
        events: List[Event] = []
        for row in hotspots_df.to_dict(orient="records"):
            variable = str(
                row.get("threshold_field")
                or row.get("variable")
                or ("pm2p5_ugm3" if pd.notna(row.get("pm25")) else "value")
            ).strip()

            raw_value = row.get("value")
            if raw_value is None or pd.isna(raw_value):
                raw_value = row.get("event_value")
            if raw_value is None or pd.isna(raw_value):
                raw_value = row.get("pm25")

            value = float(raw_value) if raw_value is not None and not pd.isna(raw_value) else 0.0
            events.append(
                Event(
                    timestamp=str(row.get("timestamp") or ""),
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                    value=value,
                    variable=variable,
                    config={
                        "threshold": row.get("threshold_value"),
                        "threshold_field": variable,
                    },
                )
            )
        return events

    @staticmethod
    def _mapping_rows_to_dataframe(mappings: List[SensorMapping]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for mapping in mappings:
            row = {
                "timestamp": mapping.timestamp,
                "hotspot_lat": float(mapping.hotspot_lat),
                "hotspot_lon": float(mapping.hotspot_lon),
                "station_id": mapping.sensor_id,
                "sensor_name": mapping.sensor_name,
                "sensor_lat": float(mapping.sensor_lat),
                "sensor_lon": float(mapping.sensor_lon),
                "distance_km": round(float(mapping.distance_km), 3),
                "threshold_field": mapping.variable,
                "threshold_value": mapping.threshold_value,
                "event_value": mapping.value,
            }
            if mapping.variable == "pm2p5_ugm3" and mapping.value is not None:
                row["pm25"] = float(mapping.value)
            rows.append(row)

        if not rows:
            return pd.DataFrame(
                columns=[
                    "timestamp",
                    "hotspot_lat",
                    "hotspot_lon",
                    "station_id",
                    "sensor_name",
                    "sensor_lat",
                    "sensor_lon",
                    "distance_km",
                    "threshold_field",
                    "threshold_value",
                    "event_value",
                    "pm25",
                ]
            )

        return pd.DataFrame(rows)

    def run_model_batch(
        self,
        *,
        region: Dict[str, Any],
        dates: Dict[str, Any],
        aurora: Dict[str, Any],
        overwrite: bool,
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        date_list = self.build_date_list(str(dates["start"]), str(dates["end"]))
        cams_dir = (self.project_root / Path("data/raw/cams_raw")).resolve()
        cams_dir.mkdir(parents=True, exist_ok=True)

        if on_progress:
            on_progress({"step": "predict", "dates": date_list, "current_date": None})

        source_config = {
            "times": list(aurora.get("times") or ["00:00", "12:00"]),
            "lead_hours": int(aurora.get("lead_hours", 12)),
            "download_dir": str(cams_dir),
            "overwrite": bool(overwrite),
        }

        outputs: List[Path] = []
        for date in date_list:
            if on_progress:
                on_progress({"step": "predict", "current_date": date})
            result = self.workflow.run(
                mode=WorkflowMode.PREDICT,
                source_config=source_config,
                region=region,
                date_range={"start": date, "end": date},
            )
            if result.get("status") != "done":
                continue
            artifact = result["artifact"]
            outputs.append(Path(artifact.path))

        if on_progress:
            on_progress({"step": "register_ndp"})
        ndp_result = self.register_prediction_files_ndp(
            outputs,
            region=str(region.get("name", "")),
            date_range={"start": str(dates["start"]), "end": str(dates["end"])},
            region_bounds={
                "lat_min": float(region["lat_min"]),
                "lat_max": float(region["lat_max"]),
                "lon_min": float(region["lon_min"]),
                "lon_max": float(region["lon_max"]),
            },
        )

        return {
            "predictions": [str(path) for path in outputs],
            "prediction_urls": [
                self.build_public_prediction_url(path.name) for path in outputs
            ],
            "predictions_dir": str(self.predictions_dir),
            "ndp": ndp_result,
            "dates": date_list,
        }

    def detect_hotspots(
        self,
        *,
        pred_path: Path,
        detection_request: Dict[str, Any],
    ) -> Dict[str, Any]:
        threshold_runs = self._normalize_threshold_runs(detection_request)
        artifact = self._prediction_artifact_from_path(pred_path)
        detect_result = self.workflow.run(
            mode=WorkflowMode.DETECT,
            artifact=artifact,
            detection_config={
                "field_thresholds": threshold_runs,
                "persistence_k": int(detection_request.get("persistence_k", 12)),
                "timestep_hours": int(detection_request.get("timestep_hours", 1)),
                "round_decimals": int(detection_request.get("round_decimals", 2)),
            },
        )

        hotspots_df = self._events_to_hotspots_dataframe(
            detect_result.get("event_details", [])
        )
        if not hotspots_df.empty:
            hotspots_df = hotspots_df.sort_values("timestamp").drop_duplicates(
                subset=["timestamp", "lat", "lon", "threshold_field"],
                keep="first",
            )

        top_k_hotspots = detection_request.get("top_k_hotspots")
        if top_k_hotspots is not None and int(top_k_hotspots) > 0:
            hotspots_df = hotspots_df.head(int(top_k_hotspots))

        multi_suffix = f"_f{len(threshold_runs)}" if len(threshold_runs) > 1 else ""
        hotspot_out = self.hotspots_dir / (
            f"{pred_path.stem}_k{int(detection_request.get('persistence_k', 12))}"
            f"{multi_suffix}_hotspots.csv"
        )
        hotspots_df.to_csv(hotspot_out, index=False)

        primary_field = threshold_runs[0]["field"]
        primary_threshold = float(threshold_runs[0]["threshold"])
        ndp_field = primary_field if len(threshold_runs) == 1 else "multi_field"
        ndp_result = self.register_hotspot_files_ndp(
            [hotspot_out],
            threshold_field=ndp_field,
            threshold=primary_threshold,
            persistence_k=int(detection_request.get("persistence_k", 12)),
        )

        return {
            "prediction_file": str(pred_path),
            "threshold_field": primary_field,
            "applied_thresholds": threshold_runs,
            "hotspots_csv": str(hotspot_out),
            "hotspot_count": int(len(hotspots_df)),
            "ndp": ndp_result,
            "preview": hotspots_df.head(10).to_dict(orient="records"),
        }

    def select_sensors(
        self,
        *,
        hotspot_path: Path,
        selection_request: Dict[str, Any],
    ) -> Dict[str, Any]:
        hotspots_df = pd.read_csv(hotspot_path)
        required_cols = {"lat", "lon"}
        missing = required_cols - set(hotspots_df.columns)
        if missing:
            raise ValueError(
                f"hotspots CSV missing required columns: {sorted(missing)}"
            )

        events = self._hotspots_dataframe_to_events(hotspots_df)
        server, client, _ = self.create_clients(selection_request.get("server"))
        discover_result = self.workflow.run(
            mode=WorkflowMode.DISCOVER,
            events=events,
            discovery_policy={
                "terms": ["sensors_2365", "sensor"],
                "keys": ["organization", "extras_dataset_kind"],
                "server": server,
            },
            catalog_client=client,
        )
        candidates = discover_result.get("candidate_details", [])
        select_result = self.workflow.run(
            mode=WorkflowMode.SELECT,
            candidates=candidates,
            events=events,
            selection_strategy={
                "selection_mode": selection_request.get("selection_mode", "nearest_k"),
                "k": int(selection_request.get("k", 1)),
                "radius_km": float(selection_request.get("radius_km", 10.0)),
            },
        )

        map_df = self._mapping_rows_to_dataframe(select_result.get("mapping_rows", []))
        map_out = self.map_dir / f"{hotspot_path.stem}_sensor_map.csv"
        map_df.to_csv(map_out, index=False)

        return {
            "hotspots_file": str(hotspot_path),
            "sensors_found": int(len(candidates)),
            "map_csv": str(map_out),
            "mapping_count": int(len(map_df)),
            "preview": map_df.to_dict(orient="records"),
        }
