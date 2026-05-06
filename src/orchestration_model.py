"""
orchestration_model.py
======================
Abstract reference architecture for forecast-triggered data workflow
orchestration with catalog-based adaptive sensor monitoring.

Defines five abstract workflow stages and typed data-flow contracts,
then provides concrete implementations for the air-quality domain
(Aurora/CAMS forecast, numeric-field persistence detection, NDP catalog
discovery, Haversine-based sensor selection, SciDx/Kafka streaming).

The abstract model is domain-agnostic: each stage specifies input/output
types, metadata contracts, and failure semantics so that the same
orchestration pattern can be instantiated for seismic, wildfire, or
other forecast-to-sensing workflows.
"""

from __future__ import annotations

import math
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


# ======================================================================
# 1. Typed data-flow contracts (lifecycle stage outputs)
# ======================================================================

class LifecycleStage(Enum):
    """Six-stage data lifecycle model."""
    S1_RAW_FORECAST = "raw_forecast"
    S2_PREDICTION_ARTIFACT = "prediction_artifact"
    S3_EVENT_SET = "event_set"
    S4_CANDIDATE_SENSORS = "candidate_sensors"
    S5_ACTIVE_STREAMS = "active_streams"
    S6_DELIVERED_OBSERVATIONS = "delivered_observations"


class WorkflowMode(Enum):
    """Supported execution modes for staged workflow use."""
    PREDICT = "predict"
    DETECT = "detect"
    DISCOVER = "discover"
    SELECT = "select"
    ACTIVATE = "activate"
    FULL = "full"


# Backward-compatible alias for older imports.
PipelineMode = WorkflowMode


@dataclass
class PredictionArtifact:
    """Stage S2 output — a regionally-subsetted, multi-variable forecast."""
    path: str
    region: str
    date: str
    n_timesteps: int = 0
    n_grid_cells: int = 0
    variables: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Event:
    """Stage S3 output — a single actionable event detected from forecasts."""
    timestamp: str
    lat: float
    lon: float
    value: float
    persistence_k: int = 1
    variable: str = ""
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SensorCandidate:
    """Stage S4 output — a candidate sensor discovered from a catalog."""
    sensor_id: str
    name: str
    lat: float
    lon: float
    distance_km: float = 0.0
    ingestion_mode: str = "unknown"
    resource_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ActiveStream:
    """Stage S5 output — an activated data stream for a selected sensor."""
    sensor_id: str
    stream_id: str
    topic: str
    ingestion_mode: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SensorMapping:
    """A per-event mapping row linking a detected event to a sensor."""
    sensor_id: str
    sensor_name: str
    sensor_lat: float
    sensor_lon: float
    distance_km: float
    hotspot_lat: float
    hotspot_lon: float
    timestamp: Optional[str] = None
    variable: Optional[str] = None
    value: Optional[float] = None
    threshold_value: Optional[float] = None


# ======================================================================
# 2. Abstract workflow stage interfaces
# ======================================================================

class ForecastIngestor(ABC):
    """
    Stage 1 → 2: Acquire raw forecast data and produce a
    regionally-subsetted PredictionArtifact.

    Failure semantics: raises RuntimeError if source is unreachable
    or data is corrupt.  Returns None if date has no data.
    """

    @abstractmethod
    def ingest(
        self,
        source_config: dict,
        region: dict,
        date_range: dict,
    ) -> Optional[PredictionArtifact]:
        ...


class EventDetector(ABC):
    """
    Stage 2 → 3: Detect actionable events from a PredictionArtifact.

    Failure semantics: returns empty list if artifact is empty or
    below detection thresholds.  Raises ValueError on schema mismatch.
    """

    @abstractmethod
    def detect(
        self,
        artifact: PredictionArtifact,
        detection_config: dict,
    ) -> List[Event]:
        ...


class ResourceDiscovery(ABC):
    """
    Stage 3 → 4: Discover candidate sensors from a metadata catalog
    based on detected events and a discovery policy.

    Failure semantics: returns empty list if catalog is unreachable
    or no sensors match.
    """

    @abstractmethod
    def discover(
        self,
        events: List[Event],
        catalog_client: Any,
        policy: dict,
    ) -> List[SensorCandidate]:
        ...


class AdaptiveSelector(ABC):
    """
    Stage 4 → 5: Select a minimal active sensor set from candidates.

    Failure semantics: returns empty list if no candidates are
    provided.  The strategy dict controls selection mode
    (nearest-k, radius, etc.).
    """

    @abstractmethod
    def select(
        self,
        candidates: List[SensorCandidate],
        events: List[Event],
        strategy: dict,
    ) -> List[SensorCandidate]:
        ...

    def build_mappings(
        self,
        candidates: List[SensorCandidate],
        events: List[Event],
        strategy: dict,
    ) -> List[SensorMapping]:
        return []


class StreamOrchestrator(ABC):
    """
    Stage 5 → 6: Activate streams for the selected sensors and
    disseminate observations to downstream consumers.

    Failure semantics: returns list of successfully activated streams.
    Partial failures are logged but do not halt the workflow.
    """

    @abstractmethod
    def observe(
        self,
        active_set: List[SensorCandidate],
        streaming_client: Any,
    ) -> List[ActiveStream]:
        ...


# ======================================================================
# 3. Concrete air-quality implementations
# ======================================================================

class AuroraIngestor(ForecastIngestor):
    """
    Wraps download_cams.py + aurora_runner.py for CAMS/Aurora
    forecast ingestion and regional prediction generation.
    """

    def ingest(
        self,
        source_config: dict,
        region: dict,
        date_range: dict,
    ) -> Optional[PredictionArtifact]:
        from src.download_cams import download_cams_forecast
        from src.aurora_runner import run_aurora_forecast

        date = date_range["start"]
        times = source_config.get("times", ["00:00", "12:00"])
        lead_hours = int(source_config.get("lead_hours", 12))
        download_dir = source_config.get("download_dir", "data/raw/cams_raw")

        # Download CAMS data
        cams = download_cams_forecast(
            date=date,
            lead_hours=lead_hours,
            times=times,
            download_dir=download_dir,
            overwrite=source_config.get("overwrite", False),
        )

        # Build output path
        time_tag = "-".join(t.replace(":", "") for t in times)
        region_name = region.get("name", "region")
        out_csv = Path(
            f"data/processed/predictions/"
            f"{date}_{time_tag}_{lead_hours}h_{region_name}.csv"
        )

        bbox = {
            "lat_min": region["lat_min"],
            "lat_max": region["lat_max"],
            "lon_min": region["lon_min"],
            "lon_max": region["lon_max"],
        }

        run_aurora_forecast(
            surf_path=Path(cams["surface_nc"]),
            atmo_path=Path(cams["atmos_nc"]),
            static_path=Path(cams["static_path"]),
            out_csv=out_csv,
            bbox=bbox,
        )

        return PredictionArtifact(
            path=str(out_csv),
            region=region_name,
            date=date,
            metadata={"source": "aurora+cams", "lead_hours": lead_hours},
        )


class NumericFieldPersistenceDetector(EventDetector):
    """
    Wraps event_detection.py — persistence-based threshold detection
    over one or more numeric forecast fields.
    """

    @staticmethod
    def _normalize_rules(detection_config: dict) -> List[Dict[str, Any]]:
        rules: List[Dict[str, Any]] = []
        raw_rules = detection_config.get("field_thresholds") or []

        for entry in raw_rules:
            if not isinstance(entry, dict):
                continue
            field_name = str(entry.get("field", "")).strip()
            if not field_name:
                continue
            rules.append({
                "field": field_name,
                "threshold": float(entry.get("threshold")),
            })

        if not rules:
            field_name = str(
                detection_config.get("threshold_field", "pm2p5_ugm3")
            ).strip()
            rules.append({
                "field": field_name,
                "threshold": float(detection_config.get("threshold", 10.0)),
            })

        return rules

    def detect(
        self,
        artifact: PredictionArtifact,
        detection_config: dict,
    ) -> List[Event]:
        from src.event_detection import detect_hotspots_for_day

        persistence_k = int(detection_config.get("persistence_k", 3))
        timestep_hours = int(detection_config.get("timestep_hours", 1))
        round_decimals = int(detection_config.get("round_decimals", 2))

        events: List[Event] = []
        for rule in self._normalize_rules(detection_config):
            field_name = rule["field"]
            threshold_value = float(rule["threshold"])
            events_raw = detect_hotspots_for_day(
                pred_csv=Path(artifact.path),
                pm25_col=field_name,
                threshold=threshold_value,
                persistence_k=persistence_k,
                timestep_hours=timestep_hours,
                round_decimals=round_decimals,
            )

            for detected in events_raw:
                events.append(
                    Event(
                        timestamp=str(detected.timestamp),
                        lat=detected.lat,
                        lon=detected.lon,
                        value=detected.value,
                        persistence_k=persistence_k,
                        variable=field_name,
                        config={
                            "threshold": threshold_value,
                            "threshold_field": field_name,
                        },
                    )
                )

        events.sort(key=lambda event: (event.timestamp, event.variable, event.lat, event.lon))
        return events


# Backward-compatible alias while the paper/code still refers to the old name.
PM25PersistenceDetector = NumericFieldPersistenceDetector


class NDPCatalogDiscovery(ResourceDiscovery):
    """
    Discovers candidate sensors from the NDP/SciDx catalog
    using metadata-based search (dataset_kind=sensor, region=...).
    """

    def discover(
        self,
        events: List[Event],
        catalog_client: Any,
        policy: dict,
    ) -> List[SensorCandidate]:
        server = policy.get("server", "local")
        terms = policy.get("terms")
        keys = policy.get("keys")
        if not terms:
            terms = ["sensors_2365", "sensor"]
        if not keys:
            keys = ["organization", "extras_dataset_kind"]

        sensor_datasets = catalog_client.search_datasets(
            terms=terms,
            keys=keys,
            server=server,
        )

        sensor_datasets = [
            ds for ds in (sensor_datasets or [])
            if isinstance(ds, dict) and str(ds.get("owner_org", "")).strip() == "sensors_2365"
        ]

        candidates = []
        for ds in sensor_datasets:
            if not isinstance(ds, dict):
                continue
            extras = ds.get("extras", {})
            if not isinstance(extras, dict):
                continue

            lat = extras.get("latitude")
            lon = extras.get("longitude")
            if lat is None or lon is None:
                continue

            candidates.append(SensorCandidate(
                sensor_id=extras.get("station_id", ds.get("name", "")),
                name=ds.get("name", "") or extras.get("station_name") or ds.get("title", ""),
                lat=float(lat),
                lon=float(lon),
                ingestion_mode=extras.get("source", "unknown"),
                resource_id=(ds.get("resources", [{}])[0].get("id")
                             if ds.get("resources") else None),
                metadata={**extras, "server": server},
            ))

        return candidates


def _normalize_lon_deg(lon: float) -> float:
    """Map longitude to (-180, 180] so 0..360° grid values match catalog -180..180°."""
    return ((float(lon) + 180.0) % 360.0) - 180.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km.

    Longitudes may be mixed (e.g. hotspot grid 0..360° vs catalog -180..180°).
    Both are normalized; longitude difference uses the shortest signed delta.
    """
    R = 6371.0
    lon1_n = _normalize_lon_deg(lon1)
    lon2_n = _normalize_lon_deg(lon2)
    dlon_deg = ((lon2_n - lon1_n) + 540.0) % 360.0 - 180.0

    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(dlon_deg)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


class HaversineSelector(AdaptiveSelector):
    """
    Selects the nearest-k sensors for each event using Haversine
    distance.  Supports two modes:
      - nearest_k: select k closest sensors per event
      - radius_km: select all sensors within radius r km
    """

    def _rank_for_event(
        self,
        candidates: List[SensorCandidate],
        event: Event,
        strategy: dict,
    ) -> List[SensorCandidate]:
        mode = strategy.get("selection_mode", "nearest_k")
        k = int(strategy.get("k", 1))
        radius = float(strategy.get("radius_km", 50.0))

        scored: List[SensorCandidate] = []
        for candidate in candidates:
            scored.append(
                SensorCandidate(
                    sensor_id=candidate.sensor_id,
                    name=candidate.name,
                    lat=candidate.lat,
                    lon=candidate.lon,
                    distance_km=_haversine_km(event.lat, event.lon, candidate.lat, candidate.lon),
                    ingestion_mode=candidate.ingestion_mode,
                    resource_id=candidate.resource_id,
                    metadata=dict(candidate.metadata),
                )
            )

        if mode == "nearest_k":
            return sorted(scored, key=lambda c: c.distance_km)[:k]
        return sorted(
            [c for c in scored if c.distance_km <= radius],
            key=lambda c: c.distance_km,
        )

    def select(
        self,
        candidates: List[SensorCandidate],
        events: List[Event],
        strategy: dict,
    ) -> List[SensorCandidate]:
        selected_ids: set = set()
        selected: List[SensorCandidate] = []

        for event in events:
            for candidate in self._rank_for_event(candidates, event, strategy):
                if candidate.sensor_id not in selected_ids:
                    selected_ids.add(candidate.sensor_id)
                    selected.append(candidate)

        return selected

    def build_mappings(
        self,
        candidates: List[SensorCandidate],
        events: List[Event],
        strategy: dict,
    ) -> List[SensorMapping]:
        mappings: List[SensorMapping] = []

        for event in events:
            ranked = self._rank_for_event(candidates, event, strategy)
            for candidate in ranked:
                mappings.append(
                    SensorMapping(
                        sensor_id=candidate.sensor_id,
                        sensor_name=candidate.name,
                        sensor_lat=candidate.lat,
                        sensor_lon=candidate.lon,
                        distance_km=round(candidate.distance_km, 3),
                        hotspot_lat=event.lat,
                        hotspot_lon=event.lon,
                        timestamp=event.timestamp,
                        variable=event.variable or None,
                        value=event.value,
                        threshold_value=event.config.get("threshold"),
                    )
                )

        return mappings


class SciDxStreamOrchestrator(StreamOrchestrator):
    """
    Activates Kafka-backed streams for selected sensors via SciDx
    Streaming, returning ActiveStream handles for downstream
    consumption.
    """

    def observe(
        self,
        active_set: List[SensorCandidate],
        streaming_client: Any,
    ) -> List[ActiveStream]:
        import asyncio

        streams: List[ActiveStream] = []

        for sensor in active_set:
            if not sensor.resource_id:
                continue
            try:
                stream = asyncio.get_event_loop().run_until_complete(
                    streaming_client.create_kafka_stream(
                        consumption_method_ids=[sensor.resource_id],
                        filter_semantics=[],
                        server=sensor.metadata.get("server", "local"),
                    )
                )
                streams.append(ActiveStream(
                    sensor_id=sensor.sensor_id,
                    stream_id=str(getattr(stream, "data_stream_id", "")),
                    topic=str(getattr(stream, "data_stream_id", "")),
                    ingestion_mode=sensor.ingestion_mode,
                    metadata={"resource_id": sensor.resource_id},
                ))
            except Exception as exc:
                # Log but do not halt — partial failure is expected
                print(f"[StreamOrchestrator] Failed to activate "
                      f"{sensor.sensor_id}: {exc}")

        return streams


# ======================================================================
# 4. Complete workflow orchestrator
# ======================================================================

class ForecastTriggeredWorkflow:
    """
    End-to-end workflow that chains the five abstract stages.

    Usage:
        workflow = ForecastTriggeredWorkflow(
            ingestor=AuroraIngestor(),
            detector=NumericFieldPersistenceDetector(),
            discovery=NDPCatalogDiscovery(),
            selector=HaversineSelector(),
            orchestrator=SciDxStreamOrchestrator(),
        )
        result = workflow.run(source_config, region, date_range,
                              detection_config, discovery_policy,
                              selection_strategy, catalog_client,
                              streaming_client)
    """

    def __init__(
        self,
        ingestor: ForecastIngestor,
        detector: EventDetector,
        discovery: ResourceDiscovery,
        selector: AdaptiveSelector,
        orchestrator: StreamOrchestrator,
    ):
        self.ingestor = ingestor
        self.detector = detector
        self.discovery = discovery
        self.selector = selector
        self.orchestrator = orchestrator

    def run_predict(
        self,
        source_config: dict,
        region: dict,
        date_range: dict,
    ) -> Dict[str, Any]:
        artifact = self.ingestor.ingest(source_config, region, date_range)
        if artifact is None:
            return {"status": "skipped", "reason": "no forecast data"}
        return {"status": "done", "artifact": artifact}

    def run_detect(
        self,
        artifact: PredictionArtifact,
        detection_config: dict,
    ) -> Dict[str, Any]:
        events = self.detector.detect(artifact, detection_config)
        return {
            "status": "done",
            "artifact": artifact,
            "events": len(events),
            "event_details": events,
        }

    def run_discover(
        self,
        events: List[Event],
        discovery_policy: dict,
        catalog_client: Any = None,
    ) -> Dict[str, Any]:
        candidates = self.discovery.discover(events, catalog_client, discovery_policy)
        return {
            "status": "done",
            "candidates": len(candidates),
            "candidate_details": candidates,
        }

    def run_select(
        self,
        candidates: List[SensorCandidate],
        events: List[Event],
        selection_strategy: dict,
    ) -> Dict[str, Any]:
        selected = self.selector.select(candidates, events, selection_strategy)
        mappings = self.selector.build_mappings(candidates, events, selection_strategy)
        return {
            "status": "done",
            "selected": len(selected),
            "selected_details": selected,
            "mapping_rows": mappings,
        }

    def run_activate(
        self,
        active_set: List[SensorCandidate],
        streaming_client: Any = None,
    ) -> Dict[str, Any]:
        streams = self.orchestrator.observe(active_set, streaming_client)
        return {
            "status": "done",
            "streams": len(streams),
            "stream_details": streams,
        }

    def run(
        self,
        source_config: Optional[dict] = None,
        region: Optional[dict] = None,
        date_range: Optional[dict] = None,
        detection_config: Optional[dict] = None,
        discovery_policy: Optional[dict] = None,
        selection_strategy: Optional[dict] = None,
        catalog_client: Any = None,
        streaming_client: Any = None,
        *,
        mode: str | WorkflowMode = WorkflowMode.FULL,
        artifact: Optional[PredictionArtifact] = None,
        events: Optional[List[Event]] = None,
        candidates: Optional[List[SensorCandidate]] = None,
        active_set: Optional[List[SensorCandidate]] = None,
    ) -> Dict[str, Any]:
        """Execute the requested workflow mode and return a summary."""
        total_t0 = time.perf_counter()
        stage_timers = {
            "ingest": 0.0,
            "detect": 0.0,
            "discover": 0.0,
            "select": 0.0,
            "stream": 0.0,
            "total": 0.0,
        }

        selected_mode = (
            mode
            if isinstance(mode, WorkflowMode)
            else WorkflowMode(str(mode).strip().lower())
        )

        if selected_mode == WorkflowMode.PREDICT:
            t0 = time.perf_counter()
            predict_result = self.run_predict(source_config or {}, region or {}, date_range or {})
            stage_timers["ingest"] = float(time.perf_counter() - t0)
            stage_timers["total"] = float(time.perf_counter() - total_t0)
            predict_result["stage_timers"] = stage_timers
            return predict_result

        if selected_mode == WorkflowMode.DETECT:
            if artifact is None:
                raise ValueError("artifact is required for detect mode")
            t1 = time.perf_counter()
            detect_result = self.run_detect(artifact, detection_config or {})
            stage_timers["detect"] = float(time.perf_counter() - t1)
            stage_timers["total"] = float(time.perf_counter() - total_t0)
            detect_result["stage_timers"] = stage_timers
            return detect_result

        if selected_mode == WorkflowMode.DISCOVER:
            if events is None:
                raise ValueError("events are required for discover mode")
            t2 = time.perf_counter()
            discover_result = self.run_discover(events, discovery_policy or {}, catalog_client)
            stage_timers["discover"] = float(time.perf_counter() - t2)
            stage_timers["total"] = float(time.perf_counter() - total_t0)
            discover_result["stage_timers"] = stage_timers
            return discover_result

        if selected_mode == WorkflowMode.SELECT:
            if candidates is None or events is None:
                raise ValueError("candidates and events are required for select mode")
            t3 = time.perf_counter()
            select_result = self.run_select(candidates, events, selection_strategy or {})
            stage_timers["select"] = float(time.perf_counter() - t3)
            stage_timers["total"] = float(time.perf_counter() - total_t0)
            select_result["stage_timers"] = stage_timers
            return select_result

        if selected_mode == WorkflowMode.ACTIVATE:
            if active_set is None:
                raise ValueError("active_set is required for activate mode")
            t4 = time.perf_counter()
            activate_result = self.run_activate(active_set, streaming_client)
            stage_timers["stream"] = float(time.perf_counter() - t4)
            stage_timers["total"] = float(time.perf_counter() - total_t0)
            activate_result["stage_timers"] = stage_timers
            return activate_result

        # S1 → S2
        t0 = time.perf_counter()
        artifact = self.ingestor.ingest(source_config or {}, region or {}, date_range or {})
        stage_timers["ingest"] = float(time.perf_counter() - t0)
        if artifact is None:
            stage_timers["total"] = float(time.perf_counter() - total_t0)
            return {
                "status": "skipped",
                "reason": "no forecast data",
                "stage_timers": stage_timers,
            }

        # S2 → S3
        t1 = time.perf_counter()
        events = self.detector.detect(artifact, detection_config or {})
        stage_timers["detect"] = float(time.perf_counter() - t1)

        # S3 → S4
        t2 = time.perf_counter()
        candidates = self.discovery.discover(
            events, catalog_client, discovery_policy or {}
        )
        stage_timers["discover"] = float(time.perf_counter() - t2)

        # S4 → S5
        t3 = time.perf_counter()
        selected = self.selector.select(candidates, events, selection_strategy or {})
        mappings = self.selector.build_mappings(candidates, events, selection_strategy or {})
        stage_timers["select"] = float(time.perf_counter() - t3)

        # S5 → S6
        t4 = time.perf_counter()
        streams = self.orchestrator.observe(selected, streaming_client)
        stage_timers["stream"] = float(time.perf_counter() - t4)
        stage_timers["total"] = float(time.perf_counter() - total_t0)

        return {
            "status": "done",
            "artifact": artifact,
            "events": len(events),
            "event_details": events,
            "candidates": len(candidates),
            "candidate_details": candidates,
            "selected": len(selected),
            "selected_details": selected,
            "mapping_rows": mappings,
            "streams": len(streams),
            "stream_details": streams,
            "stage_timers": stage_timers,
        }


# Backward-compatible alias while callers migrate to workflow terminology.
ForecastTriggeredPipeline = ForecastTriggeredWorkflow


# ======================================================================
# 5. Cross-domain applicability mappings (design only)
# ======================================================================

DOMAIN_MAPPING = {
    "air_quality": {
        "forecast_source": "CAMS + Aurora",
        "event_detector": "Any scalar field >= threshold for k consecutive steps",
        "sensor_type": "Synoptic AQ stations (WebSocket push)",
        "selection_policy": "Nearest-k (Haversine)",
        "ingestion_mode": "WebSocket push",
        "implemented": True,
    },
    "seismic": {
        "forecast_source": "USGS ShakeMap / ShakeAlert",
        "event_detector": "PGA >= threshold",
        "sensor_type": "Seismometers (SeedLink protocol)",
        "selection_policy": "Nearest-k + fault-line proximity",
        "ingestion_mode": "SeedLink real-time stream",
        "implemented": False,
    },
    "wildfire": {
        "forecast_source": "HRRR Smoke / VIIRS AOD",
        "event_detector": "AOD persistence above threshold",
        "sensor_type": "PurpleAir / cameras",
        "selection_policy": "Downwind directional buffer",
        "ingestion_mode": "REST poll / MQTT",
        "implemented": False,
    },
}
