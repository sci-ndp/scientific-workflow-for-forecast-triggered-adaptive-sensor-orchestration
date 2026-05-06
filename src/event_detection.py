# src/event_detection.py

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml


@dataclass
class DetectedFieldEvent:
    """
    A persistent threshold event detected at a single grid cell.
    """
    timestamp: pd.Timestamp   # time when persistence_k is reached
    lat: float
    lon: float
    value: float
    variable: str = "pm2p5_ugm3"

    @property
    def pm25(self) -> float:
        """
        Backward-compatible alias used by older PM2.5-specific code paths.
        """
        return self.value

    def to_dict(self, include_legacy_pm25: bool = False) -> dict:
        row = {
            "timestamp": self.timestamp.isoformat(),
            "lat": float(self.lat),
            "lon": float(self.lon),
            "value": float(self.value),
            "variable": str(self.variable),
        }
        if include_legacy_pm25 or self.variable == "pm2p5_ugm3":
            row["pm25"] = float(self.value)
        return row


# Backward-compatible alias for older imports.
HotspotEvent = DetectedFieldEvent


def detect_hotspots_for_day(
    pred_csv: Path,
    pm25_col: str = "pm2p5_ugm3",
    threshold: float = 10.0,
    persistence_k: int = 3,
    timestep_hours: int = 1,
    round_decimals: int = 4,
    return_stats: bool = False,
) -> List[DetectedFieldEvent] | Tuple[List[DetectedFieldEvent], Dict[str, Any]]:
    """
    Detect persistent threshold exceedance at the SAME grid cell (lat, lon).

    A hotspot is emitted only when:
      - value in the selected field >= threshold
      - for persistence_k consecutive timesteps
      - consecutive means timestamp difference == timestep_hours

    No bbox, no clustering.

    Note: the parameter name ``pm25_col`` is retained for backward
    compatibility with older callers, but the function is field-agnostic.
    """

    stats: Dict[str, Any] = {
        "scanned_cells": 0,
        "scanned_bytes": int(pred_csv.stat().st_size) if pred_csv.exists() else 0,
        "records_in": 0,
        # Detector-local output cardinality (detected hotspot events).
        # Note: run-level records_out in exp_runs.csv is defined as final selected sensors.
        "records_out": 0,
        "ingest_s": 0.0,
        "detect_s": 0.0,
    }

    if not pred_csv.exists():
        print(f"[event_detection] Missing predictions file: {pred_csv}")
        if return_stats:
            return [], stats
        return []

    ingest_t0 = perf_counter()
    df = pd.read_csv(pred_csv)
    stats["records_in"] = int(len(df))
    stats["ingest_s"] = float(perf_counter() - ingest_t0)

    required = {"timestamp", "latitude", "longitude", pm25_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{pred_csv} missing required columns: {sorted(missing)}")

    # Normalize timestamp. Some prediction exports include mixed formats
    # (e.g., "YYYY-MM-DD HH:MM:SS" and "YYYY-MM-DD").
    raw_ts = df["timestamp"]
    try:
        parsed_ts = pd.to_datetime(raw_ts, utc=True, format="mixed", errors="coerce")
    except TypeError:
        # Older pandas may not support format="mixed".
        parsed_ts = pd.to_datetime(raw_ts, utc=True, errors="coerce")

    bad_ts = parsed_ts.isna()
    if bad_ts.any():
        examples = raw_ts[bad_ts].astype(str).head(3).tolist()
        raise ValueError(
            f"{pred_csv} has {int(bad_ts.sum())} unparseable timestamp values. "
            f"Examples: {examples}"
        )
    df["timestamp"] = parsed_ts

    detect_t0 = perf_counter()

    # ---- Create stable cell identifiers (avoid float noise) ----
    df["lat_key"] = df["latitude"].round(round_decimals)
    df["lon_key"] = df["longitude"].round(round_decimals)
    df["cell_id"] = df["lat_key"].astype(str) + "_" + df["lon_key"].astype(str)

    # Sort so streak logic works correctly
    df = df.sort_values(["cell_id", "timestamp"])
    stats["scanned_cells"] = int(df["cell_id"].nunique())


    expected_dt = pd.Timedelta(hours=timestep_hours)

    events: List[DetectedFieldEvent] = []

    # ---- Process each grid cell independently ----
    for _, group in df.groupby("cell_id", sort=False):
        streak = 0
        prev_ts = None
        emitted = False

        for row in group.itertuples(index=False):
            ts = row.timestamp
            value = float(getattr(row, pm25_col))
            above = value >= threshold

            consecutive = (
                prev_ts is not None
                and (ts - prev_ts) == expected_dt
            )

            if above:
                if streak == 0 or not consecutive:
                    streak = 1
                    emitted = False
                else:
                    streak += 1
            else:
                streak = 0
                emitted = False

            # Emit event once when streak reaches persistence_k
            if (not emitted) and streak >= persistence_k:
                events.append(
                    DetectedFieldEvent(
                        timestamp=ts,
                        lat=float(row.lat_key),
                        lon=float(row.lon_key),
                        value=value,
                        variable=pm25_col,
                    )
                )
                emitted = True

            prev_ts = ts

    # Detector-local records_out (event count), not final selected sensor decisions.
    stats["records_out"] = int(len(events))
    stats["detect_s"] = float(perf_counter() - detect_t0)

    if return_stats:
        return events, stats
    return events


def events_to_dataframe(
    events: List[DetectedFieldEvent],
    *,
    include_legacy_pm25: Optional[bool] = None,
) -> pd.DataFrame:
    if include_legacy_pm25 is None:
        include_legacy_pm25 = bool(events) and all(
            getattr(e, "variable", None) == "pm2p5_ugm3" for e in events
        )

    if not events:
        columns = ["timestamp", "lat", "lon", "value", "variable"]
        if include_legacy_pm25:
            columns.append("pm25")
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(
        [e.to_dict(include_legacy_pm25=include_legacy_pm25) for e in events]
    )


def main():
    """
    CLI: build hotspot CSVs for all dates in config.yaml.

    Writes:
        data/processed/hotspots/{date}_{region}_hotspots.csv
    """
    here = Path(__file__).resolve()
    project_root = here.parent.parent  # src/.. -> repo root
    config_path = project_root / "config.yaml"
    cfg = yaml.safe_load(config_path.read_text())

    region_name = cfg["region"]["name"]

    date_cfg = cfg["dates"]
    start = pd.to_datetime(date_cfg["start"])
    end = pd.to_datetime(date_cfg["end"])
    dates = pd.date_range(start, end, freq="D")

    times_tag = cfg["aurora"]["timestamp"]
    lead_hours = int(cfg["aurora"]["lead_hours"])

    hotspot_cfg = cfg.get("hotspots", {})
    threshold = float(hotspot_cfg.get("pm25_threshold", 10.0))
    persistence_k = int(hotspot_cfg.get("persistence_k", 3))
    timestep_hours = int(hotspot_cfg.get("timestep_hours", 1))
    round_decimals = int(hotspot_cfg.get("round_decimals", 4))

    out_base = project_root / "data/processed/hotspots"
    out_base.mkdir(parents=True, exist_ok=True)

    for day in dates:
        day_str = day.strftime("%Y-%m-%d")
        pred_csv = (
            project_root / "data/processed/predictions"
            / f"{day_str}_{times_tag}_{lead_hours}h_{region_name}.csv"
        )

        print(f"[event_detection] Processing {pred_csv}")

        events = detect_hotspots_for_day(
            pred_csv=pred_csv,
            threshold=threshold,
            persistence_k=persistence_k,
            timestep_hours=timestep_hours,
            round_decimals=round_decimals,
        )

        df_events = events_to_dataframe(events)

        df_events = (
            df_events
            .sort_values("timestamp")
            .drop_duplicates(subset=["lat", "lon"], keep="first")
        )

        out_path = out_base / f"{day_str}_{region_name}_k{persistence_k}_hotspots.csv"
        df_events.to_csv(out_path, index=False)

        print(
            f"[event_detection] Found {len(df_events)} persistent hotspots → {out_path}"
        )


if __name__ == "__main__":
    main()
