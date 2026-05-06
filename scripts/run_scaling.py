#!/usr/bin/env python3
"""
Core scaling runner (linear/KD-tree selector + NDP discovery).

Runs matrix:
  sizes x k-values x days

Writes:
  paper/results/exp_runs.csv
  paper/results/scaling_summary.csv
  paper/results/size_k_summary.csv
  paper/results/latency_percentiles.csv
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import time
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

try:
    from src.experiment_metrics import (
        EXP_RUNS_COLUMNS,
        SCHEMA_VERSION,
        append_row,
        ensure_header,
    )
except Exception:
    from experiment_metrics import (  # type: ignore
        EXP_RUNS_COLUMNS,
        SCHEMA_VERSION,
        append_row,
        ensure_header,
    )

DEFAULT_DAYS = [
    "2025-01-13",
    "2025-01-14",
    "2025-01-15",
    "2025-01-16",
    "2025-01-17",
    "2025-01-21",
    "2025-01-22",
    "2025-01-23",
    "2025-01-24",
    "2025-01-27",
    "2025-01-28",
    "2025-01-29",
    "2025-01-30",
    "2025-01-31",
]
DEFAULT_SIZES = [8, 200, 681, 1000, 2365]
DEFAULT_K_VALUES = [1, 3, 6, 9, 12]
CSV_COLUMNS = list(EXP_RUNS_COLUMNS)


@dataclass(frozen=True)
class Candidate:
    station_id: str
    lat: float
    lon: float
    adapter_modes: tuple[str, ...] = ()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run scaling matrix and emit exp_runs.csv + summary tables."
    )
    parser.add_argument(
        "--prediction-dir",
        default="data/processed/prediction",
        help="Prediction directory (supports fallback to data/processed/predictions).",
    )
    parser.add_argument(
        "--out-csv",
        default="paper/results/exp_runs.csv",
        help="Output CSV path for per-run logs.",
    )
    parser.add_argument(
        "--days",
        default=",".join(DEFAULT_DAYS),
        help="Comma-separated dates (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--sizes",
        default=",".join(str(v) for v in DEFAULT_SIZES),
        help="Comma-separated sensor cohort sizes.",
    )
    parser.add_argument(
        "--k-values",
        default=",".join(str(v) for v in DEFAULT_K_VALUES),
        help="Comma-separated persistence k values.",
    )
    parser.add_argument(
        "--selection-k",
        type=int,
        default=1,
        help="Number of nearest sensors selected per hotspot.",
    )
    parser.add_argument(
        "--selector",
        default="linear",
        choices=("linear", "kdtree"),
        help="Selection algorithm: linear scan or KD-tree nearest-neighbor.",
    )
    parser.add_argument(
        "--server",
        default=os.getenv("SERVER", "local"),
        help="NDP server name.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing out CSV instead of overwriting.",
    )
    return parser.parse_args()


def parse_csv_ints(raw: str) -> List[int]:
    out: List[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        out.append(int(token))
    if not out:
        raise ValueError("Expected at least one integer value.")
    return out


def parse_csv_strings(raw: str) -> List[str]:
    out = [token.strip() for token in raw.split(",") if token.strip()]
    if not out:
        raise ValueError("Expected at least one string value.")
    return out


def resolve_prediction_dir(path: Path) -> Path:
    if path.exists():
        return path

    # User often says "prediction", while current tree uses "predictions".
    if path.name == "prediction":
        alt = path.with_name("predictions")
        if alt.exists():
            print(f"[warn] {path} not found; using {alt}")
            return alt
    raise FileNotFoundError(f"Prediction directory not found: {path}")


def find_prediction_file(pred_dir: Path, day: str, region: str) -> Path:
    matches = sorted(pred_dir.glob(f"{day}_*_12h_{region}.csv"))
    if not matches:
        raise FileNotFoundError(f"No prediction file for day={day} region={region} in {pred_dir}")
    if len(matches) > 1:
        print(f"[warn] Multiple prediction files for {day}/{region}; using {matches[0].name}")
    return matches[0]


def load_ndp_client():
    try:
        from dotenv import load_dotenv
        from ndp_ep import APIClient
    except Exception as exc:
        raise RuntimeError(
            "Missing dependencies for NDP discovery. Install dotenv + ndp_ep."
        ) from exc

    load_dotenv(override=False)
    token = os.getenv("TOKEN")
    api_url = os.getenv("API_URL")
    if not token or not api_url:
        raise RuntimeError("Missing TOKEN or API_URL in environment/.env for NDP discovery.")
    return APIClient(base_url=api_url, token=token)


def _normalize_adapter_modes(extras: Dict[str, Any]) -> tuple[str, ...]:
    raw = extras.get("adapter_modes") or extras.get("source") or ""
    if isinstance(raw, (list, tuple)):
        tokens = [str(v).strip().lower() for v in raw]
    else:
        normalized = str(raw).replace("|", ",").replace(";", ",")
        tokens = [tok.strip().lower() for tok in normalized.split(",")]

    modes: set[str] = set()
    for token in tokens:
        if not token:
            continue
        if "websocket" in token or token == "wss" or token.endswith("_wss"):
            modes.add("websocket")
            continue
        if "rest" in token or "http" in token:
            modes.add("rest")
            continue
        if "csv" in token:
            modes.add("csv_replay")
            continue
    if not modes:
        modes.add("unknown")
    return tuple(sorted(modes))


def discover_candidates_ndp(
    client,
    *,
    region: str,
    server: str,
    owner_org: str,
) -> List[Candidate]:
    datasets = client.advanced_search(
        {
            "owner_org": owner_org,
            "extras_region": region,
            "server": server,
        }
    ) or []

    by_station: Dict[str, Candidate] = {}
    for ds in datasets:
        if not isinstance(ds, dict):
            continue
        ds_owner_org = str(ds.get("owner_org") or "").strip()
        if ds_owner_org and ds_owner_org != owner_org:
            continue
        extras = ds.get("extras", {})
        if not isinstance(extras, dict):
            continue

        ds_region = str(extras.get("region") or "").strip().lower()
        if ds_region and ds_region != region.lower():
            continue
        station_id = str(extras.get("station_id") or "").strip()
        if not station_id:
            continue
        lat = extras.get("latitude")
        lon = extras.get("longitude")
        if lat is None or lon is None:
            continue
        try:
            cand = Candidate(
                station_id=station_id,
                lat=float(lat),
                lon=float(lon),
                adapter_modes=_normalize_adapter_modes(extras),
            )
        except Exception:
            continue

        if station_id not in by_station:
            by_station[station_id] = cand

    return sorted(by_station.values(), key=lambda c: c.station_id)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def select_linear_nearest(
    events: Sequence[Any],
    candidates: Sequence[Candidate],
    *,
    selection_k: int,
) -> List[str]:
    if selection_k < 1:
        raise ValueError("selection_k must be >= 1")

    selected: set[str] = set()
    for event in events:
        ranked = sorted(
            candidates,
            key=lambda c: haversine_km(float(event.lat), float(event.lon), c.lat, c.lon),
        )
        for cand in ranked[:selection_k]:
            selected.add(cand.station_id)
    return sorted(selected)


def _latlon_to_unit_xyz(lat: float, lon: float) -> tuple[float, float, float]:
    """
    Convert latitude/longitude to a 3D unit-sphere vector.

    This avoids longitude-domain issues (e.g., 0..360 vs -180..180) and
    preserves nearest-neighbor ordering by great-circle distance.
    """
    lat_rad = math.radians(float(lat))
    lon_rad = math.radians(float(lon))
    cos_lat = math.cos(lat_rad)
    return (
        cos_lat * math.cos(lon_rad),
        cos_lat * math.sin(lon_rad),
        math.sin(lat_rad),
    )


def select_kdtree_nearest(
    events: Sequence[Any],
    candidates: Sequence[Candidate],
    *,
    selection_k: int,
) -> List[str]:
    if selection_k < 1:
        raise ValueError("selection_k must be >= 1")
    if not events or not candidates:
        return []

    try:
        import numpy as np
        from scipy.spatial import cKDTree
    except Exception as exc:
        raise RuntimeError(
            "selector=kdtree requires scipy + numpy (cKDTree)."
        ) from exc

    k_eff = min(int(selection_k), len(candidates))
    points = np.asarray(
        [_latlon_to_unit_xyz(c.lat, c.lon) for c in candidates],
        dtype=float,
    )
    tree = cKDTree(points)
    query_points = np.asarray(
        [_latlon_to_unit_xyz(float(event.lat), float(event.lon)) for event in events],
        dtype=float,
    )
    _, idx = tree.query(query_points, k=k_eff)

    selected: set[str] = set()
    if k_eff == 1:
        for raw_i in np.asarray(idx).reshape(-1):
            selected.add(candidates[int(raw_i)].station_id)
    else:
        for row in np.asarray(idx):
            for raw_i in np.asarray(row).reshape(-1):
                selected.add(candidates[int(raw_i)].station_id)
    return sorted(selected)


def select_nearest(
    events: Sequence[Any],
    candidates: Sequence[Candidate],
    *,
    selection_k: int,
    selector: str,
) -> List[str]:
    selector_norm = str(selector).strip().lower()
    if selector_norm == "linear":
        return select_linear_nearest(events, candidates, selection_k=selection_k)
    if selector_norm == "kdtree":
        return select_kdtree_nearest(events, candidates, selection_k=selection_k)
    raise ValueError(f"Unsupported selector={selector!r}; expected linear or kdtree.")


def _adapter_counts(candidates: Sequence[Candidate]) -> Dict[str, int]:
    websocket = 0
    rest = 0
    csv_replay = 0
    for cand in candidates:
        modes = set(cand.adapter_modes)
        if "websocket" in modes:
            websocket += 1
        if "rest" in modes:
            rest += 1
        if "csv_replay" in modes:
            csv_replay += 1
    return {
        "adapter_websocket_count": websocket,
        "adapter_rest_count": rest,
        "adapter_csv_replay_count": csv_replay,
    }


def _load_detection_fn():
    try:
        from src.event_detection import detect_hotspots_for_day
    except Exception:
        # Running as "python scripts/run_scaling.py" may omit repo root from
        # sys.path; add it once, then retry src import.
        import sys

        root = str(Path(__file__).resolve().parents[1])
        if root not in sys.path:
            sys.path.insert(0, root)
        try:
            from src.event_detection import detect_hotspots_for_day
        except Exception:
            from event_detection import detect_hotspots_for_day
    return detect_hotspots_for_day


def timed_detect(prediction_file: Path, persistence_k: int) -> tuple[List[Any], Dict[str, Any]]:
    detect_hotspots_for_day = _load_detection_fn()
    t0 = time.perf_counter()
    stats: Dict[str, Any] = {
        "scanned_cells": 0,
        "scanned_bytes": int(prediction_file.stat().st_size) if prediction_file.exists() else 0,
        "records_in": 0,
        "records_out": 0,
        "detect_s": 0.0,
        "ingest_s": 0.0,
    }
    with redirect_stdout(io.StringIO()):
        try:
            result = detect_hotspots_for_day(
                pred_csv=prediction_file,
                persistence_k=persistence_k,
                return_stats=True,
            )
            if isinstance(result, tuple) and len(result) == 2:
                events, det_stats = result
                if isinstance(det_stats, dict):
                    stats.update(det_stats)
            else:
                events = result
        except TypeError:
            # Backward compatibility with older detection signatures.
            events = detect_hotspots_for_day(
                pred_csv=prediction_file,
                persistence_k=persistence_k,
            )
    if not stats.get("detect_s"):
        stats["detect_s"] = float(time.perf_counter() - t0)
    stats["records_out"] = int(stats.get("records_out") or len(events))
    return events, stats


def _summarize_by_size(df):
    return (
        df.groupby("size", as_index=False)
        .agg(
            runs=("total_s", "count"),
            total_p50_s=("total_s", "median"),
            total_p95_s=("total_s", lambda s: float(s.quantile(0.95))),
            total_q25_s=("total_s", lambda s: float(s.quantile(0.25))),
            total_q75_s=("total_s", lambda s: float(s.quantile(0.75))),
            detect_p50_s=("detect_s", "median"),
            discover_p50_s=("discover_s", "median"),
            select_p50_s=("select_s", "median"),
        )
        .sort_values("size")
    )


def _summarize_by_size_k(df):
    return (
        df.groupby(["size", "k"], as_index=False)
        .agg(
            runs=("total_s", "count"),
            select_p50_s=("select_s", "median"),
            select_p95_s=("select_s", lambda s: float(s.quantile(0.95))),
            total_p50_s=("total_s", "median"),
            total_p95_s=("total_s", lambda s: float(s.quantile(0.95))),
        )
        .sort_values(["size", "k"])
    )


def _bootstrap_quantile_ci(values, quantile: float, *, n_boot: int = 1200, alpha: float = 0.05):
    import numpy as np

    arr = np.asarray([float(v) for v in values if v == v], dtype=float)
    if arr.size == 0:
        return float("nan"), float("nan")
    point = float(np.quantile(arr, quantile))
    if arr.size < 2:
        return point, point

    rng = np.random.default_rng(20260313 + int(quantile * 1000) + arr.size)
    boots = np.empty(n_boot, dtype=float)
    for i in range(n_boot):
        draw = rng.choice(arr, size=arr.size, replace=True)
        boots[i] = float(np.quantile(draw, quantile))
    lo = float(np.quantile(boots, alpha / 2))
    hi = float(np.quantile(boots, 1.0 - alpha / 2))
    return lo, hi


def _summarize_percentiles_by_size(df):
    import numpy as np

    rows: List[Dict[str, float]] = []
    for size, group in df.groupby("size"):
        vals = np.asarray(group["total_s"].dropna().astype(float).to_numpy(), dtype=float)
        if vals.size == 0:
            continue
        rows.append(
            {
                "size": int(size),
                "runs": int(vals.size),
                "total_mean_s": float(np.mean(vals)),
                "total_std_s": float(np.std(vals, ddof=0)),
                "total_p50_s": float(np.quantile(vals, 0.50)),
                "total_p75_s": float(np.quantile(vals, 0.75)),
                "total_p90_s": float(np.quantile(vals, 0.90)),
                "total_p95_s": float(np.quantile(vals, 0.95)),
                "total_p99_s": float(np.quantile(vals, 0.99)),
            }
        )
    return rows


def write_scaling_tables(csv_path: Path) -> None:
    try:
        import pandas as pd
    except Exception as exc:
        print(f"[warn] Skipping summary generation (missing pandas): {exc}")
        return

    if not csv_path.exists():
        print(f"[warn] CSV not found, skipping summaries: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    if df.empty:
        print("[warn] Empty exp_runs.csv; skipping summaries")
        return

    df = df[df["status"] == "done"].copy()
    if df.empty:
        print("[warn] No successful rows in exp_runs.csv; skipping summaries")
        return

    for col in ["size", "k", "detect_s", "discover_s", "select_s", "total_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["size", "total_s"])
    if df.empty:
        print("[warn] No numeric size/total_s rows available; skipping summaries")
        return

    by_size = _summarize_by_size(df)
    if by_size.empty:
        print("[warn] Aggregation empty; skipping summaries")
        return

    ci_rows: List[Dict[str, float]] = []
    for size, group in df.groupby("size"):
        p50_lo, p50_hi = _bootstrap_quantile_ci(group["total_s"], 0.50)
        p95_lo, p95_hi = _bootstrap_quantile_ci(group["total_s"], 0.95)
        ci_rows.append(
            {
                "size": int(size),
                "total_p50_ci_lo_s": p50_lo,
                "total_p50_ci_hi_s": p50_hi,
                "total_p95_ci_lo_s": p95_lo,
                "total_p95_ci_hi_s": p95_hi,
            }
        )
    ci_df = pd.DataFrame(ci_rows)
    by_size = by_size.merge(ci_df, on="size", how="left")

    by_size["delta_p50_vs_prev_pct"] = by_size["total_p50_s"].pct_change() * 100.0
    by_size["delta_p95_vs_prev_pct"] = by_size["total_p95_s"].pct_change() * 100.0
    summary_csv = csv_path.with_name("scaling_summary.csv")
    by_size.to_csv(summary_csv, index=False)
    print(f"[ok] scaling summary: {summary_csv}")

    by_size_k = _summarize_by_size_k(df)
    summary_k_csv = csv_path.with_name("size_k_summary.csv")
    by_size_k.to_csv(summary_k_csv, index=False)
    print(f"[ok] size-k summary: {summary_k_csv}")

    percentile_rows = _summarize_percentiles_by_size(df)
    percentile_csv = csv_path.with_name("latency_percentiles.csv")
    pd.DataFrame(percentile_rows).sort_values("size").to_csv(percentile_csv, index=False)
    print(f"[ok] percentile summary: {percentile_csv}")


def main() -> None:
    args = parse_args()

    pred_dir = resolve_prediction_dir(Path(args.prediction_dir).resolve())
    out_csv = Path(args.out_csv).resolve()

    days = parse_csv_strings(args.days)
    sizes = parse_csv_ints(args.sizes)
    k_values = parse_csv_ints(args.k_values)
    selection_k = int(args.selection_k)
    selector = str(args.selector).strip().lower()
    if selection_k < 1:
        raise ValueError("--selection-k must be >= 1")
    if selector not in {"linear", "kdtree"}:
        raise ValueError("--selector must be one of: linear, kdtree")

    client = load_ndp_client()

    if not getattr(args, "append", False) and out_csv.exists():
        out_csv.unlink()
    ensure_header(out_csv, CSV_COLUMNS)

    total_runs = len(sizes) * len(k_values) * len(days)
    run_idx = 0
    done = 0
    failed = 0
    by_size: Dict[int, Dict[str, int]] = {size: {"done": 0, "failed": 0} for size in sizes}

    for size in sizes:
        region = "utah" if size == 8 else "usa"
        owner_org = f"sensors_{size}"
        for k in k_values:
            for day in days:
                run_idx += 1
                row: Dict[str, object] = {
                    "schema_version": SCHEMA_VERSION,
                    "run_id": run_idx,
                    "date": day,
                    "size": size,
                    "k": k,
                    "region": region,
                    "selector": selector,
                    "discovery": "ndp",
                    "adapter_mode": "",
                    "event_count": "",
                    "detect_s": "",
                    "discover_s": "",
                    "select_s": "",
                    "ingest_s": 0.0,  # Default; overwritten from detection stats.
                    "stream_s": 0.0,  # Static-run context in Phase 4.
                    "total_s": "",
                    "bytes_in": "",
                    "bytes_out": "",
                    "records_in": "",
                    "records_out": "",
                    "scanned_cells": "",
                    "scanned_bytes": "",
                    "adapter_websocket_count": "",
                    "adapter_rest_count": "",
                    "adapter_csv_replay_count": "",
                    "status": "failed",
                    "error": "",
                    "prediction_file": "",
                    "cohort_file": owner_org,
                    "candidate_count": "",
                    "selected_count": "",
                }

                print(
                    f"[run {run_idx}/{total_runs}] day={day} size={size} k={k} "
                    f"region={region} owner_org={owner_org}"
                )
                total_t0 = time.perf_counter()
                try:
                    prediction_file = find_prediction_file(pred_dir, day, region)
                    row["prediction_file"] = str(prediction_file)

                    events, detect_stats = timed_detect(prediction_file, persistence_k=k)
                    row["event_count"] = len(events)  # Detection-stage output count.
                    row["ingest_s"] = round(float(detect_stats.get("ingest_s", 0.0) or 0.0), 6)
                    row["detect_s"] = round(float(detect_stats.get("detect_s", 0.0) or 0.0), 6)
                    row["records_in"] = int(detect_stats.get("records_in", 0) or 0)
                    row["scanned_cells"] = int(detect_stats.get("scanned_cells", 0) or 0)
                    row["scanned_bytes"] = int(detect_stats.get("scanned_bytes", 0) or 0)
                    row["bytes_in"] = int(detect_stats.get("scanned_bytes", 0) or 0)

                    t1 = time.perf_counter()
                    candidates = discover_candidates_ndp(
                        client,
                        region=region,
                        server=args.server,
                        owner_org=owner_org,
                    )
                    discover_s = time.perf_counter() - t1
                    row["discover_s"] = round(discover_s, 6)
                    row["candidate_count"] = len(candidates)

                    t2 = time.perf_counter()
                    selected_ids = select_nearest(
                        events,
                        candidates,
                        selection_k=selection_k,
                        selector=selector,
                    )
                    select_s = time.perf_counter() - t2
                    row["select_s"] = round(select_s, 6)
                    row["selected_count"] = len(selected_ids)

                    by_station = {cand.station_id: cand for cand in candidates}
                    selected_candidates = [by_station[sid] for sid in selected_ids if sid in by_station]
                    adapter_counters = _adapter_counts(selected_candidates)
                    row.update(adapter_counters)

                    active_modes: set[str] = set()
                    for cand in selected_candidates:
                        active_modes.update(cand.adapter_modes)
                    if not active_modes:
                        active_modes = {"unknown"}
                    row["adapter_mode"] = ",".join(sorted(active_modes))

                    control_payload = {
                        "events": int(len(events)),
                        "selected_ids": selected_ids,
                        "selection_k": int(selection_k),
                    }
                    row["bytes_out"] = len(
                        json.dumps(control_payload, separators=(",", ":")).encode("utf-8")
                    )
                    row["records_out"] = len(selected_ids)  # Final selected-sensor control outputs.

                    row["total_s"] = round(time.perf_counter() - total_t0, 6)
                    row["status"] = "done"
                    done += 1
                    by_size[size]["done"] += 1
                except Exception as exc:
                    row["total_s"] = round(time.perf_counter() - total_t0, 6)
                    row["error"] = str(exc)
                    failed += 1
                    by_size[size]["failed"] += 1

                append_row(out_csv, row)

    print(f"[done] total_runs={total_runs} done={done} failed={failed} csv={out_csv}")
    for size in sizes:
        print(
            f"[summary] size={size} done={by_size[size]['done']} failed={by_size[size]['failed']}"
        )

    write_scaling_tables(out_csv)


if __name__ == "__main__":
    main()
