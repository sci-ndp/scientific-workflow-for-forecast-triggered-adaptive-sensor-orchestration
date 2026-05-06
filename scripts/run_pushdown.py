#!/usr/bin/env python3
"""
Query pushdown runner (USA baseline vs Utah subset).

Outputs:
  - paper/results/exp_pushdown.csv
  - paper/results/pushdown_speedup_table.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import math
import time
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Dict, List, Sequence

try:
    from src.experiment_metrics import (
        EXP_PUSHDOWN_COLUMNS,
        SCHEMA_VERSION,
        append_row,
        ensure_header,
    )
except Exception:
    from experiment_metrics import (  # type: ignore
        EXP_PUSHDOWN_COLUMNS,
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

DEFAULT_MODES = ["usa", "utah"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Phase 5 pushdown metrics for USA baseline vs Utah subset."
    )
    parser.add_argument(
        "--prediction-dir",
        default="data/processed/predictions",
        help="Prediction directory (supports fallback to data/processed/prediction).",
    )
    parser.add_argument(
        "--days",
        default=",".join(DEFAULT_DAYS),
        help="Comma-separated day list (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--modes",
        default=",".join(DEFAULT_MODES),
        help="Comma-separated modes, default: usa,utah.",
    )
    parser.add_argument(
        "--lead-hours",
        type=int,
        default=12,
        help="Lead-hours tag in prediction filename pattern.",
    )
    parser.add_argument(
        "--threshold-field",
        default="pm2p5_ugm3",
        help="Detection threshold field name.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="Detection threshold value.",
    )
    parser.add_argument(
        "--persistence-k",
        type=int,
        default=6,
        help="Persistence k for detection.",
    )
    parser.add_argument(
        "--timestep-hours",
        type=int,
        default=1,
        help="Detection timestep hours.",
    )
    parser.add_argument(
        "--out-csv",
        default="paper/results/exp_pushdown.csv",
        help="Run-level output CSV path.",
    )
    parser.add_argument(
        "--out-table-csv",
        default="paper/results/pushdown_speedup_table.csv",
        help="Aggregated pushdown summary table path.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing run CSV instead of overwriting.",
    )
    return parser.parse_args()


def parse_csv_strings(raw: str) -> List[str]:
    vals = [token.strip() for token in str(raw).split(",") if token.strip()]
    if not vals:
        raise ValueError("Expected at least one value.")
    return vals


def resolve_prediction_dir(path: Path) -> Path:
    if path.exists():
        return path
    if path.name == "predictions":
        alt = path.with_name("prediction")
        if alt.exists():
            print(f"[warn] {path} not found; using {alt}")
            return alt
    raise FileNotFoundError(f"Prediction directory not found: {path}")


def find_prediction_file(pred_dir: Path, day: str, mode: str, lead_hours: int) -> Path:
    matches = sorted(pred_dir.glob(f"{day}_*_{lead_hours}h_{mode}.csv"))
    if not matches:
        raise FileNotFoundError(
            f"No prediction file for day={day} mode={mode} lead_hours={lead_hours} in {pred_dir}"
        )
    if len(matches) > 1:
        print(f"[warn] Multiple prediction files for {day}/{mode}; using {matches[0].name}")
    return matches[0]


def _load_detection_fn():
    try:
        from src.event_detection import detect_hotspots_for_day
    except Exception:
        # Running as "python scripts/run_pushdown.py" may not include repo root
        # on sys.path. Add it once, then retry src import.
        import sys

        root = str(Path(__file__).resolve().parents[1])
        if root not in sys.path:
            sys.path.insert(0, root)
        try:
            from src.event_detection import detect_hotspots_for_day
        except Exception:
            from event_detection import detect_hotspots_for_day
    return detect_hotspots_for_day


def timed_detect(
    prediction_file: Path,
    *,
    threshold_field: str,
    threshold: float,
    persistence_k: int,
    timestep_hours: int,
) -> tuple[List[Any], Dict[str, Any]]:
    detect_hotspots_for_day = _load_detection_fn()
    start = time.perf_counter()
    stats: Dict[str, Any] = {
        "scanned_cells": 0,
        "scanned_bytes": int(prediction_file.stat().st_size) if prediction_file.exists() else 0,
        "records_in": 0,
        "records_out": 0,
        "detect_s": 0.0,
    }
    with redirect_stdout(io.StringIO()):
        try:
            result = detect_hotspots_for_day(
                pred_csv=prediction_file,
                pm25_col=threshold_field,
                threshold=threshold,
                persistence_k=persistence_k,
                timestep_hours=timestep_hours,
                return_stats=True,
            )
            if isinstance(result, tuple) and len(result) == 2:
                events, det_stats = result
                if isinstance(det_stats, dict):
                    stats.update(det_stats)
            else:
                events = result
        except TypeError:
            # Backward compatibility for older function signatures.
            events = detect_hotspots_for_day(
                pred_csv=prediction_file,
                persistence_k=persistence_k,
            )
    if not stats.get("detect_s"):
        stats["detect_s"] = float(time.perf_counter() - start)
    stats["records_out"] = int(stats.get("records_out", 0) or len(events))
    return events, stats


def _quantile(values: Sequence[float], q: float) -> float:
    vals = sorted(float(v) for v in values)
    if not vals:
        return float("nan")
    if len(vals) == 1:
        return vals[0]
    q = min(max(float(q), 0.0), 1.0)
    idx = (len(vals) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] + (vals[hi] - vals[lo]) * frac


def _safe_div(num: float, den: float) -> float:
    if den == 0.0:
        return float("nan")
    return float(num) / float(den)


def _fmt(value: float) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return f"{float(value):.15g}"


def load_done_rows(path: Path) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row.get("status") or "").strip().lower() == "done":
                out.append(row)
    return out


def print_run_qa(out_csv: Path, *, expected_days: List[str], expected_modes: List[str]) -> None:
    rows: List[Dict[str, str]] = []
    with out_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    total_rows = len(rows)
    done_rows = [r for r in rows if str(r.get("status") or "").strip().lower() == "done"]
    failed_rows = [r for r in rows if str(r.get("status") or "").strip().lower() == "failed"]

    mode_counts: Dict[str, int] = {}
    day_set: set[str] = set()
    pair_counts: Dict[tuple[str, str], int] = {}
    done_missing_metrics = 0

    for row in rows:
        day = str(row.get("date") or "").strip()
        mode = str(row.get("mode") or "").strip().lower()
        if day:
            day_set.add(day)
        if mode:
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
        if day and mode:
            pair = (day, mode)
            pair_counts[pair] = pair_counts.get(pair, 0) + 1

        if str(row.get("status") or "").strip().lower() == "done":
            for col in ("scanned_cells", "scanned_bytes", "detect_s"):
                if str(row.get(col) or "").strip() == "":
                    done_missing_metrics += 1
                    break

    expected_pairs = {(day, mode) for day in expected_days for mode in expected_modes}
    actual_pairs = set(pair_counts.keys())
    missing_pairs = sorted(expected_pairs - actual_pairs)
    duplicate_pairs = sorted([pair for pair, n in pair_counts.items() if n > 1])

    print(
        "[qa] "
        f"rows={total_rows} expected_rows={len(expected_days) * len(expected_modes)} "
        f"done={len(done_rows)} failed={len(failed_rows)}"
    )
    print(
        "[qa] "
        f"modes_present={sorted(mode_counts.keys())} "
        f"days_present={len(day_set)}"
    )
    print(
        "[qa] "
        f"missing_pairs={len(missing_pairs)} duplicate_pairs={len(duplicate_pairs)} "
        f"done_missing_metrics={done_missing_metrics}"
    )

    # Print a compact day-mode matrix preview (first 5 expected days).
    print("[qa] day_mode_head:")
    for day in sorted(expected_days)[:5]:
        counts = []
        for mode in expected_modes:
            counts.append(f"{mode}={pair_counts.get((day, mode), 0)}")
        print(f"[qa]   {day} " + " ".join(counts))

    if missing_pairs:
        print(f"[qa][warn] missing_pair_examples={missing_pairs[:5]}")
    if duplicate_pairs:
        print(f"[qa][warn] duplicate_pair_examples={duplicate_pairs[:5]}")


def write_summary_table(out_csv: Path, out_table_csv: Path, modes: List[str]) -> None:
    rows = load_done_rows(out_csv)
    if not rows:
        raise ValueError(f"No status=done rows available in {out_csv}")

    metrics_by_mode: Dict[str, Dict[str, List[float]]] = {
        mode: {"detect_s": [], "scanned_cells": [], "scanned_bytes": []}
        for mode in modes
    }
    for row in rows:
        mode = str(row.get("mode") or "").strip().lower()
        if mode not in metrics_by_mode:
            continue
        try:
            detect_s = float(row.get("detect_s", ""))
            scanned_cells = float(row.get("scanned_cells", ""))
            scanned_bytes = float(row.get("scanned_bytes", ""))
        except Exception:
            continue
        metrics_by_mode[mode]["detect_s"].append(detect_s)
        metrics_by_mode[mode]["scanned_cells"].append(scanned_cells)
        metrics_by_mode[mode]["scanned_bytes"].append(scanned_bytes)

    if "usa" not in metrics_by_mode:
        raise ValueError("USA baseline mode is required for pushdown summary.")

    summary_core: Dict[str, Dict[str, float]] = {}
    for mode in modes:
        vals = metrics_by_mode.get(mode, {})
        summary_core[mode] = {
            "runs": float(len(vals.get("detect_s", []))),
            "detect_p50_s": _quantile(vals.get("detect_s", []), 0.50),
            "detect_p95_s": _quantile(vals.get("detect_s", []), 0.95),
            "scanned_cells_p50": _quantile(vals.get("scanned_cells", []), 0.50),
            "scanned_cells_p95": _quantile(vals.get("scanned_cells", []), 0.95),
            "scanned_bytes_p50": _quantile(vals.get("scanned_bytes", []), 0.50),
            "scanned_bytes_p95": _quantile(vals.get("scanned_bytes", []), 0.95),
        }

    usa = summary_core["usa"]

    fieldnames = [
        "mode",
        "runs",
        "detect_p50_s",
        "detect_p95_s",
        "scanned_cells_p50",
        "scanned_cells_p95",
        "scanned_bytes_p50",
        "scanned_bytes_p95",
        "speedup_detect_p50_vs_usa",
        "speedup_detect_p95_vs_usa",
        "cells_reduction_pct_vs_usa",
        "bytes_reduction_pct_vs_usa",
    ]
    out_rows: List[Dict[str, str]] = []
    for mode in modes:
        cur = summary_core[mode]
        speedup_p50 = _safe_div(usa["detect_p50_s"], cur["detect_p50_s"])
        speedup_p95 = _safe_div(usa["detect_p95_s"], cur["detect_p95_s"])
        cells_red = (1.0 - _safe_div(cur["scanned_cells_p50"], usa["scanned_cells_p50"])) * 100.0
        bytes_red = (1.0 - _safe_div(cur["scanned_bytes_p50"], usa["scanned_bytes_p50"])) * 100.0

        out_rows.append(
            {
                "mode": mode,
                "runs": str(int(cur["runs"])),
                "detect_p50_s": _fmt(cur["detect_p50_s"]),
                "detect_p95_s": _fmt(cur["detect_p95_s"]),
                "scanned_cells_p50": _fmt(cur["scanned_cells_p50"]),
                "scanned_cells_p95": _fmt(cur["scanned_cells_p95"]),
                "scanned_bytes_p50": _fmt(cur["scanned_bytes_p50"]),
                "scanned_bytes_p95": _fmt(cur["scanned_bytes_p95"]),
                "speedup_detect_p50_vs_usa": _fmt(speedup_p50),
                "speedup_detect_p95_vs_usa": _fmt(speedup_p95),
                "cells_reduction_pct_vs_usa": _fmt(cells_red),
                "bytes_reduction_pct_vs_usa": _fmt(bytes_red),
            }
        )

    out_table_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_table_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in out_rows:
            writer.writerow(row)


def _next_run_id(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 1
    max_id = 0
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = str(row.get("run_id") or "").strip()
            if not raw:
                continue
            try:
                max_id = max(max_id, int(raw))
            except Exception:
                continue
    return max_id + 1


def main() -> None:
    args = parse_args()
    pred_dir = resolve_prediction_dir(Path(args.prediction_dir).resolve())
    out_csv = Path(args.out_csv).resolve()
    out_table_csv = Path(args.out_table_csv).resolve()
    modes = [m.lower() for m in parse_csv_strings(args.modes)]
    days = parse_csv_strings(args.days)

    invalid_modes = [m for m in modes if m not in {"usa", "utah"}]
    if invalid_modes:
        raise ValueError(f"Unsupported modes={invalid_modes}; expected subset of ['usa','utah'].")

    if not getattr(args, "append", False) and out_csv.exists():
        out_csv.unlink()

    ensure_header(out_csv, EXP_PUSHDOWN_COLUMNS)
    run_id = _next_run_id(out_csv)

    total_runs = len(days) * len(modes)
    done = 0
    failed = 0

    print(f"input_prediction_dir={pred_dir}")
    print(f"output_csv_file={out_csv.name}")
    print(f"output_table_file={out_table_csv.name}")
    print(
        "settings="
        f"days={len(days)} modes={modes} "
        f"threshold_field={args.threshold_field} threshold={args.threshold} "
        f"persistence_k={args.persistence_k} timestep_hours={args.timestep_hours}"
    )

    for day in days:
        for mode in modes:
            idx = done + failed + 1
            row: Dict[str, object] = {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "date": day,
                "region": mode,
                "mode": mode,
                "scanned_cells": "",
                "scanned_bytes": "",
                "records_in": "",
                "records_out": "",
                "detect_s": "",
                "total_s": "",
                "status": "failed",
                "error": "",
            }
            run_id += 1

            print(f"[run {idx}/{total_runs}] day={day} mode={mode}")
            t0 = time.perf_counter()
            try:
                prediction_file = find_prediction_file(
                    pred_dir=pred_dir,
                    day=day,
                    mode=mode,
                    lead_hours=int(args.lead_hours),
                )
                events, stats = timed_detect(
                    prediction_file,
                    threshold_field=str(args.threshold_field),
                    threshold=float(args.threshold),
                    persistence_k=int(args.persistence_k),
                    timestep_hours=int(args.timestep_hours),
                )
                row["records_out"] = int(stats.get("records_out", 0) or len(events))
                row["records_in"] = int(stats.get("records_in", 0) or 0)
                row["scanned_cells"] = int(stats.get("scanned_cells", 0) or 0)
                row["scanned_bytes"] = int(stats.get("scanned_bytes", 0) or 0)
                row["detect_s"] = round(float(stats.get("detect_s", 0.0) or 0.0), 6)
                row["status"] = "done"
                done += 1
            except Exception as exc:
                row["error"] = str(exc)
                failed += 1

            row["total_s"] = round(time.perf_counter() - t0, 6)
            append_row(out_csv, row)

    write_summary_table(out_csv=out_csv, out_table_csv=out_table_csv, modes=modes)
    print_run_qa(out_csv=out_csv, expected_days=days, expected_modes=modes)

    print(f"[done] total_runs={total_runs} done={done} failed={failed}")
    print(f"[ok] wrote {out_csv}")
    print(f"[ok] wrote {out_table_csv}")


if __name__ == "__main__":
    main()
