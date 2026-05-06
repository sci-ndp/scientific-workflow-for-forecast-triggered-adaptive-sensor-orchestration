#!/usr/bin/env python3
"""
Selection comparison runner (Phase 5): linear vs KD-tree.

Produces:
  - paper/results/exp_selection.csv
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute KD-tree vs linear selection summary from run-level CSVs."
    )
    parser.add_argument(
        "--selection-stats",
        action="store_true",
        help="Compatibility flag from previous workflow (optional no-op).",
    )
    parser.add_argument(
        "--linear-runs-csv",
        default="paper/results/exp_runs.csv",
        help="Run-level CSV for linear selector baseline.",
    )
    parser.add_argument(
        "--kdtree-runs-csv",
        default="paper/results/exp_runs_kdtree.csv",
        help="Run-level CSV for KD-tree selector runs.",
    )
    parser.add_argument(
        "--selection-out-csv",
        default="paper/results/exp_selection.csv",
        help="Output CSV path for per-(size,k) comparison summary.",
    )
    return parser.parse_args()


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


def _parse_float(value: str) -> float | None:
    raw = str(value or "").strip()
    if raw == "":
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _load_runs(path: Path, selector_override: str) -> List[Tuple[int, int, float, float]]:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    rows: List[Tuple[int, int, float, float]] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"size", "k", "select_s", "total_s", "status"}
        fields = set(reader.fieldnames or [])
        missing = sorted(required - fields)
        if missing:
            raise ValueError(f"{path} missing required columns: {missing}")

        for row in reader:
            status = str(row.get("status") or "").strip().lower()
            if status != "done":
                continue
            size = _parse_float(row.get("size", ""))
            k = _parse_float(row.get("k", ""))
            select_s = _parse_float(row.get("select_s", ""))
            total_s = _parse_float(row.get("total_s", ""))
            if None in (size, k, select_s, total_s):
                continue
            rows.append((int(size), int(k), float(select_s), float(total_s)))

    if not rows:
        raise ValueError(f"No valid status=done rows in {path} for selector={selector_override}")
    return rows


def _summarize(rows: Iterable[Tuple[int, int, float, float]]) -> Dict[Tuple[int, int], Dict[str, float]]:
    grouped: Dict[Tuple[int, int], Dict[str, List[float]]] = defaultdict(
        lambda: {"select_s": [], "total_s": []}
    )
    for size, k, select_s, total_s in rows:
        grouped[(size, k)]["select_s"].append(select_s)
        grouped[(size, k)]["total_s"].append(total_s)

    out: Dict[Tuple[int, int], Dict[str, float]] = {}
    for key, data in grouped.items():
        select_vals = data["select_s"]
        total_vals = data["total_s"]
        out[key] = {
            "runs": float(len(total_vals)),
            "select_p50_s": _quantile(select_vals, 0.50),
            "select_p95_s": _quantile(select_vals, 0.95),
            "total_p50_s": _quantile(total_vals, 0.50),
            "total_p95_s": _quantile(total_vals, 0.95),
        }
    return out


def _pct_delta(new: float, old: float) -> float:
    if old == 0.0:
        return float("nan")
    return (new / old - 1.0) * 100.0


def _pct_reduction(old: float, new: float) -> float:
    if old == 0.0:
        return float("nan")
    return (old - new) / old * 100.0


def _speedup(old: float, new: float) -> float:
    if new == 0.0:
        return float("nan")
    return old / new


def _fmt(value: float) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return f"{float(value):.15g}"


def compute_selection_stats(*, linear_csv: Path, kdtree_csv: Path, out_csv: Path) -> None:
    linear_rows = _load_runs(linear_csv, selector_override="linear")
    kdtree_rows = _load_runs(kdtree_csv, selector_override="kdtree")

    linear_sum = _summarize(linear_rows)
    kdtree_sum = _summarize(kdtree_rows)

    lin_keys = set(linear_sum.keys())
    kd_keys = set(kdtree_sum.keys())
    only_linear = sorted(lin_keys - kd_keys)
    only_kdtree = sorted(kd_keys - lin_keys)
    if only_linear or only_kdtree:
        raise ValueError(
            "Mismatched (size,k) coverage between linear and kdtree summaries. "
            f"only_linear={only_linear} only_kdtree={only_kdtree}"
        )

    fieldnames = [
        "size",
        "k",
        "runs_linear",
        "select_p50_linear_s",
        "select_p95_linear_s",
        "total_p50_linear_s",
        "total_p95_linear_s",
        "runs_kdtree",
        "select_p50_kdtree_s",
        "select_p95_kdtree_s",
        "total_p50_kdtree_s",
        "total_p95_kdtree_s",
        "delta_select_p50_s",
        "delta_select_p95_s",
        "delta_total_p50_s",
        "delta_total_p95_s",
        "delta_select_p50_pct",
        "delta_select_p95_pct",
        "delta_total_p50_pct",
        "delta_total_p95_pct",
        "speedup_select_p50_x",
        "speedup_select_p95_x",
        "speedup_total_p50_x",
        "speedup_total_p95_x",
    ]

    rows_out: List[Dict[str, str]] = []
    for size, k in sorted(lin_keys):
        lin = linear_sum[(size, k)]
        kd = kdtree_sum[(size, k)]

        # Use "linear - KD-tree" so positive values mean KD-tree is faster.
        delta_select_p50 = lin["select_p50_s"] - kd["select_p50_s"]
        delta_select_p95 = lin["select_p95_s"] - kd["select_p95_s"]
        delta_total_p50 = lin["total_p50_s"] - kd["total_p50_s"]
        delta_total_p95 = lin["total_p95_s"] - kd["total_p95_s"]

        row = {
            "size": str(size),
            "k": str(k),
            "runs_linear": str(int(lin["runs"])),
            "select_p50_linear_s": _fmt(lin["select_p50_s"]),
            "select_p95_linear_s": _fmt(lin["select_p95_s"]),
            "total_p50_linear_s": _fmt(lin["total_p50_s"]),
            "total_p95_linear_s": _fmt(lin["total_p95_s"]),
            "runs_kdtree": str(int(kd["runs"])),
            "select_p50_kdtree_s": _fmt(kd["select_p50_s"]),
            "select_p95_kdtree_s": _fmt(kd["select_p95_s"]),
            "total_p50_kdtree_s": _fmt(kd["total_p50_s"]),
            "total_p95_kdtree_s": _fmt(kd["total_p95_s"]),
            "delta_select_p50_s": _fmt(delta_select_p50),
            "delta_select_p95_s": _fmt(delta_select_p95),
            "delta_total_p50_s": _fmt(delta_total_p50),
            "delta_total_p95_s": _fmt(delta_total_p95),
            "delta_select_p50_pct": _fmt(_pct_reduction(lin["select_p50_s"], kd["select_p50_s"])),
            "delta_select_p95_pct": _fmt(_pct_reduction(lin["select_p95_s"], kd["select_p95_s"])),
            "delta_total_p50_pct": _fmt(_pct_reduction(lin["total_p50_s"], kd["total_p50_s"])),
            "delta_total_p95_pct": _fmt(_pct_reduction(lin["total_p95_s"], kd["total_p95_s"])),
            "speedup_select_p50_x": _fmt(_speedup(lin["select_p50_s"], kd["select_p50_s"])),
            "speedup_select_p95_x": _fmt(_speedup(lin["select_p95_s"], kd["select_p95_s"])),
            "speedup_total_p50_x": _fmt(_speedup(lin["total_p50_s"], kd["total_p50_s"])),
            "speedup_total_p95_x": _fmt(_speedup(lin["total_p95_s"], kd["total_p95_s"])),
        }
        rows_out.append(row)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows_out:
            w.writerow(row)

    sizes = sorted({int(r["size"]) for r in rows_out})
    ks = sorted({int(r["k"]) for r in rows_out})
    print(f"[ok] wrote {len(rows_out)} rows: {out_csv}")
    print(f"[ok] sizes={sizes}")
    print(f"[ok] k={ks}")


def main() -> None:
    args = parse_args()
    compute_selection_stats(
        linear_csv=Path(args.linear_runs_csv).resolve(),
        kdtree_csv=Path(args.kdtree_runs_csv).resolve(),
        out_csv=Path(args.selection_out_csv).resolve(),
    )


if __name__ == "__main__":
    main()
