#!/usr/bin/env python3
"""
Shared experiment logging schemas and helpers.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Mapping, Sequence


SCHEMA_VERSION = "v2"


# EXP_RUNS field semantics:
# - records_in: input records read for the run (prediction rows in Phase 4 runner).
# - records_out: final control outputs produced by the run (selected sensor decisions).
# - event_count: detection-stage output count.
EXP_RUNS_COLUMNS = [
    "schema_version",
    "run_id",
    "date",
    "size",
    "k",
    "region",
    "selector",
    "discovery",
    "adapter_mode",
    "event_count",
    "candidate_count",
    "selected_count",
    "ingest_s",
    "detect_s",
    "discover_s",
    "select_s",
    "stream_s",
    "total_s",
    "bytes_in",
    "bytes_out",
    "records_in",
    "records_out",
    "scanned_cells",
    "scanned_bytes",
    "adapter_websocket_count",
    "adapter_rest_count",
    "adapter_csv_replay_count",
    "status",
    "error",
    "prediction_file",
    "cohort_file",
]

EXP_SELECTION_COLUMNS = [
    "schema_version",
    "run_id",
    "date",
    "size",
    "k",
    "region",
    "selector",
    "event_count",
    "candidate_count",
    "selected_count",
    "select_s",
    "total_s",
    "status",
    "error",
]

EXP_PUSHDOWN_COLUMNS = [
    "schema_version",
    "run_id",
    "date",
    "region",
    "mode",
    "scanned_cells",
    "scanned_bytes",
    "records_in",
    "records_out",
    "detect_s",
    "total_s",
    "status",
    "error",
]

EXP_BYTES_KAFKA_STORAGE_COLUMNS = [
    "schema_version",
    "run_id",
    "date",
    "region",
    "bytes_all",
    "bytes_orch",
    "delta_pct",
    "messages_per_s",
    "bytes_per_s",
    "topic_bytes_day",
    "status",
    "error",
]

EXP_CONCURRENCY_COLUMNS = [
    "schema_version",
    "run_id",
    "date",
    "region",
    "workers",
    "requests_total",
    "success_total",
    "error_total",
    "duration_s",
    "throughput_rps",
    "p50_s",
    "p95_s",
    "status",
    "error",
]


CANONICAL_SCHEMAS: Dict[str, Sequence[str]] = {
    "exp_runs.csv": EXP_RUNS_COLUMNS,
    "exp_selection.csv": EXP_SELECTION_COLUMNS,
    "exp_pushdown.csv": EXP_PUSHDOWN_COLUMNS,
    "exp_bytes_kafka_storage.csv": EXP_BYTES_KAFKA_STORAGE_COLUMNS,
    "exp_concurrency.csv": EXP_CONCURRENCY_COLUMNS,
}


def ensure_header(path: Path, columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(list(columns))
        return

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        existing = next(reader, [])
    if list(existing) != list(columns):
        raise ValueError(
            f"Header mismatch for {path}. Existing={existing}, Expected={list(columns)}"
        )


def append_row(path: Path, row: Mapping[str, object]) -> None:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        columns = next(reader, [])
    if not columns:
        raise ValueError(f"Missing header in {path}. Run ensure_header(...) first.")

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writerow({k: row.get(k, "") for k in columns})


def safe_rate(n: float, d: float) -> float:
    if d <= 0:
        return 0.0
    return float(n) / float(d)


def bytes_to_mb(n_bytes: float) -> float:
    return float(n_bytes) / (1024.0 * 1024.0)
