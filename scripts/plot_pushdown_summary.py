#!/usr/bin/env python3
"""
Regenerate the pushdown summary figure from pushdown_speedup_table.csv.

Input:  paper/results/pushdown_speedup_table.csv
Output: paper/figures/pushdown_summary.png
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.ticker import MultipleLocator

from _plot_style import C_PRIMARY, C_SECONDARY, apply_style


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _value_label(value: float, *, unit: str = "") -> str:
    if unit == "cells":
        return f"{int(round(value)):,}"
    if value >= 1.0:
        return f"{value:.2f} {unit}".strip()
    return f"{value:.3f} {unit}".strip()


def _annotation_text(value: float, suffix: str) -> str:
    return f"{int(round(value))}x {suffix}"


FIG_W_IN = 1021 / 300.0
FIG_H_IN = 766 / 300.0


def _save_exact(fig: plt.Figure, stem: str, fig_dir: Path) -> Path:
    png = fig_dir / f"{stem}.png"
    canvas = FigureCanvasAgg(fig)
    canvas.draw()
    canvas.print_png(png)
    plt.close(fig)
    print(f"output: {png}")
    return png


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary-csv", default="paper/results/pushdown_speedup_table.csv")
    parser.add_argument("--figures-dir", default="paper/figures")
    args = parser.parse_args()

    apply_style()

    csv_path = Path(args.summary_csv).resolve()
    fig_dir = Path(args.figures_dir).resolve()
    fig_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)
    _require_columns(
        df,
        [
            "mode",
            "scanned_cells_p50",
            "detect_p50_s",
            "cells_reduction_pct_vs_usa",
            "speedup_detect_p50_vs_usa",
        ],
    )

    rows = {str(row["mode"]).strip().lower(): row for _, row in df.iterrows()}
    if "usa" not in rows or "utah" not in rows:
        raise ValueError("Expected both 'usa' and 'utah' rows in pushdown summary CSV.")

    usa = rows["usa"]
    utah = rows["utah"]

    labels = ["CONUS\n(full grid)", "Utah\n(subset)"]
    y = [1, 0]
    colors = [C_PRIMARY, C_SECONDARY]

    scanned_cells = [float(usa["scanned_cells_p50"]), float(utah["scanned_cells_p50"])]
    detect_p50 = [float(usa["detect_p50_s"]), float(utah["detect_p50_s"])]
    reduction_x = float(100.0 / (100.0 - float(utah["cells_reduction_pct_vs_usa"])))
    faster_x = float(utah["speedup_detect_p50_vs_usa"])

    fig, axes = plt.subplots(2, 1, figsize=(FIG_W_IN, FIG_H_IN), dpi=300)
    fig.subplots_adjust(hspace=0.60, left=0.19, right=0.98, top=0.97, bottom=0.14)

    panels = [
        (
            axes[0],
            scanned_cells,
            "Scanned cells",
            _annotation_text(reduction_x, "reduction"),
            "cells",
        ),
        (
            axes[1],
            detect_p50,
            "Detection latency (s)",
            _annotation_text(faster_x, "faster"),
            "s",
        ),
    ]

    for ax, values, xlabel, annotation, unit in panels:
        ax.barh(y, values, color=colors, height=0.40)
        ax.set_yticks(y)
        ax.set_yticklabels(labels)
        ax.set_xlabel(xlabel)
        ax.grid(True, axis="x")
        ax.grid(True, axis="y")
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_linewidth(0.8)
        ax.spines["bottom"].set_linewidth(0.8)
        ax.tick_params(axis="y", pad=6)

        xmax = max(values) * 1.50
        ax.set_xlim(0.0, xmax)

        for yi, value in zip(y, values):
            ax.text(
                value + xmax * 0.015,
                yi,
                _value_label(value, unit=unit),
                va="center",
                ha="left",
                fontsize=8,
            )

        ax.text(
            xmax * 0.36,
            0.5,
            annotation,
            ha="center",
            va="center",
            color=C_PRIMARY,
            weight="bold",
            fontsize=8.8,
            bbox={
                "boxstyle": "round,pad=0.25",
                "facecolor": "white",
                "edgecolor": "#cccccc",
                "linewidth": 0.8,
            },
        )

    axes[0].xaxis.set_major_locator(MultipleLocator(2000))
    axes[1].xaxis.set_major_locator(MultipleLocator(2))

    _save_exact(fig, "pushdown_summary", fig_dir)


if __name__ == "__main__":
    main()
