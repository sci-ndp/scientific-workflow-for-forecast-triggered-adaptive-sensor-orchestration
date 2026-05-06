#!/usr/bin/env python3
"""
Regenerate scalability Figure 2 (p50/p95 + bootstrap CI) and Figure 3 (ECDF).

Visual style matches the existing paper assets (Times New Roman, Blues palette).
Figure 2 uses evenly spaced discrete cohort positions (not log-scaled sensor count) so
tick labels stay readable; Figure 3 uses step ECDFs.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]

# Extracted from paper/figures/scaling_latency_vs_sensor_count_ci.svg (Apr 2026 build)
CI_COLOR_P50 = "#2171b5"
CI_COLOR_P95 = "#6baed6"
CI_LINEWIDTH = 1.4

# Sequential Blues for cohort lines (scaling_total_latency_ecdf.svg), sizes 8→2365
ECDF_COLORS = ["#d6e6f4", "#abd0e6", "#6aaed6", "#3787c0", "#105ba4"]

# Figure size ≈ 245.28 × 169.68 pt (IEEE column-friendly)
FIG_W_IN = 245.28 / 72.0
FIG_H_IN = 169.68 / 72.0


def _apply_matplotlib_style() -> None:
    mpl.rcParams.update(
        {
            "figure.dpi": 150,
            "savefig.dpi": 150,
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "Nimbus Roman", "DejaVu Serif"],
            "font.size": 8,
            "axes.titlesize": 9,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "axes.edgecolor": "#cccccc",
            "axes.linewidth": 0.6,
            "axes.labelcolor": "#262626",
            "xtick.color": "#262626",
            "ytick.color": "#262626",
            "grid.color": "#cccccc",
            "grid.alpha": 0.3,
            "grid.linestyle": (0, (1.48, 0.64)),
            "grid.linewidth": 0.4,
        }
    )


def plot_ci(ci_csv: Path, out_base: Path) -> None:
    _apply_matplotlib_style()
    ci = pd.read_csv(ci_csv).sort_values("size")
    sizes = ci["size"].to_numpy(dtype=float)
    p50 = ci["total_p50_s"].to_numpy()
    p95 = ci["total_p95_s"].to_numpy()
    p50_lo, p50_hi = ci["total_p50_lo_s"].to_numpy(), ci["total_p50_hi_s"].to_numpy()
    p95_lo, p95_hi = ci["total_p95_lo_s"].to_numpy(), ci["total_p95_hi_s"].to_numpy()
    yerr_p50 = np.vstack([p50 - p50_lo, p50_hi - p50])
    yerr_p95 = np.vstack([p95 - p95_lo, p95_hi - p95])

    # Evenly space discrete cohorts on x (avoids log-scale crowding at 681 vs 1000).
    x = np.arange(len(sizes), dtype=float)
    labels = [str(int(s)) for s in sizes]

    fig, ax = plt.subplots(figsize=(FIG_W_IN, FIG_H_IN), constrained_layout=True)

    ax.errorbar(
        x,
        p50,
        yerr=yerr_p50,
        fmt="-o",
        color=CI_COLOR_P50,
        ecolor=CI_COLOR_P50,
        elinewidth=CI_LINEWIDTH,
        capsize=3,
        linewidth=CI_LINEWIDTH,
        markersize=4,
        label="p50 (95 % CI)",
        zorder=3,
    )
    ax.errorbar(
        x,
        p95,
        yerr=yerr_p95,
        fmt="-s",
        color=CI_COLOR_P95,
        ecolor=CI_COLOR_P95,
        elinewidth=CI_LINEWIDTH,
        capsize=3,
        linewidth=CI_LINEWIDTH,
        markersize=4,
        label="p95 (95 % CI)",
        zorder=3,
    )

    ax.set_xlabel("Sensor cohort size")
    ax.set_ylabel("End-to-end latency (s)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlim(x[0] - 0.35, x[-1] + 0.35)
    ax.grid(True)
    ax.legend(loc="upper left", framealpha=0.95)

    for ext in (".svg", ".png"):
        fig.savefig(out_base.with_suffix(ext))
    plt.close(fig)


def plot_ecdf(runs_csv: Path, out_base: Path) -> None:
    _apply_matplotlib_style()
    df = pd.read_csv(runs_csv)
    df = df[(df["status"] == "done") & (df["selector"] == "linear")].copy()
    df["total_s"] = pd.to_numeric(df["total_s"], errors="coerce")
    df = df.dropna(subset=["size", "total_s"])

    order = sorted(df["size"].unique())
    color_by_size = {sz: ECDF_COLORS[i % len(ECDF_COLORS)] for i, sz in enumerate(order)}

    fig, ax = plt.subplots(figsize=(FIG_W_IN, FIG_H_IN), constrained_layout=True)

    for size in order:
        vals = np.sort(df.loc[df["size"] == size, "total_s"].to_numpy(dtype=float))
        if vals.size == 0:
            continue
        y = np.arange(1, vals.size + 1) / float(vals.size)
        x = np.concatenate([[vals[0]], vals])
        y = np.concatenate([[0.0], y])
        ax.step(
            x,
            y,
            where="post",
            color=color_by_size[size],
            linewidth=1.35,
            label=str(int(size)),
            solid_capstyle="round",
        )

    ax.set_xlabel("End-to-end latency (s)")
    ax.set_ylabel("ECDF")
    ax.grid(True)
    leg = ax.legend(
        title="Size",
        loc="lower right",
        framealpha=0.95,
        ncol=1,
        fontsize=8,
    )
    leg.get_title().set_fontsize(9.6)

    for ext in (".svg", ".png"):
        fig.savefig(out_base.with_suffix(ext))
    plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--runs", type=Path, default=REPO / "paper/results/exp_runs.csv")
    p.add_argument("--ci", type=Path, default=REPO / "paper/results/scaling_ci_summary.csv")
    p.add_argument("--out-dir", type=Path, default=REPO / "paper/figures")
    args = p.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    plot_ci(args.ci, args.out_dir / "scaling_latency_vs_sensor_count_ci")
    plot_ecdf(args.runs, args.out_dir / "scaling_total_latency_ecdf")
    print(f"[ok] wrote CI + ECDF (PNG/SVG) under {args.out_dir}")


if __name__ == "__main__":
    main()
