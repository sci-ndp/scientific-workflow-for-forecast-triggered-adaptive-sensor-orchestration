#!/usr/bin/env python3
"""
Linear vs KD-tree selection comparison (2x2 heatmaps).

Input:  paper/results/exp_selection.csv
Output: paper/figures/linear_vs_kdtree_selection_clean.{svg,png}
"""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from _plot_style import apply_style, save, COL_WIDTH, BLUE_CMAP


def _grid(df, metric, sizes, ks):
    pivot = df.pivot(index="size", columns="k", values=metric)
    return pivot.reindex(index=sizes, columns=ks).to_numpy(float)


def _panel(ax, data, subtitle, xlabels, ylabels, cmap, vmin=None, vmax=None):
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        im = ax.imshow(np.zeros_like(data), aspect="auto", cmap=cmap, origin="upper")
    else:
        if vmin is None:
            vmin = float(np.nanmin(finite))
        if vmax is None:
            vmax = float(np.nanmax(finite))
        if np.isclose(vmin, vmax):
            pad = 1e-6 if np.isclose(vmin, 0) else abs(vmin) * 0.05
            vmin -= pad
            vmax += pad
        im = ax.imshow(data, aspect="auto", cmap=cmap, origin="upper",
                        vmin=vmin, vmax=vmax)

    ax.set_title(subtitle, fontsize=8, pad=4, loc="left")
    ax.set_xticks(range(len(xlabels)))
    ax.set_xticklabels([str(v) for v in xlabels])
    ax.set_yticks(range(len(ylabels)))
    ax.set_yticklabels([str(v) for v in ylabels])
    ax.spines["top"].set_visible(True)
    ax.spines["right"].set_visible(True)
    ax.tick_params(length=2)
    return im


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="paper/results/exp_selection.csv")
    ap.add_argument("--figures-dir", default="paper/figures")
    args = ap.parse_args()

    apply_style()

    csv_path = Path(args.csv).resolve()
    fig_dir = Path(args.figures_dir).resolve()
    fig_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)
    for c in ["size", "k"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["size"] = df["size"].astype(int)
    df["k"] = df["k"].astype(int)

    sizes = sorted(df["size"].unique())
    ks = sorted(df["k"].unique())

    m1 = _grid(df, "speedup_select_p50_x", sizes, ks)
    m2 = _grid(df, "speedup_select_p95_x", sizes, ks)
    # CSV now stores linear - kdtree, so positive already means KD-tree faster.
    m3 = _grid(df, "delta_total_p50_s", sizes, ks)
    m4 = _grid(df, "delta_total_p95_s", sizes, ks)

    sp_min = float(np.nanmin([np.nanmin(m1), np.nanmin(m2)]))
    sp_max = float(np.nanmax([np.nanmax(m1), np.nanmax(m2)]))
    dt_min = float(np.nanmin([np.nanmin(m3), np.nanmin(m4)]))
    dt_max = float(np.nanmax([np.nanmax(m3), np.nanmax(m4)]))

    fig, axes = plt.subplots(2, 2, figsize=(COL_WIDTH, 3.4))
    fig.subplots_adjust(hspace=0.48, wspace=0.10, left=0.14, right=0.86,
                        top=0.94, bottom=0.11)

    im1 = _panel(axes[0, 0], m1, "(a) p50 speedup (×)",
                 ks, sizes, BLUE_CMAP, sp_min, sp_max)
    im2 = _panel(axes[0, 1], m2, "(b) p95 speedup (×)",
                 ks, sizes, BLUE_CMAP, sp_min, sp_max)
    im3 = _panel(axes[1, 0], m3, "(c) p50 reduction (s)",
                 ks, sizes, BLUE_CMAP, dt_min, dt_max)
    im4 = _panel(axes[1, 1], m4, "(d) p95 reduction (s)",
                 ks, sizes, BLUE_CMAP, dt_min, dt_max)

    axes[0, 0].set_ylabel("Cohort size")
    axes[1, 0].set_ylabel("Cohort size")
    axes[1, 0].set_xlabel("Persistence $k$")
    axes[1, 1].set_xlabel("Persistence $k$")
    axes[0, 0].set_xlabel("")
    axes[0, 1].set_xlabel("")
    axes[0, 1].set_ylabel("")
    axes[1, 1].set_ylabel("")
    axes[0, 1].set_yticklabels([])
    axes[1, 1].set_yticklabels([])

    cb1 = fig.colorbar(im1, ax=axes[0, :].tolist(), fraction=0.035, pad=0.03)
    cb1.ax.tick_params(labelsize=6.5)
    cb2 = fig.colorbar(im3, ax=axes[1, :].tolist(), fraction=0.035, pad=0.03)
    cb2.ax.tick_params(labelsize=6.5)

    save(fig, "linear_vs_kdtree_selection_clean", fig_dir)


if __name__ == "__main__":
    main()
