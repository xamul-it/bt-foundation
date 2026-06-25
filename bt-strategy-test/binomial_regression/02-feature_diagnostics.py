#!/usr/bin/env python3
"""Feature diagnostics for event-study outputs.

Generates:
- feature range/distribution summaries
- feature correlation matrices
- distribution and correlation plots
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


LOG = logging.getLogger("feature_diagnostics")

OUTCOME_ORDER = [
    "close_ret_5m",
    "high_ret_max_5m",
    "low_ret_min_5m",
    "close_ret_10m",
    "high_ret_max_10m",
    "low_ret_min_10m",
    "close_ret_15m",
    "high_ret_max_15m",
    "low_ret_min_15m",
    "close_ret_eod",
    "high_ret_max_eod",
    "low_ret_min_eod",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze feature distributions and correlations.")
    parser.add_argument(
        "--input-glob",
        default="results/*_feature_outcome_full.csv",
        help="Glob pattern for event-study CSV files, relative to script directory.",
    )
    parser.add_argument(
        "--output-dir",
        default="results/diagnostics",
        help="Output directory for reports and plots, relative to script directory.",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=0,
        help="Histogram bin count. If 0, uses Freedman-Diaconis adaptive bins.",
    )
    parser.add_argument(
        "--corr-method",
        choices=["pearson", "spearman", "kendall"],
        default="spearman",
        help="Correlation method.",
    )
    parser.add_argument(
        "--include-strategy-views",
        action="store_true",
        help="Also export strategy-split summaries and correlations.",
    )
    parser.add_argument(
        "--include-dispersion",
        action="store_true",
        help="Include dispersion features (disp_*). By default they are excluded.",
    )
    parser.add_argument(
        "--include-accz5",
        action="store_true",
        help="Include accZ_5 in feature correlation/distribution analysis.",
    )
    parser.add_argument(
        "--include-volz",
        action="store_true",
        help="Include volZ in feature reports. By default it is excluded.",
    )
    parser.add_argument(
        "--zoom-q-low",
        type=float,
        default=0.01,
        help="Lower quantile for zoomed histogram view.",
    )
    parser.add_argument(
        "--zoom-q-high",
        type=float,
        default=0.99,
        help="Upper quantile for zoomed histogram view.",
    )
    parser.add_argument(
        "--close-threshold-bps",
        type=float,
        default=5.0,
        help="Threshold (bps) for close_ret_* success probability analysis.",
    )
    parser.add_argument(
        "--high-threshold-bps",
        type=float,
        default=10.0,
        help="Threshold (bps) for high_ret_max_* success probability analysis.",
    )
    parser.add_argument(
        "--low-threshold-bps",
        type=float,
        default=10.0,
        help="Threshold (bps) for low_ret_min_* success probability analysis (uses negative threshold).",
    )
    return parser.parse_args()


def infer_asset_strategy(path: Path) -> tuple[str, str]:
    # Expected:
    # - AAPL_breakout_test_event_study.csv
    # - AAPL_feature_outcome_full.csv
    match = re.match(r"^([^_]+)_([^_]+)_test_event_study\.csv$", path.name)
    if not match:
        match_full = re.match(r"^([^_]+)_feature_outcome_full\.csv$", path.name)
        if not match_full:
            return "unknown", "full"
        return match_full.group(1), "full"
    return match.group(1), match.group(2)


def load_event_files(files: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for file in files:
        asset, strategy = infer_asset_strategy(file)
        df = pd.read_csv(file)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df["asset"] = asset
        df["strategy"] = strategy
        df["source_file"] = file.name
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def feature_columns(
    df: pd.DataFrame,
    include_dispersion: bool = False,
    include_accz5: bool = False,
    include_volz: bool = False,
) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if col.startswith("slope_"):
            cols.append(col)
        elif col.startswith("accZ_"):
            if col != "accZ_5" or include_accz5:
                cols.append(col)
        elif include_volz and col == "volZ":
            cols.append(col)
        elif include_dispersion and col.startswith("disp_"):
            cols.append(col)
    return cols


def outcome_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if col.startswith(("close_ret_", "high_ret_max_", "low_ret_min_")):
            cols.append(col)
    return cols


def ordered_outcomes(out_cols: list[str]) -> list[str]:
    base = [c for c in OUTCOME_ORDER if c in out_cols]
    extras = sorted([c for c in out_cols if c not in OUTCOME_ORDER])
    return base + extras


def outcome_group_palette(outcomes: list[str]) -> dict[str, str]:
    # Shades by horizon: 5m, 10m, 15m, eod
    high_shades = ["#2E7D32", "#43A047", "#66BB6A", "#A5D6A7"]  # green tones
    low_shades = ["#B71C1C", "#D32F2F", "#EF5350", "#FFCDD2"]   # red tones
    close_shades = ["#424242", "#616161", "#9E9E9E", "#BDBDBD"] # gray tones

    horizon_idx = {"5m": 0, "10m": 1, "15m": 2, "eod": 3}

    palette: dict[str, str] = {}
    for out in outcomes:
        if out.endswith("_5m"):
            idx = horizon_idx["5m"]
        elif out.endswith("_10m"):
            idx = horizon_idx["10m"]
        elif out.endswith("_15m"):
            idx = horizon_idx["15m"]
        elif out.endswith("_eod"):
            idx = horizon_idx["eod"]
        else:
            idx = 0

        if out.startswith("high_ret_max"):
            palette[out] = high_shades[idx]
        elif out.startswith("low_ret_min"):
            palette[out] = low_shades[idx]
        else:
            palette[out] = close_shades[idx]
    return palette


def add_binary_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    req = {"slope_5", "slope_20", "slope_60", "accZ_20"}
    if not req.issubset(out.columns):
        missing = sorted(req - set(out.columns))
        raise RuntimeError(f"Missing columns for binary indicators: {missing}")

    s5 = np.sign(out["slope_5"])
    s20 = np.sign(out["slope_20"])
    s60 = np.sign(out["slope_60"])
    a20 = np.sign(out["accZ_20"])

    out["ind_trend_alignment"] = ((s5 == s20) & (s20 == s60) & (s5 != 0)).astype(int)
    out["ind_curvature_mismatch_20"] = ((a20 != s20) & (a20 != 0) & (s20 != 0)).astype(int)
    ratio = out["slope_5"].abs() / (out["slope_60"].abs() + 1e-12)
    out["ind_local_slope_dominance"] = (ratio > 1.0).astype(int)
    out["slope_ratio_5_over_60"] = ratio
    return out


def _outcome_success(y: pd.Series, col: str, close_th: float, high_th: float, low_th: float) -> pd.Series:
    if col.startswith("close_ret_"):
        return (y >= close_th).astype(int)
    if col.startswith("high_ret_max_"):
        return (y >= high_th).astype(int)
    if col.startswith("low_ret_min_"):
        return (y <= -low_th).astype(int)
    raise ValueError(f"Unsupported outcome column: {col}")


def binary_indicator_analysis(
    df: pd.DataFrame,
    indicators: list[str],
    out_cols: list[str],
    close_th: float,
    high_th: float,
    low_th: float,
) -> pd.DataFrame:
    rows: list[dict] = []
    for ind in indicators:
        x = df[ind]
        for col in out_cols:
            y = df[col]
            mask = x.notna() & y.notna()
            if mask.sum() < 10:
                continue

            xv = x[mask].astype(int)
            yv = y[mask].astype(float)
            on = xv == 1
            off = xv == 0
            if on.sum() < 5 or off.sum() < 5:
                continue

            pb_corr = xv.corr(yv, method="pearson")
            mean_on = yv[on].mean()
            mean_off = yv[off].mean()
            mean_diff = mean_on - mean_off

            succ = _outcome_success(yv, col, close_th, high_th, low_th)
            p_on = succ[on].mean()
            p_off = succ[off].mean()
            lift = p_on / p_off if p_off > 0 else np.nan

            a = succ[on].sum()
            b = on.sum() - a
            c = succ[off].sum()
            d = off.sum() - c
            odds_ratio = ((a + 0.5) * (d + 0.5)) / ((b + 0.5) * (c + 0.5))

            rows.append(
                {
                    "indicator": ind,
                    "outcome": col,
                    "n_total": int(mask.sum()),
                    "n_on": int(on.sum()),
                    "n_off": int(off.sum()),
                    "on_rate": float(on.mean()),
                    "point_biserial_corr": pb_corr,
                    "mean_on": mean_on,
                    "mean_off": mean_off,
                    "mean_diff_on_minus_off": mean_diff,
                    "p_success_on": p_on,
                    "p_success_off": p_off,
                    "lift_on_vs_off": lift,
                    "odds_ratio_on_vs_off": odds_ratio,
                }
            )
    return pd.DataFrame(rows)


def feature_slice_analysis(
    df: pd.DataFrame,
    feature_cols: list[str],
    out_cols: list[str],
    close_th: float,
    high_th: float,
    low_th: float,
    n_slices: int = 10,
) -> pd.DataFrame:
    rows: list[dict] = []
    for feat in feature_cols:
        if feat not in df.columns:
            continue
        x = df[feat]
        valid_x = x.dropna()
        if len(valid_x) < 100:
            continue
        try:
            # Equal-frequency slicing; tails can be wider in value-space.
            cats, bins = pd.qcut(valid_x, q=n_slices, labels=False, retbins=True, duplicates="drop")
        except ValueError:
            continue

        n_eff = len(bins) - 1
        if n_eff < 2:
            continue

        slice_col = pd.Series(np.nan, index=df.index, dtype="float64")
        slice_col.loc[valid_x.index] = cats.astype(float)
        for out in out_cols:
            y = df[out]
            mask = slice_col.notna() & y.notna()
            if mask.sum() < 20:
                continue
            xv = slice_col[mask].astype(int)
            yv = y[mask].astype(float)

            for s in range(n_eff):
                in_slice = xv == s
                out_slice = xv != s
                if in_slice.sum() < 5 or out_slice.sum() < 5:
                    continue

                ys = yv[in_slice]
                xs = x[mask][in_slice].astype(float)
                pb = in_slice.astype(int).corr(yv, method="pearson")
                spearman_in_slice = xs.corr(ys, method="spearman") if xs.nunique() > 1 and ys.nunique() > 1 else np.nan
                spearman_global = x[mask].astype(float).corr(yv, method="spearman")
                succ = _outcome_success(yv, out, close_th, high_th, low_th)
                p_in = succ[in_slice].mean()
                p_out = succ[out_slice].mean()
                lift = p_in / p_out if p_out > 0 else np.nan

                rows.append(
                    {
                        "feature": feat,
                        "outcome": out,
                        "slice_id": s + 1,
                        "slice_low": bins[s],
                        "slice_high": bins[s + 1],
                        "n_total": int(mask.sum()),
                        "n_slice": int(in_slice.sum()),
                        "slice_rate": float(in_slice.mean()),
                        "point_biserial_corr_slice_vs_outcome": pb,
                        "spearman_corr_in_slice": spearman_in_slice,
                        "spearman_corr_global_feature_vs_outcome": spearman_global,
                        "mean_outcome_in_slice": ys.mean(),
                        "median_outcome_in_slice": ys.median(),
                        "std_outcome_in_slice": ys.std(),
                        "p_success_in_slice": p_in,
                        "p_success_outside_slice": p_out,
                        "lift_slice_vs_outside": lift,
                    }
                )
    return pd.DataFrame(rows)


def build_slice_threshold_table(slice_df: pd.DataFrame) -> pd.DataFrame:
    if slice_df.empty:
        return pd.DataFrame()
    cols = ["feature", "slice_id", "slice_low", "slice_high", "slice_rate", "n_slice"]
    out = (
        slice_df[cols]
        .drop_duplicates(subset=["feature", "slice_id"])
        .sort_values(["feature", "slice_id"])
        .reset_index(drop=True)
    )
    return out


def plot_slice_correlation_by_feature(
    slice_df: pd.DataFrame,
    out_dir: Path,
    prefix: str,
) -> None:
    if slice_df.empty:
        return
    sns.set_theme(style="whitegrid")
    for feat, gdf in slice_df.groupby("feature", sort=True):
        plot_df = (
            gdf[["feature", "outcome", "slice_id", "spearman_corr_in_slice", "spearman_corr_global_feature_vs_outcome"]]
            .dropna()
            .copy()
        )
        if plot_df.empty:
            continue
        bounds = (
            gdf[["slice_id", "slice_low", "slice_high"]]
            .dropna()
            .drop_duplicates(subset=["slice_id"])
            .sort_values("slice_id")
        )
        hue_order = ordered_outcomes(plot_df["outcome"].dropna().unique().tolist())
        palette = outcome_group_palette(hue_order)
        fig, ax = plt.subplots(figsize=(13.5, 6))
        sns.lineplot(
            data=plot_df,
            x="slice_id",
            y="spearman_corr_in_slice",
            hue="outcome",
            hue_order=hue_order,
            palette=palette,
            marker="o",
            ax=ax,
        )
        ax.axhline(0.0, color="black", linewidth=1, alpha=0.5)
        ax.set_title(f"{feat}: Spearman rho in each slice vs outcome")
        ax.set_xlabel("Slice (quantile bin 1..10)")
        ax.set_ylabel("Spearman rho")
        ax.set_xticks(sorted(plot_df["slice_id"].unique()))
        ax.legend(title="Outcome", loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8, borderaxespad=0.0)

        # Add slice-threshold reference box (1..10 -> [low, high])
        if not bounds.empty:
            lines = ["Slice thresholds:"]
            for _, r in bounds.iterrows():
                sid = int(r["slice_id"])
                lo = r["slice_low"]
                hi = r["slice_high"]
                lines.append(f"{sid}: [{lo:.4g}, {hi:.4g}]")
            txt = "\n".join(lines)
            ax.text(
                1.01,
                0.02,
                txt,
                transform=ax.transAxes,
                fontsize=7.5,
                va="bottom",
                ha="left",
                bbox=dict(boxstyle="round", facecolor="white", alpha=0.9, edgecolor="#999999"),
            )
        fig.tight_layout()
        fig.savefig(out_dir / f"{prefix}_{feat}.png", dpi=150)
        plt.close(fig)


def plot_spearman_slice_matrix_by_feature(
    slice_df: pd.DataFrame,
    out_dir: Path,
    prefix: str,
) -> None:
    if slice_df.empty:
        return
    sns.set_theme(style="whitegrid")
    for feat, gdf in slice_df.groupby("feature", sort=True):
        plot_df = gdf[
            [
                "slice_id",
                "slice_low",
                "slice_high",
                "outcome",
                "spearman_corr_in_slice",
            ]
        ].dropna(subset=["slice_id", "outcome"])
        if plot_df.empty:
            continue

        mat = (
            plot_df.pivot_table(
                index="slice_id",
                columns="outcome",
                values="spearman_corr_in_slice",
                aggfunc="mean",
            )
            .sort_index()
        )
        if mat.empty:
            continue
        ordered_cols = [c for c in ordered_outcomes(mat.columns.tolist()) if c in mat.columns]
        mat = mat[ordered_cols]

        bounds = (
            plot_df.groupby("slice_id")[["slice_low", "slice_high"]]
            .first()
            .sort_index()
        )
        y_labels = []
        for sid in mat.index:
            lo = bounds.loc[sid, "slice_low"]
            hi = bounds.loc[sid, "slice_high"]
            y_labels.append(f"{int(sid)} [{lo:.4g}, {hi:.4g}]")

        vals = mat.to_numpy(dtype=float)
        finite = vals[np.isfinite(vals)]
        if finite.size == 0:
            vmin, vmax = -0.1, 0.1
        else:
            vmin = float(finite.min())
            vmax = float(finite.max())
            if np.isclose(vmin, vmax):
                eps = max(1e-6, abs(vmin) * 0.1 + 1e-6)
                vmin -= eps
                vmax += eps

        fig, ax = plt.subplots(figsize=(1.0 * mat.shape[1] + 5, 0.55 * mat.shape[0] + 2.5))
        sns.heatmap(
            mat,
            vmin=vmin,
            vmax=vmax,
            center=0 if (vmin < 0 < vmax) else None,
            cmap="RdBu_r",
            annot=True,
            fmt=".3f",
            linewidths=0.4,
            cbar_kws={"shrink": 0.8},
            yticklabels=y_labels,
            ax=ax,
        )
        ax.set_title(f"{feat}: Spearman by feature slices vs outcomes")
        ax.set_xlabel("Outcome")
        ax.set_ylabel("Feature slices (threshold intervals)")
        fig.tight_layout()
        fig.savefig(out_dir / f"{prefix}_{feat}.png", dpi=150)
        plt.close(fig)


def traditional_spearman_table(
    df: pd.DataFrame,
    feature_cols: list[str],
    out_cols: list[str],
) -> pd.DataFrame:
    rows: list[dict] = []
    for feat in feature_cols:
        for out in out_cols:
            if feat not in df.columns or out not in df.columns:
                continue
            mask = df[feat].notna() & df[out].notna()
            if mask.sum() < 10:
                continue
            x = df.loc[mask, feat].astype(float)
            y = df.loc[mask, out].astype(float)
            rho = x.corr(y, method="spearman")
            rows.append(
                {
                    "feature": feat,
                    "outcome": out,
                    "n": int(mask.sum()),
                    "spearman_rho": rho,
                }
            )
    return pd.DataFrame(rows)


def plot_traditional_spearman_by_feature(
    spearman_df: pd.DataFrame,
    out_dir: Path,
    prefix: str,
) -> None:
    if spearman_df.empty:
        return
    sns.set_theme(style="whitegrid")
    for feat, gdf in spearman_df.groupby("feature", sort=True):
        gdf = gdf.sort_values("spearman_rho")
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(data=gdf, x="spearman_rho", y="outcome", orient="h", ax=ax, color="#2a9d8f")
        ax.axvline(0.0, color="black", linewidth=1, alpha=0.5)
        ax.set_title(f"{feat}: traditional Spearman rho vs outcomes")
        ax.set_xlabel("Spearman rho")
        ax.set_ylabel("Outcome")
        fig.tight_layout()
        fig.savefig(out_dir / f"{prefix}_{feat}.png", dpi=150)
        plt.close(fig)


def traditional_spearman_grid(spearman_df: pd.DataFrame) -> pd.DataFrame:
    if spearman_df.empty:
        return pd.DataFrame()
    grid = spearman_df.pivot(index="feature", columns="outcome", values="spearman_rho")
    ordered_cols = [c for c in ordered_outcomes(grid.columns.tolist()) if c in grid.columns]
    return grid.sort_index()[ordered_cols]


def plot_traditional_spearman_grid(grid: pd.DataFrame, out_file: Path, title: str) -> None:
    if grid.empty:
        return
    vals = grid.to_numpy(dtype=float)
    finite = vals[np.isfinite(vals)]
    if finite.size == 0:
        vmin, vmax = -0.1, 0.1
    else:
        vmin = float(finite.min())
        vmax = float(finite.max())
        if np.isclose(vmin, vmax):
            eps = max(1e-6, abs(vmin) * 0.1 + 1e-6)
            vmin -= eps
            vmax += eps

    fig, ax = plt.subplots(figsize=(1.0 * grid.shape[1] + 4, 0.8 * grid.shape[0] + 2))
    sns.heatmap(
        grid,
        vmin=vmin,
        vmax=vmax,
        center=0 if (vmin < 0 < vmax) else None,
        cmap="RdBu_r",
        annot=True,
        fmt=".3f",
        linewidths=0.4,
        cbar_kws={"shrink": 0.8},
        ax=ax,
    )
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_file, dpi=150)
    plt.close(fig)


def summary_stats(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    quantiles = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    desc = df[cols].describe(percentiles=quantiles).T
    desc["skew"] = df[cols].skew(numeric_only=True)
    desc["kurtosis"] = df[cols].kurtosis(numeric_only=True)
    rename = {
        "count": "n",
        "mean": "mean",
        "std": "std",
        "min": "min",
        "1%": "p01",
        "5%": "p05",
        "25%": "p25",
        "50%": "p50",
        "75%": "p75",
        "95%": "p95",
        "99%": "p99",
        "max": "max",
    }
    out = desc.rename(columns=rename)
    out["range"] = out["max"] - out["min"]
    out["iqr"] = out["p75"] - out["p25"]
    out["cv"] = out["std"] / out["mean"].replace(0, np.nan)
    return out.reset_index(names="feature")


def _adaptive_bins(series: pd.Series, fixed_bins: int) -> int:
    values = series.dropna().to_numpy()
    n = len(values)
    if n < 2:
        return 10
    if fixed_bins and fixed_bins > 0:
        return fixed_bins

    q75, q25 = np.percentile(values, [75, 25])
    iqr = q75 - q25
    data_range = values.max() - values.min()
    if iqr <= 0 or data_range <= 0:
        return min(100, max(10, int(np.sqrt(n))))

    # Freedman-Diaconis bin width: 2 * IQR / n^(1/3)
    bin_width = 2 * iqr / (n ** (1 / 3))
    if bin_width <= 0:
        return min(100, max(10, int(np.sqrt(n))))
    bins = int(np.ceil(data_range / bin_width))
    return min(300, max(10, bins))


def plot_distributions(
    df: pd.DataFrame,
    cols: list[str],
    out_dir: Path,
    bins: int,
    q_low: float,
    q_high: float,
    include_strategy_views: bool,
) -> None:
    sns.set_theme(style="whitegrid")

    for col in cols:
        values = df[col].dropna()
        if values.empty:
            continue
        auto_bins = _adaptive_bins(values, bins)
        p_low, p_high = values.quantile([q_low, q_high])

        fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
        ax = axes[0]
        sns.histplot(
            values,
            bins=auto_bins,
            stat="count",
            color="#2a9d8f",
            ax=ax,
        )
        ax.set_title(f"{col} histogram full range (bins={auto_bins})")
        ax.set_xlabel(col)
        ax.set_ylabel("count")

        ax2 = axes[1]
        zoom_values = values[(values >= p_low) & (values <= p_high)]
        zoom_bins = _adaptive_bins(zoom_values, bins)
        sns.histplot(
            zoom_values,
            bins=zoom_bins,
            stat="count",
            color="#1d3557",
            ax=ax2,
        )
        ax2.set_xlim(p_low, p_high)
        ax2.set_title(f"{col} histogram zoom ({q_low:.2%}-{q_high:.2%})")
        ax2.set_xlabel(col)
        ax2.set_ylabel("count")

        fig.tight_layout()
        fig.savefig(out_dir / f"distribution_{col}.png", dpi=150)
        plt.close(fig)

        if include_strategy_views and "strategy" in df.columns:
            fig, ax = plt.subplots(figsize=(10, 4.5))
            sns.histplot(
                data=df,
                x=col,
                hue="strategy",
                bins=auto_bins,
                stat="count",
                common_norm=False,
                element="step",
                fill=False,
                ax=ax,
            )
            ax.set_title(f"{col} distribution by strategy")
            ax.set_xlabel(col)
            ax.set_ylabel("count")
            fig.tight_layout()
            fig.savefig(out_dir / f"distribution_{col}_by_strategy.png", dpi=150)
            plt.close(fig)


def plot_correlation(df: pd.DataFrame, cols: list[str], out_file: Path, method: str, title: str) -> pd.DataFrame:
    corr = df[cols].corr(method=method)
    vals = corr.to_numpy(dtype=float)
    finite = vals[np.isfinite(vals)]
    if finite.size == 0:
        vmin, vmax = -0.1, 0.1
    else:
        vmin = float(finite.min())
        vmax = float(finite.max())
        if np.isclose(vmin, vmax):
            eps = max(1e-6, abs(vmin) * 0.1 + 1e-6)
            vmin -= eps
            vmax += eps
    fig, ax = plt.subplots(figsize=(1.1 * len(cols) + 3, 1.0 * len(cols) + 2))
    sns.heatmap(
        corr,
        vmin=vmin,
        vmax=vmax,
        center=0 if (vmin < 0 < vmax) else None,
        cmap="RdBu_r",
        square=True,
        linewidths=0.4,
        cbar_kws={"shrink": 0.8},
        annot=True,
        fmt=".2f",
        annot_kws={"size": 8},
        ax=ax,
    )
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_file, dpi=150)
    plt.close(fig)
    return corr


def plot_feature_outcome_correlation(
    df: pd.DataFrame,
    feat_cols: list[str],
    out_cols: list[str],
    out_file: Path,
    method: str,
    title: str,
) -> pd.DataFrame:
    corr = df[feat_cols + out_cols].corr(method=method).loc[feat_cols, out_cols]
    vals = corr.to_numpy(dtype=float)
    finite = vals[np.isfinite(vals)]
    if finite.size == 0:
        vmin, vmax = -0.1, 0.1
    else:
        vmin = float(finite.min())
        vmax = float(finite.max())
        if np.isclose(vmin, vmax):
            eps = max(1e-6, abs(vmin) * 0.1 + 1e-6)
            vmin -= eps
            vmax += eps
    fig, ax = plt.subplots(figsize=(1.1 * len(out_cols) + 3, 0.8 * len(feat_cols) + 2))
    sns.heatmap(
        corr,
        vmin=vmin,
        vmax=vmax,
        center=0 if (vmin < 0 < vmax) else None,
        cmap="RdBu_r",
        linewidths=0.4,
        cbar_kws={"shrink": 0.8},
        annot=True,
        fmt=".2f",
        annot_kws={"size": 8},
        ax=ax,
    )
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_file, dpi=150)
    plt.close(fig)
    return corr


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    base_dir = Path(__file__).resolve().parent
    files = sorted(base_dir.glob(args.input_glob))
    if not files:
        raise FileNotFoundError(f"No files found with pattern: {base_dir / args.input_glob}")

    out_dir = (base_dir / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_event_files(files)
    if df.empty:
        raise RuntimeError("Loaded files but combined dataframe is empty.")

    cols = feature_columns(
        df,
        include_dispersion=args.include_dispersion,
        include_accz5=args.include_accz5,
        include_volz=args.include_volz,
    )
    if not cols:
        raise RuntimeError("No feature columns found (expected accZ_*, slope_* and volZ, plus disp_* if enabled).")
    corr_cols = [c for c in cols if not c.startswith("disp_")]
    if not corr_cols:
        raise RuntimeError("No correlation feature columns found after excluding dispersion columns.")
    out_cols = outcome_columns(df)
    out_cols = ordered_outcomes(out_cols)
    if not out_cols:
        LOG.warning("No forward outcome columns found (close_ret_*, high_ret_max_*, low_ret_min_*).")

    df = add_binary_indicators(df)
    indicator_cols = ["ind_trend_alignment", "ind_curvature_mismatch_20", "ind_local_slope_dominance"]

    LOG.info("Loaded %d files, rows=%d", len(files), len(df))
    LOG.info("Feature columns (distribution/summary): %s", cols)
    LOG.info("Feature columns (correlation, dispersion excluded): %s", corr_cols)
    LOG.info("Binary indicators: %s", indicator_cols)
    if out_cols:
        LOG.info("Outcome columns: %s", out_cols)

    summary_df = summary_stats(df, cols)
    summary_df.to_csv(out_dir / "feature_summary.csv", index=False)

    grouped_rows: list[pd.DataFrame] = []
    for asset, gdf in df.groupby("asset", sort=True):
        gsum = summary_stats(gdf, cols)
        gsum.insert(0, "asset", asset)
        grouped_rows.append(gsum)
    if grouped_rows:
        pd.concat(grouped_rows, ignore_index=True).to_csv(out_dir / "feature_summary_by_asset.csv", index=False)

    corr = plot_correlation(
        df,
        corr_cols,
        out_dir / f"feature_corr_{args.corr_method}.png",
        args.corr_method,
        f"Feature Correlation ({args.corr_method})",
    )
    corr.to_csv(out_dir / f"feature_corr_{args.corr_method}.csv")

    if out_cols:
        fo_corr = plot_feature_outcome_correlation(
            df,
            corr_cols,
            out_cols,
            out_dir / f"feature_vs_outcome_corr_{args.corr_method}.png",
            args.corr_method,
            f"Feature vs Outcome Correlation ({args.corr_method})",
        )
        fo_corr.to_csv(out_dir / f"feature_vs_outcome_corr_{args.corr_method}.csv")
        trad = traditional_spearman_table(df, corr_cols, out_cols)
        trad.to_csv(out_dir / "traditional_spearman.csv", index=False)
        trad_grid = traditional_spearman_grid(trad)
        trad_grid.to_csv(out_dir / "traditional_spearman_grid.csv")
        plot_traditional_spearman_grid(
            trad_grid,
            out_dir / "traditional_spearman_grid.png",
            "Traditional Spearman Grid",
        )
        plot_traditional_spearman_by_feature(
            trad,
            out_dir,
            prefix="traditional_spearman",
        )

        close_th = args.close_threshold_bps / 10000.0
        high_th = args.high_threshold_bps / 10000.0
        low_th = args.low_threshold_bps / 10000.0
        bin_all = binary_indicator_analysis(
            df,
            indicator_cols,
            out_cols,
            close_th=close_th,
            high_th=high_th,
            low_th=low_th,
        )
        bin_all.to_csv(out_dir / "binary_indicator_analysis.csv", index=False)

        slice_all = feature_slice_analysis(
            df,
            corr_cols,
            out_cols,
            close_th=close_th,
            high_th=high_th,
            low_th=low_th,
            n_slices=10,
        )
        if not slice_all.empty:
            slice_all.to_csv(out_dir / "feature_slice_analysis.csv", index=False)
            thr = build_slice_threshold_table(slice_all)
            thr.to_csv(out_dir / "feature_slice_thresholds.csv", index=False)
            plot_slice_correlation_by_feature(
                slice_all,
                out_dir,
                prefix="slice_corr",
            )
            plot_spearman_slice_matrix_by_feature(
                slice_all,
                out_dir,
                prefix="spearman_slice_matrix",
            )

        by_asset: list[pd.DataFrame] = []
        slice_by_asset: list[pd.DataFrame] = []
        for asset, gdf in df.groupby("asset", sort=True):
            bt = binary_indicator_analysis(
                gdf,
                indicator_cols,
                out_cols,
                close_th=close_th,
                high_th=high_th,
                low_th=low_th,
            )
            if not bt.empty:
                bt.insert(0, "asset", asset)
                by_asset.append(bt)
            st = feature_slice_analysis(
                gdf,
                corr_cols,
                out_cols,
                close_th=close_th,
                high_th=high_th,
                low_th=low_th,
                n_slices=10,
            )
            if not st.empty:
                st.insert(0, "asset", asset)
                slice_by_asset.append(st)
        if by_asset:
            pd.concat(by_asset, ignore_index=True).to_csv(out_dir / "binary_indicator_analysis_by_asset.csv", index=False)
        if slice_by_asset:
            slice_by_asset_df = pd.concat(slice_by_asset, ignore_index=True)
            slice_by_asset_df.to_csv(out_dir / "feature_slice_analysis_by_asset.csv", index=False)
            thr_asset = (
                slice_by_asset_df[
                    ["asset", "feature", "slice_id", "slice_low", "slice_high", "slice_rate", "n_slice"]
                ]
                .drop_duplicates(subset=["asset", "feature", "slice_id"])
                .sort_values(["asset", "feature", "slice_id"])
            )
            thr_asset.to_csv(out_dir / "feature_slice_thresholds_by_asset.csv", index=False)

    if args.include_strategy_views:
        strategy_rows: list[pd.DataFrame] = []
        for (asset, strategy), gdf in df.groupby(["asset", "strategy"], sort=True):
            gsum = summary_stats(gdf, cols)
            gsum.insert(0, "strategy", strategy)
            gsum.insert(0, "asset", asset)
            strategy_rows.append(gsum)
        if strategy_rows:
            pd.concat(strategy_rows, ignore_index=True).to_csv(
                out_dir / "feature_summary_by_asset_strategy.csv",
                index=False,
            )

        for strategy, gdf in df.groupby("strategy", sort=True):
            if len(gdf) < 3:
                continue
            scorr = plot_correlation(
                gdf,
                corr_cols,
                out_dir / f"feature_corr_{strategy}_{args.corr_method}.png",
                args.corr_method,
                f"Feature Correlation ({args.corr_method}) - {strategy}",
            )
            scorr.to_csv(out_dir / f"feature_corr_{strategy}_{args.corr_method}.csv")
            if out_cols:
                so_corr = plot_feature_outcome_correlation(
                    gdf,
                    corr_cols,
                    out_cols,
                    out_dir / f"feature_vs_outcome_corr_{strategy}_{args.corr_method}.png",
                    args.corr_method,
                    f"Feature vs Outcome Correlation ({args.corr_method}) - {strategy}",
                )
                so_corr.to_csv(out_dir / f"feature_vs_outcome_corr_{strategy}_{args.corr_method}.csv")
                trad_s = traditional_spearman_table(gdf, corr_cols, out_cols)
                trad_s.to_csv(out_dir / f"traditional_spearman_{strategy}.csv", index=False)
                trad_s_grid = traditional_spearman_grid(trad_s)
                trad_s_grid.to_csv(out_dir / f"traditional_spearman_{strategy}_grid.csv")
                plot_traditional_spearman_grid(
                    trad_s_grid,
                    out_dir / f"traditional_spearman_{strategy}_grid.png",
                    f"Traditional Spearman Grid - {strategy}",
                )
                plot_traditional_spearman_by_feature(
                    trad_s,
                    out_dir,
                    prefix=f"traditional_spearman_{strategy}",
                )
                sb = binary_indicator_analysis(
                    gdf,
                    indicator_cols,
                    out_cols,
                    close_th=args.close_threshold_bps / 10000.0,
                    high_th=args.high_threshold_bps / 10000.0,
                    low_th=args.low_threshold_bps / 10000.0,
                )
                if not sb.empty:
                    sb.insert(0, "strategy", strategy)
                    sb.to_csv(out_dir / f"binary_indicator_analysis_{strategy}.csv", index=False)
                ss = feature_slice_analysis(
                    gdf,
                    corr_cols,
                    out_cols,
                    close_th=args.close_threshold_bps / 10000.0,
                    high_th=args.high_threshold_bps / 10000.0,
                    low_th=args.low_threshold_bps / 10000.0,
                    n_slices=10,
                )
                if not ss.empty:
                    ss.insert(0, "strategy", strategy)
                    ss.to_csv(out_dir / f"feature_slice_analysis_{strategy}.csv", index=False)
                    thr_s = (
                        ss[
                            ["strategy", "feature", "slice_id", "slice_low", "slice_high", "slice_rate", "n_slice"]
                        ]
                        .drop_duplicates(subset=["strategy", "feature", "slice_id"])
                        .sort_values(["strategy", "feature", "slice_id"])
                    )
                    thr_s.to_csv(out_dir / f"feature_slice_thresholds_{strategy}.csv", index=False)
                    plot_slice_correlation_by_feature(
                        ss,
                        out_dir,
                        prefix=f"slice_corr_{strategy}",
                    )
                    plot_spearman_slice_matrix_by_feature(
                        ss,
                        out_dir,
                        prefix=f"spearman_slice_matrix_{strategy}",
                    )

    plot_distributions(
        df,
        cols,
        out_dir,
        args.bins,
        args.zoom_q_low,
        args.zoom_q_high,
        args.include_strategy_views,
    )

    LOG.info("Diagnostics completed. Outputs in: %s", out_dir)


if __name__ == "__main__":
    main()
