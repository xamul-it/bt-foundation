#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

LOG = logging.getLogger("target_hit_candle_distribution")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Distribuzione della candela di primo raggiungimento target high (0.1..0.5%), "
            "con ingresso limit sulla candela 1 al prezzo close della candela 0."
        )
    )
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--output-dir", default="results/target_hit_candle_dist")

    # filtri scenario
    p.add_argument("--slope5-min", type=float, default=0.0002)
    p.add_argument("--slope20-min", type=float, default=0.0001)
    p.add_argument("--slope60-min", type=float, default=0.00005)
    p.add_argument("--quality-window", type=int, default=60, choices=[5, 20, 60])
    p.add_argument("--quality-min", type=float, default=0.35)
    p.add_argument("--volz-min", type=float, default=-1.0)
    p.add_argument("--enforce-slope-order", action="store_true", default=True)

    p.add_argument("--max-candle", type=int, default=390, help="Massima candela da plottare (dopo il segnale).")
    p.add_argument(
        "--quantiles-on-entered",
        default="0.1,0.2,0.3,0.4,0.5",
        help="Lista quantili su entrati per trovare la candela di raggiungimento (es. 0.1,0.2,0.5).",
    )
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def resolve(path_like: str) -> Path:
    p = Path(path_like)
    if p.is_absolute():
        return p
    return (Path(__file__).resolve().parent / path_like).resolve()


def load_full_data(input_glob: str) -> pd.DataFrame:
    here = Path(__file__).resolve().parent
    files = sorted(here.glob(input_glob))
    if not files:
        raise FileNotFoundError(f"No files for glob: {input_glob}")

    parts = []
    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        asset = m.group(1) if m else "UNK"
        df = pd.read_csv(fp)
        df["asset"] = asset
        parts.append(df)

    out = pd.concat(parts, ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    return out


def add_fit_quality(df: pd.DataFrame, window: int) -> pd.DataFrame:
    out = df.copy()
    need = [f"r2_{window}", f"disp_{window}", f"mse_{window}", f"rmse_{window}", f"mae_{window}"]
    if not all(c in out.columns for c in need):
        raise RuntimeError(f"Missing columns for fit_quality_{window}: {need}")

    r2_rank = pd.to_numeric(out[f"r2_{window}"], errors="coerce").rank(pct=True)
    disp_rank = pd.to_numeric(out[f"disp_{window}"], errors="coerce").rank(pct=True)
    mse_rank = pd.to_numeric(out[f"mse_{window}"], errors="coerce").rank(pct=True)
    rmse_rank = pd.to_numeric(out[f"rmse_{window}"], errors="coerce").rank(pct=True)
    mae_rank = pd.to_numeric(out[f"mae_{window}"], errors="coerce").rank(pct=True)

    out[f"fit_quality_{window}"] = pd.concat(
        [
            r2_rank.rename("r2_good"),
            (1 - disp_rank).rename("disp_good"),
            (1 - mse_rank).rename("mse_good"),
            (1 - rmse_rank).rename("rmse_good"),
            (1 - mae_rank).rename("mae_good"),
        ],
        axis=1,
    ).mean(axis=1, skipna=False)
    return out


def compute_eod_pos(ts: pd.Series) -> np.ndarray:
    day = ts.dt.date.to_numpy()
    n = len(ts)
    eod = np.zeros(n, dtype=int)
    start = 0
    for i in range(1, n + 1):
        if i == n or day[i] != day[start]:
            eod[start:i] = i - 1
            start = i
    return eod


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    quantiles_on_entered = [float(x.strip()) for x in args.quantiles_on_entered.split(",") if x.strip()]
    quantiles_on_entered = sorted(set([q for q in quantiles_on_entered if 0 < q < 1]))

    df = load_full_data(args.input_glob)
    df = add_fit_quality(df, args.quality_window)

    targets_up = [0.001, 0.002, 0.003, 0.004, 0.005]
    targets_down = [-0.001, -0.002, -0.003, -0.004, -0.005]

    hit_rows = []
    summary_rows = []

    for asset, g in df.groupby("asset", sort=False):
        g = g.sort_values("timestamp").reset_index(drop=True)

        close = pd.to_numeric(g["close"], errors="coerce").to_numpy(dtype=float)
        high = pd.to_numeric(g["high"], errors="coerce").to_numpy(dtype=float)
        low = pd.to_numeric(g["low"], errors="coerce").to_numpy(dtype=float)
        s5 = pd.to_numeric(g["slope_5"], errors="coerce").to_numpy(dtype=float)
        s20 = pd.to_numeric(g["slope_20"], errors="coerce").to_numpy(dtype=float)
        s60 = pd.to_numeric(g["slope_60"], errors="coerce").to_numpy(dtype=float)
        fitq = pd.to_numeric(g[f"fit_quality_{args.quality_window}"], errors="coerce").to_numpy(dtype=float)
        volz = pd.to_numeric(g["volZ"], errors="coerce").to_numpy(dtype=float) if "volZ" in g.columns else np.full(len(g), np.nan)
        ts = pd.to_datetime(g["timestamp"], errors="coerce", utc=True)

        eod_pos = compute_eod_pos(ts)

        mask = (s5 > args.slope5_min) & (s20 > args.slope20_min) & (s60 > args.slope60_min) & (fitq >= args.quality_min)
        if args.enforce_slope_order:
            mask &= (s5 > s20) & (s20 > s60)
        if args.volz_min >= 0:
            mask &= volz > args.volz_min

        signal_idx = np.where(mask)[0]

        entered_idx = []
        for i in signal_idx:
            if i + 1 >= len(g):
                continue
            entry_px = close[i]
            if np.isnan(entry_px) or np.isnan(high[i + 1]) or np.isnan(low[i + 1]):
                continue
            # Entry valid ONLY on candle 1
            if low[i + 1] <= entry_px <= high[i + 1]:
                entered_idx.append(i)

        entered_idx = np.array(entered_idx, dtype=int)
        n_signals = int(len(signal_idx))
        n_entered = int(len(entered_idx))

        for t in targets_up:
            n_hit = 0
            for i in entered_idx:
                entry_px = close[i]
                target_px = entry_px * (1.0 + t)

                start = i + 2  # from candle 2
                if start >= len(g):
                    continue
                end = eod_pos[i + 1]  # cap at EOD after entry bar
                if end < start:
                    continue
                if args.max_candle > 0:
                    end = min(end, i + args.max_candle)

                hit_k = None
                for j in range(start, end + 1):
                    if not np.isnan(high[j]) and high[j] >= target_px:
                        hit_k = j - i  # candle index from signal bar (candle0)
                        break

                if hit_k is not None:
                    n_hit += 1
                    hit_rows.append(
                        {
                            "asset": asset,
                            "side": "up",
                            "target_pct": t,
                            "hit_candle": int(hit_k),
                        }
                    )

            summary_rows.append(
                {
                    "asset": asset,
                    "side": "up",
                    "target_pct": t,
                    "n_signals": n_signals,
                    "n_entered": n_entered,
                    "p_enter": (n_entered / n_signals) if n_signals > 0 else np.nan,
                    "n_hit": n_hit,
                    "p_hit_given_enter": (n_hit / n_entered) if n_entered > 0 else np.nan,
                }
            )

        for t in targets_down:
            n_hit = 0
            for i in entered_idx:
                entry_px = close[i]
                target_px = entry_px * (1.0 + t)

                start = i + 2  # from candle 2
                if start >= len(g):
                    continue
                end = eod_pos[i + 1]  # cap at EOD after entry bar
                if end < start:
                    continue
                if args.max_candle > 0:
                    end = min(end, i + args.max_candle)

                hit_k = None
                for j in range(start, end + 1):
                    if not np.isnan(low[j]) and low[j] <= target_px:
                        hit_k = j - i
                        break

                if hit_k is not None:
                    n_hit += 1
                    hit_rows.append(
                        {
                            "asset": asset,
                            "side": "down",
                            "target_pct": t,
                            "hit_candle": int(hit_k),
                        }
                    )

            summary_rows.append(
                {
                    "asset": asset,
                    "side": "down",
                    "target_pct": t,
                    "n_signals": n_signals,
                    "n_entered": n_entered,
                    "p_enter": (n_entered / n_signals) if n_signals > 0 else np.nan,
                    "n_hit": n_hit,
                    "p_hit_given_enter": (n_hit / n_entered) if n_entered > 0 else np.nan,
                }
            )

    hit_df = pd.DataFrame(hit_rows)
    sum_df = pd.DataFrame(summary_rows)

    if hit_df.empty:
        LOG.warning("No target hits found with current filters.")
        sum_df.to_csv(out_dir / "target_hit_candle_summary.csv", index=False)
        return

    def _save_side_outputs(side: str, suffix: str, palette: dict[float, str], title_side: str) -> None:
        h = hit_df[hit_df["side"] == side].copy()
        s = sum_df[sum_df["side"] == side].copy()
        if h.empty or s.empty:
            return

        agg = (
            h.groupby(["target_pct", "hit_candle"], as_index=False)
            .size()
            .rename(columns={"size": "hit_count"})
        )

        total_entered_by_target = (
            s.groupby("target_pct", as_index=False)["n_entered"].sum().rename(columns={"n_entered": "n_entered_total"})
        )
        total_hits_by_target = (
            agg.groupby("target_pct", as_index=False)["hit_count"].sum().rename(columns={"hit_count": "n_hit_total"})
        )
        agg = agg.merge(total_entered_by_target, on="target_pct", how="left")
        agg = agg.merge(total_hits_by_target, on="target_pct", how="left")
        agg["p_hit_on_entered"] = agg["hit_count"] / agg["n_entered_total"]
        agg["p_hit_on_hits"] = agg["hit_count"] / agg["n_hit_total"]

        agg.to_csv(out_dir / f"target_hit_candle_distribution_{suffix}.csv", index=False)
        s.to_csv(out_dir / f"target_hit_candle_summary_by_asset_{suffix}.csv", index=False)

        overall = (
            s.groupby("target_pct", as_index=False)
            .agg(
                n_signals=("n_signals", "sum"),
                n_entered=("n_entered", "sum"),
                n_hit=("n_hit", "sum"),
            )
        )
        overall["p_enter"] = overall["n_entered"] / overall["n_signals"]
        overall["p_hit_given_enter"] = overall["n_hit"] / overall["n_entered"]
        overall.to_csv(out_dir / f"target_hit_candle_summary_overall_{suffix}.csv", index=False)

        q_rows = []
        for target, g in agg.groupby("target_pct", sort=True):
            gg = g.sort_values("hit_candle").copy()
            gg["cum_p_on_entered"] = gg["p_hit_on_entered"].cumsum()
            max_reached = float(gg["cum_p_on_entered"].max()) if len(gg) else 0.0
            for q in quantiles_on_entered:
                cand = gg.loc[gg["cum_p_on_entered"] >= q, ["hit_candle", "cum_p_on_entered"]]
                if len(cand):
                    hit_candle = int(cand.iloc[0]["hit_candle"])
                    p_reached_by_candle = float(cand.iloc[0]["cum_p_on_entered"])
                    reached = True
                else:
                    hit_candle = np.nan
                    p_reached_by_candle = max_reached
                    reached = False
                q_rows.append(
                    {
                        "target_pct": target,
                        "quantile_on_entered": q,
                        "hit_candle_at_quantile": hit_candle,
                        "p_reached_by_that_candle": p_reached_by_candle,
                        "reached_within_window": reached,
                        "max_p_reached_in_window": max_reached,
                    }
                )
        q_df = pd.DataFrame(q_rows).sort_values(["target_pct", "quantile_on_entered"])
        q_df.to_csv(out_dir / f"target_hit_candle_quantiles_on_entered_{suffix}.csv", index=False)

        fig, ax = plt.subplots(figsize=(11, 6))
        plot_df = agg.sort_values(["target_pct", "hit_candle"])
        sns.lineplot(
            data=plot_df,
            x="hit_candle",
            y="p_hit_on_entered",
            hue="target_pct",
            marker="o",
            linewidth=1.8,
            palette=palette,
            ax=ax,
        )
        ax.set_title(f"Distribuzione candela di primo hit target {title_side}")
        ax.set_xlabel("Candela di primo raggiungimento (candela 0 = segnale)")
        ax.set_ylabel("Probabilita' su trade entrati")
        ax.grid(alpha=0.2)
        handles, labels = ax.get_legend_handles_labels()
        if labels:
            new_labels = []
            for lab in labels:
                try:
                    v = float(lab)
                    new_labels.append(f"target {v*100:.1f}%")
                except Exception:
                    new_labels.append(lab)
            ax.legend(handles, new_labels, title="Livelli")
        fig.tight_layout()
        fig.savefig(out_dir / f"target_hit_candle_distribution_overlay_{suffix}.png", dpi=160)
        plt.close(fig)

        cum_df = agg.sort_values(["target_pct", "hit_candle"]).copy()
        cum_df["cum_p_hit_on_entered"] = cum_df.groupby("target_pct")["p_hit_on_entered"].cumsum()

        fig, ax = plt.subplots(figsize=(11, 6))
        sns.lineplot(
            data=cum_df,
            x="hit_candle",
            y="cum_p_hit_on_entered",
            hue="target_pct",
            marker="o",
            linewidth=1.8,
            palette=palette,
            ax=ax,
        )
        if not q_df.empty:
            q_ok = q_df[q_df["reached_within_window"] == True].copy()
            if not q_ok.empty:
                sns.scatterplot(
                    data=q_ok,
                    x="hit_candle_at_quantile",
                    y="p_reached_by_that_candle",
                    hue="target_pct",
                    palette=palette,
                    marker="X",
                    s=70,
                    legend=False,
                    ax=ax,
                )
        ax.set_title(f"Curva cumulata hit target su entrati + marker quantili ({title_side})")
        ax.set_xlabel("Candela (0=segnale, 1=entry window, da 2 in poi monitor)")
        ax.set_ylabel("Probabilita cumulata su entrati")
        ax.grid(alpha=0.2)
        handles, labels = ax.get_legend_handles_labels()
        if labels:
            new_labels = []
            for lab in labels:
                try:
                    v = float(lab)
                    new_labels.append(f"target {v*100:.1f}%")
                except Exception:
                    new_labels.append(lab)
            ax.legend(handles, new_labels, title="Livelli")
        fig.tight_layout()
        fig.savefig(out_dir / f"target_hit_candle_cumulative_on_entered_{suffix}.png", dpi=160)
        plt.close(fig)

    palette_up = {0.001: "#1b5e20", 0.002: "#2e7d32", 0.003: "#558b2f", 0.004: "#9e9d24", 0.005: "#f9a825"}
    palette_down = {-0.001: "#8e0000", -0.002: "#b71c1c", -0.003: "#d32f2f", -0.004: "#ef5350", -0.005: "#ff8a80"}
    _save_side_outputs("up", "up", palette_up, "(rialzo)")
    _save_side_outputs("down", "down", palette_down, "(ribasso)")

    # Combined plots (up + down) in the same chart, preserving current colors.
    agg_all = (
        hit_df.groupby(["side", "target_pct", "hit_candle"], as_index=False)
        .size()
        .rename(columns={"size": "hit_count"})
    )
    entered_all = (
        sum_df.groupby(["side", "target_pct"], as_index=False)["n_entered"]
        .sum()
        .rename(columns={"n_entered": "n_entered_total"})
    )
    agg_all = agg_all.merge(entered_all, on=["side", "target_pct"], how="left")
    agg_all["p_hit_on_entered"] = agg_all["hit_count"] / agg_all["n_entered_total"]
    agg_all["target_label"] = agg_all["target_pct"].map(lambda v: f"{v*100:+.1f}%")

    color_map = {}
    for k, c in palette_up.items():
        color_map[f"{k*100:+.1f}%"] = c
    for k, c in palette_down.items():
        color_map[f"{k*100:+.1f}%"] = c

    fig, ax = plt.subplots(figsize=(12, 7))
    sns.lineplot(
        data=agg_all.sort_values(["target_pct", "hit_candle"]),
        x="hit_candle",
        y="p_hit_on_entered",
        hue="target_label",
        marker="o",
        linewidth=1.6,
        palette=color_map,
        ax=ax,
    )
    ax.set_title("Distribuzione primo hit target: up + down (stessa figura)")
    ax.set_xlabel("Candela di primo raggiungimento (candela 0 = segnale)")
    ax.set_ylabel("Probabilita' su trade entrati")
    ax.grid(alpha=0.2)
    handles, labels = ax.get_legend_handles_labels()
    if labels:
        # sort legend by numeric target
        pairs = []
        for h, l in zip(handles, labels):
            try:
                pairs.append((float(l.replace("%", "")), h, l))
            except Exception:
                pairs.append((999, h, l))
        pairs.sort(key=lambda x: x[0])
        ax.legend([p[1] for p in pairs], [p[2] for p in pairs], title="Target")
    fig.tight_layout()
    fig.savefig(out_dir / "target_hit_candle_distribution_overlay_both.png", dpi=160)
    plt.close(fig)

    cum_all = agg_all.sort_values(["side", "target_pct", "hit_candle"]).copy()
    cum_all["cum_p_hit_on_entered"] = cum_all.groupby(["side", "target_pct"])["p_hit_on_entered"].cumsum()

    fig, ax = plt.subplots(figsize=(12, 7))
    sns.lineplot(
        data=cum_all,
        x="hit_candle",
        y="cum_p_hit_on_entered",
        hue="target_label",
        marker="o",
        linewidth=1.6,
        palette=color_map,
        ax=ax,
    )
    ax.set_title("Curva cumulata hit target su entrati: up + down")
    ax.set_xlabel("Candela (0=segnale, 1=entry window, da 2 in poi monitor)")
    ax.set_ylabel("Probabilita cumulata su entrati")
    ax.grid(alpha=0.2)
    handles, labels = ax.get_legend_handles_labels()
    if labels:
        pairs = []
        for h, l in zip(handles, labels):
            try:
                pairs.append((float(l.replace("%", "")), h, l))
            except Exception:
                pairs.append((999, h, l))
        pairs.sort(key=lambda x: x[0])
        ax.legend([p[1] for p in pairs], [p[2] for p in pairs], title="Target")
    fig.tight_layout()
    fig.savefig(out_dir / "target_hit_candle_cumulative_on_entered_both.png", dpi=160)
    plt.close(fig)

    LOG.info("Done. Output: %s", out_dir)


if __name__ == "__main__":
    main()
