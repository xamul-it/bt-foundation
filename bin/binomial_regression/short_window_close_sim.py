#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

LOG = logging.getLogger("short_window_close_sim")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Short simulation: sell on entry, buy at window close.")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--minute-data-dir", default="../../config/data/m/alpaca")
    p.add_argument("--output-dir", default="results/short_window_close_sim")
    p.add_argument("--horizon-step", type=int, default=5)
    p.add_argument("--horizon-max", type=int, default=60, help="Max horizon in minutes (es. 120 = 2 ore).")

    # Same signal filters
    p.add_argument("--slope5-min", type=float, default=0.0002)
    p.add_argument("--slope20-min", type=float, default=0.0001)
    p.add_argument("--slope60-min", type=float, default=0.00005)
    p.add_argument("--quality-window", type=int, default=60, choices=[5, 20, 60])
    p.add_argument("--quality-min", type=float, default=0.35)
    p.add_argument("--volz-min", type=float, default=-1.0, help="Set negative to disable vol filter.")
    p.add_argument(
        "--enforce-slope-order",
        action="store_true",
        help="Require slope_5 > slope_20 > slope_60.",
    )

    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s | %(levelname)s | %(message)s")


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

    parts: List[pd.DataFrame] = []
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


def ensure_forward_close(df: pd.DataFrame, horizons: List[int]) -> pd.DataFrame:
    out = df.copy()
    for asset, g in out.groupby("asset", sort=False):
        idx = g.sort_values("timestamp").index
        gg = out.loc[idx]
        close = pd.to_numeric(gg["close"], errors="coerce").to_numpy(dtype=float)
        n = len(gg)
        for h in horizons:
            col = f"close_ret_{h}m"
            if col in out.columns:
                continue
            ret = np.full(n, np.nan, dtype=float)
            for i in range(n):
                j = i + h
                if j < n and not np.isnan(close[i]) and not np.isnan(close[j]):
                    ret[i] = close[j] / close[i] - 1
            out.loc[idx, col] = ret
    return out


def add_capped_close_returns(df: pd.DataFrame, horizons: List[int]) -> pd.DataFrame:
    """
    Build close returns with horizon cap at end-of-day:
    ret_cap_h = close[min(i+h, eod_i)] / close[i] - 1
    """
    out = df.copy()
    for asset, g in out.groupby("asset", sort=False):
        idx = g.sort_values("timestamp").index
        gg = out.loc[idx]
        close = pd.to_numeric(gg["close"], errors="coerce").to_numpy(dtype=float)
        ts = pd.to_datetime(gg["timestamp"], errors="coerce", utc=True)
        n = len(gg)
        if n == 0:
            continue

        # Map each row to end-of-day row position
        day = ts.dt.date.to_numpy()
        eod_pos = np.zeros(n, dtype=int)
        start = 0
        for i in range(1, n + 1):
            if i == n or day[i] != day[start]:
                eod_pos[start:i] = i - 1
                start = i

        row = np.arange(n, dtype=int)
        valid_close = ~np.isnan(close)
        for h in horizons:
            col = f"close_ret_cap_{h}m"
            j = row + int(h)
            j = np.minimum(j, eod_pos)
            close_j = close[j]

            ret = np.full(n, np.nan, dtype=float)
            ok = valid_close & ~np.isnan(close_j)
            ret[ok] = close_j[ok] / close[ok] - 1.0
            out.loc[idx, col] = ret
    return out


def add_capped_close_returns_from_next_bar(df: pd.DataFrame, horizons: List[int]) -> pd.DataFrame:
    """
    Build close returns where holding starts from next bar (fill on first next bar),
    capped at end-of-day:
    ret_cap_next_h = close[min((i+1)+h, eod_(i+1))] / close[i+1] - 1
    """
    out = df.copy()
    for asset, g in out.groupby("asset", sort=False):
        idx = g.sort_values("timestamp").index
        gg = out.loc[idx]
        close = pd.to_numeric(gg["close"], errors="coerce").to_numpy(dtype=float)
        ts = pd.to_datetime(gg["timestamp"], errors="coerce", utc=True)
        n = len(gg)
        if n == 0:
            continue

        day = ts.dt.date.to_numpy()
        eod_pos = np.zeros(n, dtype=int)
        start = 0
        for i in range(1, n + 1):
            if i == n or day[i] != day[start]:
                eod_pos[start:i] = i - 1
                start = i

        row = np.arange(n, dtype=int)
        entry_idx = row + 1
        valid_entry = entry_idx < n

        for h in horizons:
            col = f"close_ret_cap_next_{h}m"
            ret = np.full(n, np.nan, dtype=float)

            j = np.minimum(entry_idx + int(h), np.where(valid_entry, eod_pos[np.minimum(entry_idx, n - 1)], n - 1))
            close_entry = np.where(valid_entry, close[np.minimum(entry_idx, n - 1)], np.nan)
            close_exit = np.where(valid_entry, close[j], np.nan)

            ok = valid_entry & ~np.isnan(close_entry) & ~np.isnan(close_exit)
            ret[ok] = close_exit[ok] / close_entry[ok] - 1.0
            out.loc[idx, col] = ret
    return out


def add_next_bar_fields(df: pd.DataFrame, minute_dir: Path) -> pd.DataFrame:
    out = df.copy()
    out["open_next"] = np.nan
    out["high_next"] = np.nan
    out["low_next"] = np.nan
    out["close_next"] = np.nan

    for asset, g in out.groupby("asset", sort=False):
        fp = minute_dir / f"{asset}.csv"
        if not fp.exists():
            LOG.warning("Minute file missing for %s: %s", asset, fp)
            continue
        md = pd.read_csv(fp, usecols=["timestamp", "open", "high", "low", "close"])
        md["timestamp"] = pd.to_datetime(md["timestamp"], errors="coerce", utc=True)
        md = md.sort_values("timestamp")
        md["open_next"] = pd.to_numeric(md["open"], errors="coerce").shift(-1)
        md["high_next"] = pd.to_numeric(md["high"], errors="coerce").shift(-1)
        md["low_next"] = pd.to_numeric(md["low"], errors="coerce").shift(-1)
        md["close_next"] = pd.to_numeric(md["close"], errors="coerce").shift(-1)
        md = md[["timestamp", "open_next", "high_next", "low_next", "close_next"]]

        left_idx = g.index
        merged = out.loc[left_idx, ["timestamp"]].merge(md, on="timestamp", how="left")
        out.loc[left_idx, "open_next"] = merged["open_next"].to_numpy()
        out.loc[left_idx, "high_next"] = merged["high_next"].to_numpy()
        out.loc[left_idx, "low_next"] = merged["low_next"].to_numpy()
        out.loc[left_idx, "close_next"] = merged["close_next"].to_numpy()

    return out


def simulate_short(df: pd.DataFrame, horizon: int, entry_mode: str) -> pd.DataFrame:
    close = pd.to_numeric(df["close"], errors="coerce")
    close_entry = pd.to_numeric(df["close_next"], errors="coerce")
    close_end = close_entry * (1 + pd.to_numeric(df[f"close_ret_cap_next_{horizon}m"], errors="coerce"))

    if entry_mode == "entry_open_first_bar":
        entry = pd.to_numeric(df["open_next"], errors="coerce")
        entered = entry.notna()
    elif entry_mode == "entry_close_calc_bar":
        entry = close
        low_next = pd.to_numeric(df["low_next"], errors="coerce")
        high_next = pd.to_numeric(df["high_next"], errors="coerce")
        entered = entry.notna() & (low_next <= entry) & (high_next >= entry)
    elif entry_mode == "entry_low_calc_bar":
        entry = pd.to_numeric(df["low"], errors="coerce")
        low_next = pd.to_numeric(df["low_next"], errors="coerce")
        high_next = pd.to_numeric(df["high_next"], errors="coerce")
        entered = entry.notna() & (low_next <= entry) & (high_next >= entry)
    else:
        raise ValueError(entry_mode)

    ret_short = pd.Series(np.nan, index=df.index, dtype=float)
    ret_short.loc[entered] = (entry.loc[entered] / close_end.loc[entered]) - 1.0

    out = pd.DataFrame(
        {
            "entered": entered,
            "ret_short": ret_short,
            "win_short": ret_short > 0,
        }
    )
    out["entry_mode"] = entry_mode
    out["horizon_m"] = horizon
    return out


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    if args.horizon_step <= 0:
        raise ValueError("--horizon-step must be > 0")
    if args.horizon_max < args.horizon_step:
        raise ValueError("--horizon-max must be >= --horizon-step")
    horizons = list(range(args.horizon_step, args.horizon_max + 1, args.horizon_step))

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_full_data(args.input_glob)
    df = add_fit_quality(df, window=args.quality_window)
    # Must be computed on full contiguous series (before filtering).
    df = add_capped_close_returns_from_next_bar(df, horizons)

    mask = (
        (pd.to_numeric(df["slope_5"], errors="coerce") > args.slope5_min)
        & (pd.to_numeric(df["slope_20"], errors="coerce") > args.slope20_min)
        & (pd.to_numeric(df["slope_60"], errors="coerce") > args.slope60_min)
        & (pd.to_numeric(df[f"fit_quality_{args.quality_window}"], errors="coerce") >= args.quality_min)
    )
    if args.enforce_slope_order:
        s5 = pd.to_numeric(df["slope_5"], errors="coerce")
        s20 = pd.to_numeric(df["slope_20"], errors="coerce")
        s60 = pd.to_numeric(df["slope_60"], errors="coerce")
        mask &= (s5 > s20) & (s20 > s60)
    if args.volz_min >= 0:
        mask &= pd.to_numeric(df["volZ"], errors="coerce") > args.volz_min

    sdf = df[mask].copy()
    sdf = add_next_bar_fields(sdf, resolve(args.minute_data_dir))
    meta = pd.DataFrame(
        [
            {
                "n_input": int(len(df)),
                "n_selected": int(len(sdf)),
                "selection_rate": float(len(sdf) / len(df)) if len(df) > 0 else np.nan,
                "slope_5_min": args.slope5_min,
                "slope_20_min": args.slope20_min,
                "slope_60_min": args.slope60_min,
                "quality_window": args.quality_window,
                "quality_min": args.quality_min,
                "volz_min": args.volz_min,
                "enforce_slope_order": bool(args.enforce_slope_order),
            }
        ]
    )
    meta.to_csv(out_dir / "short_sim_meta.csv", index=False)

    if sdf.empty:
        LOG.warning("No rows after filters.")
        return

    rows = []
    for h in horizons:
        for mode in ["entry_open_first_bar", "entry_close_calc_bar", "entry_low_calc_bar"]:
            rows.append(simulate_short(sdf, h, mode))

    rr = pd.concat(rows, ignore_index=True)
    rr.to_csv(out_dir / "short_row_level.csv", index=False)

    summ_rows = []
    for (mode, h), g in rr.groupby(["entry_mode", "horizon_m"]):
        n = int(len(g))
        n_ent = int(g["entered"].sum())
        ent = g[g["entered"]]
        summ_rows.append(
            {
                "entry_mode": mode,
                "horizon_m": int(h),
                "n": n,
                "n_entered": n_ent,
                "p_enter": float(n_ent / n) if n > 0 else np.nan,
                "mean_ret_short_given_enter": float(ent["ret_short"].mean()) if n_ent > 0 else np.nan,
                "median_ret_short_given_enter": float(ent["ret_short"].median()) if n_ent > 0 else np.nan,
                "winrate_short_given_enter": float(ent["win_short"].mean()) if n_ent > 0 else np.nan,
            }
        )

    summary = pd.DataFrame(summ_rows).sort_values(["entry_mode", "horizon_m"])
    summary.to_csv(out_dir / "short_summary.csv", index=False)

    # Plots
    n_sel = int(len(sdf))

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=summary, x="horizon_m", y="mean_ret_short_given_enter", hue="entry_mode", marker="o", ax=ax)
    ax.axhline(0.0, color="#444", linewidth=1)
    ax.set_title(f"Short mean return (given enter) | n_selected={n_sel}")
    ax.set_xlabel("Horizon (min)")
    ax.set_ylabel("Mean return")
    fig.tight_layout()
    fig.savefig(out_dir / "short_mean_return_by_horizon.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=summary, x="horizon_m", y="winrate_short_given_enter", hue="entry_mode", marker="o", ax=ax)
    ax.set_ylim(0, 1)
    ax.set_title(f"Short winrate (given enter) | n_selected={n_sel}")
    ax.set_xlabel("Horizon (min)")
    ax.set_ylabel("Winrate")
    fig.tight_layout()
    fig.savefig(out_dir / "short_winrate_by_horizon.png", dpi=150)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(data=summary, x="horizon_m", y="p_enter", hue="entry_mode", marker="o", ax=ax)
    ax.set_ylim(0, 1)
    ax.set_title(f"Entry probability | n_selected={n_sel}")
    ax.set_xlabel("Horizon (min)")
    ax.set_ylabel("P(enter)")
    fig.tight_layout()
    fig.savefig(out_dir / "short_p_enter_by_horizon.png", dpi=150)
    plt.close(fig)

    LOG.info("Short simulation complete. Output: %s", out_dir)


if __name__ == "__main__":
    main()
