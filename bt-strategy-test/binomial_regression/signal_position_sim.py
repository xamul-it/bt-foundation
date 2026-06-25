#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re

import numpy as np
import pandas as pd

LOG = logging.getLogger("signal_position_sim")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Signal-driven position simulation: entry on signal reference price if candle+1 includes it, "
            "TP/SL active from candle+2, reset TP/SL on new signals while in position, timeout close."
        )
    )
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    p.add_argument("--output-dir", default="results/signal_position_sim")

    # signal filters
    p.add_argument("--slope5-min", type=float, default=0.0002)
    p.add_argument("--slope20-min", type=float, default=0.0001)
    p.add_argument("--slope60-min", type=float, default=0.00005)
    p.add_argument("--enforce-slope-order", action="store_true", default=True)
    p.add_argument("--quality-window", type=int, default=60, choices=[5, 20, 60])
    p.add_argument("--quality-min", type=float, default=0.35)
    p.add_argument("--volz-min", type=float, default=-1.0, help="Set negative to disable vol filter.")

    # execution/policy
    p.add_argument("--entry-ref", choices=["close", "low", "high"], default="close")
    p.add_argument("--tp-pct", type=float, default=0.003, help="Take profit percentage (0.003 = 0.3%%)")
    p.add_argument("--sl-pct", type=float, default=0.003, help="Stop loss percentage (0.003 = 0.3%%)")
    p.add_argument("--timeout-bars", type=int, default=10, help="Close after N bars from last signal")
    p.add_argument(
        "--both-hit-policy",
        choices=["conservative", "optimistic"],
        default="conservative",
        help="If TP and SL both touched in same bar: conservative->SL first, optimistic->TP first",
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

    parts = []
    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        asset = m.group(1) if m else "UNK"
        d = pd.read_csv(fp)
        d["asset"] = asset
        parts.append(d)

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


def simulate_asset(g: pd.DataFrame, args: argparse.Namespace) -> tuple[list[dict], dict]:
    g = g.sort_values("timestamp").reset_index(drop=True)

    ts = pd.to_datetime(g["timestamp"], errors="coerce", utc=True)
    close = pd.to_numeric(g["close"], errors="coerce").to_numpy(float)
    high = pd.to_numeric(g["high"], errors="coerce").to_numpy(float)
    low = pd.to_numeric(g["low"], errors="coerce").to_numpy(float)
    s5 = pd.to_numeric(g["slope_5"], errors="coerce").to_numpy(float)
    s20 = pd.to_numeric(g["slope_20"], errors="coerce").to_numpy(float)
    s60 = pd.to_numeric(g["slope_60"], errors="coerce").to_numpy(float)
    fitq = pd.to_numeric(g[f"fit_quality_{args.quality_window}"], errors="coerce").to_numpy(float)
    volz = pd.to_numeric(g["volZ"], errors="coerce").to_numpy(float) if "volZ" in g.columns else np.full(len(g), np.nan)

    if args.entry_ref == "close":
        ref = close
    elif args.entry_ref == "low":
        ref = low
    else:
        ref = high

    n = len(g)
    signal = (s5 > args.slope5_min) & (s20 > args.slope20_min) & (s60 > args.slope60_min) & (fitq >= args.quality_min)
    if args.enforce_slope_order:
        signal &= (s5 > s20) & (s20 > s60)
    if args.volz_min >= 0:
        signal &= volz > args.volz_min

    in_pos = False
    entry_price = np.nan
    entry_bar = -1
    entry_signal_bar = -1

    # dynamic TP/SL state (reset on every new signal while in position)
    tp_price = np.nan
    sl_price = np.nan
    levels_active_from = 10**12
    last_signal_bar = -1

    trades: list[dict] = []

    for i in range(n):
        # 1) Manage open position with bar i market data
        if in_pos:
            exit_reason = None
            exit_price = np.nan

            # TP/SL active from bar+2 of last signal that set levels
            if i >= levels_active_from and not np.isnan(tp_price) and not np.isnan(sl_price):
                hit_tp = (not np.isnan(high[i])) and high[i] >= tp_price
                hit_sl = (not np.isnan(low[i])) and low[i] <= sl_price
                if hit_tp and hit_sl:
                    if args.both_hit_policy == "conservative":
                        exit_reason, exit_price = "sl", sl_price
                    else:
                        exit_reason, exit_price = "tp", tp_price
                elif hit_tp:
                    exit_reason, exit_price = "tp", tp_price
                elif hit_sl:
                    exit_reason, exit_price = "sl", sl_price

            # timeout close at close[i] after N bars from last signal
            if exit_reason is None and last_signal_bar >= 0 and i >= (last_signal_bar + args.timeout_bars):
                if not np.isnan(close[i]):
                    exit_reason, exit_price = "timeout", close[i]

            if exit_reason is not None:
                ret = (exit_price / entry_price) - 1.0
                trades.append(
                    {
                        "asset": g.loc[0, "asset"],
                        "entry_signal_bar": int(entry_signal_bar),
                        "entry_bar": int(entry_bar),
                        "entry_ts": ts.iloc[entry_bar],
                        "entry_price": entry_price,
                        "last_signal_bar": int(last_signal_bar),
                        "exit_bar": int(i),
                        "exit_ts": ts.iloc[i],
                        "exit_price": exit_price,
                        "exit_reason": exit_reason,
                        "ret": ret,
                        "hold_bars": int(i - entry_bar),
                    }
                )

                in_pos = False
                entry_price = np.nan
                entry_bar = -1
                entry_signal_bar = -1
                tp_price = np.nan
                sl_price = np.nan
                levels_active_from = 10**12
                last_signal_bar = -1

        # 2) Process signal at bar i (known at close of bar i)
        if not signal[i]:
            continue

        if i + 1 >= n:
            continue

        ref_px = ref[i]
        if np.isnan(ref_px) or np.isnan(high[i + 1]) or np.isnan(low[i + 1]):
            continue

        # Entry condition on candle 1 for new position
        fill_on_bar1 = (low[i + 1] <= ref_px <= high[i + 1])

        if not in_pos:
            if fill_on_bar1:
                in_pos = True
                entry_price = ref_px
                entry_signal_bar = i
                entry_bar = i + 1

                last_signal_bar = i
                tp_price = ref_px * (1 + args.tp_pct)
                sl_price = ref_px * (1 - args.sl_pct)
                levels_active_from = i + 2
        else:
            # already in position -> stay and reset TP/SL from this new signal reference
            last_signal_bar = i
            tp_price = ref_px * (1 + args.tp_pct)
            sl_price = ref_px * (1 - args.sl_pct)
            levels_active_from = i + 2

    # Close any open position at end of series (forced EOD-like dataset end)
    if in_pos and not np.isnan(close[-1]):
        exit_price = close[-1]
        ret = (exit_price / entry_price) - 1.0
        trades.append(
            {
                "asset": g.loc[0, "asset"],
                "entry_signal_bar": int(entry_signal_bar),
                "entry_bar": int(entry_bar),
                "entry_ts": ts.iloc[entry_bar],
                "entry_price": entry_price,
                "last_signal_bar": int(last_signal_bar),
                "exit_bar": int(n - 1),
                "exit_ts": ts.iloc[n - 1],
                "exit_price": exit_price,
                "exit_reason": "end_of_data",
                "ret": ret,
                "hold_bars": int((n - 1) - entry_bar),
            }
        )

    stats = {
        "asset": g.loc[0, "asset"],
        "n_rows": int(n),
        "n_signals": int(np.nansum(signal)),
        "n_trades": int(len(trades)),
        "n_tp": int(sum(1 for t in trades if t["exit_reason"] == "tp")),
        "n_sl": int(sum(1 for t in trades if t["exit_reason"] == "sl")),
        "n_timeout": int(sum(1 for t in trades if t["exit_reason"] == "timeout")),
        "n_end_of_data": int(sum(1 for t in trades if t["exit_reason"] == "end_of_data")),
    }
    return trades, stats


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    out_dir = resolve(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_full_data(args.input_glob)
    df = add_fit_quality(df, window=args.quality_window)

    all_trades: list[dict] = []
    by_asset_stats: list[dict] = []

    for asset, g in df.groupby("asset", sort=False):
        trades, stats = simulate_asset(g, args)
        all_trades.extend(trades)
        by_asset_stats.append(stats)
        LOG.info("[%s] signals=%d trades=%d tp=%d sl=%d timeout=%d", asset, stats["n_signals"], stats["n_trades"], stats["n_tp"], stats["n_sl"], stats["n_timeout"])

    trades_df = pd.DataFrame(all_trades)
    by_asset_df = pd.DataFrame(by_asset_stats)

    if trades_df.empty:
        by_asset_df.to_csv(out_dir / "sim_stats_by_asset.csv", index=False)
        LOG.warning("No trades generated with current settings.")
        return

    trades_df.to_csv(out_dir / "sim_trades.csv", index=False)

    by_asset_df["mean_ret"] = trades_df.groupby("asset")["ret"].mean().values
    by_asset_df["median_ret"] = trades_df.groupby("asset")["ret"].median().values
    by_asset_df["sum_ret"] = trades_df.groupby("asset")["ret"].sum().values
    by_asset_df["win_rate"] = trades_df.groupby("asset").apply(lambda x: (x["ret"] > 0).mean()).values
    by_asset_df.to_csv(out_dir / "sim_stats_by_asset.csv", index=False)

    overall = {
        "n_trades": int(len(trades_df)),
        "n_tp": int((trades_df["exit_reason"] == "tp").sum()),
        "n_sl": int((trades_df["exit_reason"] == "sl").sum()),
        "n_timeout": int((trades_df["exit_reason"] == "timeout").sum()),
        "n_end_of_data": int((trades_df["exit_reason"] == "end_of_data").sum()),
        "mean_ret": float(trades_df["ret"].mean()),
        "median_ret": float(trades_df["ret"].median()),
        "sum_ret": float(trades_df["ret"].sum()),
        "win_rate": float((trades_df["ret"] > 0).mean()),
    }
    pd.DataFrame([overall]).to_csv(out_dir / "sim_stats_overall.csv", index=False)

    pd.DataFrame(
        [
            {
                "slope5_min": args.slope5_min,
                "slope20_min": args.slope20_min,
                "slope60_min": args.slope60_min,
                "enforce_slope_order": bool(args.enforce_slope_order),
                "quality_window": args.quality_window,
                "quality_min": args.quality_min,
                "volz_min": args.volz_min,
                "entry_ref": args.entry_ref,
                "tp_pct": args.tp_pct,
                "sl_pct": args.sl_pct,
                "timeout_bars": args.timeout_bars,
                "both_hit_policy": args.both_hit_policy,
            }
        ]
    ).to_csv(out_dir / "sim_params.csv", index=False)

    LOG.info("Done. Output: %s", out_dir)


if __name__ == "__main__":
    main()
