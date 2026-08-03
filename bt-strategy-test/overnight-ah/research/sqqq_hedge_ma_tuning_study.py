#!/usr/bin/env python3
"""SMA vs EMA / period tuning for the SQQQ hedge crossover trigger.

Follow-up to sqqq_hedge_timing_study.py (which tested only 4 SMA pairs) and
sqqq_hedge_modularity_study.py (which found the plain binary crossover and a
linear ramp are statistically indistinguishable, and momentum/AH-index
variants are dominated). This sweeps a wider period grid and both MA types
at the binary weight scheme (0/0.15) already validated on Backtrader, to
check whether 50/200 SMA is actually a good choice or just the best of a
small original set.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/sqqq_hedge_ma_tuning_study.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sqqq_hedge_timing_study import (  # noqa: E402
    DEFAULT_DATA_DIR,
    DEFAULT_STATIC_FILE,
    build_static_night_return,
    load_close_series,
    load_night_return_series,
    load_ticker_list,
    portfolio_metrics,
    segment_mask,
)

DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "sqqq_hedge_ma_tuning_study"

HEDGE_WEIGHT = 0.15
FAST_PERIODS = [10, 20, 30, 40, 50, 65]
SLOW_PERIODS = [100, 150, 200, 252]
MA_TYPES = ["sma", "ema"]
MIN_GAP_RATIO = 1.5  # skip pairs where slow < fast * ratio (too close to be a meaningful trend filter)


def compute_ma(series: pd.Series, period: int, ma_type: str) -> pd.Series:
    if ma_type == "sma":
        return series.rolling(period).mean()
    if ma_type == "ema":
        return series.ewm(span=period, adjust=False, min_periods=period).mean()
    raise ValueError(f"unknown ma_type {ma_type}")


def run(args: argparse.Namespace) -> None:
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    static_members = load_ticker_list(args.static_file)
    static_night_ret = build_static_night_return(args.data_dir, static_members)
    sqqq_night_ret = load_night_return_series(args.data_dir, "SQQQ")
    qqq_close = load_close_series(args.data_dir, "QQQ")

    frame = pd.concat(
        [static_night_ret, sqqq_night_ret.rename("sqqq_night_ret"), qqq_close.rename("qqq_close")],
        axis=1,
    )
    frame = frame[frame.index >= pd.Timestamp(args.start)]
    if args.end:
        frame = frame[frame.index <= pd.Timestamp(args.end)]
    frame = frame.dropna(subset=["static_night_ret", "sqqq_night_ret"])
    frame["qqq_close"] = frame["qqq_close"].ffill()

    segments = {
        "full": (None, None),
        "train": (args.start, args.train_end),
        "validation": (args.validation_start, args.validation_end),
        "oos": (args.oos_start, None),
    }

    rows = []
    for ma_type in MA_TYPES:
        for fast in FAST_PERIODS:
            for slow in SLOW_PERIODS:
                if slow < fast * MIN_GAP_RATIO:
                    continue
                fast_ma = compute_ma(frame["qqq_close"], fast, ma_type)
                slow_ma = compute_ma(frame["qqq_close"], slow, ma_type)
                hedge_on = (fast_ma < slow_ma).fillna(False)
                combined_ret = frame["static_night_ret"] + HEDGE_WEIGHT * frame["sqqq_night_ret"] * hedge_on.astype(float)

                variant = f"{ma_type}_{fast}_{slow}"
                for seg, (start, end) in segments.items():
                    mask = segment_mask(frame.index, start, end)
                    row = {
                        "ma_type": ma_type, "fast": fast, "slow": slow, "variant": variant,
                        "segment": seg, "pct_nights_on": float(hedge_on[mask].mean()),
                    }
                    row.update(portfolio_metrics(combined_ret[mask]))
                    rows.append(row)

    for seg, (start, end) in segments.items():
        mask = segment_mask(frame.index, start, end)
        row = {"ma_type": "-", "fast": None, "slow": None, "variant": "static_only", "segment": seg, "pct_nights_on": 0.0}
        row.update(portfolio_metrics(frame["static_night_ret"][mask]))
        rows.append(row)

    summary = pd.DataFrame(rows)
    summary.to_csv(out_dir / "ma_tuning_summary.csv", index=False)

    seg_pivot = summary[summary["segment"].isin(["train", "validation", "oos"])].pivot_table(
        index="variant", columns="segment", values="sharpe"
    )
    seg_pivot["min_sharpe"] = seg_pivot[["train", "validation", "oos"]].min(axis=1)
    seg_pivot["avg_sharpe"] = seg_pivot[["train", "validation", "oos"]].mean(axis=1)
    seg_pivot = seg_pivot.sort_values("min_sharpe", ascending=False)
    seg_pivot.to_csv(out_dir / "ma_tuning_robustness_ranking.csv")

    print(f"wrote {out_dir}")
    print()
    print("=== Robustness ranking (min Sharpe across train/validation/oos), top 20 ===")
    print(seg_pivot.head(20).round(3).to_string())
    print()
    print(f"static_only min_sharpe (reference) = {seg_pivot.loc['static_only', 'min_sharpe']:.3f}" if "static_only" in seg_pivot.index else "")
    print()
    print("=== Worst 10 (to see how much period choice matters) ===")
    print(seg_pivot.tail(10).round(3).to_string())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQQQ hedge SMA/EMA period tuning study")
    parser.add_argument("--static-file", type=Path, default=DEFAULT_STATIC_FILE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--train-end", default="2020-12-31")
    parser.add_argument("--validation-start", default="2021-01-01")
    parser.add_argument("--validation-end", default="2023-12-31")
    parser.add_argument("--oos-start", default="2024-01-01")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
