#!/usr/bin/env python3
"""SQQQ hedge weight modularity + AH-basis study for OvernightAH.

Follow-up to sqqq_hedge_timing_study.py. That study picked a winning
candidate (SMA 50/200 on QQQ close, binary weight 0/0.15 at the crossover)
but the crossover is a lagging, discrete confirmation of a reversal that
actually started earlier, when the fast/slow spread began narrowing (and the
mirror-image at recovery, when the spread starts widening back toward zero
while still negative). This script tests two independent refinements, on top
of the same fixed SMA(50,200) pair (isolate effects, don't re-sweep periods):

1. Weight schemes (0/0.15 binary vs continuous):
   - binary: unchanged baseline, weight = hedge_weight if fast < slow else 0.
   - ramp: weight scales linearly with the normalized spread
     (fast/slow - 1), from 0 at the crossover to hedge_weight once the
     spread reaches -ramp_width.
   - ramp_momentum: same ramp, additionally scaled by whether the spread is
     currently widening (deteriorating trend, scale up) or narrowing back
     toward zero (recovering, scale down), measured over a trailing window.

2. Trend basis (what the SMA is computed on):
   - close: today's convention, SMA on QQQ's raw daily close.
   - ah_index: a synthetic cumulative index built only from QQQ's AH
     (overnight) log-gains (open[t]/close[t-1]-1), consistent with the fact
     the whole strategy trades the AH leg specifically, not the full day.

This is still a pandas proxy (no margin/leverage constraint modeled, same
caveat as sqqq_hedge_timing_study.py) meant to screen which combination is
worth porting to the real Backtrader implementation next.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/sqqq_hedge_modularity_study.py
"""

from __future__ import annotations

import argparse
import sys
from itertools import product
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

DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "sqqq_hedge_modularity_study"

FAST, SLOW = 50, 200
HEDGE_WEIGHT_MAX = 0.15
RAMP_WIDTHS = [0.015, 0.03, 0.05]
MOM_WINDOWS = [10, 20]
MOM_STRENGTHS = [0.5, 1.0]


def build_ah_index(data_dir: Path, ticker: str) -> pd.Series:
    """Cumulative index built only from the AH (overnight) log-gain series,
    open(t)/close(t-1) - 1. Level-comparable to a price series so a plain
    SMA can be computed on it the same way as on QQQ's close."""
    from edge_prediction_study import load_symbol  # local import, same dir

    df = load_symbol(data_dir / f"{ticker}.csv", ticker).sort_values("date").reset_index(drop=True)
    prev_close = df["close"].shift(1)
    ah_ret = (df["open"] / prev_close - 1.0).fillna(0.0)
    index = (1.0 + ah_ret).cumprod()
    return pd.Series(index.to_numpy(), index=df["date"], name=f"{ticker}_ah_index")


def weight_binary(spread: pd.Series, hedge_weight: float, **_) -> pd.Series:
    return pd.Series(np.where(spread < 0.0, hedge_weight, 0.0), index=spread.index)


def weight_ramp(spread: pd.Series, hedge_weight: float, ramp_width: float, **_) -> pd.Series:
    frac = ((0.0 - spread) / ramp_width).clip(lower=0.0, upper=1.0)
    return hedge_weight * frac


def weight_ramp_momentum(
    spread: pd.Series,
    hedge_weight: float,
    ramp_width: float,
    mom_window: int,
    mom_strength: float,
    **_,
) -> pd.Series:
    base = weight_ramp(spread, hedge_weight, ramp_width)
    spread_change = spread - spread.shift(mom_window)
    # spread_change < 0 -> widening further (deteriorating) -> scale weight up
    # spread_change > 0 -> narrowing back toward zero (recovering) -> scale down
    adj = 1.0 - mom_strength * (spread_change / ramp_width).clip(lower=-1.0, upper=1.0)
    adj = adj.clip(lower=0.0, upper=2.0)
    return (base * adj).clip(lower=0.0, upper=hedge_weight)


WEIGHT_SCHEMES = {
    "binary": (weight_binary, [{}]),
    "ramp": (weight_ramp, [{"ramp_width": w} for w in RAMP_WIDTHS]),
    "ramp_momentum": (
        weight_ramp_momentum,
        [
            {"ramp_width": w, "mom_window": mw, "mom_strength": ms}
            for w in RAMP_WIDTHS
            for mw in MOM_WINDOWS
            for ms in MOM_STRENGTHS
        ],
    ),
}


def run(args: argparse.Namespace) -> None:
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    static_members = load_ticker_list(args.static_file)
    static_night_ret = build_static_night_return(args.data_dir, static_members)
    sqqq_night_ret = load_night_return_series(args.data_dir, "SQQQ")
    qqq_close = load_close_series(args.data_dir, "QQQ")
    qqq_ah_index = build_ah_index(args.data_dir, "QQQ")

    frame = pd.concat(
        [
            static_night_ret,
            sqqq_night_ret.rename("sqqq_night_ret"),
            qqq_close.rename("qqq_close"),
            qqq_ah_index.rename("qqq_ah_index"),
        ],
        axis=1,
    )
    frame = frame[frame.index >= pd.Timestamp(args.start)]
    if args.end:
        frame = frame[frame.index <= pd.Timestamp(args.end)]
    frame = frame.dropna(subset=["static_night_ret", "sqqq_night_ret"])
    frame["qqq_close"] = frame["qqq_close"].ffill()
    frame["qqq_ah_index"] = frame["qqq_ah_index"].ffill()

    segments = {
        "full": (None, None),
        "train": (args.start, args.train_end),
        "validation": (args.validation_start, args.validation_end),
        "oos": (args.oos_start, None),
    }

    trend_bases = {"close": frame["qqq_close"], "ah_index": frame["qqq_ah_index"]}

    rows = []
    for basis_name, basis_series in trend_bases.items():
        fast_ma = basis_series.rolling(FAST).mean()
        slow_ma = basis_series.rolling(SLOW).mean()
        spread = fast_ma / slow_ma - 1.0

        for scheme_name, (scheme_fn, param_grid) in WEIGHT_SCHEMES.items():
            for params in param_grid:
                weight = scheme_fn(spread, HEDGE_WEIGHT_MAX, **params)
                weight = weight.reindex(frame.index).fillna(0.0)
                combined_ret = frame["static_night_ret"] + weight * frame["sqqq_night_ret"]

                variant = f"{basis_name}|{scheme_name}|" + ",".join(f"{k}={v}" for k, v in params.items())
                for seg, (start, end) in segments.items():
                    mask = segment_mask(frame.index, start, end)
                    row = {
                        "trend_basis": basis_name,
                        "scheme": scheme_name,
                        "params": str(params),
                        "variant": variant,
                        "segment": seg,
                        "mean_weight_when_on": float(weight[mask][weight[mask] > 0].mean()) if (weight[mask] > 0).any() else 0.0,
                        "pct_nights_on": float((weight[mask] > 0).mean()),
                    }
                    row.update(portfolio_metrics(combined_ret[mask]))
                    rows.append(row)

    # Static-only reference row, same segments.
    for seg, (start, end) in segments.items():
        mask = segment_mask(frame.index, start, end)
        row = {
            "trend_basis": "-", "scheme": "static_only", "params": "-", "variant": "static_only",
            "segment": seg, "mean_weight_when_on": 0.0, "pct_nights_on": 0.0,
        }
        row.update(portfolio_metrics(frame["static_night_ret"][mask]))
        rows.append(row)

    summary = pd.DataFrame(rows)
    summary.to_csv(out_dir / "modularity_summary.csv", index=False)

    # Robustness ranking: min Sharpe across train/validation/oos (same
    # approach used in sqqq_hedge_timing_study.py to avoid picking a
    # candidate that only wins in one segment).
    seg_pivot = summary[summary["segment"].isin(["train", "validation", "oos"])].pivot_table(
        index="variant", columns="segment", values="sharpe"
    )
    seg_pivot["min_sharpe"] = seg_pivot[["train", "validation", "oos"]].min(axis=1)
    seg_pivot["avg_sharpe"] = seg_pivot[["train", "validation", "oos"]].mean(axis=1)
    seg_pivot = seg_pivot.sort_values("min_sharpe", ascending=False)
    seg_pivot.to_csv(out_dir / "modularity_robustness_ranking.csv")

    print(f"wrote {out_dir}")
    print()
    print("=== Robustness ranking (min Sharpe across train/validation/oos), top 15 ===")
    print(seg_pivot.head(15).round(3).to_string())
    print()
    static_min = seg_pivot.loc["static_only", "min_sharpe"] if "static_only" in seg_pivot.index else float("nan")
    print(f"static_only min_sharpe (reference) = {static_min:.3f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQQQ hedge weight modularity + AH-basis study")
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
