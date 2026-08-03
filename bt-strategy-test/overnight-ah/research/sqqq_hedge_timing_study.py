#!/usr/bin/env python3
"""SQQQ overnight-hedge timing study for OvernightAH.

Follow-up to `counter_cyclical_symbol_study.py`: SQQQ (Nasdaq -3x) is the
strongest real inverse-AH instrument found (corr -0.52 vs the static basket,
negative in 16/16 years, +336 bps mean / 95% win-rate on the static basket's
worst nights). But SQQQ decays hard when held always-on (leveraged ETF decay
in an up-trending underlying), so an always-on hedge is expected to be a bad
trade on net. This script tests whether a simple, ex-ante, non-lookahead
trend filter on QQQ (the instrument SQQQ inverts) can time the hedge: only
add the SQQQ overnight leg on nights following a QQQ downtrend regime
(fast SMA < slow SMA using data through today's close — the same moment
OvernightAH decides tonight's entries).

Night-return convention (matches OvernightAH's own MOC/MOO leg):
  night(t)            = close(t) -> open(t+1)
  static_night_ret(t) = mean over static members of open(t+1)/close(t) - 1
  sqqq_night_ret(t)   = SQQQ open(t+1)/close(t) - 1
  hedge_on(t)         = SMA_fast(QQQ close, through t) < SMA_slow(QQQ close, through t)
  combined(t)         = static_night_ret(t) + hedge_weight * sqqq_night_ret(t) * hedge_on(t)

This is still a research-stage pandas simulation (equal-weight static book,
additive overlay, no OvernightAH's own filters/sizing/costs). A promising
candidate would need to go through the real OvernightAH sizing/filter path
and a Backtrader run before any promotion, per
`docs/context/strategy-analysis-methodology.md`.

Usage:
  bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/sqqq_hedge_timing_study.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from edge_prediction_study import load_symbol  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STATIC_FILE = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_DATA_DIR = ROOT / "config-common" / "data" / "d" / "yahoo_adj"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "sqqq_hedge_timing_study"

DEFAULT_MA_PAIRS = [(10, 50), (20, 50), (20, 100), (50, 200)]
DEFAULT_WEIGHTS = [0.15, 0.30]


def load_ticker_list(path: Path) -> list[str]:
    return [str(x).strip().upper() for x in json.loads(path.read_text()) if str(x).strip().upper() != "SPY"]


def load_close_series(data_dir: Path, ticker: str) -> pd.Series:
    df = load_symbol(data_dir / f"{ticker}.csv", ticker).sort_values("date")
    return df.set_index("date")["close"]


def load_night_return_series(data_dir: Path, ticker: str) -> pd.Series:
    """night_ret(t) = open(t+1)/close(t) - 1, indexed by t (the entry/decision day)."""
    df = load_symbol(data_dir / f"{ticker}.csv", ticker).sort_values("date").reset_index(drop=True)
    next_open = df["open"].shift(-1)
    ret = next_open / df["close"] - 1.0
    return pd.Series(ret.to_numpy(), index=df["date"], name=ticker)


def build_static_night_return(data_dir: Path, members: list[str]) -> pd.Series:
    series = [load_night_return_series(data_dir, t) for t in members]
    df = pd.concat(series, axis=1)
    return df.mean(axis=1, skipna=True).rename("static_night_ret")


def portfolio_metrics(returns: pd.Series) -> dict[str, float]:
    r = returns.dropna()
    if r.empty:
        return {"n_nights": 0, "total_return_pct": np.nan, "cagr_pct": np.nan, "sharpe": np.nan, "maxdd_pct": np.nan}
    equity = (1.0 + r).cumprod()
    years = max((r.index.max() - r.index.min()).days / 365.25, 1e-9)
    cagr = float(equity.iloc[-1] ** (1 / years) - 1)
    dd = equity / equity.cummax() - 1.0
    sharpe = float(r.mean() / r.std(ddof=1) * np.sqrt(252)) if len(r) > 1 and r.std(ddof=1) else np.nan
    return {
        "n_nights": int(len(r)),
        "total_return_pct": float((equity.iloc[-1] - 1) * 100),
        "cagr_pct": cagr * 100,
        "sharpe": sharpe,
        "maxdd_pct": float(dd.min() * 100),
    }


def segment_mask(dates: pd.Index, start: str | None, end: str | None) -> np.ndarray:
    mask = np.ones(len(dates), dtype=bool)
    if start:
        mask &= dates >= pd.Timestamp(start)
    if end:
        mask &= dates <= pd.Timestamp(end)
    return mask


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

    baseline = frame["static_night_ret"]
    for seg, (start, end) in segments.items():
        mask = segment_mask(frame.index, start, end)
        row = {"variant": "static_only", "ma_fast": None, "ma_slow": None, "hedge_weight": None, "segment": seg}
        row.update(portfolio_metrics(baseline[mask]))
        rows.append(row)

    sqqq_only = sqqq_night_ret[sqqq_night_ret.index >= pd.Timestamp(args.start)]
    row = {"variant": "sqqq_buy_and_hold_100pct", "ma_fast": None, "ma_slow": None, "hedge_weight": None, "segment": "full"}
    row.update(portfolio_metrics(sqqq_only))
    rows.append(row)

    hedge_on_columns = {}
    for fast, slow in args.ma_pairs:
        fast_ma = frame["qqq_close"].rolling(fast).mean()
        slow_ma = frame["qqq_close"].rolling(slow).mean()
        hedge_on = (fast_ma < slow_ma).fillna(False)
        hedge_on_columns[(fast, slow)] = hedge_on

        for weight in args.hedge_weights:
            always_on_ret = frame["static_night_ret"] + weight * frame["sqqq_night_ret"]
            conditional_ret = frame["static_night_ret"] + weight * frame["sqqq_night_ret"] * hedge_on.astype(float)

            for variant_name, series in [
                (f"always_on_w{weight}", always_on_ret),
                (f"conditional_ma{fast}_{slow}_w{weight}", conditional_ret),
            ]:
                for seg, (start, end) in segments.items():
                    mask = segment_mask(frame.index, start, end)
                    row = {
                        "variant": variant_name,
                        "ma_fast": fast,
                        "ma_slow": slow,
                        "hedge_weight": weight,
                        "segment": seg,
                    }
                    row.update(portfolio_metrics(series[mask]))
                    rows.append(row)

    summary_table = pd.DataFrame(rows)
    summary_table.to_csv(out_dir / "hedge_timing_summary.csv", index=False)

    # Tail diagnostic: mean combined return on the static book's own worst 5%
    # nights (ex-post bucket, diagnostic only, not a trading filter) — how
    # much of the tail does each variant actually cover?
    worst_cutoff = frame["static_night_ret"].quantile(args.worst_quantile)
    worst_mask = frame["static_night_ret"] <= worst_cutoff
    tail_rows = [
        {
            "variant": "static_only",
            "n_worst_nights": int(worst_mask.sum()),
            "worst_cutoff_bps": float(worst_cutoff * 10000),
            "mean_return_bps": float(frame.loc[worst_mask, "static_night_ret"].mean() * 10000),
        }
    ]
    for fast, slow in args.ma_pairs:
        hedge_on = hedge_on_columns[(fast, slow)]
        pct_on_in_tail = float(hedge_on[worst_mask].mean())
        for weight in args.hedge_weights:
            conditional_ret = frame["static_night_ret"] + weight * frame["sqqq_night_ret"] * hedge_on.astype(float)
            tail_rows.append(
                {
                    "variant": f"conditional_ma{fast}_{slow}_w{weight}",
                    "n_worst_nights": int(worst_mask.sum()),
                    "worst_cutoff_bps": float(worst_cutoff * 10000),
                    "mean_return_bps": float(conditional_ret[worst_mask].mean() * 10000),
                    "hedge_on_pct_of_worst_nights": pct_on_in_tail,
                }
            )
    tail_table = pd.DataFrame(tail_rows)
    tail_table.to_csv(out_dir / "worst_night_tail_coverage.csv", index=False)

    # Exposure/duration of each MA filter (cost proxy: % of nights hedge is on).
    exposure_rows = []
    for fast, slow in args.ma_pairs:
        hedge_on = hedge_on_columns[(fast, slow)]
        for seg, (start, end) in segments.items():
            mask = segment_mask(frame.index, start, end)
            exposure_rows.append(
                {
                    "ma_fast": fast,
                    "ma_slow": slow,
                    "segment": seg,
                    "pct_nights_hedge_on": float(hedge_on[mask].mean()),
                }
            )
    exposure_table = pd.DataFrame(exposure_rows)
    exposure_table.to_csv(out_dir / "hedge_exposure_by_segment.csv", index=False)

    print(f"wrote {out_dir}")
    print()
    print("=== OOS segment, all variants ===")
    print(summary_table[summary_table["segment"] == "oos"].sort_values("sharpe", ascending=False).to_string(index=False))
    print()
    print("=== Worst-night tail coverage ===")
    print(tail_table.to_string(index=False))
    print()
    print("=== Hedge exposure (% nights on) by segment ===")
    print(exposure_table.to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQQQ overnight-hedge timing study for OvernightAH")
    parser.add_argument("--static-file", type=Path, default=DEFAULT_STATIC_FILE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--start", default="2010-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--train-end", default="2020-12-31")
    parser.add_argument("--validation-start", default="2021-01-01")
    parser.add_argument("--validation-end", default="2023-12-31")
    parser.add_argument("--oos-start", default="2024-01-01")
    parser.add_argument("--ma-pairs", type=int, nargs=2, action="append", dest="ma_pairs", default=None)
    parser.add_argument("--hedge-weights", type=float, nargs="+", default=DEFAULT_WEIGHTS)
    parser.add_argument("--worst-quantile", type=float, default=0.05)
    args = parser.parse_args()
    if not args.ma_pairs:
        args.ma_pairs = DEFAULT_MA_PAIRS
    return args


if __name__ == "__main__":
    run(parse_args())
