#!/usr/bin/env python3
"""
Combination study for SuperTrend filters.

Builds direction + strength RTH feature filters and ranks them with the three
strategy indicators from 05_supertrend_period_benchmark.py.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[1]
BENCH_SCRIPT = SCRIPT_DIR / "05_supertrend_period_benchmark.py"
DEFAULT_EVENTS = SCRIPT_DIR / "out/supertrend_meta_filter/supertrend_rth_event_dataset_2023_2026.parquet"
DEFAULT_TICKERS = ROOT / "config-common/tickers/rth_stable_candidates_10.json"
DEFAULT_MINUTE_DIR = ROOT / "config-common/data/m/alpaca/sip"
DEFAULT_OUT = SCRIPT_DIR / "out/supertrend_meta_filter/combo_filter_study"


def load_bench_module():
    spec = importlib.util.spec_from_file_location("period_benchmark", BENCH_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def qcut(train: pd.DataFrame, col: str, q: float) -> float:
    return float(train[col].replace([np.inf, -np.inf], np.nan).dropna().quantile(q))


def make_filters(events: pd.DataFrame) -> dict[str, object]:
    train = events[(events["trading_date"] >= "2024-01-01") & (events["trading_date"] <= "2025-12-31")]

    direction_specs = [
        ("mom20_rank", "rth_mom_20_rank_pct", "top", [0.60, 0.70, 0.80]),
        ("momnorm20_rank", "rth_mom_norm_20_rank_pct", "top", [0.60, 0.70, 0.80]),
        ("signed_eff20_rank", "rth_signed_eff_20_rank_pct", "top", [0.60, 0.70, 0.80]),
        ("winrate20", "rth_winrate_20", "top", [0.60, 0.70]),
        ("winrate63", "rth_winrate_63", "top", [0.60, 0.70]),
        ("close_to_range", "rth_close_to_range", "top", [0.60, 0.70, 0.80]),
        ("pressure20", "rth_pressure_balance_20", "top", [0.60, 0.70]),
    ]
    strength_specs = [
        ("rvol_rank", "rth_rvol_5_20_rank_pct", "top", [0.60, 0.70, 0.80]),
        ("rvol", "rth_rvol_5_20", "top", [0.60, 0.70, 0.80]),
        ("log_volume", "log_volume", "top", [0.50, 0.60, 0.70, 0.80]),
        ("rangeexp_rank", "rth_range_exp_5_20_rank_pct", "top", [0.60, 0.70, 0.80]),
        ("logvol", "rth_logvol_z_20", "top", [0.50, 0.60, 0.70]),
        ("eff20_rank", "rth_eff_20_rank_pct", "top", [0.60, 0.70, 0.80]),
        ("std20", "rth_std_20", "top", [0.60, 0.70]),
        ("upside20", "rth_upside_20", "top", [0.60, 0.70]),
        ("downside20", "rth_downside_20", "top", [0.60, 0.70]),
    ]

    def build_terms(specs):
        terms = []
        for label, col, side, qs in specs:
            if col not in train.columns:
                continue
            if train[col].dropna().nunique() < 5:
                continue
            for q in qs:
                cut = qcut(train, col, q if side == "top" else 1.0 - q)
                suffix = f"{int(q * 100)}"
                if side == "top":
                    terms.append((f"{label}_top{suffix}", lambda d, c=col, x=cut: d[c] >= x))
                else:
                    terms.append((f"{label}_bottom{suffix}", lambda d, c=col, x=cut: d[c] <= x))
        return terms

    direction = build_terms(direction_specs)
    strength = build_terms(strength_specs)

    filters = {"st_no_filter": lambda d: pd.Series(True, index=d.index)}
    for d_name, d_fn in direction:
        filters[f"dir__{d_name}"] = d_fn
    for s_name, s_fn in strength:
        filters[f"strength__{s_name}"] = s_fn
    for d_name, d_fn in direction:
        for s_name, s_fn in strength:
            filters[f"combo__{d_name}__{s_name}"] = (
                lambda frame, a=d_fn, b=s_fn: a(frame) & b(frame)
            )
    return filters


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    ap.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    ap.add_argument("--minute-dir", type=Path, default=DEFAULT_MINUTE_DIR)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    bench = load_bench_module()
    events = bench.load_events(args.events)
    periods = bench.default_periods(events)
    tickers = json.loads(args.tickers.read_text())
    _, bench_daily = bench.benchmark_table(tickers, args.minute_dir, periods)

    filters = make_filters(events)
    bench.FILTERS.clear()
    bench.FILTERS.update(filters)

    indicators, daily_alpha = bench.daily_filter_indicators(events, bench_daily, periods)
    args.out.mkdir(parents=True, exist_ok=True)
    indicators.to_csv(args.out / "combo_strategy_indicators.csv", index=False)
    daily_alpha.to_csv(args.out / "combo_daily_alpha.csv", index=False)

    # A compact ranking: prefer 2026 alpha, require the filter to be nonawful in
    # 2024_2025, then inspect the execution/risk indicators.
    y26 = indicators[indicators["period"].eq("2026_ytd")].copy()
    in_sample = indicators[indicators["period"].eq("2024_2025")][
        ["filter", "indicator_1_alpha_vs_benchmark_bps", "indicator_2_trade_goodness", "indicator_3_intrinsic_stability"]
    ].rename(columns={
        "indicator_1_alpha_vs_benchmark_bps": "is_alpha_bps",
        "indicator_2_trade_goodness": "is_trade_goodness",
        "indicator_3_intrinsic_stability": "is_stability",
    })
    rank = y26.merge(in_sample, on="filter", how="left")
    rank = rank[
        (rank["trade_count"] >= 100)
        & (rank["is_alpha_bps"] > -1000)
    ].copy()
    rank["combo_score"] = (
        rank["indicator_1_alpha_vs_benchmark_bps"]
        + rank["is_alpha_bps"].clip(lower=-1000) * 0.25
        + rank["indicator_2_trade_goodness"].clip(lower=-50, upper=100) * 25.0
        + rank["indicator_3_intrinsic_stability"].fillna(0) * 1000.0
    )
    rank = rank.sort_values("combo_score", ascending=False)
    rank.to_csv(args.out / "combo_rank_2026_ytd.csv", index=False)

    show = [
        "filter", "combo_score",
        "indicator_1_alpha_vs_benchmark_bps", "is_alpha_bps",
        "indicator_2_trade_goodness", "indicator_3_intrinsic_stability",
        "trade_count", "edge_bps_per_trade", "avg_trades_per_day",
        "break_even_cost_bps_per_trade", "alpha_net_5bps",
    ]
    print(rank[show].head(30).to_string(index=False))
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
