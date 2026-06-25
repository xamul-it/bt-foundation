#!/usr/bin/env python3
"""Build static ticker-list benchmarks for OvernightAH universe selection."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
RESEARCH_OUT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out"
DEFAULT_PANEL = RESEARCH_OUT / "edge_prediction_study_all_adj" / "feature_target_panel.csv"
DEFAULT_UNIVERSE = ROOT / "config-common" / "tickers" / "yahoo_adj_research_universe.json"
DEFAULT_STATIC = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_OUT = RESEARCH_OUT / "edge_prediction_study_all_adj" / "static_universe_benchmark"


def load_tickers(path: Path) -> list[str]:
    return [str(x).strip() for x in json.loads(path.read_text()) if str(x).strip()]


def write_ticker_file(path: Path, symbols: list[str]) -> None:
    path.write_text(json.dumps(["SPY", *[s for s in symbols if s != "SPY"]], indent=2) + "\n")


def aggregate_feature(panel: pd.DataFrame, start: str, end: str, feature: str, min_months: int) -> pd.Series:
    p = panel.copy()
    p["month"] = pd.to_datetime(p["month"])
    p = p[(p["month"] >= pd.Timestamp(start)) & (p["month"] <= pd.Timestamp(end))]
    grouped = p.groupby("ticker")[feature].agg(["mean", "count"])
    grouped = grouped[grouped["count"] >= min_months]
    return grouped["mean"].sort_values(ascending=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build static top-N and random ticker-list benchmarks")
    parser.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--static", type=Path, default=DEFAULT_STATIC)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--rank-start", default="2016-01-01")
    parser.add_argument("--rank-end", default="2023-12-31")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--min-months", type=int, default=24)
    parser.add_argument("--random-samples", type=int, default=40)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    panel = pd.read_csv(args.panel)
    universe = [t for t in load_tickers(args.universe) if t != "SPY"]
    static_symbols = [t for t in load_tickers(args.static) if t != "SPY"]

    rows = []
    static_path = args.out_dir / "stable_ah_top10.json"
    write_ticker_file(static_path, static_symbols)
    rows.append(
        {
            "name": "stable_ah_top10",
            "method": "current_static",
            "ticker_file": str(static_path.resolve().relative_to(ROOT)),
            "symbols": ",".join(static_symbols),
        }
    )

    feature_specs = [
        ("hist_target_edge", "target_edge_mean_bps"),
        ("hist_target_win", "target_win_ratio"),
        ("hist_target_total", "target_total_bps"),
        ("hist_c2c_6m", "c2c_mean_6m"),
        ("hist_ah_6m", "ah_mean_6m"),
        ("hist_combo_c2c50_ah50", None),
        ("hist_combo_c2c60_ah40", None),
    ]
    for name, feature in feature_specs:
        if feature is None:
            p = panel.copy()
            p["month"] = pd.to_datetime(p["month"])
            p = p[(p["month"] >= pd.Timestamp(args.rank_start)) & (p["month"] <= pd.Timestamp(args.rank_end))]
            p["score"] = (
                p.groupby("month")["c2c_mean_6m"].rank(pct=True)
                + p.groupby("month")["ah_mean_6m"].rank(pct=True)
            ) / 2.0
            if name.endswith("60_ah40"):
                p["score"] = 0.6 * p.groupby("month")["c2c_mean_6m"].rank(pct=True) + 0.4 * p.groupby("month")["ah_mean_6m"].rank(pct=True)
            ranked = p.groupby("ticker")["score"].agg(["mean", "count"])
            ranked = ranked[ranked["count"] >= args.min_months]["mean"].sort_values(ascending=False)
        else:
            ranked = aggregate_feature(panel, args.rank_start, args.rank_end, feature, args.min_months)
        symbols = [s for s in ranked.index if s in universe][: args.top_n]
        path = args.out_dir / f"{name}_top{args.top_n}.json"
        write_ticker_file(path, symbols)
        rows.append(
            {
                "name": f"{name}_top{args.top_n}",
                "method": f"rank:{feature or name}",
                "ticker_file": str(path.resolve().relative_to(ROOT)),
                "symbols": ",".join(symbols),
            }
        )

    rng = np.random.default_rng(args.seed)
    for idx in range(args.random_samples):
        symbols = sorted(rng.choice(universe, size=args.top_n, replace=False).tolist())
        path = args.out_dir / f"random_{idx:03d}_top{args.top_n}.json"
        write_ticker_file(path, symbols)
        rows.append(
            {
                "name": f"random_{idx:03d}_top{args.top_n}",
                "method": "random",
                "ticker_file": str(path.resolve().relative_to(ROOT)),
                "symbols": ",".join(symbols),
            }
        )

    index = pd.DataFrame(rows)
    index.to_csv(args.out_dir / "index.csv", index=False)
    print(args.out_dir / "index.csv")
    print(index[["name", "method", "symbols"]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
