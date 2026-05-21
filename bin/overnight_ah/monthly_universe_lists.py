#!/usr/bin/env python3
"""
Generate monthly OvernightAH shortlist candidates from per-symbol 6M metrics.

This is step 2 for rotation research. It does not simulate trading the rotated
universe yet; it only creates and summarizes monthly top-N symbol lists.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_PANEL_DIR = Path(__file__).resolve().parent / "out" / "symbol_performance_panel"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "monthly_universe_lists"


def score_frame(df: pd.DataFrame, mode: str) -> pd.Series:
    if mode == "sharpe":
        return df["sharpe"]
    if mode == "sortino":
        return df["sortino"]
    if mode == "total":
        return df["total_pct"]
    if mode == "mean":
        return df["mean_bps"]
    if mode == "composite":
        sharpe = df["sharpe"].clip(lower=-5, upper=5).fillna(-5)
        sortino = df["sortino"].clip(lower=-8, upper=8).fillna(-8)
        mean = (df["mean_bps"] / 25.0).clip(lower=-5, upper=5).fillna(-5)
        dd_penalty = (df["maxdd_pct"].abs() / 10.0).fillna(5)
        return sharpe + 0.35 * sortino + 0.50 * mean - 0.35 * dd_penalty
    raise ValueError(f"unknown score mode: {mode}")


def generate_lists(
    rolling: pd.DataFrame,
    top: int,
    min_trades: int,
    max_dd: float | None,
    min_sharpe: float | None,
    score_mode: str,
    keep_rank: int | None = None,
    enter_rank: int | None = None,
) -> pd.DataFrame:
    rows = []
    previous: list[str] = []
    for month, group in rolling.groupby("rank_month"):
        candidates = group[group["trades"] >= min_trades].copy()
        if max_dd is not None:
            candidates = candidates[candidates["maxdd_pct"] >= max_dd]
        if min_sharpe is not None:
            candidates = candidates[candidates["sharpe"] >= min_sharpe]
        candidates["score"] = score_frame(candidates, score_mode)
        candidates = candidates.dropna(subset=["score"])
        candidates = candidates.sort_values(
            ["score", "sharpe", "total_pct", "trades", "ticker"],
            ascending=[False, False, False, False, True],
        )
        candidates["candidate_rank"] = range(1, len(candidates) + 1)

        if keep_rank is not None and enter_rank is not None and previous:
            keepable = set(candidates[candidates["candidate_rank"] <= keep_rank]["ticker"])
            enterable = candidates[candidates["candidate_rank"] <= enter_rank]
            selected_symbols = [ticker for ticker in previous if ticker in keepable]
            for ticker in enterable["ticker"]:
                if ticker not in selected_symbols:
                    selected_symbols.append(ticker)
                if len(selected_symbols) >= top:
                    break
            if len(selected_symbols) < top:
                for ticker in candidates["ticker"]:
                    if ticker not in selected_symbols:
                        selected_symbols.append(ticker)
                    if len(selected_symbols) >= top:
                        break
            selected = candidates.set_index("ticker").loc[selected_symbols[:top]].reset_index()
        else:
            selected = candidates.head(top).copy()

        previous = selected["ticker"].tolist()
        for rank, (_, row) in enumerate(selected.iterrows(), start=1):
            rows.append(
                {
                    "month": month,
                    "rank": rank,
                    "ticker": row["ticker"],
                    "score_mode": score_mode,
                    "score": row["score"],
                    "trades": row["trades"],
                    "total_pct": row["total_pct"],
                    "mean_bps": row["mean_bps"],
                    "sharpe": row["sharpe"],
                    "sortino": row["sortino"],
                    "maxdd_pct": row["maxdd_pct"],
                    "win_rate_pct": row["win_rate_pct"],
                }
            )
    return pd.DataFrame(rows)


def summarize_lists(lists: pd.DataFrame, top: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    prev = None
    for month, group in lists.groupby("month"):
        selected = group.sort_values("rank")["ticker"].tolist()
        selected_set = set(selected)
        if prev is None:
            overlap = np.nan
            added = len(selected_set)
            removed = np.nan
        else:
            overlap = len(selected_set & prev)
            added = len(selected_set - prev)
            removed = len(prev - selected_set)
        rows.append(
            {
                "month": month,
                "n": len(selected),
                "symbols": ",".join(selected),
                "overlap_prev": overlap,
                "added": added,
                "removed": removed,
                "turnover_pct": added / top * 100 if selected else np.nan,
                "avg_trades": group["trades"].mean(),
                "avg_sharpe": group["sharpe"].mean(),
                "avg_sortino": group["sortino"].mean(),
                "avg_total_pct": group["total_pct"].mean(),
                "worst_maxdd_pct": group["maxdd_pct"].min(),
            }
        )
        prev = selected_set

    summary = pd.DataFrame(rows)
    freq = (
        lists.groupby("ticker")
        .agg(
            months_selected=("month", "count"),
            avg_rank=("rank", "mean"),
            best_rank=("rank", "min"),
            avg_sharpe=("sharpe", "mean"),
            avg_total_pct=("total_pct", "mean"),
        )
        .reset_index()
        .sort_values(["months_selected", "avg_rank"], ascending=[False, True])
    )
    return summary, freq


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate monthly OvernightAH universe lists")
    parser.add_argument("--panel-dir", type=Path, default=DEFAULT_PANEL_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--ranking-file", default="symbol_rolling_6m.csv")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--min-trades", type=int, default=40)
    parser.add_argument("--max-dd", type=float, default=None)
    parser.add_argument("--min-sharpe", type=float, default=None)
    parser.add_argument("--score-mode", choices=["sharpe", "sortino", "total", "mean", "composite"], default="sharpe")
    parser.add_argument("--keep-rank", type=int, default=None)
    parser.add_argument("--enter-rank", type=int, default=None)
    args = parser.parse_args()

    rolling_path = args.panel_dir / args.ranking_file
    rolling = pd.read_csv(rolling_path, parse_dates=["rank_month", "window_start", "window_end"])
    args.out_dir.mkdir(parents=True, exist_ok=True)

    lists = generate_lists(
        rolling,
        top=args.top,
        min_trades=args.min_trades,
        max_dd=args.max_dd,
        min_sharpe=args.min_sharpe,
        score_mode=args.score_mode,
        keep_rank=args.keep_rank,
        enter_rank=args.enter_rank,
    )
    if lists.empty:
        raise SystemExit("No monthly selections generated")
    summary, freq = summarize_lists(lists, args.top)

    suffix = f"{args.score_mode}_top{args.top}_trades{args.min_trades}"
    if args.keep_rank is not None and args.enter_rank is not None:
        suffix += f"_keep{args.keep_rank}_enter{args.enter_rank}"
    lists_path = args.out_dir / f"monthly_lists_{suffix}.csv"
    summary_path = args.out_dir / f"monthly_summary_{suffix}.csv"
    freq_path = args.out_dir / f"symbol_frequency_{suffix}.csv"
    lists.to_csv(lists_path, index=False)
    summary.to_csv(summary_path, index=False)
    freq.to_csv(freq_path, index=False)

    print(f"Generated months: {summary['month'].nunique()}")
    print(f"Score mode: {args.score_mode}, top={args.top}, min_trades={args.min_trades}")
    print("\nTURNOVER SUMMARY")
    print(summary[["n", "overlap_prev", "added", "removed", "turnover_pct", "avg_sharpe", "worst_maxdd_pct"]].describe().to_string(float_format=lambda x: f"{x:.3f}"))
    print("\nLATEST LIST")
    latest = lists[lists["month"] == lists["month"].max()]
    print(latest[["month", "rank", "ticker", "score", "trades", "total_pct", "sharpe", "sortino", "maxdd_pct"]].to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    print("\nMOST FREQUENT")
    print(freq.head(25).to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    print("\nOutputs:")
    for path in [lists_path, summary_path, freq_path]:
        print(f"  {path}")


if __name__ == "__main__":
    main()
