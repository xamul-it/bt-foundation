#!/usr/bin/env python3
"""Walk-forward selector for RMA stability grids."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build walk-forward tables from RMA stability results")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--universe", default="global")
    parser.add_argument("--metric", choices=("mean", "median", "sharpe_score"), default="mean")
    parser.add_argument("--train-windows", default="1,2,3,all")
    return parser.parse_args()


def score(train: pd.DataFrame, metric: str) -> pd.Series:
    grouped = train.groupby("param", dropna=False)
    if metric == "median":
        return grouped["window_pnl"].median()
    if metric == "sharpe_score":
        mean = grouped["window_pnl"].mean()
        std = grouped["window_pnl"].std().replace(0, pd.NA)
        return mean / std
    return grouped["window_pnl"].mean()


def main() -> int:
    args = parse_args()
    df = pd.read_csv(args.csv)
    df = df[df["universe"].eq(args.universe)].copy()
    order = ["2024H1", "2024H2", "2025H1", "2025H2", "2026YTD"]
    df["window"] = pd.Categorical(df["window"], categories=order, ordered=True)
    df = df.sort_values(["window", "param"])

    train_sizes = []
    for value in args.train_windows.split(","):
        value = value.strip()
        train_sizes.append(value if value == "all" else int(value))

    rows = []
    for train_size in train_sizes:
        for i, window in enumerate(order):
            if i == 0:
                continue
            previous = order[:i]
            if train_size != "all":
                previous = previous[-int(train_size):]
            train = df[df["window"].isin(previous)]
            test = df[df["window"].eq(window)]
            if train.empty or test.empty:
                continue
            scores = score(train, args.metric).dropna().sort_values(ascending=False)
            if scores.empty:
                continue
            selected = scores.index[0]
            test_row = test[test["param"].eq(selected)].iloc[0]
            oracle_row = test.loc[test["window_pnl"].idxmax()]
            current = test[test["param"].eq("p150_f60_current")]
            current_pnl = current.iloc[0]["window_pnl"] if not current.empty else pd.NA
            rows.append(
                {
                    "universe": args.universe,
                    "metric": args.metric,
                    "train_windows": train_size,
                    "test_window": window,
                    "selected_param": selected,
                    "selected_period": int(test_row["period"]),
                    "selected_fast": int(test_row["fast"]),
                    "selected_pnl": test_row["window_pnl"],
                    "selected_sharpe": test_row["window_sharpe"],
                    "current_pnl": current_pnl,
                    "oracle_param": oracle_row["param"],
                    "oracle_pnl": oracle_row["window_pnl"],
                }
            )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    result = pd.DataFrame(rows)
    result.to_csv(out, index=False)
    if not result.empty:
        summary = (
            result.groupby(["universe", "metric", "train_windows"], dropna=False)
            .agg(
                mean_pnl=("selected_pnl", "mean"),
                median_pnl=("selected_pnl", "median"),
                min_pnl=("selected_pnl", "min"),
                positive_windows=("selected_pnl", lambda s: int((s > 0).sum())),
                current_mean=("current_pnl", "mean"),
                oracle_mean=("oracle_pnl", "mean"),
                windows=("selected_pnl", "count"),
            )
            .reset_index()
        )
        summary.to_csv(out.with_name(out.stem + "_summary.csv"), index=False)
        print(summary.to_string(index=False))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
