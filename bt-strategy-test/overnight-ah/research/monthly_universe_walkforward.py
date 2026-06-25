#!/usr/bin/env python3
"""
Walk-forward simulation for monthly OvernightAH universe rotation.

Inputs:
  - symbol_daily_panel.csv from symbol_performance_panel.py
  - monthly_lists_*.csv from monthly_universe_lists.py

For each trading day, the script uses the shortlist generated at the previous
month-end and selects up to N symbols that pass the current OvernightAH filters.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_PANEL_DIR = Path(__file__).resolve().parent / "out" / "symbol_performance_panel"
DEFAULT_LISTS_DIR = Path(__file__).resolve().parent / "out" / "monthly_universe_lists"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "monthly_universe_walkforward"
DEFAULT_STATIC = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_UNIVERSE = ROOT / "config-common" / "tickers" / "NASDAQ_100_US.json"


def load_tickers(path: Path) -> list[str]:
    with open(path) as f:
        return [str(t).strip() for t in json.load(f) if str(t).strip() and str(t).strip() != "SPY"]


def max_drawdown(returns: pd.Series) -> float:
    eq = (1 + returns.fillna(0)).cumprod()
    return float((eq / eq.cummax() - 1).min()) if len(eq) else np.nan


def sharpe(returns: pd.Series) -> float:
    std = returns.std(ddof=1)
    if std == 0 or pd.isna(std):
        return np.nan
    return float(returns.mean() / std * np.sqrt(252))


def metrics(daily: pd.DataFrame, label: str) -> dict:
    r = daily["ret"].dropna()
    return {
        "strategy": label,
        "days": int(len(r)),
        "avg_n": float(daily["n"].mean()) if len(daily) else np.nan,
        "total_pct": float(((1 + r).prod() - 1) * 100) if len(r) else np.nan,
        "mean_bps": float(r.mean() * 10000) if len(r) else np.nan,
        "std_bps": float(r.std(ddof=1) * 10000) if len(r) > 1 else np.nan,
        "sharpe": sharpe(r),
        "maxdd_pct": max_drawdown(r) * 100 if len(r) else np.nan,
        "win_rate_pct": float((r > 0).mean() * 100) if len(r) else np.nan,
    }


def annual(daily: pd.DataFrame, label: str) -> pd.DataFrame:
    tmp = daily.copy()
    tmp["year"] = tmp["date"].dt.year
    out = tmp.groupby("year").agg(
        days=("ret", "count"),
        total_pct=("ret", lambda x: ((1 + x).prod() - 1) * 100),
        avg_n=("n", "mean"),
    ).reset_index()
    out["strategy"] = label
    return out


def month_end(ts: pd.Series) -> pd.Series:
    return ts.dt.to_period("M").dt.to_timestamp("M")


def simulate_fixed(
    panel: pd.DataFrame,
    tickers: list[str],
    top: int,
    rank_mode: str,
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    order = {ticker: i for i, ticker in enumerate(tickers)}
    rows = []
    selected_rows = []
    base = panel[panel["ticker"].isin(order) & panel["passes_filters"]].copy()
    if base.empty:
        return pd.DataFrame(), pd.DataFrame()
    base["order"] = base["ticker"].map(order)

    for date, day in base.groupby("date"):
        if rank_mode == "adv":
            selected = day.sort_values(["adv20", "order"], ascending=[False, True]).head(top)
        elif rank_mode == "ret":
            selected = day.sort_values(["strategy_ret", "order"], ascending=[False, True]).head(top)
        else:
            selected = day.sort_values("order").head(top)
        if selected.empty:
            continue
        selected_rows.append(selected.assign(strategy=label))
        rows.append(
            {
                "date": date,
                "n": len(selected),
                "ret": selected["strategy_ret"].mean(),
                "tickers": ",".join(selected["ticker"].tolist()),
                "strategy": label,
            }
        )
    return pd.DataFrame(rows), pd.concat(selected_rows, ignore_index=True) if selected_rows else pd.DataFrame()


def simulate_rotation(
    panel: pd.DataFrame,
    lists: pd.DataFrame,
    top: int,
    rank_mode: str,
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    lists = lists.copy()
    lists["month"] = pd.to_datetime(lists["month"])
    list_map = {
        month: group.sort_values("rank")["ticker"].tolist()
        for month, group in lists.groupby("month")
    }

    rows = []
    selected_rows = []
    tradable = panel[panel["passes_filters"]].copy()
    tradable["trade_month"] = month_end(tradable["date"])
    tradable["rank_month"] = tradable["trade_month"] - pd.offsets.MonthEnd(1)

    for date, day in tradable.groupby("date"):
        rank_month = day["rank_month"].iloc[0]
        shortlist = list_map.get(rank_month)
        if not shortlist:
            continue
        order = {ticker: i for i, ticker in enumerate(shortlist)}
        candidates = day[day["ticker"].isin(order)].copy()
        if candidates.empty:
            continue
        candidates["order"] = candidates["ticker"].map(order)
        if rank_mode == "adv":
            selected = candidates.sort_values(["adv20", "order"], ascending=[False, True]).head(top)
        elif rank_mode == "ret":
            selected = candidates.sort_values(["strategy_ret", "order"], ascending=[False, True]).head(top)
        else:
            selected = candidates.sort_values("order").head(top)
        if selected.empty:
            continue
        selected_rows.append(selected.assign(strategy=label, rank_month=rank_month))
        rows.append(
            {
                "date": date,
                "rank_month": rank_month,
                "n": len(selected),
                "ret": selected["strategy_ret"].mean(),
                "tickers": ",".join(selected["ticker"].tolist()),
                "strategy": label,
            }
        )

    daily = pd.DataFrame(rows)
    selected = pd.concat(selected_rows, ignore_index=True) if selected_rows else pd.DataFrame()
    return daily, selected


def main() -> None:
    parser = argparse.ArgumentParser(description="Monthly universe walk-forward")
    parser.add_argument("--panel-dir", type=Path, default=DEFAULT_PANEL_DIR)
    parser.add_argument("--lists-dir", type=Path, default=DEFAULT_LISTS_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--static-ticker-file", type=Path, default=DEFAULT_STATIC)
    parser.add_argument("--universe-ticker-file", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--score-modes", default="sharpe,sortino,total,composite")
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--list-top", type=int, default=10)
    parser.add_argument("--min-trades", type=int, default=40)
    parser.add_argument("--list-suffix-extra", default="")
    parser.add_argument("--rank-mode", choices=["order", "adv", "ret"], default="order")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    panel = pd.read_csv(args.panel_dir / "symbol_daily_panel.csv", parse_dates=["date"])
    static_tickers = load_tickers(args.static_ticker_file)
    universe_tickers = load_tickers(args.universe_ticker_file)

    daily_frames = []
    selected_frames = []
    metric_rows = []
    annual_frames = []

    static_daily, static_selected = simulate_fixed(panel, static_tickers, args.top, "order", "static_stable")
    universe_daily, universe_selected = simulate_fixed(panel, universe_tickers, args.top, "adv", "nasdaq_adv")
    for daily, selected, label in [
        (static_daily, static_selected, "static_stable"),
        (universe_daily, universe_selected, "nasdaq_adv"),
    ]:
        if not daily.empty:
            daily_frames.append(daily)
            selected_frames.append(selected)
            metric_rows.append(metrics(daily, label))
            annual_frames.append(annual(daily, label))

    for mode in [m.strip() for m in args.score_modes.split(",") if m.strip()]:
        suffix = f"{mode}_top{args.list_top}_trades{args.min_trades}{args.list_suffix_extra}"
        list_path = args.lists_dir / f"monthly_lists_{suffix}.csv"
        lists = pd.read_csv(list_path, parse_dates=["month"])
        label = f"rotation_{mode}"
        daily, selected = simulate_rotation(panel, lists, args.top, args.rank_mode, label)
        if daily.empty:
            continue
        daily_frames.append(daily)
        selected_frames.append(selected)
        metric_rows.append(metrics(daily, label))
        annual_frames.append(annual(daily, label))

    daily_out = pd.concat(daily_frames, ignore_index=True)
    selected_out = pd.concat(selected_frames, ignore_index=True)
    metrics_out = pd.DataFrame(metric_rows).sort_values(["sharpe", "total_pct"], ascending=[False, False])
    annual_out = pd.concat(annual_frames, ignore_index=True).sort_values(["year", "strategy"])

    daily_path = args.out_dir / "walkforward_daily.csv"
    selected_path = args.out_dir / "walkforward_selected.csv"
    metrics_path = args.out_dir / "walkforward_metrics.csv"
    annual_path = args.out_dir / "walkforward_annual.csv"
    daily_out.to_csv(daily_path, index=False)
    selected_out.to_csv(selected_path, index=False)
    metrics_out.to_csv(metrics_path, index=False)
    annual_out.to_csv(annual_path, index=False)

    print("METRICS")
    print(metrics_out.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nANNUAL TOTAL %")
    print(annual_out.pivot(index="year", columns="strategy", values="total_pct").to_string(float_format=lambda x: f"{x:.2f}"))
    print("\nOutputs:")
    for path in [metrics_path, annual_path, daily_path, selected_path]:
        print(f"  {path}")


if __name__ == "__main__":
    main()
