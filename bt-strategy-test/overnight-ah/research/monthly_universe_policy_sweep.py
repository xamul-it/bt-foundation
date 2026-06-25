#!/usr/bin/env python3
"""
Sweep walk-forward policies for generating an OvernightAH monthly universe.

Each month-end ranking uses only historical data available through that month.
The generated shortlist is traded during the following month.
"""

from __future__ import annotations

import argparse
import itertools
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_PANEL = Path(__file__).resolve().parent / "out" / "symbol_performance_panel" / "symbol_daily_panel.csv"
DEFAULT_STATIC = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "monthly_universe_policy_sweep"


@dataclass(frozen=True)
class Policy:
    window: int
    score: str
    list_top: int
    min_trades: int
    keep_rank: int
    enter_rank: int
    rank_mode: str

    @property
    def name(self) -> str:
        w = "exp" if self.window == 0 else f"{self.window}m"
        hyst = "nohyst" if self.keep_rank == 0 else f"keep{self.keep_rank}_enter{self.enter_rank}"
        return (
            f"w{w}_{self.score}_top{self.list_top}_trades{self.min_trades}_"
            f"{hyst}_{self.rank_mode}"
        )


def load_tickers(path: Path) -> list[str]:
    with open(path) as f:
        return [str(x).strip() for x in json.load(f) if str(x).strip() and str(x).strip() != "SPY"]


def max_drawdown(returns: pd.Series) -> float:
    eq = (1.0 + returns.fillna(0.0)).cumprod()
    return float((eq / eq.cummax() - 1.0).min()) if len(eq) else np.nan


def sharpe(returns: pd.Series) -> float:
    std = returns.std(ddof=1)
    if std == 0 or pd.isna(std):
        return np.nan
    return float(returns.mean() / std * np.sqrt(252))


def sortino(returns: pd.Series) -> float:
    downside = returns[returns < 0]
    std = downside.std(ddof=1)
    if std == 0 or pd.isna(std):
        return np.nan
    return float(returns.mean() / std * np.sqrt(252))


def metrics(daily: pd.DataFrame, label: str) -> dict:
    r = daily["ret"].dropna()
    return {
        "strategy": label,
        "days": int(len(r)),
        "avg_n": float(daily["n"].mean()) if len(daily) else np.nan,
        "total_pct": float(((1.0 + r).prod() - 1.0) * 100.0) if len(r) else np.nan,
        "mean_bps": float(r.mean() * 10000.0) if len(r) else np.nan,
        "std_bps": float(r.std(ddof=1) * 10000.0) if len(r) > 1 else np.nan,
        "sharpe": sharpe(r),
        "maxdd_pct": max_drawdown(r) * 100.0 if len(r) else np.nan,
        "win_rate_pct": float((r > 0).mean() * 100.0) if len(r) else np.nan,
    }


def annual(daily: pd.DataFrame) -> pd.DataFrame:
    tmp = daily.copy()
    tmp["year"] = tmp["date"].dt.year
    return tmp.groupby(["strategy", "year"]).agg(
        days=("ret", "count"),
        total_pct=("ret", lambda x: ((1.0 + x).prod() - 1.0) * 100.0),
        avg_n=("n", "mean"),
    ).reset_index()


def score_symbols(hist: pd.DataFrame, min_trades: int, score: str) -> pd.DataFrame:
    if hist.empty:
        return pd.DataFrame(columns=["ticker", "rank", "score", "trades", "total_pct", "mean_bps", "sharpe", "sortino"])

    def one(group: pd.DataFrame) -> pd.Series:
        r = group["strategy_ret"].dropna()
        total = ((1.0 + r).prod() - 1.0) * 100.0 if len(r) else np.nan
        return pd.Series(
            {
                "trades": len(r),
                "total_pct": total,
                "mean_bps": r.mean() * 10000.0 if len(r) else np.nan,
                "sharpe": sharpe(r),
                "sortino": sortino(r),
                "win_rate_pct": (r > 0).mean() * 100.0 if len(r) else np.nan,
            }
        )

    scored = hist.groupby("ticker", sort=False).apply(one, include_groups=False).reset_index()
    scored = scored[scored["trades"] >= min_trades].copy()
    if scored.empty:
        return scored
    if score == "total":
        scored["score"] = scored["total_pct"]
    elif score == "mean":
        scored["score"] = scored["mean_bps"]
    elif score == "sharpe":
        scored["score"] = scored["sharpe"]
    elif score == "sortino":
        scored["score"] = scored["sortino"]
    elif score == "composite":
        scored["score"] = (
            scored["total_pct"].rank(pct=True)
            + scored["sharpe"].rank(pct=True)
            + scored["sortino"].rank(pct=True)
            + scored["win_rate_pct"].rank(pct=True)
        )
    else:
        raise ValueError(f"unsupported score: {score}")
    scored = scored.replace([np.inf, -np.inf], np.nan).dropna(subset=["score"])
    scored = scored.sort_values(["score", "trades", "ticker"], ascending=[False, False, True]).reset_index(drop=True)
    scored["rank"] = np.arange(1, len(scored) + 1)
    return scored


def build_lists(trades: pd.DataFrame, months: list[pd.Timestamp], policy: Policy) -> pd.DataFrame:
    rows = []
    previous: list[str] = []
    for month in months:
        if policy.window > 0:
            start = month - pd.offsets.MonthEnd(policy.window - 1)
            hist = trades[(trades["month"] >= start) & (trades["month"] <= month)]
        else:
            hist = trades[trades["month"] <= month]

        scored = score_symbols(hist, policy.min_trades, policy.score)
        if scored.empty:
            previous = []
            continue

        rank_by_ticker = dict(zip(scored["ticker"], scored["rank"]))
        if policy.keep_rank > 0 and previous:
            kept = [t for t in previous if rank_by_ticker.get(t, 10**9) <= policy.keep_rank]
            enter_pool = scored[scored["rank"] <= policy.enter_rank]["ticker"].tolist()
            chosen = kept + [t for t in enter_pool if t not in kept]
        else:
            chosen = scored["ticker"].tolist()
        chosen = chosen[: policy.list_top]
        previous = chosen

        score_map = dict(zip(scored["ticker"], scored["score"]))
        trade_map = dict(zip(scored["ticker"], scored["trades"]))
        raw_rank_map = dict(zip(scored["ticker"], scored["rank"]))
        for rank, ticker in enumerate(chosen, 1):
            rows.append(
                {
                    "month": month,
                    "rank": rank,
                    "ticker": ticker,
                    "score": score_map.get(ticker),
                    "raw_rank": raw_rank_map.get(ticker),
                    "trades": trade_map.get(ticker),
                    "policy": policy.name,
                }
            )
    return pd.DataFrame(rows)


def simulate(panel: pd.DataFrame, lists: pd.DataFrame, policy: Policy, max_concurrent: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    if lists.empty:
        return pd.DataFrame(), pd.DataFrame()
    list_map = {
        month: group.sort_values("rank")["ticker"].tolist()
        for month, group in lists.groupby("month")
    }
    rows = []
    selected_rows = []
    tradable = panel[panel["passes_filters"]].copy()
    tradable["trade_month"] = tradable["date"].dt.to_period("M").dt.to_timestamp("M")
    tradable["rank_month"] = tradable["trade_month"] - pd.offsets.MonthEnd(1)

    for date, day in tradable.groupby("date", sort=True):
        rank_month = day["rank_month"].iloc[0]
        shortlist = list_map.get(rank_month)
        if not shortlist:
            continue
        order = {ticker: i for i, ticker in enumerate(shortlist)}
        candidates = day[day["ticker"].isin(order)].copy()
        if candidates.empty:
            continue
        candidates["order"] = candidates["ticker"].map(order)
        if policy.rank_mode == "adv":
            selected = candidates.sort_values(["adv20", "order"], ascending=[False, True]).head(max_concurrent)
        else:
            selected = candidates.sort_values("order").head(max_concurrent)
        if selected.empty:
            continue
        selected = selected.assign(strategy=policy.name, rank_month=rank_month)
        selected_rows.append(selected)
        rows.append(
            {
                "date": date,
                "rank_month": rank_month,
                "n": len(selected),
                "ret": selected["strategy_ret"].mean(),
                "tickers": ",".join(selected["ticker"].tolist()),
                "strategy": policy.name,
            }
        )
    daily = pd.DataFrame(rows)
    selected = pd.concat(selected_rows, ignore_index=True) if selected_rows else pd.DataFrame()
    return daily, selected


def simulate_static(panel: pd.DataFrame, tickers: list[str], max_concurrent: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    order = {ticker: i for i, ticker in enumerate(tickers)}
    rows = []
    selected_rows = []
    base = panel[panel["passes_filters"] & panel["ticker"].isin(order)].copy()
    base["order"] = base["ticker"].map(order)
    for date, day in base.groupby("date", sort=True):
        selected = day.sort_values("order").head(max_concurrent).copy()
        if selected.empty:
            continue
        selected = selected.assign(strategy="static_stable")
        selected_rows.append(selected)
        rows.append({"date": date, "n": len(selected), "ret": selected["strategy_ret"].mean(), "tickers": ",".join(selected["ticker"]), "strategy": "static_stable"})
    return pd.DataFrame(rows), pd.concat(selected_rows, ignore_index=True) if selected_rows else pd.DataFrame()


def parse_ints(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def parse_strings(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Sweep ex-ante monthly universe policies")
    parser.add_argument("--panel-file", type=Path, default=DEFAULT_PANEL)
    parser.add_argument("--static-ticker-file", type=Path, default=DEFAULT_STATIC)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--windows", default="3,6,12,24,0")
    parser.add_argument("--scores", default="total,mean,sharpe,sortino,composite")
    parser.add_argument("--list-tops", default="10,15")
    parser.add_argument("--min-trades", default="10,20,40")
    parser.add_argument("--hysteresis", default="0:0,20:10,30:15")
    parser.add_argument("--rank-modes", default="adv")
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--save-top", type=int, default=20)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    panel = pd.read_csv(args.panel_file, parse_dates=["date"])
    panel = panel.dropna(subset=["strategy_ret"])
    panel["month"] = panel["date"].dt.to_period("M").dt.to_timestamp("M")
    trades = panel[panel["passes_filters"]].copy()
    months = sorted(trades["month"].drop_duplicates())

    hysteresis = []
    for spec in args.hysteresis.split(","):
        keep, enter = spec.split(":")
        hysteresis.append((int(keep), int(enter)))

    policies = [
        Policy(window=w, score=s, list_top=top, min_trades=mt, keep_rank=keep, enter_rank=enter, rank_mode=rm)
        for w, s, top, mt, (keep, enter), rm in itertools.product(
            parse_ints(args.windows),
            parse_strings(args.scores),
            parse_ints(args.list_tops),
            parse_ints(args.min_trades),
            hysteresis,
            parse_strings(args.rank_modes),
        )
    ]

    daily_frames = []
    selected_frames = []
    list_frames = []
    metric_rows = []

    static_daily, static_selected = simulate_static(panel, load_tickers(args.static_ticker_file), args.max_concurrent)
    daily_frames.append(static_daily)
    selected_frames.append(static_selected)
    metric_rows.append(metrics(static_daily, "static_stable"))

    for i, policy in enumerate(policies, 1):
        lists = build_lists(trades, months, policy)
        daily, selected = simulate(panel, lists, policy, args.max_concurrent)
        if daily.empty:
            continue
        metric = metrics(daily, policy.name)
        metric.update(
            {
                "window": policy.window,
                "score": policy.score,
                "list_top": policy.list_top,
                "min_trades": policy.min_trades,
                "keep_rank": policy.keep_rank,
                "enter_rank": policy.enter_rank,
                "rank_mode": policy.rank_mode,
            }
        )
        metric_rows.append(metric)
        daily_frames.append(daily)
        selected_frames.append(selected)
        list_frames.append(lists)
        if i % 25 == 0:
            print(f"processed {i}/{len(policies)} policies")

    metrics_df = pd.DataFrame(metric_rows).sort_values(["sharpe", "total_pct"], ascending=False)
    top_names = set(metrics_df.head(args.save_top)["strategy"])
    daily_out = pd.concat([df for df in daily_frames if not df.empty and df["strategy"].iloc[0] in top_names], ignore_index=True)
    selected_out = pd.concat([df for df in selected_frames if not df.empty and df["strategy"].iloc[0] in top_names], ignore_index=True)
    lists_out = pd.concat([df for df in list_frames if not df.empty and df["policy"].iloc[0] in top_names], ignore_index=True)
    annual_out = annual(pd.concat(daily_frames, ignore_index=True))
    annual_out = annual_out[annual_out["strategy"].isin(top_names)].sort_values(["year", "strategy"])

    metrics_df.to_csv(args.out_dir / "policy_metrics.csv", index=False)
    daily_out.to_csv(args.out_dir / "policy_daily_top.csv", index=False)
    selected_out.to_csv(args.out_dir / "policy_selected_top.csv", index=False)
    lists_out.to_csv(args.out_dir / "policy_lists_top.csv", index=False)
    annual_out.to_csv(args.out_dir / "policy_annual_top.csv", index=False)

    latest_month = lists_out["month"].max() if not lists_out.empty else None
    if latest_month is not None:
        latest = lists_out[lists_out["month"] == latest_month].sort_values(["policy", "rank"])
        latest.to_csv(args.out_dir / "policy_latest_lists_top.csv", index=False)

    print("TOP METRICS")
    print(metrics_df.head(args.save_top).to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\nOutputs:")
    for name in ["policy_metrics.csv", "policy_annual_top.csv", "policy_latest_lists_top.csv", "policy_daily_top.csv"]:
        print(f"  {args.out_dir / name}")


if __name__ == "__main__":
    main()
