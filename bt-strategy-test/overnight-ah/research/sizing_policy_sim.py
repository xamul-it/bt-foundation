#!/usr/bin/env python3
"""Compare OvernightAH sizing policies on daily data."""

from __future__ import annotations

import argparse
import json
import math
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
RESEARCH_OUT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", default="config-common/tickers/stable_ah_top10.json")
    parser.add_argument("--data-dir", default="config-common/data/d/alpaca/sip")
    parser.add_argument("--out-dir", default=str(RESEARCH_OUT / "sizing_policy_sim"))
    parser.add_argument("--fromdate", default="2025-06-03")
    parser.add_argument("--todate", default="2026-06-04")
    parser.add_argument("--base-capital", type=float, default=5000.0)
    parser.add_argument("--max-exposure", type=float, default=1.0)
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--min-intraday-vol", type=float, default=0.025)
    parser.add_argument("--max-intraday-vol", type=float, default=0.045)
    parser.add_argument("--ah-lag1-threshold", type=float, default=-0.1)
    parser.add_argument("--min-adv", type=float, default=100_000_000.0)
    parser.add_argument("--liquidity-lookback", type=int, default=20)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    return parser.parse_args()


def load_symbol(path: Path, symbol: str, liquidity_lookback: int) -> pd.DataFrame:
    df = pd.read_csv(path / f"{symbol}.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["date"] = df["timestamp"].dt.date
    df = df.sort_values("timestamp").drop_duplicates("date", keep="last")
    df["symbol"] = symbol
    df["prev_close"] = df["close"].shift(1)
    df["avg_volume_prev"] = df["volume"].rolling(liquidity_lookback).mean().shift(1)
    df["dollar_adv"] = df["avg_volume_prev"] * df["close"]
    df["next_open"] = df["open"].shift(-1)
    return df


def pass_filter(row: pd.Series, args_dict: dict) -> bool:
    if not math.isfinite(float(row.get("prev_close", float("nan")))):
        return False
    open_price = float(row["open"])
    if open_price <= 0:
        return False
    intraday_vol = (float(row["high"]) - float(row["low"])) / open_price
    if intraday_vol < args_dict["min_intraday_vol"]:
        return False
    if intraday_vol > args_dict["max_intraday_vol"]:
        return False
    dollar_adv = row.get("dollar_adv")
    if args_dict["min_adv"] > 0 and (
        dollar_adv is None or not math.isfinite(float(dollar_adv)) or float(dollar_adv) < args_dict["min_adv"]
    ):
        return False
    if args_dict["ah_lag1_threshold"] < 0:
        ah_lag1 = (open_price - float(row["prev_close"])) / float(row["prev_close"])
        if ah_lag1 < args_dict["ah_lag1_threshold"]:
            return False
    return True


def allocations(candidates: list[dict], args_dict: dict) -> dict[str, list[float]]:
    n = len(candidates)
    max_concurrent = args_dict["max_concurrent"]
    base = args_dict["base_capital"] * args_dict["max_exposure"]
    out: dict[str, list[float]] = {}

    current = []
    for idx in range(n):
        slots_left = max_concurrent - idx
        current.append(base / max(1, slots_left))
    out["current_slots"] = current

    remaining = []
    remaining_capital = base
    for idx in range(n):
        slots_left = max_concurrent - idx
        alloc = remaining_capital / max(1, slots_left)
        remaining.append(alloc)
        remaining_capital -= alloc
    out["remaining_slots"] = remaining

    out["selected_equal"] = [base / n for _ in candidates] if n else []

    weights = list(range(n, 0, -1))
    weight_sum = sum(weights) or 1
    out["rank_decay"] = [base * w / weight_sum for w in weights]
    return out


def simulate_day(payload: tuple[str, list[dict], dict]) -> list[dict]:
    date_text, rows, args_dict = payload
    rows = rows[: args_dict["max_concurrent"]]
    allocs = allocations(rows, args_dict)
    out = []
    for policy, policy_allocs in allocs.items():
        for rank, (row, alloc) in enumerate(zip(rows, policy_allocs), start=1):
            price = float(row["close"])
            shares = int(alloc / price) if price > 0 else 0
            notional = shares * price
            next_open = float(row["next_open"]) if row.get("next_open") is not None else float("nan")
            ret = (next_open / price - 1.0) if shares > 0 and next_open > 0 else float("nan")
            out.append(
                {
                    "date": date_text,
                    "policy": policy,
                    "rank": rank,
                    "symbol": row["symbol"],
                    "close": price,
                    "next_open": next_open,
                    "target_alloc": alloc,
                    "shares": shares,
                    "notional": notional,
                    "overnight_return": ret,
                    "pnl": notional * ret if math.isfinite(ret) else float("nan"),
                }
            )
    return out


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tickers = [s for s in json.loads(Path(args.tickers).read_text()) if s != "SPY"]
    data_dir = Path(args.data_dir)

    frames = []
    for symbol in tickers:
        csv_path = data_dir / f"{symbol}.csv"
        if csv_path.exists():
            frames.append(load_symbol(data_dir, symbol, args.liquidity_lookback))
    df = pd.concat(frames, ignore_index=True)
    df["date_ts"] = pd.to_datetime(df["date"].astype(str))
    df = df[(df["date_ts"] >= pd.to_datetime(args.fromdate)) & (df["date_ts"] <= pd.to_datetime(args.todate))]

    args_dict = {
        "base_capital": args.base_capital,
        "max_exposure": args.max_exposure,
        "max_concurrent": args.max_concurrent,
        "min_intraday_vol": args.min_intraday_vol,
        "max_intraday_vol": args.max_intraday_vol,
        "ah_lag1_threshold": args.ah_lag1_threshold,
        "min_adv": args.min_adv,
    }

    by_date: list[tuple[str, list[dict], dict]] = []
    for date, day in df.groupby("date", sort=True):
        candidates = []
        for symbol in tickers:
            sdf = day[day["symbol"] == symbol]
            if sdf.empty:
                continue
            row = sdf.iloc[0]
            if pass_filter(row, args_dict):
                candidates.append(row.to_dict())
        if candidates:
            by_date.append((str(date), candidates, args_dict))

    workers = max(1, min(args.workers, os.cpu_count() or args.workers))
    all_rows = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for day_rows in pool.map(simulate_day, by_date, chunksize=8):
            all_rows.extend(day_rows)

    trades = pd.DataFrame(all_rows)
    trades.to_csv(out_dir / "trades.csv", index=False)

    summary = trades.groupby("policy").agg(
        days=("date", "nunique"),
        orders=("symbol", "count"),
        avg_orders_per_day=("symbol", lambda s: len(s) / trades.loc[s.index, "date"].nunique()),
        total_notional=("notional", "sum"),
        avg_notional_per_order=("notional", "mean"),
        avg_notional_per_day=("notional", lambda s: trades.loc[s.index].groupby("date")["notional"].sum().mean()),
        avg_gross_target_per_day=("target_alloc", lambda s: trades.loc[s.index].groupby("date")["target_alloc"].sum().mean()),
        sum_pnl=("pnl", "sum"),
        avg_daily_return_on_base=("pnl", lambda s: trades.loc[s.index].groupby("date")["pnl"].sum().mean() / (args.base_capital * args.max_exposure)),
    ).reset_index()
    summary.to_csv(out_dir / "summary.csv", index=False)

    rank_summary = trades.groupby(["policy", "rank"]).agg(
        orders=("symbol", "count"),
        avg_target_alloc=("target_alloc", "mean"),
        avg_notional=("notional", "mean"),
        avg_return=("overnight_return", "mean"),
    ).reset_index()
    rank_summary.to_csv(out_dir / "rank_summary.csv", index=False)

    report = [
        "# Sizing policy simulation",
        "",
        f"Date: {args.fromdate} - {args.todate}",
        f"Base capital: {args.base_capital:.2f}; max_exposure: {args.max_exposure:.2f}; workers: {workers}",
        "",
        "## Summary",
        summary.round(6).to_markdown(index=False),
        "",
        "## Rank Summary",
        rank_summary.round(6).to_markdown(index=False),
        "",
    ]
    (out_dir / "report.md").write_text("\n".join(report), encoding="utf-8")
    print(f"wrote {out_dir} workers={workers} days={len(by_date)} trades={len(trades)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
