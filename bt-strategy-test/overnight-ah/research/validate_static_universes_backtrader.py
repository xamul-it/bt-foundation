#!/usr/bin/env python3
"""Batch-validate static OvernightAH ticker lists with Backtrader."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
RESEARCH_OUT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out"
DEFAULT_INDEX = RESEARCH_OUT / "edge_prediction_study_all_adj" / "static_universe_benchmark" / "index.csv"
DEFAULT_OUT = RESEARCH_OUT / "edge_prediction_study_all_adj" / "backtrader_validation_static_benchmark"
BTMAIN = ROOT / "bt-core" / "btmain.py"
RUN_BASE = ROOT / "out" / "overnight_ah" / "OvernightAH"

STRATARGS_BASE = (
    "max_concurrent=5 "
    "size_by_max_concurrent=True "
    "max_exposure=2 "
    "min_intraday_vol=0.025 "
    "max_intraday_vol=0.045 "
    "intraday_vol_filter_side='any' "
    "ah_lag1_threshold=-0.1 "
    "min_adv=100000000 "
    "auction=True"
)


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in value)


def read_single_result(path: Path) -> dict:
    data = json.loads(path.read_text())
    if not data:
        raise ValueError(f"empty results file: {path}")
    return next(iter(data.values()))


def return_metrics(run_dir: Path) -> dict[str, float]:
    returns_path = run_dir / "returns.csv"
    if not returns_path.exists():
        return {"maxdd_pct": math.nan, "daily_sharpe": math.nan, "cagr_pct": math.nan}
    returns = pd.read_csv(returns_path)
    if returns.empty or len(returns.columns) < 2:
        return {"maxdd_pct": math.nan, "daily_sharpe": math.nan, "cagr_pct": math.nan}
    dates = pd.to_datetime(returns.iloc[:, 0], errors="coerce")
    daily = pd.to_numeric(returns.iloc[:, 1], errors="coerce").fillna(0.0)
    equity = (1.0 + daily).cumprod()
    dd = equity / equity.cummax() - 1.0
    sharpe = float(daily.mean() / daily.std(ddof=1) * (252**0.5)) if len(daily) > 1 and daily.std(ddof=1) else math.nan
    valid_dates = dates.dropna()
    if len(valid_dates) >= 2 and len(equity):
        years = max((valid_dates.max() - valid_dates.min()).days / 365.25, 1e-9)
        cagr = float(equity.iloc[-1] ** (1 / years) - 1.0) * 100.0
    else:
        cagr = math.nan
    return {"maxdd_pct": float(dd.min() * 100.0), "daily_sharpe": sharpe, "cagr_pct": cagr}


def trade_metrics(run_dir: Path) -> dict[str, float]:
    trades_path = run_dir / "trades.json"
    if not trades_path.exists():
        return {"win_ratio_pct": math.nan, "edge_bps": math.nan}
    trades = pd.read_json(trades_path)
    if trades.empty or "pnlcomm" not in trades or "value" not in trades:
        return {"win_ratio_pct": math.nan, "edge_bps": math.nan}
    pnl = pd.to_numeric(trades["pnlcomm"], errors="coerce").fillna(0.0)
    value = pd.to_numeric(trades["value"], errors="coerce").fillna(0.0)
    return {
        "win_ratio_pct": float((pnl > 0).mean() * 100.0),
        "edge_bps": float(pnl.sum() / value.sum() * 10000.0) if value.sum() else math.nan,
    }


def summarize_run(run_id: str, policy: dict) -> dict:
    run_dir = RUN_BASE / run_id
    result = read_single_result(run_dir / "results.json")
    start_value = float(result.get("ptf inizio", 0.0))
    pnl_money = float(result.get("PNL_money", math.nan))
    final_value = start_value + pnl_money if pd.notna(pnl_money) else math.nan
    row = {
        **policy,
        "run_id": run_id,
        "status": "ok",
        "final_value": final_value,
        "time_return": result.get("TimeReturn"),
        "trades": result.get("trades"),
        "sqn": result.get("SQN"),
        "sharpe": result.get("Sharpe"),
    }
    row.update(return_metrics(run_dir))
    row.update(trade_metrics(run_dir))
    return row


def run_policy(args: argparse.Namespace, row: pd.Series, out_dir: Path) -> dict:
    name = safe_name(str(row["name"]))
    run_id = f"{args.id_prefix}_{name}_{args.segment}"
    run_dir = RUN_BASE / run_id
    ticker_file = Path(str(row["ticker_file"]))
    if not ticker_file.is_absolute():
        ticker_file = ROOT / ticker_file
    policy = {
        "name": row["name"],
        "method": row.get("method", ""),
        "ticker_file": str(ticker_file),
        "symbols": row.get("symbols", ""),
    }
    if (run_dir / "results.json").exists() and not args.force:
        return summarize_run(run_id, policy)

    cmd = [
        sys.executable,
        str(BTMAIN),
        "--strat",
        "overnight_ah.OvernightAH",
        "--ticker",
        str(ticker_file),
        "--mode",
        "backtest",
        "--timeframe",
        "daily",
        "--provider",
        args.provider,
        "--fromdate",
        args.fromdate,
        "--todate",
        args.todate,
        "--commission",
        args.commission,
        "--margin-leverage",
        "2",
        "--id",
        run_id,
        "--stratargs",
        STRATARGS_BASE,
    ]
    log_path = out_dir / f"{run_id}.log"
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, text=True, check=False)
    if proc.returncode != 0:
        return {**policy, "run_id": run_id, "status": f"failed:{proc.returncode}", "log": str(log_path.resolve().relative_to(ROOT))}
    return {**summarize_run(run_id, policy), "log": str(log_path.resolve().relative_to(ROOT))}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate static OvernightAH ticker-list benchmarks")
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--fromdate", default="2024-01-01")
    parser.add_argument("--todate", default="2026-06-23")
    parser.add_argument("--segment", default="oos")
    parser.add_argument("--provider", default="yahoo_adj")
    parser.add_argument("--commission", default="none")
    parser.add_argument("--id-prefix", default="static_bench")
    parser.add_argument("--methods", nargs="+", default=None)
    parser.add_argument("--names", nargs="+", default=None)
    parser.add_argument("--max-runs", type=int, default=None)
    parser.add_argument("--workers", type=int, default=max((os.cpu_count() or 2) - 2, 1))
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    index = pd.read_csv(args.index)
    if args.methods:
        index = index[index["method"].isin(args.methods)]
    if args.names:
        index = index[index["name"].isin(args.names)]
    if args.max_runs:
        index = index.head(args.max_runs)
    rows = []
    work = [(idx, row) for idx, row in index.reset_index(drop=True).iterrows()]
    workers = max(1, min(args.workers, len(work) or 1))
    print(f"workers={workers}", flush=True)
    if workers == 1:
        for idx, row in work:
            result = run_policy(args, row, args.out_dir)
            rows.append(result)
            pd.DataFrame(rows).to_csv(args.out_dir / "static_validation_summary.csv", index=False)
            print(f"[{idx + 1}/{len(work)}] done {row['name']}: {result.get('status')}", flush=True)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(run_policy, args, row, args.out_dir): (idx, row) for idx, row in work}
            done = 0
            for future in as_completed(futures):
                idx, row = futures[future]
                done += 1
                try:
                    result = future.result()
                except Exception as exc:
                    result = {"name": row["name"], "method": row.get("method", ""), "run_id": f"{args.id_prefix}_{safe_name(str(row['name']))}_{args.segment}", "status": f"exception:{exc}"}
                rows.append(result)
                pd.DataFrame(rows).to_csv(args.out_dir / "static_validation_summary.csv", index=False)
                print(f"[{done}/{len(work)}] done {row['name']}: {result.get('status')}", flush=True)
    summary = pd.DataFrame(rows)
    ok = summary[summary["status"] == "ok"].copy()
    if not ok.empty:
        ok = ok.sort_values(["final_value", "daily_sharpe"], ascending=[False, False])
        ok.to_csv(args.out_dir / "static_validation_ranking.csv", index=False)
        print(ok[["name", "method", "final_value", "trades", "daily_sharpe", "maxdd_pct", "win_ratio_pct", "edge_bps"]].head(25).to_string(index=False))
    print(f"wrote {args.out_dir}")


if __name__ == "__main__":
    main()
