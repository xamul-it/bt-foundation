#!/usr/bin/env python3
"""Batch-validate OvernightAH monthly universe policies with Backtrader.

The edge prediction study is useful for screening, but Backtrader is the
authority for portfolio mechanics. This runner executes exported
monthly_universe_file policies and writes a compact comparable summary.
"""

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
DEFAULT_INDEX = RESEARCH_OUT / "edge_prediction_study_all_adj" / "monthly_universes" / "index.csv"
DEFAULT_PROXY = RESEARCH_OUT / "edge_prediction_study_all_adj" / "rotation_summary.csv"
DEFAULT_OUT = RESEARCH_OUT / "edge_prediction_study_all_adj" / "backtrader_validation"
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
    date_col = returns.columns[0]
    ret_col = returns.columns[1]
    dates = pd.to_datetime(returns[date_col], errors="coerce")
    daily = pd.to_numeric(returns[ret_col], errors="coerce").fillna(0.0)
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
    return row


def load_candidates(args: argparse.Namespace) -> pd.DataFrame:
    index = pd.read_csv(args.index)
    if args.features:
        index = index[index["feature"].isin(args.features)]
    if args.top_n:
        index = index[index["top_n"].isin(args.top_n)]

    if args.proxy_sort and args.proxy_summary.exists():
        proxy = pd.read_csv(args.proxy_summary)
        proxy = proxy[(proxy["model"] == "monthly_rotation") & (proxy["segment"] == args.proxy_segment)]
        proxy = proxy[["feature", "top_n", "final_equity", "sharpe", "maxdd_pct"]].rename(
            columns={
                "final_equity": "proxy_final_equity",
                "sharpe": "proxy_sharpe",
                "maxdd_pct": "proxy_maxdd_pct",
            }
        )
        index = index.merge(proxy, on=["feature", "top_n"], how="left")
        index = index.sort_values(["proxy_final_equity", "proxy_sharpe"], ascending=[False, False], na_position="last")

    if args.max_runs:
        index = index.head(args.max_runs)
    return index.reset_index(drop=True)


def run_policy(args: argparse.Namespace, row: pd.Series, out_dir: Path) -> dict:
    feature = str(row["feature"])
    top_n = int(row["top_n"])
    run_id = f"{args.id_prefix}_{feature}_top{top_n}_{args.segment}"
    run_dir = RUN_BASE / run_id
    policy = {
        "feature": feature,
        "top_n": top_n,
        "monthly_universe_file": row["monthly_universe_file"],
        "commission": args.commission,
        "slippage": args.slippage if args.slippage is not None else 0.0,
        "proxy_final_equity": row.get("proxy_final_equity", math.nan),
        "proxy_sharpe": row.get("proxy_sharpe", math.nan),
        "proxy_maxdd_pct": row.get("proxy_maxdd_pct", math.nan),
    }

    if (run_dir / "results.json").exists() and not args.force:
        return summarize_run(run_id, policy)

    monthly_file = str(row["monthly_universe_file"])
    stratargs = f"monthly_universe_file='{monthly_file}' {STRATARGS_BASE}"
    cmd = [
        sys.executable,
        str(BTMAIN),
        "--strat",
        "overnight_ah.OvernightAH",
        "--ticker",
        args.ticker,
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
        stratargs,
    ]
    if args.slippage is not None:
        cmd.extend(["--slippage", str(args.slippage)])
    log_path = out_dir / f"{run_id}.log"
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, text=True, check=False)
    if proc.returncode != 0:
        return {**policy, "run_id": run_id, "status": f"failed:{proc.returncode}", "log": str(log_path.resolve().relative_to(ROOT))}
    return {**summarize_run(run_id, policy), "log": str(log_path.resolve().relative_to(ROOT))}


def add_static_baseline(args: argparse.Namespace, out_dir: Path) -> dict:
    run_id = f"{args.id_prefix}_static_top10_{args.segment}"
    run_dir = RUN_BASE / run_id
    policy = {
        "feature": "static_stable_ah_top10",
        "top_n": 10,
        "monthly_universe_file": "",
        "commission": args.commission,
        "slippage": args.slippage if args.slippage is not None else 0.0,
        "proxy_final_equity": math.nan,
        "proxy_sharpe": math.nan,
        "proxy_maxdd_pct": math.nan,
    }
    if (run_dir / "results.json").exists() and not args.force:
        return summarize_run(run_id, policy)

    cmd = [
        sys.executable,
        str(BTMAIN),
        "--strat",
        "overnight_ah.OvernightAH",
        "--ticker",
        args.static_ticker,
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
    if args.slippage is not None:
        cmd.extend(["--slippage", str(args.slippage)])
    log_path = out_dir / f"{run_id}.log"
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, text=True, check=False)
    if proc.returncode != 0:
        return {**policy, "run_id": run_id, "status": f"failed:{proc.returncode}", "log": str(log_path.resolve().relative_to(ROOT))}
    return {**summarize_run(run_id, policy), "log": str(log_path.resolve().relative_to(ROOT))}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate exported OvernightAH monthly universes with Backtrader")
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX)
    parser.add_argument("--proxy-summary", type=Path, default=DEFAULT_PROXY)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--fromdate", default="2024-01-01")
    parser.add_argument("--todate", default="2026-06-23")
    parser.add_argument("--segment", default="oos")
    parser.add_argument("--ticker", default="yahoo_adj_research_universe.json")
    parser.add_argument("--static-ticker", default="stable_ah_top10.json")
    parser.add_argument("--provider", default="yahoo_adj")
    parser.add_argument("--commission", default="none")
    parser.add_argument("--slippage", type=float, default=None)
    parser.add_argument("--id-prefix", default="edge_batch")
    parser.add_argument("--features", nargs="+", default=None)
    parser.add_argument("--top-n", type=int, nargs="+", default=None)
    parser.add_argument("--max-runs", type=int, default=20)
    parser.add_argument("--proxy-sort", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--proxy-segment", default="oos")
    parser.add_argument("--include-static", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--workers", type=int, default=max((os.cpu_count() or 2) - 2, 1))
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    if args.include_static:
        rows.append(add_static_baseline(args, args.out_dir))
    candidates = load_candidates(args)
    work = [(idx, row) for idx, row in candidates.iterrows()]
    workers = max(1, min(args.workers, len(work) or 1))
    print(f"workers={workers}", flush=True)
    if workers == 1:
        for idx, row in work:
            print(f"[{idx + 1}/{len(candidates)}] {row['feature']} top{int(row['top_n'])}", flush=True)
            rows.append(run_policy(args, row, args.out_dir))
            summary = pd.DataFrame(rows)
            summary.to_csv(args.out_dir / "backtrader_validation_summary.csv", index=False)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(run_policy, args, row, args.out_dir): (idx, row)
                for idx, row in work
            }
            done = 0
            for future in as_completed(futures):
                idx, row = futures[future]
                done += 1
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "feature": row["feature"],
                        "top_n": int(row["top_n"]),
                        "monthly_universe_file": row["monthly_universe_file"],
                        "run_id": f"{args.id_prefix}_{row['feature']}_top{int(row['top_n'])}_{args.segment}",
                        "status": f"exception:{exc}",
                    }
                rows.append(result)
                print(f"[{done}/{len(candidates)}] done {row['feature']} top{int(row['top_n'])}: {result.get('status')}", flush=True)
                summary = pd.DataFrame(rows)
                summary.to_csv(args.out_dir / "backtrader_validation_summary.csv", index=False)

    summary = pd.DataFrame(rows)
    ok = summary[summary["status"] == "ok"].copy()
    if not ok.empty:
        ok = ok.sort_values(["final_value", "sharpe"], ascending=[False, False])
        ok.to_csv(args.out_dir / "backtrader_validation_ranking.csv", index=False)
        print(
            ok[
                [
                    "feature",
                    "top_n",
                    "final_value",
                    "time_return",
                    "trades",
                    "sqn",
                    "sharpe",
                    "daily_sharpe",
                    "cagr_pct",
                    "maxdd_pct",
                ]
            ]
            .head(20)
            .to_string(index=False)
        )
    print(f"wrote {args.out_dir}")


if __name__ == "__main__":
    main()
