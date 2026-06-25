#!/usr/bin/env python3
"""Parallel stability study for weekly.RMAStrategy.

Runs btmain.py across time windows, universes and slow/fast parameter pairs,
then scores only the requested evaluation window using returns.csv.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class Window:
    name: str
    start: date
    end: date


@dataclass(frozen=True)
class Universe:
    name: str
    ticker: str


@dataclass(frozen=True)
class ParamSet:
    name: str
    period: int
    fast: int
    max_volatility: float
    selnum: int = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RMA stability study runner")
    parser.add_argument("--workers", type=int, default=min(24, os.cpu_count() or 1))
    parser.add_argument("--cash", default="200000")
    parser.add_argument("--commission", default="fineco")
    parser.add_argument("--max-amount", default="")
    parser.add_argument(
        "--grid",
        choices=("core", "around-180-30"),
        default="core",
        help="Parameter grid to run",
    )
    parser.add_argument("--universes", default="all", help="Comma-separated universe names, or all")
    parser.add_argument("--provider", default="yahoo")
    parser.add_argument("--timeframe", default="daily")
    parser.add_argument("--outdir", default="./out/weekly/RMAStrategy/stability")
    parser.add_argument("--warmup-days", type=int, default=450)
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def windows() -> list[Window]:
    return [
        Window("2024H1", date(2024, 1, 1), date(2024, 6, 30)),
        Window("2024H2", date(2024, 7, 1), date(2024, 12, 31)),
        Window("2025H1", date(2025, 1, 1), date(2025, 6, 30)),
        Window("2025H2", date(2025, 7, 1), date(2025, 12, 31)),
        Window("2026YTD", date(2026, 1, 1), date(2026, 4, 24)),
    ]


def universes() -> list[Universe]:
    return [
        Universe("global", "NASDAQ_100_US.json"),
        Universe("semis_hardware", "N100_semis_hardware.json"),
        Universe("software_cloud", "N100_software_cloud.json"),
        Universe("platforms_media", "N100_platforms_media.json"),
        Universe("healthcare", "N100_healthcare.json"),
        Universe("consumer_industrial", "N100_consumer_industrial.json"),
    ]


def paramsets(grid: str = "core") -> list[ParamSet]:
    if grid == "around-180-30":
        return [
            ParamSet(f"p{period}_f{fast}", period, fast, 0.055)
            for period in (160, 180, 200, 220)
            for fast in (20, 30, 40, 50)
        ]
    return [
        ParamSet("p100_f40_sharpe_semis", 100, 40, 0.055),
        ParamSet("p100_f80_healthcare", 100, 80, 0.055),
        ParamSet("p120_f30_consumer", 120, 30, 0.055),
        ParamSet("p150_f60_current", 150, 60, 0.055),
        ParamSet("p150_f80_avg", 150, 80, 0.055),
        ParamSet("p180_f30_semis_alt", 180, 30, 0.055),
        ParamSet("p200_f30_semis", 200, 30, 0.055),
    ]


def stable_run_id(window: Window, universe: Universe, params: ParamSet) -> str:
    return f"stab_{window.name}_{universe.name}_{params.name}"


def run_one(
    repo: Path,
    args: argparse.Namespace,
    window: Window,
    universe: Universe,
    params: ParamSet,
) -> dict:
    btmain = repo / "btmain.py"
    run_id = stable_run_id(window, universe, params)
    preload_start = window.start - timedelta(days=args.warmup_days)
    stratargs = (
        f"selnum={params.selnum} amount=-1 "
        f"max_volatility={params.max_volatility} "
        f"trail_stop=0.02 flatten_on_close=False "
        f"period={params.period} period_fast={params.fast} "
        f"regime_filter=False"
    )
    if args.max_amount:
        stratargs = f"{stratargs} max_amount={args.max_amount}"
    cmd = [
        sys.executable,
        str(btmain),
        "--strat=weekly.RMAStrategy",
        f"--ticker={universe.ticker}",
        f"--fromdate={preload_start.isoformat()}",
        f"--todate={window.end.isoformat()}",
        f"--stratargs={stratargs}",
        f"--timeframe={args.timeframe}",
        f"--provider={args.provider}",
        f"--cash={args.cash}",
        f"--commission={args.commission}",
        "--mode=backtest",
        f"--id={run_id}",
    ]
    out_path = repo.parent / "out" / "weekly" / "RMAStrategy" / run_id
    if args.dry_run:
        return {
            "run_id": run_id,
            "window": window.name,
            "universe": universe.name,
            "param": params.name,
            "cmd": " ".join(cmd),
        }

    if args.collect_only:
        row = {
            "run_id": run_id,
            "window": window.name,
            "window_start": window.start.isoformat(),
            "window_end": window.end.isoformat(),
            "preload_start": preload_start.isoformat(),
            "universe": universe.name,
            "ticker": universe.ticker,
            "param": params.name,
            "period": params.period,
            "fast": params.fast,
            "ratio": params.fast / params.period,
            "max_volatility": params.max_volatility,
            "selnum": params.selnum,
            "returncode": 0,
        }
        row.update(score_run(out_path, window))
        return row

    proc = subprocess.run(
        cmd,
        cwd=str(repo),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    row = {
        "run_id": run_id,
        "window": window.name,
        "window_start": window.start.isoformat(),
        "window_end": window.end.isoformat(),
        "preload_start": preload_start.isoformat(),
        "universe": universe.name,
        "ticker": universe.ticker,
        "param": params.name,
        "period": params.period,
        "fast": params.fast,
        "ratio": params.fast / params.period,
        "max_volatility": params.max_volatility,
        "selnum": params.selnum,
        "returncode": proc.returncode,
    }
    if proc.returncode != 0:
        row["error"] = proc.stderr[-2000:]
        return row

    row.update(score_run(out_path, window))
    return row


def read_results(path: Path) -> dict:
    result_path = path / "results.json"
    if not result_path.exists():
        return {}
    data = json.loads(result_path.read_text())
    if not data:
        return {}
    key = sorted(data.keys(), key=lambda k: int(k))[-1]
    return data[key]


def score_run(path: Path, window: Window) -> dict:
    returns_path = path / "returns.csv"
    result = read_results(path)
    row = {
        "full_pnl": result.get("PNL"),
        "full_trades": result.get("trades"),
        "full_sqn": result.get("SQN"),
        "full_sharpe": result.get("Sharpe"),
    }
    if not returns_path.exists():
        row["error"] = f"missing {returns_path}"
        return row

    df = pd.read_csv(returns_path, index_col=0, parse_dates=True)
    if df.empty:
        row.update(window_pnl=0.0, window_sharpe=None, window_maxdd=None, days=0)
        return row

    rets = df.iloc[:, 0].astype(float)
    rets = rets[(rets.index.date >= window.start) & (rets.index.date <= window.end)]
    if rets.empty:
        row.update(window_pnl=0.0, window_sharpe=None, window_maxdd=None, days=0)
        return row

    equity = (1.0 + rets).cumprod()
    pnl = float(equity.iloc[-1] - 1.0)
    dd = float((equity / equity.cummax() - 1.0).min())
    std = float(rets.std())
    sharpe = None if std == 0 or math.isnan(std) else float((rets.mean() / std) * math.sqrt(252))
    row.update(window_pnl=pnl, window_sharpe=sharpe, window_maxdd=dd, days=int(len(rets)))
    return row


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[2] / "bt-core"
    outdir = repo / args.outdir
    outdir.mkdir(parents=True, exist_ok=True)

    selected_universes = universes()
    if args.universes != "all":
        wanted = {value.strip() for value in args.universes.split(",") if value.strip()}
        selected_universes = [universe for universe in selected_universes if universe.name in wanted]

    jobs = [
        (window, universe, params)
        for window in windows()
        for universe in selected_universes
        for params in paramsets(args.grid)
    ]
    print(f"jobs={len(jobs)} workers={args.workers} outdir={outdir}", flush=True)

    rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(run_one, repo, args, window, universe, params)
            for window, universe, params in jobs
        ]
        for i, future in enumerate(as_completed(futures), 1):
            row = future.result()
            rows.append(row)
            print(
                f"[{i:03d}/{len(jobs)}] {row['run_id']} "
                f"rc={row.get('returncode')} pnl={row.get('window_pnl')}",
                flush=True,
            )

    raw_csv = outdir / "stability_raw.csv"
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with raw_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    if not args.dry_run:
        df = pd.DataFrame(rows)
        ok = df[df["returncode"].eq(0)].copy()
        summary_param = (
            ok.groupby(["universe", "param", "period", "fast"], dropna=False)
            .agg(
                mean_pnl=("window_pnl", "mean"),
                median_pnl=("window_pnl", "median"),
                min_pnl=("window_pnl", "min"),
                positive_windows=("window_pnl", lambda s: int((s > 0).sum())),
                mean_sharpe=("window_sharpe", "mean"),
                worst_dd=("window_maxdd", "min"),
                windows=("window_pnl", "count"),
            )
            .reset_index()
            .sort_values(["universe", "mean_pnl"], ascending=[True, False])
        )
        summary_window = (
            ok.groupby(["window", "universe"], dropna=False)
            .agg(
                best_pnl=("window_pnl", "max"),
                median_pnl=("window_pnl", "median"),
                worst_pnl=("window_pnl", "min"),
                best_sharpe=("window_sharpe", "max"),
            )
            .reset_index()
            .sort_values(["window", "universe"])
        )
        summary_param.to_csv(outdir / "stability_by_param.csv", index=False)
        summary_window.to_csv(outdir / "stability_by_window.csv", index=False)

    print(f"wrote {raw_csv}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
