#!/usr/bin/env python3
"""Focused tuner for the current weekly.RMAStrategy command line.

The defaults intentionally mirror the user's current NASDAQ_100_US run:
cash=20000, no commissions, margin-rate=0.1, margin-leverage=1, adjusted
indicators disabled.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ParamSet:
    period: int
    period_fast: int
    max_volatility: float
    trail_stop: float
    selnum: int
    pval_exit_min: float
    tstat_exit_max: float
    use_vol_target: str
    reserve: float
    rebalance_weekday: int

    @property
    def run_id_suffix(self) -> str:
        vol = str(self.max_volatility).replace(".", "p")
        trail = str(self.trail_stop).replace(".", "p")
        pval = str(self.pval_exit_min).replace(".", "p")
        tstat = str(self.tstat_exit_max).replace(".", "p")
        reserve = str(self.reserve).replace(".", "p")
        vt = "vt1" if self.use_vol_target.lower() == "true" else "vt0"
        return (
            f"p{self.period}_f{self.period_fast}_v{vol}_tr{trail}"
            f"_s{self.selnum}_pv{pval}_ts{tstat}_{vt}_r{reserve}"
            f"_rw{self.rebalance_weekday}"
        )


def csv_values(value: str, cast):
    return [cast(item.strip()) for item in value.split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    bt_core = repo_root / "bt-core"
    parser = argparse.ArgumentParser(description="Tune current weekly.RMAStrategy setup")
    parser.add_argument("--python", default=str(bt_core / ".venv" / "bin" / "python"))
    parser.add_argument("--btmain", default=str(bt_core / "btmain.py"))
    parser.add_argument("--ticker", default="NASDAQ_100_US.json")
    parser.add_argument("--fromdate", default="2010-01-01")
    parser.add_argument("--todate", default="")
    parser.add_argument("--provider", default="yahoo")
    parser.add_argument("--timeframe", default="daily")
    parser.add_argument("--cash", default="20000")
    parser.add_argument("--commission", default="none")
    parser.add_argument("--margin-rate", default="0.1")
    parser.add_argument("--margin-leverage", default="1")
    parser.add_argument("--max-adv-participation", default="0.0025")
    parser.add_argument("--amount", default="-1")
    parser.add_argument("--flatten-on-close", default="False")
    parser.add_argument("--regime-filter", default="False")
    parser.add_argument("--use-adj-indicators", default="False")
    parser.add_argument("--periods", default="160,180,200")
    parser.add_argument("--fasts", default="20,30,40")
    parser.add_argument("--max-vols", default="0.065,0.08")
    parser.add_argument(
        "--base-sets",
        default="",
        help="Comma-separated period:fast:max_volatility triples; overrides periods/fasts/max-vols product",
    )
    parser.add_argument(
        "--candidate-sets",
        default="",
        help=(
            "Comma-separated period:fast:max_volatility:selnum:use_vol_target:reserve[:trail_stop] "
            "tuples; overrides base sizing grids"
        ),
    )
    parser.add_argument("--trail-stops", default="0.001")
    parser.add_argument("--selnums", default="2")
    parser.add_argument("--pval-exit-mins", default="0.25")
    parser.add_argument("--tstat-exit-maxs", default="0.3")
    parser.add_argument("--use-vol-targets", default="False")
    parser.add_argument("--reserves", default="0.1")
    parser.add_argument("--rebalance-weekdays", default="0")
    parser.add_argument("--min-ratio", type=float, default=0.0)
    parser.add_argument("--max-ratio", type=float, default=0.0)
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1))
    parser.add_argument("--id-prefix", default="tune_rma_current")
    parser.add_argument("--outdir", default="./out/weekly/RMAStrategy/tuning")
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def paramsets(args: argparse.Namespace) -> list[ParamSet]:
    if args.candidate_sets:
        candidates = []
        for raw_candidate in csv_values(args.candidate_sets, str):
            parts = raw_candidate.split(":")
            if len(parts) not in (6, 7):
                raise ValueError(f"Invalid --candidate-sets entry: {raw_candidate!r}")
            trail_override = None if len(parts) == 6 else float(parts[6])
            candidates.append(
                (
                    int(parts[0]),
                    int(parts[1]),
                    float(parts[2]),
                    int(parts[3]),
                    parts[4],
                    float(parts[5]),
                    trail_override,
                )
            )
        return [
            ParamSet(
                period,
                fast,
                max_vol,
                trail_override if trail_override is not None else trail,
                selnum,
                pval,
                tstat,
                use_vol_target,
                reserve,
                rebalance_weekday,
            )
            for (period, fast, max_vol, selnum, use_vol_target, reserve, trail_override), trail, pval, tstat, rebalance_weekday in itertools.product(
                candidates,
                csv_values(args.trail_stops, float),
                csv_values(args.pval_exit_mins, float),
                csv_values(args.tstat_exit_maxs, float),
                csv_values(args.rebalance_weekdays, int),
            )
            if fast < period
            and (args.min_ratio <= 0 or period / fast >= args.min_ratio)
            and (args.max_ratio <= 0 or period / fast <= args.max_ratio)
        ]

    if args.base_sets:
        bases = []
        for raw_base in csv_values(args.base_sets, str):
            parts = raw_base.split(":")
            if len(parts) != 3:
                raise ValueError(f"Invalid --base-sets entry: {raw_base!r}")
            bases.append((int(parts[0]), int(parts[1]), float(parts[2])))
    else:
        bases = [
            (period, fast, max_vol)
            for period, fast, max_vol in itertools.product(
                csv_values(args.periods, int),
                csv_values(args.fasts, int),
                csv_values(args.max_vols, float),
            )
        ]

    return [
        ParamSet(period, fast, max_vol, trail, selnum, pval, tstat, use_vol_target, reserve, rebalance_weekday)
        for (period, fast, max_vol), trail, selnum, pval, tstat, use_vol_target, reserve, rebalance_weekday in itertools.product(
            bases,
            csv_values(args.trail_stops, float),
            csv_values(args.selnums, int),
            csv_values(args.pval_exit_mins, float),
            csv_values(args.tstat_exit_maxs, float),
            csv_values(args.use_vol_targets, str),
            csv_values(args.reserves, float),
            csv_values(args.rebalance_weekdays, int),
        )
        if fast < period
        and (args.min_ratio <= 0 or period / fast >= args.min_ratio)
        and (args.max_ratio <= 0 or period / fast <= args.max_ratio)
    ]


def result_record(path: Path) -> dict:
    result_path = path / "results.json"
    if not result_path.exists():
        return {}
    data = json.loads(result_path.read_text())
    if not data:
        return {}
    key = sorted(data.keys(), key=lambda item: int(item))[-1]
    return data[key]


def score_returns(path: Path) -> dict:
    returns_path = path / "returns.csv"
    if not returns_path.exists():
        return {"error": f"missing {returns_path}"}

    df = pd.read_csv(returns_path, index_col=0, parse_dates=True)
    if df.empty:
        return {"total_return": 0.0, "maxdd": None, "daily_sharpe": None}

    rets = df.iloc[:, 0].astype(float)
    equity = (1.0 + rets).cumprod()
    dd = equity / equity.cummax() - 1.0
    std = float(rets.std())
    sharpe = None if std == 0 or math.isnan(std) else float((rets.mean() / std) * math.sqrt(252))

    scores = {
        "total_return": float(equity.iloc[-1] - 1.0),
        "maxdd": float(dd.min()),
        "daily_sharpe": sharpe,
    }
    for label, start in (
        ("ret_2024_2026", date(2024, 1, 1)),
        ("ret_2025_2026", date(2025, 1, 1)),
        ("ret_2026", date(2026, 1, 1)),
    ):
        window = rets[rets.index.date >= start]
        if window.empty:
            scores[label] = None
            scores[f"{label}_maxdd"] = None
            continue
        window_equity = (1.0 + window).cumprod()
        scores[label] = float(window_equity.iloc[-1] - 1.0)
        scores[f"{label}_maxdd"] = float((window_equity / window_equity.cummax() - 1.0).min())
    return scores


def run_one(repo: Path, args: argparse.Namespace, params: ParamSet) -> dict:
    run_id = f"{args.id_prefix}_{params.run_id_suffix}"
    out_path = repo / "out" / "weekly" / "RMAStrategy" / run_id
    stratargs = (
        f"selnum={params.selnum} amount={args.amount} "
        f"max_volatility={params.max_volatility} trail_stop={params.trail_stop} "
        f"flatten_on_close={args.flatten_on_close} "
        f"period={params.period} period_fast={params.period_fast} "
        f"regime_filter={args.regime_filter} "
        f"max_adv_participation={args.max_adv_participation} "
        f"use_adj_indicators={args.use_adj_indicators} "
        f"pval_exit_min={params.pval_exit_min} tstat_exit_max={params.tstat_exit_max} "
        f"use_vol_target={params.use_vol_target} reserve={params.reserve} "
        f"rebalance_weekday={params.rebalance_weekday}"
    )
    cmd = [
        args.python,
        args.btmain,
        "--strat=weekly.RMAStrategy",
        f"--ticker={args.ticker}",
        f"--fromdate={args.fromdate}",
        f"--stratargs={stratargs}",
        f"--timeframe={args.timeframe}",
        f"--provider={args.provider}",
        f"--cash={args.cash}",
        f"--commission={args.commission}",
        "--mode=backtest",
        f"--margin-rate={args.margin_rate}",
        f"--margin-leverage={args.margin_leverage}",
        f"--id={run_id}",
    ]
    if args.todate:
        cmd.append(f"--todate={args.todate}")

    row = {
        "run_id": run_id,
        "period": params.period,
        "period_fast": params.period_fast,
        "fast_ratio": params.period_fast / params.period,
        "max_volatility": params.max_volatility,
        "trail_stop": params.trail_stop,
        "selnum": params.selnum,
        "pval_exit_min": params.pval_exit_min,
        "tstat_exit_max": params.tstat_exit_max,
        "use_vol_target": params.use_vol_target,
        "reserve": params.reserve,
        "rebalance_weekday": params.rebalance_weekday,
        "cmd": " ".join(cmd),
    }
    if args.dry_run:
        row["returncode"] = 0
        return row

    has_outputs = (out_path / "results.json").exists() and (out_path / "returns.csv").exists()
    if args.skip_existing and has_outputs:
        row["returncode"] = 0
        row["skipped_existing"] = True
    elif not args.collect_only:
        proc = subprocess.run(
            cmd,
            cwd=str(repo),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        row["returncode"] = proc.returncode
        if proc.returncode != 0:
            row["error"] = proc.stderr[-2000:]
            return row
    else:
        row["returncode"] = 0

    rec = result_record(out_path)
    row.update(
        result_pnl=rec.get("PNL"),
        result_pnl_money=rec.get("PNL_money"),
        result_sharpe=rec.get("Sharpe"),
        trades=rec.get("trades"),
        sqn=rec.get("SQN"),
        duration_sec=rec.get("run_duration_sec"),
    )
    row.update(score_returns(out_path))
    return row


def main() -> int:
    args = parse_args()
    repo = Path(__file__).resolve().parents[2] / "bt-core"
    outdir = repo / args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    params = paramsets(args)
    print(f"jobs={len(params)} workers={args.workers} outdir={outdir}", flush=True)

    rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(run_one, repo, args, params_item) for params_item in params]
        for index, future in enumerate(as_completed(futures), 1):
            row = future.result()
            rows.append(row)
            print(
                f"[{index:03d}/{len(params)}] {row['run_id']} "
                f"rc={row.get('returncode')} ret={row.get('total_return')} "
                f"dd={row.get('maxdd')}",
                flush=True,
            )

    raw_path = outdir / f"{args.id_prefix}_raw.csv"
    fieldnames = sorted({key for row in rows for key in row})
    with raw_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    if rows and not args.dry_run:
        df = pd.DataFrame(rows)
        ok = df[df["returncode"].eq(0)].copy()
        if not ok.empty:
            ok["score"] = (
                ok["total_return"].rank(pct=True)
                + ok["daily_sharpe"].rank(pct=True)
                + ok["ret_2024_2026"].rank(pct=True)
                + ok["maxdd"].rank(pct=True)
            )
            ok.sort_values(["score", "total_return"], ascending=[False, False]).to_csv(
                outdir / f"{args.id_prefix}_ranked.csv",
                index=False,
            )

    print(f"wrote {raw_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
