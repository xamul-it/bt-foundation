#!/usr/bin/env python3
"""Esegue una grid RMAStrategy (regime/vol-target) e salva un riepilogo risultati."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Grid search for RMAStrategy regime/vol targeting to reduce drawdown"
    )
    parser.add_argument("--ticker", default="NASDAQ_100_US.json")
    parser.add_argument("--fromdate", required=True)
    parser.add_argument("--todate", required=True)
    parser.add_argument("--provider", default="yahoo")
    parser.add_argument("--timeframe", default="daily")
    parser.add_argument("--outdir", default="./out/RMAStrategy")
    parser.add_argument("--id-prefix", default="grid_regime")
    parser.add_argument("--period-fast", default="40,60")
    parser.add_argument("--tstat", default="0.5,1.0")
    parser.add_argument("--pval", default="0.15")
    parser.add_argument("--regime", default="0,1")
    parser.add_argument("--voltarget", default="0,1")
    parser.add_argument("--reserve", default="0.1")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def run_one(repo_root, args, stratargs, run_id, dry_run=False):
    btmain = repo_root / "btmain.py"
    cmd = [
        sys.executable,
        "-c",
        """
import runpy, sys, logging
from pathlib import Path
repo = Path(r'""" + str(repo_root) + """')
btmain = repo / 'btmain.py'
repo_str = str(repo)
site_pkgs = next((p for p in sys.path if p.endswith('site-packages')), None)
sys.path = [p for p in sys.path if p != repo_str]
if site_pkgs and site_pkgs in sys.path:
    sys.path.remove(site_pkgs)
    sys.path.insert(0, site_pkgs)
sys.path.append(repo_str)
_orig_basic_config = logging.basicConfig
def _basic_config_wrapper(*args, **kwargs):
    kwargs['level'] = logging.WARNING
    return _orig_basic_config(*args, **kwargs)
logging.basicConfig = _basic_config_wrapper
sys.argv = [
    str(btmain),
    '--ticker', r'""" + args.ticker + """',
    '--strat', 'weekly.RMAStrategy',
    '--stratargs', r'""" + stratargs + """',
    '--fromdate', r'""" + args.fromdate + """',
    '--todate', r'""" + args.todate + """',
    '--timeframe', r'""" + args.timeframe + """',
    '--provider', r'""" + args.provider + """',
    '--mode', 'backtest',
    '--id', r'""" + run_id + """',
]
runpy.run_path(str(btmain), run_name='__main__')
""",
    ]
    if dry_run:
        print(" ".join(cmd))
        return 0
    return subprocess.run(cmd, cwd=str(repo_root)).returncode


def max_drawdown(returns_csv):
    df = pd.read_csv(returns_csv, index_col=0, parse_dates=True)
    if df.empty:
        return None
    rets = df.iloc[:, 0]
    equity = (1 + rets).cumprod()
    dd = equity / equity.cummax() - 1.0
    return float(dd.min())


def read_results(results_json):
    data = json.loads(Path(results_json).read_text())
    if not data:
        return {}
    last_key = sorted(data.keys(), key=lambda k: int(k))[-1]
    return data[last_key]


def main():
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    period_fast_vals = [v.strip() for v in args.period_fast.split(",") if v.strip()]
    tstat_vals = [v.strip() for v in args.tstat.split(",") if v.strip()]
    pval_vals = [v.strip() for v in args.pval.split(",") if v.strip()]
    regime_vals = [v.strip() for v in args.regime.split(",") if v.strip()]
    vol_vals = [v.strip() for v in args.voltarget.split(",") if v.strip()]

    rows = []
    for pf in period_fast_vals:
        for pv in pval_vals:
            for ts in tstat_vals:
                for rg in regime_vals:
                    for vt in vol_vals:
                        run_id = f"{args.id_prefix}_pf{pf}_pv{pv}_ts{ts}_rg{rg}_vt{vt}"
                        stratargs = (
                            f"period_fast={pf} pval_entry_max={pv} tstat_entry_min={ts} "
                            f"regime_filter={'True' if rg=='1' else 'False'} "
                            f"regime_exit={'True' if rg=='1' else 'False'} "
                            f"use_vol_target={'True' if vt=='1' else 'False'} reserve={args.reserve}"
                        )
                        print(f"Running {run_id}: {stratargs}")
                        code = run_one(repo_root, args, stratargs, run_id, dry_run=args.dry_run)
                        if code != 0:
                            print(f"Run failed: {run_id}")
                            continue

                        run_path = outdir / run_id
                        results_json = run_path / "results.json"
                        returns_csv = run_path / "returns.csv"
                        if not results_json.exists() or not returns_csv.exists():
                            print(f"Missing outputs for {run_id}")
                            continue

                        rec = read_results(results_json)
                        dd = max_drawdown(returns_csv)
                        rows.append({
                            "id": run_id,
                            "period_fast": pf,
                            "pval_entry_max": pv,
                            "tstat_entry_min": ts,
                            "regime_filter": rg,
                            "vol_target": vt,
                            "Sharpe": rec.get("Sharpe"),
                            "PNL": rec.get("PNL"),
                            "Trades": rec.get("trades"),
                            "SQN": rec.get("SQN"),
                            "MaxDD": dd,
                        })

    if rows:
        df = pd.DataFrame(rows)
        df = df.sort_values(by=["MaxDD", "Sharpe"], ascending=[False, False])
        out_csv = outdir / f"{args.id_prefix}_summary.csv"
        df.to_csv(out_csv, index=False)
        print(f"Summary written to {out_csv}")
    else:
        print("No results collected")


if __name__ == "__main__":
    raise SystemExit(main())
