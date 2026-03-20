#!/usr/bin/env python3
"""
HMA full HTML report — backtest pandas + Monte Carlo + stabilità + analisi LLM.

Uso:
    python bin/HMA/report.py
    python bin/HMA/report.py --symbols NFLX PLTR --from 2025-01-01 --to 2025-03-31
    python bin/HMA/report.py --out /tmp/hma_report.html --no-llm

Output: report HTML self-contained in out/reports/hma_{timestamp}.html
"""
import sys
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parent.parent.parent / "bt-agent"
sys.path.insert(0, str(AGENT_ROOT))

import argparse
from datetime import datetime

from paths import REPORTS_DIR
from agent.agent import BTPandasAgent
from strategies.generic import SignalStrategy
from sim import hma_signal, HMA_TOP9


def parse_args():
    p = argparse.ArgumentParser(description="HMA HTML report completo")
    p.add_argument("--symbols", nargs="+", default=HMA_TOP9)
    p.add_argument("--from",   dest="fromdate", default=None)
    p.add_argument("--to",     dest="todate",   default=None)
    p.add_argument("--period", type=int, default=16)
    p.add_argument("--exitbar", type=int, default=6)
    p.add_argument("--sl",     type=float, default=0.0)
    p.add_argument("--mc-sims", type=int, default=5000, help="Simulazioni Monte Carlo")
    p.add_argument("--out",    default=None, help="Path output HTML")
    p.add_argument("--no-llm", action="store_true", help="Salta analisi LLM (più veloce)")
    return p.parse_args()


def main():
    args = parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.out or str(REPORTS_DIR / f"hma_{ts}.html")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    strategy_cls = SignalStrategy
    strategy_params = {
        "signal_fn": hma_signal,
        "period":    args.period,
        "inverted":  True,
    }
    backtest_params = {
        "exitbar":              args.exitbar,
        "exitbar_mode":         "bt_style",
        "minutes_before_close": 15,
        "sl_pct":               args.sl,
    }

    agent = BTPandasAgent()

    title = (
        f"HMA Simulation — period={args.period} inverted=True exitbar={args.exitbar} "
        f"sl={args.sl}"
    )
    if args.fromdate or args.todate:
        title += f" [{args.fromdate or ''}→{args.todate or ''}]"

    print(f"\nGenerazione report: {title}")
    print(f"Simboli: {args.symbols}")
    print(f"Monte Carlo: {args.mc_sims} simulazioni")
    print(f"LLM: {'NO' if args.no_llm else 'SI'}")
    print()

    # Esegui per ogni simbolo e aggrega
    # Il BTPandasAgent.full_report accetta un singolo simbolo → loop + merge
    reports = []
    for sym in args.symbols:
        print(f"  {sym}...", end=" ", flush=True)
        try:
            path = agent.full_report(
                title=f"{title} — {sym}",
                output_path=str(REPORTS_DIR / f"hma_{sym}_{ts}.html"),
                symbol=sym,
                strategy_cls=strategy_cls,
                strategy_params=strategy_params,
                backtest_params=backtest_params,
                fromdate=args.fromdate,
                todate=args.todate,
                mc_sims=args.mc_sims,
                include_llm=not args.no_llm,
                reproduce_code=open(__file__).read(),
            )
            reports.append(path)
            print("OK")
        except Exception as e:
            print(f"ERRORE: {e}")

    print(f"\nReport generati ({len(reports)}):")
    for r in reports:
        print(f"  {r}")

    if len(reports) == 1:
        print(f"\nApertura: {reports[0]}")


if __name__ == "__main__":
    main()
