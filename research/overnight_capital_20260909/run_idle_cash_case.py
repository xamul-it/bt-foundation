#!/usr/bin/env python3
"""Run the requested fixed-sizing, idle-cash variant of the capital study.

The broker keeps every realised profit as cash.  Order sizing is nevertheless
locked to the initial capital, so no profit or loss can change a later order's
notional.  This is a research-only runner and does not alter production code.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
STUDY = ROOT / "out/overnight_ah_capital_study_20260909"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--cash", type=float, required=True)
    parser.add_argument("--strategy", choices=["legacy", "composite"], default="legacy")
    parser.add_argument("--start", default="2000-01-01")
    parser.add_argument("--end", default="2026-08-14")
    opts = parser.parse_args()

    import sys
    sys.path.insert(0, str(ROOT / "bt-core"))
    import backtrader as bt
    import btmain
    from bt_api import run_backtest
    from strategies.overnight_ah import OvernightAH

    case = STUDY / "idle_cash_cases" / opts.name
    case.mkdir(parents=True, exist_ok=True)

    # Both OvernightAH and its FlatComposite subclass use this method for the
    # stock allocation and the optional hedge.  The real broker cash/equity
    # remains untouched; only the sizing base is frozen at initial capital.
    original_sizing_equity = OvernightAH._sizing_equity
    OvernightAH._sizing_equity = lambda self: float(opts.cash)

    class IdleCashLedger(bt.Analyzer):
        def start(self):
            self.previous = float(self.strategy.broker.getvalue())
            self.rows = []

        def next(self):
            equity = float(self.strategy.broker.getvalue())
            pnl = equity - self.previous
            self.rows.append(dict(
                date=self.strategy.datetime.date(0).isoformat(),
                equity_start=self.previous,
                equity_end=equity,
                daily_pnl=pnl,
                return_on_initial_capital=pnl / float(opts.cash),
                cumulative_pnl=equity - float(opts.cash),
                cash_inactive=max(0.0, float(self.strategy.broker.getcash()) - float(opts.cash)),
            ))
            self.previous = equity

        def stop(self):
            with (case / "ledger.csv").open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(self.rows[0]))
                writer.writeheader()
                writer.writerows(self.rows)

    old_add_analyzers = btmain.add_analyzers

    def with_ledger(opt, cerebro):
        old_add_analyzers(opt, cerebro)
        cerebro.addanalyzer(IdleCashLedger, _name="research_idle_cash_ledger")

    btmain.add_analyzers = with_ledger
    btmain.quantstats.reports.html = lambda *args, **kwargs: None
    logging.disable(logging.INFO)

    saved = json.loads((STUDY / "audit_manifest.json").read_text())["old_run"]
    stratargs = saved["input_stratargs"]
    stratargs += f" entry_signal_lag_bars=1 trade_start_date={opts.start!r}"
    strategy = "overnight_ah.OvernightAH"
    if opts.strategy == "composite":
        import re
        env_text = (ROOT / "config-common/scheduled/strategies/overnight-ah-challenger.env").read_text()
        weights = re.search(r'^STUDY5_WEIGHTS="([^"]+)"', env_text, re.M).group(1)
        strategy = "overnight_ah_flat_composite.OvernightAHFlatComposite"
        stratargs += (
            " monthly_universe_score_mode='flat_panel'"
            f" monthly_universe_indicator_panel={str(STUDY / 'flat_panel_snapshot.csv')!r}"
            f" monthly_universe_flat_weights={weights!r}"
        )
    args = [
        "--strat", strategy, "--ticker", saved["input_ticker"],
        "--data", str(STUDY / "data_snapshot"), "--mode", "backtest",
        "--timeframe", "daily", "--provider", "yahoo_adj", "--fromdate", "2000-01-01",
        "--todate", opts.end, "--commission", "alpaca", "--margin-leverage", "2",
        "--margin-rate", "0.0475", "--cash", str(opts.cash), "--id", opts.name,
        "--stratargs", stratargs,
    ]
    (case / "config.json").write_text(json.dumps(dict(options=vars(opts), cli=args), indent=2))
    try:
        run_backtest(btmain.parse_args(args))
    finally:
        OvernightAH._sizing_equity = original_sizing_equity
    (case / "complete.json").write_text(json.dumps({"status": "complete"}))


if __name__ == "__main__":
    main()
