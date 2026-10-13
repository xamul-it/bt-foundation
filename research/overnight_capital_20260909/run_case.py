"""Research-only Backtrader runner with explicit external withdrawal accounting.

Uses production feed, broker, strategy, fills and fees. Changes only the
research process: injects a pre-entry withdrawal hook and a daily ledger.
The production source tree and scheduled profiles are never edited.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
STUDY = ROOT / 'out/overnight_ah_capital_study_20260909'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=True)
    parser.add_argument('--cash', type=float, default=50000)
    parser.add_argument('--strategy', choices=['legacy', 'composite'], default='legacy')
    parser.add_argument('--band', type=float, default=0.2)
    parser.add_argument('--no-withdrawals', action='store_true')
    parser.add_argument('--no-cost', action='store_true')
    parser.add_argument('--cooldown', type=float, default=0.03)
    parser.add_argument('--start', default='2000-01-01')
    parser.add_argument('--end', default='2026-08-14')
    opts = parser.parse_args()
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT / 'bt-core'))
    import backtrader as bt
    import btmain
    from bt_api import run_backtest
    from strategies.overnight_ah import OvernightAH

    case = STUDY / 'cases' / opts.name
    case.mkdir(parents=True, exist_ok=True)
    original_next = OvernightAH.next

    def research_next(self):
        day = self.datetime.date(0)
        if getattr(self, '_research_day', None) != day:
            self._research_day = day
            self._research_withdrawal = 0.0
            equity = float(self.broker.getvalue())
            flat = not any(p.size for p in self.broker.positions.values())
            pending = any(o.alive() for o in self.broker.get_orders_open())
            submitted = any(o.alive() for o in self.broker.get_orders_submitted())
            self._research_deferred = int(
                not opts.no_withdrawals and equity > opts.cash * (1 + opts.band)
                and (not flat or pending or submitted)
            )
            if (not opts.no_withdrawals and day.isoformat() >= opts.start
                    and equity > opts.cash * (1 + opts.band)
                    and flat and not pending and not submitted):
                amount = equity - opts.cash
                self.broker.add_cash(-amount)
                # Backtrader queues fund flows; apply before today's sizing.
                self.broker._get_value()
                after = float(self.broker.getvalue())
                assert abs(after - opts.cash) < max(0.01, opts.cash * 1e-10)
                self._research_withdrawal = amount
        return original_next(self)

    OvernightAH.next = research_next

    class CashFlowLedger(bt.Analyzer):
        def start(self):
            self.previous = float(self.strategy.broker.getvalue())
            self.total_withdrawn = 0.0
            self.rows = []

        def next(self):
            equity = float(self.strategy.broker.getvalue())
            flow = float(getattr(self.strategy, '_research_withdrawal', 0.0))
            self.total_withdrawn += flow
            pnl = equity + flow - self.previous
            ret = pnl / self.previous if self.previous > 0 else 0.0
            self.rows.append(dict(
                date=self.strategy.datetime.date(0).isoformat(),
                equity_start=self.previous, equity_end=equity,
                withdrawal=flow, cumulative_withdrawals=self.total_withdrawn,
                pnl=pnl, return_adjusted=ret,
                wealth=equity + self.total_withdrawn,
                withdrawal_deferred=getattr(self.strategy, '_research_deferred', 0),
            ))
            self.previous = equity

        def stop(self):
            with (case / 'ledger.csv').open('w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=list(self.rows[0]))
                writer.writeheader()
                writer.writerows(self.rows)

    add_analyzers = btmain.add_analyzers

    def with_ledger(opt, cerebro):
        add_analyzers(opt, cerebro)
        cerebro.addanalyzer(CashFlowLedger, _name='research_cash_flow')

    btmain.add_analyzers = with_ledger
    # Standard raw outputs remain available; the study builds a combined report.
    btmain.quantstats.reports.html = lambda *args, **kwargs: None
    logging.disable(logging.INFO)
    saved = json.loads((STUDY / 'audit_manifest.json').read_text())['old_run']
    stratargs = saved['input_stratargs']
    stratargs = stratargs.replace('post_up_cooldown_threshold=0.03',
                                  f'post_up_cooldown_threshold={opts.cooldown}')
    stratargs += f' entry_signal_lag_bars=1 trade_start_date={opts.start!r}'
    strategy = 'overnight_ah.OvernightAH'
    if opts.strategy == 'composite':
        import re
        env_text = (ROOT / 'config-common/scheduled/strategies/overnight-ah-challenger.env').read_text()
        weights = re.search(r'^STUDY5_WEIGHTS="([^"]+)"', env_text, re.M).group(1)
        strategy = 'overnight_ah_flat_composite.OvernightAHFlatComposite'
        stratargs += (
            " monthly_universe_score_mode='flat_panel'"
            f" monthly_universe_indicator_panel={str(STUDY / 'flat_panel_snapshot.csv')!r}"
            f' monthly_universe_flat_weights={weights!r}'
        )
    args = ['--strat', strategy, '--ticker', saved['input_ticker'],
            '--data', str(STUDY / 'data_snapshot'),
            '--mode', 'backtest', '--timeframe', 'daily', '--provider', 'yahoo_adj',
            '--fromdate', '2000-01-01', '--todate', opts.end,
            '--commission', 'none' if opts.no_cost else 'alpaca',
            '--margin-leverage', '2', '--cash', str(opts.cash),
            '--id', opts.name, '--stratargs', stratargs]
    if not opts.no_cost:
        args += ['--margin-rate', '0.0475']
    (case / 'config.json').write_text(json.dumps(dict(options=vars(opts), cli=args), indent=2))
    namespace = btmain.parse_args(args)
    run_backtest(namespace)
    (case / 'complete.json').write_text(json.dumps({'status': 'complete'}))


if __name__ == '__main__':
    main()
