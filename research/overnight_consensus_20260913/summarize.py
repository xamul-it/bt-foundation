"""Summarize the Backtrader sweep; pnl_pct is gross percent per trade."""

import csv
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / 'bt-core/out/overnight_ah_consensus/OvernightAHConsensus'
BASELINES = {
    'development': ROOT / 'bt-core/out/overnight_ah/OvernightAH',
    'challenger': ROOT / 'bt-core/out/overnight_ah_flat_composite/OvernightAHFlatComposite',
}
OUT = Path(__file__).with_name('summary.csv')
VALUES = (*range(1, 21), 30, 50, 102)


def run_metrics(run):
    trades = json.loads((run / 'trades.json').read_text())
    by_year = defaultdict(list)
    for trade in trades:
        by_year[int(trade['open_datetime'][:4])].append(float(trade['pnl_pct']))
    with (run / 'returns.csv').open(newline='') as file:
        returns = list(csv.DictReader(file))
    first = date.fromisoformat(returns[0]['index'])
    last = date.fromisoformat(returns[-1]['index'])
    years = (last - first).days / 365.25
    growth = 1.0
    for row in returns:
        growth *= 1.0 + float(row['return'])
    mean = sum(float(t['pnl_pct']) for t in trades) / len(trades)
    win_ratio = sum(float(t['pnl_pct']) > 0 for t in trades) / len(trades) * 100
    return {
        'trades': len(trades),
        'gross_mean_pnl_pct': mean,
        'win_ratio_pct': win_ratio,
        'gross_mean_pnl_pct_2013_2019': _period_mean(by_year, 2013, 2019),
        'gross_mean_pnl_pct_2020_2022': _period_mean(by_year, 2020, 2022),
        'gross_mean_pnl_pct_2023_2026': _period_mean(by_year, 2023, 2026),
        'cagr_pct': (growth ** (1 / years) - 1) * 100,
        'last_date': last.isoformat(),
    }


def summarize(n):
    run = RUNS / f'consensus_fullpool_select{n}_2000_20260910'
    consensus_params = next(iter(json.loads((run / 'results.json').read_text()).values()))
    assert consensus_params['param_selection_top_n'] == n
    assert consensus_params['param_monthly_universe_top_n'] == 10000
    assert consensus_params['param_max_notional_per_trade'] == 10000
    result = {'selection_top_n': n, **run_metrics(run)}
    for profile, folder in BASELINES.items():
        baseline_run = folder / f'{profile}_waterfall_cap10k_slots{n}_2000_20260910'
        baseline_params = next(iter(json.loads((baseline_run / 'results.json').read_text()).values()))
        assert baseline_params['param_max_concurrent'] == n, (profile, n)
        assert baseline_params['param_min_concurrent'] == min(3, n), (profile, n)
        assert baseline_params['param_monthly_universe_top_n'] == (50 if profile == 'development' else 60)
        assert baseline_params['param_max_notional_per_trade'] == 10000
        metrics = run_metrics(baseline_run)
        assert metrics['last_date'] == result['last_date'], (profile, n)
        for key in ('trades', 'gross_mean_pnl_pct', 'win_ratio_pct', 'cagr_pct'):
            result[f'{profile}_{key}'] = metrics[key]
    return result


def _period_mean(by_year, start, end):
    values = [value for year in range(start, end + 1) for value in by_year[year]]
    return sum(values) / len(values) if values else float('nan')


if __name__ == '__main__':
    rows = [summarize(n) for n in VALUES]
    with OUT.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for row in rows:
        print(row)
