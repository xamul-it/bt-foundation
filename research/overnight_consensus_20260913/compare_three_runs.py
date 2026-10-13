"""Compare three saved strategy runs on the same fixed evaluation dates."""

import csv

from compare_overlap import daily_trades
from pathlib import Path
import json
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).with_name('three_run_comparison.csv')


def consensus_trades(slots):
    path = (ROOT / 'bt-core/out/overnight_ah_consensus/OvernightAHConsensus'
            / f'consensus_fullpool_select{slots}_2000_20260910/trades.json')
    rows = defaultdict(dict)
    for trade in json.loads(path.read_text()):
        rows[trade['open_datetime'][:10]][trade['asset']] = float(trade['pnl_pct'])
    return rows


def analyze(slots):
    dev = daily_trades('development', slots)
    chall = daily_trades('challenger', slots)
    consensus = consensus_trades(slots)
    dates = sorted(day for day in dev.keys() & chall.keys()
                   if len(dev[day]) == len(chall[day]) == slots)
    rows = []
    for name, run in [('development', dev), ('challenger', chall), ('consensus', consensus)]:
        values = [value for day in dates for value in run[day].values()]
        rows.append({
            'selection_top_n': slots,
            'fixed_days': len(dates),
            'run': name,
            'trades': len(values),
            'gross_mean_pnl_pct': sum(values) / len(values),
            'win_ratio_pct': sum(value > 0 for value in values) / len(values) * 100,
        })
    return rows


if __name__ == '__main__':
    rows = analyze(3) + analyze(10)
    with OUT.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for row in rows:
        print(row)
