"""Rebuild the 3- and 10-slot ex-post overlap tables from saved trades."""

import csv
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).with_name('overlap_summary.csv')


def daily_trades(profile, slots):
    folder = (
        'overnight_ah/OvernightAH' if profile == 'development'
        else 'overnight_ah_flat_composite/OvernightAHFlatComposite'
    )
    path = ROOT / 'bt-core/out' / folder / (
        f'{profile}_waterfall_cap10k_slots{slots}_2000_20260910/trades.json'
    )
    result = defaultdict(dict)
    for trade in json.loads(path.read_text()):
        date = trade['open_datetime'][:10]
        symbol = trade['asset']
        assert symbol not in result[date], (profile, slots, date, symbol)
        result[date][symbol] = float(trade['pnl_pct'])
    return result


def summarize(values, slots, days, group):
    return {
        'max_slots': slots,
        'comparable_days': days,
        'group': group,
        'trades': len(values),
        'gross_mean_pnl_pct': sum(values) / len(values),
        'win_ratio_pct': sum(value > 0 for value in values) / len(values) * 100,
    }


def analyze(slots):
    dev = daily_trades('development', slots)
    chall = daily_trades('challenger', slots)
    dates = [day for day in dev.keys() & chall.keys()
             if len(dev[day]) == len(chall[day]) == slots]
    shared_values, dev_only, chall_only = [], [], []
    development_all, challenger_all = [], []
    for day in dates:
        development_all.extend(dev[day].values())
        challenger_all.extend(chall[day].values())
        common = dev[day].keys() & chall[day].keys()
        for symbol in common:
            assert dev[day][symbol] == chall[day][symbol], (slots, day, symbol)
            shared_values.append(dev[day][symbol])  # Count once.
        dev_only.extend(dev[day][symbol] for symbol in dev[day].keys() - common)
        chall_only.extend(chall[day][symbol] for symbol in chall[day].keys() - common)
    return [
        summarize(development_all, slots, len(dates), 'development_all'),
        summarize(challenger_all, slots, len(dates), 'challenger_all'),
        summarize(shared_values, slots, len(dates), 'intersection'),
        summarize(dev_only, slots, len(dates), 'development_only'),
        summarize(chall_only, slots, len(dates), 'challenger_only'),
        summarize(dev_only + chall_only, slots, len(dates), 'exclusive_union'),
    ]


if __name__ == '__main__':
    rows = analyze(3) + analyze(10)
    with OUT.open('w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    for row in rows:
        print(row)
