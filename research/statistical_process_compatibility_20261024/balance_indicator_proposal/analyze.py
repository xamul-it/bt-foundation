"""Descriptive financial evidence for a proposed sign/threshold indicator.

Exploratory analysis, not preregistered validation and not application code.
"""
from pathlib import Path
import hashlib
import json
import math

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
INPUTS = ROOT.parent / 'strategy_adaptation/inputs'


def longest_run(values):
    best = current = 0
    last = None
    for index, flag in enumerate(values):
        current = current + 1 if flag else 0
        if current > best:
            best, last = current, index
    return best, last


def main():
    summaries, windows = [], []
    hashes = {}
    for name in ['OvernightAH', 'FlatComposite']:
        source = INPUTS / name / 'returns.csv'
        hashes[str(source.relative_to(ROOT.parent))] = hashlib.sha256(source.read_bytes()).hexdigest()
        returns = pd.read_csv(source, index_col=0, parse_dates=True)['return'].loc[:'2018-12-31']
        assert returns.index.is_unique and returns.index.is_monotonic_increasing
        assert np.isfinite(returns).all() and (returns > -1).all()
        mean = float(returns.mean())
        negative = (returns < 0).to_numpy()
        below = (returns < mean).to_numpy()
        length, end = longest_run(below)
        period = returns.iloc[end-length+1:end+1]
        count = len(returns)
        row = dict(strategy=name, reference_start=str(returns.index[0].date()),
                   reference_end=str(returns.index[-1].date()), observations=count,
                   mean_daily_return=mean, positive_count=int((returns > 0).sum()),
                   negative_count=int(negative.sum()), zero_count=int((returns == 0).sum()),
                   below_mean_count=int(below.sum()),
                   positive_below_mean_count=int(((returns > 0) & (returns < mean)).sum()),
                   at_or_above_mean_count=int((returns >= mean).sum()),
                   longest_negative=longest_run(negative)[0],
                   longest_below_mean=length, longest_below_start=str(period.index[0].date()),
                   longest_below_end=str(period.index[-1].date()),
                   longest_below_zero_days=int((period == 0).sum()),
                   longest_below_negative_days=int((period < 0).sum()))
        for field in ['positive', 'negative', 'zero', 'below_mean']:
            row[field+'_fraction'] = row[field+'_count'] / count
        assert row['positive_count'] + row['negative_count'] + row['zero_count'] == count
        assert row['negative_count'] + row['zero_count'] + row['positive_below_mean_count'] + row['at_or_above_mean_count'] == count
        summaries.append(row)
        for n in [3, 5, 10, 15, 20]:
            observed = np.lib.stride_tricks.sliding_window_view(below, n).all(axis=1)
            # Independent scalar check, including overlapping-window membership.
            scalar_count = sum(all(below[i:i+n]) for i in range(count-n+1))
            assert scalar_count == int(observed.sum())
            naive = row['below_mean_fraction'] ** n
            assert math.isclose(naive, math.exp(n * math.log(row['below_mean_fraction'])))
            windows.append(dict(strategy=name, window=n, observed_windows=len(observed),
                                windows_all_below_mean=scalar_count,
                                historical_fraction=scalar_count / len(observed),
                                naive_iid_fraction=naive))
    result = dict(scope='Historical financial description; no statistical test calibrated or UI changed',
                  unit='Arithmetic daily portfolio return from native backtest',
                  mean_definition='Arithmetic mean of all reference daily returns, fixed as of 2018-12-31',
                  overlap_note='Windows overlap and are not independent episodes; historical frequency is descriptive',
                  retrospective_note='Threshold estimated on the same historical reference; not held-out test evidence',
                  summaries=summaries, windows=windows,
                  validation={'partition_counts_reconcile': True, 'all_window_counts_checked_by_scalar_code': True},
                  source_sha256=hashes)
    (ROOT / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


if __name__ == '__main__':
    main()
