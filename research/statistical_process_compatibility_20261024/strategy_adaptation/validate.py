"""Independent scalar feature, source, aggregation and rank checks."""
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from analyze import load, train_parts
from models import Reference, WINDOWS, windows

ROOT = Path(__file__).resolve().parent


def manual_features(path, model):
    if model.kind == 'C5_Q6':
        edges = model.component.edges.tolist()
        values = [sum(v >= edge for edge in edges) for v in path]
        result = []
        for order, probabilities in enumerate(model.component.model.probabilities, start=1):
            words = Counter(tuple(values[i:i+order]) for i in range(len(values)-order+1))
            n = sum(words.values())
            score = 0.
            for word, count in words.items():
                code = 0
                for letter in word:
                    code = code * 6 + letter
                score += 2 * count * math.log(count / (n * probabilities[code]))
            result.append(round(max(0., score), 10))
        longest = current = 1
        for a, b in zip(values[:-1], values[1:]):
            current = current + 1 if a == b else 1
            longest = max(longest, current)
        result.append(longest)
        for lag in range(1, min(10, len(values)-1)+1):
            result.append(sum(values[i] == values[i-lag] for i in range(lag, len(values))))
        return np.array(result)
    component = model.component
    x = [v / component.std for v in path]
    u = [(v - component.mean) / component.std for v in path]
    a = [abs(v) - component.abs_center for v in u]
    peak = cumulative = drawdown = 0.
    longest = current = 0
    for v in x:
        cumulative += v
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak-cumulative)
        current = current + 1 if v < 0 else 0
        longest = max(longest, current)
    result = [math.fsum(x), math.fsum(v*v for v in x)/len(x), min(x), max(x),
              drawdown, sum(v < 0 for v in path)/len(path), longest]
    for lag in range(1, len(path)):
        result.append(math.fsum(u[i] * u[i-lag] for i in range(lag, len(path))) / (len(path)-lag))
        result.append(math.fsum(a[i] * a[i-lag] for i in range(lag, len(path))) / (len(path)-lag))
    return np.array(result)


def main():
    for filename in ['source_manifest.json', 'analysis_manifest.json']:
        manifest = json.loads((ROOT / 'results' / filename).read_text())
        for rel, digest in manifest.get('sha256', manifest.get('frozen_sha256', {})).items():
            assert hashlib.sha256((ROOT / rel).read_bytes()).hexdigest() == digest, rel
    previous = ROOT.parent / 'revision_03'
    previous_manifest = json.loads((previous / 'delivery_manifest.json').read_text())
    for rel, digest in previous_manifest['files_sha256'].items():
        assert hashlib.sha256((previous / rel).read_bytes()).hexdigest() == digest, rel
    raw = pd.read_csv(ROOT / 'results/controlled_raw.csv', dtype={'window': str})
    assert not raw.duplicated(['strategy', 'method', 'scenario', 'window', 'replication']).any()
    assert np.allclose(raw.rate, raw.rejected / raw.trials)
    summary = pd.read_csv(ROOT / 'results/controlled_summary.csv', dtype={'window': str})
    for row in summary.itertuples():
        selected = raw[(raw.strategy == row.strategy) & (raw.method == row.method) &
                       (raw.scenario == row.scenario) & (raw.window == row.window)]
        assert len(selected) == 50
        assert np.isclose(selected.rate.mean(), row.rate)
        assert selected.rejected.sum() == row.rejected_total
    rank_cells = 0
    trade_rows = 0
    for name in ['OvernightAH', 'FlatComposite']:
        frame, quality = load(name)
        fit, normal, calibration = train_parts(frame)
        trade = pd.read_csv(ROOT / 'inputs' / name / 'trade_inputs.csv.gz')
        grouped = defaultdict(list)
        for row in trade.itertuples():
            grouped[row.close_datetime[:10]].append(row)
        aggregates = pd.read_csv(ROOT / 'inputs' / name / 'daily_trades.csv', index_col=0)
        for day, records in grouped.items():
            actual = aggregates.loc[day]
            assert actual.trades == len(records)
            for result, field in [('net_pnl', 'pnlcomm'), ('gross_pnl', 'pnl'),
                                  ('entry_notional', 'value'), ('sum_trade_pct', 'pnl_pct')]:
                assert math.isclose(actual[result], math.fsum(getattr(r, field) for r in records), rel_tol=1e-10, abs_tol=1e-7)
        trade_rows += len(trade)
        paths = np.load(ROOT / 'results' / f'{name}_controlled_paths.npz')
        base = paths['unchanged']
        assert np.allclose(np.sort(base, axis=1), np.sort(paths['permuted'], axis=1))
        assert np.array_equal(np.sort(base, axis=1), paths['sorted_same_values'])
        assert np.array_equal(base[:, ::-1], paths['reversed'])
        indices = np.load(ROOT / 'results' / f'{name}_resampling_indices.npz')
        for kind in ['C5_Q6', 'F1_continuous']:
            model = Reference(fit, normal, calibration, kind)
            cache = np.load(ROOT / 'results' / f'{name}_{kind}_scores.npz')
            for size in WINDOWS:
                normal_paths = windows(normal, size)
                # Entire held-out normalizer independently reconstructed from scalar features.
                independent = np.stack([manual_features(path.tolist(), model) for path in normal_paths])
                assert np.allclose(independent.mean(axis=0), model.center[size], atol=1e-8, rtol=1e-8)
                floor = .25 if kind == 'C5_Q6' else .05
                assert np.allclose(np.maximum(independent.std(axis=0, ddof=1), floor), model.scale[size], atol=1e-8, rtol=1e-8)
                for position in [0, 25, 250, 1000, len(base)-1]:
                    expected = manual_features(base[position, -size:].tolist(), model)
                    actual = model.component.features(base[position:position+1, -size:])[0]
                    assert np.allclose(expected, actual, atol=1e-8, rtol=1e-8)
            for replication in [0, 17, 49]:
                reference = cache['unchanged'][indices[f'bank_{replication:02d}']]
                selection = indices[f'eval_{replication:02d}']
                for scenario in ['unchanged', 'sorted_same_values', 'mean_minus_1sd', 'dispersion_x2']:
                    values = cache[scenario][selection]
                    for col, win in enumerate([*map(str, WINDOWS), 'joint']):
                        a = reference.max(axis=1) if win == 'joint' else reference[:, col]
                        b = values.max(axis=1) if win == 'joint' else values[:, col]
                        pvalues = (1 + np.sum(a[None, :] >= b[:, None], axis=1)) / (len(a)+1)
                        expected = int((pvalues <= .05).sum())
                        row = raw[(raw.strategy == name) & (raw.method == kind) & (raw.replication == replication) &
                                  (raw.scenario == scenario) & (raw.window == win)].iloc[0]
                        assert expected == row.rejected
                        rank_cells += 1
            # Independent scalar score and rank check against dated real observations.
            history = pd.read_csv(ROOT / 'results/historical_daily.csv', dtype={'window': str})
            dated_paths = windows(frame.log_return.to_numpy(), 20)
            dates = frame.index[19:]
            for position in [i for i, date in enumerate(dates) if date.year in (2019, 2020, 2025)][::150]:
                scores = []
                for size in WINDOWS:
                    features = manual_features(dated_paths[position, -size:].tolist(), model)
                    scores.append(max(abs((features-model.center[size])/model.scale[size])))
                for col, win in enumerate([*map(str, WINDOWS), 'joint']):
                    reference = model.calibration_scores.max(axis=1) if win == 'joint' else model.calibration_scores[:, col]
                    value = max(scores) if win == 'joint' else scores[col]
                    expected = (1 + sum(v >= value for v in reference)) / (len(reference)+1)
                    row = history[(history.strategy == name) & (history.method == kind+'_fixed') &
                                  (history.window == win) & (history.date == str(dates[position].date()))].iloc[0]
                    assert np.isclose(row.pvalue, expected)
    result = {'passed': True, 'source_and_analysis_hashes_match': True,
              'previous_revision_03_unchanged': True, 'raw_controlled_rows_checked': len(raw),
              'trade_rows_independently_aggregated': trade_rows,
              'scalar_feature_and_full_normalization_checks': 'both models, both strategies, all five window lengths',
              'independent_rank_count_cells': rank_cells,
              'detailed_rank_sampling': 'replicas 0,17,49; unchanged, sorted, mean -1sd, dispersion x2; all windows',
              'dated_scalar_score_check': 'sampled dates from 2019,2020,2025 for both models and strategies',
              'limits': 'Arithmetic and implementation audit; not proof of a stationary financial null or future performance'}
    (ROOT / 'results/validation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
