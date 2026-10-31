"""Bounded strategy-return adaptation study. No application imports or writes."""
import hashlib
import json
import platform
import resource
import time
from pathlib import Path

import numpy as np
import pandas as pd
import scipy
from scipy.stats import norm, t as student_t

from models import Reference, WINDOWS, tail, windows

ROOT = Path(__file__).resolve().parent
ALPHA = .05


def load(name):
    source = ROOT / 'inputs' / name
    baseline = json.loads((source / 'baseline.json').read_text())
    raw = pd.read_csv(source / 'returns.csv', index_col=0, parse_dates=True).iloc[:, 0]
    assert raw.index.is_unique and raw.index.is_monotonic_increasing
    assert np.isfinite(raw).all() and (raw > -1).all()
    cutoff = pd.Timestamp(baseline['window_end'])
    returns = raw.loc[raw.index <= cutoff]
    trades = pd.read_csv(source / 'daily_trades.csv', index_col=0, parse_dates=True).reindex(returns.index)
    for field in ['trades', 'net_pnl', 'gross_pnl', 'entry_notional', 'sum_trade_pct', 'sum_squared_trade_pct']:
        trades[field] = trades[field].fillna(0)
    # A missing trade ratio is not imputed to zero; NAV zero-return days stay observed.
    metadata = next(iter(json.loads((source / 'results.json').read_text()).values()))
    native_gain = metadata['PNL_money'] / metadata['ptf inizio']
    compounded = np.expm1(np.log1p(raw).sum())
    assert np.isclose(compounded, native_gain, atol=1e-7, rtol=1e-8)
    summary = {
        'strategy': name, 'baseline_id': baseline['id'], 'start': str(returns.index[0].date()),
        'end': str(returns.index[-1].date()), 'rows': len(returns),
        'source_rows': len(raw), 'excluded_dates': [str(d.date()) for d in raw.index[raw.index > cutoff]],
        'missing_values': int(raw.isna().sum()), 'duplicate_dates': int(raw.index.duplicated().sum()),
        'zero_return_days': int((returns == 0).sum()),
        'trade_days': int((trades.trades > 0).sum()), 'total_trades_in_window': int(trades.trades.sum()),
        'mean_trades_per_active_day': float(trades.loc[trades.trades > 0, 'trades'].mean()),
        'mean_daily_pct': float(100 * returns.mean()), 'std_daily_pct': float(100 * returns.std()),
        'min_daily_pct': float(100 * returns.min()), 'max_daily_pct': float(100 * returns.max()),
        'native_compound_gain': float(native_gain), 'all_bars_compound_gain': float(compounded),
        'filtered_compound_gain': float(np.expm1(np.log1p(returns).sum())),
        'net_trade_NAV_correlation_on_active_days': float(trades.net_return.corr(returns)),
        'recorded_trade_commissions': float((trades.gross_pnl - trades.net_pnl).sum()),
        'commission_configuration': metadata.get('input_commission'),
        'slippage_configuration': metadata.get('input_slippage'),
    }
    frame = pd.DataFrame({'return': returns, 'log_return': np.log1p(returns)}).join(trades)
    return frame, summary


def train_parts(frame):
    logs = frame.log_return
    return (logs.loc[:'2004-12-31'].to_numpy(),
            logs.loc['2005-01-01':'2009-12-31'].to_numpy(),
            logs.loc['2010-01-01':'2018-12-31'].to_numpy())


def dependence(frame, name):
    rows = []
    for period, part in [('2000-2018', frame.loc[:'2018-12-31']), ('2019-2025', frame.loc['2019-01-01':])]:
        values = part.log_return
        for lag in range(1, 21):
            rows.append(dict(strategy=name, period=period, lag=lag,
                             returns_acf=values.autocorr(lag), amplitude_acf=values.abs().autocorr(lag),
                             squared_acf=values.pow(2).autocorr(lag)))
    return rows


def normal_controls(frame):
    before = frame.loc[:'2018-12-31']
    mean, std = before.log_return.mean(), before.log_return.std()
    n = before.trades.sum()
    trade_mean = before.sum_trade_pct.sum() / n
    trade_std = np.sqrt((before.sum_squared_trade_pct.sum() - n * trade_mean**2) / (n - 1))
    result = []
    for size in WINDOWS:
        day_mean = frame.log_return.rolling(size).mean()
        daily_p = 2 * norm.sf(abs(day_mean - mean) / (std / np.sqrt(size)))
        trade_count = frame.trades.rolling(size).sum()
        recent_mean = frame.sum_trade_pct.rolling(size).sum() / trade_count.replace(0, np.nan)
        trade_p = 2 * norm.sf(abs(recent_mean - trade_mean) / (trade_std / np.sqrt(trade_count)))
        for label, values in [('daily_mean_iid', daily_p), ('UI_trade_mean_iid', trade_p)]:
            result.append(pd.DataFrame({'date': frame.index, 'window': str(size), 'method': label, 'pvalue': values}))
    return pd.concat(result, ignore_index=True)


def historical(name, frame, models, calibration):
    paths = windows(frame.log_return.to_numpy(), 20)
    dates = frame.index[19:]
    test_mask = dates >= pd.Timestamp('2019-01-01')
    rows, periods, capacities = [], [], []
    for kind, model in models.items():
        scores = model.scores(paths[test_mask])
        for win, pvalue in model.pvalues(scores).items():
            rows.append(pd.DataFrame({'strategy': name, 'date': dates[test_mask], 'window': win,
                                      'method': kind + '_fixed', 'pvalue': pvalue}))
        if kind != 'F1_continuous':
            continue
        for horizon in [20, 100]:
            bank = model.period_bank(calibration, horizon)
            minimum = 1 / (len(bank) + 1)
            capacities.append(dict(strategy=name, horizon=horizon, reference_periods=len(bank),
                                   minimum_p=minimum, estimable=minimum <= ALPHA))
            for start in range(0, len(scores) - horizon + 1, horizon):
                maximum = scores[start:start+horizon].max(axis=1)
                point = tail(model.calibration_scores.max(axis=1), maximum)
                pvalue = float(tail(bank, maximum.max()))
                periods.append(dict(strategy=name, horizon=horizon,
                                    start=str(dates[test_mask][start].date()),
                                    end=str(dates[test_mask][start+horizon-1].date()),
                                    any_daily_alarm=bool((point <= ALPHA).any()),
                                    period_pvalue=pvalue, period_alarm=pvalue <= ALPHA,
                                    estimable=minimum <= ALPHA))
    for year in range(2019, 2026):
        past = frame.loc[f'{year-5}-01-01':f'{year-1}-12-31', 'log_return'].to_numpy()
        rolling_model = Reference(past[:252], past[252:504], past[524:], 'F1_continuous')
        mask = dates.year == year
        for win, pvalue in rolling_model.pvalues(rolling_model.scores(paths[mask])).items():
            rows.append(pd.DataFrame({'strategy': name, 'date': dates[mask], 'window': win,
                                      'method': 'F1_continuous_rolling5y', 'pvalue': pvalue}))
    controls = normal_controls(frame)
    controls = controls.loc[controls.date >= '2019-01-01'].copy()
    controls['strategy'] = name
    rows.append(controls)
    return pd.concat(rows, ignore_index=True), periods, capacities


def scenarios(pool, fit, rng):
    mean, std = fit.mean(), fit.std(ddof=1)
    result = {'unchanged': pool.copy(), 'permuted': rng.permuted(pool, axis=1),
              'reversed': pool[:, ::-1].copy(), 'sorted_same_values': np.sort(pool, axis=1)}
    for shift in [.25, .5, 1.]:
        result[f'mean_minus_{shift:g}sd'] = pool - shift * std
    for multiplier in [1.5, 2.]:
        result[f'dispersion_x{multiplier:g}'] = mean + multiplier * (pool - mean)
    for severity in [3, 6]:
        changed = pool.copy()
        changed[:, -1] = np.minimum(changed[:, -1], -severity * std)
        result[f'last_loss_{severity}sd'] = changed
    return result


def controlled(name, fit, calibration, models, seed):
    rng = np.random.default_rng(seed)
    pool = windows(calibration, 20)
    paths = scenarios(pool, fit, rng)
    score_cache = {kind: {case: model.scores(values) for case, values in paths.items()}
                   for kind, model in models.items()}
    diagnostics = []
    left = pool.copy()
    right = pool.copy()
    low_edge = models['C5_Q6'].component.edges[0]
    left[:, -1] = low_edge - 3 * fit.std(ddof=1)
    right[:, -1] = low_edge - 6 * fit.std(ddof=1)
    for kind, model in models.items():
        base = score_cache[kind]['unchanged'].max(axis=1)
        sorted_scores = score_cache[kind]['sorted_same_values'].max(axis=1)
        lower, upper = model.scores(left).max(axis=1), model.scores(right).max(axis=1)
        diagnostics.append(dict(strategy=name, method=kind,
                                order_changes_score_fraction=float(np.mean(~np.isclose(base, sorted_scores))),
                                same_bin_loss_changes_score_fraction=float(np.mean(~np.isclose(lower, upper))),
                                same_bin_loss_median_score_ratio=float(np.median(upper / np.maximum(lower, 1e-12)))))
    rows = []
    samples = {}
    for replication in range(50):
        bank_indices = rng.integers(0, len(pool), 200)
        eval_indices = rng.integers(0, len(pool), 2000)
        samples[f'bank_{replication:02d}'] = bank_indices
        samples[f'eval_{replication:02d}'] = eval_indices
        for kind, model in models.items():
            reference = score_cache[kind]['unchanged'][bank_indices]
            for case, scores in score_cache[kind].items():
                for win, values in model.pvalues(scores[eval_indices], reference).items():
                    rejected = int(np.sum(values <= ALPHA))
                    rows.append(dict(strategy=name, method=kind, scenario=case, window=win,
                                     replication=replication, trials=len(values), rejected=rejected,
                                     rate=rejected / len(values)))
    np.savez_compressed(ROOT / 'results' / f'{name}_resampling_indices.npz', **samples)
    # Retain score tables and alterations for independent audit and rerunning.
    np.savez_compressed(ROOT / 'results' / f'{name}_controlled_paths.npz', **paths)
    for kind, cache in score_cache.items():
        np.savez_compressed(ROOT / 'results' / f'{name}_{kind}_scores.npz', **cache)
    return pd.DataFrame(rows), diagnostics


def summarize_controlled(rows):
    result = []
    for keys, group in rows.groupby(['strategy', 'method', 'scenario', 'window']):
        values = group.rate.to_numpy()
        interval = student_t.ppf(.975, len(values)-1) * values.std(ddof=1) / np.sqrt(len(values))
        result.append(dict(zip(['strategy', 'method', 'scenario', 'window'], keys)) |
                      {'rate': values.mean(), 'ci95_low': max(0, values.mean()-interval),
                       'ci95_high': min(1, values.mean()+interval), 'banks': len(values),
                       'trials_per_bank': int(group.trials.iloc[0]),
                       'rejected_total': int(group.rejected.sum())})
    return pd.DataFrame(result)


def main():
    start = time.monotonic()
    history, bootstrap, quality, acf, periods, capacity, diagnostics = [], [], [], [], [], [], []
    for name, seed in [('OvernightAH', 20261101), ('FlatComposite', 20261102)]:
        frame, q = load(name)
        frame.to_csv(ROOT / 'results' / f'{name}_daily.csv', float_format='%.17g')
        fit, normalize, calibration = train_parts(frame)
        q['fit_rows'], q['normalization_rows'], q['calibration_rows'] = len(fit), len(normalize), len(calibration)
        q['evaluation_rows'] = int((frame.index >= '2019-01-01').sum())
        q['initial_std_log_pct'] = float(fit.std(ddof=1) * 100)
        q['normalization_std_log_pct'] = float(normalize.std(ddof=1) * 100)
        q['calibration_std_log_pct'] = float(calibration.std(ddof=1) * 100)
        quality.append(q)
        acf.extend(dependence(frame, name))
        models = {kind: Reference(fit, normalize, calibration, kind) for kind in ['C5_Q6', 'F1_continuous']}
        points, p, c = historical(name, frame, models, calibration)
        history.append(points)
        periods.extend(p)
        capacity.extend(c)
        control, d = controlled(name, fit, calibration, models, seed)
        bootstrap.append(control)
        diagnostics.extend(d)
        print(name, 'finished', round(time.monotonic()-start, 2), 'seconds', flush=True)
    history, bootstrap = pd.concat(history, ignore_index=True), pd.concat(bootstrap, ignore_index=True)
    history['year'] = pd.to_datetime(history.date).dt.year
    history['alarm'] = (history.pvalue <= ALPHA).where(history.pvalue.notna())
    history.to_csv(ROOT / 'results/historical_daily.csv', index=False)
    annual = history.groupby(['strategy', 'method', 'window', 'year']).agg(
        observations=('pvalue', 'count'), alarms=('alarm', 'sum'), rate=('alarm', 'mean')).reset_index()
    annual.to_csv(ROOT / 'results/historical_annual.csv', index=False)
    bootstrap.to_csv(ROOT / 'results/controlled_raw.csv', index=False)
    summary = summarize_controlled(bootstrap)
    summary.to_csv(ROOT / 'results/controlled_summary.csv', index=False)
    pd.DataFrame(periods).to_csv(ROOT / 'results/monitoring_periods.csv', index=False)
    pd.DataFrame(capacity).to_csv(ROOT / 'results/monitoring_capacity.csv', index=False)
    pd.DataFrame(diagnostics).to_csv(ROOT / 'results/diagnostics.csv', index=False)
    pd.DataFrame(acf).to_csv(ROOT / 'results/dependence.csv', index=False)
    (ROOT / 'results/data_quality.json').write_text(json.dumps(quality, indent=2) + '\n')
    gates = []
    for name in ['OvernightAH', 'FlatComposite']:
        for scenario, low, high in [('unchanged', .025, .075), ('mean_minus_1sd', .80, 1.), ('dispersion_x2', .50, 1.)]:
            row = summary.loc[(summary.strategy == name) & (summary.method == 'F1_continuous') &
                              (summary.window == 'joint') & (summary.scenario == scenario)].iloc[0]
            gates.append(dict(strategy=name, criterion=scenario, rate=row.rate,
                              low=low, high=high, passed=bool(low <= row.rate <= high)))
        d = next(row for row in diagnostics if row['strategy'] == name and row['method'] == 'F1_continuous')
        gates.append(dict(strategy=name, criterion='order_and_magnitude_preserved',
                          passed=bool(d['order_changes_score_fraction'] > 0 and d['same_bin_loss_changes_score_fraction'] > 0)))
    assessment = {'scope': 'Feasibility on native backtest returns and controlled empirical blocks',
                  'all_diagnostic_gates_passed': all(g['passed'] for g in gates), 'gates': gates,
                  'future_real_null_false_alarm_rate_verified': False,
                  'production_application': False}
    (ROOT / 'results/assessment.json').write_text(json.dumps(assessment, indent=2) + '\n')
    owned = ['source_manifest.json', 'historical_daily.csv', 'historical_annual.csv',
             'controlled_raw.csv', 'controlled_summary.csv', 'monitoring_periods.csv',
             'monitoring_capacity.csv', 'diagnostics.csv', 'dependence.csv',
             'data_quality.json', 'assessment.json', 'OvernightAH_daily.csv', 'FlatComposite_daily.csv']
    files = sorted((ROOT / 'inputs').rglob('*')) + [ROOT / 'results' / f for f in owned]
    files += sorted((ROOT / 'results').glob('*.npz'))
    files += [ROOT / f for f in ['protocol.md', 'models.py', 'analyze.py', 'freeze_inputs.py']]
    manifest = {'elapsed_seconds': time.monotonic()-start,
                'peak_rss_mib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
                'python': platform.python_version(), 'numpy': np.__version__, 'pandas': pd.__version__,
                'scipy': scipy.__version__, 'seeds': [20261101, 20261102],
                'sha256': {str(f.relative_to(ROOT)): hashlib.sha256(f.read_bytes()).hexdigest()
                           for f in files if f.is_file() and f.name != 'analysis_manifest.json'}}
    (ROOT / 'results/analysis_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(assessment, indent=2), flush=True)


if __name__ == '__main__':
    main()
