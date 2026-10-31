"""Synthetic statistical research only; no app, broker, or operational imports."""
from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import resource
import time
from pathlib import Path

for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'):
    os.environ[key] = '1'
if hasattr(os, 'sched_getaffinity'):
    available = sorted(os.sched_getaffinity(0))
    if len(available) > 2:
        os.sched_setaffinity(0, {available[0]})

import numpy as np
import pandas as pd
import scipy
from scipy.stats import norm

ROOT = Path(__file__).resolve().parent
SEED = 20261024
WINDOWS = (3, 5, 10, 15, 20)
NULLS = ('iid', 'sticky_06', 'cycle_forward', 'lag2')
SCENARIOS = ('iid', 'sticky_06', 'sticky_09', 'cycle_forward', 'cycle_reverse', 'lag2', 'biased_iid')
METHODS = ('mean_iid', 'histogram_blocks', 'markov1_blocks', 'omnibus4_blocks', 'ensemble5_blocks')
CONFIG = dict(seed=SEED, windows=list(WINDOWS), train_size=20000, calibration_blocks=1000,
              spacing=60, gap=200, trials=10000, alpha=.05, nulls=list(NULLS),
              scenarios=list(SCENARIOS), bootstrap_trials=20000,
              outer_replications=20, outer_trials=2000, monitor_runs=1000,
              monitor_horizon=100)


def simulate(kind, count, length, rng):
    """Each row starts in its stationary distribution, independently of other rows."""
    if kind in ('iid', 'biased_iid'):
        probabilities = np.ones(6) / 6 if kind == 'iid' else np.array([.1] * 5 + [.5])
        return rng.choice(6, size=(count, length), p=probabilities).astype(np.int8)
    if kind == 'lag2':
        result = np.empty((count, length), dtype=np.int8)
        for parity in (0, 1):
            result[:, parity::2] = simulate('sticky_08', count, len(range(parity, length, 2)), rng)
        return result
    probabilities = np.ones(6) / 6
    if kind.startswith('sticky_'):
        rho = {'sticky_06': .6, 'sticky_08': .8, 'sticky_09': .9}[kind]
        probabilities *= 1 - rho
        probabilities[0] += rho
    elif kind in ('cycle_forward', 'cycle_reverse'):
        probabilities *= .2
        probabilities[1 if kind == 'cycle_forward' else 5] += .8
    else:
        raise ValueError(kind)
    increments = rng.choice(6, size=(count, max(0, length - 1)), p=probabilities)
    initial = rng.integers(0, 6, size=(count, 1))
    return np.concatenate([initial, (initial + np.cumsum(increments, axis=1)) % 6], axis=1).astype(np.int8)


def empirical_tail(sorted_reference, values, *, two_sided=False):
    """Conservative ranks including ties; not an exact dependent-data theorem."""
    size = len(sorted_reference)
    upper = (1 + size - np.searchsorted(sorted_reference, values, side='left')) / (size + 1)
    if not two_sided:
        return upper
    lower = (1 + np.searchsorted(sorted_reference, values, side='right')) / (size + 1)
    return np.minimum(1., 2 * np.minimum(lower, upper))


def wilson(successes, total):
    z = norm.ppf(.975)
    proportion = successes / total
    center = (proportion + z * z / (2 * total)) / (1 + z * z / total)
    half = z * math.sqrt(proportion * (1 - proportion) / total + z * z / (4 * total * total)) / (1 + z * z / total)
    return center - half, center + half


class HistoricalReference:
    def __init__(self, training, calibration_a, calibration_b):
        counts = np.bincount(training, minlength=6) + .5
        self.pi = counts / counts.sum()
        counts = np.bincount(training[:-1].astype(int) * 6 + training[1:], minlength=36).reshape(6, 6) + .5
        self.transitions = counts / counts.sum(axis=1, keepdims=True)
        self.mean = training.mean()
        self.std = training.std(ddof=1)
        self.feature_reference = {}
        self.markov_reference = {}
        self.aggregate_reference = {}
        global_scores = []
        for size in WINDOWS:
            features, surprise = self.raw(calibration_a[:, -size:])
            self.feature_reference[size] = [np.sort(features[:, column]) for column in range(4)]
            self.markov_reference[size] = np.sort(surprise)
            raw = self.scores(calibration_b[:, -size:])
            for method in ('omnibus4_blocks', 'ensemble5_blocks'):
                self.aggregate_reference[(size, method)] = np.sort(raw[method])
            global_scores.append(raw['ensemble5_blocks'])
        self.global_reference = np.sort(np.max(global_scores, axis=0))

    def raw(self, paths):
        count, size = paths.shape
        frequencies = np.stack([(paths == face).sum(axis=1) / size for face in range(6)], axis=1)
        histogram = np.sum((frequencies - self.pi) ** 2 / self.pi, axis=1)
        repeat1 = np.mean(paths[:, 1:] == paths[:, :-1], axis=1)
        repeat2 = np.mean(paths[:, 2:] == paths[:, :-2], axis=1)
        current = np.ones(count, dtype=int)
        longest = current.copy()
        for position in range(1, size):
            current = np.where(paths[:, position] == paths[:, position - 1], current + 1, 1)
            longest = np.maximum(longest, current)
        surprise = -np.log(self.pi[paths[:, 0]]) - np.log(self.transitions[paths[:, :-1], paths[:, 1:]]).sum(axis=1)
        return np.column_stack([histogram, repeat1, repeat2, longest]), surprise

    def scores(self, paths):
        size = paths.shape[1]
        features, surprise = self.raw(paths)
        pvalues = np.column_stack([
            empirical_tail(self.feature_reference[size][column], features[:, column], two_sided=column < 3)
            for column in range(4)
        ])
        markov_p = empirical_tail(self.markov_reference[size], surprise, two_sided=True)
        minimum4 = pvalues.min(axis=1)
        minimum5 = np.minimum(minimum4, markov_p)
        return dict(mean_iid=2 * norm.sf(np.abs(paths.mean(axis=1) - self.mean) / (self.std / np.sqrt(size))),
                    histogram_blocks=pvalues[:, 0], markov1_blocks=markov_p,
                    omnibus4_blocks=-np.log(minimum4), ensemble5_blocks=-np.log(minimum5))

    def pvalues(self, paths):
        size = paths.shape[1]
        raw = self.scores(paths)
        for method in ('omnibus4_blocks', 'ensemble5_blocks'):
            raw[method] = empirical_tail(self.aggregate_reference[(size, method)], raw[method])
        return raw

    def global_pvalue(self, paths):
        score = np.max([self.scores(paths[:, -size:])['ensemble5_blocks'] for size in WINDOWS], axis=0)
        return empirical_tail(self.global_reference, score)


def make_reference(kind, rng, *, training_size=20000, blocks=1000, spacing=60, gap=200):
    length = training_size + gap + 2 * blocks * spacing + gap
    stream = simulate(kind, 1, length, rng)[0]
    training = stream[:training_size]
    first = training_size + gap
    a = np.stack([stream[start:start + 20] for start in range(first, first + blocks * spacing, spacing)])
    first += blocks * spacing + gap
    b = np.stack([stream[start:start + 20] for start in range(first, first + blocks * spacing, spacing)])
    return HistoricalReference(training, a, b), training, stream


def rejection_record(null, scenario, size, method, pvalues, **extra):
    successes = int((pvalues <= CONFIG['alpha']).sum())
    low, high = wilson(successes, len(pvalues))
    return dict(null=null, scenario=scenario, window=size, method=method,
                rejected=successes, trials=len(pvalues), rate=successes / len(pvalues),
                ci95_low=low, ci95_high=high, is_null=null == scenario, **extra)


def run_primary(rng):
    rows, checks = [], []
    references = {}
    for kind in NULLS:
        reference, training, stream = make_reference(kind, rng)
        references[kind] = (reference, training)
        np.savez_compressed(ROOT / 'inputs' / f'{kind}_historical_stream.npz', values=stream)
        marginal = np.bincount(stream, minlength=6) / len(stream)
        checks.append(dict(process=kind, observations=len(stream), min_marginal=marginal.min(),
                           max_marginal=marginal.max(), observed_repeat1=np.mean(stream[1:] == stream[:-1]),
                           observed_repeat2=np.mean(stream[2:] == stream[:-2])))
        evaluation = {}
        for scenario in SCENARIOS:
            paths = simulate(scenario, CONFIG['trials'], 20, rng)
            evaluation[scenario] = paths
            for size in WINDOWS:
                for method, pvalues in reference.pvalues(paths[:, -size:]).items():
                    rows.append(rejection_record(kind, scenario, size, method, pvalues))
            rows.append(rejection_record(kind, scenario, 0, 'global_ensemble5', reference.global_pvalue(paths)))
        np.savez_compressed(ROOT / 'inputs' / f'{kind}_evaluation_paths.npz', **evaluation)
        print(f'Completed primary null {kind}', flush=True)
    pd.DataFrame(rows).to_csv(ROOT / 'results' / 'rejection_rates.csv', index=False)
    pd.DataFrame(checks).to_csv(ROOT / 'results' / 'generator_checks.csv', index=False)
    return references


def run_monitoring(references, rng):
    rows = []
    horizon = CONFIG['monitor_horizon']
    count = CONFIG['monitor_runs']
    for kind, (reference, _) in references.items():
        stream = simulate(kind, count, horizon + 19, rng)
        all_windows = np.lib.stride_tricks.sliding_window_view(stream, 20, axis=1).reshape(-1, 20)
        global_p = reference.global_pvalue(all_windows).reshape(count, horizon)
        local_p = np.stack([reference.pvalues(all_windows[:, -size:])['ensemble5_blocks'] for size in WINDOWS], axis=1)
        methods = {
            'five_windows_unadjusted': local_p.min(axis=1).reshape(count, horizon),
            'five_windows_jointly_calibrated': global_p,
            'one_window_20': reference.pvalues(all_windows)['ensemble5_blocks'].reshape(count, horizon),
        }
        for method, pvalues in methods.items():
            detected = pvalues <= CONFIG['alpha']
            first = np.where(detected.any(axis=1), detected.argmax(axis=1) + 1, np.nan)
            low, high = wilson(int(detected.any(axis=1).sum()), count)
            rows.append(dict(null=kind, method=method, trajectories=count, horizon=horizon,
                             any_alarm_rate=detected.any(axis=1).mean(), ci95_low=low, ci95_high=high,
                             pointwise_rate=detected.mean(),
                             median_first_alarm_if_detected=float(np.nanmedian(first)) if np.isfinite(first).any() else None))
        print(f'Completed repeated monitoring {kind}', flush=True)
    pd.DataFrame(rows).to_csv(ROOT / 'results' / 'monitoring.csv', index=False)


def run_baseline_variability(rng):
    rows = []
    for replication in range(CONFIG['outer_replications']):
        reference, _, _ = make_reference('sticky_06', rng, training_size=5000, blocks=300)
        paths = simulate('sticky_06', CONFIG['outer_trials'], 20, rng)
        for method, pvalues in reference.pvalues(paths).items():
            rows.append(rejection_record('sticky_06', 'sticky_06', 20, method, pvalues, replication=replication))
        rows.append(rejection_record('sticky_06', 'sticky_06', 0, 'global_ensemble5', reference.global_pvalue(paths), replication=replication))
    pd.DataFrame(rows).to_csv(ROOT / 'results' / 'baseline_variability.csv', index=False)
    print('Completed baseline variability', flush=True)


def run_bootstrap(training, rng):
    size, count = 20, CONFIG['bootstrap_trials']
    truth = simulate('sticky_06', 100000, size, rng).mean(axis=1)
    rows = []
    for block_length in (1, 2, 5, 10, 20, 50):
        paths = np.empty((count, size), dtype=np.int8)
        indices = rng.integers(0, len(training), count)
        paths[:, 0] = training[indices]
        for position in range(1, size):
            indices = np.where(rng.random(count) < 1 / block_length,
                               rng.integers(0, len(training), count), (indices + 1) % len(training))
            paths[:, position] = training[indices]
        means = paths.mean(axis=1)
        low, high = np.quantile(means, [.025, .975])
        rows.append(dict(method='iid_bootstrap' if block_length == 1 else 'stationary_bootstrap',
                         expected_block_length=block_length, trials=count, sd_mean=means.std(ddof=1),
                         q025=low, q975=high, null_rejection_rate=np.mean((truth < low) | (truth > high))))
    for method, values in [('contiguous_history', np.lib.stride_tricks.sliding_window_view(training, size)[::60].mean(axis=1)),
                           ('oracle_fresh_paths', truth)]:
        low, high = np.quantile(values, [.025, .975])
        rows.append(dict(method=method, expected_block_length=None, trials=len(values),
                         sd_mean=values.std(ddof=1), q025=low, q975=high,
                         null_rejection_rate=np.mean((truth < low) | (truth > high))))
    pd.DataFrame(rows).to_csv(ROOT / 'results' / 'bootstrap_sensitivity.csv', index=False)


def run_examples(references):
    rows = []
    # Equal histograms, radically different directions. Four symmetric features
    # cannot distinguish these two; the Markov score has that information.
    forward = np.tile(np.arange(6), 4)[:20].astype(np.int8)
    for label, sequence in [('forward_cycle', forward), ('reverse_order', forward[::-1]),
                            ('ten_sixes', np.repeat(5, 10).astype(np.int8))]:
        paths = sequence[None, :]
        size = len(sequence)
        for kind, (reference, _) in references.items():
            for method, pvalue in reference.pvalues(paths).items():
                rows.append(dict(null=kind, example=label, window=size, method=method,
                                 pvalue=float(pvalue[0]), sequence=' '.join(str(int(x) + 1) for x in sequence)))
    pd.DataFrame(rows).to_csv(ROOT / 'results' / 'ordered_examples.csv', index=False)
    exact = {}
    for rho in (0., .6, .9):
        q = rho + (1 - rho) / 6
        exact[str(rho)] = dict(probability_ten_sixes=(1 / 6) * q ** 9,
                              probability_ten_identical=q ** 9,
                              probability_ten_new_sixes_given_previous_six=q ** 10,
                              probability_ten_new_sixes_given_previous_one=(1 - rho) / 6 * q ** 9,
                              variance_inflation_n20=1 + 2 * sum((1 - k / 20) * rho ** k for k in range(1, 20)))
    (ROOT / 'results' / 'exact_probabilities.json').write_text(json.dumps(exact, indent=2))
    # Equivariance check: consistent relabeling leaves the appropriate methods
    # unchanged, while a mean over numeric die labels has no such guarantee.
    sample = np.array([[0, 0, 0, 0, 1] * 4], dtype=np.int8)
    permutation = np.array([0, 5, 1, 2, 3, 4])
    reference, _, stream = make_reference('iid', np.random.default_rng(991))
    training_size, gap, blocks, spacing = 20000, 200, 1000, 60
    first = training_size + gap
    a = np.stack([stream[s:s+20] for s in range(first, first + blocks * spacing, spacing)])
    first += blocks * spacing + gap
    b = np.stack([stream[s:s+20] for s in range(first, first + blocks * spacing, spacing)])
    relabeled = HistoricalReference(permutation[stream[:training_size]], permutation[a], permutation[b])
    original = reference.pvalues(sample)
    changed = relabeled.pvalues(permutation[sample])
    relabel_rows = [dict(method=method, original_p=float(original[method][0]), relabeled_p=float(changed[method][0])) for method in METHODS]
    for method in METHODS[1:]:
        assert np.allclose(original[method], changed[method]), (method, original[method], changed[method])
    pd.DataFrame(relabel_rows).to_csv(ROOT / 'results' / 'relabeling.csv', index=False)


def main():
    start = time.monotonic()
    rng = np.random.default_rng(SEED)
    (ROOT / 'results').mkdir(exist_ok=True)
    (ROOT / 'inputs').mkdir(exist_ok=True)
    references = run_primary(rng)
    run_monitoring(references, rng)
    run_baseline_variability(rng)
    run_bootstrap(references['sticky_06'][1], rng)
    run_examples(references)
    manifest = dict(config=CONFIG, elapsed_seconds=time.monotonic() - start,
                    peak_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
                    versions=dict(python=platform.python_version(), numpy=np.__version__, scipy=scipy.__version__, pandas=pd.__version__),
                    affinity=sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
                    scope='Synthetic only. No strategy backtest, broker, operational data, or production change.')
    files = [ROOT / 'protocol.md', ROOT / 'protocol_addendum_before_execution.md', ROOT / 'sources.md', ROOT / 'experiment.py']
    files += sorted((ROOT / 'inputs').glob('*')) + sorted((ROOT / 'results').glob('*.csv'))
    files += [ROOT / 'results' / 'exact_probabilities.json']
    manifest['sha256'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    (ROOT / 'results' / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    print(json.dumps({key: manifest[key] for key in ('elapsed_seconds', 'peak_rss_mib', 'versions', 'affinity')}, indent=2), flush=True)


if __name__ == '__main__':
    main()
