"""Independent arithmetic/definition checks for the consequential study claims."""
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent


def validate():
    manifest = json.loads((ROOT / 'results/manifest.json').read_text())
    for filename, expected in manifest['sha256'].items():
        assert hashlib.sha256((ROOT / filename).read_bytes()).hexdigest() == expected, filename
    data = pd.read_csv(ROOT / 'results/rejection_rates.csv')
    assert len(data) == 4 * 7 * (5 * 5 + 1)
    assert not data.duplicated(['null', 'scenario', 'window', 'method']).any()
    assert (data.trials == 10000).all()
    assert np.allclose(data.rate, data.rejected / data.trials)
    assert (data.ci95_low <= data.rate).all() and (data.rate <= data.ci95_high).all()
    for row in data.itertuples():
        interval = binomtest(row.rejected, row.trials).proportion_ci(method='wilson')
        assert abs(interval.low - row.ci95_low) < 1e-10
        assert abs(interval.high - row.ci95_high) < 1e-10
    independently_verified = 0
    for null in data['null'].unique():
        stream = np.load(ROOT / f'inputs/{null}_historical_stream.npz')['values']
        training = stream[:20000]
        archive = np.load(ROOT / f'inputs/{null}_evaluation_paths.npz')
        for scenario in data.scenario.unique():
            paths = archive[scenario]
            for size in [3, 5, 10, 15, 20]:
                recent_mean = paths[:, -size:].sum(axis=1) / size
                scale = math.sqrt(np.sum((training - training.mean()) ** 2) / (len(training) - 1) / size)
                pvalues = np.array([math.erfc(abs(value - training.mean()) / scale / math.sqrt(2)) for value in recent_mean])
                row = data[(data['null'] == null) & (data.scenario == scenario) & (data.window == size) & (data.method == 'mean_iid')].iloc[0]
                assert int(np.sum(pvalues <= .05)) == row.rejected
                independently_verified += 1
    # Independent first-principles probability and covariance calculations.
    for rho in [0., .6, .9]:
        matrix = rho * np.eye(6) + (1 - rho) * np.ones((6, 6)) / 6
        assert np.allclose(matrix.sum(axis=0), 1)
        assert np.allclose(matrix.sum(axis=1), 1)
        probability = np.full(6, 1 / 6)[5]
        for _ in range(9):
            probability *= matrix[5, 5]
        expected = json.loads((ROOT / 'results/exact_probabilities.json').read_text())[str(rho)]
        assert math.isclose(probability, expected['probability_ten_sixes'])
        values = np.arange(6) - 2.5
        covariances = [np.sum(values * ((np.linalg.matrix_power(matrix, lag) @ values)) / 6) for lag in range(20)]
        inflation = (
            20 * covariances[0] + 2 * sum((20 - lag) * covariances[lag] for lag in range(1, 20))
        ) / (20 * covariances[0])
        assert math.isclose(inflation, expected['variance_inflation_n20'])
    finite = pd.read_csv(ROOT / 'results/finite_history.csv')
    assert len(finite) == 360
    assert (finite[finite.blocks == 16].rate == 0).all()
    assert np.allclose(finite.rate, finite.rejected / finite.trials)
    result = dict(finite_history_extension_checked=True, status='Ready within reviewed synthetic scope; deployment validity not established',
                  result_rows=len(data), independently_recomputed_mean_tests=independently_verified,
                  all_wilson_intervals_recomputed=True, all_manifest_hashes_match=True,
                  exact_probabilities_and_covariance_matrix_checked=True,
                  limitations=['Monte Carlo intervals condition on the frozen reference.',
                               'No real strategy inference or anytime guarantee.'])
    (ROOT / 'results/validation.json').write_text(json.dumps(result, indent=2))
    return result


if __name__ == '__main__':
    print(json.dumps(validate(), indent=2))
