"""Research-only statistics for continuous native strategy returns."""
from pathlib import Path
import importlib.util

import numpy as np

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('categorical_r03', ROOT.parent / 'revision_03/experiment.py')
r03 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r03)
WINDOWS = (3, 5, 10, 15, 20)


def tail(reference, values):
    reference = np.sort(reference)
    return (1 + len(reference) - np.searchsorted(reference, values, side='left')) / (len(reference) + 1)


def windows(values, length):
    return np.lib.stride_tricks.sliding_window_view(np.asarray(values), length, axis=-1)


class QuantileFeatures:
    def __init__(self, fit):
        self.edges = np.quantile(fit, np.arange(1, 6) / 6)
        self.model = r03.prior.MemoryReference.__new__(r03.prior.MemoryReference)
        self.model.probabilities = []
        encoded = self.encode(fit)
        for order in (1, 2, 3):
            counts = np.bincount(r03.base.word_codes(encoded[None, :], order).ravel(), minlength=6**order) + .5
            self.model.probabilities.append(counts / counts.sum())

    def encode(self, paths):
        return np.searchsorted(self.edges, paths, side='right').astype(np.int8)

    def features(self, paths):
        return self.model.features(self.encode(paths))


class ContinuousFeatures:
    def __init__(self, fit):
        self.mean = np.mean(fit)
        self.std = np.std(fit, ddof=1)
        assert self.std > 0
        self.abs_center = np.mean(np.abs((fit - self.mean) / self.std))

    def features(self, paths):
        x = np.asarray(paths) / self.std
        u = (np.asarray(paths) - self.mean) / self.std
        amplitude = np.abs(u) - self.abs_center
        loss = paths < 0
        run = np.zeros(len(paths), dtype=int)
        longest = run.copy()
        for col in range(paths.shape[1]):
            run = np.where(loss[:, col], run + 1, 0)
            longest = np.maximum(longest, run)
        cumulative = np.column_stack([np.zeros(len(paths)), np.cumsum(x, axis=1)])
        drawdown = np.max(np.maximum.accumulate(cumulative, axis=1) - cumulative, axis=1)
        components = [x.sum(axis=1), np.mean(x*x, axis=1), x.min(axis=1), x.max(axis=1),
                      drawdown, loss.mean(axis=1), longest]
        for lag in range(1, paths.shape[1]):
            components.append(np.mean(u[:, lag:] * u[:, :-lag], axis=1))
            components.append(np.mean(amplitude[:, lag:] * amplitude[:, :-lag], axis=1))
        return np.column_stack(components)


class Reference:
    def __init__(self, fit, normalize, calibration, kind):
        self.kind = kind
        self.component = QuantileFeatures(fit) if kind == 'C5_Q6' else ContinuousFeatures(fit)
        self.center, self.scale = {}, {}
        for n in WINDOWS:
            raw = self.component.features(windows(normalize, n))
            self.center[n] = raw.mean(axis=0)
            self.scale[n] = np.maximum(raw.std(axis=0, ddof=1), .25 if kind == 'C5_Q6' else .05)
        self.calibration_paths = windows(calibration, 20)[::20]
        self.calibration_scores = self.scores(self.calibration_paths)

    def scores(self, paths):
        result = np.empty((len(paths), len(WINDOWS)))
        for j, n in enumerate(WINDOWS):
            raw = self.component.features(paths[:, -n:])
            result[:, j] = np.max(np.abs((raw - self.center[n]) / self.scale[n]), axis=1)
        return result

    def pvalues(self, scores, reference_scores=None):
        reference = self.calibration_scores if reference_scores is None else reference_scores
        result = {str(n): tail(reference[:, j], scores[:, j]) for j, n in enumerate(WINDOWS)}
        result['joint'] = tail(reference.max(axis=1), scores.max(axis=1))
        return result

    def period_bank(self, calibration, horizon):
        panels = windows(calibration, horizon + 19)[::horizon + 40]
        flattened = windows(panels, 20).reshape(-1, 20)
        return self.scores(flattened).max(axis=1).reshape(len(panels), horizon).max(axis=1)
