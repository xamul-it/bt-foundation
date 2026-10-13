"""
HMA (Hull Moving Average) - numpy/pandas implementation.

Matches bt-core esattamente (validato da final_exact_match_test.py).
Formula: HMA(n) = WMA(2*WMA(n/2) - WMA(n), sqrt(n))
"""
import numpy as np
import pandas as pd


def wma(series: pd.Series, period: int) -> pd.Series:
    """Weighted Moving Average. Matches Backtrader bt.indicators.WeightedMovingAverage."""
    weights = np.arange(1, period + 1, dtype=float)
    w_sum = weights.sum()

    def _wma(x):
        return np.dot(x, weights) / w_sum

    return series.rolling(period).apply(_wma, raw=True)


def hma(series: pd.Series, period: int) -> pd.Series:
    """
    Hull Moving Average.

    Identical to bt-core's bt.indicators.HMA(period).
    Validated against Backtrader via final_exact_match_test.py.
    """
    half = int(period / 2)
    sq = int(np.sqrt(period))
    raw = 2.0 * wma(series, half) - wma(series, period)
    return wma(raw, sq)
