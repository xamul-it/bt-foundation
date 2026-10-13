"""
ATR con SMA (non SmoothedMA).

CRITICO: usare SMA (rolling mean), non EMA/SmoothedMA.
Corrisponde a bt-core/indicators/atr_sma.py.

Formula:
    TR = max(high-low, |high-prev_close|, |low-prev_close|)
    ATR = SMA(TR, period)
"""
import pandas as pd


def atr_sma(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """ATR usando Simple Moving Average. Matcha ATR_SMA in bt-core."""
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def atr_sma_pct(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """ATR come % del close. Corrisponde a ATR_SMA_Pct in bt-core."""
    return (atr_sma(high, low, close, period) / close) * 100.0
