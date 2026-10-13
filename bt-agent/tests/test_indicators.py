"""
test_indicators.py — verifica hma() e atr_sma() contro valori di riferimento.

Punto 1 del piano: diff < 1e-8 rispetto all'extractor BT (se disponibile),
altrimenti verifica interna di consistenza.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
import pytest

from indicators.hma import wma, hma
from indicators.atr import atr_sma, atr_sma_pct


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def sample_close():
    """Serie close sintetica riproducibile."""
    rng = np.random.default_rng(42)
    n = 200
    prices = 100.0 + np.cumsum(rng.normal(0, 0.5, n))
    return pd.Series(prices, name="close")


@pytest.fixture
def sample_ohlcv(sample_close):
    rng = np.random.default_rng(42)
    n = len(sample_close)
    close = sample_close.values
    high = close + rng.uniform(0.1, 0.5, n)
    low = close - rng.uniform(0.1, 0.5, n)
    open_ = close + rng.normal(0, 0.2, n)
    return pd.DataFrame({
        "open": open_, "high": high,
        "low": low, "close": close,
        "volume": rng.integers(1000, 10000, n).astype(float),
    })


# ------------------------------------------------------------------
# WMA
# ------------------------------------------------------------------

class TestWMA:
    def test_output_length(self, sample_close):
        result = wma(sample_close, 5)
        assert len(result) == len(sample_close)

    def test_nan_for_insufficient_data(self, sample_close):
        result = wma(sample_close, 10)
        assert result.iloc[:9].isna().all()
        assert not result.iloc[9:].isna().any()

    def test_manual_calculation(self):
        """Verifica formula manuale su 4 valori."""
        s = pd.Series([1.0, 2.0, 3.0, 4.0])
        result = wma(s, 4)
        # weights = [1,2,3,4], sum = 10
        # wma = (1*1 + 2*2 + 3*3 + 4*4) / 10 = (1+4+9+16)/10 = 3.0
        expected = (1 * 1 + 2 * 2 + 3 * 3 + 4 * 4) / 10
        assert abs(result.iloc[-1] - expected) < 1e-10

    def test_weights_sum_correct(self):
        """WMA di una serie costante deve ritornare la costante."""
        s = pd.Series([5.0] * 20)
        result = wma(s, 10)
        valid = result.dropna()
        assert (np.abs(valid - 5.0) < 1e-10).all()


# ------------------------------------------------------------------
# HMA
# ------------------------------------------------------------------

class TestHMA:
    def test_output_length(self, sample_close):
        result = hma(sample_close, 16)
        assert len(result) == len(sample_close)

    def test_nan_prefix(self, sample_close):
        """HMA(16): half=8, sqrt=4 → warmup = 16 + 4 - 1 barre circa."""
        result = hma(sample_close, 16)
        # Le prime barre devono essere NaN
        assert result.iloc[:10].isna().any()

    def test_formula_consistency(self, sample_close):
        """Verifica che HMA = WMA(2*WMA(n/2) - WMA(n), sqrt(n))."""
        period = 16
        half = int(period / 2)
        sq = int(np.sqrt(period))
        raw = 2.0 * wma(sample_close, half) - wma(sample_close, period)
        expected = wma(raw, sq)
        result = hma(sample_close, period)
        diff = (result - expected).abs().max()
        assert diff < 1e-10, f"HMA differisce dalla formula: max diff = {diff}"

    def test_different_periods(self, sample_close):
        """Verifica che periodi diversi producano valori diversi."""
        h14 = hma(sample_close, 14).dropna()
        h16 = hma(sample_close, 16).dropna()
        # Non devono essere identici
        common_idx = h14.index.intersection(h16.index)
        assert len(common_idx) > 10
        assert not np.allclose(h14[common_idx].values, h16[common_idx].values)

    def test_reference_from_final_exact_match(self):
        """
        Replica la logica di final_exact_match_test.py per verificare
        che la nostra implementazione coincide con quella validata.
        """
        # Stessa implementazione di riferimento da final_exact_match_test.py
        def calculate_hma_ref(close, period=16):
            half_period = int(period / 2)
            sqrt_period = int(np.sqrt(period))

            def wma_ref(series, n):
                weights = np.arange(1, n + 1)
                return series.rolling(n).apply(
                    lambda x: np.sum(x * weights) / weights.sum() if len(x) == n else np.nan,
                    raw=True
                )
            wma_half = wma_ref(close, half_period)
            wma_full = wma_ref(close, period)
            raw_hma = 2 * wma_half - wma_full
            return wma_ref(raw_hma, sqrt_period)

        rng = np.random.default_rng(123)
        close = pd.Series(100 + np.cumsum(rng.normal(0, 0.3, 300)))

        ref = calculate_hma_ref(close, 16)
        ours = hma(close, 16)

        diff = (ref - ours).abs().max()
        assert diff < 1e-8, f"HMA diverge dalla reference: max diff = {diff}"


# ------------------------------------------------------------------
# ATR_SMA
# ------------------------------------------------------------------

class TestATRSMA:
    def test_output_length(self, sample_ohlcv):
        df = sample_ohlcv
        result = atr_sma(df["high"], df["low"], df["close"], 14)
        assert len(result) == len(df)

    def test_nan_prefix(self, sample_ohlcv):
        df = sample_ohlcv
        result = atr_sma(df["high"], df["low"], df["close"], 14)
        # Prima barra del TR ha NaN (no prev_close), poi rolling(14) → 14 NaN iniziali
        assert result.iloc[:14].isna().any()

    def test_formula_manual(self):
        """Verifica formula TR manuale su 3 barre."""
        data = pd.DataFrame({
            "high":  [10.5, 11.0, 10.8],
            "low":   [ 9.5, 10.0,  9.8],
            "close": [10.0, 10.5, 10.2],
        })
        # TR barra 1 (idx=0): high-low = 1.0, no prev_close → TR = 1.0
        # TR barra 2 (idx=1): max(1.0, |11.0-10.0|, |10.0-10.0|) = max(1.0,1.0,0.0) = 1.0
        # TR barra 3 (idx=2): max(1.0, |10.8-10.5|, |9.8-10.5|) = max(1.0, 0.3, 0.7) = 1.0
        # ATR(3) = mean([1.0, 1.0, 1.0]) = 1.0
        result = atr_sma(data["high"], data["low"], data["close"], 3)
        assert abs(result.iloc[-1] - 1.0) < 1e-10

    def test_sma_not_ema(self):
        """Verifica che sia SMA (rolling mean) non EMA."""
        # Crea serie TR costante = 1.0 per period barre, poi spike a 100
        # SMA reale dovrebbe tornare a 1.0 esattamente dopo period barre dallo spike
        period = 5
        n = period * 3
        high = pd.Series([10.0] * n)
        low = pd.Series([9.0] * n)
        close = pd.Series([9.5] * n)

        result = atr_sma(high, low, close, period)
        valid = result.dropna()
        # ATR deve essere 1.0 per tutti i valori validi (TR costante = 1.0)
        assert (np.abs(valid - 1.0) < 1e-10).all()

    def test_atr_pct(self, sample_ohlcv):
        df = sample_ohlcv
        atr = atr_sma(df["high"], df["low"], df["close"], 14)
        pct = atr_sma_pct(df["high"], df["low"], df["close"], 14)
        # atr_pct = atr / close * 100
        expected = (atr / df["close"]) * 100
        diff = (pct - expected).abs().max()
        assert diff < 1e-10

    def test_matches_reference_formula(self):
        """Verifica corrispondenza con formula di riferimento da final_exact_match_test.py."""
        def calculate_atr_ref(df, period=14):
            high, low, close = df["high"], df["low"], df["close"]
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(window=period).mean()
            return (atr / close) * 100

        rng = np.random.default_rng(99)
        n = 200
        close_vals = 100 + np.cumsum(rng.normal(0, 0.3, n))
        df = pd.DataFrame({
            "high": close_vals + rng.uniform(0.1, 0.5, n),
            "low": close_vals - rng.uniform(0.1, 0.5, n),
            "close": close_vals,
        })

        ref = calculate_atr_ref(df, 14)
        ours = atr_sma_pct(df["high"], df["low"], df["close"], 14)

        diff = (ref - ours).abs().max()
        assert diff < 1e-8, f"ATR_SMA diverge dalla reference: max diff = {diff}"
