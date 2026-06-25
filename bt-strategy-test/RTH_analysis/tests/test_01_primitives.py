"""
test_01_primitives.py — Test unitari per compute_primitives()

Tutti i test usano dati sintetici costruiti a mano, senza rete.
"""

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Aggiungi il parent directory al path per importare il modulo principale
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from importlib import import_module

# Import diretto del modulo (nome con trattino non è un identificatore valido,
# ma il file inizia con 01_ quindi usiamo import_module)
_mod = import_module("01_build_primitives")
compute_primitives = _mod.compute_primitives
check_coherence = _mod.check_coherence


# ---------------------------------------------------------------------------
# Fixtures: costruisce DataFrame OHLCV sintetici
# ---------------------------------------------------------------------------

def make_ohlcv(rows: list[dict]) -> pd.DataFrame:
    """
    Helper: costruisce un DataFrame OHLCV con DatetimeIndex da una lista di dict.
    Ogni dict deve avere: Open, High, Low, Close, Volume (e opzionalmente 'date').
    """
    dates = pd.date_range("2020-01-02", periods=len(rows), freq="B")
    df = pd.DataFrame(rows, index=dates)
    # Assicura tipi float
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df


# ---------------------------------------------------------------------------
# Test 1: rth_loggain = ln(C/O)
# ---------------------------------------------------------------------------

class TestRthLoggain:
    def test_positive_gain(self):
        """Open=100, Close=101 → rth_loggain ≈ ln(1.01)"""
        df = make_ohlcv([{"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1000}])
        result = compute_primitives(df)
        assert len(result) == 1
        expected = math.log(101 / 100)
        assert abs(result["rth_loggain"].iloc[0] - expected) < 1e-12

    def test_negative_gain(self):
        """Open=100, Close=98 → rth_loggain < 0"""
        df = make_ohlcv([{"Open": 100, "High": 101, "Low": 97, "Close": 98, "Volume": 1000}])
        result = compute_primitives(df)
        expected = math.log(98 / 100)
        assert abs(result["rth_loggain"].iloc[0] - expected) < 1e-12
        assert result["rth_loggain"].iloc[0] < 0

    def test_flat_session(self):
        """Open==Close → rth_loggain = 0"""
        df = make_ohlcv([{"Open": 100, "High": 102, "Low": 99, "Close": 100, "Volume": 1000}])
        result = compute_primitives(df)
        assert abs(result["rth_loggain"].iloc[0]) < 1e-12


# ---------------------------------------------------------------------------
# Test 2: rth_up_loggain = ln(H/O)
# ---------------------------------------------------------------------------

class TestRthUpLoggain:
    def test_basic(self):
        """Open=100, High=105 → rth_up_loggain ≈ ln(1.05)"""
        df = make_ohlcv([{"Open": 100, "High": 105, "Low": 99, "Close": 102, "Volume": 1000}])
        result = compute_primitives(df)
        expected = math.log(105 / 100)
        assert abs(result["rth_up_loggain"].iloc[0] - expected) < 1e-12

    def test_always_nonnegative(self):
        """rth_up_loggain deve essere ≥ 0 (H ≥ O per costruzione dei dati OHLC validi)"""
        df = make_ohlcv([
            {"Open": 100, "High": 104, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": 100, "High": 100, "Low": 98, "Close": 99, "Volume": 1000},
        ])
        result = compute_primitives(df)
        assert (result["rth_up_loggain"] >= -1e-12).all()


# ---------------------------------------------------------------------------
# Test 3: rth_down_loggain = ln(L/O)
# ---------------------------------------------------------------------------

class TestRthDownLoggain:
    def test_basic(self):
        """Open=100, Low=98 → rth_down_loggain ≈ ln(0.98) < 0"""
        df = make_ohlcv([{"Open": 100, "High": 105, "Low": 98, "Close": 102, "Volume": 1000}])
        result = compute_primitives(df)
        expected = math.log(98 / 100)
        assert abs(result["rth_down_loggain"].iloc[0] - expected) < 1e-12
        assert result["rth_down_loggain"].iloc[0] < 0

    def test_always_nonpositive(self):
        """rth_down_loggain deve essere ≤ 0 (L ≤ O)"""
        df = make_ohlcv([
            {"Open": 100, "High": 104, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": 100, "High": 102, "Low": 100, "Close": 101, "Volume": 1000},
        ])
        result = compute_primitives(df)
        assert (result["rth_down_loggain"] <= 1e-12).all()


# ---------------------------------------------------------------------------
# Test 4: rth_range_log = ln(H/L)
# ---------------------------------------------------------------------------

class TestRthRangeLog:
    def test_basic(self):
        """High=105, Low=98 → rth_range_log ≈ ln(105/98) > 0"""
        df = make_ohlcv([{"Open": 100, "High": 105, "Low": 98, "Close": 102, "Volume": 1000}])
        result = compute_primitives(df)
        expected = math.log(105 / 98)
        assert abs(result["rth_range_log"].iloc[0] - expected) < 1e-12
        assert result["rth_range_log"].iloc[0] > 0

    def test_always_nonnegative(self):
        """rth_range_log deve essere ≥ 0"""
        df = make_ohlcv([
            {"Open": 100, "High": 105, "Low": 98, "Close": 102, "Volume": 1000},
            {"Open": 100, "High": 101, "Low": 99, "Close": 100, "Volume": 1000},
        ])
        result = compute_primitives(df)
        assert (result["rth_range_log"] >= -1e-12).all()


# ---------------------------------------------------------------------------
# Test 5: overnight_loggain = ln(O_t / C_{t-1})
# ---------------------------------------------------------------------------

class TestOvernightLoggain:
    def test_basic(self):
        """Open_t=101, Close_{t-1}=100 → overnight_loggain ≈ ln(1.01)"""
        df = make_ohlcv([
            {"Open": 100, "High": 101, "Low": 99, "Close": 100, "Volume": 1000},
            {"Open": 101, "High": 103, "Low": 100, "Close": 102, "Volume": 1000},
        ])
        result = compute_primitives(df)
        expected = math.log(101 / 100)
        assert abs(result["overnight_loggain"].iloc[1] - expected) < 1e-12

    def test_first_row_is_nan(self):
        """Il primo giorno non ha C_{t-1}: overnight_loggain deve essere NaN"""
        df = make_ohlcv([
            {"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": 102, "High": 104, "Low": 101, "Close": 103, "Volume": 1000},
        ])
        result = compute_primitives(df)
        assert pd.isna(result["overnight_loggain"].iloc[0])

    def test_negative_overnight(self):
        """Gap ribassista: O_t < C_{t-1} → overnight_loggain < 0"""
        df = make_ohlcv([
            {"Open": 100, "High": 101, "Low": 99, "Close": 105, "Volume": 1000},
            {"Open": 103, "High": 104, "Low": 101, "Close": 102, "Volume": 1000},
        ])
        result = compute_primitives(df)
        assert result["overnight_loggain"].iloc[1] < 0


# ---------------------------------------------------------------------------
# Test 6: cc_logret ≈ overnight_loggain + rth_loggain
# ---------------------------------------------------------------------------

class TestCcCoherence:
    def test_coherence_two_rows(self):
        """Verifica che cc_logret = overnight + rth per un caso a 2 righe"""
        df = make_ohlcv([
            {"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": 103, "High": 106, "Low": 102, "Close": 105, "Volume": 1500},
        ])
        result = compute_primitives(df)

        # Per la seconda riga:
        row = result.iloc[1]
        reconstructed = row["overnight_loggain"] + row["rth_loggain"]
        assert abs(row["cc_logret"] - reconstructed) < 1e-10

    def test_coherence_many_rows(self):
        """Verifica coerenza su 10 righe con prezzi casuali ma deterministici"""
        # Prezzi costruiti deterministicamente per ripetibilità
        prices = [100.0, 101.5, 103.2, 102.0, 104.5, 106.0, 105.5, 107.0, 108.3, 110.0]
        rows = []
        for i, close in enumerate(prices):
            open_ = close * 0.995 + i * 0.1
            high = close * 1.01
            low = open_ * 0.99
            rows.append({"Open": open_, "High": high, "Low": low, "Close": close, "Volume": 1000 + i * 100})

        df = make_ohlcv(rows)
        result = compute_primitives(df)

        # Usa check_coherence
        # Costruisci un DataFrame con colonna symbol per check_coherence
        result_with_sym = result.copy()
        result_with_sym["symbol"] = "TEST"
        ok, max_err = check_coherence(result_with_sym)
        assert ok, f"Coerenza fallita: max_error={max_err:.2e}"
        assert max_err < 1e-10

    def test_cc_logret_direct(self):
        """Verifica che cc_logret sia proprio ln(C_t / C_{t-1})"""
        df = make_ohlcv([
            {"Open": 100, "High": 102, "Low": 99, "Close": 102, "Volume": 1000},
            {"Open": 103, "High": 107, "Low": 102, "Close": 106, "Volume": 1500},
        ])
        result = compute_primitives(df)
        expected_cc = math.log(106 / 102)
        assert abs(result["cc_logret"].iloc[1] - expected_cc) < 1e-12


# ---------------------------------------------------------------------------
# Test 7: range zero (H == L == O == C)
# ---------------------------------------------------------------------------

class TestZeroRange:
    def test_flat_candle(self):
        """Open=High=Low=Close=100 → rth_range_log=0, rth_loggain=0"""
        df = make_ohlcv([{"Open": 100, "High": 100, "Low": 100, "Close": 100, "Volume": 500}])
        result = compute_primitives(df)
        assert abs(result["rth_range_log"].iloc[0]) < 1e-12
        assert abs(result["rth_loggain"].iloc[0]) < 1e-12
        assert abs(result["rth_up_loggain"].iloc[0]) < 1e-12
        assert abs(result["rth_down_loggain"].iloc[0]) < 1e-12

    def test_flat_candle_in_sequence(self):
        """Candela flat in mezzo a righe normali non deve generare errori"""
        df = make_ohlcv([
            {"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": 101, "High": 101, "Low": 101, "Close": 101, "Volume": 0},
            {"Open": 101, "High": 103, "Low": 100, "Close": 102, "Volume": 900},
        ])
        result = compute_primitives(df)
        assert len(result) == 3
        assert abs(result["rth_range_log"].iloc[1]) < 1e-12


# ---------------------------------------------------------------------------
# Test aggiuntivi: edge cases e struttura output
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_output_columns(self):
        """Verifica che tutte le colonne attese siano presenti"""
        df = make_ohlcv([{"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1000}])
        result = compute_primitives(df)
        expected_cols = {
            "open", "high", "low", "close", "volume",
            "rth_loggain", "rth_up_loggain", "rth_down_loggain",
            "rth_range_log", "log_volume", "overnight_loggain", "cc_logret",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_nan_rows_dropped(self):
        """Righe con NaN vengono droppate"""
        df = make_ohlcv([
            {"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": float("nan"), "High": 102, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": 100, "High": 103, "Low": 99, "Close": 102, "Volume": 1100},
        ])
        result = compute_primitives(df)
        assert len(result) == 2

    def test_zero_ohlc_rows_dropped(self):
        """Righe con Open=0 vengono droppate"""
        df = make_ohlcv([
            {"Open": 0, "High": 102, "Low": 99, "Close": 101, "Volume": 1000},
            {"Open": 100, "High": 103, "Low": 99, "Close": 102, "Volume": 1100},
        ])
        result = compute_primitives(df)
        assert len(result) == 1
        assert abs(result["open"].iloc[0] - 100.0) < 1e-9

    def test_log_volume(self):
        """log_volume = ln(Volume)"""
        df = make_ohlcv([{"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": math.e}])
        result = compute_primitives(df)
        assert abs(result["log_volume"].iloc[0] - 1.0) < 1e-10

    def test_empty_after_cleaning(self):
        """Se tutti i dati vengono droppati, restituisce DataFrame vuoto"""
        df = make_ohlcv([
            {"Open": 0, "High": 0, "Low": 0, "Close": 0, "Volume": 0},
        ])
        result = compute_primitives(df)
        assert result.empty


# ---------------------------------------------------------------------------
# Test 8: log_volume = ln(Volume)
# ---------------------------------------------------------------------------

class TestLogVolume:
    def test_volume_1000(self):
        """Volume=1000 → log_volume ≈ ln(1000) ≈ 6.9078"""
        df = make_ohlcv([{"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1000}])
        result = compute_primitives(df)
        expected = math.log(1000)  # ≈ 6.907755...
        assert abs(result["log_volume"].iloc[0] - expected) < 1e-10

    def test_volume_1(self):
        """Volume=1 → log_volume = ln(1) = 0.0"""
        df = make_ohlcv([{"Open": 100, "High": 102, "Low": 99, "Close": 101, "Volume": 1}])
        result = compute_primitives(df)
        assert abs(result["log_volume"].iloc[0] - 0.0) < 1e-12


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
