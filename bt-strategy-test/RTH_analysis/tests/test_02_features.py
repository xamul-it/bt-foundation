"""
test_02_features.py — Test unitari per compute_features()

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

_mod = import_module("02_build_features")
compute_features = _mod.compute_features
WINDOWS = _mod.WINDOWS


# ---------------------------------------------------------------------------
# Fixtures comuni
# ---------------------------------------------------------------------------

N_DAYS = 120
UNIFORM_GAIN = 0.001  # +0.1% ogni giorno


def make_uniform_df(
    n: int = N_DAYS,
    gain: float = UNIFORM_GAIN,
    symbol: str = "SYM",
    start: str = "2020-01-02",
) -> pd.DataFrame:
    """
    Crea un DataFrame con 1 simbolo e n giorni di dati con rth_loggain uniforme.
    OHLCV coerenti con il gain dato.
    """
    dates = pd.date_range(start, periods=n, freq="B")
    o = 100.0
    rows = []
    for d in dates:
        c = o * math.exp(gain)
        h = c * 1.005   # small range sopra il close
        lo = o * 0.995  # small range sotto l'open
        v = 1_000_000.0
        rows.append({
            "feature_date": d,
            "symbol": symbol,
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": v,
            "rth_loggain": math.log(c / o),
            "rth_up_loggain": math.log(h / o),
            "rth_down_loggain": math.log(lo / o),
            "rth_range_log": math.log(h / lo),
            "log_volume": math.log(v),
            "overnight_loggain": math.log(o / (rows[-1]["close"] if rows else o)),
            "cc_logret": math.log(c / (rows[-1]["close"] if rows else o)),
        })
        o = c  # il prossimo open = close corrente (semplificazione)
    df = pd.DataFrame(rows)
    df["feature_date"] = pd.to_datetime(df["feature_date"])
    return df


def make_two_symbol_df(
    n: int = N_DAYS,
    gain_a: float = UNIFORM_GAIN,
    gain_b: float = 0.002,
) -> pd.DataFrame:
    """Crea un DataFrame con 2 simboli distinti."""
    df_a = make_uniform_df(n=n, gain=gain_a, symbol="AAA")
    df_b = make_uniform_df(n=n, gain=gain_b, symbol="BBB")
    return pd.concat([df_a, df_b], ignore_index=True)


# ---------------------------------------------------------------------------
# Fixture pytest
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def uniform_features():
    """Feature calcolate su serie uniforme +0.001, 1 simbolo."""
    df = make_uniform_df()
    return compute_features(df)


@pytest.fixture(scope="module")
def two_sym_features():
    """Feature calcolate su 2 simboli."""
    df = make_two_symbol_df()
    return compute_features(df)


# ---------------------------------------------------------------------------
# Test 1: rth_mom_5 su serie uniforme +0.001 → ≈ 5 × 0.001
# ---------------------------------------------------------------------------

class TestMomSum:
    def test_mom_sum(self, uniform_features):
        col = uniform_features["rth_mom_5"].dropna()
        expected = 5 * UNIFORM_GAIN
        # Tutti i valori validi devono essere ≈ expected
        assert len(col) > 0, "rth_mom_5 è tutto NaN"
        assert abs(col.iloc[-1] - expected) < 1e-10, (
            f"rth_mom_5 atteso ≈ {expected}, ottenuto {col.iloc[-1]}"
        )

    def test_mom_100(self, uniform_features):
        col = uniform_features["rth_mom_100"].dropna()
        expected = 100 * UNIFORM_GAIN
        assert len(col) > 0, "rth_mom_100 è tutto NaN"
        assert abs(col.iloc[-1] - expected) < 1e-8


# ---------------------------------------------------------------------------
# Test 2: rth_mean_20 ≈ 0.001 su serie uniforme
# ---------------------------------------------------------------------------

class TestMean:
    def test_mean(self, uniform_features):
        col = uniform_features["rth_mean_20"].dropna()
        assert len(col) > 0, "rth_mean_20 è tutto NaN"
        assert abs(col.iloc[-1] - UNIFORM_GAIN) < 1e-10, (
            f"rth_mean_20 atteso ≈ {UNIFORM_GAIN}, ottenuto {col.iloc[-1]}"
        )

    def test_mean_all_windows(self, uniform_features):
        for n in WINDOWS:
            col = uniform_features[f"rth_mean_{n}"].dropna()
            assert len(col) > 0, f"rth_mean_{n} è tutto NaN"
            assert abs(col.iloc[-1] - UNIFORM_GAIN) < 1e-10, (
                f"rth_mean_{n} atteso ≈ {UNIFORM_GAIN}, ottenuto {col.iloc[-1]}"
            )


# ---------------------------------------------------------------------------
# Test 3: rth_ewm_mean_5 converge verso 0.001 su serie uniforme
# ---------------------------------------------------------------------------

class TestEwmMean:
    def test_ewm_mean_converges(self, uniform_features):
        col = uniform_features["rth_ewm_mean_5"]
        # Con serie costante, EWM converge esattamente al valore costante
        last_val = col.iloc[-1]
        assert abs(last_val - UNIFORM_GAIN) < 1e-10, (
            f"rth_ewm_mean_5 non converge: {last_val}"
        )

    def test_ewm_mean_no_nan_at_start(self, uniform_features):
        # EWM non usa min_periods → deve avere un valore dalla prima riga
        col = uniform_features["rth_ewm_mean_5"]
        assert not pd.isna(col.iloc[0]), "rth_ewm_mean_5 non deve essere NaN al primo giorno"

    def test_ewm_all_spans(self, uniform_features):
        for span in [5, 20, 50]:
            col = uniform_features[f"rth_ewm_mean_{span}"]
            last_val = col.iloc[-1]
            assert abs(last_val - UNIFORM_GAIN) < 1e-10, (
                f"rth_ewm_mean_{span} non converge a {UNIFORM_GAIN}: {last_val}"
            )


# ---------------------------------------------------------------------------
# Test 4: su serie costante, rth_std_20 = 0
# ---------------------------------------------------------------------------

class TestStdZero:
    def test_std_zero(self, uniform_features):
        col = uniform_features["rth_std_20"].dropna()
        assert len(col) > 0, "rth_std_20 è tutto NaN"
        assert abs(col.iloc[-1]) < 1e-10, (
            f"rth_std_20 atteso = 0 su serie costante, ottenuto {col.iloc[-1]}"
        )

    def test_std_all_windows_zero(self, uniform_features):
        for n in WINDOWS:
            col = uniform_features[f"rth_std_{n}"].dropna()
            assert len(col) > 0, f"rth_std_{n} è tutto NaN"
            assert abs(col.iloc[-1]) < 1e-10, (
                f"rth_std_{n} atteso = 0 su serie costante, ottenuto {col.iloc[-1]}"
            )


# ---------------------------------------------------------------------------
# Test 5: rth_mom_norm_20 = NaN quando std = 0
# ---------------------------------------------------------------------------

class TestMomNormZeroStd:
    def test_mom_norm_nan_when_std_zero(self, uniform_features):
        col = uniform_features["rth_mom_norm_20"]
        # Su serie costante std=0 → mom_norm deve essere NaN in tutti i valori validi
        valid = col.dropna()
        # Se std=0 → deve essere NaN (nessun valore non-NaN dopo min_periods)
        # In realtà: std=0, quindi tutti i valori dovrebbero essere NaN
        # Verifica che il valore finale (se non NaN) sia NaN per std=0
        assert col.iloc[-1] is np.nan or pd.isna(col.iloc[-1]), (
            f"rth_mom_norm_20 atteso NaN quando std=0, ottenuto {col.iloc[-1]}"
        )

    def test_mom_norm_finite_when_std_positive(self):
        """Su serie non uniforme, mom_norm deve avere valori finiti."""
        np.random.seed(42)
        n = 120
        dates = pd.date_range("2020-01-02", periods=n, freq="B")
        gains = np.random.normal(0.001, 0.01, n)
        rows = []
        o = 100.0
        prev_close = o
        for i, (d, g) in enumerate(zip(dates, gains)):
            c = o * math.exp(g)
            h = max(o, c) * 1.002
            lo = min(o, c) * 0.998
            rows.append({
                "feature_date": d,
                "symbol": "SYM",
                "open": o,
                "high": h,
                "low": lo,
                "close": c,
                "volume": 1_000_000.0,
                "rth_loggain": math.log(c / o),
                "rth_up_loggain": math.log(h / o),
                "rth_down_loggain": math.log(lo / o),
                "rth_range_log": math.log(h / lo),
                "log_volume": math.log(1_000_000.0),
                "overnight_loggain": math.log(o / prev_close) if i > 0 else np.nan,
                "cc_logret": math.log(c / prev_close) if i > 0 else np.nan,
            })
            prev_close = c
            o = c
        df = pd.DataFrame(rows)
        df["feature_date"] = pd.to_datetime(df["feature_date"])
        feat = compute_features(df)
        col = feat["rth_mom_norm_20"].dropna()
        assert len(col) > 0
        assert np.isfinite(col.iloc[-1]), f"rth_mom_norm_20 non finito: {col.iloc[-1]}"


# ---------------------------------------------------------------------------
# Test 6: su serie uniforme positiva, rth_eff_20 ≈ 1.0
# ---------------------------------------------------------------------------

class TestEffPerfectTrend:
    def test_eff_perfect_trend(self, uniform_features):
        col = uniform_features["rth_eff_20"].dropna()
        assert len(col) > 0, "rth_eff_20 è tutto NaN"
        assert abs(col.iloc[-1] - 1.0) < 1e-10, (
            f"rth_eff_20 atteso ≈ 1.0 su serie uniforme, ottenuto {col.iloc[-1]}"
        )

    def test_eff_all_windows(self, uniform_features):
        for n in WINDOWS:
            col = uniform_features[f"rth_eff_{n}"].dropna()
            assert len(col) > 0, f"rth_eff_{n} è tutto NaN"
            assert abs(col.iloc[-1] - 1.0) < 1e-10, (
                f"rth_eff_{n} atteso 1.0, ottenuto {col.iloc[-1]}"
            )

    def test_eff_range(self, uniform_features):
        """rth_eff deve essere in [0, 1] sempre."""
        for n in WINDOWS:
            col = uniform_features[f"rth_eff_{n}"].dropna()
            assert (col >= -1e-10).all(), f"rth_eff_{n} ha valori negativi"
            assert (col <= 1.0 + 1e-10).all(), f"rth_eff_{n} ha valori > 1"


# ---------------------------------------------------------------------------
# Test 7: rth_signed_eff_20 ≈ 1.0 su positiva, ≈ -1.0 su negativa
# ---------------------------------------------------------------------------

class TestSignedEffDirection:
    def test_signed_eff_positive(self, uniform_features):
        col = uniform_features["rth_signed_eff_20"].dropna()
        assert len(col) > 0
        assert abs(col.iloc[-1] - 1.0) < 1e-10, (
            f"rth_signed_eff_20 atteso ≈ 1.0 su serie positiva, ottenuto {col.iloc[-1]}"
        )

    def test_signed_eff_negative(self):
        df = make_uniform_df(gain=-UNIFORM_GAIN)
        feat = compute_features(df)
        col = feat["rth_signed_eff_20"].dropna()
        assert len(col) > 0
        assert abs(col.iloc[-1] - (-1.0)) < 1e-10, (
            f"rth_signed_eff_20 atteso ≈ -1.0 su serie negativa, ottenuto {col.iloc[-1]}"
        )

    def test_signed_eff_range(self):
        """rth_signed_eff deve essere in [-1, 1]."""
        df = make_two_symbol_df()
        feat = compute_features(df)
        for n in WINDOWS:
            col = feat[f"rth_signed_eff_{n}"].dropna()
            assert (col >= -1.0 - 1e-10).all(), f"rth_signed_eff_{n} < -1"
            assert (col <= 1.0 + 1e-10).all(), f"rth_signed_eff_{n} > 1"


# ---------------------------------------------------------------------------
# Test 8: rth_winrate_20 = 1.0 su serie uniforme positiva
# ---------------------------------------------------------------------------

class TestWinrateAllPositive:
    def test_winrate_all_positive(self, uniform_features):
        col = uniform_features["rth_winrate_20"].dropna()
        assert len(col) > 0
        assert abs(col.iloc[-1] - 1.0) < 1e-10, (
            f"rth_winrate_20 atteso 1.0, ottenuto {col.iloc[-1]}"
        )

    def test_winrate_all_negative(self):
        df = make_uniform_df(gain=-UNIFORM_GAIN)
        feat = compute_features(df)
        col = feat["rth_winrate_20"].dropna()
        assert len(col) > 0
        assert abs(col.iloc[-1] - 0.0) < 1e-10, (
            f"rth_winrate_20 atteso 0.0 su serie negativa, ottenuto {col.iloc[-1]}"
        )

    def test_winrate_range(self, uniform_features):
        """rth_winrate deve essere in [0, 1]."""
        for n in WINDOWS:
            col = uniform_features[f"rth_winrate_{n}"].dropna()
            assert (col >= -1e-10).all()
            assert (col <= 1.0 + 1e-10).all()


# ---------------------------------------------------------------------------
# Test 9: rth_rs = 0 se tutti i simboli hanno stesso rth_loggain
# ---------------------------------------------------------------------------

class TestRsZeroWhenUniform:
    def test_rs_zero_single_symbol(self, uniform_features):
        """Con 1 simbolo, benchmark = rth_loggain → rth_rs = 0."""
        col = uniform_features["rth_rs"]
        assert (col.abs() < 1e-10).all(), (
            f"rth_rs atteso 0 con 1 simbolo (stesso valore = benchmark), max={col.abs().max()}"
        )

    def test_rs_zero_multi_symbol_same_gain(self):
        """Con N simboli tutti con stesso gain, rth_rs = 0."""
        frames = [make_uniform_df(gain=UNIFORM_GAIN, symbol=f"S{i}") for i in range(5)]
        df = pd.concat(frames, ignore_index=True)
        feat = compute_features(df)
        col = feat["rth_rs"]
        assert (col.abs() < 1e-10).all(), (
            f"rth_rs atteso 0 con simboli identici, max={col.abs().max()}"
        )

    def test_rs_positive_for_outperformer(self):
        """Il simbolo con gain maggiore ha rth_rs > 0."""
        df_a = make_uniform_df(gain=0.002, symbol="AAA")
        df_b = make_uniform_df(gain=0.001, symbol="BBB")
        df = pd.concat([df_a, df_b], ignore_index=True)
        feat = compute_features(df)
        rs_a = feat[feat["symbol"] == "AAA"]["rth_rs"].iloc[-1]
        rs_b = feat[feat["symbol"] == "BBB"]["rth_rs"].iloc[-1]
        assert rs_a > 0, f"AAA (gain=0.002) dovrebbe avere rth_rs > 0: {rs_a}"
        assert rs_b < 0, f"BBB (gain=0.001) dovrebbe avere rth_rs < 0: {rs_b}"


# ---------------------------------------------------------------------------
# Test 10: rth_share_20 ∈ [0, 1] sempre
# ---------------------------------------------------------------------------

class TestRthShareRange:
    def test_rth_share_range_single(self, uniform_features):
        col = uniform_features["rth_share_20"].dropna()
        assert len(col) > 0
        assert (col >= -1e-10).all(), f"rth_share_20 ha valori < 0: min={col.min()}"
        assert (col <= 1.0 + 1e-10).all(), f"rth_share_20 ha valori > 1: max={col.max()}"

    def test_rth_share_range_two_sym(self, two_sym_features):
        col = two_sym_features["rth_share_20"].dropna()
        assert (col >= -1e-10).all()
        assert (col <= 1.0 + 1e-10).all()


# ---------------------------------------------------------------------------
# Test 11: verifica che tutte le colonne attese siano presenti
# ---------------------------------------------------------------------------

class TestOutputColumns:
    def _expected_columns(self):
        cols = set()

        # Input originali
        cols.update([
            "feature_date", "symbol", "open", "high", "low", "close", "volume",
            "rth_loggain", "rth_up_loggain", "rth_down_loggain", "rth_range_log",
            "log_volume", "overnight_loggain", "cc_logret",
        ])

        # Famiglia 1: momentum
        for n in WINDOWS:
            cols.add(f"rth_mom_{n}")

        # Famiglia 2: drift medio
        for n in WINDOWS:
            cols.add(f"rth_mean_{n}")

        # Famiglia 3: EWM
        for sp in [5, 20, 50]:
            cols.add(f"rth_ewm_mean_{sp}")

        # Famiglia 4: accel
        cols.update(["rth_drift_accel_12_26", "rth_drift_accel_signal_9", "rth_drift_accel_hist"])

        # Famiglia 5: std
        for n in WINDOWS:
            cols.add(f"rth_std_{n}")

        # Famiglia 6: mom_norm
        for n in WINDOWS:
            cols.add(f"rth_mom_norm_{n}")

        # Famiglia 7: eff
        for n in WINDOWS:
            cols.add(f"rth_eff_{n}")
            cols.add(f"rth_signed_eff_{n}")

        # Famiglia 8: winrate
        for n in WINDOWS:
            cols.add(f"rth_winrate_{n}")

        # Famiglia 9: pressione
        for n in WINDOWS:
            cols.add(f"rth_upside_{n}")
            cols.add(f"rth_downside_{n}")
            cols.add(f"rth_pressure_balance_{n}")

        # Famiglia 10: close-to-range
        cols.add("rth_close_to_range")
        for n in [5, 20]:
            cols.add(f"rth_close_to_range_mean_{n}")

        # Famiglia 11: range expansion
        for n in [5, 20]:
            cols.add(f"rth_range_mean_{n}")
        cols.add("rth_range_exp_5_20")

        # Famiglia 12: volume relativo
        cols.update(["rth_logvol_z_20", "rth_rvol_5_20"])

        # Famiglia 13: RS
        cols.add("rth_rs")
        for n in [5, 20]:
            cols.add(f"rth_rs_{n}")

        # Famiglia 14: overnight
        for n in [5, 20]:
            cols.add(f"on_mom_{n}")
        cols.add("rth_share_20")

        return cols

    def test_output_columns(self, uniform_features):
        expected = self._expected_columns()
        actual = set(uniform_features.columns)
        missing = expected - actual
        assert not missing, f"Colonne mancanti: {sorted(missing)}"

    def test_no_extra_temp_columns(self, uniform_features):
        """Verifica che le colonne temporanee siano state rimosse."""
        temp_cols = [c for c in uniform_features.columns if c.startswith("_")]
        assert not temp_cols, f"Colonne temporanee rimaste: {temp_cols}"

    def test_ewm_intermediate_not_saved(self, uniform_features):
        """Gli EWM intermedi 12 e 26 non devono essere nel parquet finale."""
        assert "rth_ewm_mean_12" not in uniform_features.columns
        assert "rth_ewm_mean_26" not in uniform_features.columns


# ---------------------------------------------------------------------------
# Test 12: no cross-symbol contamination
# ---------------------------------------------------------------------------

class TestNoCrossSymbolContamination:
    def test_no_contamination(self):
        """
        Calcola feature su 2 simboli con valori molto diversi e verifica che
        i risultati di AAA non dipendano da BBB.
        """
        # AAA: gain = +0.001 ogni giorno
        # BBB: gain = -0.005 ogni giorno (molto diverso)
        df_aa = make_uniform_df(gain=0.001, symbol="AAA")
        df_bb = make_uniform_df(gain=-0.005, symbol="BBB")
        df_combined = pd.concat([df_aa, df_bb], ignore_index=True)

        # Calcola da soli su AAA
        feat_aa_only = compute_features(df_aa)

        # Calcola su entrambi e filtra AAA
        feat_combined = compute_features(df_combined)
        feat_aa_from_combined = feat_combined[feat_combined["symbol"] == "AAA"].reset_index(drop=True)

        # Confronta rth_mom_20 (dipende solo da rth_loggain di AAA)
        col_only = feat_aa_only["rth_mom_20"].reset_index(drop=True)
        col_combined = feat_aa_from_combined["rth_mom_20"].reset_index(drop=True)

        pd.testing.assert_series_equal(
            col_only.dropna(),
            col_combined.dropna(),
            check_names=False,
            atol=1e-10,
            rtol=0,
        )

    def test_no_contamination_std(self):
        """rth_std non deve essere influenzato dall'altro simbolo."""
        df_aa = make_uniform_df(gain=0.001, symbol="AAA")
        df_bb = make_uniform_df(gain=-0.005, symbol="BBB")
        df_combined = pd.concat([df_aa, df_bb], ignore_index=True)

        feat_aa_only = compute_features(df_aa)
        feat_combined = compute_features(df_combined)
        feat_aa_from_combined = feat_combined[feat_combined["symbol"] == "AAA"].reset_index(drop=True)

        col_only = feat_aa_only["rth_std_20"].reset_index(drop=True)
        col_combined = feat_aa_from_combined["rth_std_20"].reset_index(drop=True)

        pd.testing.assert_series_equal(
            col_only.dropna(),
            col_combined.dropna(),
            check_names=False,
            atol=1e-10,
            rtol=0,
        )

    def test_rs_depends_on_benchmark(self):
        """
        rth_rs DEVE differire tra calcolo standalone vs calcolo combinato
        (perché il benchmark cambia). Questo è il comportamento atteso.
        """
        df_aa = make_uniform_df(gain=0.001, symbol="AAA")
        df_bb = make_uniform_df(gain=-0.005, symbol="BBB")
        df_combined = pd.concat([df_aa, df_bb], ignore_index=True)

        feat_aa_only = compute_features(df_aa)
        feat_combined = compute_features(df_combined)
        feat_aa_from_combined = feat_combined[feat_combined["symbol"] == "AAA"].reset_index(drop=True)

        # rth_rs di AAA-only = 0 (benchmark è AAA stesso)
        rs_only = feat_aa_only["rth_rs"].iloc[-1]
        assert abs(rs_only) < 1e-10, f"rth_rs standalone atteso 0, ottenuto {rs_only}"

        # rth_rs di AAA nel combined != 0 (benchmark include BBB)
        rs_combined = feat_aa_from_combined["rth_rs"].iloc[-1]
        assert abs(rs_combined) > 1e-6, (
            f"rth_rs di AAA nel combined dovrebbe essere != 0 (gain AAA > benchmark)"
        )


# ---------------------------------------------------------------------------
# Test aggiuntivi: edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_min_periods_nan_at_start(self, uniform_features):
        """Le feature rolling con min_periods=N devono essere NaN per le prime N-1 righe."""
        # rth_mom_100 ha min_periods=100 → le prime 99 righe devono essere NaN
        col = uniform_features["rth_mom_100"]
        assert pd.isna(col.iloc[0]), "rth_mom_100 row 0 deve essere NaN"
        assert pd.isna(col.iloc[98]), "rth_mom_100 row 98 deve essere NaN"
        assert not pd.isna(col.iloc[99]), "rth_mom_100 row 99 (indice 0-based) deve avere valore"

    def test_accel_hist_is_difference(self, uniform_features):
        """rth_drift_accel_hist = rth_drift_accel_12_26 - rth_drift_accel_signal_9."""
        df = uniform_features
        diff = (df["rth_drift_accel_12_26"] - df["rth_drift_accel_signal_9"])
        pd.testing.assert_series_equal(
            df["rth_drift_accel_hist"],
            diff,
            check_names=False,
            atol=1e-12,
            rtol=0,
        )

    def test_pressure_balance_definition(self, uniform_features):
        """rth_pressure_balance_N = rth_upside_N - rth_downside_N."""
        for n in WINDOWS:
            balance = uniform_features[f"rth_pressure_balance_{n}"]
            expected = uniform_features[f"rth_upside_{n}"] - uniform_features[f"rth_downside_{n}"]
            pd.testing.assert_series_equal(balance, expected, check_names=False, atol=1e-12, rtol=0)

    def test_feature_date_symbol_preserved(self, uniform_features):
        """feature_date e symbol devono rimanere come colonne (non index)."""
        assert "feature_date" in uniform_features.columns
        assert "symbol" in uniform_features.columns
        assert uniform_features.index.name != "feature_date"

    def test_sorted_output(self, two_sym_features):
        """Output deve essere ordinato per symbol + feature_date."""
        df = two_sym_features
        for sym, grp in df.groupby("symbol"):
            dates = grp["feature_date"].values
            assert (dates[:-1] <= dates[1:]).all(), f"Simbolo {sym} non ordinato per data"

    def test_close_to_range_nan_on_zero_range(self):
        """rth_close_to_range deve essere NaN quando rth_range_log ≈ 0."""
        df = make_uniform_df()
        # Forza rth_range_log = 0 su tutte le righe
        df["rth_range_log"] = 0.0
        feat = compute_features(df)
        col = feat["rth_close_to_range"]
        assert col.isna().all(), "rth_close_to_range deve essere NaN quando range=0"

    def test_range_exp_nan_on_zero_range_mean(self):
        """rth_range_exp_5_20 deve essere NaN quando rth_range_mean_20 ≈ 0."""
        df = make_uniform_df()
        df["rth_range_log"] = 0.0
        feat = compute_features(df)
        col = feat["rth_range_exp_5_20"].dropna()
        # Con range=0, range_mean_20=0 → NaN
        # I valori non-NaN (se presenti) non devono esistere
        assert len(col) == 0 or col.isna().all(), (
            "rth_range_exp_5_20 deve essere NaN quando range_mean_20=0"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
