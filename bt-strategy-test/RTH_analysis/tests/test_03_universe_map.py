"""
test_03_universe_map.py — Test unitari per compute_universe_map()

Tutti i test usano dati sintetici costruiti a mano.
Nessun accesso a rete o filesystem (il calendar viene passato direttamente
come lista di date).
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

_mod = import_module("03_build_universe_map")
compute_universe_map = _mod.compute_universe_map
RANK_COLS = _mod.RANK_COLS


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

FEATURE_COLS_BASE = [
    "rth_mom_20",
    "rth_eff_20",
    "rth_signed_eff_20",
    "rth_range_exp_5_20",
    "rth_rvol_5_20",
    "rth_mom_norm_20",
]


def make_df(
    symbols=("AAA", "BBB"),
    feature_dates=None,
    seed=42,
) -> pd.DataFrame:
    """
    Costruisce un DataFrame minimale con 2 simboli e N feature_date.

    Se feature_dates è None, genera 10 giorni lavorativi a partire da 2025-01-02.
    """
    if feature_dates is None:
        feature_dates = pd.bdate_range("2025-01-02", periods=10).tolist()

    rows = []
    rng = np.random.default_rng(seed)

    for sym_i, sym in enumerate(symbols):
        for fd in feature_dates:
            row = {
                "feature_date": pd.Timestamp(fd),
                "symbol": sym,
            }
            for col in FEATURE_COLS_BASE:
                row[col] = rng.uniform(0.5 + sym_i * 0.1, 1.5 + sym_i * 0.1)
            rows.append(row)

    df = pd.DataFrame(rows)
    return df


# Calendario "fisso" per i test: 3 giorni di mercato noti
MARKET_DAYS_SIMPLE = ["2025-01-02", "2025-01-03", "2025-01-06"]

# Calendario esteso per fixture a 10 giorni
MARKET_DAYS_EXTENDED = [
    "2025-01-02",
    "2025-01-03",
    "2025-01-06",
    "2025-01-07",
    "2025-01-08",
    "2025-01-09",
    "2025-01-10",
    "2025-01-13",
    "2025-01-14",
    "2025-01-15",
    "2025-01-16",
    "2025-01-17",
]


# ---------------------------------------------------------------------------
# Test 1: trading_date è il giorno di mercato successivo
# ---------------------------------------------------------------------------

def test_trading_date_is_next_market_day():
    """
    feature_date='2025-01-02' con market_days = ['2025-01-02', '2025-01-03', '2025-01-06']
    → trading_date='2025-01-03'
    """
    df = pd.DataFrame([
        {
            "feature_date": pd.Timestamp("2025-01-02"),
            "symbol": "AAA",
            **{col: 1.0 for col in FEATURE_COLS_BASE},
        }
    ])
    result, _ = compute_universe_map(df, MARKET_DAYS_SIMPLE)
    td = result["trading_date"].iloc[0]
    assert td == pd.Timestamp("2025-01-03"), f"Atteso 2025-01-03, ottenuto {td}"


# ---------------------------------------------------------------------------
# Test 2: salta il weekend (venerdì → lunedì)
# ---------------------------------------------------------------------------

def test_trading_date_skips_weekend():
    """
    feature_date='2025-01-03' (venerdì) → trading_date='2025-01-06' (lunedì successivo)
    con market_days che include '2025-01-06' ma non i weekend.
    """
    df = pd.DataFrame([
        {
            "feature_date": pd.Timestamp("2025-01-03"),
            "symbol": "AAA",
            **{col: 1.0 for col in FEATURE_COLS_BASE},
        }
    ])
    result, _ = compute_universe_map(df, MARKET_DAYS_SIMPLE)
    td = result["trading_date"].iloc[0]
    assert td == pd.Timestamp("2025-01-06"), f"Atteso 2025-01-06, ottenuto {td}"


# ---------------------------------------------------------------------------
# Test 3: salta una festività (giorno assente dalla lista market_days)
# ---------------------------------------------------------------------------

def test_trading_date_skips_holiday():
    """
    feature_date='2025-01-02', market_days NON include '2025-01-03' (festività mock)
    → salta a '2025-01-06'.
    """
    market_days_no_jan3 = ["2025-01-02", "2025-01-06"]  # 2025-01-03 assente
    df = pd.DataFrame([
        {
            "feature_date": pd.Timestamp("2025-01-02"),
            "symbol": "AAA",
            **{col: 1.0 for col in FEATURE_COLS_BASE},
        }
    ])
    result, _ = compute_universe_map(df, market_days_no_jan3)
    td = result["trading_date"].iloc[0]
    assert td == pd.Timestamp("2025-01-06"), f"Atteso 2025-01-06, ottenuto {td}"


# ---------------------------------------------------------------------------
# Test 4: tutti i rank sono in [0, 1]
# ---------------------------------------------------------------------------

def test_rank_pct_range():
    """Tutti i valori di rank_pct devono essere in [0, 1]."""
    df = make_df(symbols=("AAA", "BBB", "CCC"), feature_dates=pd.bdate_range("2025-01-02", periods=5))
    result, _ = compute_universe_map(df, MARKET_DAYS_EXTENDED)

    for col in RANK_COLS:
        rank_col = f"{col}_rank_pct"
        vals = result[rank_col].dropna()
        assert (vals >= 0).all() and (vals <= 1).all(), (
            f"{rank_col}: valori fuori [0,1]: {vals.min():.4f}–{vals.max():.4f}"
        )


# ---------------------------------------------------------------------------
# Test 5: distribuzione uniforme dei rank su N simboli
# ---------------------------------------------------------------------------

def test_rank_pct_sum():
    """
    Con N simboli per trading_date, i rank pct si distribuiscono uniformemente:
    la somma deve essere ~N/2 (media ~0.5) e devono essere tutti distinti.
    """
    symbols = ("AAA", "BBB", "CCC", "DDD")
    df = make_df(
        symbols=symbols,
        feature_dates=[pd.Timestamp("2025-01-02")],
    )
    result, _ = compute_universe_map(df, MARKET_DAYS_SIMPLE)

    for col in RANK_COLS:
        rank_col = f"{col}_rank_pct"
        vals = result.groupby("trading_date")[rank_col].apply(list)
        for td, ranks in vals.items():
            ranks_clean = [r for r in ranks if not math.isnan(r)]
            n = len(ranks_clean)
            if n < 2:
                continue
            # Con pandas rank(pct=True, method='average'), i valori sono distinti
            assert len(set(ranks_clean)) == n, (
                f"{rank_col} @ {td}: rank non distinti: {ranks_clean}"
            )
            # La media dei rank percentili su N simboli deve essere ~0.5
            mean_rank = sum(ranks_clean) / n
            assert abs(mean_rank - 0.5) < 0.1 + 1 / n, (
                f"{rank_col} @ {td}: media rank {mean_rank:.3f} lontana da 0.5"
            )


# ---------------------------------------------------------------------------
# Test 6: NaN preservati nel rank
# ---------------------------------------------------------------------------

def test_rank_nan_preserved():
    """
    Simbolo con NaN in rth_mom_20 → rth_mom_20_rank_pct deve essere NaN.
    """
    df = pd.DataFrame([
        {
            "feature_date": pd.Timestamp("2025-01-02"),
            "symbol": "AAA",
            **{col: 1.0 for col in FEATURE_COLS_BASE},
        },
        {
            "feature_date": pd.Timestamp("2025-01-02"),
            "symbol": "BBB",
            **{col: 1.0 for col in FEATURE_COLS_BASE},
            "rth_mom_20": np.nan,  # override: NaN
        },
    ])
    result, _ = compute_universe_map(df, MARKET_DAYS_SIMPLE)

    bbb_rank = result.loc[result["symbol"] == "BBB", "rth_mom_20_rank_pct"].iloc[0]
    assert pd.isna(bbb_rank), f"Atteso NaN per BBB, ottenuto {bbb_rank}"


# ---------------------------------------------------------------------------
# Test 7: colonne di output presenti
# ---------------------------------------------------------------------------

def test_output_columns():
    """trading_date e tutte le 6 colonne rank devono essere presenti nell'output."""
    df = make_df()
    result, _ = compute_universe_map(df, MARKET_DAYS_EXTENDED)

    assert "trading_date" in result.columns, "Manca colonna trading_date"
    for col in RANK_COLS:
        rank_col = f"{col}_rank_pct"
        assert rank_col in result.columns, f"Manca colonna {rank_col}"


# ---------------------------------------------------------------------------
# Test 8: trading_date sempre > feature_date
# ---------------------------------------------------------------------------

def test_trading_date_after_feature_date():
    """trading_date deve essere strettamente successivo a feature_date per ogni riga."""
    df = make_df(feature_dates=pd.bdate_range("2025-01-02", periods=5))
    result, _ = compute_universe_map(df, MARKET_DAYS_EXTENDED)

    feature_dates = pd.to_datetime(result["feature_date"])
    trading_dates = pd.to_datetime(result["trading_date"])
    mask = trading_dates > feature_dates
    assert mask.all(), (
        f"trading_date non sempre > feature_date:\n"
        f"{result[~mask][['symbol', 'feature_date', 'trading_date']]}"
    )


# ---------------------------------------------------------------------------
# Test 9: 2 simboli con valori diversi → rank diversi
# ---------------------------------------------------------------------------

def test_two_symbols_different_ranks():
    """
    Con 2 simboli con valori distinti, i rank devono essere diversi tra loro.
    """
    df = pd.DataFrame([
        {
            "feature_date": pd.Timestamp("2025-01-02"),
            "symbol": "AAA",
            **{col: 1.0 for col in FEATURE_COLS_BASE},
        },
        {
            "feature_date": pd.Timestamp("2025-01-02"),
            "symbol": "BBB",
            **{col: 2.0 for col in FEATURE_COLS_BASE},
        },
    ])
    result, _ = compute_universe_map(df, MARKET_DAYS_SIMPLE)

    for col in RANK_COLS:
        rank_col = f"{col}_rank_pct"
        vals = result[rank_col].tolist()
        assert vals[0] != vals[1], (
            f"{rank_col}: AAA e BBB hanno lo stesso rank {vals[0]}"
        )
