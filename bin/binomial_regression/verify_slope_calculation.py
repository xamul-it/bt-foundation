#!/usr/bin/env python3
"""
Verifica critica: il calcolo dello slope è corretto?

Test logico:
- Se i prezzi salgono nel tempo (close[-1] > close[0]), lo slope DEVE essere POSITIVO
- Se i prezzi scendono nel tempo (close[-1] < close[0]), lo slope DEVE essere NEGATIVO

Questo script:
1. Carica i dati minute raw
2. Campiona 20 candele random
3. Per ogni candela, mostra le ultime 60 candele usate nella regressione
4. Calcola lo slope manualmente
5. Confronta con il risultato salvato in *_feature_outcome_full.csv
6. Verifica la coerenza logica
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
import random

# Config
ASSET = "AAPL"  # Cambia se vuoi testare altro
MINUTE_DATA_PATH = Path("../../config/data/m/alpaca") / f"{ASSET}.csv"
FEATURE_OUTCOME_PATH = Path("results") / f"{ASSET}_feature_outcome_full.csv"
WINDOWS = [5, 20, 60]
N_SAMPLES = 20

def manual_slope_linear(y_values: np.ndarray) -> dict:
    """Calcola slope manualmente usando la formula closed-form.

    IMPORTANTE: Usa la stessa formula di 01-fast_backtest.py regression_features_linear()
    """
    n = len(y_values)
    idx = np.arange(n, dtype=float)
    x_centered = idx - idx.mean()
    den = float(np.sum(x_centered**2))

    y_mean = float(np.mean(y_values))
    slope = float(np.sum((y_values - y_mean) * x_centered) / den) if den > 0 else np.nan

    # Verifica logica semplice: se i prezzi salgono, lo slope deve essere positivo
    first_price = y_values[0]
    last_price = y_values[-1]
    price_change = last_price - first_price
    expected_sign = np.sign(price_change) if price_change != 0 else 0
    actual_sign = np.sign(slope) if not np.isnan(slope) else 0

    return {
        "slope": slope,
        "first_price": first_price,
        "last_price": last_price,
        "price_change": price_change,
        "expected_sign": expected_sign,
        "actual_sign": actual_sign,
        "signs_match": expected_sign == actual_sign,
    }

def main():
    print("=" * 80)
    print("VERIFICA CALCOLO SLOPE - TEST CRITICO")
    print("=" * 80)
    print()

    # Carica dati
    if not MINUTE_DATA_PATH.exists():
        print(f"ERRORE: File minute non trovato: {MINUTE_DATA_PATH}")
        return

    df_min = pd.read_csv(MINUTE_DATA_PATH, parse_dates=["timestamp"])
    df_min = df_min.sort_values("timestamp").reset_index(drop=True)
    print(f"Caricati {len(df_min)} bar minute per {ASSET}")

    if not FEATURE_OUTCOME_PATH.exists():
        print(f"ERRORE: File feature outcome non trovato: {FEATURE_OUTCOME_PATH}")
        return

    df_feat = pd.read_csv(FEATURE_OUTCOME_PATH, parse_dates=["timestamp"])
    df_feat = df_feat.sort_values("timestamp").reset_index(drop=True)
    print(f"Caricati {len(df_feat)} feature outcome rows")
    print()

    # Merge per allineamento
    df_merged = df_min.merge(df_feat, on="timestamp", how="inner", suffixes=("_raw", "_feat"))
    print(f"Dopo merge: {len(df_merged)} righe allineate")
    print()

    # Campionamento random (escludi primi 60 bar per avere finestra completa)
    valid_indices = list(range(60, len(df_merged) - 10))
    if len(valid_indices) < N_SAMPLES:
        print("AVVISO: Non abbastanza dati per campionamento completo")
        sample_indices = valid_indices
    else:
        random.seed(42)
        sample_indices = sorted(random.sample(valid_indices, N_SAMPLES))

    print(f"Test su {len(sample_indices)} candele campionate\n")
    print("=" * 80)

    mismatches = []

    for test_num, i in enumerate(sample_indices, start=1):
        row = df_merged.iloc[i]
        ts = row["timestamp"]

        print(f"\n{'=' * 80}")
        print(f"TEST #{test_num} | Candela {i} | Timestamp: {ts}")
        print(f"{'=' * 80}")

        # Log prices - crea array completo fino alla candela corrente (inclusa)
        log_close = np.log(df_merged["close_raw"].iloc[:i+1].values)

        for window in WINDOWS:
            print(f"\n--- Finestra: {window} candele ---")

            # Estrai finestra - DEVE ESCLUDERE la candela corrente (come in 01-fast_backtest.py)
            # 01-fast_backtest.py usa: series.iloc[i - window:i]
            # Quindi prendiamo: log_close[i - window : i] (da log_close[0] fino a log_close[i+1])
            # Ma log_close ha i+1 elementi, quindi log_close[i-window:i] va da elemento i-window a i-1
            window_log_prices = log_close[i - window : i]
            assert len(window_log_prices) == window, f"Lunghezza finestra errata: {len(window_log_prices)}"

            # Calcolo manuale
            manual = manual_slope_linear(window_log_prices)

            # Valore salvato
            slope_col = f"slope_{window}"
            saved_slope = row[slope_col]

            # Confronto
            diff = abs(manual["slope"] - saved_slope)
            match = diff < 1e-10  # Tolleranza numerica

            print(f"  First log(close): {manual['first_price']:.8f}")
            print(f"  Last log(close):  {manual['last_price']:.8f}")
            print(f"  Change:           {manual['price_change']:+.8f}")
            print(f"  Expected sign:    {manual['expected_sign']:+.0f}")
            print(f"  Manual slope:     {manual['slope']:+.8f}")
            print(f"  Saved slope:      {saved_slope:+.8f}")
            print(f"  Actual sign:      {manual['actual_sign']:+.0f}")
            print(f"  Difference:       {diff:.2e}")
            print(f"  Match:            {'✅ OK' if match else '❌ FAIL'}")
            print(f"  Signs match:      {'✅ OK' if manual['signs_match'] else '❌ FAIL (LOGIC ERROR)'}")

            if not match:
                mismatches.append({
                    "test_num": test_num,
                    "index": i,
                    "timestamp": ts,
                    "window": window,
                    "manual_slope": manual["slope"],
                    "saved_slope": saved_slope,
                    "diff": diff,
                })

            if not manual["signs_match"]:
                print("  ⚠️  ATTENZIONE: Il segno dello slope NON corrisponde al movimento dei prezzi!")
                print(f"     Questo è un bug CRITICO nella formula di regressione!")

    print("\n" + "=" * 80)
    print("RIEPILOGO TEST")
    print("=" * 80)

    if mismatches:
        print(f"\n❌ TROVATI {len(mismatches)} MISMATCH tra calcolo manuale e salvato!")
        print("\nDettagli:")
        for m in mismatches:
            print(f"  Test #{m['test_num']}, idx={m['index']}, win={m['window']}: "
                  f"diff={m['diff']:.2e}")
    else:
        print("\n✅ Tutti i {len(sample_indices) * len(WINDOWS)} test PASSATI")
        print("   Il calcolo dello slope è NUMERICAMENTE CORRETTO")

    print("\nVERIFICA LOGICA (segno slope vs movimento prezzi):")
    print("  Controllare manualmente l'output sopra per eventuali '❌ FAIL (LOGIC ERROR)'")
    print()

if __name__ == "__main__":
    main()
