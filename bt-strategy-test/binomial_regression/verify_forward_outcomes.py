#!/usr/bin/env python3
"""
Verifica critica: i forward outcomes contengono look-ahead bias?

Test logico:
- high_ret_max_5m[i] deve usare SOLO high[i+1 : i+6]
- NON deve includere high[i] (la candela corrente)
- La finestra deve essere FUTURA rispetto alla barra di calcolo

Questo script:
1. Carica i dati minute raw
2. Campiona 20 candele random
3. Per ogni candela, mostra i valori high/low delle prossime 5/10/15 candele
4. Calcola high_ret_max_Nm manualmente
5. Confronta con il risultato salvato in *_feature_outcome_full.csv
6. Verifica che NON ci sia inclusione della barra corrente
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
import random

# Config
ASSET = "AAPL"
HERE = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
MINUTE_DATA_PATH = REPO_ROOT / "config-common/data/m/alpaca/sip" / f"{ASSET}.csv"
FEATURE_OUTCOME_PATH = HERE / "results" / f"{ASSET}_feature_outcome_full.csv"
HORIZONS = [5, 10, 15]
N_SAMPLES = 20

def manual_forward_outcomes(df: pd.DataFrame, i: int, horizon: int) -> dict:
    """Calcola forward outcomes manualmente.

    Regola CORRETTA:
    - Finestra futura: da i+1 a i+horizon (ESCLUSO i)
    - high_ret_max = max(high[i+1:i+horizon+1]) / close[i] - 1
    - low_ret_min = min(low[i+1:i+horizon+1]) / close[i] - 1
    - close_ret = close[i+horizon] / close[i] - 1
    """
    close_i = df.iloc[i]["close"]

    # Finestra futura: [i+1, i+horizon] (horizon candele DOPO la corrente)
    start = i + 1
    end = i + horizon + 1

    if end > len(df):
        return {
            "close_ret": np.nan,
            "high_ret_max": np.nan,
            "low_ret_min": np.nan,
            "window_start_idx": start,
            "window_end_idx": end,
            "window_size": 0,
            "out_of_bounds": True,
        }

    window = df.iloc[start:end]

    close_ret = df.iloc[i + horizon]["close"] / close_i - 1
    high_ret_max = window["high"].max() / close_i - 1
    low_ret_min = window["low"].min() / close_i - 1

    return {
        "close_ret": close_ret,
        "high_ret_max": high_ret_max,
        "low_ret_min": low_ret_min,
        "window_start_idx": start,
        "window_end_idx": end,
        "window_size": len(window),
        "out_of_bounds": False,
        "close_i": close_i,
        "close_end": df.iloc[i + horizon]["close"],
        "high_max": window["high"].max(),
        "low_min": window["low"].min(),
    }

def main():
    print("=" * 80)
    print("VERIFICA FORWARD OUTCOMES - TEST LOOK-AHEAD BIAS")
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

    # Merge
    df_merged = df_min.merge(df_feat, on="timestamp", how="inner", suffixes=("_raw", "_feat"))
    print(f"Dopo merge: {len(df_merged)} righe allineate")
    print()

    # Campionamento (escludi ultimi 20 bar per avere outcome completi)
    valid_indices = list(range(100, len(df_merged) - 20))
    if len(valid_indices) < N_SAMPLES:
        sample_indices = valid_indices
    else:
        random.seed(42)
        sample_indices = sorted(random.sample(valid_indices, N_SAMPLES))

    print(f"Test su {len(sample_indices)} candele campionate\n")
    print("=" * 80)

    mismatches = []
    lookahead_issues = []

    for test_num, i in enumerate(sample_indices, start=1):
        row = df_merged.iloc[i]
        ts = row["timestamp"]
        close_i = row["close_raw"]
        high_i = row["high_raw"]
        low_i = row["low_raw"]

        print(f"\n{'=' * 80}")
        print(f"TEST #{test_num} | Candela {i} | Timestamp: {ts}")
        print(f"{'=' * 80}")
        print(f"  Barra corrente [i={i}]:")
        print(f"    close = {close_i:.2f}")
        print(f"    high  = {high_i:.2f}")
        print(f"    low   = {low_i:.2f}")

        for h in HORIZONS:
            print(f"\n--- Orizzonte: {h} minuti ---")

            # Calcolo manuale
            manual = manual_forward_outcomes(df_merged, i, h)

            if manual["out_of_bounds"]:
                print(f"  ⚠️  Fuori dai bounds (end={manual['window_end_idx']} > len={len(df_merged)})")
                continue

            # Valori salvati
            close_ret_col = f"close_ret_{h}m"
            high_ret_col = f"high_ret_max_{h}m"
            low_ret_col = f"low_ret_min_{h}m"

            saved_close_ret = row[close_ret_col]
            saved_high_ret = row[high_ret_col]
            saved_low_ret = row[low_ret_col]

            print(f"  Finestra futura: [{manual['window_start_idx']}, {manual['window_end_idx']}) "
                  f"= candele {i+1} a {i+h}")
            print(f"  Numero candele nella finestra: {manual['window_size']}")
            print(f"  Close finale (i+{h}): {manual['close_end']:.2f}")
            print(f"  High max in finestra: {manual['high_max']:.2f}")
            print(f"  Low min in finestra:  {manual['low_min']:.2f}")
            print()

            # Confronto close_ret
            diff_close = abs(manual["close_ret"] - saved_close_ret)
            match_close = diff_close < 1e-8
            print(f"  close_ret_{h}m:")
            print(f"    Manual: {manual['close_ret']:+.8f}")
            print(f"    Saved:  {saved_close_ret:+.8f}")
            print(f"    Diff:   {diff_close:.2e}")
            print(f"    Match:  {'✅ OK' if match_close else '❌ FAIL'}")

            # Confronto high_ret_max
            diff_high = abs(manual["high_ret_max"] - saved_high_ret)
            match_high = diff_high < 1e-8
            print(f"  high_ret_max_{h}m:")
            print(f"    Manual: {manual['high_ret_max']:+.8f}")
            print(f"    Saved:  {saved_high_ret:+.8f}")
            print(f"    Diff:   {diff_high:.2e}")
            print(f"    Match:  {'✅ OK' if match_high else '❌ FAIL'}")

            # Confronto low_ret_min
            diff_low = abs(manual["low_ret_min"] - saved_low_ret)
            match_low = diff_low < 1e-8
            print(f"  low_ret_min_{h}m:")
            print(f"    Manual: {manual['low_ret_min']:+.8f}")
            print(f"    Saved:  {saved_low_ret:+.8f}")
            print(f"    Diff:   {diff_low:.2e}")
            print(f"    Match:  {'✅ OK' if match_low else '❌ FAIL'}")

            # Test look-ahead: verifica che high_max > high[i]
            # (se high_max == high[i], potrebbe esserci inclusione della barra corrente)
            if abs(manual["high_max"] - high_i) < 1e-6:
                print(f"  ⚠️  SOSPETTO LOOK-AHEAD: high_max ({manual['high_max']:.2f}) "
                      f"== high[i] ({high_i:.2f})")
                lookahead_issues.append({
                    "test_num": test_num,
                    "index": i,
                    "horizon": h,
                    "reason": "high_max == high[i]",
                })

            if abs(manual["low_min"] - low_i) < 1e-6:
                print(f"  ⚠️  SOSPETTO LOOK-AHEAD: low_min ({manual['low_min']:.2f}) "
                      f"== low[i] ({low_i:.2f})")
                lookahead_issues.append({
                    "test_num": test_num,
                    "index": i,
                    "horizon": h,
                    "reason": "low_min == low[i]",
                })

            if not (match_close and match_high and match_low):
                mismatches.append({
                    "test_num": test_num,
                    "index": i,
                    "timestamp": ts,
                    "horizon": h,
                    "diff_close": diff_close,
                    "diff_high": diff_high,
                    "diff_low": diff_low,
                })

    print("\n" + "=" * 80)
    print("RIEPILOGO TEST")
    print("=" * 80)

    if mismatches:
        print(f"\n❌ TROVATI {len(mismatches)} MISMATCH tra calcolo manuale e salvato!")
        print("\nDettagli:")
        for m in mismatches:
            print(f"  Test #{m['test_num']}, idx={m['index']}, h={m['horizon']}: "
                  f"diff_close={m['diff_close']:.2e}, "
                  f"diff_high={m['diff_high']:.2e}, "
                  f"diff_low={m['diff_low']:.2e}")
    else:
        print(f"\n✅ Tutti i {len(sample_indices) * len(HORIZONS) * 3} test PASSATI")
        print("   Il calcolo dei forward outcomes è NUMERICAMENTE CORRETTO")

    if lookahead_issues:
        print(f"\n⚠️  TROVATI {len(lookahead_issues)} POSSIBILI LOOK-AHEAD BIAS!")
        print("\nQuesto può essere normale se:")
        print("  - La barra corrente ha il max/min che si ripete nelle barre future")
        print("  - Ma se succede spesso, potrebbe essere un problema di allineamento")
        print("\nDettagli:")
        for la in lookahead_issues:
            print(f"  Test #{la['test_num']}, idx={la['index']}, h={la['horizon']}: {la['reason']}")
    else:
        print("\n✅ Nessun sospetto look-ahead bias rilevato")

    print()

if __name__ == "__main__":
    main()
