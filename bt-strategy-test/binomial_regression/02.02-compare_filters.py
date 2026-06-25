#!/usr/bin/env python3
"""Confronta diversi filtri per vedere quale funziona meglio."""

import pandas as pd
import numpy as np
from pathlib import Path

# Carica dati completi
df = pd.read_csv("results/AAPL_feature_outcome_full.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

print("="*80)
print("CONFRONTO FILTRI PER ENTRY")
print("="*80)
print(f"\nDataset: {len(df)} candele totali")
print()

# Definisci outcome target
outcome = "high_ret_max_5m"
threshold_bps = 10.0  # 10 bps = 0.1%
threshold = threshold_bps / 10000.0

# Calcola success per outcome
df["success"] = (df[outcome] >= threshold).astype(int)

# Crea indicatori
df["ind_trend_alignment"] = (
    (np.sign(df["slope_5"]) == np.sign(df["slope_20"])) &
    (np.sign(df["slope_20"]) == np.sign(df["slope_60"])) &
    (np.sign(df["slope_5"]) != 0)
).astype(int)

df["slope_5_abs"] = df["slope_5"].abs()
df["slope_20_abs"] = df["slope_20"].abs()
df["slope_60_abs"] = df["slope_60"].abs()

# Filtri da testare
filters = {
    "Baseline (nessun filtro)": df["slope_5"].notna(),  # Tutti

    # Magnitudine slope
    "abs(slope_60) > p50": df["slope_60_abs"] > df["slope_60_abs"].quantile(0.5),
    "abs(slope_60) > p75": df["slope_60_abs"] > df["slope_60_abs"].quantile(0.75),
    "abs(slope_60) > p90": df["slope_60_abs"] > df["slope_60_abs"].quantile(0.90),

    # Trend alignment
    "Trend alignment": df["ind_trend_alignment"] == 1,

    # Combinazioni
    "Alignment + abs(slope_60)>p75": (
        (df["ind_trend_alignment"] == 1) &
        (df["slope_60_abs"] > df["slope_60_abs"].quantile(0.75))
    ),

    "Alignment + abs(slope_60)>p50": (
        (df["ind_trend_alignment"] == 1) &
        (df["slope_60_abs"] > df["slope_60_abs"].quantile(0.5))
    ),

    # Triple magnitudine
    "abs(slope_5)>p75 + abs(slope_20)>p75 + abs(slope_60)>p75": (
        (df["slope_5_abs"] > df["slope_5_abs"].quantile(0.75)) &
        (df["slope_20_abs"] > df["slope_20_abs"].quantile(0.75)) &
        (df["slope_60_abs"] > df["slope_60_abs"].quantile(0.75))
    ),

    # VoZ filter
    "volZ > 0.85": df["volZ"] > 0.85,

    # Combinazione best (ipotesi)
    "abs(slope_60)>p75 + volZ>0.85": (
        (df["slope_60_abs"] > df["slope_60_abs"].quantile(0.75)) &
        (df["volZ"] > 0.85)
    ),

    "Alignment + abs(slope_60)>p75 + volZ>0.85": (
        (df["ind_trend_alignment"] == 1) &
        (df["slope_60_abs"] > df["slope_60_abs"].quantile(0.75)) &
        (df["volZ"] > 0.85)
    ),
}

# Valuta ogni filtro
results = []
for filter_name, mask in filters.items():
    filtered = df[mask & df[outcome].notna()]
    n = len(filtered)
    if n == 0:
        continue

    pct_kept = 100 * n / len(df)
    mean_outcome = filtered[outcome].mean()
    p_success = filtered["success"].mean()

    # Baseline per confronto
    baseline_outcome = df[df[outcome].notna()][outcome].mean()
    baseline_success = df[df[outcome].notna()]["success"].mean()

    lift_outcome = mean_outcome / baseline_outcome if baseline_outcome != 0 else np.nan
    lift_success = p_success / baseline_success if baseline_success != 0 else np.nan

    results.append({
        "Filter": filter_name,
        "N candele": n,
        "% kept": pct_kept,
        "Mean outcome": mean_outcome,
        "P(success)": p_success,
        "Lift outcome": lift_outcome,
        "Lift success": lift_success,
    })

res_df = pd.DataFrame(results)

# Ordina per Lift success
res_df = res_df.sort_values("Lift success", ascending=False)

print("\n" + "="*80)
print(f"TARGET: {outcome} >= {threshold_bps} bps")
print("="*80)
print()
print(res_df.to_string(index=False))
print()

# Salva
res_df.to_csv("results/filter_comparison.csv", index=False)
print(f"✅ Risultati salvati in: results/filter_comparison.csv")
print()

# Top 3
print("="*80)
print("TOP 3 FILTRI (per Lift success)")
print("="*80)
for i, (_, row) in enumerate(res_df.head(3).iterrows(), 1):
    print(f"\n#{i}: {row['Filter']}")
    print(f"    N candele:    {int(row['N candele']):,} ({row['% kept']:.1f}% del totale)")
    print(f"    Mean outcome: {row['Mean outcome']*100:.3f}%")
    print(f"    P(success):   {row['P(success)']*100:.1f}%")
    print(f"    Lift success: {row['Lift success']:.3f}x")

print()
