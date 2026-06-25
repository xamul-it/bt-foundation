#!/usr/bin/env python3
"""
Calcola metriche complete (incluso P(hit SL)) per filtro combinato magnitudine + qualità.
"""

import argparse
from pathlib import Path
import re
import pandas as pd
import numpy as np

def parse_args():
    p = argparse.ArgumentParser(description="Analizza metriche complete per filtri combinati")
    p.add_argument("--tp-pct", type=float, default=0.005, help="Take profit (default: 0.005 = 0.5%%)")
    p.add_argument("--sl-pct", type=float, default=0.003, help="Stop loss (default: 0.003 = 0.3%%)")
    p.add_argument("--input-glob", default="results/*_feature_outcome_full.csv")
    return p.parse_args()

def load_full_data(input_glob: str) -> pd.DataFrame:
    here = Path(__file__).resolve().parent
    files = sorted(here.glob(input_glob))
    if not files:
        raise FileNotFoundError(f"Nessun file trovato con glob: {input_glob}")

    parts = []
    for fp in files:
        m = re.match(r"^([^_]+)_feature_outcome_full\.csv$", fp.name)
        asset = m.group(1) if m else "UNK"
        df = pd.read_csv(fp)
        df["asset"] = asset
        parts.append(df)

    out = pd.concat(parts, ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    return out

def calculate_full_metrics(df: pd.DataFrame, tp_pct: float, sl_pct: float) -> dict:
    """Calcola metriche complete per orizzonti 5m, 10m, 15m."""
    results = []

    for horizon in [5, 10, 15]:
        high_col = f"high_ret_max_{horizon}m"
        low_col = f"low_ret_min_{horizon}m"
        close_col = f"close_ret_{horizon}m"

        high = pd.to_numeric(df[high_col], errors="coerce")
        low = pd.to_numeric(df[low_col], errors="coerce")
        close = pd.to_numeric(df[close_col], errors="coerce")

        valid = high.notna() & low.notna() & close.notna()
        n = valid.sum()

        if n == 0:
            continue

        # Usa sistema a priorità: prima TP, poi SL, poi neither
        # (una candela può toccare entrambi, ma classifichiamo come TP se tocca TP)
        hit_tp_mask = (high[valid] >= tp_pct)
        hit_sl_mask = (low[valid] <= -sl_pct) & ~hit_tp_mask  # SL solo se NON ha toccato TP
        hit_neither_mask = ~hit_tp_mask & ~hit_sl_mask

        hit_tp = hit_tp_mask.sum()
        hit_sl = hit_sl_mask.sum()
        hit_neither = hit_neither_mask.sum()

        results.append({
            "horizon_m": horizon,
            "n": int(n),
            "n_hit_tp": int(hit_tp),
            "n_hit_sl": int(hit_sl),
            "n_hit_neither": int(hit_neither),
            "p_hit_tp": hit_tp / n,
            "p_hit_sl": hit_sl / n,
            "p_hit_neither": hit_neither / n,
            "mean_high": high[valid].mean(),
            "median_high": high[valid].median(),
            "mean_low": low[valid].mean(),
            "median_low": low[valid].median(),
            "mean_close": close[valid].mean(),
            "median_close": close[valid].median(),
        })

    return pd.DataFrame(results)

def main():
    args = parse_args()

    print("Caricamento dati...")
    df = load_full_data(args.input_glob)
    print(f"  Caricati {len(df)} righe totali")

    # Parametri
    tp_pct = args.tp_pct
    sl_pct = args.sl_pct

    print(f"\nParametri TP/SL:")
    print(f"  TP = {tp_pct:.3%}")
    print(f"  SL = {sl_pct:.3%}")
    print(f"  R:R = {tp_pct/sl_pct:.2f}")

    # === BASELINE ===
    print("\n" + "="*80)
    print("BASELINE (nessun filtro)")
    print("="*80)
    metrics_baseline = calculate_full_metrics(df, tp_pct, sl_pct)
    print(metrics_baseline.to_string(index=False))

    # === SETUP 1: Triple Magnitudine ===
    print("\n" + "="*80)
    print("SETUP 1: Triple Magnitudine (p75)")
    print("="*80)
    s5_abs = pd.to_numeric(df["slope_5"], errors="coerce").abs()
    s20_abs = pd.to_numeric(df["slope_20"], errors="coerce").abs()
    s60_abs = pd.to_numeric(df["slope_60"], errors="coerce").abs()

    p5 = s5_abs.quantile(0.75)
    p20 = s20_abs.quantile(0.75)
    p60 = s60_abs.quantile(0.75)

    mag_mask = (s5_abs > p5) & (s20_abs > p20) & (s60_abs > p60)
    df_mag = df[mag_mask].copy()
    print(f"Candele filtrate: {len(df_mag)} ({100*len(df_mag)/len(df):.2f}%)")

    metrics_mag = calculate_full_metrics(df_mag, tp_pct, sl_pct)
    print(metrics_mag.to_string(index=False))

    # === SETUP 2: Magnitudine + Dispersione ===
    print("\n" + "="*80)
    print("SETUP 2: Magnitudine + Dispersione (p75)")
    print("="*80)

    # Calcola soglia dispersione sul dataset filtrato per magnitudine
    disp_60 = pd.to_numeric(df_mag["disp_60"], errors="coerce")
    disp_threshold = disp_60.quantile(0.75)

    quality_mask = disp_60 > disp_threshold
    df_mag_quality = df_mag[quality_mask].copy()
    print(f"Candele filtrate: {len(df_mag_quality)} ({100*len(df_mag_quality)/len(df):.2f}% del totale, {100*len(df_mag_quality)/len(df_mag):.2f}% del mag)")

    metrics_mag_quality = calculate_full_metrics(df_mag_quality, tp_pct, sl_pct)
    print(metrics_mag_quality.to_string(index=False))

    # === CONFRONTO ===
    print("\n" + "="*80)
    print("CONFRONTO @ 5 MINUTI")
    print("="*80)

    baseline_5m = metrics_baseline[metrics_baseline["horizon_m"] == 5].iloc[0]
    mag_5m = metrics_mag[metrics_mag["horizon_m"] == 5].iloc[0]
    mag_qual_5m = metrics_mag_quality[metrics_mag_quality["horizon_m"] == 5].iloc[0]

    print(f"\n{'Metrica':<20} {'Baseline':>12} {'Mag (Setup1)':>14} {'Mag+Disp (Setup2)':>18}")
    print("-" * 70)
    print(f"{'N candele':<20} {baseline_5m['n']:>12,} {mag_5m['n']:>14,} {mag_qual_5m['n']:>18,}")
    print(f"{'% del totale':<20} {100:>11.1f}% {100*mag_5m['n']/baseline_5m['n']:>13.1f}% {100*mag_qual_5m['n']/baseline_5m['n']:>17.1f}%")
    print()
    print(f"{'P(hit TP)':<20} {baseline_5m['p_hit_tp']:>11.1%} {mag_5m['p_hit_tp']:>13.1%} {mag_qual_5m['p_hit_tp']:>17.1%}")
    print(f"{'P(hit SL)':<20} {baseline_5m['p_hit_sl']:>11.1%} {mag_5m['p_hit_sl']:>13.1%} {mag_qual_5m['p_hit_sl']:>17.1%}")
    print(f"{'P(neither)':<20} {baseline_5m['p_hit_neither']:>11.1%} {mag_5m['p_hit_neither']:>13.1%} {mag_qual_5m['p_hit_neither']:>17.1%}")
    print()
    print(f"{'Mean high':<20} {baseline_5m['mean_high']:>11.3%} {mag_5m['mean_high']:>13.3%} {mag_qual_5m['mean_high']:>17.3%}")
    print(f"{'Mean low':<20} {baseline_5m['mean_low']:>11.3%} {mag_5m['mean_low']:>13.3%} {mag_qual_5m['mean_low']:>17.3%}")
    print()

    # Calcola expectancy
    def calc_expectancy(row, tp, sl):
        return row['p_hit_tp'] * tp + row['p_hit_sl'] * (-sl) + row['p_hit_neither'] * row['mean_close']

    exp_baseline = calc_expectancy(baseline_5m, tp_pct, sl_pct)
    exp_mag = calc_expectancy(mag_5m, tp_pct, sl_pct)
    exp_mag_qual = calc_expectancy(mag_qual_5m, tp_pct, sl_pct)

    print(f"{'Expectancy/trade':<20} {exp_baseline:>11.3%} {exp_mag:>13.3%} {exp_mag_qual:>17.3%}")
    print()

    # Lift
    print(f"{'Lift P(TP)':<20} {1.0:>11.2f}x {mag_5m['p_hit_tp']/baseline_5m['p_hit_tp']:>13.2f}x {mag_qual_5m['p_hit_tp']/baseline_5m['p_hit_tp']:>17.2f}x")
    print(f"{'Lift Expectancy':<20} {1.0:>11.2f}x {exp_mag/exp_baseline if exp_baseline != 0 else float('nan'):>13.2f}x {exp_mag_qual/exp_baseline if exp_baseline != 0 else float('nan'):>17.2f}x")

    # Salva CSV
    out_dir = Path(__file__).resolve().parent / "results" / "combined_filter_analysis"
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics_baseline.to_csv(out_dir / "baseline_full_metrics.csv", index=False)
    metrics_mag.to_csv(out_dir / "magnitude_full_metrics.csv", index=False)
    metrics_mag_quality.to_csv(out_dir / "magnitude_quality_full_metrics.csv", index=False)

    print(f"\n✅ Output salvato in: {out_dir}")

if __name__ == "__main__":
    main()
