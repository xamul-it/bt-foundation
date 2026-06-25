#!/usr/bin/env python3
"""
Analyze partial results from sampled comprehensive test.
Can be run while the test is still running.
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path


def analyze(csv_path: Path):
    """Analyze partial results."""
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        print("Il test non ha ancora salvato risultati. Riprova tra qualche minuto.")
        return

    df = pd.read_csv(csv_path)

    if len(df) == 0:
        print("❌ Nessun risultato nel file")
        return

    print("\n" + "="*80)
    print(f"📊 ANALISI PARZIALE ({len(df)} test completati)")
    print("="*80)

    # Overall stats
    print(f"\n📈 Statistiche Generali:")
    print(f"  Test completati:    {len(df)}")
    print(f"  Simboli testati:    {df['symbol'].nunique()}")
    print(f"  Run ID:             {df['run_id'].iloc[0]}")
    print(f"\n  Media expectancy:   {df['expectancy'].mean()*100:+.3f}%")
    print(f"  Mediana expectancy: {df['expectancy'].median()*100:+.3f}%")
    print(f"  Std expectancy:     {df['expectancy'].std()*100:.3f}%")
    print(f"  Min expectancy:     {df['expectancy'].min()*100:+.3f}%")
    print(f"  Max expectancy:     {df['expectancy'].max()*100:+.3f}%")
    print(f"\n  % positivi:         {(df['expectancy'] > 0).mean():.1%}")
    print(f"  % sopra 0.1%:       {(df['expectancy'] >= 0.001).mean():.1%}")
    print(f"  % sopra 0.2%:       {(df['expectancy'] >= 0.002).mean():.1%}")

    # By symbol
    print(f"\n📊 Per Simbolo (ordinati per expectancy media):")
    by_symbol = df.groupby('symbol').agg({
        'expectancy': ['mean', 'std', 'min', 'max', 'count'],
        'p_tp': 'mean',
        'opportunity_rate': 'mean'
    }).round(4)

    by_symbol.columns = ['exp_mean', 'exp_std', 'exp_min', 'exp_max', 'n', 'p_tp_mean', 'opp_rate']
    by_symbol['exp_mean_pct'] = by_symbol['exp_mean'] * 100
    by_symbol['exp_min_pct'] = by_symbol['exp_min'] * 100
    by_symbol['exp_max_pct'] = by_symbol['exp_max'] * 100
    by_symbol = by_symbol.sort_values('exp_mean', ascending=False)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 120)

    print(by_symbol[['n', 'exp_mean_pct', 'exp_std', 'exp_min_pct', 'exp_max_pct', 'p_tp_mean', 'opp_rate']].to_string())

    # By window size
    print(f"\n📊 Per Window Size:")
    by_window = df.groupby('window_days').agg({
        'expectancy': ['mean', 'std', 'count'],
        'p_tp': 'mean',
        'opportunity_rate': 'mean'
    }).round(4)

    by_window.columns = ['exp_mean', 'exp_std', 'n', 'p_tp_mean', 'opp_rate']
    by_window['exp_mean_pct'] = by_window['exp_mean'] * 100

    print(by_window[['n', 'exp_mean_pct', 'exp_std', 'p_tp_mean', 'opp_rate']].to_string())

    # Distribution
    print(f"\n📊 Distribuzione Expectancy:")
    bins = [-1, -0.001, 0, 0.0005, 0.001, 0.0015, 0.002, 0.003, 1]
    labels = ['< -0.1%', '-0.1% a 0%', '0% a 0.05%', '0.05% a 0.1%', '0.1% a 0.15%', '0.15% a 0.2%', '0.2% a 0.3%', '> 0.3%']
    df['exp_bin'] = pd.cut(df['expectancy'], bins=bins, labels=labels)

    distribution = df['exp_bin'].value_counts().sort_index()
    for label, count in distribution.items():
        pct = count / len(df) * 100
        bar = '█' * int(pct / 2)
        print(f"  {label:20s}: {count:4d} ({pct:5.1f}%) {bar}")

    # Top 10 best
    print(f"\n🏆 Top 10 Migliori Finestre:")
    top10 = df.nlargest(10, 'expectancy')[['symbol', 'window_days', 'sample_idx', 'expectancy', 'p_tp', 'opportunity_rate', 'start_date', 'end_date']].copy()
    top10['exp_pct'] = top10['expectancy'] * 100
    top10['p_tp_pct'] = top10['p_tp'] * 100
    top10['opp_pct'] = top10['opportunity_rate'] * 100

    print(top10[['symbol', 'window_days', 'exp_pct', 'p_tp_pct', 'opp_pct', 'start_date', 'end_date']].to_string(index=False))

    # Worst 10
    print(f"\n💀 Top 10 Peggiori Finestre:")
    worst10 = df.nsmallest(10, 'expectancy')[['symbol', 'window_days', 'sample_idx', 'expectancy', 'p_tp', 'opportunity_rate', 'start_date', 'end_date']].copy()
    worst10['exp_pct'] = worst10['expectancy'] * 100
    worst10['p_tp_pct'] = worst10['p_tp'] * 100
    worst10['opp_pct'] = worst10['opportunity_rate'] * 100

    print(worst10[['symbol', 'window_days', 'exp_pct', 'p_tp_pct', 'opp_pct', 'start_date', 'end_date']].to_string(index=False))

    # Symbols above threshold
    above_threshold = by_symbol[by_symbol['exp_mean'] >= 0.001].copy()
    print(f"\n✅ Simboli con expectancy media >= 0.1% ({len(above_threshold)} simboli):")
    if len(above_threshold) > 0:
        print(above_threshold[['n', 'exp_mean_pct', 'exp_min_pct', 'exp_max_pct']].to_string())
    else:
        print("  Nessuno (per ora)")

    print("\n" + "="*80)


def main():
    p = argparse.ArgumentParser(description="Analyze partial results")
    p.add_argument("--file", default="sampled_results.csv", help="Results CSV file")
    args = p.parse_args()

    analyze(Path(args.file))


if __name__ == "__main__":
    main()
