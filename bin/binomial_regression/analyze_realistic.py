#!/usr/bin/env python3
"""Analyze realistic intraday test results."""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path


def analyze(csv_path: Path):
    """Analyze realistic test results."""
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)

    if len(df) == 0:
        print("❌ Nessun risultato nel file")
        return

    print("\n" + "="*80)
    print(f"📊 ANALISI REALISTIC INTRADAY ({len(df)} test completati)")
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

    # Exit breakdown
    print(f"\n📊 Breakdown Exit Types (media per test):")
    print(f"  TP hit:             {df['p_tp'].mean():.1%}")
    print(f"  SL hit:             {df['p_sl'].mean():.1%}")
    print(f"  Hold exit (5 bar):  {df['p_hold'].mean():.1%}")
    print(f"  EOD exit:           {df['p_eod'].mean():.1%}")

    # Mean exit pct for non-TP/SL
    print(f"\n📊 Mean Exit % (per tipo):")
    print(f"  Hold exit (5 bar):  {df['mean_hold_exit'].mean()*100:+.3f}%")
    print(f"  EOD exit:           {df['mean_eod_exit'].mean()*100:+.3f}%")

    # Trades per window
    print(f"\n📊 Trades per Window:")
    print(f"  Media trades:       {df['trades_total'].mean():.1f}")
    print(f"  Mediana trades:     {df['trades_total'].median():.0f}")
    print(f"  Min/Max trades:     {df['trades_total'].min():.0f} / {df['trades_total'].max():.0f}")

    # By symbol
    print(f"\n📊 Per Simbolo (ordinati per expectancy media):")
    by_symbol = df.groupby('symbol').agg({
        'expectancy': ['mean', 'std', 'min', 'max', 'count'],
        'p_tp': 'mean',
        'trades_total': 'mean',
        'opportunity_rate': 'mean'
    }).round(4)

    by_symbol.columns = ['exp_mean', 'exp_std', 'exp_min', 'exp_max', 'n', 'p_tp_mean', 'trades_mean', 'opp_rate']
    by_symbol['exp_mean_pct'] = by_symbol['exp_mean'] * 100
    by_symbol['exp_min_pct'] = by_symbol['exp_min'] * 100
    by_symbol['exp_max_pct'] = by_symbol['exp_max'] * 100
    by_symbol = by_symbol.sort_values('exp_mean', ascending=False)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 150)

    print(by_symbol[['n', 'exp_mean_pct', 'exp_std', 'exp_min_pct', 'exp_max_pct', 'p_tp_mean', 'trades_mean']].to_string())

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
    top10 = df.nlargest(10, 'expectancy')[['symbol', 'window_days', 'expectancy', 'p_tp', 'trades_total', 'start_date', 'end_date']].copy()
    top10['exp_pct'] = top10['expectancy'] * 100
    top10['p_tp_pct'] = top10['p_tp'] * 100

    print(top10[['symbol', 'window_days', 'exp_pct', 'p_tp_pct', 'trades_total', 'start_date', 'end_date']].to_string(index=False))

    # Worst 10
    print(f"\n💀 Top 10 Peggiori Finestre:")
    worst10 = df.nsmallest(10, 'expectancy')[['symbol', 'window_days', 'expectancy', 'p_tp', 'trades_total', 'start_date', 'end_date']].copy()
    worst10['exp_pct'] = worst10['expectancy'] * 100
    worst10['p_tp_pct'] = worst10['p_tp'] * 100

    print(worst10[['symbol', 'window_days', 'exp_pct', 'p_tp_pct', 'trades_total', 'start_date', 'end_date']].to_string(index=False))

    # Symbols above threshold
    above_threshold = by_symbol[by_symbol['exp_mean'] >= 0.001].copy()
    print(f"\n✅ Simboli con expectancy media >= 0.1% ({len(above_threshold)} simboli):")
    if len(above_threshold) > 0:
        print(above_threshold[['n', 'exp_mean_pct', 'exp_min_pct', 'exp_max_pct', 'trades_mean']].to_string())
    else:
        print("  Nessuno")

    print("\n" + "="*80)


def main():
    p = argparse.ArgumentParser(description="Analyze realistic results")
    p.add_argument("--file", default="realistic_results.csv", help="Results CSV file")
    args = p.parse_args()

    analyze(Path(args.file))


if __name__ == "__main__":
    main()
