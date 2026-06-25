#!/usr/bin/env python3
"""
Analyze HMA backtest results
"""

import pandas as pd
import sys
from pathlib import Path

def analyze(csv_path: str):
    """Analyze HMA backtest results."""
    df = pd.read_csv(csv_path)

    print("="*80)
    print(f"HMA BACKTEST ANALYSIS: {csv_path}")
    print("="*80)
    print(f"Total trades: {len(df):,}")
    print(f"Symbols tested: {df['symbol'].nunique()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print()

    # Outcome distribution
    print("OUTCOME DISTRIBUTION")
    print("-"*80)
    outcome_counts = df['outcome'].value_counts()
    for outcome, count in outcome_counts.items():
        pct = count / len(df) * 100
        print(f"  {outcome.upper():8s}: {count:5,} ({pct:5.1f}%)")
    print()

    # Returns by outcome
    print("RETURNS BY OUTCOME")
    print("-"*80)
    outcome_stats = df.groupby('outcome')['exit_pct'].agg(['count', 'mean', 'median', 'std', 'min', 'max'])
    print(outcome_stats.to_string())
    print()

    # Overall stats
    print("OVERALL STATISTICS")
    print("-"*80)
    print(f"Mean return:       {df['exit_pct'].mean():+.3f}%")
    print(f"Median return:     {df['exit_pct'].median():+.3f}%")
    print(f"Std dev:           {df['exit_pct'].std():.3f}%")
    print(f"Min return:        {df['exit_pct'].min():+.3f}%")
    print(f"Max return:        {df['exit_pct'].max():+.3f}%")
    print()

    # Expectancy calculation
    wins = df[df['exit_pct'] > 0]
    losses = df[df['exit_pct'] < 0]

    if len(wins) > 0 and len(losses) > 0:
        p_win = len(wins) / len(df)
        p_loss = len(losses) / len(df)
        avg_win = wins['exit_pct'].mean()
        avg_loss = abs(losses['exit_pct'].mean())
        expectancy = p_win * avg_win - p_loss * avg_loss

        print("EXPECTANCY BREAKDOWN")
        print("-"*80)
        print(f"P(win):            {p_win:6.1%}")
        print(f"P(loss):           {p_loss:6.1%}")
        print(f"Avg win:           {avg_win:+.3f}%")
        print(f"Avg loss:          {avg_loss:+.3f}%")
        print(f"Win/Loss ratio:    {avg_win/avg_loss:.2f}")
        print()
        print(f"Expectancy:        {expectancy:+.3f}%")
        print(f"  P(win) × avg_win:  {p_win:.1%} × {avg_win:.3f}% = {p_win * avg_win:+.3f}%")
        print(f"  P(loss) × avg_loss: {p_loss:.1%} × {avg_loss:.3f}% = {p_loss * avg_loss:+.3f}%")
        print()

        if expectancy >= 0.1:
            print("✅ PASSES minimum expectancy threshold (0.1%)")
        else:
            print(f"❌ FAILS minimum expectancy threshold (0.1% required, got {expectancy:.3f}%)")
    print()

    # Top/bottom symbols
    print("PERFORMANCE BY SYMBOL")
    print("-"*80)
    symbol_stats = df.groupby('symbol').agg({
        'exit_pct': ['count', 'mean', 'sum'],
    }).round(3)
    symbol_stats.columns = ['trades', 'avg_return%', 'total_return%']
    symbol_stats = symbol_stats.sort_values('avg_return%', ascending=False)

    print("Top 10 symbols:")
    print(symbol_stats.head(10).to_string())
    print()
    print("Bottom 10 symbols:")
    print(symbol_stats.tail(10).to_string())
    print()

    # Long vs Short
    if 'side' in df.columns:
        print("LONG VS SHORT")
        print("-"*80)
        side_stats = df.groupby('side').agg({
            'exit_pct': ['count', 'mean', 'median'],
        }).round(3)
        side_stats.columns = ['trades', 'avg_return%', 'median_return%']
        print(side_stats.to_string())
        print()

    # Bars held distribution
    if 'bars_held' in df.columns:
        print("BARS HELD DISTRIBUTION")
        print("-"*80)
        print(f"Mean bars held:    {df['bars_held'].mean():.1f}")
        print(f"Median bars held:  {df['bars_held'].median():.1f}")
        print(f"Min bars held:     {df['bars_held'].min()}")
        print(f"Max bars held:     {df['bars_held'].max()}")
        print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python 02-analyze_hma.py <results.csv>")
        sys.exit(1)

    analyze(sys.argv[1])
