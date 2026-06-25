#!/usr/bin/env python3
"""Analyze ORB backtest results."""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path


def analyze(csv_path: Path):
    """Analyze ORB results."""
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)

    if len(df) == 0:
        print("❌ No results")
        return

    print("\n" + "="*80)
    print(f"📊 ORB STRATEGY ANALYSIS ({len(df)} trades)")
    print("="*80)

    # Overall stats
    print(f"\n📈 Overall Performance:")
    print(f"  Total trades:       {len(df)}")
    print(f"  Unique symbols:     {df['symbol'].nunique()}")
    print(f"  Date range:         {df['date'].min()} to {df['date'].max()}")

    print(f"\n  Mean return:        {df['exit_pct'].mean():+.3f}%")
    print(f"  Median return:      {df['exit_pct'].median():+.3f}%")
    print(f"  Std return:         {df['exit_pct'].std():.3f}%")
    print(f"  Min/Max return:     {df['exit_pct'].min():+.2f}% / {df['exit_pct'].max():+.2f}%")

    # Win/Loss stats
    print(f"\n📊 Outcome Breakdown:")
    outcomes = df['outcome'].value_counts()
    for outcome, count in outcomes.items():
        pct = count / len(df) * 100
        emoji = "✅" if outcome == 'tp' else ("❌" if outcome == 'sl' else "⏰")
        print(f"  {emoji} {outcome.upper():4s}: {count:4d} ({pct:5.1f}%)")

    # Returns by outcome
    print(f"\n💰 Mean Return by Outcome:")
    for outcome in ['tp', 'sl', 'eod']:
        if outcome in df['outcome'].values:
            mean_ret = df[df['outcome'] == outcome]['exit_pct'].mean()
            print(f"  {outcome.upper():4s}: {mean_ret:+.3f}%")

    # Win/Loss ratio
    wins = df[df['exit_pct'] > 0]
    losses = df[df['exit_pct'] < 0]
    if len(losses) > 0:
        wl_ratio = wins['exit_pct'].mean() / abs(losses['exit_pct'].mean())
        print(f"\n📊 Win/Loss Ratio: {wl_ratio:.2f}")

    # Expectancy
    p_win = len(wins) / len(df)
    p_loss = len(losses) / len(df)
    avg_win = wins['exit_pct'].mean() if len(wins) > 0 else 0
    avg_loss = losses['exit_pct'].mean() if len(losses) > 0 else 0
    expectancy = p_win * avg_win + p_loss * avg_loss

    print(f"\n💎 Expectancy:")
    print(f"  P(Win):             {p_win:.1%}")
    print(f"  Avg Win:            {avg_win:+.3f}%")
    print(f"  P(Loss):            {p_loss:.1%}")
    print(f"  Avg Loss:           {avg_loss:+.3f}%")
    print(f"  Expectancy:         {expectancy:+.3f}%")

    # By symbol
    print(f"\n📊 By Symbol (top 10):")
    by_symbol = df.groupby('symbol').agg({
        'exit_pct': ['mean', 'std', 'count']
    }).round(3)
    by_symbol.columns = ['mean_ret', 'std_ret', 'n_trades']
    by_symbol = by_symbol.sort_values('mean_ret', ascending=False)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 120)
    print(by_symbol.head(10).to_string())

    # By side
    print(f"\n📊 By Side:")
    by_side = df.groupby('side').agg({
        'exit_pct': ['mean', 'count']
    }).round(3)
    by_side.columns = ['mean_ret', 'n_trades']
    print(by_side.to_string())

    # OR size analysis
    print(f"\n📊 Opening Range Size:")
    print(f"  Mean OR size:       {df['or_size_pct'].mean():.3f}%")
    print(f"  Median OR size:     {df['or_size_pct'].median():.3f}%")
    print(f"  Min/Max OR size:    {df['or_size_pct'].min():.2f}% / {df['or_size_pct'].max():.2f}%")

    # Correlation OR size vs return
    corr = df['or_size_pct'].corr(df['exit_pct'])
    print(f"  Corr(OR size, return): {corr:+.3f}")

    # Distribution
    print(f"\n📊 Return Distribution:")
    bins = [-100, -2, -1, 0, 1, 2, 3, 5, 100]
    labels = ['< -2%', '-2% to -1%', '-1% to 0%', '0% to 1%', '1% to 2%', '2% to 3%', '3% to 5%', '> 5%']
    df['ret_bin'] = pd.cut(df['exit_pct'], bins=bins, labels=labels)

    distribution = df['ret_bin'].value_counts().sort_index()
    for label, count in distribution.items():
        pct = count / len(df) * 100
        bar = '█' * int(pct / 2)
        print(f"  {label:15s}: {count:4d} ({pct:5.1f}%) {bar}")

    # Best/Worst trades
    print(f"\n🏆 Top 10 Best Trades:")
    top10 = df.nlargest(10, 'exit_pct')[['symbol', 'date', 'side', 'outcome', 'exit_pct', 'or_size_pct']]
    print(top10.to_string(index=False))

    print(f"\n💀 Top 10 Worst Trades:")
    worst10 = df.nsmallest(10, 'exit_pct')[['symbol', 'date', 'side', 'outcome', 'exit_pct', 'or_size_pct']]
    print(worst10.to_string(index=False))

    print("\n" + "="*80)


def main():
    p = argparse.ArgumentParser(description="Analyze ORB results")
    p.add_argument("--file", default="orb_results.csv", help="Results CSV file")
    args = p.parse_args()

    analyze(Path(args.file))


if __name__ == "__main__":
    main()
