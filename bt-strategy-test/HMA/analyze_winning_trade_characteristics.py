#!/usr/bin/env python3
"""
Analyze Winning Trade Characteristics

Analizza correlazione tra metriche dei simboli e risultati dei trade.
Obiettivo: Capire quali caratteristiche predicono trade vincenti.

Author: Claude Code
Date: 2026-02-13
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import json
from scipy import stats

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_PATH = Path('config/data/m/alpaca')
TRADES_FILE = Path('bt-strategy-test/HMA/limit_order_analysis/limit_close.csv')
OUTPUT_PATH = Path('bt-strategy-test/HMA/trade_characteristics_analysis')
OUTPUT_PATH.mkdir(exist_ok=True)

# Lookback periods for metrics (days before entry)
LOOKBACKS = [7, 14, 30, 60]

# ============================================================================
# FUNCTIONS
# ============================================================================

def load_symbol_data(symbol):
    """Load minute data for a symbol"""
    filepath = DATA_PATH / f'{symbol}.csv'
    if not filepath.exists():
        return None

    try:
        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df.set_index('timestamp')
        return df
    except Exception as e:
        print(f"Error loading {symbol}: {e}")
        return None

def calculate_daily_metrics(minute_data):
    """Calculate daily OHLCV and ATR from minute data"""
    # Resample to daily
    daily = minute_data.resample('D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    # Calculate True Range
    tr1 = daily['high'] - daily['low']
    tr2 = abs(daily['high'] - daily['close'].shift(1))
    tr3 = abs(daily['low'] - daily['close'].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Calculate ATR (14-period)
    daily['atr'] = tr.rolling(14).mean()
    daily['atr_pct'] = daily['atr'] / daily['close']

    # Calculate returns
    daily['returns'] = daily['close'].pct_change()

    return daily

def calculate_intraday_metrics(minute_data, lookback_minutes=60):
    """Calculate intraday metrics (e.g., last hour volatility)"""
    if len(minute_data) < lookback_minutes:
        return {}

    recent = minute_data.iloc[-lookback_minutes:]

    metrics = {
        'intraday_range_pct': (recent['high'].max() - recent['low'].min()) / recent['close'].iloc[0],
        'intraday_volume_surge': recent['volume'].mean() / minute_data['volume'].mean() if minute_data['volume'].mean() > 0 else 1,
        'intraday_volatility': recent['close'].pct_change().std() if len(recent) > 1 else 0,
    }

    return metrics

def calculate_trade_metrics(symbol, entry_time, lookback_days=30):
    """Calculate metrics at trade entry time"""
    # Load symbol data
    minute_data = load_symbol_data(symbol)
    if minute_data is None:
        return None

    entry_time = pd.Timestamp(entry_time)

    # Get data up to entry time
    pre_entry_data = minute_data[minute_data.index < entry_time]
    if len(pre_entry_data) < 100:
        return None

    # Calculate daily metrics
    daily = calculate_daily_metrics(pre_entry_data)

    if len(daily) == 0:
        return None

    metrics = {
        'symbol': symbol,
        'entry_time': entry_time
    }

    # Calculate for each lookback period
    for lookback in LOOKBACKS:
        end_date = entry_time
        start_date = end_date - timedelta(days=lookback)

        period_daily = daily[(daily.index >= start_date) & (daily.index <= end_date)]
        period_minute = pre_entry_data[(pre_entry_data.index >= start_date) & (pre_entry_data.index <= end_date)]

        if len(period_daily) < 5:  # Minimum requirement
            continue

        # ATR metrics
        metrics[f'atr_mean_{lookback}d'] = period_daily['atr'].mean()
        metrics[f'atr_pct_mean_{lookback}d'] = period_daily['atr_pct'].mean()
        metrics[f'atr_pct_median_{lookback}d'] = period_daily['atr_pct'].median()
        metrics[f'atr_pct_std_{lookback}d'] = period_daily['atr_pct'].std()
        metrics[f'atr_pct_last_{lookback}d'] = period_daily['atr_pct'].iloc[-1] if len(period_daily) > 0 else np.nan

        # Trend of ATR (increasing or decreasing?)
        if len(period_daily) >= 10:
            x = np.arange(len(period_daily))
            y = period_daily['atr_pct'].values
            slope, _, _, _, _ = stats.linregress(x, y)
            metrics[f'atr_trend_{lookback}d'] = slope

        # Frequency of high ATR days
        high_atr_days = (period_daily['atr_pct'] >= 0.007).sum()
        metrics[f'pct_days_atr_high_{lookback}d'] = high_atr_days / len(period_daily)

        # Volatility (std of returns)
        metrics[f'returns_std_{lookback}d'] = period_daily['returns'].std()
        metrics[f'returns_mean_{lookback}d'] = period_daily['returns'].mean()

        # Price trend
        if len(period_daily) >= 5:
            price_change = (period_daily['close'].iloc[-1] / period_daily['close'].iloc[0]) - 1
            metrics[f'price_change_{lookback}d'] = price_change

        # Volume metrics
        metrics[f'volume_mean_{lookback}d'] = period_daily['volume'].mean()
        metrics[f'volume_std_{lookback}d'] = period_daily['volume'].std()

        # Recent volume surge
        if lookback >= 14 and len(period_daily) >= 14:
            recent_volume = period_daily['volume'].iloc[-7:].mean()
            older_volume = period_daily['volume'].iloc[-14:-7].mean()
            metrics[f'volume_surge_{lookback}d'] = recent_volume / older_volume if older_volume > 0 else 1

        # Trading days
        metrics[f'trading_days_{lookback}d'] = len(period_daily)

    # Intraday metrics (last hour before entry)
    intraday = calculate_intraday_metrics(pre_entry_data, lookback_minutes=60)
    metrics.update(intraday)

    # Entry-time specific metrics
    if len(pre_entry_data) > 0:
        metrics['entry_price'] = pre_entry_data['close'].iloc[-1]
        metrics['entry_volume'] = pre_entry_data['volume'].iloc[-1]

        # Distance from recent high/low
        if len(pre_entry_data) >= 60:
            recent_60 = pre_entry_data.iloc[-60:]
            metrics['dist_from_high_60min'] = (recent_60['high'].max() - metrics['entry_price']) / metrics['entry_price']
            metrics['dist_from_low_60min'] = (metrics['entry_price'] - recent_60['low'].min()) / metrics['entry_price']

    return metrics

def load_monte_carlo_trades():
    """Load Monte Carlo trade results"""
    trades = pd.read_csv(TRADES_FILE)
    trades['entry_time'] = pd.to_datetime(trades['entry_time'], utc=True)
    trades['exit_time'] = pd.to_datetime(trades['exit_time'], utc=True)

    # Add win/loss flag
    trades['is_winner'] = trades['pnl_pct'] > 0
    trades['is_big_winner'] = trades['pnl_pct'] > 0.01  # > 1%
    trades['is_loser'] = trades['pnl_pct'] < 0

    return trades

def analyze_all_trades(trades):
    """Calculate metrics for all trades"""
    print("=" * 80)
    print("ANALYZING TRADE CHARACTERISTICS")
    print("=" * 80)
    print(f"\nTotal Trades: {len(trades)}")
    print(f"Winners: {trades['is_winner'].sum()} ({trades['is_winner'].mean():.1%})")
    print(f"Losers: {trades['is_loser'].sum()} ({trades['is_loser'].mean():.1%})")
    print(f"Big Winners (>1%): {trades['is_big_winner'].sum()} ({trades['is_big_winner'].mean():.1%})")

    all_metrics = []
    errors = []

    for idx, trade in trades.iterrows():
        if (idx + 1) % 20 == 0:
            print(f"Processing... {idx + 1}/{len(trades)}")

        metrics = calculate_trade_metrics(
            trade['symbol'],
            trade['entry_time'],
            lookback_days=60
        )

        if metrics is None:
            errors.append(idx)
            continue

        # Add trade outcome
        metrics['pnl_pct'] = trade['pnl_pct']
        metrics['is_winner'] = trade['is_winner']
        metrics['is_big_winner'] = trade['is_big_winner']
        metrics['is_loser'] = trade['is_loser']
        metrics['side'] = trade['side']
        metrics['exit_reason'] = trade['exit_reason']
        metrics['bars_held'] = trade['bars_held']

        all_metrics.append(metrics)

    print(f"\nSuccessfully analyzed: {len(all_metrics)} trades")
    print(f"Errors: {len(errors)} trades")

    return pd.DataFrame(all_metrics)

def compare_winners_vs_losers(df):
    """Compare characteristics of winning vs losing trades"""
    print("\n" + "=" * 80)
    print("WINNERS vs LOSERS COMPARISON")
    print("=" * 80)

    winners = df[df['is_winner'] == True]
    losers = df[df['is_loser'] == True]

    print(f"\nWinners: {len(winners)} ({len(winners)/len(df):.1%})")
    print(f"Losers: {len(losers)} ({len(losers)/len(df):.1%})")

    # Metrics to compare
    metric_cols = [col for col in df.columns if any([
        col.startswith('atr_'),
        col.startswith('returns_'),
        col.startswith('volume_'),
        col.startswith('price_change_'),
        col.startswith('pct_days_'),
        col.startswith('intraday_'),
        col.startswith('dist_from_')
    ])]

    comparison = []
    for metric in metric_cols:
        if metric not in df.columns:
            continue

        winners_vals = winners[metric].dropna()
        losers_vals = losers[metric].dropna()

        if len(winners_vals) == 0 or len(losers_vals) == 0:
            continue

        # Statistical test
        t_stat, p_value = stats.ttest_ind(winners_vals, losers_vals, equal_var=False)

        comparison.append({
            'metric': metric,
            'winners_mean': winners_vals.mean(),
            'winners_median': winners_vals.median(),
            'losers_mean': losers_vals.mean(),
            'losers_median': losers_vals.median(),
            'diff_mean': winners_vals.mean() - losers_vals.mean(),
            'diff_pct': ((winners_vals.mean() - losers_vals.mean()) / losers_vals.mean() * 100) if losers_vals.mean() != 0 else np.nan,
            't_stat': t_stat,
            'p_value': p_value,
            'significant': p_value < 0.05
        })

    comparison_df = pd.DataFrame(comparison)
    comparison_df = comparison_df.sort_values('p_value')

    print("\nTop 20 Most Significant Differences (p < 0.05):")
    significant = comparison_df[comparison_df['significant'] == True].head(20)

    if len(significant) > 0:
        print(significant[['metric', 'winners_mean', 'losers_mean', 'diff_pct', 'p_value']].to_string(index=False))
    else:
        print("No statistically significant differences found!")

    return comparison_df

def calculate_correlations(df):
    """Calculate correlations between metrics and PnL"""
    print("\n" + "=" * 80)
    print("CORRELATION ANALYSIS (Metrics vs PnL)")
    print("=" * 80)

    # Get metric columns
    metric_cols = [col for col in df.columns if any([
        col.startswith('atr_'),
        col.startswith('returns_'),
        col.startswith('volume_'),
        col.startswith('price_change_'),
        col.startswith('pct_days_'),
        col.startswith('intraday_'),
        col.startswith('dist_from_')
    ])]

    correlations = []
    for metric in metric_cols:
        if metric not in df.columns:
            continue

        valid_data = df[[metric, 'pnl_pct']].dropna()
        if len(valid_data) < 10:
            continue

        corr, p_value = stats.pearsonr(valid_data[metric], valid_data['pnl_pct'])

        correlations.append({
            'metric': metric,
            'correlation': corr,
            'abs_correlation': abs(corr),
            'p_value': p_value,
            'significant': p_value < 0.05,
            'n_samples': len(valid_data)
        })

    corr_df = pd.DataFrame(correlations)
    corr_df = corr_df.sort_values('abs_correlation', ascending=False)

    print("\nTop 20 Strongest Correlations (sorted by |correlation|):")
    top_corr = corr_df.head(20)
    print(top_corr[['metric', 'correlation', 'p_value', 'significant']].to_string(index=False))

    print("\nPositive Correlations (higher metric → higher PnL):")
    positive = corr_df[(corr_df['correlation'] > 0) & (corr_df['significant'] == True)].head(10)
    if len(positive) > 0:
        print(positive[['metric', 'correlation', 'p_value']].to_string(index=False))
    else:
        print("None found")

    print("\nNegative Correlations (higher metric → lower PnL):")
    negative = corr_df[(corr_df['correlation'] < 0) & (corr_df['significant'] == True)].head(10)
    if len(negative) > 0:
        print(negative[['metric', 'correlation', 'p_value']].to_string(index=False))
    else:
        print("None found")

    return corr_df

def analyze_big_winners(df):
    """Analyze characteristics of big winners (>1% gain)"""
    print("\n" + "=" * 80)
    print("BIG WINNERS (>1% gain) ANALYSIS")
    print("=" * 80)

    big_winners = df[df['is_big_winner'] == True]
    others = df[df['is_big_winner'] == False]

    print(f"\nBig Winners: {len(big_winners)} ({len(big_winners)/len(df):.1%})")
    print(f"Others: {len(others)}")

    if len(big_winners) < 5:
        print("Too few big winners for meaningful analysis")
        return None

    # Compare characteristics
    metric_cols = [col for col in df.columns if any([
        col.startswith('atr_'),
        col.startswith('returns_'),
        col.startswith('pct_days_'),
        col.startswith('intraday_')
    ])]

    comparison = []
    for metric in metric_cols:
        if metric not in df.columns:
            continue

        bw_vals = big_winners[metric].dropna()
        oth_vals = others[metric].dropna()

        if len(bw_vals) == 0 or len(oth_vals) == 0:
            continue

        t_stat, p_value = stats.ttest_ind(bw_vals, oth_vals, equal_var=False)

        comparison.append({
            'metric': metric,
            'big_winners_mean': bw_vals.mean(),
            'others_mean': oth_vals.mean(),
            'diff_pct': ((bw_vals.mean() - oth_vals.mean()) / oth_vals.mean() * 100) if oth_vals.mean() != 0 else np.nan,
            'p_value': p_value,
            'significant': p_value < 0.05
        })

    comparison_df = pd.DataFrame(comparison)
    comparison_df = comparison_df.sort_values('p_value')

    print("\nTop 10 Characteristics of Big Winners (p < 0.05):")
    significant = comparison_df[comparison_df['significant'] == True].head(10)

    if len(significant) > 0:
        print(significant[['metric', 'big_winners_mean', 'others_mean', 'diff_pct', 'p_value']].to_string(index=False))
    else:
        print("No statistically significant differences found!")

    return comparison_df

def create_filter_recommendations(df, comparison_df, corr_df):
    """Create filter recommendations based on analysis"""
    print("\n" + "=" * 80)
    print("FILTER RECOMMENDATIONS")
    print("=" * 80)

    print("\nBased on statistical analysis, consider adding these filters:")

    # Find metrics that significantly predict winners
    significant_predictors = comparison_df[
        (comparison_df['significant'] == True) &
        (comparison_df['diff_pct'] > 10)  # Winners have >10% higher value
    ].head(5)

    if len(significant_predictors) > 0:
        print("\n1. ENTRY FILTERS (Metrics where winners score higher):")
        for _, row in significant_predictors.iterrows():
            threshold = row['losers_mean'] + (row['winners_mean'] - row['losers_mean']) * 0.5
            print(f"   {row['metric']} >= {threshold:.4f}")
            print(f"      Winners avg: {row['winners_mean']:.4f}, Losers avg: {row['losers_mean']:.4f}")
            print(f"      Difference: +{row['diff_pct']:.1f}%, p={row['p_value']:.4f}")
            print()

    # Find metrics that predict losers (to avoid)
    avoid_conditions = comparison_df[
        (comparison_df['significant'] == True) &
        (comparison_df['diff_pct'] < -10)  # Losers have >10% higher value
    ].head(5)

    if len(avoid_conditions) > 0:
        print("\n2. AVOID CONDITIONS (Metrics where losers score higher):")
        for _, row in avoid_conditions.iterrows():
            threshold = row['winners_mean'] + (row['losers_mean'] - row['winners_mean']) * 0.5
            print(f"   {row['metric']} < {threshold:.4f} (avoid if higher)")
            print(f"      Winners avg: {row['winners_mean']:.4f}, Losers avg: {row['losers_mean']:.4f}")
            print(f"      Difference: {row['diff_pct']:.1f}%, p={row['p_value']:.4f}")
            print()

def main():
    """Main analysis"""
    # Load Monte Carlo trades
    trades = load_monte_carlo_trades()

    # Calculate metrics for all trades
    df = analyze_all_trades(trades)

    # Save raw data
    output_file = OUTPUT_PATH / 'trade_metrics.csv'
    df.to_csv(output_file, index=False)
    print(f"\n✅ Saved trade metrics to: {output_file}")

    # Compare winners vs losers
    comparison = compare_winners_vs_losers(df)
    comparison_file = OUTPUT_PATH / 'winners_vs_losers.csv'
    comparison.to_csv(comparison_file, index=False)
    print(f"✅ Saved comparison to: {comparison_file}")

    # Calculate correlations
    correlations = calculate_correlations(df)
    corr_file = OUTPUT_PATH / 'correlations.csv'
    correlations.to_csv(corr_file, index=False)
    print(f"✅ Saved correlations to: {corr_file}")

    # Analyze big winners
    big_winners_analysis = analyze_big_winners(df)
    if big_winners_analysis is not None:
        bw_file = OUTPUT_PATH / 'big_winners_analysis.csv'
        big_winners_analysis.to_csv(bw_file, index=False)
        print(f"✅ Saved big winners analysis to: {bw_file}")

    # Create recommendations
    create_filter_recommendations(df, comparison, correlations)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nOutput files in: {OUTPUT_PATH}/")
    print("  - trade_metrics.csv: Metrics for each of 182 trades")
    print("  - winners_vs_losers.csv: Statistical comparison")
    print("  - correlations.csv: Correlation with PnL")
    print("  - big_winners_analysis.csv: Big winners characteristics")

if __name__ == '__main__':
    main()
