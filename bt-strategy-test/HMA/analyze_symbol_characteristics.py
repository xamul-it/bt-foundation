#!/usr/bin/env python3
"""
Analyze Symbol Characteristics for Pre-Filter Selection

Calcola caratteristiche dei simboli nel periodo PRE-validazione per creare
un filtro sistematico di selezione simboli ad alta volatilità.

Author: Claude Code
Date: 2026-02-13
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import json

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_PATH = Path('config/data/m/alpaca')
OUTPUT_PATH = Path('bt-strategy-test/HMA/symbol_analysis')
OUTPUT_PATH.mkdir(exist_ok=True)

# Periodi di analisi
TRAINING_START = '2025-02-06'  # 90 giorni prima della validazione
TRAINING_END = '2025-05-05'    # Giorno prima della validazione
VALIDATION_START = '2025-05-06'
VALIDATION_END = '2025-07-03'

# Lookback periods (giorni)
LOOKBACKS = [30, 60, 120]

# Simboli Monte Carlo (20 che hanno tradato)
MONTE_CARLO_SYMBOLS = [
    'AAPL', 'AMAT', 'AMD', 'BIDU', 'BKR', 'CRWD', 'DDOG', 'DOCU',
    'FTNT', 'LCID', 'MDB', 'MSTR', 'OKTA', 'PTON', 'RIVN',
    'ROKU', 'SMCI', 'SNOW', 'TSLA', 'ZM'
]

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

def calculate_daily_atr(minute_data, period=14):
    """Calculate daily ATR from minute data"""
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

    # Calculate ATR
    atr = tr.rolling(period).mean()
    atr_pct = atr / daily['close']

    return daily, atr, atr_pct

def calculate_symbol_metrics(symbol, training_data):
    """Calculate all metrics for a symbol in training period"""
    if training_data is None or len(training_data) == 0:
        return None

    # Calculate daily data and ATR
    daily, atr, atr_pct = calculate_daily_atr(training_data)

    if daily is None or len(daily) == 0:
        return None

    # Calculate returns
    daily['returns'] = daily['close'].pct_change()

    metrics = {'symbol': symbol}

    # For each lookback period
    for lookback in LOOKBACKS:
        end_date = pd.Timestamp(TRAINING_END, tz='UTC')
        start_date = end_date - timedelta(days=lookback)

        period_daily = daily[(daily.index >= start_date) & (daily.index <= end_date)]
        period_atr = atr[(atr.index >= start_date) & (atr.index <= end_date)]
        period_atr_pct = atr_pct[(atr_pct.index >= start_date) & (atr_pct.index <= end_date)]

        if len(period_daily) < 10:  # Minimum data requirement
            continue

        # ATR metrics
        metrics[f'atr_mean_{lookback}d'] = period_atr.mean()
        metrics[f'atr_pct_mean_{lookback}d'] = period_atr_pct.mean()
        metrics[f'atr_pct_median_{lookback}d'] = period_atr_pct.median()
        metrics[f'atr_pct_p75_{lookback}d'] = period_atr_pct.quantile(0.75)
        metrics[f'atr_pct_max_{lookback}d'] = period_atr_pct.max()

        # Frequency of high ATR days
        high_atr_days = (period_atr_pct >= 0.007).sum()  # >= 0.7%
        metrics[f'pct_days_atr_high_{lookback}d'] = high_atr_days / len(period_atr_pct) if len(period_atr_pct) > 0 else 0

        # Volatility (std of returns)
        metrics[f'returns_std_{lookback}d'] = period_daily['returns'].std()
        metrics[f'returns_std_ann_{lookback}d'] = period_daily['returns'].std() * np.sqrt(252)

        # Price level
        metrics[f'price_mean_{lookback}d'] = period_daily['close'].mean()
        metrics[f'price_std_{lookback}d'] = period_daily['close'].std()

        # Volume metrics
        metrics[f'volume_mean_{lookback}d'] = period_daily['volume'].mean()
        metrics[f'dollar_volume_mean_{lookback}d'] = (period_daily['close'] * period_daily['volume']).mean()

        # Trading days
        metrics[f'trading_days_{lookback}d'] = len(period_daily)

    return metrics

def analyze_all_symbols():
    """Analyze all available symbols"""
    print("=" * 80)
    print("SYMBOL CHARACTERISTICS ANALYSIS")
    print("=" * 80)
    print(f"\nTraining Period: {TRAINING_START} to {TRAINING_END}")
    print(f"Validation Period: {VALIDATION_START} to {VALIDATION_END}")
    print(f"Lookback Periods: {LOOKBACKS} days")
    print(f"\nMonte Carlo Symbols ({len(MONTE_CARLO_SYMBOLS)}): {', '.join(MONTE_CARLO_SYMBOLS)}")

    # Get all available symbols
    all_symbols = sorted([f.stem for f in DATA_PATH.glob('*.csv')])
    print(f"\nTotal Symbols Available: {len(all_symbols)}")

    # Calculate metrics for all symbols
    all_metrics = []
    errors = []

    for i, symbol in enumerate(all_symbols, 1):
        if i % 20 == 0:
            print(f"Processing... {i}/{len(all_symbols)}")

        # Load training data
        df = load_symbol_data(symbol)
        if df is None:
            errors.append(symbol)
            continue

        training_data = df[
            (df.index >= TRAINING_START) &
            (df.index <= TRAINING_END)
        ]

        if len(training_data) < 100:  # Minimum bars requirement
            errors.append(symbol)
            continue

        metrics = calculate_symbol_metrics(symbol, training_data)
        if metrics:
            # Add flag for Monte Carlo symbols
            metrics['in_monte_carlo'] = symbol in MONTE_CARLO_SYMBOLS
            all_metrics.append(metrics)
        else:
            errors.append(symbol)

    print(f"\nSuccessfully analyzed: {len(all_metrics)} symbols")
    print(f"Errors/Insufficient data: {len(errors)} symbols")

    return pd.DataFrame(all_metrics)

def find_optimal_thresholds(df):
    """Find thresholds that best identify Monte Carlo symbols"""
    print("\n" + "=" * 80)
    print("OPTIMAL THRESHOLD ANALYSIS")
    print("=" * 80)

    mc_symbols = df[df['in_monte_carlo'] == True]
    other_symbols = df[df['in_monte_carlo'] == False]

    print(f"\nMonte Carlo Symbols: {len(mc_symbols)}")
    print(f"Other Symbols: {len(other_symbols)}")

    results = []

    # Test different metrics and thresholds
    for lookback in LOOKBACKS:
        metric_col = f'atr_pct_mean_{lookback}d'
        if metric_col not in df.columns:
            continue

        # Get percentile thresholds from Monte Carlo symbols
        for percentile in [25, 50, 75]:
            threshold = mc_symbols[metric_col].quantile(percentile / 100)

            # Count how many symbols pass
            mc_pass = (mc_symbols[metric_col] >= threshold).sum()
            other_pass = (other_symbols[metric_col] >= threshold).sum()
            total_pass = mc_pass + other_pass

            # Calculate recall and precision
            recall = mc_pass / len(mc_symbols) if len(mc_symbols) > 0 else 0
            precision = mc_pass / total_pass if total_pass > 0 else 0

            results.append({
                'metric': metric_col,
                'threshold': threshold,
                'percentile': percentile,
                'mc_pass': mc_pass,
                'mc_total': len(mc_symbols),
                'other_pass': other_pass,
                'total_pass': total_pass,
                'recall': recall,
                'precision': precision,
                'f1': 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            })

    results_df = pd.DataFrame(results).sort_values('f1', ascending=False)

    print("\nTop 10 Threshold Configurations by F1 Score:")
    print(results_df.head(10).to_string(index=False))

    return results_df

def compare_monte_carlo_vs_others(df):
    """Compare characteristics of Monte Carlo symbols vs others"""
    print("\n" + "=" * 80)
    print("MONTE CARLO vs OTHER SYMBOLS COMPARISON")
    print("=" * 80)

    mc_symbols = df[df['in_monte_carlo'] == True]
    other_symbols = df[df['in_monte_carlo'] == False]

    # Key metrics to compare
    comparison_metrics = []
    for lookback in LOOKBACKS:
        comparison_metrics.extend([
            f'atr_pct_mean_{lookback}d',
            f'atr_pct_median_{lookback}d',
            f'pct_days_atr_high_{lookback}d',
            f'returns_std_ann_{lookback}d'
        ])

    comparison = []
    for metric in comparison_metrics:
        if metric not in df.columns:
            continue

        comparison.append({
            'metric': metric,
            'mc_mean': mc_symbols[metric].mean(),
            'mc_median': mc_symbols[metric].median(),
            'other_mean': other_symbols[metric].mean(),
            'other_median': other_symbols[metric].median(),
            'ratio': mc_symbols[metric].mean() / other_symbols[metric].mean() if other_symbols[metric].mean() > 0 else np.nan
        })

    comparison_df = pd.DataFrame(comparison)
    comparison_df['diff_pct'] = (comparison_df['mc_mean'] - comparison_df['other_mean']) / comparison_df['other_mean'] * 100

    print("\nKey Metrics Comparison:")
    print(comparison_df.to_string(index=False))

    return comparison_df

def create_filter_recommendation(df, threshold_results):
    """Create recommended filter based on analysis"""
    print("\n" + "=" * 80)
    print("RECOMMENDED SYMBOL FILTER")
    print("=" * 80)

    # Get best performing threshold
    best = threshold_results.iloc[0]

    print(f"\nBest Single Metric Filter:")
    print(f"  Metric: {best['metric']}")
    print(f"  Threshold: {best['threshold']:.4f} ({best['threshold']*100:.2f}%)")
    print(f"  Recall: {best['recall']:.1%} ({best['mc_pass']}/{best['mc_total']} MC symbols)")
    print(f"  Precision: {best['precision']:.1%}")
    print(f"  F1 Score: {best['f1']:.3f}")
    print(f"  Total Symbols Selected: {best['total_pass']}")

    # Test combined filter
    print(f"\nTesting Combined Filters:")

    # Multi-criteria filter
    for lookback in [30, 60]:
        atr_col = f'atr_pct_mean_{lookback}d'
        freq_col = f'pct_days_atr_high_{lookback}d'

        if atr_col not in df.columns or freq_col not in df.columns:
            continue

        # Use median of MC symbols as threshold
        mc_symbols = df[df['in_monte_carlo'] == True]
        atr_threshold = mc_symbols[atr_col].quantile(0.50)
        freq_threshold = mc_symbols[freq_col].quantile(0.50)

        # Apply filter
        passed = df[
            (df[atr_col] >= atr_threshold) &
            (df[freq_col] >= freq_threshold)
        ]

        mc_pass = passed['in_monte_carlo'].sum()
        total_pass = len(passed)

        recall = mc_pass / len(mc_symbols)
        precision = mc_pass / total_pass if total_pass > 0 else 0

        print(f"\n  Filter: {atr_col} >= {atr_threshold:.4f} AND {freq_col} >= {freq_threshold:.2%}")
        print(f"    Recall: {recall:.1%} ({mc_pass}/{len(mc_symbols)})")
        print(f"    Precision: {precision:.1%}")
        print(f"    Total Selected: {total_pass}")
        print(f"    Symbols: {', '.join(passed['symbol'].tolist()[:10])}{'...' if total_pass > 10 else ''}")

def main():
    """Main analysis"""
    # Analyze all symbols
    df = analyze_all_symbols()

    # Save raw metrics
    output_file = OUTPUT_PATH / 'symbol_metrics.csv'
    df.to_csv(output_file, index=False)
    print(f"\n✅ Saved metrics to: {output_file}")

    # Find optimal thresholds
    threshold_results = find_optimal_thresholds(df)

    # Save threshold analysis
    threshold_file = OUTPUT_PATH / 'threshold_analysis.csv'
    threshold_results.to_csv(threshold_file, index=False)
    print(f"✅ Saved threshold analysis to: {threshold_file}")

    # Compare MC vs others
    comparison = compare_monte_carlo_vs_others(df)

    # Save comparison
    comparison_file = OUTPUT_PATH / 'mc_vs_others_comparison.csv'
    comparison.to_csv(comparison_file, index=False)
    print(f"✅ Saved comparison to: {comparison_file}")

    # Create filter recommendation
    create_filter_recommendation(df, threshold_results)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nOutput files in: {OUTPUT_PATH}/")
    print("  - symbol_metrics.csv: All calculated metrics")
    print("  - threshold_analysis.csv: Optimal thresholds")
    print("  - mc_vs_others_comparison.csv: MC vs Other symbols")

if __name__ == '__main__':
    main()
