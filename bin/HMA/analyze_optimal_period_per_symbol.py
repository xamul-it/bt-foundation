#!/usr/bin/env python3
"""
Analyze Optimal HMA Period Per Symbol

Testa se period=16 è ottimale per tutti i simboli o se simboli diversi
preferiscono periodi diversi (come "frequenze di risonanza").

Author: Claude Code
Date: 2026-02-13
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import json

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_PATH = Path('config/data/m/alpaca')
TRADES_PATH = Path('bin/HMA/limit_order_analysis/limit_close.csv')
OUTPUT_PATH = Path('bin/HMA/period_optimization_analysis')
OUTPUT_PATH.mkdir(exist_ok=True)

# Periodi da testare
TEST_PERIODS = [8, 10, 12, 14, 16, 18, 20, 24, 32]

# Validation period (Monte Carlo)
VALIDATION_START = '2025-05-06'
VALIDATION_END = '2025-07-03'

# Training period per ottimizzazione adattiva (30 giorni prima)
TRAINING_DAYS = 30

# ============================================================================
# HMA CALCULATION
# ============================================================================

def calculate_wma(series, period):
    """Calculate Weighted Moving Average"""
    weights = np.arange(1, period + 1)

    def wma_at_point(values):
        if len(values) < period:
            return np.nan
        return np.dot(values[-period:], weights) / weights.sum()

    result = pd.Series(index=series.index, dtype=float)
    for i in range(len(series)):
        result.iloc[i] = wma_at_point(series.iloc[:i+1].values)

    return result

def calculate_hma(close, period, inverted=True):
    """
    Calculate Hull Moving Average

    HMA = WMA(2 * WMA(n/2) - WMA(n), sqrt(n))

    Inverted mode: Reverses direction for contrarian signals
    """
    half_period = period // 2
    sqrt_period = int(np.sqrt(period))

    # Calculate components
    wma_half = calculate_wma(close, half_period)
    wma_full = calculate_wma(close, period)

    # Raw HMA
    raw_hma = 2 * wma_half - wma_full

    # Smooth with WMA
    hma = calculate_wma(raw_hma, sqrt_period)

    # Invert if needed (contrarian mode)
    if inverted:
        # Invert the direction changes
        # In inverted mode, we want opposite signals
        # This is done in signal detection, not here
        pass

    return hma

def detect_hma_signals(hma, inverted=True):
    """
    Detect HMA direction changes (peaks and troughs)

    In inverted mode:
    - Peak (was rising, now falling) → LONG signal
    - Trough (was falling, now rising) → SHORT signal
    """
    # Calculate direction: +1 rising, -1 falling
    direction = np.sign(hma.diff())

    # Detect changes
    direction_change = direction.diff()

    signals = pd.Series(index=hma.index, dtype=object)

    for i in range(2, len(hma)):
        if pd.isna(direction_change.iloc[i]):
            continue

        # Peak: was rising (+1), now falling (-1) → change = -2
        if direction_change.iloc[i] == -2:
            if inverted:
                signals.iloc[i] = 'LONG'  # Contrarian: fade the peak
            else:
                signals.iloc[i] = 'SHORT'

        # Trough: was falling (-1), now rising (+1) → change = +2
        elif direction_change.iloc[i] == 2:
            if inverted:
                signals.iloc[i] = 'SHORT'  # Contrarian: fade the trough
            else:
                signals.iloc[i] = 'LONG'

    return signals

# ============================================================================
# ATR CALCULATION
# ============================================================================

def calculate_atr_sma(high, low, close, period=14):
    """Calculate ATR using Simple Moving Average (matching Monte Carlo)"""
    # True Range
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # ATR = SMA of TR
    atr = tr.rolling(period).mean()
    atr_pct = atr / close

    return atr, atr_pct

# ============================================================================
# BACKTESTING ENGINE
# ============================================================================

def backtest_symbol_period(symbol, period, start_date, end_date, min_atr_pct=0.007):
    """
    Backtest HMA strategy on a symbol with given period

    Returns: dict with performance metrics
    """
    # Load data
    filepath = DATA_PATH / f'{symbol}.csv'
    if not filepath.exists():
        return None

    try:
        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df.set_index('timestamp')
        df = df[(df.index >= start_date) & (df.index <= end_date)]

        if len(df) < 100:
            return None

    except Exception as e:
        print(f"Error loading {symbol}: {e}")
        return None

    # Calculate indicators
    hma = calculate_hma(df['close'], period, inverted=True)
    signals = detect_hma_signals(hma, inverted=True)
    atr, atr_pct = calculate_atr_sma(df['high'], df['low'], df['close'], period=14)

    # Filter signals by ATR and time window
    df['hma'] = hma
    df['signal'] = signals
    df['atr_pct'] = atr_pct

    # Time window filter: 15:00-20:00 UTC (10:00-15:00 EST)
    df['hour'] = df.index.hour
    df['minute'] = df.index.minute
    df['time_minutes'] = df['hour'] * 60 + df['minute']

    trading_start = 15 * 60  # 15:00 UTC
    trading_end = 20 * 60    # 20:00 UTC

    valid_signals = df[
        (df['signal'].notna()) &
        (df['atr_pct'] >= min_atr_pct) &
        (df['time_minutes'] >= trading_start) &
        (df['time_minutes'] < trading_end)
    ].copy()

    if len(valid_signals) == 0:
        return {
            'symbol': symbol,
            'period': period,
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'expectancy': 0
        }

    # Simulate trades
    trades = []
    for idx, row in valid_signals.iterrows():
        entry_price = row['close']
        entry_signal = row['signal']

        # Find exit (next opposite signal or EOD or SL)
        future_data = df[df.index > idx].copy()

        if len(future_data) == 0:
            continue

        # Stop loss
        sl_pct = 0.005  # 0.5%
        if entry_signal == 'LONG':
            sl_price = entry_price * (1 - sl_pct)
        else:  # SHORT
            sl_price = entry_price * (1 + sl_pct)

        # Look for exit
        exit_price = None
        exit_reason = None

        for exit_idx, exit_row in future_data.iterrows():
            # Check SL
            if entry_signal == 'LONG':
                if exit_row['low'] <= sl_price:
                    exit_price = sl_price
                    exit_reason = 'SL'
                    break
            else:  # SHORT
                if exit_row['high'] >= sl_price:
                    exit_price = sl_price
                    exit_reason = 'SL'
                    break

            # Check opposite signal
            if exit_row['signal'] == ('SHORT' if entry_signal == 'LONG' else 'LONG'):
                exit_price = exit_row['close']
                exit_reason = 'signal'
                break

            # Check EOD (20:30 UTC)
            exit_time_minutes = exit_idx.hour * 60 + exit_idx.minute
            if exit_time_minutes >= 20 * 60 + 30:
                exit_price = exit_row['close']
                exit_reason = 'EOD'
                break

        if exit_price is None:
            # Use last available price
            exit_price = future_data.iloc[-1]['close']
            exit_reason = 'end_of_data'

        # Calculate PnL
        if entry_signal == 'LONG':
            pnl_pct = (exit_price - entry_price) / entry_price
        else:  # SHORT
            pnl_pct = (entry_price - exit_price) / entry_price

        trades.append({
            'entry_time': idx,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'signal': entry_signal,
            'pnl_pct': pnl_pct,
            'is_winner': pnl_pct > 0
        })

    if len(trades) == 0:
        return {
            'symbol': symbol,
            'period': period,
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0,
            'avg_win': 0,
            'avg_loss': 0,
            'expectancy': 0
        }

    # Calculate metrics
    trades_df = pd.DataFrame(trades)
    wins = trades_df[trades_df['is_winner'] == True]
    losses = trades_df[trades_df['is_winner'] == False]

    return {
        'symbol': symbol,
        'period': period,
        'trades': len(trades_df),
        'wins': len(wins),
        'losses': len(losses),
        'win_rate': len(wins) / len(trades_df) if len(trades_df) > 0 else 0,
        'avg_win': wins['pnl_pct'].mean() if len(wins) > 0 else 0,
        'avg_loss': losses['pnl_pct'].mean() if len(losses) > 0 else 0,
        'expectancy': trades_df['pnl_pct'].mean()
    }

# ============================================================================
# ANALYSIS FUNCTIONS
# ============================================================================

def analyze_all_periods_all_symbols():
    """Test all periods on all symbols in validation period"""
    print("=" * 80)
    print("PERIOD OPTIMIZATION ANALYSIS - ALL SYMBOLS")
    print("=" * 80)
    print(f"\nPeriods tested: {TEST_PERIODS}")
    print(f"Validation period: {VALIDATION_START} to {VALIDATION_END}")

    # Get symbols from Monte Carlo trades
    trades_df = pd.read_csv(TRADES_PATH)
    symbols = sorted(trades_df['symbol'].unique())
    print(f"\nSymbols: {len(symbols)} - {', '.join(symbols)}")

    results = []

    for i, symbol in enumerate(symbols, 1):
        print(f"\nProcessing {symbol} ({i}/{len(symbols)})...")

        for period in TEST_PERIODS:
            metrics = backtest_symbol_period(
                symbol, period,
                VALIDATION_START, VALIDATION_END,
                min_atr_pct=0.007
            )

            if metrics:
                results.append(metrics)
                if metrics['trades'] > 0:
                    print(f"  Period {period:2d}: {metrics['trades']:3d} trades, "
                          f"WR={metrics['win_rate']:.1%}, "
                          f"Exp={metrics['expectancy']:+.3%}")

    # Save results
    results_df = pd.DataFrame(results)
    output_file = OUTPUT_PATH / 'period_optimization_all_symbols.csv'
    results_df.to_csv(output_file, index=False)
    print(f"\n✅ Saved results to: {output_file}")

    return results_df

def analyze_best_period_per_symbol(results_df):
    """Find best performing period for each symbol"""
    print("\n" + "=" * 80)
    print("BEST PERIOD PER SYMBOL")
    print("=" * 80)

    # Filter to symbols with trades
    results_with_trades = results_df[results_df['trades'] > 0].copy()

    if len(results_with_trades) == 0:
        print("No trades found!")
        return None

    # Group by symbol and find best period by expectancy
    best_periods = []

    for symbol in results_with_trades['symbol'].unique():
        symbol_results = results_with_trades[results_with_trades['symbol'] == symbol]

        # Sort by expectancy
        sorted_results = symbol_results.sort_values('expectancy', ascending=False)
        best = sorted_results.iloc[0]

        # Get period=16 for comparison
        period_16 = symbol_results[symbol_results['period'] == 16]
        period_16_exp = period_16['expectancy'].iloc[0] if len(period_16) > 0 else 0

        best_periods.append({
            'symbol': symbol,
            'best_period': int(best['period']),
            'best_expectancy': best['expectancy'],
            'best_trades': int(best['trades']),
            'best_win_rate': best['win_rate'],
            'period_16_expectancy': period_16_exp,
            'improvement_vs_16': best['expectancy'] - period_16_exp if period_16_exp != 0 else 0,
            'all_periods_tested': len(symbol_results)
        })

    best_periods_df = pd.DataFrame(best_periods)
    best_periods_df = best_periods_df.sort_values('improvement_vs_16', ascending=False)

    # Save
    output_file = OUTPUT_PATH / 'best_period_per_symbol.csv'
    best_periods_df.to_csv(output_file, index=False)
    print(f"\n✅ Saved to: {output_file}")

    # Summary
    print(f"\nSymbols where period=16 is BEST: {(best_periods_df['best_period'] == 16).sum()}")
    print(f"Symbols where other period is better: {(best_periods_df['best_period'] != 16).sum()}")
    print(f"\nAverage improvement vs period=16: {best_periods_df['improvement_vs_16'].mean():+.3%}")

    print("\nTop 10 symbols with most improvement from optimization:")
    print(best_periods_df[['symbol', 'best_period', 'best_expectancy', 'period_16_expectancy', 'improvement_vs_16']].head(10).to_string(index=False))

    return best_periods_df

def analyze_period_16_performance(results_df):
    """Analyze if period=16 is consistently good across symbols"""
    print("\n" + "=" * 80)
    print("PERIOD=16 PERFORMANCE ANALYSIS")
    print("=" * 80)

    # Get period=16 results
    period_16 = results_df[results_df['period'] == 16].copy()

    if len(period_16) == 0:
        print("No period=16 results!")
        return None

    # Aggregate stats
    total_trades = period_16['trades'].sum()
    total_wins = period_16['wins'].sum()
    total_losses = period_16['losses'].sum()

    # Weight by number of trades
    weighted_exp = (period_16['expectancy'] * period_16['trades']).sum() / total_trades if total_trades > 0 else 0

    print(f"\nPeriod=16 Overall Statistics:")
    print(f"  Total Trades: {total_trades}")
    print(f"  Win Rate: {total_wins / (total_wins + total_losses):.1%}" if (total_wins + total_losses) > 0 else "  Win Rate: N/A")
    print(f"  Weighted Expectancy: {weighted_exp:+.3%}")

    # Compare to other periods
    period_comparison = []

    for period in TEST_PERIODS:
        period_data = results_df[results_df['period'] == period]
        period_trades = period_data['trades'].sum()

        if period_trades > 0:
            period_wins = period_data['wins'].sum()
            period_losses = period_data['losses'].sum()
            period_exp = (period_data['expectancy'] * period_data['trades']).sum() / period_trades

            period_comparison.append({
                'period': period,
                'total_trades': period_trades,
                'win_rate': period_wins / (period_wins + period_losses) if (period_wins + period_losses) > 0 else 0,
                'expectancy': period_exp
            })

    comparison_df = pd.DataFrame(period_comparison).sort_values('expectancy', ascending=False)

    print("\nAll Periods Ranked by Expectancy:")
    print(comparison_df.to_string(index=False))

    # Save
    output_file = OUTPUT_PATH / 'period_comparison_aggregate.csv'
    comparison_df.to_csv(output_file, index=False)
    print(f"\n✅ Saved to: {output_file}")

    return comparison_df

def adaptive_period_simulation():
    """
    Simulate adaptive period selection:
    - Use 30 days prior to validation to select best period per symbol
    - Apply selected period in validation period
    - Compare to fixed period=16
    """
    print("\n" + "=" * 80)
    print("ADAPTIVE PERIOD SELECTION SIMULATION")
    print("=" * 80)

    # Training period: 30 days before validation
    training_start = pd.Timestamp(VALIDATION_START, tz='UTC') - timedelta(days=TRAINING_DAYS)
    training_start = training_start.strftime('%Y-%m-%d')
    training_end = pd.Timestamp(VALIDATION_START, tz='UTC') - timedelta(days=1)
    training_end = training_end.strftime('%Y-%m-%d')

    print(f"\nTraining period: {training_start} to {training_end}")
    print(f"Validation period: {VALIDATION_START} to {VALIDATION_END}")

    # Get symbols
    trades_df = pd.read_csv(TRADES_PATH)
    symbols = sorted(trades_df['symbol'].unique())

    # Step 1: Find best period per symbol in training period
    print("\nStep 1: Finding best period per symbol (training period)...")

    training_results = []
    for symbol in symbols:
        print(f"  {symbol}...", end=' ')

        best_period = 16
        best_exp = -999

        for period in TEST_PERIODS:
            metrics = backtest_symbol_period(
                symbol, period,
                training_start, training_end,
                min_atr_pct=0.007
            )

            if metrics and metrics['trades'] > 0:
                training_results.append(metrics)
                if metrics['expectancy'] > best_exp:
                    best_exp = metrics['expectancy']
                    best_period = period

        print(f"Best period: {best_period} (exp={best_exp:+.3%})")

    # Aggregate training results to find best periods
    training_df = pd.DataFrame(training_results)

    selected_periods = {}
    for symbol in symbols:
        symbol_data = training_df[training_df['symbol'] == symbol]
        if len(symbol_data) > 0:
            best_row = symbol_data.sort_values('expectancy', ascending=False).iloc[0]
            selected_periods[symbol] = int(best_row['period'])
        else:
            selected_periods[symbol] = 16  # Default

    print(f"\nSelected periods: {selected_periods}")

    # Step 2: Apply selected periods in validation period
    print("\nStep 2: Testing selected periods (validation period)...")

    adaptive_results = []
    fixed_16_results = []

    for symbol in symbols:
        # Adaptive: use selected period
        adaptive_period = selected_periods[symbol]
        adaptive_metrics = backtest_symbol_period(
            symbol, adaptive_period,
            VALIDATION_START, VALIDATION_END,
            min_atr_pct=0.007
        )
        if adaptive_metrics:
            adaptive_metrics['strategy'] = 'adaptive'
            adaptive_results.append(adaptive_metrics)

        # Fixed: use period=16
        fixed_metrics = backtest_symbol_period(
            symbol, 16,
            VALIDATION_START, VALIDATION_END,
            min_atr_pct=0.007
        )
        if fixed_metrics:
            fixed_metrics['strategy'] = 'fixed_16'
            fixed_16_results.append(fixed_metrics)

    # Compare
    adaptive_df = pd.DataFrame(adaptive_results)
    fixed_df = pd.DataFrame(fixed_16_results)

    adaptive_total_trades = adaptive_df['trades'].sum()
    fixed_total_trades = fixed_df['trades'].sum()

    adaptive_exp = (adaptive_df['expectancy'] * adaptive_df['trades']).sum() / adaptive_total_trades if adaptive_total_trades > 0 else 0
    fixed_exp = (fixed_df['expectancy'] * fixed_df['trades']).sum() / fixed_total_trades if fixed_total_trades > 0 else 0

    print("\n" + "=" * 80)
    print("ADAPTIVE vs FIXED COMPARISON")
    print("=" * 80)
    print(f"\nAdaptive (symbol-specific periods):")
    print(f"  Total Trades: {adaptive_total_trades}")
    print(f"  Weighted Expectancy: {adaptive_exp:+.3%}")

    print(f"\nFixed (period=16 for all):")
    print(f"  Total Trades: {fixed_total_trades}")
    print(f"  Weighted Expectancy: {fixed_exp:+.3%}")

    print(f"\nImprovement: {(adaptive_exp - fixed_exp):+.3%} ({((adaptive_exp - fixed_exp) / abs(fixed_exp) * 100):+.1f}%)")

    # Save comparison
    comparison = pd.DataFrame([
        {'strategy': 'adaptive', 'trades': adaptive_total_trades, 'expectancy': adaptive_exp},
        {'strategy': 'fixed_16', 'trades': fixed_total_trades, 'expectancy': fixed_exp}
    ])

    output_file = OUTPUT_PATH / 'adaptive_vs_fixed_comparison.csv'
    comparison.to_csv(output_file, index=False)
    print(f"\n✅ Saved to: {output_file}")

    # Save selected periods
    periods_file = OUTPUT_PATH / 'adaptive_selected_periods.json'
    with open(periods_file, 'w') as f:
        json.dump(selected_periods, f, indent=2)
    print(f"✅ Saved selected periods to: {periods_file}")

    return adaptive_exp, fixed_exp

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Run complete period optimization analysis"""

    # Test all periods on all symbols
    print("Starting comprehensive period analysis...")
    results_df = analyze_all_periods_all_symbols()

    # Analyze best period per symbol
    best_periods_df = analyze_best_period_per_symbol(results_df)

    # Analyze period=16 performance
    period_comparison_df = analyze_period_16_performance(results_df)

    # Adaptive period simulation
    adaptive_exp, fixed_exp = adaptive_period_simulation()

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nOutput files in: {OUTPUT_PATH}/")
    print("  - period_optimization_all_symbols.csv: All results")
    print("  - best_period_per_symbol.csv: Best period for each symbol")
    print("  - period_comparison_aggregate.csv: Overall period ranking")
    print("  - adaptive_vs_fixed_comparison.csv: Adaptive vs fixed comparison")
    print("  - adaptive_selected_periods.json: Selected periods per symbol")

if __name__ == '__main__':
    main()
