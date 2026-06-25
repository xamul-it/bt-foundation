#!/usr/bin/env python3
"""
Simulation for HMADynamic strategy - Direct comparison with Backtrader

This script simulates the exact logic of HMADynamic strategy to compare with Backtrader results.

Period: 2025-05-06 to 2025-05-13 (7 trading days)
Parameters: Same as HMADynamic (ATR=0.007, HMA(16, inverted=True), SL=0.5%)

Output:
- Daily opening analysis results
- All signals generated (with reasons if filtered)
- All trades executed with entry/exit details
- Summary CSV for comparison with Backtrader
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime, time, timedelta
import warnings
warnings.filterwarnings('ignore')

# Strategy parameters (matching HMADynamic)
PARAMS = {
    'hma_period': 16,
    'inverted': True,
    'atr_period': 14,
    'atr_min': 0.007,  # 0.7%
    'sl_pct': 0.005,   # 0.5%
    'target_risk': 0.02,  # 2%
    'max_positions': 10,
    'reserve_pct': 0.05,  # 5%
    'alloc_high': 1.5,
    'alloc_medium': 1.0,
    'alloc_low': 0.5,
    'opening_high_threshold': 0.68,
    'opening_low_threshold': 0.40,
}

# Time windows (UTC)
TIME_WINDOWS = {
    'opening_start': time(14, 30),  # 09:30 EST
    'opening_end': time(15, 0),     # 10:00 EST
    'trading_start': time(15, 0),   # 10:00 EST
    'trading_end': time(20, 0),     # 15:00 EST
    'force_close': time(20, 30),    # 15:30 EST
}

# Simulation period
START_DATE = pd.Timestamp('2025-05-06', tz='UTC')
END_DATE = pd.Timestamp('2025-05-13', tz='UTC')

# Initial capital
INITIAL_CAPITAL = 100000.0

# Data path
DATA_PATH = Path('../../config/data/m/alpaca')

# Output path
OUTPUT_PATH = Path('.')
OUTPUT_PATH.mkdir(exist_ok=True)


def calculate_hma(close, period=16):
    """Calculate Hull Moving Average"""
    half_length = period // 2
    sqrt_length = int(np.sqrt(period))

    # WMA of half period
    wma_half = close.rolling(half_length).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )

    # WMA of full period
    wma_full = close.rolling(period).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )

    # Raw HMA
    raw_hma = 2 * wma_half - wma_full

    # Final WMA on raw HMA
    hma = raw_hma.rolling(sqrt_length).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )

    return hma


def calculate_atr(high, low, close, period=14):
    """Calculate Average True Range"""
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    return atr


def load_symbol_data(symbol):
    """Load minute data for a symbol"""
    filepath = DATA_PATH / f'{symbol}.csv'
    if not filepath.exists():
        return None

    try:
        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df = df.set_index('timestamp')

        # Filter to simulation period
        mask = (df.index >= START_DATE) & (df.index <= END_DATE + timedelta(days=1))
        df = df[mask]

        if df.empty:
            return None

        return df
    except Exception as e:
        print(f"Error loading {symbol}: {e}")
        return None


def analyze_opening_period(df, date):
    """Analyze opening period (14:30-15:00 UTC) for a given date"""
    # Filter to opening period
    start_dt = pd.Timestamp(date).replace(hour=14, minute=30, second=0, microsecond=0)
    end_dt = pd.Timestamp(date).replace(hour=15, minute=0, second=0, microsecond=0)

    opening_bars = df[(df.index >= start_dt) & (df.index < end_dt)]

    if opening_bars.empty:
        return None

    # Calculate opening metrics
    opening_high = opening_bars['high'].max()
    opening_low = opening_bars['low'].min()
    opening_range = (opening_high - opening_low) / opening_low
    opening_volume = opening_bars['volume'].sum()

    return {
        'date': date,
        'num_bars': len(opening_bars),
        'opening_range': opening_range,
        'opening_volume': opening_volume,
        'opening_high': opening_high,
        'opening_low': opening_low,
    }


def allocate_capital(opening_results):
    """Allocate capital based on opening analysis"""
    allocations = {}

    for symbol, result in opening_results.items():
        if result is None:
            allocations[symbol] = PARAMS['alloc_medium']  # Default
        else:
            score = result['opening_range']
            if score >= PARAMS['opening_high_threshold']:
                allocations[symbol] = PARAMS['alloc_high']
            elif score >= PARAMS['opening_low_threshold']:
                allocations[symbol] = PARAMS['alloc_medium']
            else:
                allocations[symbol] = PARAMS['alloc_low']

    return allocations


def calculate_position_size(capital, allocation_multiplier, atr_pct, price):
    """Calculate position size with ATR-based risk parity"""
    available_capital = capital * (1 - PARAMS['reserve_pct'])
    base_capital_per_pos = available_capital / PARAMS['max_positions']
    allocated_capital = base_capital_per_pos * allocation_multiplier

    # ATR-based sizing
    base_size = (allocated_capital * PARAMS['target_risk']) / PARAMS['sl_pct']
    atr_adjustment = PARAMS['atr_min'] / max(atr_pct, 0.003)
    adjusted_size = base_size * atr_adjustment

    # Max size cap (20% of portfolio)
    max_size = (capital * 0.20) / price
    final_size = min(adjusted_size, max_size)

    return int(final_size)


def detect_hma_signal(df, idx):
    """
    Detect HMA signal using direction-change logic (inverted mode).

    Looks for HMA turning points (peaks/troughs), NOT continuous trend:
    - Peak (was rising, now not rising) → LONG (contrarian)
    - Trough (was falling, now rising) → SHORT (contrarian)
    """
    if idx < 2:  # Need at least 2 previous bars
        return None

    # Calculate HMA direction for current and previous bars
    prev_hma = df['hma'].iloc[idx - 1]
    curr_hma = df['hma'].iloc[idx]
    prev_prev_hma = df['hma'].iloc[idx - 2]

    if pd.isna(prev_hma) or pd.isna(curr_hma) or pd.isna(prev_prev_hma):
        return None

    # Determine if HMA is rising (current vs previous comparison)
    prev_hma_rising = prev_hma > prev_prev_hma
    curr_hma_rising = curr_hma > prev_hma

    # Detect direction changes (turning points)
    # Inverted mode: peak → LONG, trough → SHORT
    if prev_hma_rising and not curr_hma_rising:
        return 'LONG'  # Peak (was rising, now not) → contrarian LONG
    elif not prev_hma_rising and curr_hma_rising:
        return 'SHORT'  # Trough (was falling, now rising) → contrarian SHORT

    return None


def simulate_day(symbols_data, date, capital, open_positions):
    """Simulate trading for one day"""
    daily_log = {
        'date': date.date(),
        'opening_analysis': {},
        'signals': [],
        'trades': [],
        'capital_start': capital,
        'capital_end': capital,
        'positions_start': len(open_positions),
        'positions_end': 0,
    }

    # Step 1: Opening analysis (14:30-15:00)
    print(f"\n{'='*80}")
    print(f"DATE: {date.date()} - Starting capital: ${capital:,.2f}")
    print(f"{'='*80}")

    opening_results = {}
    for symbol, df in symbols_data.items():
        if df is None:
            continue
        result = analyze_opening_period(df, date)
        opening_results[symbol] = result
        daily_log['opening_analysis'][symbol] = result

        if result:
            print(f"  {symbol:6s} | Opening range: {result['opening_range']*100:.3f}% | "
                  f"Bars: {result['num_bars']} | Volume: {result['opening_volume']:,.0f}")

    # Step 2: Allocate capital
    allocations = allocate_capital(opening_results)
    print(f"\nCapital Allocation:")
    for symbol, alloc in allocations.items():
        if alloc != PARAMS['alloc_medium']:
            alloc_str = "HIGH" if alloc == PARAMS['alloc_high'] else "LOW"
            print(f"  {symbol:6s} → {alloc}x ({alloc_str})")

    # Step 3: Simulate trading (15:00-20:00)
    print(f"\nTrading Window (15:00-20:00 UTC):")

    for symbol, df in symbols_data.items():
        if df is None or symbol not in allocations:
            continue

        # Calculate indicators on FULL day's data (need lookback period!)
        day_start = pd.Timestamp(date).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = pd.Timestamp(date).replace(hour=23, minute=59, second=0, microsecond=0)
        day_data = df[(df.index >= day_start) & (df.index <= day_end)].copy()

        if day_data.empty:
            continue

        # Calculate indicators on full day
        day_data['hma'] = calculate_hma(day_data['close'], PARAMS['hma_period'])
        day_data['atr'] = calculate_atr(
            day_data['high'],
            day_data['low'],
            day_data['close'],
            PARAMS['atr_period']
        )
        day_data['atr_pct'] = day_data['atr'] / day_data['close']

        # Filter to trading window AFTER calculating indicators
        start_dt = pd.Timestamp(date).replace(hour=15, minute=0, second=0, microsecond=0)
        end_dt = pd.Timestamp(date).replace(hour=20, minute=0, second=0, microsecond=0)
        trading_bars = day_data[(day_data.index >= start_dt) & (day_data.index <= end_dt)].copy()

        if trading_bars.empty:
            continue

        # Check each bar for signals
        for idx in range(len(trading_bars)):
            bar_time = trading_bars.index[idx]
            bar = trading_bars.iloc[idx]

            # Check if indicators are ready
            if pd.isna(bar['hma']) or pd.isna(bar['atr_pct']):
                continue

            # Check ATR filter
            if bar['atr_pct'] < PARAMS['atr_min']:
                continue

            # Check max positions
            if len(open_positions) >= PARAMS['max_positions']:
                continue

            # Check if already in position
            if symbol in open_positions:
                # Check stop loss
                position = open_positions[symbol]
                if position['side'] == 'LONG':
                    if bar['close'] <= position['sl_price']:
                        # Stop loss hit
                        pnl = (bar['close'] - position['entry_price']) * position['size']
                        pnl_pct = (bar['close'] / position['entry_price'] - 1) * 100

                        trade = {
                            'symbol': symbol,
                            'entry_time': position['entry_time'],
                            'exit_time': bar_time,
                            'side': 'LONG',
                            'entry_price': position['entry_price'],
                            'exit_price': bar['close'],
                            'size': position['size'],
                            'pnl': pnl,
                            'pnl_pct': pnl_pct,
                            'exit_reason': 'STOP_LOSS',
                            'bars': idx - position['entry_idx'],
                        }

                        capital += position['value'] + pnl
                        del open_positions[symbol]
                        daily_log['trades'].append(trade)

                        print(f"  {bar_time.strftime('%H:%M')} | {symbol:6s} | STOP LOSS | "
                              f"Entry: ${position['entry_price']:.2f} | Exit: ${bar['close']:.2f} | "
                              f"PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)")
                        continue

                # Check for exit signal (reverse HMA)
                signal = detect_hma_signal(trading_bars, idx)
                if signal == 'SHORT' and position['side'] == 'LONG':
                    # Exit long
                    pnl = (bar['close'] - position['entry_price']) * position['size']
                    pnl_pct = (bar['close'] / position['entry_price'] - 1) * 100

                    trade = {
                        'symbol': symbol,
                        'entry_time': position['entry_time'],
                        'exit_time': bar_time,
                        'side': 'LONG',
                        'entry_price': position['entry_price'],
                        'exit_price': bar['close'],
                        'size': position['size'],
                        'pnl': pnl,
                        'pnl_pct': pnl_pct,
                        'exit_reason': 'SIGNAL_REVERSE',
                        'bars': idx - position['entry_idx'],
                    }

                    capital += position['value'] + pnl
                    del open_positions[symbol]
                    daily_log['trades'].append(trade)

                    print(f"  {bar_time.strftime('%H:%M')} | {symbol:6s} | EXIT LONG | "
                          f"Entry: ${position['entry_price']:.2f} | Exit: ${bar['close']:.2f} | "
                          f"PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)")

                continue

            # Detect entry signal
            signal = detect_hma_signal(trading_bars, idx)

            if signal == 'LONG':
                # Calculate position size
                size = calculate_position_size(
                    capital,
                    allocations[symbol],
                    bar['atr_pct'],
                    bar['close']
                )

                if size == 0:
                    continue

                value = size * bar['close']
                if value > capital * 0.20:  # Max 20% per position
                    continue

                # Open position
                sl_price = bar['close'] * (1 - PARAMS['sl_pct'])

                position = {
                    'symbol': symbol,
                    'side': 'LONG',
                    'entry_time': bar_time,
                    'entry_idx': idx,
                    'entry_price': bar['close'],
                    'size': size,
                    'value': value,
                    'sl_price': sl_price,
                    'allocation': allocations[symbol],
                }

                open_positions[symbol] = position
                capital -= value

                daily_log['signals'].append({
                    'time': bar_time,
                    'symbol': symbol,
                    'signal': 'LONG',
                    'price': bar['close'],
                    'size': size,
                    'atr_pct': bar['atr_pct'] * 100,
                    'allocation': allocations[symbol],
                })

                print(f"  {bar_time.strftime('%H:%M')} | {symbol:6s} | ENTRY LONG | "
                      f"Price: ${bar['close']:.2f} | Size: {size} | Value: ${value:,.0f} | "
                      f"ATR: {bar['atr_pct']*100:.2f}% | Alloc: {allocations[symbol]}x")

    # Step 4: Force close at end of day (20:30)
    if open_positions:
        print(f"\nForce Close (20:30 UTC):")
        for symbol, position in list(open_positions.items()):
            df = symbols_data[symbol]
            close_dt = pd.Timestamp(date).replace(hour=20, minute=30, second=0, microsecond=0)
            close_bars = df[df.index >= close_dt]

            if close_bars.empty:
                # Use last available price
                close_price = df['close'].iloc[-1]
                close_time = df.index[-1]
            else:
                close_price = close_bars['close'].iloc[0]
                close_time = close_bars.index[0]

            pnl = (close_price - position['entry_price']) * position['size']
            pnl_pct = (close_price / position['entry_price'] - 1) * 100

            trade = {
                'symbol': symbol,
                'entry_time': position['entry_time'],
                'exit_time': close_time,
                'side': 'LONG',
                'entry_price': position['entry_price'],
                'exit_price': close_price,
                'size': position['size'],
                'pnl': pnl,
                'pnl_pct': pnl_pct,
                'exit_reason': 'FORCE_CLOSE',
                'bars': -1,
            }

            capital += position['value'] + pnl
            daily_log['trades'].append(trade)

            print(f"  {symbol:6s} | FORCE CLOSE | "
                  f"Entry: ${position['entry_price']:.2f} | Exit: ${close_price:.2f} | "
                  f"PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)")

        open_positions.clear()

    daily_log['capital_end'] = capital
    daily_log['positions_end'] = len(open_positions)

    print(f"\nDay Summary:")
    print(f"  Trades: {len(daily_log['trades'])}")
    print(f"  Capital: ${daily_log['capital_start']:,.2f} → ${daily_log['capital_end']:,.2f} "
          f"({(capital / daily_log['capital_start'] - 1) * 100:+.2f}%)")

    return daily_log, capital, open_positions


def main():
    """Main simulation loop"""
    print(f"HMADynamic Simulation - Direct Comparison")
    print(f"Period: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Parameters: HMA({PARAMS['hma_period']}, inverted={PARAMS['inverted']}), "
          f"ATR>={PARAMS['atr_min']*100:.1f}%, SL={PARAMS['sl_pct']*100:.1f}%")
    print(f"Initial Capital: ${INITIAL_CAPITAL:,.2f}")

    # Load all symbol data
    print(f"\nLoading data from {DATA_PATH}...")
    symbol_files = list(DATA_PATH.glob('*.csv'))
    symbols_data = {}

    for filepath in symbol_files:
        symbol = filepath.stem
        df = load_symbol_data(symbol)
        if df is not None:
            symbols_data[symbol] = df
            print(f"  Loaded {symbol}: {len(df)} bars")

    print(f"\nTotal symbols loaded: {len(symbols_data)}")

    # Generate trading days
    trading_days = pd.date_range(START_DATE, END_DATE, freq='D', tz='UTC')
    trading_days = [d for d in trading_days if d.weekday() < 5]  # Remove weekends

    print(f"Trading days: {len(trading_days)}")

    # Simulation loop
    capital = INITIAL_CAPITAL
    open_positions = {}
    all_logs = []

    for date in trading_days:
        daily_log, capital, open_positions = simulate_day(
            symbols_data,
            date,
            capital,
            open_positions
        )
        all_logs.append(daily_log)

    # Summary
    print(f"\n{'='*80}")
    print(f"SIMULATION COMPLETE")
    print(f"{'='*80}")

    total_trades = sum(len(log['trades']) for log in all_logs)
    final_pnl = capital - INITIAL_CAPITAL
    final_pnl_pct = (capital / INITIAL_CAPITAL - 1) * 100

    print(f"Final Capital: ${capital:,.2f}")
    print(f"Total PnL: ${final_pnl:+,.2f} ({final_pnl_pct:+.2f}%)")
    print(f"Total Trades: {total_trades}")

    # Save results
    output_file = OUTPUT_PATH / f'simulation_results_{START_DATE.date()}_{END_DATE.date()}.json'
    with open(output_file, 'w') as f:
        json.dump({
            'parameters': PARAMS,
            'period': {
                'start': str(START_DATE.date()),
                'end': str(END_DATE.date()),
                'days': len(trading_days),
            },
            'results': {
                'initial_capital': INITIAL_CAPITAL,
                'final_capital': capital,
                'total_pnl': final_pnl,
                'total_pnl_pct': final_pnl_pct,
                'total_trades': total_trades,
            },
            'daily_logs': all_logs,
        }, f, indent=2, default=str)

    print(f"\nResults saved to: {output_file}")

    # Save trades CSV for comparison
    all_trades = []
    for log in all_logs:
        for trade in log['trades']:
            all_trades.append(trade)

    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        trades_csv = OUTPUT_PATH / f'simulation_trades_{START_DATE.date()}_{END_DATE.date()}.csv'
        trades_df.to_csv(trades_csv, index=False)
        print(f"Trades saved to: {trades_csv}")


if __name__ == '__main__':
    main()
