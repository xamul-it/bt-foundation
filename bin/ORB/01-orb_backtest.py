#!/usr/bin/env python3
"""
Opening Range Breakout (ORB) Strategy - Backtest

Strategy Logic:
1. Opening Range (OR) = High/Low of first N minutes (default: 30 min, 9:30-10:00)
2. After OR period, wait for breakout:
   - Long if close > OR_high
   - Short if close < OR_low
3. TP = entry + multiplier × OR_size
4. SL = opposite side of OR (OR_low for long, OR_high for short)
5. One position per day
6. Close at EOD (16:00) if not hit TP/SL

Parameters to test:
- OR duration: 15, 30, 60 minutes
- TP multiplier: 1x, 1.5x, 2x, 3x
- Min OR size filter: 0.1%, 0.2%, 0.3%
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time, datetime
from typing import Optional, List, Dict
import random

LOG = logging.getLogger("orb")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="ORB Strategy Backtest")
    p.add_argument("--symbol", help="Single symbol to test (if not specified, test all)")
    p.add_argument("--data-dir", default="../../config/data/m/alpaca")
    p.add_argument("--output", default="orb_results.csv")
    p.add_argument("--or-minutes", type=int, default=30,
                   help="Opening range duration in minutes (default: 30)")
    p.add_argument("--tp-multiplier", type=float, default=2.0,
                   help="TP = entry + multiplier × OR_size (default: 2.0)")
    p.add_argument("--min-or-pct", type=float, default=0.0,
                   help="Minimum OR size as % of price (default: 0.0 = no filter)")
    p.add_argument("--samples-per-symbol", type=int, default=5,
                   help="Number of random day samples per symbol")
    p.add_argument("--run-id", default=None)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def is_rth(timestamp: pd.Timestamp) -> bool:
    """Check if RTH (09:30-16:00)."""
    t = timestamp.time()
    return time(9, 30) <= t < time(16, 0)


def is_or_period(timestamp: pd.Timestamp, or_minutes: int) -> bool:
    """Check if timestamp is in opening range period."""
    t = timestamp.time()
    # Calculate OR end time (9:30 + or_minutes)
    total_minutes = 30 + or_minutes
    or_hour = 9 + total_minutes // 60
    or_minute = total_minutes % 60
    or_end = time(or_hour, or_minute)
    return time(9, 30) <= t < or_end


def calculate_or(day_df: pd.DataFrame, or_minutes: int) -> Optional[Dict]:
    """
    Calculate opening range for a trading day.

    Returns:
        dict with or_high, or_low, or_size, or_size_pct
        None if insufficient data
    """
    # Get OR bars
    or_bars = day_df[day_df.index.map(lambda x: is_or_period(x, or_minutes))]

    if len(or_bars) < or_minutes // 2:  # Need at least half the bars
        return None

    or_high = or_bars['high'].max()
    or_low = or_bars['low'].min()
    or_size = or_high - or_low

    # OR size as % of opening price
    open_price = or_bars['open'].iloc[0]
    or_size_pct = or_size / open_price if open_price > 0 else 0

    return {
        'or_high': or_high,
        'or_low': or_low,
        'or_size': or_size,
        'or_size_pct': or_size_pct,
        'or_end_time': or_bars.index[-1]
    }


def simulate_orb_day(day_df: pd.DataFrame, or_info: Dict, tp_multiplier: float) -> Optional[Dict]:
    """
    Simulate ORB strategy for a single day.

    Returns:
        Trade result dict or None if no trade
    """
    or_high = or_info['or_high']
    or_low = or_info['or_low']
    or_size = or_info['or_size']
    or_end_time = or_info['or_end_time']

    # Bars after OR period
    after_or = day_df[day_df.index > or_end_time]

    if len(after_or) == 0:
        return None

    # Look for breakout
    position = None

    for idx, bar in after_or.iterrows():
        # Entry logic (only if no position)
        if position is None:
            # Long breakout
            if bar['close'] > or_high:
                entry_price = bar['close']
                position = {
                    'side': 'long',
                    'entry_time': idx,
                    'entry_price': entry_price,
                    'tp': entry_price + tp_multiplier * or_size,
                    'sl': or_low
                }
            # Short breakout
            elif bar['close'] < or_low:
                entry_price = bar['close']
                position = {
                    'side': 'short',
                    'entry_time': idx,
                    'entry_price': entry_price,
                    'tp': entry_price - tp_multiplier * or_size,
                    'sl': or_high
                }
        else:
            # Check TP/SL for existing position
            if position['side'] == 'long':
                # Check TP (priority)
                if bar['high'] >= position['tp']:
                    exit_price = position['tp']
                    exit_pct = (exit_price / position['entry_price'] - 1)
                    return {
                        **position,
                        'exit_time': idx,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct,
                        'outcome': 'tp',
                        'bars_held': len(after_or.loc[position['entry_time']:idx])
                    }
                # Check SL
                elif bar['low'] <= position['sl']:
                    exit_price = position['sl']
                    exit_pct = (exit_price / position['entry_price'] - 1)
                    return {
                        **position,
                        'exit_time': idx,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct,
                        'outcome': 'sl',
                        'bars_held': len(after_or.loc[position['entry_time']:idx])
                    }
            else:  # short
                # Check TP (priority)
                if bar['low'] <= position['tp']:
                    exit_price = position['tp']
                    exit_pct = (position['entry_price'] / exit_price - 1)  # Inverse for short
                    return {
                        **position,
                        'exit_time': idx,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct,
                        'outcome': 'tp',
                        'bars_held': len(after_or.loc[position['entry_time']:idx])
                    }
                # Check SL
                elif bar['high'] >= position['sl']:
                    exit_price = position['sl']
                    exit_pct = (position['entry_price'] / exit_price - 1)  # Inverse for short
                    return {
                        **position,
                        'exit_time': idx,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct,
                        'outcome': 'sl',
                        'bars_held': len(after_or.loc[position['entry_time']:idx])
                    }

    # EOD close if still in position
    if position:
        last_bar = after_or.iloc[-1]
        exit_price = last_bar['close']

        if position['side'] == 'long':
            exit_pct = (exit_price / position['entry_price'] - 1)
        else:
            exit_pct = (position['entry_price'] / exit_price - 1)

        return {
            **position,
            'exit_time': after_or.index[-1],
            'exit_price': exit_price,
            'exit_pct': exit_pct,
            'outcome': 'eod',
            'bars_held': len(after_or.loc[position['entry_time']:])
        }

    return None


def process_symbol(symbol: str, data_dir: Path, args) -> List[Dict]:
    """Process a single symbol."""
    csv_path = data_dir / f"{symbol}.csv"

    if not csv_path.exists():
        LOG.warning(f"  [{symbol}] File not found")
        return []

    # Load data
    df = pd.read_csv(csv_path, parse_dates=['timestamp'])
    df = df.set_index('timestamp').sort_index()

    # Filter RTH
    df = df[df.index.map(is_rth)]

    if len(df) < 100:
        LOG.warning(f"  [{symbol}] Too few RTH bars")
        return []

    LOG.info(f"  [{symbol}] Loaded {len(df)} RTH bars from {df.index.min().date()} to {df.index.max().date()}")

    # Get trading days
    trading_days = df.index.normalize().unique().tolist()

    if len(trading_days) < 10:
        LOG.warning(f"  [{symbol}] Too few trading days")
        return []

    # Sample random days
    n_samples = min(args.samples_per_symbol, len(trading_days))
    sampled_days = random.sample(trading_days, n_samples)

    results = []

    for day in sampled_days:
        # Get day's data
        day_df = df[df.index.normalize() == day]

        if len(day_df) < 100:
            continue

        # Calculate OR
        or_info = calculate_or(day_df, args.or_minutes)

        if or_info is None:
            continue

        # Filter by min OR size
        if or_info['or_size_pct'] < args.min_or_pct:
            continue

        # Simulate trade
        trade = simulate_orb_day(day_df, or_info, args.tp_multiplier)

        if trade:
            result = {
                'symbol': symbol,
                'date': day.strftime('%Y-%m-%d'),
                'or_size_pct': or_info['or_size_pct'] * 100,
                'side': trade['side'],
                'outcome': trade['outcome'],
                'exit_pct': trade['exit_pct'] * 100,
                'bars_held': trade['bars_held'],
                'run_id': args.run_id
            }
            results.append(result)

            outcome_emoji = "✅" if trade['outcome'] == 'tp' else ("❌" if trade['outcome'] == 'sl' else "⏰")
            LOG.info(f"  [{symbol}] {day.date()} {outcome_emoji} {trade['side'].upper()} {trade['outcome'].upper()}: "
                    f"{trade['exit_pct']*100:+.2f}%, OR={or_info['or_size_pct']*100:.2f}%")

    return results


def main():
    args = parse_args()
    setup_logging(args.log_level)

    # Set seed
    random.seed(args.seed)
    np.random.seed(args.seed)

    # Generate run_id
    if args.run_id is None:
        args.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    LOG.info("="*80)
    LOG.info("OPENING RANGE BREAKOUT (ORB) BACKTEST")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"OR duration: {args.or_minutes} minutes")
    LOG.info(f"TP multiplier: {args.tp_multiplier}x OR size")
    LOG.info(f"Min OR size: {args.min_or_pct:.2%}")
    LOG.info(f"Samples per symbol: {args.samples_per_symbol}")
    LOG.info("")

    data_dir = Path(args.data_dir)
    output_path = Path(args.output)

    # Get symbols
    if args.symbol:
        symbols = [args.symbol]
    else:
        csv_files = sorted(data_dir.glob("*.csv"))
        symbols = [f.stem for f in csv_files]

    LOG.info(f"Testing {len(symbols)} symbols")
    LOG.info("")

    # Process all symbols
    all_results = []

    for symbol in symbols:
        results = process_symbol(symbol, data_dir, args)
        all_results.extend(results)

        if results:
            # Save incrementally
            df = pd.DataFrame(results)
            append = output_path.exists()
            df.to_csv(output_path, mode='a' if append else 'w', header=not append, index=False)

    # Summary
    if all_results:
        df = pd.DataFrame(all_results)

        LOG.info("\n" + "="*80)
        LOG.info(f"SUMMARY: {len(df)} trades")
        LOG.info("="*80)
        LOG.info(f"Win rate (TP): {(df['outcome'] == 'tp').mean():.1%}")
        LOG.info(f"Loss rate (SL): {(df['outcome'] == 'sl').mean():.1%}")
        LOG.info(f"EOD close: {(df['outcome'] == 'eod').mean():.1%}")
        LOG.info(f"Mean return: {df['exit_pct'].mean():+.2f}%")
        LOG.info(f"Median return: {df['exit_pct'].median():+.2f}%")
        LOG.info(f"Win/Loss ratio: {df[df['exit_pct'] > 0]['exit_pct'].mean() / abs(df[df['exit_pct'] < 0]['exit_pct'].mean()):.2f}")
        LOG.info(f"\nResults saved to: {output_path}")
    else:
        LOG.warning("No trades generated")


if __name__ == "__main__":
    main()
