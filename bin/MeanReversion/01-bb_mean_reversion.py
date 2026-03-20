#!/usr/bin/env python3
"""
Bollinger Bands Mean Reversion Strategy

Strategy Logic:
1. Bollinger Bands = SMA(N) ± K × StdDev(N)
2. Entry:
   - LONG when close < lower_band (oversold)
   - SHORT when close > upper_band (overbought)
3. Exit:
   - TP: Middle band (SMA) - mean reversion
   - SL: Fixed % or opposite band
4. One position at a time
5. Close at EOD

Parameters:
- BB period: 20 (default)
- BB std multiplier: 2.0 (default)
- SL: 0.5% (default) or dynamic
- Max hold: Until EOD
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time, datetime
from typing import Optional, List, Dict
import random

LOG = logging.getLogger("bb_mr")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="BB Mean Reversion Backtest")
    p.add_argument("--symbol", help="Single symbol (if not specified, test all)")
    p.add_argument("--data-dir", default="../../config/data/m/alpaca")
    p.add_argument("--output", default="bb_mr_results.csv")
    p.add_argument("--bb-period", type=int, default=20,
                   help="BB period (default: 20)")
    p.add_argument("--bb-std", type=float, default=2.0,
                   help="BB std multiplier (default: 2.0)")
    p.add_argument("--sl-pct", type=float, default=0.005,
                   help="Stop loss % (default: 0.5%)")
    p.add_argument("--min-bb-width", type=float, default=0.0,
                   help="Min BB width filter (default: 0.0 = no filter)")
    p.add_argument("--side-filter", choices=['long', 'short', 'both'], default='both',
                   help="Trade only long/short/both (default: both)")
    p.add_argument("--samples-per-symbol", type=int, default=10)
    p.add_argument("--run-id", default=None)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def is_rth(timestamp: pd.Timestamp) -> bool:
    """Check if RTH (09:30-16:00)."""
    t = timestamp.time()
    return time(9, 30) <= t < time(16, 0)


def calculate_bollinger_bands(df: pd.DataFrame, period: int, std_mult: float) -> pd.DataFrame:
    """Calculate Bollinger Bands."""
    df = df.copy()

    df['sma'] = df['close'].rolling(period).mean()
    df['std'] = df['close'].rolling(period).std()
    df['bb_upper'] = df['sma'] + std_mult * df['std']
    df['bb_lower'] = df['sma'] - std_mult * df['std']
    df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['sma']

    return df


def simulate_bb_mr_day(day_df: pd.DataFrame, bb_period: int, bb_std: float, sl_pct: float,
                      min_bb_width: float = 0.0, side_filter: str = 'both') -> List[Dict]:
    """
    Simulate BB mean reversion for a single day.

    Returns list of trades (can be multiple per day).
    """
    # Calculate BB
    day_df = calculate_bollinger_bands(day_df, bb_period, bb_std)

    # Need enough data for BB
    day_df = day_df.dropna(subset=['sma', 'bb_upper', 'bb_lower'])

    if len(day_df) < bb_period + 10:
        return []

    trades = []
    position = None
    position_end_idx = -1

    for i in range(len(day_df)):
        bar_ts = day_df.index[i]
        bar = day_df.iloc[i]

        # Skip if position open
        if position is not None and i <= position_end_idx:
            # Check TP/SL for existing position
            if position['side'] == 'long':
                # TP: price reaches SMA (middle band)
                if bar['high'] >= position['tp']:
                    exit_price = position['tp']
                    exit_pct = (exit_price / position['entry_price'] - 1)
                    trades.append({
                        **position,
                        'exit_time': bar_ts,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct * 100,
                        'outcome': 'tp',
                        'bars_held': i - position['entry_idx']
                    })
                    position = None
                    continue

                # SL
                elif bar['low'] <= position['sl']:
                    exit_price = position['sl']
                    exit_pct = (exit_price / position['entry_price'] - 1)
                    trades.append({
                        **position,
                        'exit_time': bar_ts,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct * 100,
                        'outcome': 'sl',
                        'bars_held': i - position['entry_idx']
                    })
                    position = None
                    continue

            else:  # short
                # TP: price reaches SMA
                if bar['low'] <= position['tp']:
                    exit_price = position['tp']
                    exit_pct = (position['entry_price'] / exit_price - 1)
                    trades.append({
                        **position,
                        'exit_time': bar_ts,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct * 100,
                        'outcome': 'tp',
                        'bars_held': i - position['entry_idx']
                    })
                    position = None
                    continue

                # SL
                elif bar['high'] >= position['sl']:
                    exit_price = position['sl']
                    exit_pct = (position['entry_price'] / exit_price - 1)
                    trades.append({
                        **position,
                        'exit_time': bar_ts,
                        'exit_price': exit_price,
                        'exit_pct': exit_pct * 100,
                        'outcome': 'sl',
                        'bars_held': i - position['entry_idx']
                    })
                    position = None
                    continue

        # Entry logic (if no position)
        if position is None:
            # Check BB width filter
            bb_width_pct = bar['bb_width'] * 100
            if bb_width_pct < min_bb_width:
                continue

            # LONG: price below lower band (oversold)
            if bar['close'] < bar['bb_lower'] and side_filter in ['long', 'both']:
                entry_price = bar['close']
                position = {
                    'side': 'long',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': entry_price,
                    'tp': bar['sma'],  # Target: middle band
                    'sl': entry_price * (1 - sl_pct),
                    'bb_width': bb_width_pct
                }
                position_end_idx = len(day_df) - 1  # Can hold until EOD

            # SHORT: price above upper band (overbought)
            elif bar['close'] > bar['bb_upper'] and side_filter in ['short', 'both']:
                entry_price = bar['close']
                position = {
                    'side': 'short',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': entry_price,
                    'tp': bar['sma'],  # Target: middle band
                    'sl': entry_price * (1 + sl_pct),
                    'bb_width': bb_width_pct
                }
                position_end_idx = len(day_df) - 1

    # EOD close if position still open
    if position is not None:
        last_bar = day_df.iloc[-1]
        exit_price = last_bar['close']

        if position['side'] == 'long':
            exit_pct = (exit_price / position['entry_price'] - 1)
        else:
            exit_pct = (position['entry_price'] / exit_price - 1)

        trades.append({
            **position,
            'exit_time': last_bar.name,
            'exit_price': exit_price,
            'exit_pct': exit_pct * 100,
            'outcome': 'eod',
            'bars_held': len(day_df) - 1 - position['entry_idx']
        })

    return trades


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

    if len(df) < 1000:
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

        if len(day_df) < args.bb_period + 50:
            continue

        # Simulate trades
        trades = simulate_bb_mr_day(day_df, args.bb_period, args.bb_std, args.sl_pct,
                                   args.min_bb_width, args.side_filter)

        for trade in trades:
            result = {
                'symbol': symbol,
                'date': day.strftime('%Y-%m-%d'),
                'side': trade['side'],
                'outcome': trade['outcome'],
                'exit_pct': trade['exit_pct'],
                'bars_held': trade['bars_held'],
                'bb_width': trade['bb_width'],
                'run_id': args.run_id
            }
            results.append(result)

            outcome_emoji = "✅" if trade['outcome'] == 'tp' else ("❌" if trade['outcome'] == 'sl' else "⏰")
            LOG.info(f"  [{symbol}] {day.date()} {outcome_emoji} {trade['side'].upper()} {trade['outcome'].upper()}: "
                    f"{trade['exit_pct']:+.2f}%, BB_width={trade['bb_width']:.2f}%")

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
    LOG.info("BOLLINGER BANDS MEAN REVERSION")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"BB period: {args.bb_period}")
    LOG.info(f"BB std: {args.bb_std}")
    LOG.info(f"SL: {args.sl_pct*100:.1f}%")
    LOG.info(f"Min BB width: {args.min_bb_width:.2f}%")
    LOG.info(f"Side filter: {args.side_filter}")
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

        wins = df[df['exit_pct'] > 0]
        losses = df[df['exit_pct'] < 0]
        if len(losses) > 0:
            wl_ratio = wins['exit_pct'].mean() / abs(losses['exit_pct'].mean())
            LOG.info(f"Win/Loss ratio: {wl_ratio:.2f}")

        LOG.info(f"\nResults saved to: {output_path}")
    else:
        LOG.warning("No trades generated")


if __name__ == "__main__":
    main()
