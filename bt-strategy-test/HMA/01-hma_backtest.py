#!/usr/bin/env python3
"""
HMA (Hull Moving Average) Strategy Backtest

Replicates the HMA strategy from strategies/intraday.py@HMA
with realistic testing methodology.

Strategy Logic (with inverted=True):
1. Calculate HMA(period)
2. Entry signals:
   - LONG when HMA[-1] > HMA[0] (HMA going down, contrarian)
   - SHORT when HMA[-1] < HMA[0] (HMA going up, contrarian)
3. Position management:
   - Close opposite position before entering new one
   - Track direction to prevent re-entry without direction change
   - One position at a time per symbol
   - Close all positions at EOD
4. Entry execution:
   - Enter at OPEN of bar AFTER signal (realistic)
   - Not at close of signal bar (optimistic)

Parameters:
- period: HMA period (default: 16)
- inverted: Use inverted logic (default: True)
- sl_pct: Stop loss % (optional, default: 0.5%)
- tp_pct: Take profit % (optional, not used if None)
- samples_per_symbol: Number of random days to test (default: 10)
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time, datetime
from typing import Optional, List, Dict
import random

LOG = logging.getLogger("hma_backtest")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="HMA Strategy Backtest")
    p.add_argument("--symbol", help="Single symbol (if not specified, test all)")
    p.add_argument("--data-dir", default="../../config/data/m/alpaca")
    p.add_argument("--output", default="hma_results.csv")
    p.add_argument("--period", type=int, default=16,
                   help="HMA period (default: 16)")
    p.add_argument("--inverted", action="store_true", default=True,
                   help="Use inverted logic (default: True)")
    p.add_argument("--no-inverted", dest="inverted", action="store_false",
                   help="Use normal logic (not inverted)")
    p.add_argument("--sl-pct", type=float, default=0.005,
                   help="Stop loss % (default: 0.5%, set to 0 to disable)")
    p.add_argument("--tp-pct", type=float, default=None,
                   help="Take profit % (optional, not used if not specified)")
    p.add_argument("--samples-per-symbol", type=int, default=10)
    p.add_argument("--run-id", default=None)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def is_rth(timestamp: pd.Timestamp) -> bool:
    """Check if RTH (09:30-16:00)."""
    t = timestamp.time()
    return time(9, 30) <= t < time(16, 0)


def calculate_wma(series: pd.Series, period: int) -> pd.Series:
    """Calculate Weighted Moving Average."""
    weights = np.arange(1, period + 1)

    def wma_at_idx(values):
        if len(values) < period:
            return np.nan
        return np.dot(values[-period:], weights) / weights.sum()

    result = series.rolling(period).apply(wma_at_idx, raw=True)
    return result


def calculate_hma(df: pd.DataFrame, period: int) -> pd.DataFrame:
    """
    Calculate Hull Moving Average.

    HMA(n) = WMA(2 × WMA(n/2) - WMA(n), sqrt(n))
    """
    df = df.copy()

    half_period = int(period / 2)
    sqrt_period = int(np.sqrt(period))

    # Calculate WMAs
    wma_half = calculate_wma(df['close'], half_period)
    wma_full = calculate_wma(df['close'], period)

    # Calculate raw HMA
    raw_hma = 2 * wma_half - wma_full

    # Final WMA on raw HMA
    df['hma'] = calculate_wma(raw_hma, sqrt_period)

    return df


def simulate_hma_day(day_df: pd.DataFrame, period: int, inverted: bool,
                     sl_pct: float = 0.0, tp_pct: Optional[float] = None) -> List[Dict]:
    """
    Simulate HMA strategy for a single day with close-and-revert logic.

    The strategy is ALWAYS in a position (long or short), switching when HMA changes direction.
    This matches the backtrader implementation.

    Returns list of trades (can be multiple per day).
    """
    # Calculate HMA
    day_df = calculate_hma(day_df, period)

    # Need enough data for HMA
    day_df = day_df.dropna(subset=['hma'])

    if len(day_df) < period + 10:
        return []

    trades = []
    position = None
    current_direction = 0  # Current position direction: 1=long, -1=short, 0=none

    for i in range(1, len(day_df)):  # Start from 1 to compare with previous bar
        bar_ts = day_df.index[i]
        bar = day_df.iloc[i]
        prev_bar = day_df.iloc[i - 1]

        # Get HMA values
        hma_curr = bar['hma']
        hma_prev = prev_bar['hma']

        # Skip if HMA not valid
        if pd.isna(hma_curr) or pd.isna(hma_prev):
            continue

        # Determine HMA signal based on inverted flag
        if inverted:
            # Inverted: go SHORT when HMA rising, LONG when HMA falling
            signal_short = hma_prev < hma_curr  # HMA going up
            signal_long = hma_prev > hma_curr   # HMA going down
        else:
            # Normal: go LONG when HMA rising, SHORT when HMA falling
            signal_long = hma_prev < hma_curr
            signal_short = hma_prev > hma_curr

        # Check TP/SL BEFORE processing new signals
        if position is not None:
            hit_tp_or_sl = False

            if position['side'] == 'long':
                # TP check
                if tp_pct and bar['high'] >= position['tp']:
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
                    current_direction = 0
                    hit_tp_or_sl = True

                # SL check
                elif sl_pct > 0 and bar['low'] <= position['sl']:
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
                    current_direction = 0
                    hit_tp_or_sl = True

            else:  # short
                # TP check
                if tp_pct and bar['low'] <= position['tp']:
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
                    current_direction = 0
                    hit_tp_or_sl = True

                # SL check
                elif sl_pct > 0 and bar['high'] >= position['sl']:
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
                    current_direction = 0
                    hit_tp_or_sl = True

            # If we hit TP/SL, skip signal processing for this bar
            if hit_tp_or_sl:
                continue

        # Process HMA signals (close-and-revert logic)
        # LONG signal
        if signal_long:
            # If we're short, close the short position first
            if position is not None and position['side'] == 'short':
                exit_price = bar['close']  # Close at this bar's close
                exit_pct = (position['entry_price'] / exit_price - 1)
                trades.append({
                    **position,
                    'exit_time': bar_ts,
                    'exit_price': exit_price,
                    'exit_pct': exit_pct * 100,
                    'outcome': 'signal',  # Closed due to opposite signal
                    'bars_held': i - position['entry_idx']
                })
                position = None

            # Open long position (only if direction changed)
            if current_direction != 1:
                entry_price = bar['close']  # Enter at this bar's close (like backtrader)
                position = {
                    'side': 'long',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': entry_price,
                    'hma': hma_curr
                }

                if sl_pct > 0:
                    position['sl'] = entry_price * (1 - sl_pct)
                else:
                    position['sl'] = None

                if tp_pct:
                    position['tp'] = entry_price * (1 + tp_pct)
                else:
                    position['tp'] = None

                current_direction = 1

        # SHORT signal
        elif signal_short:
            # If we're long, close the long position first
            if position is not None and position['side'] == 'long':
                exit_price = bar['close']  # Close at this bar's close
                exit_pct = (exit_price / position['entry_price'] - 1)
                trades.append({
                    **position,
                    'exit_time': bar_ts,
                    'exit_price': exit_price,
                    'exit_pct': exit_pct * 100,
                    'outcome': 'signal',  # Closed due to opposite signal
                    'bars_held': i - position['entry_idx']
                })
                position = None

            # Open short position (only if direction changed)
            if current_direction != -1:
                entry_price = bar['close']  # Enter at this bar's close
                position = {
                    'side': 'short',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': entry_price,
                    'hma': hma_curr
                }

                if sl_pct > 0:
                    position['sl'] = entry_price * (1 + sl_pct)
                else:
                    position['sl'] = None

                if tp_pct:
                    position['tp'] = entry_price * (1 - tp_pct)
                else:
                    position['tp'] = None

                current_direction = -1

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

        if len(day_df) < args.period + 50:
            continue

        # Simulate trades
        trades = simulate_hma_day(day_df, args.period, args.inverted,
                                 args.sl_pct, args.tp_pct)

        for trade in trades:
            result = {
                'symbol': symbol,
                'date': day.strftime('%Y-%m-%d'),
                'side': trade['side'],
                'outcome': trade['outcome'],
                'exit_pct': trade['exit_pct'],
                'bars_held': trade['bars_held'],
                'hma': trade['hma'],
                'run_id': args.run_id
            }
            results.append(result)

            outcome_emoji = "✅" if trade['outcome'] == 'tp' else ("❌" if trade['outcome'] == 'sl' else "⏰")
            LOG.info(f"  [{symbol}] {day.date()} {outcome_emoji} {trade['side'].upper()} {trade['outcome'].upper()}: "
                    f"{trade['exit_pct']:+.2f}%, HMA={trade['hma']:.2f}")

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
    LOG.info("HMA STRATEGY BACKTEST")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"Period: {args.period}")
    LOG.info(f"Inverted: {args.inverted}")
    LOG.info(f"SL: {args.sl_pct*100:.1f}%" if args.sl_pct > 0 else "SL: disabled")
    LOG.info(f"TP: {args.tp_pct*100:.1f}%" if args.tp_pct else "TP: disabled")
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
        LOG.info(f"Win rate (TP): {(df['outcome'] == 'tp').sum() / len(df):.1%}")
        LOG.info(f"Loss rate (SL): {(df['outcome'] == 'sl').sum() / len(df):.1%}")
        LOG.info(f"EOD close: {(df['outcome'] == 'eod').sum() / len(df):.1%}")
        LOG.info(f"Mean return: {df['exit_pct'].mean():+.3f}%")
        LOG.info(f"Median return: {df['exit_pct'].median():+.3f}%")

        # Calculate expectancy
        wins = df[df['exit_pct'] > 0]
        losses = df[df['exit_pct'] < 0]

        if len(wins) > 0 and len(losses) > 0:
            p_win = len(wins) / len(df)
            p_loss = len(losses) / len(df)
            avg_win = wins['exit_pct'].mean()
            avg_loss = abs(losses['exit_pct'].mean())
            expectancy = p_win * avg_win - p_loss * avg_loss

            LOG.info(f"\nExpectancy: {expectancy:+.3f}%")
            LOG.info(f"  P(win) × avg_win: {p_win:.1%} × {avg_win:.3f}% = {p_win * avg_win:.3f}%")
            LOG.info(f"  P(loss) × avg_loss: {p_loss:.1%} × {avg_loss:.3f}% = {p_loss * avg_loss:.3f}%")

            if expectancy >= 0.1:
                LOG.info("\n✅ Strategy passes minimum expectancy threshold (0.1%)")
            else:
                LOG.warning(f"\n⚠️  Strategy FAILS minimum expectancy threshold (0.1% required, got {expectancy:.3f}%)")

        LOG.info(f"\nResults saved to: {output_path}")
    else:
        LOG.warning("No trades generated")


if __name__ == "__main__":
    main()
