#!/usr/bin/env python3
"""
HMA Timeframe Analysis - Test on multiple timeframes

Tests the HMA strategy on different timeframes (1min, 5min, 15min, 1h)
to find optimal timeframe with best expectancy vs slippage ratio.

Theory: Larger timeframes should have:
- Higher expectancy per trade (larger moves)
- Same slippage impact (0.03% fixed)
- Better net expectancy after slippage
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time, datetime
from typing import Optional, List, Dict
import random
import sys

# Add parent directory to path to import from 01-hma_backtest.py
sys.path.insert(0, str(Path(__file__).parent))

LOG = logging.getLogger("hma_tf_analysis")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="HMA Timeframe Analysis")
    p.add_argument("--symbol", help="Single symbol (if not specified, test all)")
    p.add_argument("--data-dir", default="../../config/data/m/alpaca")
    p.add_argument("--output-dir", default="./tf_analysis")
    p.add_argument("--period", type=int, default=16,
                   help="HMA period (default: 16)")
    p.add_argument("--inverted", action="store_true", default=True,
                   help="Use inverted logic (default: True)")
    p.add_argument("--no-inverted", dest="inverted", action="store_false")
    p.add_argument("--sl-pct", type=float, default=0.005,
                   help="Stop loss % (default: 0.5%)")
    p.add_argument("--tp-pct", type=float, default=None,
                   help="Take profit %")
    p.add_argument("--slippage-pct", type=float, default=0.0003,
                   help="Expected slippage % (default: 0.03%)")
    p.add_argument("--samples-per-symbol", type=int, default=30,
                   help="Number of random days to test per symbol")
    p.add_argument("--timeframes", default="1,5,15,60",
                   help="Comma-separated timeframes in minutes (default: 1,5,15,60)")
    p.add_argument("--run-id", default=None)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def is_rth(timestamp: pd.Timestamp) -> bool:
    """Check if RTH (09:30-16:00)."""
    t = timestamp.time()
    return time(9, 30) <= t < time(16, 0)


def resample_to_timeframe(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Resample 1-minute data to larger timeframe.

    Args:
        df: DataFrame with 1-minute OHLCV data (index=timestamp)
        minutes: Timeframe in minutes (5, 15, 60, etc.)

    Returns:
        Resampled DataFrame with OHLCV
    """
    if minutes == 1:
        return df.copy()

    # Resample to target timeframe
    rule = f'{minutes}T'  # T = minutes

    resampled = df.resample(rule, label='left', closed='left').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    return resampled


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
    """Calculate Hull Moving Average."""
    df = df.copy()

    half_period = int(period / 2)
    sqrt_period = int(np.sqrt(period))

    wma_half = calculate_wma(df['close'], half_period)
    wma_full = calculate_wma(df['close'], period)
    raw_hma = 2 * wma_half - wma_full
    df['hma'] = calculate_wma(raw_hma, sqrt_period)

    return df


def simulate_hma_day(day_df: pd.DataFrame, period: int, inverted: bool,
                     sl_pct: float = 0.0, tp_pct: Optional[float] = None) -> List[Dict]:
    """
    Simulate HMA strategy for a single day with close-and-revert logic.
    Same implementation as 01-hma_backtest.py
    """
    day_df = calculate_hma(day_df, period)
    day_df = day_df.dropna(subset=['hma'])

    if len(day_df) < period + 10:
        return []

    trades = []
    position = None
    current_direction = 0

    for i in range(1, len(day_df)):
        bar_ts = day_df.index[i]
        bar = day_df.iloc[i]
        prev_bar = day_df.iloc[i - 1]

        hma_curr = bar['hma']
        hma_prev = prev_bar['hma']

        if pd.isna(hma_curr) or pd.isna(hma_prev):
            continue

        if inverted:
            signal_short = hma_prev < hma_curr
            signal_long = hma_prev > hma_curr
        else:
            signal_long = hma_prev < hma_curr
            signal_short = hma_prev > hma_curr

        # Check TP/SL before processing signals
        if position is not None:
            hit_tp_or_sl = False

            if position['side'] == 'long':
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

            if hit_tp_or_sl:
                continue

        # Process HMA signals (close-and-revert)
        if signal_long:
            if position is not None and position['side'] == 'short':
                exit_price = bar['close']
                exit_pct = (position['entry_price'] / exit_price - 1)
                trades.append({
                    **position,
                    'exit_time': bar_ts,
                    'exit_price': exit_price,
                    'exit_pct': exit_pct * 100,
                    'outcome': 'signal',
                    'bars_held': i - position['entry_idx']
                })
                position = None

            if current_direction != 1:
                entry_price = bar['close']
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

        elif signal_short:
            if position is not None and position['side'] == 'long':
                exit_price = bar['close']
                exit_pct = (exit_price / position['entry_price'] - 1)
                trades.append({
                    **position,
                    'exit_time': bar_ts,
                    'exit_price': exit_price,
                    'exit_pct': exit_pct * 100,
                    'outcome': 'signal',
                    'bars_held': i - position['entry_idx']
                })
                position = None

            if current_direction != -1:
                entry_price = bar['close']
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

    # EOD close
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


def process_symbol_timeframe(symbol: str, data_dir: Path, timeframe_min: int, args) -> List[Dict]:
    """Process a single symbol on a specific timeframe."""
    csv_path = data_dir / f"{symbol}.csv"

    if not csv_path.exists():
        LOG.warning(f"  [{symbol}] File not found")
        return []

    # Load 1-minute data
    df = pd.read_csv(csv_path, parse_dates=['timestamp'])
    df = df.set_index('timestamp').sort_index()
    df = df[df.index.map(is_rth)]

    if len(df) < 1000:
        LOG.warning(f"  [{symbol}] Too few RTH bars")
        return []

    # Resample to target timeframe
    df_resampled = resample_to_timeframe(df, timeframe_min)

    LOG.debug(f"  [{symbol}] {timeframe_min}min: {len(df)} 1min bars → {len(df_resampled)} {timeframe_min}min bars")

    # Get trading days
    trading_days = df_resampled.index.normalize().unique().tolist()

    if len(trading_days) < 10:
        LOG.warning(f"  [{symbol}] Too few trading days on {timeframe_min}min")
        return []

    # Sample random days
    n_samples = min(args.samples_per_symbol, len(trading_days))
    sampled_days = random.sample(trading_days, n_samples)

    results = []

    for day in sampled_days:
        day_df = df_resampled[df_resampled.index.normalize() == day]

        if len(day_df) < args.period + 10:
            continue

        trades = simulate_hma_day(day_df, args.period, args.inverted,
                                 args.sl_pct, args.tp_pct)

        for trade in trades:
            result = {
                'symbol': symbol,
                'timeframe_min': timeframe_min,
                'date': day.strftime('%Y-%m-%d'),
                'side': trade['side'],
                'outcome': trade['outcome'],
                'exit_pct': trade['exit_pct'],
                'bars_held': trade['bars_held'],
                'hma': trade['hma'],
                'run_id': args.run_id
            }
            results.append(result)

    return results


def calculate_expectancy_stats(df: pd.DataFrame, slippage_pct: float) -> Dict:
    """Calculate expectancy statistics including net after slippage."""
    wins = df[df['exit_pct'] > 0]
    losses = df[df['exit_pct'] < 0]

    p_win = len(wins) / len(df) if len(df) > 0 else 0
    p_loss = len(losses) / len(df) if len(df) > 0 else 0
    avg_win = wins['exit_pct'].mean() if len(wins) > 0 else 0
    avg_loss = abs(losses['exit_pct'].mean()) if len(losses) > 0 else 0

    gross_expectancy = p_win * avg_win - p_loss * avg_loss
    net_expectancy = gross_expectancy - slippage_pct * 100  # Convert to %

    return {
        'trades': len(df),
        'p_win': p_win,
        'p_loss': p_loss,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'gross_expectancy': gross_expectancy,
        'slippage_pct': slippage_pct * 100,
        'net_expectancy': net_expectancy,
        'mean_return': df['exit_pct'].mean(),
        'median_return': df['exit_pct'].median(),
        'std_return': df['exit_pct'].std(),
    }


def main():
    args = parse_args()
    setup_logging(args.log_level)

    random.seed(args.seed)
    np.random.seed(args.seed)

    if args.run_id is None:
        args.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Parse timeframes
    timeframes = [int(tf) for tf in args.timeframes.split(',')]

    LOG.info("="*80)
    LOG.info("HMA TIMEFRAME ANALYSIS")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"Timeframes: {timeframes} minutes")
    LOG.info(f"Period: {args.period}")
    LOG.info(f"Inverted: {args.inverted}")
    LOG.info(f"SL: {args.sl_pct*100:.1f}%" if args.sl_pct > 0 else "SL: disabled")
    LOG.info(f"TP: {args.tp_pct*100:.1f}%" if args.tp_pct else "TP: disabled")
    LOG.info(f"Expected slippage: {args.slippage_pct*100:.3f}%")
    LOG.info(f"Samples per symbol: {args.samples_per_symbol}")
    LOG.info("")

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    # Get symbols
    if args.symbol:
        symbols = [args.symbol]
    else:
        csv_files = sorted(data_dir.glob("*.csv"))
        symbols = [f.stem for f in csv_files]

    LOG.info(f"Testing {len(symbols)} symbols on {len(timeframes)} timeframes")
    LOG.info("")

    # Test each timeframe
    all_results = {}

    for tf_min in timeframes:
        LOG.info(f"Testing {tf_min}-minute timeframe...")
        tf_results = []

        for symbol in symbols:
            results = process_symbol_timeframe(symbol, data_dir, tf_min, args)
            tf_results.extend(results)

        all_results[tf_min] = tf_results

        if tf_results:
            output_path = output_dir / f"hma_{tf_min}min.csv"
            df = pd.DataFrame(tf_results)
            df.to_csv(output_path, index=False)
            LOG.info(f"  {tf_min}min: {len(tf_results)} trades → {output_path}")

    # Comparative summary
    LOG.info("\n" + "="*80)
    LOG.info("TIMEFRAME COMPARISON")
    LOG.info("="*80)
    LOG.info(f"{'TF':<8} {'Trades':<8} {'Win%':<8} {'AvgWin%':<10} {'AvgLoss%':<10} {'Gross%':<10} {'Slip%':<8} {'Net%':<10} {'Status'}")
    LOG.info("-"*80)

    summary_data = []

    for tf_min in timeframes:
        results = all_results.get(tf_min, [])
        if not results:
            continue

        df = pd.DataFrame(results)
        stats = calculate_expectancy_stats(df, args.slippage_pct)

        status = "✅ PASS" if stats['net_expectancy'] >= 0.04 else "⚠️  MARGINAL" if stats['net_expectancy'] >= 0.01 else "❌ FAIL"

        LOG.info(f"{tf_min}min    {stats['trades']:<8} {stats['p_win']*100:<7.1f}% {stats['avg_win']:<9.3f}% {stats['avg_loss']:<9.3f}% "
                f"{stats['gross_expectancy']:<9.3f}% {stats['slippage_pct']:<7.3f}% {stats['net_expectancy']:<9.3f}% {status}")

        summary_data.append({
            'timeframe_min': tf_min,
            **stats
        })

    # Save summary
    summary_df = pd.DataFrame(summary_data)
    summary_path = output_dir / "timeframe_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    LOG.info("")
    LOG.info(f"Summary saved to: {summary_path}")

    # Recommendation
    LOG.info("\n" + "="*80)
    LOG.info("RECOMMENDATION")
    LOG.info("="*80)

    best_tf = summary_df.loc[summary_df['net_expectancy'].idxmax()]
    LOG.info(f"Best timeframe: {int(best_tf['timeframe_min'])} minutes")
    LOG.info(f"  Net expectancy: {best_tf['net_expectancy']:.3f}%")
    LOG.info(f"  Trades: {int(best_tf['trades'])}")
    LOG.info(f"  Win rate: {best_tf['p_win']*100:.1f}%")

    if best_tf['net_expectancy'] >= 0.04:
        LOG.info("\n✅ This timeframe meets the 0.04% net expectancy target!")
    elif best_tf['net_expectancy'] >= 0.01:
        LOG.info("\n⚠️  Marginal edge. Consider Step 2 (parameter optimization).")
    else:
        LOG.info("\n❌ No timeframe meets minimum edge. Proceed to Step 2 (optimization + filters).")


if __name__ == "__main__":
    main()
