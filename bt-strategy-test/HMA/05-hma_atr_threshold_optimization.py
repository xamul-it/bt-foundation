#!/usr/bin/env python3
"""
HMA ATR Threshold Optimization

Fine-tune the ATR threshold to maximize net expectancy.
Tests multiple ATR thresholds with atr_and_time filter.

Also supports out-of-sample validation on different time periods.
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time, datetime, timedelta
from typing import Optional, List, Dict
import random
import sys

sys.path.insert(0, str(Path(__file__).parent))

LOG = logging.getLogger("hma_atr_opt")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="HMA ATR Threshold Optimization")
    p.add_argument("--symbol", help="Single symbol (default: test all)")
    p.add_argument("--data-dir", default="../../config/data/m/alpaca")
    p.add_argument("--output-dir", default="./atr_optimization")
    p.add_argument("--period", type=int, default=16)
    p.add_argument("--inverted", action="store_true", default=True)
    p.add_argument("--no-inverted", dest="inverted", action="store_false")
    p.add_argument("--sl-pct", type=float, default=0.005)
    p.add_argument("--tp-pct", type=float, default=None)
    p.add_argument("--slippage-pct", type=float, default=0.0003)
    p.add_argument("--atr-thresholds", default="0.3,0.4,0.5,0.6,0.7,0.8",
                   help="Comma-separated ATR thresholds to test (default: 0.3,0.4,0.5,0.6,0.7,0.8)")
    p.add_argument("--hour-min", type=int, default=10,
                   help="Minimum trading hour (default: 10)")
    p.add_argument("--hour-max", type=int, default=15,
                   help="Maximum trading hour (default: 15)")
    p.add_argument("--samples-per-symbol", type=int, default=50)
    p.add_argument("--start-date", default=None,
                   help="Start date for out-of-sample validation (YYYY-MM-DD)")
    p.add_argument("--end-date", default=None,
                   help="End date for out-of-sample validation (YYYY-MM-DD)")
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
    return series.rolling(period).apply(wma_at_idx, raw=True)


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


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range."""
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return true_range.rolling(period).mean()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add HMA and ATR indicators."""
    df = df.copy()
    df['atr'] = calculate_atr(df, period=14)
    df['atr_pct'] = df['atr'] / df['close'] * 100
    df['hour'] = df.index.hour
    return df


def simulate_hma_day_with_atr_filter(day_df: pd.DataFrame, period: int, inverted: bool,
                                      sl_pct: float, tp_pct: Optional[float],
                                      atr_threshold: float, hour_min: int, hour_max: int) -> List[Dict]:
    """Simulate HMA with ATR and time filters."""
    day_df = calculate_hma(day_df, period)
    day_df = add_indicators(day_df)
    day_df = day_df.dropna(subset=['hma', 'atr_pct'])

    if len(day_df) < period + 20:
        return []

    trades = []
    position = None
    current_direction = 0

    for i in range(1, len(day_df)):
        bar = day_df.iloc[i]
        prev_bar = day_df.iloc[i - 1]
        bar_ts = day_df.index[i]

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

        # Check TP/SL
        if position is not None:
            if position['side'] == 'long':
                if tp_pct and bar['high'] >= position['tp']:
                    exit_pct = (position['tp'] / position['entry_price'] - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': position['tp'],
                                 'exit_pct': exit_pct * 100, 'outcome': 'tp',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    continue
                elif sl_pct > 0 and bar['low'] <= position['sl']:
                    exit_pct = (position['sl'] / position['entry_price'] - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': position['sl'],
                                 'exit_pct': exit_pct * 100, 'outcome': 'sl',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    continue
            else:  # short
                if tp_pct and bar['low'] <= position['tp']:
                    exit_pct = (position['entry_price'] / position['tp'] - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': position['tp'],
                                 'exit_pct': exit_pct * 100, 'outcome': 'tp',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    continue
                elif sl_pct > 0 and bar['high'] >= position['sl']:
                    exit_pct = (position['entry_price'] / position['sl'] - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': position['sl'],
                                 'exit_pct': exit_pct * 100, 'outcome': 'sl',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    continue

        # Apply filters
        atr_ok = bar['atr_pct'] >= atr_threshold
        time_ok = hour_min <= bar['hour'] < hour_max

        if not (atr_ok and time_ok):
            continue

        # Process signals
        if signal_long:
            if position is not None and position['side'] == 'short':
                exit_pct = (position['entry_price'] / bar['close'] - 1)
                trades.append({**position, 'exit_time': bar_ts, 'exit_price': bar['close'],
                             'exit_pct': exit_pct * 100, 'outcome': 'signal',
                             'bars_held': i - position['entry_idx']})
                position = None

            if current_direction != 1:
                position = {
                    'side': 'long',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': bar['close'],
                    'atr_pct': bar['atr_pct'],
                    'atr_threshold': atr_threshold
                }
                if sl_pct > 0:
                    position['sl'] = bar['close'] * (1 - sl_pct)
                if tp_pct:
                    position['tp'] = bar['close'] * (1 + tp_pct)
                current_direction = 1

        elif signal_short:
            if position is not None and position['side'] == 'long':
                exit_pct = (bar['close'] / position['entry_price'] - 1)
                trades.append({**position, 'exit_time': bar_ts, 'exit_price': bar['close'],
                             'exit_pct': exit_pct * 100, 'outcome': 'signal',
                             'bars_held': i - position['entry_idx']})
                position = None

            if current_direction != -1:
                position = {
                    'side': 'short',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': bar['close'],
                    'atr_pct': bar['atr_pct'],
                    'atr_threshold': atr_threshold
                }
                if sl_pct > 0:
                    position['sl'] = bar['close'] * (1 + sl_pct)
                if tp_pct:
                    position['tp'] = bar['close'] * (1 - tp_pct)
                current_direction = -1

    # EOD close
    if position is not None:
        last_bar = day_df.iloc[-1]
        if position['side'] == 'long':
            exit_pct = (last_bar['close'] / position['entry_price'] - 1)
        else:
            exit_pct = (position['entry_price'] / last_bar['close'] - 1)
        trades.append({**position, 'exit_time': last_bar.name, 'exit_price': last_bar['close'],
                     'exit_pct': exit_pct * 100, 'outcome': 'eod',
                     'bars_held': len(day_df) - 1 - position['entry_idx']})

    return trades


def process_symbol_with_atr_threshold(symbol: str, data_dir: Path, atr_threshold: float,
                                      hour_min: int, hour_max: int, args,
                                      start_date: Optional[str] = None,
                                      end_date: Optional[str] = None) -> List[Dict]:
    """Process symbol with specific ATR threshold."""
    csv_path = data_dir / f"{symbol}.csv"
    if not csv_path.exists():
        return []

    df = pd.read_csv(csv_path, parse_dates=['timestamp'])
    df = df.set_index('timestamp').sort_index()
    df = df[df.index.map(is_rth)]

    # Filter by date range if specified
    if start_date:
        start_ts = pd.to_datetime(start_date).tz_localize('UTC')
        df = df[df.index >= start_ts]
    if end_date:
        end_ts = pd.to_datetime(end_date).tz_localize('UTC')
        df = df[df.index <= end_ts]

    if len(df) < 1000:
        return []

    trading_days = df.index.normalize().unique().tolist()
    if len(trading_days) < 10:
        return []

    # Sample days
    n_samples = min(args.samples_per_symbol, len(trading_days))
    sampled_days = random.sample(trading_days, n_samples)

    results = []
    for day in sampled_days:
        day_df = df[df.index.normalize() == day]
        if len(day_df) < args.period + 50:
            continue

        trades = simulate_hma_day_with_atr_filter(day_df, args.period, args.inverted,
                                                  args.sl_pct, args.tp_pct,
                                                  atr_threshold, hour_min, hour_max)

        for trade in trades:
            results.append({
                'symbol': symbol,
                'date': day.strftime('%Y-%m-%d'),
                'side': trade['side'],
                'outcome': trade['outcome'],
                'exit_pct': trade['exit_pct'],
                'bars_held': trade['bars_held'],
                'atr_pct': trade['atr_pct'],
                'atr_threshold': atr_threshold,
                'run_id': args.run_id
            })

    return results


def calculate_stats(df: pd.DataFrame, slippage_pct: float) -> Dict:
    """Calculate performance statistics."""
    if len(df) == 0:
        return {
            'trades': 0, 'gross_expectancy': 0, 'net_expectancy': -slippage_pct * 100,
            'p_win': 0, 'avg_win': 0, 'avg_loss': 0
        }

    wins = df[df['exit_pct'] > 0]
    losses = df[df['exit_pct'] < 0]

    p_win = len(wins) / len(df)
    p_loss = len(losses) / len(df)
    avg_win = wins['exit_pct'].mean() if len(wins) > 0 else 0
    avg_loss = abs(losses['exit_pct'].mean()) if len(losses) > 0 else 0

    gross = p_win * avg_win - p_loss * avg_loss
    net = gross - slippage_pct * 100

    return {
        'trades': len(df),
        'p_win': p_win,
        'p_loss': p_loss,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'gross_expectancy': gross,
        'net_expectancy': net,
        'mean_return': df['exit_pct'].mean(),
        'median_return': df['exit_pct'].median(),
    }


def main():
    args = parse_args()
    setup_logging(args.log_level)

    random.seed(args.seed)
    np.random.seed(args.seed)

    if args.run_id is None:
        args.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    atr_thresholds = [float(x) for x in args.atr_thresholds.split(',')]

    LOG.info("="*80)
    LOG.info("HMA ATR THRESHOLD OPTIMIZATION")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"ATR thresholds: {atr_thresholds}")
    LOG.info(f"Time window: {args.hour_min}:00 - {args.hour_max}:00")
    LOG.info(f"Period: {args.period}, Inverted: {args.inverted}")
    if args.start_date or args.end_date:
        LOG.info(f"Date range: {args.start_date or 'ANY'} to {args.end_date or 'ANY'}")
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

    LOG.info(f"Testing {len(symbols)} symbols on {len(atr_thresholds)} ATR thresholds")
    LOG.info("")

    # Test each threshold
    results_by_threshold = {}

    for atr_threshold in atr_thresholds:
        LOG.info(f"Testing ATR threshold: {atr_threshold}%")

        all_results = []
        for symbol in symbols:
            results = process_symbol_with_atr_threshold(
                symbol, data_dir, atr_threshold, args.hour_min, args.hour_max, args,
                args.start_date, args.end_date
            )
            all_results.extend(results)

        results_by_threshold[atr_threshold] = all_results

        if all_results:
            output_path = output_dir / f"atr_{atr_threshold:.1f}.csv"
            pd.DataFrame(all_results).to_csv(output_path, index=False)
            LOG.info(f"  {len(all_results)} trades → {output_path}")

    # Summary
    LOG.info("\n" + "="*80)
    LOG.info("ATR THRESHOLD COMPARISON")
    LOG.info("="*80)
    LOG.info(f"{'ATR%':<8} {'Trades':<8} {'Win%':<7} {'AvgWin%':<9} {'AvgLoss%':<9} {'Gross%':<9} {'Net%':<9} {'Status'}")
    LOG.info("-"*80)

    summary_data = []

    for atr_threshold in atr_thresholds:
        results = results_by_threshold.get(atr_threshold, [])
        if not results:
            continue

        df = pd.DataFrame(results)
        stats = calculate_stats(df, args.slippage_pct)

        status = "✅ PASS" if stats['net_expectancy'] >= 0.04 else "⚠️  POSITIVE" if stats['net_expectancy'] >= 0.01 else "❌ FAIL"

        LOG.info(f"{atr_threshold:<8.1f} {stats['trades']:<8} {stats['p_win']*100:<6.1f}% "
                f"{stats['avg_win']:<8.3f}% {stats['avg_loss']:<8.3f}% "
                f"{stats['gross_expectancy']:<8.3f}% {stats['net_expectancy']:<8.3f}% {status}")

        summary_data.append({'atr_threshold': atr_threshold, **stats})

    # Save summary
    summary_df = pd.DataFrame(summary_data)
    summary_path = output_dir / "atr_threshold_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    LOG.info("")
    LOG.info(f"Summary saved to: {summary_path}")

    # Recommendation
    if len(summary_df) > 0:
        best = summary_df.loc[summary_df['net_expectancy'].idxmax()]
        LOG.info("\n" + "="*80)
        LOG.info("RECOMMENDATION")
        LOG.info("="*80)
        LOG.info(f"Best ATR threshold: {best['atr_threshold']:.1f}%")
        LOG.info(f"  Net expectancy: {best['net_expectancy']:.3f}%")
        LOG.info(f"  Trades: {int(best['trades'])}")
        LOG.info(f"  Win rate: {best['p_win']*100:.1f}%")

        if best['net_expectancy'] >= 0.04:
            LOG.info("\n✅ Target achieved!")
        elif best['net_expectancy'] > summary_df[summary_df['atr_threshold'] == 0.5]['net_expectancy'].values[0]:
            improvement = best['net_expectancy'] - summary_df[summary_df['atr_threshold'] == 0.5]['net_expectancy'].values[0]
            LOG.info(f"\n⚠️  Improvement over baseline (0.5%): +{improvement:.3f}%")
        else:
            LOG.info("\n❌ No improvement over baseline ATR threshold (0.5%)")


if __name__ == "__main__":
    main()
