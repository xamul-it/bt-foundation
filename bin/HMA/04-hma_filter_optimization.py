#!/usr/bin/env python3
"""
HMA Filter Optimization - Test quality filters to increase edge

Tests various entry filters to improve expectancy:
1. ATR filter (volatility)
2. Volume filter
3. Time-of-day filter
4. Spread filter
5. Combined filters

Goal: Increase gross expectancy from 0.025% to ≥ 0.07% (net 0.04% after 0.03% slippage)
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time, datetime
from typing import Optional, List, Dict, Tuple
import random
import sys
from itertools import product

sys.path.insert(0, str(Path(__file__).parent))

LOG = logging.getLogger("hma_filter_opt")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="HMA Filter Optimization")
    p.add_argument("--symbol", help="Single symbol (default: test all)")
    p.add_argument("--data-dir", default="../../config/data/m/alpaca")
    p.add_argument("--output-dir", default="./filter_analysis")
    p.add_argument("--period", type=int, default=16)
    p.add_argument("--inverted", action="store_true", default=True)
    p.add_argument("--no-inverted", dest="inverted", action="store_false")
    p.add_argument("--sl-pct", type=float, default=0.005)
    p.add_argument("--tp-pct", type=float, default=None)
    p.add_argument("--slippage-pct", type=float, default=0.0003,
                   help="Expected slippage (default: 0.03%)")
    p.add_argument("--samples-per-symbol", type=int, default=50)
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
    atr = true_range.rolling(period).mean()
    return atr


def add_filter_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all filter indicators to dataframe."""
    df = df.copy()

    # ATR and ATR ratio
    df['atr'] = calculate_atr(df, period=14)
    df['atr_pct'] = df['atr'] / df['close'] * 100

    # Volume ratio vs average
    df['volume_sma'] = df['volume'].rolling(20).mean()
    df['volume_ratio'] = df['volume'] / df['volume_sma']

    # Spread (high-low range)
    df['spread_pct'] = (df['high'] - df['low']) / df['close'] * 100

    # Time of day (hour)
    df['hour'] = df.index.hour
    df['minute'] = df.index.minute

    return df


def apply_filters(bar: pd.Series, filters: Dict) -> bool:
    """
    Check if a bar passes all specified filters.

    filters dict can contain:
    - atr_pct_min: minimum ATR %
    - atr_pct_max: maximum ATR %
    - volume_ratio_min: minimum volume ratio vs SMA
    - spread_pct_max: maximum spread %
    - hour_min: minimum hour (e.g., 10 to skip first 30min)
    - hour_max: maximum hour (e.g., 15 to skip last hour)
    """
    if not filters:
        return True

    # ATR filter
    if 'atr_pct_min' in filters:
        if pd.isna(bar.get('atr_pct')) or bar['atr_pct'] < filters['atr_pct_min']:
            return False
    if 'atr_pct_max' in filters:
        if pd.isna(bar.get('atr_pct')) or bar['atr_pct'] > filters['atr_pct_max']:
            return False

    # Volume filter
    if 'volume_ratio_min' in filters:
        if pd.isna(bar.get('volume_ratio')) or bar['volume_ratio'] < filters['volume_ratio_min']:
            return False

    # Spread filter
    if 'spread_pct_max' in filters:
        if pd.isna(bar.get('spread_pct')) or bar['spread_pct'] > filters['spread_pct_max']:
            return False

    # Time of day filter
    if 'hour_min' in filters:
        if bar['hour'] < filters['hour_min']:
            return False
    if 'hour_max' in filters:
        if bar['hour'] >= filters['hour_max']:
            return False

    return True


def simulate_hma_day_with_filters(day_df: pd.DataFrame, period: int, inverted: bool,
                                  sl_pct: float, tp_pct: Optional[float],
                                  filters: Dict) -> List[Dict]:
    """
    Simulate HMA strategy with entry filters.
    Only enter positions that pass the filter criteria.
    """
    # Calculate HMA and filter indicators
    day_df = calculate_hma(day_df, period)
    day_df = add_filter_indicators(day_df)
    day_df = day_df.dropna(subset=['hma'])

    if len(day_df) < period + 20:
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

        # Check TP/SL
        if position is not None:
            hit_tp_or_sl = False

            if position['side'] == 'long':
                if tp_pct and bar['high'] >= position['tp']:
                    exit_price = position['tp']
                    exit_pct = (exit_price / position['entry_price'] - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': exit_price,
                                 'exit_pct': exit_pct * 100, 'outcome': 'tp',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    hit_tp_or_sl = True
                elif sl_pct > 0 and bar['low'] <= position['sl']:
                    exit_price = position['sl']
                    exit_pct = (exit_price / position['entry_price'] - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': exit_price,
                                 'exit_pct': exit_pct * 100, 'outcome': 'sl',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    hit_tp_or_sl = True
            else:  # short
                if tp_pct and bar['low'] <= position['tp']:
                    exit_price = position['tp']
                    exit_pct = (position['entry_price'] / exit_price - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': exit_price,
                                 'exit_pct': exit_pct * 100, 'outcome': 'tp',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    hit_tp_or_sl = True
                elif sl_pct > 0 and bar['high'] >= position['sl']:
                    exit_price = position['sl']
                    exit_pct = (position['entry_price'] / exit_price - 1)
                    trades.append({**position, 'exit_time': bar_ts, 'exit_price': exit_price,
                                 'exit_pct': exit_pct * 100, 'outcome': 'sl',
                                 'bars_held': i - position['entry_idx']})
                    position = None
                    current_direction = 0
                    hit_tp_or_sl = True

            if hit_tp_or_sl:
                continue

        # Process signals with filters
        if signal_long:
            if position is not None and position['side'] == 'short':
                exit_price = bar['close']
                exit_pct = (position['entry_price'] / exit_price - 1)
                trades.append({**position, 'exit_time': bar_ts, 'exit_price': exit_price,
                             'exit_pct': exit_pct * 100, 'outcome': 'signal',
                             'bars_held': i - position['entry_idx']})
                position = None

            # Check filters before entering
            if current_direction != 1 and apply_filters(bar, filters):
                entry_price = bar['close']
                position = {
                    'side': 'long',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': entry_price,
                    'hma': hma_curr,
                    'atr_pct': bar.get('atr_pct', np.nan),
                    'volume_ratio': bar.get('volume_ratio', np.nan),
                    'spread_pct': bar.get('spread_pct', np.nan)
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
                trades.append({**position, 'exit_time': bar_ts, 'exit_price': exit_price,
                             'exit_pct': exit_pct * 100, 'outcome': 'signal',
                             'bars_held': i - position['entry_idx']})
                position = None

            # Check filters before entering
            if current_direction != -1 and apply_filters(bar, filters):
                entry_price = bar['close']
                position = {
                    'side': 'short',
                    'entry_idx': i,
                    'entry_time': bar_ts,
                    'entry_price': entry_price,
                    'hma': hma_curr,
                    'atr_pct': bar.get('atr_pct', np.nan),
                    'volume_ratio': bar.get('volume_ratio', np.nan),
                    'spread_pct': bar.get('spread_pct', np.nan)
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
        trades.append({**position, 'exit_time': last_bar.name, 'exit_price': exit_price,
                     'exit_pct': exit_pct * 100, 'outcome': 'eod',
                     'bars_held': len(day_df) - 1 - position['entry_idx']})

    return trades


def process_symbol_with_filters(symbol: str, data_dir: Path, filters: Dict, args) -> List[Dict]:
    """Process a single symbol with specified filters."""
    csv_path = data_dir / f"{symbol}.csv"

    if not csv_path.exists():
        return []

    df = pd.read_csv(csv_path, parse_dates=['timestamp'])
    df = df.set_index('timestamp').sort_index()
    df = df[df.index.map(is_rth)]

    if len(df) < 1000:
        return []

    trading_days = df.index.normalize().unique().tolist()
    if len(trading_days) < 10:
        return []

    n_samples = min(args.samples_per_symbol, len(trading_days))
    sampled_days = random.sample(trading_days, n_samples)

    results = []
    for day in sampled_days:
        day_df = df[df.index.normalize() == day]
        if len(day_df) < args.period + 50:
            continue

        trades = simulate_hma_day_with_filters(day_df, args.period, args.inverted,
                                              args.sl_pct, args.tp_pct, filters)

        for trade in trades:
            results.append({
                'symbol': symbol,
                'date': day.strftime('%Y-%m-%d'),
                'side': trade['side'],
                'outcome': trade['outcome'],
                'exit_pct': trade['exit_pct'],
                'bars_held': trade['bars_held'],
                'hma': trade['hma'],
                'atr_pct': trade.get('atr_pct', np.nan),
                'volume_ratio': trade.get('volume_ratio', np.nan),
                'spread_pct': trade.get('spread_pct', np.nan),
                'run_id': args.run_id
            })

    return results


def calculate_expectancy_stats(df: pd.DataFrame, slippage_pct: float) -> Dict:
    """Calculate expectancy statistics."""
    if len(df) == 0:
        return {'trades': 0, 'gross_expectancy': 0, 'net_expectancy': -slippage_pct * 100}

    wins = df[df['exit_pct'] > 0]
    losses = df[df['exit_pct'] < 0]

    p_win = len(wins) / len(df)
    p_loss = len(losses) / len(df)
    avg_win = wins['exit_pct'].mean() if len(wins) > 0 else 0
    avg_loss = abs(losses['exit_pct'].mean()) if len(losses) > 0 else 0

    gross_expectancy = p_win * avg_win - p_loss * avg_loss
    net_expectancy = gross_expectancy - slippage_pct * 100

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
    }


def main():
    args = parse_args()
    setup_logging(args.log_level)

    random.seed(args.seed)
    np.random.seed(args.seed)

    if args.run_id is None:
        args.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    LOG.info("="*80)
    LOG.info("HMA FILTER OPTIMIZATION")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"Period: {args.period}, Inverted: {args.inverted}")
    LOG.info(f"Target: Net expectancy ≥ 0.04% (after {args.slippage_pct*100:.3f}% slippage)")
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

    # Define filter configurations to test
    filter_configs = {
        'baseline': {},
        'high_atr': {'atr_pct_min': 0.5},
        'very_high_atr': {'atr_pct_min': 0.8},
        'high_volume': {'volume_ratio_min': 1.5},
        'very_high_volume': {'volume_ratio_min': 2.0},
        'avoid_first_hour': {'hour_min': 10},
        'avoid_edges': {'hour_min': 10, 'hour_max': 15},
        'atr_and_volume': {'atr_pct_min': 0.5, 'volume_ratio_min': 1.5},
        'atr_and_time': {'atr_pct_min': 0.5, 'hour_min': 10, 'hour_max': 15},
        'all_filters': {'atr_pct_min': 0.5, 'volume_ratio_min': 1.5, 'hour_min': 10, 'hour_max': 15},
    }

    results_by_config = {}

    for config_name, filters in filter_configs.items():
        LOG.info(f"Testing filter: {config_name} - {filters}")

        all_results = []
        for symbol in symbols:
            results = process_symbol_with_filters(symbol, data_dir, filters, args)
            all_results.extend(results)

        results_by_config[config_name] = all_results

        if all_results:
            output_path = output_dir / f"filter_{config_name}.csv"
            df = pd.DataFrame(all_results)
            df.to_csv(output_path, index=False)
            LOG.info(f"  {len(all_results)} trades → {output_path}")

    # Comparative summary
    LOG.info("\n" + "="*80)
    LOG.info("FILTER COMPARISON")
    LOG.info("="*80)
    LOG.info(f"{'Filter':<20} {'Trades':<8} {'Win%':<7} {'AvgWin%':<9} {'AvgLoss%':<9} {'Gross%':<9} {'Net%':<9} {'Status'}")
    LOG.info("-"*80)

    summary_data = []

    for config_name in filter_configs.keys():
        results = results_by_config.get(config_name, [])
        if not results:
            continue

        df = pd.DataFrame(results)
        stats = calculate_expectancy_stats(df, args.slippage_pct)

        status = "✅ PASS" if stats['net_expectancy'] >= 0.04 else "⚠️  MARGINAL" if stats['net_expectancy'] >= 0.01 else "❌ FAIL"

        LOG.info(f"{config_name:<20} {stats['trades']:<8} {stats['p_win']*100:<6.1f}% "
                f"{stats['avg_win']:<8.3f}% {stats['avg_loss']:<8.3f}% "
                f"{stats['gross_expectancy']:<8.3f}% {stats['net_expectancy']:<8.3f}% {status}")

        summary_data.append({'filter_config': config_name, **stats})

    # Save summary
    summary_df = pd.DataFrame(summary_data)
    summary_path = output_dir / "filter_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    LOG.info("")
    LOG.info(f"Summary saved to: {summary_path}")

    # Recommendation
    best_config = summary_df.loc[summary_df['net_expectancy'].idxmax()]
    LOG.info("\n" + "="*80)
    LOG.info("RECOMMENDATION")
    LOG.info("="*80)
    LOG.info(f"Best filter: {best_config['filter_config']}")
    LOG.info(f"  Net expectancy: {best_config['net_expectancy']:.3f}%")
    LOG.info(f"  Trades: {int(best_config['trades'])}")
    LOG.info(f"  Win rate: {best_config['p_win']*100:.1f}%")

    if best_config['net_expectancy'] >= 0.04:
        LOG.info("\n✅ Target achieved! This filter meets the 0.04% net expectancy goal!")
    elif best_config['net_expectancy'] >= 0.01:
        LOG.info("\n⚠️  Marginal improvement. Consider combining with parameter optimization.")
    else:
        LOG.info("\n❌ Filters alone insufficient. Need parameter optimization or different approach.")


if __name__ == "__main__":
    main()
