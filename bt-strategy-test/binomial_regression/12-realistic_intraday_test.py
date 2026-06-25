#!/usr/bin/env python3
"""
REALISTIC INTRADAY SIMULATION

Fixes:
1. Forward outcome = close of 5th bar (not optimistic high)
2. No overlapping positions (one position at a time)
3. Close all positions at EOD (16:00)
4. Warm-up uses only RTH data from same trading day

RTH = Regular Trading Hours = 09:30 - 16:00 EST
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time
import random
from typing import List, Tuple, Optional

LOG = logging.getLogger("realistic_test")
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "config-common/data/m/alpaca/sip"


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Realistic intraday test")
    p.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR),
                   help="Directory with symbol CSV files")
    p.add_argument("--output", default="realistic_results.csv",
                   help="Output CSV file (append mode)")
    p.add_argument("--samples-per-symbol", type=int, default=2,
                   help="Number of random windows per symbol")
    p.add_argument("--window-days", type=int, default=30,
                   help="Window size in trading days")
    p.add_argument("--mag-pct", type=int, default=95)
    p.add_argument("--disp-pct", type=int, default=90)
    p.add_argument("--tp-pct", type=float, default=0.005)
    p.add_argument("--sl-pct", type=float, default=0.001)
    p.add_argument("--hold-bars", type=int, default=5,
                   help="Max hold time in bars if no TP/SL hit")
    p.add_argument("--min-expectancy", type=float, default=0.001,
                   help="Minimum expectancy threshold (0.1%%)")
    p.add_argument("--run-id", default=None,
                   help="Run identifier (default: timestamp)")
    p.add_argument("--seed", type=int, default=None,
                   help="Random seed for reproducibility")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def is_rth(timestamp: pd.Timestamp) -> bool:
    """Check if timestamp is within Regular Trading Hours (09:30-16:00 EST)."""
    t = timestamp.time()
    return time(9, 30) <= t < time(16, 0)


def filter_rth(df: pd.DataFrame) -> pd.DataFrame:
    """Filter dataframe to only RTH bars."""
    df = df.copy()
    df['is_rth'] = df.index.map(is_rth)
    return df[df['is_rth']].drop(columns=['is_rth'])


def get_trading_days(df: pd.DataFrame) -> List[pd.Timestamp]:
    """Get list of unique trading days."""
    return df.index.normalize().unique().tolist()


def calculate_regression_features(log_close: np.ndarray, window: int) -> Optional[dict]:
    """Calculate regression features for a single window."""
    if len(log_close) < window:
        return None

    # Use last 'window' bars
    y = log_close[-window:]

    # Centered x
    x = np.arange(window) - (window - 1) / 2

    # Linear regression
    y_mean = y.mean()
    slope = np.sum((y - y_mean) * x) / np.sum(x ** 2)

    # Predictions
    y_pred = y_mean + slope * x

    # R²
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

    # RMSE (in log space)
    rmse = np.sqrt(np.mean((y - y_pred) ** 2))

    # Dispersion (std of residuals)
    disp = np.std(y - y_pred)

    return {
        f"slope_{window}": slope,
        f"r2_{window}": r2,
        f"rmse_{window}": rmse,
        f"disp_{window}": disp
    }


def simulate_position(entry_idx: int, df: pd.DataFrame, tp_pct: float, sl_pct: float,
                     hold_bars: int, eod_idx: int) -> dict:
    """
    Simulate a single position with realistic exit logic.

    Returns:
        dict with outcome: 'tp', 'sl', 'hold_exit', 'eod_exit', 'no_exit'
        and exit_pct: realized return
    """
    entry_price = df['close'].iloc[entry_idx]

    # Check each bar forward (up to hold_bars or EOD)
    max_idx = min(entry_idx + hold_bars, eod_idx, len(df) - 1)

    for i in range(entry_idx + 1, max_idx + 1):
        high = df['high'].iloc[i]
        low = df['low'].iloc[i]

        # Check TP first (priority)
        if (high / entry_price - 1) >= tp_pct:
            return {'outcome': 'tp', 'exit_pct': tp_pct, 'bars_held': i - entry_idx}

        # Check SL
        if (low / entry_price - 1) <= -sl_pct:
            return {'outcome': 'sl', 'exit_pct': -sl_pct, 'bars_held': i - entry_idx}

    # No TP/SL hit
    if max_idx == eod_idx:
        # EOD exit at close
        exit_price = df['close'].iloc[eod_idx]
        exit_pct = exit_price / entry_price - 1
        return {'outcome': 'eod_exit', 'exit_pct': exit_pct, 'bars_held': eod_idx - entry_idx}
    elif max_idx == entry_idx + hold_bars:
        # Hold bars limit - exit at close of 5th bar
        exit_price = df['close'].iloc[max_idx]
        exit_pct = exit_price / entry_price - 1
        return {'outcome': 'hold_exit', 'exit_pct': exit_pct, 'bars_held': hold_bars}
    else:
        # Reached end of data
        return {'outcome': 'no_exit', 'exit_pct': 0, 'bars_held': max_idx - entry_idx}


def test_window(df: pd.DataFrame, start_day: pd.Timestamp, end_day: pd.Timestamp,
                mag_pct: int, disp_pct: int, tp_pct: float, sl_pct: float,
                hold_bars: int) -> Optional[dict]:
    """
    Test strategy on a window of trading days with realistic intraday logic.

    Key features:
    - Only RTH data (09:30-16:00)
    - Warm-up resets each trading day
    - One position at a time
    - Close at EOD
    """
    # Filter to date range and RTH only
    mask = (df.index >= start_day) & (df.index <= end_day)
    window_df = df[mask].copy()
    window_df = filter_rth(window_df)

    if len(window_df) < 500:
        return None

    # Add log_close
    window_df['log_close'] = np.log(window_df['close'])

    # Get trading days
    trading_days = get_trading_days(window_df)

    if len(trading_days) < 3:
        return None

    # Calculate features per trading day
    windows = [5, 20, 60]

    for day in trading_days:
        # Get bars for this day
        day_mask = window_df.index.normalize() == day
        day_indices = window_df.index[day_mask]

        if len(day_indices) == 0:
            continue

        # For each bar in this day, calculate features using ONLY previous bars from TODAY
        for bar_idx in range(len(window_df)):
            bar_ts = window_df.index[bar_idx]

            if bar_ts.normalize() != day:
                continue

            # Get today's data up to (but not including) this bar
            today_mask = (window_df.index.normalize() == day) & (window_df.index < bar_ts)
            today_hist = window_df.loc[today_mask, 'log_close'].values

            # Calculate features for each window
            for w in windows:
                features = calculate_regression_features(today_hist, w)
                if features:
                    for key, val in features.items():
                        window_df.loc[bar_ts, key] = val

    # Drop rows with NaN features
    window_df = window_df.dropna(subset=[f'slope_{w}' for w in windows])

    if len(window_df) < 100:
        return None

    # Calculate percentile thresholds
    p_mag = mag_pct / 100.0
    p_disp = disp_pct / 100.0

    slope_5_abs = window_df['slope_5'].abs()
    slope_20_abs = window_df['slope_20'].abs()
    slope_60_abs = window_df['slope_60'].abs()

    p5 = slope_5_abs.quantile(p_mag)
    p20 = slope_20_abs.quantile(p_mag)
    p60 = slope_60_abs.quantile(p_mag)
    disp_threshold = window_df['disp_60'].quantile(p_disp)

    # Apply filters
    mag_filter = (slope_5_abs > p5) & (slope_20_abs > p20) & (slope_60_abs > p60)
    quality_filter = window_df['disp_60'] > disp_threshold
    combined_filter = mag_filter & quality_filter

    # Get candidate signals
    candidates = window_df[combined_filter].copy()

    if len(candidates) < 5:
        return None

    # Simulate trading with realistic constraints
    trades = []
    position_open = False
    position_end_idx = -1

    for i in range(len(window_df)):
        bar_ts = window_df.index[i]

        # Skip if position already open
        if position_open and i < position_end_idx:
            continue
        else:
            position_open = False

        # Check if this bar has a signal
        if bar_ts not in candidates.index:
            continue

        # Find EOD for this day
        bar_day = bar_ts.normalize()
        day_mask = window_df.index.normalize() == bar_day
        eod_idx = window_df.index[day_mask][-1]
        eod_idx_int = window_df.index.get_loc(eod_idx)

        # Simulate position
        result = simulate_position(i, window_df, tp_pct, sl_pct, hold_bars, eod_idx_int)

        # Mark position as open
        position_open = True
        position_end_idx = i + result['bars_held']

        trades.append({
            'entry_idx': i,
            'entry_time': bar_ts,
            'outcome': result['outcome'],
            'exit_pct': result['exit_pct'],
            'bars_held': result['bars_held']
        })

    if len(trades) == 0:
        return None

    # Calculate statistics
    trades_df = pd.DataFrame(trades)

    n_tp = (trades_df['outcome'] == 'tp').sum()
    n_sl = (trades_df['outcome'] == 'sl').sum()
    n_hold_exit = (trades_df['outcome'] == 'hold_exit').sum()
    n_eod_exit = (trades_df['outcome'] == 'eod_exit').sum()
    n_total = len(trades_df)

    p_tp = n_tp / n_total
    p_sl = n_sl / n_total
    p_hold = n_hold_exit / n_total
    p_eod = n_eod_exit / n_total

    # Mean exit for hold/eod
    mean_hold_exit = trades_df[trades_df['outcome'] == 'hold_exit']['exit_pct'].mean() if n_hold_exit > 0 else 0
    mean_eod_exit = trades_df[trades_df['outcome'] == 'eod_exit']['exit_pct'].mean() if n_eod_exit > 0 else 0

    # Expectancy
    exp = (p_tp * tp_pct -
           p_sl * sl_pct +
           p_hold * mean_hold_exit +
           p_eod * mean_eod_exit)

    # Opportunity rate
    total_rth_bars = len(window_df)
    opp_rate = n_total / total_rth_bars if total_rth_bars > 0 else 0

    return {
        'n_total': n_total,
        'n_tp': n_tp,
        'n_sl': n_sl,
        'n_hold_exit': n_hold_exit,
        'n_eod_exit': n_eod_exit,
        'p_tp': p_tp,
        'p_sl': p_sl,
        'p_hold': p_hold,
        'p_eod': p_eod,
        'mean_hold_exit': mean_hold_exit,
        'mean_eod_exit': mean_eod_exit,
        'expectancy': exp,
        'opportunity_rate': opp_rate,
        'bars_total': total_rth_bars,
        'trades_total': n_total
    }


def get_random_window(df: pd.DataFrame, window_days: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Get random window of trading days."""
    # Filter to RTH only
    rth_df = filter_rth(df)

    if len(rth_df) < 100:
        return None, None

    # Get trading days
    trading_days = get_trading_days(rth_df)

    if len(trading_days) < window_days + 5:
        return None, None

    # Random start day
    max_start = len(trading_days) - window_days - 1
    start_idx = random.randint(0, max_start)

    start_day = trading_days[start_idx]
    end_day = trading_days[start_idx + window_days - 1]

    return start_day, end_day


def process_symbol(symbol: str, data_dir: Path, args) -> List[dict]:
    """Process a single symbol with random sampling."""
    csv_path = data_dir / f"{symbol}.csv"

    if not csv_path.exists():
        LOG.warning(f"  [{symbol}] File not found, skip")
        return []

    # Load data
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df = df.set_index("timestamp").sort_index()

    if len(df) < 1000:
        LOG.warning(f"  [{symbol}] Too few rows ({len(df)}), skip")
        return []

    LOG.info(f"  [{symbol}] Loaded {len(df)} rows from {df.index.min().date()} to {df.index.max().date()}")

    results = []

    for sample_idx in range(args.samples_per_symbol):
        # Get random window
        start_day, end_day = get_random_window(df, args.window_days)

        if start_day is None:
            LOG.warning(f"  [{symbol}] Cannot get window {args.window_days}d (sample {sample_idx}), skip")
            continue

        LOG.info(f"  [{symbol}] Testing window {args.window_days}d sample {sample_idx}: {start_day.date()} to {end_day.date()}")

        # Test window
        result = test_window(
            df, start_day, end_day,
            args.mag_pct, args.disp_pct,
            args.tp_pct, args.sl_pct,
            args.hold_bars
        )

        if result is None:
            LOG.warning(f"  [{symbol}] Window {args.window_days}d sample {sample_idx} failed, skip")
            continue

        # Add metadata
        result['symbol'] = symbol
        result['window_days'] = args.window_days
        result['sample_idx'] = sample_idx
        result['start_date'] = start_day.strftime("%Y-%m-%d")
        result['end_date'] = end_day.strftime("%Y-%m-%d")
        result['run_id'] = args.run_id

        results.append(result)

        # Log result
        exp_pct = result['expectancy'] * 100
        status = "✅" if result['expectancy'] >= args.min_expectancy else "❌"
        LOG.info(f"  [{symbol}] {status} exp={exp_pct:+.3f}%, P(TP)={result['p_tp']:.1%}, "
                f"trades={result['n_total']}, opp={result['opportunity_rate']:.2%}")

    return results


def save_results(results: List[dict], output_path: Path, append: bool = True):
    """Save results to CSV."""
    df = pd.DataFrame(results)

    if append and output_path.exists():
        df.to_csv(output_path, mode='a', header=False, index=False)
    else:
        df.to_csv(output_path, index=False)

    LOG.info(f"💾 Saved {len(results)} results to {output_path}")


def main():
    args = parse_args()
    setup_logging(args.log_level)

    # Set random seed
    if args.seed is not None:
        random.seed(args.seed)
        np.random.seed(args.seed)
        LOG.info(f"🎲 Random seed: {args.seed}")

    # Generate run_id if not provided
    if args.run_id is None:
        from datetime import datetime
        args.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    LOG.info("="*80)
    LOG.info("REALISTIC INTRADAY SIMULATION")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"Window size: {args.window_days} trading days")
    LOG.info(f"Samples per symbol: {args.samples_per_symbol}")
    LOG.info(f"Magnitude percentile: p{args.mag_pct}")
    LOG.info(f"Dispersion percentile: p{args.disp_pct}")
    LOG.info(f"TP/SL: {args.tp_pct*100:.1f}% / {args.sl_pct*100:.1f}%")
    LOG.info(f"Hold bars: {args.hold_bars}")
    LOG.info(f"Min expectancy target: {args.min_expectancy*100:.1f}%")
    LOG.info("")

    data_dir = Path(args.data_dir)
    output_path = Path(args.output)

    # Get all symbols
    csv_files = sorted(data_dir.glob("*.csv"))
    symbols = [f.stem for f in csv_files]

    LOG.info(f"Found {len(symbols)} symbols:")
    if len(symbols) <= 10:
        LOG.info(f"  {', '.join(symbols)}")
    else:
        LOG.info(f"  {', '.join(symbols[:10])}, ... (+{len(symbols)-10} more)")
    LOG.info("")

    # Process all symbols
    all_results = []

    for symbol in symbols:
        results = process_symbol(symbol, data_dir, args)

        # Save results after each symbol (incremental)
        if results:
            append = output_path.exists()
            save_results(results, output_path, append=append)
            all_results.extend(results)

            LOG.info(f"  [{symbol}] Total results so far: {len(all_results)}")

    # Final summary
    if all_results:
        LOG.info("\n" + "="*80)
        LOG.info(f"COMPLETED: {len(all_results)} tests on {len(set(r['symbol'] for r in all_results))} symbols")
        LOG.info("="*80)
    else:
        LOG.warning("No results generated")


if __name__ == "__main__":
    main()
