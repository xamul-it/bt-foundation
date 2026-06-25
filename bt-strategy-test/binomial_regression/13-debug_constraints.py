#!/usr/bin/env python3
"""
DEBUG: Test each constraint individually to understand performance impact.

Constraints to test:
1. Exit at close of 5th bar (not forward_high)
2. One position at a time (no overlaps)
3. Close positions at EOD
4. Daily warm-up reset (vs continuous)

Test modes:
- baseline: Original (optimistic forward_high, all features, no constraints)
- fix1: Exit at close only
- fix2: One position at a time
- fix3: EOD close
- fix4: Daily warm-up reset
- realistic: All fixes combined
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import time
from typing import Optional, List

LOG = logging.getLogger("debug")
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "config-common/data/m/alpaca/sip"


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Debug constraint impact")
    p.add_argument("--symbol", required=True, help="Symbol to test")
    p.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    p.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
    p.add_argument("--mag-pct", type=int, default=95)
    p.add_argument("--disp-pct", type=int, default=90)
    p.add_argument("--tp-pct", type=float, default=0.005)
    p.add_argument("--sl-pct", type=float, default=0.001)
    p.add_argument("--hold-bars", type=int, default=5)
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def is_rth(timestamp: pd.Timestamp) -> bool:
    """Check if timestamp is RTH (09:30-16:00)."""
    t = timestamp.time()
    return time(9, 30) <= t < time(16, 0)


def calculate_regression_features(log_close: np.ndarray, window: int) -> Optional[dict]:
    """Calculate regression features."""
    if len(log_close) < window:
        return None

    y = log_close[-window:]
    x = np.arange(window) - (window - 1) / 2
    y_mean = y.mean()
    slope = np.sum((y - y_mean) * x) / np.sum(x ** 2)
    y_pred = y_mean + slope * x
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    rmse = np.sqrt(np.mean((y - y_pred) ** 2))
    disp = np.std(y - y_pred)

    return {
        f"slope_{window}": slope,
        f"r2_{window}": r2,
        f"rmse_{window}": rmse,
        f"disp_{window}": disp
    }


def test_baseline(df: pd.DataFrame, args) -> dict:
    """
    BASELINE: Original method (optimistic).
    - Forward outcomes use forward_high (optimistic)
    - Every candle is independent trade
    - No EOD constraint
    - Continuous warm-up
    """
    LOG.info("  Testing BASELINE (original optimistic method)...")

    df = df.copy()
    df['log_close'] = np.log(df['close'])

    windows = [5, 20, 60]

    # Calculate features with CONTINUOUS warm-up
    for i in range(len(df)):
        if i < 60:
            continue

        log_close_hist = df['log_close'].iloc[:i].values

        for w in windows:
            features = calculate_regression_features(log_close_hist, w)
            if features:
                for key, val in features.items():
                    df.loc[df.index[i], key] = val

    # Calculate forward outcomes (OPTIMISTIC - use forward_high)
    df['forward_high_5'] = df['high'].shift(-1).rolling(args.hold_bars).max()
    df['forward_low_5'] = df['low'].shift(-1).rolling(args.hold_bars).min()
    df['forward_high_pct_5'] = df['forward_high_5'] / df['close'] - 1
    df['forward_low_pct_5'] = df['forward_low_5'] / df['close'] - 1

    df = df.dropna()

    if len(df) < 50:
        return None

    # Calculate thresholds
    p_mag = args.mag_pct / 100.0
    p_disp = args.disp_pct / 100.0

    p5 = df['slope_5'].abs().quantile(p_mag)
    p20 = df['slope_20'].abs().quantile(p_mag)
    p60 = df['slope_60'].abs().quantile(p_mag)
    disp_threshold = df['disp_60'].quantile(p_disp)

    # Apply filters
    mag_filter = (df['slope_5'].abs() > p5) & (df['slope_20'].abs() > p20) & (df['slope_60'].abs() > p60)
    quality_filter = df['disp_60'] > disp_threshold
    filtered = df[mag_filter & quality_filter]

    if len(filtered) < 5:
        return None

    # Classify outcomes
    hit_tp = filtered['forward_high_pct_5'] >= args.tp_pct
    hit_sl = (filtered['forward_low_pct_5'] <= -args.sl_pct) & ~hit_tp
    hit_neither = ~hit_tp & ~hit_sl

    p_tp = hit_tp.mean()
    p_sl = hit_sl.mean()
    p_neither = hit_neither.mean()

    mean_neither = filtered.loc[hit_neither, 'forward_high_pct_5'].mean() if hit_neither.sum() > 0 else 0

    exp = p_tp * args.tp_pct - p_sl * args.sl_pct + p_neither * mean_neither

    return {
        'mode': 'baseline',
        'n_total': len(filtered),
        'p_tp': p_tp,
        'p_sl': p_sl,
        'expectancy': exp,
        'opportunity_rate': len(filtered) / len(df)
    }


def test_fix1_close_exit(df: pd.DataFrame, args) -> dict:
    """
    FIX 1: Exit at CLOSE of 5th bar (not optimistic high).
    - Still every candle is independent
    - Still continuous warm-up
    - No EOD constraint
    """
    LOG.info("  Testing FIX1 (exit at close, not forward_high)...")

    df = df.copy()
    df['log_close'] = np.log(df['close'])

    windows = [5, 20, 60]

    # Continuous warm-up
    for i in range(len(df)):
        if i < 60:
            continue

        log_close_hist = df['log_close'].iloc[:i].values

        for w in windows:
            features = calculate_regression_features(log_close_hist, w)
            if features:
                for key, val in features.items():
                    df.loc[df.index[i], key] = val

    # Calculate forward outcomes (REALISTIC - check TP/SL bar by bar, else close of 5th)
    outcomes = []
    for i in range(len(df) - args.hold_bars):
        entry_price = df['close'].iloc[i]

        hit_tp_flag = False
        hit_sl_flag = False

        for j in range(1, args.hold_bars + 1):
            if i + j >= len(df):
                break

            high = df['high'].iloc[i + j]
            low = df['low'].iloc[i + j]

            if (high / entry_price - 1) >= args.tp_pct:
                hit_tp_flag = True
                break
            if (low / entry_price - 1) <= -args.sl_pct:
                hit_sl_flag = True
                break

        if hit_tp_flag:
            outcome = 'tp'
            exit_pct = args.tp_pct
        elif hit_sl_flag:
            outcome = 'sl'
            exit_pct = -args.sl_pct
        else:
            # Exit at close of 5th bar
            exit_price = df['close'].iloc[min(i + args.hold_bars, len(df) - 1)]
            exit_pct = exit_price / entry_price - 1
            outcome = 'hold'

        outcomes.append({'outcome': outcome, 'exit_pct': exit_pct})

    df = df.iloc[:len(outcomes)]
    df['outcome'] = [o['outcome'] for o in outcomes]
    df['exit_pct'] = [o['exit_pct'] for o in outcomes]

    df = df.dropna(subset=[f'slope_{w}' for w in windows])

    if len(df) < 50:
        return None

    # Calculate thresholds
    p_mag = args.mag_pct / 100.0
    p_disp = args.disp_pct / 100.0

    p5 = df['slope_5'].abs().quantile(p_mag)
    p20 = df['slope_20'].abs().quantile(p_mag)
    p60 = df['slope_60'].abs().quantile(p_mag)
    disp_threshold = df['disp_60'].quantile(p_disp)

    # Apply filters
    mag_filter = (df['slope_5'].abs() > p5) & (df['slope_20'].abs() > p20) & (df['slope_60'].abs() > p60)
    quality_filter = df['disp_60'] > disp_threshold
    filtered = df[mag_filter & quality_filter]

    if len(filtered) < 5:
        return None

    p_tp = (filtered['outcome'] == 'tp').mean()
    p_sl = (filtered['outcome'] == 'sl').mean()
    p_hold = (filtered['outcome'] == 'hold').mean()

    mean_hold_exit = filtered[filtered['outcome'] == 'hold']['exit_pct'].mean() if p_hold > 0 else 0

    exp = p_tp * args.tp_pct - p_sl * args.sl_pct + p_hold * mean_hold_exit

    return {
        'mode': 'fix1_close_exit',
        'n_total': len(filtered),
        'p_tp': p_tp,
        'p_sl': p_sl,
        'p_hold': p_hold,
        'mean_hold_exit': mean_hold_exit,
        'expectancy': exp,
        'opportunity_rate': len(filtered) / len(df)
    }


def test_fix4_daily_warmup(df: pd.DataFrame, args) -> dict:
    """
    FIX 4: Daily warm-up reset.
    - Features calculated only with same-day RTH data
    - Exit at close of 5th bar
    - Still every candle independent
    - Still no EOD constraint
    """
    LOG.info("  Testing FIX4 (daily warm-up reset)...")

    # Filter RTH only
    df = df.copy()
    df['is_rth'] = df.index.map(is_rth)
    df = df[df['is_rth']].drop(columns=['is_rth'])

    df['log_close'] = np.log(df['close'])

    windows = [5, 20, 60]
    trading_days = df.index.normalize().unique()

    # Calculate features per day
    for day in trading_days:
        day_mask = df.index.normalize() == day
        day_indices = df.index[day_mask]

        for bar_idx in range(len(df)):
            bar_ts = df.index[bar_idx]

            if bar_ts.normalize() != day:
                continue

            # Today's data up to this bar
            today_mask = (df.index.normalize() == day) & (df.index < bar_ts)
            today_hist = df.loc[today_mask, 'log_close'].values

            for w in windows:
                features = calculate_regression_features(today_hist, w)
                if features:
                    for key, val in features.items():
                        df.loc[bar_ts, key] = val

    # Calculate outcomes (close exit)
    outcomes = []
    for i in range(len(df) - args.hold_bars):
        entry_price = df['close'].iloc[i]

        hit_tp_flag = False
        hit_sl_flag = False

        for j in range(1, args.hold_bars + 1):
            if i + j >= len(df):
                break

            high = df['high'].iloc[i + j]
            low = df['low'].iloc[i + j]

            if (high / entry_price - 1) >= args.tp_pct:
                hit_tp_flag = True
                break
            if (low / entry_price - 1) <= -args.sl_pct:
                hit_sl_flag = True
                break

        if hit_tp_flag:
            outcome = 'tp'
            exit_pct = args.tp_pct
        elif hit_sl_flag:
            outcome = 'sl'
            exit_pct = -args.sl_pct
        else:
            exit_price = df['close'].iloc[min(i + args.hold_bars, len(df) - 1)]
            exit_pct = exit_price / entry_price - 1
            outcome = 'hold'

        outcomes.append({'outcome': outcome, 'exit_pct': exit_pct})

    df = df.iloc[:len(outcomes)]
    df['outcome'] = [o['outcome'] for o in outcomes]
    df['exit_pct'] = [o['exit_pct'] for o in outcomes]

    df = df.dropna(subset=[f'slope_{w}' for w in windows])

    if len(df) < 50:
        return None

    # Calculate thresholds
    p_mag = args.mag_pct / 100.0
    p_disp = args.disp_pct / 100.0

    p5 = df['slope_5'].abs().quantile(p_mag)
    p20 = df['slope_20'].abs().quantile(p_mag)
    p60 = df['slope_60'].abs().quantile(p_mag)
    disp_threshold = df['disp_60'].quantile(p_disp)

    # Apply filters
    mag_filter = (df['slope_5'].abs() > p5) & (df['slope_20'].abs() > p20) & (df['slope_60'].abs() > p60)
    quality_filter = df['disp_60'] > disp_threshold
    filtered = df[mag_filter & quality_filter]

    if len(filtered) < 5:
        return None

    p_tp = (filtered['outcome'] == 'tp').mean()
    p_sl = (filtered['outcome'] == 'sl').mean()
    p_hold = (filtered['outcome'] == 'hold').mean()

    mean_hold_exit = filtered[filtered['outcome'] == 'hold']['exit_pct'].mean() if p_hold > 0 else 0

    exp = p_tp * args.tp_pct - p_sl * args.sl_pct + p_hold * mean_hold_exit

    return {
        'mode': 'fix4_daily_warmup',
        'n_total': len(filtered),
        'p_tp': p_tp,
        'p_sl': p_sl,
        'p_hold': p_hold,
        'mean_hold_exit': mean_hold_exit,
        'expectancy': exp,
        'opportunity_rate': len(filtered) / len(df)
    }


def test_eod_only_exit(df: pd.DataFrame, args) -> dict:
    """
    EOD-ONLY EXIT (most realistic):
    - Continuous warm-up (cross-day)
    - Wait for TP or SL (no 5-bar limit)
    - Close at EOD if not hit
    - One position at a time
    - RTH bars only
    """
    LOG.info("  Testing EOD-ONLY EXIT (wait for TP/SL, close only at EOD)...")

    # Filter RTH only
    df = df.copy()
    df['is_rth'] = df.index.map(is_rth)
    df = df[df['is_rth']].drop(columns=['is_rth'])

    df['log_close'] = np.log(df['close'])

    windows = [5, 20, 60]

    # Calculate features with CONTINUOUS warm-up
    for i in range(len(df)):
        if i < 60:
            continue

        log_close_hist = df['log_close'].iloc[:i].values

        for w in windows:
            features = calculate_regression_features(log_close_hist, w)
            if features:
                for key, val in features.items():
                    df.loc[df.index[i], key] = val

    df = df.dropna(subset=[f'slope_{w}' for w in windows])

    if len(df) < 50:
        return None

    # Calculate thresholds
    p_mag = args.mag_pct / 100.0
    p_disp = args.disp_pct / 100.0

    p5 = df['slope_5'].abs().quantile(p_mag)
    p20 = df['slope_20'].abs().quantile(p_mag)
    p60 = df['slope_60'].abs().quantile(p_mag)
    disp_threshold = df['disp_60'].quantile(p_disp)

    # Apply filters
    mag_filter = (df['slope_5'].abs() > p5) & (df['slope_20'].abs() > p20) & (df['slope_60'].abs() > p60)
    quality_filter = df['disp_60'] > disp_threshold
    candidates = df[mag_filter & quality_filter].copy()

    if len(candidates) < 5:
        return None

    # Simulate trading: wait for TP/SL or EOD
    trading_days = df.index.normalize().unique()
    trades = []
    position_open = False
    position_end_idx = -1

    for i in range(len(df)):
        bar_ts = df.index[i]

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
        day_mask = df.index.normalize() == bar_day
        eod_indices = df.index[day_mask]
        if len(eod_indices) == 0:
            continue
        eod_idx = eod_indices[-1]
        eod_idx_int = df.index.get_loc(eod_idx)

        # Simulate position: NO TIME LIMIT, only EOD
        entry_price = df['close'].iloc[i]
        hit_tp = False
        hit_sl = False
        exit_idx = None
        exit_pct = 0
        outcome = None

        # Check ALL bars until EOD
        for j in range(i + 1, eod_idx_int + 1):
            high = df['high'].iloc[j]
            low = df['low'].iloc[j]

            # Check TP first (priority)
            if (high / entry_price - 1) >= args.tp_pct:
                hit_tp = True
                exit_idx = j
                exit_pct = args.tp_pct
                outcome = 'tp'
                break

            # Check SL
            if (low / entry_price - 1) <= -args.sl_pct:
                hit_sl = True
                exit_idx = j
                exit_pct = -args.sl_pct
                outcome = 'sl'
                break

        # If no TP/SL hit, close at EOD
        if not hit_tp and not hit_sl:
            exit_idx = eod_idx_int
            exit_price = df['close'].iloc[eod_idx_int]
            exit_pct = exit_price / entry_price - 1
            outcome = 'eod'

        # Mark position as open until exit
        position_open = True
        position_end_idx = exit_idx if exit_idx else eod_idx_int

        trades.append({
            'entry_idx': i,
            'outcome': outcome,
            'exit_pct': exit_pct,
            'bars_held': (exit_idx - i) if exit_idx else (eod_idx_int - i)
        })

    if len(trades) == 0:
        return None

    # Calculate statistics
    trades_df = pd.DataFrame(trades)

    p_tp = (trades_df['outcome'] == 'tp').mean()
    p_sl = (trades_df['outcome'] == 'sl').mean()
    p_eod = (trades_df['outcome'] == 'eod').mean()

    mean_eod = trades_df[trades_df['outcome'] == 'eod']['exit_pct'].mean() if p_eod > 0 else 0

    exp = (p_tp * args.tp_pct -
           p_sl * args.sl_pct +
           p_eod * mean_eod)

    return {
        'mode': 'eod_only_exit',
        'n_total': len(trades_df),
        'p_tp': p_tp,
        'p_sl': p_sl,
        'p_eod': p_eod,
        'mean_eod_exit': mean_eod,
        'expectancy': exp,
        'opportunity_rate': len(trades_df) / len(df)
    }


def test_combined_realistic(df: pd.DataFrame, args) -> dict:
    """
    COMBINED REALISTIC:
    - Continuous warm-up (cross-day)
    - Exit at close of 5th bar
    - One position at a time
    - Close at EOD
    - RTH bars only
    """
    LOG.info("  Testing COMBINED REALISTIC (continuous warmup + realistic exits + constraints)...")

    # Filter RTH only
    df = df.copy()
    df['is_rth'] = df.index.map(is_rth)
    df = df[df['is_rth']].drop(columns=['is_rth'])

    df['log_close'] = np.log(df['close'])

    windows = [5, 20, 60]

    # Calculate features with CONTINUOUS warm-up (cross-day)
    for i in range(len(df)):
        if i < 60:
            continue

        log_close_hist = df['log_close'].iloc[:i].values

        for w in windows:
            features = calculate_regression_features(log_close_hist, w)
            if features:
                for key, val in features.items():
                    df.loc[df.index[i], key] = val

    df = df.dropna(subset=[f'slope_{w}' for w in windows])

    if len(df) < 50:
        return None

    # Calculate thresholds
    p_mag = args.mag_pct / 100.0
    p_disp = args.disp_pct / 100.0

    p5 = df['slope_5'].abs().quantile(p_mag)
    p20 = df['slope_20'].abs().quantile(p_mag)
    p60 = df['slope_60'].abs().quantile(p_mag)
    disp_threshold = df['disp_60'].quantile(p_disp)

    # Apply filters
    mag_filter = (df['slope_5'].abs() > p5) & (df['slope_20'].abs() > p20) & (df['slope_60'].abs() > p60)
    quality_filter = df['disp_60'] > disp_threshold
    candidates = df[mag_filter & quality_filter].copy()

    if len(candidates) < 5:
        return None

    # Simulate trading with realistic constraints
    trading_days = df.index.normalize().unique()
    trades = []
    position_open = False
    position_end_idx = -1

    for i in range(len(df)):
        bar_ts = df.index[i]

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
        day_mask = df.index.normalize() == bar_day
        eod_indices = df.index[day_mask]
        if len(eod_indices) == 0:
            continue
        eod_idx = eod_indices[-1]
        eod_idx_int = df.index.get_loc(eod_idx)

        # Simulate position
        entry_price = df['close'].iloc[i]
        hit_tp = False
        hit_sl = False
        exit_idx = None
        exit_pct = 0
        outcome = None

        # Check each bar forward (up to hold_bars or EOD)
        max_idx = min(i + args.hold_bars, eod_idx_int, len(df) - 1)

        for j in range(i + 1, max_idx + 1):
            high = df['high'].iloc[j]
            low = df['low'].iloc[j]

            # Check TP first (priority)
            if (high / entry_price - 1) >= args.tp_pct:
                hit_tp = True
                exit_idx = j
                exit_pct = args.tp_pct
                outcome = 'tp'
                break

            # Check SL
            if (low / entry_price - 1) <= -args.sl_pct:
                hit_sl = True
                exit_idx = j
                exit_pct = -args.sl_pct
                outcome = 'sl'
                break

        # No TP/SL hit
        if not hit_tp and not hit_sl:
            if max_idx == eod_idx_int:
                # EOD exit at close
                exit_idx = eod_idx_int
                exit_price = df['close'].iloc[eod_idx_int]
                exit_pct = exit_price / entry_price - 1
                outcome = 'eod'
            else:
                # Hold bars limit - exit at close of 5th bar
                exit_idx = max_idx
                exit_price = df['close'].iloc[max_idx]
                exit_pct = exit_price / entry_price - 1
                outcome = 'hold'

        # Mark position as open
        position_open = True
        position_end_idx = exit_idx if exit_idx else i + args.hold_bars

        trades.append({
            'entry_idx': i,
            'outcome': outcome,
            'exit_pct': exit_pct,
            'bars_held': (exit_idx - i) if exit_idx else args.hold_bars
        })

    if len(trades) == 0:
        return None

    # Calculate statistics
    trades_df = pd.DataFrame(trades)

    p_tp = (trades_df['outcome'] == 'tp').mean()
    p_sl = (trades_df['outcome'] == 'sl').mean()
    p_hold = (trades_df['outcome'] == 'hold').mean()
    p_eod = (trades_df['outcome'] == 'eod').mean()

    mean_hold = trades_df[trades_df['outcome'] == 'hold']['exit_pct'].mean() if p_hold > 0 else 0
    mean_eod = trades_df[trades_df['outcome'] == 'eod']['exit_pct'].mean() if p_eod > 0 else 0

    exp = (p_tp * args.tp_pct -
           p_sl * args.sl_pct +
           p_hold * mean_hold +
           p_eod * mean_eod)

    return {
        'mode': 'combined_realistic',
        'n_total': len(trades_df),
        'p_tp': p_tp,
        'p_sl': p_sl,
        'p_hold': p_hold,
        'p_eod': p_eod,
        'mean_hold_exit': mean_hold,
        'mean_eod_exit': mean_eod,
        'expectancy': exp,
        'opportunity_rate': len(trades_df) / len(df)
    }


def main():
    args = parse_args()
    setup_logging(args.log_level)

    LOG.info("="*80)
    LOG.info("DEBUG CONSTRAINT IMPACT")
    LOG.info("="*80)
    LOG.info(f"Symbol: {args.symbol}")
    LOG.info(f"Period: {args.start_date} to {args.end_date}")
    LOG.info(f"TP/SL: {args.tp_pct*100:.1f}% / {args.sl_pct*100:.1f}%")
    LOG.info("")

    # Load data
    data_dir = Path(args.data_dir)
    csv_path = data_dir / f"{args.symbol}.csv"

    df = pd.read_csv(csv_path, parse_dates=['timestamp'])
    df = df.set_index('timestamp').sort_index()

    # Filter date range
    df = df.loc[args.start_date:args.end_date]

    LOG.info(f"Loaded {len(df)} bars")
    LOG.info("")

    # Run tests
    results = []

    # Baseline
    result = test_baseline(df, args)
    if result:
        results.append(result)

    # Fix 1
    result = test_fix1_close_exit(df, args)
    if result:
        results.append(result)

    # Fix 4
    result = test_fix4_daily_warmup(df, args)
    if result:
        results.append(result)

    # EOD-only exit
    result = test_eod_only_exit(df, args)
    if result:
        results.append(result)

    # Combined realistic
    result = test_combined_realistic(df, args)
    if result:
        results.append(result)

    # Display results
    LOG.info("")
    LOG.info("="*80)
    LOG.info("RESULTS COMPARISON")
    LOG.info("="*80)

    results_df = pd.DataFrame(results)
    results_df['exp_pct'] = results_df['expectancy'] * 100
    results_df['p_tp_pct'] = results_df['p_tp'] * 100
    results_df['p_sl_pct'] = results_df['p_sl'] * 100
    results_df['opp_pct'] = results_df['opportunity_rate'] * 100

    print("\n")
    print(results_df[['mode', 'n_total', 'exp_pct', 'p_tp_pct', 'p_sl_pct', 'opp_pct']].to_string(index=False))

    # Calculate degradation
    if len(results) > 1:
        baseline_exp = results[0]['expectancy']

        print("\n")
        print("DEGRADATION vs BASELINE:")
        for r in results[1:]:
            delta = r['expectancy'] - baseline_exp
            delta_pct = (delta / baseline_exp * 100) if baseline_exp != 0 else 0
            print(f"  {r['mode']:20s}: {delta*100:+.3f}% ({delta_pct:+.1f}%)")


if __name__ == "__main__":
    main()
