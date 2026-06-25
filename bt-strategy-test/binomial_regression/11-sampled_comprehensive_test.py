#!/usr/bin/env python3
"""
Sampled comprehensive test on all symbols using random time windows.

Strategy:
- For each symbol, take N random time windows (e.g., 30/60/90 days)
- Calculate features only on those windows
- Test strategy on those windows
- Append results to CSV for incremental analysis
- Run multiple times to assess stability

Calibrated to finish in ~10 minutes.
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta
import random
from typing import List, Tuple
import json

LOG = logging.getLogger("sampled_test")
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "config-common/data/m/alpaca/sip"


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Sampled comprehensive test")
    p.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR),
                   help="Directory with symbol CSV files")
    p.add_argument("--output", default="sampled_results.csv",
                   help="Output CSV file (append mode)")
    p.add_argument("--samples-per-symbol", type=int, default=2,
                   help="Number of random windows per symbol per window size")
    p.add_argument("--window-sizes", default="30,60,90",
                   help="Comma-separated window sizes in days")
    p.add_argument("--mag-pct", type=int, default=95)
    p.add_argument("--disp-pct", type=int, default=90)
    p.add_argument("--tp-pct", type=float, default=0.005)
    p.add_argument("--sl-pct", type=float, default=0.001)
    p.add_argument("--min-expectancy", type=float, default=0.001,
                   help="Minimum expectancy threshold (0.1%%)")
    p.add_argument("--run-id", default=None,
                   help="Run identifier (default: timestamp)")
    p.add_argument("--seed", type=int, default=None,
                   help="Random seed for reproducibility")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def calculate_regression_features(log_close: np.ndarray, window: int) -> dict:
    """Calculate regression features for a single window."""
    if len(log_close) < window:
        return None

    # Use last 'window' bars (excluding current if needed)
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


def calculate_forward_outcomes(df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
    """Calculate forward outcomes for TP/SL analysis."""
    df = df.copy()

    # Forward max/min over next N bars
    df[f"forward_high_{horizon}"] = df["high"].shift(-1).rolling(horizon).max()
    df[f"forward_low_{horizon}"] = df["low"].shift(-1).rolling(horizon).min()

    # Percent change from close
    df[f"forward_high_pct_{horizon}"] = (df[f"forward_high_{horizon}"] / df["close"] - 1)
    df[f"forward_low_pct_{horizon}"] = (df[f"forward_low_{horizon}"] / df["close"] - 1)

    return df


def get_random_window(df: pd.DataFrame, window_days: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Get a random time window from the dataframe."""
    # Assume 390 minutes per trading day
    window_bars = window_days * 390

    if len(df) < window_bars + 100:  # Need some buffer
        return None, None

    # Random start index (leave room for window + forward outcomes)
    max_start_idx = len(df) - window_bars - 100
    start_idx = random.randint(60, max_start_idx)  # Start after window 60

    end_idx = start_idx + window_bars

    return df.index[start_idx], df.index[end_idx]


def test_window(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp,
                mag_pct: int, disp_pct: int, tp_pct: float, sl_pct: float) -> dict:
    """
    Test strategy on a specific time window.

    Returns dict with results.
    """
    # Extract window
    window_df = df.loc[start:end].copy()

    if len(window_df) < 100:
        return None

    # Calculate log_close
    window_df["log_close"] = np.log(window_df["close"])

    # Calculate features for each bar
    windows = [5, 20, 60]

    for i in range(len(window_df)):
        if i < 60:
            for w in windows:
                window_df.loc[window_df.index[i], f"slope_{w}"] = np.nan
                window_df.loc[window_df.index[i], f"disp_{w}"] = np.nan
            continue

        # Calculate features using bars up to (but not including) current bar
        log_close_hist = window_df["log_close"].iloc[:i].values

        for w in windows:
            features = calculate_regression_features(log_close_hist, w)
            if features:
                for key, val in features.items():
                    window_df.loc[window_df.index[i], key] = val

    # Calculate forward outcomes
    window_df = calculate_forward_outcomes(window_df, horizon=5)

    # Drop rows with NaN
    window_df = window_df.dropna()

    if len(window_df) < 50:
        return None

    # Calculate percentile thresholds
    p_mag = mag_pct / 100.0
    p_disp = disp_pct / 100.0

    slope_5_abs = window_df["slope_5"].abs()
    slope_20_abs = window_df["slope_20"].abs()
    slope_60_abs = window_df["slope_60"].abs()

    p5 = slope_5_abs.quantile(p_mag)
    p20 = slope_20_abs.quantile(p_mag)
    p60 = slope_60_abs.quantile(p_mag)
    disp_threshold = window_df["disp_60"].quantile(p_disp)

    # Apply filters
    mag_filter = (slope_5_abs > p5) & (slope_20_abs > p20) & (slope_60_abs > p60)
    quality_filter = window_df["disp_60"] > disp_threshold

    combined_filter = mag_filter & quality_filter

    filtered = window_df[combined_filter]

    if len(filtered) < 5:
        return None

    # Classify outcomes with priority: TP > SL > neither
    hit_tp = filtered["forward_high_pct_5"] >= tp_pct
    hit_sl = (filtered["forward_low_pct_5"] <= -sl_pct) & ~hit_tp
    hit_neither = ~hit_tp & ~hit_sl

    n_tp = hit_tp.sum()
    n_sl = hit_sl.sum()
    n_neither = hit_neither.sum()
    n_total = len(filtered)

    p_tp = n_tp / n_total if n_total > 0 else 0
    p_sl = n_sl / n_total if n_total > 0 else 0
    p_neither = n_neither / n_total if n_total > 0 else 0

    # Mean close for neither case
    mean_close_pct = filtered.loc[hit_neither, "forward_high_pct_5"].mean() if n_neither > 0 else 0

    # Expectancy
    exp = p_tp * tp_pct - p_sl * sl_pct + p_neither * mean_close_pct

    # Opportunity rate
    opp_rate = len(filtered) / len(window_df) if len(window_df) > 0 else 0

    return {
        "n_total": n_total,
        "n_tp": n_tp,
        "n_sl": n_sl,
        "n_neither": n_neither,
        "p_tp": p_tp,
        "p_sl": p_sl,
        "p_neither": p_neither,
        "expectancy": exp,
        "opportunity_rate": opp_rate,
        "bars_total": len(window_df),
        "bars_filtered": len(filtered)
    }


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
    window_sizes = [int(x) for x in args.window_sizes.split(",")]

    for window_days in window_sizes:
        for sample_idx in range(args.samples_per_symbol):
            # Get random window
            start, end = get_random_window(df, window_days)

            if start is None:
                LOG.warning(f"  [{symbol}] Cannot get window {window_days}d (sample {sample_idx}), skip")
                continue

            LOG.info(f"  [{symbol}] Testing window {window_days}d sample {sample_idx}: {start.date()} to {end.date()}")

            # Test window
            result = test_window(
                df, start, end,
                args.mag_pct, args.disp_pct,
                args.tp_pct, args.sl_pct
            )

            if result is None:
                LOG.warning(f"  [{symbol}] Window {window_days}d sample {sample_idx} failed, skip")
                continue

            # Add metadata
            result["symbol"] = symbol
            result["window_days"] = window_days
            result["sample_idx"] = sample_idx
            result["start_date"] = start.strftime("%Y-%m-%d")
            result["end_date"] = end.strftime("%Y-%m-%d")
            result["run_id"] = args.run_id

            results.append(result)

            # Log result
            exp_pct = result["expectancy"] * 100
            status = "✅" if result["expectancy"] >= args.min_expectancy else "❌"
            LOG.info(f"  [{symbol}] {status} Window {window_days}d sample {sample_idx}: exp={exp_pct:+.3f}%, "
                    f"P(TP)={result['p_tp']:.1%}, opp={result['opportunity_rate']:.1%}")

    return results


def save_results(results: List[dict], output_path: Path, append: bool = True):
    """Save results to CSV (append or overwrite)."""
    df = pd.DataFrame(results)

    if append and output_path.exists():
        # Append to existing file
        df.to_csv(output_path, mode='a', header=False, index=False)
    else:
        # Create new file
        df.to_csv(output_path, index=False)

    LOG.info(f"💾 Saved {len(results)} results to {output_path}")


def analyze_results(csv_path: Path):
    """Analyze accumulated results and show statistics."""
    if not csv_path.exists():
        LOG.warning(f"No results file found at {csv_path}")
        return

    df = pd.read_csv(csv_path)

    if len(df) == 0:
        LOG.warning("No results to analyze")
        return

    LOG.info("\n" + "="*80)
    LOG.info("RESULTS ANALYSIS")
    LOG.info("="*80)

    # Overall statistics
    LOG.info(f"\n📊 Overall Statistics:")
    LOG.info(f"  Total tests: {len(df)}")
    LOG.info(f"  Unique symbols: {df['symbol'].nunique()}")
    LOG.info(f"  Unique runs: {df['run_id'].nunique()}")
    LOG.info(f"  Mean expectancy: {df['expectancy'].mean()*100:+.3f}%")
    LOG.info(f"  Median expectancy: {df['expectancy'].median()*100:+.3f}%")
    LOG.info(f"  Std expectancy: {df['expectancy'].std()*100:.3f}%")
    LOG.info(f"  Min expectancy: {df['expectancy'].min()*100:+.3f}%")
    LOG.info(f"  Max expectancy: {df['expectancy'].max()*100:+.3f}%")

    # Positive expectancy rate
    positive_rate = (df['expectancy'] > 0).mean()
    above_threshold_rate = (df['expectancy'] >= 0.001).mean()
    LOG.info(f"  % positive: {positive_rate:.1%}")
    LOG.info(f"  % above 0.1%: {above_threshold_rate:.1%}")

    # By symbol
    LOG.info(f"\n📈 By Symbol (top 10 by mean expectancy):")
    by_symbol = df.groupby('symbol').agg({
        'expectancy': ['mean', 'std', 'count'],
        'opportunity_rate': 'mean'
    }).round(4)
    by_symbol.columns = ['exp_mean', 'exp_std', 'n_tests', 'opp_rate']
    by_symbol['exp_mean_pct'] = by_symbol['exp_mean'] * 100
    by_symbol = by_symbol.sort_values('exp_mean', ascending=False)

    print(by_symbol.head(10)[['exp_mean_pct', 'exp_std', 'n_tests', 'opp_rate']].to_string())

    # By window size
    LOG.info(f"\n📊 By Window Size:")
    by_window = df.groupby('window_days').agg({
        'expectancy': ['mean', 'std', 'count']
    }).round(4)
    by_window.columns = ['exp_mean', 'exp_std', 'n_tests']
    by_window['exp_mean_pct'] = by_window['exp_mean'] * 100

    print(by_window[['exp_mean_pct', 'exp_std', 'n_tests']].to_string())

    # Distribution
    LOG.info(f"\n📊 Expectancy Distribution:")
    bins = [-1, -0.001, 0, 0.0005, 0.001, 0.002, 1]
    labels = ['< -0.1%', '-0.1% to 0%', '0% to 0.05%', '0.05% to 0.1%', '0.1% to 0.2%', '> 0.2%']
    df['exp_bin'] = pd.cut(df['expectancy'], bins=bins, labels=labels)

    distribution = df['exp_bin'].value_counts().sort_index()
    for label, count in distribution.items():
        pct = count / len(df) * 100
        LOG.info(f"  {label:20s}: {count:4d} ({pct:5.1f}%)")

    # Stability analysis (if multiple runs)
    if df['run_id'].nunique() > 1:
        LOG.info(f"\n📊 Stability Analysis (by run):")
        by_run = df.groupby('run_id').agg({
            'expectancy': ['mean', 'std', 'count']
        }).round(4)
        by_run.columns = ['exp_mean', 'exp_std', 'n_tests']
        by_run['exp_mean_pct'] = by_run['exp_mean'] * 100

        print(by_run[['exp_mean_pct', 'exp_std', 'n_tests']].to_string())


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
    LOG.info("SAMPLED COMPREHENSIVE TEST")
    LOG.info("="*80)
    LOG.info(f"Run ID: {args.run_id}")
    LOG.info(f"Data directory: {args.data_dir}")
    LOG.info(f"Samples per symbol: {args.samples_per_symbol}")
    LOG.info(f"Window sizes: {args.window_sizes}")
    LOG.info(f"Magnitude percentile: p{args.mag_pct}")
    LOG.info(f"Dispersion percentile: p{args.disp_pct}")
    LOG.info(f"TP/SL: {args.tp_pct*100:.1f}% / {args.sl_pct*100:.1f}%")
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

            # Quick analysis after each symbol
            LOG.info(f"  [{symbol}] Total results so far: {len(all_results)}")

    # Final analysis
    if all_results:
        LOG.info("\n" + "="*80)
        LOG.info("FINAL ANALYSIS")
        LOG.info("="*80)
        analyze_results(output_path)
    else:
        LOG.warning("No results generated")

    LOG.info("\n" + "="*80)
    LOG.info("DONE")
    LOG.info("="*80)


if __name__ == "__main__":
    main()
