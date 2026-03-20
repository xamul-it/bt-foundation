#!/usr/bin/env python3
"""
Process all symbols from alpaca data folder, calculate features, and run comprehensive walk-forward test.

Step 1: Calculate regression features for all symbols
Step 2: Run comprehensive walk-forward test with min expectancy 0.1%
"""

import argparse
import logging
from pathlib import Path
import subprocess
import sys

LOG = logging.getLogger("process_all_and_test")


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Process all symbols and run comprehensive test")
    p.add_argument("--data-dir", default="../../config/data/m/alpaca",
                   help="Directory with symbol CSV files")
    p.add_argument("--skip-processing", action="store_true",
                   help="Skip feature calculation (if already done)")
    p.add_argument("--mag-pct", type=int, default=95)
    p.add_argument("--disp-pct", type=int, default=90)
    p.add_argument("--min-expectancy", type=float, default=0.001,
                   help="Minimum expectancy required (default: 0.1%%)")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def get_all_symbols(data_dir: Path) -> list:
    """Get all symbols from data directory."""
    csv_files = sorted(data_dir.glob("*.csv"))
    symbols = [f.stem for f in csv_files]
    return symbols


def process_symbol(symbol: str, data_dir: Path):
    """
    Process a single symbol: load data, calculate features, save results.

    This is a simplified version of 01-fast_backtest.py focused on feature calculation.
    """
    import pandas as pd
    import numpy as np

    LOG.info(f"  [{symbol}] Loading data...")

    # Load data
    csv_path = data_dir / f"{symbol}.csv"
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df = df.set_index("timestamp").sort_index()

    if len(df) < 500:
        LOG.warning(f"  [{symbol}] Too few rows ({len(df)}), skip")
        return False

    LOG.info(f"  [{symbol}] Loaded {len(df)} rows from {df.index.min().date()} to {df.index.max().date()}")

    # Calculate log prices
    df["log_close"] = np.log(df["close"])

    # Calculate regression features for multiple windows
    windows = [5, 20, 60]

    for window in windows:
        LOG.info(f"  [{symbol}] Calculating regression features (window={window})...")

        slopes = []
        r2s = []
        disps = []
        rmses = []

        log_close = df["log_close"].values

        for i in range(len(df)):
            if i < window:
                slopes.append(np.nan)
                r2s.append(np.nan)
                disps.append(np.nan)
                rmses.append(np.nan)
                continue

            # Get window (EXCLUDES current bar i)
            y = log_close[i - window : i]

            # Centered x
            x = np.arange(window) - (window - 1) / 2

            # Linear regression
            y_mean = y.mean()
            slope = np.sum((y - y_mean) * x) / np.sum(x ** 2)

            # Predictions
            y_pred = y_mean + slope * x

            # Residuals
            residuals = y - y_pred

            # Metrics
            disp = np.std(residuals)
            rmse = np.sqrt(np.mean(residuals ** 2))

            # R²
            ss_res = np.sum(residuals ** 2)
            ss_tot = np.sum((y - y_mean) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

            slopes.append(slope)
            r2s.append(r2)
            disps.append(disp)
            rmses.append(rmse)

        # Add to dataframe
        df[f"slope_{window}"] = slopes
        df[f"r2_{window}"] = r2s
        df[f"disp_{window}"] = disps
        df[f"rmse_{window}"] = rmses

    # Calculate forward outcomes
    LOG.info(f"  [{symbol}] Calculating forward outcomes...")

    horizons = [5, 10, 15]  # minutes

    for h in horizons:
        high_ret_max = []
        low_ret_min = []
        close_ret = []

        for i in range(len(df)):
            if i + h >= len(df):
                high_ret_max.append(np.nan)
                low_ret_min.append(np.nan)
                close_ret.append(np.nan)
                continue

            # Forward window [i+1 : i+h+1]
            future_window = df.iloc[i + 1 : i + h + 1]

            if len(future_window) < h:
                high_ret_max.append(np.nan)
                low_ret_min.append(np.nan)
                close_ret.append(np.nan)
                continue

            entry_price = df.iloc[i]["close"]

            # Max high return
            max_high = future_window["high"].max()
            high_ret = (max_high / entry_price) - 1

            # Min low return
            min_low = future_window["low"].min()
            low_ret = (min_low / entry_price) - 1

            # Close return at horizon
            close_price = future_window.iloc[-1]["close"]
            close_return = (close_price / entry_price) - 1

            high_ret_max.append(high_ret)
            low_ret_min.append(low_ret)
            close_ret.append(close_return)

        df[f"high_ret_max_{h}m"] = high_ret_max
        df[f"low_ret_min_{h}m"] = low_ret_min
        df[f"close_ret_{h}m"] = close_ret

    # Save results
    output_dir = Path(__file__).resolve().parent / "results"
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / f"{symbol}_feature_outcome_full.csv"

    # Reset index to save timestamp as column
    df_output = df.reset_index()

    # Keep only necessary columns
    keep_cols = ["timestamp", "close"] + \
                [f"slope_{w}" for w in windows] + \
                [f"r2_{w}" for w in windows] + \
                [f"disp_{w}" for w in windows] + \
                [f"rmse_{w}" for w in windows] + \
                [f"high_ret_max_{h}m" for h in horizons] + \
                [f"low_ret_min_{h}m" for h in horizons] + \
                [f"close_ret_{h}m" for h in horizons]

    df_output = df_output[keep_cols]

    df_output.to_csv(output_file, index=False)

    LOG.info(f"  [{symbol}] ✅ Saved {len(df_output)} rows to {output_file.name}")

    return True


def main():
    args = parse_args()
    setup_logging(args.log_level)

    here = Path(__file__).resolve().parent
    data_dir = (here / args.data_dir).resolve()

    LOG.info("="*80)
    LOG.info("PROCESS ALL SYMBOLS AND RUN COMPREHENSIVE TEST")
    LOG.info("="*80)
    LOG.info(f"Data directory: {data_dir}")
    LOG.info(f"Min expectancy target: {args.min_expectancy:.3%}")
    LOG.info("")

    # Get all symbols
    symbols = get_all_symbols(data_dir)

    if not symbols:
        LOG.error(f"❌ No symbols found in {data_dir}")
        return

    LOG.info(f"Found {len(symbols)} symbols:")
    LOG.info(f"  {', '.join(symbols[:10])}" + (f", ... (+{len(symbols)-10} more)" if len(symbols) > 10 else ""))
    LOG.info("")

    # Step 1: Process all symbols (calculate features)
    if not args.skip_processing:
        LOG.info("="*80)
        LOG.info("STEP 1: CALCULATE FEATURES FOR ALL SYMBOLS")
        LOG.info("="*80)
        LOG.info("")

        processed_count = 0
        failed_count = 0

        for symbol in symbols:
            try:
                success = process_symbol(symbol, data_dir)
                if success:
                    processed_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                LOG.error(f"  [{symbol}] ❌ Error: {e}")
                failed_count += 1

        LOG.info("")
        LOG.info(f"✅ Processed {processed_count}/{len(symbols)} symbols")
        if failed_count > 0:
            LOG.warning(f"⚠️  Failed: {failed_count} symbols")
        LOG.info("")

    else:
        LOG.info("Skipping feature calculation (--skip-processing)")
        LOG.info("")

    # Step 2: Run comprehensive walk-forward test
    LOG.info("="*80)
    LOG.info("STEP 2: RUN COMPREHENSIVE WALK-FORWARD TEST")
    LOG.info("="*80)
    LOG.info("")

    comprehensive_script = here / "09-comprehensive_walk_forward.py"

    if not comprehensive_script.exists():
        LOG.error(f"❌ Script not found: {comprehensive_script}")
        return

    # Build command
    cmd = [
        sys.executable,
        str(comprehensive_script),
        "--mag-pct", str(args.mag_pct),
        "--disp-pct", str(args.disp_pct),
        "--window-days", "30", "60", "90",
        "--num-windows", "8",
        "--log-level", args.log_level,
    ]

    LOG.info(f"Running: {' '.join(cmd)}")
    LOG.info("")

    # Run comprehensive test
    result = subprocess.run(cmd, cwd=here)

    if result.returncode != 0:
        LOG.error("❌ Comprehensive test failed!")
        sys.exit(1)

    LOG.info("")
    LOG.info("="*80)
    LOG.info("✅ ALL DONE!")
    LOG.info("="*80)
    LOG.info("")
    LOG.info("Check results in:")
    LOG.info("  - results/*_feature_outcome_full.csv (features per symbol)")
    LOG.info("  - results/comprehensive_walk_forward/ (test results)")


if __name__ == "__main__":
    main()
