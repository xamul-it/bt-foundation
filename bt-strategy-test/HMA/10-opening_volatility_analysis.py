#!/usr/bin/env python3
"""
HMA Strategy - Opening Volatility Analysis

Analyzes relationship between:
1. Previous day's volatility (ATR daily)
2. Opening gap and range (09:30-10:00)
3. Quality/quantity of HMA setups during the day

Goal: Predict optimal capital allocation before 10:00 AM based on:
- Previous day close/ATR
- Opening gap
- First 30 minutes activity

Data sources:
- Daily data: config/data/d/alpaca or config/data/d/yahoo
- Intraday data: config/data/m/alpaca (1-minute bars)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, time, timedelta
import logging
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ATR"""
    high = df['high']
    low = df['low']
    close = df['close']

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr = tr.rolling(window=period).mean()
    return atr

def calculate_atr_pct(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ATR as percentage of close"""
    atr = calculate_atr(df, period)
    return (atr / df['close']) * 100

def load_daily_data(symbol: str, data_dir: Path) -> pd.DataFrame:
    """Load daily data for a symbol"""
    csv_file = data_dir / f"{symbol}.csv"

    if not csv_file.exists():
        return None

    df = pd.read_csv(csv_file)

    # Parse datetime (try different column names)
    datetime_col = None
    for col in ['datetime', 'timestamp', 'Date', 'date']:
        if col in df.columns:
            datetime_col = col
            break

    if datetime_col is None:
        return None

    df[datetime_col] = pd.to_datetime(df[datetime_col])
    df.set_index(datetime_col, inplace=True)

    # Normalize columns
    df.columns = df.columns.str.lower()

    # Ensure timezone aware
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')

    return df

def load_intraday_data(symbol: str, data_dir: Path) -> pd.DataFrame:
    """Load 1-minute intraday data"""
    csv_file = data_dir / f"{symbol}.csv"

    if not csv_file.exists():
        return None

    df = pd.read_csv(csv_file)

    # Parse datetime
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
    elif 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
    else:
        return None

    # Normalize columns
    df.columns = df.columns.str.lower()

    # Ensure timezone aware
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC')

    return df

def analyze_day(
    date: pd.Timestamp,
    intraday_df: pd.DataFrame,
    daily_df: pd.DataFrame
) -> Dict:
    """
    Analyze one trading day

    Returns features for prediction:
    - previous_atr_pct: ATR% from previous day
    - previous_close: Close price from previous day
    - gap_pct: Opening gap %
    - opening_range_pct: Range in first 30 minutes
    - opening_volume_ratio: Volume in first 30 min vs daily average
    - atr_at_1000: ATR calculated at 10:00 AM
    - n_setups: Number of HMA setups during 10:00-15:00
    - avg_setup_quality: Average ATR of setups
    """

    # Get previous day data
    prev_date = date - pd.Timedelta(days=1)
    prev_day_data = daily_df[daily_df.index.date == prev_date.date()]

    if len(prev_day_data) == 0:
        return None

    previous_close = prev_day_data['close'].iloc[-1]
    previous_atr = calculate_atr_pct(daily_df[daily_df.index <= prev_date], period=14).iloc[-1]

    # Get current day intraday data
    day_data = intraday_df[intraday_df.index.date == date.date()]

    if len(day_data) < 100:
        return None

    # Opening data (09:30 - 10:00)
    opening_start = pd.Timestamp(date.date()).replace(hour=9, minute=30, tzinfo=date.tzinfo)
    opening_end = pd.Timestamp(date.date()).replace(hour=10, minute=0, tzinfo=date.tzinfo)

    opening_data = day_data[(day_data.index >= opening_start) & (day_data.index < opening_end)]

    if len(opening_data) < 20:
        return None

    # Calculate opening features
    opening_first = opening_data.iloc[0]
    opening_high = opening_data['high'].max()
    opening_low = opening_data['low'].min()
    opening_volume = opening_data['volume'].sum()

    gap_pct = (opening_first['open'] - previous_close) / previous_close * 100
    opening_range_pct = (opening_high - opening_low) / opening_low * 100

    # Volume ratio (first 30 min vs average daily)
    if 'volume' in daily_df.columns:
        avg_daily_volume = daily_df['volume'].iloc[-20:].mean()
        # First 30 min should be ~1/13 of daily volume (6.5 hours = 390 minutes = 13 × 30 min)
        expected_opening_volume = avg_daily_volume / 13
        volume_ratio = opening_volume / expected_opening_volume if expected_opening_volume > 0 else 1.0
    else:
        volume_ratio = 1.0

    # Calculate ATR at 10:00
    data_up_to_1000 = day_data[day_data.index < opening_end]
    if len(data_up_to_1000) >= 14:
        atr_at_1000 = calculate_atr_pct(data_up_to_1000, period=14).iloc[-1]
    else:
        atr_at_1000 = np.nan

    # Count HMA setups during 10:00-15:00
    trading_start = opening_end
    trading_end = pd.Timestamp(date.date()).replace(hour=15, minute=0, tzinfo=date.tzinfo)

    trading_data = day_data[(day_data.index >= trading_start) & (day_data.index < trading_end)]

    if len(trading_data) < 50:
        n_setups = 0
        avg_setup_atr = np.nan
    else:
        # Calculate HMA signals (simplified: just count high ATR bars)
        trading_atr = calculate_atr_pct(trading_data, period=14)

        # Count bars with ATR >= 0.7%
        high_atr_bars = trading_atr[trading_atr >= 0.7]
        n_setups = len(high_atr_bars)
        avg_setup_atr = high_atr_bars.mean() if len(high_atr_bars) > 0 else np.nan

    return {
        'date': date.date(),
        'symbol': intraday_df['symbol'].iloc[0] if 'symbol' in intraday_df.columns else 'UNKNOWN',
        'previous_atr_pct': previous_atr,
        'previous_close': previous_close,
        'gap_pct': gap_pct,
        'opening_range_pct': opening_range_pct,
        'opening_volume_ratio': volume_ratio,
        'atr_at_1000': atr_at_1000,
        'n_setups': n_setups,
        'avg_setup_atr': avg_setup_atr
    }

def main():
    # Configuration
    DAILY_DATA_DIR = Path('config/data/d/alpaca')
    INTRADAY_DATA_DIR = Path('config/data/m/alpaca')
    OUTPUT_DIR = Path('bt-strategy-test/HMA/opening_volatility')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Date range for analysis
    START_DATE = '2025-05-06'
    END_DATE = '2025-07-03'

    log.info("=" * 80)
    log.info("OPENING VOLATILITY ANALYSIS")
    log.info("=" * 80)
    log.info(f"Period: {START_DATE} to {END_DATE}")
    log.info(f"Daily data: {DAILY_DATA_DIR}")
    log.info(f"Intraday data: {INTRADAY_DATA_DIR}")
    log.info("")

    # Get symbols
    intraday_files = sorted(INTRADAY_DATA_DIR.glob('*.csv'))
    symbols = [f.stem for f in intraday_files]

    log.info(f"Found {len(symbols)} symbols")
    log.info("")

    # Analyze each symbol
    all_results = []

    for symbol in symbols:
        log.info(f"Analyzing {symbol}...")

        # Load data
        daily_df = load_daily_data(symbol, DAILY_DATA_DIR)
        intraday_df = load_intraday_data(symbol, INTRADAY_DATA_DIR)

        if daily_df is None or intraday_df is None:
            log.warning(f"  Skipping {symbol}: missing data")
            continue

        # Filter by date range
        start_ts = pd.to_datetime(START_DATE).tz_localize('UTC')
        end_ts = pd.to_datetime(END_DATE).tz_localize('UTC')

        intraday_df = intraday_df[(intraday_df.index >= start_ts) & (intraday_df.index < end_ts)]

        if len(intraday_df) == 0:
            log.warning(f"  Skipping {symbol}: no data in range")
            continue

        # Get unique trading dates
        trading_dates = pd.Series(intraday_df.index.date).unique()

        log.info(f"  Processing {len(trading_dates)} trading days...")

        symbol_results = []

        for date in trading_dates:
            date_ts = pd.Timestamp(date).tz_localize('UTC')

            result = analyze_day(date_ts, intraday_df, daily_df)

            if result is not None:
                symbol_results.append(result)

        log.info(f"  Collected {len(symbol_results)} days of data")
        all_results.extend(symbol_results)

    # Create DataFrame
    results_df = pd.DataFrame(all_results)

    log.info("")
    log.info(f"Total records: {len(results_df)}")

    if len(results_df) == 0:
        log.error("No data collected. Exiting.")
        return

    # Save raw results
    results_file = OUTPUT_DIR / 'opening_analysis_results.csv'
    results_df.to_csv(results_file, index=False)
    log.info(f"Saved results to: {results_file}")
    log.info("")

    # Analysis
    log.info("=" * 80)
    log.info("CORRELATION ANALYSIS")
    log.info("=" * 80)
    log.info("")

    # Remove NaN values for correlation
    analysis_df = results_df.dropna(subset=['previous_atr_pct', 'gap_pct', 'opening_range_pct',
                                             'atr_at_1000', 'n_setups', 'avg_setup_atr'])

    if len(analysis_df) < 10:
        log.error("Not enough data for analysis")
        return

    # Correlations with n_setups (target variable)
    features = ['previous_atr_pct', 'gap_pct', 'opening_range_pct',
                'opening_volume_ratio', 'atr_at_1000']

    print("CORRELATION WITH NUMBER OF SETUPS:")
    print("-" * 80)
    for feature in features:
        if feature in analysis_df.columns:
            corr = analysis_df[feature].corr(analysis_df['n_setups'])
            print(f"  {feature:<25} → n_setups:  {corr:>7.3f}")
    print("")

    print("CORRELATION WITH AVERAGE SETUP QUALITY (ATR):")
    print("-" * 80)
    for feature in features:
        if feature in analysis_df.columns:
            corr = analysis_df[feature].corr(analysis_df['avg_setup_atr'])
            print(f"  {feature:<25} → avg_setup_atr:  {corr:>7.3f}")
    print("")

    # Binned analysis
    log.info("=" * 80)
    log.info("BINNED ANALYSIS - OPENING RANGE")
    log.info("=" * 80)
    log.info("")

    analysis_df['opening_range_bin'] = pd.cut(
        analysis_df['opening_range_pct'],
        bins=[0, 0.5, 1.0, 2.0, 100],
        labels=['Low (<0.5%)', 'Medium (0.5-1%)', 'High (1-2%)', 'Very High (>2%)']
    )

    print("NUMBER OF SETUPS BY OPENING RANGE:")
    print("-" * 80)
    grouped = analysis_df.groupby('opening_range_bin')['n_setups'].agg(['mean', 'median', 'std', 'count'])
    print(grouped)
    print("")

    print("AVERAGE SETUP QUALITY (ATR) BY OPENING RANGE:")
    print("-" * 80)
    grouped_quality = analysis_df.groupby('opening_range_bin')['avg_setup_atr'].agg(['mean', 'median', 'count'])
    print(grouped_quality)
    print("")

    # GAP analysis
    log.info("=" * 80)
    log.info("BINNED ANALYSIS - OPENING GAP")
    log.info("=" * 80)
    log.info("")

    analysis_df['gap_abs'] = analysis_df['gap_pct'].abs()
    analysis_df['gap_bin'] = pd.cut(
        analysis_df['gap_abs'],
        bins=[0, 0.2, 0.5, 1.0, 100],
        labels=['Small (<0.2%)', 'Medium (0.2-0.5%)', 'Large (0.5-1%)', 'Very Large (>1%)']
    )

    print("NUMBER OF SETUPS BY GAP SIZE:")
    print("-" * 80)
    grouped_gap = analysis_df.groupby('gap_bin')['n_setups'].agg(['mean', 'median', 'std', 'count'])
    print(grouped_gap)
    print("")

    # ATR previous day analysis
    log.info("=" * 80)
    log.info("BINNED ANALYSIS - PREVIOUS DAY ATR")
    log.info("=" * 80)
    log.info("")

    analysis_df['prev_atr_bin'] = pd.cut(
        analysis_df['previous_atr_pct'],
        bins=[0, 0.5, 1.0, 1.5, 100],
        labels=['Low (<0.5%)', 'Medium (0.5-1%)', 'High (1-1.5%)', 'Very High (>1.5%)']
    )

    print("NUMBER OF SETUPS BY PREVIOUS DAY ATR:")
    print("-" * 80)
    grouped_prev = analysis_df.groupby('prev_atr_bin')['n_setups'].agg(['mean', 'median', 'std', 'count'])
    print(grouped_prev)
    print("")

    # Plots
    log.info("Creating visualizations...")

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Opening Volatility Analysis - Predictive Features', fontsize=16, fontweight='bold')

    # 1. Opening range vs n_setups
    ax = axes[0, 0]
    ax.scatter(analysis_df['opening_range_pct'], analysis_df['n_setups'], alpha=0.5)
    ax.set_xlabel('Opening Range (%)', fontsize=11)
    ax.set_ylabel('Number of Setups', fontsize=11)
    ax.set_title('Opening Range vs Setups', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # Add trend line
    z = np.polyfit(analysis_df['opening_range_pct'], analysis_df['n_setups'], 1)
    p = np.poly1d(z)
    ax.plot(analysis_df['opening_range_pct'].sort_values(),
            p(analysis_df['opening_range_pct'].sort_values()),
            "r--", alpha=0.8, linewidth=2)

    # 2. Gap vs n_setups
    ax = axes[0, 1]
    ax.scatter(analysis_df['gap_abs'], analysis_df['n_setups'], alpha=0.5)
    ax.set_xlabel('Opening Gap (abs %)', fontsize=11)
    ax.set_ylabel('Number of Setups', fontsize=11)
    ax.set_title('Opening Gap vs Setups', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # 3. Previous ATR vs n_setups
    ax = axes[0, 2]
    ax.scatter(analysis_df['previous_atr_pct'], analysis_df['n_setups'], alpha=0.5)
    ax.set_xlabel('Previous Day ATR (%)', fontsize=11)
    ax.set_ylabel('Number of Setups', fontsize=11)
    ax.set_title('Previous ATR vs Setups', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # 4. ATR at 10:00 vs n_setups
    ax = axes[1, 0]
    ax.scatter(analysis_df['atr_at_1000'], analysis_df['n_setups'], alpha=0.5, color='green')
    ax.set_xlabel('ATR at 10:00 (%)', fontsize=11)
    ax.set_ylabel('Number of Setups', fontsize=11)
    ax.set_title('ATR at 10:00 vs Setups', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)

    # Add trend line
    z = np.polyfit(analysis_df['atr_at_1000'], analysis_df['n_setups'], 1)
    p = np.poly1d(z)
    ax.plot(analysis_df['atr_at_1000'].sort_values(),
            p(analysis_df['atr_at_1000'].sort_values()),
            "r--", alpha=0.8, linewidth=2)

    # 5. Box plot - Opening range bins
    ax = axes[1, 1]
    analysis_df.boxplot(column='n_setups', by='opening_range_bin', ax=ax)
    ax.set_xlabel('Opening Range Category', fontsize=11)
    ax.set_ylabel('Number of Setups', fontsize=11)
    ax.set_title('Setups by Opening Range', fontsize=12, fontweight='bold')
    plt.sca(ax)
    plt.xticks(rotation=45)

    # 6. Box plot - Previous ATR bins
    ax = axes[1, 2]
    analysis_df.boxplot(column='n_setups', by='prev_atr_bin', ax=ax)
    ax.set_xlabel('Previous Day ATR Category', fontsize=11)
    ax.set_ylabel('Number of Setups', fontsize=11)
    ax.set_title('Setups by Previous ATR', fontsize=12, fontweight='bold')
    plt.sca(ax)
    plt.xticks(rotation=45)

    plt.tight_layout()

    plot_file = OUTPUT_DIR / 'opening_volatility_analysis.png'
    plt.savefig(plot_file, dpi=150, bbox_inches='tight')
    log.info(f"Saved plots to: {plot_file}")

    # Recommendation
    log.info("")
    log.info("=" * 80)
    log.info("RECOMMENDATIONS FOR DYNAMIC ALLOCATION")
    log.info("=" * 80)
    log.info("")

    # Find thresholds
    opening_range_high = analysis_df['opening_range_pct'].quantile(0.75)
    opening_range_low = analysis_df['opening_range_pct'].quantile(0.25)

    gap_high = analysis_df['gap_abs'].quantile(0.75)
    prev_atr_high = analysis_df['previous_atr_pct'].quantile(0.75)

    print("DYNAMIC ALLOCATION RULES (based on data):")
    print("-" * 80)
    print("")
    print("HIGH OPPORTUNITY DAYS (increase allocation 1.5×):")
    print(f"  - Opening range > {opening_range_high:.2f}%")
    print(f"  - OR Gap > {gap_high:.2f}%")
    print(f"  - OR Previous day ATR > {prev_atr_high:.2f}%")
    print("")
    print("LOW OPPORTUNITY DAYS (reduce allocation 0.5× or skip):")
    print(f"  - Opening range < {opening_range_low:.2f}%")
    print(f"  - AND Gap < {gap_high/2:.2f}%")
    print("")
    print("NORMAL DAYS (standard allocation 1.0×):")
    print("  - All other days")
    print("")
    print("BEST PREDICTOR:")

    # Find best predictor
    correlations = {
        'Opening Range': analysis_df['opening_range_pct'].corr(analysis_df['n_setups']),
        'Gap': analysis_df['gap_abs'].corr(analysis_df['n_setups']),
        'Previous ATR': analysis_df['previous_atr_pct'].corr(analysis_df['n_setups']),
        'ATR at 10:00': analysis_df['atr_at_1000'].corr(analysis_df['n_setups'])
    }

    best_predictor = max(correlations, key=correlations.get)
    print(f"  {best_predictor} (correlation: {correlations[best_predictor]:.3f})")
    print("")

    print("=" * 80)
    print("")

if __name__ == '__main__':
    main()
