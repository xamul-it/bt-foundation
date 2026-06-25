#!/usr/bin/env python3
"""
HMA Strategy - Realistic Portfolio Monte Carlo Simulation

Simulates complete trading day workflow:
1. Load all symbols data for the day
2. Calculate opening volatility metrics (09:30-10:00)
3. Rank symbols by opportunity score
4. Allocate capital dynamically
5. Trade 10:00-15:00 with queue management (max positions)
6. ATR-based position sizing
7. Track equity curve and metrics

This is more realistic than resampling historical trades because it:
- Simulates capital constraints
- Simulates opportunity selection
- Tests allocation strategy
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
import logging
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

sns.set_style("whitegrid")

def load_all_intraday_data(data_dir: Path, symbols: List[str]) -> Dict[str, pd.DataFrame]:
    """Load intraday data for all symbols"""
    data = {}

    for symbol in symbols:
        csv_file = data_dir / f"{symbol}.csv"
        if not csv_file.exists():
            continue

        df = pd.read_csv(csv_file)

        # Parse datetime
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        else:
            continue

        df.columns = df.columns.str.lower()

        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')

        df['symbol'] = symbol
        data[symbol] = df

    return data

def calculate_atr_pct(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ATR as percentage"""
    high = df['high']
    low = df['low']
    close = df['close']

    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()

    return (atr / close) * 100

def calculate_hma(close: pd.Series, period: int) -> pd.Series:
    """Calculate HMA"""
    half_period = int(period / 2)
    sqrt_period = int(np.sqrt(period))

    wma_half = close.rolling(window=half_period).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )
    wma_full = close.rolling(window=period).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )

    raw_hma = 2 * wma_half - wma_full
    hma = raw_hma.rolling(window=sqrt_period).apply(
        lambda x: np.sum(x * np.arange(1, len(x) + 1)) / np.sum(np.arange(1, len(x) + 1)),
        raw=True
    )

    return hma

def analyze_opening(symbol_data: pd.DataFrame, prev_close: float) -> Dict:
    """Analyze opening 30 minutes (09:30-10:00)"""
    date = symbol_data.index[0].date()

    opening_start = pd.Timestamp(date).replace(hour=9, minute=30, tzinfo=symbol_data.index.tz)
    opening_end = pd.Timestamp(date).replace(hour=10, minute=0, tzinfo=symbol_data.index.tz)

    opening_data = symbol_data[(symbol_data.index >= opening_start) & (symbol_data.index < opening_end)]

    if len(opening_data) < 10:
        return None

    opening_high = opening_data['high'].max()
    opening_low = opening_data['low'].min()
    opening_first = opening_data.iloc[0]['open']

    opening_range_pct = (opening_high - opening_low) / opening_low * 100
    gap_pct = abs(opening_first - prev_close) / prev_close * 100

    return {
        'opening_range_pct': opening_range_pct,
        'gap_pct': gap_pct,
        'opening_end_time': opening_end
    }

def atr_position_size(capital: float, atr_pct: float, target_risk: float = 0.02, sl_pct: float = 0.005) -> float:
    """ATR-based position sizing"""
    base_size = (capital * target_risk) / sl_pct
    atr_adjustment = 0.7 / max(atr_pct, 0.3)
    adjusted_size = base_size * atr_adjustment
    return min(adjusted_size, capital * 0.20)

def simulate_day(
    date: pd.Timestamp,
    all_data: Dict[str, pd.DataFrame],
    prev_closes: Dict[str, float],
    capital: float,
    max_positions: int,
    hma_period: int,
    slippage: float
) -> Dict:
    """
    Simulate one complete trading day

    Returns:
        - n_signals: total signals generated
        - n_trades: trades executed (limited by capital/queue)
        - capital_end: ending capital
        - trades: list of trade results
    """

    # Step 1: Get data for all symbols for this date
    date_str = date.date()
    symbols_today = []

    for symbol, df in all_data.items():
        day_data = df[df.index.date == date_str]
        if len(day_data) > 100:  # Sufficient data
            symbols_today.append(symbol)

    if len(symbols_today) == 0:
        return None

    # Step 2: Calculate opening metrics and rank symbols
    opening_scores = {}

    for symbol in symbols_today:
        day_data = all_data[symbol][all_data[symbol].index.date == date_str]
        prev_close = prev_closes.get(symbol)

        if prev_close is None:
            continue

        opening_metrics = analyze_opening(day_data, prev_close)

        if opening_metrics is None:
            continue

        # Opportunity score (based on correlation analysis)
        score = 0
        if opening_metrics['opening_range_pct'] > 0.68:
            score += 1.5
        if opening_metrics['gap_pct'] > 4.72:
            score += 1.0

        # Base score
        score += opening_metrics['opening_range_pct'] * 0.5

        opening_scores[symbol] = {
            'score': score,
            'opening_range': opening_metrics['opening_range_pct'],
            'gap': opening_metrics['gap_pct']
        }

    # Step 3: Allocate capital based on scores
    ranked_symbols = sorted(opening_scores.keys(), key=lambda s: opening_scores[s]['score'], reverse=True)

    # Take top symbols (up to max_positions worth)
    top_symbols = ranked_symbols[:max_positions * 2]  # Allow 2× for filtering

    # Calculate allocation weights
    total_score = sum(opening_scores[s]['score'] for s in top_symbols)
    capital_allocation = {}

    for symbol in top_symbols:
        if total_score > 0:
            weight = opening_scores[symbol]['score'] / total_score
            capital_allocation[symbol] = capital * weight * 0.8  # Reserve 20%
        else:
            capital_allocation[symbol] = capital / len(top_symbols) * 0.8

    # Step 4: Trade 10:00-15:00
    trading_start = pd.Timestamp(date_str).replace(hour=10, minute=0, tzinfo=date.tzinfo)
    trading_end = pd.Timestamp(date_str).replace(hour=15, minute=0, tzinfo=date.tzinfo)

    active_positions = []
    trades_executed = []
    n_signals = 0
    available_capital = capital

    # Process each symbol in priority order
    for symbol in top_symbols:
        day_data = all_data[symbol][all_data[symbol].index.date == date_str]
        trading_data = day_data[(day_data.index >= trading_start) & (day_data.index < trading_end)]

        if len(trading_data) < 50:
            continue

        # Calculate HMA and ATR
        trading_data = trading_data.copy()
        trading_data['hma'] = calculate_hma(trading_data['close'], hma_period)
        trading_data['atr_pct'] = calculate_atr_pct(trading_data, period=14)
        trading_data['hma_rising'] = trading_data['hma'].diff() > 0

        # Check for signals
        for i in range(1, len(trading_data)):
            current = trading_data.iloc[i]
            prev = trading_data.iloc[i-1]

            if pd.isna(current['hma']) or pd.isna(current['atr_pct']):
                continue

            # ATR filter
            if current['atr_pct'] < 0.7:
                continue

            # HMA signal (inverted logic)
            signal_long = prev['hma_rising'] and not current['hma_rising']
            signal_short = not prev['hma_rising'] and current['hma_rising']

            if not (signal_long or signal_short):
                continue

            n_signals += 1

            # Check if we can take position
            if len(active_positions) >= max_positions:
                continue  # Queue full

            # Calculate position size
            allocated = capital_allocation.get(symbol, capital * 0.1)
            position_size = atr_position_size(
                min(allocated, available_capital),
                current['atr_pct'],
                target_risk=0.02,
                sl_pct=0.005
            )

            if position_size < available_capital * 0.02:  # Too small
                continue

            # Execute trade (simplified: random outcome based on historical stats)
            # Win rate: 34.6%, Avg win: 1.476%, Avg loss: -0.496%
            if np.random.random() < 0.346:
                # Winner
                pnl_pct = np.random.normal(0.01476, 0.005)  # ~1.476% ± noise
            else:
                # Loser (SL or small loss)
                pnl_pct = -0.005  # SL hit

            # Apply slippage
            pnl_pct -= slippage

            trade_pnl = position_size * pnl_pct
            available_capital += trade_pnl

            trades_executed.append({
                'symbol': symbol,
                'pnl_pct': pnl_pct,
                'pnl': trade_pnl,
                'position_size': position_size
            })

            # Track position
            active_positions.append(symbol)
            if len(active_positions) > max_positions:
                active_positions.pop(0)

    return {
        'date': date_str,
        'n_symbols_ranked': len(top_symbols),
        'n_signals': n_signals,
        'n_trades': len(trades_executed),
        'capital_start': capital,
        'capital_end': available_capital,
        'return_pct': (available_capital - capital) / capital * 100,
        'trades': trades_executed
    }

def run_simulation(
    all_data: Dict[str, pd.DataFrame],
    start_date: str,
    end_date: str,
    initial_capital: float,
    max_positions: int,
    hma_period: int,
    slippage: float,
    n_runs: int = 100
) -> List[Dict]:
    """Run multiple Monte Carlo simulations"""

    # Get trading dates
    all_dates = set()
    for df in all_data.values():
        all_dates.update(df.index.date)

    trading_dates = sorted([d for d in all_dates
                           if pd.Timestamp(start_date).date() <= d < pd.Timestamp(end_date).date()])

    log.info(f"Simulating {len(trading_dates)} trading days, {n_runs} runs each")

    all_results = []

    for run in range(n_runs):
        if (run + 1) % 10 == 0:
            log.info(f"  Run {run + 1}/{n_runs}...")

        capital = initial_capital
        equity_curve = [capital]
        total_trades = 0

        # Track previous closes
        prev_closes = {}
        for symbol, df in all_data.items():
            if len(df) > 0:
                prev_closes[symbol] = df.iloc[0]['close']

        day_results = []

        for date in trading_dates:
            date_ts = pd.Timestamp(date).tz_localize('UTC')

            result = simulate_day(
                date_ts,
                all_data,
                prev_closes,
                capital,
                max_positions,
                hma_period,
                slippage
            )

            if result is not None:
                capital = result['capital_end']
                equity_curve.append(capital)
                total_trades += result['n_trades']
                day_results.append(result)

                # Update prev_closes
                for symbol in all_data.keys():
                    day_data = all_data[symbol][all_data[symbol].index.date == date]
                    if len(day_data) > 0:
                        prev_closes[symbol] = day_data.iloc[-1]['close']

        # Calculate metrics
        equity_curve = np.array(equity_curve)
        total_return = (capital - initial_capital) / initial_capital

        peak = np.maximum.accumulate(equity_curve)
        drawdown = (equity_curve - peak) / peak
        max_drawdown = drawdown.min()

        all_results.append({
            'run': run,
            'total_return': total_return,
            'final_capital': capital,
            'max_drawdown': max_drawdown,
            'total_trades': total_trades,
            'trades_per_day': total_trades / len(trading_dates) if len(trading_dates) > 0 else 0,
            'equity_curve': equity_curve
        })

    return all_results

def main():
    # Configuration
    INTRADAY_DATA_DIR = Path('config/data/m/alpaca')
    OUTPUT_DIR = Path('bt-strategy-test/HMA/monte_carlo_portfolio')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    START_DATE = '2025-05-06'
    END_DATE = '2025-07-03'

    INITIAL_CAPITAL = 100000
    MAX_POSITIONS = 10
    HMA_PERIOD = 16
    SLIPPAGE = 0.0005  # 0.05%
    N_RUNS = 100

    log.info("=" * 80)
    log.info("REALISTIC PORTFOLIO MONTE CARLO SIMULATION")
    log.info("=" * 80)
    log.info(f"Period: {START_DATE} to {END_DATE}")
    log.info(f"Initial capital: ${INITIAL_CAPITAL:,}")
    log.info(f"Max positions: {MAX_POSITIONS}")
    log.info(f"Slippage: {SLIPPAGE * 100:.2f}%")
    log.info(f"Runs: {N_RUNS}")
    log.info("")

    # Load all data
    log.info("Loading intraday data for all symbols...")
    intraday_files = sorted(INTRADAY_DATA_DIR.glob('*.csv'))
    symbols = [f.stem for f in intraday_files]

    all_data = load_all_intraday_data(INTRADAY_DATA_DIR, symbols)
    log.info(f"Loaded {len(all_data)} symbols")
    log.info("")

    # Filter by date range
    for symbol in list(all_data.keys()):
        df = all_data[symbol]
        start_ts = pd.to_datetime(START_DATE).tz_localize('UTC')
        end_ts = pd.to_datetime(END_DATE).tz_localize('UTC')
        df = df[(df.index >= start_ts) & (df.index < end_ts)]

        if len(df) < 100:
            del all_data[symbol]
        else:
            all_data[symbol] = df

    log.info(f"Symbols in date range: {len(all_data)}")
    log.info("")

    # Run simulation
    results = run_simulation(
        all_data,
        START_DATE,
        END_DATE,
        INITIAL_CAPITAL,
        MAX_POSITIONS,
        HMA_PERIOD,
        SLIPPAGE,
        N_RUNS
    )

    # Analyze results
    results_df = pd.DataFrame([{k: v for k, v in r.items() if k != 'equity_curve'} for r in results])

    log.info("")
    log.info("=" * 80)
    log.info("RESULTS")
    log.info("=" * 80)
    print(f"\nANNUAL RETURN (prorated from {(pd.to_datetime(END_DATE) - pd.to_datetime(START_DATE)).days} days):")
    print(f"  Mean:               {results_df['total_return'].mean() * (252/41) * 100:>8.2f}%")
    print(f"  Median:             {results_df['total_return'].median() * (252/41) * 100:>8.2f}%")
    print(f"  Std:                {results_df['total_return'].std() * (252/41) * 100:>8.2f}%")
    print(f"  5th percentile:     {results_df['total_return'].quantile(0.05) * (252/41) * 100:>8.2f}%")
    print(f"  95th percentile:    {results_df['total_return'].quantile(0.95) * (252/41) * 100:>8.2f}%")
    print(f"\nMAX DRAWDOWN:")
    print(f"  Mean:               {results_df['max_drawdown'].mean() * 100:>8.2f}%")
    print(f"  95th percentile:    {results_df['max_drawdown'].quantile(0.05) * 100:>8.2f}%")
    print(f"\nTRADES:")
    print(f"  Total trades (mean): {results_df['total_trades'].mean():>8.1f}")
    print(f"  Trades per day:      {results_df['trades_per_day'].mean():>8.1f}")
    print("")

    # Save
    results_df.to_csv(OUTPUT_DIR / 'simulation_results.csv', index=False)
    log.info(f"Saved results to: {OUTPUT_DIR / 'simulation_results.csv'}")

    # Plot
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Returns distribution
    ax = axes[0, 0]
    ax.hist(results_df['total_return'] * (252/41) * 100, bins=30, alpha=0.7, edgecolor='black')
    ax.axvline(0, color='red', linestyle='--', linewidth=2)
    ax.set_xlabel('Annual Return (%)')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Returns')
    ax.grid(True, alpha=0.3)

    # Drawdown distribution
    ax = axes[0, 1]
    ax.hist(results_df['max_drawdown'] * 100, bins=30, alpha=0.7, color='indianred', edgecolor='black')
    ax.set_xlabel('Max Drawdown (%)')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Max Drawdown')
    ax.grid(True, alpha=0.3)

    # Trades per day
    ax = axes[1, 0]
    ax.hist(results_df['trades_per_day'], bins=20, alpha=0.7, color='seagreen', edgecolor='black')
    ax.set_xlabel('Trades per Day')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Trading Frequency')
    ax.grid(True, alpha=0.3)

    # Sample equity curves
    ax = axes[1, 1]
    for i in range(min(20, len(results))):
        eq = results[i]['equity_curve']
        ax.plot(eq / INITIAL_CAPITAL * 100, alpha=0.3, linewidth=0.5)
    ax.axhline(100, color='black', linestyle='--', linewidth=1)
    ax.set_xlabel('Day')
    ax.set_ylabel('Capital (%)')
    ax.set_title('Sample Equity Curves')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'simulation_results.png', dpi=150)
    log.info(f"Saved plots to: {OUTPUT_DIR / 'simulation_results.png'}")
    log.info("")

if __name__ == '__main__':
    main()
