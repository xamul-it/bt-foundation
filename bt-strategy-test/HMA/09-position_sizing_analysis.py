#!/usr/bin/env python3
"""
HMA Strategy - Position Sizing Analysis

Compares different position sizing methods:
1. Fixed % (baseline: 10%)
2. ATR-based (risk parity)
3. Volatility-adjusted
4. Kelly criterion
5. Limited capital simulation (realistic portfolio)

Also analyzes:
- Capital allocation with max concurrent positions
- Pre-market volatility filtering
- Dynamic allocation strategies
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

sns.set_style("whitegrid")

def fixed_sizing(capital: float, fixed_pct: float) -> float:
    """Fixed percentage position sizing"""
    return capital * fixed_pct

def atr_based_sizing(
    capital: float,
    atr_pct: float,
    target_risk_pct: float = 0.02,
    sl_pct: float = 0.005
) -> float:
    """
    ATR-based position sizing (risk parity)

    Position size such that if SL is hit, loss = target_risk_pct of capital

    Formula: position_size = (capital * target_risk_pct) / sl_pct

    But we can adjust based on ATR:
    - High ATR (high volatility) → reduce position size
    - Low ATR (low volatility) → increase position size
    """
    # Base position size for target risk
    base_size = (capital * target_risk_pct) / sl_pct

    # Adjust by ATR (normalize to typical ATR of 0.7%)
    atr_adjustment = 0.7 / max(atr_pct, 0.3)  # Don't over-leverage on low ATR

    adjusted_size = base_size * atr_adjustment

    # Cap at 20% of capital
    max_size = capital * 0.20

    return min(adjusted_size, max_size)

def volatility_adjusted_sizing(
    capital: float,
    recent_volatility: float,
    target_volatility: float = 0.01
) -> float:
    """
    Volatility-adjusted sizing

    Inverse relationship: higher volatility → smaller positions
    """
    vol_ratio = target_volatility / max(recent_volatility, 0.005)
    base_size = capital * 0.10

    adjusted_size = base_size * vol_ratio

    # Bounds: 5% to 20% of capital
    return np.clip(adjusted_size, capital * 0.05, capital * 0.20)

def kelly_criterion(
    capital: float,
    win_rate: float,
    avg_win: float,
    avg_loss: float
) -> float:
    """
    Kelly Criterion for optimal position sizing

    Formula: f = (p * b - q) / b
    where:
        p = win probability
        q = 1 - p (loss probability)
        b = avg_win / avg_loss (odds)
    """
    p = win_rate
    q = 1 - p
    b = avg_win / avg_loss if avg_loss > 0 else 1

    kelly_fraction = (p * b - q) / b

    # Use fractional Kelly (half Kelly for safety)
    fractional_kelly = kelly_fraction * 0.5

    # Bounds
    fractional_kelly = np.clip(fractional_kelly, 0.05, 0.25)

    return capital * fractional_kelly

def simulate_with_sizing_method(
    trades_df: pd.DataFrame,
    sizing_method: str,
    initial_capital: float,
    max_positions: int = 10,
    n_simulations: int = 1000
) -> Dict:
    """
    Simulate trading with different position sizing methods
    and realistic capital constraints
    """

    results = []

    for sim in range(n_simulations):
        # Resample trades
        sampled_trades = trades_df.sample(n=len(trades_df), replace=True, random_state=sim)

        capital = initial_capital
        equity_curve = [capital]
        active_positions = []

        for idx, trade in sampled_trades.iterrows():
            # Check if we can take new position
            if len(active_positions) >= max_positions:
                # Skip this trade (capital fully allocated)
                continue

            # Determine position size based on method
            if sizing_method == 'fixed_10pct':
                position_size = fixed_sizing(capital, 0.10)

            elif sizing_method == 'fixed_5pct':
                position_size = fixed_sizing(capital, 0.05)

            elif sizing_method == 'atr_based':
                atr_pct = trade.get('atr_pct', 0.007)  # Default to 0.7% if missing
                position_size = atr_based_sizing(capital, atr_pct, target_risk_pct=0.02, sl_pct=0.005)

            elif sizing_method == 'volatility_adjusted':
                # Use recent PnL volatility as proxy
                if len(equity_curve) > 20:
                    recent_returns = np.diff(equity_curve[-20:]) / equity_curve[-21:-1]
                    recent_vol = np.std(recent_returns)
                else:
                    recent_vol = 0.01
                position_size = volatility_adjusted_sizing(capital, recent_vol)

            elif sizing_method == 'kelly':
                # Calculate Kelly based on recent trades
                if len(equity_curve) > 50:
                    recent_trades = sampled_trades.iloc[max(0, idx - 50):idx]
                    win_rate = (recent_trades['pnl_pct'] > 0).mean()
                    avg_win = recent_trades[recent_trades['pnl_pct'] > 0]['pnl_pct'].mean()
                    avg_loss = abs(recent_trades[recent_trades['pnl_pct'] < 0]['pnl_pct'].mean())
                else:
                    win_rate = 0.35
                    avg_win = 0.015
                    avg_loss = 0.005
                position_size = kelly_criterion(capital, win_rate, avg_win, avg_loss)

            else:
                position_size = fixed_sizing(capital, 0.10)

            # Check if we have enough capital
            if position_size > capital * 0.95:
                # Not enough capital
                continue

            # Execute trade
            trade_pnl = position_size * trade['pnl_pct']
            capital += trade_pnl
            equity_curve.append(capital)

            # Track position (simplified)
            active_positions.append(position_size)
            if len(active_positions) > max_positions:
                active_positions.pop(0)

        # Calculate metrics
        equity_curve = np.array(equity_curve)
        total_return = (capital - initial_capital) / initial_capital

        peak = np.maximum.accumulate(equity_curve)
        drawdown = (equity_curve - peak) / peak
        max_dd = drawdown.min()

        results.append({
            'total_return': total_return,
            'max_drawdown': max_dd,
            'final_capital': capital
        })

    results_df = pd.DataFrame(results)

    return {
        'mean_return': results_df['total_return'].mean(),
        'median_return': results_df['total_return'].median(),
        'std_return': results_df['total_return'].std(),
        'mean_dd': results_df['max_drawdown'].mean(),
        'worst_dd': results_df['max_drawdown'].min(),
        'sharpe': (results_df['total_return'].mean() / results_df['total_return'].std()) * np.sqrt(252) if results_df['total_return'].std() > 0 else 0
    }

def analyze_capital_allocation(
    trades_df: pd.DataFrame,
    initial_capital: float,
    max_positions: int
) -> pd.DataFrame:
    """
    Analyze capital allocation strategies with limited capital

    Questions:
    1. Fixed allocation per symbol (e.g., max 10% per symbol)
    2. Dynamic allocation based on available capital
    3. Queue management when more signals than capital
    """

    log.info(f"Analyzing capital allocation with ${initial_capital:,.0f} and max {max_positions} positions")

    # Simulate different allocation strategies
    strategies = {
        'equal_weight': 'Equal weight across all positions',
        'atr_weighted': 'Weight by inverse ATR (more capital to lower vol)',
        'signal_strength': 'Weight by ATR magnitude (more capital to high vol)'
    }

    results = []

    for strategy_name, description in strategies.items():
        log.info(f"  Testing: {strategy_name}")

        # Simplified simulation
        total_capital_used = 0
        n_positions = min(max_positions, len(trades_df))

        if strategy_name == 'equal_weight':
            capital_per_position = initial_capital / max_positions

        elif strategy_name == 'atr_weighted':
            # More capital to lower volatility (safer) positions
            atrs = trades_df['atr_pct'].values if 'atr_pct' in trades_df.columns else np.ones(len(trades_df)) * 0.007
            weights = 1 / atrs
            weights = weights / weights.sum()
            capital_allocations = weights * initial_capital
            capital_per_position = capital_allocations.mean()

        elif strategy_name == 'signal_strength':
            # More capital to high volatility (stronger signal) positions
            atrs = trades_df['atr_pct'].values if 'atr_pct' in trades_df.columns else np.ones(len(trades_df)) * 0.007
            weights = atrs
            weights = weights / weights.sum()
            capital_allocations = weights * initial_capital
            capital_per_position = capital_allocations.mean()

        results.append({
            'strategy': strategy_name,
            'description': description,
            'avg_capital_per_position': capital_per_position,
            'capital_utilization': min(capital_per_position * max_positions / initial_capital, 1.0)
        })

    return pd.DataFrame(results)

def main():
    # Config
    INITIAL_CAPITAL = 100000
    MAX_POSITIONS = 10
    N_SIMULATIONS = 1000

    OUTPUT_DIR = Path('bt-strategy-test/HMA/position_sizing')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 80)
    log.info("POSITION SIZING ANALYSIS")
    log.info("=" * 80)
    log.info("")

    # Load trades
    trades_df = pd.read_csv('bt-strategy-test/HMA/limit_order_analysis/limit_close.csv')

    # Add ATR if not present (use placeholder)
    if 'atr_pct' not in trades_df.columns:
        log.warning("ATR not in trades data, using simulated values")
        # Simulate ATR values around 0.7% with some variation
        trades_df['atr_pct'] = np.random.normal(0.007, 0.002, len(trades_df))
        trades_df['atr_pct'] = trades_df['atr_pct'].clip(0.003, 0.015)

    log.info(f"Loaded {len(trades_df)} trades")
    log.info(f"Capital: ${INITIAL_CAPITAL:,.0f}")
    log.info(f"Max concurrent positions: {MAX_POSITIONS}")
    log.info("")

    # Test different sizing methods
    sizing_methods = {
        'fixed_10pct': 'Fixed 10% per trade (baseline)',
        'fixed_5pct': 'Fixed 5% per trade (conservative)',
        'atr_based': 'ATR-based risk parity (2% risk per trade)',
        'volatility_adjusted': 'Volatility-adjusted (target 1% vol)',
        'kelly': 'Kelly Criterion (half-Kelly for safety)'
    }

    log.info("Testing position sizing methods...")

    comparison_results = []

    for method, description in sizing_methods.items():
        log.info(f"  {method}: {description}")

        results = simulate_with_sizing_method(
            trades_df,
            method,
            INITIAL_CAPITAL,
            MAX_POSITIONS,
            N_SIMULATIONS
        )

        comparison_results.append({
            'method': method,
            'description': description,
            **results
        })

    comparison_df = pd.DataFrame(comparison_results)

    # Print comparison
    print("\n" + "=" * 100)
    print("POSITION SIZING METHOD COMPARISON")
    print("=" * 100)
    print(f"{'Method':<20} {'Mean Return':<12} {'Std Return':<12} {'Sharpe':<10} {'Mean DD':<10} {'Worst DD':<10}")
    print("-" * 100)

    for _, row in comparison_df.iterrows():
        print(f"{row['method']:<20} "
              f"{row['mean_return']*100:>10.2f}% "
              f"{row['std_return']*100:>10.2f}% "
              f"{row['sharpe']:>8.2f} "
              f"{row['mean_dd']*100:>8.2f}% "
              f"{row['worst_dd']*100:>8.2f}%")

    print("")

    # Save results
    output_file = OUTPUT_DIR / 'sizing_comparison.csv'
    comparison_df.to_csv(output_file, index=False)
    log.info(f"Saved comparison to: {output_file}")

    # Capital allocation analysis
    log.info("")
    log.info("Analyzing capital allocation strategies...")

    allocation_df = analyze_capital_allocation(trades_df, INITIAL_CAPITAL, MAX_POSITIONS)

    print("\n" + "=" * 80)
    print("CAPITAL ALLOCATION STRATEGIES")
    print("=" * 80)

    for _, row in allocation_df.iterrows():
        print(f"\n{row['strategy'].upper()}: {row['description']}")
        print(f"  Avg capital per position: ${row['avg_capital_per_position']:,.0f}")
        print(f"  Capital utilization: {row['capital_utilization']*100:.1f}%")

    print("")

    # Recommendations
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print("")

    best_method = comparison_df.loc[comparison_df['sharpe'].idxmax()]

    print(f"1. BEST RISK-ADJUSTED METHOD: {best_method['method']}")
    print(f"   - Sharpe: {best_method['sharpe']:.2f}")
    print(f"   - Mean return: {best_method['mean_return']*100:.2f}%")
    print(f"   - Max drawdown: {best_method['worst_dd']*100:.2f}%")
    print("")

    print("2. CAPITAL ALLOCATION WITH LIMITED CAPITAL:")
    print(f"   - Max concurrent positions: {MAX_POSITIONS}")
    print(f"   - Reserve per position: ${INITIAL_CAPITAL / MAX_POSITIONS:,.0f}")
    print(f"   - Actual position size: Dynamic based on method")
    print("")

    print("3. PRE-MARKET VOLATILITY FILTERING:")
    print("   - Measure ATR at 10:00 (after opening auction)")
    print("   - If ATR > 1.0%: Allocate more capital (high opportunity)")
    print("   - If ATR < 0.5%: Reduce capital (low opportunity)")
    print("   - This requires real-time data analysis")
    print("")

    print("4. DYNAMIC ALLOCATION STRATEGY:")
    print("   Step 1: At 09:30-10:00, monitor opening volatility")
    print("   Step 2: At 10:00, calculate ATR for each symbol")
    print("   Step 3: Rank symbols by ATR (high to low)")
    print("   Step 4: Allocate capital: more to high-ATR symbols")
    print("   Step 5: Set position size limits per symbol")
    print("")

    print("=" * 80)
    print("")

if __name__ == '__main__':
    main()
