#!/usr/bin/env python3
"""
HMA Strategy - Monte Carlo with Slippage Comparison

Compares Monte Carlo results with different execution methods:
1. Limit orders (zero slippage)
2. Market orders (0.03% slippage)
3. Conservative estimate (0.05% slippage)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

sns.set_style("whitegrid")

def monte_carlo_simulation(
    trades_df: pd.DataFrame,
    n_simulations: int,
    n_trades: int,
    initial_capital: float,
    position_size_pct: float
) -> np.ndarray:
    """Run Monte Carlo simulation and return array of total returns"""

    results = []

    for i in range(n_simulations):
        sampled = trades_df.sample(n=n_trades, replace=True, random_state=i)
        capital = initial_capital

        for pnl_pct in sampled['pnl_pct']:
            capital += capital * position_size_pct * pnl_pct

        total_return = (capital - initial_capital) / initial_capital
        results.append(total_return)

    return np.array(results)

def main():
    # Config
    N_SIMULATIONS = 10000
    N_TRADES = 1108  # ~1 year (4.4 trades/day * 252 days)
    INITIAL_CAPITAL = 100000
    POSITION_SIZE_PCT = 0.10

    OUTPUT_DIR = Path('bt-strategy-test/HMA/monte_carlo')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 80)
    log.info("MONTE CARLO SIMULATION - SLIPPAGE COMPARISON")
    log.info("=" * 80)
    log.info("")

    # Load trade results
    limit_trades = pd.read_csv('bt-strategy-test/HMA/limit_order_analysis/limit_close.csv')
    market_trades = pd.read_csv('bt-strategy-test/HMA/limit_order_analysis/market.csv')

    log.info("Trade Statistics:")
    log.info(f"  Limit orders:  {len(limit_trades)} trades, avg PnL: {limit_trades['pnl_pct'].mean()*100:.3f}%")
    log.info(f"  Market orders: {len(market_trades)} trades, avg PnL: {market_trades['pnl_pct'].mean()*100:.3f}%")
    log.info("")

    # Run simulations
    log.info(f"Running {N_SIMULATIONS:,} simulations for each scenario...")

    results_limit = monte_carlo_simulation(limit_trades, N_SIMULATIONS, N_TRADES, INITIAL_CAPITAL, POSITION_SIZE_PCT)
    results_market = monte_carlo_simulation(market_trades, N_SIMULATIONS, N_TRADES, INITIAL_CAPITAL, POSITION_SIZE_PCT)

    # Conservative: Add extra 0.02% slippage to market orders
    conservative_trades = market_trades.copy()
    conservative_trades['pnl_pct'] -= 0.0002  # Additional 0.02% slippage
    results_conservative = monte_carlo_simulation(conservative_trades, N_SIMULATIONS, N_TRADES, INITIAL_CAPITAL, POSITION_SIZE_PCT)

    log.info("✅ Simulations complete")
    log.info("")

    # Summary
    scenarios = {
        'Limit (0% slip)': results_limit,
        'Market (0.03% slip)': results_market,
        'Conservative (0.05% slip)': results_conservative
    }

    print("\n" + "=" * 80)
    print("ANNUAL RETURN COMPARISON")
    print("=" * 80)
    print(f"{'Scenario':<30} {'Mean':<10} {'Median':<10} {'5%ile':<10} {'95%ile':<10} {'P(profit)':<10}")
    print("-" * 80)

    for name, results in scenarios.items():
        print(f"{name:<30} "
              f"{results.mean()*100:>8.2f}% "
              f"{np.median(results)*100:>8.2f}% "
              f"{np.percentile(results, 5)*100:>8.2f}% "
              f"{np.percentile(results, 95)*100:>8.2f}% "
              f"{(results > 0).mean()*100:>8.1f}%")

    print("")
    print("=" * 80)
    print("SLIPPAGE IMPACT")
    print("=" * 80)

    baseline = results_limit.mean()
    market_cost = (baseline - results_market.mean()) / baseline * 100
    conservative_cost = (baseline - results_conservative.mean()) / baseline * 100

    print(f"Market orders (0.03%):      -{market_cost:.1f}% of returns")
    print(f"Conservative (0.05%):       -{conservative_cost:.1f}% of returns")
    print("")

    # Plot comparison
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Distribution comparison
    ax = axes[0]
    for name, results in scenarios.items():
        ax.hist(results * 100, bins=50, alpha=0.5, label=name, density=True)
    ax.axvline(0, color='red', linestyle='--', linewidth=1, alpha=0.7)
    ax.set_xlabel('Annual Return (%)', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title('Distribution Comparison', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Box plot
    ax = axes[1]
    data = [results_limit * 100, results_market * 100, results_conservative * 100]
    labels = ['Limit\n(0% slip)', 'Market\n(0.03% slip)', 'Conservative\n(0.05% slip)']
    bp = ax.boxplot(data, labels=labels, patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor('lightblue')
    ax.axhline(0, color='red', linestyle='--', linewidth=1, alpha=0.7)
    ax.set_ylabel('Annual Return (%)', fontsize=12)
    ax.set_title('Return Distribution by Execution Type', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    output_file = OUTPUT_DIR / 'slippage_comparison.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    log.info(f"Saved plot to: {output_file}")

    # Save results
    summary_df = pd.DataFrame({
        'scenario': list(scenarios.keys()),
        'mean_return': [r.mean() for r in scenarios.values()],
        'median_return': [np.median(r) for r in scenarios.values()],
        'std_return': [r.std() for r in scenarios.values()],
        'p5_return': [np.percentile(r, 5) for r in scenarios.values()],
        'p95_return': [np.percentile(r, 95) for r in scenarios.values()],
        'prob_profit': [(r > 0).mean() for r in scenarios.values()],
    })

    summary_file = OUTPUT_DIR / 'slippage_comparison.csv'
    summary_df.to_csv(summary_file, index=False)
    log.info(f"Saved summary to: {summary_file}")
    log.info("")

    print("=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    print("Use CONSERVATIVE estimate (0.05% slippage) for realistic expectations:")
    print(f"  Expected annual return: {results_conservative.mean()*100:.2f}%")
    print(f"  90% confidence interval: {np.percentile(results_conservative, 5)*100:.2f}% to {np.percentile(results_conservative, 95)*100:.2f}%")
    print(f"  Probability of profit: {(results_conservative > 0).mean()*100:.1f}%")
    print("")

if __name__ == '__main__':
    main()
