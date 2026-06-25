#!/usr/bin/env python3
"""
HMA Strategy - Monte Carlo Simulation

Uses validated trade results to simulate possible future outcomes via bootstrap resampling.

Generates:
- Distribution of annual returns
- Distribution of max drawdowns
- Sharpe ratio estimates
- Win/loss streak analysis
- Confidence intervals
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict
import logging
from datetime import datetime

# Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

# Seaborn style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

def load_trade_results(csv_file: Path) -> pd.DataFrame:
    """Load validated trade results"""
    df = pd.read_csv(csv_file)
    log.info(f"Loaded {len(df)} trades from {csv_file}")
    log.info(f"  Win rate: {(df['pnl_pct'] > 0).mean() * 100:.1f}%")
    log.info(f"  Avg PnL: {df['pnl_pct'].mean() * 100:.3f}%")
    log.info(f"  Avg Win: {df[df['pnl_pct'] > 0]['pnl_pct'].mean() * 100:.3f}%")
    log.info(f"  Avg Loss: {df[df['pnl_pct'] < 0]['pnl_pct'].mean() * 100:.3f}%")
    return df

def simulate_trading_period(
    trades_df: pd.DataFrame,
    n_trades: int,
    initial_capital: float,
    position_size_pct: float,
    seed: int = None
) -> Dict:
    """
    Simulate one trading period by resampling trades with replacement.

    Returns metrics for this simulation.
    """
    if seed is not None:
        np.random.seed(seed)

    # Resample trades with replacement
    sampled_trades = trades_df.sample(n=n_trades, replace=True)

    # Calculate equity curve
    capital = initial_capital
    equity_curve = [capital]
    trade_returns = []

    for pnl_pct in sampled_trades['pnl_pct']:
        position_size = capital * position_size_pct
        trade_pnl = position_size * pnl_pct
        capital += trade_pnl
        equity_curve.append(capital)
        trade_returns.append(trade_pnl / initial_capital)  # Normalize to initial capital

    equity_curve = np.array(equity_curve)

    # Calculate metrics
    total_return = (capital - initial_capital) / initial_capital

    # Max drawdown
    peak = np.maximum.accumulate(equity_curve)
    drawdown = (equity_curve - peak) / peak
    max_drawdown = drawdown.min()

    # Win rate
    wins = sampled_trades['pnl_pct'] > 0
    win_rate = wins.mean()

    # Sharpe ratio (annualized, assuming 252 trading days)
    if len(trade_returns) > 1:
        daily_returns = np.array(trade_returns)
        # Assume trades are distributed evenly across trading days
        trades_per_day = n_trades / 252
        # Resample to daily returns (sum trades per day)
        n_days = 252
        daily_return_series = []
        for day in range(n_days):
            # Randomly assign trades to days proportionally
            day_trades = int(trades_per_day)
            if np.random.random() < (trades_per_day - day_trades):
                day_trades += 1
            if day_trades > 0:
                day_return = daily_returns[day * int(trades_per_day):(day + 1) * int(trades_per_day)].sum()
                daily_return_series.append(day_return)
            else:
                daily_return_series.append(0.0)

        daily_return_series = np.array(daily_return_series[:n_days])
        sharpe = (daily_return_series.mean() / daily_return_series.std()) * np.sqrt(252) if daily_return_series.std() > 0 else 0
    else:
        sharpe = 0

    # Win/loss streaks
    streak = 0
    max_win_streak = 0
    max_loss_streak = 0
    current_streak_win = True

    for win in wins:
        if win:
            if current_streak_win:
                streak += 1
            else:
                max_loss_streak = max(max_loss_streak, streak)
                streak = 1
                current_streak_win = True
        else:
            if not current_streak_win:
                streak += 1
            else:
                max_win_streak = max(max_win_streak, streak)
                streak = 1
                current_streak_win = False

    # Finalize streaks
    if current_streak_win:
        max_win_streak = max(max_win_streak, streak)
    else:
        max_loss_streak = max(max_loss_streak, streak)

    return {
        'total_return': total_return,
        'max_drawdown': max_drawdown,
        'final_capital': capital,
        'win_rate': win_rate,
        'sharpe_ratio': sharpe,
        'max_win_streak': max_win_streak,
        'max_loss_streak': max_loss_streak,
        'n_wins': wins.sum(),
        'n_losses': (~wins).sum()
    }

def run_monte_carlo(
    trades_df: pd.DataFrame,
    n_simulations: int,
    n_trades_per_simulation: int,
    initial_capital: float,
    position_size_pct: float
) -> pd.DataFrame:
    """Run Monte Carlo simulation"""

    log.info(f"Running {n_simulations:,} Monte Carlo simulations...")
    log.info(f"  Trades per simulation: {n_trades_per_simulation}")
    log.info(f"  Initial capital: ${initial_capital:,.0f}")
    log.info(f"  Position size: {position_size_pct * 100:.1f}%")
    log.info("")

    results = []

    for i in range(n_simulations):
        if (i + 1) % 1000 == 0:
            log.info(f"  Completed {i + 1:,} / {n_simulations:,} simulations...")

        result = simulate_trading_period(
            trades_df,
            n_trades_per_simulation,
            initial_capital,
            position_size_pct,
            seed=i
        )
        results.append(result)

    log.info(f"✅ Completed all {n_simulations:,} simulations")
    log.info("")

    return pd.DataFrame(results)

def analyze_results(results_df: pd.DataFrame, initial_capital: float) -> Dict:
    """Analyze Monte Carlo results"""

    stats = {
        'total_return': {
            'mean': results_df['total_return'].mean(),
            'median': results_df['total_return'].median(),
            'std': results_df['total_return'].std(),
            'p5': results_df['total_return'].quantile(0.05),
            'p25': results_df['total_return'].quantile(0.25),
            'p75': results_df['total_return'].quantile(0.75),
            'p95': results_df['total_return'].quantile(0.95),
            'min': results_df['total_return'].min(),
            'max': results_df['total_return'].max(),
            'prob_positive': (results_df['total_return'] > 0).mean(),
            'prob_gt_10pct': (results_df['total_return'] > 0.10).mean(),
            'prob_gt_15pct': (results_df['total_return'] > 0.15).mean(),
            'prob_gt_20pct': (results_df['total_return'] > 0.20).mean(),
        },
        'max_drawdown': {
            'mean': results_df['max_drawdown'].mean(),
            'median': results_df['max_drawdown'].median(),
            'std': results_df['max_drawdown'].std(),
            'p5': results_df['max_drawdown'].quantile(0.05),
            'p95': results_df['max_drawdown'].quantile(0.95),
            'worst': results_df['max_drawdown'].min(),
        },
        'sharpe_ratio': {
            'mean': results_df['sharpe_ratio'].mean(),
            'median': results_df['sharpe_ratio'].median(),
            'std': results_df['sharpe_ratio'].std(),
            'p5': results_df['sharpe_ratio'].quantile(0.05),
            'p95': results_df['sharpe_ratio'].quantile(0.95),
        },
        'final_capital': {
            'mean': results_df['final_capital'].mean(),
            'median': results_df['final_capital'].median(),
            'p5': results_df['final_capital'].quantile(0.05),
            'p95': results_df['final_capital'].quantile(0.95),
            'prob_gt_initial': (results_df['final_capital'] > initial_capital).mean(),
        },
        'streaks': {
            'max_win_streak_mean': results_df['max_win_streak'].mean(),
            'max_win_streak_p95': results_df['max_win_streak'].quantile(0.95),
            'max_loss_streak_mean': results_df['max_loss_streak'].mean(),
            'max_loss_streak_p95': results_df['max_loss_streak'].quantile(0.95),
        }
    }

    return stats

def plot_distributions(results_df: pd.DataFrame, output_dir: Path, initial_capital: float):
    """Create distribution plots"""

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('HMA Strategy - Monte Carlo Simulation Results (10,000 simulations)', fontsize=16, fontweight='bold')

    # 1. Total Return Distribution
    ax = axes[0, 0]
    returns_pct = results_df['total_return'] * 100
    ax.hist(returns_pct, bins=50, alpha=0.7, color='steelblue', edgecolor='black')
    ax.axvline(returns_pct.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {returns_pct.mean():.1f}%')
    ax.axvline(returns_pct.median(), color='orange', linestyle='--', linewidth=2, label=f'Median: {returns_pct.median():.1f}%')
    ax.axvline(0, color='black', linestyle='-', linewidth=1, alpha=0.5)
    ax.set_xlabel('Annual Return (%)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of Annual Returns', fontsize=13, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 2. Max Drawdown Distribution
    ax = axes[0, 1]
    dd_pct = results_df['max_drawdown'] * 100
    ax.hist(dd_pct, bins=50, alpha=0.7, color='indianred', edgecolor='black')
    ax.axvline(dd_pct.mean(), color='darkred', linestyle='--', linewidth=2, label=f'Mean: {dd_pct.mean():.1f}%')
    ax.axvline(dd_pct.median(), color='orange', linestyle='--', linewidth=2, label=f'Median: {dd_pct.median():.1f}%')
    ax.set_xlabel('Max Drawdown (%)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of Max Drawdown', fontsize=13, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 3. Sharpe Ratio Distribution
    ax = axes[0, 2]
    ax.hist(results_df['sharpe_ratio'], bins=50, alpha=0.7, color='seagreen', edgecolor='black')
    ax.axvline(results_df['sharpe_ratio'].mean(), color='darkgreen', linestyle='--', linewidth=2,
               label=f'Mean: {results_df["sharpe_ratio"].mean():.2f}')
    ax.axvline(results_df['sharpe_ratio'].median(), color='orange', linestyle='--', linewidth=2,
               label=f'Median: {results_df["sharpe_ratio"].median():.2f}')
    ax.set_xlabel('Sharpe Ratio', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of Sharpe Ratio', fontsize=13, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 4. Final Capital Distribution
    ax = axes[1, 0]
    final_cap = results_df['final_capital'] / 1000  # in thousands
    ax.hist(final_cap, bins=50, alpha=0.7, color='goldenrod', edgecolor='black')
    ax.axvline(final_cap.mean(), color='darkgoldenrod', linestyle='--', linewidth=2,
               label=f'Mean: ${final_cap.mean():.1f}k')
    ax.axvline(initial_capital / 1000, color='black', linestyle='-', linewidth=1, alpha=0.5,
               label=f'Initial: ${initial_capital / 1000:.0f}k')
    ax.set_xlabel('Final Capital ($k)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of Final Capital', fontsize=13, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # 5. Return vs Drawdown Scatter
    ax = axes[1, 1]
    scatter = ax.scatter(dd_pct, returns_pct, alpha=0.3, c=results_df['sharpe_ratio'],
                        cmap='RdYlGn', s=20, edgecolors='none')
    ax.axhline(0, color='black', linestyle='-', linewidth=1, alpha=0.5)
    ax.set_xlabel('Max Drawdown (%)', fontsize=12)
    ax.set_ylabel('Annual Return (%)', fontsize=12)
    ax.set_title('Return vs Drawdown (colored by Sharpe)', fontsize=13, fontweight='bold')
    plt.colorbar(scatter, ax=ax, label='Sharpe Ratio')
    ax.grid(True, alpha=0.3)

    # 6. Cumulative Probability - Annual Return
    ax = axes[1, 2]
    sorted_returns = np.sort(returns_pct)
    cumulative_prob = np.arange(1, len(sorted_returns) + 1) / len(sorted_returns)
    ax.plot(sorted_returns, cumulative_prob * 100, linewidth=2, color='steelblue')
    ax.axvline(0, color='red', linestyle='--', linewidth=1, alpha=0.7, label='Break-even')
    ax.axhline(50, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax.set_xlabel('Annual Return (%)', fontsize=12)
    ax.set_ylabel('Cumulative Probability (%)', fontsize=12)
    ax.set_title('Cumulative Distribution - Annual Return', fontsize=13, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save
    output_file = output_dir / 'monte_carlo_distributions.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    log.info(f"Saved distribution plots to: {output_file}")
    plt.close()

def print_summary(stats: Dict, n_simulations: int, n_trades: int, initial_capital: float):
    """Print summary statistics"""

    print("\n" + "=" * 80)
    print("MONTE CARLO SIMULATION SUMMARY")
    print("=" * 80)
    print(f"Simulations: {n_simulations:,}")
    print(f"Trades per simulation: {n_trades:,} (equivalent to ~1 year of trading)")
    print(f"Initial capital: ${initial_capital:,.0f}")
    print("")

    print("─" * 80)
    print("ANNUAL RETURN")
    print("─" * 80)
    print(f"  Mean:                {stats['total_return']['mean'] * 100:>8.2f}%")
    print(f"  Median:              {stats['total_return']['median'] * 100:>8.2f}%")
    print(f"  Std Dev:             {stats['total_return']['std'] * 100:>8.2f}%")
    print(f"  5th percentile:      {stats['total_return']['p5'] * 100:>8.2f}%")
    print(f"  95th percentile:     {stats['total_return']['p95'] * 100:>8.2f}%")
    print(f"  Best case:           {stats['total_return']['max'] * 100:>8.2f}%")
    print(f"  Worst case:          {stats['total_return']['min'] * 100:>8.2f}%")
    print("")
    print(f"  Prob(return > 0%):   {stats['total_return']['prob_positive'] * 100:>8.1f}%")
    print(f"  Prob(return > 10%):  {stats['total_return']['prob_gt_10pct'] * 100:>8.1f}%")
    print(f"  Prob(return > 15%):  {stats['total_return']['prob_gt_15pct'] * 100:>8.1f}%")
    print(f"  Prob(return > 20%):  {stats['total_return']['prob_gt_20pct'] * 100:>8.1f}%")
    print("")

    print("─" * 80)
    print("MAX DRAWDOWN")
    print("─" * 80)
    print(f"  Mean:                {stats['max_drawdown']['mean'] * 100:>8.2f}%")
    print(f"  Median:              {stats['max_drawdown']['median'] * 100:>8.2f}%")
    print(f"  Std Dev:             {stats['max_drawdown']['std'] * 100:>8.2f}%")
    print(f"  5th percentile:      {stats['max_drawdown']['p5'] * 100:>8.2f}%  (best case)")
    print(f"  95th percentile:     {stats['max_drawdown']['p95'] * 100:>8.2f}%  (worst case)")
    print(f"  Worst ever:          {stats['max_drawdown']['worst'] * 100:>8.2f}%")
    print("")

    print("─" * 80)
    print("SHARPE RATIO (Annualized)")
    print("─" * 80)
    print(f"  Mean:                {stats['sharpe_ratio']['mean']:>8.2f}")
    print(f"  Median:              {stats['sharpe_ratio']['median']:>8.2f}")
    print(f"  Std Dev:             {stats['sharpe_ratio']['std']:>8.2f}")
    print(f"  5th percentile:      {stats['sharpe_ratio']['p5']:>8.2f}")
    print(f"  95th percentile:     {stats['sharpe_ratio']['p95']:>8.2f}")
    print("")

    print("─" * 80)
    print("FINAL CAPITAL")
    print("─" * 80)
    print(f"  Mean:                ${stats['final_capital']['mean']:>12,.0f}")
    print(f"  Median:              ${stats['final_capital']['median']:>12,.0f}")
    print(f"  5th percentile:      ${stats['final_capital']['p5']:>12,.0f}")
    print(f"  95th percentile:     ${stats['final_capital']['p95']:>12,.0f}")
    print(f"  Prob(profit):        {stats['final_capital']['prob_gt_initial'] * 100:>8.1f}%")
    print("")

    print("─" * 80)
    print("WIN/LOSS STREAKS")
    print("─" * 80)
    print(f"  Avg max win streak:  {stats['streaks']['max_win_streak_mean']:>8.1f} trades")
    print(f"  95% max win streak:  {stats['streaks']['max_win_streak_p95']:>8.0f} trades")
    print(f"  Avg max loss streak: {stats['streaks']['max_loss_streak_mean']:>8.1f} trades")
    print(f"  95% max loss streak: {stats['streaks']['max_loss_streak_p95']:>8.0f} trades")
    print("")

    print("=" * 80)
    print("INTERPRETATION")
    print("=" * 80)

    # Risk assessment
    if stats['total_return']['prob_positive'] >= 0.90:
        risk_level = "✅ VERY LOW"
    elif stats['total_return']['prob_positive'] >= 0.75:
        risk_level = "✅ LOW"
    elif stats['total_return']['prob_positive'] >= 0.60:
        risk_level = "⚠️  MODERATE"
    else:
        risk_level = "❌ HIGH"

    print(f"Risk of Loss:        {risk_level} ({(1 - stats['total_return']['prob_positive']) * 100:.1f}% chance of negative return)")

    # Sharpe interpretation
    if stats['sharpe_ratio']['mean'] >= 2.0:
        sharpe_quality = "✅ EXCELLENT"
    elif stats['sharpe_ratio']['mean'] >= 1.0:
        sharpe_quality = "✅ GOOD"
    elif stats['sharpe_ratio']['mean'] >= 0.5:
        sharpe_quality = "⚠️  ACCEPTABLE"
    else:
        sharpe_quality = "❌ POOR"

    print(f"Sharpe Quality:      {sharpe_quality} (mean {stats['sharpe_ratio']['mean']:.2f})")

    # Drawdown tolerance
    if abs(stats['max_drawdown']['p95']) <= 0.10:
        dd_level = "✅ LOW (< 10%)"
    elif abs(stats['max_drawdown']['p95']) <= 0.20:
        dd_level = "⚠️  MODERATE (10-20%)"
    else:
        dd_level = "❌ HIGH (> 20%)"

    print(f"Drawdown Risk:       {dd_level} (95th percentile: {stats['max_drawdown']['p95'] * 100:.1f}%)")

    print("")
    print("=" * 80)
    print("")

def main():
    # Configuration
    TRADES_FILE = Path('bt-strategy-test/HMA/limit_order_analysis/limit_close.csv')
    OUTPUT_DIR = Path('bt-strategy-test/HMA/monte_carlo')
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Simulation parameters
    N_SIMULATIONS = 10000
    INITIAL_CAPITAL = 100000  # $100k
    POSITION_SIZE_PCT = 0.10  # 10% per trade

    # Based on validation: ~4.4 trades/day, 252 trading days/year
    TRADES_PER_DAY = 4.4
    TRADING_DAYS_PER_YEAR = 252
    N_TRADES_PER_SIMULATION = int(TRADES_PER_DAY * TRADING_DAYS_PER_YEAR)  # ~1,109 trades

    log.info("=" * 80)
    log.info("HMA STRATEGY - MONTE CARLO SIMULATION")
    log.info("=" * 80)
    log.info("")

    # Load trade results
    trades_df = load_trade_results(TRADES_FILE)
    log.info("")

    # Run Monte Carlo
    results_df = run_monte_carlo(
        trades_df,
        N_SIMULATIONS,
        N_TRADES_PER_SIMULATION,
        INITIAL_CAPITAL,
        POSITION_SIZE_PCT
    )

    # Analyze results
    stats = analyze_results(results_df, INITIAL_CAPITAL)

    # Save results
    results_file = OUTPUT_DIR / 'simulation_results.csv'
    results_df.to_csv(results_file, index=False)
    log.info(f"Saved detailed results to: {results_file}")
    log.info("")

    # Plot distributions
    plot_distributions(results_df, OUTPUT_DIR, INITIAL_CAPITAL)
    log.info("")

    # Print summary
    print_summary(stats, N_SIMULATIONS, N_TRADES_PER_SIMULATION, INITIAL_CAPITAL)

    # Save summary
    summary_file = OUTPUT_DIR / 'summary.txt'
    with open(summary_file, 'w') as f:
        import sys
        from io import StringIO

        old_stdout = sys.stdout
        sys.stdout = buffer = StringIO()

        print_summary(stats, N_SIMULATIONS, N_TRADES_PER_SIMULATION, INITIAL_CAPITAL)

        sys.stdout = old_stdout
        f.write(buffer.getvalue())

    log.info(f"Saved summary to: {summary_file}")

if __name__ == '__main__':
    main()
