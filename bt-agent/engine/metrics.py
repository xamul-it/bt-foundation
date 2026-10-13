"""
Metriche di performance per trade DataFrame.

Calcola: Sharpe, max_drawdown, win_rate, SQN, expectancy.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Dict


def compute_metrics(
    trades: pd.DataFrame,
    initial_capital: float = 100_000.0,
    position_size_pct: float = 0.10,
    trades_per_year: int = 252,
) -> Dict:
    """
    Calcola metriche da un DataFrame di trades.

    Parameters
    ----------
    trades : output di PandasBacktest.run()
    initial_capital : capitale iniziale per equity curve
    position_size_pct : % del capitale per trade
    trades_per_year : stima annuale (usata per Sharpe normalizzato)

    Returns
    -------
    dict con: n_trades, win_rate, avg_win, avg_loss, expectancy,
              sharpe, sqn, max_drawdown, total_return, profit_factor
    """
    if trades is None or len(trades) == 0:
        return _empty_metrics()

    returns = trades["exit_pct"].values / 100.0  # decimali

    wins = returns[returns > 0]
    losses = returns[returns < 0]

    n = len(returns)
    win_rate = len(wins) / n if n > 0 else 0.0
    avg_win = wins.mean() * 100 if len(wins) > 0 else 0.0
    avg_loss = abs(losses.mean()) * 100 if len(losses) > 0 else 0.0
    expectancy = (win_rate * avg_win - (1 - win_rate) * avg_loss)

    # Equity curve
    capital = initial_capital
    equity = [capital]
    for r in returns:
        capital += capital * position_size_pct * r
        equity.append(capital)
    equity = np.array(equity)

    total_return = (equity[-1] - initial_capital) / initial_capital

    # Max drawdown
    peak = np.maximum.accumulate(equity)
    dd = (equity - peak) / peak
    max_drawdown = float(dd.min())

    # SQN (System Quality Number)
    if n >= 5:
        mean_r = returns.mean()
        std_r = returns.std(ddof=1)
        sqn = (mean_r / std_r) * np.sqrt(n) if std_r > 0 else 0.0
    else:
        sqn = 0.0

    # Sharpe (annualizzato, assumendo trades distribuiti uniformemente)
    if n >= 2 and returns.std(ddof=1) > 0:
        sharpe = (returns.mean() / returns.std(ddof=1)) * np.sqrt(trades_per_year)
    else:
        sharpe = 0.0

    profit_factor = (wins.sum() / abs(losses.sum())) if len(losses) > 0 and abs(losses.sum()) > 1e-10 else np.inf

    return {
        "n_trades": n,
        "win_rate": round(win_rate, 4),
        "avg_win_pct": round(avg_win, 4),
        "avg_loss_pct": round(avg_loss, 4),
        "expectancy_pct": round(expectancy, 4),
        "sharpe": round(sharpe, 4),
        "sqn": round(sqn, 4),
        "max_drawdown_pct": round(max_drawdown * 100, 4),
        "total_return_pct": round(total_return * 100, 4),
        "profit_factor": round(float(profit_factor), 4),
    }


def _empty_metrics() -> Dict:
    return {
        "n_trades": 0,
        "win_rate": 0.0,
        "avg_win_pct": 0.0,
        "avg_loss_pct": 0.0,
        "expectancy_pct": 0.0,
        "sharpe": 0.0,
        "sqn": 0.0,
        "max_drawdown_pct": 0.0,
        "total_return_pct": 0.0,
        "profit_factor": 0.0,
    }
