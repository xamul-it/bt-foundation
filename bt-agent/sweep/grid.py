"""
GridSweep — grid search parallelo su parametri strategia.

Generico: funziona con qualsiasi BaseStrategy + PandasBacktest.
"""
from __future__ import annotations

import itertools
import logging
from typing import Any, Dict, List, Optional, Type

import pandas as pd
from joblib import Parallel, delayed

from engine.backtest import PandasBacktest
from engine.metrics import compute_metrics
from strategies.base import BaseStrategy

LOG = logging.getLogger(__name__)


def _run_single(
    strategy_cls: Type[BaseStrategy],
    strategy_params: dict,
    backtest_params: dict,
    df: pd.DataFrame,
    symbol: str,
) -> dict:
    """Esegue un singolo punto della griglia. Chiamato in parallelo."""
    strategy = strategy_cls(**strategy_params)
    engine = PandasBacktest(**backtest_params)
    trades = engine.run(df, strategy, symbol=symbol)
    metrics = compute_metrics(trades)
    return {**strategy_params, **backtest_params, **metrics, "symbol": symbol}


class GridSweep:
    """
    Grid search parallelo su parametri di strategia e/o backtest.

    Uso
    ---
    sweep = GridSweep(
        strategy_cls=HMAStrategy,
        strategy_grid={"period": [14, 16, 18], "inverted": [True]},
        backtest_grid={"sl_pct": [0.003, 0.005], "exitbar": [10, 20]},
        n_jobs=-1,
    )
    results = sweep.run(df, symbol="NVDA")
    results.sort_values("expectancy_pct", ascending=False)
    """

    def __init__(
        self,
        strategy_cls: Type[BaseStrategy],
        strategy_grid: Dict[str, List[Any]],
        backtest_grid: Optional[Dict[str, List[Any]]] = None,
        n_jobs: int = -1,
        verbose: int = 0,
    ):
        self.strategy_cls = strategy_cls
        self.strategy_grid = strategy_grid
        self.backtest_grid = backtest_grid or {}
        self.n_jobs = n_jobs
        self.verbose = verbose

    def run(self, df: pd.DataFrame, symbol: str = "UNKNOWN") -> pd.DataFrame:
        """
        Esegue la griglia su df.

        Returns
        -------
        DataFrame con colonne: tutti i parametri + metriche
        """
        strategy_combos = list(
            dict(zip(self.strategy_grid.keys(), v))
            for v in itertools.product(*self.strategy_grid.values())
        )
        backtest_combos = list(
            dict(zip(self.backtest_grid.keys(), v))
            for v in itertools.product(*self.backtest_grid.values())
        ) if self.backtest_grid else [{}]

        all_combos = [
            (sp, bp)
            for sp in strategy_combos
            for bp in backtest_combos
        ]

        LOG.info(f"GridSweep: {len(all_combos)} combinazioni, n_jobs={self.n_jobs}")

        results = Parallel(n_jobs=self.n_jobs, verbose=self.verbose)(
            delayed(_run_single)(
                self.strategy_cls, sp, bp, df, symbol
            )
            for sp, bp in all_combos
        )

        return pd.DataFrame(results)
