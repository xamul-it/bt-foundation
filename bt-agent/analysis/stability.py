"""
StabilityAnalyzer — analisi di stabilità parametri su finestre temporali.

Risponde a: "la strategia è stabile in periodi diversi?"

Due approcci:
1. Sliding window: suddivide il dataset in finestre (es. mensile) e
   calcola metriche per ciascuna. Verifica che expectancy/Sharpe
   siano consistenti attraverso le finestre.

2. Monte Carlo window: per ciascuna finestra esegue MC bootstrap
   e calcola distribuzione di rendimento/drawdown. Verifica che
   i percentili siano stabili (bassa varianza cross-finestra).
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Type

import numpy as np
import pandas as pd

from engine.backtest import PandasBacktest
from engine.metrics import compute_metrics
from strategies.base import BaseStrategy
from sweep.montecarlo import MonteCarlo

LOG = logging.getLogger(__name__)


class StabilityAnalyzer:
    """
    Analizza la stabilità della strategia su più finestre temporali.

    Uso
    ---
    analyzer = StabilityAnalyzer(
        strategy_cls=MyStrategy,
        strategy_params={"period": 16},
        backtest_params={"sl_pct": 0.005},
    )
    df = analyzer.analyze(full_df, window="M", symbol="NVDA")
    summary = analyzer.summary(df)
    """

    def __init__(
        self,
        strategy_cls: Type[BaseStrategy],
        strategy_params: dict,
        backtest_params: Optional[dict] = None,
        mc_simulations: int = 2000,
        mc_trades_per_sim: int = 500,
    ):
        self.strategy_cls = strategy_cls
        self.strategy_params = strategy_params
        self.backtest_params = backtest_params or {}
        self.mc_simulations = mc_simulations
        self.mc_trades_per_sim = mc_trades_per_sim

    def analyze(
        self,
        df: pd.DataFrame,
        window: str = "ME",
        symbol: str = "UNKNOWN",
        run_mc: bool = False,
    ) -> pd.DataFrame:
        """
        Calcola metriche per ogni finestra temporale.

        Parameters
        ----------
        df : OHLCV DataFrame.
        window : frequenza pandas ('ME'=mese, 'W'=settimana, 'QE'=trimestre).
        symbol : nome simbolo.
        run_mc : se True, esegue MC per ogni finestra (più lento).

        Returns
        -------
        DataFrame con una riga per finestra:
            window, n_trades, expectancy_pct, win_rate, sharpe, sqn,
            max_drawdown_pct, [mc_p5, mc_median, mc_p95] se run_mc=True
        """
        strategy = self.strategy_cls(**self.strategy_params)
        engine = PandasBacktest(**self.backtest_params)

        rows = []
        periods = pd.period_range(
            df.index.min(), df.index.max(), freq=window
        )

        for period in periods:
            start = period.start_time
            end = period.end_time
            window_df = df[(df.index >= start) & (df.index <= end)]

            if len(window_df) < 50:
                LOG.debug(f"Finestra {period}: troppo pochi dati ({len(window_df)} barre), skip")
                continue

            trades = engine.run(window_df, strategy, symbol=symbol)
            metrics = compute_metrics(trades)

            row = {
                "window": str(period),
                "start": start.strftime("%Y-%m-%d"),
                "end": end.strftime("%Y-%m-%d"),
                "n_trades": metrics["n_trades"],
                "expectancy_pct": metrics["expectancy_pct"],
                "win_rate": metrics["win_rate"],
                "sharpe": metrics["sharpe"],
                "sqn": metrics["sqn"],
                "max_drawdown_pct": metrics["max_drawdown_pct"],
            }

            if run_mc and metrics["n_trades"] >= 5:
                mc = MonteCarlo(
                    n_simulations=self.mc_simulations,
                    n_trades=min(self.mc_trades_per_sim, metrics["n_trades"] * 10),
                )
                mc_results = mc.run(trades)
                mc_stats = mc.analyze(mc_results)
                tr = mc_stats.get("total_return", {})
                row.update({
                    "mc_p5": tr.get("p5", 0) * 100,
                    "mc_median": tr.get("median", 0) * 100,
                    "mc_p95": tr.get("p95", 0) * 100,
                    "mc_prob_positive": tr.get("prob_positive", 0) * 100,
                })

            rows.append(row)
            LOG.info(f"Finestra {period}: {metrics['n_trades']} trade, "
                     f"expectancy={metrics['expectancy_pct']:+.3f}%")

        return pd.DataFrame(rows)

    def summary(self, df: pd.DataFrame) -> Dict:
        """
        Statistiche di stabilità cross-finestra.

        Una strategia stabile ha:
        - expectancy positiva in >70% delle finestre
        - bassa deviazione std dell'expectancy (CV < 1)
        - nessuna finestra con drawdown > 2x la media
        """
        if df.empty:
            return {}

        exp = df["expectancy_pct"]
        n_windows = len(df)
        n_positive = (exp > 0).sum()

        cv = exp.std(ddof=1) / abs(exp.mean()) if abs(exp.mean()) > 1e-6 else float("inf")

        dd_mean = df["max_drawdown_pct"].mean()
        dd_max = df["max_drawdown_pct"].min()  # più negativo = peggiore
        dd_outlier = (df["max_drawdown_pct"] < dd_mean * 2).sum() if dd_mean < 0 else 0

        stability_score = _compute_stability_score(df)

        return {
            "n_windows": n_windows,
            "n_positive_expectancy": int(n_positive),
            "pct_positive": round(n_positive / n_windows * 100, 1),
            "expectancy_mean": round(exp.mean(), 4),
            "expectancy_std": round(exp.std(ddof=1), 4),
            "expectancy_cv": round(cv, 2),
            "win_rate_mean": round(df["win_rate"].mean(), 4),
            "win_rate_std": round(df["win_rate"].std(ddof=1), 4),
            "sharpe_mean": round(df["sharpe"].mean(), 4),
            "max_drawdown_mean_pct": round(dd_mean, 4),
            "max_drawdown_worst_pct": round(dd_max, 4),
            "dd_outlier_windows": int(dd_outlier),
            "stability_score": round(stability_score, 2),
            "verdict": _stability_verdict(stability_score, n_positive / n_windows),
        }

    def compare_windows(
        self,
        df: pd.DataFrame,
        other_df: pd.DataFrame,
        label_a: str = "Params A",
        label_b: str = "Params B",
    ) -> pd.DataFrame:
        """
        Confronta due set di risultati finestra per finestra.

        Utile per confrontare parametri diversi sulla stessa base dati.
        """
        merged = pd.merge(
            df.rename(columns=lambda c: f"{c}_a" if c != "window" else c),
            other_df.rename(columns=lambda c: f"{c}_b" if c != "window" else c),
            on="window", how="outer"
        )
        merged["winner"] = np.where(
            merged["expectancy_pct_a"] > merged["expectancy_pct_b"], label_a, label_b
        )
        return merged


def _compute_stability_score(df: pd.DataFrame) -> float:
    """
    Score [0-100] che sintetizza la stabilità.

    Componenti:
    - 40%: % finestre con expectancy > 0
    - 30%: 1 - CV(expectancy) normalizzato
    - 30%: Sharpe medio normalizzato
    """
    n = len(df)
    if n == 0:
        return 0.0

    pct_positive = (df["expectancy_pct"] > 0).mean()
    score_positive = pct_positive * 40

    exp_mean = df["expectancy_pct"].mean()
    exp_std = df["expectancy_pct"].std(ddof=1) if n > 1 else 0
    cv = exp_std / abs(exp_mean) if abs(exp_mean) > 1e-6 else 10
    score_cv = max(0, 1 - cv / 3) * 30  # CV < 1 = perfetto, CV > 3 = 0

    sharpe_norm = min(df["sharpe"].mean() / 2, 1)  # Sharpe 2 = perfetto
    score_sharpe = max(0, sharpe_norm) * 30

    return score_positive + score_cv + score_sharpe


def _stability_verdict(score: float, pct_positive: float) -> str:
    if score >= 70 and pct_positive >= 0.7:
        return "STABILE — parametri robusti"
    if score >= 50 and pct_positive >= 0.5:
        return "ACCETTABILE — monitorare"
    if pct_positive < 0.4:
        return "INSTABILE — evitare in produzione"
    return "INCERTO — campione insufficiente"
