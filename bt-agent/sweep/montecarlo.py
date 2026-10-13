"""
Monte Carlo con campionamento per giorno di trading.

Approccio: campiona N giorni casuali dalla storia (con rimpiazzo = bootstrap),
esegue la simulazione completa su quei giorni, registra le metriche.

Vantaggi rispetto al ricampionamento dei trade:
- Preserva la correlazione intraday (trade dello stesso giorno sono correlati)
- Rispetta la struttura temporale del segnale (HMA su barre continue)
- Evita artefatti di segnali generati su sequenze di barre non-contigue

Mantiene anche la classe legacy MonteCarlo (bootstrap su trade) per
compatibilità con codice esistente.
"""
from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)


# ── Day-sampling Monte Carlo ────────────────────────────────────────────────


class DayMonteCarlo:
    """
    Monte Carlo che campiona giorni di trading dalla storia disponibile.

    Uso
    ---
    mc = DayMonteCarlo(n_samples=1000, sample_size_days=252)

    results_df = mc.run(
        df=full_history_df,          # OHLCV con DatetimeIndex minute
        signal_fn=my_signal,         # f(df, **params) → Series segnali
        signal_params={"period": 16},
        backtest_params={"sl_pct": 0.005, "exitbar": 6},
    )

    stats = mc.analyze(results_df)
    """

    def __init__(
        self,
        n_samples: int = 1_000,
        sample_size_days: int = 252,
        replace: bool = True,
        n_jobs: int = 1,
    ):
        """
        Parameters
        ----------
        n_samples : numero di simulazioni da eseguire.
        sample_size_days : giorni di trading per simulazione.
        replace : campionamento con rimpiazzo (bootstrap). True = consigliato.
        n_jobs : parallelismo (1 = sequenziale, -1 = tutti i core).
        """
        self.n_samples = n_samples
        self.sample_size_days = sample_size_days
        self.replace = replace
        self.n_jobs = n_jobs

    def run(
        self,
        df: pd.DataFrame,
        signal_fn: Callable,
        signal_params: Optional[dict] = None,
        backtest_params: Optional[dict] = None,
        symbol: str = "SYM",
    ) -> pd.DataFrame:
        """
        Esegue n_samples simulazioni su giorni ricampionati.

        Parameters
        ----------
        df : DataFrame OHLCV con DatetimeIndex (tz-aware o naive, minute).
        signal_fn : funzione f(df, **params) → pd.Series segnali.
        signal_params : parametri per signal_fn.
        backtest_params : parametri per PandasBacktest.
        symbol : nome simbolo (per logging).

        Returns
        -------
        DataFrame con una riga per simulazione:
            n_trades, win_rate, expectancy_pct, sharpe, sqn, max_drawdown_pct,
            total_return_pct
        """
        from engine.backtest import PandasBacktest
        from engine.metrics import compute_metrics
        from strategies.generic import SignalStrategy

        signal_params = signal_params or {}
        backtest_params = backtest_params or {}

        # Identifica i giorni di trading disponibili
        trading_days = self._get_trading_days(df)
        n_available = len(trading_days)

        if n_available < self.sample_size_days:
            LOG.warning(
                f"{symbol}: {n_available} giorni disponibili < {self.sample_size_days} richiesti. "
                f"Riduco sample_size_days a {n_available}."
            )
            actual_size = n_available
        else:
            actual_size = self.sample_size_days

        LOG.info(
            f"DayMonteCarlo: {self.n_samples} simulazioni × {actual_size} giorni "
            f"su {n_available} disponibili (replace={self.replace})"
        )

        strategy = SignalStrategy(signal_fn, **signal_params)
        engine = PandasBacktest(**backtest_params)

        def _one_sim(seed: int) -> dict:
            rng = np.random.default_rng(seed)
            sampled_days = rng.choice(trading_days, size=actual_size, replace=self.replace)
            sampled_days_sorted = np.sort(sampled_days)

            # Ricostruisce DataFrame dai giorni campionati
            sim_df = self._build_df_from_days(df, sampled_days_sorted)
            if sim_df.empty or len(sim_df) < 10:
                return None

            try:
                trades = engine.run(sim_df, strategy, symbol=symbol)
                metrics = compute_metrics(trades)
                return metrics
            except Exception as exc:
                LOG.debug(f"Sim {seed} errore: {exc}")
                return None

        if self.n_jobs == 1:
            raw = [_one_sim(i) for i in range(self.n_samples)]
        else:
            try:
                from joblib import Parallel, delayed
                raw = Parallel(n_jobs=self.n_jobs)(
                    delayed(_one_sim)(i) for i in range(self.n_samples)
                )
            except ImportError:
                LOG.warning("joblib non disponibile, fallback sequenziale")
                raw = [_one_sim(i) for i in range(self.n_samples)]

        results = [r for r in raw if r is not None]
        if not results:
            LOG.error("Nessuna simulazione completata con successo.")
            return pd.DataFrame()

        LOG.info(f"Simulazioni completate: {len(results)}/{self.n_samples}")
        return pd.DataFrame(results)

    def analyze(self, results_df: pd.DataFrame) -> Dict:
        """
        Statistiche percentili sui risultati MC.

        Returns
        -------
        dict con statistiche per metrica chiave:
            total_return_pct, sharpe, max_drawdown_pct, win_rate
        """
        if results_df.empty:
            return {}

        def _stats(col: str) -> dict:
            if col not in results_df.columns:
                return {}
            s = results_df[col].dropna()
            return {
                "mean": round(s.mean(), 4),
                "median": round(s.median(), 4),
                "std": round(s.std(ddof=1), 4),
                "p5": round(s.quantile(0.05), 4),
                "p25": round(s.quantile(0.25), 4),
                "p75": round(s.quantile(0.75), 4),
                "p95": round(s.quantile(0.95), 4),
                "min": round(s.min(), 4),
                "max": round(s.max(), 4),
            }

        stats = {
            "total_return_pct": _stats("total_return_pct"),
            "sharpe": _stats("sharpe"),
            "max_drawdown_pct": _stats("max_drawdown_pct"),
            "win_rate": _stats("win_rate"),
            "n_trades": _stats("n_trades"),
        }

        if "total_return_pct" in results_df.columns:
            tr = results_df["total_return_pct"].dropna()
            stats["total_return_pct"]["prob_positive"] = round((tr > 0).mean(), 4)
            stats["total_return_pct"]["prob_gt_10pct"] = round((tr > 10).mean(), 4)
            stats["total_return_pct"]["prob_gt_20pct"] = round((tr > 20).mean(), 4)

        if "sharpe" in results_df.columns:
            sh = results_df["sharpe"].dropna()
            stats["sharpe"]["prob_gt_1"] = round((sh > 1.0).mean(), 4)
            stats["sharpe"]["prob_gt_0_5"] = round((sh > 0.5).mean(), 4)

        stats["n_simulations"] = len(results_df)
        stats["sample_size_days"] = self.sample_size_days
        stats["verdict"] = self._verdict(stats)
        return stats

    def print_report(self, stats: dict) -> None:
        """Stampa report leggibile dei risultati MC."""
        if not stats:
            print("Nessun risultato MC disponibile.")
            return

        print(f"\nMonte Carlo ({stats['n_simulations']} sim, {stats['sample_size_days']} giorni/sim)")
        print("─" * 65)

        for metric, label, fmt in [
            ("total_return_pct", "Total Return (%)", ".1f"),
            ("sharpe", "Sharpe", ".2f"),
            ("max_drawdown_pct", "Max Drawdown (%)", ".1f"),
            ("win_rate", "Win Rate (%)", ".1f"),
        ]:
            s = stats.get(metric, {})
            if not s:
                continue
            scale = 100 if metric == "win_rate" else 1
            p5 = s.get("p5", 0) * scale
            med = s.get("median", 0) * scale
            p95 = s.get("p95", 0) * scale
            print(f"  {label:<22} p5={p5:{fmt}}  median={med:{fmt}}  p95={p95:{fmt}}")

        tr = stats.get("total_return_pct", {})
        sh = stats.get("sharpe", {})
        print(f"\n  Prob(return > 0):   {tr.get('prob_positive', 0):.1%}")
        print(f"  Prob(return > 20%): {tr.get('prob_gt_20pct', 0):.1%}")
        print(f"  Prob(Sharpe > 1):   {sh.get('prob_gt_1', 0):.1%}")
        print(f"\n  Verdetto: {stats.get('verdict', 'N/A')}")

    @staticmethod
    def _get_trading_days(df: pd.DataFrame) -> np.ndarray:
        """Ritorna array di date di trading uniche."""
        if hasattr(df.index, 'date'):
            return np.array(sorted(set(df.index.date)))
        return np.array(sorted(set(df.index.normalize())))

    @staticmethod
    def _build_df_from_days(df: pd.DataFrame, days: np.ndarray) -> pd.DataFrame:
        """Ricostruisce DataFrame dai giorni campionati, in ordine."""
        if hasattr(df.index, 'date'):
            mask = pd.Series(df.index.date, index=df.index).isin(days)
        else:
            mask = df.index.normalize().isin(days)
        return df[mask].copy()

    @staticmethod
    def _verdict(stats: dict) -> str:
        p5_sharpe = stats.get("sharpe", {}).get("p5", 0)
        prob_positive = stats.get("total_return_pct", {}).get("prob_positive", 0)
        p5_dd = stats.get("max_drawdown_pct", {}).get("p5", 0)

        if p5_sharpe >= 1.0 and prob_positive >= 0.85:
            return "ROBUSTO — edge solido in >85% delle simulazioni"
        if p5_sharpe >= 0.5 and prob_positive >= 0.70:
            return "ACCETTABILE — monitorare in paper prima di live"
        if p5_sharpe < 0.3 or prob_positive < 0.55:
            return "FRAGILE — risultati probabilmente data-mined, non andare in paper"
        return "INCERTO — campione insufficiente o edge marginale"


# ── Legacy: bootstrap su trade (mantenuto per compatibilità) ────────────────


def simulate_trading_period(
    trades_df: pd.DataFrame,
    n_trades: int,
    initial_capital: float,
    position_size_pct: float,
    pnl_col: str = "exit_pct",
    seed: Optional[int] = None,
) -> Dict:
    """
    [LEGACY] Simula un periodo campionando trade con rimpiazzo.

    Preferire DayMonteCarlo per analisi di robustezza.
    Questa funzione è mantenuta per compatibilità con bin/HMA/ esistente.
    """
    rng = np.random.default_rng(seed)

    pnl = trades_df[pnl_col].values / 100.0
    sampled = rng.choice(pnl, size=n_trades, replace=True)

    capital = initial_capital
    equity = np.empty(n_trades + 1)
    equity[0] = capital
    for i, r in enumerate(sampled):
        capital += capital * position_size_pct * r
        equity[i + 1] = capital

    total_return = (equity[-1] - initial_capital) / initial_capital
    peak = np.maximum.accumulate(equity)
    max_drawdown = float(((equity - peak) / peak).min())
    wins = sampled > 0
    win_rate = float(wins.mean())

    if sampled.std(ddof=1) > 0:
        sharpe = float((sampled.mean() / sampled.std(ddof=1)) * np.sqrt(252))
    else:
        sharpe = 0.0

    max_win_streak = max_loss_streak = streak = 0
    streak_win = bool(wins[0]) if len(wins) > 0 else True
    for w in wins:
        if bool(w) == streak_win:
            streak += 1
        else:
            if streak_win:
                max_win_streak = max(max_win_streak, streak)
            else:
                max_loss_streak = max(max_loss_streak, streak)
            streak = 1
            streak_win = bool(w)
    if streak_win:
        max_win_streak = max(max_win_streak, streak)
    else:
        max_loss_streak = max(max_loss_streak, streak)

    return {
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "final_capital": float(equity[-1]),
        "win_rate": win_rate,
        "sharpe_ratio": sharpe,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "n_wins": int(wins.sum()),
        "n_losses": int((~wins).sum()),
    }


class MonteCarlo:
    """
    [LEGACY] Bootstrap su trade individuali.

    Preferire DayMonteCarlo per analisi di robustezza.
    """

    def __init__(
        self,
        n_simulations: int = 10_000,
        n_trades: int = 1_000,
        initial_capital: float = 100_000.0,
        position_size_pct: float = 0.10,
        pnl_col: str = "exit_pct",
    ):
        self.n_simulations = n_simulations
        self.n_trades = n_trades
        self.initial_capital = initial_capital
        self.position_size_pct = position_size_pct
        self.pnl_col = pnl_col

    def run(self, trades_df: pd.DataFrame) -> pd.DataFrame:
        LOG.info(f"[Legacy] MonteCarlo: {self.n_simulations} sim, {self.n_trades} trade/sim")
        results = [
            simulate_trading_period(
                trades_df, self.n_trades, self.initial_capital,
                self.position_size_pct, pnl_col=self.pnl_col, seed=i,
            )
            for i in range(self.n_simulations)
        ]
        return pd.DataFrame(results)

    def analyze(self, results_df: pd.DataFrame) -> Dict:
        def _stats(col):
            s = results_df[col]
            return {
                "mean": s.mean(), "median": s.median(), "std": s.std(),
                "p5": s.quantile(0.05), "p25": s.quantile(0.25),
                "p75": s.quantile(0.75), "p95": s.quantile(0.95),
                "min": s.min(), "max": s.max(),
            }

        stats = {
            "total_return": _stats("total_return"),
            "max_drawdown": _stats("max_drawdown"),
            "sharpe_ratio": _stats("sharpe_ratio"),
            "final_capital": _stats("final_capital"),
        }
        stats["total_return"]["prob_positive"] = (results_df["total_return"] > 0).mean()
        stats["total_return"]["prob_gt_10pct"] = (results_df["total_return"] > 0.10).mean()
        stats["total_return"]["prob_gt_20pct"] = (results_df["total_return"] > 0.20).mean()
        stats["max_drawdown"]["worst"] = results_df["max_drawdown"].min()
        stats["streaks"] = {
            "max_win_streak_mean": results_df["max_win_streak"].mean(),
            "max_win_streak_p95": results_df["max_win_streak"].quantile(0.95),
            "max_loss_streak_mean": results_df["max_loss_streak"].mean(),
            "max_loss_streak_p95": results_df["max_loss_streak"].quantile(0.95),
        }
        return stats
