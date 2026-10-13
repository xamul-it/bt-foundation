"""
FilterSweep — sweep multivariato di filtri aggiuntivi su segnali intraday.

Un filtro è una maschera booleana applicata al segnale: dove il filtro è False,
il segnale viene azzerato (nessun entry). I filtri riducono il numero di trade
eliminando quelli a bassa probabilità.

Uso tipico: trovare il sottoinsieme di condizioni di mercato in cui la strategia
funziona meglio, senza modificare la logica del segnale.

Uso
---
    from sweep.filter_sweep import FilterSweep, COMMON_FILTERS

    sweep = FilterSweep(signal_fn=my_signal, signal_params={"period": 16})
    results = sweep.run(df, filters=COMMON_FILTERS)
    sweep.print_report(results)
"""
from __future__ import annotations

import logging
from datetime import time
from itertools import combinations
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)


# ── Filtri predefiniti ───────────────────────────────────────────────────────

def _no_open_30min(df: pd.DataFrame) -> pd.Series:
    """No entry nei primi 30 minuti dopo l'apertura (9:30–10:00)."""
    return pd.Series(df.index.time >= time(10, 0), index=df.index)

def _no_last_30min(df: pd.DataFrame) -> pd.Series:
    """No entry negli ultimi 30 minuti prima della chiusura (15:30–16:00)."""
    return pd.Series(df.index.time <= time(15, 30), index=df.index)

def _morning_only(df: pd.DataFrame) -> pd.Series:
    """Solo entry nella mattina (9:30–12:00)."""
    return pd.Series(df.index.time <= time(12, 0), index=df.index)

def _afternoon_only(df: pd.DataFrame) -> pd.Series:
    """Solo entry nel pomeriggio (13:00–15:30)."""
    t = df.index.time
    return pd.Series((t >= time(13, 0)) & (t <= time(15, 30)), index=df.index)

def _high_volume(df: pd.DataFrame, window: int = 20, mult: float = 1.2) -> pd.Series:
    """Volume sopra la media mobile (volume sopra la norma)."""
    avg_vol = df["volume"].rolling(window, min_periods=1).mean()
    return pd.Series(df["volume"] >= avg_vol * mult, index=df.index)

def _low_volume(df: pd.DataFrame, window: int = 20, mult: float = 0.8) -> pd.Series:
    """Volume sotto la media mobile (mercato tranquillo)."""
    avg_vol = df["volume"].rolling(window, min_periods=1).mean()
    return pd.Series(df["volume"] <= avg_vol * mult, index=df.index)

def _high_atr(df: pd.DataFrame, period: int = 14, window: int = 20) -> pd.Series:
    """ATR sopra la sua media mobile (alta volatilità)."""
    atr = _compute_atr(df, period)
    avg_atr = atr.rolling(window, min_periods=1).mean()
    return pd.Series(atr >= avg_atr, index=df.index)

def _low_atr(df: pd.DataFrame, period: int = 14, window: int = 20) -> pd.Series:
    """ATR sotto la sua media mobile (bassa volatilità)."""
    atr = _compute_atr(df, period)
    avg_atr = atr.rolling(window, min_periods=1).mean()
    return pd.Series(atr <= avg_atr, index=df.index)

def _compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()


COMMON_FILTERS: Dict[str, Callable] = {
    "no_open_30min":  _no_open_30min,
    "no_last_30min":  _no_last_30min,
    "morning_only":   _morning_only,
    "afternoon_only": _afternoon_only,
    "high_volume":    _high_volume,
    "low_volume":     _low_volume,
    "high_atr":       _high_atr,
    "low_atr":        _low_atr,
}


# ── FilterSweep ─────────────────────────────────────────────────────────────

class FilterSweep:
    """
    Sweep sistematico di filtri su una strategia pandas.

    Parameters
    ----------
    signal_fn : funzione f(df, **params) → pd.Series (+1/-1/0)
    signal_params : parametri per signal_fn
    backtest_params : parametri per PandasBacktest
    min_trades_pct : scarta combinazioni con n_trades < X% del baseline (default 0.40)
    max_combo_size : massima dimensione combinazione di filtri (default 2)
    """

    def __init__(
        self,
        signal_fn: Callable,
        signal_params: Optional[dict] = None,
        backtest_params: Optional[dict] = None,
        min_trades_pct: float = 0.40,
        max_combo_size: int = 2,
    ):
        self.signal_fn = signal_fn
        self.signal_params = signal_params or {}
        self.backtest_params = backtest_params or {}
        self.min_trades_pct = min_trades_pct
        self.max_combo_size = max_combo_size

    def run(
        self,
        df: pd.DataFrame,
        filters: Optional[Dict[str, Callable]] = None,
        symbol: str = "SYM",
    ) -> pd.DataFrame:
        """
        Esegue sweep su tutti i filtri e loro combinazioni.

        Parameters
        ----------
        df : DataFrame OHLCV
        filters : dict nome → funzione filtro. Default: COMMON_FILTERS
        symbol : nome simbolo per logging

        Returns
        -------
        DataFrame risultati, ordinato per ΔSharpe decrescente.
        Colonne: filter_combo, n_trades, trades_pct, sharpe, delta_sharpe,
                 win_rate, delta_win_rate, max_dd, expectancy_pct
        """
        from engine.backtest import PandasBacktest
        from engine.metrics import compute_metrics
        from strategies.generic import SignalStrategy

        if filters is None:
            filters = COMMON_FILTERS

        engine = PandasBacktest(**self.backtest_params)
        strategy = SignalStrategy(self.signal_fn, **self.signal_params)

        # Baseline senza filtri
        baseline_trades = engine.run(df, strategy, symbol=symbol)
        baseline_metrics = compute_metrics(baseline_trades)
        baseline_n = baseline_metrics.get("n_trades", 0)
        baseline_sharpe = baseline_metrics.get("sharpe", 0)
        baseline_wr = baseline_metrics.get("win_rate", 0)
        baseline_exp = baseline_metrics.get("expectancy_pct", 0)

        LOG.info(
            f"Baseline {symbol}: {baseline_n} trade, "
            f"Sharpe={baseline_sharpe:.2f}, WR={baseline_wr:.1%}"
        )

        rows: List[Dict] = [{
            "filter_combo": "BASELINE",
            "n_trades": baseline_n,
            "trades_pct": 1.0,
            "sharpe": round(baseline_sharpe, 4),
            "delta_sharpe": 0.0,
            "win_rate": round(baseline_wr, 4),
            "delta_win_rate": 0.0,
            "max_dd": round(baseline_metrics.get("max_drawdown_pct", 0), 4),
            "expectancy_pct": round(baseline_exp, 4),
        }]

        # Precomputa maschere filtri
        filter_masks: Dict[str, pd.Series] = {}
        for name, fn in filters.items():
            try:
                mask = fn(df)
                filter_masks[name] = mask.reindex(df.index, fill_value=True)
            except Exception as exc:
                LOG.warning(f"Filtro '{name}' errore: {exc}")

        # Genera tutte le combinazioni fino a max_combo_size
        filter_names = list(filter_masks.keys())
        combos: List[Tuple[str, ...]] = []
        for size in range(1, min(self.max_combo_size, len(filter_names)) + 1):
            combos.extend(combinations(filter_names, size))

        for combo in combos:
            combo_label = " AND ".join(combo)

            # Combina le maschere con AND
            combined = filter_masks[combo[0]].copy()
            for name in combo[1:]:
                combined = combined & filter_masks[name]

            # Applica maschera al segnale: azzera segnali dove filtro è False
            row = self._run_with_mask(
                df=df,
                engine=engine,
                strategy=strategy,
                mask=combined,
                combo_label=combo_label,
                symbol=symbol,
                baseline_n=baseline_n,
                baseline_sharpe=baseline_sharpe,
                baseline_wr=baseline_wr,
            )

            if row is None:
                continue  # Troppo pochi trade

            rows.append(row)

        results_df = pd.DataFrame(rows)
        if len(results_df) > 1:
            results_df = results_df.sort_values("delta_sharpe", ascending=False)
        return results_df.reset_index(drop=True)

    def _run_with_mask(
        self,
        df: pd.DataFrame,
        engine: Any,
        strategy: Any,
        mask: pd.Series,
        combo_label: str,
        symbol: str,
        baseline_n: int,
        baseline_sharpe: float,
        baseline_wr: float,
    ) -> Optional[Dict]:
        """Esegue simulazione con filtro e ritorna riga risultati."""
        from engine.metrics import compute_metrics
        from strategies.generic import SignalStrategy

        # Crea strategia filtrata (wrapper che azzera segnali fuori maschera)
        filtered_fn = _make_filtered_signal(self.signal_fn, mask)
        filtered_strategy = SignalStrategy(filtered_fn, **self.signal_params)

        try:
            trades = engine.run(df, filtered_strategy, symbol=symbol)
            metrics = compute_metrics(trades)
        except Exception as exc:
            LOG.debug(f"Combo '{combo_label}': errore simulazione: {exc}")
            return None

        n = metrics.get("n_trades", 0)
        trades_pct = n / baseline_n if baseline_n > 0 else 0

        if trades_pct < self.min_trades_pct:
            LOG.debug(
                f"Combo '{combo_label}': {n} trade ({trades_pct:.0%} del baseline) "
                f"< {self.min_trades_pct:.0%} — scartato"
            )
            return None

        sharpe = metrics.get("sharpe", 0)
        wr = metrics.get("win_rate", 0)

        return {
            "filter_combo": combo_label,
            "n_trades": n,
            "trades_pct": round(trades_pct, 3),
            "sharpe": round(sharpe, 4),
            "delta_sharpe": round(sharpe - baseline_sharpe, 4),
            "win_rate": round(wr, 4),
            "delta_win_rate": round(wr - baseline_wr, 4),
            "max_dd": round(metrics.get("max_drawdown_pct", 0), 4),
            "expectancy_pct": round(metrics.get("expectancy_pct", 0), 4),
        }

    def print_report(self, results_df: pd.DataFrame, top_n: int = 10) -> None:
        """Stampa report leggibile dei risultati sweep."""
        if results_df.empty:
            print("Nessun risultato sweep filtri.")
            return

        print(f"\nFilter Sweep — Top {min(top_n, len(results_df))} combinazioni")
        print("─" * 85)
        print(f"{'Filtro':<35} {'Trade':>7} {'%Base':>6} {'Sharpe':>7} {'ΔSharpe':>8} {'WR':>6} {'MaxDD':>7}")
        print("─" * 85)

        for _, row in results_df.head(top_n).iterrows():
            label = row["filter_combo"]
            if len(label) > 33:
                label = label[:31] + "…"
            marker = " ✓" if row["delta_sharpe"] > 0.1 else ("  " if row["filter_combo"] == "BASELINE" else " ·")
            print(
                f"{label:<35} {int(row['n_trades']):>7} "
                f"{row['trades_pct']:>5.0%} "
                f"{row['sharpe']:>7.2f} "
                f"{row['delta_sharpe']:>+8.2f} "
                f"{row['win_rate']:>5.1%} "
                f"{row['max_dd']:>7.2f}{marker}"
            )

        # Raccomandazione
        good = results_df[
            (results_df["delta_sharpe"] > 0.1) &
            (results_df["filter_combo"] != "BASELINE")
        ]
        if good.empty:
            print("\nNessun filtro migliora Sharpe > 0.1 — considera di non usare filtri.")
        else:
            best = good.iloc[0]
            print(f"\nRaccomandazione: '{best['filter_combo']}' "
                  f"(ΔSharpe={best['delta_sharpe']:+.2f}, trade={best['trades_pct']:.0%} del baseline)")


def _make_filtered_signal(
    signal_fn: Callable,
    mask: pd.Series,
) -> Callable:
    """
    Wrap signal_fn azzerando i segnali dove mask è False.
    """
    def filtered(df: pd.DataFrame, **params) -> pd.Series:
        raw = signal_fn(df, **params)
        aligned_mask = mask.reindex(raw.index, fill_value=False)
        return raw.where(aligned_mask, 0)
    return filtered
