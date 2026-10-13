"""
AssetScanner — matrice simboli × periodi per valutare robustezza cross-asset.

Risponde a: "su quali simboli e in quali periodi la strategia funziona?"

Output principale: matrice con Sharpe per cella (simbolo × trimestre/mese),
classificazione simboli stabili/instabili, periodi avversi.

Uso
---
    from analysis.asset_scan import AssetScanner

    scanner = AssetScanner(
        signal_fn=my_signal,
        signal_params={"period": 16, "inverted": True},
        backtest_params={"sl_pct": 0.005, "exitbar": 6, "exitbar_mode": "bt_style"},
        window="QE",
    )

    matrix = scanner.run(
        symbols=["NVDA", "AAPL", "MSFT"],
        data_dir="/home/htpc/backtrader/config-common/data/minutes/alpaca/",
    )

    scanner.print_report(matrix)
    recommended, excluded = scanner.classify(matrix)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)


class AssetScanner:
    """
    Costruisce la matrice asset×periodo con Sharpe e altre metriche.

    Parameters
    ----------
    signal_fn : funzione f(df, **params) → pd.Series
    signal_params : parametri per signal_fn
    backtest_params : parametri per PandasBacktest
    window : frequenza pandas per i periodi ('QE'=trimestrale, 'ME'=mensile)
    min_trades_per_window : finestre con meno trade vengono marcate come NaN
    """

    def __init__(
        self,
        signal_fn: Callable,
        signal_params: Optional[dict] = None,
        backtest_params: Optional[dict] = None,
        window: str = "QE",
        min_trades_per_window: int = 10,
    ):
        self.signal_fn = signal_fn
        self.signal_params = signal_params or {}
        self.backtest_params = backtest_params or {}
        self.window = window
        self.min_trades_per_window = min_trades_per_window

    def run(
        self,
        symbols: List[str],
        data_dir: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> "ScanMatrix":
        """
        Esegue il scan su tutti i simboli e periodi.

        Parameters
        ----------
        symbols : lista simboli da analizzare
        data_dir : directory con CSV OHLCV (una file per simbolo)
        date_from : data inizio (opzionale, default = tutto)
        date_to : data fine (opzionale, default = tutto)

        Returns
        -------
        ScanMatrix con i risultati completi
        """
        from engine.backtest import PandasBacktest
        from engine.metrics import compute_metrics
        from strategies.generic import SignalStrategy

        data_path = Path(data_dir)
        engine = PandasBacktest(**self.backtest_params)
        strategy = SignalStrategy(self.signal_fn, **self.signal_params)

        all_rows: List[Dict] = []

        for symbol in symbols:
            csv_path = data_path / f"{symbol}.csv"
            if not csv_path.exists():
                LOG.warning(f"{symbol}: CSV non trovato in {csv_path}")
                continue

            df = _load_csv(csv_path, date_from, date_to)
            if df.empty:
                LOG.warning(f"{symbol}: DataFrame vuoto dopo filtro date")
                continue

            LOG.info(f"Scanning {symbol}: {len(df)} barre, {df.index[0].date()} → {df.index[-1].date()}")

            periods = pd.period_range(df.index.min(), df.index.max(), freq=self.window)

            for period in periods:
                start = period.start_time
                end = period.end_time
                window_df = df[(df.index >= start) & (df.index <= end)]

                if len(window_df) < 50:
                    continue

                try:
                    trades = engine.run(window_df, strategy, symbol=symbol)
                    metrics = compute_metrics(trades)
                except Exception as exc:
                    LOG.debug(f"{symbol} {period}: errore {exc}")
                    metrics = {"n_trades": 0}

                n_trades = metrics.get("n_trades", 0)
                sharpe = metrics.get("sharpe", np.nan) if n_trades >= self.min_trades_per_window else np.nan

                all_rows.append({
                    "symbol": symbol,
                    "period": str(period),
                    "n_trades": n_trades,
                    "sharpe": sharpe,
                    "win_rate": metrics.get("win_rate", np.nan) if n_trades >= self.min_trades_per_window else np.nan,
                    "expectancy_pct": metrics.get("expectancy_pct", np.nan) if n_trades >= self.min_trades_per_window else np.nan,
                    "max_drawdown_pct": metrics.get("max_drawdown_pct", np.nan) if n_trades >= self.min_trades_per_window else np.nan,
                    "sqn": metrics.get("sqn", np.nan) if n_trades >= self.min_trades_per_window else np.nan,
                })

        if not all_rows:
            LOG.error("Nessun risultato prodotto dal scan.")
            return ScanMatrix(pd.DataFrame(), pd.DataFrame())

        detail_df = pd.DataFrame(all_rows)
        sharpe_matrix = detail_df.pivot(index="symbol", columns="period", values="sharpe")

        return ScanMatrix(sharpe_matrix, detail_df)

    def classify(
        self,
        matrix: "ScanMatrix",
        stable_threshold: float = 0.7,
        min_sharpe: float = 0.8,
    ) -> Tuple[List[str], List[str]]:
        """
        Classifica simboli in stabili e instabili.

        Parameters
        ----------
        matrix : risultato di run()
        stable_threshold : % periodi con Sharpe > min_sharpe per essere STABILE
        min_sharpe : soglia Sharpe minima per periodo positivo

        Returns
        -------
        (recommended, excluded) — liste di simboli
        """
        if matrix.sharpe.empty:
            return [], []

        recommended = []
        excluded = []

        for symbol in matrix.sharpe.index:
            row = matrix.sharpe.loc[symbol].dropna()
            if len(row) == 0:
                excluded.append(symbol)
                continue

            pct_positive = (row >= min_sharpe).mean()
            cv = row.std() / abs(row.mean()) if abs(row.mean()) > 0.01 else 10

            if pct_positive >= stable_threshold and cv < 0.8:
                recommended.append(symbol)
            else:
                excluded.append(symbol)

        return recommended, excluded

    def print_report(self, matrix: "ScanMatrix", top_n_symbols: int = 20) -> None:
        """Stampa report della matrice scan."""
        if matrix.sharpe.empty:
            print("Nessun risultato scan.")
            return

        mat = matrix.sharpe

        print(f"\nMatrice Asset×Periodo — Sharpe ({self.window})")
        print("─" * 80)

        # Mostra solo ultimi N periodi se troppi
        cols = mat.columns.tolist()
        if len(cols) > 8:
            cols = cols[-8:]
        display = mat[cols].head(top_n_symbols)

        # Header periodi (corti)
        short_periods = [str(c)[-7:] for c in cols]
        print(f"{'Simbolo':<10}", end="")
        for p in short_periods:
            print(f"  {p:>8}", end="")
        print(f"  {'Media':>7}  {'CV':>5}  {'%>0.8':>6}")
        print("─" * 80)

        for symbol in display.index:
            row = mat.loc[symbol]
            values = row[cols]
            row_full = row.dropna()
            mean_s = row_full.mean() if len(row_full) > 0 else np.nan
            cv = row_full.std() / abs(mean_s) if (len(row_full) > 0 and abs(mean_s) > 0.01) else np.nan
            pct_pos = (row_full >= 0.8).mean() * 100 if len(row_full) > 0 else 0

            print(f"{symbol:<10}", end="")
            for v in values:
                if pd.isna(v):
                    print(f"  {'—':>8}", end="")
                else:
                    marker = "+" if v >= 1.0 else ("·" if v >= 0.5 else "-")
                    print(f"  {v:>7.2f}{marker}", end="")
            print(f"  {mean_s:>7.2f}  {cv:>5.2f}  {pct_pos:>5.0f}%")

        # Riepilogo periodi
        period_means = mat.mean(axis=0).dropna()
        print("─" * 80)
        print(f"{'Media periodo':<10}", end="")
        for p in cols:
            v = period_means.get(p, np.nan)
            if pd.isna(v):
                print(f"  {'—':>8}", end="")
            else:
                print(f"  {v:>8.2f}", end="")
        print()

        # Classificazione
        recommended, excluded = self.classify(matrix)
        print(f"\nRACCOMANDATI ({len(recommended)}): {', '.join(recommended) or 'nessuno'}")
        print(f"ESCLUSI ({len(excluded)}): {', '.join(excluded) or 'nessuno'}")

        # Periodi avversi (media < 0.5 su tutti i simboli)
        adverse = [str(p) for p in period_means.index if period_means[p] < 0.5]
        if adverse:
            print(f"PERIODI AVVERSI: {', '.join(adverse)}")


class ScanMatrix:
    """Contenitore risultati AssetScanner."""

    def __init__(self, sharpe: pd.DataFrame, detail: pd.DataFrame):
        self.sharpe = sharpe    # pivot simbolo × periodo → Sharpe
        self.detail = detail    # riga per (simbolo, periodo) con tutte le metriche

    def to_csv(self, path: str) -> None:
        """Salva la matrice Sharpe in CSV."""
        self.sharpe.to_csv(path)
        LOG.info(f"Matrice salvata in: {path}")

    def get_symbol_summary(self) -> pd.DataFrame:
        """Ritorna statistiche aggregate per simbolo."""
        if self.sharpe.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "mean_sharpe": self.sharpe.mean(axis=1),
            "std_sharpe": self.sharpe.std(axis=1),
            "cv_sharpe": self.sharpe.std(axis=1) / self.sharpe.mean(axis=1).abs(),
            "pct_positive": (self.sharpe >= 0.8).mean(axis=1) * 100,
            "n_periods": self.sharpe.notna().sum(axis=1),
        }).sort_values("mean_sharpe", ascending=False)

    def get_period_summary(self) -> pd.DataFrame:
        """Ritorna statistiche aggregate per periodo."""
        if self.sharpe.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "mean_sharpe": self.sharpe.mean(axis=0),
            "pct_positive": (self.sharpe >= 0.8).mean(axis=0) * 100,
            "n_symbols": self.sharpe.notna().sum(axis=0),
        }).sort_index()


def _load_csv(
    path: Path,
    date_from: Optional[str],
    date_to: Optional[str],
) -> pd.DataFrame:
    """Carica CSV OHLCV Alpaca e applica filtro date."""
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.columns = [c.lower() for c in df.columns]

        if df.index.tz is None:
            df.index = pd.to_datetime(df.index, utc=True)
        else:
            df.index = df.index.tz_convert("UTC")

        if date_from:
            df = df[df.index >= pd.Timestamp(date_from, tz="UTC")]
        if date_to:
            df = df[df.index <= pd.Timestamp(date_to, tz="UTC")]

        return df
    except Exception as exc:
        LOG.error(f"Errore caricamento {path}: {exc}")
        return pd.DataFrame()
