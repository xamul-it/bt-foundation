"""
Tools dell'agente — funzioni Python chiamate direttamente dall'agent loop.

NON usa anthropic SDK. I tool sono funzioni normali orchestrate da agent.py.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional, Type

import pandas as pd

import sys, os
sys.path.insert(0, str(Path(__file__).parent.parent))
from paths import DATA_DIR, BT_CORE, BIN_DIR, REPORTS_DIR

LOG = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Tool: load_data
# ------------------------------------------------------------------

def load_data(
    symbol: str,
    fromdate: Optional[str] = None,
    todate: Optional[str] = None,
    rth_only: bool = True,
) -> pd.DataFrame:
    """
    Carica dati OHLCV minuto da data/m/alpaca/{symbol}.csv.

    Parameters
    ----------
    symbol : ticker (es. "NVDA")
    fromdate : "YYYY-MM-DD" incluso (None = inizio file)
    todate : "YYYY-MM-DD" incluso (None = fine file)
    rth_only : filtra RTH 09:30-16:00

    Returns
    -------
    DataFrame con colonne open/high/low/close/volume, indice DatetimeIndex.
    """
    csv = DATA_DIR / f"{symbol}.csv"
    if not csv.exists():
        raise FileNotFoundError(f"Dati non trovati: {csv}")

    df = pd.read_csv(csv, parse_dates=["timestamp"])
    df = df.set_index("timestamp").sort_index()

    if fromdate:
        df = df[df.index >= fromdate]
    if todate:
        df = df[df.index <= todate + " 23:59:59"]

    if rth_only:
        from datetime import time
        t = df.index.time
        df = df[(t >= time(9, 30)) & (t < time(16, 0))]

    LOG.info(f"load_data: {symbol} — {len(df)} barre "
             f"[{df.index.min()} → {df.index.max()}]")
    return df


# ------------------------------------------------------------------
# Tool: run_pandas_backtest
# ------------------------------------------------------------------

def run_pandas_backtest(
    df: pd.DataFrame,
    strategy_cls: Type,
    strategy_params: dict,
    backtest_params: Optional[dict] = None,
    symbol: str = "UNKNOWN",
) -> tuple[pd.DataFrame, Dict]:
    """
    Esegue backtest pandas e ritorna (trades_df, metrics_dict).
    """
    from engine.backtest import PandasBacktest
    from engine.metrics import compute_metrics

    bp = backtest_params or {}
    strategy = strategy_cls(**strategy_params)
    engine = PandasBacktest(**bp)
    trades = engine.run(df, strategy, symbol=symbol)
    metrics = compute_metrics(trades)

    LOG.info(f"run_pandas_backtest: {metrics['n_trades']} trade, "
             f"win_rate={metrics['win_rate']:.1%}, "
             f"expectancy={metrics['expectancy_pct']:+.3f}%")
    return trades, metrics


# ------------------------------------------------------------------
# Tool: run_bt_backtest
# ------------------------------------------------------------------

def run_bt_backtest(
    strategy: str,
    tickers: List[str],
    fromdate: str,
    todate: str,
    stratargs: str = "",
) -> tuple[Dict, pd.DataFrame]:
    """
    Esegue btmain.py e ritorna (metrics, trades_df).
    """
    from bt_bridge.runner import BTRunner
    runner = BTRunner()
    return runner.run(strategy, tickers, fromdate, todate, stratargs)


# ------------------------------------------------------------------
# Tool: compare_trades
# ------------------------------------------------------------------

def compare_trades(
    pd_trades: pd.DataFrame,
    bt_trades: pd.DataFrame,
    symbol: Optional[str] = None,
) -> tuple[pd.DataFrame, Dict]:
    """
    Confronta trade pandas vs BT. Ritorna (diff_df, summary_dict).
    """
    from bt_bridge.comparator import TradeComparator
    comp = TradeComparator()
    diff = comp.compare(pd_trades, bt_trades, symbol=symbol)
    summary = comp.summary(diff)
    LOG.info(f"compare_trades: {summary}")
    return diff, summary


# ------------------------------------------------------------------
# Tool: sweep_parameters
# ------------------------------------------------------------------

def sweep_parameters(
    df: pd.DataFrame,
    strategy_cls: Type,
    strategy_grid: dict,
    backtest_grid: Optional[dict] = None,
    symbol: str = "UNKNOWN",
    n_jobs: int = -1,
) -> pd.DataFrame:
    """
    Grid search parallelo. Ritorna DataFrame risultati ordinato per expectancy.
    """
    from sweep.grid import GridSweep
    gs = GridSweep(
        strategy_cls=strategy_cls,
        strategy_grid=strategy_grid,
        backtest_grid=backtest_grid or {},
        n_jobs=n_jobs,
    )
    results = gs.run(df, symbol=symbol)
    return results.sort_values("expectancy_pct", ascending=False).reset_index(drop=True)


# ------------------------------------------------------------------
# Tool: extract_indicators
# ------------------------------------------------------------------

def extract_indicators(
    df: pd.DataFrame,
    indicator_factory,
    factory_params: dict,
    label: str,
    symbol: str = "UNKNOWN",
    cache: bool = True,
) -> pd.DataFrame:
    """
    Estrae valori indicatori da Cerebro BT → parquet cache.
    """
    from bt_bridge.extractor import extract_indicators as _extract
    return _extract(df, indicator_factory, factory_params, label, symbol, cache)


# ------------------------------------------------------------------
# Tool: analyze_stability
# ------------------------------------------------------------------

def analyze_stability(
    df: pd.DataFrame,
    strategy_cls: Type,
    strategy_params: dict,
    backtest_params: Optional[dict] = None,
    window: str = "ME",
    symbol: str = "UNKNOWN",
    run_mc: bool = False,
) -> tuple[pd.DataFrame, Dict]:
    """
    Analisi di stabilità su finestre temporali. Ritorna (windows_df, summary).
    """
    from analysis.stability import StabilityAnalyzer
    analyzer = StabilityAnalyzer(
        strategy_cls=strategy_cls,
        strategy_params=strategy_params,
        backtest_params=backtest_params or {},
    )
    windows_df = analyzer.analyze(df, window=window, symbol=symbol, run_mc=run_mc)
    summary = analyzer.summary(windows_df)
    LOG.info(f"analyze_stability: {len(windows_df)} finestre, {summary.get('verdict','?')}")
    return windows_df, summary


# ------------------------------------------------------------------
# Tool: analyze_coherence
# ------------------------------------------------------------------

def analyze_coherence(
    contexts: Dict,  # {"backtest": (metrics, trades), "simulation": ..., "paper": ...}
) -> tuple[pd.DataFrame, List[Dict], List[Dict]]:
    """
    Confronto backtest/sim/paper. Ritorna (comparison_df, issues, checklist).
    """
    from analysis.coherence import CoherenceAnalyzer
    comp = CoherenceAnalyzer()
    for name, (metrics, trades) in contexts.items():
        comp.add(name, metrics, trades)
    comparison = comp.compare()
    issues = comp.detect_issues(comparison)
    checklist = comp.live_trading_checklist()
    return comparison, issues, checklist


# ------------------------------------------------------------------
# Tool: generate_report
# ------------------------------------------------------------------

def generate_report(
    title: str,
    output_path: str,
    backtest_data: Optional[tuple] = None,       # (metrics, trades, label, params)
    mc_data: Optional[tuple] = None,             # (results_df, stats, n_real_trades)
    stability_data: Optional[tuple] = None,      # (windows_df, llm_comment)
    coherence_data: Optional[tuple] = None,      # (comparison_df, llm_comment)
    divergence_data: Optional[tuple] = None,     # (diff_df, summary, llm_comment)
    reproduce_code: str = "",
    llm_analysis: str = "",
    subtitle: str = "",
) -> Path:
    """
    Genera report HTML self-contained. Ritorna il path del file.
    """
    from reporting.html_report import HTMLReport
    rpt = HTMLReport(title=title, subtitle=subtitle)

    if backtest_data:
        metrics, trades, label, params = backtest_data
        rpt.add_backtest(metrics, trades, label=label, params=params)

    if mc_data:
        results_df, stats, n_real = mc_data
        rpt.add_montecarlo(results_df, stats, n_real_trades=n_real)

    if stability_data:
        windows_df, comment = stability_data
        rpt.add_stability(windows_df, llm_comment=comment)

    if coherence_data:
        comparison_df, comment = coherence_data
        rpt.add_coherence(comparison_df, llm_comment=comment)

    if divergence_data:
        diff_df, summary, comment = divergence_data
        rpt.add_divergences(diff_df, summary, llm_comment=comment)

    if llm_analysis:
        rpt.add_llm_analysis(llm_analysis)

    if reproduce_code:
        rpt.add_reproduce_code(reproduce_code)

    path = rpt.save(output_path)
    LOG.info(f"Report salvato: {path}")
    return path


# ------------------------------------------------------------------
# Tool: run_montecarlo
# ------------------------------------------------------------------

def run_montecarlo(
    trades_df: pd.DataFrame,
    n_simulations: int = 5000,
    n_trades: int = 1000,
    initial_capital: float = 100_000.0,
    position_size_pct: float = 0.10,
) -> tuple[pd.DataFrame, Dict]:
    """
    Monte Carlo bootstrap su trades. Ritorna (results_df, stats_dict).
    """
    from sweep.montecarlo import MonteCarlo
    mc = MonteCarlo(
        n_simulations=n_simulations,
        n_trades=n_trades,
        initial_capital=initial_capital,
        position_size_pct=position_size_pct,
    )
    results = mc.run(trades_df)
    stats = mc.analyze(results)
    return results, stats
