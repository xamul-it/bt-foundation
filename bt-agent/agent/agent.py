"""
BTPandasAgent — agente che usa `claude --print` (subscription locale).

NON usa anthropic SDK / API key.
Chiama il CLI `claude --print` via subprocess per analisi LLM.

Loop:
1. L'utente descrive l'obiettivo
2. L'agente esegue i tool Python in sequenza
3. Per analisi/diagnosi chiama `claude --print` con il contesto
4. Ritorna risultati + spiegazione in italiano
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

import pandas as pd

from agent.prompts import SYSTEM_PROMPT
from agent.tools import (
    analyze_coherence,
    analyze_stability,
    compare_trades,
    extract_indicators,
    generate_report,
    load_data,
    run_bt_backtest,
    run_montecarlo,
    run_pandas_backtest,
    sweep_parameters,
)

LOG = logging.getLogger(__name__)

CLAUDE_BIN = shutil.which("claude") or "claude"


def _claude_ask(prompt: str, context: str = "", max_chars: int = 8000) -> str:
    """
    Chiama `claude --print` con il prompt.

    Usa la subscription locale (nessuna API key).
    """
    if context:
        # Tronca context se troppo lungo
        if len(context) > max_chars:
            context = context[:max_chars] + "\n[... troncato ...]"
        full_prompt = f"{context}\n\n---\n{prompt}"
    else:
        full_prompt = prompt

    cmd = [
        CLAUDE_BIN,
        "--print",
        "--output-format", "text",
        "--model", "sonnet",
        "--system-prompt", SYSTEM_PROMPT,
        full_prompt,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            LOG.warning(f"claude --print rc={result.returncode}: {result.stderr[:500]}")
            return f"[errore claude: {result.stderr[:200]}]"
        return result.stdout.strip()
    except FileNotFoundError:
        return "[claude CLI non trovato — installa Claude Code]"
    except subprocess.TimeoutExpired:
        return "[timeout claude --print dopo 120s]"


def _df_summary(df: pd.DataFrame, max_rows: int = 20) -> str:
    """Serializza un DataFrame in testo compatto per il contesto LLM."""
    if df is None or df.empty:
        return "(nessun dato)"
    s = f"Shape: {df.shape}\n"
    s += df.head(max_rows).to_string(index=False)
    if len(df) > max_rows:
        s += f"\n... ({len(df) - max_rows} righe omesse)"
    return s


def _metrics_summary(metrics: Dict) -> str:
    lines = [f"{k}: {v}" for k, v in metrics.items()]
    return "\n".join(lines)


class BTPandasAgent:
    """
    Agente interattivo pandas ↔ Backtrader.

    Usa claude CLI (subscription) per analisi LLM.

    Uso base
    --------
    agent = BTPandasAgent()
    agent.run_interactive()

    Uso programmatico
    -----------------
    agent = BTPandasAgent()
    trades, metrics = agent.backtest(
        symbol="NVDA",
        strategy_cls=HMAStrategy,
        strategy_params={"period": 16},
        backtest_params={"sl_pct": 0.005},
        fromdate="2025-01-01",
        todate="2025-01-31",
    )
    """

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self._setup_logging()
        self._last_trades: Optional[pd.DataFrame] = None
        self._last_metrics: Optional[Dict] = None

    def _setup_logging(self):
        level = logging.INFO if self.verbose else logging.WARNING
        logging.basicConfig(
            level=level,
            format="%(asctime)s | %(levelname)s | %(message)s",
            stream=sys.stderr,
        )

    # ------------------------------------------------------------------
    # Shortcut methods (uso programmatico)
    # ------------------------------------------------------------------

    def backtest(
        self,
        symbol: str,
        strategy_cls: Type,
        strategy_params: dict,
        backtest_params: Optional[dict] = None,
        fromdate: Optional[str] = None,
        todate: Optional[str] = None,
        analyze: bool = True,
    ) -> tuple[pd.DataFrame, Dict]:
        """Carica dati, esegue backtest, analizza con Claude."""
        df = load_data(symbol, fromdate, todate)
        trades, metrics = run_pandas_backtest(
            df, strategy_cls, strategy_params,
            backtest_params, symbol=symbol,
        )
        self._last_trades = trades
        self._last_metrics = metrics

        if analyze and len(trades) > 0:
            analysis = self.analyze_backtest(trades, metrics, strategy_params)
            print("\n--- ANALISI ---")
            print(analysis)

        return trades, metrics

    def sweep(
        self,
        symbol: str,
        strategy_cls: Type,
        strategy_grid: dict,
        backtest_grid: Optional[dict] = None,
        fromdate: Optional[str] = None,
        todate: Optional[str] = None,
        n_jobs: int = -1,
        analyze: bool = True,
    ) -> pd.DataFrame:
        """Grid sweep con analisi automatica del top-5."""
        df = load_data(symbol, fromdate, todate)
        results = sweep_parameters(
            df, strategy_cls, strategy_grid,
            backtest_grid, symbol=symbol, n_jobs=n_jobs,
        )

        if analyze and len(results) > 0:
            analysis = self.analyze_sweep(results)
            print("\n--- ANALISI SWEEP ---")
            print(analysis)

        return results

    def validate_vs_bt(
        self,
        pd_trades: pd.DataFrame,
        bt_strategy: str,
        tickers: List[str],
        fromdate: str,
        todate: str,
        stratargs: str = "",
    ) -> tuple[pd.DataFrame, Dict]:
        """Confronta trades pandas con BT, analisi LLM delle divergenze."""
        bt_metrics, bt_trades = run_bt_backtest(
            bt_strategy, tickers, fromdate, todate, stratargs
        )
        diff_df, summary = compare_trades(pd_trades, bt_trades)

        analysis = self.analyze_divergences(diff_df, summary)
        print("\n--- ANALISI DIVERGENZE ---")
        print(analysis)

        return diff_df, summary

    def stability(
        self,
        symbol: str,
        strategy_cls: Type,
        strategy_params: dict,
        backtest_params: Optional[dict] = None,
        fromdate: Optional[str] = None,
        todate: Optional[str] = None,
        window: str = "ME",
        run_mc: bool = False,
    ) -> tuple[pd.DataFrame, Dict]:
        """Analisi stabilità finestre temporali con analisi LLM."""
        df = load_data(symbol, fromdate, todate)
        windows_df, summary = analyze_stability(
            df, strategy_cls, strategy_params,
            backtest_params, window=window,
            symbol=symbol, run_mc=run_mc,
        )
        if len(windows_df) > 0:
            analysis = self.analyze_stability_result(windows_df, summary)
            print("\n--- ANALISI STABILITÀ ---")
            print(analysis)
        return windows_df, summary

    def coherence(
        self,
        contexts: Dict,
    ) -> tuple[pd.DataFrame, List[Dict], List[Dict]]:
        """
        Confronto backtest/sim/paper con checklist live.

        contexts = {
            "backtest":   (metrics_dict, trades_df),
            "simulation": (metrics_dict, trades_df),
            "paper":      (metrics_dict, trades_df),
        }
        """
        comparison, issues, checklist = analyze_coherence(contexts)
        analysis = self.analyze_coherence_result(comparison, issues, checklist)
        print("\n--- ANALISI COERENZA ---")
        print(analysis)
        return comparison, issues, checklist

    def full_report(
        self,
        title: str,
        output_path: str,
        symbol: str,
        strategy_cls: Type,
        strategy_params: dict,
        backtest_params: Optional[dict] = None,
        fromdate: Optional[str] = None,
        todate: Optional[str] = None,
        run_stability: bool = True,
        run_mc: bool = True,
        n_mc_simulations: int = 3000,
        paper_context: Optional[tuple] = None,
        reproduce_code: str = "",
    ) -> Path:
        """
        Workflow completo: backtest → MC → stabilità → [coerenza] → report HTML.

        Parameters
        ----------
        paper_context : (metrics_dict, trades_df) da paper trading, opzionale.
        reproduce_code : codice Python da includere nel report.

        Returns
        -------
        Path del report HTML generato.
        """
        df = load_data(symbol, fromdate, todate)

        # 1. Backtest
        trades, metrics = run_pandas_backtest(
            df, strategy_cls, strategy_params, backtest_params, symbol=symbol
        )
        self._last_trades = trades
        self._last_metrics = metrics

        backtest_analysis = self.analyze_backtest(trades, metrics, strategy_params)

        # 2. Monte Carlo
        mc_results, mc_stats = (None, None)
        if run_mc and len(trades) >= 10:
            mc_results, mc_stats = run_montecarlo(
                trades, n_simulations=n_mc_simulations,
                n_trades=min(1000, len(trades) * 10)
            )

        # 3. Stabilità
        windows_df, stab_summary = (None, None)
        stab_comment = ""
        if run_stability:
            windows_df, stab_summary = analyze_stability(
                df, strategy_cls, strategy_params,
                backtest_params, symbol=symbol
            )
            if windows_df is not None and len(windows_df) > 0:
                stab_comment = self.analyze_stability_result(windows_df, stab_summary)

        # 4. Coerenza (se paper disponibile)
        comparison, coh_comment = (None, "")
        if paper_context:
            contexts = {
                "backtest": (metrics, trades),
                "simulation": (metrics, trades),
                "paper": paper_context,
            }
            comparison, issues, _ = analyze_coherence(contexts)
            coh_comment = self.analyze_coherence_result(comparison, issues, [])

        # 5. Analisi globale LLM
        global_analysis = self._global_analysis(
            metrics, mc_stats, stab_summary, title
        )

        # 6. Genera report
        subtitle = f"{symbol} · {fromdate or ''} → {todate or ''}"
        path = generate_report(
            title=title,
            output_path=output_path,
            subtitle=subtitle,
            backtest_data=(metrics, trades, "Backtest pandas", strategy_params),
            mc_data=(mc_results, mc_stats, len(trades)) if mc_results is not None else None,
            stability_data=(windows_df, stab_comment) if windows_df is not None else None,
            coherence_data=(comparison, coh_comment) if comparison is not None else None,
            reproduce_code=reproduce_code,
            llm_analysis=global_analysis,
        )
        print(f"\nReport generato: {path}")
        return path

    def montecarlo(
        self,
        trades_df: Optional[pd.DataFrame] = None,
        n_simulations: int = 5000,
        n_trades: int = 1000,
        analyze: bool = True,
    ) -> tuple[pd.DataFrame, Dict]:
        """Monte Carlo su trades. Usa last_trades se trades_df è None."""
        t = trades_df if trades_df is not None else self._last_trades
        if t is None or len(t) == 0:
            raise ValueError("Nessun trade disponibile. Esegui prima un backtest.")

        results, stats = run_montecarlo(t, n_simulations, n_trades)

        if analyze:
            analysis = self.analyze_montecarlo(stats, len(t))
            print("\n--- ANALISI MONTE CARLO ---")
            print(analysis)

        return results, stats

    # ------------------------------------------------------------------
    # LLM analysis methods
    # ------------------------------------------------------------------

    def analyze_backtest(
        self,
        trades: pd.DataFrame,
        metrics: Dict,
        params: dict,
    ) -> str:
        """Chiede a Claude di analizzare i risultati del backtest."""
        context = textwrap.dedent(f"""
            ## Parametri strategia
            {json.dumps(params, indent=2)}

            ## Metriche
            {_metrics_summary(metrics)}

            ## Distribuzione outcomes
            {trades['outcome'].value_counts().to_string() if 'outcome' in trades.columns else 'N/D'}

            ## Primi 10 trade
            {_df_summary(trades, max_rows=10)}
        """).strip()

        return _claude_ask(
            "Analizza questi risultati di backtest. "
            "Valuta la qualità della strategia e suggerisci se esplorare parametri diversi.",
            context=context,
        )

    def analyze_sweep(self, results: pd.DataFrame) -> str:
        """Analisi del top-10 risultati sweep."""
        top = results.head(10)
        context = textwrap.dedent(f"""
            ## Top 10 combinazioni parametri (ordinate per expectancy)
            {_df_summary(top, max_rows=10)}

            ## Statistiche globali griglia ({len(results)} combinazioni)
            expectancy media: {results['expectancy_pct'].mean():.4f}%
            win_rate media: {results['win_rate'].mean():.2%}
            SQN medio: {results['sqn'].mean():.2f}
            n_trades medio: {results['n_trades'].mean():.0f}
        """).strip()

        return _claude_ask(
            "Analizza questi risultati della grid search. "
            "Identifica i parametri più promettenti e pattern nella griglia.",
            context=context,
        )

    def analyze_stability_result(self, windows_df: pd.DataFrame, summary: Dict) -> str:
        context = textwrap.dedent(f"""
            ## Sommario stabilità
            {json.dumps(summary, indent=2)}

            ## Metriche per finestra
            {_df_summary(windows_df)}
        """).strip()
        return _claude_ask(
            "Analizza la stabilità della strategia attraverso le finestre temporali. "
            "La strategia è robusta o i risultati dipendono dal periodo? "
            "Identifica le finestre outlier e suggerisci se la strategia è adatta al live.",
            context=context,
        )

    def analyze_coherence_result(
        self,
        comparison: pd.DataFrame,
        issues: List[Dict],
        checklist: List[Dict],
    ) -> str:
        context = textwrap.dedent(f"""
            ## Confronto backtest/sim/paper
            {_df_summary(comparison)}

            ## Problemi rilevati ({len(issues)})
            {json.dumps(issues, indent=2, default=str)}

            ## Checklist live trading
            {json.dumps([{k:v for k,v in c.items() if k != 'description'} for c in checklist], indent=2)}
        """).strip()
        return _claude_ask(
            "Analizza la coerenza tra i 3 contesti (backtest/simulazione/paper). "
            "Quale è la causa principale delle divergenze? "
            "La strategia è adatta al live trading dati i vincoli degli ordini limit, "
            "close-revert sequenziale e TTL?",
            context=context,
        )

    def _global_analysis(
        self,
        metrics: Dict,
        mc_stats: Optional[Dict],
        stab_summary: Optional[Dict],
        title: str,
    ) -> str:
        mc_str = ""
        if mc_stats:
            tr = mc_stats.get("total_return", {})
            mc_str = (f"MC rendimento medio: {tr.get('mean',0)*100:.2f}%, "
                      f"prob>0: {tr.get('prob_positive',0)*100:.1f}%")

        stab_str = ""
        if stab_summary:
            stab_str = (f"Stabilità: {stab_summary.get('verdict','?')}, "
                        f"score={stab_summary.get('stability_score','?')}")

        context = textwrap.dedent(f"""
            ## Strategia: {title}
            ## Metriche backtest: {_metrics_summary(metrics)}
            ## {mc_str}
            ## {stab_str}
        """).strip()
        return _claude_ask(
            "Fornisci una valutazione complessiva della strategia in 4-5 punti: "
            "qualità del backtest, robustezza statistica (MC), stabilità temporale, "
            "idoneità al live trading. Concludi con una raccomandazione chiara.",
            context=context,
        )

    def analyze_divergences(self, diff_df: pd.DataFrame, summary: Dict) -> str:
        """Diagnosi LLM delle divergenze pandas vs BT."""
        div_rows = diff_df[diff_df["divergence_type"] != "match"] if not diff_df.empty else diff_df
        context = textwrap.dedent(f"""
            ## Sommario divergenze
            {json.dumps(summary, indent=2)}

            ## Trade divergenti (max 15)
            {_df_summary(div_rows, max_rows=15)}
        """).strip()

        return _claude_ask(
            "Analizza le divergenze trade-by-trade tra l'engine pandas e Backtrader. "
            "Identifica la causa più probabile per ogni tipo di divergenza. "
            "Indica se la convergenza è accettabile o richiede correzioni.",
            context=context,
        )

    def analyze_montecarlo(self, stats: Dict, n_trades_real: int) -> str:
        """Interpreta risultati Monte Carlo."""
        tr = stats.get("total_return", {})
        dd = stats.get("max_drawdown", {})
        sh = stats.get("sharpe_ratio", {})

        context = textwrap.dedent(f"""
            ## Monte Carlo — statistiche principali
            Trade reali nel campione: {n_trades_real}

            ### Rendimento annuale
            Media: {tr.get('mean', 0)*100:.2f}%
            Mediana: {tr.get('median', 0)*100:.2f}%
            P5/P95: {tr.get('p5', 0)*100:.2f}% / {tr.get('p95', 0)*100:.2f}%
            Prob(>0%): {tr.get('prob_positive', 0)*100:.1f}%
            Prob(>10%): {tr.get('prob_gt_10pct', 0)*100:.1f}%

            ### Max Drawdown
            Media: {dd.get('mean', 0)*100:.2f}%
            P95 (worst case plausibile): {dd.get('p95', 0)*100:.2f}%

            ### Sharpe
            Media: {sh.get('mean', 0):.2f}
            P5: {sh.get('p5', 0):.2f}
        """).strip()

        return _claude_ask(
            "Interpreta questi risultati Monte Carlo. "
            "Valuta il profilo rischio/rendimento della strategia e "
            "indica se è adatta al trading reale.",
            context=context,
        )

    # ------------------------------------------------------------------
    # Interactive REPL
    # ------------------------------------------------------------------

    def run_interactive(self):
        """
        Loop interattivo minimale.
        L'utente digita un obiettivo, l'agente risponde con analisi.
        """
        print("=" * 60)
        print("BTPandasAgent — usa `claude --print` (subscription locale)")
        print("Digita 'exit' per uscire, 'help' per la guida.")
        print("=" * 60)

        while True:
            try:
                user_input = input("\n> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nArrivederci.")
                break

            if not user_input:
                continue
            if user_input.lower() in ("exit", "quit", "q"):
                break
            if user_input.lower() == "help":
                self._print_help()
                continue

            # Passa l'input direttamente a Claude con contesto corrente
            context = ""
            if self._last_metrics:
                context = f"## Ultime metriche backtest\n{_metrics_summary(self._last_metrics)}"

            response = _claude_ask(user_input, context=context)
            print(f"\n{response}")

    @staticmethod
    def _print_help():
        print(textwrap.dedent("""
            Uso programmatico:
              from agent.agent import BTPandasAgent
              from strategies.hma import HMAStrategy

              agent = BTPandasAgent()

              # Backtest singolo
              trades, metrics = agent.backtest(
                  symbol="NVDA",
                  strategy_cls=HMAStrategy,
                  strategy_params={"period": 16},
                  backtest_params={"sl_pct": 0.005},
                  fromdate="2025-01-01", todate="2025-01-31",
              )

              # Grid sweep
              results = agent.sweep(
                  symbol="NVDA",
                  strategy_cls=HMAStrategy,
                  strategy_grid={"period": [14, 16, 18], "inverted": [True]},
                  backtest_grid={"sl_pct": [0.003, 0.005]},
              )

              # Monte Carlo
              mc_results, stats = agent.montecarlo()

              # Confronto con BT
              diff, summary = agent.validate_vs_bt(
                  pd_trades=trades,
                  bt_strategy="intraday.HMA",
                  tickers=["NVDA"],
                  fromdate="2025-01-01", todate="2025-01-31",
              )
        """))
