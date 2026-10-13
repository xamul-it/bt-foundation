"""
CoherenceAnalyzer — confronto backtest / simulazione pandas / paper trading.

Risponde a: "la strategia si comporta allo stesso modo nei 3 contesti?"

Problematiche live/paper note (codificate nel comparator):
- Ordini SOLO a prezzo limite (no market order)
- Close + revert = due operazioni separate (prima chiudi, poi apri)
- No ordini sovrapposti (verifica pending orders)
- TTL ordini: gli ordini scadono entro la stessa giornata se non eseguiti
- Slippage e fill rate reale vs simulato
- Latenza: il segnale arriva dopo la chiusura della barra → fill sulla barra SUCCESSIVA
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)

# Soglie di allerta
FILL_RATE_WARN = 0.85       # se fill rate paper < 85% del simulato → allerta
SLIPPAGE_WARN_PCT = 0.05    # se slippage medio > 0.05% per trade → allerta
COUNT_DIFF_WARN = 0.10      # se trade count differisce > 10% → allerta
PNL_DIFF_WARN = 0.20        # se PnL totale differisce > 20% → allerta


class CoherenceAnalyzer:
    """
    Confronta metriche e trade tra backtest, simulazione e paper trading.

    Usa
    ---
    analyzer = CoherenceAnalyzer()

    # Registra i 3 contesti
    analyzer.add("backtest", bt_metrics, bt_trades)
    analyzer.add("simulation", pd_metrics, pd_trades)
    analyzer.add("paper", paper_metrics, paper_trades)

    comparison = analyzer.compare()
    issues = analyzer.detect_issues(comparison)
    """

    # Problematiche live/paper conosciute
    LIVE_ISSUES = {
        "market_order": (
            "Ordini a mercato non disponibili in live/paper. "
            "Usare LIMIT order al prezzo della barra (open/close)."
        ),
        "close_revert": (
            "Close-and-revert non atomico in live: inviare prima ordine di chiusura, "
            "attendere fill, poi inviare ordine di apertura opposto."
        ),
        "overlapping_orders": (
            "Ordini sovrapposti: verificare che non esista un ordine pending "
            "prima di inviarne uno nuovo (controllo via broker.get_open_orders())."
        ),
        "ttl_expiry": (
            "TTL ordini: in live gli ordini limit non eseguiti devono essere "
            "cancellati prima della chiusura (EOD cancel). "
            "In BT impostare exectype=bt.Order.Limit con valid=timedelta."
        ),
        "fill_lag": (
            "Fill lag: il segnale sulla barra T porta all'ordine inviato dopo T. "
            "Il fill realistico è alla barra T+1 (open), non al close di T."
        ),
        "partial_fill": (
            "Fill parziali: in paper/live un ordine può essere riempito parzialmente. "
            "La simulazione assume fill totale → sovrastima il trade count."
        ),
    }

    def __init__(self):
        self._contexts: Dict[str, Dict] = {}

    def add(
        self,
        context: str,
        metrics: Dict,
        trades: Optional[pd.DataFrame] = None,
    ) -> "CoherenceAnalyzer":
        """
        Registra un contesto (backtest / simulation / paper).

        Parameters
        ----------
        context : nome contesto (es. "backtest", "simulation", "paper")
        metrics : dict metriche (output di compute_metrics o BTRunner)
        trades : DataFrame trade (opzionale)
        """
        self._contexts[context] = {"metrics": metrics, "trades": trades}
        return self

    def compare(self) -> pd.DataFrame:
        """
        Confronto side-by-side di tutte le metriche registrate.

        Returns
        -------
        DataFrame con una riga per metrica e una colonna per contesto.
        """
        if not self._contexts:
            return pd.DataFrame()

        metric_keys = [
            "n_trades", "win_rate", "expectancy_pct", "sharpe",
            "sqn", "max_drawdown_pct", "total_return_pct", "profit_factor",
        ]
        rows = []
        for key in metric_keys:
            row = {"metric": key}
            for ctx, data in self._contexts.items():
                row[ctx] = data["metrics"].get(key)
            rows.append(row)

        df = pd.DataFrame(rows).set_index("metric")

        # Aggiungi colonne delta (se backtest è presente come riferimento)
        if "backtest" in self._contexts:
            for ctx in self._contexts:
                if ctx != "backtest":
                    df[f"delta_{ctx}_vs_bt"] = df[ctx] - df["backtest"]

        return df

    def detect_issues(self, comparison: Optional[pd.DataFrame] = None) -> List[Dict]:
        """
        Rileva problemi di coerenza tra i contesti.

        Returns
        -------
        Lista di dict: {severity, context, issue, detail, recommendation}
        """
        if comparison is None:
            comparison = self.compare()

        issues = []

        # Confronto quantitativo
        if "backtest" in self._contexts and "paper" in self._contexts:
            bt_m = self._contexts["backtest"]["metrics"]
            pp_m = self._contexts["paper"]["metrics"]

            # Trade count
            bt_n = bt_m.get("n_trades", 0)
            pp_n = pp_m.get("n_trades", 0)
            if bt_n > 0 and pp_n > 0:
                count_diff = abs(bt_n - pp_n) / bt_n
                if count_diff > COUNT_DIFF_WARN:
                    issues.append({
                        "severity": "HIGH",
                        "context": "paper vs backtest",
                        "issue": "trade_count_mismatch",
                        "detail": f"BT: {bt_n} trade, Paper: {pp_n} trade (diff {count_diff:.1%})",
                        "recommendation": (
                            "Verificare fill rate paper. "
                            "Probabili cause: TTL scaduto, ordini non eseguiti, "
                            "slippage troppo stretto."
                        ),
                    })

            # PnL totale
            bt_pnl = bt_m.get("total_return_pct", 0) or 0
            pp_pnl = pp_m.get("total_return_pct", 0) or 0
            if abs(bt_pnl) > 0.1:
                pnl_diff = abs(bt_pnl - pp_pnl) / abs(bt_pnl)
                if pnl_diff > PNL_DIFF_WARN:
                    issues.append({
                        "severity": "HIGH",
                        "context": "paper vs backtest",
                        "issue": "pnl_divergence",
                        "detail": f"BT: {bt_pnl:.2f}%, Paper: {pp_pnl:.2f}% (diff {pnl_diff:.1%})",
                        "recommendation": (
                            "Differenza PnL significativa. Analizzare slippage e "
                            "fill timing. Verificare che la strategia usi ordini limit."
                        ),
                    })

        # Confronto simulation vs backtest
        if "backtest" in self._contexts and "simulation" in self._contexts:
            bt_m = self._contexts["backtest"]["metrics"]
            sim_m = self._contexts["simulation"]["metrics"]
            bt_n = bt_m.get("n_trades", 0)
            sim_n = sim_m.get("n_trades", 0)
            if bt_n > 0 and sim_n > 0:
                diff = abs(bt_n - sim_n) / bt_n
                if diff > 0.05:
                    issues.append({
                        "severity": "MEDIUM",
                        "context": "simulation vs backtest",
                        "issue": "trade_count_mismatch",
                        "detail": f"BT: {bt_n}, Sim: {sim_n} (diff {diff:.1%})",
                        "recommendation": (
                            "Verificare parametri timing (exitbar, minutes_before_close, "
                            "entry su close vs open). Confrontare con comparator.py."
                        ),
                    })

        # Analisi trade-level per slippage
        for ctx, data in self._contexts.items():
            trades = data.get("trades")
            if trades is not None and len(trades) > 0 and "slippage_pct" in trades.columns:
                avg_slip = trades["slippage_pct"].abs().mean()
                if avg_slip > SLIPPAGE_WARN_PCT:
                    issues.append({
                        "severity": "MEDIUM",
                        "context": ctx,
                        "issue": "high_slippage",
                        "detail": f"Slippage medio: {avg_slip:.4f}%",
                        "recommendation": (
                            "Slippage elevato. Usare LIMIT order con tolleranza "
                            "di ±1 tick rispetto al prezzo segnale."
                        ),
                    })

        return issues

    def live_trading_checklist(self, trades: Optional[pd.DataFrame] = None) -> List[Dict]:
        """
        Checklist problematiche live/paper per la strategia analizzata.

        Analizza i trade per rilevare pattern incompatibili con live trading.
        """
        items = []

        for issue_key, desc in self.LIVE_ISSUES.items():
            item = {"check": issue_key, "description": desc, "status": "INFO"}

            if trades is not None and len(trades) > 0:
                if issue_key == "close_revert":
                    # Verifica se esistono trade con bars_held == 0 (stesso timestamp)
                    zero_bars = (trades.get("bars_held", pd.Series()) == 0).sum()
                    if zero_bars > 0:
                        item["status"] = "WARN"
                        item["detail"] = (
                            f"{zero_bars} trade con bars_held=0 rilevati. "
                            "In live richiedono close+open in due operazioni separate."
                        )

                elif issue_key == "fill_lag":
                    # Controlla se entry_time == segnale (close bar)
                    # vs entry_time == barra successiva (open T+1)
                    item["detail"] = (
                        "Engine pandas: entry al close di T (=BT). "
                        "In live reale: entry all'open di T+1 (lag di 1 barra). "
                        "Impatto stimato: ~0.05-0.15% per trade di slippage aggiuntivo."
                    )

                elif issue_key == "ttl_expiry":
                    # Stima % ordini che scaderebbero con TTL = 5 min
                    if "bars_held" in trades.columns:
                        short_trades = (trades["bars_held"] <= 3).sum()
                        pct = short_trades / len(trades) * 100
                        item["detail"] = (
                            f"{pct:.1f}% dei trade durano ≤3 barre. "
                            "Con TTL stretto potrebbero non essere eseguiti."
                        )

            items.append(item)

        return items

    def full_report_data(self) -> Dict:
        """Raccoglie tutti i dati per HTMLReport."""
        return {
            "comparison": self.compare(),
            "issues": self.detect_issues(),
            "checklist": self.live_trading_checklist(
                self._contexts.get("paper", {}).get("trades")
            ),
        }
