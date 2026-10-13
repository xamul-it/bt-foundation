"""
TradeComparator — diff trade-by-trade pandas vs Backtrader.

Generico: funziona con qualsiasi coppia di DataFrame trade con colonne standard:
    symbol, entry_time, entry_price, exit_time, exit_price, exit_pct, side

Output: DataFrame diagnostico con colonna divergence_type.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)

# Tipi di divergenza
DIV_FILL_TIMING = "fill_timing"
DIV_SL_TP_LEVEL = "sl_tp_level"
DIV_EXITBAR = "exitbar"
DIV_MISSING_IN_PD = "missing_in_pd"
DIV_MISSING_IN_BT = "missing_in_bt"
DIV_MATCH = "match"

# Tolleranze
PRICE_TOL = 0.01       # $0.01 differenza entry/exit
TIME_TOL_MIN = 2       # 2 minuti tolleranza su timestamp
PNL_TOL = 0.05         # 0.05% differenza exit_pct


class TradeComparator:
    """
    Confronta trades pandas vs trades Backtrader barra per barra.

    Uso
    ---
    comp = TradeComparator()
    diff_df = comp.compare(pd_trades, bt_trades)
    summary = comp.summary(diff_df)
    """

    def compare(
        self,
        pd_trades: pd.DataFrame,
        bt_trades: pd.DataFrame,
        symbol: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Join su (symbol, entry_time) con tolleranza temporale,
        produce DataFrame diagnostico.

        Returns
        -------
        DataFrame con colonne:
            symbol, entry_time, pd_entry, bt_entry,
            pd_exit, bt_exit, pd_exit_pct, bt_exit_pct,
            delta_pnl, divergence_type, detail
        """
        pd_t = self._normalize(pd_trades, "pd")
        bt_t = self._normalize(bt_trades, "bt")

        rows = []

        # Filtra per simbolo se richiesto
        if symbol:
            pd_t = pd_t[pd_t["symbol"] == symbol]
            bt_t = bt_t[bt_t["symbol"] == symbol]

        # Match greedy su entry_time con tolleranza
        pd_matched = set()
        bt_matched = set()

        for pd_idx, pd_row in pd_t.iterrows():
            best_bt_idx = self._find_match(pd_row, bt_t, bt_matched)

            if best_bt_idx is None:
                rows.append(self._make_row(pd_row, None, DIV_MISSING_IN_BT))
                continue

            bt_row = bt_t.loc[best_bt_idx]
            pd_matched.add(pd_idx)
            bt_matched.add(best_bt_idx)

            div_type, detail = self._classify(pd_row, bt_row)
            rows.append(self._make_row(pd_row, bt_row, div_type, detail))

        # BT trades senza corrispondenza pandas
        for bt_idx, bt_row in bt_t.iterrows():
            if bt_idx not in bt_matched:
                rows.append(self._make_row(None, bt_row, DIV_MISSING_IN_PD))

        return pd.DataFrame(rows)

    def summary(self, diff_df: pd.DataFrame) -> dict:
        """Ritorna conteggi per tipo di divergenza."""
        if diff_df.empty:
            return {}
        counts = diff_df["divergence_type"].value_counts().to_dict()
        n = len(diff_df)
        counts["total"] = n
        counts["match_pct"] = round(counts.get(DIV_MATCH, 0) / n * 100, 1) if n > 0 else 0.0
        return counts

    # ------------------------------------------------------------------

    def _normalize(self, df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        df = df.copy()
        # Normalizza entry/exit_time a tz-naive
        for col in ["entry_time", "exit_time"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                if hasattr(df[col].dtype, "tz") and df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
        if "symbol" not in df.columns:
            df["symbol"] = "UNKNOWN"
        return df

    def _find_match(self, pd_row, bt_t: pd.DataFrame, already_matched: set):
        """Trova il trade BT più vicino per (symbol, side, entry_time)."""
        if bt_t.empty:
            return None

        mask = (
            (bt_t["symbol"] == pd_row["symbol"]) &
            (bt_t["side"] == pd_row["side"]) &
            (~bt_t.index.isin(already_matched))
        )
        candidates = bt_t[mask]
        if candidates.empty:
            return None

        pd_time = pd.Timestamp(pd_row["entry_time"])
        time_diffs = (candidates["entry_time"] - pd_time).abs()
        min_diff = time_diffs.min()

        if min_diff > pd.Timedelta(minutes=TIME_TOL_MIN):
            return None

        return time_diffs.idxmin()

    def _classify(self, pd_row, bt_row) -> tuple[str, str]:
        """Classifica il tipo di divergenza tra due trade matched."""
        details = []

        entry_diff = abs(pd_row.get("entry_price", 0) - bt_row.get("entry_price", 0))
        if entry_diff > PRICE_TOL:
            details.append(f"entry_price delta={entry_diff:.4f}")

        exit_diff = abs(pd_row.get("exit_price", 0) - bt_row.get("exit_price", 0))
        pnl_diff = abs(pd_row.get("exit_pct", 0) - bt_row.get("exit_pct", 0))

        pd_outcome = pd_row.get("outcome", "")
        bt_outcome = bt_row.get("outcome", "")

        if pnl_diff > PNL_TOL:
            if "sl" in (pd_outcome, bt_outcome) or "tp" in (pd_outcome, bt_outcome):
                return DIV_SL_TP_LEVEL, f"pnl_diff={pnl_diff:.4f}% outcome pd={pd_outcome} bt={bt_outcome}"
            if "exitbar" in (pd_outcome, bt_outcome):
                return DIV_EXITBAR, f"pnl_diff={pnl_diff:.4f}%"
            return DIV_FILL_TIMING, f"pnl_diff={pnl_diff:.4f}%"

        if details:
            return DIV_FILL_TIMING, "; ".join(details)

        return DIV_MATCH, ""

    @staticmethod
    def _make_row(pd_row, bt_row, div_type: str, detail: str = "") -> dict:
        def _get(row, key, default=None):
            if row is None:
                return default
            if isinstance(row, dict):
                return row.get(key, default)
            # pandas Series
            try:
                val = row[key]
                return val if not (hasattr(val, "__len__") and not isinstance(val, str)) else default
            except (KeyError, IndexError):
                return default

        # Usa il primo row non-None per symbol/side
        ref_row = pd_row if pd_row is not None else bt_row

        pd_entry = _get(pd_row, "entry_price")
        bt_entry = _get(bt_row, "entry_price")
        pd_exit = _get(pd_row, "exit_price")
        bt_exit = _get(bt_row, "exit_price")
        pd_pct = _get(pd_row, "exit_pct", 0.0)
        bt_pct = _get(bt_row, "exit_pct", 0.0)

        delta = None
        if pd_pct is not None and bt_pct is not None:
            try:
                delta = round(float(pd_pct) - float(bt_pct), 4)
            except Exception:
                delta = None

        return {
            "symbol": _get(ref_row, "symbol", "?"),
            "side": _get(ref_row, "side", "?"),
            "entry_time": _get(pd_row, "entry_time"),
            "pd_entry": pd_entry,
            "bt_entry": bt_entry,
            "pd_exit": pd_exit,
            "bt_exit": bt_exit,
            "pd_exit_pct": pd_pct,
            "bt_exit_pct": bt_pct,
            "delta_pnl": delta,
            "divergence_type": div_type,
            "detail": detail,
        }
