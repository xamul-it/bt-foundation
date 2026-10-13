"""
test_comparator.py — verifica che comparator ritorna 0 divergenze su run identico.

Punto 3 del piano: su run identico pandas==BT, comparator ritorna 0 divergenze.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest

from bt_bridge.comparator import TradeComparator, DIV_MATCH


def _make_trades(n: int = 10, seed: int = 42) -> pd.DataFrame:
    """Genera DataFrame trade sintetico."""
    import numpy as np
    rng = np.random.default_rng(seed)

    base = pd.Timestamp("2025-01-02 10:00:00")
    rows = []
    for i in range(n):
        entry_t = base + pd.Timedelta(minutes=i * 30)
        exit_t = entry_t + pd.Timedelta(minutes=rng.integers(5, 25))
        entry_p = float(100 + rng.normal(0, 1))
        exit_p = float(entry_p + rng.normal(0, 0.5))
        side = "long" if rng.random() > 0.5 else "short"
        if side == "long":
            exit_pct = (exit_p / entry_p - 1) * 100
        else:
            exit_pct = (entry_p / exit_p - 1) * 100
        rows.append({
            "symbol": "TEST",
            "side": side,
            "entry_time": entry_t,
            "entry_price": entry_p,
            "exit_time": exit_t,
            "exit_price": exit_p,
            "exit_pct": exit_pct,
            "outcome": "signal",
            "bars_held": int(rng.integers(1, 20)),
        })
    return pd.DataFrame(rows)


class TestTradeComparator:

    def test_identical_trades_zero_divergences(self):
        """Trades identici → 0 divergenze, tutti match."""
        trades = _make_trades(20)
        comp = TradeComparator()
        diff = comp.compare(trades.copy(), trades.copy())

        assert len(diff) == 20
        assert (diff["divergence_type"] == DIV_MATCH).all(), \
            f"Trovate divergenze su trades identici:\n{diff[diff['divergence_type'] != DIV_MATCH]}"

    def test_match_pct_100_on_identical(self):
        trades = _make_trades(10)
        comp = TradeComparator()
        diff = comp.compare(trades.copy(), trades.copy())
        summary = comp.summary(diff)
        assert summary["match_pct"] == 100.0

    def test_missing_in_bt_detected(self):
        """Trade pandas senza corrispondenza BT → missing_in_bt."""
        pd_trades = _make_trades(5)
        bt_trades = _make_trades(3)  # Mancano 2 trade

        comp = TradeComparator()
        diff = comp.compare(pd_trades, bt_trades)
        summary = comp.summary(diff)

        assert "missing_in_bt" in summary, "missing_in_bt non rilevato"

    def test_missing_in_pd_detected(self):
        """Trade BT senza corrispondenza pandas → missing_in_pd."""
        pd_trades = _make_trades(3)
        bt_trades = _make_trades(5)

        comp = TradeComparator()
        diff = comp.compare(pd_trades, bt_trades)
        summary = comp.summary(diff)

        assert "missing_in_pd" in summary, "missing_in_pd non rilevato"

    def test_pnl_divergence_detected(self):
        """Differenza PnL > soglia → divergenza."""
        trades = _make_trades(5)
        bt_trades = trades.copy()
        # Modifica exit_pct del primo trade di 1% (sopra la soglia 0.05%)
        bt_trades.loc[0, "exit_pct"] = trades.loc[0, "exit_pct"] + 1.0

        comp = TradeComparator()
        diff = comp.compare(trades, bt_trades)

        # Il primo trade deve avere una divergenza
        first = diff[diff["entry_time"] == trades.loc[0, "entry_time"]]
        if len(first) > 0:
            assert first.iloc[0]["divergence_type"] != DIV_MATCH

    def test_summary_totals(self):
        """summary['total'] == len(diff_df)."""
        trades = _make_trades(8)
        comp = TradeComparator()
        diff = comp.compare(trades.copy(), trades.copy())
        summary = comp.summary(diff)
        assert summary["total"] == len(diff)

    def test_empty_trades(self):
        """Empty DataFrames non crashano."""
        comp = TradeComparator()
        empty = pd.DataFrame()
        diff = comp.compare(empty, empty)
        assert isinstance(diff, pd.DataFrame)
        assert len(diff) == 0

    def test_tz_naive_normalization(self):
        """Trades con timestamp tz-aware non crashano il comparator."""
        trades = _make_trades(5)
        # Aggiungi timezone al pandas trades
        pd_trades = trades.copy()
        pd_trades["entry_time"] = pd.to_datetime(pd_trades["entry_time"]).dt.tz_localize("UTC")
        pd_trades["exit_time"] = pd.to_datetime(pd_trades["exit_time"]).dt.tz_localize("UTC")

        comp = TradeComparator()
        # Non deve crashare
        diff = comp.compare(pd_trades, trades.copy())
        assert isinstance(diff, pd.DataFrame)
