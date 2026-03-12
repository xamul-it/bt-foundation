import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


def _load_module():
    mod_path = Path(__file__).with_name("alpaca_reconcile.py")
    spec = importlib.util.spec_from_file_location("alpaca_reconcile", str(mod_path))
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


ar = _load_module()
UTC = timezone.utc


class DummyPos:
    def __init__(self, symbol, qty, avg_entry_price, market_value, unrealized_pl):
        self.symbol = symbol
        self.qty = qty
        self.avg_entry_price = avg_entry_price
        self.market_value = market_value
        self.unrealized_pl = unrealized_pl


class AlpacaReconcileTests(unittest.TestCase):
    def test_fetch_alpaca_orders_paginates_beyond_default_limit(self):
        class FakeOrder:
            def __init__(self, oid, submitted_at):
                self.id = oid
                self.submitted_at = submitted_at

        class FakeClient:
            def __init__(self, orders):
                self.orders = sorted(orders, key=lambda o: o.submitted_at)

            def get_orders(self, filter):
                after = filter.after
                until = filter.until
                limit = int(filter.limit or 50)
                out = [o for o in self.orders if o.submitted_at > after and o.submitted_at <= until]
                return out[:limit]

        base = datetime(2026, 3, 6, 15, 0, tzinfo=UTC)
        orders = [FakeOrder(f"o{i:04d}", base + ar.timedelta(seconds=i)) for i in range(1200)]
        client = FakeClient(orders)

        got = ar.fetch_alpaca_orders(
            client,
            start_dt=base - ar.timedelta(seconds=1),
            end_dt=base + ar.timedelta(seconds=2000),
        )
        self.assertEqual(len(got), 1200)
        self.assertEqual(got[0].id, "o0000")
        self.assertEqual(got[-1].id, "o1199")

    def test_infer_window_from_inputs(self):
        with tempfile.TemporaryDirectory() as td:
            sim = Path(td) / "sim"
            sim.mkdir(parents=True)
            data = [
                {
                    "datetime": "2026-03-04 14:30:00",
                    "events": [{"exec_dt": "2026-03-04 14:31:00"}],
                },
                {
                    "created": {"signal_dt": "2026-03-04 20:59:00"},
                    "lastdatetime": "2026-03-04 21:00:00",
                },
            ]
            (sim / "orderhistory.json").write_text(json.dumps(data), encoding="utf-8")
            start, end, counts = ar.infer_window_from_inputs(sim, None)
            self.assertEqual(start.strftime("%Y-%m-%d %H:%M:%S"), "2026-03-04 14:30:00")
            self.assertEqual(end.strftime("%Y-%m-%d %H:%M:%S"), "2026-03-04 21:00:00")
            self.assertGreater(counts["sim"], 0)

    def test_split_order_into_segments_flip(self):
        orders = [
            {
                "order_id": "1",
                "symbol": "AAPL",
                "side": "buy",
                "filled_qty": 10,
                "qty": 10,
                "submitted_at_dt": datetime(2026, 3, 4, 15, 0, tzinfo=UTC),
            },
            {
                "order_id": "2",
                "symbol": "AAPL",
                "side": "sell",
                "filled_qty": 15,
                "qty": 15,
                "submitted_at_dt": datetime(2026, 3, 4, 16, 0, tzinfo=UTC),
            },
        ]
        segs = ar.split_order_into_segments(orders)
        close = [s for s in segs if s["order_id"] == "2" and s["is_close"]]
        open_ = [s for s in segs if s["order_id"] == "2" and not s["is_close"]]
        self.assertEqual(len(close), 1)
        self.assertEqual(len(open_), 1)
        self.assertAlmostEqual(close[0]["segment_qty"], 10.0)
        self.assertAlmostEqual(open_[0]["segment_qty"], 5.0)

    def test_build_trade_rows_fifo(self):
        orders = [
            {
                "order_id": "o1",
                "client_order_id": "c1",
                "symbol": "MSFT",
                "side": "buy",
                "status": "filled",
                "submitted_at": "2026-03-04 15:00:00",
                "filled_at": "2026-03-04 15:00:01",
                "updated_at": "2026-03-04 15:00:01",
                "qty": 10.0,
                "filled_qty": 10.0,
                "filled_avg_price": 100.0,
                "limit_price": None,
                "time_in_force": "day",
                "order_type": "market",
                "source": "alpaca_api",
                "submitted_at_dt": datetime(2026, 3, 4, 15, 0, tzinfo=UTC),
                "filled_at_dt": datetime(2026, 3, 4, 15, 0, 1, tzinfo=UTC),
                "updated_at_dt": datetime(2026, 3, 4, 15, 0, 1, tzinfo=UTC),
            },
            {
                "order_id": "o2",
                "client_order_id": "c2",
                "symbol": "MSFT",
                "side": "sell",
                "status": "filled",
                "submitted_at": "2026-03-04 15:05:00",
                "filled_at": "2026-03-04 15:05:01",
                "updated_at": "2026-03-04 15:05:01",
                "qty": 6.0,
                "filled_qty": 6.0,
                "filled_avg_price": 101.0,
                "limit_price": None,
                "time_in_force": "day",
                "order_type": "market",
                "source": "alpaca_api",
                "submitted_at_dt": datetime(2026, 3, 4, 15, 5, tzinfo=UTC),
                "filled_at_dt": datetime(2026, 3, 4, 15, 5, 1, tzinfo=UTC),
                "updated_at_dt": datetime(2026, 3, 4, 15, 5, 1, tzinfo=UTC),
            },
        ]
        trades, recon = ar.build_trade_rows_fifo(orders)
        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t["status"], "PARTIAL")
        self.assertAlmostEqual(float(t["closed_qty"]), 6.0)
        self.assertAlmostEqual(float(t["remaining_qty"]), 4.0)
        self.assertAlmostEqual(float(t["pnl"]), 6.0)
        self.assertIn("MSFT", recon)
        self.assertAlmostEqual(float(recon["MSFT"]["net_qty"]), 4.0)

    def test_build_position_rows(self):
        positions = [DummyPos("TSLA", "3", "210", "630", "12")]
        reconstructed = {
            "TSLA": {"net_qty": 2.5, "avg_price": 208.0},
            "NVDA": {"net_qty": -1.0, "avg_price": 900.0},
        }
        rows = ar.build_position_rows(positions, reconstructed)
        by_sym = {r["symbol"]: r for r in rows}
        self.assertIn("TSLA", by_sym)
        self.assertIn("NVDA", by_sym)
        self.assertAlmostEqual(float(by_sym["TSLA"]["qty_diff"]), 0.5)
        self.assertAlmostEqual(float(by_sym["NVDA"]["qty"]), 0.0)
        self.assertAlmostEqual(float(by_sym["NVDA"]["reconstructed_net_qty"]), -1.0)

    def test_to_eod_trades_includes_partial_realized_qty(self):
        trade_rows = [
            {
                "symbol": "INTC",
                "entry_side": "long",
                "open_exec_dt": "2026-03-05 20:31:29",
                "close_exec_dt": "2026-03-05 20:35:02",
                "open_price": 45.51,
                "closed_qty": 17.0,
                "remaining_qty": 1.0,
                "pnl": 2.21,
                "pnl_pct": 0.285652,
                "status": "PARTIAL",
            }
        ]
        eod = ar.to_eod_trades(trade_rows, bar_seconds=60)
        self.assertEqual(len(eod), 1)
        row = eod[0]
        self.assertAlmostEqual(float(row["size"]), 17.0)
        self.assertAlmostEqual(float(row["pnl"]), 2.21)
        self.assertAlmostEqual(float(row["value"]), 17.0 * 45.51)


if __name__ == "__main__":
    unittest.main()
