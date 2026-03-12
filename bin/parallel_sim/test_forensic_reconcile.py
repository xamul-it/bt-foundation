import importlib.util
import sys
import unittest
from pathlib import Path


def _load_module():
    p = Path(__file__).with_name("forensic_reconcile.py")
    spec = importlib.util.spec_from_file_location("forensic_reconcile", str(p))
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


fr = _load_module()


class ForensicReconcileTests(unittest.TestCase):
    def test_classify_no_external_is_ok(self):
        row = {
            "paper_status": "accepted",
            "paper_filled_qty": 0.0,
            "invariant_fill_monotonic": True,
            "invariant_is_close_coherent": True,
        }
        cls, reason = fr._classify(row, qty_tol=1e-8, price_tol=1e-6)
        self.assertEqual(cls, "OK")
        self.assertIn("nessun match", reason)

    def test_classify_partial_misapplied_on_qty_diff(self):
        row = {
            "alpaca_order_id": "abc",
            "paper_status": "partial",
            "alpaca_status": "partial",
            "paper_filled_qty": 2.0,
            "alpaca_filled_qty": 3.0,
            "paper_filled_avg_price": 10.0,
            "alpaca_filled_avg_price": 10.0,
            "invariant_fill_monotonic": True,
            "invariant_is_close_coherent": True,
        }
        cls, _ = fr._classify(row, qty_tol=1e-8, price_tol=1e-6)
        self.assertEqual(cls, "PARTIAL_MISAPPLIED")

    def test_classify_ttl_race(self):
        row = {
            "alpaca_order_id": "abc",
            "paper_status": "canceled",
            "alpaca_status": "filled",
            "paper_filled_qty": 1.0,
            "alpaca_filled_qty": 1.0,
            "audit_fill_after_ttl": True,
            "invariant_fill_monotonic": True,
            "invariant_is_close_coherent": True,
        }
        cls, _ = fr._classify(row, qty_tol=1e-8, price_tol=1e-6)
        self.assertEqual(cls, "TTL_RACE")


if __name__ == "__main__":
    unittest.main()
