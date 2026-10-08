#!/usr/bin/env python3
"""Persist one scheduled ``today.json`` entry receipt.

The scheduler invokes this immediately after each Alpaca/backtest decision
run.  It never infers decisions from logs: a completed run without ``entry``
is deliberately stored as such so the UI can say "causale non comunicata".
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
sys.path.insert(0, str(BT_CORE))
import watchtower_runtime as wr  # noqa: E402


def _iso_date(value, field: str, required: bool = False):
    if value in (None, ""):
        if required:
            raise ValueError(f"today.json {field} is required")
        return None
    if not isinstance(value, str):
        raise ValueError(f"today.json {field} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"today.json {field} must be YYYY-MM-DD") from exc


def validate_today_contract(payload: dict, trading_date: date) -> tuple[dict | None, date | None]:
    """Validate the v2 scheduled receipt without inferring any outcome."""
    if payload.get("schema_version") != 2:
        raise ValueError("today.json schema_version must be 2")
    declared_date = _iso_date(payload.get("date"), "date", required=True)
    if declared_date != trading_date:
        raise ValueError(
            f"today.json date={declared_date.isoformat()!r} differs from "
            f"--trading-date={trading_date.isoformat()!r}"
        )
    signal_date = _iso_date(payload.get("signal_date"), "signal_date")
    entry = payload.get("entry")
    if entry is None:
        return None, signal_date
    if not isinstance(entry, dict):
        raise ValueError("today.json entry must be an object or null")
    if entry.get("outcome") not in ("orders", "no_orders"):
        raise ValueError("today.json entry.outcome must be orders or no_orders")
    reasons = entry.get("reasons")
    orders = entry.get("orders")
    if not isinstance(reasons, list) or not all(
        isinstance(reason, dict) and isinstance(reason.get("code"), str) and reason["code"].strip()
        for reason in reasons
    ):
        raise ValueError("today.json entry.reasons must contain objects with a code")
    if not isinstance(orders, list):
        raise ValueError("today.json entry.orders must be a list")
    for order in orders:
        if not isinstance(order, dict):
            raise ValueError("today.json entry.orders must contain objects")
        required = ("symbol", "side", "qty", "reference_price", "feed_date")
        if any(order.get(key) in (None, "") for key in required):
            raise ValueError(f"today.json entry order missing field: {order!r}")
        if str(order["side"]).lower() not in ("buy", "sell"):
            raise ValueError("today.json entry order side must be buy or sell")
        try:
            qty = int(order["qty"])
            price = float(order["reference_price"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"today.json entry order has invalid qty/price: {order!r}") from exc
        if qty <= 0 or not math.isfinite(price) or price <= 0:
            raise ValueError(f"today.json entry order has invalid qty/price: {order!r}")
        _iso_date(order["feed_date"], "entry.orders.feed_date", required=True)
    if entry["outcome"] == "no_orders" and orders:
        raise ValueError("today.json no_orders outcome cannot include orders")
    if "sizing_capital" not in entry:
        raise ValueError("today.json entry.sizing_capital is required")
    sizing_capital = entry["sizing_capital"]
    if sizing_capital is not None:
        try:
            sizing_capital = float(sizing_capital)
        except (TypeError, ValueError) as exc:
            raise ValueError("today.json entry.sizing_capital must be numeric or null") from exc
        if not math.isfinite(sizing_capital) or sizing_capital < 0:
            raise ValueError("today.json entry.sizing_capital must be finite and non-negative")
    if entry["outcome"] == "orders" and sizing_capital is None:
        raise ValueError("today.json orders outcome requires sizing_capital")
    return entry, signal_date


def reconcile_best_effort(repo: wr.WatchtowerRepository, profile: str, trading_date: date) -> None:
    """A comparison failure must not invalidate an already durable receipt."""
    try:
        repo.reconcile_scheduled_entry_decisions(profile, trading_date)
    except Exception as exc:  # noqa: BLE001 - observability must be non-blocking
        print(
            f"WATCHTOWER_ENTRY_COMPARISON_FAILED profile={profile} date={trading_date}: {exc}",
            file=sys.stderr,
        )


def settlement_best_effort(
    repo: wr.WatchtowerRepository, profile: str, trading_date: date, source: str, orders: list[dict],
) -> None:
    """A pending-settlement write is observability, never a trading gate."""
    if not orders:
        return
    try:
        repo.ensure_scheduled_entry_settlements(profile, trading_date, source, orders)
    except Exception as exc:  # noqa: BLE001 - receipt remains authoritative
        print(
            f"WATCHTOWER_ENTRY_SETTLEMENT_SEED_FAILED profile={profile} date={trading_date} source={source}: {exc}",
            file=sys.stderr,
        )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--source", required=True, choices=("alpaca", "backtest"))
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--today-path")
    parser.add_argument("--running", action="store_true")
    parser.add_argument("--failed")
    args = parser.parse_args(argv)
    trading_date = date.fromisoformat(args.trading_date)
    repo = wr.WatchtowerRepository()

    if args.running:
        repo.upsert_scheduled_entry_decision(args.profile, trading_date, args.source, "running")
        reconcile_best_effort(repo, args.profile, trading_date)
        return 0
    if args.failed is not None:
        repo.upsert_scheduled_entry_decision(
            args.profile, trading_date, args.source, "failed", error=args.failed,
        )
        reconcile_best_effort(repo, args.profile, trading_date)
        return 0
    if not args.today_path:
        parser.error("--today-path is required for a completed run")
    path = Path(args.today_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("today.json must contain an object")
    entry, signal_date = validate_today_contract(payload, trading_date)
    outcome = entry.get("outcome") if entry else None
    repo.upsert_scheduled_entry_decision(
        args.profile, trading_date, args.source, "completed", outcome=outcome,
        signal_date=signal_date,
        reasons=(entry or {}).get("reasons") or [], orders=(entry or {}).get("orders") or [],
        payload=payload, artifact_path=str(path),
    )
    if outcome == "orders":
        settlement_best_effort(repo, args.profile, trading_date, args.source, (entry or {}).get("orders") or [])
    reconcile_best_effort(repo, args.profile, trading_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
