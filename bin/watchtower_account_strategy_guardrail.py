#!/usr/bin/env python3
"""Requirement C guardrail: verify that no account observed in
alpaca_order_cache (populated by watchtower_poll_alpaca_orders.py) carries
orders attributable to more than one strategy/profile. Findings are
persisted in account_strategy_guardrail_alerts (never just logged and
dropped), same idea as watchtower_alerts / execution_reconciliation_results.

Two independent layers, checked together every run:

1. source_account consistency (RELIABLE TODAY): watchtower_poll_alpaca_orders.py
   tags every order it writes with source_account=<profile that owned the
   TradingClient used to fetch it> -- by construction this is already 1:1
   per portfolio_key_id (one poll = one profile's credentials), so this
   layer can never produce a false alarm on today's real data. It exists to
   catch registry/config drift (e.g. two profiles' ACCOUNT_ENV pointing at
   the same Alpaca account by mistake), which IS something client_order_id
   content can never tell you.

2. client_order_id run_tag consistency (INERT TODAY, meaningful once the
   Phase-3 fix is promoted): before the fix, every overnight_ah* profile's
   client_order_id truncates to the SAME 8-char prefix (see
   strategies/multiTickerStrategy.py's _build_client_order_id fix and
   bin/submit_moo.py's build_exit_client_order_id) -- indistinguishable by
   design, so this layer must never raise on that legacy format. It only
   recognises the NEW format (a 6-hex-char run_tag segment, produced by a
   short deterministic hash -- see _cid_short_hash/_short_hash in those two
   files), which the OLD format's plain-text truncated names never
   accidentally match (they contain non-hex letters). Until the fix is
   promoted to backtrader-prod, this layer will simply find zero matching
   tags on every account and stay silent -- not a false negative, an
   accurate "not measurable yet" state.

"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import watchtower_runtime as wr  # noqa: E402

_HEX6 = re.compile(r"^[0-9a-f]{6}$")


def extract_run_tag(client_order_id: str | None) -> str | None:
    """Recognise ONLY the new hash-based client_order_id scheme: 'bt_...'
    with a 6-hex-char run_tag in the third underscore-separated segment
    (true for both the entry format bt_{strategy6}_{run6}_... and the exit
    format bt_exit_{run6}_...). The legacy 8-char plain-text-prefix scheme
    never matches this (its tags contain non-hex letters), so this
    correctly returns None for all of today's real order data."""
    cid = str(client_order_id or "").strip().lower()
    parts = cid.split("_")
    if len(parts) < 3 or parts[0] != "bt":
        return None
    candidate = parts[2]
    return candidate if _HEX6.match(candidate) else None


def _already_open(open_alerts: list[dict[str, Any]], portfolio_key_id: str, alert_type: str) -> bool:
    return any(a["portfolio_key_id"] == portfolio_key_id and a["alert_type"] == alert_type for a in open_alerts)


def run_guardrail(repo: "wr.WatchtowerRepository", since_days: int = 90) -> dict[str, Any]:
    """`since_days` scopes the check to operationally relevant, currently
    fresh data. Discovered while first running this against real data:
    alpaca_order_cache still holds ~600 rows per account from 2026-04-20/21
    with the generic source_account='paper' label, written by the old
    intraday _sync_alpaca_order_cache pipeline before it died (see the
    Fase-0 finding, bt-watchtower/bt-live-event-writer inactive since
    2026-04-27) -- a different data generation era, not a live
    cross-strategy conflict. Comparing across that boundary produced a
    guaranteed false 'multiple_source_accounts' alarm on every real
    account. Scoping to a recent window (default 90 days, well past
    watchtower_poll_alpaca_orders.py's own --days lookback) is the
    principled fix: it evaluates "is this account's CURRENT operational
    reality consistent", not "has this portfolio_key_id ever, across any
    writer in its whole history, seen more than one label" -- the latter
    is not what a periodic guardrail is meant to answer, and would need
    reopening every time old dead-pipeline rows happen to still be cached.
    """
    registry = {row["portfolio_key_id"]: row for row in repo.list_assigned_portfolios()}
    open_alerts = repo.list_open_guardrail_alerts()

    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT portfolio_key_id, source_account, client_order_id
                FROM alpaca_order_cache
                WHERE portfolio_key_id IS NOT NULL AND portfolio_key_id <> ''
                  AND window_open >= CURRENT_DATE - (%s || ' days')::interval
                """,
                (since_days,),
            )
            rows = wr._cursor_rows(cur)

    by_account: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_account[row["portfolio_key_id"]].append(row)

    findings: list[dict[str, Any]] = []
    checked_accounts = 0
    for portfolio_key_id, orders in by_account.items():
        checked_accounts += 1
        reg = registry.get(portfolio_key_id)
        source_accounts = sorted({o["source_account"] for o in orders if o["source_account"]})
        run_tags = sorted({t for o in orders if (t := extract_run_tag(o["client_order_id"]))})

        if reg is None:
            if not _already_open(open_alerts, portfolio_key_id, "unregistered_account"):
                repo.record_guardrail_alert(
                    portfolio_key_id, "unregistered_account",
                    {"observed_source_accounts": source_accounts, "order_count": len(orders)},
                    severity="warning",
                )
            findings.append({"portfolio_key_id": portfolio_key_id, "status": "unregistered",
                              "source_accounts": source_accounts})
            continue

        assigned_profile = reg["assigned_profile"]
        problems = []

        # Layer 1 -- reliable today.
        if len(source_accounts) > 1:
            if not _already_open(open_alerts, portfolio_key_id, "multiple_source_accounts"):
                repo.record_guardrail_alert(
                    portfolio_key_id, "multiple_source_accounts",
                    {"observed_source_accounts": source_accounts, "assigned_profile": assigned_profile},
                    assigned_profile=assigned_profile, severity="error",
                )
            problems.append(f"multiple_source_accounts={source_accounts}")
        elif source_accounts and source_accounts[0] != assigned_profile:
            if not _already_open(open_alerts, portfolio_key_id, "source_account_registry_mismatch"):
                repo.record_guardrail_alert(
                    portfolio_key_id, "source_account_registry_mismatch",
                    {"observed_source_account": source_accounts[0], "assigned_profile": assigned_profile},
                    assigned_profile=assigned_profile, severity="error",
                )
            problems.append(f"source_account={source_accounts[0]!r} != assigned_profile={assigned_profile!r}")

        # Layer 2 -- inert until the client_order_id fix is promoted; only
        # fires once real tags start appearing (run_tags non-empty).
        if len(run_tags) > 1:
            if not _already_open(open_alerts, portfolio_key_id, "multiple_client_order_id_run_tags"):
                repo.record_guardrail_alert(
                    portfolio_key_id, "multiple_client_order_id_run_tags",
                    {"observed_run_tags": run_tags, "assigned_profile": assigned_profile},
                    assigned_profile=assigned_profile, severity="error",
                )
            problems.append(f"multiple_client_order_id_run_tags={run_tags}")
        elif run_tags:
            expected_tag = (reg.get("metadata") or {}).get("expected_client_order_run_tag")
            if expected_tag and run_tags[0] != expected_tag:
                if not _already_open(open_alerts, portfolio_key_id, "client_order_id_run_tag_mismatch"):
                    repo.record_guardrail_alert(
                        portfolio_key_id, "client_order_id_run_tag_mismatch",
                        {"observed_run_tag": run_tags[0], "expected_run_tag": expected_tag,
                         "assigned_profile": assigned_profile},
                        assigned_profile=assigned_profile, severity="error",
                    )
                problems.append(f"run_tag={run_tags[0]!r} != expected={expected_tag!r}")

        findings.append({
            "portfolio_key_id": portfolio_key_id, "assigned_profile": assigned_profile,
            "status": "clean" if not problems else "flagged",
            "problems": problems,
            "client_order_id_tagging": "new_scheme_observed" if run_tags else "legacy_scheme_or_no_orders_yet",
        })

    return {
        "accounts_checked": checked_accounts,
        "findings": findings,
        "open_alerts_after_run": repo.list_open_guardrail_alerts(),
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db-dsn", default=None)
    parser.add_argument("--since-days", type=int, default=90,
                         help="Only evaluate orders with window_open within this many days (default 90)")
    args = parser.parse_args(argv)
    repo = wr.WatchtowerRepository(dsn=args.db_dsn)
    print(json.dumps(run_guardrail(repo, since_days=args.since_days), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
