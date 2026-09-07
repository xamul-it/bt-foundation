#!/usr/bin/env python3
"""Poll real Alpaca orders/positions for every scheduled overnight_ah*
profile and cache them in Postgres (alpaca_order_cache /
alpaca_position_cache, bt_live_events DB) with the correct
`portfolio_key_id` and `source_account=<profile>`.

This is the "poller" side of requirement A (execution reconciliation): the
`watchtower_replay_reconcile.py` job compares this cache against a
backtest-replay, it never calls the Alpaca API itself.

Credential resolution intentionally mirrors bin/alpaca_audit/fetch_data.py
and bin/compare_live_backtest_trades.py (ALPACA_API_KEY/ALPACA_SECRET_KEY
from the profile's ACCOUNT_ENV file), NOT the alpaca_portfolios DB table --
per the closed decision that alpaca_portfolios' existing rows
(HMA-plain/OvernightAH) belong to another piece of work and must not be
reused here.

`--profile` is optional and filters to one profile; with no filter, every
profile discovered in ~/.config/backtrader/scheduled/*.env is polled --
zero hardcoded profile names.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import watchtower_runtime as wr  # noqa: E402

# Shared with watchtower_backfill_profile_params.py -- kept duplicated
# (not imported) since these are two independent standalone scripts by
# convention in this bin/ tree.
WORKSPACE_ROOT = Path(
    os.environ.get("BT_WORKSPACE_ROOT", str(Path.home() / "backtrader"))
).resolve()
SCHEDULED_PROFILES_DIR = Path(
    os.environ.get("BT_SCHEDULED_PROFILES_DIR", str(Path.home() / ".config" / "backtrader" / "scheduled"))
)
ACCOUNTS_DIR = Path(
    os.environ.get("BT_SCHEDULED_ACCOUNTS_DIR", str(Path.home() / ".config" / "backtrader" / "accounts"))
)


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def discover_profiles() -> dict[str, dict[str, str]]:
    profiles: dict[str, dict[str, str]] = {}
    if not SCHEDULED_PROFILES_DIR.is_dir():
        return profiles
    for env_file in sorted(SCHEDULED_PROFILES_DIR.glob("*.env")):
        resolved = _read_env_file(env_file)
        profile = resolved.get("ROLE") or env_file.stem
        profiles[profile] = resolved
    return profiles


def resolve_account_credentials(profile_env: dict[str, str]) -> dict[str, str]:
    account_env_path = profile_env.get("ACCOUNT_ENV")
    if not account_env_path:
        raise ValueError("profile env has no ACCOUNT_ENV")
    path = Path(account_env_path)
    if not path.is_absolute():
        path = ACCOUNTS_DIR / path
    creds = _read_env_file(path)
    key = creds.get("ALPACA_API_KEY") or creds.get("BROKER_API_KEY")
    secret = creds.get("ALPACA_SECRET_KEY") or creds.get("BROKER_SECRET_KEY")
    if not key or not secret:
        raise ValueError(f"no Alpaca credentials found in {path}")
    return {"key": key, "secret": secret}


def poll_profile(repo: "wr.WatchtowerRepository", profile: str, profile_env: dict[str, str],
                  days: int) -> dict[str, Any]:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import GetOrdersRequest, GetPortfolioHistoryRequest
    from alpaca.trading.enums import QueryOrderStatus

    creds = resolve_account_credentials(profile_env)
    trading_mode = (profile_env.get("TRADING_MODE") or "paper").strip().lower()
    paper = trading_mode != "live"
    client = TradingClient(api_key=creds["key"], secret_key=creds["secret"], paper=paper)
    portfolio_key_id = wr.portfolio_key_id_from_api_key(creds["key"])

    after = datetime.now(timezone.utc) - timedelta(days=days)
    all_orders = []
    until = None
    while True:
        req = GetOrdersRequest(status=QueryOrderStatus.ALL, after=after, until=until, limit=500)
        batch = client.get_orders(filter=req)
        if not batch:
            break
        all_orders.extend(batch)
        if len(batch) < 500:
            break
        until = min(o.created_at for o in batch)

    orders_upserted = _upsert_orders(repo, all_orders, portfolio_key_id, profile)

    positions = client.get_all_positions()
    positions_upserted = _upsert_positions(repo, positions, portfolio_key_id, profile)

    return {
        "profile": profile, "portfolio_key_id": portfolio_key_id, "paper": paper,
        "orders_seen": len(all_orders), "orders_upserted": orders_upserted,
        "positions_upserted": positions_upserted,
    }


_ORDER_UPSERT_SQL = """
    INSERT INTO alpaca_order_cache (
        alpaca_order_id, window_open, client_order_id, symbol, side, signal_intent,
        order_type, status, position_intent, qty, filled_qty, filled_avg_price,
        limit_price, stop_price, submitted_at, created_at, updated_at, filled_at,
        canceled_at, failed_at, expired_at, source_account, raw_payload, last_synced_at,
        portfolio_key_id, chain_run_id
    ) VALUES (
        %(id)s, %(window_open)s, %(client_order_id)s, %(symbol)s, %(side)s, NULL,
        %(order_type)s, %(status)s, %(position_intent)s, %(qty)s, %(filled_qty)s, %(filled_avg_price)s,
        %(limit_price)s, %(stop_price)s, %(submitted_at)s, %(created_at)s, %(updated_at)s, %(filled_at)s,
        %(canceled_at)s, %(failed_at)s, %(expired_at)s, %(source_account)s, %(raw_payload)s::jsonb, NOW(),
        %(portfolio_key_id)s, NULL
    )
    ON CONFLICT (alpaca_order_id) DO UPDATE SET
        window_open = EXCLUDED.window_open,
        client_order_id = EXCLUDED.client_order_id,
        symbol = EXCLUDED.symbol,
        side = EXCLUDED.side,
        order_type = EXCLUDED.order_type,
        status = EXCLUDED.status,
        position_intent = EXCLUDED.position_intent,
        qty = EXCLUDED.qty,
        filled_qty = EXCLUDED.filled_qty,
        filled_avg_price = EXCLUDED.filled_avg_price,
        limit_price = EXCLUDED.limit_price,
        stop_price = EXCLUDED.stop_price,
        submitted_at = EXCLUDED.submitted_at,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        filled_at = EXCLUDED.filled_at,
        canceled_at = EXCLUDED.canceled_at,
        failed_at = EXCLUDED.failed_at,
        expired_at = EXCLUDED.expired_at,
        source_account = EXCLUDED.source_account,
        raw_payload = EXCLUDED.raw_payload,
        portfolio_key_id = EXCLUDED.portfolio_key_id,
        last_synced_at = NOW()
"""


def _local_trading_date(value: datetime | None) -> date | None:
    """Bucket an order timestamp into a calendar trading day. Orders for
    this strategy family are submitted/filled either after-hours (US
    Eastern evening, still UTC same calendar day in practice for this
    host) or around the next day's open -- using the UTC date of
    `submitted_at`/`created_at` is an acceptable, simple bucket for a daily
    (not intraday) reconciliation; exact session boundaries are not needed
    here, only "which trading day does this order belong to"."""
    if value is None:
        return None
    return value.astimezone(timezone.utc).date()


def _upsert_orders(repo: "wr.WatchtowerRepository", orders: list[Any], portfolio_key_id: str, profile: str) -> int:
    import watchtower_runtime as _wr

    count = 0
    with repo.connect() as conn:
        with conn.cursor() as cur:
            for order in orders:
                payload = _wr._serialize_alpaca_payload(order)
                order_id = str(payload.get("id") or "").strip()
                if not order_id:
                    continue
                created_at = _wr._as_utc_datetime(payload.get("created_at"))
                submitted_at = _wr._as_utc_datetime(payload.get("submitted_at")) or created_at
                window_open = _local_trading_date(submitted_at or created_at)
                qty = payload.get("qty")
                filled_qty = payload.get("filled_qty")
                filled_avg_price = payload.get("filled_avg_price")
                limit_price = payload.get("limit_price")
                stop_price = payload.get("stop_price")
                row = {
                    "id": order_id,
                    "window_open": window_open,
                    "client_order_id": payload.get("client_order_id"),
                    "symbol": payload.get("symbol"),
                    "side": str(payload.get("side") or "").lower() or None,
                    "order_type": payload.get("order_type") or payload.get("type"),
                    "status": str(payload.get("status") or "").lower() or None,
                    "position_intent": payload.get("position_intent"),
                    "qty": float(qty) if qty not in (None, "") else None,
                    "filled_qty": float(filled_qty) if filled_qty not in (None, "") else None,
                    "filled_avg_price": float(filled_avg_price) if filled_avg_price not in (None, "") else None,
                    "limit_price": float(limit_price) if limit_price not in (None, "") else None,
                    "stop_price": float(stop_price) if stop_price not in (None, "") else None,
                    "submitted_at": _wr.to_iso(payload.get("submitted_at")),
                    "created_at": _wr.to_iso(payload.get("created_at")),
                    "updated_at": _wr.to_iso(payload.get("updated_at")),
                    "filled_at": _wr.to_iso(payload.get("filled_at")),
                    "canceled_at": _wr.to_iso(payload.get("canceled_at")),
                    "failed_at": _wr.to_iso(payload.get("failed_at")),
                    "expired_at": _wr.to_iso(payload.get("expired_at")),
                    "source_account": profile,
                    "raw_payload": _wr.stable_json(payload),
                    "portfolio_key_id": portfolio_key_id,
                }
                cur.execute(_ORDER_UPSERT_SQL, row)
                count += 1
        conn.commit()
    return count


def _upsert_positions(repo: "wr.WatchtowerRepository", positions: list[Any], portfolio_key_id: str,
                       profile: str) -> int:
    import watchtower_runtime as _wr

    today = datetime.now(timezone.utc).date()
    count = 0
    with repo.connect() as conn:
        with conn.cursor() as cur:
            for p in positions:
                cur.execute(
                    """
                    INSERT INTO alpaca_position_cache (
                        window_open, symbol, side, qty, market_value, avg_entry_price,
                        current_price, unrealized_pl, source_account, raw_payload, portfolio_key_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (window_open, symbol) DO UPDATE SET
                        side = EXCLUDED.side, qty = EXCLUDED.qty, market_value = EXCLUDED.market_value,
                        avg_entry_price = EXCLUDED.avg_entry_price, current_price = EXCLUDED.current_price,
                        unrealized_pl = EXCLUDED.unrealized_pl, source_account = EXCLUDED.source_account,
                        raw_payload = EXCLUDED.raw_payload, portfolio_key_id = EXCLUDED.portfolio_key_id,
                        snapshot_at = NOW()
                    """,
                    (
                        today, p.symbol, p.side.value if p.side else None, float(p.qty),
                        float(p.market_value), float(p.avg_entry_price), float(p.current_price),
                        float(p.unrealized_pl), profile, _wr.stable_json(_wr._serialize_alpaca_payload(p)),
                        portfolio_key_id,
                    ),
                )
                count += 1
        conn.commit()
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default=None, help="Restrict to one profile (default: all discovered)")
    parser.add_argument("--days", type=int, default=30, help="How many days back to poll orders for")
    parser.add_argument("--db-dsn", default=None)
    args = parser.parse_args(argv)

    profiles = discover_profiles()
    if args.profile and args.profile not in profiles:
        print(f"unknown profile {args.profile!r}, discovered: {sorted(profiles)}", file=sys.stderr)
        return 1

    repo = wr.WatchtowerRepository(dsn=args.db_dsn)
    results = []
    for profile, profile_env in profiles.items():
        if args.profile and profile != args.profile:
            continue
        try:
            results.append(poll_profile(repo, profile, profile_env, args.days))
        except Exception as exc:  # noqa: BLE001 -- one profile's failure must not stop the others
            print(f"[{profile}] poll failed: {exc}", file=sys.stderr)
            results.append({"profile": profile, "error": str(exc)})

    import json
    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
