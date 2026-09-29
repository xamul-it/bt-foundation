#!/usr/bin/env python3
"""Incrementally maintain the shared Alpaca trading-calendar cache.

This program deliberately has no dependency on Yahoo symbols or market-data
downloads.  With an empty cache it requests Alpaca's complete published
calendar; later runs request only sessions after the last cached session.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = ROOT / "config-common" / "cache" / "alpaca_calendar_cache.json"


def load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "months": {}}
    raw = json.loads(path.read_text(encoding="utf-8"))
    months = raw.get("months", {}) if isinstance(raw, dict) else {}
    return {"version": 1, "months": months if isinstance(months, dict) else {}}


def last_cached_session(cache: dict[str, Any]) -> dt.date | None:
    latest = None
    for month in cache["months"].values():
        days = month.get("days", {}) if isinstance(month, dict) else {}
        for value in days:
            try:
                day = dt.date.fromisoformat(value)
            except (TypeError, ValueError):
                continue
            latest = max(latest, day) if latest else day
    return latest


def merge_calendar(cache: dict[str, Any], entries: list[Any], loaded_at: str) -> tuple[int, int]:
    """Merge calendar entries without deleting known sessions.

    Returns ``(added, changed)``.  The cache is organized by month to retain
    compatibility with the existing strategy and Watchtower readers.
    """
    added = changed = 0
    months = cache.setdefault("months", {})
    touched = set()
    for item in entries:
        day = item.date if isinstance(item.date, dt.date) else dt.date.fromisoformat(str(item.date))
        month_key = f"{day.year:04d}-{day.month:02d}"
        month = months.setdefault(month_key, {"days": {}})
        days = month.setdefault("days", {})
        row = {
            "open": item.open.isoformat() if hasattr(item.open, "isoformat") else str(item.open),
            "close": item.close.isoformat() if hasattr(item.close, "isoformat") else str(item.close),
        }
        previous = days.get(day.isoformat())
        if previous is None:
            added += 1
        elif previous != row:
            changed += 1
        days[day.isoformat()] = row
        touched.add(month_key)
    for month_key in touched:
        months[month_key]["loaded_at_utc"] = loaded_at
    return added, changed


def write_cache(path: Path, cache: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(cache, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--full", action="store_true", help="reload all calendar sessions Alpaca currently publishes")
    parser.add_argument("--dry-run", action="store_true", help="print the request range without calling Alpaca")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cache = load_cache(args.cache)
    last_day = last_cached_session(cache)
    start = None if args.full or last_day is None else last_day + dt.timedelta(days=1)
    if args.dry_run:
        print(f"cache={args.cache.resolve()} mode={'full' if start is None else 'incremental'} start={start or 'all-published-sessions'}")
        return 0

    key = os.environ.get("ALPACA_API_KEY") or os.environ.get("BROKER_API_KEY")
    secret = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("BROKER_SECRET_KEY")
    if not key or not secret:
        print("Missing Alpaca credentials (ALPACA_API_KEY/ALPACA_SECRET_KEY)", file=sys.stderr)
        return 78

    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import GetCalendarRequest

    client = TradingClient(key, secret)
    if os.environ.get("DISABLE_SSL_VERIFY", "").lower() in {"1", "true", "yes"}:
        client._session.verify = False
    request = GetCalendarRequest(start=start) if start else None
    entries = client.get_calendar(request)
    loaded_at = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    added, changed = merge_calendar(cache, entries, loaded_at)
    if added or changed:
        write_cache(args.cache, cache)
    print(
        f"calendar cache: path={args.cache.resolve()} mode={'full' if start is None else 'incremental'} "
        f"start={start or 'all-published-sessions'} fetched={len(entries)} added={added} changed={changed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
