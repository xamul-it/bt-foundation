#!/usr/bin/env python3
"""Expose missed or abandoned scheduled entry runs in Watchtower."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
sys.path.insert(0, str(BT_CORE))
import watchtower_runtime as wr  # noqa: E402


def is_trading_session(day: date, calendar_path: Path) -> bool:
    payload = json.loads(calendar_path.read_text(encoding="utf-8"))
    month = payload.get("months", {}).get(day.strftime("%Y-%m"), {})
    return day.isoformat() in month.get("days", {})


def discover_profiles() -> list[str]:
    """Use the same local profile registry as the scheduled runners."""
    directory = Path.home() / ".config" / "backtrader" / "scheduled"
    return sorted(path.stem for path in directory.glob("*.env")) if directory.is_dir() else []


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profiles", nargs="*", help="Optional profile filter (default: discover all local profiles)")
    parser.add_argument("--trading-date", default=None, help="Check exactly one date (legacy/manual mode)")
    parser.add_argument("--lookback-days", type=int, default=1,
                        help="Check this many prior calendar days; never infer a miss for today")
    parser.add_argument("--running-timeout-minutes", type=int, default=120)
    parser.add_argument("--calendar", default=str(Path(__file__).resolve().parent.parent / "config-common/cache/alpaca_calendar_cache.json"))
    args = parser.parse_args(argv)
    profiles = args.profiles or discover_profiles()
    if not profiles:
        print(json.dumps({"error": "no scheduled profiles discovered"}))
        return 0
    repo = wr.WatchtowerRepository()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=args.running_timeout_minutes)
    today = date.today()
    dates = [date.fromisoformat(args.trading_date)] if args.trading_date else [
        today - timedelta(days=offset) for offset in range(args.lookback_days + 1)
    ]
    changes = []
    checked = []
    for trading_date in sorted(set(dates)):
        if not is_trading_session(trading_date, Path(args.calendar)):
            continue
        checked.append(trading_date.isoformat())
        changes.extend(repo.watchdog_scheduled_entry_decisions(
            profiles, trading_date, cutoff,
            materialize_missing=(trading_date < today or args.trading_date is not None),
        ))
    print(json.dumps({"trading_dates": checked, "changes": changes}, default=str))
    # A detected missed run is a data fact, not a failure of this watchdog.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
