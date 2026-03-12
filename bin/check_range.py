#!/usr/bin/env python3
"""Conta e riepiloga le righe di un CSV simbolo in un intervallo date."""

import argparse
import csv
import datetime as dt
from datetime import timezone
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check rows in date range for a symbol in config/data/m/alpaca."
    )
    parser.add_argument(
        "symbol",
        help="Ticker symbol (e.g., BKR).",
    )
    parser.add_argument(
        "--start",
        default="2025-01-01",
        help="Start date (YYYY-MM-DD). Default: 2025-01-01",
    )
    parser.add_argument(
        "--end",
        default="2026-02-03",
        help="End date (YYYY-MM-DD). Default: 2026-02-03",
    )
    parser.add_argument(
        "--data-dir",
        default="config/data/m/alpaca",
        help="Base data dir. Default: config/data/m/alpaca",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    start = dt.datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = dt.datetime.fromisoformat(args.end).replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )
    path = Path(args.data_dir) / f"{args.symbol}.csv"
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    cnt = 0
    first = None
    last = None
    with path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            ts = row.get("timestamp")
            if not ts:
                continue
            try:
                dt_value = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                continue
            if dt_value.tzinfo is None:
                dt_value = dt_value.replace(tzinfo=timezone.utc)
            if start <= dt_value <= end:
                cnt += 1
                if first is None:
                    first = dt_value
                last = dt_value

    print(f"file: {path}")
    print(f"range: {start} -> {end}")
    print(f"rows in range: {cnt}")
    print(f"first: {first}")
    print(f"last: {last}")


if __name__ == "__main__":
    main()
