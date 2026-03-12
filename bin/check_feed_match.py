#!/usr/bin/env python3
"""Confronta feed locale minuto con download Alpaca per una giornata/simbolo."""

import argparse
import csv
import datetime as dt
import os
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import load_tickers as lt
from alpaca.data.timeframe import TimeFrame


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare existing minute CSV with Alpaca download for a single day."
    )
    parser.add_argument("symbol", help="Ticker symbol (e.g., AAPL).")
    parser.add_argument(
        "--date",
        default=None,
        help="Date to check (YYYY-MM-DD). Default: last date in existing feed.",
    )
    parser.add_argument(
        "--feed",
        required=True,
        choices=["sip", "iex"],
        help="Alpaca feed to use for download (sip or iex).",
    )
    parser.add_argument(
        "--data-dir",
        default="config/data/m/alpaca",
        help="Existing data directory. Default: config/data/m/alpaca",
    )
    return parser.parse_args()


def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def main():
    args = parse_args()
    symbol = args.symbol
    check_date = None

    api_key = os.environ.get("ALPACA_API_KEY") or os.environ.get("BROKER_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("BROKER_SECRET_KEY")
    if not api_key or not secret_key:
        raise SystemExit("Missing ALPACA_API_KEY/ALPACA_SECRET_KEY")

    client = lt.StockHistoricalDataClient(api_key, secret_key)
    client._session.verify = False

    existing_path = Path(args.data_dir) / f"{symbol}.csv"
    if not existing_path.exists():
        raise SystemExit(f"Existing file not found: {existing_path}")

    with existing_path.open(newline="") as f:
        r = csv.DictReader(f)
        existing_all = [row for row in r]

    if args.date:
        check_date = dt.date.fromisoformat(args.date)
    else:
        # Use last available date in existing feed
        dates = []
        for row in existing_all:
            ts = row.get("timestamp")
            if not ts:
                continue
            try:
                d = dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
            except ValueError:
                continue
            dates.append(d)
        if not dates:
            raise SystemExit(f"No timestamps found in {existing_path}")
        check_date = max(dates)

    existing_rows = [
        row for row in existing_all if row.get("timestamp", "").startswith(check_date.isoformat())
    ]

    with tempfile.TemporaryDirectory() as tmp:
        start_dt = dt.datetime.combine(check_date, dt.time.min)
        end_dt = dt.datetime.combine(check_date, dt.time.max)
        lt.asyncio.run(
            lt.fetch_and_save_all(
                [symbol],
                TimeFrame.Minute,
                tmp,
                start_date=start_dt,
                end_date=end_dt,
                client=client,
                feed=args.feed,
            )
        )
        downloaded_path = Path(tmp) / f"{symbol}.csv"
        if not downloaded_path.exists():
            raise SystemExit("No data downloaded from Alpaca")
        with downloaded_path.open(newline="") as f:
            r = csv.DictReader(f)
            downloaded_rows = [row for row in r]

    downloaded_map = {r["timestamp"]: r for r in downloaded_rows}
    existing_map = {r["timestamp"]: r for r in existing_rows}
    common = set(downloaded_map).intersection(existing_map)

    fields = ["open", "high", "low", "close", "volume", "trade_count", "vwap"]
    mismatch = 0
    checked = 0
    for ts in sorted(common):
        sr = downloaded_map[ts]
        er = existing_map[ts]
        checked += 1
        for field in fields:
            if to_float(sr.get(field)) != to_float(er.get(field)):
                mismatch += 1
                break

    print(f"symbol: {symbol}")
    print(f"date: {check_date.isoformat()}")
    print(f"feed: {args.feed}")
    print(f"downloaded rows: {len(downloaded_rows)}")
    print(f"existing rows: {len(existing_rows)}")
    print(f"common timestamps: {len(common)}")
    print(f"rows with any mismatch: {mismatch} of {checked}")


if __name__ == "__main__":
    main()
