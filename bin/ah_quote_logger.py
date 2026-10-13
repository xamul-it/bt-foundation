#!/usr/bin/env python3
"""Log Alpaca bid/ask quotes for AH spread monitoring."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.client import TradingClient


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "out/overnight_ah/quote_logs"
CSV_FIELDS = [
    "logged_at_utc",
    "logged_at_et",
    "feed",
    "symbol",
    "bid_price",
    "ask_price",
    "bid_size",
    "ask_size",
    "bid_exchange",
    "ask_exchange",
    "quote_ts_utc",
    "quote_ts_et",
    "stale_seconds",
    "mid",
    "spread",
    "spread_bps",
    "position_qty",
    "avg_entry_price",
    "mid_bps_vs_entry",
    "bid_bps_vs_entry",
    "ask_bps_vs_entry",
    "source",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--symbols", default="", help="Comma-separated symbols.")
    parser.add_argument("--symbols-file", type=Path, default=None, help="JSON/list/text file with symbols.")
    parser.add_argument("--positions", action="store_true", help="Use currently open Alpaca positions as symbols.")
    parser.add_argument("--feed", default=os.environ.get("ALPACA_DATA_FEED", "sip"))
    parser.add_argument("--interval-seconds", type=float, default=15.0)
    parser.add_argument("--duration-minutes", type=float, default=0.0, help="0 runs until interrupted.")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--once", action="store_true", help="Take one sample and exit.")
    return parser.parse_args()


def credentials() -> tuple[str, str]:
    api_key = os.environ.get("ALPACA_API_KEY") or os.environ.get("BROKER_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("BROKER_SECRET_KEY")
    if not api_key or not secret_key:
        raise SystemExit("Missing ALPACA_API_KEY/ALPACA_SECRET_KEY or BROKER_API_KEY/BROKER_SECRET_KEY")
    return api_key, secret_key


def data_feed(value: str) -> DataFeed:
    normalized = str(value or "sip").strip().lower()
    for feed in DataFeed:
        if feed.value == normalized:
            return feed
    raise SystemExit(f"Unsupported Alpaca feed: {value!r}")


def split_symbols(text: str) -> list[str]:
    return sorted({item.strip().upper() for item in text.split(",") if item.strip()})


def load_symbols_file(path: Path) -> list[str]:
    text = path.read_text().strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return sorted({line.strip().upper() for line in text.splitlines() if line.strip()})
    if isinstance(payload, list):
        return sorted({str(item).strip().upper() for item in payload if str(item).strip()})
    if isinstance(payload, dict):
        for key in ("symbols", "tickers", "assets"):
            value = payload.get(key)
            if isinstance(value, list):
                return sorted({str(item).strip().upper() for item in value if str(item).strip()})
        return sorted({str(key).strip().upper() for key in payload.keys() if str(key).strip()})
    return []


def position_details(client: TradingClient) -> dict[str, dict[str, float]]:
    positions = client.get_all_positions()
    details: dict[str, dict[str, float]] = {}
    for pos in positions:
        symbol = str(getattr(pos, "symbol", "")).strip().upper()
        if not symbol:
            continue
        qty = float(getattr(pos, "qty", 0.0) or 0.0)
        avg_entry_price = float(getattr(pos, "avg_entry_price", 0.0) or 0.0)
        details[symbol] = {"position_qty": qty, "avg_entry_price": avg_entry_price}
    return details


def quote_to_dict(quote: Any) -> dict[str, Any]:
    if hasattr(quote, "model_dump"):
        raw = quote.model_dump()
    elif hasattr(quote, "dict"):
        raw = quote.dict()
    else:
        raw = {name: getattr(quote, name, None) for name in dir(quote) if not name.startswith("_")}
    aliases = {
        "timestamp": ["timestamp", "t"],
        "bid_price": ["bid_price", "bp"],
        "ask_price": ["ask_price", "ap"],
        "bid_size": ["bid_size", "bs"],
        "ask_size": ["ask_size", "as"],
        "bid_exchange": ["bid_exchange", "bx"],
        "ask_exchange": ["ask_exchange", "ax"],
    }
    out: dict[str, Any] = {}
    for key, names in aliases.items():
        for name in names:
            if name in raw and raw[name] is not None:
                out[key] = raw[name]
                break
    return out


def extract_latest(response: Any, symbol: str) -> Any | None:
    data = getattr(response, "data", None)
    if isinstance(data, dict):
        return data.get(symbol) or data.get(symbol.upper())
    if isinstance(response, dict):
        return response.get(symbol) or response.get(symbol.upper())
    return None


def sample_quotes(
    client: StockHistoricalDataClient,
    symbols: list[str],
    feed: DataFeed,
    source: str,
    positions: dict[str, dict[str, float]] | None = None,
) -> list[dict[str, Any]]:
    if not symbols:
        return []
    now_utc = dt.datetime.now(dt.UTC)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    request = StockLatestQuoteRequest(symbol_or_symbols=symbols, feed=feed)
    response = client.get_stock_latest_quote(request)
    rows = []
    for symbol in symbols:
        quote = extract_latest(response, symbol)
        if quote is None:
            continue
        row = quote_to_dict(quote)
        ts = row.get("timestamp")
        quote_ts_utc = None
        quote_ts_et = None
        stale_seconds = None
        if ts is not None:
            quote_ts = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")) if isinstance(ts, str) else ts
            if quote_ts.tzinfo is None:
                quote_ts = quote_ts.replace(tzinfo=dt.UTC)
            quote_ts_utc = quote_ts.astimezone(dt.UTC)
            quote_ts_et = quote_ts_utc.astimezone(ZoneInfo("America/New_York"))
            stale_seconds = (now_utc - quote_ts_utc).total_seconds()
        bid = float(row.get("bid_price") or 0.0)
        ask = float(row.get("ask_price") or 0.0)
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 and ask >= bid else None
        spread = ask - bid if mid else None
        pos = (positions or {}).get(symbol, {})
        avg_entry_price = float(pos.get("avg_entry_price") or 0.0)
        rows.append(
            {
                "logged_at_utc": now_utc.isoformat(),
                "logged_at_et": now_et.isoformat(),
                "feed": feed.value,
                "symbol": symbol,
                "bid_price": bid or "",
                "ask_price": ask or "",
                "bid_size": row.get("bid_size", ""),
                "ask_size": row.get("ask_size", ""),
                "bid_exchange": row.get("bid_exchange", ""),
                "ask_exchange": row.get("ask_exchange", ""),
                "quote_ts_utc": quote_ts_utc.isoformat() if quote_ts_utc else "",
                "quote_ts_et": quote_ts_et.isoformat() if quote_ts_et else "",
                "stale_seconds": stale_seconds if stale_seconds is not None else "",
                "mid": mid if mid is not None else "",
                "spread": spread if spread is not None else "",
                "spread_bps": (spread / mid * 10000.0) if mid and spread is not None else "",
                "position_qty": pos.get("position_qty", ""),
                "avg_entry_price": avg_entry_price or "",
                "mid_bps_vs_entry": ((mid / avg_entry_price - 1.0) * 10000.0) if mid and avg_entry_price > 0 else "",
                "bid_bps_vs_entry": ((bid / avg_entry_price - 1.0) * 10000.0) if bid > 0 and avg_entry_price > 0 else "",
                "ask_bps_vs_entry": ((ask / avg_entry_price - 1.0) * 10000.0) if ask > 0 and avg_entry_price > 0 else "",
                "source": source,
            }
        )
    return rows


def append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def main() -> None:
    args = parse_args()
    api_key, secret_key = credentials()
    feed = data_feed(args.feed)
    data_client = StockHistoricalDataClient(api_key, secret_key)
    data_client._session.verify = False
    trading_client = TradingClient(api_key, secret_key, paper=str(os.environ.get("ALPACA_PAPER", "true")).lower() != "false")

    fixed_symbols = split_symbols(args.symbols)
    if args.symbols_file:
        fixed_symbols = sorted(set(fixed_symbols).union(load_symbols_file(args.symbols_file)))
    if not fixed_symbols and not args.positions:
        raise SystemExit("Provide --symbols/--symbols-file or --positions")

    out_dir = args.out_dir if args.out_dir.is_absolute() else ROOT / args.out_dir
    out_path = out_dir / f"ah_quotes_{dt.datetime.now(dt.UTC).date().isoformat()}.csv"
    deadline = None if args.duration_minutes <= 0 else time.monotonic() + args.duration_minutes * 60.0

    while True:
        symbols = list(fixed_symbols)
        source = "symbols"
        positions: dict[str, dict[str, float]] = {}
        if args.positions:
            positions = position_details(trading_client)
            pos_symbols = sorted(positions)
            symbols = sorted(set(symbols).union(pos_symbols))
            source = "symbols+positions" if fixed_symbols else "positions"
        rows = sample_quotes(data_client, symbols, feed, source, positions=positions)
        append_rows(out_path, rows)
        print(f"{dt.datetime.now().isoformat(timespec='seconds')} symbols={len(symbols)} rows={len(rows)} out={out_path}")
        if args.once:
            break
        if deadline is not None and time.monotonic() >= deadline:
            break
        time.sleep(max(1.0, args.interval_seconds))


if __name__ == "__main__":
    main()
