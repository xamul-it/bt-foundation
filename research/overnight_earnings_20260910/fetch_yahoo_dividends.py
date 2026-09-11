#!/usr/bin/env python3
"""Download dividend events used to audit Yahoo-adjusted overnight returns."""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[2]


def fetch(symbol: str) -> tuple[list[dict], str | None]:
    try:
        series = yf.Ticker(symbol).get_dividends(period="max")
        rows = []
        for timestamp, amount in series.items():
            if not amount:
                continue
            ts = pd.Timestamp(timestamp)
            if ts.tzinfo is None:
                ts = ts.tz_localize("America/New_York")
            else:
                ts = ts.tz_convert("America/New_York")
            rows.append({"symbol": symbol, "ex_date": ts.date().isoformat(), "cash_dividend": float(amount)})
        return rows, None
    except Exception as exc:
        return [], f"{type(exc).__name__}: {exc}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker-file", default="config-common/tickers/yahoo_adj_research_universe_hedge.json")
    parser.add_argument("--out-dir", default="out/overnight_earnings_20260910")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    symbols = json.loads((ROOT / args.ticker_file).read_text(encoding="utf-8"))
    rows, errors = [], {}
    started = time.time()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(fetch, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            values, error = future.result()
            rows.extend(values)
            if error:
                errors[symbol] = error
            print(f"{symbol}: {len(values)} dividends" + (f" ({error})" if error else ""), flush=True)
    frame = pd.DataFrame(rows).sort_values(["ex_date", "symbol"]) if rows else pd.DataFrame(
        columns=["symbol", "ex_date", "cash_dividend"]
    )
    out = ROOT / args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out / "dividends.csv", index=False)
    manifest = {
        "source": "yfinance.get_dividends",
        "events": len(frame),
        "symbols_with_dividends": int(frame.symbol.nunique()) if not frame.empty else 0,
        "errors": errors,
        "elapsed_seconds": round(time.time() - started, 2),
    }
    (out / "dividends_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
