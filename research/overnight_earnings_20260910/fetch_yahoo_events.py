#!/usr/bin/env python3
"""Download historical Yahoo earnings events for the OvernightAH universe."""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[2]


def fetch_symbol(symbol: str, limit: int) -> tuple[str, list[dict], str | None]:
    try:
        frame = yf.Ticker(symbol).get_earnings_dates(limit=limit)
        if frame is None or frame.empty:
            return symbol, [], "empty"
        rows: list[dict] = []
        for timestamp, row in frame.iterrows():
            ts = pd.Timestamp(timestamp)
            if ts.tzinfo is None:
                ts = ts.tz_localize("America/New_York")
            else:
                ts = ts.tz_convert("America/New_York")
            reported = row.get("Reported EPS")
            # Future estimates are not realized historical events.
            if pd.isna(reported):
                continue
            hour = ts.hour + ts.minute / 60
            session = "before_open" if hour < 9.5 else "after_close" if hour >= 16 else "during_session"
            rows.append({
                "symbol": symbol,
                "earnings_datetime_et": ts.isoformat(),
                "earnings_date": ts.date().isoformat(),
                "session": session,
                "eps_estimate": None if pd.isna(row.get("EPS Estimate")) else float(row.get("EPS Estimate")),
                "reported_eps": float(reported),
                "surprise_pct": None if pd.isna(row.get("Surprise(%)")) else float(row.get("Surprise(%)")),
                "source": "yfinance.get_earnings_dates",
            })
        return symbol, rows, None
    except Exception as exc:  # preserve per-symbol failures in the manifest
        return symbol, [], f"{type(exc).__name__}: {exc}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker-file", default="config-common/tickers/yahoo_adj_research_universe_hedge.json")
    parser.add_argument("--out-dir", default="out/overnight_earnings_20260910")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    ticker_path = ROOT / args.ticker_file
    symbols = json.loads(ticker_path.read_text(encoding="utf-8"))
    out_dir = ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    errors: dict[str, str] = {}
    started = time.time()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(fetch_symbol, symbol, args.limit): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol, rows, error = future.result()
            all_rows.extend(rows)
            if error:
                errors[symbol] = error
            print(f"{symbol}: {len(rows)} events" + (f" ({error})" if error else ""), flush=True)

    frame = pd.DataFrame(all_rows)
    if not frame.empty:
        frame = frame.sort_values(["earnings_datetime_et", "symbol"]).drop_duplicates(
            ["symbol", "earnings_datetime_et"], keep="last"
        )
    frame.to_csv(out_dir / "earnings_events.csv", index=False)
    manifest = {
        "source": "yfinance.get_earnings_dates",
        "ticker_file": str(ticker_path.relative_to(ROOT)),
        "symbols_requested": len(symbols),
        "symbols_with_events": int(frame["symbol"].nunique()) if not frame.empty else 0,
        "events": len(frame),
        "min_datetime_et": frame["earnings_datetime_et"].min() if not frame.empty else None,
        "max_datetime_et": frame["earnings_datetime_et"].max() if not frame.empty else None,
        "errors": errors,
        "elapsed_seconds": round(time.time() - started, 2),
        "limitations": [
            "Yahoo is not a point-in-time archive of previously announced dates.",
            "Only rows with Reported EPS are retained; future estimates are excluded.",
            f"At most {args.limit} rows were requested per symbol.",
        ],
    }
    (out_dir / "earnings_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
