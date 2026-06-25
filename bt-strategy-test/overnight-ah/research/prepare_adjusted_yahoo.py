#!/usr/bin/env python3
"""Build an adjusted Yahoo daily dataset for OvernightAH studies.

The source Yahoo files are left untouched. Output files put adjusted OHLC into
the standard Open/High/Low/Close columns so existing Backtrader strategies can
read them through ``--provider yahoo_adj``. Raw OHLC columns are retained for
diagnostics.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE = ROOT / "config-common" / "data" / "d" / "yahoo"
DEFAULT_OUTPUT = ROOT / "config-common" / "data" / "d" / "yahoo_adj"
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"


def load_tickers(value: str | None) -> list[str]:
    if not value:
        return sorted(path.stem for path in DEFAULT_SOURCE.glob("*.csv"))
    if value.endswith(".json"):
        path = Path(value)
        if not path.is_absolute():
            candidates = [
                ROOT / value,
                ROOT / "config-common" / "tickers" / value,
                DEFAULT_TICKERS if value == DEFAULT_TICKERS.name else Path(value),
            ]
            path = next((candidate for candidate in candidates if candidate.exists()), path)
        return list(dict.fromkeys(json.loads(path.read_text())))
    return [ticker.strip() for ticker in value.split(",") if ticker.strip()]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    for col in df.columns:
        key = str(col).strip().lower().replace("_", " ")
        if key == "date":
            rename[col] = "Date"
        elif key == "open":
            rename[col] = "Open"
        elif key == "high":
            rename[col] = "High"
        elif key == "low":
            rename[col] = "Low"
        elif key == "close":
            rename[col] = "Close"
        elif key == "adj close":
            rename[col] = "Adj Close"
        elif key == "volume":
            rename[col] = "Volume"
    return df.rename(columns=rename)


def prepare_file(source_path: Path, output_path: Path) -> tuple[int, int]:
    df = pd.read_csv(source_path)
    if df.empty:
        raise ValueError("empty source csv")
    df = normalize_columns(df)
    required = {"Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"missing columns: {', '.join(sorted(missing))}")

    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    df = df.dropna(subset=["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
    df = df.sort_values("Date").drop_duplicates("Date", keep="last")

    close = df["Close"].replace(0, pd.NA)
    factor = df["Adj Close"] / close
    out = pd.DataFrame(
        {
            "Date": df["Date"].dt.strftime("%Y-%m-%d %H:%M:%S%z"),
            "Open": df["Open"] * factor,
            "High": df["High"] * factor,
            "Low": df["Low"] * factor,
            "Close": df["Adj Close"],
            "Volume": df["Volume"],
            "Raw Open": df["Open"],
            "Raw High": df["High"],
            "Raw Low": df["Low"],
            "Raw Close": df["Close"],
            "Raw Volume": df["Volume"],
            "Adj Open": df["Open"] * factor,
            "Adj High": df["High"] * factor,
            "Adj Low": df["Low"] * factor,
            "Adj Close": df["Adj Close"],
            "Adj Factor": factor,
            "Raw Dollar Volume": df["Close"] * df["Volume"],
        }
    )
    out = out.dropna(subset=["Open", "High", "Low", "Close"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    changed = int((factor.round(12) != 1.0).sum())
    return len(out), changed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare adjusted Yahoo daily OHLC CSVs")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--ticker", default=DEFAULT_TICKERS.name, help="JSON file, comma list, or omitted for all source CSVs")
    parser.add_argument("--fail-fast", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tickers = load_tickers(args.ticker)
    ok = failed = 0
    for ticker in tickers:
        source_path = args.source / f"{ticker}.csv"
        output_path = args.output / f"{ticker}.csv"
        if not source_path.exists():
            print(f"MISS {ticker}: {source_path}")
            failed += 1
            if args.fail_fast:
                return 1
            continue
        try:
            rows, changed = prepare_file(source_path, output_path)
            print(f"OK   {ticker}: rows={rows} adjusted_rows={changed} -> {output_path}")
            ok += 1
        except Exception as exc:
            print(f"FAIL {ticker}: {exc}")
            failed += 1
            if args.fail_fast:
                return 1
    print(f"done ok={ok} failed={failed} output={args.output}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
