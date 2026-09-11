#!/usr/bin/env python3
"""Verify that yahoo_adj close-to-next-open returns include cash dividends."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "out/overnight_earnings_20260910"


def main() -> None:
    dividends = pd.read_csv(OUT / "dividends.csv")
    checks = []
    for symbol, events in dividends.groupby("symbol"):
        path = ROOT / "config-common/data/d/yahoo_adj" / f"{symbol}.csv"
        if not path.exists():
            continue
        bars = pd.read_csv(path)
        bars["day"] = pd.to_datetime(bars["Date"], utc=True).dt.date.astype(str)
        bars = bars.set_index("day")
        for _, event in events.iterrows():
            day = event["ex_date"]
            if day not in bars.index:
                continue
            pos = bars.index.get_loc(day)
            if not isinstance(pos, int) or pos == 0:
                continue
            prev = bars.iloc[pos - 1]
            cur = bars.iloc[pos]
            if min(float(prev["Raw Close"]), float(cur["Raw Open"]), float(prev["Close"]), float(cur["Open"])) <= 0:
                continue
            cash_return = (float(cur["Raw Open"]) + float(event["cash_dividend"])) / float(prev["Raw Close"]) - 1
            adjusted_return = float(cur["Open"]) / float(prev["Close"]) - 1
            checks.append({
                "symbol": symbol,
                "ex_date": day,
                "cash_dividend": event["cash_dividend"],
                "raw_price_return": float(cur["Raw Open"]) / float(prev["Raw Close"]) - 1,
                "cash_total_return": cash_return,
                "adjusted_return": adjusted_return,
                "abs_error": abs(cash_return - adjusted_return),
            })
    frame = pd.DataFrame(checks)
    frame.to_csv(OUT / "dividend_return_audit.csv", index=False)
    summary = {
        "checks": len(frame),
        "median_abs_error": float(frame.abs_error.median()),
        "p95_abs_error": float(frame.abs_error.quantile(.95)),
        "max_abs_error": float(frame.abs_error.max()),
        "within_1bp_share": float((frame.abs_error <= .0001).mean()),
    }
    (OUT / "dividend_return_audit.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
