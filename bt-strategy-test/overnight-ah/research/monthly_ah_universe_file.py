#!/usr/bin/env python3
"""
Build an ex-ante monthly OvernightAH universe file.

For each target month M, the script looks at the previous N full calendar
months, keeps symbols whose window classification is AH and whose monthly
classification is AH in every required month, then orders them by AH return
over the lookback window.

Output format:
    year;month;symbols
    2026;07;AMD,MU,NVDA
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "NASDAQ_100_US.json"
DEFAULT_DATA = ROOT / "data" / "d" / "yahoo"
DEFAULT_BOCSOO = ROOT / "bt-strategy-test" / "BoCSoO" / "out" / "decompose_results.json"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "monthly_ah_universe.csv"


def load_tickers(path: Path) -> list[str]:
    with open(path, encoding="utf-8") as f:
        return [str(t).strip() for t in json.load(f) if str(t).strip() and str(t).strip() != "SPY"]


def find_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lookup = {str(c).strip().lower().replace(" ", "_"): c for c in df.columns}
    for name in names:
        key = name.strip().lower().replace(" ", "_")
        if key in lookup:
            return lookup[key]
    return None


def load_symbol(data_dir: Path, ticker: str) -> pd.DataFrame:
    path = data_dir / f"{ticker}.csv"
    if not path.exists():
        return pd.DataFrame()

    raw = pd.read_csv(path)
    date_col = find_col(raw, ["date", "datetime", "timestamp"]) or raw.columns[0]
    raw[date_col] = pd.to_datetime(raw[date_col], utc=True, errors="coerce")
    raw = raw.dropna(subset=[date_col]).sort_values(date_col)
    raw["date"] = raw[date_col].dt.tz_localize(None).dt.normalize()

    rename = {}
    for col in raw.columns:
        key = str(col).strip().lower().replace(" ", "_")
        if key in {"open", "close", "adj_close", "adjclose", "volume"}:
            rename[col] = "adjclose" if key in {"adj_close", "adjclose"} else key
    raw = raw.rename(columns=rename)

    required = {"open", "close", "adjclose"}
    if not required.issubset(raw.columns):
        return pd.DataFrame()

    for col in required:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=list(required))
    raw = raw[(raw["open"] > 0) & (raw["close"] > 0) & (raw["adjclose"] > 0)].copy()
    if len(raw) < 40:
        return pd.DataFrame()

    raw["ticker"] = ticker
    raw["r_total"] = np.log(raw["adjclose"] / raw["adjclose"].shift(1))
    raw["r_rth"] = np.log(raw["close"] / raw["open"])
    raw["r_ah"] = raw["r_total"] - raw["r_rth"]
    raw["month"] = raw["date"].dt.to_period("M").dt.to_timestamp("M")
    return raw.dropna(subset=["r_total", "r_rth", "r_ah"])


def load_bocsoo_stability(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {
        ticker
        for ticker, value in data.get("stability", {}).items()
        if bool(value.get("stable", False))
    }


def classify(ah_log: float, rth_log: float, threshold: float) -> tuple[str, float]:
    total_log = ah_log + rth_log
    if abs(total_log) < 1e-9:
        return "Mixed", 50.0
    ah_pct = ah_log / total_log * 100.0
    if ah_pct > threshold * 100.0:
        return "AH", ah_pct
    if (rth_log / total_log * 100.0) > threshold * 100.0:
        return "RTH", ah_pct
    return "Mixed", ah_pct


def window_score(
    df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    threshold: float,
    min_days: int,
    min_month_days: int,
    min_stable_months: int,
    stable_symbols: set[str],
    stability_mode: str,
) -> dict | None:
    sample = df[(df["date"] >= start) & (df["date"] <= end)]
    if len(sample) < min_days:
        return None

    ah_log = float(sample["r_ah"].sum())
    rth_log = float(sample["r_rth"].sum())
    classification, ah_pct = classify(ah_log, rth_log, threshold)
    if classification != "AH":
        return None
    ah_return_pct = (np.expm1(ah_log) * 100.0)
    if ah_return_pct <= 0:
        return None

    ticker = sample["ticker"].iloc[0]
    if stability_mode == "bocsoo":
        stable_months = np.nan
        months_seen = np.nan
        if ticker not in stable_symbols:
            return None
    else:
        month_classes = []
        for _, month_df in sample.groupby("month"):
            if len(month_df) < min_month_days:
                continue
            month_class, _ = classify(float(month_df["r_ah"].sum()), float(month_df["r_rth"].sum()), threshold)
            month_classes.append(month_class)

        stable_months = sum(1 for value in month_classes if value == "AH")
        months_seen = len(month_classes)
        stable = months_seen >= min_stable_months and stable_months >= min_stable_months
        if not stable:
            return None

    return {
        "ticker": ticker,
        "ah_return_pct": ah_return_pct,
        "ah_pct": ah_pct,
        "days": len(sample),
        "stable_months": stable_months,
        "months_seen": months_seen,
    }


def build_universe(
    panel: pd.DataFrame,
    lookback_months: int,
    threshold: float,
    min_days: int,
    min_month_days: int,
    min_stable_months: int,
    stable_symbols: set[str],
    stability_mode: str,
    top: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    months = sorted(panel["month"].dropna().unique())
    rows = []
    detail_rows = []

    for target_month in months:
        target_month = pd.Timestamp(target_month)
        window_end = target_month - pd.offsets.MonthEnd(1)
        window_start = window_end - pd.DateOffset(months=lookback_months - 1) + pd.offsets.MonthBegin(1)
        if window_start < pd.Timestamp(months[0]):
            continue

        scored = []
        for _, group in panel.groupby("ticker", sort=False):
            score = window_score(
                group,
                start=window_start,
                end=window_end,
                threshold=threshold,
                min_days=min_days,
                min_month_days=min_month_days,
                min_stable_months=min_stable_months,
                stable_symbols=stable_symbols,
                stability_mode=stability_mode,
            )
            if score is not None:
                scored.append(score)

        if not scored:
            continue

        ranked = pd.DataFrame(scored).sort_values(
            ["ah_return_pct", "ah_pct", "days", "ticker"],
            ascending=[False, False, False, True],
        )
        if top is not None and top > 0:
            ranked = ranked.head(top)

        symbols = ranked["ticker"].tolist()
        rows.append(
            {
                "year": target_month.year,
                "month": target_month.month,
                "symbols": ",".join(symbols),
            }
        )
        detail = ranked.copy()
        detail.insert(0, "rank", range(1, len(detail) + 1))
        detail.insert(0, "month", target_month)
        detail.insert(1, "window_start", window_start)
        detail.insert(2, "window_end", window_end)
        detail_rows.append(detail)

    return pd.DataFrame(rows), pd.concat(detail_rows, ignore_index=True) if detail_rows else pd.DataFrame()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build monthly AH universe file for OvernightAH")
    parser.add_argument("--ticker-file", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--bocsoo-file", type=Path, default=DEFAULT_BOCSOO)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--detail-out", type=Path, default=None)
    parser.add_argument("--lookback-months", type=int, default=6)
    parser.add_argument("--threshold", type=float, default=0.60)
    parser.add_argument("--min-days", type=int, default=60)
    parser.add_argument("--min-month-days", type=int, default=5)
    parser.add_argument("--stability-mode", choices=["bocsoo", "monthly"], default="bocsoo")
    parser.add_argument("--min-stable-months", type=int, default=4)
    parser.add_argument("--top", type=int, default=0, help="0 keeps all passing symbols")
    args = parser.parse_args()

    tickers = load_tickers(args.ticker_file)
    frames = []
    for ticker in tickers:
        df = load_symbol(args.data_dir, ticker)
        if df.empty:
            print(f"[warn] missing/unusable {ticker}")
            continue
        frames.append(df)
    if not frames:
        raise SystemExit("No usable data")

    panel = pd.concat(frames, ignore_index=True).sort_values(["date", "ticker"])
    stable_symbols = load_bocsoo_stability(args.bocsoo_file)
    universe, detail = build_universe(
        panel,
        lookback_months=args.lookback_months,
        threshold=args.threshold,
        min_days=args.min_days,
        min_month_days=args.min_month_days,
        min_stable_months=args.min_stable_months,
        stable_symbols=stable_symbols,
        stability_mode=args.stability_mode,
        top=args.top if args.top > 0 else None,
    )
    if universe.empty:
        raise SystemExit("No monthly universe rows generated")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    universe.to_csv(args.out, sep=";", index=False)

    detail_out = args.detail_out
    if detail_out is None:
        detail_out = args.out.with_name(args.out.stem + "_detail.csv")
    detail.to_csv(detail_out, index=False)

    latest = universe.iloc[-1]
    print(f"Rows: {len(universe)}")
    print(f"Latest: {latest['year']}-{int(latest['month']):02d} {latest['symbols']}")
    print("Outputs:")
    print(f"  {args.out}")
    print(f"  {detail_out}")


if __name__ == "__main__":
    main()
