#!/usr/bin/env python3
"""Analyze first 30 RTH minute closes relative to the RTH opening price."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


NY_TZ = "America/New_York"
OPEN_TIME = "09:30"
ROOT = Path(__file__).resolve().parents[3]
RESEARCH_OUT = ROOT / "bt-strategy-test" / "overnight-ah" / "research" / "out"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute close/open statistics for the first 30 RTH minutes."
    )
    parser.add_argument(
        "--tickers",
        default="config-common/tickers/ah_rth_stable_top10.json",
        help="JSON ticker list.",
    )
    parser.add_argument(
        "--data-dir",
        default="config-common/data/m/alpaca/sip",
        help="Directory with Alpaca minute CSV files.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(RESEARCH_OUT / "opening_30m"),
        help="Output directory.",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=30,
        help="Number of opening minutes to analyze.",
    )
    parser.add_argument("--fromdate", help="Inclusive trading date filter, YYYY-MM-DD.")
    parser.add_argument("--todate", help="Inclusive trading date filter, YYYY-MM-DD.")
    return parser.parse_args()


def load_tickers(path: Path) -> list[str]:
    with path.open() as handle:
        tickers = json.load(handle)
    return [str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()]


def load_symbol_observations(symbol: str, csv_path: Path, minutes: int) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if df.empty:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp")
    df["ny_timestamp"] = df["timestamp"].dt.tz_convert(NY_TZ)
    df["trading_date"] = df["ny_timestamp"].dt.date
    df["ny_time"] = df["ny_timestamp"].dt.strftime("%H:%M")

    rth_open = df[df["ny_time"] == OPEN_TIME][
        ["trading_date", "open"]
    ].rename(columns={"open": "open_price"})
    rth_open = rth_open.drop_duplicates("trading_date", keep="first")

    if rth_open.empty:
        return pd.DataFrame()

    rth = df[
        (df["ny_timestamp"].dt.hour == 9) & (df["ny_timestamp"].dt.minute >= 30)
        | (df["ny_timestamp"].dt.hour == 10)
        | (df["ny_timestamp"].dt.hour == 11)
        | (df["ny_timestamp"].dt.hour == 12)
        | (df["ny_timestamp"].dt.hour == 13)
        | (df["ny_timestamp"].dt.hour == 14)
        | (df["ny_timestamp"].dt.hour == 15)
    ].copy()
    rth = rth.merge(rth_open, on="trading_date", how="inner")
    rth = rth[rth["open_price"] > 0]
    if rth.empty:
        return pd.DataFrame()

    open_dt = pd.to_datetime(
        rth["trading_date"].astype(str) + " " + OPEN_TIME
    ).dt.tz_localize(NY_TZ)
    rth["minute"] = ((rth["ny_timestamp"] - open_dt).dt.total_seconds() // 60).astype(int) + 1
    rth = rth[(rth["minute"] >= 1) & (rth["minute"] <= minutes)]

    out = rth[
        [
            "trading_date",
            "minute",
            "timestamp",
            "ny_timestamp",
            "open_price",
            "close",
        ]
    ].copy()
    out.insert(0, "symbol", symbol)
    out = out.rename(columns={"close": "close_price"})
    out["close_open_ratio"] = out["close_price"] / out["open_price"]
    out["return"] = out["close_open_ratio"] - 1.0
    out["return_pct"] = out["return"] * 100.0
    return out


def summarize(group_cols: list[str], observations: pd.DataFrame) -> pd.DataFrame:
    grouped = observations.groupby(group_cols, dropna=False)
    summary = grouped.agg(
        n_obs=("return", "count"),
        mean_ratio=("close_open_ratio", "mean"),
        variance_ratio=("close_open_ratio", "var"),
        mean_return=("return", "mean"),
        variance_return=("return", "var"),
        mean_return_pct=("return_pct", "mean"),
        variance_return_pct2=("return_pct", "var"),
        prob_higher=("return", lambda s: (s > 0).mean()),
        prob_lower=("return", lambda s: (s < 0).mean()),
        prob_equal=("return", lambda s: (s == 0).mean()),
    ).reset_index()

    summary["sd_return_pct"] = summary["variance_return_pct2"] ** 0.5
    summary["se_return_pct"] = summary["sd_return_pct"] / (summary["n_obs"] ** 0.5)
    summary["t_stat_mean_return"] = summary["mean_return_pct"] / summary["se_return_pct"]

    numeric_cols = summary.select_dtypes("number").columns
    summary[numeric_cols] = summary[numeric_cols].round(8)
    return summary


def write_markdown_report(
    out_path: Path,
    tickers: list[str],
    missing: list[str],
    observations: pd.DataFrame,
    overall: pd.DataFrame,
    by_symbol: pd.DataFrame,
) -> None:
    lines: list[str] = []
    lines.append("# Opening 30m open-relative analysis")
    lines.append("")
    lines.append(f"Ticker richiesti: {', '.join(tickers)}")
    lines.append(
        "Dati minute disponibili: "
        + ", ".join(sorted(observations["symbol"].unique()))
        if not observations.empty
        else "Dati minute disponibili: nessuno"
    )
    lines.append(
        "Ticker senza CSV minute locale: " + (", ".join(missing) if missing else "nessuno")
    )
    if not observations.empty:
        start = observations["trading_date"].min()
        end = observations["trading_date"].max()
        days = observations[["symbol", "trading_date"]].drop_duplicates().shape[0]
        lines.append(f"Periodo osservato: {start} - {end}; symbol-day: {days}")
    lines.append("")
    lines.append("Metriche: `return = close/open_09:30 - 1`; varianza campionaria (`ddof=1`).")
    lines.append("")
    lines.append("## Aggregato per minuto")
    lines.append("")
    if overall.empty:
        lines.append("Nessun dato disponibile.")
    else:
        table = overall[
            [
                "minute",
                "n_obs",
                "mean_return_pct",
                "variance_return_pct2",
                "t_stat_mean_return",
                "prob_higher",
                "prob_lower",
            ]
        ].copy()
        table["prob_higher"] = (table["prob_higher"] * 100).round(2)
        table["prob_lower"] = (table["prob_lower"] * 100).round(2)
        lines.append(table.to_markdown(index=False))
    lines.append("")
    lines.append("## Per simbolo")
    lines.append("")
    if by_symbol.empty:
        lines.append("Nessun dato disponibile.")
    else:
        key_minutes = by_symbol[by_symbol["minute"].isin([1, 5, 10, 15, 20, 25, 30])][
            [
                "symbol",
                "minute",
                "n_obs",
                "mean_return_pct",
                "variance_return_pct2",
                "t_stat_mean_return",
                "prob_higher",
                "prob_lower",
            ]
        ].copy()
        key_minutes["prob_higher"] = (key_minutes["prob_higher"] * 100).round(2)
        key_minutes["prob_lower"] = (key_minutes["prob_lower"] * 100).round(2)
        lines.append(key_minutes.to_markdown(index=False))
    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    ticker_path = Path(args.tickers)
    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers = load_tickers(ticker_path)
    observations_parts: list[pd.DataFrame] = []
    missing: list[str] = []

    for symbol in tickers:
        csv_path = data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            missing.append(symbol)
            continue
        symbol_obs = load_symbol_observations(symbol, csv_path, args.minutes)
        if symbol_obs.empty:
            missing.append(symbol)
            continue
        observations_parts.append(symbol_obs)

    if observations_parts:
        observations = pd.concat(observations_parts, ignore_index=True)
    else:
        observations = pd.DataFrame()

    if not observations.empty:
        if args.fromdate:
            from_date = pd.to_datetime(args.fromdate).date()
            observations = observations[observations["trading_date"] >= from_date]
        if args.todate:
            to_date = pd.to_datetime(args.todate).date()
            observations = observations[observations["trading_date"] <= to_date]

    if observations.empty:
        overall = pd.DataFrame()
        by_symbol = pd.DataFrame()
    else:
        observations = observations.sort_values(["symbol", "trading_date", "minute"])
        overall = summarize(["minute"], observations)
        overall.insert(1, "n_symbols", observations.groupby("minute")["symbol"].nunique().values)
        by_symbol = summarize(["symbol", "minute"], observations)

    observations.to_csv(out_dir / "observations.csv", index=False)
    overall.to_csv(out_dir / "summary_all_symbols_minute.csv", index=False)
    by_symbol.to_csv(out_dir / "summary_by_symbol_minute.csv", index=False)
    (out_dir / "missing_tickers.txt").write_text("\n".join(missing) + "\n", encoding="utf-8")
    write_markdown_report(
        out_dir / "report.md",
        tickers,
        missing,
        observations,
        overall,
        by_symbol,
    )

    print(f"wrote {out_dir}")
    print(f"observations={len(observations)} missing={','.join(missing) if missing else '-'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
