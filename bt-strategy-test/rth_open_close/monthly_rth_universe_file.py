#!/usr/bin/env python3
"""
Build an ex-ante monthly RTH universe file.

For each month M, rank the input universe using only daily bars before M and
write a semicolon CSV compatible with strategies.rth_open_close.RTHOpenClose:

    year;month;symbols
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = Path(__file__).resolve().parent / "out"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--universe", default="config-common/tickers/NASDAQ_100_US.json")
    p.add_argument("--data-dir", default="config-common/data/d/yahoo")
    p.add_argument("--fromdate", default="2000-01-01")
    p.add_argument("--todate", default="2026-01-01")
    p.add_argument("--lookback-months", type=int, default=6)
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument(
        "--score",
        choices=["mean_bps", "total_return", "sharpe", "sortino", "win_rate", "composite"],
        default="sharpe",
    )
    p.add_argument(
        "--order-score",
        choices=["mean_bps", "total_return", "sharpe", "sortino", "win_rate", "composite"],
        default="",
        help="Optional secondary score used to order the selected symbols. Defaults to --score.",
    )
    p.add_argument(
        "--order-lookback-months",
        type=int,
        default=0,
        help="Optional secondary lookback used to order the selected symbols. Defaults to --lookback-months.",
    )
    p.add_argument(
        "--order-min-days",
        type=int,
        default=0,
        help="Minimum days for the ordering window. Defaults to --min-days scaled by lookback.",
    )
    p.add_argument(
        "--order-ascending",
        action="store_true",
        help="Order selected symbols from lowest to highest order score.",
    )
    p.add_argument("--min-days", type=int, default=40)
    p.add_argument("--min-adv", type=float, default=0.0)
    p.add_argument("--out", default=str(OUT_DIR / "monthly_rth_universe_6m_sharpe_top10.csv"))
    p.add_argument("--summary-out", default="")
    return p.parse_args()


def resolve(path: str) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return ROOT / p


def load_symbols(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        symbols = json.load(f)
    return list(dict.fromkeys(str(s).strip().upper() for s in symbols if str(s).strip()))


def load_frame(data_dir: Path, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    path = data_dir / f"{symbol}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=[0], index_col=0)
    df = df.rename(columns={c: c.strip().title() for c in df.columns})
    if not {"Open", "High", "Low", "Close", "Volume"}.issubset(df.columns):
        return pd.DataFrame()
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(None)
    df = df.sort_index()
    return df.loc[start:end].copy()


def load_all_frames(data_dir: Path, symbols: list[str], start: pd.Timestamp, end: pd.Timestamp) -> dict[str, pd.DataFrame]:
    frames = {}
    # Extra warmup covers the largest supported lookback without rereading files
    # inside the monthly loop.
    read_start = start - pd.DateOffset(months=36)
    for symbol in symbols:
        df = load_frame(data_dir, symbol, read_start, end)
        if not df.empty:
            frames[symbol] = df
    return frames


def score_window(df: pd.DataFrame, score: str, min_adv: float) -> dict | None:
    if df.empty:
        return None
    o = pd.to_numeric(df["Open"], errors="coerce")
    c = pd.to_numeric(df["Close"], errors="coerce")
    v = pd.to_numeric(df["Volume"], errors="coerce")
    ret = (c / o - 1.0).replace([math.inf, -math.inf], pd.NA).dropna()
    if ret.empty:
        return None
    adv = float((v * c).rolling(20).mean().dropna().iloc[-1]) if len(df) >= 20 else 0.0
    if min_adv > 0 and adv < min_adv:
        return None

    mean = float(ret.mean())
    std = float(ret.std(ddof=1)) if len(ret) > 1 else 0.0
    downside = ret[ret < 0]
    downside_std = float(downside.std(ddof=1)) if len(downside) > 1 else 0.0
    total = float((1.0 + ret).prod() - 1.0)
    sharpe = mean / std * math.sqrt(252.0) if std > 0 else 0.0
    sortino = mean / downside_std * math.sqrt(252.0) if downside_std > 0 else 0.0
    win_rate = float((ret > 0).mean())

    values = {
        "days": int(len(ret)),
        "mean_bps": mean * 10000.0,
        "total_return": total,
        "sharpe": sharpe,
        "sortino": sortino,
        "win_rate": win_rate,
        "adv": adv,
    }
    values["composite"] = (
        values["sharpe"]
        + min(max(values["sortino"], -5.0), 5.0) * 0.5
        + values["mean_bps"] * 0.05
        + (values["win_rate"] - 0.5) * 2.0
    )
    values["score"] = values[score]
    return values


def month_starts(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    start = pd.Timestamp(start.year, start.month, 1)
    end = pd.Timestamp(end.year, end.month, 1)
    return list(pd.date_range(start=start, end=end, freq="MS"))


def main() -> int:
    args = parse_args()
    symbols = load_symbols(resolve(args.universe))
    data_dir = resolve(args.data_dir)
    start = pd.Timestamp(args.fromdate)
    end = pd.Timestamp(args.todate)
    out_path = resolve(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    order_score = args.order_score or args.score
    order_lookback_months = args.order_lookback_months or args.lookback_months
    if args.order_min_days > 0:
        order_min_days = args.order_min_days
    elif args.order_lookback_months > 0:
        order_min_days = max(1, int(args.min_days * order_lookback_months / args.lookback_months))
    else:
        order_min_days = args.min_days

    frames = load_all_frames(
        data_dir,
        symbols,
        start - pd.DateOffset(months=max(args.lookback_months, order_lookback_months)),
        end,
    )

    rows = []
    summary_rows = []
    for month in month_starts(start, end):
        lookback_end = month - pd.Timedelta(days=1)
        lookback_start = month - pd.DateOffset(months=args.lookback_months)
        order_lookback_start = month - pd.DateOffset(months=order_lookback_months)
        ranked = []
        for symbol, full_df in frames.items():
            df = full_df.loc[lookback_start:lookback_end]
            metrics = score_window(df, args.score, args.min_adv)
            if metrics is None or metrics["days"] < args.min_days:
                continue
            ranked.append((symbol, metrics, full_df))

        ranked.sort(key=lambda item: item[1]["score"], reverse=True)
        selected = ranked[: args.top_n]

        ordered = []
        for symbol, metrics, full_df in selected:
            order_df = full_df.loc[order_lookback_start:lookback_end]
            order_metrics = score_window(order_df, order_score, args.min_adv)
            if order_metrics is None or order_metrics["days"] < order_min_days:
                order_value = float("-inf")
            else:
                order_value = order_metrics["score"]
            ordered.append((symbol, metrics, order_metrics, order_value))
        ordered.sort(key=lambda item: item[3], reverse=not args.order_ascending)

        rows.append(
            {
                "year": month.year,
                "month": month.month,
                "symbols": ",".join(symbol for symbol, _metrics, _order_metrics, _order_value in ordered),
            }
        )
        for rank, (symbol, metrics, order_metrics, order_value) in enumerate(ordered, start=1):
            summary = {
                "year": month.year,
                "month": month.month,
                "rank": rank,
                "symbol": symbol,
                **metrics,
                "selection_score": metrics["score"],
                "order_score": order_value,
                "order_score_name": order_score,
                "order_lookback_months": order_lookback_months,
                "order_ascending": bool(args.order_ascending),
            }
            if order_metrics:
                for key, value in order_metrics.items():
                    summary[f"order_{key}"] = value
            summary_rows.append(
                summary
            )

    pd.DataFrame(rows).to_csv(out_path, sep=";", index=False)
    if args.summary_out:
        summary_path = resolve(args.summary_out)
    else:
        summary_path = out_path.with_name(out_path.stem + "_summary.csv")
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)

    print(f"Wrote {out_path} ({len(rows)} months)")
    print(f"Wrote {summary_path} ({len(summary_rows)} ranked rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
