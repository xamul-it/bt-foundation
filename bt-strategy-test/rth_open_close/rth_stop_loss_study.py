#!/usr/bin/env python3
"""
Daily-OHLC stop-loss study for the RTH open/close candidate.

Signals are evaluated after the close of day T and traded at the regular open
of the next market day. This mirrors strategies.rth_open_close.RTHOpenClose:
monthly universe order and daily filters are known only after day T, while the
entry is the next open.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_UNIVERSE = (
    "bt-strategy-test/rth_open_close/out/"
    "monthly_rth_universe_36m_win_rate_wide30_order3m_sortino_desc.csv"
)
DEFAULT_DATA_DIR = "config-common/data/d/yahoo"
DEFAULT_PRIMITIVES = "bt-strategy-test/RTH_analysis/out/rth_primitives.parquet"
DEFAULT_OUT = "bt-strategy-test/rth_open_close/out/rth_stop_loss_study_summary.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep stop-loss thresholds for the RTH open/close candidate."
    )
    parser.add_argument(
        "--data-source",
        choices=("csv", "parquet"),
        default="csv",
        help="Use standard Yahoo CSV history or RTH primitives parquet.",
    )
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--primitives", default=DEFAULT_PRIMITIVES)
    parser.add_argument("--monthly-universe-file", default=DEFAULT_UNIVERSE)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--trades-out", default="")
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--max-exposure", type=float, default=0.90)
    parser.add_argument("--max-prev-overnight-ret", type=float, default=0.02)
    parser.add_argument(
        "--stop-loss-pcts",
        default="none,0.005,0.0075,0.01,0.0125,0.015,0.02,0.025,0.03",
        help="Comma list of decimal SL thresholds. Use 'none' for no stop.",
    )
    return parser.parse_args()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return repo_root() / path


def parse_stop_loss_pcts(text: str) -> list[float | None]:
    values: list[float | None] = []
    for raw in text.split(","):
        item = raw.strip().lower()
        if not item:
            continue
        if item in {"none", "nan", "no", "baseline"}:
            values.append(None)
        else:
            values.append(float(item))
    return values


def load_monthly_universe(path: Path) -> dict[tuple[int, int], list[str]]:
    df = pd.read_csv(path, sep=";")
    out: dict[tuple[int, int], list[str]] = {}
    for row in df.itertuples(index=False):
        symbols_text = getattr(row, "symbols")
        if not isinstance(symbols_text, str):
            symbols: list[str] = []
        else:
            symbols = [s.strip() for s in symbols_text.split(",") if s.strip()]
        out[(int(getattr(row, "year")), int(getattr(row, "month")))] = symbols
    return out


def load_standard_yahoo_csvs(data_dir: Path, symbols: list[str]) -> pd.DataFrame:
    frames = []
    missing = []
    for symbol in symbols:
        path = data_dir / f"{symbol}.csv"
        if not path.exists():
            missing.append(symbol)
            continue
        raw = pd.read_csv(path)
        lookup = {str(c).strip().lower().replace(" ", "_"): c for c in raw.columns}
        required = {
            "date": "feature_date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
        missing_cols = [name for name in required if name not in lookup]
        if missing_cols:
            raise ValueError(f"{path} missing columns: {missing_cols}")

        df = pd.DataFrame(
            {
                out_col: raw[lookup[in_col]]
                for in_col, out_col in required.items()
            }
        )
        df["feature_date"] = pd.to_datetime(df["feature_date"], utc=True).dt.date
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["feature_date", "open", "high", "low", "close", "volume"])
        df = df[(df[["open", "high", "low", "close"]] > 0).all(axis=1)]
        df.insert(1, "symbol", symbol)
        frames.append(df)

    if missing:
        print(
            "WARNING: missing standard Yahoo CSV files for symbols not used if absent "
            f"from selected monthly lists: {','.join(missing)}"
        )
    if not frames:
        raise ValueError(f"No standard Yahoo CSV files loaded from {data_dir}")

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["symbol", "feature_date"]).reset_index(drop=True)
    prev_close = out.groupby("symbol")["close"].shift(1)
    out["overnight_loggain"] = np.log(out["open"] / prev_close)
    out["prev_overnight_ret"] = np.expm1(out["overnight_loggain"])
    return out


def load_primitives(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    needed = {
        "feature_date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "overnight_loggain",
    }
    missing = needed.difference(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {path}: {sorted(missing)}")

    df = df.copy()
    df["feature_date"] = pd.to_datetime(df["feature_date"]).dt.date
    df = df.sort_values(["symbol", "feature_date"]).reset_index(drop=True)
    df["prev_overnight_ret"] = np.expm1(df["overnight_loggain"])
    return df


def monthly_symbols(monthly_universe: dict[tuple[int, int], list[str]]) -> list[str]:
    symbols = sorted({symbol for symbols in monthly_universe.values() for symbol in symbols})
    return symbols


def build_signal_trades(
    primitives: pd.DataFrame,
    monthly_universe: dict[tuple[int, int], list[str]],
    max_concurrent: int,
    max_prev_overnight_ret: float | None,
) -> pd.DataFrame:
    rows_by_symbol_date = {
        (row.symbol, row.feature_date): row
        for row in primitives.itertuples(index=False)
    }
    calendar = sorted(primitives["feature_date"].unique())
    next_date = {calendar[i]: calendar[i + 1] for i in range(len(calendar) - 1)}

    trades = []
    for signal_date in calendar[:-1]:
        symbols = monthly_universe.get((signal_date.year, signal_date.month), [])
        if not symbols:
            continue

        selected = []
        for symbol in symbols:
            row = rows_by_symbol_date.get((symbol, signal_date))
            if row is None:
                continue
            prev_overnight_ret = float(row.prev_overnight_ret)
            if not math.isfinite(prev_overnight_ret):
                continue
            if max_prev_overnight_ret is not None and prev_overnight_ret > max_prev_overnight_ret:
                continue
            selected.append((symbol, prev_overnight_ret))
            if len(selected) >= max_concurrent:
                break

        entry_date = next_date[signal_date]
        for rank, (symbol, prev_overnight_ret) in enumerate(selected, start=1):
            entry = rows_by_symbol_date.get((symbol, entry_date))
            if entry is None:
                continue
            open_price = float(entry.open)
            high_price = float(entry.high)
            low_price = float(entry.low)
            close_price = float(entry.close)
            if open_price <= 0:
                continue
            trades.append(
                {
                    "signal_date": signal_date,
                    "entry_date": entry_date,
                    "symbol": symbol,
                    "rank": rank,
                    "prev_overnight_ret": prev_overnight_ret,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "open_close_ret": close_price / open_price - 1.0,
                    "low_from_open": low_price / open_price - 1.0,
                    "high_from_open": high_price / open_price - 1.0,
                }
            )

    return pd.DataFrame(trades)


def apply_stop_loss(trades: pd.DataFrame, stop_loss_pct: float | None) -> pd.DataFrame:
    out = trades.copy()
    if stop_loss_pct is None:
        out["stop_loss_pct"] = np.nan
        out["stopped"] = False
        out["trade_ret"] = out["open_close_ret"]
    else:
        threshold = -abs(float(stop_loss_pct))
        out["stop_loss_pct"] = abs(float(stop_loss_pct))
        out["stopped"] = out["low_from_open"] <= threshold
        out["trade_ret"] = np.where(out["stopped"], threshold, out["open_close_ret"])
    return out


def equity_metrics(
    trades: pd.DataFrame,
    all_dates: list,
    max_concurrent: int,
    max_exposure: float,
    start_date=None,
) -> dict[str, float | int | str]:
    if start_date is not None:
        dates = [d for d in all_dates if d >= start_date]
        trades = trades[trades["entry_date"] >= start_date].copy()
    else:
        dates = all_dates

    if not dates:
        raise ValueError("No dates in metrics window")

    daily = pd.DataFrame({"entry_date": dates})
    if trades.empty:
        grouped = pd.DataFrame(columns=["entry_date", "sum_trade_ret", "trade_count", "stopped_count"])
    else:
        grouped = (
            trades.groupby("entry_date")
            .agg(
                sum_trade_ret=("trade_ret", "sum"),
                trade_count=("trade_ret", "size"),
                stopped_count=("stopped", "sum"),
            )
            .reset_index()
        )
    daily = daily.merge(grouped, on="entry_date", how="left").fillna(
        {"sum_trade_ret": 0.0, "trade_count": 0, "stopped_count": 0}
    )
    daily["portfolio_ret"] = daily["sum_trade_ret"] * float(max_exposure) / max(1, int(max_concurrent))
    daily["equity"] = (1.0 + daily["portfolio_ret"]).cumprod()
    running_max = daily["equity"].cummax()
    drawdown = daily["equity"] / running_max - 1.0

    years = max((dates[-1] - dates[0]).days / 365.25, 1 / 365.25)
    final_value = float(daily["equity"].iloc[-1])
    cagr = final_value ** (1.0 / years) - 1.0
    ret_std = float(daily["portfolio_ret"].std(ddof=1))
    sharpe = (
        float(daily["portfolio_ret"].mean()) / ret_std * math.sqrt(252.0)
        if ret_std > 0
        else 0.0
    )
    return {
        "date_from": str(dates[0]),
        "date_to": str(dates[-1]),
        "trading_days": int(len(dates)),
        "trades": int(len(trades)),
        "active_days": int((daily["trade_count"] > 0).sum()),
        "avg_trades_per_active_day": float(daily.loc[daily["trade_count"] > 0, "trade_count"].mean())
        if (daily["trade_count"] > 0).any()
        else 0.0,
        "final_multiple": final_value,
        "cagr": cagr,
        "max_drawdown": float(drawdown.min()),
        "sharpe": sharpe,
        "avg_daily_ret": float(daily["portfolio_ret"].mean()),
        "avg_trade_ret": float(trades["trade_ret"].mean()) if not trades.empty else 0.0,
        "trade_win_rate": float((trades["trade_ret"] > 0).mean()) if not trades.empty else 0.0,
        "stop_hit_rate": float(trades["stopped"].mean()) if not trades.empty else 0.0,
    }


def summarize(
    trades: pd.DataFrame,
    stop_loss_pcts: list[float | None],
    calendar: list,
    max_concurrent: int,
    max_exposure: float,
) -> pd.DataFrame:
    windows = [
        ("full", None),
        ("oos_2019", pd.Timestamp("2019-01-01").date()),
        ("recent_2022", pd.Timestamp("2022-01-01").date()),
    ]
    rows = []
    for sl in stop_loss_pcts:
        sl_trades = apply_stop_loss(trades, sl)
        for window, start_date in windows:
            metrics = equity_metrics(sl_trades, calendar, max_concurrent, max_exposure, start_date)
            rows.append(
                {
                    "scenario": "no_stop" if sl is None else f"sl_{sl:.4f}",
                    "stop_loss_pct": np.nan if sl is None else sl,
                    "window": window,
                    **metrics,
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    monthly_universe = load_monthly_universe(resolve_path(args.monthly_universe_file))
    if args.data_source == "csv":
        primitives = load_standard_yahoo_csvs(
            resolve_path(args.data_dir),
            monthly_symbols(monthly_universe),
        )
    else:
        primitives = load_primitives(resolve_path(args.primitives))
    trades = build_signal_trades(
        primitives,
        monthly_universe,
        max_concurrent=args.max_concurrent,
        max_prev_overnight_ret=args.max_prev_overnight_ret,
    )
    if trades.empty:
        raise SystemExit("No trades generated. Check inputs and filters.")

    stop_loss_pcts = parse_stop_loss_pcts(args.stop_loss_pcts)
    calendar = sorted(primitives["feature_date"].unique())
    summary = summarize(
        trades,
        stop_loss_pcts,
        calendar,
        max_concurrent=args.max_concurrent,
        max_exposure=args.max_exposure,
    )

    out_path = resolve_path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_path, index=False)

    if args.trades_out:
        trades_out = resolve_path(args.trades_out)
        trades_out.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(trades_out, index=False)

    display = summary[summary["window"].isin(["full", "oos_2019", "recent_2022"])].copy()
    for col in ["cagr", "max_drawdown", "sharpe", "stop_hit_rate", "trade_win_rate"]:
        display[col] = display[col].astype(float)
    print(f"Wrote {out_path}")
    if args.trades_out:
        print(f"Wrote {resolve_path(args.trades_out)}")
    print(
        display[
            [
                "scenario",
                "window",
                "trades",
                "cagr",
                "max_drawdown",
                "sharpe",
                "stop_hit_rate",
                "trade_win_rate",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
