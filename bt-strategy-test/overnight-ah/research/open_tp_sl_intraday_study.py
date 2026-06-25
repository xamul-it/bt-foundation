#!/usr/bin/env python3
"""Study whether an opening TP is hit before an opening SL on minute bars."""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent.parent
NY_TZ = "America/New_York"
DEFAULT_TICKERS = ROOT / "config-common" / "tickers" / "stable_ah_top10.json"
DEFAULT_DATA = ROOT / "config-common" / "data" / "m" / "alpaca" / "sip"
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "open_tp_sl"
_WORKER_DAYS: list[dict] = []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--fromdate", default="2026-01-01")
    parser.add_argument("--todate", default="2026-06-02")
    parser.add_argument("--tp-grid", default="0.001,0.0015,0.002,0.0025,0.003,0.004,0.005")
    parser.add_argument("--sl-grid", default="0.0005,0.001,0.0015,0.002,0.0025,0.003")
    parser.add_argument(
        "--entry-delay-minutes",
        type=int,
        default=0,
        help="0 enters at 09:30 open; 1 enters at next minute open, etc.",
    )
    parser.add_argument(
        "--entry-delay-grid",
        default="",
        help="Comma-separated entry delays. Overrides --entry-delay-minutes when set.",
    )
    parser.add_argument(
        "--bar-minutes",
        type=int,
        default=1,
        help="Aggregate RTH minute bars to this many minutes before simulating.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Parallel worker processes. Use 0 for nproc-2.",
    )
    parser.add_argument(
        "--max-minutes",
        type=int,
        default=390,
        help="Maximum RTH minutes after entry to monitor.",
    )
    return parser.parse_args()


def resolve_jobs(jobs: int) -> int:
    if jobs == 0:
        return max(1, (os.cpu_count() or 2) - 2)
    return max(1, jobs)


def load_tickers(path: Path) -> list[str]:
    values = json.loads(path.read_text())
    return [
        str(value).strip().upper()
        for value in values
        if str(value).strip() and str(value).strip().upper() != "SPY"
    ]


def load_rth_minutes(symbol: str, path: Path) -> pd.DataFrame:
    raw = pd.read_csv(path)
    if raw.empty:
        return pd.DataFrame()
    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True, errors="coerce")
    raw = raw.dropna(subset=["timestamp"]).sort_values("timestamp")
    raw["ny_timestamp"] = raw["timestamp"].dt.tz_convert(NY_TZ)
    raw["date"] = raw["ny_timestamp"].dt.tz_localize(None).dt.normalize()
    raw["time"] = raw["ny_timestamp"].dt.strftime("%H:%M")
    for col in ["open", "high", "low", "close", "volume"]:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.dropna(subset=["open", "high", "low", "close"])
    rth = raw[(raw["time"] >= "09:30") & (raw["time"] < "16:00")].copy()
    rth.insert(0, "ticker", symbol)
    return rth


def aggregate_bars(rth: pd.DataFrame, bar_minutes: int) -> pd.DataFrame:
    if bar_minutes <= 1 or rth.empty:
        return rth
    parts = []
    for (_, _), day in rth.groupby(["ticker", "date"], sort=True):
        day = day.sort_values("timestamp").reset_index(drop=True).copy()
        day["bar_group"] = day.index // bar_minutes
        agg = day.groupby("bar_group").agg(
            ticker=("ticker", "first"),
            timestamp=("timestamp", "first"),
            ny_timestamp=("ny_timestamp", "first"),
            date=("date", "first"),
            time=("time", "first"),
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        ).reset_index(drop=True)
        parts.append(agg)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def simulate_day(day: pd.DataFrame, tp: float, sl: float, entry_delay: int, max_minutes: int) -> dict | None:
    day = day.sort_values("timestamp").reset_index(drop=True)
    if len(day) <= entry_delay:
        return None
    entry_row = day.iloc[entry_delay]
    entry_price = float(entry_row["open"])
    if entry_price <= 0:
        return None

    tp_price = entry_price * (1.0 + tp)
    sl_price = entry_price * (1.0 - sl)
    monitor = day.iloc[entry_delay : entry_delay + max_minutes].copy()
    if monitor.empty:
        return None

    for i, row in monitor.iterrows():
        hit_tp = float(row["high"]) >= tp_price
        hit_sl = float(row["low"]) <= sl_price
        minute_from_entry = int(i - entry_delay)
        if hit_tp and hit_sl:
            # Minute OHLC cannot tell intraminute order. Mark it separately.
            return {
                "outcome": "both_same_minute",
                "hit_minute": minute_from_entry,
                "exit_return": None,
            }
        if hit_tp:
            return {"outcome": "tp_first", "hit_minute": minute_from_entry, "exit_return": tp}
        if hit_sl:
            return {"outcome": "sl_first", "hit_minute": minute_from_entry, "exit_return": -sl}

    close_ret = float(monitor.iloc[-1]["close"]) / entry_price - 1.0
    return {"outcome": "no_hit", "hit_minute": None, "exit_return": close_ret}


def prepare_days(panel: pd.DataFrame) -> list[dict]:
    days = []
    for (ticker, date), day in panel.groupby(["ticker", "date"], sort=True):
        day = day.sort_values("timestamp")
        days.append(
            {
                "ticker": ticker,
                "date": date,
                "open": day["open"].to_numpy(float),
                "high": day["high"].to_numpy(float),
                "low": day["low"].to_numpy(float),
                "close": day["close"].to_numpy(float),
            }
        )
    return days


def _init_worker(days: list[dict]) -> None:
    global _WORKER_DAYS
    _WORKER_DAYS = days


def simulate_day_arrays(day: dict, tp: float, sl: float, entry_delay: int, max_bars: int) -> tuple[str, int | None, float | None] | None:
    opens = day["open"]
    highs = day["high"]
    lows = day["low"]
    closes = day["close"]
    if len(opens) <= entry_delay:
        return None
    entry_price = float(opens[entry_delay])
    if entry_price <= 0:
        return None
    tp_price = entry_price * (1.0 + tp)
    sl_price = entry_price * (1.0 - sl)
    end = min(len(opens), entry_delay + max_bars)
    hit_tp = highs[entry_delay:end] >= tp_price
    hit_sl = lows[entry_delay:end] <= sl_price
    either = hit_tp | hit_sl
    if either.any():
        offset = int(np.argmax(either))
        if bool(hit_tp[offset]) and bool(hit_sl[offset]):
            return "both_same_minute", offset, None
        if bool(hit_tp[offset]):
            return "tp_first", offset, tp
        return "sl_first", offset, -sl
    close_ret = float(closes[end - 1]) / entry_price - 1.0
    return "no_hit", None, close_ret


def _task_summary(task: tuple[int, float, float, int]) -> tuple[dict, list[dict]]:
    entry_delay, tp, sl, max_bars = task
    rows = []
    for day in _WORKER_DAYS:
        result = simulate_day_arrays(day, tp, sl, entry_delay, max_bars)
        if result is None:
            continue
        outcome, hit_minute, exit_return = result
        rows.append(
            {
                "ticker": day["ticker"],
                "date": day["date"],
                "entry_delay_bars": entry_delay,
                "tp": tp,
                "sl": sl,
                "outcome": outcome,
                "hit_minute": hit_minute,
                "exit_return": exit_return,
            }
        )
    if not rows:
        return {}, []
    df = pd.DataFrame(rows)
    known = df[df["outcome"] != "both_same_minute"]
    resolved = known[known["outcome"].isin(["tp_first", "sl_first"])]
    summary = {
        "entry_delay_bars": entry_delay,
        "tp_pct": tp * 100,
        "sl_pct": sl * 100,
        "n": len(df),
        "tp_first_pct": df["outcome"].eq("tp_first").mean() * 100,
        "sl_first_pct": df["outcome"].eq("sl_first").mean() * 100,
        "both_same_minute_pct": df["outcome"].eq("both_same_minute").mean() * 100,
        "no_hit_pct": df["outcome"].eq("no_hit").mean() * 100,
        "tp_before_sl_when_resolved_pct": (
            resolved["outcome"].eq("tp_first").mean() * 100 if not resolved.empty else None
        ),
        "expected_return_bps": known["exit_return"].dropna().mean() * 10_000,
        "conservative_expected_bps": (
            (
                df["outcome"].eq("tp_first").mean() * tp
                - (
                    df["outcome"].eq("sl_first").mean()
                    + df["outcome"].eq("no_hit").mean()
                )
                * sl
            )
            * 10_000
        ),
        "certain_expected_bps": (
            (
                df["outcome"].eq("tp_first").mean() * tp
                - (
                    df["outcome"].eq("sl_first").mean()
                    + df["outcome"].eq("no_hit").mean()
                )
                * sl
            )
            * 10_000
        ),
        "median_hit_minute": resolved["hit_minute"].median() if not resolved.empty else None,
        "p90_hit_minute": resolved["hit_minute"].quantile(0.90) if not resolved.empty else None,
    }
    return summary, rows


def run_grid(panel: pd.DataFrame, tps: list[float], sls: list[float], entry_delay: int, max_minutes: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    details = []
    grouped = list(panel.groupby(["ticker", "date"], sort=True))
    for tp in tps:
        for sl in sls:
            outcomes = []
            for (ticker, date), day in grouped:
                result = simulate_day(day, tp, sl, entry_delay, max_minutes)
                if result is None:
                    continue
                row = {
                    "ticker": ticker,
                    "date": date,
                    "tp": tp,
                    "sl": sl,
                    **result,
                }
                outcomes.append(row)
                details.append(row)
            if not outcomes:
                continue
            df = pd.DataFrame(outcomes)
            known = df[df["outcome"] != "both_same_minute"]
            resolved = known[known["outcome"].isin(["tp_first", "sl_first"])]
            rows.append(
                {
                    "tp_pct": tp * 100,
                    "sl_pct": sl * 100,
                    "n": len(df),
                    "tp_first_pct": (df["outcome"].eq("tp_first").mean() * 100),
                    "sl_first_pct": (df["outcome"].eq("sl_first").mean() * 100),
                    "both_same_minute_pct": (df["outcome"].eq("both_same_minute").mean() * 100),
                    "no_hit_pct": (df["outcome"].eq("no_hit").mean() * 100),
                    "tp_before_sl_when_resolved_pct": (
                        resolved["outcome"].eq("tp_first").mean() * 100 if not resolved.empty else None
                    ),
                    "expected_return_bps": known["exit_return"].dropna().mean() * 10_000,
                    "conservative_expected_bps": (
                        (
                            df["outcome"].eq("tp_first").mean() * tp
                            - (
                                df["outcome"].eq("sl_first").mean()
                                + df["outcome"].eq("no_hit").mean()
                            )
                            * sl
                        )
                        * 10_000
                    ),
                    "certain_expected_bps": (
                        (
                            df["outcome"].eq("tp_first").mean() * tp
                            - (
                                df["outcome"].eq("sl_first").mean()
                                + df["outcome"].eq("no_hit").mean()
                            )
                            * sl
                        )
                        * 10_000
                    ),
                    "median_hit_minute": resolved["hit_minute"].median() if not resolved.empty else None,
                    "p90_hit_minute": resolved["hit_minute"].quantile(0.90) if not resolved.empty else None,
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(details)


def run_delay_grid(panel: pd.DataFrame, tps: list[float], sls: list[float], delays: list[int], max_minutes: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    summaries = []
    details = []
    for delay in delays:
        summary, detail = run_grid(panel, tps, sls, delay, max_minutes)
        if summary.empty:
            continue
        summary.insert(0, "entry_delay_bars", delay)
        detail.insert(0, "entry_delay_bars", delay)
        summaries.append(summary)
        details.append(detail)
    return (
        pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame(),
        pd.concat(details, ignore_index=True) if details else pd.DataFrame(),
    )


def run_delay_grid_parallel(
    panel: pd.DataFrame,
    tps: list[float],
    sls: list[float],
    delays: list[int],
    max_bars: int,
    jobs: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    days = prepare_days(panel)
    tasks = [(delay, tp, sl, max_bars) for delay in delays for tp in tps for sl in sls]
    if jobs <= 1:
        _init_worker(days)
        results = [_task_summary(task) for task in tasks]
    else:
        with mp.Pool(processes=jobs, initializer=_init_worker, initargs=(days,)) as pool:
            results = pool.map(_task_summary, tasks, chunksize=1)
    summaries = [summary for summary, _ in results if summary]
    details = [pd.DataFrame(rows) for _, rows in results if rows]
    return (
        pd.DataFrame(summaries),
        pd.concat(details, ignore_index=True) if details else pd.DataFrame(),
    )


def write_report(path: Path, summary: pd.DataFrame, details: pd.DataFrame) -> None:
    lines = ["# Open TP/SL intraday study", ""]
    lines.append(f"Symbol-days: {details[['ticker', 'date']].drop_duplicates().shape[0] if not details.empty else 0}")
    lines.append("")
    target = summary[(summary["tp_pct"].round(4) == 0.25) & (summary["sl_pct"].round(4) == 0.10)]
    lines.append("## Target TP 0.25 / SL 0.10")
    lines.append("")
    if target.empty:
        lines.append("Target non presente nella griglia.")
    else:
        lines.append(target.round(4).to_markdown(index=False))
    lines.append("")
    lines.append("## Highest TP-first probability")
    lines.append("")
    cols = [
        "tp_pct",
        "sl_pct",
        "n",
        "tp_first_pct",
        "sl_first_pct",
        "both_same_minute_pct",
        "no_hit_pct",
        "tp_before_sl_when_resolved_pct",
        "expected_return_bps",
        "conservative_expected_bps",
        "median_hit_minute",
        "p90_hit_minute",
    ]
    lines.append(
        summary.sort_values(["tp_first_pct", "expected_return_bps"], ascending=[False, False])
        .head(20)[cols]
        .round(4)
        .to_markdown(index=False)
    )
    lines.append("")
    lines.append("Note: se TP e SL sono entrambi dentro la stessa barra minuto, l'ordine intraminuto e' ambiguo.")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    tickers = load_tickers(args.tickers)
    frames = []
    missing = []
    for ticker in tickers:
        path = args.data_dir / f"{ticker}.csv"
        if not path.exists():
            missing.append(ticker)
            continue
        rth = load_rth_minutes(ticker, path)
        if rth.empty:
            missing.append(ticker)
            continue
        frames.append(rth)
    if not frames:
        raise SystemExit("No minute data")
    panel = pd.concat(frames, ignore_index=True)
    panel = panel[
        (panel["date"] >= pd.Timestamp(args.fromdate))
        & (panel["date"] <= pd.Timestamp(args.todate))
    ].copy()
    tps = [float(x) for x in args.tp_grid.split(",") if x]
    sls = [float(x) for x in args.sl_grid.split(",") if x]
    panel = aggregate_bars(panel, args.bar_minutes)
    delays = (
        [int(x) for x in args.entry_delay_grid.split(",") if x != ""]
        if args.entry_delay_grid
        else [args.entry_delay_minutes]
    )
    jobs = resolve_jobs(args.jobs)
    print(f"Running {len(delays) * len(tps) * len(sls)} combinations with jobs={jobs}")
    summary, details = run_delay_grid_parallel(panel, tps, sls, delays, args.max_minutes, jobs)
    summary.insert(0, "bar_minutes", args.bar_minutes)
    details.insert(0, "bar_minutes", args.bar_minutes)
    summary.to_csv(args.out_dir / "summary_grid.csv", index=False)
    details.to_csv(args.out_dir / "details.csv", index=False)
    (args.out_dir / "missing_tickers.txt").write_text("\n".join(missing) + "\n", encoding="utf-8")
    write_report(args.out_dir / "report.md", summary, details)
    print(f"Wrote {args.out_dir}")
    target = summary[(summary["tp_pct"].round(4) == 0.25) & (summary["sl_pct"].round(4) == 0.10)]
    if not target.empty:
        print(target.round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
