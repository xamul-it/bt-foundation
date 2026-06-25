#!/usr/bin/env python3
"""Search opening limit-entry TP/SL scenarios on Alpaca minute bars."""

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
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "open_limit_tp_sl"
_DAYS: list[dict] = []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--fromdate", default="2026-01-01")
    parser.add_argument("--todate", default="2026-06-02")
    parser.add_argument("--bar-minutes", type=int, default=1)
    parser.add_argument("--signal-delay-grid", default="1,2,3,5,10,15,20,30")
    parser.add_argument("--pullback-grid", default="0,0.001,0.0025,0.005,0.0075,0.01,0.0125,0.015")
    parser.add_argument("--tp-grid", default="0.0025,0.005,0.0075,0.01,0.0125,0.015,0.02")
    parser.add_argument("--sl-grid", default="0.0025,0.005,0.0075,0.01,0.0125,0.015,0.02")
    parser.add_argument(
        "--fill-from-next-bar",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If true, a signal on bar N can only fill from bar N+1 onward.",
    )
    parser.add_argument("--max-bars-after-signal", type=int, default=390)
    parser.add_argument("--jobs", type=int, default=0, help="0 = nproc-2.")
    return parser.parse_args()


def resolve_jobs(jobs: int) -> int:
    if jobs == 0:
        return max(1, (os.cpu_count() or 2) - 2)
    return max(1, jobs)


def values(text: str) -> list[float]:
    return [float(x) for x in text.split(",") if x != ""]


def int_values(text: str) -> list[int]:
    return [int(x) for x in text.split(",") if x != ""]


def load_tickers(path: Path) -> list[str]:
    raw = json.loads(path.read_text())
    return [
        str(value).strip().upper()
        for value in raw
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


def init_worker(days: list[dict]) -> None:
    global _DAYS
    _DAYS = days


def simulate_limit_day(
    day: dict,
    signal_delay: int,
    pullback: float,
    tp: float,
    sl: float,
    fill_from_next_bar: bool,
    max_bars: int,
) -> dict | None:
    opens = day["open"]
    highs = day["high"]
    lows = day["low"]
    closes = day["close"]
    if len(opens) <= signal_delay:
        return None

    reference = float(opens[signal_delay])
    if reference <= 0:
        return None
    limit_price = reference * (1.0 - pullback)
    fill_start = signal_delay + 1 if fill_from_next_bar else signal_delay
    end = min(len(opens), signal_delay + max_bars)
    if fill_start >= end:
        return {
            "outcome": "no_fill",
            "fill_bar": None,
            "hit_bar": None,
            "entry_return_vs_reference": None,
        }

    fill_hits = lows[fill_start:end] <= limit_price
    if not fill_hits.any():
        return {
            "outcome": "no_fill",
            "fill_bar": None,
            "hit_bar": None,
            "entry_return_vs_reference": None,
        }

    fill_offset = int(np.argmax(fill_hits))
    fill_bar = fill_start + fill_offset
    tp_price = limit_price * (1.0 + tp)
    sl_price = limit_price * (1.0 - sl)

    hit_tp = highs[fill_bar:end] >= tp_price
    hit_sl = lows[fill_bar:end] <= sl_price
    either = hit_tp | hit_sl
    if either.any():
        offset = int(np.argmax(either))
        hit_bar = fill_bar + offset
        if bool(hit_tp[offset]) and bool(hit_sl[offset]):
            outcome = "both_same_bar"
        elif bool(hit_tp[offset]):
            outcome = "tp_first"
        else:
            outcome = "sl_first"
        return {
            "outcome": outcome,
            "fill_bar": fill_bar,
            "hit_bar": hit_bar,
            "entry_return_vs_reference": limit_price / reference - 1.0,
        }

    return {
        "outcome": "no_exit",
        "fill_bar": fill_bar,
        "hit_bar": None,
        "entry_return_vs_reference": limit_price / reference - 1.0,
    }


def task_summary(task: tuple[int, float, float, float, bool, int]) -> tuple[dict, list[dict]]:
    signal_delay, pullback, tp, sl, fill_from_next_bar, max_bars = task
    rows = []
    for day in _DAYS:
        result = simulate_limit_day(day, signal_delay, pullback, tp, sl, fill_from_next_bar, max_bars)
        if result is None:
            continue
        rows.append(
            {
                "ticker": day["ticker"],
                "date": day["date"],
                "signal_delay_bars": signal_delay,
                "pullback": pullback,
                "tp": tp,
                "sl": sl,
                **result,
            }
        )

    if not rows:
        return {}, []

    df = pd.DataFrame(rows)
    filled = df[df["outcome"] != "no_fill"].copy()
    n = len(df)
    n_fill = len(filled)
    if n_fill:
        tp_first = filled["outcome"].eq("tp_first").mean()
        sl_or_no_exit = (
            filled["outcome"].eq("sl_first").mean()
            + filled["outcome"].eq("no_exit").mean()
        )
        ambiguous = filled["outcome"].eq("both_same_bar").mean()
        fully_conservative = tp_first * tp - (sl_or_no_exit + ambiguous) * sl
        certain_conservative = tp_first * tp - sl_or_no_exit * sl
        median_fill = filled["fill_bar"].median()
        p90_fill = filled["fill_bar"].quantile(0.90)
        resolved = filled[filled["outcome"].isin(["tp_first", "sl_first"])]
        median_hit_after_fill = (
            (resolved["hit_bar"] - resolved["fill_bar"]).median() if not resolved.empty else None
        )
        p90_hit_after_fill = (
            (resolved["hit_bar"] - resolved["fill_bar"]).quantile(0.90) if not resolved.empty else None
        )
    else:
        tp_first = sl_or_no_exit = ambiguous = fully_conservative = certain_conservative = np.nan
        median_fill = p90_fill = median_hit_after_fill = p90_hit_after_fill = None

    summary = {
        "signal_delay_bars": signal_delay,
        "pullback_pct": pullback * 100,
        "tp_pct": tp * 100,
        "sl_pct": sl * 100,
        "n_signals": n,
        "filled": n_fill,
        "fill_pct": n_fill / n * 100 if n else np.nan,
        "tp_first_pct_filled": tp_first * 100 if n_fill else np.nan,
        "sl_first_pct_filled": filled["outcome"].eq("sl_first").mean() * 100 if n_fill else np.nan,
        "no_exit_pct_filled": filled["outcome"].eq("no_exit").mean() * 100 if n_fill else np.nan,
        "same_bar_pct_filled": ambiguous * 100 if n_fill else np.nan,
        "certain_edge_bps_filled": certain_conservative * 10_000 if n_fill else np.nan,
        "fully_conservative_bps_filled": fully_conservative * 10_000 if n_fill else np.nan,
        "fully_conservative_bps_per_signal": fully_conservative * (n_fill / n) * 10_000 if n_fill and n else np.nan,
        "median_fill_bar": median_fill,
        "p90_fill_bar": p90_fill,
        "median_hit_bars_after_fill": median_hit_after_fill,
        "p90_hit_bars_after_fill": p90_hit_after_fill,
    }
    return summary, rows


def run_search(
    days: list[dict],
    delays: list[int],
    pullbacks: list[float],
    tps: list[float],
    sls: list[float],
    fill_from_next_bar: bool,
    max_bars: int,
    jobs: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    tasks = [
        (delay, pullback, tp, sl, fill_from_next_bar, max_bars)
        for delay in delays
        for pullback in pullbacks
        for tp in tps
        for sl in sls
    ]
    print(f"Running {len(tasks)} combinations with jobs={jobs}")
    if jobs <= 1:
        init_worker(days)
        results = [task_summary(task) for task in tasks]
    else:
        with mp.Pool(processes=jobs, initializer=init_worker, initargs=(days,)) as pool:
            results = pool.map(task_summary, tasks, chunksize=1)
    summaries = [summary for summary, _ in results if summary]
    details = [pd.DataFrame(rows) for _, rows in results if rows]
    return (
        pd.DataFrame(summaries),
        pd.concat(details, ignore_index=True) if details else pd.DataFrame(),
    )


def write_report(path: Path, summary: pd.DataFrame, bar_minutes: int) -> None:
    lines = ["# Open limit TP/SL study", ""]
    lines.append(f"Bar minutes: {bar_minutes}")
    lines.append("")
    cols = [
        "signal_time",
        "pullback_pct",
        "tp_pct",
        "sl_pct",
        "fill_pct",
        "tp_first_pct_filled",
        "sl_first_pct_filled",
        "no_exit_pct_filled",
        "same_bar_pct_filled",
        "fully_conservative_bps_filled",
        "fully_conservative_bps_per_signal",
        "median_fill_bar",
        "median_hit_bars_after_fill",
    ]
    lines.append("## Low ambiguity, best conservative edge per filled trade")
    lines.append("")
    filtered = summary[
        (summary["same_bar_pct_filled"] <= 1.0)
        & (summary["fill_pct"] >= 20.0)
        & (summary["fully_conservative_bps_filled"] > 0)
    ].sort_values("fully_conservative_bps_filled", ascending=False)
    lines.append(filtered.head(25)[cols].round(4).to_markdown(index=False) if not filtered.empty else "Nessun caso.")
    lines.append("")
    lines.append("## Best conservative edge per signal")
    lines.append("")
    filtered = summary[
        (summary["same_bar_pct_filled"] <= 1.0)
        & (summary["fully_conservative_bps_per_signal"] > 0)
    ].sort_values("fully_conservative_bps_per_signal", ascending=False)
    lines.append(filtered.head(25)[cols].round(4).to_markdown(index=False) if not filtered.empty else "Nessun caso.")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    missing = []
    for ticker in load_tickers(args.tickers):
        path = args.data_dir / f"{ticker}.csv"
        if not path.exists():
            missing.append(ticker)
            continue
        data = load_rth_minutes(ticker, path)
        if data.empty:
            missing.append(ticker)
            continue
        frames.append(data)
    if not frames:
        raise SystemExit("No minute data")

    panel = pd.concat(frames, ignore_index=True)
    panel = panel[
        (panel["date"] >= pd.Timestamp(args.fromdate))
        & (panel["date"] <= pd.Timestamp(args.todate))
    ].copy()
    panel = aggregate_bars(panel, args.bar_minutes)
    days = prepare_days(panel)

    jobs = resolve_jobs(args.jobs)
    summary, details = run_search(
        days,
        int_values(args.signal_delay_grid),
        values(args.pullback_grid),
        values(args.tp_grid),
        values(args.sl_grid),
        args.fill_from_next_bar,
        args.max_bars_after_signal,
        jobs,
    )
    if summary.empty:
        raise SystemExit("No results")

    def signal_time(delay: int) -> str:
        minutes = 30 + delay * args.bar_minutes
        hour = 9 + minutes // 60
        minute = minutes % 60
        return f"{hour:02d}:{minute:02d}"

    summary.insert(0, "bar_minutes", args.bar_minutes)
    summary.insert(2, "signal_time", summary["signal_delay_bars"].map(signal_time))
    details.insert(0, "bar_minutes", args.bar_minutes)
    (args.out_dir / "missing_tickers.txt").write_text("\n".join(missing) + "\n", encoding="utf-8")
    summary.to_csv(args.out_dir / "summary_grid.csv", index=False)
    details.to_csv(args.out_dir / "details.csv", index=False)
    write_report(args.out_dir / "report.md", summary, args.bar_minutes)
    print(f"Wrote {args.out_dir}")
    print(
        summary[
            (summary["same_bar_pct_filled"] <= 1.0)
            & (summary["fill_pct"] >= 20.0)
            & (summary["fully_conservative_bps_filled"] > 0)
        ]
        .sort_values("fully_conservative_bps_filled", ascending=False)
        .head(10)
        .round(4)
        .to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
