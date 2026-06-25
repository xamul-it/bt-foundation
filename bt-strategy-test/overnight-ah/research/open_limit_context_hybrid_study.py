#!/usr/bin/env python3
"""5m-context limit-entry study with 1m execution resolution.

Signals are generated from completed 5-minute RTH bars. Limit entry and bracket
TP/SL sequencing are then evaluated on the underlying 1-minute bars, so a TP in
the same 5m interval as the fill can be counted when it occurs in a later minute.
If fill and TP/SL are inside the same 1-minute bar, the sequence remains
ambiguous.
"""

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
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "open_limit_context_hybrid"
FEATURE_COLS = [
    "gap",
    "ret_1",
    "ret_3",
    "ret_6",
    "from_day_open",
    "dist_vwap",
    "range_1",
    "range_so_far",
    "close_pos_so_far",
    "drawdown_from_high",
    "high_from_open",
    "low_from_open",
    "vol_ratio",
    "bar",
]
_SIGNALS: list[dict] = []
_SUMMARY_ONLY = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", type=Path, default=DEFAULT_TICKERS)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--fromdate", default="2026-01-01")
    parser.add_argument("--todate", default="2026-06-02")
    parser.add_argument("--min-signal-bar", type=int, default=1)
    parser.add_argument("--max-signal-bar", type=int, default=24)
    parser.add_argument("--lookbacks", default="3,6,12")
    parser.add_argument("--pullback-grid", default="0.005,0.0075,0.01,0.0125,0.015,0.02")
    parser.add_argument("--tp-grid", default="0.0025,0.005,0.0075,0.01")
    parser.add_argument("--sl-grid", default="0.0075,0.01,0.0125,0.015,0.02")
    parser.add_argument(
        "--latency-minutes",
        type=int,
        default=1,
        help="Conservative activation delay after signal close, in whole minute bars.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Store per-signal rows only for the selected top setup.",
    )
    parser.add_argument("--jobs", type=int, default=0)
    return parser.parse_args()


def resolve_jobs(jobs: int) -> int:
    if jobs == 0:
        return max(1, (os.cpu_count() or 2) - 2)
    return max(1, jobs)


def floats(text: str) -> list[float]:
    return [float(x) for x in text.split(",") if x]


def ints(text: str) -> list[int]:
    return [int(x) for x in text.split(",") if x]


def load_tickers(path: Path) -> list[str]:
    raw = json.loads(path.read_text())
    return [
        str(x).strip().upper()
        for x in raw
        if str(x).strip() and str(x).strip().upper() != "SPY"
    ]


def load_minutes(symbol: str, path: Path) -> pd.DataFrame:
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
    raw = raw.dropna(subset=["open", "high", "low", "close", "volume"])
    rth = raw[(raw["time"] >= "09:30") & (raw["time"] < "16:00")].copy()
    rth.insert(0, "ticker", symbol)
    return rth


def make_day_signals(
    ticker: str,
    date: pd.Timestamp,
    minute_day: pd.DataFrame,
    prev_close: float | None,
    min_bar: int,
    max_bar: int,
    lookbacks: list[int],
    signal_id_start: int,
) -> list[dict]:
    minute_day = minute_day.sort_values("timestamp").reset_index(drop=True)
    if len(minute_day) < (max_bar + 2) * 5:
        return []

    m_open = minute_day["open"].to_numpy(float)
    m_high = minute_day["high"].to_numpy(float)
    m_low = minute_day["low"].to_numpy(float)
    m_close = minute_day["close"].to_numpy(float)

    tmp = minute_day.copy()
    tmp["bar"] = tmp.index // 5
    bars = tmp.groupby("bar").agg(
        time=("time", "first"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index(drop=True)

    opens = bars["open"].to_numpy(float)
    highs = bars["high"].to_numpy(float)
    lows = bars["low"].to_numpy(float)
    closes = bars["close"].to_numpy(float)
    volumes = bars["volume"].to_numpy(float)
    typical = (highs + lows + closes) / 3.0
    cum_vwap = np.cumsum(typical * volumes) / np.maximum(np.cumsum(volumes), 1.0)
    day_open = opens[0]
    high_so_far = np.maximum.accumulate(highs)
    low_so_far = np.minimum.accumulate(lows)
    range_so_far = np.maximum(high_so_far - low_so_far, np.nan)
    gap = day_open / prev_close - 1.0 if prev_close and prev_close > 0 else np.nan

    signals = []
    signal_id = signal_id_start
    for i in range(min_bar, min(max_bar + 1, len(bars) - 1)):
        refs = {"close": closes[i]}
        for lb in lookbacks:
            start = max(0, i - lb + 1)
            refs[f"high{lb}"] = float(highs[start : i + 1].max())
        vol_base = volumes[max(0, i - 6) : i]
        vol_ratio = volumes[i] / np.nanmean(vol_base) if len(vol_base) else np.nan
        signal_minute_end = (i + 1) * 5 - 1
        signals.append(
            {
                "signal_id": signal_id,
                "ticker": ticker,
                "date": date,
                "bar": i,
                "time": str(bars.loc[i, "time"]),
                "signal_minute_end": signal_minute_end,
                "m_open": m_open,
                "m_high": m_high,
                "m_low": m_low,
                "m_close": m_close,
                "refs": refs,
                "gap": gap,
                "ret_1": closes[i] / opens[i] - 1.0,
                "ret_3": closes[i] / opens[max(0, i - 2)] - 1.0,
                "ret_6": closes[i] / opens[max(0, i - 5)] - 1.0,
                "from_day_open": closes[i] / day_open - 1.0,
                "dist_vwap": closes[i] / cum_vwap[i] - 1.0,
                "range_1": (highs[i] - lows[i]) / opens[i],
                "range_so_far": (high_so_far[i] - low_so_far[i]) / day_open,
                "close_pos_so_far": (closes[i] - low_so_far[i]) / (range_so_far[i] if range_so_far[i] > 0 else np.nan),
                "drawdown_from_high": closes[i] / high_so_far[i] - 1.0,
                "high_from_open": high_so_far[i] / day_open - 1.0,
                "low_from_open": low_so_far[i] / day_open - 1.0,
                "vol_ratio": vol_ratio,
            }
        )
        signal_id += 1
    return signals


def build_signals(panel: pd.DataFrame, min_bar: int, max_bar: int, lookbacks: list[int]) -> list[dict]:
    signals = []
    prev_close: dict[str, float] = {}
    sid = 0
    for (ticker, date), day in panel.groupby(["ticker", "date"], sort=True):
        day_signals = make_day_signals(
            ticker,
            date,
            day,
            prev_close.get(ticker),
            min_bar,
            max_bar,
            lookbacks,
            sid,
        )
        sid += len(day_signals)
        if not day.empty:
            prev_close[ticker] = float(day.sort_values("timestamp")["close"].iloc[-1])
        signals.extend(day_signals)
    return signals


def init_worker(signals: list[dict], summary_only: bool = False) -> None:
    global _SIGNALS, _SUMMARY_ONLY
    _SIGNALS = signals
    _SUMMARY_ONLY = summary_only


def simulate(signal: dict, ref: str, pullback: float, tp: float, sl: float, latency_minutes: int) -> dict:
    limit_price = signal["refs"][ref] * (1.0 - pullback)
    fill_start = signal["signal_minute_end"] + 1 + latency_minutes
    highs = signal["m_high"]
    lows = signal["m_low"]
    if fill_start >= len(highs):
        return {"outcome": "no_fill", "fill_minute": None, "hit_minute": None}
    fill_hits = lows[fill_start:] <= limit_price
    if not fill_hits.any():
        return {"outcome": "no_fill", "fill_minute": None, "hit_minute": None}
    fill_minute = fill_start + int(np.argmax(fill_hits))
    tp_price = limit_price * (1.0 + tp)
    sl_price = limit_price * (1.0 - sl)

    # Bracket is active after fill. Same 1m bar remains ambiguous because OHLC
    # cannot order low-fill vs high-TP/low-SL within that minute.
    exit_start = fill_minute
    hit_tp = highs[exit_start:] >= tp_price
    hit_sl = lows[exit_start:] <= sl_price
    either = hit_tp | hit_sl
    if either.any():
        offset = int(np.argmax(either))
        hit_minute = exit_start + offset
        if offset == 0:
            return {"outcome": "both_same_minute", "fill_minute": fill_minute, "hit_minute": hit_minute}
        if bool(hit_tp[offset]) and bool(hit_sl[offset]):
            return {"outcome": "both_same_minute", "fill_minute": fill_minute, "hit_minute": hit_minute}
        if bool(hit_tp[offset]):
            return {"outcome": "tp_first", "fill_minute": fill_minute, "hit_minute": hit_minute}
        return {"outcome": "sl_first", "fill_minute": fill_minute, "hit_minute": hit_minute}
    return {"outcome": "no_exit", "fill_minute": fill_minute, "hit_minute": None}


def summarize(rows: list[dict], tp: float, sl: float) -> dict:
    df = pd.DataFrame(rows)
    filled = df[df["outcome"] != "no_fill"]
    n = len(df)
    nf = len(filled)
    if nf == 0:
        return {"n_signals": n, "filled": 0, "fill_pct": 0.0}
    tp_first = filled["outcome"].eq("tp_first").mean()
    sl_first = filled["outcome"].eq("sl_first").mean()
    no_exit = filled["outcome"].eq("no_exit").mean()
    same = filled["outcome"].eq("both_same_minute").mean()
    edge = tp_first * tp - (sl_first + no_exit + same) * sl
    resolved = filled[filled["outcome"].isin(["tp_first", "sl_first"])]
    return {
        "n_signals": n,
        "filled": nf,
        "fill_pct": nf / n * 100,
        "tp_first_pct_filled": tp_first * 100,
        "sl_first_pct_filled": sl_first * 100,
        "no_exit_pct_filled": no_exit * 100,
        "same_minute_pct_filled": same * 100,
        "fully_conservative_bps_filled": edge * 10_000,
        "fully_conservative_bps_per_signal": edge * (nf / n) * 10_000,
        "median_fill_delay_min": (filled["fill_minute"] - filled["signal_minute_end"]).median(),
        "p90_fill_delay_min": (filled["fill_minute"] - filled["signal_minute_end"]).quantile(0.90),
        "median_hit_after_fill_min": (resolved["hit_minute"] - resolved["fill_minute"]).median() if not resolved.empty else np.nan,
        "p90_hit_after_fill_min": (resolved["hit_minute"] - resolved["fill_minute"]).quantile(0.90) if not resolved.empty else np.nan,
    }


def task_summary(task: tuple[str, float, float, float, int]) -> tuple[dict, list[dict]]:
    ref, pullback, tp, sl, latency = task
    if _SUMMARY_ONLY:
        n = len(_SIGNALS)
        filled = 0
        tp_count = 0
        sl_count = 0
        no_exit_count = 0
        same_count = 0
        fill_delays = []
        hit_delays = []
        for signal in _SIGNALS:
            result = simulate(signal, ref, pullback, tp, sl, latency)
            outcome = result["outcome"]
            if outcome == "no_fill":
                continue
            filled += 1
            fill_delays.append(result["fill_minute"] - signal["signal_minute_end"])
            if outcome == "tp_first":
                tp_count += 1
                hit_delays.append(result["hit_minute"] - result["fill_minute"])
            elif outcome == "sl_first":
                sl_count += 1
                hit_delays.append(result["hit_minute"] - result["fill_minute"])
            elif outcome == "no_exit":
                no_exit_count += 1
            elif outcome == "both_same_minute":
                same_count += 1

        if filled == 0:
            summary = {"n_signals": n, "filled": 0, "fill_pct": 0.0}
        else:
            tp_first = tp_count / filled
            sl_first = sl_count / filled
            no_exit = no_exit_count / filled
            same = same_count / filled
            edge = tp_first * tp - (sl_first + no_exit + same) * sl
            summary = {
                "n_signals": n,
                "filled": filled,
                "fill_pct": filled / n * 100,
                "tp_first_pct_filled": tp_first * 100,
                "sl_first_pct_filled": sl_first * 100,
                "no_exit_pct_filled": no_exit * 100,
                "same_minute_pct_filled": same * 100,
                "fully_conservative_bps_filled": edge * 10_000,
                "fully_conservative_bps_per_signal": edge * (filled / n) * 10_000,
                "median_fill_delay_min": float(np.median(fill_delays)),
                "p90_fill_delay_min": float(np.quantile(fill_delays, 0.90)),
                "median_hit_after_fill_min": float(np.median(hit_delays)) if hit_delays else np.nan,
                "p90_hit_after_fill_min": float(np.quantile(hit_delays, 0.90)) if hit_delays else np.nan,
            }
        summary.update({"ref": ref, "pullback_pct": pullback * 100, "tp_pct": tp * 100, "sl_pct": sl * 100})
        return summary, []

    rows = []
    for signal in _SIGNALS:
        result = simulate(signal, ref, pullback, tp, sl, latency)
        rows.append(
            {
                "signal_id": signal["signal_id"],
                "ticker": signal["ticker"],
                "date": signal["date"],
                "bar": signal["bar"],
                "time": signal["time"],
                "signal_minute_end": signal["signal_minute_end"],
                "ref": ref,
                "pullback": pullback,
                "tp": tp,
                "sl": sl,
                **result,
                **{k: signal[k] for k in FEATURE_COLS},
            }
        )
    summary = summarize(rows, tp, sl)
    summary.update({"ref": ref, "pullback_pct": pullback * 100, "tp_pct": tp * 100, "sl_pct": sl * 100})
    return summary, rows


def feature_filter_sweep(rows: list[dict], tp: float, sl: float) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    filled = df[df["outcome"] != "no_fill"].copy()
    out = []
    for feature in FEATURE_COLS:
        tmp = filled.dropna(subset=[feature])
        if len(tmp) < 100 or tmp[feature].nunique() < 4:
            continue
        for q in [0.2, 0.25, 0.33, 0.5, 0.67, 0.75, 0.8]:
            thr = tmp[feature].quantile(q)
            for op in ["<=", ">="]:
                group = tmp[tmp[feature] <= thr] if op == "<=" else tmp[tmp[feature] >= thr]
                if len(group) < 80:
                    continue
                tp_first = group["outcome"].eq("tp_first").mean()
                sl_first = group["outcome"].eq("sl_first").mean()
                no_exit = group["outcome"].eq("no_exit").mean()
                same = group["outcome"].eq("both_same_minute").mean()
                edge = tp_first * tp - (sl_first + no_exit + same) * sl
                out.append(
                    {
                        "rule": f"{feature} {op} q{q:.2f} ({thr:.6g})",
                        "n_filled": len(group),
                        "keep_pct_filled": len(group) / len(filled) * 100,
                        "tp_first_pct": tp_first * 100,
                        "same_minute_pct": same * 100,
                        "edge_bps_filled": edge * 10_000,
                    }
                )
    return pd.DataFrame(out).sort_values("edge_bps_filled", ascending=False)


def write_report(path: Path, summary: pd.DataFrame, filters: pd.DataFrame) -> None:
    cols = [
        "ref",
        "pullback_pct",
        "tp_pct",
        "sl_pct",
        "fill_pct",
        "tp_first_pct_filled",
        "same_minute_pct_filled",
        "fully_conservative_bps_filled",
        "fully_conservative_bps_per_signal",
        "median_fill_delay_min",
        "median_hit_after_fill_min",
    ]
    lines = ["# 5m Context / 1m Execution Limit Study", ""]
    lines.append("## Best Base Setups")
    lines.append("")
    lines.append(summary.head(25)[cols].round(4).to_markdown(index=False))
    lines.append("")
    lines.append("## Best Filters On Top Setup")
    lines.append("")
    lines.append(filters.head(30).round(4).to_markdown(index=False) if not filters.empty else "Nessun filtro.")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for ticker in load_tickers(args.tickers):
        path = args.data_dir / f"{ticker}.csv"
        if path.exists():
            data = load_minutes(ticker, path)
            if not data.empty:
                frames.append(data)
    if not frames:
        raise SystemExit("No data")
    panel = pd.concat(frames, ignore_index=True)
    panel = panel[(panel["date"] >= pd.Timestamp(args.fromdate)) & (panel["date"] <= pd.Timestamp(args.todate))]
    signals = build_signals(panel, args.min_signal_bar, args.max_signal_bar, ints(args.lookbacks))
    refs = ["close"] + [f"high{x}" for x in ints(args.lookbacks)]
    tasks = [
        (ref, pullback, tp, sl, args.latency_minutes)
        for ref in refs
        for pullback in floats(args.pullback_grid)
        for tp in floats(args.tp_grid)
        for sl in floats(args.sl_grid)
    ]
    jobs = resolve_jobs(args.jobs)
    print(f"Signals: {len(signals)}")
    print(f"Running {len(tasks)} setup combinations with jobs={jobs}")
    if jobs <= 1:
        init_worker(signals, args.summary_only)
        results = [task_summary(task) for task in tasks]
    else:
        with mp.Pool(processes=jobs, initializer=init_worker, initargs=(signals, args.summary_only)) as pool:
            results = pool.map(task_summary, tasks, chunksize=1)
    all_summary = pd.DataFrame([r[0] for r in results])
    all_summary.to_csv(args.out_dir / "summary_grid_all.csv", index=False)
    summary = all_summary[
        (all_summary["fill_pct"] >= 20)
        & (all_summary["same_minute_pct_filled"] <= 1)
        & (all_summary["fully_conservative_bps_filled"] > 0)
    ].sort_values("fully_conservative_bps_filled", ascending=False)
    summary.to_csv(args.out_dir / "summary_grid.csv", index=False)
    if summary.empty:
        print("No positive low-ambiguity setup")
        return 1
    top = summary.iloc[0]
    top_idx = next(
        i
        for i, (s, _) in enumerate(results)
        if s["ref"] == top["ref"]
        and abs(s["pullback_pct"] - top["pullback_pct"]) < 1e-9
        and abs(s["tp_pct"] - top["tp_pct"]) < 1e-9
        and abs(s["sl_pct"] - top["sl_pct"]) < 1e-9
    )
    top_rows = results[top_idx][1]
    if not top_rows:
        init_worker(signals, False)
        _, top_rows = task_summary(
            (
                str(top["ref"]),
                float(top["pullback_pct"]) / 100,
                float(top["tp_pct"]) / 100,
                float(top["sl_pct"]) / 100,
                args.latency_minutes,
            )
        )
    pd.DataFrame(top_rows).to_csv(args.out_dir / "top_setup_signals.csv", index=False)
    filters = feature_filter_sweep(top_rows, float(top["tp_pct"]) / 100, float(top["sl_pct"]) / 100)
    filters.to_csv(args.out_dir / "top_setup_filter_sweep.csv", index=False)
    write_report(args.out_dir / "report.md", summary, filters)
    print(f"Wrote {args.out_dir}")
    print(summary.head(10).round(4).to_string(index=False))
    print("\nTop filters")
    print(filters.head(10).round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
