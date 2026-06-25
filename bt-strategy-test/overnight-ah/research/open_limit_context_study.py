#!/usr/bin/env python3
"""Event/context search for 5m opening pullback limit entries."""

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
DEFAULT_OUT = Path(__file__).resolve().parent / "out" / "open_limit_context_5m"
_SIGNALS: list[dict] = []


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
    parser.add_argument("--jobs", type=int, default=0)
    parser.add_argument("--topn", type=int, default=8)
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


def load_rth(symbol: str, path: Path) -> pd.DataFrame:
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


def aggregate_5m(rth: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for (_, _), day in rth.groupby(["ticker", "date"], sort=True):
        day = day.sort_values("timestamp").reset_index(drop=True).copy()
        day["bar"] = day.index // 5
        parts.append(
            day.groupby("bar").agg(
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
        )
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def make_signals(panel: pd.DataFrame, min_bar: int, max_bar: int, lookbacks: list[int]) -> list[dict]:
    signals = []
    prev_closes: dict[str, float] = {}
    for (ticker, date), day in panel.groupby(["ticker", "date"], sort=True):
        day = day.sort_values("timestamp").reset_index(drop=True)
        if len(day) < max_bar + 3:
            continue
        opens = day["open"].to_numpy(float)
        highs = day["high"].to_numpy(float)
        lows = day["low"].to_numpy(float)
        closes = day["close"].to_numpy(float)
        volumes = day["volume"].to_numpy(float)
        typical = (highs + lows + closes) / 3.0
        cum_vwap = np.cumsum(typical * volumes) / np.maximum(np.cumsum(volumes), 1.0)
        day_open = opens[0]
        prev_close = prev_closes.get(ticker, np.nan)
        prev_closes[ticker] = closes[-1]
        high_so_far = np.maximum.accumulate(highs)
        low_so_far = np.minimum.accumulate(lows)
        range_so_far = np.maximum(high_so_far - low_so_far, np.nan)
        for i in range(min_bar, min(max_bar + 1, len(day) - 1)):
            refs = {"close": closes[i]}
            for lb in lookbacks:
                start = max(0, i - lb + 1)
                refs[f"high{lb}"] = float(highs[start : i + 1].max())
            vol_base = volumes[max(0, i - 6) : i]
            vol_ratio = volumes[i] / np.nanmean(vol_base) if len(vol_base) else np.nan
            signal = {
                "ticker": ticker,
                "date": date,
                "bar": i,
                "time": str(day.loc[i, "time"]),
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "refs": refs,
                "gap": opens[0] / prev_close - 1.0 if prev_close and prev_close > 0 else np.nan,
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
            signals.append(signal)
    return signals


def init_worker(signals: list[dict]) -> None:
    global _SIGNALS
    _SIGNALS = signals


def simulate(signal: dict, ref_name: str, pullback: float, tp: float, sl: float) -> tuple[str, int | None, int | None]:
    highs = signal["high"]
    lows = signal["low"]
    ref = signal["refs"][ref_name]
    limit_price = ref * (1.0 - pullback)
    fill_start = signal["bar"] + 1
    if fill_start >= len(highs):
        return "no_fill", None, None
    fill_hits = lows[fill_start:] <= limit_price
    if not fill_hits.any():
        return "no_fill", None, None
    fill_bar = fill_start + int(np.argmax(fill_hits))
    exit_start = fill_bar + 1
    if exit_start >= len(highs):
        return "no_exit", fill_bar, None
    tp_price = limit_price * (1.0 + tp)
    sl_price = limit_price * (1.0 - sl)
    hit_tp = highs[exit_start:] >= tp_price
    hit_sl = lows[exit_start:] <= sl_price
    either = hit_tp | hit_sl
    if either.any():
        offset = int(np.argmax(either))
        hit_bar = exit_start + offset
        if bool(hit_tp[offset]) and bool(hit_sl[offset]):
            return "both_same_bar", fill_bar, hit_bar
        if bool(hit_tp[offset]):
            return "tp_first", fill_bar, hit_bar
        return "sl_first", fill_bar, hit_bar
    return "no_exit", fill_bar, None


def summarize_rows(rows: list[dict], tp: float, sl: float) -> dict:
    df = pd.DataFrame(rows)
    filled = df[df["outcome"] != "no_fill"]
    n = len(df)
    nf = len(filled)
    if nf == 0:
        return {"n_signals": n, "filled": 0, "fill_pct": 0.0}
    tp_first = filled["outcome"].eq("tp_first").mean()
    sl_first = filled["outcome"].eq("sl_first").mean()
    no_exit = filled["outcome"].eq("no_exit").mean()
    same = filled["outcome"].eq("both_same_bar").mean()
    edge = tp_first * tp - (sl_first + no_exit + same) * sl
    resolved = filled[filled["outcome"].isin(["tp_first", "sl_first"])]
    return {
        "n_signals": n,
        "filled": nf,
        "fill_pct": nf / n * 100,
        "tp_first_pct_filled": tp_first * 100,
        "sl_first_pct_filled": sl_first * 100,
        "no_exit_pct_filled": no_exit * 100,
        "same_bar_pct_filled": same * 100,
        "fully_conservative_bps_filled": edge * 10_000,
        "fully_conservative_bps_per_signal": edge * (nf / n) * 10_000,
        "median_fill_delay_bars": (filled["fill_bar"] - filled["bar"]).median(),
        "p90_fill_delay_bars": (filled["fill_bar"] - filled["bar"]).quantile(0.90),
        "median_hit_after_fill_bars": (resolved["hit_bar"] - resolved["fill_bar"]).median() if not resolved.empty else np.nan,
        "p90_hit_after_fill_bars": (resolved["hit_bar"] - resolved["fill_bar"]).quantile(0.90) if not resolved.empty else np.nan,
    }


def task_summary(task: tuple[str, float, float, float]) -> tuple[dict, list[dict]]:
    ref_name, pullback, tp, sl = task
    rows = []
    for idx, signal in enumerate(_SIGNALS):
        outcome, fill_bar, hit_bar = simulate(signal, ref_name, pullback, tp, sl)
        rows.append(
            {
                "signal_id": idx,
                "ticker": signal["ticker"],
                "date": signal["date"],
                "bar": signal["bar"],
                "time": signal["time"],
                "outcome": outcome,
                "fill_bar": fill_bar,
                "hit_bar": hit_bar,
                **{k: signal[k] for k in FEATURE_COLS},
            }
        )
    summary = summarize_rows(rows, tp, sl)
    summary.update(
        {
            "ref": ref_name,
            "pullback_pct": pullback * 100,
            "tp_pct": tp * 100,
            "sl_pct": sl * 100,
        }
    )
    return summary, rows


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
]


def feature_bins(rows: list[dict], tp: float, sl: float) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    filled = df[df["outcome"] != "no_fill"].copy()
    out = []
    for feature in FEATURE_COLS + ["bar"]:
        tmp = filled.dropna(subset=[feature]).copy()
        if tmp[feature].nunique() < 4 or len(tmp) < 100:
            continue
        tmp["bin"] = pd.qcut(tmp[feature], 4, labels=False, duplicates="drop") + 1
        for bin_id, group in tmp.groupby("bin"):
            tp_first = group["outcome"].eq("tp_first").mean()
            sl_first = group["outcome"].eq("sl_first").mean()
            no_exit = group["outcome"].eq("no_exit").mean()
            same = group["outcome"].eq("both_same_bar").mean()
            edge = tp_first * tp - (sl_first + no_exit + same) * sl
            out.append(
                {
                    "feature": feature,
                    "bin": int(bin_id),
                    "feature_min": group[feature].min(),
                    "feature_max": group[feature].max(),
                    "n_filled": len(group),
                    "tp_first_pct": tp_first * 100,
                    "same_bar_pct": same * 100,
                    "edge_bps_filled": edge * 10_000,
                }
            )
    return pd.DataFrame(out)


def filter_sweep(rows: list[dict], tp: float, sl: float) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    filled = df[df["outcome"] != "no_fill"].copy()
    out = []
    for feature in FEATURE_COLS + ["bar"]:
        tmp = filled.dropna(subset=[feature]).copy()
        if tmp[feature].nunique() < 4 or len(tmp) < 100:
            continue
        for q in [0.2, 0.25, 0.33, 0.5, 0.67, 0.75, 0.8]:
            threshold = tmp[feature].quantile(q)
            for op in ["<=", ">="]:
                group = tmp[tmp[feature] <= threshold] if op == "<=" else tmp[tmp[feature] >= threshold]
                if len(group) < 80:
                    continue
                tp_first = group["outcome"].eq("tp_first").mean()
                sl_first = group["outcome"].eq("sl_first").mean()
                no_exit = group["outcome"].eq("no_exit").mean()
                same = group["outcome"].eq("both_same_bar").mean()
                edge = tp_first * tp - (sl_first + no_exit + same) * sl
                out.append(
                    {
                        "rule": f"{feature} {op} q{q:.2f} ({threshold:.6g})",
                        "n_filled": len(group),
                        "keep_pct_filled": len(group) / len(filled) * 100,
                        "tp_first_pct": tp_first * 100,
                        "same_bar_pct": same * 100,
                        "edge_bps_filled": edge * 10_000,
                    }
                )
    return pd.DataFrame(out).sort_values("edge_bps_filled", ascending=False)


def write_report(path: Path, summary: pd.DataFrame, bins: pd.DataFrame, filters: pd.DataFrame) -> None:
    cols = [
        "ref",
        "pullback_pct",
        "tp_pct",
        "sl_pct",
        "fill_pct",
        "tp_first_pct_filled",
        "same_bar_pct_filled",
        "fully_conservative_bps_filled",
        "fully_conservative_bps_per_signal",
        "median_fill_delay_bars",
        "median_hit_after_fill_bars",
    ]
    lines = ["# Open limit context study", ""]
    lines.append("## Best base setups")
    lines.append("")
    lines.append(summary.head(20)[cols].round(4).to_markdown(index=False))
    lines.append("")
    lines.append("## Best single-feature filters on top setup")
    lines.append("")
    if filters.empty:
        lines.append("Nessun filtro.")
    else:
        lines.append(filters.head(25).round(4).to_markdown(index=False))
    lines.append("")
    lines.append("## Feature bins on top setup")
    lines.append("")
    if bins.empty:
        lines.append("Nessun bin.")
    else:
        show = bins.sort_values("edge_bps_filled", ascending=False).head(40)
        lines.append(show.round(4).to_markdown(index=False))
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    for ticker in load_tickers(args.tickers):
        path = args.data_dir / f"{ticker}.csv"
        if path.exists():
            data = load_rth(ticker, path)
            if not data.empty:
                frames.append(data)
    if not frames:
        raise SystemExit("No data")
    panel = pd.concat(frames, ignore_index=True)
    panel = panel[(panel["date"] >= pd.Timestamp(args.fromdate)) & (panel["date"] <= pd.Timestamp(args.todate))]
    bars = aggregate_5m(panel)
    signals = make_signals(bars, args.min_signal_bar, args.max_signal_bar, ints(args.lookbacks))
    print(f"Signals: {len(signals)}")
    refs = ["close"] + [f"high{x}" for x in ints(args.lookbacks)]
    tasks = [(ref, pb, tp, sl) for ref in refs for pb in floats(args.pullback_grid) for tp in floats(args.tp_grid) for sl in floats(args.sl_grid)]
    jobs = resolve_jobs(args.jobs)
    print(f"Running {len(tasks)} setup combinations with jobs={jobs}")
    if jobs <= 1:
        init_worker(signals)
        results = [task_summary(task) for task in tasks]
    else:
        with mp.Pool(processes=jobs, initializer=init_worker, initargs=(signals,)) as pool:
            results = pool.map(task_summary, tasks, chunksize=1)
    raw_summary = pd.DataFrame([r[0] for r in results])
    raw_summary.to_csv(args.out_dir / "summary_grid_all.csv", index=False)
    summary = raw_summary[
        (raw_summary["fill_pct"] >= 20)
        & (raw_summary["same_bar_pct_filled"] <= 1)
        & (raw_summary["fully_conservative_bps_filled"] > 0)
    ].sort_values("fully_conservative_bps_filled", ascending=False)
    summary.to_csv(args.out_dir / "summary_grid.csv", index=False)
    if summary.empty:
        raise SystemExit("No positive low-ambiguity setup")
    top = summary.iloc[0]
    top_idx = next(
        i for i, (s, _) in enumerate(results)
        if s["ref"] == top["ref"]
        and abs(s["pullback_pct"] - top["pullback_pct"]) < 1e-9
        and abs(s["tp_pct"] - top["tp_pct"]) < 1e-9
        and abs(s["sl_pct"] - top["sl_pct"]) < 1e-9
    )
    top_rows = results[top_idx][1]
    pd.DataFrame(top_rows).to_csv(args.out_dir / "top_setup_signals.csv", index=False)
    bins = feature_bins(top_rows, float(top["tp_pct"]) / 100, float(top["sl_pct"]) / 100)
    filters = filter_sweep(top_rows, float(top["tp_pct"]) / 100, float(top["sl_pct"]) / 100)
    bins.to_csv(args.out_dir / "top_setup_feature_bins.csv", index=False)
    filters.to_csv(args.out_dir / "top_setup_filter_sweep.csv", index=False)
    write_report(args.out_dir / "report.md", summary, bins, filters)
    print(f"Wrote {args.out_dir}")
    print(summary.head(10).round(4).to_string(index=False))
    print("\nTop filters")
    print(filters.head(10).round(4).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
