#!/usr/bin/env python3
"""Compare Backtrader earnings-filter runs and write decision tables."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "out/overnight_earnings_20260910"
BT_OUT = ROOT / "bt-core/out/research/OvernightAHFlatCompositeEarningsResearch"
PAIRS = {"fixed": ("fixed_off", "fixed_on"), "operational": ("operational_off", "operational_on")}
PERIODS = {
    "full": ("2000-01-03", "2026-09-09"),
    "reliable_2013_plus": ("2013-01-01", "2026-09-09"),
    "recent_through_2025": ("2020-01-01", "2025-12-31"),
    "recent_including_partial_2026": ("2020-01-01", "2026-09-09"),
}


def run_dir(case: str) -> Path:
    return BT_OUT / f"earnings_20260910_{case}"


def metrics(case: str) -> dict:
    returns = pd.read_csv(run_dir(case) / "returns.csv", parse_dates=["index"]).set_index("index")["return"].fillna(0)
    equity = (1 + returns).cumprod()
    years = (returns.index.max() - returns.index.min()).days / 365.2425
    result = next(iter(json.loads((run_dir(case) / "results.json").read_text()).values()))
    return {
        "case": case,
        "start": returns.index.min().date().isoformat(),
        "end": returns.index.max().date().isoformat(),
        "observations": len(returns),
        "trades": int(result["trades"]),
        "total_return": float(equity.iloc[-1] - 1),
        "cagr": float(equity.iloc[-1] ** (1 / years) - 1),
        "sharpe_daily": float(np.sqrt(252) * returns.mean() / returns.std(ddof=1)),
        "max_drawdown": float((equity / equity.cummax() - 1).min()),
        "sqn_reported": float(result["SQN"]),
        "sharpe_reported": float(result["Sharpe"]),
    }


def event_windows() -> dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]]:
    events = pd.read_csv(OUT / "earnings_events.csv")
    windows: dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
    for row in events.itertuples():
        event = pd.Timestamp(row.earnings_datetime_et)
        windows.setdefault(row.symbol, []).append((event - pd.Timedelta(hours=36), event))
    return windows


def is_event_entry(symbol: str, day: str, windows) -> bool:
    # The decision/entry is at 16:00 ET on the Backtrader open date.
    entry = pd.Timestamp(f"{day[:10]} 16:00", tz="America/New_York")
    return any(start <= entry <= event for start, event in windows.get(symbol, ()))


def pair_details(label: str, off_case: str, on_case: str, windows) -> dict:
    def trades(case):
        frame = pd.DataFrame(json.loads((run_dir(case) / "trades.json").read_text()))
        frame["day"] = frame.open_datetime.str[:10]
        return frame

    off, on = trades(off_case), trades(on_case)
    off_sets = off.groupby("day").asset.apply(set)
    on_sets = on.groupby("day").asset.apply(set)
    days = sorted(set(off_sets.index) | set(on_sets.index))
    changed = [day for day in days if off_sets.get(day, set()) != on_sets.get(day, set())]
    event_rows = off[off.apply(lambda row: is_event_entry(row.asset, row.day, windows), axis=1)].copy()
    removed = event_rows.merge(on[["day", "asset"]], on=["day", "asset"], how="left", indicator=True)
    removed = removed[removed._merge == "left_only"]

    roff = pd.read_csv(run_dir(off_case) / "returns.csv", parse_dates=["index"]).set_index("index")["return"]
    ron = pd.read_csv(run_dir(on_case) / "returns.csv", parse_dates=["index"]).set_index("index")["return"]
    paired = pd.concat([roff.rename("off"), ron.rename("on")], axis=1).dropna()
    paired["delta"] = paired["on"] - paired["off"]
    annual = (1 + paired[["off", "on"]]).groupby(paired.index.year).prod() - 1
    annual["delta"] = annual["on"] - annual["off"]
    annual.to_csv(OUT / f"{label}_annual.csv", index_label="year")
    return {
        "changed_entry_days": len(changed),
        "off_trades_in_36h_event_window": len(event_rows),
        "event_window_trades_removed": len(removed),
        "event_window_trade_mean_pnl_pct": float(event_rows.pnl_pct.mean()),
        "event_window_trade_median_pnl_pct": float(event_rows.pnl_pct.median()),
        "event_window_trade_win_rate": float((event_rows.pnl_pct > 0).mean()),
        "on_better_daily_share": float((paired.delta > 0).mean()),
        "on_better_years": int((annual.delta > 0).sum()),
        "years_compared": len(annual),
        "mean_daily_delta": float(paired.delta.mean()),
    }


def period_metrics(case: str, start: str, end: str) -> dict:
    returns = pd.read_csv(run_dir(case) / "returns.csv", parse_dates=["index"]).set_index("index")["return"].fillna(0)
    returns = returns.loc[start:end]
    equity = (1 + returns).cumprod()
    years = (returns.index.max() - returns.index.min()).days / 365.2425
    return {
        "total_return": float(equity.iloc[-1] - 1),
        "cagr": float(equity.iloc[-1] ** (1 / years) - 1),
        "sharpe_daily": float(np.sqrt(252) * returns.mean() / returns.std(ddof=1)),
        "max_drawdown": float((equity / equity.cummax() - 1).min()),
    }


def main() -> None:
    windows = event_windows()
    rows, details = [], {}
    for label, (off_case, on_case) in PAIRS.items():
        off, on = metrics(off_case), metrics(on_case)
        rows += [off, on]
        details[label] = pair_details(label, off_case, on_case, windows)
    table = pd.DataFrame(rows)
    table.to_csv(OUT / "backtest_summary.csv", index=False)
    periods = {
        label: {
            period: {
                "off": period_metrics(off_case, start, end),
                "on": period_metrics(on_case, start, end),
            }
            for period, (start, end) in PERIODS.items()
        }
        for label, (off_case, on_case) in PAIRS.items()
    }
    payload = {"metrics": rows, "paired_details": details, "periods": periods}
    (OUT / "backtest_analysis.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
