#!/usr/bin/env python3
"""Summarize fixed-sizing, idle-cash runs without compounding returns."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
STUDY = ROOT / "out/overnight_ah_capital_study_20260909"


def main() -> None:
    rows = []
    for case in sorted((STUDY / "idle_cash_cases").glob("idle_cash_*")):
        if not (case / "complete.json").exists():
            continue
        config = pd.read_json(case / "config.json", typ="series")
        capital = float(config["options"]["cash"])
        strategy = config["options"]["strategy"]
        ledger = pd.read_csv(case / "ledger.csv", parse_dates=["date"])
        years = (ledger.date.iloc[-1] - ledger.date.iloc[0]).days / 365.25
        pnl = float(ledger.cumulative_pnl.iloc[-1])
        daily = ledger.return_on_initial_capital.astype(float)
        rows.append(
            dict(
                case=case.name,
                strategy=strategy,
                capital=capital,
                start=ledger.date.iloc[0].date().isoformat(),
                end=ledger.date.iloc[-1].date().isoformat(),
                years=years,
                ending_equity=float(ledger.equity_end.iloc[-1]),
                net_pnl=pnl,
                total_noncompound_gain_pct=pnl / capital,
                avg_annual_pnl=pnl / years,
                avg_annual_simple_gain_pct=pnl / capital / years,
                sharpe_pnl_over_initial=np.sqrt(252) * daily.mean() / daily.std(ddof=1),
                min_equity=float(ledger.equity_end.min()),
                max_equity=float(ledger.equity_end.max()),
                max_inactive_cash=float(ledger.cash_inactive.max()),
            )
        )
    pd.DataFrame(rows).sort_values(["strategy", "capital"]).to_csv(
        STUDY / "idle_cash_summary.csv", index=False
    )


if __name__ == "__main__":
    main()
