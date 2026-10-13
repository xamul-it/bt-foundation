#!/usr/bin/env python3
"""Expose what the existing withdrawal sweeps actually measure.

This is deliberately a diagnostic table, not an estimate for the requested
fixed-sizing / idle-cash model.  Those runs reset capital with withdrawals.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
STUDY = ROOT / "out/overnight_ah_capital_study_20260909"


def main() -> None:
    source = pd.read_csv(STUDY / "summary.csv")
    data = source[
        source["case"].str.match(r"^capital_band20_(legacy|composite)_")
        & (source["capital"] != 30_000_000)
    ].copy()
    years = data["years"].astype(float)
    data["ending_wealth"] = data["final_equity"] + data["total_withdrawals"]
    data["net_pnl"] = data["ending_wealth"] - data["capital"]
    data["net_pnl_pct_of_initial"] = data["net_pnl"] / data["capital"]
    data["simple_avg_annual_pnl"] = data["net_pnl"] / years
    data["simple_avg_annual_pnl_pct"] = data["simple_avg_annual_pnl"] / data["capital"]
    data["withdrawal_share_of_ending_wealth"] = data["total_withdrawals"] / data["ending_wealth"]
    data["max_equity_ratio"] = data.groupby("case")["case"].transform(lambda x: 0.0)

    # The saved ledger is the authoritative daily record for the account
    # after the strategy's withdrawal/reset policy.
    max_equity, max_wealth = [], []
    for case in data["case"]:
        ledger = pd.read_csv(STUDY / "cases" / case / "ledger.csv")
        initial = float(ledger["equity_start"].iloc[0])
        max_equity.append(float(ledger["equity_end"].max()) / initial)
        max_wealth.append(float(ledger["wealth"].max()) / initial)
    data["max_equity_ratio_after_withdrawals"] = max_equity
    data["max_wealth_ratio_including_withdrawals"] = max_wealth

    columns = [
        "case", "strategy", "capital", "years", "sharpe",
        "final_equity", "total_withdrawals", "ending_wealth", "net_pnl",
        "net_pnl_pct_of_initial", "simple_avg_annual_pnl",
        "simple_avg_annual_pnl_pct", "avg_equity_ratio", "min_equity_ratio",
        "max_equity_ratio_after_withdrawals", "max_wealth_ratio_including_withdrawals",
    ]
    data[columns].sort_values(["strategy", "capital"]).to_csv(
        STUDY / "existing_withdrawal_sweep_diagnostic.csv", index=False
    )


if __name__ == "__main__":
    main()
