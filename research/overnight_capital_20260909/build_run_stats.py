#!/usr/bin/env python3
"""Build per-run QuantStats reports from the saved Backtrader return series."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import quantstats as qs


ROOT = Path(__file__).resolve().parents[2]


def build_report(path: Path) -> None:
    returns_path = path / "returns.csv"
    if not returns_path.exists():
        raise FileNotFoundError(returns_path)
    frame = pd.read_csv(returns_path, parse_dates=["index"])
    raw_returns = pd.Series(frame["return"].astype(float).to_numpy(), index=frame["index"])
    title = f"Overnight AH — {path.name}"
    # The raw Backtrader TimeReturn series treats each withdrawal as a loss
    # from the remaining account.  It is useful as an account-balance view,
    # but not as the strategy's return series.  The study ledger restores the
    # withdrawal to daily P&L before calculating the time-weighted return.
    ledger_path = ROOT / "out/overnight_ah_capital_study_20260909/cases" / path.name / "ledger.csv"
    if ledger_path.exists():
        ledger = pd.read_csv(ledger_path, parse_dates=["date"])
        returns = pd.Series(
            ledger["return_adjusted"].astype(float).to_numpy(), index=ledger["date"]
        )
        qs.reports.html(
            raw_returns,
            output=str(path / "stats_account_after_withdrawals.html"),
            title=f"{title} — conto residuo dopo i prelievi",
        )
    else:
        returns = raw_returns
    qs.reports.html(returns, output=str(path / "stats.html"), title=title)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--include-stress",
        action="store_true",
        help="Include the $30M stress runs excluded from the capital study.",
    )
    args = parser.parse_args()

    patterns = [
        ROOT / "out/overnight_ah/OvernightAH/capital_band20_legacy_*",
        ROOT / "out/overnight_ah_flat_composite/OvernightAHFlatComposite/capital_band20_composite_*",
    ]
    paths = sorted(path for pattern in patterns for path in ROOT.glob(str(pattern.relative_to(ROOT))))
    if not args.include_stress:
        paths = [path for path in paths if not path.name.endswith("_30000000")]
    for path in paths:
        build_report(path)
        print(path / "stats.html")


if __name__ == "__main__":
    main()
