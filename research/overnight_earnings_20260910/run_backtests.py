#!/usr/bin/env python3
"""Run reproducible Backtrader earnings-filter comparisons."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BT_CORE = ROOT / "bt-core"
PYTHON = BT_CORE / ".venv/bin/python"
CONFIG = ROOT / "config-common/scheduled/strategies/overnight-ah-challenger.env"
EVENTS = ROOT / "out/overnight_earnings_20260910/earnings_events.csv"
STRATEGY = "research.OvernightAHFlatCompositeEarningsResearch"


def resolved_config() -> dict[str, str]:
    command = f"set -a; source {CONFIG}; env -0"
    raw = subprocess.check_output(["bash", "-lc", command])
    return dict(item.split("=", 1) for item in raw.decode().split("\0") if "=" in item)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("case", choices=["fixed_off", "fixed_on", "operational_off", "operational_on"])
    args = parser.parse_args()
    cfg = resolved_config()
    enabled = args.case.endswith("_on")
    fixed = args.case.startswith("fixed_")
    overrides = [
        "auction=True",
        f"earnings_skip={enabled}",
        f"earnings_history_file='{EVENTS}'",
    ]
    if fixed:
        # Disable the equity-sized hedge too, otherwise this is not a true
        # fixed-notional isolation even when stock entries are fixed.
        overrides += [
            "size_by_max_concurrent=False",
            "fixed_notional_per_trade=20000",
            "hedge_enabled=False",
        ]
    stratargs = cfg["STRATARGS"] + " " + " ".join(overrides)
    run_id = f"earnings_20260910_{args.case}"
    command = [
        str(PYTHON), "btmain.py",
        "--strat", STRATEGY,
        "--ticker", cfg["TICKER"],
        "--stratargs", stratargs,
        "--timeframe", "daily",
        "--provider", cfg["DATA_PROVIDER"],
        "--fromdate", "2000-01-03",
        # btmain's daily slicer includes the following midnight boundary.
        # Using 2026-09-08 therefore includes the completed 2026-09-09 bar
        # needed to close the final overnight trade, without the partial
        # 2026-09-10 session.
        "--todate", "2026-09-08",
        "--mode", "backtest",
        "--margin-leverage", cfg["MARGIN_LEVERAGE"],
        "--commission", "alpaca",
        "--cash", "50000",
        "--id", run_id,
    ]
    out = ROOT / "out/overnight_earnings_20260910"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{args.case}_command.json").write_text(json.dumps(command, indent=2), encoding="utf-8")
    with (out / f"{args.case}.log").open("w", encoding="utf-8") as log:
        completed = subprocess.run(command, cwd=BT_CORE, stdout=log, stderr=subprocess.STDOUT)
    if completed.returncode:
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
