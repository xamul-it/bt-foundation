#!/usr/bin/env python3
"""Ingest closed trades already written by a scheduled Backtest run.

This command only reads the supplied ``trades.json`` and persists matching
pending entries.  It does not invoke Backtrader and is not a replay command.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
sys.path.insert(0, str(BT_CORE))
import watchtower_runtime as wr  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--trades-path", required=True)
    args = parser.parse_args(argv)
    trading_date = date.fromisoformat(args.trading_date)
    path = Path(args.trades_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("trades.json must contain a list")
    updated = wr.WatchtowerRepository().ingest_scheduled_backtest_settlements(
        args.profile, trading_date, payload,
    )
    print(json.dumps({"profile": args.profile, "trading_date": args.trading_date, "updated": updated}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
