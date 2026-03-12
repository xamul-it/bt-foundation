#!/usr/bin/env python3
"""Sincronizza le posizioni Alpaca in broker-position.json per broker shadow/backtest."""

import argparse
from datetime import date
from pathlib import Path
import os
import sys
import json

from alpaca.trading.client import TradingClient


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sync Alpaca account positions into config/broker-position.json"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: backtrader/config/broker-position.json)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use live Alpaca account (default: paper)",
    )
    parser.add_argument(
        "--asof",
        default=None,
        help="Override as-of date (YYYY-MM-DD). Default: today.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        print("Missing ALPACA_API_KEY/ALPACA_SECRET_KEY in environment", file=sys.stderr)
        return 2

    paper = not args.live
    client = TradingClient(api_key, secret_key, paper=paper)

    try:
        account = client.get_account()
        positions = client.get_all_positions()
    except Exception as exc:
        print(f"Failed to fetch Alpaca data: {exc}", file=sys.stderr)
        return 1

    asof = args.asof or date.today().isoformat()
    try:
        cash = float(account.cash)
    except Exception:
        cash = 0.0

    positions_dict = {}
    for pos in positions:
        symbol = getattr(pos, "symbol", None)
        if not symbol:
            continue

        try:
            qty = float(pos.qty)
        except Exception:
            continue

        side = getattr(pos, "side", "long")
        if str(side).lower() == "short":
            qty = -abs(qty)

        price_val = None
        for field in ("avg_entry_price", "current_price"):
            val = getattr(pos, field, None)
            if val is not None:
                try:
                    price_val = float(val)
                    break
                except Exception:
                    pass

        if price_val is None:
            continue

        positions_dict[str(symbol)] = {"size": qty, "price": price_val}

    if args.output:
        output_path = Path(args.output)
    else:
        repo_root = Path(__file__).resolve().parents[1]
        output_path = repo_root / "config" / "broker-position.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    state = {"asof": asof, "cash": float(cash), "positions": positions_dict}
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    print(f"Wrote {output_path}")
    print(f"Positions: {len(positions_dict)}  Cash: {cash}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
