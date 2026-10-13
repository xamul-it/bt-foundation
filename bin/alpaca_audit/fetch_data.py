#!/usr/bin/env python3
"""
Scarica dati portafoglio e ordini da Alpaca e li salva in out/.

Usage:
    cd /home/htpc/backtrader
    source bt-core/.venv/bin/activate
    set -a; source env/pa2; set +a
    python bin/alpaca_audit/fetch_data.py [--days 180]
"""
import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus

BASE_OUT = Path(__file__).parent / "out"


def get_out_dir(account: str) -> Path:
    d = BASE_OUT / account
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_client(paper: bool = True):
    key = os.environ.get("ALPACA_API_KEY") or os.environ.get("BROKER_API_KEY")
    secret = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("BROKER_SECRET_KEY")
    if not key or not secret:
        sys.exit("ERROR: credenziali mancanti. Esegui: set -a; source env/paX; set +a")
    return TradingClient(api_key=key, secret_key=secret, paper=paper)


def fetch_account(client):
    acct = client.get_account()
    data = {
        "equity": float(acct.equity),
        "cash": float(acct.cash),
        "buying_power": float(acct.buying_power),
        "long_market_value": float(acct.long_market_value),
        "short_market_value": float(acct.short_market_value),
        "portfolio_value": float(acct.portfolio_value),
        "initial_margin": float(acct.initial_margin),
        "maintenance_margin": float(acct.maintenance_margin),
        "last_equity": float(acct.last_equity),
        "daytrade_count": int(acct.daytrade_count),
        "pattern_day_trader": bool(acct.pattern_day_trader),
        "account_blocked": bool(acct.account_blocked),
        "trade_suspended_by_user": bool(acct.trade_suspended_by_user),
        "multiplier": str(acct.multiplier),
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    return data


def fetch_portfolio_history(client, days=180):
    """
    Alpaca restituisce l'equity curve del conto.
    Nota: period può essere '1D','1W','1M','3M','6M','1A'.
    Usiamo timeframe='1D' per barre giornaliere.
    """
    from alpaca.trading.requests import GetPortfolioHistoryRequest

    # calcola date
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    req = GetPortfolioHistoryRequest(
        period=f"{days}D",
        timeframe="1D",
        intraday_reporting="market_hours",
        pnl_reset="per_day",
    )
    hist = client.get_portfolio_history(req)

    timestamps = hist.timestamp  # lista di unix timestamp
    equity = hist.equity
    profit_loss = hist.profit_loss
    profit_loss_pct = hist.profit_loss_pct

    rows = []
    for ts, eq, pl, pl_pct in zip(timestamps, equity, profit_loss, profit_loss_pct):
        rows.append({
            "date": datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat(),
            "equity": eq,
            "profit_loss": pl,
            "profit_loss_pct": pl_pct,
        })
    return pd.DataFrame(rows)


def fetch_orders(client, days=365):
    """Scarica TUTTI gli ordini con paginazione (max 500 per chiamata API)."""
    after = datetime.now(timezone.utc) - timedelta(days=days)
    all_orders = []
    until = None
    batch_num = 0

    while True:
        batch_num += 1
        if until is None:
            req = GetOrdersRequest(status=QueryOrderStatus.ALL, after=after, limit=500)
        else:
            req = GetOrdersRequest(status=QueryOrderStatus.ALL, after=after, until=until, limit=500)

        batch = client.get_orders(req)
        if not batch:
            break

        all_orders.extend(batch)
        print(f"  batch {batch_num}: {len(batch)} ordini  [{min(o.created_at for o in batch).date()} → {max(o.created_at for o in batch).date()}]")

        if len(batch) < 500:
            break

        until = min(o.created_at for o in batch)

    rows = []
    for o in all_orders:
        rows.append({
            "id": str(o.id),
            "symbol": o.symbol,
            "side": o.side.value if o.side else None,
            "type": o.order_type.value if o.order_type else None,
            "qty": float(o.qty) if o.qty else None,
            "filled_qty": float(o.filled_qty) if o.filled_qty else None,
            "filled_avg_price": float(o.filled_avg_price) if o.filled_avg_price else None,
            "status": o.status.value if o.status else None,
            "created_at": o.created_at.isoformat() if o.created_at else None,
            "filled_at": o.filled_at.isoformat() if o.filled_at else None,
            "canceled_at": o.canceled_at.isoformat() if o.canceled_at else None,
            "time_in_force": o.time_in_force.value if o.time_in_force else None,
            "extended_hours": bool(o.extended_hours),
        })
    return pd.DataFrame(rows)


def fetch_positions(client):
    positions = client.get_all_positions()
    rows = []
    for p in positions:
        rows.append({
            "symbol": p.symbol,
            "qty": float(p.qty),
            "side": p.side.value if p.side else None,
            "avg_entry_price": float(p.avg_entry_price),
            "market_value": float(p.market_value),
            "cost_basis": float(p.cost_basis),
            "unrealized_pl": float(p.unrealized_pl),
            "unrealized_plpc": float(p.unrealized_plpc),
            "current_price": float(p.current_price),
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=180, help="Giorni di storia (default 180)")
    parser.add_argument("--account", type=str, default="default",
                        help="Nome account (es. pa1, pa2) — usato come sottodirectory di out/")
    parser.add_argument("--live", action="store_true", help="Usa endpoint live invece di paper")
    args = parser.parse_args()

    OUT = get_out_dir(args.account)

    client = get_client(paper=not args.live)
    print(f"Account: {args.account}  |  {'live' if args.live else 'paper'}  |  {args.days} giorni\n")

    # Account snapshot
    acct = fetch_account(client)
    acct_path = OUT / "account_snapshot.json"
    with open(acct_path, "w") as f:
        json.dump(acct, f, indent=2)
    print(f"Account snapshot → {acct_path}")
    print(f"  equity={acct['equity']:.2f}  cash={acct['cash']:.2f}  buying_power={acct['buying_power']:.2f}")
    if acct["cash"] < 0:
        print(f"  ⚠️  CASH NEGATIVO: {acct['cash']:.2f} (probabile margine usato)")
    if acct["buying_power"] < 0:
        print(f"  ⚠️  BUYING POWER NEGATIVO: {acct['buying_power']:.2f}")

    # Portfolio history
    print("\nScarico portfolio history...")
    hist = fetch_portfolio_history(client, args.days)
    hist_path = OUT / "portfolio_history.csv"
    hist.to_csv(hist_path, index=False)
    print(f"Portfolio history → {hist_path} ({len(hist)} righe)")

    # Orders
    print("\nScarico ordini...")
    orders = fetch_orders(client, args.days)
    orders_path = OUT / "orders.csv"
    orders.to_csv(orders_path, index=False)
    print(f"Ordini → {orders_path} ({len(orders)} righe)")

    # Positions
    print("\nScarico posizioni aperte...")
    pos = fetch_positions(client)
    pos_path = OUT / "positions.csv"
    pos.to_csv(pos_path, index=False)
    print(f"Posizioni → {pos_path} ({len(pos)} righe)")

    print("\nFatto. Esegui ora: python bin/alpaca_audit/analyze.py")


if __name__ == "__main__":
    main()
