#!/usr/bin/env python3
"""
submit_moo.py — Chiusura posizioni OvernightAH via MOO (Market On Open)
=======================================================================
Script standalone (nessun BT engine) da schedulare prima dell'apertura,
idealmente 09:25 ET, prima del cutoff OPG.
Legge le posizioni long aperte su Alpaca e sottomette un ordine
MOO (OPG) di vendita per ciascuna.

Uso:
    set -a; source /home/htpc/backtrader/env/pa2; set +a
    python3 bin/overnight_ah/submit_moo.py [--dry-run]
"""

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('submit_moo')


def get_client():
    from alpaca.trading.client import TradingClient
    api_key    = os.environ.get('ALPACA_API_KEY') or os.environ.get('BROKER_API_KEY')
    secret_key = os.environ.get('ALPACA_SECRET_KEY') or os.environ.get('BROKER_SECRET_KEY')
    if not api_key or not secret_key:
        logger.error("Credenziali Alpaca non trovate (ALPACA_API_KEY / ALPACA_SECRET_KEY)")
        sys.exit(1)
    paper = os.environ.get('ALPACA_API_KEY', '').startswith('PK')
    verify_ssl = os.environ.get('DISABLE_SSL_VERIFY', '').lower() not in ('true', '1', 'yes')
    return TradingClient(api_key, secret_key, paper=paper, url_override=None), paper


PENDING_ORDER_STATUSES = {
    "accepted",
    "new",
    "pending_new",
    "partially_filled",
    "pending_replace",
    "pending_cancel",
    "accepted_for_bidding",
}


def _status_value(status) -> str:
    return str(getattr(status, "value", status)).lower()


def _pending_sell_symbols(client) -> set[str]:
    from alpaca.trading.enums import OrderSide, QueryOrderStatus
    from alpaca.trading.requests import GetOrdersRequest

    after = datetime.now(timezone.utc) - timedelta(days=3)
    req = GetOrdersRequest(
        status=QueryOrderStatus.ALL,
        after=after,
        limit=500,
        nested=False,
    )
    pending = set()
    for order in client.get_orders(filter=req):
        if order.side != OrderSide.SELL:
            continue
        if _status_value(order.status) in PENDING_ORDER_STATUSES:
            pending.add(order.symbol)
    return pending


def submit_moo(dry_run: bool = False, fallback_market: bool = False):
    client, paper = get_client()
    mode = 'PAPER' if paper else 'LIVE'

    positions = client.get_all_positions()
    long_positions = [p for p in positions if float(p.qty) > 0]

    if not long_positions:
        logger.info("[%s] Nessuna posizione long aperta — nulla da fare", mode)
        return

    logger.info("[%s] Posizioni long trovate: %d", mode, len(long_positions))
    for pos in long_positions:
        logger.info(
            "  %s: qty=%s avg_entry=%.2f market_value=%s unrealized_pl=%s",
            pos.symbol, pos.qty,
            float(pos.avg_entry_price),
            pos.market_value, pos.unrealized_pl,
        )

    pending_sells = _pending_sell_symbols(client)
    if pending_sells:
        logger.info("Sell order pendenti presenti: %s", ", ".join(sorted(pending_sells)))

    submitted, failed, skipped = 0, 0, 0
    for pos in long_positions:
        symbol = pos.symbol
        qty    = int(float(pos.qty))
        if qty <= 0:
            continue
        if symbol in pending_sells:
            logger.info("SKIP %s: sell order già pendente", symbol)
            skipped += 1
            continue

        if dry_run:
            kind = "MARKET DAY" if fallback_market else "MOO OPG"
            logger.info("DRY-RUN: %s sell %s qty=%d (non inviato)", kind, symbol, qty)
            continue

        try:
            from alpaca.trading.requests import MarketOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce

            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY if fallback_market else TimeInForce.OPG,
            )
            order = client.submit_order(req)
            logger.info(
                "%s submitted %s: qty=%d order_id=%s status=%s",
                "MARKET fallback" if fallback_market else "MOO",
                symbol, qty, order.id, order.status,
            )
            submitted += 1
        except Exception as exc:
            logger.error("MOO FAILED %s: %s", symbol, exc)
            failed += 1

    logger.info(
        "Completato: %d inviati, %d skipped, %d falliti",
        submitted, skipped, failed,
    )
    if failed:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Mostra cosa verrebbe inviato senza inviare ordini',
    )
    parser.add_argument(
        '--fallback-market', action='store_true',
        help='Invia market sell DAY per posizioni ancora aperte senza sell pendente',
    )
    args = parser.parse_args()
    submit_moo(dry_run=args.dry_run, fallback_market=args.fallback_market)


if __name__ == '__main__':
    main()
