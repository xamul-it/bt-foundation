#!/usr/bin/env python3
"""
submit_moo.py — Chiusura posizioni OvernightAH via MOO (Market On Open)
=======================================================================
Script standalone (nessun BT engine) da schedulare prima dell'apertura,
idealmente 09:25 ET, prima del cutoff OPG.
Legge le posizioni long aperte su Alpaca e sottomette ordini di vendita:
- MOO/OPG per la chiusura programmata.
- MARKET/DAY per il fallback post-open.

Uso:
    set -a; source /home/htpc/backtrader/env/pa2; set +a
    python3 bin/submit_moo.py [--dry-run]
"""

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import sys
import time
from zoneinfo import ZoneInfo

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
    paper = str(api_key).startswith('PK')
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


def _seconds_until_opg_window(now: datetime | None = None) -> float:
    eastern = ZoneInfo("America/New_York")
    now_et = (now or datetime.now(timezone.utc)).astimezone(eastern)
    open_start = now_et.replace(hour=19, minute=0, second=0, microsecond=0)
    cutoff = now_et.replace(hour=9, minute=28, second=0, microsecond=0)
    if now_et >= open_start or now_et < cutoff:
        return 0.0
    return (open_start - now_et).total_seconds()


def wait_for_opg_window(max_wait_minutes: int | None = None) -> None:
    seconds = _seconds_until_opg_window()
    if seconds <= 0:
        return
    wait_minutes = seconds / 60.0
    if max_wait_minutes is not None and wait_minutes > max_wait_minutes:
        logger.error(
            "Fuori finestra OPG: prossimo invio possibile tra %.1f minuti, oltre max_wait=%d",
            wait_minutes,
            max_wait_minutes,
        )
        sys.exit(1)
    logger.info("Fuori finestra OPG: attendo %.1f minuti fino alle 19:00 ET", wait_minutes)
    time.sleep(seconds)


def _status_value(status) -> str:
    return str(getattr(status, "value", status)).lower()


def _enum_value(value) -> str:
    return str(getattr(value, "value", value)).lower()


def _float_attr(obj, attr: str, default: float = 0.0) -> float:
    try:
        value = getattr(obj, attr, default)
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _entry_tif_matches(order, entry_tif: str) -> bool:
    tif = _enum_value(getattr(order, "time_in_force", ""))
    allowed = {x.strip().lower() for x in str(entry_tif or "cls").split(",") if x.strip()}
    if not allowed or "any" in allowed:
        return True
    return tif in allowed


def _is_overnight_entry_buy(order, strategy_prefix: str, entry_tif: str) -> bool:
    client_order_id = str(getattr(order, "client_order_id", "") or "").lower()
    if not client_order_id.startswith(strategy_prefix):
        return False
    if _enum_value(getattr(order, "side", "")) != "buy":
        return False
    if _status_value(getattr(order, "status", "")) not in {"filled", "partially_filled"}:
        return False
    return _entry_tif_matches(order, entry_tif)


def _recent_overnight_entry_qty(
    client,
    lookback_hours: int,
    strategy_prefix: str,
    entry_tif: str,
) -> dict[str, int]:
    from alpaca.trading.enums import QueryOrderStatus
    from alpaca.trading.requests import GetOrdersRequest

    after = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    req = GetOrdersRequest(
        status=QueryOrderStatus.ALL,
        after=after,
        limit=500,
        nested=False,
    )

    qty_by_symbol: dict[str, int] = {}
    for order in client.get_orders(filter=req):
        if not _is_overnight_entry_buy(order, strategy_prefix, entry_tif):
            continue
        symbol = str(getattr(order, "symbol", "") or "").upper()
        qty = int(_float_attr(order, "filled_qty", _float_attr(order, "qty", 0.0)))
        if symbol and qty > 0:
            qty_by_symbol[symbol] = qty_by_symbol.get(symbol, 0) + qty
    return qty_by_symbol


def _pending_sell_orders(client):
    from alpaca.trading.enums import OrderSide, QueryOrderStatus
    from alpaca.trading.requests import GetOrdersRequest

    after = datetime.now(timezone.utc) - timedelta(days=3)
    req = GetOrdersRequest(
        status=QueryOrderStatus.ALL,
        after=after,
        limit=500,
        nested=False,
    )
    pending = []
    for order in client.get_orders(filter=req):
        if order.side != OrderSide.SELL:
            continue
        if _status_value(order.status) in PENDING_ORDER_STATUSES:
            pending.append(order)
    return pending


def _pending_sell_symbols(client) -> set[str]:
    return {str(order.symbol) for order in _pending_sell_orders(client)}


def _cancel_pending_sell_orders(client) -> tuple[int, int]:
    pending_orders = _pending_sell_orders(client)
    cancelled = failed = 0
    if not pending_orders:
        return cancelled, failed

    logger.info(
        "Cancello sell order pendenti: %s",
        ", ".join(f"{order.symbol}:{order.id}" for order in pending_orders),
    )
    for order in pending_orders:
        try:
            client.cancel_order_by_id(order.id)
            logger.info("CANCEL submitted %s sell order_id=%s", order.symbol, order.id)
            cancelled += 1
        except Exception as exc:
            logger.error("CANCEL FAILED %s order_id=%s: %s", order.symbol, order.id, exc)
            failed += 1
    return cancelled, failed


def submit_moo(
    dry_run: bool = False,
    fallback_market: bool = False,
    close_all_longs: bool = False,
    cancel_pending_sells: bool = False,
    lookback_hours: int = 36,
    strategy_prefix: str = "bt_overnigh_",
    entry_tif: str = "cls",
    wait_window: bool = False,
    max_wait_minutes: int | None = None,
):
    if wait_window and not fallback_market:
        wait_for_opg_window(max_wait_minutes=max_wait_minutes)

    client, paper = get_client()
    mode = 'PAPER' if paper else 'LIVE'

    if fallback_market and cancel_pending_sells and not dry_run:
        cancelled, cancel_failed = _cancel_pending_sell_orders(client)
        if cancelled or cancel_failed:
            logger.info(
                "Cancel pendenti completata: %d richieste, %d fallite",
                cancelled, cancel_failed,
            )
            time.sleep(2)

    positions = client.get_all_positions()
    long_positions = {p.symbol: p for p in positions if float(p.qty) > 0}

    if not long_positions:
        logger.info("[%s] Nessuna posizione long aperta — nulla da fare", mode)
        return

    if close_all_longs:
        overnight_qty = {symbol: int(float(pos.qty)) for symbol, pos in long_positions.items()}
        logger.warning("[%s] Modalità --all-longs: verranno considerate tutte le long", mode)
    else:
        overnight_qty = _recent_overnight_entry_qty(
            client,
            lookback_hours=lookback_hours,
            strategy_prefix=strategy_prefix.lower(),
            entry_tif=entry_tif,
        )
        logger.info(
            "[%s] Quantità OvernightAH da entry recenti: %d simboli (lookback=%dh entry_tif=%s)",
            mode, len(overnight_qty), lookback_hours, entry_tif,
        )

    targets = []
    for symbol, entry_qty in sorted(overnight_qty.items()):
        pos = long_positions.get(symbol)
        if pos is None:
            logger.info("SKIP %s: nessuna posizione long corrente", symbol)
            continue
        current_qty = int(float(pos.qty))
        qty = min(current_qty, int(entry_qty))
        if qty <= 0:
            continue
        targets.append((pos, qty))

    if not targets:
        logger.info("[%s] Nessuna quantità OvernightAH da chiudere", mode)
        return

    target_label = "fallback market" if fallback_market else "MOO"
    logger.info("[%s] Target chiusura %s: %d", mode, target_label, len(targets))
    for pos, qty in targets:
        logger.info(
            "  %s: close_qty=%d current_qty=%s avg_entry=%.2f market_value=%s unrealized_pl=%s",
            pos.symbol, qty, pos.qty,
            float(pos.avg_entry_price),
            pos.market_value, pos.unrealized_pl,
        )

    pending_sells = set() if fallback_market and cancel_pending_sells else _pending_sell_symbols(client)
    if pending_sells:
        logger.info("Sell order pendenti presenti: %s", ", ".join(sorted(pending_sells)))

    submitted, failed, skipped = 0, 0, 0
    for pos, qty in targets:
        symbol = pos.symbol
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
    parser.add_argument(
        '--all-longs', action='store_true',
        help='Chiude tutte le long del conto.',
    )
    parser.add_argument(
        '--cancel-pending-sells', action='store_true',
        help='Prima del fallback market cancella eventuali sell pendenti.',
    )
    parser.add_argument(
        '--lookback-hours', type=int, default=36,
        help='Ore indietro in cui cercare ordini MOC OvernightAH filled',
    )
    parser.add_argument(
        '--strategy-prefix', default='bt_overnigh_',
        help='Prefisso client_order_id della strategia OvernightAH',
    )
    parser.add_argument(
        '--entry-tif', default='cls',
        help='TIF degli ordini entry da attribuire a OvernightAH: cls, gtc, day o any',
    )
    parser.add_argument(
        '--wait-window', action='store_true',
        help='Se fuori finestra OPG, attende fino alle 19:00 ET prima di inviare MOO',
    )
    parser.add_argument(
        '--max-wait-minutes', type=int, default=None,
        help='Limite massimo di attesa per --wait-window; se superato esce con errore',
    )
    args = parser.parse_args()
    submit_moo(
        dry_run=args.dry_run,
        fallback_market=args.fallback_market,
        close_all_longs=args.all_longs,
        cancel_pending_sells=args.cancel_pending_sells,
        lookback_hours=args.lookback_hours,
        strategy_prefix=args.strategy_prefix,
        entry_tif=args.entry_tif,
        wait_window=args.wait_window,
        max_wait_minutes=args.max_wait_minutes,
    )


if __name__ == '__main__':
    main()
