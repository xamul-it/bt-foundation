#!/usr/bin/env python3
"""
auction_fallback.py — fallback generico per ordini d'asta Alpaca scaduti.

Policy:
  - BUY CLS expired/parziale: reinvia residuo come LIMIT GTC extended-hours
    al close RTH, con buffer opzionale.
  - SELL OPG expired/parziale: reinvia residuo come MARKET DAY in RTH.

Lo script non conosce la strategia: filtra solo ordini Alpaca per side, TIF,
status, orario di submit e prefisso opzionale client_order_id.
"""

from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta, timezone
import logging
import math
import os
import sys
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("auction_fallback")

ET = ZoneInfo("America/New_York")


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


def _int_qty(value: float) -> int:
    if not math.isfinite(value):
        return 0
    return max(0, int(value))


def _get_trading_client():
    from alpaca.trading.client import TradingClient

    api_key = os.environ.get("ALPACA_API_KEY") or os.environ.get("BROKER_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("BROKER_SECRET_KEY")
    if not api_key or not secret_key:
        logger.error("Credenziali Alpaca non trovate")
        sys.exit(1)
    paper = str(api_key).startswith("PK")
    return TradingClient(api_key, secret_key, paper=paper), paper


def _get_data_client():
    from alpaca.data.historical import StockHistoricalDataClient

    api_key = os.environ.get("ALPACA_API_KEY") or os.environ.get("BROKER_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("BROKER_SECRET_KEY")
    client = StockHistoricalDataClient(api_key, secret_key)
    verify_ssl = os.environ.get("DISABLE_SSL_VERIFY", "").lower() not in {"true", "1", "yes"}
    client._session.verify = verify_ssl
    return client


def _alpaca_feed():
    from alpaca.data.enums import DataFeed

    feed = os.environ.get("ALPACA_DATA_FEED", "sip").strip().lower()
    return DataFeed.IEX if feed == "iex" else DataFeed.SIP


def _today_after_et(after_hhmm: str) -> datetime:
    hour_s, minute_s = after_hhmm.split(":", 1)
    now_et = datetime.now(ET)
    return datetime.combine(
        now_et.date(),
        time(int(hour_s), int(minute_s)),
        tzinfo=ET,
    )


def _rth_close_price(data_client, symbol: str, session_date, fallback_price: float = 0.0) -> float:
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    from alpaca.data.enums import Adjustment

    start = datetime.combine(session_date, time(15, 55), tzinfo=ET)
    end = datetime.combine(session_date, time(16, 1), tzinfo=ET)
    req = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame.Minute,
        start=start.astimezone(timezone.utc),
        end=end.astimezone(timezone.utc),
        adjustment=Adjustment.RAW,
        feed=_alpaca_feed(),
    )
    bars = data_client.get_stock_bars(req)
    df = getattr(bars, "df", None)
    if df is not None and not df.empty:
        try:
            sdf = df.xs(symbol, level=0) if getattr(df.index, "nlevels", 1) > 1 else df
            sdf = sdf.sort_index()
            et_index = sdf.index.tz_convert(ET)
            rth_close = sdf[et_index.strftime("%H:%M") == "15:59"]
            row = rth_close.iloc[-1] if not rth_close.empty else sdf.iloc[-1]
            price = float(row["close"])
            if price > 0 and math.isfinite(price):
                return price
        except Exception as exc:
            logger.warning("Close RTH non letto per %s: %s", symbol, exc)
    if fallback_price > 0:
        return fallback_price
    raise RuntimeError(f"close RTH non disponibile per {symbol}")


def _recent_orders(client, after_dt: datetime):
    from alpaca.trading.enums import QueryOrderStatus
    from alpaca.trading.requests import GetOrdersRequest

    req = GetOrdersRequest(
        status=QueryOrderStatus.ALL,
        after=after_dt.astimezone(timezone.utc),
        limit=500,
        nested=False,
    )
    return client.get_orders(filter=req)


def _client_id_exists(client, client_order_id: str, after_dt: datetime) -> bool:
    for order in _recent_orders(client, after_dt - timedelta(hours=1)):
        if str(getattr(order, "client_order_id", "") or "") == client_order_id:
            return True
    return False


def _positions_by_symbol(client) -> dict[str, int]:
    out = {}
    for pos in client.get_all_positions():
        qty = _int_qty(abs(float(pos.qty)))
        if qty > 0:
            out[str(pos.symbol).upper()] = qty
    return out


def _fallback_client_order_id(order, suffix: str) -> str:
    base = str(getattr(order, "client_order_id", "") or getattr(order, "id", "") or "")
    compact = "".join(ch for ch in base.lower() if ch.isalnum() or ch in {"_", "-"})
    compact = compact[:36] if compact else str(getattr(order, "symbol", "ord")).lower()
    return f"{compact}_{suffix}"[:48]


def _eligible(order, after_dt: datetime, prefix: str) -> bool:
    submitted_at = getattr(order, "submitted_at", None)
    if submitted_at is None or submitted_at < after_dt.astimezone(timezone.utc):
        return False
    if prefix:
        cid = str(getattr(order, "client_order_id", "") or "").lower()
        if not cid.startswith(prefix.lower()):
            return False
    status = _enum_value(getattr(order, "status", ""))
    if status != "expired":
        return False
    tif = _enum_value(getattr(order, "time_in_force", ""))
    side = _enum_value(getattr(order, "side", ""))
    return (side == "buy" and tif == "cls") or (side == "sell" and tif == "opg")


def run(
    *,
    after: str,
    client_id_prefix: str,
    cls_buffer_bps: float,
    dry_run: bool,
) -> None:
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.trading.requests import LimitOrderRequest, MarketOrderRequest

    client, paper = _get_trading_client()
    data_client = _get_data_client()
    after_dt = _today_after_et(after)
    mode = "PAPER" if paper else "LIVE"
    logger.info("[%s] Auction fallback after=%s prefix=%r", mode, after_dt, client_id_prefix)

    positions = _positions_by_symbol(client)
    orders = [o for o in _recent_orders(client, after_dt) if _eligible(o, after_dt, client_id_prefix)]
    if not orders:
        logger.info("Nessun ordine auction expired eleggibile")
        return

    submitted = skipped = failed = 0
    for order in sorted(orders, key=lambda o: getattr(o, "submitted_at", datetime.min.replace(tzinfo=timezone.utc))):
        symbol = str(getattr(order, "symbol", "") or "").upper()
        side = _enum_value(getattr(order, "side", ""))
        tif = _enum_value(getattr(order, "time_in_force", ""))
        qty = _int_qty(_float_attr(order, "qty"))
        filled = _int_qty(_float_attr(order, "filled_qty"))
        remaining = max(0, qty - filled)
        if remaining <= 0:
            skipped += 1
            continue

        if side == "sell":
            pos_qty = positions.get(symbol, 0)
            remaining = min(remaining, pos_qty)
            if remaining <= 0:
                logger.info("SKIP %s OPG: nessuna posizione corrente", symbol)
                skipped += 1
                continue

        suffix = "clsfb" if tif == "cls" else "opgfb"
        client_order_id = _fallback_client_order_id(order, suffix)
        if _client_id_exists(client, client_order_id, after_dt):
            logger.info("SKIP %s: fallback già presente client_order_id=%s", symbol, client_order_id)
            skipped += 1
            continue

        try:
            if side == "buy" and tif == "cls":
                fallback_price = _float_attr(order, "filled_avg_price", 0.0)
                close_price = _rth_close_price(
                    data_client,
                    symbol,
                    getattr(order, "submitted_at").astimezone(ET).date(),
                    fallback_price=fallback_price,
                )
                limit_price = round(close_price * (1.0 + cls_buffer_bps / 10000.0), 4)
                req = LimitOrderRequest(
                    symbol=symbol,
                    qty=remaining,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.GTC,
                    limit_price=limit_price,
                    extended_hours=True,
                    client_order_id=client_order_id,
                )
                description = f"BUY LIMIT GTC AH qty={remaining} limit={limit_price}"
            elif side == "sell" and tif == "opg":
                req = MarketOrderRequest(
                    symbol=symbol,
                    qty=remaining,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY,
                    client_order_id=client_order_id,
                )
                description = f"SELL MARKET DAY qty={remaining}"
            else:
                skipped += 1
                continue

            if dry_run:
                logger.info("DRY-RUN %s %s from order=%s", symbol, description, order.id)
                continue
            submitted_order = client.submit_order(req)
            logger.info(
                "SUBMITTED %s %s order_id=%s status=%s",
                symbol,
                description,
                submitted_order.id,
                submitted_order.status,
            )
            submitted += 1
        except Exception as exc:
            logger.error("FAILED %s %s %s: %s", symbol, side, tif, exc)
            failed += 1

    logger.info("Completato: %d inviati, %d skipped, %d falliti", submitted, skipped, failed)
    if failed:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--after",
        default="15:45",
        help="Ora ET minima submitted_at da considerare, formato HH:MM",
    )
    parser.add_argument(
        "--client-id-prefix",
        default="",
        help="Prefisso opzionale client_order_id da filtrare",
    )
    parser.add_argument(
        "--cls-buffer-bps",
        type=float,
        default=0.0,
        help="Buffer in bps sopra close per fallback BUY CLS",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(
        after=args.after,
        client_id_prefix=args.client_id_prefix,
        cls_buffer_bps=args.cls_buffer_bps,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
