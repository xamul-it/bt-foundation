#!/usr/bin/env python3
"""Alpaca reconciliation wrapper with EOD-compatible exports.

Genera:
- order_alpaca.csv
- trade_alpaca.csv
- position_alpaca.csv
- raw dumps Alpaca
- output compatibili EOD: paper_alpaca/orderhistory.json e trades.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Alpaca reconciliation + EOD compatible export")
    p.add_argument("--folder", default=None, help="Base folder containing ./sim and optional ./paper")
    p.add_argument("--sim", default=None, help="Fallback SIM folder when --folder is not used")
    p.add_argument("--paper-mode", choices=["paper", "live"], default="paper", help="Alpaca account mode")
    p.add_argument("--output-dir", default=None, help="Output dir (default: out/reconcile/alpaca/<timestamp>)")
    p.add_argument("--emit-eod-compatible", action="store_true", default=True,
                   help="Emit EOD compatible files (default: true)")
    p.add_argument("--no-emit-eod-compatible", action="store_false", dest="emit_eod_compatible",
                   help="Disable EOD compatible output")
    p.add_argument("--run-eod", action="store_true", help="Run eod_analysis.py against generated paper_alpaca")
    p.add_argument("--bar-seconds", type=int, default=60, help="Bar duration in seconds")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()


def _log(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(msg)


def _to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v.astimezone(UTC)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=UTC)
        except Exception:
            continue
    return None


def _fmt_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _minute_bucket(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M")


def _status_to_bt(status: str) -> str:
    s = (status or "").lower()
    if s == "filled":
        return "Completed"
    if s in ("partially_filled", "partial_fill", "partial"):
        return "Partially Filled"
    if s in ("canceled", "cancelled"):
        return "Canceled"
    if s in ("rejected",):
        return "Rejected"
    if s in ("expired",):
        return "Expired"
    if s in ("accepted", "new", "pending_new", "pending_replace", "pending_cancel"):
        return "Accepted"
    return status or "Unknown"


def _obj_to_dict(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, datetime):
        return obj.astimezone(UTC).isoformat()
    if isinstance(obj, list):
        return [_obj_to_dict(x) for x in obj]
    if isinstance(obj, tuple):
        return [_obj_to_dict(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _obj_to_dict(v) for k, v in obj.items()}
    if hasattr(obj, "model_dump"):
        return _obj_to_dict(obj.model_dump())
    if hasattr(obj, "dict"):
        try:
            return _obj_to_dict(obj.dict())
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        raw = {k: v for k, v in vars(obj).items() if not k.startswith("_")}
        return _obj_to_dict(raw)
    return str(obj)


def _scan_dt_values(node: Any, keys: set[str], out: List[datetime]) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(k, str) and k in keys:
                dt = _parse_dt(v)
                if dt:
                    out.append(dt)
            _scan_dt_values(v, keys, out)
    elif isinstance(node, list):
        for x in node:
            _scan_dt_values(x, keys, out)


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def infer_window_from_inputs(sim_dir: Path, paper_dir: Optional[Path]) -> Tuple[datetime, datetime, Dict[str, int]]:
    date_keys = {
        "signal_dt", "datetime", "lastdatetime", "exec_dt", "open_datetime", "close_datetime", "dt",
        "submitted_at", "filled_at", "updated_at",
    }
    candidates = []
    sources = {
        "sim": [sim_dir / "orderhistory.json", sim_dir / "trades.json"],
        "paper": [],
    }
    if paper_dir:
        sources["paper"] = [paper_dir / "orderhistory.json", paper_dir / "trades.json"]

    counts: Dict[str, int] = {"sim": 0, "paper": 0}
    for tag, files in sources.items():
        for fp in files:
            data = _load_json_if_exists(fp)
            if data is None:
                continue
            before = len(candidates)
            _scan_dt_values(data, date_keys, candidates)
            counts[tag] += len(candidates) - before

    if not candidates:
        raise RuntimeError("Unable to infer date window from input files (orderhistory/trades)")

    start_dt = min(candidates)
    end_dt = max(candidates)
    return start_dt, end_dt, counts


def get_alpaca_client(mode: str) -> Any:
    from alpaca.trading.client import TradingClient

    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY")
    if not api_key or not secret_key:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY")
    paper = mode == "paper"
    client = TradingClient(api_key, secret_key, paper=paper)
    if hasattr(client, "_session") and getattr(client, "_session", None) is not None:
        try:
            client._session.verify = False
        except Exception:
            pass
    return client


def fetch_alpaca_orders(client: Any, start_dt: datetime, end_dt: datetime) -> List[Any]:
    from alpaca.trading.enums import QueryOrderStatus
    from alpaca.trading.requests import GetOrdersRequest
    from alpaca.common.enums import Sort

    def _submitted_dt(order_obj: Any) -> Optional[datetime]:
        return _parse_dt(getattr(order_obj, "submitted_at", None) or getattr(order_obj, "created_at", None))

    until_dt = end_dt + timedelta(seconds=1)
    cursor_after = start_dt
    seen_ids: set[str] = set()
    out: List[Any] = []
    batch_limit = 500

    for _ in range(2000):
        req = GetOrdersRequest(
            status=QueryOrderStatus.ALL,
            limit=batch_limit,
            after=cursor_after,
            until=until_dt,
            direction=Sort.ASC,
            nested=True,
        )
        batch = list(client.get_orders(filter=req) or [])
        if not batch:
            break

        for o in batch:
            oid = str(getattr(o, "id", "") or "")
            if oid and oid in seen_ids:
                continue
            if oid:
                seen_ids.add(oid)
            out.append(o)

        if len(batch) < batch_limit:
            break

        last_dt = _submitted_dt(batch[-1])
        if last_dt is None:
            break

        next_cursor = last_dt + timedelta(microseconds=1)
        if next_cursor <= cursor_after:
            next_cursor = cursor_after + timedelta(seconds=1)
        if next_cursor > until_dt:
            break
        cursor_after = next_cursor

    return out


def normalize_order(order_obj: Any) -> Dict[str, Any]:
    oid = str(getattr(order_obj, "id", "") or "")
    cid = str(getattr(order_obj, "client_order_id", "") or "")
    symbol = str(getattr(order_obj, "symbol", "") or "")

    side_obj = getattr(order_obj, "side", None)
    side = str(getattr(side_obj, "value", side_obj) or "").lower()

    status_obj = getattr(order_obj, "status", None)
    status = str(getattr(status_obj, "value", status_obj) or "").lower()

    tif_obj = getattr(order_obj, "time_in_force", None)
    tif = str(getattr(tif_obj, "value", tif_obj) or "").lower()

    typ_obj = getattr(order_obj, "type", None)
    typ = str(getattr(typ_obj, "value", typ_obj) or "").lower()

    submitted_at = _parse_dt(getattr(order_obj, "submitted_at", None) or getattr(order_obj, "created_at", None))
    filled_at = _parse_dt(getattr(order_obj, "filled_at", None))
    updated_at = _parse_dt(getattr(order_obj, "updated_at", None) or getattr(order_obj, "filled_at", None))

    qty = _to_float(getattr(order_obj, "qty", None), 0.0) or 0.0
    filled_qty = _to_float(getattr(order_obj, "filled_qty", None), 0.0) or 0.0
    filled_avg_price = _to_float(getattr(order_obj, "filled_avg_price", None), 0.0) or 0.0
    limit_price = _to_float(getattr(order_obj, "limit_price", None), None)

    return {
        "order_id": oid,
        "client_order_id": cid,
        "symbol": symbol,
        "side": side,
        "status": status,
        "submitted_at": _fmt_dt(submitted_at),
        "filled_at": _fmt_dt(filled_at),
        "updated_at": _fmt_dt(updated_at),
        "qty": float(qty),
        "filled_qty": float(filled_qty),
        "filled_avg_price": float(filled_avg_price),
        "limit_price": limit_price,
        "time_in_force": tif,
        "order_type": typ,
        "source": "alpaca_api",
        "submitted_at_dt": submitted_at,
        "filled_at_dt": filled_at,
        "updated_at_dt": updated_at,
    }


@dataclass
class TradeLot:
    symbol: str
    entry_side: str  # long|short
    open_exec_dt: datetime
    open_bar: str
    open_price: float
    open_qty: float
    rem_qty: float
    close_qty: float = 0.0
    close_notional: float = 0.0
    close_exec_dt_last: Optional[datetime] = None
    close_bar_last: str = ""


def build_trade_rows_fifo(norm_orders: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, float]]]:
    rows: List[Dict[str, Any]] = []
    long_lots: Dict[str, deque[TradeLot]] = defaultdict(deque)
    short_lots: Dict[str, deque[TradeLot]] = defaultdict(deque)

    ordered = sorted(
        norm_orders,
        key=lambda r: (
            r.get("filled_at_dt") or r.get("updated_at_dt") or r.get("submitted_at_dt") or datetime.max.replace(tzinfo=UTC),
            r.get("symbol") or "",
            r.get("order_id") or "",
        ),
    )

    for o in ordered:
        symbol = o.get("symbol") or ""
        side = (o.get("side") or "").lower()
        qty = abs(float(o.get("filled_qty") or 0.0))
        px = float(o.get("filled_avg_price") or 0.0)
        if not symbol or side not in ("buy", "sell") or qty <= 0 or px <= 0:
            continue

        exec_dt = o.get("filled_at_dt") or o.get("updated_at_dt") or o.get("submitted_at_dt")
        if not exec_dt:
            continue
        bar = _minute_bucket(o.get("submitted_at_dt") or exec_dt)

        rem = qty
        if side == "buy":
            # close shorts first
            target = short_lots[symbol]
            while rem > 1e-12 and target:
                lot = target[0]
                m = min(rem, lot.rem_qty)
                lot.rem_qty -= m
                lot.close_qty += m
                lot.close_notional += m * px
                lot.close_exec_dt_last = exec_dt
                lot.close_bar_last = bar
                rem -= m
                if lot.rem_qty <= 1e-12:
                    target.popleft()
                    rows.append(_finalize_lot(lot))
            if rem > 1e-12:
                long_lots[symbol].append(
                    TradeLot(
                        symbol=symbol,
                        entry_side="long",
                        open_exec_dt=exec_dt,
                        open_bar=bar,
                        open_price=px,
                        open_qty=rem,
                        rem_qty=rem,
                    )
                )
        else:
            # sell: close longs first
            target = long_lots[symbol]
            while rem > 1e-12 and target:
                lot = target[0]
                m = min(rem, lot.rem_qty)
                lot.rem_qty -= m
                lot.close_qty += m
                lot.close_notional += m * px
                lot.close_exec_dt_last = exec_dt
                lot.close_bar_last = bar
                rem -= m
                if lot.rem_qty <= 1e-12:
                    target.popleft()
                    rows.append(_finalize_lot(lot))
            if rem > 1e-12:
                short_lots[symbol].append(
                    TradeLot(
                        symbol=symbol,
                        entry_side="short",
                        open_exec_dt=exec_dt,
                        open_bar=bar,
                        open_price=px,
                        open_qty=rem,
                        rem_qty=rem,
                    )
                )

    # remaining OPEN/PARTIAL lots
    for lots in list(long_lots.values()) + list(short_lots.values()):
        for lot in lots:
            rows.append(_finalize_lot(lot))

    rows.sort(key=lambda r: ((_parse_dt(r.get("open_exec_dt")) or datetime.max.replace(tzinfo=UTC)), r.get("symbol") or ""))
    for i, r in enumerate(rows, start=1):
        r["trade_id"] = f"T{i:06d}"

    # reconstructed position snapshot from residual quantities
    recon: Dict[str, Dict[str, float]] = defaultdict(lambda: {"net_qty": 0.0, "notional": 0.0})
    for r in rows:
        status = r.get("status")
        if status == "CLOSED":
            continue
        symbol = r["symbol"]
        rem = float(r.get("remaining_qty") or 0.0)
        if rem <= 0:
            continue
        sign = 1.0 if r.get("entry_side") == "long" else -1.0
        recon[symbol]["net_qty"] += sign * rem
        recon[symbol]["notional"] += abs(rem) * float(r.get("open_price") or 0.0)

    for symbol in recon:
        net = recon[symbol]["net_qty"]
        notional = recon[symbol]["notional"]
        recon[symbol]["avg_price"] = (notional / abs(net)) if abs(net) > 1e-12 else 0.0

    return rows, recon


def _finalize_lot(lot: TradeLot) -> Dict[str, Any]:
    open_notional = lot.open_qty * lot.open_price
    closed = lot.close_qty
    close_price = (lot.close_notional / closed) if closed > 1e-12 else None
    remaining = max(lot.rem_qty, 0.0)

    if closed <= 1e-12:
        status = "OPEN"
    elif remaining > 1e-12:
        status = "PARTIAL"
    else:
        status = "CLOSED"

    pnl = None
    pnl_pct = None
    if closed > 1e-12 and close_price is not None:
        if lot.entry_side == "long":
            pnl = (close_price - lot.open_price) * closed
        else:
            pnl = (lot.open_price - close_price) * closed
        base = lot.open_price * closed
        pnl_pct = (pnl / base) * 100.0 if base > 0 else None

    return {
        "symbol": lot.symbol,
        "entry_side": lot.entry_side,
        "open_exec_dt": _fmt_dt(lot.open_exec_dt),
        "close_exec_dt": _fmt_dt(lot.close_exec_dt_last),
        "open_bar": lot.open_bar,
        "close_bar": lot.close_bar_last,
        "size": round(lot.open_qty, 8),
        "open_price": round(lot.open_price, 6),
        "close_price": round(close_price, 6) if close_price is not None else "",
        "closed_qty": round(closed, 8) if closed > 0 else 0.0,
        "remaining_qty": round(remaining, 8),
        "pnl": round(pnl, 6) if pnl is not None else "",
        "pnl_pct": round(pnl_pct, 6) if pnl_pct is not None else "",
        "status": status,
        "value": round(open_notional, 6),
    }


def build_position_rows(
    alpaca_positions_raw: List[Any],
    reconstructed: Dict[str, Dict[str, float]],
) -> List[Dict[str, Any]]:
    broker_pos: Dict[str, Dict[str, Any]] = {}
    for p in alpaca_positions_raw:
        symbol = str(getattr(p, "symbol", "") or "")
        if not symbol:
            continue
        qty = _to_float(getattr(p, "qty", None), 0.0) or 0.0
        avg_entry_price = _to_float(getattr(p, "avg_entry_price", None), 0.0) or 0.0
        market_value = _to_float(getattr(p, "market_value", None), 0.0) or 0.0
        unrealized_pl = _to_float(getattr(p, "unrealized_pl", None), 0.0) or 0.0
        broker_pos[symbol] = {
            "symbol": symbol,
            "qty": qty,
            "avg_entry_price": avg_entry_price,
            "market_value": market_value,
            "unrealized_pl": unrealized_pl,
        }

    symbols = sorted(set(broker_pos.keys()) | set(reconstructed.keys()))
    out: List[Dict[str, Any]] = []
    for sym in symbols:
        b = broker_pos.get(sym, {})
        r = reconstructed.get(sym, {})
        bqty = float(b.get("qty", 0.0) or 0.0)
        rqty = float(r.get("net_qty", 0.0) or 0.0)
        out.append({
            "symbol": sym,
            "qty": round(bqty, 8),
            "avg_entry_price": round(float(b.get("avg_entry_price", 0.0) or 0.0), 6),
            "market_value": round(float(b.get("market_value", 0.0) or 0.0), 6),
            "unrealized_pl": round(float(b.get("unrealized_pl", 0.0) or 0.0), 6),
            "reconstructed_net_qty": round(rqty, 8),
            "reconstructed_avg_price": round(float(r.get("avg_price", 0.0) or 0.0), 6),
            "qty_diff": round(bqty - rqty, 8),
        })
    return out


def _write_csv(path: Path, rows: List[Dict[str, Any]], headers: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in headers})


def _order_sort_dt(r: Dict[str, Any]) -> datetime:
    return r.get("submitted_at_dt") or r.get("updated_at_dt") or r.get("filled_at_dt") or datetime.max.replace(tzinfo=UTC)


def split_order_into_segments(norm_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Split orders into OPEN/CLOSE segments based on running position.

    This enables EOD-compatible `is_close` even when one order both closes and opens.
    """
    pos_by_symbol: Dict[str, float] = defaultdict(float)
    out: List[Dict[str, Any]] = []

    for o in sorted(norm_orders, key=lambda r: (_order_sort_dt(r), r.get("symbol") or "", r.get("order_id") or "")):
        symbol = o.get("symbol") or ""
        side = (o.get("side") or "").lower()
        qty = abs(float(o.get("filled_qty") or 0.0))
        if not symbol or side not in ("buy", "sell"):
            continue
        signed = qty if side == "buy" else -qty
        prev = pos_by_symbol[symbol]

        close_qty = 0.0
        if prev != 0 and (prev > 0 > signed or prev < 0 < signed):
            close_qty = min(abs(prev), abs(signed))
        open_qty = max(0.0, abs(signed) - close_qty)

        if close_qty > 1e-12:
            out.append({**o, "segment_qty": close_qty, "is_close": True, "segment_suffix": "C"})
        if open_qty > 1e-12:
            out.append({**o, "segment_qty": open_qty, "is_close": False, "segment_suffix": "O"})

        pos_by_symbol[symbol] = prev + signed

        # Include terminal non-filled orders for diagnostics
        if qty <= 1e-12:
            out.append({**o, "segment_qty": abs(float(o.get("qty") or 0.0)), "is_close": False, "segment_suffix": "N"})

    return out


def to_eod_orderhistory(norm_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for seg in split_order_into_segments(norm_orders):
        symbol = seg.get("symbol") or ""
        side = (seg.get("side") or "").lower()
        status = (seg.get("status") or "").lower()
        qty = abs(float(seg.get("segment_qty") or 0.0))
        if not symbol or side not in ("buy", "sell") or qty <= 0:
            continue

        submitted_dt = seg.get("submitted_at_dt")
        filled_dt = seg.get("filled_at_dt") or seg.get("updated_at_dt") or submitted_dt
        created_dt_s = _fmt_dt(submitted_dt)
        exec_dt_s = _fmt_dt(filled_dt)
        signal_dt_s = created_dt_s

        bt_status = _status_to_bt(status)

        order_id = f"{seg.get('order_id')}_{seg.get('segment_suffix')}"
        created_price = seg.get("limit_price")
        if created_price in (None, ""):
            created_price = float(seg.get("filled_avg_price") or 0.0)

        ev = {
            "status": bt_status,
            "datetime": exec_dt_s or created_dt_s,
            "signal_dt": signal_dt_s,
            "bar": 0,
            "bar_time": signal_dt_s,
            "bar_open": None,
            "bar_high": None,
            "bar_low": None,
            "bar_close": None,
        }

        filled_qty = abs(float(seg.get("filled_qty") or 0.0))
        if filled_qty > 0:
            ev.update({
                "exec_dt": exec_dt_s,
                "exec_price": float(seg.get("filled_avg_price") or 0.0),
                "exec_size": float(qty),
                "exec_value": float(qty) * float(seg.get("filled_avg_price") or 0.0),
                "exec_comm": 0.0,
                "exec_pnl": 0.0,
                "exec_delay_bars": 0,
                "exec_delay_seconds": 0.0,
            })

        rows.append({
            "id": order_id,
            "asset": symbol,
            "status": bt_status,
            "type": side,
            "quantity": float(qty),
            "datetime": created_dt_s,
            "lastdatetime": _fmt_dt(seg.get("updated_at_dt") or seg.get("filled_at_dt") or submitted_dt),
            "info": {
                "client_order_id": seg.get("client_order_id") or "",
                "alpaca_status": status,
                "id": seg.get("order_id") or "",
                "signal_dt": signal_dt_s,
                "is_close": bool(seg.get("is_close")),
                "filled_qty": float(seg.get("filled_qty") or 0.0),
                "filled_avg_price": float(seg.get("filled_avg_price") or 0.0),
            },
            "created": {
                "dt": created_dt_s,
                "signal_dt": signal_dt_s,
                "bar": 0,
                "price": float(created_price or 0.0),
                "exectype": "Market" if (seg.get("order_type") or "").lower() == "market" else "Limit",
                "is_close": bool(seg.get("is_close")),
                "bar_time": signal_dt_s,
                "bar_open": None,
                "bar_high": None,
                "bar_low": None,
                "bar_close": None,
            },
            "events": [ev],
        })

    rows.sort(key=lambda r: (_parse_dt(r.get("datetime")) or datetime.max.replace(tzinfo=UTC), r.get("asset") or ""), reverse=True)
    return rows


def to_eod_trades(trade_rows: List[Dict[str, Any]], bar_seconds: int = 60) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in trade_rows:
        status = str(t.get("status") or "").upper()
        if status not in ("CLOSED", "PARTIAL"):
            continue
        open_dt = _parse_dt(t.get("open_exec_dt"))
        close_dt = _parse_dt(t.get("close_exec_dt"))
        hold_bars = 0
        if open_dt and close_dt and close_dt >= open_dt and bar_seconds > 0:
            hold_bars = int(round((close_dt - open_dt).total_seconds() / float(bar_seconds)))
        # For PARTIAL trades, only the realized/closed quantity contributes to PnL.
        size = float(t.get("closed_qty") or 0.0)
        price = float(t.get("open_price") or 0.0)
        value = size * price
        pnl = _to_float(t.get("pnl"), 0.0) or 0.0
        if size <= 0:
            continue
        out.append({
            "asset": t.get("symbol") or "",
            "open_datetime": t.get("open_exec_dt") or "",
            "close_datetime": t.get("close_exec_dt") or "",
            "duration_bars": hold_bars,
            "pnl": pnl,
            "pnl_pct": _to_float(t.get("pnl_pct"), 0.0) or 0.0,
            "size": size,
            "price": price,
            "value": value,
            "pnlcomm": pnl,
            "entry_side": t.get("entry_side") or "",
            "entry_signal_dt": t.get("open_exec_dt") or "",
            "exit_signal_dt": t.get("close_exec_dt") or "",
            "entry_delay_bars": 0,
            "exit_delay_bars": 0,
        })
    out.sort(key=lambda r: (r.get("open_datetime") or "", r.get("close_datetime") or ""))
    return out


def _json_dump(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=True), encoding="utf-8")


def run_eod(eod_script: Path, sim_dir: Path, paper_alpaca_dir: Path, output_dir: Path, bar_seconds: int, verbose: bool) -> int:
    cmd = [
        sys.executable,
        str(eod_script),
        "--paper", str(paper_alpaca_dir),
        "--sim", str(sim_dir),
        "--bar-seconds", str(max(1, int(bar_seconds))),
        "--output", str(output_dir / "eod_report_alpaca_vs_sim.html"),
        "--intermediate-reports",
    ]
    _log("Run EOD: " + " ".join(cmd), verbose)
    cp = subprocess.run(cmd, check=False)
    return int(cp.returncode)


def main() -> int:
    args = parse_args()

    base_dir = Path(args.folder).resolve() if args.folder else None
    sim_dir: Optional[Path] = None
    paper_input_dir: Optional[Path] = None

    if base_dir:
        sim_dir = (base_dir / "sim").resolve()
        paper_input_dir = (base_dir / "paper").resolve()
    elif args.sim:
        sim_dir = Path(args.sim).resolve()

    if sim_dir is None:
        raise SystemExit("Missing --folder or --sim")
    if not sim_dir.exists():
        raise SystemExit(f"SIM directory not found: {sim_dir}")

    now_tag = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir).resolve() if args.output_dir else (Path("out") / "reconcile" / "alpaca" / now_tag).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    start_dt, end_dt, infer_counts = infer_window_from_inputs(sim_dir, paper_input_dir if (paper_input_dir and paper_input_dir.exists()) else None)
    _log(f"Inferred window UTC: {start_dt.isoformat()} -> {end_dt.isoformat()}", args.verbose)
    _log(f"Datetime hits from inputs: {infer_counts}", args.verbose)

    client = get_alpaca_client(args.paper_mode)
    raw_orders = fetch_alpaca_orders(client, start_dt, end_dt)
    raw_positions = list(client.get_all_positions() or [])
    raw_account = client.get_account()

    _json_dump(output_dir / "alpaca_orders_raw.json", _obj_to_dict(raw_orders))
    _json_dump(output_dir / "alpaca_positions_raw.json", _obj_to_dict(raw_positions))
    _json_dump(output_dir / "alpaca_account_raw.json", _obj_to_dict(raw_account))

    norm_orders = [normalize_order(o) for o in raw_orders]
    norm_orders.sort(key=lambda r: (_order_sort_dt(r), r.get("symbol") or "", r.get("order_id") or ""))

    order_headers = [
        "order_id", "client_order_id", "symbol", "side", "status",
        "submitted_at", "filled_at", "updated_at",
        "qty", "filled_qty", "filled_avg_price", "limit_price",
        "time_in_force", "order_type", "source",
    ]
    _write_csv(output_dir / "order_alpaca.csv", norm_orders, order_headers)

    trade_rows, reconstructed_positions = build_trade_rows_fifo(norm_orders)
    trade_headers = [
        "trade_id", "symbol", "entry_side",
        "open_exec_dt", "close_exec_dt", "open_bar", "close_bar",
        "size", "open_price", "close_price", "closed_qty", "remaining_qty",
        "pnl", "pnl_pct", "status", "value",
    ]
    _write_csv(output_dir / "trade_alpaca.csv", trade_rows, trade_headers)

    pos_rows = build_position_rows(raw_positions, reconstructed_positions)
    pos_headers = [
        "symbol", "qty", "avg_entry_price", "market_value", "unrealized_pl",
        "reconstructed_net_qty", "reconstructed_avg_price", "qty_diff",
    ]
    _write_csv(output_dir / "position_alpaca.csv", pos_rows, pos_headers)

    paper_alpaca_dir: Optional[Path] = None
    if args.emit_eod_compatible:
        paper_alpaca_dir = output_dir / "paper_alpaca"
        paper_alpaca_dir.mkdir(parents=True, exist_ok=True)
        eod_orderhistory = to_eod_orderhistory(norm_orders)
        eod_trades = to_eod_trades(trade_rows, bar_seconds=max(1, int(args.bar_seconds)))
        _json_dump(paper_alpaca_dir / "orderhistory.json", eod_orderhistory)
        _json_dump(paper_alpaca_dir / "trades.json", eod_trades)

    total_realized = sum(float(r.get("pnl") or 0.0) for r in trade_rows if r.get("status") in ("CLOSED", "PARTIAL"))
    qty_diff_abs = sum(abs(float(r.get("qty_diff") or 0.0)) for r in pos_rows)

    account_cash = _to_float(getattr(raw_account, "cash", None), 0.0) or 0.0
    account_equity = _to_float(getattr(raw_account, "equity", None), 0.0) or 0.0
    account_long_mv = _to_float(getattr(raw_account, "long_market_value", None), 0.0) or 0.0
    account_short_mv = _to_float(getattr(raw_account, "short_market_value", None), 0.0) or 0.0
    account_calc_equity = account_cash + account_long_mv + account_short_mv
    account_equity_diff = account_equity - account_calc_equity

    print("=== Alpaca Reconcile ===")
    print(f"Output dir              : {output_dir}")
    print(f"Window UTC              : {start_dt.strftime('%Y-%m-%d %H:%M:%S')} -> {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Orders fetched          : {len(norm_orders)}")
    print(f"Trades reconstructed    : {len(trade_rows)}")
    print(f"Positions compared      : {len(pos_rows)}")
    print(f"Realized PnL (trade csv): {total_realized:+.2f}")
    print(f"Abs qty diff sum        : {qty_diff_abs:.8f}")
    print(
        "Account check (cash+long+short ~= equity): "
        f"cash={account_cash:.2f}, long_mv={account_long_mv:.2f}, short_mv={account_short_mv:.2f}, "
        f"calc={account_calc_equity:.2f}, equity={account_equity:.2f}, diff={account_equity_diff:+.6f}"
    )

    eod_rc = 0
    if args.run_eod:
        if paper_alpaca_dir is None:
            raise SystemExit("--run-eod requires --emit-eod-compatible")
        eod_script = Path(__file__).with_name("eod_analysis.py")
        if not eod_script.exists():
            raise SystemExit(f"Missing EOD script: {eod_script}")
        eod_rc = run_eod(
            eod_script=eod_script,
            sim_dir=sim_dir,
            paper_alpaca_dir=paper_alpaca_dir,
            output_dir=output_dir,
            bar_seconds=max(1, int(args.bar_seconds)),
            verbose=args.verbose,
        )
        print(f"EOD return code         : {eod_rc}")

    if eod_rc != 0:
        return eod_rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
