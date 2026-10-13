#!/usr/bin/env python3
"""
Compare a Backtrader run against Alpaca filled orders from a cutoff date.

The script can either run btmain.py with user-provided parameters or read an
existing trades.json. It then downloads filled Alpaca orders, builds simple
round trips by symbol, and writes a date/symbol reconciliation CSV.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
ROME_TZ = dt.timezone(dt.timedelta(hours=2))
ET_TZ = dt.timezone(dt.timedelta(hours=-4))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cutoff-date", required=True, help="First trade date to compare, YYYY-MM-DD")
    p.add_argument("--env-file", required=True, help="Env file containing Alpaca credentials")
    p.add_argument("--paper", choices=["auto", "true", "false"], default="auto")
    p.add_argument("--client-id-prefix", default="", help="Optional Alpaca client_order_id prefix filter")
    p.add_argument("--order-limit", type=int, default=500, help="Max Alpaca orders to fetch")
    p.add_argument("--out", default="", help="Output CSV path")
    p.add_argument("--summary-out", default="", help="Optional daily summary CSV path")
    p.add_argument(
        "--keep-last-backtest-date",
        action="store_true",
        help="Deprecated/debug only; backtest dates are kept by default.",
    )
    p.add_argument(
        "--keep-last-live-date",
        action="store_true",
        help="Keep the last live entry date. Default drops it because the latest live cycle is the open edge case.",
    )
    p.add_argument(
        "--price-display",
        choices=["tick", "raw"],
        default="tick",
        help="CSV price display/comparison normalization. tick=2 decimals >=1, 4 decimals <1",
    )

    p.add_argument("--bt-core", default=str(REPO_ROOT / "bt-core"))
    p.add_argument("--run-backtest", action="store_true", help="Run btmain.py before comparing")
    p.add_argument("--backtest-trades", default="", help="Existing trades.json path")
    p.add_argument("--run-id", default="", help="Backtest --id. Auto-generated when omitted and --run-backtest is used")
    p.add_argument("--fromdate", default="", help="Backtest fromdate. Defaults to cutoff-date minus --warmup-days")
    p.add_argument("--todate", default="", help="Backtest todate. Defaults to today")
    p.add_argument("--warmup-days", type=int, default=540, help="Days before cutoff used when --fromdate is omitted")
    p.add_argument("--strat", default="overnight_ah.OvernightAH")
    p.add_argument("--ticker", default="yahoo_adj_research_universe.json")
    p.add_argument("--provider", default="yahoo")
    p.add_argument("--alpaca-feed", default="sip", choices=["sip", "iex"])
    p.add_argument("--timeframe", default="daily")
    p.add_argument("--commission", default="none")
    p.add_argument("--margin-leverage", default="2")
    p.add_argument("--cash", default="", help="Backtest initial cash. When omitted btmain.py default is used")
    p.add_argument("--stratargs", default="", help="Backtrader strategy args string")
    p.add_argument("--extra-bt-arg", action="append", default=[], help="Extra btmain.py argument; repeatable")
    return p.parse_args()


def read_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            env[key] = value
    return env


def iso_date(value: Any) -> str:
    if value in (None, ""):
        return ""
    text = str(value)
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return text[:10]


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def tick_round_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    price = as_float(value)
    decimals = 2 if abs(price) >= 1.0 else 4
    return f"{price:.{decimals}f}"


def comparable_price(value: Any, mode: str) -> Any:
    if mode == "raw":
        return value
    rounded = tick_round_price(value)
    return as_float(rounded) if rounded != "" else ""


def outpath_for(bt_core: Path, strat: str, run_id: str) -> Path:
    parts = strat.split(".")
    if len(parts) == 2:
        base = bt_core / "out" / parts[0].lower() / parts[1]
    else:
        base = bt_core / "out" / strat
    return base / run_id if run_id else base


def run_backtest(args: argparse.Namespace, cutoff: dt.date) -> Path:
    bt_core = Path(args.bt_core).resolve()
    run_id = args.run_id or f"reconcile_{args.strat.split('.')[-1]}_{cutoff:%Y%m%d}_{dt.datetime.now():%Y%m%d_%H%M%S}"
    fromdate = args.fromdate or (cutoff - dt.timedelta(days=args.warmup_days)).isoformat()
    todate = args.todate or dt.date.today().isoformat()
    cmd = [
        str(bt_core / ".venv" / "bin" / "python"),
        "btmain.py",
        "--strat",
        args.strat,
        "--ticker",
        args.ticker,
        "--fromdate",
        fromdate,
        "--todate",
        todate,
        "--timeframe",
        args.timeframe,
        "--provider",
        args.provider,
        "--alpaca-feed",
        args.alpaca_feed,
        "--commission",
        args.commission,
        "--margin-leverage",
        str(args.margin_leverage),
        "--mode",
        "backtest",
        "--id",
        run_id,
    ]
    if args.cash:
        cmd.extend(["--cash", str(args.cash)])
    if args.stratargs:
        cmd.extend(["--stratargs", args.stratargs])
    cmd.extend(args.extra_bt_arg)
    print("Running backtest:", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=str(bt_core), check=True)
    run_outpath = outpath_for(bt_core, args.strat, run_id)
    trades_path = run_outpath / "trades.json"
    if not trades_path.exists():
        candidates = sorted(run_outpath.glob("trades*.json"))
        if candidates:
            return candidates[-1]
        existing = ", ".join(sorted(p.name for p in run_outpath.glob("*"))) or "no files"
        raise FileNotFoundError(
            "Backtest completed but no trades*.json was found. "
            f"Run output: {run_outpath}. Files: {existing}. "
            "This usually means the backtest had no usable warmup/data window; "
            "try a larger --warmup-days or an explicit earlier --fromdate."
        )
    return trades_path


def load_backtest_trades(
    path: Path,
    cutoff: dt.date,
    drop_last_entry_date: bool = False,
) -> tuple[list[dict[str, Any]], str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for item in raw:
        symbol = str(item.get("asset") or item.get("symbol") or "").upper()
        entry_date = iso_date(
            item.get("entry_signal_dt")
            or item.get("open_datetime")
            or item.get("entry_datetime")
        )
        if not symbol or not entry_date or dt.date.fromisoformat(entry_date) < cutoff:
            continue
        entry_price = as_float(item.get("price") or item.get("entry_price"))
        pnl_pct = as_float(item.get("pnl_pct"), 0.0)
        exit_price = as_float(item.get("exit_price"), 0.0)
        if not exit_price and entry_price:
            exit_price = entry_price * (1.0 + pnl_pct / 100.0)
        size = abs(as_float(item.get("size") or item.get("qty")))
        value = abs(as_float(item.get("value"), entry_price * size))
        rows.append(
            {
                "date": entry_date,
                "symbol": symbol,
                "bt_entry_price": entry_price,
                "bt_exit_price": exit_price,
                "bt_qty": size,
                "bt_notional": value,
                "bt_pnl_pct": pnl_pct,
                "bt_close_date": iso_date(item.get("close_datetime") or item.get("exit_datetime")),
                "bt_open_date": iso_date(item.get("open_datetime") or item.get("entry_datetime")),
                "bt_entry_signal_date": iso_date(item.get("entry_signal_dt")),
            }
        )
    dropped_note = ""
    if drop_last_entry_date and rows:
        last_date = max(row["date"] for row in rows)
        before = len(rows)
        rows = [row for row in rows if row["date"] != last_date]
        dropped = before - len(rows)
        if dropped:
            dropped_note = (
                f"Dropped last backtest signal date {last_date} ({dropped} rows): "
                "daily Backtrader final positions are not reliable because the last "
                "signal/position has no fully confirmed following cycle. "
                "Extend --todate beyond the period under review, or pass "
                "--keep-last-backtest-date only for debugging."
            )
    add_daily_weights(rows, "bt_notional", "bt_weight")
    return rows, dropped_note


def drop_last_live_entry_date(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    if not rows:
        return rows, ""
    last_date = max(row["date"] for row in rows)
    kept = [row for row in rows if row["date"] != last_date]
    dropped = len(rows) - len(kept)
    if not dropped:
        return rows, ""
    note = (
        f"Dropped last live entry date {last_date} ({dropped} rows): "
        "latest live cycle can show entry/exit while the comparable daily "
        "backtest candle/cycle is not fully available yet. Extend the backtest "
        "past that date or pass --keep-last-live-date only for debugging."
    )
    return kept, note


def get_attr(obj: Any, name: str, default: Any = None) -> Any:
    return getattr(obj, name, default)


def parse_dt(value: Any) -> dt.datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def local_trade_date(value: dt.datetime | None, tz: dt.timezone) -> str:
    if value is None:
        return ""
    return value.astimezone(tz).date().isoformat()


def fetch_alpaca_orders(args: argparse.Namespace, env: dict[str, str], cutoff: dt.date) -> list[dict[str, Any]]:
    api_key = env.get("ALPACA_API_KEY") or env.get("BROKER_API_KEY")
    secret = env.get("ALPACA_SECRET_KEY") or env.get("BROKER_SECRET_KEY")
    if not api_key or not secret:
        raise RuntimeError("Missing Alpaca credentials in env file")
    if args.paper == "auto":
        paper = bool(api_key.startswith("PK"))
    else:
        paper = args.paper == "true"

    from alpaca.trading.client import TradingClient
    from alpaca.trading.enums import QueryOrderStatus
    from alpaca.trading.requests import GetOrdersRequest

    client = TradingClient(api_key, secret, paper=paper)
    after = dt.datetime.combine(cutoff, dt.time.min, tzinfo=dt.timezone.utc) - dt.timedelta(days=1)
    req = GetOrdersRequest(status=QueryOrderStatus.ALL, after=after, limit=args.order_limit, nested=False)
    orders = list(client.get_orders(filter=req) or [])
    rows: list[dict[str, Any]] = []
    for order in orders:
        symbol = str(get_attr(order, "symbol", "")).upper()
        side = str(get_attr(get_attr(order, "side", ""), "value", get_attr(order, "side", ""))).lower()
        client_order_id = str(get_attr(order, "client_order_id", "") or "")
        if args.client_id_prefix and not client_order_id.startswith(args.client_id_prefix):
            continue
        qty = abs(as_float(get_attr(order, "filled_qty", 0)))
        price = as_float(get_attr(order, "filled_avg_price", 0))
        filled_at = parse_dt(get_attr(order, "filled_at", None))
        if not symbol or not side or qty <= 0 or price <= 0 or filled_at is None:
            continue
        rows.append(
            {
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": price,
                "notional": qty * price,
                "filled_at": filled_at,
                "filled_at_utc": filled_at.astimezone(dt.timezone.utc).isoformat(),
                "date": local_trade_date(filled_at, ET_TZ),
                "client_order_id": client_order_id,
                "order_id": str(get_attr(order, "id", "") or ""),
                "order_type": str(get_attr(get_attr(order, "type", ""), "value", get_attr(order, "type", ""))),
                "time_in_force": str(get_attr(get_attr(order, "time_in_force", ""), "value", get_attr(order, "time_in_force", ""))),
            }
        )
    rows.sort(key=lambda r: (r["filled_at"], r["symbol"], r["side"]))
    return rows


def build_live_roundtrips(orders: list[dict[str, Any]], cutoff: dt.date) -> list[dict[str, Any]]:
    buys_by_symbol: dict[str, list[dict[str, Any]]] = {}
    trips: list[dict[str, Any]] = []
    for order in orders:
        symbol = order["symbol"]
        if order["side"] == "buy":
            buys_by_symbol.setdefault(symbol, []).append(order)
            continue
        if order["side"] != "sell":
            continue
        queue = buys_by_symbol.setdefault(symbol, [])
        if not queue:
            continue
        buy = queue.pop(0)
        entry_date = buy["date"]
        if not entry_date or dt.date.fromisoformat(entry_date) < cutoff:
            continue
        trips.append(
            {
                "date": entry_date,
                "symbol": symbol,
                "live_entry_price": buy["price"],
                "live_exit_price": order["price"],
                "live_qty": min(buy["qty"], order["qty"]),
                "live_notional": buy["notional"],
                "live_entry_time_utc": buy["filled_at_utc"],
                "live_exit_time_utc": order["filled_at_utc"],
                "live_buy_client_order_id": buy["client_order_id"],
                "live_sell_client_order_id": order["client_order_id"],
            }
        )
    add_daily_weights(trips, "live_notional", "live_weight")
    return trips


def add_daily_weights(rows: list[dict[str, Any]], notional_col: str, weight_col: str) -> None:
    totals: dict[str, float] = {}
    for row in rows:
        totals[row["date"]] = totals.get(row["date"], 0.0) + as_float(row.get(notional_col))
    for row in rows:
        total = totals.get(row["date"], 0.0)
        row[weight_col] = as_float(row.get(notional_col)) / total if total else 0.0


def merge_rows(
    bt_rows: list[dict[str, Any]],
    live_rows: list[dict[str, Any]],
    price_display: str,
) -> list[dict[str, Any]]:
    bt_map = {(r["date"], r["symbol"]): r for r in bt_rows}
    live_map = {(r["date"], r["symbol"]): r for r in live_rows}
    keys = sorted(set(bt_map) | set(live_map))
    merged: list[dict[str, Any]] = []
    for key in keys:
        bt = bt_map.get(key, {})
        live = live_map.get(key, {})
        row = {
            "date": key[0],
            "symbol": key[1],
            "match_status": "matched" if bt and live else ("backtest_only" if bt else "live_only"),
            **bt,
            **live,
        }
        for col in ("bt_entry_price", "live_entry_price", "bt_exit_price", "live_exit_price"):
            if col in row:
                row[col] = comparable_price(row[col], price_display)
        row["entry_diff_abs"] = diff(row.get("live_entry_price"), row.get("bt_entry_price"))
        row["entry_diff_bps"] = bps(row.get("live_entry_price"), row.get("bt_entry_price"))
        row["exit_diff_abs"] = diff(row.get("live_exit_price"), row.get("bt_exit_price"))
        row["exit_diff_bps"] = bps(row.get("live_exit_price"), row.get("bt_exit_price"))
        row["entry_edge_bps"] = signed_entry_edge_bps(row.get("live_entry_price"), row.get("bt_entry_price"))
        row["exit_edge_bps"] = signed_exit_edge_bps(row.get("live_exit_price"), row.get("bt_exit_price"))
        row["roundtrip_execution_edge_bps"] = signed_roundtrip_edge_bps(
            row.get("entry_edge_bps"),
            row.get("exit_edge_bps"),
        )
        row["weight_diff_pctpt"] = diff(row.get("live_weight"), row.get("bt_weight"), scale=100.0)
        merged.append(row)
    annotate_affordability(merged)
    return merged


def annotate_affordability(rows: list[dict[str, Any]]) -> None:
    live_notional_by_date: dict[str, list[float]] = {}
    for row in rows:
        if row.get("match_status") in ("matched", "live_only"):
            notional = as_float(row.get("live_notional"))
            if notional > 0:
                live_notional_by_date.setdefault(row["date"], []).append(notional)
    for row in rows:
        row["diagnostic_note"] = ""
        if row.get("match_status") != "backtest_only":
            continue
        live_slots = live_notional_by_date.get(row["date"], [])
        if not live_slots:
            continue
        max_live_slot = max(live_slots)
        bt_entry = as_float(row.get("bt_entry_price"))
        if bt_entry > max_live_slot > 0:
            row["diagnostic_note"] = (
                f"bt_only_probably_unaffordable_live: entry_price {bt_entry:.2f} "
                f"> observed max live slot {max_live_slot:.2f}"
            )


def diff(a: Any, b: Any, scale: float = 1.0) -> str:
    if a in (None, "") or b in (None, ""):
        return ""
    return f"{(as_float(a) - as_float(b)) * scale:.6f}"


def bps(a: Any, b: Any) -> str:
    base = as_float(b)
    if a in (None, "") or not base:
        return ""
    return f"{(as_float(a) / base - 1.0) * 10000.0:.2f}"


def signed_entry_edge_bps(live_entry: Any, bt_entry: Any) -> str:
    """Positive means live bought cheaper than backtest; negative means worse."""
    base = as_float(bt_entry)
    if live_entry in (None, "") or not base:
        return ""
    return f"{(base - as_float(live_entry)) / base * 10000.0:.2f}"


def signed_exit_edge_bps(live_exit: Any, bt_exit: Any) -> str:
    """Positive means live sold higher than backtest; negative means worse."""
    base = as_float(bt_exit)
    if live_exit in (None, "") or not base:
        return ""
    return f"{(as_float(live_exit) - base) / base * 10000.0:.2f}"


def signed_roundtrip_edge_bps(entry_edge: Any, exit_edge: Any) -> str:
    if entry_edge in (None, "") or exit_edge in (None, ""):
        return ""
    return f"{as_float(entry_edge) + as_float(exit_edge):.2f}"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    preferred = [
        "date",
        "symbol",
        "match_status",
        "bt_entry_price",
        "live_entry_price",
        "entry_diff_abs",
        "entry_diff_bps",
        "entry_edge_bps",
        "bt_exit_price",
        "live_exit_price",
        "exit_diff_abs",
        "exit_diff_bps",
        "exit_edge_bps",
        "roundtrip_execution_edge_bps",
        "bt_weight",
        "live_weight",
        "weight_diff_pctpt",
        "bt_qty",
        "live_qty",
        "bt_notional",
        "live_notional",
        "bt_pnl_pct",
        "bt_close_date",
        "live_entry_time_utc",
        "live_exit_time_utc",
        "live_buy_client_order_id",
        "live_sell_client_order_id",
        "diagnostic_note",
    ]
    extras = sorted({k for r in rows for k in r} - set(preferred))
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=preferred + extras)
        writer.writeheader()
        writer.writerows(rows)


def write_summary_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "date",
        "status",
        "matched",
        "backtest_only",
        "live_only",
        "matched_symbols",
        "backtest_only_symbols",
        "live_only_symbols",
        "avg_entry_edge_bps",
        "avg_exit_edge_bps",
        "avg_leg_edge_bps",
        "avg_roundtrip_execution_edge_bps",
        "max_abs_weight_diff_pctpt",
        "note",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(row["date"], []).append(row)
    out = []
    for date, group in sorted(by_date.items()):
        matched = [r for r in group if r["match_status"] == "matched"]
        bt_only = [r for r in group if r["match_status"] == "backtest_only"]
        live_only = [r for r in group if r["match_status"] == "live_only"]
        entry_edges = [as_float(r["entry_edge_bps"]) for r in matched if r.get("entry_edge_bps")]
        exit_edges = [as_float(r["exit_edge_bps"]) for r in matched if r.get("exit_edge_bps")]
        roundtrip_edges = [
            as_float(r["roundtrip_execution_edge_bps"])
            for r in matched
            if r.get("roundtrip_execution_edge_bps")
        ]
        avg_entry = avg(entry_edges)
        avg_exit = avg(exit_edges)
        avg_leg = avg(entry_edges + exit_edges)
        avg_roundtrip = avg(roundtrip_edges)
        if matched and not bt_only and not live_only:
            status = "OK"
            note = edge_note(avg_roundtrip)
        elif matched:
            status = "PARTIAL"
            note = "matched with composition drift"
        elif live_only and not bt_only:
            status = "LIVE_ONLY"
            note = "no backtest trades for this date/symbol set"
        elif bt_only and not live_only:
            status = "BACKTEST_ONLY"
            note = "backtest expected trades not seen live"
        else:
            status = "NO_MATCH"
            note = "no overlapping symbols"
        out.append(
            {
                "date": date,
                "status": status,
                "matched": len(matched),
                "backtest_only": len(bt_only),
                "live_only": len(live_only),
                "matched_symbols": symbols(matched),
                "backtest_only_symbols": symbols(bt_only),
                "live_only_symbols": symbols(live_only),
                "avg_entry_edge_bps": f"{avg_entry:.2f}",
                "avg_exit_edge_bps": f"{avg_exit:.2f}",
                "avg_leg_edge_bps": f"{avg_leg:.2f}",
                "avg_roundtrip_execution_edge_bps": f"{avg_roundtrip:.2f}",
                "max_abs_weight_diff_pctpt": max([abs(as_float(r["weight_diff_pctpt"])) for r in matched if r.get("weight_diff_pctpt")] or [0.0]),
                "note": note,
            }
        )
    return out


def symbols(rows: list[dict[str, Any]]) -> str:
    return " ".join(sorted(str(r.get("symbol", "")) for r in rows if r.get("symbol")))


def edge_note(avg_roundtrip_edge_bps: float) -> str:
    if avg_roundtrip_edge_bps > 0:
        return "live better than backtest"
    if avg_roundtrip_edge_bps < 0:
        return "live worse than backtest"
    return "neutral execution edge"


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def overall_diagnostics(rows: list[dict[str, Any]], summary_rows: list[dict[str, Any]]) -> dict[str, Any]:
    matched = [r for r in rows if r["match_status"] == "matched"]
    bt_total = sum(1 for r in rows if r["match_status"] in ("matched", "backtest_only"))
    live_total = sum(1 for r in rows if r["match_status"] in ("matched", "live_only"))
    entry_edges = [as_float(r["entry_edge_bps"]) for r in matched if r.get("entry_edge_bps")]
    exit_edges = [as_float(r["exit_edge_bps"]) for r in matched if r.get("exit_edge_bps")]
    roundtrip_edges = [
        as_float(r["roundtrip_execution_edge_bps"])
        for r in matched
        if r.get("roundtrip_execution_edge_bps")
    ]
    live_only_dates = [r["date"] for r in summary_rows if r["status"] == "LIVE_ONLY"]
    bt_only_dates = [r["date"] for r in summary_rows if r["status"] == "BACKTEST_ONLY"]
    partial_dates = [r["date"] for r in summary_rows if r["status"] == "PARTIAL"]
    return {
        "matched": len(matched),
        "bt_total": bt_total,
        "live_total": live_total,
        "match_rate_vs_backtest": len(matched) / bt_total if bt_total else 0.0,
        "match_rate_vs_live": len(matched) / live_total if live_total else 0.0,
        "avg_entry_edge_bps": avg(entry_edges),
        "avg_exit_edge_bps": avg(exit_edges),
        "avg_leg_edge_bps": avg(entry_edges + exit_edges),
        "avg_roundtrip_execution_edge_bps": avg(roundtrip_edges),
        "live_only_dates": live_only_dates,
        "backtest_only_dates": bt_only_dates,
        "partial_dates": partial_dates,
    }


def print_overall_diagnostics(diag: dict[str, Any]) -> None:
    print("\nOverall alignment (+ bps = live better, - bps = live worse):")
    print(
        f"  matched={diag['matched']} bt_total={diag['bt_total']} live_total={diag['live_total']} "
        f"match_vs_bt={diag['match_rate_vs_backtest']:.1%} "
        f"match_vs_live={diag['match_rate_vs_live']:.1%}"
    )
    print(
        f"  avg_entry_edge={diag['avg_entry_edge_bps']:+.2f}bps "
        f"avg_exit_edge={diag['avg_exit_edge_bps']:+.2f}bps "
        f"avg_leg_edge={diag['avg_leg_edge_bps']:+.2f}bps "
        f"avg_roundtrip_edge={diag['avg_roundtrip_execution_edge_bps']:+.2f}bps"
    )
    print(
        "  suggested backtest slippage cost: "
        f"{max(0.0, -diag['avg_leg_edge_bps']):.2f} bps per leg "
        f"({max(0.0, -diag['avg_roundtrip_execution_edge_bps']):.2f} bps roundtrip)"
    )
    if diag["partial_dates"] or diag["live_only_dates"] or diag["backtest_only_dates"]:
        print(
            "  composition drift dates: "
            f"partial={','.join(diag['partial_dates']) or '-'} "
            f"live_only={','.join(diag['live_only_dates']) or '-'} "
            f"backtest_only={','.join(diag['backtest_only_dates']) or '-'}"
        )
    if diag["match_rate_vs_live"] < 0.8 or diag["match_rate_vs_backtest"] < 0.8:
        print("  alignment warning: composition differs materially; inspect symbols before trusting slippage bps.")


def main() -> int:
    args = parse_args()
    cutoff = dt.date.fromisoformat(args.cutoff_date)
    env_file = Path(args.env_file).expanduser().resolve()
    env = read_env_file(env_file)
    os.environ.update(env)

    if args.run_backtest:
        trades_path = run_backtest(args, cutoff)
    elif args.backtest_trades:
        trades_path = Path(args.backtest_trades).expanduser().resolve()
    else:
        raise SystemExit("Provide --run-backtest or --backtest-trades")

    bt_rows, dropped_bt_note = load_backtest_trades(
        trades_path,
        cutoff,
        drop_last_entry_date=False,
    )
    orders = fetch_alpaca_orders(args, env, cutoff)
    live_rows = build_live_roundtrips(orders, cutoff)
    dropped_live_note = ""
    if not args.keep_last_live_date:
        live_rows, dropped_live_note = drop_last_live_entry_date(live_rows)
    rows = merge_rows(bt_rows, live_rows, args.price_display)

    out = Path(args.out or (REPO_ROOT / "out" / "reconciliation" / f"bt_vs_live_{cutoff:%Y%m%d}.csv")).resolve()
    write_csv(out, rows)
    print(f"Wrote comparison CSV: {out}")
    print(f"Backtest trades: {len(bt_rows)} | Live roundtrips: {len(live_rows)} | Rows: {len(rows)}")
    if dropped_bt_note:
        print(f"NOTE: {dropped_bt_note}")
    if dropped_live_note:
        print(f"NOTE: {dropped_live_note}")
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["match_status"]] = status_counts.get(row["match_status"], 0) + 1
    print("Status:", ", ".join(f"{k}={v}" for k, v in sorted(status_counts.items())))

    summary_rows = summarize(rows)
    diag = overall_diagnostics(rows, summary_rows)
    if args.summary_out:
        summary_path = Path(args.summary_out).expanduser().resolve()
        write_summary_csv(summary_path, summary_rows)
        print(f"Wrote daily summary CSV: {summary_path}")
    if summary_rows:
        print("\nDaily summary (+ bps = live better, - bps = live worse):")
        for row in summary_rows[-10:]:
            print(
                f"{row['date']} {row['status']:<13} matched={row['matched']} "
                f"bt_only={row['backtest_only']} live_only={row['live_only']} "
                f"entry_edge={as_float(row['avg_entry_edge_bps']):+.2f}bps "
                f"exit_edge={as_float(row['avg_exit_edge_bps']):+.2f}bps "
                f"rt_edge={as_float(row['avg_roundtrip_execution_edge_bps']):+.2f}bps "
                f"symbols_live_only={row['live_only_symbols']}"
            )
        print_overall_diagnostics(diag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
