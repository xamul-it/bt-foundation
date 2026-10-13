#!/usr/bin/env python3
"""Keep ZMQ quote subscriptions alive for AH spread logging."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import zmq
from alpaca.trading.client import TradingClient


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--proxy-addr", default=os.environ.get("PROXY_ADDR", "tcp://localhost:5555"))
    parser.add_argument("--symbols", default="", help="Comma-separated symbols to subscribe.")
    parser.add_argument("--symbols-file", type=Path, default=None, help="JSON/list/text file with symbols.")
    parser.add_argument("--positions", action="store_true", help="Subscribe live Alpaca position symbols.")
    parser.add_argument("--positions-once", action="store_true", help="Read Alpaca positions only once at startup.")
    parser.add_argument("--paper", action="store_true", help="Use Alpaca paper endpoint for positions.")
    parser.add_argument("--refresh-seconds", type=float, default=30.0)
    parser.add_argument("--heartbeat-seconds", type=float, default=10.0)
    parser.add_argument(
        "--resubscribe-seconds",
        type=float,
        default=0.0,
        help="Clear local subscription state periodically so the proxy is re-primed after restarts. 0 disables.",
    )
    parser.add_argument(
        "--run-until-next-et",
        default="",
        help="Keep subscriptions alive until the next occurrence of HH:MM in America/New_York.",
    )
    parser.add_argument(
        "--require-session-window-et",
        default="",
        help="Exit without subscribing unless current New York time is inside START-END, e.g. 16:05-10:00.",
    )
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def credentials() -> tuple[str, str]:
    api_key = os.environ.get("ALPACA_API_KEY") or os.environ.get("BROKER_API_KEY")
    secret_key = os.environ.get("ALPACA_SECRET_KEY") or os.environ.get("BROKER_SECRET_KEY")
    if not api_key or not secret_key:
        raise SystemExit("Missing ALPACA_API_KEY/ALPACA_SECRET_KEY or BROKER_API_KEY/BROKER_SECRET_KEY")
    return api_key, secret_key


def split_symbols(text: str) -> set[str]:
    return {item.strip().upper() for item in text.split(",") if item.strip()}


def load_symbols_file(path: Path) -> set[str]:
    text = path.read_text().strip()
    if not text:
        return set()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {line.strip().upper() for line in text.splitlines() if line.strip()}
    if isinstance(payload, list):
        return {str(item).strip().upper() for item in payload if str(item).strip()}
    if isinstance(payload, dict):
        for key in ("symbols", "tickers", "assets"):
            value = payload.get(key)
            if isinstance(value, list):
                return {str(item).strip().upper() for item in value if str(item).strip()}
        return {str(key).strip().upper() for key in payload.keys() if str(key).strip()}
    return set()


def position_symbols(client: TradingClient) -> set[str]:
    positions = client.get_all_positions()
    return {str(getattr(pos, "symbol", "")).strip().upper() for pos in positions if str(getattr(pos, "symbol", "")).strip()}


def request(socket: zmq.Socket, text: str, timeout_ms: int = 3000) -> list[bytes]:
    socket.send_multipart([b"", text.encode("utf-8")])
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    events = dict(poller.poll(timeout_ms))
    if socket not in events:
        raise TimeoutError(f"proxy timeout for {text}")
    return socket.recv_multipart()


def proxy_metadata(reply: list[bytes]) -> dict:
    for frame in reply:
        if not frame or frame in {b"ACK", b"ACK-QUOTE", b"PONG", b"CONFIG"}:
            continue
        try:
            payload = json.loads(frame.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def recommended_heartbeat_seconds(metadata: dict, fallback: float) -> float:
    try:
        value = float(metadata.get("recommended_heartbeat_s", fallback))
    except (TypeError, ValueError):
        return fallback
    return max(1.0, min(value, fallback if fallback > 0 else value))


def next_et_deadline(hhmm: str) -> float | None:
    value = str(hhmm or "").strip()
    if not value:
        return None
    hour_s, minute_s = value.split(":", 1)
    tz = ZoneInfo("America/New_York")
    now = dt.datetime.now(tz)
    target = now.replace(hour=int(hour_s), minute=int(minute_s), second=0, microsecond=0)
    if target <= now:
        target += dt.timedelta(days=1)
    return target.timestamp()


def parse_hhmm(value: str) -> dt.time:
    hour_s, minute_s = value.split(":", 1)
    return dt.time(hour=int(hour_s), minute=int(minute_s))


def in_et_window(window: str) -> bool:
    value = str(window or "").strip()
    if not value:
        return True
    start_s, end_s = value.split("-", 1)
    start = parse_hhmm(start_s.strip())
    end = parse_hhmm(end_s.strip())
    now = dt.datetime.now(ZoneInfo("America/New_York")).time().replace(second=0, microsecond=0)
    if start <= end:
        return start <= now < end
    return now >= start or now < end


def main() -> None:
    args = parse_args()
    if not in_et_window(args.require_session_window_et):
        print(f"outside_session_window_et={args.require_session_window_et} exiting")
        return

    fixed = split_symbols(args.symbols)
    if args.symbols_file:
        fixed |= load_symbols_file(args.symbols_file if args.symbols_file.is_absolute() else ROOT / args.symbols_file)
    if not fixed and not args.positions:
        raise SystemExit("Provide --symbols/--symbols-file or --positions")

    trading_client = None
    if args.positions:
        api_key, secret_key = credentials()
        trading_client = TradingClient(api_key, secret_key, paper=bool(args.paper))

    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect(args.proxy_addr)

    subscribed: set[str] = set()
    last_heartbeat = 0.0
    last_resubscribe = time.monotonic()
    heartbeat_seconds = float(args.heartbeat_seconds)
    deadline_epoch = next_et_deadline(args.run_until_next_et)
    position_cache: set[str] = set()
    if args.positions and args.positions_once and trading_client is not None:
        position_cache = position_symbols(trading_client)
        print(f"positions_once symbols={sorted(position_cache)}")

    try:
        while True:
            if deadline_epoch is not None and time.time() >= deadline_epoch:
                print(f"deadline_reached_et={args.run_until_next_et}")
                break
            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_seconds:
                try:
                    request(socket, "HEARTBEAT", timeout_ms=2000)
                except Exception as exc:
                    print(f"heartbeat_error={exc}")
                    subscribed.clear()
                last_heartbeat = now
            if args.resubscribe_seconds and now - last_resubscribe >= float(args.resubscribe_seconds):
                subscribed.clear()
                last_resubscribe = now
                print("resubscribe_due=true")

            wanted = set(fixed)
            if position_cache:
                wanted |= position_cache
            elif trading_client is not None:
                wanted |= position_symbols(trading_client)

            for symbol in sorted(wanted - subscribed):
                reply = request(socket, f"QUOTE_META:{symbol}")
                metadata = proxy_metadata(reply)
                if metadata:
                    next_heartbeat = recommended_heartbeat_seconds(metadata, heartbeat_seconds)
                    if next_heartbeat != heartbeat_seconds:
                        heartbeat_seconds = next_heartbeat
                        print(f"proxy_metadata {metadata} heartbeat_seconds={heartbeat_seconds:g}")
                subscribed.add(symbol)
                print(f"subscribed_quote {symbol} reply={reply}")

            if args.once:
                break
            time.sleep(max(1.0, min(args.refresh_seconds, heartbeat_seconds)))
    finally:
        try:
            request(socket, "DISCONNECT", timeout_ms=1000)
        except Exception:
            pass
        socket.close(0)
        context.term()


if __name__ == "__main__":
    main()
