#!/usr/bin/env python3
"""Requirement A (execution reconciliation): for one scheduled profile and
one trading day, replay a backtest with the STRATARGS/commit that were
actually in force that day (resolved via profile_param_versions, Phase 1)
and compare it against the real Alpaca orders cached by
watchtower_poll_alpaca_orders.py, classifying every divergence.

Designed to run once a day (see the module docstring bottom for the
proposed schedule) over the reconciliation_queue (D-bis catch-up): a
trading day is only ever compared once it is "closed" (all entry-day
orders terminal + evidence the exit leg for that day's positions has run),
using the parameters resolved for THAT trading date -- never whatever is
current when the catch-up itself executes.

Historical code fidelity: the replay always runs against a `git worktree`
checkout of the profile's own bt-core repo at the exact `core_commit`
resolved for that day, in a scratch directory under
bin/watchtower_scratch/ (never inside the live `backtrader`/
`backtrader-prod` checkouts) -- removed after the run. When a resolved
version has no recorded `core_commit` (e.g. a Phase-1 `reconstructed` row,
which cannot know it), this falls back to the nearest bt-core commit at or
before the trading date by commit date, and the result is explicitly
labelled `core_commit_resolution="nearest_by_date"` instead of `"exact"`
so this is never mistaken for a verified historical replay.

`--profile` is free text, resolved dynamically from
~/.config/backtrader/scheduled/*.env -- no hardcoded profile/strategy
names or checkout paths anywhere in this file.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import watchtower_runtime as wr  # noqa: E402

WORKSPACE_ROOT = Path(os.environ.get("BT_WORKSPACE_ROOT", str(Path.home() / "backtrader"))).resolve()
SCHEDULED_PROFILES_DIR = Path(
    os.environ.get("BT_SCHEDULED_PROFILES_DIR", str(Path.home() / ".config" / "backtrader" / "scheduled"))
)
ACCOUNTS_DIR = Path(
    os.environ.get("BT_SCHEDULED_ACCOUNTS_DIR", str(Path.home() / ".config" / "backtrader" / "accounts"))
)
SCRATCH_ROOT = Path(os.environ.get("BT_WATCHTOWER_SCRATCH", str(Path(__file__).resolve().parent / "watchtower_scratch")))

TERMINAL_ORDER_STATUSES = {
    "filled", "canceled", "rejected", "expired", "done_for_day", "replaced", "stopped", "suspended",
}
DEFAULT_FROM_DAYS = 420
DEFAULT_MARGIN_LEVERAGE = "2"
DEFAULT_ALPACA_FEED = "sip"


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def discover_profiles() -> dict[str, dict[str, str]]:
    profiles: dict[str, dict[str, str]] = {}
    if not SCHEDULED_PROFILES_DIR.is_dir():
        return profiles
    for env_file in sorted(SCHEDULED_PROFILES_DIR.glob("*.env")):
        profiles[_read_env_file(env_file).get("ROLE") or env_file.stem] = _read_env_file(env_file)
    return profiles


def resolve_account_credentials(profile_env: dict[str, str]) -> dict[str, str]:
    account_env_path = profile_env.get("ACCOUNT_ENV")
    path = Path(account_env_path) if account_env_path else None
    if path is not None and not path.is_absolute():
        path = ACCOUNTS_DIR / path
    creds = _read_env_file(path) if path else {}
    key = creds.get("ALPACA_API_KEY") or creds.get("BROKER_API_KEY")
    secret = creds.get("ALPACA_SECRET_KEY") or creds.get("BROKER_SECRET_KEY")
    if not key or not secret:
        raise ValueError(f"no Alpaca credentials found for account env {account_env_path!r}")
    return {"key": key, "secret": secret}


# ---------------------------------------------------------------------
# D-bis: "is this trading day closed" (never compare an open day)
# ---------------------------------------------------------------------

def is_day_closed(repo: "wr.WatchtowerRepository", profile: str, trading_date: date) -> dict[str, Any]:
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, symbol FROM alpaca_order_cache
                WHERE source_account = %s AND window_open = %s AND side = 'buy'
                """,
                (profile, trading_date),
            )
            entry_rows = wr._cursor_rows(cur)
            cur.execute(
                """
                SELECT DISTINCT symbol FROM alpaca_order_cache
                WHERE source_account = %s AND side = 'sell'
                  AND window_open > %s AND window_open <= %s
                  AND status = ANY(%s)
                """,
                (profile, trading_date, trading_date + timedelta(days=7), list(TERMINAL_ORDER_STATUSES)),
            )
            exit_symbols = {r["symbol"] for r in wr._cursor_rows(cur)}

    if not entry_rows:
        return {"closed": False, "reason": "no_entry_orders_cached"}
    non_terminal = [r for r in entry_rows if (r["status"] or "").lower() not in TERMINAL_ORDER_STATUSES]
    if non_terminal:
        return {"closed": False, "reason": "entry_orders_not_terminal", "symbols": [r["symbol"] for r in non_terminal]}
    entry_symbols = {r["symbol"] for r in entry_rows}
    exit_evidence = entry_symbols & exit_symbols
    if not exit_evidence:
        return {"closed": False, "reason": "no_exit_leg_evidence_yet"}
    return {"closed": True, "entry_symbols": sorted(entry_symbols), "exit_evidence_symbols": sorted(exit_evidence)}


# ---------------------------------------------------------------------
# Historical code checkout (git worktree, scratch dir, never the live checkouts)
# ---------------------------------------------------------------------

def resolve_core_commit(bt_core_repo: Path, trading_date: date, recorded_core_commit: str | None) -> tuple[str, str]:
    if recorded_core_commit:
        return recorded_core_commit, "exact"
    out = subprocess.run(
        ["git", "-C", str(bt_core_repo), "log", "-1", "--format=%H",
         f"--before={trading_date.isoformat()} 23:59:59"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    if not out:
        raise RuntimeError(f"no bt-core commit found at or before {trading_date} in {bt_core_repo}")
    return out, "nearest_by_date"


def create_worktree(bt_core_repo: Path, commit: str, run_tag: str, shared_config: Path) -> Path:
    SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
    scratch_dir = SCRATCH_ROOT / f"{run_tag}_{uuid.uuid4().hex[:8]}"
    worktree_bt_core = scratch_dir / "bt-core"
    subprocess.run(
        ["git", "-C", str(bt_core_repo), "worktree", "add", "--detach", str(worktree_bt_core), commit],
        check=True, capture_output=True, text=True,
    )
    (scratch_dir / "out").mkdir(parents=True, exist_ok=True)
    # Most of the codebase resolves config-common via config_paths.resolve_shared_path()
    # (REPO_ROOT/config-common, REPO_ROOT = bt-core's parent -- handled by the
    # BT_SHARED_CONFIG env var set in run_replay()). A few strategy params
    # (e.g. OvernightAHFlatComposite's monthly_universe_indicator_panel) are
    # instead a raw 'config-common/...' string opened directly, which
    # resolves against the *working directory* (this worktree's bt-core),
    # bypassing that env var entirely. A symlink covers both resolution
    # paths without touching strategy code.
    if shared_config.is_dir():
        (worktree_bt_core / "config-common").symlink_to(shared_config, target_is_directory=True)
    return worktree_bt_core


def remove_worktree(bt_core_repo: Path, worktree_bt_core: Path) -> None:
    scratch_dir = worktree_bt_core.parent
    subprocess.run(
        ["git", "-C", str(bt_core_repo), "worktree", "remove", "--force", str(worktree_bt_core)],
        capture_output=False, check=False,
    )
    shutil.rmtree(scratch_dir, ignore_errors=True)
    subprocess.run(["git", "-C", str(bt_core_repo), "worktree", "prune"], check=False)


# ---------------------------------------------------------------------
# Historical equity (for --cash)
# ---------------------------------------------------------------------

def resolve_historical_cash(profile_env: dict[str, str], trading_date: date) -> float | None:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import GetPortfolioHistoryRequest

    creds = resolve_account_credentials(profile_env)
    paper = (profile_env.get("TRADING_MODE") or "paper").strip().lower() != "live"
    client = TradingClient(api_key=creds["key"], secret_key=creds["secret"], paper=paper)
    days_back = max((datetime.now(timezone.utc).date() - trading_date).days + 5, 10)
    req = GetPortfolioHistoryRequest(period=f"{days_back}D", timeframe="1D",
                                      intraday_reporting="market_hours", pnl_reset="per_day")
    hist = client.get_portfolio_history(req)
    if not hist.timestamp:
        return None
    best: tuple[date, float] | None = None
    for ts, eq in zip(hist.timestamp, hist.equity):
        d = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        if d <= trading_date and (best is None or d > best[0]):
            best = (d, eq)
    return float(best[1]) if best else None


# ---------------------------------------------------------------------
# Replay execution
# ---------------------------------------------------------------------

def outpath_for(bt_core: Path, strat: str, run_id: str) -> Path:
    parts = strat.split(".")
    base = bt_core / "out" / parts[0].lower() / parts[1] if len(parts) == 2 else bt_core / "out" / strat
    return base / run_id


def build_stratargs_string(stratargs: dict[str, Any]) -> str:
    return " ".join(f"{key}={value!r}" for key, value in stratargs.items())


def backtest_stratargs(stratargs: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """`auction` is a broker-execution flag, not a strategy decision: the
    operational profiles run `auction=False` (market/GTC entry, real close
    handled the next day by the separate moo-exit cron), but replayed in
    BACKTEST mode that produces same-bar entry/exit at an identical price
    -- every trade PnL=0 -- documented in
    docs/context/alpaca_paper_live_overnight_ah.md and ah_context.md as
    "l'unico parametro che va tradotto, non copiato" for a backtest replay.

    overnight_ah.py itself has auto-forced auction=True in backtest mode
    since bt-core commit ce1cd555 (2026-08-18). Historical replays against
    an EARLIER commit (resolved via resolve_core_commit for any
    trading_date before that fix landed) do not get that safety net, and
    silently reproduce the exact degenerate all-zero-PnL run the fix was
    written to prevent -- confirmed 2026-08-28 while investigating a
    PNL=0/SQN=None failure pattern on pre-2026-08-18 development replays:
    trades.json showed a full trading window with real data on both sides
    of trading_date, ruling out a --todate windowing issue; the strategy's
    own per-trade log showed "PnL≈0.00" on literally every fill because
    auction stayed False on that older commit. Forcing it here makes the
    replay robust across ALL historical commits, not just the ones new
    enough to force it themselves.
    """
    forced = not bool(stratargs.get("auction", True))
    if forced:
        stratargs = {**stratargs, "auction": True}
    return stratargs, forced


def replay_data_key(profile_env: dict[str, str], version: dict[str, Any]) -> tuple[str, str, str]:
    """The market-data cache is shared by a profile checkout, so refresh it
    at most once for each ticker/provider/feed combination in a catch-up."""
    extra = version.get("metadata") or {}
    ticker = str(extra.get("TICKER") or profile_env.get("TICKER") or "").strip()
    provider = str(extra.get("DATA_PROVIDER") or profile_env.get("DATA_PROVIDER") or "yahoo").strip()
    alpaca_feed = str(extra.get("ALPACA_FEED") or DEFAULT_ALPACA_FEED).strip()
    if not ticker:
        raise ValueError("profile/version has no TICKER; cannot refresh replay market data")
    return ticker, provider, alpaca_feed


def refresh_replay_market_data(worktree_bt_core: Path, profile_env: dict[str, str], version: dict[str, Any]) -> tuple[str, str, str]:
    """Bring the profile's local feed through today before a reconciliation
    replay.  A trade opened on the prior session is absent from trades.json
    until the following-session bar exists to close the round trip.

    This deliberately runs the historical worktree's ``load_tickers.py`` but
    points it at the profile's shared config/data directory, exactly as the
    scheduled entry handler does.  It never changes the strategy checkout.
    """
    ticker, provider, alpaca_feed = replay_data_key(profile_env, version)
    code_root = Path(profile_env["CODE_ROOT"]).resolve()
    env = os.environ.copy()
    env["BT_SHARED_CONFIG"] = str(code_root / "config-common")
    account_env = profile_env.get("ACCOUNT_ENV")
    if account_env:
        account_path = Path(account_env)
        if not account_path.is_absolute():
            account_path = ACCOUNTS_DIR / account_path
        env.update(_read_env_file(account_path))
    python = code_root / "bt-core" / ".venv" / "bin" / "python"
    if not python.is_file():
        python = BT_CORE / ".venv" / "bin" / "python"
    cmd = [
        str(python), "load_tickers.py",
        "--ticker", ticker,
        "--provider", provider,
        "--alpaca-feed", alpaca_feed,
        "--timeframe", "d",
        "--incremental",
    ]
    print("Refreshing replay market data:", " ".join(cmd), file=sys.stderr)
    subprocess.run(cmd, cwd=str(worktree_bt_core), env=env, check=True, timeout=900)
    return ticker, provider, alpaca_feed


def run_replay(worktree_bt_core: Path, profile_env: dict[str, str], version: dict[str, Any],
                trading_date: date, run_id: str, cash: float | None) -> tuple[Path, bool]:
    extra = version.get("metadata") or {}
    ticker = extra.get("TICKER") or profile_env.get("TICKER")
    provider = extra.get("DATA_PROVIDER") or profile_env.get("DATA_PROVIDER") or "yahoo"
    alpaca_feed = extra.get("ALPACA_FEED") or DEFAULT_ALPACA_FEED
    margin_leverage = extra.get("MARGIN_LEVERAGE") or DEFAULT_MARGIN_LEVERAGE
    try:
        from_days = int(extra.get("FROM_DAYS") or DEFAULT_FROM_DAYS)
    except (TypeError, ValueError):
        from_days = DEFAULT_FROM_DAYS
    fromdate = trading_date - timedelta(days=from_days)
    # IMPORTANT: --todate is NOT trading_date. Backtrader's own trade log
    # (trades.json) only records *closed* round-trips -- an entry opened on
    # the very last bar of the feed has no following bar to close/log
    # against and silently never appears in trades.json, which looks
    # exactly like "no signal that day" (a false extra_live_order
    # divergence) even when the entry decision was identical. Extending
    # todate a few calendar days past trading_date gives the engine room
    # to close that round-trip; it does NOT leak future data into the
    # trading_date entry decision itself, because Backtrader is strictly
    # sequential/event-driven -- next() on the trading_date bar never sees
    # later bars. bin/compare_live_backtest_trades.py already relies on
    # this same property (its --todate defaults to today, never to the
    # cutoff date), this mirrors that established pattern.
    todate = min(trading_date + timedelta(days=10), date.today())

    code_root = Path(profile_env["CODE_ROOT"]).resolve()
    shared_config = code_root / "config-common"

    replay_stratargs, auction_forced = backtest_stratargs(version["stratargs"])

    dev_python = BT_CORE / ".venv" / "bin" / "python"
    cmd = [
        str(dev_python), "btmain.py",
        "--strat", version["strategy"],
        "--ticker", ticker,
        "--fromdate", fromdate.isoformat(),
        "--todate", todate.isoformat(),
        "--timeframe", "daily",
        "--provider", provider,
        "--alpaca-feed", alpaca_feed,
        "--commission", "none",
        "--margin-leverage", str(margin_leverage),
        "--mode", "backtest",
        "--id", run_id,
        "--stratargs", build_stratargs_string(replay_stratargs),
    ]
    if cash:
        cmd.extend(["--cash", str(cash)])

    env = os.environ.copy()
    env["BT_SHARED_CONFIG"] = str(shared_config)
    print("Running replay:", " ".join(cmd), file=sys.stderr)
    subprocess.run(cmd, cwd=str(worktree_bt_core), env=env, check=True)

    run_outpath = outpath_for(worktree_bt_core, version["strategy"], run_id)
    trades_path = run_outpath / "trades.json"
    if not trades_path.exists():
        existing = ", ".join(sorted(p.name for p in run_outpath.glob("*"))) if run_outpath.exists() else "outpath missing"
        raise FileNotFoundError(f"replay produced no trades.json ({run_outpath}); found: {existing}")
    return trades_path, auction_forced


# ---------------------------------------------------------------------
# Comparison / classification (taxonomy from
# docs/context/alpaca_paper_live_overnight_ah.md, extended per requirement A)
# ---------------------------------------------------------------------

def load_backtest_entries(trades_path: Path, trading_date: date) -> dict[str, dict[str, Any]]:
    """Per-symbol backtest round trip for `trading_date`: entry price/qty
    plus the derived exit price and the trade's own PnL. The replay
    trades.json has no explicit exit price, so it is reconstructed from
    `pnl`/`size` (pnl is signed, size is the entry position). `bt_pnl_pct`
    is notional-based (`pnl / value`) -- the same definition used for the
    per-day return in classify()."""
    raw = json.loads(trades_path.read_text(encoding="utf-8"))
    entries: dict[str, dict[str, Any]] = {}
    for item in raw:
        symbol = str(item.get("asset") or item.get("symbol") or "").upper()
        # A reconciliation window belongs to the actual opening/fill date.
        # ``close_datetime`` is the exit leg and must never select the
        # window: using it shifts every overnight trade to the following
        # session.  This exporter records the opening execution in
        # ``open_datetime``; keep the other entry fields only for older
        # export formats.
        entry_dt = (item.get("open_datetime") or item.get("entry_datetime")
                    or item.get("entry_signal_dt"))
        if not symbol or not entry_dt:
            continue
        entry_date = str(entry_dt)[:10]
        if entry_date != trading_date.isoformat():
            continue
        entry_price = item.get("price") or item.get("entry_price")
        size = abs(float(item.get("size") or item.get("qty") or 0))
        pnl = item.get("pnl")
        value = item.get("value")
        exit_price = None
        if entry_price is not None and pnl is not None and size:
            exit_price = round(float(entry_price) + float(pnl) / size, 6)
        pnl_pct = None
        if pnl is not None and value:
            pnl_pct = round(float(pnl) / float(value) * 100, 4)
        entries[symbol] = {
            "symbol": symbol,
            "bt_entry_price": entry_price,
            "bt_qty": size,
            "bt_exit_price": exit_price,
            "bt_exit_qty": size,
            "bt_pnl": float(pnl) if pnl is not None else None,
            "bt_value": float(value) if value is not None else None,
            "bt_pnl_pct": pnl_pct,
        }
    return entries


def fetch_live_exit_orders(repo: "wr.WatchtowerRepository", profile: str, trading_date: date) -> dict[str, dict[str, Any]]:
    """Filled sell orders that close `trading_date`'s positions. overnight_ah
    exits on the NEXT trading session (not calendar day+1: a Friday entry
    exits Monday), and those sells are cached under that session's
    window_open. Resolve the next session from the cache itself -- the
    earliest sell window_open strictly after trading_date within a week --
    then take that batch. Keyed by symbol.

    A fallback can complete an order that was partially filled before its
    original leg expired.  Therefore the exit is the *sum of fills* of all
    sell orders for the symbol in that next-session batch, not simply the
    single order whose final status is ``filled``."""
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT MIN(window_open) AS next_session
                FROM alpaca_order_cache
                WHERE source_account = %s AND side = 'sell'
                  AND window_open > %s AND window_open <= %s
                """,
                (profile, trading_date, trading_date + timedelta(days=7)),
            )
            row = wr._cursor_row(cur)
            next_session = row.get("next_session") if row else None
            if not next_session:
                return {}
            cur.execute(
                """
                SELECT symbol, status, qty, filled_qty, filled_avg_price,
                       client_order_id, order_type,
                       LOWER(COALESCE(raw_payload->>'time_in_force', '')) AS time_in_force
                FROM alpaca_order_cache
                WHERE source_account = %s AND window_open = %s AND side = 'sell'
                ORDER BY symbol, submitted_at, created_at, alpaca_order_id
                """,
                (profile, next_session),
            )
            rows = wr._cursor_rows(cur)
    by_symbol: dict[str, dict[str, Any]] = {}
    for order in rows:
        sym = order["symbol"]
        filled_qty = float(order.get("filled_qty") or 0)
        filled_price = order.get("filled_avg_price")
        current = by_symbol.setdefault(sym, {
            "symbol": sym,
            "status": None,
            "qty": 0.0,
            "filled_qty": 0.0,
            "filled_notional": 0.0,
            "has_filled_order": False,
            "statuses": [],
            "auction_code": None,
            "auction_filled_qty": 0.0,
            "has_market_leg": False,
        })
        current["qty"] = max(current["qty"], float(order.get("qty") or 0))
        current["filled_qty"] += filled_qty
        if filled_qty and filled_price is not None:
            current["filled_notional"] += filled_qty * float(filled_price)
        status = (order.get("status") or "unknown").lower()
        current["statuses"].append(status)
        current["has_filled_order"] = current["has_filled_order"] or status == "filled"
        tif = str(order.get("time_in_force") or "").lower()
        if tif in {"cls", "opg"}:
            current["auction_code"] = "c" if tif == "cls" else "o"
            current["auction_filled_qty"] += filled_qty
        else:
            current["has_market_leg"] = True
        if tif in {"cls", "opg"} and status in {"expired", "canceled", "cancelled", "rejected"}:
            current["auction_unfilled"] = True

    for current in by_symbol.values():
        if current["filled_qty"] and current["filled_notional"]:
            current["filled_avg_price"] = current["filled_notional"] / current["filled_qty"]
        else:
            current["filled_avg_price"] = None
        # A partial fill on an expired leg is still a real execution.  Mark
        # the aggregate as filled so classify() evaluates its quantity; an
        # insufficient aggregate then becomes exit_partial_fill.
        current["status"] = "filled" if current["filled_qty"] else current["statuses"][0]
        auction_code = current.pop("auction_code")
        auction_filled_qty = current.pop("auction_filled_qty")
        market_leg = current.pop("has_market_leg")
        unfilled_auction = current.pop("auction_unfilled", False)
        # f already means the residual was closed at market.  Show o only
        # for an actual OPG fill; a zero-fill OPG followed by fallback is f.
        if unfilled_auction and market_leg:
            current["execution_codes"] = ([auction_code] if auction_filled_qty > 0 and auction_code else []) + ["f"]
        elif auction_code:
            current["execution_codes"] = [auction_code]
        elif market_leg:
            current["execution_codes"] = ["m"]
        else:
            current["execution_codes"] = []
        current.pop("filled_notional")
        current.pop("has_filled_order")
        current.pop("statuses")
    return by_symbol


def classify(
    bt_entries: dict[str, dict[str, Any]],
    live_orders: list[dict[str, Any]],
    live_exit_orders: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict], dict]:
    live_exit_orders = live_exit_orders or {}
    live_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for order in live_orders:
        live_by_symbol.setdefault(order["symbol"], []).append(order)

    diffs: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    def bump(cat: str) -> None:
        counts[cat] = counts.get(cat, 0) + 1

    def live_modes(rows: list[dict[str, Any]]) -> list[str]:
        return list(dict.fromkeys(code for row in rows for code in (row.get("execution_codes") or [])))

    for symbol, bt_row in bt_entries.items():
        orders = live_by_symbol.pop(symbol, [])
        if not orders:
            diffs.append({"symbol": symbol, "direction": "more_in_backtest", "category": "never_submitted",
                          "bt": bt_row})
            bump("never_submitted")
            continue
        filled = [o for o in orders if (o["status"] or "").lower() == "filled"]
        if not filled:
            status = (orders[0]["status"] or "unknown").lower()
            diffs.append({"symbol": symbol, "direction": "more_in_backtest",
                          "category": f"live_order_not_filled:{status}", "bt": bt_row, "live": orders,
                          "live_entry_execution_codes": live_modes(orders),
                          "live_exit_execution_codes": []})
            bump(f"live_order_not_filled:{status}")
            continue
        live_row = filled[0]
        # A real broker-side partial fill compares the live order's OWN
        # submitted qty against its OWN filled qty -- never bt_qty. bt_qty
        # vs live filled_qty is a *sizing* comparison (see below), a
        # structurally different, softer signal: the backtest's own
        # simulated equity compounds forward from --fromdate across the
        # whole warmup window and can drift from the real account's actual
        # equity by trading_date, so qty/notional will legitimately differ
        # even on a perfectly-matched signal/price. Do not conflate the two.
        live_submitted_qty = float(live_row.get("qty") or 0)
        live_filled_qty = float(live_row.get("filled_qty") or 0)
        if live_submitted_qty > 0 and live_filled_qty < live_submitted_qty * 0.995:
            diffs.append({"symbol": symbol, "direction": "more_in_backtest", "category": "partial_fill",
                          "bt": bt_row, "live": live_row,
                          "live_entry_execution_codes": live_row.get("execution_codes") or [],
                          "live_exit_execution_codes": []})
            bump("partial_fill")
            continue
        edge_bps = None
        bt_price = bt_row.get("bt_entry_price")
        live_price = live_row.get("filled_avg_price")
        if bt_price and live_price:
            edge_bps = round((float(live_price) - float(bt_price)) / float(bt_price) * 10000, 2)
        bt_qty = float(bt_row["bt_qty"] or 0)
        sizing_ratio = round(live_filled_qty / bt_qty, 3) if bt_qty else None
        sizing_divergence = sizing_ratio is not None and not (0.85 <= sizing_ratio <= 1.15)

        # Exit leg: the position must be closed on the next session with a
        # filled sell. A matched entry whose exit is missing / unfilled /
        # partial is a HARD divergence (the day is red): the position is
        # still open or was only partly unwound vs. the backtest, which
        # always exits in full. Exit qty is compared to the LIVE entry's
        # own filled qty (not bt_qty -- sizing already covers that).
        exit_row = live_exit_orders.get(symbol)
        exit_status = (exit_row.get("status") or "").lower() if exit_row else None
        exit_filled_qty = float(exit_row.get("filled_qty") or 0) if exit_row else 0.0
        live_exit_price = exit_row.get("filled_avg_price") if exit_row else None
        live_pnl_pct = None
        if live_exit_price and live_price:
            live_pnl_pct = round((float(live_exit_price) - float(live_price)) / float(live_price) * 100, 4)

        exit_issue = None
        if not exit_row:
            exit_issue = "missing"
        elif exit_status != "filled" or exit_filled_qty <= 0:
            exit_issue = f"not_filled:{exit_status or 'unknown'}"
        elif live_filled_qty > 0 and exit_filled_qty < live_filled_qty * 0.995:
            exit_issue = "partial_fill"

        diffs.append({"symbol": symbol, "direction": "matched", "category": "matched",
                      "bt": bt_row, "live": live_row, "entry_edge_bps": edge_bps,
                      "sizing_ratio_live_over_bt": sizing_ratio, "sizing_divergence": sizing_divergence,
                      "bt_exit_price": bt_row.get("bt_exit_price"), "bt_pnl_pct": bt_row.get("bt_pnl_pct"),
                      "live_exit_price": float(live_exit_price) if live_exit_price else None,
                      "live_exit_qty": exit_filled_qty or None, "live_exit_status": exit_status,
                      "live_entry_execution_codes": live_row.get("execution_codes") or [],
                      "live_exit_execution_codes": exit_row.get("execution_codes") if exit_row else [],
                      "live_pnl_pct": live_pnl_pct, "exit_issue": exit_issue})
        bump("matched")
        if sizing_divergence:
            bump("sizing_divergence")
        if exit_issue:
            bump(f"exit_{exit_issue.split(':')[0]}")

    for symbol, orders in live_by_symbol.items():
        filled = [o for o in orders if (o["status"] or "").lower() == "filled"]
        if not filled:
            continue  # a live order that never filled and has no backtest counterpart is not a divergence worth flagging
        # An extra live entry is already a hard divergence.  Still reconcile
        # its exit leg: otherwise an Alpaca position left open is rendered as
        # the generic "ordine extra" and hides the operationally important
        # fact that inventory remains on the account.
        live_row = filled[0]
        live_filled_qty = float(live_row.get("filled_qty") or 0)
        exit_row = live_exit_orders.get(symbol)
        exit_status = (exit_row.get("status") or "").lower() if exit_row else None
        exit_filled_qty = float(exit_row.get("filled_qty") or 0) if exit_row else 0.0
        live_exit_price = exit_row.get("filled_avg_price") if exit_row else None
        exit_issue = None
        if not exit_row:
            exit_issue = "missing"
        elif exit_status != "filled" or exit_filled_qty <= 0:
            exit_issue = f"not_filled:{exit_status or 'unknown'}"
        elif live_filled_qty > 0 and exit_filled_qty < live_filled_qty * 0.995:
            exit_issue = "partial_fill"
        live_entry_price = live_row.get("filled_avg_price")
        live_pnl_pct = None
        if live_entry_price and live_exit_price:
            live_pnl_pct = round(
                (float(live_exit_price) - float(live_entry_price)) / float(live_entry_price) * 100,
                4,
            )
        diffs.append({
            "symbol": symbol, "direction": "more_in_live", "category": "extra_live_order",
            "live": live_row, "live_exit_price": float(live_exit_price) if live_exit_price else None,
            "live_exit_qty": exit_filled_qty or None, "live_exit_status": exit_status,
            "live_entry_execution_codes": live_row.get("execution_codes") or [],
            "live_exit_execution_codes": exit_row.get("execution_codes") if exit_row else [],
            "live_pnl_pct": live_pnl_pct, "exit_issue": exit_issue,
        })
        bump("extra_live_order")
        if exit_issue:
            bump(f"exit_{exit_issue.split(':')[0]}")

    # "clean" = no hard-execution divergence (missing/rejected/pending/partial
    # orders, or unexplained extra live orders). sizing_divergence is a soft,
    # informational flag (known compounding-equity artifact of the replay,
    # see the comment above) and never breaks "clean" on its own.
    hard_categories = {k for k in counts if k not in ("matched", "sizing_divergence")}

    # Per-day return, notional-weighted (decision: pnl / value).
    bt_pnl_sum = sum(float(r["bt_pnl"]) for r in bt_entries.values() if r.get("bt_pnl") is not None)
    bt_val_sum = sum(float(r["bt_value"]) for r in bt_entries.values() if r.get("bt_value"))
    bt_day_return_pct = round(bt_pnl_sum / bt_val_sum * 100, 4) if bt_val_sum else None

    live_pnl_sum = 0.0
    live_cost_sum = 0.0
    for d in diffs:
        if d.get("category") not in {"matched", "extra_live_order"}:
            continue
        entry_px = (d.get("live") or {}).get("filled_avg_price")
        qty = float((d.get("live") or {}).get("filled_qty") or 0)
        exit_px = d.get("live_exit_price")
        if entry_px and exit_px and qty:
            live_pnl_sum += (float(exit_px) - float(entry_px)) * qty
            live_cost_sum += float(entry_px) * qty
    live_day_return_pct = round(live_pnl_sum / live_cost_sum * 100, 4) if live_cost_sum else None

    summary = {
        "counts": counts,
        "clean": not hard_categories,
        "bt_entry_count": len(bt_entries),
        "bt_day_return_pct": bt_day_return_pct,
        "live_day_return_pct": live_day_return_pct,
    }
    return diffs, summary


def fetch_live_entry_orders(repo: "wr.WatchtowerRepository", profile: str, trading_date: date) -> list[dict[str, Any]]:
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, status, qty, filled_qty, filled_avg_price,
                       client_order_id, order_type,
                       LOWER(COALESCE(raw_payload->>'time_in_force', '')) AS time_in_force
                FROM alpaca_order_cache
                WHERE source_account = %s AND window_open = %s AND side = 'buy'
                ORDER BY symbol
                """,
                (profile, trading_date),
            )
            rows = wr._cursor_rows(cur)
    for row in rows:
        tif = str(row.get("time_in_force") or "").lower()
        row["execution_codes"] = ["c" if tif == "cls" else ("o" if tif == "opg" else "m")]
    return rows


# ---------------------------------------------------------------------
# One day, end to end
# ---------------------------------------------------------------------

def reconcile_one_day(repo: "wr.WatchtowerRepository", profile: str, profile_env: dict[str, str],
                       trading_date: date, keep_worktree: bool = False,
                       refreshed_data_keys: set[tuple[str, str, str]] | None = None) -> dict[str, Any]:
    version = repo.resolve_params_as_of(profile, trading_date)  # raises if unknown -- never fall back
    code_root = Path(profile_env["CODE_ROOT"]).resolve()
    bt_core_repo = code_root / "bt-core"
    core_commit, resolution = resolve_core_commit(bt_core_repo, trading_date, version.get("core_commit"))

    run_tag = f"{profile}_{trading_date:%Y%m%d}"
    worktree_bt_core = create_worktree(bt_core_repo, core_commit, run_tag, code_root / "config-common")
    try:
        data_key = replay_data_key(profile_env, version)
        if refreshed_data_keys is None or data_key not in refreshed_data_keys:
            refresh_replay_market_data(worktree_bt_core, profile_env, version)
            if refreshed_data_keys is not None:
                refreshed_data_keys.add(data_key)
        cash = resolve_historical_cash(profile_env, trading_date)
        run_id = f"reconcile_{profile}_{trading_date:%Y%m%d}_{uuid.uuid4().hex[:6]}"
        trades_path, auction_forced = run_replay(worktree_bt_core, profile_env, version, trading_date, run_id, cash)
        bt_entries = load_backtest_entries(trades_path, trading_date)
        live_orders = fetch_live_entry_orders(repo, profile, trading_date)
        live_exit_orders = fetch_live_exit_orders(repo, profile, trading_date)
        diffs, summary = classify(bt_entries, live_orders, live_exit_orders)
        summary["cash_used"] = cash
        summary["core_commit_resolution"] = resolution
        summary["auction_forced_by_harness"] = auction_forced
        summary["market_data_refreshed_for_replay"] = True

        result = repo.upsert_reconciliation_result(
            profile=profile, trading_date=trading_date,
            portfolio_key_id=wr.portfolio_key_id_from_api_key(resolve_account_credentials(profile_env)["key"]),
            strategy=version["strategy"], params_version_id=version["id"], params_hash=version["params_hash"],
            code_commit=version.get("code_commit"), core_commit=core_commit,
            core_commit_resolution=resolution, replay_run_id=run_id,
            replay_outpath=str(outpath_for(worktree_bt_core, version["strategy"], run_id)),
            status="ok", summary=summary, diffs=diffs,
        )
        result["summary"] = summary
        result["diffs_count"] = len(diffs)
        return result
    except Exception as exc:  # noqa: BLE001
        repo.upsert_reconciliation_result(
            profile=profile, trading_date=trading_date, summary={}, diffs=[],
            status="error", error=str(exc),
        )
        raise
    finally:
        if not keep_worktree:
            remove_worktree(bt_core_repo, worktree_bt_core)


def run_catchup(repo: "wr.WatchtowerRepository", profiles: dict[str, dict[str, str]],
                 only_profile: str | None) -> list[dict[str, Any]]:
    results = []
    for profile, profile_env in profiles.items():
        if only_profile and profile != only_profile:
            continue
        _seed_queue(repo, profile)
        refreshed_data_keys: set[tuple[str, str, str]] = set()
        for entry in repo.list_pending_reconciliation(profile):
            trading_date = date.fromisoformat(entry["trading_date"])
            closure = is_day_closed(repo, profile, trading_date)
            if not closure["closed"]:
                results.append({"profile": profile, "trading_date": entry["trading_date"], "action": "left_pending",
                                "reason": closure["reason"]})
                continue
            try:
                outcome = reconcile_one_day(
                    repo, profile, profile_env, trading_date,
                    refreshed_data_keys=refreshed_data_keys,
                )
                repo.mark_reconciliation_status(profile, trading_date, "done", detail={"result_id": outcome["id"]})
                results.append({"profile": profile, "trading_date": entry["trading_date"], "action": "reconciled",
                                "summary": outcome["summary"]})
            except wr.ProfileParamsUnresolvedError as exc:
                repo.mark_reconciliation_status(profile, trading_date, "blocked_missing_params", detail={"error": str(exc)})
                results.append({"profile": profile, "trading_date": entry["trading_date"], "action": "blocked_missing_params"})
            except Exception as exc:  # noqa: BLE001 -- one day's replay failure must never abort the rest of the batch
                # Left as 'pending' (not 'done'/'blocked_missing_params'): this is neither
                # "reconciled" nor "params unknown" -- it's a replay execution failure,
                # worth retrying once the underlying cause (e.g. a strategy/engine bug on
                # that historical commit) is understood. execution_reconciliation_results
                # already has a status='error' row for it (see reconcile_one_day).
                print(f"[{profile}] {trading_date}: replay failed, left pending: {exc}", file=sys.stderr)
                results.append({"profile": profile, "trading_date": entry["trading_date"], "action": "replay_failed",
                                "error": str(exc)})
    return results


def run_refresh_existing(repo: "wr.WatchtowerRepository", profiles: dict[str, dict[str, str]],
                         only_profile: str | None) -> list[dict[str, Any]]:
    """One-off backfill of reconciliation payloads after their schema gains
    a new display fact. Unlike daily catch-up, this deliberately replays
    existing closed rows; it never submits broker orders."""
    results = []
    for profile, profile_env in profiles.items():
        if only_profile and profile != only_profile:
            continue
        existing = repo.list_reconciliation_results(profile, limit=2000)
        refreshed_data_keys: set[tuple[str, str, str]] = set()
        for row in reversed(existing):
            trading_date = date.fromisoformat(row["trading_date"])
            try:
                outcome = reconcile_one_day(
                    repo, profile, profile_env, trading_date,
                    refreshed_data_keys=refreshed_data_keys,
                )
                results.append({"profile": profile, "trading_date": row["trading_date"], "action": "refreshed",
                                "summary": outcome["summary"]})
            except Exception as exc:  # noqa: BLE001
                print(f"[{profile}] {trading_date}: refresh failed: {exc}", file=sys.stderr)
                results.append({"profile": profile, "trading_date": row["trading_date"], "action": "refresh_failed",
                                "error": str(exc)})
    return results


def _seed_queue(repo: "wr.WatchtowerRepository", profile: str) -> None:
    """Enqueue any trading day seen in alpaca_order_cache for `profile`
    that has no reconciliation_queue row and no persisted result yet."""
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT window_open FROM alpaca_order_cache
                WHERE source_account = %s AND side = 'buy' AND window_open IS NOT NULL
                """,
                (profile,),
            )
            candidate_dates = [r["window_open"] for r in wr._cursor_rows(cur)]
    for trading_date in candidate_dates:
        existing = repo.get_reconciliation_result(profile, trading_date)
        if existing:
            continue
        repo.enqueue_reconciliation(profile, trading_date, reason="new")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--trading-date", default=None, help="Reconcile exactly this day, bypassing the queue")
    parser.add_argument("--catch-up", action="store_true", help="Process the reconciliation_queue (daily job mode)")
    parser.add_argument("--refresh-existing", action="store_true",
                        help="One-off: replay existing reconciliation rows to backfill a new payload field")
    parser.add_argument("--keep-worktree", action="store_true", help="Debug: don't remove the scratch worktree")
    parser.add_argument("--db-dsn", default=None)
    args = parser.parse_args(argv)

    profiles = discover_profiles()
    if args.profile and args.profile not in profiles:
        print(f"unknown profile {args.profile!r}, discovered: {sorted(profiles)}", file=sys.stderr)
        return 1

    repo = wr.WatchtowerRepository(dsn=args.db_dsn)

    if args.catch_up:
        out = run_catchup(repo, profiles, args.profile)
        print(json.dumps(out, indent=2, default=str))
        return 0

    if args.refresh_existing:
        out = run_refresh_existing(repo, profiles, args.profile)
        print(json.dumps(out, indent=2, default=str))
        return 0

    if not args.profile or not args.trading_date:
        parser.error("--trading-date requires --profile (or use --catch-up to process the queue)")

    trading_date = date.fromisoformat(args.trading_date)
    result = reconcile_one_day(repo, args.profile, profiles[args.profile], trading_date, args.keep_worktree)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# ---------------------------------------------------------------------
# Daily schedule (Europe/Rome; install in the user's crontab):
#
#   # Run after the 15:52 Europe/Rome MOO fallback, otherwise the cache only
#   # contains the expired OPG leg and the reconciliation permanently misses
#   # the later market exit.
#   5 16 * * 1-6  /home/htpc/backtrader/bt-core/.venv/bin/python \
#       /home/htpc/backtrader/bin/watchtower_poll_alpaca_orders.py --days 10
#   20 16 * * 1-6 /home/htpc/backtrader/bt-core/.venv/bin/python \
#       /home/htpc/backtrader/bin/watchtower_replay_reconcile.py --catch-up
#
# Runs once a day, well after the US market has opened and the previous
# day's exit leg (moo-exit / moo-exit-fallback) has settled, so is_day_closed()
# has real exit evidence to look at instead of leaving everything pending.
# ---------------------------------------------------------------------
