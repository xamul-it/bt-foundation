#!/usr/bin/env python3
"""Requirement B (strategy footprint drift): for a scheduled profile,
compute a statistical footprint of the strategy AS IT CURRENTLY RUNS
(latest params resolved from the Phase-1 timeline) over two periods --
before and after the profile's activation date -- and persist a drift
verdict comparing them.

Methodology (documented explicitly, not left implicit):
- Activation date = the earliest `effective_from_date` across the
  profile's profile_param_versions rows (Phase 1) -- the first day this
  profile is known to have run at all.
- Both periods use the SAME parameter set: the profile's CURRENT (latest,
  still-active) resolved STRATARGS. This is a deliberate choice: using
  each historical sub-period's own historicized params (as requirement A
  does for execution reconciliation) would confound "did the edge change"
  with "did we also retune the knobs" -- the question this requirement
  asks is "is the CURRENTLY deployed configuration's edge holding up
  in its live window vs. its pre-live history", which requires one fixed
  configuration compared across periods.
- ONE backtest is run from --history-start to today (not two separate
  runs): cheaper, and guarantees the two periods share identical data
  handling/warmup. Trades are then partitioned by entry date relative to
  the activation date -- entries before it are "pre_activation", on/after
  it are "post_activation". This is backtest-only: no Alpaca order data
  is touched (that is requirement A/C's job, not this one).
- No historical commit checkout (no git worktree): unlike requirement A,
  this is not testing execution fidelity against a specific day's code,
  it is asking a question about the strategy's own statistical behavior
  under its current configuration -- runs directly against the profile's
  CODE_ROOT/bt-core checkout as it stands today.

`--profile` is free text, profiles discovered dynamically from
~/.config/backtrader/scheduled/*.env -- no hardcoded profile/strategy
names; a new profile scheduled tomorrow works unmodified.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import watchtower_runtime as wr  # noqa: E402

DEFAULT_HISTORY_START = date(2015, 1, 1)
RECENT_TRADING_DAY_OPTIONS = (3, 5, 10, 15, 20, 25)
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


SCHEDULED_PROFILES_DIR = Path(
    os.environ.get("BT_SCHEDULED_PROFILES_DIR", str(Path.home() / ".config" / "backtrader" / "scheduled"))
)


def discover_profiles() -> dict[str, dict[str, str]]:
    profiles: dict[str, dict[str, str]] = {}
    if not SCHEDULED_PROFILES_DIR.is_dir():
        return profiles
    for env_file in sorted(SCHEDULED_PROFILES_DIR.glob("*.env")):
        profiles[_read_env_file(env_file).get("ROLE") or env_file.stem] = _read_env_file(env_file)
    return profiles


def build_stratargs_string(stratargs: dict[str, Any]) -> str:
    return " ".join(f"{key}={value!r}" for key, value in stratargs.items())


def outpath_for(bt_core: Path, strat: str, run_id: str) -> Path:
    parts = strat.split(".")
    base = bt_core / "out" / parts[0].lower() / parts[1] if len(parts) == 2 else bt_core / "out" / strat
    return base / run_id


def run_history_backtest(profile: str, profile_env: dict[str, str], version: dict[str, Any],
                          history_start: date, run_id: str) -> Path:
    extra = version.get("metadata") or {}
    ticker = extra.get("TICKER") or profile_env.get("TICKER")
    provider = extra.get("DATA_PROVIDER") or profile_env.get("DATA_PROVIDER") or "yahoo"
    alpaca_feed = extra.get("ALPACA_FEED") or DEFAULT_ALPACA_FEED
    margin_leverage = extra.get("MARGIN_LEVERAGE") or DEFAULT_MARGIN_LEVERAGE

    code_root = Path(profile_env["CODE_ROOT"]).resolve()
    bt_core_repo = code_root / "bt-core"
    shared_config = code_root / "config-common"

    # auction=False is a broker-execution flag, meaningless (degenerate
    # same-price entry/exit) in a pure backtest -- same translation applied
    # in watchtower_replay_reconcile.py (requirement A), same reasoning.
    stratargs = dict(version["stratargs"])
    if not stratargs.get("auction", True):
        stratargs["auction"] = True

    cmd = [
        str(bt_core_repo / ".venv" / "bin" / "python"), "btmain.py",
        "--strat", version["strategy"],
        "--ticker", ticker,
        "--fromdate", history_start.isoformat(),
        "--todate", date.today().isoformat(),
        "--timeframe", "daily",
        "--provider", provider,
        "--alpaca-feed", alpaca_feed,
        "--commission", "none",
        "--margin-leverage", str(margin_leverage),
        "--mode", "backtest",
        "--id", run_id,
        "--stratargs", build_stratargs_string(stratargs),
    ]
    env = os.environ.copy()
    env["BT_SHARED_CONFIG"] = str(shared_config)
    print(f"[{profile}] Running footprint backtest:", " ".join(cmd), file=sys.stderr)
    subprocess.run(cmd, cwd=str(bt_core_repo), env=env, check=True)

    run_outpath = outpath_for(bt_core_repo, version["strategy"], run_id)
    trades_path = run_outpath / "trades.json"
    if not trades_path.exists():
        existing = ", ".join(sorted(p.name for p in run_outpath.glob("*"))) if run_outpath.exists() else "outpath missing"
        raise FileNotFoundError(f"backtest produced no trades.json ({run_outpath}); found: {existing}")
    return trades_path


def _entry_date(trade: dict[str, Any]) -> date | None:
    raw = trade.get("entry_signal_dt") or trade.get("open_datetime") or trade.get("entry_datetime")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw)[:19].replace(" ", "T")).date()
    except ValueError:
        return None


def split_trades_by_activation(trades: list[dict[str, Any]], activation_date: date) -> tuple[list[dict], list[dict]]:
    pre, post = [], []
    for trade in trades:
        entry_date = _entry_date(trade)
        if entry_date is None:
            continue
        (pre if entry_date < activation_date else post).append(trade)
    return pre, post


def _is_closed_trade(trade: dict[str, Any]) -> bool:
    return bool(trade.get("close_datetime") or trade.get("exit_time"))


def build_window_comparisons(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Compare completed entry windows only.

    The most recent market session is the active entry window, even when the
    snapshot happens over a weekend and even when it produced no entry.  It
    becomes eligible only after the next market session starts.  A previous
    session with a Backtrader position still open holds the frontier back as
    well.  This makes a Sunday run end on Thursday, not on the preceding
    Friday's still-active overnight window.
    """
    sessions = sorted(date.fromisoformat(row["day"]) for row in wr._load_cached_market_windows())
    active_session = max((day for day in sessions if day <= date.today()), default=None)
    by_entry: dict[date, list[dict[str, Any]]] = {}
    for trade in trades:
        entry = _entry_date(trade)
        if entry:
            by_entry.setdefault(entry, []).append(trade)
    open_days = {day for day, rows in by_entry.items() if any(not _is_closed_trade(row) for row in rows)}
    # A new session is considered settled only once the following session has
    # begun.  This avoids treating Friday as closed in a weekend snapshot.
    last_closed = max((day for day in sessions if active_session and day < active_session), default=None)
    if open_days:
        first_open = min(open_days)
        last_closed = max((day for day in sessions if day < first_open), default=None)
    closed_sessions = [day for day in sessions if last_closed and day <= last_closed]
    closed_trades = [t for t in trades if _is_closed_trade(t)]
    comparisons: dict[str, dict[str, Any]] = {}
    for count in RECENT_TRADING_DAY_OPTIONS:
        if len(closed_sessions) < count:
            continue
        recent_start, recent_end = closed_sessions[-count], closed_sessions[-1]
        historical_end = closed_sessions[-count - 1] if len(closed_sessions) > count else None
        recent = [t for t in closed_trades if (d := _entry_date(t)) and recent_start <= d <= recent_end]
        historical = [t for t in closed_trades if (d := _entry_date(t)) and d < recent_start]
        def compact(rows, start, end):
            metrics = wr.baseline_metrics_from_trades(rows)
            return {"window_start": start.isoformat() if start else None, "window_end": end.isoformat() if end else None,
                    "sample_size": metrics.get("sample_size", 0), "metrics": {k: metrics.get(k) for k in ("mean", "win_rate", "average_daily_return_pct", "daily_sample_size")}}
        comparisons[str(count)] = {"historical": compact(historical, closed_sessions[0], historical_end),
                                   "recent": compact(recent, recent_start, recent_end)}
    return comparisons


def compute_drift_verdict(pre_pnl: list[float], post_pnl: list[float], seed: str) -> dict[str, Any]:
    outcome = wr.evaluate_outcomes(post_pnl, {**wr.summarize_distribution(pre_pnl), **wr.distribution_moments(pre_pnl)})
    wdist = wr.wasserstein_distance(pre_pnl, post_pnl)
    mc = wr.monte_carlo_subset_test(pre_pnl, post_pnl, seed=seed)
    return {
        "status": outcome["status"],
        "confidence": outcome["confidence"],
        "score": outcome["score"],
        "z_mean": outcome["metrics"]["z_mean"],
        "z_median": outcome["metrics"]["z_median"],
        "ks_distance": outcome["metrics"]["ks_distance"],
        "wasserstein_distance": wdist,
        "monte_carlo": mc,
        "pre_sample_size": len(pre_pnl),
        "post_sample_size": len(post_pnl),
    }


def compute_for_profile(repo: "wr.WatchtowerRepository", profile: str, profile_env: dict[str, str],
                         history_start: date) -> dict[str, Any]:
    versions = repo.list_profile_param_versions(profile)
    if not versions:
        raise wr.ProfileParamsUnresolvedError(f"no profile_param_versions rows for profile={profile!r} (run Phase 1 first)")
    current_version = repo.resolve_params_as_of(profile, date.today())

    run_id = f"footprint_{profile}_{uuid.uuid4().hex[:8]}"
    trades_path = run_history_backtest(profile, profile_env, current_version, history_start, run_id)
    trades = json.loads(trades_path.read_text(encoding="utf-8"))
    comparisons = build_window_comparisons(trades)
    selected = comparisons.get("5") or next(iter(comparisons.values()), None)
    if not selected:
        raise ValueError("no_closed_trading_windows")
    # Persist the default 5-session comparison for drift compatibility; the
    # full compact map lets the dashboard switch windows without rerunning BT.
    cutoff = date.fromisoformat(selected["recent"]["window_start"])
    pre_trades, post_trades = split_trades_by_activation(trades, cutoff)
    pre_trades = [t for t in pre_trades if _is_closed_trade(t)]
    post_trades = [t for t in post_trades if _is_closed_trade(t)]

    def pnl_of(trade_list: list[dict[str, Any]]) -> list[float]:
        return [float(t["pnl_pct"]) for t in trade_list if t.get("pnl_pct") is not None]

    pre_pnl, post_pnl = pnl_of(pre_trades), pnl_of(post_trades)

    pre_fp = repo.upsert_strategy_footprint(
        profile=profile, strategy=current_version["strategy"], params_hash=current_version["params_hash"],
        params=current_version["stratargs"], period="pre_activation", activation_date=cutoff,
        pnl_values=pre_pnl, trades=pre_trades, window_start=history_start, window_end=cutoff - timedelta(days=1),
        source_meta={"builder": "watchtower_compute_footprint_drift", "run_id": run_id, "window_comparisons": comparisons},
    )
    post_fp = repo.upsert_strategy_footprint(
        profile=profile, strategy=current_version["strategy"], params_hash=current_version["params_hash"],
        params=current_version["stratargs"], period="post_activation", activation_date=cutoff,
        pnl_values=post_pnl, trades=post_trades, window_start=cutoff, window_end=date.today(),
        source_meta={"builder": "watchtower_compute_footprint_drift", "run_id": run_id, "window_comparisons": comparisons},
    )

    verdict = compute_drift_verdict(pre_pnl, post_pnl, seed=profile)
    drift_row = repo.record_footprint_drift_check(
        profile=profile, strategy=current_version["strategy"],
        pre_footprint_id=pre_fp["id"], post_footprint_id=post_fp["id"],
        status=verdict["status"], score=verdict["score"], verdict=verdict,
    )

    return {
        "profile": profile, "strategy": current_version["strategy"], "recent_window_start": cutoff.isoformat(),
        "pre_footprint": {"id": pre_fp["id"], "sample_size": pre_fp["sample_size"]},
        "post_footprint": {"id": post_fp["id"], "sample_size": post_fp["sample_size"]},
        "drift": {"id": drift_row["id"], "status": drift_row["status"], "score": drift_row["score"]},
        "verdict": verdict,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default=None, help="Restrict to one profile (default: all discovered)")
    parser.add_argument("--history-start", default=DEFAULT_HISTORY_START.isoformat(),
                         help="Start of the long research backtest (default 2015-01-01)")
    parser.add_argument("--db-dsn", default=None)
    args = parser.parse_args(argv)

    profiles = discover_profiles()
    if args.profile and args.profile not in profiles:
        print(f"unknown profile {args.profile!r}, discovered: {sorted(profiles)}", file=sys.stderr)
        return 1

    repo = wr.WatchtowerRepository(dsn=args.db_dsn)
    history_start = date.fromisoformat(args.history_start)
    results = []
    for profile, profile_env in profiles.items():
        if args.profile and profile != args.profile:
            continue
        try:
            results.append(compute_for_profile(repo, profile, profile_env, history_start))
        except Exception as exc:  # noqa: BLE001 -- one profile's failure must not stop the others
            print(f"[{profile}] footprint computation failed: {exc}", file=sys.stderr)
            results.append({"profile": profile, "error": str(exc)})

    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
