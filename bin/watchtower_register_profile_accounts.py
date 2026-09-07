#!/usr/bin/env python3
"""Requirement C (account <-> strategy registry): register each real
scheduled profile's Alpaca account into `alpaca_portfolios`
(portfolio_key_id, credentials, paper flag) and explicitly set
`assigned_profile`/`assigned_strategy` on that row.

Profiles are discovered dynamically from
~/.config/backtrader/scheduled/*.env -- nothing here is a hardcoded list of
today's four profiles (live/mirror/challenger/development); a new profile
scheduled tomorrow is picked up unmodified.

Never touches the pre-existing legacy rows in alpaca_portfolios
(`HMA-plain`, `OvernightAH`) -- those were registered by a different piece
of work and are left exactly as they are; this script only
INSERT/UPDATEs rows keyed by the *real* portfolio_key_id computed from each
profile's own ACCOUNT_ENV credentials, which are guaranteed to differ from
those legacy rows' accounts.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

BT_CORE = Path(__file__).resolve().parent.parent / "bt-core"
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import watchtower_runtime as wr  # noqa: E402

SCHEDULED_PROFILES_DIR = Path(
    __import__("os").environ.get(
        "BT_SCHEDULED_PROFILES_DIR", str(Path.home() / ".config" / "backtrader" / "scheduled")
    )
)
ACCOUNTS_DIR = Path(
    __import__("os").environ.get(
        "BT_SCHEDULED_ACCOUNTS_DIR", str(Path.home() / ".config" / "backtrader" / "accounts")
    )
)


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
        resolved = _read_env_file(env_file)
        profiles[resolved.get("ROLE") or env_file.stem] = resolved
    return profiles


def resolve_strategy_config(profile_env: dict[str, str]) -> dict[str, str]:
    strategy_config = profile_env.get("STRATEGY_CONFIG")
    if not strategy_config:
        return {}
    path = Path(strategy_config)
    if not path.is_absolute():
        code_root = Path(profile_env.get("CODE_ROOT") or ".").resolve()
        path = code_root / strategy_config
    return _read_env_file(path)


def resolve_strategy(repo: "wr.WatchtowerRepository", profile: str, profile_env: dict[str, str]) -> str | None:
    """Prefer the Phase-1 parameter timeline (authoritative, machine-recorded
    'what actually ran') over re-parsing the raw config file, falling back
    to the raw STRATEGY_CONFIG only if the timeline has no entry yet."""
    try:
        versions = repo.list_profile_param_versions(profile)
        if versions:
            return versions[-1]["strategy"]
    except Exception:
        pass
    return resolve_strategy_config(profile_env).get("STRAT")


def register_profile(repo: "wr.WatchtowerRepository", profile: str, profile_env: dict[str, str]) -> dict[str, Any]:
    account_env_path = profile_env.get("ACCOUNT_ENV")
    if not account_env_path:
        return {"profile": profile, "skipped": "no ACCOUNT_ENV in profile config"}
    path = Path(account_env_path)
    if not path.is_absolute():
        path = ACCOUNTS_DIR / path
    creds = _read_env_file(path)
    api_key = creds.get("ALPACA_API_KEY") or creds.get("BROKER_API_KEY")
    secret_key = creds.get("ALPACA_SECRET_KEY") or creds.get("BROKER_SECRET_KEY")
    if not api_key or not secret_key:
        return {"profile": profile, "skipped": f"no Alpaca credentials in {path}"}

    paper = (profile_env.get("TRADING_MODE") or "paper").strip().lower() != "live"
    strategy = resolve_strategy(repo, profile, profile_env)
    run_id = profile_env.get("RUN_ID") or profile

    # expected_client_order_run_tag: same short-hash algorithm as
    # MultiTickerStrategy._cid_short_hash / submit_moo.py's _short_hash, so
    # the guardrail can recognise which run_tag *should* appear in this
    # account's client_order_ids once the entry/exit tagging fix (Phase 3)
    # is promoted -- computed here, not imported from either script, to
    # keep this registration tool independent of the strategy code path.
    import hashlib
    expected_tag = hashlib.sha1(str(run_id).lower().encode("utf-8")).hexdigest()[:6]

    portfolio = repo.upsert_alpaca_portfolio(
        api_key=api_key,
        secret_key=secret_key,
        paper=paper,
        display_name=profile,
        metadata={
            "source": "scheduled_profile_registry",
            "run_id": run_id,
            "code_root": profile_env.get("CODE_ROOT"),
            "expected_client_order_run_tag": expected_tag,
        },
    )
    assigned = repo.assign_profile_to_portfolio(portfolio["portfolio_key_id"], profile, strategy)
    return {
        "profile": profile,
        "portfolio_key_id": portfolio["portfolio_key_id"],
        "assigned_strategy": strategy,
        "expected_client_order_run_tag": expected_tag,
        "paper": paper,
        "result": assigned,
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default=None, help="Restrict to one profile (default: all discovered)")
    parser.add_argument("--db-dsn", default=None)
    args = parser.parse_args(argv)

    profiles = discover_profiles()
    if args.profile and args.profile not in profiles:
        print(f"unknown profile {args.profile!r}, discovered: {sorted(profiles)}", file=sys.stderr)
        return 1

    repo = wr.WatchtowerRepository(dsn=args.db_dsn)
    results = []
    for profile, profile_env in profiles.items():
        if args.profile and profile != args.profile:
            continue
        results.append(register_profile(repo, profile, profile_env))

    print(json.dumps(results, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
