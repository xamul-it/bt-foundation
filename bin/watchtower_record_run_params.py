#!/usr/bin/env python3
"""Durable parameter observations from the exact scheduled command environment.

The local outbox is written before touching Postgres. Pending observations are
drained in date order for each profile; a failed write stops the drain so a
later version cannot overtake an older one. --recover retries the outbox only.
"""
import argparse
import fcntl
import json
import os
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'bt-core'))
from btmain import parse_strategy_args
from run_provenance import git_provenance
import watchtower_runtime as wr


def atomic_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    with temporary.open('w') as handle:
        json.dump(payload, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def observation(env, profile, trading_date):
    params = parse_strategy_args(env['STRATARGS'])
    capital = params.pop('scheduled_entry_equity', None)
    params.pop('decision_date_override', None)
    root = Path(env['CODE_ROOT'])
    code = git_provenance(root, ignore_submodules=True)
    core = git_provenance(root / 'bt-core')
    metadata = {key: env[key] for key in ('TICKER', 'DATA_PROVIDER', 'ALPACA_FEED',
                'MARGIN_LEVERAGE', 'COMMISSION', 'FROM_DAYS') if key in env}
    metadata.update(code_dirty=code['dirty'], core_dirty=core['dirty'])
    return dict(profile=profile, strategy=env['STRAT'], trading_date=str(trading_date),
                stratargs=params, code_commit=code['commit'], core_commit=core['commit'],
                extra=metadata, sizing_capital=capital, run_id=env.get('RUN_ID'),
                captured_at=datetime.now(timezone.utc).isoformat(),
                phase='entry_command_prepared')


def drain(repo, directory):
    for path in sorted(directory.glob('*.json')):
        marker = path.with_suffix('.saved')
        if marker.exists():
            continue
        payload = json.loads(path.read_text())
        result = repo.record_profile_param_observation(
            profile=payload['profile'], strategy=payload['strategy'],
            trading_date=date.fromisoformat(payload['trading_date']),
            stratargs=payload['stratargs'], code_commit=payload['code_commit'],
            core_commit=payload['core_commit'], extra=payload['extra'], source='observed_run')
        atomic_json(marker, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--trading-date', type=date.fromisoformat, default=date.today())
    parser.add_argument('--outbox', type=Path, required=True)
    parser.add_argument('--recover', action='store_true')
    args = parser.parse_args()
    args.outbox.mkdir(parents=True, exist_ok=True)
    with (args.outbox / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not args.recover:
            payload = observation(os.environ, args.profile, args.trading_date)
            path = args.outbox / f'{args.trading_date}-{datetime.now(timezone.utc):%H%M%S%f}-{uuid.uuid4().hex[:8]}.json'
            atomic_json(path, payload)
            print(f'PARAMS_RECEIPT_SAVED path={path}', flush=True)
        try:
            drain(wr.WatchtowerRepository(), args.outbox)
        except Exception as exc:
            print(f'PARAMS_HISTORY_PENDING profile={args.profile}: {exc}', file=sys.stderr)
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
