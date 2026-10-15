#!/usr/bin/env python3
"""Backfill dashboard replay windows using current paper-profile parameters."""
import argparse
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

BT_CORE = Path(__file__).resolve().parents[1] / 'bt-core'
if str(BT_CORE) not in sys.path:
    sys.path.insert(0, str(BT_CORE))

import watchtower_replay_reconcile as replay
from btmain import parse_strategy_args
from profile_baseline import load_profile_env


def backtest_payload(previous, entries, day):
    """Replace only Backtest evidence; retain all existing Alpaca evidence."""
    for entry in entries.values():
        if entry.get('bt_entry_date') != str(day):
            raise ValueError('Replay entry date differs from reconciliation date')
    diffs = []
    seen = set()
    for old in (previous or {}).get('diffs') or []:
        row = dict(old)
        symbol = row.get('symbol')
        seen.add(symbol)
        row['bt'] = entries.get(symbol)
        for key in list(row):
            if key.startswith('bt_'):
                row.pop(key)
        if symbol in entries:
            row.update({k: v for k, v in entries[symbol].items() if k.startswith('bt_')})
        diffs.append(row)
    for symbol, entry in entries.items():
        if symbol not in seen:
            diffs.append(dict(symbol=symbol, bt=entry, category='backtest_only'))
    summary = dict((previous or {}).get('summary') or {})
    value = sum(float(e.get('bt_value') or 0) for e in entries.values())
    pnl = sum(float(e.get('bt_pnl') or 0) for e in entries.values())
    summary.update(bt_entry_count=len(entries), bt_day_return_pct=round(pnl/value*100, 4) if value else None,
                   bt_no_trade=not bool(entries), backfill_scope='backtest_only')
    return diffs, summary


def decision_reasons(log_path, day, entries):
    if entries:
        return []
    # Require both decision date and signal_date: the other cooldown log
    # uses a signal date and must never be mistaken for an entry date.
    pattern = re.compile(r'ENTRY_COOLDOWN date=' + re.escape(str(day)) + r' signal_date=([^ ]+)')
    if log_path.is_file():
        for line in log_path.read_text().splitlines():
            if pattern.search(line):
                return [dict(code='cooldown', label='Cooldown strategia', detail=line.strip())]
    return [dict(code='no_entries_in_regenerated_backtest',
                 label='Nessun ingresso nella simulazione rigenerata',
                 detail='Nessun trade con questa data effettiva di ingresso')]


def persist_day(repo, profile, day, entries, metadata):
    """Replace all simulated Backtest records for a day in one transaction."""
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM execution_reconciliation_results WHERE profile=%s AND trading_date=%s FOR UPDATE', (profile, day))
            previous = replay.wr._cursor_row(cur)
            diffs, summary = backtest_payload(previous, entries, day)
            summary.update(metadata['summary'])
            reasons = metadata.get('reasons') or []
            orders = [dict(symbol=symbol, side='buy', qty=e['bt_qty'], status='filled',
                           filled_qty=e['bt_qty'], filled_avg_price=e['bt_entry_price'],
                           reference_price=e['bt_entry_price'], feed_date=e['bt_entry_date'])
                      for symbol,e in entries.items()]
            verification = replay.wr.backtest_replay_verification(orders, entries, reasons)
            verification.update(run_id=metadata['run_id'], core_commit=metadata['core'],
                                baseline_kind=metadata.get('baseline_kind', 'current_params_counterfactual'),
                                calculation_job_id=metadata.get('job_id'),
                                baseline_id=metadata.get('baseline_id'))
            verification['backtest_history_rebuilt'] = True
            payload = dict(schema_version=2,date=str(day),signal_date=None,
                           entry=dict(outcome='orders' if orders else 'no_orders',orders=orders,
                                      reasons=reasons,sizing_capital=summary.get('cash_used')),
                           backtest_history_rebuilt=True,run_id=metadata['run_id'],
                           calculation_job_id=metadata.get('job_id'),
                           baseline_id=metadata.get('baseline_id'))
            artifact = Path(metadata['output'])/'backfill-decision.json'
            artifact.write_text(json.dumps(payload,indent=2))
            cur.execute("""INSERT INTO scheduled_entry_decisions
                (profile,trading_date,source,run_status,outcome,signal_date,reasons,orders,payload,artifact_path,error)
                VALUES (%s,%s,'backtest','completed',%s,NULL,%s::jsonb,%s::jsonb,%s::jsonb,%s,NULL)
                ON CONFLICT(profile,trading_date,source) DO UPDATE SET run_status='completed',
                outcome=EXCLUDED.outcome,signal_date=NULL,reasons=EXCLUDED.reasons,orders=EXCLUDED.orders,
                payload=EXCLUDED.payload,artifact_path=EXCLUDED.artifact_path,error=NULL,updated_at=NOW()""",
                (profile,day,payload['entry']['outcome'],replay.wr.stable_json(reasons),
                 replay.wr.stable_json(orders),replay.wr.stable_json(payload),str(artifact)))
            cur.execute("DELETE FROM scheduled_entry_settlements WHERE profile=%s AND trading_date=%s AND source='backtest'",(profile,day))
            recovered = []
            for symbol,trade in entries.items():
                if not trade.get('bt_exit_date') or trade.get('bt_exit_price') is None:
                    raise ValueError('Incomplete trade cannot replace the simulated Backtest')
                exit_price = float(trade['bt_exit_price'])
                entry_price = float(trade['bt_entry_price'])
                qty = float(trade['bt_qty'])
                detail = dict(origin='backtest_history_rebuilt',entry_execution='CLS',
                              entry_price_source='regenerated_backtest_execution',
                              exit_price_source='backtest_replay', exit_run_id=metadata['run_id'],
                              exit_core_commit=metadata['core'], exit_date=trade['bt_exit_date'],
                              calculation_job_id=metadata.get('job_id'),
                              baseline_id=metadata.get('baseline_id'))
                cur.execute("""INSERT INTO scheduled_entry_settlements
                    (profile,trading_date,source,symbol,side,status,entry_date,exit_date,qty,
                     entry_price,exit_price,pnl,pnl_pct,detail)
                    VALUES(%s,%s,'backtest',%s,'buy','closed',%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""",
                    (profile,day,symbol,day,trade['bt_exit_date'],qty,entry_price,exit_price,(exit_price-entry_price)*qty,
                     (exit_price/entry_price-1)*100 if entry_price else None,
                     replay.wr.stable_json(detail)))
                recovered.append(symbol)
            summary['bt_settlement_pending'] = False
            summary['backtest_history_rebuilt'] = True
            summary['bt_no_trade_reasons'] = reasons
            cur.execute("""INSERT INTO scheduled_backtest_replay_verifications
                (profile,trading_date,status,error,evidence) VALUES (%s,%s,'completed',NULL,%s::jsonb)
                ON CONFLICT(profile,trading_date) DO UPDATE SET status='completed',error=NULL,
                evidence=EXCLUDED.evidence,updated_at=NOW()""",(profile,day,replay.wr.stable_json(verification)))
            cur.execute("""INSERT INTO execution_reconciliation_results
                (profile,trading_date,strategy,params_hash,code_commit,core_commit,core_commit_resolution,
                 replay_run_id,replay_outpath,status,error,summary,diffs)
                VALUES (%s,%s,%s,%s,%s,%s,'current_params_counterfactual',%s,%s,'ok',NULL,%s::jsonb,%s::jsonb)
                ON CONFLICT(profile,trading_date) DO UPDATE SET strategy=EXCLUDED.strategy,
                params_version_id=NULL,params_hash=EXCLUDED.params_hash,code_commit=EXCLUDED.code_commit,
                core_commit=EXCLUDED.core_commit,core_commit_resolution=EXCLUDED.core_commit_resolution,
                replay_run_id=EXCLUDED.replay_run_id,replay_outpath=EXCLUDED.replay_outpath,
                status='ok',error=NULL,summary=EXCLUDED.summary,diffs=EXCLUDED.diffs,updated_at=NOW()""",
                (profile,day,metadata['strategy'],metadata['hash'],metadata['code'],metadata['core'],
                 metadata['run_id'],metadata['output'],replay.wr.stable_json(summary),replay.wr.stable_json(diffs)))
        conn.commit()
    return recovered


def backup_rebuild_scope(repo, profile, days, archive):
    """Write the complete affected Backtest scope before the first mutation."""
    archive.mkdir(parents=True, exist_ok=True)
    specifications = (
        ('scheduled_entry_decisions', " AND source='backtest'"),
        ('scheduled_entry_settlements', " AND source='backtest'"),
        ('scheduled_backtest_replay_verifications', ''),
        ('execution_reconciliation_results', ''),
    )
    with repo.connect() as conn:
        with conn.cursor() as cur:
            for table, source_filter in specifications:
                cur.execute(
                    f'SELECT * FROM {table} WHERE profile=%s AND trading_date=ANY(%s){source_filter}',
                    (profile, list(days)),
                )
                rows = replay.wr._cursor_rows(cur)
                (archive / f'{table}-before.json').write_text(
                    json.dumps(rows, default=str, indent=2), encoding='utf-8',
                )


def run_rebuild_from_snapshot(repo, snapshot, job_id, progress_callback=None):
    """Rebuild only Backtest evidence using the immutable baseline snapshot."""
    if snapshot.get('kind') != 'backtest_rebuild':
        raise ValueError('invalid_backtest_rebuild_snapshot')
    profile = str(snapshot['profile'])
    days = [date.fromisoformat(value) for value in snapshot.get('dates') or []]
    if len(days) != 25:
        raise ValueError('backtest_rebuild_requires_25_frozen_dates')
    config = dict(snapshot.get('configuration') or {})
    required = ('strategy', 'stratargs', 'ticker', 'provider', 'alpaca_feed',
                'margin_leverage', 'commission')
    missing = [key for key in required if config.get(key) in (None, '', {})]
    if missing:
        raise ValueError('snapshot_configuration_incomplete:' + ','.join(missing))

    root = Path(snapshot['code_root']).resolve()
    bt_core = root / 'bt-core'
    profiles = replay.discover_profiles()
    profile_env = dict(profiles.get(profile) or {})
    # Execution knobs always come from the selected baseline. The live env is
    # used only by resolve_historical_cash for the account/equity lookup.
    if not profile_env:
        raise ValueError(f'profile_not_found:{profile}')
    equity = snapshot.get('historical_equity') or {}
    if equity.get('account_env'):
        profile_env['ACCOUNT_ENV'] = equity['account_env']
    if equity.get('trading_mode'):
        profile_env['TRADING_MODE'] = equity['trading_mode']
    scratch_root = Path(os.environ.get(
        'BT_WATCHTOWER_SCRATCH', str(Path(__file__).resolve().parent / 'watchtower_scratch'),
    ))
    archive = scratch_root / f'calculation_{job_id}'
    archive.mkdir(parents=True, exist_ok=True)
    (archive / 'snapshot.json').write_text(json.dumps(snapshot, indent=2), encoding='utf-8')
    backup_rebuild_scope(repo, profile, days, archive)

    outcomes = []
    for index, day in enumerate(days, start=1):
        try:
            cash = replay.resolve_historical_cash(profile_env, day)
            if cash is None or cash <= 0:
                raise ValueError('Historical equity unavailable')
            run_id = f'baseline_{snapshot["baseline_id"]}_{profile}_{day:%Y%m%d}_{uuid.uuid4().hex[:6]}'
            output = replay.outpath_for(bt_core, config['strategy'], run_id)
            fromdate = day - timedelta(days=int(config.get('from_days') or 420))
            todate = min(day + timedelta(days=10), date.today())
            cmd = [
                str(bt_core / '.venv' / 'bin' / 'python'), 'btmain.py',
                '--strat', config['strategy'], '--ticker', config['ticker'],
                '--stratargs', replay.build_stratargs_string(dict(config['stratargs'])),
                '--provider', config['provider'], '--alpaca-feed', config['alpaca_feed'],
                '--timeframe', config.get('timeframe') or 'daily', '--mode', 'backtest',
                '--fromdate', str(fromdate), '--todate', str(todate), '--cash', str(cash),
                '--margin-leverage', str(config['margin_leverage']),
                '--commission', str(config['commission']), '--id', run_id,
            ]
            env = os.environ.copy()
            env['BT_SHARED_CONFIG'] = str(root / 'config-common')
            day_log = archive / f'{day}.log'
            with day_log.open('w', encoding='utf-8') as log:
                subprocess.run(
                    cmd, cwd=bt_core, env=env, stdout=log, stderr=subprocess.STDOUT,
                    check=True, timeout=int(os.environ.get('BT_REBUILD_DAY_TIMEOUT_SECONDS', '900')),
                )
            trades_path = output / 'trades.json'
            entries = replay.load_backtest_entries(trades_path, day) if trades_path.is_file() else {}
            summary = {
                'baseline_kind': 'selected_profile_baseline', 'baseline_id': snapshot['baseline_id'],
                'calculation_job_id': job_id, 'cash_used': cash,
                'historical_equity_source': 'alpaca_portfolio_history',
                'market_data_source': 'current_shared_dataset',
                'current_dataset_manifest': snapshot.get('current_dataset_manifest') or [],
                'entry_date_matching': 'exact_open_datetime',
                'frozen_stratargs': config['stratargs'], 'replay_execution_status': 'completed',
                'backfill_archive': str(archive),
            }
            recovered = persist_day(repo, profile, day, entries, {
                'strategy': config['strategy'],
                'hash': replay.wr.profile_param_version_hash(
                    config['stratargs'], snapshot['checkout']['code_commit'],
                    snapshot['checkout']['core_commit'], {},
                ),
                'code': snapshot['checkout']['code_commit'],
                'core': snapshot['checkout']['core_commit'], 'run_id': run_id,
                'output': str(output), 'summary': summary,
                'reasons': decision_reasons(output / 'runtime.log', day, entries),
                'job_id': job_id, 'baseline_id': snapshot['baseline_id'],
                'baseline_kind': 'selected_profile_baseline',
            })
            outcome = {
                'profile': profile, 'entry_date': str(day), 'status': 'ok',
                'trades': len(entries), 'recovered_original_closures': recovered,
            }
        except Exception as exc:
            outcome = {
                'profile': profile, 'entry_date': str(day),
                'status': 'failed', 'error': str(exc),
            }
        outcomes.append(outcome)
        (archive / 'outcomes.json').write_text(json.dumps(outcomes, indent=2), encoding='utf-8')
        print(json.dumps(outcome), flush=True)
        if progress_callback:
            progress_callback(index, day, list(outcomes))
    return {'archive': str(archive), 'dates': outcomes, 'failed': sum(r['status'] == 'failed' for r in outcomes)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profiles', nargs='+', choices=['development', 'challenger', 'mirror'],
                        default=['development', 'challenger'])
    parser.add_argument('--window', type=int, default=25)
    parser.add_argument('--as-of', type=date.fromisoformat, default=date.today())
    parser.add_argument('--execute', action='store_true', help='Default is a read-only plan')
    parser.add_argument('--db-dsn')
    args = parser.parse_args()
    if args.window <= 0:
        parser.error('--window must be positive')
    sessions = sorted({date.fromisoformat(r['day']) for r in replay.wr._load_cached_market_windows()
                       if date.fromisoformat(r['day']) <= args.as_of})[:-1]
    days = sessions[-args.window:]
    if len(days) != args.window:
        raise ValueError('Calendar has insufficient settled sessions')
    profiles = replay.discover_profiles()
    plan = {'as_of': str(args.as_of), 'entry_dates': [str(d) for d in days],
            'profiles': args.profiles, 'kind': 'current_params_counterfactual'}
    print(json.dumps(plan, indent=2), flush=True)
    if not args.execute:
        return
    repo = replay.wr.WatchtowerRepository(dsn=args.db_dsn)
    archive = Path(__file__).resolve().parent / 'watchtower_scratch' / ('backfill_' + uuid.uuid4().hex[:10])
    archive.mkdir(parents=True)
    (archive / 'plan.json').write_text(json.dumps(plan, indent=2))
    outcomes = []
    for profile in args.profiles:
        env = profiles[profile]
        if env.get('TRADING_MODE') != 'paper':
            raise ValueError(f'{profile} must be paper')
        root = Path(env['CODE_ROOT'])
        current = load_profile_env(profile)
        params = parse_strategy_args(current['STRATARGS'])
        core = subprocess.check_output(['git', '-C', str(root/'bt-core'), 'rev-parse', 'HEAD'], text=True).strip()
        code = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
        previous = repo.list_reconciliation_results(profile, limit=10000)
        (archive / (profile + '-before.json')).write_text(json.dumps(previous, default=str, indent=2))
        with repo.connect() as conn:
            with conn.cursor() as cur:
                for table in ('scheduled_entry_decisions', 'scheduled_backtest_replay_verifications', 'scheduled_entry_settlements'):
                    cur.execute('SELECT * FROM '+table+' WHERE profile=%s AND trading_date=ANY(%s)'+
                        (" AND source='backtest'" if table in ('scheduled_entry_decisions','scheduled_entry_settlements') else ''),(profile,days))
                    (archive/f'{profile}-{table}-before.json').write_text(json.dumps(replay.wr._cursor_rows(cur),default=str,indent=2))
        for day in days:
            try:
                cash = replay.resolve_historical_cash(env, day)
                if cash is None or cash <= 0:
                    raise ValueError('Historical equity unavailable')
                run_id = f'current_params_{profile}_{day:%Y%m%d}_{uuid.uuid4().hex[:6]}'
                output = replay.outpath_for(root/'bt-core', current['STRAT'], run_id)
                cmd = [str(root/'bt-core/.venv/bin/python'), 'btmain.py', '--strat', current['STRAT'],
                       '--ticker', current['TICKER'], '--stratargs', current['STRATARGS'],
                       '--provider', current['DATA_PROVIDER'], '--timeframe', 'daily', '--mode', 'backtest',
                       '--fromdate', str(day-timedelta(days=int(current['FROM_DAYS']))),
                       '--todate', str(min(day+timedelta(days=10), args.as_of)),
                       '--cash', str(cash), '--margin-leverage', current['MARGIN_LEVERAGE'],
                       '--commission', current['COMMISSION'], '--id', run_id]
                with (archive / f'{profile}-{day}.log').open('w') as log:
                    subprocess.run(cmd, cwd=root/'bt-core', stdout=log, stderr=subprocess.STDOUT,
                                   check=True, timeout=600)
                entries = replay.load_backtest_entries(output/'trades.json', day)
                assert all(e['bt_entry_date'] == str(day) for e in entries.values())
                summary = dict(baseline_kind='current_params_counterfactual', cash_used=cash,
                               historical_equity_source='alpaca_portfolio_history',
                               market_data_source='current_shared_dataset',
                               entry_date_matching='exact_open_datetime', current_stratargs=params,
                               replay_execution_status='completed', backfill_archive=str(archive))
                recovered = persist_day(repo,profile,day,entries,dict(strategy=current['STRAT'],
                    hash=replay.wr.profile_param_version_hash(params,code,core,{}),code=code,core=core,
                    run_id=run_id,output=str(output),summary=summary,
                    reasons=decision_reasons(output/'runtime.log',day,entries)))
                outcome = {'profile': profile, 'entry_date': str(day), 'status': 'ok',
                           'trades': len(entries), 'recovered_original_closures': recovered}
            except Exception as exc:
                outcome = {'profile': profile, 'entry_date': str(day), 'status': 'failed', 'error': str(exc)}
            outcomes.append(outcome)
            print(json.dumps(outcome), flush=True)
            (archive/'outcomes.json').write_text(json.dumps(outcomes, indent=2))
    if any(r['status'] == 'failed' for r in outcomes):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
