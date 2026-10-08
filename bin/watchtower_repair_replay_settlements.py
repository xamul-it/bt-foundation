#!/usr/bin/env python3
"""Invalidate legacy replay settlements and comparisons, with a durable backup."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bt-core"))
import watchtower_runtime as wr


def main():
    repo = wr.WatchtowerRepository()
    backup_dir = Path(__file__).resolve().parents[1] / "backup" / "reconciliation-repair"
    backup_dir.mkdir(parents=True, exist_ok=True)
    path = backup_dir / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".json")
    with repo.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM scheduled_entry_settlements WHERE source='backtest' AND detail->>'origin'='execution_replay' AND NOT (detail ? 'exit_date') FOR UPDATE")
            settlements = wr._cursor_rows(cur)
            cur.execute("SELECT * FROM execution_reconciliation_results FOR UPDATE")
            results = [row for row in wr._cursor_rows(cur) if
                       'bt_settlement_pending' not in row['summary'] and (
                           any(d.get('bt') and not d['bt'].get('bt_entry_date') for d in row['diffs']) or
                           row['summary'].get('bt_no_trade'))]
            keys = {(row['profile'], row['trading_date']) for row in settlements + results}
            cur.execute("SELECT * FROM reconciliation_queue")
            queues = [row for row in wr._cursor_rows(cur) if (row['profile'], row['trading_date']) in keys]
            with path.open('x') as handle:
                json.dump({'settlements': settlements, 'results': results, 'queues': queues}, handle, default=str, indent=2)
                handle.flush()
                import os
                os.fsync(handle.fileno())
            for row in settlements:
                cur.execute("SELECT orders FROM scheduled_entry_decisions WHERE profile=%s AND trading_date=%s AND source='backtest'", (row['profile'], row['trading_date']))
                decision = cur.fetchone()
                order = next((x for x in (decision[0] if decision else []) if x.get('symbol') == row['symbol']), {})
                cur.execute("""UPDATE scheduled_entry_settlements SET status='pending',
                    entry_date=trading_date, exit_date=NULL, exit_price=NULL, pnl=NULL, pnl_pct=NULL,
                    qty=%s, entry_price=%s,
                    detail='{"origin":"replay_repair","reason":"legacy replay discarded actual execution dates"}'::jsonb,
                    updated_at=NOW() WHERE id=%s""", (order.get('qty'), order.get('reference_price'), row['id']))
            for row in results:
                cur.execute("""UPDATE execution_reconciliation_results SET status='error',
                    error='Legacy replay date association invalidated; awaiting corrected replay',
                    summary='{"repair_pending":true}'::jsonb, diffs='[]'::jsonb, updated_at=NOW() WHERE id=%s""", (row['id'],))
            for profile, day in keys:
                cur.execute("""INSERT INTO reconciliation_queue(profile,trading_date,status,reason,detail)
                    VALUES(%s,%s,'pending','replay_date_repair','{}'::jsonb)
                    ON CONFLICT(profile,trading_date) DO UPDATE SET status='pending', reason='replay_date_repair', updated_at=NOW()""", (profile, day))
        conn.commit()
    print(json.dumps({'backup': str(path), 'reset_settlements': len(settlements), 'invalidated_results': len(results), 'requeued_days': len(keys)}))


if __name__ == '__main__':
    main()
