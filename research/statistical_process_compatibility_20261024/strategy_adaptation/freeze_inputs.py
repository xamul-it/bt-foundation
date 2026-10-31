"""Freeze native returns and daily trade aggregates; read-only database access."""
from pathlib import Path
import hashlib
import json
import shutil
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[2]
sys.path.insert(0, str(WORKSPACE / 'bt-core'))
import watchtower_runtime as wr

SOURCES = {
    113: ('OvernightAH', Path('/home/htpc/backtrader-prod/out/overnight_ah/OvernightAH/baseline_live_e1eb0e34')),
    108: ('FlatComposite', WORKSPACE / 'out/overnight_ah_flat_composite/OvernightAHFlatComposite/baseline_challenger_8745d633'),
}


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(2**20), b''):
            h.update(chunk)
    return h.hexdigest()


def main():
    with wr.WatchtowerRepository().connect() as conn:
        conn.read_only = True
        with conn.cursor() as cursor:
            cursor.execute('''SELECT id, profile, strategy, label, run_id,
                window_start, window_end, sample_size, created_at,
                configuration_hash, core_commit, provenance_version, metrics
                FROM profile_baselines WHERE id IN (113,108) ORDER BY id DESC''')
            columns = [c[0] for c in cursor.description]
            records = [dict(zip(columns, row)) for row in cursor.fetchall()]
    manifest = {'scope': 'Frozen native backtest outputs; database SELECT only', 'sources': []}
    for record in records:
        name, source = SOURCES[record['id']]
        target = ROOT / 'inputs' / name
        target.mkdir(parents=True, exist_ok=True)
        for filename in ['returns.csv', 'results.json']:
            shutil.copyfile(source / filename, target / filename)
        trades = pd.DataFrame(json.loads((source / 'trades.json').read_text()))
        expected = record['sample_size']
        assert len(trades) == expected, (name, len(trades), expected)
        for field in ['pnl', 'pnlcomm', 'value', 'pnl_pct']:
            assert np.isfinite(pd.to_numeric(trades[field])).all(), (name, field)
        assert (trades['value'] > 0).all()
        trades['day'] = pd.to_datetime(trades['close_datetime']).dt.normalize()
        trades['pnl_pct_squared'] = trades['pnl_pct'] ** 2
        aggregate = trades.groupby('day').agg(
            trades=('pnl', 'size'), net_pnl=('pnlcomm', 'sum'),
            gross_pnl=('pnl', 'sum'), entry_notional=('value', 'sum'),
            sum_trade_pct=('pnl_pct', 'sum'), sum_squared_trade_pct=('pnl_pct_squared', 'sum'),
            mean_trade_pct=('pnl_pct', 'mean'),
        )
        aggregate['net_return'] = aggregate.net_pnl / aggregate.entry_notional
        aggregate['gross_return'] = aggregate.gross_pnl / aggregate.entry_notional
        aggregate.to_csv(target / 'daily_trades.csv', float_format='%.17g')
        # Retain compact individual fields for independent aggregation checks.
        trades[['close_datetime', 'pnl', 'pnlcomm', 'value', 'pnl_pct']].to_csv(
            target / 'trade_inputs.csv.gz', index=False, float_format='%.17g',
            compression={'method': 'gzip', 'mtime': 0},
        )
        (target / 'baseline.json').write_text(json.dumps(record, indent=2, default=str) + '\n')
        row = next(iter(json.loads((target / 'results.json').read_text()).values()))
        assert row['input_id'] == record['run_id']
        assert row['provenance']['configuration_hash'] == record['configuration_hash']
        entry = {
            'name': name, 'baseline_id': record['id'], 'run_id': record['run_id'],
            'source_directory': str(source), 'resolved_source_directory': str(source.resolve()),
            'source_sha256': {f: sha(source / f) for f in ['returns.csv', 'results.json', 'trades.json']},
            'commission': row.get('input_commission'), 'margin_leverage': row.get('input_margin_leverage'),
            'strategy': row['strategy'], 'window_end': str(record['window_end']),
            'code_commit': row['provenance'].get('core_commit'), 'trades': len(trades),
        }
        manifest['sources'].append(entry)
        print(json.dumps({k: entry[k] for k in ['name', 'baseline_id', 'trades', 'commission', 'margin_leverage']}))
    manifest['frozen_sha256'] = {str(f.relative_to(ROOT)): sha(f) for f in sorted((ROOT / 'inputs').rglob('*')) if f.is_file()}
    (ROOT / 'results/source_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    main()
