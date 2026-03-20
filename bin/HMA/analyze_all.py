#!/usr/bin/env python3
"""
Analisi unificata di tutti i sweep HMA.

Legge i risultati da tuning2/, tuning3/, tuning4/ e produce una tabella
comparativa ordinata per SQN.

Uso:
    python3 bin/hma/analyze_all.py
"""
import json, os, sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../bt-core/out/intraday/HMA')

SWEEPS = {
    'tuning2': 'Sweep1 — period/exitbar baseline',
    'tuning3': 'Sweep2 — fine-grained intorno a p14_eb6',
    'tuning4': 'Sweep3 — SL / TP / ATR filter',
}

all_rows = []

for sweep_dir, sweep_label in SWEEPS.items():
    sweep_path = os.path.join(ROOT, sweep_dir)
    if not os.path.isdir(sweep_path):
        continue

    rows = []
    for d in sorted(os.listdir(sweep_path)):
        rfile = os.path.join(sweep_path, d, 'results.json')
        if not os.path.isfile(rfile):
            continue
        try:
            data = json.load(open(rfile))
        except Exception:
            continue
        for k, v in data.items():
            if not isinstance(v, dict) or 'trades' not in v:
                continue
            rows.append({
                'sweep':   sweep_dir,
                'name':    d,
                'period':  v.get('param_period'),
                'exitbar': v.get('param_exitbar'),
                'sl_pct':  v.get('param_sl_pct', 0.0) or 0.0,
                'tp_pct':  v.get('param_tp_pct', 0.0) or 0.0,
                'atr_min': v.get('param_atr_min_pct', 0.0) or 0.0,
                'trades':  v.get('trades', 0),
                'sqn':     v.get('SQN', 0.0),
                'pnl_pct': v.get('PNL', 0.0) * 100,
                'sharpe':  v.get('Sharpe', 0.0),
            })
            break

    if rows:
        print(f"\n{'='*80}")
        print(f"{sweep_label}  [{sweep_dir}]")
        print(f"{'='*80}")
        rows.sort(key=lambda x: x['sqn'], reverse=True)
        print(f"{'Name':18s} {'P':>4s} {'EB':>4s} {'SL%':>6s} {'TP%':>6s} {'ATR%':>6s} "
              f"{'Trades':>8s} {'SQN':>8s} {'PNL%':>8s} {'Sharpe':>8s}")
        print(f"{'-'*78}")
        for r in rows:
            p   = str(r['period'])  if r['period']  else '—'
            eb  = str(r['exitbar']) if r['exitbar'] is not None else '—'
            sl  = f"{r['sl_pct']*100:.1f}%" if r['sl_pct'] else '  —  '
            tp  = f"{r['tp_pct']*100:.1f}%" if r['tp_pct'] else '  —  '
            atr = f"{r['atr_min']*100:.2f}%" if r['atr_min'] else '  —  '
            mark = ' ← BEST' if r == rows[0] else ''
            print(f"{r['name']:18s} {p:>4s} {eb:>4s} {sl:>6s} {tp:>6s} {atr:>6s} "
                  f"{r['trades']:8d} {r['sqn']:8.3f} {r['pnl_pct']:7.1f}% {r['sharpe']:8.3f}{mark}")
        all_rows.extend(rows)

# Riepilogo globale: top 10
if all_rows:
    all_rows.sort(key=lambda x: x['sqn'], reverse=True)
    print(f"\n{'='*80}")
    print("TOP 10 — tutti i sweep")
    print(f"{'='*80}")
    print(f"{'Sweep':8s} {'Name':18s} {'SQN':>8s} {'PNL%':>8s} {'Sharpe':>8s} {'Trades':>8s}")
    print(f"{'-'*68}")
    for r in all_rows[:10]:
        print(f"{r['sweep']:8s} {r['name']:18s} {r['sqn']:8.3f} {r['pnl_pct']:7.1f}% "
              f"{r['sharpe']:8.3f} {r['trades']:8d}")

    best = all_rows[0]
    print(f"\n*** OVERALL BEST: [{best['sweep']}] {best['name']} "
          f"— SQN={best['sqn']:.3f}, PNL={best['pnl_pct']:.1f}%, Sharpe={best['sharpe']:.3f} ***\n")
