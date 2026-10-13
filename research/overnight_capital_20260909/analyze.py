"""Reproducible cash-flow-aware metrics and audit of saved daily evidence."""
from pathlib import Path
import json
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
STUDY = ROOT / 'out/overnight_ah_capital_study_20260909'


def sharpe(r):
    return float(np.sqrt(252) * r.mean() / r.std(ddof=1)) if r.std(ddof=1) else None


def main():
    rows, annual_rows, validation = [], [], []
    for case in sorted((STUDY / 'cases').glob('*')):
        if not (case / 'complete.json').exists():
            continue
        config = json.loads((case / 'config.json').read_text())['options']
        d = pd.read_csv(case / 'ledger.csv', parse_dates=['date'])
        assert d.date.is_unique and d.date.is_monotonic_increasing
        assert np.isfinite(d.select_dtypes('number').to_numpy()).all()
        d = d[d.date >= pd.Timestamp(config['start'])].copy()
        c = config['cash']
        identity_error = float(abs(d.pnl.sum() - (d.equity_end.iloc[-1] + d.withdrawal.sum() - c)))
        assert identity_error < max(0.01, c * 1e-8)
        flows = d[d.withdrawal > 0]
        assert np.allclose(flows.equity_end, c)
        assert ((flows.equity_end + flows.withdrawal) > c * (1 + config['band'])).all()
        assert (d.withdrawal >= 0).all()
        raw_path = ROOT / 'out' / ('overnight_ah_flat_composite/OvernightAHFlatComposite' if config['strategy'] == 'composite' else 'overnight_ah/OvernightAH') / case.name / 'returns.csv'
        raw = pd.read_csv(raw_path, index_col=0).iloc[:, 0]
        noflow_error = None
        if config['no_withdrawals']:
            noflow_error = float(np.max(np.abs(raw.to_numpy() - pd.read_csv(case / 'ledger.csv').return_adjusted.to_numpy())))
            assert noflow_error < 1e-10
        validation.append(dict(case=case.name, rows=len(d), identity_error=identity_error,
                               noflow_max_return_error=noflow_error, deferred_days=int(d.withdrawal_deferred.sum())))
        curve = (1 + d.return_adjusted).cumprod()
        curve_peak = curve.cummax().clip(lower=1)
        full_years = []
        for year, y in d.groupby(d.date.dt.year):
            # Fixed calendar-year income, excludes partial 2026 and partial starts.
            complete = year < 2026 and y.date.iloc[0].month == 1 and y.date.iloc[-1].month == 12
            rec = dict(case=case.name, strategy=config['strategy'], capital=c, year=int(year),
                       complete=bool(complete), days=len(y), profit=float(y.pnl.sum()),
                       yield_on_capital=float(y.pnl.sum() / c), withdrawals=float(y.withdrawal.sum()),
                       withdrawal_yield=float(y.withdrawal.sum() / c),
                       sharpe=sharpe(y.return_adjusted), end_equity=float(y.equity_end.iloc[-1]))
            annual_rows.append(rec)
            if complete:
                full_years.append(rec)
        a = pd.DataFrame(full_years)
        if a.empty:
            continue
        withdrawal_dates = list(flows.date)
        edges = [d.date.iloc[0]] + withdrawal_dates + [d.date.iloc[-1]]
        max_gap = max((b - a).days for a, b in zip(edges, edges[1:]))
        rows.append(dict(case=case.name, strategy=config['strategy'], capital=c,
                         no_withdrawals=config['no_withdrawals'], cooldown=config['cooldown'],
                         start=config['start'], years=len(a), days=len(d),
                         avg_annual_profit=float(a.profit.mean()),
                         avg_annual_yield=float(a.yield_on_capital.mean()),
                         avg_annual_withdrawal=float(a.withdrawals.mean()),
                         avg_annual_withdrawal_yield=float(a.withdrawal_yield.mean()),
                         median_annual_yield=float(a.yield_on_capital.median()),
                         p10_annual_yield=float(a.yield_on_capital.quantile(.1)),
                         worst_annual_yield=float(a.yield_on_capital.min()),
                         positive_year_share=float((a.profit > 0).mean()),
                         no_withdrawal_years=int((a.withdrawals == 0).sum()),
                         sharpe=sharpe(d.return_adjusted),
                         max_dd_twr=float((curve / curve_peak - 1).min()),
                         min_equity_ratio=float(d.equity_end.min() / c),
                         avg_equity_ratio=float(d.equity_start.mean() / c),
                         final_equity=float(d.equity_end.iloc[-1]),
                         total_withdrawals=float(d.withdrawal.sum()),
                         total_profit=float(d.pnl.sum()),
                         withdrawal_count=len(flows), max_days_without_withdrawal=max_gap))
    pd.DataFrame(rows).to_csv(STUDY / 'summary.csv', index=False)
    pd.DataFrame(annual_rows).to_csv(STUDY / 'annual.csv', index=False)
    (STUDY / 'validation.json').write_text(json.dumps(validation, indent=2))
    if rows:
        print(pd.DataFrame(rows)[['case', 'avg_annual_yield', 'avg_annual_profit', 'sharpe', 'min_equity_ratio']].to_string(index=False))


if __name__ == '__main__':
    main()
