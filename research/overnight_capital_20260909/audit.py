"""Recompute the historical/current comparison from standard daily returns."""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'out/overnight_ah_capital_study_20260909'
BASE = ROOT / 'out/overnight_ah/OvernightAH'
CASES = [
    ('Storico salvato', 'sizing_waterfall_cash50k_nocost'),
    ('Codice agosto, dati attuali', 'capital_audit_20260909_50k_aug24'),
    ('Prima del nuovo cooldown, lag 0', 'capital_audit_20260909_50k_before_cooldown'),
    ('Codice attuale, lag 0', 'capital_audit_20260909_50k_nocost'),
    ('Codice attuale, lag 1', 'capital_audit_20260909_lag1_50k'),
]


def main():
    reference = pd.read_csv(BASE / CASES[0][1] / 'returns.csv', index_col=0).iloc[:, 0]
    rows = []
    for label, name in CASES:
        r = pd.read_csv(BASE / name / 'returns.csv', index_col=0).iloc[:, 0]
        assert r.index.equals(reference.index)
        assert r.index.is_unique and np.isfinite(r).all()
        curve = (1 + r).cumprod()
        final = float(50000 * curve.iloc[-1])
        differences = abs(r - reference)
        rows.append(dict(label=label, run=name, days=len(r), final_equity=final,
                         profit=final - 50000, sharpe=float(np.sqrt(252) * r.mean() / r.std(ddof=1)),
                         max_dd=float((curve / curve.cummax().clip(lower=1) - 1).min()),
                         max_return_difference=float(differences.max()),
                         different_days=int((differences > 1e-10).sum()),
                         first_different_day=next((str(d) for d in r.index if differences[d] > 1e-10), None)))
    pd.DataFrame(rows).to_csv(OUT / 'audit_summary.csv', index=False)
    print(pd.DataFrame(rows)[['label', 'final_equity', 'sharpe', 'max_dd']].to_string(index=False))


if __name__ == '__main__':
    main()
