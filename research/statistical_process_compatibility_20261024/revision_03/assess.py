"""Decision criteria fixed in protocol; reports failures as well as successes."""
import json
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parent
STRONG=[('iid','sticky_06'),('sticky_06','iid'),('cycle_forward','cycle_reverse'),('lag2','iid'),('sticky_09','iid')]
LABELS={'iid':'Dado indipendente','sticky_06':'Magnetico, persistenza media',
        'sticky_09':'Magnetico, persistenza alta','cycle_forward':'Ciclo direzionale',
        'lag2':'Memoria a 2 passi','lag5':'Memoria a 5 passi','lag7':'Memoria a 7 passi',
        'lag10':'Memoria a 10 passi','lag8':'Memoria a 8 passi'}


def assessment():
    points=pd.read_csv(ROOT/'results/pointwise_summary.csv')
    monitoring=pd.read_csv(ROOT/'results/monitoring_summary.csv')
    confirm=points[points.phase.eq('confirmation')&points.method.eq('C5_heldout_score')&points.window.eq('joint')]
    nulls=confirm[confirm['null'].eq(confirm.scenario)]
    period=monitoring[monitoring.phase.eq('confirmation')&monitoring.method.eq('C6_period_maximum')&monitoring['null'].eq(monitoring.scenario)]
    strong=[]
    for null,scenario in STRONG:
        row=confirm[confirm['null'].eq(null)&confirm.scenario.eq(scenario)].iloc[0]
        strong.append(dict(null=null,scenario=scenario,rate=row.rate,ci95_low=row.ci95_low,
                           ci95_high=row.ci95_high,passes_80pct=bool(row.rate>=.8)))
    gates=[dict(criterion='Upper CI of pointwise false alarms <= 7.5% for every confirmation null',
                passed=bool(nulls.ci95_high.le(.075).all()),worst_upper_ci=float(nulls.ci95_high.max())),
           dict(criterion='Upper CI of whole-period false alarms <= 7.5% for every null and horizon',
                passed=bool(period.ci95_high.le(.075).all()),worst_upper_ci=float(period.ci95_high.max())),
           dict(criterion='At least 4/5 strong change families have mean recognition >=80%',
                passed=sum(row['passes_80pct'] for row in strong)>=4,
                families_passed=sum(row['passes_80pct'] for row in strong)),
           dict(criterion='No strong change family has zero recognition',
                passed=all(row['rate']>0 for row in strong))]
    long=[]
    for null in ['lag5','lag7','lag10']:
        row=confirm[confirm['null'].eq(null)&confirm.scenario.eq('iid')].iloc[0]
        long.append(dict(null=null,rate=row.rate,ci95_low=row.ci95_low,ci95_high=row.ci95_high,passes_50pct=bool(row.rate>=.5)))
    gates.append(dict(criterion='Removal of lag5/7/10 dependence recognized in at least 50% of blocks for each case',
                      passed=all(row['passes_50pct'] for row in long),long_memory_cases=long))
    result=dict(status='eligible_for_real_data_validation' if all(g['passed'] for g in gates) else 'fails_prespecified_advance_criteria',
                gates=gates,strong_change_families=strong,
                pointwise_null_range=[float(nulls.rate.min()),float(nulls.rate.max())],
                period_null_range=[float(period.rate.min()),float(period.rate.max())],
                applies_to='Synthetic categorical sequences; not a production approval or a posterior probability of sameness.')
    (ROOT/'results/assessment.json').write_text(json.dumps(result,indent=2))
    return result


if __name__=='__main__':
    print(json.dumps(assessment(),indent=2))
