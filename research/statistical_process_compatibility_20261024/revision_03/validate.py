"""Independent record, raw-score, normalization and period-alarm checks."""
from collections import Counter
from itertools import groupby
from pathlib import Path
import hashlib
import json
import math
import numpy as np
import pandas as pd
from experiment import HeldOutReference, ROOT


def manual_features(path,training):
    result=[]
    for order in (1,2,3):
        probabilities=Counter(tuple(training[i:i+order]) for i in range(len(training)-order+1))
        words=Counter(tuple(path[i:i+order]) for i in range(len(path)-order+1))
        denominator=len(training)-order+1+.5*(6**order)
        observations=len(path)-order+1
        score=0.
        for word,count in words.items():
            p=(probabilities[word]+.5)/denominator
            score+=2*count*math.log(count/(observations*p))
        result.append(max(0,score))
    result.append(max(len(list(g)) for _,g in groupby(path)))
    for lag in range(1,min(10,len(path)-1)+1):
        result.append(sum(path[i]==path[i-lag] for i in range(lag,len(path))))
    return np.array(result,dtype=float)


def validate():
    manifest=json.loads((ROOT/'results/manifest.json').read_text())
    for name,expected in manifest['sha256'].items():
        assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==expected,name
    original=ROOT.parent
    first_manifest=json.loads((original/'delivery_manifest.json').read_text())
    for name,expected in first_manifest['sha256'].items():
        assert hashlib.sha256((original/name).read_bytes()).hexdigest()==expected,name
    points=pd.read_csv(ROOT/'results/pointwise.csv')
    monitor=pd.read_csv(ROOT/'results/monitoring.csv')
    assert not points.duplicated(['phase','null','scenario','history_size','replication','method','window']).any()
    assert not monitor.duplicated(['phase','null','scenario','history_size','replication','method','horizon']).any()
    assert np.allclose(points.rate,points.rejected/points.trials)
    assert np.allclose(monitor.rate,monitor.rejected/monitor.trials)
    assert (points.rejected.between(0,points.trials)).all()
    assert (monitor.rejected.between(0,monitor.trials)).all()
    assert (points.groupby(['phase','null','scenario','history_size','method','window']).replication.nunique()==20).all()
    capacity=pd.read_csv(ROOT/'results/capacity.csv')
    assert (~capacity[capacity.history_size.eq(2480)&capacity.horizon.eq(100)].estimable).all()
    checks=[]
    for phase,size,null in [('development',2480,'sticky_06'),('confirmation',4960,'lag10'),('confirmation',4960,'lag8')]:
        archive=np.load(ROOT/'inputs'/f'{phase}_{size}_{null}_00.npz')
        history=archive['history']
        reference=HeldOutReference(history)
        sample=archive['point_iid']
        for length in [3,5,10,15,20]:
            manual=np.stack([manual_features(path[-length:],history[:500]) for path in sample[:12]])
            assert np.allclose(manual,reference.features(sample[:12,-length:]),atol=1e-8)
        normalize=np.stack([manual_features(history[i:i+20],history[:500]) for i in range(500,981)])
        assert np.allclose(reference.center[20],normalize.mean(axis=0),atol=1e-8)
        assert np.allclose(reference.scale[20],np.maximum(normalize.std(axis=0,ddof=1),.25),atol=1e-8)
        scores=reference.scores(sample)
        bank=reference.calibration_scores.max(axis=1)
        manual_p=np.array([(1+sum(v>=score for v in bank))/(len(bank)+1) for score in scores.max(axis=1)])
        selected=points[(points.phase==phase)&(points.history_size==size)&(points['null']==null)&points.replication.eq(0)&points.scenario.eq('iid')&points.method.eq('C5_heldout_score')&points.window.eq('joint')].iloc[0]
        assert (manual_p<=.05).sum()==selected.rejected
        assert manual_p.min()>=1/(len(bank)+1)
        if size==4960:
            timeline=archive['monitor_'+null]
            timeline_scores=reference.timeline_scores(timeline)
            for horizon in [20,100]:
                ref=reference.horizon_references[horizon]
                values=timeline_scores[:,:horizon].max(axis=1)
                p=np.array([(1+sum(v>=value for v in ref))/(len(ref)+1) for value in values])
                row=monitor[(monitor.phase==phase)&monitor['null'].eq(null)&monitor.scenario.eq(null)&monitor.replication.eq(0)&monitor.horizon.eq(horizon)&monitor.method.eq('C6_period_maximum')].iloc[0]
                assert (p<=.05).sum()==row.rejected
        checks.append(dict(phase=phase,history_size=size,null=null,raw_features_checked=True,
                           normalizer_heldout_checked=True,rank_counts_checked=True))
    summary=pd.read_csv(ROOT/'results/pointwise_summary.csv')
    for row in summary.itertuples():
        group=points[(points.phase==row.phase)&(points['null']==row.null)&(points.scenario==row.scenario)&(points.history_size==row.history_size)&(points.method==row.method)&(points.window==row.window)]
        assert math.isclose(group.rate.mean(),row.rate,abs_tol=1e-12)
        assert group.rejected.sum()==row.rejected
    result=dict(status='Reviewed synthetic scope; not an application approval',
                pointwise_rows=len(points),monitoring_rows=len(monitor),
                original_study_unchanged=True,all_manifest_hashes_match=True,
                representative_independent_checks=checks,
                confidence_note='t intervals across 20 histories are approximate; degenerate intervals do not establish exact probability 0 or 1.')
    (ROOT/'results/validation.json').write_text(json.dumps(result,indent=2))
    return result


if __name__=='__main__':
    print(json.dumps(validate(),indent=2))
