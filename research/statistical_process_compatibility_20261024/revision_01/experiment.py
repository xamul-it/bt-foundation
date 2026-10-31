"""Research revision: raw word-pattern scores and fixed-horizon calibration.

No application imports/writes. Original research class is used read-only as C0.
"""
from __future__ import annotations
import hashlib
import importlib.util
import json
import math
import os
import platform
import resource
import time
from pathlib import Path

for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'):
    os.environ[key] = '1'
if hasattr(os, 'sched_getaffinity'):
    available = sorted(os.sched_getaffinity(0))
    if len(available) > 2:
        os.sched_setaffinity(0, {available[0]})

import numpy as np
import pandas as pd
import scipy
from scipy.stats import t

ROOT = Path(__file__).resolve().parent
PARENT = ROOT.parent
spec = importlib.util.spec_from_file_location('original_statistical_study', PARENT / 'experiment.py')
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
WINDOWS = (3, 5, 10, 15, 20)
CASES = {
    'iid': ['iid', 'sticky_06', 'lag2', 'cycle_forward', 'bias_mild'],
    'sticky_06': ['sticky_06', 'iid', 'sticky_09', 'sticky_05', 'sticky_07'],
    'sticky_09': ['sticky_09', 'iid', 'sticky_06'],
    'cycle_forward': ['cycle_forward', 'cycle_reverse', 'iid'],
    'lag2': ['lag2', 'iid', 'lag10'],
    'lag10': ['lag10', 'iid', 'lag2'],
}
CONFIG = dict(development_seed=20261026, confirmation_seed=20261027,
              history_sizes=[2480,4960,9200], training_size=1000, gap=100,
              replications=20, development_episodes=1000, confirmation_episodes=2000,
              monitoring_trajectories=100, monitoring_horizons=[20,100],
              nominal_alpha=.05, windows=list(WINDOWS), smoothing=.5, scale_floor=.25,
              pointwise_stride=20, horizon_stride_offset=40,
              cases=CASES, confirmation_new_null='lag5')
XLOGX = np.zeros(21)
XLOGX[1:] = np.arange(1, 21) * np.log(np.arange(1, 21))


def generate(kind, count, length, rng, context=None):
    """Stationary episodes, or continuation from observed old-process context."""
    if context is not None:
        history = np.asarray(context, dtype=np.int8)
        output = np.empty((count, history.shape[1] + length), dtype=np.int8)
        output[:, :history.shape[1]] = history
        for position in range(history.shape[1], output.shape[1]):
            if kind in ('iid', 'bias_mild'):
                probabilities = np.ones(6)/6 if kind == 'iid' else np.array([.15]*5+[.25])
                output[:, position] = rng.choice(6, count, p=probabilities)
            elif kind.startswith('lag'):
                lag = int(kind[3:])
                output[:, position] = np.where(rng.random(count) < .8, output[:, position-lag], rng.integers(0,6,count))
            elif kind.startswith('sticky_'):
                rho = int(kind.split('_')[1])/10
                output[:, position] = np.where(rng.random(count) < rho, output[:, position-1], rng.integers(0,6,count))
            elif kind in ('cycle_forward', 'cycle_reverse'):
                step = 1 if kind == 'cycle_forward' else 5
                output[:, position] = (output[:, position-1] + np.where(rng.random(count)<.8,step,rng.integers(0,6,count))) % 6
            else:
                raise ValueError(kind)
        return output[:, -length:]
    if kind in ('iid', 'bias_mild'):
        probabilities = np.ones(6)/6 if kind == 'iid' else np.array([.15]*5+[.25])
        return rng.choice(6,(count,length),p=probabilities).astype(np.int8)
    if kind.startswith('lag'):
        lag = int(kind[3:])
        result = np.empty((count,length),dtype=np.int8)
        for parity in range(min(lag,length)):
            result[:,parity::lag] = generate('sticky_08',count,len(range(parity,length,lag)),rng)
        return result
    probabilities = np.ones(6)/6
    if kind.startswith('sticky_'):
        rho = int(kind.split('_')[1])/10
        probabilities *= 1-rho
        probabilities[0] += rho
    elif kind in ('cycle_forward','cycle_reverse'):
        probabilities *= .2
        probabilities[1 if kind=='cycle_forward' else 5] += .8
    else:
        raise ValueError(kind)
    initial = rng.integers(0,6,(count,1))
    increments = rng.choice(6,(count,max(0,length-1)),p=probabilities)
    return np.concatenate([initial,(initial+np.cumsum(increments,axis=1))%6],axis=1).astype(np.int8)


def word_codes(paths, order):
    codes = np.zeros((len(paths), paths.shape[1]-order+1), dtype=np.int16)
    for index in range(order):
        codes = codes*6 + paths[:,index:index+codes.shape[1]]
    return codes


def tail(reference, values):
    reference = np.sort(reference)
    return (1+len(reference)-np.searchsorted(reference,values,side='left'))/(len(reference)+1)


class DirectReference:
    def __init__(self, history):
        training = history[:1000]
        self.probabilities = []
        for order in (1,2,3):
            counts = np.bincount(word_codes(training[None,:],order).ravel(),minlength=6**order) + .5
            self.probabilities.append(counts/counts.sum())
        self.center, self.scale = {}, {}
        for size in WINDOWS:
            features = self.features(np.lib.stride_tricks.sliding_window_view(training,size))
            self.center[size] = features.mean(axis=0)
            self.scale[size] = np.maximum(features.std(axis=0,ddof=1),.25)
        calibration = history[1100:]
        self.calibration_paths = np.stack([calibration[i:i+20] for i in range(0,len(calibration)-19,20)])
        self.calibration_scores = self.scores(self.calibration_paths)
        self.horizon_references = {}
        for horizon in (20,100):
            length = horizon+19
            panels = np.stack([calibration[i:i+length] for i in range(0,len(calibration)-length+1,horizon+40)])
            scores = self.timeline_scores(panels)
            self.horizon_references[horizon] = np.sort(scores.max(axis=1))

    def features(self, paths):
        count,size = paths.shape
        features = []
        for order,p in enumerate(self.probabilities,start=1):
            codes = word_codes(paths,order)
            counts = np.bincount((codes+np.arange(count)[:,None]*len(p)).ravel(),minlength=count*len(p)).reshape(count,len(p))
            observations = codes.shape[1]
            deviance = 2 * (XLOGX[counts].sum(axis=1)-observations*np.log(observations)-counts@np.log(p))
            features.append(np.maximum(0,deviance))
        current = np.ones(count,dtype=int)
        longest = current.copy()
        for position in range(1,size):
            current = np.where(paths[:,position]==paths[:,position-1],current+1,1)
            longest = np.maximum(longest,current)
        return np.round(np.column_stack([*features,longest]),10)

    def scores(self, paths):
        result = np.empty((len(paths),len(WINDOWS)))
        for start in range(0,len(paths),2048):
            batch = paths[start:start+2048]
            for column,size in enumerate(WINDOWS):
                raw = self.features(batch[:,-size:])
                result[start:start+len(batch),column] = np.max(np.abs((raw-self.center[size])/self.scale[size]),axis=1)
        return np.round(result,10)

    def pvalues(self, scores):
        result = {str(size):tail(self.calibration_scores[:,column],scores[:,column]) for column,size in enumerate(WINDOWS)}
        result['joint'] = tail(self.calibration_scores.max(axis=1),scores.max(axis=1))
        return result

    def timeline_scores(self, paths):
        count = paths.shape[1]-19
        windows = np.lib.stride_tricks.sliding_window_view(paths,20,axis=1)
        return self.scores(windows.reshape(-1,20)).max(axis=1).reshape(len(paths),count)


def legacy_reference(history):
    blocks = (len(history)-1200)//80
    first = 1100
    a = np.stack([history[i:i+20] for i in range(first,first+blocks*40,40)])
    first += blocks*40+100
    b = np.stack([history[i:i+20] for i in range(first,first+blocks*40,40)])
    return old.HistoricalReference(history[:1000],a,b)


def point_record(phase,null,scenario,size,replication,method,window,pvalues,calibration_blocks):
    rejected = int(np.sum(pvalues<=.05))
    return dict(phase=phase,null=null,scenario=scenario,history_size=size,replication=replication,
                method=method,window=str(window),trials=len(pvalues),rejected=rejected,
                rate=rejected/len(pvalues),is_null=null==scenario,
                calibration_blocks=calibration_blocks,minimum_p=1/(calibration_blocks+1))


def monitor_records(phase,null,scenario,size,replication,reference,paths):
    scores = reference.timeline_scores(paths)
    rows = []
    for horizon in (20,100):
        recent = scores[:,:horizon]
        definitions = [('C1_daily_pointwise',reference.calibration_scores.max(axis=1)),
                       ('C2_period_maximum',reference.horizon_references[horizon])]
        for method,bank in definitions:
            pvalues = tail(bank,recent)
            alarms = pvalues<=.05
            detected = alarms.any(axis=1)
            first = np.where(detected,alarms.argmax(axis=1)+1,0)
            estimable = 1/(len(bank)+1)<=.05
            rows.append(dict(phase=phase,null=null,scenario=scenario,history_size=size,
                             replication=replication,horizon=horizon,method=method,
                             trials=len(paths),rejected=int(detected.sum()),rate=detected.mean(),
                             pointwise_rate=alarms.mean(),is_null=null==scenario,
                             calibration_blocks=len(bank),minimum_p=1/(len(bank)+1),estimable=estimable,
                             median_day_among_detected=float(np.median(first[detected])) if detected.any() else None,
                             delay_sum=int(first[detected].sum())))
    return rows


def run_phase(phase,seed):
    rng = np.random.default_rng(seed)
    rows,monitor,capacity = [],[],[]
    sizes = [2480,4960,9200] if phase=='development' else [4960]
    cases = CASES if phase=='development' else {
        **{null:([null,'sticky_06'] if null=='iid' else [null,'iid']) for null in CASES},
        'cycle_forward':['cycle_forward','iid','cycle_reverse'],
        'lag5':['lag5','iid'],
    }
    episodes = 1000 if phase=='development' else 2000
    for size in sizes:
        for null,scenarios in cases.items():
            for replication in range(20):
                history = generate(null,1,size,rng)[0]
                revised = DirectReference(history)
                previous = legacy_reference(history)
                payload = {'history':history}
                for horizon,bank in revised.horizon_references.items():
                    capacity.append(dict(phase=phase,null=null,history_size=size,replication=replication,
                                         horizon=horizon,blocks=len(bank),minimum_p=1/(len(bank)+1),estimable=1/(len(bank)+1)<=.05))
                for scenario in scenarios:
                    paths = generate(scenario,episodes,20,rng)
                    payload['point_'+scenario] = paths
                    scores = revised.scores(paths)
                    for window,pvalues in revised.pvalues(scores).items():
                        rows.append(point_record(phase,null,scenario,size,replication,'C1_direct_score',window,pvalues,len(revised.calibration_paths)))
                    legacy20 = previous.pvalues(paths)['ensemble5_blocks']
                    legacy_joint = previous.global_pvalue(paths)
                    for window,pvalues in [('20',legacy20),('joint',legacy_joint)]:
                        rows.append(point_record(phase,null,scenario,size,replication,'C0_double_rank',window,pvalues,len(previous.global_reference)))
                    if size==4960:
                        context = generate(null,100,19,rng)
                        future = generate(scenario,100,100,rng,context=context)
                        timeline = np.concatenate([context,future],axis=1)
                        payload['monitor_'+scenario] = timeline
                        monitor.extend(monitor_records(phase,null,scenario,size,replication,revised,timeline))
                np.savez_compressed(ROOT/'inputs'/f'{phase}_{size}_{null}_{replication:02d}.npz',**payload)
            print(f'{phase}: N={size}, null={null} completed',flush=True)
            pd.DataFrame(rows).to_csv(ROOT/'results'/f'{phase}_pointwise.csv',index=False)
            pd.DataFrame(monitor).to_csv(ROOT/'results'/f'{phase}_monitoring.csv',index=False)
            pd.DataFrame(capacity).to_csv(ROOT/'results'/f'{phase}_capacity.csv',index=False)
    return pd.DataFrame(rows),pd.DataFrame(monitor),pd.DataFrame(capacity)


def clustered_summary(data,keys):
    rows = []
    for values,group in data.groupby(keys):
        values = values if isinstance(values,tuple) else (values,)
        rates = group.rate.to_numpy()
        half = t.ppf(.975,len(rates)-1)*rates.std(ddof=1)/np.sqrt(len(rates))
        rows.append(dict(zip(keys,values),rate=rates.mean(),ci95_low=max(0,rates.mean()-half),
                         ci95_high=min(1,rates.mean()+half),baseline_count=len(rates),
                         median=rates.mean() if len(rates)==1 else np.median(rates),
                         minimum=rates.min(),maximum=rates.max(),trials=int(group.trials.sum()),
                         rejected=int(group.rejected.sum())))
    return pd.DataFrame(rows)


def main():
    start = time.monotonic()
    ROOT.joinpath('inputs').mkdir(exist_ok=True)
    ROOT.joinpath('results').mkdir(exist_ok=True)
    point,monitor,capacity = [],[],[]
    for phase,seed in [('development',20261026),('confirmation',20261027)]:
        a,b,c = run_phase(phase,seed)
        point.append(a);monitor.append(b);capacity.append(c)
    point,monitor,capacity = pd.concat(point),pd.concat(monitor),pd.concat(capacity)
    point.to_csv(ROOT/'results/pointwise.csv',index=False)
    monitor.to_csv(ROOT/'results/monitoring.csv',index=False)
    capacity.to_csv(ROOT/'results/capacity.csv',index=False)
    clustered_summary(point,['phase','null','scenario','history_size','method','window']).to_csv(ROOT/'results/pointwise_summary.csv',index=False)
    clustered_summary(monitor,['phase','null','scenario','history_size','method','horizon']).to_csv(ROOT/'results/monitoring_summary.csv',index=False)
    manifest = dict(config=CONFIG,elapsed_seconds=time.monotonic()-start,
                    peak_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,
                    python=platform.python_version(),numpy=np.__version__,scipy=scipy.__version__,pandas=pd.__version__,
                    cpu_affinity=sorted(os.sched_getaffinity(0)))
    files=[ROOT/'protocol.md',ROOT/'sources.md',ROOT/'experiment.py',PARENT/'experiment.py']
    files+=sorted((ROOT/'inputs').glob('*'))+sorted((ROOT/'results').glob('*.csv'))
    manifest['sha256']={str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else '../'+p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    (ROOT/'results/manifest.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps({key:manifest[key] for key in ['elapsed_seconds','peak_rss_mib','cpu_affinity']},indent=2),flush=True)


if __name__=='__main__':
    main()
