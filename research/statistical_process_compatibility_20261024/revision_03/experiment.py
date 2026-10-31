"""Held-out normalization; preserves all earlier candidate artifacts."""
from __future__ import annotations
import importlib.util
import hashlib
import json
import time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
PARENT=ROOT.parent
spec=importlib.util.spec_from_file_location('revision_two_framework',PARENT/'revision_02/experiment.py')
prior=importlib.util.module_from_spec(spec)
spec.loader.exec_module(prior)
base=prior.base
np,pd=base.np,base.pd


class HeldOutReference(prior.MemoryReference):
    def __init__(self,history):
        fit=history[:500]
        normalize=history[500:1000]
        self.probabilities=[]
        for order in (1,2,3):
            counts=np.bincount(base.word_codes(fit[None,:],order).ravel(),minlength=6**order)+.5
            self.probabilities.append(counts/counts.sum())
        self.center,self.scale={},{}
        for size in base.WINDOWS:
            raw=self.features(np.lib.stride_tricks.sliding_window_view(normalize,size))
            self.center[size]=raw.mean(axis=0)
            self.scale[size]=np.maximum(raw.std(axis=0,ddof=1),.25)
        calibration=history[1100:]
        self.calibration_paths=np.stack([calibration[i:i+20] for i in range(0,len(calibration)-19,20)])
        self.calibration_scores=self.scores(self.calibration_paths)
        self.horizon_references={}
        for horizon in (20,100):
            length=horizon+19
            panels=np.stack([calibration[i:i+length] for i in range(0,len(calibration)-length+1,horizon+40)])
            self.horizon_references[horizon]=np.sort(self.timeline_scores(panels).max(axis=1))


def run_phase(phase,seed):
    rng=np.random.default_rng(seed)
    points,monitor,capacity=[],[],[]
    sizes=[2480,4960,9200] if phase=='development' else [4960]
    cases=base.CASES if phase=='development' else {
        **{null:([null,'sticky_06'] if null=='iid' else [null,'iid']) for null in base.CASES},
        'cycle_forward':['cycle_forward','iid','cycle_reverse'],
        'lag5':['lag5','iid'],'lag7':['lag7','iid'],'lag8':['lag8','iid'],
    }
    episodes=1000 if phase=='development' else 2000
    for size in sizes:
        for null,scenarios in cases.items():
            for replication in range(20):
                history=base.generate(null,1,size,rng)[0]
                held=HeldOutReference(history)
                memory=prior.MemoryReference(history)
                raw=base.DirectReference(history)
                legacy=base.legacy_reference(history)
                payload={'history':history}
                for horizon,bank in held.horizon_references.items():
                    capacity.append(dict(phase=phase,null=null,history_size=size,replication=replication,
                                         horizon=horizon,blocks=len(bank),minimum_p=1/(len(bank)+1),
                                         estimable=1/(len(bank)+1)<=.05))
                for scenario in scenarios:
                    paths=base.generate(scenario,episodes,20,rng)
                    payload['point_'+scenario]=paths
                    for label,reference in [('C5_heldout_score',held),('C3_memory_score',memory),('C1_direct_score',raw)]:
                        for window,pvalues in reference.pvalues(reference.scores(paths)).items():
                            if label!='C5_heldout_score' and window not in ('20','joint'):
                                continue
                            points.append(base.point_record(phase,null,scenario,size,replication,label,window,
                                                            pvalues,len(reference.calibration_paths)))
                    for window,pvalues in [('20',legacy.pvalues(paths)['ensemble5_blocks']),
                                           ('joint',legacy.global_pvalue(paths))]:
                        points.append(base.point_record(phase,null,scenario,size,replication,'C0_double_rank',window,
                                                        pvalues,len(legacy.global_reference)))
                    if size==4960:
                        context=base.generate(null,100,19,rng)
                        future=base.generate(scenario,100,100,rng,context=context)
                        timeline=np.concatenate([context,future],axis=1)
                        payload['monitor_'+scenario]=timeline
                        records=base.monitor_records(phase,null,scenario,size,replication,held,timeline)
                        for row in records:
                            row['method']={'C1_daily_pointwise':'C5_daily_pointwise','C2_period_maximum':'C6_period_maximum'}[row['method']]
                        monitor.extend(records)
                np.savez_compressed(ROOT/'inputs'/f'{phase}_{size}_{null}_{replication:02d}.npz',**payload)
            print(f'{phase}: N={size}, null={null} completed',flush=True)
            pd.DataFrame(points).to_csv(ROOT/'results'/f'{phase}_pointwise.csv',index=False)
            pd.DataFrame(monitor).to_csv(ROOT/'results'/f'{phase}_monitoring.csv',index=False)
            pd.DataFrame(capacity).to_csv(ROOT/'results'/f'{phase}_capacity.csv',index=False)
    return pd.DataFrame(points),pd.DataFrame(monitor),pd.DataFrame(capacity)


def main():
    start=time.monotonic()
    points,monitor,capacity=[],[],[]
    for phase,seed in [('development',20261030),('confirmation',20261031)]:
        a,b,c=run_phase(phase,seed)
        points.append(a);monitor.append(b);capacity.append(c)
    points,monitor,capacity=pd.concat(points),pd.concat(monitor),pd.concat(capacity)
    points.to_csv(ROOT/'results/pointwise.csv',index=False)
    monitor.to_csv(ROOT/'results/monitoring.csv',index=False)
    capacity.to_csv(ROOT/'results/capacity.csv',index=False)
    base.clustered_summary(points,['phase','null','scenario','history_size','method','window']).to_csv(ROOT/'results/pointwise_summary.csv',index=False)
    base.clustered_summary(monitor,['phase','null','scenario','history_size','method','horizon']).to_csv(ROOT/'results/monitoring_summary.csv',index=False)
    manifest=dict(config={**base.CONFIG,'development_seed':20261030,'confirmation_seed':20261031,
                          'candidate':'C5/C6','fit_count':500,'normalization_count':500,
                          'explicit_lags':list(range(1,11)),'confirmation_extra_nulls':['lag5','lag7','lag8']},
                  elapsed_seconds=time.monotonic()-start,
                  peak_rss_mib=base.resource.getrusage(base.resource.RUSAGE_SELF).ru_maxrss/1024,
                  python=base.platform.python_version(),numpy=np.__version__,scipy=base.scipy.__version__,pandas=pd.__version__,
                  cpu_affinity=sorted(base.os.sched_getaffinity(0)))
    files=[ROOT/'protocol.md',ROOT/'sources.md',ROOT/'experiment.py',PARENT/'revision_02/experiment.py',PARENT/'revision_01/experiment.py',PARENT/'experiment.py']
    files+=sorted((ROOT/'inputs').glob('*'))+sorted((ROOT/'results').glob('*.csv'))
    manifest['sha256']={base.os.path.relpath(p,ROOT):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    (ROOT/'results/manifest.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps({k:manifest[k] for k in ['elapsed_seconds','peak_rss_mib','cpu_affinity']},indent=2),flush=True)


if __name__=='__main__':
    main()
