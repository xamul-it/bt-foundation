"""Second research revision. Reuses R01 framework, preserves its files/evidence."""
from __future__ import annotations
import importlib.util
import hashlib
import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FIRST = ROOT.parent/'revision_01'
spec=importlib.util.spec_from_file_location('revision_one_framework',FIRST/'experiment.py')
base=importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
np,pd=base.np,base.pd
RawReference=base.DirectReference


class MemoryReference(RawReference):
    def features(self,paths):
        raw=super().features(paths)
        repeats=[np.sum(paths[:,lag:]==paths[:,:-lag],axis=1)
                 for lag in range(1,min(10,paths.shape[1]-1)+1)]
        return np.column_stack([raw,*repeats])


def run_phase(phase,seed):
    rng=np.random.default_rng(seed)
    point,monitor,capacity=[],[],[]
    sizes=[2480,4960,9200] if phase=='development' else [4960]
    cases=base.CASES if phase=='development' else {
        **{null:([null,'sticky_06'] if null=='iid' else [null,'iid']) for null in base.CASES},
        'cycle_forward':['cycle_forward','iid','cycle_reverse'],
        'lag5':['lag5','iid'], 'lag7':['lag7','iid'],
    }
    episodes=1000 if phase=='development' else 2000
    for size in sizes:
        for null,scenarios in cases.items():
            for replication in range(20):
                history=base.generate(null,1,size,rng)[0]
                memory=MemoryReference(history)
                raw=RawReference(history)
                legacy=base.legacy_reference(history)
                payload={'history':history}
                for horizon,bank in memory.horizon_references.items():
                    capacity.append(dict(phase=phase,null=null,history_size=size,replication=replication,
                                         horizon=horizon,blocks=len(bank),minimum_p=1/(len(bank)+1),
                                         estimable=1/(len(bank)+1)<=.05))
                for scenario in scenarios:
                    paths=base.generate(scenario,episodes,20,rng)
                    payload['point_'+scenario]=paths
                    for label,reference in [('C3_memory_score',memory),('C1_direct_score',raw)]:
                        scores=reference.scores(paths)
                        for window,pvalues in reference.pvalues(scores).items():
                            if label=='C1_direct_score' and window not in ('20','joint'):
                                continue
                            point.append(base.point_record(phase,null,scenario,size,replication,label,window,
                                                           pvalues,len(reference.calibration_paths)))
                    for window,pvalues in [('20',legacy.pvalues(paths)['ensemble5_blocks']),
                                           ('joint',legacy.global_pvalue(paths))]:
                        point.append(base.point_record(phase,null,scenario,size,replication,'C0_double_rank',window,
                                                       pvalues,len(legacy.global_reference)))
                    if size==4960:
                        context=base.generate(null,100,19,rng)
                        future=base.generate(scenario,100,100,rng,context=context)
                        timeline=np.concatenate([context,future],axis=1)
                        payload['monitor_'+scenario]=timeline
                        records=base.monitor_records(phase,null,scenario,size,replication,memory,timeline)
                        for row in records:
                            row['method']={'C1_daily_pointwise':'C3_daily_pointwise',
                                           'C2_period_maximum':'C4_period_maximum'}[row['method']]
                        monitor.extend(records)
                np.savez_compressed(ROOT/'inputs'/f'{phase}_{size}_{null}_{replication:02d}.npz',**payload)
            print(f'{phase}: N={size}, null={null} completed',flush=True)
            pd.DataFrame(point).to_csv(ROOT/'results'/f'{phase}_pointwise.csv',index=False)
            pd.DataFrame(monitor).to_csv(ROOT/'results'/f'{phase}_monitoring.csv',index=False)
            pd.DataFrame(capacity).to_csv(ROOT/'results'/f'{phase}_capacity.csv',index=False)
    return pd.DataFrame(point),pd.DataFrame(monitor),pd.DataFrame(capacity)


def main():
    start=time.monotonic()
    rows,monitor,capacity=[],[],[]
    for phase,seed in [('development',20261028),('confirmation',20261029)]:
        a,b,c=run_phase(phase,seed)
        rows.append(a);monitor.append(b);capacity.append(c)
    rows,monitor,capacity=pd.concat(rows),pd.concat(monitor),pd.concat(capacity)
    rows.to_csv(ROOT/'results/pointwise.csv',index=False)
    monitor.to_csv(ROOT/'results/monitoring.csv',index=False)
    capacity.to_csv(ROOT/'results/capacity.csv',index=False)
    base.clustered_summary(rows,['phase','null','scenario','history_size','method','window']).to_csv(ROOT/'results/pointwise_summary.csv',index=False)
    base.clustered_summary(monitor,['phase','null','scenario','history_size','method','horizon']).to_csv(ROOT/'results/monitoring_summary.csv',index=False)
    manifest=dict(config={**base.CONFIG,'development_seed':20261028,'confirmation_seed':20261029,
                          'candidate':'C3/C4','explicit_lags':list(range(1,11)),
                          'confirmation_extra_nulls':['lag5','lag7']},
                  elapsed_seconds=time.monotonic()-start,
                  peak_rss_mib=base.resource.getrusage(base.resource.RUSAGE_SELF).ru_maxrss/1024,
                  python=base.platform.python_version(),numpy=np.__version__,scipy=base.scipy.__version__,pandas=pd.__version__,
                  cpu_affinity=sorted(base.os.sched_getaffinity(0)))
    files=[ROOT/'protocol.md',ROOT/'sources.md',ROOT/'experiment.py',FIRST/'experiment.py',ROOT.parent/'experiment.py']
    files+=sorted((ROOT/'inputs').glob('*'))+sorted((ROOT/'results').glob('*.csv'))
    manifest['sha256']={base.os.path.relpath(p,ROOT):
                        hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    (ROOT/'results/manifest.json').write_text(json.dumps(manifest,indent=2))
    print(json.dumps({k:manifest[k] for k in ['elapsed_seconds','peak_rss_mib','cpu_affinity']},indent=2),flush=True)


if __name__=='__main__':
    main()
