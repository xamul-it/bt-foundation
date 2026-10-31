"""Declared follow-up sensitivity; not part of the primary preregistration."""
import json
import hashlib
import resource
import time
import numpy as np
import pandas as pd
from experiment import ROOT, make_reference, rejection_record, simulate


def main():
    start = time.monotonic()
    rng = np.random.default_rng(20261025)
    rows = []
    for blocks in [16, 47, 100]:
        for replication in range(20):
            reference, _, stream = make_reference('sticky_06', rng, training_size=1000,
                                                   blocks=blocks, spacing=40, gap=100)
            for scenario in ['sticky_06', 'iid', 'sticky_09']:
                paths = simulate(scenario, 2000, 20, rng)
                for method, window, pvalues in [
                    ('ensemble5_blocks', 20, reference.pvalues(paths)['ensemble5_blocks']),
                    ('global_ensemble5', 0, reference.global_pvalue(paths)),
                ]:
                    rows.append(rejection_record('sticky_06', scenario, window, method, pvalues,
                                                 blocks=blocks, observations=len(stream),
                                                 replication=replication, minimum_p=1/(blocks+1)))
        print('Completed finite-history B=', blocks, flush=True)
    data = pd.DataFrame(rows)
    data.to_csv(ROOT / 'results/finite_history.csv', index=False)
    assert (data[data.blocks == 16].rate == 0).all()
    summary = data.groupby(['blocks','observations','scenario','method']).rate.agg(['min','median','max'])
    summary.to_csv(ROOT / 'results/finite_history_summary.csv')
    manifest = dict(seed=20261025, baseline='sticky_06', replications=20,
                    episodes_per_scenario_per_replication=2000, blocks=[16,47,100],
                    elapsed_seconds=time.monotonic()-start,
                    peak_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024)
    files = ['finite_history.py','finite_history_extension.md','results/finite_history.csv','results/finite_history_summary.csv']
    manifest['sha256'] = {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in files}
    (ROOT/'results/finite_history_manifest.json').write_text(json.dumps(manifest, indent=2))
    print(summary.round(4).to_string())


if __name__ == '__main__':
    main()
