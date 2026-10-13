"""Bounded local subprocess sweep with a shared four-process ceiling."""
import argparse
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
STUDY = ROOT / 'out/overnight_ah_capital_study_20260909'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--capitals', default='25000,50000,100000,250000,500000,1000000,3000000,10000000,30000000')
    parser.add_argument('--strategies', default='legacy,composite')
    parser.add_argument('--start', default='2000-01-01')
    parser.add_argument('--prefix', default='capital_band20')
    parser.add_argument('--workers', type=int, default=2)
    args = parser.parse_args()
    queue = []
    for capital in args.capitals.split(','):
        for strategy in args.strategies.split(','):
            name = f'{args.prefix}_{strategy}_{capital}'
            if (STUDY / 'cases' / name / 'complete.json').exists():
                continue
            command = [str(ROOT / 'bt-core/.venv/bin/python'), str(Path(__file__).with_name('run_case.py')),
                       '--name', name, '--cash', capital, '--strategy', strategy, '--start', args.start]
            queue.append((name, command))
    active, failed = [], []
    while queue or active:
        for item in active[:]:
            name, proc, log = item
            if proc.poll() is not None:
                log.close()
                active.remove(item)
                if proc.returncode:
                    failed.append(name)
                print(json.dumps(dict(event='finished', name=name, code=proc.returncode)), flush=True)
        mem = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        available_gb = int(mem['MemAvailable'].split()[0]) / 1024**2
        processes = subprocess.check_output(['ps', '-eo', 'args'], text=True).splitlines()
        global_active = sum(str(Path(__file__).with_name('run_case.py')) in line
                            and ' --name ' in line for line in processes)
        if queue and len(active) < min(args.workers, 4) and global_active < 4 and available_gb > 8:
            name, command = queue.pop(0)
            log = (STUDY / f'{name}.log').open('w')
            proc = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
            active.append((name, proc, log))
            print(json.dumps(dict(event='started', name=name, pid=proc.pid, available_gb=available_gb)), flush=True)
        time.sleep(2)
    (STUDY / f'{args.prefix}_batch.json').write_text(json.dumps(dict(failed=failed)))
    if failed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
