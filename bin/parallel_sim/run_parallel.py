#!/usr/bin/env python3
"""
Parallel Paper + Simulation Runner
===================================
Lancia simultaneamente:
  1. Paper trading  → Alpaca broker (ordini reali su paper account)
  2. Simulazione    → Backtrader broker (ordini simulati, stesso flusso ZMQ)

La differenza tra i fill è lo slippage reale, analizzabile a fine giornata
con eod_analysis.py.

Usage:
    python bin/parallel_sim/run_parallel.py \\
        --strat intraday.HMADynamic \\
        --ticker "HMA_top9.json" \\
        --stratargs "period=16 inverted=True" \\
        --timeframe minutes \\
        [--cash 100000] [--commission none] [--debug]

Output:
    out/<module>/<Strategy>/paper/  ← paper trading
    out/<module>/<Strategy>/sim/    ← simulazione
"""

import subprocess
import sys
import os
import signal
import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('parallel_sim')

BACK_DIR = Path(__file__).parent.parent.parent  # repo root
BT_CORE_DIR = BACK_DIR / 'bt-core'
BTMAIN = BT_CORE_DIR / 'btmain.py'
_VENV_PYTHON = BT_CORE_DIR / '.venv' / 'bin' / 'python'
PYTHON = str(_VENV_PYTHON) if _VENV_PYTHON.exists() else sys.executable

PAPER_ID = 'paper'
SIM_ID = 'sim'
STOP_GRACE_SECONDS = 30


def parse_args():
    p = argparse.ArgumentParser(
        description='Lancia paper trading + simulazione in parallelo',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('--strat', required=True, help='Strategia: module.ClassName')
    p.add_argument('--ticker', required=True, help='JSON file o lista ticker separata da virgola')
    p.add_argument('--stratargs', default=None, help='Parametri strategia (es: "period=16 inverted=True")')
    p.add_argument('--timeframe', default='minutes', help='Timeframe: minutes o daily (default: minutes)')
    p.add_argument('--cash', default='100000', help='Capitale iniziale (default: 100000)')
    p.add_argument('--commission', default='none', help='Schema commissioni (default: none)')
    p.add_argument('--paper-id', default=PAPER_ID, help='ID subfolder paper (default: paper)')
    p.add_argument('--sim-id', default=SIM_ID, help='ID subfolder sim (default: sim)')
    p.add_argument('--no-sim', action='store_true', help='Lancia solo paper (senza simulazione)')
    p.add_argument('--debug', action='store_true', help='Debug logging')
    p.add_argument('--audit-full', action='store_true', help='Audit completo')
    p.add_argument('--log-trades', action='store_true', help='Log trades dettagliato')
    return p.parse_args()


def build_common_args(args):
    """Args condivisi tra paper e sim."""
    cmd = [
        str(BTMAIN),
        '--strat', args.strat,
        '--ticker', args.ticker,
        '--cash', args.cash,
        '--commission', args.commission,
        '--provider', 'alpaca',
        '--alpaca-mode', 'proxy',
        '--timeframe', args.timeframe,
    ]
    if args.stratargs:
        cmd += ['--stratargs', args.stratargs]
    if args.debug:
        cmd.append('--debug')
    if args.audit_full:
        cmd.append('--audit-full')
    if args.log_trades:
        cmd.append('--log_trades')
    return cmd


def spawn(label, cmd_args, log_file):
    """Lancia btmain.py come subprocess, redirect stdout/stderr su log file."""
    full_cmd = [PYTHON] + cmd_args
    logger.info(f'[{label}] CMD: {" ".join(str(x) for x in full_cmd)}')
    f = open(log_file, 'w', buffering=1)
    proc = subprocess.Popen(
        full_cmd,
        stdout=f,
        stderr=subprocess.STDOUT,
        cwd=str(BT_CORE_DIR),
        preexec_fn=os.setsid  # nuovo process group per kill pulito
    )
    return proc, f


def tail_log(log_file, label, lines=3):
    """Mostra ultime righe di un log file."""
    try:
        with open(log_file) as f:
            content = f.readlines()
        last = content[-lines:] if len(content) >= lines else content
        for line in last:
            logger.info(f'  [{label}] {line.rstrip()}')
    except Exception:
        pass


def graceful_stop(processes, grace_seconds=STOP_GRACE_SECONDS):
    """Try graceful stop first (SIGINT), then force SIGTERM on leftovers."""
    if not processes:
        return

    logger.info(f'Shutdown richiesto: invio SIGINT ai child (grace={grace_seconds}s)')
    for label, proc in processes:
        try:
            if proc.poll() is None:
                os.killpg(os.getpgid(proc.pid), signal.SIGINT)
                logger.info(f'[{label}] SIGINT inviato')
        except ProcessLookupError:
            pass

    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        if all(proc.poll() is not None for _, proc in processes):
            return
        time.sleep(0.5)

    logger.warning('Grace period scaduto: invio SIGTERM ai processi ancora attivi')
    for label, proc in processes:
        try:
            if proc.poll() is None:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                logger.warning(f'[{label}] SIGTERM inviato')
        except ProcessLookupError:
            pass


def main():
    args = parse_args()
    logger.info(f'Python interpreter: {PYTHON}')

    # Dir log per questa sessione
    log_dir = BACK_DIR / 'logs'
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    common = build_common_args(args)

    paper_args = common + ['--mode', 'paper', '--live', '--id', args.paper_id]
    sim_args   = common + ['--mode', 'backtest', '--live', '--id', args.sim_id]

    processes = []
    log_files_handles = []
    log_paths = {}
    stop_requested = {'value': False}

    def _handle_stop(signum, _frame):
        stop_requested['value'] = True
        logger.info(f'Segnale ricevuto: {signal.Signals(signum).name}')
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    try:
        # --- Lancia Paper ---
        paper_log = log_dir / f'parallel_paper_{ts}.log'
        paper_proc, paper_f = spawn('PAPER', paper_args, paper_log)
        processes.append(('PAPER', paper_proc))
        log_files_handles.append(paper_f)
        log_paths['PAPER'] = paper_log
        logger.info(f'[PAPER] PID={paper_proc.pid}, log={paper_log}')

        # --- Lancia Sim ---
        if not args.no_sim:
            sim_log = log_dir / f'parallel_sim_{ts}.log'
            sim_proc, sim_f = spawn('SIM', sim_args, sim_log)
            processes.append(('SIM', sim_proc))
            log_files_handles.append(sim_f)
            log_paths['SIM'] = sim_log
            logger.info(f'[SIM]   PID={sim_proc.pid}, log={sim_log}')

        logger.info('Entrambi i processi avviati. Ctrl+C per fermare.')
        logger.info('')

        # Attendi completamento
        for label, proc in processes:
            ret = proc.wait()
            logger.info(f'[{label}] Uscito con codice {ret}')
            if ret != 0:
                logger.warning(f'[{label}] Errore! Ultime righe log:')
                tail_log(log_paths[label], label)

    except KeyboardInterrupt:
        if stop_requested['value']:
            logger.info('Stop service ricevuto — arresto graceful dei processi...')
        else:
            logger.info('Ctrl+C ricevuto — arresto graceful dei processi...')
        graceful_stop(processes)

    finally:
        for f in log_files_handles:
            try:
                f.close()
            except Exception:
                pass

    # Deriva output dirs per il reminder EOD
    strat_module = args.strat.split('.')[0] if '.' in args.strat else args.strat
    strat_class  = args.strat.split('.')[-1]
    paper_out = BACK_DIR / 'out' / strat_module / strat_class / args.paper_id
    sim_out   = BACK_DIR / 'out' / strat_module / strat_class / args.sim_id

    logger.info('')
    logger.info('=== EOD Analysis ===')
    logger.info(f'python bin/parallel_sim/eod_analysis.py \\')
    logger.info(f'  --paper {paper_out} \\')
    logger.info(f'  --sim   {sim_out}')


if __name__ == '__main__':
    main()
