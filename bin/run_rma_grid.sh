#!/usr/bin/env bash
# Runner batch per griglia RMAStrategy (period/tstat/pval) su storico.
# Esegue btmain in loop e salva log run-by-run in /tmp.

set -euo pipefail

PYTHON="/home/htpc/backtrader/venv/bin/python"
BTMAIN="/home/htpc/backtrader/backtrader/btmain.py"
REPO_ROOT="/home/htpc/backtrader/backtrader"

TICKER_LIST="NASDAQ_100_US.json"
FROMDATE="2021-01-01"
TODATE="2025-09-14"
PROVIDER="yahoo"
TIMEFRAME="daily"

period_fast_vals=(40 60 80)
pval_entry_vals=(0.15)
tstat_entry_vals=(1.0 0.5)

for pf in "${period_fast_vals[@]}"; do
  for pe in "${pval_entry_vals[@]}"; do
    for ts in "${tstat_entry_vals[@]}"; do
      id="grid_pf${pf}_pe${pe}_ts${ts}"
      args="period_fast=${pf} pval_entry_max=${pe} tstat_entry_min=${ts}"
      echo "Running $id with $args"
      "$PYTHON" - <<PY >"/tmp/${id}.log" 2>&1
import runpy, sys, logging
from pathlib import Path

repo = Path("$REPO_ROOT")
btmain = repo / "btmain.py"

# Reorder sys.path to prefer site-packages backtrader over local package name.
repo_str = str(repo)
site_pkgs = next((p for p in sys.path if p.endswith("site-packages")), None)
sys.path = [p for p in sys.path if p != repo_str]
if site_pkgs and site_pkgs in sys.path:
    sys.path.remove(site_pkgs)
    sys.path.insert(0, site_pkgs)
sys.path.append(repo_str)

_orig_basic_config = logging.basicConfig
def _basic_config_wrapper(*args, **kwargs):
    kwargs["level"] = logging.WARNING
    return _orig_basic_config(*args, **kwargs)
logging.basicConfig = _basic_config_wrapper

sys.argv = [
    str(btmain),
    "--ticker", "$TICKER_LIST",
    "--strat", "weekly.RMAStrategy",
    "--stratargs", "$args",
    "--fromdate", "$FROMDATE",
    "--todate", "$TODATE",
    "--timeframe", "$TIMEFRAME",
    "--provider", "$PROVIDER",
    "--mode", "backtest",
    "--id", "$id",
]

runpy.run_path(str(btmain), run_name="__main__")
PY
    done
  done
done
