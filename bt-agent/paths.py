"""
Costanti di path per bt-agent.

WORKSPACE_ROOT = /home/htpc/backtrader/   (il contenitore)
AGENT_ROOT     = /home/htpc/backtrader/bt-agent/
BT_CORE        = /home/htpc/backtrader/bt-core/
BIN_DIR        = /home/htpc/backtrader/bin/
DATA_DIR       = /home/htpc/backtrader/data/m/alpaca/
"""
from pathlib import Path

AGENT_ROOT     = Path(__file__).parent.resolve()
WORKSPACE_ROOT = AGENT_ROOT.parent          # backtrader/
BT_CORE        = WORKSPACE_ROOT / "bt-core"
BIN_DIR        = WORKSPACE_ROOT / "bin"
DATA_DIR       = WORKSPACE_ROOT / "config-common" / "data" / "minutes" / "alpaca"
DATA_DIR_DAILY = WORKSPACE_ROOT / "config-common" / "data" / "daily" / "alpaca"
OUT_DIR        = WORKSPACE_ROOT / "out"
ENV_DIR        = WORKSPACE_ROOT / "env"
CACHE_DIR      = AGENT_ROOT / "indicator_cache"
REPORTS_DIR    = OUT_DIR / "reports"
