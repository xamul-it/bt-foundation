"""
BTRunner — chiama btmain.py via subprocess, legge risultati.

Generico: funziona con qualsiasi strategia registrata in bt-core.
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from paths import BT_CORE, ENV_DIR

LOG = logging.getLogger(__name__)

BTMAIN = BT_CORE / "btmain.py"
VENV_PYTHON = BT_CORE / ".venv" / "bin" / "python"


class BTRunner:
    """
    Wrapper per eseguire btmain.py e leggere i risultati.

    Uso
    ---
    runner = BTRunner()
    metrics, trades = runner.run(
        strategy="intraday.HMA",
        tickers=["NVDA"],
        fromdate="2025-01-01",
        todate="2025-01-31",
        stratargs="period=16 sl_pct=0.005",
    )
    """

    def __init__(self, bt_core: Optional[Path] = None, env_file: Optional[str] = None):
        self.bt_core = Path(bt_core) if bt_core else BT_CORE
        self.env_file = env_file or str(ENV_DIR / "pa2")
        self.python = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

    def run(
        self,
        strategy: str,
        tickers: List[str],
        fromdate: str,
        todate: str,
        stratargs: str = "",
        extra_args: Optional[List[str]] = None,
        timeout: int = 300,
    ) -> tuple[Dict, pd.DataFrame]:
        """
        Esegue btmain.py e ritorna (metrics_dict, trades_df).

        Returns
        -------
        metrics : dict con chiavi da results.json
        trades : DataFrame con i trade da trades.json (se disponibile)
        """
        cmd = [
            self.python, str(BTMAIN),
            "--strat", strategy,
            "--ticker", ",".join(tickers),
            "--fromdate", fromdate,
            "--todate", todate,
        ]
        if stratargs:
            cmd += ["--stratargs", stratargs]
        if extra_args:
            cmd += extra_args

        LOG.info(f"Esecuzione: {' '.join(cmd)}")

        env = self._build_env()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(self.bt_core),
            env=env,
            timeout=timeout,
        )

        if result.returncode != 0:
            LOG.error(f"btmain.py fallito (rc={result.returncode}):\n{result.stderr}")
            raise RuntimeError(f"btmain.py exit {result.returncode}: {result.stderr[-2000:]}")

        # Leggi output da out/{Strategy}/
        strat_name = strategy.split(".")[-1]
        out_dir = self.bt_core / "out" / strat_name

        metrics = self._load_results(out_dir)
        trades = self._load_trades(out_dir)

        return metrics, trades

    # ------------------------------------------------------------------

    def _build_env(self) -> Dict:
        import os
        env = os.environ.copy()
        env_path = Path(self.env_file)
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
        return env

    def _load_results(self, out_dir: Path) -> Dict:
        # Cerca il results.json più recente
        candidates = sorted(out_dir.glob("results*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            LOG.warning(f"Nessun results.json in {out_dir}")
            return {}
        with open(candidates[0]) as f:
            return json.load(f)

    def _load_trades(self, out_dir: Path) -> pd.DataFrame:
        # Cerca trades.json o transactions.csv
        trades_json = out_dir / "trades.json"
        if trades_json.exists():
            with open(trades_json) as f:
                data = json.load(f)
            return pd.DataFrame(data) if data else pd.DataFrame()

        trades_csv = out_dir / "transactions.csv"
        if trades_csv.exists():
            return pd.read_csv(trades_csv, parse_dates=True)

        LOG.warning(f"Nessun file trades in {out_dir}")
        return pd.DataFrame()
