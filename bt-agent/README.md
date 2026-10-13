# bt-agent

Agente di prototipazione e ricerca per strategie intraday.
Opera nel workspace `backtrader/` (il contenitore), usa il venv di `bt-core`.
Usa `claude --print` (subscription locale, nessuna API key).

## Posizione nel workspace

```
backtrader/              ← workspace root (contenitore)
  bt-agent/              ← questo modulo
  bt-core/               ← engine produzione (strategie pronte per paper/live)
  bin/                   ← script generati dall'agente
  data/m/alpaca/         ← dati OHLCV minuto
  out/reports/           ← report HTML generati
```

## Setup (una tantum)

```bash
# Usa il venv di bt-core, già ha tutte le dipendenze
source /home/htpc/backtrader/bt-core/.venv/bin/activate
```

## Verifica installazione

```bash
cd /home/htpc/backtrader/bt-agent
source ../bt-core/.venv/bin/activate
python -m pytest tests/ -q
# Atteso: 37 passed, 1 skipped
```

## Uso nei tuoi script (in bin/)

```python
#!/usr/bin/env python3
import sys
sys.path.insert(0, '/home/htpc/backtrader/bt-agent')
sys.path.insert(0, '/home/htpc/backtrader/bt-core')

from agent.agent import BTPandasAgent
from strategies.hma import HMAStrategy   # o altra strategia

agent = BTPandasAgent()

# Report completo: backtest → MC → stabilità → HTML
path = agent.full_report(
    title="HMA NVDA Q1-2025",
    output_path="/home/htpc/backtrader/out/reports/hma_nvda_q1.html",
    symbol="NVDA",
    strategy_cls=HMAStrategy,
    strategy_params={"period": 16},
    backtest_params={"sl_pct": 0.005},
    fromdate="2025-01-01",
    todate="2025-03-31",
    reproduce_code=open(__file__).read(),
)
```

## Aggiungere una nuova strategia

1. Crea `strategies/mia_strategia.py` che estende `BaseStrategy`
2. Implementa `generate_signals(df) -> pd.Series` (1=long, -1=short, 0=nessuno)

```python
from strategies.base import BaseStrategy
import pandas as pd

class MiaStrategia(BaseStrategy):
    def __init__(self, periodo=20):
        self.periodo = periodo

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        # ... logica segnali ...
        return pd.Series(0, index=df.index)
```

## Struttura moduli

```
bt-agent/
  paths.py           ← costanti path (WORKSPACE_ROOT, BT_CORE, BIN_DIR, ...)
  indicators/        ← hma.py, atr.py (validati vs BT, diff < 1e-8)
  engine/            ← backtest.py, metrics.py
  bt_bridge/         ← extractor.py, runner.py, comparator.py
  sweep/             ← grid.py, montecarlo.py
  strategies/        ← base.py + strategie concrete
  analysis/          ← stability.py, coherence.py
  reporting/         ← html_report.py (self-contained, no CDN)
  agent/             ← agent.py, tools.py, prompts.py
  tests/             ← 37 test automatici
```

## Vincoli live/paper (nel prompt dell'agente)

- **Solo ordini LIMIT** (no market)
- **Close + Revert = 2 operazioni separate** (chiudi → fill → apri)
- **No ordini sovrapposti** (tracciare `self.order` in BT)
- **TTL ordini** (cancellare pending prima delle 15:55)
- **Fill lag T+1** (in live il fill è sull'open della barra successiva)
