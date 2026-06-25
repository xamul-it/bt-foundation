# CLAUDE.md — Workspace backtrader

Questo file copre l'intero workspace `/home/htpc/backtrader/` e si applica a tutte le sessioni Claude Code avviate da questa directory.

## Struttura workspace

```
backtrader/
  bt-core/          ← repository principale (Backtrader engine, strategie, btmain.py)
  bin/              ← script di analisi, sweep, utility (NON in bt-core)
    HMA/            ← script specifici per strategia intraday.HMA
    {strategia}/    ← una sottodirectory per strategia
  data/             ← dati OHLCV
    m/alpaca/       ← minute data Alpaca
    d/alpaca/       ← daily data Alpaca
  out/              ← output backtest (symlink da bt-core/out → ../out)
  env/              ← file credenziali (pa1, pa2, ...)
  config-common/    ← cache calendario Alpaca, dati condivisi
```

## Regole obbligatorie

### 1. Posizione degli script
- Script Python e shell di analisi/sweep/utility → `backtrader/bin/{nome_strategia}/`
- NON creare script in `bt-core/bin/` salvo utilità strettamente legate all'engine
- Esempio corretto: `backtrader/bt-strategy-test/HMA/sweep_sl_tp.py`
- Esempio sbagliato: `backtrader/bt-core/bin/HMA/sweep_sl_tp.py`

### 2. Simulazioni Pandas devono replicare Backtrader

Quando crei una simulazione Pandas per pre-screening (Monte Carlo, sweep veloce), devi
replicare **fedelmente** il comportamento della strategia Backtrader corrispondente.
Parametri critici da verificare SEMPRE prima di implementare:

- **`exitbar`**: la strategia chiude la posizione dopo N barre se nessun segnale → simula questo
- **`minutes_before_close`**: blocca nuovi ingressi N minuti prima della chiusura → simula questo
- **`sl_pct` / `tp_pct`**: SL/TP su high/low della barra (non sul close) → già corretto
- **`inverted`**: segnale contrarian vs direzionale
- **`use_calendar`**: trading solo nei giorni di mercato

Se la simulazione Pandas diverge dal Backtrader su un parametro chiave (es. exitbar), i
risultati NON sono trasferibili. Verificare sempre confrontando trade count e PNL su un
campione comune prima di trarre conclusioni.

### 2b. Verifica obbligatoria: il segnale è indipendente dall'esecuzione degli ordini?

**Prima di qualsiasi tuning o sweep**, analizzare la strategia BT (`next()`) e rispondere:

**Domanda chiave**: ogni variabile usata per decidere se entrare/uscire dipende da
`pos.size`, `order.status` o da qualsiasi stato derivato dall'esecuzione degli ordini?

Se **sì** → la strategia diverge strutturalmente tra backtest e paper/live:
- In backtest i fill sono deterministici (Limit al close esegue quasi sempre).
- In paper/live un Limit può non eseguire (gap, liquidità). Se il segnale dipende
  da `pos.size`, un mancato fill cambia la decisione al bar successivo → backtest
  e paper escono di sincronia, prezzi e PNL non sono più confrontabili.

**Scenario di test mentale obbligatorio**: *"Un Limit entry viene sottomesso ma NON
eseguito (gap contro). Al bar successivo, cosa decide la strategia?"*
- Ri-entra sullo stesso segnale? → **BUG** se la Pandas sim non fa lo stesso.
- Aspetta un nuovo segnale? → corretto, ma verifica che la Pandas sim sia uguale.

**Confronto strutturale BT vs Pandas** (da fare prima dello sweep):
1. Isola il codice di generazione segnale in `next()` — solo le righe che
   producono `long_edge` / `short_edge`.
2. Verifica che il segnale dipenda SOLO dall'indicatore tecnico (HMA, ATR, ecc.),
   mai da `self.direction`, `self.pos`, `self.entry_prices` o variabili allineate
   alla posizione aperta.
3. Confronta con il codice equivalente in `sim.py`: sono semanticamente identici?
4. Se divergono: correggere la strategia BT PRIMA di procedere con il tuning.
   Risultati di sweep ottenuti su codice BT divergente dalla Pandas sim sono da
   scartare.

### 3. Shell script Backtrader
I `.sh` di sweep che chiamano `btmain.py` devono fare `cd` in `bt-core/`:
```bash
cd "$(dirname "$0")/../../bt-core"
source .venv/bin/activate
set -a; source /home/htpc/backtrader/env/pa2; set +a
```

### 4. Ambiente
```bash
# Attiva venv (sempre da bt-core)
source /home/htpc/backtrader/bt-core/.venv/bin/activate

# Credenziali Alpaca (paper account 2)
set -a; source /home/htpc/backtrader/env/pa2; set +a
```

### 5. Dati
- Dati minute Alpaca: `/home/htpc/backtrader/data/m/alpaca/{SYMBOL}.csv`
- Scaricare mese per mese: `bin/update_data_monthly.sh` (evita OOM)
- Calendar cache: `/home/htpc/backtrader/config-common/cache/alpaca_calendar_cache.json`
