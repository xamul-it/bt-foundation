# HMA Strategy Tuning — Documentazione

## Obiettivo
Ottimizzare la strategia `intraday.HMA` su `HMA_top9.json` per massimizzare SQN e PNL
su dati minute Alpaca, senza commissioni (live trading usa limit orders a prezzo).

## Ticker: HMA_top9
`WBD, PLTR, CSX, INTC, KDP, KHC, CSCO, CMCSA, NFLX`
Dati: Jan 2025 → Mar 2026 (`config-common/data/m/alpaca/`)

## Configurazione comune
```
--fromdate 2025-01-01 --todate 2026-03-06
--provider alpaca --timeframe minutes --commission none
inverted=True  (modalità contrarian: long quando HMA scende, short quando sale)
use_calendar=True  (richiede il calendar cache Alpaca popolato)
```

## Prerequisiti
```bash
# Credenziali
set -a; source /home/htpc/backtrader/env/pa2; set +a

# Calendar cache (se non già presente per il periodo)
# Viene popolato automaticamente al primo run se le credenziali sono attive

# Dati aggiornati
./bin/update_data_monthly.sh HMA_top9.json 2025-01 2026-03
```

## Sweep eseguiti

### Sweep 1 — Baseline period/exitbar
Script: `bt-strategy-test/HMA/sweep1_period_exitbar.sh`
Output: `out/intraday/HMA/tuning2/`
Analisi: `python3 out/intraday/HMA/tuning2/analyze_sweep.py`

**Miglior risultato:** `period=14, exitbar=6` → SQN=2.237, PNL=24.5%, Sharpe=0.950

| Config    | Trades | SQN   | PNL%  | Sharpe |
|-----------|--------|-------|-------|--------|
| p14_eb6   | 22375  | 2.237 | 24.5% | 0.950  | ← BEST
| p16_eb4   | 21241  | 2.078 | 22.8% | 0.903  |
| p16_eb8   | 22334  | 1.228 | 13.2% | 0.709  |
| p14_eb4   | 21756  | 1.091 | 11.3% | 0.512  |

Pattern: exitbar=3 sempre negativo; period < 13 e > 16 peggiorano.

### Sweep 2 — Fine-grained intorno a p14_eb6
Script: `bt-strategy-test/HMA/sweep2_finegrained.sh`
Output: `out/intraday/HMA/tuning3/`
Analisi: `python3 out/intraday/HMA/tuning3/analyze_sweep.py`

Nessuna configurazione supera p14_eb6. Confermato ottimo locale.

### Sweep 3 — SL / TP / ATR filter
Script: `bt-strategy-test/HMA/sweep3_sl_tp_atr.sh`
Output: `out/intraday/HMA/tuning4/`
Analisi: `python3 out/intraday/HMA/tuning4/analyze_sweep.py`

Base fissa: `period=14, exitbar=6, inverted=True`

| Config        | Trades | SQN   | PNL%  | Sharpe | Note |
|---------------|--------|-------|-------|--------|------|
| sl015         | 22264  | 2.739 | 31.6% | 0.944  | ← BEST (+22% SQN vs base) |
| sl010         | 22406  | 2.201 | 23.9% | 0.935  |
| sl003         | 22105  | 1.917 | 20.0% | 0.975  |
| tp005         | 22397  | 1.895 | 20.5% | 0.955  |
| sl005         | 21780  | 1.608 | 16.6% | 1.060  |

Pattern: SL stretto (0.3-0.5%) peggiora; SL=1.5% ottimale (mercato ha bisogno di spazio).

Sweep 3 completo (parziali già noti + residui lanciati dom 8 mar):
da completare: tp010, tp020, sl+tp combinati, atr*, sl015+tp/atr.
Aggiornare la tabella sopra quando sweep3 termina.

## Risultato migliore attuale
**Parametri ottimali:** `period=14, exitbar=6, sl_pct=0.015, inverted=True`
SQN=2.739, PNL=31.6%, Sharpe=0.944 (da confermare con sweep3 completo)

## Script organizzati

| Script | Output | Descrizione |
|--------|--------|-------------|
| `bt-strategy-test/HMA/sweep1_period_exitbar.sh` | `out/intraday/HMA/tuning2/` | Grid period×exitbar |
| `bt-strategy-test/HMA/sweep2_finegrained.sh`    | `out/intraday/HMA/tuning3/` | Fine-grained intorno a p14_eb6 |
| `bt-strategy-test/HMA/sweep3_sl_tp_atr.sh`      | `out/intraday/HMA/tuning4/` | SL / TP / ATR filter |

Log dei run in `logs/hma_sweep{1,2,3}*.log`

## Analisi

```bash
# Singolo sweep
python3 out/intraday/HMA/tuning2/analyze_sweep.py
python3 out/intraday/HMA/tuning3/analyze_sweep.py
python3 out/intraday/HMA/tuning4/analyze_sweep.py

# Tutti i sweep unificati + top10
python3 bt-strategy-test/HMA/analyze_all.py
```
