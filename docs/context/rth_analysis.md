# RTH Analysis — Universe Filter per strategie intraday

## Scopo

Costruisce un dataset giornaliero di feature RTH-only (Regular Trading Hours) per un universo di simboli.
Il dataset viene usato come **filtro universo** dalla strategia Backtrader intraday: ogni mattina si leggono le feature del giorno precedente per decidere quali simboli tradare e in quale direzione (long / short / flat).

## Filosofia

Gli indicatori classici (EMA, MACD, RSI) sono calcolati su rendimenti close-close, che contaminano il segnale intraday con i gap overnight.

La serie fondamentale usata qui è:

```
r_t = ln(C_t / O_t)   ← log-gain RTH giornaliero
```

Tutte le feature rolling sono costruite su questa serie e sui suoi derivati (escursioni, range, volume), **senza mai toccare i prezzi assoluti o i rendimenti close-close**.

---

## Struttura directory

```
bt-strategy-test/RTH_analysis/
  analisi_step_1_rth_only_indicatori_intraday.md    ← specifica tecnica dettagliata Step 1
  01_build_primitives.py                             ← Step 1a: scarica dati e calcola primitive
  02_build_features.py                               ← Step 1b: calcola feature rolling
  03_build_universe_map.py                           ← Step 1c: aggiunge trading_date e ranking

bt-strategy-test/RTH_analysis/out/
    rth_primitives.parquet                           ← output di 01
    rth_features.parquet                             ← output di 02
    rth_universe_map.parquet                         ← output di 03 (usato da Backtrader)
```

Questo documento di contesto vive in:

```txt
docs/context/rth_analysis.md
```

---

## Script — sequenza obbligatoria

Gli script vanno eseguiti nell'ordine numerico. Ogni script legge l'output del precedente.

## Convenzioni operative locali

Per gli studi RTH usare sempre l'ambiente Python del progetto:

```bash
bt-core/.venv/bin/python
```

Il `python3` di sistema non e' il riferimento operativo e puo' non avere
dipendenze come `pandas`/`pyarrow`.

Per backtest e sweep storici lunghi, la fonte daily standard e':

```txt
config-common/data/d/yahoo/*.csv
```

Questi CSV sono il dataset Yahoo daily condiviso dal framework e coprono lo
storico lungo usato dai backtest 2000-2026. I parquet in
`bt-strategy-test/RTH_analysis/out/` sono dataset derivati per feature engineering e possono
avere una finestra piu' corta; non vanno usati come fonte predefinita per gli
sweep storici RTH open/close.

Per aggiornare o scaricare dati Yahoo daily nel path standard:

```bash
bt-core/.venv/bin/python bt-core/load_tickers.py \
  --provider yahoo \
  --ticker NASDAQ_100_US.json \
  --data data \
  --timeframe d \
  --fromdate 2000-01-01 \
  --todate YYYY-MM-DD
```

Nota: `--data data` e' intenzionale. Il loader risolve il path sotto
`config-common/`; passare `config-common/data` duplica la directory.

### 01 — build_primitives

**Input**: dati daily OHLCV da Yahoo Finance per ogni simbolo dell'universo  
**Output**: `out/rth_primitives.parquet`

Calcola per ogni `(symbol, date)`:

| Campo | Formula | Descrizione |
|---|---|---|
| `rth_loggain` | `ln(C/O)` | Pressione netta sessione RTH |
| `rth_up_loggain` | `ln(H/O)` | Escursione rialzista massima |
| `rth_down_loggain` | `ln(L/O)` | Escursione ribassista massima (negativa) |
| `rth_range_log` | `ln(H/L)` | Range complessivo sessione |
| `log_volume` | `ln(Volume)` | Volume log-normalizzato |
| `overnight_loggain` | `ln(O_t / C_{t-1})` | Diagnostico: contributo overnight |

Controlla coerenza: `overnight_loggain + rth_loggain ≈ cc_logret`.

**Universo**: letto da un file JSON (default: NASDAQ 100). Supporta `--tickers` per liste custom.

---

### 02 — build_features

**Input**: `out/rth_primitives.parquet`  
**Output**: `out/rth_features.parquet`

Calcola feature rolling su finestre `N ∈ {5, 10, 20, 50, 63, 100}`.

Famiglie di feature:

| Famiglia | Feature | Note |
|---|---|---|
| Momentum cumulato | `rth_mom_N` | `Σ r_t` negli ultimi N giorni |
| Drift medio | `rth_mean_N` | `mean(r_t, N)` |
| Drift esponenziale | `rth_ewm_mean_N` | EWM su `r_t`, span=N |
| Accelerazione drift | `rth_drift_accel_12_26`, `_signal_9`, `_hist` | Analogo MACD su `r_t` |
| Volatilità | `rth_std_N` | `std(r_t, N)` |
| Momentum normalizzato | `rth_mom_norm_N` | `rth_mom_N / (rth_std_N * sqrt(N))` |
| Efficiency ratio | `rth_eff_N`, `rth_signed_eff_N` | `Σ r_t / Σ |r_t|` — signed mantiene il segno |
| Win rate | `rth_winrate_N` | Percentuale sessioni `r_t > 0` |
| Pressione H/L | `rth_upside_N`, `rth_downside_N`, `rth_pressure_balance_N` | Asimmetria escursioni |
| Close-to-range | `rth_close_to_range`, `_mean_5`, `_mean_20` | `r_t / range_t` |
| Range expansion | `rth_range_mean_N`, `rth_range_exp_5_20` | Espansione/compressione range |
| Volume relativo | `rth_rvol_5_20`, `rth_logvol_z_20` | Partecipazione vs norma |
| Relative strength | `rth_rs_N` | `r_t(sym) - mean(r_t, paniere)` — benchmark = equal-weight paniere |
| Overnight dependency | `on_mom_N`, `rth_share_20` | Quota del movimento prodotta in RTH |

---

### 03 — build_universe_map

**Input**: `out/rth_features.parquet`  
**Output**: `out/rth_universe_map.parquet`

Aggiunge:
- `trading_date = feature_date + 1 giorno di mercato` — anti-lookahead
- Ranking cross-sectional percentile per feature chiave (es. `rth_mom_20_rank_pct`)
- Z-score cross-sectional opzionale

La colonna `trading_date` è quella che Backtrader usa per filtrare il file: all'inizio della giornata di trading carica solo le righe con `trading_date == oggi`.

---

## Regola anti-lookahead

```
feature_date  = giorno fino al quale sono calcolate le feature (es. 2026-05-26)
trading_date  = giorno in cui sono usabili in Backtrader  (es. 2026-05-27)
```

Il file `rth_universe_map.parquet` contiene entrambe le colonne. La strategia BT filtra sempre su `trading_date`.

---

## Universo titoli

Default: NASDAQ 100, lista da `config-common/tickers/nasdaq100.json`.

Override con argomento `--tickers path/to/custom_list.json`.

Formato JSON atteso:

```json
["AAPL", "MSFT", "NVDA", "..."]
```

---

## Benchmark per Relative Strength

La feature `rth_rs_N` usa come benchmark la **media equal-weight dei `r_t`** di tutti i simboli del paniere nel giorno:

```
benchmark_rt = mean(r_t, tutti i simboli attivi quel giorno)
rth_rs_sym   = r_t(sym) - benchmark_rt
```

Questo misura la sovra/sottoperformance RTH rispetto al paniere stesso, indipendentemente da QQQ (market-cap weighted).

---

## Uso nelle strategie Backtrader

La strategia intraday carica `rth_universe_map.parquet` e filtra su `trading_date == data_corrente`.

Ogni simbolo può avere un flag di direzione derivato da `rth_signed_eff_N`:

```
signed_eff > +soglia  → long candidate
signed_eff < -soglia  → short candidate
altrimenti            → flat / escluso
```

Le soglie non sono definite in questo step: appartengono alla strategia BT o allo Step 2 (feature selection).

---

## Step successivi (fuori scope Step 1)

- **Step 2**: correlazione, mutual information, selezione feature rilevanti vs target
- **Step 3**: definizione soglie operative e backtest del filtro universo
