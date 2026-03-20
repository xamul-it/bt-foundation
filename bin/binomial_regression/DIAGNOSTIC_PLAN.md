# Piano Diagnostico per Regression Strategy

## Problema Identificato
Le correlazioni tra slope e forward outcomes sono inverse rispetto alle aspettative:
- slope positivo → high_ret_max negativo
- Le slice con slope negativo performano meglio

## Possibili Cause
1. **Bug nel calcolo slope/regression**
2. **Look-ahead bias nei forward outcomes**
3. **Mean reversion dominante su TF 1m**
4. **Allineamento temporale errato**
5. **Inclusione della barra corrente nei forward outcomes**

## Piano di Analisi (in ordine di priorità)

### 1. VERIFICA CALCOLO SLOPE (CRITICO)
**File**: `bin/binomial_regression/verify_slope_calculation.py`

Obiettivo: Verificare che lo slope sia calcolato correttamente
- Campionare 10-20 candele random
- Stampare i valori delle ultime N candele usate nella regressione
- Calcolare slope manualmente e confrontare con il risultato
- Verificare che slope > 0 quando i prezzi salgono

**Test da fare:**
```python
# Per una finestra di 60 candele:
# Se close[0] < close[59] → slope dovrebbe essere POSITIVO
# Se close[0] > close[59] → slope dovrebbe essere NEGATIVO
```

### 2. VERIFICA FORWARD OUTCOMES (CRITICO)
**File**: `bin/binomial_regression/verify_forward_outcomes.py`

Obiettivo: Verificare che non ci sia look-ahead bias
- Per ogni candela i, verificare che high_ret_max_5m usi SOLO candele [i+1, i+5]
- NON deve includere high[i] (candela corrente)
- Stampare esempi di calcolo per 10 candele random

**Test da fare:**
```python
# Per candela i=100:
# high_ret_max_5m[100] deve usare SOLO high[101:106]
# NON deve usare high[100]
```

### 3. ANALISI DISTRIBUZIONE TEMPI DI HIT
**File**: `bin/binomial_regression/analyze_hit_timing.py`

Obiettivo: Capire su quali orizzonti temporali la strategia funziona
- Per posizioni filtrate (slope_5>0, slope_20>0, slope_60>0, volZ>0.85)
- Calcolare distribuzione del tempo al primo hit TP (es. +0.5%)
- Calcolare distribuzione del tempo al primo hit SL (es. -0.3%)
- Identificare % posizioni che non si chiudono entro 5/10/15/30/60 minuti

**Metriche:**
- `p_hit_tp_within_Nm` per N in [3, 5, 10, 15, 30, 60]
- `p_hit_sl_within_Nm`
- `median_time_to_tp` (in minuti)
- `median_time_to_sl`
- `p_no_close_within_60m` (% posizioni "stuck")

### 4. FILTRI BASATI SU VOLATILITÀ REALIZZATA
**File**: `bin/binomial_regression/add_volatility_filters.py`

Obiettivo: Filtrare posizioni con alta probabilità di chiusura rapida
- Calcolare volatilità realizzata su finestre 5/10/20 minuti
- `realized_vol_5m = std(close_ret_1m[-5:])`
- Correlazione tra `realized_vol` e `time_to_hit_tp`
- Identificare soglie ottimali per massimizzare `p_hit_within_6m`

**Ipotesi:**
- Alta volatilità → tempi di hit più brevi
- Bassa volatilità → posizioni "stuck" per più tempo

### 5. ANALISI CONTESTO TEMPORALE
**File**: `bin/binomial_regression/analyze_time_context.py`

Obiettivo: Identificare filtri basati su ora del giorno
- Dividere giornata in slot: [9:30-10:00], [10:00-11:00], ..., [15:30-16:00]
- Per ogni slot, calcolare:
  - `p_hit_tp_within_6m`
  - `median_time_to_tp`
  - `p_no_close_within_6m`
- Distanza dall'open (minuti da 9:30)
- Distanza dal close (minuti a 16:00)

**Ipotesi:**
- Primi 30-60 minuti: alta volatilità, tempi brevi
- Metà giornata: bassa volatilità, tempi lunghi
- Ultimi 30 minuti: alta volatilità, tempi brevi

### 6. ANALISI COMBINATA FIT_QUALITY + REALIZED_VOL
**File**: `bin/binomial_regression/optimize_quality_vol_filter.py`

Obiettivo: Trovare combinazione ottimale di filtri
- Grid search su:
  - `fit_quality_60 >= [0.3, 0.5, 0.7, 0.9]`
  - `realized_vol_5m >= [p50, p60, p70, p80, p90]`
  - `volZ >= [0.5, 0.75, 0.85, 0.95]`
- Ottimizzare per:
  - `p_hit_tp_within_6m` (massimizzare)
  - `p_no_close_within_6m` (minimizzare)
  - `sharpe_ratio_6m` (massimizzare)

## Output Attesi

### Se il problema è nel calcolo slope:
→ Fix del codice in `01-fast_backtest.py`
→ Ricalcolare tutti i risultati

### Se il problema è nei forward outcomes:
→ Fix del codice in `add_forward_outcomes_all_rows()`
→ Ricalcolare tutti i risultati

### Se il problema è mean reversion:
→ Invertire la strategia (entrare su slope negativo?)
→ O cambiare approccio (mean reversion invece di trend following)

### Se il problema è timing:
→ Aggiungere filtri su volatilità e ora del giorno
→ Restringere finestra operativa a momenti ad alta volatilità

## Ordine di Esecuzione Consigliato

1. **VERIFICA SLOPE** (30 min) - CRITICO
2. **VERIFICA FORWARD OUTCOMES** (30 min) - CRITICO
3. **ANALISI TEMPI HIT** (1-2 ore) - Per capire orizzonti ottimali
4. **VOLATILITÀ REALIZZATA** (1-2 ore) - Per filtri di screening
5. **CONTESTO TEMPORALE** (1 ora) - Per filtri addizionali
6. **OTTIMIZZAZIONE COMBINATA** (2-3 ore) - Per setup finale

## Note Implementative

- Tutti gli script devono stampare esempi numerici espliciti
- Usare sempre `pd.to_numeric(..., errors='coerce')` per robustezza
- Salvare risultati intermedi in CSV per revisione manuale
- Generare plot distribuzionali per ogni metrica chiave
