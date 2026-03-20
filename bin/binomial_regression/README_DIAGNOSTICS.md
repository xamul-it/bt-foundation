# Diagnostica Strategia Regressione - Guida Operativa

## Situazione Attuale

**PROBLEMA CRITICO IDENTIFICATO**: Le correlazioni tra slope e forward outcomes sono **inverse**:
- slope positivo → correlazione NEGATIVA con high_ret_max
- Le slice con slope negativo performano MEGLIO delle slice con slope positivo

Questo suggerisce uno o più dei seguenti problemi:
1. Bug nel calcolo dello slope
2. Look-ahead bias nei forward outcomes
3. Mean reversion dominante su TF 1m
4. Problema di allineamento temporale

## Piano di Azione (Ordine Consigliato)

### FASE 1: VERIFICHE CRITICHE (PRIORITÀ MASSIMA)

#### 1.1 Verifica Calcolo Slope
```bash
cd bin/binomial_regression
python verify_slope_calculation.py
```

**Cosa fa:**
- Campiona 20 candele random
- Ricalcola lo slope manualmente
- Confronta con i valori salvati in `*_feature_outcome_full.csv`
- **VERIFICA LOGICA**: se i prezzi salgono, lo slope DEVE essere positivo

**Cosa cercare nell'output:**
- ✅ `Match: ✅ OK` per tutti i test → calcolo numericamente corretto
- ❌ `Signs match: ❌ FAIL (LOGIC ERROR)` → **BUG CRITICO** nella formula

**Se trovi un bug:**
1. Correggi `01-fast_backtest.py` nella funzione `regression_features_linear()`
2. **RICALCOLA TUTTO** da capo:
   ```bash
   python 01-fast_backtest.py
   python 02-feature_diagnostics.py
   python 03-slice_analisys.py
   ```

#### 1.2 Verifica Forward Outcomes
```bash
python verify_forward_outcomes.py
```

**Cosa fa:**
- Campiona 20 candele random
- Ricalcola high_ret_max_Nm, low_ret_min_Nm, close_ret_Nm manualmente
- Verifica che la finestra sia FUTURA (non include barra corrente)
- Confronta con i valori salvati

**Cosa cercare:**
- ✅ `Match: ✅ OK` per tutti i test → calcolo corretto
- ⚠️ `SOSPETTO LOOK-AHEAD` → possibile inclusione barra corrente
- ❌ `FAIL` con diff > 1e-6 → **BUG** nel calcolo

**Se trovi un bug:**
1. Correggi `01-fast_backtest.py` nella funzione `add_forward_outcomes_all_rows()`
2. **RICALCOLA TUTTO** da capo

### FASE 2: ANALISI TIMING E FILTRI

#### 2.1 Analisi Distribuzione Tempi Hit
```bash
python analyze_hit_timing.py \
    --tp-pct 0.005 \
    --sl-pct 0.003 \
    --horizons 3,5,6,10,15,30,60 \
    --slope5-min 0.0004 \
    --slope20-min 0.0002 \
    --slope60-min 0.00015 \
    --volz-min 0.85 \
    --quality-window 60 \
    --quality-min 0.5
```

**Cosa fa:**
- Filtra le entrate secondo i parametri specificati
- Per ogni entry, calcola ESATTAMENTE quante barre impiega a raggiungere TP/SL
- Calcola probabilità di chiusura entro N minuti
- Identifica posizioni "stuck" (non chiudono entro max horizon)

**Output:**
- `hit_timing_summary_by_horizon.csv`:
  - `p_hit_tp_within` - Probabilità TP entro N bars
  - `p_hit_sl_within` - Probabilità SL entro N bars
  - `p_no_close_within` - **METRICA CRITICA**: % posizioni che non chiudono
- `hit_timing_global_stats.csv`:
  - `median_bars_to_tp` - Mediana tempo a TP
  - `median_bars_to_sl` - Mediana tempo a SL
  - `p_no_hit` - % posizioni che MAI raggiungono TP o SL

**Interpretazione:**
- Se `p_no_close_within` @ 6 bars > 50% → **PROBLEMA**: troppo poche chiudono in tempo
- Se `median_bars_to_tp` > 15 → TP troppo distante o filtri insufficienti
- Se `p_no_hit` > 30% → Setup non efficace, serve screening migliore

**Azioni:**
1. Se le posizioni non chiudono entro 6 minuti, prova:
   - Ridurre TP (es. da 0.5% a 0.3%)
   - Ridurre SL (es. da 0.3% a 0.2%)
   - Aggiungere filtri su volatilità (vedi fase 2.2)

#### 2.2 Aggiungere Filtri su Volatilità Realizzata

**TODO**: Creare script `add_volatility_filters.py` che:
1. Calcola `realized_vol_5m = std(log_returns[-5:])`
2. Analizza correlazione tra `realized_vol` e `bars_to_tp`
3. Trova soglia ottimale per massimizzare `p_hit_tp_within_6m`

**Ipotesi:** Alta volatilità → tempi di hit più brevi

#### 2.3 Analisi Contesto Temporale

**TODO**: Creare script `analyze_time_context.py` che:
1. Divide giornata in slot orari (30 min o 1 ora)
2. Per ogni slot, calcola `p_hit_tp_within_6m`
3. Identifica slot migliori (es. primi 30 min dopo open)

**Ipotesi:** Primi 30-60 minuti hanno volatilità più alta

### FASE 3: OTTIMIZZAZIONE COMBINATA

**TODO**: Creare script `optimize_quality_vol_filter.py` che:
- Grid search su combinazioni di:
  - `fit_quality >= X`
  - `realized_vol >= Y`
  - `volZ >= Z`
  - `hour_of_day in [slots]`
- Ottimizza per:
  - Massimizzare `p_hit_tp_within_6m`
  - Minimizzare `p_no_close_within_6m`
  - Massimizzare Sharpe ratio

## Domande Frequenti

### Q: Le correlazioni slope vs high_ret_max sono negative. È normale?
**A**: NO. Se lo slope è positivo (prezzi salgono), ci aspettiamo che high_ret_max sia positivo (prezzi continuano a salire). Una correlazione negativa indica:
- Bug nel calcolo
- Mean reversion dominante
- Problema di allineamento temporale

Esegui PRIMA i test di verifica (Fase 1).

### Q: Come faccio a minimizzare le posizioni che non si chiudono in 6 minuti?
**A**:
1. Esegui `analyze_hit_timing.py` per capire quante sono
2. Se > 50%, prova:
   - Ridurre TP/SL (obiettivi più vicini)
   - Aggiungere filtro su volatilità realizzata
   - Restringere a slot orari ad alta volatilità
   - Aumentare `quality_min` (solo setup migliori)

### Q: Come interpreto `fit_quality`?
**A**: È un ranking percentile aggregato (0-1) che combina:
- r2 alto (buon fit della regressione)
- disp basso (residui piccoli)
- mse/rmse/mae bassi (errori piccoli)

`fit_quality > 0.7` = top 30% dei setup più "puliti"

### Q: Devo invertire la strategia (entrare su slope negativo)?
**A**: SOLO se:
1. Hai verificato che il calcolo dello slope è corretto (Fase 1.1)
2. Hai verificato che i forward outcomes sono corretti (Fase 1.2)
3. La correlazione negativa persiste

In quel caso, il TF 1m potrebbe essere dominato da mean reversion, non da trend following.

### Q: Quali sono i valori "buoni" per i filtri?
**A**: Dipende dai tuoi risultati. Linee guida:
- `slope_5_min`: 0.0004 - 0.001 (cerca movimenti decisi)
- `volZ_min`: 0.85 - 1.5 (volume sopra media)
- `quality_min`: 0.5 - 0.8 (top 50% - 20%)
- `p_hit_tp_within_6m`: > 40% (almeno 4 su 10 raggiungono TP)
- `p_no_close_within_6m`: < 30% (max 3 su 10 "stuck")

## Log di Esecuzione Consigliato

```bash
# FASE 1: Verifiche critiche
python verify_slope_calculation.py > logs/verify_slope.log 2>&1
python verify_forward_outcomes.py > logs/verify_outcomes.log 2>&1

# Controlla i log
grep "FAIL" logs/verify_slope.log
grep "FAIL" logs/verify_outcomes.log

# Se OK, procedi
# FASE 2: Analisi timing
python analyze_hit_timing.py \
    --tp-pct 0.005 \
    --sl-pct 0.003 \
    --horizons 3,5,6,10,15,30,60 \
    > logs/hit_timing.log 2>&1

# Controlla risultati
cat results/hit_timing_analysis/hit_timing_global_stats.csv
cat results/hit_timing_analysis/hit_timing_summary_by_horizon.csv

# Se p_no_close_within @ 6 bars è troppo alta, ajusta filtri e riprova
```

## File di Output

### Da verify_slope_calculation.py:
- Solo stdout (nessun file salvato)
- Controlla manualmente per "❌ FAIL"

### Da verify_forward_outcomes.py:
- Solo stdout (nessun file salvato)
- Controlla manualmente per "❌ FAIL" o "⚠️ LOOK-AHEAD"

### Da analyze_hit_timing.py:
- `results/hit_timing_analysis/`:
  - `analysis_meta.csv` - Parametri usati
  - `hit_timing_row_level.csv` - Timing per ogni entry
  - `hit_timing_summary_by_horizon.csv` - Metriche aggregate per orizzonte
  - `hit_timing_global_stats.csv` - Statistiche globali
  - `hit_prob_by_horizon.png` - Grafico probabilità hit
  - `distribution_bars_to_tp.png` - Distribuzione tempo a TP
  - `distribution_bars_to_sl.png` - Distribuzione tempo a SL
  - `distribution_first_hit_type.png` - Distribuzione tipo primo hit

## Prossimi Passi

Dopo aver completato le fasi 1-2, puoi:
1. Implementare filtri addizionali (volatilità, ora del giorno)
2. Ottimizzare combinazioni di filtri
3. Testare in-sample vs out-of-sample
4. Implementare in backtrader per test realistici

## Contatti / Note

Qualsiasi anomalia nei test di verifica (Fase 1) è **CRITICA** e deve essere risolta prima di procedere.
Se slope e outcomes sono corretti ma la correlazione è negativa, considera che:
- Su TF 1m, mean reversion può dominare
- Potresti dover invertire la logica (entrare su pullback invece che su breakout)
- Oppure passare a TF più lunghi (5m, 15m)
