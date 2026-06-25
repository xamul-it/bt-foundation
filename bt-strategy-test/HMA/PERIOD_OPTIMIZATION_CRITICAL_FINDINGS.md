# Period Optimization - Scoperte Critiche

**Data**: 2026-02-13
**Status**: 🚨 **PROBLEMA IDENTIFICATO**

---

## 🎯 Risultato Principale: IL VERO PROBLEMA NON È IL PERIOD

### Aspettative vs Realtà

| Metrica | Monte Carlo | Nostro Backtest | Discrepanza |
|---------|-------------|-----------------|-------------|
| **Periodo testato** | 2025-05-06 to 2025-07-03 | 2025-05-06 to 2025-07-03 | ✅ Identico |
| **Simboli** | 20 (NASDAQ_HV) | 20 (NASDAQ_HV) | ✅ Identico |
| **HMA Period** | 16 | 8-32 (testati) | ✅ Incluso |
| **ATR Filter** | ≥ 0.7% | ≥ 0.7% | ✅ Identico |
| **Trades (period=16)** | 182 | **9** | ❌ **95% MANCANTI!** |
| **Expectancy (period=16)** | +0.189% | **-0.192%** | ❌ **OPPOSTO!** |
| **Simboli attivi** | 20 | **4 soltanto** | ❌ **80% INATTIVI!** |

---

## 🔍 Analisi Dettagliata

### 1. Distribuzione Trade per Period

| Period | Total Trades | Win Rate | Expectancy | Rank |
|--------|--------------|----------|------------|------|
| **10** | 19 | 52.6% | **+0.090%** | 🥇 1st |
| **8** | 28 | 53.6% | **+0.068%** | 🥈 2nd |
| 12 | 15 | 46.7% | -0.024% | 3rd |
| 24 | 8 | 25.0% | -0.141% | 4th |
| 14 | 12 | 33.3% | -0.146% | 5th |
| 20 | 8 | 25.0% | -0.166% | 6th |
| **16** | **9** | 33.3% | **-0.192%** | 🔴 **7th/9** |
| 32 | 6 | 16.7% | -0.249% | 8th |
| 18 | 7 | 14.3% | -0.370% | 9th |

**Scoperta Sorprendente**: Period=16 è il **7° su 9** per performance! Period 8-10 sono migliori.

### 2. Trade per Simbolo (Tutti i Period Combinati)

| Simbolo | Total Trades (all periods) | Best Period | Best Exp | Period 16 Trades |
|---------|----------------------------|-------------|----------|------------------|
| **LCID** | 49 | 8 | +0.197% | 5 |
| **TSLA** | 37 | 10 | +0.112% | 3 |
| **PTON** | 11 | 8 | -0.027% | 1 |
| **SMCI** | 6 | 20 | +1.259% | 0 |
| AAPL | **0** | - | - | 0 |
| AMAT | **0** | - | - | 0 |
| AMD | **0** | - | - | 0 |
| BIDU | **0** | - | - | 0 |
| BKR | **0** | - | - | 0 |
| CRWD | **0** | - | - | 0 |
| DDOG | **0** | - | - | 0 |
| DOCU | **0** | - | - | 0 |
| FTNT | **0** | - | - | 0 |
| MDB | **0** | - | - | 0 |
| MSTR | **0** | - | - | 0 |
| OKTA | **0** | - | - | 0 |
| RIVN | **0** | - | - | 0 |
| ROKU | **0** | - | - | 0 |
| SNOW | **0** | - | - | 0 |
| ZM | **0** | - | - | 0 |

**🚨 PROBLEMA CRITICO**: 16 simboli su 20 (80%) hanno ZERO trade in QUALSIASI period!

---

## 🔬 Test Adaptive Period: FALLITO

### Setup
- **Training**: 2025-04-06 to 2025-05-05 (30 giorni prima validation)
- **Validation**: 2025-05-06 to 2025-07-03
- **Logica**: Ottimizza period per simbolo su training, applica su validation

### Periodi Selezionati (Training)

| Simbolo | Selected Period | Training Exp |
|---------|----------------|--------------|
| AAPL | 20 | +0.338% |
| BKR | 32 | +1.102% |
| DOCU | 32 | +1.212% |
| FTNT | 24 | +1.517% |
| ZM | 32 | +1.359% |
| AMD | 16 | +0.214% |
| BIDU | 16 | +0.396% |
| CRWD | 16 | +0.342% |
| TSLA | 16 | +0.407% |
| ... | ... | ... |

**Nota**: Training period mostrava expectancy POSITIVE per molti simboli!

### Risultati Validation

| Strategy | Total Trades | Expectancy |
|----------|--------------|------------|
| **Adaptive** (symbol-specific) | 7 | **-0.370%** ❌ |
| **Fixed(16)** | 9 | **-0.192%** ❌ |

**Improvement**: -0.178% (PEGGIORAMENTO del 92.4%!)

**Interpretazione**: Adaptive optimization FALLISCE completamente. Periodi ottimali in training non trasferiscono a validation.

---

## 💡 Cosa Significa Tutto Questo?

### ❌ Cosa NON Abbiamo Scoperto

**NON è vero che**:
- Period-specific optimization funziona (è fallito)
- Period=16 è universalmente ottimale (è 7°/9!)
- "Frequenze di risonanza" esistono (non dimostrato)
- Adaptive tuning migliora performance (peggiora!)

### ✅ Cosa ABBIAMO Scoperto

**È vero che**:
1. **Il nostro backtest NON replica Monte Carlo** (9 trades vs 182)
2. **80% dei simboli non generano trade** (problema implementazione)
3. **Period 8-10 > Period 16** (nel nostro backtest)
4. **Adaptive optimization overfits** (training ≠ validation)

---

## 🔍 Root Cause Analysis: Perché Solo 9 Trade?

### Ipotesi 1: HMA Calculation Differs ⚠️

**Problema possibile**: La nostra implementazione HMA potrebbe non matchare Monte Carlo esattamente.

**Test**:
- Confrontare valori HMA punto-per-punto con Monte Carlo
- Verificare WMA calculation (weights, precision)
- Controllare handling NaN values

### Ipotesi 2: Signal Detection Logic Differs 🎯 **PROBABILE**

**Problema possibile**: Peak/trough detection non identifica gli stessi punti di Monte Carlo.

**Differenze possibili**:
```python
# Nostro codice:
direction = np.sign(hma.diff())
direction_change = direction.diff()

# Peak: direction_change == -2 (was +1, now -1)
# Trough: direction_change == +2 (was -1, now +1)
```

**Monte Carlo potrebbe usare**:
- Lookback diverso per confermare peak/trough
- Threshold su magnitude del change
- Filtro su consecutive bars in stessa direzione

**Esempio**:
```python
# Possibile logica Monte Carlo:
# Peak = HMA[t-2] < HMA[t-1] AND HMA[t-1] > HMA[t] (confirmed peak)
# Invece di solo: direction change from + to -
```

### Ipotesi 3: Data Quality Issues ⚠️

**Problema possibile**: Alcuni simboli hanno missing bars nel periodo validation.

**Evidenza**:
- 16/20 simboli zero trades
- Anche periods molto diversi (8-32) non generano trade
- Training period aveva trade positivi

**Test necessario**:
- Verificare completeness dati per ogni simbolo
- Controllare gaps in time series
- Confrontare numero bars con Monte Carlo

### Ipotesi 4: Time Window Filter Too Strict ⚠️

**Implementazione**:
```python
trading_start = 15 * 60  # 15:00 UTC
trading_end = 20 * 60    # 20:00 UTC

valid = (time_minutes >= trading_start) & (time_minutes < trading_end)
```

**Possibile problema**:
- Monte Carlo potrebbe includere 14:30-15:00 per signal generation
- O usare UTC offsets diversi

### Ipotesi 5: ATR Calculation on Wrong Timeframe ⚠️

**Implementazione**: Calcoliamo ATR su minute bars direttamente.

**Monte Carlo potrebbe**:
- Calcolare ATR su daily bars aggregati
- Poi applicare threshold ai minute signals
- Questo darebbe valori ATR % diversi

---

## 🎯 Azioni Immediate Necessarie

### Test 1: Confronto Diretto HMA Values ✅ PRIORITÀ ALTA

**Obiettivo**: Verificare se HMA calculation matcha Monte Carlo

**Procedura**:
1. Scegliere simbolo LCID, data 2025-05-09 (giorno con trade noto)
2. Calcolare HMA period=16 bar-by-bar
3. Stampare valori HMA 14:30-16:00
4. Confrontare con valori Monte Carlo (se disponibili)

### Test 2: Debug Signal Detection 🔍 PRIORITÀ ALTA

**Obiettivo**: Verificare peak/trough detection logic

**Procedura**:
1. Simbolo LCID, 2025-05-09
2. Stampare:
   - HMA values
   - Direction (+1/-1)
   - Direction changes
   - Signal fired
3. Verificare se 15:09 genera LONG signal come in Monte Carlo

### Test 3: Verificare Data Completeness ⚠️ PRIORITÀ MEDIA

**Obiettivo**: Assicurare che tutti i simboli hanno dati completi

**Procedura**:
```python
for symbol in symbols:
    df = load_data(symbol)
    print(f"{symbol}: {len(df)} bars, "
          f"range {df.index[0]} to {df.index[-1]}, "
          f"gaps: {count_gaps(df)}")
```

### Test 4: Eseguire Backtrader HMA Strategy 🎯 PRIORITÀ ALTISSIMA

**Obiettivo**: Confrontare output Backtrader strategy vs Monte Carlo

**Comando**:
```bash
python btmain.py \
  --strat intraday_hma_dynamic.HMADynamic \
  --ticker NASDAQ_HV.json \
  --fromdate 2025-05-06 --todate 2025-07-03 \
  --provider alpaca --data data --timeframe minutes \
  --commission none --amount 100000 \
  --log_trades --debug
```

**Aspettative**:
- Se anche Backtrader dà ~9 trades → problema è nella logica strategia
- Se Backtrader dà ~180 trades → problema è solo in questo script di test

---

## 📊 Conclusioni Provvisorie

### ❌ Risposta alla Domanda Originale

**Domanda**: "Ogni simbolo ha un periodo ottimale specifico (frequenze di risonanza)?"

**Risposta**: **NON POSSIAMO RISPONDERE** perché il nostro backtest non replica Monte Carlo.

**Motivi**:
1. Solo 4/20 simboli attivi (sample bias estremo)
2. Solo 9 trades totali (statisticamente insignificante)
3. Expectancy negativa quando Monte Carlo era positiva
4. 95% dei trade mancanti

**Prima di testare period optimization, dobbiamo FIX il problema fondamentale**: perché il backtest non trova gli stessi trade di Monte Carlo?

### ✅ Cosa Abbiamo Imparato

**Adaptive Period Optimization**:
- ❌ Non funziona (training ≠ validation)
- ⚠️ Alto rischio overfitting
- 📉 Peggiora performance invece di migliorare

**Period Comparison (con caveat)**:
- Period 8-10 sembrano migliori di 16 (nel nostro broken backtest)
- Ma non possiamo fidarci finché non fixiamo il bug fondamentale

---

## 🚨 NEXT STEPS - CRITICAL

### Passo 1: DEBUG Signal Detection (URGENTE)

Creare script che:
1. Carica LCID data 2025-05-09
2. Calcola HMA period=16
3. Stampa signal detection step-by-step
4. Verifica match con Monte Carlo trade @ 15:09

### Passo 2: Fix Backtrader Strategy (URGENTE)

Una volta identificato il bug in signal detection, applicare fix a:
- `strategies/intraday_hma_dynamic.py`
- O creare nuova versione corretta

### Passo 3: Re-Run Validation Test (POST-FIX)

Dopo fix:
```bash
python btmain.py --strat intraday_hma_dynamic.HMADynamic \
  --ticker NASDAQ_HV.json --fromdate 2025-05-06 --todate 2025-07-03 \
  --provider alpaca --data data --timeframe minutes \
  --commission none --amount 100000 --log_trades
```

**Aspettative POST-FIX**:
- Trades: ~180
- Expectancy: ~+0.15% to +0.20%
- Simboli attivi: 18-20
- Win rate: ~33-37%

### Passo 4: THEN Re-Test Period Optimization (OPTIONAL)

Solo DOPO che il backtest matcha Monte Carlo, possiamo:
- Re-testare period optimization
- Verificare se period=16 è veramente ottimale
- Testare adaptive tuning con dati corretti

---

## 📝 Summary for User

**Scoperta Principale**:
Il problema NON è quale period usare. Il problema è che il nostro backtest **non funziona correttamente** e trova solo 5% dei trade che Monte Carlo trova.

**Evidenze**:
- Monte Carlo: 182 trades
- Nostro backtest: 9 trades
- 80% simboli inattivi
- Expectancy opposta (+0.19% vs -0.19%)

**Prossimo Passo**:
DEBUGGARE signal detection e HMA calculation per capire perché non matchano Monte Carlo.

**Period Optimization**:
Non possiamo rispondere fino a quando non fixiamo il bug fondamentale.

**Adaptive Period**:
Testato e FALLITO - overfitting su training period, non trasferisce a validation.

---

**Data**: 2026-02-13
**Status**: 🔴 CRITICO - Bug identificato, fix necessario
**Blocco**: Period optimization è inutile finché backtest non replica Monte Carlo
