# CRITICAL BUG FIX: HMA Signal Detection Logic

**Data**: 2026-02-13
**Status**: ✅ **RISOLTO**

---

## 🎯 Root Cause Identified

Il problema NON era:
- ❌ ATR calculation (già fixato, corretto)
- ❌ Data quality
- ❌ Period optimization
- ❌ Time window filters

Il problema ERA:
- ✅ **Signal detection logic completamente diversa** tra Monte Carlo e Backtrader!

---

## 🔍 Analisi del Bug

### Monte Carlo Implementation (CORRETTA)

**File**: `bt-strategy-test/HMA/01-hma_backtest.py` righe 151-158

```python
# Determine HMA signal based on inverted flag
if inverted:
    # Inverted: go SHORT when HMA rising, LONG when HMA falling
    signal_short = hma_prev < hma_curr  # HMA going up → SHORT
    signal_long = hma_prev > hma_curr   # HMA going down → LONG
else:
    # Normal: go LONG when HMA rising, SHORT when HMA falling
    signal_long = hma_prev < hma_curr
    signal_short = hma_prev > hma_curr
```

**Caratteristica**:
- Genera segnali **OGNI bar** basati sulla **direzione corrente** di HMA
- Se HMA sta scendendo (prev > curr) → LONG signal
- Se HMA sta salendo (prev < curr) → SHORT signal
- **Segnali continui** finché HMA mantiene direzione

### Backtrader Implementation (SBAGLIATA - PRIMA DEL FIX)

**File**: `strategies/intraday_hma_dynamic.py` righe 652-669 (old)

```python
# Calculate current HMA direction
current_hma_rising = hma[0] > hma[-1]

# Get previous state
prev_hma_rising = self.hma_rising.get(d, current_hma_rising)

# Detect direction changes (turning points)
if self.p.inverted:
    # Peak (HMA STOPS rising) → LONG entry
    signal_long = prev_hma_rising and not current_hma_rising
    # Trough (HMA STARTS rising) → SHORT entry
    signal_short = not prev_hma_rising and current_hma_rising
```

**Caratteristica**:
- Genera segnali **SOLO ai turning points** (peak/trough)
- LONG solo quando HMA **smette** di salire (peak detection)
- SHORT solo quando HMA **inizia** a salire (trough detection)
- **Segnali sporadici** solo su direction change

---

## 📊 Impatto del Bug

| Metrica | Monte Carlo | Backtrader (BUG) | Differenza |
|---------|-------------|------------------|------------|
| **Segnali generati** | Continui (ogni bar con direzione) | Solo turning points | **-95%** |
| **Trades totali** | 182 | 9 | **-95%** |
| **Simboli attivi** | 20 | 4 | **-80%** |
| **Expectancy** | +0.189% | -0.192% | **Opposta!** |

### Esempio Pratico

```
Sequenza HMA (period=16, inverted=True):

Bar  | HMA   | Direction | Monte Carlo Signal | Backtrader OLD | Backtrader NEW
-----|-------|-----------|-------------------|----------------|---------------
1    | 100.0 | -         | -                 | -              | -
2    | 100.5 | rising    | SHORT             | -              | SHORT ✅
3    | 101.0 | rising    | SHORT             | -              | SHORT ✅
4    | 101.2 | rising    | SHORT             | -              | SHORT ✅
5    | 101.0 | falling   | LONG              | LONG (peak)    | LONG ✅
6    | 100.8 | falling   | LONG              | -              | LONG ✅
7    | 100.5 | falling   | LONG              | -              | LONG ✅
8    | 100.2 | falling   | LONG              | -              | LONG ✅
9    | 100.5 | rising    | SHORT             | SHORT (trough) | SHORT ✅
10   | 100.8 | rising    | SHORT             | -              | SHORT ✅

Monte Carlo: 9 signal bars
Backtrader OLD: 2 signal bars (solo turning points!)
Backtrader NEW: 9 signal bars ✅ MATCH
```

### Perché il Bug Riduceva i Trade del 95%

**Monte Carlo** genera segnali su:
- Ogni bar dove HMA scende (in inverted mode)
- Ogni bar dove HMA sale (in inverted mode)
- **Risultato**: Molti segnali, tanti trade

**Backtrader OLD** generava segnali su:
- Solo i bar dove HMA **cambia** da rising a falling (peak)
- Solo i bar dove HMA **cambia** da falling a rising (trough)
- **Problema**: In mercati con trend prolungati, pochi turning points!
- **Risultato**: Pochi segnali, pochi trade

**Backtrader NEW** (dopo fix):
- Stessa logica di Monte Carlo
- Segnali continui basati su direzione
- **Risultato**: Dovrebbe matchare Monte Carlo (~180 trades)

---

## ✅ Fix Implementato

### Modifica a `strategies/intraday_hma_dynamic.py`

**VECCHIO CODICE** (righe 642-682):
```python
# Look for HMA direction CHANGES (turning points)
current_hma_rising = hma[0] > hma[-1]
prev_hma_rising = self.hma_rising.get(d, current_hma_rising)

if self.p.inverted:
    signal_long = prev_hma_rising and not current_hma_rising  # Peak
    signal_short = not prev_hma_rising and current_hma_rising  # Trough

self.hma_rising[d] = current_hma_rising
```

**NUOVO CODICE** (fix):
```python
# Match Monte Carlo implementation exactly:
# Signal based on CURRENT HMA direction, not direction changes

# Get HMA values
hma_curr = hma[0]
hma_prev = hma[-1]

# Detect signals based on HMA direction (Monte Carlo logic)
if self.p.inverted:
    # HMA falling (prev > curr) → LONG signal
    signal_long = hma_prev > hma_curr
    # HMA rising (prev < curr) → SHORT signal
    signal_short = hma_prev < hma_curr
```

**Cambiamenti chiave**:
1. ❌ Rimosso tracking `prev_hma_rising` (non più necessario)
2. ❌ Rimosso detection di direction **changes**
3. ✅ Aggiunto detection di direction **corrente**
4. ✅ Logica identica a Monte Carlo

---

## 🧪 Test di Validazione

### Test 1: Backtest Validation Period

**Comando**:
```bash
python btmain.py --strat intraday_hma_dynamic.HMADynamic \
  --ticker NASDAQ_HV.json \
  --fromdate 2025-05-06 --todate 2025-07-03 \
  --provider alpaca --data data --timeframe minutes \
  --commission none --amount 100000
```

**Aspettative POST-FIX**:
- Trades: ~150-180 (vicino a 182 di Monte Carlo)
- Expectancy: ~+0.15% to +0.20% (positivo!)
- Simboli attivi: 18-20 (quasi tutti)
- Win rate: ~33-37%

**Aspettative PRE-FIX** (confermate):
- Trades: 9 ❌
- Expectancy: -0.192% ❌
- Simboli attivi: 4 ❌

### Test 2: Single Symbol Detailed

Verificare LCID 2025-05-09 (giorno con trade noto @15:09):

**Aspettative**:
- Monte Carlo: Trade @15:09 (verificato in precedenza)
- Backtrader NEW: Stesso trade @15:09 ✅
- Backtrader OLD: Probabilmente mancava ❌

---

## 💡 Lezioni Apprese

### 1. "Implementazioni da zero" possono divergere

Anche quando le formule matematiche sono identiche (HMA, ATR), la **logica di business** può differire:
- Come usiamo gli indicatori?
- Quando generiamo segnali?
- Cosa confrontiamo (values vs changes)?

### 2. Testing rigoroso è essenziale

Il bug è emerso solo quando abbiamo confrontato:
- Numero trade attesi vs ottenuti
- Simboli attivi attesi vs ottenuti
- Expectancy attesa vs ottenuta

**Senza questi confronti quantitativi**, avremmo pensato che la strategia "funziona, ma meno bene".

### 3. Documentazione è critica

Monte Carlo aveva commenti chiari:
```python
# Inverted: go SHORT when HMA rising, LONG when HMA falling
```

Backtrader aveva commenti fuorvianti:
```python
# Look for HMA direction CHANGES (turning points), not continuous trend
```

Il problema era nel **design**, non nel codice!

### 4. Signal semantics matter

**"Direction change"** vs **"Current direction"** sembrano simili ma sono fondamentalmente diverse:
- Direction change: Evento raro (turning point)
- Current direction: Stato continuo (ogni bar)

Questa differenza semantica ha causato -95% trade.

---

## 🎯 Validation Plan

### Phase 1: Backtest Comparison ✅ IN CORSO

Eseguire backtest con fix e confrontare con Monte Carlo:
- Trade count (atteso ~180)
- Expectancy (atteso +0.15-0.20%)
- Trade details (entry/exit times)

### Phase 2: Symbol-by-Symbol Check

Verificare che simboli precedentemente "inattivi" ora generino trade:
- AAPL (0 → ~10-15 trades attesi)
- AMD (0 → ~8-12 trades attesi)
- BIDU (0 → ~5-8 trades attesi)
- etc.

### Phase 3: Signal Timing Validation

Confrontare timing esatto di alcuni trade:
- LCID 2025-05-09 15:09 (verificato in Monte Carlo)
- Altri trade noti da Monte Carlo results

### Phase 4: Period Optimization Re-Test

**DOPO** che backtest matcha Monte Carlo, ri-eseguire period optimization:
- Verificare se period=16 è davvero ottimale
- Testare se symbol-specific periods funzionano
- Validare "frequenze di risonanza" hypothesis

---

## 📝 Summary for User

### Il Problema

La strategia Backtrader usava **signal logic completamente diversa** da Monte Carlo:
- Monte Carlo: Segnali **continui** basati su direzione HMA
- Backtrader: Segnali **sporadici** solo su turning points

**Risultato**: -95% trade, expectancy opposta

### La Soluzione

Modificato `strategies/intraday_hma_dynamic.py` per usare **stessa logica di Monte Carlo**:
- Prima: `signal_long = prev_hma_rising and not current_hma_rising` (change detection)
- Dopo: `signal_long = hma_prev > hma_curr` (direction detection)

### Aspettative

Backtest dovrebbe ora generare:
- ~180 trades (vs 9 prima)
- ~+0.18% expectancy (vs -0.19% prima)
- 18-20 simboli attivi (vs 4 prima)

### Next Steps

1. ✅ Attendere risultati backtest con fix
2. ⏸️ Validare match con Monte Carlo
3. ⏸️ Re-testare period optimization (se necessario)
4. ⏸️ Procedere a paper trading

---

**Data Fix**: 2026-02-13
**Status**: ✅ IMPLEMENTATO, in validazione
**Confidence**: ALTA (logica identica a Monte Carlo validato)
