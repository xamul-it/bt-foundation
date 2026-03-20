# Status Finale: Period Optimization & Signal Logic Fix

**Data**: 2026-02-14
**Status**: 🟡 PARZIALMENTE RISOLTO

---

## 🎯 Domanda Originale Utente

> "Il movimento è talmente statisticamente riproducibile per alcuni asset che non sembra una normale micro-exhaustion. È particolare che si basi su un periodo di 16. Mi chiedevo quale concretamente può essere il motivo. **Sistemi automatici di trading?** Inoltre mi chiedevo se **altri simboli sono sensibili ad altri intervalli di lookback**. Qualcosa tipo **frequenza di sintonizzazione della radio**, se così fosse potremmo trovare il periodo migliore per ciascun simbolo verificando con quale periodo ha dato il miglior risultato in una finestra del mese precedente."

---

## ✅ Problema 1: Signal Detection Logic - RISOLTO

### Root Cause
**Monte Carlo** e **Backtrader** usavano logiche completamente diverse:

**Monte Carlo** (CORRETTA - 182 trades):
```python
# Segnali CONTINUI basati su direzione corrente
if inverted:
    signal_long = hma_prev > hma_curr   # HMA falling → LONG
    signal_short = hma_prev < hma_curr  # HMA rising → SHORT
```

**Backtrader** (SBAGLIATA - 9 trades):
```python
# Segnali SOLO ai turning points (peak/trough)
signal_long = prev_rising and not current_rising  # Solo peak!
signal_short = not prev_rising and current_rising  # Solo trough!
```

### Fix Implementato
**File**: `strategies/intraday_hma_dynamic.py` righe 642-677

**Vecchio codice**:
```python
current_hma_rising = hma[0] > hma[-1]
prev_hma_rising = self.hma_rising.get(d, current_hma_rising)

if self.p.inverted:
    signal_long = prev_hma_rising and not current_hma_rising
    signal_short = not prev_hma_rising and current_hma_rising
```

**Nuovo codice**:
```python
hma_curr = hma[0]
hma_prev = hma[-1]

if self.p.inverted:
    signal_long = hma_prev > hma_curr   # HMA falling
    signal_short = hma_prev < hma_curr  # HMA rising
```

### Risultato
- Trades: 9 → 25 (+178%)
- Ma ancora lontano da 182 attesi!

---

## ⚠️ Problema 2: ATR Filter Troppo Restrittivo - IDENTIFICATO

### Scoperta Critica

Debug su **LCID 2025-05-09** (giorno con trade noto @15:09):

```
15:00-15:02: ATR 0.64-0.69% → FAIL (sotto 0.7%)
15:03-15:11: ATR 0.70-0.81% → PASS ✅ (8 minuti!)
15:12-20:00: ATR 0.40-0.65% → FAIL (resto della giornata!)
```

**Solo 8 minuti su 300** passano il filtro ATR ≥ 0.7%!

### Impatto

Anche con signal logic corretta:
- 95% dei segnali generati vengono **bloccati dal filtro ATR**
- ATR scende rapidamente dopo l'apertura del mercato
- Pochi minuti ad alta volatilità → pochi trade

### Possibili Cause

**Ipotesi 1**: Monte Carlo usa ATR filter **diversamente**
- Forse filtra simboli a livello **giornaliero** (ATR medio giorno ≥ 0.7%)
- Non a livello **per-bar** (ATR istantaneo ≥ 0.7%)

**Ipotesi 2**: Soglia ATR troppo alta
- 0.7% è troppo restrittiva per intraday
- Ridurre a 0.5% o 0.4% potrebbe aumentare opportunità

**Ipotesi 3**: Validation period diverso
- Maggio-Luglio 2025 potrebbe avere volatilità più bassa del previsto
- Training period aveva più bars con ATR alto

---

## ❌ Problema 3: Solo 1 Simbolo Trading - NON RISOLTO

### Osservazione

Backtest validation period (2025-05-06 to 2025-07-03):
- **Atteso**: 19-20 simboli attivi (come Monte Carlo)
- **Ottenuto**: Solo **NFLX** (1 simbolo)
- **Trades NFLX**: 25
- **Trades altri simboli**: 0

### Monte Carlo Distribution (reference)

```
LCID:  57 trades
PTON:  36 trades
SMCI:  27 trades
ROKU:  11 trades
RIVN:  11 trades
DDOG:   6 trades
CRWD:   5 trades
TSLA:   4 trades
Altri: 1-3 trades each
```

### Possibili Cause

1. **Data missing**: Altri simboli non hanno dati completi nel periodo
2. **ATR filter**: Altri simboli non raggiungono mai ATR ≥ 0.7% nel periodo
3. **Bug multi-symbol**: Problema nella gestione multi-ticker di Backtrader

---

## ❓ Period Optimization - NON TESTABILE

### Status

**NON POSSIAMO RISPONDERE** alla domanda "frequenze di risonanza" perché:

1. ❌ Backtest non replica Monte Carlo (25 vs 182 trades)
2. ❌ Solo 1 simbolo attivo vs 19 attesi
3. ❌ ATR filter blocca 95% dei segnali
4. ❌ Sample statisticamente insignificante

### Risultati Period Test (Con Bug ATR)

Test eseguito su validation period **con ATR filter attivo**:

| Period | Total Trades | Expectancy | Note |
|--------|--------------|------------|------|
| **10** | 19 | +0.090% | Migliore (ma sample piccolo) |
| **8** | 28 | +0.068% | Più trade, expectancy ok |
| 12 | 15 | -0.024% | Negativo |
| **16** | 9 | **-0.192%** | 🔴 7°/9 (PESSIMO!) |
| Altri | 6-12 | Negativi | Tutti sotto period 16 |

**ATTENZIONE**: Questi risultati **NON sono attendibili** perché:
- Sample troppo piccolo (9-28 trades)
- ATR filter distorce risultati
- Solo 4 simboli hanno generato trade (LCID, TSLA, PTON, SMCI)
- 16 simboli con ZERO trade

### Adaptive Period - FALLITO

Test: Ottimizza period su 30 giorni training, applica su validation

**Risultato**:
- Adaptive: -0.370% expectancy ❌
- Fixed(16): -0.192% expectancy ❌
- **Adaptive PEGGIORA del 92%!**

**Conclusione**: Overfitting totale su training period.

---

## 🔬 Analisi Root Cause Completa

### 1. Signal Logic ✅ FIXATO

**Prima**: Direction-change detection → Solo turning points
**Dopo**: Continuous direction → Segnali ogni bar

**Impatto**: +178% trade count (9 → 25)

### 2. ATR Filter ⚠️ CRITICO

**Problema**: Filter troppo restrittivo, blocca 95% segnali

**Evidenza**:
```python
# LCID 2025-05-09
Bars totali intraday: ~330 (15:00-20:00 UTC)
Bars con ATR ≥ 0.7%: ~8 (solo 2.4%!)
Segnali HMA generati: ~100+
Segnali che passano ATR: ~8
```

**Da verificare**: Monte Carlo usa ATR filter allo stesso modo?

### 3. Multi-Symbol Issue ❓ DA INVESTIGARE

Solo NFLX trading, altri 19 simboli silenziosi.

**Possibili cause**:
- Data quality (missing bars?)
- ATR filter estremo (altri simboli mai sopra 0.7%?)
- Backtrader multi-ticker bug?

---

## 📋 Action Items URGENTI

### Priority 1: Verificare ATR Filter Logic Monte Carlo

**Task**: Analizzare esattamente QUANDO Monte Carlo applica filtro ATR

**Domande**:
1. Filtra ogni singolo bar (come Backtrader)?
2. Filtra simboli a livello giornaliero?
3. Usa soglia diversa da 0.7%?

**Comando di test**:
```python
# In bin/HMA/06-hma_limit_order_optimization.py
# Aggiungere print per verificare quanti bar passano filter

if not apply_filters(current_bar, filters):
    # Log: quanti bar vengono filtrati?
    continue
```

### Priority 2: Test con ATR Filter Ridotto

**Opzione A**: ATR ≥ 0.5% (invece di 0.7%)
**Opzione B**: ATR ≥ 0.4%
**Opzione C**: Rimuovere completamente per confronto

**Comando**:
```bash
# Modificare parametro in strategia o tramite stratargs
python btmain.py --strat intraday_hma_dynamic.HMADynamic \
  --stratargs "atr_min=0.005" \  # 0.5% invece di 0.007
  --ticker NASDAQ_HV.json \
  --fromdate 2025-05-06 --todate 2025-07-03 \
  --provider alpaca --data data --timeframe minutes \
  --commission none --amount 100000
```

### Priority 3: Investigare Multi-Symbol Issue

**Test**: Verificare se altri simboli hanno dati

**Comando**:
```bash
for symbol in LCID PTON SMCI TSLA; do
    echo "=== $symbol ==="
    wc -l config/data/m/alpaca/$symbol.csv
    head -2 config/data/m/alpaca/$symbol.csv
done
```

### Priority 4: SOLO DOPO FIX - Re-test Period Optimization

**Prerequisiti**:
1. ✅ ATR filter corretto
2. ✅ Multi-symbol funzionante
3. ✅ Trade count ~180

**Poi**:
- Re-eseguire `analyze_optimal_period_per_symbol.py`
- Verificare se period=16 è davvero ottimale
- Testare "frequenze di risonanza" hypothesis

---

## 💡 Risposta alla Domanda Utente

### "Perché period=16 funziona?"

**NON POSSIAMO ancora rispondere definitivamente** perché il backtest è ancora rotto (ATR filter + multi-symbol issues).

**Ipotesi teoriche** (da validare dopo fix):

1. **Market Microstructure**:
   - 16 bars × 1 min = 16 minuti
   - Allineato a cicli Market Maker rebalancing (15-20 min)
   - Allineato a HFT mean-reversion windows

2. **Mathematical Properties**:
   - sqrt(16) = 4 (quadrato perfetto!)
   - No rounding in HMA calculation
   - Lag ottimale (~2-3 bars)

3. **Institutional Order Execution**:
   - VWAP/TWAP orders eseguiti in 15-30 min
   - 16 min cattura fine di questi cicli

### "Simboli hanno periodi specifici (frequenze)?"

**TESTATO MA NON CONCLUSIVO** a causa dei bug.

**Risultati (con bug)**:
- Period 8-10 migliori di 16 (sample piccolo)
- Adaptive optimization FALLITA (overfitting)

**Da ri-testare DOPO fix ATR/multi-symbol**.

---

## 📊 Metriche Comparazione

| Aspetto | Monte Carlo | Backtrader PRE-fix | Backtrader POST-fix | Target |
|---------|-------------|-------------------|---------------------|--------|
| **Signal Logic** | Continuous | Turning points ❌ | Continuous ✅ | ✅ Match |
| **Trades** | 182 | 9 | 25 | 182 |
| **Simboli attivi** | 19 | 4 | 1 (NFLX) | 19 |
| **Expectancy** | +0.189% | -0.192% | -1.3% | +0.189% |
| **ATR filter** | ≥0.7% (?) | ≥0.7% | ≥0.7% | Da verificare |

---

## 📁 Files Rilevanti

**Fix Implementati**:
- `strategies/intraday_hma_dynamic.py` - Signal logic FIXATA

**Analisi**:
- `bin/HMA/debug_backtrader_signals.py` - Debug script ATR filter
- `bin/HMA/analyze_optimal_period_per_symbol.py` - Period optimization (con bug)

**Risultati**:
- `bin/HMA/period_optimization_analysis/` - Risultati period test (non attendibili)

**Documentazione**:
- `bin/HMA/CRITICAL_BUG_FIX_SIGNAL_LOGIC.md` - Fix signal detection
- `bin/HMA/PERIOD_OPTIMIZATION_HYPOTHESIS.md` - Ipotesi teoriche
- `bin/HMA/PERIOD_OPTIMIZATION_CRITICAL_FINDINGS.md` - Bug ATR identificato
- `bin/HMA/FINAL_STATUS_PERIOD_OPTIMIZATION.md` - Questo documento

---

## 🎯 Conclusioni

### Cosa Abbiamo Risolto ✅

1. **Signal detection logic** completamente riscritta per matchare Monte Carlo
2. **Identificato ATR filter** come problema critico (blocca 95% segnali)
3. **Documentato** che period optimization NON è testabile finché backtest non funziona

### Cosa Resta da Fare ⚠️

1. **Verificare ATR filter logic** in Monte Carlo (bar-by-bar o daily?)
2. **Testare soglie ATR alternative** (0.5%, 0.4%, o nessuna)
3. **Investigare multi-symbol issue** (perché solo NFLX trade?)
4. **DOPO fix**: Re-test period optimization e "frequenze di risonanza"

### Raccomandazione Immediata 🚀

**Opzione consigliata**: Test rapido con ATR filter ridotto

```bash
python btmain.py --strat intraday_hma_dynamic.HMADynamic \
  --stratargs "atr_min=0.004" \
  --ticker NASDAQ_HV.json \
  --fromdate 2025-05-06 --todate 2025-05-09 \
  --provider alpaca --data data --timeframe minutes \
  --commission none --amount 100000 --log_trades
```

**Aspettativa**: Se ATR 0.4% è corretto, dovremmo vedere:
- Più trade (50-100 invece di 8)
- Più simboli attivi (10+ invece di 1)
- Expectancy vicino a +0.19% (se tutto va bene)

Se questo funziona, allora **la domanda period optimization diventa testabile**!

---

**Status Finale**: 🟡 **PARZIALMENTE RISOLTO**
- ✅ 1/3 problemi fixati (signal logic)
- ⚠️ 2/3 problemi identificati ma non risolti (ATR filter, multi-symbol)
- ❓ Domanda originale su "frequenze" non ancora rispondibile

**Prossimo Passo**: Decisione utente su come procedere con ATR filter.
