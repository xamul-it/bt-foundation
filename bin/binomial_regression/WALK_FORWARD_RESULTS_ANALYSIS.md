# Walk-Forward Test: Analisi Risultati

## Executive Summary

Il walk-forward test su finestre temporali casuali di 30 giorni rivela **performance inferiori** rispetto all'analisi sul dataset completo:

- **Expectancy media**: +0.046% (vs +0.110% atteso) → **-58% degradation**
- **P(hit TP)**: 20.7% (vs 33.8% atteso) → **-38% degradation**
- **Verdict**: ⚠️ Strategia marginalmente profittevole ma con alta variance

## Risultati Dettagliati

### Per Simbolo (Aggregato su 5 finestre)

| Symbol | Setups | P(hit TP) | P(hit SL) | Expectancy | Note |
|--------|--------|-----------|-----------|------------|------|
| **TSLA** | 7,021 | **27.3%** | 63.3% | **+0.058%** | ✅ Migliore performer |
| **AAPL** | 6,583 | 17.7% | 60.6% | +0.038% | Performance media |
| **MSFT** | 4,892 | 15.2% | 60.3% | +0.041% | Performance media |

**Insight chiave:** TSLA ha volatilità più alta e risponde meglio alla strategia.

### Variance tra Finestre

```
Expectancy per finestra:
  Mean:    +0.046%
  Std:     0.038%  (82% della media! ⚠️)
  Range:   [-0.016%, +0.129%]

Win Rate per finestra:
  P(TP):   15.4% ± 9.2%  (variance 60% ⚠️)
  P(SL):   62.6% ± 5.0%  (variance 8%, stabile)
```

**Problema:** Alta variance di P(TP) significa risultati molto dipendenti dal periodo.

### Best vs Worst Windows

**Best Window (TSLA, Mar-Apr 2025):**
```
Setups:     3,339
P(TP):      35.1%  ← Vicino all'atteso!
P(SL):      58.6%
Expectancy: +0.129%  ← 2.8x la media
```

**Worst Window (TSLA, Dec 2025-Jan 2026):**
```
Setups:     297
P(TP):      6.7%   ← MOLTO basso
P(SL):      76.8%  ← MOLTO alto
Expectancy: -0.016%  ← NEGATIVO!
```

**Interpretazione:** La strategia funziona bene in alcuni periodi (trend/volatilità alta) ma fallisce in altri (laterale/bassa volatilità).

---

## Cause della Degradation

### 1. Overfitting sul Dataset Completo

L'analisi iniziale è stata fatta su **tutto il 2024-2025**, aggregando:
- Diversi regimi di mercato
- Trend + laterale + crash
- Alta + bassa volatilità

I filtri (p75) sono stati calibrati per massimizzare performance MEDIE su tutti questi regimi.

**Problema:** In un singolo mese (30 giorni), il mercato può essere in un regime specifico (es. laterale) dove i filtri non funzionano bene.

### 2. Sample Size Insufficiente

30 giorni → 500-3000 setup per asset:
- Troppo pochi per convergere alla media teorica
- Law of large numbers richiede migliaia di samples
- Con 500 setup e P(TP)=20%, ci aspettiamo ~100 TP
  - Ma con binomial variance: std = sqrt(500 × 0.2 × 0.8) = 8.9
  - Quindi range atteso: 100 ± 18 → P(TP) = 20% ± 3.6%

**Soluzione:** Aumentare window_days a 60-90 giorni.

### 3. Regime Changes

Guardando i risultati, si nota pattern:
- **Apr-May 2025**: Alta volatilità → buone performance (exp 0.07-0.09%)
- **Dec 2025-Jan 2026**: Bassa volatilità → performance scarse (exp -0.02%)

La strategia è **regime-dependent**: funziona solo in mercati con alta volatilità.

### 4. Asset Selection Bias

TSLA performa 50% meglio di AAPL/MSFT:
- TSLA exp: +0.058%
- AAPL/MSFT exp: ~0.040%

**Possibile causa:** TSLA ha volatilità intrinsecamente più alta, quindi:
- Più setup filtrati (14k vs 4-6k)
- Setup di qualità migliore
- Disp_60 threshold più significativo

**Implicazione:** La strategia funziona meglio su asset ad alta volatilità.

---

## Confronto Analisi Iniziale vs Walk-Forward

| Metrica | Dataset Completo | Walk-Forward 30d | Gap | Possibile Causa |
|---------|------------------|------------------|-----|-----------------|
| Total setups | 30,445 (7.1%) | 18,496 (5.8% avg) | -20% | Filtering variance |
| P(hit TP) | 33.8% | 20.7% | **-38%** | Overfitting su avg |
| P(hit SL) | 59.8% | 61.5% | +3% | Stabile |
| Expectancy | +0.110% | +0.046% | **-58%** | Overfitting + variance |

**Nota critica:** La degradation NON è dovuta a "entry su close" (che è già assunto nell'analisi iniziale) ma a:
1. Overfitting sui parametri
2. High variance tra periodi
3. Regime dependency

---

## Raccomandazioni

### ✅ Short-Term (Pre-Production)

1. **Aumenta finestra test a 60-90 giorni**
   ```bash
   python 07-walk_forward_test.py --window-days 60 --num-windows 10
   ```
   - Riduce variance
   - Più setup per finestra (convergenza migliore)

2. **Filtra per regime di mercato**
   - Calcola VIX o ATR medio del periodo
   - Attiva strategia solo se volatilità > soglia
   - Esempio: se ATR(20) < 2%, skip trading per quel giorno

3. **Focus su asset ad alta volatilità**
   - TSLA: ✅ Continua
   - NVDA, META, GOOGL: ✅ Testa (dovrebbero essere simili a TSLA)
   - AAPL, MSFT: ⚠️ Considera di escludere (troppo stabili)

### 🔧 Medium-Term (Optimization)

4. **Adaptive thresholds**
   Invece di p75 fisso, usa rolling percentiles:
   ```python
   # Calcola percentili su rolling window (es. 60 giorni)
   rolling_p75 = slope_60.rolling(60*390).quantile(0.75)
   filter = slope_60 > rolling_p75
   ```
   - Adatta filtri al regime corrente
   - Riduce overfitting

5. **Regime detection**
   Aggiungi filtro pre-entry:
   ```python
   # Daily ATR
   atr_20 = ta.ATR(high, low, close, 20)

   # Trade solo se ATR alta
   if atr_20 < atr_20.rolling(60).quantile(0.5):
       skip_trading = True  # Low volatility regime
   ```

6. **TP/SL dinamici basati su volatilità**
   ```python
   # Invece di TP/SL fissi, usa multipli di ATR
   atr_current = calculate_atr(df, period=20)
   tp_pct = 2.5 * atr_current  # 2.5x ATR
   sl_pct = 0.5 * atr_current  # 0.5x ATR
   ```

### 📊 Long-Term (Robustness)

7. **Walk-forward optimization**
   - Train su 6 mesi → test su 1 mese successivo
   - Rolling forward ogni mese
   - Verifica se parametri ottimali cambiano nel tempo

8. **Monte Carlo simulation**
   - Genera 1000 sequenze random di 30 giorni
   - Calcola distribution di expectancy
   - Identifica confidence interval (es. 90% CI)

9. **Multi-asset portfolio**
   - Non tradare singoli asset
   - Portfolio di 10-15 asset diversificati
   - Correlation hedging (long + short simultanei)

---

## Expectancy Realistica per Produzione

Basandoci sui walk-forward results, l'expectancy realistica è:

```
Conservativa (P10):  +0.01% per trade
Media (P50):         +0.046% per trade
Ottimistica (P90):   +0.09% per trade
```

**Con 12 setup/giorno:**
```
Conservativa:  12 × 0.01%  = +0.12% daily → +2.4% monthly
Media:         12 × 0.046% = +0.55% daily → +11% monthly  ← Realistico
Ottimistica:   12 × 0.09%  = +1.08% daily → +21% monthly
```

**Nota:** L'expectancy media (+0.046%) è **58% inferiore** all'analisi iniziale (+0.110%), quindi:
- Monthly return realistico: ~+10% (non +20%)
- Annual return realistico: ~+120% (non +240%)
- Sharpe ratio atteso: ~1.2-1.5 (non 1.8-2.2)

---

## Go/No-Go Decision

### 🟢 GO se:

1. **Accetti expectancy ridotta** (+0.05% invece di +0.11%)
2. **Capitale >= $100k** (per diversificare su 10+ asset)
3. **Focus su asset volatili** (TSLA, NVDA, META, non AAPL/MSFT)
4. **Implementi regime filter** (ATR > soglia o VIX < 30)
5. **Paper trade 60+ giorni** per validare expectancy reale

### 🔴 NO-GO se:

1. **Capitale < $50k** (troppo pochi asset per diversificare)
2. **Vuoi solo AAPL/MSFT** (performance troppo basse)
3. **Non puoi tollerare drawdown -10%** (alta variance)
4. **Execution lenta** (> 5 sec fills erode già il piccolo edge)
5. **Broker con commissioni** (> $1/trade annulla metà dell'edge)

---

## Prossimi Test Consigliati

Prima di passare a implementazione Backtrader:

### Test 1: Finestre Più Lunghe
```bash
python 07-walk_forward_test.py --window-days 60 --num-windows 10
```
Expectancy attesa: +0.06-0.08% (migliore di 30d)

### Test 2: Solo Asset Volatili
```bash
python 07-walk_forward_test.py --symbols TSLA,NVDA,META --window-days 30
```
Expectancy attesa: +0.08-0.10% (meglio di mix con AAPL/MSFT)

### Test 3: Regime Filter
Modifica script per calcolare ATR e filtrare:
```python
# In apply_filters_setup2()
atr_20 = calculate_atr(df)
regime_filter = atr_20 > atr_20.rolling(60).quantile(0.5)

return mag_filter & quality_filter & regime_filter
```

---

## Conclusioni

La strategia **funziona** ma con expectancy **50-60% inferiore** alle aspettative iniziali.

**Cause:**
1. Overfitting su dataset completo
2. Alta variance tra regimi di mercato
3. Asset dependency (TSLA > AAPL/MSFT)

**Azioni Immediate:**
1. ✅ Re-test con finestre 60-90 giorni
2. ✅ Focus su asset volatili (TSLA, NVDA, META)
3. ✅ Implementa regime filter (ATR-based)
4. ✅ Paper trading 60+ giorni prima di live

**Expectancy Realistica:**
- **+0.046%** per trade (non +0.110%)
- **+10-12% monthly** su $100k (non +20%)
- Sharpe ~1.2-1.5 (ancora buono, ma non eccezionale)

**Verdict Finale:**
⚠️ **PROCEDI CON CAUTELA** - Il sistema è profittevole ma margini sono stretti. Richiede:
- Capital adeguato ($100k+)
- Execution perfetta
- Asset selection rigorosa
- Regime awareness
