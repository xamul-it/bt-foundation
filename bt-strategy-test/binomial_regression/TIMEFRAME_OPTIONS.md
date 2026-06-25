# Opzioni Timeframe per Strategia Regressione

## Situazione Attuale (TF 1m)

```
Dati:           Candele 1m
Regressione:    slope_60 = 60 candele 1m = 1 ora
Entry/Exit:     Candele 1m
TP/SL:          Valutati ogni 1 minuto
Problema:       Mean reversion dominante su 5-15 minuti
```

**Esempio finestra slope_60 @ 10:00**:
- Usa candele da 09:00 a 10:00 (60 candele 1m)
- Calcola trend su 1 ora di dati
- Entra su candela 10:01 (1m)
- TP @ +0.5% potrebbe hitare entro 5-6 minuti

---

## Opzione 1: TF 5m per Tutto ⭐ CONSIGLIATA

```
Dati:           Candele 5m
Regressione:    slope_60 = 60 candele 5m = 5 ore
Entry/Exit:     Candele 5m
TP/SL:          Valutati ogni 5 minuti
Beneficio:      Trend più persistente, meno mean reversion
```

**Esempio finestra slope_60 @ 14:00**:
- Usa candele da 09:00 a 14:00 (60 candele 5m)
- Calcola trend su 5 ore di dati (più robusto!)
- Entra su candela 14:05 (5m)
- TP @ +0.5% potrebbe hitare entro 25-30 minuti

### Come Implementare

1. **Scarica dati 5m** (se non li hai già):
   ```bash
   # Modifica 01-fast_backtest.py
   DATA_FOLDER_MIN = Path("../../config/data/5m/alpaca")
   ```

2. **Usa stessi parametri**:
   - `WINDOWS_MIN = {"long": 60, "mid": 20, "short": 5}`
   - Ma ora rappresentano finestre temporali più lunghe:
     - short: 5 candele 5m = 25 minuti
     - mid: 20 candele 5m = 100 minuti
     - long: 60 candele 5m = 5 ore

3. **Adatta TP/SL e orizzonti**:
   ```python
   # Invece di:
   ANALYSIS_HORIZONS = [5, 10, 15]  # minuti su 1m

   # Usa:
   ANALYSIS_HORIZONS = [5, 10, 15]  # candele 5m = 25, 50, 75 minuti
   ```

4. **Ricalcola tutto**:
   ```bash
   python 01-fast_backtest.py
   python 02-feature_diagnostics.py
   python 03-slice_analisys.py
   ```

### Vantaggi
- ✅ Trend più persistente (correlazioni potrebbero diventare positive)
- ✅ Meno noise nei dati
- ✅ Mean reversion meno dominante
- ✅ Regressioni calcolano su orizzonti più lunghi
- ✅ Setup più robusti

### Svantaggi
- ❌ Meno opportunità di trade (1/5 delle candele)
- ❌ Esecuzione meno precisa (granularità 5min invece di 1min)
- ❌ Possibile slippage maggiore

---

## Opzione 2: Multi-Timeframe 🔧 AVANZATA

```
Dati regressione:   Candele 5m o 15m
Segnali:            Calcolati su TF superiore
Entry/Exit:         Candele 1m
TP/SL:              Valutati ogni 1 minuto
Beneficio:          Trend robusto + esecuzione precisa
```

**Esempio workflow**:
1. Calcola slope_60 su candele 5m @ 14:00 → slope = +0.0008
2. Slope > soglia → setup valido
3. Aspetta prossima candela 1m dove condizioni sono favorevoli
4. Entra su candela 1m @ 14:01
5. Monitora TP/SL su candele 1m per esecuzione precisa

### Come Implementare

**Strategia in Backtrader**:
```python
class MultiTFStrategy(bt.Strategy):
    def __init__(self):
        # Data feed 5m per segnali
        self.data_5m = self.datas[0]  # Primary = 5m
        # Data feed 1m per esecuzione
        self.data_1m = self.datas[1]  # Secondary = 1m

        # Calcola slope su 5m
        self.slope_5m = RegressionSlope(self.data_5m.close, period=60)

    def next(self):
        # Valuta segnali su 5m
        if self.slope_5m > threshold:
            # Ma esegui su 1m per precisione
            if self.data_1m.close > entry_level:
                self.buy(data=self.data_1m)
```

### Vantaggi
- ✅ Trend robusto (calcolato su TF superiore)
- ✅ Esecuzione precisa (su TF inferiore)
- ✅ Possibile riduzione slippage
- ✅ Più controllo su entry/exit

### Svantaggi
- ❌ Implementazione complessa
- ❌ Richiede sincronizzazione tra TF
- ❌ Necessita di 2 data feeds
- ❌ Backtest più lento

---

## Opzione 3: Finestre Più Lunghe su 1m 📏 COMPROMESSO

```
Dati:           Candele 1m
Regressione:    slope_300 = 300 candele 1m = 5 ore
Entry/Exit:     Candele 1m
TP/SL:          Valutati ogni 1 minuto
Beneficio:      Catturi trend più lunghi, stessa granularità
```

**Esempio finestra slope_300 @ 14:00**:
- Usa candele da 09:00 a 14:00 (300 candele 1m)
- Calcola trend su 5 ore di dati
- Entra su candela 14:01 (1m)
- TP @ +0.5% potrebbe hitare entro 5-6 minuti

### Come Implementare

Modifica `01-fast_backtest.py`:
```python
# Invece di:
WINDOWS_MIN = {"long": 60, "mid": 20, "short": 5}

# Usa:
WINDOWS_MIN = {"long": 300, "mid": 100, "short": 20}
# long = 5 ore, mid = 1h40m, short = 20 min
```

### Vantaggi
- ✅ Catturi trend più lunghi
- ✅ Nessun cambio di dati necessario
- ✅ Stessa granularità esecuzione

### Svantaggi
- ❌ Noise rimane alto su candele 1m
- ❌ Mean reversion su orizzonti brevi (5-15 min) persiste
- ❌ Correlazioni potrebbero rimanere negative

---

## 🎯 Raccomandazione

### Per risolvere mean reversion:
**→ OPZIONE 1 (TF 5m per tutto)**

**Perché**:
- Il problema è che su 1m il prezzo si muove "troppo velocemente"
- Ogni movimento tende a invertirsi entro 5-15 minuti
- Su 5m, un movimento "short-term" dura 25-75 minuti
- Le correlazioni potrebbero diventare positive

### Se Opzione 1 non basta:
**→ Prova TF 15m**

Su 15m:
- slope_60 = 15 ore di dati (quasi 2 giorni!)
- Trend molto più robusto
- Mean reversion domina su orizzonti di 2-3 ore

### Se vuoi massima precisione:
**→ OPZIONE 2 (Multi-TF)**

Usa 15m per segnali, 1m per esecuzione

---

## 📊 Test Suggerito

Fai un quick test su TF 5m:

```bash
# 1. Crea cartella dati 5m
mkdir -p ../../config/data/5m/alpaca

# 2. Scarica/converti dati (se necessario)
# Oppure usa dati esistenti se li hai già

# 3. Modifica 01-fast_backtest.py
# DATA_FOLDER_MIN = Path("../../config/data/5m/alpaca")

# 4. Esegui
python 01-fast_backtest.py

# 5. Controlla correlazioni
python 02-feature_diagnostics.py

# 6. Verifica se slope vs high_ret_max diventa positivo
cat results/diagnostics/traditional_spearman_grid.csv
```

Se le correlazioni diventano positive → problema risolto!

---

## Confronto Numerico

| Metrica | TF 1m (ora) | TF 5m | TF 15m |
|---------|-------------|-------|--------|
| slope_60 finestra temporale | 1 ora | 5 ore | 15 ore |
| Numero candele/giorno | 390 | 78 | 26 |
| Opportunità trade | Alta | Media | Bassa |
| Noise | Alto | Medio | Basso |
| Mean reversion | Dominante | Moderata | Debole |
| Trend persistence | Bassa | Media | Alta |
| Esecuzione precision | Alta | Media | Bassa |

---

## Domande Frequenti

**Q: Devo cambiare anche TP/SL se passo a 5m?**
A: Dipende. Se TP = +0.5% rimane 0.5%. Ma ora viene valutato ogni 5 minuti invece che ogni 1 minuto.

**Q: Posso testare multi-TF senza implementare strategia complessa?**
A: Sì, puoi fare un test "manuale":
1. Calcola slope su dati 5m → salva segnali
2. Carica dati 1m → filtra solo momenti dove slope_5m > soglia
3. Valuta performance su dati 1m con filtro 5m

**Q: Qual è il TF minimo per evitare mean reversion?**
A: Dipende dall'asset. In generale:
- Liquid stocks: 5m può bastare
- Less liquid: 15m o 30m
- Crypto (24/7): anche 1h

**Q: Perdo opportunità passando a 5m?**
A: Sì, ma le opportunità sono di qualità migliore. Meglio 10 trade buoni che 50 trade con mean reversion.
