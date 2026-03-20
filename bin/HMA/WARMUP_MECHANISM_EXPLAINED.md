# Warm-up Mechanism - Spiegazione Completa

## Come Funziona il Warm-up in Paper Trading

### Overview

Quando avvii una strategia in **paper trading**, il sistema deve caricare un numero minimo di barre storiche (`minperiod`) **prima** di iniziare il live trading. Questo processo è chiamato **warm-up**.

---

## Step-by-Step Process

### 1. **Strategia Dichiara Minperiod Required**

Nel file `strategies/intraday_hma_dynamic.py`:

```python
@classmethod
def required_minperiod(cls, params=None):
    """Calcola il minperiod richiesto in base ai parametri."""
    if params is None:
        params = cls.params

    # HMA richiede period barre
    hma_period = params.period if hasattr(params, 'period') else 16

    # ATR richiede atr_period barre
    atr_period = params.atr_period if hasattr(params, 'atr_period') else 14

    # Prendiamo il massimo tra HMA e ATR
    return max(hma_period, atr_period)
```

**Per HMADynamic:**
- HMA(16) richiede 16 barre
- ATR(14) richiede 14 barre
- **Minperiod = 16 barre**

---

### 2. **AlpacaLiveData Scarica Storico**

Nel file `broker/alpaca_data.py`, il metodo `_load_historical_data()`:

```python
def _load_historical_data(self):
    """Ottiene le ultime N barre complete, gestendo orari di mercato e paginazione"""

    # 1. Calcola quante barre scaricare
    limit = max(self.p.minperiod, 500) + 1  # Minimo 500 barre per sicurezza

    # 2. Calcola finestra temporale
    if self.p.timeframe is bt.TimeFrame.Minutes:
        end_dt = datetime.datetime.now(datetime.timezone.utc)
        start_dt = end_dt - datetime.timedelta(minutes=limit)
    else:
        end_dt = datetime.datetime.now(datetime.timezone.utc)
        start_dt = end_dt - datetime.timedelta(days=limit)

    # 3. Scarica barre da Alpaca API
    client = StockHistoricalDataClient(self.p.api_key, self.p.secret_key)
    request = StockBarsRequest(
        symbol_or_symbols=self.p.symbol,
        timeframe=self.alpaca_tf_table[self.p.timeframe],
        start=start_dt.isoformat(),
        end=end_dt.isoformat(),
        limit=limit,
        feed=DataFeed.SIP  # o IEX
    )
    barset = client.get_stock_bars(request)

    # 4. Prendi ultime minperiod barre
    final_bars = all_bars.tail(self.p.minperiod)

    # 5. CRITICO: Marca le barre storiche come NON-LIVE
    for idx, row in data.iterrows():
        data_point = {
            'datetime': row['timestamp'],
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'openinterest': 0,
            'live': False  # ⬅️ BARRE STORICHE = NON LIVE
        }
        self._queue.put(data_point)
```

**Importante:**
- Scarica **almeno 500 barre** (o minperiod se maggiore)
- Prende le **ultime minperiod barre** dopo ordinamento
- Ogni barra storica ha **`'live': False`**

---

### 3. **Barre Live dalla WebSocket**

Dopo aver caricato lo storico, il thread `_proxy_data_loop()` riceve barre in tempo reale:

```python
def _proxy_data_loop(self):
    """Riceve dati dal proxy e li mette in coda"""
    while self._running:
        msg = self.proxy_sub.recv_pyobj(flags=zmq.NOBLOCK)
        if msg['symbol'] == self.p.symbol:
            self._queue.put({
                'datetime': msg['data'].timestamp,
                'open': msg['data'].open,
                'high': msg['data'].high,
                'low': msg['data'].low,
                'close': msg['data'].close,
                'volume': msg['data'].volume,
                'openinterest': 0,
                'live': True  # ⬅️ BARRE LIVE = TRUE
            })
```

**Differenza chiave:**
- Barre storiche: `'live': False`
- Barre live: `'live': True`

---

### 4. **Strategia Usa il Flag `live`**

Nel file `strategies/intraday_hma_dynamic.py`:

```python
def next(self):
    """Eseguito per ogni nuova barra di ogni simbolo."""

    # Determina se broker è live e se i dati sono live
    broker_live = hasattr(self.broker, 'islive') and self.broker.islive()

    for d in self.datas:
        data_live = hasattr(d, 'islive') and d.islive() and d.lines.live[0]

        # Phase 1: Opening Analysis (09:30-10:00)
        # ESEGUE ANCHE DURANTE WARMUP per raccogliere metriche di apertura
        if self.p.opening_start <= time_decimal < self.p.opening_end:
            self.analyze_opening(d)
            continue

        # Phase 2: Capital Allocation (10:00)
        # ESEGUE ANCHE DURANTE WARMUP per allocare capitale
        if time_decimal >= self.p.opening_end and not self.allocation_done:
            self.allocate_capital()

        # NOW check warmup - SKIP TRADING durante warmup
        # - Backtest:       broker_live=False, data_live=False → TRADE
        # - Paper warmup:   broker_live=True,  data_live=False → SKIP (no ordini su storico)
        # - Paper/Live:     broker_live=True,  data_live=True  → TRADE
        if broker_live and not data_live:
            continue  # ⬅️ SKIP TRADING, ma opening analysis già eseguita

        # Phase 3: Trading Logic (10:00-15:00)
        # ... genera ordini solo su barre LIVE
```

**Logica:**
- `broker_live=True` → broker Alpaca attivo
- `data_live=False` → barre storiche (warmup)
- `data_live=True` → barre live (trading)

**Flow:**
1. **Warmup (broker_live=True, data_live=False)**:
   - Opening analysis ✅ (raccoglie dati 09:30-10:00)
   - Capital allocation ✅ (alloca capitale alle 10:00)
   - Trading ❌ (skip, nessun ordine)

2. **Live Trading (broker_live=True, data_live=True)**:
   - Opening analysis ✅
   - Capital allocation ✅
   - Trading ✅ (genera ordini)

---

## Timeline Esempio: Avvio Paper Trading alle 09:35

### Scenario: Avvio strategia **09:35 EST**

**Minperiod = 16 barre (HMA)**

### Step 1: Download Storico (09:35:00 - 09:35:05)

```
Alpaca API downloads:
- Start: 09:35 - 500 minuti = ~01:15 (stessa giornata)
- End: 09:35 (now)
- Filter: Ultime 16 barre

Barre scaricate (esempio):
09:20, 09:21, 09:22, ..., 09:34, 09:35 (16 barre)
Tutte marcate 'live': False
```

### Step 2: Warmup (09:35:05 - 09:35:15)

Backtrader processa le 16 barre storiche:

```
09:35:05 - Barra 09:20 → data_live=False → opening analysis ✅, trading ❌
09:35:06 - Barra 09:21 → data_live=False → opening analysis ✅, trading ❌
...
09:35:09 - Barra 09:30 → data_live=False → opening analysis ✅, trading ❌
09:35:10 - Barra 09:31 → data_live=False → opening analysis ✅, trading ❌
...
09:35:15 - Barra 09:35 → data_live=False → opening analysis ✅, trading ❌
```

**Risultato warmup:**
- Opening bars (09:30-09:35) **già raccolte** ✅
- HMA(16) e ATR(14) **pronti** ✅
- Nessun ordine generato ✅

### Step 3: Live Trading (09:36:00 onwards)

Dalla barra **09:36** in poi, arrivano dati dal WebSocket:

```
09:36:00 - Barra 09:36 → data_live=True → opening analysis ✅, trading ✅
09:37:00 - Barra 09:37 → data_live=True → trading ✅
...
10:00:00 - Barra 10:00 → data_live=True → capital allocation ✅, trading ✅
10:01:00 - Barra 10:01 → data_live=True → trading ✅ (prime entry possibili)
```

**Risultato live:**
- Opening metrics: **già disponibili** (raccolti durante warmup)
- Capital allocation: **eseguita alle 10:00** ✅
- Trading: **attivo dalle 10:01** ✅

---

## Caso Speciale: Riavvio Tardivo (dopo 10:00)

### Scenario: Avvio strategia **10:30 EST**

**Problem:** Periodo opening (09:30-10:00) già passato!

**Solution:** Fallback allocation

```python
def allocate_capital(self):
    """Allocate capital with fallback for missing opening metrics."""
    for d in self.datas:
        score = self.opportunity_scores.get(d)

        if score is None:
            # Opening metrics not available - use default
            self.capital_allocation[d] = self.p.alloc_medium  # 1.0×
            logger.warning(f"{d._name} No opening metrics (late start?), "
                          f"using default allocation ({self.p.alloc_medium}x)")
        elif score >= self.p.opening_high_threshold:
            self.capital_allocation[d] = self.p.alloc_high    # 1.5×
        # ...
```

**Timeline:**

```
10:30:00 - Download storico: 10:14-10:30 (16 barre)
10:30:05 - Warmup 16 barre: opening period MISSED
10:30:15 - Live barra 10:30 arriva
10:30:15 - Capital allocation: score=None → default 1.0× ⚠️
10:31:00 - Trading attivo con allocation default
```

**Impatto:**
- No dynamic allocation (tutti simboli 1.0×)
- Strategia funziona comunque ✅
- Subottimale ma sicuro

---

## Allineamento con Look-back

### Minperiod Calculation

```python
# HMADynamic richiede:
HMA(16)  → 16 barre
ATR(14)  → 14 barre

minperiod = max(16, 14) = 16 barre
```

### Download Window

```python
limit = max(minperiod, 500) + 1 = 501 barre

# Per minuti:
start = now - 501 minuti ≈ 8.3 ore fa

# Prende ultime minperiod=16 barre
final_bars = all_bars.tail(16)
```

**Sicurezza:**
- Scarica **501 barre** (molto più di necessario)
- Prende solo **ultime 16**
- Se mercato chiuso nei weekend, pagina automaticamente indietro

---

## Quanto Aspettare in Paper Trading?

### Short Answer: **10-15 secondi**

### Breakdown:

1. **Download storico**: 2-5 secondi
   - Alpaca API response time
   - 16 barre = richiesta molto piccola

2. **Warmup processing**: 5-10 secondi
   - Backtrader processa 16 barre
   - Calcola HMA(16), ATR(14)
   - Esegue opening analysis (se periodo corretto)
   - Esegue capital allocation (alle 10:00)

3. **Live trading ready**: Barra successiva
   - Quando arriva prima barra con `live=True`
   - Strategia inizia a generare ordini

**Log evidence:**

```
2026-02-12 09:35:00 | INFO | Caricamento storico per AAPL - richieste 16 barre
2026-02-12 09:35:03 | INFO | Barre ottenute: 16 (da 09:20 a 09:35)
2026-02-12 09:35:03 | INFO | Storico iniziale caricato con successo. Totale barre: 16
2026-02-12 09:35:15 | INFO | Opening analysis collected: AAPL, bars=6, opening_range=0.0042
2026-02-12 10:00:00 | INFO | Capital allocated for 28 symbols
2026-02-12 10:01:00 | INFO | AAPL LONG: size=145, price=150.23, allocation=1.5×
```

---

## Difference: Backtest vs Paper Trading

### Backtest

```python
# Load data from CSV files
cerebro.adddata(bt.feeds.GenericCSVData(...))

# All bars have:
data_live = False (always)
broker_live = False

# Result:
- No warm-up needed
- All bars are processed
- Trading logic runs on ALL bars
```

### Paper Trading

```python
# Load data from Alpaca live feed
cerebro.adddata(AlpacaLiveData(..., minperiod=16))

# Historical bars (warm-up):
data_live = False
broker_live = True
→ Opening analysis ✅, Capital allocation ✅, Trading ❌

# Live bars:
data_live = True
broker_live = True
→ Opening analysis ✅, Capital allocation ✅, Trading ✅
```

---

## Summary

### ✅ Warm-up Mechanism

1. **AlpacaLiveData** scarica `max(minperiod, 500)` barre da API
2. Marca storico con **`'live': False`**
3. Marca live stream con **`'live': True`**
4. Strategia **skippa trading** durante warmup (`broker_live=True, data_live=False`)
5. **Opening analysis e capital allocation** eseguono ANCHE durante warmup

### ✅ Download Window

- Minuti: `now - (minperiod + buffer)` minuti
- Default: 501 barre (sicurezza)
- Prende ultime `minperiod` barre

### ✅ Alignment con Look-back

- HMADynamic: minperiod = 16 barre
- Alpaca scarica almeno 16 barre
- Indicatori pronti alla prima barra live

### ✅ Timing

- **Download**: 2-5 secondi
- **Warmup**: 5-10 secondi
- **Total**: 10-15 secondi fino al primo trade possibile

### ⚠️ Late Restart

- Se avvio dopo 10:00 → no opening metrics
- Fallback: allocation = 1.0× (default)
- Strategia funziona, ma subottimale

---

## Next Steps

1. ✅ Warm-up mechanism spiegato
2. 🔜 Fix timezone usando calendario Alpaca
3. 🔜 Test backtest completo
4. 🔜 Confronto backtest vs Monte Carlo
5. 🔜 Paper trading test 30 giorni
