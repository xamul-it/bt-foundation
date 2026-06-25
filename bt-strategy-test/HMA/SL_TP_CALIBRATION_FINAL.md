# HMA intraday — Calibrazione SL e TP: conclusioni finali

**Strategia:** `intraday.HMA`
**Parametri di riferimento:** `period=8`, `exitbar=6`, `inverted=True`, `use_calendar=True`
**Universo:** HMA_TOP9 (KHC, PLTR, CSX, KDP, INTC, CSCO, NFLX, WBD, CMCSA)
**Periodo analizzato:** 2025-01-01 → 2025-06-01 (~5 mesi, 22.052 trade BT)
**Run di riferimento BT:** `34223603731` — PNL +62.8%, SQN 8.095, cash €200.000

---

## Risultato BT senza SL/TP

| Metrica | Valore |
|---------|--------|
| Return totale | +62.8% |
| Trade totali | 22.052 |
| SQN | 8.095 |
| Media daily return | +0.43%/giorno |
| Positive days | 73.7% (84/114) |
| Mean exit stimato (BT) | ~+0.040%/trade |

---

## 1. SL — Stop Loss come paracadute operativo

### Parametro da usare

```
sl_pct = 0.020   # 2.0% — livello operativo consigliato
sl_pct = 0.025   # 2.5% — alternativa per simboli ad alta volatilità (PLTR, NFLX)
```

### Filosofia

Lo SL **non serve per migliorare il backtest**. Serve come rete di sicurezza per il live,
nei casi in cui l'ordine di chiusura naturale non si esegue:
- Gap di mercato improvviso
- Halt su una notizia
- Latenza/errore nella piattaforma
- Ordine limit di chiusura rimasto appeso per molte barre

### Perché NON usare i percentili MAE (p95/p99)

Il MAE study (finestra fissa `[entry : entry+exitbar]`) ha prodotto:

| Percentile | SL dist | Trade stoppati | di cui perdenti |
|-----------|---------|----------------|-----------------|
| p90 | 0.38% | 10% | ~89% |
| p95 | 0.57% | 5% | ~93% |
| p97 | 0.73% | 3% | ~95% |
| p99 | 1.15% | 1% | ~99% |

Tuttavia, simulando questi livelli in BT:
- **SL a p95 (0.57%) → -93% P&L** rispetto al no-SL
- **SL a p99 (1.15%) → -68% P&L** rispetto al no-SL

**Motivo strutturale:** la strategia è mean-reversion. I trade più profittevoli fanno spesso
un'escursione avversa profonda *prima* di recuperare. Uno SL stretto intercetta esattamente
quei trade — li chiude in perdita invece di lasciarli diventare grandi winner.
La calibrazione da percentile MAE su trade completati NON cattura questo effetto perché
non simula l'effetto domino (SL triggerato → nuova entry → altro potenziale SL → ...).

### Come è stato scelto il livello 2%

Non dal MAE dei trade normali, ma dalla domanda operativa:
> *"Se rimango bloccato con una posizione aperta per 20-30 barre con un trend forte contro,
> qual è la perdita massima accettabile?"*

- KHC (~$35): 2% = $0.70 per azione — accettabile
- PLTR (~$80): 2% = $1.60 per azione — accettabile
- NFLX (~$900): 2% = $18 per azione — accettabile, ma valutare 2.5%

Il livello 2% è sufficientemente sopra il rumore del trade normale (MAE p99 = 1.15%)
da non stoppare i trade legittimi, ma abbastanza stretto da limitare il danno
nel caso catastrofico.

In backtest il costo è minimo: quasi nessun trade normale raggiunge il 2%.

---

## 2. TP — Take Profit: non consigliato

### Conclusione

**`tp_pct = 0.0` (disabilitato)** — il TP non è giustificato su HMA_TOP9 con i parametri correnti.

### Analisi MFE ("crema sul latte")

Il TP è stato studiato con l'approccio "crema sul latte": TP come meccanismo *aggiuntivo*,
non sostitutivo dell'uscita naturale. Il TP scatta solo se il prezzo raggiunge T
*prima* della exit naturale.

**MFE window** = `max(high[entry : natural_exit_bar])` — finestra fino alla exit reale,
non a exitbar fisso.

Risultati del tp_study su HMA_TOP9 (Pandas engine):

| Symbol | mean_exit (Pandas) | MFE_p50 | best_tp | P(hit) | net_gain/trade |
|--------|-------------------|---------|---------|--------|----------------|
| KHC    | -0.010% | 0.038% | 0.030% | 0.81 | +0.013% |
| PLTR   | -0.045% | 0.150% | 0.020% | 0.92 | +0.037% |
| CSX    | -0.006% | 0.037% | 0.030% | 0.81 | +0.010% |
| KDP    | -0.010% | 0.033% | 0.027% | 0.81 | +0.010% |
| INTC   | -0.025% | 0.101% | 0.020% | 0.93 | +0.026% |
| CSCO   | -0.008% | 0.047% | 0.014% | 0.88 | +0.006% |
| NFLX   | -0.007% | 0.076% | 0.050% | 0.66 | +0.006% |
| WBD    | -0.017% | 0.119% | 0.040% | 0.91 | +0.027% |
| CMCSA  | -0.008% | 0.061% | 0.030% | 0.80 | +0.012% |

Net gain medio pesato: **+0.016%/trade** (su ~25K trade Pandas)

### Perché il TP non è comunque consigliato

I valori del tp_study sono calcolati con il Pandas engine, che usa `close[T_exit]` come
prezzo di uscita. Il BT usa ordini **Limit al `close[T_exit]`**, con fill a `open[T+1]`
(o al limit price se non c'è gap). Il modello di fill BT è sistematicamente migliore:

- Entry Buy Limit: fill a `min(open[T+1], close[T])` — uguale o meglio di Pandas
- Entry Sell Limit: fill a `max(open[T+1], close[T])` — uguale o meglio
- Close Sell Limit: se `open[T+1]` gap su → fill a `open[T+1]` > `close[T]`
- Close Buy Limit: se `open[T+1]` gap giù → fill a `open[T+1]` < `close[T]`

Effetto: **BT ottiene mediamente +0.055%/trade in più** rispetto al Pandas engine.

| | Mean exit |
|--|--|
| Pandas | -0.015%/trade |
| BT (run di riferimento) | +0.040%/trade |
| Gap | +0.055%/trade |

**Implicazione per il TP:** il floor "crema sul latte" è `T > +0.040%` (media BT reale),
non `T > -0.015%`. I livelli ottimali trovati dal tp_study (0.014–0.050%) sono tutti
sotto o al limite del floor BT:

| Symbol | best_tp | Floor BT stimato | Giudizio |
|--------|---------|-----------------|---------|
| KHC    | 0.030% | ~0.040% | sotto il floor → no TP |
| PLTR   | 0.020% | ~0.010%* | sopra il floor? |
| CSX    | 0.030% | ~0.040% | sotto il floor |
| KDP    | 0.027% | ~0.040% | sotto il floor |
| INTC   | 0.020% | ~0.030% | sotto il floor |
| CSCO   | 0.014% | ~0.040% | sotto il floor |
| NFLX   | 0.050% | ~0.048% | marginale |
| WBD    | 0.040% | ~0.038% | marginale |
| CMCSA  | 0.030% | ~0.040% | sotto il floor |

*Il floor per simbolo è stimato come `mean_exit_Pandas + 0.055%` — non misurato direttamente per simbolo.

**Conclusione:** per la maggioranza dei simboli il TP taglia trade che naturalmente
andrebbero oltre il livello TP. Solo NFLX e WBD sono candidati marginali.
Per un'analisi definitiva sul TP sarebbe necessario rieseguire il tp_study con i prezzi
di fill BT reali (da trades.json), non Pandas. Allo stato attuale la raccomandazione
è di non impostare TP.

---

## 3. Schema bracket order per il live

```
Entry:   Limit a close[T_signal]
SL:      Stop a entry_price * (1 - 0.020)   → per long
         Stop a entry_price * (1 + 0.020)   → per short
TP:      non impostato (tp_pct = 0.0)
Exit:    Limit a close[T_exit] — cancella SL quando eseguito
```

### Parametri da passare a btmain.py

```bash
python btmain.py \
  --strat intraday.HMA \
  --stratargs "period=8 inverted=True exitbar=6 use_calendar=True sl_pct=0.020 tp_pct=0.0" \
  --ticker HMA_top9.json \
  --fromdate 2025-01-01 \
  --provider alpaca \
  --timeframe minutes
```

---

## 4. Rischio operativo: non-fill del Limit di chiusura

Con exit Limit, c'è il rischio che l'ordine non si esegua (gap avverso).

Dal fill analysis su HMA_TOP9:
- **95%** dei limit di chiusura vengono eseguiti sulla barra T+1
- **98.8%** entro 15 barre dalla exit signal
- **1.2%** non-fill entro 15 barre

Il non-fill è il caso in cui lo SL diventa critico: se il Limit di chiusura non si esegue
e il mercato continua contro, lo SL (Stop order sempre attivo come bracket) chiude la
posizione al 2% di perdita massima invece di lasciarla andare indefinitamente.

---

## 5. Note metodologiche

### MAE window corretta

Il MAE va calcolato sulla **finestra fissa** `[entry_bar : entry_bar + exitbar]`,
non sulla finestra variabile `[entry_bar : natural_exit_bar]`.

Motivo: la finestra fissa rappresenta la massima esposizione possibile indipendentemente
da quando il trade esce. Se si usa la finestra variabile, i trade corti (3 barre)
hanno MAE piccoli e i trade lunghi (6 barre) MAE grandi — si confonde durata con rischio.

### MFE window corretta (per TP)

Il MFE va calcolato sulla **finestra fino alla exit naturale reale** `[entry_bar : natural_exit_bar]`,
non su exitbar fisso.

Motivo: il TP deve confrontarsi con quello che la strategia cattura naturalmente.
Se la exit naturale è a barra 3 e il picco MFE è a barra 5, la exit naturale non lo
cattura comunque — il TP non può aiutare. Usare exitbar fisso gonfia artificialmente
il MFE e sovrastima il beneficio del TP.

### Pandas engine vs BT: differenza sistematica

Il Pandas engine è utile per sweep veloci e Monte Carlo, ma sottostima il P&L
rispetto a BT di ~+0.055%/trade a causa del modello di fill semplificato.
**Non usare i valori assoluti del Pandas engine per decisioni di sizing o benchmark.**
Usarlo solo per confronti relativi (periodo A vs periodo B, parametro X vs Y).
