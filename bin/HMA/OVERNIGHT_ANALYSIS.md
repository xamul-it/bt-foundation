# HMA Overnight — Analisi di Applicabilità in Paper Trading

## Contesto

La strategia `intraday.HMA` con `use_calendar=True` (default in `env/psim`) chiude
tutte le posizioni entro le 16:00 e non opera fuori dall'orario di mercato.

Con `use_calendar=False` la guardia `inValidMarket()` viene bypassata: le posizioni
non vengono chiuse a fine giornata e sopravvivono al gap notturno (16:00 → 9:30 next day).

---

## Risultati a confronto (3 simboli, 2025-01-02 → 2025-05-30)

| Simbolo | Modalità       | Trade | Total P&L | Sharpe | SQN  |
|---------|----------------|------:|----------:|-------:|-----:|
| NFLX    | intraday       | 2.068 |   +62.97% |  +1.10 | 3.14 |
| NFLX    | **overnight**  | 6.228 |  +138.62% |  +1.00 | 4.96 |
| CSCO    | intraday       | 2.179 |   +95.37% |  +2.09 | 6.16 |
| CSCO    | **overnight**  | 6.398 |  +213.01% |  +1.95 | 9.81 |
| KHC     | intraday       | 2.070 |   +83.02% |  +1.88 | 5.39 |
| KHC     | **overnight**  | 6.192 |  +202.41% |  +2.14 |10.62 |

Il delta di P&L è **+75% / +118% / +119%**. Il numero di trade triplica.

---

## Perché i risultati sembrano migliori

### I trade overnight sono una minoranza…
Solo **312 trade su 18.818 (1.7%)** attraversano il confine di giornata.

### …ma portano un P&L sproporzionato
- P&L trade intraday: **+520%**
- P&L trade overnight: **+33.5%**
- Win rate overnight: **64.4%** (in linea con intraday)

### Il gap notturno vale quasi metà del P&L del trade
Il gap tra close RTH e open RTH del giorno successivo contribuisce in mediana
il **37.6% del P&L** del singolo trade overnight.

Esempi reali:
| Simbolo | Entry | Side  | P&L trade | Gap notturno | % del P&L |
|---------|-------|-------|----------:|-------------:|----------:|
| NFLX    | 04-apr | short | +4.57%   | −2.93%       | 64%       |
| CSCO    | 04-apr | short | +4.99%   | −2.93%       | 59%       |
| CSCO    | 23-mag | long  | +1.68%   | +1.68%       | 100%      |
| KHC     | 22-apr | long  | +0.35%   | +1.28%       | 363%      |

Nel caso KHC del 22 aprile il gap ha contribuito **più del trade stesso**: la
posizione in intraday sarebbe chiusa in perdita, overnight diventa profittevole
solo grazie all'apertura del giorno dopo.

---

## Il flusso live riceve barre after-hours

### Verifica sul codice (`broker/alpaca_data.py`)

```python
# riga 694
self._stream = StockDataStream(self._api_key, self._secret_key, raw_data=False)
# riga 713
self._stream.subscribe_bars(callback, symbol)
```

`StockDataStream` senza parametro `feed` esplicito usa di default **SIP**.
Il feed SIP di Alpaca include barre after-hours (16:00–20:00 ET) e pre-market
(4:00–9:30 ET) **quando c'è movimento**: se in un dato minuto avviene almeno
un trade, Alpaca emette la barra corrispondente.

**Conseguenza:** con `use_calendar=False` in paper/live, BT riceve effettivamente
barre AH e può generare segnali su di esse. Questo cambia l'analisi.

### Caratteristiche delle barre AH su Alpaca SIP
- **Volume basso**: la liquidità AH è una frazione di quella RTH
- **Spread bid/ask molto ampio**: market impact elevato su ordini limite
- **Barre sparse**: molti minuti sono vuoti, la strategia opera su dati discontinui
- **Price discovery distorto**: i prezzi AH riflettono order flow istituzionale,
  non retail, con meccanismi diversi dalla sessione normale

---

## Perché la simulazione resta incompleta

I CSV in `config-common/data/minutes/alpaca/` contengono **solo barre RTH
(9:30–16:00 ET)**. La simulazione overnight salta il periodo 16:00–9:30 senza
vedere nessuna delle barre AH/PM che il sistema live riceve.

Questo crea un'**asimmetria strutturale** tra simulazione e realtà:

| Aspetto | Simulazione overnight | Paper/live (`use_calendar=False`) |
|---------|----------------------|-----------------------------------|
| Barre 16:00–9:30 | nessuna (salto netto) | barre sparse su movimenti reali |
| Segnali AH | impossibili | generabili su volumi bassi |
| Gap 16:00→9:30 | unico salto nel P&L | frammentato in N micro-barre |
| Prezzo di fill | `open[9:30]` | può essere una barra delle 17:43 |

---

## Perché overnight NON è applicabile in paper trading (nella forma attuale)

### 1. Fill su barre AH sparse = esecuzione imprevedibile
Con `use_calendar=False`, la strategia genera ordini **limite al `close` della
barra del segnale**. In AH una barra può chiudere alle 17:43 su 200 azioni
scambiate. Il fill al prezzo di quella barra non è replicabile nella realtà
(spread ampio, nessuna liquidità). La simulazione lo tratta come fill certo.

### 2. Segnali HMA su dati sparsi sono rumore
L'HMA su serie discontinue (barre mancanti per molti minuti) produce falsi
segnali di transizione perché i gap nel dato vengono trattati come movimenti
di prezzo reali. Il periodo 16 su barre AH sparse non ha lo stesso significato
che su barre RTH continue.

### 3. Gap risk catastrofico senza stop loss
`sl_pct=0` in `env/psim`. Un gap notturno contro posizione su notizie
(earnings, macro, upgrade/downgrade) può essere −5/−10% in un minuto.
In backtest il gap appare come una singola barra 9:30 con perdita "normale".
In paper/live il conto subisce la perdita intera senza possibilità di
intervento in tempo reale.

### 4. Confronto simulazione–paper non è possibile
La simulazione usa RTH-only, il paper usa SIP con AH incluse. I due sistemi
elaborano serie temporali diverse: il confronto trade-by-trade è impossibile
e l'analisi delle divergenze non produce diagnosi affidabili.

### 5. Bias rialzista 2025
I dati coprono gennaio–maggio 2025, periodo con forte bias rialzista sul NASDAQ.
Le posizioni long overnight hanno catturato sistematicamente gap-up mattutini.
Su un campione neutro o ribassista il contributo overnight sarebbe negativo.

### 6. Margine overnight ≠ margine day-trading
Alpaca applica **Regulation T overnight margin (50% del valore nominale)**.
Con `amount=2000` su 9 simboli = $18.000 esposti overnight su un conto ~$95k.
Il requisito è rispettato oggi, ma qualsiasi drawdown riduce il buffer.

---

## Conclusione

| Domanda | Risposta |
|---------|----------|
| Alpaca manda barre AH? | **Sì**, SIP include barre sparse quando c'è movimento |
| La simulazione le include? | **No**, i CSV sono RTH-only → asimmetria strutturale |
| I risultati overnight sono reali? | Parzialmente: il premio overnight esiste, ma è sovrastimato |
| È applicabile in paper con `use_calendar=False`? | **No**, nella forma attuale |
| Può diventare applicabile? | Sì, con le modifiche sotto |

---

## Requisiti minimi per rendere overnight operativo

1. **Dati AH per la simulazione** — scaricare barre 4:00–20:00 da Alpaca SIP
   e usarle nella simulazione per allinearla al flusso live
2. **Stop loss overnight obbligatorio** — almeno `sl_pct=0.015` (1.5%) per
   assorbire gap normali senza liquidare per gap eccezionali
3. **Filtro volume AH** — non aprire nuove posizioni su barre con volume < soglia
   (es. < 1000 azioni) per evitare fill su liquidità fantasma
4. **Filtro earnings** — non aprire posizioni nelle 24h prima di un earnings
   announcement (richiede calendario earnings da API)
5. **Test su dataset esteso con AH** — includere periodi ribassisti (2022, 2020 Q1)
   e verificare che l'edge overnight sia robusto fuori dal bias 2025

---

*Analisi generata da `bin/HMA/overnight_analysis.py` — dati 2025-01-02→2025-05-30*
*Verifica codice: `broker/alpaca_data.py` riga 694 (StockDataStream SIP default)*
