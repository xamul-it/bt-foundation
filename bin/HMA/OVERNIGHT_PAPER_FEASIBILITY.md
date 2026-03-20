# HMA After-Hours in Paper Trading — Parere dell'Agente

*Risposta generata da `claude --print` (BTPandasAgent) su richiesta di analisi di fattibilità*

---

## Fattibilità After-Hours Paper Trading — intraday.HMA

---

### 1. Verdetto Immediato

**CONDIZIONATO — tecnicamente possibile ma sconsigliato nella configurazione attuale.**
Il vantaggio overnight documentato (+33.5% P&L su 1.7% dei trade) è reale ma dipende da gap RTH che la strategia *non può catturare correttamente* in AH: le barre sparse post-16:00 corrompono il calcolo HMA period=16, rendendo i segnali AH statisticamente non affidabili e operativamente pericolosi.

---

### 2. Problemi Tecnici Concreti

| # | Problema | Impatto stimato | Priorità |
|---|----------|----------------|----------|
| 1 | **HMA period=16 su serie sparse AH** | Falsi segnali +40–60% rispetto RTH. Con buchi di 30-60 min tra barre, ogni gap è un'inversione artificiale. Su 16 barre, bastano 3-4 buchi per rendere l'HMA non stazionario. | BLOCCANTE |
| 2 | **Spread bid/ask AH su Alpaca SIP** | Slippage medio AH stimabile in 0.3–0.8% vs 0.05–0.15% RTH (5-16x peggio). Su estratto: se il contributo mediano del gap è 37.6% del P&L trade, uno slippage di 0.5% erode ~20-30% del vantaggio atteso. | BLOCCANTE |
| 3 | **Fill parziali AH** | In RTH fill parziali sovrastimano trade count del 5-15%. In AH con volumi 10-50x inferiori, il tasso di fill parziale sale a 30-60% stimato. Ogni ordine aperto a fine sessione diventa overnight involontario. | ALTO |
| 4 | **TTL ordini AH** | In RTH la logica cancella pending prima delle 15:55. In AH non c'è una sessione definita: un LIMIT emesso alle 17:30 può restare aperto fino alle 20:00 e non essere eseguito. Senza TTL esplicito per sessione AH, si accumulano ordini zombie. | ALTO |
| 5 | **Dati storici AH assenti** | CSV disponibili = solo RTH. La simulazione overnight usa gap close[15:59]→open[9:30]: questo *non è* il comportamento AH reale. Impossibile validare la strategia AH senza dati storici AH. Backtest = fiction. | BLOCCANTE per validazione |
| 6 | **Margin overnight Reg-T** | 50% margin richiesto per posizioni overnight. Paper non ha margin call, ma in live la stessa strategia richiederebbe capitale doppio per le posizioni che attraversano la notte. Impatto: riduzione del 50% della leva effettiva. | MEDIO (live) |

---

### 3. Cosa Deve Cambiare in bt-core (strategia BT)

**File principale: `bt-core/strategies/intraday.py`**

```
A) Modalità AH separata (nuovo parametro `allow_ah=False`)
   - Se allow_ah=True: non chiudere a 16:00 tramite _closeAll()
   - Aggiungere `inAHMarket()` distinto da `inValidMarket()`
   - Controllare volume minimo barra AH prima di emettere segnali:
     if bar.volume < AH_MIN_VOLUME_THRESHOLD: return  # salta barra sparse

B) HMA filtering AH
   - Aggiungere controllo continuità temporale:
     if (current_bar_time - prev_bar_time) > timedelta(minutes=5): self.hma.reset()
   - Reset dell'indicatore su gap temporale evita segnali artificiali
   - Parametro `ah_max_gap_minutes=5` (default conservativo)

C) TTL sessione AH
   - Sostituire valid=timedelta(minutes=N) con valid=datetime(day, 20, 0, 0)
     (scadenza alle 20:00 ET, fine sessione AH Alpaca)
   - In notify_order: gestire EXPIRED specificamente per AH
     (non tentare reingresso, loggare solo)

D) Close+Revert AH: nessuna inversione
   - In AH il close+revert è ancora più rischioso (liquidità bassa)
   - Aggiungere guard: if not inValidMarket() and inAHMarket(): skip_revert=True
   - Solo close, mai open nuova posizione in AH

E) Slippage AH esplicito
   - Aggiungere parametro ah_slippage_pct=0.5 (conservativo)
   - Applicato solo sulle barre 16:00–20:00
   - Utile per rendere il backtest BT più realistico
```

**Righe critiche da modificare:**
- `intraday.py` riga gestione `minutes_before_close` → estendere per AH cutoff
- `btmain.py` configurazione broker → aggiungere `slip_open=True` per AH

---

### 4. Cosa Deve Cambiare nella Simulazione Pandas

**File: `bt-agent/engine/backtest.py` + nuovi dati**

```
A) Acquisizione dati AH storici (PREREQUISITO BLOCCANTE)
   - Alpaca Markets API: bars endpoint con timeframe=1Min, extended_hours=True
   - Scaricare per NFLX/CSCO/KHC almeno 12 mesi (gen 2024 → mag 2025)
   - Salvare in data/m/alpaca/{SYMBOL}_ah.csv separato da RTH
   - Stima: 16:00–20:00 = ~240 barre/giorno teoriche, reali 20–80 per simbolo liquido

B) Gestione serie temporale discontinua
   - Attuale logica: iterazione sequenziale su barre assume continuità
   - Aggiungere: timestamp_gap = (bar.datetime - prev_bar.datetime).seconds / 60
   - Se gap > ah_max_gap_minutes: reset HMA buffer (non usare barre precedenti)

C) Simulazione spread AH
   - Aggiungere parametro ah_spread_pct (default 0.5%)
   - entry_price_ah = close * (1 + ah_spread_pct/2) per long
   - Applicare solo a barre con timestamp > 16:00 ET

D) Fill probability AH
   - Aggiungere fill_prob_ah = 0.6 (40% ordini non riempiti in AH)
   - Simulare fill parziali: actual_size = order_size * random.uniform(0.3, 1.0) se fill
   - Confrontare con RTH per quantificare la differenza

E) Metrica separata per segmento AH
   - Tracciare separatamente: trade_count_rth, trade_count_ah, pnl_rth, pnl_ah
   - Senza questa separazione è impossibile attribuire performance

F) Validazione cross-engine
   - Prima del deploy paper: eseguire comparator.py su campione AH
   - Soglie accettabili AH (più permissive): trade_count diff < 20%, PnL diff < 35%
     (vs 10%/20% RTH) per via dell'incertezza fill
```

---

### 5. Stima Sforzo Implementativo

| Componente | Complessità | Stima |
|------------|-------------|-------|
| Download + pulizia dati AH storici (3 simboli, 12 mesi) | Bassa | 0.5 giorni |
| Modifiche simulazione pandas (B+C+D+E) | Media | 2 giorni |
| Modifiche strategia BT (A+B+C+D) | Alta | 3 giorni |
| Validazione cross-engine (comparator AH) | Media | 1.5 giorni |
| Paper trading test 4 settimane | — | 4 settimane calendar |
| **Totale implementazione** | | **~7 giorni dev** |
| **Totale con validazione paper** | | **~5–6 settimane** |

**Nota critica:** i 7 giorni di sviluppo producono codice *non testato su dati reali AH*. Senza dati storici AH la validazione statistica è impossibile (n_trade AH stimato < 30/mese per simbolo → campione insufficiente per qualsiasi conclusione).

---

### 6. Raccomandazione Finale

**Ottimizza intraday. Non portare in AH, almeno non ora.**

Motivazione quantitativa:

1. **Il vantaggio overnight documentato (+33.5% P&L) viene da gap RTH→open**, non da barre AH. Operare in AH non cattura quel vantaggio: il gap avviene *dopo* la sessione AH, non durante.

2. **Il rapporto rischio/rendimento AH non chiude**: slippage 5-16x peggiore, fill rate stimato 60%, HMA non affidabile su serie sparse → il segnale AH ha un edge atteso *negativo* prima ancora di considerare i costi di implementazione.

3. **L'intraday RTH ha ancora spazio**: Sharpe 1.10–2.09 con SQN 3.1–6.2 sono metriche solide. Prima di aggiungere complessità (AH), verifica se `exitbar` ottimizzato (es. 4 vs 6) o `period` (12 vs 16) migliora il Sharpe RTH. Costo: 0 giorni di infrastruttura, solo sweep parametri già esistente.

4. **Se l'obiettivo è catturare i gap overnight**: la strada corretta non è operare in AH ma emettere un ordine LIMIT a fine RTH (15:59) con target = open T+1 stimato, e gestire il position sizing per il Reg-T 50%. Questo è fattibile in 2 giorni e non richiede dati AH.

> **TL;DR**: AH = 7 giorni dev + 5 settimane validazione + rischio infrastrutturale elevato per un edge incerto. Ottimizzazione intraday o strategia "gap capture" = 1-2 giorni per risultati verificabili su dati già disponibili.

---

*Contesto tecnico usato: dati simulazione overnight NFLX/CSCO/KHC 2025-01-02→2025-05-30,*
*verifica codice broker/alpaca_data.py (StockDataStream SIP default)*
