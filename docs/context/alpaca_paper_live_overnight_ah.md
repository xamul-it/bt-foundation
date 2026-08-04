# Alpaca Paper/Live - OvernightAH

## Scopo

Questo documento raccoglie evidenze operative, bug, decisioni e checklist per
la gestione di `overnight_ah.OvernightAH` su Alpaca paper/live.

Non e' un documento di ricerca statistica sulla strategia. Serve a riprendere
rapidamente il contesto quando si analizzano:

- ordini entry non inviati o non eseguiti;
- differenze tra account `auc`, `no` e `live`;
- fallback entry/exit;
- cron e script operativi;
- divergenze tra backtest, paper e live.

## Account E Modalita'

| Profilo | Broker | Env | Entry script | Auction | Uso |
| --- | --- | --- | --- | --- | --- |
| `auc` | Alpaca paper | `env/overnight-ah-auc.key` | `scripts/overnight-ah-entry-auc.sh` | `True` | Entry in asta di chiusura Alpaca (`CLS`) |
| `no` | Alpaca paper | `env/overnight-ah-no.key` | `scripts/overnight-ah-entry-no.sh` | `False` | Entry market/GTC pre-close, senza asta |
| `live` | Alpaca live | `env/live.key` | `scripts/overnight-ah-entry-no-live.sh` | `False` | Replica live del profilo `no` |

Le uscite non sono gestite dalla strategia in paper/live: sono gestite da cron
dedicati MOO/fallback che leggono le posizioni reali Alpaca.

## Buying Power, Reg T E Leva Overnight

Per `OvernightAH` non usare il campo generico Alpaca `buying_power` come
riferimento di leva overnight. Su account margin Alpaca puo' esporre:

- `buying_power`: spesso uguale al day-trading buying power, quindi circa 4x;
- `daytrading_buying_power`: potere d'acquisto intraday, circa 4x;
- `regt_buying_power`: riferimento Reg T per posizioni overnight, circa 2x;
- `non_marginable_buying_power`: potere d'acquisto per asset non marginabili.

Snapshot verificato il 2026-06-18:

| Profilo | Equity | `buying_power` | `regt_buying_power` | `daytrading_buying_power` | Lettura operativa |
| --- | ---: | ---: | ---: | ---: | --- |
| paper `no` | 55,980.85 | 223,923.40 | 111,961.70 | 223,923.40 | 4x mostrato, 2x overnight Reg T |
| live | 5,260.90 | 21,043.60 | 10,521.80 | 21,043.60 | 4x mostrato, 2x overnight Reg T |

Conclusione operativa: il paper puo' mostrare o consentire 4x, ma per una
strategia che entra AH/pre-close e tiene overnight il cap realistico da usare e'
`regt_buying_power`, cioe' circa 2x salvo requisiti di maintenance piu' alti su
singoli titoli. Per questo il live usa `MAX_EXPOSURE=2` e
`MARGIN_LEVERAGE=2`. Eventuali profili paper a 3x/4x vanno considerati test di
stress o simulazioni non fedeli al vincolo overnight live.

## Flusso Operativo Atteso

1. Aggiornare i dati daily.
2. Lanciare `btmain.py` con broker paper/live e feed storico daily.
3. La strategia seleziona solo l'ultima barra del giorno corrente.
4. La strategia invia solo ordini di entry.
5. Il broker Alpaca traduce i flag Backtrader in ordini Alpaca.
6. Il cron MOO del mattino legge le posizioni reali e invia ordini di chiusura.
7. Il fallback MOO cancella eventuali pending di close e chiude quanto resta aperto.
8. Dopo ogni run si controllano ordini open, posizioni e log.

## Convenzioni Ordini Alpaca

| Caso | Backtrader | Alpaca atteso | Note |
| --- | --- | --- | --- |
| Entry `auc` | `buy(..., coc=True)` | `BUY CLS` | Alpaca accetta solo nella finestra CLS |
| Entry `no/live` | `buy(..., exectype=Market)` | `BUY MARKET GTC` | Fuori orario resta pending/accepted |
| Close MOO | script dedicato | `SELL OPG` | Basato sulle posizioni, non sugli ordini entry |
| Fallback MOO | script dedicato | `SELL MARKET` | Cancella pending close e chiude tutto cio' che resta aperto |

La strategia non deve conoscere direttamente `CLS` o `OPG`: il mapping e' nel
broker Alpaca. La strategia esprime solo l'intento con `coc`/`coo` o market
standard.

## Evidenze Confermate

### 2026-06-08 - `auc` genera ordini, Alpaca rifiuta fuori finestra

Contesto: run manuale serale di `scripts/overnight-ah-entry-auc.sh`, dopo la
finestra regolare di invio.

Evidenza:

- la strategia ha generato ordini su `AVGO`, `AMD`, `ASML`, `MELI`;
- Alpaca paper ha risposto:

```text
cls orders must be submitted after 7:00pm and before 3:55pm
```

Conclusione: il path `auc` genera ordini correttamente. Il rifiuto e' dovuto
alla finestra Alpaca `CLS`, non a un blocco interno della strategia.

Stato: confermato.

### 2026-06-08 - `no/live` market GTC fuori orario restano pending

Contesto: run manuale serale di `no` e `live`.

Evidenza:

- `no` paper ha creato ordini `MARKET GTC` accepted su `AVGO`, `AMD`, `ASML`, `MELI`;
- `live` ha creato ordini `MARKET GTC` accepted su `AMD`, `AVGO`, `ASML`, `MELI`;
- gli ordini sono stati cancellati manualmente prima dell'esecuzione;
- verifica successiva: `OPEN 0` su paper `no` e live.

Conclusione: `no/live` non devono essere rilanciati manualmente fuori orario
senza prima controllare e poi cancellare eventuali pending. Un market GTC
accettato fuori orario puo' essere eseguito alla prossima apertura.

Stato: confermato.

### 2026-06-08 - `auction=False` senza `exectype=Market` non arrivava al broker

Contesto: `auc` generava ordini, mentre `no/live` non mostravano submit/reject.

Evidenza:

- `auction=True` usava `buy(..., coc=True)` e l'ordine arrivava al broker;
- `auction=False` usava `buy(d, size=size)` senza `exectype`;
- su feed storico daily e ultima barra, Backtrader trattava il market standard
  come ordine da eseguire alla barra successiva, quindi non arrivava al broker.

Conclusione: per il batch operativo `no/live`, l'entry deve forzare
`exectype=bt.Order.Market`.

Stato: fix applicata in `bt-core/strategies/overnight_ah.py`.

### 2026-06-08 - CSV vuoto rompeva il loader

Contesto: `scripts/overnight-ah-entry-no-live.sh` si e' fermato durante lo step
di download dati.

Evidenza:

```text
pandas.errors.EmptyDataError: No columns to parse from file
```

Il caso osservato era un CSV daily vuoto.

Conclusione: `load_tickers.py` deve trattare file CSV a zero byte come cache
non valida e riscriverli.

Stato: fix applicata in `bt-core/load_tickers.py`.

### 2026-06-08 - Finestra 30 giorni insufficiente per ADV rolling

Contesto: `auc` e `no` arrivavano alla strategia ma non generavano ordini.

Evidenza:

- il wrapper passava `--fromdate` a 30 giorni;
- nel range c'erano esattamente 20 barre di mercato;
- il filtro ADV usa la SMA della barra precedente (`SMA[-1]`);
- con 20 barre esatte l'ADV precedente risultava non disponibile.

Conclusione: il wrapper operativo deve usare una finestra piu' ampia del
lookback minimo.

Stato: `scripts/overnight-ah-entry.sh` usa `FROM_DAYS`, default 120.

### 2026-06-18 - Feed segnale operativo: Yahoo, broker Alpaca

Contesto: il run del 2026-06-18 non ha inviato ordini perche' la selezione
giornaliera era stata valutata su Yahoo e non c'erano candidati Yahoo. La cache
Yahoo e la cache Alpaca/SIP erano entrambe aggiornate al 2026-06-18, ma il
filtro di volatilita' intraday ha prodotto una divergenza borderline su `AMD`.

Evidenza:

- Yahoo: `AMD` range intraday circa 2.499%, quindi sotto `min_intraday_vol=2.5%`;
- Alpaca/SIP: `AMD` range intraday circa 2.516%, quindi sopra soglia;
- il backtest Yahoo sul periodo comune 2016-01-04 -> 2026-06-18 produce un
  risultato sensibilmente migliore del backtest Alpaca/SIP:
  - Yahoo: final value circa 90.6M, Sharpe 1.895, 7,915 trade;
  - Alpaca/SIP: final value circa 53.0M, Sharpe 1.494, 7,909 trade.

Conclusione operativa: per questa strategia il segnale paper/live segue Yahoo,
perche' e' il feed che sostiene il backtest di riferimento. Alpaca resta il
broker di esecuzione e Alpaca/SIP resta utile come controllo diagnostico, ma non
deve sostituire Yahoo nella selezione giornaliera salvo nuova decisione.

Stato: `scripts/overnight-ah-entry.sh` defaulta a `DATA_PROVIDER=yahoo`.

Guardrail codice live: gli script operativi puntano per default a
`overnight_ah_live.OvernightAH`, copia stabile locale della strategia AH. La
versione `overnight_ah.OvernightAH` puo' essere usata per ricerca/modifiche, ma
non va agganciata direttamente al portafoglio live senza promozione esplicita
della copia stabile.

**Correzione successiva**: `overnight_ah_live.py` in questo repo e' stato
rimosso (era un file duplicato non referenziato da nessun cron attivo,
verificato: tutti i profili schedulati `live`/`mirror`/`development`
impostano esplicitamente `STRAT=overnight_ah.OvernightAH`). La promozione
reale avviene via git sul checkout separato `/home/htpc/backtrader-prod`
(branch `prod`), non via file duplicato in questo repo — vedi
`docs/context/ah_context.md` per il dettaglio.

### 2026-06-24 - Slot `auc` paper su strategia dinamica dev

Contesto: lo slot `auc` paper e' stato usato per testare in paper la versione
dinamica di ricerca senza toccare gli slot `no` e `live` stabili.

Configurazione:

- cron `auc` entry/fallback/MOO puntano a `/home/htpc/backtrader/scripts`;
- cron `no` e `live` restano su `/home/htpc/backtrader-stable/scripts`;
- `scripts/overnight-ah-entry-auc.sh` forza `TRADING_MODE=paper`;
- env Alpaca: `env/overnight-ah-auc.key`;
- strategia: `overnight_ah.OvernightAH`;
- ticker: `yahoo_adj_research_universe.json`;
- provider operativo: `yahoo`;
- leva/cap: `MAX_EXPOSURE=2`, `MARGIN_LEVERAGE=2`;
- storico operativo: `FROM_DAYS=420`;
- run id: `overnight_ah_auc_dynamic_paper`.

Parametri dinamici principali:

```text
max_concurrent=5
size_by_max_concurrent=True
min_intraday_vol=0.025
max_intraday_vol=0.045
intraday_vol_filter_side='any'
ah_lag1_threshold=-0.1
min_adv=100000000
auction=True
monthly_universe_mode='weak_theme_switch'
monthly_universe_top_n=50
monthly_universe_base_weight=0.85
monthly_universe_theme_weight=0.15
monthly_universe_theme_score='corr12'
monthly_universe_switch_feature='semis_total_3m'
monthly_universe_switch_threshold=0.0
monthly_universe_spy_dd3m_threshold=-0.10
```

Nota: `yahoo_adj_research_universe.json` non contiene `SPY`. Di conseguenza il
gate SPY della strategia viene bypassato, coerentemente con i test research
validati usando lo stesso universo.

Verifiche eseguite:

- syntax shell sugli script `auc`/entry/MOO/fallback;
- `py_compile` su strategia e script Python di fallback/close;
- smoke backtest con la stessa configurazione AUC dinamica, provider Yahoo,
  senza invio ordini;
- check read-only Alpaca paper `auc`: account attivo, trading non bloccato,
  zero ordini aperti, zero posizioni.

Lo step di download Yahoo in `scripts/overnight-ah-entry.sh` e' protetto da
timeout (`LOAD_TIMEOUT_SEC`, default 900s) per evitare run cron appese.

Comando backtest statico di riferimento Yahoo:

```bash
cd /home/htpc/backtrader/bt-core
python btmain.py \
  --strat overnight_ah.OvernightAH \
  --ticker stable_ah_top10.json \
  --mode backtest \
  --timeframe daily \
  --provider yahoo \
  --fromdate 2016-01-04 \
  --stratargs "max_concurrent=5 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 max_adv_participation=0.0025 max_exposure=2 min_price=0 min_adv=100000000 auction=True" \
  --margin-leverage 2 \
  --commission none
```

Nota: in backtest daily usare `auction=True` per simulare close-to-next-open.
Il ramo `auction=False` serve al profilo operativo `no/live`, ma nel backtest
daily produce entry/exit allo stesso prezzo e quindi non e' una misura utile.

### 2026-06-18 - Close live e paper `no` non sono equivalenti

Contesto: confronto operativo tra profilo `no` paper e `no-live`.

Evidenza:

- in live gli ordini di chiusura `SELL OPG` tendono a essere eseguiti
  all'apertura di mercato;
- in paper `no` gli stessi ordini possono restare non eseguiti/accepted e
  arrivare al fallback;
- il fallback chiude piu' tardi con `SELL MARKET`, quindi il risultato paper
  include una porzione RTH che live normalmente non include;
- questo spiega chiusure live circa 20-30 minuti prima rispetto al paper `no`
  e puo' alterare il confronto PnL paper/live.

Conclusione: per valutare la qualita' della strategia overnight, il profilo
live e' piu' coerente con l'intento MOO/open. Il paper `no` e' utile come
controllo operativo, ma quando passa al fallback non misura piu' la stessa
finestra temporale.

Stato: divergenza confermata; da considerare in ogni confronto paper/live.

## Divergenze Da Tenere Sotto Controllo

| Divergenza | Motivo | Rischio |
| --- | --- | --- |
| Paper/live filtrano solo ultima barra di oggi | Evita di riscorrere lo storico nel batch | Se la barra del giorno non e' presente, non genera ordini |
| Paper/live non inviano close dalla strategia | Close gestite da cron MOO dedicati | Serve verificare cron/fallback, non la strategia entry |
| Close OPG live vs paper `no` | Live tende a fillare all'open; paper `no` puo' arrivare al fallback | PnL paper include RTH extra e non e' confrontabile 1:1 col live |
| `auction=True` vs `auction=False` | `CLS` vs market/GTC | Fuori orario `CLS` rifiuta, market/GTC resta pending |
| Yahoo vs Alpaca daily | Provider e consolidamento possono differire | Candidati/sizing possono cambiare |
| Pending preesistenti | Ordini accettati ma non eseguiti | Possono sporcare run successivi o creare esecuzioni indesiderate |
| Broker Alpaca reale vs BackBroker | Alpaca ha finestre, stati, buying power reali | Backtest non riproduce tutto il comportamento operativo |

## Bug / Fix Applicate

| Data | Problema | Causa | Fix | Stato |
| --- | --- | --- | --- | --- |
| 2026-06-08 | Live crashava nel download | CSV vuoto letto da pandas | Gestione `EmptyDataError`/file zero byte in `load_tickers.py` | Applicato |
| 2026-06-08 | `auc/no` senza ordini | Finestra 30 giorni troppo corta per ADV `SMA[-1]` | `FROM_DAYS=120` nel wrapper entry | Applicato |
| 2026-06-08 | `no/live` non arrivavano al broker | Market senza `exectype` su ultima barra daily | Forzato `exectype=Market` per entry | Applicato |
| 2026-06-08 | Run manuale fuori orario ha creato pending GTC | Market GTC accettato fuori sessione | Cancellazione manuale ordini open | Completato |

## Rischi Aperti

- `BUY MARKET GTC` fuori orario puo' restare pending ed essere eseguito alla
  prossima apertura.
- `auc` puo' fallire se l'ordine arriva fuori finestra `CLS`.
- I test manuali su live devono sempre iniziare con controllo pending/posizioni.
- Le differenze Yahoo/Alpaca possono cambiare i candidati.
- Il fallback entry auction deve rispettare tick size/prezzi validi.
- I log `MULTITICKER_SUMMARY open_completed=0` non indicano necessariamente
  "nessun ordine inviato": in paper/live un ordine accepted ma non filled non
  aumenta `open_completed`.

## Checklist Prima Di Un Run Manuale

1. Verificare account/env corretto.
2. Verificare posizioni aperte.
3. Verificare ordini open/pending.
4. Verificare finestra oraria Alpaca:
   - `auc`: dentro finestra `CLS`;
   - `no/live`: evitare run fuori orario se non si vogliono pending GTC.
5. Verificare `DATA_PROVIDER`.
6. Verificare `MAX_EXPOSURE` e `MARGIN_LEVERAGE`; per overnight live non usare
   `buying_power` 4x come riferimento, ma `regt_buying_power`/2x.
7. Decidere se si tratta di run reale o solo diagnostico.

## Checklist Dopo Un Run

1. Controllare ordini creati (`ENTRY_SIGNAL`, `MOC submitted`).
2. Controllare submit/reject Alpaca.
3. Controllare ordini open/pending.
4. Controllare posizioni aperte.
5. Se run manuale fuori orario, cancellare immediatamente i pending non voluti.
6. Annotare evidenza e conclusione in questo documento.

## Comandi Utili

### Vedere ordini open paper `no`

```bash
set -a; source env/overnight-ah-no.key; set +a
cd bt-core
. .venv/bin/activate
python - <<'PY'
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
import os

c = TradingClient(os.environ['ALPACA_API_KEY'], os.environ['ALPACA_SECRET_KEY'], paper=True)
orders = list(c.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=100)) or [])
print('OPEN', len(orders))
for o in orders:
    print(o.symbol, o.side, o.qty, o.type, o.time_in_force, o.status, o.submitted_at, o.id)
PY
```

### Vedere ordini open live

```bash
set -a; source env/live.key; set +a
cd bt-core
. .venv/bin/activate
python - <<'PY'
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
import os

c = TradingClient(os.environ['ALPACA_API_KEY'], os.environ['ALPACA_SECRET_KEY'], paper=False)
orders = list(c.get_orders(filter=GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=100)) or [])
print('OPEN', len(orders))
for o in orders:
    print(o.symbol, o.side, o.qty, o.type, o.time_in_force, o.status, o.submitted_at, o.id)
PY
```

### Vedere buying power e margine

Usare `paper=True` con `env/overnight-ah-no.key`, oppure `paper=False` con
`env/live.key`.

```bash
set -a; source env/live.key; set +a
cd bt-core
. .venv/bin/activate
python - <<'PY'
from alpaca.trading.client import TradingClient
import os

c = TradingClient(os.environ['ALPACA_API_KEY'], os.environ['ALPACA_SECRET_KEY'], paper=False)
a = c.get_account()
for k in [
    'cash', 'equity', 'buying_power', 'regt_buying_power',
    'daytrading_buying_power', 'non_marginable_buying_power',
    'initial_margin', 'maintenance_margin', 'multiplier',
    'pattern_day_trader',
]:
    print(f'{k}={getattr(a, k, None)}')
PY
```

### Lanciare entry

```bash
scripts/overnight-ah-entry-auc.sh
scripts/overnight-ah-entry-no.sh
scripts/overnight-ah-entry-no-live.sh
```

### Log principali

```bash
tail -n 200 logs/overnight-ah-entry-auc.log
tail -n 200 logs/overnight-ah-entry-no.log
tail -n 200 logs/overnight-ah-entry-no-live.log
tail -n 200 bt-core/out/overnight_ah/OvernightAH/runtime.log
```

## Registro Decisioni

| Data | Decisione | Motivo |
| --- | --- | --- |
| 2026-06-18 | Entry live/no portata a `MAX_EXPOSURE=2` e `MARGIN_LEVERAGE=2` | Test live a 2x; non estendere a 3x overnight senza nuovi guardrail |
| 2026-06-18 | Paper `no` con fallback non confrontabile 1:1 con live | Il live chiude spesso OPG/open, paper `no` puo' chiudere piu' tardi a mercato |
| 2026-06-08 | Entry live/no con `MAX_EXPOSURE=1.5` per live | Evitare sottoutilizzo capitale su account live con importi piccoli |
| 2026-06-08 | Usare Yahoo come provider daily operativo di default | Preferenza operativa; provider coerente tra download e run |
| 2026-06-08 | Close paper/live non gestite dalla strategia | Le chiusure devono leggere le posizioni reali e chiudere tutto cio' che e' aperto |
| 2026-06-08 | Fallback MOO tra minuto 20-25 | Ridurre rischio di posizioni residue dopo OPG |
| 2026-06-08 | Non rilanciare manualmente `no/live` fuori orario senza cancellare pending | Market GTC puo' restare accepted |

## Appendice: Cron Rilevanti

Snapshot operativo da aggiornare quando cambia crontab:

```text
30 21 * * 1-5 /home/htpc/backtrader/scripts/overnight-ah-entry-auc.sh >> /home/htpc/backtrader/logs/overnight-ah-entry-auc.log 2>&1
46 21 * * 1-5 /home/htpc/backtrader/scripts/overnight-ah-entry-no.sh >> /home/htpc/backtrader/logs/overnight-ah-entry-no.log 2>&1
46 21 * * 1-5 /home/htpc/backtrader/scripts/overnight-ah-entry-no-live.sh >> /home/htpc/backtrader/logs/overnight-ah-entry-no-live.log 2>&1
05 22 * * 1-5 /home/htpc/backtrader/scripts/overnight-ah-auction-fallback-auc.sh --after 15:30 >> /home/htpc/backtrader/logs/overnight-ah-auction-fallback-auc.log 2>&1
```
