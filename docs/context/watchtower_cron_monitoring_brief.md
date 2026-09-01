# Watchtower per strategie cron (overnight_ah) — analisi e goal di finalizzazione

## Scopo di questo documento

Due parti:

1. **Analisi** (questa sezione + le successive fino a "Prompt di finalizzazione"):
   cosa esiste già nel workspace, cosa manca, quali vincoli rispettare. Serve a
   chi riprende il lavoro per non ripartire da zero e non duplicare
   infrastruttura già scritta.
2. **Prompt di finalizzazione**: un blocco autonomo, pensato per essere
   incollato come primo messaggio in una nuova sessione Claude Code (o
   riletto in questa) per guidare l'implementazione.

Richiesta originale dell'utente (riassunta): un sistema di monitoraggio che
verifichi che le strategie in paper/live si comportino come atteso, con due
assi di confronto — (A) esecuzione reale (ordini/posizioni) vs un backtest
"replay" con gli stessi parametri, e (B) comportamento storico della
strategia (win ratio, gain medio, varianza) prima vs dopo l'attivazione su un
account, per capire se un cambio di regime ne ha degradato le performance.
Deve coprire sia strategie schedulate su cron (le `overnight_ah*` di cui si è
parlato in questa conversazione) sia strategie eseguite come servizi
systemd. Deve garantire che ogni account Alpaca abbia una sola strategia
assegnata. Deve tenere conto che i parametri di una strategia cambiano nel
tempo per account, quindi il confronto storico deve usare i parametri
realmente in vigore in ciascun periodo, non quelli attuali applicati
retroattivamente.

## Stato dell'arte — NON ripartire da zero

C'è già un'infrastruttura di monitoraggio sostanziale, live in produzione.
**Il primo compito di chi implementa è leggerla per intero prima di
progettare qualunque cosa nuova**, non fidarsi solo di questo riassunto.

### Postgres già esiste ed è già lo store di riferimento

- DB `bt_live_events` su `postgresql://127.0.0.1:5433` (vedi
  `/home/htpc/backtrader/env/bt-live-events`, variabile `LIVE_EVENTS_DB_DSN`).
- Schema in `bt-core/config/sql/live_events_postgres.sql`, applicato da
  `watchtower_runtime.ensure_schema()`. Tabelle principali:
  - `live_event_stream` — eventi grezzi ordine/segnale (run_id, strategy,
    mode, symbol, event_type, signal_id, client_order_id, broker_order_id,
    status, portfolio_key_id, chain_run_id, raw_event JSONB).
  - `runs` — un run per (strategy, strategy_fingerprint, params JSONB,
    args JSONB, portfolio_key_id, chain_run_id, outpath, status). Il
    fingerprint/hash dei parametri **esiste già** (`strategy_fingerprint`,
    `params_hash` in `stat_baselines`/`stat_checks`) — è la base pronta per
    il "footprint" richiesto dall'utente, non va reinventata.
  - `run_trade_outcomes`, `run_warnings`, `run_artifacts`, `run_segments`.
  - `stat_baselines` / `stat_checks` — baseline statistica per
    (strategy, strategy_fingerprint, params_hash) con `metrics JSONB` e
    `quality_score`, più un check che confronta un run corrente contro la
    baseline. **Concettualmente è quasi esattamente il meccanismo di
    "footprint drift" richiesto**, ma la sua semantica attuale è
    baseline-per-parametri, non esplicitamente "periodo pre-attivazione vs
    periodo post-attivazione per un dato account": va verificato a fondo se
    riusarlo così com'è o se serve un livello sopra.
  - `watchtower_alerts`, `watchtower_reports`, `watchtower_windows`.
  - `alpaca_portfolios` — **registro account Alpaca** (portfolio_key_id,
    display_name, api key/secret, paper/live, metadata, is_active). È la
    base naturale per il vincolo "un solo account ↔ una sola strategia":
    manca solo il collegamento esplicito verso la strategia/profilo
    assegnato e un controllo che lo faccia rispettare.
  - `alpaca_order_cache` / `alpaca_position_cache` — cache locale di
    ordini/posizioni reali Alpaca, con `portfolio_key_id`, `client_order_id`,
    `source_account`. Sono la struttura giusta per registrare gli ordini
    reali osservati dai cron `overnight_ah*`.
  - `feed_monitor_*` — confronto dati live vs storici (minute bar), tema
    distinto dal tracking ordini, non toccare se non richiesto.

- Funzioni statistiche già scritte in `bt-core/watchtower_runtime.py`:
  `distribution_moments`, `wasserstein_distance`, `_ks_distance`,
  `monte_carlo_subset_test`, `baseline_metrics_from_trades`,
  `evaluate_outcomes`, `_classify_mismatch`, `_execution_delta`,
  `strategy_fingerprint`/`strategy_params_hash`. Prima di scrivere nuova
  logica di confronto/drift, verificare se queste funzioni sono già
  sufficienti o adattabili.

### Servizi systemd già collegati a questo Postgres

- `bt-live-event-writer.service` → `bt-core/live_event_writer.py`: legge lo
  stream Redis `bt:live_events` (consumer group) e scrive in
  `live_event_stream` (+ file, se `LIVE_EVENTS_WRITE_FS=1`).
- `bt-watchtower.service` → `bt-core/reconciliation_watchdog.py`: polling
  ogni 5s (`BT_WATCHTOWER_POLL_SECONDS`), confronta ordini `paper` vs `sim`
  per lo stesso `run_id`/finestra e scrive alert in `watchtower_alerts`.
- `bt-api/config/watchtower_services.json` elenca anche
  `parallel-sim.service`, che **non esiste** come unit systemd installata
  (verificato con `systemctl --user list-unit-files`): probabilmente un
  componente pianificato/rimosso. Da chiarire, non assumere che esista un
  broker "sim" continuo già in esecuzione.

### API già esposte (`bt-api/app/watchtower.py`, blueprint `obs_bp`)

Endpoint già implementati (elenco da `grep` delle route, non ancora letti
riga per riga — verificarli prima di riscriverli):
`/runs`, `/runs/<id>`, `/watchtower`, `/watchtower/windows`,
`/watchtower/portfolio-contexts`, `/watchtower/portfolio-session`,
`/watchtower/current-window`, `/watchtower/rebuild`,
`/watchtower/alpaca-sync[/<job_id>]`,
`/watchtower/feed-monitoring/*` (meta, coverage, sync, match, summary,
field-pivot, discrepancies, export),
`/watchtower/overview`, `/watchtower/order-matching`,
`/watchtower/export-bars`, `/watchtower/factsheet`,
`/watchtower/coherence-summary`, `/watchdog`,
`/watchtower/baselines[/by-run/<id>][/recompute][/jobs/<id>]`,
`/services`, `/services-config` (GET/PUT), `/services/<service>/<action>`,
`/scheduler`.

### UI già esistenti (`bt-dash/src/bt/pages/`)

- `WatchtowerServiceMonitoring.vue` (282 righe) — presumibilmente stato
  servizi/scheduler.
- `WatchtowerFeedMonitoring.vue` (889 righe) — confronto feed dati
  live/storico.

Nessuna vista attuale, a quanto risulta dai nomi, copre esplicitamente
"execution reconciliation per account cron" o "footprint pre/post
attivazione" — da confermare leggendo il codice, non solo i nomi file.

### Strumenti standalone già esistenti, non integrati nel DB

- `bin/compare_live_backtest_trades.py` (726 righe, funzionante): lancia (o
  legge) un backtest, scarica ordini filled da Alpaca per un
  account/env, costruisce round-trip per simbolo, confronta con i trade di
  backtest, calcola edge di esecuzione in bps, produce CSV + summary. È
  **già la logica di riconciliazione ordini richiesta dall'utente**, ma
  gira ad-hoc da riga di comando e scrive solo CSV — nessuna persistenza in
  Postgres, nessuna storia, nessuna UI.
- `bin/alpaca_audit/` (`fetch_data.py`, `analyze.py`,
  `leverage_simulation.py`): equity curve, ordini, posizioni, leva per
  account, output JSON/CSV in `bin/alpaca_audit/out/{account}/`. Utile per
  monitoraggio leva/margine, complementare ma distinto dal tracking ordini.
- `docs/context/alpaca_paper_live_overnight_ah.md`: knowledge base già
  ricca delle cause note di divergenza ordine backtest/paper/live per
  `overnight_ah` (finestre CLS, market GTC fuori orario, `exectype`
  mancante, CSV vuoti, provider Yahoo vs Alpaca, chiusura OPG vs fallback
  MOO). **Usare questo documento come tassonomia di partenza** per la
  classificazione automatica delle divergenze, invece di reinventarla.

## Il gap architetturale critico da colmare

Verificato leggendo `bt-core/strategies/multiTickerStrategy.py` (metodo
`start()`, righe ~559-597): lo streaming eventi verso Redis/Postgres si
attiva **solo se il feed dati è live** (`_feed_is_live`, calcolato da
`d._islive`/`d.islive()` sui data feed), indipendentemente dalla modalità
broker (`paper`/`live`). Commento nel codice: *"Feed mode is the
discriminator... This is independent from broker mode on purpose."*

Le strategie cron `overnight_ah*` (`live`, `mirror`, `challenger`,
`development`, vedi crontab e `~/.config/backtrader/scheduled/*.env`)
girano con `btmain.py --timeframe daily --provider yahoo|yahoo_adj --mode
paper|live` su un **feed storico daily**, non su un feed live. Quindi:

**Le strategie cron overnight_ah oggi non emettono mai eventi nello
stream Redis/Postgres.** L'intera infrastruttura Watchtower descritta sopra
copre di fatto solo i servizi intraday che usano feed live (stile HMA), non
i profili cron di cui si è discusso in questa conversazione. Questo
conferma esplicitamente quanto detto dall'utente ("pensati più per il
trading intraday a servizi"), verificato in codice.

Conseguenza per il design: per i profili cron non si può assumere di avere
già eventi in `live_event_stream`. Serve un canale di ingestione
alternativo (probabilmente: poll dell'API Alpaca ordini/posizioni subito
dopo ogni fase cron — `entry`/`exit`/`exit-fallback` — scritto in
`alpaca_order_cache`/`alpaca_position_cache` con `portfolio_key_id`
corretto, sulla falsariga di quanto già fa `watchtower_alpaca-sync` per
altri contesti — verificarne il codice prima di duplicarlo).

### Trigger di streaming: decisione finale (`WT_STREAM`)

Discusso e convergito in questa conversazione (non ancora implementato in
codice). Il trigger NON deve dedurre nulla da tipo di feed o tipo di
broker: sarebbe fragile (vedi bug sotto) e "caotico" da mantenere. Design
finale:

- In `multiTickerStrategy.py` (metodo `start()`, dove oggi si calcola
  `_feed_is_live`), sostituire interamente la logica di inferenza con:
  `self._live_streaming = os.environ.get('WT_STREAM') == '1'`.
- `WT_STREAM=1` va impostato **esplicitamente solo da automazioni**, mai
  da un utente interattivo e mai dedotto dai parametri CLI:
  - in `scheduled-job.sh`, per la fase `entry` (quella che invoca
    `btmain.py`), così i cron `overnight_ah*` iniziano a emettere eventi;
  - nel futuro job/servizio di backtest-replay per la riconciliazione
    (requisito A), così anche il lato "sim" emette eventi confrontabili;
  - negli `EnvironmentFile` systemd dei servizi a feed live che oggi
    probabilmente si affidano a `_feed_is_live` per streammare — **da
    identificare quale servizio pubblica oggi questi eventi prima di
    fare il cambio**, per non spegnere lo streaming esistente (vedi
    "Cosa NON è ancora deciso").
- Un run manuale/ad-hoc da riga di comando non esporta `WT_STREAM` → non
  streamma mai, per costruzione, senza bisogno di un nuovo parametro CLI
  esposto all'utente.

### Bug di labeling già presente, da correggere insieme al trigger

In `multiTickerStrategy.py` (costruttore, ~righe 405-429):
```python
self.broker_live = getattr(self.broker, 'live', False)
self.broker_paper = getattr(self.broker, 'paper', False)
...
if self.broker_live:
    self._live_mode = 'live'
elif self.broker_paper:
    self._live_mode = 'paper'
else:
    self._live_mode = 'backtest'
```
`AlpacaBroker.live` è un attributo di classe **incondizionato** (`True`
sia per paper sia per real, vedi `broker/alpacaBroker.py:137`), mentre
`AlpacaBroker.paper` è l'attributo di istanza che distingue davvero i due
casi (`broker/alpacaBroker.py:144`). Con l'ordine attuale, il ramo `elif
self.broker_paper` è **irraggiungibile**: qualunque run con
`AlpacaBroker` (paper o reale) risulta oggi etichettato `_live_mode =
'live'`. Finché nessun cron paper streamma, il bug è silenzioso; appena si
attiva `WT_STREAM` per i profili `mirror`/`challenger`/`development`
(tutti paper), tutti i loro eventi finirebbero etichettati `live` in
`live_event_stream.mode`, confondendo `reconciliation_watchdog.py` (che
distingue esplicitamente `paper`/`live`/`shadow` in `select_modes()`,
`reconciliation_watchdog.py:153-168`). **Fix da applicare insieme al
cambio di trigger**: controllare prima `broker_paper`, poi `broker_live`:
```python
if self.broker_paper:
    self._live_mode = 'paper'
elif self.broker_live:
    self._live_mode = 'live'
else:
    self._live_mode = 'backtest'
```

### Il ramo di uscita (`moo-exit.sh`) non può streammare per costruzione

Verificato via grep: `bin/submit_moo.py` (chiamato da `moo-exit.sh` e
`moo-exit-fallback.sh`) fa solo chiamate dirette `TradingClient` di
Alpaca — nessun oggetto strategia/broker Backtrader coinvolto, quindi
nessun punto in cui agganciare `WT_STREAM`/`LiveEventPublisher`. La fase
di uscita dei cron `overnight_ah*` **non potrà mai** emettere eventi nello
stream Redis/Postgres per questa via. La riconciliazione degli ordini di
chiusura deve quindi passare esclusivamente dal canale di polling Alpaca
già previsto sopra (`alpaca_order_cache`/`alpaca_position_cache`), non dal
meccanismo di streaming — i due canali insieme (stream per l'entry via
`WT_STREAM`, polling per l'exit) coprono l'intera giornata di un profilo
cron.

## Altri fatti di contesto già stabiliti in questa conversazione

- 4 profili cron: `live` (account reale, `CODE_ROOT=backtrader-prod`),
  `mirror` (paper, stessa STRATEGY_CONFIG di `live`),
  `challenger` (paper, `overnight_ah_flat_composite.OvernightAHFlatComposite`),
  `development` (paper, `overnight_ah.OvernightAH` con parametri in
  evoluzione). Config in `~/.config/backtrader/scheduled/{profile}.env` +
  `config-common/scheduled/strategies/overnight-ah-{profile}.env`.
- `scheduled-job.sh` logga già, a ogni esecuzione:
  `commit=<hash CODE_ROOT> bt_core_commit=<hash bt-core>` — il dato "quale
  versione di codice era attiva quel giorno" **esiste già nei log di
  testo** (`~/.local/state/backtrader/logs/{profile}/{data}.log`), va solo
  persistito in modo interrogabile invece di restare solo in log grezzi.
- I file `overnight-ah-*.env` sotto `config-common/scheduled/strategies/`
  contengono uno storico di `STRATARGS` passati **come commenti Markdown
  nel file stesso** (con date e motivazioni) — utile ma non
  machine-readable: un riconciliatore storico non può oggi ricostruire
  automaticamente "quali STRATARGS erano in vigore il 2026-07-15 per
  `development`" senza parsing ad-hoc dei commenti o git-blame sul file.
  Questo è probabilmente il punto più delicato del vincolo "i parametri
  cambiano nel tempo per account" citato dall'utente.
- Il download ticker (`load_tickers.py`) usa già un flock bloccante
  per-cartella condiviso tra tutti i checkout (`backtrader/` e
  `backtrader-prod/`, che risolvono allo stesso path fisico via symlink su
  `config-common/data`) — qualunque nuovo job di backtest-replay per la
  riconciliazione **deve passare da `load_tickers.py`/lo stesso
  meccanismo**, non scaricare dati per conto proprio, per non rompere
  quella garanzia già verificata.
- `git status` iniziale di questa sessione mostra `bin/alpaca_audit/`,
  `bin/compare_live_backtest_trades.py`,
  `bin/watchtower_build_baseline.py` come file non tracciati.
  **`watchtower_build_baseline.py` è già stato ispezionato (100 righe)**:
  scansiona una root di directory di output backtest (`**/results.json` +
  `trades.json`), estrae strategy/fingerprint/params/pnl_values e chiama
  `WatchtowerRepository.upsert_baseline(...)` per popolare `stat_baselines`.
  È già un ponte funzionante file→Postgres per il footprint (requisito B),
  riusabile puntandolo sugli output dei backtest storici/replay delle
  strategie `overnight_ah*` — non richiede feed live, lavora solo su
  `results.json`/`trades.json` su disco. Non è ancora committato né
  collegato a nessun cron/servizio: verificare se va integrato com'è o
  esteso (es. per distinguere periodo pre/post attivazione, che oggi non
  gestisce: raggruppa per fingerprint+params_hash, non per finestra
  temporale).
- `bt-agent/` (anch'esso non tracciato) è un'iniziativa **separata**
  (prototipazione pandas→Backtrader per nuove strategie), non confonderla
  con questo lavoro di monitoraggio.

## Requisiti riformulati (dalla richiesta utente)

### A. Execution reconciliation (per account, giorno per giorno)

Per ogni account/profilo cron, ogni giorno di trading:

1. Recuperare gli ordini/posizioni reali da Alpaca per quell'account
   (submitted, filled, partial, rejected, canceled — non solo i filled, a
   differenza di `compare_live_backtest_trades.py` che oggi filtra solo
   `filled_at` non nullo).
2. Eseguire un backtest "replay" con:
   - stesso codice (stesso commit `CODE_ROOT`/`bt-core` in vigore quel
     giorno, o quantomeno lo stesso comportamento — da decidere se serve
     davvero checkout storico del commit o se il comportamento è stabile),
   - stessi `STRATARGS`/parametri in vigore per quel profilo in quel
     giorno (non quelli attuali) — vedi la sezione dedicata "Timeline dei
     parametri e algoritmo di recupero" più sotto per COME risolvere
     correttamente "quali parametri erano in vigore quel giorno",
   - **`auction=True` forzato nel replay**, anche se il profilo live/paper
     gira con `auction=False`: da `alpaca_paper_live_overnight_ah.md`,
     "il ramo `auction=False` serve al profilo operativo `no/live`, ma nel
     backtest daily produce entry/exit allo stesso prezzo e quindi non è
     una misura utile" — copiare `STRATARGS` verbatim dal profilo live
     produrrebbe un replay non comparabile; questo è l'unico parametro che
     va **tradotto**, non copiato,
   - `--cash` impostato all'equity reale storica dell'account per quel
     giorno (da `bin/alpaca_audit`, `portfolio_history.csv`), non un
     valore fisso: usare un cash sintetico produrrebbe sizing diversi e
     falsi mismatch sugli ordini per ragioni di solo capitale disponibile,
   - stesso universo ticker, stesso provider dati, fino a quella data
     (nessun leakage in avanti).
3. Confrontare ordine per ordine / round-trip per round-trip e classificare
   ogni divergenza in una tassonomia esplicita, riprendendo/estendendo
   quella già annotata in `alpaca_paper_live_overnight_ah.md`:
   - **Più ordini in backtest**: rifiutato da Alpaca (finestra CLS,
     buying power, asset non tradabile...), rimasto pending/non filled,
     mai inviato (bug/crash nello script), esecuzione parziale, cash/leva
     diversi tra backtest e broker reale (`MARGIN_LEVERAGE`,
     `regt_buying_power` vs `buying_power`, vedi
     `alpaca_paper_live_overnight_ah.md`).
   - **Meno ordini in backtest**: segnale diverso per divergenza dati
     (Yahoo vs Alpaca/SIP, timing feed), equity/sizing diversi che hanno
     abilitato un trade in live e non in backtest, bug di
     replay/parametri storici sbagliati, problema di composizione
     universo ticker.
4. Persistere il risultato (non solo CSV volatili) per poterlo mostrare in
   bt-dash nel tempo, riusando `runs`/`run_trade_outcomes`/
   `alpaca_order_cache` o tabelle nuove coerenti con lo schema esistente.
5. **Il giorno di trading corrente ("aperto") va escluso dal confronto**,
   non forzato a coincidere: finché le posizioni di quel giorno non sono
   chiuse/settled, un disallineamento di timing è fisiologico (Alpaca
   riempie a inizio barra o in asta di chiusura, Backtrader "vede" solo a
   barra conclusa) e segnalarlo come mismatch produce un falso positivo.
   Il giorno escluso NON va scartato per sempre: va recuperato non appena
   chiude, usando i parametri **di quel giorno specifico**, non quelli
   correnti al momento del recupero. Questo è un punto delicato con una
   sezione dedicata più sotto ("Timeline dei parametri e algoritmo di
   recupero (catch-up) dei giorni esclusi") — leggerla per intero prima di
   implementare il punto 5, è la parte più a rischio di bug silenziosi di
   tutto il requisito A.

### B. Strategy footprint drift (per strategia, indipendente dall'account salvo che per i parametri effettivi)

1. Calcolare un "footprint" statistico della strategia (win ratio, gain
   medio per trade, varianza, altre metriche rilevanti — vedere se
   `baseline_metrics_from_trades`/`distribution_moments` in
   `watchtower_runtime.py` bastano) su un lungo backtest storico "di
   ricerca", dalla prima data utile fino alla data di attivazione
   sull'account.
2. Calcolare lo stesso footprint sul periodo successivo all'attivazione
   (in backtest, con gli stessi parametri storicizzati — questo studio
   guarda solo la strategia, non l'esecuzione reale).
3. Confrontare le due distribuzioni (test statistico di drift — vedere se
   `wasserstein_distance`/`_ks_distance`/`monte_carlo_subset_test` sono
   già adatti) e segnalare quando il regime sembra cambiato in modo
   significativo.
4. Questo è concettualmente vicino a `stat_baselines`/`stat_checks`, ma la
   chiave "pre vs post attivazione per uno specifico account" non è la
   stessa di "baseline per fingerprint/params_hash" — va chiarito in fase
   di design se estendere quelle tabelle o aggiungerne di nuove.

### C. Registro account ↔ strategia (guardrail)

- Estendere `alpaca_portfolios` (o tabella collegata) con l'assegnazione
  esplicita "questo account è dedicato a questo profilo/strategia".
- Un controllo periodico che, guardando gli ordini reali per account
  (`client_order_id`/prefissi, `RUN_ID`), verifichi che non arrivino
  ordini attribuibili a più di una strategia sullo stesso account —
  errore grave da segnalare, non silenziare.

### D. Storicizzazione parametri/versione per profilo

- Una fonte machine-readable di "quali STRATARGS e quale commit erano in
  vigore per il profilo X in data Y", per alimentare sia (A) sia i grafici
  richiesti dall'utente ("cosa cambia come strategia, es. anche commit su
  git o valori... devo vedere i valori usati"). Valutare se: (i) parsare
  git log/blame sui file `config-common/scheduled/strategies/*.env`
  esistenti, (ii) persistere il `commit`/`STRATARGS` già loggato da
  `scheduled-job.sh` a ogni run in una tabella dedicata andando avanti (non
  retroattivo per il passato non ancora loggato in modo strutturato), o
  (iii) entrambi.
- **Questo requisito è un prerequisito hard per il punto 5 di (A)**
  (recupero dei giorni esclusi). Senza una timeline dei parametri
  interrogabile "a una data", il recupero di un giorno passato non ha
  altra scelta che usare i parametri attuali — che è esattamente lo
  scenario sbagliato descritto nella sezione seguente. Non implementare il
  catch-up prima di aver implementato questo.

### D-bis. Timeline dei parametri e algoritmo di recupero (catch-up) dei giorni esclusi (dettaglio di D)

Questa sezione risponde a un requisito specifico dell'utente, riportato
qui testualmente perché è la specifica esatta da rispettare:

> "scartare il giorno aperto va bene, ma fino a un certo punto! poi va
> ripreso il giorno dopo con le configurazioni del giorno prima! Se il
> giorno aperto è X e dopo la chiusura delle posizioni cambio parametri,
> il giorno X+1 devo recuperare il giorno perso con i parametri del
> giorno X, non X+1."

In altre parole: **la data che conta per scegliere i parametri non è mai
la data in cui il recupero viene eseguito, ma la data del giorno di
trading che si sta riconciliando.** Sembra ovvio detto così, ma è
esattamente il tipo di errore facile da introdurre per disattenzione (es.
un job di catch-up che, per semplicità, legge "i parametri correnti" dal
file `.env` invece di guardare la timeline storica) — da qui la richiesta
esplicita di documentarlo "a prova di idiota".

#### Perché non basta escludere il giorno aperto (ripasso da Turn 5)

Il giorno di trading in corso (day X mentre il mercato è ancora aperto, o
anche a mercato chiuso ma prima che l'exit leg abbia settled tutti gli
ordini) va escluso dal confronto invece di essere forzato: è fisiologico,
non un bug (vedi requisito A punto 5). Ma "escluso" deve voler dire
**"messo in coda, non buttato via"**: prima o poi quel giorno chiude e va
riconciliato, altrimenti si perde silenziosamente un giorno di copertura
per ogni ciclo in cui il monitoraggio gira mentre il mercato non ha ancora
settled — che con un cron giornaliero è la norma, non l'eccezione.

#### Perché "i parametri di oggi" sono sbagliati per un giorno passato

Un profilo cron (es. `development`) può cambiare `STRATARGS` in qualsiasi
momento — tipicamente a mercato chiuso, dopo che l'exit leg del giorno ha
chiuso le posizioni. Se il giorno X viene escluso dal confronto perché
"aperto" quando il job di riconciliazione gira, e i parametri cambiano
**tra la chiusura di X e l'esecuzione del catch-up** (che avviene
tipicamente il giorno X+1 o oltre), un catch-up ingenuo che legge "i
parametri attuali del profilo" userebbe i parametri nuovi (validi da
X+1) per riconciliare un giorno (X) che in realtà girava con i parametri
vecchi. Il backtest-replay del giorno X userebbe un comportamento di
strategia che quel giorno, in paper/live, non è mai esistito — il
confronto sarebbe tra due cose diverse e produrrebbe sia falsi positivi
(mismatch che non sono reali) sia falsi negativi (coincidenze casuali
scambiate per conferme).

#### Meccanismo: timeline append-only per profilo

Tabella nuova, es. `profile_param_versions` (nome indicativo, va allineata
allo stile dello schema esistente in fase di implementazione):

| colonna | significato |
|---|---|
| `profile` | `live` / `mirror` / `challenger` / `development` |
| `params_hash` | hash canonico di (STRATARGS, ticker file, data provider, auction flag, ecc.) |
| `stratargs` | JSONB, i parametri effettivamente usati |
| `code_commit` / `core_commit` | commit `CODE_ROOT` / `bt-core` di quella versione |
| `effective_from_date` | primo giorno di trading in cui questa versione è stata **effettivamente eseguita** |
| `effective_to_date` | ultimo giorno di trading in cui questa versione è stata **confermata attiva** (NULL = ancora corrente) |
| `source` | `observed_run` (scritto automaticamente da un run reale) vs `reconstructed` (backfill manuale/best-effort dai commenti Markdown negli `.env` o da git-blame) |

Regola di scrittura, da agganciare alla fase `entry` di `scheduled-job.sh`
(l'unico punto che già risolve `STRATARGS` + `commit` per un profilo, per
ogni esecuzione):

1. Ad ogni run di entry sul profilo `P`, in data `D`, calcolare
   `hash_oggi = hash(STRATARGS risolti, commit, core_commit, ticker file,
   data provider, auction flag)`.
2. Cercare la riga con `profile=P AND effective_to_date IS NULL` (la
   versione "corrente").
3. Se non esiste ancora nessuna riga corrente → INSERT una nuova riga con
   `effective_from_date=D`, `effective_to_date=NULL`.
4. Se esiste e `hash_oggi == hash` della riga corrente → **non toccare
   `effective_from_date`**; aggiornare solo un campo di appoggio tipo
   `last_confirmed_date=D` (necessario al passo 5).
5. Se esiste e `hash_oggi != hash` della riga corrente → è cambiata la
   versione:
   - `UPDATE` la riga corrente: `effective_to_date = last_confirmed_date`
     (l'ultimo giorno in cui quella versione è stata **osservata
     realmente in esecuzione**, non "ieri" per calendario — questo evita
     di sbagliare il confine quando ci sono buchi nel cron: festivi, run
     saltati, outage);
   - `INSERT` una nuova riga per la nuova versione con
     `effective_from_date=D`, `effective_to_date=NULL`,
     `last_confirmed_date=D`.

Questo garantisce che i confini delle versioni riflettano **quello che è
realmente girato**, non quello che qualcuno ha editato nel file di
config e quando — un edit fatto la sera di X ma applicato dal cron solo
il giorno X+1 produce correttamente `effective_from_date=X+1`, non la
data dell'edit.

Popolamento storico (pre-esistente all'introduzione di questa tabella):
best-effort, `source='reconstructed'`, dai commenti Markdown datati già
presenti in `config-common/scheduled/strategies/overnight-ah-*.env` o da
`git log`/`git blame` su quei file. **Non deve mai essere trattato con la
stessa fiducia di `source='observed_run'`**: se il catch-up di un giorno
molto vecchio dipende solo da una riga `reconstructed`, la UI/il report
deve segnalarlo esplicitamente (es. "parametri storici ricostruiti, non
osservati direttamente").

#### La query che risolve "quali parametri erano in vigore il giorno X" (punto critico)

```sql
SELECT *
FROM profile_param_versions
WHERE profile = :profile
  AND effective_from_date <= :trading_date
  AND (effective_to_date IS NULL OR effective_to_date >= :trading_date)
```

Regole non negoziabili su questa query:

- Deve sempre filtrare su `:trading_date` = **la data del giorno che si
  sta riconciliando**, mai su "oggi" / "data di esecuzione del job di
  catch-up". Questo è l'intero punto del requisito dell'utente.
- Deve restituire **esattamente una riga**. Zero righe → i parametri di
  quel giorno non sono noti (storia non ricostruita, o buco nella
  timeline): il catch-up per quel giorno va marcato esplicitamente
  "bloccato per parametri sconosciuti" e **mai** fatto ripiegare
  silenziosamente sulla versione corrente. Più di una riga → bug di
  integrità nello scrittore append-only (range sovrapposti): errore duro
  da correggere nel writer, non un caso da gestire prendendo "la prima".

#### Coda di recupero (catch-up queue)

Tabella nuova, es. `reconciliation_queue` (o riuso di `watchtower_windows`
esteso per profilo — da valutare in fase di design con lo schema reale):

| colonna | significato |
|---|---|
| `profile` | profilo cron |
| `trading_date` | il giorno X da riconciliare |
| `status` | `pending` / `done` / `blocked_missing_params` |
| `reason` | es. `open_day` |

Algoritmo giornaliero (idempotente, può girare ogni giorno senza effetti
collaterali se non c'è nulla da fare):

```
per ogni profilo:
  per ogni riga pending in reconciliation_queue, ordinata per trading_date asc:
    se il giorno NON è ancora chiuso (vedi sotto) -> lascia pending, continua
    parametri = resolve_params_as_of(profilo, trading_date)   # query sopra
    se parametri è None -> segna status = blocked_missing_params, continua
    esegui backtest-replay per trading_date con QUEI parametri (mai quelli attuali)
    confronta con gli ordini reali Alpaca di trading_date
    persisti il risultato, segna status = done
```

"Il giorno è chiuso" non significa "non è più oggi": significa che gli
ordini reali Alpaca di quel profilo per quella data sono tutti in stato
terminale (filled/canceled/rejected/expired) **e** che l'exit leg
(`moo-exit.sh` o il suo fallback `moo-exit-fallback.sh`) risulta
effettivamente eseguita per quella data — perché l'exit può fallire e
essere ritentata, quindi "è passata la mezzanotte" da solo non basta a
garantire che la giornata sia settled.

#### Esempio completo (lo scenario descritto dall'utente, passo per passo)

1. Giorno X: `development` gira in entry con `STRATARGS_v1`. Scrittore
   append-only: nessuna riga precedente → INSERT
   `(development, v1, from=X, to=NULL, last_confirmed=X)`.
2. Il job di riconciliazione gira lo stesso giorno X (o comunque prima che
   l'exit abbia settled): il giorno risulta "aperto" → niente confronto,
   niente alert, INSERT in `reconciliation_queue`
   `(development, X, pending, open_day)`.
3. In serata, `moo-exit.sh` chiude le posizioni di X. Il giorno X è ora
   effettivamente chiuso.
4. Ancora in giornata (o comunque prima del prossimo run di entry), i
   parametri vengono cambiati in config: `STRATARGS_v2`.
5. Giorno X+1: il cron gira in entry con `STRATARGS_v2`. Scrittore
   append-only: hash diverso dalla riga corrente (v1) →
   `UPDATE` riga v1: `effective_to_date = last_confirmed_date = X`;
   `INSERT` nuova riga `(development, v2, from=X+1, to=NULL,
   last_confirmed=X+1)`.
6. Il job di catch-up (che può girare il giorno X+1, o anche più tardi)
   trova la riga pending per `(development, X)`, verifica che X è ora
   chiuso (passo 3) → chiama `resolve_params_as_of(development, X)`.
   La query filtra su `effective_from_date <= X <= effective_to_date`:
   la riga v1 ha `from=X, to=X` → **match**; la riga v2 ha `from=X+1` →
   non matcha (X < X+1). Risultato: **v1**, correttamente — non v2, anche
   se v2 è la versione "corrente" nel momento in cui il catch-up gira.
7. Il backtest-replay per il giorno X viene eseguito con `STRATARGS_v1`,
   confrontato con gli ordini reali Alpaca di X, persistito, la riga in
   coda passa a `done`.

Se al passo 6 qualcuno avesse invece letto "i parametri attuali del
profilo `development`" (cioè il file `.env` così com'è quando gira il
catch-up), avrebbe ottenuto v2 — sbagliato, ed esattamente l'errore che
l'utente ha chiesto di evitare esplicitamente.

#### Casi limite da non ignorare

- **Giorni antecedenti all'introduzione della tabella**: nessuna riga
  `observed_run` disponibile. Se non è stato fatto un backfill
  `reconstructed` per quel periodo, il catch-up per quei giorni resta
  `blocked_missing_params` per sempre (non è recuperabile con certezza) —
  accettabile, va solo reso visibile in bt-dash, non nascosto.
- **Profilo decommissionato**: se un profilo smette di girare, la sua
  riga corrente (`effective_to_date IS NULL`) resta aperta a tempo
  indeterminato. Non è un bug della query, ma va gestito operativamente:
  chi decommissiona un profilo deve chiudere esplicitamente l'ultima
  riga (impostare `effective_to_date` all'ultimo giorno realmente
  girato), altrimenti resterebbe "aperta" per sempre senza impatti pratici
  (nessun nuovo giorno la interrogherà) ma con dati fuorvianti se
  qualcuno la legge manualmente.
- **Modifiche retroattive "per correggere un errore di config passato"**:
  la timeline è append-only e riflette ciò che è realmente girato, non
  ciò che avrebbe dovuto girare. Se si scopre che il giorno X è girato
  con un bug di config, la riga storica per X non va "corretta" a
  posteriori — rifletterebbe una versione mai eseguita davvero e
  invaliderebbe il confronto (che deve confrontarsi con ciò che
  *effettivamente* è successo). Un'eventuale correzione va registrata
  come nuova versione con la sua propria `effective_from_date`, mai
  retroattivamente sovrascritta.

### E. Copertura anche dei servizi (non solo cron)

Il monitoraggio esistente (Watchtower via Redis/live feed) copre già i
servizi intraday. Verificare se (A)/(B)/(C)/(D) sopra hanno senso anche per
loro riusando l'infrastruttura esistente, o se sono per costruzione già
coperti e il lavoro nuovo serve solo per il ramo cron.

### F. Fruibilità web: HTTPS, autenticazione minima, installabile (PWA)

Requisito aggiunto in questa conversazione: il monitoraggio deve essere
fruibile via web con autenticazione minima, HTTPS, e installabile (es.
PWA) — non solo raggiungibile su `127.0.0.1` durante lo sviluppo.

**Traefik (Docker) NON è il reverse proxy pertinente per questo
requisito** — correggendo un'ipotesi iniziale di questa stessa
conversazione: Traefik su questo host è configurato solo con
`--providers.docker=true`, nessun `certificatesresolvers`/ACME, nessun
entrypoint HTTPS generico — viene usato esclusivamente per esporre in
HTTP semplice, su porte dedicate non pubbliche, tool interni containerizzati
(pgadmin, phpMyAdmin, RedisInsight, Odoo, OnlyOffice, Fooocus). Non fa TLS
e non vede processi non-Docker come `bt-dash`/`bt-api`.

**Il vero front-door HTTPS pubblico di questo host è Apache**
(`apache2.service`, systemd, root, verificato attivo), con certificati
Let's Encrypt reali rinnovati da `certbot.timer` per il dominio
`ilz.duckdns.org` (e sotto-domini: `moneypenny-ilz.duckdns.org`,
`docs-ilz.duckdns.org`). Pattern già in uso per ogni servizio esposto
(`/etc/apache2/sites-available/*.conf`): un `VirtualHost` dedicato, o su
una porta propria (`Listen <porta>`, es. `openwebui.conf` su 2443,
`fooocus-api.conf` su 7443) o su un sotto-dominio proprio su 443, che fa
`ProxyPass`/`ProxyPassReverse` verso un backend su `127.0.0.1:<porta>`,
con supporto opzionale al websocket upgrade (`RewriteRule ... ws://`).

**Scoperta cruciale: esiste già un vhost preparato per esattamente questo
scopo, mai attivato.** File `/etc/apache2/sites-available/bt-dash.conf`
(creato 2026-04-15, ultima modifica 2026-04-17, **non presente in**
`sites-enabled/` — quindi mai andato in produzione):

```apache
Listen 1443
<VirtualHost *:1443>
    ServerName ilz.duckdns.org
    SSLEngine on
    SSLCertificateFile /etc/letsencrypt/live/ilz.duckdns.org/fullchain.pem
    SSLCertificateKeyFile /etc/letsencrypt/live/ilz.duckdns.org/privkey.pem
    <Location />
        AuthType Basic
        AuthName "Backtrader"
        AuthUserFile /etc/apache2/backtrader.htpasswd
        Require valid-user
    </Location>
    ProxyPass /api/ http://127.0.0.1:9090/
    ProxyPassReverse /api/ http://127.0.0.1:9090/
    ProxyPass / http://127.0.0.1:8082/
    ProxyPassReverse / http://127.0.0.1:8082/
    RewriteEngine On
    RewriteCond %{HTTP:Upgrade} =websocket [NC]
    RewriteRule /(.*) ws://127.0.0.1:8082/$1 [P,L]
</VirtualHost>
```

Risponde già, praticamente parola per parola, a tutti e tre gli aspetti
del requisito:
- **HTTPS**: sì, riusa il certificato Let's Encrypt già esistente e
  rinnovato automaticamente per `ilz.duckdns.org` — nessun nuovo dominio
  o certificato da procurarsi.
- **Autenticazione minima**: sì, `AuthType Basic` con
  `AuthUserFile /etc/apache2/backtrader.htpasswd` — **il file
  htpasswd esiste già sul disco** (`-rw-r--r-- root root, 49 byte,
  creato 2026-04-15`), quindi era già stata preparata almeno una coppia
  utente/password funzionante. Tutti i moduli Apache richiesti
  (`proxy`, `proxy_http`, `proxy_wstunnel`, `rewrite`, `ssl`,
  `auth_basic`, `authn_file`, `headers`) risultano già abilitati
  (`apache2ctl -M`) — nessuna dipendenza mancante.
- **Installabile**: la parte PWA resta da fare lato `bt-dash` (vedi
  sotto), ma una volta che il vhost è attivo la app è già servita da
  un'origin HTTPS valida, che è l'unico prerequisito bloccante lato
  browser per registrare un service worker/manifest.

**Bug da correggere prima di attivare il vhost**: le porte di backend nel
draft (`9090` per l'API, `8082` per il frontend) **non corrispondono**
alla configurazione attuale dei profili `prod`, quella esplicitamente
pensata per questo scenario. Verificato negli env file:
- `env/bt-api-prod`: `SERVER_PORT=19090`, `GUNICORN_BIND=127.0.0.1:19090`
  — non `9090` (quello è `bt-api-dev`).
- `env/bt-dash-prod`: `PORT=18082`, `BT_DASH_HOST=127.0.0.1`,
  `VUE_APP_API_URL=/api` con il commento esplicito nel file *"Frontend
  published behind reverse proxy on ilz.duckdns.org. Keeping a relative
  /api path avoids hard-coding the backend host in the bundle."* — non
  `8082` (quella porta, tra l'altro, è già usata da Traefik come
  entrypoint `pgadmin`: un conflitto reale se il vhost venisse attivato
  com'è).

  In altre parole: **il profilo `prod` di `bt-dash`/`bt-api` è già stato
  progettato apposta per stare dietro questo reverse proxy** (il
  commento nel file lo dice esplicitamente), ma il vhost Apache che
  dovrebbe puntarci non è mai stato aggiornato per usare quelle porte —
  probabilmente scritto in una fase precedente con porte diverse e poi
  abbandonato senza essere né completato né eliminato. Vanno riconciliati
  prima di attivare qualunque cosa.
- Nessuna istanza systemd `bt-dash@prod.service` / `bt-api@prod.service`
  risulta oggi attiva (`systemctl --user list-units` mostra solo le
  istanze `@dev`); essendo unit template (`%i`), sono avviabili con gli
  env file `prod` già pronti, ma vanno abilitate/avviate esplicitamente.
- `apache2ctl configtest` segnala oggi un errore preesistente e
  **non correlato** (certificato mancante per `docs-ilz.duckdns.org` in
  `docs-ilz.conf`) — non toccarlo, non fa parte di questo lavoro, ma va
  tenuto presente perché un `apache2ctl configtest`/reload fatto per
  attivare `bt-dash.conf` lo segnalerà comunque (preesistente, non una
  regressione introdotta da questo lavoro).

**Passi concreti per completare il requisito, in ordine:**

1. Correggere in `bt-dash.conf` le due `ProxyPass`/`ProxyPassReverse`
   (e la `RewriteRule` websocket) per puntare a `127.0.0.1:19090`
   (API) e `127.0.0.1:18082` (dash), coerenti con `bt-api-prod`/
   `bt-dash-prod`.
2. Verificare che le credenziali in `/etc/apache2/backtrader.htpasswd`
   siano ancora valide/note (creato ad aprile, potrebbe essere stato
   dimenticato) — eventualmente rigenerarle con `htpasswd`.
3. Abilitare e avviare `bt-api@prod.service` e `bt-dash@prod.service`
   (systemd user, stessi template già in uso per `@dev`).
4. Attivare il vhost: `a2ensite bt-dash`, `apache2ctl configtest`,
   reload di `apache2.service` — l'errore preesistente su `docs-ilz` va
   ignorato se non blocca il reload (verificare comunque, un
   `configtest` con errori potrebbe impedire il reload a seconda di come
   è strutturato l'errore).
5. Attivare la modalità `pwa` di Quasar per `bt-dash` (manifest +
   service worker generati dal CLI, nessuna riscrittura delle viste
   esistenti richiesta) e verificare che il build `prod` la includa.
6. Verifica end-to-end: raggiungere `https://ilz.duckdns.org:1443/`
   da browser, autenticarsi con basic-auth, controllare che le chiamate
   relative `/api/...` risolvano correttamente verso `bt-api` attraverso
   il proxy, e che il browser proponga "Installa app".

## Vincoli operativi da rispettare

- **Non duplicare** Postgres/schema/API/UI già esistenti: estendere.
  Un secondo Postgres o un secondo schema scollegato è accettabile solo se
  la sessione di design conclude esplicitamente che riusare
  `bt_live_events` crea più problemi che vantaggi — motivarlo per iscritto
  se succede.
- Rispettare `CLAUDE.md` di workspace: nuovi script di analisi/sweep in
  `backtrader/bin/{strategia}/`, non in `bt-core/bin/`.
- Qualunque backtest di replay deve passare dal meccanismo di download
  dati esistente (`load_tickers.py`, con il suo flock condiviso) — non
  bypassarlo.
- Non promuovere nulla su `backtrader-prod` (checkout live) senza
  promozione esplicita, per convenzione già in uso (vedi `ah_context.md`).
- I parametri storici per il backtest-replay non devono mai usare
  informazioni "future" rispetto alla data replayata (niente leakage).
- Prima di scrivere codice nuovo, verificare se
  `bin/watchtower_build_baseline.py` (non tracciato, non ancora ispezionato
  in questa sessione) è già un tentativo di questa stessa funzionalità.

## Vincolo cardine: il sistema deve essere agnostico rispetto a strategia/profilo/path

Emerso esplicitamente nel round di conferma con l'utente (2026-08-25),
vale per TUTTI i requisiti (A-F), non solo per la UI:

> "challenger va gestita fin da subito, tutta la dash deve essere
> indipendente dalla strategia e i path locali di run. domani avrò la
> strategia 'pinturicchio' che è schedulata nella cartella 'pippuzzo' che
> è un branch di main."

Nessuna parte del sistema (schema Postgres, API, job di replay, viste
bt-dash) deve avere hardcoded il set attuale di profili
(`live`/`mirror`/`challenger`/`development`), i nomi delle strategie
(`OvernightAH`/`OvernightAHFlatComposite`), o i path dei checkout
(`backtrader`/`backtrader-prod`). Tutto questo deve essere **dato di
configurazione scoperto a runtime**, non un elenco scritto nel codice:
- `profile_param_versions`/`reconciliation_queue`/il registro
  account↔strategia devono trattare `profile` e `strategy` come stringhe
  libere, mai come enum/if-else nel codice.
- Il job di replay deve risolvere dinamicamente checkout/branch/percorso
  dal profilo (via la config esistente in
  `~/.config/backtrader/scheduled/*.env`), non assumere solo
  `backtrader`/`backtrader-prod`.
- Le viste bt-dash devono popolarsi dai profili/strategie realmente
  presenti nel registro, non da una lista fissa in un componente Vue.

Inoltre `live` e `mirror` **non sono logicamente collegati dal nome**: il
nome "mirror" riflette solo l'intento originale (misurare il delta
paper/live) ma nel sistema vanno trattati come due profili indipendenti
qualunque, che possono divergere nei parametri nel tempo — nessuna
assunzione di sincronia va codificata.

Principio UX collegato (dalla stessa conversazione): per le viste bt-dash
(requisito E) l'usabilità complessiva conta più della struttura tecnica
sottostante — approccio "human" (leggibile, orientato a rispondere alle
domande dell'utente) non "machine" (dump tecnico di tabelle/JSON). Non è
rilevante se questo significhi estendere `Watchtower.vue` o creare pagine
nuove: la scelta implementativa è secondaria rispetto a questo principio.

## Decisioni chiuse con l'utente (round di conferma, 2026-08-25)

Risposte esplicite dell'utente alle domande aperte emerse dalla discovery
(vedi anche il vincolo cardine sopra, emerso nello stesso round):

- **Pipeline `live_event_stream` dead**: va **riparata**, non bypassata
  (fix del bug in `reconciliation_watchdog.py:311`, riavvio dei servizi).
  In scope.
- **Registro account↔strategia**: usare i profili reali
  `development`/`mirror`/`challenger` oltre a `live` (non le righe legacy
  `HMA-plain`/`OvernightAH` già presenti in `alpaca_portfolios`, che
  appartengono a un altro lavoro).
- **Promozione su `backtrader-prod`**: resta manuale ed esplicita, fatta
  dall'utente quando una strategia matura. Il codice in prod è "una
  versione stabile": il backtest per `live`/`mirror` deve usare quella
  stessa versione. La dashboard (bt-api/bt-dash) vive solo nel checkout di
  sviluppo e invoca `btmain.py` di altri checkout secondo necessità — non
  ha bisogno di essere lei stessa promossa.
- **`WT_STREAM`**: usato solo per chiamate schedulate, servizi, e i
  relativi backtest "mirati" (il replay di riconciliazione) — mai globale
  sull'intero processo `bt-api`. Conferma il design originale.
- **Architettura di A**: il replay "mirato" passa dalla pipeline stream
  (quella da riparare), non da un canale di solo polling indipendente —
  coerente col punto precedente.
- **`parallel_sim`**: NON è la base giusta per il replay del cron.
  Risolve un problema diverso — discordanze **runtime su flusso dati
  live** (es. dati in ritardo di 40s: Alpaca e sim restano allineati tra
  loro ma divergono dal backtest storico), utile in **daytrading/feed
  live**. Il cron usa dati **batch giornalieri**, che non possono avere
  quel tipo di discordanza — per il replay di A va costruito un flusso
  indipendente più semplice, non riusando `parallel_sim`.
- **Codice storico per il replay**: usare il **commit esatto** salvato
  nella timeline dei parametri (via `git worktree`/checkout storico), non
  assumere comportamento stabile su codice attuale. È responsabilità
  dell'utente garantire che ogni cambio di `STRATARGS` sia accompagnato
  da un commit reale, cosicché la timeline (D/D-bis) catturi sempre la
  coppia (parametri, commit) corretta.
- **`live` cron entry disabilitata**: intenzionale, resterà così finché
  la dashboard non monitora bene — motivo diretto per cui questo lavoro
  è prioritario.
- **Fix collisione `client_order_id`** (`_build_client_order_id()`
  tronca a 8 caratteri, tutti i profili `overnight_ah*` collidono sullo
  stesso prefisso): in scope, va risolto per rendere affidabile il
  guardrail C.
- **`challenger`**: coperta fin da subito in B/D insieme a
  `development`, non rimandata — coerente col vincolo cardine di
  genericità sopra.
- **Metriche di footprint, nomi tabelle, granularità "un commit/giorno"**:
  accettati i default proposti nell'analisi (metriche già in
  `baseline_metrics_from_trades`; nomi `profile_param_versions`/
  `reconciliation_queue` come da bozza; un commit/parametro per giorno è
  sufficiente).
- **Replay on-demand vs schedulato**: nessuna preferenza esplicita
  dell'utente ("boh"). Default scelto: **job schedulato separato, una
  volta al giorno** (non agganciato a ogni singola fase cron
  entry/exit), perché coerente con l'algoritmo di catch-up già
  progettato in D-bis (già pensato per girare quotidianamente in modo
  idempotente) e perché la riconciliazione di un giorno richiede comunque
  attendere che l'exit leg sia settled — un trigger on-demand subito dopo
  l'entry/exit non guadagnerebbe nulla, dato che il giorno risulterebbe
  comunque "aperto" ed escluso.
- **Alerting attivo**: esplicitamente NO per ora — richiesta diretta
  dell'utente ("voglio vedere che caspita viene fuori" prima di
  automatizzare notifiche). Solo dashboard/query in questa fase. Non
  aggiungere email/Telegram/altro senza una richiesta esplicita futura.

## Cosa NON è ancora deciso (da chiarire in fase di design, non assumere)

Nessun punto residuo dal round di discovery iniziale: tutte le decisioni
elencate sopra sono state chiuse con l'utente il 2026-08-25. Eventuali
nuove ambiguità emerse durante l'implementazione vanno portate qui prima
di essere assunte.

---

## Prompt di finalizzazione

Blocco pensato per essere copiato come primo messaggio di lavoro (in questa
sessione o in una nuova). Aggiornare la sezione "stato attuale" se questo
documento invecchia rispetto al codice.

```
GOAL

Finalizzare un sistema di monitoraggio che verifichi che le strategie di
trading di questo workspace (Backtrader + Alpaca) si comportino in
paper/live come atteso, confrontandole sistematicamente con un backtest di
riferimento. Il lavoro si innesta su un'infrastruttura Watchtower già
esistente (Postgres bt_live_events, servizi systemd
bt-live-event-writer/bt-watchtower, API bt-api/app/watchtower.py, viste
bt-dash Watchtower*.vue). IMPORTANTE: quella pipeline è oggi **inattiva da
~4 mesi** (bug in `reconciliation_watchdog.py:311`, servizi
`inactive (dead)`) — ripararla è la Fase 0 di questo lavoro, non
un'assunzione. Il gap principale riguarda le strategie schedulate via cron
a barra daily (famiglia overnight_ah: profili live, mirror, challenger,
development), che oggi non emettono alcun evento in quella pipeline.

STATO: la fase di discovery e allineamento con l'utente è GIÀ STATA FATTA
in una sessione precedente (2026-08-24/25). Tutte le decisioni aperte sono
chiuse — vedi la sezione "Decisioni chiuse con l'utente" nel documento di
analisi. NON riaprire queste decisioni, NON rifare la discovery da zero:
parti dai risultati già raccolti e vai diretto al piano/implementazione.

VINCOLO CARDINE (letto per primo, vale per OGNI requisito sotto)

Il sistema deve essere agnostico rispetto a strategia/profilo/path.
Nessun elenco hardcoded dei profili attuali (live/mirror/challenger/
development), dei nomi strategia, o dei checkout (backtrader/
backtrader-prod) in nessun layer (schema, API, job di replay, viste
bt-dash). Tutto è dato di configurazione scoperto a runtime, perché domani
può comparire una nuova strategia in un nuovo profilo/branch senza che
nulla nel codice debba cambiare. Vedi la sezione dedicata nel documento di
analisi per il dettaglio e la citazione esatta dell'utente.

CONTESTO OBBLIGATORIO DA LEGGERE PRIMA DI SCRIVERE CODICE

Leggi per intero, non a campione:
- docs/context/watchtower_cron_monitoring_brief.md (questo documento —
  analisi completa, tassonomia divergenze, schema Postgres, vincolo
  cardine di genericità, TUTTE le decisioni chiuse con l'utente, vincoli
  operativi)
- docs/context/alpaca_paper_live_overnight_ah.md (tassonomia divergenze
  ordine backtest/paper/live)
- bt-core/watchtower_runtime.py, bt-core/reconciliation_watchdog.py
  (contiene il bug riga ~311 da fixare), bt-core/live_event_writer.py
- bt-core/config/sql/live_events_postgres.sql (schema completo)
- bt-core/strategies/multiTickerStrategy.py (righe ~412-429 bug labeling
  broker_paper/broker_live, righe ~621-662 bug collisione
  `_build_client_order_id()` da fixare)
- bt-core/strategies/overnight_ah.py (righe ~469-482: `auction=True` è già
  forzato internamente in modalità backtest — NON serve tradurlo nel
  replay, il brief lo segnalava come da tradurre ma è stato corretto)
- bt-api/app/watchtower.py, bt-api/app/scheduler.py (lo scheduler Python
  APScheduler qui dentro è per prove interne, NON per ordini reali — vedi
  vincolo cardine e decisioni chiuse)
- bt-dash/src/bt/pages/Watchtower.vue (2201 righe, vista principale già
  matura e portfolio_key_id-aware — punto di partenza più concreto per E
  rispetto alle altre due pagine, che sono più di servizio)
- bin/compare_live_backtest_trades.py, bin/alpaca_audit/,
  bin/watchtower_build_baseline.py
- bin/parallel_sim/ — ESISTE completo su disco ma NON va riusato per il
  replay del cron (risolve un problema diverso, discordanze runtime su
  flusso live, non applicabile a dati batch giornalieri — vedi decisioni
  chiuse)
- CLAUDE.md di workspace
- /home/htpc/bin/bt-scheduled (symlink a backtrader-prod/scripts/
  scheduled-job.sh, condiviso da tutti i profili) e
  scripts/scheduled/handlers/, ~/.config/backtrader/scheduled/*.env,
  config-common/scheduled/strategies/overnight-ah-*.env

REQUISITI FUNZIONALI (dettaglio completo nel documento di analisi)

A. Execution reconciliation giorno-per-giorno per account cron. Backtest
   "replay" con STRATARGS storici + **commit esatto storico** (via git
   worktree/checkout, non codice attuale — decisione esplicita
   dell'utente) risolti dalla timeline di D/D-bis. `auction` non va
   tradotto (già gestito dal codice). Cash da equity storica reale
   (bin/alpaca_audit). Include il punto 5: giorno "aperto" escluso dal
   confronto e rimesso in coda per il catch-up (D-bis).

B. Strategy footprint drift, pre vs post attivazione, con parametri
   storicizzati. Copre `development` E `challenger` fin dall'inizio (non
   rimandare `challenger`).

C. Registro account -> strategia assegnata (profili reali: live, mirror,
   challenger, development — non le righe legacy già presenti in
   alpaca_portfolios). Guardrail multi-strategia sullo stesso account.
   Richiede il fix della collisione `_build_client_order_id()` per essere
   affidabile.

D/D-bis. Timeline machine-readable (parametri, commit ESATTO) per profilo/
   data, scritta append-only alla fase entry di scheduled-job.sh. Algoritmo
   di catch-up dei giorni esclusi con risoluzione punto-nel-tempo — vedi
   la sezione "D-bis" nel documento di analisi, letta per intero prima di
   implementare. Replay schedulato una volta al giorno (non on-demand
   dopo ogni fase cron — decisione chiusa).

E. Estensione viste bt-dash. Approccio "human", non "machine": usabilità
   complessiva prima della struttura tecnica. Se estendere Watchtower.vue
   o fare pagine nuove è un dettaglio implementativo, non rilevante per
   l'utente.

F. Fruibilità web (HTTPS, basic-auth, PWA) — percorso già tracciato nella
   sezione F del documento di analisi (vhost Apache quasi pronto, 6 passi
   concreti). Quasi solo esecuzione, non design.

FASE 0 (nuova, non nel documento originale)

Prima di tutto: riparare la pipeline live_event_stream (fix
reconciliation_watchdog.py:311, riavvio bt-live-event-writer.service e
bt-watchtower.service) — decisione esplicita dell'utente, in scope.
Verificare che torni a scrivere eventi/riconciliazioni prima di costruire
qualunque cosa sopra.

VINCOLI

- Riusa l'infrastruttura Postgres/API/UI esistente. Non duplicarla.
- Rispetta CLAUDE.md di workspace.
- Qualunque backtest-replay deve passare da load_tickers.py (flock
  condiviso) — non bypassarlo.
- Promozione su backtrader-prod resta manuale/esplicita, fatta
  dall'utente stesso: non promuovere nulla autonomamente. Il codice della
  dashboard/job di replay vive nel checkout di sviluppo e invoca btmain.py
  degli altri checkout secondo necessità, senza bisogno di essere promosso
  lui stesso.
- NIENTE alerting attivo (email/Telegram/etc.) in questa fase — richiesta
  esplicita dell'utente, solo dashboard/query per ora.
- NON riusare bin/parallel_sim/ per il replay del cron (vedi sopra).
- Rispetta il VINCOLO CARDINE di genericità in ogni riga di codice/schema
  che scrivi.

METODO DI LAVORO ATTESO

La discovery e l'allineamento con l'utente sono già fatti (vedi STATO
sopra). Non ripeterli. Procedi così:

1. Rileggi la discovery già fatta (sezioni "Scoperte" e "Decisioni chiuse"
   nel documento di analisi) e verifica solo che lo stato del codice non
   sia cambiato da allora (git log rapido sui file citati) — non rifare
   discovery estesa da zero.
2. Scrivi/conferma un piano a fasi verificabili con criteri di
   accettazione concreti, nell'ordine: Fase 0 (riparazione pipeline) →
   D/D-bis (timeline parametri, prerequisito hard) → A (execution
   reconciliation, incluso catch-up) → C (guardrail account↔strategia,
   incluso fix client_order_id) → B (footprint drift, development +
   challenger) → E (viste bt-dash) → F (esposizione web/PWA).
3. Implementa una fase alla volta, verificando su dati reali prima di
   passare alla successiva. Per ogni fase che tocca infrastruttura
   condivisa fuori dal workspace (vhost Apache, servizi systemd, crontab)
   conferma con l'utente prima di applicare, anche se il piano è già
   approvato nel merito.

CRITERIO DI SUCCESSO FINALE

Per almeno uno dei profili cron overnight_ah (probabilmente "development"),
il sistema deve rispondere, con dati reali e senza intervento manuale:
- per un giorno specifico, gli ordini in paper/live coincidono con quelli
  di un backtest eseguito con gli stessi parametri E COMMIT di quel
  giorno? Se no, perché (categoria di divergenza)?
- un giorno precedentemente escluso perché "aperto" viene recuperato coi
  parametri corretti di quel giorno, anche se nel frattempo sono cambiati?
- il footprint della strategia dopo l'attivazione è statisticamente
  coerente con quello storico pre-attivazione (per development E
  challenger)?
- l'account di quel profilo riceve ordini solo da quella strategia
  (nessuna collisione client_order_id)?
- quali parametri/commit erano in vigore in quella data, visualizzabili
  in bt-dash?
- la dashboard è raggiungibile via HTTPS, protetta da basic-auth, e il
  browser la propone come installabile (PWA)?
- aggiungendo un profilo/strategia mai visto prima (senza toccare codice,
  solo config), il sistema lo tratta correttamente?
```
