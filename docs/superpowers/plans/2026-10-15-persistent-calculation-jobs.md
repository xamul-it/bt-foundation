# Piano operativo: calcoli persistenti e ricalcolo dalla baseline

Stato: pronto per implementazione. Questo documento non implementa né avvia calcoli.
Workspace: `/home/htpc/backtrader`. Destinatario: Codex con GPT-5.6 Sol.

## 1. Obiettivo e comportamento richiesto

- Riutilizzare APScheduler per avviare creazione baseline e ricalcolo Backtest.
- Conservare in Postgres ogni esecuzione, inclusi esito, avanzamento, log e risultato.
- Aggiungere accanto al selettore baseline di Watchtower il pulsante
  **Ricalcola ultime 25 giornate** per il profilo visualizzato.
- Il ricalcolo usa la configurazione salvata nella baseline selezionata,
  congelata all'avvio; non rilegge gli STRATARGS correnti durante il lavoro.
- La creazione baseline mantiene il comportamento attuale: usa la configurazione
  corrente del profilo, ma la congela prima di accodare il job.
- Riaprendo una pagina si ritrovano job attivi e ultimo risultato.
- Scheduler distingue le pianificazioni ricorrenti dallo storico delle esecuzioni
  manuali; i job conclusi non spariscono quando APScheduler rimuove il DateTrigger.

## 2. Vincoli da rispettare

1. Conservare tutte le modifiche preesistenti, anche nei submodule e negli script
   untracked. Leggere gli eventuali AGENTS.md applicabili prima di modificare file.
2. Nessuna modifica a parametri operativi dei profili, strategie o cron broker.
   Nessun ordine broker. Le letture equity storica già previste sono ammesse.
3. Aggiornare soltanto le evidenze Backtest per il profilo e le date richieste.
   Conservare tutte le evidenze Alpaca. Non estendere le autorizzazioni ad altri profili.
4. Non promuovere codice al checkout `/home/htpc/backtrader-prod`.
   L'utente chiama la destinazione online «becktrade»: non dedurre la destinazione
   da nomi di script o unità contenenti «prod». Verificare i percorsi effettivi.
5. La build online verificata in questa sessione è la PWA in `bt-dash/dist/pwa`,
   servita da Apache su `/dash`; `/api` punta a localhost:19090. Lo script esistente
   è `scripts/bt-dash-build-prod.sh`; il nome non significa promozione del trading.
   Riverificare la configurazione prima di pubblicare.
6. Nessun ricalcolo reale sul DB condiviso come effetto dei test o di un riavvio.
   I test usano mock o database isolato. Un ricalcolo reale parte solo dal comando
   esplicito dell'utente o dal pulsante introdotto, con conferma della sostituzione.
7. Nessun nuovo framework, Redis, Celery o secondo scheduler. Lavoro sequenziale,
   senza subagenti. Nessun commit automatico.

## 3. Evidenze già verificate: punti di ingresso

- `bt-api/app/scheduler.py`: APScheduler, `_set_job_runtime`, `job_listener`,
  endpoint `/dyn/sc/jobs`. Lo stato JSON conserva solo ultimo esito dei job;
  `/jobs` enumera i job presenti in memoria. Pool attuale: 20 thread.
- `bt-api/app/service/watchtower_scheduler_service.py`: subprocess e lock di
  manutenzione cross-process. Non cambiare i timer o bloccare poll/watchdog dietro
  un lungo calcolo baseline.
- `bt-api/app/watchtower.py`: `_start_profile_baseline_job`, dizionario
  `_profile_baseline_jobs`, thread daemon e polling oggi non persistenti.
- `bt-core/profile_baseline.py`: `compute_profile_baseline`, `load_profile_env`,
  preflight checkout, validazione provenienza e builder del comando.
- `bin/watchtower_build_profile_baseline.py`: CLI esistente per creazione baseline.
- `bin/watchtower_backfill_current_params.py`: bonifica esistente, backup prima
  delle scritture e transazione per giornata; oggi usa sempre parametri correnti.
- `bt-core/watchtower_runtime.py`: repository, schema applicativo e `profile_cockpit`.
- `bt-core/config/sql/live_events_postgres.sql`: schema idempotente esistente.
- `bt-dash/src/bt/pages/WatchtowerCronMonitoring.vue`: selettore baseline,
  ricaricamento overview e filtro unico 10/25/50 giornate, default 25.
- `bt-dash/src/bt/pages/ProfileBaselineManager.vue`: start e polling ogni 3 secondi;
  oggi l'id del job scompare al cambio pagina.
- `bt-dash/src/bt/pages/Scheduler.vue` e `src/bt/components/JobTable.vue`:
  pannello scheduler, polling e dialog output.

Baseline recenti verificate: development #85 e mirror #94 hanno params,
effective_params, run_config, result_snapshot, commit e artifact_manifest.
Baseline legacy #35 e #34 hanno provenienza incompleta. Non hardcodare questi id.

Bug già corretti da preservare:

- `profile_cockpit` riconosce `regenerated_backtest_execution` e non oscura i
  suoi prezzi/P&L. I prezzi di decisione restano esclusi.
- Il filtro dashboard conta giornate/righe; non deve tornare a giorni di calendario
  o a una seconda paginazione nascosta.

## 4. Decisioni di implementazione

### Persistenza

Aggiungere tabella `watchtower_calculation_jobs` tramite migrazione SQL idempotente
e documentare applicazione/rollback. Campi minimi:

- `id` UUID, `kind` (`baseline_build`, `backtest_rebuild`), `profile`, `baseline_id`;
- `status`: queued, running, completed, failed, interrupted;
- `request_snapshot` JSONB: configurazione risolta, date, baseline e identità codice;
- `progress_done`, `progress_total`, `phase`, `current_date` (nome SQL non riservato,
  ad esempio `processing_date`), `heartbeat_at`;
- `created_at`, `started_at`, `finished_at`, `updated_at`;
- `error`, `result` JSONB, `log_path`, `output_tail` limitato.

Vincolo parziale UNIQUE su profile per status queued/running: una sola operazione
pesante attiva per profilo, anche tra creazione baseline e ricalcolo. Usare un claim
atomico `UPDATE ... WHERE status='queued' RETURNING ...`, non lock solo in Python.
Log completi in directory dedicata con id generato dal server; percorsi mai forniti
dal client. Output restituito dall'API limitato e senza credenziali.

### Esecuzione e riavvio

- Executor APScheduler dedicato `calculations`, un solo worker per questi calcoli.
  Le manutenzioni ordinarie mantengono il loro executor e i timer attuali.
- Il DB è la fonte di verità. Inserire prima la richiesta queued, poi schedulare
  il runner con DateTrigger. Il runner effettua claim e avvia un subprocess.
- Alla ripartenza recuperare le richieste queued. Per running verificare proprietà,
  heartbeat e lock del worker: non marcare interrotto il lavoro vivo di un'altra
  istanza. Quello realmente abbandonato diventa interrupted, senza rilancio automatico.
- Usare lock cross-process per la capacità globale e per la mutazione sul profilo;
  acquisizione non bloccante con ritorno in coda, senza occupare thread all'infinito.
- Timeout esplicito configurabile per tipo di calcolo. Terminare e raccogliere il
  gruppo subprocess a timeout/shutdown; mantenere heartbeat anche quando il figlio
  non emette output. Non lasciare figli che continuano a scrivere dopo interrupted.
- Completato solo dopo esito subprocess e verifica del risultato. Errori per giornata
  raccolti nel risultato: un job con giornate fallite ha stato failed e conserva i successi.
- I listener esistenti per job ricorrenti non devono trattare questi job come
  strategie schedulate o copiarne/spostarne gli artefatti.

### Baseline selezionata

- Verificare sul server che esista e appartenga al profilo richiesto.
- Costruire snapshot da params/effective_params/run_config e provenienza salvati;
  controllare semantica dei campi nel codice, non concatenare alla cieca i dizionari.
  Congelare strategia, ticker, provider/feed, commissione, leva e STRATARGS.
- Prima versione: eseguire solo baseline con configurazione completa e commit
  compatibili col checkout disponibile. Se richiedono codice diverso, rifiutare
  con spiegazione; nessun checkout automatico o sostituzione silenziosa del codice.
  Non usare il solo hash complessivo per confrontare richieste con date diverse.
- Registrare dataset attuale e hash/manifest dei file letti; non promettere una
  riproduzione del dataset storico della baseline. Nessun download implicito.
- Le 25 giornate sono le ultime 25 sessioni concluse secondo il calendario condiviso.
  Congelare l'elenco nel job. Escludere la sessione corrente non conclusa; verificare
  disponibilità delle barre necessarie all'uscita prima di sostituire dati esistenti.
- Usare equity storica del profilo come nella bonifica esistente e dichiararla
  nello snapshot. Non inventare capitale se manca.

## 5. Attività in ordine, con risultato verificabile

### A. Repository e ciclo di vita persistente

1. Aggiungere schema/migrazione e metodi repository per creazione, ricerca,
   claim, progress, heartbeat, esito e recupero dopo riavvio.
2. Aggiungere test isolati per transizioni, vincolo duplicati e claim concorrente.
3. Aggiungere servizio `bt-api/app/service/calculation_job_service.py` e worker
   `bin/watchtower_calculation_worker.py` con allowlist dei soli due tipi.
   Evitare dipendenza circolare: il servizio riceve scheduler/repository dal chiamante.
4. Registrare executor e recupero una sola volta nel lifecycle esistente;
   verificare il comportamento anche se Gunicorn carica più worker.

Accettazione: un job finto termina e resta consultabile, un job fallito conserva
errore/log, un running abbandonato è interrupted e un queued viene recuperato una volta.

### B. Worker di calcolo e bonifica

1. Estrarre soltanto quanto necessario dalla bonifica esistente in funzioni
   importabili; mantenere CLI corrente compatibile e modalità piano senza scritture.
2. Aggiungere ingresso da snapshot immutabile con baseline_id e job_id.
   Non ricaricare `load_profile_env` per sovrascrivere i parametri congelati.
3. Pubblicare backup completo prima della prima mutazione. Per ogni giornata
   validare artefatti, poi sostituire in una transazione decisioni, settlements,
   verifiche e risultato Backtest. Conservare campi Alpaca.
4. Aggiornare progress al commit di ogni giornata e registrare risultato/errore
   per data. Associare job_id/baseline_id e provenienza ai record ricostruiti.
5. Collegare creazione baseline allo stesso worker e snapshot; conservare preflight,
   provenienza e controlli esistenti. Restituire baseline_id solo dopo insert verificato.

Accettazione: fixture 25 date, 25 esiti; rollback della singola data fallita;
nessuna mutazione Alpaca; lettura cockpit conserva prezzi e P&L rigenerati.

### C. API

Contratto suggerito (adattare solo se il repository ha convenzioni più precise):

- `POST /dyn/obs/watchtower/cron/<profile>/backtest-rebuild` con baseline_id,
  window=25 e conferma esplicita `replace_backtest=true`: valida e accoda, HTTP 202.
- Creazione baseline mantiene la route attuale e restituisce job persistente.
- `GET /dyn/sc/calculations?profile=...&limit=...` elenco bounded.
- `GET /dyn/sc/calculations/<job_id>` stato e risultato.
- `GET /dyn/sc/calculations/<job_id>/log` coda bounded del log.
- Preservare l'endpoint di polling baseline esistente come adattatore ai job nuovi.

Risposte: 404 baseline/job assente; 400 input/config incompleti; 409 lavoro attivo
o incompatibilità codice; 503 persistenza indisponibile. Mantieni auth attuale.
Non accettare comandi shell, percorsi di log o DSN dal browser. Nessuna mutazione GET.

Accettazione: test Flask di contratto, baseline di altro profilo respinta, doppio
click non crea due ricalcoli e richiesta invalida non avvia subprocess.

### D. Interfaccia

1. In Watchtower, pulsante vicino alla baseline: disabled senza baseline utilizzabile
   o con job attivo sul profilo. Tooltip con causa del blocco.
2. Conferma prima di accodare con nome profilo, baseline/id, ultime 25 sessioni e
   descrizione chiara della sostituzione Backtest. Per mostrare date esatte usare
   preview server read-only che condivide la stessa validazione della POST.
3. Pannello stato con fase, progress X/25 per bonifica, tempi, errore e link ai log.
   Per baseline lunga senza progress misurabile mostrare fase/durata e heartbeat,
   mai percentuali inventate.
4. Al mount e al cambio profilo cercare job persistenti. Poll ogni 3 secondi,
   ignorare risposte stale dopo cambio profilo/job; interrompere timer all'unmount.
   Errori di polling mostrati come errore di monitoraggio, non fallimento del job.
5. A completamento bonifica ricaricare overview; dopo baseline build ricaricare elenco.
6. ProfileBaselineManager recupera stato persistente senza dipendere dalla memoria
   della pagina. Scheduler aggiunge tabella esecuzioni con tipo/profilo/baseline,
   stato, progress, tempi, log e risultato. Nessun pulsante pause/resume dei timer
   applicato alle esecuzioni finite o ai subprocess di calcolo.
7. Conservare filtro unico 10/25/50 giornate default 25. Gli endpoint del cockpit
   devono fornire abbastanza decisioni/settlements per tutte le 25 sessioni:
   verificare limiti di righe (es. 240 settlements) quando i trade sono numerosi.

### E. Test e consegna

- Eseguire i test mirati nuovi e quelli scheduler_state, scheduler_chains,
  watchtower_profile_baselines, profile_baselines, backfill_consistency,
  cockpit_rebuilt_prices, più `node --test bt-dash/tests/*.test.cjs`.
- Per pytest eseguire dalle rispettive root bt-api/bt-core con il relativo venv,
  controllando conftest e isolamento: import scheduler non deve lanciare job reali.
- Testare con fixture: cambio pagina, cambio profilo durante polling, errore worker,
  timeout, restart, collisione, baseline legacy, codice incompatibile e dati insufficienti.
- Fare almeno una verifica dell'API con payload controllato dal cockpit: 25 date,
  nessun prezzo/P&L oscurato quando l'esecuzione rigenerata è valida.
- Build PWA con `bash scripts/bt-dash-build-prod.sh` dopo verifica destinazione.
  Non usare solo `npx quasar build` (produce SPA, non la PWA online).
- Migrazione idempotente prima di caricare API nuova. Verificare servizio effettivo
  e DB target senza stampare segreti. Reload/riavvio API dopo i controlli; non eseguire
  una bonifica per dimostrare il deploy. Documentare quali verifiche live sono state fatte.
- Aggiornare questo piano con checklist, test ed eventuali limiti reali. Handoff:
  file modificati, comandi/test, percorso build, migrazione e istruzioni di ripristino.

## 6. Checkpoint per implementazione con GPT-5.6

Procedere A → B → C → D → E. Dopo ogni fase aggiornare qui lo stato e proseguire
senza richiedere conferme per scelte ordinarie già definite nel piano. Se la sessione
finisce, lasciare ultimo passo completato e prossimo comando concreto.

- [x] A: persistenza, claim, executor, recupero e test
- [x] B: worker, snapshot baseline, progress, backup e test
- [x] C: API e compatibilità polling
- [x] D: pulsante, recupero monitoraggio, storico Scheduler
- [x] E: regressioni, migrazione/build e verifica destinazione

Non dichiarare completo perché il DB contiene righe: verificare il payload passato
alla UI, come dimostrato dal bug dei prezzi nascosti. Non attribuire un difetto al
profilo selezionato o al browser senza evidenze.

## 7. Esito implementazione (2026-10-15)

- Migrazione idempotente applicata due volte al DB dell'API: tabella con 21
  colonne, quattro indici presenti e zero job creati durante test/deploy.
- Preview read-only verificata su una baseline completa scelta dinamicamente:
  25 sessioni (2026-09-09..2026-10-13), parametri dello snapshot uguali ai
  parametri salvati e manifest del dataset corrente presente. Nessun ricalcolo
  reale è stato accodato o eseguito.
- Test API mirati: 27 passati; test core mirati: 26 passati; test dashboard:
  24 passati. Inclusi collisione per profilo, claim concorrente, recovery,
  timeout, baseline legacy/incompatibile, conferma API, 25 date e STRATARGS
  congelati, rollback atomico per giornata e conservazione Alpaca.
- Build PWA riuscita con `bash scripts/bt-dash-build-prod.sh`. Destinazione
  verificata da Apache: `/dash` serve `/home/htpc/backtrader/bt-dash/dist/pwa`;
  `/api` inoltra a `127.0.0.1:19090`. Il checkout
  `/home/htpc/backtrader-prod` non è stato modificato.
- API effettiva `bt-api@prod.service` riavviata dalla working directory
  `/home/htpc/backtrader/bt-api`; avvio Gunicorn verificato e route protetta
  `/dyn/sc/calculations` raggiungibile via proxy (401 senza sessione, come atteso).
- Limiti intenzionali: il ricalcolo usa il dataset corrente e l'equity storica
  letta al momento dell'esecuzione; non ricostruisce il dataset storico della
  baseline. Baseline incomplete/legacy e commit incompatibili sono rifiutati.
  Un job `interrupted` non viene rilanciato automaticamente.
