# Pagina "Configurazione" (onboarding profili schedulati) — Design Spec
Date: 2026-08-29

## Scopo

Oggi, per attivare una nuova strategia schedulata (day+ timeframe, tipo
`overnight_ah`) con un nuovo account Alpaca, i passi da fare esistono ma sono
sparsi tra file di config, `docs/scheduled-trading-operations.md`, e la testa
di chi ha costruito il sistema. Non c'è un unico posto in `bt-dash` che dica
"ecco cosa fare fuori dash" e "ecco cosa è già a posto per il profilo X".

Questa spec definisce una nuova pagina **Configurazione** in `bt-dash` che
copre esattamente questo: checklist con istruzioni copiabili + stato live per
verificare ogni passo, per lo scenario **strategie schedulate via cron**.

## Vincolo cardine (invariato)

Come tutto il resto di Watchtower (`docs/context/watchtower_cron_monitoring_brief.md`),
questa pagina deve restare indipendente da strategia/profilo/path. Nessun nome
di profilo hardcoded: il profilo è un campo libero digitato dall'utente, i
template sostituiscono `<profile>` col valore digitato, lo stato live viene
letto per qualunque nome di profilo passato (esistente o non ancora creato).

## Terminologia (chiarita in sessione)

"Watchtower" è l'ombrello di monitoraggio e comprende **due meccanismi**
distinti:
- **Cron** — strategie a timeframe day o superiore, girano schedulate via
  `bt-scheduled`/crontab, verificate a posteriori tramite reconciliation
  (`profile_param_versions`, `execution_reconciliation_results`, ecc.). Questo
  è lo scope di questa spec.
- **Servizi** — strategie intraday, girano su un flusso live, verificate in
  parallelo contro un broker reale e uno simulato (`parallel-sim`). **Fuori
  scope per questa iterazione** — la relativa pagina di configurazione è un
  lavoro futuro separato.

## Modifiche al menu (`bt-dash/src/layouts/MainLayout.vue`)

Oggi "Profili Cron" vive sotto il gruppo "Watchtower" insieme a pagine
intraday (Overview, Feed Monitoring), il che confonde i due meccanismi.

Modifiche:
1. Nuovo gruppo **"Strategie Schedulate"** con due voci:
   - "Profili Cron" (spostata da sotto "Watchtower" — stessa pagina,
     `WatchtowerCronMonitoring.vue`, nessuna modifica al contenuto)
   - "Configurazione" (nuova pagina, oggetto di questa spec)
2. Il gruppo "Watchtower" rimanente (Overview, Feed Monitoring, Service
   Monitoring) viene rinominato **"Strategie Intraday"**. Nessuna modifica al
   contenuto delle pagine, solo label del gruppo — riflette che "Overview" e
   "Feed Monitoring" sono entrambe intraday. "Service Monitoring" (salute
   systemd generica, trasversale a tutto) resta lì per ora: non è né
   specificamente cron né specificamente intraday, e riclassificarla è fuori
   scope.
3. "Scheduler" (in-process APScheduler-style job control, endpoint `/dyn/sc/*`)
   non viene toccato — è un meccanismo diverso dal cron OS usato dai profili
   `overnight_ah*`, riclassificarlo è fuori scope.

## Nuova pagina: Configurazione

File: `bt-dash/src/bt/pages/ScheduledProfileConfiguration.vue`
Route: `/ScheduledProfiles/Configuration`

### Fonte dati

Nessun nuovo endpoint backend. La pagina riusa
`GET /dyn/obs/watchtower/cron/<profile>/overview` (già esistente, restituisce
`profile_cockpit()`: timeline parametri, account registrato, riconciliazioni,
footprint, alert — tutto in una chiamata) e
`GET /dyn/obs/watchtower/cron/profiles` (per l'autocomplete dei profili già
noti). Chiamare l'endpoint overview con un nome di profilo mai visto prima
deve restituire liste/valori vuoti, non un errore — da verificare in fase di
implementazione (le query sono `WHERE profile = %s`, nessuna riga trovata è
il comportamento atteso, ma va confermato che nessuna funzione sollevi
un'eccezione su "profilo sconosciuto").

### Sezione 1 — Selettore profilo

Campo di testo libero con autocomplete (suggerimenti dai profili restituiti
da `/watchtower/cron/profiles`, ma il valore digitato non deve essere
vincolato alla lista — un profilo nuovo non esiste ancora in DB). Nessun
default: la pagina non mostra checklist/stato finché non è stato digitato un
nome.

### Sezione 2 — Checklist "fuori dash"

Statica, renderizzata **in pagina** (non link a documenti esterni — richiesta
esplicita). Ogni voce mostra path esatto e template copiabile con `<profile>`
sostituito dal valore digitato in Sezione 1. Contenuto (allineato a
`docs/scheduled-trading-operations.md` + le estensioni Fase 1/3 di
Watchtower):

1. **Account Alpaca** — creare `~/.config/backtrader/accounts/<profile>.env`
   con permessi `0600`:
   ```
   ALPACA_API_KEY=...
   ALPACA_SECRET_KEY=...
   BROKER_API_KEY=...
   BROKER_SECRET_KEY=...
   ```
   Nota in pagina: mai dedurre le credenziali da un file esistente di un
   altro profilo — vanno create ex novo.

2. **Parametri strategia** — creare
   `config-common/scheduled/strategies/<profile>.env`:
   ```
   STRAT=<modulo.Classe>
   TICKER=<file_universo.json>
   DATA_PROVIDER=<provider>
   ALPACA_FEED=<feed>
   FROM_DAYS=<n>
   MAX_EXPOSURE=<n>
   MARGIN_LEVERAGE=<n>
   ENTRY_TIF=<tif>
   ENTRY_HANDLER=<handler>
   EXIT_HANDLER=<handler>
   EXIT_FALLBACK_HANDLER=<handler>
   STRATARGS="..."
   ```

3. **Config profilo schedulato** — creare
   `~/.config/backtrader/scheduled/<profile>.env`:
   ```
   ROLE=<profile>
   TRADING_MODE=paper|live
   CODE_ROOT=<path checkout>
   ACCOUNT_ENV=/home/htpc/.config/backtrader/accounts/<profile>.env
   STRATEGY_CONFIG=config-common/scheduled/strategies/<profile>.env
   RUN_ID=<run_id>
   ```
   Regole da richiamare esplicitamente in pagina (validate da
   `bt-scheduled`): `ROLE` deve coincidere col nome profilo; `TRADING_MODE`
   solo `paper` o `live`; se `TRADING_MODE=live`, `CODE_ROOT` deve essere
   `backtrader-prod` sul branch `prod`.

4. **Crontab** — tre righe:
   ```
   <min> <ora> * * 1-5 /home/htpc/bin/bt-scheduled <profile> entry
   <min> <ora> * * 1-5 /home/htpc/bin/bt-scheduled <profile> exit
   <min> <ora> * * 1-5 /home/htpc/bin/bt-scheduled <profile> exit-fallback
   ```
   Verificare prima con `bt-scheduled --check <profile> <phase>` e
   `--dry-run` per ogni fase.

5. **Registrazione account↔profilo** (guardrail, Fase 3):
   ```
   bt-core/.venv/bin/python bin/watchtower_register_profile_accounts.py
   ```

6. **Nota rassicurante, esplicita in pagina**: non serve nessun passo
   aggiuntivo per i poller Watchtower — `watchtower_poll_alpaca_orders.py` e
   `watchtower_replay_reconcile.py` scoprono i profili dinamicamente dal DB
   (vincolo cardine), un nuovo profilo viene raccolto automaticamente dal
   primo cron successivo alla registrazione.

7. **Opzionale** — se il profilo ha storico pregresso da recuperare:
   `bin/watchtower_backfill_profile_params.py`.

### Sezione 3 — Stato live

Per il profilo digitato in Sezione 1, letto da
`/watchtower/cron/<profile>/overview`, un pannello con indicatori
✅/❌/⚪ (⚪ = opzionale, non blocca):

| Indicatore | Derivato da |
|---|---|
| Parametri registrati | `current_version != null` |
| Account collegato | `registry != null` |
| Prima riconciliazione avvenuta | `reconciliation_results.length > 0` |
| Baseline footprint calcolata (opzionale) | `footprints.length > 0` |
| Alert guardrail aperti | `open_guardrail_alerts.length` (evidenziato se > 0) |

### Sezione 4 — Link successivo

Una volta che tutti gli indicatori bloccanti sono ✅, link diretto a
"Profili Cron" (`WatchtowerCronMonitoring.vue`) per il monitoraggio
continuo.

## Cosa NON è incluso (scope)

- Questa *pagina Configurazione* resta di sola lettura/verifica per la
  checklist di onboarding (file env, crontab): non perché ci sia un
  vincolo generale "nessuna azione da UI" — non esiste — ma solo perché
  qui non serve. Azioni da UI che scrivono su DB / lanciano job sono
  ammesse dove hanno senso: vedi `2026-08-29-baseline-manager-design.md`
  (la pagina "Gestione baseline" lancia backtest e scrive
  `profile_baselines`).
- Nessuna configurazione per strategie intraday/`parallel-sim` — sezione
  futura separata, oggi `parallel-sim` non ha alcuna superficie API in
  `bt-api` da cui leggere stato.
- Nessuna modifica al contenuto delle pagine intraday esistenti, solo
  rinomina del gruppo di menu.
- Nessuna modifica a "Service Monitoring" o "Scheduler" (APScheduler).

## Verifica

- `GET /watchtower/cron/<profile>/overview` con un profilo mai esistito in
  DB: conferma che non lancia eccezioni e che ogni campo usato per gli
  indicatori è effettivamente `null`/`[]` invece di sollevare errore.
- Rendering della pagina con tre casi reali: profilo pienamente onboardato
  (es. `development`), profilo con solo alcuni passi fatti (se disponibile
  un caso reale, altrimenti simulato), profilo mai esistito.
- Verifica visiva che i template in Sezione 2 sostituiscano correttamente
  `<profile>` col valore digitato, aggiornandosi a ogni digitazione.
- Verifica che il menu riorganizzato non rompa nessuna route esistente: tutte
  le route restano invariate (`/Watchtower`, `/Watchtower/FeedMonitoring`,
  `/Watchtower/ServiceMonitoring`, `/Watchtower/CronMonitoring`) — la
  riorganizzazione è solo nel menu (`MainLayout.vue`), non nel router. Solo
  la nuova pagina Configurazione introduce una route nuova
  (`/ScheduledProfiles/Configuration`).
