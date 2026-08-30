# Watchtower — Stato e Roadmap di Completamento (2026-08-29)

## Scopo di questo documento

Handoff per una nuova sessione Claude Code. Non ripete l'analisi (già in
`watchtower_cron_monitoring_brief.md`): dice cosa è fatto e verificato, cosa è
approvato ma non implementato, cosa resta deciso ma non promosso, e cosa è
ancora aperto dal brief originale.

## Fatto e verificato in produzione

Fasi 0-6 del brief originale, tutte completate e confermate dall'utente:
- Fase 0: pipeline `reconciliation_watchdog.py` riparata
- Fase 1: timeline parametri per profilo (D-bis), hook in `scheduled-job.sh`
- Fase 2: execution reconciliation + catch-up (A), incluso fix `auction=True`
  forzato dall'harness di replay
- Fase 3: guardrail account↔strategia (C), fix collisione `client_order_id`
  (promosso a `backtrader-prod`)
- Fase 4: strategy footprint drift (B) per `development`/`challenger`
- Fase 5: viste `bt-dash` + endpoint `bt-api` (`/dyn/obs/watchtower/cron/*`)
- Fase 6: esposizione web HTTPS/PWA via Apache (`ilz.duckdns.org:1443`,
  `/dash`), auth basic, build statica (non `quasar dev`, mai serve un service
  worker funzionante)

Più due incidenti risolti dopo la Fase 6:
- **OOM #1** (`bt-api@prod`, SIGKILL ripetuti): codice Fase 5 scagionato con
  load-test reali; fix difensivi (`gunicorn.conf.py` `max_requests`, `LIMIT`
  espliciti su `list_profile_param_versions`/`list_pending_reconciliation`)
- **OOM #2** (root cause reale): cache calendario Alpaca stale
  (`config-common/cache/alpaca_calendar_cache.json`, ferma dal 5/8) rendeva
  senza limite superiore una finestra "ancora aperta" in
  `_window_for_anchor()` — un sync di boundary classificava erroneamente
  ~24 giorni di ordini in un solo giorno stale. Fix: guardia
  `_MAX_OPEN_WINDOW_AGE = timedelta(days=2)` in `watchtower_runtime.py`.
  Causa a monte: `_calendar_from_cache()` in `load_tickers.py` trattava
  qualunque mese già toccato come completo per sempre, anche il mese
  corrente. Fix: il mese corrente è "soddisfatto" solo se ricaricato oggi,
  si autoripara al massimo una volta al giorno.
- **Blocco host 29/8** (09:02-13:25, ~4h20 irresponsivo): causa reale
  **memoria, non backtrader** — picco di commit memoria alle 09:00 (dati
  `sar`: %commit 96,6%→134,1%, buffer cache 656MB→41MB) → swap-thrashing
  mai risolto in un OOM-kill pulito, journald stesso si è fermato. Nessun
  cron backtrader vicino alle 09:00; i job Watchtower 12:00/12:30 non sono
  nemmeno partiti (sistema già bloccato). Trovati anche (rumore di fondo
  cronico, non causa) `mrz-api`/`mrz-worker`/`moneypenny` in crash-loop da
  mesi (>212.000 riavvii) — disabilitati. **Fix**: memory cgroup guard
  (`MemoryHigh`/`MemoryMax`/`MemorySwapMax=0`/`ManagedOOMSwap=kill`) sui 4
  servizi systemd di backtrader (`bt-api@`, `bt-watchtower`,
  `bt-live-event-writer`, `bt-dash@`) — **committato e mersi** (PR #3,
  `12baf42`/`60d6712`).

## Implementato (branch `feat/baseline-manager`, non ancora merged)

- **Pagina "Configurazione" (onboarding profili schedulati)** — FATTO.
  `bt-dash/src/bt/pages/ScheduledProfileConfiguration.vue`, route
  `/ScheduledProfiles/Configuration`, riorganizzazione menu (gruppi
  "Strategie Schedulate" / "Strategie Intraday"). Spec:
  `docs/superpowers/specs/2026-08-29-scheduled-profile-config-page-design.md`.
- **Drill-down per-giorno della riconciliazione** su "Profili Cron"
  (`WatchtowerCronMonitoring.vue`): riga-giorno espandibile con backtest
  vs Alpaca per simbolo, lettura in italiano delle categorie di
  divergenza, caveat sul commit `nearest_by_date`.
- **Baseline Manager** — FATTO. Spec
  `docs/superpowers/specs/2026-08-29-baseline-manager-design.md`, piano
  `docs/superpowers/plans/2026-08-29-baseline-manager.md`. Componenti:
  - tabella `profile_baselines` (schema `config/sql/live_events_postgres.sql`,
    non tracciato in git come tutte le tabelle watchtower);
  - `bt-core/profile_baseline.py` (compute + drift, riusa
    `evaluate_outcomes` / `monte_carlo_subset_test`); CRUD su
    `WatchtowerRepository`; `bin/watchtower_build_profile_baseline.py`;
  - endpoint `bt-api`: `GET/POST/DELETE
    /dyn/obs/watchtower/cron/<profile>/baselines`,
    `GET /dyn/obs/watchtower/cron/baselines/jobs/<job_id>`,
    `GET /dyn/obs/watchtower/cron/<profile>/baselines/<id>/drift`
    (job backtest in background);
  - `bt-dash`: pagina `ProfileBaselineManager.vue`
    (`/ScheduledProfiles/Baselines`) + selettore baseline / pannello drift
    nella pagina Configurazione; indicatore rinominato "Footprint
    pre/post attivazione".
  - Nota: `pyproject.toml` di `bt-core` ha una lista `py-modules`
    esplicita — `profile_baseline` è stato aggiunto lì per l'import da
    `bt-api`.
  - Limite noto: il drift-check di un profilo il cui provider non ha dati
    aggiornati a oggi (es. `development` su `yahoo_adj`) restituisce
    `status=insufficient_recent_data` finché i dati non vengono rinfrescati.

## Decisioni prese ma non promosse/committate (da confermare con l'utente)

Questi fix sono verificati e funzionanti nella working tree ma **non
committati** (repo nested, policy di questa sessione: commit solo su
richiesta esplicita):

- `bt-core/watchtower_runtime.py` — guardia `_MAX_OPEN_WINDOW_AGE` (OOM #2)
- `bt-core/load_tickers.py` — self-heal cache calendario (causa a monte di
  OOM #2), verificato contro Alpaca reale, cache di produzione già
  effettivamente rinfrescata come effetto collaterale del test
- `bt-api/gunicorn.conf.py` — `max_requests`/`max_requests_jitter`

**Domanda aperta esplicita**: `load_tickers.py` esiste anche in
`backtrader-prod` (copia separata, usata dai cron `live`/`mirror` prima di
ogni run per il download ticker). Il fix del self-heal cache calendario non è
stato promosso lì — stesso pattern di promozione già usato per
`submit_moo.py` (Fase 3) e la riga in `scheduled-job.sh` (Fase 1).

### Nota — `watchtower_runtime.py` committato nel branch `feat/baseline-manager`

Il commit `f5437b5` di quel branch ha inglobato l'intero blocco di lavoro
watchtower pregresso della working tree (guardia `_MAX_OPEN_WINDOW_AGE`,
requisiti A/B/C/D, `insert_reconstructed_version`, `profile_cockpit`, …),
non solo il CRUD `profile_baselines`. `load_tickers.py`,
`reconciliation_watchdog.py`, `strategies/multiTickerStrategy.py`,
`bt-api/gunicorn.conf.py` restano invece non committati.

### Data repair — buchi in `profile_param_versions` (2026-08-30)

Le righe `reconstructed` di `mirror` (id 17), `challenger` (id 11) e
`development` (id 15) avevano `effective_to_date` collassato a un solo
giorno, lasciando scoperti ~6-7 settimane fino alla riga `observed_run`
del 28/08. `resolve_params_as_of` falliva per ogni giorno nel buco →
riconciliazione non ri-eseguibile lì. Fix: `UPDATE` di `effective_to_date`
(+`last_confirmed_date`) a `2026-08-27` su quelle 3 righe — il
`params_hash` coincide con quello che le righe di
`execution_reconciliation_results` di quei giorni già referenziavano.
Backfill riconciliazione completato: ultimi 30 giorni 100% arricchiti con
`bt_day_return_pct` / `live_day_return_pct` + spaccato entry/exit. Restano
non arricchite solo 16 righe `development`/`mirror` del 14–24/07 (fuori
dalla finestra di backfill).

## Aperto dal brief originale — requisito E, mai affrontato

Il brief lasciava esplicitamente da verificare: *"il monitoraggio esistente
copre già i servizi intraday? A/B/C/D hanno senso anche per loro?"* —
**verificato oggi (in un'altra conversazione dello stesso giorno): no, non è
coperto.** `parallel-sim` (il meccanismo che fa girare le strategie intraday
in parallelo su broker reale + simulato) non ha alcuna superficie API in
`bt-api`, nessuna pagina in `bt-dash`. Nemmeno `WT_STREAM` (il trigger di
streaming progettato nella sezione "Trigger di streaming" del brief) è mai
stato implementato in `multiTickerStrategy.py`.

Questo è un lavoro di design separato, non ancora iniziato: la spec di
"Configurazione" scritta oggi copre esplicitamente solo il lato cron e
rimanda il lato servizi/intraday a un'iterazione futura. Il menu in
`bt-dash` distingue ora concettualmente i due meccanismi (gruppo "Strategie
Schedulate" vs "Strategie Intraday", quest'ultimo ancora da rinominare in
implementazione) ma il contenuto/API per il lato intraday resta da
progettare da zero.

## Riferimenti

- `docs/context/watchtower_cron_monitoring_brief.md` — analisi originale,
  requisiti A-F, vincolo cardine (agnostico a strategia/profilo/path)
- `docs/superpowers/specs/2026-08-29-scheduled-profile-config-page-design.md`
  — spec pagina Configurazione (lato cron)
- `docs/scheduled-trading-operations.md` — procedura onboarding profilo
  (base pre-Watchtower, ancora valida per i passi manuali)
