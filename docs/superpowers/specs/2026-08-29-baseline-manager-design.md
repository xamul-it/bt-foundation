# Baseline Manager — Design Spec (scheduled profiles)

Date: 2026-08-29
Related: `2026-08-29-scheduled-profile-config-page-design.md`,
`watchtower_cron_monitoring_brief.md`,
`watchtower_completion_roadmap_2026-08-29.md`

> Stato: **design, 3 decisioni prese** (D1a / D2b-nuova-tabella
> `profile_baselines` / D3-default-10). Prossimo passo: conversione in
> piano di implementazione bite-sized.
>
> Nota sul nome: le baseline **non sono schedulate** — sono generate
> on-demand da una maschera e selezionate come baseline di confronto. La
> tabella si chiama `profile_baselines` (non `scheduled_baselines`): il
> legame è col `profile` cron, non con uno scheduler.

## Scopo

Dare un posto in `bt-dash` per **calcolare, salvare, elencare, selezionare
e cancellare più baseline statistiche** di una strategia schedulata, e per
**verificare che la finestra recente di backtest sia ancora della stessa
statistica della baseline** (rilevare un regime sfavorevole).

Richiesta utente, testuale:
- "la baseline è un semplice run della strategia dal 2000 a … inizio anno"
- "una maschera che consente di eseguire i run, caricare più baseline in
  cui configuro data inizio, data fine e la versione della strategia (es:
  scegliendo una data passata). Questo sta in una pagina/popup per
  calcolare le baseline"
- "nella maschera attuale [pagina Configurazione] aggiungi la possibilità
  di selezionare una baseline (o cancellarla)"
- "calcolo la probabilità che gli ultimi N giorni siano della stessa
  statistica [della baseline]"

## Modello a 3 livelli (chiarito in sessione)

Tre confronti distinti, da non mischiare:

| Livello | Confronto | Domanda | Dati | Stato |
|---|---|---|---|---|
| **1. Stabilità strategia** | baseline **backtest** (finestra lunga, congelata) vs **backtest** recente (ultimi N giorni) | il regime è ancora favorevole a questa strategia? | solo backtest | **oggetto di questa spec** |
| **2. Footprint pre/post attivazione** | backtest pre-`activation_date` vs backtest post-`activation_date`, parametri correnti | l'edge ddeployato regge dalla sua attivazione? | solo backtest | esiste: `watchtower_compute_footprint_drift.py` + `strategy_footprints`. **Non toccato qui.** |
| **3. Fedeltà esecuzione** | backtest replay vs ordini Alpaca, giorno per giorno | il reale diverge dal backtest? perché? | replay + `alpaca_order_cache` | esiste: `watchtower_replay_reconcile.py` + `execution_reconciliation_results`. Vista per-giorno aggiunta a "Profili Cron" (punto C, già fatto). **Non toccato qui.** |

Livello 1 **non esiste oggi** come feature gestita. Ci sono pezzi
riutilizzabili (sotto).

## Stato attuale del codice (verificato)

### `stat_baselines` (tabella) + API `/dyn/obs/watchtower/baselines/*`
- Colonne: `id, strategy, strategy_fingerprint, params_hash, params(jsonb),
  sample_size, quality_score, metrics(jsonb), source_meta(jsonb), computed_at`.
- **Nessuna** colonna di finestra temporale, **nessun** nome/label,
  **nessun** legame al `profile`. Chiave logica = strategy + fingerprint +
  params_hash. Oggi contiene **1 riga** (2026-04-09).
- `WatchtowerRepository`: `upsert_baseline(...)`, `fetch_baseline(...)`,
  `list_baselines(strategy=, fingerprint=, limit=)`,
  `compute_baseline_from_sources(strategy, fingerprint, params_data,
  source_paths, source_root=, generator_schema_version=, notes=)`,
  `baseline_status_for_run(run_id)`, `baseline_for_run(run_id)`.
  **Nessun** `delete_baseline`.
- `compute_baseline_from_sources` **ingerisce `trades.json`** da una o più
  cartelle di output backtest già prodotte — calcola `metrics` via
  `baseline_metrics_from_trades()`. **Non lancia lui il backtest** e **non
  ha parametri data-inizio / data-fine / versione**: prende tutti i trade
  che trova nei file.
- Endpoint: `GET /watchtower/baselines`,
  `GET /watchtower/baselines/by-run/<run_id>`,
  `POST /watchtower/baselines/recompute` (avvia `_start_baseline_job`,
  thread daemon, 202 + `job_id`), `GET /watchtower/baselines/jobs/<job_id>`.
- Usato oggi solo da `Watchtower.vue` (pagina intraday) via
  `evaluate_outcomes(current_values, baseline_metrics)` — che già calcola
  z-mean/z-median, KS distance, confidence, ecc.

### `strategy_footprints` (tabella)
- Colonne: `profile, strategy, strategy_fingerprint, params_hash,
  params(jsonb), period, activation_date, window_start, window_end,
  sample_size, metrics(jsonb), source_meta(jsonb), computed_at`.
- **Ha** le date di finestra ed è **per-profilo** — più vicina a quello
  che serve — ma `period` è un enum a 2 valori (`pre_activation` /
  `post_activation`) legato allo split del footprint drift.

### Lancio backtest
- **Non esiste** un endpoint che lancia `btmain.py` per un backtest
  arbitrario con `--fromdate/--todate/--stratargs`.
- `watchtower_compute_footprint_drift.py::run_history_backtest()` è
  l'esempio più vicino: costruisce ed esegue `btmain.py --mode backtest
  --fromdate … --todate … --stratargs …` in `subprocess.run`, cwd
  `CODE_ROOT/bt-core`.
- `watchtower_replay_reconcile.py` ha la macchina per il **git worktree
  sul commit storico** (checkout di un commit passato in una scratch dir).
- `resolve_params_as_of(profile, trading_date)` restituisce gli STRATARGS
  storicizzati a una data → "versione = data passata" lato parametri è
  già risolvibile.
- Blueprint `/dyn/mn/*` = "Main backtest execution" (da verificare in fase
  di piano se riutilizzabile o se conviene un runner dedicato).

## Decisioni prese

- **D1 → D1a**: versione = STRATARGS storicizzati a `as_of_date`
  (`resolve_params_as_of`), backtest sul codice corrente. Niente git
  worktree. D1b (commit storico) rinviata.
- **D2 → D2b**: **nuova tabella `profile_baselines`**, isolata da
  `stat_baselines` (intraside) e da `strategy_footprints` (livello 2).
- **D3 → default 10 giorni di trading**, parametro `recent_window_days`
  esposto in UI, range 3–30. Confronto via `evaluate_outcomes` +
  `monte_carlo_subset_test`.

Dettaglio del ragionamento sotto (conservato per contesto).

### D1 — "Versione della strategia scegliendo una data passata": solo parametri o anche commit del codice?

- **D1a (solo parametri)**: la versione = STRATARGS storicizzati a quella
  data (`resolve_params_as_of`). Il backtest gira sul **codice corrente**
  del checkout. Semplice, nessun git worktree.
- **D1b (parametri + commit)**: come il replay — checkout del commit del
  codice in vigore a quella data in una scratch dir, backtest lì. Fedele
  al 100% ma richiede la macchina worktree e run più lenti/pesanti.

*Raccomandazione:* **D1a** per la prima versione. Una baseline serve a
misurare il regime di mercato sotto una configurazione, non a certificare
un commit; se conta il commit esatto è il livello 3 (fedeltà esecuzione).
Aggiungere D1b dopo se emerge il bisogno.

### D2 — Dove vivono le baseline del cron: riuso `stat_baselines` o nuova tabella?

- **D2a (riuso `stat_baselines` + estensione)**: aggiungere colonne
  `label text`, `profile text`, `window_start date`, `window_end date`,
  `code_commit text NULL`, e un `delete_baseline(id)`. Riusa `list_baselines`,
  `evaluate_outcomes`, il job `_start_baseline_job`, la pagina intraday
  che già le mostra. Rischio: la tabella è condivisa con l'uso intraday,
  la chiave logica cambia (oggi 1 baseline per strategy+fingerprint+hash;
  con più baseline nominate serve `label` nella chiave).
- **D2b (nuova tabella dedicata, poi chiamata `profile_baselines`)**:
  isolata dal mondo intraday, schema su misura (label, profile, finestra,
  commit, metrics, pnl_values). Più codice nuovo (repo methods +
  endpoint), zero rischio di regressione sull'intraday.
- **D2c (riuso `strategy_footprints` con `period='baseline:<label>'`)**:
  ha già finestra + profilo; si allarga l'enum `period`. Mescola due
  concetti (footprint drift automatico vs baseline gestita a mano) sulla
  stessa tabella e sugli stessi metodi `list_strategy_footprints`.

*Raccomandazione:* **D2b** (nuova tabella). Il costo è modesto e tiene il
livello 1 completamente separato dal 2 e dall'intraday, coerente con "sono
due cose diverse".

### D3 — Finestra "backtest recente" per il confronto di stabilità

- L'utente: "non ho elementi per dire se 5, 10, 3 o 20 giorni".
- Serve un default parametrico. Il footprint drift usa già
  `evaluate_outcomes` con Monte Carlo subset test — riusabile.

*Raccomandazione:* **default 10 giorni di trading, parametro
`recent_window_days` esposto nella UI** (range consentito p.es. 3–30).
Il confronto usa `evaluate_outcomes(recent_pnl, baseline.metrics)` +
`monte_carlo_subset_test` come fa `compute_drift_verdict`.

## Design (D1a + D2b + D3)

### Entità: `profile_baselines` (nuova tabella)

```
id             bigserial pk
profile        text            -- profilo cron (free text, mai hardcoded)
strategy       text
label          text            -- nome scelto dall'utente, univoco per (profile,label)
params         jsonb           -- STRATARGS usati (risolti da resolve_params_as_of)
params_hash    text
as_of_date     date null       -- data-versione parametri scelta dall'utente
window_start   date
window_end     date
code_commit    text null       -- riservato per D1b futura, per ora NULL
ticker         text            -- universo usato
provider       text
sample_size    bigint
metrics        jsonb           -- output baseline_metrics_from_trades()
pnl_values     jsonb           -- lista pnl_pct per Monte Carlo / KS
run_id         text            -- id del run backtest che l'ha prodotta
source_meta    jsonb
created_at     timestamptz default now()
UNIQUE (profile, label)
```

Metodi repo nuovi (`WatchtowerRepository`):
`list_profile_baselines(profile)`, `get_profile_baseline(baseline_id)`,
`insert_profile_baseline(...)`, `delete_profile_baseline(baseline_id)`.

### Backend (`bt-api/app/watchtower.py` + `bt-core/watchtower_runtime.py`)

Nuovi endpoint sotto `/dyn/obs/watchtower/cron/`:

| Metodo | Path | Fa |
|---|---|---|
| `GET` | `/<profile>/baselines` | lista baseline del profilo (`repo.list_profile_baselines(profile)`) |
| `POST` | `/<profile>/baselines` | avvia job: run backtest + salva baseline. Body: `{label, window_start, window_end, as_of_date?}`. Ritorna `202 {job_id}` |
| `GET` | `/baselines/jobs/<job_id>` | stato job (`queued|running|completed|failed`, + `baseline_id` se completed) |
| `DELETE` | `/<profile>/baselines/<id>` | `repo.delete_profile_baseline(id)` |
| `GET` | `/<profile>/baselines/<id>/drift?recent_window_days=N` | esegue/legge il confronto stabilità: run backtest ultimi N giorni di trading, `evaluate_outcomes(recent_pnl, baseline.metrics)`, ritorna verdetto |

Job runner (nuovo modulo `bt-api/app/baseline_runner.py` o funzione in
`watchtower.py`, sullo stile di `_start_baseline_job`):
1. Risolve `params` = `resolve_params_as_of(profile, as_of_date or window_end)`.
2. Legge `TICKER/DATA_PROVIDER/...` dall'env del profilo
   (`~/.config/backtrader/scheduled/<profile>.env` +
   `config-common/scheduled/strategies/<profile>.env`), come fa
   `watchtower_compute_footprint_drift.py`.
3. `subprocess.run` di `btmain.py --mode backtest --fromdate window_start
   --todate window_end --stratargs <resolved> --id <run_id>` in
   `CODE_ROOT/bt-core`.
4. Legge `out/.../<run_id>/trades.json`, calcola `metrics` via
   `baseline_metrics_from_trades()`, estrae `pnl_values`.
5. `repo.insert_profile_baseline(...)`.
6. Aggiorna lo stato job in memoria (dict + lock, come i job esistenti).

Vincolo cardine invariato: `profile` è sempre free text, nessun nome
hardcoded; gli endpoint funzionano per qualunque nome.

### Frontend

**Nuova pagina/popup: "Gestione baseline"**
File: `bt-dash/src/bt/pages/ProfileBaselineManager.vue`
Route: `/ScheduledProfiles/Baselines` (voce nel gruppo di menu "Strategie
Schedulate"). Valutare in fase di piano se sia meglio un `q-dialog` aperto
dalla pagina Configurazione — la richiesta utente dice "pagina/popup".

- Selettore profilo (stesso pattern testo-libero+autocomplete della pagina
  Configurazione).
- Form "Nuova baseline": `label`, `window_start` (default 2000-01-01 o
  prima data dati disponibili), `window_end` (default 1° gennaio anno
  corrente), `as_of_date` opzionale (versione parametri) — con avviso che
  usa il codice corrente (D1a). Bottone "Calcola" → `POST` → polling job
  con progress/spinner.
- Tabella baseline esistenti: label, finestra, sample_size, quality_score,
  data calcolo, azioni (vedi metriche, **cancella** con conferma).

**Pagina Configurazione (`ScheduledProfileConfiguration.vue`) — aggiunte:**
- Sezione nuova "Baseline / stabilità strategia":
  - `q-select` per **scegliere** una delle baseline del profilo (o "nessuna").
  - Se selezionata: pannello drift — `recent_window_days` (default 10),
    bottone "Verifica ora", esito: verdetto (`ok` / `warning`),
    confidence, z-mean/z-median, KS, dimensione campione recente; frase in
    italiano ("comportamento recente coerente / possibile regime
    sfavorevole").
  - Link a "Gestione baseline" per crearne/cancellarne.
- L'indicatore "Baseline footprint" attuale (livello 2) resta, ma va
  **rinominato** per non confonderlo con la baseline gestita (livello 1) —
  es. "Footprint pre/post attivazione".

## Cosa NON è incluso

- Nessuna modifica a `watchtower_compute_footprint_drift.py` /
  `strategy_footprints` (livello 2) né a `watchtower_replay_reconcile.py`
  (livello 3).
- Nessun git worktree / commit storico (è D1b, rinviata).
- Nessuna schedulazione automatica del ricalcolo baseline: la baseline è
  congelata per definizione; si ricalcola solo a mano dalla pagina
  Gestione. (Il drift-check ricorrente sì, ma è una decisione successiva:
  cron o on-demand dalla pagina.)
- Nessun riuso della pagina intraday `Watchtower.vue`.
- La vista per-giorno della reconciliation (punto C) è già implementata a
  parte in `WatchtowerCronMonitoring.vue`.

## Verifica (quando sarà implementato)

- `POST /<profile>/baselines` con `development`: job arriva a `completed`,
  riga in `profile_baselines`, `sample_size` > 0.
- `GET /<profile>/baselines` la elenca; `DELETE` la rimuove.
- Profilo mai visto: `GET .../baselines` → `[]`, nessun errore.
- Drift con finestra recente su `development`: verdetto coerente con
  `evaluate_outcomes` chiamato direttamente sugli stessi pnl.
- Pagina Configurazione: selezione baseline persiste nella UI, pannello
  drift si aggiorna al variare di `recent_window_days`.
- Nessuna regressione su `GET /watchtower/baselines` (intraday) e su
  `Watchtower.vue`.

## Prossimo passo

1. L'utente decide **D1 / D2 / D3**.
2. Si converte questo design in
   `docs/superpowers/plans/2026-08-29-baseline-manager.md` (piano
   bite-sized, TDD, task da 2–5 min) con la sub-skill
   `superpowers:writing-plans`.
