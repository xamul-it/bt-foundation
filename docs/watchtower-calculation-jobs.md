# Job persistenti Watchtower

Le creazioni baseline e i ricalcoli Backtest manuali sono registrati in
`watchtower_calculation_jobs`. Postgres conserva stato, snapshot della richiesta,
progresso, risultato e riferimento al log anche dopo la rimozione del `DateTrigger`
di APScheduler o il cambio pagina.

## Migrazione

Applicare la migrazione prima di riavviare l'API:

```bash
set -a
. env/bt-live-events
set +a
psql "$LIVE_EVENTS_DB_DSN" -v ON_ERROR_STOP=1 \
  -f bt-core/config/sql/watchtower_calculation_jobs.sql
```

Lo script è idempotente. Il rollback richiede prima lo stop dell'API e la verifica
che non vi siano job attivi o storico da conservare:

```sql
DROP TABLE watchtower_calculation_jobs;
```

Il rollback del codice va eseguito insieme al rollback della PWA/API; lasciare la
tabella inutilizzata è comunque innocuo e conserva lo storico.

## Esecuzione e limiti

- `baseline_build` e `backtest_rebuild` sono gli unici tipi ammessi dal worker.
- È consentito un solo job `queued`/`running` per profilo e un solo calcolo globale
  alla volta. Il claim è atomico nel DB; i lock file proteggono anche più processi API.
- Il ricalcolo accetta soltanto `window=25` e richiede `replace_backtest=true`.
- Le 25 date sono congelate in preview/accodamento e richiedono una sessione
  successiva già conclusa, necessaria alla barra di uscita.
- Il worker conserva le evidenze Alpaca, crea un backup JSON completo del perimetro
  Backtest prima della prima mutazione e committa una giornata alla volta.
- Un riavvio recupera i job in coda. Un worker vivo conserva il job `running`; un
  worker abbandonato diventa `interrupted` e non viene rilanciato automaticamente.

Timeout configurabili:

- `BT_BASELINE_CALCULATION_TIMEOUT_SECONDS` (default 14400);
- `BT_REBUILD_CALCULATION_TIMEOUT_SECONDS` (default 21600);
- `BT_REBUILD_DAY_TIMEOUT_SECONDS` (default 900).

I log sono generati dal server in `out/watchtower-calculations`; il browser non può
fornire percorsi, DSN o comandi. I backup per giornata sono in
`bin/watchtower_scratch/calculation_<job-id>` (oppure `BT_WATCHTOWER_SCRATCH`).

Non usare il test o un riavvio per dimostrare il ricalcolo: un job reale parte solo
dal pulsante confermato o dalla relativa POST autenticata.
