# Documentazione e studi

Servizio locale condiviso da frontend, API autenticata, MCP e CLI. Offre ricerca
ibrida (testuale + semantica), modalità testuale o semantica separate, filtri
espliciti, catalogo, navigazione degli studi e lettura delle fonti con righe e hash.
Non interpreta automaticamente filtri dalle domande e non genera risposte.

## Archivio condiviso

Non è stato creato un secondo archivio. La configurazione predefinita legge:

- indice persistente `bt-rag-poc2-workspace-g311` nel container esistente;
- manifest/stato in `research/rag_poc_20261109/phase2/runtime/`;
- inventario e schede in `research/rag_poc_20261109/ingestion/`;
- documenti originali nelle cartelle del workspace;
- embeddings TEI locali già condivisi con DocsGPT, Granite 311M R2 a 768 dimensioni.

Il codice operativo non importa i moduli della PoC. La collocazione dei dati e
il nome dell'indice restano quelli esistenti per mantenere compatibilità senza
migrazione o duplicazione. `BT_KNOWLEDGE_DATA` permette di cambiare la radice dati
con una migrazione esplicita futura. Il modello e la revisione TEI sono verificati
prima della ricerca semantica; un modello diverso richiede reindicizzazione.

## Accessi

Frontend: route `/Knowledge`, voce **Documentazione e studi** nel menu principale.

API: `/dyn/knowledge/status`, `/catalog`, `/studies`, `/study?id=...`,
`/source?path=...&start_line=1&line_count=100` e `POST /search`.
Il proxy pubblico continua a usare `/api/dyn/knowledge/...`. Tutte le route
passano dal normale controllo di sessione. Nessun accesso browser diretto a
OpenSearch o al filesystem; le fonti devono appartenere al manifest e al workspace.

Dalla radice del workspace:

```bash
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli search 'RMA medie alternative'
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli search post_up_cooldown_days --mode keyword
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli studies
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli source docs/dev-prod-dashboard-api.md
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli status
PYTHONPATH=bt-knowledge research/rag_poc_20261109/.venv/bin/python -m bt_knowledge.mcp
```

API e CLI richiedono solo `requests`, già presente nel virtualenv API. MCP usa
`fastmcp` dal virtualenv esistente della PoC. L'entrypoint MCP precedente e
`ingestion/consult.py` restano compatibili e delegano ora al servizio condiviso.
I vecchi campioni PoC si riproducono separatamente con `RAG_NAMESPACE=main`.
`find_runs` e `open_run` consultano ora il catalogo completo riconciliato;
vedere [Guida al catalogo dei run](RUNS.md) per copertura, identità e unità.

## RAM e ciclo di vita

La prima ricerca accende solo il container OpenSearch esistente. Un piccolo
processo locale lo ferma dopo **600 secondi di inattività** e poi termina.
`BT_KNOWLEDGE_IDLE_SECONDS` modifica l'intervallo; `BT_KNOWLEDGE_RUNTIME` modifica
la cartella di stato del ciclo operativo (default `bt-knowledge/runtime`, ignorata
in Git). Volumi e indice non vengono rimossi. TEI e gli altri servizi DocsGPT
non vengono fermati.

Query e aggiornamenti gestiti condividono un lock tra processi: il timer non
interrompe lavoro attivo. Il timer ferma solo il container avviato dal servizio;
non adotta un container già acceso e riconosce un riavvio esterno. Se il motore
era stato acceso manualmente, resta acceso. L'avvio viene rimandato sotto 3 GiB
di RAM disponibile. Consultare catalogo, studi e fonti non accende il motore.

L'utente del processo API/MCP deve poter leggere le fonti e accedere a Docker.
Non sono stati aggiunti sudo, gruppi, permessi o servizi systemd. Lo stato mostrato
dall'interfaccia è quello osservato dal ciclo gestito, con relativo timestamp.

Aggiornamento incrementale autorizzato del corpus:

```bash
research/rag_poc_20261109/.venv/bin/python research/rag_poc_20261109/ingestion/consult.py update
```

La pipeline di acquisizione esistente rimane il produttore del medesimo indice.
Le fonti cambiate o fallite vengono escluse dalla ricerca e rifiutate dal lettore;
le schede dipendenti diventano non aggiornate. Il catalogo le conserva visibili
per segnalare il lavoro di riconciliazione necessario.

## Verifica e preview

```bash
bt-api/.venv/bin/python -m pytest -q bt-knowledge/tests
cd bt-api && .venv/bin/python -m pytest -q tests
cd bt-dash && node --test tests/*.test.cjs
```

Build verificata senza modificare i file serviti in produzione:

```bash
cd bt-dash
BT_DASH_DIST_DIR=/tmp/bt-knowledge-pwa-build npx quasar build -m pwa
```

Il server di preview usa la vera API documentale e il vero archivio, ma una
sessione di test isolata e nessuno scheduler. La password è solo di test e il
bind è esclusivamente loopback. Non avviarlo su indirizzi pubblici.

```bash
BT_KNOWLEDGE_IDLE_SECONDS=10 bt-api/.venv/bin/python bt-knowledge/tests/serve_ui.py --build /tmp/bt-knowledge-pwa-build
PLAYWRIGHT_MODULE=/tmp/bt-statistical-help-browser/node_modules/playwright CHROMIUM_PATH=/home/htpc/.cache/ms-playwright/chromium-1200/chrome-linux64/chrome node bt-dash/tests/knowledge.browser.cjs
```

Verifica locale MCP/servizio, a motore inizialmente spento:

```bash
PYTHONPATH=bt-knowledge research/rag_poc_20261109/.venv/bin/python bt-knowledge/tests/check_live.py
```

Risultati della consegna: 197 test API, 104 test frontend e 17 test del servizio
superati. Browser reale: autenticazione, tutte le modalità di ricerca, lettura
fonti, navigazione studi, paginazione delle righe, catalogo e mobile senza overflow.
Verificati parità MCP/servizio, stato senza risveglio, avvio a freddo e stop per
inattività. Nessuna chiamata ai client reali Codex/Claude o a modelli generativi.
Le prove di ricerca sono funzionali e non misurano validità scientifica o qualità
di una risposta generata.

## Attivazione nella dashboard servita

Attivata il 9 novembre 2026 su `https://bt-dash.ilz.it/dash/#/Knowledge`: PWA
pubblicata con lo script di produzione e `bt-api@prod.service` riavviata dopo
verifica di zero calcoli queued/running. Verificati in browser autenticato menu,
ricerca e lettura delle fonti. Gli hash di index e service worker serviti da
Apache coincidono con la build locale. Evidenze in `evidence/20261109/production.json`.
Per i rilasci successivi verificare sempre i job prima di riavviare API. Il modulo
richiede la cartella sorella `bt-knowledge` nello stesso workspace dell'API.

Aggiornamento del 10 novembre 2026: tipi documentali assegnati distinguendo le
schede automatiche dallo stato scientifico non dichiarato. Famiglie strategiche
visibili e filtrabili nelle liste; lettore Markdown completo con titoli compatti,
codice grigio, link relativi/assoluti ai documenti indicizzati e riferimenti in
backtick cliccabili. Il lettore permette di tornare alla fonte precedente.

Il catalogo dei run è disponibile e aggiornabile in background. Verificati 199
test API, 105 frontend e 26 del servizio, oltre a browser e MCP reali locali.
Nessun documento originale è stato spostato e nessuna validità scientifica è
stata dedotta dalla classificazione. Evidenze in `evidence/20261110/`.

Restano successive l'acquisizione notebook/PDF, l'espansione selettiva del codice
e la revisione scientifica dei metadati proposti.
