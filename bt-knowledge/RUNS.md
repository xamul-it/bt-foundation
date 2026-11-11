# Guida al catalogo dei run

Il catalogo del 10 novembre 2026 contiene 13.076 schede: run riconoscibili,
varianti di configurazione, esiti vuoti e riferimenti a risultati illeggibili.
Non è un conteggio di strategie validate o di esperimenti indipendenti.

## Fonti e copertura

Sono riconciliati il registro PostgreSQL dei calcoli e i risultati nelle tre radici:
`out` (risolto nell'archivio), `bt-strategy-test` e `research`. Il registro storico
`runs` è consultato ma attualmente vuoto. I job di solo aggiornamento dati non
sono run di strategia e non vengono inclusi.

L'acquisizione legge soltanto `results.json` e le relative versioni
`results-*.json`, fino a 2 MiB per file; non legge trade, prezzi, ordini o log.
I confini di profondità e numero di directory sono espliciti; se vengono superati
il nuovo catalogo non viene pubblicato. Gli errori di accesso restano nel rapporto.
I file originali non sono spostati, riscritti o corretti.

Nel censimento iniziale sono stati trovati 23.389 file candidati. Le 255 sorgenti
vuote sono mantenute con stato `empty_result`, senza inventare ID del produttore,
parametri o metriche. Due vecchi file troncati hanno stato `unreadable_result`;
un file con coda corrotta conserva solo i blocchi JSON completi e l'avviso del
parser. Un risultato statistico sintetico non di trading è escluso.

## Identità e significato dei campi

La chiave distingue strategia, ID registrato, parametri effettivi estratti,
periodo e principali input registrati nell'artefatto. Copie equivalenti condividono
la scheda e conservano tutti i percorsi/hash; ID riutilizzati con configurazioni
diverse restano separati. Il nome del run da solo non ne garantisce l'identità.

- `resolved_parameters`: campi `param_*` del risultato o parametri del registro.
- `artifact_inputs`: input registrati nel risultato, compresi periodo, capitale,
  provider, timeframe e principali opzioni di leva/costi quando presenti.
- `requested_configurations`: richieste conservate separatamente, senza trattarle
  come prova dei valori effettivamente risolti.
- `metrics`, `metric_units`, `metrics_source`: valori e provenienza. La cache API
  disponibile è riusata; negli altri casi i valori del produttore rimangono nelle
  unità native non verificate. Non vengono ricostruite metriche da returns o trade.
- `sources`: registro, artefatti, versioni, hash, indice del record e metodo parser.
- `study_documents`: documenti che citano letteralmente l'ID; una citazione non
  dimostra causalità né che il documento abbia studiato quella configurazione.

`completed` descrive il job registrato. `output_present` descrive un risultato
presente senza dedurre che il processo sia terminato correttamente. Il catalogo
non promuove strategie e non rende confrontabili automaticamente periodi, dati,
costi o metriche di origini diverse.

## Consultazione

Nella pagina **Documentazione e studi → Run** sono disponibili famiglia strategica,
stato, presenza di un parametro effettivo e ricerca testuale/ibrida/semantica.
Il dettaglio separa parametri, input registrati, richieste, metriche/unità e fonti.
I job riconciliati hanno il collegamento a **Backtest e confronti**; i documenti
che citano l'ID si aprono nel lettore.

MCP espone `find_runs` e `open_run`. Filtri esatti: `run_id`, `strategy`,
`strategy_family`, `status`, `mode`, `start`, `end`, `parameter`. I prefissi
`param.NOME`, `input.NOME`, `metric.NOME` accettano `eq/gt/gte/lt/lte`.
Le date del periodo sono distinte dai timestamp di acquisizione.

```bash
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli runs post_up_cooldown_days --mode keyword
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli runs --filters '{"strategy_family":"RMA","param.min_concurrent":{"gte":3}}'
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli run ID_DELLA_SCHEDA
```

## Aggiornamento e RAM

Il pulsante **Aggiorna catalogo run** avvia la riconciliazione in background;
non viene eseguita una scansione dell'archivio durante la lettura delle liste.
Il processo ha un lock contro aggiornamenti concorrenti e stato consultabile.
Le successive acquisizioni riusano i metadati dei file con mtime/dimensione invariati.

```bash
PYTHONPATH=bt-knowledge bt-api/.venv/bin/python -m bt_knowledge.cli update-runs
```

La riconciliazione non è un watcher continuo: per includere nuovi risultati si
usa l'aggiornamento esplicito. Quando le schede semantiche non cambiano non viene
acceso OpenSearch. Negli altri casi vengono indicizzate solo le differenze usando
il modello e la cache già condivisi, con spegnimento dopo inattività.

Il catalogo locale persistente è `bt-knowledge/runtime/runs.sqlite`; l'indice
semantico strutturato è `bt-knowledge-runs-v1`. Nessun nuovo container o modello
residente. Lo stato/checkpoint e i file runtime sono esclusi da Git. Se l'indice
viene eliminato manualmente, rimuovere anche `run-index-state.json` e rieseguire
l'aggiornamento per ricostruirlo.
