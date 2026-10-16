# Run indipendenti e benchmark gestiti

## Contratto

- Card per ID run, nome/etichetta in evidenza, pin persistente e cestino reversibile.
- Benchmark implicito: stesso universo congelato, provider/feed e periodo del run;
  Buy&Hold equiponderato all'inizio, quote costanti, senza leva/ribilanciamento/costi.
- CSV e run salvati sono statici. Le sole liste marcate per benchmark sono
  aggiornate giornalmente con download incrementale e calcolo persistente monitorato.
- Obsolescenza in giorni di calendario dall'ultima data coperta, mai dal mtime.
- Artefatti dei run e benchmark pubblicati atomicamente, nessuna cancellazione fisica.
- Nessuna modifica ai profili trading o al checkout backtrader-prod.

## Implementazione e verifica

1. Motore Buy&Hold condiviso e test che distinguono quote fisse da ribilanciamento.
2. Snapshot benchmark isolato per run, archivio tipizzato e metadati copertura.
3. Definizioni liste/provider selettive, timer ripristinabili, job download/calcolo.
4. Dashboard per run e UI benchmark con obsolescenza, stato job e log.
5. Test core/API/UI e browser, riavvio API senza interrompere job, build online.

## Esito

- Implementate card per run, pin persistenti (inclusi run oltre la finestra
  recente), cestino e ripristino, verificati dal browser anche su mobile.
- Benchmark implicito provato con run reale
  `3dbb54da-a4dd-475f-b916-36938bfbe37a`: completato, report disponibile,
  copia benchmark verificata per 2026-09-01..2026-10-15, provider yahoo_adj,
  tre ticker, leva benchmark 1 e assenza ribilanciamento. Run di prova nel cestino.
- Scheduler selettivo ripristinabile e worker download→calcolo verificati con
  test isolati; nessuna lista operativa attivata arbitrariamente, nessun ordine
  broker inviato né download reale necessario per la prova.
- Regressioni: 86 test core, 56 API, 30 frontend superati.
- Metadati obsolescenza basati sui dati; vecchi CSV privi di date valide segnalati,
  non modificati o cancellati. Guida aggiornata in docs/backtesting-studio.md.
