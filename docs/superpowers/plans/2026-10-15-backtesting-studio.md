# Ripristino Backtesting online

Obiettivo autorizzato: simulazioni robuste, parametri strategia/BackBroker,
risultati e benchmark archiviabili, pianificazioni giornaliere solo backtest,
dati ticker yahoo/yahoo_adj/alpaca aggiornabili con monitoraggio.

## Decisioni

- Riutilizzare job persistenti e worker isolati, ampliando i tipi ammessi con
  simulation e market_data_refresh. Nessuna esecuzione paper/live dalle maschere.
- Configurazioni validate e congelate; parametri derivati dai default effettivi
  delle strategie, input avanzato key=value, preset dai profili disponibili.
- BackBroker: leva, interessi margin, maintenance, slippage e flag esecuzione.
- Report e artefatti serviti solo per identità job/run, con JSON bounded e log.
- Benchmark da CSV validato o da returns.csv di un run, senza sovrascrivere file
  esistenti; archivio con etichette e provenienza. Liste/provider letti dal disco.
- Pianificazioni simulazioni in catalogo persistente separato dai cron trading;
  avvio via executor calculations e richiesta mode=backtest obbligatoria.
- Aggiornamenti dati espliciti con loader core, mai truncare gli storici per init.
- Conservare le modifiche baseline ancora non committate. Nessun push automatico.

## Fasi

- [x] 1. Core: parametri broker e API programmatica, schema tipi job, validazione
- [x] 2. Servizio/API: cataloghi, simulazioni, risultati, benchmark, ticker e schedule
- [x] 3. UI: studio, storico/dashboard, benchmark, browser ticker/provider
- [x] 4. Test isolati e smoke backtest reale con dati locali, senza ordini broker
- [x] 5. Migrazione e build PWA, verifica online e documentazione

Verifiche chiave: nessun modo live accettato, validazione tipi/range e nomi/path,
errori visibili, stato recuperabile dopo navigazione, ticker non supportati o senza
dati segnalati, risultati e config effettiva verificati, pianificazioni senza duplicati.

## Esito verificato

- Migrazione applicata, API riavviata e PWA ricompilata sul checkout online backtrader.
- Test: 80 core, 51 API e 26 frontend superati.
- Prova browser autenticata senza errori JavaScript: simulazione completata con
  90 trade chiusi, leva broker 2 verificata nella ricevuta, report e benchmark
  accessibili. Rendimenti limitati alla data finale richiesta (2026-10-14).
- Inventario verificato per yahoo, yahoo_adj e alpaca: tre ticker disponibili
  per ciascun provider. Nessun download esterno o ordine broker eseguito nei test.
- Run esplorativo precedente nascosto, benchmark di prova archiviato reversibilmente.
- Guida operativa: docs/backtesting-studio.md. Implementazione salvata nei commit
  dedicati di core, API, dashboard e repository principale.
