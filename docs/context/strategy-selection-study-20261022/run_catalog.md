# Catalogo dei run e degli artefatti

Inventario read-only del 22 ottobre 2026. Il symlink `out` punta all’archivio `/mnt/Backup/backtrader_archive/out`. Nel catalogo JSON sono indicizzati **4.012 `returns.csv` nativi** sotto le tre famiglie candidate; il CSV è un indice leggero, con start/end/rows/size/hash e link al file metadata quando disponibile. I file locali di studio restano registrati anche nel JSON.

Gli artefatti rappresentativi scelti per completezza e copertura temporale (senza metriche di performance) sono descritti in `representative_runs.json`. Per ognuno sono riportati path esatti di returns, results metadata, runtime log, ordini/trade/equity disponibili, parametri economici `param_*`, input del motore, universo, commit, stato dirty, hash configurazione e hash degli artefatti.

| Candidate | Finestra estesa disponibile | Finestra recente comune | Ultima data output trovata |
|---|---:|---:|---:|
| OvernightAH | 2000-01-03–2026-10-13 | 2023-10-23–2026-10-19 | 2026-10-20 |
| OvernightAHFlatComposite | 2000-01-03–2026-10-21 | 2023-10-23–2026-10-19 | 2026-10-21 |
| weekly.RMAStrategy | 2000-10-20–2026-10-20 | 2023-10-23–2026-10-19 | 2026-10-20 |

La finestra comune sopra è intersezione per etichette delle tre serie native scelte, non una dichiarazione di holdout indipendente né di feed certificato. L’ultimo giorno comune disponibile è **2026-10-19**; va verificata la calendar intersection esatta prima di un join. AH ha una serie recente più corta (2025-08-27–2026-10-20) e una lunga fino al 13 ottobre; flat-composite e RMA coprono altre finestre con parametri e provenance diversi. Non esiste un singolo run appaiato per tutto il lungo e il recente.

## Run e comparabilità

I file `results-*.json` dei run più recenti indicano simulazioni `mode=backtest`, `live=False`, capitale iniziale 200.000, timeframe daily/compression 1, commissione nominale `alpaca` e slippage wrapper 0. Sono disponibili parametri risolti, universo ticker, `input_fromdate`/`input_todate`, feed/dati, benchmark, broker flags e configurazione del sizing. Le differenze restano sostanziali: OvernightAH recente usa auction, entry lag e universo mensile dinamico; FlatComposite usa `flat_panel_live.csv` (sha256 incluso nel provenance) e la variante monthly flat ranking; RMA usa regressioni giornaliere RMA180/30. Il codice e il core sono spesso indicati `dirty=true`, quindi commit ID e config hash non bastano a ricostruire lo snapshot sorgente.

Il warm-up non è esplicitamente registrato nei risultati/runtime dei rappresentanti e richiede ricostruzione dal codice, dati e date negoziabili. La tariffa Alpaca nominale non prova le commissioni effettive, lo spread o l’impatto; le semantiche di fill si devono verificare nel runtime/ledger specifico. Le classi sono identificate dai campi `input_strat` dei results JSON; `weekly.RMAStrategy` rimane distinta da varianti overnight. Le serie sono backtest su dati storici già osservati e pannelli/parametri selezionati, quindi non costituiscono valutazioni indipendenti.

I run di studi precedenti sono inclusi via percorsi locali (autonomous rotation e ciclo RMA) e archivio flat-composite: alcuni hanno manifest completi, altri solo returns. La famiglia OvernightAH contiene anche `P3_P4_dwell4_invalid`, mantenuta per tracciabilità ma da escludere per il noto off-by-one della finestra di dwell. Le serie `nested_exploratory_composite.csv` sono output derivati e non returns nativi.

## Watchtower e limiti

Non è stato letto alcun DB o chiamata API Watchtower perché il collegamento avrebbe richiesto accesso a DSN/credenziali. I risultati `simulation_*` dell’archivio sono backtest artifacts con provenance; senza job record non li etichetto come esecuzioni Watchtower. L’eventuale attribuzione Watchtower resta da fare tramite una sorgente read-only già connessa o esportazione non sensibile dei job record.

La classificazione nel catalogo misura disponibilità dei metadati, non comparabilità economica. `results_provenance` garantisce che il results JSON ha provenienza/hash, non che il codice sia clean o che input, fill e costi siano comparabili. `returns_only` e `results_config_no_provenance` sono indici descrittivi da verificare sui rispettivi report/manifest.
