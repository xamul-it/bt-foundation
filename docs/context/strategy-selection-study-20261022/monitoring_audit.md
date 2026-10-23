# Watchtower: capacità verificate e limiti per questo studio

Audit del codice corrente del 22 ottobre 2026, in sola lettura. Nessuna chiamata a broker, DB, API operative o funzioni di aggiornamento. Gli artefatti `simulation_*` nell'archivio locale sono backtest con metadata: senza record job non diventano automaticamente run Watchtower o una misura della qualità dell'esecuzione reale.

## Tre livelli che richiedono riferimenti differenti

| Livello | Funzioni presenti nel codice | Ciò che lo studio può verificare |
|---|---|---|
| Integrità dati/configurazioni | `scan_feed_monitor_coverage`, `feed_monitor_summaries`, `feed_monitor_discrepancies`, timeline parametri, ricevute schedulate; `bt-core/watchtower_runtime.py` e scheduler API | Sorgente e contratto delle funzioni; manifest e qualità dei file locali congelati. La funzione di scan può scrivere record DB: non è stata chiamata. Copertura corrente del servizio non certificata dal solo codice. |
| Esecuzione osservata contro replay | `reconciliation_statistics`, `get_reconciliation_result`, `list_reconciliation_results`, settlements e factsheet matched/missed/paper-only; watchdog ordini | La contabilità dei nuovi backtest può essere ricostruita dalla tape integrale. Latenza, slippage reale, mancati fill e commissioni reali richiedono settlements/ledger osservati con scope e timeline coerenti; qui non disponibili come estratto autenticato verificato. |
| Comportamento economico del replay | Statistiche rolling, distribuzioni PnL, KS/Wasserstein e `monte_carlo_subset_test`; footprint drift | Le nuove serie native consentono diagnostica storica degli allarmi. Le funzioni esistenti non certificano automaticamente falsi allarmi sequenziali, probabilità della fine dell'edge o convenienza di una sospensione/switch. |

## Semantiche da conservare

`reconciliation_statistics` (sorgente corrente, circa riga 9873) usa i settlements chiusi come autorità, con fallback su riconciliazioni giornaliere legacy. Costruisce finestre dal calendario, esclude l'ultima seduta ancora overnight e lascia visibili le giornate senza fatti. Le finestre default sono 3/5/10/15/20/25 sedute. Il rendimento giornaliero ricostruito dai settlements è `PnL / nozionale d'ingresso × 100`: **non è automaticamente rendimento del NAV**, perché cash, esposizione, capitale e posizioni aperte hanno un denominatore differente. Non confrontarlo direttamente al daily TimeReturn del backtest.

Il factsheet (circa righe 7700–7830) separa trade observed/reference/matched/missed/paper-only e mostra copertura, gap di expectancy e distanze distributive. La numerosità conta trade con PnL disponibile; trade della stessa notte condividono fattori e non sono osservazioni indipendenti. La copertura `observed_count/reference_count` non misura da sola la perdita economica delle occasioni mancate.

`reconciliation_watchdog.py` è orientato al sottoinsieme di ordini paper rispetto al flusso sim e dichiara reporting senza stop automatico. Il solo insieme dei signal ID non certifica prezzo, quantità o sincronizzazione dei fill. Le funzioni di sincronizzazione, enqueue, calcolo e upsert non sono letture innocue e non sono state eseguite.

## Regole dello studio

Le soglie economiche vengono stimate sui rendimenti netti del NAV, solo dal passato, secondo il [protocollo](../../../bt-strategy-test/strategy-selection/study_20261022/protocol.md). Le tre finestre 20/63/126 non sono tre occasioni per scegliere il segnale più favorevole: 63 è primaria. Un episodio seguito da recupero shadow positivo è una definizione operativa di falso arresto, non una verità sul regime latente. Ritardi e mancate rilevazioni devono restare nel denominatore.

Prima di un'eventuale prova prospettica servono ricevute giornaliere di hash dati/config/panel, calendario e copertura, ordini/fill/commissioni e NAV, con un replay coerente mantenuto anche durante la sospensione. Questa è una specifica osservativa proposta; questo studio non installa soglie, scheduler o arresti.

La documentazione Alpaca distingue gli ordini OPG/CLS, le aste e i vincoli di buying power: il close/open di una barra giornaliera resta un proxy del prezzo effettivamente eseguibile in asta. [Fonte ufficiale letta](https://docs.alpaca.markets/us/docs/orders-at-alpaca). Per il monitoraggio, ARL e ritardo dipendono dal processo di riferimento; la dipendenza temporale richiede valutazione dedicata, non una soglia nominale trattata come probabilità universale. [NIST ARL](https://www.itl.nist.gov/div898/handbook/pmc/section3/pmc3131.htm), [NIST sui dati autocorrelati](https://www.nist.gov/publications/comparisons-control-charts-autocorrelated-data).
