# OvernightAH: qualità della selezione rispetto ai risultati ex post

## Domanda e conclusione provvisoria

Nel ramo in cui differiscono, la lista oggettiva di OvernightAH Research riesce a scegliere i titoli che si comporteranno meglio, confrontata con la lista fissa della base e con i migliori titoli conosciuti soltanto dopo?

**La prova non dimostra che Research selezioni meglio della base.** La win ratio delle 21 sedute precedenti mostra una relazione positiva ma debole con quella del mese successivo; tuttavia, confrontando i primi tre nomi delle liste su 51 mesi valutabili, i nomi della base ottengono in media risultati overnight successivi migliori e incontrano più spesso i migliori tre ex post. Nessuna delle due liste è vicina all'insieme scelto conoscendo il futuro. L'esito è descrittivo e limitato al ramo confrontabile: non è sufficiente per promuovere la base né per scartare Research in generale.

Questa verifica **non premia la diversificazione in sé**. Un titolo scelto ripetutamente è appropriato se il metodo lo identifica bene; la questione è se la scelta sia conoscibile prima e ripetibile fuori dal campione. La lista base è stata scelta empiricamente osservando anche dati storici e non può essere considerata una previsione blind per questi anni.

## Ambito e costruzione del riferimento

Si usano le liste e i risultati dei due job già esaminati: [Research win ratio 21](https://bt-dash.ilz.it/api/dyn/simulations/jobs/c9a7e76f-b84c-47a3-8a3d-2431f2c468df/artifacts/stats.html?download=1) e [OvernightAH base](https://bt-dash.ilz.it/api/dyn/simulations/jobs/9b14126f-cbe7-4c30-9dd6-a9ae5c3debfd/artifacts/stats.html?download=1). I log permettono di ricostruire integralmente la lista Research nei mesi in cui sostituisce la lista statica della base. **Il ramo dinamico condiviso non è oggetto di questa misura**: i log non forniscono l'intero ranking dinamico in modo sufficiente per questo confronto.

Per ogni mese del ramo statico/riserve:

1. Si prende la lista statica ordinata della base e la lista ordinata Research registrata prima delle operazioni del mese. Research confronta la win ratio overnight precedente con SPY, esclude i pari e ordina i titoli ammessi; la base segue l'ordine della sua lista fissa.
2. Si considera come universo comune quello dei titoli per i quali il selettore Research disponeva di storia sufficiente e per i quali, successivamente, si dispone di tutte le barre valide della finestra. L'intersezione si applica anche alla lista base. Questo rende confrontabili i nomi nel calcolo, ma **esclude eventuali titoli che la base avrebbe potuto usare pur non soddisfacendo il requisito storico Research**.
3. Si calcola per ogni titolo il rendimento composto delle notti **dal close della prima seduta del mese fino all'open della prima seduta del mese successivo**, sommando per capitalizzazione i singoli rendimenti `open_successivo / close − 1`. In parallelo si calcola la percentuale di notti positive.
4. Il riferimento ex post prende i 3 o 5 titoli dell'universo comune con il migliore rendimento overnight composto nella finestra appena osservata. È una scelta **conoscibile solo dopo**, non una strategia e non un massimo di portafoglio investibile.
5. Si confrontano i primi 3 o 5 nomi delle liste, attribuendo a ciascun titolo peso uguale nel calcolo della media dei rendimenti mensili per titolo. Non è un backtest di portafoglio: non considera ribilanciamento giornaliero, filtri di ingresso, cash, sizing, posizioni, leva, commissioni, slippage o giorni in cui la strategia non opera. I nomi nella lista non sono necessariamente quelli effettivamente acquistati.

Dei **57 mesi** in cui il selettore Research è stato registrato, **55** hanno storico SPY sufficiente per questa diagnostica; gennaio e febbraio 2005 sono esclusi per dati insufficienti. Nel confronto a 3 titoli sono utilizzabili **51 mesi**: Research ha meno di tre candidati nei restanti quattro. A 5 titoli i mesi appaiati sono **47**. Le medie riportate sono quindi **condizionate alla disponibilità di almeno 3 o 5 nomi per entrambe**; i casi mancanti non sono trattati come cash e il risultato non misura l'opportunità perduta quando una lista è vuota.

## Risultati ex post

### Primi tre titoli — 51 mesi appaiati

| Misura | Base | Research | Riferimento ex post |
|---|---:|---:|---:|
| Media dei rendimenti overnight mensili composti dei tre titoli | **+2,35%** | +1,44% | +15,99% |
| Percentile medio del ranking successivo, 100 = migliore | **58,2** | 54,5 | — |
| Numero medio di titoli coincidenti con i migliori tre ex post | **0,255 su 3** | 0,137 su 3 | 3 su 3 |
| Media della quota di notti positive dei titoli selezionati | **54,68%** | 52,96% | — |

La base ha un rendimento medio superiore in **29 mesi**, Research in **22**. Il riferimento ex post è molto alto perché sceglie ogni mese i vincitori sapendo già quali saranno; non rappresenta un rendimento ottenibile da una regola causale e non è direttamente confrontabile con il CAGR del portafoglio.

### Primi cinque titoli — 47 mesi appaiati

| Misura | Base | Research | Riferimento ex post |
|---|---:|---:|---:|
| Media dei rendimenti overnight mensili composti dei cinque titoli | **+1,55%** | +1,18% | +13,02% |
| Percentile medio del ranking successivo | **54,2** | 53,8 | — |
| Numero medio coincidente con i migliori cinque ex post | **0,745 su 5** | 0,340 su 5 | 5 su 5 |

Research prevale in **25 mesi su 47**, la base in **22**; la media della base resta superiore. Il confronto a 3 e quello a 5 usano campioni diversi, perché in alcuni mesi Research ha meno di cinque candidati: le due righe non sono una prova controllata dell'effetto del numero di titoli.

## Il ranking win ratio contiene informazione sul mese successivo?

Tra tutti i titoli dell'universo comune, mese per mese, si confronta la loro **win ratio delle 21 sedute precedenti**, registrata dal selettore Research, con il risultato successivo. La correlazione usata è Spearman tra ranghi, calcolata separatamente in ciascun mese; valori vicini a zero indicano un ordinamento poco associato.

| Confronto dei ranghi | Media su 55 mesi | Mediana | Mesi con correlazione positiva |
|---|---:|---:|---:|
| Win ratio precedente vs win ratio successiva | +0,139 | +0,147 | 44 su 55 |
| Win ratio precedente vs gain overnight composto successivo | +0,097 | +0,097 | 37 su 55 |

C'è un **segnale descrittivo positivo ma debole**. Non prova un vantaggio economico dopo costi né che il top della classifica sia stabile. Il periodo è già stato osservato e i mesi non sono un campione blind indipendente; non sono state stimate qui significatività o capacità predittiva prospettica.

Un esempio chiarisce perché contare notti positive e massimizzare il rendimento possano dare esiti diversi. Nell'**ottobre 2008**, sui primi tre nomi analizzati, Research ha una quota media di notti positive del **52,2%** contro **47,8%** della base, ma rendimento overnight medio per titolo **−0,63%** contro **+15,01%**. Il riferimento ex post dei migliori tre era **+21,74%**. La win ratio non misura la grandezza delle notti positive e negative; questi valori non sono P&L effettivo del portafoglio.

## Cosa significa per la scelta

- Nei limiti di questa prova, **non abbiamo evidenza che il metodo Research a 21 sedute sia migliore della lista base nel selezionare i migliori risultati overnight futuri**. La preferenza precedente per Research non è dimostrata da questa nuova analisi e va ridimensionata.
- La base è stata costruita empiricamente con osservazioni precedenti allo studio: parte del vantaggio storico potrebbe dipendere dalla scelta ex post dei nomi. Non si può quindi concludere che la lista fissa continuerà a essere migliore.
- Il risultato non esclude che Research funzioni con un'altra metrica o orizzonte; neppure autorizza a scegliere retrospettivamente la variante che vince su questi stessi dati. Va valutata una regola congelata su un periodo successivo e, se disponibile, su un universo storico point-in-time.
- Il confronto originale dei run mostra rendimenti e rischi effettivamente simulati; questa analisi isola invece un aspetto del **meccanismo di scelta**. Le due evidenze si completano, ma i numeri non sono intercambiabili.

## Qualità dei dati e riproducibilità

Le liste vengono dai log dei run. I prezzi usati per ricostruire i risultati futuri sono i **parquet Yahoo adjusted attualmente presenti**, con hash registrati in [analisi.json](../../research/overnight_ah_qualita_selezione_20261105/analisi.json); non sono uno snapshot certificato dei file utilizzati dai job. Su un campione casuale riproducibile di **120 prezzi di ingresso per ciascun run**, tutti coincidono con il close attuale entro una differenza relativa di `1e-5`: è un controllo parziale, non certifica l'intero storico né i vintage dei dati. Dettagli in [verifica_prezzi.json](../../research/overnight_ah_qualita_selezione_20261105/verifica_prezzi.json).

L'universo è limitato al paniere fornito, selezionato con conoscenze odierne: resta il survivorship bias. La richiesta di copertura completa anche nel mese futuro usa informazione successiva e può introdurre un ulteriore bias: in questa verifica ha escluso in media **0,02 titoli per mese**, massimo uno. Il rendimento close-to-open overnight è ricostruito con dati giornalieri adjusted e non distingue after-hours da pre-market.

Calcoli e liste mensili sono verificabili in [analyze.py](../../research/overnight_ah_qualita_selezione_20261105/analyze.py) e [analisi.json](../../research/overnight_ah_qualita_selezione_20261105/analisi.json); verifica campionaria dei prezzi in [check_prices.py](../../research/overnight_ah_qualita_selezione_20261105/check_prices.py). Il confronto dei portafogli simulati è nel [report precedente](confronto-overnightah-research-21-vs-base-2005-2020.md).
