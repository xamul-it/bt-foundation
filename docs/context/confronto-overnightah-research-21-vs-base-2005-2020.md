# OvernightAH: confronto tra i due run del 2005–2020

## Cosa emerge

La variante **Research con win ratio a 21 sedute** ha distribuito l'attività su molti più titoli e ha avuto un drawdown massimo inferiore. Nel periodo intero, però, il rendimento composto è inferiore a quello della base; il recupero dai ribassi è stato più lungo. Non emerge un vincitore su tutti i criteri: se conta la varietà storica e il contenimento del drawdown, Research è interessante; se conta il rendimento composto di questi due run, vince la base. È un confronto retrospettivo, non una prova che il risultato si ripeterà.

| Indicatore | Base | Research: win ratio 21 | Come leggerlo |
|---|---:|---:|---|
| Rendimento totale del conto | +38.686,5% | +29.343,2% | Compound, 2005–2020 |
| CAGR convenzionale | 45,20% | 42,71% | Annualizzazione su 252 sedute |
| Drawdown massimo | −26,88% | −17,81% | Massima discesa dal picco nel periodo |
| Durata massima sott'acqua | 217 sedute | 328 sedute | Giorni fino al recupero del massimo precedente |
| Sharpe | 1,93 | 2,07 | Giornaliero annualizzato, tasso privo di rischio zero |
| Sortino | 2,90 | 3,23 | Giornaliero annualizzato, soglia zero |
| Volatilità annua | 20,45% | 17,99% | Deviazione standard giornaliera × √252 |
| VaR storico giornaliero 95% | 1,66% | 1,43% | Perdita alla soglia del 5° percentile; non perdita massima |
| Trade chiusi, inclusi hedge | 15.971 | 15.063 | Non sono titoli distinti |

Il conto iniziale è **200.000 USD** e il sizing è composto: il conto finale simulato è circa **77,57 milioni** nella base e **58,89 milioni** nella Research. Le cifre molto alte sono il risultato del compounding e non stimano la capacità reale a tali dimensioni. Commissioni Alpaca simulate, slippage impostato a zero, margine ed esposizione massima 1×. Il confronto dipende da questi presupposti e dalla qualità dei dati.

## Parametri e fonti

- [Run Research](https://bt-dash.ilz.it/api/dyn/simulations/jobs/c9a7e76f-b84c-47a3-8a3d-2431f2c468df/artifacts/stats.html?download=1) · [Run base](https://bt-dash.ilz.it/api/dyn/simulations/jobs/9b14126f-cbe7-4c30-9dd6-a9ae5c3debfd/artifacts/stats.html?download=1).
- Stesso periodo **1 gennaio 2005–31 dicembre 2020**, ticker richiesti, provider Yahoo adjusted, cassa, leva, costi e parametri operativi comuni verificati nei `results.json`. L'unica differenza fra i parametri condivisi individuata dal confronto completo è `live_reenter_positions_pending_fallback`: `true` nella base e `false` nella Research; è una chiave legacy dichiarata `unused` nel codice, perciò non è un'impostazione economica aggiuntiva del backtest. Nella Research si aggiungono `spy_selection_metric=win_ratio` e `spy_selection_lookback=21`.
- I due job sono stati eseguiti in momenti diversi e indicano codice con modifiche locali: i metadata non provano che tutte le dipendenze e tutti i file di mercato fossero identici byte per byte. I parametri verificati sono coerenti; non attribuire automaticamente ogni scarto solo al selettore senza questo limite.
- La serie dei rendimenti ha 4.028 sedute comuni, senza date duplicate o rendimenti mancanti. Contabilità riconciliata in entrambi i run con equity e commissioni arrotondate dei log; il campo dei trade `pnlcomm` non è trattato come profitto netto. I calcoli riproducibili e i dati sintetici sono in [analyze.py](../../research/overnight_ah_confronto_20261105/analyze.py) e [analisi.json](../../research/overnight_ah_confronto_20261105/analisi.json).

## Come vengono scelti i titoli

In entrambe le strategie lo switch di regime resta legato a SEMIS. Nei log della base il ramo dinamico è chiamato **2.754 volte** e quello statico **1.097 volte**. Nella Research il selettore SPY viene chiamato 1.097 volte, con gli stessi conteggi mensili delle chiamate al ramo statico della base: è un controllo coerente con la sostituzione del solo ramo di riserva. Queste sono chiamate registrate, non necessariamente tutte le sedute del calendario.

- **Base:** se lo switch sceglie il ramo statico, usa la lista fissa configurata: `NVDA, AVGO, MU, AMD, MSTR, CEG, ASML, MRVL, ARM, MELI`, limitatamente ai feed disponibili e agli altri filtri.
- **Research:** nello stesso ramo usa la **win ratio overnight**, cioè la quota di rendimenti close-to-open strettamente positivi nelle **21 sedute precedenti**. Confronta ciascun titolo con SPY sulle stesse sedute, esclude quelli non strettamente migliori, ordina i rimanenti per win ratio decrescente e ne conserva al massimo 50. I titoli con dati incompleti o non validi non entrano. Parità di punteggio tra titoli è risolta alfabeticamente.
- Nel ramo dinamico entrambe mantengono il ranking originale e il contributo SEMIS; il filtro Research non sostituisce l'intera logica della strategia. Le liste mensili sono candidate, non acquisti garantiti: segnali, disponibilità, sizing e filtri decidono gli ordini effettivi.

Nei **55 mesi distinti** in cui il ramo Research viene registrato con storico SPY sufficiente, il numero di titoli valutabili varia da **68 a 94**, mediana **79**; quelli selezionati dopo il taglio variano da **0 a 50**, mediana **21**. In **6 mesi** più di 50 titoli superano SPY e interviene il limite; in **4 mesi** ne rimangono meno di 3, di cui **1 mese** con lista vuota. I conteggi riguardano solo i mesi in cui il ramo è stato effettivamente chiamato, non tutti i 192 mesi del periodo.

Con sole 21 sedute, la win ratio assume pochi valori possibili: nei 55 mesi validi, la mediana dei titoli con win ratio **esattamente uguale a SPY è 15**, con massimo 34. I pari a SPY vengono esclusi. Quando interviene il limite di 50, anche il criterio alfabetico può determinare quali titoli con score uguale rimangono fuori: è una caratteristica da considerare in uno studio di stabilità.

## Varietà e concentrazione degli acquisti

I run richiedono 103 simboli, inclusi SPY, QQQ e SQQQ; i log indicano **95 titoli tradabili caricati** per entrambe, non 100. I cinque simboli richiesti ma non caricati sono `ABNB, ARM, CEG, GEHC, GFS`. I numeri seguenti escludono SPY, QQQ e SQQQ e sono ricavati dai trade chiusi.

| Misura | Base | Research |
|---|---:|---:|
| Titoli distinti acquistati nel periodo | 61 | 90 |
| Trade chiusi sui titoli | 15.840 | 14.932 |
| Sedute con almeno un ingresso di titolo poi chiuso | 3.759 | 3.701 |
| Titoli medi per seduta con ingressi | 4,21 | 4,03 |
| Mediana titoli per seduta con ingressi | 4 | 4 |
| Primi 3 titoli per controvalore d'ingresso normalizzato | 49,8% | 27,4% |
| Primi 5 titoli, stessa misura | 60,3% | 35,7% |
| Gruppo dei 16 semiconduttori, stessa misura | 61,2% | 38,1% |

Research **allarga molto la varietà nel tempo**, ma non aumenta il numero medio di titoli con ingresso nello stesso giorno. La maggiore varietà storica non equivale automaticamente a una maggiore diversificazione simultanea o a un minore rischio comune: la correlazione dei rendimenti giornalieri dei due portafogli nel periodo è circa **0,87**.

La misura di concentrazione divide il controvalore di ogni ingresso per l'equity precedente all'ingresso, somma questi rapporti per titolo e normalizza sul totale. Riduce la distorsione dovuta ai grandi importi degli ultimi anni nel run composto, ma resta una **proxy dei flussi di ingresso**, non l'esposizione giornaliera effettiva né una misura di rischio. L'analisi delle posizioni giornaliere complete non è ricostruibile dal `positions.csv` di questi run, che non contiene tutte le posizioni azionarie.

Primi titoli per quota normalizzata degli ingressi:

| Posizione | Base | Quota | Research | Quota |
|---|---|---:|---|---:|
| 1 | MU | 19,1% | MU | 10,4% |
| 2 | NVDA | 16,6% | AMD | 9,6% |
| 3 | AMD | 14,0% | NVDA | 7,3% |
| 4 | AVGO | 6,4% | TSLA | 4,5% |
| 5 | TSLA | 4,2% | MELI | 3,9% |

Il numero equivalente di titoli, calcolato come `1 / somma(quota²)` sulle stesse quote aggregate, è **10,4** per la base e **25,2** per Research. Non significa che quelle posizioni fossero detenute contemporaneamente.

## Robustezza: cosa dicono e cosa non dicono questi run

- **Differenza temporale:** Research supera la base nel rendimento totale di **5 anni su 16**; la base prevale nei restanti 11. Gli anni con vantaggio Research sono 2009, 2010, 2013, 2015 e 2018. Nel 2008 Research fa **−7,88%**, la base **+12,96%**. Un drawdown massimo complessivo inferiore non implica quindi che Research protegga meglio in ogni crisi.
- **Incertezza appaiata:** la differenza Research meno base dei rendimenti logaritmici annualizzati è **−1,72 punti percentuali**; bootstrap appaiato a blocchi di 21 sedute, 2.000 repliche, intervallo percentile 95% circa **[−5,76; +2,20] punti**. È una descrizione statistica condizionata ai dati e alle ipotesi del bootstrap, non una validazione indipendente; non esclude vantaggi o svantaggi moderati.
- **Stabilità della concentrazione:** il vantaggio di varietà non è uniforme. Nel 2017 la quota dei primi tre titoli sugli ingressi normalizzati è circa **80% in entrambe**; nel 2018 è circa **62% base contro 43% Research**. Quindi la Research non elimina sempre la concentrazione.
- **Rischio di storico insufficiente:** entrambi i run caricano dati solo dal 2005, senza warm-up precedente. Il selettore Research segnala SPY insufficiente a gennaio e febbraio 2005 e il suo primo trade chiuso risulta aperto il **1° aprile 2005**; la base inizia il **5 gennaio 2005**. Il confronto include quindi un avvio diverso. Occorre ripeterlo caricando storico precedente e bloccando gli ingressi fino alla stessa data per entrambe.
- **Stabilità del criterio:** 21 notti sono una finestra breve e la win ratio non misura l'ampiezza dei guadagni o delle perdite. Le molte parità con SPY suggeriscono di verificare sensibilità a lookback e regole di spareggio; i due soli run non dimostrano che un intervallo vicino si comporti in modo simile.
- **Costi:** uno stress approssimato, che sottrae 5 bps round-trip dal controvalore storico senza ricalcolare il sizing, riduce il CAGR convenzionale a circa **28,9% base** e **27,1% Research**. Non è una simulazione nativa di slippage e non sostituisce dati Alpaca sugli eseguiti.
- **Universo e validazione:** la lista dei titoli è quella fornita oggi anche per il passato; ciò espone entrambi al survivorship bias. Il periodo è storico e già osservato, non è una nuova prova blind. Lo stesso fatto che la Research venga esaminata dopo tentativi precedenti richiede cautela rispetto alla selezione delle varianti.

## Rendimenti e varietà anno per anno

Rendimenti composti nell'anno; titoli distinti da trade chiusi con ingresso nell'anno. Sono segmenti delle simulazioni continue, non run ripartiti con lo stesso capitale a ogni anno.

| Anno | Base | Research | Titoli base | Titoli Research |
|---:|---:|---:|---:|---:|
| 2005 | +43,47% | +42,09% | 8 | 16 |
| 2006 | +35,26% | +29,95% | 12 | 20 |
| 2007 | +74,18% | +64,83% | 11 | 19 |
| 2008 | +12,96% | -7,88% | 8 | 25 |
| 2009 | +28,99% | +34,07% | 12 | 16 |
| 2010 | +28,63% | +31,14% | 13 | 17 |
| 2011 | +21,08% | +17,50% | 13 | 22 |
| 2012 | +13,87% | +5,67% | 19 | 32 |
| 2013 | +105,35% | +115,72% | 13 | 14 |
| 2014 | +74,96% | +72,90% | 14 | 13 |
| 2015 | +12,62% | +29,12% | 19 | 29 |
| 2016 | +79,63% | +78,80% | 18 | 24 |
| 2017 | +66,58% | +65,39% | 14 | 13 |
| 2018 | +17,27% | +20,85% | 22 | 41 |
| 2019 | +46,80% | +32,11% | 31 | 41 |
| 2020 | +113,06% | +111,59% | 16 | 18 |

Il prossimo confronto utile è ripetere entrambe con warm-up precedente al 2005 e stessa data di inizio operatività, poi verificare la sensibilità del lookback senza scegliere il risultato migliore sullo stesso campione. La decisione finale richiede anche un test successivo e dati point-in-time dei componenti, se disponibili.
