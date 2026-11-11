# Multifactor Research vs Multifactor SemisSwitch — 2005–2020

## Risultato principale

Nei run forniti, **la Research distribuisce gli ingressi in modo molto meno concentrato**, soprattutto sui tre titoli principali e sui semiconduttori. La base SemisSwitch, con il proprio assetto, ottiene un **rendimento composto superiore**. La Research ha volatilità, VaR storico e drawdown massimo leggermente inferiori, ma resta più a lungo sotto il precedente massimo.

La base e la Research hanno logiche di selezione e impostazioni del filtro SPY differenti. Per scegliere fra le due strategie complete il confronto è pertinente; **non permette però di attribuire la differenza al solo taglio dei titoli sotto SPY**.

## Risultati principali

Periodo **3 gennaio 2005–31 dicembre 2020**, capitale iniziale 200.000 USD, sizing composto, leva ed esposizione massima 1, commissioni Alpaca simulate, slippage zero. Le percentuali di gain sono sul conto composto per l'intero intervallo.

| Indicatore | Base SemisSwitch | Research SPY |
|---|---:|---:|
| Gain totale | +43.265,4% | +24.440,5% |
| CAGR convenzionale | **46,21%** | 41,10% |
| Drawdown massimo | −25,81% | **−24,83%** |
| Durata massima sotto il precedente massimo | **287 sedute** | 496 sedute |
| Sharpe | 1,97 | 2,01 |
| Sortino | 2,97 | 3,04 |
| Volatilità annualizzata | 20,35% | 17,96% |
| VaR storico giornaliero 95% | 1,70% | 1,41% |
| Trade chiusi, inclusi hedge | 17.652 | 15.492 |

Conto finale simulato: circa **86,73 milioni USD** per la base e **49,08 milioni USD** per Research. Le grandezze derivano dal compounding su 16 anni: non dimostrano che sarebbe stato possibile eseguire effettivamente gli ordini a quelle dimensioni. Il VaR è il 5° percentile storico dei rendimenti giornalieri con segno invertito, non la perdita massima possibile.

## Configurazioni e attendibilità del confronto

- [Run Research](https://bt-dash.ilz.it/api/dyn/simulations/jobs/deff8370-985c-42fe-838e-eac863890268/artifacts/stats.html?download=1) · [Run base](https://bt-dash.ilz.it/api/dyn/simulations/jobs/884f113b-59d9-4ba6-a8e7-120d424bd80d/artifacts/stats.html?download=1).
- Entrambi caricano i dati dal **1° gennaio 2005** e terminano il **31 dicembre 2020**; `trade_start_date` è vuota, quindi non c'è un periodo separato con storico caricato ma ordini vietati. Il primo trade azionario registrato in entrambi è il **5 gennaio 2005**. Il pannello Multifactor è precalcolato e può contenere feature basate su dati precedenti; i feed usati per lo switch e gli altri indicatori iniziano invece con il run. Non assumere che tutti gli indicatori siano già a regime dal primo giorno.
- I parametri condivisi risolti sono uguali **tranne il filtro drawdown SPY**: la base usa lookback **21** e aggiornamento **giornaliero**, la Research **63** e aggiornamento **mensile**. I due parametri sono parte dei rispettivi assetti effettivi e possono incidere su giorni operativi, rischio e rendimento.
- La base usa `weak_theme_switch`; la Research usa `weak_theme` e `spy_score_filter=True`. La prima può passare alla lista statica, la seconda usa il ranking Multifactor con soglia SPY. Il limite della classifica mensile è **50** in entrambe; `max_concurrent=60` è invece il limite massimo di posizioni.
- Stesso paniere richiesto e **95 titoli tradabili effettivamente caricati** nei log. Pannello Multifactor con lo stesso SHA256 registrato nei due risultati. I metadata indicano codice modificato localmente: l'identità dei commit non certifica da sola la parità byte per byte di tutte le dipendenze e dei dati di mercato.
- Le **4.028 sedute** dei rendimenti coincidono senza date duplicate o valori mancanti. P&L, rendimenti e commissioni dei log sono riconciliati per entrambe. Il campo `pnlcomm` dei trade non è assunto come profitto netto.

## Come vengono scelti i titoli

La **base SemisSwitch** usa SEMIS per scegliere il ramo: classifica Multifactor fino a 50 titoli quando il regime attiva il ramo dinamico, oppure lista statica configurata `NVDA, AVGO, MU, AMD, MSTR, CEG, ASML, MRVL, ARM, MELI` nell'altro ramo, nei limiti dei feed presenti e dei filtri di ingresso. Nei log si osservano **2.792 chiamate al ramo dinamico** e **1.142 al ramo statico**: sono chiamate registrate dal meccanismo di aggiornamento giornaliero, non operazioni eseguite.

La **Research** calcola lo score Multifactor includendo SPY nei ranghi e seleziona titoli con score strettamente superiore a SPY, fino a 50. Non passa alla lista statica SemisSwitch. Inserire SPY nella classifica può modificare lievemente anche gli score relativi.

Nei **174 mesi** in cui il selettore Research ha registrato uno score SPY valido:

| Misura | Minimo | Mediana | Massimo |
|---|---:|---:|---:|
| Titoli con dati sufficienti per lo score | 68 | 85 | 95 |
| Titoli con score maggiore di SPY | 24 | 49 | 78 |
| Titoli selezionati dopo il limite di 50 | 24 | 49 | 50 |

In **79 mesi** più di 50 titoli superavano SPY ed è intervenuto il limite. Nessuna lista è vuota tra questi 174 mesi registrati; il dato non dimostra che ci sia stata una lista valida in ogni mese dell'intero periodo. Una lista di candidati non coincide con gli ordini: segnali, filtri e sizing determinano le operazioni effettive.

## Varietà effettiva e concentrazione

I numeri seguenti escludono SPY, QQQ e SQQQ e considerano i trade azionari chiusi.

| Indicatore | Base | Research |
|---|---:|---:|
| Titoli distinti negoziati sui 95 caricati | 67 | 69 |
| Trade azionari chiusi | 17.521 | 15.361 |
| Sedute con almeno un ingresso poi chiuso | 3.902 | 3.599 |
| Titoli medi per seduta con ingressi | 4,49 | 4,27 |
| Mediana titoli per seduta con ingressi | 4 | 4 |
| Primi 3 titoli per ingresso normalizzato | 42,7% | 20,7% |
| Primi 5 titoli per ingresso normalizzato | 52,5% | 29,1% |
| Gruppo dei 16 semiconduttori | 55,6% | 30,4% |
| Numero equivalente `1 / Σ quota²` | 13,4 | 30,2 |

La Research **non aumenta molto il numero complessivo di titoli distinti né quelli acquistati in una giornata tipica**, ma riduce sensibilmente il peso degli ingressi sui titoli dominanti. Le quote sono calcolate dividendo ogni controvalore di ingresso per l'equity precedente e normalizzando le somme per titolo. È una proxy dei flussi che riduce la distorsione del compounding, non un'esposizione giornaliera di portafoglio. Il numero equivalente è calcolato sulle quote aggregate, non indica il numero di posizioni contemporanee. Il `positions.csv` non contiene tutte le posizioni azionarie necessarie a misurare direttamente la concentrazione giornaliera.

| Posizione per quota di ingresso normalizzata | Base | Quota | Research | Quota |
|---|---|---:|---|---:|
| 1 | MU | 16,3% | NVDA | 7,4% |
| 2 | NVDA | 15,2% | MU | 6,6% |
| 3 | AMD | 11,3% | AMD | 6,6% |
| 4 | AVGO | 6,2% | NFLX | 4,2% |
| 5 | TSLA | 3,5% | BKNG | 4,2% |

La correlazione dei rendimenti giornalieri dei due portafogli è circa **0,78**: la minore concentrazione non elimina l'esposizione a fattori comuni.

## Robustezza e regimi

| Periodo | Gain base | Max DD base | Gain Research | Max DD Research |
|---|---:|---:|---:|---:|
| 2005–2009 | +378,1% | −24,79% | +357,2% | −19,06% |
| 2010–2015 | +669,8% | −15,96% | +479,6% | −24,83% |
| 2016–2020 | +1.078,4% | −25,81% | +826,0% | −19,05% |
| 2020 | +120,7% | −14,57% | +76,3% | −14,60% |

I segmenti conservano lo stato e il capitale accumulati: non ripartono ogni volta da 200.000 USD. La Research supera la base nel rendimento di **7 anni su 16**. Nel **2008** la base registra circa **+19,3%** e la Research **−17,2%**; nel **2011** rispettivamente **−3,6%** e **−14,1%**. Il drawdown massimo sull'intero periodo simile non significa che le due strategie affrontino allo stesso modo ogni crisi.

La differenza Research meno base dei rendimenti logaritmici annualizzati è circa **−3,56 punti percentuali**. Un bootstrap appaiato a blocchi di 21 sedute, 2.000 repliche, dà un intervallo percentile 95% circa **[−8,87; +1,41] punti**. L'incertezza è ampia e include zero; il calcolo è condizionato al campione e non costituisce un test blind o una correzione per le varianti già esplorate.

Limiti della conclusione:

- **Filtro SPY e rami:** differiscono sia il metodo di selezione sia il lookback/aggiornamento del filtro drawdown. La comparazione misura l'effetto combinato dei due assetti; non è possibile quantificare separatamente il contributo di ciascuna differenza.
- **Survivorship:** i titoli del paniere fornito sono applicati al passato; non sono dimostrati come universo storico point-in-time.
- **Selezione e validazione:** il 2005–2020 e le varianti sono già stati osservati in studi precedenti; non è una finestra blind. Servono una verifica su periodi successivi e controlli di stabilità senza scegliere le regole migliori sullo stesso campione.
- **Warm-up:** i due primi trade coincidono, ma non c'è storico di feed precedente al 2005 per i componenti calcolati durante il run. Una ripetizione con dati precedenti e `trade_start_date=2005-01-03` consentirebbe di valutare l'effetto dell'avvio, senza cambiare le altre impostazioni dei singoli profili.
- **Costi e capacità:** il limite ADV e le commissioni simulate non certificano esecuzione e slippage effettivi, specialmente con equity composte molto grandi.

## Rendimenti e varietà anno per anno

Rendimento composto nel segmento annuale e numero di titoli con trade chiusi aperti nell'anno. Gli anni non sono run riavviati separatamente.

| Anno | Base | Research | Titoli base | Titoli Research |
|---:|---:|---:|---:|---:|
| 2005 | +48,88% | +71,51% | 11 | 13 |
| 2006 | +17,59% | +11,37% | 15 | 16 |
| 2007 | +95,86% | +104,14% | 12 | 11 |
| 2008 | +19,31% | -17,24% | 7 | 11 |
| 2009 | +16,85% | +41,69% | 16 | 14 |
| 2010 | +24,35% | +16,43% | 18 | 17 |
| 2011 | -3,58% | -14,08% | 13 | 15 |
| 2012 | +29,48% | +3,82% | 18 | 21 |
| 2013 | +134,10% | +140,86% | 15 | 13 |
| 2014 | +79,54% | +74,19% | 14 | 13 |
| 2015 | +17,98% | +33,03% | 18 | 19 |
| 2016 | +83,46% | +84,90% | 19 | 17 |
| 2017 | +69,90% | +63,75% | 20 | 18 |
| 2018 | +23,61% | +27,15% | 26 | 24 |
| 2019 | +38,55% | +36,45% | 34 | 29 |
| 2020 | +120,74% | +76,28% | 30 | 21 |

Calcoli riproducibili: [script di analisi](../../research/multifactor_confronto_2005_2020/analyze.py) e [dati riassuntivi](../../research/multifactor_confronto_2005_2020/analisi.json). CAGR, Sharpe e Sortino usano una convenzione di 252 sedute; il Sortino usa soglia zero e downside deviation su tutte le sedute.
