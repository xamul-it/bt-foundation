# Multifactor Research e Multifactor SemisSwitch: confronto dei run 2000–2026

## Risultato principale

Nei due run esaminati **Multifactor Research distribuisce maggiormente gli ingressi tra titoli**, con un drawdown massimo e una volatilità inferiori sull'intero periodo. Il rendimento annualizzato è molto simile, leggermente superiore nella base. Nel segmento 2020–2026, però, la Research ha un drawdown peggiore. Non emerge una superiorità stabile della Research in ogni regime.

**Il confronto non isola il solo filtro SPY.** La base è `OvernightAHMultiFactorSemisSwitch`: usa uno switch SEMIS che alterna ranking Multifactor e lista fissa. La Research è `MultiFactorSpyThreshold`: usa il ranking Multifactor con soglia SPY, senza quello switch. Stiamo quindi confrontando due politiche di scelta dei titoli, non soltanto accensione e spegnimento del filtro.

## Risultati sintetici

Periodo dei dati e del conto: **3 gennaio 2000–2 novembre 2026**. Capitale iniziale 200.000 USD, **sizing composto**, leva ed esposizione massima 1, commissioni Alpaca simulate e slippage zero. `trade_start_date` è vuota per entrambe: non esiste un warm-up separato durante il quale siano vietate operazioni.

| Indicatore | Base SemisSwitch | Research con SPY |
|---|---:|---:|
| Gain totale sul conto | +1.214.904,7% | +1.154.814,9% |
| CAGR convenzionale | 42,07% | 41,80% |
| Drawdown massimo | −30,21% | −23,96% |
| Durata massima sotto il precedente massimo | 351 sedute | 401 sedute |
| Sharpe | 1,83 | 1,93 |
| Sortino | 2,75 | 2,90 |
| Volatilità annualizzata | 20,28% | 19,01% |
| VaR storico giornaliero 95% | 1,74% | 1,62% |
| Trade chiusi, inclusi hedge | 91.688 | 97.540 |

I valori finali simulati sono circa **2,43 miliardi** e **2,31 miliardi** di dollari. Il compounding su quasi 27 anni porta il conto a dimensioni che richiedono un serio controllo di capacità: questi numeri non dimostrano che le posizioni sarebbero state eseguibili realmente a tali dimensioni. Il VaR è il quinto percentile storico dei rendimenti giornalieri con segno invertito: non è la perdita massima possibile.

## Configurazione: cosa è uguale e cosa cambia

- [Run Research](https://bt-dash.ilz.it/api/dyn/simulations/jobs/0f328a4f-efba-4503-86bc-e6f7a072c4e5/artifacts/stats.html?download=1) · [Run base](https://bt-dash.ilz.it/api/dyn/simulations/jobs/50c70a6c-dfa6-4adb-be81-5c18c9f7819b/artifacts/stats.html?download=1).
- I parametri operativi condivisi risolti sono uguali, inclusi capitale, date, universo, leverage, costi, `max_concurrent=60`, `min_concurrent=3`, `hedge_weight=0.25` e pannello Multifactor. **Il limite di lista `monthly_universe_top_n` è 50** in entrambe: 60 è il limite massimo di posizioni, non la dimensione della classifica mensile.
- Le differenze rilevate sono `monthly_universe_mode=weak_theme_switch` nella base contro `weak_theme` nella Research, e `spy_score_filter=True` presente nella Research. Sono differenze sostanziali nella selezione, non parametri operativi identici in senso completo.
- Il pannello indicatori `config-common/indicator_panel/flat_panel_live.csv` ha lo stesso SHA256 registrato nei due risultati. I run sono stati eseguiti in momenti diversi e riportano codice con modifiche locali: i metadata non certificano che ogni sorgente e dato di mercato fosse identico byte per byte.
- Le 6.749 date dei rendimenti coincidono; nessuna è duplicata o mancante. I trade e le commissioni risultano riconciliati con il P&L di ciascun portafoglio. Il campo `pnlcomm` dei trade non è usato come misura affidabile del netto.

## Criterio di scelta dei titoli

### Base: Multifactor con switch SEMIS

Il regime SEMIS decide quale lista usare:

- **Ramo dinamico:** titoli ordinati dal punteggio Multifactor, fino a 50.
- **Ramo statico:** lista configurata `NVDA, AVGO, MU, AMD, MSTR, CEG, ASML, MRVL, ARM, MELI`, soggetta a disponibilità dei feed e filtri di ingresso.

Nei log la base registra il regime dinamico in **222 mesi** e quello statico in **101 mesi**, su 323 mesi osservati. Questo dato descrive le liste attivate, non quanti ordini siano stati effettivamente eseguiti in ogni ramo.

### Research: Multifactor con soglia SPY

La Research calcola il ranking Multifactor e include SPY nel calcolo dei ranghi. Mantiene i titoli con score **strettamente superiore** a SPY, fino a un massimo di 50. Non applica lo switch verso la lista statica della base. L'inserimento di SPY nel calcolo dei ranghi può anche modificare lievemente gli score rispetto al ranking originale.

Nei **290 mesi** per cui il selettore Research ha registrato uno score SPY valido:

| Misura | Minimo | Mediana | Massimo |
|---|---:|---:|---:|
| Titoli valutabili | 57 | 86 | 100 |
| Titoli con score sopra SPY | 17 | 48 | 97 |
| Titoli in lista dopo il limite di 50 | 17 | 48 | 50 |

In **129 mesi** oltre 50 titoli superavano SPY e quindi è intervenuto il limite. Non ci sono liste vuote tra questi 290 mesi registrati; il numero non va esteso ai mesi in cui il selettore non è stato chiamato o non ha prodotto uno score valido. Le liste contengono candidati: filtri, sizing, capitale e segnali determinano poi gli ordini.

## Varietà effettiva e concentrazione

Entrambi i run caricano **100 titoli tradabili**, oltre ai feed di riferimento/hedge, e nel periodo hanno negoziato tutti e 100 almeno una volta. La differenza non è dunque il numero di titoli distinti sull'intero arco, ma **come e quando** sono stati utilizzati.

| Indicatore sui trade azionari chiusi | Base | Research |
|---|---:|---:|
| Titoli distinti nel periodo | 100 | 100 |
| Trade chiusi, esclusi SPY/QQQ/SQQQ | 91.221 | 97.102 |
| Sedute con almeno un ingresso poi chiuso | 6.418 | 5.953 |
| Titoli medi per seduta con ingressi | 14,21 | 16,31 |
| Titoli mediani per seduta con ingressi | 8 | 9 |
| Quota dei primi 3 titoli sugli ingressi normalizzati | 39,7% | 23,7% |
| Quota dei primi 5 titoli | 50,3% | 34,9% |
| Quota del gruppo dei 16 semiconduttori | 49,3% | 30,7% |
| Numero equivalente `1 / Σ quota²` | 14,7 | 25,3 |

Le quote dividono il controvalore di ingresso di ogni trade per l'equity precedente e normalizzano la somma per titolo. È una **proxy della distribuzione dei flussi di ingresso** che attenua la distorsione delle grandi equity finali; non è la quota effettiva di portafoglio detenuta in ogni giorno e non misura da sola il rischio. Il numero equivalente è riferito alle quote aggregate nel tempo, non al numero di posizioni simultanee. Il file posizioni di questi run non contiene una serie completa delle posizioni azionarie necessaria a misurare direttamente la concentrazione giornaliera.

| Posizione per quota di ingresso normalizzata | Base | Quota | Research | Quota |
|---|---|---:|---|---:|
| 1 | NVDA | 16,4% | NVDA | 10,6% |
| 2 | AMD | 11,8% | TSLA | 7,4% |
| 3 | MU | 11,5% | MU | 5,8% |
| 4 | TSLA | 6,7% | AMD | 5,8% |
| 5 | NFLX | 3,9% | AMZN | 5,5% |

La correlazione dei rendimenti giornalieri è circa **0,81**: le strategie rimangono esposte a fattori comuni pur distribuendo gli ingressi in modo diverso.

## Robustezza e sottoperiodi

| Periodo | Gain base | Max DD base | Gain Research | Max DD Research |
|---|---:|---:|---:|---:|
| 2000–2009 | +4.505,5% | −30,21% | +3.955,6% | −19,47% |
| 2010–2019 | +1.927,0% | −17,45% | +2.331,8% | −17,59% |
| 2020–2 novembre 2026 | +1.201,5% | −14,68% | +1.071,0% | −23,96% |
| 2026, fino al 2 novembre | +25,76% | −10,70% | +23,23% | −10,58% |

Sono segmenti di run continui, non conti che ripartono da 200.000 USD all'inizio di ciascun periodo. La Research supera la base nel rendimento di **17 anni su 27**, incluso il 2026 parziale; la differenza cumulata non riflette un vantaggio costante. Nel 2008, per esempio, la base realizza circa **+1,2%** e Research **−16,3%**.

La differenza Research meno base dei rendimenti logaritmici annualizzati dell'intero periodo è **−0,19 punti percentuali**. Un bootstrap appaiato a blocchi di 21 sedute, 2.000 repliche, produce un intervallo percentile 95% di circa **[−4,25; +4,38] punti**: sul campione osservato l'incertezza è ampia rispetto allo scarto stimato. Non è una prova indipendente né corregge la selezione di varianti già studiate.

Nelle ultime 21 sedute fino al 2 novembre 2026 il rendimento è **−4,36% base** e **−7,35% Research**; nelle ultime 63 è rispettivamente **−0,27%** e **−2,62%**. Le perdite recenti da sole non stabiliscono se la strategia abbia perso efficacia.

Limiti decisivi:

- **Survivorship:** il paniere contemporaneo viene applicato al passato; non è uno storico point-in-time dei componenti e può favorire i risultati.
- **Data e warm-up:** il primo trade azionario risulta aperto il 5 gennaio 2000 nella base e il 1° febbraio nella Research. Con `trade_start_date` vuota i primi periodi non hanno lo stesso trattamento operativo; caricare dati precedenti e fissare la stessa data di inizio consentirebbe una verifica migliore, se i dati sono disponibili.
- **Pannello e pesi:** lo stesso pannello è stato usato in entrambi, ma identità del file non certifica indipendenza temporale o assenza di bias nelle sue feature. I pesi derivano da studi precedenti, quindi questo periodo non è una validazione blind.
- **Capacità e costi:** leva 1 non elimina i problemi del compounding su capitali finali miliardari. Il limite ADV e le commissioni simulate non sostituiscono un modello di esecuzione e slippage verificato sugli eseguiti.
- **Attribuzione causale:** poiché switch e soglia cambiano insieme, non è possibile dire quanta parte della differenza sia dovuta al filtro SPY. Servono controlli con stesso ramo Multifactor e SPY nei ranghi con e senza soglia, oltre al confronto con SemisSwitch.

## Rendimenti anno per anno

Rendimento composto del segmento annuale e numero di titoli con trade chiusi aperti nell'anno; il 2026 termina il 2 novembre. Ogni segmento conserva lo stato e il capitale accumulati nei periodi precedenti.

| Anno | Base | Research | Titoli base | Titoli Research |
|---:|---:|---:|---:|---:|
| 2000 | +133,13% | +64,26% | 15 | 20 |
| 2001 | +2,01% | +2,28% | 13 | 15 |
| 2002 | +25,27% | +29,84% | 18 | 14 |
| 2003 | +83,00% | +50,76% | 17 | 16 |
| 2004 | +62,09% | +154,50% | 13 | 18 |
| 2005 | +59,95% | +60,19% | 18 | 18 |
| 2006 | +24,89% | +21,56% | 20 | 21 |
| 2007 | +85,75% | +92,30% | 18 | 17 |
| 2008 | +1,18% | -16,34% | 12 | 14 |
| 2009 | +38,82% | +54,68% | 19 | 16 |
| 2010 | +41,52% | +24,27% | 25 | 25 |
| 2011 | +15,17% | +3,39% | 25 | 27 |
| 2012 | +21,29% | +22,63% | 34 | 36 |
| 2013 | +101,16% | +122,34% | 25 | 20 |
| 2014 | +50,27% | +56,26% | 35 | 30 |
| 2015 | +15,72% | +38,39% | 43 | 42 |
| 2016 | +23,84% | +24,84% | 46 | 46 |
| 2017 | +54,88% | +55,26% | 49 | 48 |
| 2018 | +28,37% | +36,45% | 43 | 55 |
| 2019 | +19,04% | +21,38% | 72 | 72 |
| 2020 | +107,44% | +84,71% | 43 | 42 |
| 2021 | +46,83% | +48,77% | 65 | 62 |
| 2022 | -0,57% | +5,88% | 79 | 83 |
| 2023 | +26,90% | +24,76% | 83 | 83 |
| 2024 | +102,66% | +109,84% | 80 | 82 |
| 2025 | +32,88% | +24,75% | 80 | 89 |
| 2026 | +25,76% | +23,23% | 80 | 79 |

Calcoli riproducibili: [script di analisi](../../research/multifactor_confronto_20261105/analyze.py) e [dati riassuntivi](../../research/multifactor_confronto_20261105/analisi.json). Gli indicatori sono ricalcolati dai `returns.csv` e possono differire dai report HTML se questi usano convenzioni diverse. CAGR e Sharpe/Sortino sono annualizzati su 252 sedute; il Sortino usa soglia zero e downside deviation calcolata su tutte le sedute.
