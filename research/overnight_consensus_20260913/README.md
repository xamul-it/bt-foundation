# OvernightAH consensus: ricerca sul numero di selezionati

La nuova classe `bt-core/strategies/research/overnight_ah_consensus.py` usa due
classifiche mensili, quella legacy di development e quella flat-panel di
challenger. Ogni giorno applica gli stessi controlli di eleggibilita' ai due
ordinamenti, prende i primi `selection_top_n` titoli eleggibili di ciascuno e
opera solo sull'intersezione. Ordina i nomi condivisi per somma dei due ranghi
(poi rango legacy e ticker per i pareggi). Nel regime statico le due liste
coincidono. `bt-core/strategies/overnight_ah_consensus.py` e' solo l'alias
richiesto dal loader CLI, che accetta il formato `modulo.Classe`.

Questa e' una nuova variante **research-only** (`live_enabled=False`), non la
somma di due broker o di due portafogli. Il sizing e l'esecuzione restano
quelli ereditati da OvernightAH. Le due classifiche sono condivise in un
solo processo e memorizzate per mese. `monthly_universe_top_n=10000` impedisce
il troncamento preventivo del pool: il solo limite di shortlist effettivo e'
`selection_top_n`. La classe rifiuta un `monthly_universe_top_n` inferiore
all'universo caricato.

Questa intersezione e' definita **ex ante** sui due ranking con filtri
operativi comuni. Non coincide necessariamente con l'intersezione ex post
degli ordini dei due conti paper esistenti: i due profili hanno cooldown e
soglie `ah_lag1` differenti, mentre questa variante usa un solo percorso
di ordini e un solo stato di cooldown. Per questo i gain qui sotto non vanno
confrontati direttamente con la tabella degli ordini condivisi dei due
backtest separati.

L'esperimento usa lo score flat-panel e i pesi del profilo challenger, il
regime switch e il fallback statico attuali, `liquidity_waterfall`, floor 3,
massimo 20 slot, cap 1% ADV e $10.000/trade, cash iniziale $100.000,
esposizione e margine 1x, commissioni Alpaca, dati Yahoo adjusted dal
2000-01-03 al 2026-09-10 incluso. Il filtro `ah_lag1` e' spento (`0`):
`docs/context/ah_context.md`, sezione *Isolamento filtri*, gli attribuisce
un effetto quasi nullo con waterfall. Restano spenti i filtri di volume
intraday, `min_adv` e l'hedge, come nei profili paper correnti. Il cooldown
resta attivo: nello stesso studio storico spegnerlo peggiorava Sharpe e
drawdown. I 21 pesi dello score flat-panel restano invariati: la
documentazione non prova che eliminarne alcuni preservi il ranking.

## Risultati

`gross_mean_pnl_pct` viene da `trades.json` ed e' la media **lorda** del
ritorno percentuale per trade. La win ratio e' la quota di trade con
`pnl_pct > 0` (quelli a zero non sono vincenti). Il CAGR deriva da `returns.csv`, con le
commissioni simulate. Tutti i 69 run (23 per strategia) hanno prodotto
risultati completi e
ultima data 2026-09-10. Dati completi: `summary.csv`.

Ogni riga usa lo stesso `N`: consensus imposta `selection_top_n=N` con
`max_concurrent=20`; development e challenger impostano `max_concurrent=N`
e `min_concurrent=min(3,N)`. Restano invariati ranking, filtri e limite
mensile dei due profili originali (50 development, 60 challenger). Tutti
partono dallo stesso file di 102 ticker e dallo stesso intervallo storico;
i simboli finali differiscono proprio perche' le strategie selezionano in
modo diverso.

| selection_top_n | Consensus trade | Gain | Win ratio | CAGR | Development trade | Gain | Win ratio | CAGR | Challenger trade | Gain | Win ratio | CAGR |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.179 | 0,1224% | 55,58% | 0,64% | 5.413 | 0,1675% | 56,07% | 2,21% | 4.874 | 0,1973% | 56,57% | 2,41% |
| 2 | 5.403 | 0,1941% | 56,88% | 2,54% | 10.826 | 0,1896% | 56,93% | 4,06% | 9.748 | 0,1970% | 56,84% | 3,95% |
| 3 | 9.194 | 0,2016% | 56,55% | 3,85% | 16.239 | 0,1941% | 56,75% | 5,28% | 14.621 | 0,1841% | 56,33% | 4,86% |
| 4 | 13.030 | 0,1827% | 55,99% | 4,50% | 21.652 | 0,1875% | 56,33% | 6,05% | 19.494 | 0,1677% | 55,95% | 5,41% |
| 5 | 16.849 | 0,1676% | 55,50% | 4,98% | 27.065 | 0,1719% | 56,11% | 6,49% | 24.366 | 0,1650% | 55,69% | 6,04% |
| 6 | 20.859 | 0,1677% | 55,96% | 5,60% | 32.449 | 0,1630% | 56,06% | 6,93% | 29.197 | 0,1599% | 55,71% | 6,52% |
| 7 | 24.468 | 0,1569% | 55,76% | 5,89% | 37.244 | 0,1558% | 55,86% | 7,24% | 33.556 | 0,1530% | 55,72% | 6,83% |
| 8 | 28.219 | 0,1519% | 55,56% | 6,25% | 41.891 | 0,1524% | 55,64% | 7,58% | 37.792 | 0,1540% | 55,68% | 7,27% |
| 9 | 31.075 | 0,1549% | 55,75% | 6,63% | 46.017 | 0,1505% | 55,73% | 7,86% | 41.586 | 0,1525% | 55,71% | 7,56% |
| 10 | 33.840 | 0,1538% | 55,76% | 6,89% | 50.079 | 0,1479% | 55,73% | 8,10% | 45.334 | 0,1492% | 55,71% | 7,78% |
| 11 | 36.846 | 0,1529% | 55,88% | 7,16% | 53.988 | 0,1455% | 55,75% | 8,31% | 48.956 | 0,1504% | 55,84% | 8,09% |
| 12 | 39.778 | 0,1523% | 55,92% | 7,41% | 57.890 | 0,1444% | 55,80% | 8,53% | 52.561 | 0,1501% | 55,84% | 8,34% |
| 13 | 42.892 | 0,1508% | 56,02% | 7,64% | 61.727 | 0,1416% | 55,77% | 8,69% | 56.144 | 0,1460% | 55,87% | 8,48% |
| 14 | 45.584 | 0,1469% | 56,03% | 7,76% | 65.509 | 0,1394% | 55,74% | 8,85% | 59.724 | 0,1435% | 55,85% | 8,64% |
| 15 | 48.334 | 0,1476% | 56,07% | 7,98% | 69.291 | 0,1390% | 55,79% | 9,05% | 63.281 | 0,1420% | 55,83% | 8,81% |
| 16 | 51.092 | 0,1471% | 56,11% | 8,17% | 73.068 | 0,1366% | 55,77% | 9,18% | 66.788 | 0,1390% | 55,77% | 8,92% |
| 17 | 54.093 | 0,1459% | 56,05% | 8,34% | 76.804 | 0,1349% | 55,68% | 9,32% | 70.291 | 0,1371% | 55,73% | 9,06% |
| 18 | 56.854 | 0,1429% | 56,01% | 8,44% | 80.528 | 0,1337% | 55,65% | 9,46% | 73.789 | 0,1346% | 55,67% | 9,17% |
| 19 | 59.894 | 0,1420% | 56,09% | 8,61% | 84.168 | 0,1319% | 55,65% | 9,57% | 77.251 | 0,1337% | 55,63% | 9,31% |
| 20 | 62.679 | 0,1395% | 56,04% | 8,71% | 87.752 | 0,1302% | 55,61% | 9,68% | 80.644 | 0,1331% | 55,61% | 9,45% |
| 30 | 79.751 | 0,1248% | 55,63% | 9,20% | 122.400 | 0,1131% | 55,23% | 10,40% | 112.962 | 0,1143% | 55,17% | 10,13% |
| 50 | 79.883 | 0,1249% | 55,63% | 9,21% | 183.130 | 0,0916% | 54,70% | 11,12% | 170.889 | 0,0923% | 54,61% | 10,88% |
| 102 | 79.883 | 0,1249% | 55,63% | 9,21% | 183.130 | 0,0916% | 54,70% | 11,12% | 196.314 | 0,0850% | 54,43% | 11,09% |

La media sull'intero storico e' massima a **3**, ora default della classe.
La griglia copre ogni intero da 1 a 20 e controlla 30, 50 e 102. A 50 e
102 i trade coincidono per consensus e development; challenger puo' ancora
crescere fino al proprio limite mensile di 60. Per N oltre 50/60, il
`max_concurrent` richiesto ai profili originali non amplia il loro pool
mensile.
Non e' stabile per sottoperiodo: 2000-2012 favorisce 2 (0,2060% contro
0,2002% di 3); 2013-2019 favorisce 4 (0,1711% contro 0,1507% di 3);
2020-2026 favorisce 2 (0,3403% contro 0,2797% di 3). Quindi 3 e' il massimo
in-sample per l'obiettivo richiesto, non un ottimo prospettico dimostrato.
Il CAGR aumenta con `selection_top_n` piu' ampio perche' vengono impiegati
piu' trade e piu' capitale; ottimizzare la media per trade non massimizza il
rendimento del portafoglio.

## Confronto diretto dei tre run

Qui sono affiancati **i tre backtest effettivi**: development, challenger e
nuova strategia consensus. Tutti usano lo stesso universo di 102 ticker,
storico fino al 2026-09-10, $100.000 iniziali, leva 1x, cap ADV 1% e cap
$10.000/trade. Per non cambiare il campione, a ogni valore di selezione le
metriche sono calcolate **sugli stessi giorni fissi**: i 4.866 giorni in cui
entrambi gli originali comprano tre titoli, oppure i 3.730 giorni in cui
entrambi ne comprano dieci. Ogni run mantiene i propri filtri operativi e i
propri simboli acquistati; solo i giorni di valutazione sono identici.

| Selezione | Giorni fissi | Run | Trade | Gain medio/trade | Win ratio |
|---:|---:|---|---:|---:|---:|
| 3 | 4.866 | Development | 14.598 | 0,1929% | 56,95% |
| 3 | 4.866 | Challenger | 14.598 | 0,1829% | 56,29% |
| **3** | **4.866** | **Consensus** | **9.191** | **0,2009%** | **56,53%** |
| 10 | 3.730 | Development | 37.300 | 0,1599% | 56,55% |
| 10 | 3.730 | Challenger | 37.300 | 0,1662% | 56,30% |
| **10** | **3.730** | **Consensus** | **25.887** | **0,1795%** | **56,61%** |

Fonte riproducibile: `three_run_comparison.csv`, prodotto da
`compare_three_runs.py`. La riga consensus non e' la vecchia intersezione
ex-post: a 3 selezionati condivide 9.173 delle 9.182 coppie giorno-simbolo
originarie, e a 10 ne condivide 25.845 delle 25.867. I piccoli scarti sono
dovuti ai filtri operativi comuni della nuova classe. Se si imponesse
letteralmente lo stesso insieme di coppie giorno-simbolo a tutti e tre i
run, il gain di ogni coppia sarebbe identico per costruzione.

## Tabelle ex-post degli ordini condivisi

Le tabelle precedenti su intersezione ed esclusivi sono state ricostruite
dagli stessi `trades.json`: solo giorni in cui entrambe le strategie hanno
acquistato esattamente 3 o 10 titoli, secondo il caso. Il trade condiviso e'
contato una sola volta e il suo `pnl_pct` coincide nei due file. Dati completi
in `overlap_summary.csv`, generati da `compare_overlap.py`.

| Slot per strategia | Giorni fissi | Gruppo | Trade | Gain medio/trade | Win ratio |
|---:|---:|---|---:|---:|---:|
| 3 | 4.866 | Development, tutti i suoi acquisti | 14.598 | 0,1929% | 56,95% |
| 3 | 4.866 | Challenger, tutti i suoi acquisti | 14.598 | 0,1829% | 56,29% |
| 3 | 4.866 | Intersezione | 9.182 | 0,1973% | 56,51% |
| 3 | 4.866 | Solo development | 5.416 | 0,1855% | 57,68% |
| 3 | 4.866 | Solo challenger | 5.416 | 0,1584% | 55,91% |
| 3 | 4.866 | Esclusivi combinati | 10.832 | 0,1719% | 56,79% |
| 10 | 3.730 | Development, tutti i suoi acquisti | 37.300 | 0,1599% | 56,55% |
| 10 | 3.730 | Challenger, tutti i suoi acquisti | 37.300 | 0,1662% | 56,30% |
| 10 | 3.730 | Intersezione | 25.867 | 0,1787% | 56,60% |
| 10 | 3.730 | Solo development | 11.433 | 0,1174% | 56,42% |
| 10 | 3.730 | Solo challenger | 11.433 | 0,1381% | 55,62% |
| 10 | 3.730 | Esclusivi combinati | 22.866 | 0,1278% | 56,02% |

La win ratio misura solo la frequenza dei trade positivi: per esempio a 3
slot gli esclusivi development vincono piu' spesso dell'intersezione, ma
l'intersezione ha il gain medio maggiore.

Questa tabella usa esclusivamente gli insiemi di acquisti originari dei due
backtest separati. La tabella di tuning della nuova strategia sopra e' un
esperimento diverso e non va accostata riga per riga a questa: comprende
giorni e, in pochi casi, simboli differenti.

Per riprodurre, `run_sweep.sh` esegue i 23 run consensus. Per ogni `N`
mancante di development/challenger eseguire
`bash research/overnight_consensus_20260913/run_baseline_grid.sh PROFILO N`
con `PROFILO=development` o `challenger`; quindi eseguire
`python research/overnight_consensus_20260913/summarize.py`,
`compare_overlap.py` e `compare_three_runs.py` dalla root. Gli script usano
i profili schedulati come sorgente dei parametri e li sovrascrivono solo
per lo sweep; non modificano le schedulazioni. Nessuna delle due
strategie esistenti e' stata modificata, quindi non e' stato necessario un
run di regressione prima/dopo su di esse. Test pertinenti:
`bt-core/tests/test_overnight_ah_consensus.py` e
`bt-core/tests/test_overnight_liquidity_waterfall.py` (5 test passati).

### Esecuzione singola fino ai dati disponibili al 2026-09-13

Dal root del repository:

```bash
cd /home/htpc/backtrader/bt-core
source ../config-common/scheduled/strategies/overnight-ah-challenger.env
.venv/bin/python btmain.py \
  --mode backtest \
  --strat overnight_ah_consensus.OvernightAHConsensus \
  --ticker "$TICKER" \
  --stratargs "$STRATARGS monthly_universe_top_n=10000 selection_top_n=3 max_concurrent=20 min_concurrent=3 max_notional_per_trade=10000 ah_lag1_threshold=0 auction=True" \
  --timeframe daily --provider "$DATA_PROVIDER" \
  --fromdate 2000-01-03 --todate 2026-09-13 \
  --margin-leverage 1 --commission alpaca --cash 100000 \
  --id consensus_fullpool_select3_2000_20260913
```

`source` fornisce il pannello, i 21 pesi del composito e i parametri di
regime/cooldown. Gli override finali impostano la variante nuova. Il run di
controllo con questo intervallo e' terminato senza errori: ultimo dato
2026-09-11, 9.197 trade, gain medio 0,2020%, win ratio 56,56%.

Nessun parametro o pezzo di codice e' stato eliminato dalle strategie
esistenti. Nella nuova variante `monthly_universe_top_n` e' reso inattivo
come taglio della shortlist (10000, con validazione), mentre
`ah_lag1_threshold=0` spegne il filtro; entrambi restano implementati nella
classe base. La nuova classe aggiunge `selection_top_n` e riusa il ranking
legacy e flat-panel, il sizing e gli ordini gia' esistenti.
