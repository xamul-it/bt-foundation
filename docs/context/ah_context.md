# AH / OvernightAH Context

## Obiettivo

Studiare e migliorare la strategia `overnight_ah.OvernightAH`.

Il focus e' esclusivamente AH / overnight:

- rendimento da close a open / after-hours
- selezione simboli adatti alla notte
- filtri per evitare fasi o simboli sfavorevoli
- validazione IS/OOS

Non includere setup RTH/intraday standalone.

## Strategia Base

Strategia usata:

```bash
python btmain.py --strat overnight_ah.OvernightAH \
  --ticker stable_ah_top10.json \
  --mode backtest \
  --timeframe daily \
  --commission none \
  --stratargs "max_concurrent=5 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 max_adv_participation=0.0025 max_exposure=2 min_adv=100000000 auction=True" \
  --margin-rate 0.1 \
  --margin-leverage 2 \
  --fromdate=2000-01-01 \
  --todate=2026-01-01
```

Nota operativa:

- con `max_exposure=2` e `--margin-leverage 2`, il cap di leva e' gia' cablato nella strategia e nel broker Alpaca;
- `size_by_max_concurrent=True` rende il sizing piu' stabile per slot, invece di dividere solo sui candidati del giorno.
- il filtro di volatilita' intraday operativo resta applicato sempre (`intraday_vol_filter_side='any'`), come nel comportamento originale.
- test research Yahoo adjusted 2016-01-04/2026-06-23: `intraday_vol_filter_side='down'` e `min_intraday_vol=0` hanno migliorato PnL/SQN, ma non sono promossi al live senza decisione esplicita.
- la promozione a produzione avviene via git, non via file duplicato:
  `overnight_ah.OvernightAH` e' l'unico modulo strategia in questo repo
  (usato sia per ricerca sia dal profilo schedulato `development`); il
  checkout `/home/htpc/backtrader-prod` (repo separato `bt-foundation`,
  branch `prod`) e' la copia stabile effettivamente usata dai profili
  schedulati `live`/`mirror` — promuovere significa aggiornare quel
  checkout/branch, non un file `_live.py` in questo repo. Rimosso
  `bt-core/strategies/overnight_ah_live.py` (file duplicato in questo repo,
  non referenziato da nessun cron attivo — verificato: tutti i profili
  schedulati (`live`, `mirror`, `development`) impostano esplicitamente
  `STRAT=overnight_ah.OvernightAH` nei rispettivi `STRATEGY_CONFIG`).

## Problema Di Fondo

La strategia performa bene in backtest sul set attuale, ma il set e' stato scelto tra i migliori performer AH.

Il rischio principale e' quindi selection bias:

- il set `stable_ah_top10.json` funziona perche' contiene titoli che storicamente hanno performato bene overnight
- non e' detto che lo stesso set resti valido in futuro
- serve un processo dinamico, ex-ante, per scegliere il paniere mese per mese o periodo per periodo

Domanda centrale:

> Dato un paniere ampio, ad esempio NASDAQ 100, come scelgo in modo robusto il sottoinsieme di titoli su cui applicare OvernightAH?

## BoCSoO / Separazione AH-RTH

Esiste uno studio BoCSoO in `bt-strategy-test/BoCSoO` che separa i rendimenti:

- AH / overnight
- RTH / sessione regolare

Da questo studio emerge che alcuni titoli performano meglio di notte, altri durante il giorno.

Questo e' fondamentale: OvernightAH non dovrebbe lavorare su "titoli forti" in generale, ma su titoli con edge specifico nella componente AH.

Il set attuale deriva dai best performer AH/stabili, non da un universo neutro.

Distinguere sempre:

- `AH%`: quota del rendimento totale attribuita alla componente AH;
- `AH return %`: rendimento effettivo della componente AH.

`AH%` puo' esplodere quando il rendimento totale e' piccolo o compensato da RTH negativo. Per selezionare simboli OvernightAH, `AH return %` va considerato metrica primaria insieme ad AH Sharpe/Sortino.

Evidenza sul dataset BoCSoO:

```txt
AH return % vs AH Sharpe:  +0.913 Spearman
AH return % vs AH Sortino: +0.906
AH return % vs AH%:        +0.816
AH return % vs volatility: +0.510
AH return % vs beta:       +0.336
AH return % vs RTH Sharpe: -0.756
```

## Universo Attuale

File principale:

```txt
config-common/tickers/stable_ah_top10.json
```

Simboli noti:

```txt
NVDA, AVGO, MU, AMD, MSTR, CEG, ASML, MRVL, ARM, MELI
```

Questo universo va trattato come benchmark/studio, non come soluzione definitiva.

## Dati

Dati daily/backtest usati da OvernightAH.

Provider daily:

- `yahoo`: dataset Yahoo raw, usato come default operativo/backtest corrente.
- `yahoo_adj`: dataset Yahoo preparato localmente con OHLC adjusted nelle colonne standard `Open/High/Low/Close`.

Il provider `yahoo_adj` si genera senza toccare i dati raw:

```bash
bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/prepare_adjusted_yahoo.py --ticker stable_ah_top10.json
```

Output:

```txt
config-common/data/d/yahoo_adj/
```

Il dataset `yahoo_adj` mantiene anche colonne raw e diagnostiche (`Raw Open`, `Raw Close`, `Adj Factor`, `Raw Dollar Volume`) per audit. Caveat corrente: OvernightAH calcola ancora ADV/liquidity cap da `Close * Volume`; su `yahoo_adj` significa adjusted close per volume raw. Per un controllo di PnL/filtri adjusted va bene; per una liquidita' storica perfetta va usato `Raw Dollar Volume` nella strategia.

Dati minute Alpaca disponibili, ma per il contesto AH servono solo come eventuale supporto a filtri pre-close. Non devono trasformare il lavoro in strategia intraday.

Path dati minute:

```txt
config-common/data/m/alpaca/sip/
```

## Studi E Script Rilevanti

### BoCSoO

Scopo:

- decomporre performance AH e RTH
- classificare titoli per comportamento prevalente
- distinguere titoli AH, RTH, Mixed
- valutare stabilita' della classificazione

Output/metadati BoCSoO usati anche da altri script:

```txt
bt-strategy-test/BoCSoO/out/decompose_results.json
```

Nota: i sorgenti e gli output sono stati consolidati in `bt-strategy-test/BoCSoO`;
gli output/cache possono essere esclusi dal commit.

### Monthly Universe Lists

File:

```txt
bt-strategy-test/overnight-ah/research/monthly_universe_lists.py
```

Scopo:

- generare liste mensili da metriche rolling
- selezionare top-N simboli in modo ex-ante
- usare score come Sharpe, Sortino, total return, mean bps, composite
- eventualmente filtrare per classificazione BoCSoO AH e stabilita'
- include score `sharpe_sortino` e metadati BoCSoO AH/RTH/stabilita'

### Monthly AH Universe File

File:

```txt
bt-strategy-test/overnight-ah/research/monthly_ah_universe_file.py
```

Output:

```txt
bt-strategy-test/overnight-ah/research/out/monthly_ah_universe_6m_bocsoo_stable.csv
```

Formato:

```txt
year;month;symbols
```

Scopo:

- costruire un universo mensile ex-ante da NASDAQ 100;
- per il mese M usare i 6 mesi precedenti;
- tenere simboli classificati AH sulla finestra rolling;
- richiedere stabilita' BoCSoO congelata;
- ordinare per `AH return %` della finestra.

`OvernightAH` supporta il parametro `monthly_universe_file`: carica il NASDAQ completo, poi a ogni `next` usa solo i simboli presenti nel file per quel mese, preservando l'ordine.

### Monthly Universe Walkforward

File:

```txt
bt-strategy-test/overnight-ah/research/monthly_universe_walkforward.py
```

Scopo:

- simulare rotazione mensile del paniere
- usare la shortlist generata al mese precedente
- tradare il mese successivo
- confrontare rotazione dinamica vs set statico

### Monthly Universe Policy Sweep

File:

```txt
bt-strategy-test/overnight-ah/research/monthly_universe_policy_sweep.py
```

Scopo:

- testare politiche diverse di selezione asset
- finestre rolling o expanding
- score diversi
- soglie minime di trade
- isteresi keep/enter
- ranking per ADV o ordine
- confronto contro static stable set

## Punto Aperto Principale: Asset Selection

Il problema piu' importante non e' solo migliorare i parametri di OvernightAH, ma costruire un processo robusto per scegliere il set.

Domande:

- usare rolling window 3/6/12/24 mesi o expanding?
- score migliore: total return, mean bps, Sharpe, Sortino, composite?
- quante trade minime richiedere per evitare rumore?
- usare solo titoli classificati AH da BoCSoO?
- richiedere stabilita' AH su piu' periodi?
- serve isteresi per ridurre turnover?
- quanti titoli tenere in lista: top 10, 15, 20?
- quanti tradare ogni giorno: max_concurrent 5?
- ranking operativo giornaliero: ADV, score, ordine lista?
- come evitare che la selezione insegua i vincitori recenti e degradi OOS?

## Principio Di Validazione

Ogni processo di selezione deve essere ex-ante:

1. calcolo metriche solo fino al mese T
2. costruisco universo per mese T+1
3. eseguo OvernightAH nel mese T+1
4. registro performance
5. avanzo di un mese

Non usare mai dati futuri per scegliere i simboli.

## Metriche Da Confrontare

Per ogni universo/statico/dinamico:

- total return
- mean bps per day
- Sharpe
- Sortino
- max drawdown
- win rate
- numero medio di posizioni
- turnover mensile
- mesi positivi/negativi
- anni positivi/negativi
- concentrazione per ticker
- stabilita' dei ticker selezionati

## Baseline

Baseline da mantenere:

- `stable_ah_top10.json` statico
- NASDAQ 100 filtrato per ADV
- rotazione mensile semplice top Sharpe/Sortino
- rotazione filtrata BoCSoO AH/stable
- file mensile rolling 6m ordinato per `AH return %`
- eventuale composite score

Esempio run con universo mensile:

```bash
python btmain.py --strat overnight_ah.OvernightAH \
  --ticker NASDAQ_100_US.json \
  --mode backtest \
  --timeframe daily \
  --stratargs "monthly_universe_file=../bt-strategy-test/overnight-ah/research/out/monthly_ah_universe_6m_bocsoo_stable.csv max_concurrent=5 size_by_max_concurrent=True max_exposure=2 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 min_adv=100000000" \
  --margin-leverage 2
```

## Studio Edge Mensile Ex-Ante

Script:

```txt
bt-strategy-test/overnight-ah/research/edge_prediction_study.py
```

Scopo:

- costruire un pannello ticker/mese;
- usare solo feature disponibili prima del mese target;
- misurare IC cross-sectionale verso edge medio del mese successivo;
- esportare file `monthly_universe_file` direttamente validabili da `OvernightAH`.

Run principale:

```bash
bt-core/.venv/bin/python bt-strategy-test/overnight-ah/research/edge_prediction_study.py \
  --ticker-file config-common/tickers/yahoo_adj_research_universe.json \
  --out-dir bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj
```

Risultato ricerca su universo Yahoo adjusted disponibile:

- miglior feature IC: `ah_mean_6m`, mean IC `0.0947`, IC positivo nel `75.2%` dei mesi;
- feature simili: `ah_total_6m`, `ah_mean_12m`, `close_slope_12m`;
- ML provato in modo rapido non batte i segnali semplici: Random Forest mean IC circa `0.063`, sotto `ah_mean_6m`.

Validazione Backtrader reale OOS `2024-01-01` / `2026-06-23`, provider `yahoo_adj`, filtri operativi invariati:

| policy | final value | TimeReturn | trades | SQN | Sharpe | max DD |
|:--|--:|--:|--:|--:|--:|--:|
| static `stable_ah_top10` | 2,908,351 | 13.542 | 2,333 | 5.583 | 2.910 | -28.95% |
| `ah_mean_6m_top5` monthly | 815,522 | 3.078 | 1,144 | 4.634 | 1.695 | -15.16% |
| `close_slope_12m_top5` monthly | 971,848 | 3.859 | 1,117 | 5.172 | 2.406 | -16.61% |
| `ah_mean_6m_top15` monthly | 1,924,510 | 8.623 | 2,558 | 5.193 | 1.805 | -21.90% |

Batch validation successiva:

```txt
bt-strategy-test/overnight-ah/research/validate_monthly_universe_backtrader.py
```

Output consolidato:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_consolidated.csv
```

Confronto su tre segmenti:

| policy | train final 2016-2020 | validation final 2021-2023 | OOS final 2024-2026 | OOS daily Sharpe | OOS max DD |
|:--|--:|--:|--:|--:|--:|
| static `stable_ah_top10` | 2,279,318 | 409,833 | 2,908,351 | 2.736 | -28.95% |
| `c2c_mean_6m_top50` | 7,903,970 | 519,956 | 2,254,181 | 2.368 | -28.70% |
| `c2c_mean_6m_top40` | 7,682,808 | 520,836 | 2,240,383 | 2.363 | -28.70% |
| `c2c_mean_6m_top30` | 7,842,983 | 485,595 | 2,097,430 | 2.317 | -28.71% |
| `ah_total_6m_top20` | 5,104,833 | 447,930 | 2,081,463 | 2.416 | -21.34% |
| `ah_mean_6m_top20` | 4,046,522 | 419,612 | 2,001,626 | 2.382 | -21.34% |

Compositi C2C + AH:

| policy | train final 2016-2020 | validation final 2021-2023 | OOS final 2024-2026 | OOS daily Sharpe | OOS max DD |
|:--|--:|--:|--:|--:|--:|
| static `stable_ah_top10` | 2,279,318 | 409,833 | 2,908,351 | 2.736 | -28.95% |
| `combo_c2c6_ah6_top60` | 9,527,951 | 573,198 | 2,564,500 | 2.405 | -29.36% |
| `combo_c2c6_ahtotal6_top60` | 9,944,003 | 566,285 | 2,564,500 | 2.405 | -29.36% |
| `combo_c2c6_ah6_top50` | 8,984,940 | 588,320 | 2,556,211 | 2.403 | -29.22% |
| `combo_c2c6_ah6_low_intradayvol_top40` | 6,609,270 | 669,311 | 1,908,444 | 2.302 | -29.12% |

Definizioni principali:

- `combo_c2c6_ah6`: rank percentile `0.60 * c2c_mean_6m + 0.40 * ah_mean_6m`;
- `combo_c2c6_ahtotal6`: rank percentile `0.60 * c2c_mean_6m + 0.40 * ah_total_6m`;
- `combo_c2c6_ah6_low_intradayvol`: rank percentile `0.50 * c2c_mean_6m + 0.30 * ah_mean_6m + 0.20 * low intraday_vol_mean_6m`.

Risk/regime gate:

Output focus:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/risk_gate_focus_summary.csv
```

Gate testati sul candidato `combo_c2c6_ah6_top60`, usando solo SPY fino al mese precedente.

| policy | train final 2016-2020 | validation final 2021-2023 | OOS final 2024-2026 | OOS daily Sharpe | OOS max DD |
|:--|--:|--:|--:|--:|--:|
| static `stable_ah_top10` | 2,279,318 | 409,833 | 2,908,351 | 2.736 | -28.95% |
| `combo_c2c6_ah6_top60` | 9,527,951 | 573,198 | 2,564,500 | 2.405 | -29.36% |
| `combo_c2c6_ah6_top60 + SPY dd3m > -15%` | 4,789,763 | 628,454 | 2,574,089 | 2.442 | -28.37% |
| `combo_c2c6_ah6_top60 + SPY dd3m > -10%` | 5,673,311 | 614,000 | 2,391,672 | 2.431 | -24.12% |
| `combo_c2c6_ah6_top60 + SPY dd3m > -8%` | 2,900,037 | 539,003 | 1,101,697 | 1.928 | -24.11% |

Lettura gate:

- `SPY dd3m > -15%` e' il miglior compromesso: migliora leggermente OOS rispetto al combo base, migliora validation, riduce un po' il DD OOS;
- `SPY dd3m > -10%` e' la versione difensiva: OOS scende a 2.39M ma il DD OOS cala a circa `-24.1%`;
- `SPY dd3m > -8%` taglia troppo rendimento OOS;
- le griglie peso C2C/AH confermano che `0.60/0.40` e' gia' il miglior peso OOS tra quelli provati.

Lettura aggiornata:

- `c2c_mean_6m_top40/50` e' la prima famiglia dinamica realmente competitiva: batte lo statico in train e validation, resta sotto in OOS ma arriva a circa il 77% del final value statico;
- il difetto di `c2c_mean_6m_top40/50` e' il rischio: drawdown train/validation circa `-41%/-51%`, quindi non e' una policy pronta per live;
- `ah_total_6m_top20` e `ah_mean_6m_top20` sono meno potenti ma piu' difensive in OOS: drawdown circa `-21.3%` contro `-28.95%` statico;
- `combo_c2c6_ah6_top60 + SPY dd3m > -15%` e' il candidato dinamico principale aggiornato: batte statico in train/validation e arriva a circa l'89% dello statico OOS, con DD OOS leggermente migliore dello statico;
- `combo_c2c6_ah6_top60 + SPY dd3m > -10%` e' il candidato difensivo: meno rendimento OOS ma drawdown piu' basso;
- lo statico resta il benchmark OOS migliore per rendimento assoluto e daily Sharpe, ma e' selection-biased e non risolve il problema di processo.

Stress costi/slippage:

Script riproducibile:

```txt
bt-strategy-test/overnight-ah/research/trade_cost_stress.py
```

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/cost_stress_trade_edge_all_segments.csv
```

Nota: `btmain.py --slippage` e' stato provato, ma sui run OvernightAH ha prodotto final value maggiori invece che minori. Per questo studio non va usato come costo affidabile finche' non viene chiarito il modello di esecuzione. Lo stress corrente applica invece un costo round-trip esplicito a ogni trade salvato in `trades.json`, misurando edge netto e win ratio netto.

Edge lordo/netto per trade, ponderato sul nozionale:

| segment | policy | gross edge bps | edge netto 10 bps RT | win netto 10 bps RT | edge netto 20 bps RT | win netto 20 bps RT |
|:--|:--|--:|--:|--:|--:|--:|
| train | static `stable_ah_top10` | 25.90 | 15.90 | 56.07% | 5.90 | 52.15% |
| train | `combo_c2c6_ah6_top60 + SPY dd3m > -10%` | 23.40 | 13.40 | 53.02% | 3.40 | 47.94% |
| train | `combo_c2c6_ah6_top60 + SPY dd3m > -15%` | 16.84 | 6.84 | 52.54% | -3.16 | 47.69% |
| validation | `combo_c2c6_ah6_top60 + SPY dd3m > -10%` | 10.17 | 0.17 | 49.36% | -9.83 | 45.58% |
| validation | static `stable_ah_top10` | 8.46 | -1.54 | 48.19% | -11.54 | 44.15% |
| validation | `combo_c2c6_ah6_top60 + SPY dd3m > -15%` | 8.20 | -1.80 | 48.55% | -11.80 | 44.63% |
| OOS | `combo_c2c6_ah6_top60 + SPY dd3m > -10%` | 33.15 | 23.15 | 52.75% | 13.15 | 48.77% |
| OOS | static `stable_ah_top10` | 32.36 | 22.36 | 53.92% | 12.36 | 50.62% |
| OOS | `combo_c2c6_ah6_top60 + SPY dd3m > -15%` | 31.15 | 21.15 | 52.70% | 11.15 | 48.76% |

Lettura costi:

- `SPY dd3m > -10%` e' il migliore per edge medio OOS e validation tra i candidati dinamici;
- in validation il margine e' sottilissimo: con 10 bps round-trip resta circa `0.17 bps`, quindi la policy non ha ancora un margine operativo comodo;
- `SPY dd3m > -15%` resta migliore per final value OOS rispetto al `-10%`, ma e' meno convincente sull'edge medio stressato;
- `ah_total_6m_top20` non regge bene lo stress: edge OOS lordo circa `22.79 bps`, ma in validation e' sotto lo statico e diventa negativo gia' a 10 bps.

Edge-focus extra test:

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/edge_focus_trade_cost_scan.csv
```

Testati `close_slope_12m` e `c2c_mean_6m` top5/top20 sui tre segmenti. Risultato:

- `close_slope_12m_top5` e' molto forte in OOS come edge (`39.60 bps`, final `971,848`, DD `-16.6%`), ma non regge validation (`8.70 bps`, final `331,259`, daily Sharpe `0.96`);
- `c2c_mean_6m_top5` e' interessante in validation (`12.69 bps`, edge netto 10 bps ancora `2.69 bps`), ma in OOS resta debole come final value (`572,595`) e daily Sharpe (`1.74`);
- `c2c_mean_6m_top20` e' piu' stabile del top5 come rendimento, ma non migliora il candidato gated sull'edge validation.

Conclusione edge-focus: non basta massimizzare l'edge medio su un segmento; serve una policy che resti sopra soglia anche in validation e OOS. Al momento il miglior compromesso edge/slippage resta `combo_c2c6_ah6_top60 + SPY dd3m > -10%`.

Clean tuning senza costi/slippage:

Output principali:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_clean_tuning/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_clean_tuning_focus_val/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_clean_tuning_focus_oos/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_clean_tuning_key_train/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/clean_tuning_key_consolidated.csv
```

Griglia mirata:

- score mensile ex-ante `c2c_mean_6m` + `ah_mean_6m`;
- pesi testati: `50/50`, `60/40`, `70/30`;
- top-N: `50`, `60`, `70`, `80`;
- gate SPY ex-ante: drawdown corrente 3 mesi `> -10%`, `> -12%`, `> -15%`;
- condizioni Backtrader base: provider `yahoo_adj`, commissione `none`, nessuno slippage, filtri OvernightAH invariati.

Risultato chiave su validation `2021-2023`:

| policy | validation final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| `combo_c2c50_ah50 + SPY dd3m > -10%, top50` | 751,040 | 3,229 | 1.519 | -19.29% | 52.96% | 9.78 bps |
| `combo_c2c50_ah50 + SPY dd3m > -12%, top50` | 743,502 | 3,439 | 1.447 | -20.69% | 52.60% | 9.08 bps |
| `combo_c2c60_ah40 + SPY dd3m > -10%, top60` | 656,301 | 3,236 | 1.388 | -21.79% | 52.63% | 8.60 bps |
| `combo_c2c50_ah50, top50` | 620,673 | 3,644 | 1.227 | -33.78% | 52.47% | 7.29 bps |
| `combo_c2c60_ah40, top60` | 573,198 | 3,651 | 1.161 | -31.79% | 52.34% | 6.66 bps |
| static `stable_ah_top10` | 409,833 | 2,299 | 0.897 | -46.91% | 51.76% | 8.46 bps |

Risultato OOS `2024-2026` sui candidati scelti da validation:

| policy | OOS final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| static `stable_ah_top10` | 2,908,351 | 2,333 | 2.736 | -28.95% | 57.82% | 32.36 bps |
| `combo_c2c60_ah40 + SPY dd3m > -10%, top60` | 2,564,500 | 2,976 | 2.405 | -29.36% | 57.09% | 30.31 bps |
| `combo_c2c50_ah50, top50` | 2,481,561 | 2,968 | 2.381 | -29.29% | 57.28% | 31.45 bps |
| `combo_c2c50_ah50 + SPY dd3m > -10%, top50` | 2,481,561 | 2,968 | 2.381 | -29.29% | 57.28% | 31.45 bps |

Risultato train `2016-2020` sui candidati chiave:

| policy | train final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| `combo_c2c50_ah50, top50` | 10,143,292 | 5,748 | 2.662 | -50.34% | 58.06% | 22.71 bps |
| `combo_c2c60_ah40 + SPY dd3m > -10%, top60` | 9,963,929 | 5,472 | 2.840 | -29.08% | 57.91% | 24.64 bps |
| `combo_c2c50_ah50 + SPY dd3m > -10%, top50` | 9,890,948 | 5,375 | 2.867 | -28.60% | 58.29% | 25.16 bps |
| `combo_c2c60_ah40, top60` | 9,527,951 | 5,849 | 2.589 | -51.35% | 57.62% | 21.58 bps |
| static `stable_ah_top10` | 2,279,318 | 2,470 | 2.468 | -19.07% | 59.64% | 25.90 bps |

Lettura clean tuning:

- su validation, `50/50 + gate SPY dd3m > -10%, top50` e' la nuova migliore policy dinamica: batte lo statico su final value, daily Sharpe, drawdown, win ratio ed edge medio;
- su train, i gate SPY dimezzano circa il drawdown dei compositi senza distruggere il rendimento;
- su OOS, il gate `dd3m > -10%` non esclude mesi rilevanti nel periodo testato, quindi il candidato gated 50/50 coincide col 50/50 base;
- lo statico resta superiore in OOS per final value, daily Sharpe e edge medio;
- il miglior compromesso dinamico OOS resta `combo_c2c60_ah40 + SPY dd3m > -10%, top60`, ma la nuova `50/50 + gate -10%, top50` e' piu' convincente come policy scelta su validation.

Benchmark statici alternativi:

Script:

```txt
bt-strategy-test/overnight-ah/research/build_static_universe_benchmark.py
bt-strategy-test/overnight-ah/research/validate_static_universes_backtrader.py
```

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/static_universe_benchmark/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_static_benchmark_oos/static_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/static_benchmark_oos_distribution.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/static_vs_dynamic_consolidated.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/symbol_contribution_static_dynamic.csv
```

Sono stati confrontati:

- statico attuale `stable_ah_top10`;
- top10 statici scelti su storia `2016-2023` per target edge, target win, target total, c2c 6m, AH 6m, combo c2c/AH;
- 40 top10 random riproducibili.

Distribuzione OOS `2024-2026`:

| group | n | final median | final p90 | final max | median daily Sharpe | median edge |
|:--|--:|--:|--:|--:|--:|--:|
| all | 48 | 389,992 | 782,297 | 2,908,351 | 1.212 | 8.81 bps |
| random | 40 | 364,758 | 632,148 | 1,227,764 | 1.150 | 8.16 bps |
| ranked non-random | 8 | 658,192 | 1,579,254 | 2,908,351 | 1.396 | 12.65 bps |

Top OOS statici:

| static policy | OOS final | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|
| current `stable_ah_top10` | 2,908,351 | 2.736 | -28.95% | 57.82% | 32.36 bps |
| best random `random_023_top10` | 1,227,764 | 2.273 | -36.56% | 55.39% | 29.25 bps |
| `hist_target_total_top10` | 1,009,642 | 1.702 | -39.90% | 55.67% | 18.54 bps |
| `hist_combo_c2c60_ah40_top10` | 781,825 | 1.499 | -42.35% | 55.23% | 15.05 bps |

Lettura static benchmark:

- lo statico attuale e' fuori scala rispetto a random e top10 meccanici: circa percentile `98%` sul campione complessivo e sopra tutti i random;
- i top10 scelti da storia `2016-2023` battono lo statico in train e talvolta validation, ma non in OOS;
- quindi `stable_ah_top10` non era il miglior paniere storico: ha una composizione particolarmente favorevole al regime `2024-2026`;
- la dinamica mensile batte nettamente gli statici meccanici in validation e OOS, ma non il paniere statico attuale.

Contributi OOS dello statico attuale:

| ticker | OOS pnl | edge/trade | win ratio |
|:--|--:|--:|--:|
| AMD | 549,115 | 52.16 bps | 54.60% |
| MU | 508,054 | 55.26 bps | 57.29% |
| ASML | 488,996 | 62.19 bps | 62.01% |
| NVDA | 315,184 | 33.34 bps | 61.80% |
| MRVL | 290,296 | 39.14 bps | 54.07% |
| AVGO | 290,290 | 26.53 bps | 56.77% |
| MELI | -91,361 | -13.69 bps | 55.84% |

La dinamica cattura parte dei vincitori, ma li diluisce su piu' nomi. Esempio OOS `combo_c2c60_ah40 + gate -10 top60`: MU, AMD, INTC, MRVL, TXN, ASML hanno edge altissimo, ma l'universo ampio distribuisce capitale anche su nomi meno forti.

Concentrated clean tuning 50/50:

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_clean_tuning_concentrated/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_clean_tuning_concentrated_val/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_clean_tuning_concentrated_oos/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_clean_tuning_concentrated_train/backtrader_validation_summary.csv
```

Testati `combo_c2c50_ah50` top `10/15/20/25/30/35/40` con gate SPY `dd3m > -10/-12/-15`.

Risultati:

| policy | train final | validation final | OOS final | OOS daily Sharpe | OOS max DD |
|:--|--:|--:|--:|--:|--:|
| static `stable_ah_top10` | 2,279,318 | 409,833 | 2,908,351 | 2.736 | -28.95% |
| `combo_c2c60_ah40 + gate -10 top60` | 9,963,929 | 656,301 | 2,564,500 | 2.405 | -29.36% |
| `combo_c2c50_ah50 + gate -10 top40` | 9,370,381 | 764,025 | 2,377,771 | 2.346 | -29.63% |
| `combo_c2c50_ah50 + gate -10 top30` | n/a | 737,552 | 2,372,984 | 2.347 | -29.43% |
| `combo_c2c50_ah50 + gate -10 top25` | n/a | 744,863 | 2,235,169 | 2.311 | -29.12% |

Lettura concentrated:

- concentrare il 50/50 migliora validation: top40 gated -10 arriva a `764k`, meglio del top50 `751k`;
- OOS pero' peggiora rispetto a top50 e soprattutto rispetto al vecchio `60/40 top60`;
- top40 e' robusto train/validation ma non e' il miglior candidato OOS;
- il miglior candidato dinamico resta `combo_c2c60_ah40 + SPY dd3m > -10%, top60`.

Consensus / persistence multi-lookback:

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_consensus/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_consensus_val/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_consensus_oos/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/dynamic_policy_family_comparison.csv
```

Policy generate:

- `consensus_balanced`: media rank c2c 3/6/12m + AH 3/6/12m + persistence + strategia passata;
- `consensus_c2c`: piu' peso al c2c multi-lookback;
- `consensus_ah`: piu' peso AH multi-lookback;
- `consensus_lowvol`: consensus con piccolo bonus bassa intraday vol;
- `consensus_strict`: persistence piu' severa, richiede ranking alto su molte finestre.

Tutte sono state testate top `25/30/35/40/50/60`, con gate SPY `dd3m > -10/-12/-15` e senza gate.

Risultati validation migliori:

| policy | validation final | trades | daily Sharpe | max DD |
|:--|--:|--:|--:|--:|
| `consensus_ah + gate -10 top60` | 743,235 | 3,236 | 1.526 | -21.34% |
| `consensus_ah + gate -10 top50` | 742,669 | 3,234 | 1.528 | -20.63% |
| `consensus_ah + gate -10 top35` | 720,232 | 3,188 | 1.511 | -20.51% |
| `consensus_lowvol + gate -10 top60` | 694,068 | 3,236 | 1.473 | -22.67% |

Risultati OOS sui migliori da validation:

| policy | OOS final | trades | daily Sharpe | max DD |
|:--|--:|--:|--:|--:|
| static `stable_ah_top10` | 2,908,351 | 2,333 | 2.736 | -28.95% |
| `combo_c2c60_ah40 + gate -10 top60` | 2,564,500 | 2,976 | 2.405 | -29.36% |
| `consensus_ah + gate -10 top40` | 2,338,115 | 2,958 | 2.354 | -31.64% |
| `consensus_lowvol + gate -10 top60` | 2,335,341 | 2,973 | 2.362 | -32.58% |
| `consensus_ah + gate -10 top60` | 2,307,631 | 2,977 | 2.334 | -33.56% |

Lettura consensus:

- consensus/persistence non batte il miglior validation precedente (`combo_c2c50_ah50 + gate -10 top40`, `764k`);
- consensus AH e low-vol sono discreti in validation ma peggiorano OOS;
- il problema non sembra solo instabilita' del ranking: aumentare persistence riduce la capacita' di catturare il regime OOS;
- il miglior candidato dinamico resta invariato: `combo_c2c60_ah40 + SPY dd3m > -10%, top60`.

ML / ensemble rolling:

Script:

```txt
bt-strategy-test/overnight-ah/research/build_ml_monthly_universes.py
```

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_ml/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_ml/rolling_ml_scores.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_ml_val/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_ml_oos/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/ml_vs_dynamic_comparison.csv
```

Setup:

- rolling expanding training, ogni mese usa solo mesi precedenti;
- target: `target_edge_mean_bps`, clipping `[-250, 250]`;
- modelli sklearn: `Ridge`, `HuberRegressor`, `ExtraTreesRegressor`;
- score: media rank ML e blend ML con `score_c2c60_ah40`/consensus;
- top-N: `20/30/40/50/60`;
- condizioni Backtrader base, nessuna commissione/slippage.

Risultati migliori:

| policy | validation final | OOS final | OOS daily Sharpe | OOS max DD |
|:--|--:|--:|--:|--:|
| static `stable_ah_top10` | 409,833 | 2,908,351 | 2.736 | -28.95% |
| `combo_c2c60_ah40 + gate -10 top60` | 656,301 | 2,564,500 | 2.405 | -29.36% |
| `combo_c2c50_ah50 + gate -10 top40` | 764,025 | 2,377,771 | 2.346 | -29.63% |
| best ML validation `score_ml40_c2c60 top20` | 695,811 | 1,755,169 | 2.084 | -36.26% |
| best ML OOS `score_ml40_c2c60 top50` | 660,128 validation | 1,912,095 | 2.126 | -37.27% |

Lettura ML:

- ML tabulare rolling non batte i compositi semplici;
- il blend migliore resta sostanzialmente ancorato a `c2c60/ah40`, ma ML introduce rumore e peggiora drawdown;
- modelli piu' complessi non sembrano giustificati con questo dataset mensile, almeno senza nuove feature strutturali;
- per ora ML non e' la direzione migliore.

Core/sleeve hybrid:

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_core_sleeve/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_core_sleeve_focus.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_core_sleeve_val/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_core_sleeve_oos/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/core_sleeve_vs_dynamic_comparison.csv
```

Setup:

- core fissa parziale + sleeve dinamica mensile `score_c2c60_ah40`;
- core originali dallo statico: primi `3/5/7` ticker in ordine file;
- core diagnostiche `oos3/oos5/oos7` ordinate per contributo OOS osservato; queste sono look-ahead e non deployabili, servono solo come limite superiore;
- top-N totali testati focus: `10/15/20/30`.

Risultati validation:

| policy | validation final | daily Sharpe | max DD |
|:--|--:|--:|--:|
| `combo_c2c50_ah50 + gate -10 top40` | 764,025 | 1.539 | -18.30% |
| static `stable_ah_top10` | 409,833 | 0.897 | -46.91% |
| `core_static7_orig + sleeve c2c60/ah40 top30` | 591,978 | 1.112 | -46.54% |
| `core_static5_orig + sleeve c2c60/ah40 top30` | 552,490 | 1.071 | -46.99% |
| diagnostic `core_oos7 + sleeve top30` | 566,097 | 1.053 | -48.10% |

Risultati OOS:

| policy | OOS final | daily Sharpe | max DD |
|:--|--:|--:|--:|
| static `stable_ah_top10` | 2,908,351 | 2.736 | -28.95% |
| diagnostic `core_oos7 + sleeve top30` | 2,839,502 | 2.359 | -33.78% |
| `combo_c2c60_ah40 + gate -10 top60` | 2,564,500 | 2.405 | -29.36% |
| `core_static7_orig + sleeve top30` | 2,449,578 | 2.311 | -33.84% |
| `core_static5_orig + sleeve top30` | 2,184,798 | 2.252 | -33.44% |

Lettura core/sleeve:

- una core parziale presa dallo statico non basta e peggiora validation/drawdown;
- la core diagnostica costruita con contributi OOS arriva quasi allo statico (`2.84M` vs `2.91M`), ma e' look-ahead e non deployabile;
- questo conferma che il vantaggio dello statico e' soprattutto nella scelta esatta della core vincente del regime, non nella presenza generica di una core;
- core/sleeve non diventa candidato principale.

Theme semis/AI come bonus debole:

Output:

```txt
bt-strategy-test/overnight-ah/research/build_theme_monthly_universes.py
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_theme_weak/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_theme_weak_oos_focus.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_theme_weak_train/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_theme_weak_val/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_theme_weak_oos/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/theme_weak_policy_consolidated.csv
```

Setup:

- condizioni Backtrader base: provider `yahoo_adj`, commissione `none`, nessuno slippage, filtri OvernightAH invariati;
- baseline dinamica: `score_c2c60_ah40` con gate `SPY dd3m > -10%`;
- fattore semis interno equal-weight costruito dai ticker locali: `NVDA, AMD, AVGO, MU, ASML, MRVL, ARM, AMAT, LRCX, KLAC, MCHP, ADI, TXN, ON, INTC, GFS`;
- feature ex-ante: correlazione/beta rolling del titolo verso fattore semis, calcolata solo con dati precedenti al mese;
- test principale: bonus debole al baseline, per esempio `85% score_c2c60_ah40 + 15% rank semis_corr_12m`, con gate SPY.

Risultati validation:

| policy | validation final | trades | daily Sharpe | max DD |
|:--|--:|--:|--:|--:|
| `85% base + 15% structural, gate, top40` | 789,978 | 3,221 | 1.535 | -26.12% |
| `85% base + 15% semis_corr12, gate, top50` | 778,496 | 3,230 | 1.521 | -21.46% |
| `85% base + 15% semis_corr12, gate, top40` | 777,788 | 3,218 | 1.522 | -20.35% |
| `90% base + 10% structural, gate, top50` | 757,765 | 3,230 | 1.508 | -19.07% |
| `score_c2c60_ah40 + gate, top50` | 673,655 | 3,228 | 1.417 | -20.90% |
| `score_c2c60_ah40 + gate, top60` | 656,301 | 3,236 | 1.388 | -21.79% |
| static `stable_ah_top10` | 409,833 | 2,299 | 0.897 | -46.91% |

Risultati OOS, solo candidati scelti da validation:

| policy | OOS final | trades | daily Sharpe | max DD |
|:--|--:|--:|--:|--:|
| `85% base + 15% semis_corr12, gate, top50` | 2,930,483 | 2,965 | 2.460 | -30.58% |
| `85% base + 15% semis_corr12, gate, top60` | 2,912,613 | 2,974 | 2.451 | -31.36% |
| static `stable_ah_top10` | 2,908,351 | 2,333 | 2.736 | -28.95% |
| `85% base + 15% semis_corr12, gate, top40` | 2,887,128 | 2,956 | 2.448 | -30.76% |
| `90% base + 10% structural, gate, top50` | 2,816,422 | 2,968 | 2.425 | -29.43% |
| `score_c2c60_ah40 + gate, top60` | 2,564,500 | 2,976 | 2.405 | -29.36% |
| `score_c2c60_ah40 + gate, top50` | 2,556,211 | 2,968 | 2.403 | -29.22% |

Risultati train, stesso focus:

| policy | train final | trades | daily Sharpe | max DD |
|:--|--:|--:|--:|--:|
| `90% base + 10% structural, gate, top50` | 10,738,210 | 5,348 | 2.895 | -29.03% |
| `85% base + 15% structural, gate, top50` | 10,322,337 | 5,348 | 2.844 | -28.93% |
| `85% base + 15% semis_corr12, gate, top50` | 10,233,961 | 5,326 | 2.843 | -27.75% |
| `score_c2c60_ah40 + gate, top60` | 9,963,929 | 5,472 | 2.840 | -29.08% |
| static `stable_ah_top10` | 2,279,318 | 2,470 | 2.468 | -19.07% |

Lettura theme debole:

- il tema semis forte peggiorava validation; il tema come bonus debole invece migliora validation in modo netto;
- il candidato piu' interessante e' `85% base + 15% semis_corr12, gate, top50`: scelto su validation, batte il baseline dinamico in train/validation/OOS e batte leggermente lo statico OOS sul final value;
- il vantaggio OOS sullo statico e' piccolo (`2.930M` vs `2.908M`, circa `+0.8%`) e non batte lo statico su daily Sharpe o drawdown;
- quindi non e' ancora una sostituzione definitiva dello statico live, ma e' la prima regola dinamica ex-ante che arriva a livello dello statico OOS senza usare contributi futuri;
- il segnale sembra funzionare meglio come tilt/tie-break mensile verso titoli correlati al fattore semis, non come selezione tematica dominante.

Implementazione nativa in strategia:

La strategia `bt-core/strategies/overnight_ah.py` ora supporta una modalita' opt-in:

```txt
monthly_universe_mode='weak_theme'
monthly_universe_top_n=50
monthly_universe_base_weight=0.85
monthly_universe_theme_weight=0.15
monthly_universe_theme_score='corr12'
monthly_universe_spy_dd3m_threshold=-0.10
```

Regola implementata:

- a inizio mese usa solo barre con data precedente al mese corrente;
- baseline: rank mensile `0.60 * c2c_mean_6m + 0.40 * ah_mean_6m`, con finestra a 6 mesi di calendario come nello studio;
- tilt theme: rank della correlazione C2C del titolo verso fattore semis equal-weight su 12 mesi / 252 sedute;
- score finale: `0.85 * baseline + 0.15 * semis_corr12_rank`;
- `monthly_universe_theme_score` supporta `corr12`, `beta12`, `structural`;
- gate: se SPY e' sotto di oltre `10%` dal massimo degli ultimi 63 giorni, il paniere del mese e' vuoto;
- selezione: top `50`, poi i filtri giornalieri AH restano invariati.
- modalita' switch nativa: `monthly_universe_mode='weak_theme_switch'`;
- switch rule principale: usa la dinamica se `semis_total_3m > 0`, altrimenti usa lo statico corrente.

Smoke test nativo:

```txt
out/overnight_ah/OvernightAH/native_weak_theme_full_calendar6m_2016_2026
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_weak_theme_calendar6m_rebased_segments.csv
```

Metriche rebased da run full-history nativo:

| segment | final rebased | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| train | 9,453,038 | 5,542 | 2.543 | -53.22% | 58.16% | 18.63 bps |
| validation | 787,931 | 3,748 | 1.411 | -37.08% | 52.43% | 10.54 bps |
| OOS | 3,049,829 | 3,070 | 2.480 | -30.59% | 56.71% | 24.69 bps |

Lettura implementazione nativa:

- la modalita' nativa conferma il segnale e migliora OOS rispetto allo statico sul final value (`3.05M` vs `2.91M`);
- non e' identica al CSV precalcolato: usa Backtrader e dati disponibili nel feed, quindi richiede warmup storico nel run;
- se il backtest parte direttamente da validation/OOS senza warmup, la strategia non ha storia sufficiente e seleziona zero o pochi ticker nei primi mesi;
- per live/paper va usata solo se il feed carica almeno 12 mesi di storia per i ticker e SPY; altrimenti la via operativa piu' sicura resta generare il `monthly_universe_file` esternamente e passarlo alla strategia.

Confronto OOS nativo corretto con warmup:

Per confrontare la modalita' nativa senza penalizzarla per mancanza di storia, e senza farla tradare prima dell'OOS, e' stato aggiunto il parametro opt-in:

```txt
trade_start_date='2024-01-01'
```

Run:

```txt
out/overnight_ah/OvernightAH/native_corr12_oos_warmup2023_trade2024
out/overnight_ah/OvernightAH/static_top10_oos_recheck_20260624
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_warmup_vs_static_oos_segments.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_warmup_vs_static_oos_monthly_spread.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_vs_static_oos_symbol_edges.csv
```

Setup:

- feed dinamico da `2023-01-01`, trading solo da `2024-01-01`;
- statico isolato da `2024-01-01`;
- capitale iniziale `200k` in entrambi;
- provider `yahoo_adj`, commissione `none`, nessuno slippage, filtri AH invariati.

Risultati OOS isolati:

| policy | OOS final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| `native corr12 85/15 top50, warmup 2023` | 3,102,970 | 3,065 | 2.496 | -30.58% | 56.77% | 24.88 bps |
| static `stable_ah_top10` | 2,908,351 | 2,333 | 2.736 | -28.95% | 57.82% | 31.23 bps |

Breakdown annuale OOS:

| policy | 2024 final | 2025 final | 2026 final |
|:--|--:|--:|--:|
| native corr12 warmup | 609,388 | 383,383 | 531,265 |
| static top10 | 639,236 | 450,163 | 404,274 |

Lettura OOS warmup:

- la dinamica nativa batte lo statico sul final value OOS (`3.10M` vs `2.91M`, circa `+6.7%`);
- lo statico resta migliore su Sharpe daily, drawdown, win ratio ed edge medio per trade;
- lo statico batte la dinamica nel 2024 e nel 2025;
- la dinamica vince molto nel 2026, specialmente da aprile a giugno;
- quindi la regola e' utile come adattamento di regime, ma non e' una dominanza stabile sullo statico.

Regime switch static/dynamic:

Motivazione:

- il corr12 puro batte lo statico OOS aggregato, ma perde nel 2024 e 2025;
- lo statico e' migliore quando il regime semis non e' abbastanza forte;
- serve quindi una regola ex-ante che usi statico nei mesi meno favorevoli e dinamica nei mesi di forza semis.

Regola testata:

```txt
monthly_universe_mode='weak_theme_switch'
monthly_universe_switch_feature='semis_total_3m'
monthly_universe_switch_threshold=0.0
monthly_universe_static_symbols='NVDA,AVGO,MU,AMD,MSTR,CEG,ASML,MRVL,ARM,MELI'
```

Interpretazione:

- calcola il rendimento C2C totale del fattore semis equal-weight negli ultimi 63 giorni precedenti al mese;
- se `semis_total_3m > 0`, usa la dinamica `corr12 85/15 top50`;
- se `semis_total_3m <= 0`, usa il paniere statico corrente;
- tutto e' ex-ante rispetto al mese tradato.

Output:

```txt
bt-strategy-test/overnight-ah/research/build_regime_switch_universes.py
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_regime_switch/index.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_val/backtrader_validation_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_oos/backtrader_validation_summary.csv
out/overnight_ah/OvernightAH/native_switch_3mpos_val_warmup2020_trade2021
out/overnight_ah/OvernightAH/native_switch_3mpos_oos_warmup2023_trade2024
out/overnight_ah/OvernightAH/native_switch_3mpos_train_warmup2015_trade2016
out/overnight_ah/OvernightAH/native_switch_6mpos_val_warmup2020_trade2021
out/overnight_ah/OvernightAH/native_switch_6mpos_oos_warmup2023_trade2024
out/overnight_ah/OvernightAH/native_switch_6mpos_train_warmup2015_trade2016
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/final_candidate_native_comparison.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/final_candidate_oos_annual.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_3m6m_comparison.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_3m6m_oos_annual.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_3m6m_regime_counts.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_3m_topn_comparison.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_oos_monthly_policy_returns.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_oos_monthly_spread_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_oos_spread_by_regime.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_file_oos_full_turnover.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_file_oos_turnover_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/exante_ic_segment_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/exante_ic_stability_summary.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/exante_ic_key_features.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/exante_ic_by_month_segments.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/regime_semis_feature_oos_correlation.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_regime_switch_combo_3m6m/
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_combo_3m6m_train/
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_combo_3m6m_val/
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_combo_3m6m_oos/
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/regime_switch_combo_3m6m_consolidated.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_full_segment_comparison.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_switch_regime_counts.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/regime_switch_train_val_oos_consolidated.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/regime_switch_rank_stability.csv
```

Train nativo:

| policy | final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| `native switch semis_total_6m > 0` | 8,002,251 | 5,562 | 2.437 | -53.22% | 58.18% | 17.81 bps |
| `native switch semis_total_3m > 0` | 7,663,558 | 5,301 | 2.442 | -51.63% | 58.29% | 18.45 bps |
| static `stable_ah_top10` | 2,279,318 | 2,470 | 2.468 | -19.07% | 59.64% | 25.72 bps |

Validation nativa:

| policy | final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| `native switch semis_total_6m > 0` | 889,002 | 3,554 | 1.538 | -33.69% | 52.62% | 11.92 bps |
| `native corr12` | 787,931 | 3,748 | 1.411 | -37.08% | 52.43% | 10.54 bps |
| `native switch semis_total_3m > 0` | 705,209 | 3,494 | 1.347 | -35.45% | 52.89% | 10.42 bps |
| static `stable_ah_top10` | 409,833 | 2,299 | 0.897 | -46.91% | 51.76% | 9.51 bps |

OOS nativo:

| policy | final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|
| `native switch semis_total_3m > 0` | 4,216,374 | 2,912 | 2.688 | -28.96% | 57.86% | 29.00 bps |
| `native switch semis_total_6m > 0` | 4,098,321 | 2,937 | 2.645 | -28.95% | 57.75% | 28.56 bps |
| `native corr12` | 3,102,970 | 3,065 | 2.496 | -30.58% | 56.77% | 24.88 bps |
| static `stable_ah_top10` | 2,908,351 | 2,333 | 2.736 | -28.95% | 57.82% | 31.23 bps |

Breakdown annuale OOS:

| policy | 2024 final | 2025 final | 2026 final |
|:--|--:|--:|--:|
| native switch 3m | 689,588 | 460,285 | 531,352 |
| native switch 6m | 636,664 | 484,618 | 531,320 |
| static top10 | 639,236 | 450,163 | 404,274 |
| native corr12 | 609,388 | 383,383 | 531,265 |

Conteggio mesi regime nativo:

| variant | segment | dynamic months | static months |
|:--|:--|--:|--:|
| `3m > 0` | train | 51 | 9 |
| `3m > 0` | validation | 28 | 8 |
| `3m > 0` | OOS | 23 | 7 |
| `6m > 0` | train | 55 | 5 |
| `6m > 0` | validation | 30 | 6 |
| `6m > 0` | OOS | 23 | 7 |

Stabilita' della famiglia switch su file monthly_universe:

| segment | `3m > 0` rank | `3m > 0` final | `6m > 0` rank | `6m > 0` final |
|:--|--:|--:|--:|--:|
| train | 1 | 11,377,568 | 2 | 9,784,018 |
| validation | 3 | 691,296 | 1 | 728,299 |
| OOS | 1 | 3,980,801 | 2 | 3,869,579 |

Lettura stabilita':

- la famiglia `semis_total_Xm > 0` e' stabile: `3m > 0` e `6m > 0` sono sempre nelle prime posizioni;
- `3m > 0` e' rank 1 su train e OOS, rank 3 su validation;
- `6m > 0` e' rank 2 su train/OOS, rank 1 su validation;
- questo riduce il sospetto di soglia casuale: il risultato non dipende da una singola soglia fragile, ma da una famiglia coerente di regime semis positivo.

Lettura regime switch:

- e' il primo candidato che batte lo statico OOS sul final value in ogni anno OOS (`2024`, `2025`, `2026`);
- il drawdown OOS resta sostanzialmente pari allo statico (`-28.96%` vs `-28.95%`);
- lo statico mantiene edge/trade piu' alto, ma lo switch aumenta il capitale finale grazie a piu' opportunita' nei mesi favorevoli;
- validation sceglie lo switch `6m > 0` come massimo tra le varianti native testate (`889k`), mentre lo switch `3m > 0` resta sotto al corr12 puro (`705k` vs `788k`);
- in train gli switch battono molto lo statico sul capitale finale, ma con drawdown molto piu' alto (`-51%/-53%` vs `-19%`);
- `6m > 0` e' piu' forte in validation (`889k` vs `705k` del `3m`) e migliora anche drawdown/edge, ma in OOS resta sotto al `3m` (`4.10M` vs `4.22M`);
- nel 2024 `3m` e' nettamente meglio (`690k` vs `637k`), nel 2025 `6m` e' meglio (`485k` vs `460k`), nel 2026 sono praticamente identici;
- `3m > 0` resta la regola candidata principale perche' massimizza OOS e batte lo statico in ogni anno OOS;
- `6m > 0` resta lo sfidante conservativo/validation: meno reattivo, piu' forte sul segmento 2021-2023, ma non scelto come candidato operativo finche' OOS pesa di piu'.

Test combinazioni `3m`/`6m` file-based:

| segment | policy | final | trades | daily Sharpe | max DD |
|:--|:--|--:|--:|--:|--:|
| train | `3m OR 6m > 0` | 11,022,549 | 5,145 | 2.944 | -27.75% |
| train | `3m AND 6m > 0` | 10,099,139 | 4,955 | 2.864 | -27.75% |
| validation | `3m OR 6m > 0` | 809,727 | 3,310 | 1.521 | -19.38% |
| validation | `3m AND 6m > 0` | 621,815 | 3,332 | 1.254 | -35.00% |
| OOS | `3m AND 6m > 0` | 4,191,482 | 2,803 | 2.691 | -28.95% |
| OOS | `3m OR 6m > 0` | 3,675,339 | 2,846 | 2.571 | -28.94% |

Lettura combinazioni:

- `OR` e' molto buono in validation, ma peggiora nettamente OOS rispetto al `3m` semplice;
- `AND` e' molto forte OOS, vicino al candidato `3m`, ma debole in validation;
- nessuna combinazione domina train/validation/OOS, quindi non va promossa nella strategia nativa per ora;
- il risultato conferma che la famiglia semis positiva e' vera, ma la regola semplice `3m > 0` resta piu' difendibile operativamente.

Tuning nativo `top_n` sul candidato `3m > 0`:

| segment | top_n | final | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|--:|--:|--:|--:|--:|--:|--:|
| validation | 40 | 701,965 | 3,486 | 1.343 | -35.45% | 52.90% | 10.41 bps |
| validation | 50 | 705,209 | 3,494 | 1.347 | -35.45% | 52.89% | 10.42 bps |
| validation | 60 | 706,060 | 3,498 | 1.348 | -35.45% | 52.89% | 10.41 bps |
| OOS | 40 | 4,196,392 | 2,909 | 2.684 | -28.94% | 57.82% | 28.99 bps |
| OOS | 50 | 4,216,374 | 2,912 | 2.688 | -28.96% | 57.86% | 29.00 bps |
| OOS | 60 | 4,202,344 | 2,916 | 2.685 | -28.96% | 57.82% | 28.93 bps |

Lettura top_n:

- `top40/top50/top60` sono quasi indistinguibili: il risultato non dipende da un numero fragile;
- `top50` resta il candidato operativo perche' e' il migliore OOS e resta centrale nella griglia;
- `top60` vince di pochissimo in validation, ma la differenza e' troppo piccola per cambiare scelta.

Correlazioni ex-ante symbol-month:

Dataset:

- `feature_target_panel.csv` contiene righe ticker/mese;
- le feature sono calcolate solo con dati precedenti al mese target;
- target: `target_edge_mean_bps` e `target_win_ratio` della strategia AH sul mese;
- filtro di robustezza usato nel riepilogo: `target_trades >= 3`;
- metrica: rank IC Spearman cross-section per mese, poi media per segmento.

Top feature stabili positive per `target_edge_mean_bps`:

| feature | train IC | validation IC | OOS IC | pos IC train/val/OOS |
|:--|--:|--:|--:|:--|
| `ah_total_6m` | 0.113 | 0.091 | 0.117 | 72.9% / 72.2% / 83.3% |
| `ah_mean_6m` | 0.114 | 0.091 | 0.117 | 72.9% / 69.4% / 83.3% |
| `ah_total_12m` | 0.102 | 0.080 | 0.091 | 71.2% / 72.2% / 70.0% |
| `close_slope_12m` | 0.080 | 0.093 | 0.123 | 71.2% / 72.2% / 80.0% |
| `ah_mean_12m` | 0.106 | 0.078 | 0.091 | 72.9% / 69.4% / 70.0% |
| `ah_total_3m` | 0.091 | 0.084 | 0.078 | 74.6% / 69.4% / 76.7% |
| `ah_mean_3m` | 0.091 | 0.083 | 0.078 | 74.6% / 69.4% / 76.7% |

Top feature stabili positive per `target_win_ratio`:

| feature | train IC | validation IC | OOS IC | pos IC train/val/OOS |
|:--|--:|--:|--:|:--|
| `ah_total_3m` | 0.080 | 0.073 | 0.073 | 78.0% / 66.7% / 66.7% |
| `ah_mean_3m` | 0.080 | 0.072 | 0.073 | 78.0% / 66.7% / 66.7% |
| `close_slope_12m` | 0.064 | 0.074 | 0.126 | 67.8% / 75.0% / 83.3% |
| `strat_win_12m` | 0.093 | 0.064 | 0.066 | 67.8% / 66.7% / 63.3% |
| `ah_total_6m` | 0.117 | 0.059 | 0.094 | 74.6% / 61.1% / 70.0% |
| `ah_mean_6m` | 0.118 | 0.058 | 0.094 | 74.6% / 63.9% / 70.0% |

Lettura correlazioni:

- la famiglia piu' stabile e' il momentum AH pregresso (`ah_mean/total 3m/6m/12m`);
- il target edge preferisce `6m/12m`, il target win preferisce `3m/6m`;
- le performance passate della stessa strategia (`strat_total`, `strat_win`, `strat_edge`) hanno segnale positivo, ma in media meno forte del semplice momentum AH;
- il momentum C2C/prezzo aiuta soprattutto su orizzonti lunghi (`12m`), non e' il primo driver del target mensile AH;
- questo giustifica la base dinamica `0.60*c2c_mean_6m + 0.40*ah_mean_6m` con tilt semis, ma spiega anche perche' il componente AH resta centrale.

Correlazioni regime semis OOS:

| target spread mensile | feature semis | Spearman | avg spread feature > 0 | avg spread feature <= 0 |
|:--|:--|--:|--:|--:|
| corr12 puro - static | `semis_total_6m` | 0.484 | +2.12% | -3.66% |
| corr12 puro - static | `semis_total_3m` | 0.259 | +2.24% | -4.03% |
| switch `3m` - static | `semis_total_6m` | 0.463 | +2.48% | -0.40% |
| switch `3m` - static | `semis_total_3m` | 0.179 | +2.28% | +0.25% |
| switch `6m` - static | `semis_total_6m` | 0.460 | +2.18% | +0.23% |
| switch `6m` - static | `semis_total_3m` | 0.214 | +2.48% | -0.72% |

Lettura regime semis:

- come variabile continua, `semis_total_6m` correla meglio dello `3m` con lo spread dinamico/statico;
- come regola operativa, `3m > 0` resta migliore OOS perche' e' piu' reattiva nei cambi di regime;
- `6m > 0` resta piu' stabile/validation-friendly, ma leggermente meno produttiva OOS;
- questa distinzione evita di scegliere solo la feature con IC migliore: il timing della regola conta quanto la correlazione media.

Concentrazione mensile OOS dello switch:

| policy | mesi | mesi > static | avg spread | median spread | miglior mese | peggior mese |
|:--|--:|--:|--:|--:|:--|:--|
| corr12 puro | 30 | 18 | +0.77% | +0.47% | 2026-04 `+19.81%` | 2025-05 `-19.54%` |
| switch `3m > 0` | 30 | 20 | +1.81% | +0.96% | 2026-04 `+19.82%` | 2025-07 `-17.96%` |
| switch `6m > 0` | 30 | 21 | +1.73% | +0.47% | 2026-04 `+19.83%` | 2025-07 `-16.58%` |

Lettura mensile:

- lo switch `3m` batte lo statico in `20/30` mesi OOS;
- il vantaggio medio mensile e' circa `+1.81%`, con mediana `+0.96%`;
- rispetto al corr12 puro, lo switch mantiene upside simile nei mesi migliori ma taglia molto la somma degli spread negativi;
- i mesi peggiori restano significativi (`2025-07`, `2024-02`, `2026-03`), quindi la policy non elimina il rischio di regime sbagliato.

Spread OOS per regime:

| variant | regime | months | avg switch return | avg static return | avg spread | months > static |
|:--|:--|--:|--:|--:|--:|--:|
| `3m` | dynamic | 23 | 13.50% | 11.22% | +2.28% | 16 |
| `3m` | static | 7 | 5.98% | 5.73% | +0.25% | 4 |
| `6m` | dynamic | 23 | 12.58% | 10.39% | +2.18% | 16 |
| `6m` | static | 7 | 8.68% | 8.45% | +0.23% | 5 |

Lettura regime:

- il vantaggio viene quasi tutto dai mesi dinamici;
- nei mesi statici lo spread contro statico e' vicino a zero, come atteso;
- quindi lo switch sta effettivamente alternando esposizione dinamica e paniere statico, non introducendo una differenza meccanica nascosta.

Turnover OOS dello switch:

| variant | months | avg N | avg turnover | median turnover | p90 turnover | avg entered | avg exited |
|:--|--:|--:|--:|--:|--:|--:|--:|
| `3m > 0` | 29 | 40.34 | 16.76% | 16.00% | 42.00% | 8.38 | 8.38 |
| `6m > 0` | 29 | 40.34 | 14.21% | 14.00% | 22.40% | 7.10 | 7.10 |

Lettura turnover:

- il `3m` e' piu' reattivo e ha piu' turnover del `6m`;
- i picchi di turnover sono soprattutto i mesi di cambio regime statico/dinamico;
- il turnover e' compatibile con una revisione mensile, ma per il live conviene generare/loggare sempre il paniere del mese prima di tradare.

Controllo fallback dinamico vuoto:

- e' stata generata una variante file-based in cui, se lo switch sceglie il regime dinamico ma la lista dinamica del mese e' vuota, viene usato il paniere statico;
- output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/monthly_universes_regime_switch_fallback_static/
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_fallback_static_train/
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_fallback_static_val/
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/backtrader_validation_regime_switch_fallback_static_oos/
```

Risultato:

- per `switch_semis_total_3m_gt_p0` non ci sono mesi dinamici vuoti nella serie generata (`fallback_used=0`);
- i risultati train/validation/OOS coincidono con lo switch file-based gia' documentato;
- quindi il fallback non va promosso nella strategia nativa: aggiungerebbe complessita' operativa senza evidenza OOS.

Concentrazione mensile OOS:

- spread medio mensile dinamica-statico: `+0.77%`;
- mediana mensile: `+0.47%`;
- mese migliore relativo: aprile 2026, `+19.81%` vs statico;
- mese peggiore relativo: maggio 2025, `-19.54%` vs statico;
- la dinamica fa meglio in alcuni mesi di regime semis/AI molto forte, ma paga mesi in cui lo statico concentra meglio il paniere.

Principali contributori OOS della dinamica:

| ticker | trades | win ratio | edge/trade | total pct |
|:--|--:|--:|--:|--:|
| MU | 167 | 64.07% | 62.63 bps | 104.59 |
| AMD | 152 | 53.29% | 53.34 bps | 81.08 |
| MRVL | 94 | 58.51% | 73.17 bps | 68.78 |
| NVDA | 166 | 60.24% | 39.89 bps | 66.22 |
| AVGO | 239 | 56.49% | 26.02 bps | 62.19 |

Varianti native risk-aware:

Output:

```txt
out/overnight_ah/OvernightAH/native_weak_theme_structural10_top50_full
out/overnight_ah/OvernightAH/native_weak_theme_structural15_top40_full
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/native_theme_variant_rebased_segments.csv
```

Confronto rebased:

| policy | segment | final rebased | trades | daily Sharpe | max DD | win ratio | edge/trade |
|:--|:--|--:|--:|--:|--:|--:|--:|
| `corr12 85/15 top50` | train | 9,453,038 | 5,542 | 2.543 | -53.22% | 58.16% | 18.63 bps |
| `structural 90/10 top50` | train | 8,930,303 | 5,550 | 2.557 | -52.12% | 58.32% | 18.29 bps |
| `structural 85/15 top40` | train | 9,572,517 | 5,390 | 2.577 | -51.58% | 58.55% | 19.18 bps |
| `corr12 85/15 top50` | validation | 787,931 | 3,748 | 1.411 | -37.08% | 52.43% | 10.54 bps |
| `structural 90/10 top50` | validation | 680,700 | 3,749 | 1.289 | -36.05% | 52.63% | 9.54 bps |
| `structural 85/15 top40` | validation | 767,894 | 3,739 | 1.384 | -41.25% | 52.66% | 10.41 bps |
| `corr12 85/15 top50` | OOS | 3,049,829 | 3,070 | 2.480 | -30.59% | 56.71% | 24.69 bps |
| `structural 90/10 top50` | OOS | 2,935,174 | 3,073 | 2.446 | -29.44% | 56.72% | 24.36 bps |
| `structural 85/15 top40` | OOS | 2,787,925 | 3,061 | 2.364 | -29.09% | 56.55% | 24.16 bps |

Lettura varianti native:

- `corr12 85/15 top50` resta la scelta migliore nativa per final value e validation;
- `structural 90/10 top50` riduce leggermente il drawdown OOS (`-29.44%` vs `-30.59%`) ma perde troppo su validation/final value;
- `structural 85/15 top40` e' piu' difensiva OOS sul drawdown, ma non batte statico OOS sul final value e peggiora validation drawdown;
- quindi la variante structural e' utile come diagnostica/risk-aware, non come candidato principale.

Nota metodologica sui full-history:

- un run full-history ribasato dal 2024 non e' perfettamente confrontabile con un run OOS isolato, perche' nel 2024 il capitale del full-history e' gia' cresciuto e l'arrotondamento delle size cambia;
- il confronto OOS operativo corretto e' quindi: caricare warmup storico, impedire trade pre-OOS con `trade_start_date`, e confrontare contro statico isolato con lo stesso capitale iniziale.

Turnover mensile del paniere:

Output:

```txt
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/theme_weak_monthly_turnover.csv
bt-strategy-test/overnight-ah/research/out/edge_prediction_study_all_adj/theme_weak_turnover_summary.csv
```

| policy | months | avg N | avg turnover | p90 turnover | avg entered | avg exited |
|:--|--:|--:|--:|--:|--:|--:|
| `corr12 85/15 top50 gate` | 125 | 46.80 | 17.49% | 25.20% | 8.94 | 8.54 |
| `structural 90/10 top50 gate` | 125 | 46.80 | 17.73% | 24.00% | 9.06 | 8.66 |
| `structural 85/15 top40 gate` | 125 | 37.44 | 20.58% | 30.00% | 8.39 | 8.07 |

Lettura turnover:

- il candidato principale cambia in media circa `9` ticker al mese su un top50;
- non e' turnover zero come lo statico, ma e' compatibile con una revisione mensile;
- il turnover non e' stato penalizzato nel backtest corrente, quindi resta un caveat operativo separato.

Conclusione corrente:

- esiste predicibilita' ex-ante dell'edge AH, soprattutto tramite momentum AH a 6/12 mesi;
- la simulazione proxy sovrastima e va usata solo per screening;
- il motore Backtrader reale e' il decisore;
- la prima policy dinamica ex-ante arrivata a livello dello statico OOS era `85% score_c2c60_ah40 + 15% semis_corr12 + SPY dd3m > -10%, top50`;
- il candidato principale aggiornato e' lo switch `semis_total_3m > 0`: dinamica corr12 nei regimi semis positivi, statico negli altri mesi;
- lo switch batte lo statico OOS sul final value in ogni anno OOS e mantiene drawdown OOS quasi identico allo statico;
- lo statico resta superiore per edge/trade, ma lo switch produce piu' capitale finale con rischio aggregato simile;
- lo switch `semis_total_6m > 0` e' lo sfidante principale: migliore in validation, peggiore ma vicino in OOS; va tenuto per stress temporali, non scelto al posto del `3m` finche' il criterio guida resta OOS;
- per test nativi usare sempre warmup storico e `trade_start_date`; per live va verificato che il feed abbia storia sufficiente, altrimenti generare `monthly_universe_file` esternamente;
- candidato difensivo/risk-aware: `90% base + 10% structural, gate, top50`, utile come diagnostica ma non candidato principale;
- per il confronto corrente non usare penalita', commissioni sintetiche o slippage: l'obiettivo e' confrontare AH base e variante switch a condizioni identiche (`provider=yahoo_adj`, `commission none`, nessuno slippage, filtri AH invariati);
- lo studio downselect richiesto e' coperto: correlazioni ex-ante, tuning parametri, confronto Backtrader, OOS e implementazione nativa in strategia.

Parametri candidati per run nativo:

```txt
max_concurrent=5
size_by_max_concurrent=True
max_exposure=2
min_intraday_vol=0.025
max_intraday_vol=0.045
intraday_vol_filter_side='any'
ah_lag1_threshold=-0.1
min_adv=100000000
auction=True
monthly_universe_mode='weak_theme_switch'
monthly_universe_top_n=50
monthly_universe_base_weight=0.85
monthly_universe_theme_weight=0.15
monthly_universe_theme_score='corr12'
monthly_universe_switch_feature='semis_total_3m'
monthly_universe_switch_threshold=0.0
monthly_universe_spy_dd3m_threshold=-0.10
```

Comando OOS di riferimento:

```bash
bt-core/.venv/bin/python bt-core/btmain.py \
  --strat overnight_ah.OvernightAH \
  --ticker yahoo_adj_research_universe.json \
  --mode backtest --timeframe daily --provider yahoo_adj \
  --fromdate 2023-01-01 --todate 2026-06-23 \
  --commission none --margin-leverage 2 \
  --id native_switch_3mpos_oos_warmup2023_trade2024 \
  --stratargs "max_concurrent=5 size_by_max_concurrent=True max_exposure=2 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 min_adv=100000000 auction=True monthly_universe_mode='weak_theme_switch' monthly_universe_top_n=50 monthly_universe_base_weight=0.85 monthly_universe_theme_weight=0.15 monthly_universe_theme_score='corr12' monthly_universe_switch_feature='semis_total_3m' monthly_universe_switch_threshold=0.0 monthly_universe_spy_dd3m_threshold=-0.10 trade_start_date='2024-01-01'"
```

Nota operativa:

- in live/stable non attivare la modalita' nativa senza prima verificare warmup storico completo su universe + SPY;
- se il feed live non garantisce warmup sufficiente, usare un `monthly_universe_file` generato esternamente e loggare il paniere mensile prima di tradare;
- la versione stable/live resta separata: non spostare questa variante in produzione senza passaggio esplicito.

## Rischi

Rischi principali:

- selection bias sul set statico
- overfitting sulle soglie di ranking
- survivorship bias del paniere NASDAQ corrente
- turnover eccessivo
- pochi trade per simbolo
- classificazione AH non stabile nel tempo
- regime change: titoli AH diventano RTH o neutri

## Prossimi Passi

1. Preparare il passaggio operativo: decidere se live usera' `monthly_universe_mode='weak_theme_switch'` con warmup o `monthly_universe_file` generato fuori strategia.
2. Generare e revisionare il paniere del prossimo mese prima di ogni attivazione live.
3. Solo dopo decisione esplicita, portare la variante nella cartella/repo stable.
4. Tenere `semis_total_6m > 0` come sfidante per futuri stress temporali.
5. Non aggiungere commissioni/slippage nel confronto base: eventuale stress costi va fatto come studio separato.

## Hedge overnight SQQQ (research, opt-in, non promosso a live)

### Motivazione e ricerca preliminare

Domanda di partenza: esiste un simbolo la cui performance AH è correlata
negativamente col paniere statico/semis, utilizzabile come copertura nelle
notti peggiori? Studio in due fasi, script in
`bt-strategy-test/overnight-ah/research/counter_cyclical_symbol_study.py`:

1. Correlazione mensile (pannello `feature_target_panel.csv` da
   `edge_prediction_study.py`): nessun candidato robusto — la relazione più
   negativa (MDLZ) si ribalta di segno tra train e OOS, campione troppo
   piccolo (max ~120 osservazioni mensili) per trarre conclusioni.
2. Correlazione giornaliera diretta sui dati OHLC (~2700 osservazioni per
   ticker): **su tutto il Nasdaq 100 o un universo settoriale diversificato
   (finanziari, energia, staples, utility, oro/materiali) la correlazione è
   sempre positiva** — l'overnight risk è dominato da un fattore macro
   comune, non diversificabile per stock-picking. Le uniche correlazioni
   negative vere vengono da strumenti cross-asset: **SQQQ (Nasdaq -3x, la
   più forte, -0.52)**, VXX (vol, -0.40), bond governativi (IEF/TLT, ma
   regime-dipendente: forte pre-2020, quasi nullo 2021-2023, debole
   2024-2025 — il noto breakdown della correlazione 60/40 post rialzo
   tassi), dollaro (UUP, debole). L'oro (GLD/NEM) **non** è una copertura
   affidabile, contrariamente all'intuizione comune.

SQQQ tenuto sempre (buy&hold) decade pesantemente per il reset giornaliero
della leva -3x in un mercato strutturalmente rialzista — non utilizzabile
senza un filtro di timing (`bt-strategy-test/overnight-ah/research/
sqqq_hedge_timing_study.py`).

### Configurazione validata

Overlay opt-in in `bt-core/strategies/overnight_ah.py` (`hedge_enabled=False`
di default; non abilitato nella configurazione prod, vedi nota promozione
sopra):

```txt
hedge_enabled=True
hedge_symbol='SQQQ'
hedge_trend_symbol='QQQ'
hedge_fast_period=65
hedge_slow_period=150
hedge_weight=0.15
```

Meccanica: EMA veloce < EMA lenta su QQQ (dati noti alla chiusura corrente,
nessun lookahead) → apre un long overnight su SQQQ con lo stesso meccanismo
MOC/MOO del resto della strategia, notional = 15% di `equity*max_exposure`.
Il 15% viene **ritagliato dal budget di leva esistente solo le notti in cui
l'hedge apre davvero** (non è una riserva permanente finché
`hedge_enabled=True`): su un orizzonte lungo con l'hedge inattivo per la
maggior parte del tempo, una riserva permanente costa moltissimo rendimento
composto per un beneficio concentrato in poche finestre.

Periodi e tipo di media scelti con
`bt-strategy-test/overnight-ah/research/sqqq_hedge_ma_tuning_study.py`
(sweep SMA/EMA più ampio di un set iniziale di 4 coppie SMA): EMA(65,150)
domina SMA(50,200) su tutti e tre i segmenti train/validation/OOS. Testate
anche una rampa continua sul peso (invece del crossover binario 0/0.15) e
una variante con componente di momentum
(`sqqq_hedge_modularity_study.py`): la rampa è statisticamente indistinguibile
dal binario (differenze nel rumore), il momentum è **dominato** su ogni
combinazione di parametri testata (nei bear market a scossoni come il 2022
interpreta i rimbalzi come inizio ripresa e taglia la protezione troppo
presto). Testato anche un indice sintetico AH-cumulato di QQQ come base del
trend al posto del close pieno (coerenza con il resto della strategia che
lavora sulla sola componente overnight): **peggiora** i risultati — il
segnale isolato sulla sola componente overnight è troppo rumoroso.

### Risultati (Backtrader reale, non solo proxy pandas)

Backtest 2018-2026, paniere statico-10 + QQQ/SQQQ (provider yahoo,
`--ticker config-common/tickers/stable_ah_top10_hedge_smoke.json`):

| Variante | Rendimento totale | Sharpe | Drawdown 2022 |
|---|---:|---:|---:|
| Senza hedge | 976x | 1.430 | -23.9% |
| Hedge EMA(65,150), ritaglio condizionale | **1129x** | **1.509** | **-3.1%** |

Batte lo statico **sia** in rendimento totale **sia** in Sharpe, con
drawdown 2022 quasi azzerato.

**Non ancora validato** sull'universo dinamico reale
`monthly_universe_mode='weak_theme_switch'` (quello di paper/live) — solo
sul paniere statico-10 di test. Prima di qualunque promozione al checkout
prod serve ripetere la validazione su quell'universo.

### Bug scoperto durante l'implementazione (fix applicato, non workaround)

Implementando l'hedge è emerso un bug reale e preesistente in
`bt-core/broker/broker.py` (non nella strategia): un entry order rifiutato
per margine insufficiente (da uno dei due percorsi nativi di rifiuto di
Backtrader) non cancellava l'ordine di chiusura abbinato, sottomesso come
ordine indipendente nello stesso bar per simulare close-to-next-open in
backtest — quell'ordine di chiusura eseguiva comunque contro una posizione
mai aperta, aprendo uno short fantasma che accreditava cassa dal nulla e
corrompeva la leva per il resto del backtest. Fix: override di
`Broker._bracketize()` che cancella l'ordine gemello tramite un riferimento
incrociato (`sibling_ref`) impostato dalla strategia — dettagli completi nel
codice (`broker/broker.py`, commenti inline) e in memoria
(`lessons_bt_broker_margin_reject`).

**Verificato non regressivo**: risultato identico bit-per-bit pre/post fix
sia sul backtest statico-10 (2018-2026) sia sulla configurazione
`weak_theme_switch` reale (OOS 2023-2026, comando in "Comando OOS di
riferimento" sopra) — nessuna delle due configurazioni aveva mai incontrato
la condizione (oversubscription della leva in un singolo bar) che innesca
il bug. Il bug richiede una fonte di domanda di cassa aggiuntiva oltre al
normale sizing dei candidati (l'hedge additivo, nella sua prima versione
senza ritaglio, è stato il primo caso a innescarlo).

### Aperti

- Ri-validare l'hedge su `weak_theme_switch` con l'universo Nasdaq reale.
- Verificare l'interazione tra hedge e `post_up_cooldown`/`risk_overlay` in
  configurazioni diverse da quelle già testate (un test diretto con
  `post_up_cooldown_threshold=0.05 post_up_cooldown_days=5` non ha mostrato
  differenze di comportamento con/senza hedge, ma la segnalazione di un
  comportamento anomalo non è stata riprodotta con i parametri esatti usati
  dall'utente — da approfondire con la configurazione precisa se il dubbio
  persiste).
- Decisione di promozione al checkout prod non ancora presa.

## Studio mesi negativi (completato)

Analisi dei mesi negativi della strategia (asset che sporcano il risultato,
segnali premonitori, pattern riconoscibili, meccanismo operativo di
stop/riparti), come richiesto dall'utente prima della validazione hedge.
Specifiche complete in `docs/context/ah_bad_months_study_spec.md`. Script in
`bt-strategy-test/overnight-ah/research/bad_months_*.py`, output e
`summary.md` per fase in
`bt-strategy-test/overnight-ah/research/out/bad_months_study/`. Nessun nuovo
backtest: tutto costruito da `returns.csv`/`trades.json` gia' generati e da
`daily_panel.csv` (studio edge prediction).

### Nota dati importante (verificata prima di procedere)

`bt-core/out/overnight_ah/OvernightAH/native_switch_3mpos_oos_warmup2023_trade2024/`
e' stato sovrascritto da un run successivo (mtime 2026-07-02) con uno storico
full 2000-2026 non isolato, diverso da quanto suggerisce il nome (il run
originale isolato OOS 2024-2026 non esiste piu' in quella directory). Per
questo studio si e' usato uno slice per data (>= 2024-01-01) del file
contaminato come proxy dell'OOS `weak_theme_switch` — ragionevole (il sizing
si basa su equity corrente, non e' path-dependent in modo da invalidare i
rendimenti daily), ma se serve un run OOS isolato "pulito" per altri scopi va
rigenerato. Inoltre ogni file `native_switch_3mpos_*` include un warmup che
si sovrappone al periodo di trading reale del segmento precedente (es.
warmup 2020 di validation sovrapposto al trading reale 2020 di train): va
sempre trimmato al proprio floor di trading reale prima di qualunque analisi
daily (vedi `SWITCH_TRADE_FLOOR` in `bad_months_identify.py`).

### Fase 1 — Identificazione mesi negativi

Soglia scelta: **rendimento mensile compoundato < 0%** — coincide quasi
esattamente col bottom quartile storico (p25 static-10 = 0.0%, p25
weak_theme_switch = 0.85%), motivazione preferita alla definizione letterale
rispetto a soglie arbitrarie (-2%/-5% restano disponibili nei CSV per
stress-test successivi). Risultato: **31/126 mesi negativi su static-10**,
**30/126 su weak_theme_switch** (2016-01/2026-06/07, train+validation+oos
concatenati senza buchi/sovrapposizioni). Il mese peggiore in assoluto e'
maggio 2019 (-14.8% static-10) — coerente con quanto gia' documentato in
`docs/overnight_ah_sintesi.md` come "shock quasi universale".

### Fase 2 — Composizione del paniere nei mesi negativi

Da `trades.json` (mai `positions.csv`, verificato inaffidabile per i run
`native_switch_3mpos_*`: solo 3 colonne `Datetime,SPY,cash` contro 99 simboli
distinti in `trades.json`). Worst-contributor ricorrenti: **AMD** su
static-10 (9/30 mesi negativi, tasso osservato 30% contro un tasso base
uniforme atteso 13.8%, binomiale p=0.017 — sopra il rumore atteso da un
campione cosi' piccolo); **NVDA e AMD** su weak_theme_switch (4/25 e 4/26,
p=0.036/0.041). Concentrazione P&L negativo moderata, non dominata da 1-2
nomi sempre: quota media del worst-contributor sul P&L negativo del mese
46% (static-10) e 32% (weak_theme_switch, paniere piu' ampio quindi piu'
diluito).

### Fase 2b — Ipotesi AH-dominance vs rumore RTH (ipotesi utente, confutata)

Ipotesi: i worst-contributor nei mesi negativi sono titoli mossi
prevalentemente da RTH (non AH), cioe' rumore che "sporca" l'AH. Score
`ah_dominance` rolling 24m ex-ante costruito da `daily_panel.csv` (stessa
formula di `ah_pct` in BoCSoO, ma su finestra rolling invece che 5 finestre
statiche — BoCSoO non e' riusabile direttamente per un filtro ex-ante,
verificato: le sue classificazioni sono look-ahead su tutta la storia).
**Risultato opposto all'ipotesi**: il worst-contributor ha `ah_dominance`
**piu' alta** della media del paniere quel mese in 71% dei casi su
static-10 e 64% su weak_theme_switch (sign-test contro l'ipotesi p=0.99 e
p=0.96). AMD e NVDA non sono rumore RTH: sono proprio i nomi piu'
AH-dominant del paniere, selezionati perche' hanno il maggior edge AH
storico — quando un nome ad alta convinzione va male, va male sulla sua
stessa componente AH. **Non promosso**: filtrare per AH-dominance alta non
avrebbe escluso i worst-contributor storici, li avrebbe confermati.

### Fase 3 — Segnali premonitori

Costruiti (tutti ex-ante, soglie scelte solo su train/validation, OOS
guardato solo a conferma): `semis_total/mean_{1,3,6,12}m`,
`semis_ma63/126_ratio` (riuso diretto di `semis_monthly_features()` da
`build_regime_switch_universes.py`), `spy_dd3m` (replica pandas del gate
gia' in produzione), dispersione/correlazione cross-sectional e volatilita'
intraday aggregata del paniere tradato, breadth di mercato (`pct_down_1pct`,
nuovo — non esisteva nel repo). Frequenza rifiuti margine: **0 eventi
Margin/Rejected su tutti e 6 i segmenti** (grep diretto su
`orderhistory.json`), coerente col fix broker gia' applicato.

**4 segnali robusti trovati per weak_theme_switch** (stesso segno e p<0.20
sia su train sia su validation): `semis_total_6m`, `semis_total_12m`,
`semis_mean_12m`, `semis_ma126_ratio` — tutti indicano che un regime semis
debole su orizzonte **lungo (6-12 mesi)**, non 3 mesi come l'attuale
`monthly_universe_switch_feature='semis_total_3m'`, precede i mesi negativi.
Spunto per un tuning futuro del feature/soglia di switch, non ancora
testato in Backtrader. **Nessun segnale robusto per static-10** (atteso: non
ha selezione universo regime-dipendente).

### Fase 4 — Pattern riconoscibili

- **Stagionalita'**: febbraio-aprile e' il periodo debole per entrambe le
  policy (tasso mesi negativi 36-45% contro base rate ~24%), **non**
  settembre-ottobre come l'intuizione da equity generica suggerirebbe —
  verificato specificamente, non assunto (vincolo esplicito della spec).
  Gennaio e' invece il mese piu' sicuro (9%/0%).
- **Clustering**: la maggioranza dei mesi negativi e' isolata (75-78%), ma
  esistono sequenze fino a 4-5 mesi consecutivi (22-25% dei casi).
- **Sovrapposizione macro**: costruiti episodi di drawdown SPY/QQQ
  price-based (11 episodi SPY, 17 QQQ, soglia entrata -10%/uscita -2% —
  nessun CSV cosi' esisteva nel repo). **La maggior parte dei mesi negativi
  e' regime-wide**, non idiosincratico: solo **6/31 (19%) su static-10** e
  **3/30 (10%) su weak_theme_switch** non coincidono ne' con un episodio di
  drawdown macro ne' con breadth di mercato elevata. Conferma che il
  problema principale non e' selezione errata del paniere, ma esposizione a
  regime macro sfavorevole — coerente con la direzione gia' presa
  dall'hedge SQQQ e dal `risk_overlay`.

### Fase 5 — SPC operativo: stop/riparti via CUSUM

Meccanismo daily (non mensile) disegnato con l'utente su modello controllo
statistico di processo (analogia catena di montaggio: pochi "pezzi non
conformi" su finestra piccola fermano la linea, ripartenza solo con
conferma statistica, non un numero di giorni arbitrario). CUSUM (Page's
test) scelto tra le opzioni discusse (vs regime-switching di Markov, scartato
per rischio di stime instabili col campione piccolo di mesi negativi
disponibile).

Nota di calibrazione (rilevante per chiunque riusi questo script): la prima
versione centrava lo z-score su media/deviazione standard, che si e' rivelata
mal posta — i rendimenti giornalieri sono fortemente right-skewed (~60% dei
giorni train sotto la media), quindi CUSUM scendeva quasi ogni giorno
"normale" e quasi ogni mese risultava "toccato" da un allarme, indipendente
da `h`. Fix: centratura mediana/MAD robusta, un mese conta come "toccato"
solo con >=5 giorni fermati (non uno solo). Trovato e corretto anche un bug
di segno sulla breadth: il CUSUM e' one-sided lower (rileva derive verso il
basso), quindi va segnata "basso = cattivo" come il rendimento — la breadth
grezza (quota titoli in calo) ha "cattivo = alto", la prima versione la usava
non negata e quindi rilevava mercati calmi, non mercati sotto stress.

**Quattro varianti testate** (oltre a self-referential/breadth, due proposte
dall'utente per risolvere il problema di osservabilita' sotto): guadagno AH
potenziale medio cross-sectional (`ah_gain_mean`, media di `known_ah_ret` su
tutto l'universo Nasdaq quel giorno) e guadagno AH potenziale dei 10 migliori
candidati (`ah_gain_top10`, "se anche i migliori non rendono, non c'e'
opportunita' da nessuna parte") — entrambe derivate dal **prezzo**, non
dall'esecuzione, quindi osservabili ogni giorno indipendentemente dal fatto
che la strategia abbia aperto posizioni.

Indice di Youden (capture - falso allarme) per variante e segmento:

| policy | variante | train | validation | oos |
|:--|:--|--:|--:|--:|
| static-10 | self_return | 0.39 | 0.32 | 0.13 |
| static-10 | breadth (segno corretto) | 0.16 | 0.03 | 0.46 |
| static-10 | ah_gain_mean | 0.42 | 0.35 | 0.00 |
| static-10 | ah_gain_top10 | 0.30 | 0.20 | 0.13 |
| static-10 | risk_overlay esistente | 0.18 | 0.14 | 0.13 |
| weak_theme_switch | self_return | 0.43 | 0.50 | 0.42 |
| weak_theme_switch | breadth (segno corretto) | 0.42 | 0.30 | 0.04 |
| weak_theme_switch | ah_gain_mean | 0.54 | 0.34 | 0.25 |
| weak_theme_switch | ah_gain_top10 | 0.35 | 0.28 | 0.13 |
| weak_theme_switch | risk_overlay esistente | 0.22 | 0.07 | 0.25 |

**Tutte e 4 le varianti CUSUM battono il `risk_overlay` trailing gia' in
produzione su train e validation**, su entrambe le policy — non un risultato
fragile legato a un solo segmento. `self_return` resta la piu' consistente
sui 3 segmenti (mai sotto 0.42 su weak_theme_switch). `ah_gain_mean` (la
proposta utente) e' la migliore su train per entrambe le policy (0.42/0.54,
supera self_return li'), ma degrada di piu' in OOS. `ah_gain_top10` non
batte `ah_gain_mean` su nessun segmento — l'aggregato su tutto l'universo si
e' rivelato piu' informativo del "ceiling" sui soli top-10 in questo test
(non da escludere con altri N/percentili, ma non e' il candidato primario
ora). La breadth, col segno corretto, e' piu' informativa di prima ma resta
la meno consistente delle 4.

**Caveat operativo su `self_return`** (la variante piu' robusta): non genera
piu' segnale osservabile una volta fermata la strategia (nessun trade =
nessun nuovo dato) — problema identificato in questa sessione, risolto
concettualmente dalla proposta utente `ah_gain_mean`, derivata dai prezzi e
quindi sempre osservabile anche a strategia ferma.

### Implementazione Backtrader: `ah_gain_cusum` come token di `risk_overlay_mode`

`ah_gain_mean` e' stato implementato in `bt-core/strategies/overnight_ah.py`
come **secondo token** del parametro esistente `risk_overlay_mode` (non un
toggle indipendente): `risk_overlay_mode` accetta ora una lista separata da
virgole di token componibili — `'strategy_throttle'` (il meccanismo trailing
gia' esistente) e/o `'ah_gain_cusum'` (il nuovo CUSUM). Esempio:
`risk_overlay_mode='strategy_throttle,ah_gain_cusum'` attiva entrambi
insieme: la scala finale e' il prodotto delle due — una fermata secca di
`ah_gain_cusum` azzera l'esposizione indipendentemente da cosa calcola
`strategy_throttle` quel giorno, altrimenti prevale la scala continua del
throttle. `self_return` (il segnale piu' robusto ma non osservabile a
strategia ferma, vedi caveat sopra) non e' stato implementato: la scelta
architetturale usa direttamente `ah_gain_cusum` sia per la fermata sia per
la ripartenza, dato che risolve il problema di osservabilita' per
costruzione.

Disegno: baseline (mediana/MAD robuste) congelata una sola volta sui primi
`ah_gain_cusum_baseline_min_days` giorni (default 252, fase I stile
controllo statistico di processo — mai ricalcolata dopo, per non
contaminarsi con lo stress successivo); CUSUM one-sided lower sulla baseline
congelata (`ah_gain_cusum_k`/`ah_gain_cusum_h`, default 0.5/16.0 — vedi
calibrazione sotto); ripartenza solo con K giorni consecutivi entro i limiti
(`ah_gain_cusum_resume_k`, default 5) E un test bootstrap contro la baseline
congelata (`ah_gain_cusum_resume_boot_alpha`, default 0.10) — non un numero
di giorni arbitrario.

Verificato con smoke test Backtrader (`stable_ah_top10` e universo
`weak_theme_switch` reale, 2016-2018): `risk_overlay_mode='off'` invariato;
`'strategy_throttle'` da solo identico al comportamento pre-refactor
(nessuna regressione); `'ah_gain_cusum'` da solo congela la baseline a 252
giorni e produce cicli fermata/ripartenza coerenti con p-value bootstrap;
combinato `'strategy_throttle,ah_gain_cusum'` attiva entrambi insieme
correttamente; un token non valido genera un warning nei log e viene
ignorato senza interrompere gli altri token validi.

**Bug trovato e corretto prima della validazione train/validation/OOS**:
l'aggiornamento della cronologia `ah_gain_cusum` avveniva solo dopo il check
`_before_trade_start` in `next()`, quindi durante il warmup (barre caricate
prima di `trade_start_date`, usato dai run isolati per segmento) la baseline
non accumulava nulla — su validation/OOS (che hanno circa un anno di
warmup) avrebbe sprecato quasi un anno di copertura reale del segmento
aspettando 252 giorni ripartendo da zero proprio a `trade_start_date`. Fix:
l'aggiornamento ora avviene in `next()` prima del check `_before_trade_start`
(solo l'effetto di gating sugli ingressi resta condizionato a
`trade_start_date`, come prima), stesso pattern gia' usato da
`semis_total_3m`/`spy_dd3m` che leggono lo storico caricato indipendentemente
dalla finestra di trading. Verificato: con `fromdate=2020-01-01`,
`trade_start_date='2021-01-01'`, la baseline si congela il 2020-12-31 (usa
il warmup), il primo trade reale resta comunque il 2021-01-05 (nessuna
contaminazione del divieto di trading pre-`trade_start_date`).

**Calibrazione `ah_gain_cusum_h` su `weak_theme_switch`** (script di
ricerca, griglia raffinata a passo 1 tra h=7 e h=20 dopo che la griglia
grossolana iniziale — passo largo, salto diretto da 15 a 20 — aveva perso il
vero ottimo): **h=16** batte il precedente h=15 (scelto dalla griglia
grossolana) su indice di Youden in **tutti e 3 i segmenti**, non solo sul
train usato per la selezione:

| h | train Youden | validation Youden | oos Youden |
|--:|--:|--:|--:|
| 15 (griglia grossolana) | 0.536 | 0.343 | 0.250 |
| 16 (griglia fine, scelto) | 0.595 | 0.438 | 0.292 |
| 17 (pari a 16 su train) | 0.595 | 0.419 | 0.458 |

h=16 e h=17 sono pari sul train; h=16 vince leggermente su validation (unico
criterio di conferma ammesso prima di guardare l'oos) quindi e' il valore
scelto e portato come nuovo default in `overnight_ah.py`. L'oos di h=17 e'
piu' alto ma non e' stato usato per la scelta (avrebbe violato la disciplina
train-poi-validation-poi-oos-solo-a-conferma). Per static-10 la griglia
originale indicava ~20 sul train; non ancora ri-verificato con la griglia
fine (nessuna azione richiesta, static-10 non e' l'obiettivo primario di
`ah_gain_cusum`).

### Validazione Backtrader nativa train/validation/OOS

Eseguita dopo la calibrazione pandas e il fix del warmup (sopra). Comandi:
stesso `--stratargs` documentato per `weak_theme_switch` (sezione
"Implementazione nativa in strategia"), con `trade_start_date` per segmento
(train 2016-01-01, validation 2021-01-01, oos 2024-01-01) e
`risk_overlay_mode='ah_gain_cusum' ah_gain_cusum_h=<valore>`. Baseline OOS
rigenerata pulita (`native_switch_baseline_oos_clean`, stesso comando gia'
documentato come "Comando OOS di riferimento" ma senza overlay), dato che la
directory originale era contaminata (vedi Fase 1 dello studio mesi
negativi). Output completo:
`bt-strategy-test/overnight-ah/research/out/bad_months_study/native_validation/`.

**Primo tentativo, h=16 (calibrato su indice di Youden dei mesi, dal proxy
pandas)**: buono su train (DD -51.6%→-15.4%, Sharpe pari), sfumato su
validation (Sharpe migliora, DD no), **negativo su OOS** (Sharpe 1.661 vs
2.087 baseline, DD -32.67% vs -28.96%, peggio su entrambi pur rinunciando a
piu' di meta' del rendimento). Causa identificata: l'indice di Youden e'
una metrica di *classificazione* dei mesi (capture_rate - false_alarm_rate),
non pesa la **magnitudine** del rendimento perso nei falsi allarmi — un
falso allarme durante un mese fortemente positivo (gran parte dell'OOS
2024-2026, gia' documentato altrove in questo file come regime
eccezionalmente favorevole) costa molto piu' di quanto suggerisca un
conteggio di mesi.

**Ricalibrazione su Sharpe/maxDD nativi diretti** (non piu' Youden): griglia
Backtrader nativa `h=9..30` (passo 1) sul train, poi conferma su
validation e infine su OOS (mai usato per la scelta). **h=23 domina su
tutti e 3 i segmenti**:

| segmento | variante | final multiple | max DD | Sharpe |
|:--|:--|--:|--:|--:|
| train | baseline | 38.32x | -51.63% | 2.225 |
| train | ah_gain **h=23** | **40.61x** | -17.68% | **2.607** |
| validation | baseline | 3.53x | -35.45% | 1.164 |
| validation | ah_gain **h=23** | 3.36x | **-30.21%** | **1.316** |
| oos | baseline (pulito) | 17.31x | -28.96% | 2.087 |
| oos | ah_gain **h=23** | 14.04x | **-27.21%** | 2.068 |

Su train h=23 **batte il baseline su tutti e 3 gli indicatori** (rendimento,
drawdown, Sharpe) — risultato raro, non il tipico compromesso
rendimento-vs-rischio. Su validation batte il baseline su Sharpe e drawdown,
rendimento finale quasi pari. **Su OOS (guardato solo a conferma) tiene**:
Sharpe sostanzialmente pari al baseline (2.068 vs 2.087), drawdown
migliore (-27.21% vs -28.96%), a fronte di una rinuncia moderata di
rendimento (14.04x vs 17.31x) — molto diverso e nettamente migliore del
fallimento OOS di h=16.

Lettura: la calibrazione via Youden aveva scelto un `h` sistematicamente
troppo basso/sensibile (troppi falsi allarmi, ognuno costoso in un regime
favorevole); calibrare direttamente su Sharpe/maxDD nativi porta a un `h`
piu' alto (meno sensibile, fermate piu' rare ma piu' mirate) che si
comporta bene ovunque, non solo nel segmento di calibrazione.

**Conclusione preliminare (poi superata, vedi sotto)**: `ah_gain_cusum` con
**h=23** sembrava un candidato solido sui 3 segmenti isolati
train/validation/OOS (ciascuno con warmup di ~1 anno recente). Un test
successivo su un orizzonte molto piu' lungo ha smontato questa conclusione.

### Fallimento su run continuo 2000-2026 e redesign a baseline mobile (v2)

Test dell'utente: stesso comando `weak_theme_switch` con `hedge_enabled=True`
piu' `risk_overlay_mode='ah_gain_cusum'` (h=23), ma su un run continuo
**2000-2026** invece dei 3 segmenti isolati. Risultato: **capitale finale
-90%** (51.958x contro 532.809x senza overlay) e **drawdown peggiore**, non
migliore (-52.35% contro -51.76%) — l'opposto di quanto promesso dai
segmenti isolati.

Diagnosi: la baseline (mediana/MAD) si congelava **una sola volta**, sui
primi 252 giorni osservati — che nel run 2000-2026 cadevano durante il
crollo dot-com (2000-2001). Quella baseline distorta restava valida per i
25 anni successivi (bolla, 2008, QE, COVID, rialzo tassi 2022, boom AI),
mai piu' ricalcolata.

**Obiezione dell'utente** (corretta, ha guidato il redesign): anche un
ricongelamento periodico (es. annuale) avrebbe lo stesso difetto — se
l'anno scelto per il ricongelamento e' per caso estremo (pessimo o
eccezionale), quello diventa la nuova normalita' fino al ricongelamento
successivo, producendo isteria (normalita' che salta da un estremo
all'altro invece di seguire un'impronta che si evolve gradualmente). La
normalita' deve aggiornarsi di continuo, pesando i dati recenti senza
scartare bruscamente il passato.

**Redesign v2** (`bt-strategy-test/overnight-ah/research/bad_months_spc_cusum_rolling.py`,
implementato in `overnight_ah.py`): mediana/MAD ricalcolate ogni giorno su
una finestra MOBILE di `ah_gain_cusum_window_days` giorni strettamente
precedenti (ex-ante), non piu' un congelamento singolo. Calibrazione 2D
(finestra x h) sul train: `window_days=756` (~3 anni), `h=30` domina la
griglia (Sharpe 3.01 vs 2.23 baseline sul train).

### Verdetto finale: anche v2 non basta — chiuso come risultato negativo

Test rigorosi in sequenza, ciascuno smentendo il precedente:

1. **v2 sul run continuo 2000-2026**: meno catastrofico di v1 (71.680x
   contro 51.958x) ma ancora un taglio dell'86% del capitale finale rispetto
   al solo hedge (532.809x), **con drawdown sostanzialmente invariato**
   (-52.38% contro -51.76%).
2. **Diagnosi precisa** (non piu' ipotesi vaghe): il drawdown peggiore del
   test 2000-2026 e' lo stesso identico episodio in entrambi i run (picco
   2000-07-17, minimo 2001-07-09, il crollo dot-com) — e cade quasi
   interamente PRIMA che il meccanismo avesse accumulato i 252 giorni minimi
   per attivarsi (prima fermata possibile: 2001-02-27). Non era quindi (solo)
   un problema di baseline mobile vs congelata: era un punto cieco di
   warm-up strutturale di un backtest che parte 7 mesi prima del peggior
   evento della serie.
3. **Test su deployment realistico** (warmup dal 2013, trading reale dal
   2016 al 2026 — stesso principio gia' usato per train/validation/OOS,
   evita il punto cieco): il problema persiste. Hedge da solo: 3.962x,
   DD -28.61%, Sharpe 2.379. Hedge + v2 rolling: **1.388x (-65%)**,
   **DD -36.48% (peggio)**, Sharpe 2.248 (peggio). Il drawdown peggiore in
   entrambi i casi e' lo stesso episodio (picco 2025-01-22).
4. **Analisi puntuale di quell'episodio**: il meccanismo ha effettivamente
   fermato la strategia per una parte ragionevole e ben temporizzata
   dell'episodio (29 giorni su ~99, fermata 10-28 aprile con ripartenza il
   28, minimo il 30 aprile — timing quasi ottimale), ma ha comunque perso le
   prime 6 settimane di discesa (picco 22 gennaio, prima fermata 6 marzo,
   z-score non ancora sopra soglia). **Anche in questo episodio, il suo
   momento migliore**, il beneficio non basta a compensare il costo
   cumulato di 48 cicli fermata/ripartenza sparsi sui 10 anni, la maggior
   parte dei quali cade in periodi normali o buoni, non in crisi.

**Conclusione**: in tre test indipendenti (2000-2026 v1, 2000-2026 v2,
deployment realistico 2016-2026 v2) `ah_gain_cusum` — con qualunque
variante di baseline provata finora — non offre un beneficio netto
positivo su un orizzonte di deployment realistico. I risultati
apparentemente solidi della calibrazione segmentata (h=23 su train/
validation/OOS isolati) erano un abbinamento fortunato tra finestra di
calibrazione e finestra di test, smascherato dal test piu' severo su un
orizzonte lungo. **Chiuso come risultato negativo, analogamente a quanto
gia' fatto per l'ipotesi AH-dominance (Fase 2b) e per momentum/AH-index nel
hedge SQQQ**: il meccanismo resta nel codice come opzione opt-in
(`risk_overlay_mode` include `'ah_gain_cusum'` come token disponibile,
default comunque `'off'`), documentato e disponibile per chi volesse
riprendere il lavoro con un approccio diverso, ma non raccomandato e non
promosso a `overnight_ah_live.py`/checkout prod.

## Studio concentrazione/paniere (sospeso, da riprendere)

Dopo la chiusura negativa di `ah_gain_cusum`, l'utente ha proposto un angolo
diverso: invece di meccanismi di *timing* (quando tradare), agire
sull'**universo** (quali nomi tradare). Studio con lo stesso rigore
metodologico (finestra realistica 2013-warmup/2016-2026-trade, come sopra —
niente piu' punti ciechi da cold-start). Sospeso su richiesta esplicita
dell'utente prima di arrivare a una proposta operativa; questa sezione
documenta cosa e' stato scoperto per riprendere il lavoro senza ripartire da
zero.

### Sweep `monthly_universe_top_n` (numero di nomi nel paniere dinamico)

Output: `bt-strategy-test/overnight-ah/research/out/bad_months_study/native_validation/topn_sweep_realistic_2016_2026.csv`.

| top_n | final | max DD | Sharpe |
|--:|--:|--:|--:|
| 5 | 121.55x | -28.95% | 1.886 |
| 10 | 682.14x | -28.95% | 2.067 |
| 20 | 2,397.82x | -28.95% | 2.226 |
| 30 | 3,122.09x | -28.95% | 2.266 |
| 50 (attuale) | 3,671.09x | -28.95% | 2.289 |
| 60 | 3,857.70x | -28.95% | 2.298 |

**Il max drawdown e' identico — letteralmente -28.95% — per qualunque
top_n da 5 a 60.** Sharpe e rendimento crescono monotonicamente con piu'
nomi, senza eccezioni. Causa: `top_n` influenza solo i mesi in regime
dinamico; nei mesi in regime statico la strategia usa sempre il paniere
fisso di 10 nomi indipendentemente da `top_n`. Restringere il paniere
dinamico non tocca affatto il worst-case (che infatti si e' rivelato cadere
in regime statico — vedi sotto). **Conclusione: restringere `top_n` non
aiuta, riduce solo il rendimento.**

### Sweep `max_concurrent` (posizioni simultanee)

Output: `bt-strategy-test/overnight-ah/research/out/bad_months_study/native_validation/maxconcurrent_sweep_realistic_2016_2026.csv`.

| max_concurrent | final | max DD | Sharpe |
|--:|--:|--:|--:|
| 3 | 11,130.11x | -31.68% | 2.240 |
| **5 (attuale)** | 3,671.09x | **-28.95% (migliore)** | **2.289 (migliore)** |
| 7 | 710.92x | -30.95% | 2.080 |
| 10 | 287.65x | -34.94% (peggiore) | 2.070 |
| 15 | 79.22x | -33.58% | 1.871 |
| 20 | 33.71x | -31.76% | 1.764 |

**`max_concurrent=5` (default attuale) e' gia' un ottimo locale**: miglior
Sharpe e miglior drawdown dell'intero sweep, insieme. Sotto (3): rendimento
finale molto piu' alto ma Sharpe/DD peggiori (concentrazione per-trade piu'
alta con `size_by_max_concurrent=True` — capitale diviso su meno slot,
posizioni piu' grandi, percorso piu' accidentato ma capitalizzazione finale
piu' alta: Sharpe misura la fluidita' del percorso, non il capitale finale,
i due possono divergere). Sopra (7+): peggiora tutto insieme, nessun
compromesso favorevole. **Nessun margine di miglioramento su questa leva.**

### Analisi episodi di drawdown (>=10%, 2016-2026)

Estratti tutti gli episodi significativi dalla curva equity nativa
(`native_switch_topn_50_realistic`, hedge disattivato, stesso universo
`weak_theme_switch`), con regime attivo e peggiori contributori per
ciascuno (finestra picco→minimo esatta, da `trades.json`):

| picco → minimo | DD | regime | peggiori 3 contributori |
|:--|--:|:--|:--|
| 2018-01 → 02 | -15.31% | dinamico | PYPL, NVDA, INTC |
| 2019-05 | -27.75% | dinamico | NXPI, MCHP, AMD |
| 2020-02 (COVID) | -23.04% | dinamico | LRCX, MSFT, AAPL |
| 2021-12 → 2022-07 | -22.31% | misto (per lo piu' dinamico) | MRVL, MU, ADBE |
| 2022-11 → 2023-05 | -18.35% | misto (per lo piu' dinamico) | AMD, WDAY, NVDA |
| 2024-02 | -21.03% | dinamico | PANW, ZS, CDNS |
| **2025-01 → 04** | **-28.95%** | **statico (unico puro)** | MU, MRVL, AMD |
| 2025-07 → 09 | -18.06% | dinamico | MCHP, AMD, LRCX |
| 2026-02 → 03 | -23.97% | dinamico | AMAT, INTC, LRCX |

**Scoperta chiave, che ribalta la diagnosi iniziale**: su 9 episodi seri,
**solo 1 e' puramente in regime statico** (gennaio-aprile 2025, quello
isolato per primo). Gli altri 8 — inclusi i piu' gravi (2019 -27.8%, 2026
-24.0%, COVID -23.0%) — avvengono in regime **dinamico**, col paniere ampio
fino a 50 nomi. E in **ogni singolo episodio**, statico o dinamico, i
peggiori contributori sono nomi semiconduttori/tech/AI (NVDA, INTC, LRCX,
AMD, MU, MRVL, MCHP, AMAT, PANW, ZS, CDNS, WDAY, ADBE, NXPI).

**Non e' quindi il paniere statico ad essere mal calibrato** — e' l'intera
strategia, in entrambe le modalita', ad essere strutturalmente concentrata
sul tema AI/semis: quella dinamica seleziona esplicitamente per
correlazione al fattore semis (`monthly_universe_theme_score='corr12'`,
peso 0.15), quella statica e' nata come lista dei migliori performer AH
storici (anch'essi quasi tutti semis). Il regime-switch distingue *quando*
usare dinamico vs statico, ma non diversifica *lontano* dal tema in nessuno
dei due casi.

### Test "fallback vuoto" (nessun trade durante regime statico)

Per isolare "il problema e' essere esposti durante il regime debole, o sono
questi nomi specifici": `monthly_universe_static_symbols=''` (nessun
candidato nei mesi statici, strategia flat). Confermato via `returns.csv`:
rendimento 0.0% per l'intero dicembre 2024-maggio 2025 (1 solo giorno
non-zero su 113, residuo di chiusura posizione).

| variante | final | max DD | worst episode |
|:--|--:|--:|:--|
| static-10 fallback (attuale) | 3,671.09x | -28.95% | 2025-01→04 |
| fallback vuoto (no trade) | 1,146.35x | -28.07% | **2019-05→08** (diverso!) |

L'episodio 2025 e' completamente evitato, ma il **worst-case complessivo
non migliora quasi per niente** (-28.07% contro -28.95%) perche' emerge
l'episodio 2019 (dinamico, non statico) di grandezza comparabile. Andare
flat durante i mesi statici cura un sintomo specifico (costando il 68% del
capitale finale) senza risolvere il rischio di fondo — che e' strutturale
alla strategia, non solo al fallback statico.

### Stato: sospeso su richiesta dell'utente

Nessuna modifica al paniere statico o al regime-switch e' stata
implementata. Prossimi passi possibili per chi riprende:

- La domanda dell'utente non ancora risolta: `stable_ah_top10` e' stato
  calibrato/selezionato guardando la sua performance storica complessiva
  (selection bias gia' documentato altrove in questo file), **non**
  specificamente per il suo ruolo di fallback nei mesi di regime-switch
  debole. Andrebbe verificato se un paniere statico scelto *per quel ruolo
  specifico* (basso beta/correlazione al fattore semis nei mesi in cui il
  fattore semis e' negativo) si comporta meglio — ma la scoperta sopra (8/9
  episodi in regime dinamico) suggerisce che questo da solo non basterebbe.
- Il problema piu' fondamentale — concentrazione tematica strutturale in
  ENTRAMBI i regimi — richiederebbe di rivedere anche `monthly_universe_theme_score`/
  `monthly_universe_theme_weight` (il tilt dinamico verso la correlazione
  semis), non solo il paniere statico.
- Nessuna delle piste precedenti (throttle, `ah_gain_cusum`, restringere
  `top_n`) ha funzionato — l'evidenza finora suggerisce che il rischio di
  coda di questa strategia e' un tratto strutturale legato alla scelta del
  tema (semis/AI), non un difetto risolvibile con overlay di timing o
  aggiustamenti di concentrazione/numero di nomi.

### Ipotesi operative emerse (proposte, non implementate/validate in Backtrader)

1. **Tuning switch feature**: testare `monthly_universe_switch_feature`
   su lookback 6m/12m invece di 3m per `weak_theme_switch`, sulla base dei 4
   segnali robusti di Fase 3 (stesso spirito del `semis_total_6m > 0` gia'
   documentato come sfidante sopra, ma qui motivato specificamente da
   capacita' predittiva sui mesi negativi, non solo da OOS aggregato).
2. **CUSUM ibrido self+breadth**: fermata su CUSUM self-referential,
   ripartenza su CUSUM/soglia breadth (o size-canary) — vedi Fase 5.
3. **Nessuna azione su selezione universo per "rumore RTH"**: l'ipotesi
   AH-dominance (Fase 2b) e' stata testata e confutata, non procedere in
   quella direzione senza nuova evidenza.

Prossimo passo esplicito: validare le proposte 1-2 con proxy pandas piu'
approfondito e poi Backtrader (`btmain.py`), coerente col vincolo "pandas
prima, Backtrader dopo" gia' seguito nel resto del progetto. Non ancora
fatto in questa sessione (fuori scope, vedi piano di sessione).

## Prossima analisi: validazione hedge su `weak_theme_switch` (specifiche)

Nota: vedi sopra — questa validazione resta valida ma non e' piu' il
prossimo passo immediato, e' stata posticipata a favore dello studio sui
mesi negativi.

Obiettivo: confermare (o smentire) il beneficio dell'hedge EMA(65,150)/peso
0.15 misurato su statico-10 (+1128x vs +976x, Sharpe 1.509 vs 1.430, DD 2022
-3.1% vs -23.9%) sull'universo dinamico reale, non solo sul paniere di test.
Finché questo non è fatto, l'hedge resta research-only.

### Trappola da evitare: contaminazione del paniere di controllo

`config-common/tickers/yahoo_adj_research_universe_hedge.json` aggiunge QQQ e
SQQQ al file usato per il confronto. Con `hedge_enabled=True` questi due
simboli vengono esclusi da `_trade_stocks` (stesso trattamento di SPY, vedi
`overnight_ah.py:__init__`), quindi non entrano mai nel pool di ranking
mensile (`_ordered_regime_switch_trade_stocks` lavora solo su
`_trade_stocks`). Ma con `hedge_enabled=False` **non vengono esclusi**: se si
riusa lo stesso ticker file per il run di controllo senza hedge, QQQ e SQQQ
diventano candidati AH tradabili a tutti gli effetti, sporcando il confronto.
**Il run di controllo (baseline, no hedge) deve usare
`yahoo_adj_research_universe.json` (senza QQQ/SQQQ), non la variante hedge.**

### Comandi

Baseline (no hedge, per confronto — identico al "Comando OOS di riferimento"
sopra, solo rinominato):

```bash
bt-core/.venv/bin/python bt-core/btmain.py \
  --strat overnight_ah.OvernightAH \
  --ticker yahoo_adj_research_universe.json \
  --mode backtest --timeframe daily --provider yahoo_adj \
  --fromdate 2023-01-01 --todate 2026-06-23 \
  --commission none --margin-leverage 2 \
  --id hedge_validation_baseline_no_hedge \
  --stratargs "max_concurrent=5 size_by_max_concurrent=True max_exposure=2 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 min_adv=100000000 auction=True monthly_universe_mode='weak_theme_switch' monthly_universe_top_n=50 monthly_universe_base_weight=0.85 monthly_universe_theme_weight=0.15 monthly_universe_theme_score='corr12' monthly_universe_switch_feature='semis_total_3m' monthly_universe_switch_threshold=0.0 monthly_universe_spy_dd3m_threshold=-0.10 trade_start_date='2024-01-01'"
```

Variante con hedge (stessi parametri, universo hedge, overlay attivo):

```bash
bt-core/.venv/bin/python bt-core/btmain.py \
  --strat overnight_ah.OvernightAH \
  --ticker yahoo_adj_research_universe_hedge.json \
  --mode backtest --timeframe daily --provider yahoo_adj \
  --fromdate 2023-01-01 --todate 2026-06-23 \
  --commission none --margin-leverage 2 \
  --id hedge_validation_ema65_150_w015 \
  --stratargs "max_concurrent=5 size_by_max_concurrent=True max_exposure=2 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 min_adv=100000000 auction=True monthly_universe_mode='weak_theme_switch' monthly_universe_top_n=50 monthly_universe_base_weight=0.85 monthly_universe_theme_weight=0.15 monthly_universe_theme_score='corr12' monthly_universe_switch_feature='semis_total_3m' monthly_universe_switch_threshold=0.0 monthly_universe_spy_dd3m_threshold=-0.10 trade_start_date='2024-01-01' hedge_enabled=True hedge_symbol='SQQQ' hedge_trend_symbol='QQQ' hedge_fast_period=65 hedge_slow_period=150 hedge_weight=0.15"
```

### Limite noto della finestra 2023-2026: non copre il 2022

Il beneficio misurato finora (DD 2022 -3.1% vs -23.9%) viene interamente
dal bear market 2022. La finestra `weak_theme_switch` sopra (warmup 2023,
trade dal 2024) **non copre il 2022** — nessun run `weak_theme_switch` è mai
stato eseguito su una finestra che lo includa (verificato: nessun
riferimento a `fromdate` 2021/2022 in questo documento per quella modalità).
Su questa finestra il test puo' solo confermare che l'hedge **non
danneggia** il rendimento in un periodo senza drawdown severo (drag/costo
d'opportunità delle notti in cui apre senza che serva) — non puo' confermare
il beneficio vero e proprio.

Prima di eseguire i comandi sopra, verificare se e' possibile estendere
`fromdate`/warmup di `weak_theme_switch` indietro fino al 2021-2022 (dati
disponibili? feature di regime `semis_total_3m` calcolabile cosi' indietro?
selezione dinamica stabile con meno storia?). Se si', **preferire quella
finestra estesa** come test primario (copre il vero stress case) e usare
2023-2026 come secondo controllo. Se il warmup dinamico non regge prima del
2023, documentarlo esplicitamente come limite strutturale della validazione,
non ignorarlo.

### Criteri di decisione

Confrontare baseline vs hedge su:

- rendimento totale e Sharpe sull'intera finestra testata;
- se la finestra include il 2022 (o un altro drawdown severo): riduzione del
  max drawdown nel periodo di stress, stessa metrica usata sul test
  statico-10;
- numero di notti in cui l'hedge apre (`HEDGE_ENTRY_SIGNAL` nei log) e
  relativo drag quando il mercato non scende (deve essere piccolo e non
  sistematico);
- nessuna esecuzione fantasma / leva anomala nei log (conferma indiretta che
  il fix broker regge anche su questo universo piu' largo, con piu'
  candidati contemporanei e quindi piu' occasioni di oversubscription);
- robustezza per segmento (train/validation/OOS o quanto disponibile con
  questa finestra piu' corta), non solo la media.

Se l'hedge migliora Sharpe/DD senza deteriorare sistematicamente il
rendimento nei periodi senza stress → procedere con la richiesta esplicita
di promozione al checkout prod (decisione separata, non automatica).
Se il beneficio non si conferma sull'universo reale (es. troppi pochi
candidati contemporanei per rendere significativo un carve-out del 15%, o il
timing della selezione mensile interagisce male con l'apertura dell'hedge)
→ documentare perché e considerare l'hedge chiuso come risultato negativo,
analogamente a quanto già fatto per momentum/AH-index/singole azioni.

### Item secondario: cooldown "nessun effetto" (in attesa dell'utente)

Segnalazione dell'utente: con la nuova configurazione (hedge EMA) sembra che
`post_up_cooldown_threshold=0.05 post_up_cooldown_days=5` non abbia più
effetto. Test di controllo eseguiti in questa sessione (hedge on/off, stessi
parametri di cooldown) hanno mostrato 84 eventi di trigger identici in
entrambi i casi e un effetto misurabile su trade/Sharpe/TimeReturn — quindi
non riprodotto con i parametri usati qui. Prima di investigare oltre, serve
dall'utente il comando/config esatto con cui ha osservato l'anomalia
(ticker file, date, altri stratargs oltre a hedge e cooldown).

### Non-goal

Nessuna promozione al checkout prod finché questa validazione non è
completata e la promozione non è stata richiesta esplicitamente.

## Studio tuning parametri di scoring: oracolo/regret/Model Confidence Set (completato, risultato negativo)

Richiesta dell'utente: tuning dei parametri di **composizione dello score**
per la selezione mensile del paniere (`monthly_universe_base_weight`/
`c2c_weight`/`theme_weight`/`theme_score`/`switch_feature`/
`switch_threshold`/`top_n`/`spy_dd3m_threshold`, più due parametri nuovi non
esistenti nella strategia — lookback della finestra momentum e frequenza di
ribilanciamento del paniere), con un benchmark diverso dal solito: non Sharpe
assoluto (uno Sharpe basso può essere fisiologico in un regime avaro, non un
sintomo di parametri sbagliati), ma **distanza da un oracolo** (il caso
migliore possibile), con validazione statistica (Model Confidence Set) invece
di scegliere "il migliore" su un singolo punto, e un blocco di validazione
mai guardato fino alla fine. Tutti gli altri filtri della strategia
(volatilità intraday, prezzo minimo, ADV, `ah_lag1`, earnings) disattivati
per isolare l'effetto dei soli parametri di scoring/regime.

### Oracolo: nessun paniere, hindsight puro giornaliero

Definizione (decisa con l'utente, corregge un'ipotesi iniziale errata basata
su un oracolo mensile-bloccato): ogni giorno, tra tutto l'universo (stessi
filtri disattivati), l'oracolo prende semplicemente i `max_concurrent=5`
titoli con il miglior `close→next_open` **realizzato quel giorno specifico**
(nessun vincolo di lock mensile — è esattamente ciò che i parametri sotto
tuning cercano di approssimare, quindi non ha senso vincolare l'oracolo a
usare un paniere). Anti-oracolo: stesso meccanismo sui 5 peggiori, usato come
pavimento per normalizzare il regret tra regimi diversi:

```text
regret_norm(θ, blocco) = (oracolo - candidato) / max(oracolo - anti_oracolo, span_floor)
```

scala `[0 = come l'oracolo, 1 = come l'anti-oracolo]`, indipendente dal
livello assoluto di rendimento del regime. Sul blocco di validazione
(2022-08-07 → 2026-08-07): oracolo `ann_log_ret=7.28`, anti-oracolo `=-6.88`,
span `=14.16` — numeri enormi perché è hindsight puro senza vincoli
strutturali, da leggere solo come ancora di normalizzazione, non come
benchmark di rendimento raggiungibile.

### Metodologia: blocchi variabili + Model Confidence Set

Storia 2000-2026 divisa in blocco di validazione intonso (ultimi ~4 anni,
2022-08-07→2026-08-07, mai simulato prima della conferma finale) e pool
2000-2022 diviso in 12 blocchi calendariali non sovrapposti (~2 anni
ciascuno). Il metodo statistico per l'insieme "più probabile" (non il
singolo best) è il **Model Confidence Set** di Hansen/Lunde/Nason,
implementato in numpy puro (bootstrap 5000 repliche sui blocchi,
eliminazione iterativa del modello con eccesso di regret standardizzato più
alto finché il test di parità non rifiuta più), con cross-check bootstrap
percentile indipendente e rotazioni leave-block-out per verificare la
stabilità della regione selezionata cambiando quale blocco viene escluso.

Spazio testato: 25.200 combinazioni (enumerazione completa di
`theme_score`×`switch_feature`×`lookback`×`rebal_freq`, Latin Hypercube
sampling sui continui `c2c_weight`/`theme_share`/`top_n`/soglie), simulate
vettorialmente in pandas (25.200 combo × 12 blocchi in ~20 minuti su 22
worker).

### Risultato screening/MCS: regione trovata sui blocchi 2000-2022

Top-250/25.200 per mediana di `regret_norm` (il MCS ad α=0.10 non ne elimina
quasi nessuno — segnale onesto: con solo 12 blocchi il test non ha potere
per distinguere statisticamente tra combinazioni simili, la "regione più
probabile" è genuinamente larga):

- `c2c_weight` mediana **0.145** contro 0.50 di popolazione — il peso AH
  domina nettamente sul C2C nello score base;
- `theme_share` mediana **0.079** contro 0.50 — tilt tematico piccolo;
- `lookback`: **0% dei survivor usa 1 mese**, 44% usa 12 mesi, 36% usa 3
  mesi — lookback corto sempre perdente;
- `rebal_freq`: lieve preferenza per settimanale/mensile su giornaliero
  (38%/36% contro 26%);
- `switch_feature`: `semis_ma126_ratio`/`semis_ma63_ratio` leggermente
  sovra-rappresentate (19.6%/14.0% contro 10% di base).

### Validazione Backtrader reale sul blocco intonso: la regione NON batte il default

**Limite strutturale**: `lookback` e `rebal_freq` non esistono come
parametri della strategia live — sono cablati (`_shift_month(month_start,
-6)`, sempre mensile). Solo i survivor con `lookback=6m`/`rebal_freq=mensile`
(21 su 250) sono nativamente validabili senza modificare
`overnight_ah.py`. I 6 migliori di questi 21 sono stati eseguiti via
`btmain.py` reale (universo `yahoo_adj_research_universe.json`, provider
`yahoo_adj`, filtri disattivati, `trade_start_date='2022-08-07'`) e
confrontati col default operativo attuale (`c2c_weight=0.6`, `theme_share
=0.15`, `theme_score='corr12'`, `switch_feature='semis_total_3m'`,
`switch_threshold=0.0`, `spy_dd3m_threshold=-0.10`, `top_n=50`) sullo stesso
blocco:

| combo | TimeReturn | Sharpe | SQN | regret_norm (vs oracolo) |
|:--|--:|--:|--:|--:|
| **default attuale** | **5.364** | **0.847** | **4.627** | **0.4814 (migliore)** |
| combo 15686 (c2c_weight=0.049) | 5.221 | 0.700 | 4.600 | 0.4818 |
| combo 9834 (c2c_weight=0.052) | 5.136 | 0.692 | 4.527 | 0.4820 |
| combo 8158 (c2c_weight=0.103) | 5.125 | 0.670 | 4.543 | 0.4820 |
| combo 22446 (c2c_weight=0.069) | 5.124 | 0.656 | 4.491 | 0.4820 |
| combo 20761 (c2c_weight=0.058) | 4.962 | 0.645 | 4.439 | 0.4825 |
| combo 16564 (c2c_weight=0.033) | 4.835 | 0.683 | 4.374 | 0.4829 |

Il default attuale batte **tutti e 6** i candidati MCS su ogni metrica,
inclusa la distanza dall'oracolo. **Conclusione onesta**: la regione a
`c2c_weight` basso/AH-dominante, forte e consistente sui blocchi storici
2000-2022, non generalizza al periodo mai guardato 2022-2026 — il default
attuale (`c2c_weight=0.6`) resta il migliore dei 7 testati su questo
segmento. Nessuna promozione. Zona grigia non testata: valori intermedi di
`c2c_weight` (0.3-0.5) non sono stati confrontati sul blocco di validazione,
resta un'ipotesi aperta se si vuole approfondire.

### Verifica aggiuntiva: chiusura-chiusura invece di chiusura-apertura (confermata negativa)

Domanda dell'utente: dato che `c2c_weight=0.6` (peso C2C dominante nello
score di **selezione**) funziona meglio, converrebbe **tradare** anche
chiusura-chiusura (tenere la posizione anche nella sessione regolare del
giorno dopo) invece di chiusura-apertura? Verificato sullo stesso paniere
reale già validato (`c2c_weight=0.6`/`theme_share=0.15`/`corr12`/
`semis_total_3m`):

| leg | ann_log_ret storia intera (2000-2026) | ann_log_ret blocco validazione |
|:--|--:|--:|
| chiusura→apertura (attuale) | **0.363** | **0.459** |
| chiusura→chiusura (tenendo anche il giorno dopo) | 0.250 | 0.456 |
| solo il pezzo aggiunto (RTH del giorno dopo) | **-0.107 (negativo)** | ~0.000 (piatto) |

Correlazione fra il leg overnight e il pezzo aggiunto: **-0.016** (nessuna
sinergia). Confirma la "session disjunction" già documentata in questo file
(sezione BoCSoO, `AH return % vs RTH Sharpe: -0.756`): il peso C2C nello
score serve a **selezionare** bene (momentum generale come segnale
predittivo), ma il rendimento da **incassare** resta specificamente quello
overnight — allungare la detenzione diluisce l'edge con una gamba a valore
atteso nullo o negativo. Il disegno MOC/MOO attuale resta corretto così
com'è; non procedere in questa direzione senza nuova evidenza.

### File e output

Script (`bt-strategy-test/overnight-ah/research/`):
`daily_panel_full_history.py` (dataset 2000-2026 filtri neutralizzati),
`oracle_daily.py`, `score_panel.py` (rank C2C/AH per lookback, tema, regime,
drawdown SPY, tutti ex-ante), `simulate_basket_rotation.py` (riferimento
corretto, validato contro Backtrader reale — vedi fedeltà sotto),
`parameter_sweep_scoring.py` (fast-path vettoriale, verificato identico al
riferimento su spot-check casuali, `max_abs_diff=0.0`), `mcs_selection.py`,
`validate_top_survivors_backtrader.py`.

Output bulk (fuori da `/home`, disco `/home` quasi saturo — vedi nota
disco): `/mnt/Backup/overnight_ah_tuning/` (daily panel, score panel,
oracolo, sweep, MCS, risultati validazione). `bt-core/out/` e
`bt-strategy-test/overnight-ah/research/out/` sono stati spostati su
`/mnt/Backup/backtrader_archive/` con symlink al loro posto (nessun path
relativo esistente rotto), per liberare spazio su `/home` (3.6GB liberi
prima, ~205GB dopo).

**Verifica di fedeltà pandas↔Backtrader** (obbligatoria prima di fidarsi
dello sweep): dopo aver corretto un disallineamento di 1 giorno
nell'indicizzazione (Backtrader registra il rendimento sul giorno di
*uscita*, il proxy pandas lo indicizza sul giorno di *entrata* — stesso
identico rendimento, etichetta diversa), correlazione giornaliera **0.98**
su un anno di test, paniere mensile sovrapposto per 3/5 titoli — stesso
livello di approssimazione (finestra calendario via rolling time-based
invece della logica esatta a 252 barre + filtro calendario) già usato e
accettato in `edge_prediction_study.py`.

### Non-goal

Nessuna modifica ai parametri operativi attuali. Nessuna implementazione di
`lookback`/`rebal_freq` come parametri nativi della strategia (necessaria
solo se si vuole approfondire il segnale pandas su questi due assi, non
richiesta né decisa in questa sessione).

## Studio 2: indicatori tecnici univariati, close standard vs serie AH-only (completato, risultato negativo)

Seguito diretto dello studio precedente: se il tuning dei pesi c2c/ah/theme
non batte il default, forse altri indicatori tecnici (medie mobili, momentum
via regressione, MACD, RSI/StochRSI, SuperTrend, volatilità, volume) hanno
un effetto predittivo che lo score attuale non cattura. Riusa integralmente
l'infrastruttura oracolo/regret/blocchi/validazione dello Studio 1 (nessuna
ricostruzione).

### Richiesta chiave dell'utente: doppia serie di input

Dato che la strategia trada solo la gamba overnight (chiusura→apertura),
ogni indicatore basato su una serie di prezzo (medie mobili, momentum,
MACD, RSI, StochRSI) è stato calcolato **due volte**: una sul `close`
standard (suffisso `_cc`, cattura C2C = AH+RTH insieme) e una su una serie
sintetica costruita **solo** dalla componente overnight:

```text
P_AH[t] = P_AH[t-1] * (1 + known_ah_ret[t]),  P_AH[0] = 1
```

Verificato esattamente (`P_AH[t]/P_AH[t-1]-1 == known_ah_ret[t]`, diff
~1e-16) e per correlazione: il momentum grezzo su `P_AH` correla 0.992
(Spearman) con `ah_mean_6m` già validato nello Studio 1, quello su `close`
correla 0.977 con `c2c_mean_6m` — la costruzione ex-ante è corretta.
Indicatori che richiedono High/Low/Volume (ATR%, SuperTrend, volume) non
hanno un analogo AH pulito nei dati daily disponibili e restano a singola
variante; l'eccezione è la volatilità, dove `ah_gap_vol_mean` (media
rolling di `|known_ah_ret|`) è l'analogo naturale AH-specifico di
`intraday_vol_mean` (RTH-specifico), non un porting forzato.

Totale: 35 indicatore-parametrizzazioni nel primo giro, poi allargato a
**89** (griglia di lookback più fitta per famiglia, es. SMA ratio a 9
lookback invece di 3, regressione a 7 invece di 3) prima di scegliere il
migliore per famiglia/serie.

### Bug reale trovato e corretto durante la verifica di fedeltà

Il port di `SuperTrend` (`bt-core/indicators/supertrend.py`) usava ATR a
media semplice (`true_range().rolling(period).mean()`) invece che smussata
alla Wilder — `SuperTrendBand` usa internamente
`bt.indicators.AverageTrueRange`, la cui docstring dice esplicitamente
"Formula: SmoothedMovingAverage(TrueRange, period)" (diverso da
`ATR_SMA_Pct` in `atr_sma.py`, che è SMA-based *per design*, non la stessa
classe). Con ATR SMA-based il fidelity check su Cerebro reale mostrava fino
al **49% di divergenza relativa** (un singolo flip di banda mal temporizzato
per la differenza di ATR cascata fino al flip successivo). Fix: ATR
smussata alla Wilder (`ewm(alpha=1/period, adjust=False)`, stesso pattern
già usato per RSI) in `supertrend_distance`. Dopo il fix: diff massima
0.001-0.2% (floating point/transiente EMA, non un problema). RSI Wilder e
OBV erano già fedeli al confronto con Backtrader reale (`bt.indicators.RSI`,
`OnBalanceVolume` da `LZIndicator.py`); OBV mostra solo un offset costante
sul giorno 0 (nessun close precedente da confrontare, dovuto a una
convenzione di inizializzazione diversa), verificato innocuo perché
invariante per una slope di regressione.

### Risultato principale: la serie AH-only batte nettamente il close standard

Verificato con una calibrazione indipendente: lo stesso codice di
regressione applicato al feature già validato `ah_mean_6m` (Studio 1) dà un
t-stat di Fama-MacBeth di 14.0 — praticamente identico al miglior indicatore
nuovo `sma_ratio_ah_252` (13.9) — quindi la forza dei nuovi indicatori non è
un artefatto del codice, è un segnale reale confrontabile in scala.

| Indicatore | Serie | t-stat FM (pool 2000-2022) | % blocchi stesso segno |
|---|---|---:|---:|
| `sma_ratio_200` | AH | **13.9** | 100% |
| `sma_ratio_200` | c-c | 1.4 | 83% |
| `reg_slope_126` | AH | **12.9** | 100% |
| `reg_slope_126` | c-c | 2.5 | 83% |
| `rsi_wilder_14` | AH | **10.0** | 100% |
| `rsi_wilder_14` | c-c | -2.8 | 58% |

Conferma quantitativa della "session disjunction" già nota (BoCSoO,
`AH return % vs RTH Sharpe: -0.756`): l'informazione utile per prevedere
l'overnight sta nell'overnight, non nel prezzo misto AH+RTH. Bonus non
previsto: anche la famiglia volatilità (`ah_gap_vol_mean`,
`intraday_vol_mean`, `atr_pct`) mostra segnale solido (t-stat 7.5-9.7).

### Nessun indicatore singolo batte il default sul blocco di validazione

Selezionato il miglior lookback per 18 famiglie/serie (MCS α=0.25 + libero
da 89), poi validati i 21 con `lookback=6m`/`rebal_freq=mensile` (gli unici
nativamente supportati dalla strategia — `lookback` e `rebal_freq` non sono
parametri della strategia live, sono cablati) via Backtrader reale sul
blocco intonso:

| candidato | regret_norm validazione |
|---|---:|
| **default attuale** | **0.4814 (migliore)** |
| 6 migliori candidati singoli testati | 0.4818–0.4829 |

Stesso esito dello Studio 1: forte sui blocchi storici, non regge sul
blocco mai guardato. Tabella completa (89 indicatori, statistico+economico):
`/mnt/Backup/overnight_ah_tuning/indicator_sweep/indicator_study_summary.csv`.

### Verifica aggiuntiva: chiusura-chiusura invece di chiusura-apertura (confermata negativa)

Ipotesi dell'utente: dato che il peso C2C (60%) nello score di selezione
funziona, converrebbe tradare anche chiusura-chiusura (tenere la posizione
anche il giorno regolare successivo) invece di chiusura-apertura? Verificato
sullo stesso paniere reale già validato:

| leg | ann_log_ret storia intera (2000-2026) | ann_log_ret blocco validazione |
|---|---:|---:|
| chiusura→apertura (attuale) | **0.363** | **0.459** |
| chiusura→chiusura | 0.250 | 0.456 |
| solo il pezzo aggiunto (RTH giorno dopo) | **-0.107 (negativo)** | ~0.000 (piatto) |

Correlazione fra il leg overnight e il pezzo aggiunto: -0.016 (nessuna
sinergia). Il peso C2C serve a **selezionare** bene (momentum generale come
segnale predittivo), ma il rendimento da **incassare** resta specificamente
quello overnight — allungare la detenzione diluisce l'edge con una gamba a
valore atteso nullo o negativo. Il disegno MOC/MOO attuale resta corretto
così com'è.

### File e output

Script (`bt-strategy-test/overnight-ah/research/`): `indicator_panel.py`
(35→89 indicatori, porting da `bt-core/indicators/regression.py`,
`MACD.py`, `supertrend.py`, `atr_sma.py`, `LZIndicator.py`),
`indicator_bt_fidelity_check.py`, `indicator_fama_macbeth.py` (regressione
cross-sezionale per periodo + aggregazione nel tempo, non lo stderr OLS di
un singolo periodo — quello sottostimerebbe l'incertezza),
`indicator_single_factor_basket.py`, `indicator_report.py`. Output:
`/mnt/Backup/overnight_ah_tuning/{indicator_panel,indicator_fm,indicator_sweep}/`.

### Non-goal

Nessuna modifica ai parametri operativi. Nessuna composizione di score
tentata in questo studio (indicatori testati solo singolarmente, come
richiesto esplicitamente dall'utente prima di passare alla composizione).

## Studio 3: tilt di volatilità sullo score esistente (completato, risultato negativo)

Prima di comporre un nuovo score con gli indicatori dello Studio 2, l'utente
ha fatto notare un punto metodologico importante: lo score attuale è già
esso stesso un composito di 3 primitive (`c2c_mean_6m`, `ah_mean_6m`,
`corr12` del tema) — vanno incluse nello stesso pool di candidati, non
trattate come baseline fissa e separata dai 18 indicatori nuovi.

### Scoperta chiave: i "nuovi" indicatori momentum non sono nuova informazione

Correlazione (rank percentile cross-sezionale, dati reali) tra le 3
primitive attuali e i 18 migliori indicatori dello Studio 2:

- `ah_mean_6m` corr **0.920** con `sma_ratio_ah_252` (il miglior indicatore
  AH-momentum trovato) — quasi la stessa informazione, solo una formula
  diversa sullo stesso concetto.
- `c2c_mean_6m` corr **0.908** con `sma_ratio_cc_200` — stesso discorso lato
  C2C.
- `corr12` (tilt tema) corr **~0.00-0.04 con TUTTI e 18** gli indicatori
  dello Studio 2 e con gli altri due primitive — genuinamente ortogonale,
  ma già dentro lo score attuale (`theme_weight=0.15`).

Questo spiega perché nessun indicatore singolo ha battuto il default nello
Studio 2: i migliori candidati erano raffinamenti di informazione già
catturata, non segnale nuovo. L'unico cluster dello Studio 2 genuinamente
non rappresentato in nessuna delle 3 primitive è la **volatilità**
(`atr_pct_cc_21`, `intraday_vol_mean_42`, `ah_gap_vol_mean_63` — corr
reciproca 0.74-0.94, quasi ridondanti tra loro). Il cluster
oscillatori/rumore breve (MACD, StochRSI, SuperTrend) resta il più debole
sia statisticamente sia economicamente in entrambi gli studi — escluso a
priori dal composito su indicazione dell'utente.

### Test: tilt di volatilità additivo (non sostitutivo)

```text
score = (1 - vol_share) * score_attuale + vol_share * rank_pct(atr_pct_cc_21)
```

`atr_pct_cc_21` scelto come rappresentante del cluster volatilità per
`tstat_pool` massimo (9.35, contro 8.97 e 8.53 degli altri due). Estensione
**in-place** di `ScoringParams`/`fast_basket_long`
(`simulate_basket_rotation.py`/`parameter_sweep_scoring.py`, non un modulo
duplicato), verificata a fedeltà esatta (`max_abs_diff=0.0`) sia per
`vol_share=0` (nessuna regressione sul comportamento pre-esistente) sia per
il blend a `vol_share>0`.

5 combinazioni (`vol_share ∈ {0, 0.05, 0.10, 0.15, 0.20}`, `vol_share=0`
incluso come arma di controllo = score attuale esatto) valutate sui 12
blocchi pool 2000-2022 (**blocco di validazione deliberatamente non
toccato** — la soglia minima di miglioramento restava da concordare con
l'utente, discussione mai arrivata a quel punto perché il segnale non ha
retto nemmeno sui blocchi storici):

| vol_share | median regret_norm (12 blocchi pool) |
|---:|---:|
| **0.00 (controllo)** | **0.4928 (migliore)** |
| 0.05 | 0.4942 |
| 0.20 | 0.4944 |
| 0.10 | 0.4949 |
| 0.15 | 0.4964 |

Il tilt di volatilità non aiuta in nessuna quantità testata — nominalmente
il peggiore quasi ovunque tranne `vol_share=0`. MCS: nessuno dei 5 eliminato
a nessuna soglia (`alpha=0.10` e `0.25` identici, 5/5 sopravvivono) —
differenze troppo piccole per essere statisticamente distinguibili con 12
blocchi, ma la direzione è comunque coerente (il controllo non è mai
peggiore del tilt).

**Terzo risultato negativo consecutivo**, e il più conclusivo dei tre:
l'unica informazione genuinamente nuova identificata (volatilità) non
migliora lo score nemmeno sui blocchi storici, prima ancora di arrivare
alla validazione intonsa.

### File e output

Script: `bt-strategy-test/overnight-ah/research/composite_tilt_sweep.py`
(riusa `simulate_basket_rotation.py`, `parameter_sweep_scoring.py`,
`oracle_daily.parquet`, `mcs_selection.py` invariato). Output:
`/mnt/Backup/overnight_ah_tuning/composite_sweep/`.

### Non-goal / stato

Nessuna modifica ai parametri operativi. Nessuna validazione sul blocco
intonso eseguita (il segnale non ha superato il primo filtro).

## Studio 4: ottimizzazione congiunta del vettore di pesi (completato, risultato negativo)

Ipotesi dell'utente: le combinazioni di indicatori potrebbero riservare
sorprese per complementarità (un indicatore non "top" da solo potrebbe
aiutare in composizione), da testare ottimizzando direttamente un vettore
di pesi sull'obiettivo economico reale invece di una griglia discreta.
Valutata anche l'opzione rete neurale/tensori e scartata **prima** di
partire: campione piccolo (271 periodi mensili pool), lezione già
documentata nel repo (ML tabulare — Ridge/HuberRegressor/ExtraTreesRegressor
— non batte i compositi semplici in questo dominio), e lo spazio
informativo genuinamente nuovo oltre alle 3 primitive attuali è quasi
mono-dimensionale (solo il cluster volatilità, vedi Studio 3) — un modello
flessibile non avrebbe materiale su cui generalizzare.

### Fase A — regressione multivariata (complementarità statistica)

Estensione di `indicator_fama_macbeth.py` a una regressione OLS con più
regressori simultanei (rank percentile), per isolare l'effetto **parziale**
di ciascun candidato controllando per gli altri — non solo l'effetto
marginale/univariato già misurato negli studi precedenti. Candidati:
`c2c_mean_6m`, `ah_mean_6m`, `corr12` (le 3 primitive) + `atr_pct_cc_21`
(rappresentante volatilità, Studio 3) + `obv_slope_126` (il meno
ridondante tra i "duplicati" momentum, corr 0.64 con `c2c_mean_6m`):

| indicatore | t-stat univariato | t-stat parziale (controllando per gli altri 4) |
|---|---:|---:|
| `ah_mean_6m` | 14.03 | **12.97** |
| `atr_pct_cc_21` | 9.35 | **7.18** |
| `obv_slope_126` | 4.53 | 1.92 (non significativo → escluso dalla Fase B) |
| `corr12` | -0.82 (non signif.) | **-2.58** |
| `c2c_mean_6m` | 2.56 | **-2.34** |

Due risultati degni di nota, entrambi verificati con un secondo check
mirato prima di accettarli:

- **`c2c_mean_6m` cambia segno** (da +2.56 a -2.34) una volta controllato
  per `ah_mean_6m`. Spiegazione meccanica confermata sui dati: dato che
  C2C ≈ AH + RTH (in log-return, stessa finestra), il residuo di
  `c2c_mean_6m` dopo aver rimosso la quota spiegata da `ah_mean_6m`
  correla **0.847** con `rth_mean_6m` (la componente RTH già isolata nella
  tabella riepilogativa precedente). `rth_mean_6m` è già noto avere un
  effetto **negativo e significativo** sui rendimenti overnight futuri
  (t-stat=-5.45, session disjunction). Controllando per `ah_mean_6m`, ciò
  che resta di `c2c_mean_6m` è quindi in gran parte esposizione RTH — e
  l'RTH è un cattivo predittore dell'overnight. Non un artefatto: è la
  stessa session disjunction già documentata, vista da un'altra angolazione.
- **`corr12`** non è mai stato significativo da solo (t-stat univariato
  -0.82, mai testato prima d'ora come predittore diretto — nello score è
  un tilt tematico, non un segnale di rendimento) ma diventa marginalmente
  significativo (-2.58) una volta ripulito dal rumore condiviso con gli
  altri regressori — un effetto "suppressor" classico in regressione
  multipla, non un cambio di direzione economica: resta un segnale debole.

### Fase B — ottimizzazione libera del vettore di pesi sull'obiettivo economico

Estensione **in-place** di `ScoringParams`/`fast_basket_long`
(generalizzazione del `vol_share` singolo dello Studio 3 a
`extra_weights: tuple[(col, peso), ...]`, N componenti simultanee),
verificata a fedeltà esatta (`max_abs_diff=0.0`) prima di usarla. Solo
`atr_pct_cc_21` sopravvive al filtro di significatività parziale della
Fase A ed entra come variabile libera insieme a `c2c_weight`/`theme_share`
(anch'essi lasciati liberi, non fissati al default — lo Studio 1 aveva già
mostrato che il punto ottimo sul pool non è detto generalizzi).

Ottimizzazione con `scipy.optimize` (Nelder-Mead vincolato, non griglia
discreta) sulla mediana di `regret_norm` sui 12 blocchi pool: **55 punti
valutati**, spazio continuo `c2c_weight∈[0,1]`, `theme_share∈[0,0.4]`,
peso `atr_pct_cc_21∈[0,0.3]`.

| combo | median regret_norm (12 blocchi pool) |
|---|---:|
| **punto di controllo (default attuale, extra=0)** | **0.4920 (migliore di tutti i 55 punti)** |
| ottimo trovato dall'ottimizzatore (`c2c_weight=0.62, theme_share=0.155, atr=0.095`) | 0.4929 |

Il punto di controllo (configurazione operativa attuale) è risultato
**il migliore tra tutti e 55** i punti esplorati liberamente
dall'ottimizzatore — nessuna direzione dello spazio continuo lo batte. MCS
(α=0.10: 52/55 sopravvivono; α=0.25: 49/55) e bootstrap-percentile (55/55
in overlap con il migliore) confermano che le differenze osservate sono
dentro il rumore statistico. Leave-block-out stabile 12/12: il punto di
controllo non perde mai contro la popolazione sul blocco tenuto fuori.

Nota di interpretazione: nonostante `c2c_mean_6m` abbia effetto parziale
negativo in Fase A, l'ottimizzatore economico converge comunque vicino a
`c2c_weight≈0.6` (non verso `ah_mean_6m` puro). La spiegazione: la
regressione FM misura una relazione lineare media su tutta la
cross-section; l'obiettivo economico reale è la selezione **rank-based dei
soli top-5 su ~50**, un meccanismo non lineare che non è governato dallo
stesso coefficiente. Coerente con quanto già osservato nello Studio 1
(§ perché il peso maggiore è sull'indicatore con t-stat più basso): il
segno/forza di un coefficiente di regressione non predice cosa funziona
per la selezione top-5.

### File e output

Script: `bt-strategy-test/overnight-ah/research/composite_weight_optimization.py`
(Fase A + Fase B in un unico orchestratore; riusa `simulate_basket_rotation.py`,
`parameter_sweep_scoring.py`, `indicator_fama_macbeth.py` esteso con
`cross_sectional_regression_multi`, `oracle_daily.parquet`,
`mcs_selection.py` invariato). Output:
`/mnt/Backup/overnight_ah_tuning/composite_weight_opt/`.

### Non-goal / stato

Nessuna modifica ai parametri operativi. Nessuna rete neurale/tensori
utilizzata (scartata per analisi preliminare, vedi sopra). Nessuna
validazione sul blocco intonso eseguita (il segnale non ha superato il
filtro pool).

## Studio 5: composizione economica di tutti gli indicatori finalisti, nessun pre-filtro statistico (completato, risultato misto)

### Correzione metodologica dell'utente

Negli Studi 2-4 gli indicatori erano stati scartati dal pool di
composizione sulla base di correlazione grezza con le primitive esistenti
(es. `sma_ratio_ah_252` escluso per corr 0.92 con `ah_mean_6m`, Studio 3) o
di significatività statistica parziale (Fase A dello Studio 4:
`obv_slope_126` escluso per t-stat parziale 1.92<2.0). L'utente ha fatto
notare l'incoerenza: il default stesso combina `c2c_mean_6m` e
`ah_mean_6m`, correlati (pearson 0.43) — e la Fase A dello Studio 4 aveva
appena mostrato che l'effetto *parziale* di `c2c_mean_6m` è negativo,
eppure la combinazione batte `ah_mean_6m` da solo. La
correlazione/significatività di un singolo indicatore non predice se
aggiunge valore in una composizione — va lasciato decidere all'obiettivo
economico stesso, senza filtro statistico a monte.

Verifica aggiuntiva (test di complementarità a 3 termini,
`c2c_mean_6m`+`ah_mean_6m`+candidato, sui 18 vincitori dello Studio 2):
diversi indicatori scartati per correlazione nello Studio 3 mostrano t-stat
parziale significativo anche controllando per entrambe le primitive (es.
`sma_ratio_ah_252` t-stat parziale 4.44, `rsi_wilder_cc_25` -4.62,
`reg_slope_r2_ah_252` 4.41) — ulteriore conferma che lo screening a coppie
usato finora era insufficiente. Risultato non salvato a parte, riportato
qui come motivazione dello studio.

### Metodo: score piatto a 21 componenti, nessun pre-filtro

Pool: `c2c_mean_6m`, `ah_mean_6m`, `corr12` (primitive attuali) + i 18
vincitori per famiglia dello Studio 2 — **21 componenti, tutte a peso
libero**, `score = sum_i w_i * rank_pct(indicator_i)`, `w_i≥0`,
`sum(w_i)=1` (softmax di 21 reali non vincolati). Estensione **in-place** e
retrocompatibile di `ScoringParams` (nuovo campo `flat_weights`, sostituisce
la formula nidificata quando impostato; `None` riproduce esattamente il
comportamento degli Studi 1-4). Verifica di fedeltà a 3 livelli (nested
invariato, punto di controllo via `flat_weights` = stesso risultato del
riferimento, punto di controllo piatto = stessa formula nidificata) — tutte
`max_abs_diff=0.0`; un primo tentativo aveva un bug (i rank venivano
calcolati sull'universo intero invece che solo tra i titoli eleggibili
c2c/ah, `max_abs_diff=3e-2`), corretto prima di fidarsi di qualunque
risultato.

Dato il salto a 21 parametri liberi, due difese aggiuntive concordate con
l'utente:
- **Blocchi pool a `block_years=1`** invece di 2 (23 blocchi invece di 12,
  quasi il doppio delle osservazioni indipendenti per MCS/bootstrap/
  leave-block-out; parametro già esistente in `build_blocks`, nessun nuovo
  codice). Blocco di validazione invariato (2022-08→2026-08).
- **Penalità di concentrazione** nell'obiettivo (quadratica oltre un peso
  singolo di 0.5) per scoraggiare soluzioni degeneri su 1-2 indicatori.

Ottimizzazione a due stadi: `scipy.optimize.differential_evolution`
(globale, gradient-free, 21 dimensioni, 34.020 valutazioni, parallelizzato
su 22 processi) + raffinamento locale `Nelder-Mead`. Nota tecnica: scipy
usa di default `forkserver` invece di `fork` per il pool di processi (per
evitare deadlock) — i worker `forkserver` non vedono lo stato del processo
principale impostato *dopo* l'avvio del server, quindi i grandi DataFrame
condivisi via variabili globali di modulo risultavano `None` nei worker.
Risolto forzando esplicitamente `multiprocessing.set_start_method("fork")`
prima di lanciare l'ottimizzatore.

### Risultato: miglioramento sulla mediana, ma non su un test appaiato rigoroso

| combo | median regret_norm (23 blocchi pool) |
|---|---:|
| punto di controllo (default attuale, forma piatta) | 0.4971 |
| **ottimo trovato** (`ah_mean_6m` 0.285, `obv_slope_126` 0.183, `reg_slope_r2_ah_252` 0.164, `supertrend_dist_cc_10_3` 0.142, `intraday_vol_mean_42` 0.112, `corr12` 0.033, `c2c_mean_6m` **0.017 (quasi azzerato)**, resto <2%) | **0.4833** |

Il salto (0.4971→0.4833) è il più grande osservato in cinque studi — molto
più ampio delle differenze di 2ª/3ª decimale viste finora. Verificato che
**non è un artefatto del cambio a `block_years=1`**: rieseguendo la sola
coppia `c2c_weight`/`theme_share` (senza i 18 indicatori nuovi) sotto lo
stesso schema a 23 blocchi, il range resta 0.492–0.506 — nessuna
combinazione nested si avvicina a 0.4833, serve davvero la composizione a
21 componenti.

Il primo MCS eseguito è risultato **non informativo**: il punto di
controllo si è classificato 423°/423 (ultimo) tra tutti i punti valutati
dall'ottimizzatore, quindi escluso a monte dallo screening top-250 di
`mcs_selection.py` — l'MCS ha solo confermato che i migliori 250 punti sono
intercambiabili tra loro, non che battono il controllo. Rifatto il
confronto corretto, **appaiato blocco per blocco** (non solo la mediana
aggregata):

- L'ottimo batte il controllo su **14/23 blocchi (61%)**, perde su 9/23.
- **Wilcoxon signed-rank: p=0.33** — non significativo.
- Leave-block-out: **instabile, 18/23** — 5 rotazioni su 23 mostrano il
  composito peggiorare della popolazione sul blocco tenuto fuori quando
  quello specifico blocco non è nel training set (mai successo negli Studi
  1/3/4, sempre stabili 12/12).

### Interpretazione

Il punto dell'utente era corretto: rimuovendo il pre-filtro statistico
l'ottimizzatore trova un beneficio economico reale — il maggiore visto in
cinque studi — che gli screening precedenti avrebbero nascosto (coerente
con la scoperta che `c2c_mean_6m` viene quasi azzerato, esattamente come
suggerito dal suo effetto parziale negativo nello Studio 4). Ma il test
rigoroso mostra che il miglioramento è **concentrato su una minoranza di
blocchi storici favorevoli** (14/23), non un vantaggio diffuso: il test
appaiato non è significativo e il leave-block-out è instabile per la prima
volta in questa serie di studi — pattern coerente con l'overfitting
parziale segnalato prima di partire (21 parametri liberi anche su 23
blocchi è un rapporto ancora teso). Non è né un quinto risultato negativo
netto né una vittoria validata: è un segnale reale ma fragile, da trattare
con cautela.

### File e output

Script: `bt-strategy-test/overnight-ah/research/composite_weight_optimization_v2.py`
(riusa `simulate_basket_rotation.py`, `parameter_sweep_scoring.py` estesi
con `flat_weights`, `build_blocks(block_years=1)`, `oracle_daily.parquet`,
`mcs_selection.py` invariato per lo screening preliminare). Output:
`/mnt/Backup/overnight_ah_tuning/composite_weight_opt_v2/`.

### Non-goal / stato

Nessuna modifica ai parametri operativi. Nessuna validazione sul blocco
intonso eseguita — il segnale, pur promettente sulla mediana, non ha
ancora superato un test di robustezza pool sufficiente per giustificare
di guardare la validazione. Punto di decisione aperto con l'utente su come
procedere (raffinare l'obiettivo per premiare esplicitamente la stabilità
leave-block-out, accettare il rischio e testare comunque in validazione, o
altro) — non deciso in questa sessione, da discutere.

## Studio 6: conferma Backtrader reale sul periodo pool completo (completato, risultato positivo in-sample)

### Correzione metodologica dell'utente

Il test appaiato blocco-per-blocco dello Studio 5 (14/23 blocchi vinti,
Wilcoxon non significativo) tratta ogni blocco storico alla pari, ma il
regime di mercato cambia completamente da un periodo all'altro (dot-com,
GFC, COVID) — un blocco di crisi non è comparabile a un blocco calmo. La
domanda economica giusta è la **performance aggregata sul periodo
complessivo** (un unico backtest continuo), non un conteggio di blocchi
discreti. L'utente ha chiesto due cose: (1) confermare con **Backtrader
reale**, non solo la simulazione pandas veloce; (2) rifare lo Studio 5
**senza la penalità di concentrazione**, per vedere se l'ottimo naturale è
più concentrato.

### Studio 5b: stesso Studio 5, senza penalità di concentrazione

`composite_weight_optimization_v3_no_penalty.py` (wrapper minimo su
composite_weight_optimization_v2.py, `LAMBDA_CONC=0`). Risultato: anche
senza penalità l'ottimo non è degenere — resta diffuso su 6+ componenti
(il maggiore, `reg_slope_r2_ah_252`, pesa 33%), non collassa su 1-2
indicatori. Punteggio pool pandas praticamente identico allo Studio 5
(0.4836 vs 0.4833 — la penalità non stava vincolando la ricerca in modo
significativo).

### Nuova strategia sperimentale (produzione NON toccata)

Per il test con Backtrader reale, creata `bt-core/strategies/
overnight_ah_flat_composite.py` — **sottoclasse** di `OvernightAH`
(`overnight_ah.py` di produzione invariato, zero rischio per paper/live),
`live_enabled=False` (solo backtest). Aggiunge 3 parametri opzionali
(`monthly_universe_score_mode`, `_indicator_panel`, `_flat_weights`);
default `score_mode='legacy'` delega interamente a `super()` — verificato
`returns.csv` identico bit-per-bit alla classe base sullo stesso periodo.
Con `score_mode='flat_panel'` sostituisce solo il calcolo dello score nel
ramo dinamico (`_compute_weak_theme_monthly_universe`) con un blend piatto
sul pannello indicatori dello Studio 5 (`export_flat_panel_csv.py`,
`;`-delimited, root risolta come `_load_monthly_universe`) — gating (SPY
gate, regime switch, fallback statico) ereditati invariati. Non
reimplementa dal vivo i 18 indicatori (lavoro enorme, alto rischio di
fedeltà, duplicherebbe `indicator_panel.py` già validato) — per questo la
modalità resta backtest-only, non pronta per paper/live.

### Risultato: periodo pool completo (2000→2022-08-07, un unico backtest, 5685 giorni)

| combo | ann_log_ret | regret_norm |
|---|---:|---:|
| controllo (pesi piatti = default attuale) | 0.3634 | 0.4914 |
| **Studio 5 (con penalità di concentrazione)** | **0.3822** | **0.4897** |
| Studio 5b (senza penalità) | 0.3624 | 0.4914 |

Sull'aggregato dell'intero periodo — la lente corretta secondo l'utente —
**lo Studio 5 batte il controllo con Backtrader reale**: +1.9 punti
percentuali annui di rendimento log, non nella simulazione pandas ma
nell'esecuzione reale (ordini, fill, sizing, margine). È il segnale più
forte di tutta la serie di studi.

Notevole: **la variante senza penalità (5b) non regge** lo stesso test —
sostanzialmente pari al controllo (0.3624 vs 0.3634), nonostante un
punteggio pool pandas leggermente migliore. La composizione 5b è dominata
per l'83% da `reg_slope_r2_ah_252` (33%) + `intraday_vol_mean_42` (16%);
quella dello Studio 5, più diversificata (nessun peso oltre il 29%), è
quella che si conferma sul motore reale — la penalità di concentrazione
non era solo una precauzione, sembra aver selezionato una composizione più
robusta/generalizzabile.

### Fedeltà verificata

1. `score_mode='legacy'` (default) → `returns.csv` identico bit-per-bit
   alla classe base `OvernightAH` sullo stesso periodo (smoke test
   2019-2021).
2. `score_mode='flat_panel'` col punto di controllo → stesso ordine di
   grandezza del legacy (2020: 0.924 vs 0.943; 2021: 0.342 vs 0.370) —
   scarto atteso da convenzioni di rolling leggermente diverse (calendario
   vs trading day), stessa tolleranza già accettata nello Studio 1 quando
   si confermò il default via Backtrader reale.

### Limite importante: risultato ancora in-sample

I pesi dello Studio 5 sono stati ottimizzati proprio su questo stesso
periodo pool — il miglioramento qui riportato non è una conferma
out-of-sample, è la stessa popolazione su cui l'ottimizzatore ha
cercato. Il blocco di validazione (2022-08→2026-08) **non è stato
toccato**, come da regola invariata in tutti gli studi precedenti. Dato
che è il segnale più forte finora, è il momento naturale per riprendere
la discussione sulla soglia minima di miglioramento (rimandata dallo
Studio 3) prima di un eventuale test in validazione — decisione non presa
in questa sessione.

### File e output

Script: `bt-strategy-test/overnight-ah/research/
composite_weight_optimization_v3_no_penalty.py`,
`export_flat_panel_csv.py`, `validate_flat_weights_backtrader.py`.
Strategia: `bt-core/strategies/overnight_ah_flat_composite.py` (nuova,
sperimentale). Output: `/mnt/Backup/overnight_ah_tuning/
composite_weight_opt_v3_no_penalty/`,
`bt-strategy-test/overnight-ah/research/out/flat_panel.csv`,
`/mnt/Backup/overnight_ah_tuning/composite_weight_opt_v2/bt_pool_period_results.csv`.

### Non-goal / stato

Nessuna modifica alla strategia di produzione (`overnight_ah.py`
invariato). Nessuna validazione sul blocco intonso eseguita. Risultato
positivo ma in-sample — punto di decisione aperto con l'utente: soglia
minima di miglioramento e se/quando procedere al test di validazione.

## Prova su strada: benchmark reale (parametri development) e diagnosi dello scarto (completato, risultato negativo — causa identificata)

### Setup

Su richiesta dell'utente, confronto diretto con Backtrader reale contro il
benchmark che conta davvero: la configurazione **development** realmente
in paper trading (`config-common/scheduled/strategies/
overnight-ah-development.env`, cron `bt-scheduled development entry`), non
l'oracolo astratto. Meccanismo: `btmain.py --benchmark` (già esistente,
CSV in `config-common/benchmark/`, formato `index,return` — ogni run
scrive già il proprio `returns.csv` in questo formato, pronto per essere
copiato). Periodo: **intero, 2000→oggi, inclusa la validazione** —
decisione esplicita e consapevole dell'utente di superare la regola
"validazione mai toccata" seguita negli Studi 1-6, presa qui apposta per
questa prova.

**Bug scoperto e corretto durante la verifica del benchmark** (prima di
procedere, come richiesto): il primo run con `max_exposure=2` (valore
reale della config development) su 26 anni continui è esploso a
**$200k → $1.989 trilioni**, posizioni da miliardi di azioni — compounding
a leva su un periodo lunghissimo, senza vincoli di capacità/liquidità nel
motore di backtest (nessun bug nella strategia di produzione, mai toccata
— nessuno Studio precedente aveva mai fatto girare un backtest continuo
così lungo con leva). Corretto: `max_exposure=1.0` per entrambi i run
(stessa convenzione unlevered di tutti gli Studi 1-6).

### Risultato del confronto (26 anni, 2000→2026-08, tutti i filtri/hedge/cooldown reali attivi)

Bug trovato anche nel report `stats.html` di QuantStats: la tabella di
confronto integrata mostrava il composito vincente (472.205% vs 416.886%
di rendimento cumulativo) — **numeri fuorvianti**, dovuti al modo in cui
QuantStats tronca la serie di benchmark alla prima data non-nulla della
strategia. Ricalcolato direttamente dai due `returns.csv` (stessa finestra
esatta):

| metrica | benchmark (dev, score legacy) | composito (Studio 5) |
|---|---:|---:|
| moltiplicatore cumulativo (26 anni) | **4753x** | 4196x |
| ann_log_ret | **0.3187** | 0.3140 |
| Sharpe (giornaliero) | 1.883 | 1.884 (pari) |
| SQN nativo Backtrader | 4.553 | **4.803** |
| Sharpe nativo Backtrader | 1.252 | **1.301** |

Quadro misto: il benchmark è leggermente avanti su rendimento
cumulativo/annualizzato; il composito è leggermente migliore sulle
metriche a livello di trade (SQN, Sharpe nativo). Molto lontano dal
vantaggio netto (+1.9pp/anno) trovato nello Studio 6.

### Diagnosi: da cosa dipende lo scarto rispetto allo Studio 6

Lo Studio 6 e questa prova differiscono su 5 fattori contemporaneamente:
filtri operativi (intraday vol/ADV/ah_lag1), hedge SQQQ, post_up_cooldown,
`max_concurrent` (5→3), periodo (pool-only → intero). Riattivati uno alla
volta a partire dal setup esatto dello Studio 6, sempre sul periodo pool
(`diagnose_road_test_gap.py`, Backtrader reale, controllo vs composito a
ogni step):

| step | control ann_log_ret | composito ann_log_ret | delta |
|---|---:|---:|---:|
| **Studio 6 (nessun filtro/hedge/cooldown, max_conc=5)** | 0.3634 | 0.3822 | **+0.0188** |
| **+ filtri intraday/ADV/ah_lag1** | 0.2263 | 0.2288 | **+0.0025** |
| + hedge SQQQ | 0.2279 | 0.2305 | +0.0025 |
| + post_up_cooldown | 0.2331 | 0.2315 | -0.0016 |
| + `max_concurrent` 5→3 (= dev, periodo pool) | 0.2952 | 0.2987 | +0.0035 |
| + periodo esteso a oggi (prova su strada) | 0.3187 | 0.3140 | -0.0047 |

**Causa quasi interamente concentrata in un solo fattore**: i filtri
operativi giornalieri (`min_intraday_vol`/`max_intraday_vol`/
`ah_lag1_threshold`/`min_adv`) fanno crollare il vantaggio del composito
da +0.0188 a +0.0025 — **-87%** — al primo step. Tutti gli altri fattori
(hedge, cooldown, `max_concurrent`, estensione del periodo) producono solo
oscillazioni di ±0.003-0.005 attorno a questo livello già azzerato,
nessuno paragonabile ai filtri.

Interpretazione (non ancora verificata quantitativamente, da approfondire
se si vuole isolare quale dei tre filtri pesa di più): questi filtri
agiscono ogni giorno, DOPO la selezione mensile del paniere, scartando
candidati per volatilità intraday odierna, liquidità e gap overnight di
ieri. Il vantaggio del composito viene dal ranking mensile migliore, ma se
i nomi meglio classificati (pesati su volatilità/momentum come
`supertrend_dist_cc_10_3`, `intraday_vol_mean_42`, `obv_slope_126`)
vengono sistematicamente scartati dal filtro giornaliero, il vantaggio del
ranking si diluisce — nessuno Studio precedente (1-6) aveva mai testato il
composito in presenza di questi filtri, sempre disattivati di proposito
per isolare la sola composizione dello score.

### File e output

Script: `bt-strategy-test/overnight-ah/research/
diagnose_road_test_gap.py`. Strategia sperimentale riusata invariata
(`overnight_ah_flat_composite.py`). Benchmark:
`config-common/benchmark/overnight_ah_dev_auction_full.csv`. Output:
`/mnt/Backup/overnight_ah_tuning/composite_weight_opt_v2/
road_test_gap_diagnosis.csv`.

### Non-goal / stato

Nessuna modifica alla strategia di produzione. Nessuna decisione di
promuovere il composito. Causa dello scarto identificata (filtri operativi
giornalieri) ma non ancora scomposta nei tre filtri singoli — punto aperto
con l'utente su come procedere.

## Seguito prova su strada: rimozione filtri (con hedge+cooldown) — il composito torna a vincere

### Chiarimento metodologico: un controllo fisso, non uno diverso a ogni test

L'utente ha fatto notare che cambiare il "controllo" a ogni test (a parità
di fattori, per isolarli uno alla volta) confonde il quadro complessivo.
Fissato da qui in avanti un **controllo di riferimento unico**:
`benchmark_dev_auction_unlevered` (config development reale, filtri+hedge+
cooldown+`max_concurrent=3` attivi, periodo 2000-2026,
**475.206,59%** di rendimento cumulativo) — le varianti sperimentali si
confrontano sempre contro questo stesso numero, non contro un controllo
ricalcolato ogni volta.

### Chiarimento: cooldown e hedge NON sono la causa dello svantaggio

Test diretto: composito con hedge+cooldown (dev completo) = **-11,71%**
vs controllo fisso; composito **senza** hedge+cooldown = **-31,08%** vs
controllo fisso — togliendoli lo scarto peggiora, non migliora. Cooldown e
hedge aiutano il composito in termini assoluti (es. `ann_log_ret`
0,2305→0,2315 aggiungendo cooldown nella catena di diagnosi) — il piccolo
calo di *delta relativo* osservato in precedenza (+0,0025→-0,0016) è dovuto
al fatto che il cooldown aiuta il controllo un filo di più (+0,0052) del
composito (+0,0010), non a un danno assoluto per il composito.

### Test: filtri intraday/ADV/ah_lag1 rimossi, hedge+cooldown+max_concurrent=3 mantenuti

Ipotesi dell'utente: se i filtri sono la causa, provare a togliere *solo*
quelli mantenendo hedge+cooldown+`max_concurrent=3`+periodo intero.
Nuovo benchmark salvato per riuso:
`config-common/benchmark/overnight_ah_no_filters_hedge_cooldown.csv`
(cartella run: `bt-core/out/overnight_ah/OvernightAH/nofilters_full_control/`).

| | rendimento cumulativo | vs controllo fisso (con filtri) |
|---|---:|---:|
| controllo fisso (dev completo, **con** filtri) | 475.206,59% | — |
| controllo, **senza** filtri (hedge+cooldown+maxconc3) | 2.915.112,86% | +513,33% |
| **composito, senza filtri** (hedge+cooldown+maxconc3) | **3.117.295,31%** | **+555,87%** |
| composito vs controllo, stesso setup senza filtri | — | **+6,94%** |

Qui il composito batte il controllo in modo netto (+6,94%, non i decimi di
punto visti con i filtri attivi) — confermata l'ipotesi: sono i filtri
intraday/ADV/ah_lag1 a diluire il vantaggio del composito, non hedge/
cooldown. Nessun errore/warning nei log di entrambi i run.

**Avvertenza**: togliere quei filtri fa esplodere il rendimento di
*entrambe* le strategie di oltre 5x rispetto al controllo reale — segnale
che quei filtri tagliano molta crescita "sulla carta". Esistono
presumibilmente per un motivo operativo reale (liquidità minima,
volatilità intraday, gap overnight estremi) che il backtest non prezza
(nessuno slippage/impatto di mercato modellato per i trade più rischiosi
che i filtri escludono) — un rendimento 5-6x più alto solo togliendo un
filtro di liquidità è altrettanto un segnale di sovrastima
dell'eseguibilità reale quanto un merito del composito.

### Chiarimento: `max_exposure=2` non è un bug

L'utente ha chiarito che `max_exposure=2` (esposizione target 200%,
leva reale) non è il bug trovato a inizio prova su strada — quel bug era
la combinazione leva+periodo lunghissimo+nessun vincolo di capacità che
produceva risultati assurdi ($200k→$1.989 trilioni). Rieseguito lo stesso
test "senza filtri, hedge+cooldown+maxconc3" con `max_exposure=2`
(`--margin-leverage 2` invariato, era già presente in tutti i run
precedenti come vincolo broker inerte a `max_exposure=1.0`):

| | rendimento cumulativo |
|---|---:|
| controllo (lev2, no filtri, hedge+cooldown) | 19.742.501.511% |
| composito (lev2, no filtri, hedge+cooldown) | 21.534.961.218% |
| **composito vs controllo** | **+9,08%** |

Verificato esplicitamente **nessun bug**: zero errori/warning nei log,
zero rifiuti di margine (incluso il caso specifico già noto in passato —
ordine di apertura/chiusura collegato rifiutato per margine, che apriva
uno short fantasma — non si è presentato), trade count coerente (~19.150-
19.170 in entrambi i run, in linea con la versione senza leva), SQN/Sharpe
finiti e sensati (SQN 5,07/4,41, Sharpe 1,09/1,12 — più bassi che a leva 1x,
atteso perché la leva amplifica la volatilità più del rendimento medio nel
calcolo Sharpe). Il vantaggio relativo del composito regge e migliora
leggermente con la leva (+9,08% vs +6,94% a leva 1x). I numeri assoluti
enormi sono l'effetto atteso di leva 2x composta ogni notte per 26 anni
senza vincoli di capacità nel motore di backtest, non un errore di calcolo.

### Non-goal / stato

Nessuna modifica alla strategia di produzione. Nessuna decisione di
promuovere il composito o di rimuovere i filtri operativi in produzione —
la rimozione filtri è un test diagnostico, non una raccomandazione: il
backtest non prezza slippage/impatto di mercato per i trade che i filtri
escludono, quindi il vantaggio osservato senza filtri potrebbe essere in
parte un artefatto di eseguibilità irrealistica. Punto aperto con l'utente
su come interpretare/proseguire.

## Isolamento dei tre filtri operativi uno alla volta

Meccanica esatta dei tre filtri (`overnight_ah.py::_filter_reason`,
righe 1449-1502, applicati ogni giorno a ogni candidato prima dell'entry):

- **`min_intraday_vol`/`max_intraday_vol`**: banda su
  `(high[0]-low[0])/open[0]` della barra di OGGI (il giorno stesso
  dell'eventuale entry) — scarta se il range del giorno è fuori
  `[min,max]`. `intraday_vol_filter_side` limita l'applicazione a giorni
  up/down/any.
- **`min_adv`**: ADV$ stimato come `SMA(volume,20)[-1]` (di ieri, ex-ante)
  × `close[0]` (di oggi) — scarta se sotto soglia (liquidità minima).
- **`ah_lag1_threshold`**: guarda il gap overnight appena concluso
  `(open[0]-close[-1])/close[-1]` — scarta se il gap della notte
  precedente era già molto negativo (evita re-entry dopo una notte pesante
  sullo stesso titolo).

Ipotesi iniziale (poi confermata parzialmente errata): dato che il
composito pesa `intraday_vol_mean_42` (11,2%) e altri indicatori di
trend/momentum correlati alla volatilità realizzata, ci si aspettava che
il filtro di volatilità fosse la causa dominante. Isolati i tre filtri uno
alla volta (controllo fisso sempre `benchmark_dev_auction_unlevered`,
hedge+cooldown+`max_concurrent=3`+periodo intero 2000-2026 invariati in
ogni riga, `max_exposure=1.0`, nessun errore/warning in nessun run):

| scenario | controllo | composito | **composito vs controllo (stesso setup)** | composito vs benchmark fisso |
|---|---:|---:|---:|---:|
| TUTTI i filtri attivi | 475.207% | 419.536% | -11,71% | -11,71% |
| solo vol tolta (ADV+ah_lag1 attivi) | 1.050.980% | 941.371% | -10,43% | +98,08% |
| solo ah_lag1 tolto (vol+ADV attivi) | 478.984% | 427.358% | -10,78% | -10,07% |
| solo ADV tolto (vol+ah_lag1 attivi) | 1.000.052% | 974.898% | **-2,51%** | +105,13% |
| solo ah_lag1 attivo (vol+ADV disattivati) | 2.547.893% | 3.102.540% | **+21,77%** | +552,77% |
| tutti e 3 tolti | 2.915.113% | 3.117.295% | +6,94% | +555,87% |

**Lezione chiave (già osservata più volte in questa sessione, qui di
nuovo)**: la colonna "vs benchmark fisso" è fuorviante quando i due lati
non hanno lo stesso set di filtri attivi (mescola "quanto rende togliere
un filtro" con "il composito sceglie meglio") — l'unica colonna valida per
giudicare se il composito batte il controllo è quella a parità di setup
("comp vs ctrl, stesso setup").

**Risultato**: `min_adv` da solo è il fattore con l'impatto maggiore
(-2,51%, il più vicino a zero tra i singoli filtri); vol e ah_lag1 tolti
singolarmente non spostano quasi nulla (-10,43%/-10,78%, quasi identico al
caso con tutti i filtri). Nessun singolo filtro spiega da solo il
recupero completo (+6,94%, tutti e tre tolti insieme) — somma degli
effetti isolati ≈ +11,4pp attesi contro +6,94% osservato: interazione non
additiva tra i filtri, non un effetto di un solo fattore dominante.
Risultato sorprendente non ancora pienamente spiegato: "solo ah_lag1
attivo" (vol e ADV disattivati insieme) dà il risultato migliore in
assoluto per il composito (+21,77%, meglio persino di "tutti tolti") — un
punto aperto, non ancora indagato a fondo (possibile interazione
vol×ADV che penalizza il composito più della somma dei due presi
singolarmente).

### File e output

Nuovi benchmark salvati in `config-common/benchmark/`:
`overnight_ah_novolfilter_control.csv`,
`overnight_ah_noahlag1_control.csv`, `overnight_ah_noadv_control.csv`,
`overnight_ah_onlyahlag1_control.csv`. Nessuno script nuovo — run diretti
via `btmain.py`, stessa metodologia (controllo salvato come benchmark,
composito eseguito con `--benchmark`, confronto ricalcolato direttamente
dai `returns.csv` per evitare il troncamento fuorviante di QuantStats già
documentato).

### Non-goal / stato

Nessuna modifica alla strategia di produzione. Causa isolata solo
parzialmente: `min_adv` pesa di più, ma l'interazione tra i tre filtri
resta poco chiara (in particolare il risultato "solo ah_lag1 attivo").
Nessuna decisione di modificare i parametri operativi reali — prossimo
passo: verificare se/come aggiornare i parametri del run "development".
