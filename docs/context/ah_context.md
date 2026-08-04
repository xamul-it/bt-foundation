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
