# RTH Open/Close Context

Data sintesi: 2026-06-10

## Obiettivo

Studiare una strategia RTH long-only che compra all'open della sessione
regolare e vende al close della stessa seduta.

Il focus e' separato da AH / overnight:

- rendimento da regular open a regular close;
- selezione simboli ex-ante da paniere Nasdaq 100;
- ordinamento operativo della shortlist;
- filtri sul comportamento della sera precedente e della notte precedente;
- validazione IS/OOS.

## Strategia Base

Baseline statica iniziale:

```bash
cd /home/htpc/backtrader/bt-core
.venv/bin/python btmain.py \
  --strat generic.BuyOpenSellClose \
  --ticker=rth_stable_candidates_10.json \
  --fromdate=2000-01-01 \
  --todate=2026-01-01 \
  --timeframe=daily \
  --commission=none \
  --margin-rate 0.1 \
  --margin-leverage 1
```

Risultato indicativo:

| Caso | TimeReturn | Final Value | Sharpe BT | Sharpe daily | MaxDD | Trades |
|---|---:|---:|---:|---:|---:|---:|
| static `rth_stable_candidates_10` | 58.00x | ~11.8M | 1.011 | ~1.22 | -25% | 62,915 |

Questa baseline non e' una regola operativa: la lista e' stata scelta
guardando l'intero storico ed e' quindi contaminata da selection bias.

## Strategia Sandbox

E' stata aggiunta una clone giocabile:

```txt
bt-core/strategies/rth_open_close.py
```

Classe:

```txt
rth_open_close.RTHOpenClose
```

La strategia:

- compra al prossimo open;
- vende al close della stessa seduta con ordine `coc=True`;
- supporta `monthly_universe_file`;
- preserva l'ordine della lista mensile;
- usa `max_concurrent` come numero finale di titoli tradati;
- supporta filtri su overnight precedente, RTH precedente, range, posizione del close e statistiche RTH rolling.

Parametri di filtro principali:

```txt
min_prev_overnight_ret / max_prev_overnight_ret
min_prev_intraday_ret / max_prev_intraday_ret
min_prev_range / max_prev_range
min_prev_close_position / max_prev_close_position
min_rth_mean / min_rth_sharpe
```

## Selezione Mensile Ex-Ante

Script:

```txt
bt-strategy-test/rth_open_close/monthly_rth_universe_file.py
```

Formato output:

```txt
year;month;symbols
```

Processo:

1. Per il mese M si usano solo barre daily precedenti a M.
2. Si seleziona un filtro largo `top_n` dal Nasdaq 100.
3. Si ordina il filtro largo con una seconda finestra/metrica.
4. La strategia usa i primi `max_concurrent` simboli della lista ordinata.

Terminologia da mantenere chiara:

- `top_n`: ampiezza del filtro largo mensile, ad esempio 10/20/30;
- `max_concurrent`: numero finale di titoli tradati ogni giorno, ad esempio 5;
- se `top_n == max_concurrent`, l'ordinamento non serve.

## Metriche Di Selezione Testate

Metriche disponibili nello script:

```txt
mean_bps
total_return
sharpe
sortino
win_rate
composite
```

Prima evidenza:

- `win_rate` e' risultata la metrica piu' interessante come filtro largo;
- Sharpe non e' risultato un selettore convincente;
- il lookback corto di ordinamento va confrontato anche con il suo inverso;
- l'ordinamento discendente ha battuto l'ordinamento inverso in modo coerente nei test fatti.

## Migliore Famiglia Prima Dei Filtri Giornalieri

Setup piu' interessante prima dei filtri contestuali:

```txt
filtro largo: 36m win_rate
top_n: 30
ordinamento: 3m sortino desc
max_concurrent: 5
```

File mensile:

```txt
bt-strategy-test/rth_open_close/out/monthly_rth_universe_36m_win_rate_wide30_order3m_sortino_desc.csv
```

Risultato:

| Caso | Full CAGR | Full MaxDD | Full Sharpe | OOS 2019 CAGR | OOS MaxDD | OOS Sharpe | Recent 2022 CAGR | Recent Sharpe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| wide30 / order 3m sortino / pick 5 | 13.87% | -33.83% | 0.841 | 16.32% | -18.86% | 0.946 | 11.72% | 0.719 |

Confronti importanti:

| Caso | Full CAGR | Full MaxDD | Full Sharpe | OOS 2019 CAGR | OOS MaxDD | OOS Sharpe | Recent 2022 CAGR | Recent Sharpe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| wide10 long base | 15.00% | -33.69% | 0.883 | 16.27% | -25.42% | 0.882 | 12.75% | 0.734 |
| wide30 order3m sortino | 13.87% | -33.83% | 0.841 | 16.32% | -18.86% | 0.946 | 11.72% | 0.719 |
| wide20 order3m composite | 13.77% | -32.68% | 0.820 | 13.67% | -27.75% | 0.785 | 12.78% | 0.730 |

Interpretazione:

- `wide10 long base` e' forte sul periodo intero, ma OOS ha drawdown peggiore;
- `wide30 order3m sortino` e' meno brillante sul full sample, ma piu' pulito OOS;
- `wide20 order3m composite` non batte chiaramente gli altri due.

## Filtri Contestuali Giornalieri

Setup base per questi test:

```txt
monthly_universe_file='bt-strategy-test/rth_open_close/out/monthly_rth_universe_36m_win_rate_wide30_order3m_sortino_desc.csv'
max_concurrent=5
```

Risultati ordinati per qualita' OOS 2019:

| Contesto | Trades | Full CAGR | Full MaxDD | Full Sharpe | OOS 2019 CAGR | OOS MaxDD | OOS Sharpe | Recent 2022 CAGR | Recent MaxDD | Recent Sharpe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `prev overnight <= +2%` | 30,663 | 14.17% | -31.92% | 0.865 | 17.64% | -17.45% | 1.035 | 12.58% | -17.45% | 0.777 |
| `prev RTH >= -2%` | 30,617 | 13.27% | -38.41% | 0.821 | 16.44% | -16.14% | 0.967 | 14.37% | -16.14% | 0.865 |
| baseline candidato | 30,700 | 13.87% | -33.83% | 0.841 | 16.32% | -18.86% | 0.946 | 11.72% | -18.86% | 0.719 |
| `prev close pos >= 20%` | 30,305 | 9.75% | -37.31% | 0.627 | 16.21% | -19.55% | 0.942 | 15.02% | -19.55% | 0.880 |
| `prev overnight [-2%, +2%]` | 30,591 | 13.54% | -30.90% | 0.845 | 14.94% | -22.95% | 0.918 | 12.20% | -16.91% | 0.764 |
| `prev range <= 4%` | 30,282 | 12.54% | -31.15% | 0.850 | 13.66% | -14.91% | 0.894 | 12.29% | -13.57% | 0.805 |
| `prev range <= 5%` | 30,499 | 13.65% | -31.50% | 0.876 | 14.32% | -18.11% | 0.890 | 12.71% | -16.64% | 0.797 |
| `overnight band + range <= 5%` | 30,381 | 12.96% | -26.09% | 0.852 | 12.88% | -19.45% | 0.840 | 10.43% | -16.92% | 0.688 |
| `prev overnight >= -2%` | 30,632 | 13.40% | -33.58% | 0.828 | 13.73% | -24.01% | 0.833 | 11.60% | -17.35% | 0.718 |
| `20d RTH mean >= 0` | 29,801 | 11.74% | -39.13% | 0.763 | 13.15% | -19.08% | 0.808 | 8.06% | -19.08% | 0.539 |
| `20d RTH sharpe >= 0` | 29,801 | 11.74% | -39.13% | 0.763 | 13.15% | -19.08% | 0.808 | 8.06% | -19.08% | 0.539 |
| `band + range <= 5% + RTH mean >= 0` | 29,048 | 10.36% | -33.30% | 0.749 | 11.55% | -16.24% | 0.807 | 10.64% | -15.80% | 0.738 |
| `prev close pos >= 70%` | 24,203 | 3.32% | -43.45% | 0.297 | 8.33% | -23.43% | 0.605 | 10.20% | -14.66% | 0.725 |
| `prev RTH >= 0` | 28,621 | 5.20% | -42.84% | 0.388 | 8.01% | -22.38% | 0.537 | 6.22% | -22.38% | 0.440 |
| `prev overnight >= 0` | 27,436 | 2.77% | -44.06% | 0.250 | 0.33% | -41.68% | 0.103 | 4.10% | -20.60% | 0.323 |

Interpretazione:

- Il miglior filtro finora e' `prev overnight <= +2%`: evita gap/overnight troppo positivi prima dell'ingresso RTH.
- Comprare dopo overnight positivo non funziona in questa forma: `prev overnight >= 0` degrada molto.
- `prev RTH >= -2%` e' interessante come filtro anti-seduta precedente molto negativa, soprattutto dal 2019/2022.
- `prev range <= 4%` e' la variante piu' difensiva: meno ritorno, ma drawdown OOS migliore.
- `20d RTH mean >= 0` e `20d RTH sharpe >= 0` non migliorano il setup.

## Candidato Corrente

Comando candidato:

```bash
cd /home/htpc/backtrader/bt-core
.venv/bin/python btmain.py \
  --strat rth_open_close.RTHOpenClose \
  --ticker=NASDAQ_100_US.json \
  --fromdate=2000-01-01 \
  --todate=2026-01-01 \
  --timeframe=daily \
  --commission=none \
  --margin-rate 0.1 \
  --margin-leverage 1 \
  --stratargs "monthly_universe_file='bt-strategy-test/rth_open_close/out/monthly_rth_universe_36m_win_rate_wide30_order3m_sortino_desc.csv' max_concurrent=5 max_prev_overnight_ret=0.02" \
  --id rth_ctx_no_gap_up
```

## Sweep Contesti 2026-06-10

Output riepilogo:

```txt
bt-strategy-test/rth_open_close/out/rth_context_sweep_summary.csv
```

Sono stati testati 46 scenari sul candidato:

```txt
filtro largo: 36m win_rate
top_n: 30
ordinamento: 3m sortino desc
max_concurrent: 5
```

Famiglie testate:

- soglia massima su overnight precedente;
- soglia massima su range precedente;
- combinazioni overnight + range;
- combinazioni overnight + RTH precedente;
- posizione del close nel range della seduta precedente;
- ranking giornaliero alternativo dentro la lista mensile;
- riapplicazione del filtro overnight su `wide10 long` e `wide20 composite`.

Migliori righe ordinate per Sharpe OOS 2019:

| Contesto | Trades | Full CAGR | Full MaxDD | Full Sharpe | OOS 2019 CAGR | OOS MaxDD | OOS Sharpe | Recent 2022 CAGR | Recent MaxDD | Recent Sharpe |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `overnight <= 0%` | 26,765 | 11.74% | -36.99% | 0.764 | 18.02% | -15.02% | 1.089 | 9.17% | -15.02% | 0.599 |
| `ov<=2% + closepos>=20%` | 30,232 | 10.45% | -34.80% | 0.670 | 18.05% | -17.86% | 1.059 | 16.54% | -17.86% | 0.942 |
| `overnight <= +2%` | 30,663 | 14.17% | -31.92% | 0.865 | 17.45% | -17.45% | 1.035 | 12.75% | -17.45% | 0.777 |
| `daily rank low overnight` | 30,693 | 11.19% | -47.99% | 0.630 | 21.68% | -21.69% | 1.034 | 16.76% | -21.69% | 0.768 |
| `ov<=2% + prev RTH>=-2%` | 30,561 | 13.00% | -33.58% | 0.816 | 16.91% | -15.11% | 1.023 | 14.23% | -15.11% | 0.862 |
| `ov<=2% + prev RTH>=-3%` | 30,634 | 13.58% | -32.32% | 0.844 | 17.04% | -16.57% | 1.021 | 15.50% | -16.57% | 0.921 |
| `overnight <= +3%` | 30,688 | 14.03% | -33.57% | 0.855 | 16.81% | -18.86% | 0.992 | 12.93% | -18.86% | 0.780 |
| `range <= 6%` | 30,588 | 14.06% | -36.67% | 0.882 | 15.98% | -16.99% | 0.978 | 13.41% | -16.04% | 0.815 |
| `prev RTH >= -2%` | 30,617 | 13.27% | -38.41% | 0.821 | 16.25% | -16.14% | 0.967 | 14.54% | -16.14% | 0.865 |
| baseline | 30,700 | 13.87% | -33.83% | 0.841 | 16.13% | -18.86% | 0.946 | 11.89% | -18.86% | 0.719 |

Lettura:

- `overnight <= 0%` e' il migliore per Sharpe OOS, ma peggiora il full sample e il periodo recente. Puo' essere un regime filter, non ancora il candidato principale.
- `overnight <= +2%` resta il miglior compromesso: migliora OOS e full sample senza tagliare troppo il numero di trade.
- `ov<=2% + closepos>=20%` e' molto forte dal 2019 e dal 2022, ma il full sample e' debole. Va trattato come ipotesi regime recente.
- `ov<=2% + prev RTH>=-2%/-3%` migliora OOS/recent e riduce drawdown OOS; il compromesso `-3%` sembra piu' equilibrato del `-2%`.
- I filtri range migliorano il profilo di rischio, ma non battono `overnight <= +2%` come candidato principale.
- Il ranking giornaliero per overnight basso ha OOS alto ma full drawdown pessimo; non e' robusto.
- Riapplicare `overnight <= +2%` su `wide10 long` o `wide20 composite` non batte il candidato `wide30/order3m_sortino` in OOS.

Tre candidati da tenere vivi:

| Uso | Contesto | Motivazione |
|---|---|---|
| principale | `overnight <= +2%` | miglior compromesso full/OOS/recent |
| aggressivo recente | `ov<=2% + closepos>=20%` | OOS/recent molto forte, ma sospetto regime-dipendente |
| difensivo | `ov<=2% + prev RTH>=-3%` | OOS/recent forte con drawdown OOS migliore della baseline |

## Studio Stop Loss 2026-06-18

Motivazione:

- ipotesi: un trade RTH che parte bene tende ad avere poca escursione sotto
  l'open;
- se l'escursione ribassista intraday e' ampia, e' meno probabile che il trade
  chiuda bene;
- test iniziale: simulare SL con daily OHLC, senza ancora cambiare la strategia
  Backtrader.

Script:

```txt
bt-strategy-test/rth_open_close/rth_stop_loss_study.py
```

Output:

```txt
bt-strategy-test/rth_open_close/out/rth_stop_loss_study_summary.csv
bt-strategy-test/rth_open_close/out/rth_stop_loss_study_trades.csv
```

Convenzioni operative:

- usare `bt-core/.venv/bin/python`;
- per sweep storici RTH usare il dataset standard
  `config-common/data/d/yahoo/*.csv`;
- i parquet `bt-strategy-test/RTH_analysis/out/` sono dataset feature-engineering e possono
  coprire finestre piu' corte.

Setup testato:

```txt
monthly_universe_file = bt-strategy-test/rth_open_close/out/monthly_rth_universe_36m_win_rate_wide30_order3m_sortino_desc.csv
max_concurrent = 5
max_exposure = 0.90
max_prev_overnight_ret = 0.02
stop_loss_pcts = none, 0.5%, 0.75%, 1%, 1.25%, 1.5%, 2%, 2.5%, 3%
```

Logica simulazione:

- segnale valutato a close del giorno T;
- ingresso simulato all'open del giorno di mercato successivo;
- se `low/open - 1 <= -SL`, rendimento trade = `-SL`;
- altrimenti rendimento trade = `close/open - 1`;
- capitale giornaliero allocato come `max_exposure / max_concurrent` per trade,
  coerente con la sizing policy del candidato.

Risultati principali:

| Scenario | Full CAGR | Full MaxDD | Full Sharpe | OOS 2019 CAGR | OOS MaxDD | OOS Sharpe | Recent 2022 CAGR | Recent MaxDD | Recent Sharpe | Stop hit OOS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| no stop | 14.47% | -31.99% | 0.885 | 17.28% | -17.49% | 1.044 | 12.18% | -17.49% | 0.787 | 0.00% |
| SL 0.5% | 4.13% | -23.50% | 0.546 | 0.48% | -14.57% | 0.099 | 0.25% | -12.12% | 0.071 | 69.53% |
| SL 0.75% | 6.21% | -23.89% | 0.660 | 5.73% | -15.62% | 0.605 | 6.52% | -12.53% | 0.681 | 55.48% |
| SL 1.0% | 7.71% | -20.95% | 0.719 | 8.87% | -14.39% | 0.806 | 8.22% | -14.39% | 0.758 | 44.01% |
| SL 1.25% | 9.21% | -24.45% | 0.783 | 12.14% | -18.73% | 0.989 | 10.85% | -15.38% | 0.910 | 34.25% |
| SL 1.5% | 9.27% | -22.20% | 0.747 | 11.63% | -19.19% | 0.915 | 10.22% | -14.99% | 0.833 | 27.38% |
| SL 2.0% | 10.25% | -27.42% | 0.757 | 12.44% | -17.10% | 0.902 | 11.69% | -16.00% | 0.865 | 17.46% |
| SL 2.5% | 10.82% | -30.46% | 0.760 | 13.22% | -17.29% | 0.911 | 12.64% | -17.19% | 0.894 | 11.27% |
| SL 3.0% | 10.77% | -34.56% | 0.735 | 12.74% | -17.09% | 0.857 | 12.28% | -15.66% | 0.844 | 7.63% |

Lettura:

- nessuno stop batte il candidato senza stop su CAGR o Sharpe OOS 2019;
- stop stretti 0.5%-1.0% tagliano troppi trade: hit rate OOS 44%-70% e
  rendimento molto degradato;
- SL 1.25%-1.5% migliora il profilo recent rispetto agli stop piu' stretti, ma
  resta sotto al no-stop OOS e full sample;
- SL 2.5%-3.0% e' quasi un filtro di coda: riduce poco il rischio OOS ma non
  compensa la perdita di convexity positiva;
- la tesi "molta escursione in basso chiude male" e' plausibile come diagnostica
  MAE, ma lo stop hard al primo attraversamento del low/open non migliora il
  candidato in questa simulazione daily.

Conclusione provvisoria:

- non promuovere uno SL fisso come regola principale;
- usare lo studio trade-level per analizzare MAE/MFE e cercare filtri ex-ante
  sulla probabilita' di drawdown intraday, invece di tagliare meccanicamente
  dopo il movimento;
- se si vuole testare ancora lo SL, prossima variante sensata: stop condizionale
  solo su regime recente o su sottoinsiemi con `prev RTH`/`closepos` deboli.

## Prossimi Test

Priorita':

1. Stressare i tre candidati su split temporali piu' severi, ad esempio train fino a 2018, OOS 2019-2021, recent 2022-2026.
2. Verificare se `ov<=2% + closepos>=20%` e' overfit recente o segnale reale.
3. Testare cost stress semplificato in bps per trade, prima di modellare nel dettaglio reg fees / execution.
4. Costruire confronto contro benchmark BO-SC giornaliero sullo stesso universo/opportunity set.
5. Aggiungere un regime filter di mercato solo quando sara' disponibile un proxy daily affidabile, ad esempio QQQ.

Regola pratica:

- usare sempre parallelismo con tutti i core disponibili meno 2;
- sulla macchina corrente `nproc=24`, quindi usare `-P 22` per gli sweep esterni.

## Rischi Aperti

- La selezione e' ora ex-ante mensile, ma resta un processo ottimizzato su storico.
- Mancano costi reali di esecuzione: spread/slippage/reg fees non sono ancora modellati.
- Il confronto contro benchmark RTH daily va mantenuto, per evitare di premiare filtri che semplicemente riducono esposizione.
- QQQ non e' disponibile nei dati daily locali usati finora, quindi non e' stato ancora testato un regime filter di mercato.
