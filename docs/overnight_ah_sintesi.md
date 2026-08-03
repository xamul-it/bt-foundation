# OvernightAH - sintesi test e risultati

Data sintesi: 2026-05-21

## Obiettivo

Valutare se la strategia `OvernightAH` ha problemi strutturali e, soprattutto, se la lista statica `stable_ah_top10` introduce lookahead/cherry-picking.  
Il punto centrale emerso e che una lista scelta guardando l'intero storico non e una regola operativa robusta. Serve una procedura che generi nel tempo la lista migliore usando solo dati disponibili ex-ante.

## Parametri base usati nei test

Parametri OvernightAH principali:

```text
max_concurrent=5
min_intraday_vol=0.025
max_intraday_vol=0.045
ah_lag1_threshold=-0.1
max_adv_participation=0.0025
max_exposure=2
min_price=0
min_adv=100000000
```

Note:

- `min_price` e stato considerato poco informativo perche assoluto.
- Il filtro ADV e risultato piu utile e coerente.
- In backtest e stato corretto l'uso ADV per evitare lookahead: il sizing/liquidity cap usa il volume medio disponibile sul bar precedente.

## Filtro ADV

E' stata fatta una sweep su `min_adv` mantenendo `min_price=0`.

Risultato Backtrader con la lista corrente e parametri operativi:

| min_adv | Final Value | TimeReturn | Trades | Sharpe | Worst year |
|---:|---:|---:|---:|---:|---:|
| 0 | 24.15M | 119.74x | 13245 | 1.256 | -20.10% |
| 50M | 33.44M | 166.20x | 11743 | 1.550 | -16.76% |
| 100M | 34.73M | 172.67x | 10369 | 1.595 | -16.63% |
| 150M | 34.85M | 173.24x | 9553 | 1.520 | -16.53% |
| 200M | 32.70M | 162.48x | 8788 | 1.330 | -18.48% |

Conclusione: `min_adv=100000000` e il compromesso preferibile. `150M` aumenta leggermente il ritorno finale ma peggiora Sharpe.

Output collegati:

- `bt-strategy-test/overnight-ah/research/out/adv_sweep_current_summary.csv`
- `bt-strategy-test/overnight-ah/research/out/adv_sweep_nasdaq_summary.csv`

## Problema lookahead della lista statica

La lista `stable_ah_top10` performa molto bene:

| Strategia | Total | Sharpe | MaxDD | Avg posizioni |
|---|---:|---:|---:|---:|
| `static_stable` / `stable_order` | +39,108% | 1.866 | -29.9% | 2.80 |

Pero questa lista e stata scelta guardando il passato completo. Quindi puo essere una forma di lookahead: non dimostra che nel 2010, 2015 o 2020 avremmo scelto proprio quei simboli.

Per questo sono stati costruiti test walk-forward su Nasdaq 100.

## Walk-forward mensile: generazione lista ex-ante

Metodo:

1. Universo iniziale: Nasdaq 100.
2. A fine mese si calcola una classifica usando solo dati storici disponibili fino a quel mese.
3. La lista generata viene usata solo nel mese successivo.
4. Ogni mese la lista viene aggiornata.
5. Ogni giorno la strategia seleziona max 5 simboli tra quelli della shortlist che passano i filtri OvernightAH.

Questa e la simulazione corretta per evitare lookahead.

### Migliore regola semplice trovata

La migliore regola non-lookahead finora:

```text
6m total + hysteresis
```

Regola dettagliata:

```text
Ogni fine mese:
- calcolo per ogni simbolo Nasdaq 100 la performance OvernightAH degli ultimi 6 mesi
- metrica = edge totale, non Sharpe medio
- richiedo almeno 20 trade
- creo una top 10
- hysteresis:
  - tengo un simbolo gia in lista se resta entro rank 20
  - faccio entrare un nuovo simbolo solo se entra entro rank 10
- nel mese successivo opero solo su questa lista
- la selezione giornaliera ordina i candidati per ADV
```

Risultato:

| Strategia | Total | Sharpe | MaxDD | Avg posizioni | Turnover |
|---|---:|---:|---:|---:|---:|
| `rotation_total` 6m + hysteresis 20/10 | +11,139% | 1.637 | -34.3% | 3.62 | 1.30 nuovi/mese |
| `rotation_total` 6m senza hysteresis | +9,943% | 1.607 | -34.4% | 3.59 | 2.69 nuovi/mese |
| `rotation_sharpe` 6m senza hysteresis | +7,439% | 1.639 | -31.4% | 3.25 | 3.42 nuovi/mese |

Conclusione: il total edge a 6 mesi con hysteresis e la regola piu interessante come baseline operativa non-lookahead.

Output collegati:

- `bt-strategy-test/overnight-ah/research/out/monthly_universe_walkforward_hysteresis_comparison.csv`
- `bt-strategy-test/overnight-ah/research/out/monthly_universe_walkforward_adv_trades20_total_keep20_enter10/walkforward_metrics.csv`

Lista piu recente generata dalla regola, rank month 2026-05-31:

```text
ASML, AMD, GOOG, GOOGL, MU, KLAC, BKR, MRVL, INTC, NXPI
```

## Ranking expanding

E' stata testata anche una classifica expanding, cioe da inizio storico fino al mese corrente, invece della finestra mobile 6 mesi.

Risultati principali:

| Strategia | Total | Sharpe | MaxDD |
|---|---:|---:|---:|
| 6m total + hysteresis | +11,139% | 1.637 | -34.3% |
| expanding Sharpe | +11,375% | 1.610 | -50.8% |
| expanding total | +6,837% | 1.396 | -46.6% |

Conclusione: expanding converge verso simboli stabili, ma peggiora molto il drawdown. Non e preferibile al 6m total + hysteresis.

Output:

- `bt-strategy-test/overnight-ah/research/out/monthly_universe_expanding_comparison.csv`

## Uso della lista dinamica insieme alla lista statica

E' stato testato se la lista dinamica potesse migliorare la lista statica.

Varianti:

- `static_confirmed_by_rotation`: entro solo sui simboli della lista statica che sono anche nella shortlist 6m total + hysteresis.
- `static_fill_rotation_rank`: uso la lista statica e riempio con simboli della rotazione.
- `union_adv`: unione statica + rotazione, ordinata per ADV.
- blend static/rotation.

Risultati:

| Strategia | Total | Sharpe | MaxDD | Avg posizioni |
|---|---:|---:|---:|---:|
| `static_order` | +39,108% | 1.866 | -29.9% | 2.80 |
| `static_confirmed_by_rotation` | +33,897% | 2.232 | -22.1% | 1.62 |
| `static_fill_rotation_rank` | +28,319% | 1.892 | -31.5% | 4.18 |
| `blend_static_60_rot_40` | +25,004% | 1.871 | -27.1% | n/a |
| `rotation_total_hyst_adv` | +11,139% | 1.637 | -34.3% | 3.62 |

Conclusione:

- Come filtro di conferma, la rotazione migliora Sharpe e drawdown della lista statica.
- Pero resta contaminata dal fatto che la lista statica puo essere lookahead.
- Per una regola live difendibile, la baseline deve essere la rotazione autonoma, non la statica confermata.

Output:

- `bt-strategy-test/overnight-ah/research/out/hybrid_static_rotation_total_hyst/hybrid_metrics.csv`

## Test gate statistico sui ritorni

Ipotesi testata: se i ritorni recenti si scostano negativamente dalla distribuzione storica, sospendere o ridurre l'esposizione finche tornano coerenti.

Sono stati testati:

- quantile gate su rolling mean/sum;
- t-test su finestre 3/5/10 giorni;
- stop completo e riduzione a 50%.

Risultati principali:

| Caso | Base | Miglior gate |
|---|---:|---:|
| `stable_order` | +39,108%, Sharpe 1.866, DD -29.9% | peggiora: +35,646%, Sharpe 1.851, DD -31.7% |
| `baseline_adv` Nasdaq broad | +5,342%, Sharpe 1.453, DD -33.7% | migliora Sharpe/DD: +4,918%, Sharpe 1.531, DD -27.9% |
| `model_ridge` Nasdaq broad | +5,196%, Sharpe 1.600, DD -26.7% | migliora: +5,459%, Sharpe 1.715, DD -19.2% |
| `rotation_total_hyst` | +11,139%, Sharpe 1.637, DD -34.3% | Sharpe leggermente meglio ma ritorno peggiore |
| `static_confirmed_rotation` | +33,897%, Sharpe 2.232, DD -22.1% | peggiora Sharpe e ritorno |

Conclusione:

- Il gate statistico non va implementato come stop automatico sulla lista statica.
- Funziona meglio su universi broad/non cherry-picked, soprattutto con modello Ridge.
- Puo essere usato come alert o risk flag, non come regola hard-stop sulla lista attuale.

Output:

- `bt-strategy-test/overnight-ah/research/out/consistency_gate/consistency_gate_metrics.csv`
- `bt-strategy-test/overnight-ah/research/out/consistency_gate_baseline_adv/consistency_gate_metrics.csv`
- `bt-strategy-test/overnight-ah/research/out/consistency_gate_model_ridge/consistency_gate_metrics.csv`
- `bt-strategy-test/overnight-ah/research/out/consistency_gate_rotation_total_hyst/consistency_gate_metrics.csv`

## Modelli feature / regressione

E' stato testato un modello walk-forward con feature disponibili prima dell'ingresso MOC:

- ritorno intraday;
- posizione del close nel range giornaliero;
- AH noto precedente;
- ritorni close-to-close 3/5/10/20;
- ADV;
- relative volume;
- statistiche AH recenti 5/10/20/60;
- rank cross-sectional.

Modelli:

- Ridge regression;
- HistGradientBoosting.

Risultati:

| Strategia | Total | Sharpe | MaxDD |
|---|---:|---:|---:|
| `model_ridge` | +5,196% | 1.600 | -26.7% |
| `baseline_adv` | +5,342% | 1.453 | -33.7% |
| `baseline_recent_edge20` | +3,552% | 1.391 | -24.0% |
| `model_hgb` | +1,881% | 1.190 | -37.7% |

Dentro la lista statica:

| Strategia | Total | Sharpe | MaxDD |
|---|---:|---:|---:|
| `stable_order` | +39,108% | 1.866 | -29.9% |
| `stable_adv` | +36,580% | 1.845 | -30.6% |
| `stable_ridge` | +32,598% | 1.837 | -30.3% |
| `stable_recent_edge20` | +30,479% | 1.799 | -30.5% |

Conclusione:

- Ridge migliora la qualita del rischio su universo broad rispetto ad ADV puro.
- Dentro la lista statica non batte l'ordine originale.
- Il boosting sembra overfittare.

Output:

- `bt-strategy-test/overnight-ah/research/out/symbol_feature_model_stable/feature_model_metrics.csv`

## Analisi anni/mesi negativi

Nel 2022, anno negativo della lista statica, non tutti i simboli Nasdaq erano negativi:

- 98 simboli con almeno 10 trade;
- 30 positivi;
- 68 negativi.

Questo indica che parte del problema e cross-sectional: in alcuni periodi ci sono simboli che mantengono edge, ma la lista statica non li intercetta.

Mesi peggiori osservati:

| Mese | Positivi / negativi | Lettura |
|---|---:|---|
| 2020-03 | 3 / 79 | shock quasi universale |
| 2019-05 | 7 / 66 | shock quasi universale |
| 2022-06 | 19 / 74 | mostly bad |
| 2022-08 | 30 / 54 | dispersione sfruttabile |
| 2022-09 | 42 / 55 | dispersione sfruttabile |
| 2025-08 | 48 / 38 | forte dispersione |

Conclusione:

- Alcuni drawdown sono regime-wide e difficili da evitare.
- Altri mesi negativi mostrano dispersione e possono essere migliorati con selezione dinamica o modello cross-sectional.

## Interventi operativi live/paper

Problema osservato: ordini MOO scaduti e posizioni non chiuse.

Azioni:

- spostata schedulazione MOO a 09:25 ET;
- aggiunto fallback market a 09:35 ET per chiudere posizioni residue;
- aggiunto script `submit_moo.py --fallback-market`;
- aggiornata crontab.

Script collegati:

- `bin/submit_moo.py`
- `scripts/overnight-ah-moo.sh`
- `scripts/overnight-ah-moo-fallback.sh`
- `scripts/overnight-ah-moc.sh`

## Modifiche dati/indicatori

Sono stati predisposti flussi dati piu ricchi:

- cache Alpaca daily con raw OHLC come primario;
- colonne adjusted aggiuntive `adj_open`, `adj_high`, `adj_low`, `adj_close`;
- supporto in `btmain.py` a linee extra CSV/Parquet;
- possibilita futura di aggiungere campi come sentiment, score, vwap, trade_count;
- `weekly.RMAStrategy` puo usare indicatori adjusted mantenendo execution raw.

Razionale:

- l'execution deve restare su prezzi raw;
- gli indicatori possono usare adjusted per evitare rotture da split/dividendi;
- per OvernightAH al momento il problema principale non e il ritorno adjusted, ma la stabilita degli indicatori e della selezione.

## Conclusione operativa

La lista statica resta la migliore in backtest, ma non e una regola difendibile perche puo incorporare lookahead.

La migliore baseline non-lookahead trovata finora e:

```text
Nasdaq 100
+ ranking mensile su ultimi 6 mesi
+ metrica total edge OvernightAH
+ min 20 trade
+ top 10
+ hysteresis keep 20 / enter 10
+ selezione giornaliera per ADV
```

Questa regola non batte la lista statica, ma e molto piu realistica e puo essere usata come punto di partenza live/paper.

## Prossimi passi consigliati

1. Ottimizzare lo script `monthly_universe_policy_sweep.py` per fare sweep piu ampie senza tempi eccessivi.
2. Testare policy nested:
   - scelta metrica/finestra/hysteresis solo su periodo precedente;
   - applicazione out-of-sample sul periodo successivo.
3. Testare una rotazione per cluster/settore per ridurre concentrazione.
4. Testare Ridge come secondo layer solo sulla lista walk-forward, non sulla lista statica.
5. Trasformare la rotazione mensile in procedura operativa:
   - generazione lista a fine mese;
   - salvataggio JSON;
   - uso automatico nello script live/paper del mese successivo.

## Aggiornamento: hedge overnight SQQQ + fix broker margine (research)

Studio successivo, non ancora in produzione. Dettagli completi in
`docs/context/ah_context.md` (sezione "Hedge overnight SQQQ"). Sintesi:

**Domanda**: esiste un asset la cui performance AH e correlata negativamente
al paniere statico/semis, utilizzabile come copertura nelle notti peggiori?

**Risposta**: no per singole azioni (ne Nasdaq 100, ne un universo settoriale
diversificato: finanziari, energia, staples, utility, oro/materiali) — la
correlazione overnight e sempre positiva, dominata da un fattore macro
comune. Solo strumenti cross-asset funzionano: SQQQ (Nasdaq -3x, il piu
forte, -0.52), VXX, bond governativi (regime-dipendente).

**Configurazione validata** (opt-in, `hedge_enabled=False` di default in
`overnight_ah.py`, non presente in `overnight_ah_live.py`): overlay SQQQ
attivo quando EMA(65) < EMA(150) su QQQ close, peso 15% ritagliato dal
budget di leva esistente solo le notti in cui l'hedge apre davvero (non una
riserva permanente — su un orizzonte lungo una riserva sempre attiva costa
troppo rendimento composto). Testate e scartate: rampa continua (nel rumore
vs binario), variante momentum (dominata, taglia la protezione troppo
presto nei bear market a scossoni), indice AH-only come base del trend
(peggiora, troppo rumoroso).

Risultato Backtrader (statico-10, 2018-2026): 1128x / Sharpe 1.509 / DD 2022
-3.1%, contro 976x / Sharpe 1.430 / DD 2022 -23.9% senza hedge.

**Non ancora validato sull'universo dinamico reale `weak_theme_switch`**
(quello di paper/live) — solo sul paniere statico-10 di test. Passo
obbligato prima di qualunque promozione a `overnight_ah_live.py`.

**Bug scoperto durante l'implementazione** (fix in `bt-core/broker/
broker.py`, non nella strategia): un entry order rifiutato per margine
insufficiente non cancellava l'ordine di chiusura abbinato (sottomesso
come ordine indipendente nello stesso bar per il pattern MOC/MOO di
backtest) — quell'ordine eseguiva comunque da flat, aprendo uno short
fantasma che corrompeva cassa/leva per il resto del backtest. Fix:
override di `Broker._bracketize()` che cancella l'ordine gemello tramite
un riferimento incrociato (`sibling_ref`). Verificato non regressivo su
statico-10 e sulla configurazione OOS `weak_theme_switch` di riferimento
(risultato identico bit-per-bit pre/post fix in entrambi i casi — nessuna
delle due aveva mai incontrato la condizione che innesca il bug).

**Prossima analisi (hedge)**: specifiche complete (comandi esatti, trappola
del ticker file di controllo, limite della finestra 2023-2026 che non copre
il 2022, criteri di decisione) in `docs/context/ah_context.md`, sezione
"Prossima analisi: validazione hedge su `weak_theme_switch`". **Non e'
pero' il prossimo passo immediato**: l'utente ha chiesto di dare priorita'
prima a uno studio sui mesi negativi della strategia (asset che sporcano il
risultato, segnali premonitori, pattern riconoscibili) — specifiche
complete in `docs/context/ah_bad_months_study_spec.md`.

