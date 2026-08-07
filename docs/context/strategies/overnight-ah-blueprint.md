# Blueprint operativa — OvernightAH

**Stato:** candidata paper-operativa; non promossa automaticamente a live.

## In breve

La strategia prova a guadagnare dal movimento notturno di alcune azioni statunitensi: compra poco prima della chiusura della borsa USA e vende all'apertura del giorno lavorativo successivo. Ogni mese decide quale lista di titoli privilegiare; ogni sera compra soltanto i primi titoli di quella lista che rispettano requisiti di movimento, liquidità e rischio. Non è una strategia che resta investita nel lungo periodo.

La parte iniziale spiega il funzionamento senza presupporre conoscenze tecniche. I nomi dei parametri e i comandi sono raccolti più avanti nell'appendice tecnica.

## Mandato ed esecuzione

Strategia long-only: compra alla chiusura regolare della borsa USA del giorno t e vende all'apertura regolare del giorno t+1.

- Nel backtest storico, la modalità asta simula l'acquisto al close e la vendita all'open: è il test coerente con l'ipotesi.
- Nel paper trading (conto simulato) o nel live, la modalità asta invia un ordine specifico per l'asta di chiusura; un processo automatico separato invia l'ordine di vendita all'asta di apertura.
- L'altra modalità compra a mercato poco prima della chiusura e lascia l'ordine valido fino a esecuzione. Misura un trade leggermente diverso, dal momento dell'acquisto pre-close fino all'open, e non deve essere confrontata direttamente al test in asta.
- Nel paper/live la strategia invia soltanto la richiesta di acquisto; i processi automatici del mattino inviano la vendita e, se necessario, una chiusura di emergenza a mercato.

La misura di confronto corretta è il rendimento fra prezzo di chiusura di oggi e prezzo di apertura di domani. Non usare buy-and-hold o il rendimento nella sola giornata di borsa come confronto principale.

## Prima di iniziare: dizionario essenziale

Il documento usa queste parole con il significato seguente.

| Termine | Significato pratico |
|---|---|
| Overnight / AH | La posizione resta aperta dalla chiusura regolare americana di oggi all'apertura regolare di domani. AH qui indica il rendimento di quella notte, non la possibilità di negoziare genericamente nel mercato esteso. |
| RTH | Regular Trading Hours: seduta USA regolare, normalmente 09:30–16:00 New York. |
| Close-to-open o C2O | Il rendimento fra close di oggi e open di domani: è ciò che la strategia cerca di catturare. |
| C2C | Close-to-close: rendimento dal close di una seduta al close della successiva. È usato per classificare il trend dei titoli, non è il rendimento direttamente negoziato dalla strategia. |
| MOC / MOO | Market On Close / Market On Open: ordine eseguito rispettivamente alla chiusura o all'apertura. |
| CLS / OPG / GTC | Nomi Alpaca: CLS è l'asta di chiusura, OPG l'asta di apertura, GTC significa che un ordine resta valido finché non viene eseguito o cancellato. |
| Warm-up | Storico caricato solo per poter calcolare medie, trend e classifiche prima del primo trade. Non deve entrare nelle metriche di trading del periodo analizzato. |
| ADV$ | Dollar Average Daily Volume: volume medio giornaliero in azioni moltiplicato per prezzo. È una stima della liquidità quotidiana. |
| Regime | Una condizione di mercato che decide quale insieme di titoli usare nel mese. Non predice il singolo trade: sceglie la modalità dinamica o la lista statica. |
| Fattore semis | Media dei rendimenti dei titoli semiconduttori configurati; è un indicatore sintetico del loro contesto di mercato. |
| Score e rank percentile | Ogni metrica è trasformata in posizione relativa fra i ticker: 1 indica la parte migliore della classifica. Lo score combina tali posizioni; non è una previsione percentuale del rendimento. |
| EMA | Media mobile esponenziale: una media dei prezzi che pesa di più i giorni recenti. |
| Drawdown | Discesa percentuale dal precedente massimo del capitale o del prezzo. |
| Risk overlay | Regola aggiuntiva di protezione che riduce o ferma temporaneamente le nuove entry quando l'evidenza recente peggiora. Non cambia l'algoritmo di selezione dei titoli. |
| Post-up cooldown | Pausa volontaria dopo una notte eccezionalmente positiva: evita di entrare per alcune sedute dopo un salto di rendimento aggregato. È disabilitata di default. |
| CUSUM | Cumulative Sum: controllo statistico che somma giorno dopo giorno gli scostamenti negativi del rendimento AH medio rispetto alla sua normalità recente. Se la somma negativa supera una soglia, ferma le nuove entry; riprende solo dopo segnali di normalizzazione. È un overlay sperimentale, disabilitato di default. |

## Come funziona, in pratica

L'operatività si divide in due decisioni: una scelta **mensile** su quali titoli mettere in priorità e una scelta **giornaliera** su quali di essi comprare quella sera.

### 1. All'inizio del mese: quale lista di titoli usare?

La strategia osserva come si sono mossi, nei circa tre mesi appena conclusi, i titoli del settore dei semiconduttori. È una semplice domanda di contesto: il settore nel suo complesso ha avuto una fase positiva o negativa?

- Se la risposta è **positiva**, usa una lista dinamica: una nuova graduatoria dei titoli che si sono comportati meglio di recente.
- Se la risposta è **negativa o neutra**, usa una lista prudente e predefinita di titoli, nell'ordine già stabilito.

Non usa informazioni del mese che sta iniziando. La decisione del 1° settembre, per esempio, può usare dati fino alla chiusura del 31 agosto, mai dati di settembre.

Quando usa la lista dinamica, assegna a ogni titolo un punteggio basato soprattutto su due domande:

1. Negli ultimi sei mesi il titolo ha avuto una buona performance complessiva da chiusura a chiusura?
2. Negli ultimi sei mesi il titolo ha avuto una buona performance specificamente durante la notte, da chiusura ad apertura successiva?

Per impostazione predefinita il primo criterio pesa il 60% e il secondo il 40%; questo rapporto è ora regolabile in configurazione. A questo aggiunge un piccolo 15% di preferenza per i titoli il cui andamento assomiglia a quello del settore semiconduttori. I migliori 50 entrano nella lista del mese, già in ordine di priorità.

Esempio: se il settore semiconduttori ha avuto tre mesi complessivamente positivi, la strategia crea la graduatoria dinamica. Se AMD, ASML e MU risultano tra i punteggi più alti, saranno esaminati per primi ogni sera. Se invece il settore è stato debole, non crea quella graduatoria: parte dalla lista prudente predefinita, rispettando il suo ordine.

In alcuni set di dati è presente anche SPY, l'ETF che rappresenta il mercato USA ampio. Se negli ultimi tre mesi SPY è sceso del 10% o più dal suo massimo recente, la strategia non usa la lista dinamica: la considera una condizione di mercato troppo debole. Se SPY non è caricato nei dati, questo controllo non può essere fatto e viene ignorato.

### 2. Ogni sera: quali titoli comprare davvero?

La strategia legge la lista mensile nell'ordine appena descritto e valuta un titolo alla volta. Per ogni titolo controlla che:

1. La giornata abbia avuto un movimento sufficiente, ma non eccessivo.
2. Il titolo abbia un prezzo e una liquidità adeguati.
3. Il gap della notte precedente non sia stato eccezionalmente negativo.
4. In paper/live, se il controllo è abilitato, non ci siano imminenti risultati societari.

Compra i primi titoli che superano tutti i controlli, fino al massimo stabilito di cinque posizioni. Non cerca il titolo con il volume più alto: la liquidità minima è una condizione di accesso, non un criterio di classifica.

### 3. Protezioni e dimensione delle posizioni

Prima di acquistare, può applicare protezioni opzionali. La configurazione candidata le lascia spente: sono strumenti sperimentali, non il cuore della strategia.

- Una pausa dopo una notte eccezionalmente positiva può evitare di rientrare immediatamente per alcuni giorni.
- Una protezione di performance può ridurre l'importo investito, o non investire, quando il risultato recente della strategia è debole.
- Una protezione statistica chiamata CUSUM può fare la stessa cosa quando molti giorni recenti sono peggiori di quanto normalmente osservato. In sostanza, controlla se il vantaggio storico della strategia sembra essersi deteriorato in modo persistente, invece di reagire a una sola notte negativa.
- Un hedge opzionale può comprare una piccola posizione SQQQ quando il Nasdaq è in una tendenza ribassista. Anche questo non è attivo nella configurazione candidata.

Se nessuna protezione interviene, il capitale previsto viene ripartito fra i titoli ammessi. Con la configurazione candidata il massimo è cinque titoli e ciascuno riceve normalmente circa un quinto del capitale complessivamente destinato alla strategia.

## Corrispondenza con i nomi tecnici

Per chi deve modificare una configurazione: la scelta mensile descritta sopra corrisponde a monthly_universe_mode=weak_theme_switch. La condizione “semiconduttori positivi” è monthly_universe_switch_feature=semis_total_3m con monthly_universe_switch_threshold=0.0. La lista dinamica usa monthly_universe_top_n=50, monthly_universe_base_weight=0.85, monthly_universe_c2c_weight=0.60, monthly_universe_theme_weight=0.15 e monthly_universe_theme_score=corr12. Il peso C2O/AH non va impostato separatamente: è sempre il complemento a 1 del peso C2C. I dettagli dei parametri restano nella sezione successiva.

### Filtri giornalieri, in ordine

Dopo l'ordinamento, ciascun ticker deve superare:

1. Almeno due barre.
2. Range intraday (high-low)/open entro min_intraday_vol e max_intraday_vol, su tutte le barre (any) o solo up/down.
3. close >= min_price, se attivo.
4. SMA(volume, barra precedente) * close >= min_adv.
5. open[t]/close[t-1]-1 >= ah_lag1_threshold, solo se soglia negativa.
6. In paper/live, se disponibile il calendario: nessun earnings entro earnings_lookahead_h.

L'ADV usa il volume medio della barra precedente per evitare lookahead nella decisione MOC.

## Appendice tecnica — parametri di run manuale

Questa sezione serve a chi prepara o modifica una configurazione. I nomi nella prima colonna sono quelli da usare nel file di configurazione o nel comando di avvio; il loro effetto è stato spiegato in linguaggio operativo nelle sezioni precedenti.

### Universo e regime

| Parametro | Valori / candidato | Azione |
|---|---|---|
| monthly_universe_mode | file, weak_theme, weak_theme_switch / weak_theme_switch | File, dinamico puro, oppure dinamico condizionato con fallback statico. |
| monthly_universe_file | CSV year;month;symbols | Usato da file; assente/illeggibile → ticker file completo. |
| monthly_universe_top_n | intero / 50 | Dimensione massima del ranking dinamico. |
| monthly_universe_base_weight | frazione / 0.85 | Peso score C2C6m/AH6m. |
| monthly_universe_c2c_weight | da 0 a 1 / 0.60 | Dentro il punteggio base, peso della performance C2C a sei mesi. Il peso C2O/AH è automatico: 1 meno questo valore. Esempi: 0.60 = 60% C2C e 40% C2O; 0.50 = equilibrio; 0.75 = 75% C2C e 25% C2O. |
| monthly_universe_theme_weight | frazione / 0.15 | Peso tilt semis; mantenere somma pesi = 1. |
| monthly_universe_theme_score | corr12, beta12, structural / corr12 | Correlazione, beta, o combinazione corr/beta/appartenenza semis. |
| monthly_universe_semis | CSV ticker | Costruisce fattore semis; i ticker devono avere dati. |
| monthly_universe_static_symbols | CSV ordinato | Fallback e priorità giornaliera nel ramo statico. |
| monthly_universe_switch_feature | semis_total_1m/3m/6m/12m, semis_mean_*, semis_ma63_ratio, semis_ma126_ratio / semis_total_3m | Feature di regime. |
| monthly_universe_switch_threshold | float / 0.0 | Dinamico solo se feature strettamente maggiore. |
| monthly_universe_spy_dd3m_threshold | frazione / -0.10 | Chiude il ramo dinamico sotto soglia drawdown SPY; richiede SPY nel file. |

### Selezione e sizing

| Parametro | Valori / candidato | Azione |
|---|---|---|
| max_concurrent | intero / 5 | Massimo posizioni overnight normali. |
| min_intraday_vol, max_intraday_vol | 0.025, 0.045 | Range giornaliero ammesso: 2.5%–4.5%. |
| intraday_vol_filter_side | any, up, down / any | Dove applicare il range. |
| ah_lag1_threshold | negativo o 0 / -0.10 | Esclude solo AH precedente peggiore di -10%; 0 lo spegne. |
| min_price | >=0 / 0 | Prezzo minimo; poco informativo nel research. |
| min_adv | USD / 100000000 | ADV$ minimo; 100M è il compromesso scelto. |
| earnings_skip | bool / false nel paper dinamico | Salta earnings solo con calendario disponibile; non backtestato storicamente. |
| earnings_lookahead_h | intero / 36 | Orizzonte earnings. |
| max_exposure | float / 2 | Gross target. ≤1 usa cash; >1 usa equity. Non è il cap broker. |
| margin_leverage | float / 2 | Cap broker separato. Overnight Alpaca: riferimento Reg T ~2x, non buying power intraday ~4x. |
| sizing_policy | legacy, selected_equal, selectable_fixed, current_slots, rank_decay, reverse_rank_decay | Distribuisce budget nell'ordine candidati. rank_decay sovrappesa il primo. |
| size_by_max_concurrent | bool / true | Con legacy divide sempre per 5; false redistribuisce sui candidati passati. |
| min_cash_per_trade | USD / 100 | Notional minimo. |
| auction | bool / BT true, AUC true, no/live false | Asta close vs market GTC pre-close. |
| max_adv_participation | frazione / 0.0025 | Cap di notional su liquidità, gestito dal layer condiviso. |

Con max_exposure=2, max_concurrent=5, size_by_max_concurrent=true, uno slot riceve circa il 40% dell'equity di notional. Con tre candidati l'esposizione è circa 120%, non viene forzata al 200%.

### Overlay e hedge

| Parametro | Candidato | Azione / stato |
|---|---:|---|
| post_up_cooldown_threshold, post_up_cooldown_days | 0, 5 | Dopo forte gain aggregato sospende N sessioni; disabilitato a zero. |
| risk_overlay_mode | off | Interruttore delle protezioni: off le spegne; strategy_throttle riduce l'esposizione dopo perdite recenti; ah_gain_cusum usa il controllo CUSUM descritto nel glossario. Possono essere combinati ma sono sperimentali. |
| risk_overlay_lookback, risk_overlay_threshold, risk_overlay_stress_exposure | 10, -0.10, 1.25 | Per il throttle: osserva gli ultimi 10 rendimenti della strategia; se il rendimento cumulato è sotto -10%, usa 1.25 come esposizione target invece di quella normale. |
| ah_gain_cusum_window_days, ah_gain_cusum_min_window_days, ah_gain_cusum_k, ah_gain_cusum_h | 756, 252, 0.5, 30 | Per CUSUM: calcola ogni giorno una normalità mobile sui precedenti 756 giorni di rendimento AH medio fra i ticker; non può agire prima di 252 giorni. k è la tolleranza al rumore, h la soglia che fa scattare la pausa. |
| hedge_enabled | false | Attiva hedge SQQQ, non promosso live. |
| hedge_symbol, hedge_trend_symbol | SQQQ, QQQ | Devono stare nel ticker file; esclusi dai long normali. |
| hedge_fast_period, hedge_slow_period, hedge_weight | 65, 150, 0.15 | Se EMA65(QQQ)<EMA150(QQQ), compra SQQQ overnight per 15% del budget; sottrae capitale ai long solo quella notte. |

entry_minute e moo_timeout_min sono legacy nel flusso daily: gli orari reali sono dei cron. trade_start_date crea warm-up senza trade prima della data.

## Configurazioni riproducibili

### Backtest close-to-open statico

    cd /home/htpc/backtrader/bt-core
    .venv/bin/python btmain.py       --strat overnight_ah.OvernightAH       --ticker stable_ah_top10.json --mode backtest --timeframe daily       --provider yahoo --fromdate 2016-01-04 --commission none --margin-leverage 2       --stratargs "max_concurrent=5 max_exposure=2 auction=True min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 min_adv=100000000 size_by_max_concurrent=True"

### Paper dinamico AUC

Riferimento: bt-strategy-test/overnight-ah/run-configs/overnight_ah_auc_dynamic_paper.toml.

    cd /home/htpc/backtrader/bt-core
    .venv/bin/python btmain.py --run-config       /home/htpc/backtrader/bt-strategy-test/overnight-ah/run-configs/overnight_ah_auc_dynamic_paper.toml

Prerequisiti: virtualenv, dati Yahoo daily aggiornati, credenziali broker fuori dal TOML, semis (e SPY se richiesto) nel ticker file. Per il ranking mensile completo servono almeno circa 252 giorni di storico; il wrapper con 120 giorni è sufficiente all'ADV ma non a ricostruire in modo affidabile tutte le feature 6/12 mesi. In quel caso usare monthly_universe_file esterno.

Prima di un run manuale paper/live controllare ordini e posizioni. Non rilanciare auction=False fuori orario: un market GTC può restare pending fino all'open.

## Simulazioni multivariata: risultati da leggere con cautela

Le cifre sono Yahoo/backtest storici, non performance live. La lista statica è contaminata da selection/lookahead storico e va usata come controllo, non come regola live autonoma.

| Variante | Segmento | Risultato |
|---|---|---|
| Static stable_ah_top10 | ricerca walk-forward | +39.108%, Sharpe 1.866, MaxDD -29.9%; forte ma selezione ex-post. |
| Rotazione 6m total + hysteresis 20/10 | Nasdaq walk-forward | +11.139%, Sharpe 1.637, MaxDD -34.3%, 1.30 nuovi/mese; baseline ex-ante semplice. |
| weak_theme corr12 85/15 top50 + gate | train / validation / OOS | Sharpe 2.543 / 1.411 / 2.480; DD -53.2% / -37.1% / -30.6%. |
| weak_theme_switch, semis_total_3m>0 | train / validation / OOS | final 7.66M / 705k / 4.22M; Sharpe 2.442 / 1.347 / 2.688; DD -51.6% / -35.5% / -29.0%. |
| Switch top 40 / 50 / 60 | OOS | Sharpe 2.684 / 2.688 / 2.685: top50 è un plateau, non un picco isolato. |
| Sweep ADV 0 / 50 / 100 / 150 / 200M | statico documentato | Sharpe 1.256 / 1.550 / **1.595** / 1.520 / 1.330: scelto 100M. |
| Hedge SQQQ EMA65/150 | static-10, 2018-2026 | 1129x, Sharpe 1.509, DD 2022 -3.1%, vs 976x/1.430/-23.9% senza hedge; non validato sull'universo dinamico. |

Su 2016-01-04 → 2026-06-18 il segnale Yahoo ha reso circa 90.6M, Sharpe 1.895 e 7.915 trade; Alpaca/SIP circa 53.0M, Sharpe 1.494 e 7.909 trade. Anche un range borderline può cambiare il filtro (AMD 2.499% Yahoo vs 2.516% SIP): provider e feed devono restare fissati.

## Guardrail di promozione

1. Rigenerare run Backtrader continuo con provider, universo, warm-up, gate, entry mode e costi fissati.
2. Separare train/validation/OOS; non conteggiare warm-up come trading.
3. Stressare costi round-trip: in validation l'edge lordo è più sottile dell'OOS.
4. Non abilitare earnings, hedge, CUSUM o cooldown senza validazione della variante identica.
5. Verificare dati, paniere mensile, ordini aperti e posizioni prima di rerun manuali.

## Fonti

- Codice: bt-core/strategies/overnight_ah.py.
- Config paper: bt-strategy-test/overnight-ah/run-configs/overnight_ah_auc_dynamic_paper.toml.
- Sintesi: docs/overnight_ah_sintesi.md.
- Ricerca estesa: docs/context/ah_context.md.
- Operatività Alpaca: docs/context/alpaca_paper_live_overnight_ah.md.
- Metodo: docs/context/strategy-analysis-methodology.md.
