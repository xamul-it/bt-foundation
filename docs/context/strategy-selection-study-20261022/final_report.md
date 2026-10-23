# Quale strategia sperimentare: proposta decisionale

Studio avviato il 22 ottobre e concluso documentalmente il 23 ottobre 2026. Dati congelati al **21 ottobre 2026**: la ripresa della sessione non amplia il campione. Revisione indipendente e giudizio del Director sono collegati in fondo.

## Decisione proposta

**Usare OvernightAH A00, con configurazione stabile e target di impiego 1x, come primo riferimento per una sperimentazione prospettica condizionata alla verifica dei dati e dei costi.** Il conto è interamente capitale di rischio: nessuna quota viene riservata a esigenze finanziarie personali. L'impiego effettivo dipende da segnali, cap e disponibilità di titoli; il target non è una garanzia di esposizione esatta ai prezzi di fill.

Questa priorità serve a raccogliere evidenza confrontabile. **Non è dimostrato un vincitore universale da promuovere live:** il costo effettivo può ribaltare la scelta a favore di RMA, mentre selezione retrospettiva e proxy d'asta limitano tutte le candidate. Non introdurre adesso né switch automatico né aggiornamenti automatici dei parametri. Conservare A01 con hedge come alternativa shadow interessante; non sostituire A00 con FlatComposite sulla sola equity finale.

La leva aggiuntiva è stata valutata per la crescita del capitale. Nel confronto nativo a costo10 bps RT e finanziamento10%, passare alla policy target2x produce **CAGR15,05% contro15,46%, DD78,22% contro46,04%**: non emerge una ragione economica per aumentarla in quello scenario. Il risultato non determina la leva ottimale e non dimostra che ogni leva, costo o strumento alternativo sia inutile.

La configurazione a metà budget resta un compromesso di rischio: riduce il DD, ma dimezza circa la crescita osservata. Non è la proposta predefinita per massimizzare il rendimento di questo conto.

## Cosa è stato realmente verificato

La prima consegna, precedente alle nuove simulazioni, contiene la [mappa parametri](parameter_map.md), [17 filoni di studi e correzioni](study_register.md), [interazioni](interactions.md) e [catalogo](run_catalog.md). Sono stati inventariati65/68/32 parametri effettivi per AH/Composite/RMA e4.012 serie native archiviate, oltre ai riferimenti locali. Gli archivi non certificano da soli l'identità di un job Watchtower.

Il freeze comprende69 file sorgente/config e222 dataset, più derivati:294 riferimenti controllati prima dei batch. Il panel contiene26.839righe; audit del generatore e perturbazione di42 celle future non mostrano uso dello stesso mese nelle feature verificate. Questo verifica il calcolo corrente, non la pubblicazione storica del panel né l'origine point-in-time di universo e pesi.

Sono completati **21 tentativi nativi:1 pilota,18 core e2 controlli supplementari**. Tutti usano il vero `btmain.runstrat`/Backtrader congelato, con output isolati. Il warmup è2000–2004; il periodo economico è3 gennaio 2005–21 ottobre 2026,5.485 sedute. Il primario2010–2022 comprende3.272 sedute;2023–2026 era già osservato e non è un holdout intatto. Le procedure adattive, S126 e il mix sono screening dai returns nativi, esplicitamente distinti da conti eseguibili continui.

NAV, rendimenti, cash, fill, fee e interessi sono stati ricostruiti indipendentemente. Non risultano fill prima dell'avvio o dopo il cutoff, fill su quote mancanti/non finite/volume non positivo, submission o mark stale, né eventi Partial/Margin/Rejected nei run revisionati. Questo certifica la coerenza interna delle tape disponibili, non la capacità reale delle aste. Le posizioni aperte RMA e gli intenti AH terminali non eseguiti restano censurati, senza liquidazioni finali inventate.

## Risultati che cambiano la scelta

Tutti i rendimenti seguenti sono netti delle fee addebitate dal broker e del finanziamento convenzionale. CAGR su252 sedute; DD è perdita dal picco. Il costo5 bps RT significa2,5 bps per lato; lo stress10 bps RT significa5 bps per lato. Non è una replica completa della tariffa personale del conto.

| Configurazione | CAGR2005–2026 a5 bps RT | Max DD | Durata massima picco→recupero/cutoff, sedute | CAGR a10 bps RT |
|---|---:|---:|---:|---:|
| A00: AH base |30,93%|36,01%|632|15,46%|
| A01: AH con hedge |32,13%|26,24%|611|non simulato|
| C1: Composite matched |31,57%|39,91%|735|16,69%|
| C2: challenger Composite target2x |39,53%|44,73%|606|non simulato|
| R2: RMA next-open |22,19%|42,82%|1.134|21,15%|

![Costi e graduatoria](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/figures/cost_sensitivity.png)

**Il ranking si inverte con i costi.** A00 perde circa15,47 punti di CAGR fra i due scenari; R2 circa1,04. RMA diventa un'alternativa credibile quando gli attriti overnight sono elevati, pur con DD e debolezza recente importanti. Non interpolare una soglia precisa fra5e10 bps: compounding, quantità, cap e breadth cambiano lungo ogni percorso.

C2 ha il CAGR puntuale maggiore, ma cambia insieme ranking/pesi, topN, gap e leva; non identifica l'effetto di una sola componente. Il test2x su A00 non confuta C2 come configurazione distinta. Mancano però il suo stress matched10 bps e una validazione indipendente dalla selezione storica: C2 resta esplorativo, senza promozione dal solo CAGR39,53%.

AH e Composite condividono gran parte del rischio: correlazione giornaliera completa0,904. Su5.485 sedute,226delle275code peggiori al5% di A00 coincidono con quelle di C1, circa82,2%; AH–RMA64/275, circa23,3%. Sono frequenze descrittive e dipendenti, non una certificazione di diversificazione futura. Le tre sleeve iniziali equiponderate del mix diagnostico poi derivano; non è stato simulato un conto condiviso con ribilanciamento implicito.

## Configurazioni e metodo per aggiornarle

La specifica autorevole è il JSON completo, con tutti i parametri risolti e gli input esterni, non soltanto la lista breve:

| Uso proposto | Riferimento esatto | Impostazioni economiche principali |
|---|---|---|
| Primo riferimento prospettico |[A00 risolto](../../../bt-strategy-test/strategy-selection/study_20261022/proposals/A00_resolved_reference.json)|OvernightAH; daily; lag1; Close→Open; weak_theme_switch mensile; top50; base/theme0,85/0,15; corr12; switch semis_total_3m soglia0; gate SPY−10% su63 sedute/aggiornamento mensile; gap−0,20; waterfall minimo3/massimo50; ADV1% con lookback20; target1x; cooldown0/0; hedge off|
| Alternativa di rischio da osservare |[A01 risolto](../../../bt-strategy-test/strategy-selection/study_20261022/proposals/A01_resolved_reference.json)|A00 con hedge SQQQ, riferimento QQQ, EMA50/120, peso0,15|
| Confronto di ranking |[C1 risolto](../../../bt-strategy-test/strategy-selection/study_20261022/proposals/C1_resolved_reference.json)|A00 matched con flat_panel e21 pesi esattamente congelati; nessun rituning|
| Alternativa se gli attriti dominano |[R2 risolto](../../../bt-strategy-test/strategy-selection/study_20261022/proposals/R2_resolved_reference.json)|weekly.RMAStrategy su daily; rebalance_weekday0; period180/fast30; min_concurrent2; ADV0,25%; max_volatility0,08; trail_stop0,001; regime off; use_adj_indicators false; next-open ingresso/uscita|

Il segno del filtro gap conta:−0,20 è il limite inferiore firmato del gap singolo osservato, non un limite superiore né una salita cumulativa. `weekly` è il nome del modulo RMA, non la sua cadenza in questa configurazione. Il Composite conserva il fallback statico previsto dal codice quando il modo lo richiede: il nome della classe non prova un universo sempre dinamico.

AH/Composite usano YahooAdj e100 feed,97tradabili; RMA Yahoo raw e90 feed. Questi universi di ricerca, ripuliti ex post, non vanno esportati come certificazione point-in-time. Broker, calendario, unità, panel e regole d'ordine sono parte della configurazione. I JSON sono proposte di ricerca e riferimenti risolti; non sono profili operativi installati.

Nelle quattro procedure confrontate, AH expanding, AH trailing5y e RMA expanding coincidono con il riferimento: delta0 e statistica non identificabile. RMA trailing5y perde1,394 punti di log-return annuo nel primario. Nessuna giustifica cambiare automaticamente i valori ogni anno. **Mantenere i parametri stabili; rivedere annualmente ipotesi, dati ed esecuzione, cambiando valori solo tramite un nuovo confronto motivato e congelato.** Il risultato riguarda le coppie cooldown off/on e breadth2/5, non dimostra l'ottimalità di tutti i default o dei21 pesi.

## Protezioni e switch

I15 contrasti pianificati condividono il maxT bootstrap:2.000 repliche, seed20261022, blocco63 con sensibilità21/126. Dodici sono identificabili; i tre delta esattamente nulli non ricevono intervalli artificiali. Il gate originario richiedeva almeno+0,02 annuo log, limite simultaneo positivo, maggioranza prefissata degli anni e vincolo DD; gli stress nativi mancanti rimangono mancanti. **Nessun challenger/procedura supera il gate forte.**

| Contrasto primario2010–2022 | Delta annuo log | Limite inferiore simultaneo95%, finale | Implicazione |
|---|---:|---:|---|
| Cooldown A10/A00 |−7,717pp|−14,448pp|rendimento inferiore e DD peggiore; non adottare automaticamente0,01/3|
| Hedge A01/A00 |+0,667pp|−1,312pp|DD primario inferiore di4,00 punti; compromesso interessante ma non confermato, stress10 bps assente|
| Hedge+cooldown A11/A00 |−7,491pp|−15,392pp|le protezioni non si sommano come benefici indipendenti|
| Composite C1/A00 |+0,337pp|−8,097pp|DD primario peggiore di3,98 punti; superiorità del ranking non dimostrata|
| RMA breadth5/2 |−4,013pp|−11,210pp|DD primario inferiore di2,03 punti; cambiano anche le esenzioni dalle uscite|
| S126/A00 |−1,183pp|−9,497pp|switch non supportato|

L'interazione hedge×cooldown osservata nel fattoriale non autorizza l'adozione congiunta: entrambi i bracci con cooldown perdono crescita rispetto ad A00. Non interpretare la correlazione dei parametri dei vecchi run come effetto causale. Per il nuovo riferimento non si riaprono CUSUM, score e rotazioni già chiusi negativamente senza una nuova ragione identificante.

S126 decide mensilmente da126 sedute passate, con clock corretto, almeno63 sedute fra cambi e10 bps di costo aggiuntivo sul NAV per transizione. Nel primario effettua31cambi; perde anche controC1 e contro il mix iniziale equiponderato. Superare RMA o cash non basta. Non esiste un finalista da verificare nativamente e non viene raccomandato uno switch automatico. Questo non dimostra che ogni possibile switch futuro fallisca.

Il vecchio A00_HALF, `max_exposure=0.5`, espandeva la breadth fino a circa1x: è conservato come controllo non conforme all'intento. L'[amendment](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/half_budget_control_amendment.md) ha autorizzato prima dell'esito una sottoclasse di ricerca che dimezza solo `_sizing_equity()`, mantenendo max_exposure1. La [versione originaria](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/v1/snapshot_manifest.json) resta congelata; l'analisi finale usa il controllo corretto. Nessuna selezione negativa è stata riaperta.

## Intero capitale di rischio, budget ridotto e leva

| Policy nativa | Costo RT | CAGR completo | Max DD | Durata massima picco→recupero/cutoff | Proxy buy/NAV nei giorni buy |
|---|---:|---:|---:|---:|---:|
| A00 target1x |5 bps|30,93%|36,01%|632 sedute|1,0014|
| A00_HALF_BUDGET |5 bps|15,10%|17,75%|607 sedute|0,5007|
| A00_COST10 target1x |10 bps|15,46%|46,04%|820 sedute|1,0014|
| A00_LEV2_COST10 target2x |10 bps|15,05%|78,22%|2.172 sedute|1,9896|

La proxy somma i buy al prezzo di fill e divide per il NAV serale; non è una misura completa del rischio intraday o overnight. AH è spesso in cash all'osservazione dopo il ciclo giornaliero: quella liquidità è conseguenza della sessione negoziata, non accantonamento personale.

Metà budget riduce nettamente la perdita massima ma non migliora la crescita nel confronto ammesso. Il target1x non è un hard cap: i riepiloghi broker registrano un massimo1,22per A00,0,60nel controllo corretto e2,51nel nuovo2x. Il capitale in rischio va misurato, senza affidarsi all'etichetta del parametro.

Il test2x è una domanda supplementare dell'utente, registrata con [amendment prima dell'esito](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/leverage_user_amendment.md); non entra retroattivamente nella famiglia15. Cambia la policy nativa di sizing cash/equity e waterfall, non moltiplica semplicemente i returns1x. Nel primario il vantaggio è appena+0,553ppannuo log, CI appaiato95%[−10,758;+11,698]pp a blocco63; anche21e126includono0. Il delta sul percorso completo è negativo: **il sostegno esplorativo alla maggiore crescita fallisce**, senza imporre il vecchio limite DD2 punti alla nuova domanda.

Il2x accumula circaUSD1,715 milioni di interessi e subisce una peggiore seduta−24,19%, contro−12,05% del riferimento COST10. Il DD78,22% nasce dal picco13 dicembre 2021, minimo21 febbraio 2024 e recupero2 giugno 2026. L'episodio più lungo è diverso:9 febbraio 2006→26 settembre 2014,2.172 intervalli di seduta dal picco al recupero e2.171 osservazioni strettamente sotto il picco. Assenza di insolvenza/liquidazione osservata nel modello non significa probabilità di rovina nulla.

La [verifica dei vincoli correnti](execution_constraints.md) distingue maintenance0,25del modello dai requisiti reali per strumento e dal normale limite Reg T overnight2x descritto da [Alpaca](https://docs.alpaca.markets/us/docs/margin-and-short-selling).10x non è simulato né estrapolato; un altro veicolo richiederebbe un altro studio. Il finanziamento10% è una convenzione comune, non il tasso contrattuale del conto.

## Capitale, capacità e perdite in valuta

| Capitale iniziale | A00 CAGR / DD | C1 CAGR / DD | R2 CAGR / DD |
|---|---|---|---|
|USD20.000|30,51% /33,39%|31,83% /32,02%|20,02% /51,76%|
|USD200.000|30,93% /36,01%|31,57% /39,91%|22,19% /42,82%|
|USD2.000.000|30,91% /30,56%|28,69% /32,91%|14,95% /33,63%|

Sono9percorsi compound nativi, con arrotondamenti e cap, non una curva moltiplicata per il capitale. Non identificano un capitale ottimale o una capacità live. Un DD36,01% corrisponde aUSD72.026su un patrimonio **al picco** diUSD200.000; il DD78,22% aUSD156.450sullo stesso picco. Sono traduzioni dimensionali del rischio percentuale, non le perdite in dollari effettivamente avvenute sui percorsi composti. PnL in valuta dei run, fee, debito, breadth, prossimità ai cap e recuperi sono nelle [tabelle complete](numerical_evidence.md) e nei relativi JSON.

La prossimità al cap ADV non certifica binding o quantità d'asta eseguibile. Un gap può portare il fill oltre il cap in dollari alla creazione. Le nuove simulazioni non isolano fixed-notional: gli studi storici relativi rimangono informativi con riserve, senza attribuire a questi run una prova mancante. Non raccomandareUSD2M solo perché A00 conserva un CAGR alto nello scenario.

## Il deterioramento recente e cosa misurare dopo l'avvio

Dal1 luglio al21 ottobre,79 sedute già osservate: A00−0,035%,C1−4,44%,R2−30,41%;2026YTD rispettivamente+74,63%,+50,31%,+55,66%. L'ultimo rolling63 è+8,77%,+4,11%,−18,58%. Questi fatti mostrano debolezza eterogenea, particolarmente marcata per RMA, e non dimostrano una rottura comune o crowding. Luglio è un taglio scelto retrospettivamente. Le serie ripulite oggi non ricostruiscono automaticamente ciò che mostrava il conto o un vecchio run.

![2026 e rolling63](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/figures/recent_2026.png)

Congelare prima dell'osservazione prospettica configurazione, universo ammesso, vintage del panel, unità raw eseguibili, ordine/timing, fonte benchmark, costi e registro delle eccezioni. Il confronto deve separare tre livelli:

| Livello | Frequenza e riferimento | Azione proposta | Condizione per proseguire/rientrare |
|---|---|---|---|
|Integrità|Prima di ogni decisione: timestamp, prezzi, panel/gate, hash e riconciliazione ordini/posizioni|Bloccare nuovi ingressi se una dipendenza necessaria è mancante, invalida o lo stato non riconcilia|Causa corretta e due controlli coerenti consecutivi; mantenere la stessa configurazione. Due controlli è una proposta operativa, non un segnale alpha validato|
|Esecuzione|Ogni seduta, riepilogo mensile: atteso/fill, prezzi raw coerenti con asta, partial/rifiuti, fee, debito/interessi|Riconciliare ogni errore che cambia posizione/rischio prima di nuovi ingressi; costi ripetutamente diversi richiedono revisione economica|Ripristino della fedeltà e causa documentata; nessuna cancellazione dei fill sfavorevoli|
|Economia shadow|Osservazione mensile dopo close della prima seduta;20/63/126 giorni,63primario; calibrazione solo sugli anni precedenti|Sotto il quantile5%: attenzione e diagnosi documentata, senza switch/stop automatico|Lo shadow continua anche durante sospensioni; un rientro economico automatico non è validato e richiede una policy separata prima di usarlo|

Le [soglie congelate per2026](../../../bt-strategy-test/strategy-selection/study_20261022/proposals/monitoring_2026_reference.json), calcolate sul solo2005–2025, sono circa−9,79%su63 sedute per A00,−10,02%per C1 e−9,80%per R2. Il dato noto al21 ottobre mette RMA sotto tale riferimento, AH/C1 sopra; non è un ordine retroattivo e il calendario mensile resta quello dichiarato. Ricalibrare nel2027 usando solo il passato concluso, senza ritoccare ogni mese la soglia appena superata.

Gli allarmi storici hanno frequenti recuperi shadow nelle successive63 sedute: **9/12 episodi A00,10/13C1 e3/8RMA completi; un ulteriore episodio RMA è censurato**. È la definizione operativa di falso allarme adottata, non una probabilità universale di errore. Finestre sovrapposte e pochi episodi limitano la precisione. La diagnostica di ritardo include anche microdrawdown, quindi non misura capacità di riconoscere una fine latente dell'edge. Non è giustificato trasformare automaticamente questi allarmi in cash o sospensioni economiche.

Per i costi registrare il costo delle due gambe e l'implementation shortfall rispetto a prezzi raw e orari comparabili, rapportati al nozionale di ingresso abbinato; gestire esplicitamente partial e finanziamento. Presentare media ponderata, distribuzione e incertezza per seduta. Se i costi plausibili attraversano gli scenari5/10 bps, la graduatoria resta incerta; se superano10 bps, manca il corrispondente scenario. Replay/paper non misura da solo impatto e slippage a pieno capitale.

Proporre126 sedute iniziali come cadenza organizzativa della prima revisione, non come garanzia di significatività; continuare se costi/eventi sono insufficienti, senza rituning opportunistico. Una sospensione per dati o posizioni errati può terminare sulla causa risolta. Per sospensione/rientro economici non esiste oggi una soglia validata: qualsiasi scelta discrezionale va registrata e non presentata come policy dimostrata. Il capitale resta dedicato alla crescita anche quando una causa tecnica impedisce temporaneamente nuovi ingressi.

## Limiti che impediscono una promozione forte

Universi correnti, delistati/membership non ricostruiti, quarantena full-history e selezione storica dei21 pesi/feature producono conditioning retrospettivo. Dieci serie stale/anomale per provider sono state escluse secondo il registro; la rimozione ex post aggiunge survivorship. Prezzi/volumi adjusted, corporate action e ADV non sono certificati economicamente. Un audit contabile riuscito non risolve questi punti.

Nessun holdout intatto è stato trovato; il bootstrap controlla la famiglia dichiarata, non tutta la selezione accumulata negli anni. I fill Open/Close daily non certificano priorità/quantità/costo d'asta o equivalenza con ingresso anticipato. Il finanziamento e il margine sono modellati. Lo stress hedge/C2 a10 bps e una nuova replica fixed-notional mancano: sono limiti dichiarati, senza altri tentativi oltre21.

Le evidenze che cambierebbero davvero scelta sono: unità e vintage verificabili, costi d'esecuzione misurati al capitale pertinente, nuove osservazioni prospettiche appaiate e una motivazione distinta per un nuovo controllo. Un'altra ottimizzazione sulla stessa finestra osservata non colma il deficit di indipendenza.

## Audit, domande e riproduzione

La [review indipendente](review.md) ricostruisce le metriche decisive senza importare il calcolo dell'analista; la [sintesi del Director](decision_synthesis.md) contiene anche l'esito di tutte le questioniQ01–Q20 e ciò che cambierebbe ciascuna risposta. La domanda aggiuntiva sulla leva è chiusa come confronto esplorativo negativo nello scenario dichiarato. Questioni non identificabili sono chiuse con limite esplicito, senza inventare una risposta.

Il [protocollo v0.4](../../../bt-strategy-test/strategy-selection/study_20261022/protocol.md), i gate, gli amendment, il [registro tentativi](../../../bt-strategy-test/strategy-selection/study_20261022/experiment_registry.jsonl), gli [output finali](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/run_metrics.json), il [notebook eseguito](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/study_companion.ipynb) e la [procedura riproducibile](../../../bt-strategy-test/strategy-selection/study_20261022/README.md) permettono di risalire dalle conclusioni ai run. La versione originaria con HALF non conforme resta preservata e il controllo corretto non ha riaperto la selezione.

Non sono stati inviati ordini, modificati profili o schedulazioni, effettuati restart/deploy o promosse configurazioni. La ricerca fornisce una proposta condizionata e criteri di revisione; l'autorizzazione operativa resta distinta.
