# Sintesi decisionale del Research Director

Aggiornamento 23 ottobre 2026. **Giudizio conclusivo su 20 configurazioni economiche e 1 pilota, con controlli completati, audit numerici e review complessiva indipendenti.** Questa sintesi non modifica protocollo, dati, gate o output. Le grandezze finali provengono da `analysis/final/run_summary.csv`, `contrast_summary.csv`, `screening.json`, `monitoring.json`, `capacity_diagnostics.json`, `leverage_comparison.json` e dalle ricostruzioni indipendenti sotto `reviews/`. La versione iniziale delle analisi è conservata separatamente; il vecchio controllo A00_HALF è non conforme all'intento di dimezzamento e rimane escluso dalle interpretazioni di esposizione ridotta.

## La decisione sostenibile con le prove attuali

**Non emerge una strategia universalmente superiore. Per un primo riferimento prospettico, A00 — OvernightAH dinamica con configurazione stabile — è la scelta più semplice da mettere alla prova, a condizione di verificare i costi effettivi della gamba overnight.** Questa è una priorità per acquisire evidenza, non la certificazione che A00 massimizzi il rendimento live. RMA resta un'alternativa economica credibile quando gli attriti aumentano; A01, AH con hedge, merita un confronto shadow per il compromesso rendimento/drawdown. FlatComposite non ha dimostrato un miglioramento stabile sul controllo matched; la configurazione C2 non isola il ranking perché cambia anche leva, breadth e gap.

L'utente considera l'intero conto capitale tecnico destinato al rischio e vuole massimizzarne la crescita. La decisione riguarda dunque **rendimento geometrico netto, sopravvivenza del capitale, recupero, finanziamento ed eseguibilità**, senza inventare riserve personali. Ridurre il budget è sensato solo se migliora queste proprietà o risponde a un problema operativo; avere meno cash investito non è di per sé il risultato desiderato. Allo stesso modo, essere disponibili ad accettare leva10 non dimostra che10 sia economicamente ottimale o eseguibile.

Una promozione forte resta impedita da unità storiche/ADV non certificate, membership corrente e quarantena ex post, prezzi/aste proxy, assenza di un holdout intatto e mancata misura dell'esecuzione reale. La conclusione corretta non è che i backtest non servano: restringono alternative, mostrano costi determinanti e permettono un protocollo prospettico concreto. Non autorizzano ordini o modifiche operative.

## Quanto contano costi, epoca e capitale

I run nuovi negoziano dal **3 gennaio 2005 al21 ottobre 2026**; il 2000–2004 è warmup. Il primario2010–2022 comprende 3.272 sedute. Il 2023–2026 era già stato osservato e resta robustezza retrospettiva/supplementare. Non descrivere lo studio come un portafoglio negoziato dal 2000 né attribuirgli la copertura economica della crisi dot-com.

| Configurazione | CAGR2005–2026, fee5 bps RT | MaxDD | Massimo tempo sott'acqua, sedute | CAGR con fee10 bps RT |
|---|---:|---:|---:|---:|
| A00 AH base | 30,93% | 36,01% | 632 | 15,46% |
| A01 AH + hedge | 32,13% | 26,24% | 611 | non simulato |
| C1 Composite matched | 31,57% | 39,91% | 735 | 16,69% |
| C2 parametri challenger target 2x | 39,53% | 44,73% | 606 | non simulato |
| R2 RMA causale | 22,19% | 42,82% | 1.134 | 21,15% |

DD è perdita dal picco; il massimo tempo sott'acqua non coincide necessariamente col recupero dell'episodio più profondo e comprende osservazioni censurate. Le cifre sono condizionate alle convenzioni del motore e non stime garantite dei rendimenti futuri. Il tasso di finanziamento 10% è comune a tutti i run; target 1x non è un tetto rigido alla leva effettiva. Anche A00 ha debito temporaneo e interessi. C2 usa costi proporzionali comuni, non la tariffa Alpaca completa.

La differenza fra5 e10 bps è decisiva: A00 passa da30,93% a15,46% di CAGR, mentre R2 da22,19% a21,15%. Questi scenari **invertono il ranking**. Non interpolare linearmente una soglia precisa di convenienza: costi, compounding, cap, breadth e fee cambiano insieme nel percorso. Prima di scegliere sulla curva più alta, occorre conoscere fee, spread/slippage, impatto e costo del debito ottenibili sul capitale considerato.

| Capitale iniziale | A00 CAGR / MaxDD | C1 CAGR / MaxDD | R2 CAGR / MaxDD |
|---|---|---|---|
| USD20.000 | 30,51% / 33,39% | 31,83% / 32,02% | 20,02% / 51,76% |
| USD200.000 | 30,93% / 36,01% | 31,57% / 39,91% | 22,19% / 42,82% |
| USD2.000.000 | 30,91% / 30,56% | 28,69% / 32,91% | 14,95% / 33,63% |

Sono scenari compound nativi, non tre livelli di capacità live certificati e non fixed-notional. I loro differenti percorsi cambiano breadth/cap e mostrano perché non si può scalare una sola curva. La perdita percentuale va applicata al patrimonio del relativo picco, non automaticamente al capitale iniziale. Quote ADV pianificate possono essere superate al fill; i superamenti osservati e le unità non certificate impediscono di trattare il cap come garanzia esecutiva.

## Parametri: stabilità prima di nuova ottimizzazione

Per il riferimento A00 mantenere la configurazione completa [A00.json](../../../bt-strategy-test/strategy-selection/study_20261022/configs/A00.json): weak_theme_switch mensile, top 50, base/theme 0,85/0,15 e corr12, gate SPY −10%, gap −0,20, lag 1, waterfall minimo 3/massimo 50, ADV 1%, cooldown 0/0, hedge off. Feed, universo di ricerca ripulito, warmup, timing, costi e impostazioni ereditate sono parte dell'identità; la breve lista non sostituisce il JSON. Il panel/universo di ricerca non diventa automaticamente una configurazione point-in-time certificata da portare live.

Mantenere stabili i pesi/feature Composite e i periodi RMA 180/30: lo studio non dimostra che siano ottimali, ma non trova evidenza sufficiente per ritunarli e conserva precedenti negativi. La scelta delle finestre di aggiornamento è stata limitata alle coppie AH cooldown off/on e RMA breadth 2/5; non generalizzare a ogni parametro.

Nel primario, AH expanding e trailing5y e RMA expanding coincidono con i riferimenti: delta0, test non identificabile, nessuna prova di superiorità. RMA trailing5y perde circa1,394punti di log-return annuo contro R2. **Nessuna procedura giustifica aggiornamenti annuali automatici nella classe testata.** Raccomandazione di processo: revisione documentata annuale della validità delle ipotesi e delle fonti, senza cambio automatico dei valori; modifiche soltanto su nuova motivazione, dati e confronto congelato.

## Protezioni e switch

Tutti i cinque contrasti core falliscono il gate congiunto originario. Questo gate richiedeva un incremento di almeno0,02 annuo log e altri requisiti; il suo fallimento non equivale a dichiarare inutile qualunque compromesso di rischio.

- Cooldown A10 vs A00: delta primario−0,07717 annuo log e DD peggiore di circa 2,99 punti. Non emerge una ragione economica per adottare automaticamente il cooldown prod0,01/3 sul riferimento corrente.
- Hedge A01 vs A00: delta+0,00667 annuo log e DD primario inferiore di circa 4 punti, ma limite inferiore simultaneo negativo e stress nativo dell'hedge assente. La tabella completa è favorevole come stima puntuale; **resta un'alternativa di rischio da osservare, non un vincitore validato**.
- A11 hedge+cooldown non recupera il rendimento perso; non sommare miglioramenti osservati in studi separati.
- C1 vs A00: delta+0,00337 annuo log, sotto il minimo, con DD primario peggiore di circa 3,98 punti. Il maggior CAGR puntuale completo non dimostra superiorità del ranking.
- R5 vs R2: delta−0,04013 annuo log e DD primario ridotto di circa 2,03 punti; la breadth modifica anche le uscite, non solo diversificazione.

S126 effettua31 cambi nel primario e non passa: delta circa−0,01183 vs A00, −0,01520 vs C1 e −0,00406 vs mix proxy. Il vantaggio contro RMA e cash non basta a superare le alternative richieste. Nessuna finalizzazione della policy è giustificata dal gate. Le serie restano screening sintetico, con costo di cambio prefissato e clock corretto, non conti nativi con cash/posizioni condivisi.

La famiglia prevista ha15 contrasti;12 sono identificabili, mentre i tre adattivi coincidenti col riferimento non forniscono una statistica standardizzabile. Il maxT è stato ricostruito indipendentemente. La sostituzione del controllo HALF non conforme è stata eseguita nella versione finale separata: S126 supera il corretto budget dimezzato di circa 11,32 punti di log-return annuo, con limite inferiore simultaneo circa +1,86 punti, ma i fallimenti necessari contro A00/C1/mix e dei quattro adattivi restano indipendenti da quel riferimento. **Non si riapre la selezione e nessuna policy viene promossa.**

Non si raccomanda uno switch automatico per inseguire il vincitore recente. È possibile cambiare decisione in seguito a nuova evidenza documentata; questo è un processo di revisione, non la policy S126 respinta. AH e Composite condividono logica e rischi: nel primario135 delle 164 osservazioni nella coda inferiore5% di A00 coincidono con la coda di C1, cioè82,32%; AH/RMA hanno37 su 164,22,56%. Sono frequenze descrittive sul calendario comune e soglie ex post, non garanzie di diversificazione futura. La dipendenza nelle code va letta con i denominatori effettivi e in entrambe le direzioni.

## Recente: cosa è misurato e cosa non è dimostrato

Dal1 luglio al21 ottobre 2026,79 sedute già osservate: A00 circa−0,03%, C1−4,44%, C2−14,83%, R2−30,41%. La finestra AH non è semplicemente una perdita persistente: l'ultimo rendimento rolling 63 sedute è+8,77% per A00 e+4,11% per C1, contro−18,58% per R2. La scelta di luglio dopo aver visto i dati e la breve finestra non identificano una rottura strutturale né crowding.

Una performance piatta dopo un semestre forte può essere economicamente importante senza dimostrare morte dell'edge. Anche una perdita estrema non distingue da sola regime, dati o errore di esecuzione. Nei nuovi run sono state rimosse ex post serie stale incluse nel dataset originario: queste curve non ricostruiscono automaticamente il conto reale o ciò che l'utente ha osservato nei vecchi run.

## Protocollo prospettico proposto, da congelare prima dell'osservazione

Queste sono regole di osservazione e controllo proposte; lo studio non le ha implementate operativamente. Il capitale del conto resta interamente capitale di rischio. I primi126 giorni di borsa sono una finestra minima di osservazione organizzativa, non una promessa di significatività; se costi/eventi sono insufficienti si continua senza rituning e si dichiara il limite.

| Livello / frequenza | Misura e riferimento | Attenzione o intervento | Rientro / prosecuzione |
|---|---|---|---|
| Integrità, prima di ogni decisione | Hash config/panel, vintage/mese, timestamp e prezzi validi, presenza gate, timezone, ordini/posizioni riconciliati | Bloccare nuovi ordini se manca una dipendenza necessaria, il prezzo è invalido/stale o lo stato non riconcilia; non trasformare il problema in un segnale economico | Correggere la causa e ottenere due controlli consecutivi coerenti su dati e stato; riprendere la stessa configurazione congelata. Due controlli è una regola operativa proposta, non alpha validato |
| Esecuzione, ogni seduta; riepilogo mensile | Ordini attesi vs fill, ritardi, rifiuti, fill parziali, prezzi raw eseguibili, commissioni, debito e interessi | Un errore che cambia posizione/rischio richiede riconciliazione prima di nuovi ingressi. Scostamenti di costo ripetuti richiedono riesame della convenzione5/10 bps, non switch automatico | Ripristinare fedeltà e documentare causa/effetto; non cancellare esecuzioni sfavorevoli dal campione |
| Economia shadow, controllo mensile | Rendimento netto20/63/126sedute,63 primario; soglia downside 5% stimata sul solo passato e aggiornata annualmente; DD e durata contro riferimenti congelati | Allarme implica diagnosi e revisione documentata. Non implica automaticamente cash: i falsi allarmi storici sono numerosi. Non aumentare leva per recuperare perdite | Mantenere lo shadow anche in sospensione; rientro tecnico sulla causa risolta, rientro da scelta economica soltanto con criterio prefissato e nuova evidenza, senza usare trade reali inesistenti |
| Scelta di parametri/strategia, annuale o dopo causa materiale | Stessa configurazione, nuove osservazioni, costi/capacità e revisione delle assunzioni | Nessun retuning mensile dalla curva recente. Un cambio materiale di dati, esecuzione o fattibilità giustifica un protocollo separato | Registrare decisione, data, alternative e condizioni future; non riscrivere la storia precedente |

Per i costi di esecuzione, registrare per ogni round trip completato commissioni e implementation shortfall su prezzi raw coerenti con l'orario/asta; dividere il costo complessivo delle due gambe per il nozionale di ingresso abbinato, con gestione esplicita dei partial. Presentare media ponderata, distribuzione e incertezza per seduta, non soltanto media per ordine. Finanziamento separato, già incluso nel NAV netto. **5 e10 bps sono scenari del confronto, non soglie validate di stop:** se l'intervallo dei costi osservati li attraversa, la graduatoria rimane incerta; se li supera, manca il corrispondente scenario e va riesaminata l'economia senza fingere che il10 bps sia ancora conservativo. Un replay/paper non certifica slippage e impatto live al capitale pieno.

Gli allarmi storici63 sedute mostrano9/12 episodi A00 e10/13 C1 seguiti da rendimento shadow63 sedute non negativo; R2 ha3/8 episodi completi e1 censurato. Sono falsi allarmi **nella definizione operativa adottata**, non stime indipendenti di probabilità universali. Il conteggio dei drawdown comprende microepisodi: ritardi e mancate rilevazioni non misurano sensibilità a una morte dell'edge latente. Questo esclude una raccomandazione di stop/rientro automatico dall'attuale diagnostica; non esclude controlli operativi in presenza di dati o posizioni errati.

Se l'utente scegliesse di ridurre temporaneamente il budget per rischio, il riferimento è il controllo corretto A00_HALF_BUDGET e la misurazione del capitale effettivamente esposto, non il parametro0,5 già risultato inefficace per questo scopo. La regola che decide *quando* ridurre/rientrare sarebbe una policy ulteriore da validare: il controllo statico non ne dimostra il timing.

## Fixed-notional e prove ancora mancanti

Le simulazioni nuove sono compound. Gli studi storici fixed-notional spiegano la dipendenza del risultato da sizing/capitale, ma non costituiscono la replica delle configurazioni correnti. Non è stata simulata oggi una nuova strategia a nozionale fisso; questo è un limite distinto dalla qualità dei18run e non può essere colmato dividendo il PnL per capitale iniziale.

Analogamente, benchmark AH C_prev→O_t e RMA C2C/multi-sessione descrivono gambe diverse: non usare il loro alpha come numero comune fra strategie. Mancano una ricostruzione point-in-time completa, corporate action/share units certificati, capacità delle aste e ledger reale comparabile. La crescita geometrica stimata resta condizionata a queste lacune.

## Esito delle venti questioni e cosa cambierebbe la scelta

| Q | Esito/limite sui18 core | Evidenza capace di cambiare la decisione |
|---|---|---|
| Q01 identità | Classi/default/profili risolti; configurazioni storiche diverse distinte | Nuovo profilo effettivo o mismatch runtime impone nuova identità |
| Q02 cutoff | Freeze21ottobre; nessun fill fuori finestra nei controlli revisionati | Nuovi dati completi successivi; mai barre parziali aggiunte retroattivamente |
| Q03 unità | Conti coerenti, eseguibilità/ADV storici non certificati | Prezzi/volumi/corporate action autorevoli e rerun coerente |
| Q04 selezione | Ranking dinamico conserva selezione ex post di universo/feature/pesi | Membership/vintage point-in-time e osservazioni prospettiche |
| Q05 holdout | Nessuno storico intatto dimostrato | Dati futuri congelati e non usati per rituning |
| Q06 causalità | AH lag 1 e RMA next-open nativi; proxy policy distinta | Replay e fill reali con timestamp/prezzi confrontabili |
| Q07 run |18audit nativi puliti; catalogo storico con riserve | Ricostruzione di run/job/versione mancanti; ledger operativo |
| Q08 parametri | Mappa e17famiglie di studi; molti default senza origine completa | Nuova motivazione economica distinta e confronto preregistrato |
| Q09 calibrazione | Nessuna delle quattro procedure migliora il riferimento; tre coincidono | Nuovo campione prospettico, non nuova finestra scelta dai medesimi risultati |
| Q10 protezioni | Cooldown non supportato; hedge promettente ma non confermato; HALF corretto riduce rischio sacrificando crescita | Stress hedge e validazione distinta di quando ridurre/rientrare; nessun tuning per recuperare il negativo |
| Q11 recente | Debolezza eterogenea, particolarmente RMA; luglio non break identificato | Persistenza futura appaiata, dati/esecuzione coerenti, altra epoca confermante |
| Q12 allarmi | Falsi allarmi rilevanti; nessun detector di edge death validato | Calibrazione prospettica con episodi completi e costi delle azioni |
| Q13 rientro | Shadow necessario; nessuna regola economica automatica validata | Policy nativa stop/rientro prefissata e beneficio netto verificato |
| Q14 switch | S126 non supportato anche dopo riferimento HALF corretto; mix/switch restano proxy | Nuova ipotesi distinta e percorso nativo, non recupero della policy respinta |
| Q15 diversificazione | Dipendenza/coda comune misurata descrittivamente | Evidenza stabile di complementarità nei regimi e capitale ripartito nativo |
| Q16 capitale |20k/200k/2M nativi; unità/capacity limitano estrapolazione | ADV eseguibile, aste e impatto misurati alle quantità effettive |
| Q17 perdite/recupero | DD e tempi sott'acqua quantificati; code future non limitate dal massimo storico | Stress realistici di gap/margine e nuova storia; non promessa di rovina zero |
| Q18 scelta | A00 riferimento prospettico semplice; né metà budget né target 2x migliorano la crescita nei confronti eseguiti; nessun vincitore universale | Costi reali e nuova evidenza prospettica; altra leva/costo richiederebbe domanda e protocollo distinti |
| Q19 metriche | NAV netto primario, gain/benchmarks distinti; funding esplicito | Correzione documentata di return basis o timestamp, con audit e conservazione dei vecchi output |
| Q20 trasferibilità | Correzioni storiche preservate; HALF classificato non conforme | Rerun controllato di una correzione necessaria, senza attribuirla a nuovo alpha |

## Budget ridotto e leva: i due controlli finali

Entrambi i controlli sono stati autorizzati da amendment prima dei rispettivi esiti, eseguiti nel motore nativo e verificati indipendentemente. Il totale è21 tentativi:18 core,1 pilota,1 riparazione del controllo e1 confronto leva richiesto dallo steering. Nessun'altra simulazione è prevista.

**A00_HALF_BUDGET** dimezza la base nota passata alla waterfall mantenendo gli altri parametri A00; non altera il NAV e non accantona ricchezza personale. La proxy di deployment nel primario scende a circa 0,488 NAV e l'audit non trova debito/interessi, contro circa 0,976 del vecchio HALF non conforme. Il portafoglio corretto produce:

| Finestra | CAGR metà budget | MaxDD metà budget | CAGR A00 | MaxDD A00 |
|---|---:|---:|---:|---:|
| Primario2010–2022 | 14,80% | 14,79% | 30,09% | 28,39% |
| Completo2005–2026 | 15,10% | 17,75% | 30,93% | 36,01% |

La riduzione funziona come riduzione di rischio, ma sacrifica sostanzialmente crescita geometrica nel campione. **Non la proporrei come configurazione iniziale per l'obiettivo dichiarato di massimizzare la crescita del capitale a rischio.** Resta un compromesso misurato se dovesse servire limitare rischio o drawdown; non dimostra una regola efficace che sappia quando attivarlo. I parametri e lo scopo sono nel [controllo corretto](../../../bt-strategy-test/strategy-selection/study_20261022/configs/A00_HALF_BUDGET.json).

**A00_LEV2_COST10** cambia soltanto max_exposure e broker leverage nominali da 1 a 2 rispetto ad A00_COST10; la semantica nativa passa anche da base cash a equity e può cambiare il regime della waterfall. Commissioni10 bps round trip e finanziamento 10% restano comuni: non è un semplice raddoppio della medesima curva.

| Finestra | CAGR target 2x | MaxDD target 2x | CAGR target 1x | MaxDD target 1x |
|---|---:|---:|---:|---:|
| Primario2010–2022 | 15,86% | 60,73% | 15,22% | 33,84% |
| Completo2005–2026 | 15,05% | 78,22% | 15,46% | 46,04% |

Il delta primario è+0,00553 annuo log, inferiore al minimo+0,02; l'intervallo bootstrap95% a blocchi63 è[−0,10758;+0,11698]. Le sensibilità21/126 non cambiano la mancanza di supporto; il delta sul percorso completo è negativo. La durata massima picco→recupero target 2x è2.172 sedute, contro 820 del target 1x: dal 9 febbraio 2006 al 26 settembre 2014 ci sono2.171 osservazioni strettamente sotto il picco e2.172 intervalli fino al recupero; sono convenzioni diverse sullo stesso episodio, non una discrepanza economica. Il massimo di leva broker registrato raggiunge2,51. Gli interessi cumulativi sono circa USD 1,715 milioni sul percorso compound: non sono il costo annuale di un conto costante 200k.

**La policy target 2x non è sostenuta economicamente nello scenario eseguito**, anche per un conto interamente dedicato al rischio: non migliora la crescita completa e peggiora fortemente perdita/recupero. Questo giudizio non nasce da una riserva personale o da un limite DD arbitrario. Il maggiore rendimento2026 da solo non ribalta il confronto; altri costi, altre forme di leva e10x restano non identificati, senza una conclusione di leva ottimale universale.

Il broker congelato usa maintenance margin 25%; la [documentazione ufficiale Alpaca](https://docs.alpaca.markets/us/docs/margin-and-short-selling), verificata anche il 23 ottobre, indica 30% per posizioni long con prezzo superiore a USD 6, requisiti maggiori per altre condizioni e margine iniziale almeno 50% sui titoli marginabili. **Il modello locale non replica questi vincoli correnti** e il finanziamento 10% non è il tasso personale del conto. Si veda la [verifica di eseguibilità](execution_constraints.md). Nessun mancato evento di rovina nel campione garantisce rischio di rovina futuro nullo. Il [confronto](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/leverage_comparison.json) e la [ricostruzione indipendente](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/leverage_comparison_independent.json) conservano tutti gli esiti, senza riaprire i gate originali.

## Firma scientifica

Giudizio del Director aggiornato dopo gli audit numerici: **ricerca condizionata alle convenzioni; nessuna superiorità universale o promozione automatica.** A00 stabile è il riferimento prospettico preferibile per semplicità e crescita osservata a costi contenuti; la graduatoria cambia con attriti elevati, mentre hedge e alternativa RMA conservano ruoli distinti. Non attivare retuning o switch dalle prove attuali, non dimezzare il budget per una riserva personale inesistente e non aumentare la leva al target 2x nello scenario negativo verificato.

**Firma scientifica conclusiva — Research Director, 23 ottobre 2026.** La [review indipendente finale](review.md) esprime `READY WITHIN REVIEWED SCOPE` su conti, clock, screening, controlli, conclusioni e copertura Q01–Q20. La differenza di conteggio 2.171/2.172 è riconciliata nelle due convenzioni esplicite. I conti decisivi sono stati ricostruiti da un Reviewer distinto; la firma del Director ne recepisce l'esito senza sostituirsi alla verifica indipendente.

Lo studio è concluso entro il perimetro autorizzato: esiti negativi preservati, compromessi e lacune decisionali espliciti, protocollo prospettico proposto senza fingere dati futuri disponibili. Non è richiesta un'altra simulazione per chiudere questa ricerca. **Nessuna attività operativa è autorizzata da questa sintesi.**
