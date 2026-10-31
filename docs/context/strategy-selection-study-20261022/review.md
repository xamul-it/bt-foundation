# Revisione indipendente finale dello studio

**Esito revisionato: READY per conti e screening congelato; INCOMPLETO per la decisione di produzione richiesta.** Lo studio è riproducibile e sufficiente per chiudere i confronti storici eseguiti. Non identifica perché la produzione si comporti diversamente, non distingue contingenza recente da decay/crowding, non confronta in modo ampio storico lungo e calibrazione recente e non valida una regola economica di intervento. Non dimostra un vincitore universale, non certifica eseguibilità live e non autorizza ordini, profili, scheduler o deploy.

Reviewer sostitutivo `gpt-5.6-sol` high, 23 ottobre 2026. Il modello richiesto `gpt-6.1-sol` e il primo sostituto `gpt-6-sol` hanno raggiunto i rispettivi usage limit; la sostituzione è ammessa dal mandato e non viene presentata come equivalente. La review non ha avviato simulazioni. Ha scritto soltanto gli audit sotto `reviews/` e questo documento.

## Giudizio sui gate

| Area | Esito indipendente | Conseguenza |
|---|---|---|
| Freeze e identità | PASS | `verify_frozen.py` ricontrolla 294 hash senza failure. Protocollo, schema, runner nativo, batch e preregistrazione conservano gli hash approvati. |
| Completezza nativa | PASS | I 18 run originali e i due controlli autorizzati dagli amendment sono completi; il pilota resta distinto. I 21 slot complessivi sono esauriti. |
| Contabilità | PASS | NAV, returns, cash, mark, fee e interessi riconciliano nei tape nativi; nessuna anomalia materiale di fill o ordine. |
| Clock e screening | PASS dopo correzione pre-output | La scelta usa informazione fino a `t-2`, diventa effettiva a `t+1`; costo e dwell decorrono dalla data effettiva. Il primo giorno conserva il braccio precedente o cash. |
| Molteplicità | PASS | Famiglia maxT unica di 15 contrasti, 12 identificabili; nessun pass imputato ai tre contrasti a varianza zero. |
| Controllo metà budget | PASS tecnico dopo repair | `A00_HALF` originario non misurava metà esposizione. `A00_HALF_BUDGET`, autorizzato prima dell'esito, dimezza davvero la base di sizing. |
| Confronto leva | PASS tecnico, esito economico negativo | Target 2x a 10 bps RT e funding 10% non supera il requisito di crescita e peggiora fortemente il rischio. |
| Decisione | INCOMPLETA rispetto al mandato forte | Nessun candidato o switch passa il gate congiunto. A00 può essere il riferimento di una raccolta dati prospettica soltanto se costi e capacità effettivi sono compatibili; lo studio non supporta una preferenza forte per la produzione oggi. |

Fonti di governo: [protocollo](../../../bt-strategy-test/strategy-selection/study_20261022/protocol.md), [gate v0.4](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/protocol_gate.json), [ruling sul clock](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/timing_clarification.md), [ruling sul finanziamento](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/financing_convention_clarification.md), [amendment metà budget](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/half_budget_control_amendment.md) e [amendment leva](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/leverage_user_amendment.md).

## Audit nativo e contabile

L'audit [audit_native_batch.py](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/audit_native_batch.py) legge direttamente returns, equity, ordini, submission, trade e stato finale. Non importa funzioni da `scripts/analyze_results.py`. Ogni run contiene 6.741 osservazioni native dal 2000 e 5.485 osservazioni dal `trade_start` 3 gennaio 2005; il primario 2010–2022 contiene 3.272 sedute.

| ID | CAGR252 primario | Sharpe rf0 | Max DD | Fill | Fee USD | Interessi USD | Stato al cutoff |
|---|---:|---:|---:|---:|---:|---:|---|
| A00 | 30,09% | 1,389 | 28,39% | 35.010 | 22.760.051,93 | 215.036,85 | cash; 6 intenti terminali |
| A10 | 20,43% | 1,116 | 31,39% | 27.632 | 6.009.906,08 | 54.086,61 | cash; 6 intenti terminali |
| A01 | 30,96% | 1,453 | 24,40% | 36.888 | 25.493.657,27 | 570.001,00 | cash; 6 intenti terminali |
| A11 | 20,70% | 1,163 | 27,92% | 28.486 | 6.237.989,62 | 107.699,86 | cash; 6 intenti terminali |
| C1 | 30,53% | 1,404 | 32,37% | 37.966 | 28.180.786,08 | 262.233,37 | cash; 6 intenti terminali |
| C2 | 36,37% | 1,137 | 44,69% | 38.362 | 106.803.439,09 | 14.349.285,08 | cash; 6 intenti terminali |
| R2 | 22,74% | 1,072 | 34,86% | 2.596 | 414.942,61 | 0 | 4 posizioni aperte |
| R5 | 17,92% | 1,001 | 32,83% | 5.473 | 227.355,75 | 0 | 7 posizioni; sell BIIB pendente |

Non risultano submission prima del trade start, fill oltre il cutoff, ordini `Partial`, `Margin` o `Rejected`, quote mancanti/non finite, fill su volume zero, holding o submission stale, oppure fee residue. Tutti i fill RMA sono a Open salvo le barre con Open uguale a Close; AH e Composite separano le gambe Open e Close. Il massimo residuo assoluto fra ledger ed equity tape è nell'ordine di 2e-6 USD.

I sei intenti AH/Composite creati il 21 ottobre sono tre coppie buy/sell senza notifica perché manca una barra successiva: non entrano in posizioni o PnL. R5 conserva sette posizioni marcate al cutoff e un sell BIIB che richiederebbe l'Open seguente. Questa censura è inclusa, non nascosta.

Il runner applica a tutti i run cash yield zero e margin rate 10%, con interesse `loan incrementale × 10% × giorni/360`. Target 1x e 2x non sono hard cap alla leva effettiva. A00 registra 4.041 debit-days, massimo broker leverage annuale arrotondato 1,22 e cash minimo −9.070.015,07 USD sul percorso composto. C2 raggiunge 2,36. Gli interessi sono mediatori della policy sotto una convenzione comune, non tariffe differenti fra strategie.

## Ricalcolo indipendente dello screening

[audit_decision_metrics.py](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/audit_decision_metrics.py) ricostruisce direttamente dalle serie native le procedure annuali, il mix a sleeve, S126 e il maxT. Usa 2.000 moving-block bootstrap, seed 20261022, blocco centrale 63 e sensibilità 21/126, indici comuni e senza wrap. La selezione annuale inizia nel 2010 e non vede il rendimento della data decisionale; il mix mantiene tre patrimoni iniziali separati, quindi non è una media giornaliera dei rendimenti.

Il confronto con [screening.json](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/screening.json) coincide su delta, statistiche annuali, bootstrap, quantili, clock e schedule; la massima differenza numerica osservata sul drawdown è circa 4,7e-15. I quantili maxT sono 2,616929 per 63 sedute, 2,557577 per 21 e 2,508234 per 126.

Dei 15 contrasti pianificati, 12 sono identificabili. `AH_expanding/A00`, `AH_trailing5y/A00` e `RMA_expanding/R2` sono identici, hanno varianza zero e non ricevono un pass artificiale. Nessuna delle policy richieste passa:

- A10/A00: delta log annuo −0,07717;
- A01/A00: +0,00667, limite simultaneo 63 negativo e incremento minimo non raggiunto;
- A11/A00: −0,07491;
- C1/A00: +0,00337, sotto il minimo e con DD peggiore;
- R5/R2: −0,04013;
- RMA trailing5y/R2: limite simultaneo 63 pari a −0,03607;
- S126 effettua 31 cambi ma fallisce i riferimenti necessari; il peggior limite richiesto è −0,09853 contro C1.

Il controllo metà budget corretto rende positivo S126/A00_HALF_BUDGET: delta log annuo +0,11320 e limite simultaneo +0,01863. Questo non riapre la selezione, perché S126 fallisce già contro A00, C1, mix e le procedure adattive richieste. Nessuna finalizzazione condizionale è autorizzata.

## Controlli aggiuntivi autorizzati

`A00_HALF` era stato etichettato come metà esposizione tramite `max_exposure=0,5`, ma la waterfall espandeva la breadth fino a circa 1x. Nel primario, buy-notional/NAV era 0,97561, quasi identico ad A00 0,97581; interessi e leva massima 1,22 confermano il mancato trattamento. L'artefatto resta conservato come `NONCONFORMING_HALF_EXPOSURE_CONTROL`.

`A00_HALF_BUDGET` usa un wrapper di ricerca che modifica soltanto `_sizing_equity` a metà della base parent, con `max_exposure=1`. Il gate controlla hash di config, wrapper, runner nativo e amendment; un ticket monouso impedisce l'esecuzione diretta del child. Il test esegue i due metodi reali congelati e verifica 100.000→50.000 sia con cap sia senza cap. L'audit nativo trova buy-notional/NAV primario 0,48787, nessun debito o interesse, CAGR 14,80% e DD 14,79%; sul percorso completo CAGR 15,10% e DD 17,75%. È un compromesso misurato di rischio, non la scelta iniziale coerente con l'obiettivo dichiarato di massima crescita netta del capitale dedicato.

`A00_LEV2_COST10` confronta target 2x con `A00_COST10`, mantenendo 10 bps round trip e funding 10%. La ricostruzione [audit_leverage_comparison.py](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/audit_leverage_comparison.py) coincide esattamente con l'output dell'analista: delta log annuo primario +0,00553, sotto il requisito +0,02; intervallo bootstrap 63 al 95% [−0,10758; +0,11698]; delta completo negativo. Target 2x produce CAGR/DD 15,86%/60,73% nel primario e 15,05%/78,22% nel completo, contro 15,22%/33,84% e 15,46%/46,04% del target 1x a 10 bps. Il massimo broker leverage osservato è 2,51 e gli interessi cumulativi sono circa 1,715 milioni USD sul percorso composto.

Il massimo episodio target 2x va dal picco del 9 febbraio 2006 al recupero del 26 settembre 2014, con trough il 1 giugno 2012. L'analista conta 2.172 intervalli di seduta picco→recupero; l'audit conta 2.171 osservazioni strettamente sotto il picco. La differenza di uno è una convenzione, non un disaccordo sul percorso.

La leva target 2x non è sostenuta nello scenario eseguito. Questo non identifica 10x né altre strutture/costi di leva. Il modello locale usa maintenance margin 25%, mentre i vincoli effettivi del conto e degli strumenti vanno verificati separatamente.

## Robustezza, capacità e monitoraggio

L'inversione fra scenari 5 e 10 bps è decisionale: sul percorso completo A00 passa da 30,93% a 15,46% di CAGR, mentre R2 passa da 22,19% a 21,15%. A costi bassi A00 ha la crescita storica maggiore fra i riferimenti semplici; a 10 bps R2 la supera. I due punti non consentono di interpolare una soglia precisa.

I run a 20k, 200k e 2M sono percorsi nativi con compounding. A 2M la prossimità e i superamenti del cap aumentano molto per AH/Composite, mentre R2 si degrada in modo diverso. Il calcolo ADV replica la sorgente congelata: per RMA con lag zero usa SMA a `[-1]` e prezzo Close corrente. Quote ADV e cap restano descrittivi perché unità share/volume, corporate action e capacità d'asta non sono certificate.

Nel primario, la coda inferiore 5% contiene 164 sedute per serie. A00 e C1 coincidono in 135 casi, cioè 82,32% condizionato alla coda A00; A00 e R2 in 37 casi, 22,56%. Le soglie sono separate per serie e le frequenze sono descrittive su calendario comune; non sono una garanzia di diversificazione futura.

Gli allarmi a 63 sedute danno 9 falsi allarmi operativi su 12 episodi completi per A00, 10/13 per C1 e 3/8 per R2, con un nono episodio R2 censurato. Il censimento include micro-drawdown e non calibra la morte latente dell'edge. Supporta diagnosi e shadow tracking, non stop/rientro automatico.

Le quattro [proposte risolte](../../../bt-strategy-test/strategy-selection/study_20261022/proposals/) A00, A01, C1 e R2 coincidono con i rispettivi `effective_strategy.json`; i loro blocchi `native_engine_context` coincidono con i config eseguiti e gli hash dichiarati sono corretti. Sono marcate `research_proposal_not_operational_configuration`, contengono path e campi runtime di ricerca e non vanno caricati come profili live.

Il [riferimento monitoring 2026](../../../bt-strategy-test/strategy-selection/study_20261022/proposals/monitoring_2026_reference.json) è coerente con le serie finali: 5.221 finestre rolling63 sovrapposte fino al 2025, soglie q05 congelate prima del 2026, ultima osservazione 21 ottobre e valori finali A00 +8,77%, C1 +4,11%, R2 −18,58%. La valutazione after-close rende il segnale disponibile solo alla decisione successiva e l'azione resta diagnosi, senza stop o switch automatici.

## Rettifica di adeguatezza decisionale — 23 ottobre 2026

Questa rettifica sostituisce il precedente giudizio di adeguatezza sulle conclusioni. Il precedente `READY WITHIN REVIEWED SCOPE` vale per correttezza contabile, clock, screening e controlli del protocollo ristretto; non vale come attestazione che il mandato decisionale centrale sia completo. La correttezza dei conti non basta a chiudere le tre domande centrali del mandato.

**Produzione oggi.** A00 prevale come riferimento semplice per robustezza storica simulata nello scenario di costo basso. Lo studio non dispone però di un estratto autenticato e comparabile di settlements, fill, commissioni, slippage e NAV della produzione, né certifica capacità d'asta e unità ADV. Non misura quindi il divario produzione→replay e non identifica se esso nasca da dati, esecuzione o strategia. “A00 come primo riferimento prospettico” è un disegno di raccolta dati, non una dimostrazione che A00 sia oggi la scelta di produzione.

**Contingenza contro decay o crowding.** Le 79 sedute dopo il taglio retrospettivo di luglio, le epoche e il rolling63 descrivono il percorso recente. Non esistono un change-point preregistrato, una variabile osservabile di crowding/flow, una decomposizione produzione-vs-replay o una causa identificante. I risultati escludono soltanto l'affermazione che luglio provi da solo un break; contingenza, decay, crowding ed errore di esecuzione restano indistinguibili.

**Storico lungo contro calibrazione recente.** Il [protocollo, righe 79–83](../../../bt-strategy-test/strategy-selection/study_20261022/protocol.md) limita esplicitamente expanding/trailing5y a due menu: cooldown off/on per AH e breadth 2/5 per RMA, senza tuning dei 21 pesi Composite. La finestra trailing5y è annuale e non costituisce una calibrazione specifica sul regime 2023–2026; tre procedure coincidono col riferimento perché il menu selezionabile è piccolo. Il risultato respinge l'aggiornamento automatico fra quei bracci. Non prova in generale che configurazioni stabili o storia lunga siano superiori a finestre recenti per universo, ranking, gap, hedge o pesi Composite.

**Allarmi e interventi.** L'implementazione in [analyze_results.py, righe 338–417](../../../bt-strategy-test/strategy-selection/study_20261022/scripts/analyze_results.py) calcola rolling 20/63/126, soglia q05 sul passato e definisce falso allarme quando il forward shadow63 è non negativo. Non stima l'utilità di stop, riduzione o switch rispetto al mantenimento dopo costi; non etichetta veri episodi di perdita dell'edge e non calibra un ritardo accettabile. Il codice stesso dichiara che i micro-drawdown sono descrittivi e non stimano edge-death detection. I pochi episodi sovrapposti supportano la conclusione negativa “nessun automatismo validato”, ma non forniscono una regola decisionale pronta.

Ne consegue che Q09 è coperta solo per i due menu preregistrati, Q11 resta non identificata, Q12/Q13 producono un protocollo osservativo senza policy economica validata e Q18 consente soltanto la scelta di un braccio di riferimento prospettico. Presentare tutte queste domande come chiuse senza tale distinzione eccede l'evidenza.

## Copertura Q01–Q20

| Q | Copertura revisionata | Esito |
|---|---|---|
| Q01 | classi, config, default, effective strategy e hash | Completa; le proposte riproducono le quattro identità considerate |
| Q02 | freeze 21 ottobre, calendario, warmup e cutoff tape | Completa; nessun fill oltre finestra |
| Q03 | ledger, quote fill, volume e limiti ADV | Contabilità completa; unità/corporate action non certificate |
| Q04 | universo e panel | Selection-conditioned e non point-in-time completo |
| Q05 | epoche e storia d'uso | Nessun holdout storico intatto; futuro richiesto |
| Q06 | lag, Open/Close e clock sintetico | Verificata causalità del tape; asta daily resta proxy |
| Q07 | 18 core, due amendment e pilota | Tutti completi e distinti; 21 slot usati |
| Q08 | mappa di parametri e letteratura interna | Coperta; origine economica di alcuni default resta incompleta |
| Q09 | expanding/trailing5y su due menu ridotti | Copertura parziale; nessun miglioramento nei bracci testati, nessuna conclusione generale lungo-vs-recente |
| Q10 | cooldown, hedge, half budget | Cooldown negativo; hedge promettente ma non confermato; half riduce rischio e crescita |
| Q11 | 2023–2025 e 2026 osservato | Descritta, causa non identificata; contingenza/decay/crowding restano indistinguibili |
| Q12 | soglie rolling e falsi allarmi | Diagnostica descrittiva; non valida detector, ritardo o utilità dell'intervento |
| Q13 | rientro e shadow | Protocollo osservativo proposto; policy economica non validata |
| Q14 | S126 e mix | Clock corretto, maxT replicato, gate fallito |
| Q15 | correlazioni e co-eccedenze simmetriche | Coperta con denominatori e caveat |
| Q16 | capitale 20k/200k/2M e cap | Scenari nativi; capacità live non certificata |
| Q17 | DD, recuperi, censura e leva | Coperta; nessun limite futuro alla coda o alla rovina |
| Q18 | scelta orientata alla crescita netta | A00 soltanto come braccio prospettico condizionato; nessuna preferenza di produzione forte identificata |
| Q19 | NAV netto, fee, funding, benchmark e proxy | Metriche riconciliate e definizioni separate |
| Q20 | correzioni e trasferibilità | Vecchio HALF preservato; repair autorizzato e auditato senza attribuirgli alpha |

## Limiti e decisione revisionata

Restano quattro limiti capaci di cambiare la decisione: universo/panel survivor-conditioned, assenza di un holdout storico intatto, unità share/volume e corporate action non certificate, e fill Open/Close come proxy d'asta senza impatto o probabilità di esecuzione. Adaptive, switch e mix sono screening sintetici; una policy positiva richiederebbe un conto nativo continuo con stato, cash, liquidazioni e costi propri.

Per il capitale interamente dedicato al rischio, la lettura più fedele entro i confronti eseguiti è:

1. usare A00 soltanto come riferimento shadow o braccio di una prova prospettica controllata se costi reali all-in e capacità risultano vicini allo scenario basso;
2. non convertire il vantaggio simulato di R2 a 10 bps in una scelta finché costo ed esecuzione effettivi non sono misurati sullo stesso clock;
3. mantenere A01 in shadow come possibile trade-off di drawdown, perché il miglioramento puntuale non supera il gate e manca uno stress nativo specifico;
4. non usare HALF come default di prudenza per cash personale e non aumentare a target 2x nello scenario studiato;
5. non attivare S126, retuning annuale automatico o switch dalla debolezza recente.

Questa è una scelta di riferimento per osservazione prospettica, non una promozione live né una risposta completa su quale strategia usare in produzione. La review approva conti, ricostruzione e chiusura negativa dei gate entro le convenzioni congelate; non approva come chiuse le domande su causa recente, calibrazione generale lungo-vs-recente e intervento economico. Ogni uso operativo richiede verifica separata di dati, account, strumenti, costi, margine, aste e riconciliazione dello stato reale.
