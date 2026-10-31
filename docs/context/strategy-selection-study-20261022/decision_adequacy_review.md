# Rettifica di adeguatezza rispetto alla decisione richiesta

Research Director, 23 ottobre 2026, dopo la contestazione dell'utente. Audit del rapporto fra domanda ed evidenza: nessun nuovo esperimento, budget, modifica di strategia o alterazione di protocollo/hash congelati.

**La contestazione è fondata. È stato completato un protocollo ristretto con risultati verificati; non sono stati risolti interamente i tre quesiti centrali del mandato.** La precedente firma che dichiarava concluso lo studio entro il perimetro autorizzato era troppo ampia. La correttezza dei numeri e la review indipendente del metodo eseguito restano valide; non rendono sufficiente un disegno che copre soltanto parte della decisione.

| Domanda effettiva | Cosa risponde lo studio | Cosa manca | Adeguatezza |
|---|---|---|---|
| Quale strategia/configurazione mettere in produzione, e perché? | Confronti nativi, costi/capitale, esiti negativi di alcune modifiche, scelta condizionata di un riferimento A00 e alternative | Superiorità produttiva di A00 non dimostrata; costo/esecuzione reali e confronti decisivi fra alternative incomplete | **Parziale: proposta motivabile, non scelta ottimale dimostrata** |
| È una fase contingente del mercato, crowding o perdita dell'edge? | Misure della debolezza recente, finestre storiche e allarmi con falsi positivi; integrità distinta da economia | Nessun test specifico di deterioramento residuo condizionato al mercato/regime; nessun dato identificante sul crowding | **Non risolta** |
| Parametri scelti sullo storico2000–2026 o sul recente2023–2026? | Quattro selettori annuali fra due bracci per famiglia, expanding/trailing5y; precedenti studi di tuning censiti | Nessuna comparazione delle due procedure richieste sulla calibrazione dei parametri economici, nessuna finestra recente3anni testata, pesi Composite fissi | **Non risolta dalla nuova sperimentazione** |

## 1. Scelta produttiva: proposta condizionata e prove che non vanno confuse

Se occorre formulare oggi una scelta di lavoro dai soli risultati disponibili, **A00 è una proposta ragionevole a target1x, stabile, senza cooldown/hedge aggiuntivi, soltanto nell'ipotesi di costi effettivi compatibili con lo scenario basso**. La motivazione è parsimonia, tracciabilità del riferimento e buona crescita nelle convenzioni testate. Non è una dimostrazione che A00 sia migliore delle alternative in produzione, né una raccomandazione motivata da cash personale: il capitale rimane interamente capitale tecnico di rischio.

Il confronto corretto delle alternative richiede le seguenti precisazioni:

- **A01 non è stata economicamente confutata.** Sul completo ha CAGR32,13% e DD26,24%, contro30,93% e36,01% di A00. Il delta primario è piccolo, l'intervallo include zero e manca lo stress nativo specifico. Il gate di miglioramento congiunto non passa, ma ciò non prova superiorità di A00. Una preferenza per A00 è qui una scelta di semplicità e minore dipendenza da un beneficio non confermato, non una vittoria empirica contro l'hedge.
- **C1 non domina A00, ma non è esclusa da ogni criterio di crescita.** A5bps RT ha CAGR completo31,57% contro30,93%, con DD peggiore39,91% contro36,01%; a10bps dà16,69% contro15,46%. Il confronto controllato non dimostra un miglioramento robusto del ranking. Questo impedisce una pretesa forte, non autorizza a ignorare il suo risultato puntuale.
- **C2 ha la crescita puntuale maggiore nella tabella nativa5bps**, CAGR39,53% e DD44,73%, ma combina score, breadth, gap e target2x. Il confondimento impedisce di attribuire il vantaggio al solo ranking; **non rende privo di senso valutare la configurazione intera come candidata produttiva**. Il disegno non le ha fornito un confronto completo matched/stress sufficiente a scegliere quel pacchetto. Il fallimento di A00 target2x a10bps non è una confutazione di C2 a5bps: sono configurazioni e costi diversi.
- **R2 è un'alternativa concreta quando gli attriti sono maggiori.** A10bps il CAGR completo21,15% supera A00/C1; a5bps è22,19% contro30,93%/31,57%. Le differenze di provider, universo, orizzonte e rischio restano esplicite. Il deterioramento recente di RMA non è una regola validata per scartarla oggi o per trasferirsi automaticamente verso AH.

Perciò la risposta produttiva onesta è **condizionale**, non una graduatoria universale: A00 come riferimento semplice nel caso di costi bassi; R2 come alternativa se i costi overnight effettivi sono vicini allo scenario alto; A01 e Composite restano alternative non definitivamente risolte. Non abbiamo dimostrato quale sia la migliore configurazione da avviare oggi indipendentemente da questi elementi. Questo è il limite della conclusione disponibile, non una richiesta di una preferenza personale di rischio come prerequisito.

## 2. Mercato contingente, crowding ed edge decay

I risultati recenti sono osservazioni, non una diagnosi causale. A00 quasi piatta dopo luglio, Composite negativa e RMA molto negativa descrivono differenze fra le serie; rolling percentile, falsi allarmi e sensibilità alla lunghezza della finestra non spiegano *perché* accadano.

Manca un confronto esplicito del rendimento recente con quello atteso **a parità di esposizioni a mercato/regimi, universo, composizione, costi ed esecuzione**. Non è stata stimata e validata una procedura che confronti oscillazione contingente con deterioramento residuo persistente, ne quantifichi il potere di rilevazione e separi il campione usato per scegliere il punto luglio. Il precedente studio sui residui di fattore riguardava una diversa previsione dello spread complemento/selezione: non chiude automaticamente questo quesito.

Il crowding richiederebbe evidenza aggiuntiva pertinente, per esempio mutamenti di domanda/partecipazione nelle aste, impatto e costo a parità di quantità, flussi o posizioni di operatori comparabili. Gli OHLCV e i rendimenti di queste strategie, da soli, non identificano quella causa. Non possiamo attribuire il deterioramento al crowding e non possiamo escluderlo. Mercato e deterioramento dell'edge possono inoltre coesistere: non sono necessariamente tre categorie mutuamente esclusive.

La risposta disponibile è quindi **causa non identificata**. La ricerca distingue problemi tecnici da performance di replay e misura anomalie, ma non distingue ancora in modo validato le spiegazioni economiche richieste. Questo è un lavoro mancante, non una prova che ogni diagnosi futura sia impossibile.

## 3. Storico lungo contro2023–2026: il confronto realmente eseguito

Le procedure adattive nuove sceglievano soltanto:

- AH: cooldown0/0 contro0,01/3;
- RMA: min_concurrent2 contro5.

Per queste coppie si confrontavano riferimento congelato, selettore expanding e trailing5y. Tre percorsi coincidono col riferimento e il quarto perde. **Questo risultato non risponde alla scelta dei parametri economici delle strategie sullo storico2000–2026 rispetto al recente2023–2026.** Non sono stati ricalibrati periodi/regressioni, score, gate, filtri e altri gruppi economici con quei due metodi; i21pesi Composite sono rimasti fissi. La finestra recente richiesta è circa3anni, mentre la procedura eseguita ne usava5.

I vecchi studi negativi restringono ragionevolmente lo spazio delle ipotesi e possono motivare il mantenimento di alcuni parametri. Non sostituiscono una comparazione attuale delle procedure di calibrazione richieste. Non era necessario ottimizzare ogni parametro o esplorare il prodotto cartesiano: era necessario selezionare gruppi decisionali motivati e confrontare le procedure pertinenti, spiegando ciò che rimane fisso e perché.

Una risposta supportata avrebbe richiesto una simulazione di decisioni storiche con calibrazione su tutta la storia disponibile fino a ciascuna data contro una finestra recente prefissata di3anni, separazione cronologica di selezione/valutazione e preservazione dello stato nativo per i percorsi decisivi. La calibrazione finale2000–2026 o2023–2026 potrebbe usare quei dati per una scelta *oggi*, ma non valutarne retroattivamente la superiorità sugli stessi anni già usati nel fit. Nessun holdout intatto viene creato semplicemente chiamando una finestra OOS.

La raccomandazione attuale di parametri stabili è quindi **una scelta conservativa rispetto all'incertezza di stima e alla selezione accumulata**, non l'esito dimostrato del confronto lungo-vs-recente richiesto. Conservativa riguarda qui l'evidenza, non una riserva personale o un rifiuto del rischio economico.

## Rettifica conclusiva

Il protocollo ristretto e i21tentativi autorizzati sono conclusi e verificati. **Il mandato decisionale originario è soddisfatto solo parzialmente.** Le tabelle Q01–Q20 coprivano formalmente tutte le domande, ma alcune risposte erano dichiarazioni di limite, non verifiche delle ipotesi centrali: non andavano usate come prova di adeguatezza complessiva.

La firma precedente viene rettificata nella [sintesi](decision_synthesis.md). I risultati validi non vengono annullati e i negativi non vengono riaperti. In questo turno non si avviano nuovi test e non si promettono evidenze che non esistono; le lacune sopra sono distinte dalle risposte effettivamente già supportate.

## Aggiornamento del mandato, 23 ottobre 2026

Successivamente alla rettifica, l'utente ha autorizzato una **estensione correttiva distinta**: copertura economica2000–2010, date esplicite, calibrazione dei parametri economici lungo-vs2023–2026 e diagnosi mercato/regime/residuo. La proposta è nel [protocollo dell'estensione](../../../bt-strategy-test/strategy-selection/study_20261022/extension_20261023/protocol.md), da sottoporre a gate indipendente prima degli esiti pertinenti. La ricerca di preistoria pubblica non è una simulazione economica e non dimostra ancora la copertura effettiva del portafoglio dalgennaio2000.

Il precedente freeze non conteneva dati prima del2000: chiamare2000–2004 soltanto warmup aveva escluso un periodo economicamente importante, specialmente perRMA. La nuova prova deve distinguere datafeed, warmup, readiness, primoordine/fill e date della metrica. Le prove nuove non cancellano i21tentativi e i gate negativi precedenti. **Lo scope decisionale completo rimane aperto** fino alle verifiche indipendenti dell'estensione; l'esistenza di un nuovo protocollo non viene dichiarata completamento del mandato.
