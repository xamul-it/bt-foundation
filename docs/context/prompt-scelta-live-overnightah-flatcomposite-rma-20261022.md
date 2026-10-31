# Prompt di studio — decidere quale strategia sperimentare live tra OvernightAH, OvernightAHFlatComposite e RMAStrategy, con quali parametri e quando cambiare scelta

Documento del 22 ottobre 2026. Incollare il mandato seguente in una nuova sessione nel workspace `/home/htpc/backtrader`. Questo documento autorizza la ricerca descritta, non il deployment o modifiche operative.

## Obiettivo dello studio

Mettermi nelle condizioni di decidere **quale strategia sperimentare live oggi fra OvernightAH, OvernightAHFlatComposite e weekly.RMAStrategy, con quali parametri, e secondo quali regole continuare a usarla, ridurne l'esposizione, sospenderla o passare a un'altra**. La decisione deve poggiare su evidenze verificabili di robustezza nel tempo, comportamento recente ed eseguibilità, con una valutazione esplicita del rischio di scegliere una configurazione adattata ex post allo storico.

Il problema da risolvere è concreto: lo storico lungo e quello recente suggeriscono scelte diverse; non so quali protezioni dal drawdown conservino un beneficio, come interagiscano e se il peggioramento che osservo dopo luglio 2026 sia compatibile con la normale variabilità o richieda una diversa decisione. Voglio arrivare a una scelta motivata e a un metodo per rivederla quando arrivano nuove osservazioni.

Il risultato finale deve essere una **proposta decisionale utilizzabile**, che specifichi:

1. **Scelta iniziale:** strategia candidata e configurazione completa, alternative credibili e motivi per preferirla nel contesto attuale. Se nessuna è sufficientemente supportata, indicare quali evidenze mancano e quale osservazione prospettica permetterebbe di decidere.
2. **Scelta dei parametri:** metodo raccomandato per determinarli, storico da usare, parametri da mantenere stabili e criteri/frequenza degli eventuali aggiornamenti; spiegare perché questo metodo è più affidabile delle alternative confrontate.
3. **Regole dopo l'avvio:** cosa misurare, con quali riferimenti e frequenze, quali evidenze richiedono attenzione o un intervento e come rientrare. Quantificare falsi allarmi, ritardi e limiti del metodo, senza promettere di riconoscere con certezza la fine dell'edge.
4. **Decisione sullo switch:** stabilire se esiste una regola sufficientemente supportata per passare fra queste strategie e se conviene rispetto a mantenimento, riduzione dell'esposizione, liquidità o combinazioni semplici. Non dare per scontato che lo switch sia desiderabile.
5. **Scenari di capitale e rischio:** mostrare quali perdite, durate di drawdown, tempi di recupero, costi e limiti di capacità comportano le scelte proposte, affinché possa decidere successivamente budget ed esposizione. La mia disponibilità personale ad accettare quei rischi resta una decisione mia.

La mappatura di parametri e studi è il fondamento di questa decisione e la prima fase del lavoro. Lo studio è completo quando la proposta finale collega ciascuna raccomandazione alle evidenze, distingue ciò che è dimostrato da ciò che resta incerto e rende esplicite le condizioni che porterebbero a cambiare scelta. Un catalogo, una classifica di rendimenti o un insieme di nuove ottimizzazioni, da soli, non raggiungono l'obiettivo.

## Mandato di esecuzione

Conduci uno studio multiagente completo e riproducibile per orientare la scelta fra `OvernightAH`, `OvernightAHFlatComposite` e `weekly.RMAStrategy`, i relativi parametri e le regole di mantenimento, riduzione, sospensione o eventuale switch.

Non fermarti a un piano, a script non eseguiti o a una classifica di backtest. Parti dalla mappatura dei parametri e delle evidenze esistenti, individua le lacune, esegui gli esperimenti necessari e consegna conclusioni sottoposte a revisione indipendente. Anche nessuna strategia sufficientemente supportata, nessun beneficio dello switch o evidenza insufficiente sono esiti validi.

Budget iniziale, esposizione e rischio accettabile NON sono prerequisiti da chiedere all'utente: devono emergere dallo studio come scenari e compromessi espliciti. Non inventare una preferenza dell'utente e non indicare un unico capitale ottimale senza un criterio giustificato.

## Domande iniziali, non esaustive

Le domande seguenti sono spunti formulati dall'utente per spiegare il problema. NON delimitano il perimetro della ricerca e NON costituiscono una checklist sufficiente a dichiararla conclusa. L'obiettivo decisionale guida lo studio: devi individuare anche le domande non formulate che risultano necessarie per raggiungerlo, mettere in discussione le premesse iniziali e approfondire criticità o opportunità emerse dalle evidenze.

Durante la mappatura, il Research Director costruisce e aggiorna un registro delle questioni rilevanti: domanda, motivo per cui può cambiare la decisione, evidenze disponibili, incertezza, priorità e verifica necessaria. Il Reviewer controlla anche le omissioni e le assunzioni implicite, non soltanto le risposte prodotte. Aggiungi autonomamente gli approfondimenti pertinenti al mandato, documentandone la motivazione; evita di estendere la ricerca a temi che non incidono sulla scelta.

1. Conviene calibrare sullo storico lungo, su finestre recenti o con un metodo adattivo? Quali parametri devono restare stabili e quali possono essere aggiornati, con quale frequenza?
2. Come distinguere drawdown normale, cambiamento di regime, perdita dell'edge e problemi di dati/esecuzione? Quali decisioni sono possibili e con quale incertezza?
3. Quale strategia e quale configurazione hanno il supporto più solido per una sperimentazione prospettica nel contesto attuale?
4. Una regola di switch migliora le alternative semplici dopo costi, ritardi, falsi allarmi e rischio di selezione?
5. Quali scenari di capitale ed esposizione risultano coerenti con liquidità, costi, capacità e distribuzione delle perdite?

## Contesto dell'utente: ipotesi, non risultati acquisiti
Gli studi sono stati compiuto dal 2000 al 2026.

- RMA appare incostante: primi anni storici deboli 2000-2010, accelerazioni successive, drawdown profondi e lunghi.
- OvernightAH appare più stabile ma usa anche universi statici selezionati retrospettivamente.
- FlatComposite usa ranking dinamico e appare avere DD contenuto; non assumere per questo che ogni sua componente sia esente da selezione ex post o che non utilizzi mai fallback statici.
- L'utente sospetta un deterioramento di OvernightAH e FlatComposite dopo luglio 2026. Verifica l'ipotesi senza imporre luglio come data di rottura dimostrata e senza attribuire il fenomeno al crowding in assenza di evidenze identificanti.
- La graduatoria fra strategie e parametri cambia tra storico lungo e 2023–2026.
- Molte protezioni sono nate in studi distinti; manca una sintesi delle loro motivazioni, validità attuale e interazioni. Esistono confronti parziali fra filtri: non assumere che manchi qualsiasi analisi congiunta.
- Gli studi passati restano utili anche se il codice è cambiato. Classificarne la trasferibilità, senza scartarli automaticamente né considerarli tutti direttamente confrontabili.

## Organizzazione multiagente e modelli

Usa ruoli espliciti e adatta costo e livello di ragionamento alla difficoltà. Rispetta il limite reale di concorrenza della sessione; con quattro slot usa questa organizzazione, riutilizzando gli slot fra fasi:

| Ruolo | Modello richiesto | Ragionamento | Responsabilità |
| --- | --- | --- | --- |
| Coordinatore | `gpt-6.1-sol` | Extra High (`xhigh`) | Stato persistente, integrazione, risorse, documentazione e consegna |
| Research Director | `gpt-6-astra` | High | Metodo, ipotesi falsificabili, protocollo, gate e conclusioni |
| Analista di codice/evidenze | `gpt-6.1-sol` | High | Parametri, ereditarietà, studi, configurazioni e provenienza dei risultati |
| Esecutore | `gpt-6-luna` | Medium | Inventari ed esperimenti secondo procedure già definite |
| Reviewer indipendente | `gpt-6.1-sol` | High | Audit metodologico e numerico, sostituendo uno slot dopo la produzione delle evidenze |

Se un modello non è disponibile, registra la sostituzione e usa il modello disponibile più adeguato. Non delegare decisioni metodologiche complesse a un esecutore senza review; promuovi a Sol i compiti che superano la semplice esecuzione. Non attendere artificialmente slot per attività che il coordinatore può svolgere. Mantieni un Research Director coerente lungo lo studio e forniscigli checkpoint sintetici. Nessun agente può approvare da solo le proprie evidenze decisive.

## Fase 1 — mappa dei parametri e degli studi

Questa è la prima consegna sostanziale. NON iniziare con un nuovo sweep o con il tuning.

### Inventario autorevole

Leggi le istruzioni locali e la documentazione pertinente, segui i rimandi ai report e agli artefatti originali. Verifica nel codice l'identità esatta delle classi: `weekly.RMAStrategy` non va confusa con altre RMA o con varianti overnight della famiglia.

Inventaria tutti i parametri effettivi, compresi quelli ereditati, sovrascritti, derivati e definiti nelle classi base; includi le impostazioni esterne che modificano economicamente il comportamento: universo, pannelli, feed, timeframe, warm-up, broker, costi e calendario. Distingui default dichiarato, override di sottoclasse e valore effettivo dei profili/run. Non leggere né riportare credenziali.

Per ciascun parametro o gruppo inseparabile registra:

- nome esatto, classe che lo dichiara e classi che lo sovrascrivono;
- significato attuale, unità, dominio, condizioni di attivazione e valore che lo disabilita;
- punto del flusso in cui interviene e dati temporalmente disponibili alla decisione;
- default e valori effettivi delle configurazioni rilevanti, con fonte e data;
- studio di origine o prima motivazione documentata, distinguendo origine verificata e semplice prima menzione trovata;
- ipotesi economica o operativa che intendeva affrontare;
- studi successivi favorevoli, contrari, correttivi e sospesi;
- periodi, configurazioni e modalità di esecuzione su cui esistono evidenze;
- dipendenze, interazioni documentate, ridondanze e possibili conflitti;
- stato dell'evidenza: verificata, esplorativa, negativa, superata, sospesa o origine non ricostruita;
- trasferibilità alla versione attuale e ciò che richiederebbe una nuova validazione.

Non presumere che ogni parametro abbia necessariamente uno studio dimostrabile: cerca la provenienza e segnala le lacune. Separa parametri economici da impostazioni tecniche, compatibilità storica e salvaguardie operative: non tutto è una variabile da ottimizzare.

Produci anche un registro degli studi con domanda, protocollo, versione/configurazione, dati, periodo, tipo di simulazione, risultati, limiti, decisione finale e riferimenti riproducibili. Ricostruisci le correzioni intervenute: un risultato precedente a un bugfix non deve diventare prova definitiva ignorando il seguito.

### Interazioni fra parametri

Costruisci una mappa delle dipendenze meccaniche e delle interazioni economiche candidate. La correlazione fra numeri scelti nei run non è una stima dell'effetto dei parametri: le configurazioni storiche non sono un esperimento randomizzato.

Distingui:

1. dipendenza imposta dal codice;
2. interazione già misurata in confronti controllati;
3. associazione osservata nei run;
4. ipotesi ancora da testare.

Priorità alle combinazioni che possono ribaltare la scelta: ranking/universo × filtri, hedge × cooldown, cap ADV × capitale × breadth, sizing × compounding, lag del segnale × modalità di esecuzione. Riduci il problema con motivazioni e piccoli disegni fattoriali mirati, senza lanciare il prodotto cartesiano di tutti i parametri.

### Run passati come validazione

Inventaria i run recenti Watchtower e gli artefatti locali disponibili attraverso accessi in sola lettura. I run possono validare la mappa e fornire evidenze economiche, ma non sostituiscono la ricostruzione dei parametri.

Per ogni run usato recupera identità strategia, parametri risolti, versione o fingerprint quando presente, dati, universo, date effettive, warm-up, capitale, sizing, costi, fill e artefatti. Classifica: comparabile, comparabile con riserve, solo descrittivo o non ricostruibile. Non chiamare indipendente una validazione ottenuta su dati già usati per scegliere parametri o ipotesi.

## Riferimenti iniziali ed esempi già emersi

Questi sono indizi di ricerca, NON un inventario esaustivo né conclusioni da adottare senza verifica:

- `docs/context/ah_context.md`: storia estesa di OvernightAH, studi su universi, mesi negativi, CUSUM, concentrazione, score, indicatori, composito, filtri, cooldown, hedge, sizing, liquidità e costi.
- `docs/overnight_ah_sintesi.md`: sintesi e rimandi agli studi iniziali.
- `docs/context/strategies/overnight-ah-blueprint.md`: spiegazione e parametri; confrontare sempre con codice e profili attuali.
- `docs/context/ah_cooldown_ahlag1_tuning_brief.md` e `docs/context/ah_order_sizing_liquidity_brief.md`: motivazioni dei relativi filoni; cercare gli esiti successivi, non fermarsi al brief.
- `docs/context/ah_flat_composite_deployment_brief.md`: piano storico di deploy. Il sorgente attuale di `overnight_ah_flat_composite.py` documenta successivi aggiornamenti del pannello e abilitazione paper: esempio concreto di documento iniziale che non basta a stabilire lo stato corrente.
- Studi 1–6 in `ah_context.md`: diversi esiti negativi per score/indicatori; conferma Backtrader del composito sul pool esplicitamente in-sample; confronti operativi successivi e interazioni fra filtri.
- `ah_context.md`, sezioni CUSUM: varianti inizialmente promettenti sui segmenti ma chiuse negativamente su prove continue e deployment realistico. Non riproporre lo stesso meccanismo come nuova soluzione senza affrontare quelle evidenze.
- `bt-strategy-test/overnight-ah/studies/adaptive_cooldown_20261014/`: distinguere diagnostica, proxy, controllo Backtrader e nuovi candidati non validati; consultare il registro decisioni e i report referenziati.
- `bt-strategy-test/overnight-ah/research/autonomous_rotation_20261017/final_report.md`: ricerca negativa su anticipazione e rotazione. Il report indica otto politiche native inferiori alla baseline nel confronto primario 2007–2025; non confondere rotazione tra titoli e switch fra le tre strategie.
- `bt-strategy-test/overnight-ah/research/autonomous_rotation_20261017/extension_20261018/final_report.md`: audit ADV, composizione del gain e residui di fattore. Riporta differenze del cap superiori al 10% nel 16,6261% dei confronti disponibili e nessun nuovo candidato. Verificare convenzioni, limiti e stato attuale della questione dati prima di usarlo economicamente.
- `docs/context/strategies/rma.md`, `bt-strategy-test/RMA/study_20261020/` e relativi rimandi: ciclo principale con nove challenger respinti. Distinguere la RMA originaria dalla successiva variante di sessione in `bt-strategy-test/RMA/session_overnight_20261021/`, che non sostituisce automaticamente una delle tre candidate.
- `docs/context/strategy-analysis-methodology.md`: convenzioni e requisiti di riproducibilità.
- `docs/context/watchtower_cron_monitoring_brief.md`, `docs/context/watchtower_completion_roadmap_2026-08-29.md`, `docs/watchtower-calculation-jobs.md`, documentazione dei parametri schedulati e codice Watchtower: fonti per verificare monitoraggio e provenienza dei run. La documentazione storica non certifica da sola la funzionalità attuale.

## Fase 2 — protocollo e lacune

Il Research Director deve trasformare la mappa e il registro aggiornato delle questioni rilevanti in un numero limitato di ipotesi falsificabili, selezionate per il loro impatto sulla decisione e non solo perché corrispondono alle domande iniziali. Per ogni esperimento specifica prima degli esiti: domanda, confronto, variabili mantenute fisse, metrica primaria, criteri decisionali, intervalli di incertezza, budget di tentativi e condizioni di arresto. Il Reviewer controlla il protocollo prima degli esperimenti decisivi. Se la review respinge il protocollo, correggilo autonomamente nei limiti del mandato.

Congela manifest di codice, configurazioni e dati, inclusi modifiche locali rilevanti e pannelli esterni. Preserva il lavoro dell'utente; non usare reset o pulizie distruttive. Lavora in directory di ricerca isolate e non sovrascrivere artefatti preesistenti.

Gli studi già validi devono restringere gli esperimenti, non essere ripetuti per convenienza. Se una lacuna dati impedisce una conclusione forte, produci le analisi condizionate ancora valide ed esplicita quali decisioni restano non supportate.

## Fase 3 — esperimenti

### Storico lungo contro recente

Confronta procedure di calibrazione: configurazioni congelate, finestra crescente, finestre recenti motivate e alternative più semplici. Ogni scelta deve usare esclusivamente informazioni anteriori al periodo di applicazione. Usa walk-forward annidato quando c'è selezione interna; purge e separazioni temporali commisurate a orizzonti e dipendenze dei dati. Distingui storia informativa delle feature e finestra usata per selezionare i parametri.

Mantieni coerenza fra warm-up, stato, patrimonio e allocazioni: concatenare rendimenti di backtest ripartiti da zero non equivale automaticamente a una strategia continua. Verifica nativamente il percorso adattivo finale.

Il 2023–2026 e il periodo post luglio 2026 sono già stati osservati. Cerca e documenta eventuali holdout realmente intatti; in loro assenza dichiara la natura retrospettiva della validazione e la necessità di conferma prospettica. Registra tutti i tentativi e controlla la selezione multipla in modo proporzionato al disegno. Non riaprire i gate dopo aver visto risultati deludenti.

### Protezioni e interazioni

Confronta i gruppi di protezione e poi le componenti decisive con esperimenti controllati. Misura drawdown, durata e recupero, code, esposizione, rendimento sacrificato, turnover, falsi arresti e occasioni perse. Presenta confronti sia alle impostazioni native sia a rischio/esposizione comparabili dove metodologicamente appropriato. Non scambiare la semplice riduzione dell'esposizione per capacità predittiva.

### Deterioramento e monitoraggio

Separa almeno tre livelli: integrità dati/configurazioni, qualità dell'esecuzione reale rispetto a replay coerente, comportamento economico del replay rispetto alle aspettative storiche.

Valuta gli strumenti Watchtower esistenti nel codice e negli output prima di proporre nuove funzioni. Misura copertura, calibrazione delle soglie, dipendenza temporale, numerosità effettiva, falsi allarmi e ritardo. Disegna regole di osservazione, riduzione, sospensione e rientro senza promettere certezza sulla morte dell'edge. Mantieni osservabile la performance teorica durante le sospensioni, se necessaria al rientro.

Analizza il periodo recente con confronti per epoca e regimi, dichiarando la selezione retrospettiva del punto luglio 2026. Un campione breve può sostenere solo una conclusione di evidenza insufficiente.

### Switch e alternative

Tratta lo switch fra strategie come una nuova policy da validare, con segnali ritardati, stato osservabile, costo del cambio, gestione delle posizioni aperte, permanenza minima, condizioni di rientro e frequenza decisionale espliciti.

Confrontalo con mantenimento delle singole strategie, liquidità/riduzione di esposizione e combinazioni a pesi fissi. Valuta dipendenza comune e correlazione nelle code: OvernightAH e FlatComposite condividono molta logica, quindi non presumere diversificazione. Escludi gli oracoli dalla scelta operativa; possono soltanto quantificare un limite teorico.

### Capitale, rischio ed eseguibilità

Presenta scenari motivati di capitale ed esposizione con costi, liquidità, vincoli ADV, cap nozionali, cash inutilizzato, breadth e sensibilità agli stress. Separa compounding e nozionale fisso. Non scegliere sulla sola equity finale o su moltiplicatori non confrontabili fra capitali.

Controlla lookahead e survivorship, membership storica, split/dividendi, unità dei prezzi e volumi, timestamp delle feature e proxy delle aste. Non dichiarare risolti questi problemi perché i conti interni tornano. Fornisci risultati condizionati quando non esistono dati certificabili.

## Motore e risorse

- Esegui le simulazioni economiche con `bt-core/.venv/bin/python btmain.py` da `bt-core/`, usando il motore Backtrader reale. Python/pandas sono ammessi per inventario, aggregazioni, diagnostica e inferenza sugli output, non come sostituti del simulatore economico.
- Ricostruisci comandi corretti dall'implementazione attuale; non inventare opzioni CLI. Registra comando, ambiente, manifest, output e stato di ogni run.
- Prima di lanci massivi misura un run pilota, CPU, RSS e spazio richiesto. Mantieni almeno DUE core liberi rispetto alle risorse realmente assegnate e margine RAM esplicito; considera thread numerici e lavori già attivi. Limita i worker di conseguenza, monitora memoria e riduci la concorrenza prima di swap/OOM. Parallelismo degli agenti e dei backtest sono budget distinti.
- Congela la data finale all'ultima seduta completa disponibile con dati verificati. Non includere barre parziali o dati futuri impliciti nei pannelli.
- Usa accessi Watchtower in sola lettura e output isolati. Nessun ordine Alpaca, cambio profilo, modifica di schedulazioni, restart/deploy, migrazione operativa o promozione automatica.
- Non modificare la logica delle strategie candidate per migliorarne i risultati senza documentare una variante distinta. Eventuali correzioni necessarie alla ricerca richiedono prove mirate e separazione fra replica e versione corretta.

## Documentazione e continuità

Crea `bt-strategy-test/strategy-selection/study_20261022/` per protocollo, manifest, registro esperimenti, configurazioni e analisi. Crea `docs/context/strategy-selection-study-20261022/` per l'indice leggibile, la mappa dei parametri e degli studi, lo stato e il report finale; usa collegamenti agli artefatti di ricerca senza duplicare dataset voluminosi.

La prima mappa deve essere consultabile prima delle nuove simulazioni, ma non fermarti ad attendere approvazione per proseguire le fasi già autorizzate. Mantieni un checkpoint che permetta di riprendere dopo interruzioni: completato, in corso, prossima azione, processi attivi, decisioni e limiti. Comunica progressi con risultati e incertezze, non soltanto attività svolte.

Consegna almeno:

1. inventario dei parametri con provenienza, semantica attuale e stato dell'evidenza;
2. registro degli studi, delle correzioni e delle lacune;
3. mappa di dipendenze/interazioni e matrice delle verifiche eseguite o mancanti;
4. catalogo dei run riusati e loro comparabilità;
5. protocollo preregistrato e registro completo dei tentativi, compresi fallimenti;
6. confronti delle procedure di calibrazione, protezioni, monitoraggio e switch;
7. scenari capitale/rischio/capacità e limiti di eseguibilità;
8. review indipendente con ricostruzione delle metriche decisive dagli output e discrepanze risolte o esplicite;
9. raccomandazione finale con configurazioni esatte, motivazioni, alternative respinte, incertezze e protocollo proposto per osservazione prospettica.

Il report deve spiegare come le evidenze consentano o impediscano di raggiungere l'obiettivo decisionale, rispondere alle domande iniziali e a quelle rilevanti emerse durante il lavoro, e motivare quali questioni restano aperte o sono state escluse. Rispondere soltanto alle cinque domande iniziali non basta a dichiarare concluso lo studio. Distingui solidità della ricerca, fattibilità tecnica e autorizzazione al live. Non proporre un vincitore quando l'evidenza non lo sostiene. Non fermarti alla mappatura: completala come fondamento dello studio e procedi autonomamente fino alle conclusioni verificabili.
