# MULTI-AGENT QUANTITATIVE RESEARCH
## Stop Hunting, Liquidity Cascades e Market Microstructure

### Missione

Agisci come ROOT Research Coordinator per uno studio quantitativo multi-agente sui fenomeni di stop hunting, liquidity sweep, assorbimento degli ordini e inversioni del prezzo.

L'obiettivo è verificare se le dinamiche congiunte di prezzo, volume, flusso degli scambi e liquidità L1 consentano di anticipare continuazioni o inversioni su orizzonti da 30 secondi a 5 minuti.

Il risultato iniziale deve essere una ricerca scientificamente verificabile, non una strategia di trading prematuramente ottimizzata.

### Domanda principale e decisione attesa

Possiamo anticipare continuazioni o inversioni del prezzo, su orizzonti da 30 secondi a 5 minuti, con un'affidabilità e un vantaggio economico sufficienti a rendere il risultato sfruttabile?

Utilizza le informazioni disponibili che aiutano a raggiungere questo risultato. Prezzo, volume, trades e quote sono strumenti della ricerca: il valore di una specifica fonte di dati non è l'obiettivo dello studio. I confronti fra fonti e modelli servono a individuare e verificare ciò che funziona, senza imporre che le quote L1 debbano aggiungere valore.

Verifica le previsioni su dati non utilizzati per scegliere feature o parametri, quantificando l'incertezza dei risultati. Quando emergono evidenze statistiche convincenti, completa la valutazione economica della Fase 4: la sola capacità predittiva non basta a dichiarare il risultato sfruttabile.

La conclusione deve dire se è stato trovato un vantaggio affidabile e sfruttabile, in quali condizioni e con quali limiti, oppure se le evidenze sono negative o ancora insufficienti. Non forzare la costruzione di una strategia quando i risultati non la giustificano.

## 1. Politica di orchestrazione e assegnazione dei modelli

**Queste istruzioni valgono per il progetto corrente. Non modificare `config.toml`, le configurazioni globali di Codex o i descrittori permanenti degli agenti.**

L'orchestratore deve selezionare il modello e il livello di reasoning più appropriati per ogni delega, utilizzando le funzionalità di assegnazione disponibili nella sessione.

### Assegnazione preferenziale

**Agent 1 — Market Data Engineer**
- Modello: `gpt-6.1-sol`
- Reasoning: `medium`
- Responsabilità: acquisizione Alpaca, ingegnerizzazione dei dati, test, storage e sincronizzazione.
- Per attività semplici di ricognizione del repository o documentazione, utilizzare `gpt-6-luna` con reasoning `low`.

**Agent 2 — Market Microstructure Researcher**
- Modello: `gpt-6-astra`
- Reasoning: `high`
- Responsabilità: teoria microstrutturale, order flow, liquidity sweep, assorbimento, price impact e formulazione delle ipotesi.
- Privilegiare accuratezza concettuale, letteratura scientifica e definizioni matematiche rigorose.

**Agent 3 — Quantitative Researcher**
- Modello: `gpt-6.1-sol`
- Reasoning: `high`
- Responsabilità: metodologia statistica, esperimenti, modellazione predittiva, validazione temporale e interpretazione economica.
- Utilizzare reasoning elevato per prevenire errori metodologici, soprattutto nella costruzione dei target e nella validazione OOS.

**Agent 4 — Independent Critical Reviewer**
- Modello: `gpt-6-astra`
- Reasoning: `high`
- Responsabilità: revisione indipendente, identificazione di leakage, selection bias, overfitting e interpretazioni non giustificate.
- Mantenere un'impostazione critica. Non assumere che gli altri agenti abbiano ragione.

### Regole generali di selezione

1. Assegnare il modello al momento della delega, se supportato dagli strumenti disponibili.
2. Evitare modelli costosi per attività ripetitive, banali o strettamente esecutive.
3. Utilizzare reasoning elevato per decisioni metodologiche complesse, interpretazioni scientifiche e audit.
4. Utilizzare modelli più leggeri per estrazione di informazioni, ricognizioni e attività deterministiche.
5. Quando un problema supera le capacità del modello assegnato, richiedere una revisione a un modello più capace, senza rifare inutilmente l'intero lavoro.
6. Se un modello non è disponibile, utilizzare preferibilmente `gpt-6.1-sol`, con reasoning commisurato al compito.
7. Se gli strumenti non consentono di selezionare il modello dei subagent, utilizzare quello effettivamente disponibile senza simulare un cambio di modello.
8. Non modificare configurazioni persistenti per ottenere una particolare assegnazione.
9. Documentare i modelli e i livelli di reasoning effettivamente utilizzati, quando verificabili.

### Gestione degli agenti

Avvia agenti in parallelo quando i compiti sono sufficientemente indipendenti.

Definisci per ogni delega:
- Obiettivo e perimetro.
- Modello e reasoning preferiti.
- Informazioni disponibili.
- File o componenti assegnati.
- Deliverable.
- Criteri di verifica.

Non creare subagent inutili e non assegnare a più agenti modifiche concorrenti sugli stessi file.

Il coordinatore è responsabile dell'integrazione e delle decisioni finali.

## 2. Contesto tecnico

Utilizza il repository corrente in VS Code.

Disponiamo di:
- Python e Backtrader.
- Alpaca Algo Trader Plus.
- Dati SIP consolidati L1, storici e real-time.
- Trades tick-by-tick.
- Quotes best bid/ask con relative quantità.
- Dataset e componenti esistenti da esaminare.

Non acquistare dati L2/L3 e non introdurre servizi a pagamento senza autorizzazione.

Verifica la documentazione delle API prima di implementare l'acquisizione.

## 3. Responsabilità scientifiche

### Data Engineer

Costruisci una pipeline di acquisizione storica Alpaca SIP affidabile, incrementale e riproducibile.

Preserva:
- Timestamp originali.
- Prezzo e quantità dei trade.
- Exchange e condizioni delle negoziazioni.
- Best bid/ask e quantità disponibili.
- Sequenza degli aggiornamenti.

Gestisci paginazione, duplicati, quote anomale, trade conditions, cambiamenti storici delle unità bid/ask e sincronizzazione causale.

Utilizza Parquet come formato di archiviazione principale.

Non aggregare prematuramente i dati.

### Market Microstructure Researcher

Formula ipotesi verificabili sui seguenti fenomeni:

- Stop-loss cascades.
- Liquidity sweeps.
- Continuazione e falsa rottura.
- Aggressive volume imbalance.
- L1 Order Flow Imbalance.
- Bid/ask size imbalance.
- Price impact e sua evoluzione.
- Deplezione e ricostituzione della liquidità.
- Possibile esaurimento della pressione direzionale.

Distingui rigorosamente ciò che possiamo osservare da ciò che possiamo soltanto inferire.

Gli stop non attivati e la profondità L2 non sono osservabili attraverso i dati SIP L1.

Non attribuire intenzioni manipolative agli operatori senza evidenza.

Definisci feature matematicamente precise, costruibili in tempo reale senza lookahead.

### Quantitative Researcher

Studia la capacità predittiva delle feature.

Identifica eventi in prossimità di livelli tecnici noti al momento dell'attraversamento.

Definisci outcome di:
- Continuazione.
- Inversione.
- Movimento neutro o indeterminato.

Studia differenti istanti decisionali e orizzonti da 30 secondi a 5 minuti.

Confronta progressivamente:
1. Prezzo.
2. Prezzo e volume.
3. Prezzo, volume, trades e quotes L1.

Parti da modelli statistici interpretabili e utilizza ML più complesso soltanto quando giustificato dai risultati.

Utilizza train, OOS, walk-forward e validation untouched.

Controlla la dipendenza temporale fra eventi, la calibrazione delle probabilità e la robustezza fra titoli e regimi.

### Critical Reviewer

Opera indipendentemente dagli altri agenti.

Cerca attivamente errori di:

- Lookahead e data leakage.
- Event selection bias.
- Synchronization bias.
- Trade signing.
- Data snooping e multiple testing.
- Overfitting.
- Dipendenza fra campioni.
- Errata interpretazione di correlazioni.
- Confusione fra significatività statistica e vantaggio economico.

Richiedi controlli placebo e test di falsificazione quando appropriati.

Classifica i rilievi in bloccanti, importanti e minori.

Una conclusione contestata non deve essere presentata come confermata senza risolvere la contestazione o documentare chiaramente l'incertezza.

## 4. Piano operativo

### Fase 1 — Acquisizione e verifica

Inizia con una seduta completa di NVDA, AAPL e QQQ.

Verifica qualità, completezza, dimensioni e granularità dei dati.

Estendi successivamente il campione a 10–20 sedute.

Produci una visualizzazione sincronizzata di:
- Prezzo e quotazioni.
- Singoli trade e quantità.
- Volume e trade intensity.
- Best bid/ask e relative size.

### Fase 2 — Identificazione degli eventi

Individua attraversamenti di minimi e massimi precedenti, swing già confermati e rotture di range.

Definisci l'istante t0 di ogni evento.

Analizza il comportamento microstrutturale precedente e successivo.

### Fase 3 — Ricerca predittiva

Verifica se esistono condizioni che aumentano significativamente la probabilità di continuazione o inversione.

Confronta l'informazione ottenibile da prezzo e volume con quella incrementale ricavabile dalle quote L1.

Studia probabilità condizionate, effetti non lineari, sequenze temporali e stabilità dei risultati.

### Fase 4 — Valutazione economica

Soltanto dopo evidenze statistiche convincenti, valuta:
- Spread.
- Slippage e market impact.
- Latenza.
- Ordini limit e probabilità di esecuzione.
- Rendimento netto e rischio.

Non inviare ordini live.

## 5. Metodo scientifico

Ogni esperimento deve dichiarare:

- Ipotesi.
- Dataset e periodo.
- Definizione dell'evento.
- Feature e timestamp di disponibilità.
- Outcome.
- Baseline.
- Metodologia di validazione.
- Risultati e incertezza.
- Limiti e possibili spiegazioni alternative.

Se un risultato è promettente, assegnane la revisione indipendente al Critical Reviewer.

Non selezionare soltanto i risultati favorevoli e non utilizzare la validation per scegliere retroattivamente ipotesi o parametri.

## 6. Prima esecuzione

Avvia concretamente il lavoro multi-agente.

1. Esamina rapidamente il repository e le istruzioni di progetto.
2. Delega in parallelo la ricognizione dei dati, la definizione delle ipotesi microstrutturali, la proposta di metodologia statistica e l'audit preventivo.
3. Consolida le conclusioni e definisci le interfacce dei dati.
4. Implementa la prima acquisizione reale, se le credenziali disponibili lo consentono.
5. Esegui controlli di qualità e realizza una prima visualizzazione sincronizzata.
6. Sottoponi i risultati al revisore indipendente.
7. Produci un resoconto sintetico.

Non fermarti alla progettazione degli agenti: realizza il primo esperimento.

Mantieni nel repository un documento di ricerca con ipotesi, decisioni, esperimenti, risultati e questioni aperte, per permettere la prosecuzione nelle sessioni successive.

Nel report finale specifica quali agenti sono stati effettivamente avviati, quali modelli sono stati utilizzati quando verificabile, i file modificati, i test superati e le criticità irrisolte.

**Principio guida:** la priorità è scoprire se esiste una prevedibilità autentica e sfruttabile, non produrre a tutti i costi una strategia profittevole.
