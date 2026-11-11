# Studio: selezione dinamica overnight con soglia SPY

## Decisione richiesta

Determinare se, e con quale configurazione, una selezione dinamica dei titoli rispetto a SPY renda più conveniente e robusta l'operatività overnight rispetto a OvernightAH e OvernightAHMultiFactor attuali. La consegna deve indicare quale strategia e quali parametri meritano un'eventuale prova prospettica, oppure concludere che nessuna evidenza supporta una scelta. Distinguere la raccomandazione di ricerca dall'autorizzazione a operare.

L'utente vuole ridurre la dipendenza da una lista di titoli scelta empiricamente circa sei mesi prima dello studio, senza imporre a priori una diversificazione che possa distruggere il vantaggio della selezione. Una platea più ampia dà la possibilità di adattarsi, ma non garantisce una maggiore diversificazione effettiva. Le domande iniziali sono una base, non un limite: aggiungere quelle necessarie alla decisione, motivando eventuali esclusioni.

Il lavoro è **un unico studio comparativo con due filoni coordinati**, ciascuno con la propria ipotesi, controlli e conclusione. I due filoni condividono dati, baseline, protocollo temporale e confronto finale; non è necessario duplicare la ricerca. Questa è una specifica per la ricerca, non la dichiarazione che sia già stata eseguita.

## Mandato e fonti

Leggere prima `AGENTS.md`, `docs/context/strategy-analysis-methodology.md`, `docs/context/ah_context.md` (in particolare studi sul ranking composite, validazione e prova su strada), `docs/overnight_ah_sintesi.md`, `docs/context/ah_flat_composite_deployment_brief.md` e `docs/context/prompt-scelta-live-overnightah-flatcomposite-rma-20261022.md`. Consultare i relativi output e codici originali, non assumere che un riepilogo storico sia ancora aggiornato. Riusare, se comparabili, studi e componenti esistenti.

Ricostruire provenienza, versioni, date e grado di indipendenza della selezione dei pesi Multifactor: lo studio documenta ottimizzazione, verifiche per blocchi e Backtrader; la sezione Studio 6 di `ah_context.md` riportava il blocco 2022-08–2026-08 non ancora utilizzato per la validazione blind e la successiva prova su strada comprendeva anche quel periodo. Verificare se esistano ulteriori studi o validazioni successive prima di attribuire ai pesi una qualifica OOS. Non trattare un periodo già osservato come holdout intatto, né negare una validazione senza averne cercato le fonti.

La lista static di OvernightAH nacque da osservazione empirica delle performance AH/PM di circa sei mesi prima ed è stata poi studiata nei run: segnalarne il rischio di selezione retrospettiva. Non confondere tale lista con l'elenco `monthly_universe_semis`, che costruisce il fattore di regime, né con l'universo di input del run.

Studiare backtest e dati operativi in sola lettura; non inviare ordini, modificare profili, schedulazioni, servizi, configurazioni live/paper o eseguire deployment. Preservare strategie e file dell'utente, output e dati operativi. Implementare varianti sperimentali distinte sotto `bt-strategy-test/` o in area di ricerca isolata importabile dal motore, senza cambiare semantica delle classi esistenti. Una futura integrazione in catalogo o promozione è separata da questo studio. Non chiedere autorizzazioni ulteriori per il lavoro di ricerca già esplicitamente avviato, né dedurne autorizzazioni operative.

## Baseline e perimetro

- **OvernightAH as-is**: variante `weak_theme_switch` con configurazione effettiva congelata; ramo dinamico e lista statica correnti.
- **OvernightAHMultiFactor as-is**: `weak_theme`, classifica composita del pannello, massimo `monthly_universe_top_n` come da configurazione.
- **AH-SPY**: conserva lo switch e il ramo dinamico di OvernightAH; solo quando l'attuale switch selezionerebbe il ramo statico, usa la lista dinamica relativa a SPY descritta sotto. Non modificare implicitamente il gate SPY esistente, la lista semis che guida il regime, sizing, hedge o altri filtri.
- **MF-SPY**: deriva dalla classe Multifactor senza switch semis. Applica la soglia sul punteggio SPY alla classifica multifattoriale, secondo il contratto sotto.

Il confronto principale contro ogni genitore cambia solo il criterio di selezione interessato, mantenendo dati, capitale, periodo, universo, gate, ordine, costi, leva e altri parametri identici. Per confrontare AH-SPY e MF-SPY fra loro, aggiungere una configurazione armonizzata e mostrare separatamente il confronto con le configurazioni native. Non attribuire differenze di hedge, cooldown, rischio o parametri temporali al nuovo ranking. I parametri vanno ricostruiti da `results.json` e codice effettivo, non inferiti dai nomi.

Run indicati dall'utente, da riusare come contesto e non come confronto controllato:

| Strategia | Lungo | Medio | Breve |
|---|---|---|---|
| OvernightAH | `c7d93c0e-552a-4bbd-97d8-0c220c1f5fb5` | `1846c59e-4fcf-4fb7-9ca5-5e07503c8c67` | `c0b9e13c-827d-4abe-b2e4-b21e123eb8e2` |
| Multifactor | `7295e02e-ee24-4986-b21b-23018850ba13` | `c7057bb2-1e9a-4ec5-a2c7-1ce34edd7b67` | `2fd58f84-0711-40ab-8227-1f0d164acd65` |

I run medi e brevi usano leva 2; i lunghi leva 1. Esistono altre differenze, tra cui hedge, cooldown e numero massimo di posizioni. I run brevi iniziano nel settembre 2026 e richiedono verifica del warm-up. Nel campione recente Multifactor ha selezionato prevalentemente semiconduttori: esaminarne concentrazione e cause senza assumere che il ranking sia errato. Le finestre recenti e i risultati forniti sono già osservati.

## Contratto causale comune

Il rendimento overnight è `r[i,t] = open[i,t] / close[i,t-1] - 1`, attribuito alla sessione di apertura `t`. L'operatività valutata è acquisto in chiusura e vendita all'apertura successiva; non confondere AH con rendimento close-to-close o intraday. Specificare il calendario, i timestamp e il cutoff effettivo per la decisione in funzione dei feed e del lag. Per l'aggiornamento mensile utilizzare esclusivamente sessioni già completate prima dell'inizio del mese, coerenti con il percorso operativo; testare esplicitamente transizioni di mese, weekend, festività, signal bar e date di esecuzione. SPY è solo riferimento/gate, non un titolo acquistabile per effetto della selezione.

Usare lo stesso provider e la stessa politica di aggiustamento per titolo e SPY; nessun incrocio raw/adjusted. Allineare le sessioni osservabili per ogni coppia titolo–SPY; una coppia di prezzi mancante, non valida o non negoziabile non equivale a rendimento zero. Requisiti minimi di storico, coverage, denominatori e comportamento dei dati insufficienti devono essere espliciti e congelati prima di valutare il test: escludere il titolo se non valutabile, registrando il motivo. Nessuna sostituzione con dati futuri; se SPY non è valutabile, la nuova regola non deve essere bypassata silenziosamente: nessun candidato per quel ramo e motivo osservabile. Distinguere questa regola dal comportamento preesistente del gate SPY, che resta invariato per i confronti col genitore.

Rispettare membership e disponibilità temporale dei titoli: niente operazioni prima di IPO o disponibilità documentata. L'universo attuale è soggetto a survivorship e selezione retrospettiva, anche se il ranking è mensile; non dichiarare risolto il bias. Usare membership point-in-time e delistati se esistono dati verificabili; altrimenti quantificare coverage e limiti, includere un'analisi di sensibilità fattibile e qualificare le conclusioni. Documentare split, dividendi/ex-date, cambi ticker, eventuali delisting e la relazione dei prezzi adjusted con l'esecuzione.

## Filone A — OvernightAH: rendimento overnight rispetto a SPY

Nel ramo che oggi è statico, considerare l'intero universo tradabile caricato, non soltanto la vecchia lista statica. Per ciascun titolo calcolare la stessa misura per titolo e SPY sulle sessioni comuni del lookback. Se misura(titolo) > misura(SPY), il titolo è candidato; ordinare per misura decrescente, con spareggio deterministico per simbolo. La soglia è stretta: parità esclusa. Loggare ogni mese simbolo, numero di osservazioni, valori di titolo e SPY, ammissione e ordine.

Confrontare tre criteri formulati prima degli esiti:

1. **Win ratio AH**: numero di notti con `r > 0` diviso il numero di notti valide. Zero non è una vittoria. Confrontare la win ratio del titolo con quella di SPY sulle stesse date. Questo non è la frazione di notti in cui il titolo batte SPY, che eventualmente può essere un'analisi esplorativa separata.
2. **Gain AH cumulato**: `prod(1 + r) - 1` sulle stesse notti valide; confrontare titolo e SPY. Un grande gap può dominare il risultato: riportarne il contributo.
3. **Sortino AH**: media dei rendimenti giornalieri AH in eccesso sul target giornaliero zero, divisa per `sqrt(mean(min(r, 0)^2))` sulle notti valide; stesso insieme di date per SPY e titolo, stessa convenzione e scala. Nessun tasso privo di rischio implicito. Se denominatore zero o campione insufficiente, metrica non valutabile ed esclusione, non infinito artificiale. Rendere visibile numero di notti negative e sensibilità della stima.

Orizzonti iniziali: **63, 126 e 252 sedute**; 63 è l'ipotesi iniziale, non un vincitore predeterminato. Ricalcolo mensile. Matrice di base 3 criteri × 3 lookback, con una sola combinazione per ogni esperimento e baseline as-is. Specificare prima del lancio limite dei candidati, ordinamento, dati incompleti, storico minimo e trattamento dei casi senza candidati. Come riferimento iniziale applicare `monthly_universe_top_n` esistente dopo il taglio, per evitare un limite nascosto diverso; riportare numero ammessi prima del cap. Un diverso numero massimo è un esperimento separato, non una modifica silenziosa.

## Filone B — Multifactor: punteggio relativo a SPY

Usare i pesi e la logica Multifactor congelati del run scelto; non riottimizzarli sulla finestra di valutazione. Calcolare indicatori di SPY con stesso dataset, trasformazioni, finestre e cutoff impiegati per i titoli. Inserire SPY nel medesimo insieme di ranking percentili dei titoli eleggibili, esclusivamente come riferimento. Applicare la stessa formula `sum(peso × rango)` a SPY e ai titoli; ammettere solo `score_titolo > score_SPY`, ordinare in modo deterministico e applicare il top-N esistente dopo il taglio. I titoli e SPY devono soddisfare le feature obbligatorie; mantenere la stessa regola di gestione delle feature facoltative mancanti della strategia corrente e descriverla. Se SPY non ha i dati obbligatori, nessun candidato e motivo esplicito.

L'inclusione di SPY nei ranghi cambia il contesto di calcolo: fare un controllo senza taglio che includa SPY nei ranghi per isolare l'effetto della soglia dall'effetto del ricalcolo. Conservare anche la baseline Multifactor originale che esclude SPY dai candidati/ranghi, verificando la semantica del codice. Documentare parità, tie handling, copertura delle feature, indicatori mancanti e impatto del riferimento sui ranghi; testare pannelli parziali e mesi assenti. I periodi delle feature del modello non sono il lookback 63/126/252 del filone A: non cambiarli o riottimizzarli implicitamente.

## Disegno sperimentale e regole decisionali

Prima degli esperimenti decisivi produrre e congelare un protocollo con calendario IS/validation/OOS, cutoff e manifest, budget di tentativi e risorse, confronto primario, metrica economica primaria, misure di rischio, criteri di selezione/accettazione e arresto. Motivare le soglie in relazione alla decisione e ai dati, senza inventare vincoli di rendimento o drawdown come preferenze dell'utente. Registrare tutti i candidati e i tentativi anche falliti; nessuna modifica ai gate dopo aver visto OOS. Se un periodo è già stato visto o usato nel processo di sviluppo, etichettarlo come osservato/retrospettivo. Se manca un holdout davvero intatto, impiegare walk-forward annidato con selezione interna rigorosamente antecedente, separazioni temporali commisurate a lookback e dipendenze e riferire la necessità di conferma prospettica; non chiamarlo blind intatto. Usare un'eventuale valutazione blind solo se effettivamente isolata e documentata.

Confrontare i nove candidati del filone A entro il budget preregistrato, tenendo conto della scelta multipla e della dipendenza tra finestre. La selezione di formula/lookback avviene su train/validation, non scegliendo il migliore in OOS; l'OOS valuta la regola già congelata e può respingerla. Non promuovere il miglior risultato di nove prove solo perché numericamente primo. Valutare stabilità e incertezza con confronti appaiati, intervalli e blocchi temporali motivati. In caso di segnali fragili o inconcludenti, riferire il risultato senza forzare un vincitore.

Riportare rendimento netto e geometrico, drawdown massimo e durata/recupero, Sharpe e Sortino secondo definizioni esistenti, volatilità, perdita di coda, turnover, trade, esposizione, cash non investito, frequenza di no-candidate e candidati esclusi, numero effettivo di posizioni, concentrazione per titolo e settore, e contributi al P&L. Distinguere breadth dei candidati da quella delle posizioni effettive e confrontare anche le esposizioni comuni. Includere costi uniformi e sensibilità 0/2/5/10 bps round trip, oltre a commissioni e finanziamento realistici quando disponibili, evitando doppi conteggi e dichiarando le assunzioni. Non trasformare le soglie indicative in un requisito economico rigido senza motivazione.

Usare come riferimento di sessione SPY close-to-next-open, con capitale ed esposizione definiti; confrontare inoltre le strategie genitrici. Se si adottano le metriche basate su somme di bps della metodologia comune, dichiararne scala e ponderazione e non confonderle con l'alpha di portafoglio investibile quando numero di posizioni o esposizioni differiscono. Mostrare anche confronto a esposizione/rischio comparabili e risultati con nozionale fisso oltre al compounding, con vincoli di liquidità e capitale motivati.

Riconciliazione disponibile come evidenza di esecuzione, non come calibrazione universale: il profilo live OvernightAH esaminato contiene 63 trade abbinati NVDA/AVGO/MU su 21 sessioni ottobre–novembre 2026, con scarto medio di prezzo vicino a zero; verificare fonti e copertura prima dell'uso. Non estenderlo automaticamente a Multifactor, titoli diversi o capitali maggiori; separare prezzi, commissioni, interessi e operazioni non abbinate. Paper non dimostra qualità del fill live. Rispettare la distinzione tra rendimento del backtest e profili operativi, che possono avere parametri diversi.

## Esecuzione, verifica e deliverable

Congelare codice, eventuale dirty diff, universe, prezzi, pannello, copertura SPY, configurazioni e hash in un manifest; registrare provider, date, timeframe, warm-up, benchmark, capitale, leva, margine, commissioni, slippage e comandi completi. Non esporre credenziali. Usare directory di lavoro e output isolati sotto `bt-strategy-test/overnight-ah/research/`, senza sovrascrivere artefatti esistenti; tenere in `docs/context/` indice, stato e conclusione leggibile.

Riutilizzare eventuali strumenti esplorativi per controlli e diagnostica, ma verificare economicamente le candidate e le baseline decisive con il motore reale `bt-core/.venv/bin/python btmain.py`, eseguito dalla directory `bt-core/`, con comandi ricostruiti dall'implementazione corrente. Non spacciare proxy pandas per risultato finale. Testare parità as-is, esclusione SPY dagli acquisti, feature/pesi/rank, assenza di lookahead, ordinamento e pareggi, cutoff, tie, mesi senza candidati, dati mancanti, switch e gate indipendenti, IPO/storico insufficiente e warm-up. Se un difetto richiede correzione, separare confronto replica/corretto e non alterare as-is.

Prima dei run massivi misurare tempo, spazio e memoria di un pilota; limitare worker e thread, tenere almeno due core liberi rispetto alle risorse assegnate e margine RAM, monitorando gli altri lavori attivi. Conservare checkpoint riprendibili, registro completo, failure e output; evitare duplicazioni inutili. Un controllo critico indipendente, quando disponibile, deve verificare il protocollo prima del test decisivo e ricostruire dagli artefatti i risultati su cui si basa la conclusione. Nessuna approvazione automatica al trading.

La consegna finale deve comprendere:

1. decisione motivata tra as-is e varianti, con configurazione esatta oppure risultato negativo/insufficiente;
2. esito separato dei due filoni, formula e lookback selezionati senza usare OOS per ottimizzarli;
3. provenienza e stato della validazione dei pesi Multifactor, incluse eventuali lacune;
4. confronto economico e concentrazione reale, fasi di mercato e periodo recente, con incertezza e frequenza delle esclusioni;
5. impatto di dati, survivorship, warm-up, costi, esecuzione e capitale sulla trasferibilità;
6. manifest, protocollo, calendario, tentativi, comandi, output, test e revisione, sufficienti a riprodurre lo studio;
7. cosa monitorare in una eventuale osservazione prospettica, e quali evidenze mancano prima di un'eventuale decisione operativa.

Non fermarsi all'inventario o alla progettazione se lo studio viene successivamente avviato: procedere autonomamente alle verifiche autorizzate e consegnare la migliore decisione sostenuta dai dati.
