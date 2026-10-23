# Checkpoint scientifico del Research Director

2026-10-22, dopo apertura G2 e prima di leggere esiti comparativi. Director persistente: ruolo richiesto `gpt-6-astra high`. Il coordinatore ha comunicato la sostituzione degli agenti implementatore/reviewer per usage limit; reviewer e analista sostitutivi `gpt-6-sol high`, separati fra loro e dal Director. Nessun cambiamento metodologico deriva dalla sostituzione.

## Stato verificato

- G0 completato: [mappa](parameter_map.md), [studi](study_register.md), [interazioni](interactions.md), [catalogo](run_catalog.md), [domande](research_questions.md).
- G1 condizionato: provider e share units non certificati; quarantena ex post e membership corrente, nessun holdout intatto. Cutoff 2026-10-21.
- G2 aperto dal [gate indipendente](../../../bt-strategy-test/strategy-selection/study_20261022/reviews/protocol_gate.json) per i 18 ID core espliciti. Il gate certifica cinque hash e non autorizza ancora ID condizionali. Protocollo v0.4 e schema conservano la formulazione precedente PENDING: **non modificarli per aggiornare lo stato**, perché gli hash governano i lanci. Il gate successivo è la fonte dello stato corrente.
- Pilota A00 approvato tecnicamente: la review verifica NAV, fee e interessi; sei intenti terminali senza esecuzione sono dichiarati. Questo controllo non valida automaticamente Composite/RMA o l'economia delle policy.
- Il coordinatore riferisce avvio di otto core mediante launcher approvato, due worker, dopo verifica input. Il Director non ha eseguito processi né letto risultati comparativi. L'elenco processi e avanzamento sono nel checkpoint operativo del coordinatore, non ricostruiti qui.

## Limiti della copertura da mantenere nel report

**Fixed-notional:** gli scenari core 20k/200k/2M sono conti con compounding e allocazioni native. Non sono nozionale fisso, anche quando il capitale iniziale coincide. Le evidenze storiche S09/S10 aiutano a spiegare perché cambino breadth, capitale inutilizzato e rischio, ma non identificano la performance fixed-notional delle configurazioni congelate oggi. Riuso ammesso soltanto dopo equivalenza esplicita di codice, universo, lag, costi e sizing. Se manca, dichiarare la lacuna; vietato trasformare una curva compound in una simulazione fixed-notional dividendone il profitto per un denominatore costante.

**Benchmark:** la primaria preregistrata confronta NAV netti tra configurazioni. I benchmark di sessione servono a interpretare l'esposizione, senza modificare i gate: AH/Composite richiedono C_prev→O_t con data di uscita t e feed YahooAdj; RMA richiede multi-sessione, ad esempio QQQ C2C con feed rawYahoo coerente. Benchmark di puro prezzo non incorpora automaticamente dividendi, fee, cash, cap o investibilità. L'alpha rispetto a gambe diverse non è una misura omogenea fra famiglie. Il QQQ CSV frozen e l'assenza di missing sono condizioni tecniche, non certificazione total-return.

**Esperienza 2000–2004:** i run nuovi fanno warmup in quel periodo e negoziano dal 2005. Non mostrano il rischio del capitale durante l'intera crisi dot-com. Gli studi precedenti possono descriverla con diverse configurazioni; non chiamare il risultato nuovo un backtest negoziato 2000–2026.

**Confronti insufficienti per un PASS forte:** core A10/A01/A11/R5 senza propria variante stressata, policy sintetiche senza stato nativo e mix su sleeve da 200k anziché capitale totale ripartito. Non riempire le celle mancanti con equivalenze presunte. La riserva di due posti riguarda una sola policy scelta dal criterio preregistrato, eseguita a 5 e10 bps RT dopo review distinta; nessun risultato del 2023–2026 decide l'accesso.

**Benchmark prospettico e autorizzazione:** la conclusione potrà proporre osservazione prospettica e decisioni di rischio condizionate, ma non certifica il live e non autorizza ordini, deploy o cambi operativi. Il successo della ricerca può consistere nel dimostrare che nessun candidato supera i gate o che una decisione forte è preclusa da lacune specifiche.

## Controlli che precedono la firma scientifica finale

1. Ledger e returns riconciliati per ogni famiglia, inclusi interesse, ordine terminale, pre-start/post-cutoff e partial/margin/reject; review indipendente separata.
2. Applicazione delle 15 comparazioni e del maxT come congelati, numero effettivo di test identificabili, periodi e campioni comuni; tutti i tentativi compresi i fallimenti restano nel registro.
3. Procedure adattive dal 2010, training solo passato, tie/fallback prefissati e nessun riavvio di portafoglio presentato come continuo.
4. Risultati per capitale, rischio e costo che descrivano compromessi senza attribuire all'utente una tolleranza alle perdite.
5. Monitoraggio che separi errore tecnico, execution gap e economia, con falsi allarmi, ritardi, episodi non rilevati e rientro osservabile; insufficienza dei dati reali dichiarata.
6. Risposta a ogni questione del registro, motivi di esclusione o incertezza, configurazioni esatte della proposta e condizioni che la cambierebbero.

Nessuna firma economica o scelta strategica è formulata in questo checkpoint.
