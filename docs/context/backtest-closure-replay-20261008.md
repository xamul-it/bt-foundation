# Replay Backtest: perimetro concordato

## Principio operativo da preservare

Alpaca e Backtest sono due binari indipendenti: ciascuno prende le proprie
decisioni senza usare le decisioni o gli esiti dell'altro. Backtest è una
controverifica, non un controllore del trading Alpaca. La dashboard osserva
entrambi e rende visibili all'operatore disallineamenti e cause; i confronti
sono osservabilità, mai input o condizioni di esecuzione dei due binari.
Non aggiungere percorsi speciali senza necessità: più codice significa più
possibilità di bug. Il recupero chiusure non riscrive decisioni originali.

- Strategia, parametri e commit originali: punto confermato e chiuso.
- Il replay usa dati locali correnti e non scarica dati, non sceglie snapshot
  alternativi e non legge Alpaca. Produce trade senza decision_date_override.
- Aggiorna solo posizioni Backtest pending, senza exit_date, per simbolo e
  giornata d'ingresso. Conserva quantità, prezzo e decisione originali.
- Una corrispondenza diversa di quantità/prezzo è registrata in detail con
  entry_matches_replay=false, replay_qty e replay_entry_price; il PnL della
  posizione originale usa la sua quantità e il suo prezzo, non quelli del replay.
- Una chiusura non ritrovata resta pending con
  detail.pending_reason=replay_closed_trade_missing, run_id, outpath e core_commit.
  Da sola non distingue una decisione non riprodotta da una barra d'uscita ancora
  assente. La GUI mostra questa evidenza nella spiegazione della chiusura simulata.
- Se il log del replay riporta una causale per quella giornata (es. cooldown),
  salvarla in detail.replay_no_trade_reasons, marcare replay_decision_divergence
  e mostrarla direttamente sulla riga CHIUSURA della posizione originale.
  Questa è una spiegazione del disallineamento del replay, non una nuova causale
  dell'ingresso originale. Il codice storico è il commit del run originale.
- Il job dashboard usa --catch-up --settle-only. Il confronto storico precedente
  resta disponibile esplicitamente, ma non è il lavoro del recupero chiusure.

## Punto aperto: prestazioni del run giornaliero

Verificare il costo del Backtest continuo rispetto al percorso giornaliero con
decision_date_override e all'uso di trade_start_date. Nel codice ispezionato
trade_start_date appartiene a OvernightAH, non al parametro base MultiTickerStrategy.
Nessun beneficio prestazionale dell'override è stato misurato: non documentarlo
come ottimizzazione finché non esiste un test. Valutare prima il riuso del percorso
esistente; evitare parametri e stati speciali non necessari. Il refactoring
cooldown verso un indicatore resta separato.

## Schedulazioni dati Yahoo verificate

Dashboard: refresh 15:37 Europe/Rome, recupero chiusure 15:40; poll Alpaca 15:35
separato dal replay. Cron: refresh dati 23:30 lun-ven preservato per le altre
strategie. Il refresh usa --incremental e DATA_THROUGH predefinito a oggi.
Sono orari fissi italiani, non automaticamente legati all'OPG: verificare le
settimane di disallineamento dell'ora legale USA/Europa. Nessun orario cambiato.

Aggiornamento: il timer replay è stato spostato alle 15:50 su richiesta.
Concatenazione generica configurabile dalla UI: after_job nel medesimo store
scheduler-state.json. Un job concatenato non ha timer autonomo; parte dopo il
successo del predecessore. Errori, disabilitazione e pausa scheduler non avviano
i successori. Vietati cicli e strategie di trading. La rimozione del predecessore
ripristina l'orario salvato. La persistenza DB rimane un'attività separata.
Attivata la catena Yahoo -> recupero chiusure e verificata dopo riavvio API:
Yahoo ha timer 15:37, replay ha after_job=Yahoo e nessun prossimo timer autonomo.
L'orario 15:50 resta salvato solo per il ritorno alla modalità autonoma.

Verifica development sui dati presenti l'8 ottobre prima dell'apertura USA:
replay del 2 ottobre: cooldown, remaining_after_today=2; del 6 ottobre:
cooldown, remaining_after_today=0; del 7 ottobre: nessuna causale no-trade.
Le barre Yahoo NVDA/AVGO/MU terminano al 7 ottobre: non è ancora disponibile
l'apertura dell'8 per chiudere un ingresso del 7. Non classificare questo caso
come cooldown senza evidenza. Causali del 2 e 6 riportate sui tre simboli nella
riga CHIUSURA; ricevute originali lasciate invariate.

## Audit globale delle classificazioni replay (8 ottobre)

Verificati tutti i cinque risultati ancora con status error, con backup in
backup/reconciliation-repair/all-replay-errors-before-20261008.json.
Development 2/6 ottobre e mirror 6 ottobre: esecuzione completata, chiusure
non recuperate, causale cooldown documentata; corretta la classificazione e
copiata la causale sulle righe di chiusura Backtest, senza inventare exit.
Challenger 23 settembre e mirror 17 settembre: nuove esecuzioni diagnostiche
completate, rispettivamente 3 e 5 ingressi. L'associazione storica invalidata
resta da verificare: non ripristinati confronti non validati e non marcati puliti.
Nessun errore di esecuzione effettivo trovato fra questi cinque; la UI mantiene
gli errori reali con dettaglio e distingue l'associazione storica non validata.
Ricevute originali e dati Alpaca invariati. Test: 4 UI e 13 backend superati.

## Bonifica originale / esecuzione / verifica (8 ottobre)

Vincoli confermati: gli ingressi originali sono immutabili; market_closed del
periodo del calendario errato è un fatto storico, non una causale da correggere.
Il replay completa SOLO le uscite e registra verifiche, mai nuovi ingressi.
La UI non mostra prezzi di decisione né usa reference_price come esecuzione.

- Persistenza delle verifiche nella nuova tabella separata
  scheduled_backtest_replay_verifications; causali originali restano in
  scheduled_entry_decisions. Separati anche i blocchi UI e le descrizioni.
- Backup prima della bonifica: backup/reconciliation-repair/
  original-replay-prices-before-20261008.json. Include le 42 posizioni Backtest
  modificate e fingerprint delle tabelle lasciate immutate.
- Migrati 152 risultati storici in evidenze separate, senza modificarli.
  Identificate 12 chiusure replay senza posizione originale. 126 evidenze sono
  esplicitamente non validate per mancanza di ricevuta originale o associazione
  storica già invalidata: non significa che 126 esecuzioni siano fallite.
- Ripristinate 18 quantità delle posizioni dai rispettivi ordini originali,
  sovrascritte dal vecchio replay. Rimossi reference_price e prezzi d'ingresso
  del replay dai campi operativi delle 42 posizioni Backtest. Non è disponibile
  un prezzo di esecuzione originale verificabile per queste 42 posizioni:
  prezzo e P&L restano NULL, senza sostituti. Prezzi d'uscita verificati conservati.
- Fingerprint invariati NELLA TRANSAZIONE DI BONIFICA per ricevute originali,
  confronti originali, risultati storici, cache Alpaca e posizioni Alpaca.
  Dopo il riavvio API il poller esistente ha aggiornato normalmente la propria
  cache, i settlement e l'enrichment delle ricevute Alpaca: i fingerprint
  globali di quelle tabelle non sono quindi stabili a servizio attivo.
  Nessuna modifica ai dati Alpaca da parte della bonifica né scrittura broker.
  Il preesistente enrichment Alpaca modifica le ricevute con fatti di esecuzione;
  resta distinto dal replay Backtest, che non richiama quel percorso.
- Il replay futuro preserva qty e entry_price, gestisce entry_price NULL,
  conserva gli errori reali e registra verifiche anche per giornate originali
  senza ordini. Una mancata ricevuta non prova l'assenza di una posizione.
- Ingestione dei trades del run originale può fornire prezzi eseguiti; accetta
  soltanto posizioni originali esistenti con quantità corrispondente.
- Runtime e schema aggiornati in dev e prod, senza toccare strategie o
  modifiche target/rebalance estranee a questo intervento.
- API verificata su tutti i profili. Challenger 24/09: causali originali
  market_closed invariate, tre verifiche original_position_missing separate.
  Replay reale development 06/10 completato: cooldown, tre chiusure non
  recuperate; nessuna modifica a ingressi o Alpaca.
- Test: 34 backend e 7 UI superati; PWA ricostruita.

## Caso challenger 30/09: prezzi unici e completamento CLS originale

Il caso concreto ha chiarito il dato mancante: reference_price è il prezzo
di decisione su t-1, NON il prezzo eseguito. Per gli ordini originali BUY CLS
delle due strategie OvernightAH/OvernightAHFlatComposite, il Backtest esegue
alla Close di t: non serve rigenerare la decisione per ricavare quel prezzo.
La precedente bonifica aveva correttamente tolto reference_price, ma aveva
lasciato incompleto il passaggio di esecuzione originale, dichiarando mancanti
prezzi che potevano essere determinati dalla regola CLS e dalle barre presenti.

- recover_original_cls_entry_prices completa l'esecuzione degli ordini originali
  usando la Close del provider registrato per quel giorno, non gli ingressi del
  replay. Solo date passate, strategie CLS esplicitamente verificate, ordine
  originale presente; nessuna creazione di posizioni né modifica di quantità.
- Recuperati 42 prezzi e relativi P&L quando l'uscita è già disponibile.
  Backup: backup/reconciliation-repair/backtest-cls-entry-prices-before-20261008.json.
  Fingerprint delle ricevute Backtest originali invariato.
- Challenger 30/09 AVGO: decisione 355.100006 (29/09), esecuzione CLS
  351.190002 (30/09), uscita 352.149994 (01/10), rendimento +0.273354%.
  NVDA: 228.380005 -> 229.979996, +0.700583%.
- Rimossi i vecchi box di prezzo dal template: il pannello originale usa solo
  scheduled_entry_settlements; le verifiche replay non espongono una seconda
  coppia di prezzi concorrenti. P&L NULL non è zero, percentuale NULL non
  produce parentesi vuote. Quantità replay differenti non sono allarmi UI.
- La vecchia PWA poteva continuare ad eseguire vecchio JS, mostrando i due
  vecchi pannelli. Configurati skipWaiting/clientsClaim e aggiornamento della
  pagina al cambio worker. Per una finestra già aperta col vecchio codice
  occorre un primo aggiornamento/riapertura. HTTP verificato: indice e bundle
  serviti corrispondono al build corrente, worker restituisce 200.
- Test: 36 backend e 8 UI, incluso il caso concreto AVGO 30/09.
- Nessuna bonifica Alpaca in questo intervento.

## Punti di accoppiamento verificati

- Handler entry: esegue Alpaca e Backtest nello stesso script, in sequenza, ma
  tenta entrambi anche se uno fallisce; il Backtest non usa ordini/equity Alpaca.
  Rimane una dipendenza temporale dal termine del primo comando (limite da
  valutare separatamente, senza modificare i cron di trading in questo intervento).
- Recorder: registra la propria sorgente e invoca un confronto best-effort;
  il confronto non guida la strategia e il suo fallimento non invalida la ricevuta.
- Poller Alpaca: aggiorna soltanto evidenze Alpaca e chiusure da cache broker.
- Replay chiusure: usa capitale originale Backtest, non portfolio history Alpaca;
  non modifica ricevute, confronti, schedulazioni di trading o posizioni broker.
- Il vecchio comando di confronto storico resta manualmente disponibile e
  contiene ancora letture Alpaca; non è il percorso del nuovo job di chiusura.
