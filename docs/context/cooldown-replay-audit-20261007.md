# Cooldown e replay: verifica del 7 ottobre 2026

## Correzione

`OvernightAH.next()` saltava le barre storiche prima di aggiornare cooldown e
CUSUM quando era impostata `decision_date_override` (anche nel percorso live).
Lo stato dei controlli basati sui prezzi viene ora ricostruito durante il warmup,
prima del filtro che limita gli ordini alla sola data richiesta. Il riferimento
temporale dei controlli coincide con quello della selezione dei candidati.
Una stessa barra non viene contata due volte e le barre successive al cutoff
della decisione non aggiornano lo stato. Correzione applicata al core dev e prod.

Regressioni: 9 test nuovi; con i test di reentry, 19 test passano sul core prod.
Copertura: riavvio giornaliero contro esecuzione continua, weekend, durata del
cooldown, override con barre future, warmup CUSUM, trade_start_date e paper.

## Run senza override

`cooldown_alignment_live_20261006_07_233644`, modalità backtest, universo e
parametri dell'artefatto live del 6 ottobre, dal 2025-08-12 al 2026-10-07.
Nessuna chiamata di invio ordini al broker. Commissioni disattivate per questa
verifica della decisione, non per una stima dello scostamento di PnL.

Il cooldown blocca gli ingressi del 5, 6 e 7 ottobre (rimanenti: 2, 1, 0).
Le operazioni Alpaca già eseguite il 6 e 7 sono quindi scostamenti rispetto al
baseline corretto: il replay non può renderle identiche senza falsificare la
storia. Questa esecuzione non sostituisce gli artefatti originali né aggiorna
automaticamente le riconciliazioni storiche della dashboard.

Verifica sul core prod con dati Yahoo adjusted espliciti:
`cooldown_override_full_verified_20261006` e
`cooldown_override_full_verified_20261007`. Entrambi terminati correttamente,
con `today.json` = `no_orders`, causa cooldown, coerenti con il run continuo.
I tentativi precedenti senza percorso dati risolto non hanno caricato feed e
non costituiscono verifiche valide (un exit code zero da solo non basta).

## Altri stati da controllare

- CUSUM: stessa perdita di warmup corretta. Il suo stato cumulativo può però
  dipendere dall'inizio della finestra storica: finestre mobili diverse non
  garantiscono identità assoluta senza checkpoint. Overlay attualmente spento.
- `strategy_throttle`: storico PnL/equity dei trade chiusi locale all'istanza;
  le chiusure live esterne alla strategia non lo ricostruiscono. Overlay spento.
- `hold_confirmed_positions`: stato LONG/prezzo/data d'ingresso locale, non
  ricostruito automaticamente dalle sole posizioni broker. Opzione disattivata.
- `OvernightAHTargetRebalance` ridefinisce `next()` e non eredita il nuovo
  warmup. Non è la strategia dei profili schedulati attuali; non modificata.
- Protezioni contro doppio ingresso nello stesso giorno: alcuni marcatori sono
  locali alla singola istanza. Serve una verifica end-to-end dei controlli sugli
  ordini broker prima di affermare che un riavvio possa duplicare gli ordini.

Non sono state attivate opzioni, modificate strategie alternative o riscritte
esecuzioni Alpaca per risolvere queste condizioni non attive.

## Riproducibilità

Il replay storico usa il commit core registrato nell'artefatto originale: per
definizione continua a riprodurre il codice originale, non questa correzione.
La verifica corretta deve rimanere distinta dalla ricostruzione storica.

## Rilascio e bonifica dell'8 ottobre

Fix core committato in `e8f1019`; con le regressioni della riconciliazione,
il riferimento prod è `0538e6d`. Promossi i commit pertinenti sul branch prod
del repository principale, senza incorporare le modifiche research estranee.
I due file core precedentemente non committati sono inclusi nella release;
le copie precedenti restano in stash. La cache calendario generata è preservata
fuori dai commit di codice. Nessun push remoto effettuato.

Passano 30 test su cooldown, reentry ed evidenze no-trade; passano anche i
test shell dello scheduler. Corretta una collisione nella lettura dei log:
`signal_date=...` non deve essere interpretata come `date=...` operativa.

Backup delle righe DB prima della bonifica:
`backup/reconciliation-repair/live-baseline-before-20261008.json`.
Nuove decisioni prod verificate con commissione Alpaca:
`live_corrected_baseline_validated_20261006` e
`live_corrected_baseline_validated_20261007`, entrambe no-orders per cooldown.
Replay continui senza override usano gli snapshot delle decisioni originali
e il core corretto, con `baseline_kind=corrected_counterfactual` esplicito.

Per il solo profilo live, sostituite le decisioni Backtest del 6 e 7 con quelle
corrette e rimosse sei righe Backtest pending obsolete (ID 444-446, 468-470).
Sono recuperabili dal backup. Non sono state modificate righe Alpaca.
I confronti delle decisioni sono `diverged`: zero ordini Backtest contro tre
acquisti Alpaca per giornata, AVGO/MU/NVDA. Verificato HTTP 200 sull'endpoint
effettivamente usato dalla GUI, con gli stessi esiti e causali.

Il 6 è riconciliato anche sulle chiusure Alpaca del 7. La chiusura dei trade
Alpaca aperti il 7 è ancora attesa all'apertura dell'8: la coda rimane pending,
con il core del baseline correttivo registrato affinché il catchup successivo
non torni al vecchio codice. Il Backtest corretto non ha posizioni da chiudere.
