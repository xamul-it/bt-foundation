# Backfill dei replay con parametri attuali

`bin/watchtower_backfill_current_params.py` supporta development,
challenger e mirror paper (default: development e challenger). Usa la stessa selezione della dashboard: calendario di
mercato condiviso, sessioni fino ad as-of, esclusione della sessione più recente,
ultime 25 sessioni concluse. Non seleziona le ultime 25 righe del database.

Piano senza scritture:

```bash
bt-core/.venv/bin/python bin/watchtower_backfill_current_params.py --as-of 2026-10-13
```

Esecuzione:

```bash
bt-core/.venv/bin/python bin/watchtower_backfill_current_params.py --as-of 2026-10-13 --execute
```

Opzioni: `--profiles development challenger mirror`, `--window 25`, `--db-dsn`.
Per rigenerare solo mirror usare `--profiles mirror --execute`.
Ogni trade è associato solo alla sua data effettiva di ingresso (`open_datetime`);
il run include barre successive per permettere l'uscita. Le date senza trade
restano senza trade. Il programma usa i parametri e il codice attuali, i dati
condivisi attuali e l'equity storica Alpaca: è una simulazione retrospettiva,
non una riproduzione certificata della decisione storica.

Per ogni giornata aggiorna in una sola transazione gli
execution_reconciliation_results consumati dagli score, le verifiche
scheduled_backtest_replay_verifications e le chiusure delle posizioni
scheduled_entry_settlements con source=backtest, e la decisione schedulata
source=backtest. La simulazione viene sostituita integralmente: ordini,
quantità, prezzi, chiusure e P&L provengono dal replay rigenerato. Le righe
backtest precedenti vengono archiviate e rimosse per il solo profilo/data.
In una giornata cooldown la decisione diventa no_orders, la lista ordini è
vuota e non rimangono posizioni simulate pendenti del vecchio run.
Il cooldown viene riconosciuto solo da un log che dichiara sia la data della
decisione sia signal_date; una sola data di segnale non identifica l'ingresso.

Non esegue ordini paper/live, non riscrive ricevute o posizioni Alpaca, non consulta
ordini Alpaca e conserva i campi Alpaca degli esiti già presenti. La lettura
dell'equity storica è l'unica chiamata al conto e rimane di sola lettura.
Prima salva esiti, verifiche e posizioni backtest precedenti, il piano, i log e il risultato di ogni giorno
in bin/watchtower_scratch/backfill_*. Le righe vengono aggiornate per profilo e
data: rieseguire non crea duplicati. Un errore lascia intatto l'esito precedente
di quella giornata e viene registrato in outcomes.json.
