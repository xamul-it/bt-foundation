# Brief di avvio — Rendere `OvernightAHFlatComposite` utilizzabile in paper/live

Documento pensato per aprire una **nuova sessione** dedicata a questo
compito specifico. Leggere anche `docs/context/ah_context.md` (sezione
Studio 6 e seguenti) per il contesto completo sul composito — qui solo il
necessario per partire su questo problema preciso: **il composito oggi
funziona solo in backtest, va reso deployabile**.

## Il problema esatto

`bt-core/strategies/overnight_ah_flat_composite.py`
(`OvernightAHFlatComposite`, sottoclasse di `OvernightAH`, produzione mai
toccata) ha `live_enabled = False` per un motivo preciso: la modalità
`monthly_universe_score_mode='flat_panel'` legge i 21 indicatori da un CSV
statico (`monthly_universe_indicator_panel`, es.
`bt-strategy-test/overnight-ah/research/out/flat_panel.csv`), esportato
**una tantum** da `export_flat_panel_csv.py`. In paper/live servirebbe un
pannello che si aggiorna con i mesi, non un export congelato al giorno
della ricerca.

## Come funziona oggi la catena (verificato, non assunto)

```
config-common/data/d/yahoo_adj/*.csv   (dati grezzi, GIA' aggiornati ogni
        │                                giorno di trading da
        │                                backtrader-entry.sh via
        │                                load_tickers.py --incremental)
        ▼
daily_panel_full_history.py  →  /mnt/Backup/overnight_ah_tuning/daily_panel_full_history/daily_panel.parquet
        ▼
indicator_panel.py           →  .../indicator_panel/per_ticker_indicators.parquet
        ▼
score_panel.py                →  .../score_panel/per_ticker_features.parquet
        ▼
export_flat_panel_csv.py     →  flat_panel.csv  (year;month;ticker;<21 colonne>)
```

**Punto chiave**: i dati grezzi (`config-common/data/d/yahoo_adj/`) sono
**già** tenuti aggiornati ogni giorno feriale dal job di produzione
esistente (`scripts/scheduled/handlers/backtrader-entry.sh`, righe ~13-20,
chiama `load_tickers.py --incremental` prima di ogni `btmain.py`) — quindi
la fonte dati non è il problema. Il problema è che **nessuno step
automatico** rigenera la catena `daily_panel_full_history.py → ... →
export_flat_panel_csv.py`: oggi viene eseguita solo a mano, durante la
ricerca. Serve aggiungere un trigger schedulato per questa catena, non
costruire una nuova pipeline dati.

**Buona notizia sui tempi**: il lookback massimo tra i 18 indicatori dello
Studio 2 è **252 giorni di trading (~1 anno)** (`SMA_RATIO_LOOKBACKS`/
`REG_SLOPE_LOOKBACKS` in `indicator_panel.py`, righe 35-42). E dato che le
feature sono ex-ante e calcolate **una sola volta all'inizio di ogni mese**
(stesso pattern di caching già usato dallo score legacy,
`_dynamic_monthly_universe` in `overnight_ah.py`), il refresh del
pannello serve **una volta al mese**, non ogni giorno — non serve
ricalcolare 26 anni di storia a ogni run, e non serve nemmeno farlo
quotidianamente.

## Due strade possibili (con raccomandazione)

### A. Refresh schedulato del pannello (RACCOMANDATO — riusa tutto il codice già validato)

Aggiungere un nuovo job schedulato (nuovo handler, es.
`scripts/scheduled/handlers/refresh-indicator-panel.sh`, o uno script
Python dedicato) che gira **una volta al mese**, prima del primo giorno di
trading del mese e prima del job di entry serale, eseguendo la catena
sopra con `oggi` come data di fine invece della data fissa usata nello
studio di ricerca. Nessun nuovo codice di calcolo indicatori — solo un
trigger schedulato per script già scritti e già validati (fidelity-check
fatto nello Studio 2).

Da verificare/adattare (compito della nuova sessione, non assunto qui):
- `daily_panel_full_history.py`/`indicator_panel.py`/`score_panel.py`/
  `export_flat_panel_csv.py` oggi vengono lanciati a mano con un range di
  date implicito (fino a "adesso" al momento dell'esecuzione) — confermare
  che non abbiano una data di fine hardcoded residua da uno studio
  precedente prima di schedularli.
- Dove scrivere l'output "live": **non** in
  `bt-strategy-test/overnight-ah/research/out/` (gitignored, pensato come
  scratch di ricerca — vedi `.gitignore` righe 16-17) — usare una
  posizione versionata sotto `config-common/` (stesso principio già usato
  da `monthly_universe_file`, convenzionalmente in
  `config-common/tickers/`), es. `config-common/indicator_panel/
  flat_panel_live.csv`.
- Rigenerare tutta la storia ogni mese (pandas su ~100 ticker, non 26 anni
  di calcolo pesante) è probabilmente abbastanza veloce da non avere
  bisogno di una modalità incrementale — solo se i tempi risultano un
  problema reale vale la pena costruire un refresh incrementale (append
  del solo mese nuovo).

### B. Reimplementare i 18 indicatori dal vivo in Backtrader (bt.Indicator)

Scartata esplicitamente nel piano dello Studio 6 (lavoro enorme, alto
rischio di fedeltà, duplicherebbe `indicator_panel.py` già validato) per
una singola conferma via backtest. Per un **deployment reale** potrebbe
valere la pena riconsiderarla in futuro (eliminerebbe la dipendenza da un
pannello esterno, più coerente con come lo score legacy calcola già c2c/
ah/corr12/beta12 dal vivo) — ma è un progetto a sé, di scala paragonabile
a un intero studio precedente. Non è il punto di partenza consigliato:
partire da A, tenere B come nota per il futuro.

## Passi concreti suggeriti (ordine indicativo, da confermare con l'utente)

1. Verificare/adattare i 4 script della catena per un run "a oggi" invece
   che con date fisse da ricerca.
2. Scegliere e creare la posizione versionata per il pannello live
   (`config-common/...`).
3. Scrivere il nuovo handler schedulato (mensile, prima del primo entry
   del mese) — riusare i pattern esistenti in
   `scripts/scheduled/handlers/`.
4. **Fidelity check obbligatorio** prima di fidarsi: il pannello
   rigenerato "a oggi" deve produrre valori storici identici (sulle date
   già coperte dallo studio di ricerca) al pannello usato negli Studi 5/6
   — stessa disciplina già seguita in tutta la sessione precedente.
5. Solo dopo il check: `live_enabled = True` su
   `OvernightAHFlatComposite`, e un run di prova in **paper** (non live)
   per almeno un ciclo mensile completo prima di qualunque discorso su
   capitale reale.
6. Decidere (con l'utente, non assumere): il composito diventa un profilo
   schedulato **separato** da `development` (stesso account paper, id
   diverso) o sostituisce lo score di `development`? Sono scelte
   operative diverse con implicazioni diverse, non decidere da soli.

## Cosa NON fare

- Non toccare `overnight_ah.py` (produzione) — tutto il lavoro resta sulla
  sottoclasse sperimentale.
- Non scrivere il pannello live nella cartella `research/out/` (gitignored
  — sparirebbe/non sarebbe versionato).
- Non impostare `live_enabled = True` prima del fidelity check del punto 4.
- Non confondere questo lavoro con `bin/update_data_monthly.sh` — quello
  aggiorna dati **minute Alpaca**, pipeline completamente separata e non
  rilevante per questo pannello (che usa dati **daily yahoo_adj**, già
  tenuti aggiornati da `backtrader-entry.sh`).

## Domande da fare all'utente prima di iniziare

1. Rifinire i parametri operativi (cooldown/ah_lag1, vedi
   `ah_cooldown_ahlag1_tuning_brief.md`) prima o dopo aver reso il
   composito deployabile? Sono due filoni indipendenti, ordine da
   decidere.
2. Profilo paper separato per il composito, o sostituzione diretta di
   `development`?
3. Cadenza di refresh: una volta al mese basta (come lo score legacy), o
   si preferisce comunque un margine di sicurezza (es. refresh settimanale)
   nonostante il calcolo sia ex-ante mensile?
4. Chi/cosa monitora che il job di refresh mensile sia effettivamente
   girato prima del primo entry del mese (alerting/fallback se salta)?
