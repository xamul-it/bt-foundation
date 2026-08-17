# Brief di avvio — Ritaratura `post_up_cooldown` / `ah_lag1_threshold` sotto il nuovo regime filtri

Documento pensato per aprire una **nuova sessione** dedicata a questo
compito specifico. Leggere anche `docs/context/ah_context.md` (sezioni
"Prova su strada", "Isolamento dei tre filtri operativi uno alla volta")
per il contesto completo — qui solo il necessario per partire.

## Contesto

La strategia `OvernightAH` in produzione/paper (`config-common/scheduled/
strategies/overnight-ah-development.env`, cron `bt-scheduled development
entry`) aveva tre filtri giornalieri attivi prima di ogni entry
(`_filter_reason` in `bt-core/strategies/overnight_ah.py`):
`min_intraday_vol`/`max_intraday_vol` (banda sul range intraday di oggi),
`min_adv` (liquidità minima $), `ah_lag1_threshold` (soglia sul gap
overnight appena concluso).

Uno studio di isolamento (2026-08-16, `ah_context.md`) ha confermato via
Backtrader reale (2000-2026, hedge+cooldown+`max_concurrent=3` invariati)
che rimuovendo **volatilità intraday e ADV** insieme, lasciando
`ah_lag1_threshold` attivo, lo score attuale ottiene un netto
miglioramento rispetto allo scenario con tutti e tre i filtri attivi.
Nessun singolo filtro isolato spiegava da solo l'effetto — interazione
non additiva tra vol e ADV, non ancora pienamente compresa.

**Di conseguenza, il 2026-08-16 `min_intraday_vol`/`max_intraday_vol` e
`min_adv` sono stati disattivati nella config `development`** (commit
`0155d80`, motivazione completa nel commento sopra `STRATARGS` nel file
stesso). `ah_lag1_threshold=-0.1` e i parametri di `post_up_cooldown`
(`threshold=0.05`, `days=5`) sono rimasti **invariati** — perché non erano
l'oggetto di quello studio, non perché già ottimali nel nuovo regime.

## Perché serve una nuova ritaratura

`ah_lag1_threshold` e `post_up_cooldown_*` sono stati storicamente scelti
(valori legacy, non ritarati in questa sessione) **quando vol e ADV erano
ancora attivi** — cioè su un sottoinsieme di titoli/giorni diverso da
quello che la strategia vede ora. Dato che lo studio di isolamento ha
appena mostrato un'interazione non additiva e non banale tra i filtri
esistenti, non c'è motivo di assumere che i valori di `ah_lag1_threshold`
e `post_up_cooldown` restino ottimali (o anche solo ragionevoli) ora che
la popolazione di titoli candidati è cambiata sostanzialmente. Vanno
ritarati da zero sul nuovo regime, non ereditati.

## Parametri da tarare

- `ah_lag1_threshold` (attuale: `-0.1`) — soglia sul gap overnight appena
  concluso sotto la quale si salta l'entry. Esplorare sia l'intensità
  (es. `-0.05, -0.10, -0.15, -0.20`) sia, se ha senso, la disattivazione
  totale (`-999`) come punto di confronto.
- `post_up_cooldown_threshold` (attuale: `0.05`) e `post_up_cooldown_days`
  (attuale: `5`) — soglia di rendimento giornaliero aggregato sopra la
  quale si sospendono le nuove entry per N sessioni. Esplorare una griglia
  2D ragionevole attorno ai valori attuali (es. threshold
  `{0.03, 0.05, 0.08}` × days `{3, 5, 8}`), più `threshold=0.0` (= off)
  come controllo.

`min_intraday_vol`/`max_intraday_vol`/`min_adv` restano **disattivati**
(non riaprire questo fronte senza motivo — è già stato deciso e
committato).

## Infrastruttura riusabile (tutta già scritta e validata in questa sessione)

- Strategia di produzione: `bt-core/strategies/overnight_ah.py`
  (**non modificarla** — i parametri si passano via `--stratargs`).
- Pattern di confronto reale via `btmain.py` (vedi commit di questa
  sessione, es. `nofilters_full_control`/`nofilters_full_composite`):
  eseguire il controllo, copiare il suo `returns.csv` in
  `config-common/benchmark/<nome>.csv`, eseguire la variante da testare
  con `--benchmark <nome>` per generare `stats.html`.
- **Verificare SEMPRE i numeri direttamente dai `returns.csv`** (stesso
  indice comune, `(1+r).cumprod()`) invece di fidarsi della tabella
  affiancata di QuantStats — in questa sessione quella tabella ha mostrato
  un troncamento fuorviante più volte (vedi `ah_context.md`).
- Oracolo/regret/`mcs_selection.py` (Studi 1-6, `bt-strategy-test/
  overnight-ah/research/`) disponibili se si vuole un layer statistico più
  rigoroso invece di un confronto diretto — probabilmente eccessivo per
  soli 2-3 parametri operativi, ma l'infrastruttura c'è.

## Metodologia proposta (di massima — da confermare con l'utente all'avvio)

1. Griglia piccola e mirata (i valori sopra), non uno sweep enorme —
   pochi parametri, non serve LHS/qmc.
2. Un **controllo fisso** per tutta la sessione (i valori attuali di
   `ah_lag1_threshold`/`post_up_cooldown`, con vol/ADV già disattivati) —
   **non ricalcolare il controllo a ogni variante testata**: è stata la
   fonte di confusione più grande nella sessione precedente. Un solo
   riferimento, tutte le varianti confrontate contro quello.
3. Real Backtrader (non solo pandas) fin dall'inizio, dato che si tuning
   parametri della strategia di produzione direttamente, non uno score
   sperimentale isolato.
4. Periodo: **da concordare esplicitamente con l'utente prima di
   partire** — se includere il periodo storico intero o restare su una
   finestra più recente/realistica; nella sessione precedente l'utente ha
   scelto consapevolmente di includere l'intero periodo (2000-oggi) per la
   prova su strada, ma va ridiscusso, non assunto.
5. Unità di misura coerenti in ogni tabella (percentuale, come nei report
   — non convertire in "moltiplicatore x" senza dirlo esplicitamente,
   altra fonte di confusione nella sessione precedente).

## Lezioni operative da questa sessione (da rispettare)

- **Verificare prima di assumere un bug**: un risultato enorme o strano
  può essere compounding/leva legittimi, non un errore — controllare
  errori/warning nei log, non concludere "bug" solo perché il numero è
  grande.
- **Non pubblicare mai file/report come Artifact senza richiesta esplicita
  dell'utente** — dare il path locale, salvo diversa richiesta.
- **Non modificare file operativi/di produzione (incluse config
  scheduled) senza conferma esplicita su cosa cambiare esattamente.**
- **Non fare il commit automaticamente** — solo quando l'utente lo chiede
  esplicitamente; se il file ha altre modifiche non correlate già presenti
  (non tue), segnalarlo prima di includerle nel commit.
- Documentare i risultati (anche negativi) in `docs/context/ah_context.md`
  con lo stile già stabilito (tabelle, numeri precisi, sezione "Non-goal /
  stato") quando richiesto.

## Domande da fare all'utente prima di iniziare il tuning vero e proprio

1. Periodo di test (intero storico vs finestra recente)?
2. Metodologia: confronto diretto (come la prova su strada) o layer
   statistico oracolo/regret/MCS (come Studi 1-6)?
3. Griglia di valori da testare per `ah_lag1_threshold` e
   `post_up_cooldown_*` — quelli suggeriti sopra sono solo un punto di
   partenza ragionevole, non decisi.
4. Se un risultato migliora il controllo fisso, si aggiorna subito la
   config `development`, o si documenta e basta in attesa di conferma?
