# Brief di avvio: dimensionamento realistico degli ordini (liquidità, slot concorrenti)

## Contesto

Nella sessione precedente (vedi `docs/context/ah_context.md`, sezioni
"Ritaratura ripetuta senza compounding", "Fix: `auction=False` in
backtest produce PnL=0 per costruzione", "Due metodologie di Sharpe
mescolate senza dirlo", "Isolamento hedge EMA vs cooldown" e
"Confronto con tutto spento") è emerso un problema strutturale nel
dimensionamento degli ordini di `OvernightAH`, testato in due modi
opposti sullo stesso backtest 2000-2026:

- **`sizing_policy='fixed_notional'`** (importo $ fisso per trade, non
  compounding): dà numeri piccoli e interpretabili (dal +2,94% al
  +12,49pp a seconda dello studio), ma **non riflette come la
  strategia funzionerà davvero** — deve capitalizzare (compound) e
  usare leva 2x come le altre strategie in produzione.
- **`sizing_policy='legacy'`** (equity-proportional, compounding, leva
  2x reale, come `development.env`): su 26 anni continui produce
  moltiplicatori assurdi — **95-159 milioni di volte** il capitale
  iniziale. Anche senza leva (`max_exposure=1`) il solo compounding
  porta a **19.000-30.000x**. Nessun mercato reale potrebbe assorbire
  ordini di quella dimensione senza spostare il prezzo — il risultato
  non è economicamente eseguibile, anche se numericamente "corretto"
  dato il codice attuale.

L'utente vuole un nuovo filone di studio con l'obiettivo esplicito di
**restare in regime compound + leva 2x** (il regime operativo reale
della strategia), ma **dimensionare ogni ordine in modo che non alteri
il mercato** — tenendo conto del regime di mercato, della struttura/
rischio del titolo, e valutando se il numero di slot concorrenti
(`max_concurrent`, oggi 3) può salire a 4-5-10 quando la liquidità lo
permette senza perturbare il prezzo.

## Scoperta chiave da verificare per prima cosa

La codebase ha **già** un meccanismo di cap per liquidità, mai attivato
in nessuno studio della sessione precedente. In
`bt-core/strategies/multiTickerStrategy.py` (classe base di
`OvernightAH`):

- Parametri: `max_adv_participation` (frazione del volume $ medio
  giornaliero, default `None` = disattivato), `liquidity_lookback`
  (finestra SMA del volume, default 20 barre).
- `_liquidity_avg_volume[data]`: SMA del volume per ogni ticker,
  calcolata in `__init__`.
- `_liquidity_cap_notional(data)`: calcola
  `avg_volume * price * max_adv_participation` come tetto massimo
  del notional per un ordine su quel titolo quel giorno.
- `_cap_entry_notional(data, desired_notional)`: se il notional
  desiderato supera il tetto, lo riduce e logga
  `"ordine ingresso ridotto per liquidità %.2f -> %.2f (max_adv_participation=%s)"`.
- `_order_target_percent_entry_capped`: variante che applica lo stesso
  cap a ordini espressi come target percentuale di equity.

`_cap_entry_notional` è già chiamato sia nell'entry normale
(`overnight_ah.py`, dentro il ciclo di apertura posizioni, righe
~636-643) sia nel sizing dell'hedge SQQQ (riga ~1690) — è quindi già
**cablato ovunque serve**, semplicemente mai acceso: in ogni
`STRATARGS` usato finora in questa sessione (e in `development.env`)
`max_adv_participation` non è mai stato impostato, quindi resta al
default `None` (nessun cap).

**Prima domanda da rispondere nella nuova sessione**: basta accendere
`max_adv_participation` con un valore sensato per contenere
l'esplosione da compounding+leva osservata nella sessione precedente,
mantenendo `sizing_policy='legacy'` così com'è? Se sì, gran parte del
lavoro è già fatto e serve "solo" calibrare il valore giusto — se no,
serve capire cosa manca (es. il cap agisce per-titolo ma non
ridistribuisce il capitale non investito verso altri candidati, quindi
potrebbe semplicemente lasciare cash inutilizzato invece di risolvere
il problema).

## Obiettivo dello studio

Trovare una combinazione di `sizing_policy` + `max_adv_participation`
(+ eventualmente una nuova policy di sizing) + `max_concurrent` che,
con leva 2x reale e capitalizzazione (compound) attivi, produca una
curva equity 2000-2026 in cui ogni ordine sia sempre dimensionabile in
modo realistico rispetto al volume scambiato di quel titolo quel
giorno — non un moltiplicatore assoluto enorme e non eseguibile nella
realtà.

## Requisiti espliciti dell'utente

1. **Restare in compound + leva 2x.** Non è uno studio per tornare a
   `fixed_notional` — è per rendere `legacy`/compounding credibile.
2. **Vincolo di liquidità**: la dimensione di ogni ordine non deve
   alterare il mercato. Punto di partenza: `max_adv_participation`/
   `_cap_entry_notional` già esistenti (vedi sopra). Verificare nel
   dettaglio: quanti ordini vengono tagliati, di quanto, su quali
   titoli/periodi, e quale valore di `max_adv_participation` è
   ragionevole per l'universo tradabile (`config-common/tickers/
   yahoo_adj_research_universe_hedge.json`).
3. **Sizing sensibile a regime di mercato, struttura del titolo,
   rischio** — non necessariamente banale: valutare se serve una
   nuova `sizing_policy` (es. inverse-volatility, risk-budget per
   titolo, scaling per regime di mercato) oltre al semplice cap di
   liquidità, o se il cap da solo basta. Questa è analisi/design da
   fare nella nuova sessione, non decisa qui.
4. **Numero di slot concorrenti**: oggi `max_concurrent=3`. Se la
   liquidità satura 3 slot senza perturbare il mercato, valutare
   l'estensione a 4/5/10 slot — verificare prima quanta liquidità è
   disponibile nell'universo tradabile con il cap attivo, poi decidere
   se più slot sono sostenibili o diluiscono solo il capitale su
   candidati meno liquidi.

## Cosa NON fare (vincoli da preservare, appresi nella sessione precedente)

- **Non toccare `overnight-ah-development.env`** né alcuna
  configurazione di produzione/paper — questo è uno studio, non un
  deployment. Applicare eventuali risultati solo dopo conferma esplicita
  dell'utente.
- **`auction`**: resta sempre `True` in ogni backtest. `OvernightAH.__init__`
  ora forza `auction=True` quando `self._live_mode=='backtest'` (fix
  applicato nella sessione precedente, `bt-core` commit `ce1cd55`) —
  con `auction=False` in backtest ogni trade chiude a PnL=0 per
  costruzione (entry e uscita appaiata eseguono sulla stessa barra allo
  stesso prezzo). Il fix rende sicuro anche non specificarlo
  esplicitamente, ma non affidarsi al fix per pigrizia: passare sempre
  `auction=True` esplicitamente nei nuovi studi per chiarezza.
- **Metodologia Sharpe**: usare sempre il calcolo giornaliero
  (`r.mean()/r.std()*sqrt(252)` sui `returns.csv`, coincide con
  QuantStats/`stats.html`). **Mai** il valore stampato in coda al log
  di `btmain.py` dall'analyzer nativo (`bt.analyzers.SharpeRatio`, che
  usa `timeframe=Years, riskfreerate=0.01` — calcolato su ~26 punti
  annuali, non giornalieri, numeri non confrontabili con QuantStats).
  Questa discrepanza ha già causato confusione una volta.
- **Un controllo fisso per studio**: non ricambiare il benchmark a
  metà confronto — stessa disciplina già stabilita nella sessione
  precedente.
- **Verificare sempre errori/warning nei log prima di trarre
  conclusioni** (`grep -iE "error|exception|traceback" runtime.log`,
  scartando i falsi positivi noti tipo "Impossibile mostrare i
  risultati di PyFolio"). Sia il bug `auction=False` sia il bug del
  prezzo storico di SQQQ erano silenziosi nei numeri finali ma visibili
  nei log — non dare per buono un risultato senza controllare.

## File e funzioni di riferimento

- `bt-core/strategies/multiTickerStrategy.py`: `max_adv_participation`,
  `liquidity_lookback` (params, dichiarati vicino agli altri params
  della classe base), `_liquidity_avg_volume` (SMA volume per ticker,
  `__init__`), `_liquidity_cap_notional()`, `_cap_entry_notional()`,
  `_order_target_percent_entry_capped()` (righe indicative ~370-1160,
  verificare i numeri di riga esatti all'apertura della nuova sessione
  perché il file può essere cambiato).
- `bt-core/strategies/overnight_ah.py`: `_candidate_allocations()`
  (tutte le `sizing_policy` esistenti: `legacy`, `selectable_fixed`,
  `selected_equal`, `current_slots`, `rank_decay`, `reverse_rank_decay`,
  `fixed_notional`), `max_concurrent`/`size_by_max_concurrent` (params),
  uso di `_cap_entry_notional` nell'entry normale e nel sizing hedge.
- `docs/context/ah_context.md`: leggere almeno le sezioni elencate nel
  Contesto sopra prima di iniziare — spiegano perché `fixed_notional`
  è nato, il bug `auction=False`, la discrepanza Sharpe, e i numeri
  del compounding con leva letterale, per non ripetere gli stessi
  errori metodologici già trovati e corretti.

## Piano di lavoro suggerito

1. Leggere le sezioni rilevanti di `ah_context.md` indicate sopra.
2. **Esperimento 0 (baseline)**: `sizing_policy='legacy'`, leva 2x
   reale (`max_exposure=$MAX_EXPOSURE`, valore di `development.env`),
   `max_concurrent=3` (invariato), accendere `max_adv_participation`
   con un valore di partenza plausibile (es. 1-2%) e confrontare contro
   lo stesso controllo senza cap (`max_adv_participation` non
   impostato). Quanto si riduce il moltiplicatore finale su 26 anni?
   Quanti ordini vengono effettivamente tagliati (contare i log
   "ordine ingresso ridotto per liquidità")? Su quali titoli/periodi
   si concentrano i tagli?
3. **Sweep di `max_adv_participation`** (es. 0,5% / 1% / 2% / 5%) per
   trovare un valore che tenga il moltiplicatore finale in un range
   credibile senza svuotare troppo il segnale (troppi ordini
   tagliati/skippati per cash insufficiente sotto `min_cash_per_trade`).
4. Solo dopo aver capito l'effetto del semplice cap di liquidità:
   valutare se serve una `sizing_policy` nuova (che tenga conto di
   regime di mercato, struttura del titolo, rischio) o se il cap di
   liquidità unito a `legacy` è già sufficiente per un risultato
   credibile.
5. **Sweep di `max_concurrent`** (4/5/10) sopra la configurazione
   trovata ai punti 3-4, verificando se la liquidità disponibile
   nell'universo tradabile sostiene più slot concorrenti o se satura
   già a 3 (troppi ordini tagliati/skippati all'aumentare degli slot).
6. Documentare ogni passo in `ah_context.md` con lo stesso stile già
   usato in questa sessione (tabelle numeriche, un controllo fisso per
   confronto, Sharpe giornaliero, verifica di errori/warning prima di
   ogni conclusione riportata).

## Verifica end-to-end

- Ogni run deve terminare senza errori/eccezioni nel log.
- Il moltiplicatore finale su 26 anni va sempre confrontato
  esplicitamente con i tre punti di riferimento già trovati nella
  sessione precedente (stesso periodo, stesso metodo Sharpe
  giornaliero):
  - `fixed_notional`: rendimenti nell'ordine di centinaia di %.
  - `legacy` senza cap, senza leva: ~19.000-30.000x.
  - `legacy` senza cap, con leva 2x: ~95-159 milioni x.
  La nuova configurazione deve posizionarsi in un punto dello spettro
  che l'utente giudichi credibile — non è un numero deciso a priori in
  questo brief.
- Nessuna modifica a `development.env` senza presentare prima i
  risultati e ottenere conferma esplicita dell'utente.

## Non-goal

- Nessuna modifica a `overnight_ah_flat_composite.py` o al profilo
  paper `challenger` — questo studio riguarda solo il dimensionamento
  ordini della strategia base `OvernightAH`.
- Nessuna decisione presa qui su quale `max_adv_participation` o
  `max_concurrent` sia "giusto" — è oggetto dello studio stesso.
