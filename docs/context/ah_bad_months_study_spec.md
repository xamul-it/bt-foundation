# Studio mesi negativi OvernightAH — specifiche per la prossima analisi

Questo documento e' la specifica autosufficiente per lo studio successivo a
quello su hedge SQQQ / fix broker margine (vedi `docs/context/ah_context.md`
e `docs/overnight_ah_sintesi.md` per il contesto completo di cosa e' gia'
stato fatto). Scritto per essere letto in una sessione ripartita da zero,
senza memoria della conversazione precedente.

**Priorita' esplicita dell'utente**: questo studio (mesi negativi) viene
prima della validazione dell'hedge su `weak_theme_switch` gia' pianificata
in `ah_context.md` ("Prossima analisi: validazione hedge su
`weak_theme_switch`"). Quella resta valida e da fare, ma non e' piu' il
prossimo passo immediato.

## Domanda di partenza (testuale, dall'utente)

> la prossima analisi si deve concentrare sui mesi negativi. ad esempio ci
> sono asset che entrano in questi mesi e sporcano il risultato? qualche
> segnale premonitore, azioni da fare quando siamo nella palta [difficolta]?
> pattern riconoscibili?

Tradotto in domande operative:

1. Nei mesi negativi della strategia, ci sono asset specifici la cui
   presenza nel paniere tradato coincide con (o spiega in parte) il
   risultato negativo? E' un problema di *selezione universo* (weak_theme /
   weak_theme_switch scelgono l'asset sbagliato quel mese) o di
   *comportamento del singolo asset* (un ticker che va storicamente male in
   certe condizioni)?
2. Esistono segnali osservabili **prima** che il mese negativo si manifesti
   (quindi utilizzabili operativamente, no lookahead) che lo anticipano?
3. Se un segnale esiste, quali azioni concrete si potrebbero attivare
   ("cosa fare quando siamo nella palta"): ridurre esposizione, escludere
   temporaneamente un ticker, anticipare l'hedge, fermare la strategia per
   N giorni?
4. Ci sono pattern riconoscibili e ricorrenti (stagionalita', clustering
   temporale, sovrapposizione con drawdown macro noti) piuttosto che mesi
   negativi isolati e imprevedibili?

## Vincoli metodologici (validi per l'intero progetto, non solo questo studio)

- **No lookahead**: qualunque segnale premonitore proposto deve essere
  calcolabile solo con dati noti prima dell'inizio del periodo che si vuole
  anticipare. Se un segnale usa dati del mese stesso che si vuole prevedere,
  e' inutile operativamente anche se "funziona" in analisi.
- **Pandas prima, Backtrader dopo**: costruire le serie di rendimento e le
  feature via pandas per iterare velocemente; solo un'ipotesi che sopravvive
  al proxy pandas va poi verificata/implementata in Backtrader.
- **Granularita' daily, non mensile**, per costruire feature e serie di
  rendimento: nello studio hedge precedente, partire da un pannello
  mensile (poche decine di osservazioni) ha prodotto conclusioni fragili
  che si sono ribaltate passando a granularita' daily. Il "mese negativo"
  e' l'unita' con cui si etichetta l'esito, ma le feature/segnali andrebbero
  costruiti su dati daily quando possibile.
- **Campione piccolo, alto rischio overfitting**: i mesi negativi storici
  sono presumibilmente poche decine su un intero storico dal 2016/2018 al
  2026. Qualunque pattern trovato va verificato su un segmento OOS separato
  (train/validation/OOS, ranking per **minima** metrica tra segmenti, non
  media) prima di essere considerato valido. Con cosi' pochi eventi, e'
  facile trovare un "pattern" che e' solo rumore.
- **Correlazione vs causalita'**: se un ticker compare piu' spesso nei mesi
  negativi, verificare se e' perche' contribuisce lui stesso al risultato
  negativo o perche' viene semplicemente selezionato piu' spesso in
  condizioni di mercato che sono negative per tutti (confusione tra
  "il ticker causa il problema" e "il ticker e' un sintomo del regime").

## Dati gia' disponibili da riusare (verificarne la freschezza prima di usarli)

Percorsi indicativi sotto `bt-core/out/overnight_ah/OvernightAH/`, uno per
ogni run gia' eseguito in sessioni precedenti (nomi esatti delle directory
da confermare con `ls`, potrebbero essere stati puliti):

- `static_bench_train_stable_ah_top10_train/`, `..._val_.../`, `..._oos_.../`
  — backtest statico-10 per segmento.
- `native_switch_3mpos_train_warmup2015_trade2016/`,
  `native_switch_3mpos_val_warmup2020_trade2021/`,
  `native_switch_3mpos_oos_warmup2023_trade2024/` (e varianti `_top40`/
  `_top60`/`6mpos`) — backtest `weak_theme_switch` per segmento.

Dentro ciascuna directory, i file utili:

- `returns.csv` (colonne `index,return`): rendimento giornaliero del
  portafoglio. Base per costruire la serie di rendimento mensile (Fase 1).
- `trades.json` (lista di dict, uno per trade chiuso): campi rilevanti
  `asset`, `open_datetime`, `close_datetime`, `pnl`, `pnl_pct`, `size`,
  `entry_side`. **Questo e' il file chiave per la Fase 2** (decomposizione
  per-simbolo del P&L mensile) — non serve rifare backtest per quello gia'
  disponibile.
- `positions.csv`: valore di posizione per simbolo per giorno — utile per
  capire l'esposizione effettiva giorno per giorno, ma verificare che
  l'header includa tutti i simboli attesi (in almeno un run controllato in
  questa sessione conteneva una sola colonna simbolo oltre a `cash`: puo'
  darsi che quel run specifico avesse un paniere degenere, o vada
  rigenerato).
- `orders.json`: storico ordini con eventi — utile per capire fallimenti/
  rifiuti (es. per verificare se margine/rifiuti concentrano nei mesi
  negativi, collegamento diretto col bug broker gia' risolto).

Script di ricerca riusabili in `bt-strategy-test/overnight-ah/research/`:

- `edge_prediction_study.py`: genera il pannello mensile
  `feature_target_panel.csv` con feature ex-ante per simbolo/mese (base di
  partenza per candidate feature premonitrici, gia' usato per
  `c2c_mean_6m`, `ah_mean_6m`, `semis_total_3m` ecc.).
- `counter_cyclical_symbol_study.py`: loader di dati daily raw da
  `config-common/data/d/yahoo_adj/*.csv` dal 2000, e util di correlazione —
  riusabile per costruire feature daily per qualunque ticker candidato.
- `sqqq_hedge_timing_study.py`: contiene `load_night_return_series`,
  `load_close_series`, `build_static_night_return`, `portfolio_metrics`,
  `segment_mask` — utili direttamente anche per questo studio (costruzione
  rendimento overnight per simbolo/paniere, metriche di portafoglio,
  maschere di segmento train/val/OOS).

Se questi output sono stati puliti o sono scaduti, vanno rigenerati con i
comandi di riferimento gia' documentati in `ah_context.md` (baseline
statico-10 e OOS `weak_theme_switch`) prima di iniziare l'analisi.

## Definizione operativa di "mese negativo" (primo passo, da fissare)

Non ancora deciso — scegliere/testare in ordine di preferenza:

1. Rendimento mensile del portafoglio (da `returns.csv` compoundato per
   mese) sotto una soglia fissa (candidati: 0%, -2%, -5% — sensibilita' da
   testare, non assumere una soglia arbitraria senza guardare la
   distribuzione reale dei rendimenti mensili storici).
2. Percentile: bottom decile o bottom quartile dei mesi storici per
   rendimento — piu' robusto a soglie arbitrarie ma dipende dalla lunghezza
   campione.
3. Basato su drawdown: mesi che fanno parte dei principali episodi di
   drawdown gia' noti (2018 Q4? 2020 marzo? 2022) invece di una soglia
   mensile isolata — cattura la persistenza (un mese -1% dentro un
   drawdown di 3 mesi e' diverso da un mese -1% isolato).

Iniziare guardando la distribuzione reale (istogramma dei rendimenti
mensili) prima di fissare una soglia: la soglia deve emergere dai dati, non
essere scelta a priori.

Fare l'analisi **sia su static-10 sia su weak_theme_switch**, ma dare
priorita' a static-10 come caso primario: e' il caso con lo storico piu'
lungo e gia' ben validato (2018-2026), mentre `weak_theme_switch` ha solo
la finestra 2023-2026 e un paniere che ruota (utile per la Fase 2 sulla
selezione universo, ma un campione di mesi negativi piu' corto per la Fase
1/3).

## Piano di analisi (fasi)

### Fase 1 — Identificare i mesi negativi

Costruire la serie di rendimento mensile da `returns.csv` (o rigenerarla se
mancante), applicare la definizione scelta, produrre una lista di mesi
negativi con rendimento associato. Output atteso: una tabella/CSV
`{mese, rendimento, e' negativo}` per static-10 e per weak_theme_switch.

### Fase 2 — Composizione del paniere nei mesi negativi vs positivi

Da `trades.json`, per ogni mese (negativo e positivo):

- Static-10 (paniere fisso): scomporre il P&L mensile per simbolo. Domanda:
  nei mesi negativi, il P&L negativo e' concentrato su 1-2 simboli o
  distribuito su tutto il paniere? Se concentrato, quali simboli ricorrono
  piu' spesso come "peggior contributore" nei mesi negativi rispetto alla
  loro quota attesa?
- Weak_theme / weak_theme_switch (paniere che ruota): estrarre quali ticker
  erano effettivamente tradabili quel mese (dal CSV di selezione mensile se
  disponibile, altrimenti dai simboli presenti in `trades.json`/
  `positions.csv` quel mese) e confrontare la composizione dei mesi
  negativi vs quelli positivi. Domanda aggiuntiva specifica per questo
  modo: la selezione (`weak_theme`/`weak_theme_switch`) sceglie sistemati-
  camente peggio proprio nei mesi che poi risultano negativi (es. tilt
  semis quando i semis sono il problema di quel mese)?
- In entrambi i casi: verificare con un test statistico semplice (non
  serve altro, campione piccolo) se la differenza di frequenza/contributo
  osservata e' oltre il rumore atteso da un campione cosi' piccolo, prima
  di trarre conclusioni.

### Fase 3 — Segnali premonitori

Costruire, a fine mese precedente (dato noto, niente lookahead), le
seguenti feature candidate e confrontarne la distribuzione tra "mese prima
di un mese negativo" vs "mese prima di un mese normale":

- `semis_total_3m` / `semis_total_6m` (gia' feature nel codice, usata come
  switch feature in `weak_theme_switch`) — verificare se il suo segno/
  livello nei giorni prima del mese negativo era gia' negativo: se si', il
  segnale di regime esiste gia' nel codice ma forse soglia/lag attuali
  (`monthly_universe_switch_threshold=0.0`) non lo sfruttano abbastanza in
  anticipo.
- Drawdown SPY a 3 mesi (`spy_dd3m`, gia' usato in
  `monthly_universe_spy_dd3m_threshold`) — stessa verifica.
- Dispersione/correlazione cross-sectional del paniere tradato nell'ultimo
  mese (se i ticker iniziano a muoversi tutti insieme, spesso precede
  stress/perdita di diversificazione).
- Livello aggregato di volatilita' intraday del paniere (la strategia gia'
  filtra per trade su `min_intraday_vol`/`max_intraday_vol`; qui interessa
  il livello medio/aggregato del mese precedente, non il filtro per-trade).
- Frequenza di rifiuti per margine/cancellazioni nel mese precedente (da
  `orders.json`) — ipotesi debole ma verificabile a costo quasi zero dato
  che il dato esiste gia'.

Per ciascun segnale candidato: costruire la serie mensile (valore a fine
mese) e confrontare la distribuzione nei due gruppi (prima di mese
negativo vs prima di mese normale). Solo segnali con separazione chiara e
robusta tra train/validation/OOS vanno considerati.

### Fase 4 — Pattern riconoscibili

- Stagionalita': i mesi negativi si concentrano in certi mesi dell'anno
  (es. settembre/ottobre, notoriamente deboli per l'equity in generale)?
  Verificare se vale specificamente per il comportamento overnight/AH di
  questo paniere, non assumere che la stagionalita' equity generica si
  applichi automaticamente.
- Clustering temporale: i mesi negativi sono isolati o tendono a
  raggrupparsi in sequenze di 2-3 mesi consecutivi? Rilevante per capire se
  serve una regola di "uscita durante" (una volta dentro, quanto dura)
  piuttosto che solo un filtro preventivo.
- Sovrapposizione con drawdown macro noti (2018 Q4, 2020 marzo, 2022): i
  mesi negativi della strategia coincidono sempre con drawdown di mercato
  generali, o esistono mesi negativi idiosincratici della strategia mentre
  il mercato (SPY/QQQ) e' calmo? Se esistono casi idiosincratici, meritano
  un'indagine separata (probabilmente selezione universo, non regime
  macro).

### Fase 5 — Ipotesi di azione operativa (solo proposte, da validare)

Sulla base di quanto emerge nelle fasi precedenti, proporre 2-3 regole
testabili, ad esempio (esempi indicativi, non prescrittivi — le regole
vere devono emergere dai dati):

- Escludere temporaneamente dal paniere un ticker che ha mostrato N segnali
  di stress ravvicinati (definizione di "segnale di stress" da Fase 3).
- Ridurre `max_exposure` (o anticipare l'attivazione dell'hedge SQQQ gia'
  implementato) quando un segnale premonitore di Fase 3 supera una soglia
  — verificare prima se `risk_overlay` (gia' esistente in
  `overnight_ah.py`, trigger su rendimento trailing) copre gia' questo
  caso o se i segnali trovati qui sono complementari/piu' anticipatori.
- Uscita anticipata o stop temporaneo della strategia per M giorni dopo N
  segnali di stress consecutivi.

Ogni ipotesi va poi validata con lo stesso rigore del resto del progetto:
proxy pandas veloce prima, poi Backtrader, poi split train/validation/OOS
con ranking sulla minima metrica tra segmenti (mai la media).

## Cosa NON fare

- Non modificare `overnight_ah_live.py` o i parametri di produzione sulla
  base di pattern trovati su un campione di poche decine di mesi negativi
  senza validazione OOS separata.
- Non confondere correlazione con causalita' nelle Fasi 2/3 (vedi vincoli
  metodologici sopra).
- Non abbandonare la disciplina anti-lookahead gia' collaudata nel resto
  del progetto: qualunque segnale o regola deve essere calcolabile solo
  con dati noti prima del periodo che si vuole anticipare.
- Non ripetere l'errore gia' fatto nello studio hedge di partire da
  granularita' mensile per le feature/correlazioni: usare daily quando
  possibile, mensile solo per l'etichetta finale (mese negativo si'/no).

## Riferimenti

- `docs/context/ah_context.md`: meccanica completa della strategia,
  risultati storici, sezione "Hedge overnight SQQQ" (errori gia' commessi
  e corretti nello studio precedente, applicabili anche qui), sezione
  "Prossima analisi: validazione hedge su `weak_theme_switch`" (studio
  successivo a questo, non ancora iniziato).
- `docs/overnight_ah_sintesi.md`: sintesi strategica generale e tabellare.
