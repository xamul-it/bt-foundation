# Strategia RTH: percorso agentico di ricerca e validazione

Data: 2026-09-14. Stato: protocollo da eseguire, non risultato di backtest.
Revisione 2: target e vincoli esecutivi specificati dall'utente; prevalgono
sulle assunzioni della prima versione.

## Ambito tecnico: Daily Open-to-Close, timeframe D

Questo playbook definisce una ricerca **RTH Daily Open-to-Close (O-C),
timeframe D**. Il target di previsione è il rendimento dall'apertura ufficiale
alla chiusura ufficiale della sessione successiva. Le feature sono costruite su
una osservazione per sessione RTH e il ranking dei titoli è mensile.

Non è un protocollo per strategie su barre 5 minuti o 15 minuti. Non include
test di Supertrend, RSI, MACD, breakout intraday o sistemi con più decisioni di
ingresso durante la seduta. I dati minute hanno soltanto due ruoli: controllo
qualità degli estremi open/close giornalieri e verifica dell'esecuzione di un
candidato Daily O-C già selezionato. Non alimentano feature, ranking o modelli
P3/P4. Di conseguenza, un risultato positivo o negativo di questo protocollo è
limitato al disegno Daily O-C e non può essere esteso a strategie 5m/15m.

La copia in `manifests/playbook_frozen.md` resta l'istantanea normativa della
ricerca già eseguita; questo chiarimento rende esplicito l'ambito per letture e
versioni successive senza cambiare retroattivamente quel protocollo congelato.

## 1. Mandato e risultato richiesto

Costruire e verificare una strategia su azioni USA che seleziona titoli prima
dell'apertura, compra durante la sessione regolare e termina ogni seduta senza
posizioni. Riutilizzare l'architettura di ricerca AH: feature ex ante, ranking,
selezione, portafoglio ed esecuzione. Stimare il segnale sul rendimento RTH.

Il lavoro deve rispondere a tre domande, in ordine:

1. Le feature RTH disponibili ieri distinguono i rendimenti RTH di domani?
2. La selezione produce un portafoglio redditizio dopo costi, con rischio
   accettabile e vantaggio rispetto a un benchmark RTH comparabile?
3. Il comportamento è riproducibile in Backtrader e nell'esecuzione prevista?

Una strategia «funzionante» in questo protocollo deve superare tutte e tre.
Un motore che genera ordini o un backtest positivo non bastano.
Il protocollo non garantisce la scoperta di un vantaggio economico. Un esito
negativo riproducibile è un completamento valido della ricerca, ma NON significa
che l'obiettivo economico sia raggiunto. Vietato continuare a cercare finché
qualcosa appare positivo.

L'esecuzione autonoma autorizzata dal protocollo termina alla preparazione di
un candidato al paper e del relativo dossier. La richiesta di questo documento
non autorizza ordini, modifiche ai profili esistenti, deployment o capitale reale.

## 2. Fonti da leggere e ordine di precedenza

Percorsi relativi alla root `/home/htpc/backtrader`:

| Fonte | Uso |
|---|---|
| `bt-strategy-test/RTH_analysis/analisi_step_1_rth_only_indicatori_intraday.md` | Definizione delle feature RTH native |
| `docs/context/rth_analysis.md` | Pipeline primitive → feature → trading_date |
| `docs/rth_context.md` | Ricerca daily, ranking, filtri e stop iniziali |
| `bt-strategy-test/rth_open_close/rth_dynamic_selection_notes.md` | Diario esteso; leggere prima l'aggiornamento metodologico di luglio |
| `bt-strategy-test/rth_open_close/README.md` | Stato del candidato q4 declassato |
| `docs/context/strategy-analysis-methodology.md` | Convenzioni generali e finalizzazione Backtrader |
| `bt-core/strategies/rth_open_close.py` | Implementazione daily da verificare prima del riuso |
| `bt-core/strategies/rth_minute_execution.py` | Possibile base per verifica esecutiva al minuto |

Le conclusioni finali del diario prevalgono sulle sue precedenti diciture
«candidato migliore». Codice e dati devono confermare le descrizioni: non
assumere che file, parametri o copertura siano ancora identici.

Questo protocollo specifica i confronti economici e i gate della nuova ricerca.
Le somme di bps non ponderate previste nella metodologia storica restano
diagnostiche: non sono l'alpha di un portafoglio quando cambiano numero di
titoli, nozionali o esposizione. I benchmark decisionali qui sotto usano pesi
e capitale espliciti. Non mescolare Sharpe annuale nativo BT e Sharpe daily.

### Risultati precedenti da non riscoprire

- La lista statica selezionata sullo storico completo è un controllo contaminato.
- Win rate RTH a 36 mesi → top 30 → ordinamento a 3 mesi per Sortino/media
  è una baseline esistente, non una nuova scoperta.
- Stop daily stretti hanno degradato il candidato originario.
- Miglioramenti su daily OHLC con TP non si sono trasferiti automaticamente
  all'esecuzione al minuto.
- Filtri scoperti su copertura SIP parziale si sono indeboliti ampliando i dati.
- q4 era forte nel 2023–2026, ma la verifica continua 2020–giugno 2025 riportava
  DD circa −51% e Sharpe circa 0,53 con filtro semis. Non ripromuoverlo.
- Aggiunte minute-native e ML sui posti liberi non hanno risolto il problema.
- Il precedente obiettivo di sostituire AH non coincide con verificare una
  strategia RTH autonoma. Il nuovo confronto primario è RTH contro RTH.

Questi sono risultati riportati dalle fonti, non ricalcolati per il documento.
Non trasferirne numeri a nuovi periodi o configurazioni.

## 3. Configurazione iniziale congelata

| Dimensione | Prima versione |
|---|---|
| Direzione | Long-only; motivazione: continuità con AH e assenza di modello prestito titoli |
| Sessione | Solo RTH; ingresso limite a inizio sessione, uscita limite prima del close con fallback market |
| Segnale | Ranking dopo il close precedente; quote RTH correnti ammesse solo per prezzo limite/esecuzione |
| Frequenza ranking | Mensile; eleggibilità e sizing aggiornati ogni giorno |
| Esposizione | Controllo 1x; scenari 2x e fino a 4x solo se consentiti da conto, titolo e buying power Alpaca |
| Capitale simulato | USD 100.000 |
| Posizioni principali | Pool operativo massimo 10 slot; target almeno 5 round-trip completati per giorno di borsa in media |
| Controlli breadth | 5 e 20, usati come sensibilità, non per scegliere il massimo finale |
| Budget iniziale | 95% del budget consentito alla leva di scenario; stesso scenario anche sui benchmark |
| Liquidità | ADV20 raw precedente ≥ USD 20 milioni; prezzo raw precedente ≥ USD 5 |
| Cap per ordine | Minimo fra budget slot, buying power disponibile e 1% ADV20 precedente; USD 10.000 solo come scenario diagnostico separato |
| Quantità | Intere, determinate da prezzo noto al momento dell'ordine |
| Stop/TP | Assenti nella prima versione; uscita a fine sessione obbligatoria |
| Earnings/AH/semis | Nessun filtro nel modello principale iniziale |
| Target edge | Media netta ≥ 10 bps per round-trip dopo fill realisti, slippage, commissioni e altri costi attribuibili |
| Costi screening | Commissioni Alpaca esplicite + ipotesi round-trip di frizione 0/5/10/20 bps; 10 bps di edge NON sono il budget costi |

Questi valori sono scelte progettuali, non parametri empiricamente ottimali,
né stime garantite di slippage. Congelarli prima dei risultati.
Il cap nozionale riguarda l'ordine stimato: un gap può portare il fill oltre
il cap. Simulare il problema e applicare le regole del broker, senza usare
retroattivamente l'open per scegliere quantità perfette.

Il modello daily apertura/chiusura resta un controllo ideale di segnale, non
una prova di raggiungimento del target con ordini limite. La versione esecutiva
ha prezzi e orari propri e deve essere simulata fin da P2 su un campione.

### 3a. Target richiesto e rendimento del conto

Requisiti dell'utente: almeno 10 bps netti medi per trade e media di almeno
5 trade completati per giorno di borsa, includendo giorni senza eseguiti.
Un trade è un round-trip economico: i fill parziali non sono trade aggiuntivi.
Registrare separatamente l'obiettivo espresso di +0,5% medio giornaliero sul
conto: NON deriva dalla moltiplicazione 5 × 0,1% dei rendimenti per trade.

```text
edge_net_i = PnL_net_i / nozionale_ingresso_i
ritorno_conto_d = sum_i PnL_net_i / equity_inizio_d
               = sum_i (nozionale_ingresso_i / equity_inizio_d) * edge_net_i
```

Con cinque posizioni uguali simultanee, piena allocazione e ciascuna a +0,1%
netto, il conto rende +0,1% a 1x, +0,2% a 2x, +0,4% a 4x; con buffer e cap
anche meno. +0,5% richiederebbe in questo esempio 5x, oppure edge maggiore:
12,5 bps a 4x pienamente utilizzata, 25 bps a 2x, 50 bps a 1x.
Riutilizzi sequenziali di capitale non sono dimostrati dalla strategia che
mantiene posizioni per la sessione; non inventarli per raggiungere il target.

Esempio a capitale USD 100.000 e cinque trade da USD 10.000: 10 bps netti
producono USD 10 per trade, USD 50 al giorno, cioè +0,05% del conto.
Per questo il vecchio cap assoluto USD 10.000 non è più un vincolo principale.

Esportare due esiti distinti: `trade_targets_pass` (edge e frequenza) e
`account_target_pass` (media aritmetica dei rendimenti daily netti ≥ 0,005).
Il secondo è un obiettivo aggiuntivo da misurare: se fallisce, non dichiarare
raggiunto il target giornaliero dell'utente anche se i target per trade passano.
La leva non migliora automaticamente l'edge sul nozionale e non crea segnale.

### 3b. Contratto ordini limite e fallback

Regole iniziali proposte per rendere eseguibile il requisito, da congelare in
P0; orari e offset sono assunzioni progettuali, non valori forniti dall'utente:

1. Alle 09:30:05 ET, con prima quota RTH valida, inviare buy LIMIT DAY al
   bid osservato (tick valido); nessun aggiornamento automatico del prezzo.
2. Deadline ingresso 09:35 ET: cancellare il residuo, conservare eventuale
   quantità eseguita. Nessun fallback market in ingresso, né rimpiazzo del
   titolo non eseguito. La frequenza target riguarda gli eseguiti, non gli invii.
3. A close ufficiale meno 10 minuti, inviare sell LIMIT DAY all'ask valido
   della quantità realmente detenuta. È un'uscita temporale, non un take profit.
4. A close meno 5 minuti, cancellare il residuo limite; attendere conferma,
   riconciliare eventuali fill durante la cancellazione, poi inviare MARKET
   per la sola posizione residua. Non avere limite e market concorrenti che
   possano aprire involontariamente uno short.
5. Verificare flat e stato ordini fino al close. Se cancellazione o mercato
   non permettono il flattening, registrare incidente e posizione residua:
   niente chiusura fittizia al prezzo daily. Early close usa lo stesso offset.

La quota determina il limite e non modifica il ranking. Una quota stale o
assente blocca l'ingresso. Un limite protegge il prezzo, non garantisce il
fill: considerare coda, spread, latenza, fill parziali e selezione avversa.
Su barre minute un semplice touch high/low non basta a dimostrare l'esecuzione.
Senza quote/tick usare scenari conservativi di attraversamento e partecipazione,
senza fill nella barra usata per decidere; etichettarli come proxy e non come
verifica esecutiva finale. La simulazione finale richiede dati compatibili con
la precisione temporale dichiarata o conferma prospettica; niente dati inventati.

Riportare tassi di fill, ingressi persi, uscita limite/fallback, slippage del
fallback, costo medio/p95, ritardo, partecipazione e PnL delle opportunità perse.
Stressare fallback con slippage aggiuntivo 5/10/20 bps per la sola quantità
venduta market, oltre al prezzo bid/ask simulato; non sommare una seconda volta
spread/slippage già inclusi nei prezzi effettivi.

### 3c. Commissioni e leva Alpaca

Riutilizzare `CommInfo_Alpaca` in `bt-core/commission.py` dopo verifica del
tariffario applicabile: SEC, FINRA TAF, CAT, arrotondamenti, minimi/massimi,
eventuali commissioni specifiche del piano e altri addebiti pertinenti.
Commission-free non significa costo totale zero. Non assumere corretti i
default del codice per ogni data o account. Salvare tariffario, data e hash.
Confrontare scenario costi correnti applicati allo storico e, se disponibile,
scenario con tariffe storiche; non confondere i due. Allocare fee per fill/ordine
secondo la politica verificata, senza contare ogni fill come round-trip separato.

Scenari di leva 1x/2x/4x con segnali invariati: selezionare sulle validazioni
la leva minima che soddisfa i target e i vincoli di rischio, poi congelarla
prima della conferma. Verificare moltiplicatore, buying power effettivo,
marginabilità e requisiti per titolo, ordini aperti, restrizioni del conto e
capitali impegnati da altre strategie. Il massimale Alpaca intraday può arrivare
a 4x, ma non è un'autorizzazione universale o una disponibilità garantita.
Non alzare artificialmente il cap broker per eliminare i rifiuti nel backtest.
L'interesse margin dipende dai saldi debitori e dalla politica Alpaca: non
applicare automaticamente un costo overnight a ogni trade RTH; modellare i
saldi residui e i costi effettivamente dovuti.

Fonti ufficiali consultate il 2026-09-14, da riverificare in P0/P6:

- [Ordini Alpaca](https://docs.alpaca.markets/us/docs/orders-at-alpaca)
- [Margin e buying power](https://docs.alpaca.markets/us/docs/margin-and-short-selling)
- [Tariffario Alpaca Securities](https://files.alpaca.markets/disclosures/BrokFeeSched.pdf)

## 4. Organizzazione per agenti esecutori

### Directory e artefatti

Creare alla prima esecuzione `bt-strategy-test/rth_feature_selection_v1/` con:

```text
README.md                 istruzioni e comando di ripresa
protocol.yaml             configurazione congelata, split, gate e seed
state.json                fase corrente, tentativi, prossimo task
tasks/                    una scheda per task, con dipendenze
manifests/                hash input, commit, ambiente, copertura
scripts/                  harness e controlli della nuova ricerca
tests/                    test delle invarianti economiche e temporali
outputs/                  risultati immutabili per run_id
decisions/                decisioni e motivazioni dei gate
handoffs/                 consegne fra agenti
final/                    dossier conclusivo e config candidata
```

Non riscrivere output dei vecchi studi. Non pulire modifiche dell'utente.
Usare `bt-core/.venv/bin/python`. Prima di editare, leggere eventuali AGENTS.md
applicabili e verificare lo stato Git di root e repository annidati.
Non fare commit o installazioni globali automaticamente per questa ricerca.

### Ruoli

| Ruolo | Responsabilità | Non può fare |
|---|---|---|
| Coordinatore | Assegna task pronti, verifica gate, aggiorna stato | Cambiare obiettivo o soglie dopo aver visto un risultato |
| Esecutore dati | Manifest, feature, allineamenti, QA | Scegliere il modello guardando i rendimenti di test |
| Esecutore ricerca | Implementa modelli e produce metriche | Leggere il holdout finale prima dello sblocco |
| Esecutore BT | Replica segnale e implementa ordini | «Correggere» i risultati cambiando selezione |
| Revisore | Ricostruisce invarianti e controlla evidenze | Accettare una tabella senza manifest/log/output |

Un solo agente può svolgere più ruoli in turni separati. La revisione resta
un task distinto. Con più agenti, parallelizzare solo attività indipendenti:
audit dati e lettura codice; esecuzioni già congelate; revisione di artefatti
immutabili. Mai due agenti sullo stesso file o output. Un solo processo scrive
`state.json`. Non utilizzare tutti i core per ogni agente: budget globale massimo
`max(1, nproc-2)` worker, ridotto se memoria o I/O diventano limitanti.

### Scheda obbligatoria per ogni task

```yaml
task_id: P2_T01
objective: frase concreta con un solo risultato verificabile
depends_on: [P1_PASS]
inputs: [{path: percorso, sha256: hash}]
allowed_write_paths: [percorso]
forbidden_actions: [ordini, modifica_profili, lettura_holdout]
instructions: [passi numerati senza scelte implicite]
acceptance_checks: [comandi e risultato atteso]
outputs: [file obbligatori]
failure_action: codice di recupero dalla sezione 12
```

Ogni consegna riporta: task_id, hash input/output, comandi reali, exit code,
esito controlli, limiti, prossimo task consentito. Nessun «sembra funzionare».
Le istruzioni che seguono descrivono strumenti da costruire dove mancanti;
non sono comandi CLI già disponibili nel repository.

## 5. P0 — Inventario e congelamento del protocollo

1. Inventariare script esistenti, parametri `--help`, dati e run storici.
2. Salvare ambiente Python, versioni, Git HEAD e stato dirty senza segreti.
3. Congelare universo dal file `NASDAQ_100_US.json` se presente e valido;
   risolverne il percorso reale. Escludere ETF inversi/a leva e simboli ausiliari.
4. Registrare che un Nasdaq attuale applicato al passato introduce survivorship
   bias. Cercare dati point-in-time già disponibili; non inventare componenti
   storiche. Se mancanti, limitare esplicitamente le conclusioni all'universo
   sopravvissuto e richiedere evidenza prospettica prima di generalizzare.
5. Fissare data finale comune e dati disponibili. Non aggiornare i CSV durante
   uno studio: creare snapshot o usare hash verificati prima e dopo ogni run.
6. Scrivere protocol.yaml con tutti i numeri del documento e firmarlo via hash.

Gate P0: ogni parametro economico, calendario, split e percorso è risolto.
Se dati insufficienti impediscono gli split previsti, fermare l'accettazione
economica; è consentita solo una prova tecnica etichettata come tale.

### Split temporali e contaminazione storica

- Sviluppo tecnico e prima stima: 2000–2012, usando warm-up precedente dove
  disponibile. Le prime osservazioni prive di storia completa non sono tradabili.
- Validazioni walk-forward annuali: 2013–2022. Ogni anno usa esclusivamente
  dati precedenti, con finestra train espandente.
- Conferma storica congelata: 2023–2025, accessibile solo dopo P4. I modelli
  possono aggiornarsi secondo regole fissate, non secondo risultati osservati.
- 2026 fino alla data congelata: diagnostica recente separata.
- Periodo realmente nuovo: sessioni successive al congelamento del modello.

2023–2026 sono già stati osservati nella ricerca precedente: chiamarli
«conferma storica», non OOS intonso. Nessun artificio di split elimina questa
conoscenza. Valutare anche 2020 e 2022 separatamente senza usarli per aggiungere
filtri correttivi. Non cancellare anni negativi.

## 6. P1 — Contratto dati e controlli anti-lookahead

Riutilizzare le funzioni dei builder in `bt-strategy-test/RTH_analysis/` dopo
averne verificato le formule. Evitare un nuovo download implicito nei builder:
adattare l'ingresso allo snapshot locale e al periodo congelato.

### Schema minimo

Una riga per `(symbol, feature_date)` con:

- OHLCV raw e/o coerentemente adjusted, provider, calendario e timestamp;
- `feature_date`, `available_at`, `trading_date`, `decision_at`;
- feature e flag di eleggibilità;
- target in tabella separata: `target_rth = close[trading_date]/open[trading_date]-1`.

La decisione per la sessione t avviene dopo il close di t−1 e prima degli ordini
di apertura. `available_at <= decision_at` deve valere per ogni ingresso.
Il ranking del mese M usa solo date precedenti a M; il primo giorno del mese
non deve ricevere per errore il ranking vecchio perché l'ordine nasce la sera
precedente. Distinguere sempre data decisione e data sessione target.

### Controlli bloccanti

1. Chiavi uniche; date ordinate; prezzi positivi; OHLC coerenti; volumi validi.
2. Calendario di borsa America/New_York, DST e chiusure anticipate: nessuna
   approssimazione «giorno successivo = +1 giorno» o «chiusura sempre 16:00».
3. No forward fill dei prezzi target o delle sessioni mancanti. Un prezzo di
   uscita mancante dopo un ingresso simulato rende il run incompleto; non
   eliminare quel trade. Separare difetti dati da vere sospensioni di mercato.
4. RTH usa O/H/L/C della stessa base di aggiustamento. ADV e quantità usano
   prezzi/volumi raw o ricostruzione verificata. Split e cambi ticker tracciati.
5. Rendimenti anomali, ad esempio |RTH| > 30%, sono segnalati e verificati;
   non rimossi solo perché scomodi. Se irrisolti, bloccare la conclusione.
6. Verificare `log(C/O) + log(O/C_prev) = log(C/C_prev)` sulle righe valide.
7. Test causale: cambiare tutte le barre dopo decision_at non deve cambiare
   score, selezione o quantità decisi prima. Includere il confine mese/anno.
8. Audit copertura per anno/simbolo, incluse IPO, delisting e gap di dati.
9. Confrontare daily e minute sugli stessi simbolo-giorni; quantificare scarti
   di open/close senza sostituire selettivamente il provider più favorevole.

Output: data_manifest.json, coverage.csv, data_issues.csv, temporal_audit.json.
Gate P1: zero violazioni temporali, zero anomalie bloccanti irrisolte.
La copertura incompleta va risolta o dichiarata nell'universo prima dei modelli;
mai selezionare retroattivamente solo le opportunità con esecuzione conveniente.

## 7. P2 — Baseline economiche e simulatore verificato

Costruire prima i controlli, poi i modelli. Conservare i giorni senza trade con
rendimento zero, salvo eventuali costi realmente dovuti.

| ID | Regola |
|---|---|
| B0 | Cash, rendimento zero, nessuna remunerazione ipotizzata |
| B1 | Equal-weight di tutto l'universo eleggibile, solo RTH, budget 95% |
| B2 | 10 nomi casuali dall'universo eleggibile, 100 percorsi con seed 0…99 |
| B3 | Vecchia selezione 36m win rate → top30 → 3m Sortino; 10 slot |
| B4 | Ranking mensile per media RTH delle ultime 63 sessioni; 10 slot |

B1 misura il rendimento RTH ampio; B2 controlla concentrazione e numero di
ordini; B3/B4 controllano se il nuovo modello aggiunge qualcosa alla selezione
semplice. Tutti usano gli stessi dati, cap, costi e intervallo di valutazione.
La copertura iniziale richiesta da B3 è 36 mesi: i confronti comuni iniziano
quando tutte le baseline sono disponibili. Non penalizzare una baseline con
warm-up conteggiato come rendimento zero.

### Contabilità

Per giorno d: equity iniziale E_d, quantità q_i decisa prima dell'ingresso,
prezzi effettivi P_in/P_out, costi monetari F_i.

```text
PnL_d = sum_i q_i * (P_out_i - P_in_i) - sum_i F_i
r_d = PnL_d / E_d
E_(d+1) = E_d + PnL_d
alpha_d = r_strategia_d - r_benchmark_d
```

Nel modello veloce le frizioni in bps si applicano al nozionale effettivamente
negoziato, oltre alle commissioni Alpaca esplicite. Nel modello esecutivo
usare i prezzi di fill e le fee, senza duplicare la frizione già nei prezzi.
Riportare sia edge lordo/netto per trade sia rendimento del capitale.
Con cap e cash idle, edge/trade × numero trade non è il rendimento del conto.

Prima dello sweep, audit di `RTHOpenClose.next()` e del broker: capire come
`pos.size`, rifiuti e ordini pendenti influenzano le decisioni. Segnale desiderato
e stato esecutivo devono essere distinti. Il controllo esecutivo può impedire
ordini duplicati, ma non deve alterare silenziosamente il ranking.

Eseguire una replica pandas/BT su un campione comune comprendente un cambio
mese, una chiusura anticipata, un gap e un ordine rifiutato. Con identiche
assunzioni richiedere stesso piano ordini e trade abbinati; differenza del
PnL netto totale ≤ max(USD 1, 0,001% del capitale iniziale). Qualunque scarto
maggiore deve avere causa e correzione prima dello sweep. Non calibrare il
simulatore sui risultati desiderati.

Output: baseline_daily.csv, baseline_metrics.csv, order_plan.parquet,
fidelity_report.json, test temporali e di contabilità.
Gate P2: contabilità riconciliata e fedeltà verificata.

## 8. P3 — Feature RTH e test della capacità predittiva

Non iniziare con 21 pesi liberi copiati dal challenger AH.
Usare feature native RTH con semantica verificata nel builder:

| Famiglia | Rappresentanti iniziali |
|---|---|
| Drift | `rth_mean_20`, `rth_mean_63`, `rth_ewm_mean_20` |
| Continuità | `rth_signed_eff_20`, `rth_winrate_63` |
| Rischio | `rth_std_20`, `rth_mom_norm_63` |
| Pressione | `rth_pressure_balance_20`, `rth_close_to_range_mean_20` |
| Range | `rth_range_exp_5_20` |
| Partecipazione | `rth_rvol_5_20` |
| Forza relativa | `rth_rs_20` |

Se il nome differisce, registrare una mappa verso la formula equivalente;
non sostituire con un'altra metrica per far girare lo script.
Momentum cumulato e media alla stessa finestra sono ridondanti: non contarli
come evidenze indipendenti. Feature AH, gap e close-close sono escluse dal
modello principale; restano diagnostiche separate e chiaramente etichettate.

Per ciascuna feature, usando solo train:

1. Misurare missingness, distribuzione e correlazioni fra feature.
2. Calcolare Spearman cross-sectional tra feature al momento decisionale e
   target RTH, quindi aggregare per giorno/mese. Nessuna correlazione pooled
   simbolo-giorno trattata come migliaia di osservazioni indipendenti.
3. Creare quintili cross-sectional, documentando sessioni con meno di 20 nomi.
4. Misurare rendimento futuro dei quintili e spread Q5−Q1 per anno.
5. Decidere orientamento del segnale sul train. Non invertirlo dopo il test.
6. Calcolare incertezza con bootstrap di blocchi temporali, mantenendo tutti
   i simboli dello stesso giorno nello stesso blocco.

Non eliminare una feature solo per p-value univariato: una combinazione può
aggiungere informazione. I risultati servono a controllare formule, ridondanza
e stabilità, non a selezionare retroattivamente il miglior indicatore OOS.

Output: feature_dictionary.csv, feature_audit.csv, quintile_returns.csv,
ic_by_period.csv. Gate P3: feature causali, formule corrette e diagnostica
completa. Segnale debole non autorizza ad aggiungere altre cento feature.

## 9. P4 — Ricerca limitata e walk-forward

Massimo tre famiglie nuove, tutte con ranking mensile e stessa esecuzione:

- M1: media dei rank percentile dei rappresentanti di famiglia, prima media
  entro famiglia poi fra famiglie, così la numerosità non determina il peso.
  Orientamento stimato annualmente solo sul train: segno dello spread Q5−Q1;
  spread esattamente zero → componente omessa.
- M2: Ridge sul target RTH successivo, feature normalizzate per data; alpha
  nella sola griglia {0.1, 1, 10, 100}. Selezione iperparametro tramite split
  temporali interni al train, sulla media dei rendimenti giornalieri netti.
- M3: regressione logistica sul segno del target, C in {0.1, 1, 10}; stesso
  schema temporale. Serve a verificare se prevedere frequenza di successo
  produce valore economico; la win rate da sola non determina il vincitore.

Per M2/M3 normalizzazioni apprese, imputazione e rimozione di correlazioni
ridondanti si stimano solo sul train. Pesi dei simbolo-giorni normalizzati
per giornata, evitando che gli anni con più titoli dominino automaticamente.
Imputazione con mediana train e flag missing; se oltre il 20% delle feature
necessarie di un titolo è mancante, quel titolo è ineleggibile per tutti i
confronti abbinati. Pubblicare anche B1 sull'universo completo come contesto.

Il modello è ristimato una volta l'anno; gli score vengono calcolati a fine
mese con l'ultimo modello disponibile. Gli split interni hanno train di almeno
5 anni e validazione di 1 anno, espandenti, solo entro il train esterno.
Rimuovere ogni label il cui esito non era disponibile al cutoff; per target
giornaliero applicare embargo di una sessione al confine. Non fare split casuali.

Convenzioni deterministiche: rank percentile crescente con pareggi medi;
ordine finale score decrescente, poi ticker crescente. Per M2/M3 usare rank
percentile delle feature meno 0,5 come input, nessuna standardizzazione sul
dataset completo; mediana di imputazione appresa sul solo train. In caso di
parità nella selezione iperparametri preferire regolarizzazione più forte.
Fissare random_state=42 dove supportato e salvare la versione della libreria.
Validare convergenza del modello; un warning di mancata convergenza non è PASS.

Ogni mese calcolare l'ordine per tutti i nomi con feature disponibili, senza
troncamento nascosto del pool. Ogni sera scorrere quell'ordine, applicare
l'eleggibilità nota e prendere i primi N; quantità sotto una azione o notional
sotto USD 100 lasciano lo slot in cash. Nessun rimpiazzo dopo un rifiuto o un
fill mancato all'apertura. Score mensile assente → nessun ingresso sul titolo,
con motivo registrato; nessun fallback silenzioso alla lista statica.

Per scegliere fra M1/M2/M3 usare esclusivamente le validazioni 2013–2022:
prima i gate della sezione 10, poi mediana dell'alpha annuale netto contro B1,
poi Sharpe daily, poi semplicità (M1, M2, M3) in caso di parità numerica.
Solo un candidato arriva alla conferma 2023–2025. Gli altri non vengono aperti
su quel periodo per scegliere a posteriori.

Budget: al massimo 3 famiglie, 12 rappresentanti, 8 configurazioni complessive
M1/M2/M3 per fold (1+4+3), 2 cicli di ricerca. I tre scenari di leva della
sezione 3c sono controlli di sizing successivi, non tre nuove ricerche del segnale.
B3/B4 possono vincere: la
complessità non è un requisito del risultato.

Secondo ciclo consentito solo se il primo fallisce prima della conferma e
mostra una diagnosi precisa. Scegliere UNA variante già prevista:

- ranking settimanale, stessi modelli e feature, se l'analisi train indica
  decadimento del vantaggio entro il mese;
- filtro flat quando la previsione train-calibrata non copre i costi stimati
  più 10 bps di margine netto,
  se il problema è edge lordo positivo ma netto insufficiente.

La scelta della variante usa train e validazioni; registrare la contaminazione
di queste ultime. Dopo due cicli falliti, chiudere la ricerca negativa.
Nessun boosting, deep learning, short, stop/TP o ricerca illimitata di soglie.
Sono nuovi protocolli, non recuperi automatici.

## 10. Gate economici e metriche obbligatorie

Edge 10 bps netti e frequenza media 5 trade/giorno sono requisiti dell'utente.
Le altre soglie sono guardrail progettuali, da congelare in P0, non leggi universali.
Applicarle sia alle validazioni concatenate sia alla conferma storica, con
le differenze di numerosità esplicitate sotto. Vietato abbassarle ex post.

| Controllo | Condizione per candidato al paper |
|---|---|
| Edge richiesto | Media aritmetica netta per round-trip ≥ 10 bps dopo tutti i costi; pubblicare anche media ponderata per nozionale |
| Frequenza richiesta | Round-trip completati / tutti i giorni di borsa valutati ≥ 5; fill parziali aggregati |
| Target conto separato | Media daily netta ≥ +0,5% per `account_target_pass`; non inferirla da edge × trade |
| Redditività minima | CAGR netto ≥ 5% sul capitale; non sostituisce i target edge/frequenza/conto |
| Rischio | Sharpe daily ≥ 0,8; max drawdown non peggiore di −25% alla leva candidata; riportare anche controllo 1x |
| Vantaggio | Media alpha daily contro B1 > 0; limite inferiore CI 95% > 0 |
| Concentrazione | Supera il percentile 95 delle medie daily dei 100 percorsi B2 |
| Complessità | Se M1/M2/M3, alpha medio > 0 contro il migliore B3/B4 scelto sulle validazioni; altrimenti preferire la baseline |
| Stabilità | Rendimento netto positivo in almeno 70% degli anni validazione e almeno 2/3 anni conferma |
| Stress | Nessun anno di validazione/conferma sotto −15%; pubblicare 2020 e 2022 |
| Costi | Screening positivo con commissioni Alpaca + 20 bps frizione round-trip; finale positivo con +20 bps sul fallback market, senza duplicazioni |
| Numerosità | ≥ 1.000 trade e ≥ 500 sessioni attive nelle validazioni; ≥ 300 trade e ≥ 150 sessioni attive nella conferma |
| Eseguibilità | P6 superato, zero posizioni residue e ordini orfani non spiegati |

Bootstrap: 5.000 ricampionamenti di blocchi contigui di 20 sessioni, seed 42,
confronti appaiati sulla stessa sequenza di giorni; sensibilità blocchi 5 e 60.
Usare percentili 2,5 e 97,5. In validazione il CI è descrittivo dopo selezione;
non presentarlo come correzione completa del multiple testing. Nella conferma
storica il gate CI deve passare per tutte e tre le lunghezze dei blocchi.
Resta conferma su storia già nota, non prova prospettica.

Il gate edge richiede stima puntuale ≥ 10 bps e limite inferiore CI 95%
dell'edge netto > 0, con bootstrap temporale. Non significa dimostrare che il
vero edge sia almeno 10 bps: riportare anche se il limite inferiore supera 10.
Il target frequenza vale sul calendario completo, non solo sui giorni attivi.
Per il target conto usare mean(r_d), non CAGR/252 né somme non ponderate di bps.

Metriche da esportare sempre: date, sessioni totali/attive, trade, gain lordo
e netto per trade, win rate, payoff medio, PnL monetario, CAGR, Sharpe daily
0rf, maxDD, esposizione media/massima, turnover, costi totali, cash idle,
cap attivati, rejections e missing data. Formula Sharpe: mean(r)/std(r, ddof=1)
× sqrt(252), comprendendo i giorni di borsa senza trade.

Attribuzione per anno, simbolo e settore se mappa disponibile. Pubblicare il
risultato rimuovendo i 10 migliori giorni e quello senza il simbolo maggiore
contributore: se il PnL netto cambia segno, classificare fragile e non promuovere.
Non ricalibrare il modello su questi campioni diagnostici.

Un modello con redditività positiva ma senza vantaggio su B1 è esposizione
RTH, non selezione dimostrata. Un modello che non batte B3/B4 non giustifica
complessità aggiuntiva. Un modello che fallisce un gate è REJECT o INCONCLUSIVE
per evidenza insufficiente, mai PASS «con fiducia».

## 11. P5–P7 — Conferma, esecuzione, dossier

### P5: aprire una volta la conferma storica

Congelare feature, orientamenti/regole di stima, modello, iperparametri o loro
procedura annuale, soglie, ordine candidati, costi e sizing. Salvare hash.
Eseguire solo candidato selezionato e benchmark congelati su 2023–2025.
Se fallisce, non scegliere il secondo classificato sullo stesso periodo.
Chiudere la versione o raccogliere nuovo periodo prospettico per nuova versione.
Eseguire il 2026 separatamente, senza usarlo per ribaltare un fallimento P5.

### P6: Backtrader e microstruttura

1. Usare o estendere isolatamente RTHOpenClose, senza modificare AH o profili
   schedulati. Nessuna assunzione che parametri waterfall AH siano già supportati.
2. Implementare la macchina ordini della sezione 3b. `coc=True` e i fill
   open/close ideali servono solo come controllo: non sostituiscono limite,
   cancellazione, riconciliazione e fallback market.
3. Verificare supporto LIMIT DAY, MARKET, cancellazione e flusso eventi Alpaca
   su documentazione ufficiale aggiornata; salvarne URL e data. Includere
   chiusure anticipate e restrizioni effettive dell'account.
4. Confrontare il piano giornaliero con gli ordini effettivi: simbolo, quantità,
   data, direzione, timestamp, fill, fee, exit e motivi di omissione.
5. Integrare commissioni Alpaca e slippage per lato come componenti separate.
   Il target 10 bps è NETTO, non una fee. Non sommare ai prezzi effettivi
   anche la frizione generica dello screening. Riconciliare il
   costo effettivo round-trip in bps; non assumere che `--slippage 0.001`
   penalizzi ogni tipo di ordine allo stesso modo.
6. Validare su dati SIP/minute comuni e rappresentativi, includendo stress
   2020/2022 e anni recenti se disponibili. Mancanza di dati stress rende la
   validazione esecutiva INCONCLUSIVE, non autorizza a usare solo il bull market.
7. Per il confronto ideale open/close vs limite/fallback, mantenere lo stesso
   piano segnali. Quantificare separatamente feed, timing, fill mancati e costi.
   Nessun tuning di offset o timeout sulla conferma; se i limiti non raggiungono
   la frequenza richiesta, registrare il fallimento invece di assumere fill.
8. Testare ordini rifiutati, fill parziali, restart, dato stale, festività,
   early close, doppio invio, fine dataset e cancellazione bracket/orfani.
9. Eseguire un run continuo completo con stato reale del broker simulato.
   Non sostituirlo con moltiplicazione di run annuali riavviati.

Per importi ai quali i gate sono dichiarati, misurare partecipazione al volume
disponibile, spread e differenze con le aste. Il cap ADV non dimostra assenza
di impatto. Se non si possono stimare costi credibili, classificare esecuzione
non dimostrata. Ripetere la fedeltà P2 dopo ogni cambiamento esecutivo.

### P7: consegna e fase prospettica

Produrre `final/REPORT.md` con una conclusione esplicita, configurazione,
comandi verificati, periodo e manifest, tabella gate, benchmark, attribuzione,
limiti ed elenco delle ipotesi scartate. Copiare solo artefatti necessari a
riprodurre il candidato. Per ogni metrica collegare il file sorgente.

Il massimo stato autonomo di questa ricerca è `CANDIDATE_FOR_PAPER`.
Questo stato richiede tutti i gate tranne il target conto aggiuntivo, il cui
esito va esposto separatamente. Se `account_target_pass=false`, il dossier
deve dire «target +0,5% giornaliero non raggiunto» e non dichiarare soddisfatto
l'intero obiettivo economico espresso dall'utente.
Preparare un run-config inattivo e istruzioni paper specifiche, senza credenziali
né account impliciti. Chiedere autorizzazione solo per l'attivazione concreta,
presentando strategia, account, dimensioni, orari e gestione errori.

Dopo autorizzazione, una verifica prospettica richiede almeno 60 sessioni e
300 round-trip completati, entrambe le condizioni. Nessun tuning nel periodo.
Confrontare selezione prevista/eseguita, costo realizzato, posizioni residue,
PnL e incidenti. Zero incidenti critici irrisolti per completare il test tecnico.
Il paper verifica soprattutto il flusso operativo: fill simulati non dimostrano
impatto reale. Redditività prospettica e intervalli di confidenza vanno riportati
anche se sfavorevoli. Mai abilitare live automaticamente al sessantesimo giorno.

Un futuro studio AH+RTH richiede contabilità condivisa, transizione di posizioni
all'open, capitale effettivamente disponibile e costi abbinati. Non sommare due
curve indipendenti assumendo due volte lo stesso capitale.

## 12. Macchina a stati, recupero e condizioni di arresto

```text
P0_FREEZE → P1_DATA → P2_BASELINES → P3_FEATURES → P4_WALK_FORWARD
 → P5_CONFIRMATION → P6_EXECUTION → P7_DOSSIER → CANDIDATE_FOR_PAPER

Ogni fase: TODO → RUNNING → REVIEW → PASS
Uscite alternative: TECHNICAL_BLOCK, INCONCLUSIVE, REJECTED, NEGATIVE_RESULT
```

`state.json` deve contenere phase, status, protocol_hash, input_hashes,
completed_tasks, running_processes, retries, research_cycle, decision_file e
next_task. Salvare in modo atomico. Conservare i log anche dei run falliti.

| Evento | Azione obbligatoria |
|---|---|
| Crash tecnico | Conservare log, diagnosticare, massimo 2 retry dopo fix; poi TECHNICAL_BLOCK |
| Dato mancante | Tentare recupero read-only da fonte autorizzata; nessuna imputazione del target; se non risolto INCONCLUSIVE |
| Input modificato | Invalidare fase e discendenti; nuovo manifest/run_id, mai riuso cache invisibile |
| Errore temporale | Bloccare ogni metrica; correggere e rigenerare tutti i risultati dipendenti |
| Proxy ≠ BT | Tornare a P2/P6, isolare scarto trade per trade; niente sweep ulteriore |
| Risultato economico negativo | Applicare budget P4; non trattarlo come bug né ripetere lo stesso run |
| Fallimento conferma | REJECTED; niente sostituzione del vincitore usando la conferma |
| Dati/campione insufficienti | INCONCLUSIVE, indicare quali dati mancano e perché |
| Due cicli falliti | NEGATIVE_RESULT con dossier e ipotesi confutate |
| Processo lungo | Registrare PID/sessione e avanzamento; non avviare duplicati alla ripresa |
| Richiesta deployment | Preparare artefatti concreti; attivare solo con autorizzazione specifica |

Il budget non si azzera cambiando nome alla strategia. Nuova feature, universo,
direzione o finestra dopo fallimento è una nuova ipotesi da registrare, non un
fix tecnico. Non riutilizzare la conferma come test intonso della nuova ipotesi.

## 13. Prompt pronto per l'agente coordinatore

```text
Leggi docs/context/strategies/rth-agentic-research-playbook.md.
Esegui il protocollo come ricerca isolata. Non attivare paper/live, non
modificare profili esistenti, non fare commit automatici.

Se state.json esiste, riprendi dalla prima dipendenza non completata dopo
aver verificato hash e processi attivi. Altrimenti crea P0 e le schede task.
Non assumere che gli script proposti dal playbook esistano già: verifica e
implementa solo quelli necessari alla fase corrente.

Assegna agli esecutori un solo task delimitato per volta. Fornisci percorsi,
formule, controlli, output e azione in caso di fallimento. Un revisore deve
verificare ogni gate prima dell'avanzamento. Non aprire la conferma prima P5.

Prosegui autonomamente nelle attività locali autorizzate. Se un controllo
economico fallisce applica la macchina a stati, senza cambiare le soglie.
Alla conclusione consegna il dossier con stato CANDIDATE_FOR_PAPER,
NEGATIVE_RESULT o INCONCLUSIVE; in caso di blocco tecnico indica la causa
precisa, i tentativi effettuati e l'input necessario alla ripresa.
Non promettere una strategia profittevole e non dichiararla validata sulla
sola base di una metrica favorevole.
```

## 14. Checklist del revisore finale

- [ ] Il protocollo e il budget sono stati congelati prima dei risultati.
- [ ] Nessuna informazione futura influenza ranking, quantità o universo.
- [ ] Survivorship e storia già osservata sono dichiarati.
- [ ] Benchmark, sizing, costi e periodi sono comparabili.
- [ ] I giorni senza trade e gli ordini falliti sono conservati.
- [ ] Il vantaggio delle feature è distinto dal semplice rendimento RTH.
- [ ] Le scelte non sono state corrette sul periodo di conferma.
- [ ] Costi per lato, commissioni e cap sono riconciliati in moneta.
- [ ] La strategia termina flat e replica il piano segnali verificato.
- [ ] Stress, continuo completo e attribuzione non contraddicono il verdetto.
- [ ] Ogni gate ha PASS/FAIL/INCONCLUSIVE, evidenza e comando riproducibile.
- [ ] L'esito finale non implica autorizzazione al trading reale.
