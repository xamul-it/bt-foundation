# Step 1 — Analisi delle feature RTH-only basate su gain/log-gain

## 1. Premessa corretta

L’obiettivo non è adattare meccanicamente indicatori tecnici classici come EMA, MACD, RSI o Supertrend a una serie RTH.

Il problema di partenza è diverso: gli indicatori classici, costruiti su serie close-close, incorporano salti overnight e quindi non sono coerenti con una strategia intraday che opera durante la sola sessione RTH.

La soluzione non è “ricalcolare gli stessi indicatori” in modo artificiale, ma costruire un nuovo set di feature basato su:

```text
Open RTH
High RTH
Low RTH
Close RTH
Volume RTH
```

assumendo che i dati daily Yahoo siano già riferiti alla sessione regolare, quindi senza premarket e after-hours.

La trasformazione centrale è:

```text
r_t = ln(C_t / O_t)
```

dove:

```text
O_t = open RTH del giorno t
C_t = close RTH del giorno t
```

Questa serie non è un prezzo. È una serie di log-gain giornalieri RTH.

Quindi non deve essere trattata come una normale serie close-close. Gli indicatori derivati da essa devono avere nomi, interpretazioni e logiche proprie.

---

## 2. Obiettivo dello Step 1

Lo Step 1 serve a costruire e studiare un dataset giornaliero `symbol/date` contenente feature candidate RTH-only, calcolate su gain, log-gain, escursioni e volumi della sessione regolare.

Non serve ancora decidere soglie operative.  
Non serve ancora ottimizzare una strategia.  
Non serve ancora stabilire quale feature “funziona”.

Serve invece a:

- costruire feature matematicamente coerenti;
- evitare contaminazione overnight;
- avere valori confrontabili tra simboli diversi;
- avere valori confrontabili tra periodi diversi;
- verificare distribuzione, stabilità e ridondanza delle feature;
- preparare il dataset per Step 2: correlazione, mutual information e selezione feature.

---

## 3. Assunzioni operative

Per questa versione del framework si assumono queste condizioni:

```text
1. I dati daily Yahoo sono già RTH-only.
2. Open = apertura della sessione regolare.
3. High = massimo della sessione regolare.
4. Low = minimo della sessione regolare.
5. Close = chiusura della sessione regolare.
6. Volume = volume della sessione regolare.
7. Premarket e after-hours non sono inclusi nei dati daily.
```

Se una di queste assunzioni non fosse vera, occorrerebbe tornare ai dati intraday e ricostruire la sessione RTH.  
Ma se le assunzioni sono vere, non serve ricostruire nulla.

---

## 4. Perché non usare close-close

Il rendimento daily classico è:

```text
cc_ret_t = ln(C_t / C_{t-1})
```

Questo rendimento mescola due componenti:

```text
ln(O_t / C_{t-1})   = overnight / premarket repricing
ln(C_t / O_t)       = rendimento RTH
```

Quindi:

```text
ln(C_t / C_{t-1}) = ln(O_t / C_{t-1}) + ln(C_t / O_t)
```

Per una strategia intraday, il secondo termine è quello rilevante.  
Il primo può essere informativo come variabile diagnostica, ma non deve contaminare il filtro principale se l’obiettivo è selezionare titoli con pressione durante RTH.

---

## 5. Serie primitive RTH

Per ogni giorno `t` e simbolo `S`, si calcolano le seguenti primitive.

### 5.1 Close gain RTH

```text
r_t = ln(C_t / O_t)
```

Interpretazione:

```text
pressione netta della sessione RTH
```

Valori positivi indicano chiusura sopra l’apertura RTH.  
Valori negativi indicano chiusura sotto l’apertura RTH.

Questa è la serie principale.

---

### 5.2 Upper excursion RTH

```text
u_t = ln(H_t / O_t)
```

Interpretazione:

```text
massima esplorazione rialzista rispetto all’apertura RTH
```

Misura quanto il titolo è riuscito a salire durante la sessione, indipendentemente da dove ha chiuso.

---

### 5.3 Lower excursion RTH

```text
d_t = ln(L_t / O_t)
```

Interpretazione:

```text
massima esplorazione ribassista rispetto all’apertura RTH
```

Di norma è un valore negativo o nullo.

---

### 5.4 Range RTH

```text
range_t = ln(H_t / L_t)
```

Interpretazione:

```text
escursione complessiva della sessione RTH
```

Serve come proxy di volatilità/tradabilità giornaliera.

---

### 5.5 Volume RTH

```text
vol_t = Volume_t
logvol_t = ln(Volume_t)
```

Il volume è una misura di partecipazione e liquidità.  
Per molte analisi conviene usare `logvol_t`, perché il volume grezzo è fortemente asimmetrico.

---

### 5.6 Overnight return diagnostico

Facoltativo ma utile:

```text
on_t = ln(O_t / C_{t-1})
```

Non è una feature RTH, ma serve a capire se un titolo è mosso prevalentemente da overnight oppure da RTH.

---

## 6. Punto concettuale fondamentale: gain vs livello

La serie:

```text
r_t = ln(C_t / O_t)
```

non è una serie di prezzo.  
È una serie di incrementi giornalieri RTH.

Di conseguenza:

- una media mobile su `r_t` misura drift medio RTH;
- una somma rolling su `r_t` misura momentum cumulato RTH;
- una deviazione standard su `r_t` misura volatilità della pressione RTH;
- un rapporto tra somma e somma dei valori assoluti misura efficienza direzionale RTH.

Non bisogna chiamare queste misure con gli stessi nomi degli indicatori classici se il loro significato cambia.

Per esempio:

```text
EMA20(close)         = trend smussato del prezzo
EWM20(r_t)           = drift medio esponenziale dei gain RTH
sum20(r_t)           = momentum cumulato RTH a 20 giorni
sum20(r_t)/vol20     = momentum RTH normalizzato
```

Sono oggetti diversi.

---

## 7. Due strade possibili

Dalla serie dei gain RTH si possono costruire due famiglie di feature.

---

## 7.1 Famiglia A — feature rolling direttamente sui gain RTH

Questa è la famiglia più semplice, robusta e confrontabile.

Si lavora direttamente su:

```text
r_t = ln(C_t / O_t)
```

E si calcolano misure rolling su finestre fisse.

Esempi:

```text
rth_sum_5
rth_sum_20
rth_mean_20
rth_std_20
rth_z_20
rth_eff_20
rth_winrate_20
```

Vantaggi:

- valori confrontabili tra simboli;
- valori confrontabili tra date;
- niente dipendenza dall’origine del backtest;
- nessun bisogno di costruire pseudo-prezzi;
- interpretazione chiara.

Svantaggi:

- non replica la logica visiva degli indicatori classici;
- non produce un “trend line” tradizionale;
- alcuni indicatori classici non sono trasferibili.

Questa è la famiglia da privilegiare nello Step 1.

---

## 7.2 Famiglia B — pseudo-livello cumulativo RTH

Si può costruire una curva cumulata:

```text
R_t = Σ r_i
```

cioè:

```text
R_t = r_1 + r_2 + ... + r_t
```

Questa rappresenta la performance cumulata che si otterrebbe considerando solo il contributo RTH, ignorando overnight.

Su `R_t` si potrebbero calcolare oggetti simili a:

```text
EMA su R_t
MACD su R_t
Bollinger su R_t
```

Tuttavia il livello di `R_t` dipende dall’origine del dataset.

Quindi il valore assoluto di:

```text
EMA20(R_t)
```

non è direttamente confrontabile tra periodi e simboli se la cumulata parte da origini diverse.

Sono più sensate le differenze normalizzate, per esempio:

```text
R_t - EWM20(R_t)
EWM20(R_t) - EWM100(R_t)
```

normalizzate per volatilità rolling.

Questa famiglia può essere utile, ma è più complessa e meno pulita per lo scopo iniziale.

---

## 8. Scelta consigliata per lo Step 1

Per lo Step 1 si consiglia di partire dalla Famiglia A:

```text
feature rolling direttamente sui gain/log-gain RTH
```

La Famiglia B può essere aggiunta in un secondo momento come confronto, ma non deve essere il nucleo del dataset iniziale.

La ragione è semplice: l’obiettivo dello Step 1 è studiare indicatori normalizzati, confrontabili e non contaminati dai gap. Le feature rolling sui gain RTH soddisfano direttamente questo obiettivo.

---

## 9. Feature candidate principali

### 9.1 Momentum cumulato RTH

Per una finestra `N`:

```text
rth_mom_N = Σ r_t negli ultimi N giorni
```

Esempi:

```text
rth_mom_3
rth_mom_5
rth_mom_10
rth_mom_20
rth_mom_50
```

Interpretazione:

```text
performance cumulata prodotta solo durante RTH negli ultimi N giorni
```

È una delle feature più importanti.

---

### 9.2 Drift medio RTH

```text
rth_mean_N = mean(r_t, N)
```

Interpretazione:

```text
guadagno medio RTH per sessione negli ultimi N giorni
```

È simile a `rth_mom_N / N`.

Probabilmente sarà altamente correlato al momentum cumulato sulla stessa finestra.

---

### 9.3 Drift esponenziale RTH

Invece di chiamarlo EMA, è preferibile chiamarlo:

```text
rth_ewm_mean_N
```

Definizione:

```text
rth_ewm_mean_N = media esponenziale di r_t con span N
```

Interpretazione:

```text
drift RTH recente con maggiore peso alle sessioni più recenti
```

Non è una EMA del prezzo.  
È una stima esponenziale del rendimento intraday medio.

Esempi:

```text
rth_ewm_mean_5
rth_ewm_mean_20
rth_ewm_mean_50
```

---

### 9.4 Accelerazione del drift RTH

Oggetto simile alla logica MACD, ma da non chiamare MACD classico.

```text
rth_drift_accel_12_26 = rth_ewm_mean_12 - rth_ewm_mean_26
```

Eventuale signal:

```text
rth_drift_accel_signal_9 = EWM9(rth_drift_accel_12_26)
```

Istogramma:

```text
rth_drift_accel_hist = rth_drift_accel_12_26 - rth_drift_accel_signal_9
```

Interpretazione:

```text
il drift RTH recente sta migliorando o peggiorando rispetto al drift più lento
```

---

### 9.5 Volatilità dei gain RTH

```text
rth_std_N = std(r_t, N)
```

Esempi:

```text
rth_std_5
rth_std_20
rth_std_50
```

Interpretazione:

```text
instabilità della pressione RTH
```

Serve anche come denominatore per normalizzare momentum e drift.

---

### 9.6 Momentum RTH normalizzato

```text
rth_mom_norm_N = rth_mom_N / rth_std_N
```

Meglio, se si vuole evitare il problema della scala con N:

```text
rth_mom_norm_N = rth_mom_N / (rth_std_N * sqrt(N))
```

Interpretazione:

```text
forza cumulata RTH rispetto alla volatilità storica RTH
```

Questa è una feature direttamente confrontabile tra simboli e periodi.

---

### 9.7 Efficiency ratio RTH

```text
rth_eff_N = abs(Σ r_t) / Σ abs(r_t)
```

Dove le somme sono sugli ultimi `N` giorni.

Valori:

```text
vicino a 1 = movimento RTH direzionale e coerente
vicino a 0 = alternanza / rumore / mancanza di direzione
```

Esempi:

```text
rth_eff_5
rth_eff_10
rth_eff_20
```

Se serve mantenere anche il segno:

```text
rth_signed_eff_N = Σ r_t / Σ abs(r_t)
```

Questa versione è spesso più utile per ranking long/short.

---

### 9.8 Win rate RTH

```text
rth_winrate_N = count(r_t > 0, N) / N
```

Esempi:

```text
rth_winrate_5
rth_winrate_10
rth_winrate_20
```

Interpretazione:

```text
percentuale di sessioni RTH positive nella finestra
```

Questa misura è grezza ma intuitiva.  
Probabilmente sarà correlata con efficiency e momentum, ma non necessariamente identica.

---

### 9.9 Upper/lower pressure

Usando:

```text
u_t = ln(H_t / O_t)
d_t = ln(L_t / O_t)
```

si possono costruire:

```text
rth_upside_N = mean(u_t, N)
rth_downside_N = mean(abs(d_t), N)
rth_pressure_balance_N = rth_upside_N - rth_downside_N
```

Interpretazione:

```text
asimmetria tra escursione rialzista e ribassista durante RTH
```

Questa famiglia è utile perché un titolo può chiudere poco sopra l’open ma avere avuto forte esplorazione rialzista, oppure viceversa.

---

### 9.10 Close efficiency dentro il range RTH

Si può misurare dove chiude il titolo rispetto alle escursioni intraday.

Una forma semplice:

```text
rth_close_to_range_t = r_t / range_t
```

Dove:

```text
range_t = ln(H_t / L_t)
```

Interpretazione:

```text
quanta parte del range RTH si traduce in chiusura netta
```

Valori positivi indicano chiusura sopra open; valori negativi chiusura sotto open.

Attenzione: se il range è molto piccolo, serve gestire divisioni instabili.

Rolling:

```text
rth_close_to_range_mean_5
rth_close_to_range_mean_20
```

---

### 9.11 Range expansion RTH

```text
rth_range_mean_N = mean(range_t, N)
```

Espansione:

```text
rth_range_exp_5_20 = mean(range_t, 5) / mean(range_t, 20)
```

Interpretazione:

```text
la tradabilità/escursione RTH recente è in espansione o compressione?
```

Questa feature è importante per strategie intraday tipo Supertrend, perché un titolo troppo compresso può generare segnali deboli.

---

### 9.12 Volume participation RTH

Usando `logvol_t`:

```text
rth_logvol_z_N = (logvol_t - mean(logvol_t, N)) / std(logvol_t, N)
```

Oppure rapporto semplice:

```text
rth_rvol_5_20 = mean(volume, 5) / mean(volume, 20)
```

Interpretazione:

```text
partecipazione recente rispetto alla norma del titolo
```

Il volume serve a distinguere movimento reale da movimento poco partecipato.

---

### 9.13 Relative strength RTH vs benchmark

Per benchmark `B`, ad esempio QQQ o SPY:

```text
rth_rs_t = r_t(symbol) - r_t(benchmark)
```

Rolling:

```text
rth_rs_mom_N = Σ rth_rs_t su N giorni
```

Esempi:

```text
rth_rs_qqq_5
rth_rs_qqq_20
rth_rs_spy_5
rth_rs_spy_20
```

Interpretazione:

```text
sovra/sottoperformance RTH del titolo rispetto al mercato durante la sola sessione regolare
```

Per titoli tecnologici/Nasdaq, QQQ è probabilmente più coerente di SPY.

---

### 9.14 Overnight dependency diagnostica

Anche se il filtro principale è RTH, è utile misurare quanto il titolo dipende da overnight.

```text
on_t = ln(O_t / C_{t-1})
```

Rolling:

```text
on_mom_N = Σ on_t
rth_vs_on_N = Σ r_t / (abs(Σ on_t) + epsilon)
```

Oppure:

```text
rth_share_N = abs(Σ r_t) / (abs(Σ r_t) + abs(Σ on_t))
```

Interpretazione:

```text
il movimento multiday del titolo nasce più durante RTH o fuori orario?
```

Questa feature può servire per evitare titoli che sembrano forti solo per gap overnight.

---

## 10. Feature table consigliata

La tabella base deve avere una riga per:

```text
symbol + feature_date
```

Campi minimi:

```text
feature_date
symbol
open
high
low
close
volume

rth_loggain
rth_up_loggain
rth_down_loggain
rth_range_log
log_volume
overnight_loggain
```

Feature rolling candidate:

```text
rth_mom_5
rth_mom_10
rth_mom_20
rth_mom_50

rth_mean_5
rth_mean_20
rth_mean_50

rth_ewm_mean_5
rth_ewm_mean_20
rth_ewm_mean_50

rth_drift_accel_12_26
rth_drift_accel_signal_9
rth_drift_accel_hist

rth_std_5
rth_std_20
rth_std_50

rth_mom_norm_5
rth_mom_norm_20
rth_mom_norm_50

rth_eff_5
rth_eff_10
rth_eff_20
rth_signed_eff_5
rth_signed_eff_10
rth_signed_eff_20

rth_winrate_5
rth_winrate_10
rth_winrate_20

rth_upside_5
rth_upside_20
rth_downside_5
rth_downside_20
rth_pressure_balance_5
rth_pressure_balance_20

rth_close_to_range
rth_close_to_range_mean_5
rth_close_to_range_mean_20

rth_range_mean_5
rth_range_mean_20
rth_range_exp_5_20

rth_rvol_5_20
rth_logvol_z_20

rth_rs_qqq_5
rth_rs_qqq_20
rth_rs_spy_5
rth_rs_spy_20

on_mom_5
on_mom_20
rth_share_20
```

---

## 11. Normalizzazione e confrontabilità

La priorità è rendere le feature confrontabili tra:

```text
1. simboli diversi
2. date diverse
3. regimi diversi
```

### 11.1 Feature già naturalmente normalizzate

Sono già adimensionali:

```text
ln(C/O)
ln(H/O)
ln(L/O)
ln(H/L)
Σ ln(C/O)
rth_eff_N
rth_winrate_N
rth_close_to_range
rth_rs_N
```

Queste feature sono già molto più confrontabili del prezzo o delle medie mobili sul prezzo.

---

### 11.2 Normalizzazione per volatilità

Per momentum e drift conviene aggiungere versioni volatility-adjusted:

```text
rth_mom_norm_N = rth_mom_N / (rth_std_N * sqrt(N))
```

Questo evita di selezionare automaticamente solo titoli più volatili.

---

### 11.3 Ranking cross-sectional giornaliero

Per ogni `feature_date`, calcolare anche:

```text
feature_rank_pct
```

Esempio:

```text
rth_mom_20_rank_pct
rth_eff_20_rank_pct
rth_range_exp_5_20_rank_pct
rth_rvol_5_20_rank_pct
```

Questa trasformazione è molto utile perché il problema operativo è selezionare simboli da un universo.

---

### 11.4 Z-score cross-sectional

Opzionale:

```text
feature_cs_z = (feature - mean_cross_section) / std_cross_section
```

Meno robusto del rank percentile in presenza di outlier, ma utile per alcune analisi.

---

## 12. Regola anti-lookahead

Le feature calcolate usando i dati del giorno `D` possono essere usate solo dal giorno `D+1`.

Pertanto è consigliabile generare due date:

```text
feature_date = giorno fino al quale sono calcolate le feature
trading_date = giorno in cui le feature sono utilizzabili
```

Esempio:

```text
feature_date = 2026-05-25
trading_date = 2026-05-26
```

Nel backtest intraday del 26 maggio si usano solo righe con:

```text
trading_date = 2026-05-26
```

---

## 13. Output dello Step 1

### 13.1 File primitive

```text
rth_primitives.parquet
```

Contiene:

```text
feature_date
symbol
open
high
low
close
volume
rth_loggain
rth_up_loggain
rth_down_loggain
rth_range_log
log_volume
overnight_loggain
```

---

### 13.2 File feature rolling

```text
rth_features.parquet
```

Contiene tutte le feature candidate rolling.

---

### 13.3 File Backtrader universe map

```text
rth_universe_map.parquet
```

Contiene:

```text
trading_date
feature_date
symbol
feature columns
optional_score
optional_flags
```

Questo file sarà usato dal framework Backtrader per selezionare i simboli eleggibili all’inizio di ogni giornata.

---

## 14. Analisi descrittiva dello Step 1

Per ogni feature calcolare:

```text
count
missing_ratio
mean
median
std
min
max
p01
p05
p25
p75
p95
p99
skewness
kurtosis
```

Obiettivi:

- identificare feature instabili;
- identificare feature dominate da outlier;
- identificare feature quasi costanti;
- individuare problemi di dati;
- capire la distribuzione prima di passare alla correlazione.

---

## 15. Controlli specifici importanti

### 15.1 Confronto RTH vs close-close

Per verificare che la trasformazione abbia senso, calcolare:

```text
cc_logret = ln(C_t / C_{t-1})
rth_loggain = ln(C_t / O_t)
overnight_loggain = ln(O_t / C_{t-1})
```

E verificare:

```text
cc_logret ≈ overnight_loggain + rth_loggain
```

Questa identità serve come controllo di coerenza.

---

### 15.2 Titoli forti close-close ma deboli RTH

Individuare casi in cui:

```text
cc_mom_20 > 0
rth_mom_20 <= 0
```

Questi sono esattamente i titoli che il filtro RTH dovrebbe evitare se la strategia intraday cerca pressione durante la sessione.

---

### 15.3 Titoli forti RTH ma neutri close-close

Individuare casi in cui:

```text
rth_mom_20 > 0
cc_mom_20 circa 0 o negativo
```

Questi possono essere candidati interessanti, perché mostrano pressione RTH non visibile nel ranking daily tradizionale.

---

### 15.4 Stabilità temporale

Le feature devono essere analizzate per:

```text
anno
trimestre
regimi di volatilità
```

Una feature utile non deve dipendere esclusivamente da un singolo periodo di mercato.

---

### 15.5 Stabilità cross-symbol

Controllare se una feature è significativa su molti simboli o solo su pochi titoli estremi.

---

## 16. Cosa NON fare nello Step 1

### 16.1 Non chiamare EMA/MACD classici oggetti che lavorano su gain

Meglio usare nomi come:

```text
rth_ewm_mean
rth_drift_accel
rth_mom
```

per evitare confusione concettuale.

---

### 16.2 Non costruire pseudo-livelli se non necessari

La cumulata globale può essere utile, ma introduce dipendenza dall’origine del dataset.

Per lo Step 1 è preferibile lavorare su finestre rolling.

---

### 16.3 Non ottimizzare soglie

Lo Step 1 serve a costruire e validare il dataset di feature.

La selezione di soglie appartiene agli step successivi.

---

### 16.4 Non usare indicatori operativi intraday sul daily RTH

Supertrend, ATR intraday, VWAP intraday, opening range e segnali 5m appartengono alla strategia intraday.  
Qui si costruisce il filtro universo multiday.

---

## 17. Domande ancora aperte

Prima dell’implementazione conviene chiarire alcuni punti.

### 17.1 Universo titoli

Qual è l’universo iniziale?

Esempi:

```text
NASDAQ 100
S&P 500
lista custom
solo titoli con prezzo > X
solo titoli con volume medio > Y
```

---

### 17.2 Benchmark

Quale benchmark usare per la relative strength RTH?

Possibili scelte:

```text
QQQ
SPY
settoriale ETF
benchmark diverso per settore
```

Per strategie su titoli Nasdaq, QQQ sembra la scelta naturale.

---

### 17.3 Direzione strategia

La strategia intraday sarà:

```text
solo long
solo short
long/short
```

Questo incide sulla scelta tra:

```text
rth_eff_N
rth_signed_eff_N
```

Per solo long, il segno è fondamentale.

---

### 17.4 Orizzonte del filtro multiday

Quali finestre testare inizialmente?

Proposta minima:

```text
5 giorni
10 giorni
20 giorni
50 giorni
```

---

### 17.5 Modalità di selezione universe

La selezione sarà fatta per:

```text
soglie fisse
ranking top N
top percentile
combinazione score
```

Per lo Step 1 basta calcolare le feature; per Backtrader servirà decidere come leggere la mappa.

---

## 18. Sintesi finale

La nuova impostazione corretta è:

```text
Non adattare indicatori classici close-close.
Costruire nuove feature RTH-only basate su log-gain.
```

La serie centrale è:

```text
r_t = ln(C_t / O_t)
```

A questa si affiancano:

```text
u_t = ln(H_t / O_t)
d_t = ln(L_t / O_t)
range_t = ln(H_t / L_t)
logvol_t = ln(Volume_t)
on_t = ln(O_t / C_{t-1}) come diagnostica
```

Le feature principali non sono EMA/MACD/RSI classici, ma:

```text
rolling RTH momentum
rolling RTH drift
exponential RTH drift
RTH drift acceleration
RTH volatility
RTH normalized momentum
RTH efficiency
RTH win rate
RTH range expansion
RTH volume participation
RTH relative strength
RTH vs overnight dependency
```

Questo dataset sarà poi usato nello Step 2 per:

```text
correlazione
mutual information
scarto feature sovrapposte
valutazione informatività rispetto al target
```

La strategia Backtrader intraday rimane separata: userà queste feature solo come filtro universo giornaliero, non come segnali operativi 5m.

