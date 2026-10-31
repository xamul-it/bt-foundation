# Revisione 01 — protocollo prima delle simulazioni

Data: 24 ottobre 2026. Mandato: rivedere il candidato, poi decidere l'applicazione.
Scope: ricerca statistica sintetica, nessuna modifica al cruscotto o ai servizi.
Gli artefatti del primo studio restano immutati. L'esito può essere negativo.

## Decisione

Valutare se un singolo score non limitato da ranghi intermedi recuperi potenza
con storie corte, e se un limite calibrato sul massimo dell'intero periodo riduca
i falsi allarmi di controlli ripetuti. Distinguere idoneità descrittiva puntuale,
regola di allarme su un orizzonte fissato, e applicazione alle strategie reali.

## Difetti del candidato precedente

- Due calibrazioni rade consumano lo storico: 2.480 osservazioni danno solo 16
  blocchi nella calibrazione finale; p minimo 1/17 > 5%.
- Il minimo di p-value intermedi satura: eventi più estremi possono ricevere lo
  stesso score, prima ancora dell'ultima calibrazione.
- Correggere cinque finestre allo stesso istante non controlla cento date.

## Candidati fissati

C0: ensemble precedente, ricostruito sullo stesso storico complessivo e con lo
stesso training di 1.000; gap 100, due segmenti con blocchi di 20, passo 40.

C1: un solo score, una sola calibrazione. Training di 1.000 osservazioni; dal
solo training si stimano frequenze di parole ordinate lunghe 1/2/3, con pseudoconteggio
0,5. Per ogni finestra di 3/5/10/15/20 si calcolano tre scarti di composizione
G = 2 sum c log(c/(m*p)), più la serie identica massima. G è qui uno score:
NON viene confrontato con una chi-quadro iid. Ogni componente è centrata sulla
media e divisa per la deviazione standard delle finestre mobili di training,
con minimo della scala 0,25. Non si attribuisce significato gaussiano a tale
normalizzazione. Lo score è il massimo scarto assoluto fra componenti; lo score
comune alle cinque finestre è il massimo dei loro score. Nessun rango intermedio.

Dopo gap 100, calibrazione puntuale sui blocchi successivi di 20, passo 20.
Rango superiore finale (1 + numero score storici >= score recente)/(B+1), pareggi
conservativi. I blocchi adiacenti non vengono dichiarati indipendenti/scambiabili;
la calibrazione temporale è empirica e ne misuriamo l'errore su molte baseline.
Lo score può crescere oltre tutti gli score di training; il p-value finale resta
correttamente limitato dalla quantità reale di calibrazione. Nessun p=0.

C2: stesso score C1, soglia appresa da periodi storici completi. Un periodo lungo
H richiede H+19 osservazioni, include H controlli e tutte e cinque le finestre.
Si calibra il massimo dell'intero periodo. H=20 e H=100, con passo H+40 fra
periodi: 21 osservazioni fra la fine di un periodo e l'inizio del successivo.
Stessa soglia al 5% per l'intero periodo prefissato. Non si rinnova automaticamente
il budget ogni mese e non si promette controllo su un orizzonte illimitato.
Se 1/(B+1)>5%, il risultato di allarme è NON STIMABILE al livello scelto,
non evidenza di stabilità. Si conserva comunque la misura continua descrittiva.

## Dati e confronti

Storie totali N=2.480, 4.960, 9.200; uniche trasformazioni apprese nel training.
Null: iid uniforme, sticky rho=0,6, sticky rho=0,9, ciclo direzionale, memoria a
lag 2 e memoria a lag 10. I processi di memoria sono catene sticky indipendenti
interlacciate, inizializzate nella marginale uniforme; non si assume ordine 1
per la validità dello score. Non esistono garanzie contro memoria arbitraria.

Scenari predefiniti:
- iid -> iid, sticky 0,6, lag2, ciclo, bias marginale lieve (p6=0,25, altri=0,15);
- sticky 0,6 -> se stesso, iid, sticky 0,9, sticky 0,5, sticky 0,7;
- sticky 0,9 -> se stesso, iid, sticky 0,6;
- ciclo -> se stesso, ciclo inverso, iid;
- lag2 -> se stesso, iid, lag10;
- lag10 -> se stesso, iid, lag2.
20 baseline indipendenti per combinazione null/N, 1.000 episodi di 20 osservazioni
per scenario/baseline. Salvataggio di input e conteggi. Soglia nominale 5%.

Monitoraggio: N=4.960, 20 baseline/null; 100 traiettorie per scenario/baseline,
100 controlli. Stessa lista di scenari. Prima del cambiamento, 19 risultati del
null; dal primo nuovo risultato entra la legge alternativa, continuando dal
contesto precedente. Sono quindi misurati anche ritardi reali dopo il cambio,
non soltanto blocchi già interamente sotto una nuova distribuzione.
Confronto C1 controllato ogni data con C2 su H=20 e H=100. Per null invariato:
probabilità di almeno un allarme nel periodo. Per alternative: frequenza di
rilevazione e mediana del primo giorno solo tra i casi rilevati.

Conferma indipendente, nessuna scelta di score o soglia dopo il test:
seed distinto, N=4.960, 20 baseline; 2.000 episodi/scenario e 100 traiettorie.
Null precedenti più nuovo null lag5, mai usato nella costruzione del candidato.
Scenari: processo invariato e iid; per iid, alternativa sticky0,6; per il ciclo, anche ciclo inverso. Conferma dei
falsi allarmi e dei cambiamenti forti, non una prova su tutti i processi.

Seed sviluppo 20261026; seed conferma 20261027. Un thread/core di calcolo.

## Incertezza e criteri di decisione

Le unità indipendenti per gli intervalli principali sono le 20 baseline.
Riportare media, min/mediana/max e IC t al 95% della media dei tassi fra baseline,
troncato a [0,1]. Non trattare tutti gli episodi come indipendenti dopo aver
condiviso lo stesso riferimento. Gli IC sono approssimati con sole 20 baseline;
non sono una garanzia di copertura universale. Conteggi grezzi conservati.

Criteri dichiarati prima dei risultati, per avanzamento alla prova su dati reali:
- sul campione di conferma, limite superiore IC dei falsi allarmi <=7,5% per
  ciascun null sia C1 puntuale aggregato sia C2 su 20/100 controlli;
- nessuna equivalenza fra assenza di dati sufficienti e test passato;
- nessuna potenza nulla sistematica contro i cambiamenti forti con N=4.960;
- potenza media C1 >=80% in almeno 4 delle 5 famiglie di cambio forte:
  iid->sticky0,6; sticky0,6->iid; ciclo->inverso; lag2->iid; sticky0,9->iid;
- conservare casi lievi e lag10 come stress/limiti, senza riscrivere i criteri.

Se un criterio non passa, dichiarare il limite e distinguere i possibili usi;
non ritoccare soglie per chiamare il candidato promosso. Non si sceglie la
procedura col maggior numero di segnalazioni. Nessuna distribuzione sintetica
rappresenta automaticamente rendimenti, code, volatilità o slippage reali.

## Fonti e autorità

Il protocollo e i generatori dichiarano le ipotesi; le simulazioni misurano
prestazioni, non dimostrano teoremi. Il primo studio documenta C0. Riferimenti
primari in sources.md per massimi multipli, dipendenza e p-value non nulli.
Il candidato non implementa stepdown Romano-Wolf né un test di permutazione
esatto, e non eredita automaticamente le loro garanzie.
