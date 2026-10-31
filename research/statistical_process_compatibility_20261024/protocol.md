# Protocollo statistico congelato prima delle simulazioni

Data di disegno: 24 ottobre 2026. Mandato: studiare se una sequenza consecutiva
recente sia coerente con il processo storico, anche con dipendenze ignote.
Nessuna modifica applicativa, deployment, ordine o simulazione economica.

## Decisione e risultato richiesto

Scegliere una famiglia di procedure statistiche da approfondire per il defect
bt-dash#11, indicando che cosa misura, quando è affidabile e quali cambiamenti
non vede. Una percentuale di anomalia non è P(stesso processo | dati).
Si consegnano sintesi in chat, note statistiche locali e notebook riproducibile.
Non si decide se una strategia reale abbia perso il proprio vantaggio.

## Fonti e autorità

- Mandato dell'utente in questa conversazione: definisce il problema e lo scope.
- Letteratura primaria consultata: dipendenza, blocchi, test a kernel, controllo
  sequenziale; riferimenti e limiti in sources.md.
- Snapshot del calcolo attuale: documenta esclusivamente il test di media corrente.
- Generatori sintetici dichiarati: unica fonte dei numeri sperimentali.
  Nessun risultato sintetico sarà presentato come risultato di strategie reali.

## Ipotesi

H0: legge congiunta dei blocchi recenti uguale alla legge storica alla stessa
scala e con lo stesso condizionamento. La stazionarietà/mescolamento di riferimento
è un'assunzione, non una conseguenza del mantenere la stessa strategia.
Due leggi possono avere uguale marginale ma diversa legge temporale.
L'identificazione di una legge arbitraria da una storia finita è impossibile:
servono vincoli e si può avere solo evidenza contro differenze osservabili.

## Esperimento preregistrato

Seed radice 20261024, finestre 3, 5, 10, 15, 20, soglia nominale 5%.
Un processo seriale per calibrazione, episodi indipendenti per valutazione.
Ogni null principale: training 20.000 osservazioni, pausa 200; calibrazione A
60.000 osservazioni, pausa 200; calibrazione B 60.000 osservazioni. Nelle due
calibrazioni si prendono 1.000 blocchi di 20 osservazioni con passo 60: almeno
40 osservazioni di separazione. Le cinque finestre sono suffix dello stesso
blocco. Calibrazioni separate per modello e aggregazione; nessun dato di test
usato per stimare o scegliere soglie. La spaziatura non garantisce indipendenza
esatta; questa calibrazione empirica non viene chiamata conformal esatta.

Null: iid uniforme; sticky con rho=0,6; ciclo +1 con intensità 0,8;
dipendenza a lag 2 con intensità 0,8 e coppie adiacenti marginalmente iid.
Tutte le marginali sono esattamente uniformi per costruzione e inizializzazione.
Alternative: iid, sticky 0,6, sticky 0,9, ciclo +1 0,8, ciclo -1 0,8,
lag 2 0,8, iid con probabilità (0,1;0,1;0,1;0,1;0,1;0,5).
10.000 episodi indipendenti di 20 lanci per scenario; le righe corrispondenti
allo stesso null misurano falsi allarmi, le altre potenza per quello scenario.
Le alternative sono episodi stazionari: non misurano automaticamente il ritardo
subito dopo una sostituzione. Gli stessi episodi sono usati tra metodi, quindi
le differenze fra metodi sono appaiate e non indipendenti.

Metodi:
1. Z della media con errore standard iid, analogo al calcolo applicativo.
2. Scarto dell'istogramma marginale rispetto al training, rango empirico bilaterale
   rispetto a blocchi storici consecutivi; ignora l'ordine dentro il blocco.
3. Sorpresa predittiva Markov di ordine 1 (Laplace 0,5), calibrata bilateralmente
   su blocchi storici. Si confronta il comportamento del modello, non lo si
   assume vero. Una misura unilaterale perderebbe cambiamenti verso maggiore
   prevedibilità; anche due code possono restare cieche se lo score è inadeguato.
4. Score combinato su: scarto istogramma, quota ripetizioni a lag 1,
   quota ripetizioni a lag 2, massima serie identica. Le prime tre componenti
   sono bilaterali, l'ultima superiore. Rangi da A, minimo p delle componenti;
   aggregato ricalibrato su B. Non è un test universale di tutta la legge.
5. Score combinato anche sulle cinque finestre allo stesso istante; calibrazione
   B con la stessa aggregazione. Controllo sperimentale per un singolo istante,
   non per osservazioni ripetute ogni giorno.

Intervalli Wilson 95% per frequenze Monte Carlo su episodi indipendenti:
condizionati allo storico e alla calibrazione congelati; non incorporano tutta
l'incertezza di stimare una nuova baseline.

## Robustezza prevista

- Probabilità analitiche di dieci facce identiche e di dieci sei; tutti i percorsi
  iid uniformi hanno uguale probabilità esatta, quindi serve un criterio d'anomalia.
- Confronto costante a stessa composizione marginale ma ordine differente.
- Permutazione delle etichette del dado: uno Z sulle cifre non è invariante,
  mentre ripetizioni/istogramma/Markov a relabeling coerente lo sono.
- Variazione della storia di calibrazione: 20 repliche indipendenti di una
  baseline sticky, calibrazioni ridotte 300 blocchi ciascuna, 2.000 null recenti
  per replica; stimare variabilità fra baseline, non solo errore Monte Carlo.
- Storie finite: conteggio blocchi e minima risoluzione p = 1/(B+1), limiti delle
  finestre sovrapposte. Sensibilità della statistica media a bootstrap iid,
  stationary bootstrap con lunghezze medie 2, 5, 10, 20, 50, e blocchi veri.
- Controllo ripetuto: 1.000 traiettorie null di 100 istanti consecutivi;
  misurare probabilità di almeno un allarme con soglia puntuale 5% e di più
  finestre senza/con aggregazione. Nessuna promessa di garanzia anytime.
- Studio condizionale del confine: confronto di una prima faccia fissata rispetto
  al risultato appena precedente. La prova generale resta incondizionata;
  condizionare su memoria ignota richiede un ulteriore modello e validazione.

## Criteri di giudizio

Una procedura con molti falsi allarmi non vince perché ha più potenza.
Confrontare null, alternative a marginali uguali e caso di memoria mal specificata.
Non scegliere una soglia dopo aver visto il test. Registrare anche test senza
potenza. Nessuna promozione alla produzione senza validazione cronologica su
serie reali comparabili, controllo del monitoraggio ripetuto e audit dei dati.

## Risorse e riproduzione

Un processo Python, BLAS/OMP a un thread, almeno due core lasciati disponibili.
Registrare versioni, seed, hash degli input, durata e memoria. Notebook da eseguire
interamente; figure native Matplotlib, esportate e ispezionate.
