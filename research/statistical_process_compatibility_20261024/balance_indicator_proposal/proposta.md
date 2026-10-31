# Proposta: bilanciamento dei rendimenti e persistenza

24 ottobre 2026. Ragionamento sui rendimenti giornalieri del portafoglio delle
strategie, con approfondimento descrittivo sui file finanziari congelati. Nessuna
modifica all'applicazione e nessuna nuova simulazione di dadi o monete.

## Domanda e giudizio

La classificazione rispetto a zero e alla media storica è adatta a costruire un
indicatore mirato: verificare se cambia la frequenza delle perdite, la frequenza
dei risultati sotto la media e la loro persistenza. È un complemento al confronto
continuo, perché la classe non distingue la dimensione economica delle perdite.
Non permette da sola di identificare ogni cambiamento nella legge dei rendimenti.

## Definizioni proposte

Usare la stessa variabile scelta per l'altro indicatore: rendimento giornaliero
del portafoglio, secondo la contabilità del backtest. La media di riferimento è
aritmetica, stimata esclusivamente sulla baseline precedente al periodo recente,
poi mantenuta fissa. La media non è la mediana: la frequenza sotto media viene
misurata nello storico, senza porla al 50%.

Con media storica positiva, rendere esplicite quattro condizioni:

1. rendimento negativo;
2. rendimento esattamente zero;
3. rendimento positivo ma inferiore alla media;
4. rendimento uguale o superiore alla media.

L'uguaglianza con la media appartiene all'ultimo gruppo. Per media zero o negativa,
usare direttamente due indicatori binari paralleli, r<0 e r<media, oltre alla
frequenza degli zeri: evita categorie impossibili o significati invertiti. Anche
con media positiva i due indicatori sono legati: ogni perdita è anche sotto media.
Non si moltiplicano probabilità di due confronti supposti indipendenti.

Gli zeri non vengono rimossi o chiamati vittorie. Una giornata con rendimento
mancante resta mancante. Un valore zero osservato non dimostra da solo assenza
di posizioni: la causa richiede dati di esposizione separati.

## Che cosa misurare sulle ultime 3/5/10/15/20 sedute

- Bilanciamento: numero di giornate negative, positive, nulle e sotto media;
  confronto con i conteggi di periodi storici della stessa durata.
- Persistenza: massima e attuale sequenza di perdite e di giornate sotto media;
  confronto con le lunghezze storiche. Per una sequenza attuale che precede la
  finestra, distinguere lunghezza dentro la finestra e durata complessiva.
- Raggruppamento: quanto spesso una perdita segue una perdita, e un risultato
  sotto media ne segue un altro. Frequenza totale e dipendenza possono cambiare
  separatamente, quindi il win rate da solo non basta.

Esempio di restituzione comprensibile: «12 giornate su 20 sotto media;
sequenza massima di 7; nello storico, in periodi di 20 giorni, …».
I puntini devono essere riempiti con confronti effettivamente calcolati, non con
una probabilità binomiale che presuppone indipendenza.

## Riscontro finanziario già eseguito

Sulla baseline OvernightAH 113, limitata al riferimento 2000–2018 (4.779 sedute):

- Media giornaliera del portafoglio: circa +0,1232%.
- Giornate negative: 34,11%; positive: 47,75%; a zero: 18,14%.
- Giornate sotto la media storica: 57,12%, quindi non il 50%.
- Massima sequenza sotto media: 49 sedute, dal 27 febbraio al 7 maggio 2009;
  contiene 47 zeri e 2 giornate negative. Non sono 49 perdite.
- Finestre di 20 sedute interamente sotto media: 63 su 4.760 finestre storiche
  osservate, pari all'1,3235%. Le finestre si sovrappongono: non sono 63 episodi
  indipendenti. Il calcolo che imponesse indipendenza darebbe circa 0,00137%.
  La differenza descrive quanto sarebbe fuorviante quella scorciatoia su questo
  storico; non dimostra una probabilità futura dell'1,3235%.

FlatComposite 108 è incluso come controllo nel notebook e nei risultati.
La soglia viene qui stimata sul medesimo riferimento storico per una descrizione
retrospettiva; queste cifre non sono una validazione fuori campione del candidato.
Input, hash e verifiche dei conteggi sono in results.json.

## Come valutare il candidato prima di assegnargli un semaforo

Usare blocchi consecutivi realmente prodotti dalla strategia, conservando le
dipendenze, con riferimento precedente ai periodi valutati. Confrontare insieme
conteggi, sequenze e transizioni; tener conto dei confronti su entrambe le soglie,
sulle cinque finestre e dei controlli ripetuti. Non scegliere cinque test al 5%
separati e chiamare il loro insieme un test al 5%.

Validare separatamente cambiamento di frequenza e di dipendenza mantenendo, per
quanto possibile, invariato l'altro aspetto. Per esempio, riordini vincolati dello
stesso insieme di rendimenti mantengono le frequenze ma alterano l'ordine; periodi
di backtest scelti indipendentemente dal punteggio possono verificare la
portabilità. Questi controlli non sono ancora stati eseguiti per questo candidato.

Una sintesi potrebbe distinguere «bilanciamento abituale», «più perdite del solito»,
«più giornate sotto media», «persistenza insolita» e «dati insufficienti».
Un cambiamento favorevole va distinto da un deterioramento. Il segnale rimane
evidenza di uno scostamento dal riferimento; non è la probabilità che il processo
sia cambiato e non identifica la causa. Le soglie finali richiedono calibrazione,
non si ereditano automaticamente il 5% e il 20% descrittivi dell'altro indicatore.

## Collegamento alla statistica esistente

Il tema è quello dei conteggi di segni rispetto a soglie e delle sequenze
consecutive. Il [runs test classico descritto dal NIST](https://www.itl.nist.gov/div898/handbook/eda/section3/eda35d.htm)
verifica un'ipotesi di ordine casuale. Qui la domanda è diversa: mantenimento
della dipendenza storica, che può già contenere raggruppamenti. Non si applica
automaticamente quel test o un test binomiale ai rendimenti della strategia.

**Decisione proposta:** approfondire questo indicatore mirato, mantenendolo
separato da quello sull'ampiezza. La semplificazione rende il significato più
leggibile; l'affidabilità e la sensibilità restano da misurare sui dati finanziari.
