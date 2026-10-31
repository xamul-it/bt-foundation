# Adattabilità ai rendimenti delle strategie — 24 ottobre 2026

## Decisione

Stabilire quali parti dello studio precedente si possono riusare per confrontare
una successione recente di rendimenti finanziari con la storia della strategia.
La consegna è un'estensione dello studio, con notebook eseguito e conclusioni.
Nessun aggiornamento dell'applicazione, nessun ordine e nessuna nuova ottimizzazione
economica delle strategie. Le simulazioni del dado restano un controllo concettuale.

## Fonti e significato

Fonti principali: baseline Watchtower 113, profilo live, OvernightAH,
`baseline_live_e1eb0e34`, e baseline 108, challenger, FlatComposite,
`baseline_challenger_8745d633`. Il database locale viene interrogato solo in lettura.
Si leggono gli artefatti nativi dei rispettivi run; le serie congelate vengono
salvate qui. Il profilo live identifica una baseline di backtest, non esecuzioni
reali del broker. Non si presume che i parametri del 2026 fossero noti nel 2000.

Serie primaria: rendimento giornaliero del portafoglio di `returns.csv`, prodotto
dal PyFolio analyzer del motore (contabilità ed esposizione effettivamente simulate).
Trasformazione interna log(1+r); le quantità economiche vengono esposte in %.
Serie di controllo: PnL netto dei trade chiusi nel giorno / somma dei nozionali
di ingresso degli stessi trade. Si conserva anche l'equivalente lordo usato
dalla riconciliazione. Un giorno senza chiusure ha numerosità zero: il suo rapporto
è mancante, non un rendimento osservato di zero. Non si forza l'equivalenza fra
questa serie e la variazione del NAV.

Riscontro nella verifica degli input, prima delle conclusioni: i run dichiarano
`commission=alpaca`, ma PnL lordo e `pnlcomm` coincidono per tutti i trade conservati.
Si riportano quindi rendimenti secondo la contabilità di questi backtest, senza
attribuire loro costi reali del broker o costi di negoziazione non registrati.

Controlli preliminari: date, duplicati, valori non finiti, rendimenti <= -100%,
giorni iniziali piatti, copertura, eventuali barre successive alla data finale
dichiarata e riconciliazione del rendimento composto col valore finale nativo.
Si escludono le barre oltre il `window_end` registrato, dichiarando l'effetto.

## Confronto fissato prima degli esperimenti

Riferimento: 2000–2004 per stime iniziali; 2005–2009 per normalizzare le statistiche;
2010–2018 per calibrare il confronto. Valutazione cronologica: 2019–2025,
con gli ultimi 19 giorni del riferimento utilizzabili esclusivamente come contesto.
Finestre richieste: 3, 5, 10, 15, 20 sedute. Soglia esplorativa: 5%.

Si confrontano:

1. **Media iid giornaliera**, controllo con ipotesi esplicita di indipendenza.
   Non è la riproduzione esatta della UI, che usa la media dei singoli trade.
   Si ricostruisce anche **UI trade iid** con i trade delle ultime N sedute:
   media e deviazione standard di riferimento precedenti al 2019, errore standard
   diviso per la radice del numero di trade recenti. Questo conserva il significato
   della UI rendendo esplicito il raggruppamento per giornata.
2. **C5-Q6**, trasferimento della statistica del dado a sei categorie definite dai
   quantili dei rendimenti nel periodo iniziale. Riusa parole di lunghezza 1/2/3,
   ripetizioni a distanza 1..10 e massima sequenza della stessa categoria.
   Le date dei tre periodi sono quelle qui fissate, non i conteggi 500/500 di R03.
3. **F1 continuo**, stessa architettura con statistiche finanziarie: rendimento
   cumulato, energia quadratica, peggior/miglior rendimento, drawdown interno,
   frequenza e massima sequenza di perdite; prodotti a ritardo dei rendimenti
   centrati e delle loro ampiezze, a tutte le distanze osservabili nella finestra.
   Il limite è quindi 19 sedute con finestre di 20, non un'assunzione sulla memoria.
   Le componenti si normalizzano esclusivamente sul 2005–2009. Si calibra una sola
   volta il massimo delle componenti e, separatamente, il massimo delle cinque
   finestre. Un punteggio massimo non identifica tutte le leggi congiunte possibili.
   I rendimenti sono espressi in deviazioni standard giornaliere del periodo
   iniziale; pavimento della scala di normalizzazione F1 pari a 0,05, fissato
   prima dei risultati. Le autocovarianze usano media iniziale e ampiezze centrate
   sulla media iniziale di |r-media|; non si assegna significato gaussiano ai punteggi.

Calibrazione storica: gruppi consecutivi non sovrapposti di 20 sedute.
Rango conservativo `(1 + conteggi riferimento >= osservato)/(B+1)`.
Non sovrapposizione non implica indipendenza; nessuna garanzia di esattezza per
una storia finanziaria arbitraria. La percentuale è una misura di anomalia,
non la probabilità che la strategia sia rimasta invariata.

## Esperimenti controllati su blocchi di rendimenti reali

Si campionano con reinserimento interi blocchi di 20 sedute del 2010–2018:
la distribuzione nulla costruita è quella empirica dei blocchi, non una legge
finanziaria dimostrata. Si mantengono i valori e l'ordine dentro ogni blocco.
50 banche di riferimento da 200 blocchi; 2.000 blocchi valutati per banca,
semi 20261101 per OvernightAH, 20261102 per FlatComposite.
Le banche sono repliche Monte Carlo condizionate alla medesima storia finita;
non equivalgono a 50 storie finanziarie indipendenti. Intervalli fra banche
misurano solo l'incertezza della simulazione condizionata.

Scenari fissati: invariato; permutazione interna; inversione; ordinamento crescente
(stesso insieme di rendimenti, ordine diverso); sottrazione giornaliera di
0,25/0,5/1 deviazioni standard iniziali; dispersione moltiplicata per 1,5 e 2;
ultima perdita imposta a -3 e -6 deviazioni standard iniziali.
Gli scenari alterati sono esperimenti, non risultati realmente prodotti da una
strategia diversa. Non si ritoccano componenti o soglie in base ai loro esiti.

Criteri diagnostici: il punteggio continuo deve conservare l'ampiezza oltre i
quantili e deve dipendere dall'ordine; il tasso medio di allarmi sotto la legge
empirica costruita deve essere fra 2,5% e 7,5%; riconoscimento >=80% per lo
spostamento di -1 deviazione standard e >=50% per dispersione x2. Il mancato
superamento resta un risultato e impedisce di presentare il candidato come validato.

## Cronologia reale e controlli ripetuti

Sul 2019–2025 si riportano i segnali per anno e finestra, senza chiamarli falsi
allarmi: non conosciamo i periodi nei quali la legge vera è rimasta invariata.
Si verifica anche la sensibilità a un riferimento mobile precedente di 5 anni,
con 252 giorni per stime, 252 per normalizzazione, 20 di separazione e il resto
per calibrazione. Ricalcolo al primo giorno di ogni anno, poi riferimento fisso
per quell'anno. Questa è sensibilità della baseline, non selezione del vincitore.

Per il controllo di periodi di 20 o 100 sedute, si misura la capacità dello storico:
massimo su intervalli lunghi H+19, passo H+40. Se il rango minimo supera il 5%,
il livello richiesto è non stimabile, senza creare evidenza tramite duplicazioni.
Nessun rinnovo automatico della protezione fra periodi e nessuna garanzia perpetua.

## Limiti decisionali

Si distinguono fattibilità matematica, verifica controllata e affidabilità su
future osservazioni reali. Un allarme non separa regime, perdita dell'edge,
esposizione, selezione dei titoli, dati o costi. La conclusione deve dire quali
parti riusare, quali sostituire e che cosa manca prima dell'applicazione.
