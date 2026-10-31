# Revisione 02 — addendum e nuovo test prima dell'esecuzione

24 ottobre 2026. La revisione 01 è archiviata senza modifiche: C1 ha corretto
saturazione e spreco dello storico, ma NON ha passato il requisito di potenza
almeno 80% in 4 delle 5 famiglie forti (solo 3/5). La rimozione di una dipendenza
a lag 2 è riconosciuta nel 34,22% dei blocchi; quella a lag5/10 quasi mai.
I risultati di conferma di R01 sono ora dati di sviluppo, non vengono riutilizzati
come nuova conferma. Nessuna applicazione viene aggiornata.

## Revisione fissata prima di nuovi risultati

C3 conserva le quattro componenti raw di C1 e aggiunge il NUMERO di uguaglianze
fra valori a distanza 1,2,...,10, per tutti i ritardi inferiori alla lunghezza
della finestra. Dieci è metà della finestra massima 20, scelta di scala, non
una legge magnetica nota. Le componenti restano conteggi e mantengono la scala
minima 0,25; non si divide una proporzione per una scala inadatta. Ogni componente
è normalizzata usando soltanto training. Non viene assunta indipendenza dei
confronti a ritardi diversi. Il massimo su componenti e finestre è calibrato
una sola volta sui blocchi cronologici di calibrazione.

C4 usa lo score C3, calibrato sul massimo di un periodo completo lungo 20 o 100
controlli. Restano tutte le definizioni, gli split, le formule e gli obiettivi
di calibrazione dichiarati in revision_01/protocol.md. Nessun budget si rinnova
implicitamente dopo la fine del periodo.

Si confrontano C0 (doppio rango originario), C1 (raw senza ritardi espliciti) e
C3 sullo stesso nuovo storico e sugli stessi episodi. Il monitoraggio confronta
C3 puntuale ogni giorno e C4 sul periodo, preservando il contesto prima del cambio.

Sviluppo seed 20261028: stessi N=2.480,4.960,9.200, sei null e scenari di R01,
20 baseline per combinazione, 1.000 episodi/scenario; per N=4.960 anche 100
traiettorie di 100 controlli. Tutti i risultati saranno conservati.

Conferma seed 20261029: N=4.960, 20 baseline/null, 2.000 episodi/scenario e 100
traiettorie. Null: i sei precedenti, lag5, e NUOVO lag7, mai osservato nella
revisione 01. Scenari: processo invariato e iid; per iid alternativa sticky0,6;
per il ciclo anche ciclo inverso. Nessun altro tuning dopo questi risultati.

## Criteri invariati

- Su conferma, limite superiore IC 95% dei falsi allarmi <=7,5% per ogni null:
  C3 puntuale aggregato, C4 su 20 controlli e C4 su 100 controlli.
- Nessuna potenza zero sistematica contro i cambiamenti forti a N=4.960.
- Potenza media C3 >=80% in almeno 4/5 famiglie: iid->sticky0,6;
  sticky0,6->iid; ciclo->inverso; lag2->iid; sticky0,9->iid.
- Nessuna interpretazione di dati insufficienti come stabilità; nessun passaggio
  automatico da test sintetico ad applicazione a rendimenti reali.

Intervalli principali: IC t tra le 20 baseline, con le limitazioni di R01.
L'80% e il 7,5% sono criteri di avanzamento dello studio, non teoremi di potenza
minima o una ridefinizione del livello nominale 5%. Casi lievi, lunga memoria,
risoluzione finita e variabilità fra baseline restano nella decisione.

## Confini

Il candidato studia l'ordine fino alla finestra/ritardi osservati. Non garantisce
rilevazione di qualsiasi dipendenza, non calcola P(stesso dado | dati), non
rappresenta automaticamente i rendimenti. La futura scelta delle variabili
finanziarie e dell'eventuale discretizzazione richiede una verifica separata;
un crollo entro una categoria non può essere dichiarato innocuo perché il codice
categorico è lo stesso. Nessun ordine, servizio o parametro operativo modificato.
