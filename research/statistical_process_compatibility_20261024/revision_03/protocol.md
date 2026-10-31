# Revisione 03 — normalizzazione fuori dai dati usati per stimare le frequenze

Data: 24 ottobre 2026. Scope invariato: solo studio statistico, applicazione da
decidere separatamente. R01 e R02 restano archiviate; nessun loro risultato è
riclassificato come una nuova conferma.

## Perché un'ulteriore revisione

C3 ha passato i criteri dichiarati di R02 sui cinque cambiamenti forti, ma la
rimozione della memoria a lag5/7/10 è riconosciuta solo nel 53,5%/41,8%/10,4% dei
blocchi di conferma. La stima di parole da 216 categorie e la normalizzazione
dei loro scarti sugli stessi 1.000 dati può rendere quelle componenti troppo
ottimistiche nel training e dominanti sui dati successivi. Il livello finale
può restare calibrato grazie ai blocchi esterni, pur perdendo potenza.

## Candidato fissato prima di nuove simulazioni

C5: stesso insieme di componenti e ritardi 1..10 di C3. Cambia SOLO la costruzione
del riferimento nel primo segmento di 1.000 dati: primi 500 per stimare le
frequenze di parole 1/2/3 (pseudoconteggio 0,5), successivi 500 per stimare media
e deviazione standard di ogni componente sulle finestre mobili. Il modello
non viene poi rifittato su tutti i 1.000 dati. Nessuna normalizzazione delle
frequenze sui dati con cui quelle stesse frequenze erano state apprese.
Scala minima 0,25, score massimo degli scarti assoluti e calibrazione finale
singola restano invariati. Una separazione cronologica non prova indipendenza
sotto memoria arbitraria: la validazione continua a essere empirica.

C6: score C5 e calibrazione del massimo dell'intero periodo prefissato di
20 o 100 controlli, come C4. Non è un e-process né una garanzia su ogni data futura.

Confronto appaiato con C0, C1 e C3, stessi nuovi storici ed episodi.
Stessi N=2.480/4.960/9.200, sviluppo 20 baseline/null e 1.000 episodi/scenario;
monitoraggio a N=4.960 con 100 traiettorie di 100 controlli per scenario/baseline.
Seed sviluppo 20261030, casi e sei null di R01/R02.

Conferma seed 20261031, N=4.960, 20 baseline/null e 2.000 episodi/scenario; 100
traiettorie di 100 controlli. Null: iid, sticky0,6, sticky0,9, ciclo, lag2, lag5,
lag7, lag10 e NUOVO lag8 (mai visto prima). Scenari: null invariato e iid;
per iid alternativa sticky0,6; per ciclo anche ciclo inverso. Nessun tuning
successivo sui risultati di questa conferma.

## Criteri

Restano i quattro criteri di R02: limite superiore IC dei falsi allarmi <=7,5%
per tutti i null (puntuale, periodo20, periodo100), riconoscimento >=80% in
almeno 4/5 cambiamenti forti prefissati, nessun zero sistematico in quei casi.
Si aggiunge: riconoscimento medio puntuale di rimozione della memoria a lag5,
lag7 e lag10 almeno 50% per ciascun caso. Si misura anche il nuovo lag8 senza
assumere copertura di una classe di memoria sconosciuta. Il 50% è criterio di
utilità minima nello studio, non un'affermazione di potenza garantita.

Intervalli principali: IC t fra 20 baseline, approssimati; min/mediana/max e
conteggi grezzi sono conservati. Se tutti i tassi campionari sono zero o uno,
l'IC t può collassare: non certifica probabilità esatta zero/uno. I criteri
sui falsi allarmi sono valutati lontano da tali confini.

## Riproduzione e confini

Riuso in sola lettura del motore R01 e della costruzione C3 in R02. Nuove
scritture solo in revision_03. Un core di calcolo. Tutti gli input, confronti,
criteri falliti e versioni restano ispezionabili. I periodi sono prefissati,
non rinnovabili implicitamente. Le categorie dei dadi non sostituiscono
rendimenti continui: valori estremi entro un bin richiedono componenti numeriche
specifiche prima di ogni futura applicazione finanziaria. Nessuna modifica UI.
