# Addendum prima di qualsiasi esecuzione sperimentale

Durante il disegno, senza osservare risultati, emerge un caso cieco prevedibile:
le quattro caratteristiche simmetriche non distinguono un ciclo 1→2→…→6 da
quello inverso. Entrambi hanno uguali ripetizioni a lag 1/2 e run massima.
Si aggiunge una quinta procedura locale, ensemble di quelle quattro componenti
più sorpresa Markov bilaterale, ricalibrata su B. Lo score globale sulle cinque
finestre usa questo ensemble. Il confronto a quattro componenti resta nei
risultati, per rendere visibile il suo limite. Nessuna statistica viene scelta
sui dati di valutazione. Restano invariati seed, fonti, soglie e split.
