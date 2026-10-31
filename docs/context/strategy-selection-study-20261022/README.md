# Studio di scelta fra OvernightAH, FlatComposite e RMA

**Ripresa del 27 ottobre 2026:** verificati indipendentemente i due replay iniziali AH/RMA con trading dal gennaio 2000. Avviato il completamento dei 34 scenari statici/accessori dell'estensione, con massimo due worker. Il nuovo manifest conserva la correzione del pannello Composite senza alterare quello storico. [Avanzamento aggiornato](../../../bt-strategy-test/strategy-selection/study_20261022/extension_20261023/progress_20261027.json), [review dei dati e delle baseline](../../../bt-strategy-test/strategy-selection/study_20261022/extension_20261023/reviews/resume_static_review.md), [checkpoint del Director](director_resume_20261027.md). La fase adattiva e le conclusioni richiedono ancora i rispettivi controlli; nessuna scelta produttiva definitiva deriva dal semplice avvio del batch.

**RICERCA RIAPERTA il 23 ottobre 2026.** I 21 tentativi precedenti e i loro audit sono conclusi, ma non completavano il mandato. È in corso un’estensione correttiva per coprire RMA dal 2000 al 2010, datare tutte le metriche e confrontare calibrazioni lunghe e recenti su gruppi economici rappresentativi. Il cutoff rimane il 21 ottobre 2026; nessuna modifica operativa.

La precedente preferenza per A00 è preliminare. Le lacune sui tre quesiti centrali sono nella [rettifica di adeguatezza](decision_adequacy_review.md); le conclusioni precedenti non valgono come scelta di produzione già risolta. Il capitale del conto resta interamente dedicato al rischio.

## Consegne consultabili

- [Report decisionale finale](final_report.md): scelta, configurazioni, leva, monitoraggio e condizioni per cambiare idea.
- [Sintesi e firma del Research Director](decision_synthesis.md): conclusioni e copertura delle venti questioni.
- [Tabelle numeriche complete](numerical_evidence.md): periodi, costi, capitale, DD/recuperi, breadth e finanziamento.
- [Notebook eseguito](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/study_companion.ipynb), [HTML locale](../../../bt-strategy-test/strategy-selection/study_20261022/analysis/final/study_companion.html) e [procedura riproducibile](../../../bt-strategy-test/strategy-selection/study_20261022/README.md).
- [Chiarimento sul capitale di rischio](user_clarifications.md) e [vincoli di esecuzione](execution_constraints.md).
- [Mappa autorevole dei parametri](parameter_map.md): 65/68/32 parametri effettivi, proprietà, semantica, fonti e lacune; appendici JSON/CSV complete.
- [Registro degli studi](study_register.md): 17 filoni, origine, esiti negativi/correttivi e trasferibilità.
- [Dipendenze e interazioni](interactions.md): meccaniche, confronti controllati, associazioni e ipotesi.
- [Catalogo run](run_catalog.md): 4.012 serie native e riferimenti locali; comparabilità e missing espliciti.
- [Questioni decisionali](research_questions.md): 20 questioni e gate.
- [Audit monitoraggio Watchtower](monitoring_audit.md): capacità verificate nel codice e limiti dei denominatori.
- [Review indipendente](review.md).
- [Protocollo preregistrato](../../../bt-strategy-test/strategy-selection/study_20261022/protocol.md), prevale addendum v0.4.
- [Checkpoint persistente](../../../bt-strategy-test/strategy-selection/study_20261022/state.json) e [processi](../../../bt-strategy-test/strategy-selection/study_20261022/active_processes.json).
- [Registro completo tentativi](../../../bt-strategy-test/strategy-selection/study_20261022/experiment_registry.jsonl).

## Limiti già identificati

Gli universi correnti e i pesi del Composite hanno selezione retrospettiva. Il generatore corrente usa informazioni anteriori al mese, ma ciò non certifica il vintage dei dati o la pubblicazione storica del pannello. Unità ADV e corporate action non sono certificate. La quarantena full-history di simboli oggi stale/anomali aggiunge survivor conditioning: i nuovi risultati saranno condizionati, non una prova di eseguibilità storica.

2023–2026 e luglio2026 sono già osservati, senza holdout intatto dimostrato. Nessun ordine Alpaca, cambio profilo, schedulazione, restart, deploy o promozione automatica è autorizzato.

## Ruoli e disponibilità

Coordinatore: sessione corrente, modello non modificabile tramite delega. Research Director gpt-6-astra high; inventario gpt-6.1-sol high; catalogo gpt-6-luna medium. I limiti di disponibilità hanno richiesto gpt-6-sol high per l'analisi e una prima review, quindi gpt-5.6-sol high per la review conclusiva. Sostituzioni registrate, ammesse dal mandato; ricostruzione indipendente dall'implementatore. Il Director originario ha firmato la sintesi finale.
