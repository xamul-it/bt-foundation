# Studio di scelta fra OvernightAH, FlatComposite e RMA

Avviato il 22 ottobre e completato il 23 ottobre 2026. **21 tentativi nativi conclusi, review indipendente e firma scientifica completate.** Cutoff invariato al 21 ottobre. Nessuna modifica operativa.

La proposta è OvernightAH A00 stabile come riferimento prospettico condizionato ai dati e ai costi, con tutto il conto dedicato al rischio e target 1x. Non emerge un vincitore universale: costi elevati cambiano il confronto con RMA; né switch automatico né target 2x sono sostenuti nei confronti eseguiti. La metà del budget è un compromesso di rischio, senza riserva personale presunta.

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
