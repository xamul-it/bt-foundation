# Prompt di avvio per Codex GPT-5.6 Sol

Copia il testo seguente nella sessione che lavorerà su `/home/htpc/backtrader`.

```text
Implementa il piano docs/superpowers/plans/2026-10-15-persistent-calculation-jobs.md.
Leggilo completamente e controlla gli AGENTS.md applicabili e lo stato dei repository.
L'obiettivo è aggiungere esecuzioni persistenti nello scheduler per creazione baseline
e ricalcolo delle ultime 25 giornate dalla baseline selezionata, con pulsante e
monitoraggio recuperabile nelle pagine Watchtower, Baseline Manager e Scheduler.

Segui le decisioni del piano, procedi per fasi A-E e aggiorna la checklist.
Lavora con un solo agente. Conserva le modifiche preesistenti e non creare commit.
Non cambiare parametri dei profili, cron trading o checkout backtrader-prod.
Non avviare bonifiche reali sul DB condiviso durante i test; usa fixture/mock o DB isolato.
Completa implementazione, test e build PWA online verificando la destinazione effettiva.
I nomi storici degli script/unità contenenti prod non identificano da soli il target.
Non fermarti a proporre un altro piano. Chiedi solo se trovi un blocco concreto che
richiede una decisione assente dal documento; altrimenti procedi fino alla consegna.
Riporta risultati verificati, file cambiati, migrazione e limiti rimasti.
```

Impostazione consigliata: GPT-5.6 Sol, ragionamento High, modalità di implementazione
(Default/Code, non solo Plan), workspace locale indicato sopra. Questa è una scelta
operativa per il task, non una garanzia di costo o disponibilità sul proprio account.
Per risparmiare contesto, fornire questi due documenti anziché l'intera conversazione.
