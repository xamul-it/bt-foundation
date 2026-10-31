# Studio statistico della compatibilità temporale

24 ottobre 2026. Mandato: studiare se una sequenza consecutiva recente è compatibile
con il processo storico, senza assumere osservazioni indipendenti. Solo ricerca
sintetica: nessun calcolo applicativo, servizio o dato operativo modificato.

Aprire [studio_statistico.md](studio_statistico.md) per la decisione e i limiti,
[studio.html](studio.html) per il notebook leggibile, oppure
[studio.ipynb](studio.ipynb) per il notebook eseguito e modificabile.

Il confronto principale è dichiarato in protocol.md e nell'addendum preventivo.
L'estensione con storici corti è dichiarata separatamente in
finite_history_extension.md; ha cambiato la conclusione sul candidato specifico:
la sua aggregazione empirica può avere potenza zero con storici più corti e
non è raccomandata per il trasferimento diretto nell'applicazione.

## Riproduzione dalla radice /home/htpc/backtrader

Con l'ambiente esistente bt-core/.venv:

```bash
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/experiment.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/finite_history.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/validate_study.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/write_study.py
```

Per eseguire il notebook dall'inizio, inclusi gli esperimenti e le figure:

```bash
JUPYTER_PATH="$PWD/research/statistical_process_compatibility_20261024/jupyter" bt-core/.venv/bin/python -m jupyter nbconvert --execute --to notebook --inplace research/statistical_process_compatibility_20261024/studio.ipynb
bt-core/.venv/bin/python -m jupyter nbconvert --to html research/statistical_process_compatibility_20261024/studio.ipynb
```

Jupyter richiede socket locali per il kernel. Il kernelspec incluso punta al
virtualenv del workspace; su un'altra macchina occorre cambiarne argv[0].
Pacchetti e versioni dell'esperimento sono in results/manifest.json. Le librerie
Jupyter, Matplotlib e le relative dipendenze devono essere presenti per notebook
ed export; l'analisi tabellare richiede NumPy, SciPy e pandas.

experiment.py limita il calcolo a un core e BLAS a un thread. I risultati sono
rigenerati solo dentro questa cartella. Non importa server, broker o codice di
produzione. Lo snapshot JavaScript serve a documentare il confronto attuale.

## Evidenza

- inputs/: sequenze storiche e blocchi recenti sintetici, conservati per audit.
- results/: tutte le frequenze di rifiuto, intervalli Monte Carlo, sensibilità,
  manifest e controlli indipendenti. Tutti gli intervalli Wilson sono condizionati
  alla baseline stimata; gli estremi fra baseline sono descrittivi.
- figures/: quattro figure esportate PNG e SVG; le stesse sono nel notebook.
- sources.md: letteratura primaria, uso nel lavoro e limiti di trasferibilità.
- results/validation.json: validazione numerica del notebook.
- delivery_manifest.json: hash dei file finali dopo esecuzione e controllo visivo.

Non è stato sperimentato MMD né un metodo conforme con teorema di copertura.
Non è stata calcolata una probabilità di permanenza di una strategia reale.
