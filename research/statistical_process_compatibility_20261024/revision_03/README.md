# Revisione finale del candidato statistico

Mandato del 24 ottobre 2026: rivedere il candidato; decidere successivamente
l'applicazione. Solo ricerca sintetica. Il cruscotto e i servizi restano invariati.

- [Conclusioni e proposta](conclusioni.md): lettura principale, con esempi per cento gruppi e costo dei falsi allarmi.
- [Notebook leggibile](studio.html) e [notebook eseguito](studio.ipynb).
- [Protocollo prima dei nuovi test](protocol.md), [fonti e confini](sources.md).
- results/assessment.json: criteri predefiniti ed esito, compresi quelli sulla memoria lunga.
- results/validation.json: controlli numerici indipendenti e campioni ricostruiti.

## Percorso conservato

1. Studio originale: il doppio rango può non riconoscere cambiamenti con storici corti.
2. revision_01: score diretto; recupera alcuni casi, fallisce il requisito di sensibilità.
3. revision_02: aggiunge ritardi 1..10; passa i primi criteri, resta debole sui ritardi lunghi.
4. revision_03: frequenze e normalizzazione su dati distinti; nuova conferma con un ulteriore ritardo mai usato prima.

C5 è la misura diagnostica finale. C6 è la regola distinta per un periodo prefissato.
Superare gli esperimenti sui dadi consente una prova su dati reali; non certifica
un comportamento su rendimenti finanziari o un rischio su tutte le date future.

## Riproduzione dalla radice del workspace

Il motore ha già eseguito tutti gli esperimenti e preservato gli input. Per rigenerarli:

```bash
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/revision_03/experiment.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/revision_03/assess.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/revision_03/validate.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/revision_03/write_conclusions.py
```

Per eseguire il notebook di analisi sui risultati congelati:

```bash
JUPYTER_PATH="$PWD/research/statistical_process_compatibility_20261024/jupyter" bt-core/.venv/bin/python -m jupyter nbconvert --execute --to notebook --inplace research/statistical_process_compatibility_20261024/revision_03/studio.ipynb
bt-core/.venv/bin/python -m jupyter nbconvert --to html research/statistical_process_compatibility_20261024/revision_03/studio.ipynb
```

Il notebook esegue analisi, verifiche e figure. REGENERATE_EXPERIMENT=True ne
rigenera anche le simulazioni; il default False legge le evidenze già eseguite.
Jupyter richiede socket locali. Il kernelspec conservato nella cartella jupyter
principale punta al virtualenv bt-core del workspace.

I moduli di R01/R02 vengono riusati in lettura, senza modificarne i risultati.
Un solo core/thread di calcolo; nessun collegamento a broker o servizi. Le versioni
delle librerie e gli hash sono in results/manifest.json. delivery_manifest.json
registra la consegna e il controllo visivo degli output finali.
