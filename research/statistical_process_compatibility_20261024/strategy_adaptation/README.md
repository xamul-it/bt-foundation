# Adattabilità ai rendimenti finanziari

Estensione del 24 ottobre 2026, successiva a R03. Nessuna modifica al motore,
alle API, al dashboard o ai profili; versioni precedenti conservate.

- [Conclusioni e significato dei numeri](conclusioni.md)
- [Notebook leggibile](studio.html)
- [Notebook eseguito](studio.ipynb)
- [Protocollo](protocol.md)
- [Fonti](sources.md)

Esito: architettura riutilizzabile, candidato categoriale da sostituire come
misura finanziaria unica. Anche il continuo non supera tutti i criteri, in
particolare dispersione x2. Applicazione non pronta.

Input: baseline Watchtower 113 OvernightAH e 108 FlatComposite; 6.539 rendimenti
giornalieri per strategia, 2000–2025, esclusa la barra extra del 2 gennaio 2026.

## Riproduzione

Da /home/htpc/backtrader, usando i file congelati:

```bash
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/strategy_adaptation/analyze.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/strategy_adaptation/validate.py
MPLCONFIGDIR=/tmp/statistical-study-mpl bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/strategy_adaptation/figures.py
bt-core/.venv/bin/python research/statistical_process_compatibility_20261024/strategy_adaptation/write_notes.py
JUPYTER_PATH=/home/htpc/backtrader/research/statistical_process_compatibility_20261024/jupyter bt-core/.venv/bin/python -m jupyter nbconvert --execute --to notebook --inplace research/statistical_process_compatibility_20261024/strategy_adaptation/studio.ipynb --ExecutePreprocessor.kernel_name=statistical-study
bt-core/.venv/bin/python -m jupyter nbconvert --to html research/statistical_process_compatibility_20261024/strategy_adaptation/studio.ipynb
```

Il notebook legge i risultati per default. REGENERATE_EXPERIMENT=True riesegue
il calcolo prima della verifica. Jupyter richiede socket locali.
freeze_inputs.py documenta l'estrazione originale: non serve rieseguirla per
riprodurre lo studio e non va usata per sostituire input congelati con dati nuovi.

results/analysis_manifest.json: versioni, semi e hash del calcolo;
results/validation.json: controlli completi e campionati;
delivery_manifest.json: artefatti finali e verifica del notebook.
