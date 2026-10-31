# Fonti e limiti

## Dati e definizioni

- inputs/OvernightAH/baseline.json: baseline Watchtower 113; FlatComposite: 108.
- results/source_manifest.json: percorsi originali e risolti, hash, commit
  produttore, commissioni configurate e leva. Query SELECT in sola lettura.
- bt-core/btmain.py: scrittura returns.csv dalla serie PyFolio del portafoglio.
- bt-core/profile_baseline.py, extract_baseline_inputs: PnL % per singolo trade.
- bt-dash/src/bt/utils/baselineCompatibility.js, compatibilityLabel: test normale
  della media con numerosità pari al numero di trade recenti.
- bt-core/watchtower_runtime.py, reconciliation_statistics e compact: aggregazione
  giornaliera e distinzione fra trade chiusi e osservazioni mancanti.
  Qui i dati sono dei backtest, senza chiamate alle API del broker.

## Letteratura primaria consultata

- Politis e Romano, The Stationary Bootstrap (1994),
  https://doi.org/10.1080/01621459.1994.10476870.
  Ricampionamento di blocchi e dipendenza locale richiedono ipotesi specifiche;
  non dimostrano stazionarietà di una strategia su 26 anni. Qui non si implementa
  lo stationary bootstrap dell'articolo: si campionano blocchi completi di 20
  sedute per un esperimento empirico esplicito.
- Politis e Romano, A Circular Block-Resampling Procedure for Stationary Data,
  rapporto Stanford (1991),
  https://statistics.stanford.edu/technical-reports/circular-block-resampling-procedure-stationary-data.
- Chwialkowski, Sejdinovic e Gretton, A Wild Bootstrap for Degenerate Kernel Tests,
  https://arxiv.org/abs/1408.5404.
  Per processi dipendenti la permutazione ingenua non fornisce una calibrazione
  generale. Qui le permutazioni sono scenari alterati, non il riferimento per
  il p-value del processo invariato. Non è implementato un test kernel e non
  se ne rivendicano le garanzie.

Rango empirico e massimo sulle componenti restano candidati esplorativi.
Blocchi non sovrapposti possono ancora essere dipendenti. Il ricampionamento è
condizionato alla storia finita e non trasferisce automaticamente un rischio del
5% al mercato futuro. Un'anomalia non identifica una causa e non quantifica
P(processo invariato | dati).
