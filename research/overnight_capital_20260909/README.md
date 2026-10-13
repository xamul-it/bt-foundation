# Overnight AH — capitale operativo e prelievi

Studio richiesto il 9 settembre 2026. Output e dati congelati:
`out/overnight_ah_capital_study_20260909/`.

## Protocollo

1. Confronto a $50k, senza costi, con il run originale
   `sizing_waterfall_cash50k_nocost` (2000-01-03–2026-08-14).
2. Controlli sul commit di agosto `ec18d60`, prima del nuovo cooldown
   (`c993eaf^`), codice corrente `c993eaf`, e codice corrente con lag 1.
   I comandi originali e le copie dei sorgenti dei controlli sono negli output.
3. Due strategie con gli stessi parametri base del run storico, salvo
   `entry_signal_lag_bars=1` esplicito. La composite cambia solo il ranking,
   usando i pesi esistenti, non riottimizzati, e il pannello congelato.
4. Capitale C = 25k, 50k, 100k, 250k, 500k, 1M, 3M e 10M.
   Quando equity > 1,2C, si preleva equity−C. Controllo giornaliero dopo i
   fill, prima dei nuovi ordini, solo senza posizioni o ordini pendenti.
   Nessun versamento, nessun reddito fisso, nessun interesse sul prelevato.
   I run a 30M sono conservati come stress test escluso: ordini/posizioni
   hanno impedito il ritorno a C per lunghi intervalli, violando la policy.
5. Costi `alpaca`, margine annuo 4,75%, leva 2×. Cooldown 3% come parametro
   del vecchio studio, ma sul segnale del codice corrente. Non è la soglia
   1% del profilo development corrente. Configurazioni operative invariate.
6. Riavvii indipendenti nel 2013 a 50k, 500k e 3M, per entrambe le strategie.

## Metriche

`pnl_t = equity_end_t + withdrawal_t - equity_start_t`.
`return_t = pnl_t / equity_start_t`.
Sharpe = sqrt(252) × mean(return) / std(return, ddof=1), risk-free zero.
La media annua usa gli anni civili completi 2000–2025 (2013–2025 per i
riavvii); lo Sharpe e i minimi del conto includono agosto 2026.
L'analisi distingue profitti economici, prelievi, rendimento sul capitale
C, drawdown time-weighted e minimo conto/C. Il CAGR con reinvestimento non
è usato come stima del reddito da prelievi.

La soglia di Sharpe non viene inventata come requisito utente: si mostra
una tabella di sensibilità (1; 1,25; 1,5; 1,75; 2). Il valore 1,5 nella
sintesi è illustrativo. La fascia efficiente conserva almeno il 95% del
massimo prelievo annuo/C eleggibile. Il bootstrap appaiato di blocchi di
tre anni misura sensibilità campionaria, non affidabilità predittiva.

## Riproduzione

Da `/home/htpc/backtrader`, usando l'ambiente esistente:

```bash
bt-core/.venv/bin/python research/overnight_capital_20260909/run_sweep.py --workers 4
bt-core/.venv/bin/python research/overnight_capital_20260909/run_sweep.py --capitals 50000,500000,3000000 --start 2013-01-01 --prefix capital_restart2013_band20
bt-core/.venv/bin/python research/overnight_capital_20260909/audit.py
bt-core/.venv/bin/python research/overnight_capital_20260909/analyze.py
bt-core/.venv/bin/python research/overnight_capital_20260909/build_report.py
```

Il dispatcher salta i casi completati; al massimo quattro processi di
ricerca complessivi, con controllo della memoria disponibile. Non avviare
due dispatcher sulla stessa griglia contemporaneamente.

`run_case.py` aggiunge un hook di prelievo e un analyzer nel solo processo
di ricerca. Riusa feed, broker, ordini, strategia e commissioni di btmain.
Non modifica il codice di produzione. Conserva il ledger e gli output
standard di Backtrader; disabilita soltanto i log INFO e il report
QuantStats individuale, sostituito dal report aggregato.

## Evidenza e limiti

- `audit_manifest.json`: commit, parametri, SHA256 dei 103 file dati e del pannello.
- `data_quality.csv`: copertura e controlli OHLCV.
- `cases/*/ledger.csv`: percorso giornaliero e prelievi effettivi.
- `summary.csv`, `annual.csv`, `sweet_spots.csv`: tabelle di analisi.
- `validation.json`: identità contabili e controllo senza flussi.
- `paired_year_sensitivity.csv`: sensibilità appaiata della graduatoria.
- `artifact.json`, `report.html`: report canonico e lettore portabile.

La copia esatta del dataset e dello stato non committato del run originale
non era disponibile. Il commit di agosto riproduce da vicino quel run,
ma non certifica l'identità dell'ambiente originario. L'universo e i pesi
sono stati ricercati ex post; i riavvii non sono test fuori campione dei
pesi. Il modello non include impatto di mercato aggiuntivo, tassazione o
garanzie di eseguibilità nelle aste. Il cap ADV è quello implementato,
non una misura validata di capacità realmente negoziabile.

## Scelte dei grafici

Tre curve sui livelli discreti di capitale rispondono alla medesima
domanda di capacità, con metriche diverse: percentuale, dollari e Sharpe.
L'asse dei capitali è ordinale ed esplicitamente etichettato: le distanze
non rappresentano incrementi uguali di dollari. Il grafico annuale a barre
mostra l'irregolarità dei prelievi. Tabelle complete per valori esatti,
soglie e controlli di periodo. Dati arricchiti con denominatori, anni,
metriche di rischio e prelievi; palette e lettore condivisi del plugin.
