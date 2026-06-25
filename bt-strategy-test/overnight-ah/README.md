# Overnight AH Operational Bundle

Snapshot operativo della strategia `overnight_ah.OvernightAH`, creato il
2026-06-25.

Questa directory serve a conservare materiale utile per riprendere test,
paper trading e diagnosi senza dover ricostruire tutto da cron, script e note
sparse. Non e' la sorgente live primaria: il codice operativo resta in
`/home/htpc/backtrader` e la copia stabile resta in `/home/htpc/backtrader-stable`.

## Contenuto

- `docs/ah_context.md`: diario di ricerca e risultati AH.
- `docs/alpaca_paper_live_overnight_ah.md`: note operative Alpaca paper/live.
- `scripts/`: snapshot degli script shell OvernightAH utili per entry, MOO e fallback.
- `tickers/yahoo_adj_research_universe.json`: universo research operativo, con `SPY` incluso per il gate.
- `ops/crontab-overnight-ah-current.txt`: solo blocco crontab OvernightAH, organizzato per portafoglio.
- `run-configs/overnight_ah_auc_dynamic_paper.toml`: run-config TOML della configurazione paper AUC dinamica.
- `source-snapshot/overnight_ah.py`: snapshot della strategia per lettura/confronto.

## Cosa Non Contiene

- Nessun file `env/*.key`.
- Nessuna chiave Alpaca.
- Nessun log operativo.
- Nessun dato storico OHLCV.

## Stato Operativo Salvato

Lo slot `auc` paper usa la strategia dinamica su `/home/htpc/backtrader`:

- `TRADING_MODE=paper`
- `DATA_PROVIDER=yahoo`
- `TICKER=yahoo_adj_research_universe.json`
- `STRAT=overnight_ah.OvernightAH`
- `AUCTION=False`
- entry cron alle `21:40` Europe/Rome, cioe' circa `15:40 ET`
- fallback auction entry commentato, perche' non serve con market/GTC
- MOO e fallback close ancora attivi

Gli slot `no` paper e `live` restano su `/home/htpc/backtrader-stable`.

## Note Importanti

`SPY` e' incluso nel ticker file solo per alimentare il gate mensile della
strategia. `OvernightAH` lo esclude dai ticker tradabili, quindi non dovrebbe
aprire posizioni su `SPY`.

Il run-config e' una configurazione riproducibile, non un sostituto degli script
cron. Per paper/live servono comunque le variabili ambiente Alpaca caricate
fuori da questo bundle.
