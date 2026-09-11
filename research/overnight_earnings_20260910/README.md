# Verifica del filtro earnings della strategia overnight

Data dell'analisi: 10 settembre 2026.

## Decisione

Il filtro `earnings_skip` non è utile per questa strategia ed è stato rimosso dal percorso operativo. Nel confronto Backtrader richiesto, a parità di configurazione, il filtro riduce il CAGR dal 78,78% al 71,21%, abbassa lo Sharpe giornaliero da 1,698 a 1,667 e peggiora il massimo drawdown dal -46,37% al -47,91%.

La conclusione resta negativa nel test a nozionale fisso, usato per separare la selezione dei titoli da sizing e hedge: CAGR 81,36% senza filtro e 72,76% con filtro; Sharpe 1,706 contro 1,668; drawdown -49,04% contro -51,19%.

Il filtro modifica 308 giornate operative. Le 319 operazioni eliminate perché vicine a un earnings hanno prodotto senza filtro un P&L medio di +0,79%, mediano di +0,42%, con il 60,8% di operazioni positive. Il filtro migliora il rendimento annuale in 8 anni su 27.

## Protocollo

Sono stati eseguiti quattro backtest dal 3 gennaio 2000 al 9 settembre 2026 sulla stessa base dati e sullo stesso universo di 103 simboli:

- coppia `fixed`: nozionale fisso di 20.000 dollari, hedge disabilitato;
- coppia `operational`: configurazione completa del challenger, inclusi sizing e hedge;
- in ciascuna coppia cambia soltanto il filtro earnings;
- l'ultimo run eseguito è `operational_off`, cioè il confronto Backtrader con `earnings_skip=False` richiesto.

La finestra esclusa va dalla chiusura della sessione alle 16:00 ET fino alle 36 ore successive. Gli eventi sono stati scaricati con `yfinance.Ticker.get_earnings_dates`, mantenendo solo righe con EPS riportato per evitare date future. Il dataset contiene 7.805 eventi confermati per 99 azioni; SPY, QQQ e SQQQ sono ETF e non hanno earnings, mentre EA non ha restituito righe confermate.

## Dividendi

I backtest usano prezzi Yahoo adjusted. Il controllo indipendente confronta il rendimento adjusted close-to-next-open con `(open grezzo + dividendo) / close grezzo - 1` nelle date ex-dividendo. Su 3.961 osservazioni, l'errore assoluto mediano è 0,164 punti base e il 92,88% dei casi è entro 1 punto base. Quindi, salvo distribuzioni speciali e corporate action complesse, il rendimento incorpora il dividendo come richiesto: una perdita di prezzo del 5% con dividendo del 6% equivale circa a +1%; con dividendo del 4% equivale circa a -1%.

## Robustezza e limiti

La storia Yahoo non è un archivio point-in-time delle date annunciate e la chiamata è limitata a circa 100 righe per simbolo; 28 simboli raggiungono il limite. Per questo il periodo più vecchio ha copertura incompleta. Dal 2013, dove la copertura è più uniforme, il run operativo resta negativo: CAGR 81,15% senza filtro e 70,69% con filtro, con Sharpe 1,669 contro 1,626.

Dal 2020 al 2025 compreso il filtro resta negativo: CAGR 69,47% senza filtro e 62,47% con filtro. Includendo il 2026 parziale appare un vantaggio recente, CAGR 71,62% contro 70,26%, ma è concentrato nel solo 2026 parziale e non è sufficiente a ribaltare la decisione.

I livelli assoluti di rendimento sono molto elevati e risentono dell'universo storico statico e degli altri assunti del modello. La decisione usa il confronto accoppiato fra run identici, non il rendimento assoluto come previsione realistica.

## Artefatti riproducibili

- `fetch_yahoo_events.py`: acquisizione e normalizzazione degli earnings storici.
- `fetch_yahoo_dividends.py`: acquisizione dei dividendi.
- `audit_adjusted_dividends.py`: verifica dell'aggiustamento economico dei dividendi.
- `run_backtests.py`: definizione dei quattro run.
- `analyze_backtests.py`: metriche complessive, annuali e per sottoperiodo.
- `out/overnight_earnings_20260910/backtest_analysis.json`: risultato strutturato.
- `out/overnight_earnings_20260910/backtest_summary.csv`: tabella principale.

La classe sperimentale risiede in `bt-core/strategies/research/overnight_ah_earnings.py`; nessun modulo operativo è stato spostato.
