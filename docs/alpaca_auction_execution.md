# Alpaca Auction Execution

Questo documento definisce la convenzione interna tra Backtrader e Alpaca per
gli ordini d'asta e il fallback operativo.

## Convenzione broker

La strategia esprime l'intento di esecuzione con i flag Backtrader:

| Intento Backtrader | Ordine Alpaca |
| --- | --- |
| `coc=True` | `TimeInForce.CLS` |
| `coo=True` | `TimeInForce.OPG` |
| nessun flag auction | mapping standard del broker (`DAY` o `GTC`) |

La strategia non deve conoscere `CLS` o `OPG`: questa traduzione appartiene al
broker Alpaca.

## OvernightAH

`OvernightAH` usa il parametro:

```text
auction=True
```

Con `auction=True`:

- entry: `buy(..., coc=True)`; Alpaca lo invia come `CLS`;
- exit: `sell(..., coo=True)`; Alpaca lo invia come `OPG`.

Nel backtest daily di `OvernightAH`, l'uscita resta implementata come market
next-open (`coc=False`) per preservare la semantica storica Backtrader: entry
al close della barra D, exit all'open della barra D+1. Il flag `coo=True` e' la
convenzione broker per paper/live Alpaca, non il meccanismo usato da quel
backtest daily.

Con `auction=False`:

- la strategia invia ordini market standard;
- in paper/live il broker Alpaca li tratta con mapping standard, quindi senza
  asta.
- il backtest daily non e' una simulazione realistica di questa modalita',
  perche' non rappresenta l'entry schedulata intraday prima della close.
  Usare `auction=False` solo per prove paper/live schedulate, non per
  confrontare performance storiche daily.

Nelle strategie daily schedulate, la schedulazione e' parte del comportamento
operativo. La strategia gira quando viene lanciato il batch; non riceve una
barra minuto continua e non decide internamente il minuto di esecuzione.

## Fallback auction

Il fallback e' esterno alla strategia e non conosce i segnali. Lavora solo sugli
ordini Alpaca gia' inviati.

Policy:

- `BUY CLS` scaduto o parziale: reinvia la quantita' residua come `BUY LIMIT`
  con `extended_hours=True`, `time_in_force=GTC`, al close RTH della seduta
  con buffer opzionale.
- `SELL OPG` scaduto o parziale: reinvia la quantita' residua come `SELL MARKET`
  `DAY` durante la sessione RTH.

Questa distinzione gestisce due rischi diversi:

- in ingresso si controlla il prezzo;
- in uscita si riduce prima di tutto il rischio di esposizione intraday.

Script:

```bash
python bin/auction_fallback.py --after 15:45
```

Opzioni utili:

```bash
--dry-run
--client-id-prefix bt_overnigh_
--cls-buffer-bps 5
```

## Blue-green

`no` e `auc` possono girare in parallelo per misurare la meccanica di
esecuzione:

- `no`: `auction=False`, batch prima della close, nessun fallback auction;
- `auc`: `auction=True`, batch `CLS`, fallback dopo asta.

I due ambienti devono mantenere crontab, env e client order id separati.
