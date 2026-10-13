# Development: equity condivisa prima degli ingressi

Il profilo development abilita SYNC_BACKTEST_EQUITY=1. Default: 0.
L'handler rifiuta l'attivazione per altri profili o in modalità live.

Prima di avviare la strategia Alpaca, scheduled_entry_equity.py legge il
conto paper e salva una fotografia con equity, cash, buying power e ora UTC
negli artefatti del run. Nessuna operazione broker è inviata dal lettore.
L'equity positiva viene passata come scheduled_entry_equity a entrambi i
run e come --cash al Backtest. Hedge e waterfall usano la stessa base fissa;
max_exposure resta la leva desiderata, margin_leverage il limite del broker.
L'attuale leva development resta 1: questa modifica non cambia l'esposizione.

Se la fotografia non è disponibile, Alpaca mantiene il sizing precedente;
il Backtest sincronizzato è segnalato come fallito e non usa i 200.000 di
default. Nessun dato storico o posizione esistente viene riscritto.
Il recupero chiusure mantiene quantità e prezzi originali; il replay può
riutilizzare il parametro di equity conservato negli artefatti originali.

L'opzione non viene inserita negli STRATARGS versionati: i benchmark storici
continuano a usare il proprio capitale composto. Disattivare
SYNC_BACKTEST_EQUITY ripristina il percorso precedente. Checkout prod,
profili live/mirror/challenger, cron e dashboard non sono modificati.

Commissioni e calcolo del P&L della dashboard restano quelli esistenti:
l'allineamento del capitale non risolve eventuali differenze fra P&L lordo
e netto. Questo richiede una verifica separata.

Prima di estendere il percorso ad altri profili osservare un ciclo paper:
fotografia e sizing_capital uguali nei due ingressi, input_cash Backtest
uguale alla fotografia, allocazioni waterfall e chiusure originali coerenti.
