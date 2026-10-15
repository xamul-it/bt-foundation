# Backtesting online

## Schermate

- **Backtesting → Nuova simulazione**: strategia, universo, date, provider e
  parametri. I campi vuoti ereditano i default del motore; gli override sono
  distinti. Per simboli liberi digitare `NVDA,AVGO,MU` e confermare con Invio.
- **Importa configurazione profilo** crea una copia per il backtest: non modifica
  il profilo e non cambia la modalità di trading del conto.
- **Verifica configurazione e dati** controlla i valori e mostra la presenza dei
  file locali; la copertura temporale è consultabile in Dati ticker. Presenza del
  file non garantisce che bastino barre per il warmup della strategia.
- **Risultati e archivio** e **Dashboard**: stato persistente, ricerca, preferiti,
  note, duplicazione, curva equity, trade paginati, configurazione richiesta,
  ricevuta effettiva, log e download. Nascondere un run non elimina gli artefatti.
- **Simulazioni giornaliere**: configura ora/minuti Europe/Rome e attiva/disattiva
  il timer. La finestra scorre mantenendo la durata in giorni di calendario.
  Il timer non scarica automaticamente dati e non esegue ordini broker.
- **Benchmark**: curve da CSV presenti in config-common/benchmark, importazione
  CSV date/return, salvataggio dei rendimenti di un run, etichette, note tramite
  API, archivio reversibile e download. Un import non sovrascrive un benchmark.
- **Dati ticker**: yahoo, yahoo_adj e Alpaca SIP/IEX; timeframe daily/minutes,
  copertura e aggiornamento file, ultime 500 barre e grafico normalizzato.
  Il download è un job monitorato che usa load_tickers.py incrementale con i
  lock esistenti; richiede connessione/credenziali valide per il provider.

## Parametri

I valori numerici del broker sono frazioni: `0.10` = 10% annuo per gli interessi,
`0.001` = 0.1% di slippage. Leva broker ed esposizione della strategia sono due
impostazioni differenti: il limite broker non obbliga la strategia a usarlo tutto.

Sono disponibili maintenance margin, cheat-on-close, cheat-on-open, shortcash,
controllo fondi alla submission e slippage open/match/out. I default del motore
precedente sono conservati. I flag vengono salvati nei risultati/provenienza e
supportati anche dalla configurazione TOML e dall'API programmatica del core.

Gli override strategia usano il tipo del default: numeri/booleani/array/oggetti in
JSON, stringhe come testo. L'input avanzato usa letterali Python (`True`, `False`,
`None`, liste e dizionari) in coppie `key=value` e prevale sugli override.

Il rendimento mostrato nei risultati è il composto della serie returns.csv
rispetto al capitale iniziale; il drawdown usa la curva partendo da equity 1.
Il numero di trade indica operazioni chiuse: BuyAndHold può avere posizioni
aperte e rendimento non nullo anche con zero trade chiusi.

## Architettura e deploy

API `/dyn/simulations`, autenticazione a sessione esistente. Job `simulation` e
`market_data_refresh` nel registro watchtower_calculation_jobs, executor isolato,
timeout e recupero interruzioni già usati dalle baseline. Il nuovo form accetta
solo backtest e forza nuovamente la modalità nel worker.

Le configurazioni delle pianificazioni e metadati archivio sono in
`bt-api/config/backtesting/registry.json`, con lock e sostituzione atomica.
Gli artefatti restano nell'archivio output condiviso. Non vengono esposti comandi
shell, path arbitrari o credenziali dal browser. Il catalogo dei job è limitato
alle ultime 200 esecuzioni; il registro conserva anche le precedenti.

Per aggiornare un'installazione:

1. Installare core aggiornato nell'ambiente API con `pip install --no-deps -e ../bt-core`.
2. Applicare `bt-core/config/sql/backtesting_studio.sql` sul DB configurato.
   La migrazione aggiorna soltanto il vincolo dei tipi di job, senza eliminare dati.
3. Verificare job attivi prima di riavviare l'API effettiva.
4. Ricompilare PWA con `bash scripts/bt-dash-build-prod.sh`, controllando il target
   Apache `/dash` e il proxy `/api` della destinazione online.

La rimozione del vincolo esteso non è un rollback valido se esistono già job dei
nuovi tipi: conservare tabella e storico e ripristinare soltanto UI/API se necessario.
Nessuna promozione al checkout di trading backtrader-prod è necessaria.

## Verifiche

Test isolati: validazione, broker/CLI, input malevoli, parser con stringhe/liste,
provider/feed, CSV/Parquet, data finale inclusiva senza barra del giorno successivo,
benchmark invalidi e pianificazioni persistenti. Smoke reale su dati locali e
prova browser autenticata: invio, monitoraggio, completamento, report, benchmark,
barre e grafico. Le prove non inviano ordini né scaricano dati da provider.
