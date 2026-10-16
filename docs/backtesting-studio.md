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
- **Dashboard**: una card per ID run completato, con nome/etichetta in evidenza;
  run della stessa strategia, anche con etichette uguali, restano indipendenti.
  Pin persistente per tenerli in alto e cestino reversibile. Ultimi 200 job più
  i preferiti anche più vecchi, con rendimento composto, drawdown, trade chiusi,
  capitale iniziale/finale, periodo, provider, leva e link a risultati e report.
  Le card non sono una classifica: periodi e configurazioni possono differire.
- **Risultati e archivio**: stato persistente, ricerca, preferiti,
  note, duplicazione, curva equity, trade paginati, configurazione richiesta,
  ricevuta effettiva, log e download. Nascondere un run non elimina gli artefatti.
- **Simulazioni giornaliere**: configura ora/minuti Europe/Rome e attiva/disattiva
  il timer. La finestra scorre mantenendo la durata in giorni di calendario.
  Il timer non scarica automaticamente dati e non esegue ordini broker.
- **Benchmark**: curve da CSV presenti in config-common/benchmark, importazione
  CSV date/return, salvataggio dei rendimenti di un run, etichette, note tramite
  API, archivio reversibile e download. Un import non sovrascrive un benchmark.
  Provenienze distinte: CSV statico, run salvato statico, Buy&Hold lista mantenuto.
  La colonna obsolescenza conta i giorni di calendario dall'ultima data contenuta
  nella serie (Europe/Rome), non dalla data di scrittura del file. CSV storici
  vuoti o senza date leggibili sono segnalati ed esclusi dalla selezione.
- **Dati ticker**: yahoo, yahoo_adj e Alpaca SIP/IEX; timeframe daily/minutes,
  copertura e aggiornamento file, ultime 500 barre e grafico normalizzato.
  Date daily senza orario, date minute con orario; formato e disponibilità
  compattati in icone e semafori con tooltip e legenda.
  Il download è un job monitorato che usa load_tickers.py incrementale con i
  lock esistenti; richiede connessione/credenziali valide per il provider.
- **Liste ticker**: elenco paginato, ricerca per nome e appartenenza di un simbolo
  esatto, numero di simboli unici, sorgenti, download e benchmark. Il nome della
  lista apre Dati ticker con lista/provider/feed preselezionati. Creazione da
  CSV con colonna `symbol`/`ticker`, TXT con un simbolo per riga oppure array JSON
  (UTF-8, massimo 1 MB e 1000 simboli); i duplicati vengono rimossi. La modifica
  può sostituire i simboli con un nuovo file. L'archiviazione sposta il file in
  `config-common/tickers/.archived`, conserva gli storici e disattiva i timer;
  il ripristino recupera la lista con schedulazione disabilitata.

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

## Benchmark automatici e giornalieri

Nei nuovi run, lasciare vuoto il benchmark genera il Buy&Hold degli stessi
ticker congelati nel run, con lo stesso provider/feed e periodo richiesto.
Pesi iniziali uguali e quote frazionarie costanti: si media il rapporto dei
prezzi rispetto al prezzo iniziale, NON i rendimenti giornalieri (che causerebbe
ribilanciamento). Nessuna leva, commissione o slippage. Si usano le chiusure
giornaliere; per simulazioni minute la chiusura giornaliera delle barre minute.
Il periodo effettivo comune è registrato: se un componente inizia più tardi,
il benchmark parte dalla prima data comune; se termina prima, termina alla
data comune finale. Componenti mancanti o copertura insufficiente fanno fallire
il job esplicitamente, senza sostituzioni di provider o esclusioni silenziose.

In **Benchmark → Liste da usare per benchmark**, selezionare lista, provider,
feed e data iniziale, spuntare **Usa per benchmark** e salvare. Solo quelle
combinazioni lista/provider/feed entrano nel batch condiviso; le altre liste
vengono scaricate solo se il download è abilitato in Liste ticker.
Default 01:15 Europe/Rome, modificabile nel form.
Salvare una configurazione abilitata avvia anche il primo job. Il refresh manuale
è possibile anche con timer disattivato. Disattivare il benchmark conserva il
download; disattivare il download disattiva anche il benchmark. Un cambio di
configurazione non cancella un job già accodato né un benchmark pubblicato.

Un solo timer per orario raccoglie le liste abilitate e congela la composizione.
I ticker sovrapposti vengono deduplicati per sorgente/feed; Yahoo grezzo e
aggiustato condividono una richiesta di rete. Si scarica dall'inizio storico
più remoto richiesto dal gruppo, usando il loader incrementale e i suoi lock.
I batch di catalogo manuali e automatici condividono un profilo esclusivo.
La deduplicazione riguarda il catalogo: gli script dei profili trading conservano
la propria pianificazione e continuano a usare i lock dei dati esistenti.

Il loader produce una ricevuta per simbolo: un CSV vecchio già presente non
certifica il successo di un nuovo download. Gli errori parziali non impediscono
l'aggiornamento delle altre liste; il benchmark della lista incompleta resta
quello precedente. Ultimo batch ed esito sono distinti dall'ultimo download
completato: nessuna delle due date garantisce una data uniforme delle barre.
Per la copertura effettiva consultare Dati ticker. I log del batch sono accessibili
dall'elenco; le configurazioni benchmark precedenti vengono riconosciute dal catalogo.

Il job persistente `market_data_refresh` esegue download incrementale dei simboli
congelati, poi la fase `benchmark_buyhold`. Stato, errore e log sono visibili
accanto alla lista. Un errore di download/calcolo mantiene il CSV precedente.
Il calcolo è applicato solo dopo il download: nessun CSV parziale viene pubblicato.
I timer vengono ripristinati all'avvio API. CSV importati e run salvati restano
statici; archiviare un benchmark lo nasconde ma non disabilita il suo timer.

Ogni simulazione conserva `benchmark.csv` negli artefatti: anche un benchmark
esplicito viene copiato prima dell'esecuzione, per impedire agli aggiornamenti
giornalieri di alterare un confronto già eseguito. Le copie interne `__run_benchmark_*`
non appaiono nell'archivio. I run precedenti non vengono ricalcolati retroattivamente.

## Architettura e deploy

API `/dyn/simulations`, autenticazione a sessione esistente. Job `simulation` e
`market_data_refresh` nel registro watchtower_calculation_jobs, executor isolato,
timeout e recupero interruzioni già usati dalle baseline. Il nuovo form accetta
solo backtest e forza nuovamente la modalità nel worker.

Le configurazioni delle pianificazioni e metadati archivio sono in
`bt-api/config/backtesting/registry.json`, con lock e sostituzione atomica.
Gli artefatti restano nell'archivio output condiviso. Non vengono esposti comandi
shell, path arbitrari o credenziali dal browser. Il catalogo dei job è limitato
alle ultime 200 esecuzioni più i preferiti; il registro conserva anche le precedenti.

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
