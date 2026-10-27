# Proxy, flussi live e registrazioni

## Pagine

- **Liquidità L1** (`/dash/#/QuoteMonitoring`): selezione persistente dei ticker, aggiunta manuale dal portafoglio, stato quote e download storico SIP AH.
- **Amministrazione proxy** (`/dash/#/ProxyAdministration`): servizi proxy/logger, identità complete dei client, applicazioni, canali richiesti e campione live con bid/ask, trade tick e close delle barre distinti.
- **Registrazioni flussi** (`/dash/#/RecordedFlows`): inventario per simbolo, estratto finale, replay mediante proxy dedicato e cancellazione definitiva con conferma del simbolo. Il download non è esposto tra le azioni principali, come richiesto.
- **Watchtower**, anche in **Feed Monitoring**: selezione dei simboli richiesti da Watchtower, indipendente dalla lista Liquidità L1.

La raccolta continua a pagina chiusa. L'importazione dal portafoglio è manuale, non segue automaticamente le posizioni successive. Nessun componente invia ordini.

## Registrazioni e client

Configurazione condivisa `config-common/quote-monitor.json`: `symbols` per Liquidità L1 e `sources.watchtower` per Watchtower. Il registratore usa l'unione, massimo 100 simboli. Gli aggiornamenti sono serializzati con flock e pubblicati atomicamente: salvare una lista conserva l'altra.

Il logger ha un client dedicato, identificato mediante `REGISTER`. Ogni 5 secondi legge la configurazione e interroga `ADMIN`; ogni 15 secondi rinnova `MIN`, `QUOTE`, `TRADE`. Il proxy condivide ogni sottoscrizione upstream tra client e la rilascia quando si disconnette l'ultimo richiedente.

`ADMIN` restituisce identità complete, applicazioni, heartbeat e canali minute/daily/quote/trade. Le nuove connessioni bt-core usano identità `bt-core:<pid>:<symbol>:<uuid>`. I client legacy non identificabili restano esplicitamente senza nome.

Il feed a barre bt-core ignora quote e trade sul topic condiviso, senza interpretarli come OHLCV. I trade contengono prezzo/size reali del singolo scambio, exchange, ID, condizioni, tape, feed e timestamp. Le quote conservano anche condizioni/tape/feed. Le size restano native, senza conversioni implicite in azioni.

## Live e telemetria

`out/quote-monitor-status.json`, aggiornato atomicamente circa ogni secondo: snapshot proxy, contatori dall'avvio, sessione e sequenza logger, buffer circolare di 500 eventi. `/status` e `/admin` escludono il buffer; `/live?after=...&session=...` restituisce eventi e segnala gap o riavvio.

La pagina legge ogni secondo, mostra fino a 500 eventi nel grafico e gli ultimi 100 in tabella. Il campione browser non è lossless: traffico elevato o pause possono superare il buffer, con avviso. I contatori non provano l'assenza di perdite upstream. La pausa riguarda solo la visualizzazione.

Grafici e tabella usano Europe/Rome e timestamp originali con frazioni di secondo. Logger vivo solo con snapshot di età inferiore a 20 secondi. Servizio attivo e ACK non provano che arrivino dati Alpaca.

## Archivi

Il logger conserva `out/dump/<SYMBOL>.json` in formato JSONL, preservando i file precedenti (circa 30 GB rilevati). Nessun archivio reale è stato cancellato.

- `/archives`: inventario dimensioni, ultima scrittura e versione.
- `/recordings/<symbol>`: ultimi 2 MB, fino a 500 eventi.
- `/archives/<symbol>/events?offset=...`: lettura progressiva fino a 1.000 eventi o circa 1 MB. La riga finale incompleta viene riprovata; righe oltre 1 MB interrompono la consultazione.
- `/archives/<symbol>/download`: download HTTP, incluse richieste parziali. Un archivio attivo può continuare a crescere.
- `DELETE /archives/<symbol>`: richiede simbolo digitato e versione inventario. Rifiuta file cambiati, simboli nelle liste salvate o sottoscritti da altri client, oppure telemetria non verificabile. La verifica delle sottoscrizioni usa l'ultimo snapshot (circa 5 secondi), non blocca nuove richieste concorrenti. Non arresta applicazioni.

I link simbolici sono rifiutati. La cancellazione è bloccata anche se un replay gestito sta utilizzando l'archivio. La consultazione mostra un estratto; la riproduzione passa dal proxy ZMQ descritto sotto.

## Replay ZMQ

Configurazione preesistente verificata: `scripts/replay-proxy.sh`, `scripts/replay-proxy.service` (collegato a user-systemd) e `env/replay`. Al controllo il servizio era inattivo, porte 5557/5558 libere, data configurata 2026-01-12 e velocità 60×. Questi file operativi non sono stati modificati o avviati. La gestione da API crea un processo dello stesso motore; non controlla o arresta il servizio systemd preesistente e fallisce se le porte sono già occupate.

Il percorso estende il proxy esistente `bt-gateway/bt_alpaca_zmq/replay_zmq_proxy.py`, senza un secondo motore di riproduzione nel browser. La vecchia implementazione restituiva ACK anche per simboli inesistenti, trasformava tutte le righe in barre, collassava timestamp uguali, usava un passo fisso di 60/speed secondi e orari UTC fissi. Questi comportamenti sono stati corretti.

- Porte default: REQ **5557**, PUB **5558**, in ascolto su loopback, separate da 5555/5556 live.
- Sessione/data in **America/New_York**: intera giornata, RTH 09:30–16:00 o AH 16:00–20:00. Il fuso determina correttamente i confini UTC anche attraverso l'ora legale. Le mezze giornate richiedono una scelta/interpretazione separata della finestra.
- Preparare il replay indicizza su SQLite solo i file dei simboli selezionati; il processo resta IDLE. La lettura usa una dimensione iniziale fissa degli archivi e non insegue una registrazione in crescita. L'indice è temporaneo su disco, con cache limitata; lo storico di warmup è limitato a 10.000 barre precedenti la sessione per simbolo, anch'esse indicizzate su disco.
- Il primo client con una registrazione valida avvia il clock dopo un breve intervallo per l'handshake PUB/SUB. 1× mantiene le distanze temporali, 60× le divide per 60, 0 elimina le attese. I client devono mantenere heartbeat (timeout 60 s).
- Canali separati: `MIN:<symbol>`, `QUOTE:<symbol>`, `TRADE:<symbol>`. Dati assenti per simbolo o canale restituiscono rispettivamente `ERR:NO_DATA_FOR_SYMBOL` e `ERR:NO_DATA_FOR_CHANNEL`; niente ACK fittizio. `DAY` e comandi non supportati restituiscono errore.
- Registrazione atomica multipla: `SUBSCRIBE:{"symbols":["AAPL"],"channels":["minute","quote","trade"],"application":"Studio"}`. Tutti i canali richiesti devono esistere per tutti i simboli; usare `CONFIG` prima per vedere `available`.
- Conserva eventi distinti con timestamp coincidenti e ordina con precisione ai nanosecondi; il tie-break è l'ordine di lettura delle righe. Conserva i timestamp originali per default. L'opzione esplicita `--remap-today` trasla le date mantenendo l'orario New York anche tra regimi DST diversi; `recorded_timestamp` conserva l'originale.
- I client tardivi si aggiungono al clock corrente. Il vecchio fast-forward pubblicava eventi anche ai client già presenti; non viene più usato. Non esiste una coda privata di recupero per il singolo client.
- `DISCONNECT` rilascia solo il client; quando non restano client il proxy torna IDLE, pronto per un nuovo clock. Fine dati pubblica `EOD` con reason `day_complete`. `RESET` è rifiutato se sono collegati client esterni.
- `PREVIEW:START` registra il client persistente "Dashboard replay" e una vera sottoscrizione SUB al PUB del replay. La pagina visualizza gli ultimi eventi effettivamente ricevuti da quel socket, non un timer JavaScript. `PREVIEW:STOP` scollega solo quel client. Il buffer visuale è di 500 eventi; PUB/SUB non garantisce consegna lossless a consumatori lenti, soprattutto a velocità massima.

API `/dyn/quote-monitor/replay`: GET stato, POST preparazione, DELETE arresto del solo processo gestito. `/replay/control` accetta `preview_start`, `preview_stop`, `reset`. Un'unica sessione per API; il subprocess usa il venv gateway, non eredita credenziali broker e termina se il worker API genitore scompare. Nessuna strategia viene avviata e nessun broker dei client viene modificato.

Esempio CLI (nessun accesso Alpaca):

```bash
bt-gateway/.venv/bin/python bt-gateway/bt_alpaca_zmq/replay_zmq_proxy.py \
  --date 2026-07-14 --symbols AAPL --session ah --speed 60 \
  --log-dir /home/htpc/backtrader/out/dump
```

Il client punta `PROXY_ADDR=tcp://127.0.0.1:5557` e `proxy_pub_addr=tcp://127.0.0.1:5558`; il flusso è msgpack con topic del simbolo, `*` per EOD. Lo storico `HISTORICAL|stock|...|minute|...` resta compatibile col client bt-core e usa le ultime barre registrate prima della sessione, entro il limite di 10.000; come nel vecchio proxy, non interpreta il range from/to come un download completo.

Verifica isolata effettuata con processo e client ZMQ reali su porte temporanee e archivi sintetici: simbolo inesistente rifiutato; quote/trade/bar ricevuti a 600×; nanosecondi/timestamp uguali conservati; EOD, reset con client esterno, dashboard SUB e arresto verificati. Test aggiuntivi per finestre AH estive/invernali, intervalli scalati e rimappatura DST. Non è stata avviata una sessione sugli archivi operativi dell'utente.

## Storico

`GET /history/<symbol>?portfolio_key_id=...&date=YYYY-MM-DD`: quote SIP tra 16:00 e 20:00 America/New_York, con DST. Sessione conclusa da almeno 15 minuti. Le mezze giornate richiedono interpretazione separata.

Fino a 10.000 quote per pagina; la UI conserva `next_page_token` e segnala lo storico parziale. Ogni pagina viene scaricata nel browser come JSONL: nessun backfill automatico sul server. Fine delle pagine non certifica la completezza del provider. Il conto scelto serve a storico/posizioni; il live usa conto/feed del gateway.

L1 non ricostruisce profondità, priorità o fill. Confronto con segnali e simulazione economica di uscita non sono implementati.

## Verifica e attivazione al 27 ottobre 2026

La versione precedente L1 è attiva: servizi verificati, API autenticata HTTP 200, feed configurato SIP, lista iniziale vuota.

L'estensione amministrativa è pronta nel codice e nella PWA compilata. Passano 24 test API mirati, 16 gateway, la regressione bt-core per quote/trade sul feed a barre e 23 file frontend. Browser con API simulate: client/canali, grafico quote/tick, gap, consultazione/controlli replay ZMQ, cancellazione di fixture, registrazione Watchtower e mobile. Nessuna cancellazione reale.

Suite complete: API 135 passati e un errore PostgreSQL nel test Watchtower baseline; bt-core 349 passati e sei fallimenti in HMA retry, tre test PostgreSQL baseline, confronto ingressi schedulati e contratto snapshot. Nessuno interessa i file modificati qui. `diff --check` passa sui file dell'intervento; nel diff bt-core complessivo resta whitespace preesistente in `strategies/overnight_ah.py`.

L'estensione è stata attivata dopo consenso esplicito al riavvio di proxy, logger e API. Tutti e tre risultano active/running, senza riavvii anomali. Le nuove API autenticate `/status`, `/admin`, `/archives`, `/replay` rispondono HTTP 200; il logger conferma la connessione al proxy e il feed SIP. Verifica browser autenticata sulla PWA servita, senza fixture: client/servizi del proxy e pagina archivi/replay visibili.

Una sottoscrizione temporanea AAPL (MIN/QUOTE/TRADE, applicazione "Verifica post-riavvio") ha ricevuto 10 trade e una quote bid/ask reali ed è stata rilasciata con DISCONNECT; le liste persistenti sono rimaste invariate. La gestione replay è disponibile ma nessuna sessione è stata lasciata avviata: la prova del motore replay resta quella isolata su dati sintetici descritta sopra. Nessun ordine inviato e nessun archivio reale cancellato.
