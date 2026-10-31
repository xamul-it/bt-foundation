# My portfolio: dashboard del conto Alpaca

Prima versione: 26 ottobre 2026.

La pagina `/Alpaca`, denominata **My portfolio** nel menu, osserva il conto
broker nella sua interezza. Non attribuisce l'equity a una strategia. Il selettore
**Spazio Alpaca** nella barra della pagina usa il registro dei conti attivi;
l'eventuale profilo associato è un'etichetta, mentre `portfolio_key_id` identifica
il conto effettivamente interrogato. La scelta viene conservata nel browser.
Paper e Real sono espliciti e determinano anche l'host delle API di lettura.
Un identificatore esplicito non risolvibile non ricade sul conto di default.

## Dati e semantica

- Equity e drawdown: campioni di Portfolio History Alpaca, non OHLC del conto.
  Il timeframe effettivo è indicato nel grafico; oltre la finestra intraday
  supportata viene usato 1D. Per 30 giorni intraday, richieste sequenziali
  di massimo 29 giorni preservano il TF scelto senza superare il limite
  di calendario del broker; i campioni ai confini vengono deduplicati. Il drawdown è calcolato sul massimo progressivo
  all'interno del periodo caricato, non sul massimo storico precedente.
- Variazione equity: differenza tra primo e ultimo campione. Non è un rendimento
  depurato da depositi, prelievi e altri flussi esterni.
- Posizioni: fotografia attuale del broker, indipendente dai filtri temporali.
  Non si ricostruisce il passato ridistribuendo l'equity secondo i pesi attuali.
- Ordini: ordini inviati nel periodo più tutti quelli attualmente aperti. Il
  dettaglio espone i timestamp presenti nel record broker, i collegamenti alle
  sostituzioni e gli eseguiti caricati nel periodo. Non è un event log completo.
  Il limite di 500 ordini per richiesta è segnalato; restringere il periodo.
- Eseguiti: attività FILL, comprese le esecuzioni parziali, con timestamp preciso.
  Il limite di paginazione e l'eventuale uso della cache vengono segnalati.
- Analisi per titolo: numero e controvalori degli eseguiti. Vendite meno acquisti
  è un flusso di cassa, non P&L realizzato.
- Orari visualizzati: America/New_York; date personalizzate: confini UTC
  esplicitati nei campi. Le richieste usano timestamp ISO8601.

Aggiornamento manuale, nessun comando di invio/modifica/cancellazione ordini
nella dashboard. Gli errori dei singoli pannelli non impediscono di vedere i dati
che sono stati caricati correttamente. Il cambio conto annulla le richieste in
corso e svuota i dati precedenti.

## Evoluzioni concordate, non implementate

1. Registrazione ZMQ attivabile per i simboli in portafoglio oppure per una lista
   esplicita, con inizio/fine e copertura ispezionabili.
2. Estensione delle barre minute a trade tick e quote bid/ask; quote e trade sono
   eventi distinti. Lo spread richiede bid/ask sincronizzati; la profondità del
   book richiede un feed con i livelli necessari.
3. Replay dello stream registrato con velocità, pausa e avanzamento temporale,
   separato dal conto broker e dai suoi ordini reali.
4. Storico delle posizioni ricostruito da eseguiti, quantità iniziali e movimenti
   societari; riconciliazione periodica con snapshot broker.
5. Valutazione storica del portafoglio dai prezzi registrati e dalle quantità
   effettivamente detenute. Eventuali OHLC vanno aggregati dalla curva valutata
   e qualificati con frequenza e copertura: gli OHLC dei singoli titoli non sono
   sufficienti a determinare gli estremi simultanei del portafoglio.
6. Performance corretta per flussi esterni, analisi per fascia oraria, slippage
   su quote di riferimento e attribuzione dei costi con copertura esplicita.

Per preservare la riproducibilità, i futuri eventi registrati devono distinguere
orario del mercato e di ricezione, sorgente/feed, simbolo e tipo evento;
registrazioni, sessioni di replay e osservazioni del conto devono avere identità
separate.

## Filtri guidati, indicatori e marcatori

I limiti dei TF sono centralizzati in `TIMEFRAME_LIMITS`: 1Min, 5Min, 15Min e
1H fino a 30 giorni tramite richieste segmentate; 1D per periodi più lunghi.
La combo Periodo nasconde i preset oltre il limite del TF corrente; la combo TF
nasconde quelli incompatibili con il periodo. Per espandere l'orizzonte oltre
30 giorni scegliere prima 1D. Le date personalizzate sono inclusive: un intervallo
oltre 30 giorni aggiorna il TF a 1D. I filtri validi aggiornano automaticamente i
dati dopo un breve debounce, annullando le richieste precedenti.

Gli indicatori del periodo sono separati dalla fotografia attuale del conto:
P&L equity (ultima meno prima equity, flussi esterni inclusi), variazione %,
drawdown massimo % e in valuta, durata massima e attuale del drawdown in giorni
di calendario, variazione giornaliera media in valuta e percentuale. I giorni
sono raggruppati in America/New_York. Per l'intraday il primo giorno parte dal
primo campione disponibile e l'ultimo può essere parziale; per 1D il primo
campione è la base e la media usa le variazioni successive. Il numero di intervalli
osservati è riportato nell'aiuto della metrica. La durata del drawdown include
weekend e frazioni di giorno e parte dal massimo osservato nel periodo scelto.

Selezionare uno o più simboli per aggiungere marcatori all'equity del conto.
La legenda usa ▲ Ingresso e ▼ Uscita, anche per posizioni short. Sul grafico il
testo è soltanto `SIMBOLO #ciclo`, con colori distinti per simbolo. Il numero
parte da 1 per ciascun simbolo nel periodo caricato e resta invariato per tutti
gli incrementi e le chiusure parziali fino al ritorno a posizione piatta. Il
prossimo ingresso appartiene al ciclo successivo. Una singola esecuzione che
ribalta la posizione è separata in uscita del ciclo precedente e ingresso del
successivo; entrambe conservano il riferimento all'eseguito broker originale.

Prima di numerare i movimenti vengono letti gli eseguiti precedenti all'inizio
del periodo, in ordine cronologico, per ricostruire la quantità iniziale. La
lettura viene avviata solo se si selezionano simboli e riutilizzata per lo stesso
conto/intervallo. Se lo storico è troncato, indisponibile o in fallback locale,
la numerazione non viene mostrata e la pagina espone il motivo. La ricostruzione
si basa sulle quantità FILL; rettifiche societarie e trasferimenti titoli
richiedono anche il registro dei movimenti non commerciali, evoluzione separata.

Il dettaglio dei movimenti riporta ciclo, ingresso/uscita, quantità e posizione
residua dopo ciascun eseguito, oltre a prezzo e timestamp preciso. I marcatori
sono allineati al campione equity precedente (al giorno per il TF 1D); quelli
fuori dalla copertura equity sono segnalati nel conteggio. Selezionare simboli
filtra i marcatori, senza trasformare equity e indicatori del conto in quelli
del singolo titolo.
