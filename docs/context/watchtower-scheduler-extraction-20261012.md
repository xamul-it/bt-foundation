# Proposta: scheduler Watchtower separato

Data: 2026-10-12
Stato: backlog, proposta discussa; implementazione e attivazione non decise.

## Contesto e obiettivo

La dashboard è stabile e usa il checkout development per facilitare lo
sviluppo. Le strategie Alpaca sono eseguite da cron: live e mirror usano
il checkout prod, gli altri profili development. L'API ospita anche lo
scheduler dei lavori di monitoraggio.

Valutare l'estrazione dello scheduler esistente in un servizio autonomo,
con propria unità systemd e limiti di risorse. Il beneficio atteso è poter
riavviare e sviluppare l'API senza interrompere i job di monitoraggio e
isolare il carico dei lavori pesanti.

Non si propone di trasferire questi job su cron. Il cron di trading e i
suoi orari restano invariati. La pagina Scheduler conserva configurazione,
avvio manuale e visualizzazione dello stato.

## Perimetro da verificare prima dell'implementazione

- Accoppiamento fra endpoint API e istanza scheduler nel processo API.
- Comunicazione API-servizio per configurazione, comandi e stato.
- Unico proprietario delle scritture di scheduler-state.json; l'eventuale
  migrazione al database va decisa separatamente.
- Conservazione di orari, concatenazioni after_job, pausa, errori e recupero
  dopo riavvio.
- Passaggio senza doppie istanze attive o perdita dei lavori previsti.
- Versione del codice e configurazione usate da ciascun profilo: separare
  il processo non rende automaticamente stabile il codice per live/mirror.

Il Backtest originale giornaliero oggi parte dall'handler cron di ingresso;
il recupero chiusure parte dallo scheduler API. L'estrazione iniziale riguarda
i job già nello scheduler API. Spostare il Backtest originale richiede una
decisione distinta e la conservazione del contesto dell'ingresso.

## Verifica e rilascio futuri

Provare prima su development, verificando configurazione dalla dashboard,
avvio manuale, concatenazioni, riavvii e assenza di esecuzioni duplicate.
Mantenere un percorso di ritorno allo scheduler nel processo API.
Nessuna modifica o attivazione su live/mirror è autorizzata da questa nota.

## Priorità attuale

L'estrazione non è un prerequisito per il lavoro sul capitale del Backtest.
Si torna alla sincronizzazione dell'equity prima dei nuovi ingressi,
da implementare e provare solo sul profilo development con opzione
disabilitata per default. Live resta invariato. Quantità e prezzi delle
posizioni originali vanno preservati fino alla chiusura.
