# Configurazione corrente e storico parametri

La baseline legge il profilo in ~/.config/backtrader/scheduled e il relativo
STRATEGY_CONFIG, con source Bash nello stesso ordine del cron. Il DB non
fornisce i parametri della baseline. Espansioni come $MAX_EXPOSURE e i pesi
compositi vengono risolte prima di costruire il comando.

Il handler entry invoca bin/watchtower_record_run_params.py dopo la lettura
eventuale dell'equity e prima dell'avvio della strategia. Registra il comando
preparato per l'ingresso, non attesta che il run sia poi riuscito: l'esito resta
nelle ricevute scheduled_entry_decisions. La ricevuta locale include data,
profilo, run ID, parametri espansi, capitale e commit/stato dirty dei checkout.
Non contiene credenziali.

scheduled_entry_equity e decision_date_override vengono esclusi dall'identità
di versione. Il capitale rimane nella ricevuta del run. Le osservazioni vengono
salvate in profile_param_versions, mantenendo la logica append-only esistente.

Prima del DB viene scritto atomicamente un JSON in
CODE_ROOT/out/watchtower-param-outbox/PROFILE. Un lock serializza registrazione
e recupero. Le ricevute pendenti vengono recuperate dalla più vecchia; al primo
errore il recupero si arresta e il trading prosegue segnalando telemetria
degradata. La successiva entry riprova automaticamente. I file .saved indicano
ricevute confermate dal DB e impediscono duplicazioni di recupero.

Recupero manuale (caricare l'ambiente DB come per gli altri script Watchtower):

```bash
bt-core/.venv/bin/python bin/watchtower_record_run_params.py --profile mirror \
  --outbox /home/htpc/backtrader-prod/out/watchtower-param-outbox/mirror --recover
```

Se esistono osservazioni precedenti a una versione già confermata nel DB,
il metodo rifiuta l'ordine: i JSON rimangono recuperabili e l'errore è esplicito.
Non si retrodata automaticamente la configurazione attuale e non si inventano
parametri per i run passati. La CLI btmain termina con codice diverso da zero
per argomenti strategia non validi, evitando falsi errori results.json mancante.
