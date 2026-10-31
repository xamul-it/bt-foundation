# Preparazione commit complessivo — 27 ottobre 2026

Staging delle modifiche residue dopo i commit del monitoraggio proxy/replay. Il repository `bt-strategy-test` è interamente escluso su richiesta dell’utente. Nessun push e nessun ulteriore riavvio di servizi.

## Contenuto

| Repository | File nello staging | Ambito |
|---|---:|---|
| bt-core | 9 | Sharpe giornaliero, report logaritmico, coerenza storica, documentazione strategia |
| bt-api | 9 | Dashboard conto Alpaca, diagnostica e gestione simulazioni, configurazione servizi |
| bt-dash | 96 | Studio backtest, grafici, diagnostica, dashboard portafoglio, icone PWA e test |
| bt-gateway | 1 | Documentazione del runtime/venv separato |
| bt-strategy-test | 0 | Interamente escluso su richiesta dell’utente |
| root | 1752 | Documentazione, studi statistici, risultati riproducibili e parametri di strategia |

Lo staging include i cambiamenti di parametri già presenti in `config-common/run-configs/rma_daily.toml` e `config-common/scheduled/strategies/overnight-ah-prod.env`. Il file env tracciato contiene configurazione di strategia: il controllo dei candidati non ha rilevato credenziali letterali. Non vengono applicati nuovi cambiamenti ai servizi operativi.

## Esclusioni

Sono conservati sul disco e lasciati fuori dallo staging:

- l’intero repository `bt-strategy-test`, compresi codice, report, risultati sintetici, dataset e modifiche allo stato degli studi; nessun aggiornamento del suo riferimento nella root;
- cache generate in config-common/benchmark;
- backup, copie `.old`, file temporaneo `.codex.bak.empty`;
- symlink locali data/out verso archivi operativi;
- file ignorati già dalle regole dei repository, incluse credenziali e runtime.

Includiamo invece i dati congelati e i risultati dei piccoli studi in `research/`, necessari a rendere ispezionabili le conclusioni. Non sono stati modificati i dati degli esperimenti.

## Verifiche

- Controllo dei candidati per chiavi private, token noti e credenziali letterali: nessun riscontro.
- Il controllo preliminare includeva 291 file Python: 289 validi. Le due eccezioni seguenti appartengono a `bt-strategy-test`, ora interamente escluso dallo staging:
  - overnight-ah/research/autonomous_rotation_20261017/frozen_development/bt-core/indicators/prod_indicators.py;
  - quantconnect_5/sources_original/S1.py (inizia con `region imports`).
- `diff --check` pulito per le modifiche applicative core/API/gateway; rimossa una riga vuota con spazi nella dashboard. Gli archivi di ricerca e sorgenti originali contengono CRLF e whitespace preesistente, mantenuti per non cambiare le copie congelate.
- Ultime suite eseguite sulla stessa workspace: API 135 passati e 1 errore PostgreSQL; core 349 passati e 6 fallimenti già segnalati; gateway 16 passati; frontend 23 file di test passati. Non sono state rieseguite simulazioni né validati tutti i risultati scientifici per questa preparazione Git.

I commit dei repository annidati vanno creati prima di aggiornare i relativi riferimenti nella root. L'indice non include i dataset esclusi; la workspace può quindi continuare a mostrare file non tracciati.
