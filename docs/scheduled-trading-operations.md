# Scheduled trading operations

Documento operativo canonico per schedulazioni, dati e benchmark OvernightAH.
In particolare, la sezione [Canonical scheduled benchmarks](#canonical-scheduled-benchmarks)
definisce i risultati ufficiali di `development` e `challenger` da rigenerare
quando cambiano i parametri dei profili.

## Ownership delle schedulazioni

Questa separazione è vincolante anche per nuovi agenti e modifiche future:

- **cron** è l'unica sede delle esecuzioni che possono inviare, cancellare o
  modificare ordini della strategia (`entry`, `exit`, `exit-fallback`);
- **Scheduler / Watchtower** possiede soltanto attività osservabili e
  idempotenti: polling dei fatti Alpaca, watchdog dei run mancati, replay di
  riconciliazione e drift delle baseline;
- nessun job Watchtower può inviare ordini; nessuna strategia può essere
  programmata dalla UI/API Scheduler.

I worker Watchtower recuperano i trigger mancati al riavvio: poll rilegge lo
storico recente, watchdog analizza le sessioni recenti e replay processa la
coda delle giornate chiuse. L'ordine è sempre `poll → watchdog → replay`.

## Roles

| Profile | Checkout | Mode | Purpose |
|---|---|---|---|
| `live` | `backtrader-prod` | live | real production account |
| `mirror` | `backtrader-prod` | paper | same release and parameters as live |
| `challenger` | `backtrader-prod` | paper | production code with alternative parameters |
| `development` | `backtrader` | paper | candidate code or strategy |

Live and mirror deliberately reference the same versioned strategy file. Account
credentials, profile-to-account mapping, logs and locks are outside Git.

## Safe migration

The existing cron entries and `overnight-ah-*` scripts remain unchanged during
initial validation. In particular, the currently commented live entry remains
disabled.

1. Run `scripts/install-scheduled-runtime.sh` in the development checkout.
2. Verify the four generated profiles; create account files manually with mode
   `0600`. Never infer the live account from an old filename.
3. Run `scripts/test-scheduled-jobs.sh`.
4. Run `scripts/scheduled-job.sh --check PROFILE PHASE` for every phase.
5. Run `--dry-run` for every phase and compare the generated commands with the
   legacy scripts.
6. Install `~/bin/bt-scheduled` only after the comparison succeeds.
7. Migrate cron in order: development, mirror, challenger, live. Observe at
   least one complete entry/exit cycle before moving to the next role.
8. Keep the old cron file and legacy scripts for one full release as rollback.

The installed cron interface is:

```cron
40 15 * * 1-5 /home/htpc/bin/bt-scheduled development entry
43 15 * * 1-5 /home/htpc/bin/bt-scheduled mirror entry
38 15 * * 1-5 /home/htpc/bin/bt-scheduled challenger entry
01 01 * * 1-5 /home/htpc/bin/bt-scheduled development exit
01 01 * * 1-5 /home/htpc/bin/bt-scheduled mirror exit
01 01 * * 1-5 /home/htpc/bin/bt-scheduled challenger exit
52 15 * * 1-5 /home/htpc/bin/bt-scheduled development exit-fallback
52 15 * * 1-5 /home/htpc/bin/bt-scheduled mirror exit-fallback
52 15 * * 1-5 /home/htpc/bin/bt-scheduled challenger exit-fallback
15 00 1 * * /home/htpc/backtrader/scripts/refresh-alpaca-calendar-cache.sh
30 23 * * 1-5 /home/htpc/backtrader/scripts/refresh-scheduled-daily-data.sh
```

The live entry remains deliberately disabled; its exit and fallback can remain
installed as defensive cleanup. Redirecting cron output is optional because the
runner writes daily per-profile logs itself.

## Market-data contract

All OvernightAH scheduled profiles evaluate signals with `yahoo_adj`. Adjusted
OHLCV is required on both sides of a live/replay comparison: dividends and
splits must not introduce artificial gaps in indicators. Alpaca remains the
execution venue, so actual fills and position sizes are not expected to equal a
Backtrader simulation; the selected symbol set is expected to equal it.

Entry jobs do not download data. They use a fixed cutoff equal to the previous
calendar day (`DATA_CUTOFF`, default `yesterday`) and pass it to `btmain`. With
`live_use_last_completed_bar=True`, the live run evaluates that last available
completed bar directly. The target is resolved as `t-N` from the shared Alpaca
trading calendar (`config-common/cache/alpaca_calendar_cache.json`), never from
SPY or another ticker in the universe. The entry decision runs once only when
the master and every feed that contains the target session are positioned on
that date. A symbol without that bar is excluded as `stale_feed`, with both its
feed date and the target date logged. Every log line that reports an entry price
also reports `feed_date`. The historical replay reaches the same information
set with the normal one-bar signal lag. Consequently, runs at 09:10 and 16:00
on the same execution date must produce the same candidates.

The Alpaca calendar is maintained independently of Yahoo data by
`scripts/refresh-alpaca-calendar-cache.sh`. The monthly job runs on the first
day at 00:15 Europe/Rome and incrementally appends sessions newly published by
Alpaca. An entry only reads this local cache; it never refreshes the calendar
or downloads symbols.

The scheduled profiles also set
`live_reenter_positions_pending_fallback=True`. A residual Alpaca position from
the preceding overnight does not consume an entry slot and does not exclude its
symbol from the new selection: the entry run still submits the full new `CLS`
quantity. The 15:52 `exit-fallback` independently cancels pending sells and
closes every residual long at market before the close auction. The strategy
emits `ENTRY_EXISTING_POSITION_IGNORED` as a warning whenever this path is used;
an order already submitted by the current strategy run is never duplicated.

The independent 23:30 job refreshes the adjusted daily dataset after the US
session. Set `REFRESH_MARKET_DATA=1` only for an explicit diagnostic run; it is
disabled in every scheduled profile. Before entry, the runner creates a
point-in-time snapshot below
`~/.local/state/backtrader/market-data-snapshots/<profile>/<execution-date>/`.
Parquet files are hard-linked when possible (copied otherwise), while ticker
definitions and indicator panels are copied.

Watchtower reconciliation uses that snapshot and does not access the network by
default. It preserves the frozen history and appends only rows strictly newer
than the snapshot when later bars are needed to complete the replay. The legacy
escape hatch `REPLAY_REFRESH_MARKET_DATA=1` is for diagnostics only. Dates before
snapshot collection was introduced cannot always be reconstructed exactly.

The scheduler lock is shared by the `entry` and `exit` jobs for the same
profile. `exit-fallback` deliberately bypasses that lock: it must immediately
cancel pending sell orders and submit market exits even if `entry` is still
calculating or submitting its BUY CLS orders.

## Canonical scheduled benchmarks

> **Memoria futura:** questi non sono output sperimentali usa-e-getta. Sono il
> riferimento ufficiale con cui confrontare ogni modifica successiva alle due
> schedulazioni. Non creare ogni volta un ID diverso e non confrontare risultati
> ottenuti con vecchi parametri senza dichiararlo esplicitamente.

Every change to the `development` or `challenger` strategy parameters must be
followed by a full-history backtest using the profile's exact `STRATARGS`, data
provider, margin leverage and commission model. The canonical run ID is always
`scheduled_benchmark`, which gives each strategy one stable benchmark directory:

- development: `bt-core/out/overnight_ah/OvernightAH/scheduled_benchmark/`
- challenger: `bt-core/out/overnight_ah_flat_composite/OvernightAHFlatComposite/scheduled_benchmark/`

The benchmark starts on `2000-01-01` and ends on the latest consolidated
session. Development and challenger currently use `commission=alpaca`. Keep
dated `results-*.json` files as audit history; the unqualified `results.json`,
`returns.csv`, `trades.json` and related files represent the latest canonical
benchmark.

In pratica, nello stesso commit che cambia una schedulazione bisogna:

1. rieseguire entrambi i benchmark dal 2000 all'ultima sessione consolidata;
2. usare i parametri effettivi dei file `.env`, senza ricostruirli a mano;
3. aggiornare gli output `scheduled_benchmark` sopra indicati;
4. riportare sempre periodo, esposizione, margine, commissioni, numero di trade,
   ritorno medio per trade, CAGR, Sharpe e massimo drawdown.

## Production promotion

`main` is the effective development branch. Promote reviewed changes to `prod`
without maintaining a second set of configuration edits on the production
machine. The production commit must pin the intended commits of every submodule.

After merging to and checking out `prod` in a clean release worktree:

```bash
scripts/tag-prod-release.sh prod-YYYY.MM.DD-N
scripts/tag-prod-release.sh --push prod-YYYY.MM.DD-N
```

The first invocation is validation only. The second creates and pushes the
annotated tag on the exact `origin/prod` commit.

Deploy from outside the production checkout with:

```bash
/home/htpc/backtrader/scripts/update-prod-checkout.sh /home/htpc/backtrader-prod
```

The update refuses tracked local changes and uses only a fast-forward. It then
checks out the production submodules pinned by the main repository. By default
only `bt-core` is materialized, because the other foundation submodules are not
part of scheduled trading and some historical gitlinks are no longer available
from their remotes. Override this explicit allowlist with `BT_PROD_SUBMODULES`
only after validating the requested commits. No runtime profile or credential
is stored or edited in `backtrader-prod`.

## Rollback

Before each cron migration save `crontab -l` to a timestamped file. Rollback is
performed by restoring that cron file; the legacy scripts remain available.
Code rollback is a new commit on `prod` reverting the bad release, followed by a
new production tag and the normal fast-forward update. Do not modify files
directly in `backtrader-prod`.
