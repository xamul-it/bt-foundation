# Scheduled trading operations

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
45 15 * * 1-5 /home/htpc/bin/bt-scheduled challenger entry
01 01 * * 1-5 /home/htpc/bin/bt-scheduled development exit
01 01 * * 1-5 /home/htpc/bin/bt-scheduled mirror exit
01 01 * * 1-5 /home/htpc/bin/bt-scheduled challenger exit
52 15 * * 1-5 /home/htpc/bin/bt-scheduled development exit-fallback
52 15 * * 1-5 /home/htpc/bin/bt-scheduled mirror exit-fallback
52 15 * * 1-5 /home/htpc/bin/bt-scheduled challenger exit-fallback
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
completed bar directly. The historical replay reaches the same information set
with the normal one-bar signal lag. Consequently, runs at 09:10 and 16:00 on the
same execution date must produce the same candidates.

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

The scheduler lock is shared by jobs for the same profile. Fallback waits up to
15 minutes for the main phase instead of reporting a successful but empty run
while another phase still owns the lock.

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
