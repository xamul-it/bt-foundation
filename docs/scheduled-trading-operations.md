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

The target cron interface is:

```cron
# Preserve the existing times; only the command changes.
40 21 * * 1-5 /home/htpc/bin/bt-scheduled development entry
30 01 * * 1-5 /home/htpc/bin/bt-scheduled development exit
52 15 * * 1-5 /home/htpc/bin/bt-scheduled development exit-fallback
```

Use analogous commands for `mirror` and finally `live`. Redirecting cron output
is optional because the runner writes daily per-profile logs itself.

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

The update refuses dirty worktrees and uses only a fast-forward. It then checks
out the submodule commits pinned by the main repository. No runtime profile or
credential is stored or edited in `backtrader-prod`.

## Rollback

Before each cron migration save `crontab -l` to a timestamped file. Rollback is
performed by restoring that cron file; the legacy scripts remain available.
Code rollback is a new commit on `prod` reverting the bad release, followed by a
new production tag and the normal fast-forward update. Do not modify files
directly in `backtrader-prod`.
