# Scheduled trading profiles

The repository contains versioned strategy parameters and profile templates.
Machine-specific profiles and account credentials are installed outside Git:

```text
~/.config/backtrader/
├── scheduled/{live,mirror,challenger,development}.env
└── accounts/{live,mirror,challenger,development}.env
```

Runtime logs and locks are written below `~/.local/state/backtrader/`.

Run validation without placing orders:

```bash
scripts/scheduled-job.sh --check development entry
scripts/scheduled-job.sh --dry-run development entry
```

The public interface is always `PROFILE PHASE`; strategy names do not appear in
cron. Supported phases are `entry`, `exit`, and `exit-fallback`.
