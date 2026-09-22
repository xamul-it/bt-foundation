# Scheduled trading profiles

> **Riferimento operativo importante:** parametri, cron, contratto dati e
> benchmark ufficiali di `development` e `challenger` sono descritti in
> [Scheduled trading operations — Canonical scheduled benchmarks](../../docs/scheduled-trading-operations.md#canonical-scheduled-benchmarks).
> Consultare e rigenerare quei benchmark ogni volta che cambia un file di
> configurazione in questa directory.

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

## OvernightAH data lifecycle

Scheduled entries consume already consolidated previous-day data. They use
`yahoo_adj` for signal generation and set `REFRESH_MARKET_DATA=0`; a separate
post-close job runs `scripts/refresh-scheduled-daily-data.sh` once per weekday.
The entry handler applies `DATA_CUTOFF` (default: yesterday) to both any optional
loader and `btmain`, so intraday execution time cannot change the input bars.

Immediately before a run, `scripts/snapshot-scheduled-data.sh` freezes its
point-in-time inputs under
`~/.local/state/backtrader/market-data-snapshots/<profile>/<date>/`. Watchtower
replay starts from this snapshot and does not refresh market data. The optional
`REPLAY_REFRESH_MARKET_DATA=1` and `REFRESH_MARKET_DATA=1` switches exist only
for explicit diagnostics, not normal scheduling.

Adjusted prices are intentional: live signal evaluation and replay must see the
same dividend/split-continuous series. Alpaca prices are used for execution and
fills, not as a second signal dataset.
