# Live execution prices — 2026-10-10

Keep the current session's Backtest entry price absent: COC may see an
incomplete daily candle. Recover the original entry's session Close on a
later day, before settlement of its exit. Backtest forces auction for the
two supported OvernightAH strategies; a stored auction=false must not
prevent this recovery. Never use decision prices or replay entry selection.

The existing dashboard scheduler invokes the corrected script with
--catch-up --settle-only. Include historical entries with missing prices,
even if their exits have already been recorded. No evening job was added.

Live repair only: six missing Backtest entry prices on October 8 and 9
were recovered from their own daily bars. October 8's three closed trades
had P&L recomputed; October 9's three positions remain pending. Original
decisions, quantities and Alpaca records were not updated by this repair.
Backup: backup/reconciliation-repair/live-cls-entry-prices-before-20261010.json.

Summary percentage is total daily P&L divided by the invested capital
(sum of execution entry price times executed quantity), multiplied by 100.
It is not an arithmetic mean and does not depend on a comparison record.
The arithmetic mean initially published was corrected at the user's request.

Verification: 8 closure tests and 15 UI tests passed; production PWA built.
