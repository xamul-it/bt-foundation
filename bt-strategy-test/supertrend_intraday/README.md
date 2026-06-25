# SuperTrend Intraday Study

This folder contains strategy-specific analysis scripts for the SuperTrend
intraday study. General RTH feature builders and generated artifacts live in
`bt-strategy-test/RTH_analysis`.

Scripts:

- `04_supertrend_meta_filter_study.py`: builds the event dataset by joining
  SuperTrend trades with RTH features and evaluates statistical/model filters.
- `05_supertrend_period_benchmark.py`: compares strategy/filter results against
  the buy-open/sell-close intraday benchmark and computes reusable indicators.
- `06_supertrend_combo_filter_study.py`: tests direction plus strength filter
  combinations, with volume-oriented filters included.

Outputs are written under `bt-strategy-test/supertrend_intraday/out/supertrend_meta_filter`.
