#!/usr/bin/env bash
# auc paper: dynamic strategy test, entry market/GTC before close.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export PSIM_ENV=overnight-ah-auc.key
export AUCTION=False
export TRADING_MODE=paper
export TICKER=yahoo_adj_research_universe.json
export STRAT=overnight_ah.OvernightAH
export DATA_PROVIDER=yahoo
export MAX_EXPOSURE=2
export MARGIN_LEVERAGE=2
export FROM_DAYS=420
export RUN_ID=overnight_ah_dev
export STRATARGS="max_concurrent=5 size_by_max_concurrent=True max_exposure=$MAX_EXPOSURE min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 min_adv=100000000 auction=$AUCTION monthly_universe_mode='weak_theme_switch' monthly_universe_top_n=50 monthly_universe_base_weight=0.85 monthly_universe_theme_weight=0.15 monthly_universe_theme_score='corr12' monthly_universe_switch_feature='semis_total_3m' monthly_universe_switch_threshold=0.0 monthly_universe_spy_dd3m_threshold=-0.10 post_up_cooldown_threshold=0.05 post_up_cooldown_days=5 risk_overlay_mode='off' risk_overlay_lookback=10 risk_overlay_threshold=-0.10 risk_overlay_stress_exposure=1.25"

exec "$SCRIPT_DIR/overnight-ah-entry.sh" "$@"
