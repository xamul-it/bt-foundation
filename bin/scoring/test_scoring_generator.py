#!/usr/bin/env python3
"""
Quick test per verificare scoring generator
"""

import json
from generate_scoring_data import ScoringDataGenerator
from datetime import datetime, timedelta

# Test config
test_config = {
    "symbols": ["AAPL"],  # Solo 1 simbolo per test veloce
    "data_folder": "data/yahoo",  # Usa data esistente
    "window_days": 180,
    "min_bars": 120
}

print("="*60)
print("SCORING GENERATOR - QUICK TEST")
print("="*60)

# Inizializza generator
generator = ScoringDataGenerator(test_config)

# Test 1: Paper Trading Mode (oggi)
print("\n[TEST 1] Paper Trading Mode")
print("-"*60)
results = generator.generate_papertrading_data(["AAPL"])

if len(results) > 0:
    print("✓ Paper trading test PASSED")
    print(results.to_string(index=False))
else:
    print("✗ Paper trading test FAILED: no results")

# Test 2: Backtest Mode (ultime 5 date)
print("\n[TEST 2] Backtest Mode (last 5 days)")
print("-"*60)
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

results = generator.generate_backtest_data(
    ["AAPL"],
    start_date,
    end_date
)

if len(results) > 0:
    print(f"✓ Backtest test PASSED ({len(results)} records)")
    print(results.to_string(index=False))
else:
    print("✗ Backtest test FAILED: no results")

print("\n" + "="*60)
print("TEST COMPLETED")
print("="*60)
print("\nIf tests passed, you can run full generation:")
print("  python3 generate_scoring_data.py --mode backtest --config config_scoring.json")
print("  python3 generate_scoring_data.py --mode papertrading --config config_scoring.json")
