#!/usr/bin/env python3
"""
Test script to verify HMADynamic strategy loads correctly.

This checks:
1. Strategy can be imported
2. Parameters are correct
3. Required methods exist
4. No syntax errors

Usage:
    python bt-strategy-test/HMA/test_hmadynamic_load.py
"""

import sys
import os

# Add project root to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

def test_import():
    """Test that HMADynamic can be imported."""
    print("="*60)
    print("TEST 1: Import HMADynamic")
    print("="*60)

    try:
        from strategies.intraday_hma_dynamic import HMADynamic
        print("✅ HMADynamic imported successfully")
        return HMADynamic
    except ImportError as e:
        print(f"❌ Failed to import HMADynamic: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None


def test_parameters(HMADynamic):
    """Test that all expected parameters exist."""
    print("\n" + "="*60)
    print("TEST 2: Check Parameters")
    print("="*60)

    expected_params = {
        'period': 16,
        'inverted': True,
        'atr_min': 0.007,
        'atr_period': 14,
        'sl_pct': 0.005,
        'target_risk': 0.02,
        'max_positions': 10,
        'reserve_pct': 0.05,
        'opening_start': 9.5,
        'opening_end': 10.0,
        'time_start': 10.0,
        'time_end': 15.0,
        'close_time': 15.5,
        'alloc_high': 1.5,
        'alloc_medium': 1.0,
        'alloc_low': 0.5,
        'opening_high_threshold': 0.68,
        'opening_low_threshold': 0.40,
    }

    all_correct = True
    for param_name, expected_value in expected_params.items():
        actual_value = getattr(HMADynamic.params, param_name, None)
        if actual_value == expected_value:
            print(f"✅ {param_name:25s} = {actual_value}")
        else:
            print(f"❌ {param_name:25s} = {actual_value} (expected {expected_value})")
            all_correct = False

    if all_correct:
        print("\n✅ All parameters correct")
    else:
        print("\n⚠️ Some parameters don't match expected values")

    return all_correct


def test_methods(HMADynamic):
    """Test that required methods exist."""
    print("\n" + "="*60)
    print("TEST 3: Check Methods")
    print("="*60)

    required_methods = [
        'required_minperiod',
        '__init__',
        'prenext',
        'next',
        'get_current_time_decimal',
        'reset_daily_state',
        'analyze_opening',
        'allocate_capital',
        'calculate_position_size',
        'check_stop_loss',
    ]

    all_exist = True
    for method_name in required_methods:
        if hasattr(HMADynamic, method_name):
            print(f"✅ {method_name}")
        else:
            print(f"❌ {method_name} missing")
            all_exist = False

    if all_exist:
        print("\n✅ All methods exist")
    else:
        print("\n⚠️ Some methods are missing")

    return all_exist


def test_attributes(HMADynamic):
    """Test that class attributes are correct."""
    print("\n" + "="*60)
    print("TEST 4: Check Class Attributes")
    print("="*60)

    checks = {
        'live_enabled': (HMADynamic.live_enabled, True),
    }

    all_correct = True
    for attr_name, (actual, expected) in checks.items():
        if actual == expected:
            print(f"✅ {attr_name} = {actual}")
        else:
            print(f"❌ {attr_name} = {actual} (expected {expected})")
            all_correct = False

    if all_correct:
        print("\n✅ All attributes correct")

    return all_correct


def test_required_minperiod(HMADynamic):
    """Test that required_minperiod works correctly."""
    print("\n" + "="*60)
    print("TEST 5: Check required_minperiod()")
    print("="*60)

    try:
        minperiod = HMADynamic.required_minperiod()
        expected = max(16, 14)  # max(period, atr_period)

        if minperiod == expected:
            print(f"✅ required_minperiod() returns {minperiod}")
        else:
            print(f"❌ required_minperiod() returns {minperiod} (expected {expected})")

        # Test with custom parameters
        minperiod_custom = HMADynamic.required_minperiod(period=20, atr_period=30)
        expected_custom = max(20, 30)

        if minperiod_custom == expected_custom:
            print(f"✅ required_minperiod(20, 30) returns {minperiod_custom}")
        else:
            print(f"❌ required_minperiod(20, 30) returns {minperiod_custom} (expected {expected_custom})")

        return minperiod == expected and minperiod_custom == expected_custom

    except Exception as e:
        print(f"❌ Error calling required_minperiod(): {e}")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("HMADYNAMIC STRATEGY LOAD TEST")
    print("="*60 + "\n")

    # Test 1: Import
    HMADynamic = test_import()
    if HMADynamic is None:
        print("\n❌ FAILED: Cannot import strategy")
        return False

    # Test 2: Parameters
    params_ok = test_parameters(HMADynamic)

    # Test 3: Methods
    methods_ok = test_methods(HMADynamic)

    # Test 4: Attributes
    attrs_ok = test_attributes(HMADynamic)

    # Test 5: required_minperiod
    minperiod_ok = test_required_minperiod(HMADynamic)

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    all_tests = [
        ("Import", HMADynamic is not None),
        ("Parameters", params_ok),
        ("Methods", methods_ok),
        ("Attributes", attrs_ok),
        ("required_minperiod", minperiod_ok),
    ]

    for test_name, passed in all_tests:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:20s}: {status}")

    all_passed = all(passed for _, passed in all_tests)

    print("="*60)
    if all_passed:
        print("🎉 ALL TESTS PASSED")
        print("HMADynamic is ready to use!")
    else:
        print("⚠️ SOME TESTS FAILED")
        print("Check errors above and fix before using")
    print("="*60 + "\n")

    return all_passed


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
