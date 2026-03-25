import pandas as pd
import numpy as np
import sys
import os

# Add current directory to path so we can import scanner
sys.path.append(os.getcwd())

# Mock to_f if needed or just use scanner's
try:
    from scanner import detect_crt_model_1, to_f
except ImportError as e:
    print(f"Error importing from scanner: {e}")
    sys.exit(1)

def test_bearish_mini_wick():
    print("--- Testing Bearish Mini Wick Rule ---")
    
    # Setup mock pools
    htf_pools = {
        "TEST": {
            "PDH": 100, "PDL": 90, "PDR": 10, "ADR_10": 10,
            "PWH": 105, "PWL": 85, "PMH": 110, "PML": 80
        }
    }
    
    # Scenario 1: Clean Bearish Setup (Prev candle Green, no upper wick)
    # Body = 100 - 95 = 5. Upper Wick = 100.01 - 100 = 0.01. 0.01/5 = 0.2% (< 1%) -> PASS
    df = pd.DataFrame([
        {"Open": 95, "High": 96, "Low": 94, "Close": 95}, # -5
        {"Open": 95, "High": 96, "Low": 94, "Close": 95}, # -4
        {"Open": 95, "High": 100.01, "Low": 94, "Close": 100.0}, # -3 (Prev Candle)
        {"Open": 100.0, "High": 102, "Low": 98, "Close": 99}, # -2 (Trigger Candle) Sweep PDH
        {"Open": 99, "High": 99, "Low": 99, "Close": 99}, # -1 (Current)
    ])
    
    sig = detect_crt_model_1("TEST", df, "1H", htf_pools)
    if sig:
        print("✅ Scenario 1 (Clean): Signal detected as expected.")
    else:
        print("❌ Scenario 1 (Clean): Signal NOT detected.")

    # Scenario 2: Rejected Bearish Setup (Prev candle Green, LARGE upper wick)
    # Body = 100 - 95 = 5. Upper Wick = 105 - 100 = 5. 5/5 = 100% (> 1%) -> REJECT
    df2 = pd.DataFrame([
        {"Open": 95, "High": 96, "Low": 94, "Close": 95}, # -5
        {"Open": 95, "High": 96, "Low": 94, "Close": 95}, # -4
        {"Open": 95, "High": 105, "Low": 94, "Close": 100.0}, # -3 (Prev Candle)
        {"Open": 100, "High": 106, "Low": 98, "Close": 99}, # -2 (Trigger Candle) Sweep PDH
        {"Open": 99, "High": 99, "Low": 99, "Close": 99}, # -1 (Current)
    ])
    
    sig2 = detect_crt_model_1("TEST", df2, "1H", htf_pools)
    if not sig2:
        print("✅ Scenario 2 (Dirty): Signal rejected as expected.")
    else:
        print("❌ Scenario 2 (Dirty): Signal WAS detected (failure).")

def test_bullish_mini_wick():
    print("\n--- Testing Bullish Mini Wick Rule ---")
    
    # Setup mock pools
    htf_pools = {
        "TEST": {
            "PDH": 110, "PDL": 100, "PDR": 10, "ADR_10": 10,
            "PWH": 115, "PWL": 95, "PMH": 120, "PML": 90
        }
    }
    
    # Scenario 3: Clean Bullish Setup (Prev candle Red, small lower wick)
    # Body = 105 - 100 = 5. Lower Wick = 100 - 99.99 = 0.01. 0.01/5 = 0.2% (< 1%) -> PASS
    df3 = pd.DataFrame([
        {"Open": 105, "High": 106, "Low": 104, "Close": 105}, # -5
        {"Open": 105, "High": 106, "Low": 104, "Close": 105}, # -4
        {"Open": 105, "High": 106, "Low": 99.99, "Close": 100.0}, # -3 (Prev Candle)
        {"Open": 100.0, "High": 102, "Low": 98, "Close": 101}, # -2 (Trigger Candle) Sweep PDL (100)
        {"Open": 101, "High": 101, "Low": 101, "Close": 101}, # -1 (Current)
    ])
    
    sig3 = detect_crt_model_1("TEST", df3, "1H", htf_pools)
    if sig3:
        print("✅ Scenario 3 (Clean): Signal detected as expected.")
    else:
        print("❌ Scenario 3 (Clean): Signal NOT detected.")

    # Scenario 4: Rejected Bullish Setup (Prev candle Red, LARGE lower wick)
    # Body = 105 - 100 = 5. Lower Wick = 100 - 95 = 5. 5/5 = 100% (> 1%) -> REJECT
    df4 = pd.DataFrame([
        {"Open": 105, "High": 106, "Low": 104, "Close": 105}, # -5
        {"Open": 105, "High": 106, "Low": 104, "Close": 105}, # -4
        {"Open": 105, "High": 106, "Low": 95, "Close": 100.0}, # -3 (Prev Candle)
        {"Open": 100, "High": 102, "Low": 94, "Close": 101}, # -2 (Trigger Candle) Sweep PDL
        {"Open": 101, "High": 101, "Low": 101, "Close": 101}, # -1 (Current)
    ])
    
    sig4 = detect_crt_model_1("TEST", df4, "1H", htf_pools)
    if not sig4:
        print("✅ Scenario 4 (Dirty): Signal rejected as expected.")
    else:
        print("❌ Scenario 4 (Dirty): Signal WAS detected (failure).")

if __name__ == "__main__":
    test_bearish_mini_wick()
    test_bullish_mini_wick()
