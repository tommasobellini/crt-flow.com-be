import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json

# Mocking the Supabase client and Logger for the test
class MockSupabase:
    def table(self, name):
        return self
    def update(self, payload):
        print(f"[DB MOCK] Updated with: {payload}")
        return self
    def eq(self, field, value):
        return self
    def execute(self):
        return self

class MockLogger:
    def info(self, msg): print(f"[INFO] {msg}")
    def error(self, msg): print(f"[ERROR] {msg}")

logger = MockLogger()
supabase = MockSupabase()

def validate_existing_signals_mock(ticker, df, active_signals):
    """Simplified version of the logic in scanner.py for testing."""
    updates = []
    curr_candle = df.iloc[-1]
    curr_high = float(curr_candle['High'])
    curr_low = float(curr_candle['Low'])
    curr_close = float(curr_candle['Close'])

    for sig in active_signals:
        sl = float(sig['stop_loss'])
        tp = float(sig['take_profit'])
        entry = float(sig.get('entry_price', curr_close))
        s_type = sig['type']
        status = sig.get('status', 'active')
        
        if status == 'pending':
            triggered = False
            missed = False
            
            if 'bullish' in s_type:
                if curr_low <= entry: triggered = True
                elif curr_high >= tp: missed = True
            elif 'bearish' in s_type:
                if curr_high >= entry: triggered = True
                elif curr_low <= tp: missed = True
            
            if triggered:
                print(f"✅ {ticker}: PENDING -> ACTIVE (Entry hit)")
                updates.append({"id": sig['id'], "status": 'active'})
            elif missed:
                print(f"👻 {ticker}: PENDING -> MISSED (Ghost Win Avoided)")
                updates.append({"id": sig['id'], "status": 'missed', "is_active": False, "result": 'MISSED'})
            else:
                print(f"⏳ {ticker}: Still PENDING")
    return updates

# --- TEST CASES ---

# Setup test signal
signal = {
    "id": 1,
    "symbol": "BTCUSD",
    "type": "bullish_wick",
    "status": "pending",
    "entry_price": 50000.0,
    "stop_loss": 49000.0,
    "take_profit": 55000.0,
    "timeframe": "1H"
}

print("\n--- TEST 1: Entry Triggered ---")
df_trigger = pd.DataFrame([{ "High": 51000, "Low": 49500, "Close": 50500 }], index=[datetime.now()])
# Price hits 49500, which is below entry 50000
updates1 = validate_existing_signals_mock("BTCUSD", df_trigger, [signal])
assert any(u['status'] == 'active' for u in updates1)

print("\n--- TEST 2: Ghost Win Avoided (TP hit first) ---")
df_missed = pd.DataFrame([{ "High": 56000, "Low": 51000, "Close": 52000 }], index=[datetime.now()])
# Price hits 56000 (TP) but low is 51000 (Entry 50000 not hit)
updates2 = validate_existing_signals_mock("BTCUSD", df_missed, [signal])
assert any(u['status'] == 'missed' for u in updates2)

print("\n--- TEST 3: Still Pending ---")
df_pending = pd.DataFrame([{ "High": 54000, "Low": 51000, "Close": 52000 }], index=[datetime.now()])
# Price stays between entry and TP
updates3 = validate_existing_signals_mock("BTCUSD", df_pending, [signal])
assert len(updates3) == 0

print("\n✅ Verification script completed successfully!")
