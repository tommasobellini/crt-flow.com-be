import pandas as pd

def to_f(val):
    if hasattr(val, 'iloc'):
        if hasattr(val, 'empty') and val.empty: return 0.0
        v = val.iloc[0]
        if hasattr(v, 'iloc'): v = v.iloc[0]
        return float(v)
    return float(val)

def detect_crt_model_1_logic(df, htf_pools):
    ticker = "TEST"
    tf = "1H"
    pools = htf_pools.get(ticker)
    pdh, pdl, pdr, adr = pools.get("PDH"), pools.get("PDL"), pools.get("PDR"), pools.get("ADR_10")
    pwh, pwl, pmh, pml = pools.get("PWH"), pools.get("PWL"), pools.get("PMH"), pools.get("PML")

    c = df.iloc[-2]
    prev_candle = df.iloc[-3]
    
    c_open, c_close = to_f(c['Open']), to_f(c['Close'])
    c_high, c_low = to_f(c['High']), to_f(c['Low'])
    c_range = c_high - c_low
    
    pc_open, pc_close = to_f(prev_candle['Open']), to_f(prev_candle['Close'])
    pc_high, pc_low = to_f(prev_candle['High']), to_f(prev_candle['Low'])
    pc_body = abs(pc_close - pc_open)

    context_window = df.iloc[-5:-2] # Simplified context
    recent_min = to_f(context_window['Low'].min())
    recent_max = to_f(context_window['High'].max())

    # Bearish
    if recent_max <= c_high:
        level_val, swept_level = None, None
        if c_high > pmh and c_close < pmh: level_val, swept_level = pmh, 'PMH'
        elif c_high > pwh and c_close < pwh: level_val, swept_level = pwh, 'PWH'
        elif c_high > pdh and c_close < pdh: level_val, swept_level = pdh, 'PDH'
        
        # Mini Wick Rule (Short): prev candle must be Green and close 'full' at the top
        if pc_close <= pc_open: level_val = None 
        elif (pc_high - pc_close) > (pc_body * 0.01): level_val = None

        if level_val and c_close < c_open and c_close <= (c_low + c_range * 0.5):
            return f"BEARISH {swept_level}"

    # Bullish
    if recent_min >= c_low:
        level_val, swept_level = None, None
        if c_low < pml and c_close > pml: level_val, swept_level = pml, 'PML'
        elif c_low < pwl and c_close > pwl: level_val, swept_level = pwl, 'PWL'
        elif c_low < pdl and c_close > pdl: level_val, swept_level = pdl, 'PDL'
            
        # Mini Wick Rule (Long): prev candle must be Red and close 'full' at the bottom
        if pc_close >= pc_open: level_val = None
        elif (pc_close - pc_low) > (pc_body * 0.01): level_val = None

        if level_val and c_close > c_open and c_close >= (c_high - c_range * 0.5):
            return f"BULLISH {swept_level}"
    return None

def run_tests():
    htf_pools = {"TEST": {"PDH": 100, "PDL": 90, "PDR": 10, "ADR_10": 10, "PWH": 105, "PWL": 85, "PMH": 110, "PML": 80}}
    
    # 1. Bearish Clean
    df1 = pd.DataFrame([
        {"Open": 95, "High": 96, "Low": 94, "Close": 95}, # -5
        {"Open": 95, "High": 96, "Low": 94, "Close": 95}, # -4
        {"Open": 95, "High": 100.01, "Low": 94, "Close": 100.0}, # -3 Body=5, Wick=0.01 (0.2%)
        {"Open": 100.0, "High": 102, "Low": 98, "Close": 99}, # -2 Sweep 100
        {"Open": 99, "High": 99, "Low": 99, "Close": 99}, # -1
    ])
    print(f"Test 1 (Bearish Clean): {detect_crt_model_1_logic(df1, htf_pools)}")

    # 2. Bearish Dirty
    df2 = pd.DataFrame([
        {"Open": 95, "High": 96, "Low": 94, "Close": 95},
        {"Open": 95, "High": 96, "Low": 94, "Close": 95},
        {"Open": 95, "High": 105, "Low": 94, "Close": 100.0}, # -3 Body=5, Wick=5 (100%)
        {"Open": 100, "High": 106, "Low": 98, "Close": 99}, # -2
        {"Open": 99, "High": 99, "Low": 99, "Close": 99},
    ])
    print(f"Test 2 (Bearish Dirty): {detect_crt_model_1_logic(df2, htf_pools)}")

    # 3. Bullish Clean
    df3 = pd.DataFrame([
        {"Open": 105, "High": 106, "Low": 104, "Close": 105},
        {"Open": 105, "High": 106, "Low": 104, "Close": 105},
        {"Open": 105, "High": 106, "Low": 99.99, "Close": 100.0}, # -3 Body=5, Wick=0.01 (0.2%)
        {"Open": 100.0, "High": 102, "Low": 98, "Close": 101}, # -2 Sweep 100
        {"Open": 101, "High": 101, "Low": 101, "Close": 101},
    ])
    print(f"Test 3 (Bullish Clean): {detect_crt_model_1_logic(df3, htf_pools)}")

    # 4. Bullish Dirty
    df4 = pd.DataFrame([
        {"Open": 105, "High": 106, "Low": 104, "Close": 105},
        {"Open": 105, "High": 106, "Low": 104, "Close": 105},
        {"Open": 105, "High": 106, "Low": 95, "Close": 100.0}, # -3 Body=5, Wick=5 (100%)
        {"Open": 100, "High": 102, "Low": 94, "Close": 101}, # -2
        {"Open": 101, "High": 101, "Low": 101, "Close": 101},
    ])
    print(f"Test 4 (Bullish Dirty): {detect_crt_model_1_logic(df4, htf_pools)}")

if __name__ == "__main__":
    run_tests()
