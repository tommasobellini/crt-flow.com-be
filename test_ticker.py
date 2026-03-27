import os
import yfinance as yf
import pandas as pd
import sys

def to_f(val):
    if hasattr(val, 'iloc'):
        if hasattr(val, 'empty') and val.empty: return 0.0
        v = val.iloc[0]
        if hasattr(v, 'iloc'): v = v.iloc[0]
        return float(v)
    return float(val)

def test_ticker(ticker_symbol):
    print(f"🔍 Diagnostic for {ticker_symbol}...")
    
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.history(period="2y", interval="1d") # Reduced from 10y for speed
    
    if df.empty:
        print(f"❌ No data found for {ticker_symbol}")
        return

    tfs = {
        "12M": "YE",   # Annual
        "6M": "6MS",   # 6 Months (Start)
        "3M": "3MS",   # Quarter (Start)
        "1M": "MS"     # Month (Start)
    }

    current_price = to_f(df['Close'].iloc[-1])
    print(f"Current Price: {current_price}")

    for tf_name, tf_code in tfs.items():
        print(f"\n--- Checking Timeframe: {tf_name} ({tf_code}) ---")
        try:
            df_res = df.resample(tf_code).agg({
                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
            }).dropna()
            
            if len(df_res) < 2:
                print(f"⚠️ Not enough candles for {tf_name}")
                continue
                
            wall_c = df_res.iloc[-2] # Last closed candle
            w_o, w_c, w_h, w_l = to_f(wall_c['Open']), to_f(wall_c['Close']), to_f(wall_c['High']), to_f(wall_c['Low'])
            w_body = abs(w_c - w_o)
            if w_body == 0: w_body = 0.001
            
            lower_wick_abs = abs(min(w_o, w_c) - w_l)
            upper_wick_abs = abs(w_h - max(w_o, w_c))
            
            # THE LOGIC (Relaxed per latest feedback)
            # Wall side tolerance: 10% of body
            # Opposite wick requirement: 5% of body
            low_wick_limit = w_body * 0.10
            opp_wick_req = w_body * 0.05
            
            # SL LOGIC: 0.2% buffer
            sl_bull = w_l * 0.998
            sl_bear = w_h * 1.002
            
            print(f"Body: {round(w_body, 2)}")
            print(f"Lower Wick: {round(lower_wick_abs, 2)} (Limit: < {round(low_wick_limit, 2)}) -> {'✅' if is_low_wick_ok else '❌'}")
            print(f"Upper Wick (Opposite): {round(upper_wick_abs, 2)} (Req: > {round(opp_wick_req, 2)}) -> {'✅' if is_opp_wick_ok else '❌'}")
            
            if is_low_wick_ok and is_opp_wick_ok:
                print(f"🎯 MATCH! {ticker_symbol} has a valid Institutional Wall on {tf_name}")
            else:
                if not is_low_wick_ok:
                    print(f"💡 Suggestion: Wick side is too long. Currently {round(lower_wick_abs/w_body*100, 1)}% of body.")
                if not is_opp_wick_ok:
                    print(f"💡 Suggestion: Opposite wick is too short. Currently {round(upper_wick_abs/w_body*100, 1)}% of body.")
                    
        except Exception as e:
            print(f"❌ Error during resampling: {e}")

if __name__ == "__main__":
    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    test_ticker(ticker)
