import os
import time
import json
import logging
import argparse
import yfinance as yf
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Import our detection logic from the main scanner
from scanner import detect_macro_sweep, detect_tbs_setup

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backtest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- SUPABASE CONFIG ---
if os.path.exists(".env.local"):
    load_dotenv(".env.local")
    logger.info("Loaded credentials from .env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase connection established for backtesting.")
    except Exception as e:
        logger.error(f"Failed to connect to Supabase: {e}")

def run_backtest(ticker, start_date="2023-01-01", end_date=None, save_to_db=False):
    """
    Simulates the scanner moving through time to detect setups and validate outcomes.
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
        
    logger.info(f"Starting Backtest for {ticker} from {start_date} to {end_date}...")
    
    # Download full history (Daily)
    # We download a bit more than start_date to have lookback for resamples
    full_df = yf.download(ticker, start="2022-01-01", end=end_date, interval="1d", progress=False)
    full_df = full_df.dropna()
    
    if full_df.empty or len(full_df) < 100:
        logger.warning(f"Not enough data for {ticker}")
        return []

    # Filter to start processing from requested start_date
    process_start_idx = full_df.index.get_indexer([pd.to_datetime(start_date)], method='bfill')[0]
    if process_start_idx < 100: process_start_idx = 100 # Ensure lookback for HTF levels

    historical_signals = []
    
    # SCANNING LOOP
    for i in range(process_start_idx, len(full_df)):
        # Slide through time: the algorithm only "knows" data up to 'i'
        df_slice = full_df.iloc[:i+1] # Include the current candle
        current_candle = df_slice.iloc[-1]
        current_date_str = df_slice.index[-1].strftime('%Y-%m-%d')
        
        # 1. Check Macro Sweeps
        signal = detect_macro_sweep(ticker, df_slice, "1D")
        
        # 2. Check TBS if no sweep found (or check both)
        if not signal:
            signal = detect_tbs_setup(ticker, df_slice, "1D")
            
        if signal:
            # SIGNAL DETECTED!
            logger.info(f"[{current_date_str}] SIGNAL: {ticker} {signal['type']} @ {signal['price']}")
            
            # Outcome Validation: Look into the "future" (remaining df)
            future_df = full_df.iloc[i+1:]
            result = "OPEN"
            closed_at = None
            
            sl = signal['stop_loss']
            tp = signal['take_profit']
            
            for f_idx, future_candle in future_df.iterrows():
                # Extract scalars to avoid FutureWarnings and ambiguity
                def get_val(row, col):
                    val = row[col]
                    if hasattr(val, 'iloc'): return float(val.iloc[0])
                    return float(val)

                f_high = get_val(future_candle, 'High')
                f_low = get_val(future_candle, 'Low')
                
                if 'bearish' in signal['type']:
                    if f_high >= sl:
                        result = 'LOSS'
                        closed_at = f_idx
                        break
                    elif f_low <= tp:
                        result = 'WIN'
                        closed_at = f_idx
                        break
                elif 'bullish' in signal['type']:
                    if f_low <= sl:
                        result = 'LOSS'
                        closed_at = f_idx
                        break
                    elif f_high >= tp:
                        result = 'WIN'
                        closed_at = f_idx
                        break
            
            # Update signal with backtest results
            signal['result'] = result
            signal['is_active'] = False if result != "OPEN" else True
            signal['created_at'] = f"{current_date_str} 10:00:00" # Standardized time
            
            if closed_at:
                signal['closed_at'] = closed_at.strftime('%Y-%m-%d %H:%M:%S')
            
            # If outcome is closed, we add it to our record
            if result != "OPEN":
                historical_signals.append(signal)
                # Skip forward in the scanning loop to avoid detecting multiples of the same move?
                # Optional: i = full_df.index.get_loc(closed_at)
    
    logger.info(f"Completed {ticker}: Found {len(historical_signals)} signals.")
    return historical_signals

def main():
    parser = argparse.ArgumentParser(description='CRT Flow Backtester')
    parser.add_argument('--tickers', type=str, default="AAPL,MSFT,NVDA,META,AMZN,TSLA,GOOGL", help='Comma separated tickers')
    parser.add_argument('--start', type=str, default="2023-01-01", help='Start date YYYY-MM-DD')
    parser.add_argument('--save', action='store_true', help='Save results to Supabase')
    args = parser.parse_args()

    ticker_list = [t.strip().upper() for t in args.tickers.split(",")]
    
    all_results = []
    for t in ticker_list:
        results = run_backtest(t, start_date=args.start)
        all_results.extend(results)
        
    if not all_results:
        print("No signals found across selected tickers.")
        return

    # FINAL STATISTICS
    wins = len([s for s in all_results if s['result'] == 'WIN'])
    losses = len([s for s in all_results if s['result'] == 'LOSS'])
    total = wins + losses
    
    winrate = (wins / total * 100) if total > 0 else 0
    
    print("\n" + "="*30)
    print("BACKTEST SUMMARY")
    print(f"Total Signals: {len(all_results)}")
    print(f"Wins: {wins}")
    print(f"Losses: {losses}")
    print(f"Winrate: {winrate:.2f}%")
    print(f"Avg RR: 1:3 (Calculated Fixed)")
    print("="*30 + "\n")

    # SAVE TO DATABASE
    if args.save and supabase:
        print(f"Saving {len(all_results)} signals to Supabase...")
        try:
            # Batch insert
            for i in range(0, len(all_results), 1000):
                batch = all_results[i : i + 1000]
                supabase.table("crt_signals").insert(batch).execute()
            print("Successfully populated the Trade Graveyard!")
        except Exception as e:
            print(f"Error saving to DB: {e}")
    else:
        # Save to local file as fallback/review
        with open("backtest_results.json", "w") as f:
            json.dump(all_results, f, indent=2, default=str)
        print("Results saved to backtest_results.json (Local).")

if __name__ == "__main__":
    main()
