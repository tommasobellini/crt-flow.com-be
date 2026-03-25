import os
import time
import yfinance as yf
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Setup Supabase
if os.path.exists(".env.local"):
    load_dotenv(".env.local")

url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
key = os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(url, key)

def to_f(val):
    try: return float(val)
    except: return 0.0

def get_exit_reason(sig, df):
    """Calcola l'exit_reason basandosi sui dati storici della candela di chiusura."""
    if df.empty: return "Data Missing"
    
    # Cerchiamo la candela più vicina a closed_at
    closed_at = pd.to_datetime(sig['closed_at'])
    df.index = pd.to_datetime(df.index).tz_localize(None)
    closed_at = closed_at.replace(tzinfo=None)
    
    # Troviamo la riga dove è avvenuta la chiusura
    mask = (df.index <= closed_at)
    if not any(mask): return "Standard Exit"
    
    curr_candle = df[mask].iloc[-1]
    curr_high = to_f(curr_candle['High'])
    curr_low = to_f(curr_candle['Low'])
    curr_close = to_f(curr_candle['Close'])
    curr_open = to_f(curr_candle['Open'])
    
    sl = to_f(sig['stop_loss'])
    tp = to_f(sig['take_profit'])
    entry = to_f(sig.get('entry_price', sig.get('price')))
    s_type = sig['type']
    result = sig['result']
    
    exit_reason_text = "Standard Exit"
    
    if 'bullish' in s_type:
        if result == 'LOSS' or result == 'BREAKEVEN':
            if curr_close > sl: exit_reason_text = "Stop Hunt (Wicked Out)"
            elif curr_high >= entry + (tp - entry) * 0.8: exit_reason_text = "Greed (Missed TP <20%)"
            else: exit_reason_text = "Trend Failure"
            if result == 'BREAKEVEN': exit_reason_text = "Breakeven Secured"
        elif result == 'WIN':
            if curr_low <= entry - (entry - sl) * 0.8: exit_reason_text = "Struggle Hit (Almost Stopped)"
            else: exit_reason_text = "Clean Snipe"
            
    elif 'bearish' in s_type:
        if result == 'LOSS' or result == 'BREAKEVEN':
            if curr_close < sl: exit_reason_text = "Stop Hunt (Wicked Out)"
            elif curr_low <= entry - (entry - tp) * 0.8: exit_reason_text = "Greed (Missed TP <20%)"
            else: exit_reason_text = "Trend Failure"
            if result == 'BREAKEVEN': exit_reason_text = "Breakeven Secured"
        elif result == 'WIN':
            if curr_high >= entry + (sl - entry) * 0.8: exit_reason_text = "Struggle Hit (Almost Stopped)"
            else: exit_reason_text = "Clean Snipe"
            
    return exit_reason_text

def main():
    print("🧹 Avvio Riparazione Autopsie Segnali...")
    res = supabase.table("crt_signals").select("*").eq("is_active", False).is_("exit_reason", "null").execute()
    signals = res.data
    print(f"Trovati {len(signals)} segnali senza autopsia.")
    
    for sig in signals:
        try:
            if not sig.get('closed_at'): continue
            
            ticker = sig['symbol']
            print(f"Riparando {ticker} ({sig['id']})...")
            
            # Download dati orari intorno alla data di chiusura
            # closed_at è in formato '2026-03-24 15:45:00'
            closed_dt = pd.to_datetime(sig['closed_at'])
            start = (closed_dt - pd.Timedelta(days=2)).strftime('%Y-%m-%d')
            end = (closed_dt + pd.Timedelta(days=2)).strftime('%Y-%m-%d')
            
            df = yf.download(ticker, start=start, end=end, interval="1h", progress=False)
            if df.empty: continue
            
            reason = get_exit_reason(sig, df)
            print(f" -> Risultato: {reason}")
            
            supabase.table("crt_signals").update({"exit_reason": reason}).eq("id", sig['id']).execute()
            time.sleep(1) # Rate limit yfinance
            
        except Exception as e:
            print(f"Errore su {sig.get('id')}: {e}")

if __name__ == "__main__":
    main()
