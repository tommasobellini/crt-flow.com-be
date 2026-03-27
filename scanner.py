import os
import argparse
import csv
import logging
import time
import io
import requests
import json
import yfinance as yf
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

import sys
import concurrent.futures

# --- 1. CONFIGURAZIONE LOGGING ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')

class SupabaseLoggingHandler(logging.Handler):
    def __init__(self, supabase_client):
        super().__init__()
        self.supabase = supabase_client
        self.source = "scanner_new_engine"

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if "system_logs" in log_entry: return
            self.supabase.table("system_logs").insert({
                "level": record.levelname, "message": log_entry, "source": self.source
            }).execute()
        except: pass

def setup_logging():
    file_handler = logging.FileHandler("scanner_new.log", encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if sys.platform == "win32":
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
        except:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# --- 2. SETUP SUPABASE ---
supabase = None
def setup_supabase():
    global supabase
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")

    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    if url and key:
        try:
            supabase = create_client(url, key)
            sb_handler = SupabaseLoggingHandler(supabase)
            sb_handler.setFormatter(formatter)
            logger.addHandler(sb_handler)
        except Exception as e:
            print(f"Errore Supabase: {e}")

# --- 3. TIMEFRAME E TICKER LISTS ---
TF_CONFIG = {
    "1H": {"period": "60d", "interval": "1h"}
}

def get_sp500_tickers():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        table = pd.read_html(io.StringIO(response.text))
        tickers = table[0]['Symbol'].tolist()
        return [t.replace('.', '-') for t in tickers if isinstance(t, str) and len(t) <= 8 and ' ' not in t]
    except Exception as e:
        logger.error(f"Errore SP500: {e}")
        return ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]

def get_nasdaq100_tickers():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        table = pd.read_html(io.StringIO(response.text))
        for t in table:
            if 'Ticker' in t.columns:
                return [x.replace('.', '-') for x in t['Ticker'].tolist() if isinstance(x, str)]
            elif 'Symbol' in t.columns:
                return [x.replace('.', '-') for x in t['Symbol'].tolist() if isinstance(x, str)]
        return []
    except Exception as e:
        logger.error(f"Errore NASDAQ: {e}")
        return []

def get_forex_tickers():
    return ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X"]

def get_crypto_tickers():
    return ["BTC-USD", "ETH-USD", "SOL-USD"]

# --- 4. UTILS ---
def to_f(val):
    if hasattr(val, 'iloc'):
        if hasattr(val, 'empty') and val.empty: return 0.0
        v = val.iloc[0]
        if hasattr(v, 'iloc'): v = v.iloc[0]
        return float(v)
    return float(val)

def clean_df(df):
    if df is None or df.empty: return df
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(-1)
    if any(isinstance(c, tuple) for c in df.columns):
        df.columns = [c[-1] if isinstance(c, tuple) else c for c in df.columns]
    
    new_cols = []
    for c in df.columns:
        c_str = str(c).strip().lower()
        if c_str == 'open': new_cols.append('Open')
        elif c_str == 'high': new_cols.append('High')
        elif c_str == 'low': new_cols.append('Low')
        elif c_str == 'close': new_cols.append('Close')
        elif c_str == 'volume': new_cols.append('Volume')
        elif 'adj' in c_str: new_cols.append('Adj Close')
        else: new_cols.append(str(c).strip())
    df.columns = new_cols
    df = df.loc[:, ~df.columns.duplicated()]
    return df

# --- 5. PRE-FETCH LIQUIDITY & ADR FILTER ---
LIQUIDITY_CACHE = {}

def prefetch_all_htf_liquidity(tickers):
    global LIQUIDITY_CACHE
    logger.info(f"🌊 Analisi HTF Walls (D/W/M) per {len(tickers)} ticker...")

    try:
        data = yf.download(tickers, period="12mo", interval="1d", group_by='ticker', progress=False, threads=True)
        if data.empty: return

        for ticker in tickers:
            try:
                df = data[ticker] if len(tickers) > 1 else data
                df = clean_df(df.dropna())
                if df.empty or len(df) < 30: continue

                # --- LIVELLI DAILY ---
                d_prev = df.iloc[-2]
                pdh, pdl = to_f(d_prev['High']), to_f(d_prev['Low'])
                d_open, d_close = to_f(d_prev['Open']), to_f(d_prev['Close'])
                d_body = abs(d_close - d_open)
                if d_body == 0: d_body = 0.001
                
                # PDH Perfect Wall: Green, Upper Wick < 10%, Lower Wick > 5%
                pdh_wall = (d_close > d_open) and ((to_f(d_prev['High']) - d_close) < (d_body * 0.10)) and ((d_open - to_f(d_prev['Low'])) > (d_body * 0.05))
                # PDL Perfect Wall: Red, Lower Wick < 10%, Upper Wick > 5%
                pdl_wall = (d_close < d_open) and ((to_f(d_prev['Low']) - d_close) < (d_body * 0.10)) and ((to_f(d_prev['High']) - d_open) > (d_body * 0.05))

                # --- LIVELLI WEEKLY ---
                weekly = df.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
                w_prev = weekly.iloc[-2] if len(weekly) >= 2 else weekly.iloc[-1]
                pwh, pwl = to_f(w_prev['High']), to_f(w_prev['Low'])
                w_open, w_close = to_f(w_prev['Open']), to_f(w_prev['Close'])
                w_body = abs(w_close - w_open)
                if w_body == 0: w_body = 0.001
                pwh_wall = (w_close > w_open) and ((to_f(w_prev['High']) - w_close) < (w_body * 0.10)) and ((w_open - to_f(w_prev['Low'])) > (w_body * 0.05))
                pwl_wall = (w_close < w_open) and ((to_f(w_prev['Low']) - w_close) < (w_body * 0.10)) and ((to_f(w_prev['High']) - w_open) > (w_body * 0.05))

                # --- LIVELLI MONTHLY ---
                monthly = df.resample('M').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
                m_prev = monthly.iloc[-2] if len(monthly) >= 2 else monthly.iloc[-1]
                pmh, pml = to_f(m_prev['High']), to_f(m_prev['Low'])
                m_open, m_close = to_f(m_prev['Open']), to_f(m_prev['Close'])
                m_body = abs(m_close - m_open)
                if m_body == 0: m_body = 0.001
                pmh_wall = (m_close > m_open) and ((to_f(m_prev['High']) - m_close) < (m_body * 0.10)) and ((m_open - to_f(m_prev['Low'])) > (m_body * 0.05))
                pml_wall = (m_close < m_open) and ((to_f(m_prev['Low']) - m_close) < (m_body * 0.10)) and ((to_f(m_prev['High']) - m_open) > (m_body * 0.05))

                # ADR per filtro volatilità
                last_10_days = df.iloc[-12:-2]
                adr_10 = (last_10_days['High'] - last_10_days['Low']).mean()

                LIQUIDITY_CACHE[ticker] = {
                    "PDH": pdh, "PDL": pdl, "PDH_WALL": pdh_wall, "PDL_WALL": pdl_wall,
                    "PDH_CANDLE": {"t": str(d_prev.name), "o": d_open, "h": pdh, "l": pdl, "c": d_close},
                    "PDL_CANDLE": {"t": str(d_prev.name), "o": d_open, "h": pdh, "l": pdl, "c": d_close},
                    "PWH": pwh, "PWL": pwl, "PWH_WALL": pwh_wall, "PWL_WALL": pwl_wall,
                    "PWH_CANDLE": {"t": str(w_prev.name), "o": w_open, "h": pwh, "l": pwl, "c": w_close},
                    "PWL_CANDLE": {"t": str(w_prev.name), "o": w_open, "h": pwh, "l": pwl, "c": w_close},
                    "PMH": pmh, "PML": pml, "PMH_WALL": pmh_wall, "PML_WALL": pml_wall,
                    "PMH_CANDLE": {"t": str(m_prev.name), "o": m_open, "h": pmh, "l": pml, "c": m_close},
                    "PML_CANDLE": {"t": str(m_prev.name), "o": m_open, "h": pmh, "l": pml, "c": m_close},
                    "ADR_10": adr_10, "PDR": pdh - pdl
                }
            except Exception: continue
        logger.info(f"✅ HTF Walls e ADR calcolati per {len(LIQUIDITY_CACHE)} ticker.")
    except Exception as e:
        logger.error(f"Errore prefetch HTF: {e}")

# --- 6. AGGIORNAMENTO SEGNALI ATTIVI (Monitoring & Autopsy) ---
def validate_existing_signals(ticker, df, active_signals_map):
    updates = []
    if ticker not in active_signals_map: return updates

    signals = active_signals_map[ticker]
    curr_candle = df.iloc[-1]
    curr_high = to_f(curr_candle['High'])
    curr_low = to_f(curr_candle['Low'])
    curr_close = to_f(curr_candle['Close'])
    curr_open = to_f(curr_candle['Open'])

    for sig in signals:
        sl = to_f(sig.get('stop_loss', 0))
        tp = to_f(sig.get('take_profit', 0))
        entry = to_f(sig.get('entry_price', sig.get('price')))
        s_type = sig.get('type', '')
        status = sig.get('status', 'active')
        
        # Filter out new signals (Safe Zone / Breathing Room):
        # Give the trade at least 5 minutes to "breathe" before monitoring exits.
        try:
            created_at = pd.to_datetime(sig.get('created_at'))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=pd.Timestamp.now(tz='UTC').tzinfo)
            if (pd.Timestamp.now(tz='UTC') - created_at).total_seconds() < 300:
                continue
        except: pass

        # Protezione d'urgenza: se SL o TP sono zero, non validiamo l'uscita
        if sl == 0 or tp == 0:
            if status != 'watchlist':
                logger.warning(f"⚠️ {ticker} [{sig['timeframe']}]: Segnale saltato per validazione (SL o TP a zero).")
            continue
            
        # Protezione Monitoraggio: Validiamo solo i segnali effettivamente in gioco
        if status not in ['active', 'pending']:
            continue
        
        if status == 'pending':
            triggered, missed = False, False
            if 'bullish' in s_type:
                if curr_low <= entry: triggered = True
                elif curr_high >= tp: missed = True
            elif 'bearish' in s_type:
                if curr_high >= entry: triggered = True
                elif curr_low <= tp: missed = True
            
            if triggered:
                logger.info(f"⚡ {ticker} [{sig['timeframe']}]: Limit Order ESEGUITO @ {entry}")
                updates.append({"id": sig['id'], "status": 'active'})
                continue 
            elif missed:
                logger.info(f"👻 {ticker} [{sig['timeframe']}]: Ghost Win EVITATA.")
                updates.append({
                    "id": sig['id'], "is_active": False, "status": 'missed', "result": 'MISSED',
                    "closed_at": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
                })
                continue
            else: continue

        should_expire = False
        reason = ""
        exit_reason_text = "Standard Exit"
        new_sl = sl

        if 'bullish' in s_type:
            # Breakeven Trailing logic
            if curr_high >= entry + (tp - entry) * 0.5 and sl < entry:
                new_sl = entry
                updates.append({"id": sig['id'], "stop_loss": entry})
            
            if curr_low <= new_sl:
                should_expire = True
                reason = "STOPPED" if new_sl != entry else "BREAKEVEN"
                if reason == "STOPPED":
                    if curr_close > new_sl: exit_reason_text = "Stop Hunt (Wicked Out)"
                    elif curr_high >= entry + (tp - entry) * 0.8: exit_reason_text = "Greed (Missed TP <20%)"
                    else: exit_reason_text = "Trend Failure"
                else: exit_reason_text = "Breakeven Secured"
            elif curr_high >= tp:
                should_expire = True
                reason = "PROFIT"
                if curr_low <= entry - (entry - sl) * 0.8: exit_reason_text = "Struggle Hit (Almost Stopped)"
                else: exit_reason_text = "Clean Snipe"
        
        elif 'bearish' in s_type:
            # Breakeven Trailing logic
            if curr_low <= entry - (entry - tp) * 0.5 and sl > entry:
                new_sl = entry
                updates.append({"id": sig['id'], "stop_loss": entry})
            
            if curr_high >= new_sl:
                should_expire = True
                reason = "STOPPED" if new_sl != entry else "BREAKEVEN"
                if reason == "STOPPED":
                    if curr_close < new_sl: exit_reason_text = "Stop Hunt (Wicked Out)"
                    elif curr_low <= entry - (entry - tp) * 0.8: exit_reason_text = "Greed (Missed TP <20%)"
                    else: exit_reason_text = "Trend Failure"
                else: exit_reason_text = "Breakeven Secured"
            elif curr_low <= tp:
                should_expire = True
                reason = "PROFIT"
                if curr_high >= entry + (sl - entry) * 0.8: exit_reason_text = "Struggle Hit (Almost Stopped)"
                else: exit_reason_text = "Clean Snipe"
        
        if should_expire:
            result_code = 'WIN' if reason == 'PROFIT' else 'LOSS' if reason == 'STOPPED' else 'BREAKEVEN'
            logger.info(f"✅ Segnale CONCLUSO per {ticker}: {result_code} ({exit_reason_text})")
            updates.append({
                "id": sig['id'], "is_active": False, "result": result_code,
                "exit_reason": exit_reason_text,
                "closed_at": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            })
    return updates

# --- 7. LOGICA PURE CRT MODEL #1 ---
def create_pure_crt_signal(ticker, tf, s_type, subtype, high, low, entry, sl, tp, diamond_score, swept_level, wall_candle_data):
    rr_ratio = 0
    if abs(entry - sl) > 0:
        rr_ratio = abs(entry - tp) / abs(entry - sl)
    
    # Surgical metadata for chart highlighting
    trigger_metadata = {
        "wall_candle_time": wall_candle_data.get('t'),
        "wall_high": wall_candle_data.get('h'),
        "wall_low": wall_candle_data.get('l'),
        "opposite_wick_limit": tp,
        "wall_coords": {
            "time": wall_candle_data.get('t'),
            "open": wall_candle_data.get('o'),
            "high": wall_candle_data.get('h'),
            "low": wall_candle_data.get('l'),
            "close": wall_candle_data.get('c')
        }
    }

    return {
        "symbol": ticker, "timeframe": tf, "type": s_type, "subtype": subtype,
        "range_high": round(high, 2), "range_low": round(low, 2),
        "price": round(entry, 2), "entry_price": round(entry, 2),
        "status": "active", "is_active": True, "result": None,
        "stop_loss": round(sl, 2), "take_profit": round(tp, 2),
        "rr_ratio": round(rr_ratio, 1),
        "liquidity_tier": f"{swept_level} Sweep", "session_tag": "Market Order",
        "diamond_score": diamond_score, "confluence_level": "CRT Model #1",
        "has_divergence": False, "seasonality_score": 0, "seasonality_data": "{}",
        "fvg_detected": False, "hitting_fvg": False, "smt_divergence": False, "adr_percent": 0,
        "rel_volume": 0, "volatility_warning": False, "is_golden_wick": False, "touches": 1,
        "market_bias": None, "max_favorable_excursion": 0.0, 
        "trigger_candles": json.dumps(trigger_metadata) # Packing everything here for the chart
    }

def create_watchlist_signal(ticker, tf, tier, level_val, dist, wall_candle=None):
    trigger_metadata = {}
    is_bearish = "H" in tier # PDH, PWH, PMH
    
    tp = 0
    sl = 0
    if wall_candle:
        trigger_metadata = {
            "wall_coords": {
                "time": wall_candle.get('t'),
                "open": wall_candle.get('o'),
                "high": wall_candle.get('h'),
                "low": wall_candle.get('l'),
                "close": wall_candle.get('c')
            }
        }
        # Take Profit: Opposite Wick of HTF source candle
        tp = wall_candle['l'] if is_bearish else wall_candle['h']
        # Preliminary Stop Loss: 0.5% safety buffer behind the wall
        sl = level_val * 1.005 if is_bearish else level_val * 0.995

    return {
        "symbol": ticker, "timeframe": tf, "type": "bearish_tbs" if is_bearish else "bullish_tbs", 
        "subtype": f"Approaching {tier}", "price": round(level_val, 2),
        "entry_price": round(level_val, 2),
        "stop_loss": round(sl, 2),
        "take_profit": round(tp, 2),
        "status": "watchlist", "is_active": True,
        "liquidity_tier": tier, "diamond_score": "PRE-ALERT",
        "confluence_level": f"Dist: {round(dist*100, 2)}%",
        "trigger_candles": json.dumps(trigger_metadata)
    }

def update_signal_lifecycle(ticker, df, tf, htf_pools):
    if tf != '1H': return
    df = clean_df(df)
    if df is None or len(df) < 5: return
    pools = htf_pools.get(ticker)
    if not pools: return

    current_price = to_f(df['Close'].iloc[-1])
    
    levels_to_check = [
        (pools["PMH"], "Monthly Wall", "PMH", "bearish"), 
        (pools["PML"], "Monthly Wall", "PML", "bullish"),
        (pools["PWH"], "Weekly Wall", "PWH", "bearish"), 
        (pools["PWL"], "Weekly Wall", "PWL", "bullish"),
        (pools["PDH"], "Daily Wall", "PDH", "bearish"), 
        (pools["PDL"], "Daily Wall", "PDL", "bullish")
    ]

    for lv_val, lv_name, code, l_type in levels_to_check:
        is_perfect = pools.get(f"{code}_WALL")
        dist = abs(current_price - lv_val) / lv_val
        
        # 0. TRACE LOGGING
        if is_perfect:
             logger.info(f"🔍 Checking {ticker} - Price: {current_price} vs {code}: {lv_val} (Wall: {is_perfect})")

        # FASE 3: ENTRY (CRT Model #1) - RECLAIM (HISTORICAL SEARCH)
        if is_perfect:
            # Look back 24 candles to find a Reclaim
            lookback = min(24, len(df) - 1)
            for i in range(1, lookback + 1):
                c = df.iloc[-i]
                c_open, c_close = to_f(c['Open']), to_f(c['Close'])
                c_high, c_low = to_f(c['High']), to_f(c['Low'])
                c_range = c_high - c_low
                if c_range == 0: c_range = 0.001
                
                setup = None
                # RECLAIM BEARISH (SHORT): Sweep della High, Chiusura SOTTO, Candela ROSSA
                if l_type == "bearish" and c_high > lv_val and c_close < lv_val:
                    if c_close < c_open: # Candela ROSSA
                        # Simplified Rule: Just check if it closed in the lower half
                        if c_close <= (c_low + c_range * 0.6): # Relaxed to 0.6
                            setup = ("bearish_tbs", f"{lv_name} Sweep", "A+", lv_val, code)
                
                # RECLAIM BULLISH (LONG): Sweep della Low, Chiusura SOPRA, Candela VERDE
                elif l_type == "bullish" and c_low < lv_val and c_close > lv_val:
                    if c_close > c_open: # Candela VERDE
                        # Simplified Rule: Just check if it closed in the upper half
                        if c_close >= (c_high - c_range * 0.6): # Relaxed to 0.6
                            setup = ("bullish_tbs", f"{lv_name} Sweep", "A+", lv_val, code)
                
                if setup:
                    s_type, s_sub, d_score, lv, tier_code = setup
                    entry = c_close
                    # Relaxed Stop Loss: 0.2% margin instead of 0.01%
                    sl = (c_high * 1.002) if l_type == "bearish" else (c_low * 0.998)
                    wall_candle = pools.get(f"{tier_code}_CANDLE")
                    tp = wall_candle['l'] if l_type == "bearish" else wall_candle['h']
                    
                    # Pre-creation SL Check: Prevent "suicide" trades
                    if (l_type == "bullish" and current_price <= sl) or (l_type == "bearish" and current_price >= sl):
                        logger.warning(f"🚫 {ticker}: Rejected - Price already at/beyond Stop Loss.")
                        continue

                    # Double Check: If current price is already past TP, skip
                    if (l_type == "bearish" and current_price <= tp) or (l_type == "bullish" and current_price >= tp):
                        continue

                    # Monitor for duplicate trigger (Already active)
                    existing_active = supabase.table("crt_signals").select("id").eq("symbol", ticker).eq("status", "active").eq("is_active", True).execute()
                    if existing_active.data: break 

                    # If everything is ok, create the signal
                    signal = create_pure_crt_signal(ticker, tf, s_type, s_sub, c_high, c_low, entry, sl, tp, d_score, tier_code, wall_candle)
                    
                    existing = supabase.table("crt_signals").select("id").eq("symbol", ticker).eq("is_active", True).in_("status", ["watchlist", "breached"]).execute()
                    if existing.data:
                        supabase.table("crt_signals").update(signal).eq("id", existing.data[0]['id']).execute()
                        logger.info(f"🚀 UPGRADED to ACTIVE (Confirmed): {ticker} [{tier_code}] @ {entry}")
                    else:
                        supabase.table("crt_signals").insert(signal).execute()
                        logger.info(f"🎯 NEW ENTRY (Confirmed): {ticker} [{tier_code}] @ {entry}")
                    return # Exit after finding the first valid reclaim in lookback

        # FASE 2: BREACHED (Solo se non attivo)
        if is_perfect:
            current_low = to_f(df['Low'].iloc[-1])
            current_high = to_f(df['High'].iloc[-1])
            is_breached = False
            if l_type == "bearish" and current_high > lv_val: is_breached = True
            elif l_type == "bullish" and current_low < lv_val: is_breached = True
            
            if is_breached:
                existing_active = supabase.table("crt_signals").select("id").eq("symbol", ticker).eq("status", "active").eq("is_active", True).execute()
                if existing_active.data: continue

                existing = supabase.table("crt_signals").select("id", "status").eq("symbol", ticker).eq("is_active", True).execute()
                if not existing.data or existing.data[0]['status'] == 'watchlist':
                    if not existing.data:
                        sig = create_watchlist_signal(ticker, tf, code, lv_val, 0, pools.get(f"{code}_CANDLE"))
                        sig["status"] = "breached"
                        supabase.table("crt_signals").insert(sig).execute()
                    else:
                        supabase.table("crt_signals").update({"status": "breached", "subtype": f"RECLAIMING {code}"}).eq("id", existing.data[0]['id']).execute()
                    logger.info(f"⚠️ {ticker} BREACHED {code} (Sweep in corso)")
                continue

        # FASE 1: WATCHLIST (Prossimità)
        if dist <= 0.005 and is_perfect:
            existing = supabase.table("crt_signals").select("id").eq("symbol", ticker).eq("is_active", True).execute()
            if not existing.data:
                watchlist_sig = create_watchlist_signal(ticker, tf, code, lv_val, dist, pools.get(f"{code}_CANDLE"))
                supabase.table("crt_signals").insert(watchlist_sig).execute()
                logger.info(f"👀 {ticker} added to WATCHLIST ({code})")

def main():
    setup_logging()
    setup_supabase()

    if sys.platform.startswith('win'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except: pass

    logger.info("🚀 Avvio scanner_new (Proactive Institutional Terminal)...")
    
    all_tickers = get_sp500_tickers() + get_nasdaq100_tickers() + get_forex_tickers() + get_crypto_tickers()
    tickers = list(set(all_tickers))
    logger.info(f"Totale Ticker unici: {len(tickers)}")

    def check_mcap(t):
        try:
            ticker_obj = yf.Ticker(t)
            mcap = ticker_obj.fast_info.get("marketCap", 0) if hasattr(ticker_obj, 'fast_info') else 0
            return t if mcap >= 10_000_000 else None
        except: return None

    logger.info("Filtro Market Cap in corso...")
    filtered_tickers = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for res in executor.map(check_mcap, tickers):
            if res: filtered_tickers.append(res)
    tickers = filtered_tickers
    logger.info(f"Ticker post M-Cap: {len(tickers)}")
    if not tickers: return

    active_signals_map = {}
    try:
        res = supabase.table("crt_signals").select("*").eq("is_active", True).execute()
        for x in res.data:
            t = x['symbol']
            if t not in active_signals_map: active_signals_map[t] = []
            active_signals_map[t].append(x)
        logger.info(f"Caricati {len(res.data)} segnali attivi.")
    except Exception as e:
        logger.error(f"Errore trade attivi: {e}")

    prefetch_all_htf_liquidity(tickers)

    for tf, cfg in TF_CONFIG.items():
        logger.info(f"=== Scansione {tf} ===")
        try:
            data = yf.download(tickers, period=cfg['period'], interval=cfg['interval'], group_by='ticker', threads=True, progress=False)
            if data.empty: continue

            for ticker in tickers:
                try:
                    df = data[ticker] if len(tickers) > 1 else data
                    df = df.dropna()
                    if df.empty: continue

                    updates = validate_existing_signals(ticker, df, active_signals_map)
                    for up in updates:
                        try:
                            sig_id = up.pop('id')
                            supabase.table("crt_signals").update(up).eq("id", sig_id).execute()
                        except Exception as e:
                            logger.error(f"Errore update {ticker}: {e}")

                    # Only run lifecycle if no active signal (or update existing)
                    if float(df['Close'].iloc[-1]) < 0.1: continue
                    update_signal_lifecycle(ticker, df, tf, LIQUIDITY_CACHE)

                except Exception as e: 
                    logger.error(f"Errore loop {ticker}: {e}")
        except Exception as e:
            logger.error(f"Errore download {tf}: {e}")

    logger.info("✅ Completato!")

if __name__ == "__main__":
    main()
