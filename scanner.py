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
    "1H": {"period": "60d", "interval": "1h"} # Meno dati bastano per l'H1
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
    logger.info(f"🌊 Pre-fetching dati Daily (3mo) in bulk per {len(tickers)} ticker (Calcolo HTF Pools e ADR)...")

    try:
        data = yf.download(tickers, period="3mo", interval="1d", group_by='ticker', progress=False, threads=True)
        if data.empty: return

        for ticker in tickers:
            try:
                df = data[ticker] if len(tickers) > 1 else data
                df = clean_df(df.dropna())

                if df.empty or len(df) < 15:
                    continue

                # Calcolo Daily HTF (Prendiamo la candela daily precedente chiusa: .iloc[-2] se oggi è ancora in corso
                pdh = to_f(df['High'].iloc[-2])
                pdl = to_f(df['Low'].iloc[-2])
                prev_day_range = pdh - pdl
                
                # Calcolo Weekly HTF
                try:
                    weekly = df.resample('W').agg({'High': 'max', 'Low': 'min'}).dropna()
                    pwh = to_f(weekly['High'].iloc[-2]) if len(weekly) >= 2 else pdh
                    pwl = to_f(weekly['Low'].iloc[-2]) if len(weekly) >= 2 else pdl
                except Exception:
                    pwh, pwl = pdh, pdl

                # Calcolo Monthly HTF
                try:
                    monthly = df.resample('ME').agg({'High': 'max', 'Low': 'min'}).dropna()
                    pmh = to_f(monthly['High'].iloc[-2]) if len(monthly) >= 2 else pdh
                    pml = to_f(monthly['Low'].iloc[-2]) if len(monthly) >= 2 else pdl
                except Exception:
                    try:
                        monthly = df.resample('M').agg({'High': 'max', 'Low': 'min'}).dropna()
                        pmh = to_f(monthly['High'].iloc[-2]) if len(monthly) >= 2 else pdh
                        pml = to_f(monthly['Low'].iloc[-2]) if len(monthly) >= 2 else pdl
                    except Exception:
                        pmh, pml = pdh, pdl

                # Calcolo ADR 10 giorni
                # Prendiamo le 10 candele daily *prima* di quella corrente: da -12 a -2 compresso
                last_10_days = df.iloc[-12:-2]
                adr_10 = (last_10_days['High'] - last_10_days['Low']).mean()

                LIQUIDITY_CACHE[ticker] = {
                    "PDH": pdh, "PDL": pdl, "PDR": prev_day_range,
                    "PWH": pwh, "PWL": pwl,
                    "PMH": pmh, "PML": pml,
                    "ADR_10": adr_10
                }
            except Exception:
                pass

        logger.info(f"✅ HTF Pools e ADR calcolati per {len(LIQUIDITY_CACHE)} ticker.")
    except Exception as e:
        logger.error(f"Errore prefetch HTF: {e}")

# --- 6. LOGICA PURE CRT MODEL #1 ---
def create_pure_crt_signal(ticker, tf, s_type, subtype, high, low, entry, sl, tp, diamond_score, swept_level):
    rr_ratio = 0
    if abs(entry - sl) > 0:
        rr_ratio = abs(entry - tp) / abs(entry - sl)
        
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
        "market_bias": None, "max_favorable_excursion": 0.0, "trigger_candles": None
    }

def detect_crt_model_1(ticker, df, tf, htf_pools):
    if tf != '1H': return None
    df = clean_df(df)
    if df is None or len(df) < 5: return None
    
    # Recuperiamo gli HTF dal cache
    pools = htf_pools.get(ticker)
    if not pools: return None

    pdh = pools.get("PDH")
    pdl = pools.get("PDL")
    pdr = pools.get("PDR")
    pwh = pools.get("PWH")
    pwl = pools.get("PWL")
    pmh = pools.get("PMH")
    pml = pools.get("PML")
    adr = pools.get("ADR_10")

    if not all([pdh, pdl, pdr, adr, pmh, pml, pwh, pwl]): return None

    # FILTRO ADR: Se il range del giorno precedente è < 90% dell'ADR a 10 giorni, saltiamo
    if pdr < (adr * 0.90):
        return None

    # L'ULTIMA CANDELA H1 CHIUSA DEFINITIVAMENTE
    c = df.iloc[-2]
    c_open, c_close = to_f(c['Open']), to_f(c['Close'])
    c_high, c_low = to_f(c['High']), to_f(c['Low'])
    c_range = c_high - c_low
    
    if c_range == 0: return None

    # --- FILTRO ESTREMO ASSOLUTO (Absolute Extremum Filter) ---
    # Controlliamo le ultime 50 ore di contrattazione (circa 2 giorni)
    context_window = df.iloc[-52:-2]
    if context_window.empty: return None
    recent_min = to_f(context_window['Low'].min())
    recent_max = to_f(context_window['High'].max())

    # --- SETUP SHORT (Bearish Model #1) ---
    # Lo Sweep deve essere l'Estremo Assoluto recente
    if recent_max > c_high:
        return None

    if c_high > pmh and c_close < pmh:
        swept_level = 'PMH'
        diamond_score = 'A+++'
        level_val = pmh
        tp = pml
    elif c_high > pwh and c_close < pwh:
        swept_level = 'PWH'
        diamond_score = 'A++'
        level_val = pwh
        tp = pwl
    elif c_high > pdh and c_close < pdh:
        swept_level = 'PDH'
        diamond_score = 'A+'
        level_val = pdh
        tp = pdl
    else:
        level_val = None

    if level_val is not None:
        # Displacement Rule: Rossa (C < O) E chiude nella metà inferiore
        if c_close < c_open and c_close <= (c_low + c_range * 0.5):
            entry = c_close
            sl = c_high + (c_close * 0.001) # Fisso: poco sopra la wick
            if sl > entry and entry > tp:
                return create_pure_crt_signal(ticker, tf, "bearish_tbs", "Bearish Model #1", c_high, c_low, entry, sl, tp, diamond_score, swept_level)

    # --- SETUP LONG (Bullish Model #1) ---
    # Lo Sweep deve essere l'Estremo Assoluto recente
    if recent_min < c_low:
        return None

    if c_low < pml and c_close > pml:
        swept_level = 'PML'
        diamond_score = 'A+++'
        level_val = pml
        tp = pmh
    elif c_low < pwl and c_close > pwl:
        swept_level = 'PWL'
        diamond_score = 'A++'
        level_val = pwl
        tp = pwh
    elif c_low < pdl and c_close > pdl:
        swept_level = 'PDL'
        diamond_score = 'A+'
        level_val = pdl
        tp = pdh
    else:
        level_val = None

    if level_val is not None:
        # Displacement Rule: Verde (C > O) E chiude nella metà superiore
        if c_close > c_open and c_close >= (c_high - c_range * 0.5):
            entry = c_close
            sl = c_low - (c_close * 0.001) # Fisso: poco sotto la wick
            if sl < entry and entry < tp:
                return create_pure_crt_signal(ticker, tf, "bullish_tbs", "Bullish Model #1", c_high, c_low, entry, sl, tp, diamond_score, swept_level)

    return None

# --- 7. MAIN ENGINE ---
def main():
    setup_logging()
    setup_supabase()

    if sys.platform.startswith('win'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except: pass

    logger.info("🚀 Avvio scanner_new (Pure CRT Model #1)...")
    
    # Arg parser default a SP500 e Nasdaq per facilità
    all_tickers = get_sp500_tickers() + get_nasdaq100_tickers() + get_forex_tickers() + get_crypto_tickers()
    tickers = list(set(all_tickers))
    logger.info(f"Totale Ticker unici: {len(tickers)}")

    # Filtro Market Cap semplificato
    def check_mcap(t):
        try:
            ticker_obj = yf.Ticker(t)
            mcap = ticker_obj.fast_info.get("marketCap", 0) if hasattr(ticker_obj, 'fast_info') else 0
            return t if mcap >= 10_000_000_000 else None
        except:
            return None

    logger.info("Filtro Market Cap in corso... (Minimo 10B)")
    filtered_tickers = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for res in executor.map(check_mcap, tickers):
            if res: filtered_tickers.append(res)
    
    tickers = filtered_tickers
    logger.info(f"Ticker rimanenti post M-Cap: {len(tickers)}")
    if not tickers: return

    # ONE ACTIVE TRADE POLICY PREFETCH
    active_signals_map = {}
    try:
        res = supabase.table("crt_signals").select("id, symbol").eq("is_active", True).execute()
        for x in res.data:
            active_signals_map[x['symbol']] = True
    except Exception as e:
        logger.error(f"Errore recupero trade attivi: {e}")

    prefetch_all_htf_liquidity(tickers)

    for tf, cfg in TF_CONFIG.items():
        logger.info(f"=== Download Bulk {tf} ===")
        try:
            data = yf.download(tickers, period=cfg['period'], interval=cfg['interval'], group_by='ticker', threads=True, progress=False)
        except Exception as e:
            logger.error(f"Errore download {tf}: {e}")
            continue

        if data.empty: continue

        for ticker in tickers:
            if active_signals_map.get(ticker):
                continue # Ha già un trade aperto

            try:
                df = data[ticker] if len(tickers) > 1 else data
                df = df.dropna()
                if df.empty: continue

                # Filtro Penny Stock
                if float(df['Close'].iloc[-1]) < 5.00:
                    continue

                signal = detect_crt_model_1(ticker, df, tf, LIQUIDITY_CACHE)
                
                if signal:
                    # R/R ratio deve essere superire a 1.0 (Come da prompt)
                    if signal['rr_ratio'] > 1.0:
                        logger.info(f"🎯 TROVATO PURE CRT {signal['type'].upper()} su {ticker} | Entry: {signal['entry_price']} | SL: {signal['stop_loss']} | TP: {signal['take_profit']} | R/R: {signal['rr_ratio']}")
                        try:
                            supabase.table("crt_signals").insert(signal).execute()
                            active_signals_map[ticker] = True # Mark come attivo
                        except Exception as e:
                            logger.error(f"Errore salvataggio segnale {ticker}: {e}")
                            
            except Exception as e:
                # Silenzioso sui ticker rotti
                pass

    logger.info("✅ Scansione Pure CRT Model #1 Completata!")

if __name__ == "__main__":
    main()
