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

from notifications import send_telegram_alert
from indicators import calculate_atr, get_wick_analysis, get_seasonality_score, calculate_rsi, check_divergence, calculate_adr_percent, detect_fvg_confluence

# 1. CONFIGURAZIONE LOGGING
# Use a custom setup to ensure UTF-8 encoding on Windows
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# File Handler with UTF-8
file_handler = logging.FileHandler("scanner.log", encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 2. CARICAMENTO VARIABILI D'AMBIENTE
# Carica .env.local solo se esiste (sviluppo locale)
if os.path.exists(".env.local"):
    load_dotenv(".env.local")
    logger.info("Caricate variabili da .env.local")
else:
    logger.info("File .env.local non trovato, utilizzo variabili d'ambiente di sistema")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")

logger.info(SUPABASE_URL)
# Si consiglia l'uso della SERVICE_ROLE_KEY per operazioni di backend/cron
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
logger.info(SUPABASE_KEY)
if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Credenziali Supabase mancanti. Verifica il file .env")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 2B. Admin Console Logging System
def admin_log(level, message):
    """Invia i log alla tabella system_logs per la visualizzazione Realtime nel Dashboard."""
    try:
        supabase.table("system_logs").insert({
            "level": level,
            "message": message,
            "source": "scanner_bot"
        }).execute()
        # Manteniamo anche il log locale
        if level == "ERROR": logger.error(message)
        elif level == "WARNING": logger.warning(message)
        else: logger.info(message)
    except Exception as e:
        print(f"[LOG ERROR] Database logging failed: {e}")
        logger.error(message)

# 3. CONFIGURAZIONE TIMEFRAME
TF_CONFIG = {
    # Il bias Daily viene calcolato a parte. Lo scanner scansionerà SOLO l'1H.
    "1H": {"period": "730d", "interval": "1h"}
}

def get_sp500_tickers():
    """Recupera la lista aggiornata dei ticker S&P 500 da Wikipedia."""
    try:
        # Aggiungiamo User-Agent per evitare errore 403 Forbidden da Wikipedia
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        table = pd.read_html(io.StringIO(response.text))
        tickers = table[0]['Symbol'].tolist()
        # Pulizia ticker (es. BRK.B invece di BRK-B per yfinance)
        # Pulizia ticker (es. BRK.B invece di BRK-B per yfinance)
        valid_tickers = []
        for t in tickers:
            if isinstance(t, str) and len(t) <= 8 and ' ' not in t:
                valid_tickers.append(t.replace('.', '-'))
        return valid_tickers
        logger.info(f"Ottenuti {len(tickers)} ticker S&P 500.")
        return tickers
    except Exception as e:
        logger.error(f"Errore nel recupero ticker S&P 500: {e}")
        return ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]

def get_nasdaq100_tickers():
    """Recupera la lista aggiornata dei ticker NASDAQ 100 da Wikipedia."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        table = pd.read_html(io.StringIO(response.text))
        # La tabella 'Constituents' è solitamente la indice 4 su questa pagina, ma cerchiamo quella con 'Ticker' o 'Symbol'
        # Nota: Wikipedia cambia spesso. Cerchiamo la tabella corretta.
        for t in table:
            if 'Ticker' in t.columns:
                tickers = t['Ticker'].tolist()
                break
            elif 'Symbol' in t.columns:
                tickers = t['Symbol'].tolist()
                break
        else:
             # Fallback index 4 (storicamente corretto)
             tickers = table[4]['Ticker'].tolist()

        # Validate tickers
        valid_tickers = []
        for t in tickers:
            if isinstance(t, str) and len(t) <= 8 and ' ' not in t:
                valid_tickers.append(t.replace('.', '-'))
                
        logger.info(f"Ottenuti {len(valid_tickers)} ticker NASDAQ 100.")
        return valid_tickers
    except Exception as e:
        logger.error(f"Errore nel recupero ticker NASDAQ 100: {e}")
        return []

def get_forex_tickers():
    """Ritorna la lista dei principali cambi Forex per yfinance."""
    return [
        "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X", 
        "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X"
    ]

def get_crypto_tickers():
    """Ritorna la lista delle principali Crypto per yfinance."""
    return [
        "BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD", "ADA-USD", 
        "DOGE-USD", "AVAX-USD", "DOT-USD", "LINK-USD"
    ]

def has_reliable_volume(ticker):
    """
    Determina se il volume di yfinance è affidabile per questo asset.
    Forex (=X) e Crypto (-USD) hanno volumi frammentati/unreliable in yfinance.
    """
    t = str(ticker).upper()
    if "=X" in t or "-USD" in t:
        return False
    return True

def get_russell2000_tickers():
    """Recupera la lista Russell 2000 dal file CSV locale (IWM_holdings.csv)."""
    try:
        csv_path = os.path.join(os.path.dirname(__file__), 'IWM_holdings.csv')
        if not os.path.exists(csv_path): 
            logger.warning("File IWM_holdings.csv non trovato.")
            return []
            
        tickers = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Skip first 9 lines (metadata)
            for _ in range(9):
                next(f, None)
            
            reader = csv.DictReader(f)
            for row in reader:
                val = row.get('Ticker')
                if val and isinstance(val, str) and val.strip() and val != '-':
                    val = val.strip()
                    # Sanity Check: Ticker validi sono brevi e senza spazi
                    if len(val) > 8 or ' ' in val: 
                        continue
                    
                    # Clean ticker (e.g. BRK.B -> BRK-B)
                    tickers.append(val.replace('.', '-'))
                    
        logger.info(f"Ottenuti {len(tickers)} ticker Russell 2000.")
        return tickers
    except Exception as e:
        logger.error(f"Errore lettura Russell 2000 CSV: {e}")
        return []

def get_session_tag(timestamp):
    # Timestamp assumed UTC or tz-naive (from yfinance)
    # London: 07:00-11:00 UTC
    # NY: 12:30-16:00 UTC
    # Asia: 23:00-06:00 UTC 
    # Simplified logic
    try:
        h = timestamp.hour
        m = timestamp.minute
        if 7 <= h < 11: return 'London'
        if (h == 12 and m >= 30) or (13 <= h < 16): return 'NY'
        if h >= 23 or h < 6: return 'Asia'
    except:
        pass
    return 'None'





# --- FALLBACK MECHANISM ---
def save_failed_signals(signals):
    """Salva i segnali su disco se il database fallisce."""
    try:
        filename = "failed_signals.json"
        # Se esiste già, appende o sovrascrive? Sovrascriviamo o mergiamo.
        existing = []
        if os.path.exists(filename):
            try:
                with open(filename, 'r') as f:
                    existing = json.load(f)
            except: pass
        
        merged = existing + signals
        with open(filename, 'w') as f:
            json.dump(merged, f, indent=2, default=str) # default=str per date/time
        logger.warning(f"Salvati {len(signals)} segnali in {filename} per retry successivo.")
    except Exception as e:
        logger.error(f"Impossibile salvare backup locale: {e}")

def retry_failed_uploads():
    """Tenta di caricare i segnali salvati localmente."""
    filename = "failed_signals.json"
    if not os.path.exists(filename):
        return

    logger.info("Trovato file di backup segnali. Tentativo di ripristino...")
    try:
        with open(filename, 'r') as f:
            signals = json.load(f)
        
        if not signals:
            os.remove(filename)
            return

        # Insert batch
        success = True
        for j in range(0, len(signals), 1000):
            batch = signals[j : j + 1000]
            try:
                supabase.table("crt_signals").insert(batch).execute()
            except Exception as e:
                logger.error(f"Errore ripristino batch: {e}")
                success = False
        
        if success:
             logger.info(f"Ripristinati con successo {len(signals)} segnali.")
             os.remove(filename)
        else:
             logger.warning("Alcuni segnali non sono stati ripristinati. Il file di backup rimane.")

    except Exception as e:
        logger.error(f"Errore generale retry uploads: {e}")




def clean_df(df):
    """Garantisce che il DataFrame sia in un formato standard (singolo indice, no duplicati, nomi puliti)."""
    if df is None or df.empty: return df
    df = df.copy()
    
    # 1. Se ha MultiIndex (yfinance bulk), prendi l'ultimo livello
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(-1)
    
    # 2. Se le colonne sono comunque tuple (es. ('AAPL', 'Open')), prendi l'ultimo elemento
    if any(isinstance(c, tuple) for c in df.columns):
        df.columns = [c[-1] if isinstance(c, tuple) else c for c in df.columns]

    # 3. Normalizzazione Nomi (Case-insensitive -> Title Case standard)
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

    # 4. Rimuovi colonne duplicate
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def to_f(val):
    """Converte in float in modo robusto, gestendo Series o array."""
    if hasattr(val, 'iloc'): 
        if hasattr(val, 'empty') and val.empty: return 0.0
        v = val.iloc[0]
        # Se è ancora un oggetto complesso (es. slice di DataFrame), scendi ancora
        if hasattr(v, 'iloc'): v = v.iloc[0]
        return float(v)
    return float(val)

def to_b(val):
    """Converte in bool in modo robusto."""
    if hasattr(val, 'iloc'):
        if hasattr(val, 'empty') and val.empty: return False
        v = val.iloc[0]
        if hasattr(v, 'iloc'): v = v.iloc[0]
        return bool(v)
    return bool(val)

def detect_macro_sweep(ticker, df, tf, config=None):
    """
    Cerca SOLO "Macro Sweeps" (HTF Confluence):
    1. Aggrega i dati Daily in Weekly, Monthly, Quarterly.
    2. Controlla se la candela DAILY corrente (Trigger) ha 'sweeppato' un livello HTF precedente.
    3. Reclaim (chiusura dentro il range del periodo precedente).
    """
    # Permettiamo allo scanner di analizzare sia il Daily che l'Orario
    if tf not in ['1D', '1H']: return None
    
    df = clean_df(df)
    if df is None or len(df) < 60: # Servono abbastanza dati per resample Monthly/Quarterly
        return None

    try:
        results = []
        # --- 1. DATA AGGREGATION (RESAMPLING) ---
        agg_map = {'High': 'max', 'Low': 'min', 'Close': 'last', 'Open': 'first'}
        
        # Weekly
        df_w = df.resample('W').agg(agg_map).dropna()
        if len(df_w) < 2: return None
        prev_w = df_w.iloc[-2]

        # Daily (PDH/PDL for 1H timeframes)
        prev_d = None
        if tf == '1H':
            df_d = df.resample('D').agg(agg_map).dropna()
            if len(df_d) >= 2: prev_d = df_d.iloc[-2]

        # Monthly (ME = Month End)
        df_m = df.resample('ME').agg(agg_map).dropna() 
        prev_m = None if len(df_m) < 2 else df_m.iloc[-2]

        # Quarterly
        df_q = df.resample('QE').agg(agg_map).dropna()
        prev_q = None if len(df_q) < 2 else df_q.iloc[-2]

        # Semi-Annual (6M)
        prev_6m = None
        try:
             df_6m = df.resample('6ME').agg(agg_map).dropna()
             if len(df_6m) >= 2: prev_6m = df_6m.iloc[-2]
        except:
             pass

        # --- 2. THE TRIGGER (Daily Candle) ---
        curr = df.iloc[-1]
        c_high = to_f(curr['High'])
        c_low = to_f(curr['Low'])
        c_close = to_f(curr['Close'])
        
        # Basic context
        month_now = curr.name.month if hasattr(curr.name, 'month') else 0
        
        # Helper per check sweep
        def check_sweep(level_name, level_high, level_low, importance_score):
             # NOISE FILTER: Ignore scans < 0.05% difference
             # Logic is: Did we sweep? How much?
             sweep_pct = 0.0
             
             # A. Bearish Sweep
             if c_high > level_high and c_close < level_high:
                 sweep_diff = abs(c_high - level_high)
                 sweep_pct = sweep_diff / level_high
                 if sweep_pct < 0.0005: 
                     return None # Too small, just noise

                 seas_score = get_seasonality_score(month_now, 'bearish')
                 
                 # Map level name to confluence code
                 confluence_code = "PWH" 
                 if "monthly" in level_name: confluence_code = "PMH"
                 if "quarterly" in level_name: confluence_code = "PQH"
                 if "semi" in level_name: confluence_code = "6MH"
                 
                 # Map importance to Diamond Score
                 diamond_score = "B"
                 if importance_score == "GOD_MODE": diamond_score = "A++"
                 if importance_score == "A++": diamond_score = "A++"
                 if importance_score == "A": diamond_score = "A+"

                 sl = c_high
                 risk = sl - c_close
                 # NUOVO CODICE (Intraday Risk Sizing)
                 min_risk = c_close * 0.015
                 
                 if risk < min_risk:
                     sl = c_close + min_risk
                     risk = sl - c_close

                 # --- TARGET STRUTTURALE (Draw on Liquidity) ---
                 # Cerchiamo il minimo più basso delle ultime 25 candele
                 lookback_window = df.iloc[-25:-1]
                 structural_target = to_f(lookback_window['Low'].min())
                 
                 tp = structural_target
                 reward = c_close - tp
                 
                 if reward <= 0: return None
                     
                 actual_rr = reward / risk
                 
                 if actual_rr < 1.5:
                     return None
                 # ----------------------------------------------
                 
                 return {
                    "symbol": ticker,
                    "timeframe": tf,
                    "type": "bearish_sweep",
                    "subtype": f"{level_name}_sweep",
                    "range_high": level_high,
                    "range_low": level_low,
                    "price": c_close,
                    "entry_price": c_close,
                    "result": None,
                    "is_active": True,
                    "stop_loss": round(sl, 2),
                    "take_profit": round(tp, 2),
                    "rr_ratio": round(actual_rr, 1),
                    "liquidity_tier": level_name.capitalize(),
                    "session_tag": importance_score,
                    "has_divergence": seas_score > 0,
                    "seasonality_score": seas_score,
                    "diamond_score": diamond_score,
                    "confluence_level": confluence_code,
                    "fvg_detected": False,
                    "hitting_fvg": False,
                    "smt_divergence": False,
                    "adr_percent": 0,
                    "rel_volume": 0,
                    "volatility_warning": False,
                    "is_golden_wick": True
                 }
                 
             # B. Bullish Sweep
             if c_low < level_low and c_close > level_low:
                 sweep_diff = abs(level_low - c_low)
                 sweep_pct = sweep_diff / level_low
                 if sweep_pct < 0.0005: 
                     return None # Too small

                 seas_score = get_seasonality_score(month_now, 'bullish')
                 
                 confluence_code = "PWL"
                 if "monthly" in level_name: confluence_code = "PML"
                 if "quarterly" in level_name: confluence_code = "PQL"
                 if "semi" in level_name: confluence_code = "6ML"

                 diamond_score = "B"
                 if importance_score == "GOD_MODE": diamond_score = "A++"
                 if importance_score == "A++": diamond_score = "A++"
                 if importance_score == "A": diamond_score = "A+"

                 # Swing Trading Check: Min risk 2% of price for big moves
                 sl = c_low
                 risk = c_close - sl
                 # NUOVO CODICE (Intraday Risk Sizing)
                 min_risk = c_close * 0.015
                 
                 if risk < min_risk:
                     sl = c_close - min_risk
                     risk = c_close - sl

                 # --- TARGET STRUTTURALE (Draw on Liquidity) ---
                 # Cerchiamo il massimo più alto delle ultime 80 candele
                 lookback_window = df.iloc[-80:-1]
                 structural_target = to_f(lookback_window['High'].max())
                 
                 # Il Take profit è il target strutturale
                 tp = structural_target
                 reward = tp - c_close
                 
                 # Se il target è troppo vicino o sotto il prezzo, il trade è sballato
                 if reward <= 0: return None
                     
                 # Calcolo R/R reale
                 actual_rr = reward / risk
                 
                 # FILTRO DI QUALITÀ: Scartiamo i trade che non danno almeno 1:1.5
                 if actual_rr < 1.5:
                     return None
                 # ----------------------------------------------
                 
                 return {
                    "symbol": ticker,
                    "timeframe": tf,
                    "type": "bullish_sweep",
                    "subtype": f"{level_name}_sweep",
                    "range_high": level_high,
                    "range_low": level_low,
                    "price": c_close,
                    "entry_price": c_close,
                    "result": None,
                    "is_active": True,
                    "stop_loss": round(sl, 2),
                    "take_profit": round(tp, 2),
                    "rr_ratio": round(actual_rr, 1),
                    "liquidity_tier": level_name.capitalize(),
                    "session_tag": importance_score,
                    "has_divergence": seas_score > 0,
                    "seasonality_score": seas_score,
                    "diamond_score": diamond_score,
                    "confluence_level": confluence_code,
                    "fvg_detected": False,
                    "hitting_fvg": False,
                    "smt_divergence": False,
                    "adr_percent": 0,
                    "rel_volume": 0,
                    "volatility_warning": False,
                    "is_golden_wick": True,
                    "touches": 0,
                    "market_bias": None,
                    "max_favorable_excursion": 0.0
                 }
             return None

        # Check Hierarchy: Quarterly > Monthly > Weekly
        
        # 0. Semi-Annual Sweep (God Mode - Tier 2)
        if prev_6m is not None:
             sig = check_sweep("semi_annual", prev_6m['High'], prev_6m['Low'], "GOD_MODE")
             if sig: return sig

        # 1. Quarterly Sweep (God Mode - Tier 1)
        if prev_q is not None:
            sig = check_sweep("quarterly", prev_q['High'], prev_q['Low'], "GOD_MODE")
            if sig: return sig
                    # 2. Monthly Sweep (A++)
            if prev_m is not None:
                sig = check_sweep("monthly", to_f(prev_m['High']), to_f(prev_m['Low']), "A++")
                if sig: results.append(sig)
            
        # --- 3. CHECK EVERY LEVEL ---
        
        # PWH/PWL
        res_w = check_sweep("weekly", to_f(prev_w['High']), to_f(prev_w['Low']), "A")
        if res_w: results.append(res_w)

        # PDH/PDL (Only 1H)
        if prev_d is not None:
             res_d = check_sweep("daily", to_f(prev_d['High']), to_f(prev_d['Low']), "B")
             if res_d: results.append(res_d)

        # Return the best signal based on diamond_score
        if results:
            # Define a mapping for diamond_score to a numerical value for sorting
            score_map = {"A+++": 5, "A++": 4, "A+": 3, "A": 2, "B": 1}
            # Sort by diamond_score (descending) and then by rr_ratio (descending)
            results.sort(key=lambda x: (score_map.get(x['diamond_score'], 0), x['rr_ratio']), reverse=True)
            return results[0]

    except Exception as e:
        pass

    return None


def detect_tbs_setup(ticker, df, tf, config=None):
    """
    Rileva il pattern TBS (Turtle Body Soup) ad "alta probabilità".
    """
    # Permettiamo allo scanner di analizzare sia il Daily che l'Orario
    if tf not in ['1D', '1H']: return None
    df = clean_df(df)
    if df is None or len(df) < 20: return None

    try:
        # Troviamo i pivot recenti (ultime 20 candele, escludendo le ultime 2 che sono il setup)
        lookback = 20
        recent_df = df.iloc[-lookback:-2]
        if recent_df.empty: return None

        # Calcoliamo la Media Mobile a 20 periodi del Volume per il breakout
        if 'Volume' in df.columns:
            vol_data = df['Volume']
            if hasattr(vol_data, 'iloc') and len(vol_data.shape) > 1: # DataFrame case
                vol_data = vol_data.iloc[:, 0]
            avg_vol_20 = float(vol_data.rolling(20).mean().iloc[-2])
        else:
            avg_vol_20 = 0

        # Candele di setup
        breakout_candle = df.iloc[-2]
        reversal_candle = df.iloc[-1]

        # Estrai prezzi setup (assicurandosi che siano scalari)
        b_open = to_f(breakout_candle['Open'])
        b_close = to_f(breakout_candle['Close'])
        b_high = to_f(breakout_candle['High'])
        b_low = to_f(breakout_candle['Low'])
        
        r_open = to_f(reversal_candle['Open'])
        r_close = to_f(reversal_candle['Close'])
        r_high = to_f(reversal_candle['High'])
        r_low = to_f(reversal_candle['Low'])

        # BEARISH TBS (Short) - Ricerca di uno Swing High precedente
        pivot_high = to_f(recent_df['High'].max())
        
        # Check consolidamento: Quante volte il prezzo ha "toccato" l'area dell'1% del pivot?
        # Usiamo .iloc[:, 0] se è un DataFrame per evitare ambiguità
        high_vals = recent_df['High']
        if isinstance(high_vals, pd.DataFrame): high_vals = high_vals.iloc[:, 0]
        
        touches_high = int((high_vals >= pivot_high * 0.99).sum())
        is_high_prob_consolidation = touches_high >= 2
        
        # Check volume breakout: Il volume della trappola era superiore alla media?
        if 'Volume' in breakout_candle and avg_vol_20 > 0:
            high_volume_breakout = to_b(breakout_candle['Volume'] > avg_vol_20)
        else:
            high_volume_breakout = False
        
        # Regola Bearish:
        # 1. Breakout candle rompe al rialzo e CHIUDE SOPRA il pivot_high
        is_breakout_up = to_b(b_close > pivot_high) and to_b(b_open < b_close)
        
        # Calculate tentative risk early for strength check
        tentative_risk_bearish = max(b_high, r_high) - r_close
        if tentative_risk_bearish <= 0: tentative_risk_bearish = r_close * 0.01

        # 2. Reversal candle inverte e CHIUDE SOTTO il pivot_high (Tarta Soup) in modo forte
        is_reversal_down = to_b(r_close < (pivot_high - tentative_risk_bearish * 0.1)) and to_b(r_close < r_open)

        if is_breakout_up and is_reversal_down:
            sl = max(b_high, r_high)
            risk = sl - r_close
            # NUOVO CODICE (Intraday Risk Sizing): 1.5%
            min_risk = r_close * 0.015
            if risk < min_risk:
                risk = min_risk
                sl = r_close + risk

            # --- TARGET STRUTTURALE (Draw on Liquidity) ---
            # Cerchiamo il minimo più basso delle ultime 25 candele
            lookback_window = df.iloc[-25:-1]
            structural_target = to_f(lookback_window['Low'].min())
            
            tp = structural_target
            reward = r_close - tp
            
            if reward <= 0: return None
                
            actual_rr = reward / risk
            
            if actual_rr < 1.5:
                return None
            # ----------------------------------------------
            
            # Entry Validation Rule: Did the reversal candle also break the low of the breakout candle?
            is_validated = r_close < b_low or r_low < b_low
            confluence_level = "TBS VALIDATED" if is_validated else "TBS"
            
            # Dinamic Diamond Score
            # Se ha consolidato E rompe con volumi alti, è un pattern d'élite.
            diamond_score = "A++" if (to_b(is_high_prob_consolidation) and to_b(high_volume_breakout)) else "A"
            if not is_high_prob_consolidation and not high_volume_breakout:
                diamond_score = "B" # Setup base
                
            # Calcola quanto è grande il corpo rispetto al range totale
            body_size = abs(r_close - r_open)
            total_range = r_high - r_low
            body_ratio = body_size / total_range if total_range > 0 else 0
            if body_ratio > 0.6:
                diamond_score = "A+++"

            return {
                "symbol": ticker,
                "timeframe": tf,
                "type": "bearish_tbs",
                "subtype": "tbs_setup",
                "range_high": pivot_high,
                "range_low": to_f(recent_df['Low'].min()),
                "price": r_close,
                "entry_price": r_close,
                "result": None,
                "is_active": True,
                "stop_loss": round(sl, 2),
                "take_profit": round(tp, 2),
                "rr_ratio": round(actual_rr, 1),
                "liquidity_tier": "Daily",
                "session_tag": "TBS Pattern",
                "has_divergence": False,
                "seasonality_score": 0,
                "diamond_score": diamond_score,
                "confluence_level": confluence_level,
                "fvg_detected": False,
                "hitting_fvg": False,
                "smt_divergence": False,
                "adr_percent": 0,
                "rel_volume": int(breakout_candle['Volume'] / avg_vol_20 * 100) if avg_vol_20 > 0 else 0,
                "volatility_warning": False,
                "is_golden_wick": False,
                "touches": int(touches_high),
                "market_bias": None,
                "max_favorable_excursion": 0.0
            }

        # BULLISH TBS (Long) - Ricerca di uno Swing Low precedente
        pivot_low = to_f(recent_df['Low'].min())
        
        # Check consolidamento: Quante volte il prezzo ha "toccato" l'area dell'1% del pivot?
        low_vals = recent_df['Low']
        if isinstance(low_vals, pd.DataFrame): low_vals = low_vals.iloc[:, 0]

        touches_low = int((low_vals <= pivot_low * 1.01).sum())
        is_high_prob_consolidation = touches_low >= 2
        
        # Check volume breakout: Il volume della trappola era superiore alla media?
        if 'Volume' in breakout_candle and avg_vol_20 > 0:
            high_volume_breakout = to_b(breakout_candle['Volume'] > avg_vol_20)
        else:
            high_volume_breakout = False
        
        # Regola Bullish:
        # 1. Breakout candle rompe al ribasso e CHIUDE SOTTO il pivot_low
        is_breakout_down = to_b(b_close < pivot_low) and to_b(b_open > b_close)
        
        # Calculate tentative risk early for strength check
        tentative_risk_bullish = r_close - min(b_low, r_low)
        if tentative_risk_bullish <= 0: tentative_risk_bullish = r_close * 0.01

        # 2. Reversal candle inverte e CHIUDE SOPRA il pivot_low in modo forte
        is_reversal_up = to_b(r_close > (pivot_low + tentative_risk_bullish * 0.1)) and to_b(r_close > r_open)

        if is_breakout_down and is_reversal_up:
            sl = min(b_low, r_low)
            risk = r_close - sl
            # NUOVO CODICE (Intraday Risk Sizing)
            min_risk = r_close * 0.015
            if risk < min_risk:
                risk = min_risk
                sl = r_close - risk

            # --- TARGET STRUTTURALE (Draw on Liquidity) ---
            # Cerchiamo il massimo più alto delle ultime 25 candele orarie
            lookback_window = df.iloc[-25:-1]
            structural_target = to_f(lookback_window['High'].max())
            
            # Il Take profit è il target strutturale
            tp = structural_target
            reward = tp - r_close
            
            # Se il target è troppo vicino o sotto il prezzo, il trade è sballato
            if reward <= 0: return None
                
            # Calcolo R/R reale
            actual_rr = reward / risk
            
            # FILTRO DI QUALITÀ: Scartiamo i trade che non danno almeno 1:1.5
            if actual_rr < 1.5:
                return None
            # ----------------------------------------------
            
            # Entry Validation Rule: Did the reversal candle also break the high of the breakout candle?
            is_validated = r_close > b_high or r_high > b_high
            confluence_level = "TBS VALIDATED" if is_validated else "TBS"
            
            # Dinamic Diamond Score
            diamond_score = "A++" if (is_high_prob_consolidation and high_volume_breakout) else "A"
            if not is_high_prob_consolidation and not high_volume_breakout:
                diamond_score = "B" # Setup base
                
            # Calcola quanto è grande il corpo rispetto al range totale
            body_size = abs(r_close - r_open)
            total_range = r_high - r_low
            body_ratio = body_size / total_range if total_range > 0 else 0
            if body_ratio > 0.6:
                diamond_score = "A+++"

            return {
                "symbol": ticker,
                "timeframe": tf,
                "type": "bullish_tbs",
                "subtype": "tbs_setup",
                "range_high": to_f(recent_df['High'].max()),
                "range_low": pivot_low,
                "price": r_close,
                "entry_price": r_close,
                "result": None,
                "is_active": True,
                "stop_loss": round(sl, 2),
                "take_profit": round(tp, 2),
                "rr_ratio": round(actual_rr, 1),
                "liquidity_tier": "Daily",
                "session_tag": "TBS Pattern",
                "has_divergence": False,
                "seasonality_score": 0,
                "diamond_score": diamond_score,
                "confluence_level": confluence_level,
                "fvg_detected": False,
                "hitting_fvg": False,
                "smt_divergence": False,
                "adr_percent": 0,
                "rel_volume": int(breakout_candle['Volume'] / avg_vol_20 * 100) if avg_vol_20 > 0 else 0,
                "volatility_warning": False,
                "is_golden_wick": False,
                "touches": int(touches_low),
                "market_bias": None,
                "max_favorable_excursion": 0.0
            }

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Errore detect_tbs_setup per {ticker}: {e}")

    return None

def detect_crt_models(ticker, df, tf, config=None):
    """
    Rileva i 4 modelli classici CRT (Candle Range Theory) come da manuale.
    Cerca una 'Mother Bar' direzionale seguita da una rottura del suo High/Low.
    """
    # Permettiamo allo scanner di analizzare sia il Daily che l'Orario
    if tf not in ['1D', '1H']: return None
    df = clean_df(df)
    if df is None or len(df) < 15: return None

    try:
        # La candela che fa scattare l'alert è l'ultima (quella appena chiusa)
        trigger = df.iloc[-1]
        t_open, t_close, t_high, t_low = to_f(trigger['Open']), to_f(trigger['Close']), to_f(trigger['High']), to_f(trigger['Low'])

        # Calcoliamo la dimensione media del corpo per assicurarci che la Mother Bar sia una "vera" candela forte
        avg_body = df['Close'].diff().abs().rolling(14).mean().iloc[-2]

        # Cerchiamo la Mother Bar nelle ultime 6 candele
        for i in range(2, 7): 
            mb = df.iloc[-i]
            mb_open, mb_close, mb_high, mb_low = to_f(mb['Open']), to_f(mb['Close']), to_f(mb['High']), to_f(mb['Low'])
            mb_body = abs(mb_close - mb_open)

            # Filtro: La Mother bar deve essere abbastanza grande (non una doji)
            if mb_body < (avg_body * 0.7):
                continue

            intermediate_candles = df.iloc[-i+1:-1]
            num_candles = i  # Numero totale di candele nel pattern (MB + Intermedie + Trigger)

            # --- SETUP LONG (Bullish CRT) ---
            # La Mother Bar è ribassista (Nera nell'immagine), e noi rompiamo il suo High
            if mb_close < mb_open: 
                crt_high = mb_high
                crt_low = mb_low

                # Condizione di innesco: La candela attuale chiude in modo deciso sopra il CRT High
                if t_close > crt_high:
                    valid = True
                    has_inside_bar = False

                    # Analizziamo le candele in mezzo
                    for _, row in intermediate_candles.iterrows():
                        # Se una candela intermedia CHIUDE sotto il CRT Low, il pattern è invalidato
                        if to_f(row['Close']) < crt_low:
                            valid = False
                            break
                        # Check se c'è un Inside Bar
                        if to_f(row['High']) <= mb_high and to_f(row['Low']) >= mb_low:
                            has_inside_bar = True

                    if valid:
                        # Classifichiamo i 4 Modelli esatti dall'immagine
                        model_name = "Multiple Candle CRT"
                        if num_candles == 2: model_name = "2 Candle CRT"
                        elif num_candles == 3: model_name = "Inside Bar CRT" if has_inside_bar else "Classic 3 Candle CRT"
                        elif has_inside_bar: model_name = "Inside Bar CRT (Extended)"

                        # Stop Loss calcolato sul punto più basso dell'intera formazione strutturale
                        sl = min([crt_low, t_low] + [to_f(r['Low']) for _, r in intermediate_candles.iterrows()])
                        risk = t_close - sl
                        
                        # NUOVO CODICE (Intraday Risk Sizing)
                        if risk < t_close * 0.015:
                            risk = t_close * 0.015
                            sl = t_close - risk

                        # --- TARGET STRUTTURALE (Draw on Liquidity) ---
                        # Cerchiamo il massimo più alto delle ultime 25 candele orarie
                        lookback_window = df.iloc[-25:-1]
                        structural_target = to_f(lookback_window['High'].max())
                        
                        # Il Take profit è il target strutturale
                        tp = structural_target
                        reward = tp - t_close
                        
                        # Se il target è troppo vicino o sotto il prezzo, il trade è sballato
                        if reward <= 0: continue
                            
                        # Calcolo R/R reale
                        actual_rr = reward / risk
                        
                        # FILTRO DI QUALITÀ: Scartiamo i trade che non danno almeno 1:1.5
                        if actual_rr < 1.5:
                            continue
                        # ----------------------------------------------

                        signal_data = create_signal_dict(ticker, tf, "bullish_crt", model_name, crt_high, crt_low, t_close, sl, tp, num_candles)
                        signal_data['rr_ratio'] = round(actual_rr, 1)
                        return signal_data

            # --- SETUP SHORT (Bearish CRT) ---
            # La Mother Bar è rialzista, e noi rompiamo il suo Low
            elif mb_close > mb_open:
                crt_high = mb_high
                crt_low = mb_low

                if t_close < crt_low:
                    valid = True
                    has_inside_bar = False

                    for _, row in intermediate_candles.iterrows():
                        if to_f(row['Close']) > crt_high:
                            valid = False
                            break
                        if to_f(row['High']) <= mb_high and to_f(row['Low']) >= mb_low:
                            has_inside_bar = True

                    if valid:
                        model_name = "Multiple Candle CRT"
                        if num_candles == 2: model_name = "2 Candle CRT"
                        elif num_candles == 3: model_name = "Inside Bar CRT" if has_inside_bar else "Classic 3 Candle CRT"
                        elif has_inside_bar: model_name = "Inside Bar CRT (Extended)"

                        sl = max([crt_high, t_high] + [to_f(r['High']) for _, r in intermediate_candles.iterrows()])
                        risk = sl - t_close
                        
                        # NUOVO CODICE (Intraday Risk Sizing)
                        if risk < t_close * 0.015:
                            risk = t_close * 0.015
                            sl = t_close + risk

                        # --- TARGET STRUTTURALE (Draw on Liquidity) ---
                        # Cerchiamo il minimo più basso delle ultime 25 candele orarie
                        lookback_window = df.iloc[-25:-1]
                        structural_target = to_f(lookback_window['Low'].min())
                        
                        tp = structural_target
                        reward = t_close - tp
                        
                        if reward <= 0: continue
                            
                        actual_rr = reward / risk
                        
                        if actual_rr < 1.5:
                            continue
                        # ----------------------------------------------

                        signal_data = create_signal_dict(ticker, tf, "bearish_crt", model_name, crt_high, crt_low, t_close, sl, tp, num_candles)
                        signal_data['rr_ratio'] = round(actual_rr, 1)
                        return signal_data

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Errore detect_crt_models per {ticker}: {e}")

    return None

def create_signal_dict(ticker, tf, s_type, subtype, high, low, price, sl, tp, touches):
    return {
        "symbol": ticker, "timeframe": tf, "type": s_type, "subtype": subtype,
        "range_high": high, "range_low": low, "price": price, "entry_price": price,
        "result": None, "is_active": True, "stop_loss": round(sl, 2), "take_profit": round(tp, 2),
        "rr_ratio": 3.0, "liquidity_tier": "CRT Framework", "session_tag": "Price Action",
        "has_divergence": False, "seasonality_score": 0, "diamond_score": "A+" if touches <= 3 else "A",
        "confluence_level": subtype, # Es: "Classic 3 Candle CRT"
        "fvg_detected": False, "hitting_fvg": False, "smt_divergence": False, "adr_percent": 0,
        "rel_volume": 0, "volatility_warning": False, "is_golden_wick": False, "touches": touches,
        "market_bias": None, "max_favorable_excursion": 0.0
    }

def analyze_market_context():
    """Analizza indici principali e gli 11 settori S&P 500 per Rotazione Settoriale."""
    
    tickers_map = {
        # Indici
        'SPY': 'S&P 500', 'QQQ': 'Nasdaq', 'IWM': 'Russell 2000',
        # Settori
        'XLK': 'Technology', 'XLF': 'Financials', 'XLV': 'Health Care',
        'XLY': 'Consumer Discr', 'XLC': 'Communication', 'XLI': 'Industrials',
        'XLP': 'Consumer Staples', 'XLE': 'Energy', 'XLB': 'Materials',
        'XLRE': 'Real Estate', 'XLU': 'Utilities'
    }
    
    ticker_list = list(tickers_map.keys())
    
    try:
        # Scarica 1 mese di dati per tutti in una sola chiamata
        data = yf.download(ticker_list, period="1mo", interval="1d", progress=False, group_by='ticker')
        
        analysis = {"indices": {}, "sectors": {}}
        sector_momentum = {}
        
        for ticker, name in tickers_map.items():
            if ticker in data and not data[ticker].empty:
                df = data[ticker].dropna()
                if len(df) < 20: continue
                
                # Calcoli
                sma20 = df['Close'].rolling(window=20).mean().iloc[-1]
                curr_price = float(df['Close'].iloc[-1])
                
                # Prezzo di una settimana fa (5 giorni lavorativi fa)
                if len(df) >= 6:
                    price_5d_ago = float(df['Close'].iloc[-6])
                else:
                    price_5d_ago = float(df['Close'].iloc[0])
                
                # Bias (Sopra/Sotto SMA 20)
                bias = 'BULLISH' if curr_price > sma20 else 'BEARISH'
                
                # Momentum (% di crescita negli ultimi 5 giorni)
                momentum_pct = ((curr_price - price_5d_ago) / price_5d_ago) * 100
                
                info = {
                    "bias": bias,
                    "momentum_pct": round(momentum_pct, 2)
                }
                
                if ticker in ['SPY', 'QQQ', 'IWM']:
                    analysis["indices"][ticker] = info
                else:
                    analysis["sectors"][name] = info
                    sector_momentum[name] = momentum_pct
        
        # Determina i settori Leader e Laggard (I 3 più forti e i 3 più deboli)
        sorted_sectors = sorted(sector_momentum.items(), key=lambda x: x[1], reverse=True)
        top_3 = [s[0] for s in sorted_sectors[:3]]
        bottom_3 = [s[0] for s in sorted_sectors[-3:]]
        
        # Bias Globale basato su SPY e QQQ
        global_bias = 'NEUTRAL'
        spy_info = analysis["indices"].get('SPY', {})
        qqq_info = analysis["indices"].get('QQQ', {})
        
        if spy_info.get('bias') == 'BULLISH' and qqq_info.get('bias') == 'BULLISH':
            global_bias = 'BULLISH'
        elif spy_info.get('bias') == 'BEARISH' and qqq_info.get('bias') == 'BEARISH':
            global_bias = 'BEARISH'
            
        return {
            "global_bias": global_bias,
            "top_sectors": top_3,
            "bottom_sectors": bottom_3,
            "details": analysis
        }
        
    except Exception as e:
        logger.error(f"Errore analisi settoriale: {e}")
        return {"global_bias": "NEUTRAL", "top_sectors": [], "bottom_sectors": [], "details": {}}




def validate_existing_signals(ticker, df, active_signals_map):
    """
    Controlla se i segnali attivi per questo ticker sono scaduti (TP/SL).
    Ritorna una lista di aggiornamenti da fare al DB.
    Include logica di Breakeven (BE) se il prezzo ha raggiunto il 50% del TP.
    """
    updates = []
    if ticker not in active_signals_map:
        return updates

    signals = active_signals_map[ticker]
    curr_candle = df.iloc[-1]
    curr_high = float(curr_candle['High'])
    curr_low = float(curr_candle['Low'])
    curr_close = float(curr_candle['Close'])

    for sig in signals:
        try:
            signal_time = str(sig['created_at'])[:13] 
            candle_time = curr_candle.name.strftime('%Y-%m-%dT%H')
            if signal_time == candle_time:
                continue 
        except Exception as e:
            pass

        sl = float(sig['stop_loss'])
        tp = float(sig['take_profit'])
        entry = float(sig.get('entry_price', sl))
        s_type = sig['type']
        
        # --- LOGICA BREAKEVEN (BE) ---
        # Se il prezzo ha percorso il 50% verso il target, il nuovo SL è l'Entry.
        new_sl = sl
        is_trailing_be = False
        
        if 'bullish' in s_type:
            # Se siamo a metà strada per il TP
            if curr_high >= entry + (tp - entry) * 0.5:
                if sl < entry:
                    new_sl = entry
                    is_trailing_be = True
        elif 'bearish' in s_type:
            if curr_low <= entry - (entry - tp) * 0.5:
                if sl > entry:
                    new_sl = entry
                    is_trailing_be = True

        # Logica scadenza
        should_expire = False
        reason = ""

        if 'bullish' in s_type:
            if curr_low <= new_sl:
                should_expire = True
                reason = "STOPPED" if new_sl != entry else "BREAKEVEN"
            elif curr_high >= tp:
                should_expire = True
                reason = "PROFIT"
        elif 'bearish' in s_type:
            if curr_high >= new_sl:
                should_expire = True
                reason = "STOPPED" if new_sl != entry else "BREAKEVEN"
            elif curr_low <= tp:
                should_expire = True
                reason = "PROFIT"
        
        if should_expire:
            result_code = 'WIN' if reason == 'PROFIT' else 'LOSS' if reason == 'STOPPED' else 'BREAKEVEN'
            logger.info(f"Segnale SCADUTO per {ticker} ({sig['timeframe']}): {reason} -> {result_code}")
            updates.append({
                "id": sig['id'],
                "is_active": False,
                "result": result_code,
                "closed_at": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            })
        elif is_trailing_be:
            # Aggiorna solo lo Stop Loss nel DB
            logger.info(f"BE Triggerato per {ticker}: SL spostato a {entry}")
            updates.append({
                "id": sig['id'],
                "stop_loss": entry
            })

    return updates


def check_open_trades():
    """
    Controlla tutti i trade 'OPEN' nel database usando dati DAILY (molto più veloce).
    """
    try:
        response = supabase.table('crt_signals').select("*").or_("result.eq.OPEN,result.is.null").execute()
        open_signals = response.data
        
        if not open_signals:
            logger.info("Nessun trade OPEN da validare.")
            return

        logger.info(f"Validazione di {len(open_signals)} trade OPEN (Modalità Daily)...")
        updates_count = 0
        
        symbols = list(set([s['symbol'] for s in open_signals]))
        
        try:
            tickers_str = " ".join(symbols)
            if not tickers_str: return
            
            # OTTIMIZZAZIONE: Usiamo interval="1d" invece di "1m". 
            # Per trade Macro, basta sapere se High/Low di oggi ha toccato i livelli.
            curr_data = yf.download(tickers_str, period="5d", interval="1d", progress=False, group_by='ticker')
            
            for signal in open_signals:
                try:
                    sym = signal['symbol']
                    
                    if len(symbols) == 1:
                        df = curr_data
                    else:
                        if sym not in curr_data.columns.get_level_values(0):
                            continue
                        df = curr_data[sym]
                    
                    df = df.dropna()
                    if df.empty: continue
                    
                    # Prendi l'ultima candela disponibile (Oggi o Ieri chiusa)
                    last_candle = df.iloc[-1]
                    curr_high = float(last_candle['High'])
                    curr_low = float(last_candle['Low'])
                    result = None
                    if signal.get('stop_loss') is None or signal.get('take_profit') is None:
                        continue
                        
                    # Salta la chiusura immediata SOLO se è un trade Daily.
                    # Se è un trade 1H, vogliamo validarlo anche se è lo stesso giorno!
                    try:
                        if signal.get('timeframe') == '1D':
                            signal_date = str(signal['created_at'])[:10]
                            candle_date = last_candle.name.strftime('%Y-%m-%d')
                            if signal_date == candle_date:
                                continue
                    except Exception as e:
                        pass

                    sl = float(signal['stop_loss'])
                    tp = float(signal['take_profit'])
                    
                    # Logica: Controlliamo se High o Low della giornata hanno colpito i livelli
                    if 'bearish' in signal['type']:
                        # Short: Stop Loss se il prezzo SALE (High >= SL)
                        if curr_high >= sl: result = 'LOSS'
                        # Short: Take Profit se il prezzo SCENDE (Low <= TP)
                        elif curr_low <= tp: result = 'WIN'
                            
                    elif 'bullish' in signal['type']:
                        # Long: Stop Loss se il prezzo SCENDE (Low <= SL)
                        if curr_low <= sl: result = 'LOSS'
                        # Long: Take Profit se il prezzo SALE (High >= TP)
                        elif curr_high >= tp: result = 'WIN'
                    
                    if result:
                        logger.info(f"Trade CONCLUSO {sym}: {result}")
                        supabase.table('crt_signals').update({
                            "result": result,
                            "is_active": False,
                            "closed_at": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
                        }).eq('id', signal['id']).execute()
                        updates_count += 1
                        
                except Exception as inner_e:
                    logger.error(f"Errore validazione trade {signal['symbol']}: {inner_e}")
                    continue
                    
        except Exception as batch_e:
             logger.error(f"Errore download prezzi batch: {batch_e}")

        if updates_count > 0:
            logger.info(f"Aggiornati {updates_count} trade conclusi.")
            
    except Exception as e:
        logger.error(f"Errore generale in check_open_trades: {e}")


def main():
    # Fix console encoding on Windows for Emojis
    import sys
    if sys.platform.startswith('win'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    admin_log("INFO", "🚀 Avvio scansione CRT Flow...")
    start_time = time.time()
    
    # 0. Retry Uploads
    retry_failed_uploads()
    
    # 1. Analisi Bias di Mercato (Rotazione Settoriale)
    market_context = analyze_market_context()
    market_bias = market_context["global_bias"]
    admin_log("INFO", f"Global Market Bias: {market_bias}")
    admin_log("INFO", f"Leading Sectors: {', '.join(market_context.get('top_sectors', []))}")
    
    # Save Market Context to DB for Frontend
    try:
        bias_signal = {
            "symbol": "_MARKET_STATUS_", # Special Ticker
            "timeframe": "1D",
            "type": "market_bias",
            # Salviamo il bias globale qui per retrocompatibilità
            "session_tag": market_bias, 
            "is_active": True,
            "created_at": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
            "price": 0,
            "range_high": 0,
            "range_low": 0,
            # Trasformiamo l'intero dizionario in una stringa JSON e lo salviamo nel campo 'subtype'
            "subtype": json.dumps(market_context),
            "liquidity_tier": "Status",
            "rr_ratio": 0
        }
        # Delete old status
        supabase.table("crt_signals").delete().eq("symbol", "_MARKET_STATUS_").execute()
        # Insert new
        supabase.table("crt_signals").insert(bias_signal).execute()
    except Exception as e:
        logger.error(f"Errore salvataggio Market Status: {e}")

    # 1B. Fetch Scanner Config from DB
    scanner_config = {
        "min_volume": 0,
        "rvol_threshold": 1.5,
        "min_market_cap": 0
    }
    try:
        stats_response = supabase.table("launch_stats").select("scanner_min_volume, scanner_rvol_threshold, scanner_min_market_cap").limit(1).single().execute()
        if stats_response.data:
            d = stats_response.data
            scanner_config["min_volume"] = d.get("scanner_min_volume", 0)
            scanner_config["rvol_threshold"] = float(d.get("scanner_rvol_threshold", 1.5))
            scanner_config["min_market_cap"] = d.get("scanner_min_market_cap", 0)
            logger.info(f"Scanner Config caricata: {scanner_config}")
    except Exception as e:
        logger.error(f"Errore caricamento config scanner: {e}")

    # 2. Fetch Active Signals for Validation
    try:
        active_response = supabase.table("crt_signals").select("*").eq("is_active", True).execute()
        active_signals_list = active_response.data
        # Raggruppa per ticker: { 'AAPL': [sig1, sig2], ... }
        active_signals_map = {}
        for s in active_signals_list:
            t = s['symbol']
            if t not in active_signals_map: active_signals_map[t] = []
            active_signals_map[t].append(s)
        logger.info(f"Caricati {len(active_signals_list)} segnali attivi per validazione.")
    except Exception as e:
        logger.error(f"Errore fetch segnali attivi: {e}")
        active_signals_map = {}

    # 3. VALIDAZIONE AUTOMATICA (Paper Trading Update)
    logger.info("Esecuzione validazione trade automatici...")
    # check_open_trades() - Rimosso per evitare false LOSS con dati Daily su segnali 1H

    # --- ARGUMENT PARSING ---
    parser = argparse.ArgumentParser(description='CRT Flow Scanner')
    parser.add_argument('--all', action='store_true', help='Scan ALL indices')
    parser.add_argument('--sp500', action='store_true', help='Scan S&P 500')
    parser.add_argument('--nasdaq', action='store_true', help='Scan NASDAQ 100')
    parser.add_argument('--russell', action='store_true', help='Scan Russell 2000')
    args = parser.parse_args()

    scan_sp = args.sp500 or args.all
    scan_ndx = args.nasdaq or args.all
    scan_russell = args.russell or args.all

    # Default behavior if NO args are passed: Scan SP + NDX (Standard)
    if not (scan_sp or scan_ndx or scan_russell):
        logger.info("Nessun argomento specificato. Default: S&P 500 + NASDAQ 100.")
        scan_sp = True
        scan_ndx = True
    
    all_tickers = []
    if scan_sp: 
        all_tickers += get_sp500_tickers()
    if scan_ndx: 
        all_tickers += get_nasdaq100_tickers()
    if scan_russell: 
        all_tickers += get_russell2000_tickers()
    
    # ALWAYS SCAN FOREX & CRYPTO (Asset Class Expansion)
    all_tickers += get_forex_tickers()
    all_tickers += get_crypto_tickers()

    # Unione liste e rimozione duplicati
    tickers = list(set(all_tickers))
    logger.info(f"Totale Ticker unici iniziali: {len(tickers)}")
    
    # --- FILTRO MARKET CAP (< 10B) ---
    logger.info("Filtro Market Cap in corso... (Minimo 10B)")
    import concurrent.futures
    
    def check_mcap(t):
        try:
            ticker_obj = yf.Ticker(t)
            # Robust check for fast_info
            if hasattr(ticker_obj, 'fast_info') and ticker_obj.fast_info:
                try:
                    mcap = ticker_obj.fast_info.get("marketCap", 0)
                except:
                    # In case fast_info is not subscriptable or no .get method
                    mcap = 0
            else:
                mcap = 0
            return t if mcap >= 10_000_000_000 else None
        except:
            return None

    filtered_tickers = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(check_mcap, tickers):
            if result:
                filtered_tickers.append(result)
                
    tickers = filtered_tickers
    logger.info(f"Ticker rimanenti dopo filtro Market Cap (>= 10B): {len(tickers)}")
    if not tickers:
        logger.warning("Nessun ticker ha superato il filtro Market Cap. Esco.")
        return
    
    all_detected_signals = []
    expired_signals_updates = []

    for tf, cfg in TF_CONFIG.items():
        logger.info(f"=== Avvio scansione per Timeframe: {tf} ===")
        logger.info(f"Scaricamento dati in blocco (Bulk)...")
        try:
            # Download data for all tickers at once
            data = yf.download(
                tickers, 
                period=cfg['period'], 
                interval=cfg['interval'], 
                group_by='ticker', 
                threads=True, 
                progress=False
            )
        except Exception as down_err:
            logger.error(f"Errore critico yfinance bulk download (TF {tf}): {down_err}")
            continue

        if data.empty:
            logger.warning(f"Nessun dato scaricato da yfinance per TF {tf}.")
            continue
            
        logger.info(f"Download completato TF {tf}. Elaborazione {len(tickers)} ticker...")

        for ticker in tickers:
            try:
                # Estrazione dati per il singolo ticker dal DataFrame MultiIndex
                if len(tickers) == 1:
                    df = data
                else:
                    if ticker not in data.columns.get_level_values(0):
                        continue
                    df = data[ticker]
                
                df = df.dropna()
                if df.empty or df['Close'].isnull().all():
                    continue
                
                # FILTRO PENNY STOCK (Obbligatorio)
                try:
                    current_close = float(df['Close'].iloc[-1])
                    if current_close < 5.00:
                        continue
                except:
                    pass

                # A. Validazione Segnali Esistenti 
                updates = []
                if tf == "1H":
                    updates = validate_existing_signals(ticker, df, active_signals_map)
                    expired_signals_updates.extend(updates)

                # --- 🛑 FILTRO ANTI-SPAM (ONE ACTIVE TRADE POLICY) ---
                # Vogliamo sapere se questo ticker ha un trade ancora in corso
                has_active_trade = False
                if ticker in active_signals_map:
                    # Prendiamo gli ID dei segnali che sono appena scaduti in questo esatto ciclo
                    expired_ids = [u['id'] for u in updates]
                    
                    # Controlliamo se tra i segnali attivi ce n'è almeno uno che NON è appena scaduto
                    for sig in active_signals_map[ticker]:
                        if sig['id'] not in expired_ids:
                            has_active_trade = True
                            break
                
                # Se il ticker ha un trade già aperto, skippiamo TUTTE le detection e passiamo al prossimo asset
                if has_active_trade:
                    # De-commenta il logger qui sotto se vuoi vedere nel terminale i ticker ignorati
                    # logger.info(f"Skipping {ticker}: Trade già in corso.") 
                    continue
                # -----------------------------------------------------

                # B. Helper per l'allineamento al Trend
                def apply_trend_alignment(sig, bias):
                    is_bullish = 'bullish' in str(sig.get('type', '')).lower() or 'long' in str(sig.get('type', '')).lower()
                    is_bearish = 'bearish' in str(sig.get('type', '')).lower() or 'short' in str(sig.get('type', '')).lower()
                    
                    if is_bullish:
                        if bias == 'BULLISH':
                            sig['diamond_score'] = 'A++'
                            sig['trend_alignment'] = 'Trend-Aligned'
                        elif bias == 'NEUTRAL':
                            sig['diamond_score'] = 'B'
                            sig['trend_alignment'] = 'Neutral'
                        elif bias == 'BEARISH':
                            sig['diamond_score'] = 'C'
                            sig['trend_alignment'] = 'Counter-Trend'
                    elif is_bearish:
                        if bias == 'BEARISH':
                            sig['diamond_score'] = 'A++'
                            sig['trend_alignment'] = 'Trend-Aligned'
                        elif bias == 'NEUTRAL':
                            sig['diamond_score'] = 'B'
                            sig['trend_alignment'] = 'Neutral'
                        elif bias == 'BULLISH':
                            sig['diamond_score'] = 'C'
                            sig['trend_alignment'] = 'Counter-Trend'
                    else:
                        sig['trend_alignment'] = 'Neutral'
                    return sig

                # B. Detection nuovi segnali
                signal = detect_macro_sweep(ticker, df, tf, scanner_config)
                if signal:
                    signal['session_tag'] = f"Sweep - Bias {market_bias}"
                    signal['market_bias'] = market_bias
                    signal = apply_trend_alignment(signal, market_bias)
                    
                    # --- FILTRO QUALITÀ (SNIPER MODE) ---
                    # Prendiamo SOLO i segnali con confluenza macro A++ 
                    if signal.get('diamond_score') != 'A++':
                        continue
                        
                    all_detected_signals.append(signal)
                    admin_log("SUCCESS", f"💎 {ticker} [{tf}]: {signal['liquidity_tier']} Sweep - Score A++")

                # C. Detection TBS Pattern
                tbs_signal = detect_tbs_setup(ticker, df, tf, scanner_config)
                if tbs_signal:
                    # VOLUME BYPASS: Se l'asset non ha volumi affidabili, ignoriamo il filtro rvol
                    rel_vol = tbs_signal.get('rel_volume', 0)
                    rvol_threshold = float(scanner_config.get("rvol_threshold", 1.5))
                    
                    if has_reliable_volume(ticker):
                        # Per gli Stock il volume deve essere alto
                        if rel_vol < rvol_threshold:
                            tbs_signal = None
                    
                if tbs_signal:
                    tbs_signal['session_tag'] = f"TBS - Bias {market_bias}"
                    tbs_signal['market_bias'] = market_bias
                    tbs_signal = apply_trend_alignment(tbs_signal, market_bias)
                    
                    # --- FILTRO QUALITÀ (SNIPER MODE) ---
                    if tbs_signal.get('diamond_score') != 'A++':
                        continue

                    all_detected_signals.append(tbs_signal)
                    admin_log("SUCCESS", f"🐢 {ticker} [{tf}]: TBS Pattern - Score A++")

                # D. Detection 4 Models CRT
                crt_signal = detect_crt_models(ticker, df, tf, scanner_config)
                if crt_signal:
                    crt_signal['market_bias'] = market_bias
                    crt_signal = apply_trend_alignment(crt_signal, market_bias)
                    
                    # --- FILTRO QUALITÀ (SNIPER MODE) ---
                    if crt_signal.get('diamond_score') != 'A++':
                        continue

                    all_detected_signals.append(crt_signal)
                    admin_log("SUCCESS", f"🕯️ {ticker} [{tf}]: {crt_signal['subtype']} - Score A++")

            except Exception as e:
                logger.error(f"Errore elaborazione ticker {ticker} su {tf}: {e}")
            
    # 4. SALVATAGGIO SU SUPABASE
    
    # A. Aggiornamento segnali scaduti
    if expired_signals_updates:
        try:
            logger.info(f"Disattivazione di {len(expired_signals_updates)} segnali scaduti (TP/SL)...")
            
            # Group by update type to minimize DB calls
            updates_by_result = {}
            be_updates = []
            
            for u in expired_signals_updates:
                if 'result' in u:
                    res = u['result']
                    if res not in updates_by_result: updates_by_result[res] = []
                    updates_by_result[res].append(u['id'])
                elif 'stop_loss' in u:
                    be_updates.append(u)
                
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            
            # 1. Update concluded trades (Grouped)
            for result_code, ids in updates_by_result.items():
                for k in range(0, len(ids), 500):
                    batch = ids[k : k + 500]
                    supabase.table("crt_signals").update({
                        "is_active": False,
                        "result": result_code,
                        "closed_at": current_time
                    }).in_("id", batch).execute()
            
            # 2. Update BE moves (Individually or batching if many)
            for u in be_updates:
                supabase.table("crt_signals").update({
                    "stop_loss": u['stop_loss']
                }).eq("id", u['id']).execute()
                    
            if updates_by_result:
                logger.info(f"DB aggiornato con risultati: {list(updates_by_result.keys())}")
            if be_updates:
                logger.info(f"DB aggiornato con {len(be_updates)} spostamenti a BE.")
                
            # Final stats reporting
            win_count = len(updates_by_result.get('WIN', []))
            loss_count = len(updates_by_result.get('LOSS', []))
            be_count = len(updates_by_result.get('BREAKEVEN', [])) + len(be_updates)
            
            if win_count > 0 or loss_count > 0 or be_count > 0:
                summary_msg = f"📊 SUMMARY TRADE: {win_count} WIN, {loss_count} LOSS, {be_count} BE"
                admin_log("INFO", summary_msg)

        except Exception as e:
             logger.error(f"Errore aggiornamento segnali scaduti: {e}")

    # B. Inserimento nuovi segnali (Sempre come attivi)
    if all_detected_signals:
        try:
            # --- FILTRO DUPLICATI GIORNALIERI ---
            # Evitiamo di inserire lo stesso segnale (Ticker + TF + Type) se è già stato creato oggi
            try:
                # Fetch minimal data of signals created today
                # Usiamo il formato ISO (T00:00:00.000Z) per compatibilità ottimale con Supabase/Postgres
                today_date = time.strftime('%Y-%m-%d', time.gmtime())
                existing_today = supabase.table("crt_signals").select("symbol, timeframe, type").gte("created_at", f"{today_date}T00:00:00.000Z").execute()
                existing_set = set()
                if existing_today.data:
                    for s in existing_today.data:
                        existing_set.add((s['symbol'], s['timeframe'], s['type']))
                
                # Filter out what we already have
                new_unique_signals = []
                # We also track what we are about to insert to avoid duplicates internal to this run
                seen_in_run = set()
                
                for sig in all_detected_signals:
                    key = (sig['symbol'], sig['timeframe'], sig['type'])
                    if key not in existing_set and key not in seen_in_run:
                        new_unique_signals.append(sig)
                        seen_in_run.add(key)
                
                all_detected_signals = new_unique_signals
            except Exception as e:
                logger.error(f"Errore durante il filtraggio duplicati: {e}")

            if not all_detected_signals:
                logger.info("Nessun NUOVO segnale unico da inserire (tutti già presenti oggi).")
            else:
                logger.info(f"Inserimento di {len(all_detected_signals)} segnali unici su Supabase...")
                # Inserimento massivo con Fallback
                for j in range(0, len(all_detected_signals), 1000):
                    batch = all_detected_signals[j : j + 1000]
                    try:
                        supabase.table("crt_signals").insert(batch).execute()
                    except Exception as e:
                        logger.error(f"Errore insert batch: {e}")
                        logger.info("Tentativo salvataggio locale per questo batch...")
                        save_failed_signals(batch)
                
                logger.info("Database aggiornato con successo.")
        except Exception as e:
            logger.error(f"Errore generale durante l'upload su Supabase: {e}")
    else:
        logger.info("Nessun NUOVO segnale rilevato in questa scansione.")

    total_time = round(time.time() - start_time, 2)
    admin_log("INFO", f"✅ Scansione completata in {total_time}s.")

if __name__ == "__main__":
    main()