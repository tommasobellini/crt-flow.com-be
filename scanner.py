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
import pandas_ta as ta
from notifications import send_telegram_alert
from indicators import calculate_atr, get_historical_seasonality

# SEASONALITY CACHE to avoid redundant API calls
SEASONALITY_CACHE = {}

# LIQUIDITY CACHE to avoid redundant HTF calls
LIQUIDITY_CACHE = {}

def prefetch_all_htf_liquidity(tickers):
    """
    Scarica i dati Daily (6 mesi) per TUTTI i ticker in una singola chiamata API
    e pre-calcola i livelli di liquidità HTF, salvandoli nella LIQUIDITY_CACHE globale.
    """
    global LIQUIDITY_CACHE
    logger.info(f"🌊 Pre-fetching dati Daily (6mo) in bulk per {len(tickers)} ticker (Calcolo HTF Pools)...")

    try:
        # BULK DOWNLOAD: 1 singola chiamata API!
        data = yf.download(tickers, period="6mo", interval="1d", group_by='ticker', progress=False, threads=True)

        if data.empty:
            logger.warning("Nessun dato Daily scaricato nel pre-fetch.")
            return

        for ticker in tickers:
            try:
                # Gestione sicura del MultiIndex di yfinance
                df = data[ticker] if len(tickers) > 1 else data
                df = clean_df(df.dropna())

                if df.empty or len(df) < 5:
                    continue

                # Calcolo dei livelli
                pdh = to_f(df['High'].iloc[-2])
                pdl = to_f(df['Low'].iloc[-2])

                df_w = df.resample('W').agg({'High': 'max', 'Low': 'min'}).dropna()
                pwh = to_f(df_w['High'].iloc[-2]) if len(df_w) >= 2 else None
                pwl = to_f(df_w['Low'].iloc[-2]) if len(df_w) >= 2 else None

                df_m = df.resample('ME').agg({'High': 'max', 'Low': 'min'}).dropna()
                pmh = to_f(df_m['High'].iloc[-2]) if len(df_m) >= 2 else None
                pml = to_f(df_m['Low'].iloc[-2]) if len(df_m) >= 2 else None

                # Salvataggio diretto in memoria locale
                LIQUIDITY_CACHE[ticker] = {
                    "PDH": pdh, "PDL": pdl,
                    "PWH": pwh, "PWL": pwl,
                    "PMH": pmh, "PML": pml
                }
            except Exception as e:
                pass # Ignoriamo silenziosamente i ticker difettosi

        logger.info(f"✅ HTF Pools calcolati e cachati per {len(LIQUIDITY_CACHE)} ticker in 1 sola chiamata API.")

    except Exception as e:
        logger.error(f"Errore critico durante il prefetch HTF: {e}")

def get_htf_liquidity_pools(ticker):
    """
    Legge i livelli HTF direttamente dalla memoria RAM (0 chiamate API).
    """
    if ticker in LIQUIDITY_CACHE:
        return LIQUIDITY_CACHE[ticker]
    return None

def fetch_seasonality_with_cache(ticker):
    if ticker not in SEASONALITY_CACHE:
        logger.info(f"📊 Recupero stagionalità storica per {ticker}...")
        SEASONALITY_CACHE[ticker] = get_historical_seasonality(ticker)
    return SEASONALITY_CACHE[ticker]

# 1. CONFIGURAZIONE LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')

class SupabaseLoggingHandler(logging.Handler):
    def __init__(self, supabase_client):
        super().__init__()
        self.supabase = supabase_client
        self.source = "scanner_engine"

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if "system_logs" in log_entry: return
            self.supabase.table("system_logs").insert({
                "level": record.levelname, "message": log_entry, "source": self.source
            }).execute()
        except: pass

def setup_logging():
    # File Handler with UTF-8
    file_handler = logging.FileHandler("scanner.log", encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console Handler - Force UTF-8 for Windows compatibility (Emojis)
    if sys.platform == "win32":
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
        except:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

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

    # FILTRO ORARIO (Killzones)
    # Assumendo che i dati yfinance arrivino in orario locale NY
    current_hour = df.index[-1].hour
    if current_hour not in [9, 10, 11, 13, 14, 15]:
        return None

    try:
        results = []
        # --- 1. DATA AGGREGATION (RESAMPLING) ---
        agg_map = {'High': 'max', 'Low': 'min', 'Close': 'last', 'Open': 'first'}
        
        # HTF Pools from centralized helper
        pools = get_htf_liquidity_pools(ticker)
        if not pools: return None

        prev_d = {"High": pools["PDH"], "Low": pools["PDL"]}
        prev_w = {"High": pools["PWH"], "Low": pools["PWL"]}
        prev_m = {"High": pools["PMH"], "Low": pools["PML"]}
        
        # Quarterly & Semi-Annual (Still aggregated locally for now or we could extend the helper)
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

                 # Filtro Displacement (Corpo > 50% del range per rotture decise)
                 c_body = abs(c_close - to_f(curr['Open']))
                 c_range = c_high - c_low
                 if c_range > 0 and (c_body / c_range) < 0.50:
                     return None # Breakout debole, scartalo

                 # Real Seasonality Logic
                 seas_data = fetch_seasonality_with_cache(ticker)
                 m_stats = seas_data.get(str(month_now), {})
                 seas_score = 0
                 if m_stats:
                     # Bearish: favor those with negative avg return or low win rate (<50%)
                     if m_stats.get('avg_return', 0) < 0: seas_score += 1
                     if m_stats.get('win_rate', 0) < 50: seas_score += 1
                 # Map level name to confluence code
                 confluence_code = "PWH" 
                 if "daily" in level_name: confluence_code = "PDH"
                 if "monthly" in level_name: confluence_code = "PMH"
                 if "quarterly" in level_name: confluence_code = "PQH"
                 if "semi" in level_name: confluence_code = "6MH"
                 
                 # Calculate Dynamic Buffer using ATR
                 atr = calculate_atr(df)
                 buffer = atr * 1.5 if atr > 0 else (c_high * 0.005)
                 
                 sl = c_high + buffer
                 risk = sl - c_close
                 # NUOVO CODICE (Intraday Risk Sizing)
                 min_risk = c_close * 0.004
                 
                 if risk < min_risk:
                     sl = c_close + min_risk
                     risk = sl - c_close
                 
                 # Map importance to Diamond Score
                 diamond_score = "B"
                 if importance_score == "GOD_MODE": diamond_score = "A++ (Institution)"
                 if importance_score == "A++": diamond_score = "A++ (Diamond Edge)"
                 if importance_score == "A": diamond_score = "A+"
                 if importance_score == "Daily": diamond_score = "A"

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
                     "is_golden_wick": True,
                     "seasonality_data": json.dumps(seas_data)
                  }
                 
             # B. Bullish Sweep
             if c_low < level_low and c_close > level_low:
                 sweep_diff = abs(level_low - c_low)
                 sweep_pct = sweep_diff / level_low
                 if sweep_pct < 0.0005: 
                     return None # Too small

                 # Filtro Displacement (Corpo > 50% del range per rotture decise)
                 c_body = abs(c_close - to_f(curr['Open']))
                 c_range = c_high - c_low
                 if c_range > 0 and (c_body / c_range) < 0.50:
                     return None # Breakout debole, scartalo

                 # Real Seasonality Logic
                 seas_data = fetch_seasonality_with_cache(ticker)
                 m_stats = seas_data.get(str(month_now), {})
                 seas_score = 0
                 if m_stats:
                     # Bullish: favor those with positive avg return or high win rate (>50%)
                     if m_stats.get('avg_return', 0) > 0: seas_score += 1
                     if m_stats.get('win_rate', 0) > 50: seas_score += 1
                 
                 confluence_code = "PWL"
                 if "monthly" in level_name: confluence_code = "PML"
                 if "quarterly" in level_name: confluence_code = "PQL"
                 if "semi" in level_name: confluence_code = "6ML"

                 diamond_score = "B"
                 if importance_score == "GOD_MODE": diamond_score = "A++"
                 if importance_score == "A++": diamond_score = "A++"
                 if importance_score == "A": diamond_score = "A+"

                 # Calculate Dynamic Buffer using ATR
                 atr = calculate_atr(df)
                 buffer = atr * 1.5 if atr > 0 else (c_close * 0.005)

                 # Swing Trading Check: Min risk 2% of price for big moves
                 sl = c_low - buffer
                 risk = c_close - sl
                 # NUOVO CODICE (Intraday Risk Sizing)
                 min_risk = c_close * 0.004
                 
                 if risk < min_risk:
                     sl = c_close - min_risk
                     risk = c_close - sl

                 # --- TARGET STRUTTURALE (Draw on Liquidity) ---
                 # Cerchiamo il massimo più alto delle ultime 25 candele
                 lookback_window = df.iloc[-25:-1]
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
             res_d = check_sweep("daily", to_f(prev_d['High']), to_f(prev_d['Low']), "Daily")
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



def detect_smc_orderpairing(ticker, df, tf, config=None, htf_pools=None):
    """
    Rileva esattamente il pattern 'CRT with TurtleSoup / Orderpairing'.
    Regole ferree:
    1. DEVE esserci un OHP (Old High Purged) o OLP (Old Low Purged).
    2. L'entrata NON è a mercato, ma un Limit Order al 50% della candela CRT.
    3. Lo Stop Loss usa l'ATR per evitare i 'Wicked Out' (Caccia agli stop).
    """
    if tf not in ['1H', '1D']: return None
    df = clean_df(df)
    if df is None or len(df) < 20: return None
    
    if not htf_pools:
        htf_pools = get_htf_liquidity_pools(ticker)
    if not htf_pools: return None

    try:
        # 1. Calcolo ATR per il buffer dello Stop Loss (La cura per il 37% Wicked Out)
        atr_series = ta.atr(df['High'], df['Low'], df['Close'], length=14)
        if atr_series is None or atr_series.empty: return None
        atr_val = float(atr_series.iloc[-1])

        # Analizziamo la candela appena chiusa (La potenziale CRT Candle)
        crt_candle = df.iloc[-1]
        c_open, c_close = to_f(crt_candle['Open']), to_f(crt_candle['Close'])
        c_high, c_low = to_f(crt_candle['High']), to_f(crt_candle['Low'])
        c_range = c_high - c_low

        if c_range == 0: return None

        # 2. FILTRO "TREND FAILURE" (La cura per il 53% Full Body Break)
        # Se il corpo è > 80% del range (Marubozu), è inerzia pura, non c'è rifiuto. Scartiamo.
        c_body = abs(c_close - c_open)
        if (c_body / c_range) > 0.80:
            return None

        # --- SETUP SHORT (Bearish TurtleSoup / OHP) ---
        # Condizione: La candela deve chiudere rossa (c_close < c_open)
        if c_close < c_open:
            swept_level = None
            
            # Cerca se ha purgato un OLD HIGH (OHP)
            for p_name, p_val in htf_pools.items():
                if p_val is None: continue
                if "H" in p_name: # PDH, PWH, PMH
                    # Regola OHP: Il massimo ha rotto il livello, ma il corpo ha chiuso SOTTO
                    if c_high > p_val and c_close < p_val:
                        swept_level = p_name
                        break
            
            if swept_level:
                # LA MAGIA DELL'IMMAGINE 2: Entrata al 50% della CRT Candle
                crt_50_level = c_low + (c_range * 0.5)
                
                # Stop Loss: Massimo assoluto + 0.5 ATR (Stanza per respirare)
                sl = c_high + (atr_val * 0.5)
                
                entry = crt_50_level
                risk = sl - entry
                if risk <= 0: return None
                
                # Target: Strutturale (es. minimo delle ultime 20 candele) o Fisso 1:3
                lookback_low = df['Low'].iloc[-20:-1].min()
                tp = min(lookback_low, entry - (risk * 3.0)) 
                
                actual_rr = (entry - tp) / risk
                if actual_rr < 2.0: return None

                return create_smc_signal(ticker, tf, "bearish_tbs", f"Orderpairing 50% on {swept_level} Purged", c_high, c_low, entry, sl, tp)

        # --- SETUP LONG (Bullish TurtleSoup / OLP) ---
        # Condizione: La candela deve chiudere verde (c_close > c_open)
        elif c_close > c_open:
            swept_level = None
            
            # Cerca se ha purgato un OLD LOW (OLP)
            for p_name, p_val in htf_pools.items():
                if p_val is None: continue
                if "L" in p_name: # PDL, PWL, PML
                    # Regola OLP: Il minimo ha rotto il livello, ma il corpo ha chiuso SOPRA
                    if c_low < p_val and c_close > p_val:
                        swept_level = p_name
                        break
            
            if swept_level:
                # Entrata al 50% della CRT Candle
                crt_50_level = c_low + (c_range * 0.5)
                
                # Stop Loss: Minimo assoluto - 0.5 ATR
                sl = c_low - (atr_val * 0.5)
                
                entry = crt_50_level
                risk = entry - sl
                if risk <= 0: return None
                
                # Target: Massimo delle ultime 20 candele
                lookback_high = df['High'].iloc[-20:-1].max()
                tp = max(lookback_high, entry + (risk * 3.0))
                
                actual_rr = (tp - entry) / risk
                if actual_rr < 2.0: return None

                return create_smc_signal(ticker, tf, "bullish_tbs", f"Orderpairing 50% on {swept_level} Purged", c_high, c_low, entry, sl, tp)

    except Exception as e:
        logger.error(f"Errore SMC Orderpairing per {ticker}: {e}")

    return None

def create_smc_signal(ticker, tf, s_type, subtype, high, low, entry, sl, tp):
    """Helper pulito per generare il dizionario del segnale Limit (Pending)"""
    return {
        "symbol": ticker, "timeframe": tf, "type": s_type, "subtype": subtype,
        "range_high": round(high, 2), "range_low": round(low, 2), 
        "price": round(entry, 2), # Questo è il prezzo TRIGGER attuale per Supabase
        "entry_price": round(entry, 2),
        "result": None, "is_active": True, 
        "status": "pending", # FONDAMENTALE: L'ordine è pendente, aspetta il ritracciamento!
        "stop_loss": round(sl, 2), "take_profit": round(tp, 2),
        "rr_ratio": round(abs(entry-tp)/abs(entry-sl), 1), 
        "liquidity_tier": "HTF Sweep", "session_tag": "SMC Limit Order",
        "diamond_score": "A++", "confluence_level": "Orderpairing 50%",
        "has_divergence": False, "seasonality_score": 0, "seasonality_data": "{}", 
        "fvg_detected": False, "hitting_fvg": False, "smt_divergence": False, "adr_percent": 0,
        "rel_volume": 0, "volatility_warning": False, "is_golden_wick": True, "touches": 1,
        "market_bias": None, "max_favorable_excursion": 0.0, "trigger_candles": None
    }

def detect_golden_wick(ticker, df, tf, config=None):
    """
    Rileva le 'Sponsor Candles' (Wick lunghe) e calcola la zona di entrata
    istituzionale (dal 50% al 100% del riempimento della wick).
    """
    if tf not in ['1D', '1H']: return None
    df = clean_df(df)
    if df is None or len(df) < 25: return None

    try:
        c = df.iloc[-1]
        c_open, c_close, c_high, c_low = to_f(c['Open']), to_f(c['Close']), to_f(c['High']), to_f(c['Low'])
        c_range = c_high - c_low

        if c_range <= 0: return None

        # REGOLA: Il range della candela deve essere anomalo (> 1.4x la media)
        avg_range = (df['High'] - df['Low']).rolling(20).mean().iloc[-2]
        if c_range < (avg_range * 1.4):
            return None

        upper_wick = c_high - max(c_open, c_close)
        lower_wick = min(c_open, c_close) - c_low

        signal_data = None

        # --- SETUP LONG (Wick in basso) ---
        if lower_wick > (c_range * 0.45) and c_close > (c_low + c_range * 0.60):
            
            wick_100 = c_low
            wick_50 = c_low + (lower_wick * 0.5)
            
            # Calculate Dynamic Buffer using ATR
            atr = calculate_atr(df)
            buffer = atr * 0.3 if atr > 0 else (wick_50 * 0.002)
            
            sl = wick_100 - buffer
            risk_from_50 = wick_50 - sl
            
            # NUOVO FIX: Intraday Risk Sizing di sicurezza
            min_risk = wick_50 * 0.004
            if risk_from_50 < min_risk:
                risk_from_50 = min_risk
                sl = wick_50 - risk_from_50 # Abbassiamo lo SL per garantire il buffer minimo

            if risk_from_50 <= 0: return None
            
            tp = wick_50 + (risk_from_50 * 2.0)
            
            signal_data = create_signal_dict(ticker, tf, "bullish_wick", "Golden Wick (Buy Zone)", c_high, c_low, c_close, sl, tp, 1, status="pending")
            signal_data['entry_price'] = round(wick_50, 2)
            signal_data['range_low'] = round(wick_100, 2) 
            signal_data['rr_ratio'] = 2.0
            signal_data['diamond_score'] = "A++"
            signal_data['session_tag'] = "Limit Order @ 50% Wick"
            signal_data['trigger_candles'] = json.dumps([int(df.index[-1].timestamp())])

        # --- SETUP SHORT (Wick in alto) ---
        elif upper_wick > (c_range * 0.45) and c_close < (c_high - c_range * 0.60):
            
            wick_100 = c_high
            wick_50 = c_high - (upper_wick * 0.5)
            
            # Calculate Dynamic Buffer using ATR
            atr = calculate_atr(df)
            buffer = atr * 0.3 if atr > 0 else (wick_50 * 0.002)
            
            sl = wick_100 + buffer
            risk_from_50 = sl - wick_50
            
            # NUOVO FIX: Intraday Risk Sizing di sicurezza
            min_risk = wick_50 * 0.004
            if risk_from_50 < min_risk:
                risk_from_50 = min_risk
                sl = wick_50 + risk_from_50 # Alziamo lo SL per garantire il buffer minimo

            if risk_from_50 <= 0: return None
            
            tp = wick_50 - (risk_from_50 * 2.0)
            
            signal_data = create_signal_dict(ticker, tf, "bearish_wick", "Golden Wick (Sell Zone)", c_high, c_low, c_close, sl, tp, 1, status="pending")
            signal_data['entry_price'] = round(wick_50, 2)
            signal_data['range_high'] = round(wick_100, 2)
            signal_data['rr_ratio'] = 2.0
            signal_data['diamond_score'] = "A++"
            signal_data['session_tag'] = "Limit Order @ 50% Wick"
            signal_data['trigger_candles'] = json.dumps([int(df.index[-1].timestamp())])

        return signal_data

    except Exception as e:
        return None

def create_signal_dict(ticker, tf, s_type, subtype, high, low, price, sl, tp, touches, status="active"):
    return {
        "symbol": ticker, "timeframe": tf, "type": s_type, "subtype": subtype,
        "range_high": high, "range_low": low, "price": price, "entry_price": price,
        "result": None, "is_active": True, "status": status, "stop_loss": round(sl, 2), "take_profit": round(tp, 2),
        "rr_ratio": 3.0, "liquidity_tier": "CRT Framework", "session_tag": "Price Action",
        "has_divergence": False, "seasonality_score": 0, "seasonality_data": json.dumps(fetch_seasonality_with_cache(ticker)), "diamond_score": "A+" if touches <= 3 else "A",
        "confluence_level": subtype, # Es: "Classic 3 Candle CRT"
        "fvg_detected": False, "hitting_fvg": False, "smt_divergence": False, "adr_percent": 0,
        "rel_volume": 0, "volatility_warning": False, "is_golden_wick": False, "touches": touches,
        "market_bias": None, "max_favorable_excursion": 0.0, "trigger_candles": None
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
        entry = float(sig.get('entry_price', sig.get('price')))
        s_type = sig['type']
        status = sig.get('status', 'active')
        
        # --- GESTIONE SEGNALI PENDING (Limit Orders) ---
        if status == 'pending':
            triggered = False
            missed = False
            
            if 'bullish' in s_type:
                if curr_low <= entry:
                    triggered = True
                elif curr_high >= tp:
                    missed = True
            elif 'bearish' in s_type:
                if curr_high >= entry:
                    triggered = True
                elif curr_low <= tp:
                    missed = True
            
            if triggered:
                logger.info(f"⚡ {ticker} [{sig['timeframe']}]: Limit Order ESEGUITO @ {entry}")
                updates.append({
                    "id": sig['id'],
                    "status": 'active'
                })
                # Una volta attivato, continuiamo la validazione come trade attivo per la candela corrente
                status = 'active'
            elif missed:
                logger.info(f"👻 {ticker} [{sig['timeframe']}]: Ghost Win EVITATA (Target hit prima dell'entry). Segnale mancato.")
                updates.append({
                    "id": sig['id'],
                    "is_active": False,
                    "status": 'missed',
                    "result": 'MISSED',
                    "closed_at": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
                })
                continue # Skip rest of validation for this signal
            else:
                continue # Ancora in attesa, skip validation

        # --- LOGICA BREAKEVEN (BE) ---
        # Solo per trade già ATTIVI
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
        exit_reason_text = "Standard Exit"

        if 'bullish' in s_type:
            if curr_low <= new_sl:
                should_expire = True
                reason = "STOPPED" if new_sl != entry else "BREAKEVEN"
                
                # --- AUTOPSIA DEL LOSS (BULLISH) ---
                if reason == "STOPPED":
                    if curr_close > new_sl:
                        exit_reason_text = "Stop Hunt (Wicked Out)"
                    elif curr_high >= entry + (tp - entry) * 0.8:
                        exit_reason_text = "Greed (Missed TP by <20%)"
                    else:
                        exit_reason_text = "Trend Failure (Full Body Break)"

            elif curr_high >= tp:
                should_expire = True
                reason = "PROFIT"
                
                # --- ANALISI DEL WIN (BULLISH) ---
                if curr_low <= entry - (entry - sl) * 0.8:
                    exit_reason_text = "Struggle Hit (Almost Stopped)"
                else:
                    exit_reason_text = "Clean Snipe"

        elif 'bearish' in s_type:
            if curr_high >= new_sl:
                should_expire = True
                reason = "STOPPED" if new_sl != entry else "BREAKEVEN"
                
                # --- AUTOPSIA DEL LOSS (BEARISH) ---
                if reason == "STOPPED":
                    if curr_close < new_sl:
                        exit_reason_text = "Stop Hunt (Wicked Out)"
                    elif curr_low <= entry - (entry - tp) * 0.8:
                        exit_reason_text = "Greed (Missed TP by <20%)"
                    else:
                        exit_reason_text = "Trend Failure (Full Body Break)"

            elif curr_low <= tp:
                should_expire = True
                reason = "PROFIT"
                
                # --- ANALISI DEL WIN (BEARISH) ---
                if curr_high >= entry + (sl - entry) * 0.8:
                    exit_reason_text = "Struggle Hit (Almost Stopped)"
                else:
                    exit_reason_text = "Clean Snipe"
        
        if should_expire:
            result_code = 'WIN' if reason == 'PROFIT' else 'LOSS' if reason == 'STOPPED' else 'BREAKEVEN'
            logger.info(f"Segnale SCADUTO per {ticker} ({sig['timeframe']}): {reason} -> {result_code} ({exit_reason_text})")
            updates.append({
                "id": sig['id'],
                "is_active": False,
                "result": result_code,
                "exit_reason": exit_reason_text,
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
    setup_logging()
    setup_supabase()
    
    # Fix console encoding on Windows for Emojis
    if sys.platform.startswith('win'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    logger.info("🚀 Avvio scansione CRT Flow...")
    start_time = time.time()
    
    # 0. Retry Uploads
    retry_failed_uploads()
    
    # 1. Analisi Bias di Mercato (Rotazione Settoriale)
    market_context = analyze_market_context()
    market_bias = market_context["global_bias"]
    logger.info(f"Global Market Bias: {market_bias}")
    logger.info(f"Leading Sectors: {', '.join(market_context.get('top_sectors', []))}")
    
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
    
    # --- NOVITÀ: PRE-FETCH HTF POOLS ---
    prefetch_all_htf_liquidity(tickers)
    # -----------------------------------
    
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

                # C. Pre-fetch HTF Liquidity Pools for filters
                htf_pools = get_htf_liquidity_pools(ticker)
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

                # --- NUOVO CODICE SMC ---
                smc_signal = detect_smc_orderpairing(ticker, df, tf, scanner_config, htf_pools)
                if smc_signal:
                    smc_signal['market_bias'] = market_bias
                    smc_signal = apply_trend_alignment(smc_signal, market_bias)
                    
                    all_detected_signals.append(smc_signal)
                    logger.info(f"💎 {ticker} [{tf}]: {smc_signal['subtype']} - LIMIT @ {smc_signal['entry_price']}")

                # E. Detection Golden Wick
                gw_signal = detect_golden_wick(ticker, df, tf, scanner_config)
                if gw_signal:
                    gw_signal['market_bias'] = market_bias
                    gw_signal = apply_trend_alignment(gw_signal, market_bias)
                    if gw_signal.get('diamond_score') == 'A++':
                        all_detected_signals.append(gw_signal)
                        logger.info(f"✨ {ticker} [{tf}]: Golden Wick Pattern - Score A++")

            except Exception as e:
                logger.error(f"Errore elaborazione ticker {ticker} su {tf}: {e}")
            
    # 4. SALVATAGGIO SU SUPABASE
    
    # A. Aggiornamento segnali scaduti
    if expired_signals_updates:
        try:
            logger.info(f"Disattivazione di {len(expired_signals_updates)} segnali scaduti (TP/SL)...")
            
            # Group by both result and exit_reason to preserve autopsy data
            updates_by_group = {}
            be_updates = []
            status_updates = []
            
            for u in expired_signals_updates:
                if 'result' in u:
                    res = u['result']
                    reason_txt = u.get('exit_reason', 'Standard Exit')
                    group_key = (res, reason_txt)
                    
                    if group_key not in updates_by_group: 
                        updates_by_group[group_key] = []
                    updates_by_group[group_key].append(u['id'])
                elif 'stop_loss' in u:
                    be_updates.append(u)
                elif 'status' in u:
                    status_updates.append(u)
                
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            
            # 1. Update concluded trades (Grouped by Result + Reason)
            for (result_code, exit_reason_val), ids in updates_by_group.items():
                for k in range(0, len(ids), 500):
                    batch = ids[k : k + 500]
                    supabase.table("crt_signals").update({
                        "is_active": False,
                        "result": result_code,
                        "exit_reason": exit_reason_val,
                        "closed_at": current_time
                    }).in_("id", batch).execute()
            
            # 2. Update status and sl moves (Individually or batching if many)
            for u in be_updates:
                supabase.table("crt_signals").update({"stop_loss": u['stop_loss']}).eq("id", u['id']).execute()
            
            for u in status_updates:
                supabase.table("crt_signals").update({"status": u['status']}).eq("id", u['id']).execute()
                    
            if updates_by_group:
                logger.info(f"DB aggiornato con risultati: {list(updates_by_group.keys())}")
            if be_updates:
                logger.info(f"DB aggiornato con {len(be_updates)} spostamenti a BE.")
            if status_updates:
                logger.info(f"DB aggiornato con {len(status_updates)} inneschi ordini Limite.")
                
            # Final stats reporting
            win_count = sum(len(ids) for (res, reason), ids in updates_by_group.items() if res == 'WIN')
            loss_count = sum(len(ids) for (res, reason), ids in updates_by_group.items() if res == 'LOSS')
            be_count = sum(len(ids) for (res, reason), ids in updates_by_group.items() if res == 'BREAKEVEN') + len(be_updates)
            
            if win_count > 0 or loss_count > 0 or be_count > 0:
                summary_msg = f"📊 SUMMARY TRADE: {win_count} WIN, {loss_count} LOSS, {be_count} BE"
                logger.info(summary_msg)

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
    logger.info(f"✅ Scansione completata in {total_time}s.")

if __name__ == "__main__":
    main()