import os
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

def get_russell1000_tickers():
    """Tenta di recuperare una lista Russell 1000 (approssimata da file statico o ETF se possibile)."""
    # Nota: Non esiste una fonte Wikipedia stabile e pulita per Russell 1000 come per S&P 500.
    # Per ora ritorniamo lista vuota per evitare errori, o in futuro implementare scraping di ETF IWB.
    logger.warning("Scraping Russell 1000 non implementato (fonte instabile). Uso solo S&P 500 + NASDAQ 100.")
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

def detect_crt_logic(ticker, df, tf):
    if df is None or len(df) < 5:
        return None

    try:
        # Prendi le ultime due candele COMPLETATE
        prev_candle = df.iloc[-2]
        curr_candle = df.iloc[-1]

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
        
        # Bullish CRT
        if c_low < p_low and c_close > p_low:
            logger.info(f"MATCH BULLISH: {ticker} su {tf}")
            sl = c_low
            tp = p_high # Target prev High of the range/candle
            risk = c_close - sl
            reward = tp - c_close
            rr = round(reward / risk, 2) if risk > 0 else 0
            
            return {
                "symbol": ticker,
                "timeframe": tf,
                "type": "bullish_sweep",
                "range_high": p_high,
                "range_low": p_low,
                "price": c_close,
                "is_active": True,
                "stop_loss": round(sl, 2),
                "take_profit": round(tp, 2),
                "rr_ratio": rr,
                "liquidity_tier": tier,
                "session_tag": session,
                "fvg_detected": False,
                "smt_divergence": False
            }

        # Bearish CRT
        if c_high > p_high and c_close < p_high:
            logger.info(f"MATCH BEARISH: {ticker} su {tf}")
            sl = c_high
            tp = p_low
            risk = sl - c_close
            reward = c_close - tp
            rr = round(reward / risk, 2) if risk > 0 else 0

            return {
                "symbol": ticker,
                "timeframe": tf,
                "type": "bearish_sweep",
                "range_high": p_high,
                "range_low": p_low,
                "price": c_close,
                "is_active": True,
                "stop_loss": round(sl, 2),
                "take_profit": round(tp, 2),
                "rr_ratio": rr,
                "liquidity_tier": tier,
                "session_tag": session,
                "fvg_detected": False,
                "smt_divergence": False
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
            result_code = 'TP_HIT' if reason == 'PROFIT' else 'SL_HIT' if reason == 'STOPPED' else 'MANUAL_CLOSE'
            
            logger.info(f"Segnale SCADUTO per {ticker} ({sig['timeframe']}): {reason}")
            updates.append({
                "id": sig['id'],
                "is_active": False,
                "result": result_code
            })

    return updates

def main():
    # Fix console encoding on Windows for Emojis
    import sys
    if sys.platform.startswith('win'):
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass

    logger.info("Inizio scansione CRTFlow...")
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

    # 2. Fetch active signals per validazione
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

    tickers_sp500 = get_sp500_tickers()
    tickers_nasdaq = get_nasdaq100_tickers()
    tickers_russell = get_russell1000_tickers()

    # Unione liste e rimozione duplicati
    tickers = list(set(tickers_sp500 + tickers_nasdaq + tickers_russell))
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
                    
                    # A. Validazione Segnali Esistenti (Solo su 1D o timeframe corrente appropriato)
                    # Per semplicità validiamo ogni volta che abbiamo dati freschi
                    updates = validate_existing_signals(ticker, df, active_signals_map)
                    expired_signals_updates.extend(updates)

                    # B. Detection nuovi segnali
                    # 1. Scan timeframe base (1M, 1W, 1D, 1H)
                    sig = detect_crt_logic(ticker, df, tf)
                    if sig: 
                        # Check contro-trend
                        if (market_bias == 'BULLISH' and sig['type'] == 'bearish_sweep') or \
                           (market_bias == 'BEARISH' and sig['type'] == 'bullish_sweep'):
                           # Potremmo flaggarlo o scartarlo. Per ora lo teniamo ma potremmo aggiungere un campo 'warning'
                           pass
                        
                        # --- ALERT LOGIC ---
                        is_major = sig['liquidity_tier'] == 'Major'
                        is_good_setup = sig['timeframe'] in ['4H', '1D'] and sig['rr_ratio'] >= 2.0
                        
                        if is_major or is_good_setup:
                             send_telegram_alert(sig, market_bias)
                        # -------------------

                        all_detected_signals.append(sig)

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

            except Exception as e:
                logger.error(f"Errore download batch {tf}: {e}")
        
        # Pausa di cortesia per evitare rate limiting
        time.sleep(1)

    # 4. SALVATAGGIO SU SUPABASE
    
    # A. Aggiornamento segnali scaduti
    if expired_signals_updates:
        try:
            logger.info(f"Disattivazione di {len(expired_signals_updates)} segnali scaduti (TP/SL)...")
            # Supabase non supporta update bulk complessi facilmente, facciamo loop o chiamata singola per ID
            # Ottimizzazione: Raccogliamo tutti gli ID da disattivare
            ids_to_expire = [u['id'] for u in expired_signals_updates]
            # Batch updates in chunks of 500
            for k in range(0, len(ids_to_expire), 500):
                 batch_ids = ids_to_expire[k : k + 500]
                 supabase.table("crt_signals").update({"is_active": False}).in_("id", batch_ids).execute()
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