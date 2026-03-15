import os
import time
import requests
import io
import logging
import concurrent.futures
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from typing import List, Dict
from dotenv import load_dotenv
from supabase import create_client, Client

# --- 1. CONFIGURAZIONE LOGGING ---
logger = logging.getLogger("DcaScreener")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')

class SupabaseLoggingHandler(logging.Handler):
    def __init__(self, supabase_client):
        super().__init__()
        self.supabase = supabase_client
        self.source = "dca_screener"

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if "system_logs" in log_entry: return
            self.supabase.table("system_logs").insert({
                "level": record.levelname, "message": log_entry, "source": self.source
            }).execute()
        except: pass

def setup_logging(supabase_client):
    # Console Handler - Force UTF-8 for Windows compatibility (Emojis)
    import sys
    if sys.platform == "win32":
        import io
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
        except:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Supabase Handler
    if supabase_client:
        sb_handler = SupabaseLoggingHandler(supabase_client)
        sb_handler.setFormatter(formatter)
        logger.addHandler(sb_handler)

# --- 2. CONFIGURAZIONE SUPABASE ---
if os.path.exists(".env.local"):
    load_dotenv(".env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    # Use standard logging as fallback if setup fails
    logging.basicConfig(level=logging.INFO)
    logging.error("❌ Credenziali Supabase mancanti! Controlla il file .env.local")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Advanced Logging
setup_logging(supabase)

# --- 2. RECUPERO TICKERS (S&P 500 + NASDAQ 100) ---
def get_sp500_tickers() -> List[str]:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        res = requests.get(url, headers=headers)
        table = pd.read_html(io.StringIO(res.text))[0]
        return [str(t).replace('.', '-') for t in table['Symbol'].tolist()]
    except Exception as e:
        logger.error(f"Errore recupero S&P 500: {e}")
        return []

def get_nasdaq100_tickers() -> List[str]:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = 'https://en.wikipedia.org/wiki/NASDAQ-100'
        res = requests.get(url, headers=headers)
        tables = pd.read_html(io.StringIO(res.text))
        for t in tables:
            if 'Ticker' in t.columns: return [str(x).replace('.', '-') for x in t['Ticker'].tolist()]
            elif 'Symbol' in t.columns: return [str(x).replace('.', '-') for x in t['Symbol'].tolist()]
        return [str(x).replace('.', '-') for x in tables[4]['Ticker'].tolist()]
    except Exception as e:
        logger.error(f"Errore recupero Nasdaq 100: {e}")
        return []

# --- 3. MOTORE DELLO SCREENER ---
def check_market_cap(ticker_symbol: str):
    """Verifica velocemente se l'azienda è > 50 Miliardi di $."""
    try:
        ticker = yf.Ticker(ticker_symbol)
        if hasattr(ticker, 'fast_info'):
            mcap = ticker.fast_info.get("marketCap", 0)
            if mcap >= 50_000_000_000:
                name = ticker.info.get('shortName', ticker_symbol) if hasattr(ticker, 'info') else ticker_symbol
                return {"ticker": ticker_symbol, "name": name, "mcap": mcap}
    except:
        pass
    return None

def run_accumulation_screener():
    logger.info("🚀 Avvio Smart DCA Screener (Institutional Accumulation Radar)...")
    start_time = time.time()

    # 1. Raccolta Tickers
    all_tickers = list(set(get_sp500_tickers() + get_nasdaq100_tickers()))
    logger.info(f"Trovati {len(all_tickers)} ticker unici. Esecuzione filtro Market Cap (> $50B)...")

    # 2. Filtro Market Cap Parallelo (Estremamente veloce)
    valid_assets = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for result in executor.map(check_market_cap, all_tickers):
            if result: valid_assets.append(result)

    logger.info(f"🏢 Aziende qualificate (Mega Caps): {len(valid_assets)}")
    if not valid_assets: return

    valid_tickers = [a["ticker"] for a in valid_assets]
    asset_dict = {a["ticker"]: a for a in valid_assets} 

    # 3. Download Dati di Massa (1 Anno, Settimanale)
    logger.info("📊 Scaricamento dati settimanali in bulk da Yahoo Finance...")
    data = yf.download(valid_tickers, period="1y", interval="1wk", group_by='ticker', progress=False)
    
    opportunities = []

    # 4. Analisi Matematica Quant
    for ticker in valid_tickers:
        try:
            # Estrazione sicura dal MultiIndex di yfinance
            df = data[ticker] if len(valid_tickers) > 1 else data
            df = df.dropna()
            
            if df.empty or len(df) < 14:
                continue

            current_price = float(df['Close'].iloc[-1])
            high_52w = float(df['High'].max())
            
            if high_52w <= 0: continue
            
            drawdown_pct = ((current_price - high_52w) / high_52w) * 100

            # FILTRO 1: Deep Discount (-25% a -60%)
            if not (-60 <= drawdown_pct <= -25):
                continue

            # FILTRO 2: Institutional Exhaustion (Weekly RSI < 40)
            rsi_series = ta.rsi(df['Close'], length=14)
            if rsi_series is None or rsi_series.dropna().empty: continue
            
            current_rsi = float(rsi_series.iloc[-1])
            
            if current_rsi >= 40:
                continue

            # Assegnazione Status Visivo
            status = "PRIME DCA ZONE"
            if drawdown_pct <= -50:
                status = "EXTREME VALUE"
            elif drawdown_pct <= -40:
                status = "DEEP DISCOUNT"

            # Costruiamo il Record ESATTO per la tabella dca_assets
            opportunities.append({
                "symbol": ticker,
                "name": asset_dict[ticker]["name"],
                "price": round(current_price, 2),
                "discount": round(drawdown_pct, 2), # Negativo (es: -38.5)
                "rsi": round(current_rsi, 1),
                "market_cap": asset_dict[ticker]["mcap"],
                "status": status,
                # Formato ISO 8601 per timestamp Postgres
                "last_scanned_at": time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime()) 
            })
            logger.info(f"🎯 MATCH TROVATO: {ticker} a {drawdown_pct:.1f}% di sconto (RSI: {current_rsi:.1f})")

        except Exception as e:
            pass # Ignoriamo silenziosamente i ticker con dati sballati

    # 5. Sincronizzazione con SUPABASE
    logger.info(f"Analisi completata. {len(opportunities)} opportunità trovate. Sincronizzazione DB...")
    
    try:
        # A. Estrai i ticker attualmente nel DB
        existing_res = supabase.table("dca_assets").select("symbol").execute()
        existing_tickers = [row['symbol'] for row in existing_res.data] if existing_res.data else []
        
        new_tickers = [opp['symbol'] for opp in opportunities]
        
        # B. Elimina i ticker che non rispettano più i criteri (es. sono risaliti)
        tickers_to_delete = [t for t in existing_tickers if t not in new_tickers]
        if tickers_to_delete:
            supabase.table("dca_assets").delete().in_("symbol", tickers_to_delete).execute()
            logger.info(f"🗑️ Rimossi {len(tickers_to_delete)} asset scaduti: {tickers_to_delete}")
            
        # C. Upsert (Inserisci nuovi o Aggiorna esistenti)
        if opportunities:
            supabase.table("dca_assets").upsert(opportunities, on_conflict="symbol").execute()
            logger.info(f"✅ Upsert completato per {len(opportunities)} asset.")
            
    except Exception as e:
        logger.error(f"❌ Errore critico durante l'aggiornamento di Supabase: {e}")

    logger.info(f"🏁 Smart DCA Screener completato in {round(time.time() - start_time, 2)}s.")


if __name__ == "__main__":
    run_accumulation_screener()