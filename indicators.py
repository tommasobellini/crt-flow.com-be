import pandas as pd
import logging
import yfinance as yf
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
    try:
        high = df['High']
        low = df['Low']
        close = df['Close'].shift(1)
        
        tr1 = high - low
        tr2 = (high - close).abs()
        tr3 = (low - close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean().iloc[-1]
        return float(atr)
    except:
        return 0.0

def get_wick_analysis(candle, type_direction: str, atr: float):
    """
    Analizza la wick in base alla logica Wyckoff (Effort is Result).
    Returns: (is_golden, is_volatile, wick_ratio)
    """
    try:
        full_range = float(candle['High']) - float(candle['Low'])
        if full_range == 0: return False, False, 0.0
        
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
        
        return bool(is_golden), bool(is_volatile), float(wick_ratio)
    except:
        return False, False, 0.0

def get_seasonality_score(month_num: int, trend_type: str) -> int:
    """
    Ritorna un bonus score based on simple seasonality rules.
    month_num: 1-12
    trend_type: 'bullish' or 'bearish'
    """
    score = 0
    # Sell in May (May-Sept often bearish/choppy)
    if trend_type == 'bearish':
        if month_num in [5, 8, 9]: score += 1
    
    # End of Year Rally (Oct-Dec) & Jan Effect
    if trend_type == 'bullish':
        if month_num in [10, 11, 12, 1]: score += 1
        
    return score

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
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

def check_divergence(df: pd.DataFrame, rsi_series: pd.Series, type_direction: str) -> bool:
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
             try:
                 prev_rsi = rsi_series.loc[prev_low_idx]
             except:
                 return False

             # Logica: Prezzo Current < Prezzo Prev (Lower Low) MA RSI Current > RSI Prev (Higher Low)
             if curr_low < prev_low and curr_rsi > prev_rsi:
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

def calculate_adr_percent(df: pd.DataFrame, period: int = 5) -> int:
    """Calcola % del range odierno rispetto all'ADR 5 giorni."""
    try:
        daily_ranges = df['High'] - df['Low']
        current_range = df['High'].iloc[-1] - df['Low'].iloc[-1]
        avg_range = daily_ranges.rolling(window=period).mean().iloc[-2] # Media precedenti
        
        if avg_range == 0: return 0
        return int((current_range / avg_range) * 100)
    except:
        return 0

def get_historical_seasonality(ticker: str) -> dict:
    """
    Fetch historical monthly data for the ticker and calculate 
    average monthly returns and win rates over the maximum available period.
    """
    try:
        # Fetch max 20 years of monthly data to keep it efficient
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * 20)
        
        df = yf.download(ticker, start=start_date, end=end_date, interval="1mo", progress=False)
        
        if df.empty or len(df) < 12:
            return {}
            
        # Calculate monthly percentage returns
        df['Return'] = df['Close'].pct_change()
        df = df.dropna()
        
        # Group by month
        df['Month'] = df.index.month
        
        monthly_stats = {}
        for month in range(1, 13):
            month_data = df[df['Month'] == month]
            if not month_data.empty:
                avg_return = month_data['Return'].mean()
                win_rate = (month_data['Return'] > 0).sum() / len(month_data)
                
                monthly_stats[str(month)] = {
                    "avg_return": float(round(avg_return * 100, 2)),
                    "win_rate": float(round(win_rate * 100, 2)),
                    "sample_size": int(len(month_data))
                }
            else:
                monthly_stats[str(month)] = {"avg_return": 0, "win_rate": 0, "sample_size": 0}
                
        return monthly_stats
    except Exception as e:
        logger.error(f"Error calculating historical seasonality for {ticker}: {e}")
        return {}

def detect_fvg_confluence(df: pd.DataFrame, type_direction: str) -> bool:
    """
    Rileva se il prezzo tocca un FVG opposto recente.
    Bearish Sweep -> Tocca FVG Bearish sopra?
    Bullish Sweep -> Tocca FVG Bullish sotto?
    """
    try:
        # Cerca FVG nelle ultime 30 candele
        lookback = 30
        fvgs = []
        
        for i in range(len(df)-lookback, len(df)-2):
            c1 = df.iloc[i]
            c2 = df.iloc[i+1] # Gap candle
            c3 = df.iloc[i+2]
            
            if type_direction == 'bearish':
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

import numpy as np

def calculate_poc(df: pd.DataFrame, lookback: int = 50, bins: int = 50) -> float:
    """
    Calcola il Point of Control (POC) sulle ultime 'lookback' candele.
    Il POC è il livello di prezzo dove è stato scambiato più volume.
    """
    try:
        # Assicuriamoci che ci sia la colonna Volume e che non sia vuota
        if 'Volume' not in df.columns or df['Volume'].sum() == 0:
            return None
            
        recent_df = df.iloc[-lookback:]
        
        min_price = float(recent_df['Low'].min())
        max_price = float(recent_df['High'].max())
        
        if min_price == max_price:
            return min_price
            
        # Creiamo i "bin" (livelli di prezzo)
        price_bins = np.linspace(min_price, max_price, bins)
        volume_profile = np.zeros(bins - 1)
        
        for _, row in recent_df.iterrows():
            v = float(row['Volume'])
            if v == 0: continue
                
            l = float(row['Low'])
            h = float(row['High'])
            
            # Troviamo a quali bin appartiene l'escursione di questa candela
            low_idx = np.digitize(l, price_bins) - 1
            high_idx = np.digitize(h, price_bins) - 1
            
            # Assicuriamoci che gli indici siano nei limiti
            low_idx = max(0, min(low_idx, bins - 2))
            high_idx = max(0, min(high_idx, bins - 2))
            
            # Distribuiamo il volume della candela equamente sui livelli che ha attraversato
            if high_idx == low_idx:
                volume_profile[low_idx] += v
            else:
                vol_per_bin = v / (high_idx - low_idx + 1)
                for i in range(low_idx, high_idx + 1):
                    volume_profile[i] += vol_per_bin
                    
        # Troviamo l'indice del bin con il volume massimo
        poc_idx = np.argmax(volume_profile)
        # Il prezzo POC è il punto medio di quel bin
        poc_price = (price_bins[poc_idx] + price_bins[poc_idx + 1]) / 2.0
        
        return float(poc_price)
    except Exception as e:
        return None
