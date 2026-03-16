import yfinance as yf
import json
import sys

def validate_symbol(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Basic validation check
        if not info or 'regularMarketPrice' not in info and 'currentPrice' not in info:
            # Try history as fallback for some symbols
            hist = ticker.history(period="1d")
            if hist.empty:
                return {"success": False, "error": "Symbol not found or no data available"}
            
            price = hist['Close'].iloc[-1]
            high_52w = ticker.history(period="1y")['High'].max() if not hist.empty else price
        else:
            price = info.get('currentPrice') or info.get('regularMarketPrice')
            high_52w = info.get('fiftyTwoWeekHigh') or price
        
        name = info.get('longName') or info.get('shortName') or symbol.upper()
        
        return {
            "success": True,
            "symbol": symbol.upper(),
            "name": name,
            "price": float(price),
            "high_52w": float(high_52w),
            "discount": float(((high_52w - price) / high_52w) * 100) if high_52w > 0 else 0
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "No symbol provided"}))
    else:
        print(json.dumps(validate_symbol(sys.argv[1])))
