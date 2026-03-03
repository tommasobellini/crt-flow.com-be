import yfinance as yf
import time
import concurrent.futures

tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"] * 10 # 50 tickers
tickers = list(set(["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "JNJ"])) * 10 # 10 unique


def get_mcap(t):
    try:
        return t, yf.Ticker(t).fast_info.get("marketCap", 0)
    except Exception as e:
        return t, 0

start = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    results = list(executor.map(get_mcap, tickers))
    
print(f"Time taken: {time.time() - start:.2f}s")
for r in results[:5]:
    print(r)
