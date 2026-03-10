
import pandas as pd
import datetime
from scanner import validate_existing_signals

# Mock signal mapping
active_signals_map = {
    'AAPL': [{
        'id': 1,
        'created_at': datetime.datetime.now() - datetime.timedelta(hours=2),
        'stop_loss': 140,
        'take_profit': 160,
        'entry_price': 150,
        'type': 'bullish',
        'timeframe': '1H'
    }, {
        'id': 2,
        'created_at': datetime.datetime.now() - datetime.timedelta(hours=2),
        'stop_loss': 140,
        'take_profit': 160,
        'price': 150, # entry_price missing, uses price
        'type': 'bullish',
        'timeframe': '1H'
    }]
}

# Mock dataframe
df = pd.DataFrame({
    'High': [155, 155],
    'Low': [145, 145],
    'Close': [150, 150]
}, index=[pd.Timestamp.now() - datetime.timedelta(hours=1), pd.Timestamp.now()])

try:
    updates = validate_existing_signals('AAPL', df, active_signals_map)
    print(f"Updates: {updates}")
    print("Success: No NameError raised.")
except NameError as e:
    print(f"FAILED: NameError: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
