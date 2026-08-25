"""Tunable parameters for Liquidity Sweep (Flipping Markets) screening."""

MIN_BARS_4H = 30
MIN_BARS_1H = 50
MIN_BARS_15M = 40
MIN_BARS_5M = 60

PIVOT_WINDOW = 2  # fractal: compare ±2 bars → 5-bar local extreme
FIB_OTE_RATIO = 0.71
SL_BUFFER_PCT = 0.0005
OB_ENTRY_TOLERANCE_PCT = 0.001  # 0.1% — fib must land in OB zone
SETUP_LOOKBACK = 20  # sweep/MSS must complete within last N LTF bars
MIN_RR = 1.0

# Universe filter: only large-cap and above
MIN_MARKET_CAP = 10_000_000_000  # $10B

# Legacy — kept for deprecated modules
EMA_PERIOD = 20
STRUCTURE_LOOKBACK = 20
RSI_PERIOD = 14
TP_RR_RATIO = 2.0

YF_PERIOD_1H = "60d"
YF_PERIOD_15M = "60d"
YF_PERIOD_5M = "30d"
LTF_INTERVAL = "15m"
