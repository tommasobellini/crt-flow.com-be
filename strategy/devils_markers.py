"""
Devil's Markers — replicated from TradingView "Devils Mark" Pine.

Pine core condition (current / HTF bar):
    open == high   OR   open == low

- Open == High → BEARISH mark at Open (no upper wick from the open)
- Open == Low  → BULLISH mark at Open (no lower wick from the open)
- Invalidation: later bar range crosses through the mark level (Pine line delete)

Trade layer (our addition for performance tracking):
- SL = mark level (Open)
- Entry = Close of the mark candle (must leave room vs SL)
- TP = prior structural swing
"""
from __future__ import annotations

from typing import Literal

import pandas as pd

PIVOT_WINDOW = 3
RECENT_CANDLES = 3
MIN_RR = 1.5
MIN_RISK_PCT = 0.0008
MAX_RR = 25.0
MAX_TARGET_MOVE_PCT = 0.35
# Float tolerance for open==high / open==low (Yahoo rarely exact)
PRICE_EQ_TOL = 1e-6

Direction = Literal["BULLISH", "BEARISH"]
Status = Literal["ACTIVE", "INVALIDATED", "CLOSED_WIN", "CLOSED_LOSS"]
TfLabel = Literal["4H", "1D", "1W", "1M"]


def _to_f(val) -> float:
    return float(val)


def _to_unix(ts: pd.Timestamp) -> int:
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


def _price_eq(a: float, b: float) -> bool:
    """Pine `open == high` / `open == low` with float-safe equality."""
    scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) <= max(scale * PRICE_EQ_TOL, 1e-8)


def _is_open_eq_high(o: float, h: float) -> bool:
    return _price_eq(o, h)


def _is_open_eq_low(o: float, l: float) -> bool:
    return _price_eq(o, l)


def _find_prior_swing_high(df: pd.DataFrame, before_i: int, window: int = PIVOT_WINDOW) -> float | None:
    for i in range(before_i - 1, window - 1, -1):
        h = _to_f(df.iloc[i]["High"])
        left = df["High"].iloc[i - window : i]
        right = df["High"].iloc[i + 1 : min(i + window + 1, before_i)]
        if len(left) < window or len(right) < 1:
            continue
        if h >= float(left.max()) and h >= float(right.max()):
            return h
    start = max(0, before_i - 40)
    if start >= before_i:
        return None
    return float(df["High"].iloc[start:before_i].max())


def _find_prior_swing_low(df: pd.DataFrame, before_i: int, window: int = PIVOT_WINDOW) -> float | None:
    for i in range(before_i - 1, window - 1, -1):
        l = _to_f(df.iloc[i]["Low"])
        left = df["Low"].iloc[i - window : i]
        right = df["Low"].iloc[i + 1 : min(i + window + 1, before_i)]
        if len(left) < window or len(right) < 1:
            continue
        if l <= float(left.min()) and l <= float(right.min()):
            return l
    start = max(0, before_i - 40)
    if start >= before_i:
        return None
    return float(df["Low"].iloc[start:before_i].min())


def _compute_rr(direction: Direction, entry: float, stop: float, target: float) -> float | None:
    if entry <= 0 or stop <= 0 or target <= 0:
        return None
    if direction == "BULLISH":
        risk = entry - stop
        reward = target - entry
    else:
        risk = stop - entry
        reward = entry - target
    if risk <= 0 or reward <= 0:
        return None
    if risk / entry < MIN_RISK_PCT:
        return None
    if reward / entry > MAX_TARGET_MOVE_PCT:
        return None
    rr = reward / risk
    if rr > MAX_RR:
        return None
    return round(rr, 4)


def _bar_crosses_level(
    level: float,
    high: float,
    low: float,
    prev_high: float | None,
    prev_low: float | None,
) -> bool:
    """Pine: delete line when bar range crosses the mark."""
    if high > level and low < level:
        return True
    if prev_high is not None and high > level and prev_high <= level:
        return True
    if prev_low is not None and low < level and prev_low >= level:
        return True
    return False


def _status_after_marker(df: pd.DataFrame, mark_i: int, level: float) -> Status:
    """ACTIVE until a later bar crosses the Devil's Mark level (Pine invalidation)."""
    after = df.iloc[mark_i + 1 :]
    if after.empty:
        return "ACTIVE"

    prev_high = _to_f(df.iloc[mark_i]["High"])
    prev_low = _to_f(df.iloc[mark_i]["Low"])
    for _, row in after.iterrows():
        high = _to_f(row["High"])
        low = _to_f(row["Low"])
        if _bar_crosses_level(level, high, low, prev_high, prev_low):
            return "INVALIDATED"
        prev_high, prev_low = high, low
    return "ACTIVE"


def _build_signal(
    *,
    ticker: str,
    timeframe: TfLabel,
    direction: Direction,
    devil: float,
    entry: float,
    stop: float,
    target: float,
    rr: float,
    status: Status,
    mark_i: int,
    df: pd.DataFrame,
) -> dict:
    ts = df.index[mark_i]
    return {
        "ticker": ticker.upper(),
        "timeframe": timeframe,
        "direction": direction,
        "devils_marker_price": round(devil, 4),
        "entry_price": entry,
        "stop_loss": round(stop, 4),
        "target": round(target, 4),
        "rr_ratio": rr,
        "is_high_rr": rr >= MIN_RR,
        "status": status,
        "pattern_at": pd.Timestamp(ts).isoformat(),
        "pattern_unix": _to_unix(pd.Timestamp(ts)),
        "cisd_level": entry,
        "is_cisd_broken": True,
        "cisd_label": "ENTRY",
        "meta_rule": "open_eq_high" if direction == "BEARISH" else "open_eq_low",
    }


def find_devils_markers(
    df: pd.DataFrame,
    *,
    ticker: str,
    timeframe: TfLabel,
    recent_candles: int = RECENT_CANDLES,
    include_invalidated: bool = False,
) -> list[dict]:
    """
    Pine Devils Mark on the last `recent_candles` bars:
        open == high  → BEARISH at Open
        open == low   → BULLISH at Open
    """
    if df is None or df.empty or len(df) < 10:
        return []

    n = len(df)
    start_i = max(1, n - recent_candles)
    signals: list[dict] = []

    for i in range(n - 1, start_i - 1, -1):
        row = df.iloc[i]
        o, h, l, c = (
            _to_f(row["Open"]),
            _to_f(row["High"]),
            _to_f(row["Low"]),
            _to_f(row["Close"]),
        )

        # --- Pine: open == high ---
        if _is_open_eq_high(o, h):
            level = o
            # Need close below mark for short entry risk
            if c >= level:
                continue
            target = _find_prior_swing_low(df, i)
            if target is None or target >= c or target <= 0:
                continue
            entry = round(c, 4)
            stop = level
            rr = _compute_rr("BEARISH", entry, stop, target)
            if rr is None:
                continue
            status = _status_after_marker(df, i, level)
            if status == "INVALIDATED" and not include_invalidated:
                continue
            signals.append(
                _build_signal(
                    ticker=ticker,
                    timeframe=timeframe,
                    direction="BEARISH",
                    devil=level,
                    entry=entry,
                    stop=stop,
                    target=target,
                    rr=rr,
                    status=status,
                    mark_i=i,
                    df=df,
                )
            )

        # --- Pine: open == low ---
        if _is_open_eq_low(o, l):
            level = o
            if c <= level:
                continue
            target = _find_prior_swing_high(df, i)
            if target is None or target <= c or target <= 0:
                continue
            entry = round(c, 4)
            stop = level
            rr = _compute_rr("BULLISH", entry, stop, target)
            if rr is None:
                continue
            status = _status_after_marker(df, i, level)
            if status == "INVALIDATED" and not include_invalidated:
                continue
            signals.append(
                _build_signal(
                    ticker=ticker,
                    timeframe=timeframe,
                    direction="BULLISH",
                    devil=level,
                    entry=entry,
                    stop=stop,
                    target=target,
                    rr=rr,
                    status=status,
                    mark_i=i,
                    df=df,
                )
            )

    return signals


def detect_active_devils_marker(
    df: pd.DataFrame,
    *,
    ticker: str,
    timeframe: TfLabel,
    recent_candles: int = RECENT_CANDLES,
) -> dict | None:
    """Newest ACTIVE Pine-style Devil's Mark; prefer R:R ≥ 1.5."""
    markers = find_devils_markers(
        df,
        ticker=ticker,
        timeframe=timeframe,
        recent_candles=recent_candles,
        include_invalidated=False,
    )
    active = [m for m in markers if m["status"] == "ACTIVE"]
    if not active:
        return None
    high_rr = [m for m in active if m.get("is_high_rr")]
    return (high_rr or active)[0]
