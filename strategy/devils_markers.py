"""Devil's Markers — Pure No-Wick on last 3 HTF candles (isolated from IC-CISD)."""
from __future__ import annotations

from typing import Literal

import pandas as pd

PIVOT_WINDOW = 3
NO_WICK_RATIO = 0.05
RECENT_CANDLES = 3  # analyze only bars -3, -2, -1 (0-based: n-3 .. n-1)
MIN_RR = 1.5

Direction = Literal["BULLISH", "BEARISH"]
Status = Literal["ACTIVE", "INVALIDATED", "CLOSED_WIN", "CLOSED_LOSS"]
TfLabel = Literal["4H", "7H", "1D", "1W", "1M"]


def _to_f(val) -> float:
    return float(val)


def _to_unix(ts: pd.Timestamp) -> int:
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


def _candle_range(high: float, low: float) -> float:
    return max(high - low, 1e-12)


def _is_no_wick_high(o: float, h: float, l: float, c: float) -> bool:
    return (h - max(o, c)) <= NO_WICK_RATIO * _candle_range(h, l)


def _is_no_wick_low(o: float, h: float, l: float, c: float) -> bool:
    return (min(o, c) - l) <= NO_WICK_RATIO * _candle_range(h, l)


def _is_local_swing_high(df: pd.DataFrame, i: int, window: int = 1) -> bool:
    """Soft swing: higher high than immediate neighbors (works on last bars)."""
    n = len(df)
    h = _to_f(df.iloc[i]["High"])
    left_ok = i == 0 or h >= _to_f(df.iloc[i - 1]["High"])
    right_ok = i == n - 1 or h > _to_f(df.iloc[i + 1]["High"])
    # Also beat left window if available
    if i >= window:
        left_ok = left_ok and h >= float(df["High"].iloc[i - window : i].max())
    return left_ok and right_ok


def _is_local_swing_low(df: pd.DataFrame, i: int, window: int = 1) -> bool:
    n = len(df)
    l = _to_f(df.iloc[i]["Low"])
    left_ok = i == 0 or l <= _to_f(df.iloc[i - 1]["Low"])
    right_ok = i == n - 1 or l < _to_f(df.iloc[i + 1]["Low"])
    if i >= window:
        left_ok = left_ok and l <= float(df["Low"].iloc[i - window : i].min())
    return left_ok and right_ok


def _find_prior_swing_high(df: pd.DataFrame, before_i: int, window: int = PIVOT_WINDOW) -> float | None:
    """Most recent confirmed pivot high before `before_i` (target for shorts)."""
    for i in range(before_i - 1, window - 1, -1):
        h = _to_f(df.iloc[i]["High"])
        left = df["High"].iloc[i - window : i]
        right = df["High"].iloc[i + 1 : min(i + window + 1, before_i)]
        if len(left) < window or len(right) < 1:
            continue
        if h >= float(left.max()) and h >= float(right.max()):
            return h
    # Fallback: highest high in prior lookback
    start = max(0, before_i - 40)
    if start >= before_i:
        return None
    return float(df["High"].iloc[start:before_i].max())


def _find_prior_swing_low(df: pd.DataFrame, before_i: int, window: int = PIVOT_WINDOW) -> float | None:
    """Most recent confirmed pivot low before `before_i` (target for longs)."""
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


def _entry_price(direction: Direction, o: float, c: float) -> float:
    """Entry at close of the No-Wick candle (continuation setup)."""
    return round(c, 4)


def _compute_rr(direction: Direction, entry: float, stop: float, target: float) -> float | None:
    if direction == "BULLISH":
        risk = entry - stop
        reward = target - entry
    else:
        risk = stop - entry
        reward = entry - target
    if risk <= 0 or reward <= 0:
        return None
    return round(reward / risk, 4)


def _status_after_marker(
    df: pd.DataFrame,
    swing_i: int,
    direction: Direction,
    stop: float,
    target: float,
) -> Status:
    """Invalidate only on break of No-Wick SL; otherwise stay ACTIVE until closed by resolver."""
    after = df.iloc[swing_i + 1 :]
    for _, row in after.iterrows():
        high = _to_f(row["High"])
        low = _to_f(row["Low"])
        if direction == "BEARISH":
            if high > stop:
                return "INVALIDATED"
            if low <= target:
                return "ACTIVE"  # TP reachable — keep active for resolver
        else:
            if low < stop:
                return "INVALIDATED"
            if high >= target:
                return "ACTIVE"
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
    swing_i: int,
    df: pd.DataFrame,
) -> dict:
    ts = df.index[swing_i]
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
        # Legacy fields kept nullable-compatible for older UI rows
        "cisd_level": entry,
        "is_cisd_broken": True,
        "cisd_label": "ENTRY",
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
    Pure No-Wick on the last `recent_candles` bars only (indices n-3..n-1).
    No CISD gate. SL = No-Wick extreme. TP = prior structural swing.
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

        # Bearish Devil: No-Wick swing high
        if _is_no_wick_high(o, h, l, c) and _is_local_swing_high(df, i):
            target = _find_prior_swing_low(df, i)
            if target is None or target >= h:
                continue
            entry = _entry_price("BEARISH", o, c)
            stop = h
            rr = _compute_rr("BEARISH", entry, stop, target)
            if rr is None:
                continue
            status = _status_after_marker(df, i, "BEARISH", stop, target)
            if status == "INVALIDATED" and not include_invalidated:
                continue
            signals.append(
                _build_signal(
                    ticker=ticker,
                    timeframe=timeframe,
                    direction="BEARISH",
                    devil=h,
                    entry=entry,
                    stop=stop,
                    target=target,
                    rr=rr,
                    status=status,
                    swing_i=i,
                    df=df,
                )
            )

        # Bullish Devil: No-Wick swing low
        if _is_no_wick_low(o, h, l, c) and _is_local_swing_low(df, i):
            target = _find_prior_swing_high(df, i)
            if target is None or target <= l:
                continue
            entry = _entry_price("BULLISH", o, c)
            stop = l
            rr = _compute_rr("BULLISH", entry, stop, target)
            if rr is None:
                continue
            status = _status_after_marker(df, i, "BULLISH", stop, target)
            if status == "INVALIDATED" and not include_invalidated:
                continue
            signals.append(
                _build_signal(
                    ticker=ticker,
                    timeframe=timeframe,
                    direction="BULLISH",
                    devil=l,
                    entry=entry,
                    stop=stop,
                    target=target,
                    rr=rr,
                    status=status,
                    swing_i=i,
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
    """Newest ACTIVE marker on last N candles; prefer high R:R."""
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
    pool = high_rr or active
    return pool[0]
