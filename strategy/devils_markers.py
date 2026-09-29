"""Devil's Markers — HTF No-Wick Continuation (isolated from IC-CISD scalping)."""
from __future__ import annotations

from typing import Literal

import pandas as pd

PIVOT_WINDOW = 3
NO_WICK_RATIO = 0.05
# Only evaluate Devil's Markers on the N most recent confirmable bars
# (a pivot needs `PIVOT_WINDOW` bars to the right, so the newest candidate is n-window-1).
RECENT_LOOKBACK = 3

Direction = Literal["BULLISH", "BEARISH"]
Status = Literal["MARKER_PENDING", "CONTINUATION_ACTIVE", "INVALIDATED"]
TfLabel = Literal["4H", "7H", "1D", "1W"]


def _to_f(val) -> float:
    return float(val)


def _to_unix(ts: pd.Timestamp) -> int:
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


def _candle_range(high: float, low: float) -> float:
    return max(high - low, 1e-12)


def _is_no_wick_swing_high(o: float, h: float, l: float, c: float) -> bool:
    total = _candle_range(h, l)
    upper_wick = h - max(o, c)
    return upper_wick <= NO_WICK_RATIO * total


def _is_no_wick_swing_low(o: float, h: float, l: float, c: float) -> bool:
    total = _candle_range(h, l)
    lower_wick = min(o, c) - l
    return lower_wick <= NO_WICK_RATIO * total


def _is_pivot_high(df: pd.DataFrame, i: int, window: int = PIVOT_WINDOW) -> bool:
    h = _to_f(df.iloc[i]["High"])
    left = df["High"].iloc[i - window : i]
    right = df["High"].iloc[i + 1 : i + window + 1]
    if len(left) < window or len(right) < window:
        return False
    return h >= float(left.max()) and h > float(right.max())


def _is_pivot_low(df: pd.DataFrame, i: int, window: int = PIVOT_WINDOW) -> bool:
    l = _to_f(df.iloc[i]["Low"])
    left = df["Low"].iloc[i - window : i]
    right = df["Low"].iloc[i + 1 : i + window + 1]
    if len(left) < window or len(right) < window:
        return False
    return l <= float(left.min()) and l < float(right.min())


def _bearish_cisd_from_swing(df: pd.DataFrame, swing_i: int) -> float | None:
    """Before bearish devil swing: bullish candle run; cisd = min(Open) of that run."""
    if swing_i < 1:
        return None
    seq_start = swing_i - 1
    while seq_start >= 0:
        row = df.iloc[seq_start]
        if _to_f(row["Close"]) <= _to_f(row["Open"]):
            break
        seq_start -= 1
    seq_start += 1
    if seq_start >= swing_i:
        return None
    return min(_to_f(df.iloc[j]["Open"]) for j in range(seq_start, swing_i))


def _bullish_cisd_from_swing(df: pd.DataFrame, swing_i: int) -> float | None:
    """Before bullish devil swing: bearish candle run; cisd = max(Open) of that run."""
    if swing_i < 1:
        return None
    seq_start = swing_i - 1
    while seq_start >= 0:
        row = df.iloc[seq_start]
        if _to_f(row["Close"]) >= _to_f(row["Open"]):
            break
        seq_start -= 1
    seq_start += 1
    if seq_start >= swing_i:
        return None
    return max(_to_f(df.iloc[j]["Open"]) for j in range(seq_start, swing_i))


def _measured_target(direction: Direction, devil: float, cisd: float) -> float:
    risk = abs(devil - cisd)
    if direction == "BEARISH":
        return round(cisd - risk, 4)
    return round(cisd + risk, 4)


def _evaluate_status(
    df: pd.DataFrame,
    swing_i: int,
    direction: Direction,
    devil: float,
    cisd: float,
    df_ltf: pd.DataFrame | None = None,
) -> tuple[bool, Status]:
    """Retest toward devil is OK; invalidate only on structure break beyond devil."""
    after = df.iloc[swing_i + 1 :]
    is_cisd_broken = False
    invalidated = False

    for _, row in after.iterrows():
        high = _to_f(row["High"])
        low = _to_f(row["Low"])
        close = _to_f(row["Close"])
        if direction == "BEARISH":
            if high > devil:
                invalidated = True
                break
            if close < cisd:
                is_cisd_broken = True
        else:
            if low < devil:
                invalidated = True
                break
            if close > cisd:
                is_cisd_broken = True

    if not is_cisd_broken and df_ltf is not None and not df_ltf.empty:
        swing_ts = pd.Timestamp(df.index[swing_i])
        if swing_ts.tzinfo is not None:
            swing_ts = swing_ts.tz_convert(None)
        ltf = df_ltf.copy()
        if getattr(ltf.index, "tz", None) is not None:
            ltf.index = ltf.index.tz_convert(None)
        ltf_after = ltf[ltf.index > swing_ts]
        for _, row in ltf_after.iterrows():
            close = _to_f(row["Close"])
            if direction == "BEARISH" and close < cisd:
                is_cisd_broken = True
                break
            if direction == "BULLISH" and close > cisd:
                is_cisd_broken = True
                break

    if invalidated:
        return is_cisd_broken, "INVALIDATED"
    if is_cisd_broken:
        return True, "CONTINUATION_ACTIVE"
    return False, "MARKER_PENDING"


def _build_signal(
    *,
    ticker: str,
    timeframe: TfLabel,
    direction: Direction,
    devil: float,
    cisd: float,
    status: Status,
    is_cisd_broken: bool,
    swing_i: int,
    df: pd.DataFrame,
) -> dict:
    target = _measured_target(direction, devil, cisd)
    ts = df.index[swing_i]
    return {
        "ticker": ticker.upper(),
        "timeframe": timeframe,
        "direction": direction,
        "devils_marker_price": round(devil, 4),
        "cisd_level": round(cisd, 4),
        "is_cisd_broken": is_cisd_broken,
        "status": status,
        "stop_loss": round(devil, 4),
        "target": target,
        "pattern_at": pd.Timestamp(ts).isoformat(),
        "pattern_unix": _to_unix(pd.Timestamp(ts)),
        "cisd_label": "-CISD" if direction == "BEARISH" else "+CISD",
    }


def find_devils_markers(
    df: pd.DataFrame,
    *,
    ticker: str,
    timeframe: TfLabel,
    window: int = PIVOT_WINDOW,
    recent_lookback: int = RECENT_LOOKBACK,
    df_ltf: pd.DataFrame | None = None,
    include_invalidated: bool = False,
) -> list[dict]:
    """Return Devil's Markers only on the last `recent_lookback` confirmable bars."""
    if df is None or df.empty or len(df) < window * 2 + 2:
        return []

    n = len(df)
    # Newest confirmable pivot index (needs `window` bars on the right).
    end_i = n - window - 1
    start_i = max(window, end_i - recent_lookback + 1)
    signals: list[dict] = []

    for i in range(end_i, start_i - 1, -1):
        row = df.iloc[i]
        o, h, l, c = (
            _to_f(row["Open"]),
            _to_f(row["High"]),
            _to_f(row["Low"]),
            _to_f(row["Close"]),
        )

        if _is_pivot_high(df, i, window) and _is_no_wick_swing_high(o, h, l, c):
            cisd = _bearish_cisd_from_swing(df, i)
            if cisd is None or cisd >= h:
                continue
            broken, status = _evaluate_status(df, i, "BEARISH", h, cisd, df_ltf)
            if status == "INVALIDATED" and not include_invalidated:
                continue
            signals.append(
                _build_signal(
                    ticker=ticker,
                    timeframe=timeframe,
                    direction="BEARISH",
                    devil=h,
                    cisd=cisd,
                    status=status,
                    is_cisd_broken=broken,
                    swing_i=i,
                    df=df,
                )
            )

        if _is_pivot_low(df, i, window) and _is_no_wick_swing_low(o, h, l, c):
            cisd = _bullish_cisd_from_swing(df, i)
            if cisd is None or cisd <= l:
                continue
            broken, status = _evaluate_status(df, i, "BULLISH", l, cisd, df_ltf)
            if status == "INVALIDATED" and not include_invalidated:
                continue
            signals.append(
                _build_signal(
                    ticker=ticker,
                    timeframe=timeframe,
                    direction="BULLISH",
                    devil=l,
                    cisd=cisd,
                    status=status,
                    is_cisd_broken=broken,
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
    window: int = PIVOT_WINDOW,
    recent_lookback: int = RECENT_LOOKBACK,
    df_ltf: pd.DataFrame | None = None,
) -> dict | None:
    """
    Prefer newest CONTINUATION_ACTIVE, else newest MARKER_PENDING.
    Only considers the last `recent_lookback` confirmable bars.
    """
    markers = find_devils_markers(
        df,
        ticker=ticker,
        timeframe=timeframe,
        window=window,
        recent_lookback=recent_lookback,
        df_ltf=df_ltf,
        include_invalidated=False,
    )
    if not markers:
        return None

    active = [m for m in markers if m["status"] == "CONTINUATION_ACTIVE"]
    if active:
        return active[0]
    pending = [m for m in markers if m["status"] == "MARKER_PENDING"]
    if pending:
        return pending[0]
    return None
