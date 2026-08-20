from __future__ import annotations

from typing import Literal

import pandas as pd

from strategy.candles import is_bearish, is_bullish, to_f
from strategy.config import (
    EMA_PERIOD,
    SL_BUFFER_PCT,
    STRUCTURE_LOOKBACK,
    TP_RR_RATIO,
)

Direction = Literal["BULLISH", "BEARISH"]
TimeframeLabel = Literal["15M", "1H", "4H"]

# Min bars: structure lookback ending at i-4, plus C0..C3 => lookback + 4
MIN_PATTERN_BARS = STRUCTURE_LOOKBACK + 4


def _mid_wick_inf(row) -> float:
    low = to_f(row["Low"])
    body_low = min(to_f(row["Open"]), to_f(row["Close"]))
    wick_inf = body_low - low
    return low + wick_inf * 0.5


def _mid_wick_sup(row) -> float:
    high = to_f(row["High"])
    body_high = max(to_f(row["Open"]), to_f(row["Close"]))
    wick_sup = high - body_high
    return high - wick_sup * 0.5


def _ema_close(df: pd.DataFrame, period: int = EMA_PERIOD) -> pd.Series:
    """EMA on Close, aligned with pandas ewm(span=period, adjust=False)."""
    return df["Close"].astype(float).ewm(span=period, adjust=False).mean()


def _structure_slice_end(i: int) -> tuple[int, int]:
    """Window ending at i-4 (bar before C0): [i-23, i-4] inclusive for lookback=20."""
    end = i - 4
    start = end - STRUCTURE_LOOKBACK + 1
    return start, end


def _structure_low(df: pd.DataFrame, i: int) -> float:
    start, end = _structure_slice_end(i)
    return float(df["Low"].iloc[start : end + 1].astype(float).min())


def _structure_high(df: pd.DataFrame, i: int) -> float:
    start, end = _structure_slice_end(i)
    return float(df["High"].iloc[start : end + 1].astype(float).max())


def _volume_ok(c1, c3) -> bool:
    return to_f(c3["Volume"]) > to_f(c1["Volume"])


def _ema_ok_bullish(df: pd.DataFrame, i: int, ema: pd.Series) -> bool:
    return to_f(df.iloc[i]["Close"]) > float(ema.iloc[i])


def _ema_ok_bearish(df: pd.DataFrame, i: int, ema: pd.Series) -> bool:
    return to_f(df.iloc[i]["Close"]) < float(ema.iloc[i])


def _sweep_ok_bullish(df: pd.DataFrame, i: int) -> bool:
    c0 = df.iloc[i - 3]
    c1 = df.iloc[i - 2]
    swept = min(to_f(c0["Low"]), to_f(c1["Low"]))
    return swept < _structure_low(df, i)


def _sweep_ok_bearish(df: pd.DataFrame, i: int) -> bool:
    c0 = df.iloc[i - 3]
    c1 = df.iloc[i - 2]
    swept = max(to_f(c0["High"]), to_f(c1["High"]))
    return swept > _structure_high(df, i)


def _wick_ok_bullish(c1, c2, c3) -> bool:
    return to_f(c2["Low"]) >= _mid_wick_inf(c1) and to_f(c3["Low"]) >= _mid_wick_inf(c2)


def _wick_ok_bearish(c1, c2, c3) -> bool:
    return to_f(c2["High"]) <= _mid_wick_sup(c1) and to_f(c3["High"]) <= _mid_wick_sup(c2)


def _is_valid_bullish_block(df: pd.DataFrame, i: int, ema: pd.Series) -> bool:
    """Full SMC bullish validation at C3 index i."""
    c0 = df.iloc[i - 3]
    c1 = df.iloc[i - 2]
    c2 = df.iloc[i - 1]
    c3 = df.iloc[i]

    if not is_bearish(c0):
        return False
    if not (is_bullish(c1) and is_bullish(c2) and is_bullish(c3)):
        return False
    if not _wick_ok_bullish(c1, c2, c3):
        return False
    if not _ema_ok_bullish(df, i, ema):
        return False
    if not _volume_ok(c1, c3):
        return False
    if not _sweep_ok_bullish(df, i):
        return False
    return True


def _is_valid_bearish_block(df: pd.DataFrame, i: int, ema: pd.Series) -> bool:
    """Full SMC bearish validation at C3 index i."""
    c0 = df.iloc[i - 3]
    c1 = df.iloc[i - 2]
    c2 = df.iloc[i - 1]
    c3 = df.iloc[i]

    if not is_bullish(c0):
        return False
    if not (is_bearish(c1) and is_bearish(c2) and is_bearish(c3)):
        return False
    if not _wick_ok_bearish(c1, c2, c3):
        return False
    if not _ema_ok_bearish(df, i, ema):
        return False
    if not _volume_ok(c1, c3):
        return False
    if not _sweep_ok_bearish(df, i):
        return False
    return True


def compute_pattern_signals(df: pd.DataFrame) -> pd.Series:
    """smc_signal: 1 bullish block, -1 bearish block, 0 otherwise (only C1–C3 tagged)."""
    signals = pd.Series(0, index=df.index, dtype=int)
    if df is None or len(df) < MIN_PATTERN_BARS:
        return signals

    ema = _ema_close(df)
    for i in range(MIN_PATTERN_BARS - 1, len(df)):
        if _is_valid_bullish_block(df, i, ema):
            signals.iloc[i - 2] = 1
            signals.iloc[i - 1] = 1
            signals.iloc[i] = 1
        elif _is_valid_bearish_block(df, i, ema):
            signals.iloc[i - 2] = -1
            signals.iloc[i - 1] = -1
            signals.iloc[i] = -1

    return signals


def _to_unix(ts) -> int:
    if hasattr(ts, "timestamp"):
        return int(ts.timestamp())
    return int(pd.Timestamp(ts).timestamp())


def _build_pattern_result(
    df: pd.DataFrame,
    i: int,
    direction: Direction,
    timeframe: TimeframeLabel,
    ema: pd.Series,
) -> dict:
    c0 = df.iloc[i - 3]
    c1 = df.iloc[i - 2]
    c2 = df.iloc[i - 1]
    c3 = df.iloc[i]

    entry = to_f(c3["Close"])
    buffer = entry * SL_BUFFER_PCT
    ema_val = float(ema.iloc[i])

    if direction == "BULLISH":
        sl = min(to_f(c1["Low"]), to_f(c2["Low"]), to_f(c3["Low"])) - buffer
        risk = entry - sl
        tp = entry + risk * TP_RR_RATIO if risk > 0 else entry
        swept_level = min(to_f(c0["Low"]), to_f(c1["Low"]))
        structure_level = _structure_low(df, i)
    else:
        sl = max(to_f(c1["High"]), to_f(c2["High"]), to_f(c3["High"])) + buffer
        risk = sl - entry
        tp = entry - risk * TP_RR_RATIO if risk > 0 else entry
        swept_level = max(to_f(c0["High"]), to_f(c1["High"]))
        structure_level = _structure_high(df, i)

    pattern_candles = [
        {"time": _to_unix(df.index[i - 2]), "index": "C1"},
        {"time": _to_unix(df.index[i - 1]), "index": "C2"},
        {"time": _to_unix(df.index[i]), "index": "C3"},
    ]

    ts = df.index[i]
    timestamp = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

    return {
        "direction": direction,
        "timeframe": timeframe,
        "pattern_candles": pattern_candles,
        "entry_price": round(entry, 4),
        "stop_loss": round(sl, 4),
        "take_profit": round(tp, 4),
        "timestamp": timestamp,
        "ema_20": round(ema_val, 4),
        "swept_level": round(swept_level, 4),
        "structure_level": round(structure_level, 4),
    }


def detect_latest_pattern(
    df: pd.DataFrame | None,
    timeframe: TimeframeLabel,
) -> dict | None:
    """Detect SMC pattern only if C3 is the last closed bar."""
    if df is None or len(df) < MIN_PATTERN_BARS:
        return None
    if "Volume" not in df.columns:
        return None

    i = len(df) - 1
    ema = _ema_close(df)

    if _is_valid_bullish_block(df, i, ema):
        return _build_pattern_result(df, i, "BULLISH", timeframe, ema)
    if _is_valid_bearish_block(df, i, ema):
        return _build_pattern_result(df, i, "BEARISH", timeframe, ema)
    return None
