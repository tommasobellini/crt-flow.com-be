from __future__ import annotations

from typing import Literal

import pandas as pd

EngulfingDirection = Literal["BULLISH", "BEARISH"]


def to_f(val) -> float:
    if hasattr(val, "iloc"):
        if hasattr(val, "empty") and val.empty:
            return 0.0
        v = val.iloc[0]
        if hasattr(v, "iloc"):
            v = v.iloc[0]
        return float(v)
    return float(val)


def is_bullish(row) -> bool:
    return to_f(row["Close"]) > to_f(row["Open"])


def is_bearish(row) -> bool:
    return to_f(row["Close"]) < to_f(row["Open"])


def is_bullish_engulfing(prev, curr) -> bool:
    if not is_bearish(prev) or not is_bullish(curr):
        return False
    return (
        to_f(curr["Open"]) <= to_f(prev["Close"])
        and to_f(curr["Close"]) >= to_f(prev["Open"])
    )


def is_bearish_engulfing(prev, curr) -> bool:
    if not is_bullish(prev) or not is_bearish(curr):
        return False
    return (
        to_f(curr["Open"]) >= to_f(prev["Close"])
        and to_f(curr["Close"]) <= to_f(prev["Open"])
    )


def engulfing_at(df: pd.DataFrame, bar_index: int) -> EngulfingDirection | None:
    if bar_index < 1 or bar_index >= len(df):
        return None
    prev = df.iloc[bar_index - 1]
    curr = df.iloc[bar_index]
    if is_bullish_engulfing(prev, curr):
        return "BULLISH"
    if is_bearish_engulfing(prev, curr):
        return "BEARISH"
    return None


def latest_engulfing(
    df: pd.DataFrame, lookback: int
) -> tuple[EngulfingDirection | None, int | None]:
    if df is None or len(df) < 2:
        return None, None
    start = max(1, len(df) - lookback)
    for i in range(len(df) - 1, start - 1, -1):
        direction = engulfing_at(df, i)
        if direction is not None:
            return direction, i
    return None, None


def recent_engulfing(
    df: pd.DataFrame,
    direction: EngulfingDirection,
    lookback: int,
    *,
    last_bar_only: bool = False,
) -> bool:
    if df is None or len(df) < 2:
        return False
    if last_bar_only:
        return engulfing_at(df, len(df) - 1) == direction
    start = max(1, len(df) - lookback)
    for i in range(len(df) - 1, start - 1, -1):
        if engulfing_at(df, i) == direction:
            return True
    return False
