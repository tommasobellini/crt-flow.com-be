from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

import pandas as pd

from strategy.config import MIN_BARS_15M, MIN_BARS_1H, MIN_BARS_4H, RSI_PERIOD
from strategy.heikin_ashi import last_two_ha_green, to_heikin_ashi
from strategy.indicators import compute_rsi

FunnelStage = Literal[
    "no_data",
    "no_4h_structure",
    "no_1h_alignment",
    "signal",
]


def _frames_ready(
    df_4h: pd.DataFrame | None,
    df_1h: pd.DataFrame | None,
    df_15m: pd.DataFrame | None,
) -> bool:
    return (
        df_4h is not None
        and df_1h is not None
        and df_15m is not None
        and len(df_4h) >= MIN_BARS_4H
        and len(df_1h) >= MIN_BARS_1H
        and len(df_15m) >= MIN_BARS_15M
    )


def assess_timeframe(df: pd.DataFrame) -> tuple[bool, float | None]:
    """Return whether bullish HA+RSI rules pass and the latest RSI value."""
    min_bars = RSI_PERIOD + 2
    if df is None or len(df) < min_bars:
        return False, None

    ha = to_heikin_ashi(df)
    rsi_series = compute_rsi(ha["Close"], RSI_PERIOD)
    current_rsi = rsi_series.iloc[-1]
    rsi_val = round(float(current_rsi), 1) if not pd.isna(current_rsi) else None

    ha_green = last_two_ha_green(df)
    rsi_bullish = rsi_val is not None and rsi_val > 50

    return ha_green and rsi_bullish, rsi_val


def evaluate_funnel(
    df_4h: pd.DataFrame | None,
    df_1h: pd.DataFrame | None,
    df_15m: pd.DataFrame | None,
) -> tuple[FunnelStage, None]:
    if not _frames_ready(df_4h, df_1h, df_15m):
        return "no_data", None

    ok_4h, _ = assess_timeframe(df_4h)
    if not ok_4h:
        return "no_4h_structure", None

    ok_1h, _ = assess_timeframe(df_1h)
    if not ok_1h:
        return "no_1h_alignment", None

    return "signal", None


def evaluate_symbol(
    ticker: str,
    df_4h: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_15m: pd.DataFrame,
) -> dict | None:
    stage, _ = evaluate_funnel(df_4h, df_1h, df_15m)
    if stage != "signal":
        return None

    _, rsi_4h = assess_timeframe(df_4h)
    _, rsi_1h = assess_timeframe(df_1h)
    ok_15m, rsi_15m = assess_timeframe(df_15m)

    ts = df_15m.index[-1]
    timestamp = (
        ts.isoformat()
        if hasattr(ts, "isoformat")
        else datetime.now(timezone.utc).isoformat()
    )

    return {
        "ticker": ticker,
        "signal_type": "BULLISH",
        "is_golden": ok_15m,
        "4h_rsi": rsi_4h,
        "1h_rsi": rsi_1h,
        "15m_rsi": rsi_15m,
        "timestamp": timestamp,
    }
