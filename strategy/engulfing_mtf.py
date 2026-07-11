from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

import pandas as pd

from strategy.candles import latest_engulfing, recent_engulfing
from strategy.config import (
    HTF_ENGULF_LOOKBACK,
    ITF_ENGULF_LOOKBACK,
    LTF_INTERVAL,
    MIN_BARS_4H,
    MIN_BARS_1H,
    MIN_BARS_5M,
)

FunnelStage = Literal[
    "no_data",
    "no_4h_engulfing",
    "no_1h_alignment",
    "no_ltf_trigger",
    "signal",
]


def _direction_label(direction: str) -> str:
    return "Bullish" if direction == "BULLISH" else "Bearish"


def _frames_ready(
    df_4h: pd.DataFrame | None,
    df_1h: pd.DataFrame | None,
    df_ltf: pd.DataFrame | None,
) -> bool:
    return (
        df_4h is not None
        and df_1h is not None
        and df_ltf is not None
        and len(df_4h) >= MIN_BARS_4H
        and len(df_1h) >= MIN_BARS_1H
        and len(df_ltf) >= MIN_BARS_5M
    )


def evaluate_funnel(
    df_4h: pd.DataFrame | None,
    df_1h: pd.DataFrame | None,
    df_ltf: pd.DataFrame | None,
) -> tuple[FunnelStage, str | None]:
    if not _frames_ready(df_4h, df_1h, df_ltf):
        return "no_data", None

    htf_dir, _ = latest_engulfing(df_4h, HTF_ENGULF_LOOKBACK)
    if htf_dir is None:
        return "no_4h_engulfing", None

    if not recent_engulfing(df_1h, htf_dir, ITF_ENGULF_LOOKBACK):
        return "no_1h_alignment", htf_dir

    if not recent_engulfing(df_ltf, htf_dir, lookback=1, last_bar_only=True):
        return "no_ltf_trigger", htf_dir

    return "signal", htf_dir


def evaluate_symbol(
    ticker: str,
    df_4h: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_ltf: pd.DataFrame,
) -> dict | None:
    stage, htf_dir = evaluate_funnel(df_4h, df_1h, df_ltf)
    if stage != "signal" or htf_dir is None:
        return None

    label = _direction_label(htf_dir)
    ts = df_ltf.index[-1]
    timestamp = ts.isoformat() if hasattr(ts, "isoformat") else datetime.now(timezone.utc).isoformat()

    return {
        "ticker": ticker,
        "signal_type": "LONG" if htf_dir == "BULLISH" else "SHORT",
        "4h_engulfing": f"Detected ({label})",
        "1h_engulfing": f"Aligned ({label})",
        "ltf_trigger": f"Triggered on {LTF_INTERVAL}",
        "timestamp": timestamp,
    }
