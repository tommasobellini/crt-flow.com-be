"""Validate radar candidates without changing the lifecycle of simulated open trades."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import isclose

import pandas as pd

from signal_context import bar_end, pattern_position
from strategy.devils_markers import PRICE_EQ_TOL, STRATEGY_VERSION, _is_open_eq_high, _is_open_eq_low

PRICE_BASIS = "split_adjusted_no_dividends"
# Reject stalled/delisted feeds, while allowing weekends and exchange holidays.
MAX_FEED_AGE_DAYS = {"4H": 7, "1D": 7, "1W": 14, "1M": 45}


def same_candle(saved: dict | None, current: dict | None) -> bool:
    if not saved or not current:
        return False
    return all(saved.get(k) is not None and current.get(k) is not None
               and isclose(float(saved[k]), float(current[k]), rel_tol=PRICE_EQ_TOL, abs_tol=1e-8)
               for k in ("open", "high", "low", "close"))


def source_validation(signal: dict, df) -> str:
    """Return why the saved source is unsafe, or VERIFIED, independently of radar age."""
    if df is None or df.empty:
        return "DATA_UNAVAILABLE"
    pos = pattern_position(df, signal.get("pattern_at"))
    if pos is None:
        return "SOURCE_BAR_MISSING"
    bar = df.iloc[pos]
    candle = {k.lower(): float(bar[k]) for k in ("Open", "High", "Low", "Close")}
    long = signal.get("direction") == "BULLISH"
    valid_mark = (_is_open_eq_low(candle["open"], candle["low"]) and candle["close"] > candle["open"]
                  if long else _is_open_eq_high(candle["open"], candle["high"]) and candle["close"] < candle["open"])
    if not valid_mark:
        return "PATTERN_NO_LONGER_VALID"
    meta = signal.get("meta") or {}
    if meta.get("strategy_version") != STRATEGY_VERSION or meta.get("price_basis") != PRICE_BASIS:
        return "UNVERIFIED_PRICE_BASIS"
    if not same_candle(meta.get("pattern_candle"), candle):
        return "SOURCE_CANDLE_REVISED"
    return "VERIFIED"


def radar_validation(signal: dict, df, recent_candles: int, *, now: datetime | None = None) -> dict:
    """A signal needs a matching OHLC snapshot, recent source bar and live observation."""
    now = now or datetime.now(timezone.utc)
    result = {"radar_eligible": False, "radar_checked_at": now.isoformat(),
              "radar_window_candles": recent_candles}
    reason = source_validation(signal, df)
    if reason != "VERIFIED":
        return {**result, "radar_reason": reason}
    tf = signal["timeframe"]
    latest_end = bar_end(pd.DatetimeIndex([df.index[-1]]), tf)[0].to_pydatetime()
    expiry = latest_end + timedelta(days=MAX_FEED_AGE_DAYS[tf])
    result["radar_expires_at"] = expiry.isoformat()
    if latest_end > now:
        return {**result, "radar_reason": "UNCLOSED_SOURCE_BAR"}
    if expiry <= now:
        return {**result, "radar_reason": "STALE_MARKET_DATA"}
    pos = pattern_position(df, signal["pattern_at"])
    if pos < len(df) - recent_candles:
        return {**result, "radar_reason": "OUTSIDE_RECENT_WINDOW"}
    meta = signal.get("meta") or {}
    if not meta.get("first_live_seen_at"):
        return {**result, "radar_reason": "HISTORICAL_ONLY"}
    return {**result, "radar_eligible": True, "radar_reason": "VERIFIED"}
