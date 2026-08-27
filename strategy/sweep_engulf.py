"""Sweep & Engulf (Balanced) — single-TF pattern from Pine Script."""
from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from strategy.config import (
    ADX_THRESHOLD,
    EMA_LENGTH,
    MIN_BODY_RATIO,
    RR_RATIO,
    SWING_LOOKBACK,
    USE_ADX_FILTER,
    USE_BODY_FILTER,
    USE_EMA_FILTER,
    USE_VOLUME_FILTER,
    VOL_SMA_LENGTH,
)

TimeframeLabel = Literal["4H", "1H"]


def _to_unix(ts: pd.Timestamp) -> int:
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    up = high - high.shift(1)
    down = low.shift(1) - low
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    atr = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(
        alpha=1 / period, min_periods=period, adjust=False
    ).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(
        alpha=1 / period, min_periods=period, adjust=False
    ).mean() / atr
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) * 100
    return dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def _compute_quality_score(
    *,
    vol_ratio: float,
    body_ratio: float,
    adx_val: float,
    ema_aligned: bool,
) -> float:
    score = 40.0
    if vol_ratio >= 1.5:
        score += 20
    elif vol_ratio >= 1.2:
        score += 12
    elif vol_ratio >= 1.0:
        score += 5
    if body_ratio >= 0.6:
        score += 15
    elif body_ratio >= 0.4:
        score += 8
    if adx_val >= 25:
        score += 15
    elif adx_val >= 20:
        score += 8
    if ema_aligned:
        score += 10
    return round(min(100.0, score), 1)


def _bar_features(df: pd.DataFrame, i: int) -> dict:
    row = df.iloc[i]
    vol = float(row.get("Volume", 0) or 0)
    vol_sma = float(df["Volume"].iloc[max(0, i - VOL_SMA_LENGTH + 1) : i + 1].mean())
    vol_ratio = vol / vol_sma if vol_sma > 0 else 1.0
    bar_range = float(row["High"] - row["Low"])
    body = abs(float(row["Close"] - row["Open"]))
    body_ratio = body / bar_range if bar_range > 0 else 0.0
    adx_val = float(df["_adx"].iloc[i]) if "_adx" in df.columns else 0.0
    ema_val = float(df["_ema"].iloc[i]) if "_ema" in df.columns else float(row["Close"])
    close = float(row["Close"])
    ema_aligned_bull = close > ema_val
    ema_aligned_bear = close < ema_val
    return {
        "vol_ratio": vol_ratio,
        "body_ratio": body_ratio,
        "adx": adx_val,
        "ema_dist_pct": abs(close - ema_val) / close if close > 0 else 0.0,
        "ema_aligned_bull": ema_aligned_bull,
        "ema_aligned_bear": ema_aligned_bear,
    }


def _filters_allow(
    features: dict,
    direction: Literal["BULLISH", "BEARISH"],
) -> bool:
    if USE_VOLUME_FILTER and features["vol_ratio"] <= 1.0:
        return False
    if USE_ADX_FILTER and features["adx"] < ADX_THRESHOLD:
        return False
    if USE_BODY_FILTER and features["body_ratio"] < MIN_BODY_RATIO:
        return False
    if USE_EMA_FILTER:
        if direction == "BULLISH" and not features["ema_aligned_bull"]:
            return False
        if direction == "BEARISH" and not features["ema_aligned_bear"]:
            return False
    return True


def _still_valid(
    df: pd.DataFrame,
    i: int,
    *,
    direction: Literal["BULLISH", "BEARISH"],
    sl: float,
    tp: float,
) -> bool:
    """Signal active if subsequent bars have not hit SL or TP."""
    for j in range(i + 1, len(df)):
        bar = df.iloc[j]
        hi = float(bar["High"])
        lo = float(bar["Low"])
        if direction == "BULLISH":
            if lo <= sl or hi >= tp:
                return False
        else:
            if hi >= sl or lo <= tp:
                return False
    return True


def _build_signal(
    df: pd.DataFrame,
    i: int,
    direction: Literal["BULLISH", "BEARISH"],
    timeframe: TimeframeLabel,
) -> dict | None:
    row = df.iloc[i]
    close = float(row["Close"])
    if direction == "BULLISH":
        sl = float(row["Low"])
        risk = close - sl
        if risk <= 0:
            return None
        tp = close + risk * RR_RATIO
    else:
        sl = float(row["High"])
        risk = sl - close
        if risk <= 0:
            return None
        tp = close - risk * RR_RATIO

    if not _still_valid(df, i, direction=direction, sl=sl, tp=tp):
        return None

    features = _bar_features(df, i)
    ema_aligned = (
        features["ema_aligned_bull"]
        if direction == "BULLISH"
        else features["ema_aligned_bear"]
    )
    quality = _compute_quality_score(
        vol_ratio=features["vol_ratio"],
        body_ratio=features["body_ratio"],
        adx_val=features["adx"],
        ema_aligned=ema_aligned,
    )
    ts = df.index[i]
    if hasattr(ts, "isoformat"):
        timestamp = ts.isoformat()
    else:
        timestamp = str(ts)
    prev = df.iloc[i - 1]
    sweep_extreme = float(row["Low"] if direction == "BULLISH" else row["High"])
    setup = (
        "SWEEP_ENGULF_LONG" if direction == "BULLISH" else "SWEEP_ENGULF_SHORT"
    )

    return {
        "direction": direction,
        "timeframe": timeframe,
        "entry_price": close,
        "stop_loss": sl,
        "take_profit": tp,
        "timestamp": timestamp,
        "pattern_candles": [{"time": _to_unix(pd.Timestamp(ts)), "index": "SWEEP"}],
        "pattern_levels": {
            "setup": setup,
            "lookback": SWING_LOOKBACK,
            "sweep_extreme": sweep_extreme,
            "prev_high": float(prev["High"]),
            "prev_low": float(prev["Low"]),
            "vol_ratio": round(features["vol_ratio"], 4),
            "body_ratio": round(features["body_ratio"], 4),
            "adx": round(features["adx"], 2),
            "ema_dist_pct": round(features["ema_dist_pct"], 6),
            "rr": RR_RATIO,
            "quality_score": quality,
        },
    }


def detect_latest_pattern(
    df: pd.DataFrame,
    timeframe: TimeframeLabel,
) -> dict | None:
    """Return the most recent active Sweep & Engulf setup on this TF."""
    if df is None or len(df) < SWING_LOOKBACK + 2:
        return None

    work = df.copy()
    work["_ema"] = _ema(work["Close"], EMA_LENGTH)
    work["_adx"] = _adx(work, 14)
    work["_vol_sma"] = work["Volume"].rolling(VOL_SMA_LENGTH, min_periods=1).mean()

    n = len(work)
    for i in range(n - 1, SWING_LOOKBACK, -1):
        row = work.iloc[i]
        prev = work.iloc[i - 1]
        window_low = work["Low"].iloc[i - SWING_LOOKBACK : i].min()
        window_high = work["High"].iloc[i - SWING_LOOKBACK : i].max()
        features = _bar_features(work, i)

        low = float(row["Low"])
        high = float(row["High"])
        close = float(row["Close"])
        prev_high = float(prev["High"])
        prev_low = float(prev["Low"])

        if low < window_low and close > prev_high:
            if _filters_allow(features, "BULLISH"):
                sig = _build_signal(work, i, "BULLISH", timeframe)
                if sig is not None:
                    return sig

        if high > window_high and close < prev_low:
            if _filters_allow(features, "BEARISH"):
                sig = _build_signal(work, i, "BEARISH", timeframe)
                if sig is not None:
                    return sig

    return None
