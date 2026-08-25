"""
Liquidity Sweep Model (Flipping Markets) — deterministic SMC setup.

Sequence:
  1. Liquidity Sweep of prior swing low/high (Protected Low/High)
  2. Market Structure Shift (MSS) breaking opposing swing
  3. FVG in impulse + Order Block
  4. Fib 0.71 OTE entry inside OB
  5. TP at BSL / SSL; SL beyond protected extreme
"""
from __future__ import annotations

from typing import Literal

import pandas as pd

from strategy.config import (
    FIB_OTE_RATIO,
    MIN_BARS_5M,
    MIN_BARS_15M,
    MIN_RR,
    OB_ENTRY_TOLERANCE_PCT,
    PIVOT_WINDOW,
    SETUP_LOOKBACK,
    SL_BUFFER_PCT,
)

Direction = Literal["BULLISH", "BEARISH"]
TimeframeLabel = Literal["5M", "15M"]


def to_f(v) -> float:
    return float(v)


def _to_unix(ts) -> int:
    if hasattr(ts, "timestamp"):
        return int(ts.timestamp())
    return int(pd.Timestamp(ts).timestamp())


def _pivot_lows(df: pd.DataFrame, window: int = PIVOT_WINDOW) -> list[tuple[int, float]]:
    """Return (index, price) for confirmed swing lows."""
    lows = df["Low"].values
    n = len(df)
    out: list[tuple[int, float]] = []
    for i in range(window, n - window):
        center = lows[i]
        if all(center <= lows[i - w] for w in range(1, window + 1)) and all(
            center < lows[i + w] for w in range(1, window + 1)
        ):
            out.append((i, float(center)))
    return out


def _pivot_highs(df: pd.DataFrame, window: int = PIVOT_WINDOW) -> list[tuple[int, float]]:
    highs = df["High"].values
    n = len(df)
    out: list[tuple[int, float]] = []
    for i in range(window, n - window):
        center = highs[i]
        if all(center >= highs[i - w] for w in range(1, window + 1)) and all(
            center > highs[i + w] for w in range(1, window + 1)
        ):
            out.append((i, float(center)))
    return out


def _structure_bias(df: pd.DataFrame | None) -> Direction | None:
    """
    Macro bias from recent swing structure.
    Long: last close above midpoint of last swing high/low and last swing low >= prior.
    Short: mirror.
    """
    if df is None or len(df) < 20:
        return None
    lows = _pivot_lows(df)
    highs = _pivot_highs(df)
    if len(lows) < 2 or len(highs) < 2:
        return None
    last_close = to_f(df["Close"].iloc[-1])
    last_low_i, last_low = lows[-1]
    prev_low = lows[-2][1]
    last_high_i, last_high = highs[-1]
    prev_high = highs[-2][1]
    mid = (last_low + last_high) / 2.0

    bullish = last_low >= prev_low * 0.998 and last_close >= mid
    bearish = last_high <= prev_high * 1.002 and last_close <= mid
    if bullish and not bearish:
        return "BULLISH"
    if bearish and not bullish:
        return "BEARISH"
    # Prefer most recent swing event
    if last_low_i > last_high_i and last_close > last_low:
        return "BULLISH"
    if last_high_i > last_low_i and last_close < last_high:
        return "BEARISH"
    return None


def combined_bias(
    df_4h: pd.DataFrame | None,
    df_1h: pd.DataFrame | None,
) -> Direction | None:
    """Require 1H bias; 4H when available must not contradict."""
    b1 = _structure_bias(df_1h)
    if b1 is None:
        return None
    b4 = _structure_bias(df_4h)
    if b4 is not None and b4 != b1:
        return None
    return b1


def _find_bullish_fvg(
    df: pd.DataFrame, start: int, end: int
) -> tuple[int, float, float] | None:
    """Bullish FVG: low[i] > high[i-2]. Returns (i, top, bottom) of most recent unfilled."""
    best = None
    for i in range(max(start + 2, 2), end + 1):
        hi_left = to_f(df["High"].iloc[i - 2])
        lo_right = to_f(df["Low"].iloc[i])
        if lo_right > hi_left:
            top = lo_right
            bottom = hi_left
            # Unfilled if subsequent lows stay above bottom
            filled = False
            for j in range(i + 1, end + 1):
                if to_f(df["Low"].iloc[j]) <= bottom:
                    filled = True
                    break
            if not filled:
                best = (i, top, bottom)
    return best


def _find_bearish_fvg(
    df: pd.DataFrame, start: int, end: int
) -> tuple[int, float, float] | None:
    best = None
    for i in range(max(start + 2, 2), end + 1):
        lo_left = to_f(df["Low"].iloc[i - 2])
        hi_right = to_f(df["High"].iloc[i])
        if hi_right < lo_left:
            top = lo_left
            bottom = hi_right
            filled = False
            for j in range(i + 1, end + 1):
                if to_f(df["High"].iloc[j]) >= top:
                    filled = True
                    break
            if not filled:
                best = (i, top, bottom)
    return best


def _find_bullish_ob(df: pd.DataFrame, sweep_i: int, mss_i: int) -> tuple[int, float, float] | None:
    """Last bearish candle before the MSS impulse leg."""
    for i in range(mss_i - 1, sweep_i - 1, -1):
        if i < 0:
            break
        o, c = to_f(df["Open"].iloc[i]), to_f(df["Close"].iloc[i])
        if c < o:  # bearish
            return i, to_f(df["High"].iloc[i]), to_f(df["Low"].iloc[i])
    return None


def _find_bearish_ob(df: pd.DataFrame, sweep_i: int, mss_i: int) -> tuple[int, float, float] | None:
    for i in range(mss_i - 1, sweep_i - 1, -1):
        if i < 0:
            break
        o, c = to_f(df["Open"].iloc[i]), to_f(df["Close"].iloc[i])
        if c > o:
            return i, to_f(df["High"].iloc[i]), to_f(df["Low"].iloc[i])
    return None


def _entry_in_ob(entry: float, ob_high: float, ob_low: float) -> bool:
    tol = entry * OB_ENTRY_TOLERANCE_PCT
    return (ob_low - tol) <= entry <= (ob_high + tol)


def _build_long(
    df: pd.DataFrame,
    timeframe: TimeframeLabel,
    swing_lows: list[tuple[int, float]],
    swing_highs: list[tuple[int, float]],
) -> dict | None:
    n = len(df)
    if n < MIN_BARS_15M or len(swing_lows) < 1 or len(swing_highs) < 2:
        return None

    last_i = n - 1
    lookback_start = max(PIVOT_WINDOW + 1, n - SETUP_LOOKBACK)

    # Find most recent sweep of a prior swing low inside lookback
    for sweep_i in range(last_i, lookback_start - 1, -1):
        sweep_low = to_f(df["Low"].iloc[sweep_i])
        sweep_close = to_f(df["Close"].iloc[sweep_i])
        # Prior confirmed swing low strictly before sweep bar
        prior_sl = [p for p in swing_lows if p[0] < sweep_i - PIVOT_WINDOW]
        if not prior_sl:
            continue
        sl_i, sl_price = prior_sl[-1]
        if not (sweep_low < sl_price and sweep_close > sl_price):
            continue

        protected_low = sweep_low
        # Swing high that formed before sweep (structure that MSS must break)
        prior_sh = [p for p in swing_highs if sl_i < p[0] < sweep_i]
        if not prior_sh:
            prior_sh = [p for p in swing_highs if p[0] < sweep_i]
        if not prior_sh:
            continue
        mss_level_i, mss_level = prior_sh[-1]

        # MSS: a later bar's high breaks that swing high
        mss_i = None
        mss_extreme = None
        for j in range(sweep_i + 1, last_i + 1):
            h = to_f(df["High"].iloc[j])
            if h > mss_level:
                mss_i = j
                mss_extreme = h
                # Continue to capture the peak of the MSS leg within a few bars
                for k in range(j + 1, min(j + 6, last_i + 1)):
                    hk = to_f(df["High"].iloc[k])
                    if hk >= mss_extreme:
                        mss_extreme = hk
                        mss_i = k
                    else:
                        break
                break
        if mss_i is None or mss_extreme is None:
            continue

        fvg = _find_bullish_fvg(df, sweep_i, mss_i)
        if fvg is None:
            continue
        _, fvg_top, fvg_bottom = fvg

        ob = _find_bullish_ob(df, sweep_i, mss_i)
        if ob is None:
            continue
        ob_i, ob_high, ob_low = ob

        price_range = mss_extreme - protected_low
        if price_range <= 0:
            continue
        fibo_71 = mss_extreme - price_range * FIB_OTE_RATIO
        if not _entry_in_ob(fibo_71, ob_high, ob_low):
            continue

        # BSL: swing high before the sweep (liquidity above)
        bsl_candidates = [p for p in swing_highs if p[0] < sweep_i]
        if len(bsl_candidates) < 1:
            continue
        # Prefer a high above MSS level if available, else the pre-sweep swing high
        bsl_above = [p for p in bsl_candidates if p[1] > mss_extreme]
        if bsl_above:
            _, take_profit = bsl_above[-1]
        else:
            # Use next higher historical swing or mss * extension
            higher = [p for p in bsl_candidates if p[1] >= mss_level]
            take_profit = higher[-1][1] if higher else mss_extreme * 1.01
            if take_profit <= fibo_71:
                take_profit = mss_extreme + price_range * 0.5

        stop_loss = protected_low * (1 - SL_BUFFER_PCT)
        entry = fibo_71
        risk = entry - stop_loss
        reward = take_profit - entry
        if risk <= 0 or reward / risk < MIN_RR:
            continue

        # Freshness / not invalidated
        last_close = to_f(df["Close"].iloc[-1])
        last_low = to_f(df["Low"].iloc[-1])
        if last_low <= stop_loss:
            continue
        if last_close >= take_profit:
            continue
        # Prefer setups where price has not already left OB far above without pullback opportunity
        # Allow if price is still between SL and TP
        if last_close <= stop_loss or last_close >= take_profit:
            continue

        ts = df.index[mss_i]
        timestamp = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

        pattern_levels = {
            "setup": "LIQUIDITY_SWEEP_LONG",
            "sweep_time": _to_unix(df.index[sweep_i]),
            "mss_time": _to_unix(df.index[mss_i]),
            "ob_time": _to_unix(df.index[ob_i]),
            "protected_level": round(protected_low, 4),
            "mss_extreme": round(mss_extreme, 4),
            "fibo_71": round(fibo_71, 4),
            "ob_high": round(ob_high, 4),
            "ob_low": round(ob_low, 4),
            "fvg_top": round(fvg_top, 4),
            "fvg_bottom": round(fvg_bottom, 4),
            "bsl_or_ssl": round(take_profit, 4),
            "swing_swept": round(sl_price, 4),
            "mss_level": round(mss_level, 4),
        }

        return {
            "direction": "BULLISH",
            "timeframe": timeframe,
            "entry_price": round(entry, 4),
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4),
            "protected_level": round(protected_low, 4),
            "mss_extreme": round(mss_extreme, 4),
            "fibo_71": round(fibo_71, 4),
            "ob_high": round(ob_high, 4),
            "ob_low": round(ob_low, 4),
            "fvg_top": round(fvg_top, 4),
            "fvg_bottom": round(fvg_bottom, 4),
            "bsl_or_ssl": round(take_profit, 4),
            "timestamp": timestamp,
            "pattern_levels": pattern_levels,
            "pattern_candles": [
                {"time": _to_unix(df.index[sweep_i]), "index": "SWEEP"},
                {"time": _to_unix(df.index[mss_i]), "index": "MSS"},
                {"time": _to_unix(df.index[ob_i]), "index": "OB"},
            ],
        }
    return None


def _build_short(
    df: pd.DataFrame,
    timeframe: TimeframeLabel,
    swing_lows: list[tuple[int, float]],
    swing_highs: list[tuple[int, float]],
) -> dict | None:
    n = len(df)
    if n < MIN_BARS_15M or len(swing_highs) < 1 or len(swing_lows) < 2:
        return None

    last_i = n - 1
    lookback_start = max(PIVOT_WINDOW + 1, n - SETUP_LOOKBACK)

    for sweep_i in range(last_i, lookback_start - 1, -1):
        sweep_high = to_f(df["High"].iloc[sweep_i])
        sweep_close = to_f(df["Close"].iloc[sweep_i])
        prior_sh = [p for p in swing_highs if p[0] < sweep_i - PIVOT_WINDOW]
        if not prior_sh:
            continue
        sh_i, sh_price = prior_sh[-1]
        if not (sweep_high > sh_price and sweep_close < sh_price):
            continue

        protected_high = sweep_high
        prior_sl = [p for p in swing_lows if sh_i < p[0] < sweep_i]
        if not prior_sl:
            prior_sl = [p for p in swing_lows if p[0] < sweep_i]
        if not prior_sl:
            continue
        mss_level_i, mss_level = prior_sl[-1]

        mss_i = None
        mss_extreme = None
        for j in range(sweep_i + 1, last_i + 1):
            lo = to_f(df["Low"].iloc[j])
            if lo < mss_level:
                mss_i = j
                mss_extreme = lo
                for k in range(j + 1, min(j + 6, last_i + 1)):
                    lk = to_f(df["Low"].iloc[k])
                    if lk <= mss_extreme:
                        mss_extreme = lk
                        mss_i = k
                    else:
                        break
                break
        if mss_i is None or mss_extreme is None:
            continue

        fvg = _find_bearish_fvg(df, sweep_i, mss_i)
        if fvg is None:
            continue
        _, fvg_top, fvg_bottom = fvg

        ob = _find_bearish_ob(df, sweep_i, mss_i)
        if ob is None:
            continue
        ob_i, ob_high, ob_low = ob

        price_range = protected_high - mss_extreme
        if price_range <= 0:
            continue
        fibo_71 = mss_extreme + price_range * FIB_OTE_RATIO
        if not _entry_in_ob(fibo_71, ob_high, ob_low):
            continue

        ssl_candidates = [p for p in swing_lows if p[0] < sweep_i]
        if not ssl_candidates:
            continue
        ssl_below = [p for p in ssl_candidates if p[1] < mss_extreme]
        if ssl_below:
            _, take_profit = ssl_below[-1]
        else:
            lower = [p for p in ssl_candidates if p[1] <= mss_level]
            take_profit = lower[-1][1] if lower else mss_extreme * 0.99
            if take_profit >= fibo_71:
                take_profit = mss_extreme - price_range * 0.5

        stop_loss = protected_high * (1 + SL_BUFFER_PCT)
        entry = fibo_71
        risk = stop_loss - entry
        reward = entry - take_profit
        if risk <= 0 or reward / risk < MIN_RR:
            continue

        last_close = to_f(df["Close"].iloc[-1])
        last_high = to_f(df["High"].iloc[-1])
        if last_high >= stop_loss:
            continue
        if last_close <= take_profit:
            continue

        ts = df.index[mss_i]
        timestamp = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

        pattern_levels = {
            "setup": "LIQUIDITY_SWEEP_SHORT",
            "sweep_time": _to_unix(df.index[sweep_i]),
            "mss_time": _to_unix(df.index[mss_i]),
            "ob_time": _to_unix(df.index[ob_i]),
            "protected_level": round(protected_high, 4),
            "mss_extreme": round(mss_extreme, 4),
            "fibo_71": round(fibo_71, 4),
            "ob_high": round(ob_high, 4),
            "ob_low": round(ob_low, 4),
            "fvg_top": round(fvg_top, 4),
            "fvg_bottom": round(fvg_bottom, 4),
            "bsl_or_ssl": round(take_profit, 4),
            "swing_swept": round(sh_price, 4),
            "mss_level": round(mss_level, 4),
        }

        return {
            "direction": "BEARISH",
            "timeframe": timeframe,
            "entry_price": round(entry, 4),
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4),
            "protected_level": round(protected_high, 4),
            "mss_extreme": round(mss_extreme, 4),
            "fibo_71": round(fibo_71, 4),
            "ob_high": round(ob_high, 4),
            "ob_low": round(ob_low, 4),
            "fvg_top": round(fvg_top, 4),
            "fvg_bottom": round(fvg_bottom, 4),
            "bsl_or_ssl": round(take_profit, 4),
            "timestamp": timestamp,
            "pattern_levels": pattern_levels,
            "pattern_candles": [
                {"time": _to_unix(df.index[sweep_i]), "index": "SWEEP"},
                {"time": _to_unix(df.index[mss_i]), "index": "MSS"},
                {"time": _to_unix(df.index[ob_i]), "index": "OB"},
            ],
        }
    return None


def detect_latest_pattern(
    df: pd.DataFrame | None,
    timeframe: TimeframeLabel,
    bias: Direction | None = None,
    df_4h: pd.DataFrame | None = None,
    df_1h: pd.DataFrame | None = None,
) -> dict | None:
    """Detect Liquidity Sweep setup on LTF if macro bias aligns."""
    min_bars = MIN_BARS_5M if timeframe == "5M" else MIN_BARS_15M
    if df is None or len(df) < min_bars:
        return None

    if bias is None:
        bias = combined_bias(df_4h, df_1h)
    if bias is None:
        return None

    swing_lows = _pivot_lows(df)
    swing_highs = _pivot_highs(df)

    if bias == "BULLISH":
        return _build_long(df, timeframe, swing_lows, swing_highs)
    return _build_short(df, timeframe, swing_lows, swing_highs)
