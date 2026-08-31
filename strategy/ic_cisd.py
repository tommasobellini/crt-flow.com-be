"""IC-CISD (Ideal Formation & Intracandle Change in State of Delivery) strategy."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from strategy.config import (
    CISD_TIMING_MAX_PCT,
    ENTRY_READY_MIN_RR,
    HTF_SWEEP_LOOKBACK,
    MIN_RR,
    SL_BUFFER_PCT,
    SL_BUFFER_TICKS,
    USE_CISD_LIMIT_ENTRY,
)
from strategy.liquidity_sweep import _find_bearish_fvg, _find_bullish_fvg

LtfLabel = Literal["15M", "5M"]
Bias = Literal["BULLISH", "BEARISH"]


@dataclass
class HtfContext:
    bias: Bias
    htf_context: str
    htf_timeframe: str
    sweep_bar_index: int
    df_htf: pd.DataFrame


def _to_unix(ts: pd.Timestamp) -> int:
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


def _to_f(val) -> float:
    return float(val)


def _htf_sweep_on_frame(
    df: pd.DataFrame, lookback: int, tf_label: str
) -> HtfContext | None:
    n = len(df)
    if n < lookback + 2:
        return None

    for i in range(n - 1, lookback, -1):
        window_low = df["Low"].iloc[i - lookback : i].min()
        window_high = df["High"].iloc[i - lookback : i].max()
        low = _to_f(df["Low"].iloc[i])
        high = _to_f(df["High"].iloc[i])
        close = _to_f(df["Close"].iloc[i])

        if low < window_low and close > window_low:
            ctx = f"{tf_label} Low Swept"
            start = max(0, i - lookback)
            fvg = _find_bullish_fvg(df, start, i)
            if fvg:
                ctx = f"{tf_label} Low Swept + Bull FVG"
            return HtfContext("BULLISH", ctx, tf_label, i, df)

        if high > window_high and close < window_high:
            ctx = f"{tf_label} High Swept"
            start = max(0, i - lookback)
            fvg = _find_bearish_fvg(df, start, i)
            if fvg:
                ctx = f"{tf_label} High Swept + Bear FVG"
            return HtfContext("BEARISH", ctx, tf_label, i, df)

    return None


def resolve_htf_bias(
    df_daily: pd.DataFrame,
    df_4h: pd.DataFrame | None,
) -> HtfContext | None:
    """Daily priority, then 4H."""
    daily_ctx = _htf_sweep_on_frame(df_daily, HTF_SWEEP_LOOKBACK, "Daily")
    if daily_ctx is not None:
        return daily_ctx
    if df_4h is not None and not df_4h.empty:
        return _htf_sweep_on_frame(df_4h, HTF_SWEEP_LOOKBACK, "4H")
    return None


def _norm_ts(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts


def _within_htf_timing(
    ltf_ts: pd.Timestamp,
    htf_ctx: HtfContext,
) -> bool:
    """IC-CISD must form in the first portion of the current HTF candle."""
    df = htf_ctx.df_htf
    idx = df.index
    ltf_ts = _norm_ts(ltf_ts)

    htf_i = len(df) - 1
    htf_start = _norm_ts(idx[htf_i])
    if htf_i + 1 < len(df):
        htf_end = _norm_ts(idx[htf_i + 1])
    else:
        if htf_ctx.htf_timeframe == "Daily":
            htf_end = htf_start + pd.Timedelta(days=1)
        else:
            htf_end = htf_start + pd.Timedelta(hours=4)

    if ltf_ts < htf_start or ltf_ts >= htf_end:
        return False

    duration = (htf_end - htf_start).total_seconds()
    if duration <= 0:
        return True
    elapsed = (ltf_ts - htf_start).total_seconds()
    return elapsed / duration <= CISD_TIMING_MAX_PCT


def _compute_tp(
    direction: Bias,
    df_daily: pd.DataFrame,
    df_ltf: pd.DataFrame,
    entry: float,
) -> float:
    prev_high = _to_f(df_daily["High"].iloc[-2]) if len(df_daily) >= 2 else _to_f(df_daily["High"].iloc[-1])
    prev_low = _to_f(df_daily["Low"].iloc[-2]) if len(df_daily) >= 2 else _to_f(df_daily["Low"].iloc[-1])

    session_high = _to_f(df_ltf["High"].tail(78).max())
    session_low = _to_f(df_ltf["Low"].tail(78).min())

    if direction == "BULLISH":
        candidates = [prev_high, session_high]
        candidates = [c for c in candidates if c > entry]
        if candidates:
            return min(candidates)
        risk = entry - _to_f(df_ltf["Low"].iloc[-1])
        return entry + risk * 2.0

    candidates = [prev_low, session_low]
    candidates = [c for c in candidates if c < entry]
    if candidates:
        return max(candidates)
    risk = _to_f(df_ltf["High"].iloc[-1]) - entry
    return entry - risk * 2.0


def _still_valid(
    df: pd.DataFrame,
    i: int,
    *,
    direction: Bias,
    sl: float,
    tp: float,
) -> bool:
    for j in range(i + 1, len(df)):
        bar = df.iloc[j]
        hi = _to_f(bar["High"])
        lo = _to_f(bar["Low"])
        if direction == "BULLISH":
            if lo <= sl or hi >= tp:
                return False
        else:
            if hi >= sl or lo <= tp:
                return False
    return True


def _find_bullish_cisd(
    df: pd.DataFrame,
    htf_ctx: HtfContext,
    df_daily: pd.DataFrame,
) -> dict | None:
    n = len(df)
    for i in range(n - 1, 4, -1):
        row = df.iloc[i]
        close = _to_f(row["Close"])
        if close <= _to_f(row["Open"]):
            continue

        seq_start = i - 1
        while seq_start >= 0 and _to_f(df.iloc[seq_start]["Close"]) < _to_f(df.iloc[seq_start]["Open"]):
            seq_start -= 1
        seq_start += 1

        if seq_start >= i:
            continue

        cisd_level = max(_to_f(df.iloc[j]["Open"]) for j in range(seq_start, i))
        if close <= cisd_level:
            continue

        protected = min(_to_f(df.iloc[j]["Low"]) for j in range(seq_start, i + 1))
        ltf_ts = pd.Timestamp(df.index[i])
        if not _within_htf_timing(ltf_ts, htf_ctx):
            continue

        entry = cisd_level if USE_CISD_LIMIT_ENTRY else close
        sl = protected - max(protected * SL_BUFFER_PCT, SL_BUFFER_TICKS)
        tp = _compute_tp("BULLISH", df_daily, df, entry)
        risk = entry - sl
        if risk <= 0:
            continue
        reward = tp - entry
        if reward <= 0:
            continue
        rr = reward / risk
        if rr < MIN_RR:
            continue

        if not _still_valid(df, i, direction="BULLISH", sl=sl, tp=tp):
            continue

        setup_status = "ENTRY_READY" if rr >= ENTRY_READY_MIN_RR else "LOW_RR"
        ts = df.index[i]
        return _build_signal(
            direction="BULLISH",
            timeframe="15M",
            entry=entry,
            sl=sl,
            tp=tp,
            rr=rr,
            cisd_level=cisd_level,
            protected=protected,
            htf_ctx=htf_ctx,
            ts=ts,
            trigger_i=i,
            seq_start=seq_start,
            setup_status=setup_status,
        )
    return None


def _find_bearish_cisd(
    df: pd.DataFrame,
    htf_ctx: HtfContext,
    df_daily: pd.DataFrame,
) -> dict | None:
    n = len(df)
    for i in range(n - 1, 4, -1):
        row = df.iloc[i]
        close = _to_f(row["Close"])
        if close >= _to_f(row["Open"]):
            continue

        seq_start = i - 1
        while seq_start >= 0 and _to_f(df.iloc[seq_start]["Close"]) > _to_f(df.iloc[seq_start]["Open"]):
            seq_start -= 1
        seq_start += 1

        if seq_start >= i:
            continue

        cisd_level = min(_to_f(df.iloc[j]["Open"]) for j in range(seq_start, i))
        if close >= cisd_level:
            continue

        protected = max(_to_f(df.iloc[j]["High"]) for j in range(seq_start, i + 1))
        ltf_ts = pd.Timestamp(df.index[i])
        if not _within_htf_timing(ltf_ts, htf_ctx):
            continue

        entry = cisd_level if USE_CISD_LIMIT_ENTRY else close
        sl = protected + max(protected * SL_BUFFER_PCT, SL_BUFFER_TICKS)
        tp = _compute_tp("BEARISH", df_daily, df, entry)
        risk = sl - entry
        if risk <= 0:
            continue
        reward = entry - tp
        if reward <= 0:
            continue
        rr = reward / risk
        if rr < MIN_RR:
            continue

        if not _still_valid(df, i, direction="BEARISH", sl=sl, tp=tp):
            continue

        setup_status = "ENTRY_READY" if rr >= ENTRY_READY_MIN_RR else "LOW_RR"
        ts = df.index[i]
        return _build_signal(
            direction="BEARISH",
            timeframe="15M",
            entry=entry,
            sl=sl,
            tp=tp,
            rr=rr,
            cisd_level=cisd_level,
            protected=protected,
            htf_ctx=htf_ctx,
            ts=ts,
            trigger_i=i,
            seq_start=seq_start,
            setup_status=setup_status,
        )
    return None


def _build_signal(
    *,
    direction: Bias,
    timeframe: LtfLabel,
    entry: float,
    sl: float,
    tp: float,
    rr: float,
    cisd_level: float,
    protected: float,
    htf_ctx: HtfContext,
    ts,
    trigger_i: int,
    seq_start: int,
    setup_status: str,
) -> dict:
    timestamp = pd.Timestamp(ts).isoformat()
    setup = "IC_CISD_LONG" if direction == "BULLISH" else "IC_CISD_SHORT"
    return {
        "direction": direction,
        "timeframe": timeframe,
        "entry_price": entry,
        "stop_loss": sl,
        "take_profit": tp,
        "timestamp": timestamp,
        "pattern_candles": [
            {"time": _to_unix(pd.Timestamp(ts)), "index": "CISD"},
            {"time": _to_unix(pd.Timestamp(ts)), "index": "PROTECTED"},
        ],
        "pattern_levels": {
            "setup": setup,
            "htf_context": htf_ctx.htf_context,
            "htf_timeframe": htf_ctx.htf_timeframe,
            "cisd_level": round(cisd_level, 4),
            "protected_level": round(protected, 4),
            "setup_status": setup_status,
            "rr": round(rr, 2),
            "seq_start_index": seq_start,
            "trigger_index": trigger_i,
        },
    }


def detect_latest_pattern(
    df: pd.DataFrame,
    timeframe: LtfLabel,
    *,
    htf_ctx: HtfContext,
    df_daily: pd.DataFrame,
) -> dict | None:
    """Return latest valid IC-CISD setup on LTF aligned with HTF bias."""
    if df is None or len(df) < 10:
        return None

    if htf_ctx.bias == "BULLISH":
        sig = _find_bullish_cisd(df, htf_ctx, df_daily)
    else:
        sig = _find_bearish_cisd(df, htf_ctx, df_daily)

    if sig is not None:
        sig["timeframe"] = timeframe
    return sig
