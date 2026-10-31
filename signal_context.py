"""Context snapshot for every Devil's Marker signal (stored in devils_signals.meta["ctx"]).

ONE function, compute_signal_context(), is used by the live scanner, the history backfill and
backfill_context.py, so live and historical values are produced by the same code.

No lookahead: every series is cut at the moment the signal becomes known, i.e. the close of the
pattern candle ("signal time"). Only bars whose END is <= signal time are used, for the signal
timeframe, the higher timeframe, SPY, ^VIX and the stock's daily bars. Any field that cannot be
computed is None; compute_signal_context() never raises.
"""
from __future__ import annotations

import math
import threading
from typing import Any, Callable

import numpy as np
import pandas as pd

from market_data import NY_TZ, SESSION_CLOSE, SESSION_MID, SESSION_OPEN, _ny_dates, clean_df

CTX_VERSION = 1

# Next-higher timeframe used for htf_trend. The scanner uses 4H/1D/1W/1M; 1M has no higher frame.
HTF_OF = {"4H": "1D", "1D": "1W", "1W": "1M", "1M": None}
INTRADAY_TF = {"4H", "1h"}


# --------------------------------------------------------------------------- bar timing
def _to_utc(index) -> pd.DatetimeIndex:
    """Naive intraday timestamps in this repo are UTC."""
    idx = pd.DatetimeIndex(index)
    return idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")


def bar_end(index, tf: str) -> pd.DatetimeIndex:
    """UTC time at which each bar is closed (same conventions as market_data.drop_unclosed).
    4H/1h: index = bar start (naive UTC). 1D: session date. 1W: week-ending Friday. 1M: month end."""
    if tf in INTRADAY_TF:
        ny = _to_utc(index).tz_convert(NY_TZ)
        day = ny.normalize()
        if tf == "4H":  # session-aligned bars: 09:30-13:30, 13:30-16:00
            end = (day + SESSION_MID).where(ny < day + SESSION_MID, day + SESSION_CLOSE)
        else:
            end = ny + pd.Timedelta(hours=1)
            cap = day + SESSION_CLOSE
            end = end.where(end <= cap, cap)
        return end.tz_convert("UTC")
    d = _ny_dates(index)
    if tf == "1M":  # month end on a weekend -> last Friday
        d = d - pd.to_timedelta(np.maximum(np.asarray(d.weekday) - 4, 0), unit="D")
    return (d + SESSION_CLOSE).tz_convert("UTC")


def closed_at(df: pd.DataFrame | None, tf: str, t: pd.Timestamp) -> pd.DataFrame | None:
    """Rows of df already closed at UTC time t."""
    if df is None or getattr(df, "empty", True):
        return None
    out = df.loc[np.asarray(bar_end(df.index, tf) <= t)]
    return out if not out.empty else None


def pattern_position(df: pd.DataFrame, pattern_at) -> int | None:
    ts = pd.Timestamp(pattern_at)
    target = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    hits = np.flatnonzero(_to_utc(df.index) == target)
    return int(hits[-1]) if len(hits) else None


# --------------------------------------------------------------------------- indicators
def atr14(df: pd.DataFrame) -> float | None:
    """Simple 14-bar mean of True Range on the last bar (deterministic, no warm-up dependency)."""
    if df is None or len(df) < 15:
        return None
    h, l, c = df["High"].astype(float), df["Low"].astype(float), df["Close"].astype(float)
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    v = float(tr.iloc[-14:].mean())
    return v if v > 0 else None


def _num(x, nd: int = 4):
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else round(f, nd)


# --------------------------------------------------------------------------- market data cache
class MarketCache:
    """Per-run cache of SPY, ^VIX, sector and earnings dates (thread-safe).
    Loaders are injectable for tests."""

    def __init__(self, history: Callable | None = None, info: Callable | None = None,
                 earnings: Callable | None = None):
        self._history = history or _yf_history
        self._info = info or _yf_info
        self._earnings = earnings or _yf_earnings
        self._lock = threading.Lock()
        self._data: dict[tuple, Any] = {}

    def _get(self, key, fn):
        with self._lock:
            if key in self._data:
                return self._data[key]
        try:
            val = fn()
        except Exception:
            val = None
        with self._lock:
            self._data.setdefault(key, val)
            return self._data[key]

    def history(self, symbol: str, interval: str):
        period = "730d" if interval == "1h" else "max"
        return self._get(("hist", symbol, interval), lambda: self._history(symbol, period, interval))

    def sector(self, symbol: str):
        return self._get(("sector", symbol), lambda: (self._info(symbol) or {}).get("sector"))

    def earnings_dates(self, symbol: str):
        return self._get(("earn", symbol), lambda: self._earnings(symbol))


def _yf_history(symbol: str, period: str, interval: str):
    import yfinance as yf

    raw = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=True)
    df = clean_df(raw.dropna() if raw is not None else None)
    return df if df is not None and not df.empty else None


def _yf_info(symbol: str) -> dict:
    import yfinance as yf

    return yf.Ticker(symbol).info or {}


def _yf_earnings(symbol: str) -> pd.DatetimeIndex | None:
    import yfinance as yf

    df = yf.Ticker(symbol).get_earnings_dates(limit=100)
    if df is None or df.empty:
        return None
    return pd.DatetimeIndex(df.index).tz_convert(NY_TZ).normalize().unique().sort_values()


_DEFAULT: MarketCache | None = None


def default_market() -> MarketCache:
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = MarketCache()
    return _DEFAULT


# --------------------------------------------------------------------------- fields
def _safe(fn, *a):
    try:
        return fn(*a)
    except Exception:
        return None


def _htf_trend(htf_df, htf_tf, t):
    d = closed_at(htf_df, htf_tf, t)
    if d is None or len(d) < 55:
        return None
    ema = d["Close"].astype(float).ewm(span=50, adjust=False).mean()
    return {
        "above_ema50": bool(float(d["Close"].iloc[-1]) > float(ema.iloc[-1])),
        "ema50_slope_pct": _num((ema.iloc[-1] / ema.iloc[-6] - 1) * 100),
    }


def _rel_vol(hist):
    if "Volume" not in hist.columns or len(hist) < 21:
        return None
    prev = float(hist["Volume"].iloc[-21:-1].astype(float).mean())
    return _num(float(hist["Volume"].iloc[-1]) / prev) if prev > 0 else None


def _gap_pct(tf, hist, daily, t):
    if tf in INTRADAY_TF:
        # Open of the signal day's first session bar vs the last session close of the previous day,
        # both from the same intraday series (daily bars are dividend-adjusted over a longer
        # period and would shift the level).
        ny = _to_utc(hist.index).tz_convert(NY_TZ).normalize()
        day = ny[-1]
        today = hist.loc[np.asarray(ny == day)]
        prev = hist.loc[np.asarray(ny < day)]
        if today.empty or prev.empty:
            return None
        return _num((float(today["Open"].iloc[0]) / float(prev["Close"].iloc[-1]) - 1) * 100)
    # daily+: last closed daily bar (for 1D the signal day itself) vs its previous close
    d = closed_at(daily, "1D", t)
    if d is None or len(d) < 2:
        return None
    return _num((float(d["Open"].iloc[-1]) / float(d["Close"].iloc[-2]) - 1) * 100)


def _spy_regime(spy_d):
    if spy_d is None or len(spy_d) < 50:
        return None
    c = spy_d["Close"].astype(float)
    return bool(c.iloc[-1] > c.iloc[-50:].mean())


def _spy_day_ret(tf, market, t, spy_d):
    """Intraday: SPY return from the previous session's last 1h close to the last SPY 1h close at
    signal time (same NY day; both from the 1h series so dividend adjustment cancels). Daily+: return of the last closed SPY daily bar at signal time, i.e. the signal
    day itself for 1D (its close is known when the 1D signal is), the last session of the
    week/month for 1W/1M. SPY 1h history is limited to ~730 days, older intraday signals -> None."""
    if spy_d is None or len(spy_d) < 2:
        return None
    if tf not in INTRADAY_TF:
        c = spy_d["Close"].astype(float)
        return _num((c.iloc[-1] / c.iloc[-2] - 1) * 100)
    h = closed_at(market.history("SPY", "1h"), "1h", t)
    if h is None:
        return None
    ny = _to_utc(h.index).tz_convert(NY_TZ)
    h = h.loc[np.asarray((ny - ny.normalize()) >= SESSION_OPEN)]  # regular session only
    days = _to_utc(h.index).tz_convert(NY_TZ).normalize()
    day = t.tz_convert(NY_TZ).normalize()
    today, prev = h.loc[np.asarray(days == day)], h.loc[np.asarray(days < day)]
    if today.empty or prev.empty:
        return None
    return _num((float(today["Close"].iloc[-1]) / float(prev["Close"].iloc[-1]) - 1) * 100)


def _earnings_within_5d(market, symbol, t):
    dates = market.earnings_dates(symbol)
    if dates is None or len(dates) == 0:
        return None
    day = t.tz_convert(NY_TZ).normalize()
    if dates.min() > day:  # Yahoo history does not reach back this far: unknown
        return None
    end = day + pd.offsets.BDay(5)
    return bool(((dates > day) & (dates <= end)).any())


def _bars_since_prev_marker(hist):
    """Bars between the pattern bar and the newest earlier bar that is a Devil's mark candle
    (Open==High or Open==Low, same tolerance as strategy.devils_markers), on the same
    symbol+timeframe. Only bars strictly before the pattern bar are scanned. None if none."""
    from strategy.devils_markers import _is_open_eq_high, _is_open_eq_low

    o, h, l = (hist[c].to_numpy(float) for c in ("Open", "High", "Low"))
    last = len(hist) - 1
    for k in range(last - 1, -1, -1):
        if _is_open_eq_high(o[k], h[k]) or _is_open_eq_low(o[k], l[k]):
            return last - k
    return None


def compute_signal_context(signal: dict, frames: dict, market: MarketCache | None = None) -> dict | None:
    """signal: devils signal dict or devils_signals row (needs timeframe, direction, entry_price,
    stop_loss, devils_marker_price, pattern_at and ticker/symbol).
    frames: {"4H","1D","1W","1M": DataFrame} as returned by devils_scanner.fetch_devils_frames.
    Returns the ctx dict or None if the pattern bar is not in the frame. Never raises."""
    try:
        return _compute(signal, frames, market or default_market())
    except Exception:
        return None


def _compute(signal, frames, market):
    tf = signal["timeframe"]
    df = frames.get(tf)
    if df is None or getattr(df, "empty", True):
        return None
    pos = pattern_position(df, signal["pattern_at"])
    if pos is None:
        return None
    hist = df.iloc[: pos + 1]  # pattern candle is closed; nothing after it
    t = bar_end(hist.index[-1:], tf)[0]  # signal time (UTC)
    symbol = (signal.get("ticker") or signal.get("symbol") or "").upper()
    entry = float(signal["entry_price"])
    stop = float(signal["stop_loss"])
    target = float(signal["target"]) if signal.get("target") is not None else None
    atr = _safe(atr14, hist)
    close = float(hist["Close"].iloc[-1])
    direction = str(signal.get("direction", "")).upper()
    spy_d = _safe(closed_at, market.history("SPY", "1d"), "1D", t)
    vix_d = _safe(closed_at, market.history("^VIX", "1d"), "1D", t)
    htf_tf = HTF_OF.get(tf)
    pat_ny = _to_utc(hist.index[-1:]).tz_convert(NY_TZ)[0] if tf in INTRADAY_TF else _ny_dates(hist.index[-1:])[0]
    return {
        "atr14_pct": _num(atr / close * 100) if atr else None,
        "stop_dist_pct": _num(abs(entry - stop) / entry * 100) if entry else None,
        "stop_atr": _num(abs(entry - stop) / atr) if atr else None,
        "htf_trend": _safe(_htf_trend, frames.get(htf_tf), htf_tf, t) if htf_tf else None,
        "rel_vol": _safe(_rel_vol, hist),
        "gap_pct": _safe(_gap_pct, tf, hist, frames.get("1D"), t),
        # minutes from 09:30 NY to the signal time (close of the pattern candle); None for 1D+
        "minutes_from_open": (
            int((t.tz_convert(NY_TZ) - (t.tz_convert(NY_TZ).normalize() + SESSION_OPEN)).total_seconds() // 60)
            if tf in INTRADAY_TF else None
        ),
        "weekday": int(pat_ny.weekday()),
        "spy_regime": _safe(_spy_regime, spy_d),
        "spy_day_ret": _safe(_spy_day_ret, tf, market, t, spy_d),
        "vix_level": _num(float(vix_d["Close"].iloc[-1]), 2) if vix_d is not None else None,
        "sector": _safe(market.sector, symbol),  # NOTE: today's sector from yfinance, not point-in-time
        "earnings_within_5d": _safe(_earnings_within_5d, market, symbol, t),
        "target_dist_atr": _num(abs(target - entry) / atr) if atr and target is not None else None,
        "planned_rr": _num(abs(target - entry) / abs(entry - stop)) if target is not None and entry != stop else None,
        "bars_since_prev_marker": _safe(_bars_since_prev_marker, hist),
        "signal_direction": {"BULLISH": "LONG", "BEARISH": "SHORT", "LONG": "LONG", "SHORT": "SHORT"}.get(direction),
    }


def keep_existing_context(new_meta: dict | None, old_meta: dict | None) -> dict | None:
    """If the stored row already has a ctx of the current version, keep that snapshot (computed
    when the signal was first seen) instead of the recomputed one."""
    old = old_meta or {}
    if new_meta is None or old.get("ctx_version") != CTX_VERSION or old.get("ctx") is None:
        return new_meta
    return {**new_meta, "ctx": old["ctx"], "ctx_version": CTX_VERSION}


def attach_context(meta: dict, ctx: dict | None) -> dict:
    out = dict(meta or {})
    out["ctx"] = ctx
    out["ctx_version"] = CTX_VERSION
    return out
