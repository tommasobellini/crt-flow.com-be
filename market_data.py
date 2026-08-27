from __future__ import annotations

import time

import pandas as pd
import yfinance as yf

from strategy.config import MIN_BARS_1H, MIN_BARS_4H, YF_PERIOD_1H

_OHLCV_NAMES = frozenset({"open", "high", "low", "close", "volume", "adj close"})
_YF_RETRIES = 3
_YF_BACKOFF_SEC = 1.5


def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.columns, pd.MultiIndex):
        return df
    levels = [df.columns.get_level_values(i) for i in range(df.columns.nlevels)]
    best_level = 0
    best_hits = -1
    for i, lvl in enumerate(levels):
        hits = sum(1 for x in lvl if str(x).strip().lower() in _OHLCV_NAMES)
        if hits > best_hits:
            best_hits = hits
            best_level = i
    df.columns = levels[best_level]
    return df


def clean_df(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    df = df.copy()
    df = _flatten_yf_columns(df)
    if any(isinstance(c, tuple) for c in df.columns):
        df.columns = [c[-1] if isinstance(c, tuple) else c for c in df.columns]

    new_cols = []
    for c in df.columns:
        c_str = str(c).strip().lower()
        if c_str == "open":
            new_cols.append("Open")
        elif c_str == "high":
            new_cols.append("High")
        elif c_str == "low":
            new_cols.append("Low")
        elif c_str == "close":
            new_cols.append("Close")
        elif c_str == "volume":
            new_cols.append("Volume")
        elif "adj" in c_str:
            new_cols.append("Adj Close")
        else:
            new_cols.append(str(c).strip())
    df.columns = new_cols
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def resample_to_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    df = df_1h.copy()
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(None)
    agg = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
    }
    if "Volume" in df.columns:
        agg["Volume"] = "sum"
    return df.resample("4h").agg(agg).dropna()


def _history_with_retry(
    stock: yf.Ticker, *, period: str, interval: str
) -> pd.DataFrame | None:
    last: pd.DataFrame | None = None
    for attempt in range(_YF_RETRIES):
        try:
            last = stock.history(period=period, interval=interval, auto_adjust=True)
            if last is not None and not last.empty:
                return last
        except Exception:
            last = None
        if attempt < _YF_RETRIES - 1:
            time.sleep(_YF_BACKOFF_SEC * (attempt + 1))
    return last


def fetch_htf_frames(
    ticker: str,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Return (4H, 1H) resampled from 1H history for Sweep & Engulf scanning."""
    try:
        stock = yf.Ticker(ticker)
        df_1h = _history_with_retry(stock, period=YF_PERIOD_1H, interval="1h")
    except Exception:
        return None, None

    df_1h = clean_df(df_1h.dropna() if df_1h is not None else None)
    if df_1h is None or df_1h.empty or len(df_1h) < MIN_BARS_1H:
        return None, None

    df_4h = resample_to_4h(df_1h)
    if df_4h.empty or len(df_4h) < MIN_BARS_4H:
        return None, None

    return df_4h, df_1h


def fetch_mtf_frames(
    ticker: str,
) -> tuple[
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
]:
    """Legacy shim — returns (4H, 1H, None, None)."""
    df_4h, df_1h = fetch_htf_frames(ticker)
    return df_4h, df_1h, None, None
