from __future__ import annotations

import time

import pandas as pd
import yfinance as yf

from strategy.config import (
    MIN_BARS_15M,
    MIN_BARS_4H,
    MIN_BARS_5M,
    MIN_BARS_DAILY,
    YF_PERIOD_15M,
    YF_PERIOD_1H,
    YF_PERIOD_5M,
    YF_PERIOD_DAILY,
)

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


def _resample_ohlcv(df_src: pd.DataFrame, rule: str) -> pd.DataFrame:
    df = df_src.copy()
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
    return df.resample(rule).agg(agg).dropna()


def resample_to_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    return _resample_ohlcv(df_1h, "4h")


def resample_to_7h(df_1h: pd.DataFrame) -> pd.DataFrame:
    return _resample_ohlcv(df_1h, "7h")


def resample_to_weekly(df_daily: pd.DataFrame) -> pd.DataFrame:
    return _resample_ohlcv(df_daily, "W")


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


def _extract_ticker_ohlc(raw: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    """Slice a yf.download MultiIndex (or single-ticker) frame to OHLCV for one symbol."""
    if raw is None or raw.empty:
        return None
    df = raw
    if isinstance(df.columns, pd.MultiIndex):
        names = [str(n).lower() if n is not None else "" for n in (df.columns.names or [])]
        try:
            if "ticker" in names:
                df = df.xs(ticker, axis=1, level="ticker")
            elif len(df.columns.names) >= 2 and names[-1] == "ticker":
                df = df.xs(ticker, axis=1, level=-1)
            elif ticker in df.columns.get_level_values(0):
                df = df.xs(ticker, axis=1, level=0)
            elif ticker in df.columns.get_level_values(-1):
                df = df.xs(ticker, axis=1, level=-1)
            else:
                return None
        except (KeyError, ValueError):
            return None
    cleaned = clean_df(df.dropna() if df is not None else None)
    if cleaned is None or cleaned.empty:
        return None
    return cleaned


def _download_interval(tickers: list[str], *, period: str, interval: str) -> pd.DataFrame | None:
    last: pd.DataFrame | None = None
    for attempt in range(_YF_RETRIES):
        try:
            last = yf.download(
                tickers=tickers,
                period=period,
                interval=interval,
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
            if last is not None and not last.empty:
                return last
        except Exception:
            last = None
        if attempt < _YF_RETRIES - 1:
            time.sleep(_YF_BACKOFF_SEC * (attempt + 1))
    return last


def _frames_from_ohlc(
    df_daily: pd.DataFrame | None,
    df_1h: pd.DataFrame | None,
    df_15m: pd.DataFrame | None,
    df_5m: pd.DataFrame | None,
) -> tuple[
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
]:
    if df_daily is None or df_daily.empty or len(df_daily) < MIN_BARS_DAILY:
        return None, None, None, None

    df_4h = None
    if df_1h is not None and not df_1h.empty:
        df_4h = resample_to_4h(df_1h)
        if df_4h.empty or len(df_4h) < MIN_BARS_4H:
            df_4h = None

    if df_15m is not None and (df_15m.empty or len(df_15m) < MIN_BARS_15M):
        df_15m = None
    if df_5m is not None and (df_5m.empty or len(df_5m) < MIN_BARS_5M):
        df_5m = None

    if df_15m is None and df_5m is None:
        return None, None, None, None

    return df_daily, df_4h, df_15m, df_5m


def download_ic_cisd_universe(
    tickers: list[str],
) -> dict[str, tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]]:
    """Batch-download Daily/1H/15M/5M for the watchlist (4 Yahoo calls instead of 4×N)."""
    unique = [t.strip().upper() for t in tickers if t and str(t).strip()]
    unique = list(dict.fromkeys(unique))
    out: dict[
        str,
        tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None],
    ] = {t: (None, None, None, None) for t in unique}
    if not unique:
        return out

    raw_daily = _download_interval(unique, period=YF_PERIOD_DAILY, interval="1d")
    raw_1h = _download_interval(unique, period=YF_PERIOD_1H, interval="1h")
    raw_15m = _download_interval(unique, period=YF_PERIOD_15M, interval="15m")
    raw_5m = _download_interval(unique, period=YF_PERIOD_5M, interval="5m")

    for ticker in unique:
        daily = _extract_ticker_ohlc(raw_daily, ticker) if raw_daily is not None else None
        h1 = _extract_ticker_ohlc(raw_1h, ticker) if raw_1h is not None else None
        m15 = _extract_ticker_ohlc(raw_15m, ticker) if raw_15m is not None else None
        m5 = _extract_ticker_ohlc(raw_5m, ticker) if raw_5m is not None else None
        out[ticker] = _frames_from_ohlc(daily, h1, m15, m5)
    return out


def fetch_ic_cisd_frames(
    ticker: str,
) -> tuple[
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
]:
    """Return (Daily, 4H, 15M, 5M) for IC-CISD scanning (single ticker)."""
    try:
        stock = yf.Ticker(ticker)
        df_daily = _history_with_retry(stock, period=YF_PERIOD_DAILY, interval="1d")
        df_1h = _history_with_retry(stock, period=YF_PERIOD_1H, interval="1h")
        df_15m = _history_with_retry(stock, period=YF_PERIOD_15M, interval="15m")
        df_5m = _history_with_retry(stock, period=YF_PERIOD_5M, interval="5m")
    except Exception:
        return None, None, None, None

    df_daily = clean_df(df_daily.dropna() if df_daily is not None else None)
    df_1h = clean_df(df_1h.dropna() if df_1h is not None else None)
    df_15m = clean_df(df_15m.dropna() if df_15m is not None else None)
    df_5m = clean_df(df_5m.dropna() if df_5m is not None else None)

    return _frames_from_ohlc(df_daily, df_1h, df_15m, df_5m)


def fetch_htf_frames(
    ticker: str,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Legacy shim for sweep_engulf."""
    daily, h4, _, _ = fetch_ic_cisd_frames(ticker)
    if daily is None:
        return None, None
    try:
        stock = yf.Ticker(ticker)
        df_1h = _history_with_retry(stock, period=YF_PERIOD_1H, interval="1h")
        df_1h = clean_df(df_1h.dropna() if df_1h is not None else None)
        if df_1h is None or df_1h.empty:
            return h4, h4
        df_4h = resample_to_4h(df_1h)
        return df_4h, df_1h
    except Exception:
        return h4, h4


def fetch_mtf_frames(
    ticker: str,
) -> tuple[
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
]:
    daily, h4, m15, m5 = fetch_ic_cisd_frames(ticker)
    return h4, daily, m15, m5
