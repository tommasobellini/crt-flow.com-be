"""Unit tests for IC-CISD detection."""
from __future__ import annotations

import pandas as pd

from signal_adapter import signal_to_crt_row
from strategy.ic_cisd import detect_latest_pattern, resolve_htf_bias


def _make_df(rows: list[dict], freq: str = "15min", start: str = "2026-08-01 09:30") -> pd.DataFrame:
    idx = pd.date_range(start, periods=len(rows), freq=freq, tz="UTC")
    return pd.DataFrame(rows, index=idx)


def _daily_bullish_sweep_frame(n: int = 40) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append(
            {
                "Open": 100.0,
                "High": 101.0,
                "Low": 99.5,
                "Close": 100.5,
                "Volume": 1_000_000,
            }
        )
    rows[-2]["Low"] = 98.0
    rows[-1]["Low"] = 97.0
    rows[-1]["Close"] = 99.0
    rows[-1]["Open"] = 98.5
    return _make_df(rows, freq="1D", start="2026-07-22")


def test_resolve_htf_bias_bullish_daily():
    df = _daily_bullish_sweep_frame()
    ctx = resolve_htf_bias(df, None)
    assert ctx is not None
    assert ctx.bias == "BULLISH"
    assert "Low Swept" in ctx.htf_context


def test_bullish_ic_cisd_on_15m():
    df_daily = _daily_bullish_sweep_frame()
    df_daily.iloc[-2, df_daily.columns.get_loc("High")] = 103.0
    ctx = resolve_htf_bias(df_daily, None)
    assert ctx is not None

    trigger_i = 10  # 09:30 + 2.5h = 12:00 UTC, within first 50% of daily candle
    rows = []
    for i in range(trigger_i + 1):
        rows.append(
            {
                "Open": 98.5,
                "High": 102.0,
                "Low": 98.4,
                "Close": 99.0,
                "Volume": 50_000,
            }
        )
    # down-close sequence immediately before trigger (only these bars)
    for j in range(trigger_i - 4, trigger_i):
        rows[j]["Open"] = 99.0
        rows[j]["Close"] = 98.5
        rows[j]["Low"] = 98.3
        rows[j]["High"] = 99.1
    # trigger: close above cisd (max open of down series ~99.0)
    rows[trigger_i]["Open"] = 98.8
    rows[trigger_i]["Close"] = 99.5
    rows[trigger_i]["High"] = 99.6
    rows[trigger_i]["Low"] = 98.7

    df_15m = _make_df(rows, freq="15min", start="2026-08-30 09:30")
    sig = detect_latest_pattern(df_15m, "15M", htf_ctx=ctx, df_daily=df_daily)
    assert sig is not None
    assert sig["direction"] == "BULLISH"
    assert sig["pattern_levels"]["setup_status"] in ("ENTRY_READY", "LOW_RR")


def test_adapter_maps_ic_cisd_row():
    signal = {
        "direction": "BULLISH",
        "timeframe": "15M",
        "entry_price": 45.2,
        "stop_loss": 44.7,
        "take_profit": 47.2,
        "timestamp": "2026-08-31T14:30:00+00:00",
        "pattern_candles": [{"time": 1787676300, "index": "CISD"}],
        "pattern_levels": {
            "setup": "IC_CISD_LONG",
            "htf_context": "Daily Low Swept",
            "cisd_level": 45.0,
            "protected_level": 44.7,
            "setup_status": "ENTRY_READY",
        },
    }
    row = signal_to_crt_row(signal, ticker="PLTR")
    assert row["type"] == "bullish_ic_cisd"
    assert row["subtype"] == "IC-CISD"
    assert row["pattern_at"] is not None


def test_rr_below_min_not_returned():
    df_daily = _daily_bullish_sweep_frame()
    ctx = resolve_htf_bias(df_daily, None)
    rows = []
    for i in range(20):
        rows.append({"Open": 50, "High": 50.2, "Low": 49.8, "Close": 49.9, "Volume": 1000})
    rows[-2]["Open"] = 50
    rows[-2]["Close"] = 49.5
    rows[-1]["Open"] = 49.6
    rows[-1]["Close"] = 50.05
    rows[-1]["High"] = 50.1
    rows[-1]["Low"] = 49.5
    df = _make_df(rows, freq="15min", start="2026-08-30 09:30")
    sig = detect_latest_pattern(df, "15M", htf_ctx=ctx, df_daily=df_daily)
    # May be None if RR too low or timing fails — acceptable
    if sig is not None:
        assert sig["pattern_levels"]["rr"] >= 1.5


def test_extract_ticker_ohlc_from_grouped_download():
    from market_data import _extract_ticker_ohlc

    idx = pd.date_range("2026-08-01", periods=3, freq="1D")
    cols = pd.MultiIndex.from_product(
        [["AAPL", "MSFT"], ["Open", "High", "Low", "Close", "Volume"]],
        names=["ticker", "price"],
    )
    data = {
        ("AAPL", "Open"): [1, 2, 3],
        ("AAPL", "High"): [1.5, 2.5, 3.5],
        ("AAPL", "Low"): [0.9, 1.9, 2.9],
        ("AAPL", "Close"): [1.2, 2.2, 3.2],
        ("AAPL", "Volume"): [10, 20, 30],
        ("MSFT", "Open"): [10, 11, 12],
        ("MSFT", "High"): [11, 12, 13],
        ("MSFT", "Low"): [9, 10, 11],
        ("MSFT", "Close"): [10.5, 11.5, 12.5],
        ("MSFT", "Volume"): [100, 110, 120],
    }
    raw = pd.DataFrame(data, index=idx)
    raw.columns = cols
    aapl = _extract_ticker_ohlc(raw, "AAPL")
    assert aapl is not None
    assert list(aapl.columns)[:5] == ["Open", "High", "Low", "Close", "Volume"]
    assert float(aapl["Close"].iloc[-1]) == 3.2

    # yfinance group_by="ticker" often uses (Ticker, Field) without names
    unnamed = pd.DataFrame(data, index=idx)
    unnamed.columns = pd.MultiIndex.from_tuples(list(data.keys()))
    msft = _extract_ticker_ohlc(unnamed, "MSFT")
    assert msft is not None
    assert float(msft["Close"].iloc[0]) == 10.5
