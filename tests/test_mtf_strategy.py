import pandas as pd
import pytest

from market_data import clean_df, resample_to_4h
from signal_adapter import signal_to_crt_row
from strategy.ha_rsi_mtf import assess_timeframe, evaluate_funnel, evaluate_symbol
from strategy.heikin_ashi import to_heikin_ashi


def _make_ohlcv(rows: list[dict], freq: str = "1h") -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=len(rows), freq=freq)
    return pd.DataFrame(rows, index=idx)


def _flat_bar(open_: float, close: float, spread: float = 1.0) -> dict:
    low = min(open_, close) - spread * 0.2
    high = max(open_, close) + spread * 0.2
    return {
        "Open": open_,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": 1000.0,
    }


def _uptrend_bars(n: int, start: float = 100.0, step: float = 0.5) -> list[dict]:
    rows = []
    price = start
    for _ in range(n):
        o = price
        c = price + step
        rows.append(_flat_bar(o, c, spread=0.1))
        price = c
    return rows


def _sideways_bars(n: int, price: float = 100.0) -> list[dict]:
    return [_flat_bar(price, price) for _ in range(n)]


def test_to_heikin_ashi_formula():
    df = _make_ohlcv(
        [
            {"Open": 10.0, "High": 12.0, "Low": 9.0, "Close": 11.0, "Volume": 100},
            {"Open": 11.0, "High": 13.0, "Low": 10.0, "Close": 12.0, "Volume": 100},
        ],
        "1h",
    )
    ha = to_heikin_ashi(df)
    assert ha.iloc[0]["Close"] == pytest.approx((10 + 12 + 9 + 11) / 4)
    assert ha.iloc[1]["Open"] == pytest.approx((ha.iloc[0]["Open"] + ha.iloc[0]["Close"]) / 2)


def test_assess_timeframe_bullish():
    df = _make_ohlcv(_uptrend_bars(30), "1h")
    ok, rsi = assess_timeframe(df)
    assert ok is True
    assert rsi is not None
    assert rsi > 50


def test_assess_timeframe_fails_on_sideways():
    df = _make_ohlcv(_sideways_bars(30), "1h")
    ok, _ = assess_timeframe(df)
    assert ok is False


def test_evaluate_funnel_stages():
    uptrend = _uptrend_bars(45)
    sideways = _sideways_bars(45)

    df_4h_ok = _make_ohlcv(uptrend, "4h")
    df_1h_ok = _make_ohlcv(uptrend, "1h")
    df_15m_ok = _make_ohlcv(uptrend, "15min")

    stage, _ = evaluate_funnel(None, df_1h_ok, df_15m_ok)
    assert stage == "no_data"

    stage, _ = evaluate_funnel(_make_ohlcv(sideways, "4h"), df_1h_ok, df_15m_ok)
    assert stage == "no_4h_structure"

    stage, _ = evaluate_funnel(df_4h_ok, _make_ohlcv(sideways, "1h"), df_15m_ok)
    assert stage == "no_1h_alignment"

    stage, _ = evaluate_funnel(df_4h_ok, df_1h_ok, df_15m_ok)
    assert stage == "signal"


def test_evaluate_symbol_golden_vs_base():
    uptrend = _uptrend_bars(45)
    sideways = _sideways_bars(45)

    df_4h = _make_ohlcv(uptrend, "4h")
    df_1h = _make_ohlcv(uptrend, "1h")
    df_15m_golden = _make_ohlcv(uptrend, "15min")
    df_15m_base = _make_ohlcv(sideways, "15min")

    golden = evaluate_symbol("TEST", df_4h, df_1h, df_15m_golden)
    assert golden is not None
    assert golden["signal_type"] == "BULLISH"
    assert golden["is_golden"] is True
    assert golden["4h_rsi"] > 50
    assert golden["1h_rsi"] > 50
    assert golden["15m_rsi"] > 50

    base = evaluate_symbol("TEST", df_4h, df_1h, df_15m_base)
    assert base is not None
    assert base["is_golden"] is False


def test_signal_adapter_ha_rsi():
    signal = {
        "ticker": "NVDA",
        "signal_type": "BULLISH",
        "is_golden": True,
        "4h_rsi": 54.2,
        "1h_rsi": 58.5,
        "15m_rsi": 51.1,
        "timestamp": "2024-06-01T12:00:00",
    }
    row = signal_to_crt_row(signal, entry_price=200.0)
    assert row["symbol"] == "NVDA"
    assert row["type"] == "bullish_ha_rsi"
    assert row["timeframe"] == "15M"
    assert row["is_golden"] is True
    assert row["rsi_4h"] == 54.2
    assert row["rsi_1h"] == 58.5
    assert row["rsi_15m"] == 51.1
    assert row["entry_price"] == 200.0


def test_clean_df_flattens_yfinance_multiindex():
    idx = pd.date_range("2024-01-01", periods=3, freq="1h")
    raw = pd.DataFrame(
        {
            ("Close", "VTR"): [10.0, 11.0, 12.0],
            ("High", "VTR"): [10.5, 11.5, 12.5],
            ("Low", "VTR"): [9.5, 10.5, 11.5],
            ("Open", "VTR"): [10.0, 10.5, 11.5],
            ("Volume", "VTR"): [100, 200, 300],
        },
        index=idx,
    )
    raw.columns = pd.MultiIndex.from_tuples(raw.columns)

    cleaned = clean_df(raw)
    assert list(cleaned.columns) == ["Close", "High", "Low", "Open", "Volume"]
    assert len(resample_to_4h(cleaned)) >= 1
