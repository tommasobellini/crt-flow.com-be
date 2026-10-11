"""Unit tests for Pure No-Wick Devil's Markers."""
from __future__ import annotations

import pandas as pd

from market_data import resample_to_7h, resample_to_monthly
from strategy.devils_markers import (
    detect_active_devils_marker,
    find_devils_markers,
)


def _make_df(rows: list[dict], freq: str = "1D", start: str = "2025-01-01") -> pd.DataFrame:
    idx = pd.date_range(start, periods=len(rows), freq=freq)
    return pd.DataFrame(rows, index=idx)


def _base_rows(n: int, mid: float = 100.0) -> list[dict]:
    rows = []
    for i in range(n):
        px = mid - i * 0.05
        rows.append(
            {
                "Open": px,
                "High": px + 0.8,
                "Low": px - 0.8,
                "Close": px + 0.1,
                "Volume": 1_000_000,
            }
        )
    return rows


def test_bearish_pure_no_wick_sane_rr():
    rows = _base_rows(25, mid=110.0)
    # Nearby structural low as TP
    rows[18]["Low"] = 104.0
    rows[18]["High"] = 106.0
    rows[18]["Open"] = 105.5
    rows[18]["Close"] = 104.5
    rows[17]["Low"] = 105.0
    rows[19]["Low"] = 105.0

    i = 24
    rows[i]["Open"] = 108.0
    rows[i]["Close"] = 109.95  # no upper wick (~2% of range)
    rows[i]["High"] = 110.0
    rows[i]["Low"] = 107.5
    rows[i - 1]["High"] = 109.2

    df = _make_df(rows)
    sig = detect_active_devils_marker(df, ticker="TEST", timeframe="1D")
    assert sig is not None
    assert sig["direction"] == "BEARISH"
    assert sig["stop_loss"] == 110.0
    assert sig["entry_price"] < sig["stop_loss"]
    assert abs(sig["entry_price"] - sig["stop_loss"]) / sig["entry_price"] >= 0.0008
    assert 0.5 <= sig["rr_ratio"] <= 25.0
    assert sig["target"] < sig["entry_price"]


def test_rejects_zero_risk_entry_equals_sl():
    rows = _base_rows(25, mid=110.0)
    rows[18]["Low"] = 104.0
    i = 24
    rows[i]["Open"] = 110.0
    rows[i]["Close"] = 110.0
    rows[i]["High"] = 110.0
    rows[i]["Low"] = 108.0
    rows[i - 1]["High"] = 109.0
    df = _make_df(rows)
    assert detect_active_devils_marker(df, ticker="TEST", timeframe="1D") is None


def test_bullish_pure_no_wick_sane_rr():
    rows = _base_rows(25, mid=100.0)
    rows[18]["High"] = 106.0
    rows[18]["Low"] = 104.0
    rows[18]["Open"] = 104.5
    rows[18]["Close"] = 105.5
    rows[17]["High"] = 105.0
    rows[19]["High"] = 105.0

    i = 24
    rows[i]["Open"] = 101.5
    rows[i]["Close"] = 100.05  # no lower wick
    rows[i]["Low"] = 100.0
    rows[i]["High"] = 102.0
    rows[i - 1]["Low"] = 100.5

    df = _make_df(rows)
    markers = find_devils_markers(df, ticker="TEST", timeframe="1D")
    bull = [m for m in markers if m["direction"] == "BULLISH"]
    assert bull
    assert bull[0]["entry_price"] > bull[0]["stop_loss"]
    assert 0.5 <= bull[0]["rr_ratio"] <= 25.0


def test_ignores_old_candles_outside_last_3():
    rows = _base_rows(30, mid=110.0)
    rows[10]["Open"] = 108.0
    rows[10]["Close"] = 109.85
    rows[10]["High"] = 110.0
    rows[10]["Low"] = 107.5
    rows[9]["High"] = 109.0
    rows[11]["High"] = 109.0
    df = _make_df(rows)
    assert find_devils_markers(df, ticker="TEST", timeframe="1D") == []


def test_resample_helpers():
    idx = pd.date_range("2025-01-02 09:30", periods=21, freq="1h")
    df = pd.DataFrame(
        {
            "Open": [100.0] * 21,
            "High": [101.0] * 21,
            "Low": [99.0] * 21,
            "Close": [100.5] * 21,
            "Volume": [1000] * 21,
        },
        index=idx,
    )
    assert len(resample_to_7h(df)) >= 2
    daily_idx = pd.date_range("2020-01-01", periods=400, freq="D")
    daily = pd.DataFrame(
        {
            "Open": [1.0] * 400,
            "High": [2.0] * 400,
            "Low": [0.5] * 400,
            "Close": [1.5] * 400,
            "Volume": [1] * 400,
        },
        index=daily_idx,
    )
    assert len(resample_to_monthly(daily)) >= 12
