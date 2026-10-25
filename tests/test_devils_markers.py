"""Unit tests — Pine Devils Mark: open==high / open==low."""
from __future__ import annotations

import pandas as pd

from market_data import resample_to_4h, resample_to_monthly
from strategy.devils_markers import (
    _is_open_eq_high,
    _is_open_eq_low,
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


def test_pine_open_eq_high_bearish():
    assert _is_open_eq_high(110.0, 110.0)
    assert not _is_open_eq_high(109.0, 110.0)

    rows = _base_rows(25, mid=110.0)
    rows[18]["Low"] = 104.0
    rows[18]["High"] = 106.0
    rows[17]["Low"] = 105.0
    rows[19]["Low"] = 105.0

    i = 24
    # Pine: open == high, close below for short entry
    rows[i]["Open"] = 110.0
    rows[i]["High"] = 110.0
    rows[i]["Low"] = 107.5
    rows[i]["Close"] = 108.0

    df = _make_df(rows)
    sig = detect_active_devils_marker(df, ticker="TEST", timeframe="1D")
    assert sig is not None
    assert sig["direction"] == "BEARISH"
    assert sig["devils_marker_price"] == 110.0
    assert sig["stop_loss"] == 110.0
    assert sig["entry_price"] == 108.0
    assert sig["entry_price"] < sig["stop_loss"]
    assert 0.5 <= sig["rr_ratio"] <= 25.0


def test_pine_open_eq_low_bullish():
    assert _is_open_eq_low(100.0, 100.0)

    rows = _base_rows(25, mid=100.0)
    rows[18]["High"] = 106.0
    rows[18]["Low"] = 104.0
    rows[17]["High"] = 105.0
    rows[19]["High"] = 105.0

    i = 24
    rows[i]["Open"] = 100.0
    rows[i]["Low"] = 100.0
    rows[i]["High"] = 102.5
    rows[i]["Close"] = 101.5

    df = _make_df(rows)
    markers = find_devils_markers(df, ticker="TEST", timeframe="1D")
    bull = [m for m in markers if m["direction"] == "BULLISH"]
    assert bull
    assert bull[0]["devils_marker_price"] == 100.0
    assert bull[0]["stop_loss"] == 100.0
    assert bull[0]["entry_price"] == 101.5
    assert bull[0]["entry_price"] > bull[0]["stop_loss"]


def test_invalidated_when_price_crosses_mark():
    """Pine deletes the line when a later bar crosses the level."""
    rows = _base_rows(25, mid=110.0)
    rows[18]["Low"] = 104.0
    rows[17]["Low"] = 105.0
    rows[19]["Low"] = 105.0

    i = 22  # within last 3 of n=25 → indices 22,23,24
    rows[i]["Open"] = 110.0
    rows[i]["High"] = 110.0
    rows[i]["Low"] = 107.5
    rows[i]["Close"] = 108.0
    # Next bar crosses through 110
    rows[i + 1]["High"] = 111.0
    rows[i + 1]["Low"] = 109.0
    rows[i + 1]["Open"] = 109.5
    rows[i + 1]["Close"] = 110.5

    df = _make_df(rows)
    active = detect_active_devils_marker(df, ticker="TEST", timeframe="1D")
    # Mark at 22 is invalidated by cross; no other valid mark
    assert active is None

    all_m = find_devils_markers(
        df, ticker="TEST", timeframe="1D", include_invalidated=True
    )
    bear = [m for m in all_m if m["direction"] == "BEARISH"]
    assert bear
    assert bear[0]["status"] == "INVALIDATED"


def test_ignores_old_outside_last_3():
    rows = _base_rows(30, mid=110.0)
    rows[10]["Open"] = 110.0
    rows[10]["High"] = 110.0
    rows[10]["Low"] = 107.0
    rows[10]["Close"] = 108.0
    df = _make_df(rows)
    assert find_devils_markers(df, ticker="TEST", timeframe="1D") == []


def test_resample_helpers():
    idx = pd.date_range("2025-01-02 09:30", periods=21, freq="1h", tz="America/New_York")
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
    assert len(resample_to_4h(df)) >= 2
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
