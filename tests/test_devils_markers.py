"""Unit tests for Devil's Markers (No-Wick Continuation)."""
from __future__ import annotations

import pandas as pd

from market_data import resample_to_7h
from strategy.devils_markers import (
    RECENT_LOOKBACK,
    detect_active_devils_marker,
    find_devils_markers,
)


def _make_df(rows: list[dict], freq: str = "1D", start: str = "2025-01-01") -> pd.DataFrame:
    idx = pd.date_range(start, periods=len(rows), freq=freq)
    return pd.DataFrame(rows, index=idx)


def _base_rows(n: int, mid: float = 100.0) -> list[dict]:
    rows = []
    for _ in range(n):
        rows.append(
            {
                "Open": mid,
                "High": mid + 1.0,
                "Low": mid - 1.0,
                "Close": mid + 0.2,
                "Volume": 1_000_000,
            }
        )
    return rows


def test_bearish_devils_marker_continuation():
    """
    Swing in the last 3 confirmable bars: bullish run → no-wick high →
    close through -CISD without breaking the devil high.
    n=20, window=3 → candidates indices 14..16; swing at 14.
    """
    rows = _base_rows(20, mid=100.0)
    swing = 14

    # Bullish run into swing
    for j, open_px in enumerate([100.0, 101.0, 102.0]):
        i = swing - 3 + j
        rows[i]["Open"] = open_px
        rows[i]["Close"] = open_px + 0.8
        rows[i]["High"] = open_px + 1.0
        rows[i]["Low"] = open_px - 0.2

    rows[swing]["Open"] = 103.0
    rows[swing]["Close"] = 104.9
    rows[swing]["High"] = 105.0
    rows[swing]["Low"] = 103.0

    for k in range(1, 4):
        rows[swing + k]["High"] = 104.0 - k * 0.3
        rows[swing + k]["Low"] = 102.0
        rows[swing + k]["Open"] = 103.5
        rows[swing + k]["Close"] = 102.5

    for k in range(1, 4):
        rows[swing - k]["High"] = 104.0 - k * 0.2

    # Break CISD (min open of bullish run = 100)
    rows[18]["Open"] = 101.0
    rows[18]["Close"] = 99.0
    rows[18]["High"] = 101.2
    rows[18]["Low"] = 98.8

    df = _make_df(rows)
    sig = detect_active_devils_marker(df, ticker="TEST", timeframe="1D")
    assert sig is not None
    assert sig["direction"] == "BEARISH"
    assert sig["devils_marker_price"] == 105.0
    assert sig["cisd_level"] == 100.0
    assert sig["is_cisd_broken"] is True
    assert sig["status"] == "CONTINUATION_ACTIVE"
    assert sig["stop_loss"] == 105.0
    assert sig["target"] == 95.0


def test_bullish_devils_marker_pending():
    rows = _base_rows(20, mid=100.0)
    swing = 14

    for j, open_px in enumerate([100.0, 99.0, 98.0]):
        i = swing - 3 + j
        rows[i]["Open"] = open_px
        rows[i]["Close"] = open_px - 0.8
        rows[i]["High"] = open_px + 0.2
        rows[i]["Low"] = open_px - 1.0

    rows[swing]["Open"] = 97.0
    rows[swing]["Close"] = 95.1
    rows[swing]["Low"] = 95.0
    rows[swing]["High"] = 97.0

    for k in range(1, 4):
        rows[swing + k]["Low"] = 95.5 + k * 0.2
        rows[swing + k]["High"] = 98.0
        rows[swing + k]["Open"] = 96.0
        rows[swing + k]["Close"] = 96.5

    for k in range(1, 4):
        rows[swing - k]["Low"] = 95.5 + k * 0.2

    for i in range(swing + 4, 20):
        rows[i]["Open"] = 97.0
        rows[i]["Close"] = 97.5
        rows[i]["High"] = 98.5
        rows[i]["Low"] = 96.5

    df = _make_df(rows)
    markers = find_devils_markers(df, ticker="TEST", timeframe="1D")
    bull = [m for m in markers if m["direction"] == "BULLISH"]
    assert bull
    assert bull[0]["status"] == "MARKER_PENDING"
    assert bull[0]["is_cisd_broken"] is False
    assert bull[0]["cisd_level"] == 100.0


def test_invalidated_on_structure_break():
    rows = _base_rows(20, mid=100.0)
    swing = 14

    for j, open_px in enumerate([100.0, 101.0, 102.0]):
        i = swing - 3 + j
        rows[i]["Open"] = open_px
        rows[i]["Close"] = open_px + 0.8
        rows[i]["High"] = open_px + 1.0
        rows[i]["Low"] = open_px - 0.2

    rows[swing]["Open"] = 103.0
    rows[swing]["Close"] = 104.9
    rows[swing]["High"] = 105.0
    rows[swing]["Low"] = 103.0

    for k in range(1, 4):
        rows[swing + k]["High"] = 104.0 - k * 0.3
        rows[swing + k]["Low"] = 102.0
        rows[swing + k]["Open"] = 103.5
        rows[swing + k]["Close"] = 102.5

    for k in range(1, 4):
        rows[swing - k]["High"] = 104.0 - k * 0.2

    rows[18]["High"] = 106.0
    rows[18]["Close"] = 105.5
    rows[18]["Open"] = 104.0
    rows[18]["Low"] = 103.8

    df = _make_df(rows)
    active = detect_active_devils_marker(df, ticker="TEST", timeframe="1D")
    assert active is None

    all_m = find_devils_markers(
        df, ticker="TEST", timeframe="1D", include_invalidated=True
    )
    bear = [m for m in all_m if m["direction"] == "BEARISH"]
    assert bear
    assert bear[0]["status"] == "INVALIDATED"


def test_ignores_old_devils_outside_recent_lookback():
    """An old no-wick swing far from the end must not be returned."""
    rows = _base_rows(30, mid=100.0)
    swing = 10  # well outside last 3 confirmable (indices 26..28 for n=30)

    for j, open_px in enumerate([100.0, 101.0, 102.0]):
        i = swing - 3 + j
        rows[i]["Open"] = open_px
        rows[i]["Close"] = open_px + 0.8
        rows[i]["High"] = open_px + 1.0
        rows[i]["Low"] = open_px - 0.2

    rows[swing]["Open"] = 103.0
    rows[swing]["Close"] = 104.9
    rows[swing]["High"] = 105.0
    rows[swing]["Low"] = 103.0

    for k in range(1, 4):
        rows[swing + k]["High"] = 104.0 - k * 0.3
        rows[swing + k]["Low"] = 102.0
        rows[swing + k]["Open"] = 103.5
        rows[swing + k]["Close"] = 102.5

    for k in range(1, 4):
        rows[swing - k]["High"] = 104.0 - k * 0.2

    df = _make_df(rows)
    assert find_devils_markers(df, ticker="TEST", timeframe="1D") == []
    assert detect_active_devils_marker(df, ticker="TEST", timeframe="1D") is None
    # Explicit wide lookback still finds it
    wide = find_devils_markers(
        df, ticker="TEST", timeframe="1D", recent_lookback=25
    )
    assert any(m["devils_marker_price"] == 105.0 for m in wide)
    assert RECENT_LOOKBACK == 3


def test_resample_to_7h():
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
    h7 = resample_to_7h(df)
    assert len(h7) >= 2
    assert list(h7.columns)[:4] == ["Open", "High", "Low", "Close"]
