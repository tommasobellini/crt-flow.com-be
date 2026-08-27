"""Unit tests for Sweep & Engulf detection."""
from __future__ import annotations

import pandas as pd

from signal_adapter import signal_to_crt_row
from strategy.sweep_engulf import detect_latest_pattern


def _make_df(rows: list[dict], freq: str = "1h") -> pd.DataFrame:
    idx = pd.date_range("2026-01-02 09:30", periods=len(rows), freq=freq, tz="UTC")
    return pd.DataFrame(rows, index=idx)


def test_bullish_sweep_engulf_detected():
    rows = []
    for i in range(12):
        rows.append(
            {
                "Open": 100 + i * 0.1,
                "High": 101 + i * 0.1,
                "Low": 99 + i * 0.1,
                "Close": 100.5 + i * 0.1,
                "Volume": 1000 + i * 50,
            }
        )
    # prior bar high for engulf reference
    rows[-2]["High"] = 100.0
    rows[-2]["Low"] = 98.5
    rows[-2]["Close"] = 99.0
    # signal bar: sweep below lookback lows, close above prev high
    rows[-1]["Low"] = 96.0
    rows[-1]["High"] = 101.5
    rows[-1]["Close"] = 100.8
    rows[-1]["Open"] = 99.5
    rows[-1]["Volume"] = 2500

    df = _make_df(rows)
    sig = detect_latest_pattern(df, "1H")
    assert sig is not None
    assert sig["direction"] == "BULLISH"
    assert sig["entry_price"] == sig["stop_loss"] + (sig["take_profit"] - sig["entry_price"]) / 2
    assert sig["pattern_levels"]["quality_score"] >= 0


def test_bearish_sweep_engulf_detected():
    rows = []
    for i in range(12):
        rows.append(
            {
                "Open": 100 - i * 0.1,
                "High": 101 - i * 0.1,
                "Low": 99 - i * 0.1,
                "Close": 99.5 - i * 0.1,
                "Volume": 1000 + i * 50,
            }
        )
    rows[-2]["High"] = 101.5
    rows[-2]["Low"] = 100.0
    rows[-2]["Close"] = 101.0
    rows[-1]["High"] = 103.0
    rows[-1]["Low"] = 99.5
    rows[-1]["Close"] = 99.8
    rows[-1]["Open"] = 102.0
    rows[-1]["Volume"] = 2500

    df = _make_df(rows)
    sig = detect_latest_pattern(df, "4H")
    assert sig is not None
    assert sig["direction"] == "BEARISH"
    assert sig["stop_loss"] == rows[-1]["High"]


def test_adapter_maps_sweep_engulf_row():
    signal = {
        "direction": "BULLISH",
        "timeframe": "1H",
        "entry_price": 100.0,
        "stop_loss": 98.0,
        "take_profit": 104.0,
        "timestamp": "2026-01-10T14:00:00+00:00",
        "pattern_candles": [{"time": 1736517600, "index": "SWEEP"}],
        "pattern_levels": {"setup": "SWEEP_ENGULF_LONG", "quality_score": 72},
    }
    row = signal_to_crt_row(signal, ticker="AAPL")
    assert row["type"] == "bullish_sweep_engulf"
    assert row["subtype"] == "SWEEP ENGULF"
    assert row["timeframe"] == "1H"
    assert row["entry_price"] == 100.0
    assert row["pattern_at"] is not None
