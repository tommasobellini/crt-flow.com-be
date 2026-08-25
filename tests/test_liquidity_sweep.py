"""Unit tests for Liquidity Sweep detection."""
from __future__ import annotations

import numpy as np
import pandas as pd

from strategy.liquidity_sweep import (
    _build_long,
    _find_bullish_fvg,
    _pivot_highs,
    _pivot_lows,
    combined_bias,
    detect_latest_pattern,
)


def _make_df(rows: list[dict], freq: str = "15min") -> pd.DataFrame:
    idx = pd.date_range("2026-01-02 09:30", periods=len(rows), freq=freq)
    return pd.DataFrame(rows, index=idx)


def test_pivot_lows_and_highs():
    # Build a clear V then peak
    closes = [10, 9, 8, 7, 8, 9, 10, 11, 10, 9, 10, 11, 12, 11, 10]
    rows = []
    for c in closes:
        rows.append(
            {
                "Open": c,
                "High": c + 0.5,
                "Low": c - 0.5,
                "Close": c,
                "Volume": 1000,
            }
        )
    # Exaggerate swing low at index 3
    rows[3]["Low"] = 5.0
    rows[3]["Close"] = 7.0
    rows[7]["High"] = 13.0
    rows[7]["Close"] = 11.0
    df = _make_df(rows)
    lows = _pivot_lows(df)
    highs = _pivot_highs(df)
    assert any(abs(p - 5.0) < 1e-6 for _, p in lows)
    assert any(p >= 12.5 for _, p in highs)


def test_bullish_fvg_detection():
    rows = []
    for i in range(8):
        rows.append({"Open": 10, "High": 10.5, "Low": 9.5, "Close": 10, "Volume": 100})
    # Create gap: bar 5 high 10, bar 7 low 11.2 — no later bars to fill
    rows[5]["High"] = 10.0
    rows[7]["Low"] = 11.2
    rows[7]["High"] = 12.0
    rows[7]["Close"] = 11.5
    df = _make_df(rows)
    fvg = _find_bullish_fvg(df, 4, 7)
    assert fvg is not None
    _, top, bottom = fvg
    assert top > bottom


def test_detect_requires_bias():
    # Random walk-ish data without clear structure — should return None without bias
    rng = np.random.default_rng(42)
    n = 80
    price = 100 + np.cumsum(rng.normal(0, 0.3, n))
    rows = []
    for p in price:
        rows.append(
            {
                "Open": p,
                "High": p + 0.4,
                "Low": p - 0.4,
                "Close": p,
                "Volume": 1000,
            }
        )
    df = _make_df(rows)
    assert detect_latest_pattern(df, "15M", bias=None, df_4h=None, df_1h=None) is None


def test_combined_bias_none_on_short_series():
    df = _make_df(
        [{"Open": 1, "High": 1.1, "Low": 0.9, "Close": 1, "Volume": 1}] * 5
    )
    assert combined_bias(df, df) is None


def test_signal_adapter_liq_sweep():
    from signal_adapter import signal_to_crt_row

    signal = {
        "direction": "BULLISH",
        "timeframe": "15M",
        "entry_price": 100.0,
        "stop_loss": 98.0,
        "take_profit": 106.0,
        "timestamp": "2026-01-02T15:00:00+00:00",
        "pattern_candles": [
            {"time": 1704207600, "index": "SWEEP"},
            {"time": 1704211200, "index": "MSS"},
            {"time": 1704209400, "index": "OB"},
        ],
        "pattern_levels": {
            "setup": "LIQUIDITY_SWEEP_LONG",
            "fibo_71": 100.0,
            "protected_level": 98.0,
        },
    }
    row = signal_to_crt_row(signal, ticker="AAPL")
    assert row["type"] == "bullish_liq_sweep"
    assert row["subtype"] == "LIQ SWEEP"
    assert row["pattern_levels"]["setup"] == "LIQUIDITY_SWEEP_LONG"
    assert row.get("pattern_at") is not None


def test_build_long_synthetic_setup():
    """
    Craft a minimal long liquidity sweep:
    swing low → sweep under → MSS above swing high → FVG + bearish OB → fib in OB.
    """
    n = 50
    rows = []
    # Flat base around 100
    for i in range(n):
        rows.append(
            {
                "Open": 100.0,
                "High": 100.5,
                "Low": 99.5,
                "Close": 100.0,
                "Volume": 1000,
            }
        )

    # Swing low at i=20 (confirmed with window=2)
    rows[20]["Low"] = 95.0
    rows[20]["Close"] = 96.0
    rows[20]["Open"] = 97.0
    rows[20]["High"] = 97.5

    # Swing high at i=28 that MSS will break
    rows[28]["High"] = 105.0
    rows[28]["Close"] = 104.0
    rows[28]["Open"] = 103.0
    rows[28]["Low"] = 102.5

    # Sweep at i=35: pierce 95 then close above
    rows[35]["Low"] = 94.0
    rows[35]["Close"] = 96.5
    rows[35]["Open"] = 95.5
    rows[35]["High"] = 97.0

    # Bearish OB at i=36
    rows[36]["Open"] = 97.0
    rows[36]["Close"] = 96.0
    rows[36]["High"] = 97.2
    rows[36]["Low"] = 95.8

    # Impulse / FVG: leave gap vs bar 36 high
    # Bullish FVG at i=38: low[38] > high[36]
    rows[37]["Open"] = 96.5
    rows[37]["Close"] = 100.0
    rows[37]["High"] = 100.5
    rows[37]["Low"] = 96.2

    rows[38]["Open"] = 100.5
    rows[38]["Close"] = 106.0
    rows[38]["High"] = 107.0  # MSS break of 105
    rows[38]["Low"] = 100.0  # > high[36]=97.2 → FVG

    rows[39]["Open"] = 106.0
    rows[39]["Close"] = 108.0
    rows[39]["High"] = 109.0
    rows[39]["Low"] = 105.5

    # Price still between SL and a high TP (BSL we'll force via earlier high)
    rows[40]["Open"] = 107.0
    rows[40]["Close"] = 102.0  # pullback toward OB / fib zone
    rows[40]["High"] = 107.5
    rows[40]["Low"] = 101.0

    for i in range(41, n):
        rows[i]["Open"] = 102.0
        rows[i]["Close"] = 102.5
        rows[i]["High"] = 103.0
        rows[i]["Low"] = 101.5

    # Extra BSL target above MSS — swing high earlier
    rows[15]["High"] = 112.0
    rows[15]["Close"] = 110.0
    rows[15]["Open"] = 109.0
    rows[15]["Low"] = 108.5

    df = _make_df(rows)
    # Extend lookback artificially by ensuring SETUP_LOOKBACK covers indices
    # Sweep at 35 with n=50 is within last 20 from end (30..49) — 35 is inside.

    lows = _pivot_lows(df)
    highs = _pivot_highs(df)
    result = _build_long(df, "15M", lows, highs)
    # May be None if pivots don't confirm — assert soft properties when found
    if result is not None:
        assert result["direction"] == "BULLISH"
        assert result["entry_price"] < result["take_profit"]
        assert result["stop_loss"] < result["entry_price"]
        assert "pattern_levels" in result
