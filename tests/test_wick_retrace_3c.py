import pandas as pd
import pytest

from strategy.wick_retrace_3c import (
    MIN_PATTERN_BARS,
    compute_pattern_signals,
    detect_latest_pattern,
    _is_valid_bullish_block,
    _is_valid_bearish_block,
    _ema_close,
)


def _bar(open_: float, high: float, low: float, close: float, volume: float = 1000) -> dict:
    return {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume}


def _df_from_bars(bars: list[dict]) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=len(bars), freq="1h")
    return pd.DataFrame(bars, index=idx)


def _flat(price: float, volume: float = 1000) -> dict:
    return _bar(price, price + 0.05, price - 0.05, price, volume)


def _bullish_green(
    open_: float, close: float, lower_wick: float, upper_wick: float = 0.1, volume: float = 1000
) -> dict:
    low = min(open_, close) - lower_wick
    high = max(open_, close) + upper_wick
    return _bar(open_, high, low, close, volume)


def _bearish_red(
    open_: float, close: float, upper_wick: float, lower_wick: float = 0.1, volume: float = 1000
) -> dict:
    low = min(open_, close) - lower_wick
    high = max(open_, close) + upper_wick
    return _bar(open_, high, low, close, volume)


def _warmup_flat(n: int, price: float = 100.0, volume: float = 1000) -> list[dict]:
    return [_flat(price, volume) for _ in range(n)]


def _bullish_smc_bars(
    *,
    sweep_low: float = 94.0,
    vol_c1: float = 1000,
    vol_c3: float = 2000,
    close_c3: float = 103.0,
) -> list[dict]:
    """20 structure bars + C0..C3 forming a valid bullish SMC setup."""
    # Structure lows stay at 96.5 so sweep_low < structure_low
    structure = []
    for i in range(20):
        p = 100.0 + (i % 3) * 0.1
        structure.append(_bar(p, p + 0.5, 96.5, p + 0.2, 1000))

    c0 = _bar(102, 102.5, sweep_low, 100, 1000)  # red C0, low sweeps
    c1 = _bar(100, 101.1, 98.0, 101, vol_c1)  # mid wick = 99.0
    c2 = _bar(101, 102.1, 99.5, 102, 1200)  # low >= mid C1
    c3 = _bar(102, close_c3 + 0.1, 101.5, close_c3, vol_c3)
    return structure + [c0, c1, c2, c3]


def _bearish_smc_bars(
    *,
    sweep_high: float = 106.0,
    vol_c1: float = 1000,
    vol_c3: float = 2000,
    close_c3: float = 97.0,
) -> list[dict]:
    structure = []
    for i in range(20):
        p = 100.0 - (i % 3) * 0.1
        structure.append(_bar(p, 103.5, p - 0.5, p - 0.2, 1000))

    c0 = _bar(98, sweep_high, 97.5, 100, 1000)  # green C0, high sweeps
    c1 = _bar(100, 102.0, 98.9, 99, vol_c1)  # red, upper wick
    c2 = _bar(99, 100.5, 97.9, 98, 1200)
    c3 = _bar(98, 98.5, close_c3 - 0.1, close_c3, vol_c3)
    return structure + [c0, c1, c2, c3]


def test_min_pattern_bars():
    assert MIN_PATTERN_BARS == 24


def test_bullish_smc_valid():
    df = _df_from_bars(_bullish_smc_bars())
    assert len(df) == 24
    ema = _ema_close(df)
    i = len(df) - 1
    assert _is_valid_bullish_block(df, i, ema) is True
    result = detect_latest_pattern(df, "1H")
    assert result is not None
    assert result["direction"] == "BULLISH"
    assert "ema_20" in result
    assert "swept_level" in result
    assert "structure_level" in result


def test_bearish_smc_valid():
    df = _df_from_bars(_bearish_smc_bars())
    ema = _ema_close(df)
    i = len(df) - 1
    assert _is_valid_bearish_block(df, i, ema) is True
    result = detect_latest_pattern(df, "15M")
    assert result is not None
    assert result["direction"] == "BEARISH"


def test_bullish_fails_volume():
    df = _df_from_bars(_bullish_smc_bars(vol_c1=3000, vol_c3=1000))
    ema = _ema_close(df)
    assert _is_valid_bullish_block(df, len(df) - 1, ema) is False


def test_bullish_fails_no_sweep():
    # Sweep low stays above structure floor (96.5)
    df = _df_from_bars(_bullish_smc_bars(sweep_low=97.0))
    ema = _ema_close(df)
    assert _is_valid_bullish_block(df, len(df) - 1, ema) is False


def test_bullish_fails_ema():
    # Close C3 well below EMA after flat high structure — force low close
    bars = _bullish_smc_bars(close_c3=90.0)
    # Fix colors: last bar still green but close far below EMA
    bars[-1] = _bar(89.5, 90.1, 89.0, 90.0, 2000)
    df = _df_from_bars(bars)
    ema = _ema_close(df)
    # May fail wick or color path — ensure EMA fails when other conditions held
    # Rebuild with valid wick colors but low close still green above open
    structure = []
    for i in range(20):
        p = 110.0
        structure.append(_bar(p, p + 0.5, 96.5, p, 1000))
    bars = structure + [
        _bar(112, 112.5, 94.0, 110, 1000),  # C0 red
        _bar(110, 111.1, 108.0, 111, 1000),  # C1
        _bar(111, 112.1, 109.5, 112, 1200),  # C2
        _bar(100, 100.5, 99.5, 100.2, 2000),  # C3 green but deep below EMA ~110
    ]
    df = _df_from_bars(bars)
    ema = _ema_close(df)
    assert float(ema.iloc[-1]) > 100.2
    assert _is_valid_bullish_block(df, len(df) - 1, ema) is False


def test_bullish_fails_c0_same_color():
    bars = _bullish_smc_bars()
    # Make C0 green
    bars[-4] = _bar(99, 102.5, 94.0, 100, 1000)
    df = _df_from_bars(bars)
    ema = _ema_close(df)
    assert _is_valid_bullish_block(df, len(df) - 1, ema) is False


def test_bullish_fails_wick_50pct():
    bars = _bullish_smc_bars()
    # C2 dips below mid of C1 (mid ≈ 99.0)
    bars[-2] = _bar(101, 102.1, 97.0, 102, 1200)
    df = _df_from_bars(bars)
    ema = _ema_close(df)
    assert _is_valid_bullish_block(df, len(df) - 1, ema) is False


def test_continuation_c4_not_tagged():
    bars = _bullish_smc_bars()
    bars.append(_bar(103, 104.1, 102.5, 104, 2500))
    df = _df_from_bars(bars)
    signals = compute_pattern_signals(df)
    # Pattern confirmed at index 23 (0-based), C1=21,C2=22,C3=23; C4=24 must be 0
    assert signals.iloc[21] == 1
    assert signals.iloc[22] == 1
    assert signals.iloc[23] == 1
    assert signals.iloc[24] == 0


def test_detect_latest_none_if_not_on_last_bar():
    bars = _bullish_smc_bars()
    bars.append(_bearish_red(103, 102, upper_wick=0.5))
    df = _df_from_bars(bars)
    assert detect_latest_pattern(df, "1H") is None


def test_too_few_bars():
    df = _df_from_bars(_warmup_flat(10))
    assert detect_latest_pattern(df, "1H") is None
    assert compute_pattern_signals(df).sum() == 0
