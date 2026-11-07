from dataclasses import replace

import pandas as pd
import pytest

from signal_context import bar_end
from strategy.devils_sweep_cisd import SweepCisdConfig, find_entries, previous_day_levels


def fixture(short=False, count=20):
    days = pd.bdate_range("2025-01-02", periods=15, tz="America/New_York")
    index = pd.DatetimeIndex([day + pd.Timedelta(hours=h, minutes=30)
                              for day in days for h in (9, 13)]).tz_convert("UTC")[:count]
    rows = [[102, 103, 101, 101.5] for _ in range(count)]
    rows[14] = [101.5, 101.8, 99, 100.4]  # sweep previous-day low 100 and reclaim
    rows[15] = [100.4, 102.6, 100.4, 102.2]  # strict Devil + CISD at the same close
    for i in range(16, count):
        rows[i] = [102.3, 104, 101.8, 103.2]
    df = pd.DataFrame(rows, columns=["Open", "High", "Low", "Close"], index=index)
    levels = pd.DataFrame({"High": 108.0, "Low": 100.0,
                           "reference_at": pd.Timestamp("2025-01-01T05:00:00Z"),
                           "available_at": pd.Timestamp("2025-01-01T21:00:00Z")}, index=index)
    if short:
        df = pd.DataFrame({"Open": 200-df.Open, "High": 200-df.Low,
                           "Low": 200-df.High, "Close": 200-df.Close}, index=index)
        levels = levels.assign(High=100.0, Low=92.0)
    return df, levels


@pytest.mark.parametrize("short", [False, True])
def test_ordered_sweep_marker_cisd_and_structural_stop(short):
    df, levels = fixture(short)
    result = find_entries(df, levels, "TEST")
    signal, = result["entries"]
    assert signal["direction"] == ("BEARISH" if short else "BULLISH")
    assert signal["pattern_at"] == df.index[15].isoformat()
    setup = signal["setup"]
    assert setup["sweep_at"] == df.index[14].isoformat()
    assert setup["marker_at"] == setup["cisd_at"] == signal["pattern_at"]
    assert signal["signal_available_at"] == bar_end(df.index[15:16], "4H")[0].isoformat()
    if short:
        assert signal["stop_loss"] > df.High.iloc[14] and signal["target"] == 92
    else:
        assert signal["stop_loss"] < df.Low.iloc[14] and signal["target"] == 108
    assert signal["entry_price"] == df.Close.iloc[15]
    assert setup["reference_available_at"] < setup["sweep_at"]


def test_marker_is_required_for_variant_but_not_ablation():
    df, levels = fixture()
    df.iloc[15, df.columns.get_loc("Low")] = 99.8
    assert find_entries(df, levels, "TEST")["entries"] == []
    assert len(find_entries(df, levels, "TEST", require_marker=False)["entries"]) == 1


def test_marker_must_be_near_the_swept_liquidity_level():
    df, levels = fixture()
    df.iloc[15] = [101.8, 102.6, 101.8, 102.2]
    result = find_entries(df, levels, "TEST")
    assert result["entries"] == []
    assert result["setups"][0]["status"] == "CISD_BEFORE_MARKER"


def test_a_wick_above_cisd_does_not_confirm_the_close():
    df, levels = fixture(count=17)
    df.iloc[15] = [100.4, 102.6, 100.4, 101.6]
    df.iloc[16] = [101.7, 103, 101.2, 101.9]
    result = find_entries(df, levels, "TEST")
    assert result["entries"] == []
    assert result["setups"][0]["status"] == "WAITING_CISD"


def test_cisd_can_confirm_on_a_later_candle_with_frozen_level():
    df, levels = fixture()
    df.iloc[15] = [100.4, 101.9, 100.4, 101.6]
    df.iloc[16] = [101.7, 102.6, 101.2, 102.3]
    result = find_entries(df, levels, "TEST")
    signal, = result["entries"]
    assert signal["pattern_at"] == df.index[16].isoformat()
    assert signal["setup"]["marker_at"] == df.index[15].isoformat()
    assert signal["cisd_level"] == 102


def test_optional_strict_sequence_rejects_simultaneous_confirmation():
    df, levels = fixture()
    config = replace(SweepCisdConfig(), allow_same_bar_cisd=False)
    result = find_entries(df, levels, "TEST", config=config)
    assert result["entries"] == []
    assert result["setups"][0]["status"] == "SIMULTANEOUS_CISD_REJECTED"


def test_setup_expires_after_three_bars():
    df, levels = fixture()
    for i in (15, 16, 17):
        df.iloc[i] = [100.5, 101.9, 100.2, 101.6]
    df.iloc[18] = [100.4, 102.6, 100.4, 102.2]
    result = find_entries(df, levels, "TEST")
    assert result["entries"] == []
    assert result["setups"][0]["status"] == "EXPIRED"


def test_risk_rejection_reports_the_actual_blocking_parameter():
    df, levels = fixture()
    result = find_entries(df, levels, "TEST", config=replace(SweepCisdConfig(),min_rr=2.0))
    assert result["entries"] == []
    setup = result["setups"][0]
    assert setup["status"] == "RISK_FILTERED" and setup["risk_rejection"] == "RR_BELOW_MINIMUM"
    assert 1.5 < setup["candidate_rr"] < 2.0
    assert setup["cisd_at"] == df.index[15].isoformat()


def test_new_sweep_extreme_and_consumed_target_cancel_old_setup():
    for candle, reason in (([100.4, 102.6, 98.5, 101.6], "SWEEP_INVALIDATED"),
                           ([100.4, 108.2, 100.4, 101.6], "TARGET_CONSUMED")):
        df, levels = fixture()
        df.iloc[15] = candle
        result = find_entries(df, levels, "TEST")
        assert result["entries"] == []
        assert result["setups"][0]["status"] == reason


def test_entry_cannot_use_a_forming_marker_cisd_candle():
    df, levels = fixture()
    available = bar_end(df.index[15:16], "4H")[0]
    assert find_entries(df, levels, "TEST", as_of=available-pd.Timedelta(seconds=1))["entries"] == []
    assert len(find_entries(df, levels, "TEST", as_of=available)["entries"]) == 1


def test_future_prices_cannot_change_existing_entries():
    df, levels = fixture()
    prefix = find_entries(df.iloc[:16], levels.iloc[:16], "TEST")["entries"]
    df.iloc[18] = [100, 120, 80, 90]
    whole = find_entries(df, levels, "TEST")["entries"]
    assert [s for s in whole if s["pattern_at"] <= df.index[15].isoformat()] == prefix


def test_unknown_future_liquidity_and_invalid_ohlc_are_rejected():
    df, levels = fixture()
    levels["available_at"] = levels.index + pd.Timedelta(days=1)
    with pytest.raises(ValueError, match="not known"):
        find_entries(df, levels, "TEST")
    df.iloc[0, df.columns.get_loc("High")] = 1
    with pytest.raises(ValueError, match="geometry"):
        find_entries(df, levels, "TEST")


def test_previous_day_levels_are_closed_and_dst_safe():
    daily = pd.DataFrame({"Open": [100,101,102], "High": [110,111,999],
                          "Low": [90,91,1], "Close": [101,102,103]},
                         index=pd.to_datetime(["2025-03-07", "2025-03-10", "2025-03-11"]).tz_localize("America/New_York"))
    idx = pd.to_datetime(["2025-03-10T13:30:00Z", "2025-03-10T17:30:00Z", "2025-03-11T13:30:00Z"])
    bars = pd.DataFrame({"Open": 100, "High": 101, "Low": 99, "Close": 100}, index=idx)
    levels = previous_day_levels(bars, daily)
    assert levels.High.tolist() == [110,110,111]
    assert levels.Low.tolist() == [90,90,91]
    assert levels.available_at.iloc[0] == pd.Timestamp("2025-03-07T21:00:00Z")
    assert levels.available_at.iloc[2] == pd.Timestamp("2025-03-10T20:00:00Z")


@pytest.mark.parametrize("changes", [{"max_setup_bars":0}, {"max_setup_bars":1.5},
                                    {"atr_period":1}, {"stop_buffer_atr":-1}, {"min_rr":30}])
def test_invalid_experiment_parameters_are_rejected(changes):
    with pytest.raises(ValueError):
        replace(SweepCisdConfig(), **changes)
