from dataclasses import replace

import pandas as pd
import pytest

from signal_context import bar_end
from strategy.swing_4h import BENCHMARK, STRATEGY, SwingConfig, candidates
from strategy.swing_portfolio import metrics, replay


def market():
    days = pd.bdate_range("2025-01-02", periods=35, tz="America/New_York")
    dates = pd.DatetimeIndex([d+pd.Timedelta(hours=h, minutes=30)
                              for d in days[-10:] for h in (9, 13)]).tz_convert("UTC")
    rows = [[105, 106, 104, 105] for _ in dates]
    rows[-2] = [101, 102, 99, 100]
    rows[-1] = [100, 117, 100, 116]
    h4 = pd.DataFrame(rows, index=dates, columns=["Open", "High", "Low", "Close"])
    daily = pd.DataFrame({"Open": 100., "High": 120., "Low": 90.,
                          "Close": [100.+i/2 for i in range(35)], "Volume": 2_000_000.}, index=days)
    config = replace(SwingConfig(), ema_fast=2, ema_slow=3, daily_sma=3, atr_period=2,
                     liquidity_days=2, breakout_bars=3)
    return h4, daily, config


@pytest.mark.parametrize("variant", [STRATEGY, BENCHMARK])
def test_evening_confirmation_daily_context_and_causality(variant):
    h4, daily, cfg = market()
    signal, = candidates(h4, daily, "TEST", config=cfg, variant=variant)
    assert signal["pattern_at"] == h4.index[-1].isoformat()
    assert signal["available_at"] == bar_end(h4.index[-1:], "4H")[0].isoformat()
    assert signal["daily_available_at"] == signal["available_at"]
    assert signal["stop_loss"] < signal["signal_close"] < signal["max_entry"] < signal["target"]
    cutoff = pd.Timestamp(signal["available_at"])
    assert candidates(h4, daily, "TEST", config=cfg, as_of=cutoff-pd.Timedelta(seconds=1)) == []
    later = h4.copy()
    later.loc[h4.index[-1]+pd.Timedelta(days=1)] = [1, 200, 1, 150]
    future_daily = daily.copy()
    future_daily.loc[daily.index[-1]+pd.Timedelta(days=1)] = [1, 200, 1, 150, 99_000_000]
    all_entries = candidates(later, future_daily, "TEST", config=cfg, variant=variant)
    assert [s for s in all_entries if s["available_at"] <= signal["available_at"]] == [signal]


def test_liquidity_is_required_and_not_assumed_from_ticker():
    h4, daily, cfg = market()
    with pytest.raises(ValueError, match="volume is required"):
        candidates(h4, daily.drop(columns="Volume"), "AAPL", config=cfg)
    daily.Volume = 20
    assert candidates(h4, daily, "AAPL", config=cfg) == []


def paper_fixture(symbol="TEST"):
    index = pd.to_datetime(["2026-01-09T18:30Z", "2026-01-12T14:30Z", "2026-01-12T18:30Z"])
    frame = pd.DataFrame([[100, 101, 99, 100], [100, 102, 99, 101], [90, 92, 88, 91]],
                         index=index, columns=["Open", "High", "Low", "Close"], dtype=float)
    cfg = SwingConfig()
    signal = {"id": symbol, "strategy": STRATEGY, "config_hash": cfg.fingerprint, "symbol": symbol,
              "status": "PENDING", "pattern_at": index[0].isoformat(),
              "available_at": "2026-01-09T21:00:00+00:00", "first_seen_at": "2026-01-09T21:10:00+00:00",
              "signal_close": 100., "stop_loss": 95., "target": 110., "max_entry": 101.,
              "source_candles": [{"at": index[0].isoformat(), "open":100., "high":101., "low":99., "close":100.}]}
    return frame, signal, cfg


def test_next_open_gap_fill_costs_and_cash_are_real_not_capped_at_one_r():
    frame, signal, cfg = paper_fixture()
    state = replay({"TEST": frame}, [signal], config=cfg)
    trade = state["journal"]["TEST"]
    assert trade["entry_at"] == frame.index[1].isoformat()
    assert trade["quantity"] == 10
    assert trade["exit_price"] == 90 and trade["execution"]["exit_gap"]
    assert trade["net_r"] == pytest.approx(-2.038)
    assert state["cash"] == pytest.approx(9898.1)
    assert metrics(state)["return_pct"] == pytest.approx(-1.019)


def test_no_fill_on_signal_bar_late_observation_or_unacceptable_gap():
    frame, signal, cfg = paper_fixture()
    assert replay({"TEST": frame.iloc[:1]}, [signal])["journal"]["TEST"]["status"] == "PENDING"
    late = {**signal, "first_seen_at":"2026-01-12T15:00:00Z"}
    assert replay({"TEST":frame}, [late])["journal"]["TEST"]["status"] == "MISSED"
    frame.iloc[1] = [105, 106, 104, 105]
    assert replay({"TEST":frame}, [signal])["journal"]["TEST"]["status"] == "SKIPPED_GAP"


def test_incremental_replay_equals_complete_replay_and_is_idempotent():
    frame, signal, cfg = paper_fixture()
    cutoff = "2026-01-13T00:00Z"
    prefix = replay({"TEST": frame.iloc[:2]}, [signal], as_of=cutoff)
    complete = replay({"TEST": frame}, [signal], as_of=cutoff)
    resumed = replay({"TEST": frame}, [signal], state=prefix, as_of=cutoff)
    assert resumed == complete
    assert replay({"TEST": frame}, [signal], state=resumed, as_of=cutoff) == resumed
    with pytest.raises(ValueError, match="different rule version"):
        replay({"TEST":frame}, [], state=prefix, config=replace(cfg, target_r=3))
    revised = frame.copy()
    revised.iloc[0, 3] = 100.1
    with pytest.raises(ValueError, match="source revised"):
        replay({"TEST":revised}, [], state=prefix)
    revised = frame.copy()
    revised.iloc[1, 1] += 1
    with pytest.raises(ValueError, match="price path revised"):
        replay({"TEST":revised}, [], state=prefix)


def test_portfolio_capacity_priority_and_no_spending_future_intrabar_proceeds():
    frame, one, cfg = paper_fixture("AAA")
    _, two, _ = paper_fixture("BBB")
    capped = replace(cfg, max_positions=1)
    state = replay({"AAA":frame, "BBB":frame}, [two, one], config=capped)
    assert state["journal"]["AAA"]["status"] == "CLOSED"
    assert state["journal"]["BBB"]["status"] == "SKIPPED_CAPACITY"
    assert state["cash"] >= 0


def test_ambiguous_entry_bar_uses_stop_and_time_exit_is_finite():
    frame, signal, cfg = paper_fixture()
    frame.iloc[1] = [100, 112, 94, 101]
    closed = replay({"TEST":frame.iloc[:2]}, [signal])["journal"]["TEST"]
    assert closed["exit_price"] == 95 and closed["execution"]["ambiguous_exit_bar"]
    frame.iloc[1] = [100, 102, 99, 101]
    timed = replay({"TEST":frame.iloc[:2]}, [signal], config=replace(cfg, max_hold_bars=1))["journal"]["TEST"]
    assert timed["exit_reason"] == "TIME" and timed["exit_price"] == 101


def test_partial_week_is_excluded_and_completed_flat_week_is_not_positive():
    frame, signal, cfg = paper_fixture()
    state = replay({"TEST":frame.iloc[:1]}, [], as_of="2026-01-09T21:00Z")
    stats = metrics(state)
    assert stats["completed_weeks"] == 1
    assert stats["positive_weeks_pct"] == 0
    state = replay({"TEST":frame.iloc[:2]}, [signal], as_of="2026-01-12T18:00Z")
    assert metrics(state)["completed_weeks"] == 1


def test_future_observations_and_clock_rollback_are_rejected():
    frame, signal, cfg = paper_fixture()
    with pytest.raises(ValueError, match="Future observations"):
        replay({"TEST":frame}, [signal], as_of="2026-01-09T20:00Z")
    state = replay({"TEST":frame}, [signal], as_of="2026-01-12T22:00Z")
    with pytest.raises(ValueError, match="clock cannot"):
        replay({"TEST":frame}, [], state=state, as_of="2026-01-12T21:59Z")


@pytest.mark.parametrize("change", [{"max_positions":0}, {"risk_fraction":-1},
                                   {"fee_bps":-1}, {"daily_sma":1.5}, {"target_r":1}])
def test_invalid_configuration_is_rejected(change):
    with pytest.raises(ValueError):
        replace(SwingConfig(), **change)
