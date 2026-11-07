import json

import pandas as pd
import pytest

from devils_sweep_cisd_research import (
    STRATEGY, compare_symbol, evaluate_entry, partition, run_research, summarize,
)
from tests.test_devils_sweep_cisd import fixture


def market():
    h4, _ = fixture(count=17)
    idx = pd.bdate_range("2024-12-20", "2025-02-01", tz="America/New_York")
    daily = pd.DataFrame({"Open":102.,"High":108.,"Low":100.,"Close":101.}, index=idx)
    return h4, daily


def test_opening_gap_and_costs_use_the_same_frozen_trade_levels():
    df = pd.DataFrame({"Open":[100,95],"High":[101,108],"Low":[99,94],"Close":[100,100]},
                      index=pd.to_datetime(["2025-01-02T14:30Z","2025-01-02T18:30Z"]))
    signal = {"ticker":"TEST","direction":"BULLISH","pattern_at":df.index[0].isoformat(),
              "entry_price":100,"stop_loss":98,"target":106,"rr_ratio":3}
    result = evaluate_entry(signal, df)
    assert result["r_multiple"] == -2.5
    assert result["execution"]["exit_price"] == 95
    assert result["cost_r"] == pytest.approx((100+95)*0.0005/2)
    assert result["net_r"] == pytest.approx(-2.54875)
    assert signal["entry_price"] == 100 and "execution" not in signal


def test_no_exit_before_the_signal_candle_has_closed():
    df = pd.DataFrame({"Open":[100,100],"High":[110,101],"Low":[90,99],"Close":[100,100]},
                      index=pd.to_datetime(["2025-01-02T14:30Z","2025-01-02T18:30Z"]))
    signal = {"ticker":"TEST","direction":"BULLISH","pattern_at":df.index[0].isoformat(),
              "entry_price":100,"stop_loss":98,"target":106,"rr_ratio":3}
    result = evaluate_entry(signal, df)
    assert result["status"] == "ACTIVE" and result["net_r"] is None


def test_all_variants_share_data_and_ablation_only_removes_marker_gate():
    h4, daily = market()
    saved = h4.copy(deep=True)
    result = compare_symbol("TEST", h4, daily)
    assert set(result["trades"]) == {"pure_no_wick","sweep_cisd",STRATEGY}
    full, = result["trades"][STRATEGY]
    plain, = result["trades"]["sweep_cisd"]
    assert full["pattern_at"] == plain["pattern_at"]
    assert full["entry_price"] == plain["entry_price"]
    assert full["stop_loss"] == plain["stop_loss"] and full["target"] == plain["target"]
    assert full["require_marker"] and not plain["require_marker"]
    pd.testing.assert_frame_equal(h4, saved)


def test_split_excludes_exits_whose_bar_was_still_forming():
    sample = {"ticker":"TEST","pattern_at":"2025-01-02T14:30:00Z",
              "signal_available_at":"2025-01-02T18:30:00Z","closed_at":"2025-01-03T14:30:00Z",
              "net_r":1.,"r_multiple":1.,"cost_r":0.,"sized_net_r":1.,
              "execution":{"ambiguous_exit_bar":False}}
    held = {**sample,"signal_available_at":"2025-01-03T16:00:00Z"}
    out = partition([sample, held], "2025-01-03T16:00:00Z")
    assert out["before_split"]["entries"] == 0
    assert out["crossing_split"]["entries"] == 1
    assert out["held_out"]["entries"] == 1


def test_drawdown_uses_exit_order_and_open_trades_are_not_wins():
    trades = [{"ticker":"TEST","pattern_at":"2025-01-01T00:00:00Z",
               "closed_at":f"2025-01-0{i+2}T14:30:00Z", "net_r":r,"r_multiple":r,
               "cost_r":0.,"sized_net_r":r,"execution":{"ambiguous_exit_bar":False}}
              for i,r in enumerate([1.,-2.,0.5])]
    trades.reverse()
    trades.append({"ticker":"TEST","net_r":None})
    result = summarize(trades)
    assert result["closed"] == 3 and result["open"] == 1
    assert result["net_r"] == -0.5 and result["max_drawdown_signal_r"] == -2
    assert result["profit_factor_net"] == 0.75


def test_report_registers_fixed_rules_and_never_promotes_small_samples(tmp_path):
    h4,daily = market()
    def fetch(symbol, *_args, **_kw):
        if symbol == "BAD": raise ValueError("missing market data")
        return h4,daily
    output = tmp_path/"report.json"
    result = run_research(["TEST","BAD"], output=output, workers=1, fetch=fetch)
    assert "BAD" in result["errors"]
    assert not result["evidence"]["sample_sufficient"]
    assert not result["evidence"]["automatically_promoted"]
    saved = json.loads(output.read_text())
    registered = json.loads(output.with_suffix(".registration.json").read_text())
    assert saved["config_hash"] == registered["config_hash"]
    assert registered["parameter_search"] is False
    with pytest.raises(ValueError, match="immutable"):
        run_research(["TEST"], output=output, fetch=fetch)


@pytest.mark.parametrize("cost", [-1,float("nan"),float("inf")])
def test_invalid_costs_fail_before_data_download(tmp_path,cost):
    with pytest.raises(ValueError, match="costs"):
        run_research(["TEST"], output=tmp_path/"report.json",fee_bps=cost,
                     fetch=lambda *a, **kw: pytest.fail("should not fetch"))
