"""Regression coverage for gap fills, opening chronology and historical repair."""
import pandas as pd
import pytest

import backfill_r_multiple as bf
import devils_outcome_resolver as res
from strategy import devils_cisd_v1 as v1
from strategy.devils_markers import resolve_trade_after_marker
from strategy.gap_fill import RESOLVER_VERSION, exit_on_bar


def _df(bars, start="2025-01-02"):
    idx = pd.date_range(start, periods=len(bars), freq="D")
    return pd.DataFrame(bars, columns=["Open", "High", "Low", "Close"], index=idx)


def _sig(direction, entry, stop, target, rr, at="2025-01-01"):
    return {"direction": direction, "entry_price": entry, "stop_loss": stop, "target": target,
            "rr_ratio": rr, "pattern_at": at, "meta": {"ctx": {"a": 1}}}


def test_gap_stop_long():
    # entry 100, stop 98 (risk 2), bar opens 95 -> (95-100)/2 = -2.5
    out = res.resolve_outcome(_sig("BULLISH", 100, 98, 106, 3.0), _df([(95, 96, 94, 95.5)]))
    assert out["status"] == "CLOSED_LOSS" and out["r_multiple"] == pytest.approx(-2.5)
    assert out["meta"]["resolver_version"] == RESOLVER_VERSION and out["meta"]["ctx"] == {"a": 1}


def test_gap_stop_short():
    out = res.resolve_outcome(_sig("BEARISH", 100, 102, 94, 3.0), _df([(103, 104, 102.5, 103)]))
    assert out["status"] == "CLOSED_LOSS" and out["r_multiple"] == pytest.approx(-1.5)


@pytest.mark.parametrize("direction,entry,stop,target,bar,r", [
    ("BULLISH", 100, 98, 106, (108, 109, 107, 108), 4.0),
    ("BEARISH", 100, 102, 94, (93, 93.5, 92, 93), 3.5),
])
def test_gap_target_fills_at_open(direction, entry, stop, target, bar, r):
    out = res.resolve_outcome(_sig(direction, entry, stop, target, 3.0), _df([bar]))
    assert out["status"] == "CLOSED_WIN" and out["r_multiple"] == pytest.approx(r)


@pytest.mark.parametrize("direction,entry,stop,target,bar", [
    ("BULLISH", 100, 98, 106, (99, 100, 97, 98)),
    ("BEARISH", 100, 102, 94, (101, 103, 100, 102)),
    ("BULLISH", 100, 98, 106, (98, 99, 97, 98)),  # opens exactly at the stop
])
def test_normal_stop_is_exactly_minus_one(direction, entry, stop, target, bar):
    out = res.resolve_outcome(_sig(direction, entry, stop, target, 3.0), _df([bar]))
    assert out["status"] == "CLOSED_LOSS" and out["r_multiple"] == -1.0


def test_normal_target_is_planned_r():
    out = res.resolve_outcome(_sig("BULLISH", 100, 98, 106, 3.0), _df([(101, 106.5, 100.5, 106)]))
    assert out["status"] == "CLOSED_WIN" and out["r_multiple"] == 3.0


@pytest.mark.parametrize("bar,r", [((100.5, 107, 97, 99), -1.0), ((97, 107, 96, 99), -1.5)])
def test_same_bar_sl_tp_is_loss(bar, r):
    out = res.resolve_outcome(_sig("BULLISH", 100, 98, 106, 3.0), _df([bar]))
    assert out["status"] == "CLOSED_LOSS" and out["r_multiple"] == pytest.approx(r)


@pytest.mark.parametrize("long,stop,target,bar,r", [
    (True, 98, 106, (107, 108, 97), 3.5),
    (False, 102, 94, (93, 103, 92), 3.5),
    (True, 98, 106, (106, 108, 97), 3.0),
    (False, 102, 94, (94, 103, 92), 3.0),
])
def test_target_filled_at_open_precedes_later_stop(long, stop, target, bar, r):
    assert exit_on_bar(*bar, long=long, entry=100, stop=stop, target=target, planned_r=3.0)[:2] == ("CLOSED_WIN", r)


def test_markers_resolver_gap_stop():
    df = _df([(100, 101, 99, 100), (95, 96, 94, 95)])
    assert resolve_trade_after_marker(df, 0, "BULLISH", 98, 106, entry=100, planned_r=3.0) == ("CLOSED_LOSS", -2.5)


@pytest.mark.parametrize("bar,stop,target", [
    ((100, float("nan"), 97), 98, 106),
    ((100, 99, 97), 98, 106),
    ((100, 101, 99), 100, 106),
    ((100, 101, 99), 98, 97),
])
def test_invalid_prices_cannot_silently_advance_the_simulation(bar, stop, target):
    with pytest.raises(ValueError):
        exit_on_bar(*bar, long=True, entry=100, stop=stop, target=target)


def test_cisd_v1_simulate_gap_stop():
    O = [100, 95]; H = [101, 96]; L = [99, 94]; C = [100, 95]
    assert v1._simulate(H, L, C, 0, 1, 100.0, 98.0, O) == ("CLOSED_LOSS", 1, -2.5)
    assert v1._simulate(H, L, C, 0, 1, 100.0, 98.0) == ("CLOSED_LOSS", 1, -1.0)  # legacy path


class _FakeQ:
    def __init__(self, rows, log):
        self.rows, self.log, self._upd = rows, log, None
    def select(self, *_): return self
    def eq(self, *_a): return self
    def in_(self, *_a): return self
    def order(self, *_a): return self
    def range(self, a, b): self._r = (a, b); return self
    def update(self, payload): self._upd = payload; return self
    def execute(self):
        if self._upd is not None:
            self.log.append(self._upd); return type("R", (), {"data": []})()
        a, b = self._r
        return type("R", (), {"data": self.rows[a:b + 1]})()


class _FakeSB:
    def __init__(self, rows): self.rows, self.log = rows, []
    def table(self, _): return _FakeQ(self.rows, self.log)


def test_backfill_idempotent_and_dry_run():
    df = _df([(100, 101, 99, 100), (95, 96, 94, 95)], start="2025-01-01")
    base = {"symbol": "TST", "timeframe": "1D", "direction": "BULLISH", "entry_price": 100,
            "stop_loss": 98, "target": 106, "rr_ratio": 3.0, "pattern_at": "2025-01-01T00:00:00+00:00",
            "status": "CLOSED_LOSS", "r_multiple": -1.0}
    rows = [{**base, "id": 1, "meta": {"ctx": {}}},
            {**base, "id": 2, "meta": {"resolver_version": RESOLVER_VERSION}}]
    sb = _FakeSB(rows)
    st = bf.run(sb, dry_run=True, limit=10, batch=10, fetch=lambda s: {"1D": df}, new_client=lambda: sb)
    assert st["rows"] == 1 and st["changed"] == 1 and sb.log == []
    st = bf.run(sb, dry_run=False, limit=10, batch=10, fetch=lambda s: {"1D": df}, new_client=lambda: sb)
    assert len(sb.log) == 1
    update = sb.log[0]
    assert update["r_multiple"] == -2.5 and update["closed_at"].startswith("2025-01-02")
    assert update["meta"]["ctx"] == {} and update["meta"]["resolver_version"] == RESOLVER_VERSION
    assert update["meta"]["execution"]["exit_price"] == 95


def test_backfill_label_repair_requires_explicit_flag():
    df = _df([(100, 101, 99, 100), (107, 108, 97, 99)], start="2025-01-01")
    row = {"id": 1, "symbol": "TST", "timeframe": "1D", "direction": "BULLISH",
           "entry_price": 100, "stop_loss": 98, "target": 106, "rr_ratio": 3,
           "pattern_at": "2025-01-01", "status": "CLOSED_LOSS", "r_multiple": -1,
           "meta": {"ctx": {"saved": True}, "outcome": {"outcome_version": 1}}}
    sb = _FakeSB([row])
    args = dict(dry_run=False, limit=10, batch=10, fetch=lambda s: {"1D": df}, new_client=lambda: sb)
    assert bf.run(sb, **args)["mismatch"] == 1 and sb.log == []
    assert bf.run(sb, repair_outcomes=True, **args)["written"] == 1
    update = sb.log[0]
    assert (update["status"], update["result"], update["r_multiple"]) == ("CLOSED_WIN", "WIN", 3.5)
    assert update["meta"]["ctx"] == {"saved": True}
    assert update["meta"]["execution"]["ambiguous_exit_bar"] is False
    assert update["meta"]["outcome"]["mae_r_capped"] == 0
