"""Patch 0020: gap fill at the open (resolver_version 2)."""
import pandas as pd
import pytest

import backfill_r_multiple as bf
import devils_outcome_resolver as res
from strategy import devils_cisd_v1 as v1
from strategy.devils_markers import resolve_trade_after_marker
from strategy.gap_fill import exit_on_bar


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
    assert out["meta"]["resolver_version"] == 2 and out["meta"]["ctx"] == {"a": 1}


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


def test_same_bar_opening_beyond_target_but_touching_stop_stays_loss():
    # conservative rule kept even when the open is past the target
    assert exit_on_bar(107, 108, 97, long=True, entry=100, stop=98, target=106, planned_r=3.0)[:2] == ("CLOSED_LOSS", -1.0)


def test_markers_resolver_gap_stop():
    df = _df([(100, 101, 99, 100), (95, 96, 94, 95)])
    assert resolve_trade_after_marker(df, 0, "BULLISH", 98, 106, entry=100, planned_r=3.0) == ("CLOSED_LOSS", -2.5)


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
    rows = [{**base, "id": 1, "meta": {"ctx": {}}}, {**base, "id": 2, "meta": {"resolver_version": 2}}]
    sb = _FakeSB(rows)
    st = bf.run(sb, dry_run=True, limit=10, batch=10, fetch=lambda s: {"1D": df}, new_client=lambda: sb)
    assert st["rows"] == 1 and st["changed"] == 1 and sb.log == []
    st = bf.run(sb, dry_run=False, limit=10, batch=10, fetch=lambda s: {"1D": df}, new_client=lambda: sb)
    assert sb.log == [{"r_multiple": -2.5, "meta": {"ctx": {}, "resolver_version": 2}}]
