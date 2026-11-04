"""signal_outcome: mae_r/mfe_r on closed trades, kept out of meta.ctx, meta preserved."""
from __future__ import annotations

import pandas as pd
import pytest

import backfill_outcome
from devils_scanner import signal_to_row
from signal_outcome import OUTCOME_VERSION, attach_outcome, compute_trade_outcome

IDX = pd.bdate_range("2025-01-02", periods=8, tz="America/New_York")


def _df(bars):
    """bars: list of (open, high, low, close); first bar = pattern bar."""
    return pd.DataFrame([dict(Open=o, High=h, Low=l, Close=c, Volume=1e6) for o, h, l, c in bars],
                        index=IDX[: len(bars)])


def _sig(status, direction="BULLISH", entry=100.0, stop=98.0, target=104.0):
    return {"ticker": "TEST", "symbol": "TEST", "timeframe": "1D", "direction": direction,
            "entry_price": entry, "stop_loss": stop, "target": target, "rr_ratio": 2.0,
            "devils_marker_price": stop, "status": status, "pattern_at": IDX[0].isoformat(),
            "pattern_unix": 0, "is_high_rr": True}


LONG_LOSS = _df([(98, 100.5, 98, 100), (100, 102, 99.5, 101), (101, 101.5, 97.9, 98.5)])
LONG_GAP_LOSS = _df([(98, 100.5, 98, 100), (100, 103, 99, 102), (96, 96.5, 95, 95.5)])
LONG_WIN = _df([(98, 100.5, 98, 100), (100, 101, 99, 100.5), (101, 104.6, 100.5, 104)])
SHORT_WIN = _df([(102, 102, 99.5, 100), (100, 101, 98, 99), (99, 99.5, 95.5, 96)])


def test_loss_mae_is_minus_one():
    oc = compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": LONG_LOSS})
    assert oc["mae_r"] <= -1 and oc["mae_r"] == pytest.approx(-1.05)
    assert oc["mfe_r"] == pytest.approx(1.0) and oc["bars_held"] == 2
    assert oc["outcome_version"] == OUTCOME_VERSION


def test_gap_loss_mae_below_minus_one():
    oc = compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": LONG_GAP_LOSS})
    assert oc["mae_r"] == pytest.approx(-2.5)  # gross, not capped at the stop
    assert oc["mfe_r"] == pytest.approx(1.5)


def test_win_mfe_reaches_target():
    oc = compute_trade_outcome(_sig("CLOSED_WIN"), {"1D": LONG_WIN})
    assert oc["mfe_r"] >= 2.0 and oc["mfe_r"] == pytest.approx(2.3)
    assert oc["mae_r"] == pytest.approx(-0.5)


def test_short_win():
    s = _sig("CLOSED_WIN", "BEARISH", entry=100, stop=102, target=96)
    oc = compute_trade_outcome(s, {"1D": SHORT_WIN})
    assert oc["mfe_r"] >= 2.0 and oc["mae_r"] == pytest.approx(-0.5) and oc["bars_held"] == 2


def test_bars_after_close_ignored():
    extra = pd.concat([LONG_LOSS, _df([(0, 0, 0, 0)] * 4).iloc[3:4].set_axis(IDX[3:4]).assign(High=200.0, Low=50.0)])
    assert compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": extra}) == \
        compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": LONG_LOSS})


def test_none_for_open_mismatch_or_missing():
    assert compute_trade_outcome(_sig("ACTIVE"), {"1D": LONG_LOSS}) is None
    assert compute_trade_outcome(_sig("CLOSED_WIN"), {"1D": LONG_LOSS}) is None  # bars say loss
    assert compute_trade_outcome(_sig("CLOSED_LOSS"), {}) is None
    assert compute_trade_outcome({"status": "CLOSED_LOSS"}, {"1D": LONG_LOSS}) is None  # never raises


def test_no_influence_on_ctx(monkeypatch):
    import devils_scanner
    monkeypatch.setattr(devils_scanner, "compute_signal_context", lambda s, f: {"x": 1})
    s = _sig("CLOSED_LOSS")
    row = signal_to_row(s, {"1D": LONG_LOSS})
    assert row["meta"]["ctx"] == {"x": 1} and "mae_r" not in row["meta"]["ctx"]
    assert row["meta"]["outcome"]["mae_r"] == pytest.approx(-1.05)
    # active signals never get an outcome; ctx identical with/without the outcome step
    act = signal_to_row(_sig("ACTIVE"), {"1D": LONG_LOSS})
    assert "outcome" not in act["meta"] and act["meta"]["ctx"] == row["meta"]["ctx"]


def test_attach_preserves_meta():
    meta = {"ctx": {"a": 1}, "ctx_version": 1, "pattern_unix": 5, "other": [1]}
    out = attach_outcome(meta, {"mae_r": -1.0, "mfe_r": 0.2, "outcome_version": 1, "bars_held": 3})
    assert {k: out[k] for k in meta} == meta and out["outcome"]["bars_held"] == 3
    assert "outcome" not in meta  # input not mutated


class _Q:
    def __init__(self, db, log):
        self.db, self.log, self._upd, self._range, self._id = db, log, None, None, None

    def select(self, *_):
        return self

    def eq(self, k, v):
        if k == "id":
            self._id = v
        return self

    def in_(self, *_):
        return self

    def order(self, *_):
        return self

    def range(self, a, b):
        self._range = (a, b)
        return self

    def update(self, d):
        self._upd = d
        return self

    def execute(self):
        if self.log.pop("fail", 0):
            raise ConnectionError("RemoteProtocolError")
        if self._upd is not None:
            self.log.setdefault("writes", []).append((self._id, self._upd))
            return type("R", (), {"data": []})()
        a, b = self._range
        return type("R", (), {"data": self.db[a:b + 1]})()


class _SB:
    def __init__(self, db, log):
        self.db, self.log = db, log

    def table(self, _):
        return _Q(self.db, self.log)


def _db():
    r1 = {**_sig("CLOSED_LOSS"), "id": 1, "meta": {"ctx": {"a": 1}, "ctx_version": 1}}
    r2 = {**_sig("CLOSED_LOSS"), "id": 2,
          "meta": {"outcome": {"outcome_version": 1, "mae_r": -1, "mfe_r": 0, "bars_held": 1}}}
    return [r1, r2]


def test_backfill_idempotent_preserves_meta_and_retries(monkeypatch):
    monkeypatch.setattr(backfill_outcome.time, "sleep", lambda s: None)
    log = {}
    db = _db()
    sb = _SB(db, log)
    st = backfill_outcome.run(sb, dry_run=False, limit=10, batch=10, fetch=lambda s: {"1D": LONG_LOSS},
                              new_client=lambda: (log.__setitem__("reconnects", log.get("reconnects", 0) + 1), sb)[1])
    assert st["rows"] == 1 and st["written"] == 1  # row 2 skipped (outcome_version 1)
    (rid, upd), = log["writes"]
    assert rid == 1 and upd["meta"]["ctx"] == {"a": 1} and upd["meta"]["ctx_version"] == 1
    assert upd["meta"]["outcome"]["mae_r"] == pytest.approx(-1.05)
    # dropped connection on the next write -> reconnect + retry
    log.clear()
    log["fail"] = 1
    db[0]["meta"] = {"ctx": {"a": 1}}
    st = backfill_outcome.run(sb, dry_run=False, limit=10, batch=10, fetch=lambda s: {"1D": LONG_LOSS},
                              new_client=lambda: (log.__setitem__("reconnects", 1), sb)[1])
    assert log.get("reconnects") == 1 and st["written"] == 1


def test_backfill_dry_run_writes_nothing():
    log = {}
    st = backfill_outcome.run(_SB(_db(), log), dry_run=True, limit=10, batch=10,
                              fetch=lambda s: {"1D": LONG_LOSS}, new_client=lambda: None)
    assert st["with_outcome"] == 1 and "writes" not in log


def test_capped_and_pre_exit_normal_loss():
    oc = compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": LONG_LOSS})
    assert oc["mae_r_capped"] == pytest.approx(-1.0)  # opened above the stop -> capped at -1
    assert oc["mfe_r_pre_exit"] == pytest.approx(1.0) and oc["ambiguous_exit_bar"] is False


def test_capped_gap_uses_open():
    oc = compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": LONG_GAP_LOSS})
    assert oc["mae_r_capped"] == pytest.approx(-2.0)  # open 96, entry 100, R 2
    assert oc["mae_r"] == pytest.approx(-2.5) and oc["mfe_r_pre_exit"] == pytest.approx(1.5)


def test_short_gap_loss():
    df = _df([(102, 102, 99.5, 100), (103, 104, 102.5, 103.5)])
    oc = compute_trade_outcome(_sig("CLOSED_LOSS", "BEARISH", entry=100, stop=102, target=96), {"1D": df})
    assert oc["mae_r_capped"] == pytest.approx(-1.5) and oc["mae_r"] == pytest.approx(-2.0)


def test_win_capped_equals_mae():
    oc = compute_trade_outcome(_sig("CLOSED_WIN"), {"1D": LONG_WIN})
    assert oc["mae_r_capped"] == oc["mae_r"] and oc["mfe_r_pre_exit"] == pytest.approx(0.5)


def test_first_bar_exit_pre_exit_zero():
    df = _df([(98, 100.5, 98, 100), (100, 103, 97, 98)])
    oc = compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": df})
    assert oc["bars_held"] == 1 and oc["mfe_r_pre_exit"] == 0.0 and oc["mfe_r"] == pytest.approx(1.5)


def test_ambiguous_exit_bar():
    df = _df([(98, 100.5, 98, 100), (100, 101, 99, 100.5), (100, 105, 97, 101)])
    oc = compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": df})  # SL and TP same bar -> LOSS
    assert oc["ambiguous_exit_bar"] is True and oc["mae_r_capped"] == pytest.approx(-1.0)
    assert oc["mfe_r"] == pytest.approx(2.5) and oc["mfe_r_pre_exit"] == pytest.approx(0.5)
    assert compute_trade_outcome(_sig("CLOSED_LOSS"), {"1D": LONG_LOSS})["ambiguous_exit_bar"] is False
