"""Regression tests: Devil's Markers outcome resolution (survivorship bias, same-bar, symmetry).
Offline: synthetic candles only, no network / Supabase."""
from __future__ import annotations

import pandas as pd
import pytest

import devils_outcome_resolver as res
import strategy.devils_markers as dm


def _df(rows, start="2025-01-01", freq="1D"):
    return pd.DataFrame(rows, index=pd.date_range(start, periods=len(rows), freq=freq))


def _signal(direction, entry, stop, target, rr, at="2025-01-01"):
    return {"direction": direction, "entry_price": entry, "stop_loss": stop,
            "target": target, "rr_ratio": rr, "pattern_at": at}


def _bar(o, h, l, c):
    return {"Open": o, "High": h, "Low": l, "Close": c, "Volume": 1_000_000}


def _series_with_mark(direction, stop_hit_after):
    """25 trend bars, then a mark bar, then one follow-up bar (optionally crossing the mark)."""
    rows = []
    for i in range(25):
        px = 100 - i * 0.05 if direction == "BULLISH" else 100 + i * 0.05
        rows.append(_bar(px, px + 0.8, px - 0.8, px + 0.1))
    if direction == "BULLISH":
        rows.append(_bar(95.0, 96.2, 95.0, 96.0))           # open == low
        rows.append(_bar(95.5, 96.0, 94.0, 94.5) if stop_hit_after else _bar(96.0, 96.5, 95.6, 96.3))
    else:
        rows.append(_bar(105.0, 105.0, 103.8, 104.0))        # open == high
        rows.append(_bar(104.5, 106.0, 104.0, 105.5) if stop_hit_after else _bar(104.0, 104.4, 103.5, 103.7))
    return _df(rows)


# ---------- 1) survivorship bias ----------
@pytest.mark.parametrize("direction", ["BULLISH", "BEARISH"])
def test_stop_hit_mark_is_not_silently_dropped(direction):
    """A mark whose stop was hit must be emitted (as CLOSED_LOSS / -1R or resolvable to a loss),
    not discarded — otherwise losses never reach the stats."""
    df = _series_with_mark(direction, stop_hit_after=True)
    sigs = dm.find_devils_markers(df, ticker="TST", timeframe="1D")
    mark = [s for s in sigs if s["direction"] == direction]
    assert mark, "stop-hit mark dropped by scanner default (survivorship bias)"
    s = mark[0]
    if s["status"] == "CLOSED_LOSS":
        return
    out = res.resolve_outcome({**s, "symbol": "TST"}, df)
    assert out and out["status"] == "CLOSED_LOSS" and out["r_multiple"] == -1.0


@pytest.mark.parametrize("direction", ["BULLISH", "BEARISH"])
def test_open_mark_still_detected(direction):
    df = _series_with_mark(direction, stop_hit_after=False)
    sigs = dm.find_devils_markers(df, ticker="TST", timeframe="1D")
    assert any(s["direction"] == direction and s["status"] == "ACTIVE" for s in sigs)


@pytest.mark.parametrize("direction,stop,target,bar", [
    ("BULLISH", 95.0, 100.0, _bar(96, 96.5, 94.5, 95.0)),
    ("BEARISH", 105.0, 100.0, _bar(104, 105.5, 103.5, 105.0)),
])
def test_saved_signal_crossing_stop_resolves_loss(direction, stop, target, bar):
    df = _df([bar], start="2025-01-02")
    out = res.resolve_outcome(_signal(direction, 96.0 if direction == "BULLISH" else 104.0, stop, target, 4.0), df)
    assert out["status"] == "CLOSED_LOSS" and out["result"] == "LOSS" and out["r_multiple"] == -1.0
    assert out["is_active"] is False


# ---------- 2) target first -> win ; 4) symmetry ----------
@pytest.mark.parametrize("direction,entry,stop,target,bars", [
    ("BULLISH", 96.0, 95.0, 100.0, [_bar(96, 98, 95.5, 97.5), _bar(97.5, 100.5, 97, 100), _bar(100, 100, 90, 91)]),
    ("BEARISH", 104.0, 105.0, 100.0, [_bar(104, 104.5, 102, 102.5), _bar(102.5, 103, 99.5, 100), _bar(100, 110, 100, 109)]),
])
def test_target_before_stop_is_win(direction, entry, stop, target, bars):
    out = res.resolve_outcome(_signal(direction, entry, stop, target, 4.0), _df(bars, start="2025-01-02"))
    assert out["status"] == "CLOSED_WIN" and out["exit_reason"] == "TP_HIT"
    assert out["r_multiple"] == pytest.approx(4.0)


def test_unresolved_signal_stays_open():
    bars = [_bar(96, 97, 95.5, 96.5)]
    assert res.resolve_outcome(_signal("BULLISH", 96.0, 95.0, 100.0, 4.0), _df(bars, start="2025-01-02")) is None


# ---------- 3) stop and target inside the same weekly / monthly candle ----------
@pytest.mark.parametrize("freq", ["W-FRI", "MS"])
@pytest.mark.parametrize("direction", ["BULLISH", "BEARISH"])
def test_same_htf_candle_is_conservative_loss(freq, direction):
    bar = _bar(100, 106, 94, 100)
    entry, stop, target = (96.0, 95.0, 100.0) if direction == "BULLISH" else (104.0, 105.0, 100.0)
    out = res.resolve_outcome(_signal(direction, entry, stop, target, 4.0, at="2024-12-01"),
                              _df([bar], start="2025-01-03", freq=freq))
    assert out["status"] == "CLOSED_LOSS" and out["r_multiple"] == -1.0


@pytest.mark.parametrize("tp_first", [True, False])
def test_same_weekly_candle_daily_drilldown(tp_first):
    """If the resolver supports a daily drill-down, daily order must decide the outcome."""
    drill = next((getattr(res, n) for n in ("resolve_outcome_with_daily", "resolve_with_drilldown")
                  if hasattr(res, n)), None)
    if drill is None:
        pytest.skip("resolver has no daily drill-down (conservative SL-first only)")
    tp, sl = _bar(97, 100.5, 96.5, 100), _bar(96, 96.2, 94.5, 95)
    daily = _df([tp, sl] if tp_first else [sl, tp], start="2025-01-06")
    weekly = _df([_bar(97, 100.5, 94.5, 98)], start="2025-01-10", freq="W-FRI")
    out = drill(_signal("BULLISH", 96.0, 95.0, 100.0, 4.0, at="2025-01-03"), weekly, daily)
    assert out["status"] == ("CLOSED_WIN" if tp_first else "CLOSED_LOSS")


# ---------- 5) open==high tolerance sensitivity (informational) ----------
def _near_marks(gaps):
    rows = []
    for g in gaps:  # open sits g*price below high
        o = 100.0
        rows.append(_bar(o, o * (1 + g), o - 2, o - 1))
    return rows


@pytest.mark.parametrize("tol,expected", [(1e-6, 1), (1e-4, 3), (1e-3, 5)])
def test_tolerance_sensitivity(monkeypatch, tol, expected):
    gaps = [0.0, 5e-5, 9e-5, 5e-4, 9e-4, 5e-3]
    monkeypatch.setattr(dm, "PRICE_EQ_TOL", tol)
    n = sum(dm._is_open_eq_high(r["Open"], r["High"]) for r in _near_marks(gaps))
    print(f"tol={tol}: {n}/{len(gaps)} open==high matches")
    assert n == expected
