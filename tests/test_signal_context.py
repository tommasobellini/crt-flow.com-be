"""signal_context: fields, no-lookahead, failure safety, row integration."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import backfill_context
from devils_scanner import signal_to_row
from market_data import resample_to_4h, resample_to_monthly, resample_to_weekly
from signal_context import CTX_VERSION, MarketCache, bar_end, compute_signal_context

NY = "America/New_York"


def _daily(n=400, start="2023-01-02", seed=0, base=100.0):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range(start, periods=n, tz=NY)
    c = base * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    o = np.r_[c[0], c[:-1]] * (1 + rng.normal(0, 0.003, n))
    return pd.DataFrame({"Open": o, "High": np.maximum(o, c) * 1.01, "Low": np.minimum(o, c) * 0.99,
                         "Close": c, "Volume": rng.integers(1e6, 2e6, n).astype(float)}, index=idx)


def _hourly(daily):
    rows, idx = [], []
    for d, r in daily.iterrows():
        for k, hm in enumerate(["09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:30"]):
            ts = pd.Timestamp(f"{d.date()} {hm}", tz=NY)
            px = r.Open + (r.Close - r.Open) * (k + 1) / 7
            rows.append({"Open": r.Open if k == 0 else px * 0.999, "High": px * 1.002, "Low": px * 0.998,
                         "Close": px, "Volume": r.Volume / 7})
            idx.append(ts)
    return pd.DataFrame(rows, index=pd.DatetimeIndex(idx))


def _frames(daily, now):
    return {"1D": daily, "1W": resample_to_weekly(daily, now=now), "1M": resample_to_monthly(daily, now=now),
            "4H": resample_to_4h(_hourly(daily), now=now)}


def _market(spy, spy_h, vix, earn=None):
    data = {("SPY", "1d"): spy, ("SPY", "1h"): spy_h, ("^VIX", "1d"): vix}
    return MarketCache(history=lambda s, p, i: data.get((s, i)), info=lambda s: {"sector": "Technology"},
                       earnings=lambda s: earn)


def _signal(df, pos, tf):
    r = df.iloc[pos]
    return {"ticker": "TEST", "timeframe": tf, "direction": "BULLISH", "devils_marker_price": float(r.Open),
            "entry_price": float(r.Open) * 1.001, "stop_loss": float(r.Low) * 0.995,
            "target": float(r.High) * 1.03,
            "pattern_at": pd.Timestamp(df.index[pos]).isoformat()}


FULL = _daily(500)
SPY = _daily(500, seed=1, base=400)
VIX = _daily(500, seed=2, base=18)
CUT = FULL.index[350]  # pattern day
EARN = pd.DatetimeIndex([FULL.index[100].normalize(), FULL.index[353].normalize()])


def _ctx_for(tf, data_end):
    """Context of the signal on CUT computed with data available up to data_end."""
    now = pd.Timestamp(data_end) + pd.Timedelta(hours=17)
    d, s, v = FULL.loc[:data_end], SPY.loc[:data_end], VIX.loc[:data_end]
    fr = _frames(d, now)
    df = fr[tf]
    if tf == "4H":
        pos = int(np.flatnonzero(pd.DatetimeIndex(df.index).normalize() <= CUT.tz_convert("UTC").tz_localize(None).normalize())[-1])
    else:
        pos = int(np.flatnonzero(bar_end(df.index, tf) <= bar_end(FULL.index[350:351], "1D")[0])[-1])
    sig = _signal(df, pos, tf)
    return compute_signal_context(sig, fr, _market(s, _hourly(s), v, EARN))


@pytest.mark.parametrize("tf", ["4H", "1D", "1W", "1M"])
def test_no_lookahead_future_bars_do_not_change_ctx(tf):
    truncated = _ctx_for(tf, CUT)
    extended = _ctx_for(tf, FULL.index[-1])
    assert truncated is not None
    assert truncated == extended


def test_fields_daily():
    ctx = _ctx_for("1D", FULL.index[-1])
    for k in ("atr14_pct", "stop_dist_pct", "stop_atr", "rel_vol", "gap_pct", "spy_day_ret", "vix_level"):
        assert isinstance(ctx[k], float), k
    assert ctx["minutes_from_open"] is None
    assert ctx["signal_direction"] == "LONG"
    assert ctx["sector"] == "Technology"
    assert ctx["earnings_within_5d"] is True  # earnings 3 sessions after
    assert set(ctx["htf_trend"]) == {"above_ema50", "ema50_slope_pct"}
    assert ctx["weekday"] == CUT.weekday()
    exp_gap = (FULL["Open"].iloc[350] / FULL["Close"].iloc[349] - 1) * 100
    assert ctx["gap_pct"] == pytest.approx(exp_gap, abs=1e-3)
    exp_spy = (SPY["Close"].iloc[350] / SPY["Close"].iloc[349] - 1) * 100
    assert ctx["spy_day_ret"] == pytest.approx(exp_spy, abs=1e-3)
    assert ctx["spy_regime"] == bool(SPY["Close"].iloc[350] > SPY["Close"].iloc[301:351].mean())


def test_fields_4h_intraday():
    ctx = _ctx_for("4H", FULL.index[-1])
    assert ctx["minutes_from_open"] in (240, 390)
    assert ctx["htf_trend"] is not None
    assert isinstance(ctx["spy_day_ret"], float)


def _mark(df, i, side="Low"):
    df.iloc[i, df.columns.get_loc(side)] = df["Open"].iloc[i]


def _plain(n=500):
    df = FULL.copy()
    df["High"] = df[["Open", "Close"]].max(axis=1) * 1.01
    df["Low"] = df[["Open", "Close"]].min(axis=1) * 0.99
    return df


M0 = MarketCache(history=lambda *a: None, info=lambda s: {}, earnings=lambda s: None)


def test_bars_since_prev_marker_and_no_lookahead():
    df = _plain()
    sig = _signal(df, 350, "1D")
    assert compute_signal_context(sig, {"1D": df}, M0)["bars_since_prev_marker"] is None
    _mark(df, 343, "High")
    assert compute_signal_context(sig, {"1D": df}, M0)["bars_since_prev_marker"] == 7
    _mark(df, 350)  # the pattern bar itself is not a "previous" marker
    base = compute_signal_context(sig, {"1D": df}, M0)
    assert base["bars_since_prev_marker"] == 7
    fut = df.copy()  # mutate future bars into markers + append more: unchanged
    for i in (351, 352, 400):
        _mark(fut, i)
    fut.iloc[351:, :4] *= 1.3
    extra = fut.iloc[-5:].copy()
    extra.index = extra.index + pd.Timedelta(days=30)
    fut = pd.concat([fut, extra])
    assert compute_signal_context(sig, {"1D": fut}, M0) == base
    assert "marker_age_bars" not in base and "marker_dist_atr" not in base


def test_target_dist_atr_and_planned_rr_matches_rr_ratio():
    from strategy.devils_markers import find_devils_markers
    from signal_context import atr14
    tested = 0
    for seed in range(6):
        d = _daily(400, seed=seed)
        for i in range(30, 400, 9):
            _mark(d, i, "Low" if i % 2 else "High")
        for m in find_devils_markers(d, ticker="T", timeframe="1D", recent_candles=380):
            ctx = compute_signal_context(m, {"1D": d}, M0)
            assert ctx["planned_rr"] == pytest.approx(m["rr_ratio"], abs=0.02)
            pos = list(d.index).index(pd.Timestamp(m["pattern_at"]))
            exp = abs(m["target"] - m["entry_price"]) / atr14(d.iloc[:pos + 1])
            assert ctx["target_dist_atr"] == pytest.approx(exp, abs=1e-3)
            tested += 1
    assert tested > 5


def test_backfill_row_key_matches_across_timezone_formats():
    from devils_backfill import row_key
    db = {"timeframe": "1D", "direction": "BULLISH", "pattern_at": "2025-01-02T05:00:00+00:00"}
    yf_ = {"timeframe": "1D", "direction": "BULLISH", "pattern_at": "2025-01-02T00:00:00-05:00"}
    naive = {"timeframe": "4H", "direction": "BULLISH", "pattern_at": "2025-01-02T14:30:00"}
    assert row_key(db) == row_key(yf_)
    assert row_key(naive) == row_key({**naive, "pattern_at": "2025-01-02T14:30:00+00:00"})
    assert row_key(db) != row_key({**yf_, "direction": "BEARISH"})


def test_monthly_has_no_htf():
    assert _ctx_for("1M", FULL.index[-1])["htf_trend"] is None


def test_unknown_earnings_is_none_and_failures_are_none():
    fr = _frames(FULL, None)
    sig = _signal(FULL, 350, "1D")
    boom = MarketCache(history=lambda *a: (_ for _ in ()).throw(RuntimeError("yahoo down")),
                       info=lambda s: (_ for _ in ()).throw(RuntimeError()), earnings=lambda s: None)
    ctx = compute_signal_context(sig, fr, boom)
    assert ctx["earnings_within_5d"] is None
    assert ctx["spy_regime"] is None and ctx["vix_level"] is None and ctx["sector"] is None
    assert isinstance(ctx["atr14_pct"], float)
    assert compute_signal_context({"timeframe": "1D"}, fr, boom) is None  # never raises


def test_signal_to_row_stores_ctx_in_meta():
    fr = _frames(FULL, None)
    m = MarketCache(history=lambda *a: None, info=lambda s: {}, earnings=lambda s: None)
    import signal_context
    signal_context._DEFAULT = m
    try:
        sig = {**_signal(FULL, 350, "1D"), "target": 1.0, "rr_ratio": 2.0, "status": "CLOSED_WIN"}
        row = signal_to_row(sig, fr)
    finally:
        signal_context._DEFAULT = None
    assert row["meta"]["ctx_version"] == CTX_VERSION
    assert row["meta"]["strategy"] == "pure_no_wick"
    assert row["meta"]["ctx"]["signal_direction"] == "LONG"
    assert "ctx" not in signal_to_row(sig)["meta"]


class _FakeQ:
    def __init__(self, sb, rows):
        self.sb, self.rows = sb, rows
    def select(self, *_): return self
    def eq(self, *_): return self
    def in_(self, *_): return self
    def order(self, *_): return self
    def range(self, a, b):
        self._r = (a, b); return self
    def update(self, payload):
        self.sb.updates.append(payload); return self
    def execute(self):
        class R: pass
        r = R()
        r.data = self.rows[self._r[0]:self._r[1] + 1] if hasattr(self, "_r") else []
        return r


class _FakeSB:
    def __init__(self, rows):
        self.rows, self.updates = rows, []
    def table(self, _):
        return _FakeQ(self, self.rows)


def test_backfill_context_dry_run_never_writes_and_live_writes_only_meta(monkeypatch):
    import signal_context
    signal_context._DEFAULT = MarketCache(history=lambda *a: None, info=lambda s: {}, earnings=lambda s: None)
    try:
        sig = _signal(FULL, 350, "1D")
        row = {"id": 1, "symbol": "TEST", "status": "CLOSED_LOSS", "meta": {"strategy": "pure_no_wick"}, **sig}
        sb = _FakeSB([row])
        st = backfill_context.run(sb, dry_run=True, limit=10, batch=5, fetch=lambda s: _frames(FULL, None))
        assert st["with_ctx"] == 1 and sb.updates == []
        st = backfill_context.run(sb, dry_run=False, limit=10, batch=5, fetch=lambda s: _frames(FULL, None))
        assert st["written"] == 1
        assert list(sb.updates[0]) == ["meta"]
        assert sb.updates[0]["meta"]["strategy"] == "pure_no_wick"
        assert sb.updates[0]["meta"]["ctx_version"] == CTX_VERSION
    finally:
        signal_context._DEFAULT = None


class _UpsertQ(_FakeQ):
    def upsert(self, payload, **_):
        self.sb.upserts.append(payload); return self
    def limit(self, *_): return self
    def execute(self):
        class R: pass
        r = R(); r.data = self.rows; return r


class _UpsertSB(_FakeSB):
    def __init__(self, rows):
        super().__init__(rows); self.upserts = []
    def table(self, _):
        return _UpsertQ(self, self.rows)


def test_reupsert_keeps_original_ctx_snapshot():
    from devils_scanner import upsert_signal
    from signal_context import keep_existing_context
    old_ctx = {"sector": "Technology", "earnings_within_5d": False}
    new = {"symbol": "T", "timeframe": "1D", "direction": "BULLISH", "pattern_at": "x",
           "meta": {"strategy": "pure_no_wick", "ctx": {"sector": "Other"}, "ctx_version": CTX_VERSION}}
    sb = _UpsertSB([{"id": 1, "result": None, "status": "ACTIVE",
                     "meta": {"ctx": old_ctx, "ctx_version": CTX_VERSION}}])
    upsert_signal(sb, dict(new), dry_run=False)
    assert sb.upserts[0]["meta"]["ctx"] == old_ctx
    # new row, old version or missing ctx -> freshly computed ctx is used
    sb = _UpsertSB([])
    upsert_signal(sb, dict(new), dry_run=False)
    assert sb.upserts[0]["meta"]["ctx"] == {"sector": "Other"}
    assert keep_existing_context(new["meta"], {"ctx": old_ctx, "ctx_version": 0})["ctx"] == {"sector": "Other"}
    assert keep_existing_context(new["meta"], {"ctx_version": CTX_VERSION})["ctx"] == {"sector": "Other"}
