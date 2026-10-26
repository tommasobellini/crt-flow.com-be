"""Parity: scanner port of devils_cisd_v1 must reproduce bt_ab/ab.py version B (1D, tol 1e-4)."""
import importlib.util, os, sys
import numpy as np, pandas as pd, pytest
from strategy.devils_cisd_v1 import add_trend, find_signals

AB = "/workspace/bt_ab/ab.py"


def _synthetic(n=400, seed=1):
    r = np.random.default_rng(seed)
    c = 100 + np.cumsum(r.normal(0, 1, n))
    o = c + r.normal(0, 0.5, n)
    h = np.maximum(o, c) + np.abs(r.normal(0, 0.6, n))
    l = np.minimum(o, c) - np.abs(r.normal(0, 0.6, n))
    m = r.random(n) < 0.08
    h[m & (c < o)] = o[m & (c < o)]
    l[m & (c > o)] = o[m & (c > o)]
    return pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c}, index=pd.bdate_range("2020-01-01", periods=n))


@pytest.mark.skipif(not os.path.exists(AB), reason="bt_ab not available")
def test_entries_match_backtest():
    src = open(AB).read().split("data={t:load(t)")[0]
    ns: dict = {}
    exec(src, ns)
    df = add_trend(_synthetic())
    ref = ns["run"](df, 1e-4, "B")
    got = find_signals(df, "TEST")
    assert [str(pd.Timestamp(d)) for d, _, _ in ref] == [str(pd.Timestamp(s["pattern_at"])) for s in got]
    assert len(got) > 0
