"""QA: mutate every value after signal time (not just append) -> ctx must be identical."""
import numpy as np, pandas as pd, pytest
from tests.test_signal_context import FULL, SPY, VIX, EARN, _hourly, _frames, _market, _signal
from signal_context import bar_end, compute_signal_context

def _mut(df, t_utc, seed, tf):
    if df is None: return None
    df = df.copy(); end = bar_end(df.index, tf)
    m = np.asarray(end > t_utc)
    rng = np.random.default_rng(seed)
    for c in ["Open","High","Low","Close","Volume"]:
        if c in df: df.loc[m, c] = df.loc[m, c].astype(float) * rng.uniform(0.5, 1.5, m.sum())
    return df

@pytest.mark.parametrize("tf,which", [("4H",0),("4H",1),("1D",0),("1W",0),("1M",0)])
def test_mutating_future_keeps_ctx(tf, which):
    now = FULL.index[-1] + pd.Timedelta(hours=17)
    fr = _frames(FULL, now); df = fr[tf]
    day = FULL.index[350].tz_convert("UTC").tz_localize(None).normalize()
    if tf == "4H":
        pos = int(np.flatnonzero(pd.DatetimeIndex(df.index).normalize() == day)[which])
    else:
        pos = int(np.flatnonzero(bar_end(df.index, tf) <= bar_end(FULL.index[350:351], "1D")[0])[-1])
    sig = _signal(df, pos, tf); t = bar_end(df.index[pos:pos+1], tf)[0]
    spy_h = _hourly(SPY)
    base = compute_signal_context(sig, fr, _market(SPY, spy_h, VIX, EARN))
    h1 = _hourly(FULL)
    from market_data import resample_to_4h, resample_to_weekly, resample_to_monthly
    d2 = _mut(FULL, t, 1, "1D")
    fr2 = {"1D": d2, "1W": _mut(fr["1W"], t, 2, "1W"), "1M": _mut(fr["1M"], t, 3, "1M"),
           "4H": _mut(fr["4H"], t, 4, "4H")}
    mk2 = _market(_mut(SPY, t, 5, "1D"), _mut(spy_h, t, 6, "1h"), _mut(VIX, t, 7, "1D"), EARN)
    got = compute_signal_context(sig, fr2, mk2)
    assert base is not None and got == base, {k:(base[k],got[k]) for k in base if base[k]!=got[k]}
