"""
devils_cisd_v1 — FROZEN paper-trading rule (do not tune; see devils_cisd_v1_spec.md).

Ported 1:1 from /workspace/bt_ab/ab.py, version "B", timeframe 1D:
- Devil mark: |Open-High| <= 1e-4*price and red candle -> BEARISH;
              |Open-Low|  <= 1e-4*price and green candle -> BULLISH.
- Trend filter: sign(prior daily Close - prior SMA50) must equal direction.
- Sweep: within W=10 bars after the mark, a bar breaks the low (long) / high (short)
  of the previous SW=5 bars.
- CISD: close back beyond the open of the sweep's opposite-colour run, within SW bars.
- Entry = close of CISD bar; stop = sweep extreme -/+ 0.01%; target = 2R; max hold 50 bars.
PAPER ONLY: signals are tagged paper=True and must never be used for real orders.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

STRATEGY = "devils_cisd_v1"
TOL = 1e-4
W = 10
SW = 5
MAXH = 50
RR = 2.0
MIN_RISK_PCT = 0.001


def add_trend(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["tr"] = np.sign(df["Close"] - df["Close"].rolling(50).mean()).shift(1)
    return df


def _simulate(H, L, C, i0, dr, e, st):
    """Returns (status, exit_index, r_multiple) with gross R (costs are not booked live)."""
    n = len(C)
    risk = (e - st) * dr
    tg = e + RR * risk * dr
    for k in range(i0 + 1, min(i0 + MAXH + 1, n)):
        if (dr == 1 and L[k] <= st) or (dr == -1 and H[k] >= st):
            return "CLOSED_LOSS", k, -1.0
        if (dr == 1 and H[k] >= tg) or (dr == -1 and L[k] <= tg):
            return "CLOSED_WIN", k, RR
    if i0 + MAXH < n:
        k = i0 + MAXH
        return "CLOSED_TIME", k, round((C[k] - e) * dr / risk, 3)
    return "ACTIVE", None, None


def find_signals(df: pd.DataFrame, ticker: str) -> list[dict]:
    if df is None or len(df) < 60:
        return []
    if "tr" not in df:
        df = add_trend(df)
    O, H, L, C = (df[c].values for c in ("Open", "High", "Low", "Close"))
    tr = df["tr"].values
    n = len(C)
    red, grn = C < O, C > O
    out: list[dict] = []
    busy = -1
    for i in range(SW + 1, n - 1):
        if i <= busy:
            continue
        sc = max(O[i], H[i], 1)
        dr = 0
        if abs(O[i] - H[i]) <= TOL * sc and red[i]:
            dr = -1
        elif abs(O[i] - L[i]) <= TOL * sc and grn[i]:
            dr = 1
        if dr == 0 or tr[i] != dr:
            continue
        ent = sw = None
        st = None
        for k in range(i + 1, min(i + W + 1, n)):
            if sw is None:
                if dr == 1 and L[k] < L[k - SW : k].min():
                    sw = k
                if dr == -1 and H[k] > H[k - SW : k].max():
                    sw = k
                if sw is None:
                    continue
            ext = L[sw : k + 1].min() if dr == 1 else H[sw : k + 1].max()
            j = sw
            if dr == 1:
                while j > 0 and red[j - 1] and red[j]:
                    j -= 1
                if not red[j]:
                    j = sw
                if (C[k] > O[j] and k > sw) or (k == sw and C[k] > O[j] and red[j] and j < sw):
                    ent, st = k, ext * (1 - 1e-4)
                    break
            else:
                while j > 0 and grn[j - 1] and grn[j]:
                    j -= 1
                if not grn[j]:
                    j = sw
                if (C[k] < O[j] and k > sw) or (k == sw and C[k] < O[j] and grn[j] and j < sw):
                    ent, st = k, ext * (1 + 1e-4)
                    break
            if k - sw >= SW:
                break
        if ent is None:
            continue
        e = float(C[ent])
        risk = (e - st) * dr
        if risk <= 0 or risk / e < MIN_RISK_PCT:
            continue
        status, k_exit, r = _simulate(H, L, C, ent, dr, e, st)
        busy = k_exit if k_exit is not None else n
        out.append(
            {
                "ticker": ticker.upper(),
                "timeframe": "1D",
                "direction": "BULLISH" if dr == 1 else "BEARISH",
                "devils_marker_price": round(float(O[i]), 4),
                "mark_at": pd.Timestamp(df.index[i]).isoformat(),
                "pattern_at": pd.Timestamp(df.index[ent]).isoformat(),
                "entry_price": round(e, 4),
                "stop_loss": round(float(st), 4),
                "target": round(e + RR * risk * dr, 4),
                "rr_ratio": RR,
                "status": status,
                "r_multiple": r,
                "closed_at": pd.Timestamp(df.index[k_exit]).isoformat() if k_exit is not None else None,
                "strategy": STRATEGY,
                "paper": True,
            }
        )
    return out
