"""Devils exits: resolve the known opening price before unknown intrabar order.

When the open is between SL and TP and both are touched, SL-first remains the
conservative assumption. An exit already filled at the open cannot be reversed
by a later stop touch in the same candle.
"""
from __future__ import annotations

from math import isfinite

RESOLVER_VERSION = 3


def exit_on_bar(o, h, l, *, long: bool, entry: float, stop: float, target: float,
                planned_r: float | None = None):
    """Returns None (bar does not close the trade) or (status, r_multiple, fill_price, gap)."""
    o, h, l = float(o), float(h), float(l)
    entry, stop, target = float(entry), float(stop), float(target)
    risk = abs(entry - stop)
    valid_levels = stop < entry < target if long else target < entry < stop
    if not all(isfinite(v) for v in (o, h, l, entry, stop, target)) or not valid_levels:
        raise ValueError("Exit resolution requires finite prices and correctly ordered trade levels")
    if l > min(o, h) or o > h:
        raise ValueError("Exit resolution requires Low <= Open <= High")
    if planned_r is not None and (not isfinite(float(planned_r)) or float(planned_r) <= 0):
        raise ValueError("Planned R must be finite and positive")
    d = 1.0 if long else -1.0
    sl_hit = l <= stop if long else h >= stop
    tp_hit = h >= target if long else l <= target
    sl_open = o <= stop if long else o >= stop
    tp_open = o >= target if long else o <= target
    if sl_open:
        gap = o != stop
        r = round((o - entry) * d / risk, 4) if risk > 0 else -1.0
        return "CLOSED_LOSS", r, o, gap
    if tp_open:
        gap = o != target
        r = round((o - entry) * d / risk, 4) if risk > 0 else 0.0
        if not gap and planned_r is not None:
            r = float(planned_r)
        return "CLOSED_WIN", r, o, gap
    if sl_hit:
        return "CLOSED_LOSS", -1.0, stop, False
    if tp_hit:
        if planned_r is None:
            planned_r = round((target - entry) * d / risk, 4) if risk > 0 else 0.0
        return "CLOSED_WIN", float(planned_r), target, False
    return None


def exit_metadata(o, h, l, *, long: bool, stop: float, target: float, exit: tuple) -> dict:
    """Persist the fill and flag only intrabar ordering that is genuinely unknown."""
    at_open = (o <= stop or o >= target) if long else (o >= stop or o <= target)
    both = (l <= stop and h >= target) if long else (h >= stop and l <= target)
    return {"exit_price": float(exit[2]), "exit_gap": bool(exit[3]),
            "ambiguous_exit_bar": bool(both and not at_open), "exit_time_precision": "bar"}
