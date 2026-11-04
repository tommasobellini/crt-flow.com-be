"""Exit fill for Devils trades (resolver_version 2).

One bar decides the exit. Rules:
- stop touched (also when the target is touched on the same bar -> LOSS, conservative):
  if the bar OPENS beyond the stop (gap) the fill is the open, r = (open-entry)/risk signed (< -1);
  otherwise the fill is the stop, r = -1.0 exactly.
- target touched (stop not touched): if the bar OPENS beyond the target (gap) the fill is the
  open, r = (open-entry)/risk signed (> planned R); otherwise r = planned R (rr_ratio).
"""
from __future__ import annotations

RESOLVER_VERSION = 2


def exit_on_bar(o, h, l, *, long: bool, entry: float, stop: float, target: float,
                planned_r: float | None = None):
    """Returns None (bar does not close the trade) or (status, r_multiple, fill_price, gap)."""
    o, h, l = float(o), float(h), float(l)
    entry, stop, target = float(entry), float(stop), float(target)
    risk = abs(entry - stop)
    d = 1.0 if long else -1.0
    sl_hit = l <= stop if long else h >= stop
    tp_hit = h >= target if long else l <= target
    if sl_hit:
        gap = (o < stop) if long else (o > stop)
        if gap and risk > 0:
            return "CLOSED_LOSS", round((o - entry) * d / risk, 4), o, True
        return "CLOSED_LOSS", -1.0, stop, False
    if tp_hit:
        gap = (o > target) if long else (o < target)
        if gap and risk > 0:
            return "CLOSED_WIN", round((o - entry) * d / risk, 4), o, True
        if planned_r is None:
            planned_r = round((target - entry) * d / risk, 4) if risk > 0 else 0.0
        return "CLOSED_WIN", float(planned_r), target, False
    return None
