"""Trade outcome excursions for closed Devils signals: meta.outcome (NOT meta.ctx).

mae_r / mfe_r are known only after the trade closes. They are for judging stop and target
after the fact; they must never be used as entry filters and never go in meta.ctx.

    R      = |entry - stop|
    window = bars after the pattern (entry) bar, up to and including the closing bar
             (first bar where SL or TP is hit; same bar SL+TP -> LOSS, as the trade layer)
    mae_r  = worst gross excursion against the trade, in R (<= 0; <= -1 on a loss, < -1 on gaps)
    mfe_r  = best gross excursion in favour, in R (>= 0; >= planned R on a win)
No costs, no capping (gross values). bars_held = bars in the window.
    mae_r_capped   = loss: max(mae_r, -1), or the exit bar's open in R if it opened beyond the
                     stop (gap); win: mae_r
    mfe_r_pre_exit = MFE excluding the closing bar (0 if the trade closed on the first bar)
    ambiguous_exit_bar = the closing bar touched both stop and target (order unknown)
"""
from __future__ import annotations

from signal_context import pattern_position

OUTCOME_VERSION = 1
CLOSED = ("CLOSED_WIN", "CLOSED_LOSS")


def _r(x: float) -> float:
    return round(float(x), 4)


def compute_trade_outcome(signal: dict, frames: dict) -> dict | None:
    """signal: devils signal dict or devils_signals row (timeframe, direction, entry_price,
    stop_loss, target, pattern_at, status). Returns {mae_r, mfe_r, outcome_version, bars_held}
    or None (not closed, pattern bar not in frame, trade not closed in the frame, or the bars
    give a different outcome than the stored status). Never raises."""
    try:
        return _compute(signal, frames)
    except Exception:
        return None


def _compute(signal, frames):
    status = signal.get("status")
    if status not in CLOSED:
        return None
    df = (frames or {}).get(signal["timeframe"])
    if df is None or getattr(df, "empty", True):
        return None
    pos = pattern_position(df, signal["pattern_at"])
    if pos is None:
        return None
    entry, stop = float(signal["entry_price"]), float(signal["stop_loss"])
    target = float(signal["target"])
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    long = str(signal["direction"]).upper() in ("BULLISH", "LONG")
    mae = mfe = mfe_pre = 0.0
    bars = 0
    result = None
    ambiguous = False
    exit_open = None
    after = df.iloc[pos + 1:]
    for o, h, l in zip(after["Open"].astype(float), after["High"].astype(float), after["Low"].astype(float)):
        bars += 1
        if long:
            adv, fav = (l - entry) / risk, (h - entry) / risk
            sl_hit, tp_hit = l <= stop, h >= target
        else:
            adv, fav = (entry - h) / risk, (entry - l) / risk
            sl_hit, tp_hit = h >= stop, l <= target
        mfe_pre = mfe  # MFE of the bars before this one
        mae, mfe = min(mae, adv), max(mfe, fav)
        if sl_hit or tp_hit:
            result = "CLOSED_LOSS" if sl_hit else "CLOSED_WIN"
            ambiguous = bool(sl_hit and tp_hit)
            exit_open = o
            break
    if result is None or result != status:
        return None
    if result == "CLOSED_LOSS":
        gap = exit_open < stop if long else exit_open > stop
        mae_capped = ((exit_open - entry) if long else (entry - exit_open)) / risk if gap else max(mae, -1.0)
    else:
        mae_capped = mae
    return {"mae_r": _r(mae), "mfe_r": _r(mfe), "mae_r_capped": _r(mae_capped),
            "mfe_r_pre_exit": _r(mfe_pre), "ambiguous_exit_bar": ambiguous,
            "outcome_version": OUTCOME_VERSION, "bars_held": bars}


def attach_outcome(meta: dict | None, outcome: dict | None) -> dict:
    """Adds meta.outcome; every other meta key (ctx, ctx_version, ...) is kept unchanged."""
    out = dict(meta or {})
    if outcome is not None:
        out["outcome"] = dict(outcome)
    return out


def has_outcome(meta: dict | None) -> bool:
    return ((meta or {}).get("outcome") or {}).get("outcome_version") == OUTCOME_VERSION
