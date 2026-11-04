"""Position sizing risk rule (Trading Strategist, risk rule - not a strategy filter).

Risk per trade = RISK_PCT of account; position value capped at MAX_EXPOSURE_PCT of account.
With a stop closer than RISK_PCT/MAX_EXPOSURE_PCT (0.5% by default) the full risk would need
leverage, so the trade's risk is scaled down by size_factor and its R scales the same way:
    size_factor = min(1, stop_dist_pct * MAX_EXPOSURE_PCT / 100 / RISK_PCT)
    sized_r     = r_multiple * size_factor   (account-risk units)
Mirrors devils_analysis_config (risk_pct, max_exposure_pct) on Supabase.
"""
from __future__ import annotations

RISK_PCT = 0.5
MAX_EXPOSURE_PCT = 100.0
SIZING_VERSION = 1


def stop_distance_pct(entry: float | None, stop: float | None) -> float | None:
    try:
        e, s = float(entry), float(stop)
    except (TypeError, ValueError):
        return None
    if e <= 0 or e == s:
        return None
    return abs(e - s) / e * 100.0


def size_factor(entry: float | None, stop: float | None, *,
                risk_pct: float = RISK_PCT, max_exposure_pct: float = MAX_EXPOSURE_PCT) -> float | None:
    """Fraction of the nominal risk actually taken (1.0 = full risk). None if undefined."""
    d = stop_distance_pct(entry, stop)
    if d is None or risk_pct <= 0:
        return None
    return round(min(1.0, d * (max_exposure_pct / 100.0) / risk_pct), 6)


def sized_r(r_multiple: float | None, factor: float | None) -> float | None:
    if r_multiple is None or factor is None:
        return None
    return float(r_multiple) * float(factor)


def attach_size_factor(meta: dict | None, entry, stop) -> dict:
    """Return a copy of meta with size_factor/sizing_version (only when computable)."""
    out = dict(meta or {})
    f = size_factor(entry, stop)
    if f is not None:
        out["size_factor"] = f
        out["sizing_version"] = SIZING_VERSION
    return out
