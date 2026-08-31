from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _parse_pattern_at(signal: dict[str, Any]) -> str | None:
    """Normalize setup timestamp to ISO8601 UTC for pattern_at column."""
    raw = signal.get("timestamp") or signal.get("pattern_at")
    if raw is None:
        candles = signal.get("pattern_candles") or []
        for key in ("CISD", "SWEEP", "MSS", "C3"):
            for c in candles:
                if c.get("index") == key and c.get("time"):
                    raw = datetime.fromtimestamp(int(c["time"]), tz=timezone.utc).isoformat()
                    break
            if raw is not None:
                break
        if raw is None:
            levels = signal.get("pattern_levels") or {}
            for key in ("sweep_time", "mss_time", "cisd_time"):
                if levels.get(key):
                    raw = datetime.fromtimestamp(int(levels[key]), tz=timezone.utc).isoformat()
                    break
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    text = str(raw).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        return text


def _resolve_signal_type(signal: dict[str, Any]) -> tuple[str, str]:
    """Map pattern setup to crt_signals type and subtype."""
    direction = signal.get("direction", "BULLISH")
    is_bullish = direction == "BULLISH"
    setup = (signal.get("pattern_levels") or {}).get("setup", "")

    if "SWEEP_ENGULF" in setup:
        return (
            "bullish_sweep_engulf" if is_bullish else "bearish_sweep_engulf",
            "SWEEP ENGULF",
        )
    if "LIQUIDITY_SWEEP" in setup:
        return (
            "bullish_liq_sweep" if is_bullish else "bearish_liq_sweep",
            "LIQ SWEEP",
        )
    return (
        "bullish_ic_cisd" if is_bullish else "bearish_ic_cisd",
        "IC-CISD",
    )


def signal_to_crt_row(signal: dict[str, Any], ticker: str) -> dict[str, Any]:
    """Map strategy pattern to crt_signals row shape."""
    entry = float(signal.get("entry_price") or 0)
    sl = float(signal.get("stop_loss") or 0)
    tp = float(signal.get("take_profit") or 0)
    risk = abs(entry - sl) if entry and sl else 0
    rr = abs(tp - entry) / risk if risk > 0 else 2.0

    pattern_candles = signal.get("pattern_candles", [])
    pattern_levels = signal.get("pattern_levels")
    market_cap = signal.get("market_cap")
    pattern_at = _parse_pattern_at(signal)
    sig_type, subtype = _resolve_signal_type(signal)

    row: dict[str, Any] = {
        "symbol": ticker,
        "timeframe": signal.get("timeframe", "15M"),
        "type": sig_type,
        "subtype": subtype,
        "price": round(entry, 2) if entry else None,
        "entry_price": round(entry, 2) if entry else None,
        "stop_loss": round(sl, 2) if sl else None,
        "take_profit": round(tp, 2) if tp else None,
        "rr_ratio": round(rr, 2) if rr else None,
        "pattern_candles": pattern_candles,
        "pattern_levels": pattern_levels,
        "market_cap": int(market_cap) if market_cap else None,
        "status": "pending",
        "is_active": True,
        "result": None,
    }
    if pattern_at:
        row["pattern_at"] = pattern_at
    return row
