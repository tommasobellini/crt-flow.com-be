from __future__ import annotations

from typing import Any


def signal_to_crt_row(signal: dict[str, Any], ticker: str) -> dict[str, Any]:
    """Map 3C SMC pattern to crt_signals row shape."""
    direction = signal.get("direction", "BULLISH")
    is_bullish = direction == "BULLISH"
    entry = float(signal.get("entry_price") or 0)
    sl = float(signal.get("stop_loss") or 0)
    tp = float(signal.get("take_profit") or 0)
    risk = abs(entry - sl) if entry and sl else 0
    rr = abs(tp - entry) / risk if risk > 0 else 2.0

    pattern_candles = signal.get("pattern_candles", [])
    market_cap = signal.get("market_cap")

    return {
        "symbol": ticker,
        "timeframe": signal.get("timeframe", "15M"),
        "type": "bullish_wick_3c" if is_bullish else "bearish_wick_3c",
        "subtype": "3C SMC",
        "price": round(entry, 2) if entry else None,
        "entry_price": round(entry, 2) if entry else None,
        "stop_loss": round(sl, 2) if sl else None,
        "take_profit": round(tp, 2) if tp else None,
        "rr_ratio": round(rr, 2) if rr else None,
        "pattern_candles": pattern_candles,
        "market_cap": int(market_cap) if market_cap else None,
        "status": "pending",
        "is_active": True,
        "result": None,
    }
