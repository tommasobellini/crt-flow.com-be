from __future__ import annotations

import json
from typing import Any


def signal_to_crt_row(signal: dict[str, Any], entry_price: float | None = None) -> dict[str, Any]:
    """Map HA+RSI MTF signal to crt_signals row shape."""
    is_bullish = signal.get("signal_type", "BULLISH") == "BULLISH"
    is_golden = bool(signal.get("is_golden"))
    entry = float(entry_price or 0)
    sl_pct = 0.01
    rr = 2.0
    if entry > 0:
        sl = entry * (1 - sl_pct) if is_bullish else entry * (1 + sl_pct)
        risk = abs(entry - sl)
        tp = entry + risk * rr if is_bullish else entry - risk * rr
    else:
        sl = tp = 0.0

    rsi_4h = signal.get("4h_rsi")
    rsi_1h = signal.get("1h_rsi")
    rsi_15m = signal.get("15m_rsi")

    trigger_metadata = {
        "strategy": "ha_rsi_mtf",
        "is_golden": is_golden,
        "4h_rsi": rsi_4h,
        "1h_rsi": rsi_1h,
        "15m_rsi": rsi_15m,
    }

    return {
        "symbol": signal.get("ticker"),
        "timeframe": "15M",
        "type": "bullish_ha_rsi" if is_bullish else "bearish_ha_rsi",
        "subtype": "HA+RSI MTF",
        "range_high": None,
        "range_low": None,
        "price": round(entry, 2) if entry else None,
        "entry_price": round(entry, 2) if entry else None,
        "stop_loss": round(sl, 2) if entry else None,
        "take_profit": round(tp, 2) if entry else None,
        "rr_ratio": rr if entry else None,
        "status": "pending",
        "is_active": True,
        "result": None,
        "liquidity_tier": f"1H RSI {rsi_1h}" if rsi_1h is not None else None,
        "session_tag": "HA+RSI MTF",
        "diamond_score": "GOLDEN" if is_golden else "MTF",
        "confluence_level": f"4H RSI {rsi_4h}" if rsi_4h is not None else None,
        "trigger_candles": json.dumps(trigger_metadata),
    }
