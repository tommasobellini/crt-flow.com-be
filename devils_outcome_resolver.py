"""
Resolve open Devil's Markers to WIN/LOSS — isolated from IC-CISD outcome_resolver.

    python devils_outcome_resolver.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from supabase import Client, create_client

from market_data import (
    clean_df,
    resample_to_4h,
    resample_to_7h,
    resample_to_monthly,
    resample_to_weekly,
)

logger = logging.getLogger(__name__)

OPEN_STATUSES = ("ACTIVE", "MARKER_PENDING", "CONTINUATION_ACTIVE")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def setup_supabase() -> Client | None:
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )
    if not url or not key:
        logger.error("Missing Supabase credentials")
        return None
    return create_client(url, key)


def _parse_ts(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
        return ts
    except Exception:
        return None


def bars_for_tf(symbol: str, timeframe: str) -> pd.DataFrame | None:
    tf = (timeframe or "1D").upper()
    try:
        if tf in ("4H", "7H"):
            raw = yf.Ticker(symbol).history(period="730d", interval="1h", auto_adjust=True)
            df = clean_df(raw.dropna() if raw is not None else None)
            if df is None or df.empty:
                return None
            if getattr(df.index, "tz", None) is not None:
                df.index = df.index.tz_convert(None)
            return resample_to_4h(df) if tf == "4H" else resample_to_7h(df)

        raw = yf.Ticker(symbol).history(period="5y", interval="1d", auto_adjust=True)
        df = clean_df(raw.dropna() if raw is not None else None)
        if df is None or df.empty:
            return None
        if getattr(df.index, "tz", None) is not None:
            df.index = df.index.tz_convert(None)
        if tf == "1W":
            return resample_to_weekly(df)
        if tf == "1M":
            return resample_to_monthly(df)
        return df
    except Exception as e:
        logger.warning(f"  {symbol} {tf}: {e}")
        return None


def resolve_outcome(signal: dict[str, Any], df: pd.DataFrame) -> dict[str, Any] | None:
    entry = float(signal.get("entry_price") or signal.get("cisd_level") or 0)
    stop = float(signal.get("stop_loss") or 0)
    target = float(signal.get("target") or 0)
    if not entry or not stop or not target:
        return None

    is_long = str(signal.get("direction", "")).upper() == "BULLISH"
    start = _parse_ts(signal.get("pattern_at") or signal.get("created_at"))
    if start is None:
        return None

    future = df[df.index > start]
    if future.empty:
        return None

    for ts, candle in future.iterrows():
        high = float(candle["High"])
        low = float(candle["Low"])
        if is_long:
            sl_hit = low <= stop
            tp_hit = high >= target
        else:
            sl_hit = high >= stop
            tp_hit = low <= target

        # Same bar: SL first (conservative)
        if sl_hit and tp_hit:
            return _close_payload("LOSS", "SL_HIT", ts, signal)
        if sl_hit:
            return _close_payload("LOSS", "SL_HIT", ts, signal)
        if tp_hit:
            return _close_payload("WIN", "TP_HIT", ts, signal)
    return None


def _close_payload(
    result: str, exit_reason: str, ts: pd.Timestamp, signal: dict
) -> dict[str, Any]:
    closed = ts.to_pydatetime()
    if closed.tzinfo is None:
        closed = closed.replace(tzinfo=timezone.utc)
    else:
        closed = closed.astimezone(timezone.utc)

    rr = float(signal.get("rr_ratio") or 0)
    r_multiple = rr if result == "WIN" else -1.0

    return {
        "result": result,
        "exit_reason": exit_reason,
        "closed_at": closed.isoformat(),
        "is_active": False,
        "status": "CLOSED_WIN" if result == "WIN" else "CLOSED_LOSS",
        "r_multiple": r_multiple,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def load_open(supabase: Client, limit: int = 2000) -> list[dict]:
    resp = (
        supabase.table("devils_signals")
        .select(
            "id, symbol, timeframe, direction, entry_price, cisd_level, "
            "stop_loss, target, rr_ratio, pattern_at, created_at, result, status"
        )
        .eq("is_active", True)
        .is_("result", "null")
        .limit(limit)
        .execute()
    )
    rows = resp.data or []
    return [r for r in rows if r.get("status") in OPEN_STATUSES or r.get("status") == "ACTIVE"]


def resolve_all(supabase: Client, dry_run: bool = False) -> tuple[int, int]:
    signals = load_open(supabase)
    if not signals:
        logger.info("No open Devil's Markers to resolve.")
        return 0, 0

    logger.info(f"Checking {len(signals)} open Devil marker(s)...")
    closed = 0
    checked = 0
    cache: dict[tuple[str, str], pd.DataFrame | None] = {}

    for sig in signals:
        checked += 1
        symbol = sig["symbol"]
        tf = (sig.get("timeframe") or "1D").upper()
        key = (symbol, tf)
        if key not in cache:
            cache[key] = bars_for_tf(symbol, tf)
        df = cache[key]
        if df is None or df.empty:
            continue

        update = resolve_outcome(sig, df)
        if not update:
            continue

        logger.info(
            f"  ✓ {symbol} {tf} → {update['result']} ({update['exit_reason']}) "
            f"R={update.get('r_multiple')}"
        )
        if not dry_run:
            supabase.table("devils_signals").update(update).eq("id", sig["id"]).execute()
        closed += 1

    return checked, closed


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="Resolve Devil's Markers outcomes")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    supabase = setup_supabase()
    if supabase is None:
        sys.exit(1)

    checked, closed = resolve_all(supabase, dry_run=args.dry_run)
    mode = "DRY-RUN" if args.dry_run else "LIVE"
    logger.info(f"[{mode}] Checked {checked}, closed {closed} Devil marker(s).")


if __name__ == "__main__":
    main()
