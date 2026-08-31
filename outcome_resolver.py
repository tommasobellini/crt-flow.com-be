"""
Resolve open crt_signals to WIN/LOSS when price hits take_profit or stop_loss.

Run after scanner persist (e.g. hourly cron):
    python outcome_resolver.py
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

from market_data import clean_df

logger = logging.getLogger(__name__)

IC_CISD_TYPES = (
    "bullish_ic_cisd",
    "bearish_ic_cisd",
    "bullish_sweep_engulf",
    "bearish_sweep_engulf",
    "bullish_liq_sweep",
    "bearish_liq_sweep",
)

TF_INTERVAL: dict[str, str] = {
    "5M": "5m",
    "5m": "5m",
    "15M": "15m",
    "15m": "15m",
    "1H": "1h",
    "1h": "1h",
    "4H": "1h",
    "4h": "1h",
}

TF_PERIOD: dict[str, str] = {
    "5m": "30d",
    "15m": "60d",
    "1h": "730d",
}


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
        logger.error("Missing Supabase credentials in .env.local")
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


def fetch_bars(symbol: str, timeframe: str) -> pd.DataFrame | None:
    interval = TF_INTERVAL.get(timeframe, "1h")
    period = TF_PERIOD.get(interval, "60d")
    try:
        df = yf.Ticker(symbol).history(
            period=period, interval=interval, auto_adjust=True
        )
    except Exception as e:
        logger.warning(f"  {symbol}: yfinance error — {e}")
        return None
    df = clean_df(df.dropna() if df is not None else None)
    if df is None or df.empty:
        return None
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(None)
    return df


def resample_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    agg: dict[str, str] = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
    }
    if "Volume" in df_1h.columns:
        agg["Volume"] = "sum"
    return df_1h.resample("4h").agg(agg).dropna()


def bars_for_signal(symbol: str, timeframe: str) -> pd.DataFrame | None:
    tf = (timeframe or "1H").upper()
    if tf == "4H":
        df_1h = fetch_bars(symbol, "1H")
        if df_1h is None or df_1h.empty:
            return None
        return resample_4h(df_1h)
    return fetch_bars(symbol, timeframe)


def resolve_outcome(signal: dict[str, Any], df: pd.DataFrame) -> dict[str, Any] | None:
    """Return update payload if TP or SL hit; else None."""
    entry = float(signal.get("entry_price") or signal.get("price") or 0)
    stop = float(signal.get("stop_loss") or 0)
    target = float(signal.get("take_profit") or 0)
    if not entry or not stop or not target:
        return None

    sig_type = signal.get("type", "")
    is_long = "bullish" in str(sig_type).lower()

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

        if sl_hit and tp_hit:
            return _close_payload("LOSS", "SL_HIT", ts)
        if sl_hit:
            return _close_payload("LOSS", "SL_HIT", ts)
        if tp_hit:
            return _close_payload("WIN", "TP_HIT", ts)
    return None


def _close_payload(result: str, exit_reason: str, ts: pd.Timestamp) -> dict[str, Any]:
    closed = ts.to_pydatetime()
    if closed.tzinfo is None:
        closed = closed.replace(tzinfo=timezone.utc)
    else:
        closed = closed.astimezone(timezone.utc)
    return {
        "result": result,
        "exit_reason": exit_reason,
        "closed_at": closed.isoformat(),
        "is_active": False,
        "status": "closed",
    }


def load_open_signals(supabase: Client, limit: int = 500) -> list[dict[str, Any]]:
    resp = (
        supabase.table("crt_signals")
        .select(
            "id, symbol, timeframe, type, entry_price, price, stop_loss, "
            "take_profit, created_at, pattern_at, result"
        )
        .in_("type", list(IC_CISD_TYPES))
        .eq("is_active", True)
        .order("created_at", desc=False)
        .limit(limit)
        .execute()
    )
    rows = resp.data or []
    return [r for r in rows if _is_open(r.get("result"))]


def _is_open(result: str | None) -> bool:
    return result is None or str(result).upper() in ("OPEN", "")


def resolve_all(supabase: Client, dry_run: bool = False) -> tuple[int, int]:
    signals = load_open_signals(supabase)
    if not signals:
        logger.info("No open signals to resolve.")
        return 0, 0

    logger.info(f"Checking {len(signals)} open signal(s)...")
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for sig in signals:
        by_symbol.setdefault(sig["symbol"], []).append(sig)

    closed = 0
    checked = 0
    bar_cache: dict[tuple[str, str], pd.DataFrame | None] = {}

    for symbol, sym_signals in by_symbol.items():
        for sig in sym_signals:
            checked += 1
            tf = sig.get("timeframe") or "1H"
            cache_key = (symbol, tf.upper())
            if cache_key not in bar_cache:
                bar_cache[cache_key] = bars_for_signal(symbol, tf)
            df = bar_cache[cache_key]
            if df is None or df.empty:
                logger.warning(f"  #{sig['id']} {symbol} {tf}: no OHLC data")
                continue

            update = resolve_outcome(sig, df)
            if not update:
                continue

            logger.info(
                f"  ✓ #{sig['id']} {symbol} {tf} → {update['result']} "
                f"({update['exit_reason']}) @ {update['closed_at']}"
            )
            if not dry_run:
                supabase.table("crt_signals").update(update).eq("id", sig["id"]).execute()
            closed += 1

    return checked, closed


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="Resolve open CRT signals to WIN/LOSS")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log outcomes without updating Supabase",
    )
    args = parser.parse_args()

    supabase = setup_supabase()
    if supabase is None:
        sys.exit(1)

    checked, closed = resolve_all(supabase, dry_run=args.dry_run)
    mode = "DRY-RUN" if args.dry_run else "LIVE"
    logger.info(f"[{mode}] Checked {checked}, closed {closed} signal(s).")


if __name__ == "__main__":
    main()
