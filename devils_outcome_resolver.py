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

from strategy.gap_fill import RESOLVER_VERSION, exit_metadata, exit_on_bar
from strategy.sizing import attach_size_factor
from signal_outcome import attach_outcome, compute_trade_outcome
from signal_context import bar_end, pattern_position
from devils_signal_validation import source_validation
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from supabase import Client, create_client

from market_data import (
    clean_df,
    drop_unclosed,
    resample_to_4h,
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
    )
    if not url or not key:
        logger.error("Missing Supabase service-role credentials")
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
        if tf == "7H":
            logger.info(f"  {symbol} 7H: timeframe removed, skipping")
            return None
        if tf == "4H":
            raw = yf.Ticker(symbol).history(period="730d", interval="1h", auto_adjust=False)
            df = clean_df(raw.dropna() if raw is not None else None)
            if df is None or df.empty:
                return None
            return resample_to_4h(df)  # session-aligned, closed bars only

        raw = yf.Ticker(symbol).history(period="5y", interval="1d", auto_adjust=False)
        df = clean_df(raw.dropna() if raw is not None else None)
        if df is None or df.empty:
            return None
        df = drop_unclosed(df, "1d")
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

    future = df.loc[pd.to_datetime(df.index, utc=True) > start.tz_localize("UTC")]
    if future.empty:
        return None

    rr = signal.get("rr_ratio")
    for ts, candle in future.iterrows():
        # The opening price is known to precede any intrabar stop/target touch.
        ex = exit_on_bar(candle["Open"], candle["High"], candle["Low"], long=is_long,
                         entry=entry, stop=stop, target=target,
                         planned_r=float(rr) if rr else None)
        if ex is None:
            continue
        status, r, _fill, _gap = ex
        result, reason = ("LOSS", "SL_HIT") if status == "CLOSED_LOSS" else ("WIN", "TP_HIT")
        payload = _close_payload(result, reason, ts, signal, r)
        payload["meta"]["execution"] = exit_metadata(
            candle["Open"], candle["High"], candle["Low"], long=is_long, stop=stop, target=target, exit=ex)
        tf = signal.get("timeframe", "1D")
        outcome = compute_trade_outcome({**signal, "timeframe": tf, "status": status}, {tf: df})
        payload["meta"] = attach_outcome(payload["meta"], outcome)
        return payload
    return None


def resolve_outcome_with_daily(signal: dict, df: pd.DataFrame,
                               daily: pd.DataFrame | None) -> dict | None:
    """Resolve weekly/monthly trades after their entry close, including the forming HTF period."""
    tf = str(signal.get("timeframe") or "1W").upper()
    if tf not in ("1W", "1M") or daily is None or daily.empty:
        return resolve_outcome(signal, df)
    available = bar_end(pd.DatetimeIndex([signal["pattern_at"]]), tf)[0]
    # Exclude every daily bar that belongs to the signal's still-forming entry period.
    future = daily.loc[bar_end(daily.index, "1D") > available].copy()
    if future.empty:
        return None
    future.index = pd.to_datetime(future.index, utc=True)
    entry = float(signal.get("entry_price") or signal.get("cisd_level") or 0)
    if entry <= 0:
        return None
    # An entry event anchors the existing excursion calculation; it is never a held bar
    # and is never persisted as a market candle. Only subsequent daily bars can cause exits.
    anchor = pd.DataFrame({k: [entry] for k in ("Open", "High", "Low", "Close")},
                          index=pd.DatetimeIndex([available]))
    replay = pd.concat([anchor, future])
    view = {**signal, "timeframe": "1D", "pattern_at": available.isoformat()}
    update = resolve_outcome(view, replay)
    if update:
        live_seen = update["meta"].get("first_live_seen_at")
        if live_seen and bar_end(pd.DatetimeIndex([update["closed_at"]]), "1D")[0] < pd.Timestamp(live_seen):
            update["meta"].pop("first_live_seen_at", None)
            update["meta"]["origin"] = "reconstructed"
        update["meta"]["radar_eligible"] = False
        update["meta"]["radar_reason"] = "TRADE_CLOSED"
        update["meta"]["execution"]["resolution_timeframe"] = "1D"
        if update["meta"].get("outcome"):
            update["meta"]["outcome"]["resolution_timeframe"] = "1D"
    return update


def _close_payload(
    result: str, exit_reason: str, ts: pd.Timestamp, signal: dict, r_multiple: float | None = None
) -> dict[str, Any]:
    closed = ts.to_pydatetime()
    if closed.tzinfo is None:
        closed = closed.replace(tzinfo=timezone.utc)
    else:
        closed = closed.astimezone(timezone.utc)

    if r_multiple is None:
        rr = float(signal.get("rr_ratio") or 0)
        r_multiple = rr if result == "WIN" else -1.0
    meta = dict(signal.get("meta") or {})
    meta["resolver_version"] = RESOLVER_VERSION
    meta = attach_size_factor(meta, signal.get("entry_price") or signal.get("cisd_level"),
                              signal.get("stop_loss"))  # exposure cap (reporting only)

    return {
        "result": result,
        "exit_reason": exit_reason,
        "closed_at": closed.isoformat(),
        "is_active": False,
        "status": "CLOSED_WIN" if result == "WIN" else "CLOSED_LOSS",
        "r_multiple": r_multiple,
        "meta": meta,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def load_open(supabase: Client, limit: int | None = None) -> list[dict]:
    """Load all eligible rows before closing any, so pagination cannot skip moved rows."""
    rows, offset = [], 0
    while limit is None or len(rows) < limit:
        count = min(500, limit - len(rows)) if limit is not None else 500
        data = (supabase.table("devils_signals")
                .select("id, symbol, timeframe, direction, entry_price, cisd_level, "
                        "stop_loss, target, rr_ratio, pattern_at, created_at, result, status, meta")
                .eq("is_active", True).is_("result", "null")
                .eq("strategy", "pure_no_wick").eq("is_paper", False)
                .in_("status", list(OPEN_STATUSES)).order("id")
                .range(offset, offset + count - 1).execute().data or [])
        if not data:
            break
        rows.extend(data)
        offset += len(data)
    return rows


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
        if pattern_position(df, sig.get("pattern_at") or sig.get("created_at")) is None:
            logger.warning("  %s %s: entry bar missing from history; outcome left unresolved", symbol, tf)
            continue
        source_state = source_validation(sig, df)
        if source_state != "VERIFIED":
            logger.warning("  %s %s: %s; outcome left unresolved", symbol, tf, source_state)
            continue

        if tf in ("1W", "1M"):
            daily_key = (symbol, "1D")
            if daily_key not in cache:
                cache[daily_key] = bars_for_tf(symbol, "1D")
            update = resolve_outcome_with_daily(sig, df, cache[daily_key])
        else:
            update = resolve_outcome(sig, df)
        if not update:
            continue

        logger.info(
            f"  ✓ {symbol} {tf} → {update['result']} ({update['exit_reason']}) "
            f"R={update.get('r_multiple')}"
        )
        if not dry_run:
            (supabase.table("devils_signals").update(update).eq("id", sig["id"])
             .eq("is_active", True).is_("result", "null").execute())
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
