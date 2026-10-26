"""
One-off history recalculation for pure Devil's Markers (run AFTER the migration).

Re-scans the full history of each symbol/timeframe, so markers that were never
saved (losses) are recorded with their real outcome. Existing rows are upserted
on the new unique key (strategy, symbol, timeframe, direction, pattern_at).

    python devils_backfill.py --dry-run
    python devils_backfill.py --symbol AAPL
"""
from __future__ import annotations

import argparse
import time

from devils_scanner import TIMEFRAMES, fetch_devils_frames, get_universe, signal_to_row
from devils_outcome_resolver import setup_logging, setup_supabase
from strategy.devils_markers import find_devils_markers


KEY = ("timeframe", "direction", "pattern_at")


def _write_symbol(sb, symbol, rows, dry_run):
    """One select + chunked upserts per symbol (instead of 2 requests per signal).
    Rebuilds the client and retries when Supabase drops the HTTP/2 connection."""
    if dry_run or not rows:
        return sb
    for attempt in range(4):
        try:
            closed = (
                sb.table("devils_signals").select("timeframe,direction,pattern_at,result,status")
                .eq("strategy", "pure_no_wick").eq("symbol", symbol).execute().data or []
            )
            done = {tuple(str(r[k]) for k in KEY) for r in closed
                    if r.get("result") in ("WIN", "LOSS") or r.get("status") in ("CLOSED_WIN", "CLOSED_LOSS")}
            todo = [r for r in rows if tuple(str(r[k]) for k in KEY) not in done]
            for i in range(0, len(todo), 500):
                sb.table("devils_signals").upsert(
                    todo[i:i + 500], on_conflict="strategy,symbol,timeframe,direction,pattern_at"
                ).execute()
            return sb
        except Exception as e:  # httpx RemoteProtocolError / ConnectionTerminated
            if attempt == 3:
                raise
            print(f"  ! write failed ({type(e).__name__}), new client, retry {attempt + 1}")
            time.sleep(2 * (attempt + 1))
            sb = setup_supabase()
    return sb


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--symbol", default=None)
    p.add_argument("--symbols", default=None, help="comma-separated list (default: scanner universe S&P 500 + NASDAQ 100)")
    p.add_argument("--start-from", default=None, help="skip symbols before this one (resume)")
    a = p.parse_args()
    sb = setup_supabase()
    if a.symbol:
        syms = [a.symbol]
    elif a.symbols:
        syms = a.symbols.split(",")
    else:
        syms = get_universe()  # same S&P 500 + NASDAQ 100 universe as the live scanner
    if a.start_from:
        syms = syms[syms.index(a.start_from.upper()):]
    total = {"ACTIVE": 0, "CLOSED_WIN": 0, "CLOSED_LOSS": 0}
    missing: list[str] = []
    for t in syms:
        sb = setup_supabase()  # fresh connection per symbol
        frames = {}
        for attempt in range(3):  # Yahoo throttles long runs: retry before giving up
            frames = fetch_devils_frames(t) or {}
            if any(df is not None and not getattr(df, "empty", True) for df in frames.values()):
                break
            time.sleep(5 * (attempt + 1))
        else:
            missing.append(t)
            print(f"  ! {t}: no data from Yahoo")
            continue
        before = sum(total.values())
        rows = []
        for tf in TIMEFRAMES:
            df = frames.get(tf)
            if df is None or getattr(df, "empty", True):
                continue
            for m in find_devils_markers(df, ticker=t, timeframe=tf, recent_candles=len(df)):
                if m["status"] not in total or not m.get("is_high_rr"):
                    continue
                total[m["status"]] += 1
                rows.append(signal_to_row(m))
        sb = _write_symbol(sb, t, rows, a.dry_run)
        print(f"  {t}: {sum(total.values()) - before} marker(s)")
    print(f"Backfill {'DRY-RUN' if a.dry_run else 'LIVE'}: {total}")
    print(f"Symbols: {len(syms)}, without data: {len(missing)}")
    if missing:
        print("Rerun only these with: --symbols " + ",".join(missing))


if __name__ == "__main__":
    main()
