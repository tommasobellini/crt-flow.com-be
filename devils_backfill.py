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

from devils_scanner import TIMEFRAMES, fetch_devils_frames, signal_to_row, upsert_signal
from devils_outcome_resolver import setup_logging, setup_supabase
from strategy.devils_markers import find_devils_markers


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--symbol", default=None)
    p.add_argument("--symbols", default=None, help="comma-separated list (default: symbols already in devils_signals)")
    a = p.parse_args()
    sb = setup_supabase()
    if a.symbol:
        syms = [a.symbol]
    elif a.symbols:
        syms = a.symbols.split(",")
    else:
        rows = sb.table("devils_signals").select("symbol").limit(5000).execute().data or []
        syms = sorted({r["symbol"] for r in rows})
    total = {"ACTIVE": 0, "CLOSED_WIN": 0, "CLOSED_LOSS": 0}
    missing: list[str] = []
    for t in syms:
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
        for tf in TIMEFRAMES:
            df = frames.get(tf)
            if df is None or getattr(df, "empty", True):
                continue
            for m in find_devils_markers(df, ticker=t, timeframe=tf, recent_candles=len(df)):
                if m["status"] not in total or not m.get("is_high_rr"):
                    continue
                total[m["status"]] += 1
                upsert_signal(sb, signal_to_row(m), dry_run=a.dry_run)
        print(f"  {t}: {sum(total.values()) - before} marker(s)")
    print(f"Backfill {'DRY-RUN' if a.dry_run else 'LIVE'}: {total}")
    print(f"Symbols: {len(syms)}, without data: {len(missing)}")
    if missing:
        print("Rerun only these with: --symbols " + ",".join(missing))


if __name__ == "__main__":
    main()
