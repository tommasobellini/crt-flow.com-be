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
    for t in syms:
        frames = fetch_devils_frames(t)
        for tf in TIMEFRAMES:
            df = frames.get(tf)
            if df is None or getattr(df, "empty", True):
                continue
            for m in find_devils_markers(df, ticker=t, timeframe=tf, recent_candles=len(df)):
                if m["status"] not in total or not m.get("is_high_rr"):
                    continue
                total[m["status"]] += 1
                upsert_signal(sb, signal_to_row(m), dry_run=a.dry_run)
    print(f"Backfill {'DRY-RUN' if a.dry_run else 'LIVE'}: {total}")


if __name__ == "__main__":
    main()
