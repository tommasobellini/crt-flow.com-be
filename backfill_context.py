"""Fill devils_signals.meta.ctx for existing rows (run by a human with the service-role key).

Only the `meta` column is updated (existing meta keys are kept, ctx + ctx_version are added);
no other column is touched. Rows are read in batches, grouped by symbol, and the context is
computed with signal_context.compute_signal_context(), the same code as the live scanner.

    python backfill_context.py --dry-run --limit 50     # read only, prints the ctx it would write
    python backfill_context.py --limit 5000             # writes meta for up to 5000 rows
    python backfill_context.py --symbols AAPL,SPOT --include-open
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict

from devils_scanner import fetch_devils_frames, setup_supabase
from signal_context import CTX_VERSION, attach_context, compute_signal_context

COLS = "id,symbol,timeframe,direction,entry_price,stop_loss,target,devils_marker_price,pattern_at,status,meta"


def fetch_rows(sb, *, limit, batch, symbols=None, include_open=False):
    """Rows of pure_no_wick whose meta has no ctx for the current CTX_VERSION."""
    out, start = [], 0
    while len(out) < limit:
        q = (sb.table("devils_signals").select(COLS).eq("strategy", "pure_no_wick")
             .order("id").range(start, start + batch - 1))
        if not include_open:
            q = q.in_("status", ["CLOSED_WIN", "CLOSED_LOSS"])
        if symbols:
            q = q.in_("symbol", symbols)
        data = q.execute().data or []
        if not data:
            break
        out += [r for r in data if (r.get("meta") or {}).get("ctx_version") != CTX_VERSION]
        start += batch
    return out[:limit]


def run(sb, *, dry_run, limit, batch, symbols=None, include_open=False, fetch=fetch_devils_frames):
    rows = fetch_rows(sb, limit=limit, batch=batch, symbols=symbols, include_open=include_open)
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[r["symbol"]].append(r)
    stats = {"rows": len(rows), "with_ctx": 0, "no_ctx": 0, "written": 0}
    for sym in sorted(by_sym):
        frames = fetch(sym) or {}
        for r in by_sym[sym]:
            ctx = compute_signal_context(r, frames)
            if ctx is None:  # pattern bar outside the downloaded history (e.g. 4H older than 730d)
                stats["no_ctx"] += 1
                continue
            stats["with_ctx"] += 1
            meta = attach_context(r.get("meta") or {}, ctx)
            if dry_run:
                print(f"[dry-run] id={r['id']} {sym} {r['timeframe']} {r['pattern_at']} ctx={json.dumps(ctx, sort_keys=True)}")
                continue
            sb.table("devils_signals").update({"meta": meta}).eq("id", r["id"]).execute()
            stats["written"] += 1
        print(f"  {sym}: {len(by_sym[sym])} row(s)")
    return stats


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="read only, never writes")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--batch", type=int, default=500)
    p.add_argument("--symbols", default=None, help="comma-separated")
    p.add_argument("--include-open", action="store_true", help="also ACTIVE/OPEN rows")
    a = p.parse_args()
    sb = setup_supabase()
    syms = [s.strip().upper() for s in a.symbols.split(",")] if a.symbols else None
    stats = run(sb, dry_run=a.dry_run, limit=a.limit, batch=a.batch, symbols=syms, include_open=a.include_open)
    print(f"{'DRY-RUN' if a.dry_run else 'LIVE'}: {stats}")


if __name__ == "__main__":
    main()
