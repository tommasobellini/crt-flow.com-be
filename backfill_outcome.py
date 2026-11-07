"""Fill devils_signals.meta.outcome (mae_r, mfe_r, bars_held) for closed rows.

Run by a human with the service-role key. Only `meta` is updated: existing keys (ctx,
ctx_version, ...) are kept and `outcome` is added. Rows already at the current outcome version
are skipped, so the script can be re-run after an interruption. Uses the same candles as
the scanner (devils_scanner.fetch_devils_frames).

    python backfill_outcome.py --dry-run --limit 50
    python backfill_outcome.py --limit 60000 --batch 500
    python backfill_outcome.py --symbols AAPL,SPOT,NVDA --dry-run
"""
from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict

from devils_scanner import fetch_devils_frames, setup_supabase
from devils_signal_validation import PRICE_BASIS
from signal_outcome import CLOSED, attach_outcome, compute_trade_outcome, has_outcome

COLS = "id,symbol,timeframe,direction,entry_price,stop_loss,target,pattern_at,status,meta"
RETRIES = 4


def _retry(fn, state, what):
    """Run fn(sb); on a dropped connection (httpx RemoteProtocolError etc.) build a new
    client and retry."""
    for attempt in range(RETRIES):
        try:
            return fn(state["sb"])
        except Exception as e:  # httpx RemoteProtocolError / ConnectionTerminated / ReadError
            if attempt == RETRIES - 1:
                raise
            print(f"  ! {what} failed ({type(e).__name__}), new client, retry {attempt + 1}")
            time.sleep(2 * (attempt + 1))
            state["sb"] = state["new_client"]()


def fetch_rows(state, *, limit, batch, symbols=None):
    """Closed pure_no_wick rows without the current meta.outcome version."""
    out, start = [], 0
    while len(out) < limit:
        def q(sb, start=start):
            qq = (sb.table("devils_signals").select(COLS).eq("strategy", "pure_no_wick")
                  .in_("status", list(CLOSED)).order("id").range(start, start + batch - 1))
            if symbols:
                qq = qq.in_("symbol", symbols)
            return qq.execute().data or []
        data = _retry(q, state, "read")
        if not data:
            break
        out += [r for r in data if not has_outcome(r.get("meta"))]
        start += len(data)
    return out[:limit]


def run(sb, *, dry_run, limit, batch, symbols=None, fetch=fetch_devils_frames, new_client=setup_supabase):
    state = {"sb": sb, "new_client": new_client}
    rows = fetch_rows(state, limit=limit, batch=batch, symbols=symbols)
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[r["symbol"]].append(r)
    stats = {"rows": len(rows), "with_outcome": 0, "no_outcome": 0, "written": 0, "unsupported_basis": 0}
    for sym in sorted(by_sym):
        frames = fetch(sym) or {}
        for r in by_sym[sym]:
            if (r.get("meta") or {}).get("price_basis") != PRICE_BASIS:
                stats["unsupported_basis"] += 1
                continue
            oc = compute_trade_outcome(r, frames)
            if oc is None:  # pattern bar outside history, or bars disagree with stored status
                stats["no_outcome"] += 1
                continue
            stats["with_outcome"] += 1
            meta = attach_outcome(r.get("meta"), oc)
            if dry_run:
                print(f"[dry-run] id={r['id']} {sym} {r['timeframe']} {r['status']} outcome={json.dumps(oc, sort_keys=True)}")
                continue
            _retry(lambda c, rid=r["id"], m=meta: c.table("devils_signals").update({"meta": m}).eq("id", rid).execute(),
                   state, f"write id={r['id']}")
            stats["written"] += 1
        print(f"  {sym}: {len(by_sym[sym])} row(s)")
    return stats


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="read only, never writes")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--batch", type=int, default=500)
    p.add_argument("--symbols", default=None, help="comma-separated")
    a = p.parse_args()
    syms = [s.strip().upper() for s in a.symbols.split(",")] if a.symbols else None
    stats = run(setup_supabase(), dry_run=a.dry_run, limit=a.limit, batch=a.batch, symbols=syms)
    print(f"{'DRY-RUN' if a.dry_run else 'LIVE'}: {stats}")


if __name__ == "__main__":
    main()
