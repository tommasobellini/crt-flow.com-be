"""Repair closed pure_no_wick rows with the current versioned exit resolver.

Run by a human with the service-role key. A bar that OPENS beyond the stop fills at the open
(r < -1); a bar that OPENS beyond the target fills at the open (r > planned R).
An opening fill precedes intrabar touches. Saves R, fill metadata and exit bar time.
Current-version rows are skipped. Status changes require --repair-outcomes; otherwise
they are reported as mismatches. Context snapshots are preserved.

    python backfill_r_multiple.py --dry-run --limit 50
    python backfill_r_multiple.py --limit 60000 --batch 500
    python backfill_r_multiple.py --symbols AAPL,SPOT,NVDA --dry-run
"""
from __future__ import annotations

import argparse
import time
from collections import defaultdict

from devils_scanner import fetch_devils_frames, setup_supabase
from devils_signal_validation import PRICE_BASIS
from signal_context import pattern_position
from signal_outcome import attach_outcome, compute_trade_outcome
from strategy.devils_markers import resolve_trade_details_after_marker
from strategy.gap_fill import RESOLVER_VERSION

CLOSED = ("CLOSED_WIN", "CLOSED_LOSS")
COLS = "id,symbol,timeframe,direction,entry_price,stop_loss,target,rr_ratio,pattern_at,status,r_multiple,meta"
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


def is_current(meta) -> bool:
    return isinstance(meta, dict) and meta.get("resolver_version") == RESOLVER_VERSION


def recompute(row: dict, frames: dict):
    """(status, r_multiple) from the bars after pattern_at, or None (bar not in history /
    trade not closed in the frame). Never raises."""
    details = recompute_details(row, frames)
    return (details["status"], details["r_multiple"]) if details is not None else None


def recompute_details(row: dict, frames: dict):
    try:
        df = (frames or {}).get(row["timeframe"])
        if df is None or getattr(df, "empty", True):
            return None
        pos = pattern_position(df, row["pattern_at"])
        if pos is None:
            return None
        rr = row.get("rr_ratio")
        direction = "BULLISH" if str(row["direction"]).upper() in ("BULLISH", "LONG") else "BEARISH"
        details = resolve_trade_details_after_marker(
            df, pos, direction, row["stop_loss"], row["target"], row["entry_price"], float(rr) if rr else None)
        return details if details["status"] in CLOSED else None
    except Exception:
        return None


def fetch_rows(state, *, limit, batch, symbols=None):
    """Closed pure_no_wick rows not yet at the current resolver version."""
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
        out += [r for r in data if not is_current(r.get("meta"))]
        start += len(data)
    return out[:limit]


def run(sb, *, dry_run, limit, batch, symbols=None, fetch=fetch_devils_frames,
        new_client=setup_supabase, repair_outcomes=False):
    state = {"sb": sb, "new_client": new_client}
    rows = fetch_rows(state, limit=limit, batch=batch, symbols=symbols)
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[r["symbol"]].append(r)
    stats = {"rows": len(rows), "recomputed": 0, "changed": 0, "no_bars": 0, "mismatch": 0,
             "written": 0, "sum_r_old": 0.0, "sum_r_new": 0.0, "unsupported_basis": 0}
    for sym in sorted(by_sym):
        frames = fetch(sym) or {}
        for r in by_sym[sym]:
            if (r.get("meta") or {}).get("price_basis") != PRICE_BASIS:
                stats["unsupported_basis"] += 1
                continue
            res = recompute_details(r, frames)
            if res is None:
                stats["no_bars"] += 1
                continue
            status, r_new = res["status"], res["r_multiple"]
            if status != r["status"] and not repair_outcomes:
                stats["mismatch"] += 1
                continue
            stats["recomputed"] += 1
            r_old = float(r["r_multiple"]) if r.get("r_multiple") is not None else None
            stats["sum_r_old"] += r_old or 0.0
            stats["sum_r_new"] += r_new
            if r_old is None or abs(r_old - r_new) > 1e-9:
                stats["changed"] += 1
            meta = dict(r.get("meta") or {})
            meta["resolver_version"] = RESOLVER_VERSION
            meta["execution"] = res["execution"]
            # Old excursions may describe a different exit under the previous resolver.
            meta.pop("outcome", None)
            meta = attach_outcome(meta, compute_trade_outcome({**r, "status": status}, frames))
            payload = {"r_multiple": r_new, "closed_at": res["closed_at"], "meta": meta}
            if status != r["status"]:
                payload.update(status=status, result="WIN" if status == "CLOSED_WIN" else "LOSS",
                               exit_reason="TP_HIT" if status == "CLOSED_WIN" else "SL_HIT", is_active=False)
            if dry_run:
                if r_old is None or abs(r_old - r_new) > 1e-9:
                    print(f"[dry-run] id={r['id']} {sym} {r['timeframe']} {r['status']} r {r_old} -> {r_new}")
                continue
            _retry(lambda c, rid=r["id"], p=payload: c.table("devils_signals")
                   .update(p).eq("id", rid).execute(),
                   state, f"write id={r['id']}")
            stats["written"] += 1
        print(f"  {sym}: {len(by_sym[sym])} row(s)")
    stats["sum_r_old"] = round(stats["sum_r_old"], 4)
    stats["sum_r_new"] = round(stats["sum_r_new"], 4)
    return stats


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="read only, never writes")
    p.add_argument("--limit", type=int, default=1000)
    p.add_argument("--batch", type=int, default=500)
    p.add_argument("--symbols", default=None, help="comma-separated")
    p.add_argument("--repair-outcomes", action="store_true", help="also repair WIN/LOSS labels that changed")
    a = p.parse_args()
    syms = [s.strip().upper() for s in a.symbols.split(",")] if a.symbols else None
    stats = run(setup_supabase(), dry_run=a.dry_run, limit=a.limit, batch=a.batch, symbols=syms,
                repair_outcomes=a.repair_outcomes)
    print(f"{'DRY-RUN' if a.dry_run else 'LIVE'}: {stats}")


if __name__ == "__main__":
    main()
