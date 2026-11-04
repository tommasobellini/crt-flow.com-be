"""Recompute devils_signals.r_multiple for closed rows with the v2 resolver (gap fill).

Run by a human with the service-role key. A bar that OPENS beyond the stop fills at the open
(r < -1); a bar that OPENS beyond the target fills at the open (r > planned R). Same bar
SL+TP stays a loss. Updates `r_multiple` and `meta.resolver_version = 2` (other meta keys are
kept). Rows already at resolver_version 2 are skipped, so the script can be re-run after an
interruption. Rows whose recomputed status differs from the stored one are left untouched and
counted as `mismatch`. Uses the same candles as the scanner (devils_scanner.fetch_devils_frames).

    python backfill_r_multiple.py --dry-run --limit 50
    python backfill_r_multiple.py --limit 60000 --batch 500
    python backfill_r_multiple.py --symbols AAPL,SPOT,NVDA --dry-run
"""
from __future__ import annotations

import argparse
import time
from collections import defaultdict

from devils_scanner import fetch_devils_frames, setup_supabase
from signal_context import pattern_position
from strategy.gap_fill import RESOLVER_VERSION, exit_on_bar

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


def is_v2(meta) -> bool:
    return isinstance(meta, dict) and meta.get("resolver_version") == RESOLVER_VERSION


def recompute(row: dict, frames: dict):
    """(status, r_multiple) from the bars after pattern_at, or None (bar not in history /
    trade not closed in the frame). Never raises."""
    try:
        df = (frames or {}).get(row["timeframe"])
        if df is None or getattr(df, "empty", True):
            return None
        pos = pattern_position(df, row["pattern_at"])
        if pos is None:
            return None
        long = str(row["direction"]).upper() in ("BULLISH", "LONG")
        rr = row.get("rr_ratio")
        after = df.iloc[pos + 1:]
        for o, h, l in zip(after["Open"], after["High"], after["Low"]):
            ex = exit_on_bar(o, h, l, long=long, entry=row["entry_price"], stop=row["stop_loss"],
                             target=row["target"], planned_r=float(rr) if rr else None)
            if ex is not None:
                return ex[0], ex[1]
        return None
    except Exception:
        return None


def fetch_rows(state, *, limit, batch, symbols=None):
    """Closed pure_no_wick rows not yet at resolver_version 2."""
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
        out += [r for r in data if not is_v2(r.get("meta"))]
        start += batch
    return out[:limit]


def run(sb, *, dry_run, limit, batch, symbols=None, fetch=fetch_devils_frames, new_client=setup_supabase):
    state = {"sb": sb, "new_client": new_client}
    rows = fetch_rows(state, limit=limit, batch=batch, symbols=symbols)
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[r["symbol"]].append(r)
    stats = {"rows": len(rows), "recomputed": 0, "changed": 0, "no_bars": 0, "mismatch": 0,
             "written": 0, "sum_r_old": 0.0, "sum_r_new": 0.0}
    for sym in sorted(by_sym):
        frames = fetch(sym) or {}
        for r in by_sym[sym]:
            res = recompute(r, frames)
            if res is None:
                stats["no_bars"] += 1
                continue
            status, r_new = res
            if status != r["status"]:
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
            if dry_run:
                if r_old is None or abs(r_old - r_new) > 1e-9:
                    print(f"[dry-run] id={r['id']} {sym} {r['timeframe']} {r['status']} r {r_old} -> {r_new}")
                continue
            _retry(lambda c, rid=r["id"], m=meta, v=r_new: c.table("devils_signals")
                   .update({"r_multiple": v, "meta": m}).eq("id", rid).execute(),
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
    a = p.parse_args()
    syms = [s.strip().upper() for s in a.symbols.split(",")] if a.symbols else None
    stats = run(setup_supabase(), dry_run=a.dry_run, limit=a.limit, batch=a.batch, symbols=syms)
    print(f"{'DRY-RUN' if a.dry_run else 'LIVE'}: {stats}")


if __name__ == "__main__":
    main()
