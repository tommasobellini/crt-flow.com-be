"""Separate experimental paper recorder. Default is dry-run; never places orders."""
from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict
import json
from math import isclose

import pandas as pd

from devils_scanner import setup_supabase
from devils_signal_validation import PRICE_BASIS
from devils_sweep_cisd_research import evaluate_entry, load_frames, summarize
from signal_context import bar_end, pattern_position
from signal_outcome import attach_outcome, compute_trade_outcome
from strategy.config import TARGET_WATCHLIST
from strategy.devils_sweep_cisd import STRATEGY, SweepCisdConfig, checked_frame, find_entries, previous_day_levels
from strategy.gap_fill import RESOLVER_VERSION
from strategy.sizing import attach_size_factor


def _clock():
    return pd.Timestamp.now(tz="UTC")


def source_snapshots(signal: dict) -> list[dict]:
    setup = signal["setup"]
    snapshots = [*setup["cisd_source_candles"], setup["sweep_candle"],
                 setup["marker_candle"], setup["cisd_candle"]]
    return list({c["at"]: c for c in snapshots if c}.values())


def verify_source(row: dict, four_hour: pd.DataFrame) -> bool:
    meta = row.get("meta") or {}
    if meta.get("price_basis") != PRICE_BASIS or not meta.get("source_snapshots"):
        return False
    frame = checked_frame(four_hour)
    for saved in meta["source_snapshots"]:
        pos = pattern_position(frame, saved["at"])
        if pos is None:
            return False
        current = frame.iloc[pos]
        if not all(isclose(float(saved[key.lower()]), float(current[key]), rel_tol=1e-9, abs_tol=1e-8)
                   for key in ("Open", "High", "Low", "Close")):
            return False
    return True


def to_row(signal: dict, frame: pd.DataFrame, config: SweepCisdConfig, *, first_seen=None) -> dict:
    trade = evaluate_entry(signal, frame)
    now = first_seen or _clock().isoformat()
    closed = trade["status"] != "ACTIVE"
    meta = {"strategy": STRATEGY, "paper": True, "rule_version": 1,
            "config": asdict(config), "config_hash": config.fingerprint,
            "price_basis": PRICE_BASIS, "provider": "yahoo", "session": "US_RTH",
            "entry_basis": "signal_close", "signal_available_at": signal["signal_available_at"],
            "first_seen_at": now, "origin": "reconstructed" if closed else "scanner",
            "resolver_version": RESOLVER_VERSION, "setup": deepcopy(signal["setup"]),
            "source_snapshots": source_snapshots(signal), "source_verified": True,
            "radar_eligible": False, "cost_model": {"fee_bps_per_side":5.0, "slippage_bps_per_side":0.0}}
    if not closed:
        meta["first_live_seen_at"] = now
    if trade.get("execution"):
        meta["execution"] = trade["execution"]
        meta["estimated_cost_r"] = trade["cost_r"]
        meta["estimated_net_r"] = trade["net_r"]
    meta = attach_size_factor(meta, signal["entry_price"], signal["stop_loss"])
    meta = attach_outcome(meta, compute_trade_outcome({**trade, "timeframe":"4H"}, {"4H":frame}))
    return {"symbol":signal["ticker"], "timeframe":"4H", "direction":signal["direction"],
            "devils_marker_price":signal["devils_marker_price"], "entry_price":signal["entry_price"],
            "cisd_level":signal["cisd_level"], "is_cisd_broken":True,
            "stop_loss":signal["stop_loss"], "target":signal["target"], "rr_ratio":signal["rr_ratio"],
            "pattern_at":signal["pattern_at"], "status":trade["status"], "is_active":not closed,
            "r_multiple":trade["r_multiple"], "closed_at":trade["closed_at"],
            "result":None if not closed else "WIN" if trade["status"] == "CLOSED_WIN" else "LOSS",
            "exit_reason":None if not closed else "TP_HIT" if trade["status"] == "CLOSED_WIN" else "SL_HIT",
            "strategy":STRATEGY, "is_paper":True, "meta":meta, "updated_at":now}


def refresh_open(sb, symbol: str, frame: pd.DataFrame, *, dry_run: bool) -> int:
    # One strategy owns these rows; production and frozen v1 records are never selected.
    rows = (sb.table("devils_signals").select("*").eq("strategy", STRATEGY).eq("is_paper", True)
            .eq("symbol", symbol).eq("is_active", True).is_("result", "null").execute().data or [])
    closed = 0
    for row in rows:
        if not verify_source(row, frame):
            if not dry_run:
                meta = {**row["meta"], "source_verified":False, "source_reason":"SOURCE_REVISED_OR_MISSING",
                        "source_checked_at":_clock().isoformat()}
                (sb.table("devils_signals").update({"meta":meta}).eq("id", row["id"])
                 .eq("is_active", True).is_("result", "null").execute())
            continue
        # Use the saved trade's frozen levels, even after the strategy code changes.
        signal = {**row, "ticker":row["symbol"]}
        trade = evaluate_entry(signal, frame)
        verified_meta = {**row["meta"], "source_verified":True, "source_checked_at":_clock().isoformat()}
        verified_meta.pop("source_reason", None)
        if trade["status"] == "ACTIVE":
            if not dry_run:
                (sb.table("devils_signals").update({"meta":verified_meta}).eq("id", row["id"])
                 .eq("is_active", True).is_("result", "null").execute())
            continue
        meta = {**verified_meta, "resolver_version":RESOLVER_VERSION,
                "execution":trade["execution"], "estimated_cost_r":trade["cost_r"],
                "estimated_net_r":trade["net_r"]}
        meta = attach_outcome(meta, compute_trade_outcome({**trade, "timeframe":"4H"}, {"4H":frame}))
        payload = {"status":trade["status"], "result":"WIN" if trade["status"] == "CLOSED_WIN" else "LOSS",
                   "exit_reason":"TP_HIT" if trade["status"] == "CLOSED_WIN" else "SL_HIT",
                   "is_active":False, "r_multiple":trade["r_multiple"], "closed_at":trade["closed_at"],
                   "meta":meta, "updated_at":_clock().isoformat()}
        if not dry_run:
            (sb.table("devils_signals").update(payload).eq("id", row["id"])
             .eq("is_active", True).is_("result", "null").execute())
        closed += 1
    return closed


def paper_stats(sb) -> dict:
    rows, offset = [], 0
    while True:
        data = (sb.table("devils_signals").select("*").eq("strategy", STRATEGY).eq("is_paper", True)
                .order("id").range(offset, offset+499).execute().data or [])
        if not data:
            break
        rows.extend(data)
        offset += len(data)
    trades = [{**r, "ticker":r["symbol"], "net_r":r["meta"].get("estimated_net_r"),
               "cost_r":r["meta"].get("estimated_cost_r"),
               "sized_net_r":(r["meta"].get("estimated_net_r") or 0)*(r["meta"].get("size_factor") or 0),
               "execution":r["meta"].get("execution")} for r in rows]
    return {"strategy":STRATEGY, "paper":True, "stats":summarize(trades),
            "unverified_open":sum(bool(r.get("is_active")) and not r["meta"].get("source_verified") for r in rows),
            "rule_hashes":sorted({r["meta"].get("config_hash", "unknown") for r in rows})}


def scan_paper(symbol: str, *, since, sb=None, dry_run=True, as_of=None, frames=None) -> dict:
    since = pd.Timestamp(since)
    if pd.isna(since):
        raise ValueError("A valid paper start time is required")
    since = since.tz_localize("UTC") if since.tzinfo is None else since.tz_convert("UTC")
    if not dry_run and sb is None:
        raise ValueError("A database client is required for persistence")
    h4, daily = frames if frames is not None else load_frames(symbol)
    h4 = checked_frame(h4)
    config = SweepCisdConfig()
    cutoff = _clock() if as_of is None else pd.Timestamp(as_of)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    h4 = h4.loc[bar_end(h4.index, "4H") <= cutoff]
    if h4.empty or cutoff-bar_end(h4.index[-1:], "4H")[0] > pd.Timedelta(days=7):
        raise ValueError("Stale or missing closed 4H market data")
    scan = find_entries(h4, previous_day_levels(h4, daily), symbol, config=config, as_of=cutoff)
    closed = refresh_open(sb, symbol, h4, dry_run=dry_run) if sb is not None else 0
    rows = []
    for signal in scan["entries"]:
        if pd.Timestamp(signal["signal_available_at"]) < since:
            continue
        pos = pattern_position(h4, signal["pattern_at"])
        if pos is None or pos < len(h4)-3:
            continue  # no historical backfill on a fresh/reset paper account
        row = to_row(signal, h4, config)
        rows.append(row)
        if not dry_run:
            # A concurrent rescan cannot replace an existing entry or reopen a closed row.
            sb.table("devils_signals").upsert(row, ignore_duplicates=True,
                on_conflict="strategy,symbol,timeframe,direction,pattern_at").execute()
    return {"symbol":symbol, "new_candidates":rows, "closed_existing":closed,
            "setup_states":[s for s in scan["setups"] if s["status"] in ("WAITING_MARKER","WAITING_CISD")]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbols", default=",".join(TARGET_WATCHLIST))
    p.add_argument("--since", help="Required for scans: paper start time; never backfill older entries")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--persist", action="store_true", help="Write only experimental paper rows")
    mode.add_argument("--dry-run", action="store_true", help="Default: no database writes")
    mode.add_argument("--stats", action="store_true", help="Read only this experimental strategy's paper statistics")
    args = p.parse_args()
    if args.stats:
        print(json.dumps(paper_stats(setup_supabase()), indent=2, allow_nan=False))
        return
    if not args.since:
        p.error("--since is required when scanning")
    try:
        since = pd.Timestamp(args.since)
        if pd.isna(since): raise ValueError()
    except (ValueError, TypeError):
        p.error("--since must be a valid date or ISO timestamp")
    symbols = sorted(set(s.strip().upper() for s in args.symbols.split(",") if s.strip()))
    if not symbols or any(not s.replace("-", "").isalnum() or len(s)>8 for s in symbols):
        p.error("Invalid ticker list")
    sb = setup_supabase() if args.persist else None
    errors = 0
    for symbol in symbols:
        try:
            result = scan_paper(symbol, since=since, sb=sb, dry_run=not args.persist)
            print(json.dumps({"symbol":symbol,"new_candidates":len(result["new_candidates"]),
                              "closed_existing":result["closed_existing"],"pending_setups":len(result["setup_states"])}), flush=True)
        except Exception as exc:
            errors += 1
            print(f"{symbol}: paper error ({type(exc).__name__})", flush=True)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
