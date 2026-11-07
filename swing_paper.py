"""Single-writer forward Swing 4H account. No historical imports or broker orders."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json

import pandas as pd

from devils_scanner import setup_supabase
from signal_context import bar_end
from strategy.config import TARGET_WATCHLIST
from strategy.swing_4h import STRATEGY, SwingConfig, candidates, utc
from strategy.swing_portfolio import initial_state, metrics, replay
from swing_research import chart_context, market_frames
from swing_storage import ensure_bucket, paper_lock, read_json, write_json


def scan(symbols, *, previous=None, config=None, as_of=None, cache_dir=None):
    cfg = config or SwingConfig()
    now = utc(as_of if as_of is not None else pd.Timestamp.now(tz="UTC"))
    symbols = sorted(set(symbols))
    if previous and (previous.get("strategy") != STRATEGY or previous["symbols"] != symbols
                     or previous["config_hash"] != cfg.fingerprint):
        raise ValueError("Universe/rules changed; an existing paper account cannot be overwritten")
    frames, errors, digests = market_frames(symbols, cache_dir=cache_dir, as_of=now)
    if errors:
        raise RuntimeError("Paper data incomplete: "+", ".join(sorted(errors)))
    latest = {bar_end(f["4H"].index[-1:], "4H")[0] for f in frames.values()}
    if len(latest) != 1 or now-next(iter(latest)) > pd.Timedelta(days=4):
        raise ValueError("Stale or unsynchronized 4H data; paper account was not changed")
    h4 = {s: f["4H"] for s, f in frames.items()}
    state = previous["state"] if previous else initial_state(cfg)
    if not previous:
        state["last_bar_at"] = max(f.index[-1] for f in h4.values()).isoformat()
        state["equity"] = [{"at": now.isoformat(), "equity": cfg.initial_capital,
                            "cash": cfg.initial_capital, "positions": 0}]
    signals = []
    today = now.tz_convert("America/New_York").date()
    for symbol, data in frames.items():
        for record in candidates(data["4H"], data["1D"], symbol, config=cfg, as_of=now):
            # Only today's newly confirmed evening setup. An older pattern is
            # never made into a fresh actionable signal on first startup.
            if utc(record["available_at"]).tz_convert("America/New_York").date() != today:
                continue
            record["first_seen_at"] = now.isoformat()
            signals.append(record)
    state = replay(h4, signals, config=cfg, state=state, as_of=now)
    journal = sorted(state["journal"].values(), key=lambda r: r["available_at"], reverse=True)
    for record in journal[:80]:
        if record["status"] in ("OPEN", "PENDING") or record.get("chart_closed") is not True:
            record["chart"] = chart_context(h4[record["symbol"]], record, cfg)
            record["chart_closed"] = record["status"] == "CLOSED"
    return {"strategy": STRATEGY, "config": asdict(cfg), "config_hash": cfg.fingerprint,
            "symbols": symbols, "started_at": previous["started_at"] if previous else now.isoformat(),
            "generated_at": now.isoformat(), "last_market_close": next(iter(latest)).isoformat(),
            "price_basis": "split_adjusted_no_dividends", "session": "US_RTH",
            "provider": "yahoo", "data_sha256": digests, "paper_only": True,
            "entry_model": "next_session_open_with_frozen_price_cap",
            "stats": metrics(state), "state": state, "journal": journal}


def persist_scan(sb, symbols, *, config=None, as_of=None, cache_dir=None):
    storage = ensure_bucket(sb)
    with paper_lock(storage):
        previous = read_json(storage, "paper/latest.json")
        try:
            report = scan(symbols, previous=previous, config=config, as_of=as_of, cache_dir=cache_dir)
        except Exception as error:
            write_json(storage, "paper/health.json", {
                "status": "error", "at": pd.Timestamp.now(tz="UTC").isoformat(),
                "reason": type(error).__name__}, replace=True)
            raise
        stamp = utc(report["generated_at"]).strftime("%Y%m%dT%H%M%S%fZ")
        write_json(storage, f"paper/snapshots/{stamp}.json", report)
        write_json(storage, "paper/latest.json", report, replace=True)
        write_json(storage, "paper/health.json", {"status":"ok", "at": report["generated_at"]}, replace=True)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default=",".join(TARGET_WATCHLIST))
    parser.add_argument("--cache-dir")
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--scheduled", action="store_true", help="Only scan after 16:00 New York on weekdays")
    args = parser.parse_args()
    now_ny = pd.Timestamp.now(tz="America/New_York")
    if args.scheduled and (now_ny.weekday() >= 5 or now_ny.hour < 16):
        print("Outside the evening decision window; account unchanged", flush=True)
        return
    symbols = sorted(set(s.strip().upper() for s in args.symbols.split(",") if s.strip()))
    if not symbols or any(not s.replace("-", "").isalnum() or len(s)>8 for s in symbols):
        parser.error("Invalid symbols")
    if args.persist:
        report = persist_scan(setup_supabase(), symbols, cache_dir=args.cache_dir)
    else:
        report = scan(symbols, cache_dir=args.cache_dir)
    print(json.dumps({"strategy": STRATEGY, "paper_only": True, "persisted": args.persist,
                      "generated_at": report["generated_at"],
                      "stats": {k: v for k, v in report["stats"].items() if k != "weekly"}}), flush=True)


if __name__ == "__main__":
    main()
