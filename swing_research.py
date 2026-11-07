"""Registered comparison of evening 4H pullbacks and 4H breakouts."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import numpy as np
import yfinance as yf

from devils_scanner import fetch_devils_frames, setup_supabase
from signal_context import bar_end
from strategy.config import TARGET_WATCHLIST
from strategy.swing_4h import BENCHMARK, OHLC, STRATEGY, SwingConfig, candidates, checked_frame, utc
from strategy.swing_portfolio import metrics, replay
from swing_storage import ensure_bucket, write_json


def load_frames(symbol, cache_dir=None):
    path = Path(cache_dir)/f"{symbol}.json.gz" if cache_dir else None
    if path is not None:
        # Explicit cache mode is frozen: missing files must never fetch replacements.
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            packed = json.load(stream)
        frames = {tf: pd.DataFrame(v["values"], columns=OHLC,
                                   index=pd.to_datetime(v["index"], utc=True))
                  for tf, v in packed.items() if tf in ("4H", "1D")}
        if "volume" in packed["1D"]:
            frames["1D"]["Volume"] = packed["1D"]["volume"]
        return frames
    fetched = fetch_devils_frames(symbol)
    return {tf: fetched.get(tf) for tf in ("4H", "1D")}


def prepare_cache(symbols, source, target):
    """Add today's historical daily-volume snapshot without changing archived OHLC."""
    target = Path(target)
    target.mkdir(parents=True, exist_ok=False)
    def job(symbol):
        with gzip.open(Path(source)/f"{symbol}.json.gz", "rt", encoding="utf-8") as stream:
            packed = json.load(stream)
        old = load_frames(symbol, source)
        fetched = yf.Ticker(symbol).history(period="5y", interval="1d", auto_adjust=False)
        fresh = checked_frame(fetched)
        volume = pd.Series(fetched.Volume.to_numpy(), index=fresh.index)
        overlap = old["1D"].index.intersection(fresh.index)
        if not len(overlap) or not np.allclose(old["1D"].loc[overlap, OHLC], fresh.loc[overlap, OHLC],
                                              rtol=1e-7, atol=1e-6):
            raise ValueError("Archived and current daily price bases disagree")
        aligned = volume.reindex(old["1D"].index)
        relevant = old["1D"].index >= old["4H"].index[0]-pd.Timedelta(days=60)
        if aligned.loc[relevant].isna().any():
            raise ValueError("Historical daily volume is incomplete")
        packed["1D"]["volume"] = [float(v) if pd.notna(v) else None for v in aligned]
        packed["volume_snapshot_at"] = pd.Timestamp.now(tz="UTC").isoformat()
        with gzip.open(target/f"{symbol}.json.gz", "wt", encoding="utf-8") as stream:
            json.dump(packed, stream, allow_nan=False)
        return symbol
    with ThreadPoolExecutor(max_workers=4) as pool:
        for symbol in pool.map(job, symbols):
            print("VOLUME_VERIFIED", symbol, flush=True)


def market_frames(symbols, *, cache_dir=None, as_of=None):
    cutoff = utc(as_of if as_of is not None else pd.Timestamp.now(tz="UTC"))
    frames, errors, digests = {}, {}, {}
    def job(symbol):
        raw = load_frames(symbol, cache_dir)
        h4, daily = checked_frame(raw["4H"]), checked_frame(raw["1D"])
        if "Volume" not in raw["1D"]:
            raise ValueError("Daily volume missing")
        daily["Volume"] = pd.to_numeric(raw["1D"].Volume, errors="coerce").to_numpy()
        h4 = h4.loc[bar_end(h4.index, "4H") <= cutoff]
        daily = daily.loc[bar_end(daily.index, "1D") <= cutoff]
        if len(h4) < 2 or len(daily) < 1:
            raise ValueError("Missing closed-bar history")
        return h4, daily
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {s: pool.submit(job, s) for s in symbols}
        for symbol, future in jobs.items():
            try:
                h4, daily = future.result()
                frames[symbol] = {"4H": h4, "1D": daily}
                digests[symbol] = {tf: hashlib.sha256(f.to_json(date_format="iso", double_precision=15).encode()).hexdigest()
                                   for tf, f in frames[symbol].items()}
            except Exception as error:
                errors[symbol] = type(error).__name__
    return frames, errors, digests


def chart_context(frame, record, config):
    ts = utc(record["pattern_at"])
    pos = frame.index.searchsorted(ts)
    tail = frame.iloc[max(0, pos-65):min(len(frame), pos+23)]
    fast = frame.Close.ewm(span=config.ema_fast, adjust=False).mean()
    slow = frame.Close.ewm(span=config.ema_slow, adjust=False).mean()
    return [{"time": int(t.timestamp()), "open": float(r.Open), "high": float(r.High),
             "low": float(r.Low), "close": float(r.Close), "ema_fast": float(fast.loc[t]),
             "ema_slow": float(slow.loc[t])} for t, r in tail.iterrows()]


def audit_prefixes(frames, signals, full_state, cfg, variant):
    """Rebuild signals and account equity with four independent future cutoffs."""
    h4 = {s: f["4H"] for s, f in frames.items()}
    timeline = sorted(set().union(*(set(f.index) for f in h4.values())))
    for fraction in (0.25, 0.5, 0.75, 1.0):
        last = timeline[min(len(timeline)-1, int((len(timeline)-1)*fraction))]
        cutoff = bar_end(pd.DatetimeIndex([last]), "4H")[0]
        known = [r for s, data in frames.items() for r in candidates(
            data["4H"], data["1D"], s, config=cfg, variant=variant, as_of=cutoff)]
        expected = [r for r in signals if utc(r["available_at"]) <= cutoff]
        if known != expected:
            raise AssertionError("Future data changed a historical swing candidate")
        prefix = replay(h4, known, config=cfg, as_of=cutoff)
        if prefix["equity"] != [e for e in full_state["equity"] if utc(e["at"]) <= cutoff]:
            raise AssertionError("Future data changed historical portfolio equity")
    return {"signal_and_portfolio_prefixes": 4, "passed": True}


def run_research(symbols, *, output, cache_dir=None, config=None, as_of=None):
    cfg = config or SwingConfig()
    cutoff = utc(as_of if as_of is not None else pd.Timestamp.now(tz="UTC"))
    output = Path(output)
    if output.exists() or output.with_suffix(".registration.json").exists():
        raise ValueError("Choose a new immutable research output")
    output.parent.mkdir(parents=True, exist_ok=True)
    manifest = {"strategy": STRATEGY, "benchmark": BENCHMARK, "config": asdict(cfg),
                "config_hash": cfg.fingerprint, "registered_at": cutoff.isoformat(),
                "symbols": sorted(set(symbols)), "entry_model": "next_session_open_with_frozen_price_cap",
                "selection_rule": "fixed watchlist; price >= 5; prior/current closed 20-day mean volume >= 1M shares",
                "price_basis": "split_adjusted_no_dividends", "provider": "yahoo",
                "session": "US_RTH", "parameter_search": False,
                "late_period_already_inspected": True, "forward_test_required": True,
                "survivorship_bias": "current fixed watchlist, not historical index membership",
                "dividends": "excluded; price-return simulation",
                "portfolio_priority": "alphabetical at simultaneous opens; no leverage",
                "execution_model": "paper only; conditional opening fill is not a broker guarantee"}
    manifest["source_sha256"] = {
        p: hashlib.sha256((Path(__file__).parent/p).read_bytes()).hexdigest()
        for p in ("strategy/swing_4h.py", "strategy/swing_portfolio.py", "swing_research.py")}
    output.with_suffix(".registration.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    frames, errors, digests = market_frames(manifest["symbols"], cache_dir=cache_dir, as_of=cutoff)
    if errors:
        raise RuntimeError("Incomplete research universe: "+", ".join(sorted(errors)))
    h4 = {s: f["4H"] for s, f in frames.items()}
    first = min(f.index[0] for f in h4.values())
    last = max(f.index[-1] for f in h4.values())
    split = first+(last-first)*0.8
    variants = {}
    for variant in (STRATEGY, BENCHMARK):
        signals = [signal for s, f in frames.items() for signal in candidates(
            f["4H"], f["1D"], s, config=cfg, variant=variant, as_of=cutoff)]
        state = replay(h4, signals, config=cfg, as_of=cutoff)
        audit = audit_prefixes(frames, signals, state, cfg, variant)
        earlier_frames = {s: f.loc[bar_end(f.index, "4H") < split] for s, f in h4.items()}
        earlier = replay(earlier_frames, [s for s in signals if utc(s["available_at"]) < split],
                         config=cfg, as_of=split)
        later = replay(h4, [s for s in signals if utc(s["available_at"]) >= split],
                       config=cfg, start_at=split, as_of=cutoff)
        stress_cfg = SwingConfig(**{**asdict(cfg), "slippage_bps": cfg.slippage_bps+5})
        # Candidate levels are identical; this is an execution-cost stress,
        # rather than a re-optimized signal model.
        stress = replay(h4, signals, config=stress_cfg, as_of=cutoff)
        variants[variant] = {"all": metrics(state), "earlier": metrics(earlier),
                             "recent": metrics(later), "stress": metrics(stress),
                             "equity": state["equity"],
                             "causality_audit": audit,
                             "journal": list(state["journal"].values())}
        print(variant, json.dumps({k: v for k, v in variants[variant]["all"].items()
                                  if k != "weekly"}), flush=True)
    own = variants[STRATEGY]
    records = sorted(own["journal"], key=lambda r: r["available_at"], reverse=True)
    # A bounded chart payload is enough for review without redownloading candles.
    for record in records[:80]:
        record["chart"] = chart_context(h4[record["symbol"]], record, cfg)
    report = {**manifest, "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
              "from": first.isoformat(), "to": last.isoformat(), "split_at": split.isoformat(),
              "data_sha256": digests, "errors": errors, "variants": variants,
              "warmup_only_symbols": [s for s, f in frames.items()
                                      if len(f["1D"]) < cfg.daily_sma or len(f["4H"]) < cfg.ema_slow],
              "assessment": {"positive_full_sample": own["all"]["return_pct"] > 0,
                             "positive_recent_sample": own["recent"]["return_pct"] > 0,
                             "recent_closed": own["recent"]["closed"],
                             "validated_for_live": False}}
    output.write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default=",".join(TARGET_WATCHLIST))
    parser.add_argument("--cache-dir")
    parser.add_argument("--prepare-cache-from", help="Copy frozen OHLC and attach verified daily-volume history")
    parser.add_argument("--output")
    parser.add_argument("--persist", action="store_true", help="Store research in the separate private Swing bucket")
    args = parser.parse_args()
    symbols = sorted(set(s.strip().upper() for s in args.symbols.split(",") if s.strip()))
    if not symbols or any(not s.replace("-", "").isalnum() or len(s) > 8 for s in symbols):
        parser.error("Invalid symbols")
    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    output = Path(args.output or f".local-backups/swing-4h/{stamp}/research.json")
    if args.prepare_cache_from:
        if not args.cache_dir:
            parser.error("--cache-dir is required with --prepare-cache-from")
        prepare_cache(symbols, args.prepare_cache_from, args.cache_dir)
    report = run_research(symbols, output=output, cache_dir=args.cache_dir)
    if args.persist:
        storage = ensure_bucket(setup_supabase())
        write_json(storage, f"research/{stamp}.json", report)
        write_json(storage, "research/latest.json", report, replace=True)
    print("REPORT", str(output.resolve()), flush=True)


if __name__ == "__main__":
    main()
