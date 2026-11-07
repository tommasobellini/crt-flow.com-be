"""Read-only A/B/C research for the experimental 4H sweep -> Devil -> CISD rule."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
from math import isfinite
from pathlib import Path

import pandas as pd

from devils_scanner import fetch_devils_frames
from signal_context import bar_end, pattern_position
from strategy.config import TARGET_WATCHLIST
from strategy.devils_markers import find_devils_markers, resolve_trade_details_after_marker
from strategy.devils_sweep_cisd import (
    OHLC, STRATEGY, SweepCisdConfig, checked_frame, find_entries, previous_day_levels,
)
from strategy.sizing import size_factor

VARIANTS = ("pure_no_wick", "sweep_cisd", STRATEGY)


def evaluate_entry(signal: dict, frame: pd.DataFrame, *, fee_bps=5.0, slippage_bps=0.0) -> dict:
    if not all(isfinite(v) and v >= 0 for v in (fee_bps, slippage_bps)):
        raise ValueError("Execution costs cannot be negative")
    frame = checked_frame(frame)
    pos = pattern_position(frame, signal["pattern_at"])
    if pos is None:
        raise ValueError("Entry source bar missing from replay")
    details = resolve_trade_details_after_marker(
        frame, pos, signal["direction"], signal["stop_loss"], signal["target"],
        signal["entry_price"], signal["rr_ratio"])
    status = "ACTIVE" if details["status"] == "OPEN" else details["status"]
    result = {**signal, **details, "status": status, "net_r": None, "cost_r": None,
              "sized_net_r": None, "entry_basis": "signal_close"}
    if status == "ACTIVE":
        return result
    risk = abs(signal["entry_price"] - signal["stop_loss"])
    fill = details["execution"]["exit_price"]
    cost = (signal["entry_price"] + fill) * (fee_bps + slippage_bps) / 10_000 / risk
    result.update(cost_r=cost, net_r=float(details["r_multiple"]) - cost)
    result["sized_net_r"] = result["net_r"] * size_factor(signal["entry_price"], signal["stop_loss"])
    return result


def summarize(trades: list[dict]) -> dict:
    closed = sorted((t for t in trades if t.get("net_r") is not None),
                    key=lambda t: (t["closed_at"], t["ticker"], t["pattern_at"]))
    net = [t["net_r"] for t in closed]
    gain, loss = sum(max(0, r) for r in net), -sum(min(0, r) for r in net)
    balance = peak = drawdown = 0.0
    for value in net:
        balance += value
        peak = max(peak, balance)
        drawdown = min(drawdown, balance - peak)
    return {"entries": len(trades), "closed": len(closed), "open": len(trades) - len(closed),
            "closed_tickers": len({t["ticker"] for t in closed}),
            "net_win_rate": sum(r > 0 for r in net) / len(net) if net else None,
            "gross_r": sum(t["r_multiple"] for t in closed), "cost_r": sum(t["cost_r"] for t in closed),
            "net_r": sum(net), "avg_net_r": sum(net) / len(net) if net else None,
            "profit_factor_net": gain / loss if loss > 0 else None,
            "max_drawdown_signal_r": drawdown,
            "sized_net_r": sum(t["sized_net_r"] for t in closed),
            "ambiguous_exits": sum(t["execution"]["ambiguous_exit_bar"] for t in closed)}


def partition(trades: list[dict], split_at: str) -> dict:
    split = pd.Timestamp(split_at)
    pre, held_out, crossing = [], [], []
    for trade in trades:
        available = pd.Timestamp(trade["signal_available_at"])
        if available >= split:
            held_out.append(trade)
        elif trade.get("closed_at") and bar_end(pd.DatetimeIndex([trade["closed_at"]]), "4H")[0] < split:
            pre.append(trade)
        else:
            crossing.append(trade)
    return {"before_split": summarize(pre), "held_out": summarize(held_out),
            "crossing_split": summarize(crossing)}


def compare_symbol(symbol, four_hour, daily, *, config=None, as_of=None,
                   fee_bps=5.0, slippage_bps=0.0) -> dict:
    config = config or SweepCisdConfig()
    frame = checked_frame(four_hour)
    cutoff = pd.Timestamp.now(tz="UTC") if as_of is None else pd.Timestamp(as_of)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    frame = frame.loc[bar_end(frame.index, "4H") <= cutoff]
    if len(frame) <= config.atr_period:
        raise ValueError("Insufficient closed 4H history")
    levels = previous_day_levels(frame, daily)
    plain = find_devils_markers(frame, ticker=symbol, timeframe="4H",
                                recent_candles=len(frame), include_invalidated=True)
    candidates = {"pure_no_wick": [t for t in plain if t.get("is_high_rr")]}
    diagnostics, rejected = {}, {}
    for name, require in (("sweep_cisd", False), (STRATEGY, True)):
        scan = find_entries(frame, levels, symbol, config=config, require_marker=require, as_of=cutoff)
        candidates[name] = scan["entries"]
        diagnostics[name] = dict(Counter(s["status"] for s in scan["setups"]))
        rejected[name] = [s for s in scan["setups"] if s.get("risk_rejection")]
    trades = {}
    start = frame.index[config.atr_period - 1]
    for name, entries in candidates.items():
        trades[name] = []
        for entry in entries:
            if pd.Timestamp(entry["pattern_at"]) < start:
                continue
            entry = {**entry, "signal_available_at": bar_end(pd.DatetimeIndex([entry["pattern_at"]]), "4H")[0].isoformat()}
            trades[name].append(evaluate_entry(entry, frame, fee_bps=fee_bps, slippage_bps=slippage_bps))
    digest = hashlib.sha256(frame.to_json(date_format="iso", double_precision=15).encode()).hexdigest()
    return {"ticker": symbol, "bars": len(frame), "from": frame.index[0].isoformat(),
            "to": frame.index[-1].isoformat(), "data_sha256": digest,
            "trades": trades, "setup_diagnostics": diagnostics, "rejected_risk_setups": rejected}


def load_frames(symbol: str, cache_dir: Path | None = None, *, refresh=False) -> tuple:
    cache = cache_dir / f"{symbol}.json.gz" if cache_dir else None
    if cache and cache.exists() and not refresh:
        with gzip.open(cache, "rt", encoding="utf-8") as f:
            saved = json.load(f)
        frames = {}
        for tf, data in saved.items():
            frames[tf] = pd.DataFrame(data["values"], columns=OHLC,
                                      index=pd.to_datetime(data["index"], utc=True))
        return frames["4H"], frames["1D"]
    frames = fetch_devils_frames(symbol)
    if any(frames.get(tf) is None or frames[tf].empty for tf in ("4H", "1D")):
        raise ValueError("Provider did not supply both 4H and daily history")
    if cache:
        cache.parent.mkdir(parents=True, exist_ok=True)
        packed = {tf: {"index": [pd.Timestamp(t).isoformat() for t in frames[tf].index],
                       "values": frames[tf].loc[:, list(OHLC)].astype(float).values.tolist()}
                  for tf in ("4H", "1D")}
        with gzip.open(cache, "wt", encoding="utf-8") as f:
            json.dump(packed, f, allow_nan=False)
    return frames["4H"], frames["1D"]


def run_research(symbols, *, output: Path, config=None, fee_bps=5.0, slippage_bps=0.0,
                 workers=4, refresh=False, as_of=None, fetch=load_frames) -> dict:
    config = config or SweepCisdConfig()
    if not all(isfinite(v) and v >= 0 for v in (fee_bps, slippage_bps)):
        raise ValueError("Execution costs must be finite and non-negative")
    output = Path(output)
    if output.exists():
        raise ValueError("Use a new report path; research reports are immutable")
    output.parent.mkdir(parents=True, exist_ok=True)
    started = pd.Timestamp.now(tz="UTC") if as_of is None else pd.Timestamp(as_of)
    started = started.tz_localize("UTC") if started.tzinfo is None else started.tz_convert("UTC")
    manifest = {"started_at": started.isoformat(), "symbols": list(symbols), "config": asdict(config),
                "config_hash": config.fingerprint, "fee_bps_per_side": fee_bps,
                "slippage_bps_per_side": slippage_bps, "entry_basis": "signal_close",
                "price_basis": "split_adjusted_no_dividends", "timeframe": "4H",
                "liquidity_reference": "previous_closed_daily_session", "parameter_search": False,
                "portfolio_model": "independent_signals; signal-R drawdown is not account drawdown"}
    registration = output.with_suffix(".registration.json")
    with registration.open("x", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    results, errors = [], {}

    def job(symbol):
        h4, daily = fetch(symbol, output.parent / "data", refresh=refresh)
        return compare_symbol(symbol, h4, daily, config=config, as_of=started,
                              fee_bps=fee_bps, slippage_bps=slippage_bps)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(job, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                results.append(result)
                print(f"{symbol}: " + ", ".join(f"{k}={len(v)}" for k, v in result["trades"].items()), flush=True)
            except Exception as exc:
                errors[symbol] = f"{type(exc).__name__}: {exc}"
                print(f"{symbol}: data/replay error ({type(exc).__name__})", flush=True)
    if not results:
        raise RuntimeError("No usable market data; no research verdict is available")
    # One chronological cut across the fixed universe, not a different favourable date per asset.
    first = min(pd.Timestamp(r["from"]) for r in results)
    last = max(pd.Timestamp(r["to"]) for r in results)
    split = first + (last - first) * 0.8
    aggregate = {}
    for name in VARIANTS:
        trades = [t for result in results for t in result["trades"][name]]
        aggregate[name] = {"all": summarize(trades), **partition(trades, split.isoformat())}
    evidence = aggregate[STRATEGY]["held_out"]
    report = {**manifest, "completed_at": datetime.now(timezone.utc).isoformat(),
              "split_at": split.isoformat(), "errors": errors, "results": sorted(results, key=lambda r: r["ticker"]),
              "aggregate": aggregate, "evidence": {"held_out_min_closed": 30,
                  "held_out_min_tickers": 3, "sample_sufficient": evidence["closed"] >= 30 and evidence["closed_tickers"] >= 3,
                  "forward_test_required": True, "automatically_promoted": False}}
    with output.open("x", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, allow_nan=False)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbols", default=",".join(TARGET_WATCHLIST))
    p.add_argument("--output", default=None)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--fee-bps", type=float, default=5.0)
    p.add_argument("--slippage-bps", type=float, default=0.0)
    p.add_argument("--refresh", action="store_true")
    args = p.parse_args()
    symbols = sorted(set(s.strip().upper() for s in args.symbols.split(",") if s.strip()))
    if not symbols or any(not s.replace("-", "").isalnum() or len(s) > 8 for s in symbols):
        p.error("Invalid ticker list")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = Path(args.output or f".local-backups/{STRATEGY}/{stamp}/comparison.json")
    report = run_research(symbols, output=path, workers=args.workers, fee_bps=args.fee_bps,
                          slippage_bps=args.slippage_bps, refresh=args.refresh)
    print(json.dumps(report["aggregate"], indent=2))
    print("REPORT", path.resolve())
    if report["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
