"""Devil's Markers HTF scanner — isolated from IC-CISD scalping pipeline."""
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client

from market_data import (
    clean_df,
    resample_to_4h,
    resample_to_7h,
    resample_to_weekly,
)
from strategy.config import TARGET_WATCHLIST, YF_PERIOD_1H, YF_PERIOD_DAILY
from strategy.devils_markers import TfLabel, detect_active_devils_marker

TIMEFRAMES: list[TfLabel] = ["4H", "7H", "1D", "1W"]
MIN_BARS = 40


def setup_supabase():
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )
    if not url or not key:
        raise RuntimeError("Missing Supabase credentials in scanner/.env.local")
    return create_client(url, key)


def _history(stock: yf.Ticker, *, period: str, interval: str):
    try:
        raw = stock.history(period=period, interval=interval, auto_adjust=True)
        cleaned = clean_df(raw.dropna() if raw is not None else None)
        if cleaned is None or cleaned.empty:
            return None
        return cleaned
    except Exception:
        return None


def fetch_devils_frames(ticker: str) -> tuple[dict[TfLabel, object], object]:
    """Return (HTF frames dict, 1H frame for optional LTF CISD confirm)."""
    stock = yf.Ticker(ticker)
    df_daily = _history(stock, period=YF_PERIOD_DAILY, interval="1d")
    df_1h = _history(stock, period=YF_PERIOD_1H, interval="1h")

    frames: dict[TfLabel, object] = {
        "4H": None,
        "7H": None,
        "1D": None,
        "1W": None,
    }
    if df_daily is not None and len(df_daily) >= MIN_BARS:
        frames["1D"] = df_daily
        weekly = resample_to_weekly(df_daily)
        if weekly is not None and len(weekly) >= 12:
            frames["1W"] = weekly

    if df_1h is not None and not df_1h.empty:
        h4 = resample_to_4h(df_1h)
        h7 = resample_to_7h(df_1h)
        if len(h4) >= MIN_BARS:
            frames["4H"] = h4
        if len(h7) >= MIN_BARS:
            frames["7H"] = h7

    return frames, df_1h


def signal_to_row(signal: dict) -> dict:
    return {
        "symbol": signal["ticker"],
        "timeframe": signal["timeframe"],
        "direction": signal["direction"],
        "devils_marker_price": signal["devils_marker_price"],
        "cisd_level": signal["cisd_level"],
        "is_cisd_broken": signal["is_cisd_broken"],
        "status": signal["status"],
        "stop_loss": signal["stop_loss"],
        "target": signal["target"],
        "pattern_at": signal.get("pattern_at"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "meta": {
            "cisd_label": signal.get("cisd_label"),
            "pattern_unix": signal.get("pattern_unix"),
        },
    }


def upsert_signal(supabase, row: dict, *, dry_run: bool) -> None:
    if dry_run:
        print(
            f"  [dry-run] {row['symbol']} {row['timeframe']} "
            f"{row['direction']} {row['status']} "
            f"devil={row['devils_marker_price']} cisd={row['cisd_level']}"
        )
        return
    supabase.table("devils_signals").upsert(
        row, on_conflict="symbol,timeframe"
    ).execute()


def scan_ticker(
    ticker: str,
    *,
    supabase=None,
    dry_run: bool = False,
    persist: bool = True,
) -> list[dict]:
    frames, df_1h = fetch_devils_frames(ticker)
    results: list[dict] = []

    for tf in TIMEFRAMES:
        df = frames.get(tf)
        if df is None or getattr(df, "empty", True):
            continue
        sig = detect_active_devils_marker(
            df,
            ticker=ticker,
            timeframe=tf,
            df_ltf=df_1h,
        )
        if sig is None:
            continue
        results.append(sig)
        if persist:
            upsert_signal(supabase, signal_to_row(sig), dry_run=dry_run)

    return results


def parse_args():
    p = argparse.ArgumentParser(description="Devil's Markers HTF Scanner")
    p.add_argument("--symbol", type=str, default=None, help="Single ticker")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-persist", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    tickers = (
        [args.symbol.strip().upper()]
        if args.symbol
        else list(TARGET_WATCHLIST)
    )
    persist = not args.no_persist
    dry_run = args.dry_run

    supabase = None
    if persist and not dry_run:
        supabase = setup_supabase()

    print(f"Devil's Markers scan — {len(tickers)} tickers, TFs={TIMEFRAMES}")
    total = 0
    errors = 0

    def _job(sym: str) -> tuple[str, list[dict], str | None]:
        try:
            sigs = scan_ticker(
                sym,
                supabase=supabase,
                dry_run=dry_run,
                persist=persist,
            )
            return sym, sigs, None
        except Exception as exc:
            return sym, [], str(exc)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futs = {pool.submit(_job, t): t for t in tickers}
        for fut in as_completed(futs):
            sym, sigs, err = fut.result()
            if err:
                errors += 1
                print(f"[{sym}] error: {err}")
            else:
                total += len(sigs)
                print(f"[{sym}] {len(sigs)} marker(s)" if sigs else f"[{sym}] none")

    print(f"Done. markers={total} errors={errors}")


if __name__ == "__main__":
    main()
