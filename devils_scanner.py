"""Devil's Markers HTF scanner — S&P 500 + NASDAQ 100, Pure No-Wick."""
from __future__ import annotations

import argparse
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client

from market_data import (
    clean_df,
    drop_unclosed,
    resample_to_4h,
    resample_to_monthly,
    resample_to_weekly,
)
from strategy.config import YF_PERIOD_1H
from tv_watchlists import load_watchlist
from strategy.devils_markers import TfLabel, closed_devils_markers, detect_active_devils_marker

# 7H removed: not a session-aligned timeframe (no TradingView equivalent).
TIMEFRAMES: list[TfLabel] = ["4H", "1D", "1W", "1M"]
MIN_BARS = 40
MIN_BARS_MONTHLY = 12
YF_PERIOD_DAILY_LONG = "5y"


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


def _normalize_tickers(raw: list) -> list[str]:
    out: list[str] = []
    for t in raw:
        if not isinstance(t, str):
            continue
        sym = t.strip().replace(".", "-")
        if sym and len(sym) <= 8 and " " not in sym:
            out.append(sym.upper())
    return out


def get_sp500_tickers() -> list[str]:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        table = pd.read_html(io.StringIO(response.text))
        return _normalize_tickers(table[0]["Symbol"].tolist())
    except Exception as e:
        print(f"SP500 fetch error: {e}")
        return ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]


def get_nasdaq100_tickers() -> list[str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    sources = [
        ("stockanalysis", "https://stockanalysis.com/list/nasdaq-100-stocks/"),
        ("wikipedia", "https://en.wikipedia.org/wiki/Nasdaq-100"),
    ]
    for label, url in sources:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            tables = pd.read_html(io.StringIO(response.text))
            for table in tables:
                col = "Symbol" if "Symbol" in table.columns else (
                    "Ticker" if "Ticker" in table.columns else None
                )
                if col is None:
                    continue
                tickers = _normalize_tickers(table[col].tolist())
                if len(tickers) >= 80:
                    print(f"NASDAQ 100: {len(tickers)} ({label})")
                    return tickers
        except Exception as e:
            print(f"NASDAQ 100 ({label}): {e}")
    return [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
        "NFLX", "AMD", "PEP", "ADBE", "CSCO", "TMUS", "INTC", "INTU", "QCOM", "AMAT",
    ]


def get_universe(supabase=None) -> list[str]:
    sp = get_sp500_tickers()
    nd = get_nasdaq100_tickers()
    try:
        wl = load_watchlist(supabase)
    except Exception as e:  # watchlist must never break the scan
        print(f"WARNING watchlist skipped: {e}")
        wl = []
    print(f"S&P 500: {len(sp)} | NASDAQ 100: {len(nd)} | TradingView: {len(wl)}")
    return sorted(set(sp + nd + wl))


def _history(stock: yf.Ticker, *, period: str, interval: str):
    try:
        raw = stock.history(period=period, interval=interval, auto_adjust=True)
        cleaned = clean_df(raw.dropna() if raw is not None else None)
        if cleaned is None or cleaned.empty:
            return None
        return cleaned
    except Exception:
        return None


def fetch_devils_frames(ticker: str) -> dict[TfLabel, object]:
    stock = yf.Ticker(ticker)
    df_daily = _history(stock, period=YF_PERIOD_DAILY_LONG, interval="1d")
    df_1h = _history(stock, period=YF_PERIOD_1H, interval="1h")

    frames: dict[TfLabel, object] = {
        "4H": None,
        "1D": None,
        "1W": None,
        "1M": None,
    }
    # Only closed bars: a forming candle can show open==high/low and then repaint.
    df_daily = drop_unclosed(df_daily, "1d")
    if df_daily is not None and len(df_daily) >= MIN_BARS:
        frames["1D"] = df_daily
        weekly = resample_to_weekly(df_daily)
        if weekly is not None and len(weekly) >= 12:
            frames["1W"] = weekly
        monthly = resample_to_monthly(df_daily)
        if monthly is not None and len(monthly) >= MIN_BARS_MONTHLY:
            frames["1M"] = monthly

    if df_1h is not None and not df_1h.empty:
        h4 = resample_to_4h(df_1h)  # regular session, closed bars only
        if len(h4) >= MIN_BARS:
            frames["4H"] = h4

    return frames


def signal_to_row(signal: dict) -> dict:
    return {
        "symbol": signal["ticker"],
        "timeframe": signal["timeframe"],
        "direction": signal["direction"],
        "devils_marker_price": signal["devils_marker_price"],
        "entry_price": signal["entry_price"],
        "cisd_level": signal.get("cisd_level") or signal["entry_price"],
        "is_cisd_broken": True,
        "status": signal["status"],
        "stop_loss": signal["stop_loss"],
        "target": signal["target"],
        "rr_ratio": signal.get("rr_ratio"),
        "result": {"CLOSED_WIN": "WIN", "CLOSED_LOSS": "LOSS"}.get(signal["status"]),
        "exit_reason": {"CLOSED_WIN": "TP_HIT", "CLOSED_LOSS": "SL_HIT"}.get(signal["status"]),
        "r_multiple": (
            signal.get("rr_ratio") if signal["status"] == "CLOSED_WIN"
            else -1.0 if signal["status"] == "CLOSED_LOSS" else None
        ),
        "closed_at": None,
        "is_active": signal["status"] == "ACTIVE",
        "pattern_at": signal.get("pattern_at"),
        "strategy": "pure_no_wick",
        "is_paper": False,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "meta": {
            "pattern_unix": signal.get("pattern_unix"),
            "is_high_rr": signal.get("is_high_rr"),
            "strategy": "pure_no_wick",
        },
    }


def upsert_signal(supabase, row: dict, *, dry_run: bool) -> None:
    if dry_run:
        print(
            f"  [dry-run] {row['symbol']} {row['timeframe']} "
            f"{row['direction']} RR={row.get('rr_ratio')} "
            f"SL={row['stop_loss']} TP={row['target']}"
        )
        return
    # Do not overwrite already closed outcomes
    existing = (
        supabase.table("devils_signals")
        .select("id, result, status")
        .eq("symbol", row["symbol"])
        .eq("timeframe", row["timeframe"])
        .eq("direction", row["direction"])
        .eq("pattern_at", row["pattern_at"])
        .limit(1)
        .execute()
    )
    if existing.data:
        prev = existing.data[0]
        if prev.get("result") in ("WIN", "LOSS") or prev.get("status") in (
            "CLOSED_WIN",
            "CLOSED_LOSS",
        ):
            return
    supabase.table("devils_signals").upsert(
        row, on_conflict="strategy,symbol,timeframe,direction,pattern_at"
    ).execute()


def scan_ticker(
    ticker: str,
    *,
    supabase=None,
    dry_run: bool = False,
    persist: bool = True,
) -> list[dict]:
    frames = fetch_devils_frames(ticker)
    results: list[dict] = []

    for tf in TIMEFRAMES:
        df = frames.get(tf)
        if df is None or getattr(df, "empty", True):
            continue
        if persist:
            # Trades that already closed between scans are real outcomes: record them.
            for closed in closed_devils_markers(df, ticker=ticker, timeframe=tf):
                upsert_signal(supabase, signal_to_row(closed), dry_run=dry_run)
        sig = detect_active_devils_marker(df, ticker=ticker, timeframe=tf)
        if sig and not sig.get("is_high_rr"):  # same RR >= 1.5 filter as closed markers
            sig = None
        if sig is None:
            continue
        results.append(sig)
        if persist:
            upsert_signal(supabase, signal_to_row(sig), dry_run=dry_run)

    return results


def parse_args():
    p = argparse.ArgumentParser(description="Devil's Markers Pure No-Wick Scanner")
    p.add_argument("--symbol", type=str, default=None)
    p.add_argument("--workers", type=int, default=10)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-persist", action="store_true")
    p.add_argument(
        "--universe",
        choices=["sp500_nasdaq", "watchlist"],
        default="sp500_nasdaq",
    )
    return p.parse_args()


def backfill_new_watchlist_symbols() -> None:
    """Full history once for TradingView tickers never backfilled. A ticker with no Yahoo data
    is retried at most 3 times, then paused 7 days."""
    try:
        from datetime import timedelta

        sb = setup_supabase()
        now = datetime.now(timezone.utc)
        rows = (sb.table("devils_watchlist").select("symbol,backfill_attempts,last_backfill_attempt_at")
                .is_("backfilled_at", "null").execute().data or [])
        todo = []
        for r in rows:
            last = r.get("last_backfill_attempt_at")
            paused = (r.get("backfill_attempts") or 0) >= 3 and last and \
                datetime.fromisoformat(last.replace("Z", "+00:00")) > now - timedelta(days=7)
            if not paused:
                todo.append(r)
        if todo:  # tickers that already have history (e.g. S&P 500) only need the flag
            have = {r["symbol"] for r in todo  # limit(1) per ticker: PostgREST caps rows at 1000
                    if sb.table("devils_signals").select("id").eq("symbol", r["symbol"])
                    .eq("strategy", "pure_no_wick").limit(1).execute().data}
            if have:
                sb.table("devils_watchlist").update({"backfilled_at": now.isoformat()}).in_("symbol", sorted(have)).execute()
            todo = [r for r in todo if r["symbol"] not in have]
        if not todo:
            return
        from devils_backfill import backfill_symbols

        syms = sorted(r["symbol"] for r in todo)
        print(f"Backfill for new watchlist tickers: {syms}")
        _, missing = backfill_symbols(sb, syms)
        sb = setup_supabase()
        done = [s for s in syms if s not in missing]
        if done:
            sb.table("devils_watchlist").update({"backfilled_at": now.isoformat()}).in_("symbol", done).execute()
        for r in todo:
            if r["symbol"] in missing:
                attempts = (r.get("backfill_attempts") or 0)
                attempts = 1 if attempts >= 3 else attempts + 1  # new cycle after a 7-day pause
                sb.table("devils_watchlist").update(
                    {"backfill_attempts": attempts, "last_backfill_attempt_at": now.isoformat()}
                ).eq("symbol", r["symbol"]).execute()
    except Exception as e:  # never block the live scan
        print(f"WARNING watchlist backfill skipped: {e}")


def main():
    args = parse_args()
    if args.symbol:
        tickers = [args.symbol.strip().upper()]
    elif args.universe == "watchlist":
        from strategy.config import TARGET_WATCHLIST

        tickers = list(TARGET_WATCHLIST)
    else:
        # also syncs devils_watchlist (favourites filter in the web app) when persisting
        tickers = get_universe(setup_supabase() if not args.no_persist and not args.dry_run else None)

    if not args.symbol and args.universe != "watchlist" and not args.no_persist and not args.dry_run:
        backfill_new_watchlist_symbols()

    persist = not args.no_persist
    dry_run = args.dry_run

    supabase = None
    if persist and not dry_run:
        supabase = setup_supabase()

    print(f"Devil's Markers Pure No-Wick — {len(tickers)} tickers, TFs={TIMEFRAMES}")
    total = 0
    errors = 0

    def _job(sym: str) -> tuple[str, list[dict], str | None]:
        try:
            sigs = scan_ticker(
                sym, supabase=supabase, dry_run=dry_run, persist=persist
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
                if sigs:
                    print(f"[{sym}] {len(sigs)} marker(s)")

    print(f"Done. markers={total} errors={errors}")


if __name__ == "__main__":
    main()
