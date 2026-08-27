import os
import argparse
import logging
import time
import io
import requests
import json
import sys
import concurrent.futures

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client

from market_data import fetch_htf_frames
from strategy.sweep_engulf import detect_latest_pattern
from strategy.config import MIN_BARS_1H, MIN_BARS_4H, MIN_MARKET_CAP
from signal_adapter import signal_to_crt_row

# --- LOGGING ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")


class SupabaseLoggingHandler(logging.Handler):
    def __init__(self, supabase_client):
        super().__init__()
        self.supabase = supabase_client
        self.source = "scanner_sweep_engulf_engine"

    def emit(self, record):
        try:
            log_entry = self.format(record)
            if "system_logs" in log_entry:
                return
            self.supabase.table("system_logs").insert(
                {"level": record.levelname, "message": log_entry, "source": self.source}
            ).execute()
        except Exception:
            pass


def setup_logging():
    file_handler = logging.FileHandler("scanner_new.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if sys.platform == "win32":
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
        except Exception:
            pass

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


supabase = None


def setup_supabase():
    global supabase
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")

    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )

    if url and key:
        try:
            supabase = create_client(url, key)
            sb_handler = SupabaseLoggingHandler(supabase)
            sb_handler.setFormatter(formatter)
            logger.addHandler(sb_handler)
        except Exception as e:
            print(f"Errore Supabase: {e}")


def get_sp500_tickers():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        table = pd.read_html(io.StringIO(response.text))
        tickers = table[0]["Symbol"].tolist()
        return [
            t.replace(".", "-")
            for t in tickers
            if isinstance(t, str) and len(t) <= 8 and " " not in t
        ]
    except Exception as e:
        logger.error(f"Errore SP500: {e}")
        return ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]


def _normalize_tickers(raw: list) -> list[str]:
    out: list[str] = []
    for t in raw:
        if not isinstance(t, str):
            continue
        sym = t.strip().replace(".", "-")
        if sym and len(sym) <= 8 and " " not in sym:
            out.append(sym)
    return out


def get_nasdaq100_tickers():
    """Load NASDAQ-100 constituents. Wikipedia no longer exposes a Ticker table."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    sources: list[tuple[str, str]] = [
        ("stockanalysis", "https://stockanalysis.com/list/nasdaq-100-stocks/"),
        ("wikipedia", "https://en.wikipedia.org/wiki/Nasdaq-100"),
    ]

    for label, url in sources:
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            tables = pd.read_html(io.StringIO(response.text))
            for table in tables:
                col = None
                if "Symbol" in table.columns:
                    col = "Symbol"
                elif "Ticker" in table.columns:
                    col = "Ticker"
                if col is None:
                    continue
                tickers = _normalize_tickers(table[col].tolist())
                if len(tickers) >= 80:
                    logger.info(f"   NASDAQ 100: {len(tickers)} ticker ({label})")
                    return tickers
        except Exception as e:
            logger.warning(f"NASDAQ 100 ({label}): {e}")

    fallback = [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
        "NFLX", "AMD", "PEP", "ADBE", "CSCO", "TMUS", "INTC", "INTU", "QCOM", "AMAT",
        "ISRG", "BKNG", "CMCSA", "TXN", "VRTX", "AMGN", "HON", "SBUX", "GILD", "ADI",
        "PANW", "MU", "LRCX", "REGN", "MELI", "ADP", "KLAC", "SNPS", "CDNS", "CRWD",
        "MAR", "CTAS", "ORLY", "CSX", "PCAR", "NXPI", "FTNT", "AEP", "ROST", "PAYX",
        "ODFL", "FAST", "KDP", "VRSK", "EXC", "BKR", "CTSH", "GEHC", "XEL", "EA",
        "IDXX", "FANG", "CCEP", "TTWO", "ON", "ANSS", "CDW", "ZS", "DXCM", "BIIB",
        "GFS", "MDB", "WBD", "ILMN", "TEAM", "DDOG", "MRVL", "WDAY", "ABNB", "DASH",
        "ARM", "PLTR", "APP", "MSTR", "SHOP",
    ]
    logger.warning(
        f"NASDAQ 100: usando fallback statico ({len(fallback)} ticker)"
    )
    return fallback


def _is_open_result(result: str | None) -> bool:
    return result is None or str(result).upper() in ("OPEN", "")


def _signal_already_exists(row: dict) -> bool:
    """Skip insert if same pattern or another open signal exists for symbol/tf/type."""
    assert supabase is not None
    symbol = row["symbol"]
    timeframe = row["timeframe"]
    sig_type = row["type"]
    pattern_at = row.get("pattern_at")

    if pattern_at:
        dup = (
            supabase.table("crt_signals")
            .select("id")
            .eq("symbol", symbol)
            .eq("timeframe", timeframe)
            .eq("type", sig_type)
            .eq("pattern_at", pattern_at)
            .limit(1)
            .execute()
        )
        if dup.data:
            logger.info(
                f"⏭️  Skip duplicate pattern {symbol} {timeframe} {sig_type} @ {pattern_at}"
            )
            return True

    open_rows = (
        supabase.table("crt_signals")
        .select("id, result")
        .eq("symbol", symbol)
        .eq("timeframe", timeframe)
        .eq("type", sig_type)
        .eq("is_active", True)
        .execute()
    )
    for existing in open_rows.data or []:
        if _is_open_result(existing.get("result")):
            logger.info(
                f"⏭️  Skip open signal already exists for {symbol} {timeframe} {sig_type}"
            )
            return True
    return False


def _persist_signal_row(row: dict) -> None:
    """Insert crt_signals row with dedup; retry without market_cap if column missing."""
    assert supabase is not None
    if _signal_already_exists(row):
        return
    try:
        supabase.table("crt_signals").insert(row).execute()
        return
    except Exception as e:
        code = getattr(e, "code", None)
        err = str(e)
        if code == "23505" or "duplicate key" in err.lower():
            logger.info(
                f"⏭️  Skip duplicate (unique index) {row.get('symbol')} "
                f"{row.get('timeframe')} {row.get('type')}"
            )
            return
        missing_col = code == "PGRST204" or "PGRST204" in err or "schema cache" in err
        if missing_col:
            stripped = dict(row)
            for optional in ("market_cap", "pattern_levels", "pattern_at"):
                if optional in err and optional in stripped:
                    stripped.pop(optional, None)
                    logger.warning(
                        f"crt_signals.{optional} missing in schema cache — "
                        "retrying insert without it"
                    )
            if stripped != row:
                if _signal_already_exists(stripped):
                    return
                try:
                    supabase.table("crt_signals").insert(stripped).execute()
                except Exception as e2:
                    if getattr(e2, "code", None) == "23505" or "duplicate key" in str(e2).lower():
                        logger.info(
                            f"⏭️  Skip duplicate (unique index) {row.get('symbol')}"
                        )
                        return
                    # Last resort: strip both optional jsonb/market columns
                    stripped2 = {
                        k: v
                        for k, v in stripped.items()
                        if k not in ("market_cap", "pattern_levels")
                    }
                    if stripped2 != stripped:
                        supabase.table("crt_signals").insert(stripped2).execute()
                        return
                    raise
                return
        raise


def _parse_iwm_holdings_csv(text: str) -> list[str]:
    if text.lstrip().startswith("<!") or text.lstrip().startswith("<html"):
        raise ValueError("Risposta HTML invece di CSV (iShares ha bloccato il download)")

    header_idx = None
    for i, line in enumerate(text.splitlines()):
        if line.strip().startswith("Ticker,") or line.strip().startswith('"Ticker"'):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Colonna Ticker non trovata nel CSV IWM")

    df = pd.read_csv(io.StringIO(text), skiprows=header_idx)
    if "Ticker" not in df.columns:
        raise ValueError("Colonna Ticker mancante dopo il parse")

    tickers = df["Ticker"].dropna().astype(str).str.strip().str.replace('"', "", regex=False)
    return list(
        {
            t.replace(".", "-")
            for t in tickers
            if t and t != "-" and len(t) <= 8 and " " not in t
        }
    )


def get_russell2000_tickers():
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    headers = {"User-Agent": "Mozilla/5.0"}
    local_path = os.path.join(os.path.dirname(__file__), "IWM_holdings.csv")

    sources: list[tuple[str, str]] = []
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        sources.append(("iShares", response.text))
    except Exception as e:
        logger.warning(f"Download Russell 2000 fallito: {e}")

    if os.path.isfile(local_path):
        try:
            with open(local_path, encoding="utf-8-sig") as f:
                sources.append(("IWM_holdings.csv locale", f.read()))
        except Exception as e:
            logger.warning(f"Lettura CSV locale fallita: {e}")

    for label, text in sources:
        try:
            tickers = _parse_iwm_holdings_csv(text)
            if tickers:
                logger.info(f"   Russell 2000: {len(tickers)} ticker ({label})")
                return tickers
        except Exception as e:
            logger.warning(f"   Russell 2000 ({label}): {e}")

    logger.error(
        "Errore Russell 2000: impossibile caricare holdings. "
        "Scarica IWM_holdings.csv da iShares e salvalo in scanner/"
    )
    return []


def check_mcap(ticker: str) -> str | None:
    try:
        ticker_obj = yf.Ticker(ticker)
        mcap = (
            ticker_obj.fast_info.get("marketCap", 0)
            if hasattr(ticker_obj, "fast_info")
            else 0
        )
        return ticker if mcap >= MIN_MARKET_CAP else None
    except Exception:
        return None


def get_market_cap(ticker: str) -> int | None:
    try:
        ticker_obj = yf.Ticker(ticker)
        mcap = 0
        if hasattr(ticker_obj, "fast_info"):
            try:
                mcap = ticker_obj.fast_info.get("marketCap", 0) or 0
            except Exception:
                mcap = 0
        if not mcap:
            info = getattr(ticker_obj, "info", None) or {}
            mcap = info.get("marketCap", 0) or 0
        mcap_f = float(mcap or 0)
        return int(mcap_f) if mcap_f > 0 else None
    except Exception:
        return None


def scan_ticker(ticker: str, persist: bool) -> tuple[list[dict], str, int]:
    df_4h, df_1h = fetch_htf_frames(ticker)

    if df_4h is None or df_1h is None:
        return [], "no_data", 0

    frames: list[tuple[str, object, int]] = [
        ("4H", df_4h, MIN_BARS_4H),
        ("1H", df_1h, MIN_BARS_1H),
    ]

    signals: list[dict] = []
    for tf_label, df, min_bars in frames:
        if df is None or len(df) < min_bars:
            continue
        pattern = detect_latest_pattern(df, tf_label)  # type: ignore[arg-type]
        if pattern is not None:
            pattern["ticker"] = ticker
            signals.append(pattern)

    if not signals:
        return [], "no_pattern", 0

    market_cap = get_market_cap(ticker) if persist else None
    if persist and market_cap is None:
        logger.warning(f"⚠️  market_cap unavailable for {ticker}")
    for signal in signals:
        if market_cap is not None:
            signal["market_cap"] = market_cap
        logger.info(f"🎯 SIGNAL {ticker} [{signal['timeframe']}]: {json.dumps(signal)}")
        if persist and supabase is not None:
            row = signal_to_crt_row(signal, ticker=ticker)
            try:
                _persist_signal_row(row)
                logger.info(
                    f"💾 Persisted {signal['direction']} signal for {ticker} "
                    f"({signal['timeframe']})"
                    + (f" mcap={market_cap}" if market_cap else " mcap=null")
                )
            except Exception as e:
                logger.error(f"Errore persist {ticker} ({signal['timeframe']}): {e}")

    return signals, "signal", len(signals)


def main():
    setup_logging()
    setup_supabase()

    parser = argparse.ArgumentParser(description="CRT Flow Sweep & Engulf Scanner")
    parser.add_argument(
        "--index",
        type=str,
        default="us",
        choices=["sp500", "nasdaq", "russell", "us", "all"],
        help="Universe: us = S&P 500 + NASDAQ 100 (default), all = us + Russell 2000",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo log JSON, non salva su Supabase",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Deprecated: persist is the default (kept for CI compat)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Scansiona un solo ticker (es. AAPL)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Thread pool size per download/analisi",
    )
    args = parser.parse_args()
    persist = not args.dry_run

    if sys.platform.startswith("win"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    mode = "DRY-RUN" if args.dry_run else "PERSIST"
    logger.info(f"🚀 Sweep & Engulf Scanner ({mode})")

    if args.symbol:
        tickers = [args.symbol.upper()]
    else:
        all_tickers: list[str] = []
        index_counts: dict[str, int] = {}
        scan_sp = args.index in ["sp500", "us", "all"]
        scan_nd = args.index in ["nasdaq", "us", "all"]
        scan_ru = args.index in ["russell", "all"]

        if scan_sp:
            logger.info("📡 Caricamento S&P 500...")
            sp = get_sp500_tickers()
            index_counts["S&P 500"] = len(sp)
            all_tickers += sp
        if scan_nd:
            logger.info("📡 Caricamento NASDAQ 100...")
            nd = get_nasdaq100_tickers()
            index_counts["NASDAQ 100"] = len(nd)
            all_tickers += nd
        if scan_ru:
            logger.info("📡 Caricamento Russell 2000...")
            ru = get_russell2000_tickers()
            index_counts["Russell 2000"] = len(ru)
            all_tickers += ru

        for name, count in index_counts.items():
            logger.info(f"   {name}: {count} ticker")
        tickers = list(set(all_tickers))
        logger.info(f"✅ Ticker unici: {len(tickers)} (overlap tra indici rimosso)")

        logger.info("Filtro Market Cap in corso...")
        filtered: list[str] = []
        # Keep mcap workers low — Yahoo rate-limits hard on --index all (~2.5k tickers)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            for res in executor.map(check_mcap, tickers):
                if res:
                    filtered.append(res)
        tickers = filtered
        logger.info(f"Ticker post M-Cap (>= $10B): {len(tickers)}")
        # Cooldown so the history phase is less likely to get empty responses
        time.sleep(8)

    if not tickers:
        logger.info("Nessun ticker da scansionare.")
        return

    signals_found = 0
    funnel_counts: dict[str, int] = {
        "no_data": 0,
        "no_pattern": 0,
        "signal": 0,
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(scan_ticker, t, persist): t for t in tickers
        }
        for future in concurrent.futures.as_completed(futures):
            ticker = futures[future]
            try:
                signals, stage, count = future.result()
                funnel_counts[stage] = funnel_counts.get(stage, 0) + 1
                if signals:
                    signals_found += count
            except Exception as e:
                logger.error(f"Errore {ticker}: {e}")

    logger.info(
        "Pipeline: "
        f"no_data={funnel_counts['no_data']} | "
        f"no_pattern={funnel_counts['no_pattern']} | "
        f"signals={funnel_counts['signal']} | "
        f"total_rows={signals_found}"
    )
    logger.info(f"✅ Completato. Segnali trovati: {signals_found}")


if __name__ == "__main__":
    main()
