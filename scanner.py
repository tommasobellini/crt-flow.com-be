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

from market_data import fetch_mtf_frames
from strategy.ha_rsi_mtf import evaluate_funnel, evaluate_symbol
from signal_adapter import signal_to_crt_row

# --- LOGGING ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")


class SupabaseLoggingHandler(logging.Handler):
    def __init__(self, supabase_client):
        super().__init__()
        self.supabase = supabase_client
        self.source = "scanner_ha_rsi_engine"

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
    key = os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY") or os.getenv(
        "NEXT_PUBLIC_SUPABASE_ANON_KEY"
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


def get_nasdaq100_tickers():
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = "https://en.wikipedia.org/wiki/NASDAQ-100"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        table = pd.read_html(io.StringIO(response.text))
        for t in table:
            if "Ticker" in t.columns:
                return [x.replace(".", "-") for x in t["Ticker"].tolist() if isinstance(x, str)]
            if "Symbol" in t.columns:
                return [x.replace(".", "-") for x in t["Symbol"].tolist() if isinstance(x, str)]
        return []
    except Exception as e:
        logger.error(f"Errore NASDAQ: {e}")
        return []


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
        return ticker if mcap >= 3_000_000 else None
    except Exception:
        return None


def scan_ticker(ticker: str, persist: bool) -> tuple[dict | None, str, bool]:
    df_4h, df_1h, df_15m = fetch_mtf_frames(ticker)
    stage, _ = evaluate_funnel(df_4h, df_1h, df_15m)
    if stage == "no_data":
        return None, stage, False

    signal = evaluate_symbol(ticker, df_4h, df_1h, df_15m)
    if signal is None:
        return None, stage, False

    is_golden = bool(signal.get("is_golden"))
    logger.info(f"🎯 SIGNAL {ticker}: {json.dumps(signal)}")
    if persist and supabase is not None:
        entry = float(df_15m["Close"].iloc[-1])
        row = signal_to_crt_row(signal, entry_price=entry)
        try:
            supabase.table("crt_signals").insert(row).execute()
            logger.info(f"💾 Persisted signal for {ticker}")
        except Exception as e:
            logger.error(f"Errore persist {ticker}: {e}")
    return signal, "signal", is_golden


def main():
    setup_logging()
    setup_supabase()

    parser = argparse.ArgumentParser(description="CRT Flow HA+RSI MTF Scanner")
    parser.add_argument(
        "--index",
        type=str,
        default="us",
        choices=["sp500", "nasdaq", "russell", "us", "all"],
        help="Universe: us = S&P 500 + NASDAQ 100 (default), all = us + Russell 2000",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Salva segnali su Supabase (default: dry-run, solo log JSON)",
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

    if sys.platform.startswith("win"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    mode = "PERSIST" if args.persist else "DRY-RUN"
    logger.info(f"🚀 HA+RSI MTF Scanner ({mode})")

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
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            for res in executor.map(check_mcap, tickers):
                if res:
                    filtered.append(res)
        tickers = filtered
        logger.info(f"Ticker post M-Cap (>= $3M): {len(tickers)}")

    if not tickers:
        logger.info("Nessun ticker da scansionare.")
        return

    signals_found = 0
    golden_found = 0
    funnel_counts: dict[str, int] = {
        "no_data": 0,
        "no_4h_structure": 0,
        "no_1h_alignment": 0,
        "signal": 0,
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(scan_ticker, t, args.persist): t for t in tickers
        }
        for future in concurrent.futures.as_completed(futures):
            ticker = futures[future]
            try:
                signal, stage, is_golden = future.result()
                funnel_counts[stage] = funnel_counts.get(stage, 0) + 1
                if signal is not None:
                    signals_found += 1
                    if is_golden:
                        golden_found += 1
            except Exception as e:
                logger.error(f"Errore {ticker}: {e}")

    logger.info(
        "Pipeline: "
        f"no_data={funnel_counts['no_data']} | "
        f"no_4h={funnel_counts['no_4h_structure']} | "
        f"no_1h={funnel_counts['no_1h_alignment']} | "
        f"signals={funnel_counts['signal']} | "
        f"golden={golden_found}"
    )
    logger.info(f"✅ Completato. Segnali trovati: {signals_found} (golden: {golden_found})")


if __name__ == "__main__":
    main()
