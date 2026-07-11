import argparse
import os
from dataclasses import asdict
from datetime import timezone

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client

from backtester import ScannerParams, compute_htf_pools, find_reclaim


def setup_supabase():
    if os.path.exists(".env.local"):
        load_dotenv(".env.local")
    url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_SERVICE_ROLE_KEY") or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("Missing Supabase credentials in scanner/.env.local")
    return create_client(url, key)


def load_si_data(path: str) -> pd.DataFrame:
    if not path:
        return pd.DataFrame(columns=["symbol", "as_of_date", "short_float_pct", "days_to_cover"])
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext == ".json":
        df = pd.read_json(path)
    else:
        raise ValueError("Short-interest file must be CSV or JSON")
    required = {"symbol", "as_of_date", "short_float_pct", "days_to_cover"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"Short-interest file missing columns: {required}")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], utc=True, errors="coerce")
    df = df.dropna(subset=["as_of_date"]).sort_values(["symbol", "as_of_date"])
    return df


def resolve_si_row(si_df: pd.DataFrame, symbol: str, ts: pd.Timestamp) -> dict:
    if si_df.empty:
        return {"short_float_pct": None, "days_to_cover": None}
    symbol_df = si_df[si_df["symbol"] == symbol]
    if symbol_df.empty:
        return {"short_float_pct": None, "days_to_cover": None}
    eligible = symbol_df[symbol_df["as_of_date"] <= ts]
    if eligible.empty:
        return {"short_float_pct": None, "days_to_cover": None}
    row = eligible.iloc[-1]
    return {
        "short_float_pct": float(row["short_float_pct"]) if pd.notna(row["short_float_pct"]) else None,
        "days_to_cover": float(row["days_to_cover"]) if pd.notna(row["days_to_cover"]) else None,
    }


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def score_squeeze(short_float_pct, days_to_cover, rvol) -> float:
    si_score = clamp01((short_float_pct or 0.0) / 35.0)
    dtc_score = clamp01((days_to_cover or 0.0) / 10.0)
    rvol_score = clamp01((rvol or 0.0) / 3.0)
    return round((si_score * 0.45 + dtc_score * 0.35 + rvol_score * 0.20) * 100.0, 2)


def classify_event(signal: dict | None, score: float) -> tuple[str, bool]:
    if signal and score >= 55:
        return "ACTIVE_SQUEEZE", True
    if signal:
        return "SETUP", False
    return "HISTORICAL_FAILURE", False


def fetch_latest_params(supabase) -> ScannerParams:
    try:
        res = (
            supabase.table("sqz_scanner_params")
            .select("*")
            .eq("is_active", True)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if not res.data:
            return ScannerParams()
        row = res.data[0]
        return ScannerParams(
            wall_wick_pct=row.get("wall_wick_pct", 0.001),
            fuel_wick_pct=row.get("fuel_wick_pct", 0.40),
            displacement_mult=row.get("displacement_mult", 1.2),
            opposite_wick_tol=row.get("opposite_wick_tol", 0.30),
            sl_buffer_pct=row.get("sl_buffer_pct", 0.001),
            proximity_filter_pct=row.get("proximity_filter_pct", 0.01),
        )
    except Exception:
        return ScannerParams()


def upsert_signal(supabase, payload: dict, dry_run: bool):
    if dry_run:
        return
    supabase.table("squeeze_signals").upsert(payload, on_conflict="symbol,timestamp,event_type").execute()


def run_symbol(
    supabase,
    symbol: str,
    params: ScannerParams,
    mode: str,
    start: str | None,
    end: str | None,
    si_df: pd.DataFrame,
    rvol_period: int,
    min_rvol: float,
    dry_run: bool,
):
    ticker = yf.Ticker(symbol)
    daily = ticker.history(period="5y", interval="1d").dropna()
    hourly = ticker.history(period="730d", interval="1h").dropna()
    if daily.empty or hourly.empty:
        return 0
    daily.index = pd.to_datetime(daily.index, utc=True)
    hourly.index = pd.to_datetime(hourly.index, utc=True)
    if start:
        daily = daily[daily.index >= pd.Timestamp(start, tz=timezone.utc)]
    if end:
        daily = daily[daily.index <= pd.Timestamp(end, tz=timezone.utc)]
    if len(daily) < 40:
        return 0

    daily["rvol"] = daily["Volume"] / daily["Volume"].rolling(rvol_period).mean()

    rows = 0
    scan_indices = [len(daily) - 1] if mode == "scan" else list(range(30, len(daily)))
    for i in scan_indices:
        daily_slice = daily.iloc[: i + 1]
        pools = compute_htf_pools(daily_slice, params.wall_wick_pct, params.fuel_wick_pct)
        if not pools:
            continue
        day_ts = daily_slice.index[-1]
        current_price = float(daily_slice.iloc[-1]["Close"])
        prior_1h = hourly[hourly.index <= day_ts].tail(25)
        if len(prior_1h) < 5:
            continue

        signal = find_reclaim(pools, prior_1h, current_price, params)
        si_row = resolve_si_row(si_df, symbol, day_ts)
        rvol = float(daily_slice.iloc[-1]["rvol"]) if pd.notna(daily_slice.iloc[-1]["rvol"]) else 0.0
        score = score_squeeze(si_row["short_float_pct"], si_row["days_to_cover"], rvol)
        event_type, is_active = classify_event(signal, score)

        if signal and event_type == "HISTORICAL_FAILURE":
            event_type = "HISTORICAL_SUCCESS"

        if rvol < min_rvol and event_type == "ACTIVE_SQUEEZE":
            event_type = "SETUP"
            is_active = False

        payload = {
            "symbol": symbol,
            "timestamp": day_ts.isoformat(),
            "event_type": event_type,
            "direction": signal["direction"] if signal else "bullish",
            "liquidity_tier": signal["tier"] if signal else None,
            "entry_price": signal["entry"] if signal else None,
            "stop_loss": signal["stop"] if signal else None,
            "take_profit": signal["target"] if signal else None,
            "rr_ratio": signal["rr"] if signal else None,
            "short_float_pct": si_row["short_float_pct"],
            "days_to_cover": si_row["days_to_cover"],
            "rvol": round(rvol, 3) if pd.notna(rvol) else None,
            "squeeze_score": score,
            "is_active_squeeze": is_active,
            "hit_target_10d": None,
            "max_gain_10d_pct": None,
            "meta": {
                "mode": mode,
                "params": asdict(params),
                "reclaim_candle_time": signal.get("reclaim_candle_time") if signal else None,
            },
        }
        upsert_signal(supabase, payload, dry_run=dry_run)
        rows += 1
    return rows


def parse_args():
    parser = argparse.ArgumentParser(description="CRT Squeeze Intelligence Engine")
    parser.add_argument("--mode", choices=["scan", "backfill"], default="scan")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "NVDA", "TSLA"])
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--si-file", type=str, default="short_interest_sample.csv")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    supabase = setup_supabase()
    params = fetch_latest_params(supabase)
    si_df = load_si_data(args.si_file) if args.si_file else pd.DataFrame()

    total = 0
    for symbol in [s.upper() for s in args.symbols]:
        try:
            inserted = run_symbol(
                supabase=supabase,
                symbol=symbol,
                params=params,
                mode=args.mode,
                start=args.start,
                end=args.end,
                si_df=si_df,
                rvol_period=20,
                min_rvol=1.2,
                dry_run=args.dry_run,
            )
            total += inserted
            print(f"[{symbol}] processed rows: {inserted}")
        except Exception as exc:
            print(f"[{symbol}] error: {exc}")

    print(f"Done. Total rows processed: {total}")
    print("Note: for production SI ingestion, use licensed datasets; avoid scraping HTML pages that may violate ToS.")


if __name__ == "__main__":
    main()
