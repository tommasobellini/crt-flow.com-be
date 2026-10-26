"""
PAPER ONLY — records devils_cisd_v1 signals and outcomes (no orders, ever).

    python devils_cisd_v1_paper.py --dry-run
    python devils_cisd_v1_paper.py --since 2026-10-26   # paper start date (count only new trades)
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

import yfinance as yf

from devils_outcome_resolver import setup_logging, setup_supabase
from market_data import clean_df, drop_unclosed
from strategy.devils_cisd_v1 import STRATEGY, find_signals

UNIVERSE = (
    "MU ALB CHTR APP AAPL MSFT NVDA AMZN GOOGL META TSLA AMD AVGO NFLX JPM BAC XOM CVX UNH JNJ "
    "PG KO PEP WMT COST HD DIS INTC CSCO ORCL CRM ADBE QCOM TXN CAT BA GS V MA PFE"
).split()


def daily(t: str):
    raw = yf.Ticker(t).history(period="2y", interval="1d", auto_adjust=True)
    df = clean_df(raw.dropna() if raw is not None else None)
    if df is None or df.empty:
        return None
    df = drop_unclosed(df, "1d")
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(None)
    return df


def to_row(s: dict) -> dict:
    closed = s["status"] != "ACTIVE"
    return {
        "symbol": s["ticker"], "timeframe": "1D", "direction": s["direction"],
        "devils_marker_price": s["devils_marker_price"], "entry_price": s["entry_price"],
        "cisd_level": s["entry_price"], "is_cisd_broken": True, "status": s["status"],
        "stop_loss": s["stop_loss"], "target": s["target"], "rr_ratio": s["rr_ratio"],
        "result": None if not closed else ("WIN" if s["r_multiple"] > 0 else "LOSS"),
        "exit_reason": {"CLOSED_WIN": "TP_HIT", "CLOSED_LOSS": "SL_HIT", "CLOSED_TIME": "TIME"}.get(s["status"]),
        "r_multiple": s["r_multiple"], "closed_at": s["closed_at"], "is_active": not closed,
        "pattern_at": s["pattern_at"], "strategy": STRATEGY, "is_paper": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "meta": {"mark_at": s["mark_at"], "paper": True},
    }


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--since", default="2026-10-26", help="paper start: ignore entries before this date")
    a = p.parse_args()
    sb = None if a.dry_run else setup_supabase()
    n = 0
    for t in UNIVERSE:
        df = daily(t)
        for s in find_signals(df, t):
            if s["pattern_at"][:10] < a.since:
                continue
            row = to_row(s)
            n += 1
            if a.dry_run:
                print(f"[paper] {t} {row['direction']} {row['pattern_at'][:10]} {row['status']} R={row['r_multiple']}")
            else:
                sb.table("devils_signals").upsert(row, on_conflict="strategy,symbol,timeframe,direction,pattern_at").execute()
    print(f"{STRATEGY}: {n} paper signal(s) since {a.since}")


if __name__ == "__main__":
    main()
