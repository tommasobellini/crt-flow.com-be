"""Tommaso's public TradingView watchlists: favourites filter + extra scanner tickers."""
from __future__ import annotations

import json
import re

import requests

WATCHLIST_IDS = ["125514311", "153934167", "144009314", "141465549"]
STOCK_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
_SYMBOLS_RE = re.compile(r'"symbols":(\[[^\]]*\])')


def parse_symbols(raw: list[str]) -> list[str]:
    """'NYSE:BRK.B' -> 'BRK-B'; drops '###' sections, futures, indices, forex, crypto."""
    out: list[str] = []
    for s in raw:
        s = (s or "").strip()
        if not s or s.startswith("###") or ":" not in s:
            continue
        exch, sym = s.split(":", 1)
        if exch.upper() not in STOCK_EXCHANGES or not re.fullmatch(r"[A-Z0-9.]+", sym.upper()):
            continue
        sym = sym.upper().replace(".", "-")
        if sym not in out:
            out.append(sym)
    return out


def fetch_watchlist(list_id: str, timeout: int = 20) -> list[str]:
    r = requests.get(
        f"https://www.tradingview.com/watchlists/{list_id}/",
        headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout,
    )
    r.raise_for_status()
    m = _SYMBOLS_RE.search(r.text)
    if not m:
        raise ValueError(f"watchlist {list_id}: symbols not found in page")
    return parse_symbols(json.loads(m.group(1)))


def watchlist_ids(supabase=None) -> list[str]:
    """Lists configured in the web app settings (devils_watchlist_sources); default 4 if none."""
    if supabase is not None:
        try:
            rows = supabase.table("devils_watchlist_sources").select("url").eq("enabled", True).execute().data or []
            ids = [m.group(1) for r in rows if (m := re.search(r"watchlists/(\d+)", r.get("url") or ""))]
            if ids:
                return ids
        except Exception as e:
            print(f"WARNING watchlist sources not read ({e}); using defaults")
    return list(WATCHLIST_IDS)


def load_watchlist(supabase=None) -> list[str]:
    """Current favourites. If any list fails or returns 0 tickers, keep the last saved list
    from devils_watchlist (never silently shrink)."""
    by_symbol: dict[str, list[str]] = {}
    ok = True
    for lid in watchlist_ids(supabase):
        try:
            syms = fetch_watchlist(lid)
            if not syms:
                raise ValueError("0 tickers")
            for s in syms:
                by_symbol.setdefault(s, []).append(lid)
        except Exception as e:
            ok = False
            print(f"WARNING TradingView watchlist {lid} not read ({e}); keeping last saved list")
    if not ok:
        if supabase is None:
            return sorted(by_symbol)
        rows = supabase.table("devils_watchlist").select("symbol").execute().data or []
        return sorted({r["symbol"] for r in rows} | set(by_symbol))
    if supabase is not None:
        supabase.table("devils_watchlist").upsert(
            [{"symbol": s, "list_ids": ids} for s, ids in by_symbol.items()], on_conflict="symbol"
        ).execute()
        supabase.table("devils_watchlist").delete().not_.in_("symbol", list(by_symbol)).execute()
    print(f"TradingView watchlist: {len(by_symbol)} tickers")
    return sorted(by_symbol)
