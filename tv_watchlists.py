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


MAX_WATCHLIST_TICKERS = 1000  # cap on extra tickers from all users' lists (scan time)
MAX_TICKERS_PER_USER = 200


def watchlist_sources(supabase=None) -> tuple[list[tuple[str, str | None]], bool]:
    """((list_id, user_id) of enabled lists of every user, from_table).
    from_table=False means defaults were used (table empty or unreadable)."""
    if supabase is not None:
        try:
            rows = supabase.table("devils_watchlist_sources").select("url,enabled,user_id").execute().data or []
            if rows:  # table configured: respect it, even if every list is disabled
                return [(m.group(1), r.get("user_id")) for r in rows if r.get("enabled")
                        and (m := re.search(r"watchlists/(\d+)", r.get("url") or ""))], True
        except Exception as e:
            print(f"WARNING watchlist sources not read ({e}); using defaults")
    return [(lid, None) for lid in WATCHLIST_IDS], False


def watchlist_ids(supabase=None) -> list[str]:
    return list(dict.fromkeys(lid for lid, _ in watchlist_sources(supabase)[0]))


def load_watchlist(supabase=None) -> list[str]:
    """Union of all users' lists (max 200 tickers per user, 1000 total). A list that fails or
    returns 0 tickers keeps the last saved data; per-user rows go to devils_user_watchlist."""
    sources, from_table = watchlist_sources(supabase)
    fetched: dict[str, list[str] | None] = {}
    for lid in dict.fromkeys(l for l, _ in sources):
        try:
            syms = fetch_watchlist(lid)
            if not syms:
                raise ValueError("0 tickers")
            fetched[lid] = syms
        except Exception as e:
            fetched[lid] = None
            print(f"WARNING TradingView watchlist {lid} not read ({e}); keeping last saved list")
    users: dict[str | None, list[str]] = {}
    for lid, uid in sources:
        users.setdefault(uid, []).append(lid)
    user_syms: dict[str | None, list[str]] = {}
    for uid, lids in users.items():
        mine: list[str] = []
        for l in lids:
            for sym in fetched.get(l) or []:
                if sym not in mine:
                    mine.append(sym)
        if len(mine) > MAX_TICKERS_PER_USER:
            print(f"WARNING user {uid}: {len(mine)} tickers, keeping first {MAX_TICKERS_PER_USER}")
            mine = mine[:MAX_TICKERS_PER_USER]
        user_syms[uid] = mine
    by_symbol: dict[str, list[str]] = {}
    for uid, mine in user_syms.items():
        for sym in mine:
            for l in users[uid]:
                if sym in (fetched.get(l) or []) and l not in by_symbol.get(sym, []):
                    by_symbol.setdefault(sym, []).append(l)
    if len(by_symbol) > MAX_WATCHLIST_TICKERS:
        print(f"WARNING global cap: {len(by_symbol)} watchlist tickers, keeping first "
              f"{MAX_WATCHLIST_TICKERS} in list order; some users' tickers are excluded")
        by_symbol = dict(list(by_symbol.items())[:MAX_WATCHLIST_TICKERS])
    ok = all(v is not None for v in fetched.values())
    if supabase is None:
        return sorted(by_symbol)
    for uid, lids in users.items():  # rewrite only users whose lists were all read
        if uid is None or any(fetched.get(l) is None for l in lids):
            continue
        mine = [s for s in user_syms[uid] if s in by_symbol]
        supabase.table("devils_user_watchlist").delete().eq("user_id", uid).execute()
        if mine:
            supabase.table("devils_user_watchlist").insert([{"user_id": uid, "symbol": s} for s in mine]).execute()
    if from_table:  # users with no active list left: clear their favourites
        active = [u for u in users if u]
        q = supabase.table("devils_user_watchlist").delete()
        (q.not_.in_("user_id", active) if active else q.neq("symbol", "")).execute()
    if not ok:
        rows = supabase.table("devils_watchlist").select("symbol").execute().data or []
        return sorted({r["symbol"] for r in rows} | set(by_symbol))
    if not by_symbol:  # all lists disabled
        supabase.table("devils_watchlist").delete().neq("symbol", "").execute()
    else:
        supabase.table("devils_watchlist").upsert(
            [{"symbol": s, "list_ids": ids} for s, ids in by_symbol.items()], on_conflict="symbol"
        ).execute()
        supabase.table("devils_watchlist").delete().not_.in_("symbol", list(by_symbol)).execute()
    print(f"TradingView watchlist: {len(by_symbol)} tickers, {len([u for u in users if u])} users")
    return sorted(by_symbol)
