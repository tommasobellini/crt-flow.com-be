"""
CRT Flow Backtester v3 — aligned with scanner.py (production engine)

Replicates the exact logic of scanner.py:
  - Multi-timeframe walls: Daily (PDH/PDL), Weekly (PWH/PWL), Monthly (PMH/PML)
  - 24-candle 1H lookback for reclaim detection
  - Displacement check (body expansion + opposite wick tolerance)
  - Dynamic SL from sweep candle wick + buffer
  - Proximity filter (chasing prevention)
  - Grid search over all 6 tunable parameters

Usage:
    python backtester.py --ticker AAPL
    python backtester.py --ticker NVDA --optimize
    python backtester.py --ticker MSFT --period 2y --verbose
    python backtester.py --ticker AAPL --optimize --params wall_wick fuel_wick displacement
"""
import argparse
import itertools
from dataclasses import dataclass

import yfinance as yf
import pandas as pd


# ─────────────────────────────────────────────────────────────
# PARAMETERS (mirrors scanner.py hard-coded values)
# ─────────────────────────────────────────────────────────────

@dataclass
class ScannerParams:
    wall_wick_pct: float        = 0.001   # max wall wick / body  (calc_integrity_score)
    fuel_wick_pct: float        = 0.40    # min fuel wick / body
    displacement_mult: float    = 1.2     # reclaim body > avg_body * mult
    opposite_wick_tol: float    = 0.30    # reclaim opposite wick < body * tol
    sl_buffer_pct: float        = 0.001   # SL buffer behind sweep wick (0.1%)
    proximity_filter_pct: float = 0.01    # ignore signal if price >1% from level


PARAM_GRID = {
    "wall_wick_pct":        [0.0005, 0.002],    # stricter vs looser wall
    "fuel_wick_pct":        [0.40, 0.55],        # standard vs high fuel
    "displacement_mult":    [1.2, 1.5],          # moderate vs strong displacement
    "opposite_wick_tol":    [0.20, 0.35],        # tight vs relaxed wick
    "sl_buffer_pct":        [0.0005, 0.002],     # tight vs wider SL buffer
    "proximity_filter_pct": [0.005, 0.015],      # closer vs further from level
}

MIN_TRADES = 3  # minimum closed trades for a grid result to be valid


# ─────────────────────────────────────────────────────────────
# HTF WALL CALCULATION (mirrors prefetch_all_htf_liquidity)
# ─────────────────────────────────────────────────────────────

def _calc_integrity(wall_wick: float, body: float, wall_wick_pct: float) -> bool:
    """Returns True if wall wick is clean enough (mirrors calc_integrity_score == 100)."""
    return wall_wick <= body * wall_wick_pct


def compute_htf_pools(daily_df: pd.DataFrame, wall_wick_pct: float, fuel_wick_pct: float) -> dict:
    """
    Given a slice of daily data, compute PDH/PDL/PWH/PWL/PMH/PML walls.
    Returns a dict in the same format as LIQUIDITY_CACHE in scanner.py.
    Uses the second-to-last row as the "previous closed candle".
    """
    if len(daily_df) < 5:
        return {}

    def make_candle(row):
        return {"t": str(row.name), "o": float(row["Open"]), "h": float(row["High"]),
                "l": float(row["Low"]), "c": float(row["Close"])}

    # --- Daily (previous closed candle) ---
    d = daily_df.iloc[-2]
    d_o, d_h, d_l, d_c = float(d["Open"]), float(d["High"]), float(d["Low"]), float(d["Close"])
    d_body = abs(d_c - d_o) or 0.001

    pdh_wall_wick = d_h - d_o
    pdh_fuel_wick = d_c - d_l
    pdl_wall_wick = d_o - d_l
    pdl_fuel_wick = d_h - d_c

    pdh_wall = (d_c < d_o) and _calc_integrity(pdh_wall_wick, d_body, wall_wick_pct) and (pdh_fuel_wick > d_body * fuel_wick_pct)
    pdl_wall = (d_c > d_o) and _calc_integrity(pdl_wall_wick, d_body, wall_wick_pct) and (pdl_fuel_wick > d_body * fuel_wick_pct)

    # --- Weekly ---
    weekly = daily_df.resample("W").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
    pools = {
        "PDH": d_h, "PDL": d_l, "PDH_WALL": pdh_wall, "PDL_WALL": pdl_wall,
        "PDH_CANDLE": make_candle(d), "PDL_CANDLE": make_candle(d),
        "PDH_INTEGRITY": pdh_wall, "PDL_INTEGRITY": pdl_wall,
    }

    if len(weekly) >= 2:
        w = weekly.iloc[-2]
        w_o, w_h, w_l, w_c = float(w["Open"]), float(w["High"]), float(w["Low"]), float(w["Close"])
        w_body = abs(w_c - w_o) or 0.001
        pwh_wall_wick = w_h - w_o
        pwh_fuel_wick = w_c - w_l
        pwl_wall_wick = w_o - w_l
        pwl_fuel_wick = w_h - w_c
        pwh_wall = (w_c < w_o) and _calc_integrity(pwh_wall_wick, w_body, wall_wick_pct) and (pwh_fuel_wick > w_body * fuel_wick_pct)
        pwl_wall = (w_c > w_o) and _calc_integrity(pwl_wall_wick, w_body, wall_wick_pct) and (pwl_fuel_wick > w_body * fuel_wick_pct)
        pools.update({
            "PWH": w_h, "PWL": w_l, "PWH_WALL": pwh_wall, "PWL_WALL": pwl_wall,
            "PWH_CANDLE": make_candle(w), "PWL_CANDLE": make_candle(w),
        })

    # --- Monthly ---
    monthly = daily_df.resample("ME").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
    if len(monthly) >= 2:
        m = monthly.iloc[-2]
        m_o, m_h, m_l, m_c = float(m["Open"]), float(m["High"]), float(m["Low"]), float(m["Close"])
        m_body = abs(m_c - m_o) or 0.001
        pmh_wall_wick = m_h - m_o
        pmh_fuel_wick = m_c - m_l
        pml_wall_wick = m_o - m_l
        pml_fuel_wick = m_h - m_c
        pmh_wall = (m_c < m_o) and _calc_integrity(pmh_wall_wick, m_body, wall_wick_pct) and (pmh_fuel_wick > m_body * fuel_wick_pct)
        pml_wall = (m_c > m_o) and _calc_integrity(pml_wall_wick, m_body, wall_wick_pct) and (pml_fuel_wick > m_body * fuel_wick_pct)
        pools.update({
            "PMH": m_h, "PML": m_l, "PMH_WALL": pmh_wall, "PML_WALL": pml_wall,
            "PMH_CANDLE": make_candle(m), "PML_CANDLE": make_candle(m),
        })

    return pools


# ─────────────────────────────────────────────────────────────
# RECLAIM DETECTION (mirrors update_signal_lifecycle)
# ─────────────────────────────────────────────────────────────

def find_reclaim(pools: dict, hourly_window: pd.DataFrame, current_price: float, params: ScannerParams) -> dict | None:
    """
    Scans a 1H window (last 24 candles) for a valid reclaim of any HTF wall.
    Returns a signal dict or None.
    Mirrors the FASE 3 (ENTRY) block in update_signal_lifecycle().
    """
    levels = []
    for code, l_type, score in [
        ("PMH", "bearish", "A+++"), ("PML", "bullish", "A+++"),
        ("PWH", "bearish", "A++"),  ("PWL", "bullish", "A++"),
        ("PDH", "bearish", "A+"),   ("PDL", "bullish", "A+"),
    ]:
        if not pools.get(f"{code}_WALL"):
            continue
        lv_val = pools.get(code)
        if lv_val is None:
            continue
        levels.append((code, l_type, score, lv_val))

    for code, l_type, d_score, lv_val in levels:
        lookback_window = hourly_window.iloc[-min(24, len(hourly_window) - 1):-1]
        if lookback_window.empty:
            continue

        for i in range(len(lookback_window) - 1, -1, -1):
            c = lookback_window.iloc[i]
            c_o, c_c = float(c["Open"]), float(c["Close"])
            c_h, c_l = float(c["High"]), float(c["Low"])

            # Reclaim bearish
            if l_type == "bearish" and c_h > lv_val and c_c < lv_val and c_c < c_o:
                pass
            # Reclaim bullish
            elif l_type == "bullish" and c_l < lv_val and c_c > lv_val and c_c > c_o:
                pass
            else:
                continue

            # Displacement check
            c_body = abs(c_c - c_o)
            prev_slice = lookback_window.iloc[max(0, i - 10):i]
            avg_body = (prev_slice["Close"] - prev_slice["Open"]).abs().mean() if not prev_slice.empty else 0.001
            avg_body = avg_body or 0.001

            has_displacement = c_body > avg_body * params.displacement_mult
            if l_type == "bearish":
                has_displacement = has_displacement and (c_h - max(c_o, c_c) < c_body * params.opposite_wick_tol)
            else:
                has_displacement = has_displacement and (min(c_o, c_c) - c_l < c_body * params.opposite_wick_tol)

            if not has_displacement:
                continue

            # Proximity filter
            if l_type == "bullish" and current_price > lv_val * (1 + params.proximity_filter_pct):
                continue
            if l_type == "bearish" and current_price < lv_val * (1 - params.proximity_filter_pct):
                continue

            entry = lv_val
            sl = (c_h * (1 + params.sl_buffer_pct)) if l_type == "bearish" else (c_l * (1 - params.sl_buffer_pct))
            wall_candle = pools.get(f"{code}_CANDLE", {})
            tp = wall_candle.get("l") if l_type == "bearish" else wall_candle.get("h")
            if tp is None:
                continue

            # Sanity checks
            if l_type == "bullish" and current_price <= sl:
                continue
            if l_type == "bearish" and current_price >= sl:
                continue
            if l_type == "bearish" and current_price <= tp:
                continue
            if l_type == "bullish" and current_price >= tp:
                continue

            sl_dist = abs(entry - sl)
            tp_dist = abs(entry - tp)
            if sl_dist == 0 or tp_dist == 0:
                continue

            return {
                "direction": l_type,
                "tier": code,
                "diamond_score": d_score,
                "entry": entry,
                "stop": sl,
                "target": tp,
                "rr": round(tp_dist / sl_dist, 2),
                "reclaim_candle_time": str(c.name),
            }

    return None


# ─────────────────────────────────────────────────────────────
# SIMULATION ENGINE
# ─────────────────────────────────────────────────────────────

def simulate(ticker: str, daily_df: pd.DataFrame, hourly_df: pd.DataFrame,
             params: ScannerParams, verbose: bool = False) -> list:
    """
    Sliding window backtest: advances one day at a time, recomputes HTF walls,
    searches for reclaim in the 24 preceding 1H candles, then simulates the trade.
    """
    trades = []
    h_tz = hourly_df.index.tz

    # Need at least 30 daily candles for monthly resampling
    for i in range(30, len(daily_df)):
        daily_window = daily_df.iloc[:i]

        # Compute HTF walls on this window
        pools = compute_htf_pools(daily_window, params.wall_wick_pct, params.fuel_wick_pct)
        if not pools:
            continue

        # Get current price (last close in daily window)
        current_price = float(daily_window.iloc[-1]["Close"])

        # Map daily index to hourly
        day_ts = daily_window.index[-1]
        if h_tz is not None and day_ts.tzinfo is None:
            day_ts = day_ts.tz_localize(h_tz)
        elif h_tz is None and day_ts.tzinfo is not None:
            day_ts = day_ts.tz_localize(None)

        # 24 1H candles ending at this daily close
        prior_1h = hourly_df[hourly_df.index <= day_ts].tail(25)
        if len(prior_1h) < 5:
            continue

        signal = find_reclaim(pools, prior_1h, current_price, params)
        if not signal:
            continue

        entry, stop, target, rr = signal["entry"], signal["stop"], signal["target"], signal["rr"]
        entry_ts = day_ts

        # Simulate outcome on subsequent 1H candles
        result = "OPEN"
        close_ts = None
        direction = signal["direction"]

        for ts, candle in hourly_df[hourly_df.index > entry_ts].iterrows():
            c_h, c_l = float(candle["High"]), float(candle["Low"])
            if direction == "bullish":
                sl_hit = c_l <= stop
                tp_hit = c_h >= target
            else:
                sl_hit = c_h >= stop
                tp_hit = c_l <= target

            if sl_hit and tp_hit:
                result, close_ts = "LOSS", ts
                break
            elif sl_hit:
                result, close_ts = "LOSS", ts
                break
            elif tp_hit:
                result, close_ts = "WIN", ts
                break

        trades.append({
            "ticker": ticker,
            "day": str(daily_window.index[-1].date()),
            "tier": signal["tier"],
            "direction": direction,
            "diamond_score": signal["diamond_score"],
            "entry": round(entry, 4),
            "stop": round(stop, 4),
            "target": round(target, 4),
            "rr": rr,
            "result": result,
            "close_ts": str(close_ts) if close_ts else None,
        })

        if verbose:
            icon = "WIN" if result == "WIN" else ("LOSS" if result == "LOSS" else "...")
            print(f"  {daily_window.index[-1].date()}  {signal['tier']:4s}  {direction.upper():8s}  "
                  f"RR={rr:.1f}  entry={entry:.2f}  SL={stop:.2f}  TP={target:.2f}  → {icon}")

    return trades


# ─────────────────────────────────────────────────────────────
# STATISTICS
# ─────────────────────────────────────────────────────────────

def compute_stats(trades: list) -> dict | None:
    closed = [t for t in trades if t["result"] in ("WIN", "LOSS")]
    if not closed:
        return None
    wins   = [t for t in closed if t["result"] == "WIN"]
    losses = [t for t in closed if t["result"] == "LOSS"]
    total  = len(closed)
    total_r = sum(t["rr"] for t in wins) - len(losses)

    # Breakdown by tier
    tier_stats = {}
    for t in closed:
        tier = t["tier"]
        if tier not in tier_stats:
            tier_stats[tier] = {"win": 0, "loss": 0}
        tier_stats[tier][t["result"].lower()] += 1

    return {
        "total":      total,
        "wins":       len(wins),
        "losses":     len(losses),
        "winrate":    round(len(wins) / total * 100, 1),
        "total_r":    round(total_r, 2),
        "expectancy": round(total_r / total, 3),
        "avg_rr":     round(sum(t["rr"] for t in wins) / len(wins), 2) if wins else 0.0,
        "tier_stats": tier_stats,
    }


def print_report(stats: dict | None, ticker: str, params: ScannerParams):
    print(f"\n{'='*60}")
    print(f"  {ticker}")
    print(f"  wall_wick={params.wall_wick_pct*100:.3f}%  fuel_wick={params.fuel_wick_pct*100:.0f}%  "
          f"disp={params.displacement_mult}x  opp_wick={params.opposite_wick_tol*100:.0f}%  "
          f"sl_buf={params.sl_buffer_pct*100:.2f}%  prox={params.proximity_filter_pct*100:.1f}%")
    print(f"{'='*60}")
    if not stats:
        print("  No closed trades found.")
        return
    print(f"  Trades     : {stats['total']}  ({stats['wins']}W / {stats['losses']}L)")
    print(f"  Winrate    : {stats['winrate']}%")
    print(f"  Total R    : {stats['total_r']:+.2f} R")
    print(f"  Expectancy : {stats['expectancy']:+.3f} R/trade")
    print(f"  Avg Win RR : {stats['avg_rr']:.2f} R")
    if stats["tier_stats"]:
        print(f"\n  By Tier:")
        for tier, s in sorted(stats["tier_stats"].items()):
            total_t = s["win"] + s["loss"]
            wr = round(s["win"] / total_t * 100, 1) if total_t else 0
            print(f"    {tier:4s}: {total_t} trades  {wr}% WR")
    edge = "POSITIVE EDGE" if stats["total_r"] > 0 else "NEGATIVE EDGE"
    print(f"\n  >>> {edge} <<<")


# ─────────────────────────────────────────────────────────────
# GRID SEARCH
# ─────────────────────────────────────────────────────────────

def grid_search(ticker: str, daily_df: pd.DataFrame, hourly_df: pd.DataFrame,
                param_keys: list | None = None) -> dict | None:
    """
    Grid search over the specified param_keys (default: all 6).
    Returns the best parameter dict sorted by expectancy.
    """
    if param_keys is None:
        param_keys = list(PARAM_GRID.keys())

    grid = {k: PARAM_GRID[k] for k in param_keys}
    keys = list(grid.keys())
    combos = list(itertools.product(*grid.values()))
    print(f"\n[GRID SEARCH] {ticker} — {len(combos)} combos over: {keys}")

    # Default values for params not in the grid
    defaults = ScannerParams()
    rows = []

    for idx, combo in enumerate(combos, 1):
        print(f"  [{idx}/{len(combos)}] testing {dict(zip(keys, combo))}...", end="\r", flush=True)
        kw = {k: v for k, v in zip(keys, combo)}
        p = ScannerParams(
            wall_wick_pct        = kw.get("wall_wick_pct",        defaults.wall_wick_pct),
            fuel_wick_pct        = kw.get("fuel_wick_pct",        defaults.fuel_wick_pct),
            displacement_mult    = kw.get("displacement_mult",    defaults.displacement_mult),
            opposite_wick_tol    = kw.get("opposite_wick_tol",    defaults.opposite_wick_tol),
            sl_buffer_pct        = kw.get("sl_buffer_pct",        defaults.sl_buffer_pct),
            proximity_filter_pct = kw.get("proximity_filter_pct", defaults.proximity_filter_pct),
        )
        trades = simulate(ticker, daily_df, hourly_df, p)
        stats  = compute_stats(trades)
        if stats and stats["total"] >= MIN_TRADES:
            rows.append({**kw, **stats})

    if not rows:
        print(f"  No combo produced >= {MIN_TRADES} closed trades.")
        return None

    df = pd.DataFrame(rows).sort_values("expectancy", ascending=False)
    display_cols = keys + ["total", "winrate", "total_r", "expectancy", "avg_rr"]
    print(f"\n  TOP RESULTS (sorted by Expectancy):")
    print(df[display_cols].head(10).to_string(index=False))

    best = df.iloc[0].to_dict()
    best["_ticker"] = ticker
    print(f"\n  BEST PARAMS:")
    for k in keys:
        current = getattr(defaults, k)
        print(f"    {k}: {current} → {best[k]}")
    print(f"\n  RECOMMENDATION — update scanner.py:")
    for k in keys:
        if k == "wall_wick_pct":
            print(f"    calc_integrity_score: body * {best[k]}  (was {defaults.wall_wick_pct})")
        elif k == "fuel_wick_pct":
            print(f"    fuel_wick condition:  body * {best[k]}  (was {defaults.fuel_wick_pct})")
        elif k == "displacement_mult":
            print(f"    displacement check:   avg_body * {best[k]}  (was {defaults.displacement_mult})")
        elif k == "opposite_wick_tol":
            print(f"    opposite wick tol:    c_body * {best[k]}  (was {defaults.opposite_wick_tol})")
        elif k == "sl_buffer_pct":
            print(f"    SL buffer:            * {1 + best[k]:.4f} / * {1 - best[k]:.4f}  (was {defaults.sl_buffer_pct})")
        elif k == "proximity_filter_pct":
            print(f"    proximity filter:     lv_val * {1 + best[k]:.3f}  (was {defaults.proximity_filter_pct})")

    return best


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CRT Flow Backtester v3 — aligned with scanner.py")
    parser.add_argument("--ticker",     type=str,   default="AAPL",  help="Ticker (e.g. AAPL, NVDA)")
    parser.add_argument("--period",     type=str,   default="2y",    help="Daily history period (1y/2y/5y)")
    parser.add_argument("--optimize",   action="store_true",          help="Grid search over parameters")
    parser.add_argument("--params",     nargs="+",  default=None,
                        choices=list(PARAM_GRID.keys()),
                        help="Which params to optimize (default: all)")
    parser.add_argument("--verbose",    action="store_true",          help="Print each trade")
    # Single-run param overrides
    parser.add_argument("--wall-wick",     type=float, default=0.001)
    parser.add_argument("--fuel-wick",     type=float, default=0.40)
    parser.add_argument("--displacement",  type=float, default=1.2)
    parser.add_argument("--opp-wick",      type=float, default=0.30)
    parser.add_argument("--sl-buffer",     type=float, default=0.001)
    parser.add_argument("--proximity",     type=float, default=0.01)
    args = parser.parse_args()

    ticker = args.ticker.upper()
    print(f"\n[*] CRT Flow Backtester v3 (scanner.py logic) — {ticker}")
    print(f"[*] Downloading {args.period} daily + 730d 1H data...")

    obj       = yf.Ticker(ticker)
    daily_df  = obj.history(period=args.period, interval="1d")
    daily_df.dropna(inplace=True)
    hourly_df = obj.history(period="730d", interval="1h")
    hourly_df.dropna(inplace=True)

    print(f"[+] Daily: {len(daily_df)} candles | 1H: {len(hourly_df)} candles\n")

    if args.optimize:
        grid_search(ticker, daily_df, hourly_df, param_keys=args.params)
    else:
        params = ScannerParams(
            wall_wick_pct        = args.wall_wick,
            fuel_wick_pct        = args.fuel_wick,
            displacement_mult    = args.displacement,
            opposite_wick_tol    = args.opp_wick,
            sl_buffer_pct        = args.sl_buffer,
            proximity_filter_pct = args.proximity,
        )
        trades = simulate(ticker, daily_df, hourly_df, params, verbose=args.verbose)
        stats  = compute_stats(trades)
        print_report(stats, ticker, params)


if __name__ == "__main__":
    main()
