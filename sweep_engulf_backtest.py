"""
Grid backtest for Sweep & Engulf on 1H / 4H.

Usage:
    python sweep_engulf_backtest.py --symbol AAPL --timeframe 1H
    python sweep_engulf_backtest.py --symbol AAPL --optimize
"""
from __future__ import annotations

import argparse
import itertools
from dataclasses import dataclass

import pandas as pd
import yfinance as yf

from market_data import clean_df, resample_to_4h
from strategy import config as cfg
from strategy.sweep_engulf import detect_latest_pattern


@dataclass
class TradeResult:
    direction: str
    entry: float
    sl: float
    tp: float
    outcome: str  # WIN | LOSS | OPEN


def _bars(symbol: str, timeframe: str) -> pd.DataFrame | None:
    df = yf.Ticker(symbol).history(period="730d", interval="1h", auto_adjust=True)
    df = clean_df(df.dropna() if df is not None else None)
    if df is None or df.empty:
        return None
    if timeframe.upper() == "4H":
        return resample_to_4h(df)
    return df


def _simulate(df: pd.DataFrame, i: int, direction: str, entry: float, sl: float, tp: float) -> str:
    for j in range(i + 1, len(df)):
        bar = df.iloc[j]
        hi = float(bar["High"])
        lo = float(bar["Low"])
        if direction == "BULLISH":
            if lo <= sl:
                return "LOSS"
            if hi >= tp:
                return "WIN"
        else:
            if hi >= sl:
                return "LOSS"
            if lo <= tp:
                return "WIN"
    return "OPEN"


def run_backtest(
    symbol: str,
    timeframe: str,
    *,
    lookback: int,
    rr: float,
    use_volume: bool,
) -> dict:
    df = _bars(symbol, timeframe)
    if df is None or len(df) < lookback + 5:
        return {"trades": 0, "winrate": 0.0, "expectancy_r": 0.0}

    old_lb, old_rr, old_vol = cfg.SWING_LOOKBACK, cfg.RR_RATIO, cfg.USE_VOLUME_FILTER
    cfg.SWING_LOOKBACK = lookback
    cfg.RR_RATIO = rr
    cfg.USE_VOLUME_FILTER = use_volume

    trades: list[TradeResult] = []
    try:
        for i in range(lookback + 1, len(df)):
            window = df.iloc[: i + 1]
            sig = detect_latest_pattern(window, timeframe.upper())  # type: ignore[arg-type]
            if sig is None:
                continue
            ts = window.index[-1]
            if trades and trades[-1].outcome == "OPEN":
                continue
            outcome = _simulate(
                df,
                i,
                sig["direction"],
                float(sig["entry_price"]),
                float(sig["stop_loss"]),
                float(sig["take_profit"]),
            )
            trades.append(
                TradeResult(
                    direction=sig["direction"],
                    entry=float(sig["entry_price"]),
                    sl=float(sig["stop_loss"]),
                    tp=float(sig["take_profit"]),
                    outcome=outcome,
                )
            )
            _ = ts
    finally:
        cfg.SWING_LOOKBACK, cfg.RR_RATIO, cfg.USE_VOLUME_FILTER = old_lb, old_rr, old_vol

    closed = [t for t in trades if t.outcome in ("WIN", "LOSS")]
    if not closed:
        return {"trades": 0, "winrate": 0.0, "expectancy_r": 0.0}
    wins = sum(1 for t in closed if t.outcome == "WIN")
    winrate = wins / len(closed)
    expectancy = sum(rr if t.outcome == "WIN" else -1.0 for t in closed) / len(closed)
    return {
        "trades": len(closed),
        "winrate": round(winrate * 100, 1),
        "expectancy_r": round(expectancy, 3),
    }


def optimize(symbol: str, timeframe: str) -> None:
    grid = []
    for lb, rr, vol in itertools.product([2, 3, 5], [1.5, 2.0, 2.5, 3.0], [True, False]):
        stats = run_backtest(symbol, timeframe, lookback=lb, rr=rr, use_volume=vol)
        if stats["trades"] >= 5:
            grid.append({"lookback": lb, "rr": rr, "volume": vol, **stats})

    grid.sort(key=lambda x: (x["expectancy_r"], x["winrate"]), reverse=True)
    print(f"\n=== {symbol} {timeframe} top configs (min 5 trades) ===")
    for row in grid[:10]:
        print(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sweep & Engulf backtest / grid")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--timeframe", default="1H", choices=["1H", "4H"])
    parser.add_argument("--optimize", action="store_true")
    args = parser.parse_args()

    if args.optimize:
        optimize(args.symbol, args.timeframe)
        return

    stats = run_backtest(
        args.symbol,
        args.timeframe,
        lookback=cfg.SWING_LOOKBACK,
        rr=cfg.RR_RATIO,
        use_volume=cfg.USE_VOLUME_FILTER,
    )
    print(f"{args.symbol} {args.timeframe} default config: {stats}")


if __name__ == "__main__":
    main()
