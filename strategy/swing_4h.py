"""Causal, end-of-session swing candidates. Paper only; no broker orders."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json

import numpy as np
import pandas as pd

from signal_context import bar_end

STRATEGY = "swing_pullback_4h_v1"
BENCHMARK = "swing_breakout_4h_v1"
OHLC = ["Open", "High", "Low", "Close"]


@dataclass(frozen=True)
class SwingConfig:
    ema_fast: int = 20
    ema_slow: int = 50
    daily_sma: int = 200
    atr_period: int = 14
    pullback_bars: int = 3
    breakout_bars: int = 20
    stop_buffer_atr: float = 0.1
    breakout_stop_atr: float = 2.0
    target_r: float = 2.0
    min_entry_rr: float = 1.5
    min_price: float = 5.0
    liquidity_days: int = 20
    min_avg_volume: float = 1_000_000.0
    max_hold_bars: int = 20
    initial_capital: float = 10_000.0
    risk_fraction: float = 0.005
    max_positions: int = 5
    max_position_fraction: float = 0.2
    fee_bps: float = 5.0
    slippage_bps: float = 5.0

    def __post_init__(self):
        ints = (self.ema_fast, self.ema_slow, self.daily_sma, self.atr_period,
                self.pullback_bars, self.breakout_bars, self.max_hold_bars, self.max_positions,
                self.liquidity_days)
        if any(type(n) is not int or n < 1 for n in ints) or self.ema_fast >= self.ema_slow:
            raise ValueError("Invalid swing periods or position count")
        positive = (self.stop_buffer_atr, self.breakout_stop_atr, self.target_r,
                    self.min_entry_rr, self.min_price, self.initial_capital,
                    self.risk_fraction, self.max_position_fraction, self.min_avg_volume)
        if not all(np.isfinite(n) and n > 0 for n in positive):
            raise ValueError("Swing parameters must be finite and positive")
        if (self.risk_fraction > 1 or self.max_position_fraction > 1
                or self.min_entry_rr > self.target_r):
            raise ValueError("Invalid swing sizing or reward/risk")
        if not all(np.isfinite(n) and n >= 0 for n in (self.fee_bps, self.slippage_bps)):
            raise ValueError("Execution costs cannot be negative")

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:16]


def utc(value):
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError("A valid timestamp is required")
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def checked_frame(frame):
    out = frame.loc[:, OHLC].astype(float).copy()
    out.index = pd.to_datetime(out.index, utc=True)
    if not out.index.is_unique or not out.index.is_monotonic_increasing:
        raise ValueError("OHLC index must be unique and increasing")
    if not np.isfinite(out.to_numpy()).all() or (out <= 0).any().any():
        raise ValueError("OHLC must contain finite positive prices")
    if ((out.Low > out[["Open", "Close"]].min(axis=1))
            | (out.High < out[["Open", "Close"]].max(axis=1))).any():
        raise ValueError("Invalid OHLC geometry")
    return out


def snapshot(frame, ts):
    return {"at": pd.Timestamp(ts).isoformat(),
            **{c.lower(): float(frame.loc[ts, c]) for c in OHLC}}


def candidates(four_hour, daily, symbol, *, config=None, variant=STRATEGY, as_of=None):
    """All indicators are causal. Confirm only the 13:30-16:00 NY closing bar.

    Daily values closing exactly with that bar are available to the evening
    decision. Execution is always a later regular-session open, never this close.
    """
    cfg = config or SwingConfig()
    if variant not in (STRATEGY, BENCHMARK):
        raise ValueError("Unknown swing variant")
    cutoff = utc(as_of if as_of is not None else pd.Timestamp.now(tz="UTC"))
    frame = checked_frame(four_hour)
    frame = frame.loc[bar_end(frame.index, "4H") <= cutoff]
    days = daily.copy()
    idx = pd.DatetimeIndex(days.index)
    if idx.tz is None and (idx == idx.normalize()).all():
        days.index = idx.tz_localize("America/New_York")
    days = checked_frame(days)
    if "Volume" not in daily:
        raise ValueError("Daily volume is required to verify liquidity")
    days["Volume"] = pd.to_numeric(daily["Volume"], errors="coerce").to_numpy()
    if (days.Volume.dropna() < 0).any() or not np.isfinite(days.Volume.dropna()).all():
        raise ValueError("Daily volume must be finite and nonnegative")
    days = days.loc[bar_end(days.index, "1D") <= cutoff]
    if frame.empty or days.empty:
        return []
    fast = frame.Close.ewm(span=cfg.ema_fast, adjust=False, min_periods=cfg.ema_fast).mean()
    slow = frame.Close.ewm(span=cfg.ema_slow, adjust=False, min_periods=cfg.ema_slow).mean()
    tr = pd.concat([frame.High-frame.Low, (frame.High-frame.Close.shift()).abs(),
                    (frame.Low-frame.Close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(cfg.atr_period, min_periods=cfg.atr_period).mean()
    refs = pd.DataFrame({"daily_close": days.Close, "daily_at": days.index,
                         "daily_sma": days.Close.rolling(cfg.daily_sma).mean(),
                         "avg_volume": days.Volume.rolling(cfg.liquidity_days).mean(),
                         "daily_available_at": bar_end(days.index, "1D")}).reset_index(drop=True)
    end = bar_end(frame.index, "4H")
    known = pd.merge_asof(pd.DataFrame({"at": frame.index, "end": end}),
                          refs.sort_values("daily_available_at"), left_on="end",
                          right_on="daily_available_at", direction="backward").set_index("at")
    ny = frame.index.tz_convert("America/New_York")
    evening = (ny.hour == 13) & (ny.minute == 30)
    pullback = ((frame.Close.shift() <= fast.shift()) & (frame.Close > fast)
                & (frame.Close > frame.High.shift()))
    breakout = frame.Close > frame.High.shift().rolling(cfg.breakout_bars).max()
    valid = (evening & (frame.Close >= cfg.min_price) & (fast > slow)
             & (known.daily_close > known.daily_sma) & (known.avg_volume >= cfg.min_avg_volume) & (atr > 0)
             & (pullback if variant == STRATEGY else breakout))
    entries = []
    for ts in frame.index[valid.fillna(False)]:
        i = frame.index.get_loc(ts)
        row = frame.loc[ts]
        stop = (float(frame.Low.iloc[max(0, i-cfg.pullback_bars+1):i+1].min())
                - cfg.stop_buffer_atr*float(atr.loc[ts])) if variant == STRATEGY else (
                    float(row.Close)-cfg.breakout_stop_atr*float(atr.loc[ts]))
        risk = float(row.Close)-stop
        if stop <= 0 or risk <= 0:
            continue
        target = float(row.Close)+cfg.target_r*risk
        ref = known.loc[ts]
        available = bar_end(frame.index[i:i+1], "4H")[0].isoformat()
        identity = f"{variant}|{cfg.fingerprint}|{symbol.upper()}|{ts.isoformat()}"
        entries.append({
            "id": hashlib.sha256(identity.encode()).hexdigest()[:24],
            "strategy": variant, "config_hash": cfg.fingerprint, "symbol": symbol.upper(),
            "direction": "LONG", "pattern_at": ts.isoformat(), "available_at": available,
            "first_seen_at": available, "signal_close": float(row.Close),
            "stop_loss": stop, "target": target,
            "max_entry": (target+cfg.min_entry_rr*stop)/(1+cfg.min_entry_rr),
            "daily_at": pd.Timestamp(ref.daily_at).isoformat(),
            "daily_available_at": pd.Timestamp(ref.daily_available_at).isoformat(),
            "daily_close": float(ref.daily_close), "daily_sma": float(ref.daily_sma),
            "avg_volume": float(ref.avg_volume),
            "ema_fast": float(fast.loc[ts]), "ema_slow": float(slow.loc[ts]),
            "atr": float(atr.loc[ts]), "status": "PENDING",
            "source_candles": [snapshot(frame, p) for p in
                               frame.index[max(0, i-cfg.pullback_bars):i+1]],
        })
    return entries
