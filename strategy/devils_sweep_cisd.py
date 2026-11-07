"""Experimental 4H paper rule: previous-day sweep -> nearby Devil -> closed CISD.

Entries are causal; outcomes are evaluated separately. No production radar writes.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import hashlib
import json

import numpy as np
import pandas as pd

from signal_context import bar_end
from strategy.devils_markers import _is_open_eq_high, _is_open_eq_low

STRATEGY = "devils_sweep_cisd_v2"
TIMEFRAME = "4H"
OHLC = ("Open", "High", "Low", "Close")


@dataclass(frozen=True)
class SweepCisdConfig:
    max_setup_bars: int = 3
    atr_period: int = 14
    marker_distance_atr: float = 0.5
    stop_buffer_atr: float = 0.1
    min_buffer: float = 0.01
    min_rr: float = 1.5
    max_rr: float = 25.0
    min_risk_pct: float = 0.0008
    max_target_move_pct: float = 0.35
    allow_same_bar_cisd: bool = True

    def __post_init__(self):
        if (not isinstance(self.max_setup_bars, int) or not isinstance(self.atr_period, int)
                or self.max_setup_bars < 1 or self.atr_period < 2):
            raise ValueError("Setup window and ATR period must be positive integers")
        values = (self.marker_distance_atr, self.stop_buffer_atr, self.min_buffer,
                  self.min_rr, self.max_rr, self.min_risk_pct, self.max_target_move_pct)
        if not all(np.isfinite(v) and v > 0 for v in values) or self.max_rr < self.min_rr:
            raise ValueError("Invalid sweep/CISD risk or distance parameters")
        if not isinstance(self.allow_same_bar_cisd, bool):
            raise ValueError("Same-bar confirmation must be a boolean")

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()[:16]


def utc_index(index) -> pd.DatetimeIndex:
    return pd.to_datetime(index, utc=True)


def checked_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.loc[:, list(OHLC)].astype(float).copy()
    frame.index = utc_index(frame.index)
    if not frame.index.is_unique or not frame.index.is_monotonic_increasing:
        raise ValueError("OHLC index must be unique and increasing")
    if not np.isfinite(frame.to_numpy()).all() or (frame <= 0).any().any():
        raise ValueError("OHLC prices must be finite and positive")
    bad = ((frame.Low > frame[["Open", "Close"]].min(axis=1)) |
           (frame.High < frame[["Open", "Close"]].max(axis=1)))
    if bad.any():
        raise ValueError("Invalid OHLC geometry")
    return frame


def previous_day_levels(four_hour: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    """Only use daily highs/lows whose session already ended before the 4H bar opens."""
    bars = checked_frame(four_hour)
    daily = daily.copy()
    idx = pd.DatetimeIndex(daily.index)
    if idx.tz is None and (idx == idx.normalize()).all():
        daily.index = idx.tz_localize("America/New_York")
    days = checked_frame(daily)
    right = days[["High", "Low"]].copy()
    right["reference_at"] = days.index
    right["available_at"] = bar_end(days.index, "1D")
    right = right.reset_index(drop=True).sort_values("available_at")
    left = pd.DataFrame({"bar_at": bars.index})
    joined = pd.merge_asof(left, right, left_on="bar_at", right_on="available_at",
                           direction="backward", allow_exact_matches=False)
    return joined.set_index("bar_at").reindex(bars.index)


def _snapshot(frame: pd.DataFrame, i: int) -> dict:
    return {"at": frame.index[i].isoformat(),
            **{key.lower(): float(frame.iloc[i][key]) for key in OHLC}}


def _opposite_run(frame: pd.DataFrame, i: int, long: bool) -> list[dict]:
    """Freeze the immediately preceding opposite-colour run, including a matching sweep bar."""
    def opposite(j):
        row = frame.iloc[j]
        return row.Close < row.Open if long else row.Close > row.Open
    end = i if opposite(i) else i - 1
    if end < 0 or not opposite(end):
        return []
    start = end
    while start > 0 and opposite(start - 1):
        start -= 1
    return [_snapshot(frame, j) for j in range(start, end + 1)]


def _entry(frame, i, setup, ticker, config, require_marker):
    long = setup["direction"] == "BULLISH"
    entry, stop, target = float(frame.iloc[i].Close), setup["stop_loss"], setup["target"]
    risk = entry - stop if long else stop - entry
    reward = target - entry if long else entry - target
    rr = reward / risk if risk > 0 else None
    setup.update(cisd_at=frame.index[i].isoformat(), cisd_candle=_snapshot(frame, i),
                 candidate_entry=entry, candidate_risk=risk, candidate_reward=reward,
                 candidate_rr=rr)
    if min(entry, stop, target) <= 0 or risk <= 0 or reward <= 0:
        setup["risk_rejection"] = "INVALID_TRADE_LEVELS"
        return None
    if risk / entry < config.min_risk_pct:
        setup["risk_rejection"] = "STOP_TOO_CLOSE"
        return None
    if reward / entry > config.max_target_move_pct:
        setup["risk_rejection"] = "TARGET_TOO_FAR"
        return None
    if not config.min_rr <= rr <= config.max_rr:
        setup["risk_rejection"] = "RR_BELOW_MINIMUM" if rr < config.min_rr else "RR_ABOVE_MAXIMUM"
        return None
    setup.update(status="ENTRY_READY")
    return {"ticker": ticker.upper(), "timeframe": TIMEFRAME, "direction": setup["direction"],
            "pattern_at": frame.index[i].isoformat(), "entry_price": entry,
            "stop_loss": stop, "target": target, "rr_ratio": rr,
            "devils_marker_price": (setup.get("marker_candle") or {}).get("open"),
            "cisd_level": setup["cisd_level"],
            "signal_available_at": bar_end(frame.index[i:i + 1], TIMEFRAME)[0].isoformat(),
            "setup": deepcopy(setup), "config_hash": config.fingerprint,
            "require_marker": require_marker}


def find_entries(four_hour: pd.DataFrame, levels: pd.DataFrame, ticker: str, *,
                 config: SweepCisdConfig | None = None, require_marker: bool = True,
                 as_of=None) -> dict:
    """Return entries and setup diagnostics; every decision uses data available at that close.

    A single pending setup is tracked. Completed entries are independent simulated
    signals, as in Pure No-Wick; these are not portfolio or broker orders.
    """
    config = config or SweepCisdConfig()
    frame = checked_frame(four_hour)
    cutoff = pd.Timestamp.now(tz="UTC") if as_of is None else pd.Timestamp(as_of)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff.tz_convert("UTC")
    frame = frame.loc[bar_end(frame.index, TIMEFRAME) <= cutoff]
    refs = levels.copy()
    refs.index = utc_index(refs.index)
    if not refs.index.is_unique:
        raise ValueError("Liquidity level index must be unique")
    refs = refs.reindex(frame.index)
    tr = pd.concat([(frame.High - frame.Low), (frame.High - frame.Close.shift()).abs(),
                    (frame.Low - frame.Close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(config.atr_period, min_periods=config.atr_period).mean()
    entries, setups = [], []
    pending = None
    sweep_i = -1
    for i, (ts, row) in enumerate(frame.iterrows()):
        if pending is not None:
            long = pending["direction"] == "BULLISH"
            invalid = row.Low < pending["sweep_extreme"] if long else row.High > pending["sweep_extreme"]
            consumed = row.High >= pending["target"] if long else row.Low <= pending["target"]
            if i - sweep_i > config.max_setup_bars or invalid or consumed:
                reason = "EXPIRED" if i - sweep_i > config.max_setup_bars else "SWEEP_INVALIDATED" if invalid else "TARGET_CONSUMED"
                pending.update(status=reason, ended_at=ts.isoformat())
                pending = None
            else:
                mark = ((_is_open_eq_low(row.Open, row.Low) and row.Close > row.Open) if long
                        else (_is_open_eq_high(row.Open, row.High) and row.Close < row.Open))
                nearby = abs(row.Open - pending["liquidity_level"]) <= config.marker_distance_atr * pending["atr_at_sweep"]
                if require_marker and pending.get("marker_at") is None and mark and nearby:
                    pending.update(marker_at=ts.isoformat(), marker_candle=_snapshot(frame, i), status="WAITING_CISD")
                crossed = row.Close > pending["cisd_level"] if long else row.Close < pending["cisd_level"]
                if crossed:
                    has_marker = not require_marker or pending.get("marker_at") is not None
                    simultaneous = require_marker and pending.get("marker_at") == ts.isoformat()
                    if not has_marker:
                        pending.update(status="CISD_BEFORE_MARKER", ended_at=ts.isoformat())
                    elif simultaneous and not config.allow_same_bar_cisd:
                        pending.update(status="SIMULTANEOUS_CISD_REJECTED", ended_at=ts.isoformat())
                    else:
                        signal = _entry(frame, i, pending, ticker, config, require_marker)
                        if signal:
                            entries.append(signal)
                        else:
                            pending.update(status="RISK_FILTERED", ended_at=ts.isoformat())
                    pending = None
                    continue
                continue
        if not np.isfinite(atr.iloc[i]) or atr.iloc[i] <= 0:
            continue
        ref = refs.iloc[i]
        if pd.isna(ref.get("available_at")) or pd.isna(ref.get("reference_at")):
            continue
        available = pd.Timestamp(ref.available_at)
        available = available.tz_localize("UTC") if available.tzinfo is None else available.tz_convert("UTC")
        if available >= ts:
            raise ValueError("Liquidity levels were not known before the source bar opened")
        low, high = float(ref.Low), float(ref.High)
        if not np.isfinite([low, high]).all() or not 0 < low < high:
            raise ValueError("Invalid reference liquidity levels")
        bull, bear = row.Low < low < row.Close, row.High > high > row.Close
        if bull == bear:
            continue  # also reject bars sweeping both sides: their intrabar order is unknown
        long = bool(bull)
        run = _opposite_run(frame, i, long)
        if not run:
            continue
        cisd = max(c["open"] for c in run) if long else min(c["open"] for c in run)
        extreme, target = (float(row.Low), high) if long else (float(row.High), low)
        buffer = max(config.min_buffer, config.stop_buffer_atr * float(atr.iloc[i]))
        pending = {"direction": "BULLISH" if long else "BEARISH",
                   "status": "WAITING_MARKER" if require_marker else "WAITING_CISD",
                   "reference_at": pd.Timestamp(ref.reference_at).isoformat(),
                   "reference_available_at": available.isoformat(), "reference_high": high, "reference_low": low,
                   "liquidity_level": low if long else high, "sweep_at": ts.isoformat(),
                   "sweep_candle": _snapshot(frame, i), "sweep_extreme": extreme,
                   "atr_at_sweep": float(atr.iloc[i]), "cisd_level": cisd, "cisd_source_candles": run,
                   "stop_loss": extreme - buffer if long else extreme + buffer, "target": target,
                   "marker_at": None, "marker_candle": None}
        setups.append(pending)
        sweep_i = i
        already_crossed = row.Close > cisd if long else row.Close < cisd
        consumed = row.High >= target if long else row.Low <= target
        if already_crossed or consumed:
            pending.update(status="CISD_ON_SWEEP" if already_crossed else "TARGET_CONSUMED", ended_at=ts.isoformat())
            pending = None
    return {"entries": entries, "setups": setups, "config": asdict(config),
            "config_hash": config.fingerprint, "require_marker": require_marker}
