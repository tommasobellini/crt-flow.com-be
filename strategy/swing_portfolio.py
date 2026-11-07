"""Cash-constrained paper portfolio shared by historical and forward replay."""
from __future__ import annotations

from copy import deepcopy
from math import floor, isclose

import pandas as pd

from signal_context import bar_end
from strategy.gap_fill import exit_metadata, exit_on_bar
from strategy.swing_4h import OHLC, SwingConfig, checked_frame, utc


def initial_state(config):
    return {"config_hash": config.fingerprint, "cash": config.initial_capital,
            "positions": {}, "journal": {}, "equity": [], "last_bar_at": None,
            "last_prices": {}, "initial_capital": config.initial_capital}


def source_is_valid(signal, frame):
    for candle in signal["source_candles"]:
        ts = utc(candle["at"])
        if ts not in frame.index:
            return False
        if not all(isclose(candle[c.lower()], float(frame.loc[ts, c]),
                           rel_tol=1e-9, abs_tol=1e-8) for c in OHLC):
            return False
    return True


def replay(frames, signals, *, config=None, state=None, start_at=None, as_of=None):
    cfg = config or SwingConfig()
    out = deepcopy(state) if state is not None else initial_state(cfg)
    if out["config_hash"] != cfg.fingerprint:
        raise ValueError("A different rule version requires a separate paper account")
    cutoff = utc(as_of if as_of is not None else pd.Timestamp.now(tz="UTC"))
    if out.get("as_of") and cutoff < utc(out["as_of"]):
        raise ValueError("Paper clock cannot move backwards")
    clean = {s: checked_frame(f) for s, f in frames.items()}
    clean = {s: f.loc[bar_end(f.index, "4H") <= cutoff] for s, f in clean.items()}
    for record in out["journal"].values():
        if record["status"] in ("OPEN", "PENDING"):
            if record["symbol"] not in clean or not source_is_valid(record, clean[record["symbol"]]):
                raise ValueError("Open paper source revised or missing; account was not changed")
    for symbol, position in out["positions"].items():
        for candle in position.get("observed_bars", []):
            if not source_is_valid({"source_candles":[candle]}, clean[symbol]):
                raise ValueError("Open paper price path revised; account was not changed")
    for signal in signals:
        if utc(signal["available_at"]) > cutoff or utc(signal["first_seen_at"]) > cutoff:
            raise ValueError("Future observations cannot enter the paper journal")
        if signal["id"] not in out["journal"]:
            out["journal"][signal["id"]] = deepcopy(signal)
    cursor = utc(out["last_bar_at"]) if out["last_bar_at"] else None
    start = utc(start_at) if start_at is not None else None
    # Resolve the first *actual* later session bar. No guessed holiday calendar.
    scheduled = {}
    for record in out["journal"].values():
        if record["status"] != "PENDING":
            continue
        f = clean.get(record["symbol"])
        if f is None:
            raise ValueError("Pending paper symbol missing from market data")
        next_i = f.index.searchsorted(utc(record["available_at"]), side="right")
        if next_i < len(f):
            at = f.index[next_i]
            if cursor is not None and at <= cursor:
                record["status"] = "MISSED"
            else:
                scheduled.setdefault(at, []).append(record["id"])
    events = {}
    for symbol, f in clean.items():
        for ts, values in zip(f.index, f.to_numpy()):
            if (cursor is not None and ts <= cursor) or (start is not None and ts < start):
                continue
            events.setdefault(ts, {})[symbol] = tuple(float(v) for v in values)
    cost_rate = (cfg.fee_bps+cfg.slippage_bps)/10_000

    def nav():
        return out["cash"]+sum(p["quantity"]*out["last_prices"][s]
                               for s, p in out["positions"].items())

    def close(symbol, price, at, reason, execution):
        position = out["positions"].pop(symbol)
        record = out["journal"][position["signal_id"]]
        exit_cost = position["quantity"]*price*cost_rate
        pnl = position["quantity"]*(price-position["entry_price"])-position["entry_cost"]-exit_cost
        risk_money = position["quantity"]*(position["entry_price"]-record["stop_loss"])
        out["cash"] += position["quantity"]*price-exit_cost
        record.update(status="CLOSED", closed_at=at.isoformat(), exit_price=price, exit_bar_at=ts.isoformat(),
                      exit_reason=reason, net_pnl=pnl, net_r=pnl/risk_money,
                      exit_cost=exit_cost, execution=execution, bars_held=position["bars_held"])

    for ts, bars in sorted(events.items()):
        closed_at = bar_end(pd.DatetimeIndex([ts]), "4H")[0]
        # Opening gaps release cash before new entries. Intrabar exits never do.
        for symbol, (o, h, l, c) in bars.items():
            out["last_prices"][symbol] = o
            if symbol in out["positions"]:
                pos = out["positions"][symbol]
                rec = out["journal"][pos["signal_id"]]
                if o <= rec["stop_loss"] or o >= rec["target"]:
                    hit = exit_on_bar(o, h, l, long=True, entry=pos["entry_price"],
                                      stop=rec["stop_loss"], target=rec["target"])
                    close(symbol, hit[2], ts, "STOP" if o <= rec["stop_loss"] else "TARGET",
                          exit_metadata(o, h, l, long=True, stop=rec["stop_loss"],
                                        target=rec["target"], exit=hit))
        for signal_id in sorted(scheduled.get(ts, []),
                                key=lambda key: out["journal"][key]["symbol"]):
            rec = out["journal"][signal_id]
            symbol = rec["symbol"]
            o = bars[symbol][0]
            if utc(rec["first_seen_at"]) > ts:
                rec["status"] = "MISSED"
                continue
            if not rec["stop_loss"] < o <= rec["max_entry"]:
                rec["status"] = "SKIPPED_GAP"
                continue
            if symbol in out["positions"] or len(out["positions"]) >= cfg.max_positions:
                rec["status"] = "SKIPPED_CAPACITY"
                continue
            equity = nav()
            quantity = floor(min(equity*cfg.risk_fraction/(o-rec["stop_loss"]),
                                 equity*cfg.max_position_fraction/o,
                                 out["cash"]/(o*(1+cost_rate))))
            if quantity < 1:
                rec["status"] = "SKIPPED_CAPACITY"
                continue
            entry_cost = quantity*o*cost_rate
            out["cash"] -= quantity*o+entry_cost
            out["positions"][symbol] = {"signal_id": signal_id, "entry_price": o,
                                       "quantity": quantity, "entry_cost": entry_cost, "bars_held": 0,
                                       "observed_bars": []}
            rec.update(status="OPEN", entry_at=ts.isoformat(), entry_price=o,
                       quantity=quantity, entry_cost=entry_cost,
                       actual_rr=(rec["target"]-o)/(o-rec["stop_loss"]))
        for symbol, (o, h, l, c) in bars.items():
            if symbol in out["positions"]:
                position = out["positions"][symbol]
                rec = out["journal"][position["signal_id"]]
                position["bars_held"] += 1
                position["observed_bars"].append({"at":ts.isoformat(), "open":o, "high":h, "low":l, "close":c})
                hit = exit_on_bar(o, h, l, long=True, entry=position["entry_price"],
                                  stop=rec["stop_loss"], target=rec["target"])
                if hit:
                    close(symbol, hit[2], closed_at, "STOP" if hit[0] == "CLOSED_LOSS" else "TARGET",
                          exit_metadata(o, h, l, long=True, stop=rec["stop_loss"],
                                        target=rec["target"], exit=hit))
                elif position["bars_held"] >= cfg.max_hold_bars:
                    close(symbol, c, closed_at, "TIME",
                          {"exit_price": c, "exit_gap": False, "ambiguous_exit_bar": False})
            out["last_prices"][symbol] = c
        out["last_bar_at"] = ts.isoformat()
        out["equity"].append({"at": closed_at.isoformat(), "equity": nav(),
                              "cash": out["cash"], "positions": len(out["positions"])})
    out["as_of"] = cutoff.isoformat()
    return out


def metrics(state):
    records = list(state["journal"].values())
    closed = [r for r in records if r["status"] == "CLOSED"]
    pnl = [r["net_pnl"] for r in closed]
    gains, losses = sum(max(0, p) for p in pnl), -sum(min(0, p) for p in pnl)
    base = state["initial_capital"]
    equity = pd.Series([r["equity"] for r in state["equity"]],
                       index=pd.to_datetime([r["at"] for r in state["equity"]], utc=True),
                       dtype=float)
    peak, dd = base, 0.0
    for value in equity:
        peak = max(peak, value)
        dd = min(dd, value/peak-1)
    weekly, previous = [], base
    as_of = utc(state.get("as_of", equity.index[-1] if not equity.empty else pd.Timestamp.now(tz="UTC")))
    if not equity.empty:
        for at, value in equity.resample("W-FRI").last().dropna().items():
            week_end = pd.Timestamp(at.date(), tz="America/New_York")+pd.Timedelta(hours=16)
            weekly.append({"week": at.date().isoformat(), "return_pct": (value/previous-1)*100,
                           "equity": float(value), "complete": bool(as_of >= week_end)})
            previous = value
    completed = [w for w in weekly if w["complete"]]
    final = float(equity.iloc[-1]) if not equity.empty else base
    years = (equity.index[-1]-equity.index[0]).total_seconds()/(365.25*86400) if len(equity)>1 else 0
    return {"candidates": len(records), "entered": sum(r["status"] in ("OPEN", "CLOSED") for r in records),
            "closed": len(closed), "open": len(state["positions"]),
            "win_rate": sum(p > 0 for p in pnl)/len(pnl) if pnl else None,
            "profit_factor": gains/losses if losses else None,
            "avg_net_r": sum(r["net_r"] for r in closed)/len(closed) if closed else None,
            "net_r": sum(r["net_r"] for r in closed), "realized_net_pnl": sum(pnl),
            "equity": final, "return_pct": (final/base-1)*100, "max_drawdown_pct": dd*100,
            "cagr_pct": ((final/base)**(1/years)-1)*100 if years >= 1 else None,
            "completed_weeks": len(completed),
            "positive_weeks_pct": sum(w["return_pct"] > 0 for w in completed)/len(completed)*100 if completed else None,
            "worst_week_pct": min((w["return_pct"] for w in completed), default=None),
            "weekly": weekly}
