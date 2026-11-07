"""Offline contracts for signal isolation, closure metadata and full resolver coverage."""
from types import SimpleNamespace

import pandas as pd

from devils_outcome_resolver import load_open, resolve_outcome
from devils_scanner import signal_to_row, upsert_signal
from devils_backfill import _write_symbol
from strategy.devils_markers import resolve_trade_details_after_marker


class Query:
    def __init__(self, db):
        self.db, self.filters, self.bounds = db, [], None

    def select(self, *_): return self
    def eq(self, key, value):
        self.filters.append((key, value))
        return self
    def is_(self, key, value): return self.eq(key, None if value == "null" else value)
    def in_(self, key, values):
        self.filters.append((key, set(values)))
        return self
    def order(self, *_): return self
    def range(self, start, end):
        self.bounds = (start, end)
        return self
    def limit(self, *_): return self
    def upsert(self, row, **_):
        self.db.writes.append(row)
        return self
    def execute(self):
        self.db.queries.append(self.filters)
        rows = [r for r in self.db.rows if all(
            r.get(k) in v if isinstance(v, set) else r.get(k) == v for k, v in self.filters)]
        if self.bounds is not None:
            start, end = self.bounds
            rows = rows[start:min(end + 1, start + self.db.page_cap)]
        return SimpleNamespace(data=rows)


class Database:
    def __init__(self, rows, page_cap=500):
        self.rows, self.page_cap, self.writes, self.queries = rows, page_cap, [], []
    def table(self, _): return Query(self)


def signal():
    return {"ticker": "TEST", "symbol": "TEST", "timeframe": "1D", "direction": "BULLISH",
            "entry_price": 100, "stop_loss": 98, "target": 106, "rr_ratio": 3,
            "devils_marker_price": 98, "pattern_at": "2025-01-01T00:00:00-05:00", "status": "ACTIVE"}


def candles():
    return pd.DataFrame({"Open": [98, 101], "High": [101, 107], "Low": [98, 100], "Close": [100, 106]},
                        index=pd.date_range("2025-01-01", periods=2, tz="America/New_York"))


def test_resolver_pages_past_server_cap_and_excludes_other_strategies():
    base = {"strategy": "pure_no_wick", "is_paper": False, "is_active": True, "result": None, "status": "ACTIVE"}
    rows = [{**base, "id": n} for n in range(7)]
    rows += [{**base, "id": 8, "strategy": "devils_cisd_v1"}, {**base, "id": 9, "is_paper": True}]
    db = Database(rows, page_cap=2)
    assert [r["id"] for r in load_open(db)] == list(range(7))
    assert len(db.queries) == 5
    assert [r["id"] for r in load_open(db, limit=3)] == [0, 1, 2]


def test_live_closure_attaches_outcome_and_preserves_context():
    s = {**signal(), "meta": {"ctx": {"saved": 1}, "origin": "scanner"}}
    out = resolve_outcome(s, candles())
    assert out["closed_at"].startswith("2025-01-02")
    assert out["meta"]["execution"]["exit_price"] == 106
    assert out["meta"]["outcome"]["bars_held"] == 1
    assert out["meta"]["ctx"] == {"saved": 1} and "execution" not in s["meta"]


def test_scanner_persists_exit_time_fill_and_signal_provenance():
    details = resolve_trade_details_after_marker(candles(), 0, "BULLISH", 98, 106, 100, 3)
    row = signal_to_row({**signal(), **details})
    assert row["closed_at"] is not None and row["meta"]["execution"]["exit_price"] == 106
    assert row["meta"]["origin"] == "reconstructed"
    assert row["meta"]["entry_basis"] == "signal_close"
    assert row["meta"]["signal_available_at"] == "2025-01-01T21:00:00+00:00"
    assert signal_to_row(signal(), origin="historical")["meta"]["origin"] == "historical"


def test_upsert_is_scoped_to_strategy_and_keeps_first_observation():
    row = signal_to_row(signal())
    paper = {**row, "id": 1, "strategy": "devils_cisd_v1", "result": "WIN"}
    db = Database([paper])
    upsert_signal(db, row, dry_run=False)
    assert len(db.writes) == 1
    old = {**row, "id": 2, "meta": {**row["meta"], "origin": "scanner", "first_seen_at": "original"}}
    db = Database([old])
    upsert_signal(db, signal_to_row(signal()), dry_run=False)
    assert db.writes[0]["meta"]["first_seen_at"] == "original"


def test_historical_backfill_preserves_live_observation_provenance():
    old = signal_to_row(signal())
    old["meta"].update(origin="scanner", first_seen_at="original")
    db = Database([old])
    _write_symbol(db, "TEST", [signal_to_row(signal(), origin="historical")], False)
    saved = db.writes[0][0]
    assert saved["meta"]["origin"] == "scanner" and saved["meta"]["first_seen_at"] == "original"
