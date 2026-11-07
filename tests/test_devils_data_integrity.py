"""Offline contracts for signal isolation, closure metadata and full resolver coverage."""
from types import SimpleNamespace
from datetime import datetime, timezone

import pandas as pd

from devils_outcome_resolver import load_open, resolve_outcome
from devils_scanner import signal_to_row, upsert_signal
from devils_backfill import _write_symbol
from devils_signal_validation import PRICE_BASIS, radar_validation, source_validation
from strategy.devils_markers import RECENT_CANDLES, _is_open_eq_high, _is_open_eq_low
from strategy.devils_markers import resolve_trade_details_after_marker


class Query:
    def __init__(self, db):
        self.db, self.filters, self.bounds, self.payload = db, [], None, None

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
    def update(self, payload):
        self.payload = payload
        return self
    def execute(self):
        self.db.queries.append(self.filters)
        rows = [r for r in self.db.rows if all(
            r.get(k) in v if isinstance(v, set) else r.get(k) == v for k, v in self.filters)]
        if self.payload is not None:
            for row in rows:
                row.update(self.payload)
            self.db.updates.append(self.payload)
        if self.bounds is not None:
            start, end = self.bounds
            rows = rows[start:min(end + 1, start + self.db.page_cap)]
        return SimpleNamespace(data=rows)


class Database:
    def __init__(self, rows, page_cap=500):
        self.rows, self.page_cap, self.writes, self.queries, self.updates = rows, page_cap, [], [], []
    def table(self, _): return Query(self)


def signal():
    return {"ticker": "TEST", "symbol": "TEST", "timeframe": "1D", "direction": "BULLISH",
            "entry_price": 100, "stop_loss": 98, "target": 106, "rr_ratio": 3,
            "devils_marker_price": 98, "pattern_at": "2025-01-01T00:00:00-05:00", "status": "ACTIVE",
            "pattern_candle": {"open": 98, "high": 101, "low": 98, "close": 100}}


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
    assert db.writes == []
    assert db.rows[0]["meta"]["origin"] == "scanner" and db.rows[0]["meta"]["first_seen_at"] == "original"


def test_real_one_cent_wicks_are_rejected_at_high_and_low_prices():
    assert not _is_open_eq_low(71.36, 71.35)  # CVNA: the former saved false positive
    assert not _is_open_eq_high(500, 500.01)  # used to pass at the old 0.01% tolerance
    assert _is_open_eq_low(71.36, 71.36 + 1e-10)


def test_radar_requires_latest_bar_and_live_observation():
    row = signal_to_row(signal())
    clock = datetime(2025, 1, 3, tzinfo=timezone.utc)
    assert RECENT_CANDLES == 1
    assert source_validation(row, candles()) == "VERIFIED"
    assert radar_validation(row, candles(), 1, now=clock)["radar_reason"] == "OUTSIDE_RECENT_WINDOW"
    assert radar_validation(row, candles(), 3, now=clock)["radar_eligible"] is True
    historical = signal_to_row(signal(), origin="historical")
    assert radar_validation(historical, candles(), 3, now=clock)["radar_reason"] == "HISTORICAL_ONLY"
    legacy = {**row, "meta": {}}
    assert source_validation(legacy, candles()) == "UNVERIFIED_PRICE_BASIS"
    assert radar_validation(row, candles(), 3)["radar_reason"] == "STALE_MARKET_DATA"


def test_revised_source_candles_are_hidden_without_repricing_the_trade():
    row = {**signal_to_row(signal()), "id": 1}
    df = candles().copy()
    df.iloc[0, df.columns.get_loc("High")] = 102
    assert source_validation(row, df) == "SOURCE_CANDLE_REVISED"
    revised = signal_to_row({**signal(), "pattern_candle": {**signal()["pattern_candle"], "high": 102}})
    db = Database([row])
    upsert_signal(db, revised, dry_run=False)
    assert db.writes == [] and len(db.updates) == 1
    assert row["entry_price"] == 100 and row["stop_loss"] == 98
    assert row["meta"]["radar_eligible"] is False


def test_rescans_preserve_live_seen_time_and_original_target():
    row = {**signal_to_row(signal()), "id": 1}
    row["meta"]["first_live_seen_at"] = "2025-01-01T21:10:00Z"
    db = Database([row])
    upsert_signal(db, signal_to_row({**signal(), "target": 108, "rr_ratio": 4}), dry_run=False)
    saved = db.writes[0]
    assert saved["target"] == 106 and saved["rr_ratio"] == 3
    assert saved["meta"]["first_live_seen_at"] == "2025-01-01T21:10:00Z"


def test_legacy_price_basis_cannot_be_silently_upgraded_or_repaired():
    import backfill_context
    import backfill_outcome
    import backfill_r_multiple
    row = {**signal_to_row(signal()), "id": 1, "meta": {}, "status": "CLOSED_LOSS", "r_multiple": -1}
    db = Database([row])
    for repair in (backfill_context, backfill_outcome, backfill_r_multiple):
        stats = repair.run(db, dry_run=False, limit=10, batch=10, fetch=lambda _: {"1D": candles()})
        assert stats["unsupported_basis"] == 1 and stats["written"] == 0
    row.update(status="ACTIVE", is_active=True)
    upsert_signal(db, signal_to_row(signal()), dry_run=False)
    assert db.writes == [] and row["meta"]["radar_reason"] == "UNVERIFIED_PRICE_BASIS"


def test_history_uses_non_dividend_adjusted_prices():
    from devils_scanner import _history
    calls = []
    stock = SimpleNamespace(history=lambda **kw: (calls.append(kw), candles())[1])
    _history(stock, period="5y", interval="1d")
    assert calls[0]["auto_adjust"] is False


def test_radar_expiry_does_not_close_or_delete_the_simulated_trade():
    from devils_scanner import reconcile_radar
    row = {**signal_to_row(signal()), "id": 1}
    db = Database([row])
    reconcile_radar(db, "TEST", {"1D": candles()}, recent_candles=1, dry_run=False)
    assert row["meta"]["radar_eligible"] is False
    assert row["is_active"] is True and row["result"] is None and row["status"] == "ACTIVE"


def test_monthly_stop_resolves_during_forming_month_and_excludes_entry_period():
    from devils_outcome_resolver import resolve_outcome_with_daily
    daily = pd.DataFrame({
        "Open": [19.89, 19.21, 19.15, 19.94], "High": [20.5, 19.61, 19.74, 20.22],
        "Low": [15, 18.99, 19.15, 19.48], "Close": [19.03, 19.48, 19.56, 20.17],
    }, index=pd.to_datetime(["2026-10-30", "2026-11-02", "2026-11-03", "2026-11-04"]).tz_localize("America/New_York"))
    s = {"timeframe": "1M", "direction": "BEARISH", "entry_price": 19.03,
         "stop_loss": 19.89, "target": 15.7, "rr_ratio": 3.8721,
         "pattern_at": "2026-10-31T04:00:00Z", "meta": {
             "ctx": {"saved": True}, "first_live_seen_at": "2026-11-07T08:00:00Z", "origin": "scanner"}}
    result = resolve_outcome_with_daily(s, pd.DataFrame(), daily)
    assert result["status"] == "CLOSED_LOSS" and result["closed_at"].startswith("2026-11-04")
    assert result["r_multiple"] == round((19.03 - 19.94) / (19.89 - 19.03), 4)
    assert result["meta"]["execution"]["exit_price"] == 19.94
    assert result["meta"]["execution"]["resolution_timeframe"] == "1D"
    assert result["meta"]["outcome"]["bars_held"] == 3
    assert result["meta"]["ctx"] == {"saved": True}
    assert result["meta"]["origin"] == "reconstructed" and "first_live_seen_at" not in result["meta"]


def test_scanner_never_advertises_a_monthly_trade_already_closed_on_daily_bars(monkeypatch):
    import devils_scanner as ds
    df = candles()
    s = {**signal(), "timeframe": "1M"}
    monkeypatch.setattr(ds, "fetch_devils_frames", lambda _: {"1M": df})
    monkeypatch.setattr(ds, "detect_active_devils_marker", lambda *a, **kw: {**s, "is_high_rr": True})
    monkeypatch.setattr(ds, "resolve_outcome_with_daily", lambda *a: {"status": "CLOSED_LOSS"})
    assert ds.scan_ticker("TEST", persist=False) == []
