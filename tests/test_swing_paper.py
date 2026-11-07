from copy import deepcopy
from dataclasses import replace

import pandas as pd
import pytest
from storage3.exceptions import StorageApiError

import swing_paper
from strategy.swing_4h import SwingConfig
from swing_storage import paper_lock, read_json, write_json
from tests.test_swing_4h import market


class MemoryStorage:
    def __init__(self):
        self.files = {}

    def upload(self, path, data, file_options):
        if path in self.files and file_options["upsert"] == "false":
            raise StorageApiError("Already exists", "Duplicate", 409)
        self.files[path] = data

    def download(self, path):
        if path not in self.files:
            raise StorageApiError("Missing", "not_found", 404)
        return self.files[path]

    def remove(self, paths):
        for path in paths:
            self.files.pop(path, None)


def test_private_writer_lock_cannot_be_stolen_and_releases_on_failure():
    storage = MemoryStorage()
    with pytest.raises(ValueError):
        with paper_lock(storage):
            with pytest.raises(RuntimeError, match="owns the lock"):
                with paper_lock(storage):
                    pass
            raise ValueError("network failed")
    assert storage.files == {}
    with paper_lock(storage):
        write_json(storage, "paper/latest.json", {"frozen":True})
    assert read_json(storage, "paper/latest.json") == {"frozen":True}
    assert read_json(storage, "absent.json") is None


def test_failed_update_preserves_last_account_and_publishes_health(monkeypatch):
    storage = MemoryStorage()
    previous = {"frozen":"previous-account"}
    write_json(storage, "paper/latest.json", previous)
    monkeypatch.setattr(swing_paper, "ensure_bucket", lambda _: storage)
    def fail(*args, **kwargs):
        raise ValueError("source changed")
    monkeypatch.setattr(swing_paper, "scan", fail)
    with pytest.raises(ValueError, match="source changed"):
        swing_paper.persist_scan(object(), ["TEST"])
    assert read_json(storage, "paper/latest.json") == previous
    assert read_json(storage, "paper/health.json")["status"] == "error"
    assert "paper/write-lock.json" not in storage.files


def test_paper_never_backfills_and_rules_or_universe_cannot_replace_account(monkeypatch):
    h4, daily, cfg = market()
    data = {"TEST":{"4H":h4, "1D":daily}}
    monkeypatch.setattr(swing_paper, "market_frames", lambda *a, **k: (data, {}, {}))
    now = pd.Timestamp(h4.index[-1]).normalize()+pd.Timedelta(days=1, hours=12)
    first = swing_paper.scan(["TEST"], config=cfg, as_of=now)
    assert first["journal"] == [] and first["stats"]["entered"] == 0
    assert first["stats"]["equity"] == cfg.initial_capital
    repeated = swing_paper.scan(["TEST"], config=cfg, as_of=now, previous=first)
    assert repeated["state"] == first["state"]
    with pytest.raises(ValueError, match="cannot be overwritten"):
        swing_paper.scan(["OTHER"], previous=first, config=cfg, as_of=now)
    with pytest.raises(ValueError, match="cannot be overwritten"):
        swing_paper.scan(["TEST"], previous=first, config=replace(cfg, target_r=3), as_of=now)
    with pytest.raises(ValueError, match="cannot be overwritten"):
        swing_paper.scan(["TEST"], previous={**first,"strategy":"other"}, config=cfg, as_of=now)


def test_today_signal_remains_pending_until_future_open_and_failed_feed_preserves_state(monkeypatch):
    h4, daily, cfg = market()
    data = {"TEST":{"4H":h4, "1D":daily}}
    monkeypatch.setattr(swing_paper, "market_frames", lambda *a, **k: (data, {}, {}))
    from signal_context import bar_end
    now = bar_end(h4.index[-1:], "4H")[0]+pd.Timedelta(minutes=15)
    first = swing_paper.scan(["TEST"], config=cfg, as_of=now)
    assert len(first["journal"]) == 1 and first["journal"][0]["status"] == "PENDING"
    assert first["journal"][0]["first_seen_at"] == now.isoformat()
    frozen = deepcopy(first)
    monkeypatch.setattr(swing_paper, "market_frames", lambda *a, **k: ({}, {"TEST":"Timeout"}, {}))
    with pytest.raises(RuntimeError, match="data incomplete"):
        swing_paper.scan(["TEST"], previous=first, config=cfg, as_of=now)
    assert first == frozen
