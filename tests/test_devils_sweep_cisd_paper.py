from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from devils_sweep_cisd_paper import STRATEGY, paper_stats, scan_paper, refresh_open, verify_source
from tests.test_devils_sweep_cisd_research import market


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr("devils_sweep_cisd_paper._clock",lambda:pd.Timestamp("2025-01-15T12:00:00Z"))


class Query:
    def __init__(self,db): self.db,self.filters,self.payload=db,[],None
    def select(self,*_): return self
    def order(self,*_): return self
    def range(self,start,end): self.bounds=(start,end); return self
    def eq(self,key,value): self.filters.append((key,value)); return self
    def is_(self,key,value): return self.eq(key,None if value=="null" else value)
    def update(self,payload): self.payload=payload; return self
    def upsert(self,row,**kwargs):
        self.db.inserts.append((row,kwargs)); return self
    def execute(self):
        rows=[r for r in self.db.rows if all(r.get(k)==v for k,v in self.filters)]
        if hasattr(self,"bounds"): rows=rows[self.bounds[0]:self.bounds[1]+1]
        if self.payload is not None:
            self.db.updates.append((self.payload,self.filters))
            for row in rows: row.update(self.payload)
        return SimpleNamespace(data=rows)


class Database:
    def __init__(self,rows=()): self.rows,self.inserts,self.updates=list(rows),[],[]
    def table(self,name): assert name=="devils_signals"; return Query(self)


def test_default_paper_dry_run_never_writes_and_since_prevents_backfill():
    frames=market(); db=Database()
    result=scan_paper("TEST",since="2025-01-01",sb=db,frames=frames)
    assert len(result["new_candidates"])==1 and not db.inserts and not db.updates
    fresh=scan_paper("TEST",since="2026-11-07",sb=db,frames=frames,dry_run=False)
    assert fresh["new_candidates"]==[] and not db.inserts
    with pytest.raises(ValueError,match="start time"):
        scan_paper("TEST",since=None,frames=frames)


def test_persistence_is_new_paper_strategy_and_ignores_duplicate_rows():
    db=Database(); frames=market()
    scan_paper("TEST",since="2025-01-01",sb=db,frames=frames,dry_run=False)
    (row,options),=db.inserts
    assert row["strategy"]==STRATEGY and row["is_paper"] is True
    assert row["meta"]["radar_eligible"] is False
    assert row["cisd_level"] != row["entry_price"]
    assert row["meta"]["entry_basis"]=="signal_close"
    assert options["ignore_duplicates"] is True
    assert verify_source(row,frames[0])


def test_revised_source_cannot_reprice_or_close_saved_paper_trade():
    frames=market()
    row=scan_paper("TEST",since="2025-01-01",frames=frames)["new_candidates"][0]
    row={**deepcopy(row),"id":"paper"}; db=Database([row])
    changed=frames[0].copy()
    changed.iloc[14,changed.columns.get_loc("High")]+=0.01
    assert not verify_source(row,changed)
    refresh_open(db,"TEST",changed,dry_run=False)
    assert row["status"]=="ACTIVE" and row["meta"]["source_verified"] is False
    assert row["entry_price"]==102.2
    assert set(db.updates[0][0])=={"meta"}


def test_closure_uses_frozen_levels_and_only_owns_experimental_paper_rows():
    h4,_=market()
    row=scan_paper("TEST",since="2025-01-01",frames=market())["new_candidates"][0]
    row={**row,"id":"paper"}; original_stop=row["stop_loss"]
    other={**deepcopy(row),"id":"production","strategy":"pure_no_wick","is_paper":False}
    closed={**deepcopy(row),"id":"closed","is_active":False,"result":"WIN"}
    db=Database([row,other,closed])
    h4=h4.copy(); h4.iloc[16]=[109,110,108,109]
    assert refresh_open(db,"TEST",h4,dry_run=False)==1
    assert row["result"]=="WIN" and row["stop_loss"]==original_stop
    assert row["meta"]["execution"]["exit_price"]==109
    assert other["status"]=="ACTIVE" and closed["result"]=="WIN"
    assert ("is_active",True) in db.updates[0][1] and ("result",None) in db.updates[0][1]


def test_paper_outcomes_do_not_consume_a_forming_exit_bar():
    h4,daily=market()
    h4=h4.copy(); h4.iloc[16]=[109,110,108,109]
    # index 16 starts 09:30 NY, but has not yet closed at 10:00 NY.
    as_of=h4.index[16]+pd.Timedelta(minutes=30)
    result=scan_paper("TEST",since="2025-01-01",frames=(h4,daily),as_of=as_of)
    row,=result["new_candidates"]
    assert row["status"]=="ACTIVE" and row["result"] is None


def test_paper_statistics_are_empty_after_reset_and_do_not_include_production():
    prod={"id":"prod","strategy":"pure_no_wick","is_paper":False}
    assert paper_stats(Database([prod]))["stats"]["entries"] == 0
    row=scan_paper("TEST",since="2025-01-01",frames=market())["new_candidates"][0]
    result=paper_stats(Database([prod,{**row,"id":"paper"}]))
    assert result["stats"]["entries"]==1 and result["stats"]["open"]==1


def test_stalled_feed_cannot_reimport_old_entries_or_mutate_paper_rows():
    db=Database()
    with pytest.raises(ValueError,match="Stale"):
        scan_paper("TEST",since="2025-01-01",sb=db,frames=market(),dry_run=False,
                   as_of="2025-01-25T12:00:00Z")
    assert not db.inserts and not db.updates


def test_recovered_source_updates_validation_without_repricing():
    frames=market()
    row=scan_paper("TEST",since="2025-01-01",frames=frames)["new_candidates"][0]
    row={**row,"id":"paper"}
    row["meta"].update(source_verified=False,source_reason="SOURCE_REVISED_OR_MISSING")
    db=Database([row])
    assert refresh_open(db,"TEST",frames[0],dry_run=False)==0
    assert row["meta"]["source_verified"] is True and "source_reason" not in row["meta"]
    assert row["entry_price"]==102.2 and set(db.updates[0][0])=={"meta"}
