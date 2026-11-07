"""Optimization must fail closed and reserve future observations before grid search."""
import pandas as pd
import pytest

import optimizer_agent as agent


def history():
    daily = pd.DataFrame({"Close": range(200)}, index=pd.bdate_range("2024-01-01", periods=200, tz="UTC"))
    hourly = pd.DataFrame({"Close": range(1400)}, index=pd.date_range("2024-01-01", periods=1400, freq="4h", tz="UTC"))
    return daily, hourly


def trades(rr=2, result="WIN", day="2024-09-02"):
    return [{"day": day, "entry": 100, "stop": 98, "rr": rr, "result": result}
            for _ in range(agent.VALIDATION_MIN_TRADES)]


def test_no_data_cannot_approve_optimization():
    assert agent.validate_improvement({}, {}) == (False, 0, 0)


def test_loss_analysis_without_grid_cannot_invoke_code_changes(monkeypatch):
    logged = []
    monkeypatch.setattr(agent.sys, "argv", ["optimizer_agent.py", "--no-grid"])
    monkeypatch.setattr(agent, "setup_supabase", lambda: object())
    monkeypatch.setattr(agent, "pull_losses", lambda sb: [{"symbol": "TEST", "result": "LOSS"}])
    monkeypatch.setattr(agent, "log_run", lambda sb, payload: logged.append(payload))
    def forbidden(*a, **kw):
        raise AssertionError("No candidate was validated")
    monkeypatch.setattr(agent, "invoke_opencode", forbidden)
    monkeypatch.setattr(agent, "git_snapshot", forbidden)
    agent.main()
    assert logged[0]["status"] == "no_data" and logged[0]["gate_passed"] is False


def test_snapshot_cannot_commit_other_staged_files_or_use_parent_repository(monkeypatch):
    calls = []
    def capture(argv, **kwargs):
        calls.append((argv, kwargs))
        return type("Result", (), {"returncode": 0, "stdout": "snapshot", "stderr": ""})()
    monkeypatch.setattr(agent.subprocess, "run", capture)
    assert agent.git_snapshot()
    argv, kwargs = calls[0]
    assert "--only" in argv and argv[-2:] == ["--", "scanner.py"]
    assert kwargs["cwd"] == agent.os.path.dirname(agent.__file__)


def test_training_cannot_resolve_trades_using_holdout_candles():
    daily, hourly = history()
    cutoff, train_d, train_h = agent.training_history(daily, hourly)
    assert len(train_d) == 160 and train_d.index.max() < cutoff
    assert train_h.index.max() < cutoff
    assert cutoff == daily.index[160]


def test_validation_ignores_training_entries_and_deducts_costs():
    batch = trades() + trades(rr=100, day="2024-01-02")
    assert agent.validation_expectancy(batch, pd.Timestamp("2024-08-01")) == pytest.approx(1.95)
    assert agent.validation_expectancy(trades(result="OPEN"), pd.Timestamp("2024-08-01")) is None
    assert agent.validation_expectancy(trades()[:2], pd.Timestamp("2024-08-01")) is None


def test_only_paired_tickers_count_toward_evidence(monkeypatch):
    data = {t: history() for t in ["A", "B", "C"]}
    def fake_sim(ticker, daily, hourly, params):
        candidate = params.wall_wick_pct == 0.25
        if ticker == "C" and candidate:
            return []
        return trades(rr=3 if candidate else 2)
    monkeypatch.setattr(agent, "simulate", fake_sim)
    assert agent.validate_improvement({"wall_wick_pct": 0.25}, data)[0] is False


def test_positive_held_out_improvement_can_pass(monkeypatch):
    monkeypatch.setattr(agent, "simulate", lambda t, d, h, p: trades(rr=3 if p.wall_wick_pct == 0.25 else 2))
    approved, baseline, candidate = agent.validate_improvement(
        {"wall_wick_pct": 0.25}, {t: history() for t in ["A", "B", "C"]})
    assert approved and baseline == pytest.approx(1.95) and candidate == pytest.approx(2.95)


def test_improving_a_negative_strategy_is_not_enough(monkeypatch):
    monkeypatch.setattr(agent, "simulate", lambda t, d, h, p: trades(rr=0.02 if p.wall_wick_pct == 0.25 else 0.01))
    approved, _, candidate = agent.validate_improvement(
        {"wall_wick_pct": 0.25}, {t: history() for t in ["A", "B", "C"]})
    assert approved is False and candidate < 0
