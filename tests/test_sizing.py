import pytest

from strategy.sizing import attach_size_factor, size_factor, sized_r, stop_distance_pct


def test_wide_stop_full_risk():
    assert size_factor(100.0, 99.0) == 1.0  # 1% stop
    assert size_factor(100.0, 99.5) == 1.0  # exactly 0.5% -> 100% exposure


def test_tight_stop_scaled():
    # NVDA 29/04/2025 case: 0.17% stop -> 34% of nominal risk
    assert size_factor(100.0, 99.83) == pytest.approx(0.34)
    assert sized_r(-24.7, size_factor(100.0, 99.83)) == pytest.approx(-8.398)


def test_short_side_and_params():
    assert size_factor(100.0, 100.25) == pytest.approx(0.5)
    assert size_factor(100.0, 100.25, risk_pct=1.0, max_exposure_pct=200) == pytest.approx(0.5)


def test_undefined():
    assert stop_distance_pct(0, 1) is None
    assert size_factor(100, 100) is None
    assert size_factor(None, 1) is None
    assert sized_r(None, 1.0) is None
    m = attach_size_factor({"a": 1}, None, 1)
    assert m == {"a": 1}


def test_attach_copies_meta():
    src = {"resolver_version": 2}
    m = attach_size_factor(src, 100.0, 99.9)
    assert m["size_factor"] == pytest.approx(0.2) and m["sizing_version"] == 1
    assert "size_factor" not in src


def test_resolver_close_payload_has_size_factor():
    pd = pytest.importorskip("pandas")
    import devils_outcome_resolver as r
    sig = {"entry_price": 100.0, "stop_loss": 99.9, "rr_ratio": 2, "meta": {"strategy": "pure_no_wick"}}
    out = r._close_payload("LOSS", "SL_HIT", pd.Timestamp("2025-01-02", tz="UTC"), sig, -1.0)
    assert out["meta"]["size_factor"] == pytest.approx(0.2)
