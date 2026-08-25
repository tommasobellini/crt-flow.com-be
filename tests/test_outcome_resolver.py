def test_resolve_outcome_long_tp():
    import pandas as pd
    from outcome_resolver import resolve_outcome

    idx = pd.date_range("2026-01-01", periods=5, freq="1h")
    df = pd.DataFrame(
        {
            "Open": [100.0] * 5,
            "High": [101.0, 102.0, 103.0, 104.0, 105.0],
            "Low": [99.0] * 5,
            "Close": [100.0] * 5,
        },
        index=idx,
    )
    sig = {
        "type": "bullish_liq_sweep",
        "entry_price": 100.0,
        "stop_loss": 98.0,
        "take_profit": 104.0,
        "pattern_at": "2026-01-01T00:00:00",
        "created_at": "2026-01-01T00:00:00",
    }
    out = resolve_outcome(sig, df)
    assert out is not None
    assert out["result"] == "WIN"
    assert out["exit_reason"] == "TP_HIT"


def test_resolve_outcome_short_sl():
    import pandas as pd
    from outcome_resolver import resolve_outcome

    idx = pd.date_range("2026-01-01", periods=3, freq="1h")
    df = pd.DataFrame(
        {
            "Open": [100.0] * 3,
            "High": [101.0, 102.0, 103.0],
            "Low": [99.0, 98.5, 97.0],
            "Close": [100.0] * 3,
        },
        index=idx,
    )
    sig = {
        "type": "bearish_liq_sweep",
        "entry_price": 100.0,
        "stop_loss": 102.0,
        "take_profit": 96.0,
        "pattern_at": "2026-01-01T00:00:00",
        "created_at": "2026-01-01T00:00:00",
    }
    out = resolve_outcome(sig, df)
    assert out is not None
    assert out["result"] == "LOSS"
    assert out["exit_reason"] == "SL_HIT"
