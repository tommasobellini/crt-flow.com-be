
def test_signal_adapter_liq_sweep():
    from signal_adapter import signal_to_crt_row

    signal = {
        "direction": "BEARISH",
        "timeframe": "5M",
        "entry_price": 50.0,
        "stop_loss": 51.0,
        "take_profit": 47.0,
        "timestamp": "2026-03-01T14:30:00+00:00",
        "pattern_candles": [
            {"time": 1709303400, "index": "SWEEP"},
            {"time": 1709305200, "index": "MSS"},
            {"time": 1709304000, "index": "OB"},
        ],
        "pattern_levels": {"setup": "LIQUIDITY_SWEEP_SHORT", "fibo_71": 50.0},
    }
    row = signal_to_crt_row(signal, ticker="META")
    assert row["type"] == "bearish_liq_sweep"
    assert row["subtype"] == "LIQ SWEEP"
    assert row["timeframe"] == "5M"
    assert row["pattern_levels"]["setup"] == "LIQUIDITY_SWEEP_SHORT"
