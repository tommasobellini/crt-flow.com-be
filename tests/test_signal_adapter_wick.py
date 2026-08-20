
def test_signal_adapter_wick_3c():
    from signal_adapter import signal_to_crt_row

    signal = {
        "direction": "BULLISH",
        "timeframe": "1H",
        "entry_price": 100.0,
        "stop_loss": 98.0,
        "take_profit": 104.0,
        "pattern_candles": [
            {"time": 1704067200, "index": "C1"},
            {"time": 1704070800, "index": "C2"},
            {"time": 1704074400, "index": "C3"},
        ],
    }
    row = signal_to_crt_row(signal, ticker="AAPL")
    assert row["type"] == "bullish_wick_3c"
    assert row["symbol"] == "AAPL"
    assert row["timeframe"] == "1H"
    assert row["subtype"] == "3C SMC"
    assert len(row["pattern_candles"]) == 3
