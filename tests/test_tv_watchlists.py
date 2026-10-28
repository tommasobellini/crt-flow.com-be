from tv_watchlists import parse_symbols


def test_parse_symbols():
    raw = ["###GROWTH", "NASDAQ:META", "NYSE:BRK.B", "CME_MINI:NQ1!", "TVC:VIX",
           "FOREXCOM:CHINA50", "NYSE:T", "NASDAQ:META", "###\u2064ACTIVE"]
    assert parse_symbols(raw) == ["META", "BRK-B", "T"]
