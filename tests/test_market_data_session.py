"""Session-aligned 4H resampling (America/New_York) + dropping unclosed bars."""
from __future__ import annotations

import pandas as pd
import pytest

from market_data import (
    drop_unclosed,
    regular_session_only,
    resample_to_4h,
    resample_to_monthly,
    resample_to_weekly,
)

NY = "America/New_York"
FAR_FUTURE = pd.Timestamp("2030-01-01", tz="UTC")


def _hourly_session(day: str, *, close: str = "16:00", prepost: bool = False) -> pd.DataFrame:
    """Yahoo-style 1h bars (09:30, 10:30, ... last partial bar) for one NY day."""
    start = pd.Timestamp(f"{day} 04:00" if prepost else f"{day} 09:30", tz=NY)
    end = pd.Timestamp(f"{day} 20:00" if prepost else f"{day} {close}", tz=NY)
    idx = []
    t = start
    while t < end:
        idx.append(t)
        t += pd.Timedelta(hours=1)
    n = len(idx)
    return pd.DataFrame(
        {
            "Open": [100.0 + i for i in range(n)],
            "High": [101.0 + i for i in range(n)],
            "Low": [99.0 + i for i in range(n)],
            "Close": [100.5 + i for i in range(n)],
            "Volume": [10] * n,
        },
        index=pd.DatetimeIndex(idx),
    )


def _starts_ny(df: pd.DataFrame) -> list[str]:
    idx = pd.DatetimeIndex(df.index).tz_localize("UTC").tz_convert(NY)
    return [t.strftime("%Y-%m-%d %H:%M") for t in idx]


def test_two_bars_per_session_aligned_to_ny_open():
    out = resample_to_4h(_hourly_session("2025-07-15"), now=FAR_FUTURE)
    assert _starts_ny(out) == ["2025-07-15 09:30", "2025-07-15 13:30"]
    a, b = out.iloc[0], out.iloc[1]
    # bar A = 09:30,10:30,11:30,12:30 ; bar B = 13:30,14:30,15:30
    assert a["Open"] == 100.0 and a["Close"] == 103.5 and a["Volume"] == 40
    assert b["Open"] == 104.0 and b["High"] == 107.0 and b["Volume"] == 30


@pytest.mark.parametrize(
    "days",
    [
        # US DST start 2025-03-09 (Sunday): week before (EST) and after (EDT)
        ["2025-03-07", "2025-03-10"],
        # US DST end 2025-11-02 (Sunday)
        ["2025-10-31", "2025-11-03"],
        # Europe/US mismatch weeks (US already EDT, EU still CET)
        ["2025-03-12", "2025-03-28"],
    ],
)
def test_dst_weeks_keep_ny_alignment(days):
    df = pd.concat([_hourly_session(d) for d in days])
    out = resample_to_4h(df, now=FAR_FUTURE)
    starts = _starts_ny(out)
    assert starts == [f"{d} {t}" for d in days for t in ("09:30", "13:30")]
    # NY wall clock is stable; UTC start hour shifts only across a DST change
    utc_hours = {pd.Timestamp(x).hour for x in out.index[::2]}
    offsets = {pd.Timestamp(d, tz=NY).utcoffset() for d in days}
    assert len(utc_hours) == len(offsets)


def test_half_day_session_single_bar():
    # Day after Thanksgiving 2025: market closes 13:00 ET
    df = _hourly_session("2025-11-28", close="13:00")
    out = resample_to_4h(df, now=FAR_FUTURE)
    assert _starts_ny(out) == ["2025-11-28 09:30"]
    # half day: 13:00 < nominal 13:30, still forming at 13:10 ET...
    at_1310 = pd.Timestamp("2025-11-28 13:10", tz=NY)
    assert resample_to_4h(df, now=at_1310).empty
    # ...closed once the NY day is over
    next_day = pd.Timestamp("2025-11-29 00:05", tz=NY)
    assert len(resample_to_4h(df, now=next_day)) == 1


def test_extended_hours_are_dropped():
    df = _hourly_session("2025-07-15", prepost=True)
    assert len(df) == 16
    reg = regular_session_only(df)
    assert len(reg) == 6  # 09:00 bar starts before the open and is excluded
    out = resample_to_4h(df, now=FAR_FUTURE)
    assert _starts_ny(out) == ["2025-07-15 09:30", "2025-07-15 13:30"]
    assert out.iloc[0]["Volume"] == 40  # 10:00,11:00,12:00,13:00 only


def test_naive_utc_input_matches_aware():
    df = _hourly_session("2025-01-15")
    naive = df.copy()
    naive.index = naive.index.tz_convert("UTC").tz_localize(None)
    pd.testing.assert_frame_equal(
        resample_to_4h(df, now=FAR_FUTURE), resample_to_4h(naive, now=FAR_FUTURE)
    )


def test_forming_4h_bar_is_dropped():
    df = _hourly_session("2025-07-15")
    at_1200 = pd.Timestamp("2025-07-15 12:00", tz=NY)
    at_1331 = pd.Timestamp("2025-07-15 13:31", tz=NY)
    at_1559 = pd.Timestamp("2025-07-15 15:59", tz=NY)
    at_1600 = pd.Timestamp("2025-07-15 16:00", tz=NY)
    assert resample_to_4h(df[df.index <= at_1200], now=at_1200).empty
    assert _starts_ny(resample_to_4h(df, now=at_1331)) == ["2025-07-15 09:30"]
    assert len(resample_to_4h(df, now=at_1559)) == 1
    assert len(resample_to_4h(df, now=at_1600)) == 2
    # IC-CISD path keeps the current candle on purpose
    assert len(resample_to_4h(df, now=at_1331, drop_forming=False)) == 2


def _daily(start: str, periods: int) -> pd.DataFrame:
    idx = pd.bdate_range(start, periods=periods, tz=NY)
    return pd.DataFrame(
        {"Open": 1.0, "High": 2.0, "Low": 0.5, "Close": 1.5, "Volume": 1},
        index=idx,
    )


def test_drop_unclosed_daily():
    df = _daily("2025-07-14", 5)  # Mon..Fri
    fri_1500 = pd.Timestamp("2025-07-18 15:00", tz=NY)
    fri_1601 = pd.Timestamp("2025-07-18 16:01", tz=NY)
    assert len(drop_unclosed(df, "1d", now=fri_1500)) == 4
    assert len(drop_unclosed(df, "1d", now=fri_1601)) == 5
    # naive date index (no tz) behaves the same
    naive = df.copy()
    naive.index = naive.index.tz_localize(None)
    assert len(drop_unclosed(naive, "1d", now=fri_1500)) == 4


def test_drop_unclosed_intraday():
    df = _hourly_session("2025-07-15")
    now = pd.Timestamp("2025-07-15 12:00", tz=NY)
    kept = drop_unclosed(df, "1h", now=now)
    # 09:30 & 10:30 closed, 11:30 still forming
    assert len(kept) == 2
    # last partial bar 15:30 ends at 16:00 (capped), not 16:30
    assert len(drop_unclosed(df, "1h", now=pd.Timestamp("2025-07-15 16:00", tz=NY))) == 7


def test_weekly_and_monthly_drop_forming():
    df = _daily("2025-06-02", 45)  # through 2025-08-01 (Fri)
    wed = pd.Timestamp("2025-07-30 12:00", tz=NY)
    weekly = resample_to_weekly(df[df.index <= wed], now=wed)
    last_week = pd.DatetimeIndex(weekly.index).tz_localize("UTC").tz_convert(NY)[-1]
    assert last_week.strftime("%Y-%m-%d") == "2025-07-25"  # current week dropped
    monthly = resample_to_monthly(df[df.index <= wed], now=wed)
    last_month = pd.DatetimeIndex(monthly.index).tz_localize("UTC").tz_convert(NY)[-1]
    assert last_month.strftime("%Y-%m") == "2025-06"  # July still forming
    after = pd.Timestamp("2025-07-31 16:05", tz=NY)
    monthly2 = resample_to_monthly(df[df.index <= after], now=after)
    assert pd.DatetimeIndex(monthly2.index).tz_localize("UTC").tz_convert(NY)[-1].month == 7


def test_month_ending_on_weekend_closes_friday():
    # May 2025 ends on Saturday -> closes Fri 2025-05-30 16:00 ET
    df = _daily("2025-05-01", 22)  # through Fri 2025-05-30
    fri = pd.Timestamp("2025-05-30 16:01", tz=NY)
    assert len(resample_to_monthly(df[df.index <= fri], now=fri)) == 1
