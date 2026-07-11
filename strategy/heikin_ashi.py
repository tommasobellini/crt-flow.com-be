from __future__ import annotations

import pandas as pd


def to_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw OHLC into Heikin-Ashi candles."""
    o = df["Open"].astype(float)
    h = df["High"].astype(float)
    low = df["Low"].astype(float)
    c = df["Close"].astype(float)

    ha_close = (o + h + low + c) / 4
    ha_open = pd.Series(index=df.index, dtype=float)
    ha_open.iloc[0] = (o.iloc[0] + c.iloc[0]) / 2

    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i - 1] + ha_close.iloc[i - 1]) / 2

    ha_high = pd.concat([h, ha_open, ha_close], axis=1).max(axis=1)
    ha_low = pd.concat([low, ha_open, ha_close], axis=1).min(axis=1)

    return pd.DataFrame(
        {"Open": ha_open, "High": ha_high, "Low": ha_low, "Close": ha_close},
        index=df.index,
    )


def last_two_ha_green(df: pd.DataFrame) -> bool:
    ha = to_heikin_ashi(df)
    if len(ha) < 2:
        return False
    last_two = ha.iloc[-2:]
    return bool((last_two["Close"] > last_two["Open"]).all())
