"""
aggregator.py — OHLCV Silver bar builder
==========================================
Takes a batch of raw quote messages (Bronze) and aggregates
them into 1-minute OHLCV bars per symbol (Silver).

Silver schema:
  symbol, sector, date, hour, minute,
  open, high, low, close, volume,
  bar_start, bar_end, tick_count,
  processed_at
"""

import logging
import pandas as pd
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def build_silver_bars(messages: list[dict]) -> pd.DataFrame:
    """
    Aggregate raw quote messages into 1-min OHLCV bars.

    Input:  list of raw quote dicts from Kafka
    Output: DataFrame with one row per symbol per minute bar
    """
    if not messages:
        return pd.DataFrame()

    df = pd.DataFrame(messages)

    # Parse timestamps
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    df["bar_time"]   = pd.to_datetime(df["bar_time"],   utc=True, errors="coerce")

    # Minute-level truncation for grouping
    df["minute_bucket"] = df["fetched_at"].dt.floor("1min")

    # Aggregate to OHLCV per symbol per minute
    bars = (
        df.groupby(["symbol", "sector", "minute_bucket"])
        .agg(
            open       = ("close", "first"),
            high       = ("close", "max"),
            low        = ("close", "min"),
            close      = ("close", "last"),
            volume     = ("volume", "sum"),
            tick_count = ("close", "count"),
            bar_start  = ("fetched_at", "min"),
            bar_end    = ("fetched_at", "max"),
        )
        .reset_index()
    )

    bars.rename(columns={"minute_bucket": "bar_time"}, inplace=True)

    bars["date"]         = bars["bar_time"].dt.date.astype(str)
    bars["hour"]         = bars["bar_time"].dt.hour
    bars["minute"]       = bars["bar_time"].dt.minute
    bars["processed_at"] = datetime.now(timezone.utc).isoformat()

    # Round prices
    for col in ["open", "high", "low", "close"]:
        bars[col] = bars[col].round(4)

    log.info(f"  📊 Built {len(bars)} Silver bars from {len(df)} raw messages")
    return bars
