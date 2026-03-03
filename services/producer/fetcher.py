"""
fetcher.py — yfinance data fetcher
====================================
Fetches real-time (15-min delayed) quote data for all watchlist symbols.
Also fetches options chain data for put/call ratio calculation.

yfinance docs: https://github.com/ranaroussi/yfinance
No API key required.
"""

import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from typing import Optional
from config import ALL_SYMBOLS, SYMBOL_TO_SECTOR

log = logging.getLogger(__name__)


def fetch_quote(symbol: str) -> Optional[dict]:
    """
    Fetch current quote + 1-min OHLCV for a symbol.

    Returns normalised dict or None on failure.
    """
    try:
        ticker = yf.Ticker(symbol)

        # Current fast info (price, volume)
        info = ticker.fast_info

        # 5-day 1-minute OHLCV history (covers weekends/holidays)
        hist = ticker.history(period="5d", interval="1m")

        if hist.empty:
            log.warning(f"No history data for {symbol}")
            return None

        # Use the last COMPLETED bar (volume > 0).
        # hist.iloc[-1] is the in-progress current minute which has volume=0.
        completed = hist[hist["Volume"] > 0]
        if completed.empty:
            log.warning(f"No completed bars with volume for {symbol}")
            return None
        latest = completed.iloc[-1]

        return {
            "symbol":         symbol,
            "sector":         SYMBOL_TO_SECTOR.get(symbol, "Unknown"),
            "open":           round(float(latest["Open"]),   4),
            "high":           round(float(latest["High"]),   4),
            "low":            round(float(latest["Low"]),    4),
            "close":          round(float(latest["Close"]),  4),
            "volume":         int(latest["Volume"]),
            "market_price":   round(float(info.last_price),  4) if info.last_price else None,
            "market_volume":  getattr(info, "three_month_average_volume", None),
            "bar_time":       latest.name.isoformat(),
            "fetched_at":     datetime.now(timezone.utc).isoformat(),
            "source":         "yfinance",
        }

    except Exception as e:
        log.error(f"Failed to fetch {symbol}: {e}")
        return None


def fetch_options_ratio(symbol: str) -> Optional[dict]:
    """
    Fetch put/call volume ratio from options chain.
    Used by the agent's investigation tool.
    """
    try:
        ticker  = yf.Ticker(symbol)
        expiries = ticker.options

        if not expiries:
            return None

        # Use nearest expiry
        chain = ticker.option_chain(expiries[0])

        call_vol = chain.calls["volume"].sum()
        put_vol  = chain.puts["volume"].sum()

        ratio = round(float(put_vol) / max(float(call_vol), 1), 3)

        return {
            "symbol":       symbol,
            "put_volume":   int(put_vol),
            "call_volume":  int(call_vol),
            "put_call_ratio": ratio,
            "expiry":       expiries[0],
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        log.warning(f"Options fetch failed for {symbol}: {e}")
        return None


def fetch_all_quotes() -> list[dict]:
    """
    Fetch quotes for all watchlist symbols.
    Returns list of successful quote dicts (failures silently dropped).
    """
    results = []
    for symbol in ALL_SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            results.append(quote)
    return results
