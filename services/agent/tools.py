"""
tools.py — Deterministic Investigation Tools
=============================================
These are the 5 tools the agent runs for every investigation.
They are called in a FIXED ORDER — not decided by the LLM.
The LLM only sees the collected results, not the tool calls.

Tools:
  1. get_anomaly_detail(anomaly_id)      → full context from Gold
  2. get_price_history(symbol, days)     → OHLCV baseline
  3. get_sector_peers(symbol)            → peer comparison
  4. get_sec_filings(symbol, days)       → SEC EDGAR insider + 8-K
  5. get_options_data(symbol)            → put/call ratio via yfinance
"""

import logging
import requests
import yfinance as yf
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta
from typing import Optional
from config import TimescaleConfig, SYMBOL_TO_SECTOR, WATCHLIST

log = logging.getLogger(__name__)

SEC_HEADERS = {"User-Agent": "StockAgentBot contact@example.com"}


# ── Tool 1: Anomaly Detail ─────────────────────────────────────────

def get_anomaly_detail(anomaly_id: str) -> dict:
    """
    Fetch full anomaly context from gold.anomaly_feed.
    Returns: symbol, anomaly_type, score, severity, context JSON.
    """
    try:
        conn = psycopg2.connect(TimescaleConfig.dsn())
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(
                """
                SELECT symbol, sector, bar_time, anomaly_type,
                       anomaly_score, severity_score, severity_label,
                       context, detected_at
                FROM gold.anomaly_feed
                WHERE anomaly_id = %s
                """,
                (anomaly_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else {}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        log.error(f"get_anomaly_detail error: {e}")
        return {"error": str(e)}


# ── Tool 2: Price History ──────────────────────────────────────────

def get_price_history(symbol: str, days: int = 20) -> dict:
    """
    Fetch last N days of daily OHLCV from yfinance.
    Returns summary stats for baseline context.
    """
    try:
        ticker = yf.Ticker(symbol)
        hist   = ticker.history(period=f"{days}d", interval="1d")

        if hist.empty:
            return {"error": f"No history for {symbol}"}

        avg_vol    = int(hist["Volume"].mean())
        avg_close  = round(float(hist["Close"].mean()), 2)
        price_high = round(float(hist["High"].max()), 2)
        price_low  = round(float(hist["Low"].min()), 2)

        # Last 5 days trend
        last5 = hist.tail(5)
        trend_pct = round(
            float((last5["Close"].iloc[-1] - last5["Close"].iloc[0])
                  / last5["Close"].iloc[0] * 100),
            2,
        )

        direction = "up" if trend_pct > 0 else "down"
        return {
            "symbol":           symbol,
            "days_analyzed":    days,
            "avg_daily_volume": avg_vol,
            "avg_close_price":  avg_close,
            "52w_high":         price_high,
            "52w_low":          price_low,
            "5d_trend_pct":     trend_pct,
            "trend_direction":  direction,
            "assessment":       f"5d trend {trend_pct:+.2f}% ({direction}), avg vol {avg_vol:,}",
        }

    except Exception as e:
        log.error(f"get_price_history error for {symbol}: {e}")
        return {"error": str(e)}


# ── Tool 3: Sector Peers ───────────────────────────────────────────

def get_sector_peers(symbol: str) -> dict:
    """
    Compare symbol's recent volume z-score vs peers in same sector.
    If peers are also anomalous → sector/macro event, not isolated.
    If peers are normal → company-specific event.
    """
    try:
        sector = SYMBOL_TO_SECTOR.get(symbol, "Unknown")
        peers  = [s for s in WATCHLIST.get(sector, []) if s != symbol]

        if not peers:
            return {"error": f"No peers found for {symbol}"}

        conn = psycopg2.connect(TimescaleConfig.dsn())
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            # Get latest anomaly scores for peers
            cur.execute(
                """
                SELECT DISTINCT ON (symbol)
                    symbol, volume_zscore, is_anomaly, anomaly_severity,
                    close, price_change_pct, bar_time
                FROM gold.volume_anomalies
                WHERE symbol = ANY(%s)
                ORDER BY symbol, bar_time DESC
                """,
                (peers,),
            )
            peer_data = [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()

        anomalous_peers = [p for p in peer_data if p.get("is_anomaly")]

        return {
            "symbol":                symbol,
            "sector":                sector,
            "peers_checked":         peers,
            "peers_data":            peer_data,
            "anomalous_peers_count": len(anomalous_peers),
            "anomalous_peers":       [p["symbol"] for p in anomalous_peers],
            "is_sector_wide_event":  len(anomalous_peers) >= 2,
            "assessment":            (
                "SECTOR-WIDE: multiple peers anomalous"
                if len(anomalous_peers) >= 2
                else "ISOLATED: peers appear normal"
            ),
        }

    except Exception as e:
        log.error(f"get_sector_peers error: {e}")
        return {"error": str(e)}


# ── Tool 4: SEC EDGAR Filings ──────────────────────────────────────

def get_sec_filings(symbol: str, days: int = 7) -> dict:
    """
    Query SEC EDGAR API for recent filings for this symbol.
    Looks for: Form 4 (insider trades), 8-K (material events).
    No API key required — public API.
    """
    try:
        resp = requests.get(
            f"https://efts.sec.gov/LATEST/search-index?q=%22{symbol}%22"
            f"&forms=4,8-K"
            f"&dateRange=custom"
            f"&startdt={(datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')}"
            f"&enddt={datetime.now().strftime('%Y-%m-%d')}",
            headers = SEC_HEADERS,
            timeout = 10,
        )
        resp.raise_for_status()
        data = resp.json()

        hits    = data.get("hits", {}).get("hits", [])
        filings = []

        for hit in hits[:10]:   # max 10 results
            src = hit.get("_source", {})
            filings.append({
                "form_type":    src.get("form_type", ""),
                "filed_at":     src.get("period_of_report", ""),
                "entity_name":  src.get("entity_name", ""),
                "description":  src.get("file_description", ""),
            })

        # Categorise
        form4_filings = [f for f in filings if f["form_type"] == "4"]
        form8k_filings = [f for f in filings if f["form_type"] == "8-K"]

        return {
            "symbol":           symbol,
            "days_searched":    days,
            "total_filings":    len(filings),
            "form4_count":      len(form4_filings),
            "form8k_count":     len(form8k_filings),
            "form4_filings":    form4_filings,
            "form8k_filings":   form8k_filings,
            "has_insider_activity": len(form4_filings) > 0,
            "has_material_events":  len(form8k_filings) > 0,
            "assessment":           (
                f"FOUND: {len(form4_filings)} insider trades, "
                f"{len(form8k_filings)} material events"
                if filings
                else "No recent filings found"
            ),
        }

    except requests.exceptions.Timeout:
        return {"error": "SEC EDGAR timeout", "total_filings": 0}
    except Exception as e:
        log.error(f"get_sec_filings error for {symbol}: {e}")
        return {"error": str(e), "total_filings": 0}


# ── Tool 5: Options Data ───────────────────────────────────────────

def get_options_data(symbol: str) -> dict:
    """
    Fetch put/call volume ratio from nearest expiry options chain.
    Elevated put/call (> 1.2) signals bearish positioning.
    Normal put/call for large caps: 0.5 - 0.8
    """
    try:
        ticker   = yf.Ticker(symbol)
        expiries = ticker.options

        if not expiries:
            return {
                "symbol": symbol,
                "error":  "No options data available",
                "put_call_ratio": None,
            }

        # Use nearest expiry for most current sentiment
        chain     = ticker.option_chain(expiries[0])
        call_vol  = float(chain.calls["volume"].sum())
        put_vol   = float(chain.puts["volume"].sum())
        ratio     = round(put_vol / max(call_vol, 1), 3)

        # Implied volatility (avg of calls)
        avg_iv_calls = round(
            float(chain.calls["impliedVolatility"].mean()) * 100, 2
        )

        return {
            "symbol":              symbol,
            "expiry":              expiries[0],
            "call_volume":         int(call_vol),
            "put_volume":          int(put_vol),
            "put_call_ratio":      ratio,
            "avg_iv_pct":          avg_iv_calls,
            "normal_range":        "0.5 - 0.8 for large caps",
            "assessment":          (
                f"ELEVATED puts ({ratio:.2f}) — bearish sentiment"
                if ratio > 1.2
                else f"NORMAL put/call ({ratio:.2f})"
                if ratio > 0.3
                else f"ELEVATED calls ({ratio:.2f}) — bullish sentiment"
            ),
        }

    except Exception as e:
        log.warning(f"get_options_data error for {symbol}: {e}")
        return {
            "symbol":        symbol,
            "error":         str(e),
            "put_call_ratio": None,
        }
