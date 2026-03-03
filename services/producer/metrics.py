"""
metrics.py — Prometheus metrics for the producer service
"""

from prometheus_client import Counter, Gauge, Histogram

quotes_published = Counter(
    "stock_producer_quotes_published_total",
    "Total quote messages published to Kafka",
    ["symbol", "sector"],
)

fetch_errors = Counter(
    "stock_producer_fetch_errors_total",
    "Total yfinance fetch errors",
    ["symbol"],
)

poll_cycles = Counter(
    "stock_producer_poll_cycles_total",
    "Total completed poll cycles",
)

poll_duration = Histogram(
    "stock_producer_poll_duration_seconds",
    "Time to fetch all symbols in one cycle",
)

symbols_per_cycle = Gauge(
    "stock_producer_symbols_per_cycle",
    "Number of symbols successfully fetched in last cycle",
)
