"""
metrics.py — Prometheus metrics for the consumer service
"""

from prometheus_client import Counter, Gauge, Histogram

messages_consumed = Counter(
    "stock_consumer_messages_total",
    "Total Kafka messages consumed",
)

bronze_writes = Counter(
    "stock_consumer_bronze_writes_total",
    "Total Parquet files written to Bronze",
    ["symbol"],
)

silver_writes = Counter(
    "stock_consumer_silver_writes_total",
    "Total Parquet files written to Silver",
    ["symbol"],
)

batch_duration = Histogram(
    "stock_consumer_batch_duration_seconds",
    "Time to process one batch end-to-end",
)

batch_size_gauge = Gauge(
    "stock_consumer_last_batch_size",
    "Number of messages in last processed batch",
)
