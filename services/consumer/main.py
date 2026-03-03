"""
consumer/main.py — Stock Quote Consumer
Reads raw_quotes Kafka topic → Bronze Parquet → Silver OHLCV bars.
Prometheus metrics on port 8002.
"""

import json
import os
import time
import logging
import pandas as pd
from collections import defaultdict
from kafka import KafkaConsumer
from prometheus_client import start_http_server

from config import KafkaConfig
import storage
import aggregator
from metrics import (
    messages_consumed, bronze_writes, silver_writes,
    batch_duration, batch_size_gauge,
)

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [CONSUMER] %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

BATCH_SIZE    = 100
BATCH_TIMEOUT = 60   # seconds


def process_batch(messages: list[dict], minio) -> None:
    if not messages:
        return

    start = time.time()
    batch_size_gauge.set(len(messages))

    by_symbol: dict = defaultdict(list)
    for msg in messages:
        by_symbol[msg["symbol"]].append(msg)

    log.info(f"── Batch: {len(messages)} msgs, {len(by_symbol)} symbols ───")

    # Bronze — raw per symbol
    for symbol, sym_msgs in by_symbol.items():
        df = pd.DataFrame(sym_msgs)
        storage.write_parquet(minio, "bronze", df, symbol)
        bronze_writes.labels(symbol=symbol).inc()

    # Silver — aggregated OHLCV bars
    silver_df = aggregator.build_silver_bars(messages)
    for symbol, sym_df in silver_df.groupby("symbol"):
        storage.write_parquet(minio, "silver", sym_df.reset_index(drop=True), symbol)
        silver_writes.labels(symbol=symbol).inc()

    # Silver → TimescaleDB (required by dbt staging model → Gold)
    storage.write_silver_to_db(silver_df)

    elapsed = time.time() - start
    batch_duration.observe(elapsed)
    log.info(f"── Batch done in {elapsed:.2f}s ─────────────────────────\n")


def make_consumer():
    """Create a KafkaConsumer with retry."""
    for attempt in range(1, 21):
        try:
            c = KafkaConsumer(
                KafkaConfig.TOPIC,
                bootstrap_servers   = KafkaConfig.BROKER,
                group_id            = KafkaConfig.GROUP_ID,
                value_deserializer  = lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset   = "earliest",
                enable_auto_commit  = True,
                consumer_timeout_ms = BATCH_TIMEOUT * 1000,
            )
            log.info(f"✅ Kafka consumer connected")
            return c
        except Exception as e:
            log.warning(f"Kafka not ready (attempt {attempt}/20): {e}")
            time.sleep(5)
    raise RuntimeError("Cannot connect to Kafka after 20 attempts")


def main():
    start_http_server(8002)
    log.info("📊 Prometheus metrics → http://0.0.0.0:8002")

    wait = int(os.getenv("KAFKA_STARTUP_WAIT", "20"))
    log.info(f"⏳ Waiting {wait}s for Kafka to settle...")
    time.sleep(wait)

    minio = storage.get_client()
    log.info(f"👂 Listening on topic: {KafkaConfig.TOPIC}")

    # Outer loop: reconnect if consumer_timeout_ms fires (no messages)
    # or if Kafka drops the connection. Container stays alive forever.
    while True:
        try:
            consumer   = make_consumer()
            batch      = []
            last_flush = time.time()

            for message in consumer:
                batch.append(message.value)
                messages_consumed.inc()

                should_flush = (
                    len(batch) >= BATCH_SIZE
                    or (time.time() - last_flush) >= BATCH_TIMEOUT
                )
                if should_flush:
                    process_batch(batch, minio)
                    batch      = []
                    last_flush = time.time()

            # consumer_timeout_ms fired — flush remainder then reconnect
            process_batch(batch, minio)
            log.info("Consumer poll timeout — reconnecting...")

        except Exception as e:
            log.error(f"Consumer error: {e} — retrying in 10s")
            time.sleep(10)


if __name__ == "__main__":
    main()
