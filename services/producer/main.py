"""
producer/main.py — Stock Quote Producer
Polls yfinance every 60s, publishes to Kafka.
Prometheus metrics on port 8001.
"""

import os
import time
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from prometheus_client import start_http_server

from config import PipelineConfig
from fetcher import fetch_all_quotes
from kafka_client import create_producer, publish
from metrics import (
    quotes_published, fetch_errors, poll_cycles,
    poll_duration, symbols_per_cycle,
)

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [PRODUCER] %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)


def run_poll_cycle(producer) -> None:
    log.info("── Poll cycle starting ──────────────────────────────────")
    with poll_duration.time():
        quotes = fetch_all_quotes()

    success = 0
    for quote in quotes:
        try:
            publish(producer, quote)
            quotes_published.labels(symbol=quote["symbol"], sector=quote["sector"]).inc()
            success += 1
            log.info(
                f"  ✅ {quote['symbol']:<6}  "
                f"${quote['close']:>8.2f}  "
                f"vol:{quote['volume']:>10,}  [{quote['sector']}]"
            )
        except Exception as e:
            fetch_errors.labels(symbol=quote["symbol"]).inc()
            log.error(f"  ❌ {quote['symbol']}: {e}")

    producer.flush()
    poll_cycles.inc()
    symbols_per_cycle.set(success)
    log.info(f"── Cycle done: {success}/20 published ───────────────────\n")


def main():
    start_http_server(8001)
    log.info("📊 Prometheus metrics → http://0.0.0.0:8001")

    # Wait for Kafka to be fully ready even after healthcheck passes.
    # The healthcheck confirms the broker is up but topic metadata
    # may not be ready yet — an extra 15s prevents connection race.
    wait = int(os.getenv("KAFKA_STARTUP_WAIT", "15"))
    log.info(f"⏳ Waiting {wait}s for Kafka to settle...")
    time.sleep(wait)

    # create_producer() has its own retry loop (20 attempts, 5s apart)
    producer = create_producer()

    run_poll_cycle(producer)

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        run_poll_cycle,
        trigger = "interval",
        seconds = PipelineConfig.POLL_INTERVAL_SECONDS,
        args    = [producer],
        id      = "poll_quotes",
    )
    log.info(f"⏱  Polling every {PipelineConfig.POLL_INTERVAL_SECONDS}s")
    scheduler.start()


if __name__ == "__main__":
    main()
