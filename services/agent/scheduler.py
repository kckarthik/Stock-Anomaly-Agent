"""
scheduler.py — Agent Scheduler
Wakes every 5 min, reads anomaly_feed, dispatches Celery tasks.
"""

import time
import logging
import psycopg2
import psycopg2.extras
from apscheduler.schedulers.blocking import BlockingScheduler
from prometheus_client import start_http_server, Counter, Gauge

from config import TimescaleConfig, PipelineConfig
from tasks import investigate_anomaly

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [SCHEDULER] %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

anomalies_dispatched = Counter(
    "agent_anomalies_dispatched_total",
    "Total anomaly investigations dispatched",
    ["severity"],
)
anomalies_pending = Gauge(
    "agent_anomalies_pending",
    "Uninvestigated anomalies in feed",
)
scheduler_cycles = Counter(
    "agent_scheduler_cycles_total",
    "Total scheduler cycles",
)

QUEUE_MAP = {3: "high", 2: "medium", 1: "low"}


def fetch_pending() -> list[dict]:
    try:
        conn = psycopg2.connect(TimescaleConfig.dsn())
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(
                """
                SELECT anomaly_id, symbol, anomaly_type,
                       anomaly_score, severity_score, severity_label
                FROM gold.anomaly_feed
                WHERE investigated = FALSE
                ORDER BY severity_score DESC, anomaly_score DESC
                LIMIT 20
                """
            )
            return [dict(r) for r in cur.fetchall()]
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        log.error(f"fetch_pending error: {e}")
        return []


def dispatch_cycle() -> None:
    scheduler_cycles.inc()
    log.info("── Scheduler cycle ─────────────────────────────────────")
    anomalies = fetch_pending()
    anomalies_pending.set(len(anomalies))

    if not anomalies:
        log.info("  💤 No pending anomalies")
        return

    log.info(f"  📋 {len(anomalies)} to investigate")
    for anomaly in anomalies:
        queue = QUEUE_MAP.get(anomaly["severity_score"], "low")
        try:
            investigate_anomaly.apply_async(
                args  = [str(anomaly["anomaly_id"]), anomaly["severity_score"]],
                queue = queue,
            )
            anomalies_dispatched.labels(severity=anomaly["severity_label"]).inc()
            log.info(f"  ➤ [{queue.upper()}] {anomaly['symbol']} — {anomaly['anomaly_type']}")
        except Exception as e:
            log.error(f"  ❌ Dispatch failed: {e}")

    log.info("── Done ─────────────────────────────────────────────────\n")


def main():
    start_http_server(8004)
    log.info("⏳ Waiting 90s for Gold layer to populate...")
    time.sleep(90)
    dispatch_cycle()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        dispatch_cycle,
        trigger = "interval",
        minutes = PipelineConfig.AGENT_SCHEDULE_MINUTES,
        id      = "agent_dispatch",
    )
    log.info(f"⏱  Every {PipelineConfig.AGENT_SCHEDULE_MINUTES} min")
    scheduler.start()


if __name__ == "__main__":
    main()
