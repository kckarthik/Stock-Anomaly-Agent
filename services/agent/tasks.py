"""
tasks.py — Celery Task Definitions
=====================================
Defines the investigate_anomaly Celery task.
Workers import this module and execute tasks from the queue.

Queue routing:
  severity_score 3 → queue: high    (investigated first)
  severity_score 2 → queue: medium
  severity_score 1 → queue: low

Retry policy:
  Max 3 retries with 30s backoff (handles Ollama timeouts).
"""

import logging
from celery import Celery
from celery.utils.log import get_task_logger
from config import RedisConfig, PipelineConfig
import investigator

# ── Celery app ─────────────────────────────────────────────────────
celery_app = Celery(
    "stock_agent",
    broker        = RedisConfig.URL,
    backend       = RedisConfig.URL,
)

celery_app.conf.update(
    task_serializer            = "json",
    result_serializer          = "json",
    accept_content             = ["json"],
    timezone                   = "UTC",
    enable_utc                 = True,
    task_track_started         = True,
    task_acks_late             = True,   # don't ack until task succeeds
    worker_prefetch_multiplier = 1,      # one task at a time per worker
    # No task_routes: scheduler dispatches to high/medium/low queues
    # directly via apply_async(queue=...) and the worker listens on all three.
)

log = get_task_logger(__name__)


@celery_app.task(
    bind            = True,
    name            = "tasks.investigate_anomaly",
    max_retries     = PipelineConfig.AGENT_MAX_RETRIES,
    default_retry_delay = PipelineConfig.AGENT_RETRY_DELAY,
    soft_time_limit = 360,   # 6 min: 4 decisions×60s + synthesis×60s + overhead
    time_limit      = 420,   # 7 min hard limit
)
def investigate_anomaly(self, anomaly_id: str, severity_score: int = 1) -> dict:
    """
    Celery task: investigate one anomaly end-to-end.

    Args:
        anomaly_id:     UUID from gold.anomaly_feed
        severity_score: 1=low, 2=medium, 3=high (for logging)

    Returns:
        Completed investigation report dict.

    Retries:
        On any exception, retries up to AGENT_MAX_RETRIES times
        with AGENT_RETRY_DELAY seconds backoff.
        Handles: Ollama timeouts, DB connection blips, SEC EDGAR 429s.
    """
    log.info(
        f"🔍 Task received: anomaly={anomaly_id[:16]}  "
        f"severity={severity_score}  attempt={self.request.retries + 1}"
    )

    try:
        report = investigator.run(anomaly_id)
        log.info(
            f"✅ Task complete: {report.get('symbol')}  "
            f"severity={report.get('severity')}"
        )
        return report

    except Exception as exc:
        log.error(
            f"❌ Task failed (attempt {self.request.retries + 1}/"
            f"{PipelineConfig.AGENT_MAX_RETRIES}): {exc}"
        )
        raise self.retry(exc=exc)
