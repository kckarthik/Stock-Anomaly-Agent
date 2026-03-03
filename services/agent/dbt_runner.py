"""
dbt_runner.py — dbt transformation runner
Runs dbt models every 5 minutes to refresh Gold tables.
Exposes Prometheus metrics on port 8003.
"""

import time
import subprocess
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from config import PipelineConfig

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [DBT] %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

DBT_DIR = "/app/dbt"

dbt_runs     = Counter("stock_dbt_runs_total", "Total dbt runs", ["status"])
dbt_duration = Histogram("stock_dbt_run_duration_seconds", "dbt run duration")
dbt_success  = Gauge("stock_dbt_last_run_success", "1 if last run succeeded")


def run_dbt():
    log.info("── dbt run starting ────────────────────────────────────")
    start = time.time()

    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR],
        capture_output = True,
        text           = True,
        cwd            = DBT_DIR,
    )

    elapsed = time.time() - start
    dbt_duration.observe(elapsed)

    if result.returncode == 0:
        log.info(f"✅ dbt run succeeded in {elapsed:.1f}s")
        dbt_runs.labels(status="success").inc()
        dbt_success.set(1)
    else:
        log.error(f"❌ dbt run failed:\n{result.stderr[-800:]}")
        dbt_runs.labels(status="failure").inc()
        dbt_success.set(0)


def main():
    start_http_server(8003)
    log.info("⏳ Waiting 60s for Silver data...")
    time.sleep(60)
    run_dbt()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(run_dbt, "interval", minutes=5, id="dbt_run")
    log.info("⏱  dbt runs every 5 min")
    scheduler.start()


if __name__ == "__main__":
    main()
