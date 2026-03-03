"""
investigator.py — Core Investigation Runner
=============================================
Runs the 5 deterministic tool calls then calls the LLM once.
Called by Celery workers for each anomaly.

Flow per investigation:
  1. get_anomaly_detail()      → full context from Gold
  2. get_price_history()       → 20-day baseline
  3. get_sector_peers()        → peer comparison
  4. get_sec_filings()         → insider trades + material events
  5. get_options_data()        → put/call ratio
  ─────────────────────────────────────────────────
  6. llm.synthesise()          → ONE model call
  7. save_report()             → write to PostgreSQL
  8. mark_investigated()       → update anomaly_feed
"""

import uuid
import time
import logging
import json
import psycopg2
from datetime import datetime, timezone
from typing import Optional

from config import TimescaleConfig
from tools import (
    get_anomaly_detail,
    get_price_history,
    get_sector_peers,
    get_sec_filings,
    get_options_data,
)
import llm

log = logging.getLogger(__name__)


def run(anomaly_id: str) -> dict:
    """
    Full investigation for one anomaly.
    Returns the completed investigation report dict.
    """
    run_id   = str(uuid.uuid4())
    start    = time.time()

    log.info(f"═══ Investigation start  anomaly={anomaly_id[:16]}  run={run_id[:8]} ═══")


    # ── Tool 1: Anomaly Detail ──────────────────────────────────
    log.info("  [1/5] get_anomaly_detail")
    anomaly = get_anomaly_detail(anomaly_id)

    if not anomaly or "error" in anomaly:
        log.error(f"  ❌ Cannot fetch anomaly {anomaly_id}: {anomaly}")
        return {"error": "anomaly_not_found", "anomaly_id": anomaly_id}

    symbol = anomaly["symbol"]
    log.info(f"  ✅ {symbol} — {anomaly['anomaly_type']} — severity {anomaly['severity_score']}/3")

    # ── Tool 2: Price History ───────────────────────────────────
    log.info(f"  [2/5] get_price_history({symbol})")
    history = get_price_history(symbol, days=20)
    log.info(f"  ✅ 5d trend: {history.get('5d_trend_pct')}%  avg_vol: {history.get('avg_daily_volume', 0):,}")

    # ── Tool 3: Sector Peers ────────────────────────────────────
    log.info(f"  [3/5] get_sector_peers({symbol})")
    peers = get_sector_peers(symbol)
    log.info(f"  ✅ {peers.get('assessment')}")

    # ── Tool 4: SEC Filings ─────────────────────────────────────
    log.info(f"  [4/5] get_sec_filings({symbol})")
    sec = get_sec_filings(symbol, days=7)
    log.info(f"  ✅ {sec.get('assessment')}")

    # ── Tool 5: Options Data ────────────────────────────────────
    log.info(f"  [5/5] get_options_data({symbol})")
    options = get_options_data(symbol)
    log.info(f"  ✅ {options.get('assessment')}")

    # ── Collect all findings ────────────────────────────────────
    findings = {
        "price_history": history,
        "sector_peers":  peers,
        "sec_filings":   sec,
        "options_data":  options,
    }

    # ── LLM Synthesis (ONE call) ────────────────────────────────
    log.info("  [LLM] Calling Qwen2.5 0.5b for synthesis...")
    t_llm = time.time()
    report = llm.synthesise(anomaly, findings, trace_id=run_id)
    llm_time = time.time() - t_llm
    log.info(f"  ✅ LLM done in {llm_time:.2f}s — severity={report.get('severity')}")

    # ── Build final report ──────────────────────────────────────
    elapsed = time.time() - start

    final_report = {
        "report_id":          f"inv_{run_id[:8]}",
        "anomaly_id":         anomaly_id,
        "symbol":             symbol,
        "sector":             anomaly.get("sector"),
        "anomaly_type":       anomaly.get("anomaly_type"),
        "anomaly_score":      anomaly.get("anomaly_score"),
        "detected_at":        str(anomaly.get("detected_at")),
        "investigated_at":    datetime.now(timezone.utc).isoformat(),
        "severity":           report.get("severity", "MEDIUM"),
        "hypothesis":         report.get("hypothesis", ""),
        "evidence_summary":   report.get("evidence_summary", ""),
        "conclusion":         report.get("conclusion", ""),
        "confidence":         report.get("confidence", "LOW"),
        "recommended_action": report.get("recommended_action", "MONITOR"),
        "findings_json":      json.dumps(findings, default=str),
        "steps_taken":        5,
        "llm_time_seconds":   round(llm_time, 2),
        "total_time_seconds": round(elapsed, 2),
    }

    # ── Persist ─────────────────────────────────────────────────
    save_report(final_report)
    mark_investigated(anomaly_id)


    log.info(
        f"═══ Done  {symbol}  severity={final_report['severity']}  "
        f"time={elapsed:.1f}s ═══\n"
    )
    return final_report


# ── DB helpers ─────────────────────────────────────────────────────

def save_report(report: dict) -> None:
    """Persist investigation report to gold.investigation_reports."""
    try:
        conn = psycopg2.connect(TimescaleConfig.dsn())
        cur  = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO gold.investigation_reports (
                    report_id, anomaly_id, symbol, sector,
                    anomaly_type, anomaly_score, detected_at, investigated_at,
                    severity, hypothesis, evidence_summary, conclusion,
                    confidence, recommended_action, findings_json,
                    steps_taken, llm_time_seconds, total_time_seconds
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (anomaly_id) DO UPDATE SET
                    severity             = EXCLUDED.severity,
                    hypothesis           = EXCLUDED.hypothesis,
                    evidence_summary     = EXCLUDED.evidence_summary,
                    conclusion           = EXCLUDED.conclusion,
                    investigated_at      = EXCLUDED.investigated_at
                """,
                (
                    report["report_id"],       report["anomaly_id"],
                    report["symbol"],          report["sector"],
                    report["anomaly_type"],    report["anomaly_score"],
                    report["detected_at"],     report["investigated_at"],
                    report["severity"],        report["hypothesis"],
                    report["evidence_summary"],report["conclusion"],
                    report["confidence"],      report["recommended_action"],
                    report["findings_json"],   report["steps_taken"],
                    report["llm_time_seconds"],report["total_time_seconds"],
                ),
            )
            conn.commit()
            log.info("  💾 Report saved → gold.investigation_reports")
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        log.error(f"save_report failed: {e}")


def mark_investigated(anomaly_id: str) -> None:
    """Mark anomaly as investigated so scheduler skips it next cycle."""
    try:
        conn = psycopg2.connect(TimescaleConfig.dsn())
        cur  = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE gold.anomaly_feed
                SET investigated    = TRUE,
                    investigated_at = NOW()
                WHERE anomaly_id = %s
                """,
                (anomaly_id,),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        log.error(f"mark_investigated failed: {e}")
