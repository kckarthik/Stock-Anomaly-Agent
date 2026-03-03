"""
investigator.py — ReAct Investigation Runner
=============================================
Implements a Reason+Act loop: the LLM decides which tool to run next
based on accumulating evidence, and can conclude early when satisfied.

Flow per investigation:
  Step 0: get_anomaly_detail()   ← mandatory, no LLM choice
  Loop up to MAX_REACT_STEPS:
    - llm.decide_next_action()   → pick tool or CONCLUDE
    - run chosen tool
    - update evidence
  Final: llm.synthesise()        → ONE synthesis call
         save_report()           → write to PostgreSQL
         mark_investigated()     → update anomaly_feed
"""

import uuid
import time
import logging
import json
import psycopg2
from datetime import datetime, timezone

from config import TimescaleConfig, PipelineConfig
from tools import (
    get_anomaly_detail,
    get_price_history,
    get_sector_peers,
    get_sec_filings,
    get_options_data,
)
import llm

log = logging.getLogger(__name__)


# ── Tool dispatch table ─────────────────────────────────────────────
TOOL_DISPATCH = {
    "get_price_history": lambda s: get_price_history(s, days=20),
    "get_sector_peers":  lambda s: get_sector_peers(s),
    "get_sec_filings":   lambda s: get_sec_filings(s, days=7),
    "get_options_data":  lambda s: get_options_data(s),
}

# Maps tool name → findings dict key (for build_prompt() compatibility)
_TOOL_FINDING_KEYS = {
    "get_price_history": "price_history",
    "get_sector_peers":  "sector_peers",
    "get_sec_filings":   "sec_filings",
    "get_options_data":  "options_data",
}


def _build_evidence_summary(findings: dict) -> str:
    """
    One terse line per finding for the decision prompt (~150 tokens max).
    """
    if not findings:
        return "No evidence collected yet."
    lines = []
    for key, data in findings.items():
        if not isinstance(data, dict):
            continue
        if "error" in data:
            lines.append(f"- {key}: ERROR — {data['error'][:60]}")
        elif data.get("assessment"):
            lines.append(f"- {key}: {data['assessment'][:100]}")
    return "\n".join(lines) if lines else "No meaningful findings yet."


def _result_summary(tool_name: str, result: dict) -> str:
    """One-liner for react_steps JSONB storage."""
    if "error" in result:
        return f"ERROR: {result['error'][:80]}"
    assessment = result.get("assessment", "")
    if assessment:
        return assessment[:120]
    if tool_name == "get_price_history":
        avg_vol = result.get("avg_daily_volume", 0)
        avg_vol_str = f"{avg_vol:,}" if isinstance(avg_vol, int) else str(avg_vol)
        return f"trend={result.get('5d_trend_pct')}%  avg_vol={avg_vol_str}"
    return "OK"


def _validate_decision(raw: str, available_tools: list, step: int, min_steps: int) -> tuple:
    """
    Normalise LLM's raw decision to a valid action.
    Returns (action, override_reason). override_reason="" means no override.
    """
    if not available_tools:
        return "CONCLUDE", "no_tools_left"

    if not raw:
        return available_tools[0], "empty_response"

    clean = raw.strip()

    # Direct match
    if clean in available_tools:
        return clean, ""

    # CONCLUDE check
    if "CONCLUDE" in clean.upper():
        if step < min_steps:
            return available_tools[0], f"early_conclude_at_step_{step}"
        return "CONCLUDE", ""

    # Partial match: "price_history" → "get_price_history"
    lower = clean.lower()
    for tool in available_tools:
        if tool.replace("get_", "") in lower or lower in tool:
            return tool, "partial_match"

    # Numeric match: "1" → available_tools[0]
    try:
        idx = int(clean.split()[0]) - 1
        if 0 <= idx < len(available_tools):
            return available_tools[idx], "numeric_choice"
    except (ValueError, IndexError):
        pass

    return available_tools[0], "no_match_fallback"


def react_run(anomaly_id: str) -> dict:
    """
    ReAct investigation loop for one anomaly.
    Returns the completed investigation report dict.
    """
    run_id = str(uuid.uuid4())
    start  = time.time()

    log.info(
        f"═══ ReAct Investigation start  anomaly={anomaly_id[:16]}  run={run_id[:8]} ═══"
    )

    # ── Step 0: Anomaly Detail (mandatory, no LLM choice) ──────────
    log.info("  [0] get_anomaly_detail (mandatory)")
    anomaly = get_anomaly_detail(anomaly_id)

    if not anomaly or "error" in anomaly:
        log.error(f"  ❌ Cannot fetch anomaly {anomaly_id}: {anomaly}")
        return {"error": "anomaly_not_found", "anomaly_id": anomaly_id}

    symbol         = anomaly["symbol"]
    anomaly_type   = anomaly.get("anomaly_type", "unknown")
    severity_score = anomaly.get("severity_score", 1)
    log.info(f"  ✅ {symbol} — {anomaly_type} — severity {severity_score}/3")

    findings         = {}
    react_steps_list = []
    available_tools  = list(TOOL_DISPATCH.keys())  # shallow copy; shrinks as tools are used
    max_steps        = PipelineConfig.MAX_REACT_STEPS
    min_steps        = PipelineConfig.MIN_REACT_STEPS

    # ── ReAct Loop ──────────────────────────────────────────────────
    for step in range(max_steps):
        evidence_summary = _build_evidence_summary(findings)

        raw = llm.decide_next_action(
            symbol           = symbol,
            anomaly_type     = anomaly_type,
            severity_score   = severity_score,
            evidence_summary = evidence_summary,
            available_tools  = available_tools,
            step             = step,
            min_steps        = min_steps,
            trace_id         = run_id,
        )

        action, override_reason = _validate_decision(raw, available_tools, step, min_steps)

        log.info(
            f"  [step {step}] LLM says={repr(raw)} → action={action}"
            + (f" (override: {override_reason})" if override_reason else "")
        )

        if action == "CONCLUDE":
            log.info(f"  ✅ Agent concluded after {step} steps")
            break

        # ── Run chosen tool ────────────────────────────────────────
        t_tool  = time.time()
        success = True
        result  = {}
        try:
            result = TOOL_DISPATCH[action](symbol)
            log.info(f"  ✅ {action}: {result.get('assessment', 'done')}")
        except Exception as e:
            log.error(f"  ❌ {action} failed: {e}")
            result  = {"error": str(e)}
            success = False

        duration_ms = int((time.time() - t_tool) * 1000)

        # Store under canonical key (synthesise/build_prompt compatibility)
        finding_key = _TOOL_FINDING_KEYS.get(action, action)
        findings[finding_key] = result

        # Remove used tool so LLM cannot pick it again
        available_tools = [t for t in available_tools if t != action]

        react_steps_list.append({
            "step":           step,
            "decision":       raw,
            "tool":           action,
            "rationale":      "override" if override_reason else "model_choice",
            "result_summary": _result_summary(action, result),
            "duration_ms":    duration_ms,
            "success":        success,
        })

        if not available_tools:
            log.info("  ✅ All tools exhausted")
            break

    # ── LLM Synthesis ───────────────────────────────────────────────
    log.info("  [LLM] Calling Qwen2.5 0.5b for synthesis...")
    t_llm    = time.time()
    report   = llm.synthesise(anomaly, findings, trace_id=run_id)
    llm_time = time.time() - t_llm
    log.info(f"  ✅ LLM done in {llm_time:.2f}s — severity={report.get('severity')}")

    # ── Build final report ──────────────────────────────────────────
    elapsed = time.time() - start

    final_report = {
        "report_id":          f"inv_{run_id[:8]}",
        "anomaly_id":         anomaly_id,
        "symbol":             symbol,
        "sector":             anomaly.get("sector"),
        "anomaly_type":       anomaly_type,
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
        "steps_taken":        len(react_steps_list),
        "llm_time_seconds":   round(llm_time, 2),
        "total_time_seconds": round(elapsed, 2),
        "react_steps":        react_steps_list,
    }

    save_report(final_report)
    mark_investigated(anomaly_id)

    log.info(
        f"═══ Done  {symbol}  severity={final_report['severity']}  "
        f"steps={len(react_steps_list)}  time={elapsed:.1f}s ═══\n"
    )
    return final_report


def run(anomaly_id: str) -> dict:
    """Compatibility shim — tasks.py calls this unchanged."""
    return react_run(anomaly_id)


# ── DB helpers ──────────────────────────────────────────────────────

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
                    steps_taken, llm_time_seconds, total_time_seconds,
                    react_steps
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (anomaly_id) DO UPDATE SET
                    severity             = EXCLUDED.severity,
                    hypothesis           = EXCLUDED.hypothesis,
                    evidence_summary     = EXCLUDED.evidence_summary,
                    conclusion           = EXCLUDED.conclusion,
                    confidence           = EXCLUDED.confidence,
                    recommended_action   = EXCLUDED.recommended_action,
                    findings_json        = EXCLUDED.findings_json,
                    investigated_at      = EXCLUDED.investigated_at,
                    react_steps          = EXCLUDED.react_steps,
                    steps_taken          = EXCLUDED.steps_taken,
                    llm_time_seconds     = EXCLUDED.llm_time_seconds,
                    total_time_seconds   = EXCLUDED.total_time_seconds
                """,
                (
                    report["report_id"],          report["anomaly_id"],
                    report["symbol"],             report["sector"],
                    report["anomaly_type"],       report["anomaly_score"],
                    report["detected_at"],        report["investigated_at"],
                    report["severity"],           report["hypothesis"],
                    report["evidence_summary"],   report["conclusion"],
                    report["confidence"],         report["recommended_action"],
                    report["findings_json"],      report["steps_taken"],
                    report["llm_time_seconds"],   report["total_time_seconds"],
                    json.dumps(report.get("react_steps", []), default=str),
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
