"""
llm.py — Ollama Qwen2.5 client
================================
Observability: dual-track
  1. Langfuse (self-hosted at localhost:3002) — full UI, traces, token counts
     Only active once you paste real keys into .env after Langfuse first-boot setup.
     Until then: silently skipped, no crash.
  2. TimescaleDB obs.llm_traces — always written, zero config needed.

Both run independently. If Langfuse keys are placeholder, only DB trace is written.
"""

import json
import time
import logging
import threading
import requests
import psycopg2
from typing import Optional
from config import OllamaConfig, TimescaleConfig

log = logging.getLogger(__name__)

# ── Langfuse — best-effort, never crashes pipeline ─────────────────
_langfuse = None


def _init_langfuse():
    """Create a fresh Langfuse client. Called at import and after Celery fork."""
    global _langfuse
    try:
        import os
        pk = os.getenv("LANGFUSE_PUBLIC_KEY", "")
        sk = os.getenv("LANGFUSE_SECRET_KEY", "")
        host = os.getenv("LANGFUSE_HOST", "http://langfuse:3000")

        if pk.startswith("pk-lf-") and not pk.endswith("placeholder"):
            from langfuse import Langfuse
            _langfuse = Langfuse(public_key=pk, secret_key=sk, host=host)
            log.info(f"✅ Langfuse connected → {host}")
        else:
            log.info("ℹ️  Langfuse keys not set yet — traces go to TimescaleDB only")
            _langfuse = None
    except Exception as e:
        log.warning(f"Langfuse init skipped: {e}")
        _langfuse = None


_init_langfuse()


# ── Reinit after Celery fork — threads don't survive fork() ────────
try:
    from celery.signals import worker_process_init

    @worker_process_init.connect
    def _reinit_langfuse_post_fork(**kwargs):
        """Each Celery fork child needs its own Langfuse client/thread."""
        _init_langfuse()
        if _langfuse:
            log.info("✅ Langfuse re-initialized in fork worker")
except ImportError:
    pass  # Not running inside Celery — fine


def _safe_flush(timeout_s: int = 10) -> None:
    """Flush Langfuse with a wall-clock timeout — safe for Celery fork workers."""
    if not _langfuse:
        return
    t = threading.Thread(target=_langfuse.flush, daemon=True)
    t.start()
    t.join(timeout=timeout_s)
    # If t is still alive after timeout, the flush is still running in background.
    # The daemon flag ensures it won't prevent process exit.


SYSTEM_PROMPT = """You are a stock market analyst. You will be given evidence
collected about a stock market anomaly. Write a concise investigation report.

Respond ONLY with valid JSON in this exact format:
{
  "severity": "HIGH|MEDIUM|LOW",
  "hypothesis": "one sentence describing what you think is happening",
  "evidence_summary": "2-3 sentences summarising the key findings",
  "conclusion": "one sentence with your final assessment",
  "confidence": "HIGH|MEDIUM|LOW",
  "recommended_action": "MONITOR|INVESTIGATE_FURTHER|ESCALATE|NO_ACTION"
}

Rules:
- Be concise. Maximum 2 sentences per field.
- Base severity on: HIGH if SEC filings found OR sector-wide, MEDIUM if isolated, LOW otherwise.
- Do not add any text outside the JSON object."""


def build_prompt(anomaly: dict, findings: dict) -> str:
    ctx = anomaly.get("context", {})
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except Exception:
            ctx = {}

    history = findings.get("price_history", {})
    peers   = findings.get("sector_peers",  {})
    sec     = findings.get("sec_filings",   {})
    options = findings.get("options_data",  {})

    avg_vol = history.get("avg_daily_volume", 0)
    avg_vol_str = f"{avg_vol:,}" if isinstance(avg_vol, int) else str(avg_vol)

    return f"""ANOMALY DETECTED:
Symbol: {anomaly.get('symbol')}
Type: {anomaly.get('anomaly_type')}
Severity score: {anomaly.get('severity_score')}/3
Time: {anomaly.get('bar_time')}

EVIDENCE COLLECTED:

1. VOLUME/PRICE:
{json.dumps(ctx, indent=2, default=str)}

2. PRICE HISTORY ({history.get('days_analyzed', 20)} days):
- 5-day trend: {history.get('5d_trend_pct')}% ({history.get('trend_direction')})
- Avg daily volume: {avg_vol_str}
- Avg price: ${history.get('avg_close_price')}

3. SECTOR PEERS ({peers.get('sector')}):
- {peers.get('assessment', 'No peer data')}
- Anomalous peers: {peers.get('anomalous_peers_count', 0)}/{len(peers.get('peers_checked', []))}

4. SEC EDGAR FILINGS (last 7 days):
- {sec.get('assessment', 'No SEC data')}
- Form 4 (insider trades): {sec.get('form4_count', 0)}
- Form 8-K (material events): {sec.get('form8k_count', 0)}

5. OPTIONS ACTIVITY:
- {options.get('assessment', 'No options data')}
- Put/call ratio: {options.get('put_call_ratio', 'N/A')}

Write your investigation report as JSON now:"""


def _save_to_db(trace_id: str, symbol: str, anomaly_type: str,
                prompt: str, response: str, input_tokens: int,
                output_tokens: int, latency_ms: int, success: bool,
                error: Optional[str] = None) -> None:
    """Always-on fallback: write trace to obs.llm_traces in TimescaleDB."""
    try:
        conn = psycopg2.connect(TimescaleConfig.dsn())
        cur  = conn.cursor()
        try:
            cur.execute(
                """INSERT INTO obs.llm_traces
                   (trace_id, symbol, anomaly_type, model, prompt, response,
                    input_tokens, output_tokens, latency_ms, success, error_message)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (trace_id, symbol, anomaly_type, OllamaConfig.MODEL,
                 prompt[:4000], response[:2000],
                 input_tokens, output_tokens, latency_ms, success, error),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        log.warning(f"DB trace write failed: {e}")


def synthesise(anomaly: dict, findings: dict,
               trace_id: Optional[str] = None) -> dict:
    """
    Call Qwen2.5 0.5b. Returns structured report dict.
    Traces written to both Langfuse (if configured) and TimescaleDB.
    """
    symbol       = anomaly.get("symbol", "UNKNOWN")
    anomaly_type = anomaly.get("anomaly_type", "unknown")
    prompt       = build_prompt(anomaly, findings)
    raw_content  = ""
    t_id         = trace_id or "no-id"
    start_ms     = int(time.time() * 1000)

    # Start Langfuse trace + generation span (best-effort)
    lf_generation = None
    if _langfuse and trace_id:
        try:
            lf_trace = _langfuse.trace(
                id   = trace_id,
                name = f"investigate:{symbol}:{anomaly_type}",
                metadata = {"symbol": symbol, "anomaly_type": anomaly_type},
            )
            lf_generation = lf_trace.generation(
                name  = "llm:qwen2.5-synthesise",
                model = OllamaConfig.MODEL,
                input = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
            )
        except Exception:
            pass

    try:
        response = requests.post(
            f"{OllamaConfig.HOST}/api/chat",
            json={
                "model":  OllamaConfig.MODEL,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 300},
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
            },
            timeout=60,
        )
        response.raise_for_status()

        data          = response.json()
        raw_content   = data["message"]["content"].strip()
        input_tokens  = data.get("prompt_eval_count", 0)
        output_tokens = data.get("eval_count", 0)
        latency_ms    = int(time.time() * 1000) - start_ms

        # Strip markdown fences if model wrapped output
        clean = raw_content
        if "```" in clean:
            lines = [ln for ln in clean.split("\n") if not ln.strip().startswith("```")]
            clean = "\n".join(lines)

        report = json.loads(clean.strip())

        # ── Langfuse (best-effort) ──────────────────────────────
        if lf_generation:
            try:
                lf_generation.end(
                    output = report,
                    usage  = {"input": input_tokens, "output": output_tokens},
                )
                _safe_flush()  # force send — Celery forks kill background thread
            except Exception:
                pass

        # ── TimescaleDB (always) ────────────────────────────────
        _save_to_db(t_id, symbol, anomaly_type, prompt, raw_content,
                    input_tokens, output_tokens, latency_ms, True)

        log.info(f"  🤖 LLM: {latency_ms}ms  "
                 f"in={input_tokens} out={output_tokens} tok  "
                 f"severity={report.get('severity')}")
        return report

    except json.JSONDecodeError:
        latency_ms = int(time.time() * 1000) - start_ms
        log.error(f"LLM non-JSON: {raw_content[:200]}")
        if lf_generation:
            try:
                lf_generation.end(output={"error": "json_parse_failed"}, level="ERROR")
                _safe_flush()
            except Exception:
                pass
        _save_to_db(t_id, symbol, anomaly_type, prompt, raw_content,
                    0, 0, latency_ms, False, "json_parse_failed")
        return _fallback("JSON parse failed")

    except Exception as e:
        latency_ms = int(time.time() * 1000) - start_ms
        log.error(f"LLM call failed: {e}")
        if lf_generation:
            try:
                lf_generation.end(output={"error": str(e)}, level="ERROR")
                _safe_flush()
            except Exception:
                pass
        _save_to_db(t_id, symbol, anomaly_type, prompt, "",
                    0, 0, latency_ms, False, str(e)[:200])
        return _fallback(str(e))


def _fallback(reason: str) -> dict:
    return {
        "severity":           "MEDIUM",
        "hypothesis":         f"LLM unavailable: {reason[:80]}",
        "evidence_summary":   "Investigation incomplete — LLM error",
        "conclusion":         "Manual review required",
        "confidence":         "LOW",
        "recommended_action": "INVESTIGATE_FURTHER",
    }


# ── ReAct decision function ─────────────────────────────────────────

DECISION_SYSTEM_PROMPT = (
    "You are a stock analyst deciding what to investigate next. "
    "Reply with ONLY one of the options listed. No explanation, no punctuation."
)


def _build_decision_prompt(
    symbol: str, anomaly_type: str, severity_score: int,
    evidence_summary: str, available_tools: list,
    step: int, min_steps: int,
) -> str:
    lines = [
        "SITUATION:",
        f"Symbol: {symbol} | Anomaly: {anomaly_type} | Severity: {severity_score}/3",
        "",
        "EVIDENCE SO FAR:",
        evidence_summary or "None yet.",
        "",
        "TOOLS AVAILABLE:",
    ]
    for i, tool in enumerate(available_tools, start=1):
        lines.append(f"{i}. {tool}")
    if step >= min_steps:
        lines.append(f"{len(available_tools) + 1}. CONCLUDE")
    lines.extend([
        "",
        "Reply with ONLY the tool name or CONCLUDE. No other text.",
    ])
    return "\n".join(lines)


def decide_next_action(
    symbol: str, anomaly_type: str, severity_score: int,
    evidence_summary: str, available_tools: list,
    step: int, min_steps: int, trace_id: Optional[str] = None,
) -> str:
    """
    Ask the LLM which tool to run next in the ReAct loop.
    Returns the raw response string. Never raises — returns "" on exception.
    Writes to obs.llm_traces always; attaches to Langfuse trace if configured.
    """
    t_id      = trace_id or "no-id"
    start_ms  = int(time.time() * 1000)
    prompt    = _build_decision_prompt(
        symbol, anomaly_type, severity_score,
        evidence_summary, available_tools, step, min_steps,
    )
    raw_content = ""

    # Langfuse span — attach to same trace as synthesise() (best-effort)
    lf_generation = None
    if _langfuse and trace_id:
        try:
            lf_trace = _langfuse.trace(
                id       = trace_id,
                name     = f"investigate:{symbol}:{anomaly_type}",
                metadata = {"symbol": symbol, "anomaly_type": anomaly_type},
            )
            lf_generation = lf_trace.generation(
                name  = f"llm:qwen2.5-decide-step{step}",
                model = OllamaConfig.MODEL,
                input = [
                    {"role": "system", "content": DECISION_SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
            )
        except Exception:
            pass

    try:
        response = requests.post(
            f"{OllamaConfig.HOST}/api/chat",
            json={
                "model":  OllamaConfig.MODEL,
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": 100},
                "messages": [
                    {"role": "system", "content": DECISION_SYSTEM_PROMPT},
                    {"role": "user",   "content": prompt},
                ],
            },
            timeout=60,
        )
        response.raise_for_status()

        data          = response.json()
        raw_content   = data["message"]["content"].strip()
        input_tokens  = data.get("prompt_eval_count", 0)
        output_tokens = data.get("eval_count", 0)
        latency_ms    = int(time.time() * 1000) - start_ms

        if lf_generation:
            try:
                lf_generation.end(
                    output = {"decision": raw_content},
                    usage  = {"input": input_tokens, "output": output_tokens},
                )
                _safe_flush()
            except Exception:
                pass

        _save_to_db(
            t_id, symbol, f"decide-step{step}:{anomaly_type}",
            prompt, raw_content, input_tokens, output_tokens, latency_ms, True,
        )
        return raw_content

    except Exception as e:
        latency_ms = int(time.time() * 1000) - start_ms
        log.error(f"decide_next_action failed at step {step}: {e}")

        if lf_generation:
            try:
                lf_generation.end(output={"error": str(e)}, level="ERROR")
                _safe_flush()
            except Exception:
                pass

        _save_to_db(
            t_id, symbol, f"decide-step{step}:{anomaly_type}",
            prompt, "", 0, 0, latency_ms, False, str(e)[:200],
        )
        return ""
