"""
api/main.py — FastAPI Natural Language Query Service
Docs: http://localhost:8080/docs
"""
import logging
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from config import TimescaleConfig, OllamaConfig

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(
    title="Stock Market Agent API",
    description="Natural language interface to the Stock Anomaly Investigation Agent",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def get_conn():
    return psycopg2.connect(TimescaleConfig.dsn())

class AskRequest(BaseModel):
    question: str
    max_reports: Optional[int] = 10

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/reports")
def get_reports(limit: int = 20, severity: Optional[str] = None):
    conn = get_conn()
    try:
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where = "WHERE severity = %s" if severity else ""
        args  = (severity.upper(),) if severity else ()
        cur.execute(
            f"""SELECT report_id, symbol, sector, anomaly_type, severity, hypothesis,
                   evidence_summary, conclusion, confidence, recommended_action,
                   investigated_at, total_time_seconds, steps_taken, react_steps
               FROM gold.investigation_reports {where}
               ORDER BY investigated_at DESC LIMIT %s""",
            args + (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return {"count": len(rows), "reports": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/reports/{symbol}")
def get_symbol_reports(symbol: str, limit: int = 10):
    conn = get_conn()
    try:
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT report_id, symbol, anomaly_type, severity, hypothesis,
                      evidence_summary, conclusion, confidence, recommended_action,
                      investigated_at, steps_taken, react_steps
               FROM gold.investigation_reports WHERE symbol = %s
               ORDER BY investigated_at DESC LIMIT %s""",
            (symbol.upper(), limit),
        )
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return {"symbol": symbol.upper(), "count": len(rows), "reports": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/anomalies")
def get_anomalies(investigated: bool = False, limit: int = 20):
    conn = get_conn()
    try:
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT anomaly_id, symbol, sector, anomaly_type, anomaly_score,
                      severity_score, severity_label, context, detected_at,
                      investigated, investigated_at
               FROM gold.anomaly_feed
               WHERE investigated = %s
               ORDER BY severity_score DESC, detected_at DESC LIMIT %s""",
            (investigated, limit),
        )
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        return {"count": len(rows), "anomalies": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/stats")
def get_stats():
    conn = get_conn()
    try:
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT COUNT(*) AS total_today,
                      COUNT(*) FILTER (WHERE severity='HIGH')   AS high_count,
                      COUNT(*) FILTER (WHERE severity='MEDIUM') AS medium_count,
                      COUNT(*) FILTER (WHERE severity='LOW')    AS low_count,
                      ROUND(AVG(total_time_seconds)::NUMERIC,2) AS avg_investigation_seconds,
                      ROUND(AVG(llm_time_seconds)::NUMERIC,2)  AS avg_llm_seconds
               FROM gold.investigation_reports
               WHERE investigated_at::date = CURRENT_DATE"""
        )
        stats = dict(cur.fetchone())
        cur.close()
        return {"date": datetime.now().date().isoformat(), "stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/ask")
def ask(request: AskRequest):
    # ── DB query (connection closed before LLM call) ────────────
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """SELECT symbol, anomaly_type, severity, hypothesis,
                      evidence_summary, conclusion, investigated_at
               FROM gold.investigation_reports
               WHERE investigated_at::date = CURRENT_DATE
               ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
                        investigated_at DESC
               LIMIT %s""",
            (request.max_reports,),
        )
        reports = [dict(r) for r in cur.fetchall()]
        cur.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

    if not reports:
        return {"question": request.question,
                "answer": "No reports available yet. Agent runs every 5 min during market hours.",
                "sources": []}

    # ── LLM synthesis ───────────────────────────────────────────
    try:
        context = "\n\n".join([
            f"[{r['investigated_at']}] {r['symbol']} — {r['severity']}\n"
            f"Type: {r['anomaly_type']}\nHypothesis: {r['hypothesis']}\n"
            f"Evidence: {r['evidence_summary']}\nConclusion: {r['conclusion']}"
            for r in reports
        ])

        prompt = f"""You are a stock market analyst. Today's investigation reports:

{context}

Question: {request.question}

Answer concisely based only on the reports above."""

        resp = requests.post(
            f"{OllamaConfig.HOST}/api/chat",
            json={"model": OllamaConfig.MODEL, "stream": False,
                  "options": {"temperature": 0.2, "num_predict": 400},
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=60,
        )
        resp.raise_for_status()
        return {"question": request.question,
                "answer": resp.json()["message"]["content"].strip(),
                "sources": [r["symbol"] for r in reports],
                "based_on": len(reports)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
