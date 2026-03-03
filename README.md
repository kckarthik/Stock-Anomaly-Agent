# Stock Market Anomaly Investigation Agent

> A production-grade agentic data engineering pipeline that monitors real stock market data, detects statistical anomalies, and autonomously investigates them using a ReAct (Reason+Act) loop driven by a local LLM — entirely on-premises, with no cloud dependencies and no paid APIs.

---

## Overview

Every 5 minutes, the agent:

1. Reads newly flagged anomalies from the Gold data layer
2. Launches a **ReAct investigation loop** — the LLM decides which tool to run next based on accumulating evidence, adapting its strategy per anomaly
3. Concludes when sufficient evidence is gathered (2–4 tool calls, not a fixed sequence)
4. Synthesises all findings with a local LLM (Qwen2.5 0.5b via Ollama) into a structured report
5. Persists the full reasoning chain and report to TimescaleDB
6. Surfaces everything in Grafana and a REST API

The key distinction from rule-based pipelines: **the agent reasons about what to investigate next**, not just what threshold was breached. A volume spike in the Energy sector triggers different tool choices than an isolated intraday price move.

---

## Architecture

```
 Yahoo Finance (15-min delayed, free)
        │
        ▼  poll every 60s
 ┌─────────────┐
 │  Producer   │  fetches 20 symbols across 4 sectors
 └──────┬──────┘
        │  topic: raw_quotes
        ▼
 ┌─────────────┐
 │    Kafka    │  KRaft mode · 24h retention
 └──────┬──────┘
        │  batch every 60s
        ▼
 ┌─────────────┐
 │  Consumer   │──── Bronze ──► MinIO  (raw Parquet, partitioned by symbol/date/hour)
 │             │──── Silver ──► MinIO  (OHLCV 1-min bars)
 └──────┬──────┘          └──► TimescaleDB  silver.ohlcv_1min
        │  every 5 min
        ▼
 ┌─────────────┐    Gold layer (TimescaleDB)
 │ dbt Runner  │──► ohlcv_1min        base 1-min OHLCV bars
 │             │──► volume_baseline   20-day rolling avg per symbol/hour
 │             │──► volume_anomalies  z-score spikes vs baseline
 │             │──► price_anomalies   moves > 2% in < 5 min
 │             │──► sector_summary    sector-level aggregation
 │             │──► anomaly_feed      unified flagged events  ◄── agent reads this
 └──────┬──────┘
        │  every 5 min
        ▼
 ┌──────────────────┐
 │ Agent Scheduler  │  reads anomaly_feed · dispatches Celery tasks by priority
 └────────┬─────────┘
          │  HIGH / MEDIUM / LOW priority queues
          ▼
 ┌────────────────────────────────────────────────┐
 │          Celery Workers  (x2 parallel)         │
 │                                                │
 │  Step 0 (mandatory):  get_anomaly_detail       │  ← Gold DB
 │                                                │
 │  ReAct Loop (LLM-directed, 2–4 iterations):   │
 │  ┌──────────────────────────────────────────┐  │
 │  │  LLM decides:  which tool to run next?   │  │
 │  │                                          │  │
 │  │  Available tools:                        │  │
 │  │    • get_price_history  ← yfinance       │  │
 │  │    • get_sector_peers   ← Gold DB        │  │
 │  │    • get_sec_filings    ← SEC EDGAR      │  │
 │  │    • get_options_data   ← yfinance       │  │
 │  │    • CONCLUDE           (early exit)     │  │
 │  │                                          │  │
 │  │  → run chosen tool → add to evidence     │  │
 │  │  → loop until CONCLUDE or step limit     │  │
 │  └──────────────────────────────────────────┘  │
 │                                                │
 │  Final: LLM synthesises all evidence once  ────┼──► TimescaleDB  gold.investigation_reports
 └────────────────────────────────────────────────┘

 Observability
 ├── Langfuse    multi-span traces — one span per LLM decision + synthesis
 ├── Flower      every Celery task — active · retried · failed
 ├── Prometheus  pipeline throughput metrics from all services
 └── Grafana     14 live panels — data health · anomalies · reports · LLM performance
```

---

## Services

| Service | Port | Role |
|---|---|---|
| Kafka (KRaft) | 9092 | Message broker |
| Kafka UI | 8090 | Topic browser, consumer lag |
| MinIO | 9000 / 9001 | Local S3 — Bronze & Silver Parquet |
| TimescaleDB | 5432 | Time-series Postgres — all structured data |
| Redis | 6379 | Celery broker & result backend |
| Ollama | 11434 | Local LLM server |
| Langfuse | 3002 | LLM observability UI |
| Prometheus | 9090 | Metrics scraping |
| Grafana | 3001 | Dashboards |
| Flower | 5555 | Celery task monitor |
| Producer | 8001 | yfinance poller → Kafka |
| Consumer | 8002 | Kafka → MinIO + TimescaleDB |
| dbt Runner | 8003 | Silver → Gold transforms |
| Agent Scheduler | 8004 | Anomaly dispatch |
| FastAPI | 8080 | REST API + `/ask` endpoint |

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker Desktop 4.x+ | Must be running |
| 8 GB RAM | Stack uses ~3 GB at steady state |
| 5 GB disk | Ollama model + data volumes |

---

## Quick Start

```bash
cd stock-agent
chmod +x start.sh stop.sh
./start.sh
```

First run downloads the Qwen2.5:0.5b model (~400 MB). Allow 3–5 minutes for all services to become healthy.

---

## User Interfaces

| Interface | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3001 | admin / admin123 |
| Langfuse | http://localhost:3002 | create on first visit |
| Flower | http://localhost:5555 | — |
| API Docs | http://localhost:8080/docs | — |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | — |

---

## Grafana Dashboard

Fourteen live panels auto-provisioned at startup:

| Panel | Description |
|---|---|
| Last Quote Received | Timestamp of most recent market bar |
| Bars Last 5 Min | Live data freshness indicator |
| Investigations Today | Report count for current day |
| Pending Anomalies | Uninvestigated queue depth |
| Latest Agent Reports | Last 20 reports with hypothesis, steps taken, and recommended action |
| Severity Breakdown | Pie chart — HIGH / MEDIUM / LOW distribution |
| Anomaly Feed | Current queue with anomaly scores |
| Quotes Published / min | Producer throughput (Prometheus) |
| Agent Tasks Dispatched | Dispatch rate by severity (Prometheus) |
| LLM Traces | Last 20 calls — model, tokens, latency, call type |
| Avg LLM Latency | Rolling 24-hour average response time |
| LLM Success Rate | Parse success rate as a reliability gauge |
| Avg Steps / Investigation | Mean ReAct iterations per investigation (lower = more decisive) |
| ReAct Override Rate % | Fraction of decisions where the validator overrode LLM output |

---

## ReAct Investigation Loop

The agent uses a **ReAct (Reason + Act)** pattern for all investigations. Rather than running every tool in a fixed sequence, the LLM examines the evidence collected so far and selects the most relevant next action.

### How it works

```
Step 0:  get_anomaly_detail   ← always runs first (no LLM decision needed)

Step 1:  LLM receives evidence summary → chooses one of:
           get_price_history | get_sector_peers | get_sec_filings | get_options_data
         → tool runs → result added to findings

Step 2:  LLM receives updated evidence → chooses next tool or CONCLUDE
         (CONCLUDE only available after MIN_REACT_STEPS = 2)

...up to MAX_REACT_STEPS (default 4) iterations

Final:   LLM synthesises all accumulated findings into structured report
```

### Decision validation

Every LLM decision passes through a deterministic validator before execution:

| LLM output | Outcome |
|---|---|
| Exact tool name | Used directly |
| Partial match (e.g. `"price_history"`) | Resolved to `get_price_history` |
| Numeric choice (e.g. `"1"`) | Mapped to tool at that index |
| `CONCLUDE` before `MIN_REACT_STEPS` | Overridden — continue with next available tool |
| Unrecognisable output | Fallback to first remaining tool |

Override decisions are logged, written to `obs.llm_traces`, and exposed in the **ReAct Override Rate %** Grafana panel — giving full observability into model reliability.

### react_steps schema

The full reasoning chain is stored in the `react_steps` JSONB column of `gold.investigation_reports`:

```json
[
  {
    "step": 0,
    "decision": "get_anomaly_detail",
    "tool": "get_anomaly_detail",
    "rationale": "mandatory",
    "result_summary": "volume_zscore=3.8, price_change=+1.2%",
    "duration_ms": 45,
    "success": true
  },
  {
    "step": 1,
    "decision": "get_sec_filings",
    "tool": "get_sec_filings",
    "rationale": "model_choice",
    "result_summary": "FOUND: 2 Form 4 insider trades",
    "duration_ms": 820,
    "success": true
  },
  {
    "step": 2,
    "decision": "get_sector_peers",
    "tool": "get_sector_peers",
    "rationale": "model_choice",
    "result_summary": "ISOLATED: sector peers normal",
    "duration_ms": 310,
    "success": true
  }
]
```

---

## Investigation Report Schema

```json
{
  "report_id":          "uuid",
  "symbol":             "AAPL",
  "sector":             "Technology",
  "anomaly_type":       "volume_spike",
  "severity":           "HIGH | MEDIUM | LOW",
  "hypothesis":         "one sentence on what is happening",
  "evidence_summary":   "2–3 sentences on key findings",
  "conclusion":         "final assessment",
  "confidence":         "HIGH | MEDIUM | LOW",
  "recommended_action": "MONITOR | INVESTIGATE_FURTHER | ESCALATE | NO_ACTION",
  "steps_taken":        3,
  "react_steps":        [...],
  "total_time_seconds": 42.1,
  "investigated_at":    "2026-03-03T10:32:15Z"
}
```

**Retry policy:** up to 3 retries with 30-second backoff — handles Ollama timeouts, DB connection blips, and SEC EDGAR rate limits.

---

## Anomaly Detection

### Detection thresholds (`.env`)

| Parameter | Default | Meaning |
|---|---|---|
| `VOLUME_ZSCORE_THRESHOLD` | 2.5 | Flag if volume exceeds 2.5 standard deviations above the 20-day rolling average |
| `PRICE_CHANGE_THRESHOLD` | 2.0 | Flag if price moves more than 2% within a 5-minute window |

### Agent behaviour (`.env`)

| Parameter | Default | Meaning |
|---|---|---|
| `MAX_REACT_STEPS` | 4 | Maximum tool calls per investigation |
| `MIN_REACT_STEPS` | 2 | Minimum before LLM may choose CONCLUDE |
| `AGENT_SCHEDULE_MINUTES` | 5 | Scheduler polling interval |
| `MAX_INVESTIGATION_STEPS` | 5 | Celery task hard cap |
| `AGENT_MAX_RETRIES` | 3 | Retry attempts per failed task |
| `AGENT_RETRY_DELAY_SECONDS` | 30 | Backoff between retries |

---

## REST API

```bash
# Health check
GET  /health

# Investigation reports
GET  /reports
GET  /reports?severity=HIGH
GET  /reports/{symbol}

# Anomaly feed
GET  /anomalies
GET  /anomalies?investigated=false

# Pipeline statistics
GET  /stats

# Natural language query (powered by local LLM)
POST /ask
{ "question": "What was the most unusual event today and why?" }
```

Full interactive documentation: http://localhost:8080/docs

---

## LLM Observability

### Langfuse traces

When Langfuse is configured, each investigation produces a multi-span trace — one generation span per ReAct decision, plus the final synthesis span, all linked under a single `trace_id`:

```
Trace: investigate:AAPL:volume_spike                    (total 42s)
  ├── llm:qwen2.5-decide-step1    480ms   → get_sec_filings
  ├── tool:get_sec_filings        820ms   Form 4: insider sold $9.2M
  ├── llm:qwen2.5-decide-step2    510ms   → get_sector_peers
  ├── tool:get_sector_peers       310ms   ISOLATED: peers normal
  ├── llm:qwen2.5-decide-step3    490ms   → CONCLUDE
  └── llm:qwen2.5-synthesise      38.2s   severity: HIGH  input=480 out=210 tok
```

### TimescaleDB fallback

Without Langfuse keys, every LLM call is still written to `obs.llm_traces` in TimescaleDB — zero configuration required. The Grafana "LLM Traces" panel reads from this table directly.

### Enabling Langfuse

1. Visit http://localhost:3002 → sign up (fully local, no internet required)
2. Create a project → Settings → API Keys → generate a key pair
3. Paste the keys into `.env` under `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY`
4. `docker compose restart celery-worker agent-scheduler`

---

## Symbols Tracked

| Sector | Symbols |
|---|---|
| Technology | AAPL · MSFT · GOOGL · NVDA · META |
| Finance | JPM · BAC · GS · MS · WFC |
| Healthcare | JNJ · PFE · UNH · ABBV · MRK |
| Energy | XOM · CVX · COP · SLB · EOG |

To add symbols, edit `WATCHLIST` in any service's `config.py` and run `docker compose build`.

---

## Project Structure

```
stock-agent/
├── docker-compose.yml
├── .env
├── start.sh
├── stop.sh
│
├── services/
│   ├── producer/           yfinance poller → Kafka
│   │   ├── main.py
│   │   ├── fetcher.py
│   │   ├── kafka_client.py
│   │   └── metrics.py
│   │
│   ├── consumer/           Kafka → Bronze → Silver
│   │   ├── main.py
│   │   ├── aggregator.py
│   │   └── storage.py
│   │
│   ├── agent/              orchestration · ReAct investigation · LLM
│   │   ├── scheduler.py    reads anomaly_feed · dispatches tasks by priority
│   │   ├── tasks.py        Celery task definitions + retry policy
│   │   ├── investigator.py ReAct loop · tool dispatch · decision validation
│   │   ├── tools.py        4 evidence-gathering tools (price · peers · SEC · options)
│   │   ├── llm.py          Ollama client · decide_next_action · Langfuse tracing
│   │   ├── config.py       environment-driven configuration
│   │   ├── Dockerfile
│   │   └── Dockerfile.dbt
│   │
│   └── api/                FastAPI · REST endpoints + /ask (NL query)
│       └── main.py
│
├── dbt/
│   └── models/
│       ├── staging/        stg_silver_bars.sql
│       └── gold/           ohlcv_1min · volume_baseline · volume_anomalies
│                           price_anomalies · sector_summary · anomaly_feed
│
└── infra/
    ├── postgres/init.sql   TimescaleDB schema (hypertables, Gold models)
    ├── prometheus/
    └── grafana/provisioning/
```

---

## Resource Usage

| Service | RAM |
|---|---|
| Kafka | ~400 MB |
| TimescaleDB | ~300 MB |
| Ollama + Qwen2.5 | ~600 MB |
| Langfuse + Postgres | ~500 MB |
| Grafana + Prometheus | ~250 MB |
| MinIO | ~200 MB |
| Redis | ~100 MB |
| Python services (×5) | ~500 MB |
| Kafka UI + Flower | ~200 MB |
| **Total** | **~3.1 GB** |

To free ~400 MB, stop non-essential monitoring UIs — the pipeline continues unaffected:
```bash
docker compose stop kafka-ui flower
```

---

## Startup Timeline

| Elapsed | Event |
|---|---|
| 0:00 | Infrastructure boots — Kafka, MinIO, TimescaleDB, Redis |
| 3:00 | Ollama model ready |
| 4:00 | Producer begins polling yfinance |
| 5:00 | Consumer writes first Bronze / Silver files |
| 6:00 | First dbt run — Gold tables created |
| 7:00 | Scheduler checks anomaly_feed |
| 8:00 | First Celery tasks dispatched (if anomalies detected) |
| 10:00 | First investigation reports visible in Grafana |

---

## Troubleshooting

**Kafka fails to start**
```bash
docker compose down -v && ./start.sh
```

**Ollama model missing**
```bash
docker exec -it stock-ollama ollama pull qwen2.5:0.5b
```

**No data in Grafana**
- Confirm market hours: 9:30 AM – 4:00 PM ET
- `docker logs stock-producer` — should show symbols being fetched every 60s
- `docker logs stock-dbt` — should show successful dbt runs every 5 min

**Agent not investigating**
- `docker logs stock-agent-scheduler` — check anomalies being dispatched
- `docker logs stock-celery-worker` — check task execution
- Thresholds in `.env` may be too high — try lowering `VOLUME_ZSCORE_THRESHOLD` to 2.0

**ReAct Override Rate too high (>50%)**
- The model is struggling to follow the decision format
- Consider switching to a larger model: `OLLAMA_MODEL=qwen2.5:1.5b` in `.env`
- Restart: `docker compose restart celery-worker agent-scheduler`

---

## Stopping

```bash
./stop.sh              # stop containers, keep data volumes
docker compose down -v # stop and delete all data
```

---

*Built with Kafka · dbt · TimescaleDB · Celery · Ollama · Langfuse · Grafana · FastAPI*
