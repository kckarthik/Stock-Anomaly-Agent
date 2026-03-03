# Stock Market Anomaly Investigation Agent

> A production-grade agentic data engineering pipeline that monitors real stock market data, detects anomalies, and autonomously investigates them using a local LLM — entirely on your machine, with no cloud dependencies and no paid APIs.

---

## Overview

Every 5 minutes, the agent:

1. Reads newly flagged anomalies from the Gold data layer
2. Runs 5 deterministic investigation tools per anomaly — price history, sector peers, SEC filings, options activity
3. Synthesises all evidence with a local LLM (Qwen2.5 0.5b via Ollama)
4. Persists a structured investigation report to TimescaleDB
5. Surfaces everything in Grafana and a REST API

It doesn't just alert — it **investigates**, the way a junior analyst would.

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
 │  Consumer   │──── Bronze ──► MinIO  (raw Parquet, by symbol/date/hour)
 │             │──── Silver ──► MinIO  (OHLCV 1-min bars)
 └──────┬──────┘          └──► TimescaleDB  silver.ohlcv_1min
        │  every 5 min
        ▼
 ┌─────────────┐    Gold layer (TimescaleDB)
 │ dbt Runner  │──► ohlcv_1min        base 1-min OHLCV bars
 │             │──► volume_baseline   20-day avg per symbol/hour
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
          │  HIGH / MEDIUM / LOW queues
          ▼
 ┌──────────────────────────────────┐
 │   Celery Workers  (x2 parallel)  │
 │                                  │
 │   1. get_anomaly_detail          │  ← Gold DB
 │   2. get_price_history           │  ← yfinance  (20-day baseline)
 │   3. get_sector_peers            │  ← Gold DB   (isolated or sector-wide?)
 │   4. get_sec_filings             │  ← SEC EDGAR (Form 4 insider · 8-K events)
 │   5. get_options_data            │  ← yfinance  (put/call ratio)
 │   ────────────────────────────   │
 │   6. Qwen2.5 0.5b  (one call)    │  ← Ollama    (local, offline)
 │   7. write report                │  → TimescaleDB  gold.investigation_reports
 └──────────────────────────────────┘

 Observability
 ├── Langfuse    every LLM call — prompt · response · tokens · latency
 ├── Flower      every Celery task — active · retried · failed
 ├── Prometheus  pipeline metrics from all services
 └── Grafana     live dashboard — price data · anomalies · reports · LLM health
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

Twelve live panels auto-provisioned at startup:

| Panel | Description |
|---|---|
| Last Quote Received | Timestamp of most recent bar |
| Bars Last 5 Min | Live data freshness indicator |
| Investigations Today | Report count for current day |
| Pending Anomalies | Uninvestigated queue depth |
| Latest Agent Reports | Last 20 reports with hypothesis |
| Severity Breakdown | Pie chart — HIGH / MEDIUM / LOW |
| Anomaly Feed | Current queue with scores |
| Quotes Published / min | Producer throughput (Prometheus) |
| Agent Tasks Dispatched | Dispatch rate by severity (Prometheus) |
| LLM Traces | Last 20 calls — model, tokens, latency |
| Avg LLM Latency | Rolling 24-hour average |
| LLM Success Rate | Reliability gauge |

---

## REST API

```bash
# Health
GET  /health

# Reports
GET  /reports
GET  /reports?severity=HIGH
GET  /reports/{symbol}

# Anomaly feed
GET  /anomalies
GET  /anomalies?investigated=false

# Pipeline stats
GET  /stats

# Natural language query
POST /ask
{ "question": "What was the most unusual event today and why?" }
```

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

## Investigation Pipeline

For every anomaly the agent executes these steps in order:

| Step | Tool | Source | Output |
|---|---|---|---|
| 1 | `get_anomaly_detail` | TimescaleDB Gold | Volume z-score, price change, severity score |
| 2 | `get_price_history` | yfinance | 20-day OHLCV baseline, trend direction |
| 3 | `get_sector_peers` | TimescaleDB Gold | Isolated vs sector-wide assessment |
| 4 | `get_sec_filings` | SEC EDGAR | Form 4 insider trades, 8-K material events (last 7 days) |
| 5 | `get_options_data` | yfinance | Put/call ratio, implied volatility |
| 6 | LLM synthesis | Ollama (local) | Structured JSON report |

The LLM is called **exactly once** per investigation, after all evidence is in hand.

**Report schema:**
```json
{
  "severity":           "HIGH | MEDIUM | LOW",
  "hypothesis":         "one sentence on what is happening",
  "evidence_summary":   "2–3 sentences on key findings",
  "conclusion":         "final assessment",
  "confidence":         "HIGH | MEDIUM | LOW",
  "recommended_action": "MONITOR | INVESTIGATE_FURTHER | ESCALATE | NO_ACTION"
}
```

**Retry policy:** up to 3 retries with 30-second backoff — handles Ollama timeouts, DB blips, and SEC EDGAR rate limits.

---

## Anomaly Detection Thresholds

Set in `.env`:

| Parameter | Default | Meaning |
|---|---|---|
| `VOLUME_ZSCORE_THRESHOLD` | 2.5 | Flag if volume exceeds 2.5 standard deviations above the 20-day average |
| `PRICE_CHANGE_THRESHOLD` | 2.0 | Flag if price moves more than 2% within a 5-minute window |

---

## LLM Observability

Langfuse traces every investigation automatically once keys are configured:

```
Trace: investigate — AAPL — 10:32:15                          (total 4.2s)
  ├── tool:get_anomaly_detail      340ms   volume_zscore: 3.8
  ├── tool:get_price_history       510ms   5d_trend: -1.2%  avg_vol: 2.1M
  ├── tool:get_sector_peers        290ms   ISOLATED: peers normal
  ├── tool:get_sec_filings         890ms   Form 4: CFO sold $9.2M
  ├── tool:get_options_data        420ms   put/call 1.8 — elevated
  └── llm:qwen2.5-synthesise      1.4s    severity: HIGH  580 tokens
```

Without Langfuse keys, all traces fall back to `obs.llm_traces` in TimescaleDB — zero config required.

**To enable Langfuse:**
1. Visit http://localhost:3002 → sign up (fully local, no internet)
2. Create a project → Settings → API Keys → generate a key pair
3. Paste the keys into `.env` under `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY`
4. `docker compose restart celery-worker agent-scheduler`

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
│   ├── agent/              orchestration · investigation · LLM
│   │   ├── scheduler.py    reads anomaly_feed · dispatches tasks
│   │   ├── tasks.py        Celery task definitions
│   │   ├── investigator.py investigation runner
│   │   ├── tools.py        5 deterministic tools
│   │   ├── llm.py          Ollama client · Langfuse tracing
│   │   ├── Dockerfile
│   │   └── Dockerfile.dbt
│   │
│   └── api/                FastAPI · REST + /ask endpoint
│       └── main.py
│
├── dbt/
│   └── models/
│       ├── staging/        stg_silver_bars.sql
│       └── gold/           ohlcv_1min · volume_baseline · volume_anomalies
│                           price_anomalies · sector_summary · anomaly_feed
│
└── infra/
    ├── postgres/init.sql   TimescaleDB schema
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

To free ~400 MB, stop non-essential UIs — pipeline continues unaffected:
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
- Thresholds in `.env` may be too high for current conditions — try lowering `VOLUME_ZSCORE_THRESHOLD` to 2.0

---

## Stopping

```bash
./stop.sh              # stop containers, keep data volumes
docker compose down -v # stop and delete all data
```

---

*Built with Kafka · dbt · TimescaleDB · Celery · Ollama · Langfuse · Grafana · FastAPI*
