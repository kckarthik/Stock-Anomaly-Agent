# Troubleshooting Guide

## ❌ "container stock-langfuse is unhealthy"

**Root cause:** Langfuse runs database migrations on first boot. This takes
60-90 seconds, longer than the old healthcheck allowed.

**Fixed in this version.** But if you see it:

```bash
# Option 1 — Wait and retry. Langfuse is slow to start.
docker compose up -d langfuse
sleep 90
docker compose ps stock-langfuse   # should say "healthy"

# Option 2 — Check Langfuse logs
docker logs stock-langfuse --tail 50

# Option 3 — Start without Langfuse entirely (agent still works)
docker compose up -d --scale langfuse=0
```

**Important:** Langfuse is observability-only. The agent runs perfectly
without it. You just won't see LLM traces. The pipeline, reports, and
Grafana dashboards all work independently.

---

## ❌ Ollama model not found

```bash
# Manually pull the model
docker exec -it stock-ollama ollama pull qwen2.5:0.5b

# Check it's there
docker exec -it stock-ollama ollama list
```

---

## ❌ No data in Grafana after 15 minutes

```bash
# Check producer is fetching
docker logs stock-producer --tail 20

# Check consumer is writing
docker logs stock-consumer --tail 20

# Check dbt is running
docker logs stock-dbt --tail 20

# Manually trigger dbt run
docker exec stock-dbt dbt run --profiles-dir /app/dbt --project-dir /app/dbt
```

Note: yfinance only returns data during market hours (9:30 AM – 4:00 PM ET).
Outside those hours, no new data flows — this is expected.

---

## ❌ Agent not investigating anomalies

```bash
# Check scheduler is finding anomalies
docker logs stock-agent-scheduler --tail 30

# Check Celery worker is running
docker logs stock-celery-worker --tail 30

# Check anomaly feed directly
docker exec stock-timescaledb psql -U stock -d stockdb \
  -c "SELECT COUNT(*) FROM gold.anomaly_feed WHERE investigated=FALSE;"

# Lower thresholds in .env to generate more anomalies
VOLUME_ZSCORE_THRESHOLD=1.5
PRICE_CHANGE_THRESHOLD=1.0
docker compose restart agent-scheduler celery-worker
```

---

## ❌ Out of memory

```bash
# Stop non-essential UI services (~600MB savings)
docker compose stop kafka-ui flower langfuse langfuse-postgres

# Pipeline keeps running without these
# Grafana, Prometheus, API all still work
```

---

## ✅ Quick Health Check

```bash
chmod +x scripts/status.sh
./scripts/status.sh
```

---

## ✅ Langfuse Setup (after it's healthy)

```bash
chmod +x scripts/langfuse-setup.sh
./scripts/langfuse-setup.sh
```
