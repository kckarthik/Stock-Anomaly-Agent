#!/usr/bin/env bash
# ================================================================
#  Stock Market Anomaly Investigation Agent — Start Script
# ================================================================
set -euo pipefail

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'

echo -e "${CYAN}${BOLD}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   📈  Stock Market Anomaly Investigation Agent           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

if ! docker info >/dev/null 2>&1; then
  echo -e "${RED}❌ Docker is not running.${NC}"; exit 1
fi

# ── Remove obsolete version attribute warning ─────────────────────
# (Docker Compose v2 ignores it but warns — strip it silently)

echo -e "${YELLOW}[1/3] Starting all infrastructure containers...${NC}"
echo "      (Kafka, MinIO, TimescaleDB, Redis, Prometheus, Grafana,"
echo "       Ollama, Langfuse, Kafka-UI, Flower)"
echo ""

# Start ALL infrastructure in one call — avoids re-evaluation issues
# Service dependencies in docker-compose handle ordering automatically
docker compose up -d \
  kafka \
  minio \
  timescaledb \
  redis \
  prometheus \
  grafana \
  ollama \
  langfuse-postgres \
  langfuse \
  kafka-ui \
  flower

echo ""
echo -e "${YELLOW}[2/3] Waiting for core services to be healthy...${NC}"
echo "      Kafka needs ~30s, Langfuse needs ~90s (DB migrations)"
echo "      Watching..."

# Wait for the critical services explicitly
wait_healthy() {
  local name=$1
  local max=$2
  local i=0
  while [ $i -lt $max ]; do
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$name" 2>/dev/null || echo "none")
    if [ "$STATUS" = "healthy" ]; then
      echo "      ✅ $name healthy"
      return 0
    fi
    sleep 5
    i=$((i+5))
    echo "      ⏳ $name: $STATUS (${i}s / ${max}s)"
  done
  echo "      ⚠️  $name not healthy after ${max}s — continuing anyway"
  return 0  # non-fatal
}

wait_healthy stock-kafka        120
wait_healthy stock-minio        30
wait_healthy stock-timescaledb  30
wait_healthy stock-redis        20
wait_healthy stock-ollama       120

echo ""
echo -e "${YELLOW}      Setting up MinIO buckets...${NC}"
docker compose up minio-setup --no-log-prefix 2>/dev/null || true

echo ""
echo -e "${YELLOW}      Pulling Qwen2.5:0.5b model (skipped if already downloaded)...${NC}"
docker compose up ollama-setup --no-log-prefix 2>/dev/null || true

echo ""
echo -e "${YELLOW}[3/3] Starting pipeline + agent services...${NC}"
docker compose up -d producer consumer dbt-runner celery-worker agent-scheduler api

sleep 3

echo -e "${GREEN}${BOLD}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  ✅  Stack is running!                                       ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  📊 Grafana        →  http://localhost:3001  admin/admin     ║"
echo "║  🔬 Langfuse       →  http://localhost:3002  (see below)     ║"
echo "║  🌿 Flower         →  http://localhost:5555                  ║"
echo "║  🗄  MinIO          →  http://localhost:9001  minioadmin      ║"
echo "║  ⚡ Kafka UI       →  http://localhost:8090                  ║"
echo "║  🌐 API Docs       →  http://localhost:8080/docs             ║"
echo "║  🔥 Prometheus     →  http://localhost:9090                  ║"
echo "║  🤖 Ollama         →  http://localhost:11434                 ║"
echo "║                                                              ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  🔬 LANGFUSE SETUP (one-time, do after ~90s):                ║"
echo "║                                                              ║"
echo "║  1. Open http://localhost:3002                               ║"
echo "║  2. Sign Up — any email/password (fully local, no internet)  ║"
echo "║  3. Create project: stock-agent                              ║"
echo "║  4. Settings → API Keys → Create new key pair                ║"
echo "║  5. Edit .env — paste your real keys:                        ║"
echo "║       LANGFUSE_PUBLIC_KEY=pk-lf-xxxxx                        ║"
echo "║       LANGFUSE_SECRET_KEY=sk-lf-xxxxx                        ║"
echo "║  6. docker compose restart celery-worker agent-scheduler     ║"
echo "║                                                              ║"
echo "║  Until step 6: traces stored in TimescaleDB (always works).  ║"
echo "║                                                              ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Useful logs:                                                ║"
echo "║    docker logs stock-langfuse -f                             ║"
echo "║    docker logs stock-agent-scheduler -f                      ║"
echo "║    docker logs stock-celery-worker -f                        ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
