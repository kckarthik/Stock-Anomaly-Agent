#!/usr/bin/env bash
# Quick status check for all services
echo "=== Stock Agent — Service Status ==="
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
echo ""
echo "=== Recent Agent Reports ==="
docker exec stock-timescaledb psql -U stock -d stockdb -c \
  "SELECT symbol, severity, LEFT(hypothesis,60) AS hypothesis, investigated_at FROM gold.investigation_reports ORDER BY investigated_at DESC LIMIT 5;" 2>/dev/null || echo "No reports yet"
echo ""
echo "=== Pending Anomalies ==="
docker exec stock-timescaledb psql -U stock -d stockdb -c \
  "SELECT symbol, anomaly_type, severity_label, detected_at FROM gold.anomaly_feed WHERE investigated=FALSE ORDER BY severity_score DESC LIMIT 5;" 2>/dev/null || echo "No pending anomalies"
