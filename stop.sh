#!/usr/bin/env bash
echo "🛑 Stopping all containers..."
docker compose down
echo "✅ Stopped. Data preserved in Docker volumes."
echo ""
echo "To delete all data: docker compose down -v"
