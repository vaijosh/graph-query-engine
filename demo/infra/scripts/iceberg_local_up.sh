#!/usr/bin/env zsh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
COMPOSE_FILE="$ROOT/demo/infra/iceberg/docker-compose.yml"

cd "$ROOT"
docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for Trino to accept queries..."
for i in {1..60}; do
  if docker exec iceberg-trino trino --server http://localhost:8080 --execute "SELECT 1" >/dev/null 2>&1; then
    echo "Iceberg local stack is ready."
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for Trino. Check container logs:"
docker compose -f "$COMPOSE_FILE" logs --tail=100
exit 1

