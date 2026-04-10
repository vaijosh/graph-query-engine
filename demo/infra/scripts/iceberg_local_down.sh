#!/usr/bin/env zsh
set -euo pipefail

ROOT="/Users/vjoshi/SourceCode/graph-query-engine"
COMPOSE_FILE="$ROOT/infra/iceberg/docker-compose.yml"

cd "$ROOT"
docker compose -f "$COMPOSE_FILE" down -v

echo "Iceberg local stack stopped and volumes removed."

