#!/usr/bin/env zsh
set -euo pipefail

ROOT="/Users/vjoshi/SourceCode/graph-query-engine"
DEFAULT_SEED_SQL="$ROOT/infra/iceberg/sql/seed_aml_demo.sql"
CSV_PATH="${ROOT}/demo/data/aml-demo.csv"
MAX_ROWS="100000"
SEED_SQL="$DEFAULT_SEED_SQL"
GEN_SQL="$ROOT/infra/iceberg/sql/seed_aml_bulk_generated.sql"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --csv)
      CSV_PATH="$2"
      shift 2
      ;;
    --rows)
      MAX_ROWS="$2"
      shift 2
      ;;
    --tiny)
      CSV_PATH=""
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--csv <path>] [--rows <n>] [--tiny]"
      exit 1
      ;;
  esac
done

if [[ -n "$CSV_PATH" && -f "$CSV_PATH" ]]; then
  echo "Generating Iceberg bulk seed SQL from CSV: $CSV_PATH (rows=$MAX_ROWS)"
  python3 "$ROOT/scripts/generate_iceberg_seed_sql_from_csv.py" \
    --csv "$CSV_PATH" \
    --rows "$MAX_ROWS" \
    --out "$GEN_SQL"
  SEED_SQL="$GEN_SQL"
else
  echo "CSV not found or disabled; using tiny seed: $DEFAULT_SEED_SQL"
fi

if [[ ! -f "$SEED_SQL" ]]; then
  echo "Seed SQL file not found: $SEED_SQL"
  exit 1
fi

echo "Seeding Iceberg tables via Trino..."
python3 "$ROOT/scripts/run_trino_sql_chunked.py" \
  --sql "$SEED_SQL" \
  --server "http://localhost:8080" \
  --container "iceberg-trino"

echo "Seed complete."

