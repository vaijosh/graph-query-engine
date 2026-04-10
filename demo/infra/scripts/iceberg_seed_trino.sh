#!/usr/bin/env zsh
# iceberg_seed_trino.sh
# ---------------------
# Seeds the AML Iceberg tables.
#
# ⚡ RECOMMENDED (faster): use the Python CSV bulk loader instead:
#
#   python3 demo/infra/scripts/seed_iceberg_from_csv.py --csv data/aml-demo.csv
#
# That script:
#   1. Parses the CSV in Python
#   2. Writes per-table CSV files → uploads to MinIO
#   3. Creates a Hive external CSV table per Iceberg table
#   4. Runs one  INSERT INTO iceberg.aml.<t> SELECT FROM hive_csv_table  per table
#
# This shell script is retained for backwards-compatibility and CI environments
# where the Python trino/boto3 packages are not available.  It generates a SQL
# file from the CSV and feeds it to Trino statement by statement.
#
# Usage:
#   bash iceberg_seed_trino.sh [--csv <path>] [--rows <n>] [--tiny]
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DEFAULT_SEED_SQL="$ROOT/demo/infra/iceberg/sql/seed_aml_demo.sql"
CSV_PATH="${ROOT}/demo/data/aml-demo.csv"
MAX_ROWS="100000"
SEED_SQL="$DEFAULT_SEED_SQL"
GEN_SQL="$ROOT/demo/infra/iceberg/sql/seed_aml_generated.sql"

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
  python3 "$ROOT/demo/infra/scripts/generate_iceberg_seed_sql_from_csv.py" \
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
python3 "$ROOT/demo/infra/scripts/run_trino_sql_chunked.py" \
  --sql "$SEED_SQL" \
  --server "http://localhost:8080" \
  --container "iceberg-trino"

echo "Seed complete."

