#!/usr/bin/env zsh
# True file-based seeding: stage CSV/JSON files to MinIO and load via Trino Hive external tables.
set -euo pipefail

ROOT="/Users/vjoshi/SourceCode/graph-query-engine"
CSV_PATH="${ROOT}/demo/data/aml-demo.csv"
MAX_ROWS="100000"
FILE_FORMAT="csv"
CONTAINER="iceberg-trino"
RAW_CSV_DIR="/tmp/iceberg-csv-gen"
RAW_JSON_DIR="/tmp/iceberg-json-gen"
SEED_SQL="$ROOT/infra/iceberg/sql/seed_aml_from_hive_files.sql"

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
    --format)
      FILE_FORMAT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--csv <path>] [--rows <n>] [--format csv|json]"
      exit 1
      ;;
  esac
done

if [[ "$FILE_FORMAT" != "csv" && "$FILE_FORMAT" != "json" ]]; then
  echo "Unsupported format: $FILE_FORMAT (expected csv or json)"
  exit 1
fi

if [[ ! -f "$CSV_PATH" ]]; then
  echo "CSV not found: $CSV_PATH"
  exit 1
fi

if ! docker exec "$CONTAINER" trino --server http://localhost:8080 --execute "SHOW CATALOGS" 2>/dev/null | grep -q '"hive"'; then
  echo "❌ Trino catalog 'hive' is not available."
  exit 1
fi

if ! docker exec "$CONTAINER" trino --server http://localhost:8080 --execute "SHOW CATALOGS" 2>/dev/null | grep -q '"iceberg"'; then
  echo "❌ Trino catalog 'iceberg' is not available."
  exit 1
fi

echo "🚀 True file-based Iceberg seeding via Hive external tables"
echo "Input CSV: $CSV_PATH"
echo "Max rows: $MAX_ROWS"
echo "File format: $FILE_FORMAT"
echo ""

# 1) Generate per-table seed files
start_gen=$(date +%s.%N)
echo "[1/5] Generating per-table seed files..."
python3 "$ROOT/scripts/generate_iceberg_seed_csv_connector.py" \
  --csv "$CSV_PATH" \
  --rows "$MAX_ROWS" \
  --out-sql /tmp/ignored_seed_sql.sql \
  --out-csvs "$RAW_CSV_DIR"

if [[ "$FILE_FORMAT" == "json" ]]; then
  echo "  -> Converting CSV files to JSONL..."
  python3 "$ROOT/scripts/convert_seed_csv_to_jsonl.py" --in-dir "$RAW_CSV_DIR" --out-dir "$RAW_JSON_DIR"
fi
end_gen=$(date +%s.%N)
gen_time=$(echo "$end_gen - $start_gen" | bc)
echo "  ✓ File generation time: ${gen_time}s"
echo ""

# 2) Stage files to MinIO
start_stage=$(date +%s.%N)
echo "[2/5] Staging files to MinIO object storage..."
if [[ "$FILE_FORMAT" == "csv" ]]; then
  SOURCE_DIR="$RAW_CSV_DIR"
  EXT="csv"
  RAW_BASE="s3://aml/staging/seed_csv"
else
  SOURCE_DIR="$RAW_JSON_DIR"
  EXT="json"
  RAW_BASE="s3://aml/staging/seed_json"
fi

docker run --rm --entrypoint /bin/sh --network iceberg_default -v "$SOURCE_DIR:/work" minio/mc -c '
mc alias set local http://iceberg-minio:9000 minioadmin minioadmin --api s3v4 >/dev/null
mc mb --ignore-existing local/aml >/dev/null
for t in countries banks accounts transfers bank_country account_bank alerts account_country account_alert; do
  mc cp --quiet /work/$t.'"$EXT"' local/aml/staging/seed_'"$FILE_FORMAT"'/$t/data.'"$EXT"'
done
'

end_stage=$(date +%s.%N)
stage_time=$(echo "$end_stage - $start_stage" | bc)
echo "  ✓ MinIO staging time: ${stage_time}s"
echo ""

# 3) Generate SQL for hive external tables -> iceberg
start_sql=$(date +%s.%N)
echo "[3/5] Generating Hive-to-Iceberg load SQL..."
python3 "$ROOT/scripts/generate_iceberg_seed_hive_sql.py" \
  --out "$SEED_SQL" \
  --format "$FILE_FORMAT" \
  --raw-base "$RAW_BASE"
end_sql=$(date +%s.%N)
sql_time=$(echo "$end_sql - $start_sql" | bc)
echo "  ✓ SQL generation time: ${sql_time}s"
echo ""

# 4) Execute load SQL
start_load=$(date +%s.%N)
echo "[4/5] Loading Iceberg tables from Hive external files..."
python3 "$ROOT/scripts/run_trino_sql_chunked.py" \
  --sql "$SEED_SQL" \
  --server "http://localhost:8080" \
  --container "$CONTAINER"
end_load=$(date +%s.%N)
load_time=$(echo "$end_load - $start_load" | bc)
echo "  ✓ Data load time: ${load_time}s"
echo ""

# 5) Validate
start_verify=$(date +%s.%N)
echo "[5/5] Validating row counts..."
raw_count=$(docker exec "$CONTAINER" trino --server http://localhost:8080 --output-format CSV --execute "SELECT count(*) FROM hive.aml_raw.transfers_raw" 2>/dev/null | tr -d '"' | tail -n 1 | tr -d '\r')
iceberg_count=$(docker exec "$CONTAINER" trino --server http://localhost:8080 --output-format CSV --execute "SELECT count(*) FROM iceberg.aml.transfers" 2>/dev/null | tr -d '"' | tail -n 1 | tr -d '\r')
end_verify=$(date +%s.%N)
verify_time=$(echo "$end_verify - $start_verify" | bc)

if [[ -z "$raw_count" || -z "$iceberg_count" ]]; then
  echo "❌ Unable to validate transfer row counts."
  exit 1
fi

if [[ "$raw_count" != "$iceberg_count" ]]; then
  echo "❌ Row count mismatch: hive.aml_raw.transfers_raw=$raw_count, iceberg.aml.transfers=$iceberg_count"
  exit 1
fi

echo "  ✓ Row counts match: $iceberg_count transfers"
echo ""

total_time=$(echo "$end_verify - $start_gen" | bc)
echo "✅ File-based seeding complete"
echo "Performance summary:"
echo "  File generation: ${gen_time}s"
echo "  MinIO staging:   ${stage_time}s"
echo "  SQL generation:  ${sql_time}s"
echo "  Data load:       ${load_time}s"
echo "  Validation:      ${verify_time}s"
echo "  TOTAL:           ${total_time}s"

