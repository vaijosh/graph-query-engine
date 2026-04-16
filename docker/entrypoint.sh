#!/bin/sh
# ─────────────────────────────────────────────────────────────────────────────
# entrypoint.sh — starts the graph-query-engine and uploads default mappings
# ─────────────────────────────────────────────────────────────────────────────
set -e

ENGINE_URL="http://localhost:${SERVER_PORT:-7000}"
MAPPINGS_DIR="/app/mappings"

# ── 1. Start the engine in the background ────────────────────────────────────
echo "[entrypoint] Starting graph-query-engine on port ${SERVER_PORT:-7000} …"
java \
  ${JAVA_OPTS:--Xmx2g -Xms512m} \
  -Dserver.port="${SERVER_PORT:-7000}" \
  -DDB_URL="${DB_URL}" \
  -DDB_USER="${DB_USER}" \
  -DDB_PASSWORD="${DB_PASSWORD}" \
  -DWCOJ_ENABLED="${WCOJ_ENABLED:-true}" \
  -DWCOJ_MAX_EDGES="${WCOJ_MAX_EDGES:-5000000}" \
  -DWCOJ_DISK_QUOTA_MB="${WCOJ_DISK_QUOTA_MB:-2048}" \
  -DWCOJ_INDEX_TTL_SECONDS="${WCOJ_INDEX_TTL_SECONDS:-30}" \
  -jar /app/app.jar &
ENGINE_PID=$!

# ── 2. Wait for /health to return 200 ────────────────────────────────────────
echo "[entrypoint] Waiting for engine to become ready …"
MAX_WAIT=120
waited=0
until curl -sf "${ENGINE_URL}/health" > /dev/null 2>&1; do
  if [ $waited -ge $MAX_WAIT ]; then
    echo "[entrypoint] ERROR: engine did not start within ${MAX_WAIT}s"
    exit 1
  fi
  sleep 2
  waited=$((waited + 2))
done
echo "[entrypoint] Engine ready after ${waited}s."

# ── 3. Upload all mapping files found in /app/mappings/ ──────────────────────
# Mapping ID is derived from the filename (strip .json extension).
# The *last* mapping uploaded becomes active; set ACTIVE_MAPPING env var to
# control which mapping is active by default (defaults to 'aml-h2').
upload_mapping() {
  local file="$1"
  local id
  id=$(basename "$file" .json)
  echo "[entrypoint] Uploading mapping '${id}' from ${file} …"
  result=$(curl -sf -X POST "${ENGINE_URL}/mapping/upload?id=${id}" \
    -F "file=@${file}" 2>&1) || {
    echo "[entrypoint] WARNING: failed to upload ${id}: ${result}"
    return
  }
  echo "[entrypoint] ✓ ${id} — $(echo "${result}" | grep -o '"mappingId":"[^"]*"' | head -1)"
}

for f in "${MAPPINGS_DIR}"/*.json; do
  [ -f "$f" ] && upload_mapping "$f"
done

# ── 4. Activate the requested default mapping ─────────────────────────────────
ACTIVE_MAPPING="${ACTIVE_MAPPING:-aml-h2}"
echo "[entrypoint] Setting active mapping → '${ACTIVE_MAPPING}' …"
curl -sf -X POST "${ENGINE_URL}/mapping/active?id=${ACTIVE_MAPPING}" > /dev/null 2>&1 \
  && echo "[entrypoint] ✓ Active mapping: ${ACTIVE_MAPPING}" \
  || echo "[entrypoint] WARNING: could not activate '${ACTIVE_MAPPING}' (mapping may not exist yet)"

echo "[entrypoint] Setup complete. Engine PID=${ENGINE_PID}"

# ── 5. Wait for engine process (keeps container alive) ───────────────────────
wait $ENGINE_PID

