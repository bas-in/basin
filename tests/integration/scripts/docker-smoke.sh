#!/usr/bin/env bash
# tests/integration/scripts/docker-smoke.sh
#
# Phase 5.31.A — pgwire smoke test for the Basin Docker image.
# Run from inside a postgres:16 sidecar container (psql is pre-installed).
#
# Environment variables (set by the GHA workflow, or override locally):
#   PGHOST      — hostname/IP of the Basin container   (default: 127.0.0.1)
#   PGPORT      — pgwire port                          (default: 5432)
#   PGUSER      — Postgres user                        (default: basin)
#   PGDATABASE  — target database                      (default: basin)
#   SMOKE_TIMEOUT — max seconds to wait for Basin to   (default: 60)
#                   accept connections

set -euo pipefail

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-basin}"
PGDATABASE="${PGDATABASE:-basin}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-60}"

# ── helpers ────────────────────────────────────────────────────────────────

log()  { echo "[smoke] $*" >&2; }
fail() { echo "[smoke] FAIL: $*" >&2; exit 1; }

psql_run() {
  psql \
    --host="$PGHOST" \
    --port="$PGPORT" \
    --username="$PGUSER" \
    --dbname="$PGDATABASE" \
    --no-password \
    --tuples-only \
    --no-align \
    "$@"
}

# ── 1. Wait for Basin to accept connections ────────────────────────────────

log "Waiting for Basin at ${PGHOST}:${PGPORT} (timeout ${SMOKE_TIMEOUT}s)..."

deadline=$(( $(date +%s) + SMOKE_TIMEOUT ))
connected=0

while true; do
  if psql_run --command="SELECT 1" >/dev/null 2>&1; then
    connected=1
    break
  fi
  now=$(date +%s)
  if (( now >= deadline )); then
    break
  fi
  log "  ...not ready yet, retrying in 2 s"
  sleep 2
done

if (( connected == 0 )); then
  fail "Basin did not become ready within ${SMOKE_TIMEOUT} seconds."
fi

log "Basin is accepting connections."

# ── 2. Round-trip: CREATE TABLE / INSERT / SELECT ──────────────────────────

log "Creating smoke table..."
psql_run --command="DROP TABLE IF EXISTS smoke;"
psql_run --command="CREATE TABLE smoke(id int, name text);"

log "Inserting test row..."
psql_run --command="INSERT INTO smoke VALUES (1, 'ok');"

log "Running SELECT..."
result=$(psql_run --command="SELECT id, name FROM smoke WHERE id = 1;")

# ── 3. Assert expected output ──────────────────────────────────────────────

expected_id="1"
expected_name="ok"

id_val=$(echo "$result" | awk -F'|' 'NR==1 { gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1); print $1 }')
name_val=$(echo "$result" | awk -F'|' 'NR==1 { gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2 }')

if [[ "$id_val" != "$expected_id" ]]; then
  fail "Expected id='${expected_id}', got '${id_val}'. Full result: ${result}"
fi

if [[ "$name_val" != "$expected_name" ]]; then
  fail "Expected name='${expected_name}', got '${name_val}'. Full result: ${result}"
fi

# ── 4. Cleanup ─────────────────────────────────────────────────────────────

log "Cleaning up smoke table..."
psql_run --command="DROP TABLE smoke;" || true

log "Smoke test PASSED. Basin is healthy."
