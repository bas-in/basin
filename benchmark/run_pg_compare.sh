#!/usr/bin/env bash
# benchmark/run_pg_compare.sh
#
# One-command local Basin-vs-Postgres benchmark.
#
# Brings up a throwaway Postgres 18 container on 127.0.0.1:5432 (if Docker is
# present), runs the compare_postgres integration-test suite, and writes
# benchmark/data/compare_postgres.json.
#
# Usage:
#   ./benchmark/run_pg_compare.sh              # auto-detect docker vs native pg
#   SKIP_DOCKER=1 ./benchmark/run_pg_compare.sh   # skip docker, expect a pg on :5432
#
# Idempotency:
#   - Named container "basin_pg_bench" is stopped+removed on EXIT (trap).
#   - --rm on docker run means the container cleans up even if the trap fires late.
#   - Rerunning is safe; a stale container from a prior crash is killed first.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${REPO_ROOT}/benchmark/data"
OUT_JSON="${DATA_DIR}/compare_postgres.json"
CONTAINER_NAME="basin_pg_bench"
PG_PORT=5432
PG_IMAGE="postgres:18"
PG_USER="postgres"
PG_PASSWORD="basinbenchpass"

SKIP_DOCKER="${SKIP_DOCKER:-0}"

# ---- helpers ----------------------------------------------------------------

log() { printf '[bench] %s\n' "$*" >&2; }

wait_for_pg() {
    local max_secs="${1:-60}"
    local elapsed=0
    log "waiting for Postgres on 127.0.0.1:${PG_PORT} (up to ${max_secs}s)..."
    while ! docker exec "${CONTAINER_NAME}" pg_isready -h 127.0.0.1 -U "${PG_USER}" -q 2>/dev/null; do
        if (( elapsed >= max_secs )); then
            log "ERROR: Postgres did not become ready within ${max_secs}s"
            exit 1
        fi
        sleep 1
        (( elapsed++ )) || true
    done
    log "Postgres is ready (${elapsed}s)"
}

# ---- Docker setup -----------------------------------------------------------

DOCKER_USED=0

if [[ "$SKIP_DOCKER" == "1" ]]; then
    log "SKIP_DOCKER=1: assuming a Postgres is already available on :${PG_PORT}"
else
    if ! command -v docker >/dev/null 2>&1; then
        log "WARNING: docker not found on PATH."
        log "  Please start a Postgres 18 server on 127.0.0.1:${PG_PORT} manually, then re-run with:"
        log "    SKIP_DOCKER=1 ./benchmark/run_pg_compare.sh"
        exit 1
    fi

    DOCKER_USED=1

    # Kill any stale container from a prior run so we always start fresh.
    if docker inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
        log "removing stale container '${CONTAINER_NAME}'..."
        docker rm -f "${CONTAINER_NAME}" >/dev/null
    fi

    log "starting ${PG_IMAGE} container '${CONTAINER_NAME}' on port ${PG_PORT}..."
    docker run \
        --rm \
        --detach \
        --name "${CONTAINER_NAME}" \
        -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
        -e POSTGRES_USER="${PG_USER}" \
        -p "127.0.0.1:${PG_PORT}:5432" \
        "${PG_IMAGE}" \
        >/dev/null

    wait_for_pg 90
fi

# ---- Tear-down trap ---------------------------------------------------------
# Runs on EXIT for any reason (success, failure, Ctrl-C).
cleanup() {
    local exit_code=$?
    if [[ "$DOCKER_USED" == "1" ]] && docker inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
        log "stopping container '${CONTAINER_NAME}'..."
        docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
    fi
    exit $exit_code
}
trap cleanup EXIT

# ---- Run the benchmark ------------------------------------------------------

log "running compare_postgres integration tests..."
log "  cargo test -p basin-integration-tests --test compare_postgres"

mkdir -p "${DATA_DIR}"

PGPASSWORD="${PG_PASSWORD}" \
CARGO_BUILD_JOBS=1 \
    cargo test \
        -p basin-integration-tests \
        --test compare_postgres \
        -- \
        --nocapture \
        --test-threads=1 \
    2>&1

# ---- Report -----------------------------------------------------------------

if [[ -f "${OUT_JSON}" ]]; then
    log "results written to ${OUT_JSON}"
    # Pretty-print a quick summary from the JSON if jq is available.
    if command -v jq >/dev/null 2>&1; then
        echo
        echo "=== Results summary ==="
        jq -r '
            .name,
            (.metrics[] |
                "  \(.label): basin=\(.basin | . * 100 | round / 100) \(.unit)  pg=\(.postgres | . * 100 | round / 100) \(.unit)  winner=\(.better)\(if .ratio_text then "  (\(.ratio_text))" else "" end)")
        ' "${OUT_JSON}"
        echo "======================="
    fi
else
    log "WARNING: ${OUT_JSON} was not written — check test output above."
fi
