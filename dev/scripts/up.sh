#!/usr/bin/env bash
# dev/scripts/up.sh — bring the Basin dev-stack up and wait for health.
#
# Usage:
#   bash dev/scripts/up.sh [--replicas N] [--no-build]
#
# Options:
#   --replicas N   start N basin-server replicas (1 or 2; default 1)
#   --no-build     skip docker compose build (use cached images)
#
# Environment overrides (export before running, or set in dev/.env):
#   BASIN_SERVER_BYO_BINARY   path to a pre-built basin-server binary.
#                             When set, the docker build stage is skipped and
#                             the binary is bind-mounted into the container.
#                             See dev/README.md for the full BYO pattern.
#   POSTGRES_PORT             host port for catalog Postgres (default 5532)
#   MINIO_API_PORT            host port for MinIO API (default 9100)
#   BASIN_PORT_BASE           base host port for basin-server (default 5533)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$DEV_DIR/.." && pwd)"

REPLICAS=1
BUILD_FLAG="--build"

while [[ $# -gt 0 ]]; do
  case $1 in
    --replicas) REPLICAS="$2"; shift 2 ;;
    --no-build) BUILD_FLAG=""; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

POSTGRES_PORT="${POSTGRES_PORT:-5532}"
MINIO_API_PORT="${MINIO_API_PORT:-9100}"
BASIN_PORT_BASE="${BASIN_PORT_BASE:-5533}"

cd "$DEV_DIR"

echo "==> Basin dev-stack: bring-up (replicas=$REPLICAS)"

# ── Compose profiles ──────────────────────────────────────────────────────────
PROFILES=""
if [[ "$REPLICAS" -ge 2 ]]; then
  PROFILES="--profile replica-1"
fi

# ── Build + start ──────────────────────────────────────────────────────────────
# shellcheck disable=SC2086
docker compose $PROFILES up -d $BUILD_FLAG

# ── Wait: catalog Postgres ────────────────────────────────────────────────────
echo "--> Waiting for catalog Postgres on port $POSTGRES_PORT..."
TRIES=0
until docker compose exec -T catalog-pg pg_isready -U basin -d basin -q 2>/dev/null; do
  TRIES=$((TRIES + 1))
  if [[ $TRIES -ge 40 ]]; then
    echo "ERROR: catalog-pg did not become ready within 80s" >&2
    docker compose logs catalog-pg | tail -20 >&2
    exit 1
  fi
  sleep 2
done
echo "    catalog-pg ready."

# ── Wait: MinIO ───────────────────────────────────────────────────────────────
echo "--> Waiting for MinIO on port $MINIO_API_PORT..."
TRIES=0
until curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; do
  TRIES=$((TRIES + 1))
  if [[ $TRIES -ge 30 ]]; then
    echo "ERROR: minio did not become healthy within 60s" >&2
    docker compose logs minio | tail -20 >&2
    exit 1
  fi
  sleep 2
done
echo "    minio ready."

# ── Wait: minio-init (bucket creation) ───────────────────────────────────────
echo "--> Waiting for minio-init (bucket bootstrap)..."
TRIES=0
until [[ "$(docker compose ps minio-init --format '{{.Status}}' 2>/dev/null)" =~ Exited ]]; do
  TRIES=$((TRIES + 1))
  if [[ $TRIES -ge 20 ]]; then
    echo "ERROR: minio-init did not complete within 40s" >&2
    docker compose logs minio-init | tail -20 >&2
    exit 1
  fi
  sleep 2
done
# Check it exited cleanly (exit code 0).
EXIT_CODE=$(docker compose ps minio-init --format '{{.ExitCode}}' 2>/dev/null || echo "1")
if [[ "$EXIT_CODE" != "0" ]]; then
  echo "ERROR: minio-init exited with code $EXIT_CODE" >&2
  docker compose logs minio-init | tail -20 >&2
  exit 1
fi
echo "    basin-dev bucket created."

# ── Wait: basin-server replica(s) ────────────────────────────────────────────
wait_basin_replica() {
  local name="$1"
  local port="$2"
  echo "--> Waiting for $name on port $port (pgwire)..."
  TRIES=0
  until pg_isready -h 127.0.0.1 -p "$port" -U alice -q 2>/dev/null; do
    TRIES=$((TRIES + 1))
    if [[ $TRIES -ge 40 ]]; then
      echo "ERROR: $name pgwire port $port did not open within 80s" >&2
      docker compose logs "$name" | tail -30 >&2
      exit 1
    fi
    sleep 2
  done
  echo "    $name pgwire ready."
}

wait_basin_replica "basin-server-0" "$BASIN_PORT_BASE"

if [[ "$REPLICAS" -ge 2 ]]; then
  BASIN_PORT_REPLICA1="${BASIN_PORT_REPLICA1:-5534}"
  wait_basin_replica "basin-server-1" "$BASIN_PORT_REPLICA1"
fi

echo ""
echo "==> Dev-stack is UP."
echo "    catalog-pg : postgres://basin:basin@localhost:${POSTGRES_PORT}/basin"
echo "    minio      : http://localhost:${MINIO_API_PORT}  (console: http://localhost:${MINIO_CONSOLE_PORT:-9101})"
echo "    basin[0]   : postgres://alice@localhost:${BASIN_PORT_BASE}/postgres"
if [[ "$REPLICAS" -ge 2 ]]; then
  echo "    basin[1]   : postgres://alice@localhost:${BASIN_PORT_REPLICA1:-5534}/postgres"
fi
echo ""
echo "    Next: bash dev/scripts/smoke.sh"
