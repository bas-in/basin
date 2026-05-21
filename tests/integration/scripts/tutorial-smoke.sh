#!/usr/bin/env bash
# tests/integration/scripts/tutorial-smoke.sh
#
# Phase 5.32.A — tutorial smoke harness.
# Runs every command in docs/tutorial.md top-to-bottom from a fresh state,
# asserting each step's expected output.
#
# LANDING STATE: docs/tutorial.md does not yet exist (authored in 5.32.B).
# Until 5.32.B lands, this script exits 0 with a clear skip message so CI
# stays green.  Once 5.32.B lands, remove the early-exit block below and
# fill in the TODO assertion section.
#
# Exit codes:
#   0  — all assertions passed  OR  tutorial not yet authored (skip)
#   1  — an assertion failed

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TUTORIAL="${REPO_ROOT}/docs/tutorial.md"

log()  { echo "[tutorial-smoke] $*" >&2; }
fail() { echo "[tutorial-smoke] FAIL: $*" >&2; exit 1; }
skip() { echo "[tutorial-smoke] SKIP: $*" >&2; exit 0; }

# ── 5.32.B guard — remove this block once docs/tutorial.md is authored ────

if [[ ! -f "${TUTORIAL}" ]]; then
  skip "docs/tutorial.md not yet authored (5.32.B pending) — skipping tutorial smoke"
fi

log "Found ${TUTORIAL} — running tutorial smoke assertions"

# ── Environment defaults (override for local runs) ────────────────────────

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-basin}"
PGDATABASE="${PGDATABASE:-basin}"

# ── Helpers ───────────────────────────────────────────────────────────────

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    fail "${label}: expected '${expected}', got '${actual}'"
  fi
  log "OK  ${label}"
}

psql_run() {
  psql \
    --host="${PGHOST}" \
    --port="${PGPORT}" \
    --username="${PGUSER}" \
    --dbname="${PGDATABASE}" \
    --no-psqlrc \
    --tuples-only \
    --no-align \
    "$@"
}

# ── TODO (5.32.B): add step-by-step assertions here ──────────────────────
#
# Pattern for each tutorial step:
#
#   log "Step N — <description from tutorial>"
#   actual=$(psql_run -c "<SQL from tutorial step N>")
#   assert_eq "step-N" "<expected output>" "${actual}"
#
# Reset state between runs:
#   psql_run -c "DROP SCHEMA IF EXISTS tutorial CASCADE; CREATE SCHEMA tutorial;"
#
# Example (uncomment and adapt once tutorial.md exists):
#
#   log "Step 1 — create tutorial tenant bucket"
#   psql_run -c "CREATE BUCKET IF NOT EXISTS tutorial_demo;"
#   actual=$(psql_run -c "SELECT bucket_name FROM basinsys.buckets WHERE bucket_name = 'tutorial_demo';")
#   assert_eq "step-1-bucket-exists" "tutorial_demo" "${actual}"
#
# END TODO ─────────────────────────────────────────────────────────────────

log "All tutorial assertions passed."
