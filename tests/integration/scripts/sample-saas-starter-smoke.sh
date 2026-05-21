#!/usr/bin/env bash
# tests/integration/scripts/sample-saas-starter-smoke.sh
#
# Phase 5.32.A — smoke harness for the SaaS Starter sample app.
# From a clean checkout: npm ci && npm test && npm run build.
#
# LANDING STATE: examples/saas-starter/ does not yet exist (scaffolded in
# 5.32.C).  Until 5.32.C lands, this script exits 0 with a clear skip
# message so CI stays green.
#
# Exit codes:
#   0  — build + tests passed  OR  sample app not yet scaffolded (skip)
#   1  — build or tests failed

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APP_DIR="${REPO_ROOT}/examples/saas-starter"

log()  { echo "[saas-starter-smoke] $*" >&2; }
fail() { echo "[saas-starter-smoke] FAIL: $*" >&2; exit 1; }
skip() { echo "[saas-starter-smoke] SKIP: $*" >&2; exit 0; }

# ── 5.32.C guard — remove this block once examples/saas-starter is scaffolded

if [[ ! -d "${APP_DIR}" ]]; then
  skip "examples/saas-starter/ not yet scaffolded (5.32.C pending) — skipping SaaS Starter smoke"
fi

log "Found ${APP_DIR} — running SaaS Starter smoke"

# ── Verify prerequisites ──────────────────────────────────────────────────

if ! command -v node &>/dev/null; then
  fail "node not found in PATH — ensure Node.js is installed (see workflow node-setup step)"
fi

if ! command -v npm &>/dev/null; then
  fail "npm not found in PATH"
fi

log "Node $(node --version) / npm $(npm --version)"

# ── Run the smoke sequence ────────────────────────────────────────────────

cd "${APP_DIR}"

log "npm ci"
npm ci

log "npm test"
npm test

log "npm run build"
npm run build

log "SaaS Starter smoke passed."
