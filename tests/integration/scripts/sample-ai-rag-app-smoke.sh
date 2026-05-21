#!/usr/bin/env bash
# tests/integration/scripts/sample-ai-rag-app-smoke.sh
#
# Phase 5.32.A — smoke harness for the AI RAG App sample.
# From a clean checkout: npm ci && npm test && npm run build.
#
# LANDING STATE: examples/ai-rag-app/ does not yet exist (scaffolded in
# 5.32.D).  Until 5.32.D lands, this script exits 0 with a clear skip
# message so CI stays green.
#
# Exit codes:
#   0  — build + tests passed  OR  sample app not yet scaffolded (skip)
#   1  — build or tests failed

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APP_DIR="${REPO_ROOT}/examples/ai-rag-app"

log()  { echo "[ai-rag-app-smoke] $*" >&2; }
fail() { echo "[ai-rag-app-smoke] FAIL: $*" >&2; exit 1; }
skip() { echo "[ai-rag-app-smoke] SKIP: $*" >&2; exit 0; }

# ── 5.32.D guard — remove this block once examples/ai-rag-app is scaffolded

if [[ ! -d "${APP_DIR}" ]]; then
  skip "examples/ai-rag-app/ not yet scaffolded (5.32.D pending) — skipping AI RAG App smoke"
fi

log "Found ${APP_DIR} — running AI RAG App smoke"

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

log "AI RAG App smoke passed."
