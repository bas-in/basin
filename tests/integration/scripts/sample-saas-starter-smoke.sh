#!/usr/bin/env bash
# tests/integration/scripts/sample-saas-starter-smoke.sh
#
# Phase 5.32.A — smoke harness for the SaaS Starter sample app.
# From a clean checkout: npm ci && npm test && npm run build.
#
# FAIL CLOSED. There is no skip path: a missing app directory, a missing
# toolchain, a failing test or a failing build are all non-zero exits with a
# diagnostic. `npm test` output is additionally checked for a suite that
# reported zero tests, because "0 total" and "all green" look identical in a
# summary line.
#
# Exit codes:
#   0  — build + tests passed
#   1  — anything else

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
APP_DIR="${REPO_ROOT}/examples/saas-starter"

log()  { echo "[saas-starter-smoke] $*" >&2; }
fail() { echo "[saas-starter-smoke] FAIL: $*" >&2; exit 1; }

# A missing app directory is a FAILURE, not a skip. examples/saas-starter/ is
# tracked in git; its absence means a broken checkout, not "nothing to do". The
# old `skip` here was written when the app was unscaffolded and was never
# removed once it landed — an exit-0 path guarding a condition that can no
# longer legitimately occur.

if [[ ! -d "${APP_DIR}" ]]; then
  fail "examples/saas-starter/ not found at ${APP_DIR}. It is tracked in git, so this
  is a broken checkout — not a reason to pass."
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

# ── npm test, with a guard on the summary ────────────────────────────────
#
# `npm test` exiting 0 is not enough. jest reports "Tests: 0 total" as a
# success, so a testPathPattern that stops matching, a renamed tests/ directory
# or a suite that fails to load all read as green. And this suite's live slices
# are allowed to be *skipped* (BASIN_SKIP_LIVE_TESTS=1) but never to be
# *silently absent* — so the summary is captured and inspected.
log "npm test"
TEST_LOG="$(mktemp -t saas-starter-smoke-test)"
trap 'rm -f "${TEST_LOG}"' EXIT

set +e
npm test 2>&1 | tee "${TEST_LOG}"
TEST_STATUS="${PIPESTATUS[0]}"
set -e

if [[ "${TEST_STATUS}" -ne 0 ]]; then
  fail "npm test exited ${TEST_STATUS} — see output above."
fi

# Runner-agnostic on purpose: the two sample apps use different test runners
# and print different summaries —
#   jest    "Tests:       4 passed, 4 total"
#   vitest  "     Tests  14 passed (14)"
# — so this matches either and extracts the total from "N total" or from the
# trailing "(N)". Anchoring on one runner's format is how a guard silently
# stops guarding when someone swaps runners.
#
# Ends in `|| true` and is then tested for emptiness: under `set -e` +
# `pipefail` a grep that matches nothing would kill the script here, before the
# guard that is supposed to report it.
SUMMARY="$(grep -E '(^|[[:space:]])Tests:?[[:space:]]' "${TEST_LOG}" | tail -1 || true)"
if [[ -z "${SUMMARY}" ]]; then
  fail "npm test produced no test-count summary line ('Tests: ...' / 'Tests ...').
  Nothing can be concluded from this run, so it does not pass. Usually a suite
  that failed to load, or a runner whose summary format changed — in which case
  fix the pattern here rather than dropping the guard."
fi
log "${SUMMARY}"

# jest: "N total". vitest: "(N)".
TOTAL="$(printf '%s' "${SUMMARY}" | sed -n 's/.*[^0-9]\([0-9]\{1,\}\)[[:space:]]*total.*/\1/p' || true)"
if [[ -z "${TOTAL}" ]]; then
  TOTAL="$(printf '%s' "${SUMMARY}" | sed -n 's/.*(\([0-9]\{1,\}\))[^0-9]*$/\1/p' || true)"
fi
if [[ -z "${TOTAL}" ]]; then
  fail "could not parse a test total out of: ${SUMMARY}"
fi
if [[ "${TOTAL}" -eq 0 ]]; then
  fail "npm test ran 0 tests. Both jest and vitest report that as success; this
  script does not. Check the test path pattern in package.json and that tests/
  still contains suites."
fi

log "npm run build"
npm run build

log "SaaS Starter smoke passed."
