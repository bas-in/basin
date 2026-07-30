#!/usr/bin/env bash
# tests/integration/scripts/tutorial-smoke.sh
#
# Replays the psql-reachable part of docs/tutorial.md against a live Basin and
# asserts each documented result. Run from the Docker Smoke workflow, against the
# container that workflow already started, so the tutorial is checked without
# paying for a second engine build.
#
# SCOPE: tutorial steps 2 and 3 — the pgwire round-trip, the schema, and that
# RLS is actually ENFORCED (an unauthenticated session is admitted to nothing).
# Step 5's per-user reads need a running basin-auth, which the quickstart
# container does not start; see the step-5 note below for why that is named as a
# gap rather than skipped silently.
#
# ── FAIL-CLOSED CONTRACT ───────────────────────────────────────────────────
# There is no skip path and no "nothing to check" path. Every exit is either
# "the tutorial's documented behaviour reproduced" (0) or non-zero with a
# diagnostic naming what was wrong. Specifically:
#
#   * A missing docs/tutorial.md is a FAILURE, not a skip. The file is tracked
#     in git; its absence means the checkout is wrong or the doc was deleted,
#     and either way the correct answer is red.
#   * An unreachable Basin is a FAILURE, not a skip. The previous revision of
#     this script exited 0 whenever docs/tutorial.md was absent, and once the
#     tutorial landed it fell straight through a commented-out TODO block to
#     print "All tutorial assertions passed" having run zero assertions.
#   * Running zero assertions is a FAILURE. The counter below is checked
#     before the success message is allowed to print, so deleting or
#     commenting out the assertion body can never produce a pass.
#   * The tutorial drifting away from this harness is a FAILURE. Each step
#     first asserts that the SQL it is about to run is still literally present
#     in docs/tutorial.md, so a rewritten tutorial cannot leave behind a
#     harness that passes while testing statements nobody documents any more.
#
# Exit codes:
#   0  every assertion passed (and there was at least one)
#   1  an assertion failed, a prerequisite is missing, or too few ran
#
# Environment (defaults match the tutorial's own instructions):
#   PGHOST         default 127.0.0.1
#   PGPORT         default 5432
#   PGUSER         default basin
#   PGDATABASE     default basin
#   SMOKE_TIMEOUT  seconds to wait for Basin to accept connections (default 60)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TUTORIAL="${REPO_ROOT}/docs/tutorial.md"

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-basin}"
PGDATABASE="${PGDATABASE:-basin}"
SMOKE_TIMEOUT="${SMOKE_TIMEOUT:-60}"

ASSERTIONS=0

log()  { echo "[tutorial-smoke] $*" >&2; }
fail() { echo "[tutorial-smoke] FAIL: $*" >&2; exit 1; }

# ── Prerequisites — each missing one is a failure with an actionable message ─

[[ -f "${TUTORIAL}" ]] || fail \
"docs/tutorial.md not found at ${TUTORIAL}. It is tracked in git, so this is a
  broken checkout or a deleted doc — not a reason to pass. Restore the file, or
  delete this harness and its CI job together."

command -v psql >/dev/null 2>&1 || fail \
"psql not found in PATH. In CI this harness runs inside the postgres:16
  sidecar (see .github/workflows/docker-smoke.yml). Locally: install
  postgresql-client."

# ── Helpers ────────────────────────────────────────────────────────────────

psql_run() {
  psql \
    --host="${PGHOST}" \
    --port="${PGPORT}" \
    --username="${PGUSER}" \
    --dbname="${PGDATABASE}" \
    --no-password \
    --no-psqlrc \
    --tuples-only \
    --no-align \
    --quiet \
    "$@"
}

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    fail "${label}: expected '${expected}', got '${actual}'"
  fi
  ASSERTIONS=$((ASSERTIONS + 1))
  log "OK  ${label}"
}

# Anti-drift guard: this harness may only assert on SQL the tutorial still
# documents. `grep -F` (fixed string, no regex) so punctuation in the snippet
# cannot widen the match.
assert_documented() {
  local label="$1" snippet="$2"
  if ! grep -qF -- "${snippet}" "${TUTORIAL}"; then
    fail "${label}: docs/tutorial.md no longer contains this exact text:
    ${snippet}
  The tutorial changed and this harness did not. Update the assertion to match
  the new tutorial text — do not delete the guard."
  fi
  ASSERTIONS=$((ASSERTIONS + 1))
  log "OK  ${label} (still documented in tutorial.md)"
}

# ── Wait for Basin — timing out here is red, not a skip ────────────────────

log "Waiting for Basin at ${PGHOST}:${PGPORT} (timeout ${SMOKE_TIMEOUT}s)..."
deadline=$(( $(date +%s) + SMOKE_TIMEOUT ))
connected=0
while true; do
  if psql_run --command="SELECT 1" >/dev/null 2>&1; then
    connected=1
    break
  fi
  if (( $(date +%s) >= deadline )); then
    break
  fi
  sleep 2
done
(( connected == 1 )) || fail \
"Basin did not accept a connection at ${PGHOST}:${PGPORT} within
  ${SMOKE_TIMEOUT}s. The tutorial's step 1 (\`docker run\`) is part of what is
  under test here — an unreachable server means the tutorial does not work, so
  this is a failure and not a skip."
log "Basin is accepting connections."

# ── Fresh state ────────────────────────────────────────────────────────────

log "Resetting tutorial state"
psql_run --command="DROP TABLE IF EXISTS notes;" >/dev/null
psql_run --command="DROP TABLE IF EXISTS users;" >/dev/null
psql_run --command="DROP TABLE IF EXISTS smoke;" >/dev/null

# ── Step 2 — connect and round-trip ("2. Connect with psql") ───────────────

assert_documented "step-2-documented" "INSERT INTO smoke VALUES (1, 'hello basin');"

log "Step 2 — CREATE TABLE / INSERT / SELECT round-trip"
psql_run --command="CREATE TABLE smoke (id int, name text);" >/dev/null
psql_run --command="INSERT INTO smoke VALUES (1, 'hello basin');" >/dev/null
actual="$(psql_run --command="SELECT name FROM smoke WHERE id = 1;")"
assert_eq "step-2-round-trip" "hello basin" "${actual}"
psql_run --command="DROP TABLE smoke;" >/dev/null

# ── Step 3 — schema + RLS policies ("3. Create a schema and an RLS policy") ─
#
# ORDER MATTERS, and it is the thing the tutorial used to get wrong. Basin has
# NO privileged pgwire bypass: a psql session carries no JWT, so `auth.uid()` is
# NULL, so `USING (user_id = auth.uid())` matches nothing and the session sees
# ZERO rows once RLS is on. So: seed first, assert the rows, then enable RLS and
# assert the lockout. Both halves are asserted, because "0 rows" on its own is
# also what a broken table returns.
#
# Ground truth for the lockout is `rls_with_auth_uid_filters_per_user` in
# tests/integration/tests/auth_rls_uid.rs ("anonymous session must see 0 rows").

assert_documented "step-3-documented-rls-enable" "ALTER TABLE notes ENABLE ROW LEVEL SECURITY;"
assert_documented "step-3-documented-policy"     "CREATE POLICY notes_owner_select ON notes"
assert_documented "step-3-documented-no-bypass"  "There is no privileged bypass."

log "Step 3a — create users + notes and seed, BEFORE enabling RLS"
psql_run <<'SQL' >/dev/null
CREATE TABLE users (
  id           TEXT        NOT NULL PRIMARY KEY,
  email        TEXT        NOT NULL UNIQUE,
  display_name TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE notes (
  id         UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  user_id    TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title      TEXT        NOT NULL,
  body       TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO users (id, email, display_name) VALUES
  ('user_alice', 'alice@example.com', 'Alice'),
  ('user_bob',   'bob@example.com',   'Bob');
INSERT INTO notes (user_id, title, body) VALUES
  ('user_alice', 'First note',  'Hello from Alice'),
  ('user_alice', 'Second note', 'Still Alice'),
  ('user_bob',   'Bob note',    'Only Bob sees this');
SQL

# Positive control: the rows are really there and really readable. Without this
# the zero-rows assertion below would pass just as happily against an empty or
# broken table.
actual="$(psql_run --command="SELECT count(*) FROM notes;")"
assert_eq "step-3a-rows-visible-before-rls" "3" "${actual}"

actual="$(psql_run --command="SELECT title FROM notes ORDER BY title;")"
assert_eq "step-3a-titles-before-rls" "Bob note
First note
Second note" "${actual}"

log "Step 3b — enable RLS and add the four owner policies"
psql_run <<'SQL' >/dev/null
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_owner_select ON notes FOR SELECT USING (user_id = auth.uid());
CREATE POLICY notes_owner_insert ON notes FOR INSERT WITH CHECK (user_id = auth.uid());
CREATE POLICY notes_owner_update ON notes FOR UPDATE USING (user_id = auth.uid());
CREATE POLICY notes_owner_delete ON notes FOR DELETE USING (user_id = auth.uid());
SQL

# THE SECURITY ASSERTION. An unauthenticated session must be admitted to
# nothing. This is the half of per-tenant isolation that is checkable without a
# running basin-auth, and it is the half that fails open if RLS regresses.
actual="$(psql_run --command="SELECT count(*) FROM notes;")"
assert_eq "step-3b-anonymous-locked-out" "0" "${actual}"

# Same query, spelled the other way, so a count-specific shortcut cannot pass it.
actual="$(psql_run --command="SELECT title FROM notes ORDER BY title;")"
assert_eq "step-3b-anonymous-sees-no-titles" "" "${actual}"

# The `users` table has no policy, so it must be unaffected — RLS is per-table,
# and a change that locked out every table would otherwise look like success.
actual="$(psql_run --command="SELECT count(*) FROM users;")"
assert_eq "step-3b-unpoliced-table-unaffected" "2" "${actual}"

# ── Step 5 — the JWT path is NOT asserted here, on purpose ─────────────────
#
# The tutorial's step 5 reads an RLS-protected table as a specific user. That
# requires a verified JWT, which requires basin-auth, which the quickstart
# container this harness runs against does not start (no BASIN_AUTH_ENABLED, no
# JWT secret). Asserting it here would mean either a permanently red gate or a
# skip that reads as a pass — and a skip-that-reads-as-a-pass is exactly the
# defect this harness was rewritten to remove.
#
# So the boundary is drawn explicitly: this harness proves RLS is ENFORCED
# (anonymous is admitted to nothing, above). Proof that RLS is *selective*
# — Alice sees her rows and not Bob's — lives at the engine level in
# `rls_with_auth_uid_filters_per_user` (tests/integration/tests/auth_rls_uid.rs),
# which runs in the ordinary `cargo test` job with real AuthContexts.
#
# To cover the JWT path end-to-end here, docker-smoke.yml would have to start the
# container with basin-auth enabled and mint a token. That is a real gap, and it
# is named rather than papered over.
#
# What must NOT come back: `SET request.jwt.claims`. It does not exist in Basin
# (grep: zero hits across crates/ and services/). The tutorial documented it for
# months as "a development introspection tool" that "always works today".

log "Cleaning up tutorial state"
psql_run --command="DROP TABLE IF EXISTS notes;" >/dev/null
psql_run --command="DROP TABLE IF EXISTS users;" >/dev/null

# ── The gate on the gate ───────────────────────────────────────────────────
# Nothing above prints a success message. This is the only place that can, and
# it refuses when the assertion counter says no work was done.

MIN_ASSERTIONS=9
if (( ASSERTIONS < MIN_ASSERTIONS )); then
  fail "ran ${ASSERTIONS} assertion(s), expected at least ${MIN_ASSERTIONS}.
  Either assertions were removed or the script left a branch early. A pass is
  only allowed to mean 'the assertions ran and held'."
fi

log "PASSED — ${ASSERTIONS} assertions over docs/tutorial.md steps 2 and 3 (see the step-5 note above for what is deliberately not covered here)."
