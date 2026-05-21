#!/usr/bin/env bash
# dev/scripts/smoke.sh — quick psql round-trip checks against basin-server.
#
# Runs ~10 SELECT/INSERT/UPDATE statements via psql to confirm basic function.
# Exits 0 on success, 1 on any failure.
#
# Usage:
#   bash dev/scripts/smoke.sh [--host H] [--port P] [--user U]
#
# Defaults: host=127.0.0.1, port=$BASIN_PORT_BASE (5533), user=alice

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

HOST="127.0.0.1"
PORT="${BASIN_PORT_BASE:-5533}"
USER="alice"

while [[ $# -gt 0 ]]; do
  case $1 in
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --user) USER="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

PSQL="psql -h $HOST -p $PORT -U $USER -d postgres --no-psqlrc -v ON_ERROR_STOP=1"

PASS=0
FAIL=0

run_check() {
  local label="$1"
  local sql="$2"
  local expected_pattern="${3:-}"   # optional grep pattern to validate output

  local output
  if output=$($PSQL -c "$sql" 2>&1); then
    if [[ -n "$expected_pattern" ]] && ! echo "$output" | grep -q "$expected_pattern"; then
      echo "  [FAIL] $label — output did not match '$expected_pattern'"
      echo "         output: $output"
      FAIL=$((FAIL + 1))
    else
      echo "  [PASS] $label"
      PASS=$((PASS + 1))
    fi
  else
    echo "  [FAIL] $label — psql error:"
    echo "         $output"
    FAIL=$((FAIL + 1))
  fi
}

echo "==> Smoke tests against basin-server at $HOST:$PORT (user=$USER)"
echo ""

# 1. Basic connectivity
run_check "SELECT 1" "SELECT 1 AS one;" "one"

# 2. Server version string (Basin reports as PostgreSQL-compatible)
run_check "SELECT version()" "SELECT version();" ""

# 3. Create a table
run_check "CREATE TABLE smoke_test" \
  "CREATE TABLE IF NOT EXISTS smoke_test (id BIGINT, label TEXT, value DOUBLE PRECISION);" ""

# 4. INSERT a row
run_check "INSERT single row" \
  "INSERT INTO smoke_test VALUES (1, 'alpha', 3.14);" ""

# 5. INSERT multiple rows
run_check "INSERT batch" \
  "INSERT INTO smoke_test VALUES (2, 'beta', 2.72), (3, 'gamma', 1.41), (4, 'delta', 1.73);" ""

# 6. Point SELECT (equality predicate)
run_check "Point SELECT id=1" \
  "SELECT label FROM smoke_test WHERE id = 1;" "alpha"

# 7. Range scan
run_check "Range scan id 2..4" \
  "SELECT COUNT(*) FROM smoke_test WHERE id BETWEEN 2 AND 4;" "3"

# 8. Aggregate
run_check "Aggregate SUM" \
  "SELECT ROUND(SUM(value)::numeric, 2) AS total FROM smoke_test;" ""

# 9. UPDATE
run_check "UPDATE row" \
  "UPDATE smoke_test SET label = 'alpha-updated' WHERE id = 1;" ""

# 10. Confirm UPDATE took effect
run_check "SELECT after UPDATE" \
  "SELECT label FROM smoke_test WHERE id = 1;" "alpha-updated"

# 11. DROP table (cleanup)
run_check "DROP TABLE smoke_test" \
  "DROP TABLE IF EXISTS smoke_test;" ""

echo ""
if [[ $FAIL -eq 0 ]]; then
  echo "==> Smoke PASSED ($PASS/$((PASS + FAIL)))"
  exit 0
else
  echo "==> Smoke FAILED ($FAIL failures out of $((PASS + FAIL)))"
  exit 1
fi
