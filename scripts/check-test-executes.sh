#!/usr/bin/env bash
# CI gate wrapper: run a `cargo test` invocation and refuse to report success
# unless tests actually executed.
#
# WHY THIS EXISTS
# ───────────────
# `cargo test <filter> -- --ignored` runs ONLY `#[ignore]`d tests matching the
# filter. If the named test is not ignored, the run reports
#
#   test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out
#
# and exits 0. That is a green check that executed nothing, and in the Actions
# UI it is indistinguishable from a check that verified everything it claims to.
# `hypertable-soak.yml`'s ORM-compat step was in exactly that state, nightly,
# for months.
#
# `scripts/check-pg-dump-harness.sh` closed this hole for one harness by also
# pinning a declined-slice count. This script is the general form, for the jobs
# whose requirement is just "the tests this job is named for actually ran": the
# external-tool compat harnesses and the hypertable soak.
#
# It also fails on a runtime SKIP line, because those harnesses return early —
# and pass — when the external binary they drive is missing from PATH. A
# workflow whose install step silently produced nothing usable would otherwise
# be green off a test that ran, printed "SKIP — flyway not on PATH" and
# asserted nothing. Pass --allow-skips only where declining is the committed,
# intentional outcome.
#
# FAIL CLOSED
# ───────────
# Non-zero when: cargo fails; no `test result:` line is produced at all (a link
# failure or a crash before the summary would otherwise leave the grep empty and
# the guard unreachable); a summary cannot be parsed; zero tests executed across
# every test binary in the run; or a SKIP line appears without --allow-skips.
# There is no path on which this exits 0 having checked nothing.
#
# Usage:
#   scripts/check-test-executes.sh <label> [--allow-skips] -- <cargo test args...>
#   scripts/check-test-executes.sh --selftest
#
# Example:
#   scripts/check-test-executes.sh "5.25.B Flyway" -- \
#     -p basin-integration-tests --test migration_tool_flyway -- --ignored --nocapture
#
# Exit codes:
#   0  the run happened and at least one test executed
#   1  it did not, or we cannot tell
#   2  usage error

set -euo pipefail

cd "$(dirname "$0")/.."

# ── The verdict, as a pure function of (captured output, cargo exit status) ──
#
# Split out from the invocation so `--selftest` can drive every refusal path
# with synthetic output and no cargo. A guard whose failure paths are never
# exercised is a guard that has probably stopped failing.
#
#   verdict <label> <allow_skips:0|1> <output-file> <cargo-status>
verdict() {
  local label="$1" allow_skips="$2" out="$3" cargo_status="$4"

  if [ "${cargo_status}" -ne 0 ]; then
    echo "[check-test-executes] ${label}: FAIL — cargo test exited ${cargo_status}." >&2
    return 1
  fi

  # Every lookup ends in `|| true` and is then tested for emptiness: under
  # `set -e` + `pipefail`, a grep matching nothing would kill the script before
  # the guard meant to report it.
  local summaries
  summaries="$(grep -E '^test result:' "${out}" || true)"
  if [ -z "${summaries}" ]; then
    echo "[check-test-executes] ${label}: FAIL — no 'test result:' line in the output." >&2
    echo "  The run produced no summary, so there is nothing to verify and this" >&2
    echo "  gate will not pass on the assumption that it was fine. Usually a link" >&2
    echo "  failure or a crash before any test ran." >&2
    return 1
  fi

  # Sum across every test binary in the run. `--test X` is one binary today,
  # but a future `--tests` would be several, and a per-binary check would then
  # pass on the strength of the first non-empty one.
  local executed=0 n line
  while IFS= read -r line; do
    n="$(printf '%s' "${line}" | sed -n 's/.*[.:] \([0-9]\{1,\}\) passed.*/\1/p' || true)"
    if [ -z "${n}" ]; then
      echo "[check-test-executes] ${label}: FAIL — could not parse a passed-count out of:" >&2
      echo "    ${line}" >&2
      echo "  Refusing to pass on an unparsed summary." >&2
      return 1
    fi
    executed=$(( executed + n ))
  done <<EOF
${summaries}
EOF

  echo "[check-test-executes] ${label}: ${executed} test(s) executed"

  if [ "${executed}" -eq 0 ]; then
    echo "[check-test-executes] ${label}: FAIL — 0 tests executed." >&2
    echo "  This is the exact failure this gate exists to catch: a green check" >&2
    echo "  that ran nothing. The usual cause is \`-- --ignored\` against a test" >&2
    echo "  that carries no #[ignore] (--ignored runs ONLY ignored tests), or a" >&2
    echo "  name filter that no longer matches anything." >&2
    return 1
  fi

  if [ "${allow_skips}" -eq 0 ]; then
    local skips
    skips="$(grep -E 'SKIP' "${out}" || true)"
    if [ -n "${skips}" ]; then
      echo "[check-test-executes] ${label}: FAIL — a slice declined at runtime:" >&2
      printf '%s\n' "${skips}" >&2
      echo "  These harnesses return early, and pass, when the external binary" >&2
      echo "  they drive is absent from PATH. A job whose install step produced" >&2
      echo "  nothing usable would otherwise be green. Fix the install step; do" >&2
      echo "  not add --allow-skips to get to green." >&2
      return 1
    fi
  fi

  echo "[check-test-executes] ${label}: ok"
  return 0
}

# ── Selftest ────────────────────────────────────────────────────────────────
selftest() {
  local tmp pass=0 fail=0
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/basin-cte-selftest.XXXXXX")"
  # shellcheck disable=SC2064  # expand tmp now, not at trap time
  trap "rm -rf '${tmp}'" RETURN

  echo "check-test-executes selftest — verdict() against synthetic runs"
  echo
  printf '  %-42s %-8s %s\n' "CASE" "EXPECT" "VERDICT"
  printf '  %s\n' "----------------------------------------------------------------------"

  # case <name> <expected-rc> <allow_skips> <cargo-status> <output-body>
  case_() {
    local name="$1" want="$2" allow="$3" status="$4" body="$5"
    local f="${tmp}/out" got
    printf '%s\n' "${body}" > "${f}"
    set +e
    verdict "selftest" "${allow}" "${f}" "${status}" >/dev/null 2>&1
    got=$?
    set -e
    if [ "${got}" -eq "${want}" ]; then
      printf '  %-42s rc=%-6s ok\n' "${name}" "${want}"
      pass=$(( pass + 1 ))
    else
      printf '  %-42s rc=%-6s FAILED (got rc=%s)\n' "${name}" "${want}" "${got}"
      fail=$(( fail + 1 ))
    fi
  }

  case_ "happy path (3 passed)"            0 0 0 "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"
  case_ "zero executed (--ignored trap)"   1 0 0 "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out"
  case_ "cargo exited non-zero"            1 0 101 "test result: FAILED. 1 passed; 1 failed; 0 ignored"
  case_ "no summary line at all"           1 0 0 "error: linking with cc failed"
  case_ "empty output"                     1 0 0 ""
  case_ "unparsable summary"               1 0 0 "test result: ok. some passed; 0 failed"
  case_ "runtime SKIP, skips disallowed"   1 0 0 "[flyway] SKIP — flyway not on PATH
test result: ok. 1 passed; 0 failed; 0 ignored"
  case_ "runtime SKIP, skips allowed"      0 1 0 "[flyway] SKIP — flyway not on PATH
test result: ok. 1 passed; 0 failed; 0 ignored"
  case_ "multi-binary, all zero"           1 0 0 "test result: ok. 0 passed; 0 failed; 0 ignored
test result: ok. 0 passed; 0 failed; 0 ignored"
  case_ "multi-binary, one non-zero"       0 0 0 "test result: ok. 0 passed; 0 failed; 0 ignored
test result: ok. 2 passed; 0 failed; 0 ignored"

  echo
  if [ "${fail}" -ne 0 ]; then
    echo "check-test-executes: selftest FAILED — ${fail} of $(( pass + fail )) cases." >&2
    return 1
  fi
  echo "check-test-executes: selftest passed — ${pass} cases, every refusal path fired."
  return 0
}

# ── Argument handling ───────────────────────────────────────────────────────

if [ "${1:-}" = "--selftest" ]; then
  selftest
  exit $?
fi

LABEL="${1:-}"
if [ -z "${LABEL}" ]; then
  echo "check-test-executes: usage: $0 <label> [--allow-skips] -- <cargo test args...>" >&2
  echo "                     or: $0 --selftest" >&2
  exit 2
fi
shift

ALLOW_SKIPS=0
if [ "${1:-}" = "--allow-skips" ]; then
  ALLOW_SKIPS=1
  shift
fi

if [ "${1:-}" != "--" ]; then
  echo "check-test-executes: expected '--' before the cargo test arguments." >&2
  echo "  got: ${1:-<nothing>}" >&2
  exit 2
fi
shift

if [ "$#" -eq 0 ]; then
  echo "check-test-executes [${LABEL}]: no cargo test arguments given." >&2
  echo "  Refusing to run a bare \`cargo test\`, which would test something" >&2
  echo "  other than what this gate is named for." >&2
  exit 2
fi

OUT="$(mktemp "${TMPDIR:-/tmp}/basin-check-test-executes.XXXXXX")"
trap 'rm -f "${OUT}"' EXIT

echo "[check-test-executes] ${LABEL}: cargo test $*"

# Status captured rather than allowed to kill the script, so a test failure
# reaches the reporting below instead of dying under `set -e` with no
# diagnostic.
set +e
cargo test "$@" 2>&1 | tee "${OUT}"
CARGO_STATUS="${PIPESTATUS[0]}"
set -e

echo
echo "[check-test-executes] ${LABEL} ── verdict ──"
verdict "${LABEL}" "${ALLOW_SKIPS}" "${OUT}" "${CARGO_STATUS}"
