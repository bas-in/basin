#!/usr/bin/env bash
# CI gate: run tests/integration/tests/pg_dump_harness.rs and refuse to report
# success unless the run is the one we expect.
#
# WHY A WRAPPER AND NOT JUST `cargo test`
# ───────────────────────────────────────
# The workflow used to run:
#
#   cargo test -p basin-integration-tests --test pg_dump_harness -- --ignored
#
# `--ignored` runs ONLY `#[ignore]`d tests. No test in that file is ignored
# (the file's own doc comment said otherwise, which is how it survived), so the
# run reported:
#
#   test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out
#
# — a green check named "pg_dump/psql round-trip" that executed nothing, on
# every push, for as long as the workflow has existed.
#
# Removing `--ignored` is necessary but not sufficient: every slice calls
# `require_env()` and returns early when its database URL is absent, so all
# four still pass by declining. That is the state today, and it is legitimate
# (the slices invoke a `basin-cli dump --url/--table` contract the shipped CLI
# does not offer — see the baseline comment below) — but it must not be
# indistinguishable from real coverage.
#
# So this script pins BOTH numbers:
#
#   * tests executed must be > 0            (catches the --ignored class of bug)
#   * declined slices must equal the        (catches silent drift in either
#     committed baseline below              direction: a slice that starts
#                                            running, or one that stops)
#
# FAIL CLOSED
# ───────────
# Exits non-zero when the harness fails, when zero tests execute, when the
# `test result:` line cannot be found at all (a crash before the summary would
# otherwise leave the grep empty and the guard unreachable), or when the
# declined count drifts. There is no skip path.
#
# Exit codes:
#   0  harness ran, and both counts match what is committed here
#   1  harness failed, or the run did not verify what this gate claims
#
# When a slice starts genuinely running, LOWER the baseline in the same commit.

set -euo pipefail

cd "$(dirname "$0")/.."

# Number of slices expected to decline for want of a database URL.
#
# 4 = all of them. The slices shell out to `basin-cli dump --url <pg-url>
# --table <t>`; the shipped CLI's binary is `basin` (not `basin-cli`), its
# `dump` takes `--project/--format/--file` (no `--url`, no `--table`), and it
# assembles output from the control plane's /v1/projects/{ref}/sql/query rather
# than from a bare pgwire URL. So they could not pass even with every env var
# set. See the coverage-status section of
# tests/integration/tests/pg_dump_harness.rs. Lower this as slices land.
EXPECTED_DECLINED_SLICES="${EXPECTED_DECLINED_SLICES:-4}"

OUT="$(mktemp "${TMPDIR:-/tmp}/basin-pgdump-out.XXXXXX")"
trap 'rm -f "${OUT}"' EXIT

echo "[check-pg-dump-harness] running harness (no --ignored: nothing in it is #[ignore]d)"

# `|| true` so a test failure reaches the reporting below rather than killing
# the script under `set -e` with no diagnostic. The exit status is captured and
# re-asserted afterwards.
set +e
cargo test -p basin-integration-tests --test pg_dump_harness -- --nocapture 2>&1 | tee "${OUT}"
CARGO_STATUS="${PIPESTATUS[0]}"
set -e

echo
echo "[check-pg-dump-harness] ── verdict ──"

if [ "${CARGO_STATUS}" -ne 0 ]; then
  echo "[check-pg-dump-harness] FAIL: cargo test exited ${CARGO_STATUS}." >&2
  echo "  The harness itself failed — see the output above. Do not 'fix' this" >&2
  echo "  by restoring the --ignored flag." >&2
  exit 1
fi

# Every lookup pipeline ends in `|| true` and its result is then tested for
# emptiness. Under `set -e` + `pipefail` a grep that matches nothing would
# otherwise kill the script before the guard that reports it.
SUMMARY="$(grep -E '^test result:' "${OUT}" | tail -1 || true)"
if [ -z "${SUMMARY}" ]; then
  echo "[check-pg-dump-harness] FAIL: no 'test result:' line in the output." >&2
  echo "  The harness produced no summary, so there is nothing to verify and" >&2
  echo "  this gate will not pass on the assumption that it was fine. Usually" >&2
  echo "  a link failure or a crash before any test ran." >&2
  exit 1
fi
echo "[check-pg-dump-harness] ${SUMMARY}"

EXECUTED="$(printf '%s' "${SUMMARY}" | sed -n 's/.*ok\. \([0-9]*\) passed.*/\1/p' || true)"
if [ -z "${EXECUTED}" ]; then
  echo "[check-pg-dump-harness] FAIL: could not parse the passed-count out of:" >&2
  echo "    ${SUMMARY}" >&2
  echo "  Refusing to pass on an unparsed summary." >&2
  exit 1
fi

if [ "${EXECUTED}" -eq 0 ]; then
  echo "[check-pg-dump-harness] FAIL: 0 tests executed." >&2
  echo "  This is the exact failure this gate exists to catch: a green check" >&2
  echo "  that ran nothing. Check for a stray --ignored / --skip / filter" >&2
  echo "  argument, or a test binary that no longer contains any #[test]." >&2
  exit 1
fi

DECLINED="$(grep -c 'pg_dump_harness\] SKIP' "${OUT}" || true)"
DECLINED="${DECLINED:-0}"
echo "[check-pg-dump-harness] ${EXECUTED} test(s) executed; ${DECLINED} declined for a missing database URL"

if [ "${DECLINED}" -ne "${EXPECTED_DECLINED_SLICES}" ]; then
  echo "[check-pg-dump-harness] FAIL: ${DECLINED} slice(s) declined, baseline says ${EXPECTED_DECLINED_SLICES}." >&2
  echo "  If a slice now runs for real, lower EXPECTED_DECLINED_SLICES in this" >&2
  echo "  script in the same commit that made it run. If a slice stopped" >&2
  echo "  running, that is a regression in coverage and the correct outcome is" >&2
  echo "  this red build." >&2
  exit 1
fi

if [ "${DECLINED}" -eq "${EXECUTED}" ]; then
  echo "[check-pg-dump-harness] NOTE: every executed slice declined."
  echo "  This gate is therefore asserting only that the harness compiles, runs"
  echo "  and reports honestly — NOT that pg_dump compatibility works. That is"
  echo "  the committed, intentional state (see the coverage-status section of"
  echo "  tests/integration/tests/pg_dump_harness.rs); it is pinned so it"
  echo "  cannot drift unnoticed, and it is not a claim of coverage."
fi

echo "[check-pg-dump-harness] ok"
