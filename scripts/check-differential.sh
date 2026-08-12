#!/usr/bin/env bash
# Differential-conformance gate: run `differential_pg` against a live Postgres
# and compare the failures against a recorded baseline.
#
# WHY THIS EXISTS
# ───────────────
# `differential_pg` runs every query against BOTH Basin and a real PostgreSQL
# and compares the results cell by cell. It is the only instrument in this repo
# that can catch a *wrong answer* rather than a slow one, which makes it the
# governing check for ADR 0030's owned-engine migration — semantic drift is that
# migration's highest-severity risk.
#
# It cannot simply be required to pass: Basin genuinely diverges from Postgres
# on a known set of shapes today. A permanently-red gate teaches everyone to
# ignore it, which is worse than no gate. So the contract is:
#
#   * a failure NOT in the baseline is a REGRESSION           → fail
#   * a baseline entry that now PASSES means the baseline is  → fail
#     stale and must shrink
#   * everything else                                         → pass
#
# The second rule matters as much as the first. A baseline nobody prunes rots
# into a permanent excuse list; forcing it to shrink the moment a bug is fixed
# is what keeps it honest. Fixing a divergence and updating the file are one
# change, not two.
#
# FAIL CLOSED
# ───────────
# Non-zero when: the DSN is unset or unreachable; cargo fails to build; no
# `test result:` line is produced at all; the summary cannot be parsed; or zero
# tests executed. There is no path on which this exits 0 having checked nothing.
#
# The unreachable-DSN check is not paranoia. Every test in `differential_pg`
# returns early — and reports `ok` — when `PG_DIFF_TEST_DSN` is unset. A plain
# zero-test guard sees a full green run and waves it through, which is exactly
# how this suite sat un-run in CI for its whole existence.
#
# Usage:
#   scripts/check-differential.sh [--baseline FILE]
#   scripts/check-differential.sh --selftest
#
# Exit codes: 0 ok · 1 regression or stale baseline · 2 harness/setup failure

set -uo pipefail

BASELINE_DEFAULT="tests/integration/differential-baseline.txt"
BASELINE="$BASELINE_DEFAULT"

die() {
  printf '[check-differential] %s\n' "$1" >&2
  exit "${2:-2}"
}

# ── Parse the cargo test output into a sorted list of failed test names ──────
#
# `cargo test` lists failures twice: once inline as `test NAME ... FAILED` and
# once in the trailing `failures:` block as bare indented names. The trailing
# block is used, because the inline form is interleaved with test stdout and can
# be split across a line boundary under `--nocapture`.
parse_failures() {
  # cargo prints TWO `failures:` blocks: first the per-test stdout, headed by
  # `---- name stdout ----`, then a bare list. Matching exactly four leading
  # spaces followed by an identifier picks up only the bare list; the `----`
  # headers and any test output are excluded by shape rather than by position.
  awk '
    /^failures:$/     { infailures = 1; next }
    /^test result:/   { infailures = 0 }
    infailures && /^    [A-Za-z_][A-Za-z0-9_]*$/ {
      gsub(/^ +/, ""); print
    }
  ' "$1" | sort -u
}

# Total tests that actually executed, summed across binaries.
executed_count() {
  awk '/^test result:/ {
    for (i = 1; i <= NF; i++) {
      if ($i == "passed;") p += $(i-1)
      if ($i == "failed;") f += $(i-1)
    }
  } END { print p + f + 0 }' "$1"
}

# ── Selftest ────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--selftest" ]]; then
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  fails=0
  check() { # name expected actual
    if [[ "$2" == "$3" ]]; then printf '  %-42s rc=%-7s ok\n' "$1" "$3"
    else printf '  %-42s rc=%-7s WANTED %s\n' "$1" "$3" "$2"; fails=1; fi
  }

  cat > "$tmp/out_clean" <<'EOF'
running 3 tests
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
EOF
  cat > "$tmp/out_two" <<'EOF'
running 3 tests

failures:
    diff_alpha
    diff_beta

test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out
EOF
  cat > "$tmp/out_zero" <<'EOF'
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
EOF

  got="$(parse_failures "$tmp/out_two" | tr '\n' ',')"
  check "parses the failures block" "diff_alpha,diff_beta," "$got"
  check "parses a clean run as no failures" "" "$(parse_failures "$tmp/out_clean" | tr '\n' ',')"
  check "counts executed tests" "3" "$(executed_count "$tmp/out_two")"
  check "counts a zero-test run as zero" "0" "$(executed_count "$tmp/out_zero")"

  printf 'diff_alpha\ndiff_beta\n' > "$tmp/base_exact"
  printf 'diff_alpha\n' > "$tmp/base_missing_one"
  printf 'diff_alpha\ndiff_beta\ndiff_gamma\n' > "$tmp/base_stale"

  cmp_sets() { # baseline actualfile -> prints "regressions|fixed"
    local b="$1" a="$2"
    local reg fix
    reg="$(comm -13 <(grep -vE '^\s*(#|$)' "$b" | sort -u) "$a" | wc -l | tr -d ' ')"
    fix="$(comm -23 <(grep -vE '^\s*(#|$)' "$b" | sort -u) "$a" | wc -l | tr -d ' ')"
    printf '%s|%s' "$reg" "$fix"
  }
  parse_failures "$tmp/out_two" > "$tmp/actual"
  check "exact baseline: no regressions, none fixed" "0|0" "$(cmp_sets "$tmp/base_exact" "$tmp/actual")"
  check "new failure counts as a regression"        "1|0" "$(cmp_sets "$tmp/base_missing_one" "$tmp/actual")"
  check "fixed test makes the baseline stale"       "0|1" "$(cmp_sets "$tmp/base_stale" "$tmp/actual")"

  [[ $fails -eq 0 ]] || die "selftest FAILED" 2
  printf '\ncheck-differential: selftest passed — 7 cases, every refusal path fired.\n'
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline) BASELINE="${2:?--baseline needs a path}"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -f "$BASELINE" ]] || die "baseline file not found: $BASELINE"
[[ -n "${PG_DIFF_TEST_DSN:-}" ]] || die "PG_DIFF_TEST_DSN is unset — every differential test would skip and report ok"

command -v psql >/dev/null || die "psql not on PATH; cannot verify the oracle is reachable"
psql "$PG_DIFF_TEST_DSN" -tAc 'select 1' >/dev/null 2>&1 \
  || die "oracle unreachable at PG_DIFF_TEST_DSN — refusing to report a green run that compared nothing"

out="$(mktemp)"
trap 'rm -f "$out"' EXIT
cargo test -p basin-integration-tests --test differential_pg --release --locked >"$out" 2>&1
grep -q '^test result:' "$out" || { cat "$out" >&2; die "no test summary produced — build or link failure"; }

executed="$(executed_count "$out")"
[[ "$executed" -gt 0 ]] || die "zero tests executed"

parse_failures "$out" > "$out.failed"

# A `!name` line declares a KNOWN-FLAKY test: one that passes or fails
# depending on run order. It is excluded from both directions of the
# comparison, because a flaky entry breaks the gate either way — baseline it
# and the gate cries "stale" when it passes; omit it and the gate cries
# "regression" when it fails. Declaring it is the only honest option short of
# fixing or ignoring the test, and each one carries a comment saying why.
grep -E '^!' "$BASELINE" | sed 's/^!//' | sort -u > "$out.flaky"
grep -vE '^\s*(#|!|$)' "$BASELINE" | sort -u > "$out.base"
comm -23 "$out.failed" "$out.flaky" > "$out.failed.stable"
mv "$out.failed.stable" "$out.failed"

regressions="$(comm -13 "$out.base" "$out.failed")"
fixed="$(comm -23 "$out.base" "$out.failed")"
n_known="$(wc -l < "$out.base" | tr -d ' ')"
n_reg="$(printf '%s' "$regressions" | grep -c . || true)"
n_fix="$(printf '%s' "$fixed" | grep -c . || true)"

n_flaky="$(wc -l < "$out.flaky" | tr -d ' ')"
printf '[check-differential] %s executed · %s known divergences · %s declared-flaky (ignored) · %s fixed · %s regressions\n' \
  "$executed" "$n_known" "$n_flaky" "$n_fix" "$n_reg"

rc=0
if [[ "$n_reg" -gt 0 ]]; then
  printf '\n[check-differential] REGRESSION — these diverge from Postgres and are not in the baseline:\n' >&2
  printf '%s\n' "$regressions" | sed 's/^/    /' >&2
  rc=1
fi
if [[ "$n_fix" -gt 0 ]]; then
  printf '\n[check-differential] STALE BASELINE — these now match Postgres and must be removed from\n%s:\n' "$BASELINE" >&2
  printf '%s\n' "$fixed" | sed 's/^/    /' >&2
  printf '\nFixing a divergence and pruning the baseline are one change, not two.\n' >&2
  rc=1
fi

[[ $rc -eq 0 ]] && printf '[check-differential] ok — no regressions, baseline current.\n'
exit $rc
