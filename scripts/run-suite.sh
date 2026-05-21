#!/usr/bin/env bash
# run-suite.sh — run the full Basin compliance suite, fast first, slow last,
# and print a per-category pass/fail summary.
#
# Usage:
#   scripts/run-suite.sh            # fast tier only (CI default)
#   scripts/run-suite.sh --slow     # also run the #[ignore]-gated slow tier
#   scripts/run-suite.sh --help
#
# See tests/integration/suite/README.md for the category layout.
#
# Requires: cargo. Optional: PG_DIFF_TEST_DSN for the differential category
# (skips cleanly if unset).
set -uo pipefail

PKG="basin-integration-tests"
RUN_SLOW=0

for arg in "$@"; do
  case "$arg" in
    --slow) RUN_SLOW=1 ;;
    -h|--help)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown arg: $arg (try --help)" >&2
      exit 2
      ;;
  esac
done

# cd to repo root (this script lives in scripts/).
cd "$(dirname "$0")/.."

# category name|test target|extra cargo-test args
FAST_CATEGORIES=(
  "sql-compat|sql_support_matrix|"
  "feature-coverage|feature_coverage|"
  "differential-pg|differential_pg|"
  "performance|perf_suite|"
)

SLOW_CATEGORIES=(
  "perf-scaling|perf_suite|-- --ignored"
)

declare -a NAMES=()
declare -a RESULTS=()

run_category() {
  local name="$1" target="$2" extra="$3"
  echo ""
  echo "════════════════════════════════════════════════════════════════"
  echo "  [$name]  cargo test -p $PKG --test $target $extra"
  echo "════════════════════════════════════════════════════════════════"
  # shellcheck disable=SC2086
  if cargo test -p "$PKG" --test "$target" $extra; then
    NAMES+=("$name")
    RESULTS+=("PASS")
  else
    NAMES+=("$name")
    RESULTS+=("FAIL")
  fi
}

for entry in "${FAST_CATEGORIES[@]}"; do
  IFS='|' read -r name target extra <<<"$entry"
  run_category "$name" "$target" "$extra"
done

if [[ "$RUN_SLOW" == "1" ]]; then
  for entry in "${SLOW_CATEGORIES[@]}"; do
    IFS='|' read -r name target extra <<<"$entry"
    run_category "$name" "$target" "$extra"
  done
else
  echo ""
  echo "[run-suite] slow tier skipped (pass --slow to include perf scaling sweeps)"
fi

# ── Per-category summary ──────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  Basin compliance suite — summary"
echo "════════════════════════════════════════════════════════════════"
fail_count=0
for i in "${!NAMES[@]}"; do
  printf "  %-20s %s\n" "${NAMES[$i]}" "${RESULTS[$i]}"
  [[ "${RESULTS[$i]}" == "FAIL" ]] && fail_count=$((fail_count + 1))
done
echo "────────────────────────────────────────────────────────────────"
if [[ "$fail_count" -eq 0 ]]; then
  echo "  ALL ${#NAMES[@]} CATEGORIES PASSED"
  exit 0
else
  echo "  $fail_count/${#NAMES[@]} CATEGORIES FAILED"
  exit 1
fi
