#!/usr/bin/env sh
# coverage-gate.sh — fail CI when total test coverage drops below 80%.
#
# Runs `go test -coverprofile` across all packages, extracts the
# coverage percentage from `go tool cover -func`, and exits non-zero
# when the number is below the threshold.
#
# Usage: sh scripts/coverage-gate.sh
# Exit: 0 when coverage >= threshold, 1 otherwise.
set -eu

THRESHOLD=80
COVER_OUT=/tmp/basin-cover.out

printf 'coverage-gate: running go test -coverprofile...\n'
go test -coverprofile="$COVER_OUT" ./...

# Extract the "total:" line from go tool cover -func output.
# Example line: total:		(statements)	73.4%
total_line=$(go tool cover -func="$COVER_OUT" | tail -1)
printf 'coverage-gate: raw total line: %s\n' "$total_line"

# Pull the percentage value (strips the trailing '%').
pct_raw=$(printf '%s' "$total_line" | awk '{print $NF}')
# Strip the '%' suffix so we can compare numerically.
pct=$(printf '%s' "$pct_raw" | tr -d '%')

printf 'coverage-gate: total coverage = %s%% (threshold = %s%%)\n' "$pct" "$THRESHOLD"

# POSIX sh has no floating-point, so we strip the decimal and compare
# the integer part only. This is conservative: 79.9% still fails the
# 80% gate. Use awk for a proper numeric compare.
awk -v pct="$pct" -v threshold="$THRESHOLD" 'BEGIN {
  if (pct + 0 < threshold + 0) {
    printf "coverage-gate: FAIL — %s%% is below the %s%% threshold\n", pct, threshold
    exit 1
  }
  printf "coverage-gate: PASS — %s%% meets the %s%% threshold\n", pct, threshold
  exit 0
}'
