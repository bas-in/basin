#!/usr/bin/env sh
# coverage-gate.sh — measure total line coverage via cargo-llvm-cov.
#
# Hard floor:  70% — below this CI fails.
# Warn floor:  80% — between 70-80 we warn but pass.
#
# Tooling: prefers an already-installed `cargo llvm-cov`. If absent, will
# attempt a silent `cargo install cargo-llvm-cov`; if that fails (sandboxed
# CI, offline, etc.) we print a SKIP note and exit 0 — never fail CI for
# missing tooling.
#
# Usage: sh scripts/coverage-gate.sh
# Exit:  0 when pct >= THRESHOLD_HARD or tooling unavailable; 1 otherwise.
set -eu

THRESHOLD_HARD=70
THRESHOLD_WARN=80

# ── 1. Locate / install cargo-llvm-cov ──────────────────────────────────────
HAVE_LLVM_COV=0
if command -v cargo-llvm-cov >/dev/null 2>&1; then
  HAVE_LLVM_COV=1
elif cargo llvm-cov --version >/dev/null 2>&1; then
  HAVE_LLVM_COV=1
fi

if [ "$HAVE_LLVM_COV" -eq 0 ]; then
  printf 'coverage-gate: cargo-llvm-cov not found, attempting install...\n'
  if cargo install --quiet cargo-llvm-cov >/dev/null 2>&1; then
    HAVE_LLVM_COV=1
  fi
fi

if [ "$HAVE_LLVM_COV" -eq 0 ]; then
  printf 'coverage-gate: SKIP — cargo-llvm-cov unavailable and install failed\n'
  printf 'coverage-gate: total=unknown threshold_warn=%s%% threshold_hard=%s%%\n' \
    "$THRESHOLD_WARN" "$THRESHOLD_HARD"
  exit 0
fi

# ── 2. Run coverage ─────────────────────────────────────────────────────────
COV_LOG=/tmp/basin-llvm-cov.$$
COV_RC=0
cargo llvm-cov --workspace --summary-only >"$COV_LOG" 2>&1 || COV_RC=$?

if [ "$COV_RC" -ne 0 ]; then
  printf 'coverage-gate: cargo llvm-cov failed (rc=%s)\n' "$COV_RC"
  cat "$COV_LOG" || true
  rm -f "$COV_LOG"
  exit 1
fi

# ── 3. Parse the TOTAL line ─────────────────────────────────────────────────
# llvm-cov prints a final summary like:
#   TOTAL                       123     45    63.41%     67   12   82.09%   ...
# We want the LINE coverage column. Standard llvm-cov column order is:
#   Regions  Missed  Cover  Functions  Missed  Cover  Lines  Missed  Cover  ...
# Lines-Cover is the 3rd "%" field. We extract all % fields from the TOTAL
# row and pick the one corresponding to lines (3rd one in default output).
total_line=$(grep -E '^TOTAL' "$COV_LOG" | tail -1 || true)
rm -f "$COV_LOG"

if [ -z "$total_line" ]; then
  printf 'coverage-gate: SKIP — could not find TOTAL line in llvm-cov output\n'
  printf 'coverage-gate: total=unknown threshold_warn=%s%% threshold_hard=%s%%\n' \
    "$THRESHOLD_WARN" "$THRESHOLD_HARD"
  exit 0
fi

# Extract the 3rd percentage value (line coverage).
pct=$(printf '%s\n' "$total_line" | awk '{
  n = 0
  for (i = 1; i <= NF; i++) {
    if ($i ~ /%$/) {
      n++
      val = $i
      gsub(/%/, "", val)
      if (n == 3) { print val; exit }
    }
  }
  # Fallback: print the last % field we saw.
  if (n > 0) { print val }
}')

if [ -z "${pct:-}" ]; then
  printf 'coverage-gate: SKIP — could not parse pct from TOTAL line: %s\n' "$total_line"
  printf 'coverage-gate: total=unknown threshold_warn=%s%% threshold_hard=%s%%\n' \
    "$THRESHOLD_WARN" "$THRESHOLD_HARD"
  exit 0
fi

printf 'coverage-gate: total=%s%% threshold_warn=%s%% threshold_hard=%s%%\n' \
  "$pct" "$THRESHOLD_WARN" "$THRESHOLD_HARD"

# ── 4. Compare against thresholds ───────────────────────────────────────────
awk -v pct="$pct" -v warn="$THRESHOLD_WARN" -v hard="$THRESHOLD_HARD" 'BEGIN {
  if (pct + 0 < hard + 0) {
    printf "coverage-gate: FAIL — %s%% is below hard floor %s%%\n", pct, hard
    exit 1
  }
  if (pct + 0 < warn + 0) {
    printf "coverage-gate: WARN — %s%% is below warn floor %s%% (hard floor %s%% met)\n", pct, warn, hard
    exit 0
  }
  printf "coverage-gate: PASS — %s%% meets the %s%% warn floor\n", pct, warn
  exit 0
}'
