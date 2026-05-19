#!/usr/bin/env sh
# test-coverage-gate.sh — fail CI when a cmd_*.go lands without a
# matching cmd_*_test.go file.
#
# Scans the repo root for every cmd_*.go (excluding *_test.go files
# themselves) and asserts that a corresponding cmd_*_test.go exists.
# Prints the full list of missing files and exits 1 if any are absent.
#
# Usage: sh scripts/test-coverage-gate.sh
# Exit: 0 when every cmd_*.go has a matching test file, 1 otherwise.
set -eu

# Change to the repo root (one level up from this script's directory).
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/.."

missing=0

for src in cmd_*.go; do
  # Skip test files themselves.
  case "$src" in
    *_test.go) continue ;;
  esac

  # Derive the expected test file name.
  base="${src%.go}"
  expected="${base}_test.go"

  if [ ! -f "$expected" ]; then
    printf 'test-coverage-gate: MISSING %s (required for %s)\n' "$expected" "$src"
    missing=$((missing + 1))
  fi
done

if [ "$missing" -gt 0 ]; then
  printf 'test-coverage-gate: FAIL — %d cmd_*.go file(s) lack a matching test file\n' "$missing"
  exit 1
fi

printf 'test-coverage-gate: PASS — every cmd_*.go has a matching test file\n'
