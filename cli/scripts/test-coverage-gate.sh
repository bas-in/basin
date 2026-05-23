#!/usr/bin/env sh
# test-coverage-gate.sh — assert every src/commands/*.rs has an inline
# `#[cfg(test)] mod tests` block.
#
# Excluded: src/commands/mod.rs and src/commands/help.rs (no business logic).
#
# Usage: sh scripts/test-coverage-gate.sh
# Exit:  0 when every eligible file has an inline test module, 1 otherwise.
set -eu

# Change to the repo root (one level up from this script's directory).
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/.."

missing=""
checked=0

for f in src/commands/*.rs; do
  [ -f "$f" ] || continue

  base=$(basename "$f")
  case "$base" in
    mod.rs|help.rs) continue ;;
  esac

  checked=$((checked + 1))

  has_cfg=0
  has_mod=0
  if grep -q '#\[cfg(test)\]' "$f"; then
    has_cfg=1
  fi
  if grep -q 'mod tests' "$f"; then
    has_mod=1
  fi

  if [ "$has_cfg" -eq 0 ] || [ "$has_mod" -eq 0 ]; then
    if [ -z "$missing" ]; then
      missing="$f"
    else
      missing="$missing
$f"
    fi
  fi
done

if [ -n "$missing" ]; then
  printf 'test-coverage-gate: FAIL — the following command files lack an inline #[cfg(test)] mod tests block:\n'
  printf '%s\n' "$missing"
  exit 1
fi

printf 'test-coverage-gate: all %d commands have inline test modules\n' "$checked"
exit 0
