#!/usr/bin/env sh
# smoke.sh — build the basin binary and run a quick sanity check.
#
# Exercises:
#   1. cargo build succeeds.
#   2. --version prints non-empty output containing "basin".
#   3. help prints something containing "Usage" / "Commands" / "USAGE".
#   4. An unknown subcommand exits non-zero.
#   5. Running without a token exits non-zero with a "not signed in" hint.
#
# Set SMOKE_RELEASE=1 to build/test the release binary; default is dev.
#
# Usage: sh scripts/smoke.sh
# Exit:  0 on all assertions passing, 1 on first failure.
set -eu

# ── Resolve build profile + binary path ─────────────────────────────────────
PROFILE_FLAG=""
BINARY="target/debug/basin"
if [ "${SMOKE_RELEASE:-0}" = "1" ]; then
  PROFILE_FLAG="--release"
  BINARY="target/release/basin"
fi

# ── 1. Build ────────────────────────────────────────────────────────────────
printf 'smoke: building binary... '
# shellcheck disable=SC2086
cargo build --bin basin --quiet $PROFILE_FLAG
printf 'ok\n'

if [ ! -x "$BINARY" ]; then
  printf 'FAIL: expected binary at %s but it is missing or not executable\n' "$BINARY"
  exit 1
fi

# ── 2. --version prints non-empty output containing "basin" ─────────────────
printf 'smoke: --version... '
ver_out=$("$BINARY" --version 2>&1 || true)
case "$ver_out" in
  *basin*)
    printf 'ok  (%s)\n' "$ver_out"
    ;;
  "")
    printf '\nFAIL: --version produced empty output\n'
    exit 1
    ;;
  *)
    printf '\nFAIL: --version did not mention "basin": %s\n' "$ver_out"
    exit 1
    ;;
esac

# ── 3. help mentions Usage / Commands / USAGE ───────────────────────────────
printf 'smoke: help... '
help_out=$("$BINARY" help 2>&1 || "$BINARY" --help 2>&1 || true)
case "$help_out" in
  *Usage*|*Commands*|*USAGE*)
    printf 'ok\n'
    ;;
  *)
    printf '\nFAIL: help output lacks Usage/Commands/USAGE\n'
    printf '      got: %s\n' "$help_out"
    exit 1
    ;;
esac

# ── 4. Unknown subcommand exits non-zero ────────────────────────────────────
printf 'smoke: unknown subcommand exits non-zero... '
if "$BINARY" nosuch-subcommand >/dev/null 2>&1; then
  printf '\nFAIL: unknown subcommand exited 0 (want non-zero)\n'
  exit 1
fi
printf 'ok\n'

# ── 5. No-token path exits non-zero with a hint ─────────────────────────────
printf 'smoke: unauthenticated whoami exits non-zero... '
NOCFG_DIR=/tmp/basin-smoke-noconfig.$$
mkdir -p "$NOCFG_DIR"
unauth_out=$(BASIN_TOKEN="" XDG_CONFIG_HOME="$NOCFG_DIR" HOME="$NOCFG_DIR" \
  "$BINARY" whoami 2>&1 || true)
unauth_rc=0
BASIN_TOKEN="" XDG_CONFIG_HOME="$NOCFG_DIR" HOME="$NOCFG_DIR" \
  "$BINARY" whoami >/dev/null 2>&1 || unauth_rc=$?
rm -rf "$NOCFG_DIR"

if [ "$unauth_rc" -eq 0 ]; then
  printf '\nFAIL: whoami without token exited 0 (want non-zero)\n'
  exit 1
fi
case "$unauth_out" in
  *token*|*Token*|*"signed in"*|*"not logged in"*|*login*|*Login*)
    printf 'ok\n'
    ;;
  *)
    printf '\nFAIL: expected token/signed-in/login hint, got: %s\n' "$unauth_out"
    exit 1
    ;;
esac

# ── Done ────────────────────────────────────────────────────────────────────
printf 'smoke: all assertions ok\n'
