#!/usr/bin/env sh
# smoke.sh — build the basin binary and run a quick sanity check.
#
# Exercises:
#   1. go build succeeds.
#   2. --version prints non-empty output.
#   3. help prints something containing "login".
#   4. An unknown subcommand exits non-zero.
#   5. Running without a token exits non-zero with "not signed in" text.
#
# Usage: sh scripts/smoke.sh
# Exit: 0 on all assertions passing, 1 on first failure.
set -eu

BINARY=/tmp/basin-smoke

# ── 1. Build ────────────────────────────────────────────────────────────────
printf 'smoke: building binary... '
go build -o "$BINARY" ./...
printf 'ok\n'

# ── 2. --version prints non-empty output ────────────────────────────────────
printf 'smoke: --version... '
ver_out=$("$BINARY" --version 2>&1 || true)
case "$ver_out" in
  ?*)
    printf 'ok  (%s)\n' "$ver_out"
    ;;
  *)
    printf '\nFAIL: --version produced empty output\n'
    exit 1
    ;;
esac

# ── 3. help contains "login" ─────────────────────────────────────────────────
printf 'smoke: help contains "login"... '
help_out=$("$BINARY" help 2>&1 || true)
case "$help_out" in
  *login*)
    printf 'ok\n'
    ;;
  *)
    printf '\nFAIL: help output does not contain "login"\n'
    printf '      got: %s\n' "$help_out"
    exit 1
    ;;
esac

# ── 4. Unknown subcommand exits non-zero ─────────────────────────────────────
printf 'smoke: unknown subcommand exits non-zero... '
if "$BINARY" this-command-does-not-exist 2>/dev/null; then
  printf '\nFAIL: unknown subcommand exited 0 (want non-zero)\n'
  exit 1
fi
printf 'ok\n'

# ── 5. No-token path exits non-zero with "not signed in" ────────────────────
printf 'smoke: unauthenticated command exits non-zero... '
unauth_out=$(BASIN_TOKEN="" XDG_CONFIG_HOME=/tmp/basin-smoke-noconfig \
  "$BINARY" whoami 2>&1 || true)
case "$unauth_out" in
  *"not signed in"*|*"not authenticated"*|*"not logged in"*|*"login"*)
    printf 'ok\n'
    ;;
  *)
    printf '\nFAIL: expected "not signed in" hint, got: %s\n' "$unauth_out"
    exit 1
    ;;
esac

# ── Done ─────────────────────────────────────────────────────────────────────
printf 'smoke: ok\n'
