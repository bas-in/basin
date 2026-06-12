#!/usr/bin/env bash
# Drizzle live suite: install deps, run suite.mjs (which itself drives
# drizzle-kit generate + migrate). TAP on stdout; tool noise on stderr.
set -u
cd "$(dirname "$0")"

if [ ! -d node_modules ]; then
  npm install --no-audit --no-fund 1>&2 || { echo "not ok - drizzle.install # npm install failed"; exit 0; }
fi
exec node suite.mjs
