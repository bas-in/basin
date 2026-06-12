#!/usr/bin/env bash
# Prisma live suite: install deps, generate client (offline), run suite.mjs.
# TAP on stdout; tool noise on stderr.
set -u
cd "$(dirname "$0")"

if [ ! -d node_modules ]; then
  npm install --no-audit --no-fund 1>&2 || { echo "not ok - prisma.install # npm install failed"; exit 0; }
fi
if [ ! -d generated/prisma ]; then
  node node_modules/prisma/build/index.js generate 1>&2 \
    || { echo "not ok - prisma.generate # prisma generate failed"; exit 0; }
fi
exec node suite.mjs
