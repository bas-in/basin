#!/usr/bin/env bash
# TypeORM live suite. TAP on stdout; tool noise on stderr.
set -u
cd "$(dirname "$0")"

if [ ! -d node_modules ]; then
  npm install --no-audit --no-fund 1>&2 || { echo "not ok - typeorm.install # npm install failed"; exit 0; }
fi
exec node suite.mjs
