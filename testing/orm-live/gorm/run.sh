#!/usr/bin/env bash
# GORM live suite. TAP on stdout; tool noise on stderr.
set -u
cd "$(dirname "$0")"

if [ ! -f go.sum ]; then
  go mod tidy 1>&2 || { echo "not ok - gorm.install # go mod tidy failed"; exit 0; }
fi
exec go run .
