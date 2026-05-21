#!/usr/bin/env bash
# dev/scripts/down.sh — tear down the Basin dev-stack.
#
# Usage:
#   bash dev/scripts/down.sh [--volumes]
#
# Options:
#   --volumes   also remove named Docker volumes (catalog data, minio data,
#               basin data). Use when you want a clean-slate restart.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VOLUME_FLAG=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --volumes) VOLUME_FLAG="--volumes"; shift ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

cd "$DEV_DIR"

echo "==> Basin dev-stack: tearing down..."
# shellcheck disable=SC2086
docker compose --profile replica-1 down $VOLUME_FLAG

echo "==> Dev-stack stopped."
if [[ -n "$VOLUME_FLAG" ]]; then
  echo "    Named volumes removed. Next 'up' will start fresh."
fi
