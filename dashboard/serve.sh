#!/usr/bin/env bash
# Serve the Basin dashboard over HTTP. Required because browsers block
# fetch() over file://, which the dashboard uses to read data/*.json.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${PORT:-8000}"

echo "Basin dashboard:  http://localhost:${PORT}/"
echo "Serving directory ${DIR}"
echo "Press Ctrl+C to stop."
exec python3 -m http.server -d "${DIR}" "${PORT}"
