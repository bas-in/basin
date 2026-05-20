#!/usr/bin/env bash
# build-docs-index.sh — regenerates docs/README.md from frontmatter.
#
# Usage:
#   ./scripts/build-docs-index.sh           # writes docs/README.md
#   ./scripts/build-docs-index.sh --check   # exits 1 if committed file differs
#
# Requires: python3 (stdlib only), no extra deps.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCS_DIR="$REPO_ROOT/docs"
OUT="$DOCS_DIR/README.md"
SCRIPT_DIR="$REPO_ROOT/scripts"

GENERATED="$(python3 "$SCRIPT_DIR/_build_docs_index.py" "$DOCS_DIR")"

if [[ "${1:-}" == "--check" ]]; then
    ACTUAL="$(cat "$OUT")"
    if [[ "$ACTUAL" != "$GENERATED" ]]; then
        echo "docs/README.md is out of date. Run: ./scripts/build-docs-index.sh" >&2
        diff <(echo "$ACTUAL") <(echo "$GENERATED") >&2 || true
        exit 1
    fi
    echo "docs/README.md is up to date."
else
    printf '%s\n' "$GENERATED" > "$OUT"
    echo "Wrote $OUT"
fi
