#!/usr/bin/env bash
# CI gate: every markdown file under docs/ MUST have valid YAML frontmatter
# per docs/frontmatter-spec.md (Phase 5.15.A).
#
# Validates:
#   - file starts with `---` line
#   - frontmatter block closes with `---`
#   - required fields present: title, nav_section, sidebar_position, summary
#   - nav_section is one of the enum values
#   - sidebar_position is a non-negative integer
#   - summary is ≤ 200 chars
#
# Exit 0 if all pass; exit 1 otherwise.

set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v python3 >/dev/null 2>&1; then
  echo "check-frontmatter: python3 not found" >&2
  exit 1
fi

python3 - <<'PY'
import os, re, sys

ALLOWED_NAV = {
    "overview", "architecture", "storage", "sql",
    "deployment", "operations", "decisions", "meta", "reference",
}
REQUIRED = ["title", "nav_section", "sidebar_position", "summary"]

errors = []

for root, _, files in os.walk("docs"):
    for fname in files:
        if not fname.endswith(".md"):
            continue
        path = os.path.join(root, fname)
        with open(path) as f:
            text = f.read()

        if not text.startswith("---\n"):
            errors.append(f"{path}: missing frontmatter (file does not start with ---)")
            continue

        end = text.find("\n---\n", 4)
        if end < 0:
            errors.append(f"{path}: unterminated frontmatter block")
            continue

        block = text[4:end]
        fields = {}
        for line in block.splitlines():
            m = re.match(r"^([a-z_]+):\s*(.*)$", line)
            if not m:
                continue
            fields[m.group(1)] = m.group(2).strip()

        for req in REQUIRED:
            if req not in fields:
                errors.append(f"{path}: missing required field `{req}`")

        nav = fields.get("nav_section", "")
        if nav and nav not in ALLOWED_NAV:
            errors.append(
                f"{path}: nav_section `{nav}` not in allowed enum "
                f"({sorted(ALLOWED_NAV)})"
            )

        pos = fields.get("sidebar_position", "")
        if pos:
            try:
                n = int(pos)
                if n < 0:
                    errors.append(f"{path}: sidebar_position must be >= 0, got {n}")
            except ValueError:
                errors.append(f"{path}: sidebar_position `{pos}` is not an integer")

        summary = fields.get("summary", "")
        if summary:
            inner = summary.strip()
            if inner.startswith('"') and inner.endswith('"'):
                inner = inner[1:-1]
            if len(inner) > 200:
                errors.append(
                    f"{path}: summary is {len(inner)} chars (max 200)"
                )

if errors:
    print("frontmatter validation failed:", file=sys.stderr)
    for e in errors:
        print(f"  {e}", file=sys.stderr)
    sys.exit(1)

print("frontmatter ok")
PY
