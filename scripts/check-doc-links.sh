#!/usr/bin/env bash
# CI gate: every relative markdown link in the repo must resolve to a file that
# exists.
#
# WHY
# ───
# Nothing checked this, and the tree had accumulated sixteen dead links —
# including four pointing at `docs/basin-cloud-roadmap.md`, which had been
# deleted one commit earlier, and three at `gen_types_map.go` / `cmd_gen.go`,
# files that stopped existing when the CLI was rewritten from Go to Rust. Two
# more pointed at ADR filenames that were never right (`0008-io-scheduler.md`,
# `0016-htap-hottier.md`), so the "see the ADR" cross-reference on the storage
# runbook had never worked.
#
# FAIL CLOSED
# ───────────
# Non-zero on any unresolvable link, and — the part that matters — non-zero if
# it finds no links to check at all. A link checker that silently examined zero
# links because a path assumption changed is the failure mode this whole audit
# keeps turning up.
#
# Only relative links are checked. http(s)/mailto/tel and bare `#anchor`
# fragments are out of scope: verifying external URLs would put a network call
# on the critical path of every push and make the gate flaky on someone else's
# outage, which is a worse trade than not checking them.
#
# Exit codes:
#   0  every relative link resolves (and there was at least one)
#   1  a link is broken, or the checker found nothing to check

set -euo pipefail

cd "$(dirname "$0")/.."

command -v python3 >/dev/null 2>&1 || {
  echo "check-doc-links: python3 not found in PATH" >&2
  exit 1
}

python3 - <<'PY'
import os
import re
import sys

SKIP_DIR_PARTS = (
    os.sep + "target",
    os.sep + "node_modules",
    os.sep + ".git",
    os.sep + ".basin-seaweedfs-data",
    os.sep + "dist",
)

LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")

files = []
for root, dirs, names in os.walk("."):
    if any(part in root for part in SKIP_DIR_PARTS):
        dirs[:] = []
        continue
    dirs.sort()
    for name in sorted(names):
        if name.endswith(".md"):
            files.append(os.path.join(root, name))

checked = 0
broken = []
for path in files:
    base = os.path.dirname(path)
    try:
        text = open(path, encoding="utf-8").read()
    except (OSError, UnicodeDecodeError) as exc:
        print(f"check-doc-links: cannot read {path}: {exc}", file=sys.stderr)
        sys.exit(1)
    for match in LINK.finditer(text):
        href = match.group(1)
        if href.startswith(("http://", "https://", "mailto:", "tel:", "#")):
            continue
        target = href.split("#")[0].split("?")[0]
        if not target:
            continue
        checked += 1
        if not os.path.exists(os.path.normpath(os.path.join(base, target))):
            broken.append((path, href))

# Guard the guard. Zero links checked means the walk found nothing — a changed
# layout, a bad skip rule — and "no broken links" would then be true but
# meaningless.
if checked == 0:
    print(
        "check-doc-links: examined 0 relative links across "
        f"{len(files)} markdown file(s).\n"
        "  That is not a pass: it means the checker found nothing to check.\n"
        "  Fix the walk or the link pattern in scripts/check-doc-links.sh.",
        file=sys.stderr,
    )
    sys.exit(1)

if broken:
    print(
        f"check-doc-links: {len(broken)} broken relative link(s) "
        f"out of {checked} checked:",
        file=sys.stderr,
    )
    for path, href in broken:
        print(f"  {path} -> {href}", file=sys.stderr)
    print(
        "\n  Each one is a reader hitting a 404. Fix the path, or drop the link\n"
        "  and keep the prose.",
        file=sys.stderr,
    )
    sys.exit(1)

print(
    f"check-doc-links: ok — {checked} relative link(s) across "
    f"{len(files)} markdown file(s) all resolve."
)
PY
