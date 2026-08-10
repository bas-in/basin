#!/usr/bin/env bash
# CI gate: every relative markdown link in the repo must resolve to a file that
# exists.
#
# WHY
# ───
# Nothing checked this, and the tree had accumulated sixteen dead links —
# including four pointing at a cloud-roadmap doc under `docs/` that had been
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
# THE ONE EXCEPTION — self-referential GitHub links. `site/index.html` and
# `site/docs.html` cannot use relative paths (they are served from vulos.org,
# not from the repo), so every doc they link to is an absolute
# `https://github.com/vul-os/basin/blob/main/<path>` URL. Those are external in
# form but internal in fact: the path after `main/` is a path in THIS checkout,
# resolvable with no network call. Left unchecked, the whole product site sat
# outside every link gate — and did have a dead one (`ROADMAP.md`, a file that
# has never existed in this repo, linked as "Roadmap" from the site footer).
# So this class is resolved against the working tree, in .md and .html alike.
#
# Exit codes:
#   0  every checked link resolves (and there was at least one of each class)
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

# Self-referential GitHub URLs: the path after blob|tree/<ref>/ is a path in
# this checkout. Any ref is accepted (main today, a tag in a release note
# tomorrow) because what is being verified is the path, not the ref.
#
# Matched only against text already extracted as a LINK TARGET, never against
# raw file text. A bare scan also hits things that merely look like URLs and are
# not links — cli/README.md documents a cosign `--certificate-identity-regexp`
# whose value is a github.com/... *pattern*, complete with regex escapes, naming
# a workflow identity rather than a file to fetch. Reporting that as a dead link
# would be a false positive, and a gate that cries wolf gets muted.
SELF_LINK = re.compile(
    r"^https://github\.com/vul-os/basin/(?:blob|tree)/[^/\s]+/(.+)$"
)
HREF = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)

files = []
html_files = []
for root, dirs, names in os.walk("."):
    if any(part in root for part in SKIP_DIR_PARTS):
        dirs[:] = []
        continue
    dirs.sort()
    for name in sorted(names):
        if name.endswith(".md"):
            files.append(os.path.join(root, name))
        elif name.endswith(".html") and (
            root == os.path.join(".", "site") or root.startswith(os.path.join(".", "site") + os.sep)
        ):
            html_files.append(os.path.join(root, name))

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

# ── Self-referential GitHub links, in markdown and in the product site ──────
self_checked = 0
self_broken = []
for path in files + html_files:
    try:
        text = open(path, encoding="utf-8").read()
    except (OSError, UnicodeDecodeError) as exc:
        print(f"check-doc-links: cannot read {path}: {exc}", file=sys.stderr)
        sys.exit(1)
    hrefs = [m.group(1) for m in LINK.finditer(text)]
    hrefs += [m.group(1) for m in HREF.finditer(text)]
    for href in hrefs:
        match = SELF_LINK.match(href.strip())
        if not match:
            continue
        target = match.group(1).split("#")[0].split("?")[0].rstrip("/")
        if not target:
            continue
        self_checked += 1
        if not os.path.exists(os.path.normpath(target)):
            self_broken.append((path, href))

# Guard the guard, per class. site/ is tracked and full of these URLs, so zero
# of them means the HTML walk or the pattern broke — and a gate that reports
# "no broken site links" while examining none is the failure this file exists
# to not have.
if self_checked == 0:
    print(
        "check-doc-links: examined 0 self-referential github.com/vul-os/basin "
        f"links across {len(files) + len(html_files)} file(s).\n"
        "  site/*.html links to repo paths exclusively that way, so zero means\n"
        "  the walk or the SELF_LINK pattern stopped matching — not that the\n"
        "  links are fine.",
        file=sys.stderr,
    )
    sys.exit(1)

if self_broken:
    print(
        f"check-doc-links: {len(self_broken)} self-referential GitHub link(s) "
        f"point at a path that does not exist, out of {self_checked} checked:",
        file=sys.stderr,
    )
    for path, href in self_broken:
        print(f"  {path} -> {href}", file=sys.stderr)
    print(
        "\n  These render as a 404 on github.com. The path after blob|tree/<ref>/\n"
        "  is a path in this repo — fix it, or drop the link.",
        file=sys.stderr,
    )
    sys.exit(1)

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
    f"{len(files)} markdown file(s), and {self_checked} self-referential "
    f"GitHub link(s) across those plus {len(html_files)} site HTML file(s), "
    "all resolve."
)
PY
