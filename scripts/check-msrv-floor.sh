#!/usr/bin/env bash
# CI gate: the workspace's declared `rust-version` must be >= the highest
# `rust-version` any package in the resolved dependency graph declares.
#
# WHY THIS EXISTS
# ───────────────
# `rust-version` is load-bearing, not documentation: cargo hard-errors when the
# active rustc is below the highest declared floor anywhere in the graph —
#
#   error: rustc 1.85.1 is not supported by the following packages:
#     vortex-error@0.71.0 requires rustc 1.91.0
#
# The workspace declared 1.85 while the lockfile required 1.92, and nothing
# noticed for months because:
#   * every CI job installs a pinned toolchain and then has it silently
#     overridden by rust-toolchain.toml's `channel = "stable"`, so CI never
#     actually built at the declared MSRV; and
#   * the only place the pin *was* honoured — the Dockerfile's
#     `FROM rust:1.85` builder — is a workflow that had been red long enough to
#     read as background noise.
#
# So this gate asserts the claim directly, from `cargo metadata`, on a runner
# whose toolchain cannot mask it.
#
# FAIL CLOSED
# ───────────
# Exits non-zero, with the offending packages named, when:
#   * the declared rust-version is lower than the graph floor;
#   * `cargo metadata` fails, or emits no workspace member with a rust-version
#     (that would otherwise "pass" by comparing against nothing).
# There is no skip path. `--locked` so the answer is about the committed
# lockfile rather than whatever resolution today's registry produces.
#
# Exit codes:
#   0  declared rust-version >= graph floor
#   1  drift, or the check could not be performed

set -euo pipefail

cd "$(dirname "$0")/.."

command -v cargo >/dev/null 2>&1 || {
  echo "check-msrv-floor: cargo not found in PATH" >&2
  exit 1
}
command -v python3 >/dev/null 2>&1 || {
  echo "check-msrv-floor: python3 not found in PATH" >&2
  exit 1
}

META_FILE="$(mktemp "${TMPDIR:-/tmp}/basin-msrv-meta.XXXXXX")"
trap 'rm -f "${META_FILE}"' EXIT

# The python program below arrives on stdin, so the metadata cannot: it goes to
# a temp file whose path is passed as argv[1].
cargo metadata --format-version 1 --locked >"${META_FILE}" 2>/dev/null || {
  echo "check-msrv-floor: \`cargo metadata --locked\` failed." >&2
  echo "  Cannot verify the MSRV claim, so this run fails rather than passing" >&2
  echo "  on an unread graph. Usually a Cargo.lock that is out of date with" >&2
  echo "  Cargo.toml — run \`cargo metadata\` locally to see the real error." >&2
  exit 1
}

python3 - "${META_FILE}" <<'PY'
import json, sys

with open(sys.argv[1]) as fh:
    meta = json.load(fh)


def ver(v):
    parts = [int(x) for x in v.split(".")]
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


members = set(meta["workspace_members"])
declared = {}
for p in meta["packages"]:
    if p["id"] in members and p.get("rust_version"):
        declared[p["name"]] = p["rust_version"]

# Guard the guard: if nothing declares a rust-version, there is no claim to
# check and a silent pass would be indistinguishable from a real one.
if not declared:
    print(
        "check-msrv-floor: no workspace member declares `rust-version`.\n"
        "  Nothing to compare against means this gate verifies nothing, so it\n"
        "  fails instead. Set `rust-version` under [workspace.package] in\n"
        "  Cargo.toml.",
        file=sys.stderr,
    )
    sys.exit(1)

# The workspace inherits one value via [workspace.package]; take the lowest
# declared among members so the check is against the weakest claim made.
declared_floor = min(declared.values(), key=ver)

offenders = [
    (p["name"], p["version"], p["rust_version"])
    for p in meta["packages"]
    if p.get("rust_version") and ver(p["rust_version"]) > ver(declared_floor)
]

if offenders:
    offenders.sort(key=lambda r: ver(r[2]), reverse=True)
    worst = offenders[0][2]
    print(
        f"check-msrv-floor: declared rust-version {declared_floor} is below the "
        f"resolved graph floor {worst}.",
        file=sys.stderr,
    )
    print(
        "  cargo will refuse to build on any toolchain below the graph floor, so\n"
        f"  the declared {declared_floor} is a claim no build can satisfy.\n"
        f"  Fix: set rust-version = \"{worst}\" under [workspace.package] in\n"
        "  Cargo.toml, and bump the Dockerfile's RUST_VERSION arg to match.\n",
        file=sys.stderr,
    )
    print(f"  {len(offenders)} package(s) above the declared floor:", file=sys.stderr)
    for name, version, rv in offenders[:15]:
        print(f"    {name} {version} requires rustc {rv}", file=sys.stderr)
    if len(offenders) > 15:
        print(f"    ... and {len(offenders) - 15} more", file=sys.stderr)
    sys.exit(1)

print(
    f"check-msrv-floor: ok — declared rust-version {declared_floor} is at or "
    f"above every rust-version in the resolved graph "
    f"({len(meta['packages'])} packages checked)."
)
PY
