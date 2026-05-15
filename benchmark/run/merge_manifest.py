#!/usr/bin/env python3
"""Rebuild <data_dir>/manifest.json from the per-test JSON files present.

The dashboard (assets/dashboard.js) iterates the manifest's
viability/scaling/compare arrays and fetches `<kind>_<id>.json` for each
id. So a manifest member is exactly the JSON file stem with its leading
`<kind>_` stripped. This script derives the member list from the files
that actually exist — no stale members can survive — and orders members
deterministically (sorted) so reruns produce stable diffs.

Canonicalisation: Basin's term is "project", not "tenant". Any legacy
`tenant`-flavoured member name is rewritten to its `project` form so the
manifest never reintroduces the old vocabulary even if an old JSON file
lingers. (New test runs already emit project_* ids; this only guards
against stale files.)

Usage:
    merge_manifest.py [--since EPOCH] <data_dir> [<data_dir> ...]

--since EPOCH (unix seconds): only count cards whose `generated_at`
(`@<epoch>`) is >= EPOCH. This makes a manifest reflect exactly the
cards a single run regenerated — a test that failed (and therefore did
NOT re-emit its JSON) is omitted rather than carried forward with stale
numbers. Files older than EPOCH are LEFT ON DISK (not deleted) but kept
out of the manifest, and the omission is logged.

Exit non-zero if a data dir has no qualifying JSON reports at all (a run
that produced nothing is a failure worth surfacing).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

KINDS = ("viability", "scaling", "compare")

# Legacy -> canonical id fragments. Substring rewrite on the member id.
TENANT_TO_PROJECT = (
    ("idle_tenant_ram", "idle_project_ram"),
    ("idle_tenants", "idle_projects"),
    ("tenant_deletion_at_scale", "project_deletion_at_scale"),
    ("tenant_deletion", "project_deletion"),
    ("tenant_count", "project_count"),
    ("cross_tenant", "cross_project"),
    ("tenant", "project"),
)


def canonical(member: str) -> str:
    out = member
    for legacy, new in TENANT_TO_PROJECT:
        if legacy in out:
            out = out.replace(legacy, new)
    return out


def _gen_epoch(doc: dict) -> int | None:
    g = doc.get("generated_at")
    if isinstance(g, str) and g.startswith("@"):
        try:
            return int(g[1:])
        except ValueError:
            return None
    return None


def rebuild(data_dir: Path, since: int | None) -> int:
    if not data_dir.is_dir():
        print(f"merge_manifest: not a dir: {data_dir}", file=sys.stderr)
        return 1

    members: dict[str, set[str]] = {k: set() for k in KINDS}
    found = 0
    skipped_stale = 0
    for p in sorted(data_dir.glob("*.json")):
        if p.name == "manifest.json":
            continue
        try:
            doc = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError) as e:
            print(f"merge_manifest: skip unreadable {p.name}: {e}",
                  file=sys.stderr)
            continue
        kind = doc.get("kind")
        if kind not in KINDS:
            # Not a dashboard card (e.g. compare_vortex_parquet has no
            # kind / is consumed elsewhere) — leave the file in place but
            # don't add it to the manifest.
            continue
        if since is not None:
            ge = _gen_epoch(doc)
            if ge is None or ge < since:
                skipped_stale += 1
                print(f"merge_manifest: OMIT stale/failed card {p.name} "
                      f"(generated_at={doc.get('generated_at')!r} < "
                      f"run start {since}) — not in manifest",
                      file=sys.stderr)
                continue
        stem = p.stem
        prefix = f"{kind}_"
        member = stem[len(prefix):] if stem.startswith(prefix) else stem
        members[kind].add(canonical(member))
        found += 1

    if skipped_stale:
        print(f"merge_manifest: {data_dir.name}: omitted {skipped_stale} "
              f"stale/failed card(s) from manifest", file=sys.stderr)

    if found == 0:
        print(f"merge_manifest: {data_dir} has no card JSONs — refusing "
              f"to write an empty manifest", file=sys.stderr)
        return 1

    manifest = {k: sorted(members[k]) for k in KINDS}
    out = data_dir / "manifest.json"
    out.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"merge_manifest: {out} "
          f"(viability={len(manifest['viability'])}, "
          f"scaling={len(manifest['scaling'])}, "
          f"compare={len(manifest['compare'])})")
    return 0


def main(argv: list[str]) -> int:
    args = argv[1:]
    since: int | None = None
    dirs: list[str] = []
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--since":
            i += 1
            if i >= len(args):
                print("merge_manifest: --since needs an epoch arg",
                      file=sys.stderr)
                return 2
            try:
                since = int(args[i])
            except ValueError:
                print(f"merge_manifest: bad --since {args[i]!r}",
                      file=sys.stderr)
                return 2
        else:
            dirs.append(a)
        i += 1
    if not dirs:
        print(__doc__, file=sys.stderr)
        return 2
    rc = 0
    for d in dirs:
        rc |= rebuild(Path(d), since)
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
