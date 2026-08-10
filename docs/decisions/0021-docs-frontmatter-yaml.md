---
title: "ADR 0021 — YAML frontmatter as the docs metadata contract"
nav_section: decisions
sidebar_position: 21
summary: "All markdown files under docs/ carry YAML frontmatter with a fixed schema (title, nav_section, sidebar_position, summary, optional version bounds). MDX and sidecar TOML were considered and rejected."
tags: [docs, meta, tooling]
---

# 0021 — YAML frontmatter as the docs metadata contract

- **Status:** Accepted (2026-05-20) — locks the format that Phase 5.15.B (migration) and 5.15.C (top-level index) build against.
- **Tags:** docs, meta, tooling
- **Supersedes:** none
- **Cross-references:** [`docs/frontmatter-spec.md`](../frontmatter-spec.md) — the normative field-by-field schema; Phase 5.15 spec in [`../TASK.md`](../TASK.md)

## Context

Basin's `docs/` directory is growing past the point where contributors can
find things by scanning filenames. The unified docs site needs
to pull each OSS repo's `docs/` at build time and render a coherent nav tree
across products (Basin OSS, `basin-js`, `basin-cli`). That pull-and-render
pipeline needs stable, machine-parseable metadata per file — at minimum:
page title, navigation section, ordering within that section, and a short
summary for search indexing.

Without a metadata contract, the docs site would have to infer page titles
from heading text (fragile), guess nav placement from directory structure
(doesn't survive doc reorganisations), and produce no search summaries at
all. Every OSS contributor would need to consult a separate config file
in the docs-site repo to add or rename a doc, coupling two repos
unnecessarily.

Three formats were in scope: YAML frontmatter embedded in the markdown
file, MDX (Markdown + JSX imports), and sidecar TOML (a separate
`.toml` file next to each `.md` file).

## Decision

Adopt **YAML frontmatter** as the sole metadata format for all markdown
files under `docs/`. The normative field schema is in
[`docs/frontmatter-spec.md`](../frontmatter-spec.md). The required fields
are `title`, `nav_section`, `sidebar_position`, and `summary`. The optional
fields are `version_since` and `version_until` (semver strings) and `tags`
(string array).

The required / optional split was chosen by asking exactly what a unified
docs site needs at build time:

- `title`, `nav_section`, `sidebar_position` — necessary to render the
  navigation tree at all. Without them, the renderer cannot place the
  page anywhere.
- `summary` — necessary to populate the search index and nav previews
  without fetching and parsing the full page body at render time.
- `version_since` / `version_until` — useful for version-gated content
  (e.g. an HTAP guide that only applies from v0.4.0 onward) but not
  meaningful for most docs, so optional. Forcing every doc to declare
  versions would generate meaningless `version_since: "0.1.0"` noise on
  every historical file.
- `tags` — useful for cross-cutting taxonomy (performance, security) but
  optional: the nav and search still work without them.

CI enforcement is via `scripts/check-frontmatter.sh` (Phase 5.15.B).

## Why YAML frontmatter over the alternatives

### MDX rejected

MDX (Markdown + JSX) is the native format for Docusaurus and Mintlify. It
allows embedding React components directly in document source. The appeal is
that presentation logic (callouts, version badges, live code playgrounds)
can live next to the prose.

**Rejected for Basin OSS docs for three reasons:**

1. **GitHub doesn't render MDX.** Contributors browsing `docs/` on
   github.com would see raw JSX import syntax at the top of every file —
   a poor experience for a project that wants low friction for external
   contributors. YAML frontmatter is rendered by GitHub as a clean
   key/value table above the markdown body.

2. **It locks the OSS repo to a specific renderer.** If Basin ever migrates
   from Mintlify to Astro Starlight or a custom renderer, MDX components
   embedded in OSS source may not translate. YAML frontmatter is
   renderer-agnostic; the same block is understood by Jekyll, Hugo,
   Docusaurus, Mintlify, Astro, and Next.js content collections.

3. **It invites presentation leakage.** Once `.mdx` is allowed, the
   temptation is to add `<Callout type="warning">` or
   `<VersionBadge since="0.4.0" />` inline. These component names are
   coupling: the OSS repo now depends on whatever component library
   the docs site exports. A YAML frontmatter approach keeps the OSS side
   plain markdown; the docs site applies presentation entirely at render
   time on its own side.

### Sidecar TOML rejected

The sidecar model places metadata in a separate file next to each doc:
`architecture.md` + `architecture.toml`. TOML's syntax is less
whitespace-sensitive than YAML, which reduces one class of contributor
mistake.

**Rejected for three reasons:**

1. **Doubles the file count.** A `docs/` tree with 40 files becomes 80
   files. PR diffs show two hunks for every one-line title change. GitHub's
   file list for the PR becomes harder to scan.

2. **Metadata is invisible when reading the doc.** A contributor browsing
   `architecture.md` on GitHub sees no metadata unless they separately open
   `architecture.toml`. With YAML frontmatter, the metadata is right there
   at the top of the file, immediately visible.

3. **Two-file invariant is fragile.** If someone adds `new-guide.md` and
   forgets `new-guide.toml`, the CI linter produces a confusing error at
   build time in the docs site rather than a clear error at PR time in the
   OSS repo. The single-file approach makes the invariant trivially
   checkable: every `.md` file either has a frontmatter block or it
   doesn't.

### HTML comments rejected (briefly)

A variant of the sidecar idea is to embed metadata in HTML comments
(`<!-- title: Foo -->`). Invisible in the rendered output (good), but: no
schema validation, every tooling consumer rolls its own parser, and the
comments are not surfaced by GitHub's rendering at all. YAML frontmatter
wins on all three axes.

## Trade-off: YAML whitespace sensitivity

YAML is whitespace-sensitive. A stray tab character, an off-by-one
indentation, or a missing space after the colon produces a parse error.
This is the most common complaint from contributors new to the format.

**Mitigation:** Phase 5.15.B adds `scripts/check-frontmatter.sh`, which
runs in CI on every PR touching `docs/**/*.md`. The script uses a strict
YAML parser (not a regex) and reports the exact line and field that fails,
with a suggestion to copy the template from `docs/frontmatter-spec.md`.
The spec itself includes a complete copy-paste example block for exactly
this purpose. The lint surface is small — six fields, all scalar types
except `tags` — so the whitespace sensitivity is a shallow gotcha rather
than a structural risk.

## Consequences

### What is now possible

- **Phase 5.15.B (doc migration):** each existing `docs/**/*.md` file can
  be migrated by adding the frontmatter block. The spec is the checklist.
- **Phase 5.15.C (top-level index):** the `docs/README.md` index can be
  generated from frontmatter fields rather than maintained by hand.
- **Docs-site fetch pipeline:** the build-time fetcher can parse every
  OSS repo's docs without per-file config in the cloud repo. The nav tree
  is fully self-describing from the frontmatter.
- **Version-gated docs:** `version_since` / `version_until` let the cloud
  renderer hide stale or forward-only content per the user's selected
  Basin version, without touching OSS source files.

### What is now harder

- **Every new doc requires four required fields.** A contributor adding a
  quick reference file cannot skip the frontmatter block; CI will reject it.
  This is an intentional friction: undiscoverable docs are almost as bad as
  missing docs. The template in `docs/frontmatter-spec.md` reduces the cost
  to a copy-paste.
- **Frontmatter must stay in sync with the spec.** If the field schema
  changes (e.g. `nav_section` gains a new enum value), all existing files
  using the old value must be updated. The spec file is the single source of
  truth; breaking changes require a `check-frontmatter.sh` update followed
  by a bulk migration.

## Trigger to reconsider

If the docs site switches its docs renderer to one that requires MDX
natively and has no YAML-frontmatter ingestion path, the trade-off flips:
MDX would become the lower-friction format. Until then, the renderer
portability and GitHub rendering benefits of YAML frontmatter outweigh the
whitespace-sensitivity downside.

## References

- [`docs/frontmatter-spec.md`](../frontmatter-spec.md) — normative field schema and copy-paste example.
- Phase 5.15 spec in `TASK.md` (lines 1157–1162) — acceptance gates that this ADR's schema satisfies.
- [Docusaurus frontmatter docs](https://docusaurus.io/docs/markdown-features#front-matter) — the upstream reference for the format; Basin's schema is a compatible subset.
