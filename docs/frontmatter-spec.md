---
title: "Frontmatter spec for Basin OSS docs"
nav_section: meta
sidebar_position: 0
summary: "YAML frontmatter contract every markdown file under docs/ obeys, so the docs site can fetch and render the whole OSS doc set."
---

# Frontmatter spec for Basin OSS docs

Every markdown file under `docs/` in this repo (and in future OSS sibling
repos `basin-js`, `basin-cli`) MUST begin with a YAML frontmatter block.
The block lets the docs site build-time-fetch every product's docs and
render them in a unified nav tree without per-file overrides.

This spec is Phase 5.15.A — see [`TASK.md`](./TASK.md) Phase 5.15.

## Schema

```yaml
---
title: "Human-readable doc title"        # required
nav_section: architecture                # required, one of below
sidebar_position: 3                      # required, integer >= 0
summary: "One-sentence description."     # required, ≤ 200 chars
version_since: "0.1.0"                   # optional, semver
version_until: "0.2.0"                   # optional, semver
tags: [storage, performance]             # optional, free-form
---
```

### Fields

| Field | Required | Type | Notes |
|---|---|---|---|
| `title` | yes | string | Shown in the rendered site's H1 + browser tab. Should be the same string as the `# Heading` in the body so GitHub direct rendering still looks right. |
| `nav_section` | yes | enum | One of: `overview`, `architecture`, `storage`, `sql`, `deployment`, `operations`, `decisions`, `meta`, `reference`. Drives the top-level nav grouping on the docs site. |
| `sidebar_position` | yes | int ≥ 0 | Order within `nav_section`. Lower = earlier. Gaps allowed (use multiples of 10 so insertions are cheap). |
| `summary` | yes | string ≤ 200 chars | Shown in nav previews + search results. Should make sense out of context. |
| `version_since` | no | semver | If present, doc applies only from this OSS version onward. Rendered site hides on older version views. |
| `version_until` | no | semver | If present, doc applies only up to this OSS version. Rendered site hides on newer version views. |
| `tags` | no | string[] | Free-form for cross-cutting taxonomy (e.g. `[performance]`, `[security]`). Used by search and "related docs" widgets. |

## `nav_section` reference

| Section | Use for |
|---|---|
| `overview` | Project intro, getting-started, value proposition |
| `architecture` | How the system is put together; component diagrams |
| `storage` | File formats, encodings, catalogs, object storage |
| `sql` | SQL surface, supported syntax, function reference |
| `deployment` | Running in production, configuration, deployment topologies |
| `operations` | Day-2 ops: backups, monitoring, upgrades, troubleshooting |
| `decisions` | ADRs (architectural decision records) |
| `meta` | Documentation about the documentation itself (this spec, contribution guides) |
| `reference` | Auto-generated reference material (API, SQL function matrices) |

## Cross-doc links

Inside a single repo, use relative paths: `[architecture](./architecture.md)`.

For cross-product links (one OSS repo to another), use the `[[product:path]]`
syntax which the docs site resolves at render time:

```markdown
See [[basin-js:auth/login]] for the JS client equivalent.
See [[basin-cli:commands/migrate]] for the migration tool.
```

Path is relative to the target product's `docs/` root, without the
`.md` extension. The link resolver is Phase 5.15.H — until it ships,
cross-product links render verbatim and humans read them as a
deferred-link.

## Why YAML, not MDX or sidecar TOML

Considered alternatives:

- **MDX** (Markdown + JSX) — Mintlify and Docusaurus both support it,
  and it allows embedding React components. Rejected because: (1)
  GitHub doesn't render MDX natively, so contributors browsing the OSS
  repo wouldn't see a useful page; (2) it locks the OSS repo into a
  specific renderer; (3) embedding components encourages presentation
  logic to leak into the OSS repo, which then can't be displayed
  consistently anywhere else.
- **Sidecar TOML** (`architecture.md` + `architecture.toml`) — keeps
  the markdown completely portable. Rejected because: (1) doubles the
  number of files; (2) makes "view this doc on GitHub" not show the
  metadata; (3) tooling has to read two files in sync, doubling the
  failure modes.
- **HTML comments** (`<!-- title: ... -->`) — invisible on GitHub,
  parseable by tooling. Rejected because: (1) no schema validation;
  (2) every renderer rolls its own comment-extraction parser.

YAML frontmatter is the convention for Jekyll, Hugo, Docusaurus,
Mintlify, Next.js content collections, Astro, and Notion-style
tools. GitHub renders the frontmatter as a clean key/value table at
the top of the rendered file, so it adds context for direct readers
rather than subtracting from the page.

## CI enforcement

A check runs in CI on every PR that touches `docs/**/*.md`:

```bash
scripts/check-frontmatter.sh
```

The script parses every markdown file's frontmatter, validates against
this spec, and fails the PR if any file:

- has no frontmatter block
- has unknown required fields missing
- uses a `nav_section` not in the enum
- has a non-integer `sidebar_position`
- has a `summary` longer than 200 chars

## Out of scope

- **Translation / localisation** — frontmatter doesn't carry locale; a
  future locale-routing layer on the docs site handles it.
- **Author attribution** — git history is the source of truth.
- **Last-updated timestamps** — git history is the source of truth;
  the docs site's renderer can fetch and display the most recent commit
  date if it wants to.
- **Custom routing** — the rendered URL is derived from
  `nav_section` + file path. Files can't override their own URL slug
  by frontmatter. Keeps the path → URL mapping predictable.
