---
title: "basin-cloud roadmap spec (OSS-side companion)"
nav_section: meta
sidebar_position: 5
summary: "Cloud-side items that pair with OSS phases. Lives in this OSS repo as a forward-spec; the bas-in/basin-cloud repo is the actual home and adopts these items into its own roadmap."
---

# basin-cloud roadmap spec

`basin-cloud` lives in a separate repository
([`bas-in/basin-cloud`](https://github.com/bas-in/basin-cloud)) and has its
own roadmap.  This file is a **forward-spec** for cloud-side work that
**pairs with OSS phases shipped here**.  When an item below is adopted into
basin-cloud's own roadmap, this file gets a back-link to the cloud-repo
ticket and the item is otherwise treated as scoped from this OSS side.

Two priorities live here today, both as companions to OSS phases:

---

## Phase 5.15-cloud — Unified docs platform (cloud webapp side)

Companion to OSS Phase 5.15 (frontmatter spec, OSS migration, top-level
index — all shipped in commit `d5ffbe1`).  The cloud side ships the
rendered surface that consumes OSS docs.

**Architecture (decided 2026-05-19 — supersedes the earlier
git-clone-during-build draft).**

Each OSS repo (`basin`, future `basin-js`, future `basin-cli`) keeps
its `docs/` as standard markdown with YAML frontmatter (see
[`frontmatter-spec.md`](./frontmatter-spec.md)).  `basin-cloud`'s
webapp has an `npm run dev:docs` script that reads docs from **local
sibling folders**, not from a git clone during build:

```text
~/code/exo/
├── basin/        ← this OSS repo
│   └── docs/
├── basin-js/     ← future OSS repo
│   └── docs/
├── basin-cli/    ← future OSS repo
│   └── docs/
└── basin-cloud/  ← cloud repo
    └── webapp/
        ├── content/
        │   ├── oss/        ← populated by `npm run dev:docs`
        │   │   ├── basin/
        │   │   ├── basin-js/
        │   │   └── basin-cli/
        │   └── cloud/      ← basin-cloud's own docs (live in
        │       └── ...       basin-cloud repo, not OSS)
        └── package.json
```

**Why sibling-folder rather than `git clone --depth=1`?**

1. **Live reload during dev.** Editing a doc in `basin/docs/` and
   seeing it update on the basin-cloud preview without re-running a
   clone is the only sane authoring loop.  `npm run dev:docs --watch`
   recopies on every save.
2. **Single source of truth.** OSS contributors edit one markdown
   file; basin-cloud reads it.  No build-time race where the cloud
   site renders a stale clone.
3. **No CI shell-out to git.** The cloud webapp build is pure npm —
   no need for git binaries on every build agent, no SSH key
   management for private mirrors.

**Fallback when a sibling repo is missing.**  `dev:docs` checks for
each sibling (`../basin/docs/`, `../basin-js/docs/`, `../basin-cli/docs/`)
relative to the basin-cloud checkout.  If any are missing, the script
**fails fast** with a clear message:

```
[basin-cloud dev:docs] ../basin-js not found.
Clone the OSS repos as siblings of basin-cloud before running:
  cd ..
  git clone https://github.com/vul-os/basin
  git clone https://github.com/bas-in/basin-js
  git clone https://github.com/bas-in/basin-cli
Then re-run `npm run dev:docs`.
```

The user fix is one shell command per missing repo, predictable and
documented.

**CI / production deployment.**  CI does not run `dev:docs` directly;
instead a `npm run build:docs` variant clones the OSS repos by tag
(production wants pinned versions, not `main` HEAD) into a temp
directory and points the same fetcher at those paths.  Same code
path, different inputs.

**Items (mirror of TASK.md Phase 5.15.E – 5.15.I):**

- **5.15.E** — basin-cloud webapp scaffold (pick Docusaurus or Mintlify).
  Acceptance gate: `npm install && npm run dev` renders a "hello world"
  docs page locally with no OSS docs imported yet.
- **5.15.F** — `npm run dev:docs` script (local sibling-folder mode).
  Files: `webapp/scripts/fetch-docs.mjs` (or similar), `package.json`
  script entries.  Reads `../basin/docs/`, `../basin-js/docs/`,
  `../basin-cli/docs/`; copies each into
  `webapp/content/oss/<product>/`; preserves frontmatter; fails fast
  with the "clone these first" message if any sibling is missing.
  Acceptance gate: with all 4 repos checked out as siblings, fresh
  checkout → `npm install` → `npm run dev:docs` → `npm run dev` shows
  OSS docs at `/docs/basin/architecture`, `/docs/basin-js/getting-started`,
  etc.  With basin-cli missing: script exits non-zero with the
  clone-these-first message verbatim.
- **5.15.F.2** — `npm run build:docs` script (CI / production mode).
  Same fetcher as 5.15.F but accepts a `BASIN_DOCS_SOURCES` env or
  config file: `{ basin: { mode: "git-tag", tag: "v0.1.9" }, ... }`.
  Clones each repo at the specified tag into a temp dir, points the
  fetcher there.  Acceptance gate: CI builds the cloud site without
  any sibling folders present, pulling each OSS repo at its pinned
  tag.
- **5.15.G** — Cloud-only docs namespace under
  `webapp/content/cloud/` — billing, dashboards, security,
  scaling-as-a-service.  basin-cloud owns these files in its own
  repo; the fetcher never touches them.  Acceptance gate: cloud docs
  render at `/docs/cloud/*` without colliding with imported OSS
  content.
- **5.15.H** — Cross-product link resolver — `[[basin-js:auth/login]]`
  → canonical URL on the rendered site.  Docusaurus plugin or
  Mintlify config.  Acceptance gate: a cross-reference from a Basin
  doc resolves to the right basin-js page in the rendered site.
- **5.15.I** — Auto-rebuild on OSS docs change.  In the local dev
  loop, `npm run dev:docs -- --watch` re-copies on every save.  In
  CI / production, a GitHub Action on each OSS repo's `main` ping
  basin-cloud to pull the latest tag and rebuild.  Acceptance gate:
  editing `basin/docs/architecture.md` and saving causes the
  basin-cloud preview to update within 2 s.

**Out of scope (post-launch follow-ups, both sides):** search (Algolia or
built-in), versioned-docs UI dropdown, localisation, in-page edit links
back to the OSS repo.

---

## Phase 5.16-cloud — Query insights (cloud ingest + UI + cross-customer aggregates)

Companion to OSS Phase 5.16 (plan-shape hashing, per-shape histograms,
scale-regression tracking, OTLP export schema).  The cloud side ingests
those exports and surfaces them in a customer-facing UI plus
Basin-engineer-only anonymised cross-customer aggregates.

The OSS side strips literals at the LogicalPlan layer before any
persistence or export.  By the time the cloud side sees a record there is
no project-identifying SQL content; only the canonical plan-shape hash +
aggregate metrics.  This is the privacy invariant ADR 0017 will record.

Items (mirror of TASK.md Phase 5.16.E – 5.16.H):

- **5.16.E** — Cloud ingest pipeline.  OTLP receiver → time-series store
  (ClickHouse is the leading candidate; VictoriaMetrics or TimescaleDB
  are alternatives).  Per-customer retention windows; bounded cardinality
  guards (max 500 distinct shapes per project per hour).  Acceptance
  gate: 10k OSS basin instances each exporting 100 shape-records/sec sustain
  for 1 hour with no ingest drops; ingest p99 latency ≤ 500 ms.  Estimate:
  ~2-3 weeks.
- **5.16.F** — Cloud Query Insights UI ("your slowest queries" view).
  Per-customer drilldown: shape list ordered by p99 / total time / scale
  regression score; per-shape detail page with histogram of latency,
  rows scanned, files opened, cache hit rate; an example normalised SQL
  template (literals shown as `$1`, `$2`, …) reconstructed from the plan
  shape — never the customer's actual query text.  Plan diff: this
  version had a SortExec, that version didn't — links to the
  `basin.sort_by` DDL fix.  Acceptance gate: a customer with 50 distinct
  shapes can find their slowest in under 30 s of UI navigation.
  Estimate: ~3-4 weeks.
- **5.16.G** — Anti-pattern detection + suggestion engine.  Curated
  catalog of "known bad shape → suggested fix" mappings (e.g. `WHERE id
  = ?` without `basin.sort_by='id'` → suggest the DDL; window function
  with PARTITION BY non-sort-by column → suggest compound `basin.sort_by`).
  Surfaces as in-line tips on the per-shape detail page.  Catalog
  starts hand-curated; later phases may add automated regression
  detection.  Acceptance gate: 10 anti-pattern → fix mappings seeded;
  one customer query that matches an anti-pattern surfaces the fix tip
  in the UI.  Estimate: ~2 weeks + ongoing curation.
- **5.16.H** — Anonymised cross-customer aggregates (Basin engineers
  only).  Internal UI surfacing "% of customers running shape X",
  "% of customers with table > 100 GB hitting shape Y", etc.
  k-anonymity threshold (k ≥ 5): no aggregate shown for fewer than 5
  contributing customers.  Never drill from an aggregate to per-customer
  data.  Acceptance gate: legal / privacy review sign-off; k-anonymity
  enforced at query time, not just at display time.  Estimate: ~1 week
  for the basic surface + ~1 week for privacy review.

---

## Out of scope here (handled in basin-cloud's own roadmap)

The following items live entirely in basin-cloud and do **not** depend
on or affect this OSS repo:

- Org / billing / Stripe integration
- Per-project Fly Machine provisioning
- BYO-bucket / BYO-key flows
- Cloud-side auth (basin-auth in OSS provides the per-engine identity;
  the cloud's own admin auth is separate)
- Cross-region routing / multi-region read-replica orchestration

When OSS phases ship that affect those (e.g. `basin.row_block_size` DDL
in OSS implies a per-project default knob in basin-cloud's project
config UI), this file gets an entry.

---

## Naming convention

OSS-side items use the bare phase number (e.g. **5.16.A**).  Cloud-side
counterparts append `-cloud` to the phase (e.g. **5.16-cloud**, items
**5.16.E** – **5.16.H** scoped from the OSS side as cloud-residency).
A single phase number across both repositories binds the two halves of
the same initiative.
