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

Architecture (decided in OSS-side ADR-style block of Phase 5.15):
each OSS repo (`basin`, future `basin-js`, future `basin-cli`) keeps its
`docs/` as standard markdown with YAML frontmatter.  `basin-cloud`'s
webapp has `npm run dev:docs` which build-time-fetches each OSS repo via
`git clone --depth=1` into `webapp/content/oss/<product>/`, then renders
the union via Docusaurus or Mintlify.  `basin-cloud` keeps its own
cloud-specific docs under `webapp/content/cloud/` separate from imported
OSS content.  CI hook on each OSS repo's `main` triggers basin-cloud
rebuild; pinning by git tag gives versioned docs per OSS release.

Items (mirror of TASK.md Phase 5.15.E – 5.15.I):

- **5.15.E** — basin-cloud webapp scaffold (pick Docusaurus or Mintlify).
  Acceptance gate: `npm install && npm run dev` renders a "hello world"
  docs page locally.
- **5.15.F** — `npm run dev:docs` script.  Build-time fetch of each OSS
  repo's `docs/` into `webapp/content/oss/<product>/`.  Acceptance gate:
  fresh checkout → `npm install` → `npm run dev:docs` → `npm run dev`
  shows OSS docs at `/docs/basin/architecture` etc.
- **5.15.G** — Cloud-only docs namespace under `webapp/content/cloud/`
  (billing, dashboards, security, scaling-as-a-service).  Acceptance
  gate: cloud docs render at `/docs/cloud/*` without colliding with
  imported OSS content.
- **5.15.H** — Cross-product link resolver — `[[basin-js:auth/login]]`
  → canonical URL on the rendered site.  Docusaurus plugin or Mintlify
  config.  Acceptance gate: a cross-reference from a Basin doc resolves
  to the right basin-js page in the rendered site.
- **5.15.I** — CI sync — webhook on each OSS repo's `main` triggers
  basin-cloud rebuild; nightly cron as safety net.  Acceptance gate:
  merging a docs-only PR in the OSS repo causes the cloud rendered site
  to update within ~15 minutes.

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
no tenant-identifying SQL content; only the canonical plan-shape hash +
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
