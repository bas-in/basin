---
title: "ADR index"
nav_section: decisions
sidebar_position: 0
summary: "Index of every architectural decision record. Each \"no\" with the trigger that would change our mind."
---

# Architecture Decision Records

One file per load-bearing decision. Numbered, append-only, immutable.

## Why ADRs

When someone — a future contributor, a customer, a confused engineer six
months from now — asks "why doesn't Basin do X?", we want a stable,
findable answer that records the *reason* and the *trigger that would
change our mind*. Not folklore. Not "we discussed it once on Slack."

ADRs are particularly load-bearing for the **deferred** features. The
build prompt's section 6 is explicit: scope discipline is the hardest
part of this project. The ADRs are the artifact that makes "no" stick.

## Format

Each file is `NNNN-kebab-case-title.md` with this structure:

```markdown
# NNNN — Title

- **Status:** Accepted | Superseded by NNNN | Withdrawn
- **Date:** YYYY-MM-DD
- **Tags:** scope, architecture, security, etc.

## Context
What's the situation that forced a decision?

## Decision
What did we decide?

## Consequences
- Positive
- Negative
- Mitigations

## Architectural compatibility
What does today's design preserve, in case we have to flip later?

## Trigger to reconsider
What concrete signal — almost always a paying customer with contract
terms — would cause us to write a successor ADR?

## Alternatives considered and why we didn't pick them
Show your work. Future-you needs to see the options that were on the
table.
```

## Rules

1. **Don't edit accepted ADRs.** To change direction, write a new ADR
   that supersedes it. The old one stays as historical record.
2. **Number sequentially.** First ADR is `0001-…`, next is `0002-…`,
   and so on. No skipping.
3. **One decision per file.** If a decision spans multiple unrelated
   axes, write multiple ADRs.
4. **Trigger must be specific.** "When customers ask" is not a trigger.
   "When a customer signs ≥ $50k/yr ARR contingent on this" is a trigger.
5. **Link from `CAPABILITIES.md`** so the customer-facing description
   has a one-click route to the engineering reasoning.

## Index

| # | Title | Status | Summary |
|---|---|---|---|
| 0001 | Single-region only | Accepted | No cross-region 2PC; strong consistency deferred |
| 0002 | No Postgres extensions | Accepted | Native Rust crates instead of upstream .so loading |
| 0003 | Native vector search | Accepted | HNSW in-engine; no pg_vector wire-compat |
| 0004 | Multi-region read replicas | Accepted | Eventual-consistent read replicas scoped for Phase 6 |
| 0005 | Auth system | Accepted | basin-auth scope: signup, JWT, refresh, API keys, magic-link |
| 0006 | REST API layer | Accepted | basin-rest: PostgREST-shaped, requires auth enabled |
| 0007 | Connection pooling | Accepted | Native ProjectSession cache; per-project cap; LRU eviction |
| 0008 | Noisy-neighbor fairness | Accepted | EDF scheduler; priority by op-shape; p99 13.97 ms under load |
| 0009 | Multi-region architecture | Accepted | One cluster per region; data-plane topology locked |
| 0010 | Catalog replication | Accepted | Single-writer global Postgres + regional read replicas via logical replication |
| 0011 | Cross-shard 2PC | Accepted | Deferred; locked architecturally; gated on customer demand |
| 0012 | Change event primitive | Accepted | ChangeEventSink trait; declarative lifecycle + reactors replace PL/pgSQL triggers |
| 0013 | Auth per-project schema | Accepted | Removes loopback pgwire; auth data in per-project storage; auth.uid/role/jwt |
| 0014 | pg_query as canonical parser | Accepted, in progress | libpg_query frontend; sqlparser-rs demoted to transitional fallback; DataFusion-sql to executor-only |
| 0015 | Vortex storage format | Accepted | Vortex default since 2026-05-18; Parquet first-class selectable; ~1.95× smaller, on-par-to-better scan |
| 0016 | HTAP hot tier architecture | Accepted 2026-05-19 | Row-format LSM memtable (`basin-hottier` crate) per (project, table); closes OLTP point_eq + single-row UPDATE gap; 6 sub-items C1-C6 in TASK.md Phase 5.14.C; schema-evolution policy addendum 2026-05-19 |
| 0017 | Query-shape stats privacy + stability | Accepted 2026-05-19 | xxhash3 64-bit seeded; literal-stripping at LogicalPlan; per-customer template uses real names; cross-project template anonymised (`t1.c0` form); k-anonymity ≥ 5; ADR for Phase 5.16 |
| 0018 | Subsystem feature flags + minimal-build target | Proposed 2026-05-19 | Gate optional subsystems (auth, rest, webhooks, future realtime/wasm-udf) behind Cargo features; OSS users get a minimal pgwire-only binary; basin-cloud and default OSS build keep the kitchen sink |
| 0019 | Declarative BaaS surface: inbound webhooks + RPC mount | Accepted 2026-05-19 | Close the BaaS gap with CREATE INBOUND WEBHOOK and POST /rpc/<fn> composing with existing reactors; no V8/Deno edge-function runtime; covers ~95% of edge-function use cases as declarative SQL |
| 0020 | WAL transaction markers + replay suppression | Accepted 2026-05-19 | WAL adopts explicit Begin/Commit/Rollback markers for HTAP memtable integration; replay discards entries inside rolled-back or crash-interrupted transactions; pre-marker WAL files replay identically |
| 0021 | YAML frontmatter as the docs metadata contract | Accepted 2026-05-20 | All `docs/**/*.md` files carry YAML frontmatter (title, nav_section, sidebar_position, summary, optional version bounds); MDX and sidecar TOML rejected; enables basin-cloud fetch pipeline and CI lint |
| 0021 | Object storage (catalog-backed blobs) | Accepted 2026-05-20 | Supabase-style blobs as `storage.objects` rows; RLS-gated; bytes in object_store; HMAC signed URLs; new basin-blob crate. (NOTE: number collides with the frontmatter ADR above — pre-existing dup from parallel authoring; renumber one in a cleanup pass.) |
| 0022 | System-schema namespacing | Accepted 2026-05-20 | Reserved system schemas (auth/storage/cron/net/realtime/public/pg_catalog/information_schema) made first-class with `(schema,table)` keying + real search_path + honest introspection, replacing prefix hacks; user-defined schemas stay flat-aliased to public (projects already own project-membership). Phase 5.18 |
| 0023 | Lease-based ownership + partition routing + heartbeat budgets | Accepted 2026-05-21 | Per-(project,partition) ownership becomes a lease in catalog Postgres; stateless replicas + partition-level routing + heartbeat-reconciled budgets fix hot-project pinning AND multi-instance cap-bypass in one architecture. No Raft, no distributed counters on hot path, no separate budget service — Postgres CAS + epoch fencing. **TOP PRIORITY** Phase 6.X. Driven by the 2026-05-21 noisy-neighbor audit |
| 0024 | UUID-as-Decimal256 storage encoding | Accepted 2026-05-21 | Single-boundary translation at basin-storage ↔ Vortex: UUID `FixedSizeBinary(16)` round-trips as `Decimal256(39, 0)` (BE unsigned magnitude, left-padded to 32 bytes); engine, planner, pgwire, REST never see it. Workaround for Vortex's missing `FixedSizeBinary(N)` encoder (cluster C1 of #40). Gated on cluster C4 (BASIN_TYPE field-metadata round-trip). Removed when upstream Vortex PR lands. **PRIORITY** Phase 6.TR |
