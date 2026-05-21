---
title: "Basin documentation index"
nav_section: overview
sidebar_position: 0
summary: "Top-level navigation for Basin's OSS documentation. Both for humans browsing on GitHub and for basin-cloud's build-time fetcher."
---

# Basin documentation

Navigation root for the Basin OSS repo. Every file is organised by
`nav_section` declared in its YAML frontmatter
(see [`frontmatter-spec.md`](./frontmatter-spec.md)).
basin-cloud's build-time fetcher uses this index to assemble the
unified nav tree; GitHub renders it for humans browsing `docs/`.

## Architecture

- [Basin architecture](architecture.md) — Four-layer design: router, shard owners, WAL service, storage. Reference doc for how the code is laid out.
- [Multi-project SaaS on Basin](multi-project.md) — Tenancy model and isolation primitives for thousands of projects on one cluster.

## SQL

- [Basin SQL compatibility](sql-compatibility.md) — What Postgres SQL Basin accepts, what it doesn't, and the design rationale for each gap.

## Deployment

- [Deployment and cloud architecture](deployment.md) — How to run Basin in production: storage backends, deployment topologies, configuration.

## Operations

- [HTAP guide — hybrid transactional + analytical queries in Basin](htap-guide.md) — Hot-tier + columnar storage for point queries, recent writes, and aggregates in one engine. When to declare basin.sort_by, when to ALTER PROJECT memtable caps, how to read the latency story.
- [Wasm functions: authoring, ABI, deploy, limits](functions.md) — TypeScript HTTP handlers compiled to WebAssembly and run inside Basin under per-invocation CPU, memory, wall-clock, and per-project concurrency caps. Authoring, host ABI, deploy, limits.
- [Scaling: object storage](scaling/object-storage.md) — How Basin scales storage and what to expect from S3/GCS/MinIO at different sizes.
- [Scaling: read replicas](scaling/read-replicas.md) — Read-replica architecture, replication lag, fail-over story, and what's deferred to Phase 6.
- [Lease ownership — operator runbook](operators/lease-ownership.md) — Day-2 ops guide for ADR 0023 lease-based ownership: how it works, how to query lease state, how to rebalance hot replicas, when to bump partition count, and the stuck-lease incident playbook.
- [Scaling: shard rebalancing](scaling/shard-rebalance.md) — How shards split, merge, and migrate; the operator-visible touchpoints.

## Architecture decisions (ADRs)

- [ADR index](decisions/README.md)
- [ADR 0001 — Single-region only (superseded in part)](decisions/0001-single-region-only.md) — ADR 0001: Single-region only (superseded in part). See body for status, context, decision, consequences.
- [ADR 0002 — No Postgres extensions](decisions/0002-no-postgres-extensions.md) — ADR 0002: No Postgres extensions. See body for status, context, decision, consequences.
- [ADR 0003 — Native vector search](decisions/0003-native-vector-search.md) — ADR 0003: Native vector search. See body for status, context, decision, consequences.
- [ADR 0004 — Multi-region read replicas](decisions/0004-multi-region-read-replicas.md) — ADR 0004: Multi-region read replicas. See body for status, context, decision, consequences.
- [ADR 0005 — Auth system](decisions/0005-auth-system.md) — ADR 0005: Auth system. See body for status, context, decision, consequences.
- [ADR 0006 — REST API layer](decisions/0006-rest-api-layer.md) — ADR 0006: REST API layer. See body for status, context, decision, consequences.
- [ADR 0007 — Connection pooling](decisions/0007-connection-pooling.md) — ADR 0007: Connection pooling. See body for status, context, decision, consequences.
- [ADR 0008 — Noisy-neighbor fairness](decisions/0008-noisy-neighbor-fairness.md) — ADR 0008: Noisy-neighbor fairness. See body for status, context, decision, consequences.
- [ADR 0009 — Multi-region architecture](decisions/0009-multi-region-architecture.md) — ADR 0009: Multi-region architecture. See body for status, context, decision, consequences.
- [ADR 0010 — Catalog replication](decisions/0010-catalog-replication.md) — ADR 0010: Catalog replication. See body for status, context, decision, consequences.
- [ADR 0011 — Cross-shard 2PC](decisions/0011-cross-shard-2pc.md) — ADR 0011: Cross-shard 2PC. See body for status, context, decision, consequences.
- [ADR 0012 — Change-event primitive](decisions/0012-change-event-primitive.md) — ADR 0012: Change-event primitive. See body for status, context, decision, consequences.
- [ADR 0013 — Auth per-project schema](decisions/0013-auth-per-project-schema.md) — ADR 0013: Auth per-project schema. See body for status, context, decision, consequences.
- [ADR 0014 — pg_query as canonical parser](decisions/0014-pg-query-as-canonical-parser.md) — ADR 0014: pg_query as canonical parser. See body for status, context, decision, consequences.
- [ADR 0015 — Vortex storage format (default)](decisions/0015-vortex-storage-format.md) — ADR 0015: Vortex storage format (default). See body for status, context, decision, consequences.
- [ADR 0016 — HTAP hot tier architecture](decisions/0016-htap-hot-tier-architecture.md) — Row-format LSM-style memtable for recent writes, in a new basin-hottier crate. Closes the OLTP gap (point_eq, single-row UPDATE) without sacrificing OLAP wins on Vortex.
- [ADR 0017 — Query-shape stats: privacy + cross-process stability](decisions/0017-query-shape-privacy.md) — Phase 5.16 exports per-query-shape stats from OSS basin to basin-cloud for cross-customer analytics. Privacy invariants, hash-function stability, and template anonymisation rules live here.
- [ADR 0018 — Subsystem feature flags + minimal-build target](decisions/0018-subsystem-feature-flags.md) — Gate optional subsystems behind Cargo features so OSS users can ship a minimal pgwire-only binary; basin-cloud and the default OSS build keep the full feature set.
- [ADR 0019 — Declarative BaaS surface: inbound webhooks + RPC mount](decisions/0019-declarative-baas-surface.md) — Two declarative primitives — CREATE INBOUND WEBHOOK and POST /rpc/<fn> — cover ~95% of edge-function use cases as SQL, with no V8/Deno runtime or language sandbox.
- [ADR 0020 — Auth v2: OAuth providers + MFA](decisions/0020-auth-v2-oauth-mfa.md) — Lifts ADR 0005's OAuth + MFA deferral. OSS basin-auth ships OAuth2/OIDC (provider presets + generic OIDC config) and MFA (TOTP + WebAuthn/passkeys together), with an AAL claim in the JWT. Cloud builds provider-registration UI only — no new primitives.
- [ADR 0020 — WAL transaction markers + replay suppression](decisions/0020-wal-transaction-markers.md) — WAL adopts explicit Begin/Commit/Rollback markers for HTAP memtable integration. Replay discards entries inside rolled-back or crash-interrupted transactions. Pre-marker WAL files replay identically.
- [ADR 0021 — YAML frontmatter as the docs metadata contract](decisions/0021-docs-frontmatter-yaml.md) — All markdown files under docs/ carry YAML frontmatter with a fixed schema (title, nav_section, sidebar_position, summary, optional version bounds). MDX and sidecar TOML were considered and rejected.
- [ADR 0021 — Object storage (catalog-backed blobs)](decisions/0021-object-storage.md) — Supabase-style blob storage. Objects are rows in a storage.objects system table, access control reuses the RLS engine, bytes live in the same object_store the engine uses, signed URLs are HMAC over (path, expiry). New basin-blob crate. Cloud builds quota/billing/CDN/image-transforms.
- [ADR 0022 — System-schema namespacing (reserved schemas first-class; user schemas stay flat)](decisions/0022-system-schema-namespacing.md) — Make the system namespaces (auth, storage, cron, net, realtime, public, pg_catalog, information_schema) real reserved schemas with honest (schema, table) keying + introspection + search_path. User-defined schemas stay flat-aliased to public — projects already own the tenancy/isolation axis, so arbitrary user schemas are a redundant second isolation boundary with ~zero wedge benefit.
- [ADR 0023 — Lease-based ownership + partition-level routing + heartbeat budgets](decisions/0023-leases-and-partition-routing.md) — Convert per-(project,partition) ownership from a hash on ProjectId into a lease in the catalog Postgres. Stateless replicas + partition-level routing + heartbeat-reconciled budgets fix hot-tenant pinning and multi-instance cap-bypass in one architecture, without a central coordinator service or distributed counters on the hot path. The architectural commitment for Basin's multi-replica scale-out.

## Meta

- [Frontmatter spec for Basin OSS docs](frontmatter-spec.md) — YAML frontmatter contract every markdown file under docs/ obeys, so basin-cloud can fetch + render the whole OSS doc set.
- [basin-cloud roadmap spec (OSS-side companion)](basin-cloud-roadmap.md) — Cloud-side items that pair with OSS phases. Lives in this OSS repo as a forward-spec; the bas-in/basin-cloud repo is the actual home and adopts these items into its own roadmap.
- [Phase 5.14 remaining + 5.16 — wave decomposition (2026-05-19)](roadmap/2026-05-19-decomposition.md) — Five-wave execution plan for Phase 5.14 HTAP + Phase 5.16 Query Insights, decomposed into file-disjoint sonnet-agent-ready specs by an opus decomposer on 2026-05-19.
- [basin-cli v0.1 design spec](basin-cli-design.md) — Forward-spec for basin-cli — operator daily-driver Go CLI. Lives in bas-in/basin-cli when that repo is bootstrapped. This is the design contract.
- [basin-js v0.1 design spec](basin-js-design.md) — Forward-spec for basin-js — TypeScript client SDK. Supabase-shaped API; talks pgwire/REST/WebSocket directly to a Basin engine. MIT-licensed.
- [WebSocket subscriptions — design spec](websocket-subscription-design.md) — Forward-spec for basin-rest /realtime/v1/subscribe. Tails the change-event primitive (ADR 0012); emits JSON over a multiplexed single-socket protocol. Realtime parity with Supabase.
- [Phase 0 customer interview script — wedge validation](customer-interview-script.md) — 5-10 founder interviews to validate the Basin wedge (multi-tenant Postgres-compat HTAP) before committing 3-6 months to basin-cloud. Question bank, facilitation guide, scoring rubric, pivot triggers.

## 

- [Noisy-neighbor / fairness audit — single-instance + load-balanced](audits/2026-05-21-noisy-neighbor-fairness.md)

## Project-level documents

These live at the repo root, not under `docs/`:

- [`../README.md`](../README.md) — project intro and landing page
- [`../CAPABILITIES.md`](../CAPABILITIES.md) — capability matrix (shipped, in-progress, planned, off-roadmap)
- [`../CHANGELOG.md`](../CHANGELOG.md) — release-by-release shipped changes
- [`../WEDGE.md`](../WEDGE.md) — six-month prioritised roadmap
- [`../TASK.md`](../TASK.md) — full Phase 0–7 build plan, agent-decomposed
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — contribution guide

---

_This file is generated by [`scripts/build-docs-index.sh`](../scripts/build-docs-index.sh)._
_Re-run the script after adding or editing a doc's frontmatter._
