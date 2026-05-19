---
title: "Basin documentation index"
nav_section: overview
sidebar_position: 0
summary: "Top-level navigation for Basin's OSS documentation. Both for humans browsing on GitHub and for basin-cloud's build-time fetcher."
---

# Basin documentation

This is the documentation root for the Basin OSS repo. Files are
organised by `nav_section` (declared in each file's YAML frontmatter
per [`frontmatter-spec.md`](./frontmatter-spec.md)).

For the live rendered site that combines this repo's docs with
`basin-js`, `basin-cli`, and `basin-cloud`-specific content, see
basin-cloud (Phase 5.15 — currently in flight; until that ships,
GitHub renders these files directly).

## Architecture

- [Basin architecture](./architecture.md) — four-layer design (router, shard owners, WAL service, storage)
- [Multi-project SaaS on Basin](./multi-project.md) — tenancy model and isolation primitives

## Storage

- [ADR 0015 — Vortex storage format (default)](./decisions/0015-vortex-storage-format.md)

## SQL

- [Basin SQL compatibility](./sql-compatibility.md) — what Postgres SQL Basin accepts and why
- [SQL support matrix (auto-generated)](./sql-support.md) — per-syntax pass/fail from `sql_support_matrix` tests

## Deployment

- [Deployment and cloud architecture](./deployment.md) — running Basin in production

## Operations

- [Scaling: object storage](./scaling/object-storage.md)
- [Scaling: read replicas](./scaling/read-replicas.md)
- [Scaling: shard rebalancing](./scaling/shard-rebalance.md)

## Decisions (ADRs)

- [ADR index](./decisions/README.md) — full index of architectural decision records

## Meta

- [Frontmatter spec](./frontmatter-spec.md) — YAML contract every doc obeys (Phase 5.15.A)

## Project-level documents

These live at the repo root, not under `docs/`:

- [`../README.md`](../README.md) — project intro and landing page
- [`../CAPABILITIES.md`](../CAPABILITIES.md) — capability matrix (what's shipped, in-progress, planned, off-roadmap)
- [`../CHANGELOG.md`](../CHANGELOG.md) — release-by-release shipped changes
- [`../WEDGE.md`](../WEDGE.md) — six-month prioritised roadmap (current #1 is Phase 5.15 — unified docs platform)
- [`../TASK.md`](../TASK.md) — full Phase 0–7 build plan, agent-decomposed
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — contribution guide
