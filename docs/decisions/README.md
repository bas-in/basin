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
| 0007 | Connection pooling | Accepted | Native TenantSession cache; per-tenant cap; LRU eviction |
| 0008 | Noisy-neighbor fairness | Accepted | EDF scheduler; priority by op-shape; p99 13.97 ms under load |
| 0009 | Multi-region architecture | Accepted | One cluster per region; data-plane topology locked |
| 0010 | Catalog replication | Accepted | Single-writer global Postgres + regional read replicas via logical replication |
| 0011 | Cross-shard 2PC | Accepted | Deferred; locked architecturally; gated on customer demand |
| 0012 | Change event primitive | Accepted | ChangeEventSink trait; declarative lifecycle + reactors replace PL/pgSQL triggers |
| 0013 | Auth per-tenant schema | Accepted | Removes loopback pgwire; auth data in per-tenant storage; auth.uid/role/jwt |
