---
title: "ADR 0017 — Query-shape stats: privacy + cross-process stability"
nav_section: decisions
sidebar_position: 17
summary: "Phase 5.16 exports per-query-shape stats from OSS basin to basin-cloud for cross-customer analytics. Privacy invariants, hash-function stability, and template anonymisation rules live here."
tags: [observability, privacy, security]
---

# 0017 — Query-shape stats: privacy + cross-process stability

- **Status:** Accepted (2026-05-19) — implementation Phase 5.16.A–D (OSS) and 5.16.E–H (basin-cloud), see [TASK.md](../../TASK.md) Phase 5.16 + [basin-cloud-roadmap.md](../basin-cloud-roadmap.md).
- **Tags:** observability, privacy, security, htap
- **Supersedes:** none
- **Cross-references:** [ADR 0013 (auth per-project schema)](./0013-auth-per-project-schema.md), [ADR 0016 (HTAP hot tier)](./0016-htap-hot-tier-architecture.md)

## Context

Phase 5.16 introduces per-query-shape telemetry: every executed query
gets a canonical plan-shape hash, then `QueryStatRegistry` aggregates
per-shape p50/p95/p99 latency, rows scanned, files opened, bytes
decoded.  These records flow over OTLP to basin-cloud, which renders
per-customer "Query Insights" UI and Basin-engineer-only anonymised
cross-project aggregates.

Two correctness questions:

1. **Privacy.** What customer-identifying data can leak — directly via
   exported records, or indirectly via cross-project aggregation?
2. **Cross-process / cross-version stability.** The shape hash is the
   join key across the OSS → cloud pipeline.  If `hash(plan)` differs
   between Rust versions or between OSS basin instances, the cloud's
   shape catalog corrupts the moment any node upgrades.

## Decision

### Hash function: xxhash3, 64-bit

OSS basin computes `QueryShapeHash` via the `xxhash-rust` crate's
`xxh3_64`, seeded with a fixed seed `BASIN_QUERY_SHAPE_SEED = 0xBA51_4145_7E11_5A95`
(arbitrary 64-bit constant; documented and immutable).

Rejected alternatives:

- **`std::hash::DefaultHasher`** — explicitly documented as unstable
  across Rust versions.  Two basin nodes at different patch levels
  would produce different hashes for the same plan.  Unacceptable.
- **`fnv1a-64`** — stable and fast but weak; at cloud scale
  (≥ 10 k projects × 500 shapes ≈ 5 M distinct shapes) collision
  probability rises into the measurable range.
- **`siphash13`** — what fastbloom uses; stable and strong but ~3 ×
  slower than xxhash3.  Reserved as fallback if xxhash-rust ever
  becomes problematic.
- **`blake3`** — cryptographic; overkill for non-adversarial hashing.

Collision math at cloud scale: 5 M shapes, 64-bit hash → birthday
probability ≈ (5 M)² / 2⁶⁵ ≈ 1.4 × 10⁻⁷.  Practically zero.

### Privacy invariants

Three layers, ordered by sensitivity:

**Layer 1 — In-process registry (OSS basin, single process).**
The `QueryStatRegistry` stores per-shape stats keyed by `(ProjectId,
QueryShapeHash)`.  Each shape entry carries:

- 64-bit hash
- canonical normalised plan template (real table / column names; literal
  slots as `$1`/`$2`/…)
- HDR histograms (latency µs, rows scanned, files opened, bytes decoded)
- monotonic counters

**No literal values appear anywhere in registry state.** Literal
stripping happens at hash-computation time (Phase 5.16.A) by replacing
every `Expr::Literal` node with `Expr::LiteralSlot(DataType)` before
the plan walk reaches the hash sink.  This is the bedrock invariant —
without it, every other privacy guarantee unravels.

**Layer 2 — Per-customer OTLP export (OSS → cloud, single project view).**
The basin-cloud Query Insights UI for a customer shows that customer
their own real schema names: `customers.email_hash`, `orders.amount`.
This is correct: a customer must be able to read shape templates to
act on them.  Exported record carries:

- 64-bit hash
- plan template with real table / column names
- histograms + counters
- timestamp

**No customer's records are ever shown to a different customer.** Per-
project scoping is enforced at the cloud ingest pipeline by the
`ProjectId` field on every record.

**Layer 3 — Cross-project aggregates (Basin engineers only).**
For roadmap evidence ("X % of customers run shape Y at > 100 GB table
size") basin-cloud surfaces aggregate views to Basin engineers.  These:

- Use the 64-bit hash + an **anonymised template** with table / column
  names replaced by positional placeholders (`t1`, `t1.c0`, `t1.c1`,
  `t2.c0`, …).  Anonymisation happens at the cloud ingest stage by
  walking the imported template AST and renaming.
- Apply k-anonymity (k ≥ 5): no aggregate is shown for fewer than 5
  contributing customers.  Enforced at query time, not just at display
  time, so an internal user cannot decompose an aggregate by adding
  filters.
- Are gated behind a Basin-engineer role distinct from any customer
  role; audit-logged on every access.

### What is never exported

- Literal values in WHERE / VALUES / SET clauses
- Customer's row data
- pgwire raw SQL text
- Project / table / column names in cross-project aggregates (only in
  the per-customer view)

### What may be exported

- Plan-shape hash (xxh3_64 of canonical literal-stripped plan)
- Plan template (real names in per-customer; positional placeholders
  in cross-project)
- HDR histograms
- Counters
- Aggregate metadata (number of files, table-size bucket — but not
  table identity in cross-project view)

### Opt-out

OSS basin reads `BASIN_QUERY_STATS_EXPORT_DISABLED=1` to suppress all
OTLP export.  `QueryStatRegistry` continues to populate in-process
(needed for `basin_stat_statements` SQL view).  Self-hosted users who
do not connect to basin-cloud get the local SQL view with zero
external traffic.

## Alternatives considered

- **128-bit hash.** Would push collision probability to genuinely
  cryptographic levels.  Rejected — the marginal safety is not worth
  the wire-size doubling.  64-bit at 1.4 × 10⁻⁷ collision rate is
  already several orders of magnitude below "would ever matter".
- **Per-customer hash salt.** Would make cross-project aggregation
  impossible by design.  Rejected — Phase 5.16's value proposition is
  precisely the cross-customer evidence loop.  k-anonymity is the right
  privacy tool for that goal, not unsalvageable hash separation.
- **Cryptographic plan-template encoding.** Customer-side encryption
  of the template before export.  Rejected — Basin engineers can't
  diagnose cloud-aggregate slowdowns without seeing structure.  The
  anonymisation step at the cloud ingest stage gives equivalent
  protection without locking out the operations team.

## What would change our mind

- **A customer or regulator flags table / column name as PII** in the
  per-customer view → add an opt-in flag to anonymise per-customer
  templates too (cloud-side change only; OSS export unaffected).
- **xxhash-rust crate becomes unmaintained** → switch to `siphash13`
  (slower but maintained-by-the-Rust-foundation via siphasher).  The
  on-the-wire format is unaffected because the hash is bound to its
  fixed seed; the export already carries the hash bytes verbatim.
- **k-anonymity ≥ 5 proves insufficient** (e.g. small-population
  customer cohort identifiable by query shape pattern) → raise to
  k ≥ 10 or add ε-differential-privacy noise to histogram bins.
- **A non-Postgres backend** appears in Basin (currently impossible per
  ADR 0002; would be Phase 6+ if ever) → re-evaluate plan-template
  canonical form.
