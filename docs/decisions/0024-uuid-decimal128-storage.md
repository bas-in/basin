---
title: "ADR 0024 — UUID-as-Decimal128 storage encoding (C1 workaround pending Vortex FixedSizeBinary support)"
nav_section: decisions
sidebar_position: 24
summary: "Until Vortex supports FixedSizeBinary(N) encoding, basin-storage transparently translates UUID columns (Arrow FixedSizeBinary(16)) to Decimal128(38, 0) at the storage boundary on write and back on read, using a per-column `BASIN_TYPE=\"uuid\"` sidecar in field metadata. The translation is invisible above the storage trait — engine, planner, pgwire, REST all keep seeing UUIDs. When the upstream Vortex PR lands, the translation layer is deleted with zero callsite churn."
tags: [storage, types, vortex, workaround]
---

# 0024 — UUID-as-Decimal128 storage encoding

- **Status:** Accepted, 2026-05-21.
- **Tags:** storage, types, vortex, workaround
- **Driving issue:** triage cluster C1 in [task #40] — three tests blocked
  on Vortex's missing `FixedSizeBinary(16)` encoder
  (`jsonb_uuid_param_binding`, `smoke_pgx`, `viability_uuid`).
- **Strengthens:** [ADR 0015 (Vortex storage format)](./0015-vortex-storage-format.md).
- **Hard prerequisite:** triage cluster C4 — `BASIN_TYPE` field-level
  metadata round-trip through storage must work first; without it the
  read path cannot tell a UUID-disguised Decimal128 from a genuine
  `NUMERIC(38, 0)` column. C4 is on the same task #40.

## Context

UUID is the default primary-key type for Basin-hosted apps. Arrow
represents UUID as `FixedSizeBinary(16)`. Vortex (Basin's default
storage format since 2026-05-18, ADR 0015) currently has no encoder for
`FixedSizeBinary(N)`. This blocks any table containing a UUID column
from being written to Vortex.

Three workaround paths exist (see also the 2026-05-21 chat with the
maintainer, captured in `decisions.md`):

**(a) Upstream a `FixedSizeBinary(N)` encoder to vortex-data/vortex.**
Cleanest long-term, but 2-4 weeks of upstream lag at minimum (PR review,
release cut, dep pin in Basin). Vortex is actively maintained; an
upstream PR is realistic but slow.

**(b) Lower UUID → Int128-shaped column at the storage boundary.**
basin-storage transparently converts. Engine, planner, pgwire, and REST
keep seeing UUIDs. Hidden below the storage trait.

**(c) Per-table Parquet fallback for any table containing a UUID
column.** Surgical workaround but means UUID-heavy tables (i.e. most
tables) lose every Vortex benefit.

## Decision

Adopt **(b)** until **(a)** lands. Specifically:

### Physical representation: `Decimal128(38, 0)`

Vortex already supports `Decimal128`. The 16 raw UUID bytes are
interpreted as the **unsigned big-endian magnitude** of a 128-bit
decimal, scale = 0.

This representation has two load-bearing properties:

1. **Total order match.** PostgreSQL sorts UUIDs **bytewise**.
   Big-endian unsigned-magnitude `Decimal128` comparison produces the
   identical total order. `ORDER BY uuid_col`, `WHERE uuid_col > $1`,
   and range index lookups all behave correctly without any planner
   coercion.
2. **Equality is native.** Two UUIDs are equal iff their Decimal128
   representations are equal — no per-row callback required.

### Where the translation lives — single boundary

Translation lives **only** in basin-storage, at the Arrow-to-Vortex and
Vortex-to-Arrow boundaries:

- **Write path** (`basin-storage::vortex::write_batch`-equivalent): walk
  the `RecordBatch` schema; for every column whose Arrow type is
  `FixedSizeBinary(16)` *and* whose field metadata has `BASIN_TYPE="uuid"`,
  reinterpret the 16-byte buffer as `Decimal128(38, 0)` (big-endian
  magnitude) before handing to Vortex. Preserve the `BASIN_TYPE` sidecar
  in the written schema so the read path can recognize it.
- **Read path** (`basin-storage::vortex::read_batch`-equivalent): walk
  the schema returned by Vortex; for every column whose Arrow type is
  `Decimal128(38, 0)` *and* whose field metadata has `BASIN_TYPE="uuid"`,
  reinterpret the 128-bit magnitude back to `FixedSizeBinary(16)` for the
  caller (basin-engine).

basin-engine, basin-router, the executor, pgwire encode/decode, REST
serialization — **none of these change**. They keep treating UUID as
`FixedSizeBinary(16)` because that is what they receive from the storage
trait above the translation layer.

### Endianness

Big-endian unsigned. PostgreSQL's wire format for UUID is the canonical
8-4-4-4-12 hex form serialized as 16 bytes in the obvious order; this
matches Arrow's `FixedSizeBinary(16)` byte layout; reinterpreted as
unsigned BE produces a magnitude whose sort order is bytewise. Little-
endian would invert sort order and break range queries.

### What survives the translation layer's eventual removal

When the upstream Vortex `FixedSizeBinary(N)` PR lands and we pin the
new version:

1. Delete the read-path and write-path conditional branches in
   basin-storage that key on `BASIN_TYPE="uuid"` + `Decimal128`.
2. Existing tables already on disk: data files stay as Decimal128. A
   small read-path back-compat shim (or a compactor pass that rewrites)
   handles the transition. The `BASIN_TYPE` sidecar is the unambiguous
   marker — we can tell old files from new.
3. Engine, pgwire, REST unchanged — they never knew.

No schema migration for live tables, no API break. The translation layer
removal is a basin-storage-internal refactor.

## Consequences

**Positive**

- Three test clusters unblock (jsonb_uuid_param_binding, smoke_pgx,
  viability_uuid) within a day of C4 closing.
- Engine and protocol layers stay UUID-native — no leaky abstraction.
- Removal path is mechanical when upstream Vortex catches up — no
  ecosystem-wide refactor.
- Compose with C4's BASIN_TYPE sidecar work: it pays for itself across
  MONEY, INET, CIDR, MACADDR, MACADDR8, BIT, VARBIT (the 7 extra_types
  tests also blocked on C4).

**Negative / accepted trade-offs**

- Per-row storage overhead: `Decimal128` is 16 bytes; `FixedSizeBinary(16)`
  would also be 16 bytes. **No per-row overhead** in the steady state.
  Some Decimal128 codecs may have small metadata overhead per RecordBatch
  but it's bounded and amortized.
- A second pair of conditional branches in basin-storage to maintain
  until upstream lands. Tagged with a `TODO(adr-0024)` so it's easy to
  find when the trigger fires.
- Coupling: C1 cannot ship before C4. Acceptable — C4 is the higher-
  value of the two (closes 7 tests vs C1's 3) and is on the same task.

## Architectural compatibility

What today's design preserves so we can flip later: the engine never
sees the `Decimal128` representation. Removing the storage-layer
translation is a no-op for every other crate. The `BASIN_TYPE` sidecar
is also the right mechanism for any future logical-type-on-physical-
storage decision (e.g., compact JSON, IPv6 as `FixedSizeBinary(16)` —
which would share the C1 fix), so it's not single-purpose plumbing.

## Trigger to reconsider

Either: (a) upstream Vortex ships a `FixedSizeBinary(N)` encoder + a
release we can pin to. Or: (b) a measurable performance gap between
Decimal128-as-UUID and native-FixedSizeBinary surfaces under real
workload (e.g., > 10% scan-throughput delta on a UUID-keyed table at
≥ 100M rows). At that point write a successor ADR and remove the
translation per the steady-state plan above.

## Alternatives considered and why we didn't pick them

- **(a) Upstream PR first.** Right long-term, wrong for "ship now":
  blocks 3 tests for weeks. We'll do (a) in parallel as a separate
  effort — this ADR only commits to the workaround.
- **(b'') Lower to `Struct<hi: Int64, lo: Int64>`.** Same logical idea
  but worse: comparison and equality need custom physical-expr handling
  in DataFusion, and the planner has to know about it. Decimal128 keeps
  native operator semantics.
- **(b''') Lower to varlen `Binary`.** Loses fixed-size memory layout,
  costs varlen offset overhead per row (≥ 4 extra bytes), needs a
  length-check on every read.
- **(c) Per-table Parquet fallback.** Defeats the Vortex moat for every
  UUID-keyed table — i.e. most tables.
- **Skip UUIDs entirely (require text PKs).** No.

## Cross-references

- [ADR 0015 — Vortex storage format](./0015-vortex-storage-format.md)
  — Vortex is the default; this ADR is the workaround for one of its
  current limitations.
- Task #40 — Engine bug clusters from #39 triage — C1 (this ADR's
  scope) and C4 (its prerequisite) both originate from the post-Phase-6.X
  cargo-test triage.
- `decisions.md` 2026-05-21 entries for the triage and the cluster
  breakdown.
