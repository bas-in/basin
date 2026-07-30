---
title: "ADR 0025 — Postgres-compatibility surface decisions: JSONB GIN indexing, full-text search, citext, and session timeouts (Phases 5.19–5.30)"
nav_section: decisions
sidebar_position: 25
summary: "Shared patterns behind four Postgres-compat features that landed together: the BASIN_TYPE metadata sidecar, GIN posting lists shared by JSONB and FTS, PG-accurate SQLSTATEs, test-first harnesses."
tags: [pg-compat, gin, fts, citext, timeouts, types, engine]
---

# 0025 — Postgres-compatibility surface decisions: JSONB GIN indexing, full-text search, citext, and session timeouts (Phases 5.19–5.30)

- **Status:** Accepted, 2026-05-21.
- **Tags:** pg-compat, gin, fts, citext, timeouts, types, engine
- **Strengthens:**
  [ADR 0024 (UUID-as-Decimal256 storage encoding)](./0024-uuid-decimal128-storage.md) —
  the `BASIN_TYPE` sidecar introduced there is extended here to cover
  `TSVECTOR`, `TSQUERY`, and `CITEXT`.
- **Related:** ADR 0014 (pg-query as canonical parser), ADR 0015 (Vortex storage
  format).

## Context

Four Postgres-compatibility features landed in close succession during Phases
5.19–5.30. Each touched the same layers (DDL parser → catalog → engine →
pgwire) and each made the same architectural choices independently. This ADR
captures the shared patterns so future contributors apply them consistently
rather than re-deriving them.

The four features are:

1. **JSONB GIN indexing (Phase 5.19)** — `CREATE INDEX … USING gin (col
   [jsonb_path_ops])`.
2. **Full-text search (Phase 5.20)** — `TSVECTOR` / `TSQUERY` types, `@@`,
   `to_tsvector`, `ts_rank`, Snowball English stemming.
3. **citext (Phase 5.30)** — case-insensitive text with UNIQUE folding.
4. **Session timeouts (Phase 5.28)** — `lock_timeout` (SQLSTATE 55P03) and
   `idle_in_transaction_session_timeout` (SQLSTATE 25P03), complementing the
   already-wired `statement_timeout`.

## Decision

### 1. JSONB GIN indexing (Phase 5.19)

**Catalog representation.** The `SecondaryIndex` struct in
`basin-catalog/src/metadata.rs` was extended with two new fields:

```
access_method: String  // "btree" (default) | "gin"
opclass: Option<String> // None for btree; Some("jsonb_ops") or Some("jsonb_path_ops") for GIN
```

Both fields are `#[serde(default)]` so all existing catalog payloads
deserialise cleanly as B-tree indexes. The DDL parser (`basin-engine/src/ddl.rs`)
maps `USING GIN` → `access_method = "gin"` and the optional opclass keyword
to the `opclass` field.

**Two opclasses.** Both match PostgreSQL semantics:

- `jsonb_ops` (default): indexes every top-level `key` presence and
  `key=value` pair, enabling `@>` and `<@` over any key or value.
- `jsonb_path_ops`: hashes each root-to-leaf path into a stable
  `path_hash:<fnv64>` term, producing a more selective but narrower index
  (only `@>` on full paths).

Term extraction and AND-merge logic lives in
`basin-engine/src/index_probe.rs`, which also hosts the in-memory
`GinIndexRegistry` (one posting list per `(project, table, col)`).

**Probe result contract.** The registry returns one of three variants:
`NoIndex` (fall through to full scan — conservative, no false negatives),
`Empty` (intersection is empty, zero rows can match), or
`FileCandidates(set)` (files that may contain matching rows; the caller
re-applies `jsonb_contains` for correctness).

**What is NOT yet wired (5.19.E, pending).** The posting-list probe is an
advisory candidate-prune only. The GIN registry is not yet plugged into
the DataFusion physical scan (`ListingTable` path). The ≥10× performance
gate defined in `tests/integration/tests/jsonb_index_harness.rs` is
therefore still red. The posting lists live entirely in RAM; on-disk
persistence is the Phase 5.19.E work item.

### 2. Full-text search (Phase 5.20)

**Storage representation.** `TSVECTOR` and `TSQUERY` columns are stored as
Arrow `Utf8` holding the PostgreSQL canonical text form (lexemes sorted with
positions for tsvector; `&`/`|`/`!`/`<->` boolean tree for tsquery). The
logical type is signalled by `BASIN_TYPE_TSVECTOR = "TSVECTOR"` and
`BASIN_TYPE_TSQUERY = "TSQUERY"` in Arrow field metadata, exactly as
`BASIN_TYPE_UUID = "UUID"` did for UUID columns (ADR 0024).

Constants are defined in `basin-engine/src/types.rs`; the pgwire encoder
reads the sidecar to emit the correct PG OID rather than the plain-text OID.

**UDFs registered in `basin-engine/src/fts_udf.rs`:**
- `to_tsvector([config,] text)` — tokenises, removes English stop-words
  (127-word list mirroring Snowball's English stopword set), stems with
  Snowball English (`rust-stemmers` crate, `Algorithm::English`), assigns
  1-based positions, emits PG canonical sorted form.
- `to_tsquery([config,] text)`, `plainto_tsquery`, `phraseto_tsquery`.
- `@@` — rewritten by `pg_ast::rewrite_tsvector_at_at` to
  `tsvector_match_udf(tsvector, tsquery)`, a correct boolean evaluator
  supporting AND/OR/NOT/phrase operators.
- `ts_rank` / `ts_rank_cd` — simplified deterministic score
  (matched-distinct-lexemes / vector-length); documented as a simplification
  of PG's cover-density algorithm.

**Shared GIN posting-list infrastructure.** `CREATE INDEX … USING GIN` on a
`TSVECTOR` column is accepted at DDL time and persisted in the catalog using
the same `access_method = "gin"` path as JSONB GIN. The posting-list builder
for FTS lands storage-side in Phase 5.20.E but engine-side wiring (so the
`@@` filter uses the index rather than a sequential scan) is deferred — the
scan is correct, just not fast.

**What is NOT yet wired (pending).** GIN-on-tsvector is a catalog-level
declaration only; `@@` always executes as a sequential scan. The Phase 5.20.E
posting-list builder and engine integration are future work items analogous to
the 5.19.E JSONB gap.

### 3. citext (Phase 5.30)

**Storage representation.** `CITEXT` columns are stored as Arrow `Utf8`
(original case preserved, matching PostgreSQL semantics: citext stores bytes
verbatim, case-folding only occurs at comparison time). The logical type is
signalled by `BASIN_TYPE_CITEXT = "CITEXT"` in Arrow field metadata, defined
in `basin-engine/src/types.rs`.

The pgwire encoder emits OID 25 (TEXT) for citext columns; citext is a PG
extension, not a distinct wire-level type.

**Comparison semantics.** Case-folded comparison UDFs apply `lower()` before
comparing values for equality and ordering. UNIQUE constraint enforcement
case-folds both the stored and incoming values before the collision check
(Phase 5.30.D).

**What is NOT yet wired.** Full WHERE-clause rewrite (so that a plain
`WHERE email = 'FOO@X.COM'` on a citext column is automatically treated as
case-insensitive without an explicit `::citext` cast) is deferred — marked
as 5.30.E in `tests/integration/tests/citext_harness.rs`. The harness tests
land red and are un-ignored slice-by-slice as each sub-task closes.

### 4. Session timeouts (Phase 5.28)

**`statement_timeout`.** Already wired before Phase 5.28; the executor checks
the per-session deadline at query start and cancels via DataFusion's
`TaskContext` cancellation token. SQLSTATE 57014 (`query_canceled`).

**`lock_timeout` (Phase 5.28.B).** Implemented in
`basin-shard/src/lock_wait.rs` as `bounded_lock_wait` — an async primitive
that wraps a non-blocking `try_acquire` closure in a Tokio-yield retry loop,
capping total wait by a deadline derived from the per-session `lock_timeout`
GUC. Returns `false` on deadline; the caller maps this to
`BasinError::LockNotAvailable`, which `basin-router/src/error.rs` maps to
SQLSTATE 55P03 (`lock_not_available`). A synchronous variant
(`bounded_lock_wait_sync`) is available for DataFusion UDF contexts where
`await` is unavailable.

**`idle_in_transaction_session_timeout` (Phase 5.28.C).** Implemented in
`basin-engine/src/session_reaper.rs` as a lightweight background task
(`SessionReaperRegistry::spawn`). Every `check_interval` (1 s in production,
10 ms in tests) the reaper iterates the weak-reference registry, identifies
sessions with an open transaction idle past their per-session GUC value, and
sets an `AtomicBool` (`ReapedFlag`). The executor reads this flag at the top
of every `execute()` call and returns `BasinError::IdleInTransactionTimeout`
before doing any work. `basin-router/src/error.rs` maps this to SQLSTATE
25P03 severity FATAL. Dead session entries are pruned automatically when
`Weak::upgrade` fails.

## Unifying themes

### (a) BASIN_TYPE Arrow-metadata sidecar

All four features follow the pattern established by ADR 0024 for UUID and
extended for `INET`, `CIDR`, `MACADDR`, `BIT`, and `MONEY`: the logical PG
type rides on an Arrow physical type that is compatible with the storage layer,
with the logical type identity carried as a string value under the key
`BASIN_TYPE` in Arrow `Field::metadata`. The constants are all defined in
`basin-engine/src/types.rs`. Consumers:

| Logical type | Physical Arrow type | `BASIN_TYPE` value |
|---|---|---|
| `UUID` | `FixedSizeBinary(16)` | `"UUID"` |
| `TSVECTOR` | `Utf8` | `"TSVECTOR"` |
| `TSQUERY` | `Utf8` | `"TSQUERY"` |
| `CITEXT` | `Utf8` | `"CITEXT"` |
| `INET` / `CIDR` / `MACADDR` / `MACADDR8` | `Utf8` | `"INET"` / `"CIDR"` / … |

The storage layer is type-agnostic: it reads and writes Arrow `RecordBatch`
values without inspecting `BASIN_TYPE`. The pgwire encoder, info_schema, and
comparison operators are the primary consumers.

### (b) Test-first harnesses, red → green slice-by-slice

Each feature follows the same convention:

1. A harness file lands in `tests/integration/tests/` with every test
   decorated `#[ignore = "N.NN.A harness — feature pending"]`.
2. Implementation sub-tasks (B, C, D, …) close named slices; the
   corresponding `#[ignore]` is dropped when the slice is green.
3. No test is weakened to match wrong output — the expected value is always
   the correct PostgreSQL output.

Harnesses: `jsonb_index_harness.rs` (5.19.A), `fts_harness.rs` (5.20.A),
`citext_harness.rs` (5.30.A), `timeout_trio_harness.rs` (5.28.A).

### (c) Shared GIN posting-list infrastructure

The `GinIndexRegistry` in `basin-engine/src/index_probe.rs` is designed for
both JSONB and FTS:

- Term extraction is opclass-specific (`extract_terms` for JSONB; FTS uses
  lexemes from `to_tsvector`), but the posting-list data structure
  (`TermPostingList`, AND-merge probe, LRU eviction at 500 000 entries/column)
  is shared.
- The same `SecondaryIndex.access_method = "gin"` catalog field is used for
  both JSONB and tsvector GIN indexes.
- Both features have the same pending gap: probe results are available at the
  engine layer but are not yet passed as a file-skip hint to the DataFusion
  physical scan.

### (d) PG-accurate SQLSTATE mapping

`basin-router/src/error.rs` is the single point of translation from
`BasinError` variants to pgwire `ErrorResponse` codes. The session-timeout
features added two new mappings that match PostgreSQL exactly:

- `BasinError::LockNotAvailable` → SQLSTATE `55P03` (severity ERROR)
- `BasinError::IdleInTransactionTimeout` → SQLSTATE `25P03` (severity FATAL)

These join existing accurate mappings for `23505` (unique violation),
`23514` (check violation), `40001` (serialization failure), `57014`
(query canceled / statement_timeout), and `0A000` (feature not supported).

## Consequences

**Positive**

- The `BASIN_TYPE` sidecar is now the established, documented convention for
  all logical-type-on-physical-storage decisions. New types follow the same
  three-step pattern: add a constant to `types.rs`, handle the sidecar in the
  pgwire encoder, add the DDL mapping.
- GIN posting-list infrastructure is shared, so JSONB and FTS index work
  compounds rather than duplicates effort.
- Session timeout GUCs are wired accurately; psql, DBeaver, pgcli, and ORMs
  that set these GUCs on connection will see PG-compatible behaviour.
- Test-first harnesses give new contributors a clear definition-of-done for
  each feature slice without requiring a running PG instance.

**Negative / accepted trade-offs**

- GIN physical-scan integration is not yet complete for either JSONB or FTS;
  the posting-list probe is advisory only. Tables with GIN indexes see correct
  query results but not yet the expected query-speed improvement.
- citext WHERE-clause rewrite (implicit case-folding without an explicit
  `::citext` cast) is deferred; applications must use explicit casts or rely
  on column type for comparison semantics.
- `ts_rank` / `ts_rank_cd` are simplified; they are documented as such, but
  applications relying on PG's exact cover-density scores will see different
  numeric values.
- The posting-list registry is in-process memory only; an engine restart
  clears it and the index rebuilds lazily from new writes.

## Cross-references

- [ADR 0024 — UUID-as-Decimal256 storage encoding](./0024-uuid-decimal128-storage.md)
  — origin of the `BASIN_TYPE` sidecar convention.
- [ADR 0014 — pg-query as canonical parser](./0014-pg-query-as-canonical-parser.md)
  — the SQL parsing layer that accepts `USING GIN`, `@@`, and `::citext` casts.
- `basin-engine/src/index_probe.rs` — GIN posting-list registry and probe logic.
- `basin-engine/src/fts_udf.rs` — FTS UDFs (to_tsvector, to_tsquery, @@, ts_rank).
- `basin-engine/src/types.rs` — all `BASIN_TYPE_*` constants.
- `basin-shard/src/lock_wait.rs` — `bounded_lock_wait` (lock_timeout primitive).
- `basin-engine/src/session_reaper.rs` — idle-in-transaction session reaper.
- `basin-router/src/error.rs` — single-point PG SQLSTATE mapping.
- `basin-catalog/src/metadata.rs` — `SecondaryIndex` struct with `access_method`
  and `opclass` fields.
- `tests/integration/tests/jsonb_index_harness.rs` — Phase 5.19 harness.
- `tests/integration/tests/fts_harness.rs` — Phase 5.20 harness.
- `tests/integration/tests/citext_harness.rs` — Phase 5.30 harness.
- `tests/integration/tests/timeout_trio_harness.rs` — Phase 5.28 harness.
