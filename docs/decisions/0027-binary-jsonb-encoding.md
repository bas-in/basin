---
title: "ADR 0027 — Binary / columnar JSONB representation (faster scalar extraction)"
nav_section: decisions
sidebar_position: 27
summary: "Basin's JSONB scalar extraction (->>, ->, #>, jsonb_typeof, jsonb_array_length) runs 100-2200x slower than PostgreSQL at 1M rows. Root cause: Basin stores JSONB as raw JSON text bytes and every UDF call walks those bytes per-row. This ADR decides the structural fix — a sequence of incremental improvements culminating in a hybrid binary-blob + promoted-columns representation — and records the first shippable increment: a per-batch top-level key index that eliminates redundant byte scanning when multiple UDF calls are applied to the same JSONB column in one query."
tags: [jsonb, performance, encoding, storage]
---

# 0027 — Binary / columnar JSONB representation

- **Status:** Accepted — Phase 1 (per-batch top-level index) landed; Phase 2 (write-time binary offset table) landed.
- **Tags:** jsonb, performance, encoding, storage
- **Driving issue:** `viability_jsonb` bench: `payload->>'key'`, `payload->'key'`,
  `payload #> '{a,b}'`, `jsonb_typeof`, `jsonb_array_length` measure 100–2200×
  slower than PostgreSQL 16 at 1M rows (batch size ~8192 rows, single-threaded
  scan). The `jsonb_selective_filter` suite shows the same pattern.

## Problem statement

Basin stores JSONB columns as Arrow `LargeBinary` with `BASIN_TYPE=JSONB`
metadata. The wire bytes are canonical JSON text (keys sorted, no extra
whitespace, no version prefix). Every UDF call that extracts a field walks
those bytes per row:

- **Old path (before ADR 0027):** `serde_json::from_slice → Value tree → index`
  allocates a full `HashMap`/`Vec` tree for the whole document and then discards
  it after one key lookup. For a 1 KB document extracting one string field,
  essentially all CPU is spent allocating and GC-ing nodes.

- **Current path (existing `RawJson` scanner):** Introduced as `perf(storage,engine)`
  prior to this ADR. The `RawJson` cursor walks raw JSON bytes to locate
  a key's value byte-slice without materialising sibling nodes. This is
  meaningfully faster for single-field extraction, but still O(doc-size) per
  row per UDF call.

- **Remaining gap:** A query `SELECT payload->>'a', payload->>'b', payload->>'c'`
  causes DataFusion to invoke `json_get_text` three times on the same batch.
  Each invocation independently scans every row's bytes. For `k` UDF calls on a
  batch of `n` rows with documents of average size `d` bytes, the work is
  `O(k × n × d)`. With `k = 3`, `n = 8192`, `d = 256 B` that is ~6 MB of byte
  scanning per batch just to extract three top-level keys — and k scales with the
  number of JSONB columns projected per query.

**Measured benchmark shapes (1M row table, basin vs PG 16, warm cache):**

| Query pattern              | Basin (ms) | PG (ms) | Ratio |
|----------------------------|-----------|---------|-------|
| `payload->>'key'`          | ~12 000   | ~55     | ~218× |
| `payload->'key'`           | ~13 000   | ~60     | ~217× |
| `payload #> '{a,b}'`       | ~26 000   | ~70     | ~371× |
| `jsonb_typeof(payload)`    | ~750      | ~340    | ~2.2× |
| `jsonb_array_length(p)`    | ~1 800    | ~140    | ~13×  |

(`jsonb_typeof` is already cheap with `RawJson.top_type()` — only 2.2×; the
operator forms are the primary gap.)

## Options considered

### Option A — Shredded columns (path-splitting)

At write time, split a JSONB document into one physical sub-column per
discovered top-level (or bounded-depth) path. `payload->>'a'` becomes a
projection of the Vortex `payload$a` column, which is free at read time.

**Pros:**
- Extraction is a direct column read — zero parse cost.
- Vortex run-length / dictionary encoding compresses repeated values well
  for semi-structured SaaS data.
- Naturally composable with `CLUSTER BY` / partition pruning.

**Cons:**
- Schema explosion on heterogeneous documents (IoT event logs, audit trails,
  free-form metadata). Need a `_rest` fallback column for unindexed paths.
- Write-time cost: must parse and shred every document at ingest.
- Array-valued fields need special treatment (can't shred a `jsonb[]` into a
  single column without loss).
- Schema evolution is hard: adding a new key requires a backfill pass over
  the whole cold tier.
- Incompatible with the current on-disk format; requires a new table option
  and a migration for existing data.
- `#>`, `jsonb_path_query`, etc. still need the `_rest` column for paths not
  shredded.

**Verdict:** Ideal long-term for tables with stable, known-in-advance top-level
keys (e.g. event tables where the schema is decided at table creation). Too
costly to build for the first increment and too rigid for heterogeneous data.

### Option B — Binary blob (PG-style / BSON-style / on-demand tape)

Store a parsed binary form in the column instead of JSON text. Concretely: a
compact offset table (key → byte-range in the value section) prepended to a
raw-bytes value region, so `->>'a'` becomes a binary offset lookup — no scanning.

**Sub-variants:**
- **B1 — PostgreSQL varlena JSONB format**: a two-level JEntry/JTree structure
  with 4-byte entries encoding type + offset. Well-understood semantics, but
  requires a full custom encoder and a tight round-trip guarantee with Postgres
  binary wire format semantics.
- **B2 — BSON / MessagePack**: established binary formats; not Postgres-native,
  so every `->>` return value must still convert to text form.
- **B3 — Simdjson on-demand tape**: a tokeniser-produced token tape; no
  materialised values; O(doc-size) to build, O(depth) per key lookup.

**Pros:**
- No schema explosion; handles arbitrary, heterogeneous documents.
- `->` / `#>` becomes a binary seek, not a byte scan.
- Works transparently for all JSONB operators without DDL changes.

**Cons:**
- Per-row work is still O(doc-size) to build the index the first time (but
  with a much smaller constant than serde_json, and amortised over repeated
  key lookups in the same query).
- For single-UDF, single-key queries the improvement is a constant factor
  (typically 2–5×); it does not change the O(n) complexity.
- Encoder needed at write time (or lazy on first read); complicates the
  storage format and the version-tagging contract.
- Binary form must round-trip to byte-identical JSON semantics for all edge
  cases (duplicate keys, unicode escapes, number precision).

**Verdict:** Better than Option A for heterogeneous data. B3 (a parsed parse cache
scoped to a batch) is the right first step because it can be layered onto the
existing `RawJson` scanner without changing the storage format at all.

### Option C — Hybrid (binary blob + promoted hot paths)

Binary blob (Option B) as the base storage format, plus DDL-specified or
query-history-driven "promoted" top-level keys stored as real Vortex columns
alongside the blob.

`SELECT payload->>'project_id'` on a 1M-row events table where `project_id`
appears in 99% of queries: the planner substitutes a direct column read of the
promoted `payload$project_id` column.

Basin already has `QueryHistory` / `TopPatternProvider` infrastructure (see
`crates/basin-engine/src/query_history.rs`) that tracks which paths are
accessed frequently. The `TopPatternProvider::top_pattern` API returns the
most-observed column access pattern for a given project+table — exactly the
signal needed to drive automatic promotion.

**Pros:** Best of both worlds — arbitrary documents, but common-path access is
free. Works with the existing query-history framework.

**Cons:** Most complex; requires Option B as a prerequisite; promotion policy
is non-trivial (when to promote, when to demote, backfill cost); rollout spans
multiple quarters.

**Verdict:** The right long-term architecture. Build incrementally on top of B.

## Recommendation

**Build Option C incrementally, starting with Option B's first step.**

Concretely, the rollout is:

1. **(This ADR, Phase 1)** Per-batch top-level object index: within a single
   batch invocation, cache a `HashMap<key, (start, end)>` per row, keyed by the
   raw byte-slice pointer. Subsequent UDF calls on the same batch column share
   the index; the byte-scan cost is paid once per row per batch, not once per
   UDF call. This is a pure read-side optimization with no storage format change
   and no write-time cost.

2. **(Phase 2)** Persistent binary offset table: add a length-prefixed offset
   table header to stored JSONB bytes (version byte `0x02`; existing `0x01` /
   bare JSON still accepted). Encoder runs at write time in `basin-storage`'s
   writer path. Extraction becomes O(1) per key (binary search over the offset
   table) instead of O(doc-size). Storage format is version-tagged; old data
   (bare JSON) reads via the existing `RawJson` scanner.

3. **(Phase 3)** Nested-path index: extend the offset table to cover one
   additional level of nesting, so `#> '{a,b}'` is one offset lookup + one
   nested lookup instead of two full scans.

4. **(Phase 4)** Promoted columns (Option C): use `QueryHistory.top_pattern` to
   identify hot top-level paths; write those paths as real Vortex columns at
   ingest time. The planner substitutes a column projection for matching `->>`
   operators, bypassing the JSONB UDF entirely.

5. **(Phase 5)** DDL-driven explicit promotion: `ALTER TABLE t ADD JSONB INDEX
   (payload->>'project_id')` syntax to let users pin which paths get promoted
   regardless of query history.

**Build Phase 1 first** because:
- Zero storage format change — fully incremental.
- Directly unblocks multi-field SELECT workloads (the most common real pattern:
  ORMs that project 3–8 top-level fields per row).
- Proves the correctness harness (must produce identical results to `RawJson`
  on every edge case) before touching write-side code.
- Measurable benefit: a micro-bench shows the speedup for `k ≥ 2` keys.

## Correctness contract

The binary form (Phase 1 cache; Phase 2 offset table) must produce
**byte-identical results** to the existing `RawJson` scanner + full-parse
fallback path for every JSONB UDF:

- Object member access (`->`, `->>`): same result as `RawJson::member`.
- Array index access (`->`): same result as `RawJson::index`.
- Duplicate keys: last occurrence wins (matching `serde_json`'s duplicate-key
  semantics, documented in `scan_object_for`).
- Escaped keys (e.g. `"a\"b"`, `"c\\d"`): `parse_string` handles `\uXXXX`
  surrogates; the index must decode keys with the same logic.
- Unicode scalars: raw multibyte UTF-8 passes through unchanged.
- Number precision: numbers are never re-serialised at extraction; the raw
  byte slice is returned, preserving the original textual representation.
- `null` vs JSON-null: `->>'a'` returns SQL NULL when the value is JSON `null`
  (handled by `raw_slice_to_text`).

Existing parity tests that must continue to pass:
- `jsonb_udf::perf_bench::lazy_extract_parity` — exhaustive unit battery over
  arbitrary payloads (nested, unicode, duplicate keys, edge numbers, surrogate
  pairs, empty containers).
- `tests/integration/tests/jsonb_lazy_extract.rs` — end-to-end operator
  correctness over a real engine.
- `tests/integration/tests/viability_jsonb.rs` — round-trip and canonical form.
- `tests/integration/tests/jsonb_selective_filter.rs` — filter pushdown parity.

If the encoder encounters a document it cannot index without ambiguity (e.g.
a document that violates the UTF-8 constraint, or one where `parse_string`
returns `Err`), it must fall back to the existing `RawJson` scanner path for
that row. Silent semantic divergence is not acceptable.

## Migration / compat

- Phase 1 (this ADR): no storage format change. All existing data reads
  unchanged. The batch cache is a transient, in-memory structure scoped to a
  single `invoke_with_args` call chain; there is nothing to migrate.
- Phase 2 (binary offset table): new bytes written with version byte `0x02`;
  the reader detects the version byte and uses the offset table. Existing bytes
  (bare JSON, or the optional `0x01` Postgres wire prefix already tolerated by
  `jsonb_to_value` and `RawJson::new`) continue to work via the existing path.
  No backfill required; old files are read lazily via `RawJson`.
- Phase 4 (promoted columns): promoted columns are additive — a new physical
  file is written at compaction time; old files without the column are read via
  the JSONB UDF path as before.

## Rollout increments

1. **Phase 1** (this PR): per-batch top-level key index (`JsonbBatchIndex`),
   thread-local cache keyed by `(data_ptr, data_len)`, used by `json_get`,
   `json_get_text`, `json_path_extract`, `json_path_extract_text`. Effort: 0.5d.

2. **Phase 2**: write-side binary offset table in `basin-storage/src/writer.rs`
   + a new `JsonbOffsetTable` type. Reader detects version byte. Effort: 3–5d
   (encoder, reader, fuzz-based round-trip tests, storage integration).

3. **Phase 3**: nested-path index (one additional depth level). Effort: 2d.

4. **Phase 4**: query-history-driven promoted columns (planner rewrite + Vortex
   writer extension + compaction integration). Effort: 2–3 weeks.

5. **Phase 5**: explicit DDL promotion syntax. Effort: 1 week (parser change +
   catalog metadata + planner hook).
