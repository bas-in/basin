---
title: "PostGIS R-tree spatial index — design"
nav_section: architecture
sidebar_position: 45
summary: "How a GIST-equivalent R-tree spatial index ships in basin-geo, for PostGIS-shape workloads at 1M+ geometries. Design only; pending review."
---

# PostGIS R-tree Spatial Index — Design Doc

**Status:** Pending user review. Gate for task #139 (PG-Wave 4 IMPL).

**Driver:** Dependent customer needs PostGIS-shape workloads to perform at scale (1M+ geometries). v0.1 `basin-geo` is Rust-API correctness only — no SQL surface, no spatial index, no real predicate library. This doc specifies how a GIST-equivalent R-tree spatial index ships in basin.

**Produced by:** Read-only sonnet investigation (task #138), 2026-05-30.

---

## 1. TL;DR

Basin's `basin-geo` crate has complete POINT/BOX2D/ST_DWithin/ST_Contains Rust API and a clear "v0.2 spatial index" placeholder in `geo_glue.rs:44`. The geo UDFs are no-ops at the SQL level (`geo_glue::install()` is empty — `geo_glue.rs:53`), so the first week of work wires those before the index can be used. For the index itself:

- **Per-file sidecar** (`projects/{p}/tables/{t}/index/{col}.rtree/{ulid}.rtree`) — same pattern as the existing HNSW vector index
- **Built at compaction time** in `compact_one` (in_process.rs:1264), exact same hook as GIN bloom indexing
- **`RTreeRegistry` + `RTreePrunedTable` + `RTreeScanExec`** mirroring the GIN row-group pruning pattern
- **`SpatialPredicate` enum** for `DWithin` (Inexact) / `BboxIntersects` (Exact) / `PointEq` (Exact)
- **`rstar = "0.12"`** dep — MIT/Apache-2, ~2 transitive deps, serde-feature for bincode serialisation

Hot-tier rows (WAL tail) are NOT indexed — query plan UNIONs unindexed hot scan with indexed cold scan, same shape as `HotTombstoneTable` today.

---

## 2. File format — sidecar per Parquet file

**Recommendation: sidecar file.** Path: `projects/{project}/tables/{tbl}/index/{col}.rtree/{ulid}.rtree`.

Evidence:
- `crates/basin-storage/src/vector_index.rs:1-7` — HNSW already uses `…/index/{col}.hnsw/{ulid}.hnsw`. Sidecar is the only storage-side index format in basin today; no Parquet KV footer path exists.
- `crates/basin-storage/src/data_file.rs:19-48` — DataFile carries bloom/HLL/tdigest sketches in RAM (a few KB per file). An R-tree for 65k rows × 2D points is ~1-5 MB; embedding it in the Parquet KV footer would bloat every footer fetch.
- `vector_index_segment_key_for_data_file()` (`vector_index.rs:272`) gives the exact key-derivation helper to copy.
- `rstar` entries are POD; serialise cleanly with `bincode` (workspace dep at `Cargo.toml:116`).

**Alternative (Parquet footer KV block):** Viable only if serialised size were reliably under ~128 KB. At 1M rows × 15+ row-groups × hundreds of KB each — over budget. Rejected.

---

## 3. Build-on-write

**Hook:** `compact_one` (`in_process.rs:1264`), immediately after `commit_with_retry`. Same hook as GIN bloom (`in_process.rs:1419-1458`).

**Concrete wiring:**
1. New `reindex_compacted_file_rtree(...)` mirroring `reindex_compacted_file_gin(...)` at `in_process.rs:1452`.
2. Function receives `merged: RecordBatch`, iterates `SecondaryIndex` entries with `access_method == "gist"` (PG-compatible string), extracts geometry column, builds `rstar::RTree<SpatialEntry>` where `SpatialEntry { row_group_id: u32, row_id_in_group: u32, envelope: AABB<[f64; 2]> }`, serialises via bincode, writes sidecar via `storage.project_store(project).put(...)`.
3. Add `rtree_registry: std::sync::RwLock<Option<Arc<RTreeRegistry>>>` to `InProcessShard` (mirror `gin_rowgroup_registry` slot at `in_process.rs:145`).

**Hot vs cold:** Hot-tier rows (in-memory `PartitionState::tail`) NOT indexed. Same decision as GIN (`in_process.rs:1422-1425`). Plan UNIONs indexed cold scan with unindexed hot scan via the existing `HotTombstoneTable` / `UnionExec` pattern (`session.rs:2000`, `hot_tombstone.rs:889`). Hot rows brute-force scan; bounded by WAL flush cycle (seconds to minutes) — acceptable for 1M+ datasets.

**Concurrency:** R-tree built with no Basin lock held (same as GIN — `compact_one` drops the per-partition `RwLock` before object-store I/O). `RTreeRegistry` uses the `Mutex<HashMap<RegKey, Arc<Mutex<...>>>>` double-lock pattern from `GinRowGroupRegistry` (`gin_rowgroup.rs:184`). File removal on compaction calls `rtree_registry.remove_file(...)` mirror.

**Lazy warm-up:** Engine restart → registry empty. Completeness guard `indexed_files ⊇ live_files` (same as `gin_tsvector.rs:64`) gates pruning; until warm, falls back to full scan. Background warm-up task lists `index/{col}.rtree/` and deserialises each sidecar — week-2 task (see §7 Risk R3).

---

## 4. Predicate pushdown

**Pattern:** `GinRowGroupPrunedTable` (`gin_rowgroup_scan.rs:78`) registered via `apply_gin_pruning_for_query` (`session.rs:2181`). R-tree analogue: `RTreePrunedTable` + `RTreeScanExec` + `apply_rtree_pruning_for_query`.

**`SpatialPredicate` enum:**

```rust
pub enum SpatialPredicate {
    /// ST_DWithin(col, ST_MakePoint(x, y), r) — INEXACT.
    /// R-tree probes expanded bbox of [x - r_deg, x + r_deg, y - r_deg, y + r_deg].
    /// Survivors re-evaluated by Haversine UDF as residual filter.
    DWithin { col: String, x: f64, y: f64, radius_m: f64 },

    /// ST_Contains(bbox_literal, col) or col && bbox — EXACT.
    /// R-tree envelope test == predicate semantics.
    BboxIntersects { col: String, min_x: f64, min_y: f64, max_x: f64, max_y: f64 },

    /// col = ST_MakePoint(x, y) — EXACT. Row-group-level point eq.
    PointEq { col: String, x: f64, y: f64 },
}
```

**Exact vs inexact (DataFusion `TableProviderFilterPushDown`):**
- `BboxIntersects`, `PointEq` → `Exact` (no FilterExec needed above)
- `DWithin` → `Inexact` (DataFusion keeps a residual FilterExec; Haversine UDF re-runs)

**Planner flow:**
1. `apply_rtree_pruning_for_query` called from session pre-scan hook (same callsite as `apply_gin_pruning_for_query` at `session.rs:2181`). Fast pre-check: SQL contains `ST_DWithin`, `ST_Contains`, or `&&`?
2. Parse + detect: extract `SpatialPredicate` from WHERE AST.
3. Verify table has GIST index on referenced column: `catalog.load_table()` → `meta.indexes.iter().find(|i| i.access_method == "gist")`.
4. Probe `RTreeRegistry::candidates_for_predicate(pred, live_paths)`. For each cold file: deserialise sidecar (or hit segment cache), run `rstar::RTree::locate_in_envelope_intersecting(...)`, collect row-group indices.
5. If ALL live files indexed, replace `ListingTable` with `RTreePrunedTable { row_group_selection: HashMap<path, Vec<u32>>, … }`.
6. `RTreeScanExec::execute()` drives `storage.read_paths_with_schema(paths, ReadOptions { row_group_selection: Some(map), … }, …)` exactly as `GinRowGroupScanExec` does (`gin_rowgroup_scan.rs:284-330`).

---

## 5. Statistics integration — coexist, don't replace

**ColumnStats today** (`metadata.rs:209`): `null_count`, `min_bytes`, `max_bytes`, `sum_bytes`. File-level scalar min/max. For geometry columns (today Utf8 — `types.rs:852` — eventually FixedSizeBinary(21)), Parquet stats are byte-comparisons of WKB → meaningless for spatial pruning.

**Recommendation:**
- R-tree provides spatial pruning at row-group granularity (segment stores `row_group_id`).
- Zone-map min/max in `ColumnStats` continues serving non-spatial columns unchanged.
- For geometry columns, zone-map is near-useless but harmless.
- GIN row-group bloom and R-tree registries are additive and orthogonal: a table with both JSONB GIN index AND geometry GIST index uses both registries independently.

**Planner priority for `WHERE ST_DWithin(geom, …) AND metadata @> '{…}'`:**
- Both pushdowns fire: GIN narrows file set; R-tree narrows row-groups within each surviving file.
- Pipeline order: GIN file-level first, R-tree row-group second — matches existing precedent.

Per-row-group bbox stats in `DataFileRef` not needed in week 1; sidecar R-tree gives finer granularity.

---

## 6. Dependency

**`rstar = "0.12"`** with `serde` feature.

- MIT/Apache-2 dual license
- Transitive deps: `num-traits` (already transitive via arrow). Two crates total.
- Zero unsafe in public API
- Entries are user-defined `RTreeObject` impls; `SpatialEntry` is fully Copy / POD — no boxing
- `serde` feature → bincode round-trip works directly (bincode is workspace dep at `Cargo.toml:116`)

**Alternative considered:** `geo` crate's R-tree (backed by rstar internally). Adds ~20 crate deps for the polygon-ops surface. We want polygon ops in PG-Wave 3 separately — keep deps split.

`basin-storage/Cargo.toml` add: `rstar = { version = "0.12", features = ["serde"] }`.

---

## 7. Risks & open questions

**R1 — SQL surface is not wired.** `geo_glue::install()` is empty (`geo_glue.rs:53`); `POINT` DDL type maps to `Utf8` not `FixedSizeBinary(21)` (`types.rs:852`). Before any spatial predicate can be recognised by the pushdown planner, the UDF registration (6+ UDFs) and type DDL change must land. **This is ~1 week of engine work orthogonal to the index — it is NOT done.** PG-Wave 1 + PG-Wave 2 are hard prerequisites for PG-Wave 4. Failing to account for this blows the 2-4 week estimate immediately.

**R2 — row_id → row_group mapping.** Naive approach: store row position within file; convert to row-group index at query time via Parquet footer round-trip. Adds latency on first access. **Recommendation:** store `(row_group_id, row_id_in_group, bbox)` directly in the R-tree entry — same as `TsvPostingEntry.row_group: u32`. Eliminates re-derivation.

**R3 — Cold warm-up / restart correctness.** GIN registries rebuild lazily from writes. The sidecar approach instead requires listing object-store keys and deserialising potentially hundreds of bincode blobs before pruning fires. At 1M rows × ~16 files × ~200 KB → ~3 MB of I/O + decode on first query. **Needs a warm-up background task and `RTreeSegmentCache`** (analogous to `HnswSegmentCache` at `metadata_cache.rs:252`). Missing this in week 1 → first-query cold latency is high; completeness guard falls back to full scan every restart.

---

## 8. Suggested first implementation wave

**Week 1 (unblocking — PG-Wave 1 + PG-Wave 2 prerequisites land first or alongside):**
1. Wire `geo_glue::install()`: register `ST_MakePoint`, `ST_X`, `ST_Y`, `ST_Distance`, `ST_DWithin`, `ST_Contains` as DataFusion ScalarUDFs (per the plan in `geo_glue.rs:32-47`).
2. Change `POINT` DDL type mapping from `Utf8` to `FixedSizeBinary(21)` (`types.rs:852`).
3. Add `access_method = "gist"` handling to `SecondaryIndex` + DDL parser (`CREATE INDEX … USING gist`). The existing `SecondaryIndex` struct (`metadata.rs:64`) needs only a new access method string — no schema change.
4. New `basin-storage/src/index/rtree.rs` mirroring `gin_rowgroup.rs`: `RTreeRegistry`, `build_rtree_for_batch(batch, col, rg_size) -> RTree<SpatialEntry>`, serialise/deserialise via bincode.
5. Hook in `compact_one`: call `reindex_compacted_file_rtree(...)` after `commit_with_retry` for tables with a GIST index, writing the sidecar to object store.

**Weeks 2-4 (pushdown + warm-up):**
1. `basin-engine/src/rtree_rowgroup_scan.rs` — `RTreePrunedTable` + `RTreeScanExec` cloned from `gin_rowgroup_scan.rs`, parameterised by per-file row-group allowlist.
2. `detect_spatial_predicate()` in `index_probe.rs` — SQL pattern recogniser for `ST_DWithin(col, ST_MakePoint(x,y), r)` and `ST_Contains(box_literal, col)`, returning `SpatialPredicate`.
3. `apply_rtree_pruning_for_query()` in `session.rs` — mirrors `apply_gin_pruning_for_query` (line 2181): detect predicate, check catalog for GIST index, probe `RTreeRegistry`, if complete coverage build `RTreePrunedTable` and re-register in `SessionContext`.
4. `RTreeSegmentCache` in `metadata_cache.rs` — LRU cache of parsed `rstar::RTree<SpatialEntry>` by sidecar object path, with background warm-up task that pre-loads all segments on engine startup.
5. Viability benchmark: `viability_large_spatial_dwithin` at 1M points comparing indexed vs unindexed `ST_DWithin` selectivity.

---

## 9. Open decision for review

The week-1 list above (UDF wiring + POINT type DDL + access_method=gist + index build hook) overlaps with PG-Waves 1, 2, and the first half of PG-Wave 4. Option A is to keep them as separate waves and ship strictly 1→2→4. Option B is to bundle "spatial substrate" (UDFs + POINT type + GIST DDL + index build) into one wave and ship the SQL pushdown as a second wave. **Recommend Option B** — the four pieces are interdependent and an agent shipping them together can hold the type contract in head.

If approved, dispatch order becomes:
1. **PG-Wave-α "Spatial substrate"** — UDF + POINT FSB(21) + gist DDL + rtree.rs + compact_one hook (week 1)
2. **PG-Wave-β "Spatial pushdown"** — RTreePrunedTable + planner integration + segment cache + warm-up + viability bench (weeks 2-4)
3. **PG-Wave 3** (expanded predicates via `geo` crate) — can land in parallel with β
4. **PG-Wave 5** (SRID + PROJ)
5. **PG-Wave 6** (bench vs PostGIS+GIST)
