//! PG-Wave KNN — nearest-neighbour spatial search execution for
//! `SELECT … FROM t ORDER BY <point_col> <-> ST_MakePoint(x,y) LIMIT k`.
//!
//! This is the spatial analogue of [`crate::executor::execute_vector_search_plan`]
//! (the pgvector HNSW top-k path). Where the vector path probes an HNSW
//! sidecar and re-ranks by exact vector distance, this path probes the
//! per-file R-tree sidecar ([`basin_storage::index::rtree`]) and re-ranks by
//! exact great-circle (Haversine) distance.
//!
//! # Execution shape
//!
//! 1. **Warm-up** — for every live data file of the table, ensure its
//!    `.rtree` sidecar is loaded into the process [`RTreeRegistry`] (lazy
//!    fetch + bincode-decode on demand, mirroring
//!    [`crate::session::apply_rtree_pruning_for_query`]). Files whose sidecar
//!    can't be loaded fall back to a full read (no false negatives).
//!
//! 2. **Per-file nearest probe** — call `nearest_entries([qx, qy], over_fetch)`
//!    on each file's R-tree. `rstar` yields entries in ascending **L2-degree**
//!    order. We OVER-FETCH `k * over_fetch_factor` (default 4, env-overridable
//!    via `BASIN_KNN_OVERFETCH`) candidates per file, because L2-degree order
//!    is NOT the same as Haversine-meter order across large spans / high
//!    latitudes — the true nearest by meters can sit slightly further out in
//!    degree space.
//!
//! 3. **Row-group open + exact re-rank** — collect the candidate row-group
//!    ids per file, read exactly those row-groups via
//!    `Storage::read_paths_with_schema { row_group_selection }`, decode every
//!    row's POINT geometry, compute `haversine_meters` to the query point, and
//!    keep the global top-k by exact distance. Reading whole row-groups means
//!    no candidate within an opened row-group is ever missed; the over-fetch
//!    governs which row-groups open.
//!
//! 4. **LIMIT-exact projection** — sort the surviving rows by ascending
//!    Haversine distance, truncate to exactly `k` (or fewer if the table has
//!    `< k` non-null-geom rows), and project to the user's column list.
//!
//! # Correctness
//!
//! * **L2-vs-Haversine** — step 2's over-fetch + step 3's exact re-rank is the
//!   load-bearing correctness mechanism. A pure L2-degree top-k would be wrong
//!   near the poles where a degree of longitude is far shorter in meters than a
//!   degree of latitude.
//! * **Null / undecodable geom** — skipped (they have no distance).
//! * **Exactness** — returns exactly `min(k, #rows-with-geom-in-opened-rgs)`.
//!
//! # Fallback
//!
//! On any error or coverage gap (sidecar missing, non-canonical path) the
//! function returns `Ok(None)` and the caller runs the standard brute-force
//! DataFusion pipeline, which is correct (just slower).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, RecordBatch, UInt32Array};
use arrow_schema::Schema;
use basin_common::{BasinError, Result};
use basin_storage::ReadOptions;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;

use crate::index_probe::SpatialKnnPlan;
use crate::{ExecResult, ProjectSession};

/// Default per-file over-fetch multiple of `k`. A factor of 4 means we pull
/// the 4·k nearest candidates (in L2-degree order) from each file's R-tree so
/// the exact Haversine re-rank has enough headroom that the true k-nearest are
/// never excluded by the degree-vs-meter ordering skew.
const DEFAULT_OVERFETCH_FACTOR: usize = 4;

/// Read `BASIN_KNN_OVERFETCH` once; default [`DEFAULT_OVERFETCH_FACTOR`].
fn overfetch_factor() -> usize {
    use std::sync::OnceLock;
    static F: OnceLock<usize> = OnceLock::new();
    *F.get_or_init(|| {
        std::env::var("BASIN_KNN_OVERFETCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(DEFAULT_OVERFETCH_FACTOR)
    })
}

/// Execute a [`SpatialKnnPlan`]. Returns `Ok(Some(rows))` when the R-tree
/// path produced the answer, or `Ok(None)` when the planner should fall back
/// to the standard pipeline (coverage gap / sidecar miss / non-canonical
/// path).
pub(crate) async fn execute_knn_plan(
    sess: &ProjectSession,
    plan: SpatialKnnPlan,
) -> Result<Option<ExecResult>> {
    let project = &sess.project;
    let engine = &sess.engine;
    let catalog = &engine.config().catalog;
    let storage = engine.config().storage.clone();

    let meta = catalog.load_table(project, &plan.table).await?;
    let live_files = meta.live_data_files();
    if live_files.is_empty() {
        // No data → empty result with the projected schema.
        return Ok(Some(empty_result(&meta.schema, &plan.projection)?));
    }
    let live_paths: Vec<String> = live_files.iter().map(|f| f.path.to_string()).collect();

    // ── Warm-up: ensure every live file's R-tree sidecar is resident. ──
    let registry = engine.rtree_registry();
    for p in &live_paths {
        if registry.is_file_indexed(project, &plan.table, &plan.col, p) {
            continue;
        }
        let Some(sidecar) = basin_storage::index::rtree::rtree_segment_key_for_data_file(
            None,
            project,
            &plan.table,
            &plan.col,
            p,
        ) else {
            // Non-canonical path → can't locate sidecar → fall back.
            return Ok(None);
        };
        use object_store::ObjectStoreExt as _;
        let store = storage.project_object_store(project);
        let bytes = match store.get(&sidecar).await {
            Ok(g) => match g.bytes().await {
                Ok(b) => b,
                Err(_) => return Ok(None),
            },
            Err(_) => return Ok(None), // sidecar missing → fall back
        };
        let rtree = match basin_storage::index::rtree::deserialize_rtree(&bytes) {
            Some(r) => r,
            None => return Ok(None),
        };
        registry.insert_segment(project, &plan.table, &plan.col, p, rtree);
    }
    // Re-verify coverage defensively (a racing compaction can't slip past).
    if !live_paths
        .iter()
        .all(|p| registry.is_file_indexed(project, &plan.table, &plan.col, p))
    {
        return Ok(None);
    }

    // ── Per-file nearest probe (over-fetch in L2-degree order). ──
    let query = [plan.qx, plan.qy];
    let take = plan.k.saturating_mul(overfetch_factor()).max(plan.k);
    // Per-file row-group allowlist: the union of row-groups holding the
    // over-fetched candidates. Reading whole row-groups guarantees we never
    // miss a row that should be in the true top-k.
    let mut rg_map: HashMap<String, Vec<u32>> = HashMap::new();
    for p in &live_paths {
        let entries = registry
            .nearest_entries(project, &plan.table, &plan.col, p, query, take)
            .unwrap_or_default();
        if entries.is_empty() {
            continue;
        }
        let mut rgs: Vec<u32> = entries.iter().map(|(rg, _, _, _)| *rg).collect();
        rgs.sort_unstable();
        rgs.dedup();
        rg_map.insert(p.clone(), rgs);
    }
    if rg_map.is_empty() {
        // No candidates anywhere (every file's R-tree was empty) → empty.
        return Ok(Some(empty_result(&meta.schema, &plan.projection)?));
    }

    // ── Read the candidate row-groups and exact-rank by Haversine. ──
    let paths: Vec<ObjectPath> = live_paths
        .iter()
        .filter(|p| rg_map.contains_key(*p))
        .map(|p| ObjectPath::from(p.as_str()))
        .collect();
    let catalog_schema = crate::convert::schema_ws_to_df(&meta.schema)
        .map(Arc::new)
        .map_err(|e| BasinError::internal(format!("knn schema convert: {e}")))?;
    let opts = ReadOptions {
        row_group_selection: Some(rg_map),
        ..ReadOptions::default()
    };
    let mut stream = storage
        .read_paths_with_schema(project, paths, opts, Some(catalog_schema))
        .await?;

    // Top-k by exact Haversine. We keep candidate rows as
    // (distance_m, batch_index, row_index) then `take` from the source
    // batches at the end to preserve exact column types/order.
    let qpoint = basin_geo::Point::new(plan.qx, plan.qy);
    let mut batches: Vec<RecordBatch> = Vec::new();
    // (distance, batch_idx, row_idx)
    let mut scored: Vec<(f64, usize, usize)> = Vec::new();

    while let Some(item) = stream.next().await {
        let batch = item?;
        let geom_idx = match batch.schema().index_of(&plan.col) {
            Ok(i) => i,
            Err(_) => continue,
        };
        let col = batch.column(geom_idx);
        let Some(arr) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() else {
            continue;
        };
        let bidx = batches.len();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let Ok(pt) = basin_geo::decode_point(arr.value(row)) else {
                continue;
            };
            let d = basin_geo::haversine_meters(&pt, &qpoint);
            scored.push((d, bidx, row));
        }
        batches.push(batch);
    }

    if scored.is_empty() {
        return Ok(Some(empty_result(&meta.schema, &plan.projection)?));
    }

    // Ascending distance; total order via total_cmp (no NaN — Haversine is
    // finite and clamped). Stable secondary key (batch, row) for determinism.
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    scored.truncate(plan.k);

    // Materialise the top-k rows: group surviving row indices by batch (in
    // global top-k order) and `take` per batch, then concatenate so the final
    // batch is in ascending-distance order.
    let topk = build_topk_batch(&batches, &scored)?;
    let projected = project_batch(&topk, &plan.projection)?;
    let schema = projected.schema();
    sess.engine.note_vector_routed();
    Ok(Some(ExecResult::Rows {
        schema,
        batches: vec![projected],
    }))
}

/// Build a single batch containing the `scored` rows (already truncated to k
/// and sorted ascending by distance), preserving that order.
fn build_topk_batch(
    batches: &[RecordBatch],
    scored: &[(f64, usize, usize)],
) -> Result<RecordBatch> {
    let schema = batches[0].schema();
    let ncols = schema.fields().len();
    // For each output column, `take` the selected rows from their source
    // batch. Because rows can come from different batches we build per-column
    // by concatenating single-row takes — but that is O(rows²). Instead, group
    // by source batch, take a slice per batch, then interleave via a final
    // global take over a concatenation. Simpler: concatenate all source
    // batches, then take by a recomputed global row offset.
    //
    // Compute the global row offset of each source batch.
    let mut offsets = Vec::with_capacity(batches.len());
    let mut acc = 0usize;
    for b in batches {
        offsets.push(acc);
        acc += b.num_rows();
    }
    let concatenated = arrow_select::concat::concat_batches(&schema, batches)
        .map_err(|e| BasinError::internal(format!("knn concat: {e}")))?;
    let indices: UInt32Array = scored
        .iter()
        .map(|(_, bidx, row)| (offsets[*bidx] + *row) as u32)
        .collect();
    let mut cols = Vec::with_capacity(ncols);
    for c in concatenated.columns() {
        let t = arrow_select::take::take(c.as_ref(), &indices, None)
            .map_err(|e| BasinError::internal(format!("knn take: {e}")))?;
        cols.push(t);
    }
    RecordBatch::try_new(schema, cols)
        .map_err(|e| BasinError::internal(format!("knn rebuild: {e}")))
}

/// Project `batch` down to the user's column list (`None` → all columns).
fn project_batch(batch: &RecordBatch, cols: &Option<Vec<String>>) -> Result<RecordBatch> {
    let Some(names) = cols else {
        return Ok(batch.clone());
    };
    let schema = batch.schema();
    let mut idxs = Vec::with_capacity(names.len());
    for n in names {
        let i = schema
            .index_of(n)
            .map_err(|e| BasinError::internal(format!("knn project col {n}: {e}")))?;
        idxs.push(i);
    }
    let proj_schema = Arc::new(
        schema
            .project(&idxs)
            .map_err(|e| BasinError::internal(format!("knn project schema: {e}")))?,
    );
    let proj_cols = idxs.iter().map(|i| batch.column(*i).clone()).collect();
    RecordBatch::try_new(proj_schema, proj_cols)
        .map_err(|e| BasinError::internal(format!("knn project batch: {e}")))
}

/// Empty result with the user's projected schema (zero rows).
fn empty_result(table_schema: &Schema, projection: &Option<Vec<String>>) -> Result<ExecResult> {
    let df_schema = crate::convert::schema_ws_to_df(table_schema)
        .map_err(|e| BasinError::internal(format!("knn empty schema: {e}")))?;
    let names: Vec<String> = match projection {
        Some(c) => c.clone(),
        None => df_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect(),
    };
    let fields = names
        .iter()
        .map(|n| {
            df_schema
                .field_with_name(n)
                .map(|f| Arc::new(f.clone()))
                .map_err(|e| BasinError::internal(format!("knn empty field {n}: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    let cols = fields
        .iter()
        .map(|f| arrow_array::new_empty_array(f.data_type()))
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| BasinError::internal(format!("knn empty batch: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}
