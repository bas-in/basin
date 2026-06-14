//! Trigram-index-assisted kNN execution for
//! `SELECT … FROM t ORDER BY <text_col> <-> 'needle' LIMIT k`.
//!
//! This is the trigram analogue of [`crate::rtree_knn_scan`] (spatial KNN) and
//! [`crate::executor::execute_vector_search_plan`] (pgvector HNSW top-k). Where
//! those probe a geometry / vector sidecar, this path probes the trigram GIN
//! posting list ([`crate::index_probe::GinIndexRegistry`]) to generate the
//! CANDIDATE row set and then re-ranks by EXACT trigram distance.
//!
//! # Why the trigram postings bound the answer
//!
//! The trigram distance of a row is `1 - similarity(col, needle)`. A row sharing
//! ZERO needle trigrams has `similarity = 0`, hence the maximal distance `1`. So
//! every row whose distance is `< 1` shares `>= 1` needle trigram, and the
//! posting list enumerates exactly those rows. The exact top-k by ascending
//! distance is therefore drawn entirely from the candidate set, UNLESS fewer
//! than `k` candidate rows exist — in which case the remaining slots are filled
//! by arbitrary distance-1 (zero-shared-trigram) rows, exactly as PostgreSQL
//! returns `min(k, #rows)` rows with ties at distance 1 broken arbitrarily.
//!
//! # Exactness (NOT an approximation)
//!
//! The postings only NARROW the candidate SET. The `<->` distance is recomputed
//! EXACTLY on every materialised candidate row via `basin_trgm::similarity`
//! (the same function the SQL `similarity()` UDF and the `<->` rewrite use), so
//! the ranking is never approximate — the result equals the full sequential
//! scan + sort, row-for-row and order-for-order (with the same arbitrary
//! tie-break at distance 1).
//!
//! # Execution shape
//!
//! 1. **Decline gates** — fall back (`Ok(None)`) to the standard scan + sort on:
//!    a live UPDATE/DELETE overlay (the pruned reader is overlay-blind), no
//!    usable trigram index, or a needle too short to produce any trigram.
//! 2. **Candidate probe** — [`GinIndexRegistry::probe_trgm_knn_candidates`]
//!    classifies each live file as `Rows(offsets)` (sealed row tier, enumerable
//!    candidates), `Full` (decode whole file: un-indexed / no row tier / dense
//!    trigram), or `None` (no needle trigram present — fill-only).
//! 3. **Candidate read + exact rank** — read `Rows`/`Full` files (Rows files
//!    restricted to candidate offsets via `ReadOptions::row_selection`), decode
//!    the text column, compute `1 - similarity` per row, keep the global top-k.
//! 4. **Boundary fill** — if fewer than `k` rows survive, read additional
//!    distance-1 rows (from `None` files / the unread remainder) and append them
//!    until `k` rows (or the table is exhausted).
//!
//! On any error / coverage gap the function returns `Ok(None)` and the caller
//! runs the standard DataFusion pipeline (correct, just slower).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::Schema;
use basin_common::{BasinError, Result};
use basin_storage::ReadOptions;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;

use crate::index_probe::{TrgmKnnFile, TrgmKnnPlan};
use crate::{ExecResult, ProjectSession};

/// Execute a [`TrgmKnnPlan`]. Returns `Ok(Some(rows))` when the trigram-index
/// path produced the answer, or `Ok(None)` when the planner should fall back to
/// the standard scan + sort pipeline (overlay present, no index, needle too
/// short, or any coverage gap).
pub(crate) async fn execute_trgm_knn_plan(
    sess: &ProjectSession,
    plan: TrgmKnnPlan,
) -> Result<Option<ExecResult>> {
    let project = &sess.project;
    let engine = &sess.engine;
    let catalog = &engine.config().catalog;
    let storage = engine.config().storage.clone();

    // ── Decline gate: a live hot-tier overlay makes the pruned cold reader
    // blind to in-flight UPDATE/DELETE rows. Same blocker as the `%` prune. ──
    if crate::session::table_has_live_overlay(engine, project, &plan.table) {
        return Ok(None);
    }

    let meta = catalog.load_table(project, &plan.table).await?;
    let live_files = meta.live_data_files();
    if live_files.is_empty() {
        return Ok(Some(empty_result(&meta.schema, &plan.projection)?));
    }
    let live_paths: Vec<String> = live_files.iter().map(|f| f.path.to_string()).collect();

    // ── Candidate probe. `None` ⇒ no usable index / needle too short ⇒ fall
    // back to the standard scan + sort. ──
    let verdicts = match engine.gin_index_registry().probe_trgm_knn_candidates(
        project,
        &plan.table,
        &plan.col,
        &plan.needle,
        &live_paths,
    ) {
        Some(v) => v,
        None => return Ok(None),
    };

    let catalog_schema = crate::convert::schema_ws_to_df(&meta.schema)
        .map(Arc::new)
        .map_err(|e| BasinError::internal(format!("trgm knn schema convert: {e}")))?;

    // ── Build the candidate read set. `Rows` files are restricted to their
    // candidate offsets via `row_selection`; `Full` files read every row;
    // `None` files are excluded (fill-only). ──
    let mut candidate_paths: Vec<ObjectPath> = Vec::new();
    let mut row_selection: HashMap<String, Vec<u64>> = HashMap::new();
    // `None`-verdict files: no needle trigram occurs, so every row is a
    // distance-1 fill row. These are the ONLY files NOT read by the candidate
    // pass — `Rows`/`Full` files are read whole, so their distance-1 rows are
    // already scored. Reading the candidate files whole (rather than via the
    // row_selection narrow) makes the fill format-agnostic: it draws ONLY from
    // disjoint `None` files, so no row is ever double-counted regardless of
    // whether the storage layer honours the row_selection hint (Parquet does,
    // Vortex ignores it). The row_selection is still attached as a SAFE perf
    // SUPERSET hint for Parquet candidate files — the exact distance recompute
    // below re-ranks whatever rows arrive, so an honoured-or-ignored hint is
    // equally correct.
    let mut fill_paths: Vec<String> = Vec::new();
    for path in &live_paths {
        match verdicts.get(path) {
            Some(TrgmKnnFile::Rows(offs)) => {
                candidate_paths.push(ObjectPath::from(path.as_str()));
                row_selection.insert(path.clone(), offs.clone());
            }
            Some(TrgmKnnFile::Full) => {
                candidate_paths.push(ObjectPath::from(path.as_str()));
            }
            Some(TrgmKnnFile::None) | None => {
                fill_paths.push(path.clone());
            }
        }
    }

    // ── Read the candidate files and exact-rank by `1 - similarity`. ──
    let mut batches: Vec<RecordBatch> = Vec::new();
    // (distance, batch_idx, row_idx)
    let mut scored: Vec<(f32, usize, usize)> = Vec::new();
    if !candidate_paths.is_empty() {
        let opts = ReadOptions {
            row_selection: if row_selection.is_empty() {
                None
            } else {
                Some(row_selection)
            },
            ..ReadOptions::default()
        };
        let mut stream = storage
            .read_paths_with_schema(project, candidate_paths, opts, Some(catalog_schema.clone()))
            .await?;
        while let Some(item) = stream.next().await {
            let batch = item?;
            score_batch_into(&batch, &plan, &mut batches, &mut scored)?;
        }
    }

    // Ascending distance; total order via total_cmp. Stable secondary key
    // (batch, row) for deterministic output across runs.
    scored.sort_by(|a, b| {
        a.0.total_cmp(&b.0)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
    });

    // ── Boundary fill: when the candidate files hold fewer than k rows in
    // total, the answer is short of k. The remaining slots are arbitrary
    // distance-1 rows (PG returns min(k, #rows) with ties at distance 1
    // arbitrary), which live exclusively in the `None` files (disjoint from the
    // candidate files). Fill BEFORE the final truncate so the appended
    // distance-1 rows sit after every scored candidate row. ──
    if scored.len() < plan.k && !fill_paths.is_empty() {
        let deficit = plan.k - scored.len();
        let fill = read_fill_rows(sess, &plan, &catalog_schema, &fill_paths, deficit).await?;
        for (batch, rows) in fill {
            let bidx = batches.len();
            for r in rows {
                // Fill rows carry no shared needle trigram → distance exactly 1.
                scored.push((1.0, bidx, r));
                if scored.len() >= plan.k {
                    break;
                }
            }
            batches.push(batch);
            if scored.len() >= plan.k {
                break;
            }
        }
        // Re-sort so the appended fill rows sit after the near rows (all near
        // rows have distance < 1 ≤ fill distance, so this only orders within
        // the distance-1 tier deterministically).
        scored.sort_by(|a, b| {
            a.0.total_cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.2.cmp(&b.2))
        });
    }
    // Final exact top-k cut (covers both the no-fill path — candidate rows >= k
    // — and the post-fill path).
    scored.truncate(plan.k);

    if scored.is_empty() {
        return Ok(Some(empty_result(&meta.schema, &plan.projection)?));
    }

    let topk = build_topk_batch(&batches, &scored)?;
    let projected = project_batch(&topk, &plan.projection)?;
    let schema = projected.schema();
    engine.note_trgm_knn_routed();
    Ok(Some(ExecResult::Rows {
        schema,
        batches: vec![projected],
    }))
}

/// Decode `batch`'s text column, compute `1 - similarity(value, needle)` per
/// non-null row, and push `(distance, batch_idx, row_idx)` into `scored`. The
/// batch is appended to `batches` and indexed by its position there.
///
/// NULL text rows are skipped (PG sorts NULLs last with ASC; a NULL has no
/// trigram similarity — it can never be in the top-k while any non-null row
/// exists, and the fill path covers the all-null degenerate case).
fn score_batch_into(
    batch: &RecordBatch,
    plan: &TrgmKnnPlan,
    batches: &mut Vec<RecordBatch>,
    scored: &mut Vec<(f32, usize, usize)>,
) -> Result<()> {
    let col_idx = match batch.schema().index_of(&plan.col) {
        Ok(i) => i,
        Err(_) => return Ok(()),
    };
    let col = batch.column(col_idx);
    let Some(arr) = col.as_any().downcast_ref::<StringArray>() else {
        // Unexpected column type → caller falls back is not possible here; just
        // skip (the candidate read can't rank a non-text column).
        return Ok(());
    };
    let bidx = batches.len();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            continue;
        }
        let sim = basin_trgm::similarity(arr.value(row), &plan.needle);
        scored.push((1.0 - sim, bidx, row));
    }
    batches.push(batch.clone());
    Ok(())
}

/// Read up to `deficit` distance-1 fill rows from `fill_paths` (the `None`
/// verdict files — disjoint from the candidate files, so no row is double
/// counted).
///
/// Returns `(batch, row_indices)` pairs; the caller appends them as distance-1
/// rows. Reads files one at a time and stops as soon as the deficit is met, so
/// a tiny fill never over-fetches a large table.
async fn read_fill_rows(
    sess: &ProjectSession,
    plan: &TrgmKnnPlan,
    catalog_schema: &Arc<Schema>,
    fill_paths: &[String],
    deficit: usize,
) -> Result<Vec<(RecordBatch, Vec<usize>)>> {
    let storage = sess.engine.config().storage.clone();
    let mut out: Vec<(RecordBatch, Vec<usize>)> = Vec::new();
    let mut need = deficit;

    for path in fill_paths {
        if need == 0 {
            break;
        }
        let paths = vec![ObjectPath::from(path.as_str())];
        let opts = ReadOptions::default();
        let mut stream = storage
            .read_paths_with_schema(
                &sess.project,
                paths,
                opts,
                Some(catalog_schema.clone()),
            )
            .await?;
        while let Some(item) = stream.next().await {
            if need == 0 {
                break;
            }
            let batch = item?;
            let col_idx = match batch.schema().index_of(&plan.col) {
                Ok(i) => i,
                Err(_) => continue,
            };
            let col = batch.column(col_idx);
            let arr = col.as_any().downcast_ref::<StringArray>();
            let mut rows: Vec<usize> = Vec::new();
            for r in 0..batch.num_rows() {
                if need == 0 {
                    break;
                }
                // Skip NULL text rows: a NULL is not a real distance-1 row a
                // user expects ahead of materialised rows, and PG sorts NULLs
                // last under ASC. Non-null distance-1 rows fill first.
                if let Some(a) = arr {
                    if a.is_null(r) {
                        continue;
                    }
                }
                rows.push(r);
                need -= 1;
            }
            if !rows.is_empty() {
                out.push((batch, rows));
            }
        }
    }
    Ok(out)
}

/// Build a single batch containing the `scored` rows (already truncated to k and
/// sorted ascending by distance), preserving that order. Mirrors
/// [`crate::rtree_knn_scan`]'s `build_topk_batch`.
fn build_topk_batch(
    batches: &[RecordBatch],
    scored: &[(f32, usize, usize)],
) -> Result<RecordBatch> {
    let schema = batches[0].schema();
    let mut offsets = Vec::with_capacity(batches.len());
    let mut acc = 0usize;
    for b in batches {
        offsets.push(acc);
        acc += b.num_rows();
    }
    let concatenated = arrow_select::concat::concat_batches(&schema, batches)
        .map_err(|e| BasinError::internal(format!("trgm knn concat: {e}")))?;
    let indices: UInt32Array = scored
        .iter()
        .map(|(_, bidx, row)| (offsets[*bidx] + *row) as u32)
        .collect();
    let mut cols = Vec::with_capacity(schema.fields().len());
    for c in concatenated.columns() {
        let t = arrow_select::take::take(c.as_ref(), &indices, None)
            .map_err(|e| BasinError::internal(format!("trgm knn take: {e}")))?;
        cols.push(t);
    }
    RecordBatch::try_new(schema, cols)
        .map_err(|e| BasinError::internal(format!("trgm knn rebuild: {e}")))
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
            .map_err(|e| BasinError::internal(format!("trgm knn project col {n}: {e}")))?;
        idxs.push(i);
    }
    let proj_schema = Arc::new(
        schema
            .project(&idxs)
            .map_err(|e| BasinError::internal(format!("trgm knn project schema: {e}")))?,
    );
    let proj_cols = idxs.iter().map(|i| batch.column(*i).clone()).collect();
    RecordBatch::try_new(proj_schema, proj_cols)
        .map_err(|e| BasinError::internal(format!("trgm knn project batch: {e}")))
}

/// Empty result with the user's projected schema (zero rows). Mirrors
/// [`crate::rtree_knn_scan`]'s `empty_result`.
fn empty_result(
    table_schema: &Schema,
    projection: &Option<Vec<String>>,
) -> Result<ExecResult> {
    let df_schema = crate::convert::schema_ws_to_df(table_schema)
        .map_err(|e| BasinError::internal(format!("trgm knn empty schema: {e}")))?;
    let names: Vec<String> = match projection {
        Some(c) => c.clone(),
        None => df_schema.fields().iter().map(|f| f.name().clone()).collect(),
    };
    let fields = names
        .iter()
        .map(|n| {
            df_schema
                .field_with_name(n)
                .map(|f| Arc::new(f.clone()))
                .map_err(|e| BasinError::internal(format!("trgm knn empty field {n}: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Arc::new(Schema::new(
        fields.iter().map(|f| f.as_ref().clone()).collect::<Vec<_>>(),
    ));
    let cols = fields
        .iter()
        .map(|f| arrow_array::new_empty_array(f.data_type()))
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| BasinError::internal(format!("trgm knn empty batch: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}
