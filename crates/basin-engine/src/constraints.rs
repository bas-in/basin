//! PRIMARY KEY / CHECK / FOREIGN KEY enforcement on INSERT, UPDATE, DELETE.
//!
//! Three surfaces share this module:
//!
//! - `enforce_pk_on_insert` — full-table scan for any row whose PK
//!   tuple matches a row already in the table. v0.2 secondary indexes
//!   (Phase 5.7 B1) will replace the scan with a B-tree probe; for
//!   v0.1 the cost is `O(rows_in_table)` per INSERT batch. Documented
//!   in code as the dependency.
//! - `enforce_check_constraints` — evaluates each CHECK predicate over
//!   the batch via DataFusion (same template as
//!   `crate::generated_cols::eval_expression` and
//!   `crate::type_ddl::enforce_domain_checks`).
//! - `enforce_fk_on_insert` — for each referencing row, verifies the
//!   referenced PK tuple exists on the referenced table (same-project
//!   only). NULL in any FK column makes the row exempt (PG MATCH SIMPLE
//!   default).
//! - `enforce_fk_on_delete_or_pk_update` — for DELETE / UPDATE-of-PK
//!   on a parent table, finds all child tables whose FK references it
//!   and either rejects (NO ACTION) or cascades (CASCADE).
//!
//! 5.11.C2 reactor-style constraint reactors share SQLSTATE 23514 with
//! CHECK but live in a separate module (`reactor_ddl`) — those fire on
//! published events; CHECK fires inside the write path.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use arrow_array::{
    Array, BooleanArray, Decimal256Array, FixedSizeBinaryArray, Float64Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Schema};
use basin_catalog::{
    Catalog, CheckConstraint, ForeignKeyDef, RefAction, TableMetadata, UniqueConstraint,
};
use basin_common::{BasinError, ProjectId, Result, TableName};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use crate::convert::{batch_df_to_ws, batch_ws_to_df};
use crate::types::field_is_citext;

// --------------------------------------------------------------------
// Tier 2 — PK / UNIQUE set memoization across batches
// --------------------------------------------------------------------

/// Per-`(project, table)` cached "existing PK set" for the bulk-INSERT
/// constraint scan. Between compactions (the common case) the on-disk file
/// set for a table does not change, so the `existing` HashSet rebuilt on
/// each batch in [`enforce_pk_on_insert`] is byte-identical to the previous
/// batch's set. The cache reuses the prior set when the current file-set
/// signature matches the cached one, and falls back to a full rebuild
/// otherwise.
///
/// **Correctness contract**: the cache reflects only what is on disk via
/// `list_data_files_with_stats`. The shard fast-path memtable + WAL are
/// **not** consulted here; the existing `enforce_pk_on_insert` body has
/// always ignored them for non-tombstone reasons (see the long comment in
/// `enforce_pk_on_insert`), so the cache preserves byte-identical
/// semantics. The signature is the FNV-1a hash of the sorted file paths;
/// any UPDATE/DELETE/COMPACT that drops or replaces a file changes the
/// signature and the cached entry is rebuilt from scratch.
///
/// **Tombstone interaction**: when tombstone suppression is active (single
/// PK + a `MemTableRegistry`), the cached set excludes rows whose PK is
/// tombstoned at cache-build time. A later tombstone added between batches
/// would not invalidate the cache — so the cache is **bypassed** when the
/// caller supplies a non-empty tombstone set. The common bench / bulk-INSERT
/// case has no tombstones, so the bypass does not regress the hot path.
pub(crate) struct PkSetCache {
    inner: RwLock<HashMap<(ProjectId, TableName), Arc<CachedPkSet>>>,
}

pub(crate) struct CachedPkSet {
    pub files_sig: u64,
    /// The data-file paths this PK set was built from. When a later enforcement
    /// finds the file set has only GROWN (new files added, none removed — the
    /// bulk-COPY/append case where each committed chunk adds a file), it seeds
    /// from this cached set and scans ONLY the new files instead of re-reading
    /// every file from cold storage. That turns a PK-table bulk load from
    /// O(files²) cold reads into O(files).
    pub files: Arc<std::collections::HashSet<String>>,
    /// On-disk PK tuples — string-canonical form, matching the shape the
    /// constraint comparator hashes on (`scalar_to_canonical_string`).
    pub set: Arc<std::collections::HashSet<Vec<String>>>,
    /// Fast-path for single Int64 PKs. When `Some`, callers may avoid the
    /// `Vec<String>` allocation per cold row and probe directly. When the
    /// PK shape is anything else this is `None` and callers fall back to
    /// `set` above. See Tier 3.
    pub set_i64: Option<Arc<std::collections::HashSet<i64>>>,
}

impl PkSetCache {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    fn get(&self, project: &ProjectId, table: &TableName) -> Option<Arc<CachedPkSet>> {
        self.inner
            .read()
            .expect("pk_set_cache poisoned")
            .get(&(*project, table.clone()))
            .cloned()
    }

    fn put(&self, project: &ProjectId, table: &TableName, entry: Arc<CachedPkSet>) {
        self.inner
            .write()
            .expect("pk_set_cache poisoned")
            .insert((*project, table.clone()), entry);
    }

    /// Drop any cached entry for `(project, table)`. Called whenever the
    /// on-disk file set for the table changes (UPDATE/DELETE replace, DROP
    /// TABLE, COMPACT, etc.). Cheap: O(1) write-lock + remove.
    pub(crate) fn invalidate(&self, project: &ProjectId, table: &TableName) {
        self.inner
            .write()
            .expect("pk_set_cache poisoned")
            .remove(&(*project, table.clone()));
    }

    /// Drop every cached entry for `project`. Used on project deletion.
    #[allow(dead_code)]
    pub(crate) fn invalidate_project(&self, project: &ProjectId) {
        self.inner
            .write()
            .expect("pk_set_cache poisoned")
            .retain(|(p, _), _| p != project);
    }
}

/// FNV-1a hash of the sorted file paths in the current `(project, table)`
/// data-file set. Stable across calls when the set is unchanged.
fn files_signature(files: &[basin_storage::DataFile]) -> u64 {
    let mut paths: Vec<&str> = files.iter().map(|f| f.path.as_ref()).collect();
    paths.sort_unstable();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for p in &paths {
        p.hash(&mut hasher);
        0u8.hash(&mut hasher); // separator
    }
    hasher.finish()
}

// --------------------------------------------------------------------
// PRIMARY KEY enforcement
// --------------------------------------------------------------------

/// Reject the new batch if any row's PK tuple already exists in the
/// table, OR if two rows in the same batch share a PK tuple.
///
/// The optional `registry` parameter gates tombstone-aware filtering: when
/// `Some`, any cold-tier row whose PK RowKey has a `MemRowValue::Tombstone`
/// in the registry is treated as logically absent.  This allows re-INSERT
/// of a fast-path-DELETEd PK without a spurious `UniqueViolation`.
///
/// Cost note: this scans the entire `(project, table)` data file set
/// once per call. v0.2 (Phase 5.7 B1) secondary indexes will replace
/// the scan with a B-tree probe. For v0.1 the per-INSERT cost is
/// `O(rows_in_table + rows_in_batch)`.
pub(crate) async fn enforce_pk_on_insert(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    pk_columns: &[String],
    batch: &RecordBatch,
    registry: Option<(&basin_hottier::MemTableRegistry, &ProjectId)>,
    pk_cache: Option<&PkSetCache>,
) -> Result<()> {
    if pk_columns.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }

    // Resolve PK column indexes in the batch's schema.
    let pk_idx: Vec<usize> = pk_columns
        .iter()
        .map(|c| {
            batch
                .schema()
                .index_of(c)
                .map_err(|_| BasinError::internal(format!("PK column {c:?} missing from batch")))
        })
        .collect::<Result<Vec<_>>>()?;

    // Tier 3 fast-path detection: single Int64 PK. Avoids the `Vec<String>`
    // allocation per cold row when probing the existing set.
    let single_i64_pk = pk_columns.len() == 1
        && batch
            .schema()
            .field(pk_idx[0])
            .data_type()
            == &DataType::Int64;

    // 1. Intra-batch dup check.
    //
    // Tier-3 fastpath: when the PK is a single Int64 column the generic
    // `pk_tuple_for_row` -> `Vec<String>` -> `HashSet<Vec<String>>` path
    // allocates two strings + a vec per row. For bulk INSERT (10k+ rows)
    // that dominates this stage. Probe a `HashSet<i64>` directly off the
    // Int64Array and only fall through to the generic path for other PK
    // shapes (composite, non-i64).
    if single_i64_pk {
        let arr = batch
            .column(pk_idx[0])
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                BasinError::internal("single_i64_pk detected but column is not Int64Array")
            })?;
        let mut seen_i64: std::collections::HashSet<i64> =
            std::collections::HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if arr.is_null(row) {
                return Err(BasinError::CheckViolation(format!(
                    "null value in column violates not-null constraint on PRIMARY KEY of \
                     \"{table_name_str}\""
                )));
            }
            let v = arr.value(row);
            if !seen_i64.insert(v) {
                return Err(BasinError::UniqueViolation(format!(
                    "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                     Key ({})=({}) already exists.",
                    pk_columns.join(", "),
                    v
                )));
            }
        }
    } else {
        let mut seen: std::collections::HashSet<Vec<String>> =
            std::collections::HashSet::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = pk_tuple_for_row(batch, &pk_idx, row)?;
            // Any null in the PK is a hard error — PK columns are NOT
            // NULL by construction, but defend against future regressions.
            if key.is_none() {
                return Err(BasinError::CheckViolation(format!(
                    "null value in column violates not-null constraint on PRIMARY KEY of \
                     \"{table_name_str}\""
                )));
            }
            let k = key.unwrap();
            if !seen.insert(k.clone()) {
                return Err(BasinError::UniqueViolation(format!(
                    "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                     Key ({})=({}) already exists.",
                    pk_columns.join(", "),
                    k.join(", ")
                )));
            }
        }
    }

    // 2. Existing-table scan. Read every data file's PK columns.
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    if data_files.is_empty() {
        return Ok(());
    }

    // Pre-compute the tombstone set from the registry once, outside the file
    // loop.  We only attempt tombstone filtering for single-column PKs (the
    // only shape the hot-tier fast-path tombstone writer produces today).
    // For composite PKs `tombstone_keys` stays empty and all cold rows are
    // included in `existing` — the conservative (but always-correct) path.
    let tombstone_keys: std::collections::HashSet<Vec<u8>> = if let Some((reg, proj)) = registry {
        if pk_columns.len() == 1 {
            collect_tombstone_keys(reg, proj, table)
        } else {
            std::collections::HashSet::new()
        }
    } else {
        std::collections::HashSet::new()
    };
    // DataType of the single PK column — needed for tombstone RowKey encoding.
    // Derived from the batch schema (authoritative for this INSERT).
    let pk_dt_for_tombstone: Option<DataType> = if tombstone_keys.is_empty() {
        None
    } else {
        // pk_columns.len() == 1 is guaranteed by the branch above.
        pk_columns.first().and_then(|c| {
            batch
                .schema()
                .field_with_name(c)
                .ok()
                .map(|f| f.data_type().clone())
        })
    };

    // Tier 1 (projection pushdown) + Tier 2 (cross-batch memoization) +
    // Tier 3 (i64 fast-path) all converge here.
    //
    // The cache stores the on-disk PK set keyed by the FNV-1a hash of the
    // sorted file paths. Between compactions (the common bulk-INSERT case),
    // the file set does not change → cache hit → no cold scan at all.
    // When the file set changes (a new file appears, an old one is
    // replaced) the signature differs and we rebuild from scratch.
    //
    // Tombstone-active paths bypass the cache entirely (the cached set
    // was built against an older tombstone snapshot and might admit a
    // re-INSERT that should now be allowed, or vice-versa). Bypass is
    // safe — it falls back to the old (still-correct) full scan path.
    let files_sig = files_signature(&data_files);
    let cache_eligible = tombstone_keys.is_empty();
    // Path set of the current live files — used both to detect the "files only
    // grew" case (incremental extend) and stored on the cache entry we write.
    let current_paths: Arc<std::collections::HashSet<String>> =
        Arc::new(data_files.iter().map(|f| f.path.to_string()).collect());
    let raw_cached: Option<Arc<CachedPkSet>> = if cache_eligible {
        pk_cache.and_then(|c| c.get(project, table))
    } else {
        None
    };
    let cached_entry: Option<Arc<CachedPkSet>> = raw_cached
        .as_ref()
        .filter(|entry| entry.files_sig == files_sig)
        .cloned();

    if let Some(entry) = cached_entry {
        // Cache hit. Probe directly against the cached set.
        if single_i64_pk {
            if let Some(set_i64) = entry.set_i64.as_ref() {
                let arr = batch
                    .column(pk_idx[0])
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| BasinError::internal("Int64Array downcast (batch PK)"))?;
                for row in 0..batch.num_rows() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let v = arr.value(row);
                    if set_i64.contains(&v) {
                        return Err(BasinError::UniqueViolation(format!(
                            "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                             Key ({})=({}) already exists.",
                            pk_columns.join(", "),
                            v
                        )));
                    }
                }
                return Ok(());
            }
        }
        // Composite-key / other-type path.
        for row in 0..batch.num_rows() {
            let Some(k) = pk_tuple_for_row(batch, &pk_idx, row)? else {
                continue;
            };
            if entry.set.contains(&k) {
                return Err(BasinError::UniqueViolation(format!(
                    "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                     Key ({})=({}) already exists.",
                    pk_columns.join(", "),
                    k.join(", ")
                )));
            }
        }
        return Ok(());
    }

    // Cache miss: rebuild the existing PK set. If a cached set was built from a
    // SUBSET of the current files (only additions since — the bulk-COPY case
    // where each committed chunk adds a file), seed from it and scan ONLY the
    // newly-added files. Otherwise (first build, or files were replaced by a
    // compaction → not a subset) rebuild from scratch over every file. Uses
    // projection pushdown to read only the PK columns, skipping JSONB / TEXT.
    let read_opts = basin_storage::ReadOptions {
        projection: Some(pk_columns.to_vec()),
        ..Default::default()
    };
    let incremental_base = raw_cached
        .as_ref()
        .filter(|entry| cache_eligible && entry.files.is_subset(&current_paths));
    let (mut existing, mut existing_i64, scan_files): (
        std::collections::HashSet<Vec<String>>,
        Option<std::collections::HashSet<i64>>,
        Vec<&basin_storage::DataFile>,
    ) = match incremental_base {
        Some(entry) => {
            let base = entry.set.as_ref().clone();
            let base_i64 = if single_i64_pk {
                Some(
                    entry
                        .set_i64
                        .as_ref()
                        .map(|s| s.as_ref().clone())
                        .unwrap_or_default(),
                )
            } else {
                None
            };
            let new_files: Vec<&basin_storage::DataFile> = data_files
                .iter()
                .filter(|f| !entry.files.contains(f.path.as_ref()))
                .collect();
            (base, base_i64, new_files)
        }
        None => {
            let ex = std::collections::HashSet::new();
            let ex_i64 = if single_i64_pk {
                Some(std::collections::HashSet::new())
            } else {
                None
            };
            (ex, ex_i64, data_files.iter().collect())
        }
    };
    for f in scan_files {
        let mut stream = storage
            .read_file_with_options(project, &f.path, read_opts.clone())
            .await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            let rb_pk_idx: Vec<usize> = pk_columns
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!("PK column {c:?} missing from data file"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            // Tier 3 hot loop for single-Int64 PKs. We still build the
            // string `existing` set in parallel so cache fill and the
            // tombstone path remain byte-compatible; the i64 probe is
            // additive.
            if single_i64_pk {
                let arr = rb
                    .column(rb_pk_idx[0])
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| BasinError::internal("Int64Array downcast (cold PK)"))?;
                for row in 0..rb.num_rows() {
                    if arr.is_null(row) {
                        continue;
                    }
                    // Tombstone suppression on the same row.
                    if !tombstone_keys.is_empty() {
                        if let Some(dt) = &pk_dt_for_tombstone {
                            let col = rb.column(rb_pk_idx[0]);
                            if let Some(rk) = crate::constraint_union::array_value_to_row_key(
                                col.as_ref(),
                                row,
                                dt,
                            ) {
                                if tombstone_keys.contains(rk.as_bytes()) {
                                    continue;
                                }
                            }
                        }
                    }
                    let v = arr.value(row);
                    if let Some(s) = existing_i64.as_mut() {
                        s.insert(v);
                    }
                    existing.insert(vec![v.to_string()]);
                }
            } else {
                for row in 0..rb.num_rows() {
                    if !tombstone_keys.is_empty() {
                        if let Some(dt) = &pk_dt_for_tombstone {
                            let col = rb.column(rb_pk_idx[0]);
                            if let Some(rk) = crate::constraint_union::array_value_to_row_key(
                                col.as_ref(),
                                row,
                                dt,
                            ) {
                                if tombstone_keys.contains(rk.as_bytes()) {
                                    continue;
                                }
                            }
                        }
                    }
                    if let Some(k) = pk_tuple_for_row(&rb, &rb_pk_idx, row)? {
                        existing.insert(k);
                    }
                }
            }
        }
    }

    // Probe the batch against the freshly-built set.
    if single_i64_pk {
        if let Some(set_i64) = existing_i64.as_ref() {
            let arr = batch
                .column(pk_idx[0])
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BasinError::internal("Int64Array downcast (batch PK)"))?;
            for row in 0..batch.num_rows() {
                if arr.is_null(row) {
                    continue;
                }
                let v = arr.value(row);
                if set_i64.contains(&v) {
                    // Fill cache before returning so a retry sees the same set.
                    if cache_eligible {
                        if let Some(c) = pk_cache {
                            c.put(
                                project,
                                table,
                                Arc::new(CachedPkSet {
                                    files_sig,
                                    files: current_paths.clone(),
                                    set: Arc::new(existing),
                                    set_i64: Some(Arc::new(set_i64.clone())),
                                }),
                            );
                        }
                    }
                    return Err(BasinError::UniqueViolation(format!(
                        "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                         Key ({})=({}) already exists.",
                        pk_columns.join(", "),
                        v
                    )));
                }
            }
            if cache_eligible {
                if let Some(c) = pk_cache {
                    c.put(
                        project,
                        table,
                        Arc::new(CachedPkSet {
                            files_sig,
                            files: current_paths.clone(),
                            set: Arc::new(existing),
                            set_i64: Some(Arc::new(set_i64.clone())),
                        }),
                    );
                }
            }
            return Ok(());
        }
    }
    for row in 0..batch.num_rows() {
        let Some(k) = pk_tuple_for_row(batch, &pk_idx, row)? else {
            continue;
        };
        if existing.contains(&k) {
            if cache_eligible {
                if let Some(c) = pk_cache {
                    c.put(
                        project,
                        table,
                        Arc::new(CachedPkSet {
                            files_sig,
                            files: current_paths.clone(),
                            set: Arc::new(existing),
                            set_i64: existing_i64.map(Arc::new),
                        }),
                    );
                }
            }
            return Err(BasinError::UniqueViolation(format!(
                "duplicate key value violates unique constraint \"{table_name_str}_pkey\": \
                 Key ({})=({}) already exists.",
                pk_columns.join(", "),
                k.join(", ")
            )));
        }
    }
    if cache_eligible {
        if let Some(c) = pk_cache {
            c.put(
                project,
                table,
                Arc::new(CachedPkSet {
                    files_sig,
                    files: current_paths.clone(),
                    set: Arc::new(existing),
                    set_i64: existing_i64.map(Arc::new),
                }),
            );
        }
    }
    Ok(())
}

/// Snapshot the tombstone `RowKey` bytes from the registry for `(project, table)`.
/// Returns an empty set when the registry has no entry for the table.
fn collect_tombstone_keys(
    registry: &basin_hottier::MemTableRegistry,
    project: &ProjectId,
    table: &TableName,
) -> std::collections::HashSet<Vec<u8>> {
    let Some(entry) = registry.get(project, table) else {
        return std::collections::HashSet::new();
    };
    let snap = entry.memtable.snapshot();
    snap.into_iter()
        .filter_map(|(key, val)| {
            if matches!(val, basin_hottier::MemRowValue::Tombstone) {
                Some(key.as_bytes().to_vec())
            } else {
                None
            }
        })
        .collect()
}

/// Reject the new batch if any row's UNIQUE-constraint tuple already
/// exists in the table, OR if two rows in the same batch share a
/// UNIQUE tuple.
///
/// NULL handling: PG's default `UNIQUE (...)` treats every NULL as
/// distinct — any number of rows may have NULL in a UNIQUE column.
/// We mirror that: rows with NULL in *any* UNIQUE column are skipped
/// entirely from the comparison. (`NULLS NOT DISTINCT`, introduced
/// in PG 15, is not modelled in v0.1.)
///
/// Cost note: same shape as `enforce_pk_on_insert` — one full scan
/// of every data file per UNIQUE constraint per call. v0.2 (Phase 5.7
/// B1) secondary indexes will replace the scan with a B-tree probe.
/// For v0.1 the per-INSERT cost is `O(rows_in_table * unique_count +
/// rows_in_batch * unique_count)`. basin-auth's targeted tables
/// (users, sessions, api_keys, refresh_tokens, magic_links) stay in
/// the thousands-to-low-millions range where the scan is fine.
pub(crate) async fn enforce_unique_on_insert(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    unique_constraints: &[UniqueConstraint],
    batch: &RecordBatch,
    // Tombstone-aware: `registry` + `pk_columns` let the cold scan skip rows
    // freed by a prior hot-tier DELETE, so re-INSERTing a UNIQUE value of a
    // deleted row succeeds (mirrors `enforce_pk_on_insert`).
    registry: Option<(&basin_hottier::MemTableRegistry, &ProjectId)>,
    pk_columns: &[String],
) -> Result<()> {
    if unique_constraints.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    // Single-PK tombstone set + dtype, computed once for all constraints.
    let tombstone_keys: std::collections::HashSet<Vec<u8>> = match registry {
        Some((reg, proj)) if pk_columns.len() == 1 => collect_tombstone_keys(reg, proj, table),
        _ => std::collections::HashSet::new(),
    };
    let pk_dt: Option<DataType> = if tombstone_keys.is_empty() {
        None
    } else {
        pk_columns.first().and_then(|c| {
            batch.schema().field_with_name(c).ok().map(|f| f.data_type().clone())
        })
    };
    for u in unique_constraints {
        enforce_one_unique(
            storage,
            project,
            table,
            table_name_str,
            &u.name,
            &u.columns,
            batch,
            &[],
            &tombstone_keys,
            pk_dt.as_ref(),
            pk_columns,
        )
        .await?;
    }
    Ok(())
}

/// ALTER-side UNIQUE backfill validation: verify that the rows ALREADY in
/// the table satisfy a UNIQUE constraint about to be registered
/// (`ALTER TABLE … ADD CONSTRAINT <name> UNIQUE (cols)`). Same scan shape
/// as [`enforce_one_unique`]'s existing-table pass — projection-pushdown
/// read of just the constraint columns over every live data file — but
/// with no incoming batch: the duplicate check is existing-vs-existing.
///
/// NULL handling matches the INSERT-side enforcement (PG default): a row
/// with NULL in *any* constraint column is exempt from the check.
///
/// citext case-folding is derived from the CATALOG schema (`schema`),
/// which carries the `BASIN_TYPE` field metadata that the Parquet
/// read-back schema loses (see the metadata note inside
/// [`enforce_one_unique`]).
///
/// The caller (executor's `exec_alter_table`) has already materialized any
/// hot-tier overlay into the cold tier before the ALTER dispatch, so the
/// cold-file scan here sees every committed row.
pub(crate) async fn verify_unique_over_existing(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    constraint_name: &str,
    columns: &[String],
    schema: &Schema,
) -> Result<()> {
    if columns.is_empty() {
        return Ok(());
    }
    let citext_positions: std::collections::HashSet<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            schema
                .field_with_name(c)
                .map(field_is_citext)
                .unwrap_or(false)
        })
        .map(|(i, _)| i)
        .collect();
    let read_opts = basin_storage::ReadOptions {
        projection: Some(columns.to_vec()),
        ..Default::default()
    };
    let mut seen: std::collections::HashSet<Vec<String>> = Default::default();
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    for f in &data_files {
        let mut stream = storage
            .read_file_with_options(project, &f.path, read_opts.clone())
            .await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            // A column added via ALTER TABLE ADD COLUMN after this file was
            // written is physically absent from the file; its value is NULL
            // for every row here, which exempts those rows from the check
            // (PG NULLS-distinct default) — skip the batch.
            let rb_idx: Option<Vec<usize>> = columns
                .iter()
                .map(|c| rb.schema().index_of(c).ok())
                .collect();
            let Some(rb_idx) = rb_idx else { continue };
            for row in 0..rb.num_rows() {
                let Some(k) = pk_tuple_for_row_citext(&rb, &rb_idx, row, &citext_positions)?
                else {
                    continue;
                };
                if !seen.insert(k.clone()) {
                    return Err(BasinError::UniqueViolation(format!(
                        "could not create unique constraint \"{constraint_name}\" on table \
                         \"{table}\": Key ({})=({}) is duplicated.",
                        columns.join(", "),
                        k.join(", ")
                    )));
                }
            }
        }
    }
    Ok(())
}

/// UPDATE-side UNIQUE enforcement: same as `enforce_unique_on_insert`
/// but excludes data files in `replaced_paths` from the
/// existing-table scan (those files are being rewritten in this
/// transaction so the rows they contain aren't really "still present").
pub(crate) async fn enforce_unique_on_update(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    unique_constraints: &[UniqueConstraint],
    batches: &[RecordBatch],
    replaced_paths: &[String],
) -> Result<()> {
    if unique_constraints.is_empty() {
        return Ok(());
    }
    for u in unique_constraints {
        for b in batches {
            enforce_one_unique(
                storage,
                project,
                table,
                table_name_str,
                &u.name,
                &u.columns,
                b,
                replaced_paths,
                // UPDATE path excludes rewritten files via `replaced_paths`;
                // tombstone suppression is not threaded here (no registry).
                &std::collections::HashSet::new(),
                None,
                &[],
            )
            .await?;
        }
    }
    Ok(())
}

/// Workhorse for both the INSERT and UPDATE flavours. Scans the
/// (non-replaced) data files, then walks `batch` row-by-row checking
/// for collisions both against the existing set and within the batch
/// itself. NULL in any UNIQUE column makes the row exempt.
#[allow(clippy::too_many_arguments)]
async fn enforce_one_unique(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    constraint_name: &str,
    columns: &[String],
    batch: &RecordBatch,
    replaced_paths: &[String],
    // Tombstone suppression (single-column PK only): cold rows whose PK is
    // tombstoned in the hot-tier registry are logically deleted and must NOT
    // count as existing values — otherwise a re-INSERT of a UNIQUE value freed
    // by a prior hot-tier DELETE wrongly fails (the delete_then_reinsert bug).
    // Empty `tombstone_keys` (composite PK / no registry) = conservative scan.
    tombstone_keys: &std::collections::HashSet<Vec<u8>>,
    pk_dt: Option<&DataType>,
    pk_columns: &[String],
) -> Result<()> {
    if columns.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    let col_idx: Vec<usize> = columns
        .iter()
        .map(|c| {
            batch.schema().index_of(c).map_err(|_| {
                BasinError::internal(format!("UNIQUE column {c:?} missing from batch"))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Determine which constraint columns are citext so we can apply
    // case-folding when building the dedup key. citext UNIQUE treats
    // 'Foo' and 'foo' as duplicates (PG citext extension semantics).
    let batch_schema = batch.schema();
    let citext_positions: std::collections::HashSet<usize> = col_idx
        .iter()
        .enumerate()
        .filter_map(|(pos, &arr_idx)| {
            let field = batch_schema.field(arr_idx);
            if field_is_citext(field) { Some(pos) } else { None }
        })
        .collect();

    // 1. Existing-table scan (skip replaced files on UPDATE).
    let replaced: std::collections::HashSet<&str> =
        replaced_paths.iter().map(|s| s.as_str()).collect();
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    // Tier 1: projection pushdown — read only the UNIQUE columns (plus the
    // PK column when tombstone suppression is active, so we can look up the
    // RowKey). Skips JSONB / TEXT payload decode entirely.
    let mut projection_cols: Vec<String> = columns.to_vec();
    if !tombstone_keys.is_empty() {
        if let Some(pk_col) = pk_columns.first() {
            if !projection_cols.iter().any(|c| c == pk_col) {
                projection_cols.push(pk_col.clone());
            }
        }
    }
    let read_opts = basin_storage::ReadOptions {
        projection: Some(projection_cols),
        ..Default::default()
    };
    let mut existing: std::collections::HashSet<Vec<String>> = Default::default();
    for f in &data_files {
        if replaced.contains(f.path.as_ref()) {
            continue;
        }
        let mut stream = storage
            .read_file_with_options(project, &f.path, read_opts.clone())
            .await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            let rb_idx: Vec<usize> = columns
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!("UNIQUE column {c:?} missing from data file"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            // PK column index in this cold batch, for tombstone suppression
            // (single-column PK only — same shape the hot-tier tombstone writer
            // produces). None disables suppression (composite PK / no PK col).
            let rb_pk_idx: Option<usize> = if tombstone_keys.is_empty() || pk_dt.is_none() {
                None
            } else {
                pk_columns.first().and_then(|c| rb.schema().index_of(c).ok())
            };
            // Reuse `citext_positions` derived from the INSERT batch's schema
            // (which HAS Arrow field metadata) rather than re-deriving from the
            // Parquet-read-back schema.  Parquet's `ArrowWriter` does NOT
            // preserve Arrow field metadata in the per-batch schema emitted by
            // the reader — only the `ARROW:schema` blob in the file footer
            // carries it, and `read_file` reads without a catalog_schema hint
            // so the re-stamping path does not run here.  The INSERT batch
            // schema and the on-disk schema have the same columns in the same
            // logical positions for the columns listed in the constraint, so
            // reusing `citext_positions` is correct.
            for row in 0..rb.num_rows() {
                // Skip rows whose PK is tombstoned (logically deleted) so a
                // re-INSERT of a freed UNIQUE value is allowed.
                if let (Some(pk_i), Some(dt)) = (rb_pk_idx, pk_dt) {
                    if let Some(rk) = crate::constraint_union::array_value_to_row_key(
                        rb.column(pk_i).as_ref(),
                        row,
                        dt,
                    ) {
                        if tombstone_keys.contains(rk.as_bytes()) {
                            continue;
                        }
                    }
                }
                if let Some(k) =
                    pk_tuple_for_row_citext(&rb, &rb_idx, row, &citext_positions)?
                {
                    existing.insert(k);
                }
            }
        }
    }

    // 2. Intra-batch + existing check.
    let mut seen: std::collections::HashSet<Vec<String>> = Default::default();
    for row in 0..batch.num_rows() {
        // NULL in any column → row exempt (PG default).
        let Some(k) = pk_tuple_for_row_citext(batch, &col_idx, row, &citext_positions)? else {
            continue;
        };
        if existing.contains(&k) || !seen.insert(k.clone()) {
            return Err(BasinError::UniqueViolation(format!(
                "duplicate key value violates unique constraint \"{constraint_name}\" on \
                 table \"{table_name_str}\": Key ({})=({}) already exists.",
                columns.join(", "),
                k.join(", ")
            )));
        }
    }
    Ok(())
}

/// Variant of `pk_tuple_for_row` that lower-cases string values for columns
/// whose position in the key is in `citext_positions` (citext column indexes).
/// This ensures UNIQUE constraints on citext columns treat 'Foo' and 'foo'
/// as duplicates, matching PostgreSQL citext semantics.
fn pk_tuple_for_row_citext(
    batch: &RecordBatch,
    pk_idx: &[usize],
    row: usize,
    citext_positions: &std::collections::HashSet<usize>,
) -> Result<Option<Vec<String>>> {
    let mut out = Vec::with_capacity(pk_idx.len());
    for (pos, &i) in pk_idx.iter().enumerate() {
        let arr = batch.column(i);
        if arr.is_null(row) {
            return Ok(None);
        }
        let s = scalar_to_canonical_string(arr.as_ref(), row)?;
        // Apply case-folding for citext columns so 'Foo' == 'foo'.
        let s = if citext_positions.contains(&pos) {
            s.to_lowercase()
        } else {
            s
        };
        out.push(s);
    }
    Ok(Some(out))
}

/// Format a row's PK tuple as a Vec<String>. Returns None if any PK
/// column is null (caller decides whether that's a violation; for PK
/// columns it always is, for FK columns it makes the row exempt).
pub(crate) fn pk_tuple_for_row(
    batch: &RecordBatch,
    pk_idx: &[usize],
    row: usize,
) -> Result<Option<Vec<String>>> {
    let mut out = Vec::with_capacity(pk_idx.len());
    for &i in pk_idx {
        let arr = batch.column(i);
        if arr.is_null(row) {
            return Ok(None);
        }
        out.push(scalar_to_canonical_string(arr.as_ref(), row)?);
    }
    Ok(Some(out))
}

/// Convert an Arrow scalar at `(arr, row)` into a canonical string the
/// PK tuple comparison hashes on. Two rows with equal logical values
/// must hash to the same string.
fn scalar_to_canonical_string(arr: &dyn Array, row: usize) -> Result<String> {
    match arr.data_type() {
        DataType::Int16 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| BasinError::internal("Int16Array downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Int32 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| BasinError::internal("Int32Array downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BasinError::internal("Int64Array downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Float64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| BasinError::internal("Float64Array downcast"))?;
            // Use a fixed precision so 1.0 == 1.000000 hash the same.
            Ok(format!("{:?}", a.value(row)))
        }
        DataType::Utf8 => {
            let a = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| BasinError::internal("StringArray downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Boolean => {
            let a = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| BasinError::internal("BooleanArray downcast"))?;
            Ok(a.value(row).to_string())
        }
        DataType::Timestamp(_, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .ok_or_else(|| BasinError::internal("TimestampMicrosecondArray downcast"))?;
            Ok(a.value(row).to_string())
        }
        // UUID columns land as `FixedSizeBinary(16)` (see
        // `crate::types::arrow_data_type`). Canonicalise by formatting the
        // 16 bytes back through `uuid::Uuid::from_bytes` so two PK rows
        // with identical UUIDs hash to the same string regardless of byte
        // ordering quirks.
        DataType::FixedSizeBinary(16) => {
            let a = arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| BasinError::internal("FixedSizeBinaryArray downcast"))?;
            let bytes = a.value(row);
            let mut buf = [0u8; 16];
            buf.copy_from_slice(bytes);
            Ok(uuid::Uuid::from_bytes(buf).hyphenated().to_string())
        }
        // UUID columns in Vortex cold storage: Vortex has no FixedSizeBinary(N)
        // encoder, so the writer reinterprets UUID FSB(16) as Decimal256(39,0)
        // (left-zero-padded big-endian i256, lower 16 bytes = UUID).  When the
        // constraint cold-scan uses `read_file_with_options` without a catalog
        // schema the restamp + restore chain never runs, so the UUID arrives
        // here as Decimal256.  We unpack the UUID bytes via the same inverse
        // used in `basin_storage::reader::decimal256_to_uuid_fsb`.
        DataType::Decimal256(39, 0) => {
            let a = arr
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| BasinError::internal("Decimal256Array downcast for UUID"))?;
            let full = a.value(row).to_be_bytes(); // 32 bytes; UUID is [16..32]
            let mut buf = [0u8; 16];
            buf.copy_from_slice(&full[16..32]);
            Ok(uuid::Uuid::from_bytes(buf).hyphenated().to_string())
        }
        other => Err(BasinError::InvalidSchema(format!(
            "PRIMARY KEY / FOREIGN KEY column type {other:?} is not supported in v0.1 (use \
             BIGINT / INTEGER / SMALLINT / TEXT / BOOLEAN / TIMESTAMPTZ / UUID)"
        ))),
    }
}

// --------------------------------------------------------------------
// CHECK enforcement
// --------------------------------------------------------------------

/// Normalise a stored CHECK predicate so it is a bare boolean expression
/// suitable for `SELECT (<expr>) FROM t`.
///
/// The column-level DDL path historically called `CheckConstraint::to_string()`
/// which renders `CHECK (<inner>)` including the keyword wrapper. DataFusion
/// would then see `check(...)` as an unknown scalar function call. This helper
/// strips a leading (case-insensitive) `CHECK` keyword followed by a balanced
/// outer `(...)`, returning only the inner expression text. If the predicate
/// does not start with the `CHECK` keyword the function is a no-op (idempotent).
///
/// Robustness: we locate the matching close-paren by counting nesting depth
/// rather than naive string-slicing, so expressions that contain function
/// calls with their own parentheses are handled correctly.
fn strip_check_wrapper(predicate: &str) -> &str {
    let trimmed = predicate.trim();
    // Case-insensitive prefix check for "CHECK".
    let after_kw = if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("CHECK") {
        trimmed[5..].trim_start()
    } else {
        return predicate;
    };
    // Expect the next character to be '('.
    if !after_kw.starts_with('(') {
        return predicate;
    }
    // Walk the string tracking paren depth to find the matching ')'.
    let mut depth: usize = 0;
    let mut close_pos: Option<usize> = None;
    for (i, ch) in after_kw.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_pos = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    match close_pos {
        // The matching ')' must be the last non-whitespace character.
        Some(end) if after_kw[end + 1..].trim().is_empty() => &after_kw[1..end],
        _ => predicate,
    }
}

/// Evaluate every CHECK predicate against the batch via DataFusion.
/// Rows where the predicate is false (or NULL — PG treats NULL as
/// satisfying the predicate, so we accept NULL) raise SQLSTATE 23514.
///
/// Predicates that start with the sentinel prefix `__basin_excl:` are NOT
/// DataFusion expressions — they encode an `EXCLUDE USING gist` constraint
/// stored at DDL time. Those are dispatched to
/// [`enforce_one_exclusion_constraint`] which performs a table scan and raises
/// SQLSTATE 23P01 (`exclusion_violation`) on conflict.
pub(crate) async fn enforce_check_constraints(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    schema: &Schema,
    checks: &[CheckConstraint],
    batch: &RecordBatch,
) -> Result<()> {
    if checks.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    // One DataFusion context per call; the predicates are evaluated as
    // projections against a temp `t` table whose schema is the input
    // batch.
    let ctx = SessionContext::new();
    crate::udf::register_distance_udfs(&ctx);
    crate::udf::register_pg_udfs(&ctx);
    crate::udf::register_pg_compat_udfs(&ctx);

    let df_batch = batch_ws_to_df(batch)?;
    let df_schema = df_batch.schema();
    let provider = MemTable::try_new(df_schema, vec![vec![df_batch]])
        .map_err(|e| BasinError::internal(format!("CHECK MemTable: {e}")))?;
    ctx.register_table("t", Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("CHECK register: {e}")))?;

    for c in checks {
        // Phase 5.24.F: sentinel predicates encode EXCLUDE USING gist constraints.
        // Route them to the exclusion enforcer instead of DataFusion evaluation.
        if let Some(spec) = crate::ddl::parse_exclusion_sentinel(&c.predicate) {
            enforce_one_exclusion_constraint(
                storage,
                project,
                table,
                table_name_str,
                &spec,
                batch,
            )
            .await?;
            continue;
        }

        // Normalize: strip any leading CHECK (...) wrapper that may have been
        // stored by an earlier version of the DDL path, making evaluation
        // idempotent whether the stored form is bare or wrapped.
        let bare = strip_check_wrapper(&c.predicate);
        let sql = format!("SELECT ({bare}) AS ok FROM t");
        let df = ctx.sql(&sql).await.map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CHECK constraint {:?} predicate {:?}: {e}",
                c.name, c.predicate
            ))
        })?;
        let results = df.collect().await.map_err(|e| {
            BasinError::InvalidSchema(format!("CHECK constraint {:?} evaluation: {e}", c.name))
        })?;
        let mut row_global: usize = 0;
        for rb in &results {
            let ws_rb = batch_df_to_ws(rb)?;
            let ok = ws_rb
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BasinError::InvalidSchema(format!(
                        "CHECK constraint {:?}: predicate must return BOOLEAN",
                        c.name
                    ))
                })?;
            for r in 0..ws_rb.num_rows() {
                // PG: NULL satisfies CHECK (only FALSE fails). Match.
                if !ok.is_null(r) && !ok.value(r) {
                    let row_text = render_row_as_pg_tuple(schema, batch, row_global)?;
                    return Err(BasinError::CheckViolation(format!(
                        "new row for relation \"{table_name_str}\" violates check constraint \
                         \"{}\": Failing row contains ({row_text}).",
                        c.name
                    )));
                }
                row_global += 1;
            }
        }
    }
    Ok(())
}

// --------------------------------------------------------------------
// EXCLUSION constraint enforcement (Phase 5.24.F)
// --------------------------------------------------------------------

/// Enforce one `EXCLUDE USING gist` constraint stored as a sentinel
/// `CheckConstraint` predicate (see `ddl::encode_exclusion_sentinel`).
///
/// For each new row in `batch`, scans every existing row in the table and
/// checks whether *all* element conditions hold simultaneously:
///
/// - `col WITH =`  → the existing row's column value equals the new row's.
/// - `col WITH &&` → the existing range column overlaps the new row's range.
///
/// If all conditions hold, the row pair violates the exclusion constraint and
/// we return `BasinError::CheckViolation` with SQLSTATE 23P01 in the message
/// so downstream tests / the router can classify it correctly.
///
/// Cost: `O(new_rows × existing_rows)` per constraint — same scan pattern as
/// `enforce_pk_on_insert`. Acceptable for v0.1; Phase 5.24.G can add a GIST
/// probe path once the index is queryable at enforcement time.
async fn enforce_one_exclusion_constraint(
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table: &TableName,
    table_name_str: &str,
    spec: &crate::ddl::ExcludeConstraintSpec,
    batch: &RecordBatch,
) -> Result<()> {
    if spec.elements.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }

    // Build column-index maps for the new batch.
    let new_col_idx: Vec<usize> = spec
        .elements
        .iter()
        .map(|e| {
            batch.schema().index_of(&e.column).map_err(|_| {
                BasinError::internal(format!(
                    "exclusion constraint column {:?} not in batch",
                    e.column
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Collect all new-row tuples: Vec<(row_idx, Vec<String>)>.
    let mut new_tuples: Vec<(usize, Vec<String>)> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut vals = Vec::with_capacity(spec.elements.len());
        let mut any_null = false;
        for &ci in &new_col_idx {
            let arr = batch.column(ci);
            if arr.is_null(row) {
                any_null = true;
                break;
            }
            vals.push(scalar_to_string_for_excl(arr.as_ref(), row)?);
        }
        if !any_null {
            new_tuples.push((row, vals));
        }
    }
    if new_tuples.is_empty() {
        return Ok(());
    }

    // Intra-batch check: two new rows must not conflict with each other.
    for i in 0..new_tuples.len() {
        for j in (i + 1)..new_tuples.len() {
            if exclusion_tuples_conflict(&spec.elements, &new_tuples[i].1, &new_tuples[j].1)? {
                return Err(BasinError::CheckViolation(format!(
                    "conflicting key value violates exclusion constraint \"{name}\" \
                     on table \"{table_name_str}\" (SQLSTATE 23P01): \
                     two rows in the same INSERT conflict with each other.",
                    name = spec.name
                )));
            }
        }
    }

    // Existing-row scan.
    let data_files = storage.list_data_files_with_stats(project, table).await?;
    for f in &data_files {
        let mut stream = storage.read_file(project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            // Build column index map for this file batch (may differ from
            // the insert batch's schema).
            let file_col_idx: Vec<usize> = spec
                .elements
                .iter()
                .map(|e| {
                    rb.schema().index_of(&e.column).map_err(|_| {
                        BasinError::internal(format!(
                            "exclusion constraint column {:?} not in data file",
                            e.column
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            for exist_row in 0..rb.num_rows() {
                // Build existing-row tuple.
                let mut exist_vals = Vec::with_capacity(spec.elements.len());
                let mut any_null = false;
                for &ci in &file_col_idx {
                    let arr = rb.column(ci);
                    if arr.is_null(exist_row) {
                        any_null = true;
                        break;
                    }
                    exist_vals.push(scalar_to_string_for_excl(arr.as_ref(), exist_row)?);
                }
                if any_null {
                    continue;
                }

                // Check each new row against this existing row.
                for (_, new_vals) in &new_tuples {
                    if exclusion_tuples_conflict(&spec.elements, new_vals, &exist_vals)? {
                        return Err(BasinError::CheckViolation(format!(
                            "conflicting key value violates exclusion constraint \"{name}\" \
                             on table \"{table_name_str}\" (SQLSTATE 23P01): \
                             key conflicts with existing row.",
                            name = spec.name
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Returns `true` if `a_vals` and `b_vals` jointly satisfy ALL element
/// conditions, meaning they constitute an exclusion-constraint violation.
///
/// For `=` : the two values are string-equal.
/// For `&&`: the two range values overlap (reuses the same logic as the
///           `range_overlaps` UDF — non-overlapping / adjacent → false).
fn exclusion_tuples_conflict(
    elements: &[crate::ddl::ExcludeElement],
    a_vals: &[String],
    b_vals: &[String],
) -> Result<bool> {
    for (i, elem) in elements.iter().enumerate() {
        let a = &a_vals[i];
        let b = &b_vals[i];
        let matches = match elem.op.as_str() {
            "=" => a == b,
            "&&" => ranges_overlap(a, b),
            other => {
                return Err(BasinError::FeatureNotSupported(format!(
                    "EXCLUDE USING gist: operator {other:?} is not supported in v0.1 \
                     (only `=` and `&&` are); open a feature request"
                )));
            }
        };
        if !matches {
            // One element didn't match → no conflict.
            return Ok(false);
        }
    }
    // All elements matched → conflict.
    Ok(true)
}

/// Check whether two range strings overlap using the same bound semantics as
/// the `range_overlaps` UDF. Range strings may be in either the Basin JSON
/// form (`{"l":…,"u":…,"li":…,"ui":…}`) or the PG text form (`[lo,hi)`).
///
/// Returns `false` when either value cannot be parsed (defensive: treat
/// unparseable ranges as non-conflicting so a bad value doesn't hard-stop
/// all inserts — the DataFusion path will catch type errors on SELECT).
fn ranges_overlap(a: &str, b: &str) -> bool {
    // Re-use the existing basin_common range parser.
    use basin_common::types::range::RangeValue;

    let parse = |s: &str| -> Option<RangeValue> {
        let t = s.trim();
        if t.starts_with('{') {
            RangeValue::from_json_str(t)
        } else {
            RangeValue::from_pg_text(t)
        }
    };

    let Some(ra) = parse(a) else { return false };
    let Some(rb) = parse(b) else { return false };

    // Convert to comparable f64 bounds via the JSON representation.
    let ra_json = ra.to_json_string();
    let rb_json = rb.to_json_string();

    let va: serde_json::Value = match serde_json::from_str(&ra_json) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let vb: serde_json::Value = match serde_json::from_str(&rb_json) {
        Ok(v) => v,
        Err(_) => return false,
    };

    // Helper: extract a bound as f64 (null / missing = infinity).
    let bound_f64 = |v: &serde_json::Value, key: &str| -> Option<f64> {
        let val = v.get(key)?;
        if val.is_null() {
            return None; // unbounded / infinity
        }
        if let Some(n) = val.as_f64() {
            return Some(n);
        }
        // String-encoded timestamp or similar — convert to micros via parse.
        if let Some(s) = val.as_str() {
            // Try naive timestamp parse (PG epoch in microseconds).
            if let Ok(ts) = s.parse::<i64>() {
                return Some(ts as f64);
            }
            // Try chrono naive datetime.
            if let Ok(dt) =
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
            {
                return Some(dt.and_utc().timestamp_micros() as f64);
            }
        }
        None
    };
    let bound_inc = |v: &serde_json::Value, key: &str, default: bool| -> bool {
        v.get(key).and_then(|x| x.as_bool()).unwrap_or(default)
    };

    let a_lo = bound_f64(&va, "l");
    let a_hi = bound_f64(&va, "u");
    let b_lo = bound_f64(&vb, "l");
    let b_hi = bound_f64(&vb, "u");
    let a_li = bound_inc(&va, "li", true);
    let a_ui = bound_inc(&va, "ui", false);
    let b_li = bound_inc(&vb, "li", true);
    let b_ui = bound_inc(&vb, "ui", false);

    // a ends before b starts?
    let a_before_b = match (a_hi, b_lo) {
        (Some(ah), Some(bl)) => {
            if a_ui && b_li { ah < bl } else { ah <= bl }
        }
        _ => false,
    };
    // b ends before a starts?
    let b_before_a = match (b_hi, a_lo) {
        (Some(bh), Some(al)) => {
            if b_ui && a_li { bh < al } else { bh <= al }
        }
        _ => false,
    };
    !(a_before_b || b_before_a)
}

/// Convert an Arrow scalar at `(arr, row)` to a string for exclusion
/// comparisons. This is similar to `scalar_to_canonical_string` but also
/// handles `Utf8` (range values are stored as JSON strings) and returns
/// the raw string value directly for `&&` operator comparisons.
fn scalar_to_string_for_excl(arr: &dyn Array, row: usize) -> Result<String> {
    // Utf8 columns (includes range types stored as JSON strings).
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(row).to_string());
    }
    // Fall back to the shared canonical-string helper for numeric / UUID types.
    scalar_to_canonical_string(arr, row)
}

/// Render `(v1, v2, ...)` in the loose PG style for the failing-row
/// hint in a CHECK violation message. We don't mirror PG byte-for-byte
/// — drivers key off the SQLSTATE, not the message text.
fn render_row_as_pg_tuple(_schema: &Schema, batch: &RecordBatch, row: usize) -> Result<String> {
    let mut parts = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        let arr = batch.column(i);
        if arr.is_null(row) {
            parts.push("null".to_string());
            continue;
        }
        parts.push(scalar_to_canonical_string(arr.as_ref(), row)?);
    }
    Ok(parts.join(", "))
}

// --------------------------------------------------------------------
// FOREIGN KEY enforcement (insert / update side)
// --------------------------------------------------------------------

/// For each FK on `meta`, verify each row of `batch` references an
/// existing row in `fk.ref_table` (PK tuple match). NULL in any FK
/// column exempts the row (PG MATCH SIMPLE).
pub(crate) async fn enforce_fk_on_insert(
    catalog: &Arc<dyn Catalog>,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    table_name_str: &str,
    foreign_keys: &[ForeignKeyDef],
    batch: &RecordBatch,
) -> Result<()> {
    if foreign_keys.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    for fk in foreign_keys {
        // DEFERRABLE INITIALLY DEFERRED: Postgres validates these at COMMIT,
        // not at the statement, so a transaction may legally insert children
        // before their parents (Django/Rails migrations do exactly this).
        // Basin has no commit-time FK pass yet, so we accept a deferred FK
        // without per-row enforcement rather than reject the legal ordering —
        // a documented v0.1 limitation scoped to FKs the user explicitly
        // marked INITIALLY DEFERRED. Immediate FKs are still enforced here.
        if fk.initially_deferred {
            continue;
        }
        // Local column indexes.
        let local_idx: Vec<usize> = fk
            .columns
            .iter()
            .map(|c| {
                batch.schema().index_of(c).map_err(|_| {
                    BasinError::internal(format!("FK local column {c:?} missing from batch"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Build the tuple set we need to look up.
        let mut needed: std::collections::HashSet<Vec<String>> = Default::default();
        let mut row_keys: Vec<Option<Vec<String>>> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let k = pk_tuple_for_row(batch, &local_idx, row)?;
            if let Some(k) = k.as_ref() {
                needed.insert(k.clone());
            }
            row_keys.push(k);
        }
        if needed.is_empty() {
            continue;
        }
        // Load referenced table (same project).
        let ref_table_name = TableName::new(fk.ref_table.clone())?;
        let ref_meta = catalog
            .load_table(project, &ref_table_name)
            .await
            .map_err(|e| match e {
                BasinError::NotFound(_) => BasinError::ForeignKeyViolation(format!(
                    "insert or update on table \"{table_name_str}\" violates foreign key \
                     constraint \"{}\": referenced table {:?} no longer exists",
                    fk.name, fk.ref_table
                )),
                other => other,
            })?;
        let ref_pk_set = collect_pk_tuples(&ref_meta, storage, project, &fk.ref_columns).await?;
        for (row, key) in row_keys.iter().enumerate() {
            let Some(k) = key else { continue };
            if !ref_pk_set.contains(k) {
                return Err(BasinError::ForeignKeyViolation(format!(
                    "insert or update on table \"{table_name_str}\" violates foreign key \
                     constraint \"{}\": Key ({})=({}) is not present in table \"{}\".",
                    fk.name,
                    fk.columns.join(", "),
                    k.join(", "),
                    fk.ref_table
                )));
            }
            let _ = row;
        }
    }
    Ok(())
}

/// Read every existing row of `meta`'s table and return the set of PK
/// tuples (using the columns named in `pk_cols`, which v0.1 always
/// equals the table's PK).
async fn collect_pk_tuples(
    meta: &TableMetadata,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    pk_cols: &[String],
) -> Result<std::collections::HashSet<Vec<String>>> {
    let mut out: std::collections::HashSet<Vec<String>> = Default::default();
    let data_files = storage
        .list_data_files_with_stats(project, &meta.table)
        .await?;
    // Tier 1: projection pushdown — only the referenced-PK columns matter.
    let read_opts = basin_storage::ReadOptions {
        projection: Some(pk_cols.to_vec()),
        ..Default::default()
    };
    for f in &data_files {
        let mut stream = storage
            .read_file_with_options(project, &f.path, read_opts.clone())
            .await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            let idx: Vec<usize> = pk_cols
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!(
                            "PK column {c:?} missing from referenced data file"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            for row in 0..rb.num_rows() {
                if let Some(k) = pk_tuple_for_row(&rb, &idx, row)? {
                    out.insert(k);
                }
            }
        }
    }
    Ok(out)
}

// --------------------------------------------------------------------
// FOREIGN KEY enforcement (parent delete / update side)
// --------------------------------------------------------------------

/// Walk every table in `project` and find FKs that point at
/// `parent_table`. Returns `(child_table, fk)` pairs.
pub(crate) async fn fks_referencing(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    parent_table: &str,
) -> Result<Vec<(TableName, ForeignKeyDef)>> {
    let mut out = Vec::new();
    let names = catalog.list_tables(project).await?;
    for n in &names {
        let meta = catalog.load_table(project, n).await?;
        for fk in &meta.foreign_keys {
            if fk.ref_table.eq_ignore_ascii_case(parent_table) {
                out.push((n.clone(), fk.clone()));
            }
        }
    }
    Ok(out)
}

/// For each parent-row PK tuple in `deleted_keys`, decide whether the
/// referencing children must be rejected (NO ACTION) or cascaded
/// (CASCADE). Returns the child rows to delete keyed by `(child_table,
/// row_predicate)` so the caller can dispatch a CASCADE DELETE through
/// the standard DELETE path.
///
/// For NO ACTION (the default), the function returns
/// `BasinError::ForeignKeyViolation` if any referencing rows exist.
pub(crate) async fn check_parent_delete(
    catalog: &Arc<dyn Catalog>,
    storage: &basin_storage::Storage,
    project: &ProjectId,
    parent_table_str: &str,
    deleted_pk_tuples: &std::collections::HashSet<Vec<String>>,
    parent_pk_columns: &[String],
) -> Result<Vec<CascadeDelete>> {
    let mut out: Vec<CascadeDelete> = Vec::new();
    if deleted_pk_tuples.is_empty() {
        return Ok(out);
    }
    let referencing = fks_referencing(catalog, project, parent_table_str).await?;
    for (child_table, fk) in &referencing {
        // Map child FK columns → parent PK columns by ref_columns ↔ parent PK position.
        // Build the rows-of-child-with-FK-tuple-in-deleted_pk_tuples.
        let child_meta = catalog.load_table(project, child_table).await?;
        let data_files = storage
            .list_data_files_with_stats(project, child_table)
            .await?;
        // Re-order child FK columns to match the parent's PK order
        // so the tuple lookup against `deleted_pk_tuples` is byte-
        // identical.
        let mut local_in_parent_pk_order: Vec<String> = Vec::with_capacity(parent_pk_columns.len());
        for p in parent_pk_columns {
            // Find index of p in fk.ref_columns; the corresponding
            // local column is the one to read.
            let pos = fk
                .ref_columns
                .iter()
                .position(|r| r.eq_ignore_ascii_case(p))
                .ok_or_else(|| {
                    BasinError::internal(format!(
                        "FK {:?} ref_columns does not include parent PK column {p:?}",
                        fk.name
                    ))
                })?;
            local_in_parent_pk_order.push(fk.columns[pos].clone());
        }

        let mut matching_rows: Vec<HashMap<String, String>> = Vec::new();
        for f in &data_files {
            let mut stream = storage.read_file(project, &f.path).await?;
            while let Some(rb) = stream.next().await {
                let rb = rb?;
                let idx: Vec<usize> = local_in_parent_pk_order
                    .iter()
                    .map(|c| {
                        rb.schema().index_of(c).map_err(|_| {
                            BasinError::internal(format!("FK column {c:?} missing from data file"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..rb.num_rows() {
                    let Some(k) = pk_tuple_for_row(&rb, &idx, row)? else {
                        continue;
                    };
                    if deleted_pk_tuples.contains(&k) {
                        // Capture the matching child row's FK columns
                        // as a HashMap so the caller can build a
                        // WHERE clause for CASCADE DELETE.
                        let mut m = HashMap::new();
                        for (col_name, col_idx) in local_in_parent_pk_order.iter().zip(idx.iter()) {
                            let arr = rb.column(*col_idx);
                            if arr.is_null(row) {
                                m.insert(col_name.clone(), "NULL".into());
                            } else {
                                m.insert(
                                    col_name.clone(),
                                    scalar_to_canonical_string(arr.as_ref(), row)?,
                                );
                            }
                        }
                        matching_rows.push(m);
                    }
                }
            }
        }
        if matching_rows.is_empty() {
            continue;
        }
        match fk.on_delete {
            RefAction::NoAction => {
                let example_row = &matching_rows[0];
                let example_tuple: Vec<String> = local_in_parent_pk_order
                    .iter()
                    .map(|c| example_row.get(c).cloned().unwrap_or_else(|| "?".into()))
                    .collect();
                return Err(BasinError::ForeignKeyViolation(format!(
                    "update or delete on table \"{parent_table_str}\" violates foreign key \
                     constraint \"{}\" on table \"{}\": Key ({})=({}) is still referenced from \
                     table \"{}\".",
                    fk.name,
                    child_table,
                    parent_pk_columns.join(", "),
                    example_tuple.join(", "),
                    child_table
                )));
            }
            RefAction::Cascade => {
                out.push(CascadeDelete {
                    child_table: child_table.clone(),
                    fk_columns: local_in_parent_pk_order,
                    rows: matching_rows,
                });
            }
        }
        let _ = child_meta;
    }
    Ok(out)
}

/// One pending CASCADE DELETE the parent operation has authorised.
/// The caller dispatches a DELETE on `child_table` whose predicate
/// matches every captured row.
#[derive(Debug)]
pub(crate) struct CascadeDelete {
    pub child_table: TableName,
    pub fk_columns: Vec<String>,
    pub rows: Vec<HashMap<String, String>>,
}

/// Helper exported for the DELETE path to extract PK tuples from a
/// captured "before" RecordBatch.
pub(crate) fn pk_tuples_from_batches(
    batches: &[RecordBatch],
    pk_columns: &[String],
) -> Result<std::collections::HashSet<Vec<String>>> {
    let mut out: std::collections::HashSet<Vec<String>> = Default::default();
    for b in batches {
        let idx: Vec<usize> = pk_columns
            .iter()
            .map(|c| {
                b.schema().index_of(c).map_err(|_| {
                    BasinError::internal(format!("PK column {c:?} missing from before-batch"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..b.num_rows() {
            if let Some(k) = pk_tuple_for_row(b, &idx, row)? {
                out.insert(k);
            }
        }
    }
    Ok(out)
}

/// Render a WHERE clause matching exactly the PK tuples in `tuples`
/// (one big OR of column-tuple equality literals). The caller routes
/// the resulting SQL through the standard DELETE path so RLS, AUDIT,
/// and the sink registry all fire normally.
pub(crate) fn build_in_predicate_sql(
    rows: &[HashMap<String, String>],
    fk_columns: &[String],
    schema: &Schema,
) -> Result<String> {
    let mut clauses = Vec::with_capacity(rows.len());
    for r in rows {
        let mut parts = Vec::with_capacity(fk_columns.len());
        for c in fk_columns {
            let raw = r
                .get(c)
                .ok_or_else(|| BasinError::internal(format!("cascade row missing column {c}")))?;
            let field = schema
                .field_with_name(c)
                .map_err(|_| BasinError::internal(format!("cascade column {c} missing")))?;
            let lit = match field.data_type() {
                DataType::Utf8 => format!("'{}'", raw.replace('\'', "''")),
                _ => raw.clone(),
            };
            parts.push(format!("{c} = {lit}"));
        }
        clauses.push(format!("({})", parts.join(" AND ")));
    }
    Ok(clauses.join(" OR "))
}

// --------------------------------------------------------------------
// Unit tests
// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::strip_check_wrapper;

    // --- strip_check_wrapper ------------------------------------------------

    /// Bare expression without a CHECK wrapper must be returned unchanged.
    #[test]
    fn strip_bare_expression_is_noop() {
        assert_eq!(strip_check_wrapper("score >= 0"), "score >= 0");
        assert_eq!(strip_check_wrapper("price > 0"), "price > 0");
        assert_eq!(strip_check_wrapper("a AND b"), "a AND b");
    }

    /// Typical column-level predicate stored with the CHECK (...) wrapper.
    #[test]
    fn strip_column_level_check_wrapper() {
        assert_eq!(strip_check_wrapper("CHECK (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("CHECK (price > 0)"), "price > 0");
    }

    /// Case-insensitive matching.
    #[test]
    fn strip_check_wrapper_case_insensitive() {
        assert_eq!(strip_check_wrapper("check (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("Check (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("CHECK (score >= 0)"), "score >= 0");
    }

    /// Whitespace between CHECK and the opening paren.
    #[test]
    fn strip_check_wrapper_with_extra_whitespace() {
        assert_eq!(strip_check_wrapper("CHECK  (score >= 0)"), "score >= 0");
        assert_eq!(strip_check_wrapper("  CHECK (score >= 0)  "), "score >= 0");
    }

    /// Nested parentheses inside the expression must not be stripped incorrectly.
    #[test]
    fn strip_check_wrapper_nested_parens() {
        // The outer CHECK (...) wraps an expression that contains a function call.
        let stored = "CHECK (coalesce(score, 0) >= 0)";
        assert_eq!(strip_check_wrapper(stored), "coalesce(score, 0) >= 0");
    }

    /// Named constraint form: `CONSTRAINT name CHECK (expr)` — the stored predicate
    /// is just the bare inner expr (table-level path uses `expr.to_string()`),
    /// so the wrapper is not present and strip is a no-op.
    #[test]
    fn strip_bare_named_constraint_predicate_is_noop() {
        assert_eq!(
            strip_check_wrapper("end_day >= start_day"),
            "end_day >= start_day"
        );
    }

    /// If `CHECK` appears but is not followed by `(`, treat as bare expression.
    #[test]
    fn strip_check_without_paren_is_noop() {
        // Pathological but must not panic.
        assert_eq!(strip_check_wrapper("CHECK_COLUMN > 0"), "CHECK_COLUMN > 0");
    }

    /// Idempotent: stripping twice gives the same result as stripping once.
    #[test]
    fn strip_check_wrapper_idempotent() {
        let stored = "CHECK (score >= 0)";
        let once = strip_check_wrapper(stored);
        let twice = strip_check_wrapper(once);
        assert_eq!(once, twice);
        assert_eq!(once, "score >= 0");
    }
}
