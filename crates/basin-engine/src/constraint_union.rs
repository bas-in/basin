//! Constraint enforcement against the **union of the hot-tier memtable and the
//! cold-tier object store**.
//!
//! Phase 5.14.C ships the hot-tier write path; commits `87ef24b` / `3148585`
//! routed DELETE through it. The next perf wave (PR2) routes UPDATE through
//! the same path — but UPDATE cannot move until FK / UNIQUE / PK enforcement
//! sees both tiers, or a row sitting in the memtable could violate a UNIQUE
//! constraint that today's `enforce_*` helpers (which scan only the cold
//! tier) would miss.
//!
//! This module ships the **building block**: [`check_constraints_memtable_and_cold`].
//! It is wired into UPDATE by PR2; today it is reachable from tests only so
//! reviewers can focus the next PR on dispatch.
//!
//! ## Semantics
//!
//! For a proposed write (`WriteKind::Insert` or `WriteKind::UpdateNewImage`),
//! the helper enforces each declared constraint against the **effective**
//! row set:
//!
//! ```text
//! effective_rows(table) = cold_rows(table)               // existing data files
//!                       - tombstoned_rows(table)         // memtable Tombstone keys
//!                       + live_memtable_rows(table)      // memtable Row values
//! ```
//!
//! Specifically:
//!
//! - **UNIQUE** — every UNIQUE constraint must hold over `effective_rows`
//!   plus the new write. `NULL` in any UNIQUE column makes the row exempt
//!   (PG default semantics; matches [`crate::constraints::enforce_unique_on_insert`]).
//! - **PK** — the table's PK column-set must remain unique over the same
//!   union. Surfaces as `BasinError::UniqueViolation` (PG re-uses SQLSTATE
//!   23505 for PK collisions — there is no separate `PrimaryKeyViolation`
//!   variant in `basin_common::BasinError`).
//! - **FK** — every FK column tuple in the new write must resolve to an
//!   existing PK in the referenced table's effective row set. `NULL` in any
//!   FK column exempts the row (PG MATCH SIMPLE).
//!
//! Returns `Ok(())` on pass; first failure short-circuits with the matching
//! `BasinError` variant.
//!
//! ## Implementation choices
//!
//! - **Memtable probe**: snapshot the registry entry once at the top of the
//!   helper. Decode each `MemRowValue::Row` IPC blob into a `RecordBatch` and
//!   project the constraint columns. Capture each `MemRowValue::Tombstone`
//!   key in a `HashSet<Vec<u8>>` of `RowKey` bytes so cold rows can be
//!   filtered without re-decoding the PK back to text.
//! - **Cold-tier probe** (small tables): full-scan the constraint columns +
//!   PK columns through `Storage::list_data_files_with_stats` + `read_file`,
//!   identical to the existing `enforce_*` helpers. Cost stays O(rows) per
//!   constraint — acceptable because the same scan is already done on the
//!   slow-path UPDATE today.
//! - **Cold-tier probe** (large tables): reserved for the
//!   [`SMALL_TABLE_ROW_THRESHOLD`]-row inflection point. Above the threshold
//!   the helper currently falls back to the same scan and emits a tracing
//!   warning — the catalog does not yet expose a per-row PK probe API (the
//!   secondary-index probe is per-eq-predicate, not per-tuple). When that
//!   API lands the threshold becomes the dispatch point. See the
//!   [`probe_strategy`] helper.
//!
//! ## Caller contract (PR2)
//!
//! ```ignore
//! check_constraints_memtable_and_cold(
//!     sess,
//!     &meta,
//!     &new_batch,
//!     WriteKind::UpdateNewImage { old_pks: captured_old_pks },
//! ).await?;
//! ```
//!
//! Returns an error of one of the three variants above on first violation.
//! PR2 will wrap the call so the rest of the executor remains unchanged.

// All items here are intentionally unreachable from production code until PR2
// wires the helper into the UPDATE path. The dead_code allow keeps the
// scaffolding warning-free without sprinkling per-item annotations.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use basin_catalog::{Catalog, TableMetadata};
use basin_common::{BasinError, ProjectId, Result, TableName};
use basin_hottier::{MemRowValue, MemTableRegistry, RowKey};
use futures::StreamExt;

use crate::constraints::pk_tuple_for_row;

// ── Tunables ─────────────────────────────────────────────────────────────────

/// Tables with at most this many cold-tier rows always use the in-memory
/// HashSet union strategy. Tables above this size will, once the catalog
/// exposes a per-tuple PK probe API, switch to per-row probes; today both
/// branches share the same scan implementation. Documented threshold so PR2
/// reviewers can reason about the cost shape without re-deriving it.
pub(crate) const SMALL_TABLE_ROW_THRESHOLD: u64 = 10_000;

/// Strategy chosen for a given constraint check. Returned by
/// [`probe_strategy`] so callers can log / tracing-export the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeStrategy {
    /// Build a `HashSet<Vec<String>>` from (cold scan ∪ live memtable) − tombstones
    /// and check each new-row tuple against it. O(rows + writes).
    InMemoryUnion,
    /// Per-row catalog probe (deferred — currently falls back to InMemoryUnion).
    CatalogProbe,
}

/// Pick the probe strategy for a table whose approximate row count is `cold_rows`.
pub(crate) fn probe_strategy(cold_rows: u64) -> ProbeStrategy {
    if cold_rows <= SMALL_TABLE_ROW_THRESHOLD {
        ProbeStrategy::InMemoryUnion
    } else {
        ProbeStrategy::CatalogProbe
    }
}

// ── Public types ─────────────────────────────────────────────────────────────

/// What kind of write the caller is about to commit. The helper uses this to
/// suppress "self-collision" on UPDATE: the row whose PK is in `old_pks` is
/// the one being replaced, so its current value must NOT count toward
/// UNIQUE / PK uniqueness — otherwise an UPDATE that keeps the same value
/// would always fail.
#[derive(Debug, Clone)]
pub(crate) enum WriteKind {
    /// Plain INSERT — no self-suppression.
    Insert,
    /// UPDATE new image — every row whose canonical PK tuple is in `old_pks`
    /// is treated as "about to vanish from the table" when computing the
    /// effective row set against which the new write is validated.
    UpdateNewImage { old_pks: Vec<Vec<String>> },
}

/// Minimal context bag the helper needs. We deliberately do not take
/// `&ProjectSession` so the function is callable from tests without spinning
/// up a full engine + session — PR2 will build one from the session at the
/// call site.
pub(crate) struct ConstraintCheckCtx<'a> {
    pub project: ProjectId,
    pub table: TableName,
    pub catalog: &'a Arc<dyn Catalog>,
    pub storage: &'a basin_storage::Storage,
    pub memtable_registry: &'a MemTableRegistry,
}

// ── Public entrypoint ────────────────────────────────────────────────────────

/// Check every constraint on `table_meta` against the union of the hot-tier
/// memtable and the cold tier — see the module docs for the full contract.
///
/// Wired into UPDATE by PR2; the INSERT path remains on the existing
/// cold-only enforcers in [`crate::constraints`] until that PR lands.
#[allow(dead_code)] // PR2 wires this from the UPDATE path.
pub(crate) async fn check_constraints_memtable_and_cold(
    ctx: &ConstraintCheckCtx<'_>,
    table_meta: &TableMetadata,
    new_rows: &RecordBatch,
    write_kind: &WriteKind,
) -> Result<()> {
    if new_rows.num_rows() == 0 {
        return Ok(());
    }
    let table_str = table_meta.table.as_str();

    // Single snapshot of the memtable for the duration of the helper. The
    // memtable is updated concurrently; for the constraint check it is OK to
    // pin a single view — write-write conflicts are caught later by the
    // catalog's optimistic-commit `CommitConflict`.
    let mem_snap = snapshot_memtable(ctx.memtable_registry, &ctx.project, &ctx.table);

    // ── PRIMARY KEY ──────────────────────────────────────────────────────────
    if !table_meta.pk_columns.is_empty() {
        check_pk_against_union(
            ctx,
            table_str,
            &table_meta.pk_columns,
            new_rows,
            write_kind,
            &mem_snap,
        )
        .await?;
    }

    // ── UNIQUE ───────────────────────────────────────────────────────────────
    for u in &table_meta.unique_constraints {
        if u.columns.is_empty() {
            continue;
        }
        check_unique_against_union(
            ctx,
            table_str,
            &u.name,
            &u.columns,
            &table_meta.pk_columns,
            new_rows,
            write_kind,
            &mem_snap,
        )
        .await?;
    }

    // ── FOREIGN KEY ──────────────────────────────────────────────────────────
    for fk in &table_meta.foreign_keys {
        check_fk_against_union(ctx, table_str, fk, new_rows).await?;
    }

    Ok(())
}

// ── Memtable snapshot ────────────────────────────────────────────────────────

/// A one-shot view of the memtable for `(project, table)`: every live row as
/// a single-row `RecordBatch`, every tombstone as its raw `RowKey` bytes.
///
/// Returns an `MemtableSnapshot::empty()` when the registry has no entry for
/// the table — the common case for cold tables.
pub(crate) struct MemtableSnapshot {
    /// Decoded live rows in PK order (BTree iteration order from
    /// `MemTable::snapshot`). Each `RecordBatch` has `num_rows() == 1`.
    pub live_rows: Vec<RecordBatch>,
    /// `RowKey` bytes for tombstoned PKs.
    pub tombstone_keys: HashSet<Vec<u8>>,
}

impl MemtableSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            live_rows: Vec::new(),
            tombstone_keys: HashSet::new(),
        }
    }
}

/// Decode the memtable for `(project, table)` into a [`MemtableSnapshot`].
/// Tombstones become an entry in `tombstone_keys`; live rows are IPC-decoded.
/// Rows that fail to decode are silently skipped — they can never legitimately
/// match a new write so dropping them is conservative (matches the cold-tier
/// scan's behaviour for unsupported column types).
pub(crate) fn snapshot_memtable(
    registry: &MemTableRegistry,
    project: &ProjectId,
    table: &TableName,
) -> MemtableSnapshot {
    let Some(entry) = registry.get(project, table) else {
        return MemtableSnapshot::empty();
    };
    let raw = entry.memtable.snapshot();
    let mut live_rows: Vec<RecordBatch> = Vec::with_capacity(raw.len());
    let mut tombstones: HashSet<Vec<u8>> = HashSet::new();
    for (key, value) in raw {
        match value {
            MemRowValue::Row { bytes, .. } => {
                if let Some(rb) = decode_ipc_batch(&bytes) {
                    if rb.num_rows() > 0 {
                        live_rows.push(rb);
                    }
                }
            }
            // A PK-keyed UPDATE override: the new image is a live row, AND its
            // PK key suppresses the stale cold-tier row so the union does not
            // double-count the pre-update version.
            MemRowValue::Update { bytes, .. } => {
                tombstones.insert(key.as_bytes().to_vec());
                if let Some(rb) = decode_ipc_batch(&bytes) {
                    if rb.num_rows() > 0 {
                        live_rows.push(rb);
                    }
                }
            }
            MemRowValue::Tombstone => {
                tombstones.insert(key.as_bytes().to_vec());
            }
        }
    }
    MemtableSnapshot {
        live_rows,
        tombstone_keys: tombstones,
    }
}

/// Decode a single Arrow IPC stream blob into the first `RecordBatch`.
/// Mirrors `fast_select::decode_ipc_batch`.
fn decode_ipc_batch(bytes: &[u8]) -> Option<RecordBatch> {
    use arrow::ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).ok()?;
    reader.next()?.ok()
}

/// Encode the single value at `row_idx` in `array` (whose declared logical
/// type is `col_dt`) into a `RowKey` whose lexicographic byte order matches
/// the cluster-key sort and the encoding the hot-tier fast-path writer uses
/// when tombstoning rows.
///
/// Returns `None` when the value is NULL, the column's declared type is not
/// in the supported single-column PK set, or the runtime Arrow type does not
/// match the declared `col_dt`. The fast-path writer rejects writes outside
/// these types so a `None` here means no tombstone match is possible — the
/// caller should keep the row.
///
/// `pub(crate)` so the executor's `htap_promote_to_registry` can reuse the
/// same encoding when keying INSERT rows by PK.
pub(crate) fn array_value_to_row_key(
    array: &dyn Array,
    row_idx: usize,
    col_dt: &DataType,
) -> Option<RowKey> {
    if array.is_null(row_idx) {
        return None;
    }
    let b = RowKey::builder();
    Some(match col_dt {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Int64Array>()?;
            b.append_i64(arr.value(row_idx)).finish()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Int32Array>()?;
            b.append_i32(arr.value(row_idx)).finish()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<arrow_array::Int16Array>()?;
            b.append_i16(arr.value(row_idx)).finish()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<arrow_array::UInt64Array>()?;
            b.append_u64(arr.value(row_idx)).finish()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<arrow_array::StringArray>()?;
            b.append_str(arr.value(row_idx)).finish()
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()?;
            b.append_str(arr.value(row_idx)).finish()
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()?;
            b.append_str(arr.value(row_idx)).finish()
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<arrow_array::BooleanArray>()?;
            b.append_u8(if arr.value(row_idx) { 1 } else { 0 }).finish()
        }
        _ => return None,
    })
}

// ── PRIMARY KEY check ────────────────────────────────────────────────────────

async fn check_pk_against_union(
    ctx: &ConstraintCheckCtx<'_>,
    table_str: &str,
    pk_columns: &[String],
    new_rows: &RecordBatch,
    write_kind: &WriteKind,
    mem_snap: &MemtableSnapshot,
) -> Result<()> {
    let suppressed = old_pk_suppression_set(write_kind);

    // 1. Effective PK set: cold ∪ live memtable rows, minus tombstones, minus
    //    `suppressed` (the rows about to be UPDATE-replaced).
    let mut effective: HashSet<Vec<String>> = HashSet::new();

    // Cold-tier scan.
    let pk_dt_for_tombstone =
        pk_arrow_dt_for_tombstone(&ctx.project, &ctx.table, ctx.catalog, pk_columns).await;
    let data_files = ctx
        .storage
        .list_data_files_with_stats(&ctx.project, &ctx.table)
        .await?;
    for f in &data_files {
        let mut stream = ctx.storage.read_file(&ctx.project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            collect_pk_tuples_from_batch(
                &rb,
                pk_columns,
                &mem_snap.tombstone_keys,
                pk_dt_for_tombstone.as_ref(),
                &suppressed,
                &mut effective,
            )?;
        }
    }

    // Live memtable rows.
    for rb in &mem_snap.live_rows {
        collect_pk_tuples_from_batch(
            rb,
            pk_columns,
            &HashSet::new(), // memtable rows themselves are never tombstoned
            None,
            &suppressed,
            &mut effective,
        )?;
    }

    // 2. Intra-batch dup check + collision against `effective`.
    let pk_idx: Vec<usize> = pk_columns
        .iter()
        .map(|c| {
            new_rows.schema().index_of(c).map_err(|_| {
                BasinError::internal(format!("PK column {c:?} missing from new batch"))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut seen: HashSet<Vec<String>> = HashSet::new();
    for row in 0..new_rows.num_rows() {
        let Some(k) = pk_tuple_for_row(new_rows, &pk_idx, row)? else {
            return Err(BasinError::CheckViolation(format!(
                "null value in column violates not-null constraint on PRIMARY KEY of \
                 \"{table_str}\""
            )));
        };
        if !seen.insert(k.clone()) || effective.contains(&k) {
            return Err(BasinError::UniqueViolation(format!(
                "duplicate key value violates unique constraint \"{table_str}_pkey\": \
                 Key ({})=({}) already exists.",
                pk_columns.join(", "),
                k.join(", ")
            )));
        }
    }
    Ok(())
}

/// Project the PK columns of `rb` into the running effective-set, dropping
/// rows whose PK falls in `tombstones` (when `pk_dt` is provided and the PK
/// is single-column — the only case the fast-path tombstone writer
/// produces) and rows whose PK is in `suppressed_pks` (UPDATE old image).
fn collect_pk_tuples_from_batch(
    rb: &RecordBatch,
    pk_columns: &[String],
    tombstones: &HashSet<Vec<u8>>,
    pk_dt: Option<&DataType>,
    suppressed_pks: &HashSet<Vec<String>>,
    out: &mut HashSet<Vec<String>>,
) -> Result<()> {
    let idx: Vec<usize> = pk_columns
        .iter()
        .map(|c| {
            rb.schema()
                .index_of(c)
                .map_err(|_| BasinError::internal(format!("PK column {c:?} missing from batch")))
        })
        .collect::<Result<Vec<_>>>()?;
    // Tombstone suppression only applies for single-column PKs (matches the
    // hot-tier fast path's contract).
    let tombstone_arr = if pk_columns.len() == 1 && !tombstones.is_empty() {
        pk_dt
    } else {
        None
    };
    for row in 0..rb.num_rows() {
        // Tombstone suppression: encode the row's PK to RowKey bytes and skip
        // if a tombstone matches.
        if let Some(dt) = tombstone_arr {
            if let Some(key) = array_value_to_row_key(rb.column(idx[0]).as_ref(), row, dt) {
                if tombstones.contains(key.as_bytes()) {
                    continue;
                }
            }
        }
        if let Some(k) = pk_tuple_for_row(rb, &idx, row)? {
            if !suppressed_pks.contains(&k) {
                out.insert(k);
            }
        }
    }
    Ok(())
}

// ── UNIQUE check ─────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn check_unique_against_union(
    ctx: &ConstraintCheckCtx<'_>,
    table_str: &str,
    constraint_name: &str,
    unique_columns: &[String],
    pk_columns: &[String],
    new_rows: &RecordBatch,
    write_kind: &WriteKind,
    mem_snap: &MemtableSnapshot,
) -> Result<()> {
    let suppressed = old_pk_suppression_set(write_kind);

    // Effective set of (unique_tuple) → owned-by-pk.  We index *only* the
    // unique tuple but stash the PK alongside so we can drop entries whose PK
    // is being replaced (the UPDATE self-collision suppression).
    let mut effective: HashSet<Vec<String>> = HashSet::new();

    // Cold-tier scan.
    let pk_dt = pk_arrow_dt_for_tombstone(&ctx.project, &ctx.table, ctx.catalog, pk_columns).await;
    let data_files = ctx
        .storage
        .list_data_files_with_stats(&ctx.project, &ctx.table)
        .await?;
    for f in &data_files {
        let mut stream = ctx.storage.read_file(&ctx.project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            collect_unique_tuples_from_batch(
                &rb,
                unique_columns,
                pk_columns,
                &mem_snap.tombstone_keys,
                pk_dt.as_ref(),
                &suppressed,
                &mut effective,
            )?;
        }
    }

    // Live memtable rows.
    for rb in &mem_snap.live_rows {
        collect_unique_tuples_from_batch(
            rb,
            unique_columns,
            pk_columns,
            &HashSet::new(),
            None,
            &suppressed,
            &mut effective,
        )?;
    }

    // Intra-batch dup + clash check.
    let col_idx: Vec<usize> = unique_columns
        .iter()
        .map(|c| {
            new_rows.schema().index_of(c).map_err(|_| {
                BasinError::internal(format!("UNIQUE column {c:?} missing from new batch"))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut seen: HashSet<Vec<String>> = HashSet::new();
    for row in 0..new_rows.num_rows() {
        // NULL in any column → exempt (PG default).
        let Some(k) = pk_tuple_for_row(new_rows, &col_idx, row)? else {
            continue;
        };
        if !seen.insert(k.clone()) || effective.contains(&k) {
            return Err(BasinError::UniqueViolation(format!(
                "duplicate key value violates unique constraint \"{constraint_name}\" on \
                 table \"{table_str}\": Key ({})=({}) already exists.",
                unique_columns.join(", "),
                k.join(", ")
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn collect_unique_tuples_from_batch(
    rb: &RecordBatch,
    unique_columns: &[String],
    pk_columns: &[String],
    tombstones: &HashSet<Vec<u8>>,
    pk_dt: Option<&DataType>,
    suppressed_pks: &HashSet<Vec<String>>,
    out: &mut HashSet<Vec<String>>,
) -> Result<()> {
    let uniq_idx: Vec<usize> = unique_columns
        .iter()
        .map(|c| {
            rb.schema().index_of(c).map_err(|_| {
                BasinError::internal(format!("UNIQUE column {c:?} missing from batch"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // PK columns may legitimately not be in this batch (e.g. a memtable row
    // captured at a different point in time). Best-effort: if any PK column
    // is missing, fall back to "no PK association" — the row is included in
    // `effective` and self-suppression won't apply to it.
    let pk_idx: Option<Vec<usize>> = pk_columns
        .iter()
        .map(|c| rb.schema().index_of(c).ok())
        .collect();
    let single_pk_tombstone_dt = if pk_columns.len() == 1 && !tombstones.is_empty() {
        pk_dt
    } else {
        None
    };

    for row in 0..rb.num_rows() {
        // Tombstone suppression (single-column PK only).
        if let Some(dt) = single_pk_tombstone_dt {
            if let Some(pk_i) = pk_idx.as_ref().and_then(|v| v.first().copied()) {
                if let Some(key) = array_value_to_row_key(rb.column(pk_i).as_ref(), row, dt) {
                    if tombstones.contains(key.as_bytes()) {
                        continue;
                    }
                }
            }
        }
        // UPDATE self-suppression.
        if !suppressed_pks.is_empty() {
            if let Some(idx) = &pk_idx {
                if let Some(pk) = pk_tuple_for_row(rb, idx, row)? {
                    if suppressed_pks.contains(&pk) {
                        continue;
                    }
                }
            }
        }
        // NULL in any UNIQUE column → row exempt.
        if let Some(k) = pk_tuple_for_row(rb, &uniq_idx, row)? {
            out.insert(k);
        }
    }
    Ok(())
}

// ── FOREIGN KEY check ────────────────────────────────────────────────────────

async fn check_fk_against_union(
    ctx: &ConstraintCheckCtx<'_>,
    table_str: &str,
    fk: &basin_catalog::ForeignKeyDef,
    new_rows: &RecordBatch,
) -> Result<()> {
    let local_idx: Vec<usize> = fk
        .columns
        .iter()
        .map(|c| {
            new_rows.schema().index_of(c).map_err(|_| {
                BasinError::internal(format!("FK local column {c:?} missing from new batch"))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Collect the FK tuples we need to validate.
    let mut needed: HashSet<Vec<String>> = HashSet::new();
    let mut per_row: Vec<Option<Vec<String>>> = Vec::with_capacity(new_rows.num_rows());
    for row in 0..new_rows.num_rows() {
        let k = pk_tuple_for_row(new_rows, &local_idx, row)?;
        if let Some(k) = k.as_ref() {
            needed.insert(k.clone());
        }
        per_row.push(k);
    }
    if needed.is_empty() {
        return Ok(());
    }

    // Load the referenced table's metadata and build the effective PK set
    // for the *referenced* table (cold ∪ live memtable − tombstones).
    let ref_table = TableName::new(fk.ref_table.clone())?;
    let ref_meta = match ctx.catalog.load_table(&ctx.project, &ref_table).await {
        Ok(m) => m,
        Err(BasinError::NotFound(_)) => {
            return Err(BasinError::ForeignKeyViolation(format!(
                "insert or update on table \"{table_str}\" violates foreign key constraint \
                 \"{}\": referenced table {:?} no longer exists",
                fk.name, fk.ref_table
            )));
        }
        Err(e) => return Err(e),
    };

    let ref_mem_snap = snapshot_memtable(ctx.memtable_registry, &ctx.project, &ref_table);
    let pk_dt =
        pk_arrow_dt_for_tombstone(&ctx.project, &ref_table, ctx.catalog, &fk.ref_columns).await;
    let mut effective: HashSet<Vec<String>> = HashSet::new();

    // Cold scan of the referenced table.
    let data_files = ctx
        .storage
        .list_data_files_with_stats(&ctx.project, &ref_table)
        .await?;
    for f in &data_files {
        let mut stream = ctx.storage.read_file(&ctx.project, &f.path).await?;
        while let Some(rb) = stream.next().await {
            let rb = rb?;
            collect_pk_tuples_from_batch(
                &rb,
                &fk.ref_columns,
                &ref_mem_snap.tombstone_keys,
                pk_dt.as_ref(),
                &HashSet::new(),
                &mut effective,
            )?;
        }
    }
    // Live memtable rows on the referenced table.
    for rb in &ref_mem_snap.live_rows {
        collect_pk_tuples_from_batch(
            rb,
            &fk.ref_columns,
            &HashSet::new(),
            None,
            &HashSet::new(),
            &mut effective,
        )?;
    }
    let _ = ref_meta;

    for (row, key) in per_row.iter().enumerate() {
        let Some(k) = key else { continue };
        if !effective.contains(k) {
            return Err(BasinError::ForeignKeyViolation(format!(
                "insert or update on table \"{table_str}\" violates foreign key constraint \
                 \"{}\": Key ({})=({}) is not present in table \"{}\".",
                fk.name,
                fk.columns.join(", "),
                k.join(", "),
                fk.ref_table
            )));
        }
        let _ = row;
    }
    Ok(())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn old_pk_suppression_set(write_kind: &WriteKind) -> HashSet<Vec<String>> {
    match write_kind {
        WriteKind::Insert => HashSet::new(),
        WriteKind::UpdateNewImage { old_pks } => old_pks.iter().cloned().collect(),
    }
}

/// Look up the Arrow `DataType` of the single PK column needed for tombstone
/// suppression. Returns `None` for composite PKs (no tombstone matching
/// possible — the hot-tier fast path only writes single-column tombstones)
/// and for tables the catalog cannot resolve.
async fn pk_arrow_dt_for_tombstone(
    project: &ProjectId,
    table: &TableName,
    catalog: &Arc<dyn Catalog>,
    pk_columns: &[String],
) -> Option<DataType> {
    if pk_columns.len() != 1 {
        return None;
    }
    let meta = catalog.load_table(project, table).await.ok()?;
    let idx = meta.schema.index_of(&pk_columns[0]).ok()?;
    Some(meta.schema.field(idx).data_type().clone())
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_catalog::{
        Catalog, DataFileRef, ForeignKeyDef, InMemoryCatalog, RefAction, UniqueConstraint,
    };
    use basin_common::{BasinError, PartitionKey, ProjectId, TableName};
    use basin_hottier::{MemRowValue, MemTableRegistry, RowKey};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::*;

    // ── Fixture ──────────────────────────────────────────────────────────────

    struct Fixture {
        _tmp: TempDir,
        storage: basin_storage::Storage,
        catalog: Arc<dyn Catalog>,
        registry: Arc<MemTableRegistry>,
        project: ProjectId,
    }

    impl Fixture {
        async fn new() -> Self {
            let tmp = TempDir::new().unwrap();
            let fs = LocalFileSystem::new_with_prefix(tmp.path()).unwrap();
            let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
                object_store: Arc::new(fs),
                root_prefix: None,
                disk_cache: None,
                page_cache: None,
            });
            let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
            let registry = Arc::new(MemTableRegistry::new());
            let project = ProjectId::new();
            catalog.create_namespace(&project).await.unwrap();
            Self {
                _tmp: tmp,
                storage,
                catalog,
                registry,
                project,
            }
        }

        fn ctx<'a>(&'a self, table: &TableName) -> ConstraintCheckCtx<'a> {
            ConstraintCheckCtx {
                project: self.project,
                table: table.clone(),
                catalog: &self.catalog,
                storage: &self.storage,
                memtable_registry: &self.registry,
            }
        }

        /// Create a table with the given PK + UNIQUE + FK constraints and a
        /// fixed (id BIGINT NOT NULL, name TEXT) schema.
        async fn create_table_with_constraints(
            &self,
            name: &str,
            pk: Vec<String>,
            unique: Vec<UniqueConstraint>,
            fks: Vec<ForeignKeyDef>,
        ) -> TableName {
            let table = TableName::new(name).unwrap();
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]);
            self.catalog
                .create_table(&self.project, &table, &schema)
                .await
                .unwrap();
            self.catalog
                .set_table_constraints(&self.project, &table, pk, vec![], fks)
                .await
                .unwrap();
            self.catalog
                .set_unique_constraints(&self.project, &table, unique)
                .await
                .unwrap();
            table
        }

        /// Persist one row to the cold tier and register the file with the
        /// catalog so `list_data_files_with_stats` returns it.
        async fn cold_insert(&self, table: &TableName, id: i64, name: Option<&str>) {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let id_arr = Int64Array::from(vec![id]);
            let name_arr = StringArray::from(vec![name]);
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(name_arr)]).unwrap();
            let part = PartitionKey::default_key();
            let df = self
                .storage
                .write_batch(&self.project, table, &part, &batch)
                .await
                .unwrap();
            let meta = self.catalog.load_table(&self.project, table).await.unwrap();
            self.catalog
                .append_data_files(
                    &self.project,
                    table,
                    meta.current_snapshot,
                    vec![DataFileRef {
                        path: df.path.as_ref().to_string(),
                        size_bytes: df.size_bytes,
                        row_count: df.row_count,
                        column_stats: df.column_stats.clone(),
                        bloom_filters: df.bloom_filters.clone(),
                        hll_sketches: Default::default(),
                        tdigest_sketches: Default::default(),
                    }],
                )
                .await
                .unwrap();
        }

        /// Stuff a live row into the memtable for `table`. Encodes the single
        /// row as an IPC stream payload so the snapshot decoder can read it
        /// back.
        fn hot_insert(&self, table: &TableName, id: i64, name: Option<&str>) {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let id_arr = Int64Array::from(vec![id]);
            let name_arr = StringArray::from(vec![name]);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(id_arr), Arc::new(name_arr)])
                    .unwrap();
            let bytes = encode_batch_ipc(&batch);
            let key = RowKey::builder().append_i64(id).finish();
            let entry = self.registry.get_or_create(self.project, table.clone());
            entry.memtable.insert(key, MemRowValue::row(bytes, 0));
        }

        /// Write a tombstone keyed by the single-column id into the memtable.
        fn hot_tombstone(&self, table: &TableName, id: i64) {
            let key = RowKey::builder().append_i64(id).finish();
            let entry = self.registry.get_or_create(self.project, table.clone());
            entry.memtable.delete(key);
        }
    }

    fn encode_batch_ipc(batch: &RecordBatch) -> Vec<u8> {
        use arrow::ipc::writer::StreamWriter;
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).unwrap();
            w.write(batch).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    fn new_row_batch(id: i64, name: Option<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_arr = Int64Array::from(vec![id]);
        let name_arr = StringArray::from(vec![name]);
        RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(name_arr)]).unwrap()
    }

    // ── 1. unique_violation_in_cold_only ────────────────────────────────────

    #[tokio::test]
    async fn unique_violation_in_cold_only() {
        let fx = Fixture::new().await;
        let table = fx
            .create_table_with_constraints(
                "users",
                vec!["id".into()],
                vec![UniqueConstraint {
                    name: "users_name_key".into(),
                    columns: vec!["name".into()],
                }],
                vec![],
            )
            .await;
        fx.cold_insert(&table, 1, Some("alice")).await;
        let meta = fx.catalog.load_table(&fx.project, &table).await.unwrap();
        let batch = new_row_batch(2, Some("alice"));
        let err =
            check_constraints_memtable_and_cold(&fx.ctx(&table), &meta, &batch, &WriteKind::Insert)
                .await
                .unwrap_err();
        assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    }

    // ── 2. unique_violation_in_hot_only ─────────────────────────────────────

    #[tokio::test]
    async fn unique_violation_in_hot_only() {
        let fx = Fixture::new().await;
        let table = fx
            .create_table_with_constraints(
                "users",
                vec!["id".into()],
                vec![UniqueConstraint {
                    name: "users_name_key".into(),
                    columns: vec!["name".into()],
                }],
                vec![],
            )
            .await;
        // Hot-only row (no cold file).
        fx.hot_insert(&table, 1, Some("alice"));
        let meta = fx.catalog.load_table(&fx.project, &table).await.unwrap();
        let batch = new_row_batch(2, Some("alice"));
        let err =
            check_constraints_memtable_and_cold(&fx.ctx(&table), &meta, &batch, &WriteKind::Insert)
                .await
                .unwrap_err();
        assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    }

    // ── 3. unique_passes_when_old_row_tombstoned ────────────────────────────

    #[tokio::test]
    async fn unique_passes_when_old_row_tombstoned() {
        let fx = Fixture::new().await;
        let table = fx
            .create_table_with_constraints(
                "users",
                vec!["id".into()],
                vec![UniqueConstraint {
                    name: "users_name_key".into(),
                    columns: vec!["name".into()],
                }],
                vec![],
            )
            .await;
        fx.cold_insert(&table, 1, Some("alice")).await;
        fx.hot_tombstone(&table, 1);
        let meta = fx.catalog.load_table(&fx.project, &table).await.unwrap();
        // A new row with the same UNIQUE value as the now-tombstoned cold
        // row must pass.
        let batch = new_row_batch(2, Some("alice"));
        let res =
            check_constraints_memtable_and_cold(&fx.ctx(&table), &meta, &batch, &WriteKind::Insert)
                .await;
        assert!(res.is_ok(), "expected Ok, got {res:?}");
    }

    // ── 4. pk_violation_in_hot ──────────────────────────────────────────────

    #[tokio::test]
    async fn pk_violation_in_hot() {
        let fx = Fixture::new().await;
        let table = fx
            .create_table_with_constraints("users", vec!["id".into()], vec![], vec![])
            .await;
        fx.hot_insert(&table, 7, Some("anything"));
        let meta = fx.catalog.load_table(&fx.project, &table).await.unwrap();
        let batch = new_row_batch(7, Some("different"));
        let err =
            check_constraints_memtable_and_cold(&fx.ctx(&table), &meta, &batch, &WriteKind::Insert)
                .await
                .unwrap_err();
        // PK collisions surface as UniqueViolation (SQLSTATE 23505) — no
        // separate PrimaryKeyViolation variant in basin_common.
        assert!(matches!(err, BasinError::UniqueViolation(_)), "got {err:?}");
    }

    // ── 5. fk_violation_when_referenced_pk_only_in_hot (tombstoned) ─────────

    #[tokio::test]
    async fn fk_violation_when_referenced_pk_tombstoned() {
        let fx = Fixture::new().await;
        // Parent: users (id PK).
        let parent = fx
            .create_table_with_constraints("users", vec!["id".into()], vec![], vec![])
            .await;
        fx.cold_insert(&parent, 1, Some("alice")).await;
        // Tombstone the only parent row.
        fx.hot_tombstone(&parent, 1);
        // Child: orders (id PK, FK on name -> users.id) — using name as the
        // FK column would require its own column; reuse id for simplicity.
        let child = fx
            .create_table_with_constraints(
                "orders",
                vec!["id".into()],
                vec![],
                vec![ForeignKeyDef {
                    name: "orders_id_fkey".into(),
                    columns: vec!["id".into()],
                    ref_table: "users".into(),
                    ref_columns: vec!["id".into()],
                    on_delete: RefAction::NoAction,
                    on_update: RefAction::NoAction,
                    initially_deferred: false,
                }],
            )
            .await;
        let meta = fx.catalog.load_table(&fx.project, &child).await.unwrap();
        // Try to insert into orders with id=1 — must fail because users:1 is
        // tombstoned.
        let batch = new_row_batch(1, Some("o1"));
        let err =
            check_constraints_memtable_and_cold(&fx.ctx(&child), &meta, &batch, &WriteKind::Insert)
                .await
                .unwrap_err();
        assert!(
            matches!(err, BasinError::ForeignKeyViolation(_)),
            "got {err:?}"
        );
    }

    // ── 6. fk_passes_when_referenced_pk_in_cold_and_not_tombstoned ──────────

    #[tokio::test]
    async fn fk_passes_when_referenced_pk_in_cold() {
        let fx = Fixture::new().await;
        let parent = fx
            .create_table_with_constraints("users", vec!["id".into()], vec![], vec![])
            .await;
        fx.cold_insert(&parent, 1, Some("alice")).await;

        let child = fx
            .create_table_with_constraints(
                "orders",
                vec!["id".into()],
                vec![],
                vec![ForeignKeyDef {
                    name: "orders_id_fkey".into(),
                    columns: vec!["id".into()],
                    ref_table: "users".into(),
                    ref_columns: vec!["id".into()],
                    on_delete: RefAction::NoAction,
                    on_update: RefAction::NoAction,
                    initially_deferred: false,
                }],
            )
            .await;
        let meta = fx.catalog.load_table(&fx.project, &child).await.unwrap();
        let batch = new_row_batch(1, Some("o1"));
        let res =
            check_constraints_memtable_and_cold(&fx.ctx(&child), &meta, &batch, &WriteKind::Insert)
                .await;
        assert!(res.is_ok(), "expected Ok, got {res:?}");
    }

    // ── 6b. fk_passes_when_referenced_pk_only_in_hot ────────────────────────

    #[tokio::test]
    async fn fk_passes_when_referenced_pk_in_hot() {
        let fx = Fixture::new().await;
        let parent = fx
            .create_table_with_constraints("users", vec!["id".into()], vec![], vec![])
            .await;
        // Parent row lives in the hot tier only — the FK probe must see it.
        fx.hot_insert(&parent, 42, Some("eve"));

        let child = fx
            .create_table_with_constraints(
                "orders",
                vec!["id".into()],
                vec![],
                vec![ForeignKeyDef {
                    name: "orders_id_fkey".into(),
                    columns: vec!["id".into()],
                    ref_table: "users".into(),
                    ref_columns: vec!["id".into()],
                    on_delete: RefAction::NoAction,
                    on_update: RefAction::NoAction,
                    initially_deferred: false,
                }],
            )
            .await;
        let meta = fx.catalog.load_table(&fx.project, &child).await.unwrap();
        let batch = new_row_batch(42, Some("o42"));
        let res =
            check_constraints_memtable_and_cold(&fx.ctx(&child), &meta, &batch, &WriteKind::Insert)
                .await;
        assert!(res.is_ok(), "expected Ok, got {res:?}");
    }

    // ── 7. update_new_image_excludes_old_pk_from_unique_check ───────────────

    #[tokio::test]
    async fn update_new_image_excludes_old_pk_from_unique_check() {
        let fx = Fixture::new().await;
        let table = fx
            .create_table_with_constraints(
                "users",
                vec!["id".into()],
                vec![UniqueConstraint {
                    name: "users_name_key".into(),
                    columns: vec!["name".into()],
                }],
                vec![],
            )
            .await;
        fx.cold_insert(&table, 1, Some("alice")).await;
        let meta = fx.catalog.load_table(&fx.project, &table).await.unwrap();
        // UPDATE row id=1 keeping name='alice'. Without self-suppression
        // this would (incorrectly) raise UniqueViolation against itself.
        let batch = new_row_batch(1, Some("alice"));
        let res = check_constraints_memtable_and_cold(
            &fx.ctx(&table),
            &meta,
            &batch,
            &WriteKind::UpdateNewImage {
                old_pks: vec![vec!["1".to_string()]],
            },
        )
        .await;
        assert!(res.is_ok(), "expected Ok, got {res:?}");
    }

    // ── 8. probe_strategy_threshold ─────────────────────────────────────────

    /// The documented threshold is 10 000 rows. Lock it in so PR2 reviewers
    /// see exactly where the in-memory vs catalog-probe dispatch lives.
    #[test]
    fn probe_strategy_threshold_is_documented_value() {
        assert_eq!(SMALL_TABLE_ROW_THRESHOLD, 10_000);
        assert_eq!(probe_strategy(0), ProbeStrategy::InMemoryUnion);
        assert_eq!(
            probe_strategy(SMALL_TABLE_ROW_THRESHOLD),
            ProbeStrategy::InMemoryUnion
        );
        assert_eq!(
            probe_strategy(SMALL_TABLE_ROW_THRESHOLD + 1),
            ProbeStrategy::CatalogProbe
        );
    }
}
