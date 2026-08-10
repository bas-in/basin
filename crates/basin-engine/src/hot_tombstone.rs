//! Read-path merge-on-read tombstone suppression for the DELETE hot-tier fast
//! path.
//!
//! When `dml_mutate::exec_delete` takes the `BASIN_HOTTIER_DELETE_FASTPATH`
//! shortcut it writes `MemRowValue::Tombstone` entries into the process-wide
//! `MemTableRegistry` and skips the cold-tier copy-on-write rewrite. The
//! corresponding read paths must therefore consult the registry to drop any
//! cold-tier rows that have been tombstoned, or follow-up SELECTs would return
//! the now-deleted rows.
//!
//! This module provides:
//!
//! * [`snapshot_tombstones`] — gather the tombstone `RowKey` bytes for a
//!   `(project, table)` pair as a fast-lookup `HashSet`. Empty when the
//!   memtable is missing or contains no tombstones.
//! * [`array_value_to_row_key`] — encode a single column value out of an
//!   Arrow `RecordBatch` into the same `RowKey` form `dml_mutate` uses when
//!   writing tombstones. The encoding mirrors `pk_scalar_to_row_key`.
//! * [`TombstoneFilterExec`] — a thin `ExecutionPlan` wrapper around a cold
//!   scan that drops every row whose encoded PK matches one of the
//!   snapshotted tombstones. No-op when the snapshot is empty.
//! * [`maybe_wrap_with_tombstone_filter`] — convenience helper used by
//!   `refresh_table*` and `HtapUnionTable::scan` to apply the filter only
//!   when at least one tombstone exists.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use basin_common::{ProjectId, TableName};
use basin_hottier::{MemRowValue, MemTableRegistry, RowKey};
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

// ── Snapshot helper ──────────────────────────────────────────────────────────

/// Gather every `MemRowValue::Tombstone` key currently registered for
/// `(project, table)` in `registry` into a `HashSet<Vec<u8>>` keyed by raw
/// `RowKey` bytes.
///
/// Returns an empty set when the registry has no entry for the table or when
/// every entry is a live row. Cheap when the table is cold or write-only.
///
/// The snapshot is bounded in size: tombstones live in the memtable only
/// between the DELETE and the next compaction/flush, so the typical OLTP
/// working-set fits comfortably in memory (thousands of keys at most before
/// the flush task drains them).
/// `watermark` implements hot-tier transaction-snapshot isolation:
/// * `None` — auto-commit read. No filtering; every registered tombstone is
///   returned (the read sees the latest committed hot-tier state). Zero extra
///   cost: takes the lighter `snapshot()` path that does not carry seqs.
/// * `Some(w)` — in-transaction read. Only tombstones written at or before the
///   transaction's pinned sequence watermark `w` are returned; a tombstone with
///   `seq > w` was written by another session *after* this transaction pinned
///   its snapshot and MUST stay invisible (it must not hide a row the
///   transaction is entitled to see). The transaction's OWN in-tx tombstones
///   are layered separately by [`merge_tx_overlay`] and always win.
pub(crate) fn snapshot_tombstones(
    registry: &MemTableRegistry,
    project: &ProjectId,
    table: &TableName,
    watermark: Option<u64>,
) -> HashSet<Vec<u8>> {
    let Some(entry) = registry.get(project, table) else {
        return HashSet::new();
    };
    // S4 O(1) emptiness gate (auto-commit only). `tombstone_count` counts
    // entries whose NEWEST version is a Tombstone, so when it is zero an
    // unwatermarked snapshot cannot yield one — skip the O(n) map walk that
    // the steady-state read path otherwise pays per query. A PINNED read
    // (`Some(w)`) may still resolve a HISTORICAL tombstone inside a chain
    // whose newest version is a Row/Update (delete-then-reinsert), which the
    // newest-version counter does not see — so the gate only fires when no
    // watermark applies.
    if watermark.is_none() && entry.memtable.tombstone_count() == 0 {
        return HashSet::new();
    }
    let mut out: HashSet<Vec<u8>> = HashSet::new();
    // `snapshot_with_seq(watermark)` yields, per key, the newest version at or
    // before the watermark (`None` = auto-commit newest). S4 MVCC chains: an
    // overwrite no longer destroys the prior version, so a pinned reader resolves
    // the historical tombstone/row at its watermark instead of skipping the key.
    for (key, value) in entry.memtable.snapshot_with_seq(watermark) {
        if matches!(value, MemRowValue::Tombstone) {
            out.insert(key.as_bytes().to_vec());
        }
    }
    out
}

// ── UPDATE-override snapshot ───────────────────────────────────────────────────

/// Decode one Arrow IPC stream blob into the first `RecordBatch`. Mirrors
/// `fast_select::decode_ipc_batch` / `merge::decode_ipc_row`. `pub(crate)` so
/// `dml_mutate::materialize_hot_overlay_into_cold` can decode the `Update`
/// overrides it pulls out of a seq-carrying `dirty_snapshot` (the snapshot
/// shape that threads per-key seqs into the post-materialize `mark_flushed`
/// ack) with the exact same decoder the read path uses.
pub(crate) fn decode_ipc_row(bytes: &[u8]) -> Option<RecordBatch> {
    use arrow::ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).ok()?;
    reader.next()?.ok()
}

/// Gather every `MemRowValue::Update` override registered for `(project,
/// table)` as a map from raw `RowKey` bytes (the encoded PK) to the decoded
/// post-SET single-row `RecordBatch`.
///
/// These are written by the hot-tier UPDATE fast path
/// (`BASIN_HOTTIER_UPDATE_FASTPATH`). On the read path each override must (a)
/// suppress the stale cold-tier row at the same PK and (b) be surfaced in its
/// place. Returns an empty map when the table has no overrides — the common
/// case, which keeps the read path zero-overhead.
///
/// The override map is bounded like the tombstone snapshot: overrides live in
/// the memtable only between the UPDATE and the next compaction/flush.
/// `watermark` implements the same hot-tier transaction-snapshot isolation as
/// [`snapshot_tombstones`]:
/// * `None` — auto-commit read. Every registered UPDATE override is surfaced.
/// * `Some(w)` — in-transaction read. Only overrides written at or before the
///   transaction's pinned sequence `w` are surfaced; an override with `seq > w`
///   was written by another session after the snapshot was pinned and is
///   dropped so the transaction continues to see the pre-snapshot value (the
///   cold row, or its own earlier in-tx override via [`merge_tx_overlay`]).
///
/// ## Auto-commit memoization
///
/// The `None` (auto-commit) path is memoized per table in
/// `MemTableEntry::overlay_memo`: the IPC decode of every override otherwise
/// re-runs on EVERY read while any override is outstanding (every
/// `TombstoneFilteringTable::scan`, `supports_filters_pushdown`, and
/// fast-select cold merge). The memo key is `(epoch, update_count)`:
///
/// * every memtable mutation bumps `epoch`, so a new/changed/removed override
///   invalidates the memo;
/// * `mark_flushed` re-tags acked `Update`s as `Row` — which REMOVES them from
///   this snapshot's output — deliberately WITHOUT bumping `epoch` (its
///   documented invariant: the observable row VALUES are unchanged, only the
///   overlay membership shrinks). Pure epoch keying would therefore serve a
///   stale, larger override map after a flush ack. Every such re-tag
///   decrements `update_count`, and no path increments `update_count` without
///   also bumping `epoch`, so adding `update_count` to the key makes the pair
///   change whenever the output can change.
///
/// The key is captured BEFORE the decode; `epoch` is monotonic, so a memo
/// built concurrently with a mutation is keyed strictly in the past and can
/// never match a later read's key. Pinned (`Some(w)`) reads bypass the memo
/// entirely — their output varies by watermark.
pub(crate) fn snapshot_updates(
    registry: &MemTableRegistry,
    project: &ProjectId,
    table: &TableName,
    watermark: Option<u64>,
) -> std::collections::HashMap<Vec<u8>, RecordBatch> {
    let Some(entry) = registry.get(project, table) else {
        return std::collections::HashMap::new();
    };
    // S4 O(1) emptiness gate (auto-commit only) — mirror of the
    // `snapshot_tombstones` gate above. `update_count` counts entries whose
    // NEWEST version is an `Update`; a pinned read may still resolve a
    // historical `Update` in a chain whose newest version is something else
    // (update-then-delete), so the gate only fires with no watermark.
    if watermark.is_none() && entry.memtable.update_count() == 0 {
        return std::collections::HashMap::new();
    }
    if watermark.is_none() {
        // Capture the validity key BEFORE decoding (see the doc note above).
        let key = (entry.memtable.epoch(), entry.memtable.update_count());
        if let Some(memo) = entry.overlay_memo.read().as_ref() {
            if (memo.epoch, memo.update_count) == key {
                if let Ok(map) = Arc::clone(&memo.decoded)
                    .downcast::<std::collections::HashMap<Vec<u8>, RecordBatch>>()
                {
                    // Hand the caller an owned map (some callers layer the tx
                    // overlay on top). Cloning is per-entry `Arc` bumps plus
                    // the key bytes — far cheaper than re-decoding IPC.
                    return map.as_ref().clone();
                }
            }
        }
        let out = decode_update_overlay(&entry, None);
        *entry.overlay_memo.write() = Some(basin_hottier::OverlayMemo {
            epoch: key.0,
            update_count: key.1,
            decoded: Arc::new(out.clone()),
        });
        return out;
    }
    decode_update_overlay(&entry, watermark)
}

/// Decode the `Update` overrides of `entry` at `watermark` into the override
/// map. Factored out of [`snapshot_updates`] so the memoized (auto-commit)
/// and pinned paths share one decoder.
fn decode_update_overlay(
    entry: &basin_hottier::MemTableEntry,
    watermark: Option<u64>,
) -> std::collections::HashMap<Vec<u8>, RecordBatch> {
    let mut out: std::collections::HashMap<Vec<u8>, RecordBatch> = std::collections::HashMap::new();
    // `snapshot_with_seq(watermark)` yields, per key, the newest version at or
    // before the watermark (`None` = auto-commit newest). S4 MVCC chains: a
    // pinned reader resolves the historical override at its watermark, even if
    // the key was overwritten again by a later (post-snapshot) write.
    for (key, value) in entry.memtable.snapshot_with_seq(watermark) {
        if let MemRowValue::Update { bytes, .. } = value {
            if let Some(rb) = decode_ipc_row(&bytes) {
                if rb.num_rows() > 0 {
                    out.insert(key.as_bytes().to_vec(), rb);
                }
            }
        }
    }
    out
}

/// Merge a transaction-scoped overlay map (encoded PK → `Update`/`Tombstone`)
/// ON TOP of the shared-registry snapshot pair `(tombstones, updates)` so the
/// owning transaction's own uncommitted single-row PK UPDATE/DELETE fast-path
/// writes win over both the shared registry and the cold tier.
///
/// Precedence per PK (highest first): tx overlay > shared registry > cold.
/// For each tx-overlay entry:
///   * `Tombstone` → insert the key into `tombstones`, remove any shared
///     `updates` override for that key (a tombstone hides it).
///   * `Update`    → decode the post-image row and insert into `updates`,
///     remove the key from `tombstones` (the override resurrects/replaces it).
///
/// `tx_overlay` is empty (the no-tx / no-in-tx-fast-path case) → both maps are
/// returned unchanged, so the steady-state read path pays nothing.
pub(crate) fn merge_tx_overlay(
    tombstones: &mut HashSet<Vec<u8>>,
    updates: &mut std::collections::HashMap<Vec<u8>, RecordBatch>,
    tx_overlay: &std::collections::BTreeMap<RowKey, MemRowValue>,
) {
    for (key, value) in tx_overlay {
        let kb = key.as_bytes().to_vec();
        match value {
            MemRowValue::Tombstone => {
                updates.remove(&kb);
                tombstones.insert(kb);
            }
            MemRowValue::Update { bytes, .. } | MemRowValue::Row { bytes, .. } => {
                if let Some(rb) = decode_ipc_row(bytes) {
                    if rb.num_rows() > 0 {
                        tombstones.remove(&kb);
                        updates.insert(kb, rb);
                    }
                }
            }
        }
    }
}

// ── Array → RowKey encoder ───────────────────────────────────────────────────

/// Encode the single value at `row_idx` in `array` (whose declared logical
/// type is `col_dt`) into a `RowKey` whose lexicographic byte order matches
/// the cluster-key sort and the `pk_scalar_to_row_key` encoding used by the
/// fast-path DELETE writer.
///
/// Returns `None` when:
/// * the value is NULL (PK columns are NOT NULL — but we degrade gracefully
///   rather than panic so a single bad row never corrupts the merge),
/// * the column's datatype is not in the supported set (`Int16/Int32/Int64
///   /UInt64/Utf8/LargeUtf8/Utf8View/Boolean`), or
/// * the Arrow array's runtime type does not match the declared `col_dt`.
///
/// Mirrors `dml_mutate::pk_scalar_to_row_key`. Any addition to that function
/// must be reflected here to keep tombstone matching correct.
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

// ── ExecutionPlan wrapper ────────────────────────────────────────────────────

/// Drops every row whose encoded PK matches one of `tombstones`.
///
/// When the PK column is not present in the scan's output schema (e.g. a
/// projection that omits it) the filter degrades to a pass-through: there is
/// no safe way to evaluate "is this row tombstoned?" without the key bytes.
/// In practice this only matters for `SELECT non_pk_col FROM t` issued
/// immediately after a fast-path DELETE — a vanishingly rare shape — and the
/// next compaction flush will reconcile correctness regardless.
///
/// Single-column PKs only. Composite PKs trigger an empty `tombstones` set
/// in `snapshot_tombstones` because the fast-path writer rejects them in
/// `try_resolve_fast_path_pks`, so we never construct a multi-column key
/// to compare against.
#[derive(Debug)]
pub(crate) struct TombstoneFilterExec {
    inner: Arc<dyn ExecutionPlan>,
    /// Name of the single PK column the tombstones key on. Read from the
    /// table's catalog metadata at plan time.
    pk_column: String,
    /// Declared Arrow data type of the PK column. Read from the catalog
    /// schema at plan time so we always encode against the catalog's type,
    /// not whatever the scan happens to emit (e.g. Utf8 vs Utf8View).
    pk_dt: DataType,
    /// Snapshot of tombstone `RowKey` bytes at plan time. Bounded by the
    /// memtable's tombstone count, which the flush task keeps small.
    tombstones: Arc<HashSet<Vec<u8>>>,
    /// Cached `PlanProperties` mirroring `inner`'s (the filter is a pure
    /// row-discarding pass with no schema or ordering change).
    props: Arc<PlanProperties>,
}

impl TombstoneFilterExec {
    pub(crate) fn new(
        inner: Arc<dyn ExecutionPlan>,
        pk_column: String,
        pk_dt: DataType,
        tombstones: Arc<HashSet<Vec<u8>>>,
    ) -> Self {
        let props = Arc::new(PlanProperties::new(
            inner.equivalence_properties().clone(),
            inner.output_partitioning().clone(),
            inner.pipeline_behavior(),
            inner.boundedness(),
        ));
        Self {
            inner,
            pk_column,
            pk_dt,
            tombstones,
            props,
        }
    }
}

impl DisplayAs for TombstoneFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TombstoneFilterExec: pk={} suppressed={}",
            self.pk_column,
            self.tombstones.len()
        )
    }
}

impl ExecutionPlan for TombstoneFilterExec {
    fn name(&self) -> &str {
        "TombstoneFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TombstoneFilterExec::new(
            children.swap_remove(0),
            self.pk_column.clone(),
            self.pk_dt.clone(),
            Arc::clone(&self.tombstones),
        )))
    }

    // ── Physical filter pushdown (transparent passthrough) ───────────────────
    //
    // TombstoneFilterExec ONLY suppresses rows whose PK is tombstoned — it
    // never adds or transforms rows. A row filter therefore commutes with the
    // suppression: `filter(suppress(scan)) == suppress(filter(scan))`. So a
    // predicate can be pushed transparently to the cold child scan (which can
    // then prune via Vortex/Parquet pushdown) AND removed from above us.
    //
    // Without these two methods the default `all_unsupported` blocks pushdown:
    // the `FilterExec` stays stuck above us and the cold scan reads EVERY row
    // (a selective `WHERE id < 100` on a table with any DELETE tombstone became
    // a full-table scan — the JSONB-at-1M regression). Mirrors the transparent
    // passthrough that `CoalesceBatchesExec` uses.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let schema = self.inner.schema();
        let pk_column = self.pk_column.clone();
        let pk_dt = self.pk_dt.clone();
        let tombstones = Arc::clone(&self.tombstones);
        let mapped = inner_stream.map(move |batch_res| {
            batch_res.and_then(|batch| filter_batch(&batch, &pk_column, &pk_dt, &tombstones))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }
}

// ── UPDATE override overlay ExecutionPlan ──────────────────────────────────────

/// Drops every cold row whose PK matches an `Update` override key, then emits
/// the override (post-SET) rows once the inner stream is exhausted.
///
/// This is the DataFusion-plan twin of [`apply_update_overlay_to_batches`]:
/// the `fast_select` point-lookup path uses the `Vec<RecordBatch>` helper,
/// while the catalog-registered read path (`refresh_table` →
/// `TombstoneFilteringTable`) wraps the cold scan with this plan so bulk
/// SELECTs / COUNT(*) / ORDER BY after a fast-path UPDATE surface the new
/// values and never the stale cold ones.
///
/// Single-column PKs only — the fast-path UPDATE writer rejects composite PKs,
/// so the override map is always empty for those tables and the wrap is a
/// no-op.
#[derive(Debug)]
pub(crate) struct UpdateOverlayExec {
    inner: Arc<dyn ExecutionPlan>,
    pk_column: String,
    pk_dt: DataType,
    /// Override rows reprojected to `inner`'s output schema, keyed by encoded
    /// PK bytes. The keys double as the suppression set for cold rows.
    updates: Arc<std::collections::HashMap<Vec<u8>, RecordBatch>>,
    props: Arc<PlanProperties>,
}

impl UpdateOverlayExec {
    pub(crate) fn new(
        inner: Arc<dyn ExecutionPlan>,
        pk_column: String,
        pk_dt: DataType,
        updates: Arc<std::collections::HashMap<Vec<u8>, RecordBatch>>,
    ) -> Self {
        let props = Arc::new(PlanProperties::new(
            inner.equivalence_properties().clone(),
            inner.output_partitioning().clone(),
            inner.pipeline_behavior(),
            inner.boundedness(),
        ));
        Self {
            inner,
            pk_column,
            pk_dt,
            updates,
            props,
        }
    }
}

impl DisplayAs for UpdateOverlayExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UpdateOverlayExec: pk={} overridden={}",
            self.pk_column,
            self.updates.len()
        )
    }
}

impl ExecutionPlan for UpdateOverlayExec {
    fn name(&self) -> &str {
        "UpdateOverlayExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UpdateOverlayExec::new(
            children.swap_remove(0),
            self.pk_column.clone(),
            self.pk_dt.clone(),
            Arc::clone(&self.updates),
        )))
    }

    // ── Physical filter pushdown (conservative: PK-only into child) ──────────
    //
    // `UpdateOverlayExec` does TWO things to its child stream:
    //   1. Suppresses cold rows whose PK matches an override key.
    //   2. UNCONDITIONALLY appends every override (post-SET) row at the tail.
    //
    // (1) commutes with a row filter (just like `TombstoneFilterExec`). But (2)
    // does NOT: the appended rows are produced from `self.updates.values()`
    // regardless of any predicate the cold child was given. So if a parent
    // `WHERE col = X` filter is pushed into the cold scan and the upper
    // `FilterExec` is then removed, override rows that don't match the
    // predicate would leak into the output.
    //
    // Two-axis strategy:
    //
    //   * To the **child**: declare the filter "supported" iff it references
    //     ONLY the PK column. The cold scan can then use it for I/O reduction
    //     (Vortex/Parquet row-group prune, GIN row-group selection, predicate
    //     pushdown). Filters touching any non-PK column stay unsupported at
    //     the child so they are not pushed into the cold scan (the overlay
    //     may have set that column to a value the cold pre-filter would have
    //     dropped — see safety note below).
    //
    //   * To the **parent**: ALWAYS report `unsupported`, regardless of
    //     whether the child accepted. This keeps the upper `FilterExec` in
    //     place so it re-evaluates the predicate above us, removing any
    //     appended override row that does not match. Without this the
    //     unconditional-append path (2) leaks non-matching overrides.
    //
    // Safety analysis (PK-only is conservative-correct):
    //   * The hot-tier UPDATE fast path rejects assignments that touch the PK
    //     (`dml_mutate::try_resolve_fast_path_pks` / `hot_tier_update_by_pk`),
    //     so an override row's PK is byte-identical to the cold row's PK.
    //     Therefore a `WHERE pk = …` predicate evaluated on the cold scan
    //     gives exactly the same row-set as if it were evaluated post-overlay.
    //   * For any non-PK column, the override is a full-row replacement — the
    //     fast-path writer does not know which columns SQL assigned, so the
    //     overlay must be treated as if it could have changed any non-PK
    //     value. Pushing a non-PK filter into the cold scan could drop a row
    //     whose cold value fails the predicate but whose overlay value
    //     satisfies it. Hence: non-PK filters stay above the overlay.
    //
    // Net effect: I/O reduction at the cold scan for PK predicates (the bug
    // #88 win), correctness re-enforced by the retained upper `FilterExec`
    // for the appended override rows. When no override is in flight
    // (`updates.is_empty()`) the wrap is skipped by `TombstoneFilteringTable`,
    // so the steady-state read path is unchanged.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        // Resolve the PK column's index in our (= the child's) schema. If the
        // PK isn't in the schema (e.g. a projection that omitted it — the
        // overlay's `filter_batch` already degrades to pass-through in that
        // case) we have no safe filter to push, so mark every filter
        // unsupported and let the upper `FilterExec` handle them.
        let schema = self.inner.schema();
        let Ok(pk_idx) = schema.index_of(&self.pk_column) else {
            return Ok(FilterDescription::new()
                .with_child(ChildFilterDescription::all_unsupported(&parent_filters)));
        };
        let mut allowed = HashSet::new();
        allowed.insert(pk_idx);
        let child = &self.inner;
        let desc = ChildFilterDescription::from_child_with_allowed_indices(
            &parent_filters,
            allowed,
            child,
        )?;
        Ok(FilterDescription::new().with_child(desc))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // Tell the parent NONE of the filters are fully evaluated by us so the
        // upper `FilterExec` stays in place. The child may still have used the
        // PK predicate for I/O reduction inside its scan, but we cannot claim
        // exactness because of the unconditional override-row append in
        // `execute`. See the safety note on `gather_filters_for_pushdown`.
        Ok(FilterPushdownPropagation::all_unsupported(
            child_pushdown_result,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = self.inner.schema();
        // Only the first partition appends the override rows; otherwise an
        // N-partition cold scan would surface each override N times. All
        // partitions still suppress overridden cold rows.
        let inner_stream = self.inner.execute(partition, context)?;
        let pk_column = self.pk_column.clone();
        let pk_dt = self.pk_dt.clone();
        let updates = Arc::clone(&self.updates);
        let suppress: HashSet<Vec<u8>> = updates.keys().cloned().collect();
        // Reproject the override rows to the output schema once.
        let appended: Vec<RecordBatch> = if partition == 0 {
            updates
                .values()
                .map(|ov| reproject_row(ov, &schema))
                .collect::<DFResult<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let suppressed = inner_stream.map(move |batch_res| {
            batch_res.and_then(|batch| filter_batch(&batch, &pk_column, &pk_dt, &suppress))
        });
        // Chain the suppressed cold stream with the override rows.
        let tail = futures::stream::iter(appended.into_iter().map(Ok));
        let combined = suppressed.chain(tail);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, combined)))
    }
}

/// Build a `BooleanArray` mask that drops rows whose PK is in `tombstones`.
/// Returns the input batch unchanged when the PK column is absent (we can't
/// safely evaluate the filter — see `TombstoneFilterExec` docs).
pub(crate) fn filter_batch(
    batch: &RecordBatch,
    pk_column: &str,
    pk_dt: &DataType,
    tombstones: &HashSet<Vec<u8>>,
) -> DFResult<RecordBatch> {
    let Ok(pk_idx) = batch.schema().index_of(pk_column) else {
        return Ok(batch.clone());
    };
    let pk_array = batch.column(pk_idx);
    let n = batch.num_rows();
    let mut keep: Vec<bool> = Vec::with_capacity(n);
    for r in 0..n {
        match array_value_to_row_key(pk_array.as_ref(), r, pk_dt) {
            Some(key) => keep.push(!tombstones.contains(key.as_bytes())),
            // Unsupported type / NULL PK / type mismatch — keep the row.
            // The fast-path writer would have rejected the DELETE in this
            // case, so no live tombstone can match. Conservatively retain.
            None => keep.push(true),
        }
    }
    let mask = BooleanArray::from(keep);
    let filtered = arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(filtered)
}

// ── Batch-level convenience wrapper ──────────────────────────────────────────

/// Apply the merge-on-read tombstone filter to an already-collected vector of
/// `RecordBatch`es and return the surviving rows.
///
/// Used by the point-lookup fast path in `fast_select::execute_simple_select`,
/// which reads cold-tier files directly into `Vec<RecordBatch>` and never
/// constructs an `ExecutionPlan` we could wrap with `TombstoneFilterExec`.
///
/// Callers should only invoke this when `tombstones` is non-empty (a
/// `snapshot_tombstones` call followed by a `.is_empty()` check) so the happy
/// path stays zero-overhead. When `tombstones` is empty this still returns
/// `batches` unchanged but pays one allocation per batch.
///
/// Mirrors the behaviour of `TombstoneFilterExec::execute`: rows whose PK
/// encodes to one of the snapshotted `RowKey` bytes are dropped; batches
/// missing the PK column pass through unchanged (no safe key to compare).
pub(crate) fn apply_tombstone_filter_to_batches(
    batches: Vec<RecordBatch>,
    tombstones: &HashSet<Vec<u8>>,
    pk_column: &str,
    pk_dt: &DataType,
) -> DFResult<Vec<RecordBatch>> {
    if tombstones.is_empty() {
        return Ok(batches);
    }
    batches
        .into_iter()
        .map(|b| filter_batch(&b, pk_column, pk_dt, tombstones))
        .collect()
}

/// Apply the merge-on-read **UPDATE override** overlay to an already-collected
/// `Vec<RecordBatch>` of cold-tier rows: drop every cold row whose PK matches
/// an `Update` override key, then append the override (post-SET) rows.
///
/// Used by the point-lookup fast path in `fast_select::execute_simple_select`
/// (which reads cold-tier files into `Vec<RecordBatch>` and never builds an
/// `ExecutionPlan`). Mirrors the suppression-then-surface semantics of the
/// DataFusion `UpdateOverlayExec` so both read paths agree.
///
/// Override rows are reprojected to the cold batches' schema so a SELECT with
/// a column projection still lines up. When a batch is missing the PK column
/// it passes through unchanged (no safe key to compare) — the same
/// conservative degradation as the tombstone filter.
///
/// `tombstones` is the union of true tombstones AND the override PK keys; the
/// caller passes the override map separately so the appended rows can be
/// reprojected. When `updates` is empty this is a zero-overhead pass-through.
///
/// `predicates` is the query's pushed-down conjunctive WHERE filter. It is
/// applied to each override row BEFORE it is appended — without it the overlay
/// surfaces EVERY outstanding hot UPDATE override regardless of the query's
/// WHERE clause. On the `Vec<RecordBatch>` fast path there is no downstream
/// `FilterExec` to drop the non-matching overrides (unlike the DataFusion
/// `UpdateOverlayExec` twin, which always has a `FilterExec` above it), so a
/// `SELECT … WHERE pk = 4` would otherwise also emit the override row for
/// `pk = 2`, producing the nondeterministic crossed-value results that the
/// HashMap iteration order selected between. An override row whose full-schema
/// form fails any predicate is skipped; an override missing a predicate column
/// (e.g. a projection that dropped it) is conservatively kept.
pub(crate) fn apply_update_overlay_to_batches(
    batches: Vec<RecordBatch>,
    updates: &std::collections::HashMap<Vec<u8>, RecordBatch>,
    pk_column: &str,
    pk_dt: &DataType,
    predicates: &[basin_storage::Predicate],
) -> DFResult<Vec<RecordBatch>> {
    if updates.is_empty() {
        return Ok(batches);
    }
    // Suppress cold rows whose PK has been overridden.
    let suppress: HashSet<Vec<u8>> = updates.keys().cloned().collect();
    let mut out: Vec<RecordBatch> =
        apply_tombstone_filter_to_batches(batches, &suppress, pk_column, pk_dt)?;
    // Determine the output schema to reproject overrides onto. Prefer an
    // existing cold batch (which already carries the requested projection);
    // fall back to the override row's own (full) schema when the cold side is
    // empty (every matched row was an override over an absent/flushed file).
    let target_schema = out.first().map(|b| b.schema());
    let mut appended: Vec<RecordBatch> = Vec::new();
    for ov in updates.values() {
        // Re-apply the query's WHERE filter to the override row. The override
        // carries the FULL row schema, so every predicate column is present —
        // evaluate against `ov` (not the reprojected row, whose projection may
        // have dropped a filter column). A predicate column genuinely absent
        // from the override schema bubbles an error from `evaluate_predicate`;
        // treat that as "keep" (conservative — never hide a live row).
        if !override_row_matches(ov, predicates) {
            continue;
        }
        let row = match &target_schema {
            Some(schema) => reproject_row(ov, schema)?,
            None => ov.clone(),
        };
        appended.push(row);
    }
    // Surface the passing overrides as ONE concatenated batch instead of one
    // single-row batch each: every downstream consumer (sort, aggregate,
    // projection, the pgwire encoder) pays per-batch overhead, so N
    // outstanding overrides used to add N tiny batches to every read. The
    // reprojection above normalizes each row to the same schema; if any rows
    // still diverge (defensive — only reachable via the no-cold-batch
    // `ov.clone()` path), fall back to appending them individually rather
    // than failing the read.
    match appended.len() {
        0 => {}
        1 => out.push(appended.remove(0)),
        _ => {
            let schema = appended[0].schema();
            if appended.iter().all(|b| b.schema() == schema) {
                let merged =
                    arrow_select::concat::concat_batches(&schema, &appended).map_err(|e| {
                        datafusion::common::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                out.push(merged);
            } else {
                out.extend(appended);
            }
        }
    }
    Ok(out)
}

/// Test whether the single-row override batch `ov` satisfies every conjunctive
/// `predicate`. Returns `true` (keep) when there are no predicates or when a
/// predicate cannot be evaluated against the row (missing column / unsupported
/// type) — the conservative choice that never hides a live override.
fn override_row_matches(ov: &RecordBatch, predicates: &[basin_storage::Predicate]) -> bool {
    if ov.num_rows() == 0 {
        return false;
    }
    for p in predicates {
        match basin_storage::evaluate_predicate(ov, p) {
            Ok(mask) => {
                // Row matches the atom only if the (row 0) mask bit is a
                // non-null true. A null or false drops the override.
                if mask.is_null(0) || !mask.value(0) {
                    return false;
                }
            }
            // Column missing / unsupported coercion: keep conservatively.
            Err(_) => continue,
        }
    }
    true
}

/// Reproject a (full-schema) override row to `target` by selecting the
/// `target` columns by name. Missing columns degrade to passing the override
/// row through unchanged (defensive — the caller's projection should always be
/// a subset of the full row schema).
///
/// After projection the row is normalized to `target`'s physical types via
/// [`normalize_batch_to_schema`]. This is the load-bearing step for the JSONB
/// concat bug: the override row is decoded from the memtable's Arrow-IPC blob,
/// which preserves the WRITER's catalog types (JSONB → `LargeBinary`). The
/// `target` schema, however, is the COLD scan's output schema, which for a
/// Vortex table round-trips JSONB through `BinaryView` → plain `Binary`
/// (`normalize_view_types_schema`). Chaining a `LargeBinary` override row into
/// a stream / concat of `Binary` cold batches makes arrow reject with
/// "It is not possible to concatenate arrays of different data types
/// (Binary, LargeBinary)". Casting to `target` here keeps the appended override
/// row physically identical to the cold batches it is unioned with.
fn reproject_row(row: &RecordBatch, target: &SchemaRef) -> DFResult<RecordBatch> {
    let mut idxs: Vec<usize> = Vec::with_capacity(target.fields().len());
    for f in target.fields() {
        match row.schema().index_of(f.name()) {
            Ok(i) => idxs.push(i),
            Err(_) => return Ok(normalize_batch_to_schema(row.clone(), target)),
        }
    }
    let projected = row
        .project(&idxs)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(normalize_batch_to_schema(projected, target))
}

/// Cast any column whose physical Arrow type differs from `target`'s
/// same-named field across the byte-string width/view family
/// (`Binary`↔`LargeBinary`, `Utf8`↔`LargeUtf8`↔`Utf8View`) so the returned
/// batch is physically concat-compatible with batches that already carry
/// `target`'s types. This normalizes at the READ/merge boundary only — the
/// on-disk writer output is untouched (casting there would orphan existing
/// files; see `dml_mutate::materialize_hot_overlay_into_cold`).
///
/// Producers that diverge for the same logical column:
///   * Vortex cold decode → JSONB (`LargeBinary` in the catalog) surfaces as
///     `Binary`; `VARCHAR`/`TEXT` (`Utf8`) may surface as `Utf8View`/`Utf8`.
///   * Memtable Arrow-IPC override rows → carry the writer's catalog types
///     verbatim (JSONB → `LargeBinary`, text → `Utf8`).
///   * Parquet cold decode → catalog-aligned already (this is a no-op there).
///
/// Only the listed string/binary families are coerced; any other type mismatch
/// is left untouched so a genuine schema bug still surfaces downstream rather
/// than being silently papered over. Field NAME (not index) is the join key, so
/// a projected override missing some `target` columns is handled per-column.
/// The cast is `arrow::compute::cast`, which is zero-copy for the offset-only
/// widenings arrow can do without re-buffering and cheap for the small
/// (single-row override / UPDATE-touched) batches on these paths.
/// Pad `batch` up to the full catalog `target` schema: columns the batch
/// lacks (files/rows written before an ALTER ADD COLUMN) are appended as
/// all-NULL arrays, and columns are emitted in target order. Type-family
/// coercion is delegated to [`normalize_batch_to_schema`]. No-op (same
/// batch back) when the shapes already match.
pub(crate) fn pad_batch_to_schema(
    batch: RecordBatch,
    target: &arrow_schema::SchemaRef,
) -> crate::Result<RecordBatch> {
    let batch = normalize_batch_to_schema(batch, target.as_ref());
    if batch.schema().fields().len() == target.fields().len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(target.fields())
            .all(|(a, b)| a.name() == b.name())
    {
        return Ok(batch);
    }
    let n = batch.num_rows();
    let mut cols: Vec<arrow_array::ArrayRef> = Vec::with_capacity(target.fields().len());
    for f in target.fields() {
        match batch.schema().index_of(f.name()) {
            Ok(i) => cols.push(batch.column(i).clone()),
            Err(_) => cols.push(arrow_array::new_null_array(f.data_type(), n)),
        }
    }
    RecordBatch::try_new(target.clone(), cols)
        .map_err(|e| basin_common::BasinError::internal(format!("pad batch to schema: {e}")))
}

pub(crate) fn normalize_batch_to_schema(
    batch: RecordBatch,
    target: &arrow_schema::Schema,
) -> RecordBatch {
    use arrow_array::ArrayRef;
    let read_schema = batch.schema();
    let mut changed = false;
    let mut fields: Vec<Arc<arrow_schema::Field>> = Vec::with_capacity(read_schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(read_schema.fields().len());
    for (idx, f) in read_schema.fields().iter().enumerate() {
        let col = batch.column(idx).clone();
        let want = target
            .field_with_name(f.name())
            .ok()
            .map(|cf| cf.data_type().clone());
        let coerce_to = match (&want, f.data_type()) {
            (Some(w), have) if w != have && is_str_bin_family_pair(have, w) => Some(w.clone()),
            _ => None,
        };
        match coerce_to {
            Some(w) => match arrow::compute::cast(&col, &w) {
                Ok(casted) => {
                    changed = true;
                    fields.push(Arc::new(
                        arrow_schema::Field::new(f.name(), w, f.is_nullable())
                            .with_metadata(f.metadata().clone()),
                    ));
                    columns.push(casted);
                }
                // A cast we expected to be infallible failed — fall back to the
                // original column. The downstream concat/stream will then surface
                // the type mismatch with its own diagnostic rather than us masking
                // it here.
                Err(_) => {
                    fields.push(f.clone());
                    columns.push(col);
                }
            },
            None => {
                fields.push(f.clone());
                columns.push(col);
            }
        }
    }
    if !changed {
        return batch;
    }
    let schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        read_schema.metadata().clone(),
    ));
    // Infallible: column count/lengths are unchanged and each cast preserves
    // row count. Fall back to the input on the impossible error branch.
    RecordBatch::try_new(schema, columns).unwrap_or(batch)
}

/// True iff `have`→`want` is one of the byte-string width/view widenings or
/// narrowings we treat as a safe read-boundary normalization.
///
/// Includes `BinaryView ↔ LargeBinary`: Vortex 0.71 can surface an on-disk
/// `LargeBinary` column as `BinaryView` depending on its internal layout.
/// `normalize_batch_to_schema` must coerce it to the catalog `LargeBinary` so
/// that `extract_promoted_value` (promoted JSONB shadow columns) and any other
/// engine layer that downcasts to `LargeBinaryArray` works correctly.
fn is_str_bin_family_pair(have: &DataType, want: &DataType) -> bool {
    use DataType::{Binary, BinaryView, LargeBinary, LargeUtf8, Utf8, Utf8View};
    matches!(
        (have, want),
        (Binary, LargeBinary)
            | (LargeBinary, Binary)
            | (BinaryView, LargeBinary)
            | (LargeBinary, BinaryView)
            | (BinaryView, Binary)
            | (Binary, BinaryView)
            | (Utf8, LargeUtf8)
            | (LargeUtf8, Utf8)
            | (Utf8, Utf8View)
            | (Utf8View, Utf8)
            | (LargeUtf8, Utf8View)
            | (Utf8View, LargeUtf8)
    )
}

// ── Convenience wrapper ──────────────────────────────────────────────────────

/// Wrap `inner` with a `TombstoneFilterExec` when the table has at least one
/// tombstone in the process-wide memtable registry. Returns `inner` unchanged
/// otherwise.
///
/// The PK column is looked up from `pk_columns` (single-column PK only); if
/// the table has a composite PK we skip the wrap because the fast-path writer
/// never writes tombstones for composite-PK tables.
pub(crate) fn maybe_wrap_with_tombstone_filter(
    inner: Arc<dyn ExecutionPlan>,
    registry: &MemTableRegistry,
    project: &ProjectId,
    table: &TableName,
    pk_columns: &[String],
    schema: &arrow_schema::Schema,
) -> Arc<dyn ExecutionPlan> {
    if pk_columns.len() != 1 {
        return inner;
    }
    let pk_col = &pk_columns[0];
    // Auto-commit DataFusion read path: no transaction watermark.
    let tombs = snapshot_tombstones(registry, project, table, None);
    if tombs.is_empty() {
        return inner;
    }
    let Ok(pk_idx) = schema.index_of(pk_col) else {
        return inner;
    };
    let pk_dt = schema.field(pk_idx).data_type().clone();
    Arc::new(TombstoneFilterExec::new(
        inner,
        pk_col.clone(),
        pk_dt,
        Arc::new(tombs),
    ))
}

// ── TableProvider wrapper ────────────────────────────────────────────────────

use crate::Engine;

/// A [`datafusion::catalog::TableProvider`] that wraps the cold-tier provider
/// and inserts a [`TombstoneFilterExec`] above its scan whenever the
/// process-wide `MemTableRegistry` has at least one tombstone for the table.
///
/// Used by `session::refresh_table*` so that any SELECT issued AFTER a
/// fast-path DELETE drops the now-tombstoned cold-tier rows. When the
/// registry has no tombstones for the table the scan is a zero-cost
/// pass-through (one `DashMap::get` + one `BTreeMap::iter`).
pub(crate) struct TombstoneFilteringTable {
    cold: Arc<dyn datafusion::catalog::TableProvider>,
    engine: Engine,
    project: ProjectId,
    table: TableName,
    pk_columns: Vec<String>,
    // Cached catalog schema — used to resolve the PK's declared `DataType`,
    // which may differ from the per-batch schema the scan emits (e.g.
    // Utf8 vs Utf8View after the listing-format promotion).
    catalog_schema: SchemaRef,
}

impl TombstoneFilteringTable {
    pub(crate) fn new(
        cold: Arc<dyn datafusion::catalog::TableProvider>,
        engine: Engine,
        project: ProjectId,
        table: TableName,
        pk_columns: Vec<String>,
        catalog_schema: SchemaRef,
    ) -> Self {
        Self {
            cold,
            engine,
            project,
            table,
            pk_columns,
            catalog_schema,
        }
    }
}

impl fmt::Debug for TombstoneFilteringTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TombstoneFilteringTable")
            .field("table", &self.table)
            .field("pk_columns", &self.pk_columns)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for TombstoneFilteringTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.cold.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.cold.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let registry = self.engine.memtable_registry();
        // `TombstoneFilteringTable` is the AUTO-COMMIT (non-transactional) cold
        // read provider; transaction-isolated reads go through `HtapUnionTable`
        // instead. No watermark → surface the latest committed hot-tier state.
        let tombs = snapshot_tombstones(registry.as_ref(), &self.project, &self.table, None);
        let updates = snapshot_updates(registry.as_ref(), &self.project, &self.table, None);

        // No tombstones AND no UPDATE overrides → zero-overhead pass-through.
        // Hand the cold provider the original projection / filters / limit.
        if (tombs.is_empty() && updates.is_empty()) || self.pk_columns.len() != 1 {
            let cold_plan = self.cold.scan(state, projection, filters, limit).await?;
            return Ok(cold_plan);
        }

        let pk_col = &self.pk_columns[0];
        let Ok(pk_idx_in_schema) = self.catalog_schema.index_of(pk_col) else {
            // PK column missing from catalog schema (defensive — would be a
            // catalog corruption). Fall back to the pass-through so we never
            // crash a read just because of bad metadata.
            let cold_plan = self.cold.scan(state, projection, filters, limit).await?;
            return Ok(cold_plan);
        };
        let pk_dt = self
            .catalog_schema
            .field(pk_idx_in_schema)
            .data_type()
            .clone();

        // If the caller's projection omits the PK column we must add it so the
        // tombstone filter / update overlay has the key bytes to compare. We
        // then strip the augmented column back out with a `ProjectionExec` so
        // the surrounding plan sees the originally-requested schema. This is
        // the fix for `SELECT COUNT(*)` / `SELECT non_pk_col` after a fast-path
        // DELETE/UPDATE: without the PK in the cold batch the filter has
        // nothing to key on and stale rows leak into aggregates / non-PK
        // projections.
        //
        // Limit pushdown is also dropped when augmenting because the limit
        // applies to post-filter rows; passing a limit to the cold scan would
        // truncate before tombstone/override removal and under-count survivors.
        let (cold_projection_owned, augmented, effective_limit) = match projection {
            Some(p) if !p.contains(&pk_idx_in_schema) => {
                let mut p2 = p.clone();
                p2.push(pk_idx_in_schema);
                (Some(p2), true, None)
            }
            Some(p) => (Some(p.clone()), false, limit),
            None => (None, false, limit),
        };
        let cold_projection_ref = cold_projection_owned.as_ref();
        let cold_plan = self
            .cold
            .scan(state, cold_projection_ref, filters, effective_limit)
            .await?;

        // Wrap with the tombstone row-filter (no-op when no tombstones).
        let mut filtered: Arc<dyn ExecutionPlan> = if tombs.is_empty() {
            cold_plan
        } else {
            Arc::new(TombstoneFilterExec::new(
                cold_plan,
                pk_col.clone(),
                pk_dt.clone(),
                Arc::new(tombs),
            ))
        };

        // Wrap with the UPDATE override overlay: suppress overridden cold rows
        // and append the post-SET rows. The override rows are reprojected to
        // the (possibly PK-augmented) cold scan schema inside the exec.
        if !updates.is_empty() {
            filtered = Arc::new(UpdateOverlayExec::new(
                filtered,
                pk_col.clone(),
                pk_dt,
                Arc::new(updates),
            ));
        }

        if !augmented {
            return Ok(filtered);
        }

        // Strip the appended PK column so the outer plan sees the schema it
        // originally asked for. The original projection lives in the first
        // `p.len()` output columns of `filtered`.
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
        let filtered_schema = filtered.schema();
        let original_len = cold_projection_owned
            .as_ref()
            .map(|p| p.len() - 1)
            .unwrap_or(0);
        let exprs: Vec<ProjectionExpr> = (0..original_len)
            .map(|i| {
                let field = filtered_schema.field(i);
                ProjectionExpr {
                    expr: Arc::new(Column::new(field.name(), i)),
                    alias: field.name().to_owned(),
                }
            })
            .collect();
        let projected = ProjectionExec::try_new(exprs, filtered)?;
        Ok(Arc::new(projected))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        use datafusion::logical_expr::TableProviderFilterPushDown as Pd;
        // When tombstones / UPDATE overrides are present, offer the cold
        // provider's pushdown ability but cap it at `Inexact` (never `Exact`).
        //
        // Pushing the predicate into the cold scan only PRE-FILTERS the cold
        // rows; it is still correct because:
        //   * `TombstoneFilterExec` wraps the cold scan INSIDE this provider's
        //     `scan()` output, so deleted rows are suppressed regardless of
        //     pushdown — a tombstoned row that matches the predicate cannot
        //     leak through.
        //   * `UpdateOverlayExec` appends every override's post-SET row
        //     UNCONDITIONALLY (see its `execute`: the tail is built from
        //     `updates.values()`, not gated on the cold stream), so a row
        //     updated to NEWLY match the predicate is never dropped by the
        //     cold pre-filter.
        //   * `Inexact` makes DataFusion keep a `FilterExec` ABOVE this
        //     provider, which re-applies every predicate authoritatively after
        //     the tombstone/overlay merge.
        // The previous `Unsupported` was over-conservative: it handed the cold
        // (Vortex/Parquet) scan zero filters, so a selective `WHERE id < 100`
        // on a table with any DELETE/UPDATE overlay became a full-column scan.
        let registry = self.engine.memtable_registry();
        // Pushdown gate is a presence check on the auto-commit overlay; no
        // transaction watermark applies here (see `scan`).
        let tombs = snapshot_tombstones(registry.as_ref(), &self.project, &self.table, None);
        let updates = snapshot_updates(registry.as_ref(), &self.project, &self.table, None);
        if !tombs.is_empty() || !updates.is_empty() {
            let cold = self.cold.supports_filters_pushdown(filters)?;
            return Ok(cold
                .into_iter()
                .map(|p| match p {
                    Pd::Unsupported => Pd::Unsupported,
                    _ => Pd::Inexact,
                })
                .collect());
        }
        self.cold.supports_filters_pushdown(filters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    #[test]
    fn array_value_to_row_key_int64_matches_pk_writer() {
        // Mirror `dml_mutate::pk_scalar_to_row_key(Int64(7), Int64)`.
        let arr = Int64Array::from(vec![7i64]);
        let key = array_value_to_row_key(&arr, 0, &DataType::Int64).unwrap();
        let expected = RowKey::builder().append_i64(7).finish();
        assert_eq!(key.as_bytes(), expected.as_bytes());
    }

    #[test]
    fn array_value_to_row_key_utf8_matches_pk_writer() {
        let arr = StringArray::from(vec!["hello"]);
        let key = array_value_to_row_key(&arr, 0, &DataType::Utf8).unwrap();
        let expected = RowKey::builder().append_str("hello").finish();
        assert_eq!(key.as_bytes(), expected.as_bytes());
    }

    #[test]
    fn array_value_to_row_key_null_returns_none() {
        let arr = Int64Array::from(vec![None]);
        assert!(array_value_to_row_key(&arr, 0, &DataType::Int64).is_none());
    }

    #[test]
    fn filter_batch_drops_tombstoned_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let tombstones: HashSet<Vec<u8>> = [2i64, 4]
            .iter()
            .map(|v| {
                RowKey::builder()
                    .append_i64(*v)
                    .finish()
                    .as_bytes()
                    .to_vec()
            })
            .collect();
        let out = filter_batch(&batch, "id", &DataType::Int64, &tombstones).unwrap();
        assert_eq!(out.num_rows(), 3);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 3);
        assert_eq!(col.value(2), 5);
    }

    #[test]
    fn apply_tombstone_filter_to_batches_drops_across_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
        )
        .unwrap();
        let tombstones: HashSet<Vec<u8>> = [2i64, 5]
            .iter()
            .map(|v| {
                RowKey::builder()
                    .append_i64(*v)
                    .finish()
                    .as_bytes()
                    .to_vec()
            })
            .collect();
        let out =
            apply_tombstone_filter_to_batches(vec![b1, b2], &tombstones, "id", &DataType::Int64)
                .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn apply_tombstone_filter_to_batches_empty_snapshot_is_passthrough() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let b =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let empty: HashSet<Vec<u8>> = HashSet::new();
        let out =
            apply_tombstone_filter_to_batches(vec![b.clone()], &empty, "id", &DataType::Int64)
                .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[test]
    fn filter_batch_passes_through_when_pk_column_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let arr = StringArray::from(vec!["a", "b"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let tombstones: HashSet<Vec<u8>> = [1i64]
            .iter()
            .map(|v| {
                RowKey::builder()
                    .append_i64(*v)
                    .finish()
                    .as_bytes()
                    .to_vec()
            })
            .collect();
        let out = filter_batch(&batch, "id", &DataType::Int64, &tombstones).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn normalize_casts_binary_to_largebinary_for_jsonb() {
        use arrow_array::{BinaryArray, LargeBinaryArray};
        // Source batch: JSONB column decoded as plain `Binary` (the Vortex
        // cold-decode shape). Target schema: catalog `LargeBinary` (JSONB).
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Binary, true),
        ]));
        let src = RecordBatch::try_new(
            src_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(BinaryArray::from(vec![
                    Some(b"{}".as_ref()),
                    Some(b"[]".as_ref()),
                ])),
            ],
        )
        .unwrap();
        let target = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::LargeBinary, true),
        ]);
        let out = normalize_batch_to_schema(src, &target);
        assert_eq!(out.schema().field(1).data_type(), &DataType::LargeBinary);
        let col = out
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("payload normalized to LargeBinary");
        assert_eq!(col.value(0), b"{}");
        assert_eq!(col.value(1), b"[]");
    }

    #[test]
    fn normalize_makes_mixed_binary_batches_concat_compatible() {
        use arrow_array::{BinaryArray, LargeBinaryArray};
        // A `Binary` cold batch and a `LargeBinary` override row — exactly the
        // pre-fix mismatch. After normalizing both to the catalog
        // `LargeBinary`, arrow concat must succeed.
        let target = Schema::new(vec![Field::new("payload", DataType::LargeBinary, true)]);
        let cold = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Binary,
                true,
            )])),
            vec![Arc::new(BinaryArray::from(vec![Some(b"cold".as_ref())]))],
        )
        .unwrap();
        let overlay = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::LargeBinary,
                true,
            )])),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(
                b"overlay".as_ref(),
            )]))],
        )
        .unwrap();
        let target_ref = Arc::new(target);
        let n_cold = normalize_batch_to_schema(cold, &target_ref);
        let n_overlay = normalize_batch_to_schema(overlay, &target_ref);
        let merged =
            arrow_select::concat::concat_batches(&target_ref, &[n_cold, n_overlay]).unwrap();
        assert_eq!(merged.num_rows(), 2);
        assert_eq!(merged.schema().field(0).data_type(), &DataType::LargeBinary);
    }

    #[test]
    fn normalize_is_noop_when_types_match() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
            .unwrap();
        let out = normalize_batch_to_schema(b, schema.as_ref());
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn normalize_leaves_unrelated_type_mismatch_untouched() {
        // Int64 source vs Int32 target is NOT in the str/bin family — must be
        // left as-is so a genuine bug still surfaces downstream.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let target = Schema::new(vec![Field::new("v", DataType::Int32, false)]);
        let out = normalize_batch_to_schema(b, &target);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
    }
}
