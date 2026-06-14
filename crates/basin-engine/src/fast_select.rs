//! Point-query fast path.
//!
//! When the SQL is a "simple SELECT" — a flat projection from a single table
//! with at most one equality predicate against a column literal and an
//! optional LIMIT — we bypass DataFusion entirely and read directly through
//! `basin_storage::Storage` (or the shard's [`ProjectHandle`] when one is
//! configured). Skipping the DataFusion plan saves the per-query planning +
//! arrow-version conversion cost that dominates point-query latency on this
//! PoC.
//!
//! Anything outside the pattern (JOINs, GROUP BY, ORDER BY, expressions in
//! the projection, OR'd predicates, etc.) is left to the existing
//! DataFusion-based [`exec_select`](crate::executor::exec_select) path.
//!
//! The recogniser is conservative: when in doubt, return `None` and let the
//! caller fall through. The cost of a missed fast path is the planner work
//! we already pay today; the cost of an over-eager match is a wrong answer.
//!
//! [`ProjectHandle`]: basin_shard::ProjectHandle

use crate::pg_ast::{ObjectNamePartExt, OrderByExt, QueryClauseExt};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use basin_catalog::TableMetadata;
use basin_common::{BasinError, PartitionKey, Result, TableName};
use basin_storage::{
    bloom_from_bytes, evaluate_compound_for_pruning, CompoundPredicate, Predicate, PruneOutcome,
    ReadOptions, ScalarValue,
};
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    ObjectName, Query, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, Value,
};

use crate::{ExecResult, ProjectSession};

// ── Phase 5.14.C3: memtable probe helpers ────────────────────────────────────

/// Decode one Arrow IPC stream `bytes` back into a `RecordBatch`.
fn decode_ipc_batch(bytes: &[u8]) -> Option<RecordBatch> {
    use arrow::ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).ok()?;
    reader.next()?.ok()
}

/// Test whether a decoded `RecordBatch` (single row) satisfies all the
/// `Predicate`s in `predicates`. Returns `true` when every predicate matches.
///
/// Only the types representable as `ScalarValue` (Int64, Utf8, Boolean) are
/// handled; rows with columns of other types are treated as non-matching
/// (falling through to the cold-tier path, which is safe but conservative).
fn batch_matches_predicates(batch: &RecordBatch, predicates: &[Predicate]) -> bool {
    for pred in predicates {
        // StartsWith doesn't have an associated ScalarValue, so handle it
        // separately before the generic (col, scalar, op) match.
        if let Predicate::StartsWith {
            column,
            prefix,
            case_insensitive,
        } = pred
        {
            let Ok(col_idx) = batch.schema().index_of(column) else {
                return false;
            };
            let col = batch.column(col_idx);
            if col.is_null(0) {
                return false;
            }
            // Match by VALUE across every string-encoded column variant
            // (Utf8 / LargeUtf8 / Utf8View). A strict `StringArray` downcast
            // would silently reject LargeUtf8 / Utf8View overlays — the same
            // class of hole the numeric arms below close.
            use arrow_array::cast::AsArray;
            use arrow_schema::DataType as Dt;
            let v: &str = match col.data_type() {
                Dt::Utf8 => col.as_string::<i32>().value(0),
                Dt::LargeUtf8 => col.as_string::<i64>().value(0),
                Dt::Utf8View => col.as_string_view().value(0),
                _ => return false,
            };
            let ok = if *case_insensitive {
                v.to_ascii_lowercase()
                    .starts_with(&prefix.to_ascii_lowercase())
            } else {
                v.starts_with(prefix.as_str())
            };
            if !ok {
                return false;
            }
            continue;
        }
        // Sorted-IN membership: single-row batch, binary-search the key list.
        if let Predicate::InInt64(column, keys) = pred {
            let Ok(col_idx) = batch.schema().index_of(column) else {
                return false;
            };
            let col = batch.column(col_idx);
            if col.is_null(0) {
                return false;
            }
            let v = match col.data_type() {
                arrow_schema::DataType::Int64 => col
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .map(|a| a.value(0)),
                arrow_schema::DataType::Int32 => col
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .map(|a| i64::from(a.value(0))),
                arrow_schema::DataType::Int16 => col
                    .as_any()
                    .downcast_ref::<arrow_array::Int16Array>()
                    .map(|a| i64::from(a.value(0))),
                _ => None,
            };
            match v {
                Some(v) if keys.binary_search(&v).is_ok() => continue,
                _ => return false,
            }
        }
        let (col_name, expected, op) = match pred {
            Predicate::Eq(c, v) => (c, v, "eq"),
            Predicate::Gt(c, v) => (c, v, "gt"),
            Predicate::Lt(c, v) => (c, v, "lt"),
            // Handled above.
            Predicate::StartsWith { .. } | Predicate::InInt64(..) => unreachable!(),
        };
        let Ok(col_idx) = batch.schema().index_of(col_name) else {
            return false;
        };
        let col = batch.column(col_idx);
        if col.is_null(0) {
            return false;
        }
        let matches = match expected {
            ScalarValue::Int64(expected_v) => {
                // Coerce numerically: an `Int64` scalar must compare by VALUE
                // against any narrower-or-equal integer column (Int16/Int32/
                // Int64) and the unsigned UInt32 column.  Widen the array's
                // element to i64 and compare there.  A strict
                // `downcast::<Int64Array>` here was the root of the Int32-PK
                // bug: overlay entries decoded as Int32 arrays failed the
                // re-validation in `probe_memtable`, making the hot UPDATE
                // overlay invisible to the point-SELECT direct-get hit.
                use arrow_array::cast::AsArray;
                use arrow_array::types::{Int16Type, Int32Type, Int64Type, UInt32Type};
                use arrow_schema::DataType as Dt;
                let widened: Option<i64> = match col.data_type() {
                    Dt::Int64 => Some(col.as_primitive::<Int64Type>().value(0)),
                    Dt::Int32 => Some(col.as_primitive::<Int32Type>().value(0) as i64),
                    Dt::Int16 => Some(col.as_primitive::<Int16Type>().value(0) as i64),
                    Dt::UInt32 => Some(col.as_primitive::<UInt32Type>().value(0) as i64),
                    _ => None,
                };
                match widened {
                    Some(v) => match op {
                        "eq" => v == *expected_v,
                        "gt" => v > *expected_v,
                        "lt" => v < *expected_v,
                        _ => false,
                    },
                    None => false,
                }
            }
            ScalarValue::Utf8(expected_s) => {
                // Compare by VALUE against any string-encoded column variant:
                // Utf8 (i32 offsets), LargeUtf8 (i64 offsets), Utf8View.  A
                // strict `downcast::<StringArray>` would have made overlays on
                // LargeUtf8 / Utf8View columns invisible — the same class of
                // bug as the Int32 hole above.
                use arrow_array::cast::AsArray;
                use arrow_schema::DataType as Dt;
                let v: Option<&str> = match col.data_type() {
                    Dt::Utf8 => Some(col.as_string::<i32>().value(0)),
                    Dt::LargeUtf8 => Some(col.as_string::<i64>().value(0)),
                    Dt::Utf8View => Some(col.as_string_view().value(0)),
                    _ => None,
                };
                match v {
                    Some(v) => match op {
                        "eq" => v == expected_s.as_str(),
                        "gt" => v > expected_s.as_str(),
                        "lt" => v < expected_s.as_str(),
                        _ => false,
                    },
                    None => false,
                }
            }
            ScalarValue::Boolean(expected_b) => {
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                    let v = arr.value(0);
                    match op {
                        "eq" => v == *expected_b,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            ScalarValue::UInt64(expected_v) => {
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
                    let v = arr.value(0);
                    match op {
                        "eq" => v == *expected_v,
                        "gt" => v > *expected_v,
                        "lt" => v < *expected_v,
                        _ => false,
                    }
                } else {
                    false
                }
            }
            ScalarValue::Float64(expected_v) => {
                // Coerce Float32 columns by widening the element to f64, the
                // same widening `basin_storage::predicate::evaluate` performs
                // for a Float64 scalar vs a Float32 (FLOAT4) column.
                use arrow_array::cast::AsArray;
                use arrow_array::types::{Float32Type, Float64Type};
                use arrow_schema::DataType as Dt;
                let widened: Option<f64> = match col.data_type() {
                    Dt::Float64 => Some(col.as_primitive::<Float64Type>().value(0)),
                    Dt::Float32 => Some(col.as_primitive::<Float32Type>().value(0) as f64),
                    _ => None,
                };
                match widened {
                    Some(v) => match op {
                        "eq" => (v - *expected_v).abs() < f64::EPSILON,
                        "gt" => v > *expected_v,
                        "lt" => v < *expected_v,
                        _ => false,
                    },
                    None => false,
                }
            }
        };
        if !matches {
            return false;
        }
    }
    true
}

/// Probe the process-wide `MemTableRegistry` for rows from `(project, table)`
/// that satisfy `predicates`.
///
/// Returns `Some((matching_rows, has_tombstone_match))` when:
/// * `matching_rows` contains decoded `RecordBatch`es from the hot tier that
///   pass all predicates and are live rows (not tombstones).
/// * `has_tombstone_match` is `true` when at least one tombstone entry also
///   passes the predicates — meaning those keys were deleted and cold-tier rows
///   for those PKs must be suppressed.
///
/// Returns `None` when there are no memtable entries for this table at all
/// (avoids any overhead when the hot tier is cold/empty for the table).
///
/// This is the Phase 5.14.C3 point-lookup probe: `execute_simple_select` calls
/// this for Eq-predicate queries before (or instead of) reading the cold tier.
///
/// Fast path (single PK Eq predicate). When `predicates` is exactly one
/// `Eq(pk_col, lit)` against the table's primary-key column and the literal
/// encodes to a `RowKey`, a single `BTreeMap::get(&pk_key)` resolves the lookup
/// in O(log n) under a short read-lock — bypassing the O(n) BTreeMap clone
/// that `snapshot()` performs.  This is correct *only* for entries keyed by
/// the encoded PK (`MemRowValue::Update` and `MemRowValue::Tombstone`, both
/// written by the hot-tier UPDATE / DELETE fast paths in `dml_mutate.rs`).
///
/// `MemRowValue::Row` entries written by `htap_promote_to_registry` are keyed
/// by a monotonic counter (see [`MemRowValue::Update`] doc comment) and would
/// be missed by a PK-keyed `get`.  When the PK-keyed `get` returns `None` we
/// therefore fall through to the snapshot-walk path so any counter-keyed
/// HTAP-cached row whose PK happens to match the predicate is still found.
///
/// Lock contention. The snapshot path holds the read-lock for the entire
/// BTreeMap clone (cost grows with memtable size). The direct-get path holds
/// it only for the O(log n) lookup. For workloads where the memtable is hot
/// with PK-keyed UPDATE/DELETE overlays (read+write mix), this cuts the
/// critical-section length proportionally to the memtable population.
/// `hot_watermark`: when `Some(w)`, this is a transaction-pinned read and any
/// registry entry whose MVCC `seq > w` was written AFTER the transaction pinned
/// its read-view (by a concurrent committer, since this table is untouched by
/// THIS tx — the gate guarantees that). Such entries MUST be invisible to the
/// pinned read, so both the direct-get and the snapshot-walk use the
/// seq-carrying memtable accessors (`get_with_seq` / `snapshot_with_seq`) and
/// skip entries past the watermark. `None` = auto-commit / no pin: today's
/// behaviour (every committed overlay entry is visible).
fn probe_memtable(
    sess: &ProjectSession,
    table: &TableName,
    predicates: &[Predicate],
    meta: &TableMetadata,
    hot_watermark: Option<u64>,
) -> Option<(Vec<RecordBatch>, bool)> {
    let registry = sess.engine.memtable_registry();
    let entry = registry.get(&sess.project, table)?;

    // ── PK direct-get fast path ──────────────────────────────────────────────
    //
    // Only fires for the canonical point-lookup shape:
    //   * exactly one predicate, an `Eq(col, lit)`;
    //   * `col` matches the table's single PK column;
    //   * `lit` encodes to a `RowKey` via the same encoding the cold-tier
    //     cluster-sort uses (i.e. supported PK types: Int64/Int32/Int16/
    //     UInt64/Utf8/Boolean).
    //
    // When all three hold we attempt a direct `get(&pk_row_key)`.
    //   * `Some(Update { .. })` — a PK-keyed override (hot UPDATE).  Decode &
    //     return it; this row supersedes any counter-keyed `Row` that may also
    //     exist for the same logical PK (merge semantics: Update wins).
    //   * `Some(Tombstone)` — return `(vec![], has_tombstone=true)` so the
    //     caller's overlay logic suppresses the cold-tier row.
    //   * `Some(Row { .. })` — a PK-keyed Row (test helpers; not the
    //     production INSERT path which is counter-keyed).  Treat as a hit.
    //   * `None` — fall through to the snapshot path: a counter-keyed
    //     HTAP-cached row may match the predicate via `batch_matches_predicates`.
    // Set when the canonical PK-Eq direct-get ran and MISSED (no entry / no
    // version at the watermark). Lets the auto-commit fallback below skip the
    // O(n) walk entirely when the O(1) counters prove nothing else can match.
    let mut pk_eq_direct_get_missed = false;
    if predicates.len() == 1 && meta.pk_columns.len() == 1 {
        if let Predicate::Eq(col, val) = &predicates[0] {
            if col == &meta.pk_columns[0] {
                // Encode the literal to a RowKey using the SAME helper the
                // hot-tier UPDATE / DELETE fast paths use (`dml_mutate::
                // pk_scalar_to_row_key`).  Identical encoding is load-bearing:
                // an Update/Tombstone written by those paths will only be
                // found here if our `RowKey` matches byte-for-byte.
                if let Ok(pk_idx) = meta.schema.index_of(col) {
                    let pk_dt = meta.schema.field(pk_idx).data_type().clone();
                    if let Some(pk_key) = crate::dml_mutate::pk_scalar_to_row_key(val, &pk_dt) {
                        // Pinned read: fetch the entry's `seq` so a post-pin
                        // write (seq > watermark) by a concurrent session is
                        // treated as absent (fall through to the cold tier,
                        // which is the pinned historical view). Auto-commit
                        // (`hot_watermark == None`) uses the plain `get` and
                        // never filters.
                        let probed: Option<basin_hottier::MemRowValue> = match hot_watermark {
                            // S4 MVCC chains: `get_with_seq(key, Some(w))` returns
                            // the newest version at or before the watermark — the
                            // historical image this pinned read is entitled to,
                            // even if the key was overwritten after the pin. `None`
                            // → no version <= w (falls through to cold).
                            Some(w) => entry.memtable.get_with_seq(&pk_key, Some(w)),
                            None => entry.memtable.get(&pk_key),
                        };
                        match probed {
                            Some(basin_hottier::MemRowValue::Update { bytes, .. })
                            | Some(basin_hottier::MemRowValue::Row { bytes, .. }) => {
                                // Decode and re-validate the predicate against
                                // the decoded row (cheap; one row).  An Update
                                // SET col = ? doesn't change the PK column, but
                                // re-checking is defence-in-depth in case the
                                // entry's PK column value isn't what the
                                // predicate expects.
                                if let Some(batch) = decode_ipc_batch(&bytes) {
                                    if batch_matches_predicates(&batch, predicates) {
                                        return Some((vec![batch], false));
                                    }
                                    // Predicate mismatch on a PK-keyed entry is
                                    // a contradiction (the key encodes the PK
                                    // value).  Fall through to snapshot in case
                                    // of an encoding edge we missed.
                                }
                            }
                            Some(basin_hottier::MemRowValue::Tombstone) => {
                                return Some((Vec::new(), true));
                            }
                            None => {
                                // PK not present in memtable under the PK-keyed
                                // half (or a post-pin write filtered above). A
                                // counter-keyed Row (HTAP cache) may still match
                                // — fall through to snapshot.
                                pk_eq_direct_get_missed = true;
                            }
                        }
                    }
                }
            }
        }
    }

    // ── Snapshot fallback ────────────────────────────────────────────────────
    // The slow but always-correct path: clone the memtable entries under a
    // read-lock, then filter via `batch_matches_predicates`.  Required for
    // predicate shapes the direct-get can't handle (non-PK Eq, multi-
    // predicate, range), and as a safety net for counter-keyed HTAP-cached
    // INSERT rows.
    //
    // S4 age-based residency, auto-commit (`hot_watermark == None`):
    //   * Skip the walk entirely when the direct-get above already answered
    //     the sole-PK-Eq shape with a miss AND the O(1) counters prove no
    //     counter-keyed Row and no Update override is resident — no other
    //     entry kind can satisfy a PK equality (other keys' tombstones are
    //     irrelevant to this PK, and clean Rows are excluded below anyway).
    //   * Otherwise walk only the DIRTY entries (`dirty_snapshot`). CLEAN
    //     entries are flushed-and-retained copies of cold rows; surfacing
    //     them here was an UNDER-return hazard: a non-PK Eq that matched a
    //     retained clean row returned ONLY the hot matches and skipped the
    //     cold read, hiding every other cold row that matched.
    //
    // Pinned read (`Some(w)`): keep the full seq-carrying snapshot, clean
    // entries INCLUDED — the pinned cold snapshot may predate the flush that
    // made an entry clean, so the retained copy can be the only visible
    // source of the version this transaction is entitled to. (`Some(w)`
    // yields, per key, the newest version at or before the watermark.)
    // ── Completeness guard (auto-commit) ────────────────────────────────────
    // The CALLER treats a non-empty `live_matches` as the WHOLE answer (it
    // returns the hot rows and skips the cold read entirely). That is only
    // sound when the predicate set PINS the table's single PK with an Eq atom:
    // at most one row table-wide can match, and the memtable's version of that
    // row supersedes the cold image. For every other shape — a non-PK Eq, in
    // particular — the hot matches are a SUBSET of the answer, and returning
    // them here HIDES every cold row that also matches (measured: a fast-path
    // `UPDATE … WHERE id = 41` left a dirty override matching `bucket = 3`,
    // and the next `SELECT … WHERE bucket = 3 LIMIT 100` returned 2 rows
    // instead of 100). Those shapes fall through to the cold read, whose
    // overlay merge surfaces dirty Update rows and suppresses tombstones —
    // complete AND fresh. This is the dirty-entry twin of the clean-row
    // under-return fix described below; production INSERT residency is
    // written CLEAN (`insert_clean`), so no committed row is lost by skipping
    // the walk.
    //
    // Pinned reads (`Some(w)`) keep the historical walk unchanged: the pinned
    // cold snapshot may predate the flush that retained an entry, so the
    // retained copy can be the only visible source of an entitled version
    // (pre-existing behavior, unchanged here).
    let pk_pinning_eq = meta.pk_columns.len() == 1
        && predicates
            .iter()
            .any(|p| matches!(p, Predicate::Eq(c, _) if c == &meta.pk_columns[0]));
    let snapshot: Vec<(basin_hottier::RowKey, basin_hottier::MemRowValue)> = match hot_watermark {
        None => {
            if !pk_pinning_eq {
                return None;
            }
            if pk_eq_direct_get_missed
                && entry.memtable.counter_key_rows() == 0
                && entry.memtable.update_count() == 0
            {
                return None;
            }
            entry
                .memtable
                .dirty_snapshot()
                .into_iter()
                .map(|(k, v, _seq)| (k, v))
                .collect()
        }
        watermark @ Some(_) => entry.memtable.snapshot_with_seq(watermark),
    };
    if snapshot.is_empty() {
        return None;
    }

    let mut live_matches: Vec<RecordBatch> = Vec::new();
    let mut has_tombstone = false;

    for (_key, value) in snapshot {
        match value {
            basin_hottier::MemRowValue::Row { bytes, .. }
            | basin_hottier::MemRowValue::Update { bytes, .. } => {
                if let Some(batch) = decode_ipc_batch(&bytes) {
                    if predicates.is_empty() || batch_matches_predicates(&batch, predicates) {
                        live_matches.push(batch);
                    }
                }
            }
            basin_hottier::MemRowValue::Tombstone => {
                // For tombstones we don't have the row data to filter by
                // predicates; conservatively mark has_tombstone=true only when
                // there are no equality predicates (full-table tombstone) OR
                // when predicates are present but we cannot tell without the
                // original row data. In practice, DELETE writes a tombstone
                // with the original row's key, so we mark this conservatively.
                // A missed suppression (false negative) is safe — it means the
                // cold tier row still shows; a false positive would hide a live
                // row, which we avoid by only setting this flag, not filtering.
                //
                // For the C3 acceptance gate: point-query latency for recently
                // INSERTed rows — tombstones are only relevant for DELETE.
                // We set the flag so callers know to be cautious.
                has_tombstone = true;
            }
        }
    }

    Some((live_matches, has_tombstone))
}

/// A single item in the user-requested SELECT projection.
///
/// Plain column references keep their fast-path identity; aliased scalar
/// arithmetic expressions are carried as a parsed AST fragment so the
/// execution path can evaluate them via Arrow compute kernels without
/// re-parsing SQL or touching DataFusion.
#[derive(Debug, Clone)]
pub(crate) enum ProjectionItem {
    /// A bare column reference: `SELECT col`.
    Column(String),
    /// An aliased scalar arithmetic expression: `SELECT expr AS alias`.
    ///
    /// `sql_expr` is the validated subtree (only `BinaryOp` with `+`, `-`,
    /// `*`, `/` over `Identifier`/`Number` leaves). `alias` is the output
    /// column name. `source_cols` lists every table column referenced by
    /// `sql_expr` so the storage layer can be asked to decode them.
    Computed {
        sql_expr: Expr,
        alias: String,
        source_cols: Vec<String>,
    },
}

/// Aggregate function in a `SELECT` projection recognized by the fast path.
/// Only the patterns relevant to the benchmark battery are supported; anything
/// richer falls through to DataFusion.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AggregateFn {
    /// `COUNT(*)`
    CountStar,
    /// `SUM(<col>)` where `<col>` is a bare identifier.
    Sum(String),
    /// `MIN(<col>)`
    Min(String),
    /// `MAX(<col>)`
    Max(String),
}

/// Eligible-shape descriptor for the deep top-K late-materialization branch.
///
/// Recognised shape (set by `match_query` only when every gate passes):
///
///   `SELECT <cols> FROM t ORDER BY <sort_col> [ASC|DESC] [, <pk> [ASC]]
///    [WHERE …handled by existing pruning…] LIMIT k`
///
/// where `<pk>` is the table's single primary-key column (so a winning row's
/// identity is exactly its PK), `k` is under the `BASIN_TOPK_LATE_MAX_K` cap,
/// and there are no joins / aggregates / GROUP BY / OFFSET / computed
/// projection. The optional `, <pk>` is the deterministic tie-break the deep
/// top-K benchmark uses (`ORDER BY amount DESC, id`); when absent the PK still
/// serves as the row identity for phase 2.
///
/// Execution (`try_topk_late_materialize`): phase 1 decodes ONLY
/// `[sort_col, pk]` (+ any WHERE columns) across the live files, file-skipping
/// any file whose sort-column min/max cannot beat the current k-th bound, and
/// keeps a bounded top-`k` of `(sort_col, pk)` rows. Phase 2 fetches the full
/// wide rows for just those ≤ `k` PKs via an `InInt64(pk, winners)` pushdown,
/// re-sorts by `(sort_col, pk)`, and projects. The wide columns of the
/// (1M-k) losing rows are never decoded — the cost the full-scan path pays.
#[derive(Debug, Clone)]
pub(crate) struct TopKLatePlan {
    /// The leading ORDER BY column (any sortable Arrow type).
    pub sort_col: String,
    /// `true` for ASC (or unspecified), `false` for DESC.
    pub ascending: bool,
    /// The single-PK column used as the row identity for phase-2 point-fetch.
    /// Equal to `sort_col` when the ORDER BY is on the PK itself.
    pub pk_col: String,
    /// `true` when the ORDER BY carries an explicit `, <pk> [ASC]` tie-break
    /// (so the phase-2 re-sort is a two-key lexical sort); `false` when the
    /// ORDER BY is the single `sort_col` (phase-2 re-sort uses `sort_col`
    /// alone, with the PK as the implicit stable secondary via the row order).
    pub pk_tiebreak: bool,
}

/// Recognised "simple SELECT" plan. When `predicates` is empty the read is
/// an unfiltered scan; when `limit` is `Some(n)` we truncate the merged
/// batches to `n` rows total.
#[derive(Debug)]
pub(crate) struct SimpleSelectPlan {
    pub table: TableName,
    /// `None` means project every column (`SELECT *`); `Some(items)` is the
    /// user-requested output list in SELECT order. Items may be plain column
    /// references or aliased computed expressions. Set to `None` for aggregate
    /// queries (which have no row-returning projection).
    pub projection: Option<Vec<ProjectionItem>>,
    /// The superset of table columns that must be decoded from storage so
    /// that every `ProjectionItem` (and any filter references) can be
    /// satisfied. `None` means read all columns (wildcard or aggregate path).
    /// When all projection items are plain `Column` references this equals
    /// the column names from `projection`; when computed items are present it
    /// is the union of all referenced source columns plus filter columns.
    pub read_cols: Option<Vec<String>>,
    /// When `Some`, the query is an aggregate (no ORDER BY, no LIMIT).
    /// When `None`, it is a row-returning SELECT.
    pub aggregates: Option<Vec<AggregateFn>>,
    /// Zero or more conjunctive predicates. Up to three atoms are accepted
    /// (single `col op lit`, BETWEEN, or `col op lit AND col op lit [AND ...]`).
    pub predicates: Vec<Predicate>,
    /// `IS NULL` checks that cannot be represented as a [`Predicate`] (the
    /// storage-layer enum has no `IsNull` variant). Each entry is a column
    /// name; the execution path applies them as a post-read Arrow filter.
    /// Catalog-level pruning uses `CompoundPredicate::IsNull` to skip files
    /// where `null_count == 0` for the column.
    pub is_null_cols: Vec<String>,
    /// `col IN (lit, lit, …)` predicates carried separately from
    /// [`predicates`] because `Predicate` has no `In` variant and
    /// `ReadOptions.filters` only accepts `Vec<Predicate>`.  Applied as a
    /// post-read Arrow filter via [`CompoundPredicate::In`].  When the column
    /// is the sole primary-key column the execution path also feeds these
    /// values into the bloom+zone-map IN-list probe to prune cold-tier files.
    pub in_list_preds: Vec<(String, Vec<ScalarValue>)>,
    pub limit: Option<usize>,
    /// `OFFSET n` row skip for the ORDER BY + LIMIT pagination shape. Only ever
    /// `Some` together with `order_by.is_some()` and `limit.is_some()` (the
    /// `SELECT … ORDER BY k LIMIT m OFFSET n` keyset/offset-paging form); the
    /// recogniser rejects OFFSET in every other shape so its non-deterministic
    /// PG semantics never reach the fast path. `execute_simple_select` skips
    /// the first `offset` rows of the global top-`(limit+offset)` before
    /// returning the `limit`-row window — see `apply_order_by_limit`.
    pub offset: Option<usize>,
    /// `Some((column, ascending))` for a single-column ORDER BY recognised by
    /// the fast path. `ascending=true` means ASC (or no direction specified);
    /// `ascending=false` means DESC. When `Some`, `execute_simple_select` sorts
    /// all decoded rows by this column and applies the limit post-sort.
    /// Always `None` for aggregate queries.
    pub order_by: Option<(String, bool)>,
    /// `true` when the WHERE clause is provably 3VL-FALSE (e.g. `x = NULL`
    /// or `x <> NULL`). `execute_simple_select` returns an empty row set
    /// immediately without consulting storage or running aggregates.
    pub always_empty: bool,
    /// `Some` when the statement matches the deep top-K late-materialization
    /// shape (`ORDER BY k [DESC] [, pk] LIMIT n` over a single-PK table, k
    /// under `BASIN_TOPK_LATE_MAX_K`). `execute_simple_select_inner` then
    /// attempts the two-phase narrow-key-scan + PK-fetch path; any runtime
    /// ineligibility (live overlay, un-flushed tail, missing sort-column
    /// stats…) Ok-falls through to the existing decode-everything-then-sort
    /// path. `None` for every other shape and for aggregate queries.
    pub topk_late: Option<TopKLatePlan>,
}

/// Recognise the supported "simple SELECT" shape. Returns `None` if any
/// clause we don't handle is present, in which case the caller should fall
/// back to the DataFusion path.
pub(crate) fn match_simple_select(stmt: &Statement) -> Option<SimpleSelectPlan> {
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return None,
    };
    match_query(query.as_ref())
}

fn match_query(q: &Query) -> Option<SimpleSelectPlan> {
    // `q.locks` (the `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` /
    // `FOR KEY SHARE` row-locking clause) is INTENTIONALLY allowed through: Basin
    // is append-only / optimistic-concurrency, so row locks are advisory and have
    // NO effect on the rows a SELECT returns (the executor's read path and the
    // DataFusion path both ignore them — see `rewrite_for_no_key_update_and_key_share`).
    // Admitting them lets the common `BEGIN; SELECT … FOR UPDATE; UPDATE; COMMIT`
    // OLTP shape take the fast point-read path instead of paying full DataFusion
    // on its single in-tx read. (`q.for_clause` — the unrelated `FOR XML/JSON`
    // result-formatting clause — is still rejected below.)
    if q.with.is_some()
        || !q.ext_limit_by().is_empty()
        || q.fetch.is_some()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
    {
        return None;
    }
    // OFFSET is parsed (not blanket-rejected) so the `ORDER BY k LIMIT m
    // OFFSET n` pagination shape can take the fast path's two-phase top-k.
    // It is admitted ONLY on the ORDER BY + LIMIT form below; on every other
    // shape (no ORDER BY, or no LIMIT) we fall through to DataFusion because
    // OFFSET without a total order has non-deterministic PG semantics the fast
    // path must not silently fix to a particular row set.
    let offset: Option<usize> = match q.ext_offset() {
        None => None,
        Some(off) => match &off.value {
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }) => match s.parse::<i64>() {
                Ok(n) if n >= 0 => Some(n as usize),
                _ => return None,
            },
            _ => return None,
        },
    };

    // Parse the ORDER BY. We recognise two row shapes here:
    //
    //   * single column  `ORDER BY c [ASC|DESC]` — the historical fast-path
    //     shape; `order_by = Some((c, asc))`, `topk_second = None`.
    //   * two columns    `ORDER BY c [ASC|DESC], c2 [ASC]` — the deep top-K
    //     tie-break shape (`ORDER BY amount DESC, id`). `order_by` carries the
    //     LEADING key; `topk_second = Some((c2, true))` carries the secondary
    //     ASC tie-break. This shape is ONLY ever served by the two-phase top-K
    //     branch (whose runtime gate confirms `c2` is the single PK); if that
    //     branch declines, the statement falls through to DataFusion — never to
    //     the single-column `apply_order_by_limit`, which cannot honour the
    //     secondary key.
    //
    // Anything more complex (3+ columns, expressions, a non-ASC second key,
    // NULLS FIRST/LAST, WITH FILL) falls through to DataFusion so NULL ordering
    // and multi-key semantics match it exactly. `None` here = "no ORDER BY".
    let mut topk_second: Option<(String, bool)> = None;
    let order_by: Option<(String, bool)> = match q.order_by.as_ref() {
        None => None,
        Some(ob) => {
            let exprs = ob.ext_exprs();
            if exprs.is_empty() || exprs.len() > 2 {
                return None;
            }
            // Helper: a plain ascending/descending identifier ORDER BY term
            // with no NULLS / WITH FILL modifiers → `(col, ascending)`.
            let parse_term = |e: &sqlparser::ast::OrderByExpr| -> Option<(String, bool)> {
                if e.with_fill.is_some() || e.options.nulls_first.is_some() {
                    return None;
                }
                let col = match &e.expr {
                    Expr::Identifier(id) => id.value.clone(),
                    _ => return None,
                };
                Some((col, e.options.asc.unwrap_or(true)))
            };
            let (lead_col, lead_asc) = parse_term(&exprs[0])?;
            if exprs.len() == 2 {
                // Second key must be a bare ASC identifier (the deterministic
                // PK tie-break PG uses). A DESC second key is rejected (off the
                // fast path) — the runtime gate further requires it to be the
                // single PK column.
                let (sec_col, sec_asc) = parse_term(&exprs[1])?;
                if !sec_asc {
                    return None;
                }
                topk_second = Some((sec_col, sec_asc));
            }
            Some((lead_col, lead_asc))
        }
    };

    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };

    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || !select.connect_by.is_empty()
    {
        return None;
    }

    // GROUP BY: accept only the empty-expression form. `GROUP BY ALL` and
    // any explicit grouping expressions take us off the fast path.
    match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty() => {}
        _ => return None,
    }

    // FROM clause: exactly one bare table, no joins.
    if select.from.len() != 1 {
        return None;
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return None;
    }
    let table = match &from.relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            ..
        } => {
            if alias.is_some()
                || args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
            {
                return None;
            }
            single_part_table(name)?
        }
        _ => return None,
    };

    // Projection: either `*` (unqualified, no ILIKE/EXCLUDE/etc.) or a list
    // of bare column identifiers. Compound names (`t.col`), expressions, and
    // aliases are left to DataFusion so we don't have to teach the fast path
    // about them.
    // WHERE clause: zero or more conjunctive predicates. Each atom is one of
    // `<col> <op> <literal>` where `<op>` is `=`, `>`, `<`, `>=`, `<=`;
    // `<col> BETWEEN <lo> AND <hi>` is also accepted (expands to two atoms).
    // `<col> IS NULL` is accepted (pruned at catalog layer, filtered in-RAM).
    // A bare `AND` of up to three such atoms is accepted. Anything richer —
    // OR, IS NOT NULL, expressions, nested ANDs beyond the cap — falls through
    // to DataFusion.
    let parsed_where: ParsedWhere = match &select.selection {
        None => ParsedWhere {
            predicates: vec![],
            is_null_cols: vec![],
            in_list_preds: vec![],
            always_false: false,
        },
        Some(expr) => parse_where(expr)?,
    };

    // Projection: try aggregate functions first (COUNT/SUM/MIN/MAX), then fall
    // back to the standard column-list or wildcard form. Both paths are
    // mutually exclusive; a mix (e.g. `SELECT COUNT(*), id`) is not supported.
    //
    // Aggregate constraints:
    //  - No ORDER BY (no sensible sort on a single-row result)
    //  - No LIMIT
    // Row-returning constraints:
    //  - ORDER BY requires a paired LIMIT
    if let Some(aggs) = parse_aggregate_projection(&select.projection) {
        // Aggregate query — ORDER BY, LIMIT, and OFFSET are incompatible.
        if order_by.is_some() || q.ext_limit().is_some() || offset.is_some() {
            return None;
        }
        // Minimal read projection for the aggregate: the union of the columns
        // the aggregate functions actually consume (SUM/MIN/MAX args; COUNT(*)
        // needs none) plus every column the WHERE clause references. Decoding
        // ANYTHING ELSE is wasted I/O + CPU — a `COUNT(*) WHERE
        // __promoted$payload$category = '…'` only needs the one small Utf8
        // shadow column, NOT the whole `payload` JSONB blob the `None` (read
        // every column) behaviour used to pull. At 1M rows that is the
        // difference between decoding one narrow column and decoding every
        // column in the file. When the union is empty (e.g. unfiltered
        // `COUNT(*)`) we keep `None` so the reader can pick its own cheapest
        // column to satisfy the row count.
        let agg_read_cols: Option<Vec<String>> = {
            let mut cols: Vec<String> = Vec::new();
            let mut push = |c: &str, cols: &mut Vec<String>| {
                if !cols.iter().any(|x| x == c) {
                    cols.push(c.to_string());
                }
            };
            for a in &aggs {
                match a {
                    AggregateFn::CountStar => {}
                    AggregateFn::Sum(c) | AggregateFn::Min(c) | AggregateFn::Max(c) => {
                        push(c, &mut cols)
                    }
                }
            }
            for p in &parsed_where.predicates {
                push(p.column(), &mut cols);
            }
            for c in &parsed_where.is_null_cols {
                push(c, &mut cols);
            }
            for (c, _) in &parsed_where.in_list_preds {
                push(c, &mut cols);
            }
            if cols.is_empty() {
                None
            } else {
                Some(cols)
            }
        };
        return Some(SimpleSelectPlan {
            table,
            projection: None, // not a row projection
            read_cols: agg_read_cols,
            aggregates: Some(aggs),
            predicates: parsed_where.predicates,
            is_null_cols: parsed_where.is_null_cols,
            in_list_preds: parsed_where.in_list_preds,
            limit: None,
            offset: None,
            order_by: None,
            always_empty: parsed_where.always_false,
            topk_late: None,
        });
    }

    let (projection, read_cols) = parse_projection(&select.projection, &parsed_where)?;

    // LIMIT: literal non-negative integer. `LIMIT ALL`, expressions, and
    // placeholders fall through to DataFusion.
    let limit = match q.ext_limit() {
        None => None,
        Some(Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        })) => match s.parse::<i64>() {
            Ok(n) if n >= 0 => Some(n as usize),
            _ => return None,
        },
        Some(_) => return None,
    };

    // ORDER BY without LIMIT means "sort ALL rows" — let DataFusion handle
    // that; it can parallelise and pipeline the sort better. Only handle
    // ORDER BY when paired with a LIMIT.
    if order_by.is_some() && limit.is_none() {
        return None;
    }

    // OFFSET is admitted ONLY on the `ORDER BY k LIMIT m OFFSET n` shape — a
    // total order makes the skipped window deterministic and the two-phase
    // top-k in `apply_order_by_limit` resolves it cheaply. An OFFSET without
    // ORDER BY (or without LIMIT) returns to DataFusion: its row choice is
    // implementation-defined in PG and the fast path must not pin it.
    if offset.is_some() && (order_by.is_none() || limit.is_none()) {
        return None;
    }

    // A two-column `ORDER BY c, c2` is ONLY served by the deep top-K branch.
    // If we cannot build a top-K candidate for it here (no LIMIT, OFFSET
    // present, k over the cap), bail to DataFusion: the single-column path
    // below would silently drop the secondary tie-break key.
    let topk_late = build_topk_late_candidate(
        order_by.as_ref(),
        topk_second.as_ref(),
        limit,
        offset,
    );
    if topk_second.is_some() && topk_late.is_none() {
        return None;
    }

    Some(SimpleSelectPlan {
        table,
        projection,
        read_cols,
        aggregates: None,
        predicates: parsed_where.predicates,
        is_null_cols: parsed_where.is_null_cols,
        in_list_preds: parsed_where.in_list_preds,
        limit,
        offset,
        order_by,
        always_empty: parsed_where.always_false,
        topk_late,
    })
}

/// Default cap on `k` for the deep top-K late-materialization branch. A query
/// with a huge `LIMIT` would build a `k`-sized winner heap and a `k`-element
/// `InInt64` phase-2 fetch; past several thousand rows the narrow-key-scan win
/// shrinks (phase 2 reopens most files) and the bounded-heap memory grows, so
/// we cap engagement and let larger limits take the full-scan path. The cap is
/// a performance safety valve, not a correctness boundary — the two-phase
/// result is byte-identical to the full scan at any `k` (the differential suite
/// gates this up to `LIMIT 9000`), so the ceiling only governs WHEN the
/// optimisation engages. Overridable via `BASIN_TOPK_LATE_MAX_K` (`0` disables).
const TOPK_LATE_MAX_K_DEFAULT: usize = 10_000;

/// `BASIN_TOPK_LATE_MAX_K` — the inclusive `k` cap for the deep top-K branch.
/// `0` disables the branch entirely (every top-K falls through to the existing
/// path). An unparseable value uses the default.
fn topk_late_max_k() -> usize {
    match std::env::var("BASIN_TOPK_LATE_MAX_K") {
        Ok(v) => v.trim().parse::<usize>().unwrap_or(TOPK_LATE_MAX_K_DEFAULT),
        Err(_) => TOPK_LATE_MAX_K_DEFAULT,
    }
}

/// Build the syntactic top-K candidate from the parsed ORDER BY + LIMIT.
/// Returns `None` (no top-K branch) when: there is no ORDER BY, no LIMIT, an
/// OFFSET is present (the paginated form keeps the existing two-phase
/// `apply_order_by_limit`), `k` is `0` or over the env cap, or the optional
/// second key is anything but a single ascending identifier. The runtime gate
/// in `execute_simple_select_inner` further confirms the identity column
/// (`pk_col` / the optional second key) is the table's single PK.
fn build_topk_late_candidate(
    order_by: Option<&(String, bool)>,
    second: Option<&(String, bool)>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Option<TopKLatePlan> {
    let (sort_col, ascending) = order_by?;
    let k = limit?;
    // Paginated top-K (OFFSET) stays on the existing path; the late-material
    // identity heap is sized for the top window only.
    if offset.is_some() {
        return None;
    }
    let cap = topk_late_max_k();
    if k == 0 || k > cap {
        return None;
    }
    // The phase-2 identity column: the explicit second ORDER BY key when
    // present (the `, id` tie-break), else the leading sort column itself
    // (ORDER BY on the PK directly). The runtime gate confirms it is the PK.
    let (pk_col, pk_tiebreak) = match second {
        Some((c2, _asc)) => (c2.clone(), true),
        None => (sort_col.clone(), false),
    };
    Some(TopKLatePlan {
        sort_col: sort_col.clone(),
        ascending: *ascending,
        pk_col,
        pk_tiebreak,
    })
}

fn single_part_table(name: &ObjectName) -> Option<TableName> {
    if name.0.len() != 1 {
        return None;
    }
    TableName::new(name.0[0].id_val().clone()).ok()
}

/// Parse the SELECT item list into a `(projection, read_cols)` pair.
///
/// Returns `None` to fall through to DataFusion for anything we don't handle.
///
/// `projection` is `None` for a bare `SELECT *`; otherwise it is the
/// user-requested output list in SELECT order. `read_cols` is the union of
/// all table columns that must be fetched from storage: plain column refs +
/// every computed expression's source columns + every filter-referenced
/// column (from `where_info`). `read_cols` is `None` for the wildcard path
/// (read every column).
fn parse_projection(
    items: &[SelectItem],
    where_info: &ParsedWhere,
) -> Option<(Option<Vec<ProjectionItem>>, Option<Vec<String>>)> {
    if items.len() == 1 {
        if let SelectItem::Wildcard(opts) = &items[0] {
            if opts.opt_ilike.is_none()
                && opts.opt_exclude.is_none()
                && opts.opt_except.is_none()
                && opts.opt_replace.is_none()
                && opts.opt_rename.is_none()
            {
                return Some((None, None));
            }
            return None;
        }
    }

    let mut proj_items: Vec<ProjectionItem> = Vec::with_capacity(items.len());
    let mut has_computed = false;

    for item in items {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                proj_items.push(ProjectionItem::Column(ident.value.clone()));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                // Only accept aliased scalar arithmetic: BinaryOp trees over
                // Identifier leaves (columns) and Number literals.
                let mut src_cols: Vec<String> = Vec::new();
                if !validate_arithmetic_expr(expr, &mut src_cols) {
                    return None;
                }
                src_cols.sort_unstable();
                src_cols.dedup();
                has_computed = true;
                proj_items.push(ProjectionItem::Computed {
                    sql_expr: expr.clone(),
                    alias: alias.value.clone(),
                    source_cols: src_cols,
                });
            }
            _ => return None,
        }
    }

    // Build read_cols = union of (all column refs) ∪ (filter cols).
    // Only materialise when there are computed items; for plain column
    // lists the set equals the projection list so we build it either way.
    let read_cols = {
        let mut set: Vec<String> = Vec::new();
        for item in &proj_items {
            match item {
                ProjectionItem::Column(c) => set.push(c.clone()),
                ProjectionItem::Computed { source_cols, .. } => {
                    set.extend(source_cols.iter().cloned());
                }
            }
        }
        // Add columns referenced by WHERE predicates / IS NULL / IN so
        // Vortex doesn't have to decode them separately.
        for p in &where_info.predicates {
            set.push(p.column().to_string());
        }
        for c in &where_info.is_null_cols {
            set.push(c.clone());
        }
        for (c, _) in &where_info.in_list_preds {
            set.push(c.clone());
        }
        set.sort_unstable();
        set.dedup();
        Some(set)
    };

    // If there are no computed items the projection is a plain column list.
    // We still build `read_cols` above for the filter-column superset but
    // return a flag so the caller can decide whether to run the compute pass.
    let _ = has_computed; // consumed implicitly via ProjectionItem variants

    Some((Some(proj_items), read_cols))
}

/// Recursively validate that `expr` is a scalar arithmetic tree:
/// `BinaryOp { +, -, *, / }` over leaves that are `Identifier` (table
/// column) or `Value(Number)` (integer/float literal). On success the set
/// of referenced column names is accumulated into `cols`. Returns `false`
/// for anything else (functions, casts, strings, NULLs, nested sub-queries).
fn validate_arithmetic_expr(expr: &Expr, cols: &mut Vec<String>) -> bool {
    match expr {
        Expr::Identifier(id) => {
            cols.push(id.value.clone());
            true
        }
        Expr::Value(ValueWithSpan {
            value: Value::Number(_, _),
            ..
        }) => true,
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr: inner,
        } => validate_arithmetic_expr(inner, cols),
        Expr::BinaryOp { op, left, right } => {
            matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) && validate_arithmetic_expr(left, cols)
                && validate_arithmetic_expr(right, cols)
        }
        // Parenthesised expressions (sqlparser wraps `(expr)` as Nested).
        Expr::Nested(inner) => validate_arithmetic_expr(inner, cols),
        _ => false,
    }
}

/// Try to parse the SELECT projection as a list of aggregate functions.
/// Returns `None` when the projection contains any non-aggregate item (mixed
/// aggregate + row expressions are not supported on the fast path). Only
/// COUNT(*), COUNT(col), SUM(col), MIN(col), MAX(col) with bare-identifier
/// column arguments are recognised; anything with aliases, expressions, or
/// DISTINCT falls back to DataFusion.
fn parse_aggregate_projection(items: &[SelectItem]) -> Option<Vec<AggregateFn>> {
    if items.is_empty() {
        return None;
    }
    let mut aggs = Vec::with_capacity(items.len());
    for item in items {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            _ => return None, // aliases, qualified wildcards, etc.
        };
        let func = match expr {
            Expr::Function(f) => f,
            _ => return None,
        };
        // Must be a plain, unqualified name with no filter/order/over.
        if func.over.is_some() || func.filter.is_some() || func.null_treatment.is_some() {
            return None;
        }
        let name = func
            .name
            .0
            .last()
            .map(|p| p.id_val().to_ascii_lowercase())?;

        let agg = match name.as_str() {
            "count" => {
                // COUNT(*) or COUNT(col) — must have exactly one argument.
                let args = match &func.args {
                    FunctionArguments::List(list) => {
                        // Reject DISTINCT or additional clauses (ORDER BY, LIMIT inside aggregate).
                        if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                            return None;
                        }
                        &list.args
                    }
                    _ => return None,
                };
                if args.len() != 1 {
                    return None;
                }
                match &args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => AggregateFn::CountStar,
                    // COUNT(col) counts non-null values — different from COUNT(*).
                    // Fall back to DataFusion so NULL semantics are correct.
                    _ => return None,
                }
            }
            "sum" => {
                let col = parse_single_col_agg_arg(func)?;
                AggregateFn::Sum(col)
            }
            "min" => {
                let col = parse_single_col_agg_arg(func)?;
                AggregateFn::Min(col)
            }
            "max" => {
                let col = parse_single_col_agg_arg(func)?;
                AggregateFn::Max(col)
            }
            _ => return None,
        };
        aggs.push(agg);
    }
    Some(aggs)
}

/// Extract a single bare-identifier column argument from an aggregate
/// function (for SUM, MIN, MAX). Returns `None` for anything other than
/// `fn(col)`.
fn parse_single_col_agg_arg(func: &sqlparser::ast::Function) -> Option<String> {
    let args = match &func.args {
        FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return None;
            }
            &list.args
        }
        _ => return None,
    };
    if args.len() != 1 {
        return None;
    }
    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
            Some(id.value.clone())
        }
        _ => None,
    }
}

/// Parsed WHERE clause: a conjunction of typed atoms.
///
/// Two kinds of atoms:
/// * `predicates` — `col op lit` comparisons representable as [`Predicate`]
///   (pushed into the storage layer for Parquet/Vortex pruning).
/// * `is_null_cols` — `col IS NULL` checks (no [`Predicate`] variant exists
///   for these; handled via post-read Arrow filter + catalog pruning only).
struct ParsedWhere {
    predicates: Vec<Predicate>,
    is_null_cols: Vec<String>,
    /// `col IN (lit, lit, …)` atoms parsed from the WHERE clause.  These
    /// cannot be pushed into `ReadOptions.filters` (which takes
    /// `Vec<Predicate>` — a conjunction of atoms with no `In` variant) so
    /// they are carried separately and applied as a post-read Arrow filter.
    /// For a single-column PK column they also feed the bloom+zone-map
    /// IN-list probe that prunes cold-tier files before any file body is
    /// opened.
    in_list_preds: Vec<(String, Vec<ScalarValue>)>,
    /// `true` when any conjunctive atom in the WHERE clause is provably
    /// always-FALSE under 3VL — currently just `<col> = NULL`, `NULL = <col>`,
    /// `<col> <> NULL`, `NULL <> <col>`. Inside an AND tree any such atom
    /// poisons the whole WHERE to FALSE (`F AND X ≡ F`), so the query
    /// returns zero rows without touching storage. OR-trees never reach this
    /// recogniser (parse rejects them), so the AND-only short-circuit is
    /// sound.
    always_false: bool,
}

/// Parse the WHERE expression into a [`ParsedWhere`]. Returns `None` to fall
/// back to DataFusion on anything we cannot represent cleanly.
///
/// Accepted atoms:
/// * `<col> <op> <literal>` where `<op>` ∈ {`=`, `>`, `<`, `>=`, `<=`}
/// * `<col> BETWEEN <lo> AND <hi>` (Int64 only, expands to two `Predicate`s)
/// * `<col> IS NULL` (stored separately in `is_null_cols`)
/// * `<left> AND <right>` — at most three combined atoms from the above
///
/// OR, sub-queries, IS NOT NULL, etc. all return `None`.
fn parse_where(expr: &Expr) -> Option<ParsedWhere> {
    let mut out = ParsedWhere {
        predicates: Vec::new(),
        is_null_cols: Vec::new(),
        in_list_preds: Vec::new(),
        always_false: false,
    };
    parse_where_into(expr, &mut out)?;
    Some(out)
}

/// Detect a 3VL-FALSE atom: `<col> = NULL`, `NULL = <col>`, `<col> <> NULL`,
/// or `NULL <> <col>`. All of these evaluate to NULL under SQL three-valued
/// logic, and a NULL in WHERE filters the row out — so the conjunctive
/// predicate is logically FALSE.
///
/// Returns `true` if `expr` is one of these shapes. The caller treats the
/// containing WHERE clause as always-empty.
fn is_3vl_false_atom(expr: &Expr) -> bool {
    let Expr::BinaryOp { op, left, right } = expr else {
        return false;
    };
    if !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) {
        return false;
    }
    let is_null = |e: &Expr| {
        matches!(
            e,
            Expr::Value(ValueWithSpan {
                value: Value::Null,
                ..
            })
        )
    };
    // One side NULL literal AND the other side a column reference (anything
    // else — expr-on-expr — falls through to DataFusion for correctness).
    (is_null(left) && as_identifier(right).is_some())
        || (is_null(right) && as_identifier(left).is_some())
}

/// Recursively populate `out` with atoms from `expr`. Returns `None` to
/// signal "fall through to DataFusion" when a non-fast-path expression is
/// encountered or when the combined atom count exceeds the cap.
fn parse_where_into(expr: &Expr, out: &mut ParsedWhere) -> Option<()> {
    // 3VL-FALSE short-circuit: `col = NULL` / `NULL = col` / `col <> NULL` /
    // `NULL <> col`. Any such atom inside an AND chain makes the whole
    // WHERE clause logically FALSE — the query returns zero rows. Mark the
    // ParsedWhere and bail out of further atom parsing; execute_simple_select
    // honours `always_false` by returning an empty row set without touching
    // storage. Closes the `WHERE x = NULL returns 0 (3VL)` bench gap that
    // ran ~300x slower than PG.
    if is_3vl_false_atom(expr) {
        out.always_false = true;
        return Some(());
    }

    // `col IS NULL`
    if let Expr::IsNull(inner) = expr {
        let col = as_identifier(inner.as_ref())?;
        out.is_null_cols.push(col);
        return Some(());
    }

    // `col BETWEEN lo AND hi` — sqlparser represents this as `Between { expr, low, high }`.
    // Expands to `col > lo-1 AND col < hi+1` for Int64.
    if let Expr::Between {
        expr: col_expr,
        negated: false,
        low,
        high,
    } = expr
    {
        let col = as_identifier(col_expr)?;
        let lo = match literal_value(low)? {
            ScalarValue::Int64(v) => v.checked_sub(1)?,
            _ => return None,
        };
        let hi = match literal_value(high)? {
            ScalarValue::Int64(v) => v.checked_add(1)?,
            _ => return None,
        };
        out.predicates.push(Predicate::Gt(col.clone(), ScalarValue::Int64(lo)));
        out.predicates.push(Predicate::Lt(col, ScalarValue::Int64(hi)));
        return Some(());
    }

    // `<left> AND <right>` — recurse into both sides. Cap at 3 atoms total.
    if let Expr::BinaryOp {
        op: BinaryOperator::And,
        left,
        right,
    } = expr
    {
        parse_where_into(left, out)?;
        parse_where_into(right, out)?;
        if out.predicates.len() + out.is_null_cols.len() > 3 {
            return None;
        }
        return Some(());
    }

    // `<col> LIKE 'foo%'` or `<col> ILIKE 'foo%'` with a literal pattern that
    // is a pure prefix (no `%` or `_` before the trailing `%`). Translates to
    // `Predicate::StartsWith` so the storage layer can prune row groups by
    // min/max bytes and filter per-row without materialising through
    // DataFusion's generic LIKE evaluator.
    //
    // We deliberately reject:
    //   * NOT LIKE (`negated: true`) — no clean pushdown for negated prefix
    //   * `LIKE ANY (...)`            — multi-pattern (`any: true`)
    //   * Patterns with `%`/`_` anywhere except a single trailing `%`
    //   * Non-literal patterns        — bind params, expressions
    //   * Custom `escape_char`        — keep the matcher byte-exact
    if let Expr::Like {
        negated: false,
        any: false,
        expr: like_expr,
        pattern,
        escape_char: None,
    } = expr
    {
        if let Some(p) = parse_prefix_like(like_expr, pattern, false) {
            out.predicates.push(p);
            return Some(());
        }
    }
    if let Expr::ILike {
        negated: false,
        any: false,
        expr: like_expr,
        pattern,
        escape_char: None,
    } = expr
    {
        if let Some(p) = parse_prefix_like(like_expr, pattern, true) {
            out.predicates.push(p);
            return Some(());
        }
    }

    // `col IN (lit, lit, …)` — non-negated only.  All list elements must be
    // recognisable literals; otherwise fall through to DataFusion.  An empty
    // IN-list is 3VL-FALSE (SQL semantics: `x IN ()` is always false), so we
    // mark the WHERE clause as always-empty.
    if let Expr::InList {
        expr: col_expr,
        list,
        negated: false,
    } = expr
    {
        let col = as_identifier(col_expr)?;
        if list.is_empty() {
            out.always_false = true;
            return Some(());
        }
        let vals: Option<Vec<ScalarValue>> = list.iter().map(literal_value).collect();
        let vals = vals?; // any non-literal element → fall through to DataFusion
        out.in_list_preds.push((col, vals));
        return Some(());
    }

    // Single comparison atom.
    let p = parse_predicate(expr)?;
    out.predicates.push(p);
    Some(())
}


/// Parse a single `<col> <op> <literal>` predicate where `<op>` is one of
/// `=`, `>`, `<`, `>=`, `<=` (and their mirror forms). Anything else returns
/// `None` and falls back to the DataFusion path.
///
/// `>=` and `<=` are encoded as the open-ended variants `Gt`/`Lt` of the
/// adjacent integer: `col >= v` → `Predicate::Gt(col, v-1)` and
/// `col <= v` → `Predicate::Lt(col, v+1)`. This transformation is only
/// applied to `Int64` literals where the predecessor/successor is
/// representable; all other types return `None` so the fast path does not
/// silently widen the result set.
fn parse_predicate(expr: &Expr) -> Option<Predicate> {
    let (op, left, right) = match expr {
        Expr::BinaryOp { op, left, right } => (op, left.as_ref(), right.as_ref()),
        _ => return None,
    };

    // (col, literal, flipped): flipped=true means the user wrote `lit op col`
    // so we have to invert the comparison direction.
    let (col, lit, flipped) = if let (Some(c), Some(v)) = (as_identifier(left), literal_value(right)) {
        (c, v, false)
    } else if let (Some(c), Some(v)) = (as_identifier(right), literal_value(left)) {
        (c, v, true)
    } else {
        return None;
    };

    // Map the SQL operator (accounting for col/literal order flip) to a
    // `Predicate` variant. `>=` / `<=` use the predecessor/successor trick for
    // Int64 only — other scalar types remain unsupported to avoid silent
    // widening of float or string comparisons.
    let pred = match (op, flipped) {
        (BinaryOperator::Eq, _) => Predicate::Eq(col, lit),

        // col > lit  /  lit < col
        (BinaryOperator::Gt, false) | (BinaryOperator::Lt, true) => Predicate::Gt(col, lit),

        // col < lit  /  lit > col
        (BinaryOperator::Lt, false) | (BinaryOperator::Gt, true) => Predicate::Lt(col, lit),

        // col >= lit → Gt(col, lit-1), only for Int64
        (BinaryOperator::GtEq, false) | (BinaryOperator::LtEq, true) => match lit {
            ScalarValue::Int64(v) => {
                let prev = v.checked_sub(1)?; // don't fast-path i64::MIN
                Predicate::Gt(col, ScalarValue::Int64(prev))
            }
            _ => return None,
        },

        // col <= lit → Lt(col, lit+1), only for Int64
        (BinaryOperator::LtEq, false) | (BinaryOperator::GtEq, true) => match lit {
            ScalarValue::Int64(v) => {
                let next = v.checked_add(1)?; // don't fast-path i64::MAX
                Predicate::Lt(col, ScalarValue::Int64(next))
            }
            _ => return None,
        },

        _ => return None,
    };
    Some(pred)
}

fn as_identifier(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.clone()),
        _ => None,
    }
}

/// Extract a `Predicate::StartsWith` from a parsed LIKE/ILIKE node.
///
/// Both arguments come straight from sqlparser's `Expr::Like { expr,
/// pattern, .. }`. The left side must be an identifier (`col`) and the
/// pattern must be a string literal whose only wildcard is a trailing `%`.
fn parse_prefix_like(left: &Expr, pattern: &Expr, case_insensitive: bool) -> Option<Predicate> {
    let col = as_identifier(left)?;
    let pat = string_literal_value(pattern)?;
    let prefix = extract_prefix(pat)?;
    if prefix.is_empty() {
        // `LIKE '%'` matches everything; not worth a custom Predicate (would
        // confuse pruning into NoMatch-by-empty-string-min). Fall through.
        return None;
    }
    Some(Predicate::StartsWith {
        column: col,
        prefix,
        case_insensitive,
    })
}

/// Pull a `String` out of an `Expr::Value(SingleQuoted/...)`. Returns
/// `None` for non-literal patterns (bind params, expressions, NULL).
fn string_literal_value(e: &Expr) -> Option<&str> {
    if let Expr::Value(ValueWithSpan { value, .. }) = e {
        match value {
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::EscapedStringLiteral(s)
            | Value::NationalStringLiteral(s) => return Some(s.as_str()),
            _ => return None,
        }
    }
    None
}

/// Map a LIKE pattern to a literal prefix when the pattern is anchored
/// (no `%`/`_` except an optional trailing `%`):
///
///   `"foo%"`    → `Some("foo".to_string())`
///   `"foo"`     → `Some("foo".to_string())` — exact match also fits as a
///                 degenerate prefix; the storage filter then behaves like
///                 equality (still correct because the row-filter checks
///                 `starts_with(prefix)` and the equality form just matches
///                 a superset which the per-row scan then narrows).
///   `"foo_"`    → `None` (single-char wildcard)
///   `"foo%bar"` → `None` (interior wildcard)
///   `"%foo"`    → `None` (leading wildcard — suffix, not prefix)
///
/// We do NOT attempt to honour LIKE-escape sequences (`\%`, `\_`) here —
/// the engine rejected `escape_char: Some(_)` before reaching this helper,
/// and bare `\` in a LIKE pattern is literal in PostgreSQL's default
/// (non-standard) mode. This keeps the matcher byte-exact.
fn extract_prefix(pat: &str) -> Option<String> {
    // Bail on `_` (single-char wildcard) anywhere.
    if pat.contains('_') {
        return None;
    }
    // Strip exactly one trailing `%`. If the result still contains `%`,
    // it's an interior wildcard — bail.
    let stripped = pat.strip_suffix('%').unwrap_or(pat);
    if stripped.contains('%') {
        return None;
    }
    Some(stripped.to_string())
}

/// Crate-visible wrapper over [`literal_value`] for sibling fast-path modules
/// (e.g. `point_join`) that need the same WHERE-literal recognition without
/// duplicating the signed-integer / string / boolean parsing rules.
pub(crate) fn literal_value_pub(e: &Expr) -> Option<ScalarValue> {
    literal_value(e)
}

/// Recognise the literal forms we can push down: signed integers, finite
/// floats, strings, booleans. Anything richer (NULL, casts, vectors) drops us
/// out of the fast path. A numeric literal that is not a valid `i64` (it has a
/// decimal point or an exponent) is parsed as `Float64` — the storage filter
/// path compares `Float64` predicates natively, so e.g. `WHERE amount > 50000.0`
/// over a `Float64` column is pushable rather than forcing the whole statement
/// to DataFusion.
fn literal_value(e: &Expr) -> Option<ScalarValue> {
    let (negate, inner) = match e {
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => (true, expr.as_ref()),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => (false, expr.as_ref()),
        other => (false, other),
    };
    match inner {
        Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => {
            if let Ok(parsed) = s.parse::<i64>() {
                return Some(ScalarValue::Int64(if negate { -parsed } else { parsed }));
            }
            // Not an integer literal → try a finite float (decimal / exponent
            // forms). NaN / inf never appear as SQL number literals; reject a
            // non-finite parse defensively so a pushed predicate stays sane.
            let f: f64 = s.parse().ok()?;
            if !f.is_finite() {
                return None;
            }
            Some(ScalarValue::Float64(if negate { -f } else { f }))
        }
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::EscapedStringLiteral(s),
            ..
        })
        | Expr::Value(ValueWithSpan {
            value: Value::NationalStringLiteral(s),
            ..
        }) => {
            if negate {
                None
            } else {
                Some(ScalarValue::Utf8(s.clone()))
            }
        }
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(b),
            ..
        }) => {
            if negate {
                None
            } else {
                Some(ScalarValue::Boolean(*b))
            }
        }
        _ => None,
    }
}


/// A transaction's pinned read-view, threaded into the fast path so an in-tx
/// SELECT reads AT the pin (snapshot-stable / REPEATABLE-READ-ish) instead of
/// the moving live head.
///
/// Captured by the executor gate via the NON-capturing peeks
/// (`tx_read_snapshot_peek` / `tx_hot_seq_watermark_peek`) — the fast path
/// never pins; a missing pin makes the gate bail to DataFusion, which pins via
/// `load_table_for_read` after its own flush. Both halves must agree:
///   * `snapshot` — the cold (catalog) snapshot id; metadata is loaded via
///     `Catalog::load_table_at_snapshot(snapshot)` so `live_data_files()`
///     reconstructs the historical file set this tx is supposed to see.
///   * `hot_watermark` — the hot-tier MVCC high-water mark; memtable probes
///     drop any registry entry whose `seq` exceeds it (a concurrent session's
///     post-pin UPDATE/DELETE overlay).
#[derive(Debug, Clone, Copy)]
pub(crate) struct PinnedReadView {
    pub snapshot: basin_catalog::SnapshotId,
    pub hot_watermark: u64,
}

/// The in-transaction read-view the executor gate hands the fast path.
///
/// The gate cannot itself CAPTURE a pin: capturing must happen at the SAME
/// post-flush moment a DataFusion read (`load_table_for_read`) would capture
/// it, and the gate runs BEFORE the fast path's own tail flush. So it passes a
/// *request* and the fast path resolves it after flushing:
///
///   * [`PinnedReadRequest::AlreadyPinned`] — both halves were captured by an
///     earlier read of this table; the gate PEEKED them (non-capturing) and the
///     fast path reads AT them. This is the long-standing safe sub-case.
///   * [`PinnedReadRequest::FirstTouch`] — this tx has NOT touched the table yet
///     and no pin exists, but the table is otherwise eligible (no pending data
///     files / HTAP batches / overlay). The fast path FLUSHES first, then
///     CAPTURES both pins at the post-flush head via the SAME helpers
///     `load_table_for_read` uses (`tx_read_snapshot_for` /
///     `tx_hot_seq_watermark_for`), then serves at that just-captured pin.
///
/// Capture-ordering correctness (the historical leak was pinning BEFORE the
/// flush): on `FirstTouch` the fast path pins the head AFTER its own
/// `flush_to_parquet()`, exactly where `load_table_for_read` pins. The capture
/// helpers are idempotent peek-or-insert, so if this read instead BAILS to
/// DataFusion after (or before) capturing, DataFusion's `load_table_for_read`
/// reads back the IDENTICAL pin (or captures the same head — the flush is also
/// idempotent once the tail is drained). Either path yields the same read-view.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PinnedReadRequest {
    /// Both pins already captured by an earlier read; read AT them.
    AlreadyPinned(PinnedReadView),
    /// First touch of this untouched table — flush, then capture-and-serve.
    FirstTouch,
}

/// Run a recognised plan against the engine's storage layer (or, when wired
/// up, the shard's [`ProjectHandle`]). Returns the merged result set ready to
/// hand back to the caller.
///
/// `prefetched_meta` may carry `TableMetadata` already loaded by the caller's
/// fast-path gate check (which loaded it to inspect `rls_enabled` and the
/// soft-delete column). When supplied we skip the redundant `load_table` call;
/// when `None` we fall back to loading it ourselves (e.g. callers that don't
/// pre-check).
///
/// This is the auto-commit / no-pin entrypoint (e.g. `point_join`); it reads
/// the live head. For in-transaction repeatable reads use
/// [`execute_simple_select_pinned`].
///
/// [`ProjectHandle`]: basin_shard::ProjectHandle
pub(crate) async fn execute_simple_select(
    sess: &ProjectSession,
    plan: SimpleSelectPlan,
    prefetched_meta: Option<TableMetadata>,
    raw_sql: &str,
    include_deleted: bool,
) -> Result<ExecResult> {
    execute_simple_select_inner(sess, plan, prefetched_meta, raw_sql, include_deleted, None).await
}

/// In-transaction entrypoint: same as [`execute_simple_select`] but reads at
/// the transaction's pinned read-view (`pinned`) when supplied. The executor
/// gate only passes `Some` for the safe sub-case (table untouched by this tx,
/// both pins already captured); a `None` pin behaves exactly like the
/// auto-commit path. Any piece that cannot honour the pin Ok-falls back to the
/// DataFusion path (`exec_select`).
pub(crate) async fn execute_simple_select_pinned(
    sess: &ProjectSession,
    plan: SimpleSelectPlan,
    prefetched_meta: Option<TableMetadata>,
    raw_sql: &str,
    include_deleted: bool,
    request: Option<PinnedReadRequest>,
) -> Result<ExecResult> {
    execute_simple_select_inner(sess, plan, prefetched_meta, raw_sql, include_deleted, request)
        .await
}

/// S4 commit 5a — pre-flush PK residency probe.
///
/// Attempt to answer the canonical auto-commit point lookup from the hot-tier
/// memtable BEFORE the shard tail flush / catalog load / cold read. Returns:
///   * `Ok(Some(result))` — DEFINITIVE memtable answer (a live `Row`/`Update`
///     for the queried PK, or a `Tombstone` ⇒ empty result). The caller
///     returns it directly: NO flush, NO catalog load, NO file open.
///   * `Ok(None)` — any shape gate missed or the memtable has no entry for
///     the key: the caller proceeds down the existing path unchanged.
///
/// Shape gates (all derived from the gate-prefetched `meta`, which the
/// executor's fast-path gate epoch-validated against the catalog):
///   * RLS disabled (`!meta.rls_enabled`) — defence in depth; the executor
///     gate already refuses RLS tables;
///   * single-column PK; exactly one predicate, an `Eq` on that PK, whose
///     literal encodes to a `RowKey`;
///   * no aggregates / IN-list / IS NULL / ORDER BY (and hence no OFFSET) /
///     computed projection / 3VL-false WHERE;
///   * every projected column resolves in the CATALOG schema — this also
///     rejects promoted `__promoted$…` shadow columns (absent from the
///     catalog schema), which have their own guarded path.
///
/// Correctness: an auto-commit read is entitled to the NEWEST committed
/// value. A memtable hit IS that value — a dirty `Update`/`Row` overrides
/// cold by overlay semantics, and a CLEAN entry is byte-identical to its
/// cold/tail image (S4 residency invariant) — so skipping the flush cannot
/// serve a stale row. A `Tombstone` is the committed deletion of the row, so
/// the empty result needs no cold consultation either.
fn try_serve_point_pre_flush(
    sess: &ProjectSession,
    plan: &SimpleSelectPlan,
    meta: &TableMetadata,
) -> Result<Option<ExecResult>> {
    if meta.rls_enabled || meta.pk_columns.len() != 1 {
        return Ok(None);
    }
    if plan.always_empty
        || plan.aggregates.is_some()
        || plan.order_by.is_some()
        || !plan.in_list_preds.is_empty()
        || !plan.is_null_cols.is_empty()
        || plan.predicates.len() != 1
    {
        return Ok(None);
    }
    let Predicate::Eq(col, val) = &plan.predicates[0] else {
        return Ok(None);
    };
    if col != &meta.pk_columns[0] {
        return Ok(None);
    }
    // Plain-column projection resolvable in the catalog schema only.
    let proj_idxs: Option<Vec<usize>> = match &plan.projection {
        None => None,
        Some(items) => {
            let mut idxs = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    ProjectionItem::Column(c) => match meta.schema.index_of(c) {
                        Ok(i) => idxs.push(i),
                        Err(_) => return Ok(None), // unknown / shadow column
                    },
                    ProjectionItem::Computed { .. } => return Ok(None),
                }
            }
            Some(idxs)
        }
    };
    let Ok(pk_idx) = meta.schema.index_of(col) else {
        return Ok(None);
    };
    let pk_dt = meta.schema.field(pk_idx).data_type().clone();
    let Some(pk_key) = crate::dml_mutate::pk_scalar_to_row_key(val, &pk_dt) else {
        return Ok(None);
    };
    let registry = sess.engine.memtable_registry();
    let Some(entry) = registry.get(&sess.project, &plan.table) else {
        return Ok(None);
    };
    // Output schema shared by the hit and tombstone arms, projected from the
    // catalog schema.
    let projected_schema = |idxs: &Option<Vec<usize>>| -> Result<Arc<Schema>> {
        Ok(match idxs {
            None => meta.schema.clone(),
            Some(ix) => Arc::new(
                meta.schema
                    .project(ix)
                    .map_err(|e| BasinError::internal(format!("pre-flush project schema: {e}")))?,
            ),
        })
    };
    match entry.memtable.get(&pk_key) {
        Some(basin_hottier::MemRowValue::Row { bytes, .. })
        | Some(basin_hottier::MemRowValue::Update { bytes, .. }) => {
            let Some(batch) = decode_ipc_batch(&bytes) else {
                // Undecodable resident bytes — fall through, never error.
                return Ok(None);
            };
            // Re-check the predicate against the decoded row (defence in
            // depth; the key encodes the PK value, so a mismatch means an
            // encoding edge — fall through to the authoritative path).
            if !batch_matches_predicates(&batch, &plan.predicates) {
                return Ok(None);
            }
            // Project BY NAME against the decoded row's own schema (the IPC
            // image carries the write-time column order; name-based selection
            // is robust even if it ever diverged from catalog order).
            let (schema, batches): (Arc<Schema>, Vec<RecordBatch>) = match &plan.projection {
                None => (batch.schema(), vec![batch]),
                Some(items) => {
                    let mut idxs = Vec::with_capacity(items.len());
                    for item in items {
                        let ProjectionItem::Column(c) = item else {
                            return Ok(None); // unreachable per the gate above
                        };
                        match batch.schema().index_of(c) {
                            Ok(i) => idxs.push(i),
                            Err(_) => return Ok(None), // column absent in image
                        }
                    }
                    let projected = batch.project(&idxs).map_err(|e| {
                        BasinError::internal(format!("pre-flush memtable project: {e}"))
                    })?;
                    (projected.schema(), vec![projected])
                }
            };
            let trimmed = match plan.limit {
                Some(limit) => apply_limit(batches, limit),
                None => batches,
            };
            Ok(Some(ExecResult::Rows {
                schema,
                batches: trimmed,
            }))
        }
        Some(basin_hottier::MemRowValue::Tombstone) => Ok(Some(ExecResult::Rows {
            schema: projected_schema(&proj_idxs)?,
            batches: vec![],
        })),
        None => Ok(None),
    }
}

/// Render a DataFusion-plannable statement text for the `exec_select`
/// fallbacks inside the fast path.
///
/// The executor gate hands this module `raw_sql` — the user's ORIGINAL text,
/// captured BEFORE the string-rewrite pipeline ran. For the common fast-path
/// shapes (`SELECT … WHERE id = 5`) no pipeline pass fires, the two texts are
/// identical, and this helper is a zero-allocation passthrough.
///
/// But a [`SimpleSelectPlan`] that references promoted `__promoted$col$key`
/// shadow columns can only exist because the pipeline lowered
/// `payload->>'k'` to `json_get_text(payload, 'k')` and then swapped in the
/// shadow column (`executor::rewrite_promoted_cols_for_query`). In that case
/// `raw_sql` STILL CONTAINS the raw `->>` / `->` / `#>` / `#>>` operator
/// forms — which DataFusion cannot plan (no `ExprPlanner` is registered for
/// the PG JSON operators; the physical planner rejects `Operator::LongArrow`
/// & friends). Handing `raw_sql` straight to `exec_select` therefore turned
/// the conservative "fall back to the always-correct DataFusion path" branch
/// into a hard plan error precisely when the fallback matters: a dirty
/// hot-tier overlay row (a single fast-path UPDATE), a tombstone, a pending
/// shard tail, or a file missing the shadow column.
///
/// Fix: re-apply the SAME two pipeline passes the executor ran (operator
/// spacing, then JSON-operator lowering) so the fallback text carries the
/// `json_get_text(...)` UDF form. We deliberately do NOT re-apply the
/// promoted-column swap: the fallback fires exactly when the promoted fast
/// read is not safe, and the UDF form computes the value from the source
/// JSONB on the DataFusion path (`HtapUnionTable` merges hot + cold), which
/// is correct in every one of those states — including identical
/// NULL/absent-key semantics (`json_get_text` and the shadow column both
/// yield SQL NULL for a missing key, a JSON `null`, or a NULL source row).
fn fallback_sql(raw_sql: &str) -> std::borrow::Cow<'_, str> {
    // Marker probe mirrors `executor::needs_rewrite_pipeline`'s spirit:
    // `->` catches `->`/`->>`; `#>` catches `#>`/`#>>`. False positives
    // (e.g. `->` inside a string literal) only cost re-running the same
    // idempotent passes the pipeline already ran on this text.
    if raw_sql.contains("->") || raw_sql.contains("#>") {
        let spaced = crate::pg_operators::rewrite_jsonb_arrow_op_spacing(raw_sql);
        std::borrow::Cow::Owned(crate::udf::rewrite_json_operators(&spaced))
    } else {
        std::borrow::Cow::Borrowed(raw_sql)
    }
}

/// Default small-tail threshold for the read-path merge-on-read gate
/// (`BASIN_READ_FLUSH_MIN_TAIL_ROWS`). When an auto-commit SELECT finds the
/// shard tail holds at most this many un-flushed rows, it merges the tail on
/// read (via the shard's tail-merging `read`) instead of paying a synchronous
/// flush. Above the threshold the tail is flushed once and amortized over
/// future reads. `0` disables the small-tail gate (the tail-empty fast-gate
/// still applies). Sized to cover OLTP write bursts (a handful of single-row
/// INSERTs between reads) without re-merging large bulk loads on every read.
const READ_FLUSH_MIN_TAIL_ROWS_DEFAULT: usize = 256;

fn read_flush_min_tail_rows() -> usize {
    std::env::var("BASIN_READ_FLUSH_MIN_TAIL_ROWS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(READ_FLUSH_MIN_TAIL_ROWS_DEFAULT)
}

async fn execute_simple_select_inner(
    sess: &ProjectSession,
    plan: SimpleSelectPlan,
    prefetched_meta: Option<TableMetadata>,
    raw_sql: &str,
    include_deleted: bool,
    request: Option<PinnedReadRequest>,
) -> Result<ExecResult> {
    // Phase 5.16.A: fast path bypasses DataFusion (no LogicalPlan); shape
    // hash is computed on the DataFusion path (executor::exec_select).
    // 5.16.B will wire both paths into the histogram registry.
    let _ = &plan.table;

    // Flush the in-RAM tail before we look up the table's metadata so the
    // post-flush snapshot is used for catalog reads. Without this the catalog
    // reports zero data files (everything still in WAL).
    //
    // Correctness: the `prefetched_meta` passed by the executor gate was
    // loaded BEFORE `flush_to_parquet()` runs, so it reflects a snapshot
    // that may have zero data files (all rows still in the shard tail).
    // When the shard is configured we MUST reload after the flush so that
    // the newly-committed Parquet files are visible to `live_data_files()`.
    // When no shard is configured the pre-fetch is always current (writes go
    // directly to storage + catalog synchronously) and can be reused as-is.
    // Helper: load the table's metadata at the read-view this call must honour.
    //
    //   * Auto-commit / no pin (`pinned == None`): the live head — exactly the
    //     historical behaviour.
    //   * Pinned (in-tx repeatable read): the HISTORICAL metadata at
    //     `pinned.snapshot` via `load_table_at_snapshot`, so `live_data_files()`
    //     reconstructs the file set this transaction is supposed to see — NOT
    //     the moved head a concurrent committer / our own flush advanced to.
    //     `FeatureNotSupported` (the pinned snapshot is no longer retained, or
    //     the backend lacks history) is NOT a hard error: we Ok-fall back to the
    //     DataFusion path, which degrades to read-committed for that one read
    //     rather than serving a wrong point-in-time. This MUST reload at the pin
    //     after the shard flush too: flushing ADVANCES the head, but the pin
    //     protects the read, so the post-flush reload pins the historical view,
    //     never the new head.
    //
    // NB: the per-session meta cache (`load_table_meta_cached`) and the
    // `prefetched_meta` reflect the live head; under a pin we must NOT reuse
    // them, so we always go to the catalog at the pinned snapshot here.
    async fn load_at_view(
        sess: &ProjectSession,
        table: &TableName,
        pinned: Option<PinnedReadView>,
    ) -> Result<TableMetadata> {
        match pinned {
            None => {
                sess.engine
                    .config()
                    .catalog
                    .load_table(&sess.project, table)
                    .await
            }
            Some(p) => {
                sess.engine
                    .config()
                    .catalog
                    .load_table_at_snapshot(&sess.project, table, p.snapshot)
                    .await
            }
        }
    }

    // Capture BOTH halves of the read-view for an untouched, not-yet-pinned
    // table at THIS moment (the caller has just flushed the shard tail / there
    // is no shard, so `live_meta.current_snapshot` is the post-flush head). Uses
    // the SAME peek-or-insert helpers `load_table_for_read` uses, at the SAME
    // post-flush moment, so a subsequent DataFusion read of this table reads back
    // the IDENTICAL pin. The hot watermark is captured FIRST (matching
    // `load_table_for_read`'s ordering) so the two halves agree. `_for` is
    // idempotent: if this read later bails to DataFusion, that path peeks back
    // exactly what we pinned here.
    fn capture_first_touch(
        sess: &ProjectSession,
        table: &TableName,
        head: basin_catalog::SnapshotId,
    ) -> PinnedReadView {
        let current_hot_seq = sess.engine.memtable_registry().hot_tier_seq(&sess.project, table);
        let hot_watermark = crate::session::tx_hot_seq_watermark_for(&sess.state, table, current_hot_seq)
            .expect("FirstTouch implies an active transaction");
        let snapshot = crate::session::tx_read_snapshot_for(&sess.state, table, head)
            .expect("FirstTouch implies an active transaction");
        PinnedReadView { snapshot, hot_watermark }
    }

    // ── S4 pre-flush PK residency probe (read-own-insert, zero file opens) ───
    //
    // For the canonical auto-commit point lookup, a memtable PK direct-get can
    // answer DEFINITIVELY before we pay the `shard.flush_to_parquet()` below
    // (which LISTs/scans partitions even when the tail is empty), the catalog
    // `load_table`, and the cold file open/decode:
    //   * `Row` / `Update` hit — the newest committed value for that PK
    //     (write-through residency, retained clean rows, or a live overlay
    //     override): decode, re-check the predicate, project, return.
    //   * `Tombstone` — the row is definitively deleted: empty result.
    //   * miss — fall through to the existing path UNCHANGED (flush first).
    //
    // Auto-commit only (`request == None`): a pinned request must keep
    // today's order — `FirstTouch` captures its watermark/snapshot at the
    // POST-flush moment, so probing pre-flush would change what gets pinned.
    // The shape gates live in `try_serve_point_pre_flush`; any gate miss
    // falls through with zero behavior change.
    if request.is_none() {
        if let Some(meta) = prefetched_meta.as_ref() {
            if let Some(res) = try_serve_point_pre_flush(sess, &plan, meta)? {
                return Ok(res);
            }
        }
    }

    // ── Flush-on-read decision (concurrency fix) ─────────────────────────────
    //
    // The unconditional `shard.flush_to_parquet()` below is the read path's
    // single biggest serialization point under mixed read/write load: every
    // SELECT takes the per-partition compact lock and drains the tail to
    // Parquet, so readers queue behind writers (and behind each other) doing
    // flush work they rarely need. Two gates avoid the flush in the common
    // AUTO-COMMIT case (`request.is_none()` — the shape both benchmark losses
    // exercise; pinned / in-tx / FirstTouch reads keep the unconditional flush,
    // which they depend on for snapshot correctness):
    //
    //   1. Tail-empty fast-gate (`has_pending_tail` == false): there is nothing
    //      to drain, so the flush is a pure no-op for data — but the real
    //      `flush_to_parquet` still LISTs/scans every resident partition and
    //      takes the compact lock. Skip it entirely: `load_table` already
    //      reflects the correct head (no un-flushed rows exist), the cold read
    //      is authoritative, and `handle.read` would merge an empty tail anyway.
    //
    //   2. Small-tail merge-on-read (`pending_tail_rows <= threshold`): rather
    //      than flush a tiny tail, leave it in RAM and MERGE it into the read.
    //      The shard's own `ProjectHandle::read` already unions the in-RAM tail
    //      over the Parquet base (see `InProcessProjectHandle::read`), so the
    //      default shard read branch is tail-complete for free. We only force
    //      `tail_merge_via_shard_read = true` so the cold-file-DIRECT bypass
    //      branches (`had_pk_probe`, the unordered-LIMIT per-file path — which
    //      read `live_data_files()` straight from storage and would MISS the
    //      un-flushed tail) are disabled for this statement, routing the read
    //      through the tail-merging `handle.read`. Read-own-write is preserved:
    //      a session's just-INSERTed row is either already served by the
    //      pre-flush PK probe / write-through residency above, or it lives in
    //      the tail that `handle.read` now merges.
    //
    // Above the threshold the tail is large enough that draining it once (and
    // amortizing the cost over future reads) beats re-merging it on every read,
    // so we flush as before. The threshold is env-tunable.
    //
    // Both gates are pure reads of the resident tail maps (no list, no drain,
    // no compact lock); a write landing between the probe and the read is simply
    // not part of this statement's view (same as a cold row landing mid-scan),
    // and a write that lands and is NOT merged here is still durable in the
    // WAL/tail and visible to the next read.
    let mut tail_merge_via_shard_read = false;
    let read_flush_skip = if request.is_none() {
        if let Some(shard) = sess.engine.config().shard.as_ref() {
            if !shard.has_pending_tail(&sess.project, &plan.table).await {
                // Gate 1: empty tail → flush is a no-op; skip it.
                true
            } else {
                let threshold = read_flush_min_tail_rows();
                if threshold > 0
                    && shard.pending_tail_rows(&sess.project, &plan.table).await <= threshold
                {
                    // Gate 2: small tail → merge on read via `handle.read`
                    // instead of flushing.
                    tail_merge_via_shard_read = true;
                    true
                } else {
                    false
                }
            }
        } else {
            false
        }
    } else {
        // Pinned / FirstTouch reads always flush (correctness, not perf).
        false
    };

    // Resolve the gate's read-view REQUEST into a concrete `Option<PinnedReadView>`,
    // loading metadata at the resolved pin. `FirstTouch` must flush (shard) before
    // it captures, so resolution happens per-branch alongside the flush.
    let (meta, pinned): (TableMetadata, Option<PinnedReadView>) =
        if let Some(shard) = sess.engine.config().shard.as_ref() {
            if !read_flush_skip {
                shard.flush_to_parquet().await?;
            }
            match request {
                // First touch of an untouched table: the flush above drained the
                // tail and ADVANCED the head, so the live metadata IS the
                // post-flush head. Capture both pins against it (post-flush, the
                // same moment `load_table_for_read` would), then serve at that
                // pin — `load_at_view(Some(pin))` loads the same head, trivially.
                Some(PinnedReadRequest::FirstTouch) => {
                    let live = sess
                        .engine
                        .config()
                        .catalog
                        .load_table(&sess.project, &plan.table)
                        .await?;
                    let pin = capture_first_touch(sess, &plan.table, live.current_snapshot);
                    (live, Some(pin))
                }
                // Already pinned (or auto-commit): reload after flush. Under a pin
                // this reloads the HISTORICAL view (the flush advanced the head,
                // but the pin protects the read).
                other => {
                    let pin = match other {
                        Some(PinnedReadRequest::AlreadyPinned(v)) => Some(v),
                        _ => None,
                    };
                    let m = match load_at_view(sess, &plan.table, pin).await {
                        Ok(m) => m,
                        // Pinned snapshot no longer retained → degrade to the
                        // DataFusion path, which rewinds via `load_table_for_read`.
                        // `fallback_sql` restores the plannable UDF form when the
                        // original text carries raw JSON operators (see its doc).
                        Err(BasinError::FeatureNotSupported(_)) if pin.is_some() => {
                            let fb = fallback_sql(raw_sql);
                            return crate::executor::exec_select(
                                sess,
                                &fb,
                                include_deleted,
                                Some(raw_sql),
                            )
                            .await;
                        }
                        Err(e) => return Err(e),
                    };
                    (m, pin)
                }
            }
        } else {
            // No shard: writes land directly in storage + catalog synchronously,
            // so no flush is needed — capture / read against the current head.
            match request {
                Some(PinnedReadRequest::FirstTouch) => {
                    let live = sess
                        .engine
                        .config()
                        .catalog
                        .load_table(&sess.project, &plan.table)
                        .await?;
                    let pin = capture_first_touch(sess, &plan.table, live.current_snapshot);
                    (live, Some(pin))
                }
                Some(PinnedReadRequest::AlreadyPinned(v)) => {
                    // A pin is active: the pre-fetched metadata reflects the live
                    // head and must NOT be reused — load the historical view.
                    let m = match load_at_view(sess, &plan.table, Some(v)).await {
                        Ok(m) => m,
                        Err(BasinError::FeatureNotSupported(_)) => {
                            let fb = fallback_sql(raw_sql);
                            return crate::executor::exec_select(
                                sess,
                                &fb,
                                include_deleted,
                                Some(raw_sql),
                            )
                            .await;
                        }
                        Err(e) => return Err(e),
                    };
                    (m, Some(v))
                }
                None => {
                    // Auto-commit, no pin: use the pre-fetched metadata when
                    // available (saves one catalog round-trip the gate paid).
                    let m = match prefetched_meta {
                        Some(m) => m,
                        None => {
                            sess.engine
                                .config()
                                .catalog
                                .load_table(&sess.project, &plan.table)
                                .await?
                        }
                    };
                    (m, None)
                }
            }
        };

    // Self-driving physical layout: observe non-PK Eq predicates and
    // auto-create a secondary index once a column crosses the threshold.
    // Best-effort, non-blocking; the CREATE INDEX is spawned fire-and-forget.
    crate::index_advisor::observe_eq_predicates(sess, &plan.table, &meta, &plan.predicates);

    // ── ADR 0027 Phase 4: promoted JSONB shadow-column read path ─────────────
    //
    // The SQL rewriter swaps `json_get_text(col,'key')` for the materialised
    // shadow column `__promoted$col$key` (a plain `Utf8` column) when the path
    // is promoted, so `plan.read_cols` / `plan.projection` may reference shadow
    // columns that are absent from `meta.schema` (shadow columns are kept out
    // of the catalog schema; they live only in the physical files).
    //
    // To read them we (1) verify the correctness guard, then (2) extend the
    // working `meta.schema` with a `Utf8` field per referenced shadow column.
    // After step (2) every downstream `meta.schema` lookup (projection,
    // read_cols validation, the reader's `catalog_schema`) transparently
    // includes the shadow columns — no other code path needs to change.
    let referenced_shadow_cols: Vec<String> = {
        let mut cols: Vec<String> = Vec::new();
        let mut push = |c: &str, cols: &mut Vec<String>| {
            if c.starts_with("__promoted$") && !cols.iter().any(|x| x == c) {
                cols.push(c.to_string());
            }
        };
        if let Some(rc) = plan.read_cols.as_ref() {
            for c in rc {
                push(c, &mut cols);
            }
        }
        if let Some(items) = plan.projection.as_ref() {
            for it in items {
                if let ProjectionItem::Column(c) = it {
                    push(c, &mut cols);
                }
            }
        }
        // Predicates may reference a promoted shadow column even when the
        // projection / read_cols do not — e.g. `COUNT(*) WHERE
        // __promoted$payload$category = '…'` has projection/read_cols = None
        // (aggregate reads "all columns") yet the WHERE filter still needs the
        // shadow column decoded. Without scanning predicates here the schema is
        // not extended and the read omits the shadow column, so the storage
        // predicate evaluator fails with "predicate column missing".
        for p in &plan.predicates {
            push(p.column(), &mut cols);
        }
        for c in &plan.is_null_cols {
            push(c, &mut cols);
        }
        for (c, _) in &plan.in_list_preds {
            push(c, &mut cols);
        }
        cols
    };
    // True once we have extended `meta.schema` with promoted shadow columns.
    // The shard read path (`handle.read`) re-derives its schema from the catalog
    // (which excludes shadow columns), so it cannot read them; when this is set
    // the cold files must be read directly with the extended schema instead.
    let shadow_cols_present = !referenced_shadow_cols.is_empty();
    let meta = if referenced_shadow_cols.is_empty() {
        // No shadow column referenced — the common case, untouched.
        meta
    } else {
        // Correctness guard: a shadow column exists only in files
        // written/backfilled AFTER the path was promoted.  The reader
        // null-fills any projected column absent from a file's Arrow schema
        // (see `reader::read_paths_inner`), so reading the shadow column from a
        // pre-promotion file would yield a spurious NULL — a WRONG answer, not
        // just a slow one.  The authoritative per-file presence signal is the
        // catalog's `DataFileRef::column_stats`: the writer inserts one entry
        // per physical column for both Parquet and Vortex
        // (`writer::extract_column_stats` / `vortex_format::column_stats_from_batch`
        // iterate every batch field), so a shadow column appears in
        // `column_stats` iff the file carries it.  We take the fast path only
        // when EVERY live data file has EVERY referenced shadow column; if any
        // file is missing one we delegate to the DataFusion / UDF path
        // (`exec_select`), which computes the value from the source JSONB and
        // is therefore always correct (slow but never wrong).
        //
        // This runs against the post-flush `meta` reloaded above, so the shard
        // tail has already been drained to cold-tier files and the file set is
        // authoritative.
        //
        // Hot-tier exception: the engine's `MemTableRegistry` holds post-COMMIT
        // HTAP-cached rows (and fast-path UPDATE/DELETE overlays) that are NOT
        // covered by the cold-file `column_stats` guard.  A row INSERTed before
        // the path was promoted and still resident in the memtable would carry
        // no shadow column, so the fast-path read (which reads the shadow
        // column physically from cold files only and would not see this hot
        // row's correct extracted value) could be wrong.  Whenever the memtable
        // has ANY live entry for this table we therefore delegate to the
        // DataFusion path, whose `HtapUnionTable` merges hot+cold and computes
        // `json_get_text` from the source JSONB for every row.  This is the
        // common-case-fast / rare-case-correct split: a freshly-written table
        // pays the DataFusion cost until its tail drains, then the fast path
        // engages.
        // NOTE: this `fast_select` shadow path reads the shadow column
        // PHYSICALLY from cold files only — it does NOT apply hot-tier
        // UPDATE/DELETE overlays or include un-flushed hot INSERTs (unlike
        // the DataFusion `HtapUnionTable` path). So any DIRTY memtable entry
        // must force a fallback here, even one that carries the shadow
        // column: an updated PK would otherwise read its STALE cold shadow
        // value, and a not-yet-cold INSERT would be missed entirely.
        //
        // S4 age-based residency: retained CLEAN entries no longer block the
        // fast shadow read. A clean entry is byte-identical to its cold image
        // — INCLUDING the physically materialised shadow column the cold file
        // carries — so the cold shadow value is exactly the hot value. The
        // pre-S4 blanket "any live entry" reject would have permanently
        // disabled this path on any table with retained residency. Four O(1)
        // signals replace the O(n) snapshot walk: dirty bytes, the
        // update/tombstone newest-version counters (a dirty Tombstone holds
        // ZERO bytes, so `bytes_dirty` alone cannot see it), and the
        // shadow-dirty flag (a pre-promotion row may lack the column even
        // when clean).
        let memtable_has_entries = sess
            .engine
            .memtable_registry()
            .get(&sess.project, &plan.table)
            .map(|e| {
                e.memtable.bytes_dirty() > 0
                    || e.memtable.update_count() > 0
                    || e.memtable.tombstone_count() > 0
                    || e.shadow_dirty.load(std::sync::atomic::Ordering::Acquire)
            })
            .unwrap_or(false);
        // Defense-in-depth: also reject when the shard holds un-flushed tail
        // rows (a row written before promotion, not yet a cold file, carries no
        // shadow column).  This is a cheap O(1) no-flush probe — it never lists
        // or drains storage — and closes any cross-session window between the
        // SQL rewrite's guard and execution here.
        let has_pending_tail = match sess.engine.config().shard.as_ref() {
            Some(shard) => shard.has_pending_tail(&sess.project, &plan.table).await,
            None => false,
        };
        let live_files = meta.live_data_files();
        let all_present = !memtable_has_entries
            && !has_pending_tail
            && live_files.iter().all(|f| {
                referenced_shadow_cols
                    .iter()
                    .all(|sc| f.column_stats.contains_key(sc))
            });
        if !all_present {
            // At least one file predates promotion (or backfill has not yet
            // covered it), or a dirty hot-tier entry / pending tail makes the
            // cold-only shadow read unsafe.  Fall back to the correct
            // DataFusion path.  The plan references `__promoted$…` columns, so
            // `raw_sql` necessarily carries raw `->>` operator forms DataFusion
            // cannot plan — `fallback_sql` lowers them back to the
            // always-correct `json_get_text(...)` UDF form (see its doc).
            let fb = fallback_sql(raw_sql);
            return crate::executor::exec_select(sess, &fb, include_deleted, Some(raw_sql))
                .await;
        }
        // Every file carries the shadow column(s).  Extend the working schema
        // with a `Utf8` field per shadow column so the projection / validation
        // / reader code below treats them as first-class projectable columns.
        let mut fields: Vec<arrow_schema::FieldRef> =
            meta.schema.fields().iter().cloned().collect();
        for sc in &referenced_shadow_cols {
            if meta.schema.field_with_name(sc).is_err() {
                fields.push(Arc::new(arrow_schema::Field::new(
                    sc,
                    arrow_schema::DataType::Utf8,
                    true,
                )));
            }
        }
        let extended_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
            fields,
            meta.schema.metadata().clone(),
        ));
        // Record that the promoted shadow-column read path actually engaged
        // (test introspection: proves we did NOT fall back to DataFusion).
        sess.engine.note_promoted_fast_select();
        let mut m = meta;
        m.schema = extended_schema;
        m
    };

    // 3VL constant-fold: a WHERE clause containing `<col> = NULL` (or its
    // variants — see `is_3vl_false_atom`) returns the empty set under SQL
    // three-valued logic. Return an empty row or empty aggregate result
    // without consulting storage. `apply_aggregates` over an empty
    // `Vec<RecordBatch>` produces the empty-relation answer naturally
    // (count=0, sum/min/max=NULL).
    if plan.always_empty {
        if let Some(aggs) = &plan.aggregates {
            return apply_aggregates(Vec::new(), aggs, &meta.schema);
        }
        // Row-returning path: project the requested schema and return zero
        // rows. Mirrors the PkProbeOutcome::Absent branch below.
        let output_schema = match &plan.projection {
            None => meta.schema.clone(),
            Some(items) => {
                let idxs: Result<Vec<usize>> = items
                    .iter()
                    .filter_map(|item| match item {
                        ProjectionItem::Column(c) => Some(
                            meta.schema
                                .index_of(c)
                                .map_err(|_| {
                                    BasinError::UndefinedColumn(c.to_string())
                                }),
                        ),
                        _ => None,
                    })
                    .collect();
                match idxs {
                    Ok(ix) => Arc::new(
                        meta.schema
                            .project(&ix)
                            .unwrap_or_else(|_| meta.schema.as_ref().clone()),
                    ),
                    Err(_) => meta.schema.clone(),
                }
            }
        };
        return Ok(ExecResult::Rows {
            schema: output_schema,
            batches: vec![],
        });
    }

    // Validate the read-superset columns against the schema. For computed
    // projections `read_cols` is the union of all source columns + filter
    // columns; for plain projections it equals the column list; for wildcard
    // it is `None`. Also guard that every computed source column is Int64 or
    // Float64 — other types fall through to DataFusion.
    if let Some(ref items) = plan.projection {
        for item in items {
            if let ProjectionItem::Computed { source_cols, .. } = item {
                for c in source_cols {
                    let idx = meta
                        .schema
                        .index_of(c)
                        .map_err(|_| BasinError::UndefinedColumn(c.to_string()))?;
                    let dt = meta.schema.field(idx).data_type();
                    match dt {
                        arrow_schema::DataType::Int64 | arrow_schema::DataType::Float64 => {}
                        _ => {
                            // Non-numeric column in arithmetic — bail to DataFusion.
                            return Err(BasinError::internal(format!(
                                "fast_select: computed expr on non-numeric column {c} ({dt}); \
                                 use DataFusion path"
                            )));
                        }
                    }
                }
            }
        }
    }

    // Validate read_cols so unknown columns surface as a clean error.
    if let Some(ref cols) = plan.read_cols {
        for c in cols {
            meta.schema
                .index_of(c)
                .map_err(|_| BasinError::UndefinedColumn(c.to_string()))?;
        }
    }

    // Build read options using the storage-superset column list. For plain
    // column projections this is identical to the user list; for computed
    // projections it is the union of all source columns + filter columns.
    //
    // LIMIT pushdown: when the query carries a `LIMIT n` AND there is no
    // ORDER BY (no global sort needed) AND no aggregate (which folds every
    // row) AND no post-read shrinking filter (IS NULL post-filter, or a
    // live tombstone / UPDATE-overlay for this table — which can REMOVE
    // cold-tier rows the limit just admitted), pass the cap into
    // ReadOptions so the storage layer can stop emitting batches once `n`
    // post-filter rows have been produced. Otherwise leave the read
    // unbounded and let the existing `apply_limit` pass after the post-read
    // shrinks bound the final result — this preserves the "≥ n when more
    // exist" guarantee.
    // Under a transaction pin, thread the hot-seq watermark through EVERY
    // hot-tier overlay probe so a concurrent session's post-pin UPDATE/DELETE
    // (seq > watermark) is invisible to this read. `None` = auto-commit / no
    // pin: today's behaviour (every committed overlay entry is visible).
    let hot_watermark = pinned.map(|p| p.hot_watermark);
    // Probe the memtable registry for any overlay activity on this
    // (project, table), capturing the snapshots ONCE per statement. Both
    // probes are O(1) when the overlay is empty (the common case). The same
    // snapshots are reused (a) to size the overlay SLACK for the keyset /
    // unordered-LIMIT per-file limits below and (b) by the merge-on-read
    // suppression + append step after the cold read — so the slack bound and
    // the overlay actually applied can never diverge mid-statement (an
    // overlay write landing after this point is simply not part of this
    // statement's read view, exactly like a cold row landing mid-scan).
    // Under a pin the watermark hides post-pin overlays so the decisions
    // below match what the read will actually merge.
    let (overlay_tombs, overlay_updates) = {
        let registry = sess.engine.memtable_registry();
        (
            crate::hot_tombstone::snapshot_tombstones(
                registry.as_ref(),
                &sess.project,
                &plan.table,
                hot_watermark,
            ),
            crate::hot_tombstone::snapshot_updates(
                registry.as_ref(),
                &sess.project,
                &plan.table,
                hot_watermark,
            ),
        )
    };
    let overlay_active = !overlay_tombs.is_empty() || !overlay_updates.is_empty();
    // Number of distinct overlay keys. Merge-on-read can REMOVE a cold row
    // only when its PK equals an overlay key, and a PK occurs at most once
    // per cold file (single-PK uniqueness within a file), so this bounds how
    // many rows suppression can remove from any per-file prefix — the basis
    // for the keyset / unordered-LIMIT overlay tolerance below.
    let overlay_slack: usize = overlay_tombs.len() + overlay_updates.len();
    // The merge-on-read step below only knows how to suppress/append via a
    // single-column PK; overlay entries are only ever written for such
    // tables, but gate defensively — a live overlay the merge CANNOT apply
    // must keep every limit-pushdown path disabled.
    let overlay_mergeable = meta.pk_columns.len() == 1;
    // Overlay-aware read projection: the merge-on-read suppression below
    // (`apply_tombstone_filter_to_batches` / `apply_update_overlay_to_batches`)
    // encodes each COLD row's PK to decide whether a tombstone or UPDATE
    // override hides it — and `filter_batch` degrades to a PASS-THROUGH when
    // the PK column is absent from the batch. A narrowed read projection that
    // omits the PK (e.g. `SELECT SUM(v)`'s minimal aggregate read set, or a
    // non-PK column projection) would therefore keep every overridden cold row
    // AND append the override rows on top — double-counting every overridden
    // key. Augment the storage read with the PK column whenever an overlay is
    // live; downstream consumers select columns BY NAME (aggregates, the
    // row-projection rebuild), so the extra column never leaks into results.
    // Mirrors the projection augmentation `TombstoneFilteringTable::scan`
    // performs on the DataFusion path.
    let mut plan = plan;
    if overlay_active && meta.pk_columns.len() == 1 {
        if let Some(cols) = plan.read_cols.as_mut() {
            let pk = meta.pk_columns[0].as_str();
            if !cols.iter().any(|c| c == pk) && meta.schema.index_of(pk).is_ok() {
                cols.push(pk.to_string());
            }
        }
    }
    let plan = plan;
    let post_read_shrinking = !plan.is_null_cols.is_empty()
        || !plan.in_list_preds.is_empty()
        || overlay_active;
    let pushdown_limit = if plan.order_by.is_none()
        && plan.aggregates.is_none()
        && !post_read_shrinking
    {
        plan.limit
    } else {
        None
    };

    // ── Keyset-pagination per-file LIMIT pushdown ────────────────────────────
    //
    // Shape:  `SELECT … FROM t WHERE k > $1 ORDER BY k ASC LIMIT n`
    // where `k` is the table's single effective cluster column (the column the
    // writer physically sorts each file on; see `effective_cluster_col`).
    //
    // Why this is a win.  The recogniser already pushes the `Gt` predicate, but
    // the ORDER BY gates the LIMIT pushdown OFF (above) because the storage
    // layer's stream LIMIT is GLOBAL across paths and would truncate the merge
    // input before the global sort.  Each cold file is, however, internally
    // sorted ASC on `k` (writer `sort_batch_by_cluster_cols`, ascending), so
    // the first `n` rows of each file that survive the `k > $1` filter are a
    // SUPERSET of that file's contribution to the global top-`n`.  Reading each
    // candidate file with its OWN per-file LIMIT therefore bounds the total
    // merge input to `n_files × n` rows (tiny), and the existing
    // `apply_order_by_limit` (two-phase key-column top-k) then produces the
    // exact global top-`n` cheaply — the dominant cost (reading every surviving
    // row of every file in full) is eliminated.
    //
    // Direction.  Files are sorted ASC, so the first `n` rows are the SMALLEST
    // `k`.  This superset reasoning holds ONLY for ASC + Gt (we want the
    // smallest matching keys).  A DESC + Lt query wants the LARGEST matching
    // keys, which are at the END of each file — a leading per-file LIMIT would
    // drop exactly the rows we need.  DESC therefore does NOT get the pushdown;
    // it reads files in full and the existing path produces the correct answer
    // (slower, but never wrong).
    //
    // Correctness guards.  The pushdown is DISABLED for IS NULL / IN
    // post-filters (their selectivity is unbounded — no slack covers them).
    // A live hot-tier tombstone / UPDATE overlay, however, is TOLERATED via
    // a slack margin instead of declining: merge-on-read can remove at most
    // ONE cold row per overlay key from any per-file prefix (PKs are unique
    // within a file), so inflating each file's head LIMIT by
    // `overlay_slack = |tombstones| + |updates|` guarantees that, after
    // suppression, each file still contributes at least `limit + offset`
    // surviving head rows — a superset of its global-top-`(limit+offset)`
    // contribution. Override rows the overlay APPENDS only add candidates
    // (they re-enter through `apply_update_overlay_to_batches`, which
    // re-applies the pushed predicates, and the final
    // `apply_order_by_limit` sorts them into place), and an override cannot
    // move a row's keyset position in a way the slack misses: its cold image
    // is suppressed (covered by the slack) and its new image is appended
    // globally, independent of any per-file cut. The overlay must be
    // MERGEABLE (single-column PK) — otherwise we decline as before.
    //
    // RLS / soft-delete / views: the executor gate (`exec_select`) already
    // refuses to call `execute_simple_select` for those, so the keyset path
    // inherits the exclusion — no extra check here.
    let effective_cluster = effective_cluster_col(&meta);
    let keyset_per_file_limit: Option<usize> = match (&plan.order_by, plan.limit) {
        (Some((ob_col, ascending)), Some(limit))
            if (!overlay_active || overlay_mergeable)
                && plan.aggregates.is_none()
                && plan.is_null_cols.is_empty()
                && plan.in_list_preds.is_empty()
                && plan.predicates.len() == 1
                && effective_cluster
                    .as_deref()
                    .is_some_and(|c| c == ob_col.as_str()) =>
        {
            match &plan.predicates[0] {
                // ASC + `k > $1`: smallest matching keys are the file head.
                // With an OFFSET, the global top-`(limit+offset)` is needed
                // before the skip, so each ASC-sorted file's head must supply
                // `limit + offset` rows (still a superset of its contribution).
                // `overlay_slack` rows are added on top so merge-on-read
                // suppression cannot leave a shortfall (see the guard note
                // above). `saturating_add` keeps a pathological OFFSET from
                // wrapping; an over-large per-file cap just reads the whole
                // file (correct, never wrong).
                Predicate::Gt(col, _) if *ascending && col == ob_col => Some(
                    limit
                        .saturating_add(plan.offset.unwrap_or(0))
                        .saturating_add(overlay_slack),
                ),
                // DESC + `k < $1` (and every other shape) is not eligible for
                // the per-file head LIMIT — leave it to the full-file path.
                _ => None,
            }
        }
        _ => None,
    };
    // Small-tail merge-on-read: the keyset per-file path reads
    // `live_data_files()` straight from storage (never via the tail-merging
    // `handle.read`), so a skipped flush would leave the un-flushed tail
    // invisible to it — a just-INSERTed row with `k > $1` could be dropped from
    // the page. Disable the per-file LIMIT pushdown for this statement so the
    // read routes through `handle.read`, which merges the tail. (The whole-file
    // keyset answer stays correct — slower, never wrong.)
    let keyset_per_file_limit = if tail_merge_via_shard_read {
        None
    } else {
        keyset_per_file_limit
    };
    // Unordered-LIMIT early-exit target (the `LIMIT k`, no-ORDER-BY twin of
    // the keyset slack above): collect `k + overlay_slack` post-pushdown cold
    // rows so that, after merge-on-read suppression (≤ `overlay_slack` rows
    // removed), at least `k` survive — `apply_limit` then trims to exactly
    // `k`. Appended override rows only ADD matching candidates. With no
    // overlay this degenerates to today's exact-`k` early exit. IS NULL /
    // IN-list post-filters stay excluded (unbounded selectivity, no slack
    // covers them), as do non-mergeable overlays.
    let unordered_limit_target: Option<usize> = if plan.order_by.is_none()
        && plan.aggregates.is_none()
        && plan.is_null_cols.is_empty()
        && plan.in_list_preds.is_empty()
        && (!overlay_active || overlay_mergeable)
    {
        plan.limit.map(|k| k.saturating_add(overlay_slack))
    } else {
        None
    };
    // Small-tail merge-on-read: the unordered-LIMIT per-file path reads
    // `live_data_files()` straight from storage (it never routes through the
    // tail-merging `handle.read`), so a skipped flush would leave the un-flushed
    // tail invisible to it. Disable the pushdown for this statement so the read
    // falls into the shard `handle.read` branch, which merges the tail.
    let unordered_limit_target = if tail_merge_via_shard_read {
        None
    } else {
        unordered_limit_target
    };
    // Synthesise bounding-range predicates from IN-list predicates so the
    // Parquet row-group pruner sees a compact `[min, max]` filter rather than
    // nothing.  The actual IN-list re-check (`apply_in_list_filter` below)
    // still runs as the correctness filter; the range is strictly a superset
    // and is therefore safe.
    //
    // Why this helps: `ReadOptions.filters` is a `Vec<Predicate>` (no `In`
    // variant).  When `in_list_preds` is non-empty and `predicates` is empty,
    // the Parquet reader receives zero pushdown predicates and cannot prune any
    // row-groups inside a file, even when every queried value falls inside a
    // narrow range.  Converting `id IN (1,8,...,694)` into
    // `Gt(id, 0) AND Lt(id, 695)` lets the per-file row-group stats pruner
    // skip all row-groups whose [min,max] doesn't overlap [1,694].
    //
    // Correctness invariant: the range is a superset of the IN-list.  Any row
    // that matches the IN-list also matches the range; a row that matches the
    // range but not the IN-list is filtered out by `apply_in_list_filter`.
    // There are NO false negatives.
    //
    // Scope: only Int64 scalars (the bench PK type).  Other scalar types
    // (`Utf8`, `UInt64`) are skipped — string lex-range pruning is brittle for
    // arbitrary IN-lists and the other types are uncommon for PK lookups.  A
    // type we can't handle just produces no extra filter atoms, which is the
    // same conservative behaviour as before this change.
    let mut augmented_filters: Vec<Predicate> = plan.predicates.clone();
    for (col, vals) in &plan.in_list_preds {
        // Compute the tight [min, max] of Int64 scalars in this IN-list.
        let mut min_v: Option<i64> = None;
        let mut max_v: Option<i64> = None;
        for v in vals {
            if let ScalarValue::Int64(n) = v {
                min_v = Some(match min_v { None => *n, Some(cur) => cur.min(*n) });
                max_v = Some(match max_v { None => *n, Some(cur) => cur.max(*n) });
            }
        }
        if let (Some(lo), Some(hi)) = (min_v, max_v) {
            // Gt(col, lo-1) encodes `col >= lo`; Lt(col, hi+1) encodes `col <= hi`.
            // Use checked arithmetic to avoid wrapping at i64 boundaries.
            if let Some(lo_pred) = lo.checked_sub(1) {
                augmented_filters.push(Predicate::Gt(col.clone(), ScalarValue::Int64(lo_pred)));
            }
            if let Some(hi_pred) = hi.checked_add(1) {
                augmented_filters.push(Predicate::Lt(col.clone(), ScalarValue::Int64(hi_pred)));
            }
        }
    }
    // Sorted-skip fast path: push the cluster-column IN-list down as a
    // sorted InInt64 predicate plus the physical-sort hint, so storage can
    // binary-search each PK-sorted chunk instead of filtering every row.
    // apply_in_list_filter below remains the source of truth.
    let sorted_by_col = effective_cluster_col(&meta);
    if let Some(sc) = &sorted_by_col {
        for (col, vals) in &plan.in_list_preds {
            if col == sc {
                let mut keys: Vec<i64> = vals
                    .iter()
                    .filter_map(|v| match v {
                        ScalarValue::Int64(n) => Some(*n),
                        _ => None,
                    })
                    .collect();
                if keys.len() == vals.len() && !keys.is_empty() {
                    keys.sort_unstable();
                    keys.dedup();
                    augmented_filters.push(Predicate::InInt64(col.clone(), keys));
                }
            }
        }
    }
    let mut opts = ReadOptions {
        projection: plan.read_cols.clone(),
        filters: augmented_filters,
        partition: None,
        limit: pushdown_limit,
        row_group_selection: None,
        row_selection: None,
        sorted_by: sorted_by_col,
    };

    // ── Phase 5.14.C3: memtable point-lookup probe ───────────────────────────
    //
    // For point queries (any Eq predicate) probe the process-wide
    // `MemTableRegistry` before touching the cold tier.  A recently-inserted
    // (post-COMMIT) row lives only in the memtable until the background flush
    // drains it to Parquet — the cold-tier catalog snapshot does not know about
    // it yet.  Without this probe such rows would be invisible to point queries.
    //
    // Strategy:
    //   1. If the plan has at least one Eq predicate (point lookup), scan the
    //      memtable for this (project, table) and decode + filter all entries.
    //      In auto-commit the probe only yields matches when an Eq atom PINS
    //      the single PK (completeness guard inside `probe_memtable`): hot
    //      matches replace the cold read, which is only sound when at most one
    //      row table-wide can match. Non-PK Eq shapes fall through to the cold
    //      read + overlay merge (complete and fresh).
    //   2. Live matches → return them directly (memtable wins; no cold read).
    //   3. Tombstone present (any deleted key) AND no live matches → the cold
    //      tier may have a stale row but the deletion is definitive; still
    //      proceed to cold-tier read so the user gets accurate results.
    //      (Cold-tier rows for truly-deleted PKs would only show if a tombstone
    //      exactly corresponds to a cold key; full merge-on-read deduplication
    //      is handled by `merge_scan` in the non-fast path.)
    //   4. Not in memtable at all → fall through to cold tier (normal path).
    //
    // Non-Eq queries (range scans, aggregates, full-table reads) do NOT short-
    // circuit here: they need a full merge of hot + cold.  The DataFusion path
    // via `HtapUnionTable` handles those (registered in exec_select on COMMIT).
    // The fast path is only for point lookups (typically `WHERE pk = ?`).
    let is_point_lookup = plan
        .predicates
        .iter()
        .any(|p| matches!(p, Predicate::Eq(..)));
    if is_point_lookup && plan.aggregates.is_none() {
        if let Some((mem_rows, _has_tombstone)) =
            probe_memtable(sess, &plan.table, &plan.predicates, &meta, hot_watermark)
        {
            if !mem_rows.is_empty() {
                // Hot-tier hit: apply projection + limit and return immediately.
                // No cold-tier read required.
                let batches = mem_rows;
                let (projected_schema, batches): (Arc<Schema>, Vec<RecordBatch>) =
                    match &plan.projection {
                        None => {
                            // `SELECT *`: declared schema is the CURRENT catalog
                            // schema. A memtable row materialised before an
                            // `ALTER TABLE ADD COLUMN` carries the pre-ALTER
                            // column set, so pad each batch up to `meta.schema`
                            // (missing trailing columns become NULL) — otherwise
                            // the declared schema and the batch columns disagree
                            // and any consumer (pgwire encoder, concat) breaks.
                            let padded: Vec<RecordBatch> = batches
                                .into_iter()
                                .map(|b| {
                                    crate::hot_tombstone::pad_batch_to_schema(b, &meta.schema)
                                })
                                .collect::<Result<_>>()?;
                            (meta.schema.clone(), padded)
                        }
                        Some(items) => {
                            let has_computed = items
                                .iter()
                                .any(|it| matches!(it, ProjectionItem::Computed { .. }));
                            if has_computed {
                                // For computed projections fall through to cold (rare for hot rows).
                                // We don't want to duplicate the compute path here; just go cold.
                                // Reset and continue with cold-tier read below.
                                // (We never actually reach this return — see the `else` branch.)
                                (meta.schema.clone(), batches)
                            } else {
                                let mut idxs = Vec::with_capacity(items.len());
                                for item in items {
                                    let c = match item {
                                        ProjectionItem::Column(c) => c,
                                        ProjectionItem::Computed { .. } => unreachable!(),
                                    };
                                    let i = meta
                                        .schema
                                        .index_of(c)
                                        .map_err(|_| BasinError::UndefinedColumn(c.to_string()))?;
                                    idxs.push(i);
                                }
                                // Project each decoded memtable batch.
                                let schema = Arc::new(
                                    meta.schema
                                        .project(&idxs)
                                        .map_err(|e| BasinError::internal(format!("project schema: {e}")))?,
                                );
                                let projected: Vec<RecordBatch> = batches
                                    .into_iter()
                                    .map(|b| {
                                        // Re-project using the target indices.
                                        // `RecordBatch::project` selects columns by index.
                                        b.project(&idxs).map_err(|e| {
                                            BasinError::internal(format!("memtable project: {e}"))
                                        })
                                    })
                                    .collect::<Result<Vec<_>>>()?;
                                (schema, projected)
                            }
                        }
                    };
                // No ORDER BY + LIMIT needed for a single-row point-lookup result.
                let trimmed = match plan.limit {
                    Some(limit) => apply_limit(batches, limit),
                    None => batches,
                };
                return Ok(ExecResult::Rows {
                    schema: projected_schema,
                    batches: trimmed,
                });
            }
            // mem_rows is empty but entry exists (tombstone-only or filtered-out).
            // Fall through to cold-tier read. The post-read tombstone filter
            // installed below (see `apply_tombstone_filter_to_batches`) will
            // suppress any cold-tier rows whose PK has been fast-path deleted.
        }
        // No memtable entry for this table → fall through to cold-tier read.
    }
    // ── End Phase 5.14.C3 probe ──────────────────────────────────────────────

    // ── PK row cache context (correctness-critical; always-on) ───────────────
    //
    // Build the cache descriptor for the canonical single-PK-Eq point-lookup
    // shape. The cache is always consulted (capacity bounded by
    // `BASIN_PK_ROW_CACHE_BYTES`, default 64 MiB). We only enter this when:
    //   * the table has RLS DISABLED (`!meta.rls_enabled`) — a cached row is the
    //     RAW row, never an RLS-filtered view, so it must NEVER be served for an
    //     RLS table (bug family #159). NB: `execute_simple_select` is already
    //     only reached when `!has_rls` (executor gate), so this is defence in
    //     depth, never the sole guard;
    //   * exactly one Eq predicate on the sole PK column, no IN-list, no IS NULL,
    //     no aggregate, no computed projection — i.e. a plain `SELECT cols FROM t
    //     WHERE pk = X`;
    //   * the PK literal encodes to a `RowKey` (same encoding the cold tier +
    //     hot-tier DML use).
    //
    // The two watermarks are captured HERE (epoch + snapshot) and re-checked on
    // GET. The hot-tier epoch was already advanced by any fast-path DML; the
    // snapshot id moves on any cold-tier commit (incl. other replicas).
    //
    // Reaching this point means the memtable probe above found NO hot row for
    // this PK (it returned early on a hit) and NO tombstone forcing suppression
    // for this key — so the cold-tier read is authoritative and cacheable.
    // Under a transaction pin the PK row cache is BYPASSED entirely (no GET and
    // no INSERT). Its entries are keyed by the CURRENT hot-tier epoch + the
    // table's CURRENT-head snapshot id (captured below), not by the pinned
    // historical (snapshot, hot_watermark) view this read honours — so a hit
    // could serve a row from the live head (a concurrent commit), and an insert
    // would cache the pinned historical row under current-epoch keys and leak it
    // back to auto-commit reads. Setting `pk_cache_ctx = None` disables both the
    // GET (`pk_cache_hit`) and the INSERT downstream (both gate on this `Some`).
    let pk_cache_ctx: Option<(basin_hottier::RowKey, u64, u64, u64)> =
        if pinned.is_none()
            && !meta.rls_enabled
            && meta.pk_columns.len() == 1
            && plan.aggregates.is_none()
            && plan.in_list_preds.is_empty()
            && plan.is_null_cols.is_empty()
            && plan.predicates.len() == 1
            && plan
                .projection
                .as_ref()
                .map(|items| {
                    items
                        .iter()
                        .all(|it| matches!(it, ProjectionItem::Column(_)))
                })
                .unwrap_or(true)
        {
            let pk_col = &meta.pk_columns[0];
            match &plan.predicates[0] {
                Predicate::Eq(col, val) if col == pk_col => {
                    if let Ok(pk_idx) = meta.schema.index_of(col) {
                        let pk_dt = meta.schema.field(pk_idx).data_type().clone();
                        crate::dml_mutate::pk_scalar_to_row_key(val, &pk_dt).map(|rk| {
                            let hot_epoch = sess
                                .engine
                                .memtable_registry()
                                .hot_tier_epoch(&sess.project, &plan.table);
                            let snap = meta.current_snapshot.0;
                            let proj_hash =
                                crate::pk_row_cache::hash_read_cols(plan.read_cols.as_deref());
                            (rk, hot_epoch, snap, proj_hash)
                        })
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        };

    // PK row cache GET: on a valid dual-watermark hit, serve the cached cold-row
    // batches directly and skip the secondary-index probe, file pruning, and
    // Parquet decode entirely. The cached batches are in `read_cols` order (the
    // shape the cold read produced), exactly what the projection code below
    // expects — so a hit short-circuits straight into the shared projection.
    let pk_cache_hit: Option<Vec<RecordBatch>> = pk_cache_ctx.as_ref().and_then(
        |(rk, hot_epoch, snap, proj_hash)| {
            sess.engine
                .pk_row_cache()
                .get(&sess.project, &plan.table, rk, *hot_epoch, *snap, *proj_hash)
                .map(|arc| (*arc).clone())
        },
    );

    // ── Phase 5.7 B1: secondary index probe ─────────────────────────────────
    //
    // For Eq-predicate point queries, check if we have a loaded secondary
    // index for the queried column.  If so, restrict the live-file set to
    // only those files listed in the index for that key value.
    //
    // The probe is conservative:
    //   * If the index is not loaded, we fall through to the full file scan.
    //   * If the index is loaded but the key is absent, we know no file
    //     contains the key → return zero rows immediately.
    //   * If the index is loaded and returns some file paths, we further
    //     intersect with the catalog's live-file list (so rolled-back /
    //     deleted files are still excluded).
    //
    // The probe runs for exactly ONE Eq predicate.  If the query has multiple
    // Eq predicates we use the first indexed column found.
    // secondary_index_probe_result:
    //   None              → index not consulted / not loaded (full scan)
    //   Some(None)        → key definitively absent (return empty)
    //   Some(Some(locs))  → file allowlist + row-group map derived from locations
    struct SecondaryIndexHit {
        /// Files that may contain the key (for catalog-level file pruning).
        allowlist: std::collections::HashSet<String>,
        /// Per-file row-group allowlist for the reader (deduped).
        rg_selection: std::collections::HashMap<String, Vec<u32>>,
    }
    let secondary_index_file_allowlist: Option<Option<SecondaryIndexHit>> = {
        let registry = sess.engine.secondary_index_registry();
        let mut result: Option<Option<SecondaryIndexHit>> = None;

        // A live tombstone/update overlay makes the in-RAM secondary-index
        // allowlist unsafe to trust for pruning: a row deleted (or updated) via
        // the hot-tier overlay fast path is still physically present in the cold
        // file the index points at — the file is not rewritten until the overlay
        // drains — and this allowlist path does not consult the tombstone set.
        // Pruning to only the indexed file would then return the deleted row.
        // While an overlay is live, skip index pruning entirely and fall through
        // to the overlay-aware scan (which applies `TombstoneFilterExec`); the
        // prune re-engages O(1) once the overlay drains and the cold files +
        // index are rewritten in lockstep. Mirrors the GIN pruning guard
        // (`apply_gin_pruning_for_query`).
        let overlay_live =
            crate::session::table_has_live_overlay(&sess.engine, &sess.project, &plan.table);

        if plan.aggregates.is_none() && !overlay_live {
            for pred in &plan.predicates {
                if let Predicate::Eq(col, val) = pred {
                    // Check if the table has a declared index on this column.
                    let has_index = meta.indexes.iter().any(|idx| {
                        idx.columns.len() == 1
                            && !idx.columns[0].starts_with("expr:")
                            && idx.columns[0] == col.as_str()
                    });
                    if !has_index {
                        continue;
                    }

                    // Try to load from disk if not yet in RAM.
                    if !registry.is_loaded(&sess.project, &plan.table, col) {
                        crate::secondary_index::load_index(
                            registry,
                            &sess.engine.config().storage,
                            &sess.project,
                            &plan.table,
                            col,
                        )
                        .await;
                    }

                    // Now probe the in-RAM index for full locations.
                    if let Some(key_text) = crate::secondary_index::scalar_to_key(val) {
                        if let Some(locs) = registry.probe_locations(
                            &sess.project,
                            &plan.table,
                            col,
                            &key_text,
                        ) {
                            // Build file allowlist and per-file row-group map.
                            // Dedup same (file, rg) pairs — multiple rows in the
                            // same row group still only need one row-group entry.
                            let mut rg_map: std::collections::HashMap<String, Vec<u32>> =
                                std::collections::HashMap::new();
                            for loc in &locs {
                                let rgs = rg_map.entry(loc.file_path.clone()).or_default();
                                if !rgs.contains(&loc.row_group) {
                                    rgs.push(loc.row_group);
                                }
                            }
                            let allowlist: std::collections::HashSet<String> =
                                rg_map.keys().cloned().collect();
                            result = Some(Some(SecondaryIndexHit { allowlist, rg_selection: rg_map }));
                        }
                        // Probe MISS (`probe_locations` → `None`): the registry
                        // collapses "key indexed-then-deleted" and "key never
                        // indexed / index incomplete" into the SAME `None`, and
                        // its documented contract is that callers MUST treat
                        // `None` as "unknown — fall through to full scan". The
                        // in-RAM B-tree index can legitimately be INCOMPLETE for
                        // a present key: it is built fire-and-forget by the
                        // auto-index advisor (and by `CREATE INDEX`) over a point-
                        // in-time file set, is FIFO-evicted past
                        // `MAX_INDEX_ENTRIES_PER_COL`, and does not retroactively
                        // cover rows written before it existed. The previous
                        // `result = Some(None)` ("definitely empty") short-circuit
                        // therefore DROPPED live rows whenever a queried key was
                        // simply not (yet) in the index — e.g. a point read of a
                        // freshly-inserted non-PK value after the advisor fired
                        // (`prepared_select_point_reads`). We never set a negative
                        // result here: a miss leaves `result = None`, falling
                        // through to the zone-map/bloom prune + full cold read,
                        // which is always correct. The POSITIVE allowlist above
                        // (a HIT) is still used only to PRUNE, and the existing
                        // per-file re-check filters any superset.
                    }
                    break; // Only use one index column per query.
                }
            }
        }
        result
    };

    // Wire the row-group selection from the secondary index probe into the
    // ReadOptions so the Parquet reader can skip non-matching row groups.
    if let Some(Some(ref hit)) = secondary_index_file_allowlist {
        opts.row_group_selection = Some(hit.rg_selection.clone());
    }

    // If the secondary index says the key is definitely absent, return empty
    // without opening any files.
    if matches!(secondary_index_file_allowlist, Some(None)) {
        let output_schema = match &plan.projection {
            None => meta.schema.clone(),
            Some(items) => {
                let idxs: Result<Vec<usize>, _> = items
                    .iter()
                    .filter_map(|item| match item {
                        ProjectionItem::Column(c) => Some(meta.schema.index_of(c)),
                        _ => None,
                    })
                    .collect();
                match idxs {
                    Ok(ix) => Arc::new(
                        meta.schema
                            .project(&ix)
                            .unwrap_or_else(|_| meta.schema.as_ref().clone()),
                    ),
                    Err(_) => meta.schema.clone(),
                }
            }
        };
        return Ok(crate::ExecResult::Rows {
            schema: output_schema,
            batches: vec![],
        });
    }

    // ── PK probe: bloom + zone-map file prune for `WHERE pk = <lit>` and
    //    `WHERE pk IN (v1, …, vN)` ──────────────────────────────────────────
    //
    // The dominant cost of a point lookup (or a small IN-list batch fetch) is
    // opening every live file body when most of them cannot contain any of the
    // queried keys.  When the WHERE clause is a single-column PK equality OR a
    // non-negated IN-list on the sole PK column we can decide which files to
    // open using catalog metadata alone (per-file zone-map + bloom).  A
    // definitive miss (every live file pruned) returns zero rows immediately.
    //
    // Gating (both shapes):
    //   * single-column PK (meta.pk_columns.len() == 1)
    //   * no aggregate, no IS NULL atoms
    //   * for Eq:     exactly one Predicate::Eq on the PK column, no IN-list
    //   * for IN-list: exactly one in_list_preds entry on the PK column,
    //                  no other Predicate atoms
    //
    // Shard correctness: `shard.flush_to_parquet()` ran above (line ~1077) so
    // the shard in-RAM tail is drained to cold-tier Parquet; the engine
    // `MemTableRegistry` was probed above. Both hot caches are clear, so a
    // cold-tier prune is sound. When the probe yields candidates the shard
    // branch reads those paths directly via `storage.read_paths_with_schema`
    // (bypassing `handle.read`'s full file discovery).
    //
    // Correctness: the probe is conservative — it returns a superset.  The
    // per-row predicate (Eq equality or the IN post-read filter) re-checks
    // every surviving row so no false negatives escape.

    /// Build the empty-result response for a definitive all-files-pruned miss.
    fn empty_probe_result(
        plan: &SimpleSelectPlan,
        meta: &TableMetadata,
    ) -> ExecResult {
        let output_schema = match &plan.projection {
            None => meta.schema.clone(),
            Some(items) => {
                let idxs: std::result::Result<Vec<usize>, _> = items
                    .iter()
                    .filter_map(|item| match item {
                        ProjectionItem::Column(c) => Some(meta.schema.index_of(c)),
                        _ => None,
                    })
                    .collect();
                match idxs {
                    Ok(ix) => Arc::new(
                        meta.schema
                            .project(&ix)
                            .unwrap_or_else(|_| meta.schema.as_ref().clone()),
                    ),
                    Err(_) => meta.schema.clone(),
                }
            }
        };
        ExecResult::Rows {
            schema: output_schema,
            batches: vec![],
        }
    }

    // Build the catalog-driven live file list. `live_data_files()` replays the
    // snapshot chain up to `current_snapshot`, so after a rollback it returns
    // only the pre-rollback files — physical files from post-rollback snapshots
    // are never included (bug #41 fix). Computed ONCE here and reused by the
    // PK probes and the `live_paths` build below: every call clones each
    // DataFileRef (including bloom-filter blobs), and `meta` is not reloaded
    // between these uses so one snapshot replay is authoritative for all of them.
    let live_files = meta.live_data_files();

    // ── Deep top-K late materialization ──────────────────────────────────────
    //
    // `SELECT … FROM t ORDER BY k [DESC] [, pk] LIMIT n` over a wide single-PK
    // table. The existing path below decodes EVERY column of EVERY row, then
    // sorts + limits — paying the full wide-row decode for the (rows − n) losers
    // that never appear in the result. Instead: phase 1 decodes only
    // `[k, pk]` (+ WHERE columns) to find the winning n PKs with file-level
    // min/max skipping, phase 2 fetches the full rows for just those n PKs.
    //
    // Eligibility is confirmed inside `try_topk_late_materialize` (the syntactic
    // shape was recognised in `match_query`); ANY runtime ineligibility — live
    // overlay, an un-flushed tail to merge, a pinned read, the identity column
    // not being the single PK, a non-Int64 PK — returns `Ok(None)` and falls
    // through to the existing decode-everything path UNCHANGED. A two-column
    // `ORDER BY k, pk` that the branch declines falls through to DataFusion (the
    // single-column `apply_order_by_limit` below cannot honour the tie-break),
    // via the gate that set `topk_late` only when the shape is fully handled.
    if plan.topk_late.is_some() {
        if let Some(res) = try_topk_late_materialize(
            sess,
            &plan,
            &meta,
            &live_files,
            overlay_active,
            tail_merge_via_shard_read,
            pinned.is_some(),
            raw_sql,
            include_deleted,
        )
        .await?
        {
            return Ok(res);
        }
    }

    let pk_probe_paths: Option<Vec<object_store::path::Path>> = if
        // PK row cache hit: the cached batches already answer this point query;
        // skip the zone-map + bloom probe (and its possible Absent early-return)
        // entirely so the warm path is a pure in-RAM HashMap lookup. The cache's
        // valid watermark guarantees the cold files are unchanged, so the probe
        // would only re-derive what we already hold.
        pk_cache_hit.is_some() {
        None
    } else if
        // Small-tail merge-on-read: the PK point-probe prunes to cold-file
        // candidates AND can EARLY-RETURN an empty result when its bloom finds
        // the key in no cold file. With the flush skipped, a just-INSERTed PK
        // can live only in the un-flushed tail — which the probe's cold blooms
        // do not see — so this gate is disabled when we are merging the tail on
        // read. The read then routes through the tail-merging `handle.read`,
        // preserving read-own-write for tail-only PKs.
        tail_merge_via_shard_read {
        None
    } else if
        plan.aggregates.is_none()
        && plan.is_null_cols.is_empty()
        && meta.pk_columns.len() == 1
    {
        let pk_col = &meta.pk_columns[0];

        // ── Single Eq probe (`WHERE pk = <lit>`) ────────────────────────────
        if plan.predicates.len() == 1
            && plan.in_list_preds.is_empty()
        {
            if let Predicate::Eq(col, val) = &plan.predicates[0] {
                if col == pk_col {
                    match crate::index_probe::pk_point_probe(
                        col,
                        val,
                        &live_files,
                        meta.schema.as_ref(),
                    ) {
                        crate::index_probe::PkProbeOutcome::Absent { files_pruned } => {
                            for _ in 0..files_pruned {
                                sess.engine.note_bloom_skipped();
                            }
                            return Ok(empty_probe_result(&plan, &meta));
                        }
                        crate::index_probe::PkProbeOutcome::Candidates {
                            paths,
                            files_pruned,
                        } => {
                            for _ in 0..files_pruned {
                                sess.engine.note_bloom_skipped();
                            }
                            Some(paths)
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        // ── IN-list probe (`WHERE pk IN (v1, …, vN)`) ───────────────────────
        //
        // Gating: exactly one IN-list predicate on the sole PK column, no
        // other Predicate atoms.  An empty IN-list was already turned into
        // `always_empty = true` in the parser so we cannot reach here with
        // an empty value vector.
        else if plan.predicates.is_empty()
            && plan.in_list_preds.len() == 1
            && plan.in_list_preds[0].0 == *pk_col
        {
            let vals = &plan.in_list_preds[0].1;
            match crate::index_probe::pk_point_probe_multi(
                pk_col,
                vals,
                &live_files,
                meta.schema.as_ref(),
            ) {
                crate::index_probe::PkProbeOutcome::Absent { files_pruned } => {
                    for _ in 0..files_pruned {
                        sess.engine.note_bloom_skipped();
                    }
                    return Ok(empty_probe_result(&plan, &meta));
                }
                crate::index_probe::PkProbeOutcome::Candidates {
                    paths,
                    files_pruned,
                } => {
                    for _ in 0..files_pruned {
                        sess.engine.note_bloom_skipped();
                    }
                    Some(paths)
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // ── Keyset ordered-traversal zone maps ──────────────────────────────────
    //
    // For the keyset per-file-LIMIT branch below, capture every live file's
    // decoded `[min, max]` Int64 zone map for the keyset (ORDER BY / cluster)
    // column BEFORE `live_files` is consumed by the `live_paths` build. The
    // map is keyed by the same `object_store::path::Path` the `live_paths`
    // build produces (`Path::from(f.path.as_str())`), so the keyset branch can
    // re-associate each SURVIVING path (the general zone-map prune below
    // already dropped every file whose `max <= cursor`) with its PK range and
    // then open candidates in ascending-min order, short-circuiting once the
    // page is provably complete.
    //
    // `None` whenever the shape doesn't apply (no keyset pushdown), the
    // keyset column is not Int64, or ANY live file lacks a decodable Int64
    // min/max — the branch then keeps the existing open-all-candidates
    // behaviour, which is always correct.
    let keyset_zone_maps: Option<
        std::collections::HashMap<object_store::path::Path, (i64, i64)>,
    > = match (keyset_per_file_limit, plan.order_by.as_ref()) {
        (Some(_), Some((kcol, _)))
            if meta
                .schema
                .field_with_name(kcol)
                .map(|f| matches!(f.data_type(), arrow_schema::DataType::Int64))
                .unwrap_or(false) =>
        {
            let mut m = std::collections::HashMap::with_capacity(live_files.len());
            let mut complete = true;
            for f in &live_files {
                let cs = f.column_stats.get(kcol.as_str());
                let mn = cs.and_then(|c| decode_stat_i64(c.min_bytes.as_deref()));
                let mx = cs.and_then(|c| decode_stat_i64(c.max_bytes.as_deref()));
                match (mn, mx) {
                    (Some(mn), Some(mx)) => {
                        m.insert(object_store::path::Path::from(f.path.as_str()), (mn, mx));
                    }
                    _ => {
                        complete = false;
                        break;
                    }
                }
            }
            complete.then_some(m)
        }
        _ => None,
    };

    // Convert each live file's path string to an `ObjectPath` for
    // `storage.read_paths`.
    // Catalog-stats file pruning: skip any data file whose per-file
    // column_stats (min/max/null-count, populated at write time for BOTH
    // Parquet and Vortex) prove the predicate cannot match a single row
    // (`PruneOutcome::NoMatch`). Done BEFORE `read_paths` so a pruned file
    // is never opened — decisive for Vortex, whose per-file open is far
    // heavier than a Parquet footer read, and a win for Parquet too
    // (point/range/compound/IS NULL queries touch fewer files).
    let had_pk_probe = pk_probe_paths.is_some();
    let live_paths: Vec<object_store::path::Path> = if let Some(paths) = pk_probe_paths {
        // The PK point-probe already pruned the live set to its candidates
        // (zone-map + catalog bloom) for the single-PK-Eq shape; reuse them
        // directly and skip the general per-file prune loop.
        paths
    } else if plan.predicates.is_empty() && plan.is_null_cols.is_empty() {
        live_files
            .into_iter()
            .map(|f| object_store::path::Path::from(f.path.as_str()))
            .collect()
    } else {
        // Build a compound AND predicate for catalog-level file pruning. The
        // compound predicate includes both comparison atoms and IS NULL checks.
        // `CompoundPredicate::IsNull` can prune files where null_count == 0.
        let mut cp_children: Vec<CompoundPredicate> = plan
            .predicates
            .iter()
            .map(|p| CompoundPredicate::Atom(p.clone()))
            .chain(
                plan.is_null_cols
                    .iter()
                    .map(|c| CompoundPredicate::IsNull(c.clone())),
            )
            .collect();
        let cp = if cp_children.len() == 1 {
            cp_children.pop().unwrap()
        } else {
            CompoundPredicate::And(cp_children)
        };
        let schema = meta.schema.as_ref();
        live_files
            .into_iter()
            .filter(|f| {
                // Phase 5.7 B1: secondary index allowlist pruning.
                // If the index has a definitive answer for which files contain
                // the queried key, skip any file NOT in that set.
                if let Some(Some(ref hit)) = secondary_index_file_allowlist {
                    if !hit.allowlist.contains(f.path.as_str()) {
                        sess.engine.note_secondary_index_skipped();
                        return false;
                    }
                }

                // Min/max catalog-stats pruning — the existing pass.
                if matches!(
                    evaluate_compound_for_pruning(&cp, &f.column_stats, schema, f.row_count),
                    PruneOutcome::NoMatch
                ) {
                    return false;
                }
                // Phase 5.14.A3 — bloom probe: for each Eq predicate atom,
                // if the file carries a bloom filter for the column, probe it.
                // A definitive miss (contains == false) lets us skip the file
                // without opening it. False positives fall through as normal.
                // A bloom result of true (may-contain) is not sufficient to
                // KEEP the file on its own — we only use it to SKIP; the
                // existing min/max check already passed, so we keep the file
                // unless a bloom says "definitely absent".
                for pred in &plan.predicates {
                    if let Predicate::Eq(col, val) = pred {
                        if let Some(bloom_bytes) = f.bloom_filters.get(col.as_str()) {
                            if let Some(filter) = bloom_from_bytes(bloom_bytes) {
                                let absent = match val {
                                    ScalarValue::Int64(v) => {
                                        let bytes = v.to_le_bytes();
                                        !filter.contains(bytes.as_ref())
                                    }
                                    ScalarValue::Utf8(s) => {
                                        !filter.contains(s.as_bytes())
                                    }
                                    // Other types: no bloom encoding defined —
                                    // fall through (do not prune).
                                    _ => false,
                                };
                                if absent {
                                    sess.engine.note_bloom_skipped();
                                    return false;
                                }
                            }
                        }
                    }
                }
                true
            })
            .map(|f| object_store::path::Path::from(f.path.as_str()))
            .collect()
    };

    // PK row cache HIT short-circuit: when the dual-watermark GET above
    // succeeded we already hold the post-overlay cold-row batches (in
    // `read_cols` order). Skip the cold read AND the hot-overlay below — a
    // valid hot_epoch watermark means the memtable is byte-for-byte unchanged
    // since the entry was cached, so re-applying tombstone/update overlay would
    // be a no-op anyway. Fall straight into the shared projection.
    let pk_cache_served = pk_cache_hit.is_some();
    let batches = if let Some(cached) = pk_cache_hit {
        cached
    } else if let Some(per_file_limit) = keyset_per_file_limit {
        // ── Keyset-pagination read: per-file LIMIT, no global cut ────────────
        //
        // Each candidate cold file is read with its OWN `LIMIT per_file_limit`.
        // We deliberately do NOT use the multi-path `read_paths_with_schema`
        // call with a single `opts.limit`, because that combinator applies the
        // limit GLOBALLY across the path stream (`apply_limit_to_stream`) — it
        // would stop after the first `per_file_limit` rows total and never open
        // the later files, dropping rows the global sort needs.  Issuing one
        // single-path `read_paths_with_schema` call per file makes the stream
        // limit coincide with the per-file limit (one path ⇒ global == per-file)
        // while still threading the catalog schema for schema-evolution
        // null-fill.  Total rows read ≤ n_files × per_file_limit; the head of
        // each ASC-sorted file is a superset of its top-`limit` contribution, so
        // `apply_order_by_limit` below produces the exact global top-`limit`.
        //
        // Shard correctness mirrors the `had_pk_probe` branch: the top-of-fn
        // `flush_to_parquet()` drained the shard tail to cold files and the
        // engine memtable was probed, so reading `live_paths` directly (instead
        // of `handle.read`) is sound. Live tombstone / UPDATE overlays are
        // handled by the merge-on-read step below the branch — the per-file
        // limit already carries `overlay_slack` extra rows so suppression
        // cannot leave a shortfall (see the eligibility guard note above).
        sess.engine.note_keyset_fast_select();
        use futures::StreamExt;
        let mut keyset_opts = opts.clone();
        keyset_opts.limit = Some(per_file_limit);
        let project = sess.project;
        let storage = &sess.engine.config().storage;

        // ── Ordered short-circuit traversal (disjoint PK layouts) ────────────
        //
        // Statement-affine striping + stripe-merge compaction produce cold
        // files whose PK ranges are pairwise DISJOINT (each flush cycle writes
        // one PK-sorted file covering its own id band). For such layouts the
        // global top-`per_file_limit` of `k > cursor ORDER BY k ASC` lives in
        // the FIRST candidate file in ascending-min order, plus at most its
        // successors when the page straddles a file boundary — opening every
        // candidate (the fan-out below) reads `n_files × limit` rows when 1-2
        // files suffice. So: when every candidate carries a decoded Int64 zone
        // map AND the sorted ranges are pairwise disjoint, open candidates one
        // at a time in ascending-min order and STOP as soon as
        //
        //   (a) at least `per_file_limit` (= limit + offset) keyset keys have
        //       been collected, and
        //   (b) the NEXT file's min is STRICTLY greater than the
        //       `per_file_limit`-th smallest key collected so far —
        //
        // every row in the next (and all later, min-sorted, disjoint) files
        // has key ≥ that min > kth, so none can displace a collected row from
        // the global top-`per_file_limit`; `apply_order_by_limit` below then
        // produces the exact page. Ties stop nothing (`>` not `≥`): a later
        // file whose min EQUALS the kth key may still contribute, so we keep
        // reading. Sequential awaits are fine here precisely because the
        // short-circuit bounds the traversal at 1-2 files; layouts that fail
        // the disjointness check (legacy round-robin stripes where every file
        // spans the whole PK range) keep the bounded-concurrency fan-out
        // below, where overlap means most files genuinely contribute.
        //
        // Hot-tier correctness is untouched: this prunes/short-circuits the
        // COLD file set only. The shard tail was flushed to cold files at the
        // top of the fn (so those rows ARE in `live_paths`/zone maps). Live
        // tombstone/UPDATE overlays are tolerated: `per_file_limit` carries
        // `overlay_slack` extra rows, so even though the collected `keys`
        // include rows the merge below may suppress, at least
        // `limit + offset` of the `per_file_limit` smallest collected keys
        // survive — the stop test `next_min > kth(per_file_limit)` therefore
        // still proves no later file can contribute to the surviving
        // top-`(limit+offset)`. Appended override rows enter the final sort
        // independent of this traversal.
        let ordered_disjoint: Option<Vec<(object_store::path::Path, i64, i64)>> =
            keyset_zone_maps.as_ref().and_then(|zm| {
                let mut v: Vec<(object_store::path::Path, i64, i64)> =
                    Vec::with_capacity(live_paths.len());
                for p in &live_paths {
                    let (mn, mx) = zm.get(p)?;
                    v.push((p.clone(), *mn, *mx));
                }
                v.sort_by_key(|(_, mn, mx)| (*mn, *mx));
                v.windows(2)
                    .all(|w| w[0].2 < w[1].1)
                    .then_some(v)
            });
        if let Some(files) = ordered_disjoint {
            use arrow_array::Array;
            let (kcol, _) = plan
                .order_by
                .as_ref()
                .expect("keyset_per_file_limit implies order_by");
            let mut all: Vec<RecordBatch> = Vec::new();
            // Keyset-column values collected so far (already `> cursor` —
            // storage applies the pushed Gt filter before its stream limit).
            let mut keys: Vec<i64> = Vec::new();
            // Conservative kill-switch: if any batch refuses to yield Int64
            // keyset keys (missing column / unexpected type / nulls), stop
            // short-circuiting and read every remaining candidate — slower,
            // never wrong.
            let mut keys_complete = true;
            for (i, (path, _mn, _mx)) in files.iter().enumerate() {
                let stream = storage
                    .read_paths_with_schema(
                        &project,
                        vec![path.clone()],
                        keyset_opts.clone(),
                        Some(meta.schema.clone()),
                    )
                    .await?;
                let collected: Vec<Result<RecordBatch>> = stream.collect().await;
                let file_batches = collected.into_iter().collect::<Result<Vec<_>>>()?;
                for b in &file_batches {
                    match b
                        .schema()
                        .index_of(kcol)
                        .ok()
                        .and_then(|ci| {
                            b.column(ci)
                                .as_any()
                                .downcast_ref::<arrow_array::Int64Array>()
                                .cloned()
                        }) {
                        Some(arr) if arr.null_count() == 0 => {
                            keys.extend(arr.values().iter().copied());
                        }
                        _ => keys_complete = false,
                    }
                }
                all.extend(file_batches);
                if keys_complete && per_file_limit > 0 && keys.len() >= per_file_limit {
                    match files.get(i + 1) {
                        None => break, // last file — nothing left to skip
                        Some((_, next_min, _)) => {
                            // kth = the per_file_limit-th smallest collected
                            // key. The collected set is tiny (≤ files_opened ×
                            // per_file_limit), so a select_nth on a scratch
                            // copy is cheap.
                            let mut scratch = keys.clone();
                            let (_, kth, _) =
                                scratch.select_nth_unstable(per_file_limit - 1);
                            if *next_min > *kth {
                                break;
                            }
                        }
                    }
                }
            }
            all
        } else {
            // Drive the per-file reads with bounded concurrency rather than one
            // fully-awaited file at a time.  Each file is still its OWN single-path
            // `read_paths_with_schema` call, so its stream-level limit coincides
            // with the per-file limit (one path ⇒ global == per-file) — the
            // superset invariant the branch relies on is untouched.  The earlier
            // sequential `for path { … .await }` loop serialised all file
            // setups+awaits: at the 16-file 1M layout that linear chain was the
            // keyset latency regression (8-file ≈ 5-7ms → 16-file ≈ 20ms).
            // `buffered(KEYSET_READ_CONCURRENCY)` overlaps them exactly as the
            // non-keyset cold path's own `buffered(4)` does, and `buffered`
            // preserves input order so the collected batches stay in `live_paths`
            // order (irrelevant to correctness — `apply_order_by_limit` re-sorts —
            // but keeps the merge input deterministic).
            const KEYSET_READ_CONCURRENCY: usize = 4;
            let per_file: Vec<Result<Vec<RecordBatch>>> = futures::stream::iter(live_paths)
                .map(|path| {
                    let keyset_opts = keyset_opts.clone();
                    let schema = Some(meta.schema.clone());
                    async move {
                        let stream = storage
                            .read_paths_with_schema(&project, vec![path], keyset_opts, schema)
                            .await?;
                        let collected: Vec<Result<RecordBatch>> = stream.collect().await;
                        collected.into_iter().collect::<Result<Vec<_>>>()
                    }
                })
                .buffered(KEYSET_READ_CONCURRENCY)
                .collect()
                .await;
            let mut all: Vec<RecordBatch> = Vec::new();
            for file_batches in per_file {
                all.extend(file_batches?);
            }
            all
        }
    } else if unordered_limit_target.is_some()
        && !had_pk_probe
        && !shadow_cols_present
        && pinned.is_none()
        && !live_paths.is_empty()
    {
        // ── Unordered-LIMIT early exit: one file at a time, stop at target ───
        //
        // Shape: `SELECT … FROM t [WHERE cheap-pushdown-preds] LIMIT k` with NO
        // ORDER BY. `unordered_limit_target` is `Some` only when there is no
        // ORDER BY, no aggregate, and no IS NULL / IN-list post-filter. With
        // no live overlay the target is exactly `k` and every row the storage
        // read emits is a final result row. With a live (mergeable) overlay
        // the target is `k + overlay_slack`: suppression below removes at
        // most `overlay_slack` of the collected rows (one per overlay key),
        // so at least `k` survive whenever `k` matching cold rows exist in
        // the traversed prefix — and the appended override rows only add
        // matches. `apply_limit` trims the merged result to exactly `k`.
        //
        // Why not the multi-path read below: `read_paths_with_schema` over ALL
        // candidate paths drives its per-file opens through `buffered(4)`, and
        // `read_one` is EAGER per file (whole-blob GET + full/chunk-pruned
        // decode before the first batch is emitted). The global
        // `apply_limit_to_stream` cut therefore lands only after several files
        // have already been fetched + decoded in full — at a 1M-row layout
        // that is hundreds of thousands of decoded rows for a LIMIT 100. PG
        // early-exits after k matching rows; this branch does the same: open
        // candidate files ONE AT A TIME, push the REMAINING limit into each
        // single-path read (one path ⇒ the storage stream limit coincides with
        // the per-file limit — the same mechanics the keyset branch uses), and
        // stop as soon as `k` post-filter rows have been collected. Work is
        // bounded by ceil(k / matches-per-file) file opens instead of the
        // whole candidate set.
        //
        // Correctness gates (ANY doubt ⇒ this branch is skipped and today's
        // path runs unchanged):
        //   * Overlay: tolerated via the `overlay_slack` inflation baked into
        //     `unordered_limit_target` (see its definition); a NON-mergeable
        //     overlay (multi-column PK — never produced today) keeps the
        //     target `None` and skips the branch.
        //   * Hot tail: `shard.flush_to_parquet()` ran at the top of this fn
        //     BEFORE `meta`/`live_files` were loaded, so every pre-statement
        //     tail row is in `live_paths`. Rows arriving mid-statement are
        //     invisible, exactly like the keyset / pk-probe / pinned branches
        //     that already read `live_paths` directly instead of `handle.read`.
        //   * Hot memtable: any-Eq point shapes were probed above (a hit
        //     returned early); non-probe shapes never saw memtable rows on
        //     this path before either (only the overlay merge below, which
        //     appends/suppresses from the snapshots captured above) — no new
        //     under-count.
        //   * `had_pk_probe` / `shadow_cols_present` / `pinned` keep their own
        //     dedicated branch below; `LIMIT 0` collects nothing and returns
        //     empty (same answer `apply_limit` would produce).
        sess.engine.note_unordered_limit_fast_select();
        use futures::StreamExt;
        let k = unordered_limit_target.expect("guard checked unordered_limit_target.is_some()");
        let storage = &sess.engine.config().storage;
        let mut all: Vec<RecordBatch> = Vec::new();
        let mut collected: usize = 0;
        for path in &live_paths {
            if collected >= k {
                break;
            }
            // Remaining-limit pushdown: the single-path stream slices its
            // boundary batch to exactly `k - collected` rows and stops, so
            // `collected` can never overshoot `k` (the slack-inflated target;
            // `apply_limit` below the overlay merge trims to the user LIMIT).
            let mut file_opts = opts.clone();
            file_opts.limit = Some(k - collected);
            let stream = storage
                .read_paths_with_schema(
                    &sess.project,
                    vec![path.clone()],
                    file_opts,
                    Some(meta.schema.clone()),
                )
                .await?;
            let file_batches: Vec<Result<RecordBatch>> = stream.collect().await;
            for b in file_batches {
                let b = b?;
                if b.num_rows() > 0 {
                    collected += b.num_rows();
                    all.push(b);
                }
            }
        }
        all
    } else if had_pk_probe || shadow_cols_present || pinned.is_some() {
        // Direct cold-file read (bypass `handle.read`). Three cases reach here:
        //
        //   * `had_pk_probe`: the single-PK-Eq probe already narrowed the cold
        //     tier to a handful of candidate files; reading them directly skips
        //     `handle.read`'s full file discovery, which would re-list every
        //     live file and defeat the prune.
        //   * `shadow_cols_present`: the query references a promoted JSONB
        //     shadow column (`__promoted$…`). `handle.read` re-derives its read
        //     schema from the catalog, which deliberately EXCLUDES shadow
        //     columns, so it would never decode the shadow column and the
        //     downstream predicate/projection would fail with "predicate column
        //     missing". Reading the cold files directly threads the extended
        //     `meta.schema` (which carries the shadow column) so the column is
        //     decoded. The shadow-column correctness guard above already proved
        //     no hot-tier / un-flushed-tail rows exist for this table, so the
        //     cold files are the authoritative complete row set.
        //   * `pinned.is_some()` (in-tx repeatable read): SNAPSHOT-CORRECTNESS,
        //     not perf. `handle.read` discovers the shard's CURRENT live file
        //     set + in-RAM tail (the moved head), so it would re-admit a
        //     concurrent committer's rows that the top-of-fn flush pushed to a
        //     HIGHER catalog snapshot than the pin — the snapshot_isolation
        //     leak (e.g. a pinned `COUNT(*)` counting +1 for B's row). `meta`
        //     was reloaded at `pinned.snapshot`, so `live_files`/`live_paths`
        //     here are exactly the pinned historical file set; reading those
        //     paths directly (then applying the watermark-filtered hot overlay
        //     below) yields the pinned view. The top-of-fn `flush_to_parquet()`
        //     already drained the tail into cold files before `live_files` was
        //     consumed, so no pinned-snapshot row is still tail-only.
        //
        // Safety (shared):
        //   * Hot tier covered: the engine's `MemTableRegistry` was probed
        //     above (line ~1209); a hit returned early. Reaching here means
        //     the queried PK is NOT in the engine memtable.
        //   * Shard tail drained: `shard.flush_to_parquet()` ran above
        //     (line ~1077), so the shard's in-RAM `state.tail` was flushed
        //     to cold-tier Parquet before the probe consumed `live_files`.
        //   * Tombstone overlay applied below as on the non-shard path (and,
        //     under a pin, filtered by the hot-seq watermark).
        if live_paths.is_empty() {
            vec![]
        } else {
            use futures::StreamExt;
            let schema = Some(meta.schema.clone());
            let stream = sess
                .engine
                .config()
                .storage
                .read_paths_with_schema(&sess.project, live_paths, opts, schema)
                .await?;
            let collected: Vec<Result<RecordBatch>> = stream.collect().await;
            collected.into_iter().collect::<Result<Vec<_>>>()?
        }
    } else if let Some(shard) = sess.engine.config().shard.as_ref() {
        // Shard path: this read merges the in-RAM tail with the Parquet base.
        if tail_merge_via_shard_read {
            // Small-tail merge-on-read: the flush was SKIPPED above, so the
            // un-flushed tail is still resident — and statement-affine striping
            // spreads it across the `s1`, `s2`, … partitions, not only
            // `default_key()`. A single-partition `handle.read` would miss the
            // striped tails (the keyset-seam / read-own-write loss), so merge
            // EVERY resident partition's tail over the (project, table)-scoped
            // cold base.
            shard
                .read_table_merging_tails(&sess.project, &plan.table, opts)
                .await?
        } else {
            // Normal path: the pre-flush above already drained every partition's
            // tail into Parquet, so the default-partition `handle.read` only has
            // to scan whatever new tail rows have arrived since. Await directly:
            // the shard's `handle.read` drives its own async I/O / WAL-replay
            // cooperatively; nesting a runtime would re-create the fast_select
            // livelock the non-shard heavy path documents.
            let handle = shard
                .get(&sess.project, &PartitionKey::default_key())
                .await?;
            handle.read(&plan.table, opts).await?
        }
    } else if live_paths.is_empty() {
        // No live files at this snapshot — return an empty batch set rather
        // than listing the directory (which would see rolled-back files).
        vec![]
    } else {
        // Non-shard path (both heavy and light scans).
        //
        // We previously wrapped heavy scans in `run_blocking` (spawn_blocking +
        // a nested current_thread runtime) to "keep Parquet decode off the
        // cooperative worker pool". That pattern is the source of the
        // fast_select livelock: when four concurrent noisy tasks each call
        // `run_blocking`, their nested `current_thread` runtimes issue their
        // own `spawn_blocking` calls for LocalFileSystem I/O via
        // `object_store::maybe_spawn_blocking`. Under load this creates a
        // deadlock triangle:
        //
        //   outer task → outer spawn_blocking (blocks worker) →
        //   inner runtime → inner spawn_blocking (saturates pool) →
        //   inner blocking tasks wait for OS-file wakeups, but no worker is
        //   free to drive the inner reactor → eternal stall.
        //
        // The correct fix: `read_paths_with_schema` already drives I/O
        // cooperatively (the stream is `buffered(4)`, so at most 4 files
        // are fetched concurrently and the future yields between completions).
        // The Parquet decode CPU is already dispatched to `spawn_blocking`
        // tasks *inside* the parquet reader — there is no CPU-pinning on the
        // cooperative worker. Awaiting the stream directly is both correct and
        // deadlock-free.
        use futures::StreamExt;
        let schema = Some(meta.schema.clone());
        let stream = sess
            .engine
            .config()
            .storage
            .read_paths_with_schema(&sess.project, live_paths, opts, schema)
            .await?;
        let collected: Vec<Result<RecordBatch>> = stream.collect().await;
        collected.into_iter().collect::<Result<Vec<_>>>()?
    };

    // Merge-on-read tombstone suppression for the DELETE hot-tier fast path.
    //
    // `dml_mutate::exec_delete` can shortcut a DELETE by writing
    // `MemRowValue::Tombstone` entries into the process-wide
    // `MemTableRegistry` and skipping the cold-tier copy-on-write rewrite.
    // The cold-tier file we just read above therefore may still carry rows
    // whose PK has been tombstoned by a more recent fast-path DELETE. Drop
    // those rows here, mirroring the `TombstoneFilterExec` wrap that
    // `session::wrap_with_tombstone_filter` installs on the DataFusion path.
    //
    // Happy path: `snapshot_tombstones` returns an empty set (the common case
    // for tables that have not been touched by a fast-path DELETE), so the
    // helper is a noop and the batches pass through unchanged.
    let batches = if pk_cache_served {
        // A cache hit already returned post-overlay batches; the valid
        // hot_epoch watermark proves the memtable is unchanged, so re-running
        // the overlay would be a no-op. Skip it.
        batches
    } else if meta.pk_columns.len() == 1 {
        let pk_col = &meta.pk_columns[0];
        // Reuse the overlay snapshots captured at the top of the statement
        // (before the limit-pushdown decisions). This keeps the keyset /
        // unordered-LIMIT `overlay_slack` bound exact — the merge applied
        // here can never be LARGER than the snapshot the slack was sized
        // from — and makes the statement's overlay view a single point in
        // time (an overlay write landing mid-read is not part of this
        // statement, exactly like a cold row landing mid-scan). Under a pin
        // the snapshots were already watermark-filtered (post-pin
        // tombstones/overrides dropped so a concurrent DELETE/UPDATE does
        // NOT suppress/override this tx's pinned cold row). The UPDATE
        // overlay merge mirrors the `UpdateOverlayExec` wrap on the
        // DataFusion read path so a fast-path UPDATE is visible to bulk
        // fast-path SELECTs too.
        let tombs = overlay_tombs;
        let updates = overlay_updates;
        if tombs.is_empty() && updates.is_empty() {
            batches
        } else if let Ok(pk_idx) = meta.schema.index_of(pk_col) {
            let pk_dt = meta.schema.field(pk_idx).data_type().clone();
            let batches = crate::hot_tombstone::apply_tombstone_filter_to_batches(
                batches, &tombs, pk_col, &pk_dt,
            )
            .map_err(|e| BasinError::internal(format!("tombstone filter: {e}")))?;
            crate::hot_tombstone::apply_update_overlay_to_batches(
                batches, &updates, pk_col, &pk_dt, &plan.predicates,
            )
            .map_err(|e| BasinError::internal(format!("update overlay: {e}")))?
        } else {
            batches
        }
    } else {
        batches
    };

    // PK row cache POPULATE (miss path only): we have the authoritative
    // post-overlay cold-row batches in `read_cols` order. For the cacheable
    // shape there are no IS NULL / IN-list post-filters (gated out when
    // building `pk_cache_ctx`), so these batches are the final per-PK cold-row
    // content. Store them with the two watermarks captured BEFORE the read —
    // capturing pre-read is conservative: if a concurrent DML advanced the
    // epoch/snapshot during our read, the stored watermark is older than the
    // live one, so the very next GET sees a mismatch and refuses the entry
    // (no stale serve). We only cache 0- or 1-row results (a PK point lookup
    // yields at most one row); a multi-row result would indicate a non-PK
    // shape and is left uncached.
    if !pk_cache_served {
        if let Some((rk, hot_epoch, snap, proj_hash)) = pk_cache_ctx {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            if row_count <= 1 {
                sess.engine.pk_row_cache().insert(
                    &sess.project,
                    &plan.table,
                    rk,
                    hot_epoch,
                    snap,
                    proj_hash,
                    batches.clone(),
                );
            }
        }
    }

    // Apply post-read IS NULL filters. `Predicate` has no `IsNull` variant so
    // these cannot be pushed into the storage layer; we apply them here using
    // Arrow's `is_null` compute kernel after the decode. The per-file catalog
    // pruning above already skipped files where `null_count == 0`, so the
    // remaining files are known to have at least one NULL — the post-filter
    // only removes non-null rows, which is typically cheap.
    let batches = if plan.is_null_cols.is_empty() {
        batches
    } else {
        apply_is_null_filter(batches, &plan.is_null_cols)?
    };

    // Apply post-read IN-list filters. `ReadOptions.filters` only accepts
    // `Vec<Predicate>` (no `In` variant), so IN predicates are applied here
    // as a row-level Arrow filter using `CompoundPredicate::In`.  The PK
    // bloom+zone-map probe above (when fired) already narrowed the file set
    // so most surviving rows DO match — this filter is the correctness check
    // for the probe's conservative superset (and handles non-PK IN-list cases
    // that did not benefit from the probe).
    let batches = if plan.in_list_preds.is_empty() {
        batches
    } else {
        apply_in_list_filter(batches, &plan.in_list_preds)?
    };

    // If this is an aggregate query, compute the aggregate functions over the
    // (already filtered) batches and return a single-row result.
    if let Some(ref aggs) = plan.aggregates {
        return apply_aggregates(batches, aggs, &meta.schema);
    }

    // Build the output schema and apply computed projections. For wildcard
    // queries `plan.projection` is `None` and the schema is the full table
    // schema. For plain column lists with no computed items we project by
    // index. For lists that include computed items we evaluate the
    // arithmetic expressions per-batch and rebuild each batch with the
    // user-requested output columns and aliases.
    let (projected_schema, batches): (Arc<Schema>, Vec<RecordBatch>) =
        match &plan.projection {
            None => {
                // `SELECT *`: the declared output schema is the CURRENT catalog
                // schema. Pad each batch up to it so a row read from a data file
                // (or overlay) written before an `ALTER TABLE ADD COLUMN` gains
                // the new trailing columns as NULL — the declared schema and the
                // batch columns must agree for every downstream consumer.
                let padded: Vec<RecordBatch> = batches
                    .into_iter()
                    .map(|b| crate::hot_tombstone::pad_batch_to_schema(b, &meta.schema))
                    .collect::<Result<_>>()?;
                (meta.schema.clone(), padded)
            }
            Some(items) => {
                // Check whether any item is a Computed variant.
                let has_computed = items
                    .iter()
                    .any(|it| matches!(it, ProjectionItem::Computed { .. }));

                if has_computed {
                    // Build the output schema from the projection list.
                    // Computed columns get the alias as name and the type
                    // determined by the first non-empty batch (we verify
                    // Int64/Float64 above so it is safe to infer at runtime).
                    let out_schema =
                        build_computed_schema(items, &batches, &meta.schema)?;
                    let out_schema = Arc::new(out_schema);

                    let evaluated: Vec<RecordBatch> = batches
                        .into_iter()
                        .map(|b| evaluate_computed_projections(&b, items, &out_schema))
                        .collect::<Result<Vec<_>>>()?;

                    (out_schema, evaluated)
                } else {
                    // Plain column list — build output schema from the full table
                    // schema and re-project each batch to the requested columns.
                    //
                    // `read_cols` (the columns handed to `read_paths_with_schema`)
                    // is the union of the user-requested columns and all filter
                    // columns, sorted alphabetically. The storage layer returns
                    // batches whose columns are in that sorted order. We must
                    // re-project each batch so the output matches the SELECT list
                    // order and contains only the user-requested columns.
                    let col_names: Vec<&str> = items
                        .iter()
                        .map(|item| match item {
                            ProjectionItem::Column(c) => c.as_str(),
                            ProjectionItem::Computed { .. } => unreachable!(),
                        })
                        .collect();
                    let idxs: Vec<usize> = col_names
                        .iter()
                        .map(|c| {
                            meta.schema.index_of(c).map_err(|_| {
                                BasinError::UndefinedColumn(c.to_string())
                            })
                        })
                        .collect::<Result<_>>()?;
                    let schema = Arc::new(
                        meta.schema
                            .project(&idxs)
                            .map_err(|e| BasinError::internal(format!("project schema: {e}")))?,
                    );
                    // Project each batch: find each requested column by NAME in
                    // the batch schema (which reflects read_cols order, not the
                    // full table schema order) and assemble a new RecordBatch.
                    let projected: Vec<RecordBatch> = batches
                        .into_iter()
                        .map(|b| {
                            let cols: Result<Vec<_>> = col_names
                                .iter()
                                .map(|c| {
                                    b.schema()
                                        .index_of(c)
                                        .map(|i| b.column(i).clone())
                                        .map_err(|_| {
                                            BasinError::InvalidSchema(format!(
                                                "batch missing column {c}"
                                            ))
                                        })
                                })
                                .collect();
                            RecordBatch::try_new(schema.clone(), cols?)
                                .map_err(|e| BasinError::internal(format!("project batch: {e}")))
                        })
                        .collect::<Result<_>>()?;
                    (schema, projected)
                }
            }
        };

    // Normalize cold scan batches to the catalog-typed projected_schema:
    // Vortex decode narrows LargeBinary (JSONB) to Binary, while memtable
    // overlay rows keep the catalog type — mixed batches would fail any
    // downstream concat (including apply_order_by_limit below) and the
    // result batches must be physically consistent with their declared
    // schema either way.
    let batches: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| crate::hot_tombstone::normalize_batch_to_schema(b, projected_schema.as_ref()))
        .collect();

    // ORDER BY + LIMIT: merge batches into one, sort by the column, take
    // the first `limit` rows. We only reach here when `order_by.is_some()`
    // implies `limit.is_some()` (enforced in match_query).
    let trimmed = if let Some((ref col, ascending)) = plan.order_by {
        let limit = plan.limit.expect("order_by implies limit");
        let offset = plan.offset.unwrap_or(0);
        apply_order_by_limit(batches, col, ascending, limit, offset, &projected_schema)?
    } else {
        match plan.limit {
            Some(limit) => apply_limit(batches, limit),
            None => batches,
        }
    };

    Ok(ExecResult::Rows {
        schema: projected_schema,
        batches: trimmed,
    })
}

/// Build the Arrow output schema for a projection that contains at least one
/// `ProjectionItem::Computed` item.
///
/// Plain `Column` items take their type from `table_schema`. Computed items
/// take their type from the first non-empty batch's evaluated array (since
/// Arrow's numeric kernels return the same type as the operands for
/// integer-only expressions, and the widest float type for mixed). We fall
/// back to the table schema column type for the first source column when
/// `batches` is empty.
fn build_computed_schema(
    items: &[ProjectionItem],
    batches: &[RecordBatch],
    table_schema: &Arc<Schema>,
) -> Result<Schema> {
    use arrow_schema::Field;

    let mut fields: Vec<arrow_schema::FieldRef> = Vec::with_capacity(items.len());

    for item in items {
        match item {
            ProjectionItem::Column(c) => {
                let idx = table_schema
                    .index_of(c)
                    .map_err(|_| BasinError::UndefinedColumn(c.to_string()))?;
                fields.push(table_schema.field(idx).clone().into());
            }
            ProjectionItem::Computed {
                sql_expr,
                alias,
                source_cols,
            } => {
                // Infer output type purely from the schema — no batch allocation.
                // For integer-only expressions Arrow's kernels return the operand
                // type; mixed Int64+Float64 returns Float64. infer_expr_output_type
                // mirrors that rule walk-wise. Evaluating on a batch just to read
                // its DataType allocated a full ArrayRef per Computed item.
                let c = source_cols.first().map(|s| s.as_str()).unwrap_or("");
                let dt = infer_expr_output_type(sql_expr, table_schema, c)?;
                // Nullable: arithmetic on nullable columns propagates NULLs.
                fields.push(Arc::new(Field::new(alias.as_str(), dt, true)));
            }
        }
    }

    Ok(Schema::new(fields))
}

/// Infer the output `DataType` of an arithmetic expression when no batch
/// is available, based on the declared column types in `table_schema`. Falls
/// back to `Int64` for number literals. Mixed Int64+Float64 expressions
/// infer `Float64` (matching Arrow kernel behaviour).
fn infer_expr_output_type(
    expr: &Expr,
    schema: &Arc<Schema>,
    _hint_col: &str,
) -> Result<arrow_schema::DataType> {
    use arrow_schema::DataType;
    fn walk(expr: &Expr, schema: &Arc<Schema>) -> Result<DataType> {
        match expr {
            Expr::Identifier(id) => {
                let idx = schema
                    .index_of(&id.value)
                    .map_err(|_| BasinError::InvalidSchema(format!("unknown column {}", id.value)))?;
                Ok(schema.field(idx).data_type().clone())
            }
            Expr::Value(ValueWithSpan {
                value: Value::Number(_, _),
                ..
            }) => Ok(DataType::Int64),
            Expr::UnaryOp { expr: inner, .. } => walk(inner, schema),
            Expr::BinaryOp { left, right, .. } => {
                let lt = walk(left, schema)?;
                let rt = walk(right, schema)?;
                if lt == DataType::Float64 || rt == DataType::Float64 {
                    Ok(DataType::Float64)
                } else {
                    Ok(lt)
                }
            }
            Expr::Nested(inner) => walk(inner, schema),
            _ => Ok(DataType::Int64),
        }
    }
    walk(expr, schema)
}

/// Thin wrapper so both a full-length `ArrayRef` (batch column) and a
/// single-element scalar array can be threaded through the recursive
/// evaluator and handed to the Arrow numeric kernels as `&dyn Datum`.
///
/// `Array(arr)` — non-scalar; length equals `batch.num_rows()`.
/// `ScalarI64(v)` / `ScalarF64(v)` — will be wrapped in `Scalar<>` before
/// being passed to the kernel so the kernel broadcasts correctly.
enum EvalVal {
    Array(arrow_array::ArrayRef),
    ScalarI64(i64),
    ScalarF64(f64),
}

impl EvalVal {
    /// Return the Arrow `DataType` of this value.
    fn data_type(&self) -> arrow_schema::DataType {
        match self {
            EvalVal::Array(a) => a.data_type().clone(),
            EvalVal::ScalarI64(_) => arrow_schema::DataType::Int64,
            EvalVal::ScalarF64(_) => arrow_schema::DataType::Float64,
        }
    }

    /// Materialise as a concrete `ArrayRef`. Arrays are returned as-is;
    /// scalar values become a single-element array (NOT broadcast — caller
    /// must use the `datum` helper when passing to kernels).
    fn into_array(self) -> arrow_array::ArrayRef {
        match self {
            EvalVal::Array(a) => a,
            EvalVal::ScalarI64(v) => {
                Arc::new(arrow_array::Int64Array::from(vec![v])) as arrow_array::ArrayRef
            }
            EvalVal::ScalarF64(v) => {
                Arc::new(arrow_array::Float64Array::from(vec![v])) as arrow_array::ArrayRef
            }
        }
    }
}

/// Apply an Arrow numeric kernel to two `EvalVal`s, producing an `ArrayRef`.
/// Scalars are wrapped in `Scalar<>` so the kernel broadcasts them correctly.
fn apply_kernel(
    op: &BinaryOperator,
    lhs: EvalVal,
    rhs: EvalVal,
) -> std::result::Result<arrow_array::ArrayRef, String> {
    use arrow::compute::kernels::numeric::{add_wrapping, div, mul_wrapping, sub_wrapping};

    // Materially, we need `&dyn Datum` for both sides. We dispatch on the
    // combination of (lhs_is_scalar, rhs_is_scalar) to set up the correct
    // Datum representation.
    macro_rules! call_kernel {
        ($kernel:ident, $l:expr, $r:expr) => {
            $kernel($l, $r).map_err(|e| e.to_string())
        };
    }

    let result = match (lhs, rhs) {
        (EvalVal::Array(la), EvalVal::Array(ra)) => match op {
            BinaryOperator::Plus => call_kernel!(add_wrapping, &la.as_ref(), &ra.as_ref()),
            BinaryOperator::Minus => call_kernel!(sub_wrapping, &la.as_ref(), &ra.as_ref()),
            BinaryOperator::Multiply => call_kernel!(mul_wrapping, &la.as_ref(), &ra.as_ref()),
            BinaryOperator::Divide => call_kernel!(div, &la.as_ref(), &ra.as_ref()),
            _ => Err(format!("unsupported operator {op}")),
        },
        (EvalVal::Array(la), EvalVal::ScalarI64(rv)) => {
            let rs = arrow_array::Int64Array::new_scalar(rv);
            match op {
                BinaryOperator::Plus => call_kernel!(add_wrapping, &la.as_ref(), &rs),
                BinaryOperator::Minus => call_kernel!(sub_wrapping, &la.as_ref(), &rs),
                BinaryOperator::Multiply => call_kernel!(mul_wrapping, &la.as_ref(), &rs),
                BinaryOperator::Divide => call_kernel!(div, &la.as_ref(), &rs),
                _ => Err(format!("unsupported operator {op}")),
            }
        }
        (EvalVal::Array(la), EvalVal::ScalarF64(rv)) => {
            let rs = arrow_array::Float64Array::new_scalar(rv);
            match op {
                BinaryOperator::Plus => {
                    let la_f = cast_to_f64(&la)?;
                    call_kernel!(add_wrapping, &la_f.as_ref(), &rs)
                }
                BinaryOperator::Minus => {
                    let la_f = cast_to_f64(&la)?;
                    call_kernel!(sub_wrapping, &la_f.as_ref(), &rs)
                }
                BinaryOperator::Multiply => {
                    let la_f = cast_to_f64(&la)?;
                    call_kernel!(mul_wrapping, &la_f.as_ref(), &rs)
                }
                BinaryOperator::Divide => {
                    let la_f = cast_to_f64(&la)?;
                    call_kernel!(div, &la_f.as_ref(), &rs)
                }
                _ => Err(format!("unsupported operator {op}")),
            }
        }
        (EvalVal::ScalarI64(lv), EvalVal::Array(ra)) => {
            let ls = arrow_array::Int64Array::new_scalar(lv);
            match op {
                BinaryOperator::Plus => call_kernel!(add_wrapping, &ls, &ra.as_ref()),
                BinaryOperator::Minus => call_kernel!(sub_wrapping, &ls, &ra.as_ref()),
                BinaryOperator::Multiply => call_kernel!(mul_wrapping, &ls, &ra.as_ref()),
                BinaryOperator::Divide => call_kernel!(div, &ls, &ra.as_ref()),
                _ => Err(format!("unsupported operator {op}")),
            }
        }
        (EvalVal::ScalarF64(lv), EvalVal::Array(ra)) => {
            let ls = arrow_array::Float64Array::new_scalar(lv);
            match op {
                BinaryOperator::Plus => {
                    let ra_f = cast_to_f64(&ra)?;
                    call_kernel!(add_wrapping, &ls, &ra_f.as_ref())
                }
                BinaryOperator::Minus => {
                    let ra_f = cast_to_f64(&ra)?;
                    call_kernel!(sub_wrapping, &ls, &ra_f.as_ref())
                }
                BinaryOperator::Multiply => {
                    let ra_f = cast_to_f64(&ra)?;
                    call_kernel!(mul_wrapping, &ls, &ra_f.as_ref())
                }
                BinaryOperator::Divide => {
                    let ra_f = cast_to_f64(&ra)?;
                    call_kernel!(div, &ls, &ra_f.as_ref())
                }
                _ => Err(format!("unsupported operator {op}")),
            }
        }
        // Both scalars: materialise and operate element-wise (1-element arrays).
        (lhs, rhs) => {
            let la = lhs.into_array();
            let ra = rhs.into_array();
            match op {
                BinaryOperator::Plus => call_kernel!(add_wrapping, &la.as_ref(), &ra.as_ref()),
                BinaryOperator::Minus => call_kernel!(sub_wrapping, &la.as_ref(), &ra.as_ref()),
                BinaryOperator::Multiply => call_kernel!(mul_wrapping, &la.as_ref(), &ra.as_ref()),
                BinaryOperator::Divide => call_kernel!(div, &la.as_ref(), &ra.as_ref()),
                _ => Err(format!("unsupported operator {op}")),
            }
        }
    }?;
    Ok(result)
}

/// Cast an `ArrayRef` to `Float64`. Used when mixing Int64 columns with
/// Float64 literals so the kernel sees matching types.
fn cast_to_f64(arr: &arrow_array::ArrayRef) -> std::result::Result<arrow_array::ArrayRef, String> {
    use arrow_schema::DataType;
    if arr.data_type() == &DataType::Float64 {
        return Ok(arr.clone());
    }
    arrow::compute::cast(arr.as_ref(), &DataType::Float64).map_err(|e| e.to_string())
}

/// Evaluate a single validated arithmetic expression against a `RecordBatch`
/// and return an `EvalVal`. Uses the wrapping variants of the Arrow numeric
/// kernels for integer arithmetic (matching DataFusion's wrapping-i64
/// semantics). Returns `Err` on kernel errors (e.g. division by zero, type
/// mismatch) so the caller can propagate as a `BasinError`.
fn eval_arithmetic_expr(
    expr: &Expr,
    batch: &RecordBatch,
) -> std::result::Result<EvalVal, String> {
    match expr {
        Expr::Identifier(id) => {
            let idx = batch
                .schema()
                .index_of(&id.value)
                .map_err(|_| format!("column not in batch: {}", id.value))?;
            Ok(EvalVal::Array(batch.column(idx).clone()))
        }
        Expr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(EvalVal::ScalarI64(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(EvalVal::ScalarF64(f))
            } else {
                Err(format!("cannot parse numeric literal: {s}"))
            }
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            let v = eval_arithmetic_expr(inner, batch)?;
            match v {
                EvalVal::ScalarI64(i) => Ok(EvalVal::ScalarI64(-i)),
                EvalVal::ScalarF64(f) => Ok(EvalVal::ScalarF64(-f)),
                EvalVal::Array(arr) => {
                    use arrow_schema::DataType;
                    let result = match arr.data_type() {
                        DataType::Int64 => {
                            let zero = arrow_array::Int64Array::new_scalar(0i64);
                            arrow::compute::kernels::numeric::sub_wrapping(&zero, &arr.as_ref())
                                .map_err(|e| e.to_string())?
                        }
                        DataType::Float64 => {
                            let zero = arrow_array::Float64Array::new_scalar(0.0f64);
                            arrow::compute::kernels::numeric::sub(&zero, &arr.as_ref())
                                .map_err(|e| e.to_string())?
                        }
                        dt => return Err(format!("unary minus on unsupported type {dt}")),
                    };
                    Ok(EvalVal::Array(result))
                }
            }
        }
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr: inner,
        } => eval_arithmetic_expr(inner, batch),
        Expr::BinaryOp { op, left, right } => {
            let lv = eval_arithmetic_expr(left, batch)?;
            let rv = eval_arithmetic_expr(right, batch)?;
            let arr = apply_kernel(op, lv, rv)?;
            Ok(EvalVal::Array(arr))
        }
        Expr::Nested(inner) => eval_arithmetic_expr(inner, batch),
        _ => Err(format!("unsupported expression in arithmetic eval: {expr}")),
    }
}

/// Apply the user-requested projection (with computed expressions) to a
/// single `RecordBatch` and return a new batch in SELECT-list order.
///
/// For `ProjectionItem::Column` items the column is taken directly from the
/// input batch. For `ProjectionItem::Computed` items the arithmetic
/// expression is evaluated via Arrow compute kernels. Columns that were
/// read only to satisfy filter predicates or computed-expression sources
/// but are NOT in the user projection list are dropped.
///
/// The output batch schema must match `out_schema` exactly.
fn evaluate_computed_projections(
    batch: &RecordBatch,
    items: &[ProjectionItem],
    out_schema: &Arc<Schema>,
) -> Result<RecordBatch> {
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(items.len());

    for item in items {
        match item {
            ProjectionItem::Column(c) => {
                let idx = batch.schema().index_of(c).map_err(|_| {
                    BasinError::InvalidSchema(format!("output column {c} not in batch"))
                })?;
                columns.push(batch.column(idx).clone());
            }
            ProjectionItem::Computed { sql_expr, alias, .. } => {
                let val = eval_arithmetic_expr(sql_expr, batch).map_err(|e| {
                    BasinError::internal(format!("compute expr for {alias}: {e}"))
                })?;
                columns.push(val.into_array());
            }
        }
    }

    RecordBatch::try_new(out_schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("computed projection batch: {e}")))
}


/// The single effective cluster column the writer physically sorts every cold
/// file on, or `None` when the table is not single-column clustered.
///
/// Resolution order matches the write path's `WriteOptions.cluster_columns`
/// source:
///   1. An explicit `CLUSTER BY (col)` / `basin.cluster_by=col` —
///      `meta.cluster_columns` (used verbatim by the writer).
///   2. Otherwise the implicit single-PK clustering — `default_cluster_cols()`
///      (which already type-gates to prunable PK types and returns empty for
///      composite PKs or an explicit `global_sort_order`).
///
/// Only a single-column result qualifies for the keyset fast path; anything
/// composite returns `None` so the caller stays on the full-file path.
/// 8-byte little-endian Int64 zone-map stat decode — the writer's
/// `ColumnStats::min_bytes` / `max_bytes` encoding for Int64 columns. Mirrors
/// the `stat_i64` decode `index_probe::pk_point_probe_multi` uses for its
/// exact IN-list zone-map prune (kept local there as a nested fn); any change
/// to the writer's stat encoding must update both.
fn decode_stat_i64(b: Option<&[u8]>) -> Option<i64> {
    let arr: [u8; 8] = b?.try_into().ok()?;
    Some(i64::from_le_bytes(arr))
}

fn effective_cluster_col(meta: &TableMetadata) -> Option<String> {
    if !meta.cluster_columns.is_empty() {
        return match meta.cluster_columns.as_slice() {
            [only] => Some(only.clone()),
            _ => None,
        };
    }
    match meta.default_cluster_cols().as_slice() {
        [only] => Some(only.clone()),
        _ => None,
    }
}

/// 8-byte little-endian `f64` zone-map stat decode — the writer's
/// `ColumnStats::min_bytes` / `max_bytes` encoding for `Float64` columns
/// (`vortex_format.rs` and the Parquet writer both emit `f64::to_le_bytes`).
/// Mirrors `decode_stat_i64` for the floating sort key. Returns `None` for a
/// missing / wrong-width / NaN stat (NaN ordering is unspecified, so a NaN
/// bound disables the file skip rather than risk a wrong prune).
fn decode_stat_f64(b: Option<&[u8]>) -> Option<f64> {
    let arr: [u8; 8] = b?.try_into().ok()?;
    let v = f64::from_le_bytes(arr);
    if v.is_nan() {
        None
    } else {
        Some(v)
    }
}

/// `SortOptions` matching PG / DataFusion NULL defaults for a key sorted in
/// `ascending` direction: NULLS LAST for ASC, NULLS FIRST for DESC (the same
/// rule `apply_order_by_limit` uses). The PK tie-break key is always ASC.
fn topk_sort_options(ascending: bool) -> arrow::compute::SortOptions {
    arrow::compute::SortOptions {
        descending: !ascending,
        nulls_first: !ascending,
    }
}

/// Build the `(sort_col [, pk]) → row order` `SortColumn` list from the merged
/// sort-key array and (when `pk_tiebreak`) the merged ascending PK tie-break
/// array. `ArrayRef` clones are cheap (`Arc`). Used by both the phase-1 narrow
/// heap-build and the phase-2 winner re-sort so the two orderings are
/// byte-identical.
fn topk_sort_columns(
    sort_arr: &arrow_array::ArrayRef,
    pk_arr: Option<&arrow_array::ArrayRef>,
    ascending: bool,
) -> Vec<arrow::compute::SortColumn> {
    let mut cols = vec![arrow::compute::SortColumn {
        values: sort_arr.clone(),
        options: Some(topk_sort_options(ascending)),
    }];
    if let Some(pk) = pk_arr {
        cols.push(arrow::compute::SortColumn {
            // PK tie-break: ascending, NULLS LAST (a PK is never NULL, but be
            // explicit so the ordering is fully specified).
            values: pk.clone(),
            options: Some(topk_sort_options(true)),
        });
    }
    cols
}

/// Deep top-K late materialization.
///
/// Two phases over `SELECT … FROM t ORDER BY k [DESC] [, pk] LIMIT n`:
///
///   * **Phase 1 — narrow key scan.** Read ONLY `[k, pk]` (+ any WHERE columns)
///     from the live cold files, applying the same pushed predicates the slow
///     path would. File-skip any file whose `k` min/max proves it cannot
///     contribute to the global top-`n` given the current `n`-th bound (Int64 /
///     Float64 sort keys with catalog `column_stats`; other types skip the
///     prune and read every file — correct, just less pruned). Lexically
///     top-`n`-sort the surviving `(k, pk)` rows and collect the winning ≤ `n`
///     PK values.
///   * **Phase 2 — wide fetch.** Read the FULL projected rows for just those
///     PKs via an `InInt64(pk, winners)` pushdown (the existing sorted-key skip
///     seam), re-sort by `(k, pk)`, and project. The wide columns of the
///     `rows − n` losers are never decoded.
///
/// Returns `Ok(None)` to fall through to the existing decode-everything path
/// for ANY ineligibility:
///   * the env cap disabled the branch (`k == 0`);
///   * a live hot-tier overlay (`overlay_active`) — the merge is complex and an
///     overlay-updated sort value would need the NEW value; we decline (the
///     existing path applies the overlay correctly);
///   * a small un-flushed tail to merge (`tail_merge_via_shard_read`) — a hot
///     row could win; we decline so the tail-merging path serves it. (An empty
///     tail or a large tail already flushed to cold leaves every committed row
///     in `live_files`, so a hot row cannot be missed here.);
///   * a pinned in-tx read (`pinned`) — keep the existing pinned path;
///   * the identity column is not the table's single Int64 PK (so the phase-2
///     `InInt64` point-fetch cannot address the winners);
///   * `SELECT` carries a computed projection (handled by the existing path).
///
/// When a two-column `ORDER BY k, pk` declines here, the caller's `match_query`
/// gate guaranteed the statement is served ONLY by this branch — so a decline
/// must reach DataFusion, not the single-column `apply_order_by_limit`. We do
/// that by re-dispatching to `exec_select` on the decline paths that the
/// single-column fallback would mishandle (i.e. `pk_tiebreak`).
#[allow(clippy::too_many_arguments)]
async fn try_topk_late_materialize(
    sess: &ProjectSession,
    plan: &SimpleSelectPlan,
    meta: &TableMetadata,
    live_files: &[basin_catalog::DataFileRef],
    overlay_active: bool,
    tail_merge_via_shard_read: bool,
    pinned: bool,
    raw_sql: &str,
    include_deleted: bool,
) -> Result<Option<ExecResult>> {
    use arrow_array::Array;
    let Some(tk) = plan.topk_late.as_ref() else {
        return Ok(None);
    };

    // Helper: a decline that the single-column fallback path below CANNOT serve
    // (a two-column `ORDER BY k, pk`) must go to DataFusion; a single-column
    // decline falls through to the existing `apply_order_by_limit`.
    async fn decline(
        sess: &ProjectSession,
        pk_tiebreak: bool,
        raw_sql: &str,
        include_deleted: bool,
    ) -> Result<Option<ExecResult>> {
        if pk_tiebreak {
            // The existing single-column path can't honour the PK tie-break;
            // route the whole statement to DataFusion, which sorts both keys.
            let fb = fallback_sql(raw_sql);
            let res = crate::executor::exec_select(sess, &fb, include_deleted, Some(raw_sql)).await?;
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    // ── Runtime eligibility gates ────────────────────────────────────────────
    let limit = match plan.limit {
        Some(n) if n > 0 => n,
        // k == 0 (empty result) — let the existing path return empty; no win.
        _ => return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await,
    };
    // Env cap disabled or pinned / overlay / tail-merge → decline.
    if topk_late_max_k() == 0
        || pinned
        || overlay_active
        || tail_merge_via_shard_read
    {
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    }
    // Identity must be the single PK column, and (for the phase-2 InInt64
    // point-fetch) that PK must be an Int64-family column.
    if meta.pk_columns.len() != 1 || meta.pk_columns[0] != tk.pk_col {
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    }
    let Ok(pk_idx) = meta.schema.index_of(&tk.pk_col) else {
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    };
    if !matches!(
        meta.schema.field(pk_idx).data_type(),
        arrow_schema::DataType::Int64
    ) {
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    }
    // Sort column must exist in the schema.
    let Ok(sort_idx) = meta.schema.index_of(&tk.sort_col) else {
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    };
    let sort_dt = meta.schema.field(sort_idx).data_type().clone();
    // Computed projections are handled by the existing path's rebuild; decline.
    if let Some(items) = plan.projection.as_ref() {
        if items
            .iter()
            .any(|it| matches!(it, ProjectionItem::Computed { .. }))
        {
            return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
        }
    }
    // `include_deleted` (soft-delete-visibility variant) takes the existing
    // path so its tombstone visibility is unchanged.
    if include_deleted {
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    }

    // ── Phase 1: narrow key scan ([sort_col, pk] + WHERE columns) ────────────
    //
    // Read only the columns we need to (a) order, (b) identify the winners, and
    // (c) re-check any WHERE predicate. The WHERE predicates are pushed exactly
    // as the slow path pushes them (same `filters`), so phase 1's surviving
    // rows are the same rows the slow path would keep. The PK is unique within
    // and across files, so the winning PKs uniquely address the winner rows in
    // phase 2.
    let mut phase1_cols: Vec<String> = Vec::with_capacity(4);
    phase1_cols.push(tk.sort_col.clone());
    if tk.pk_col != tk.sort_col {
        phase1_cols.push(tk.pk_col.clone());
    }
    for p in &plan.predicates {
        let c = p.column();
        if !phase1_cols.iter().any(|x| x == c) && meta.schema.index_of(c).is_ok() {
            phase1_cols.push(c.to_string());
        }
    }
    for (c, _) in &plan.in_list_preds {
        if !phase1_cols.iter().any(|x| x == c) && meta.schema.index_of(c).is_ok() {
            phase1_cols.push(c.clone());
        }
    }
    for c in &plan.is_null_cols {
        if !phase1_cols.iter().any(|x| x == c) && meta.schema.index_of(c).is_ok() {
            phase1_cols.push(c.clone());
        }
    }

    // File-level min/max skip on the sort column. A file can contribute to the
    // global top-`limit` only if its `sort_col` range overlaps the region the
    // current `limit`-th bound admits. We do a simpler, always-correct variant:
    // decode each candidate file's `sort_col` min/max, sort the files by the
    // bound that matters (max for DESC, min for ASC), and read greedily until
    // we have `limit` survivors whose worst key already beats every unread
    // file's best key. Files proven unable to beat the running `limit`-th bound
    // are skipped entirely. Only Int64 / Float64 sort columns carry decodable
    // catalog stats; for any other type (or a file missing the stat) we keep
    // that file unconditionally (correct, just unpruned).
    //
    // To stay simple and robust we read ALL surviving files' narrow `[k, pk]`
    // projection (the per-file skip below only DROPS files the stats prove
    // irrelevant), then do the global bounded top-`limit` sort once. The narrow
    // projection makes even the unpruned read cheap (2 columns vs the wide row).
    // The row-budget file skip is sound only when every row of a file is a
    // top-K candidate. A WHERE filter makes the per-file SURVIVOR count
    // unknown (fewer than `row_count` may pass), so the `limit`-th bound built
    // from raw `row_count` could prune a file that, post-filter, should still
    // contribute. When any filter is present we therefore keep every file (the
    // narrow `[k, pk]` read is still cheap; only the file SKIP is forgone).
    let has_filter = !plan.predicates.is_empty()
        || !plan.in_list_preds.is_empty()
        || !plan.is_null_cols.is_empty();
    let pruned_paths: Vec<object_store::path::Path> = if has_filter {
        live_files
            .iter()
            .map(|f| object_store::path::Path::from(f.path.as_str()))
            .collect()
    } else {
        topk_phase1_file_skip(live_files, &tk.sort_col, &sort_dt, tk.ascending, limit)
    };
    if pruned_paths.is_empty() {
        // No live files (empty table / everything pruned to nothing): the
        // existing path returns an empty result cheaply. Decline so its empty
        // handling and schema construction run unchanged.
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    }

    let phase1_opts = ReadOptions {
        projection: Some(phase1_cols.clone()),
        filters: plan.predicates.clone(),
        partition: None,
        limit: None,
        row_group_selection: None,
        row_selection: None,
        sorted_by: None,
    };
    let phase1_schema = {
        let idxs: Vec<usize> = phase1_cols
            .iter()
            .map(|c| meta.schema.index_of(c).expect("phase1 col validated"))
            .collect();
        Arc::new(
            meta.schema
                .project(&idxs)
                .map_err(|e| BasinError::internal(format!("topk phase1 schema: {e}")))?,
        )
    };
    let phase1_batches: Vec<RecordBatch> = {
        use futures::StreamExt;
        let stream = sess
            .engine
            .config()
            .storage
            .read_paths_with_schema(
                &sess.project,
                pruned_paths,
                phase1_opts,
                Some(phase1_schema.clone()),
            )
            .await?;
        stream.collect::<Vec<Result<RecordBatch>>>().await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
    };

    // Apply the post-read shrinking filters (IS NULL / IN-list) the slow path
    // applies, so phase 1's surviving rows match the slow path's exactly. These
    // operate on phase1 batches (which include the referenced columns).
    let phase1_batches = if plan.is_null_cols.is_empty() {
        phase1_batches
    } else {
        apply_is_null_filter(phase1_batches, &plan.is_null_cols)?
    };
    let phase1_batches = if plan.in_list_preds.is_empty() {
        phase1_batches
    } else {
        apply_in_list_filter(phase1_batches, &plan.in_list_preds)?
    };

    // Bounded top-`limit` over the narrow batches → winning PK values.
    let p1_sort_i = phase1_schema.index_of(&tk.sort_col).expect("sort col in phase1");
    let p1_pk_i = phase1_schema.index_of(&tk.pk_col).expect("pk col in phase1");
    let total_p1: usize = phase1_batches.iter().map(|b| b.num_rows()).sum();
    if total_p1 == 0 {
        // Empty after filtering — return an empty result in the output schema
        // the existing projection would build. Easiest correct answer: decline
        // and let the existing path produce the empty set.
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    }
    // Concatenate ONLY the key + pk columns (narrow) for the global sort.
    let sort_arrays: Vec<&dyn Array> = phase1_batches
        .iter()
        .map(|b| b.column(p1_sort_i).as_ref())
        .collect();
    let pk_arrays: Vec<&dyn Array> = phase1_batches
        .iter()
        .map(|b| b.column(p1_pk_i).as_ref())
        .collect();
    let merged_sort = arrow::compute::concat(&sort_arrays)
        .map_err(|e| BasinError::internal(format!("topk sort concat: {e}")))?;
    let merged_pk = arrow::compute::concat(&pk_arrays)
        .map_err(|e| BasinError::internal(format!("topk pk concat: {e}")))?;
    let sort_cols = topk_sort_columns(
        &merged_sort,
        tk.pk_tiebreak.then_some(&merged_pk),
        tk.ascending,
    );
    let indices = arrow::compute::lexsort_to_indices(&sort_cols, Some(limit))
        .map_err(|e| BasinError::internal(format!("topk lexsort: {e}")))?;
    // Collect the winning PK values (Int64) in winner order.
    let merged_pk_i64 = merged_pk
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .ok_or_else(|| BasinError::internal("topk pk not Int64".to_string()))?;
    let mut winners: Vec<i64> = Vec::with_capacity(indices.len());
    for i in indices.values().iter() {
        let row = *i as usize;
        if merged_pk_i64.is_null(row) {
            // A NULL PK is impossible (PK is NOT NULL); be defensive and
            // decline rather than emit a wrong winner set.
            return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
        }
        winners.push(merged_pk_i64.value(row));
    }
    if winners.is_empty() {
        return decline(sess, tk.pk_tiebreak, raw_sql, include_deleted).await;
    }
    // Dedup defensively for the InInt64 fetch (PKs are unique, so this is a
    // no-op, but a malformed file with duplicate PKs must not fetch twice).
    let mut winner_set = winners.clone();
    winner_set.sort_unstable();
    winner_set.dedup();

    // ── Phase 2: wide fetch for just the winning PKs ─────────────────────────
    //
    // `InInt64(pk, winners)` over the live files. The PK column is the table's
    // effective cluster column, so storage binary-searches each PK-sorted chunk
    // (the `sorted_by` hint enables the sorted-key skip) and `take`s only the
    // matching rows — never an O(rows) Arrow filter over the wide columns.
    let pk_col = tk.pk_col.clone();
    let phase2_filters = vec![Predicate::InInt64(pk_col.clone(), winner_set.clone())];
    let sorted_by = effective_cluster_col(meta).filter(|c| c == &pk_col);
    // Phase-2 read projection. For `SELECT *` (`read_cols == None`) every column
    // is read. For a column-list projection we must ALSO decode the PK (the
    // `InInt64` filter + final re-sort tie-break need it) and the sort column
    // (the re-sort needs it), even if the user did not select them — they are
    // projected back OUT by `topk_project_batches` below, so they never leak
    // into the result. Mirrors the overlay-PK augmentation the slow path does.
    let phase2_projection: Option<Vec<String>> = plan.read_cols.clone().map(|mut cols| {
        for need in [pk_col.as_str(), tk.sort_col.as_str()] {
            if !cols.iter().any(|c| c == need) && meta.schema.index_of(need).is_ok() {
                cols.push(need.to_string());
            }
        }
        cols
    });
    let phase2_opts = ReadOptions {
        projection: phase2_projection,
        filters: phase2_filters,
        partition: None,
        limit: None,
        row_group_selection: None,
        row_selection: None,
        sorted_by,
    };
    let phase2_paths: Vec<object_store::path::Path> = live_files
        .iter()
        .map(|f| object_store::path::Path::from(f.path.as_str()))
        .collect();
    let phase2_batches: Vec<RecordBatch> = {
        use futures::StreamExt;
        let stream = sess
            .engine
            .config()
            .storage
            .read_paths_with_schema(
                &sess.project,
                phase2_paths,
                phase2_opts,
                Some(meta.schema.clone()),
            )
            .await?;
        stream.collect::<Vec<Result<RecordBatch>>>().await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
    };
    // The InInt64 pushdown is a SUPERSET filter on some encodings; re-apply it
    // as the source of truth (mirrors the slow path, which always re-checks).
    let phase2_batches = apply_in_list_filter(
        phase2_batches,
        &[(
            pk_col.clone(),
            winner_set.iter().map(|v| ScalarValue::Int64(*v)).collect(),
        )],
    )?;

    // ── Build the output: project, then re-sort by (sort_col [, pk]) ─────────
    //
    // Reuse the existing projection rebuild by routing through the shared
    // helper shape: the slow path projects AFTER the read using
    // `plan.projection`; we do the same here on the (≤ limit) winner rows, then
    // a final two-key sort produces the exact global order.
    let (out_schema, out_batches) =
        topk_project_batches(plan, meta, phase2_batches)?;

    // Final re-sort of the ≤ limit winners by (sort_col [, pk]). The output
    // already contains only the winners (phase-2 fetched exactly them), so this
    // is a tiny O(k log k) sort over k rows.
    let result = topk_resort_window(out_batches, &out_schema, &tk.sort_col, tk.ascending, tk.pk_tiebreak.then_some(tk.pk_col.as_str()), limit)?;

    sess.engine.note_topk_late_fast_select();
    Ok(Some(ExecResult::Rows {
        schema: out_schema,
        batches: result,
    }))
}

/// Phase-1 file skip for the deep top-K narrow scan. Returns the subset of
/// `live_files` paths whose `sort_col` min/max cannot be excluded from the
/// global top-`limit`. Files whose stats prove they cannot beat the best
/// `limit` keys seen in higher-ranked files are dropped.
///
/// Strategy (Int64 / Float64 only; other types keep every file): sort files by
/// the bound that matters for the direction (max desc for DESC, min asc for
/// ASC). Walk in best-first order accumulating a row budget; once the
/// accumulated `row_count` reaches `limit`, every later file whose best key is
/// strictly worse than the `limit`-th best key already guaranteed cannot
/// contribute and is dropped. A file missing a decodable stat is kept (read
/// in full — correct, just unpruned). This is a conservative SUPERSET: the
/// global sort in phase 1 still produces the exact top-`limit`.
fn topk_phase1_file_skip(
    live_files: &[basin_catalog::DataFileRef],
    sort_col: &str,
    sort_dt: &arrow_schema::DataType,
    ascending: bool,
    limit: usize,
) -> Vec<object_store::path::Path> {
    use arrow_schema::DataType;
    // Whether NULLs in the sort column sort to the WINNING side (the top of the
    // result). `topk_sort_options` uses `nulls_first = !ascending`, so a DESC
    // sort places NULLs first — they are the absolute top-`k` winners. A file's
    // `column_stats` min/max are over NON-NULL values only, so they say nothing
    // about a file's NULL rows. If NULLs win and a file may contain a NULL
    // (`null_count > 0`, or unknown), the min/max skip is UNSOUND for that file
    // (it could drop a file whose NULL rows belong in the top-`k`), so we keep
    // it unconditionally. When NULLs LOSE (ASC, nulls_last) they can only enter
    // the top-`k` after every non-NULL key, which the min/max budget already
    // accounts for, so the skip stays sound.
    let nulls_win = !ascending;
    // Decode a file's `sort_col` [min, max] as f64 (Int64 widened). `None` ⇒
    // stat undecodable (or a NULL-bearing file when NULLs win) ⇒ the file is
    // kept unconditionally.
    let to_f = |df: &basin_catalog::DataFileRef| -> Option<(f64, f64)> {
        let cs = df.column_stats.get(sort_col)?;
        // NULLs win and this file may hold one → cannot skip on non-null bounds.
        if nulls_win && cs.null_count.map(|n| n > 0).unwrap_or(true) {
            return None;
        }
        match sort_dt {
            DataType::Int64 => {
                let mn = decode_stat_i64(cs.min_bytes.as_deref())? as f64;
                let mx = decode_stat_i64(cs.max_bytes.as_deref())? as f64;
                Some((mn, mx))
            }
            DataType::Float64 => {
                let mn = decode_stat_f64(cs.min_bytes.as_deref())?;
                let mx = decode_stat_f64(cs.max_bytes.as_deref())?;
                Some((mn, mx))
            }
            _ => None,
        }
    };

    // Files with a decodable [min,max] participate in the skip; undecodable
    // ones are always kept.
    struct FileBound {
        path: object_store::path::Path,
        best: f64,
        worst: f64,
        rows: u64,
    }
    let mut decodable: Vec<FileBound> = Vec::new();
    let mut always_keep: Vec<object_store::path::Path> = Vec::new();
    for df in live_files {
        let path = object_store::path::Path::from(df.path.as_str());
        match to_f(df) {
            Some((mn, mx)) => {
                let (best, worst) = if ascending { (mn, mx) } else { (mx, mn) };
                decodable.push(FileBound {
                    path,
                    best,
                    worst,
                    rows: df.row_count,
                });
            }
            None => always_keep.push(path),
        }
    }
    if decodable.is_empty() {
        return always_keep;
    }
    // Order best-first. For ASC best = smallest min; for DESC best = largest
    // max. Sort so the most promising files come first.
    decodable.sort_by(|a, b| {
        if ascending {
            a.best.partial_cmp(&b.best).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            b.best.partial_cmp(&a.best).unwrap_or(std::cmp::Ordering::Equal)
        }
    });
    // The `limit`-th best key bound: walk accumulating rows; the file at which
    // the cumulative row count first reaches `limit` carries (in its WORST key)
    // a guaranteed bound — every later file whose BEST key is strictly worse
    // than that worst key cannot contribute and is dropped. `always_keep`
    // files (undecodable) prepend to the budget conservatively (counted as if
    // they all rank ahead) — we simply never skip when any are present, to keep
    // the bound sound without knowing their keys.
    let mut kept: Vec<object_store::path::Path> = Vec::with_capacity(decodable.len());
    if !always_keep.is_empty() {
        // Undecodable files could hold arbitrary keys; do not skip any decodable
        // file. Keep everything (still correct; just no pruning this call).
        kept.extend(always_keep);
        kept.extend(decodable.into_iter().map(|f| f.path));
        return kept;
    }
    let mut budget: u64 = 0;
    let mut bound: Option<f64> = None; // the limit-th best file's worst key
    for f in &decodable {
        if bound.is_none() {
            budget = budget.saturating_add(f.rows);
            if budget >= limit as u64 {
                bound = Some(f.worst);
            }
        }
    }
    let cutoff = match bound {
        // Fewer than `limit` rows total across decodable files: every file is
        // needed.
        None => {
            return decodable.into_iter().map(|f| f.path).collect();
        }
        Some(b) => b,
    };
    for f in decodable {
        // A file contributes only if its BEST key can beat the bound. For DESC
        // (best = max) it must be >= bound; for ASC (best = min) it must be <=
        // bound. Ties (==) are KEPT (a tie can still displace via the PK
        // tie-break / equal keys), so we use a non-strict comparison.
        let keep = if ascending {
            f.best <= cutoff
        } else {
            f.best >= cutoff
        };
        if keep {
            kept.push(f.path);
        }
    }
    kept
}

/// Project the phase-2 winner batches into the user's SELECT projection,
/// producing `(output_schema, projected_batches)`. Plain-column projections
/// only (computed projections were declined upstream); `SELECT *` passes the
/// full catalog-schema rows through. Normalises each batch to the projected
/// schema so a later concat/sort never fails on a decode type mismatch (e.g.
/// Vortex narrowing LargeBinary→Binary), mirroring the slow path.
fn topk_project_batches(
    plan: &SimpleSelectPlan,
    meta: &TableMetadata,
    batches: Vec<RecordBatch>,
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let (schema, batches): (Arc<Schema>, Vec<RecordBatch>) = match &plan.projection {
        None => (meta.schema.clone(), batches),
        Some(items) => {
            let mut idxs = Vec::with_capacity(items.len());
            for item in items {
                let c = match item {
                    ProjectionItem::Column(c) => c,
                    ProjectionItem::Computed { .. } => {
                        return Err(BasinError::internal(
                            "topk computed projection should have declined".to_string(),
                        ));
                    }
                };
                let i = meta
                    .schema
                    .index_of(c)
                    .map_err(|_| BasinError::UndefinedColumn(c.to_string()))?;
                idxs.push(i);
            }
            let schema = Arc::new(
                meta.schema
                    .project(&idxs)
                    .map_err(|e| BasinError::internal(format!("topk project schema: {e}")))?,
            );
            let projected: Vec<RecordBatch> = batches
                .into_iter()
                .map(|b| {
                    b.project(&idxs)
                        .map_err(|e| BasinError::internal(format!("topk project batch: {e}")))
                })
                .collect::<Result<Vec<_>>>()?;
            (schema, projected)
        }
    };
    let batches: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| crate::hot_tombstone::normalize_batch_to_schema(b, schema.as_ref()))
        .collect();
    Ok((schema, batches))
}

/// Final re-sort of the ≤ `limit` phase-2 winner rows by `(sort_col [, pk])`
/// and trim to `limit`. The input already contains only the winners, so this is
/// O(k log k). Uses `lexsort_to_indices` + `interleave` so the wide columns are
/// touched once per output row.
fn topk_resort_window(
    batches: Vec<RecordBatch>,
    schema: &Arc<Schema>,
    sort_col: &str,
    ascending: bool,
    pk_col: Option<&str>,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    use arrow_array::Array;
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let sort_i = schema.index_of(sort_col).map_err(|_| {
        BasinError::InvalidSchema(format!("topk re-sort: column '{sort_col}' not in result"))
    })?;
    let pk_i = match pk_col {
        Some(c) => Some(schema.index_of(c).map_err(|_| {
            BasinError::InvalidSchema(format!("topk re-sort: pk '{c}' not in result"))
        })?),
        None => None,
    };
    let sort_arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(sort_i).as_ref()).collect();
    let merged_sort = arrow::compute::concat(&sort_arrays)
        .map_err(|e| BasinError::internal(format!("topk re-sort concat: {e}")))?;
    let merged_pk = match pk_i {
        Some(i) => {
            let a: Vec<&dyn Array> = batches.iter().map(|b| b.column(i).as_ref()).collect();
            Some(
                arrow::compute::concat(&a)
                    .map_err(|e| BasinError::internal(format!("topk re-sort pk concat: {e}")))?,
            )
        }
        None => None,
    };
    let sort_cols = topk_sort_columns(&merged_sort, merged_pk.as_ref(), ascending);
    let indices = arrow::compute::lexsort_to_indices(&sort_cols, Some(limit))
        .map_err(|e| BasinError::internal(format!("topk re-sort lexsort: {e}")))?;
    if indices.is_empty() {
        return Ok(Vec::new());
    }
    // Map each global index → (batch_idx, row_idx) and interleave every column.
    let mut offsets: Vec<usize> = Vec::with_capacity(batches.len() + 1);
    let mut acc = 0usize;
    offsets.push(0);
    for b in &batches {
        acc += b.num_rows();
        offsets.push(acc);
    }
    let pairs: Vec<(usize, usize)> = indices
        .values()
        .iter()
        .map(|&g| {
            let g = g as usize;
            let bi = offsets.partition_point(|&o| o <= g) - 1;
            (bi, g - offsets[bi])
        })
        .collect();
    let columns: Vec<arrow_array::ArrayRef> = (0..schema.fields().len())
        .map(|i| {
            let per_batch: Vec<&dyn Array> =
                batches.iter().map(|b| b.column(i).as_ref()).collect();
            arrow::compute::interleave(&per_batch, &pairs)
                .map_err(|e| BasinError::internal(format!("topk re-sort interleave {i}: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;
    let out = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("topk re-sort batch: {e}")))?;
    Ok(vec![out])
}

/// Two-phase global top-k for `ORDER BY <col> {ASC|DESC} LIMIT limit [OFFSET
/// offset]`.
///
/// The old implementation concatenated EVERY column of EVERY surviving batch
/// (`concat_batches`) before sorting — an O(n × width) materialisation of the
/// whole result set even though the answer is only the `limit` rows of the
/// window. At 1M rows that concat (not the partial sort) dominated the OFFSET
/// pagination residual.
///
/// The two phases here keep the work O(n) in the key column plus O(k) in the
/// output window, where `k = limit` (`offset` rows are sorted into the prefix
/// but never materialised across the wide columns):
///
///   1. Key scan. Concatenate ONLY the ORDER BY key column (one array, 1M
///      cheap scalars — not the full-width rows) and `sort_to_indices` it with
///      a partial-sort bound of `limit + offset`. Arrow's `sort_to_indices`
///      with a `Some(k)` limit does a bounded partial sort, so this is O(n) +
///      O(k log k), never a full O(n log n) sort.
///   2. Window materialise. Map each surviving global index back to its
///      `(batch_idx, row_idx)` via per-batch row offsets, drop the first
///      `offset` of them, and `interleave` each output column over the
///      per-batch arrays for the remaining ≤ `limit` index pairs. Only the
///      window rows are ever touched in the wide columns.
fn apply_order_by_limit(
    batches: Vec<RecordBatch>,
    col: &str,
    ascending: bool,
    limit: usize,
    offset: usize,
    schema: &Arc<Schema>,
) -> Result<Vec<RecordBatch>> {
    use arrow::compute::{SortOptions, concat, interleave, sort_to_indices};
    use arrow_array::Array;

    if batches.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    let col_idx = schema.index_of(col).map_err(|_| {
        BasinError::InvalidSchema(format!("ORDER BY column '{col}' not in result schema"))
    })?;

    // ── Phase 1: key scan ────────────────────────────────────────────────────
    // Concatenate ONLY the sort-key column across batches. `take`-window is
    // limit + offset rows of the GLOBAL order (we keep `offset` rows in the
    // prefix so the slice below lands on the right window). `saturating_add`
    // guards a pathological OFFSET near usize::MAX; an over-large bound just
    // sorts the whole key column (correct, never wrong).
    let key_window = limit.saturating_add(offset);
    let key_cols: Vec<&dyn arrow_array::Array> =
        batches.iter().map(|b| b.column(col_idx).as_ref()).collect();
    let merged_key = concat(&key_cols)
        .map_err(|e| BasinError::internal(format!("order_by key concat: {e}")))?;

    // nulls_first=false: NULLs sort LAST (DataFusion default for ASC).
    // For DESC, NULLs sort FIRST (DataFusion default), so nulls_first=true.
    let opts = SortOptions {
        descending: !ascending,
        nulls_first: !ascending,
    };
    let indices = sort_to_indices(merged_key.as_ref(), Some(opts), Some(key_window))
        .map_err(|e| BasinError::internal(format!("order_by sort_to_indices: {e}")))?;

    // Drop the OFFSET prefix; everything past it (≤ limit rows) is the window.
    let total = indices.len();
    if offset >= total {
        return Ok(Vec::new());
    }
    let window = &indices.values()[offset..total];
    if window.is_empty() {
        return Ok(Vec::new());
    }

    // ── Phase 2: window materialise ──────────────────────────────────────────
    // Map each surviving GLOBAL index → (batch_idx, row_idx) via per-batch row
    // offsets, then `interleave` each output column over the per-batch arrays
    // for only the window's ≤ limit index pairs. The wide columns are touched
    // for the window rows alone — never the full n rows.
    let mut batch_offsets: Vec<usize> = Vec::with_capacity(batches.len() + 1);
    let mut acc = 0usize;
    batch_offsets.push(0);
    for b in &batches {
        acc += b.num_rows();
        batch_offsets.push(acc);
    }
    let pairs: Vec<(usize, usize)> = window
        .iter()
        .map(|&g| {
            let g = g as usize;
            // partition_point gives the first offset strictly greater than g;
            // its index minus one is the owning batch.
            let bi = batch_offsets.partition_point(|&o| o <= g) - 1;
            (bi, g - batch_offsets[bi])
        })
        .collect();

    let columns: Vec<arrow_array::ArrayRef> = (0..schema.fields().len())
        .map(|i| {
            let per_batch: Vec<&dyn arrow_array::Array> =
                batches.iter().map(|b| b.column(i).as_ref()).collect();
            interleave(&per_batch, &pairs)
                .map_err(|e| BasinError::internal(format!("order_by interleave col {i}: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;

    let result = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("order_by result batch: {e}")))?;

    Ok(vec![result])
}

/// Apply one or more `IS NULL` column filters to `batches`. For each
/// column named in `is_null_cols`, rows where the column is NOT null are
/// excluded. Multiple column names are AND-ed (a row survives only when
/// ALL listed columns are null).
///
/// Uses `arrow::compute::is_null` (from `arrow-arith`) to build a boolean
/// mask and `arrow_select::filter::filter_record_batch` to select the
/// passing rows.
fn apply_is_null_filter(
    batches: Vec<RecordBatch>,
    is_null_cols: &[String],
) -> Result<Vec<RecordBatch>> {
    use arrow::compute::is_null as arrow_is_null;
    use arrow_select::filter::filter_record_batch;

    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // Build an AND mask over all IS NULL columns.
        let mut mask: Option<arrow_array::BooleanArray> = None;
        for col_name in is_null_cols {
            let col = batch
                .column_by_name(col_name)
                .ok_or_else(|| BasinError::InvalidSchema(format!("IS NULL: unknown column {col_name}")))?;
            let col_mask = arrow_is_null(col.as_ref())
                .map_err(|e| BasinError::internal(format!("is_null kernel: {e}")))?;
            mask = Some(match mask {
                None => col_mask,
                Some(prev) => {
                    // AND: row survives only when BOTH masks are true.
                    arrow::compute::and(&prev, &col_mask)
                        .map_err(|e| BasinError::internal(format!("is_null and: {e}")))?
                }
            });
        }
        if let Some(m) = mask {
            let filtered = filter_record_batch(&batch, &m)
                .map_err(|e| BasinError::internal(format!("is_null filter: {e}")))?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        } else {
            out.push(batch);
        }
    }
    Ok(out)
}

/// A compiled form of a single `col IN (v1, v2, …, vN)` predicate,
/// built once at filter-prep time.
///
/// For `Int64` and `Utf8` value lists a `HashSet`-backed probe is used
/// (O(1) per row after the O(N) build).  For every other homogeneous or
/// mixed-type list we fall back to the OR-chain path via
/// [`basin_storage::evaluate_compound`].
enum CompiledInPred {
    /// `HashSet<i64>` for homogeneous `Int64` lists.
    Int64Set(String, std::collections::HashSet<i64>),
    /// `HashSet<String>` for homogeneous `Utf8` lists.
    Utf8Set(String, std::collections::HashSet<String>),
    /// OR-chain fallback for mixed or unsupported types.
    Fallback(String, Vec<ScalarValue>),
}

impl CompiledInPred {
    /// Compile a `(col, vals)` pair into the most efficient probe form.
    fn compile(col: String, vals: Vec<ScalarValue>) -> Self {
        if vals.is_empty() {
            return Self::Fallback(col, vals);
        }
        // Detect homogeneous Int64.
        let all_i64 = vals.iter().all(|v| matches!(v, ScalarValue::Int64(_)));
        if all_i64 {
            let set: std::collections::HashSet<i64> = vals
                .iter()
                .map(|v| match v {
                    ScalarValue::Int64(n) => *n,
                    _ => unreachable!(),
                })
                .collect();
            return Self::Int64Set(col, set);
        }
        // Detect homogeneous Utf8.
        let all_utf8 = vals.iter().all(|v| matches!(v, ScalarValue::Utf8(_)));
        if all_utf8 {
            let set: std::collections::HashSet<String> = vals
                .into_iter()
                .map(|v| match v {
                    ScalarValue::Utf8(s) => s,
                    _ => unreachable!(),
                })
                .collect();
            return Self::Utf8Set(col, set);
        }
        // Mixed / unsupported — fall back to OR-chain.
        Self::Fallback(col, vals)
    }

    /// Evaluate this predicate against `batch` and return a boolean mask.
    ///
    /// For `Int64Set` and `Utf8Set` the probe is O(rows) after the one-time
    /// O(N) HashSet build in [`compile`]. For `Fallback` the OR-chain
    /// delegate handles it.
    fn evaluate(&self, batch: &RecordBatch) -> Result<arrow_array::BooleanArray> {
        match self {
            Self::Int64Set(col, set) => {
                let col_idx = batch
                    .schema()
                    .index_of(col)
                    .map_err(|_| BasinError::InvalidSchema(format!("IN: unknown column {col}")))?;
                let arr = batch.column(col_idx);
                // If the column isn't Int64 (schema mismatch, e.g. Int32 after
                // a projection), fall through to a safe false-mask rather than
                // panicking.  Real schemas always match their declared type.
                let Some(i64_arr) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() else {
                    // Return all-false — no row can match.
                    return Ok(arrow_array::BooleanArray::from(vec![false; batch.num_rows()]));
                };
                let mask: arrow_array::BooleanArray = i64_arr
                    .iter()
                    .map(|opt| opt.map(|v| set.contains(&v)).unwrap_or(false))
                    .collect();
                Ok(mask)
            }
            Self::Utf8Set(col, set) => {
                let col_idx = batch
                    .schema()
                    .index_of(col)
                    .map_err(|_| BasinError::InvalidSchema(format!("IN: unknown column {col}")))?;
                let arr = batch.column(col_idx);
                let Some(str_arr) = arr.as_any().downcast_ref::<arrow_array::StringArray>() else {
                    return Ok(arrow_array::BooleanArray::from(vec![false; batch.num_rows()]));
                };
                let mask: arrow_array::BooleanArray = str_arr
                    .iter()
                    .map(|opt| opt.map(|v| set.contains(v)).unwrap_or(false))
                    .collect();
                Ok(mask)
            }
            Self::Fallback(col, vals) => {
                use basin_storage::{evaluate_compound, CompoundPredicate as CP};
                let pred = CP::In(col.clone(), vals.clone());
                evaluate_compound(batch, &pred)
                    .map_err(|e| BasinError::internal(format!("in_list fallback eval: {e}")))
            }
        }
    }
}

/// Apply one or more `col IN (lits)` filters to `batches`.  Each entry in
/// `in_list_preds` is a `(column_name, values)` pair; a row survives when
/// ALL listed IN-predicates match (AND semantics over the predicates, OR
/// semantics within each value list).
///
/// For homogeneous `Int64` or `Utf8` value lists a [`CompiledInPred`] with a
/// `HashSet` probe is used — O(1) per row after the one-time O(N) build.  For
/// mixed-type or unsupported lists the original OR-chain delegate handles it.
///
/// NULL mask entries (rows where the column is NULL) are treated as
/// non-matching — consistent with SQL three-valued-logic WHERE semantics.
/// `filter_record_batch` already skips NULL/false mask entries, so no
/// explicit null-to-false conversion is needed.
fn apply_in_list_filter(
    batches: Vec<RecordBatch>,
    in_list_preds: &[(String, Vec<ScalarValue>)],
) -> Result<Vec<RecordBatch>> {
    use arrow_select::filter::filter_record_batch;

    // Compile each predicate once (builds the HashSet here, not per-batch).
    let compiled: Vec<CompiledInPred> = in_list_preds
        .iter()
        .map(|(col, vals)| CompiledInPred::compile(col.clone(), vals.clone()))
        .collect();

    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        // Build one mask per compiled predicate, then AND them together.
        let mut mask: Option<arrow_array::BooleanArray> = None;
        for cp in &compiled {
            let col_mask = cp.evaluate(&batch)?;
            mask = Some(match mask {
                None => col_mask,
                Some(prev) => arrow::compute::and_kleene(&prev, &col_mask)
                    .map_err(|e| BasinError::internal(format!("in_list and: {e}")))?,
            });
        }
        if let Some(m) = mask {
            let filtered = filter_record_batch(&batch, &m)
                .map_err(|e| BasinError::internal(format!("in_list filter_batch: {e}")))?;
            if filtered.num_rows() > 0 {
                out.push(filtered);
            }
        } else {
            out.push(batch);
        }
    }
    Ok(out)
}

/// Truncate the merged batches to at most `limit` rows total. Empty batches
/// are dropped so the caller doesn't see zero-row trailers.
fn apply_limit(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    if limit == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(batches.len());
    let mut remaining = limit;
    for b in batches {
        if remaining == 0 {
            break;
        }
        if b.num_rows() <= remaining {
            remaining -= b.num_rows();
            out.push(b);
        } else {
            // Slice keeps the underlying buffers; no copy.
            out.push(b.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

/// Compute a list of aggregate functions over `batches` and return a single
/// `ExecResult::Rows` with one output row. The output schema has one column
/// per aggregate in the same order as `aggs`.
///
/// Supports: `COUNT(*)` → `Int64`, `SUM(col)` → `Int64`, `MIN(col)` → `Int64`,
/// `MAX(col)` → `Int64`. SUM/MIN/MAX on non-Int64 columns fall back (return an
/// error) rather than silently widening or narrowing. NULL values in the source
/// are handled correctly: SUM(col) skips nulls, MIN/MAX skip nulls;
/// COUNT(*) counts all rows including those with NULLs.
fn apply_aggregates(
    batches: Vec<RecordBatch>,
    aggs: &[AggregateFn],
    _table_schema: &Arc<Schema>,
) -> Result<ExecResult> {
    use arrow::compute::min as arrow_min;
    use arrow::compute::max as arrow_max;
    use arrow::compute::sum as arrow_sum;
    use arrow_array::{Int64Array, ArrayRef};
    use arrow_schema::{DataType, Field};

    // Compute per-batch partial aggregates, then combine.
    // COUNT(*): accumulate row counts.
    // SUM/MIN/MAX: accumulate over all batches.
    struct Acc {
        count: i64,
        sum: Option<i64>,
        min: Option<i64>,
        max: Option<i64>,
    }

    let mut accs: Vec<Acc> = aggs
        .iter()
        .map(|_| Acc {
            count: 0,
            sum: None,
            min: None,
            max: None,
        })
        .collect();

    for batch in &batches {
        for (i, agg) in aggs.iter().enumerate() {
            let acc = &mut accs[i];
            match agg {
                AggregateFn::CountStar => {
                    acc.count += batch.num_rows() as i64;
                }
                AggregateFn::Sum(col) => {
                    // Schema evolution: a file written before ALTER ADD COLUMN
                    // lacks the column entirely — every row there is NULL, and
                    // SUM ignores NULLs, so this batch contributes nothing.
                    let Ok(col_idx) = batch.schema().index_of(col) else {
                        continue;
                    };
                    let arr = batch.column(col_idx);
                    let i64_arr = arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            BasinError::internal(format!("SUM: column {col} is not Int64"))
                        })?;
                    if let Some(partial) = arrow_sum(i64_arr) {
                        acc.sum = Some(acc.sum.unwrap_or(0) + partial);
                    }
                }
                AggregateFn::Min(col) => {
                    // Schema evolution: a file written before ALTER ADD COLUMN
                    // lacks the column entirely — every row there is NULL, and
                    // MIN ignores NULLs, so this batch contributes nothing.
                    let Ok(col_idx) = batch.schema().index_of(col) else {
                        continue;
                    };
                    let arr = batch.column(col_idx);
                    let i64_arr = arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            BasinError::internal(format!("MIN: column {col} is not Int64"))
                        })?;
                    if let Some(partial) = arrow_min(i64_arr) {
                        acc.min = Some(match acc.min {
                            Some(prev) => prev.min(partial),
                            None => partial,
                        });
                    }
                }
                AggregateFn::Max(col) => {
                    // Schema evolution: a file written before ALTER ADD COLUMN
                    // lacks the column entirely — every row there is NULL, and
                    // MAX ignores NULLs, so this batch contributes nothing.
                    let Ok(col_idx) = batch.schema().index_of(col) else {
                        continue;
                    };
                    let arr = batch.column(col_idx);
                    let i64_arr = arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            BasinError::internal(format!("MAX: column {col} is not Int64"))
                        })?;
                    if let Some(partial) = arrow_max(i64_arr) {
                        acc.max = Some(match acc.max {
                            Some(prev) => prev.max(partial),
                            None => partial,
                        });
                    }
                }
            }
        }
    }

    // Build the output schema and result arrays.
    let mut fields: Vec<arrow_schema::FieldRef> = Vec::with_capacity(aggs.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(aggs.len());

    for (i, agg) in aggs.iter().enumerate() {
        let acc = &accs[i];
        match agg {
            AggregateFn::CountStar => {
                fields.push(Arc::new(Field::new("count(*)", DataType::Int64, false)));
                columns.push(Arc::new(Int64Array::from(vec![acc.count])) as ArrayRef);
            }
            AggregateFn::Sum(col) => {
                // Derive output name the same way DataFusion does: `sum(col)`.
                fields.push(Arc::new(Field::new(
                    format!("sum({col})"),
                    DataType::Int64,
                    true, // nullable: NULL when no rows
                )));
                columns.push(Arc::new(Int64Array::from(vec![acc.sum])) as ArrayRef);
            }
            AggregateFn::Min(col) => {
                fields.push(Arc::new(Field::new(
                    format!("min({col})"),
                    DataType::Int64,
                    true,
                )));
                columns.push(Arc::new(Int64Array::from(vec![acc.min])) as ArrayRef);
            }
            AggregateFn::Max(col) => {
                fields.push(Arc::new(Field::new(
                    format!("max({col})"),
                    DataType::Int64,
                    true,
                )));
                columns.push(Arc::new(Int64Array::from(vec![acc.max])) as ArrayRef);
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("aggregate result batch: {e}")))?;

    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        let mut s = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        s.pop().unwrap()
    }

    // ── batch_matches_predicates numeric coercion ────────────────────────────
    //
    // Regression for the Int32-PK crossed-value race: an `Int64` scalar
    // predicate must compare BY VALUE against narrower integer columns
    // (Int16/Int32) and string-variant columns, not via a strict same-type
    // downcast. A strict downcast made hot UPDATE overlays (decoded as Int32
    // arrays) invisible to the point-SELECT probe, dropping the query onto a
    // cold path that nondeterministically served another key's row.
    fn one_row_batch(field: arrow_schema::Field, col: ArrayRef) -> RecordBatch {
        use arrow_schema::Schema as ArrowSchema;
        let schema = Arc::new(ArrowSchema::new(vec![field]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    use arrow_array::ArrayRef;

    #[test]
    fn batch_matches_int32_column_vs_int64_scalar() {
        use arrow_schema::{DataType, Field};
        let b = one_row_batch(
            Field::new("id", DataType::Int32, false),
            Arc::new(arrow_array::Int32Array::from(vec![4])),
        );
        // Eq matches by value across the Int32/Int64 width gap.
        assert!(batch_matches_predicates(
            &b,
            &[Predicate::Eq("id".into(), ScalarValue::Int64(4))]
        ));
        assert!(!batch_matches_predicates(
            &b,
            &[Predicate::Eq("id".into(), ScalarValue::Int64(2))]
        ));
        // Gt / Lt also widen.
        assert!(batch_matches_predicates(
            &b,
            &[Predicate::Gt("id".into(), ScalarValue::Int64(3))]
        ));
        assert!(batch_matches_predicates(
            &b,
            &[Predicate::Lt("id".into(), ScalarValue::Int64(5))]
        ));
        assert!(!batch_matches_predicates(
            &b,
            &[Predicate::Gt("id".into(), ScalarValue::Int64(4))]
        ));
    }

    #[test]
    fn batch_matches_int16_column_vs_int64_scalar() {
        use arrow_schema::{DataType, Field};
        let b = one_row_batch(
            Field::new("k", DataType::Int16, false),
            Arc::new(arrow_array::Int16Array::from(vec![20i16])),
        );
        assert!(batch_matches_predicates(
            &b,
            &[Predicate::Eq("k".into(), ScalarValue::Int64(20))]
        ));
        assert!(!batch_matches_predicates(
            &b,
            &[Predicate::Eq("k".into(), ScalarValue::Int64(21))]
        ));
    }

    #[test]
    fn batch_matches_largeutf8_column_vs_utf8_scalar() {
        use arrow_schema::{DataType, Field};
        let b = one_row_batch(
            Field::new("name", DataType::LargeUtf8, false),
            Arc::new(arrow_array::LargeStringArray::from(vec!["hi"])),
        );
        assert!(batch_matches_predicates(
            &b,
            &[Predicate::Eq("name".into(), ScalarValue::Utf8("hi".into()))]
        ));
        assert!(!batch_matches_predicates(
            &b,
            &[Predicate::Eq("name".into(), ScalarValue::Utf8("bye".into()))]
        ));
    }

    #[test]
    fn batch_matches_float32_column_vs_float64_scalar() {
        use arrow_schema::{DataType, Field};
        let b = one_row_batch(
            Field::new("x", DataType::Float32, false),
            Arc::new(arrow_array::Float32Array::from(vec![2.5f32])),
        );
        assert!(batch_matches_predicates(
            &b,
            &[Predicate::Eq("x".into(), ScalarValue::Float64(2.5))]
        ));
        assert!(batch_matches_predicates(
            &b,
            &[Predicate::Gt("x".into(), ScalarValue::Float64(2.0))]
        ));
    }

    #[test]
    fn matches_select_with_eq_int_literal() {
        let stmt = parse_one("SELECT id, name FROM users WHERE id = 5");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.table.as_str(), "users");
        let col_names: Vec<String> = plan
            .projection
            .as_ref()
            .expect("expected projection")
            .iter()
            .map(|item| match item {
                ProjectionItem::Column(c) => c.clone(),
                ProjectionItem::Computed { alias, .. } => alias.clone(),
            })
            .collect();
        assert_eq!(col_names, vec!["id", "name"]);
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Eq(col, ScalarValue::Int64(v)) => {
                assert_eq!(col, "id");
                assert_eq!(*v, 5);
            }
            other => panic!("unexpected predicate: {other:?}"),
        }
        assert!(plan.limit.is_none());
    }

    #[test]
    fn matches_select_star_with_string_literal() {
        let stmt = parse_one("SELECT * FROM events WHERE name = 'alice' LIMIT 10");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert!(
            plan.projection.is_none(),
            "wildcard should mean no projection"
        );
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Eq(col, ScalarValue::Utf8(v)) => {
                assert_eq!(col, "name");
                assert_eq!(v, "alice");
            }
            other => panic!("unexpected predicate: {other:?}"),
        }
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn matches_select_without_where() {
        let stmt = parse_one("SELECT id FROM t");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert!(plan.predicates.is_empty());
    }

    #[test]
    fn matches_gt_predicate() {
        let stmt = parse_one("SELECT id FROM t WHERE k > 10");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Gt(col, ScalarValue::Int64(10)) => assert_eq!(col, "k"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_lt_predicate() {
        let stmt = parse_one("SELECT id FROM t WHERE k < 5");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Lt(col, ScalarValue::Int64(5)) => assert_eq!(col, "k"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_gteq_predicate_as_gt_predecessor() {
        let stmt = parse_one("SELECT * FROM t WHERE id >= 6000");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        // >=6000 is encoded as >5999 so existing Predicate::Gt handles it.
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Gt(col, ScalarValue::Int64(5999)) => assert_eq!(col, "id"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_lteq_predicate_as_lt_successor() {
        let stmt = parse_one("SELECT * FROM t WHERE id <= 100");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        // <=100 is encoded as <101.
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Lt(col, ScalarValue::Int64(101)) => assert_eq!(col, "id"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_order_by_limit_with_inequality() {
        let stmt =
            parse_one("SELECT * FROM t WHERE id >= 6000 ORDER BY id DESC LIMIT 10");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.order_by, Some(("id".to_string(), false)));
        assert_eq!(plan.predicates.len(), 1);
        match &plan.predicates[0] {
            Predicate::Gt(col, ScalarValue::Int64(5999)) => assert_eq!(col, "id"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_order_by_limit_offset_captures_offset() {
        // The pagination shape now takes the fast path WITH the OFFSET captured
        // (previously OFFSET forced a DataFusion fallback).
        let stmt = parse_one("SELECT id FROM t ORDER BY id DESC LIMIT 50 OFFSET 100");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.limit, Some(50));
        assert_eq!(plan.offset, Some(100));
        assert_eq!(plan.order_by, Some(("id".to_string(), false)));
    }

    #[test]
    fn offset_without_order_by_falls_through() {
        // OFFSET without a total order has implementation-defined row choice in
        // PG; the fast path must NOT pin it — fall through to DataFusion.
        let stmt = parse_one("SELECT id FROM t LIMIT 50 OFFSET 100");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn offset_without_limit_falls_through() {
        // OFFSET without LIMIT is unbounded paging — leave it to DataFusion.
        let stmt = parse_one("SELECT id FROM t ORDER BY id DESC OFFSET 100");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn offset_zero_on_order_by_limit_is_captured() {
        let stmt = parse_one("SELECT id FROM t ORDER BY id ASC LIMIT 10 OFFSET 0");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.offset, Some(0));
        assert_eq!(plan.limit, Some(10));
    }

    #[test]
    fn matches_between_as_two_predicates() {
        let stmt = parse_one("SELECT * FROM t WHERE id BETWEEN 100 AND 200");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 2);
        // BETWEEN 100 AND 200 → Gt(id, 99) AND Lt(id, 201)
        match (&plan.predicates[0], &plan.predicates[1]) {
            (
                Predicate::Gt(c1, ScalarValue::Int64(99)),
                Predicate::Lt(c2, ScalarValue::Int64(201)),
            ) => {
                assert_eq!(c1, "id");
                assert_eq!(c2, "id");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn matches_and_of_two_atoms() {
        let stmt = parse_one("SELECT * FROM t WHERE id = 1 AND k > 5");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(plan.predicates.len(), 2);
        assert!(plan.is_null_cols.is_empty());
    }

    #[test]
    fn matches_is_null_predicate() {
        let stmt = parse_one("SELECT * FROM t WHERE s IS NULL");
        let plan = match_simple_select(&stmt).expect("fast path should match IS NULL");
        assert!(plan.predicates.is_empty());
        assert_eq!(plan.is_null_cols, vec!["s".to_string()]);
    }

    #[test]
    fn matches_is_null_combined_with_comparison() {
        let stmt = parse_one("SELECT * FROM t WHERE s IS NULL AND id > 5");
        let plan = match_simple_select(&stmt).expect("fast path should match IS NULL AND compare");
        assert_eq!(plan.predicates.len(), 1);
        assert_eq!(plan.is_null_cols, vec!["s".to_string()]);
    }

    #[test]
    fn rejects_is_not_null() {
        // IS NOT NULL is not supported — falls through to DataFusion.
        let stmt = parse_one("SELECT * FROM t WHERE s IS NOT NULL");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_four_atom_conjunction() {
        // Four atoms (BETWEEN = 2 + two more) exceeds the cap → DataFusion.
        let stmt =
            parse_one("SELECT * FROM t WHERE id BETWEEN 1 AND 10 AND k > 2 AND s = 'x'");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_join() {
        let stmt = parse_one("SELECT a.id FROM a JOIN b ON a.id = b.id");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_order_by() {
        let stmt = parse_one("SELECT id FROM t WHERE id = 1 ORDER BY id");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_group_by() {
        let stmt = parse_one("SELECT id FROM t GROUP BY id");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_or_predicate() {
        // OR is not supported — always falls through to DataFusion.
        let stmt = parse_one("SELECT id FROM t WHERE id = 1 OR id = 2");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn matches_aggregate_count_star() {
        let stmt = parse_one("SELECT COUNT(*) FROM t");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        assert_eq!(
            plan.aggregates.as_deref(),
            Some([AggregateFn::CountStar].as_slice())
        );
        assert!(plan.predicates.is_empty());
        assert!(plan.limit.is_none());
    }

    #[test]
    fn matches_aggregate_count_sum_with_between() {
        let stmt =
            parse_one("SELECT COUNT(*), SUM(id) FROM t WHERE id BETWEEN 100 AND 200");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        let aggs = plan.aggregates.as_deref().expect("should be aggregate");
        assert_eq!(aggs.len(), 2);
        assert_eq!(aggs[0], AggregateFn::CountStar);
        assert_eq!(aggs[1], AggregateFn::Sum("id".to_string()));
        assert_eq!(plan.predicates.len(), 2); // BETWEEN expands to Gt + Lt
    }

    #[test]
    fn matches_aggregate_min_max() {
        let stmt = parse_one("SELECT MIN(k), MAX(k) FROM t");
        let plan = match_simple_select(&stmt).expect("fast path should match");
        let aggs = plan.aggregates.as_deref().expect("should be aggregate");
        assert_eq!(aggs.len(), 2);
        assert_eq!(aggs[0], AggregateFn::Min("k".to_string()));
        assert_eq!(aggs[1], AggregateFn::Max("k".to_string()));
    }

    #[test]
    fn rejects_aggregate_with_order_by() {
        // Aggregate + ORDER BY is not sensible for a single-row result.
        let stmt = parse_one("SELECT COUNT(*) FROM t ORDER BY id DESC LIMIT 1");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_mixed_aggregate_and_column() {
        // Mixed aggregate + column projection is not supported.
        let stmt = parse_one("SELECT COUNT(*), id FROM t");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_aliased_table() {
        let stmt = parse_one("SELECT t.id FROM t AS t WHERE t.id = 1");
        assert!(match_simple_select(&stmt).is_none());
    }

    #[test]
    fn rejects_expression_in_projection() {
        let stmt = parse_one("SELECT id + 1 FROM t");
        assert!(match_simple_select(&stmt).is_none());
    }

    // ── IN-list recogniser tests ──────────────────────────────────────────────

    #[test]
    fn matches_in_list_of_int_literals() {
        let stmt = parse_one("SELECT * FROM t WHERE id IN (1, 2, 3)");
        let plan = match_simple_select(&stmt).expect("in-list should match fast path");
        assert!(plan.predicates.is_empty(), "no Predicate atoms for IN-list");
        assert_eq!(plan.in_list_preds.len(), 1);
        let (col, vals) = &plan.in_list_preds[0];
        assert_eq!(col, "id");
        assert_eq!(
            vals,
            &[
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Int64(3)
            ]
        );
    }

    #[test]
    fn matches_in_list_of_string_literals() {
        let stmt = parse_one("SELECT * FROM t WHERE name IN ('alice', 'bob')");
        let plan = match_simple_select(&stmt).expect("string in-list should match fast path");
        assert_eq!(plan.in_list_preds.len(), 1);
        let (col, vals) = &plan.in_list_preds[0];
        assert_eq!(col, "name");
        assert_eq!(
            vals,
            &[ScalarValue::Utf8("alice".to_string()), ScalarValue::Utf8("bob".to_string())]
        );
    }

    #[test]
    fn empty_in_list_is_always_empty() {
        // SQL semantics: `x IN ()` is always false → always_empty = true.
        // sqlparser rejects `IN ()` (empty list is a syntax error in the
        // PostgreSQL dialect), so this test exercises our fallback behaviour
        // by constructing the AST node directly, bypassing the SQL text parser.
        let col_expr = Box::new(Expr::Identifier(sqlparser::ast::Ident::new("id")));
        let empty_in = Expr::InList {
            expr: col_expr,
            list: vec![],
            negated: false,
        };
        let result = parse_where(&empty_in);
        let pw = result.expect("empty IN-list should produce a ParsedWhere");
        assert!(pw.always_false, "empty IN-list must mark always_false");
    }

    #[test]
    fn negated_in_list_falls_through_to_datafusion() {
        // NOT IN is not handled by the fast path.
        let stmt = parse_one("SELECT * FROM t WHERE id NOT IN (1, 2, 3)");
        assert!(
            match_simple_select(&stmt).is_none(),
            "NOT IN should fall through to DataFusion"
        );
    }

    #[test]
    fn matches_100_element_in_list_with_multi_col_projection() {
        // Exactly the bench shape: SELECT id, user_id, amount FROM events WHERE id IN (...)
        // with 100 integer values.
        let vals: String = (0..100i64).map(|k| (k * 7 + 1).to_string()).collect::<Vec<_>>().join(",");
        let sql = format!("SELECT id, user_id, amount FROM events WHERE id IN ({vals})");
        let stmt = parse_one(&sql);
        let plan = match_simple_select(&stmt).expect("100-element IN-list with multi-col projection must match fast path");
        assert!(plan.predicates.is_empty(), "no Predicate atoms — only in_list_preds");
        assert_eq!(plan.in_list_preds.len(), 1);
        let (col, vals_out) = &plan.in_list_preds[0];
        assert_eq!(col, "id");
        assert_eq!(vals_out.len(), 100);
        // Check projection has 3 columns.
        let proj = plan.projection.as_ref().expect("must have projection");
        assert_eq!(proj.len(), 3);
    }

    #[test]
    fn in_list_with_non_literal_element_falls_through() {
        // A subquery or column reference in the list is not a literal.
        let stmt = parse_one("SELECT * FROM t WHERE id IN (1, id)");
        assert!(
            match_simple_select(&stmt).is_none(),
            "non-literal IN element should fall through to DataFusion"
        );
    }

    /// Timing micro-bench: measures median latency of the fast-path SELECT
    /// (catalog lookup + Parquet read) over 200 iterations on a 10k-row table.
    /// Run with:
    ///   `CARGO_BUILD_JOBS=1 cargo test -p basin-engine --lib \
    ///    fast_select::tests::bench_fast_select_latency -- --nocapture --ignored`
    #[tokio::test]
    #[ignore]
    async fn bench_fast_select_latency() {
        use std::sync::Arc;
        use std::time::Instant;

        use crate::{Engine, EngineConfig};
        use basin_catalog::{Catalog, InMemoryCatalog};
        use basin_common::ProjectId;
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        let engine = Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        });

        let project = ProjectId::new();
        let sess = engine.open_session(project).await.unwrap();

        // Create a table and insert 10k rows.
        sess.execute("CREATE TABLE audit_log (id BIGINT, user_id BIGINT, action TEXT, ts BIGINT)")
            .await
            .unwrap();
        let mut tuples = Vec::with_capacity(10_000);
        for i in 0i64..10_000 {
            tuples.push(format!("({i}, {}, 'login', {})", i % 100, i * 1000));
        }
        sess.execute(&format!(
            "INSERT INTO audit_log VALUES {}",
            tuples.join(",")
        ))
        .await
        .unwrap();

        const ITERS: usize = 200;
        let mut samples: Vec<f64> = Vec::with_capacity(ITERS);

        // Warm up.
        for _ in 0..10 {
            let _ = sess
                .execute("SELECT id, user_id FROM audit_log WHERE id = 42")
                .await
                .unwrap();
        }

        for i in 0..ITERS {
            let t0 = Instant::now();
            let _ = sess
                .execute(&format!(
                    "SELECT id, user_id FROM audit_log WHERE id = {}",
                    i as i64 % 10_000
                ))
                .await
                .unwrap();
            samples.push(t0.elapsed().as_secs_f64() * 1000.0);
        }

        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = samples[ITERS / 2];
        let p95 = samples[(ITERS * 95) / 100];
        let p99 = samples[(ITERS * 99) / 100];
        let mean = samples.iter().sum::<f64>() / ITERS as f64;
        println!(
            "\nbench_fast_select_latency: mean={mean:.3}ms  p50={p50:.3}ms  p95={p95:.3}ms  p99={p99:.3}ms"
        );
    }

    // ── HashSet IN-list probe tests ───────────────────────────────────────────

    /// Build a small RecordBatch with an `id` (Int64) column and a `name`
    /// (Utf8) column for use in `CompiledInPred` unit tests.
    fn make_test_batch_i64_utf8() -> RecordBatch {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids: Int64Array = vec![
            Some(1), Some(2), Some(3), Some(50), None, Some(99),
        ]
        .into_iter()
        .collect();
        let names: StringArray = vec![
            Some("alice"), Some("bob"), Some("carol"), Some("dave"), Some("eve"), None,
        ]
        .into_iter()
        .collect();
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    /// `WHERE id IN (1,3,...,99)` with 100 integers uses the HashSet path and
    /// returns only the rows whose `id` is in the set.
    #[test]
    fn in_list_int64_fastpath() {
        // Build a set of 100 Int64 values that includes 1, 3, 50, 99 from our
        // test batch (but not 2 and not NULL).
        let vals: Vec<ScalarValue> = (0..100i64)
            .map(|k| ScalarValue::Int64(k * 1 + 1)) // 1..=100
            .collect();

        // CompiledInPred should resolve to Int64Set.
        let cp = CompiledInPred::compile("id".to_string(), vals);
        assert!(
            matches!(cp, CompiledInPred::Int64Set(_, _)),
            "homogeneous Int64 list must compile to Int64Set"
        );

        let batch = make_test_batch_i64_utf8();
        let mask = cp.evaluate(&batch).unwrap();

        // Row 0: id=1  → in set  → true
        // Row 1: id=2  → in set  → true
        // Row 2: id=3  → in set  → true
        // Row 3: id=50 → in set  → true
        // Row 4: id=NULL → false
        // Row 5: id=99 → in set  → true
        let expected = vec![true, true, true, true, false, true];
        let got: Vec<bool> = mask.iter().map(|v| v.unwrap_or(false)).collect();
        assert_eq!(got, expected);

        // Verify the filter actually removes the NULL row.
        let filtered = apply_in_list_filter(vec![batch], &[
            ("id".to_string(), (1..=100).map(ScalarValue::Int64).collect()),
        ])
        .unwrap();
        let total_rows: usize = filtered.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "5 non-null matching rows expected");
    }

    /// `WHERE name IN ('alice','carol')` uses the Utf8 HashSet path.
    #[test]
    fn in_list_utf8_fastpath() {
        let vals = vec![
            ScalarValue::Utf8("alice".to_string()),
            ScalarValue::Utf8("carol".to_string()),
        ];

        let cp = CompiledInPred::compile("name".to_string(), vals.clone());
        assert!(
            matches!(cp, CompiledInPred::Utf8Set(_, _)),
            "homogeneous Utf8 list must compile to Utf8Set"
        );

        let batch = make_test_batch_i64_utf8();
        let mask = cp.evaluate(&batch).unwrap();

        // Row 0: "alice" → true
        // Row 1: "bob"   → false
        // Row 2: "carol" → true
        // Row 3: "dave"  → false
        // Row 4: "eve"   → false
        // Row 5: NULL    → false
        let expected = vec![true, false, true, false, false, false];
        let got: Vec<bool> = mask.iter().map(|v| v.unwrap_or(false)).collect();
        assert_eq!(got, expected);

        let filtered = apply_in_list_filter(vec![batch], &[("name".to_string(), vals)]).unwrap();
        let total_rows: usize = filtered.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    /// `WHERE id IN (1, 'x')` — mixed scalar types (Int64 + Utf8) route to
    /// `CompiledInPred::Fallback`, not the HashSet paths.  This is a compile-
    /// time routing test; the fallback itself is not evaluated here because the
    /// storage-layer OR-chain errors on a genuine type mismatch (Int64 column
    /// vs Utf8 literal), which is the correct behaviour — the SQL parser should
    /// have rejected such a query before it reaches the filter.
    #[test]
    fn in_list_falls_back_for_mixed_types() {
        let vals_mixed = vec![
            ScalarValue::Int64(1),
            ScalarValue::Utf8("x".to_string()),
        ];
        let cp_mixed = CompiledInPred::compile("id".to_string(), vals_mixed);
        assert!(
            matches!(cp_mixed, CompiledInPred::Fallback(_, _)),
            "mixed Int64+Utf8 list must compile to Fallback, not HashSet"
        );

        // A list containing a Boolean value is also not Int64 or Utf8 — falls
        // through to Fallback.
        let vals_bool = vec![
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
        ];
        let cp_bool = CompiledInPred::compile("flag".to_string(), vals_bool);
        assert!(
            matches!(cp_bool, CompiledInPred::Fallback(_, _)),
            "Boolean list must compile to Fallback"
        );

        // Empty list also routes to Fallback.
        let cp_empty = CompiledInPred::compile("id".to_string(), vec![]);
        assert!(
            matches!(cp_empty, CompiledInPred::Fallback(_, _)),
            "empty list must compile to Fallback"
        );
    }
}
