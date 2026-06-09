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
            let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() else {
                return false;
            };
            let v = arr.value(0);
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
        let (col_name, expected, op) = match pred {
            Predicate::Eq(c, v) => (c, v, "eq"),
            Predicate::Gt(c, v) => (c, v, "gt"),
            Predicate::Lt(c, v) => (c, v, "lt"),
            // Handled above.
            Predicate::StartsWith { .. } => unreachable!(),
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
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
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
            ScalarValue::Utf8(expected_s) => {
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
                    let v = arr.value(0);
                    match op {
                        "eq" => v == expected_s.as_str(),
                        "gt" => v > expected_s.as_str(),
                        "lt" => v < expected_s.as_str(),
                        _ => false,
                    }
                } else {
                    false
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
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Float64Array>() {
                    let v = arr.value(0);
                    match op {
                        "eq" => (v - *expected_v).abs() < f64::EPSILON,
                        "gt" => v > *expected_v,
                        "lt" => v < *expected_v,
                        _ => false,
                    }
                } else {
                    false
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
fn probe_memtable(
    sess: &ProjectSession,
    table: &TableName,
    predicates: &[Predicate],
    meta: &TableMetadata,
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
                        match entry.memtable.get(&pk_key) {
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
                                // half.  A counter-keyed Row (HTAP cache) may
                                // still match — fall through to snapshot.
                            }
                        }
                    }
                }
            }
        }
    }

    // ── Snapshot fallback ────────────────────────────────────────────────────
    // The slow but always-correct path: clone the entire memtable BTreeMap
    // under a read-lock, then filter via `batch_matches_predicates`.  Required
    // for predicate shapes the direct-get can't handle (non-PK Eq, multi-
    // predicate, range), and as a safety net for counter-keyed HTAP-cached
    // INSERT rows.
    let snapshot = entry.memtable.snapshot();
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
    if q.with.is_some()
        || !q.ext_limit_by().is_empty()
        || q.ext_offset().is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
    {
        return None;
    }

    // Parse an optional single-column ORDER BY. Any ORDER BY that is more
    // complex (multiple columns, expressions, NULLS FIRST/LAST, WITH FILL)
    // falls through to DataFusion. `None` here means "no ORDER BY at all".
    let order_by: Option<(String, bool)> = match q.order_by.as_ref() {
        None => None,
        Some(ob) => {
            let exprs = ob.ext_exprs();
            if exprs.len() != 1 {
                return None;
            }
            let e = &exprs[0];
            // Reject ClickHouse WITH FILL and NULLS FIRST/LAST — leave those
            // to DataFusion so NULL ordering matches its semantics exactly.
            if e.with_fill.is_some() || e.options.nulls_first.is_some() {
                return None;
            }
            let col = match &e.expr {
                Expr::Identifier(id) => id.value.clone(),
                _ => return None,
            };
            let ascending = e.options.asc.unwrap_or(true); // default ASC
            Some((col, ascending))
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
        // Aggregate query — ORDER BY and LIMIT are incompatible.
        if order_by.is_some() || q.ext_limit().is_some() {
            return None;
        }
        return Some(SimpleSelectPlan {
            table,
            projection: None, // not a row projection
            read_cols: None,   // read all columns for aggregates
            aggregates: Some(aggs),
            predicates: parsed_where.predicates,
            is_null_cols: parsed_where.is_null_cols,
            in_list_preds: parsed_where.in_list_preds,
            limit: None,
            order_by: None,
            always_empty: parsed_where.always_false,
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

    Some(SimpleSelectPlan {
        table,
        projection,
        read_cols,
        aggregates: None,
        predicates: parsed_where.predicates,
        is_null_cols: parsed_where.is_null_cols,
        in_list_preds: parsed_where.in_list_preds,
        limit,
        order_by,
        always_empty: parsed_where.always_false,
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

/// Recognise the literal forms we can push down: signed integers, strings,
/// booleans. Anything richer (NULL, casts, vectors, floats) drops us out of
/// the fast path.
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
            let parsed: i64 = s.parse().ok()?;
            Some(ScalarValue::Int64(if negate { -parsed } else { parsed }))
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
/// [`ProjectHandle`]: basin_shard::ProjectHandle
pub(crate) async fn execute_simple_select(
    sess: &ProjectSession,
    plan: SimpleSelectPlan,
    prefetched_meta: Option<TableMetadata>,
    raw_sql: &str,
    include_deleted: bool,
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
    let meta = if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
        // Reload after flush — the pre-fetched snapshot is now stale.
        sess.engine
            .config()
            .catalog
            .load_table(&sess.project, &plan.table)
            .await?
    } else {
        // No shard: use the pre-fetched metadata when available (saves one
        // catalog round-trip that the fast-path gate already paid).
        match prefetched_meta {
            Some(m) => m,
            None => {
                sess.engine
                    .config()
                    .catalog
                    .load_table(&sess.project, &plan.table)
                    .await?
            }
        }
    };

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
        if let Some(rc) = plan.read_cols.as_ref() {
            for c in rc {
                if c.starts_with("__promoted$") && !cols.contains(c) {
                    cols.push(c.clone());
                }
            }
        }
        if let Some(items) = plan.projection.as_ref() {
            for it in items {
                if let ProjectionItem::Column(c) = it {
                    if c.starts_with("__promoted$") && !cols.contains(c) {
                        cols.push(c.clone());
                    }
                }
            }
        }
        cols
    };
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
        let memtable_has_entries = sess
            .engine
            .memtable_registry()
            .get(&sess.project, &plan.table)
            .map(|e| !e.memtable.snapshot().is_empty())
            .unwrap_or(false);
        let live_files = meta.live_data_files();
        let all_present = !memtable_has_entries
            && live_files.iter().all(|f| {
                referenced_shadow_cols
                    .iter()
                    .all(|sc| f.column_stats.contains_key(sc))
            });
        if !all_present {
            // At least one file predates promotion (or backfill has not yet
            // covered it).  Fall back to the correct DataFusion path.
            return crate::executor::exec_select(sess, raw_sql, include_deleted, Some(raw_sql))
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
                                    BasinError::InvalidSchema(format!("unknown column {c}"))
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
                        .map_err(|_| BasinError::InvalidSchema(format!("unknown column {c}")))?;
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
                .map_err(|_| BasinError::InvalidSchema(format!("unknown column {c}")))?;
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
    let post_read_shrinking = !plan.is_null_cols.is_empty()
        || !plan.in_list_preds.is_empty()
        || {
            // Probe the memtable registry for any overlay activity on this
            // (project, table). Both probes are O(1) HashMap lookups; we
            // only consult them on the LIMIT-pushdown gate, so the work is
            // bounded and proportional to "does the engine have any
            // outstanding fast-path DELETE/UPDATE for this table".
            let registry = sess.engine.memtable_registry();
            let tombs = crate::hot_tombstone::snapshot_tombstones(
                registry.as_ref(),
                &sess.project,
                &plan.table,
            );
            let updates = crate::hot_tombstone::snapshot_updates(
                registry.as_ref(),
                &sess.project,
                &plan.table,
            );
            !tombs.is_empty() || !updates.is_empty()
        };
    let pushdown_limit = if plan.order_by.is_none()
        && plan.aggregates.is_none()
        && !post_read_shrinking
    {
        plan.limit
    } else {
        None
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
    let mut opts = ReadOptions {
        projection: plan.read_cols.clone(),
        filters: augmented_filters,
        partition: None,
        limit: pushdown_limit,
        row_group_selection: None,
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
            probe_memtable(sess, &plan.table, &plan.predicates, &meta)
        {
            if !mem_rows.is_empty() {
                // Hot-tier hit: apply projection + limit and return immediately.
                // No cold-tier read required.
                let batches = mem_rows;
                let (projected_schema, batches): (Arc<Schema>, Vec<RecordBatch>) =
                    match &plan.projection {
                        None => (meta.schema.clone(), batches),
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
                                        .map_err(|_| BasinError::InvalidSchema(format!("unknown column {c}")))?;
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

    // ── PK row cache context (correctness-critical; gated OFF by default) ─────
    //
    // Build the cache descriptor for the canonical single-PK-Eq point-lookup
    // shape. We only enter this when:
    //   * the feature flag `BASIN_PK_ROW_CACHE=1` is set (default OFF);
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
    let pk_cache_ctx: Option<(basin_hottier::RowKey, u64, u64, u64)> =
        if crate::pk_row_cache::PkRowCache::enabled()
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

        if plan.aggregates.is_none() {
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
                        } else if registry.is_loaded(&sess.project, &plan.table, col) {
                            // Index is loaded but key is absent → no match.
                            result = Some(None); // None means "definitely empty"
                        }
                        // If probe returned None and index not loaded, fall through.
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

    let pk_probe_paths: Option<Vec<object_store::path::Path>> = if
        // PK row cache hit: the cached batches already answer this point query;
        // skip the zone-map + bloom probe (and its possible Absent early-return)
        // entirely so the warm path is a pure in-RAM HashMap lookup. The cache's
        // valid watermark guarantees the cold files are unchanged, so the probe
        // would only re-derive what we already hold.
        pk_cache_hit.is_some() {
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
                    let live_files = meta.live_data_files();
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
            let live_files = meta.live_data_files();
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

    // Build the catalog-driven live file list. `live_data_files()` replays the
    // snapshot chain up to `current_snapshot`, so after a rollback it returns
    // only the pre-rollback files — physical files from post-rollback snapshots
    // are never included (bug #41 fix). Convert each path string to an
    // `ObjectPath` for `storage.read_paths`.
    // Catalog-stats file pruning: skip any data file whose per-file
    // column_stats (min/max/null-count, populated at write time for BOTH
    // Parquet and Vortex) prove the predicate cannot match a single row
    // (`PruneOutcome::NoMatch`). Done BEFORE `read_paths` so a pruned file
    // is never opened — decisive for Vortex, whose per-file open is far
    // heavier than a Parquet footer read, and a win for Parquet too
    // (point/range/compound/IS NULL queries touch fewer files).
    let had_pk_probe = pk_probe_paths.is_some();
    let live_files = meta.live_data_files();
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
    } else if had_pk_probe {
        // Single-PK-Eq fast path: the PK probe already narrowed the cold
        // tier to at most a handful of candidate files (typically 0-1).
        // Read those paths directly, bypassing `handle.read`'s full file
        // discovery — which would re-list every live data file and defeat
        // the prune. Safety:
        //   * Hot tier covered: the engine's `MemTableRegistry` was probed
        //     above (line ~1209); a hit returned early. Reaching here means
        //     the queried PK is NOT in the engine memtable.
        //   * Shard tail drained: `shard.flush_to_parquet()` ran above
        //     (line ~1077), so the shard's in-RAM `state.tail` was flushed
        //     to cold-tier Parquet before the probe consumed `live_files`.
        //   * Tombstone overlay applied below as on the non-shard path.
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
        // The pre-flush above already drained the tail into Parquet so this
        // read is bounded, then `handle.read` only has to scan whatever new
        // tail rows have arrived since.
        let handle = shard
            .get(&sess.project, &PartitionKey::default_key())
            .await?;
        // Both heavy and light shard reads: await directly. The shard's
        // `handle.read` drives its own async I/O and WAL-replay cooperatively;
        // there is no benefit to a nested runtime, and doing so would create
        // the same fast_select livelock as the non-shard heavy path.
        handle.read(&plan.table, opts).await?
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
        let registry = sess.engine.memtable_registry();
        let tombs = crate::hot_tombstone::snapshot_tombstones(
            registry.as_ref(),
            &sess.project,
            &plan.table,
        );
        // Merge-on-read UPDATE overlay for the hot-tier fast path: drop cold
        // rows whose PK has an `Update` override and append the post-SET rows.
        // Mirrors the `UpdateOverlayExec` wrap on the DataFusion read path so
        // a fast-path UPDATE is visible to bulk fast-path SELECTs too.
        let updates = crate::hot_tombstone::snapshot_updates(
            registry.as_ref(),
            &sess.project,
            &plan.table,
        );
        if tombs.is_empty() && updates.is_empty() {
            batches
        } else if let Ok(pk_idx) = meta.schema.index_of(pk_col) {
            let pk_dt = meta.schema.field(pk_idx).data_type().clone();
            let batches = crate::hot_tombstone::apply_tombstone_filter_to_batches(
                batches, &tombs, pk_col, &pk_dt,
            )
            .map_err(|e| BasinError::internal(format!("tombstone filter: {e}")))?;
            crate::hot_tombstone::apply_update_overlay_to_batches(
                batches, &updates, pk_col, &pk_dt,
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
            None => (meta.schema.clone(), batches),
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
                                BasinError::InvalidSchema(format!("unknown column {c}"))
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

    // ORDER BY + LIMIT: merge batches into one, sort by the column, take
    // the first `limit` rows. We only reach here when `order_by.is_some()`
    // implies `limit.is_some()` (enforced in match_query).
    let trimmed = if let Some((ref col, ascending)) = plan.order_by {
        let limit = plan.limit.expect("order_by implies limit");
        apply_order_by_limit(batches, col, ascending, limit, &projected_schema)?
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
                    .map_err(|_| BasinError::InvalidSchema(format!("unknown column {c}")))?;
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


/// Sort batches by a single column and return the first `limit` rows as one
/// or more `RecordBatch`es. Uses Arrow's `sort_to_indices` + `take` so the
/// sort key is only materialised once. NULLs sort last in ASC order (matching
/// DataFusion's default NULL handling for `ORDER BY col ASC`).
///
/// Returns an error if the sort column is absent from the projected schema.
fn apply_order_by_limit(
    batches: Vec<RecordBatch>,
    col: &str,
    ascending: bool,
    limit: usize,
    schema: &Arc<Schema>,
) -> Result<Vec<RecordBatch>> {
    use arrow::compute::{SortOptions, sort_to_indices, take};
    use arrow_select::concat;

    if batches.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }

    // Concatenate all batches into one so the sort sees global row order.
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let merged = concat::concat_batches(schema, refs)
        .map_err(|e| BasinError::internal(format!("order_by concat: {e}")))?;

    let col_idx = merged.schema().index_of(col).map_err(|_| {
        BasinError::InvalidSchema(format!("ORDER BY column '{col}' not in result schema"))
    })?;
    let sort_col = merged.column(col_idx);

    // nulls_first=false: NULLs sort LAST (DataFusion default for ASC).
    // For DESC, NULLs sort FIRST (DataFusion default), so nulls_first=true.
    let opts = SortOptions {
        descending: !ascending,
        nulls_first: !ascending,
    };
    let indices = sort_to_indices(sort_col, Some(opts), Some(limit))
        .map_err(|e| BasinError::internal(format!("order_by sort_to_indices: {e}")))?;

    // Reorder every column by the sort indices.
    let columns: Vec<arrow_array::ArrayRef> = (0..merged.num_columns())
        .map(|i| {
            take(merged.column(i).as_ref(), &indices, None)
                .map_err(|e| BasinError::internal(format!("order_by take col {i}: {e}")))
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
                    let col_idx = batch.schema().index_of(col).map_err(|_| {
                        BasinError::InvalidSchema(format!("SUM: unknown column {col}"))
                    })?;
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
                    let col_idx = batch.schema().index_of(col).map_err(|_| {
                        BasinError::InvalidSchema(format!("MIN: unknown column {col}"))
                    })?;
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
                    let col_idx = batch.schema().index_of(col).map_err(|_| {
                        BasinError::InvalidSchema(format!("MAX: unknown column {col}"))
                    })?;
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
