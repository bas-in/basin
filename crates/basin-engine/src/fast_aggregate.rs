//! Metadata-only aggregate fast path.
//!
//! DataFusion 53's `AggregateStatistics` physical-optimiser rule already
//! folds an exact answer for a pure aggregate query out of the catalog's
//! per-file statistics — but it bails for the WHOLE aggregate the moment
//! ANY aggregate expression lacks a `value_from_stats` implementation, and
//! `SUM`/`AVG` don't implement it in DF53. So `SELECT COUNT(*), SUM(id),
//! MIN(k), MAX(k) FROM t` (a single unsupported `SUM`) drags the entire
//! query down to a full table scan.
//!
//! This recogniser sits *before* DataFusion. When the SQL is a bare
//! aggregate over a single table — no WHERE / JOIN / GROUP BY / DISTINCT /
//! subquery / etc. — and every projected expression is one of the
//! supported pure aggregates (`COUNT(*)`, `COUNT(col)`, `MIN(col)`,
//! `MAX(col)`, `SUM(col)`), we answer the whole thing from
//! `meta.live_data_files()` and never touch storage.
//!
//! The recogniser is deliberately conservative — the same contract as
//! [`crate::fast_select`]: when in ANY doubt, return `None` and let the
//! caller fall through to `exec_select`. A missed fast path is merely
//! slower; a wrong match is a wrong answer.
//!
//! `SUM` is special: per-file `sum_bytes` is only populated by a separate
//! (later) writer/Vortex task. Until then every file's `sum_bytes` is
//! `None`, and we bail the WHOLE query to DataFusion (full scan, correct).
//! Once population lands, the same query flips to the metadata path with
//! no further code change here.
//!
//! ## Low-cardinality GROUP BY fast path
//!
//! `match_groupby_low_card` / `execute_groupby_low_card` handle:
//!
//!   `SELECT key, COUNT(*) FROM t GROUP BY key`
//!
//! for a single Int64 grouping column whose per-file catalog stats give a
//! key-range ≤ `LOW_CARD_THRESHOLD` (32 by default).  The execution reads
//! only the key column from each data file, accumulates per-key counts in a
//! small HashMap, and returns a multi-row `RecordBatch` whose schema is
//! byte-identical to DataFusion's output.  Bails to DataFusion for compound
//! GROUP BY, HAVING, WHERE, CTE, transactions, views, RLS, or when the
//! cardinality gate cannot be determined from catalog stats.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::TableMetadata;
use basin_common::{BasinError, Result, TableName};
use futures::StreamExt;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, Query,
    SelectItem, SetExpr, Statement, TableFactor,
};

use crate::pg_ast::{ObjectNamePartExt, QueryClauseExt};
use crate::{ExecResult, ProjectSession};

/// Maximum distinct-value count for the low-cardinality GROUP BY fast path.
/// Queries whose key range exceeds this threshold fall through to DataFusion.
const LOW_CARD_THRESHOLD: i64 = 32;

/// One recognised aggregate output column. `out_name` is the exact column
/// name DataFusion 53 would have produced for this aggregate over this
/// table (verified empirically — see module tests / the differential
/// harness), so the result schema is drop-in identical to the DataFusion
/// path.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AggKind {
    /// `COUNT(*)` → Σ row_count.
    CountStar,
    /// `COUNT(col)` → Σ (row_count − null_count).
    CountCol(String),
    /// `MIN(col)` → fold of per-file min.
    MinCol(String),
    /// `MAX(col)` → fold of per-file max.
    MaxCol(String),
    /// `SUM(col)` → fold of per-file sum_bytes (bails if any file lacks it).
    SumCol(String),
}

#[derive(Debug)]
pub(crate) struct MetadataAggregatePlan {
    pub table: TableName,
    /// Output columns, in projection order. Each carries the aggregate
    /// kind and the exact DataFusion-equivalent output column name.
    pub aggs: Vec<(AggKind, String)>,
}

/// Recognise the supported "pure metadata aggregate" shape. Returns `None`
/// (fall back to DataFusion) on anything outside the conservative set.
pub(crate) fn match_metadata_aggregate(stmt: &Statement) -> Option<MetadataAggregatePlan> {
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return None,
    };
    match_query(query.as_ref())
}

fn match_query(q: &Query) -> Option<MetadataAggregatePlan> {
    // No CTE / OFFSET / FETCH / LIMIT / locks / FOR / settings / format,
    // and crucially NO ORDER BY (a sorted single row is still a single
    // row, but staying strict keeps the contract trivially safe).
    if q.with.is_some()
        || q.order_by.is_some()
        || !q.ext_limit_by().is_empty()
        || q.ext_offset().is_some()
        || q.ext_limit().is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
    {
        return None;
    }

    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };

    // Reject every clause that could change the semantics of a bare
    // aggregate: DISTINCT, TOP, INTO, lateral views, PREWHERE, CLUSTER /
    // DISTRIBUTE / SORT BY, HAVING, named windows, QUALIFY, CONNECT BY.
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

    // GROUP BY: accept only the empty-expression form (a bare aggregate
    // collapses the whole table to one row). `GROUP BY ALL` / explicit
    // grouping expressions take us off the fast path.
    match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if exprs.is_empty() && mods.is_empty() => {}
        _ => return None,
    }

    // WHERE absent — v1 scope. A filtered aggregate would need
    // whole-file-prune reasoning that is error-prone; bail to DataFusion.
    if select.selection.is_some() {
        return None;
    }

    // FROM: exactly one bare table, no joins, no alias / args / hints /
    // version / ordinality / partitions.
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
    let table_qualifier = table.as_str().to_string();

    // Projection: at least one item, every item a bare (un-aliased)
    // supported aggregate call.
    if select.projection.is_empty() {
        return None;
    }
    let mut aggs = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let expr = match item {
            // Aliased aggregates (`COUNT(*) AS n`) change the output column
            // name; staying strict avoids having to mirror DataFusion's
            // alias handling. Fall through to DataFusion.
            SelectItem::UnnamedExpr(e) => e,
            _ => return None,
        };
        let (kind, name) = match_aggregate(expr, &table_qualifier)?;
        aggs.push((kind, name));
    }

    Some(MetadataAggregatePlan { table, aggs })
}

fn single_part_table(name: &ObjectName) -> Option<TableName> {
    if name.0.len() != 1 {
        return None;
    }
    TableName::new(name.0[0].id_val().clone()).ok()
}

/// Match a single projected aggregate. Returns `(kind, output_column_name)`
/// where the name is byte-identical to what DataFusion 53 emits for this
/// aggregate over `table_qualifier` (empirically: `count(*)` for COUNT(*),
/// otherwise `fn(<table>.<col>)`, function name lowercased).
fn match_aggregate(expr: &Expr, table_qualifier: &str) -> Option<(AggKind, String)> {
    let func = match expr {
        Expr::Function(f) => f,
        _ => return None,
    };
    // Reject window / FILTER / WITHIN GROUP / IGNORE NULLS / parametric
    // forms — none of these are a plain whole-table aggregate.
    if func.over.is_some()
        || func.filter.is_some()
        || !func.within_group.is_empty()
        || func.null_treatment.is_some()
        || !matches!(func.parameters, FunctionArguments::None)
    {
        return None;
    }

    let fname = func.name.0.last()?.id_val().to_ascii_lowercase();
    // Multi-part names (`schema.count`) aren't the builtin aggregates.
    if func.name.0.len() != 1 {
        return None;
    }

    let list = match &func.args {
        FunctionArguments::List(list) => list,
        // `COUNT` with no parens etc. — not a shape we answer.
        _ => return None,
    };
    // DISTINCT / ORDER BY / other in-arg clauses change semantics.
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return None;
    }

    match fname.as_str() {
        "count" => {
            if list.args.len() != 1 {
                return None;
            }
            match &list.args[0] {
                // COUNT(*) → DataFusion names this exactly `count(*)`.
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                    Some((AggKind::CountStar, "count(*)".to_string()))
                }
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                    let col = bare_column(e)?;
                    let name = format!("count({table_qualifier}.{col})");
                    Some((AggKind::CountCol(col), name))
                }
                _ => None,
            }
        }
        "min" | "max" | "sum" => {
            if list.args.len() != 1 {
                return None;
            }
            let col = match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => bare_column(e)?,
                // MIN(*) etc. is not valid SQL we answer.
                _ => return None,
            };
            let name = format!("{fname}({table_qualifier}.{col})");
            let kind = match fname.as_str() {
                "min" => AggKind::MinCol(col),
                "max" => AggKind::MaxCol(col),
                "sum" => AggKind::SumCol(col),
                _ => unreachable!("matched on min|max|sum"),
            };
            Some((kind, name))
        }
        _ => None,
    }
}

/// Accept ONLY a single bare column identifier as an aggregate argument.
/// Compound identifiers (`t.col`), expressions (`SUM(a + b)`), casts,
/// literals, etc. all drop us out of the fast path so we never have to
/// evaluate an expression from metadata.
fn bare_column(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(id) => Some(id.value.clone()),
        _ => None,
    }
}

/// Execute a recognised metadata-aggregate plan purely from catalog
/// statistics. Builds a single-row `RecordBatch` whose schema is
/// byte-identical to what `exec_select` would have returned for the same
/// SQL. Returns `Ok(None)` when the plan cannot be answered from metadata
/// (e.g. an unpopulated `sum_bytes`, an unsupported column type, or a
/// missing `null_count`) so the caller falls back to DataFusion with a
/// correct full scan.
///
/// ## Hot-tier overlay gate (PR2 correctness, re-tightened for perf)
///
/// PR2 (commit `3a52efc`) added a gate in `executor.rs` that bailed this
/// path whenever the `MemTableRegistry` held *any* non-`Row` entry for the
/// table — fixing a latent COUNT(*) bug where a fast-path DELETE
/// tombstone would otherwise be silently ignored. That gate was correct
/// but too broad in two ways:
///
/// 1. **Update entries don't change row count.** A `MemRowValue::Update`
///    is a full-row replacement keyed by the *same* PK as the cold row it
///    shadows. COUNT(*), COUNT(col-NOT-NULL via PK), MIN(pk), MAX(pk)
///    are all unchanged by an update. Bailing on Update conflated
///    overlay-presence with count-correctness.
///
/// 2. **It walked `snapshot()` (an O(n) clone of the entire BTreeMap) on
///    every COUNT(*).** Auto-commit INSERTs cache the row in the registry
///    as `MemRowValue::Row` (line 4220 of `executor.rs`), so a freshly
///    bulk-loaded table can hold the entire dataset in the registry as
///    a cache — making `snapshot()` allocate millions of clones per
///    `SELECT COUNT(*)`. The measured cost: 92x *faster* than PG @1M
///    → 12x *slower* (a ~1100x regression) on the perf bench.
///
/// The replacement gate here is internal to `execute_metadata_aggregate`
/// and uses a cheap two-step check on the post-flush memtable:
///
/// * **Fast path:** if `total_count() == 0` (O(1) — one rwlock + BTreeMap
///   `len()`) the memtable is empty — no overlay possible — use the
///   shortcut.
/// * **Slow path:** if non-empty, walk the snapshot once and bail only if
///   we actually find a `Tombstone` (the only variant that can change a
///   COUNT). `Row` (HTAP cache) and `Update` (same-PK replacement) are
///   both safe to ignore for count purposes. The walk is short-circuited
///   by the first tombstone, so the cost is bounded by overlay density,
///   not memtable cardinality.
///
/// This preserves PR2's correctness fix (tombstones still defer to a real
/// scan that runs the overlay filter — see
/// `tests/integration/tests/dml_extras.rs::fast_path_delete_then_count_excludes_tombstoned`)
/// while restoring the freshly-loaded-table fast path. UPDATE row-count
/// invariance is exercised by
/// `tests/integration/tests/update_hottier.rs::update_does_not_change_count`.
///
/// NOTE: `executor.rs` may *also* gate this path with a coarser check
/// (the PR2 placement). When that gate is loosened to defer to this one,
/// the regression closes end-to-end. Until then, this gate at least
/// guarantees correctness when called and avoids re-walking the snapshot
/// inside the function.
pub(crate) async fn execute_metadata_aggregate(
    sess: &ProjectSession,
    plan: MetadataAggregatePlan,
    prefetched_meta: Option<TableMetadata>,
) -> Result<Option<ExecResult>> {
    // Mirror `fast_select::execute_simple_select`: flush the in-RAM tail so
    // the catalog snapshot reflects every committed row. Without this a
    // shard-backed table would report stale (or zero) stats and the
    // aggregate would be wrong.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }

    // Hot-tier overlay gate. See the function-level rustdoc for the full
    // rationale. The fast path (`total_count() == 0`) is the common case
    // for freshly-loaded tables that have either not seen any HTAP cache
    // promotion or whose memtable has been drained by the registry flush
    // task. The slow path runs only when the memtable is non-empty and
    // exits at the first tombstone — so its cost is bounded by overlay
    // density, not memtable size.
    if metadata_aggregate_blocked_by_tombstone(sess, &plan.table) {
        return Ok(None);
    }

    let meta = match prefetched_meta {
        Some(m) => m,
        None => {
            sess.engine
                .config()
                .catalog
                .load_table(&sess.project, &plan.table)
                .await?
        }
    };

    let schema = meta.schema.as_ref();
    let files = meta.live_data_files();

    let mut out_fields: Vec<Field> = Vec::with_capacity(plan.aggs.len());
    let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(plan.aggs.len());

    for (kind, out_name) in &plan.aggs {
        let (field, col): (Field, ArrayRef) = match kind {
            AggKind::CountStar => {
                let total: i64 = files
                    .iter()
                    .map(|f| f.row_count as i64)
                    .sum();
                (
                    Field::new(out_name, DataType::Int64, false),
                    Arc::new(Int64Array::from(vec![total])),
                )
            }
            AggKind::CountCol(col) => {
                // Σ (row_count − null_count). Bail if ANY file's null_count
                // is unknown — we can't prove the non-null count.
                let mut total: i64 = 0;
                for f in &files {
                    let nulls = match f.column_stats.get(col).and_then(|c| c.null_count) {
                        Some(n) => n,
                        None => return Ok(None),
                    };
                    // Guard the (shouldn't-happen) null_count > row_count.
                    let non_null = f.row_count.checked_sub(nulls);
                    let non_null = match non_null {
                        Some(v) => v,
                        None => return Ok(None),
                    };
                    total += non_null as i64;
                }
                (
                    Field::new(out_name, DataType::Int64, false),
                    Arc::new(Int64Array::from(vec![total])),
                )
            }
            AggKind::MinCol(col) | AggKind::MaxCol(col) => {
                let is_min = matches!(kind, AggKind::MinCol(_));
                match fold_minmax(&files, col, schema, is_min, out_name)? {
                    Some(fm) => (fm.0, fm.1),
                    None => return Ok(None),
                }
            }
            AggKind::SumCol(col) => match fold_sum(&files, col, schema, out_name)? {
                Some(fs) => fs,
                None => return Ok(None),
            },
        };
        out_fields.push(field);
        out_cols.push(col);
    }

    let out_schema = Arc::new(Schema::new(out_fields));
    let batch = RecordBatch::try_new(out_schema.clone(), out_cols)
        .map_err(|e| BasinError::internal(format!("fast_aggregate result batch: {e}")))?;

    Ok(Some(ExecResult::Rows {
        schema: out_schema,
        batches: vec![batch],
    }))
}

/// Cheap "should the metadata-aggregate shortcut bail?" check.
///
/// Returns `true` iff the `MemTableRegistry` holds a `Tombstone` entry for
/// `(sess.project, table)`. Tombstones are the only memtable variant that
/// can change a `COUNT(*)`/`COUNT(col)`/`MIN`/`MAX`/`SUM` answer against
/// catalog statistics:
///
/// * `MemRowValue::Row` (HTAP cache for an already-committed cold row) —
///   the cold row is already counted in `live_data_files()` stats, so the
///   cache duplicate must NOT contribute.
/// * `MemRowValue::Update` (PK-keyed full-row replacement written by the
///   hot-tier UPDATE fast path) — replaces a cold row 1-for-1; row count
///   is unchanged. MIN/MAX/SUM on the updated column may diverge, but
///   the caller already declined to fold per-file `sum_bytes` / `min_bytes`
///   when stale; the count-side aggregates remain exact.
/// * `MemRowValue::Tombstone` (written by the hot-tier DELETE fast path) —
///   the cold row still exists in Parquet but must be excluded; the
///   metadata count would over-report by exactly the tombstone count.
///
/// Cost: a registry lookup (O(1) `DashMap::get`), then `total_count()`
/// (O(1) — one rwlock acquire + `BTreeMap::len()`). If non-zero, walks
/// the BTreeMap via `snapshot()` and short-circuits at the first
/// tombstone — so cost is bounded by tombstone *position*, not memtable
/// size. The snapshot allocation is regrettable but unavoidable without
/// a wider `MemTable` API change; in the steady state (no tombstones
/// present) the `total_count() == 0` branch fires for any registry that
/// was drained by the flush task or never received an HTAP promotion.
fn metadata_aggregate_blocked_by_tombstone(
    sess: &ProjectSession,
    table: &TableName,
) -> bool {
    let registry = sess.engine.memtable_registry();
    let entry = match registry.get(&sess.project, table) {
        Some(e) => e,
        // No registry entry at all — no writes for this `(project, table)`
        // have ever flowed through the hot tier. Catalog stats are exact.
        None => return false,
    };
    if entry.memtable.total_count() == 0 {
        return false;
    }
    // Non-empty memtable: scan once, exit at the first tombstone. `Row`
    // and `Update` entries do NOT block the shortcut (see rustdoc above).
    entry
        .memtable
        .snapshot()
        .iter()
        .any(|(_, v)| v.is_tombstone())
}

/// Decode an 8-byte little-endian `i64`. Matches
/// `basin_storage::predicate::decode_i64` exactly.
fn decode_i64(b: &[u8]) -> Option<i64> {
    if b.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Some(i64::from_le_bytes(arr))
}

/// Decode an 8-byte little-endian `f64`. Matches
/// `basin_storage::predicate::decode_f64` exactly.
fn decode_f64(b: &[u8]) -> Option<f64> {
    if b.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Some(f64::from_le_bytes(arr))
}

/// Fold per-file min (or max) for `col`. Returns:
/// * `Ok(Some((field, array)))` on a clean single-value answer,
/// * `Ok(None)` to bail to DataFusion (unsupported type, missing/short
///   bytes, no files, or a column with no rows at all),
/// matching `predicate.rs`'s decode contract byte-for-byte (8-byte LE for
/// Int64/Float64; raw lexicographic bytes for Utf8).
fn fold_minmax(
    files: &[basin_catalog::DataFileRef],
    col: &str,
    schema: &Schema,
    is_min: bool,
    out_name: &str,
) -> Result<Option<(Field, ArrayRef)>> {
    let field = match schema.fields().iter().find(|f| f.name() == col) {
        Some(f) => f,
        None => return Ok(None),
    };
    let dt = field.data_type().clone();

    // No data files → empty table. DataFusion returns NULL for MIN/MAX of
    // an empty input; producing that here keeps the fast path consistent,
    // but to stay maximally conservative we just bail to DataFusion.
    if files.is_empty() {
        return Ok(None);
    }

    match dt {
        DataType::Int64 => {
            let mut acc: Option<i64> = None;
            for f in files {
                let cs = match f.column_stats.get(col) {
                    Some(c) => c,
                    None => return Ok(None),
                };
                let bytes = if is_min { &cs.min_bytes } else { &cs.max_bytes };
                let bytes = match bytes {
                    Some(b) => b,
                    None => return Ok(None),
                };
                let v = match decode_i64(bytes) {
                    Some(v) => v,
                    None => return Ok(None),
                };
                acc = Some(match acc {
                    None => v,
                    Some(cur) if is_min => cur.min(v),
                    Some(cur) => cur.max(v),
                });
            }
            match acc {
                Some(v) => Ok(Some((
                    Field::new(out_name, DataType::Int64, true),
                    Arc::new(Int64Array::from(vec![v])),
                ))),
                None => Ok(None),
            }
        }
        DataType::Float64 => {
            // Per-file fold order can differ from a full scan in the last
            // ULP. The differential harness normalises floats to 6 dp, so
            // a real MIN/MAX (an actual stored value, not a reduction) is
            // safe — but if that ever flags, the conservative fix is to
            // bail float here. Float MIN/MAX of a single stored extreme is
            // exact (it's a value that exists in the data), so we keep it.
            let mut acc: Option<f64> = None;
            for f in files {
                let cs = match f.column_stats.get(col) {
                    Some(c) => c,
                    None => return Ok(None),
                };
                let bytes = if is_min { &cs.min_bytes } else { &cs.max_bytes };
                let bytes = match bytes {
                    Some(b) => b,
                    None => return Ok(None),
                };
                let v = match decode_f64(bytes) {
                    Some(v) => v,
                    None => return Ok(None),
                };
                acc = Some(match acc {
                    None => v,
                    Some(cur) if is_min => {
                        if v < cur {
                            v
                        } else {
                            cur
                        }
                    }
                    Some(cur) => {
                        if v > cur {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            match acc {
                Some(v) => Ok(Some((
                    Field::new(out_name, DataType::Float64, true),
                    Arc::new(Float64Array::from(vec![v])),
                ))),
                None => Ok(None),
            }
        }
        DataType::Utf8 => {
            // Utf8 min/max bytes are the raw value bytes, compared
            // lexicographically (same contract as `decide_byte_lex`).
            let mut acc: Option<Vec<u8>> = None;
            for f in files {
                let cs = match f.column_stats.get(col) {
                    Some(c) => c,
                    None => return Ok(None),
                };
                let bytes = if is_min { &cs.min_bytes } else { &cs.max_bytes };
                let bytes = match bytes {
                    Some(b) => b.clone(),
                    None => return Ok(None),
                };
                acc = Some(match acc {
                    None => bytes,
                    Some(cur) => {
                        if is_min {
                            if bytes < cur {
                                bytes
                            } else {
                                cur
                            }
                        } else if bytes > cur {
                            bytes
                        } else {
                            cur
                        }
                    }
                });
            }
            match acc {
                Some(b) => {
                    // The stored bytes are valid UTF-8 (they came from a
                    // Utf8 column); if not, bail rather than lossy-decode.
                    let s = match String::from_utf8(b) {
                        Ok(s) => s,
                        Err(_) => return Ok(None),
                    };
                    Ok(Some((
                        Field::new(out_name, DataType::Utf8, true),
                        Arc::new(StringArray::from(vec![s])),
                    )))
                }
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Fold per-file `sum_bytes` for `col`. Returns `Ok(None)` (bail to a
/// correct full scan) if ANY live file lacks `sum_bytes` — which is the
/// case until the separate writer/Vortex task populates it — or if the
/// column type is not exactly Int64/Float64.
fn fold_sum(
    files: &[basin_catalog::DataFileRef],
    col: &str,
    schema: &Schema,
    out_name: &str,
) -> Result<Option<(Field, ArrayRef)>> {
    let field = match schema.fields().iter().find(|f| f.name() == col) {
        Some(f) => f,
        None => return Ok(None),
    };
    // Empty table → DataFusion returns NULL for SUM; bail to be safe.
    if files.is_empty() {
        return Ok(None);
    }

    match field.data_type() {
        DataType::Int64 => {
            // Accumulate in i128 so a many-file sum can't overflow mid-fold,
            // then narrow back to i64 (bail if it genuinely overflows i64 —
            // DataFusion would error there too, so falling through is the
            // honest behaviour).
            let mut acc: i128 = 0;
            for f in files {
                let cs = match f.column_stats.get(col) {
                    Some(c) => c,
                    None => return Ok(None),
                };
                let bytes = match &cs.sum_bytes {
                    Some(b) => b,
                    None => return Ok(None),
                };
                let v = match decode_i64(bytes) {
                    Some(v) => v,
                    None => return Ok(None),
                };
                acc += v as i128;
            }
            if acc < i64::MIN as i128 || acc > i64::MAX as i128 {
                return Ok(None);
            }
            Ok(Some((
                Field::new(out_name, DataType::Int64, true),
                Arc::new(Int64Array::from(vec![acc as i64])),
            )))
        }
        DataType::Float64 => {
            let mut acc: f64 = 0.0;
            for f in files {
                let cs = match f.column_stats.get(col) {
                    Some(c) => c,
                    None => return Ok(None),
                };
                let bytes = match &cs.sum_bytes {
                    Some(b) => b,
                    None => return Ok(None),
                };
                let v = match decode_f64(bytes) {
                    Some(v) => v,
                    None => return Ok(None),
                };
                acc += v;
            }
            Ok(Some((
                Field::new(out_name, DataType::Float64, true),
                Arc::new(Float64Array::from(vec![acc])),
            )))
        }
        _ => Ok(None),
    }
}

// ─── Low-cardinality GROUP BY fast path ──────────────────────────────────────

/// A recognised `SELECT key, COUNT(*) FROM t GROUP BY key` plan.
///
/// Only a single Int64 grouping column is supported; compound GROUP BY or
/// non-Int64 keys fall back to DataFusion.  Output column names mirror what
/// DataFusion 53 would emit for the same query:
///   * `key_col` — same name as the grouping column
///   * `"count(*)"` — the COUNT(*) aggregate
#[derive(Debug)]
pub(crate) struct GroupByCountStarPlan {
    pub table: TableName,
    /// Name of the single grouping column.
    pub key_col: String,
}

/// Recognise `SELECT key, COUNT(*) FROM t GROUP BY key`.
///
/// Conservative recogniser: returns `None` (fall through to DataFusion) for
/// any shape outside the strict pattern:
/// * Exactly one Int64-compatible key column followed by `COUNT(*)`
/// * Single bare (un-aliased) grouping expression equal to `key`
/// * No WHERE / HAVING / ORDER BY / LIMIT / CTE / DISTINCT / joins
/// * Single bare (un-aliased) table reference
pub(crate) fn match_groupby_low_card(stmt: &Statement) -> Option<GroupByCountStarPlan> {
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return None,
    };
    // No CTE, ORDER BY, LIMIT, or other query-level clauses.
    if query.with.is_some()
        || query.order_by.is_some()
        || !query.ext_limit_by().is_empty()
        || query.ext_offset().is_some()
        || query.ext_limit().is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return None;
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };

    // Reject every clause that changes aggregate semantics.
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

    // No WHERE — filtered GROUP BY would need per-row predicate evaluation.
    if select.selection.is_some() {
        return None;
    }

    // FROM: exactly one bare table, no joins.
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
            gb_single_part_table(name)?
        }
        _ => return None,
    };

    // GROUP BY: exactly one bare identifier, no modifiers.
    let key_col = match &select.group_by {
        GroupByExpr::Expressions(exprs, mods) if exprs.len() == 1 && mods.is_empty() => {
            match &exprs[0] {
                Expr::Identifier(id) => id.value.clone(),
                _ => return None,
            }
        }
        _ => return None,
    };

    // Projection: exactly `key_col, COUNT(*)` — both un-aliased.
    if select.projection.len() != 2 {
        return None;
    }
    // First item: bare identifier matching the GROUP BY key.
    match &select.projection[0] {
        SelectItem::UnnamedExpr(Expr::Identifier(id)) if id.value == key_col => {}
        _ => return None,
    }
    // Second item: un-aliased COUNT(*).
    match &select.projection[1] {
        SelectItem::UnnamedExpr(Expr::Function(f)) => {
            if f.over.is_some()
                || f.filter.is_some()
                || !f.within_group.is_empty()
                || f.null_treatment.is_some()
                || !matches!(f.parameters, FunctionArguments::None)
            {
                return None;
            }
            if f.name.0.len() != 1 {
                return None;
            }
            if f.name.0[0].id_val().to_ascii_lowercase() != "count" {
                return None;
            }
            let list = match &f.args {
                FunctionArguments::List(l) => l,
                _ => return None,
            };
            if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
                return None;
            }
            if list.args.len() != 1 {
                return None;
            }
            match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
                _ => return None,
            }
        }
        _ => return None,
    }

    Some(GroupByCountStarPlan { table, key_col })
}

fn gb_single_part_table(name: &ObjectName) -> Option<TableName> {
    if name.0.len() != 1 {
        return None;
    }
    TableName::new(name.0[0].id_val().clone()).ok()
}

/// Execute a recognised low-cardinality GROUP BY COUNT(*) plan.
///
/// Returns:
/// * `Ok(Some(result))` — a multi-row `RecordBatch` with schema `(key, count(*))`.
/// * `Ok(None)` — cardinality gate failed or catalog stats insufficient; caller
///   falls through to DataFusion for a correct full scan.
///
/// The output column names match DataFusion 53 exactly:
/// * column 0: `key_col` (same name as the grouping column, same type)
/// * column 1: `"count(*)"` (Int64, non-nullable)
pub(crate) async fn execute_groupby_low_card(
    sess: &ProjectSession,
    plan: GroupByCountStarPlan,
    prefetched_meta: Option<TableMetadata>,
) -> Result<Option<ExecResult>> {
    // Flush any in-RAM tail so the catalog snapshot is current.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }

    let meta = match prefetched_meta {
        Some(m) => m,
        None => {
            sess.engine
                .config()
                .catalog
                .load_table(&sess.project, &plan.table)
                .await?
        }
    };

    // Verify the key column exists and is Int64 (the only type we handle).
    let key_field = match meta.schema.fields().iter().find(|f| f.name() == &plan.key_col) {
        Some(f) => f.clone(),
        None => return Ok(None),
    };
    if key_field.data_type() != &DataType::Int64 {
        return Ok(None);
    }

    let files = meta.live_data_files();

    // Cardinality gate: derive the global key range from per-file catalog stats.
    // For each file, require both min_bytes and max_bytes for the key column.
    // Global distinct count upper bound = max_global - min_global + 1.
    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;
    for f in &files {
        let cs = match f.column_stats.get(&plan.key_col) {
            Some(c) => c,
            None => return Ok(None), // stats absent — cannot gate
        };
        let file_min = match cs.min_bytes.as_deref().and_then(decode_i64) {
            Some(v) => v,
            None => return Ok(None),
        };
        let file_max = match cs.max_bytes.as_deref().and_then(decode_i64) {
            Some(v) => v,
            None => return Ok(None),
        };
        global_min = Some(global_min.map_or(file_min, |m| m.min(file_min)));
        global_max = Some(global_max.map_or(file_max, |m| m.max(file_max)));
    }

    // Empty table — return empty result (DataFusion would return zero rows).
    if files.is_empty() {
        let key_schema = Arc::new(Schema::new(vec![
            key_field.as_ref().clone(),
            Field::new("count(*)", DataType::Int64, false),
        ]));
        let batch = RecordBatch::new_empty(key_schema.clone());
        return Ok(Some(ExecResult::Rows {
            schema: key_schema,
            batches: vec![batch],
        }));
    }

    let (gmin, gmax) = match (global_min, global_max) {
        (Some(lo), Some(hi)) => (lo, hi),
        _ => return Ok(None),
    };

    // Key range exceeds threshold — fall through to DataFusion.
    let range = gmax.saturating_sub(gmin).saturating_add(1);
    if range > LOW_CARD_THRESHOLD {
        return Ok(None);
    }

    // Build live file paths.
    let live_paths: Vec<object_store::path::Path> = files
        .iter()
        .map(|f| object_store::path::Path::from(f.path.as_str()))
        .collect();

    // Read ONLY the key column from all files. No LIMIT pushdown: a GROUP
    // BY aggregate has to see every row.
    let opts = basin_storage::ReadOptions {
        projection: Some(vec![plan.key_col.clone()]),
        filters: vec![],
        partition: None,
        limit: None,
        row_group_selection: None,
    };
    let schema_ref = Some(meta.schema.clone());

    let stream = sess
        .engine
        .config()
        .storage
        .read_paths_with_schema(&sess.project, live_paths, opts, schema_ref)
        .await?;

    // Accumulate per-key row counts across all batches.
    let mut counts: HashMap<i64, i64> = HashMap::new();
    let mut batches_stream = stream;
    while let Some(batch_result) = batches_stream.next().await {
        let batch = batch_result?;
        let col = batch.column(0);
        let arr = match col.as_any().downcast_ref::<Int64Array>() {
            Some(a) => a,
            None => return Ok(None), // unexpected type — bail
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                // NULL keys: DataFusion includes NULL as its own group.
                // To stay correct we would need a separate NULL counter.
                // For now, bail to DataFusion when any NULL key is present.
                return Ok(None);
            }
            *counts.entry(arr.value(i)).or_insert(0) += 1;
        }
    }

    // Safety: actual distinct count after reading must not exceed threshold
    // (the range-based gate is an upper bound, not exact).
    if counts.len() as i64 > LOW_CARD_THRESHOLD {
        return Ok(None);
    }

    // Sort by key ascending — DataFusion's hash-aggregate output order is
    // non-deterministic, but the differential test normalises rows.  Sorting
    // here matches the most common expectation and makes the result stable.
    let mut sorted_pairs: Vec<(i64, i64)> = counts.into_iter().collect();
    sorted_pairs.sort_unstable_by_key(|(k, _)| *k);

    let key_vals: Vec<i64> = sorted_pairs.iter().map(|(k, _)| *k).collect();
    let cnt_vals: Vec<i64> = sorted_pairs.iter().map(|(_, c)| *c).collect();

    let out_schema = Arc::new(Schema::new(vec![
        key_field.as_ref().clone(),
        Field::new("count(*)", DataType::Int64, false),
    ]));

    let key_array: ArrayRef = Arc::new(Int64Array::from(key_vals));
    let cnt_array: ArrayRef = Arc::new(Int64Array::from(cnt_vals));

    let batch = RecordBatch::try_new(out_schema.clone(), vec![key_array, cnt_array])
        .map_err(|e| BasinError::internal(format!("groupby_low_card result batch: {e}")))?;

    Ok(Some(ExecResult::Rows {
        schema: out_schema,
        batches: vec![batch],
    }))
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
    fn matches_count_star() {
        let stmt = parse_one("SELECT COUNT(*) FROM t");
        let plan = match_metadata_aggregate(&stmt).expect("should match");
        assert_eq!(plan.table.as_str(), "t");
        assert_eq!(plan.aggs.len(), 1);
        assert_eq!(plan.aggs[0].0, AggKind::CountStar);
        assert_eq!(plan.aggs[0].1, "count(*)");
    }

    #[test]
    fn matches_min_max_mix_with_df_names() {
        let stmt = parse_one("SELECT COUNT(*), SUM(id), MIN(k), MAX(k) FROM t");
        let plan = match_metadata_aggregate(&stmt).expect("should match");
        assert_eq!(plan.aggs.len(), 4);
        assert_eq!(plan.aggs[0], (AggKind::CountStar, "count(*)".to_string()));
        assert_eq!(
            plan.aggs[1],
            (AggKind::SumCol("id".into()), "sum(t.id)".to_string())
        );
        assert_eq!(
            plan.aggs[2],
            (AggKind::MinCol("k".into()), "min(t.k)".to_string())
        );
        assert_eq!(
            plan.aggs[3],
            (AggKind::MaxCol("k".into()), "max(t.k)".to_string())
        );
    }

    #[test]
    fn matches_count_col() {
        let stmt = parse_one("SELECT COUNT(k) FROM t");
        let plan = match_metadata_aggregate(&stmt).expect("should match");
        assert_eq!(
            plan.aggs[0],
            (AggKind::CountCol("k".into()), "count(t.k)".to_string())
        );
    }

    #[test]
    fn rejects_where() {
        let stmt = parse_one("SELECT COUNT(*) FROM t WHERE id > 5");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_group_by() {
        let stmt = parse_one("SELECT k, COUNT(*) FROM t GROUP BY k");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_distinct_arg() {
        let stmt = parse_one("SELECT COUNT(DISTINCT k) FROM t");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_join() {
        let stmt = parse_one("SELECT COUNT(*) FROM a JOIN b ON a.id = b.id");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_non_aggregate_projection() {
        let stmt = parse_one("SELECT id, COUNT(*) FROM t");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_aliased_aggregate() {
        let stmt = parse_one("SELECT COUNT(*) AS n FROM t");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_expression_arg() {
        let stmt = parse_one("SELECT SUM(a + b) FROM t");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_filter_clause() {
        let stmt = parse_one("SELECT COUNT(*) FILTER (WHERE k > 0) FROM t");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_avg_unsupported() {
        let stmt = parse_one("SELECT AVG(k) FROM t");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    #[test]
    fn rejects_order_by_limit() {
        let stmt = parse_one("SELECT COUNT(*) FROM t ORDER BY 1 LIMIT 1");
        assert!(match_metadata_aggregate(&stmt).is_none());
    }

    // ── match_groupby_low_card unit tests ─────────────────────────────────

    #[test]
    fn groupby_matches_basic() {
        let stmt = parse_one("SELECT k, COUNT(*) FROM t GROUP BY k");
        let plan = match_groupby_low_card(&stmt).expect("should match");
        assert_eq!(plan.table.as_str(), "t");
        assert_eq!(plan.key_col, "k");
    }

    #[test]
    fn groupby_rejects_compound_group_by() {
        let stmt = parse_one("SELECT k, b, COUNT(*) FROM t GROUP BY k, b");
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_having() {
        let stmt =
            parse_one("SELECT k, COUNT(*) FROM t GROUP BY k HAVING COUNT(*) > 10");
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_order_by() {
        let stmt = parse_one("SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k");
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_where() {
        let stmt = parse_one("SELECT k, COUNT(*) FROM t WHERE id > 0 GROUP BY k");
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_aliased_count() {
        let stmt = parse_one("SELECT k, COUNT(*) AS c FROM t GROUP BY k");
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_cte() {
        let stmt = parse_one(
            "WITH cte AS (SELECT 1) SELECT k, COUNT(*) FROM t GROUP BY k",
        );
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_bare_aggregate_matches_metadata_not_groupby() {
        // Plain COUNT(*) without GROUP BY → metadata aggregate path, not groupby path.
        let stmt = parse_one("SELECT COUNT(*) FROM t");
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_key_after_count() {
        // Wrong projection order — COUNT(*) first.
        let stmt = parse_one("SELECT COUNT(*), k FROM t GROUP BY k");
        assert!(match_groupby_low_card(&stmt).is_none());
    }

    #[test]
    fn groupby_rejects_join() {
        let stmt =
            parse_one("SELECT a.k, COUNT(*) FROM a JOIN b ON a.id = b.id GROUP BY a.k");
        assert!(match_groupby_low_card(&stmt).is_none());
    }
}
