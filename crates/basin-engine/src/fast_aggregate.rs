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

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::TableMetadata;
use basin_common::{BasinError, Result, TableName};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, Query,
    SelectItem, SetExpr, Statement, TableFactor,
};

use crate::pg_ast::{ObjectNamePartExt, QueryClauseExt};
use crate::{ExecResult, ProjectSession};

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
}
