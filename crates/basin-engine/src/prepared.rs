//! Prepared-statement support for the Postgres extended-query protocol.
//!
//! v1 strategy is "rebind into a fresh SQL string": we scan the SQL once at
//! prepare time to count `$N` placeholders and try to plan it for column
//! schema discovery, then at bind time we substitute the parameter values
//! into the SQL as literals and re-run the simple-query path. No plan caching
//! and no DataFusion-level parameter binding — the goal here is correctness
//! and unblocking every Postgres driver, not throughput.
//!
//! ## Why a custom scanner instead of `sqlparser`
//!
//! sqlparser doesn't model PostgreSQL `$N` placeholders as a first-class
//! token in every position we'd want them (especially within INSERT VALUES),
//! and we don't actually need a parse tree — we only need to:
//!
//! 1. Find `$N` outside string literals and quoted identifiers.
//! 2. Substitute literal text in those exact byte ranges.
//!
//! A 50-line forward scanner does that with no grammar surprises.
//!
//! ## Quoting rules used at substitution time
//!
//! - integers / floats / bool: rendered as their Rust `Display` form.
//! - text: wrapped in `'...'` with single quotes inside the body doubled per
//!   the SQL standard. We do **not** emit a trailing `\` or do any other
//!   escaping; backslashes pass through as data, which matches the Postgres
//!   `standard_conforming_strings = on` default.
//! - bytea: hex form prefixed with `\x`, wrapped in single quotes and cast
//!   via `::bytea`. Drivers rarely send bytea through the cleartext-text
//!   substitution path, but we handle it for completeness.
//! - NULL: literal `NULL`.

use crate::pg_ast::{ObjectNamePartExt, OrderByExt, QueryClauseExt};
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;
use arrow_schema::TimeUnit;
use arrow_schema::{DataType, Field};
use basin_common::{BasinError, Result, TableName};
use sqlparser::ast::DataType as SqlDataType;
use sqlparser::ast::ValueWithSpan;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, CastKind, Expr, FromTable, ObjectName, Query,
    SelectItem, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{ExecResult, ProjectSession};

/// Opaque identifier for one prepared statement, scoped to one
/// [`ProjectSession`]. Closing the session implicitly closes the statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StatementHandle(pub Uuid);

impl StatementHandle {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for StatementHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Schema of a prepared statement: the parameters the client must supply at
/// `Bind` time, plus the columns the result set will return at `Execute`
/// time. `columns` is empty for statements that return no rows
/// (INSERT / CREATE / etc.).
#[derive(Clone, Debug)]
pub struct StatementSchema {
    pub param_types: Vec<DataType>,
    /// Phase 5.10 wire-protocol fix: parallel to `param_types`, marks slots
    /// whose source column carries `BASIN_TYPE=JSONB` metadata. The pgwire
    /// layer surfaces OID 3802 (jsonb) for these slots in ParameterDescription
    /// and decodes the binary wire format (strip the 0x01 version byte, the
    /// rest is canonical-form JSON bytes) at Bind time.
    pub param_is_jsonb: Vec<bool>,
    /// Same shape, for `BASIN_TYPE=UUID` columns. OID 2950 (uuid). Binary
    /// wire format is 16 raw RFC 4122 bytes; we render those as a canonical
    /// hyphenated string for the engine's text-substitution path, which
    /// already accepts UUID string literals.
    pub param_is_uuid: Vec<bool>,
    pub columns: Vec<Field>,
}

/// Mirrors `basin_engine::types::BASIN_TYPE_*`. Duplicated as a `&str` here
/// so `prepared` doesn't need a back-edge import (these constants live in a
/// crate-private module, and this file is the only consumer outside it that
/// reads field metadata directly).
const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
const BASIN_TYPE_JSONB: &str = "JSONB";
const BASIN_TYPE_UUID: &str = "UUID";

fn field_is_jsonb(f: &Field) -> bool {
    f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_JSONB)
}

fn field_is_uuid(f: &Field) -> bool {
    f.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID)
}

/// One scalar parameter value supplied at bind time. The variants here mirror
/// the Postgres types we already wire OIDs for in `basin-router::types`. Any
/// type the driver sends that doesn't fit one of these falls back to `Text`
/// (the receiving driver does the cast on its end).
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarParam {
    Null,
    Int8(i64),
    Int4(i32),
    Bool(bool),
    Float8(f64),
    Text(String),
    Bytea(Vec<u8>),
}

/// Output of [`ProjectSession::bind`]. Holds the substituted SQL and the
/// originating handle (for traceability). Opaque on purpose — the router
/// should never read its fields directly. `Clone` so the router can hold a
/// portal across separate `Execute` rounds without re-binding.
#[derive(Clone)]
pub struct BoundStatement {
    pub(crate) handle: StatementHandle,
    pub(crate) sql: String,
}

impl BoundStatement {
    pub fn handle(&self) -> StatementHandle {
        self.handle
    }

    /// Test-only accessor for the substituted SQL. Production code reaches
    /// the SQL through `execute_bound`.
    #[doc(hidden)]
    pub fn debug_sql(&self) -> &str {
        &self.sql
    }

    /// Test-only constructor. Lets `basin-router` tests build a marker
    /// `BoundStatement` whose SQL they can inspect, without standing up a
    /// real engine. Not meant for production use.
    #[doc(hidden)]
    pub fn for_tests(sql: String) -> Self {
        Self {
            handle: StatementHandle::new(),
            sql,
        }
    }
}

/// Per-session prepared-statement registry. Held inside `ProjectSession::state`
/// so each session gets its own map; closing the session drops everything.
pub(crate) struct PreparedRegistry {
    inner: RwLock<HashMap<StatementHandle, PreparedEntry>>,
}

struct PreparedEntry {
    sql: String,
    placeholder_count: usize,
    schema: StatementSchema,
}

impl PreparedRegistry {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Drop every prepared statement. Used by the connection-pool scrub on
    /// Session-mode reuse (DISCARD ALL / `DEALLOCATE ALL` semantics).
    pub(crate) async fn clear_all(&self) {
        self.inner.write().await.clear();
    }
}

/// Implementation of [`ProjectSession::prepare`]. See module docs for the
/// substitution strategy.
pub(crate) async fn prepare(
    sess: &ProjectSession,
    sql: &str,
) -> Result<(StatementHandle, StatementSchema)> {
    let placeholder_count = scan_placeholders(sql)?;

    // Best-effort parameter-type inference from the SQL's structure plus the
    // catalog. Anything we can't classify falls back to TEXT — drivers cope
    // either by sending strings or by coercing.
    let mut param_types = vec![DataType::Utf8; placeholder_count];
    let mut param_is_jsonb = vec![false; placeholder_count];
    let mut param_is_uuid = vec![false; placeholder_count];
    if placeholder_count > 0 {
        if let Err(e) = infer_param_types(
            sess,
            sql,
            &mut param_types,
            &mut param_is_jsonb,
            &mut param_is_uuid,
        )
        .await
        {
            tracing::debug!(error = %e, "param type inference fell back to TEXT");
        }
    }

    // Try to discover the result schema by planning the SQL with typed
    // placeholders. DataFusion 44 doesn't accept `$N`, so we substitute
    // representative literal values matching the inferred types. If the plan
    // fails we fall back to an empty column list — drivers will discover the
    // schema at execute time.
    let probe_sql = substitute_for_probe(sql, &param_types);
    let columns = probe_schema(sess, &probe_sql).await.unwrap_or_default();

    let schema = StatementSchema {
        param_types,
        param_is_jsonb,
        param_is_uuid,
        columns,
    };

    let handle = StatementHandle::new();
    let entry = PreparedEntry {
        sql: sql.to_owned(),
        placeholder_count,
        schema: schema.clone(),
    };
    sess.state
        .prepared
        .inner
        .write()
        .await
        .insert(handle, entry);
    Ok((handle, schema))
}

/// Best-effort placeholder type inference. Walks the parsed SQL looking for
/// patterns we can resolve against the catalog:
///
/// - `INSERT INTO t VALUES ($1, $2, ...)` — types come from `t`'s schema in
///   column-declaration order.
/// - `INSERT INTO t (c1, c2) VALUES ($1, $2)` — types come from the named
///   columns.
/// - `WHERE col OP $N` (and `$N OP col`) — type comes from `col` on the
///   referenced table.
///
/// Any pattern we don't recognise leaves the slot at TEXT.
async fn infer_param_types(
    sess: &ProjectSession,
    sql: &str,
    out: &mut [DataType],
    is_jsonb_out: &mut [bool],
    is_uuid_out: &mut [bool],
) -> Result<()> {
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| BasinError::internal(format!("parse for inference: {e}")))?;
    let stmt = match stmts.into_iter().next() {
        Some(s) => s,
        None => return Ok(()),
    };
    match stmt {
        Statement::Insert(ins) => {
            let table_name = name_to_table(crate::pg_ast::insert_object_name(&ins)?)?;
            let meta = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, &table_name)
                .await?;
            // Column ordering for the insert: the user's listed columns if
            // present, otherwise table-declaration order.
            let col_order: Vec<String> = if !ins.columns.is_empty() {
                ins.columns.iter().map(|c| c.value.clone()).collect()
            } else {
                meta.schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect()
            };
            // Walk the VALUES rows; for each placeholder cell, pick up the
            // matching column's data type and JSONB/UUID metadata flag.
            if let Some(source) = ins.source {
                if let SetExpr::Values(v) = source.body.as_ref() {
                    for row in &v.rows {
                        for (i, expr) in row.iter().enumerate() {
                            if let Some(n) = placeholder_index(expr) {
                                if let Some(col_name) = col_order.get(i) {
                                    if let Some(field) =
                                        column_field(meta.schema.as_ref(), col_name)
                                    {
                                        if let Some(slot) = out.get_mut(n - 1) {
                                            *slot = field.data_type().clone();
                                        }
                                        if field_is_jsonb(field) {
                                            if let Some(s) = is_jsonb_out.get_mut(n - 1) {
                                                *s = true;
                                            }
                                        } else if field_is_uuid(field) {
                                            if let Some(s) = is_uuid_out.get_mut(n - 1) {
                                                *s = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Statement::Query(q) => {
            walk_select_for_predicates(sess, &q, out, is_jsonb_out, is_uuid_out).await?;
        }
        Statement::Update(sqlparser::ast::Update {
            table,
            assignments,
            selection,
            ..
        }) => {
            // Resolve the target table once; placeholder slots in SET and
            // WHERE are typed against its column list.
            if let TableFactor::Table { name, .. } = &table.relation {
                if let Ok(tn) = name_to_table(name) {
                    if let Ok(meta) = sess
                        .engine
                        .config()
                        .catalog
                        .load_table(&sess.project, &tn)
                        .await
                    {
                        let schema = (*meta.schema).clone();
                        infer_assignments(&assignments, &schema, out, is_jsonb_out, is_uuid_out);
                        if let Some(pred) = &selection {
                            let outer_tables = vec![(tn.as_str().to_owned(), schema)];
                            walk_pred_with_subqueries(
                                sess,
                                pred,
                                &outer_tables,
                                out,
                                is_jsonb_out,
                                is_uuid_out,
                            )
                            .await;
                        }
                    }
                }
            }
        }
        Statement::Delete(del) => {
            // Single-table DELETE only — the engine rejects multi-table
            // forms at execute time and we mirror the same scope here.
            let tables = match &del.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            if let Some(twj) = tables.first() {
                if let TableFactor::Table { name, .. } = &twj.relation {
                    if let Ok(tn) = name_to_table(name) {
                        if let Ok(meta) = sess
                            .engine
                            .config()
                            .catalog
                            .load_table(&sess.project, &tn)
                            .await
                        {
                            let schema = (*meta.schema).clone();
                            if let Some(pred) = &del.selection {
                                let outer_tables = vec![(tn.as_str().to_owned(), schema)];
                                walk_pred_with_subqueries(
                                    sess,
                                    pred,
                                    &outer_tables,
                                    out,
                                    is_jsonb_out,
                                    is_uuid_out,
                                )
                                .await;
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Pin SET-clause placeholder slots to the destination column's data type.
/// `SET col = $1` → slot 0 takes the type of `col`. JSONB / UUID flags are
/// also propagated so the wire-protocol layer can advertise the right OIDs.
fn infer_assignments(
    assignments: &[Assignment],
    schema: &arrow_schema::Schema,
    out: &mut [DataType],
    is_jsonb_out: &mut [bool],
    is_uuid_out: &mut [bool],
) {
    for a in assignments {
        if let AssignmentTarget::ColumnName(name) = &a.target {
            if name.0.len() == 1 {
                let col = &name.0[0].id_val();
                if let Some(n) = placeholder_index(&a.value) {
                    if let Some(field) = column_field(schema, col) {
                        if let Some(slot) = out.get_mut(n - 1) {
                            *slot = field.data_type().clone();
                        }
                        if field_is_jsonb(field) {
                            if let Some(s) = is_jsonb_out.get_mut(n - 1) {
                                *s = true;
                            }
                        } else if field_is_uuid(field) {
                            if let Some(s) = is_uuid_out.get_mut(n - 1) {
                                *s = true;
                            }
                        }
                    }
                }
            }
        }
    }
}

fn name_to_table(name: &ObjectName) -> Result<TableName> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified table names not supported: {name}"
        )));
    }
    TableName::new(name.0[0].id_val().clone())
}

fn placeholder_index(e: &Expr) -> Option<usize> {
    match e {
        Expr::Value(ValueWithSpan {
            value: Value::Placeholder(s),
            ..
        }) => s.strip_prefix('$').and_then(|d| d.parse::<usize>().ok()),
        _ => None,
    }
}

/// Look up a column by name on an Arrow schema. Returns `None` if missing,
/// otherwise the borrowed `Field` so callers can read both the data type
/// and the `BASIN_TYPE` metadata (JSONB / UUID).
fn column_field<'a>(schema: &'a arrow_schema::Schema, col: &str) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .find(|f| f.name() == col)
        .map(|f| f.as_ref())
}

/// Map a `sqlparser` SQL [`DataType`][SqlDataType] that appears in an explicit
/// cast (`$1::int4`, `CAST($1 AS timestamptz)`) to the Arrow [`DataType`] that
/// Basin uses for that Postgres type.
///
/// Returns `None` for any type not in Basin's supported set; the caller leaves
/// that placeholder at the TEXT default (safe degradation, no panic).
fn cast_data_type_to_arrow(sql: &SqlDataType) -> Option<DataType> {
    match sql {
        // ── Integer family ──────────────────────────────────────────────────
        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => Some(DataType::Int16),
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => {
            Some(DataType::Int32)
        }
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => Some(DataType::Int64),

        // ── Floating-point ──────────────────────────────────────────────────
        // REAL and FLOAT4 are synonyms in PostgreSQL; sqlparser 0.61 has both.
        SqlDataType::Real | SqlDataType::Float4 => Some(DataType::Float32),
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float(_) => Some(DataType::Float64),

        // ── Text ────────────────────────────────────────────────────────────
        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::String(_) => Some(DataType::Utf8),

        // ── Boolean ─────────────────────────────────────────────────────────
        SqlDataType::Boolean | SqlDataType::Bool => Some(DataType::Boolean),

        // ── Binary ──────────────────────────────────────────────────────────
        SqlDataType::Bytea => Some(DataType::Binary),

        // ── Date / Time ─────────────────────────────────────────────────────
        SqlDataType::Date => Some(DataType::Date32),
        SqlDataType::Time(_, sqlparser::ast::TimezoneInfo::None)
        | SqlDataType::Time(_, sqlparser::ast::TimezoneInfo::WithoutTimeZone) => {
            Some(DataType::Time64(TimeUnit::Microsecond))
        }
        SqlDataType::Timestamp(_, tz) => match tz {
            sqlparser::ast::TimezoneInfo::Tz | sqlparser::ast::TimezoneInfo::WithTimeZone => Some(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            _ => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        },

        // ── UUID ─────────────────────────────────────────────────────────────
        // UUID occupies FixedSizeBinary(16) in Basin. The is_uuid flag is set
        // separately by the caller so the pgwire layer emits OID 2950.
        SqlDataType::Uuid => Some(DataType::FixedSizeBinary(16)),
        SqlDataType::Custom(name, modifiers)
            if name.0.len() == 1
                && name.0[0].id_val().eq_ignore_ascii_case("uuid")
                && modifiers.is_empty() =>
        {
            Some(DataType::FixedSizeBinary(16))
        }

        // ── Interval ────────────────────────────────────────────────────────
        SqlDataType::Interval { .. } => {
            Some(DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano))
        }

        // ── Numeric / Decimal ────────────────────────────────────────────────
        // Use (38, 0) — Basin's default for bare NUMERIC — rather than trying
        // to infer precision/scale at this stage. The probe SQL will carry the
        // right literal anyway; type-inference just needs a non-TEXT Arrow type.
        SqlDataType::Numeric(_) | SqlDataType::Decimal(_) | SqlDataType::Dec(_) => {
            Some(DataType::Decimal128(38, 0))
        }

        // Anything else: fall back to TEXT (safe default).
        _ => None,
    }
}

/// Walk each SELECT projection item looking for `$N::type` / `CAST($N AS type)`
/// patterns. For each match, record the inferred Arrow type (and UUID flag) for
/// placeholder slot N without overwriting any type already resolved by the more
/// authoritative WHERE-clause / catalog path.
fn walk_projection_for_casts(
    projection: &[SelectItem],
    out: &mut [DataType],
    is_uuid_out: &mut [bool],
) {
    for item in projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        pin_cast_placeholder(expr, out, is_uuid_out);
    }
}

/// If `expr` is `Cast { expr: Placeholder($N), data_type, .. }` (any CastKind),
/// record the inferred Arrow type in slot N. Only fills a slot that still holds
/// the TEXT default — it will not overwrite a type already set by the more
/// authoritative catalog-backed WHERE-clause path.
fn pin_cast_placeholder(expr: &Expr, out: &mut [DataType], is_uuid_out: &mut [bool]) {
    if let Expr::Cast {
        kind: CastKind::Cast | CastKind::DoubleColon | CastKind::TryCast | CastKind::SafeCast,
        expr: inner,
        data_type,
        ..
    } = expr
    {
        // We only handle a direct placeholder inside the cast, e.g. `$1::int4`.
        // Nested casts (`CAST(CAST($1 AS text) AS int4)`) are uncommon in
        // ORM output; if needed they can be added later.
        if let Some(n) = placeholder_index(inner) {
            if let Some(arrow_dt) = cast_data_type_to_arrow(data_type) {
                if let Some(slot) = out.get_mut(n - 1) {
                    // Only fill slots that are still TEXT (the default). A slot
                    // set by the WHERE-clause catalog path takes priority.
                    if *slot == DataType::Utf8 {
                        *slot = arrow_dt;
                    }
                }
                // UUID needs the metadata flag regardless of whether we set the type.
                let is_uuid = matches!(data_type, SqlDataType::Uuid)
                    || matches!(
                        data_type,
                        SqlDataType::Custom(name, modifiers)
                        if name.0.len() == 1
                            && name.0[0].id_val().eq_ignore_ascii_case("uuid")
                            && modifiers.is_empty()
                    );
                if is_uuid {
                    if let Some(s) = is_uuid_out.get_mut(n - 1) {
                        *s = true;
                    }
                }
            }
        }
    }
}

/// Walk a SELECT looking for `<col> OP $N` and `$N OP <col>` predicates we
/// can resolve to a column type. JSONB / UUID metadata flags are propagated
/// for the SELECT WHERE side so prepared `WHERE id = $1` over a UUID column
/// surfaces OID 2950 / 3802 instead of the BYTEA / TEXT default.
///
/// Also handles `WITH … DML_CTE … SELECT` queries: for each CTE whose body
/// is an INSERT, the INSERT VALUES columns are typed against the target table.
async fn walk_select_for_predicates(
    sess: &ProjectSession,
    q: &Query,
    out: &mut [DataType],
    is_jsonb_out: &mut [bool],
    is_uuid_out: &mut [bool],
) -> Result<()> {
    // ── DML CTE bodies ────────────────────────────────────────────────────────
    // `WITH ins AS (INSERT INTO t (c1, c2) VALUES ($1, $2) RETURNING …) SELECT …`
    // The outer query body is a plain SELECT; the INSERT lives in the CTE.
    // Walk each CTE's body: for INSERT, apply the same column-order inference
    // as the top-level INSERT handler.
    if let Some(ref with) = q.with {
        for cte in &with.cte_tables {
            if let SetExpr::Insert(Statement::Insert(ins)) = cte.query.body.as_ref() {
                // Best-effort: ignore errors (table may not exist yet, etc.).
                if let Ok(table_name) =
                    crate::pg_ast::insert_object_name(ins).and_then(|n| name_to_table(n))
                {
                    if let Ok(meta) = sess
                        .engine
                        .config()
                        .catalog
                        .load_table(&sess.project, &table_name)
                        .await
                    {
                        let col_order: Vec<String> = if !ins.columns.is_empty() {
                            ins.columns.iter().map(|c| c.value.clone()).collect()
                        } else {
                            meta.schema
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect()
                        };
                        if let Some(ref source) = ins.source {
                            if let SetExpr::Values(v) = source.body.as_ref() {
                                for row in &v.rows {
                                    for (i, expr) in row.iter().enumerate() {
                                        if let Some(n) = placeholder_index(expr) {
                                            if let Some(col_name) = col_order.get(i) {
                                                if let Some(field) =
                                                    column_field(meta.schema.as_ref(), col_name)
                                                {
                                                    if let Some(slot) = out.get_mut(n - 1) {
                                                        *slot = field.data_type().clone();
                                                    }
                                                    if field_is_jsonb(field) {
                                                        if let Some(s) = is_jsonb_out.get_mut(n - 1)
                                                        {
                                                            *s = true;
                                                        }
                                                    } else if field_is_uuid(field) {
                                                        if let Some(s) = is_uuid_out.get_mut(n - 1)
                                                        {
                                                            *s = true;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Ok(()),
    };
    // Collect (alias_or_name, schema) for the FROM clause's tables.
    let mut tables: Vec<(String, arrow_schema::Schema)> = Vec::new();
    for table_with_joins in &select.from {
        if let TableFactor::Table { name, alias, .. } = &table_with_joins.relation {
            if let Ok(tn) = name_to_table(name) {
                if let Ok(meta) = sess
                    .engine
                    .config()
                    .catalog
                    .load_table(&sess.project, &tn)
                    .await
                {
                    let label = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| tn.as_str().to_owned());
                    tables.push((label, (*meta.schema).clone()));
                }
            }
        }
    }

    // Walk SELECT projection items for explicit-cast placeholders: `$1::int4`,
    // `CAST($2 AS timestamptz)`, etc. This runs before the WHERE-clause walk
    // so that if the same placeholder appears in both positions the
    // catalog-backed WHERE inference (below) takes priority (it overwrites only
    // if the slot is still TEXT after projection inference — see `pin_cast_placeholder`).
    // Actually we want WHERE to win, so we call projection inference FIRST and
    // WHERE inference SECOND; `pin_cast_placeholder` only fills TEXT slots, so
    // a catalog-set type from WHERE will NOT be overwritten.
    //
    // Order: projection (explicit cast, low-authority) then WHERE (catalog, high-authority).
    walk_projection_for_casts(&select.projection, out, is_uuid_out);

    if let Some(pred) = &select.selection {
        walk_pred_with_subqueries(sess, pred, &tables, out, is_jsonb_out, is_uuid_out).await;
    }

    // LIMIT $N and OFFSET $N — Postgres types these as int8 (BIGINT). Drivers
    // that don't get this hint refuse to bind i64 values for them. Without
    // this, every ORM with `.limit(?)` / `.offset(?)` breaks at the client.
    if let Some(Expr::Value(ValueWithSpan {
        value: Value::Placeholder(s),
        ..
    })) = q.ext_limit()
    {
        if let Some(idx) = s.strip_prefix('$').and_then(|d| d.parse::<usize>().ok()) {
            if let Some(slot) = out.get_mut(idx.saturating_sub(1)) {
                *slot = DataType::Int64;
            }
        }
    }
    if let Some(off) = q.ext_offset() {
        if let Expr::Value(ValueWithSpan {
            value: Value::Placeholder(s),
            ..
        }) = &off.value
        {
            if let Some(idx) = s.strip_prefix('$').and_then(|d| d.parse::<usize>().ok()) {
                if let Some(slot) = out.get_mut(idx.saturating_sub(1)) {
                    *slot = DataType::Int64;
                }
            }
        }
    }
    Ok(())
}

/// Async variant of [`walk_pred`] that recurses into `EXISTS (subquery)` and
/// `col IN (subquery)` bodies.  For each subquery, the function loads the
/// subquery's own FROM-clause tables from the catalog and passes the combined
/// outer + subquery table set into a recursive call, enabling placeholder slots
/// like `$1` in `WHERE EXISTS (SELECT 1 FROM t WHERE t.col < $1)` to be typed
/// against `t.col`.
///
/// Falls back to the synchronous `walk_pred` for all expression shapes that
/// do not contain subqueries.
async fn walk_pred_with_subqueries(
    sess: &ProjectSession,
    expr: &Expr,
    tables: &[(String, Schema)],
    out: &mut [DataType],
    is_jsonb_out: &mut [bool],
    is_uuid_out: &mut [bool],
) {
    match expr {
        // Recurse into AND / OR compound predicates.
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And | BinaryOperator::Or,
            right,
        } => {
            // Box the future to avoid infinitely-sized recursive types.
            Box::pin(walk_pred_with_subqueries(
                sess,
                left,
                tables,
                out,
                is_jsonb_out,
                is_uuid_out,
            ))
            .await;
            Box::pin(walk_pred_with_subqueries(
                sess,
                right,
                tables,
                out,
                is_jsonb_out,
                is_uuid_out,
            ))
            .await;
        }
        // EXISTS (subquery) / NOT EXISTS — load the subquery's FROM tables and
        // recurse into its WHERE clause.
        Expr::Exists { subquery, .. } => {
            let sub_tables = load_query_from_tables(sess, subquery).await;
            let combined: Vec<(String, Schema)> =
                tables.iter().cloned().chain(sub_tables).collect();
            if let SetExpr::Select(sel) = subquery.body.as_ref() {
                if let Some(pred) = &sel.selection {
                    Box::pin(walk_pred_with_subqueries(
                        sess,
                        pred,
                        &combined,
                        out,
                        is_jsonb_out,
                        is_uuid_out,
                    ))
                    .await;
                }
            }
        }
        // col IN (subquery) — load the subquery's FROM tables and recurse into
        // its WHERE clause.
        Expr::InSubquery { subquery, .. } => {
            let sub_tables = load_query_from_tables(sess, subquery).await;
            let combined: Vec<(String, Schema)> =
                tables.iter().cloned().chain(sub_tables).collect();
            if let SetExpr::Select(sel) = subquery.body.as_ref() {
                if let Some(pred) = &sel.selection {
                    Box::pin(walk_pred_with_subqueries(
                        sess,
                        pred,
                        &combined,
                        out,
                        is_jsonb_out,
                        is_uuid_out,
                    ))
                    .await;
                }
            }
        }
        // For all other expression shapes, fall back to the synchronous walker.
        other => walk_pred(other, tables, out, is_jsonb_out, is_uuid_out),
    }
}

/// Load the `(label, Schema)` pairs for every direct `Table` factor in a
/// query's FROM clause.  Used to resolve placeholder types inside subqueries.
/// Errors are silently ignored — this is best-effort inference.
async fn load_query_from_tables(sess: &ProjectSession, query: &Query) -> Vec<(String, Schema)> {
    let mut result = Vec::new();
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return result,
    };
    for twj in &select.from {
        if let TableFactor::Table { name, alias, .. } = &twj.relation {
            if let Ok(tn) = name_to_table(name) {
                if let Ok(meta) = sess
                    .engine
                    .config()
                    .catalog
                    .load_table(&sess.project, &tn)
                    .await
                {
                    let label = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| tn.as_str().to_owned());
                    result.push((label, (*meta.schema).clone()));
                }
            }
        }
    }
    result
}

fn walk_pred(
    expr: &Expr,
    tables: &[(String, arrow_schema::Schema)],
    out: &mut [DataType],
    is_jsonb_out: &mut [bool],
    is_uuid_out: &mut [bool],
) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                walk_pred(left, tables, out, is_jsonb_out, is_uuid_out);
                walk_pred(right, tables, out, is_jsonb_out, is_uuid_out);
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                pin_pair(left, right, tables, out, is_jsonb_out, is_uuid_out);
                pin_pair(right, left, tables, out, is_jsonb_out, is_uuid_out);
            }
            _ => {}
        },
        // `<col> IN ($1, $2, ...)` — type each placeholder in the list
        // to the column's type. Both `col IN (...)` and `$N IN (col)` are
        // handled; ORMs almost always use the former.
        Expr::InList {
            expr: col_expr,
            list,
            negated: _,
        } => {
            if let Some((dt, is_jsonb, is_uuid)) = resolve_column_meta(col_expr, tables) {
                for item in list {
                    if let Some(n) = placeholder_index(item) {
                        if let Some(slot) = out.get_mut(n - 1) {
                            *slot = dt.clone();
                        }
                        if is_jsonb {
                            if let Some(s) = is_jsonb_out.get_mut(n - 1) {
                                *s = true;
                            }
                        } else if is_uuid {
                            if let Some(s) = is_uuid_out.get_mut(n - 1) {
                                *s = true;
                            }
                        }
                    }
                }
            }
        }
        // Recurse into nested parentheses or NOT expressions.
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
            walk_pred(inner, tables, out, is_jsonb_out, is_uuid_out);
        }
        _ => {}
    }
}

/// If `placeholder` is a `$N` and `column` resolves to a known column on one
/// of the tables in scope, copy the column's data type into the output slot
/// and (when applicable) set its JSONB / UUID flag.
fn pin_pair(
    placeholder: &Expr,
    column: &Expr,
    tables: &[(String, arrow_schema::Schema)],
    out: &mut [DataType],
    is_jsonb_out: &mut [bool],
    is_uuid_out: &mut [bool],
) {
    let n = match placeholder_index(placeholder) {
        Some(n) => n,
        None => return,
    };
    if let Some((dt, is_jsonb, is_uuid)) = resolve_column_meta(column, tables) {
        if let Some(slot) = out.get_mut(n - 1) {
            *slot = dt;
        }
        if is_jsonb {
            if let Some(s) = is_jsonb_out.get_mut(n - 1) {
                *s = true;
            }
        } else if is_uuid {
            if let Some(s) = is_uuid_out.get_mut(n - 1) {
                *s = true;
            }
        }
    }
}

/// Resolve a column expression against a set of in-scope tables. Returns the
/// Arrow data type plus the JSONB / UUID flags read off the field metadata.
fn resolve_column_meta(
    e: &Expr,
    tables: &[(String, arrow_schema::Schema)],
) -> Option<(DataType, bool, bool)> {
    match e {
        Expr::Identifier(id) => {
            for (_, schema) in tables {
                if let Some(field) = column_field(schema, &id.value) {
                    return Some((
                        field.data_type().clone(),
                        field_is_jsonb(field),
                        field_is_uuid(field),
                    ));
                }
            }
            None
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let (qualifier, col) = (&parts[0].value, &parts[1].value);
            for (label, schema) in tables {
                if label == qualifier {
                    return column_field(schema, col)
                        .map(|f| (f.data_type().clone(), field_is_jsonb(f), field_is_uuid(f)));
                }
            }
            None
        }
        _ => None,
    }
}

pub(crate) async fn describe_statement(
    sess: &ProjectSession,
    handle: &StatementHandle,
) -> Result<StatementSchema> {
    let guard = sess.state.prepared.inner.read().await;
    let entry = guard
        .get(handle)
        .ok_or_else(|| BasinError::not_found(format!("prepared statement {handle:?}")))?;
    Ok(entry.schema.clone())
}

pub(crate) async fn close_statement(sess: &ProjectSession, handle: &StatementHandle) {
    sess.state.prepared.inner.write().await.remove(handle);
}

pub(crate) async fn bind(
    sess: &ProjectSession,
    handle: &StatementHandle,
    params: Vec<ScalarParam>,
) -> Result<BoundStatement> {
    let guard = sess.state.prepared.inner.read().await;
    let entry = guard
        .get(handle)
        .ok_or_else(|| BasinError::not_found(format!("prepared statement {handle:?}")))?;
    if params.len() != entry.placeholder_count {
        return Err(BasinError::InvalidSchema(format!(
            "bind: expected {} parameters, got {}",
            entry.placeholder_count,
            params.len()
        )));
    }
    let sql = substitute(&entry.sql, &params)?;
    Ok(BoundStatement {
        handle: *handle,
        sql,
    })
}

pub(crate) async fn execute_bound(
    sess: &ProjectSession,
    bound: BoundStatement,
) -> Result<ExecResult> {
    crate::executor::execute(sess, &bound.sql).await
}

/// Count `$N` placeholders. Returns the maximum N (so `$1, $1, $2` => 2).
/// Errors on `$0` (Postgres uses 1-based numbering) or numbered gaps that
/// would be ambiguous to substitute back.
///
/// The scanner respects:
/// - single-quoted strings: `'...'`, with `''` for an embedded apostrophe
///   (per `standard_conforming_strings = on`).
/// - double-quoted identifiers: `"..."`, with `""` for an embedded quote.
/// - line comments `-- ... \n`
/// - block comments `/* ... */` (non-nested; PostgreSQL nests, but the PoC
///   doesn't need that yet — real customer SQL rarely has nested block
///   comments and the limit is documented).
fn scan_placeholders(sql: &str) -> Result<usize> {
    let mut max_n: usize = 0;
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
                // Skip to the closing quote.
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                if i + 1 < bytes.len() {
                    i += 2;
                }
            }
            b'$' => {
                let mut j = i + 1;
                let start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start {
                    let digits = std::str::from_utf8(&bytes[start..j])
                        .map_err(|e| BasinError::internal(format!("placeholder utf8: {e}")))?;
                    let n: usize = digits.parse().map_err(|e| {
                        BasinError::InvalidSchema(format!("bad placeholder ${digits}: {e}"))
                    })?;
                    if n == 0 {
                        return Err(BasinError::InvalidSchema(
                            "placeholder $0 is not valid; numbering starts at $1".into(),
                        ));
                    }
                    if n > max_n {
                        max_n = n;
                    }
                    i = j;
                    continue;
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    Ok(max_n)
}

/// Substitute `$N` placeholders with literal SQL forms of `params[N-1]`.
/// Reuses the same scanner walking pattern as [`scan_placeholders`] so the
/// rules about strings/comments/identifiers stay in sync.
fn substitute(sql: &str, params: &[ScalarParam]) -> Result<String> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + params.len() * 8);
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            b'"' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                let start = i;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                let start = i;
                i += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                if i + 1 < bytes.len() {
                    i += 2;
                }
                out.push_str(&sql[start..i]);
            }
            b'$' => {
                let mut j = i + 1;
                let start = j;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j > start {
                    let digits = &sql[start..j];
                    let n: usize = digits.parse().map_err(|e| {
                        BasinError::InvalidSchema(format!("bad placeholder ${digits}: {e}"))
                    })?;
                    let p = params.get(n - 1).ok_or_else(|| {
                        BasinError::InvalidSchema(format!("placeholder ${n} has no bound value"))
                    })?;
                    out.push_str(&render_param(p));
                    i = j;
                    continue;
                }
                out.push('$');
                i += 1;
            }
            _ => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    Ok(out)
}

fn render_param(p: &ScalarParam) -> String {
    match p {
        ScalarParam::Null => "NULL".into(),
        ScalarParam::Int4(v) => v.to_string(),
        ScalarParam::Int8(v) => v.to_string(),
        ScalarParam::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        ScalarParam::Float8(f) => {
            if f.is_nan() {
                "'NaN'::float8".into()
            } else if f.is_infinite() {
                if *f > 0.0 {
                    "'Infinity'::float8".into()
                } else {
                    "'-Infinity'::float8".into()
                }
            } else {
                // Rust's float Display drops trailing zeros; explicitly request
                // a decimal point so SQL doesn't accidentally read "3" as int.
                let s = format!("{f}");
                if s.contains('.') || s.contains('e') || s.contains('E') {
                    s
                } else {
                    format!("{s}.0")
                }
            }
        }
        ScalarParam::Text(s) => quote_string(s),
        ScalarParam::Bytea(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2 + 4);
            hex.push_str("'\\x");
            for b in bytes {
                hex.push_str(&format!("{:02x}", b));
            }
            hex.push_str("'::bytea");
            hex
        }
    }
}

/// Wrap a string in single quotes, doubling embedded single quotes. This is
/// the SQL-standard `standard_conforming_strings = on` form Postgres uses by
/// default; it does NOT interpret backslashes specially.
fn quote_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
            out.push('\'');
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Substitute every placeholder with a representative literal of the
/// inferred parameter type, so DataFusion's planner can run successfully and
/// return us a result schema. NULL would be ambiguous in many positions
/// (e.g. `WHERE col = $1` becomes `WHERE col = NULL` which is always false
/// but at least valid; `INSERT INTO t VALUES ($1)` becomes a NULL into a
/// nullable column or a type-check failure into a NOT NULL column). The
/// representative literals here just need to be syntactically valid for the
/// surrounding operation.
fn substitute_for_probe(sql: &str, param_types: &[DataType]) -> String {
    let params: Vec<ScalarParam> = param_types
        .iter()
        .map(|dt| match dt {
            DataType::Int64 => ScalarParam::Int8(0),
            DataType::Int32 | DataType::Int16 | DataType::Int8 => ScalarParam::Int4(0),
            DataType::Float64 | DataType::Float32 => ScalarParam::Float8(0.0),
            DataType::Boolean => ScalarParam::Bool(false),
            DataType::Utf8 | DataType::LargeUtf8 => ScalarParam::Text(String::new()),
            _ => ScalarParam::Null,
        })
        .collect();
    substitute(sql, &params).unwrap_or_else(|_| sql.to_owned())
}

async fn probe_schema(sess: &ProjectSession, sql: &str) -> Result<Vec<Field>> {
    // Only SELECT (and WITH ... SELECT) can yield a row description ahead of
    // execute; for anything else we return no columns and the caller falls back.
    let trimmed = sql.trim_start().to_ascii_uppercase();
    if !trimmed.starts_with("SELECT") && !trimmed.starts_with("WITH") {
        return Ok(Vec::new());
    }

    // Apply the same SELECT-path text rewrites that `execute()` applies, so
    // that DataFusion can plan SQL forms it wouldn't accept verbatim (e.g.
    // LATERAL aggregate patterns rewritten to GROUP BY joins).
    let rewritten = crate::pg_operators::rewrite_lateral_nested_agg(sql);
    let rewritten = crate::pg_operators::rewrite_lateral_uncorrelated(&rewritten);
    let probe_sql = &rewritten;

    // Try planning the (possibly rewritten) SQL directly. This handles plain
    // SELECTs and WITH-pure-SELECT CTEs.
    let plan_result = sess.ctx.sql(probe_sql).await;

    let ws_schema = match plan_result {
        Ok(logical) => {
            let df_schema = logical.schema().inner().clone();
            Arc::new(crate::convert::schema_df_to_ws(df_schema.as_ref())?)
        }
        Err(_) => {
            // Direct planning failed. Try the DML-CTE special path: for
            // `WITH cte AS (INSERT/UPDATE/DELETE ... RETURNING ...) SELECT ...`
            // we synthesize a MemTable from the RETURNING columns and probe
            // just the outer SELECT so the RowDescription is correct at
            // Describe time.
            if let Some(fields) = probe_dml_cte_schema(sess, probe_sql).await {
                Arc::new(Schema::new(fields))
            } else {
                // Unplannable form — fall back to no columns; the schema will
                // be discovered at execute time.
                return Ok(Vec::new());
            }
        }
    };

    // Annotate json_agg / jsonb_agg result columns with BASIN_TYPE=JSONB so the
    // RowDescription sent at Describe time advertises OID 3802 (JSONB) rather than
    // TEXT. This is required for drivers that use the Describe-time RowDescription
    // to choose the correct wire deserializer (tokio-postgres, psycopg3, etc.).
    let ws_schema = crate::executor::annotate_json_agg_columns(&ws_schema, probe_sql);
    Ok(ws_schema.fields().iter().map(|f| (**f).clone()).collect())
}

/// Synthesise the output schema for a data-modifying CTE
/// `WITH cte AS (INSERT/UPDATE/DELETE … RETURNING …) SELECT …` at prepare
/// time.  DataFusion cannot plan the INSERT/UPDATE/DELETE body, but we can:
///
/// 1. Parse the statement to find DML CTEs with RETURNING clauses.
/// 2. Determine the RETURNING column types from the catalog.
/// 3. Register a temporary MemTable for each such CTE.
/// 4. Plan just the outer SELECT body to get its schema.
/// 5. Deregister the temporary tables before returning.
///
/// Returns `None` if the SQL cannot be parsed or the required catalog tables
/// are not found (falls back to 0-column RowDescription, safe degradation).
async fn probe_dml_cte_schema(sess: &ProjectSession, sql: &str) -> Option<Vec<Field>> {
    use datafusion::arrow::datatypes as dfa;
    use datafusion::datasource::memory::MemTable;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    let stmt = stmts.into_iter().next()?;
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return None,
    };
    let with = query.with.as_ref()?;
    if !with.cte_tables.iter().any(|cte| {
        matches!(
            cte.query.body.as_ref(),
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
        )
    }) {
        return None;
    }

    let mut registered: Vec<String> = Vec::new();

    for cte in &with.cte_tables {
        let cte_name = cte.alias.name.value.clone();
        // Only handle INSERT-RETURNING CTEs for now.
        let ins = match cte.query.body.as_ref() {
            SetExpr::Insert(Statement::Insert(s)) => s,
            _ => continue,
        };

        // Determine the RETURNING columns: explicit list or default to all table columns.
        let returning = ins.returning.as_deref().unwrap_or(&[]);
        let table_name = match crate::pg_ast::insert_object_name(ins).and_then(|n| name_to_table(n))
        {
            Ok(tn) => tn,
            Err(_) => continue,
        };
        let meta = match sess
            .engine
            .config()
            .catalog
            .load_table(&sess.project, &table_name)
            .await
        {
            Ok(m) => m,
            Err(_) => continue,
        };

        // Build the MemTable schema from RETURNING columns.
        let df_fields: Vec<dfa::FieldRef> = if returning.is_empty() {
            // No RETURNING clause — treat as 0-column result.
            vec![]
        } else {
            let mut fields = Vec::new();
            for sel in returning {
                match sel {
                    SelectItem::Wildcard(_) => {
                        // RETURNING * — all table columns.
                        for f in meta.schema.fields() {
                            let ws_dt = crate::convert::schema_df_to_ws(&Schema::new(vec![f
                                .as_ref()
                                .clone()]))
                            .ok()
                            .and_then(|s| s.fields().first().map(|f2| f2.data_type().clone()));
                            if let Some(_dt) = ws_dt {
                                // Convert ws field → df field for MemTable.
                                if let Ok(df_schema) =
                                    crate::convert::schema_ws_to_df(&Schema::new(vec![f
                                        .as_ref()
                                        .clone()]))
                                {
                                    if let Some(df_f) = df_schema.fields().first() {
                                        fields.push(Arc::clone(df_f));
                                    }
                                }
                            }
                        }
                    }
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                        if let Some(f) = column_field(meta.schema.as_ref(), &id.value) {
                            if let Ok(df_schema) =
                                crate::convert::schema_ws_to_df(&Schema::new(vec![f.clone()]))
                            {
                                if let Some(df_f) = df_schema.fields().first() {
                                    fields.push(Arc::clone(df_f));
                                }
                            }
                        }
                    }
                    SelectItem::ExprWithAlias {
                        expr: Expr::Identifier(id),
                        alias,
                    } => {
                        if let Some(f) = column_field(meta.schema.as_ref(), &id.value) {
                            if let Ok(df_schema) =
                                crate::convert::schema_ws_to_df(&Schema::new(vec![f.clone()]))
                            {
                                if let Some(df_f) = df_schema.fields().first() {
                                    // Rename the field to the alias.
                                    let renamed = dfa::Field::new(
                                        &alias.value,
                                        df_f.data_type().clone(),
                                        df_f.is_nullable(),
                                    );
                                    fields.push(Arc::new(renamed));
                                }
                            }
                        }
                    }
                    _ => {} // Complex expressions — skip for now.
                }
            }
            fields
        };

        if df_fields.is_empty() {
            continue;
        }

        let df_schema = Arc::new(dfa::Schema::new(df_fields));
        // Register an empty MemTable so DataFusion can plan the outer SELECT.
        let provider = MemTable::try_new(df_schema, vec![vec![]]).ok()?;
        // Deregister any existing table with this name first.
        let _ = sess.ctx.deregister_table(&cte_name);
        sess.ctx
            .register_table(&cte_name, Arc::new(provider))
            .ok()?;
        registered.push(cte_name);
    }

    if registered.is_empty() {
        // Clean up and give up.
        return None;
    }

    // Build the outer SELECT SQL (body without DML CTEs).
    let outer_sql = query.body.to_string();
    let result = match sess.ctx.sql(&outer_sql).await {
        Ok(logical) => {
            let df_schema = logical.schema().inner().clone();
            crate::convert::schema_df_to_ws(df_schema.as_ref())
                .ok()
                .map(|ws| {
                    ws.fields()
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect::<Vec<_>>()
                })
        }
        Err(_) => None,
    };

    // Deregister temp tables.
    for name in &registered {
        let _ = sess.ctx.deregister_table(name);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_finds_basic_placeholders() {
        assert_eq!(
            scan_placeholders("SELECT * FROM t WHERE id = $1").unwrap(),
            1
        );
        assert_eq!(
            scan_placeholders("INSERT INTO t VALUES ($1, $2, $3)").unwrap(),
            3
        );
        assert_eq!(scan_placeholders("SELECT 1").unwrap(), 0);
    }

    #[test]
    fn scan_skips_string_literals() {
        // `$1` inside a string literal must not be counted.
        let n = scan_placeholders("SELECT '$1', $2").unwrap();
        assert_eq!(n, 2, "outer $2 should still count");
    }

    #[test]
    fn scan_skips_doubled_quote_inside_string() {
        // `''` inside a single-quoted literal does not terminate it.
        let n = scan_placeholders("SELECT 'it''s a $1', $1").unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn scan_skips_quoted_identifier() {
        let n = scan_placeholders("SELECT \"col$1\", $2 FROM t").unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn scan_rejects_zero() {
        assert!(scan_placeholders("SELECT $0").is_err());
    }

    #[test]
    fn substitute_int_and_string() {
        let sql = "INSERT INTO t VALUES ($1, $2)";
        let out = substitute(sql, &[ScalarParam::Int8(7), ScalarParam::Text("hi".into())]).unwrap();
        assert_eq!(out, "INSERT INTO t VALUES (7, 'hi')");
    }

    #[test]
    fn substitute_escapes_apostrophe() {
        let out = substitute("SELECT $1", &[ScalarParam::Text("it's".into())]).unwrap();
        assert_eq!(out, "SELECT 'it''s'");
    }

    #[test]
    fn substitute_null() {
        let out = substitute("SELECT $1", &[ScalarParam::Null]).unwrap();
        assert_eq!(out, "SELECT NULL");
    }

    #[test]
    fn substitute_bool() {
        let out = substitute(
            "SELECT $1, $2",
            &[ScalarParam::Bool(true), ScalarParam::Bool(false)],
        )
        .unwrap();
        assert_eq!(out, "SELECT TRUE, FALSE");
    }

    #[test]
    fn substitute_float_keeps_decimal_point() {
        let out = substitute("SELECT $1", &[ScalarParam::Float8(3.0)]).unwrap();
        assert_eq!(out, "SELECT 3.0");
        // Use a non-PI value to keep clippy::approx_constant happy —
        // the test only cares that decimal-point preservation works.
        let out = substitute("SELECT $1", &[ScalarParam::Float8(2.25)]).unwrap();
        assert_eq!(out, "SELECT 2.25");
    }

    #[test]
    fn substitute_string_in_literal_left_alone() {
        // `$1` inside an existing single-quoted literal must NOT be substituted.
        let out = substitute("SELECT '$1', $1", &[ScalarParam::Int8(42)]).unwrap();
        assert_eq!(out, "SELECT '$1', 42");
    }

    #[test]
    fn substitute_rejects_injection_attempt() {
        // The classic ' OR 1=1 -- attack: when shoved into a Text param, the
        // surrounding single-quote escaping prevents it from breaking out of
        // the literal context.
        let out = substitute(
            "SELECT * FROM t WHERE name = $1",
            &[ScalarParam::Text("'; DROP TABLE t; --".into())],
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE name = '''; DROP TABLE t; --'");
    }

    #[test]
    fn substitute_param_appearing_twice() {
        let out = substitute("SELECT $1 + $1", &[ScalarParam::Int4(5)]).unwrap();
        assert_eq!(out, "SELECT 5 + 5");
    }

    // -------------------------------------------------------------------------
    // Projection-cast type inference unit tests (#60)
    //
    // These tests exercise `walk_projection_for_casts` + `pin_cast_placeholder`
    // + `cast_data_type_to_arrow` via a small harness that parses a SQL string
    // and runs the projection walker, mimicking what `infer_param_types` does.
    // -------------------------------------------------------------------------

    /// Parse a SELECT SQL string, run the projection-cast walker, and return the
    /// resulting `out` vector (one DataType per placeholder, 1-indexed).
    fn infer_via_projection(sql: &str, placeholder_count: usize) -> (Vec<DataType>, Vec<bool>) {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).expect("parse");
        let stmt = stmts.into_iter().next().expect("one stmt");
        let q = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!("expected Query"),
        };
        let select = match q.body.as_ref() {
            sqlparser::ast::SetExpr::Select(s) => s,
            _ => panic!("expected Select"),
        };
        let mut out = vec![DataType::Utf8; placeholder_count];
        let mut is_uuid = vec![false; placeholder_count];
        walk_projection_for_casts(&select.projection, &mut out, &mut is_uuid);
        (out, is_uuid)
    }

    #[test]
    fn projection_cast_infers_int2() {
        let (out, _) = infer_via_projection("SELECT $1::int2 AS v", 1);
        assert_eq!(out[0], DataType::Int16);
    }

    #[test]
    fn projection_cast_infers_int4_double_colon() {
        let (out, _) = infer_via_projection("SELECT $1::int4 AS v", 1);
        assert_eq!(out[0], DataType::Int32);
    }

    #[test]
    fn projection_cast_infers_int4_cast_syntax() {
        let (out, _) = infer_via_projection("SELECT CAST($1 AS int4) AS v", 1);
        assert_eq!(out[0], DataType::Int32);
    }

    #[test]
    fn projection_cast_infers_int8() {
        let (out, _) = infer_via_projection("SELECT $1::int8 AS v", 1);
        assert_eq!(out[0], DataType::Int64);
    }

    #[test]
    fn projection_cast_infers_bigint() {
        let (out, _) = infer_via_projection("SELECT $1::bigint AS v", 1);
        assert_eq!(out[0], DataType::Int64);
    }

    #[test]
    fn projection_cast_infers_float4() {
        let (out, _) = infer_via_projection("SELECT $1::float4 AS v", 1);
        assert_eq!(out[0], DataType::Float32);
    }

    #[test]
    fn projection_cast_infers_float8() {
        let (out, _) = infer_via_projection("SELECT $1::float8 AS v", 1);
        assert_eq!(out[0], DataType::Float64);
    }

    #[test]
    fn projection_cast_infers_text() {
        // text → Utf8 (stays at the default but is explicitly asserted)
        let (out, _) = infer_via_projection("SELECT $1::text AS v", 1);
        assert_eq!(out[0], DataType::Utf8);
    }

    #[test]
    fn projection_cast_infers_bool() {
        let (out, _) = infer_via_projection("SELECT $1::bool AS v", 1);
        assert_eq!(out[0], DataType::Boolean);
    }

    #[test]
    fn projection_cast_infers_boolean() {
        let (out, _) = infer_via_projection("SELECT $1::boolean AS v", 1);
        assert_eq!(out[0], DataType::Boolean);
    }

    #[test]
    fn projection_cast_infers_date() {
        let (out, _) = infer_via_projection("SELECT $1::date AS v", 1);
        assert_eq!(out[0], DataType::Date32);
    }

    #[test]
    fn projection_cast_infers_timestamp() {
        let (out, _) = infer_via_projection("SELECT $1::timestamp AS v", 1);
        assert_eq!(out[0], DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn projection_cast_infers_timestamptz() {
        let (out, _) = infer_via_projection("SELECT $1::timestamptz AS v", 1);
        assert_eq!(
            out[0],
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn projection_cast_infers_uuid_sets_flag() {
        let (out, is_uuid) = infer_via_projection("SELECT $1::uuid AS v", 1);
        assert_eq!(out[0], DataType::FixedSizeBinary(16));
        assert!(is_uuid[0], "UUID placeholder must set is_uuid flag");
    }

    #[test]
    fn projection_cast_infers_bytea() {
        let (out, _) = infer_via_projection("SELECT $1::bytea AS v", 1);
        assert_eq!(out[0], DataType::Binary);
    }

    #[test]
    fn projection_cast_infers_multiple_params() {
        let (out, is_uuid) =
            infer_via_projection("SELECT $1::int4 AS a, $2::uuid AS b, $3::text AS c", 3);
        assert_eq!(out[0], DataType::Int32);
        assert_eq!(out[1], DataType::FixedSizeBinary(16));
        assert!(is_uuid[1]);
        assert_eq!(out[2], DataType::Utf8);
    }

    #[test]
    fn projection_cast_does_not_overwrite_already_set_type() {
        // Simulate: slot 0 already set to Int64 by the WHERE-clause path.
        // walk_projection_for_casts must not overwrite it with Int32.
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let sql = "SELECT $1::int4 AS v";
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).expect("parse");
        let stmt = stmts.into_iter().next().unwrap();
        let q = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => panic!(),
        };
        let select = match q.body.as_ref() {
            sqlparser::ast::SetExpr::Select(s) => s,
            _ => panic!(),
        };
        // Pre-set slot 0 to Int64 (as if WHERE col = $1 on a BIGINT column had run).
        let mut out = vec![DataType::Int64];
        let mut is_uuid = vec![false];
        walk_projection_for_casts(&select.projection, &mut out, &mut is_uuid);
        // Int64 must survive; projection cast cannot overwrite a non-TEXT slot.
        assert_eq!(
            out[0],
            DataType::Int64,
            "catalog type must not be overwritten"
        );
    }

    #[test]
    fn projection_cast_unknown_type_leaves_text() {
        // An unsupported custom type (e.g., a domain type) must leave TEXT.
        // "citext" is not in cast_data_type_to_arrow's supported set.
        let (out, _) = infer_via_projection("SELECT $1::citext AS v", 1);
        assert_eq!(
            out[0],
            DataType::Utf8,
            "unsupported cast type must degrade to TEXT"
        );
    }
}
