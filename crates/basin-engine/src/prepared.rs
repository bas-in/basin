//! Prepared-statement support for the Postgres extended-query protocol.
//!
//! v1 strategy is "rebind into a fresh SQL string": we scan the SQL once at
//! prepare time to count `$N` placeholders and try to plan it for column
//! schema discovery, then at bind time we substitute the parameter values
//! into the SQL as literals and re-run the simple-query path. No plan caching
//! and no DataFusion-level parameter binding — the goal here is correctness
//! and unblocking every Postgres driver, not throughput.
//!
//! Layered on top of that baseline are three per-`Execute` fast paths, each
//! with the text route as its guaranteed fallback:
//!
//! 1. **AST cache** (perf-w-prepared): INSERT / plain-SELECT templates whose
//!    text the rewrite pipeline doesn't touch are parsed once at prepare;
//!    each bind substitutes values into a clone and dispatches the
//!    `Statement` directly (no per-`Execute` re-parse).
//! 2. **Bind-direct INSERT** ([`build_bind_insert_plan`] →
//!    `executor::try_insert_bind_direct`): plain all-placeholder
//!    `INSERT … VALUES ($1, …)` templates additionally precompute a
//!    (table, columns, param→tuple-position) plan; each `Execute` builds the
//!    Arrow batch straight from the decoded bind values — no SQL text and no
//!    AST in the loop.
//! 3. **Prepared-literal INSERT**: zero-parameter templates the values_fast
//!    classifier accepts skip prepare-time parsing entirely and execute
//!    through the text route's pre-parse scanner hook each time (cheaper
//!    than cloning + re-rendering a multi-MB literal AST per `Execute`).
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
    Assignment, AssignmentTarget, BinaryOperator, CastKind, Expr, FromTable, FunctionArg,
    FunctionArgExpr, FunctionArguments, ObjectName, Query, SelectItem, SetExpr, Statement,
    TableFactor, Value,
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
///
/// `Array` carries the elements of a PG one-dimensional array (`int4[]`,
/// `int8[]`, `text[]`, `bool[]`, `float8[]`). At substitution time we render
/// it as `ARRAY[a, b, c]` so the engine's existing `= ANY(ARRAY[…])` →
/// `IN (…)` rewriter can plan it. Multi-dimensional arrays are not supported
/// (no ORM emits them through prepared statements in practice).
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarParam {
    Null,
    Int8(i64),
    Int4(i32),
    Bool(bool),
    Float8(f64),
    Text(String),
    Bytea(Vec<u8>),
    /// TIMESTAMPTZ bind value as microseconds since the Unix epoch, UTC.
    /// The router decodes both wire formats into this variant (binary:
    /// i64 micros since the PG epoch 2000-01-01, rebased; text: ISO-8601 /
    /// PG-shaped literals). Substitution renders it as a
    /// `'…+00'::timestamptz` literal so the text and AST routes coerce it
    /// exactly like a user-written timestamptz literal; the bind-direct
    /// INSERT path feeds the same literal through the values_fast
    /// accumulators (which parse it with the slow path's
    /// `parse_timestamp_string`).
    Timestamptz(i64),
    Array(Vec<ScalarParam>),
}

/// Output of [`ProjectSession::bind`]. Holds the substituted SQL and the
/// originating handle (for traceability). Opaque on purpose — the router
/// should never read its fields directly. `Clone` so the router can hold a
/// portal across separate `Execute` rounds without re-binding.
#[derive(Clone)]
pub struct BoundStatement {
    pub(crate) handle: StatementHandle,
    pub(crate) sql: String,
    /// Bind-INSERT fast path (perf-w-prepared): `Some` when the originating
    /// prepared statement was AST-fast-path eligible AND every bind value was
    /// substituted into a clone of the cached template AST. `execute_bound`
    /// then dispatches this `Statement` directly via
    /// `executor::execute_statement`, skipping the per-`Execute` re-parse.
    /// `None` falls back to executing `sql` through the normal text route, so
    /// the slow path is unchanged.
    pub(crate) fast_ast: Option<Statement>,
    /// Bind-direct INSERT fast path (extended-protocol shape 2): the
    /// prepare-time plan plus the decoded bind values, carried through so
    /// `execute_bound` can build the RecordBatch directly
    /// (`executor::try_insert_bind_direct`) without touching `fast_ast` or
    /// `sql`. `None`, or an execute-time decline, falls through to the
    /// `fast_ast` / text routes above unchanged.
    pub(crate) bind_direct: Option<(Arc<crate::executor::BindInsertPlan>, Vec<ScalarParam>)>,
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
            fast_ast: None,
            bind_direct: None,
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
    /// The template SQL parsed once at `prepare` time. `None` when the SQL
    /// couldn't be parsed as a single sqlparser `Statement` (multi-statement,
    /// or a form only libpg_query accepts) — the bind path then renders text
    /// and re-parses through the normal `execute()` route, exactly as before.
    ast: Option<Arc<Statement>>,
    /// `true` when the bind path may substitute values into a clone of `ast`
    /// and dispatch the `Statement` directly via `executor::execute_statement`,
    /// skipping the per-`Execute` re-parse. Gated at `prepare` time on:
    ///   * the template parses to a single `Statement`, AND
    ///   * it is an `INSERT` or a plain `SELECT` (`Statement::Query`), AND
    ///   * the template text triggers NO entry in the executor's string-rewrite
    ///     pipeline (`needs_rewrite_pipeline` is false) — so the AST is exactly
    ///     what the text path would have dispatched.
    /// Any `false` here means the bind path falls back to the text route.
    ast_fast_ok: bool,
    /// Bind-direct parameterized-INSERT plan (extended-protocol shape 2),
    /// precomputed by [`build_bind_insert_plan`] when the template is an
    /// AST-fast-eligible `INSERT INTO t [(cols)] VALUES ($1, …)[, (…)]` with
    /// EVERY tuple cell a bare placeholder. `Some` lets `execute_bound` build
    /// the RecordBatch straight from the decoded bind values
    /// (`executor::try_insert_bind_direct`); the AST/text routes remain the
    /// fallback when the execute-time gates decline.
    bind_plan: Option<Arc<crate::executor::BindInsertPlan>>,
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

    // Prepared-literal INSERT fast path (perf-w-prepared, extended-protocol
    // shape 1): a ZERO-parameter template whose header the O(prefix)
    // classifier accepts is a literal bulk INSERT prepared once and Executed
    // as-is (some ORMs batch by interpolating literals into one statement and
    // then PREPARE/EXECUTE it). For these we skip every O(statement) cost in
    // this function — the schema probe, the full-statement sqlparser parse,
    // and the rewrite-pipeline no-op probe (each walks a potentially multi-MB
    // text) — and deliberately leave `ast_fast_ok = false`. Each Execute then
    // takes the text route, where `executor::execute`'s pre-parse hook
    // re-classifies the stored SQL (O(prefix)) and runs the values_fast tuple
    // scanner fresh into `exec_insert_prebuilt`, so neither whole-statement
    // parser ever runs. Caching the parsed AST instead would be strictly
    // WORSE: the bind fast path clones the (multi-MB) tree and re-renders it
    // for the leftover-placeholder guard on every Execute. Any shape the
    // scanner later declines (trailing RETURNING / ON CONFLICT, unsupported
    // literals, …) falls through `execute`'s normal double-parse path
    // unchanged. A template WITH parameters never takes this route
    // (`placeholder_count` gate) — the classifier verdict is only trusted for
    // pure-literal statements.
    let literal_fast =
        placeholder_count == 0 && crate::values_fast::classify_literal_insert(sql).is_some();

    // Try to discover the result schema by planning the SQL with typed
    // placeholders. DataFusion 44 doesn't accept `$N`, so we substitute
    // representative literal values matching the inferred types. If the plan
    // fails we fall back to an empty column list — drivers will discover the
    // schema at execute time. (Literal-fast INSERTs skip the probe: it would
    // return no columns anyway, after an O(statement) substitution pass.)
    let columns = if literal_fast {
        Vec::new()
    } else {
        let probe_sql = substitute_for_probe(sql, &param_types);
        probe_schema(sess, &probe_sql).await.unwrap_or_default()
    };

    let schema = StatementSchema {
        param_types,
        param_is_jsonb,
        param_is_uuid,
        columns,
    };

    // Bind-INSERT fast path (perf-w-prepared): parse the template ONCE here
    // and cache the AST so each `Execute` can substitute bind values into a
    // clone and dispatch the `Statement` directly — eliminating the
    // per-`Execute` re-parse of freshly-substituted SQL text.
    //
    // We gate the fast path conservatively. The AST is eligible only when:
    //   1. the template parses to exactly one sqlparser `Statement`;
    //   2. that statement is `INSERT` or a plain `SELECT` (`Query`); and
    //   3. running the executor's string-rewrite pipeline over the TEMPLATE
    //      leaves it byte-for-byte unchanged. The pipeline runs over SQL TEXT
    //      before parse (json operators, casts, lateral, etc.); if it would
    //      rewrite the template, the parsed AST would diverge from what the
    //      text path dispatches, so those keep the text route. We use the EXACT
    //      `rewrite_pipeline_is_noop` check (run-and-compare) rather than the
    //      cheap `needs_rewrite_pipeline` pre-screen, because the latter
    //      over-triggers on the `(` in every `INSERT … VALUES (…)` and would
    //      exclude the entire common INSERT case.
    // Anything not eligible leaves `ast_fast_ok = false` and binds through the
    // unchanged text-substitution path.
    //
    // On top of the AST cache, an AST-fast-eligible parameterized INSERT of
    // the plain all-placeholder shape additionally gets a bind-direct plan
    // (extended-protocol shape 2, see `build_bind_insert_plan`): at Execute,
    // the executor builds the RecordBatch straight from the decoded bind
    // values, skipping AST substitution and the whole AST→rows→batch
    // pipeline. The AST stays cached as the fallback for when the bind-direct
    // execute-time gates decline (open transaction, schema drift, …).
    let (ast, ast_fast_ok, bind_plan) = if literal_fast {
        (None, false, None)
    } else {
        let dialect = PostgreSqlDialect {};
        let ast: Option<Arc<Statement>> = match Parser::parse_sql(&dialect, sql) {
            Ok(mut stmts) if stmts.len() == 1 => Some(Arc::new(stmts.pop().unwrap())),
            _ => None,
        };
        let kind_ok = ast
            .as_ref()
            .map(|s| matches!(s.as_ref(), Statement::Insert(_) | Statement::Query(_)))
            .unwrap_or(false);
        let ast_fast_ok = kind_ok && crate::executor::rewrite_pipeline_is_noop(sess, sql).await;
        let bind_plan = if ast_fast_ok && placeholder_count > 0 {
            ast.as_deref()
                .and_then(|s| build_bind_insert_plan(s, placeholder_count))
                .map(Arc::new)
        } else {
            None
        };
        (ast, ast_fast_ok, bind_plan)
    };

    let handle = StatementHandle::new();
    let entry = PreparedEntry {
        sql: sql.to_owned(),
        placeholder_count,
        schema: schema.clone(),
        ast,
        ast_fast_ok,
        bind_plan,
    };
    sess.state
        .prepared
        .inner
        .write()
        .await
        .insert(handle, entry);
    Ok((handle, schema))
}

/// Precompute the bind-direct INSERT plan (extended-protocol shape 2) for a
/// template of the form `INSERT INTO t [(cols)] VALUES ($1, …)[, (…)]` where
/// EVERY tuple cell is a bare `$N` placeholder — the dominant shape ORMs
/// prepare. Returns `None` for any other shape; those keep the existing
/// AST / text bind routes. Declines:
///
/// * ON CONFLICT / RETURNING / `PARTITION` / after-columns / table alias —
///   the AST path owns those clauses (RETURNING in particular keeps its
///   existing projection behaviour there);
/// * a source that is not a bare `VALUES` body (CTEs, `INSERT … SELECT`,
///   ORDER BY / LIMIT decorations);
/// * any tuple cell that is not a bare placeholder (mixed literal/expression
///   tuples are rare in prepared ORM output; the AST fast path serves them);
/// * quoted column identifiers — the fast paths' case-insensitive name
///   mapping must not be asked to replicate quoted-ident case semantics;
/// * a schema-qualified table name, or a `$N` outside the declared range.
///
/// Everything schema-dependent (column resolution, identity / generated /
/// default / type eligibility) is deliberately NOT checked here — the table
/// can change between Parse and Execute, so those gates re-run per Execute in
/// `executor::try_insert_bind_direct`.
fn build_bind_insert_plan(
    stmt: &Statement,
    placeholder_count: usize,
) -> Option<crate::executor::BindInsertPlan> {
    let ins = match stmt {
        Statement::Insert(i) => i,
        _ => return None,
    };
    if ins.on.is_some()
        || ins.returning.is_some()
        || ins.partitioned.is_some()
        || !ins.after_columns.is_empty()
        || ins.table_alias.is_some()
    {
        return None;
    }
    let source = ins.source.as_ref()?;
    // A bare VALUES body only — no CTE / ORDER BY / LIMIT decoration. (The
    // slow path also reads only `source.body`, but stay conservative.)
    if source.with.is_some() || source.order_by.is_some() || source.limit_clause.is_some() {
        return None;
    }
    let values = match source.body.as_ref() {
        SetExpr::Values(v) => v,
        _ => return None,
    };
    if values.rows.is_empty() {
        return None;
    }
    // Single-part table name only (`name_to_table` rejects qualified names).
    let table = name_to_table(crate::pg_ast::insert_object_name(ins).ok()?).ok()?;
    let mut columns = Vec::with_capacity(ins.columns.len());
    for c in &ins.columns {
        if c.quote_style.is_some() {
            return None;
        }
        columns.push(c.value.clone());
    }
    let arity = if columns.is_empty() {
        values.rows.first()?.len()
    } else {
        columns.len()
    };
    if arity == 0 {
        return None;
    }
    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        if row.len() != arity {
            return None;
        }
        let mut idxs = Vec::with_capacity(arity);
        for cell in row {
            let n = placeholder_index(cell)?;
            if n == 0 || n > placeholder_count {
                return None;
            }
            idxs.push(n - 1);
        }
        rows.push(idxs);
    }
    Some(crate::executor::BindInsertPlan {
        table,
        columns,
        rows,
    })
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

        // ── Array types: `$1::int[]`, `$1::text[]`, etc. ────────────────────
        // Drizzle's `DELETE FROM t WHERE id = ANY($1::int[])` cast form lands
        // here; we surface the matching `List(<elem>)` so ParameterDescription
        // advertises the right `<elem>[]` array OID.  Bare `ARRAY` (no element)
        // is unrepresentable and falls through to TEXT.
        SqlDataType::Array(sqlparser::ast::ArrayElemTypeDef::SquareBracket(elem, _))
        | SqlDataType::Array(sqlparser::ast::ArrayElemTypeDef::AngleBracket(elem))
        | SqlDataType::Array(sqlparser::ast::ArrayElemTypeDef::Parenthesis(elem)) => {
            let elem_dt = cast_data_type_to_arrow(elem)?;
            Some(DataType::List(Arc::new(Field::new("item", elem_dt, true))))
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

/// Recursively walk `expr` looking for function calls (`Expr::Function`) whose
/// positional arguments include a bare `$N` placeholder. For each such
/// placeholder, resolve the parameter type from the function's registered
/// DataFusion [`Signature`][datafusion::logical_expr::Signature] via
/// [`infer_function_call_args`] and pin the slot (only if still TEXT — the
/// authoritative catalog path wins).
///
/// This is the general mechanism behind the ORM-compat fix for
/// `SELECT pg_try_advisory_lock($1)`: parameter-type inference reaches through
/// UDF/function argument positions, so `Describe` reports `bigint` for `$1`
/// instead of falling back to TEXT (which made strict drivers refuse the Bind).
fn walk_expr_for_function_args(sess: &ProjectSession, expr: &Expr, out: &mut [DataType]) {
    match expr {
        Expr::Function(func) => {
            // Collect the positional argument expressions, in order.
            if let FunctionArguments::List(list) = &func.args {
                let pos_args: Vec<&Expr> = list
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })
                    .collect();
                infer_function_call_args(sess, &func.name, &pos_args, out);
                // Recurse into each argument so nested calls
                // (`fn(other_fn($1))`) are also covered.
                for arg in &pos_args {
                    walk_expr_for_function_args(sess, arg, out);
                }
            }
        }
        // Recurse through the common compound-expression shapes so a function
        // call nested inside an operator / cast / paren is still reached.
        Expr::BinaryOp { left, right, .. } => {
            walk_expr_for_function_args(sess, left, out);
            walk_expr_for_function_args(sess, right, out);
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Cast { expr: inner, .. } => {
            walk_expr_for_function_args(sess, inner, out);
        }
        _ => {}
    }
}

/// Resolve placeholder argument types for a single function call.
///
/// Mechanism: look up the registered [`ScalarUDF`][datafusion::logical_expr::ScalarUDF]
/// by lowercased (unqualified) name on the session's `SessionContext` and read
/// its [`Signature`][datafusion::logical_expr::Signature]. For each positional
/// argument that is a bare `$N`, we compute the set of candidate Arrow types
/// that the signature allows at that position and only pin the slot when that
/// set collapses to a single unambiguous type. This handles
/// `Signature::Exact` / `Uniform` directly and `OneOf` by intersecting across
/// its member signatures; ambiguous positions (e.g. one member says Int64 and
/// another says Utf8) are left at the current default.
fn infer_function_call_args(
    sess: &ProjectSession,
    name: &ObjectName,
    pos_args: &[&Expr],
    out: &mut [DataType],
) {
    // Only single-part (unqualified) function names are resolved; the engine
    // registers UDFs under their bare lowercased name.
    if name.0.len() != 1 {
        return;
    }
    let fn_name = name.0[0].id_val().to_ascii_lowercase();

    // Bail early unless at least one argument is a placeholder — avoids a
    // registry lookup for the common no-placeholder call.
    if !pos_args.iter().any(|e| placeholder_index(e).is_some()) {
        return;
    }

    // `udf` is the `FunctionRegistry::udf` lookup; bring the trait into scope.
    use datafusion::execution::FunctionRegistry;
    let udf = match sess.ctx.udf(&fn_name) {
        Ok(u) => u,
        Err(_) => return,
    };

    let arity = pos_args.len();
    for (pos, arg) in pos_args.iter().enumerate() {
        let n = match placeholder_index(arg) {
            Some(n) => n,
            None => continue,
        };
        if let Some(dt) = signature_arg_type(udf.signature(), pos, arity) {
            if let Some(slot) = out.get_mut(n - 1) {
                // Only fill slots still at the TEXT default; never override a
                // type set by the more authoritative catalog/WHERE path.
                if *slot == DataType::Utf8 {
                    *slot = dt;
                }
            }
        }
    }
}

/// Given a function [`Signature`][datafusion::logical_expr::Signature], the
/// zero-based argument position, and the call's arity, return the single Arrow
/// type the signature unambiguously requires at that position — or `None` when
/// the position is unconstrained, variadic, ambiguous (multiple distinct
/// candidate types), or otherwise not safely inferable.
///
/// Only the `Exact` and `Uniform` leaf signatures (and `OneOf` over them) are
/// considered authoritative; everything else (`Variadic*`, `Coercible`,
/// `Numeric`, `String`, `Any`, `UserDefined`, …) is treated as "don't infer".
fn signature_arg_type(
    sig: &datafusion::logical_expr::Signature,
    pos: usize,
    arity: usize,
) -> Option<DataType> {
    use datafusion::logical_expr::TypeSignature;

    /// Candidate type for `pos` from one leaf type-signature, filtered to those
    /// whose declared arity matches the actual call so we never pick a type
    /// from a non-applicable overload (e.g. the `(int4,int4)` 2-arg form when
    /// the call is 1-arg).
    fn leaf_candidate(ts: &TypeSignature, pos: usize, arity: usize) -> Option<DataType> {
        match ts {
            TypeSignature::Exact(types) if types.len() == arity => types.get(pos).cloned(),
            // Uniform(n, valid): every arg shares one of `valid`. Only inferable
            // when there's exactly one valid type AND the declared arg count
            // matches the call.
            TypeSignature::Uniform(n, valid) if *n == arity && valid.len() == 1 => {
                valid.first().cloned()
            }
            _ => None,
        }
    }

    let candidates: Vec<DataType> = match &sig.type_signature {
        TypeSignature::OneOf(members) => members
            .iter()
            .filter_map(|m| leaf_candidate(m, pos, arity))
            .collect(),
        other => leaf_candidate(other, pos, arity).into_iter().collect(),
    };

    let first = candidates.first()?.clone();
    // Fast path: every applicable overload agrees on one exact type.
    if candidates.iter().all(|c| *c == first) {
        return Some(first);
    }

    // The overloads disagree. We still infer *if* they only disagree within a
    // single coercible family — then we pick the widest member, which is how
    // PostgreSQL resolves an untyped placeholder argument (the literal `unknown`
    // is coerced up to the function's widest applicable input). The advisory
    // functions are the motivating case: `pg_try_advisory_lock` is registered
    // as `OneOf([Exact([Int64]), Exact([Int32]), …])`, and PG types `$1` there
    // as `bigint`. Any cross-family disagreement (e.g. Int64 vs Utf8) stays
    // ambiguous and we keep the current default.
    if candidates.iter().all(is_signed_int) {
        return candidates.into_iter().max_by_key(|c| int_width(c));
    }
    if candidates.iter().all(is_float) {
        return candidates.into_iter().max_by_key(|c| float_width(c));
    }
    None
}

fn is_signed_int(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn int_width(dt: &DataType) -> u8 {
    match dt {
        DataType::Int8 => 8,
        DataType::Int16 => 16,
        DataType::Int32 => 32,
        DataType::Int64 => 64,
        _ => 0,
    }
}

fn is_float(dt: &DataType) -> bool {
    matches!(dt, DataType::Float16 | DataType::Float32 | DataType::Float64)
}

fn float_width(dt: &DataType) -> u8 {
    match dt {
        DataType::Float16 => 16,
        DataType::Float32 => 32,
        DataType::Float64 => 64,
        _ => 0,
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

    // Function-argument inference: `SELECT pg_try_advisory_lock($1)` and similar
    // forms where a placeholder sits directly in a registered UDF's argument
    // list. Resolves the parameter type from the function's DataFusion
    // Signature (see `infer_function_call_args`). Only fills slots still at the
    // TEXT default, so the catalog-backed WHERE inference below stays
    // authoritative for placeholders that appear in both positions.
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        walk_expr_for_function_args(sess, expr, out);
    }

    if let Some(pred) = &select.selection {
        walk_pred_with_subqueries(sess, pred, &tables, out, is_jsonb_out, is_uuid_out).await;
        walk_expr_for_function_args(sess, pred, out);
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
        // `<col> = ANY($N)` / `= SOME($N)` — Drizzle's
        // `.where(inArray(t.col, ids))` shape and the universal
        // batch-DELETE-by-id pattern. The placeholder is an array whose
        // element type matches the column. Without this inference the
        // ParameterDescription falls back to TEXT (OID 25), and strict
        // drivers like tokio-postgres reject the Bind of a `Vec<i64>` /
        // `Vec<String>` with WrongType. Also accepts `$N = ANY(col)` (rare,
        // but symmetric — same array typing applies).
        //
        // Both bare-placeholder (`ANY($1)`) and cast-placeholder
        // (`ANY($1::int[])`) forms feed through `pin_array_pair`; the cast
        // form has its inner element type pulled from the cast target via
        // `pin_array_pair_via_cast`.
        Expr::AnyOp {
            left,
            compare_op: _,
            right,
            is_some: _,
        } => {
            pin_array_pair(left, right, tables, out);
            pin_array_pair(right, left, tables, out);
            // Handle `ANY($1::int[])` cast form: the cast's element type is
            // authoritative.  Set slot N to List(<elem>) and the UUID flag
            // (if any) propagates from the cast in pin_cast_placeholder, but
            // for arrays we just fix the data type.
            pin_array_pair_via_cast(left, right, out);
            pin_array_pair_via_cast(right, left, out);
            walk_pred(left, tables, out, is_jsonb_out, is_uuid_out);
            walk_pred(right, tables, out, is_jsonb_out, is_uuid_out);
        }
        // Recurse into nested parentheses or NOT expressions.
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
            walk_pred(inner, tables, out, is_jsonb_out, is_uuid_out);
        }
        _ => {}
    }
}

/// If `placeholder` is a `$N` and `column` resolves to a known column on one
/// of the tables in scope, pin slot `N` to a `List(<col_type>)` so the
/// pgwire ParameterDescription advertises the matching `<col_type>[]` array
/// OID (`int4[]`, `int8[]`, `text[]`, …).  JSONB / UUID flags are
/// intentionally NOT propagated: PG has no `jsonb[]` / `uuid[]` array OIDs
/// we currently surface, and downgrading to BYTEA / TEXT array element is
/// the safer default.
fn pin_array_pair(
    placeholder: &Expr,
    column: &Expr,
    tables: &[(String, arrow_schema::Schema)],
    out: &mut [DataType],
) {
    let n = match placeholder_index(placeholder) {
        Some(n) => n,
        None => return,
    };
    if let Some((dt, _is_jsonb, _is_uuid)) = resolve_column_meta(column, tables) {
        if let Some(slot) = out.get_mut(n - 1) {
            *slot = DataType::List(Arc::new(Field::new("item", dt, true)));
        }
    }
}

/// Pin slot N when the placeholder sits inside an explicit cast:
/// `$N::int[]`, `$N::text[]`, etc.  The cast target is authoritative —
/// it's what the user (or ORM) declared the parameter type to be — so it
/// overrides whatever column-derived type a sibling call to
/// `pin_array_pair` may have set.  No-op when the form doesn't match.
fn pin_array_pair_via_cast(placeholder_side: &Expr, _other_side: &Expr, out: &mut [DataType]) {
    if let Expr::Cast {
        kind:
            CastKind::Cast | CastKind::DoubleColon | CastKind::TryCast | CastKind::SafeCast,
        expr: inner,
        data_type,
        ..
    } = placeholder_side
    {
        let n = match placeholder_index(inner) {
            Some(n) => n,
            None => return,
        };
        if let Some(arrow_dt) = cast_data_type_to_arrow(data_type) {
            if let Some(slot) = out.get_mut(n - 1) {
                *slot = arrow_dt;
            }
        }
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
    // Always produce the substituted text: it is the fast-path's logging /
    // DataFusion-fallthrough SQL and the slow-path's whole input. Cheap
    // (scanner-based, no parse).
    let sql = substitute(&entry.sql, &params)?;

    // Fast path: when the template was AST-fast-path eligible, clone the cached
    // AST and substitute the bind values directly into the placeholder nodes.
    // Any failure (shouldn't happen for eligible statements) degrades silently
    // to `None`, i.e. the unchanged text route.
    let fast_ast: Option<Statement> = if entry.ast_fast_ok {
        match entry.ast.as_ref() {
            Some(arc) => {
                let mut stmt = (**arc).clone();
                match substitute_ast(&mut stmt, &params) {
                    Ok(()) => Some(stmt),
                    Err(e) => {
                        tracing::debug!(error = %e, "AST bind substitution failed; using text path");
                        None
                    }
                }
            }
            None => None,
        }
    } else {
        None
    };

    // Bind-direct INSERT fast path (extended-protocol shape 2): carry the
    // prepare-time plan plus the decoded params through to `execute_bound`,
    // which builds the RecordBatch straight from them. The substituted SQL
    // and (when eligible) the substituted AST are still produced above — they
    // are the fallback when the execute-time gates decline (open transaction,
    // schema drift, unsupported param/column shape).
    let bind_direct = entry
        .bind_plan
        .as_ref()
        .map(|plan| (Arc::clone(plan), params));

    Ok(BoundStatement {
        handle: *handle,
        sql,
        fast_ast,
        bind_direct,
    })
}

pub(crate) async fn execute_bound(
    sess: &ProjectSession,
    bound: BoundStatement,
) -> Result<ExecResult> {
    // Bind-direct INSERT fast path: build the batch straight from the decoded
    // bind values. A `None` decline falls through to the AST / text routes,
    // which reproduce every behaviour (in-tx buffering, canonical errors, …).
    if let Some((plan, params)) = bound.bind_direct.as_ref() {
        if let Some(result) = crate::executor::try_insert_bind_direct(sess, plan, params).await {
            return result;
        }
    }
    if let Some(stmt) = bound.fast_ast {
        // Dispatch the pre-substituted AST without re-parsing. `bound.sql`
        // (the rendered text) is passed for logging and as the DataFusion
        // fallthrough input for a SELECT the fast paths decline.
        return crate::executor::execute_statement(sess, stmt, &bound.sql).await;
    }
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

/// Build the literal `Expr` that a `$N` placeholder substitutes to, for the
/// AST fast path. To guarantee the node is byte-for-byte identical to what the
/// text path would have produced, we render the parameter with the SAME
/// [`render_param`] used by the text substitution and parse that tiny fragment
/// back into an `Expr`. Parsing `SELECT <literal>` and lifting the single
/// projection expression is trivially cheap (a few tokens) versus re-parsing
/// the whole statement, and it inherits `render_param`'s exact quoting / cast /
/// `ARRAY[…]` / `NULL` / float-NaN handling with zero duplication.
fn param_to_expr(p: &ScalarParam) -> Result<Expr> {
    let literal = render_param(p);
    let probe = format!("SELECT {literal}");
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, &probe)
        .map_err(|e| BasinError::internal(format!("param literal parse: {e}")))?;
    let stmt = stmts
        .pop()
        .ok_or_else(|| BasinError::internal("param literal produced no statement".to_string()))?;
    if let Statement::Query(q) = stmt {
        if let SetExpr::Select(select) = q.body.as_ref() {
            if let Some(SelectItem::UnnamedExpr(e)) = select.projection.first() {
                return Ok(e.clone());
            }
        }
    }
    Err(BasinError::internal(
        "param literal did not parse to a projection expr".to_string(),
    ))
}

/// Deep-substitute every `$N` placeholder in a parsed statement with the
/// literal `Expr` for `params[N-1]`, in place. Used by the bind-INSERT fast
/// path so an `Execute` need not re-parse freshly-substituted SQL text.
///
/// Correctness contract: after the walk, NO `Value::Placeholder` may remain
/// anywhere in the statement. If the walker reaches a placeholder it cannot
/// substitute — including one nested in an `Expr` shape this targeted walker
/// doesn't descend into — the leftover-scan at the end returns `Err`, and the
/// caller ([`bind`]) silently falls back to the text route. The fast path can
/// therefore never emit a statement with an unbound placeholder; worst case it
/// declines and the unchanged text path runs.
fn substitute_ast(stmt: &mut Statement, params: &[ScalarParam]) -> Result<()> {
    match stmt {
        Statement::Insert(ins) => {
            if let Some(source) = ins.source.as_mut() {
                subst_query(source, params)?;
            }
        }
        Statement::Query(q) => {
            subst_query(q, params)?;
        }
        // The caller only sets `ast_fast_ok` for INSERT / Query, so this is
        // unreachable in practice; bail to text for anything else.
        _ => {
            return Err(BasinError::internal(
                "substitute_ast: only INSERT/Query supported".to_string(),
            ));
        }
    }
    // Hard guard: any surviving placeholder means we'd dispatch an unbound
    // statement. Refuse so the caller falls back to the text path.
    if stmt_has_placeholder(stmt) {
        return Err(BasinError::internal(
            "substitute_ast: placeholder survived substitution".to_string(),
        ));
    }
    Ok(())
}

fn subst_query(q: &mut Query, params: &[ScalarParam]) -> Result<()> {
    if let Some(with) = q.with.as_mut() {
        for cte in &mut with.cte_tables {
            subst_query(&mut cte.query, params)?;
        }
    }
    subst_setexpr(&mut q.body, params)?;
    if let Some(ob) = q.order_by.as_mut() {
        for obe in ob.ext_exprs_mut() {
            subst_expr(&mut obe.expr, params)?;
        }
    }
    // sqlparser 0.61 folds LIMIT/OFFSET into `limit_clause`. Substitute the
    // common `LIMIT $1` / `OFFSET $1` placeholders via the project's ext
    // accessors; any clause shape they don't expose leaves the placeholder for
    // the leftover guard to catch (→ text fallback).
    if let Some(limit) = q.ext_limit_mut() {
        subst_expr(limit, params)?;
    }
    if let Some(clause) = q.limit_clause.as_mut() {
        match clause {
            sqlparser::ast::LimitClause::LimitOffset {
                offset: Some(off), ..
            } => subst_expr(&mut off.value, params)?,
            sqlparser::ast::LimitClause::OffsetCommaLimit { offset, .. } => {
                subst_expr(offset, params)?
            }
            _ => {}
        }
    }
    Ok(())
}

fn subst_setexpr(body: &mut SetExpr, params: &[ScalarParam]) -> Result<()> {
    match body {
        SetExpr::Select(select) => {
            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(e) => subst_expr(e, params)?,
                    SelectItem::ExprWithAlias { expr, .. } => subst_expr(expr, params)?,
                    _ => {}
                }
            }
            for twj in &mut select.from {
                subst_table_factor(&mut twj.relation, params)?;
                for join in &mut twj.joins {
                    subst_table_factor(&mut join.relation, params)?;
                    if let sqlparser::ast::JoinOperator::Inner(jc)
                    | sqlparser::ast::JoinOperator::LeftOuter(jc)
                    | sqlparser::ast::JoinOperator::RightOuter(jc)
                    | sqlparser::ast::JoinOperator::FullOuter(jc) = &mut join.join_operator
                    {
                        if let sqlparser::ast::JoinConstraint::On(e) = jc {
                            subst_expr(e, params)?;
                        }
                    }
                }
            }
            if let Some(sel) = select.selection.as_mut() {
                subst_expr(sel, params)?;
            }
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &mut select.group_by {
                for e in exprs {
                    subst_expr(e, params)?;
                }
            }
            if let Some(having) = select.having.as_mut() {
                subst_expr(having, params)?;
            }
        }
        SetExpr::Query(q) => subst_query(q, params)?,
        SetExpr::SetOperation { left, right, .. } => {
            subst_setexpr(left, params)?;
            subst_setexpr(right, params)?;
        }
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for e in row {
                    subst_expr(e, params)?;
                }
            }
        }
        // Insert / Update / Delete bodies and Table shorthand: the caller never
        // routes these here (INSERT VALUES go through `Statement::Insert`'s
        // `source`, which is a Query whose body is `Values`). Leave untouched;
        // the leftover-placeholder guard catches anything unexpected.
        _ => {}
    }
    Ok(())
}

fn subst_table_factor(tf: &mut TableFactor, params: &[ScalarParam]) -> Result<()> {
    match tf {
        TableFactor::Derived { subquery, .. } => subst_query(subquery, params),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            subst_table_factor(&mut table_with_joins.relation, params)?;
            for join in &mut table_with_joins.joins {
                subst_table_factor(&mut join.relation, params)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Recursively substitute placeholders inside an `Expr`. When the expr IS a
/// `$N` placeholder, replace the whole node with the literal expr for
/// `params[N-1]`; otherwise descend into every sub-expression that can hold a
/// placeholder.
fn subst_expr(expr: &mut Expr, params: &[ScalarParam]) -> Result<()> {
    if let Some(n) = placeholder_index(expr) {
        let p = params.get(n - 1).ok_or_else(|| {
            BasinError::InvalidSchema(format!("placeholder ${n} has no bound value"))
        })?;
        *expr = param_to_expr(p)?;
        return Ok(());
    }
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            subst_expr(left, params)?;
            subst_expr(right, params)?;
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::Cast { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner) => {
            subst_expr(inner, params)?;
        }
        Expr::InList { expr: e, list, .. } => {
            subst_expr(e, params)?;
            for item in list {
                subst_expr(item, params)?;
            }
        }
        Expr::Between {
            expr: e,
            low,
            high,
            ..
        } => {
            subst_expr(e, params)?;
            subst_expr(low, params)?;
            subst_expr(high, params)?;
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => {
            subst_expr(e, params)?;
            subst_expr(pattern, params)?;
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            subst_expr(left, params)?;
            subst_expr(right, params)?;
        }
        Expr::Function(func) => {
            if let FunctionArguments::List(list) = &mut func.args {
                for arg in &mut list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        subst_expr(e, params)?;
                    }
                }
            }
        }
        Expr::Array(arr) => {
            for e in &mut arr.elem {
                subst_expr(e, params)?;
            }
        }
        Expr::Tuple(items) => {
            for e in items {
                subst_expr(e, params)?;
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                subst_expr(op, params)?;
            }
            for when in conditions {
                subst_expr(&mut when.condition, params)?;
                subst_expr(&mut when.result, params)?;
            }
            if let Some(er) = else_result {
                subst_expr(er, params)?;
            }
        }
        // Any other shape: don't descend. A placeholder hiding inside an
        // un-walked variant is caught by the leftover guard in
        // `substitute_ast`, which forces a fallback to the text path.
        _ => {}
    }
    Ok(())
}

/// Conservative leftover-placeholder detector. Renders the statement back to
/// text and scans for a `$<digit>` token outside string/identifier literals,
/// reusing the same byte scanner as [`scan_placeholders`]. Used as the hard
/// post-substitution guard so the fast path can never dispatch an unbound
/// placeholder.
fn stmt_has_placeholder(stmt: &Statement) -> bool {
    scan_placeholders(&stmt.to_string()).map(|n| n > 0).unwrap_or(true)
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
        ScalarParam::Timestamptz(us) => {
            format!("'{}'::timestamptz", timestamptz_micros_to_literal(*us))
        }
        ScalarParam::Bytea(bytes) => {
            let mut hex = String::with_capacity(bytes.len() * 2 + 4);
            hex.push_str("'\\x");
            for b in bytes {
                hex.push_str(&format!("{:02x}", b));
            }
            hex.push_str("'::bytea");
            hex
        }
        // PG one-dimensional array → `ARRAY[a, b, c]`. The engine's
        // `rewrite_any_array` pass converts `col = ANY(ARRAY[…])` into the
        // `IN (…)` form DataFusion can plan, so binding `Vec<i64>` against
        // `WHERE id = ANY($1)` works end-to-end without touching the
        // planner. Empty arrays render as `ARRAY[]` which PG accepts.
        ScalarParam::Array(elems) => {
            let mut out = String::with_capacity(elems.len() * 4 + 8);
            out.push_str("ARRAY[");
            for (i, e) in elems.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&render_param(e));
            }
            out.push(']');
            out
        }
    }
}

/// Render a TIMESTAMPTZ bind value (microseconds since the Unix epoch, UTC)
/// as the PG-shaped literal body `YYYY-MM-DD HH:MM:SS.ffffff+00` — no quotes,
/// no cast. Shared by [`render_param`] (which wraps it in `'…'::timestamptz`)
/// and the bind-direct cell mapping in `values_fast` (whose Timestamp
/// accumulator parses exactly this form via `dml::parse_timestamp_string`'s
/// `%#z` patterns).
///
/// Out-of-chrono-range values cannot be produced by the router's wire
/// decoders (both validate range with the same chrono check); should one
/// appear anyway, saturate to chrono's representable bounds rather than
/// panic in the render path.
pub(crate) fn timestamptz_micros_to_literal(us: i64) -> String {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(us).unwrap_or(if us < 0 {
        chrono::DateTime::<chrono::Utc>::MIN_UTC
    } else {
        chrono::DateTime::<chrono::Utc>::MAX_UTC
    });
    dt.format("%Y-%m-%d %H:%M:%S%.6f+00").to_string()
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

    fn parse_one(sql: &str) -> Statement {
        let dialect = PostgreSqlDialect {};
        let mut stmts = Parser::parse_sql(&dialect, sql).expect("parses");
        assert_eq!(stmts.len(), 1);
        stmts.pop().unwrap()
    }

    #[test]
    fn bind_plan_single_row_all_placeholders() {
        let stmt = parse_one("INSERT INTO t (a, b, c) VALUES ($1, $2, $3)");
        let plan = build_bind_insert_plan(&stmt, 3).expect("plan");
        assert_eq!(plan.table.as_str(), "t");
        assert_eq!(plan.columns, vec!["a", "b", "c"]);
        assert_eq!(plan.rows, vec![vec![0, 1, 2]]);
    }

    #[test]
    fn bind_plan_multi_row_and_no_column_list() {
        let stmt = parse_one("INSERT INTO t VALUES ($1, $2), ($3, $4)");
        let plan = build_bind_insert_plan(&stmt, 4).expect("plan");
        assert!(plan.columns.is_empty());
        assert_eq!(plan.rows, vec![vec![0, 1], vec![2, 3]]);
        // Repeated placeholder across rows is fine — same param feeds both.
        let stmt = parse_one("INSERT INTO t (a, b) VALUES ($1, $2), ($1, $3)");
        let plan = build_bind_insert_plan(&stmt, 3).expect("plan");
        assert_eq!(plan.rows, vec![vec![0, 1], vec![0, 2]]);
    }

    #[test]
    fn bind_plan_declines_out_of_scope_shapes() {
        for (sql, n) in [
            // Mixed literal cell → decline (AST path serves it).
            ("INSERT INTO t (a, b) VALUES ($1, 'x')", 1),
            // Expression cell.
            ("INSERT INTO t (a) VALUES ($1 + 1)", 1),
            // ON CONFLICT / RETURNING.
            (
                "INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO NOTHING",
                1,
            ),
            ("INSERT INTO t (a) VALUES ($1) RETURNING a", 1),
            // INSERT … SELECT.
            ("INSERT INTO t (a) SELECT $1", 1),
            // Schema-qualified target.
            ("INSERT INTO s.t (a) VALUES ($1)", 1),
            // Quoted column ident → decline (case-fold semantics stay slow).
            ("INSERT INTO t (\"A\") VALUES ($1)", 1),
            // Placeholder out of declared range.
            ("INSERT INTO t (a, b) VALUES ($1, $5)", 2),
            // Ragged rows.
            ("INSERT INTO t (a, b) VALUES ($1, $2), ($3)", 3),
        ] {
            let stmt = parse_one(sql);
            assert!(
                build_bind_insert_plan(&stmt, n).is_none(),
                "must decline: {sql}"
            );
        }
        // Non-INSERT statements never get a plan.
        let stmt = parse_one("SELECT $1");
        assert!(build_bind_insert_plan(&stmt, 1).is_none());
    }

    #[test]
    fn render_timestamptz_param_as_pg_literal() {
        // 2026-01-02 03:04:05.123456 UTC.
        let us = 1_767_323_045_123_456_i64;
        let p = ScalarParam::Timestamptz(us);
        assert_eq!(
            render_param(&p),
            "'2026-01-02 03:04:05.123456+00'::timestamptz"
        );
        // Text substitution carries the same rendering end-to-end…
        let sql = substitute("INSERT INTO t (at) VALUES ($1)", &[p.clone()]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO t (at) VALUES ('2026-01-02 03:04:05.123456+00'::timestamptz)"
        );
        // …and the AST substitution parses the literal to a Cast expr.
        let expr = param_to_expr(&p).expect("param literal must parse");
        assert!(
            matches!(expr, Expr::Cast { .. }),
            "expected a Cast expr, got {expr:?}"
        );
        // The literal body round-trips through the slow path's timestamp
        // parser to the exact micros value (bind-direct relies on this).
        let micros =
            crate::dml::parse_timestamp_string(&timestamptz_micros_to_literal(us)).unwrap();
        assert_eq!(micros, us);
        // Pre-epoch values keep sub-second precision (euclidean rebase).
        let neg = -1_500_000_i64; // 1969-12-31 23:59:58.5 UTC
        assert_eq!(
            timestamptz_micros_to_literal(neg),
            "1969-12-31 23:59:58.500000+00"
        );
    }

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
    fn signature_arg_type_widens_advisory_oneof_to_bigint() {
        use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
        // Mirrors `advisory_lock::advisory_signature()`: OneOf over int4/int8
        // single- and two-arg exact forms. For a 1-arg call, $1 must resolve to
        // Int64 (PG's `bigint`), picking the widest applicable integer overload.
        let sig = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
            ],
            Volatility::Volatile,
        );
        assert_eq!(signature_arg_type(&sig, 0, 1), Some(DataType::Int64));
        // For the 2-arg call, each position also widens int4→int8.
        assert_eq!(signature_arg_type(&sig, 0, 2), Some(DataType::Int64));
        assert_eq!(signature_arg_type(&sig, 1, 2), Some(DataType::Int64));
    }

    #[test]
    fn signature_arg_type_exact_single_overload() {
        use datafusion::logical_expr::{Signature, Volatility};
        // `nextval(text) -> bigint`: a single Exact([Utf8]) overload yields Utf8.
        let sig = Signature::exact(vec![DataType::Utf8], Volatility::Volatile);
        assert_eq!(signature_arg_type(&sig, 0, 1), Some(DataType::Utf8));
        // Arity mismatch (2-arg call against a 1-arg sig) is not inferable.
        assert_eq!(signature_arg_type(&sig, 0, 2), None);
    }

    #[test]
    fn signature_arg_type_bails_on_cross_family_ambiguity() {
        use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
        // Int64 vs Utf8 at the same position is a cross-family disagreement —
        // we must NOT guess; keep the caller's TEXT default.
        let sig = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Volatile,
        );
        assert_eq!(signature_arg_type(&sig, 0, 1), None);
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

    #[test]
    fn substitute_array_int() {
        // Drizzle's `id = ANY($1::int[])` shape; substituted value becomes
        // an ARRAY literal which the `rewrite_any_array` pass then folds
        // into `IN (1, 3)` for DataFusion.
        let out = substitute(
            "DELETE FROM t WHERE id = ANY($1)",
            &[ScalarParam::Array(vec![
                ScalarParam::Int8(1),
                ScalarParam::Int8(3),
            ])],
        )
        .unwrap();
        assert_eq!(out, "DELETE FROM t WHERE id = ANY(ARRAY[1, 3])");
    }

    #[test]
    fn substitute_array_text_escapes() {
        // Text elements respect the same single-quote-doubling rule as
        // scalar Text params, so embedded apostrophes can't break out.
        let out = substitute(
            "SELECT $1",
            &[ScalarParam::Array(vec![
                ScalarParam::Text("a".into()),
                ScalarParam::Text("it's".into()),
            ])],
        )
        .unwrap();
        assert_eq!(out, "SELECT ARRAY['a', 'it''s']");
    }

    #[test]
    fn substitute_array_empty() {
        let out = substitute(
            "SELECT $1",
            &[ScalarParam::Array(vec![])],
        )
        .unwrap();
        assert_eq!(out, "SELECT ARRAY[]");
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
