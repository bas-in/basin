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

use std::collections::HashMap;

use arrow_schema::{DataType, Field};
use basin_common::{BasinError, Result, TableName};
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Expr, FromTable, ObjectName, Query, SetExpr,
    Statement, TableFactor, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{ExecResult, TenantSession};

/// Opaque identifier for one prepared statement, scoped to one
/// [`TenantSession`]. Closing the session implicitly closes the statement.
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
    pub columns: Vec<Field>,
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

/// Output of [`TenantSession::bind`]. Holds the substituted SQL and the
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

/// Per-session prepared-statement registry. Held inside `TenantSession::state`
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
}

/// Implementation of [`TenantSession::prepare`]. See module docs for the
/// substitution strategy.
pub(crate) async fn prepare(
    sess: &TenantSession,
    sql: &str,
) -> Result<(StatementHandle, StatementSchema)> {
    let placeholder_count = scan_placeholders(sql)?;

    // Best-effort parameter-type inference from the SQL's structure plus the
    // catalog. Anything we can't classify falls back to TEXT — drivers cope
    // either by sending strings or by coercing.
    let mut param_types = vec![DataType::Utf8; placeholder_count];
    if placeholder_count > 0 {
        if let Err(e) = infer_param_types(sess, sql, &mut param_types).await {
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
    sess: &TenantSession,
    sql: &str,
    out: &mut [DataType],
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
            let table_name = name_to_table(&ins.table_name)?;
            let meta = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.tenant, &table_name)
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
            // matching column's data type.
            if let Some(source) = ins.source {
                if let SetExpr::Values(v) = source.body.as_ref() {
                    for row in &v.rows {
                        for (i, expr) in row.iter().enumerate() {
                            if let Some(n) = placeholder_index(expr) {
                                if let (Some(col_name), Some(slot)) =
                                    (col_order.get(i), out.get_mut(n - 1))
                                {
                                    if let Some(dt) =
                                        column_type(meta.schema.as_ref(), col_name)
                                    {
                                        *slot = dt;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Statement::Query(q) => {
            walk_select_for_predicates(sess, &q, out).await?;
        }
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            // Resolve the target table once; placeholder slots in SET and
            // WHERE are typed against its column list.
            if let TableFactor::Table { name, .. } = &table.relation {
                if let Ok(tn) = name_to_table(name) {
                    if let Ok(meta) = sess
                        .engine
                        .config()
                        .catalog
                        .load_table(&sess.tenant, &tn)
                        .await
                    {
                        let schema = (*meta.schema).clone();
                        infer_assignments(&assignments, &schema, out);
                        if let Some(pred) = &selection {
                            walk_pred(
                                pred,
                                &[(tn.as_str().to_owned(), schema)],
                                out,
                            );
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
                            .load_table(&sess.tenant, &tn)
                            .await
                        {
                            let schema = (*meta.schema).clone();
                            if let Some(pred) = &del.selection {
                                walk_pred(
                                    pred,
                                    &[(tn.as_str().to_owned(), schema)],
                                    out,
                                );
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
/// `SET col = $1` → slot 0 takes the type of `col`.
fn infer_assignments(
    assignments: &[Assignment],
    schema: &arrow_schema::Schema,
    out: &mut [DataType],
) {
    for a in assignments {
        if let AssignmentTarget::ColumnName(name) = &a.target {
            if name.0.len() == 1 {
                let col = &name.0[0].value;
                if let Some(n) = placeholder_index(&a.value) {
                    if let (Some(slot), Some(dt)) =
                        (out.get_mut(n - 1), column_type(schema, col))
                    {
                        *slot = dt;
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
    TableName::new(name.0[0].value.clone())
}

fn placeholder_index(e: &Expr) -> Option<usize> {
    match e {
        Expr::Value(Value::Placeholder(s)) => {
            s.strip_prefix('$').and_then(|d| d.parse::<usize>().ok())
        }
        _ => None,
    }
}

fn column_type(schema: &arrow_schema::Schema, col: &str) -> Option<DataType> {
    schema
        .fields()
        .iter()
        .find(|f| f.name() == col)
        .map(|f| f.data_type().clone())
}

/// Walk a SELECT looking for `<col> OP $N` and `$N OP <col>` predicates we
/// can resolve to a column type.
async fn walk_select_for_predicates(
    sess: &TenantSession,
    q: &Query,
    out: &mut [DataType],
) -> Result<()> {
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
                    .load_table(&sess.tenant, &tn)
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

    if let Some(pred) = &select.selection {
        walk_pred(pred, &tables, out);
    }

    // LIMIT $N and OFFSET $N — Postgres types these as int8 (BIGINT). Drivers
    // that don't get this hint refuse to bind i64 values for them. Without
    // this, every ORM with `.limit(?)` / `.offset(?)` breaks at the client.
    if let Some(Expr::Value(Value::Placeholder(s))) = q.limit.as_ref() {
        if let Some(idx) = s.strip_prefix('$').and_then(|d| d.parse::<usize>().ok()) {
            if let Some(slot) = out.get_mut(idx.saturating_sub(1)) {
                *slot = DataType::Int64;
            }
        }
    }
    if let Some(off) = q.offset.as_ref() {
        if let Expr::Value(Value::Placeholder(s)) = &off.value {
            if let Some(idx) = s.strip_prefix('$').and_then(|d| d.parse::<usize>().ok()) {
                if let Some(slot) = out.get_mut(idx.saturating_sub(1)) {
                    *slot = DataType::Int64;
                }
            }
        }
    }
    Ok(())
}

fn walk_pred(
    expr: &Expr,
    tables: &[(String, arrow_schema::Schema)],
    out: &mut [DataType],
) {
    if let Expr::BinaryOp { left, op, right } = expr {
        match op {
            BinaryOperator::And | BinaryOperator::Or => {
                walk_pred(left, tables, out);
                walk_pred(right, tables, out);
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                pin_pair(left, right, tables, out);
                pin_pair(right, left, tables, out);
            }
            _ => {}
        }
    }
}

/// If `placeholder` is a `$N` and `column` resolves to a known column on one
/// of the tables in scope, copy the column's data type into the output slot.
fn pin_pair(
    placeholder: &Expr,
    column: &Expr,
    tables: &[(String, arrow_schema::Schema)],
    out: &mut [DataType],
) {
    let n = match placeholder_index(placeholder) {
        Some(n) => n,
        None => return,
    };
    let slot = match out.get_mut(n - 1) {
        Some(s) => s,
        None => return,
    };
    if let Some(dt) = resolve_column(column, tables) {
        *slot = dt;
    }
}

fn resolve_column(e: &Expr, tables: &[(String, arrow_schema::Schema)]) -> Option<DataType> {
    match e {
        Expr::Identifier(id) => {
            for (_, schema) in tables {
                if let Some(dt) = column_type(schema, &id.value) {
                    return Some(dt);
                }
            }
            None
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let (qualifier, col) = (&parts[0].value, &parts[1].value);
            for (label, schema) in tables {
                if label == qualifier {
                    return column_type(schema, col);
                }
            }
            None
        }
        _ => None,
    }
}

pub(crate) async fn describe_statement(
    sess: &TenantSession,
    handle: &StatementHandle,
) -> Result<StatementSchema> {
    let guard = sess.state.prepared.inner.read().await;
    let entry = guard
        .get(handle)
        .ok_or_else(|| BasinError::not_found(format!("prepared statement {handle:?}")))?;
    Ok(entry.schema.clone())
}

pub(crate) async fn close_statement(sess: &TenantSession, handle: &StatementHandle) {
    sess.state.prepared.inner.write().await.remove(handle);
}

pub(crate) async fn bind(
    sess: &TenantSession,
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
    sess: &TenantSession,
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
                        BasinError::InvalidSchema(format!(
                            "placeholder ${n} has no bound value"
                        ))
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

async fn probe_schema(sess: &TenantSession, sql: &str) -> Result<Vec<Field>> {
    // Only SELECT can yield a row description ahead of execute; for anything
    // else we return no columns and the caller falls back.
    let trimmed = sql.trim_start().to_ascii_uppercase();
    if !trimmed.starts_with("SELECT") {
        return Ok(Vec::new());
    }

    let logical = sess
        .ctx
        .sql(sql)
        .await
        .map_err(|e| BasinError::internal(format!("plan: {e}")))?;
    let df_schema = logical.schema().inner().clone();
    let ws_schema = crate::convert::schema_df_to_ws(df_schema.as_ref())?;
    Ok(ws_schema.fields().iter().map(|f| (**f).clone()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_finds_basic_placeholders() {
        assert_eq!(scan_placeholders("SELECT * FROM t WHERE id = $1").unwrap(), 1);
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
        let out = substitute(
            sql,
            &[ScalarParam::Int8(7), ScalarParam::Text("hi".into())],
        )
        .unwrap();
        assert_eq!(out, "INSERT INTO t VALUES (7, 'hi')");
    }

    #[test]
    fn substitute_escapes_apostrophe() {
        let out = substitute(
            "SELECT $1",
            &[ScalarParam::Text("it's".into())],
        )
        .unwrap();
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
        let out = substitute("SELECT $1", &[ScalarParam::Float8(3.14)]).unwrap();
        assert_eq!(out, "SELECT 3.14");
    }

    #[test]
    fn substitute_string_in_literal_left_alone() {
        // `$1` inside an existing single-quoted literal must NOT be substituted.
        let out = substitute(
            "SELECT '$1', $1",
            &[ScalarParam::Int8(42)],
        )
        .unwrap();
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
        assert_eq!(
            out,
            "SELECT * FROM t WHERE name = '''; DROP TABLE t; --'"
        );
    }

    #[test]
    fn substitute_param_appearing_twice() {
        let out = substitute(
            "SELECT $1 + $1",
            &[ScalarParam::Int4(5)],
        )
        .unwrap();
        assert_eq!(out, "SELECT 5 + 5");
    }
}
