//! `ReactorSink` — pre-commit `ChangeEventSink` that runs SQL-bodied
//! reactors on every matching mutation.
//!
//! The catalog row (`basin_catalog::ReactorDef`) carries the body and an
//! optional `WHEN` predicate; both can reference `NEW.col` / `OLD.col` /
//! `TG_OP` / `TG_TABLE_NAME` bind variables. On each `publish`:
//!
//! 1. `Catalog::lookup_reactors_for(tenant, table, op)` returns matching
//!    reactors in registration order.
//! 2. For each reactor:
//!    - Substitute the bind variables into the predicate text and
//!      evaluate it via the engine (`SELECT ((predicate))::BOOLEAN`).
//!    - If the predicate fires (or is absent), substitute into the body
//!      text and execute it through the engine on a fresh tenant
//!      session.
//! 3. Any `Err` short-circuits and propagates up; Tier 0's
//!    `dispatch_pre_commit` rolls back the source mutation. PG
//!    `BEFORE`-trigger semantics.
//!
//! ## Bind-variable substitution
//!
//! Hybrid approach: parse the predicate / body via sqlparser, walk the
//! AST, rewrite every `NEW.col` / `OLD.col` / `TG_OP` / `TG_TABLE_NAME`
//! reference into a literal SQL `Expr` rendered from the matching
//! `ChangeEvent` field. The rewrite is a syntactic AST transform; it
//! never touches identifiers inside string literals (sqlparser already
//! distinguishes those for us). After rewrite, the engine sees
//! a vanilla SQL statement / expression with no remaining bind vars.
//!
//! ## Engine reentry
//!
//! `ReactorSink` holds a `Weak<EngineInner>` and upgrades it on every
//! `publish`. Holding a strong `Arc<EngineInner>` would create a cycle
//! (the engine attaches the sink to itself). On engine drop, the weak
//! reference returns `None` and `publish` reports an internal error
//! indicating a torn-down engine.
//!
//! ## Multi-row mutations
//!
//! Tier 0 emits one `ChangeEvent` per affected row, so `publish` is
//! called N times for an UPDATE that touches N rows. The reactor body
//! also runs N times. Reactor failure mid-row aborts the whole source
//! mutation (Tier 0's `dispatch_pre_commit` returns the first `Err`
//! without writing the replacement parquet); rows already-emitted up to
//! that point are not visible because nothing is committed yet.

use std::sync::{Arc, Weak};

use async_trait::async_trait;
use basin_catalog::{Catalog, ReactorDef};
use basin_common::{BasinError, ChangeEvent, ChangeEventSink, ChangeOp, Result};
use serde_json::Value;
use sqlparser::ast::{Expr, Statement, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::{EngineInner, ExecResult};

/// Pre-commit sink that fires SQL-bodied reactors. Auto-attached by
/// [`crate::Engine::new`] / [`crate::Engine::with_analytical`]; users
/// don't construct it directly.
pub(crate) struct ReactorSink {
    catalog: Arc<dyn Catalog>,
    engine: Weak<EngineInner>,
}

impl ReactorSink {
    pub(crate) fn new(catalog: Arc<dyn Catalog>, engine: Weak<EngineInner>) -> Self {
        Self { catalog, engine }
    }

    /// Open a session as the engine and run one SQL statement against
    /// the tenant of `event`. We use `open_session_as` with the event's
    /// `causation_user` so RLS evaluation inside the reactor body sees
    /// the same principal that triggered the source mutation.
    async fn run_sql(&self, event: &ChangeEvent, sql: &str) -> Result<ExecResult> {
        let inner = self
            .engine
            .upgrade()
            .ok_or_else(|| BasinError::internal("reactor sink: engine has been dropped"))?;
        let engine = crate::Engine { inner };
        let user = event
            .causation_user
            .clone()
            .unwrap_or_else(|| crate::ANONYMOUS_USER.to_string());
        let session = engine.open_session_as(event.tenant, user).await?;
        session.execute(sql).await
    }

    /// Evaluate the (rewritten) predicate by wrapping it in
    /// `SELECT (<expr>) AS r` and reading the single boolean column.
    /// `NULL` collapses to `false` (PG semantics for `WHERE` on NULL).
    async fn eval_predicate(&self, predicate_sql: &str, event: &ChangeEvent) -> Result<bool> {
        let sql = format!("SELECT ({predicate_sql}) AS r");
        let res = self.run_sql(event, &sql).await?;
        match res {
            ExecResult::Rows { batches, .. } => {
                for batch in &batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let arr = batch.column(0);
                    if arr.is_null(0) {
                        return Ok(false);
                    }
                    if let Some(b) = arr.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                        return Ok(b.value(0));
                    }
                    return Err(BasinError::InvalidSchema(format!(
                        "reactor predicate must evaluate to BOOLEAN; got {}",
                        arr.data_type()
                    )));
                }
                // No rows: treat as false.
                Ok(false)
            }
            other => Err(BasinError::internal(format!(
                "reactor predicate did not return rows: {other:?}"
            ))),
        }
    }

    async fn run_reactor_body(&self, reactor: &ReactorDef, event: &ChangeEvent) -> Result<()> {
        let body_sql = rewrite_sql_statement(&reactor.body, event).map_err(|e| {
            BasinError::InvalidSchema(format!("reactor {:?}: body: {e}", reactor.name))
        })?;
        let _ = self.run_sql(event, &body_sql).await?;
        Ok(())
    }
}

#[async_trait]
impl ChangeEventSink for ReactorSink {
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        let reactors = self
            .catalog
            .lookup_reactors_for(&event.tenant, &event.table, event.op)
            .await;
        if reactors.is_empty() {
            return Ok(());
        }
        for reactor in reactors {
            if let Some(predicate) = &reactor.when_predicate {
                let rewritten = rewrite_predicate(predicate, event).map_err(|e| {
                    BasinError::InvalidSchema(format!(
                        "reactor {:?}: WHEN predicate: {e}",
                        reactor.name
                    ))
                })?;
                let matched = self.eval_predicate(&rewritten, event).await?;
                if !matched {
                    continue;
                }
            }
            self.run_reactor_body(&reactor, event).await?;
        }
        Ok(())
    }
}

// --- Bind-variable substitution -------------------------------------------

/// Rewrite a SQL statement (the reactor body), substituting `NEW.col` /
/// `OLD.col` / `TG_OP` / `TG_TABLE_NAME` references with literal values
/// from `event`. Returns the rewritten SQL text.
fn rewrite_sql_statement(sql: &str, event: &ChangeEvent) -> std::result::Result<String, String> {
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse: {e}"))?;
    if stmts.len() != 1 {
        return Err(format!("expected one statement, got {}", stmts.len()));
    }
    let stmt = stmts.first_mut().unwrap();
    rewrite_statement(stmt, event);
    Ok(stmt.to_string())
}

/// Rewrite a SQL expression (the reactor `WHEN` predicate). Uses the
/// same substitution rules as the body.
fn rewrite_predicate(sql: &str, event: &ChangeEvent) -> std::result::Result<String, String> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| format!("parse: {e}"))?;
    let mut expr = parser.parse_expr().map_err(|e| format!("parse: {e}"))?;
    rewrite_expr(&mut expr, event);
    Ok(expr.to_string())
}

/// Walk a `Statement` and rewrite every `Expr` it contains.
fn rewrite_statement(stmt: &mut Statement, event: &ChangeEvent) {
    match stmt {
        Statement::Insert(ins) => {
            if let Some(source) = ins.source.as_mut() {
                rewrite_query(source, event);
            }
        }
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for a in assignments.iter_mut() {
                rewrite_expr(&mut a.value, event);
            }
            if let Some(sel) = selection.as_mut() {
                rewrite_expr(sel, event);
            }
        }
        Statement::Delete(del) => {
            if let Some(sel) = del.selection.as_mut() {
                rewrite_expr(sel, event);
            }
        }
        Statement::Query(q) => {
            rewrite_query(q.as_mut(), event);
        }
        _ => {
            // Other statement kinds aren't accepted at registration; if
            // we ever reach here it means the catalog let something
            // through unchecked. Leave the AST untouched and let the
            // engine reject it.
        }
    }
}

fn rewrite_query(query: &mut sqlparser::ast::Query, event: &ChangeEvent) {
    use sqlparser::ast::SetExpr;
    match query.body.as_mut() {
        SetExpr::Select(sel) => {
            for proj in sel.projection.iter_mut() {
                use sqlparser::ast::SelectItem;
                match proj {
                    SelectItem::UnnamedExpr(e) => rewrite_expr(e, event),
                    SelectItem::ExprWithAlias { expr, .. } => rewrite_expr(expr, event),
                    _ => {}
                }
            }
            if let Some(sel_expr) = sel.selection.as_mut() {
                rewrite_expr(sel_expr, event);
            }
            if let Some(having) = sel.having.as_mut() {
                rewrite_expr(having, event);
            }
        }
        SetExpr::Values(vals) => {
            for row in vals.rows.iter_mut() {
                for e in row.iter_mut() {
                    rewrite_expr(e, event);
                }
            }
        }
        SetExpr::Query(inner) => rewrite_query(inner.as_mut(), event),
        _ => {}
    }
}

/// Recursively rewrite a single `Expr`, replacing `NEW.col` / `OLD.col`
/// / `TG_OP` / `TG_TABLE_NAME` references with literal values from
/// `event`. Anything that doesn't match a bind variable is left as-is.
fn rewrite_expr(expr: &mut Expr, event: &ChangeEvent) {
    if let Some(replacement) = match_bind_variable(expr, event) {
        *expr = replacement;
        return;
    }
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(left, event);
            rewrite_expr(right, event);
        }
        Expr::UnaryOp { expr, .. } => rewrite_expr(expr, event),
        Expr::Nested(inner) => rewrite_expr(inner, event),
        Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e) => rewrite_expr(e, event),
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_expr(expr, event);
            rewrite_expr(low, event);
            rewrite_expr(high, event);
        }
        Expr::InList { expr, list, .. } => {
            rewrite_expr(expr, event);
            for e in list.iter_mut() {
                rewrite_expr(e, event);
            }
        }
        Expr::Cast { expr, .. } => rewrite_expr(expr, event),
        Expr::Function(func) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            if let FunctionArguments::List(args) = &mut func.args {
                for a in args.args.iter_mut() {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
                        rewrite_expr(e, event);
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(o) = operand.as_mut() {
                rewrite_expr(o, event);
            }
            for c in conditions.iter_mut() {
                rewrite_expr(c, event);
            }
            for r in results.iter_mut() {
                rewrite_expr(r, event);
            }
            if let Some(else_) = else_result.as_mut() {
                rewrite_expr(else_, event);
            }
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_expr(expr, event);
            rewrite_expr(pattern, event);
        }
        _ => {}
    }
}

/// If `expr` is a `NEW.col` / `OLD.col` / `TG_OP` / `TG_TABLE_NAME`
/// reference, return the rendered SQL literal that replaces it. Otherwise
/// `None`. The match is purely structural — bind variables nested inside
/// other expressions are handled by the recursive walk in
/// [`rewrite_expr`].
fn match_bind_variable(expr: &Expr, event: &ChangeEvent) -> Option<Expr> {
    match expr {
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => {
            let head = idents[0].value.to_ascii_uppercase();
            let col = idents[1].value.as_str();
            match head.as_str() {
                "NEW" => Some(json_field_to_expr(event.after.as_ref(), col)),
                "OLD" => Some(json_field_to_expr(event.before.as_ref(), col)),
                _ => None,
            }
        }
        Expr::Identifier(ident) => {
            let v = ident.value.to_ascii_uppercase();
            match v.as_str() {
                "TG_OP" => Some(string_lit(op_text(event.op))),
                "TG_TABLE_NAME" => Some(string_lit(event.table.as_str())),
                _ => None,
            }
        }
        _ => None,
    }
}

fn op_text(op: ChangeOp) -> &'static str {
    match op {
        ChangeOp::Insert => "insert",
        ChangeOp::Update => "update",
        ChangeOp::Delete => "delete",
    }
}

/// Render a JSON `Value` as a SQL `Expr` literal. Numbers, strings, and
/// booleans render as their natural SQL form; missing fields and JSON
/// `null` render as `NULL`.
fn json_field_to_expr(payload: Option<&Value>, col: &str) -> Expr {
    let Some(payload) = payload else {
        return null_expr();
    };
    let Some(field) = payload.get(col) else {
        return null_expr();
    };
    json_value_to_expr(field)
}

fn json_value_to_expr(v: &Value) -> Expr {
    match v {
        Value::Null => null_expr(),
        Value::Bool(b) => Expr::Value(SqlValue::Boolean(*b)),
        Value::Number(n) => {
            // Numbers render as their canonical JSON text; sqlparser
            // accepts that as `Number(repr, false)`.
            Expr::Value(SqlValue::Number(n.to_string(), false))
        }
        Value::String(s) => string_lit(s),
        // Compound JSON values (arrays, objects) render as their
        // canonical JSON serialised text. The engine sees a string
        // literal; a reactor that wants to feed an array column would
        // need to cast it explicitly. v0.1 scope.
        other => string_lit(&other.to_string()),
    }
}

fn null_expr() -> Expr {
    Expr::Value(SqlValue::Null)
}

fn string_lit(s: &str) -> Expr {
    Expr::Value(SqlValue::SingleQuotedString(s.to_string()))
}
