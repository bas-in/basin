//! `CREATE/DROP PROCEDURE` and `CALL` SQL surface (Phase 5.11.F).
//!
//! Three statement shapes route through this module:
//!
//! 1. `CREATE PROCEDURE <name>(<args>) LANGUAGE sql AS $$ stmt1; stmt2; … $$`
//!    — register a `LANGUAGE sql` procedure. sqlparser 0.52's
//!    `Statement::CreateProcedure` parses only the T-SQL `AS BEGIN … END`
//!    shape, so we recognise the PG-style form via the textual
//!    pre-screen [`match_create_procedure`].
//!
//! 2. `CALL <name>(<args>)` — sqlparser parses this natively as
//!    `Statement::Call(Function)`. We resolve the procedure from the
//!    catalog, substitute call-site arguments into each body statement,
//!    and run the substituted statements in order through the
//!    standard engine entry point.
//!
//! 3. `DROP PROCEDURE [IF EXISTS] <name>(<args>) [CASCADE | RESTRICT]`
//!    — sqlparser parses this natively as
//!    `Statement::DropProcedure { proc_desc, … }`. The argument list
//!    from the AST is accepted but not used to disambiguate; v0.1
//!    forbids per-project overloading so `(project, name)` is the unique
//!    key.
//!
//! Transactional semantics (v0.1): **best-effort sequential**. Each
//! body statement is executed via the existing engine path; if a
//! mid-statement error occurs, **prior statements remain committed**
//! (no rollback). This will tighten when single-shard transactions
//! land per Phase 5; until then, callers writing destructive
//! procedures must design for partial-completion recovery (idempotent
//! statements, explicit progress flags, etc.).
//!
//! Out of scope per ADR 0012 / TASK.md 5.11.F:
//!
//! * `LANGUAGE plpgsql` — rejected with SQLSTATE 0A000.
//! * Nested `CALL` inside a procedure body — rejected at registration.
//! * Control flow (`IF`, `LOOP`, variables) — needs the PL/pgSQL
//!   interpreter Basin explicitly skips.

use std::collections::HashMap;
use crate::pg_ast::ObjectNamePartExt;
use std::sync::Arc;

use basin_catalog::{Catalog, SqlArgType, SqlFunctionArg, SqlProcedureDef};
use basin_common::{BasinError, Result};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, FunctionDesc, ObjectName,
    SetExpr, Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::{ExecResult, ProjectSession};

/// Execute a parsed `CREATE PROCEDURE` from the textual pre-screen
/// [`match_create_procedure`]. The catalog stores the body verbatim
/// after [`SqlProcedureDef`] validation; the engine reparses on every
/// `CALL`.
pub(crate) async fn exec_create_procedure(
    sess: &ProjectSession,
    name: &str,
    args: Vec<SqlFunctionArg>,
    body: &str,
) -> Result<ExecResult> {
    let def = SqlProcedureDef {
        project: sess.project,
        name: name.to_string(),
        args,
        body: body.to_string(),
    };
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    if catalog.lookup_procedure(&sess.project, name).await.is_some() {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE PROCEDURE: procedure {name:?} already exists; \
             drop it first (v0.1 has no CREATE OR REPLACE PROCEDURE)"
        )));
    }
    catalog.register_procedure(def).await?;
    Ok(ExecResult::Empty {
        tag: "CREATE PROCEDURE".into(),
    })
}

/// Execute `DROP PROCEDURE [IF EXISTS] <name>(<args>)`. The argument
/// list from the AST is accepted (and parsed) but not used to
/// disambiguate; v0.1 forbids overloading per project so
/// `(project, name)` is the unique key.
pub(crate) async fn exec_drop_procedure(
    sess: &ProjectSession,
    if_exists: bool,
    proc_desc: Vec<FunctionDesc>,
) -> Result<ExecResult> {
    if proc_desc.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DROP PROCEDURE: at least one procedure name is required".into(),
        ));
    }
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    for desc in proc_desc {
        let name = single_part_object_name(&desc.name)?;
        match catalog.drop_procedure(&sess.project, &name).await {
            Ok(()) => {}
            Err(BasinError::NotFound(_)) if if_exists => {}
            Err(BasinError::NotFound(_)) => {
                return Err(BasinError::not_found(format!(
                    "procedure {name:?} does not exist"
                )));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP PROCEDURE".into(),
    })
}

/// Execute `CALL <name>(<args>)`. Resolves the procedure from the
/// per-project catalog, substitutes the call-site arguments into each
/// body statement (via `Expr::Identifier(arg_name) → call-site Expr`,
/// the same mechanism `sql_functions.rs` uses for scalar UDFs), and
/// runs the substituted statements in order through the standard
/// engine entry point.
///
/// **Best-effort sequential**: if a mid-statement error occurs, prior
/// statements remain committed (until single-shard transactions land
/// per Phase 5). The return tag carries the count of statements that
/// ran successfully so callers can distinguish full from partial
/// completion.
pub(crate) async fn exec_call(sess: &ProjectSession, call: Function) -> Result<ExecResult> {
    let proc_name = single_part_object_name(&call.name)?;

    // Reject the same shape modifiers the function inliner rejects
    // for scalar UDF calls — none make sense on a procedure.
    if call.over.is_some() || call.filter.is_some() || !call.within_group.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "CALL {proc_name}: OVER / FILTER / WITHIN GROUP not permitted on procedure call"
        )));
    }

    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let def = catalog
        .lookup_procedure(&sess.project, &proc_name)
        .await
        .ok_or_else(|| BasinError::not_found(format!("procedure {proc_name:?} does not exist")))?;

    // Extract call-site arguments. Wildcard / qualified-wildcard arg
    // forms are rejected — only named or positional expressions can be
    // substituted into a body.
    let call_args: Vec<FunctionArg> = match &call.args {
        FunctionArguments::List(list) => list.args.clone(),
        FunctionArguments::None => Vec::new(),
        FunctionArguments::Subquery(_) => {
            return Err(BasinError::InvalidSchema(format!(
                "CALL {proc_name}: subquery argument form is not supported"
            )));
        }
    };
    if call_args.len() != def.args.len() {
        return Err(BasinError::InvalidSchema(format!(
            "CALL {proc_name}: expected {} argument(s), got {}",
            def.args.len(),
            call_args.len()
        )));
    }

    // Build the param-name → call-site-Expr substitution map.
    let mut subs: HashMap<String, Expr> = HashMap::with_capacity(def.args.len());
    for (param, call_arg) in def.args.iter().zip(call_args.iter()) {
        let expr = function_arg_to_expr(call_arg, &proc_name)?;
        subs.insert(param.name.to_ascii_lowercase(), expr);
    }

    // Reparse the body. The catalog re-validated this at registration,
    // so a parse failure here is an internal invariant violation — but
    // we surface it as InvalidSchema rather than panic so a
    // catalog-driver-mismatch shows up cleanly.
    let dialect = PostgreSqlDialect {};
    let mut body_stmts = Parser::parse_sql(&dialect, &def.body).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "CALL {proc_name}: stored body fails to reparse: {e}"
        ))
    })?;

    let mut ran = 0usize;
    for stmt in body_stmts.iter_mut() {
        substitute_args_in_statement(stmt, &subs)?;
        let stmt_sql = stmt.to_string();
        // Recurse through the public engine entry point so each
        // substituted statement gets the full pipeline (RLS rewrites,
        // cost check, sequence rewrite, function inlining, analytical
        // routing, …). v0.1 has no transaction wrapper here — see the
        // module doc-comment for the documented "best-effort
        // sequential" semantics. `Box::pin` breaks the async recursion
        // through `executor::execute` (same pattern as
        // [`cv_ddl::exec_create_materialized_view`]); without it rustc
        // would have to size an infinite-recursion future.
        Box::pin(crate::executor::execute(sess, &stmt_sql))
            .await
            .map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "CALL {proc_name}: statement #{} failed (after {ran} successful \
                     statement(s); prior statements remain committed in v0.1): {e}",
                    ran + 1,
                ))
            })?;
        ran += 1;
    }

    Ok(ExecResult::Empty {
        tag: format!("CALL {ran}"),
    })
}

/// Recognise `CREATE PROCEDURE <name> [(<args>)] LANGUAGE <lang> AS
/// $$ <body> $$ [;]` and `CREATE PROCEDURE <name> [(<args>)] AS $$
/// <body> $$ LANGUAGE <lang> [;]`. Returns `(name, args, body)` on a
/// match, `None` when the statement is something else, and an error
/// when the leading `CREATE PROCEDURE` is recognised but the rest is
/// malformed (so the caller can surface a clean diagnostic instead of
/// falling through to sqlparser's generic "unexpected token").
///
/// sqlparser 0.52 only parses the T-SQL `AS BEGIN … END` shape (see
/// `parse_create_procedure`); we recognise the PG-style form before
/// sqlparser sees the SQL. The matcher is deliberately strict — we
/// do not accept a missing `LANGUAGE` clause (the engine has to know
/// which language to refuse for non-`sql` cases) and we do not accept
/// a `RETURNS` clause (procedures don't return).
pub(crate) fn match_create_procedure(
    sql: &str,
) -> Result<Option<(String, Vec<SqlFunctionArg>, String)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "CREATE") {
        return Ok(None);
    }
    let after_create = skip_word(trimmed).trim_start();
    // `OR REPLACE PROCEDURE` — we recognise the prefix so a follow-up
    // phase can opt in; v0.1 does not allow it (no `CREATE OR REPLACE
    // PROCEDURE`), but recognising it lets us reject cleanly rather
    // than falling through to sqlparser's parser.
    let (or_replace, after_or_replace) = if starts_with_kw(after_create, "OR") {
        let after_or = skip_word(after_create).trim_start();
        if !starts_with_kw(after_or, "REPLACE") {
            return Ok(None);
        }
        (true, skip_word(after_or).trim_start())
    } else {
        (false, after_create)
    };
    if !starts_with_kw(after_or_replace, "PROCEDURE") {
        return Ok(None);
    }
    if or_replace {
        return Err(BasinError::InvalidSchema(
            "CREATE OR REPLACE PROCEDURE is not supported in v0.1; \
             DROP PROCEDURE first then re-create"
                .into(),
        ));
    }
    let after_proc = skip_word(after_or_replace).trim_start();
    let (name, after_name) = read_simple_identifier(after_proc)?;
    let after_name = after_name.trim_start();

    // Optional argument list. We tolerate both `()` (no args) and a
    // missing parenthesis block (also "no args" — PG accepts both).
    let (args, after_args) = if after_name.starts_with('(') {
        let (inside, rest) = take_paren_block(after_name)?;
        (parse_arg_list(&inside, &name)?, rest.trim_start())
    } else {
        (Vec::new(), after_name)
    };

    // Now scan for `LANGUAGE <ident>` and `AS $$ <body> $$` in either
    // order. Any other clause (`SECURITY DEFINER`, `SET …`,
    // `RETURNS …`) is rejected for v0.1.
    let mut cursor = after_args;
    let mut language: Option<String> = None;
    let mut body: Option<String> = None;
    loop {
        let c = cursor.trim_start();
        if c.is_empty() {
            break;
        }
        if starts_with_kw(c, "LANGUAGE") {
            let after_kw = skip_word(c).trim_start();
            let (lang, rest) = read_simple_identifier(after_kw)?;
            language = Some(lang);
            cursor = rest;
            continue;
        }
        if starts_with_kw(c, "AS") {
            let after_as = skip_word(c).trim_start();
            let (b, rest) = read_dollar_quoted_or_string(after_as)?;
            body = Some(b);
            cursor = rest;
            continue;
        }
        if starts_with_kw(c, "RETURNS") {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE PROCEDURE {name}: RETURNS clause not allowed on a procedure \
                 (use CREATE FUNCTION for value-returning callables)"
            )));
        }
        return Err(BasinError::InvalidSchema(format!(
            "CREATE PROCEDURE {name}: unexpected clause {:?}",
            c.chars().take(32).collect::<String>()
        )));
    }

    let lang = language.ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "CREATE PROCEDURE {name}: LANGUAGE clause is required"
        ))
    })?;
    if !lang.eq_ignore_ascii_case("sql") {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE PROCEDURE {name}: only LANGUAGE sql is supported \
             (got LANGUAGE {lang:?}); SQLSTATE 0A000 feature_not_supported. \
             PL/pgSQL is out of scope per docs/decisions/0012-change-event-primitive.md"
        )));
    }
    let body = body.ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "CREATE PROCEDURE {name}: missing body (`AS $$ … $$` or `AS '…'`)"
        ))
    })?;

    Ok(Some((name, args, body)))
}

/// Parse the parenthesised argument list of a `CREATE PROCEDURE`
/// declaration. We re-use the same scalar type set as
/// [`crate::function_ddl`] so the two surfaces accept the same
/// declarations and reject the same. Returns
/// `Vec<SqlFunctionArg>`.
fn parse_arg_list(inside: &str, proc_name: &str) -> Result<Vec<SqlFunctionArg>> {
    let s = inside.trim();
    if s.is_empty() {
        return Ok(Vec::new());
    }
    // Wrap in a synthetic CREATE FUNCTION so sqlparser does the heavy
    // lifting — keeps the type-token logic in one place
    // (`function_ddl::sql_data_type_to_arg`-equivalent here).
    let synth = format!("CREATE FUNCTION __tmp({s}) RETURNS BIGINT LANGUAGE sql AS $$ SELECT 1 $$");
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, &synth).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "CREATE PROCEDURE {proc_name}: argument list does not parse: {e}"
        ))
    })?;
    let stmt = stmts.pop().ok_or_else(|| {
        BasinError::internal(format!(
            "CREATE PROCEDURE {proc_name}: empty parse result for argument list"
        ))
    })?;
    let parsed_args = match stmt {
        Statement::CreateFunction { args, .. } => args.unwrap_or_default(),
        _ => {
            return Err(BasinError::internal(format!(
                "CREATE PROCEDURE {proc_name}: synth parse returned wrong shape"
            )))
        }
    };
    let mut out: Vec<SqlFunctionArg> = Vec::with_capacity(parsed_args.len());
    for (idx, a) in parsed_args.iter().enumerate() {
        let aname = a.name.as_ref().map(|i| i.value.clone()).ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "CREATE PROCEDURE {proc_name}: argument #{idx} is unnamed; \
                     all arguments must be named in v0.1"
            ))
        })?;
        let dt = sql_data_type_to_arg(&a.data_type).map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CREATE PROCEDURE {proc_name}: arg {aname:?} type unsupported ({e})"
            ))
        })?;
        out.push(SqlFunctionArg {
            name: aname,
            data_type: dt,
        });
    }
    Ok(out)
}

/// Map a sqlparser `DataType` to a catalog `SqlArgType`. Mirrors the
/// equivalent helper in `function_ddl.rs`; kept here as a private
/// duplicate to avoid leaking that crate-private function across module
/// boundaries.
fn sql_data_type_to_arg(dt: &sqlparser::ast::DataType) -> Result<SqlArgType> {
    use sqlparser::ast::{DataType as SqlDataType, TimezoneInfo};
    match dt {
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => Ok(SqlArgType::Int),
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => Ok(SqlArgType::BigInt),
        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::String(_) => Ok(SqlArgType::Text),
        SqlDataType::Boolean | SqlDataType::Bool => Ok(SqlArgType::Boolean),
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float(_) => Ok(SqlArgType::Double),
        SqlDataType::Bytea => Ok(SqlArgType::Bytea),
        SqlDataType::Date => Ok(SqlArgType::Date),
        SqlDataType::Timestamp(_, tz_info) => match tz_info {
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => Ok(SqlArgType::TimestampTz),
            _ => Ok(SqlArgType::Timestamp),
        },
        other => Err(BasinError::InvalidSchema(format!(
            "unsupported procedure arg type: {other}"
        ))),
    }
}

// --- Argument substitution ------------------------------------------------

/// Substitute `Expr::Identifier(arg_name)` references with the
/// call-site expression for every statement kind a procedure body
/// can carry. Mirrors `sql_functions::substitute_args_in_query` but
/// covers the wider statement set (INSERT / UPDATE / DELETE / CREATE
/// TABLE / DROP TABLE / SELECT).
fn substitute_args_in_statement(stmt: &mut Statement, subs: &HashMap<String, Expr>) -> Result<()> {
    match stmt {
        Statement::Query(q) => substitute_args_in_query(q, subs),
        Statement::Insert(ins) => {
            if let Some(source) = ins.source.as_mut() {
                substitute_args_in_query(source, subs)?;
            }
            // INSERT … RETURNING expressions live on the AST; if a
            // future engine version respects RETURNING for INSERT we
            // need to walk it too. Today we leave it alone — the
            // engine ignores RETURNING for INSERT (covered by
            // existing tests).
            Ok(())
        }
        Statement::Update {
            assignments,
            from,
            selection,
            returning,
            ..
        } => {
            for assn in assignments.iter_mut() {
                substitute_args_in_expr(&mut assn.value, subs)?;
            }
            // `from` is a TableWithJoins; we only need to walk its
            // ON-clause expressions if the procedure body's UPDATE
            // uses one. Keeping the surface narrow for v0.1 — the
            // common case (`UPDATE t SET … WHERE …`) is fully covered.
            let _ = from;
            if let Some(sel) = selection.as_mut() {
                substitute_args_in_expr(sel, subs)?;
            }
            if let Some(items) = returning.as_mut() {
                for item in items.iter_mut() {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(e)
                    | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } = item
                    {
                        substitute_args_in_expr(e, subs)?;
                    }
                }
            }
            Ok(())
        }
        Statement::Delete(del) => {
            if let Some(sel) = del.selection.as_mut() {
                substitute_args_in_expr(sel, subs)?;
            }
            Ok(())
        }
        // CREATE TABLE / DROP TABLE — no expression slots to
        // substitute. We allow them in the body (per the catalog
        // whitelist) so a procedure can stage / retire a table, but
        // there's nothing to rewrite.
        Statement::CreateTable(_) | Statement::Drop { .. } => Ok(()),
        // The catalog validator rejects everything else; this arm is
        // a defensive no-op for any kind that slipped through.
        _ => Ok(()),
    }
}

fn substitute_args_in_query(
    query: &mut sqlparser::ast::Query,
    subs: &HashMap<String, Expr>,
) -> Result<()> {
    substitute_args_in_set_expr(&mut query.body, subs)?;
    if let Some(limit) = query.limit.as_mut() {
        substitute_args_in_expr(limit, subs)?;
    }
    Ok(())
}

fn substitute_args_in_set_expr(set: &mut SetExpr, subs: &HashMap<String, Expr>) -> Result<()> {
    match set {
        SetExpr::Select(sel) => {
            for item in sel.projection.iter_mut() {
                if let sqlparser::ast::SelectItem::UnnamedExpr(e)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } = item
                {
                    substitute_args_in_expr(e, subs)?;
                }
            }
            if let Some(sel_expr) = sel.selection.as_mut() {
                substitute_args_in_expr(sel_expr, subs)?;
            }
            if let Some(having) = sel.having.as_mut() {
                substitute_args_in_expr(having, subs)?;
            }
            Ok(())
        }
        SetExpr::Query(q) => substitute_args_in_query(q, subs),
        SetExpr::SetOperation { left, right, .. } => {
            substitute_args_in_set_expr(left, subs)?;
            substitute_args_in_set_expr(right, subs)
        }
        SetExpr::Values(vals) => {
            for row in vals.rows.iter_mut() {
                for e in row.iter_mut() {
                    substitute_args_in_expr(e, subs)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn substitute_args_in_expr(expr: &mut Expr, subs: &HashMap<String, Expr>) -> Result<()> {
    if let Expr::Identifier(id) = expr {
        if let Some(replacement) = subs.get(&id.value.to_ascii_lowercase()) {
            *expr = wrap_paren(replacement.clone());
            return Ok(());
        }
    }
    walk_expr_children(expr, subs)
}

fn walk_expr_children(expr: &mut Expr, subs: &HashMap<String, Expr>) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            substitute_args_in_expr(left, subs)?;
            substitute_args_in_expr(right, subs)
        }
        Expr::UnaryOp { expr: inner, .. } => substitute_args_in_expr(inner, subs),
        Expr::Cast { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => substitute_args_in_expr(inner, subs),
        Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
            substitute_args_in_expr(a, subs)?;
            substitute_args_in_expr(b, subs)
        }
        Expr::InList { expr: e, list, .. } => {
            substitute_args_in_expr(e, subs)?;
            for item in list.iter_mut() {
                substitute_args_in_expr(item, subs)?;
            }
            Ok(())
        }
        Expr::InSubquery {
            expr: e, subquery, ..
        } => {
            substitute_args_in_expr(e, subs)?;
            substitute_args_in_query(subquery, subs)
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            substitute_args_in_expr(e, subs)?;
            substitute_args_in_expr(low, subs)?;
            substitute_args_in_expr(high, subs)
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        }
        | Expr::SimilarTo {
            expr: e, pattern, ..
        } => {
            substitute_args_in_expr(e, subs)?;
            substitute_args_in_expr(pattern, subs)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand.as_mut() {
                substitute_args_in_expr(op, subs)?;
            }
            for c in conditions.iter_mut() {
                substitute_args_in_expr(&mut c.condition, subs)?;
                substitute_args_in_expr(&mut c.result, subs)?;
            }
            if let Some(er) = else_result.as_mut() {
                substitute_args_in_expr(er, subs)?;
            }
            Ok(())
        }
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => substitute_args_in_query(q, subs),
        Expr::Function(func) => {
            if let FunctionArguments::List(list) = &mut func.args {
                for arg in list.args.iter_mut() {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        substitute_args_in_expr(e, subs)?;
                    }
                }
            }
            Ok(())
        }
        // Identifiers, literals, wildcards — leaves; the identifier
        // case is handled at the top of `substitute_args_in_expr`.
        _ => Ok(()),
    }
}

/// Wrap `e` in `Expr::Nested` so the resulting SQL parenthesises the
/// substitution, matching `sql_functions::wrap_paren`. Avoids
/// precedence surprises after substituting a complex expression into
/// a position that was an identifier in the source body.
fn wrap_paren(e: Expr) -> Expr {
    match e {
        Expr::Nested(_)
        | Expr::Subquery(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::Function(_) => e,
        other => Expr::Nested(Box::new(other)),
    }
}

fn function_arg_to_expr(arg: &FunctionArg, proc_name: &str) -> Result<Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => Ok(e.clone()),
        _ => Err(BasinError::InvalidSchema(format!(
            "CALL {proc_name}: wildcard / qualified-wildcard argument form not supported"
        ))),
    }
}

fn single_part_object_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified procedure names not supported in v0.1: {name}"
        )));
    }
    Ok(name.0[0].id_val().clone())
}

// --- Lexer helpers (mirror function_ddl.rs / type_ddl.rs) ----------------

fn starts_with_kw(s: &str, kw: &str) -> bool {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let kw = kw.as_bytes();
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    if bytes.len() == kw.len() {
        return true;
    }
    let next = bytes[kw.len()];
    !(next.is_ascii_alphanumeric() || next == b'_')
}

fn skip_word(s: &str) -> &str {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    &s[i..]
}

fn read_simple_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidIdent(
            "expected an identifier, got end of statement".into(),
        ));
    }
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return Err(BasinError::InvalidIdent(format!(
            "expected an identifier at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Ok((s[..i].to_string(), &s[i..]))
}

/// `s` must start with `(`. Returns `(inside, rest)` where `inside`
/// is the parenthesised body without the surrounding parens. Tracks
/// nested parens and skips simple string literals.
fn take_paren_block(s: &str) -> Result<(String, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'(' {
        return Err(BasinError::InvalidSchema(
            "expected '(' at procedure argument list".into(),
        ));
    }
    let mut depth: i32 = 1;
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((s[1..i].to_string(), &s[i + 1..]));
                }
            }
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(BasinError::InvalidSchema(
        "unterminated argument-list parenthesis".into(),
    ))
}

/// Read a dollar-quoted string (`$$ … $$` or `$tag$ … $tag$`) or a
/// single-quoted string literal. Returns `(body, rest)`.
fn read_dollar_quoted_or_string(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidSchema(
            "expected procedure body after AS".into(),
        ));
    }
    if bytes[0] == b'$' {
        // Dollar-quoted: read tag (the substring between the two `$`).
        let mut i = 1;
        while i < bytes.len() && bytes[i] != b'$' {
            // The tag is an identifier-ish token; we accept any
            // non-`$` ASCII byte to match PG, which permits
            // `$_quote_$ … $_quote_$`.
            i += 1;
        }
        if i >= bytes.len() {
            return Err(BasinError::InvalidSchema(
                "unterminated dollar-quote tag at procedure body".into(),
            ));
        }
        let tag_end = i; // bytes[tag_end] == b'$'
        let opener = &s[..tag_end + 1]; // includes both `$`s
        let body_start = tag_end + 1;
        // Find the matching closer.
        let mut j = body_start;
        while j + opener.len() <= bytes.len() {
            if &s[j..j + opener.len()] == opener {
                let body = &s[body_start..j];
                let rest = &s[j + opener.len()..];
                return Ok((body.to_string(), rest));
            }
            j += 1;
        }
        return Err(BasinError::InvalidSchema(format!(
            "unterminated dollar-quoted procedure body (missing closing {opener:?})"
        )));
    }
    if bytes[0] == b'\'' {
        let mut out = String::new();
        let mut i = 1;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push('\'');
                    i += 2;
                    continue;
                }
                return Ok((out, &s[i + 1..]));
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        return Err(BasinError::InvalidSchema(
            "unterminated single-quoted procedure body".into(),
        ));
    }
    Err(BasinError::InvalidSchema(format!(
        "expected $$ … $$ or '…' after AS, got {:?}",
        s.chars().take(8).collect::<String>()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_create_procedure_basic() {
        let sql = "CREATE PROCEDURE p(x BIGINT) LANGUAGE sql AS $$ INSERT INTO t VALUES (x) $$";
        let r = match_create_procedure(sql).unwrap().expect("matched");
        assert_eq!(r.0, "p");
        assert_eq!(r.1.len(), 1);
        assert_eq!(r.1[0].name, "x");
        assert!(r.2.contains("INSERT INTO t VALUES (x)"));
    }

    #[test]
    fn match_create_procedure_no_args() {
        let sql = "CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$";
        let r = match_create_procedure(sql).unwrap().expect("matched");
        assert_eq!(r.0, "p");
        assert!(r.1.is_empty());
    }

    #[test]
    fn match_create_procedure_multi_statement_body() {
        let sql = "CREATE PROCEDURE p(x INT) LANGUAGE sql AS $$ \
                   INSERT INTO t VALUES (x); \
                   DELETE FROM t WHERE id = x \
                   $$";
        let r = match_create_procedure(sql).unwrap().expect("matched");
        assert_eq!(r.0, "p");
        assert!(r.2.contains("INSERT"));
        assert!(r.2.contains("DELETE"));
    }

    #[test]
    fn match_create_procedure_body_then_language() {
        let sql = "CREATE PROCEDURE p() AS $$ SELECT 1 $$ LANGUAGE sql";
        let r = match_create_procedure(sql).unwrap().expect("matched");
        assert_eq!(r.0, "p");
    }

    #[test]
    fn match_create_procedure_case_insensitive() {
        let sql = "create procedure P() language sql as $$ select 1 $$";
        let r = match_create_procedure(sql).unwrap().expect("matched");
        assert_eq!(r.0, "P");
    }

    #[test]
    fn match_create_procedure_trailing_semicolon() {
        let sql = "CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$;";
        let r = match_create_procedure(sql).unwrap();
        assert!(r.is_some());
    }

    #[test]
    fn match_create_procedure_no_paren_args() {
        let sql = "CREATE PROCEDURE p LANGUAGE sql AS $$ SELECT 1 $$";
        let r = match_create_procedure(sql).unwrap().expect("matched");
        assert!(r.1.is_empty());
    }

    #[test]
    fn match_create_procedure_or_replace_rejected() {
        let err = match_create_procedure(
            "CREATE OR REPLACE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$",
        )
        .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_procedure_returns_clause_rejected() {
        let err = match_create_procedure(
            "CREATE PROCEDURE p() RETURNS INT LANGUAGE sql AS $$ SELECT 1 $$",
        )
        .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_procedure_plpgsql_rejected() {
        let err =
            match_create_procedure("CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN END $$")
                .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_procedure_missing_language_rejected() {
        let err = match_create_procedure("CREATE PROCEDURE p() AS $$ SELECT 1 $$").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_procedure_missing_body_rejected() {
        let err = match_create_procedure("CREATE PROCEDURE p() LANGUAGE sql").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_procedure_dollar_tagged_body() {
        let sql = "CREATE PROCEDURE p() LANGUAGE sql AS $body$ SELECT 1 $body$";
        let r = match_create_procedure(sql).unwrap().expect("matched");
        assert_eq!(r.2.trim(), "SELECT 1");
    }

    #[test]
    fn match_create_procedure_non_create_returns_none() {
        let r = match_create_procedure("INSERT INTO t VALUES (1)").unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn match_create_procedure_create_table_returns_none() {
        let r = match_create_procedure("CREATE TABLE t (x INT)").unwrap();
        assert!(r.is_none());
    }
}
