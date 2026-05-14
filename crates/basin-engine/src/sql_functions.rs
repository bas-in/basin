//! Planning-time inliner for `LANGUAGE sql` user-defined scalar functions.
//!
//! The contract: function-call evaluation is **inlining**, not runtime
//! interpretation. A reference to `display_name(u)` becomes a sub-query
//! (or, more often, a substituted expression) in the plan with the
//! argument expression substituted into the body — same shape PG itself
//! uses for `LANGUAGE sql`.
//!
//! Where the rewrite happens: at the sqlparser-AST level, before the SQL
//! string is handed to DataFusion. The rewritten statement is serialised
//! back to SQL via [`Statement::to_string`]; DataFusion plans it as if the
//! user had written the inlined query by hand — which means predicate
//! pushdown, projection pushdown, the simple-select fast path, and every
//! optimizer rule downstream all see the inlined body as a first-class
//! sub-expression. The catalog is consulted once per query (not per
//! function call); when no function is registered for the tenant the
//! whole pass is a no-op.
//!
//! Scope (v0.1):
//!
//! * Scalar return types only.
//! * Scalar argument types only — composite-row args are rejected at
//!   registration.
//! * Recursion (direct *and* mutual) is rejected at registration.
//!   [`validate_for_registration`] catches `f → f`; the sibling
//!   [`validate_no_mutual_recursion`] takes the tenant's existing
//!   function map and walks the call-graph closure of the new body so
//!   `f → g → f`, `f → g → h → f`, etc. are caught before the def is
//!   persisted. The inliner's [`MAX_INLINE_DEPTH`] is now purely a
//!   defensive backstop.

use std::collections::HashMap;
use std::sync::Arc;

use basin_catalog::{Catalog, SqlFunctionDef, SqlFunctionLanguage, SqlReturnType};
use basin_common::{BasinError, Result, TenantId};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Query, Select, SelectItem,
    SetExpr, Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Maximum nested-call depth permitted by the inliner. A plain function
/// call is depth 1; a function whose body calls another registered
/// function is depth 2; and so on. v0.1 caps at 16 — well above any
/// real-world depth. Mutual recursion is rejected at registration time
/// (see [`validate_no_mutual_recursion`]) so this cap exists purely as
/// a defensive backstop in case the registration-time check is ever
/// bypassed.
const MAX_INLINE_DEPTH: usize = 16;

/// Hard cap on call-graph traversal depth in
/// [`validate_no_mutual_recursion`]. The walker visits each function at
/// most once via a `visited` set, so the cap normally never triggers;
/// it exists so a pre-existing cycle in the catalog (which shouldn't
/// happen, since this very check guards every registration) cannot
/// drive an unbounded walk.
const MUTUAL_RECURSION_WALK_CAP: usize = 256;

/// Validate `def`, parse its body, and run the registration-time checks
/// that aren't tied to a particular catalog backend (name validity,
/// recursion, single-SELECT shape, no composite args).
///
/// Returns the validated, ready-to-store [`SqlFunctionDef`]. The caller
/// hands it to [`Catalog::register_sql_function`].
pub fn validate_for_registration(def: SqlFunctionDef) -> Result<SqlFunctionDef> {
    if !is_valid_identifier(&def.name) {
        return Err(BasinError::InvalidIdent(format!(
            "sql function name {:?} is not a valid SQL identifier",
            def.name
        )));
    }
    if is_reserved_function_name(&def.name) {
        return Err(BasinError::InvalidSchema(format!(
            "sql function name {:?} collides with a built-in function",
            def.name
        )));
    }
    if !matches!(def.language, SqlFunctionLanguage::Sql) {
        return Err(BasinError::InvalidSchema(
            "only LANGUAGE sql is supported in v0.1".into(),
        ));
    }
    // For RETURNS TABLE, the column list must be non-empty and column
    // names unique. (The CREATE FUNCTION parser also enforces this; the
    // check is repeated here so direct catalog API callers can't bypass.)
    if let SqlReturnType::Table(cols) = &def.return_type {
        if cols.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "function {:?}: RETURNS TABLE must declare at least one column",
                def.name
            )));
        }
        let mut seen_cols = std::collections::HashSet::new();
        for (cn, _) in cols {
            if !is_valid_identifier(cn) {
                return Err(BasinError::InvalidIdent(format!(
                    "function {:?}: RETURNS TABLE column name {:?} is not a valid SQL identifier",
                    def.name, cn
                )));
            }
            if !seen_cols.insert(cn.to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "function {:?}: RETURNS TABLE column {:?} appears more than once",
                    def.name, cn
                )));
            }
        }
    }
    // Argument-name uniqueness — a body identifier-substitution pass would
    // otherwise be ambiguous.
    let mut seen = std::collections::HashSet::new();
    for arg in &def.args {
        if !is_valid_identifier(&arg.name) {
            return Err(BasinError::InvalidIdent(format!(
                "sql function arg name {:?} is not a valid SQL identifier",
                arg.name
            )));
        }
        if !seen.insert(arg.name.to_ascii_lowercase()) {
            return Err(BasinError::InvalidSchema(format!(
                "duplicate argument name {:?} in function {:?}",
                arg.name, def.name
            )));
        }
    }

    let body_query = parse_body(&def.body, &def.name)?;
    let body_call_names = collect_function_calls_in_query(&body_query);
    if body_call_names
        .iter()
        .any(|n| n.eq_ignore_ascii_case(&def.name))
    {
        return Err(BasinError::InvalidSchema(format!(
            "recursive function {:?} is not supported (LANGUAGE sql does not allow recursion)",
            def.name
        )));
    }

    // RETURNS TABLE: outermost SELECT must project the same number of
    // columns as the declared table shape. We can't fully type-check at
    // registration (the body may reference tables not yet created) so the
    // narrow check is arity-only — the wider type compatibility is
    // resolved at call time when DataFusion sees the inlined body.
    if let SqlReturnType::Table(cols) = &def.return_type {
        let proj_arity = projection_arity(&body_query).ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "function {:?}: RETURNS TABLE body must be a single SELECT \
                 with a fixed projection list (no `*`)",
                def.name
            ))
        })?;
        if proj_arity != cols.len() {
            return Err(BasinError::InvalidSchema(format!(
                "function {:?}: RETURNS TABLE declares {} column(s) but body's \
                 SELECT projects {}",
                def.name,
                cols.len(),
                proj_arity
            )));
        }
    }

    Ok(def)
}

/// Walk the call-graph closure of `def`'s body across the tenant's
/// already-registered functions in `existing` and reject if `def.name`
/// appears anywhere in the closure. Catches mutual recursion
/// (`f → g → f`, `f → g → h → f`, ...) which `validate_for_registration`
/// alone cannot see — it only inspects `def.body`.
///
/// `existing` is a snapshot of the tenant's function map keyed by
/// lowercased name. Built-in functions are not in this map and therefore
/// don't trigger spurious rejections; the walker simply ignores any
/// callee that isn't a registered key.
///
/// For `CREATE OR REPLACE FUNCTION` semantics the caller passes the
/// snapshot as-is — the new `def` is treated as authoritative for its
/// own name (the walker uses `def.body` as the starting point and only
/// consults `existing` for *other* names).
///
/// O(N) in the size of the tenant's function catalog: parsed bodies are
/// cached in `parsed` so each function is parsed at most once. Bounded
/// by [`MUTUAL_RECURSION_WALK_CAP`] as a defensive backstop against a
/// pre-existing cycle in the catalog.
pub fn validate_no_mutual_recursion(
    def: &SqlFunctionDef,
    existing: &HashMap<String, SqlFunctionDef>,
) -> Result<()> {
    let target = def.name.to_ascii_lowercase();

    // Cache: lowercased-name → direct callees (lowercased) that are
    // themselves registered functions in `existing`. Only includes names
    // we've parsed so far.
    let mut callees_cache: HashMap<String, Vec<String>> = HashMap::new();

    // Parse the new def's body once.
    let body_query = parse_body(&def.body, &def.name)?;
    let direct = registered_callees(&body_query, existing, &target);
    callees_cache.insert(target.clone(), direct.clone());

    // BFS keyed on lowercased function name. Each entry carries the path
    // from `def.name` so we can render it on a hit. Visited keeps each
    // function to one expansion.
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    visited.insert(target.clone());
    let mut queue: std::collections::VecDeque<Vec<String>> = std::collections::VecDeque::new();
    for c in &direct {
        queue.push_back(vec![target.clone(), c.clone()]);
    }

    let mut steps = 0usize;
    while let Some(path) = queue.pop_front() {
        steps += 1;
        if steps > MUTUAL_RECURSION_WALK_CAP {
            // Defensive: a pre-existing cycle in the catalog would let
            // BFS run forever without `visited` because each path is
            // distinct. `visited` already prevents that, so reaching
            // this branch means the catalog is huge — surface as a
            // schema error so the registration fails closed.
            return Err(BasinError::InvalidSchema(format!(
                "function {:?}: call-graph traversal exceeded {} steps; \
                 refusing to register",
                def.name, MUTUAL_RECURSION_WALK_CAP
            )));
        }
        let current = path.last().unwrap().clone();
        if current == target {
            // Cycle: render the path with original casing where we have
            // it (def.name for the endpoints, existing[].name for the
            // middle hops).
            let display = render_cycle_path(&path, def, existing);
            return Err(BasinError::InvalidSchema(format!(
                "function {} would create a mutual-recursion cycle: {}",
                def.name, display
            )));
        }
        if !visited.insert(current.clone()) {
            continue;
        }
        // Expand this node's callees, parsing+caching on first visit.
        let next = if let Some(cached) = callees_cache.get(&current) {
            cached.clone()
        } else if let Some(callee_def) = existing.get(&current) {
            let q = parse_body(&callee_def.body, &callee_def.name)?;
            let cs = registered_callees(&q, existing, &target);
            callees_cache.insert(current.clone(), cs.clone());
            cs
        } else {
            // Not a registered function (built-in or unknown) — no
            // children to expand. Built-in callees were already filtered
            // by `registered_callees` so this branch is mostly defensive.
            Vec::new()
        };
        for c in next {
            let mut new_path = path.clone();
            new_path.push(c);
            queue.push_back(new_path);
        }
    }
    Ok(())
}

/// Direct callees of `q` that are registered functions in `existing`,
/// plus an explicit re-test against `target` so a body that calls the
/// function being registered (mutual cycle through one hop) is included
/// even though `target` is not yet in `existing`. Lowercased names,
/// deduplicated, order preserved.
fn registered_callees(
    q: &Query,
    existing: &HashMap<String, SqlFunctionDef>,
    target: &str,
) -> Vec<String> {
    let raw = collect_function_calls_in_query(q);
    let mut out: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for n in raw {
        let lower = n.to_ascii_lowercase();
        if !seen.insert(lower.clone()) {
            continue;
        }
        if lower == target || existing.contains_key(&lower) {
            out.push(lower);
        }
    }
    out
}

/// Render a cycle path (lowercased internal names) using each
/// function's original casing for the error message. The first and last
/// hop are always `def.name`; intermediate hops come from `existing`.
fn render_cycle_path(
    path: &[String],
    def: &SqlFunctionDef,
    existing: &HashMap<String, SqlFunctionDef>,
) -> String {
    let target = def.name.to_ascii_lowercase();
    let mut parts = Vec::with_capacity(path.len());
    for hop in path {
        if hop == &target {
            parts.push(def.name.clone());
        } else if let Some(d) = existing.get(hop) {
            parts.push(d.name.clone());
        } else {
            parts.push(hop.clone());
        }
    }
    parts.join(" -> ")
}

/// Count the columns the outermost SELECT in `q` projects, returning
/// `None` when the projection is not a fixed expression list (e.g. it
/// contains `*` or a qualified `t.*` we can't statically count).
fn projection_arity(q: &Query) -> Option<usize> {
    let sel = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    let mut n = 0usize;
    for item in &sel.projection {
        match item {
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => n += 1,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => return None,
        }
    }
    Some(n)
}

/// Inline every registered-SQL-function call in `sql` for the tenant.
/// Returns the rewritten SQL string. When the tenant has no functions
/// registered (and `sql` references no functions to expand) the input is
/// returned unchanged via the early-out in
/// [`maybe_inline_query_statement`].
pub async fn rewrite_sql_inlining_functions(
    catalog: &Arc<dyn Catalog>,
    tenant: &TenantId,
    sql: &str,
) -> Result<String> {
    // Cheap parse to look for any function calls. If sqlparser fails or the
    // statement isn't a Query / DML, fall through unchanged — the existing
    // executor takes the same shape into its own parse error handling.
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(sql.to_string()),
    };
    if stmts.len() != 1 {
        return Ok(sql.to_string());
    }
    let mut stmt = stmts.into_iter().next().unwrap();

    // Quick gate: does the statement reference any function whose name
    // looks user-defined (i.e. not built-in)? When the statement has no
    // function calls at all we skip the catalog hop entirely.
    if !statement_has_any_function_call(&stmt) {
        return Ok(sql.to_string());
    }

    // Pre-load the tenant's full function catalog. Building the map up
    // front avoids re-locking the catalog mutex for every inlined call;
    // for tenants with one or two functions this is also cheaper than
    // per-call lookups.
    let funcs = catalog.list_sql_functions(tenant).await;
    if funcs.is_empty() {
        return Ok(sql.to_string());
    }
    let mut by_name: HashMap<String, SqlFunctionDef> = HashMap::with_capacity(funcs.len());
    for f in funcs {
        by_name.insert(f.name.to_ascii_lowercase(), f);
    }

    rewrite_statement(&mut stmt, &by_name, 0)?;
    Ok(stmt.to_string())
}

fn rewrite_statement(
    stmt: &mut Statement,
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<()> {
    match stmt {
        Statement::Query(q) => rewrite_query(q, by_name, depth),
        // `INSERT INTO ... SELECT ...` carries a source query whose
        // projection / filter expressions may reference user-defined
        // functions. Descend so the inlined body shows up before
        // sqlparser hands the statement to the executor. Other DML
        // surfaces (UPDATE, DELETE) keep their narrow v0.1 scope —
        // they can adopt the same hook when needed.
        Statement::Insert(ins) => {
            if let Some(source) = ins.source.as_mut() {
                rewrite_query(source, by_name, depth)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn rewrite_query(
    query: &mut Query,
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<()> {
    rewrite_set_expr(&mut query.body, by_name, depth)?;
    if let Some(limit) = query.limit.as_mut() {
        rewrite_expr(limit, by_name, depth)?;
    }
    Ok(())
}

fn rewrite_set_expr(
    set: &mut SetExpr,
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<()> {
    match set {
        SetExpr::Select(sel) => rewrite_select(sel, by_name, depth),
        SetExpr::Query(q) => rewrite_query(q, by_name, depth),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left, by_name, depth)?;
            rewrite_set_expr(right, by_name, depth)
        }
        // Values / Insert / Update / Table — leave alone for v0.1.
        _ => Ok(()),
    }
}

fn rewrite_select(
    sel: &mut Select,
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<()> {
    for item in sel.projection.iter_mut() {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                rewrite_expr(e, by_name, depth)?;
            }
            _ => {}
        }
    }
    if let Some(sel_expr) = sel.selection.as_mut() {
        rewrite_expr(sel_expr, by_name, depth)?;
    }
    if let Some(having) = sel.having.as_mut() {
        rewrite_expr(having, by_name, depth)?;
    }
    // `from` joins may carry ON-clause expressions. Walk them, and
    // additionally inline any table-valued user-defined function
    // references appearing as a `TableFactor`.
    for twj in sel.from.iter_mut() {
        rewrite_table_factor(&mut twj.relation, by_name, depth)?;
        for join in twj.joins.iter_mut() {
            rewrite_table_factor(&mut join.relation, by_name, depth)?;
            if let sqlparser::ast::JoinOperator::Inner(constraint)
            | sqlparser::ast::JoinOperator::LeftOuter(constraint)
            | sqlparser::ast::JoinOperator::RightOuter(constraint)
            | sqlparser::ast::JoinOperator::FullOuter(constraint)
            | sqlparser::ast::JoinOperator::LeftSemi(constraint)
            | sqlparser::ast::JoinOperator::LeftAnti(constraint)
            | sqlparser::ast::JoinOperator::RightSemi(constraint)
            | sqlparser::ast::JoinOperator::RightAnti(constraint) = &mut join.join_operator
            {
                if let sqlparser::ast::JoinConstraint::On(on) = constraint {
                    rewrite_expr(on, by_name, depth)?;
                }
            }
        }
    }
    Ok(())
}

/// Inline a table-valued user-defined function reference when one appears
/// in `FROM` / `JOIN` position. `TableFactor::Table { args: Some(_), .. }`
/// is the call site; the function body becomes a derived sub-query with
/// the call-site args substituted in. `LATERAL` is emitted whenever any
/// argument is more than a literal (a column reference from another
/// table in the same FROM clause), so DataFusion's planner sees the
/// cross-row correlation explicitly.
fn rewrite_table_factor(
    tf: &mut sqlparser::ast::TableFactor,
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<()> {
    use sqlparser::ast::TableFactor;
    match tf {
        TableFactor::Table {
            name,
            args: Some(table_args),
            alias,
            ..
        } => {
            let fname = match object_name_unqualified(name) {
                Some(n) => n,
                None => return Ok(()),
            };
            let Some(def) = by_name.get(&fname.to_ascii_lowercase()) else {
                return Ok(());
            };
            // Refuse a table-position call into a scalar function.
            if let SqlReturnType::Scalar(_) = def.return_type {
                return Err(BasinError::InvalidSchema(format!(
                    "function {fname} returns a scalar; cannot be used in FROM position. \
                     Use it in expression position (e.g. SELECT {fname}(...) FROM t)."
                )));
            }
            // Build the substituted body and wrap as a derived table.
            let call_args: Vec<FunctionArg> = table_args.args.clone();
            let needs_lateral = call_args.iter().any(arg_references_outer);
            let body = inline_table_call(def, &call_args, by_name, depth + 1)?;
            let alias_for_derived = alias.take().unwrap_or_else(|| sqlparser::ast::TableAlias {
                name: sqlparser::ast::Ident::new(def.name.clone()),
                columns: Vec::new(),
            });
            *tf = TableFactor::Derived {
                lateral: needs_lateral,
                subquery: Box::new(body),
                alias: Some(alias_for_derived),
            };
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => rewrite_query(subquery, by_name, depth),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_factor(&mut table_with_joins.relation, by_name, depth)?;
            for join in table_with_joins.joins.iter_mut() {
                rewrite_table_factor(&mut join.relation, by_name, depth)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// True when `arg` is anything other than a constant literal — i.e. the
/// substituted body would refer to a column / expression from the
/// surrounding FROM list, and therefore needs `LATERAL` semantics.
fn arg_references_outer(arg: &FunctionArg) -> bool {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => expr_is_non_literal(e),
        _ => false,
    }
}

fn expr_is_non_literal(e: &Expr) -> bool {
    match e {
        Expr::Value(_) => false,
        Expr::UnaryOp { expr, .. } => expr_is_non_literal(expr),
        Expr::Nested(inner) => expr_is_non_literal(inner),
        _ => true,
    }
}

/// Substitute a table-valued function call's body and return the result
/// as a `Query` ready to be wrapped in `TableFactor::Derived`. Mirrors
/// `inline_one_call` but always returns the body verbatim (never the
/// bare-projection short-circuit, which only makes sense in scalar
/// position).
fn inline_table_call(
    def: &SqlFunctionDef,
    call_args: &[FunctionArg],
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<Query> {
    if call_args.len() != def.args.len() {
        return Err(BasinError::InvalidSchema(format!(
            "function {}: expected {} args, got {}",
            def.name,
            def.args.len(),
            call_args.len()
        )));
    }
    let mut subs: HashMap<String, Expr> = HashMap::with_capacity(def.args.len());
    for (param, call_arg) in def.args.iter().zip(call_args.iter()) {
        let expr = function_arg_to_expr(call_arg, &def.name)?;
        subs.insert(param.name.to_ascii_lowercase(), expr);
    }
    let mut body = parse_body(&def.body, &def.name)?;
    substitute_args_in_query(&mut body, &subs)?;
    rewrite_query(&mut body, by_name, depth)?;
    Ok(body)
}

fn rewrite_expr(
    expr: &mut Expr,
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<()> {
    if depth > MAX_INLINE_DEPTH {
        return Err(BasinError::InvalidSchema(format!(
            "sql function inliner exceeded max depth {MAX_INLINE_DEPTH}; \
             possible mutual recursion"
        )));
    }
    // Walk children first (post-order). For each `Expr::Function` whose name
    // matches a registered SQL function, replace the node with the
    // substituted body expression.
    walk_expr_children_mut(expr, by_name, depth)?;

    if let Expr::Function(func) = expr {
        let name = match object_name_unqualified(&func.name) {
            Some(n) => n,
            None => return Ok(()),
        };
        let Some(def) = by_name.get(&name.to_ascii_lowercase()) else {
            return Ok(());
        };
        let call_args = match &func.args {
            FunctionArguments::List(list) => list.args.clone(),
            FunctionArguments::None => Vec::new(),
            FunctionArguments::Subquery(_) => {
                return Err(BasinError::InvalidSchema(format!(
                    "function {name} called with subquery arg form is not supported"
                )));
            }
        };
        // Reject window / FILTER / WITHIN GROUP / NULLS clauses on a
        // user-defined scalar function call — none make sense on a
        // `LANGUAGE sql` scalar.
        if func.over.is_some() || func.filter.is_some() || !func.within_group.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "function {name} is a LANGUAGE sql scalar and does not support \
                 OVER / FILTER / WITHIN GROUP"
            )));
        }
        // A `RETURNS TABLE` function cannot be used in expression position
        // — only in `FROM`. Surface the type mismatch with a hint at the
        // correct shape.
        if matches!(def.return_type, SqlReturnType::Table(_)) {
            return Err(BasinError::InvalidSchema(format!(
                "function {name} returns a table; cannot be used in expression position. \
                 Use it in FROM (e.g. SELECT * FROM {name}(...))."
            )));
        }
        let inlined = inline_one_call(def, &call_args, by_name, depth + 1)?;
        *expr = inlined;
    }
    Ok(())
}

/// Recurse into the immediate sub-expressions of `expr`. Used as the
/// post-order step of [`rewrite_expr`].
fn walk_expr_children_mut(
    expr: &mut Expr,
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(left, by_name, depth)?;
            rewrite_expr(right, by_name, depth)
        }
        Expr::UnaryOp { expr: inner, .. } => rewrite_expr(inner, by_name, depth),
        Expr::Cast { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => rewrite_expr(inner, by_name, depth),
        Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
            rewrite_expr(a, by_name, depth)?;
            rewrite_expr(b, by_name, depth)
        }
        Expr::InList { expr: e, list, .. } => {
            rewrite_expr(e, by_name, depth)?;
            for item in list.iter_mut() {
                rewrite_expr(item, by_name, depth)?;
            }
            Ok(())
        }
        Expr::InSubquery {
            expr: e, subquery, ..
        } => {
            rewrite_expr(e, by_name, depth)?;
            rewrite_query(subquery, by_name, depth)
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            rewrite_expr(e, by_name, depth)?;
            rewrite_expr(low, by_name, depth)?;
            rewrite_expr(high, by_name, depth)
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
            rewrite_expr(e, by_name, depth)?;
            rewrite_expr(pattern, by_name, depth)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand.as_mut() {
                rewrite_expr(op, by_name, depth)?;
            }
            for c in conditions.iter_mut() {
                rewrite_expr(c, by_name, depth)?;
            }
            for r in results.iter_mut() {
                rewrite_expr(r, by_name, depth)?;
            }
            if let Some(er) = else_result.as_mut() {
                rewrite_expr(er, by_name, depth)?;
            }
            Ok(())
        }
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => rewrite_query(q, by_name, depth),
        Expr::Function(func) => {
            // Recurse into arguments so nested calls (`f(g(x))`) get
            // inlined inside-out.
            if let FunctionArguments::List(list) = &mut func.args {
                for arg in list.args.iter_mut() {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        rewrite_expr(e, by_name, depth)?;
                    }
                }
            }
            Ok(())
        }
        // Identifiers, literals, wildcards, etc. — leaves.
        _ => Ok(()),
    }
}

/// Substitute a single function call's body and return the resulting
/// expression. The rule:
///
/// * Body is a single `SELECT <projection>` (`FROM` / `WHERE` permitted but
///   the v0.1 `LANGUAGE sql` shape is mostly bare-SELECT).
/// * If the body has no `FROM` (and no `WHERE` / `GROUP BY` / etc.) and
///   exactly one projection, we substitute arg identifiers in the
///   projection and return that expression directly. This is the common
///   case (`add_one(x) = SELECT x + 1`) and produces the cleanest plan.
/// * Otherwise we wrap the entire body as a scalar sub-query
///   `(SELECT ... FROM ...)`. DataFusion handles scalar sub-queries with
///   correlated references as long as the outer scope is in scope, which
///   it is here because the args have been substituted by the call-site
///   expressions.
fn inline_one_call(
    def: &SqlFunctionDef,
    call_args: &[FunctionArg],
    by_name: &HashMap<String, SqlFunctionDef>,
    depth: usize,
) -> Result<Expr> {
    if call_args.len() != def.args.len() {
        return Err(BasinError::InvalidSchema(format!(
            "function {}: expected {} args, got {}",
            def.name,
            def.args.len(),
            call_args.len()
        )));
    }
    let mut subs: HashMap<String, Expr> = HashMap::with_capacity(def.args.len());
    for (param, call_arg) in def.args.iter().zip(call_args.iter()) {
        let expr = function_arg_to_expr(call_arg, &def.name)?;
        subs.insert(param.name.to_ascii_lowercase(), expr);
    }

    let mut body = parse_body(&def.body, &def.name)?;

    // Substitute arg identifiers in the body, then run the inliner over
    // the body itself so nested registered-function calls also expand.
    substitute_args_in_query(&mut body, &subs)?;
    rewrite_query(&mut body, by_name, depth)?;

    // Detect the bare-SELECT shape: single projection, no FROM / WHERE /
    // GROUP BY / HAVING / etc. → return the substituted expression
    // directly.
    if let SetExpr::Select(sel) = body.body.as_ref() {
        if is_bare_select(sel) && sel.projection.len() == 1 {
            if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } =
                &sel.projection[0]
            {
                return Ok(wrap_paren(e.clone()));
            }
        }
    }
    // Fall back to a scalar sub-query wrap.
    Ok(Expr::Subquery(Box::new(body)))
}

/// Wrap `e` in `Expr::Nested` so the resulting SQL parenthesises the
/// substitution. Avoids precedence surprises after substituting an
/// identifier into a position that expected an opaque token.
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

fn is_bare_select(sel: &Select) -> bool {
    sel.from.is_empty()
        && sel.selection.is_none()
        && sel.having.is_none()
        && sel.qualify.is_none()
        && sel.distinct.is_none()
        && match &sel.group_by {
            sqlparser::ast::GroupByExpr::Expressions(v, _) => v.is_empty(),
            sqlparser::ast::GroupByExpr::All(_) => false,
        }
}

fn function_arg_to_expr(arg: &FunctionArg, fn_name: &str) -> Result<Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => Ok(e.clone()),
        _ => Err(BasinError::InvalidSchema(format!(
            "function {fn_name}: wildcard / qualified-wildcard arg form not supported"
        ))),
    }
}

fn substitute_args_in_query(query: &mut Query, subs: &HashMap<String, Expr>) -> Result<()> {
    substitute_args_in_set_expr(&mut query.body, subs)?;
    if let Some(limit) = query.limit.as_mut() {
        substitute_args_in_expr(limit, subs)?;
    }
    Ok(())
}

fn substitute_args_in_set_expr(set: &mut SetExpr, subs: &HashMap<String, Expr>) -> Result<()> {
    match set {
        SetExpr::Select(sel) => substitute_args_in_select(sel, subs),
        SetExpr::Query(q) => substitute_args_in_query(q, subs),
        SetExpr::SetOperation { left, right, .. } => {
            substitute_args_in_set_expr(left, subs)?;
            substitute_args_in_set_expr(right, subs)
        }
        _ => Ok(()),
    }
}

fn substitute_args_in_select(sel: &mut Select, subs: &HashMap<String, Expr>) -> Result<()> {
    for item in sel.projection.iter_mut() {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                substitute_args_in_expr(e, subs)?;
            }
            _ => {}
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

fn substitute_args_in_expr(expr: &mut Expr, subs: &HashMap<String, Expr>) -> Result<()> {
    if let Expr::Identifier(id) = expr {
        if let Some(replacement) = subs.get(&id.value.to_ascii_lowercase()) {
            *expr = wrap_paren(replacement.clone());
            return Ok(());
        }
    }
    walk_expr_children_substitute(expr, subs)
}

fn walk_expr_children_substitute(expr: &mut Expr, subs: &HashMap<String, Expr>) -> Result<()> {
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
            results,
            else_result,
        } => {
            if let Some(op) = operand.as_mut() {
                substitute_args_in_expr(op, subs)?;
            }
            for c in conditions.iter_mut() {
                substitute_args_in_expr(c, subs)?;
            }
            for r in results.iter_mut() {
                substitute_args_in_expr(r, subs)?;
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
        _ => Ok(()),
    }
}

fn parse_body(body: &str, fn_name: &str) -> Result<Query> {
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, body).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "function {fn_name}: body does not parse as SQL: {e}"
        ))
    })?;
    if stmts.len() != 1 {
        return Err(BasinError::InvalidSchema(format!(
            "function {fn_name}: body must be a single statement (got {})",
            stmts.len()
        )));
    }
    let stmt = stmts.pop().unwrap();
    match stmt {
        Statement::Query(q) => Ok(*q),
        _ => Err(BasinError::InvalidSchema(format!(
            "function {fn_name}: body must be a SELECT statement"
        ))),
    }
}

fn collect_function_calls_in_query(q: &Query) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    collect_set_expr(&q.body, &mut out);
    if let Some(limit) = q.limit.as_ref() {
        collect_expr(limit, &mut out);
    }
    out
}

fn collect_set_expr(set: &SetExpr, out: &mut Vec<String>) {
    match set {
        SetExpr::Select(sel) => collect_select(sel, out),
        SetExpr::Query(q) => out.extend(collect_function_calls_in_query(q)),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr(left, out);
            collect_set_expr(right, out);
        }
        _ => {}
    }
}

fn collect_select(sel: &Select, out: &mut Vec<String>) {
    for item in sel.projection.iter() {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                collect_expr(e, out);
            }
            _ => {}
        }
    }
    if let Some(sel_expr) = sel.selection.as_ref() {
        collect_expr(sel_expr, out);
    }
    if let Some(having) = sel.having.as_ref() {
        collect_expr(having, out);
    }
    // Table-position user-defined function calls also count as references
    // for the pre-gate; without this the inliner would early-out on
    // `SELECT * FROM recent_orders(42)`.
    for twj in sel.from.iter() {
        collect_table_factor(&twj.relation, out);
        for join in twj.joins.iter() {
            collect_table_factor(&join.relation, out);
        }
    }
}

fn collect_table_factor(tf: &sqlparser::ast::TableFactor, out: &mut Vec<String>) {
    use sqlparser::ast::TableFactor;
    match tf {
        TableFactor::Table {
            name,
            args: Some(table_args),
            ..
        } => {
            if let Some(n) = object_name_unqualified(name) {
                out.push(n);
            }
            for arg in table_args.args.iter() {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } = arg
                {
                    collect_expr(e, out);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            out.extend(collect_function_calls_in_query(subquery));
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_table_factor(&table_with_joins.relation, out);
            for join in table_with_joins.joins.iter() {
                collect_table_factor(&join.relation, out);
            }
        }
        _ => {}
    }
}

fn collect_expr(expr: &Expr, out: &mut Vec<String>) {
    if let Expr::Function(func) = expr {
        if let Some(name) = object_name_unqualified(&func.name) {
            out.push(name);
        }
        if let FunctionArguments::List(list) = &func.args {
            for arg in list.args.iter() {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } = arg
                {
                    collect_expr(e, out);
                }
            }
        }
    }
    walk_expr_children_collect(expr, out);
}

fn walk_expr_children_collect(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_expr(left, out);
            collect_expr(right, out);
        }
        Expr::UnaryOp { expr: inner, .. } => collect_expr(inner, out),
        Expr::Cast { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsTrue(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotUnknown(inner) => collect_expr(inner, out),
        Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
            collect_expr(a, out);
            collect_expr(b, out);
        }
        Expr::InList { expr: e, list, .. } => {
            collect_expr(e, out);
            for item in list.iter() {
                collect_expr(item, out);
            }
        }
        Expr::InSubquery {
            expr: e, subquery, ..
        } => {
            collect_expr(e, out);
            out.extend(collect_function_calls_in_query(subquery));
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_expr(e, out);
            collect_expr(low, out);
            collect_expr(high, out);
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
            collect_expr(e, out);
            collect_expr(pattern, out);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand.as_ref() {
                collect_expr(op, out);
            }
            for c in conditions.iter() {
                collect_expr(c, out);
            }
            for r in results.iter() {
                collect_expr(r, out);
            }
            if let Some(er) = else_result.as_ref() {
                collect_expr(er, out);
            }
        }
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            out.extend(collect_function_calls_in_query(q));
        }
        _ => {}
    }
}

fn statement_has_any_function_call(stmt: &Statement) -> bool {
    match stmt {
        Statement::Query(q) => !collect_function_calls_in_query(q).is_empty(),
        // `INSERT INTO … SELECT …` carries a source `Query` whose
        // projection / filter expressions may reference user-defined
        // functions. Without this branch the cheap-gate returns false
        // and the inliner skips the rewrite — which is wrong for
        // procedure bodies like
        // `INSERT INTO summary SELECT add_one(x) FROM t`.
        Statement::Insert(ins) => ins
            .source
            .as_ref()
            .is_some_and(|q| !collect_function_calls_in_query(q).is_empty()),
        _ => false,
    }
}

fn object_name_unqualified(name: &ObjectName) -> Option<String> {
    if name.0.len() != 1 {
        return None;
    }
    Some(name.0[0].value.clone())
}

fn is_valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Names we refuse to register over. Two layers: SQL keywords sqlparser
/// would reject anyway, plus the explicit Basin built-in UDFs that
/// `register_pg_compat_udfs` / `register_pg_udfs` / `register_distance_udfs`
/// install on every session. Keeping the list central means the
/// `register_sql_function` validator and the inliner agree on what's a
/// user-defined name.
fn is_reserved_function_name(name: &str) -> bool {
    const RESERVED: &[&str] = &[
        // Distance UDFs (basin-engine::udf::register_distance_udfs)
        "l2_distance",
        "cosine_distance",
        "dot_product",
        // pgcrypto / uuid (register_pg_udfs)
        "gen_random_uuid",
        "uuid_generate_v4",
        "digest",
        "encode",
        "decode",
        "crypt",
        "gen_salt",
        // PG-compat numeric / time (register_pg_compat_udfs)
        "mod",
        "age",
        "to_char",
        "to_timestamp",
        "to_date",
        "to_number",
        "power",
        // DataFusion built-ins customers may surprise themselves with.
        "coalesce",
        "concat",
        "concat_ws",
        "lower",
        "upper",
        "length",
        "char_length",
        "abs",
        "round",
        "floor",
        "ceil",
        "now",
        "current_timestamp",
        "current_date",
        "current_time",
    ];
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_catalog::{SqlArgType, SqlFunctionArg};

    fn dummy_def(name: &str, args: Vec<(&str, SqlArgType)>, body: &str) -> SqlFunctionDef {
        SqlFunctionDef {
            tenant: TenantId::new(),
            name: name.into(),
            args: args
                .into_iter()
                .map(|(n, t)| SqlFunctionArg {
                    name: n.into(),
                    data_type: t,
                })
                .collect(),
            return_type: SqlReturnType::Scalar(SqlArgType::BigInt),
            body: body.into(),
            language: SqlFunctionLanguage::Sql,
        }
    }

    #[test]
    fn rejects_recursive_function_at_validation() {
        let def = dummy_def("f", vec![("x", SqlArgType::BigInt)], "SELECT f(x)");
        let err = validate_for_registration(def).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
    }

    #[test]
    fn rejects_reserved_name() {
        let def = dummy_def("coalesce", vec![("x", SqlArgType::BigInt)], "SELECT x");
        let err = validate_for_registration(def).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
    }

    #[test]
    fn rejects_invalid_identifier() {
        let def = dummy_def("1bad", vec![("x", SqlArgType::BigInt)], "SELECT x");
        let err = validate_for_registration(def).unwrap_err();
        assert!(matches!(err, BasinError::InvalidIdent(_)), "got {err:?}");
    }

    #[test]
    fn rejects_multi_statement_body() {
        let def = dummy_def("f", vec![("x", SqlArgType::BigInt)], "SELECT 1; SELECT 2");
        let err = validate_for_registration(def).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
    }

    #[test]
    fn rejects_non_select_body() {
        let def = dummy_def(
            "f",
            vec![("x", SqlArgType::BigInt)],
            "INSERT INTO t VALUES (1)",
        );
        let err = validate_for_registration(def).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)), "got {err:?}");
    }

    #[test]
    fn validates_simple_function() {
        let def = dummy_def("add_one", vec![("x", SqlArgType::BigInt)], "SELECT x + 1");
        let v = validate_for_registration(def).unwrap();
        assert_eq!(v.name, "add_one");
    }

    fn validated(def: SqlFunctionDef) -> SqlFunctionDef {
        validate_for_registration(def).unwrap()
    }

    fn map_with(defs: Vec<SqlFunctionDef>) -> HashMap<String, SqlFunctionDef> {
        let mut m = HashMap::new();
        for d in defs {
            m.insert(d.name.to_ascii_lowercase(), d);
        }
        m
    }

    #[test]
    fn direct_recursion_still_rejected() {
        let def = dummy_def("f", vec![("x", SqlArgType::BigInt)], "SELECT f(x)");
        let err = validate_for_registration(def).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.to_ascii_lowercase().contains("recurs"),
            "expected a recursion error, got {msg}"
        );
    }

    #[test]
    fn mutual_recursion_two_functions_rejected() {
        // First registration: `f` calling `g` against an empty catalog
        // succeeds (g doesn't exist yet — its callee is a built-in or
        // unknown name as far as the closure walk is concerned).
        let f = validated(dummy_def(
            "f",
            vec![("x", SqlArgType::BigInt)],
            "SELECT g(x)",
        ));
        let existing = map_with(vec![]);
        validate_no_mutual_recursion(&f, &existing).unwrap();

        // Now register `g` calling `f` against a catalog that has `f`.
        let g = validated(dummy_def(
            "g",
            vec![("x", SqlArgType::BigInt)],
            "SELECT f(x)",
        ));
        let existing = map_with(vec![f]);
        let err = validate_no_mutual_recursion(&g, &existing).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("mutual-recursion cycle"),
            "expected mutual-recursion error, got {msg}"
        );
        assert!(
            msg.contains("g") && msg.contains("f"),
            "error must name both functions, got {msg}"
        );
    }

    #[test]
    fn mutual_recursion_three_functions_rejected() {
        // f → g → h → f
        let f = validated(dummy_def(
            "f",
            vec![("x", SqlArgType::BigInt)],
            "SELECT g(x)",
        ));
        validate_no_mutual_recursion(&f, &map_with(vec![])).unwrap();

        let g = validated(dummy_def(
            "g",
            vec![("x", SqlArgType::BigInt)],
            "SELECT h(x)",
        ));
        // g calling h is fine when h isn't registered yet.
        validate_no_mutual_recursion(&g, &map_with(vec![f.clone()])).unwrap();

        let h = validated(dummy_def(
            "h",
            vec![("x", SqlArgType::BigInt)],
            "SELECT f(x)",
        ));
        let err = validate_no_mutual_recursion(&h, &map_with(vec![f, g])).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("mutual-recursion cycle"),
            "expected mutual-recursion error, got {msg}"
        );
    }

    #[test]
    fn path_in_error_message_correct() {
        let f = validated(dummy_def(
            "f",
            vec![("x", SqlArgType::BigInt)],
            "SELECT g(x)",
        ));
        let g = validated(dummy_def(
            "g",
            vec![("x", SqlArgType::BigInt)],
            "SELECT h(x)",
        ));
        let h = validated(dummy_def(
            "h",
            vec![("x", SqlArgType::BigInt)],
            "SELECT f(x)",
        ));
        let err =
            validate_no_mutual_recursion(&h, &map_with(vec![f.clone(), g.clone()])).unwrap_err();
        let msg = format!("{err}");
        // Path is rendered "h -> f -> g -> h"; assert each function
        // appears in the message and the arrow separator is present.
        assert!(msg.contains(" -> "), "expected ' -> ' in path, got {msg}");
        for n in ["f", "g", "h"] {
            assert!(msg.contains(n), "expected {n} in path, got {msg}");
        }
    }

    #[test]
    fn non_recursive_chain_succeeds() {
        // f → g → h with no cycle back to f. Any of these registrations
        // should pass the closure check.
        let h = validated(dummy_def(
            "h",
            vec![("x", SqlArgType::BigInt)],
            "SELECT x + 1",
        ));
        validate_no_mutual_recursion(&h, &map_with(vec![])).unwrap();

        let g = validated(dummy_def(
            "g",
            vec![("x", SqlArgType::BigInt)],
            "SELECT h(x)",
        ));
        validate_no_mutual_recursion(&g, &map_with(vec![h.clone()])).unwrap();

        let f = validated(dummy_def(
            "f",
            vec![("x", SqlArgType::BigInt)],
            "SELECT g(x)",
        ));
        validate_no_mutual_recursion(&f, &map_with(vec![g, h])).unwrap();
    }

    #[test]
    fn built_in_calls_dont_trigger() {
        // `coalesce`, `lower`, `now` are built-ins and not in the
        // catalog; the closure walker must not flag them.
        let f = validated(dummy_def(
            "f",
            vec![("x", SqlArgType::BigInt)],
            "SELECT coalesce(lower(now()), x)",
        ));
        validate_no_mutual_recursion(&f, &map_with(vec![])).unwrap();
        // Same when other unrelated funcs are in the map.
        let other = validated(dummy_def(
            "other",
            vec![("y", SqlArgType::BigInt)],
            "SELECT y * 2",
        ));
        validate_no_mutual_recursion(&f, &map_with(vec![other])).unwrap();
    }
}
