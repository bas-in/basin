//! Postgres-compatible column naming for unaliased `SELECT`-list items.
//!
//! ## The bug this fixes
//!
//! DataFusion names every unaliased projection column after its own
//! expression-display text (`Expr::schema_name()`), e.g.
//! `pg_try_advisory_lock(Int64(424242))` or `Int64(1) + Int64(1)`. Basin was
//! copying those names verbatim into `RowDescription` (see
//! `crates/basin-router/src/types.rs::field_description`, fed by
//! `crates/basin-engine/src/convert.rs::schema_df_to_ws`, fed by the Arrow
//! schema DataFusion hands back). Any client that reads columns by name
//! (ORMs, `psql`, `row["col"]`) sees the wrong key.
//!
//! Real Postgres (`FigureColnameInternal` in
//! `backend/parser/parse_target.c`) uses a much narrower rule for an
//! unaliased result column:
//!   - a bare column reference -> the column's own name
//!   - a function call (scalar / aggregate / window) -> the function's bare
//!     name, no arguments
//!   - a cast -> recurse into the argument; if *it* has a name, use that;
//!     otherwise fall back to the target type's short `pg_type.typname`
//!   - anything else -> the literal string `?column?`
//!
//! This module reproduces that narrower rule and applies it to the Arrow
//! schema Basin is about to hand to the pgwire layer -- *after* DataFusion
//! has already planned and executed the query, so it never touches the
//! planner (`crates/basin-plan`) or the executor (`crates/basin-exec`).
//!
//! ## Two call sites, two expression trees
//!
//! - [`pg_style_column_names`] runs on the *normal* path: it reads the
//!   already-planned `LogicalPlan`'s top-level `Projection` expressions
//!   (DataFusion's `datafusion_expr::Expr`) and renames fields in place.
//! - [`pg_names_for_select_list`] runs only on the *duplicate-name retry*
//!   path (see `dedupe_select_list_for_planning`): DataFusion's
//!   `Projection::try_new` refuses a plan with two columns that would
//!   display under the same name (`SELECT f(5), f(5)`; Postgres allows
//!   this). We re-parse the original SQL's `sqlparser::ast::Expr` tree,
//!   compute the Postgres name for every select-list item ourselves, then
//!   synthetically alias the DataFusion-bound copy of the SQL so the
//!   planner accepts it, and finally stamp our own precomputed names back
//!   onto the result schema -- DataFusion's opinion of the name is
//!   discarded entirely for that query.
use std::sync::Arc;

use arrow_schema::{DataType as WsDataType, Field, Schema};
use datafusion::arrow::datatypes::DataType as DfDataType;
use datafusion::logical_expr::{Expr, LogicalPlan, WindowFunctionDefinition};

/// Postgres's fallback name for any select-list expression that is not a
/// bare column reference, function call, or (recursively) a cast whose
/// argument has a name of its own.
pub(crate) const PG_UNNAMED_COLUMN: &str = "?column?";

// ---------------------------------------------------------------------------
// Path A: rename from the already-planned DataFusion `LogicalPlan`.
// ---------------------------------------------------------------------------

/// Compute the Postgres output-column names for `schema`'s fields from the
/// DataFusion logical `plan` that produced it, and rename any field whose
/// current (DataFusion-display) name diverges from the Postgres rule.
///
/// Best-effort: if the plan's outermost node isn't a `Projection` (after
/// skipping the pass-through nodes `Sort` / `Limit` / `Distinct` /
/// `SubqueryAlias`, none of which change column semantics), or the
/// projection's expression count doesn't match `schema`'s field count
/// (shouldn't happen; a defensive bail-out rather than a mis-aligned
/// rename), `schema` is returned unchanged.
pub(crate) fn pg_style_column_names(schema: &Arc<Schema>, plan: &LogicalPlan) -> Arc<Schema> {
    if std::env::var_os("BASIN_DEBUG_PG_COLNAMES").is_some() {
        eprintln!("BASIN_DEBUG_PG_COLNAMES plan: {plan:?}");
    }
    let Some(exprs) = top_projection_exprs(plan) else {
        return Arc::clone(schema);
    };
    if exprs.len() != schema.fields().len() {
        return Arc::clone(schema);
    }
    let mut changed = false;
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(exprs.iter())
        .map(|(f, e)| {
            let name = pg_name_for_projection_item(e);
            if name.as_str() != f.name().as_str() {
                changed = true;
                f.as_ref().clone().with_name(name)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    if changed {
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
    } else {
        Arc::clone(schema)
    }
}

/// Walk down through plan nodes that pass their input's columns through
/// unchanged, to find the `Projection` whose `expr` list defines the
/// `SELECT` list's per-column expressions.
fn top_projection_exprs(plan: &LogicalPlan) -> Option<Vec<Expr>> {
    match plan {
        LogicalPlan::Projection(p) => {
            let exprs = resolve_pass_through_aggregate_columns(&p.expr, &p.input);
            Some(resolve_pass_through_projection_columns(&exprs, &p.input))
        }
        LogicalPlan::Sort(s) => top_projection_exprs(&s.input),
        LogicalPlan::Limit(l) => top_projection_exprs(&l.input),
        LogicalPlan::Distinct(d) => top_projection_exprs(d.input()),
        LogicalPlan::SubqueryAlias(s) => top_projection_exprs(&s.input),
        // A bare `SELECT agg(...) FROM t` (no GROUP BY, nothing else in the
        // select list to force a wrapping `Projection`) can plan straight to
        // `Aggregate` with no `Projection` on top of it -- DataFusion only
        // inserts one when it needs to reorder/rename/re-derive columns, and
        // an `Aggregate`'s own output schema (group-by columns, then
        // aggregate expressions, in that order -- see `Aggregate::
        // output_expressions` in `datafusion-expr`) is already exactly the
        // select list. Without this arm, `pg_style_column_names` fell to
        // `_ => None` for that plan shape and DataFusion's raw,
        // argument-inclusive expression text (`count(Int64(1))`, `sum(CASE
        // WHEN ...)`) reached the wire verbatim instead of Postgres's
        // bare-function-name rule.
        LogicalPlan::Aggregate(a) => {
            let mut exprs = a.group_expr.clone();
            exprs.extend(a.aggr_expr.iter().cloned());
            Some(exprs)
        }
        _ => None,
    }
}

/// Undo DataFusion's pass-through `Column` references over a directly-nested
/// `Aggregate`, so `pg_expr_display_name` sees the real
/// `Expr::AggregateFunction` it already knows how to name instead of a bare
/// column name.
///
/// The `Aggregate` arm above was written on the assumption that a bare
/// `SELECT agg(...) FROM t` (no `GROUP BY`) plans straight to
/// `LogicalPlan::Aggregate` with nothing on top. Verified live against this
/// plan (`BASIN_DEBUG_PG_COLNAMES=1`): that is false. DataFusion always
/// wraps it in a `Projection`, so the `Projection` arm above is what
/// actually fires, and that `Projection`'s own expressions are just a
/// rename/pass-through of the `Aggregate` child's output columns -- which
/// come in two shapes:
///   - `Alias(Column(inner_name), display_name)`, when DataFusion needs to
///     restore original call text that its own rewrite lost (`COUNT(*)` is
///     physically `count(Int64(1))`) -- `pg_name_for_projection_item`'s
///     `bare_call_prefix` heuristic on the alias text already handles this.
///   - a bare `Column(name)` with **no** `Alias` at all, when the
///     `Aggregate`'s own field name already equals the wanted display text
///     (`sum(t.a)`) -- and a plain column reference has no alias text to
///     pattern-match, so it fell through to `Expr::Column`'s "use the name
///     verbatim" rule and leaked `sum(t.a)`, arguments and all, onto the
///     wire.
///
/// Text-matching a second heuristic onto the bare `Column`'s name (mirroring
/// `bare_call_prefix`) would work for `sum(a)` but not `sum(a) FILTER (WHERE
/// ...)` or `count(DISTINCT a)`, whose rendered text does not end at the
/// first balanced `)`. Resolving structurally instead -- swap the `Column`
/// for the real `group_expr`/`aggr_expr` the `Aggregate` computed for that
/// output field -- fixes all of them at once, because it never re-derives a
/// name from text; it hands the original expression to the
/// `Expr::AggregateFunction` arm that already only looks at `func.name()`.
/// The same problem as [`resolve_pass_through_aggregate_columns`], one node
/// type over: an outer `Projection` whose expressions are bare `Column`
/// references into an INNER `Projection` that holds the real call.
///
/// `SELECT data -> 'a' FROM t ORDER BY id` plans as
/// `Projection([Column("json_get(t.data,Utf8(\"a\"))")])` over `Sort` over
/// `Projection([ScalarFunction(json_get)])`. The outer expression is a bare
/// column whose NAME is DataFusion's display text, so it reaches
/// `Expr::Column`'s "use the name verbatim" rule and the internal rewrite
/// target goes straight to the wire. Neither the `ScalarFunction` arm nor the
/// `Alias` arm of the naming rule ever sees it.
///
/// Doc 16 inferred this double-`Projection` shape from the code and from which
/// tests had `ORDER BY`, and flagged explicitly that it had never been dumped
/// and confirmed. It was dumped; the inference was right. That is why the
/// override table alone closed only `list_has_all` (which plans without the
/// extra projection) and left the three jsonb operators diverging.
///
/// Resolved structurally — swap the pass-through column for the inner
/// projection's own expression at the same position — rather than by
/// text-matching the name, for the reason the aggregate version gives: a name
/// heuristic works for `json_get(a,b)` and breaks on anything whose rendered
/// text does not end at the first balanced `)`.
fn resolve_pass_through_projection_columns(exprs: &[Expr], input: &LogicalPlan) -> Vec<Expr> {
    // Look through the nodes that do not change column semantics. `ORDER BY`
    // is the reason this shape exists at all, so `Sort` is the one that
    // matters, but the others are equally transparent.
    let mut inner = input;
    while let Some(next) = match inner {
        LogicalPlan::Sort(s) => Some(&*s.input),
        LogicalPlan::Limit(l) => Some(&*l.input),
        LogicalPlan::Distinct(d) => Some(d.input().as_ref()),
        LogicalPlan::SubqueryAlias(s) => Some(&*s.input),
        _ => None,
    } {
        inner = next;
    }
    let LogicalPlan::Projection(inner_proj) = inner else {
        return exprs.to_vec();
    };
    if inner_proj.expr.len() != inner_proj.schema.fields().len() {
        return exprs.to_vec();
    }
    exprs
        .iter()
        .map(|e| match e {
            Expr::Column(c) => inner_proj
                .schema
                .fields()
                .iter()
                .position(|f| f.name() == &c.name)
                .and_then(|i| inner_proj.expr.get(i))
                .cloned()
                .unwrap_or_else(|| e.clone()),
            other => other.clone(),
        })
        .collect()
}

fn resolve_pass_through_aggregate_columns(exprs: &[Expr], input: &LogicalPlan) -> Vec<Expr> {
    let LogicalPlan::Aggregate(a) = input else {
        return exprs.to_vec();
    };
    let by_name: std::collections::HashMap<&str, &Expr> = a
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .zip(a.group_expr.iter().chain(a.aggr_expr.iter()))
        .collect();
    exprs
        .iter()
        .map(|e| match e {
            Expr::Column(c) => by_name
                .get(c.name.as_str())
                .map(|real| (*real).clone())
                .unwrap_or_else(|| e.clone()),
            other => other.clone(),
        })
        .collect()
}

/// Compute the Postgres name for one top-level projection item.
///
/// A plain (unaliased) expression is handled by [`pg_expr_display_name`]
/// directly. An `Expr::Alias` is trickier: DataFusion wraps a projection
/// item in `Alias` both when the *user* wrote an explicit `AS name` *and*,
/// unprompted, when it needs to re-attach a human-readable name to a bare
/// pass-through `Column` reference into a child plan node -- e.g.
/// `SELECT count(*) FROM t` plans as `Projection[Alias(Column("count(Int64(1))"),
/// name="count(*)")] -> Aggregate[...]`, where "count(*)" is DataFusion's
/// own reconstruction of the original call text, not anything the user
/// typed. The two cases are indistinguishable from the `Expr` shape alone,
/// so we use a text heuristic: if the alias name is *itself* shaped exactly
/// like a bare function call (`ident(...)`, matching the whole string), it
/// is almost certainly one of these synthesized "restore the display text"
/// wrappers -- a real Postgres identifier essentially never contains a
/// literal `(` -- so we reduce it with the same function-name rule. Any
/// other alias text is trusted verbatim: it is either a real user alias or
/// DataFusion's synthesized name already happens to equal what the user
/// wrote (e.g. `SELECT a AS a`).
fn pg_name_for_projection_item(e: &Expr) -> String {
    match e {
        Expr::Alias(a) => {
            if let Some(name) = bare_call_prefix(&a.name) {
                // An internal rewrite target reaches this arm too, not only
                // the `Expr::ScalarFunction` one — `list_has_all` arrives here
                // as alias TEXT that `bare_call_prefix` happily reduces to the
                // bare internal name. Overriding in only one of the two paths
                // fixes the operators that happen to plan as a scalar function
                // and leaves the ones that plan as an alias still leaking.
                pg_scalar_function_display_name(&name)
                    .unwrap_or_else(|| PG_UNNAMED_COLUMN.to_string())
            } else {
                a.name.clone()
            }
        }
        other => pg_expr_display_name(other).unwrap_or_else(|| PG_UNNAMED_COLUMN.to_string()),
    }
}

/// If `s` is shaped exactly like a bare function call -- `ident` followed by
/// a single balanced, non-empty `(...)` that runs to the end of the string
/// -- return `ident`. Used only to recognise DataFusion's own synthesized
/// "restore the original call text" aliases (see
/// [`pg_name_for_projection_item`]); never applied to user-visible SQL text.
fn bare_call_prefix(s: &str) -> Option<String> {
    let open = s.find('(')?;
    if open == 0 || !s.ends_with(')') {
        return None;
    }
    let ident = &s[..open];
    if !ident
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return None;
    }
    // Confirm the parens are balanced and the outer pair closes exactly at
    // the end of the string (so "a(b)+c(d)" -- not a bare call -- is
    // rejected).
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return if i == bytes.len() - 1 {
                        let base = ident.rsplit('.').next().unwrap_or(ident);
                        (!base.is_empty()).then(|| base.to_string())
                    } else {
                        None
                    };
                }
            }
            _ => {}
        }
    }
    None
}

/// Recursively compute the Postgres name for a raw (unaliased) DataFusion
/// expression. Returns `None` for anything that maps to Postgres's generic
/// `?column?` fallback.
fn pg_expr_display_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Column(c) => Some(c.name.clone()),
        Expr::ScalarFunction(f) => pg_scalar_function_display_name(f.func.name()),
        Expr::AggregateFunction(f) => Some(f.func.name().to_string()),
        Expr::WindowFunction(w) => Some(
            match &w.fun {
                WindowFunctionDefinition::AggregateUDF(f) => f.name(),
                WindowFunctionDefinition::WindowUDF(f) => f.name(),
            }
            .to_string(),
        ),
        Expr::Cast(c) => pg_expr_display_name(&c.expr).or_else(|| pg_cast_type_name(&c.data_type)),
        Expr::TryCast(c) => {
            pg_expr_display_name(&c.expr).or_else(|| pg_cast_type_name(&c.data_type))
        }
        // A nested Alias (rare at this depth) still resolves the same way a
        // top-level one would.
        Expr::Alias(a) => bare_call_prefix(&a.name).or_else(|| Some(a.name.clone())),
        _ => None,
    }
}

/// Compute the Postgres display name for a planned `Expr::ScalarFunction`
/// named `name`, accounting for the fact that some of these calls are not
/// what the user typed at all.
///
/// Postgres's own rule (`FigureColnameInternal`) only ever looks at the
/// *parse tree*: a genuine function call gets the function's bare name; an
/// *operator* expression (`a -> b`, `a <@ b`, ...) gets `?column?`, full
/// stop, regardless of which function the operator happens to lower to
/// internally. Basin's SQL-rewrite pipeline (`pg_operators::rewrite_json_operators`,
/// `rewrite_array_op_once` in `pg_operators.rs`) turns several Postgres
/// *operators* into function-call **text** before DataFusion's parser ever
/// sees the query -- `data -> 'a'` becomes the literal string
/// `json_get(data, 'a')`. By the time this module inspects the planned
/// `Expr` tree, that rewritten call is byte-for-byte indistinguishable from
/// a user who typed `json_get(...)` directly: `pg_style_column_names` only
/// receives the already-planned `LogicalPlan` and result `Schema` (see the
/// module docs' "Path A"), never the original SQL text, so there is no way
/// to recover "this identifier came from an operator" at this call site.
///
/// The fix is a name-based table, not a structural one, because the
/// information needed (was this text an operator in the original query?)
/// was already discarded by the time an `Expr` exists. Two shapes:
///
///   - **Operator-only rewrite targets** -- functions that exist in Basin's
///     planned `Expr` tree *only* because an operator was rewritten into
///     them (`json_get`/`json_get_text` for `->`/`->>`, `json_path_extract`/
///     `json_path_extract_text` for `#>`/`#>>`, `array_contains` for
///     `@>`/`<@` on arrays and `arrays_overlap` for `&&` -- confirmed at the
///     source, `pg_operators::rewrite_array_op_once` and `udf.rs:4345-4356`)
///     -- these map to `None` (`?column?`), Postgres's actual rule for the
///     operator forms that produce them. Measured on 18.2: `SELECT ARRAY[1,2]
///     @> ARRAY[1]`, `SELECT ARRAY[1,2] && ARRAY[1]` and `SELECT
///     '{"a":1}'::jsonb @> '{"a":1}'::jsonb` all name their column `?column?`.
///     `list_has_all` stays listed even though `@>`/`<@` no longer route to
///     it: DataFusion's builtin is still reachable under that name, and it
///     reached clients as an operator target for long enough that dropping the
///     entry would silently change the name back.
///   - **`strpos`** -- the mirror image. Basin implements the *function*
///     syntax `POSITION(substr IN str)` by planning to a call literally
///     named `strpos` (DataFusion's own SQL-to-`Expr` lowering for
///     `POSITION`, confirmed live: `SELECT position('ll' IN 'hello')` plans
///     to `Expr::ScalarFunction { func: strpos, ... }`), but Postgres's
///     parser resolves `POSITION(...)` syntax to a function it displays as
///     `position` -- the two engines simply chose different *internal*
///     names for the same syntax form. Live Postgres 18.2, verified
///     directly (`psql \gdesc`):
///       `SELECT position('ll' in 'hello')`  -> column `position`
///       `SELECT strpos('hello', 'll')`      -> column `strpos`
///     Renaming `strpos` -> `position` here is therefore imprecise in the
///     same direction as the deny-list above: a query that calls Basin's
///     `strpos` UDF *directly* (rather than through `POSITION(... IN ...)`
///     syntax) would also display as `position`, which real Postgres would
///     not do. Basin does not currently have a way to tell the two apart
///     at this call site (same "no original SQL text" limitation as above),
///     and `POSITION(... IN ...)` is by far the common spelling this exists
///     to fix.
///
/// Any other scalar function name is a real, reduced function name -- use
/// it verbatim, same as before this table existed.
fn pg_scalar_function_display_name(name: &str) -> Option<String> {
    match name {
        "json_get"
        | "json_get_text"
        | "json_path_extract"
        | "json_path_extract_text"
        | "list_has_all"
        | "array_contains"
        | "arrays_overlap" => None,
        // NOT handled here, deliberately: `strpos`. Basin rewrites
        // `POSITION(x IN y)` to `strpos`, and Postgres names that column
        // `position` — but it names a directly-written `strpos(y, x)` call
        // `strpos`, verified on 18.2:
        //
        //   SELECT strpos('hello','ll');        -> column "strpos"
        //   SELECT position('ll' IN 'hello');   -> column "position"
        //
        // Both are the same function by the time they reach this shim, so a
        // table keyed on the target name cannot tell them apart. Mapping
        // `strpos` to `position` closes one divergence and opens another for
        // the spelling that is already correct. Fixing it needs the ORIGINAL
        // spelling carried from the rewrite site, which is what doc 16 means
        // by "when it came from" — not a rule that can live in this function.
        other => Some(other.to_string()),
    }
}

/// Postgres's short `pg_type.typname` for the target type of a cast whose
/// argument has no name of its own (e.g. `1::text`, `NULL::int`). Only the
/// handful of types Basin's Arrow bridge actually produces are covered;
/// anything else falls back to `?column?` via the caller, which is no worse
/// than before this fix.
fn pg_cast_type_name(dt: &DfDataType) -> Option<String> {
    let name = match dt {
        DfDataType::Int16 | DfDataType::Int8 | DfDataType::UInt8 | DfDataType::UInt16 => "int2",
        DfDataType::Int32 => "int4",
        DfDataType::Int64 | DfDataType::UInt32 | DfDataType::UInt64 => "int8",
        DfDataType::Float32 => "float4",
        DfDataType::Float64 => "float8",
        DfDataType::Utf8 | DfDataType::LargeUtf8 | DfDataType::Utf8View => "text",
        DfDataType::Boolean => "bool",
        DfDataType::Date32 | DfDataType::Date64 => "date",
        DfDataType::Timestamp(_, Some(_)) => "timestamptz",
        DfDataType::Timestamp(_, None) => "timestamp",
        DfDataType::Decimal128(_, _) | DfDataType::Decimal256(_, _) => "numeric",
        DfDataType::Binary | DfDataType::LargeBinary => "bytea",
        DfDataType::Interval(_) => "interval",
        _ => return None,
    };
    Some(name.to_string())
}

/// Same mapping as [`pg_cast_type_name`] but keyed off the *workspace*
/// (post-execution, v54) Arrow `DataType` -- used by the duplicate-name
/// retry path, which reasons about the already-materialized result schema
/// rather than DataFusion's own type system.
fn pg_cast_type_name_ws(dt: &WsDataType) -> Option<String> {
    let name = match dt {
        WsDataType::Int16 | WsDataType::Int8 | WsDataType::UInt8 | WsDataType::UInt16 => "int2",
        WsDataType::Int32 => "int4",
        WsDataType::Int64 | WsDataType::UInt32 | WsDataType::UInt64 => "int8",
        WsDataType::Float32 => "float4",
        WsDataType::Float64 => "float8",
        WsDataType::Utf8 | WsDataType::LargeUtf8 | WsDataType::Utf8View => "text",
        WsDataType::Boolean => "bool",
        WsDataType::Date32 | WsDataType::Date64 => "date",
        WsDataType::Timestamp(_, Some(_)) => "timestamptz",
        WsDataType::Timestamp(_, None) => "timestamp",
        WsDataType::Decimal128(_, _) | WsDataType::Decimal256(_, _) => "numeric",
        WsDataType::Binary | WsDataType::LargeBinary => "bytea",
        WsDataType::Interval(_) => "interval",
        _ => return None,
    };
    Some(name.to_string())
}

// ---------------------------------------------------------------------------
// Path B: duplicate select-list names ("Projections require unique
// expression names"). Postgres allows `SELECT f(5), f(5)`; DataFusion's
// `Projection` does not. We work around DataFusion's restriction by
// synthetically aliasing every top-level select-list item so the planner
// accepts it, and separately recomputing Postgres's own (possibly
// duplicate) names straight from the original SQL text -- DataFusion's
// resulting field names are not consulted at all for this query.
// ---------------------------------------------------------------------------

/// Prefix for the synthetic per-position aliases injected by
/// [`dedupe_select_list_for_planning`]. Chosen to be exceedingly unlikely to
/// collide with a real identifier.
const SYNTHETIC_ALIAS_PREFIX: &str = "__basin_pgname_";

/// Substring DataFusion's planner error carries when a `Projection` would
/// have two columns sharing a display name. Matched against
/// `DataFusionError::to_string()` by the caller.
pub(crate) const UNIQUE_PROJECTION_NAMES_ERROR: &str =
    "Projections require unique expression names";

/// If `sql` is a single top-level `SELECT` (or `SELECT ... ORDER BY / LIMIT`
/// wrapping one) sqlparser can parse, rewrite every item in its select list
/// to carry a distinct synthetic alias (`__basin_pgname_<position>`) and
/// return `(rewritten_sql, real_names)`, where `real_names[i]` is the
/// Postgres name that item should display under -- computed from the
/// *original* expression, independent of whatever DataFusion would have
/// named it. The returned `Vec<PgColNameSource>` may resolve to duplicate
/// names; that's the whole point (Postgres allows it, DataFusion's
/// `Projection` doesn't). A cast whose argument has no name of its own can't
/// be resolved here -- it needs the *executed* result column's actual Arrow
/// type, which isn't available until the retried query has run; see
/// [`pg_names_for_select_list`], called once it has.
///
/// Returns `None` when the statement isn't a shape this rewrite knows how
/// to handle (failed to parse, not a plain `SELECT`, multiple statements) --
/// the caller then surfaces DataFusion's original error unchanged.
pub(crate) fn dedupe_select_list_for_planning(sql: &str) -> Option<(String, Vec<PgColNameSource>)> {
    use sqlparser::ast::{Ident, Query, SelectItem, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let query: &mut Query = match &mut stmts[0] {
        Statement::Query(q) => q.as_mut(),
        _ => return None,
    };
    let select = match query.body.as_mut() {
        SetExpr::Select(s) => s.as_mut(),
        _ => return None,
    };
    if select.projection.is_empty() {
        return None;
    }
    let mut sources = Vec::with_capacity(select.projection.len());
    for (i, item) in select.projection.iter_mut().enumerate() {
        let source = match item {
            SelectItem::UnnamedExpr(expr) => PgColNameSource::Expr(pg_name_from_sql_expr(expr)),
            SelectItem::ExprWithAlias { alias, .. } => PgColNameSource::Fixed(alias.value.clone()),
            // Wildcards can't collide by construction (each expands to a
            // distinct real column name) -- bail out rather than guess.
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return None,
        };
        sources.push(source);
        let alias = Ident::new(format!("{SYNTHETIC_ALIAS_PREFIX}{i}"));
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e.clone(),
            SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => unreachable!(),
        };
        *item = SelectItem::ExprWithAlias { expr, alias };
    }
    Some((stmts[0].to_string(), sources))
}

/// Where a retried select-list item's final Postgres name comes from.
pub(crate) enum PgColNameSource {
    /// The item had an explicit `AS alias` (or the source text is already
    /// spoken for some other way) -- use this text verbatim.
    Fixed(String),
    /// The item was unaliased; carries the name computed from its expression
    /// by the recursive rule (bare column, function call, or a cast whose
    /// argument had a name), or a marker for the two cases that need more
    /// information than the original SQL text alone provides -- see
    /// [`NameOrCastFallback`].
    Expr(NameOrCastFallback),
}

/// Result of [`pg_name_from_sql_expr`]: either a concrete Postgres name, or
/// "no name at this level, but if this turns out to be a bare cast target
/// with an unnamed argument, resolve via the column's actual output type."
pub(crate) enum NameOrCastFallback {
    Name(String),
    /// This expression was (recursively) a cast whose argument had no name;
    /// resolve via the executed result type once known.
    CastFallback,
    /// No rule applies at all -> `?column?`, regardless of type.
    None,
}

/// Postgres's naming rule, applied to a `sqlparser` AST expression (used
/// only by the duplicate-name retry path -- see module docs).
fn pg_name_from_sql_expr(e: &sqlparser::ast::Expr) -> NameOrCastFallback {
    use crate::pg_ast::ObjectNamePartExt;
    use sqlparser::ast::Expr;
    match e {
        Expr::Identifier(ident) => NameOrCastFallback::Name(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|i| NameOrCastFallback::Name(i.value.clone()))
            .unwrap_or(NameOrCastFallback::None),
        Expr::Function(f) => f
            .name
            .0
            .last()
            .map(|p| NameOrCastFallback::Name(p.id_val().clone()))
            .unwrap_or(NameOrCastFallback::None),
        Expr::Nested(inner) => pg_name_from_sql_expr(inner),
        // `CAST(x AS t)`, `x::t`, and `TRY_CAST(x AS t)` all parse to
        // `Expr::Cast` in sqlparser (distinguished only by `kind`); the
        // Postgres naming rule treats all three the same way.
        Expr::Cast { expr, .. } => match pg_name_from_sql_expr(expr) {
            NameOrCastFallback::Name(n) => NameOrCastFallback::Name(n),
            _ => NameOrCastFallback::CastFallback,
        },
        _ => NameOrCastFallback::None,
    }
}

/// Resolve the final Postgres names for a dedup-rewritten select list, now
/// that the retried query has actually run and `result_schema` gives each
/// column's real Arrow type (needed for the cast-target-type fallback).
pub(crate) fn pg_names_for_select_list(
    sources: &[PgColNameSource],
    result_schema: &Schema,
) -> Vec<String> {
    sources
        .iter()
        .enumerate()
        .map(|(i, s)| match s {
            PgColNameSource::Fixed(name) => name.clone(),
            PgColNameSource::Expr(NameOrCastFallback::Name(name)) => name.clone(),
            PgColNameSource::Expr(NameOrCastFallback::CastFallback) => result_schema
                .fields()
                .get(i)
                .and_then(|f| pg_cast_type_name_ws(f.data_type()))
                .unwrap_or_else(|| PG_UNNAMED_COLUMN.to_string()),
            PgColNameSource::Expr(NameOrCastFallback::None) => PG_UNNAMED_COLUMN.to_string(),
        })
        .collect()
}

/// Apply precomputed `names` (from [`pg_names_for_select_list`]) to
/// `schema`'s fields positionally, ignoring whatever DataFusion named them.
/// Duplicates in `names` are preserved verbatim -- Postgres allows a result
/// set to have repeated column names; nothing downstream of this (row
/// encoding, `RowDescription`) looks a column up by name.
pub(crate) fn rename_fields(schema: &Arc<Schema>, names: &[String]) -> Arc<Schema> {
    if names.len() != schema.fields().len() {
        return Arc::clone(schema);
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(names.iter())
        .map(|(f, n)| f.as_ref().clone().with_name(n.clone()))
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    async fn ctx_with_t() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t (a INT, b INT)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx
    }

    async fn names_for(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.logical_plan().clone();
        let df_schema = df.schema().inner().clone();
        let ws_schema: Arc<Schema> = Arc::new(Schema::new(
            df_schema
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), f.is_nullable()))
                .collect::<Vec<_>>(),
        ));
        let renamed = pg_style_column_names(&ws_schema, &plan);
        renamed.fields().iter().map(|f| f.name().clone()).collect()
    }

    #[tokio::test]
    async fn bare_column_uses_column_name() {
        let ctx = ctx_with_t().await;
        assert_eq!(names_for(&ctx, "SELECT a FROM t").await, vec!["a"]);
        assert_eq!(names_for(&ctx, "SELECT t.a FROM t").await, vec!["a"]);
    }

    #[tokio::test]
    async fn function_call_uses_bare_function_name() {
        // A bare `SessionContext` only has DataFusion's builtin scalar/
        // aggregate functions registered (Basin's own UDFs, e.g.
        // `pg_typeof`, are wired up by `ProjectSession::open`, not exercised
        // here) -- `abs` / `now` / `count` are enough to cover the scalar,
        // nullary-scalar, and aggregate cases of the naming rule.
        let ctx = ctx_with_t().await;
        assert_eq!(names_for(&ctx, "SELECT abs(-1)").await, vec!["abs"]);
        assert_eq!(names_for(&ctx, "SELECT now()").await, vec!["now"]);
        assert_eq!(
            names_for(&ctx, "SELECT count(*) FROM t").await,
            vec!["count"]
        );
    }

    // Traced live with `BASIN_DEBUG_PG_COLNAMES=1`: the `Aggregate` arm in
    // `top_projection_exprs` was never reached. `SELECT sum(a) FROM t` plans
    // to `Projection[Column("sum(t.a)")] -> Aggregate[...]` — DataFusion
    // always wraps a bare aggregate query in a `Projection`, so the
    // `Projection` arm fires, and its expr is a bare `Column` (no `Alias`)
    // because the `Aggregate`'s own field name already equals the wanted
    // display text. `pg_expr_display_name`'s `Expr::AggregateFunction` arm
    // was correct all along; it just never got handed an `AggregateFunction`
    // — `resolve_pass_through_aggregate_columns` now resolves that `Column`
    // back to the real expression before naming it.
    #[tokio::test]
    async fn aggregate_with_no_wrapping_projection_uses_bare_function_name() {
        // Regression test for C1a: a bare `SELECT agg(...) FROM t` with no
        // `GROUP BY` (and no `ORDER BY`/`LIMIT`/alias to force DataFusion to
        // wrap the `Aggregate` in a `Projection`) can plan straight to
        // `LogicalPlan::Aggregate` with nothing on top of it. Before the
        // `LogicalPlan::Aggregate` arm was added to `top_projection_exprs`,
        // `pg_style_column_names` fell to its `_ => None` bail-out for this
        // shape and left DataFusion's raw, argument-inclusive expression
        // text on the wire (`count(Int64(1))`, `sum(CASE WHEN ...)`)
        // instead of Postgres's bare-function-name.
        let ctx = ctx_with_t().await;
        assert_eq!(
            names_for(&ctx, "SELECT count(*) FROM t").await,
            vec!["count"]
        );
        assert_eq!(names_for(&ctx, "SELECT sum(a) FROM t").await, vec!["sum"]);
        assert_eq!(
            names_for(&ctx, "SELECT sum(a) FILTER (WHERE a > 0) FROM t").await,
            vec!["sum"]
        );
        assert_eq!(
            names_for(&ctx, "SELECT count(DISTINCT a) FROM t").await,
            vec!["count"]
        );
        // GROUP BY still resolves the same way: the grouping column keeps
        // its own name, the aggregate gets its bare function name.
        assert_eq!(
            names_for(&ctx, "SELECT a, count(*) FROM t GROUP BY a").await,
            vec!["a", "count"]
        );
    }

    #[tokio::test]
    async fn cast_recurses_into_named_argument() {
        let ctx = ctx_with_t().await;
        // Column argument has a name -> use it, not the target type.
        assert_eq!(names_for(&ctx, "SELECT a::int FROM t").await, vec!["a"]);
        assert_eq!(names_for(&ctx, "SELECT t.a::int FROM t").await, vec!["a"]);
        // Function-call argument has a name -> use it.
        assert_eq!(names_for(&ctx, "SELECT abs(-1)::text").await, vec!["abs"]);
        // Nested cast on a column still resolves to the column's name.
        assert_eq!(
            names_for(&ctx, "SELECT a::int::text FROM t").await,
            vec!["a"]
        );
    }

    #[tokio::test]
    async fn cast_falls_back_to_type_name_for_unnamed_argument() {
        let ctx = ctx_with_t().await;
        assert_eq!(names_for(&ctx, "SELECT 1::text").await, vec!["text"]);
        assert_eq!(names_for(&ctx, "SELECT 5::bigint").await, vec!["int8"]);
        assert_eq!(names_for(&ctx, "SELECT 5::smallint").await, vec!["int2"]);
        assert_eq!(names_for(&ctx, "SELECT true::boolean").await, vec!["bool"]);
        assert_eq!(names_for(&ctx, "SELECT NULL::int").await, vec!["int4"]);
        // Binary-op argument has no name -> falls back to the target type.
        assert_eq!(
            names_for(&ctx, "SELECT (a+1)::int FROM t").await,
            vec!["int4"]
        );
    }

    #[tokio::test]
    async fn everything_else_is_question_column() {
        let ctx = ctx_with_t().await;
        assert_eq!(names_for(&ctx, "SELECT 1").await, vec!["?column?"]);
        assert_eq!(names_for(&ctx, "SELECT 1+1").await, vec!["?column?"]);
        assert_eq!(names_for(&ctx, "SELECT -1").await, vec!["?column?"]);
        assert_eq!(names_for(&ctx, "SELECT a+1 FROM t").await, vec!["?column?"]);
    }

    #[tokio::test]
    async fn explicit_alias_is_preserved_verbatim() {
        let ctx = ctx_with_t().await;
        assert_eq!(
            names_for(&ctx, "SELECT a AS x, a AS y FROM t").await,
            vec!["x", "y"]
        );
        assert_eq!(
            names_for(&ctx, "SELECT count(*) AS total FROM t").await,
            vec!["total"]
        );
    }

    // Regression test for C1c (differential-baseline.txt): Basin's SQL-rewrite
    // pipeline turns the `->`/`->>`/`#>`/`#>>` JSON operators and array
    // `@>`/`<@` into function-call *text* (`json_get(...)`, `list_has_all(...)`,
    // etc.) before DataFusion ever parses the query -- see
    // `pg_scalar_function_display_name`'s doc comment for the full mechanism.
    // These tests exercise the already-rewritten call shape directly (the
    // shape `pg_style_column_names` actually sees), matching Postgres's own
    // rule that an *operator* expression names its result column `?column?`
    // regardless of what it lowers to internally. Verified live against
    // Postgres 18.2: `SELECT data -> 'a' FROM t`, `SELECT data ->> 'name' FROM t`,
    // `SELECT data #> '{a,b}' FROM t`, and `SELECT ARRAY[1,2] <@ ARRAY[1,2,3]`
    // all report column `?column?`.

    #[tokio::test]
    async fn operator_rewrite_targets_report_question_column() {
        let ctx = ctx_with_t().await;
        crate::jsonb_udf::register_jsonb_udfs(&ctx);
        assert_eq!(
            names_for(&ctx, "SELECT json_get(a, 1) FROM t").await,
            vec!["?column?"]
        );
        assert_eq!(
            names_for(&ctx, "SELECT json_get_text(a, 1) FROM t").await,
            vec!["?column?"]
        );
        assert_eq!(
            names_for(&ctx, "SELECT json_path_extract(a, 1) FROM t").await,
            vec!["?column?"]
        );
        assert_eq!(
            names_for(&ctx, "SELECT json_path_extract_text(a, 1) FROM t").await,
            vec!["?column?"]
        );
        // `@>`/`<@` on arrays -> `list_has_all(...)` (`pg_operators.rs:978-981`).
        // `list_has_all` is a genuine DataFusion builtin (alias `array_has_all`),
        // registered by default -- no extra UDF registration needed.
        assert_eq!(
            names_for(
                &ctx,
                "SELECT list_has_all(make_array(1,2,3), make_array(1,2))"
            )
            .await,
            vec!["?column?"]
        );
    }

    // Regression test for C1c (the mirror-image half): Postgres's parser
    // resolves `POSITION(substr IN str)` syntax to a function it displays as
    // `position`; Basin plans the same syntax to a call literally named
    // `strpos` (DataFusion's own SQL-to-`Expr` lowering, not a Basin
    // rewrite). See `pg_scalar_function_display_name`'s doc comment for the
    // live-verified asymmetry with an explicit `strpos(...)` call. Verified
    // live against Postgres 18.2: `SELECT position('ll' IN 'hello')` reports
    // column `position`.
    #[tokio::test]
    /// Pins the behaviour Postgres actually has, which is the opposite of what
    /// this test asserted when it was written. Verified on 18.2:
    /// `SELECT strpos('hello','ll')` names its column `strpos`, and only
    /// `POSITION(... IN ...)` names it `position`. Since Basin rewrites the
    /// latter into the former, a directly-written `strpos` must keep its own
    /// name — renaming it here would close one divergence and open another.
    async fn a_directly_written_strpos_keeps_its_own_name() {
        let ctx = ctx_with_t().await;
        assert_eq!(
            names_for(&ctx, "SELECT strpos('hello', 'll')").await,
            vec!["strpos"]
        );
    }

    // ── duplicate-name retry path ──────────────────────────────────────

    #[test]
    fn dedupe_rewrites_unaliased_duplicates_and_records_real_names() {
        let (rewritten, sources) =
            dedupe_select_list_for_planning("SELECT abs(5), abs(5)").expect("should rewrite");
        assert!(rewritten.contains(&format!("{SYNTHETIC_ALIAS_PREFIX}0")));
        assert!(rewritten.contains(&format!("{SYNTHETIC_ALIAS_PREFIX}1")));
        assert_eq!(sources.len(), 2);
        let dummy_schema = Schema::new(vec![
            Field::new("x", WsDataType::Int64, true),
            Field::new("y", WsDataType::Int64, true),
        ]);
        let names = pg_names_for_select_list(&sources, &dummy_schema);
        assert_eq!(names, vec!["abs", "abs"]);
    }

    #[test]
    fn dedupe_preserves_explicit_aliases_including_duplicate_ones() {
        let (_, sources) =
            dedupe_select_list_for_planning("SELECT 1 AS x, 2 AS x").expect("should rewrite");
        let dummy_schema = Schema::new(vec![
            Field::new("c0", WsDataType::Int64, true),
            Field::new("c1", WsDataType::Int64, true),
        ]);
        let names = pg_names_for_select_list(&sources, &dummy_schema);
        assert_eq!(names, vec!["x", "x"]);
    }

    #[test]
    fn dedupe_cast_fallback_uses_executed_result_type() {
        let (_, sources) =
            dedupe_select_list_for_planning("SELECT 1::text, 1::text").expect("should rewrite");
        let dummy_schema = Schema::new(vec![
            Field::new("c0", WsDataType::Utf8, true),
            Field::new("c1", WsDataType::Utf8, true),
        ]);
        let names = pg_names_for_select_list(&sources, &dummy_schema);
        assert_eq!(names, vec!["text", "text"]);
    }

    #[test]
    fn rename_fields_applies_positionally_and_allows_duplicates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("abs(Int64(5))", WsDataType::Int64, true),
            Field::new("abs(Int64(5))", WsDataType::Int64, true),
        ]));
        let renamed = rename_fields(&schema, &["abs".to_string(), "abs".to_string()]);
        let names: Vec<&str> = renamed.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["abs", "abs"]);
    }
}
