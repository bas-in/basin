//! Lowering a `pg_query` `SelectStmt` into Basin's [`LogicalPlan`].
//!
//! This is the module that turns SQL text into a plan the physical engine
//! (`basin-exec/src/build.rs`) can run. Expression lowering (`lower::expr`)
//! and physical execution already exist; nothing before this increment
//! actually connected the two for a full statement.
//!
//! # Catalogs that don't exist yet
//!
//! There is no table catalog. Where one would be needed, this module defines
//! [`TableResolver`] — a trait seam exactly like `lower::expr`'s
//! [`ColumnResolver`] — instead of hardcoding table shapes. A mock resolver
//! makes lowering testable today; a real catalog implements the same trait
//! later without this file changing. Column resolution reuses
//! `lower::expr`'s [`ColumnResolver`] directly: this module's job is to build
//! one (backed by whatever the `FROM` clause put in scope), not to define a
//! second column-resolution mechanism.
//!
//! # What is in scope
//!
//! `FROM` (single table, comma-list, explicit `JOIN ... ON`), the `SELECT`
//! list (including `*` expansion), `WHERE`, `GROUP BY` / `HAVING`,
//! `ORDER BY`, `LIMIT` / `OFFSET`, `VALUES`, a `FROM`-less `SELECT`,
//! `UNION` / `INTERSECT` / `EXCEPT`, plain `DISTINCT`, `WITH` / `WITH
//! RECURSIVE` (see [`lower_with_clause`]), and window functions (`OVER
//! (...)`, see [`apply_windows`]). Everything else — a named `WINDOW`
//! clause referenced via `OVER <name>`, `DISTINCT ON`, `LATERAL`, a
//! subquery or set-returning function in `FROM`, `NATURAL`/`USING` joins, a
//! CTE's own column-alias list, a data-modifying CTE — returns
//! [`LowerError::Unsupported`] naming the construct. That is a correct
//! outcome for this increment, not a bug.
//!
//! # Equijoin extraction
//!
//! A `JOIN ... ON` condition's top-level `AND`-conjuncts are split into
//! equality conjuncts whose two sides come from exactly one side each (into
//! [`LogicalPlan::Join::on`]) and everything else (into
//! [`LogicalPlan::Join::filter`]). The same split is applied to a `WHERE`
//! clause sitting directly above a comma-joined `FROM` list (`FROM a, b
//! WHERE a.x = b.y`), converting the cross join the comma produced into the
//! equivalent hash-joinable shape. This is only sound for `Inner`/`Cross` —
//! pushing a `WHERE` conjunct into a `Left`/`Right`/`Full` join's `on` would
//! filter before null-extension instead of after, changing the answer — so
//! `WHERE`-level extraction is restricted to those two kinds. A join's own
//! `ON` clause has no such restriction: splitting its conjuncts into
//! `on`/`filter` never changes when they are evaluated, whatever the join
//! kind.
//!
//! # Aggregation as a rewrite, not a second lowering pass
//!
//! `GROUP BY`/aggregate handling lowers the `SELECT` list, `HAVING`, and
//! `ORDER BY` exactly once, against the pre-aggregation scope (so a column
//! inside `sum(x)`'s argument resolves correctly). A single Rust-level tree
//! rewrite ([`rewrite_post_agg`]) then walks each lowered expression,
//! replacing any subexpression that structurally matches a `GROUP BY` key
//! with a reference to that key's position in [`LogicalPlan::Aggregate`]'s
//! output, and any `Expr::Aggregate` call with a reference to a newly
//! allocated (or reused, by structural equality) position after the group
//! keys — mirroring `LogicalPlan::Aggregate`'s own documented output order.
//! A bare column matching neither is rejected, the same "column must appear
//! in the GROUP BY clause or be used in an aggregate function" rule Postgres
//! itself enforces. This sidesteps a much harder problem: resolving columns
//! *twice*, once pre- and once post-aggregation, would need the column
//! resolver to know which lexical position of an expression it's inside
//! (an aggregate's own argument list resolves against the pre-aggregation
//! scope even when everything around it resolves against the post-aggregation
//! one), which `ColumnResolver`'s flat `resolve(&self, parts)` signature
//! cannot express.

use pg_query::protobuf::{
    node::Node as NodeEnum, AExprKind, BoolExprType, JoinType, LimitOption, Node, SelectStmt,
    SetOperation,
};

use basin_pgtype::PgType;

use crate::lower::expr::{
    best_effort_type, lower_expr, lower_sort_by, ColumnResolver, FunctionResolver, LowerCtx,
    OperatorResolver, SubqueryLowerer,
};
use crate::lower::LowerError;
use crate::{
    ColId, ColumnRef, CteId, Expr, FrameBound, JoinKind, LogicalPlan, Schema, SetOpKind,
    SnapshotId, SortKey, TableId, WindowFrame,
};

/// Resolves a (possibly schema-qualified) table name to its catalog identity
/// and column schema.
///
/// There is no catalog to back this yet — see the module docs. A mock
/// implementation makes `FROM` lowering testable today; a real one (backed
/// by `pg_class`/`pg_attribute`) plugs in later without this file changing.
pub trait TableResolver {
    fn resolve_table(&self, name: &[String]) -> Option<(TableId, Schema)>;
}

/// Lower a `SelectStmt` parse-tree node (as produced by `pg_query::parse`,
/// or found as a [`pg_query::protobuf::SubLink::subselect`]) into a
/// [`LogicalPlan`].
pub fn lower_select(
    node: &Node,
    tables: &dyn TableResolver,
    operators: &dyn OperatorResolver,
    functions: &dyn FunctionResolver,
) -> Result<LogicalPlan, LowerError> {
    let Some(NodeEnum::SelectStmt(stmt)) = node.node.as_ref() else {
        return Err(LowerError::Malformed("expected a SelectStmt node"));
    };
    let res = Resolvers {
        tables,
        operators,
        functions,
    };
    let (plan, _schema) = lower_select_stmt(stmt, &res)?;
    Ok(plan)
}

/// The three resolver seams a statement-level lowering pass needs, bundled
/// for call-site ergonomics — the statement-level analogue of `lower::expr`'s
/// `LowerCtx` (which also carries a `ColumnResolver` and a `SubqueryLowerer`,
/// both of which vary per clause here rather than staying fixed for the
/// whole statement).
struct Resolvers<'a> {
    tables: &'a dyn TableResolver,
    operators: &'a dyn OperatorResolver,
    functions: &'a dyn FunctionResolver,
}

impl<'a> Resolvers<'a> {
    fn subqueries(&self) -> SelectSubqueries<'a> {
        SelectSubqueries {
            tables: self.tables,
            operators: self.operators,
            functions: self.functions,
        }
    }
}

/// Lowers a nested `SELECT` (scalar subquery, `EXISTS`, `IN`, ...) by
/// recursing back into [`lower_select`] with the same resolvers — this is
/// what makes `lower::expr`'s `SubqueryLowerer` seam real rather than a mock,
/// for anything that reaches this crate through a full statement.
struct SelectSubqueries<'a> {
    tables: &'a dyn TableResolver,
    operators: &'a dyn OperatorResolver,
    functions: &'a dyn FunctionResolver,
}

impl<'a> SubqueryLowerer for SelectSubqueries<'a> {
    fn lower(&self, subselect: &Node) -> Result<LogicalPlan, LowerError> {
        lower_select(subselect, self.tables, self.operators, self.functions)
    }
}

// ─── Scope: column resolution over the current FROM list ──────────────────

/// One relation contributed to the current scope by a `FROM` item: the
/// name(s) it may be qualified by (`t.col`) and its column schema, in the
/// flat order it contributes to the combined scope.
#[derive(Debug, Clone)]
struct ScopeRelation {
    /// The alias, if `AS x` was written; the table's own (unqualified) name
    /// otherwise. Postgres hides the underlying table name once an alias is
    /// given, so there is exactly one qualifier, never both.
    qualifier: String,
    schema: Schema,
}

/// The ordered, concatenated column list a clause resolves column references
/// against — built up as `FROM` items are processed, and unchanged by
/// `WHERE`/`Filter` (a predicate narrows rows, never columns).
#[derive(Debug, Clone, Default)]
struct Scope {
    relations: Vec<ScopeRelation>,
}

impl Scope {
    fn empty() -> Self {
        Scope::default()
    }

    fn single(qualifier: String, schema: Schema) -> Self {
        Scope {
            relations: vec![ScopeRelation { qualifier, schema }],
        }
    }

    fn concat(mut self, mut other: Scope) -> Self {
        self.relations.append(&mut other.relations);
        self
    }

    fn total_len(&self) -> usize {
        self.relations.iter().map(|r| r.schema.len()).sum()
    }

    /// Resolve `t.col` / `col` against this scope. A qualified reference
    /// checks only the named relation; an unqualified one searches every
    /// relation and refuses to guess if more than one has a matching column
    /// (Postgres's own "column reference is ambiguous" rule — reported here
    /// as `None`, the same as an unresolvable name, since `LowerError` has no
    /// dedicated ambiguity variant yet).
    fn resolve(&self, parts: &[String]) -> Option<ColumnRef> {
        if parts.len() >= 2 {
            let qualifier = &parts[parts.len() - 2];
            let name = parts.last()?;
            let mut offset = 0usize;
            for rel in &self.relations {
                if &rel.qualifier == qualifier {
                    let pos = rel.schema.iter().position(|(n, _)| n == name)?;
                    return Some(ColumnRef {
                        relation: 0,
                        index: (offset + pos) as u16,
                        name: name.clone(),
                    });
                }
                offset += rel.schema.len();
            }
            return None;
        }
        let name = parts.first()?;
        let mut offset = 0usize;
        let mut found = None;
        for rel in &self.relations {
            if let Some(pos) = rel.schema.iter().position(|(n, _)| n == name) {
                if found.is_some() {
                    return None; // ambiguous
                }
                found = Some(offset + pos);
            }
            offset += rel.schema.len();
        }
        found.map(|idx| ColumnRef {
            relation: 0,
            index: idx as u16,
            name: name.clone(),
        })
    }

    /// The type at a given flat column index, if any — used to give a
    /// bare-column SELECT list entry its real type in
    /// [`select_output_schema`] without needing a second, catalog-free type
    /// inference pass (`crate::schema::expr_type` cannot help here: it
    /// itself needs an input schema, which for anything rooted at a `Scan`
    /// is exactly what that module's own docs say it cannot produce without
    /// a catalog — this `Scope` already carries the real one).
    fn flat_type(&self, index: u16) -> Option<PgType> {
        let mut offset = 0u16;
        for rel in &self.relations {
            let len = rel.schema.len() as u16;
            if index < offset + len {
                return rel.schema.get((index - offset) as usize).map(|(_, ty)| *ty);
            }
            offset += len;
        }
        None
    }

    /// Every `(name, flat index)` pair `*` or `t.*` expands to.
    fn star_columns(&self, qualifier: Option<&str>) -> Vec<(String, u16)> {
        let mut out = Vec::new();
        let mut offset = 0usize;
        for rel in &self.relations {
            let matches = qualifier.is_none_or(|q| rel.qualifier == q);
            if matches {
                for (i, (name, _)) in rel.schema.iter().enumerate() {
                    out.push((name.clone(), (offset + i) as u16));
                }
            }
            offset += rel.schema.len();
        }
        out
    }
}

/// A [`ColumnResolver`] backed by a [`Scope`] — the adapter that lets
/// `lower::expr::lower_expr` resolve columns against whatever the `FROM`
/// clause put in scope, reusing that module's resolution entirely rather
/// than inventing a second column-resolution mechanism.
struct ScopeResolver<'a>(&'a Scope);

impl ColumnResolver for ScopeResolver<'_> {
    fn resolve(&self, parts: &[String]) -> Option<ColumnRef> {
        self.0.resolve(parts)
    }
}

fn expr_ctx<'a>(res: &'a Resolvers<'a>, columns: &'a dyn ColumnResolver) -> LowerCtxOwned<'a> {
    LowerCtxOwned {
        subqueries: res.subqueries(),
        columns,
        operators: res.operators,
        functions: res.functions,
    }
}

/// Owns the `SubqueryLowerer` (constructed fresh per clause from `res`) so a
/// borrowed [`LowerCtx`] can point at it without every call site juggling an
/// extra named local.
struct LowerCtxOwned<'a> {
    subqueries: SelectSubqueries<'a>,
    columns: &'a dyn ColumnResolver,
    operators: &'a dyn OperatorResolver,
    functions: &'a dyn FunctionResolver,
}

impl<'a> LowerCtxOwned<'a> {
    fn ctx(&self) -> LowerCtx<'_> {
        LowerCtx {
            columns: self.columns,
            operators: self.operators,
            functions: self.functions,
            subqueries: &self.subqueries,
        }
    }
}

// ─── Top-level statement dispatch ──────────────────────────────────────────

/// One `WITH`-list entry visible to whatever is being lowered right now — the
/// statement itself, or a later CTE in the same list. `FROM <name>` resolves
/// against this (checked before [`TableResolver`], so a CTE shadows a real
/// table of the same name, matching Postgres) rather than against a second,
/// bolted-on lookup mechanism.
#[derive(Debug, Clone)]
struct CteBinding {
    name: String,
    id: CteId,
    /// The row shape `FROM <name>` puts in scope — the exposed SELECT list's
    /// aliases, typed best-effort against the body's own `FROM` scope (see
    /// [`select_output_schema`]; a computed column such as an aggregate or
    /// window result reports [`PgType::UNKNOWN`] rather than a guess, for the
    /// same reason `Values`'s schema does).
    schema: Schema,
}

/// Lower a top-level (or nested, e.g. a `UNION` arm or a `WITH`-list body)
/// `SELECT`, with no `WITH`-list of its own currently in scope.
fn lower_select_stmt(
    stmt: &SelectStmt,
    res: &Resolvers,
) -> Result<(LogicalPlan, Schema), LowerError> {
    lower_select_stmt_ctx(stmt, res, &[])
}

/// Lower a `SELECT` with `ctes` — the `WITH`-list entries of the enclosing
/// statement (and any of its own CTEs already lowered) — visible to its
/// `FROM` clause. Dispatches to [`lower_with_clause`] when `stmt` carries its
/// own `WITH`, so a CTE's body may itself start a fresh (nested) `WITH`
/// without this recursing back into that same `with_clause`.
fn lower_select_stmt_ctx(
    stmt: &SelectStmt,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<(LogicalPlan, Schema), LowerError> {
    match stmt.with_clause.as_ref() {
        Some(with) => lower_with_clause(with, stmt, res, ctes),
        None => lower_select_stmt_body(stmt, res, ctes),
    }
}

/// Lower a `WITH [RECURSIVE] name AS (body), ... <stmt-without-its-WITH>`.
///
/// Each CTE is assigned a [`CteId`] in list order and lowered against every
/// binding that came before it (a CTE may reference an earlier sibling, not a
/// later one — the same left-to-right rule Postgres enforces) plus whatever
/// was already visible from an enclosing `WITH`. The final plan nests one
/// [`LogicalPlan::Cte`] per entry, innermost (last-defined) first, wrapping
/// the main query — [`LogicalPlan::Cte::input`] is that nesting's "everything
/// this CTE (and any later sibling) is visible to".
fn lower_with_clause(
    with: &pg_query::protobuf::WithClause,
    stmt: &SelectStmt,
    res: &Resolvers,
    outer_ctes: &[CteBinding],
) -> Result<(LogicalPlan, Schema), LowerError> {
    let mut ctes: Vec<CteBinding> = outer_ctes.to_vec();
    let mut defs: Vec<(CteId, bool, LogicalPlan)> = Vec::with_capacity(with.ctes.len());

    for node in &with.ctes {
        let Some(NodeEnum::CommonTableExpr(cte)) = node.node.as_ref() else {
            return Err(LowerError::Malformed(
                "WITH list entry is not a CommonTableExpr",
            ));
        };
        if !cte.aliascolnames.is_empty() {
            return Err(LowerError::Unsupported(
                "a CTE's own column-alias list (WITH x(a, b) AS ...) is not yet lowered".into(),
            ));
        }
        let ctequery = cte
            .ctequery
            .as_deref()
            .ok_or(LowerError::Malformed("CTE with no query"))?;
        let Some(NodeEnum::SelectStmt(cte_stmt)) = ctequery.node.as_ref() else {
            return Err(LowerError::Unsupported(
                "a data-modifying CTE (WITH x AS (INSERT/UPDATE/DELETE ...)) is not yet lowered"
                    .into(),
            ));
        };

        // IDs are assigned densely from `ctes.len()`, so they stay unique
        // across nested `WITH`s too: `outer_ctes` (if any) already occupies
        // `0..outer_ctes.len()`, and every push below extends contiguously
        // from there.
        let id = CteId(ctes.len() as u16);
        let (body, schema, recursive) = if with.recursive {
            lower_recursive_cte(cte_stmt, res, &ctes, id, &cte.ctename)?
        } else {
            let (body, schema) = lower_select_stmt_ctx(cte_stmt, res, &ctes)?;
            (body, schema, false)
        };

        ctes.push(CteBinding {
            name: cte.ctename.clone(),
            id,
            schema: schema.clone(),
        });
        defs.push((id, recursive, body));
    }

    let (input, input_schema) = lower_select_stmt_body(stmt, res, &ctes)?;

    let plan = defs
        .into_iter()
        .rev()
        .fold(input, |input, (id, recursive, body)| LogicalPlan::Cte {
            name: id,
            recursive,
            body: Box::new(body),
            input: Box::new(input),
        });
    Ok((plan, input_schema))
}

/// Lower one `WITH RECURSIVE` member. `WITH RECURSIVE` is a property of the
/// whole `WITH`-list, but Postgres only actually recurses a member that both
/// is shaped `anchor UNION [ALL] recursive-term` *and* has its recursive
/// term reference the member's own name — a member that is neither is an
/// ordinary (non-recursive) CTE that merely sits inside a `WITH RECURSIVE`
/// block, which is legal SQL and exactly what the non-`Union` branch below
/// falls back to.
///
/// For the `Union` shape, the anchor is lowered first (its schema is what
/// `basin-exec`'s recursive fixpoint loop feeds back on every iteration —
/// `build.rs`'s `build_recursive_cte`), then the recursive term is lowered
/// with `name` bound to a [`CteRef`](LogicalPlan::CteRef) carrying that same
/// [`CteId`] — this is the one shape where a name resolves to a `CteId` the
/// enclosing [`LogicalPlan::Cte`] hasn't been built yet to register, which is
/// sound only because `build_recursive_cte` re-registers that id itself,
/// once per iteration, before ever building this exact plan.
fn lower_recursive_cte(
    stmt: &SelectStmt,
    res: &Resolvers,
    ctes: &[CteBinding],
    id: CteId,
    name: &str,
) -> Result<(LogicalPlan, Schema, bool), LowerError> {
    let op_kind = SetOperation::try_from(stmt.op).unwrap_or(SetOperation::Undefined);
    if op_kind != SetOperation::SetopUnion {
        let (body, schema) = lower_select_stmt_ctx(stmt, res, ctes)?;
        return Ok((body, schema, false));
    }
    if !stmt.sort_clause.is_empty() || stmt.limit_count.is_some() || stmt.limit_offset.is_some() {
        return Err(LowerError::Unsupported(
            "ORDER BY / LIMIT directly on a WITH RECURSIVE member is not yet lowered".into(),
        ));
    }
    let larg = stmt.larg.as_deref().ok_or(LowerError::Malformed(
        "WITH RECURSIVE member with no anchor",
    ))?;
    let rarg = stmt.rarg.as_deref().ok_or(LowerError::Malformed(
        "WITH RECURSIVE member with no recursive term",
    ))?;
    let (anchor, anchor_schema) = lower_select_stmt_ctx(larg, res, ctes)?;
    let mut inner = ctes.to_vec();
    inner.push(CteBinding {
        name: name.to_string(),
        id,
        schema: anchor_schema.clone(),
    });
    let (recursive_term, _) = lower_select_stmt_ctx(rarg, res, &inner)?;
    let body = LogicalPlan::SetOp {
        left: Box::new(anchor),
        right: Box::new(recursive_term),
        op: SetOpKind::Union,
        all: stmt.all,
    };
    Ok((body, anchor_schema, true))
}

fn lower_select_stmt_body(
    stmt: &SelectStmt,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<(LogicalPlan, Schema), LowerError> {
    if !stmt.window_clause.is_empty() {
        return Err(LowerError::Unsupported(
            "a named WINDOW clause (referenced via OVER <name>) is not yet lowered".into(),
        ));
    }
    if !stmt.locking_clause.is_empty() {
        return Err(LowerError::Unsupported(
            "FOR UPDATE / FOR SHARE / FOR KEY SHARE is not yet lowered".into(),
        ));
    }
    if stmt.into_clause.is_some() {
        return Err(LowerError::Unsupported(
            "SELECT INTO is not yet lowered".into(),
        ));
    }

    let op_kind = SetOperation::try_from(stmt.op).unwrap_or(SetOperation::Undefined);
    if op_kind != SetOperation::SetopNone {
        return lower_set_op(stmt, op_kind, res, ctes);
    }

    // Postgres's own parse-tree convention: plain `DISTINCT` puts a single
    // EMPTY placeholder node in this list, while `DISTINCT ON (...)` puts the
    // real expressions. Distinguishing them by node emptiness rather than by
    // list length is what Postgres's own gram.y does.
    //
    // `DISTINCT ON` stays unsupported: it keeps the FIRST row per key group in
    // the input's current order, which is only deterministic when an ORDER BY
    // agrees with the ON list. Lowering it without checking that agreement
    // would produce a plan whose answer depends on scan order — a wrong answer
    // that changes between runs, which is worse than an honest refusal.
    let is_distinct = !stmt.distinct_clause.is_empty();
    if is_distinct && stmt.distinct_clause.iter().any(|n| n.node.is_some()) {
        return Err(LowerError::Unsupported(
            "DISTINCT ON is not yet lowered".into(),
        ));
    }

    if !stmt.values_lists.is_empty() {
        return lower_values(stmt, res);
    }

    let from = build_from_clause(&stmt.from_clause, res, ctes)?;
    let (base_plan, scope) = apply_where(from, stmt, res)?;

    let resolver = ScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();

    let group_exprs = lower_group_by(&stmt.group_clause, &ctx)?;
    let raw_target = lower_target_list(&stmt.target_list, &scope, &ctx)?;
    let having_expr = stmt
        .having_clause
        .as_deref()
        .map(|n| lower_expr(n, &ctx))
        .transpose()?;

    // Postgres permits a window function only in the SELECT list and in
    // ORDER BY — never in HAVING (checked here; ORDER BY is checked once its
    // own keys are lowered, below, since a GROUP BY query's ORDER BY isn't
    // lowered until after the post-aggregation rewrite).
    if having_expr.as_ref().is_some_and(contains_window) {
        return Err(LowerError::Unsupported(
            "window functions are not allowed in HAVING".into(),
        ));
    }

    // The CTE-exposed row shape (see `CteBinding::schema`), computed from the
    // SELECT list exactly as written — before any aggregate/window rewrite
    // renumbers it — so a bare-column entry's type comes straight from the
    // real `FROM`-clause schema and everything else best-effort-falls back to
    // `PgType::UNKNOWN` rather than a guess.
    let out_schema = select_output_schema(&raw_target, &scope);

    let has_agg = !group_exprs.is_empty()
        || having_expr.is_some()
        || raw_target.iter().any(|(e, _)| e.contains_aggregate());

    let plan = if has_agg {
        let mut aggs = Vec::new();
        let target = raw_target
            .into_iter()
            .map(|(e, alias)| Ok((rewrite_post_agg(&e, &group_exprs, &mut aggs)?, alias)))
            .collect::<Result<Vec<_>, LowerError>>()?;
        let having = having_expr
            .map(|e| rewrite_post_agg(&e, &group_exprs, &mut aggs))
            .transpose()?;
        let order_keys = stmt
            .sort_clause
            .iter()
            .map(|n| lower_sort_by(n, &ctx))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|k| {
                Ok(SortKey {
                    expr: rewrite_post_agg(&k.expr, &group_exprs, &mut aggs)?,
                    descending: k.descending,
                    nulls_first: k.nulls_first,
                })
            })
            .collect::<Result<Vec<_>, LowerError>>()?;
        if sort_keys_contain_window(&order_keys) {
            return Err(LowerError::Unsupported(
                "window functions in ORDER BY are not yet lowered".into(),
            ));
        }

        let agg_width = group_exprs.len() + aggs.len();
        let agg_plan = LogicalPlan::Aggregate {
            input: Box::new(base_plan),
            group: group_exprs,
            aggs,
            grouping_sets: None,
        };
        let having_applied = match having {
            Some(predicate) => LogicalPlan::Filter {
                input: Box::new(agg_plan),
                predicate,
            },
            None => agg_plan,
        };
        // Window functions are computed over the post-WHERE, post-GROUP BY
        // (and post-HAVING) row set — `windows` sees exactly the rows/columns
        // `having_applied` produces, per the module docs.
        let (windowed, target) = apply_windows(having_applied, agg_width, target);
        let sorted = if order_keys.is_empty() {
            windowed
        } else {
            LogicalPlan::Sort {
                input: Box::new(windowed),
                keys: order_keys,
            }
        };
        let projected = LogicalPlan::Project {
            input: Box::new(sorted),
            exprs: target,
        };
        apply_limit(apply_distinct(projected, is_distinct), stmt, res)?
    } else {
        let order_keys = stmt
            .sort_clause
            .iter()
            .map(|n| lower_sort_by(n, &ctx))
            .collect::<Result<Vec<_>, _>>()?;
        if sort_keys_contain_window(&order_keys) {
            return Err(LowerError::Unsupported(
                "window functions in ORDER BY are not yet lowered".into(),
            ));
        }

        let base_width = scope.total_len();
        let (windowed, target) = apply_windows(base_plan, base_width, raw_target);
        let sorted = if order_keys.is_empty() {
            windowed
        } else {
            LogicalPlan::Sort {
                input: Box::new(windowed),
                keys: order_keys,
            }
        };
        let projected = LogicalPlan::Project {
            input: Box::new(sorted),
            exprs: target,
        };
        apply_limit(apply_distinct(projected, is_distinct), stmt, res)?
    };
    Ok((plan, out_schema))
}

/// Wrap `plan` in `DISTINCT` when the statement asked for it.
///
/// Position matters. `DISTINCT` applies to the SELECT LIST, so it sits ABOVE
/// the projection and BELOW the limit: `SELECT DISTINCT x ... LIMIT 5` means
/// five distinct rows, not the distinct rows of the first five. Putting the
/// limit underneath would silently return fewer rows than the query asked for.
fn apply_distinct(plan: LogicalPlan, is_distinct: bool) -> LogicalPlan {
    if is_distinct {
        LogicalPlan::Distinct {
            input: Box::new(plan),
            on: None,
        }
    } else {
        plan
    }
}

fn lower_set_op(
    stmt: &SelectStmt,
    op_kind: SetOperation,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<(LogicalPlan, Schema), LowerError> {
    if !stmt.sort_clause.is_empty() || stmt.limit_count.is_some() || stmt.limit_offset.is_some() {
        return Err(LowerError::Unsupported(
            "ORDER BY / LIMIT directly on a UNION/INTERSECT/EXCEPT result is not yet lowered"
                .into(),
        ));
    }
    let larg = stmt
        .larg
        .as_deref()
        .ok_or(LowerError::Malformed("set operation with no left arm"))?;
    let rarg = stmt
        .rarg
        .as_deref()
        .ok_or(LowerError::Malformed("set operation with no right arm"))?;
    let (left, left_schema) = lower_select_stmt_ctx(larg, res, ctes)?;
    let (right, _right_schema) = lower_select_stmt_ctx(rarg, res, ctes)?;
    let op = match op_kind {
        SetOperation::SetopUnion => SetOpKind::Union,
        SetOperation::SetopIntersect => SetOpKind::Intersect,
        SetOperation::SetopExcept => SetOpKind::Except,
        _ => return Err(LowerError::Malformed("set operation with an unknown op")),
    };
    // Postgres always takes the left (leftmost) arm's column names/types for
    // a set operation's own output — `crate::schema::output_schema`'s
    // `SetOp` branch documents the same rule; matched here rather than
    // reused because that module cannot yet resolve a `Scan`'s schema at all
    // (no catalog — see its own module docs), which every real `FROM` clause
    // bottoms out at.
    Ok((
        LogicalPlan::SetOp {
            left: Box::new(left),
            right: Box::new(right),
            op,
            all: stmt.all,
        },
        left_schema,
    ))
}

fn lower_values(stmt: &SelectStmt, res: &Resolvers) -> Result<(LogicalPlan, Schema), LowerError> {
    let scope = Scope::empty();
    let resolver = ScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();

    let mut rows = Vec::with_capacity(stmt.values_lists.len());
    let mut width = None;
    for row_node in &stmt.values_lists {
        let Some(NodeEnum::List(l)) = row_node.node.as_ref() else {
            return Err(LowerError::Malformed("VALUES row is not a list"));
        };
        let row = l
            .items
            .iter()
            .map(|n| lower_expr(n, &ctx))
            .collect::<Result<Vec<_>, _>>()?;
        match width {
            None => width = Some(row.len()),
            Some(w) if w != row.len() => {
                return Err(LowerError::Unsupported(
                    "VALUES rows with differing arity are not yet lowered".into(),
                ));
            }
            _ => {}
        }
        rows.push(row);
    }
    let width = width.unwrap_or(0);
    let schema: Schema = (0..width)
        .map(|i| {
            let ty = rows
                .iter()
                .map(|r| best_effort_type(&r[i]))
                .find(|t| !t.is_unknown())
                .unwrap_or(PgType::UNKNOWN);
            (format!("column{}", i + 1), ty)
        })
        .collect();
    let plan = apply_limit(
        LogicalPlan::Values {
            rows,
            schema: schema.clone(),
        },
        stmt,
        res,
    )?;
    Ok((plan, schema))
}

fn apply_limit(
    plan: LogicalPlan,
    stmt: &SelectStmt,
    res: &Resolvers,
) -> Result<LogicalPlan, LowerError> {
    if stmt.limit_count.is_none() && stmt.limit_offset.is_none() {
        return Ok(plan);
    }
    // LIMIT/OFFSET expressions cannot reference table columns — Postgres
    // itself rejects that at parse analysis — so an empty scope is correct
    // here, not merely convenient.
    let scope = Scope::empty();
    let resolver = ScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();

    let fetch = stmt
        .limit_count
        .as_deref()
        .map(|n| lower_expr(n, &ctx))
        .transpose()?;
    let skip = stmt
        .limit_offset
        .as_deref()
        .map(|n| lower_expr(n, &ctx))
        .transpose()?;
    let with_ties = LimitOption::try_from(stmt.limit_option).unwrap_or(LimitOption::Undefined)
        == LimitOption::WithTies;
    Ok(LogicalPlan::Limit {
        input: Box::new(plan),
        skip,
        fetch,
        with_ties,
    })
}

// ─── FROM / JOIN ────────────────────────────────────────────────────────────

/// A `FROM` item (or fold of items) lowered so far.
struct FromBuilt {
    plan: LogicalPlan,
    scope: Scope,
    /// `Some(n)` exactly when `plan` is `LogicalPlan::Join { kind: Inner |
    /// Cross, .. }` at the root, with `n` the number of columns the left
    /// side contributes. This is what lets a `WHERE`-clause equijoin
    /// conjunct be folded into that join's `on` list: `WHERE`-level
    /// extraction is only sound for `Inner`/`Cross` (see the module docs),
    /// so any other join kind carries `None` here even though it is itself a
    /// `LogicalPlan::Join`.
    top_join_left_len: Option<usize>,
}

fn build_from_clause(
    items: &[Node],
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<Option<FromBuilt>, LowerError> {
    let mut iter = items.iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    let mut acc = build_from_item(first, res, ctes)?;
    for item in iter {
        let rhs = build_from_item(item, res, ctes)?;
        let left_len = acc.scope.total_len();
        let plan = LogicalPlan::Join {
            left: Box::new(acc.plan),
            right: Box::new(rhs.plan),
            kind: JoinKind::Cross,
            on: vec![],
            filter: None,
        };
        let scope = acc.scope.concat(rhs.scope);
        acc = FromBuilt {
            plan,
            scope,
            top_join_left_len: Some(left_len),
        };
    }
    Ok(Some(acc))
}

fn build_from_item(
    item: &Node,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<FromBuilt, LowerError> {
    match item.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => build_range_var(rv, res, ctes),
        Some(NodeEnum::JoinExpr(je)) => build_join_expr(je, res, ctes),
        Some(NodeEnum::RangeSubselect(_)) => Err(LowerError::Unsupported(
            "a subquery in FROM is not yet lowered".into(),
        )),
        Some(NodeEnum::RangeFunction(_)) => Err(LowerError::Unsupported(
            "a set-returning function in FROM is not yet lowered".into(),
        )),
        Some(_) => Err(LowerError::Unsupported(
            "this FROM item is not yet lowered".into(),
        )),
        None => Err(LowerError::Malformed("empty FROM item")),
    }
}

fn build_range_var(
    rv: &pg_query::protobuf::RangeVar,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<FromBuilt, LowerError> {
    // A CTE shadows a real table of the same (unqualified) name, matching
    // Postgres — checked first, and only for an unqualified name, since a
    // CTE never lives in a schema. The most recently defined binding with
    // this name wins, which only matters for `WITH RECURSIVE`'s own
    // self-reference (`lower_recursive_cte` pushes a same-named binding on
    // top of whatever an outer `WITH` already carries).
    if rv.schemaname.is_empty() {
        if let Some(cte) = ctes.iter().rev().find(|c| c.name == rv.relname) {
            let qualifier = rv
                .alias
                .as_ref()
                .map(|a| a.aliasname.clone())
                .unwrap_or_else(|| rv.relname.clone());
            let plan = LogicalPlan::CteRef {
                name: cte.id,
                schema: cte.schema.clone(),
            };
            let scope = Scope::single(qualifier, cte.schema.clone());
            return Ok(FromBuilt {
                plan,
                scope,
                top_join_left_len: None,
            });
        }
    }
    let mut parts = Vec::new();
    if !rv.schemaname.is_empty() {
        parts.push(rv.schemaname.clone());
    }
    parts.push(rv.relname.clone());
    let (table, schema) = res
        .tables
        .resolve_table(&parts)
        .ok_or_else(|| LowerError::UnknownName(parts.join(".")))?;
    let qualifier = rv
        .alias
        .as_ref()
        .map(|a| a.aliasname.clone())
        .unwrap_or_else(|| rv.relname.clone());
    let projection = (0..schema.len() as u16).map(ColId).collect();
    let plan = LogicalPlan::Scan {
        table,
        projection,
        filters: vec![],
        // No transaction manager exists yet to hand out a real snapshot —
        // see `SnapshotId`'s own docs ("today there is exactly one snapshot
        // per statement"). `0` is a placeholder for that one snapshot, not a
        // guess at a real value.
        snapshot: SnapshotId(0),
    };
    let scope = Scope::single(qualifier, schema);
    Ok(FromBuilt {
        plan,
        scope,
        top_join_left_len: None,
    })
}

fn build_join_expr(
    je: &pg_query::protobuf::JoinExpr,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<FromBuilt, LowerError> {
    if je.is_natural {
        return Err(LowerError::Unsupported(
            "NATURAL JOIN is not yet lowered".into(),
        ));
    }
    if !je.using_clause.is_empty() {
        return Err(LowerError::Unsupported(
            "JOIN ... USING is not yet lowered".into(),
        ));
    }
    let jt = JoinType::try_from(je.jointype).unwrap_or(JoinType::Undefined);
    let kind = match jt {
        JoinType::JoinInner => JoinKind::Inner,
        JoinType::JoinLeft => JoinKind::Left,
        JoinType::JoinRight => JoinKind::Right,
        JoinType::JoinFull => JoinKind::Full,
        other => {
            return Err(LowerError::Unsupported(format!(
                "{other:?} joins are not yet lowered"
            )))
        }
    };

    let larg = je
        .larg
        .as_deref()
        .ok_or(LowerError::Malformed("JOIN with no left side"))?;
    let rarg = je
        .rarg
        .as_deref()
        .ok_or(LowerError::Malformed("JOIN with no right side"))?;
    let left = build_from_item(larg, res, ctes)?;
    let right = build_from_item(rarg, res, ctes)?;
    let left_len = left.scope.total_len();
    let scope = left.scope.concat(right.scope);
    let total_len = scope.total_len();

    let resolver = ScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();

    // `CROSS JOIN` and a plain `INNER JOIN` with no `ON` both arrive as
    // `JoinInner` with `quals: None`; Basin models both as `JoinKind::Cross`,
    // matching what an unconditional inner join actually is.
    let (on, filter, effective_kind) = match je.quals.as_deref() {
        Some(q) => {
            let (on, leftover) = split_equijoin_conjuncts(q, left_len, total_len, &ctx)?;
            (on, and_together(leftover, res.operators)?, kind)
        }
        None => (
            vec![],
            None,
            if kind == JoinKind::Inner {
                JoinKind::Cross
            } else {
                kind
            },
        ),
    };
    let top_join_left_len =
        matches!(effective_kind, JoinKind::Inner | JoinKind::Cross).then_some(left_len);
    let plan = LogicalPlan::Join {
        left: Box::new(left.plan),
        right: Box::new(right.plan),
        kind: effective_kind,
        on,
        filter,
    };
    Ok(FromBuilt {
        plan,
        scope,
        top_join_left_len,
    })
}

// ─── WHERE ──────────────────────────────────────────────────────────────────

fn apply_where(
    from: Option<FromBuilt>,
    stmt: &SelectStmt,
    res: &Resolvers,
) -> Result<(LogicalPlan, Scope), LowerError> {
    let (plan, scope, top_join_left_len) = match from {
        Some(f) => (f.plan, f.scope, f.top_join_left_len),
        None => (
            LogicalPlan::Empty {
                produce_one_row: true,
                schema: vec![],
            },
            Scope::empty(),
            None,
        ),
    };
    let Some(where_node) = stmt.where_clause.as_deref() else {
        return Ok((plan, scope));
    };

    let resolver = ScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();

    match (top_join_left_len, plan) {
        (
            Some(left_len),
            LogicalPlan::Join {
                left,
                right,
                kind,
                on: existing_on,
                filter: existing_filter,
            },
        ) => {
            let total_len = scope.total_len();
            let (mut new_on, mut leftover) =
                split_equijoin_conjuncts(where_node, left_len, total_len, &ctx)?;
            let mut on = existing_on;
            on.append(&mut new_on);
            if let Some(f) = existing_filter {
                leftover.insert(0, f);
            }
            let filter = and_together(leftover, res.operators)?;
            // A `Cross` join means "unconditional" by convention; once a
            // `WHERE`-clause equijoin (or leftover filter) attaches to it,
            // it is an `Inner` join in every way that matters downstream
            // (`basin-exec`'s hash-join builder among them), so relabel it
            // rather than leaving a `Cross` that secretly carries a
            // condition.
            let kind = if kind == JoinKind::Cross && (!on.is_empty() || filter.is_some()) {
                JoinKind::Inner
            } else {
                kind
            };
            let new_plan = LogicalPlan::Join {
                left,
                right,
                kind,
                on,
                filter,
            };
            Ok((new_plan, scope))
        }
        (_, plan) => {
            let conjuncts = flatten_and_conjuncts(where_node);
            let mut exprs = Vec::with_capacity(conjuncts.len());
            for node in conjuncts {
                let e = lower_expr(node, &ctx)?;
                if e.contains_aggregate() {
                    return Err(LowerError::Unsupported(
                        "aggregate functions are not allowed in WHERE".into(),
                    ));
                }
                if contains_window(&e) {
                    return Err(LowerError::Unsupported(
                        "window functions are not allowed in WHERE".into(),
                    ));
                }
                exprs.push(e);
            }
            let predicate = and_together(exprs, res.operators)?
                .expect("a present WHERE clause lowers to at least one conjunct");
            Ok((
                LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                },
                scope,
            ))
        }
    }
}

/// Split a raw quals node's top-level `AND`-conjuncts into equijoin pairs
/// (one side's columns entirely within `[0, left_len)`, the other's entirely
/// within `[left_len, total_len)`) and everything else. The equijoin side's
/// right-hand expression is rebased so its column indices are relative to
/// the right input's own schema, matching what `LogicalPlan::Join::on`
/// expects (see `basin-exec/src/build.rs`'s `column_index`, which reads an
/// `on` pair's indices directly with no join-relative offset).
/// The equijoin conjuncts extracted for `on`, and everything left over.
type SplitConjuncts = (Vec<(Expr, Expr)>, Vec<Expr>);

fn split_equijoin_conjuncts(
    node: &Node,
    left_len: usize,
    total_len: usize,
    ctx: &LowerCtx,
) -> Result<SplitConjuncts, LowerError> {
    let mut on = Vec::new();
    let mut leftover = Vec::new();
    for conjunct in flatten_and_conjuncts(node) {
        let e = lower_expr(conjunct, ctx)?;
        if e.contains_aggregate() {
            return Err(LowerError::Unsupported(
                "aggregate functions are not allowed in a join condition".into(),
            ));
        }
        if contains_window(&e) {
            return Err(LowerError::Unsupported(
                "window functions are not allowed in a join condition".into(),
            ));
        }
        if is_raw_equality(conjunct) {
            if let Expr::Binary { lhs, rhs, .. } = &e {
                if side_range(lhs, 0, left_len) && side_range(rhs, left_len, total_len) {
                    on.push(((**lhs).clone(), rebase_columns(rhs, left_len)));
                    continue;
                }
                if side_range(rhs, 0, left_len) && side_range(lhs, left_len, total_len) {
                    on.push(((**rhs).clone(), rebase_columns(lhs, left_len)));
                    continue;
                }
            }
        }
        leftover.push(e);
    }
    Ok((on, leftover))
}

/// Whether `node` is a raw `A_Expr` for the `=` operator — checked on the
/// parse tree rather than on the lowered `Expr` so this doesn't need to
/// re-resolve an operator just to compare its `OpId` against "whatever `=`
/// resolves to here".
fn is_raw_equality(node: &Node) -> bool {
    let Some(NodeEnum::AExpr(ae)) = node.node.as_ref() else {
        return false;
    };
    if AExprKind::try_from(ae.kind).unwrap_or(AExprKind::Undefined) != AExprKind::AexprOp {
        return false;
    }
    matches!(
        ae.name.last().and_then(|n| n.node.as_ref()),
        Some(NodeEnum::String(s)) if s.sval == "="
    )
}

/// Flatten a run of top-level `AND`s in a raw parse-tree node into its leaf
/// conjuncts, without lowering them. Doing this before lowering (rather than
/// on the lowered `Expr` tree) sidesteps needing to compare `OpId`s to tell a
/// lowered `AND` apart from any other binary operator.
fn flatten_and_conjuncts(node: &Node) -> Vec<&Node> {
    if let Some(NodeEnum::BoolExpr(be)) = node.node.as_ref() {
        if BoolExprType::try_from(be.boolop).unwrap_or(BoolExprType::Undefined)
            == BoolExprType::AndExpr
        {
            return be.args.iter().flat_map(flatten_and_conjuncts).collect();
        }
    }
    vec![node]
}

/// Whether every column reference inside `expr` falls within `[lo, hi)`, and
/// there is at least one — an expression with no column references at all
/// (a bare literal) must not count as belonging to either side, or `x = 5`
/// would be wrongly folded into a join's `on` list.
fn side_range(expr: &Expr, lo: usize, hi: usize) -> bool {
    let mut cols = Vec::new();
    collect_columns(expr, &mut cols);
    !cols.is_empty() && cols.iter().all(|i| (lo..hi).contains(i))
}

fn collect_columns(expr: &Expr, out: &mut Vec<usize>) {
    if let Expr::Column(cr) = expr {
        out.push(cr.index as usize);
    }
    expr.for_each_child(&mut |c| collect_columns(c, out));
}

/// AND a list of already-lowered boolean expressions together, folding left
/// the same way `lower::expr::lower_bool_expr` folds a parsed run of `AND`s.
/// `None` for an empty list — "no leftover conjuncts" is not the same as "a
/// leftover `TRUE`".
fn and_together(
    exprs: Vec<Expr>,
    operators: &dyn OperatorResolver,
) -> Result<Option<Expr>, LowerError> {
    let mut iter = exprs.into_iter();
    let Some(mut acc) = iter.next() else {
        return Ok(None);
    };
    for next in iter {
        let op = operators
            .resolve("AND", Some(best_effort_type(&acc)), best_effort_type(&next))
            .ok_or_else(|| {
                LowerError::NoMatchingOperator("no boolean AND operator available".into())
            })?;
        acc = Expr::Binary {
            op,
            lhs: Box::new(acc),
            rhs: Box::new(next),
        };
    }
    Ok(Some(acc))
}

// ─── GROUP BY / HAVING / SELECT list ───────────────────────────────────────

fn lower_group_by(group_clause: &[Node], ctx: &LowerCtx) -> Result<Vec<Expr>, LowerError> {
    group_clause
        .iter()
        .map(|n| {
            if matches!(n.node.as_ref(), Some(NodeEnum::GroupingSet(_))) {
                return Err(LowerError::Unsupported(
                    "ROLLUP / CUBE / GROUPING SETS are not yet lowered".into(),
                ));
            }
            let e = lower_expr(n, ctx)?;
            if contains_window(&e) {
                return Err(LowerError::Unsupported(
                    "window functions are not allowed in GROUP BY".into(),
                ));
            }
            Ok(e)
        })
        .collect()
}

fn contains_window(e: &Expr) -> bool {
    e.any(&mut |x| matches!(x, Expr::Window { .. }))
}

fn lower_target_list(
    target_list: &[Node],
    scope: &Scope,
    ctx: &LowerCtx,
) -> Result<Vec<(Expr, String)>, LowerError> {
    let mut out = Vec::new();
    for item in target_list {
        let Some(NodeEnum::ResTarget(rt)) = item.node.as_ref() else {
            return Err(LowerError::Malformed(
                "SELECT list entry is not a ResTarget",
            ));
        };
        let val = rt.val.as_deref().ok_or(LowerError::Malformed(
            "SELECT list entry with no expression",
        ))?;

        if let Some(NodeEnum::ColumnRef(cr)) = val.node.as_ref() {
            if let Some(qualifier) = star_marker(cr)? {
                for (name, index) in scope.star_columns(qualifier.as_deref()) {
                    out.push((
                        Expr::Column(ColumnRef {
                            relation: 0,
                            index,
                            name: name.clone(),
                        }),
                        name,
                    ));
                }
                continue;
            }
        }

        let expr = lower_expr(val, ctx)?;
        if expr.contains_srf() {
            return Err(LowerError::Unsupported(
                "set-returning functions in the SELECT list are not yet lowered".into(),
            ));
        }
        let alias = if !rt.name.is_empty() {
            rt.name.clone()
        } else {
            default_alias(&expr)
        };
        out.push((expr, alias));
    }
    Ok(out)
}

/// `Some(None)` for a bare `*`, `Some(Some(qualifier))` for `t.*`, `None` for
/// anything that isn't a star reference at all.
fn star_marker(cr: &pg_query::protobuf::ColumnRef) -> Result<Option<Option<String>>, LowerError> {
    let Some(last) = cr.fields.last() else {
        return Ok(None);
    };
    if !matches!(last.node.as_ref(), Some(NodeEnum::AStar(_))) {
        return Ok(None);
    }
    match cr.fields.len() {
        1 => Ok(Some(None)),
        2 => match cr.fields[0].node.as_ref() {
            Some(NodeEnum::String(s)) => Ok(Some(Some(s.sval.clone()))),
            _ => Err(LowerError::Malformed(
                "qualified `*` has a non-name qualifier",
            )),
        },
        _ => Err(LowerError::Unsupported(
            "a multiply-qualified `*` (e.g. `db.t.*`) is not yet lowered".into(),
        )),
    }
}

/// Postgres's own default column name for an unaliased target-list entry,
/// simplified the same way `crate::schema`'s private `display_name` is: a
/// bare column keeps its name, a cast passes the name of what it wraps
/// through, everything else is `?column?`.
fn default_alias(expr: &Expr) -> String {
    match expr {
        Expr::Column(cr) => cr.name.clone(),
        Expr::Cast { arg, .. } => default_alias(arg),
        _ => "?column?".to_string(),
    }
}

/// The row shape a `SELECT` list exposes to whatever references it by name —
/// used to build a [`CteBinding`]. Computed straight from `target` (as
/// written, before any aggregate/window rewrite touches it) and `scope` (the
/// real `FROM`-clause schema), so a bare-column entry gets its exact type and
/// everything else best-effort-falls back to [`PgType::UNKNOWN`] the same way
/// [`lower_values`]'s own schema does — see [`Scope::flat_type`]'s docs for
/// why this doesn't reuse `crate::schema`'s inference instead.
fn select_output_schema(target: &[(Expr, String)], scope: &Scope) -> Schema {
    target
        .iter()
        .map(|(e, alias)| (alias.clone(), best_effort_column_type(e, scope)))
        .collect()
}

fn best_effort_column_type(e: &Expr, scope: &Scope) -> PgType {
    match e {
        Expr::Column(cr) => scope.flat_type(cr.index).unwrap_or(PgType::UNKNOWN),
        _ => best_effort_type(e),
    }
}

// ─── WINDOW ─────────────────────────────────────────────────────────────────

/// Collect every distinct (by structural equality) `Expr::Window` used
/// anywhere inside `expr`, in first-encounter order — the window analogue of
/// how [`rewrite_post_agg`] allocates an `Expr::Aggregate` a slot. A found
/// window is treated as a leaf (its own children are not walked into):
/// Postgres rejects a window call nested inside another window call's
/// PARTITION BY/ORDER BY/args at parse analysis, so there is never a second,
/// deeper one to find, and stopping here keeps this the mirror image of
/// [`rewrite_post_window`], which must also stop there.
fn collect_windows(expr: &Expr, out: &mut Vec<Expr>) {
    if matches!(expr, Expr::Window { .. }) {
        if !out.contains(expr) {
            out.push(expr.clone());
        }
        return;
    }
    expr.for_each_child(&mut |c| collect_windows(c, out));
}

/// Replace every `Expr::Window` inside `expr` with a `Column` reference into
/// the [`LogicalPlan::Window`] node(s) [`stack_windows`] built for `flat`, at
/// `base_width + <its position in flat>` — `base_width` is `input`'s own
/// width (see `apply_windows`), and every stacked `Window` node appends its
/// columns after that in `flat`'s order, so this offset is correct
/// regardless of which physical node actually computes a given window.
fn rewrite_post_window(expr: &Expr, base_width: usize, flat: &[Expr]) -> Expr {
    try_transform(expr, &mut |e| {
        if matches!(e, Expr::Window { .. }) {
            let pos = flat
                .iter()
                .position(|w| w == e)
                .expect("every Expr::Window was collected into `flat` before this rewrite ran");
            return Some(Ok(Expr::Column(ColumnRef {
                relation: 0,
                index: (base_width + pos) as u16,
                name: "?column?".to_string(),
            })));
        }
        None
    })
    .expect("rewrite_post_window's callback never returns Err")
}

/// Group `windows` (already deduplicated by [`collect_windows`]) by shared
/// PARTITION BY/ORDER BY, preserving the order each distinct spec was first
/// seen and each window's relative order within its group.
/// `basin-exec/src/build.rs`'s `window_keys` requires every expression inside
/// one `LogicalPlan::Window` node to agree on both (the operator computes one
/// partitioning per node, so disagreement would silently mis-window one of
/// them) — this grouping is what decides which windows share a node and
/// which get their own.
fn group_by_window_spec(windows: Vec<Expr>) -> Vec<Vec<Expr>> {
    let mut groups: Vec<Vec<Expr>> = Vec::new();
    for w in windows {
        let Expr::Window {
            partition_by,
            order_by,
            ..
        } = &w
        else {
            unreachable!("collect_windows only ever collects Expr::Window");
        };
        let existing = groups.iter_mut().find(|g| {
            let Expr::Window {
                partition_by: p2,
                order_by: o2,
                ..
            } = &g[0]
            else {
                unreachable!("collect_windows only ever collects Expr::Window");
            };
            p2 == partition_by && o2 == order_by
        });
        match existing {
            Some(g) => g.push(w),
            None => groups.push(vec![w]),
        }
    }
    groups
}

/// Stack one `Sort` + [`LogicalPlan::Window`] pair per distinct spec in
/// `groups` above `input`. The `Sort` exists because `basin-exec/src/window.rs`
/// says, prominently, that `WindowAgg` never sorts its own input and expects
/// it already ordered by PARTITION BY then ORDER BY — omitting this produces
/// a plan that runs and returns a plausible-looking wrong answer rather than
/// an error, exactly the failure mode that module's docs call out. Groups
/// are stacked (each on top of the last) rather than run side by side because
/// each needs its OWN sort order, which a single shared input could not
/// satisfy for more than one group at a time.
fn stack_windows(input: LogicalPlan, groups: Vec<Vec<Expr>>) -> LogicalPlan {
    let mut plan = input;
    for group in groups {
        let Expr::Window {
            partition_by,
            order_by,
            ..
        } = &group[0]
        else {
            unreachable!("collect_windows only ever collects Expr::Window");
        };
        // PARTITION BY has no ASC/DESC of its own in SQL — any order that
        // keeps one partition's rows contiguous is correct, so a fixed
        // ascending/nulls-last convention is used here.
        let mut keys: Vec<SortKey> = partition_by
            .iter()
            .map(|e| SortKey {
                expr: e.clone(),
                descending: false,
                nulls_first: false,
            })
            .collect();
        keys.extend(order_by.iter().cloned());
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys,
        };
        plan = LogicalPlan::Window {
            input: Box::new(plan),
            windows: group,
        };
    }
    plan
}

/// Extract every window-function call inside `target`'s expressions into one
/// or more `Sort` + [`LogicalPlan::Window`] pairs stacked above `input` (see
/// [`stack_windows`]), and rewrite `target` to reference their output columns
/// instead of carrying `Expr::Window` directly. `input`'s own width
/// (`base_width`) is where the appended window columns start — the module
/// docs on `LogicalPlan::Window`'s position (between the input and the
/// projection) is what fixes `input` here to `base_plan` (no `GROUP BY`) or
/// the post-`HAVING` aggregate output (`GROUP BY` present), never anything
/// already carrying the query's own final `ORDER BY`/`Project`.
fn apply_windows(
    input: LogicalPlan,
    base_width: usize,
    target: Vec<(Expr, String)>,
) -> (LogicalPlan, Vec<(Expr, String)>) {
    let mut collected = Vec::new();
    for (e, _) in &target {
        collect_windows(e, &mut collected);
    }
    if collected.is_empty() {
        return (input, target);
    }
    let groups = group_by_window_spec(collected);
    let flat: Vec<Expr> = groups.iter().flatten().cloned().collect();
    let plan = stack_windows(input, groups);
    let target = target
        .into_iter()
        .map(|(e, alias)| (rewrite_post_window(&e, base_width, &flat), alias))
        .collect();
    (plan, target)
}

fn sort_keys_contain_window(keys: &[SortKey]) -> bool {
    keys.iter().any(|k| contains_window(&k.expr))
}

// ─── Aggregate output rewriting ────────────────────────────────────────────

/// Rewrite `expr` (lowered against the pre-aggregation scope) to reference
/// [`LogicalPlan::Aggregate`]'s own output instead — see the module docs for
/// why this is a Rust-level tree rewrite rather than a second lowering pass.
fn rewrite_post_agg(expr: &Expr, group: &[Expr], aggs: &mut Vec<Expr>) -> Result<Expr, LowerError> {
    try_transform(expr, &mut |e| {
        if let Some(pos) = group.iter().position(|g| g == e) {
            return Some(Ok(Expr::Column(ColumnRef {
                relation: 0,
                index: pos as u16,
                name: default_alias(e),
            })));
        }
        if matches!(e, Expr::Aggregate { .. }) {
            let pos = match aggs.iter().position(|a| a == e) {
                Some(p) => p,
                None => {
                    aggs.push(e.clone());
                    aggs.len() - 1
                }
            };
            return Some(Ok(Expr::Column(ColumnRef {
                relation: 0,
                index: (group.len() + pos) as u16,
                name: "?column?".to_string(),
            })));
        }
        if let Expr::Column(cr) = e {
            return Some(Err(LowerError::Unsupported(format!(
                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                cr.name
            ))));
        }
        // `Expr::Window` is deliberately NOT special-cased here (unlike
        // `Expr::Aggregate` above): a window function's own PARTITION
        // BY/ORDER BY/args are computed over the post-aggregation row set
        // too (`SELECT a, rank() OVER (PARTITION BY a) FROM t GROUP BY a` —
        // the window's `a` must resolve to the SAME group-key column the
        // rest of the SELECT list does), so returning `None` here lets the
        // default recursion below walk into its children and apply this same
        // rewrite to each, exactly like any other compound expression.
        // `apply_windows` (called once this whole rewrite is done) is what
        // extracts the now-rewritten `Expr::Window` nodes themselves into a
        // `LogicalPlan::Window`.
        None
    })
}

/// Rebase every `Column` inside `expr` by subtracting `offset` from its
/// index — what turns a join-side expression indexed against the *combined*
/// scope into one indexed against just that side's own schema, which is what
/// `LogicalPlan::Join::on` expects (see `split_equijoin_conjuncts`'s docs).
fn rebase_columns(expr: &Expr, offset: usize) -> Expr {
    try_transform(expr, &mut |e| match e {
        Expr::Column(cr) => Some(Ok(Expr::Column(ColumnRef {
            relation: cr.relation,
            index: cr.index - offset as u16,
            name: cr.name.clone(),
        }))),
        _ => None,
    })
    .expect("rebase_columns's callback never returns Err")
}

/// Rebuild `expr`, replacing any node where `f` returns `Some` and otherwise
/// recursing into its children. The one place [`rewrite_post_agg`] and
/// [`rebase_columns`] walk `Expr`'s full shape, written once so neither has
/// to duplicate the traversal.
fn try_transform(
    expr: &Expr,
    f: &mut impl FnMut(&Expr) -> Option<Result<Expr, LowerError>>,
) -> Result<Expr, LowerError> {
    if let Some(replaced) = f(expr) {
        return replaced;
    }
    Ok(match expr {
        Expr::Column(_) | Expr::Literal(..) | Expr::Parameter { .. } => expr.clone(),
        Expr::Unary { op, arg } => Expr::Unary {
            op: *op,
            arg: Box::new(try_transform(arg, f)?),
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op: *op,
            lhs: Box::new(try_transform(lhs, f)?),
            rhs: Box::new(try_transform(rhs, f)?),
        },
        Expr::Cast { arg, to, kind } => Expr::Cast {
            arg: Box::new(try_transform(arg, f)?),
            to: *to,
            kind: *kind,
        },
        Expr::Case {
            operand,
            whens,
            else_,
        } => Expr::Case {
            operand: operand
                .as_deref()
                .map(|o| try_transform(o, f))
                .transpose()?
                .map(Box::new),
            whens: whens
                .iter()
                .map(|(w, t)| Ok((try_transform(w, f)?, try_transform(t, f)?)))
                .collect::<Result<Vec<_>, LowerError>>()?,
            else_: else_
                .as_deref()
                .map(|e| try_transform(e, f))
                .transpose()?
                .map(Box::new),
        },
        Expr::Coalesce(xs) => Expr::Coalesce(
            xs.iter()
                .map(|x| try_transform(x, f))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::IsNull { arg, negated } => Expr::IsNull {
            arg: Box::new(try_transform(arg, f)?),
            negated: *negated,
        },
        Expr::BoolTest { arg, test } => Expr::BoolTest {
            arg: Box::new(try_transform(arg, f)?),
            test: *test,
        },
        Expr::DistinctFrom { lhs, rhs, negated } => Expr::DistinctFrom {
            lhs: Box::new(try_transform(lhs, f)?),
            rhs: Box::new(try_transform(rhs, f)?),
            negated: *negated,
        },
        Expr::InList { arg, list, negated } => Expr::InList {
            arg: Box::new(try_transform(arg, f)?),
            list: list
                .iter()
                .map(|x| try_transform(x, f))
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => Expr::Between {
            arg: Box::new(try_transform(arg, f)?),
            low: Box::new(try_transform(low, f)?),
            high: Box::new(try_transform(high, f)?),
            symmetric: *symmetric,
            negated: *negated,
        },
        Expr::Like {
            arg,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            arg: Box::new(try_transform(arg, f)?),
            pattern: Box::new(try_transform(pattern, f)?),
            escape: escape
                .as_deref()
                .map(|e| try_transform(e, f))
                .transpose()?
                .map(Box::new),
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expr::ScalarFn { func, args } => Expr::ScalarFn {
            func: *func,
            args: args
                .iter()
                .map(|a| try_transform(a, f))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => Expr::Aggregate {
            func: *func,
            args: args
                .iter()
                .map(|a| try_transform(a, f))
                .collect::<Result<Vec<_>, _>>()?,
            distinct: *distinct,
            filter: filter
                .as_deref()
                .map(|x| try_transform(x, f))
                .transpose()?
                .map(Box::new),
            order_by: order_by
                .iter()
                .map(|k| {
                    Ok(SortKey {
                        expr: try_transform(&k.expr, f)?,
                        descending: k.descending,
                        nulls_first: k.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, LowerError>>()?,
        },
        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func: *func,
            args: args
                .iter()
                .map(|a| try_transform(a, f))
                .collect::<Result<Vec<_>, _>>()?,
            partition_by: partition_by
                .iter()
                .map(|a| try_transform(a, f))
                .collect::<Result<Vec<_>, _>>()?,
            order_by: order_by
                .iter()
                .map(|k| {
                    Ok(SortKey {
                        expr: try_transform(&k.expr, f)?,
                        descending: k.descending,
                        nulls_first: k.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, LowerError>>()?,
            frame: transform_frame(frame, f)?,
        },
        Expr::SetReturning { func, args } => Expr::SetReturning {
            func: *func,
            args: args
                .iter()
                .map(|a| try_transform(a, f))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Expr::Subquery {
            kind,
            subplan,
            operand,
        } => Expr::Subquery {
            kind: *kind,
            subplan: subplan.clone(),
            operand: operand
                .as_deref()
                .map(|o| try_transform(o, f))
                .transpose()?
                .map(Box::new),
        },
        Expr::ArrayLit(xs) => Expr::ArrayLit(
            xs.iter()
                .map(|x| try_transform(x, f))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::RowLit(xs) => Expr::RowLit(
            xs.iter()
                .map(|x| try_transform(x, f))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::Subscript { arg, indices } => Expr::Subscript {
            arg: Box::new(try_transform(arg, f)?),
            indices: indices
                .iter()
                .map(|i| {
                    Ok(match i {
                        crate::Subscript::Index(e) => crate::Subscript::Index(try_transform(e, f)?),
                        crate::Subscript::Slice { lower, upper } => crate::Subscript::Slice {
                            lower: lower.as_ref().map(|e| try_transform(e, f)).transpose()?,
                            upper: upper.as_ref().map(|e| try_transform(e, f)).transpose()?,
                        },
                    })
                })
                .collect::<Result<Vec<_>, LowerError>>()?,
        },
        Expr::FieldSelect { arg, field } => Expr::FieldSelect {
            arg: Box::new(try_transform(arg, f)?),
            field: *field,
        },
    })
}

fn transform_frame(
    frame: &WindowFrame,
    f: &mut impl FnMut(&Expr) -> Option<Result<Expr, LowerError>>,
) -> Result<WindowFrame, LowerError> {
    let mut xform_bound = |b: &FrameBound| -> Result<FrameBound, LowerError> {
        Ok(match b {
            FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow => FrameBound::CurrentRow,
            FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
            FrameBound::Preceding(e) => FrameBound::Preceding(Box::new(try_transform(e, f)?)),
            FrameBound::Following(e) => FrameBound::Following(Box::new(try_transform(e, f)?)),
        })
    };
    Ok(WindowFrame {
        units: frame.units,
        start: xform_bound(&frame.start)?,
        end: xform_bound(&frame.end)?,
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Datum;
    use crate::SnapshotId;
    use basin_pgtype::Oid;
    use std::collections::HashMap;

    // --- Mock resolvers ------------------------------------------------------

    #[derive(Default)]
    struct MockTables {
        tables: HashMap<String, (TableId, Schema)>,
    }

    impl MockTables {
        fn with(mut self, name: &str, id: u32, schema: &[(&str, PgType)]) -> Self {
            self.tables.insert(
                name.to_string(),
                (
                    TableId(id),
                    schema.iter().map(|(n, t)| (n.to_string(), *t)).collect(),
                ),
            );
            self
        }
    }

    impl TableResolver for MockTables {
        fn resolve_table(&self, name: &[String]) -> Option<(TableId, Schema)> {
            let last = name.last()?;
            self.tables.get(last).cloned()
        }
    }

    /// An operator resolver backed by the real `pg_operator` table, plus the
    /// synthetic `AND`/`OR`/`NOT` names that table deliberately doesn't carry
    /// — the same wrapper `lower::expr`'s own tests use, reproduced here
    /// because it is `#[cfg(test)]`-private there. A real `OperatorResolver`
    /// will need this exact same wrapper for the exact same reason.
    struct CatalogOperators;

    impl OperatorResolver for CatalogOperators {
        fn resolve(&self, name: &str, left: Option<PgType>, right: PgType) -> Option<crate::OpId> {
            match name {
                "AND" | "OR" | "NOT" => Some(crate::OpId(Oid(u32::MAX))),
                _ => {
                    let left_oid = left.map(|t| t.oid);
                    basin_pgtype::operator::resolve(name, left_oid, right.oid)
                        .map(|sig| crate::OpId(sig.oid))
                }
            }
        }
    }

    struct MockFunctions;

    impl FunctionResolver for MockFunctions {
        fn resolve(
            &self,
            name: &[String],
            _args: &[PgType],
        ) -> Option<(crate::FuncId, crate::lower::expr::FuncKind)> {
            use crate::lower::expr::FuncKind;
            match name.last().map(String::as_str) {
                Some("sum") => Some((crate::FuncId(Oid(2108)), FuncKind::Aggregate)),
                Some("count") => Some((crate::FuncId(Oid(2803)), FuncKind::Aggregate)),
                Some("upper") => Some((crate::FuncId(Oid(871)), FuncKind::Scalar)),
                Some("rank") => Some((crate::FuncId(Oid(3100)), FuncKind::Window)),
                Some("generate_series") => Some((crate::FuncId(Oid(1066)), FuncKind::SetReturning)),
                _ => None,
            }
        }
    }

    fn t_schema() -> Vec<(&'static str, PgType)> {
        vec![
            ("id", PgType::INT4),
            ("a", PgType::INT4),
            ("b", PgType::TEXT),
        ]
    }

    fn u_schema() -> Vec<(&'static str, PgType)> {
        vec![
            ("id", PgType::INT4),
            ("t_id", PgType::INT4),
            ("c", PgType::TEXT),
        ]
    }

    fn tables() -> MockTables {
        MockTables::default()
            .with("t", 100, &t_schema())
            .with("u", 200, &u_schema())
    }

    fn lower(sql: &str) -> Result<LogicalPlan, LowerError> {
        let result = pg_query::parse(sql).expect("parse failed");
        let raw = result.protobuf.stmts.first().expect("no stmt").clone();
        let node = *raw.stmt.expect("no stmt node");
        lower_select(&node, &tables(), &CatalogOperators, &MockFunctions)
    }

    fn col(index: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: name.to_string(),
        })
    }

    fn int_lit(v: i32) -> Expr {
        Expr::Literal(Datum::Int32(v), PgType::INT4)
    }

    // --- 1: FROM a single table -> Scan -------------------------------------

    #[test]
    fn from_single_table_scans_with_a_full_projection() {
        let plan = lower("SELECT id, a, b FROM t").unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "id"), "id".to_string()),
                (col(1, "a"), "a".to_string()),
                (col(2, "b"), "b".to_string()),
            ]
        );
        let LogicalPlan::Scan {
            table,
            projection,
            filters,
            snapshot,
        } = *input
        else {
            panic!("expected Scan under Project");
        };
        assert_eq!(table, TableId(100));
        assert_eq!(projection, vec![ColId(0), ColId(1), ColId(2)]);
        assert!(filters.is_empty());
        assert_eq!(snapshot, SnapshotId(0));
    }

    #[test]
    fn an_unknown_table_is_an_unknown_name_error() {
        let err = lower("SELECT 1 FROM nope").unwrap_err();
        assert!(matches!(err, LowerError::UnknownName(_)));
    }

    // --- 2: SELECT list -> Project, including `*` expansion ----------------

    #[test]
    fn star_expands_to_every_column_of_the_input() {
        let plan = lower("SELECT * FROM t").unwrap();
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "id"), "id".to_string()),
                (col(1, "a"), "a".to_string()),
                (col(2, "b"), "b".to_string()),
            ]
        );
    }

    #[test]
    fn an_unaliased_non_column_expr_defaults_to_question_column() {
        let plan = lower("SELECT a + 1 FROM t").unwrap();
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0].1, "?column?");
    }

    #[test]
    fn an_explicit_alias_is_used_over_the_default() {
        let plan = lower("SELECT a AS renamed FROM t").unwrap();
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0], (col(1, "a"), "renamed".to_string()));
    }

    // --- 3: WHERE -> Filter --------------------------------------------------

    #[test]
    fn where_lowers_to_a_filter_under_the_project() {
        let plan = lower("SELECT id FROM t WHERE a > 1").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { input, predicate } = *input else {
            panic!("expected Filter under Project");
        };
        let Expr::Binary { lhs, rhs, .. } = predicate else {
            panic!("expected a Binary predicate");
        };
        assert_eq!(*lhs, col(1, "a"));
        assert_eq!(*rhs, int_lit(1));
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn an_aggregate_in_where_is_rejected() {
        let err = lower("SELECT id FROM t WHERE sum(a) > 1").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("WHERE"), "message should mention WHERE: {msg}");
    }

    // --- 4: GROUP BY / HAVING -> Aggregate + Filter -------------------------

    #[test]
    fn group_by_and_a_bare_aggregate_build_an_aggregate_node() {
        let plan = lower("SELECT a, sum(id) FROM t GROUP BY a").unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        // group position 0 -> "a", agg position 1 (group.len()=1 + 0) -> sum(id)
        assert_eq!(exprs[0], (col(0, "a"), "a".to_string()));
        assert_eq!(exprs[1].0, col(1, "?column?"));

        let LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } = *input
        else {
            panic!("expected Aggregate under Project");
        };
        assert_eq!(group, vec![col(1, "a")]); // "a" is base-scope index 1
        assert_eq!(aggs.len(), 1);
        let Expr::Aggregate { func, args, .. } = &aggs[0] else {
            panic!("expected an Aggregate call");
        };
        assert_eq!(*func, crate::FuncId(Oid(2108)));
        assert_eq!(*args, vec![col(0, "id")]);
        assert!(grouping_sets.is_none());
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn a_bare_aggregate_with_no_group_by_still_builds_an_aggregate_node() {
        let plan = lower("SELECT count(id) FROM t").unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0].0, col(0, "?column?"));
        let LogicalPlan::Aggregate { group, aggs, .. } = *input else {
            panic!("expected Aggregate");
        };
        assert!(group.is_empty());
        assert_eq!(aggs.len(), 1);
    }

    #[test]
    fn having_lowers_to_a_filter_above_the_aggregate_reusing_the_same_agg_slot() {
        let plan = lower("SELECT a, sum(id) FROM t GROUP BY a HAVING sum(id) > 10").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { input, predicate } = *input else {
            panic!("expected Filter (HAVING) under Project");
        };
        let Expr::Binary { lhs, rhs, .. } = predicate else {
            panic!("expected a Binary predicate");
        };
        // Reuses the same aggregate slot the SELECT list's sum(id) uses
        // (group.len()==1, agg position 0 -> index 1), rather than adding a
        // second, duplicate aggregate.
        assert_eq!(*lhs, col(1, "?column?"));
        assert_eq!(*rhs, int_lit(10));

        let LogicalPlan::Aggregate { aggs, .. } = *input else {
            panic!("expected Aggregate under the HAVING Filter");
        };
        assert_eq!(
            aggs.len(),
            1,
            "HAVING's sum(id) must reuse the SELECT list's aggregate slot, not add a second one"
        );
    }

    #[test]
    fn a_column_outside_group_by_and_not_aggregated_is_rejected() {
        let err = lower("SELECT a, b FROM t GROUP BY a").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("GROUP BY"), "message should explain: {msg}");
    }

    #[test]
    fn rollup_is_unsupported() {
        let err = lower("SELECT a, sum(id) FROM t GROUP BY ROLLUP (a)").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

    // --- 5: ORDER BY -> Sort, with Postgres's null defaults -----------------

    #[test]
    fn order_by_asc_defaults_to_nulls_last() {
        let plan = lower("SELECT a FROM t ORDER BY a").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Sort { keys, .. } = *input else {
            panic!("expected Sort under Project");
        };
        assert_eq!(keys.len(), 1);
        assert!(!keys[0].descending);
        assert!(!keys[0].nulls_first, "ASC must default to NULLS LAST");
    }

    #[test]
    fn order_by_desc_defaults_to_nulls_first() {
        let plan = lower("SELECT a FROM t ORDER BY a DESC").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Sort { keys, .. } = *input else {
            panic!("expected Sort under Project");
        };
        assert_eq!(keys.len(), 1);
        assert!(keys[0].descending);
        assert!(keys[0].nulls_first, "DESC must default to NULLS FIRST");
    }

    #[test]
    fn order_by_can_reference_a_column_not_in_the_select_list() {
        // Sort must sit below the Project so it can still see `b`, which the
        // final projection drops.
        let plan = lower("SELECT a FROM t ORDER BY b").unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs, vec![(col(1, "a"), "a".to_string())]);
        let LogicalPlan::Sort { keys, .. } = *input else {
            panic!("expected Sort under Project");
        };
        assert_eq!(keys[0].expr, col(2, "b"));
    }

    #[test]
    fn order_by_after_aggregation_references_the_aggregate_output() {
        let plan = lower("SELECT a, sum(id) FROM t GROUP BY a ORDER BY sum(id)").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Sort { input, keys } = *input else {
            panic!("expected Sort under Project");
        };
        assert_eq!(keys[0].expr, col(1, "?column?")); // same agg slot as the SELECT list
        assert!(matches!(*input, LogicalPlan::Aggregate { .. }));
    }

    // --- 6: LIMIT / OFFSET -> Limit ------------------------------------------

    #[test]
    fn limit_and_offset_wrap_the_final_plan() {
        let plan = lower("SELECT a FROM t LIMIT 10 OFFSET 5").unwrap();
        let LogicalPlan::Limit {
            input,
            skip,
            fetch,
            with_ties,
        } = plan
        else {
            panic!("expected Limit");
        };
        assert_eq!(fetch, Some(int_lit(10)));
        assert_eq!(skip, Some(int_lit(5)));
        assert!(!with_ties);
        assert!(matches!(*input, LogicalPlan::Project { .. }));
    }

    #[test]
    fn limit_with_no_offset_leaves_skip_none() {
        let plan = lower("SELECT a FROM t LIMIT 10").unwrap();
        let LogicalPlan::Limit { skip, fetch, .. } = plan else {
            panic!("expected Limit");
        };
        assert!(skip.is_none());
        assert_eq!(fetch, Some(int_lit(10)));
    }

    #[test]
    fn no_limit_or_offset_produces_no_limit_node() {
        let plan = lower("SELECT a FROM t").unwrap();
        assert!(!matches!(plan, LogicalPlan::Limit { .. }));
    }

    // --- 7: FROM a, b / explicit JOIN -> Join, with equijoin extraction ----

    #[test]
    fn explicit_join_on_extracts_the_equijoin_into_on_and_the_rest_into_filter() {
        let plan = lower("SELECT * FROM t JOIN u ON t.id = u.t_id AND t.a > 0").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } = *input
        else {
            panic!("expected Join");
        };
        assert_eq!(kind, JoinKind::Inner);
        assert_eq!(on, vec![(col(0, "id"), col(1, "t_id"))]);
        let Some(Expr::Binary { lhs, rhs, .. }) = filter else {
            panic!("expected a leftover filter");
        };
        assert_eq!(*lhs, col(1, "a"));
        assert_eq!(*rhs, int_lit(0));
        assert!(matches!(*left, LogicalPlan::Scan { .. }));
        assert!(matches!(*right, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn comma_from_with_a_where_equality_becomes_an_inner_join_with_on() {
        let plan = lower("SELECT * FROM t, u WHERE t.id = u.t_id").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Join {
            kind, on, filter, ..
        } = *input
        else {
            panic!("expected Join");
        };
        assert_eq!(
            kind,
            JoinKind::Inner,
            "an equijoin WHERE clause must promote the comma cross join to Inner"
        );
        assert_eq!(on, vec![(col(0, "id"), col(1, "t_id"))]);
        assert!(filter.is_none());
    }

    #[test]
    fn comma_from_with_a_non_equijoin_where_still_carries_the_filter() {
        // No equijoin conjunct to extract into `on`, but the residual WHERE
        // predicate still attaches as the join's `filter` — and once a
        // cross join carries a condition at all it is relabelled `Inner`
        // (see the module docs: `Cross` means unconditional by convention).
        let plan = lower("SELECT * FROM t, u WHERE t.a > 0").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Join {
            kind, on, filter, ..
        } = *input
        else {
            panic!("expected Join");
        };
        assert_eq!(kind, JoinKind::Inner);
        assert!(on.is_empty());
        let Some(Expr::Binary { lhs, rhs, .. }) = filter else {
            panic!("expected a filter");
        };
        assert_eq!(*lhs, col(1, "a"));
        assert_eq!(*rhs, int_lit(0));
    }

    #[test]
    fn cross_join_keyword_with_no_condition_is_a_cross_join() {
        let plan = lower("SELECT * FROM t CROSS JOIN u").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Join {
            kind, on, filter, ..
        } = *input
        else {
            panic!("expected Join");
        };
        assert_eq!(kind, JoinKind::Cross);
        assert!(on.is_empty());
        assert!(filter.is_none());
    }

    #[test]
    fn left_join_on_still_splits_on_and_filter() {
        // Splitting a JOIN's own ON clause is safe for any join kind (see the
        // module docs) — only WHERE-level extraction is restricted to
        // Inner/Cross.
        let plan = lower("SELECT * FROM t LEFT JOIN u ON t.id = u.t_id AND t.a > 0").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Join {
            kind, on, filter, ..
        } = *input
        else {
            panic!("expected Join");
        };
        assert_eq!(kind, JoinKind::Left);
        assert_eq!(on, vec![(col(0, "id"), col(1, "t_id"))]);
        assert!(filter.is_some());
    }

    // --- 8: VALUES -> Values; SELECT with no FROM -> Empty ------------------

    #[test]
    fn values_lowers_to_a_values_plan_with_positional_column_names() {
        let plan = lower("VALUES (1, 'x'), (2, 'y')").unwrap();
        let LogicalPlan::Values { rows, schema } = plan else {
            panic!("expected Values");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(schema[0].0, "column1");
        assert_eq!(schema[1].0, "column2");
        assert_eq!(schema[0].1, PgType::INT4);
    }

    #[test]
    fn select_with_no_from_is_a_projection_over_empty() {
        let plan = lower("SELECT 1 + 1 AS two").unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0].1, "two");
        let LogicalPlan::Empty {
            produce_one_row,
            schema,
        } = *input
        else {
            panic!("expected Empty under Project");
        };
        assert!(produce_one_row);
        assert!(schema.is_empty());
    }

    // --- 9: Set operations -> SetOp ------------------------------------------

    #[test]
    fn union_lowers_to_set_op_union_with_all_false_by_default() {
        let plan = lower("SELECT a FROM t UNION SELECT t_id FROM u").unwrap();
        let LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } = plan
        else {
            panic!("expected SetOp");
        };
        assert_eq!(op, SetOpKind::Union);
        assert!(!all);
        assert!(matches!(*left, LogicalPlan::Project { .. }));
        assert!(matches!(*right, LogicalPlan::Project { .. }));
    }

    #[test]
    fn union_all_sets_the_all_flag() {
        let plan = lower("SELECT a FROM t UNION ALL SELECT t_id FROM u").unwrap();
        let LogicalPlan::SetOp { all, .. } = plan else {
            panic!("expected SetOp");
        };
        assert!(all);
    }

    #[test]
    fn intersect_and_except_map_to_their_own_kinds() {
        let plan = lower("SELECT a FROM t INTERSECT SELECT t_id FROM u").unwrap();
        let LogicalPlan::SetOp { op, .. } = plan else {
            panic!("expected SetOp");
        };
        assert_eq!(op, SetOpKind::Intersect);

        let plan = lower("SELECT a FROM t EXCEPT SELECT t_id FROM u").unwrap();
        let LogicalPlan::SetOp { op, .. } = plan else {
            panic!("expected SetOp");
        };
        assert_eq!(op, SetOpKind::Except);
    }

    // --- 10: Window functions -> Window (+ inserted Sort), between input and Project ---

    #[test]
    fn window_sits_between_the_input_and_the_projection_with_an_inserted_sort() {
        let plan =
            lower("SELECT id, rank() OVER (PARTITION BY a ORDER BY b) FROM t").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0], (col(0, "id"), "id".to_string()));
        // `t` is 3 columns wide (id, a, b); the window's output column is
        // appended right after it.
        assert_eq!(exprs[1].0, col(3, "?column?"));

        let LogicalPlan::Window { input, windows } = *input else {
            panic!("expected Window directly under Project, got {input:?}");
        };
        assert_eq!(windows.len(), 1);
        let Expr::Window {
            func,
            partition_by,
            order_by,
            ..
        } = &windows[0]
        else {
            panic!("expected an Expr::Window, got {:?}", windows[0]);
        };
        assert_eq!(*func, crate::FuncId(Oid(3100)));
        assert_eq!(*partition_by, vec![col(1, "a")]);
        assert_eq!(order_by.len(), 1);
        assert_eq!(order_by[0].expr, col(2, "b"));

        // The operator never sorts its own input (`basin-exec/src/window.rs`),
        // so the planner must insert one: PARTITION BY keys first, then
        // ORDER BY keys, immediately beneath the Window node.
        let LogicalPlan::Sort { input, keys } = *input else {
            panic!("expected an inserted Sort directly under Window, got {input:?}");
        };
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].expr, col(1, "a"));
        assert_eq!(keys[1].expr, col(2, "b"));
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn window_calls_with_differing_specs_get_their_own_stacked_window_nodes() {
        // `window_keys` in `basin-exec/src/build.rs` rejects two window
        // expressions with disagreeing PARTITION BY/ORDER BY inside one
        // `LogicalPlan::Window` node, so calls with different specs must land
        // in separate, stacked nodes rather than one shared node.
        let plan = lower(
            "SELECT rank() OVER (PARTITION BY a ORDER BY b), \
                    rank() OVER (PARTITION BY b ORDER BY a) FROM t",
        )
        .expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0].0, col(3, "?column?"));
        assert_eq!(exprs[1].0, col(4, "?column?"));

        let LogicalPlan::Window { input, windows } = *input else {
            panic!("expected an outer Window node");
        };
        assert_eq!(windows.len(), 1, "differing specs must not share a node");
        let LogicalPlan::Sort { input, keys } = *input else {
            panic!("expected a Sort under the outer Window");
        };
        assert_eq!(keys[0].expr, col(2, "b"));
        assert_eq!(keys[1].expr, col(1, "a"));

        let LogicalPlan::Window { input, windows } = *input else {
            panic!("expected an inner Window node");
        };
        assert_eq!(windows.len(), 1);
        let LogicalPlan::Sort { input, keys } = *input else {
            panic!("expected a Sort under the inner Window");
        };
        assert_eq!(keys[0].expr, col(1, "a"));
        assert_eq!(keys[1].expr, col(2, "b"));
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn window_over_grouped_rows_sits_above_the_aggregate_below_the_projection() {
        let plan = lower("SELECT a, sum(id), rank() OVER (PARTITION BY a) FROM t GROUP BY a")
            .expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        // group.len()==1, aggs.len()==1 -> the window's own column lands at 2.
        assert_eq!(exprs[2].0, col(2, "?column?"));

        let LogicalPlan::Window { input, .. } = *input else {
            panic!("expected Window above the Aggregate");
        };
        let LogicalPlan::Sort { input, keys } = *input else {
            panic!("expected the inserted Sort under Window");
        };
        // PARTITION BY a resolves against the Aggregate's OWN output (group
        // key position 0), not the pre-aggregation scope's index 1 — window
        // functions see the post-GROUP BY row set.
        assert_eq!(keys[0].expr, col(0, "a"));
        assert!(matches!(*input, LogicalPlan::Aggregate { .. }));
    }

    #[test]
    fn a_window_function_in_having_is_rejected_with_a_precise_message() {
        let err = lower("SELECT a, sum(id) FROM t GROUP BY a HAVING rank() OVER (ORDER BY a) > 1")
            .unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("HAVING"),
            "message should mention HAVING: {msg}"
        );
    }

    #[test]
    fn a_window_function_in_where_is_rejected_with_a_precise_message() {
        let err = lower("SELECT id FROM t WHERE rank() OVER (ORDER BY a) > 1").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("WHERE"), "message should mention WHERE: {msg}");
    }

    #[test]
    fn a_named_window_clause_is_unsupported() {
        let err = lower("SELECT rank() OVER w FROM t WINDOW w AS (ORDER BY a)").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

    // --- 11: WITH / CTE -> Cte / CteRef -------------------------------------

    #[test]
    fn a_cte_lowers_to_cte_wrapping_a_cteref() {
        let plan = lower("WITH x AS (SELECT id, a FROM t) SELECT a FROM x").expect("lowers");
        let LogicalPlan::Cte {
            name,
            recursive,
            body,
            input,
        } = plan
        else {
            panic!("expected Cte at the top");
        };
        assert_eq!(name, CteId(0));
        assert!(!recursive);
        assert!(matches!(*body, LogicalPlan::Project { .. }));

        let LogicalPlan::Project { input, exprs } = *input else {
            panic!("expected the outer SELECT's own Project under Cte");
        };
        // x's exposed schema is (id, a); "a" is flat index 1.
        assert_eq!(exprs[0].0, col(1, "a"));
        let LogicalPlan::CteRef { name, schema } = *input else {
            panic!("expected FROM x to resolve to a CteRef, got {input:?}");
        };
        assert_eq!(name, CteId(0));
        assert_eq!(
            schema,
            vec![
                ("id".to_string(), PgType::INT4),
                ("a".to_string(), PgType::INT4),
            ]
        );
    }

    #[test]
    fn a_cte_referenced_twice_resolves_to_the_same_cte_id() {
        let plan = lower(
            "WITH x AS (SELECT id, a FROM t) \
             SELECT x1.a FROM x AS x1 JOIN x AS x2 ON x1.id = x2.id",
        )
        .expect("lowers");
        let LogicalPlan::Cte { name, input, .. } = plan else {
            panic!("expected Cte at the top");
        };
        assert_eq!(name, CteId(0));

        let LogicalPlan::Project { input, .. } = *input else {
            panic!("expected Project");
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("expected Join");
        };
        let LogicalPlan::CteRef { name: left_id, .. } = *left else {
            panic!("expected a CteRef on the join's left, got {left:?}");
        };
        let LogicalPlan::CteRef { name: right_id, .. } = *right else {
            panic!("expected a CteRef on the join's right, got {right:?}");
        };
        assert_eq!(left_id, CteId(0));
        assert_eq!(
            right_id,
            CteId(0),
            "both references to `x` must resolve to the SAME CteId"
        );
    }

    #[test]
    fn a_recursive_cte_sets_the_recursive_flag() {
        let plan = lower(
            "WITH RECURSIVE x AS (\
                SELECT 1 AS n \
                UNION ALL \
                SELECT n + 1 FROM x WHERE n < 5\
             ) SELECT n FROM x",
        )
        .expect("lowers");
        let LogicalPlan::Cte {
            name,
            recursive,
            body,
            ..
        } = plan
        else {
            panic!("expected Cte at the top");
        };
        assert_eq!(name, CteId(0));
        assert!(recursive, "WITH RECURSIVE must set the flag");

        let LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } = *body
        else {
            panic!("expected the recursive body to be a UNION of anchor and recursive term");
        };
        assert_eq!(op, SetOpKind::Union);
        assert!(all);
        assert!(matches!(*left, LogicalPlan::Project { .. }), "anchor");

        let LogicalPlan::Project { input, .. } = *right else {
            panic!("expected a Project for the recursive term");
        };
        let LogicalPlan::Filter { input, .. } = *input else {
            panic!("expected the recursive term's WHERE n < 5");
        };
        let LogicalPlan::CteRef { name: ref_id, .. } = *input else {
            panic!("expected the recursive term's FROM x to resolve to a CteRef");
        };
        assert_eq!(
            ref_id,
            CteId(0),
            "the recursive term must reference the SAME id as the Cte it's nested inside"
        );
    }

    #[test]
    fn a_ctes_own_column_alias_list_is_unsupported() {
        let err = lower("WITH x(n) AS (SELECT 1) SELECT n FROM x").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.to_lowercase().contains("column"),
            "message should be precise: {msg}"
        );
    }

    #[test]
    fn a_data_modifying_cte_is_unsupported() {
        let err = lower("WITH x AS (INSERT INTO t (id) VALUES (1) RETURNING id) SELECT id FROM x")
            .unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

    // --- Unsupported constructs ------------------------------------------------

    #[test]
    fn distinct_on_is_unsupported_with_a_precise_message() {
        let err = lower("SELECT DISTINCT ON (a) a, b FROM t").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("DISTINCT ON"),
            "message should be precise: {msg}"
        );
    }

    #[test]
    fn plain_distinct_lowers_to_a_distinct_node_above_the_projection() {
        let plan = lower("SELECT DISTINCT a FROM t").expect("plain DISTINCT lowers");
        let LogicalPlan::Distinct { input, on } = plan else {
            panic!("expected Distinct at the top, got {plan:?}");
        };
        assert!(on.is_none(), "plain DISTINCT has no ON list");
        // DISTINCT applies to the SELECT LIST, so it must sit ABOVE the
        // projection. Below it, the deduplication would run against the input's
        // full width and `SELECT DISTINCT a` would return one row per distinct
        // (a, b, c) rather than per distinct a.
        assert!(
            matches!(*input, LogicalPlan::Project { .. }),
            "Distinct must wrap the projection, not sit under it: {input:?}"
        );
    }

    /// `SELECT DISTINCT x ... LIMIT 5` means five DISTINCT rows, not the
    /// distinct rows of the first five. Getting the order wrong returns fewer
    /// rows than the query asked for, silently.
    #[test]
    fn distinct_sits_below_the_limit_not_above_it() {
        let plan = lower("SELECT DISTINCT a FROM t LIMIT 5").expect("lowers");
        let LogicalPlan::Limit { input, .. } = plan else {
            panic!("expected Limit outermost, got {plan:?}");
        };
        assert!(
            matches!(*input, LogicalPlan::Distinct { .. }),
            "Limit must be applied AFTER dedup: {input:?}"
        );
    }

    #[test]
    fn a_lateral_subquery_in_from_is_unsupported() {
        let err = lower("SELECT * FROM t, LATERAL (SELECT t.a) sub").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

    #[test]
    fn a_subquery_in_from_is_unsupported() {
        let err = lower("SELECT * FROM (SELECT 1 AS a) sub").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("subquery"), "message should be precise: {msg}");
    }
}
