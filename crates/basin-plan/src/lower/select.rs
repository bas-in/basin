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
//! `ORDER BY`, `LIMIT` / `OFFSET`, `VALUES`, a `FROM`-less `SELECT`, and
//! `UNION` / `INTERSECT` / `EXCEPT`. Everything else — CTEs, window clauses,
//! `DISTINCT` / `DISTINCT ON`, `LATERAL`, a subquery or set-returning
//! function in `FROM`, `NATURAL`/`USING` joins — returns
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
    ColId, ColumnRef, Expr, FrameBound, JoinKind, LogicalPlan, Schema, SetOpKind, SnapshotId,
    SortKey, TableId, WindowFrame,
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
    lower_select_stmt(stmt, &res)
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

fn lower_select_stmt(stmt: &SelectStmt, res: &Resolvers) -> Result<LogicalPlan, LowerError> {
    if stmt.with_clause.is_some() {
        return Err(LowerError::Unsupported(
            "WITH / common table expressions are not yet lowered".into(),
        ));
    }
    if !stmt.window_clause.is_empty() {
        return Err(LowerError::Unsupported(
            "WINDOW clauses are not yet lowered".into(),
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
        return lower_set_op(stmt, op_kind, res);
    }

    if !stmt.distinct_clause.is_empty() {
        // Postgres's own parse-tree convention: plain `DISTINCT` puts a
        // single empty placeholder node in this list; `DISTINCT ON (...)`
        // puts the real expressions.
        let is_on = stmt.distinct_clause.iter().any(|n| n.node.is_some());
        return Err(LowerError::Unsupported(if is_on {
            "DISTINCT ON is not yet lowered".into()
        } else {
            "SELECT DISTINCT is not yet lowered".into()
        }));
    }

    if !stmt.values_lists.is_empty() {
        return lower_values(stmt, res);
    }

    let from = build_from_clause(&stmt.from_clause, res)?;
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

    if raw_target.iter().any(|(e, _)| contains_window(e))
        || having_expr.as_ref().is_some_and(contains_window)
    {
        return Err(LowerError::Unsupported(
            "window functions are not yet lowered".into(),
        ));
    }

    let has_agg = !group_exprs.is_empty()
        || having_expr.is_some()
        || raw_target.iter().any(|(e, _)| e.contains_aggregate());

    if has_agg {
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
        let sorted = if order_keys.is_empty() {
            having_applied
        } else {
            LogicalPlan::Sort {
                input: Box::new(having_applied),
                keys: order_keys,
            }
        };
        let projected = LogicalPlan::Project {
            input: Box::new(sorted),
            exprs: target,
        };
        apply_limit(projected, stmt, res)
    } else {
        let order_keys = stmt
            .sort_clause
            .iter()
            .map(|n| lower_sort_by(n, &ctx))
            .collect::<Result<Vec<_>, _>>()?;
        let sorted = if order_keys.is_empty() {
            base_plan
        } else {
            LogicalPlan::Sort {
                input: Box::new(base_plan),
                keys: order_keys,
            }
        };
        let projected = LogicalPlan::Project {
            input: Box::new(sorted),
            exprs: raw_target,
        };
        apply_limit(projected, stmt, res)
    }
}

fn lower_set_op(
    stmt: &SelectStmt,
    op_kind: SetOperation,
    res: &Resolvers,
) -> Result<LogicalPlan, LowerError> {
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
    let left = lower_select_stmt(larg, res)?;
    let right = lower_select_stmt(rarg, res)?;
    let op = match op_kind {
        SetOperation::SetopUnion => SetOpKind::Union,
        SetOperation::SetopIntersect => SetOpKind::Intersect,
        SetOperation::SetopExcept => SetOpKind::Except,
        _ => return Err(LowerError::Malformed("set operation with an unknown op")),
    };
    Ok(LogicalPlan::SetOp {
        left: Box::new(left),
        right: Box::new(right),
        op,
        all: stmt.all,
    })
}

fn lower_values(stmt: &SelectStmt, res: &Resolvers) -> Result<LogicalPlan, LowerError> {
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
    apply_limit(LogicalPlan::Values { rows, schema }, stmt, res)
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

fn build_from_clause(items: &[Node], res: &Resolvers) -> Result<Option<FromBuilt>, LowerError> {
    let mut iter = items.iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    let mut acc = build_from_item(first, res)?;
    for item in iter {
        let rhs = build_from_item(item, res)?;
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

fn build_from_item(item: &Node, res: &Resolvers) -> Result<FromBuilt, LowerError> {
    match item.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => build_range_var(rv, res),
        Some(NodeEnum::JoinExpr(je)) => build_join_expr(je, res),
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
) -> Result<FromBuilt, LowerError> {
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
    let left = build_from_item(larg, res)?;
    let right = build_from_item(rarg, res)?;
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
            lower_expr(n, ctx)
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
        if matches!(e, Expr::Window { .. }) {
            return Some(Err(LowerError::Unsupported(
                "window functions are not yet lowered".into(),
            )));
        }
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

    // --- Unsupported constructs ------------------------------------------------

    #[test]
    fn a_cte_is_unsupported() {
        let err = lower("WITH x AS (SELECT 1 AS a) SELECT a FROM x").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.to_lowercase().contains("with") || msg.to_lowercase().contains("cte"));
    }

    #[test]
    fn a_window_function_is_unsupported() {
        let err = lower("SELECT rank() OVER (ORDER BY a) FROM t").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

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
    fn plain_distinct_is_unsupported_with_a_precise_message() {
        let err = lower("SELECT DISTINCT a FROM t").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("DISTINCT"), "message should be precise: {msg}");
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
