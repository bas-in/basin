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
//! list (including `*` expansion and set-returning functions such as
//! `generate_series`/`unnest`, see [`apply_project_set`]), `WHERE`, `GROUP
//! BY` / `HAVING`, `ORDER BY`, `LIMIT` / `OFFSET`, `VALUES`, a `FROM`-less
//! `SELECT`, `UNION` / `INTERSECT` / `EXCEPT`, `DISTINCT` and `DISTINCT ON`
//! (see [`materialize_distinct_on`]), `WITH` / `WITH RECURSIVE` — including a
//! CTE's own column-alias list, `WITH x(a, b) AS ...` (see
//! [`apply_column_alias_list`]) — a bare `VALUES` list in `FROM` (also
//! subject to [`apply_column_alias_list`]), and window functions (`OVER
//! (...)`, see [`apply_windows`]) — including a named `WINDOW w AS (...)`
//! clause referenced by a bare `OVER w`, resolved by `lower::expr`'s
//! `lower_window_def`. A set operation's own trailing `ORDER BY`/`LIMIT`
//! (which apply to the whole result, under much stricter rules than an
//! ordinary `SELECT`'s) are lowered by [`lower_set_op`] /
//! [`lower_set_op_sort_key`]. Everything else — `OVER (w ...)` extending a
//! named window, `LATERAL`, a genuine subquery (anything but
//! a bare `VALUES` list) or set-returning function in `FROM`,
//! `NATURAL`/`USING` joins, a data-modifying CTE, `DISTINCT ON` combined with
//! `GROUP BY`/an aggregate, a set-returning function combined with `GROUP
//! BY`/an aggregate, or one nested inside another set-returning function's
//! own arguments — returns [`LowerError::Unsupported`] naming the construct.
//! That is a correct outcome for this increment, not a bug.
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
    SetOperation, SortByDir, SortByNulls,
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
    lower_select_with_outer(node, tables, operators, functions, None)
}

/// [`lower_select`]'s real body, plus the one seam that entry point cannot
/// expose without breaking every existing caller's signature: `outer`, the
/// enclosing query's own column resolver, non-`None` only when this is a
/// recursive call lowering a subquery's body (see [`SelectSubqueries::lower`]).
///
/// This is what lets a correlated reference (`WHERE EXISTS (SELECT 1 FROM u
/// WHERE u.id = t.id)`'s `t.id`, unresolvable against the subquery's own
/// `FROM u`) resolve against the enclosing query's scope instead of failing
/// with [`LowerError::UnknownName`] — see [`ScopeResolver`] and [`OUTER_REF`]
/// for how the resulting reference is tagged so `opt::decorrelate` can later
/// tell it apart from an ordinary, local one.
fn lower_select_with_outer(
    node: &Node,
    tables: &dyn TableResolver,
    operators: &dyn OperatorResolver,
    functions: &dyn FunctionResolver,
    outer: Option<&dyn ColumnResolver>,
) -> Result<LogicalPlan, LowerError> {
    let Some(NodeEnum::SelectStmt(stmt)) = node.node.as_ref() else {
        return Err(LowerError::Malformed("expected a SelectStmt node"));
    };
    let res = Resolvers {
        tables,
        operators,
        functions,
        outer,
    };
    let (plan, _schema) = lower_select_stmt(stmt, &res)?;
    Ok(plan)
}

/// Within a subquery lowered by [`SelectSubqueries::lower`], the
/// [`ColumnRef::relation`] value that marks a reference reaching into the
/// enclosing query's current row rather than the subquery's own `FROM`.
/// `opt::decorrelate` defines and documents this exact same convention
/// (`OUTER_REF`, also `1`) for the plan shapes it rewrites — this is the
/// producing side of that convention, not a second one; see that module's
/// "A convention this file invents" for the full rationale, and note that
/// module's own docs (as of when this constant was added) call out that no
/// real lowering path constructed this shape yet — this is that path.
const OUTER_REF: u16 = 1;

/// The three resolver seams a statement-level lowering pass needs, bundled
/// for call-site ergonomics — the statement-level analogue of `lower::expr`'s
/// `LowerCtx` (which also carries a `ColumnResolver` and a `SubqueryLowerer`,
/// both of which vary per clause here rather than staying fixed for the
/// whole statement).
struct Resolvers<'a> {
    tables: &'a dyn TableResolver,
    operators: &'a dyn OperatorResolver,
    functions: &'a dyn FunctionResolver,
    /// The enclosing query's own column resolver, when this `Resolvers` is
    /// lowering a subquery's body — `None` at the top level. Threaded
    /// through so [`ScopeResolver`] can fall back to it (see that type) and
    /// so [`Resolvers::subqueries`] can hand the *current* clause's resolver
    /// down as the next nesting level's `outer`.
    outer: Option<&'a dyn ColumnResolver>,
}

impl<'a> Resolvers<'a> {
    /// `columns` is the calling clause's own resolver (its `Scope`, wrapped
    /// — already falling back to `self.outer` itself, if any) — handed to a
    /// nested subquery as *its* `outer`, one level further in. This is the
    /// single seam that turns a fixed, statement-wide `outer` into the
    /// correctly-nested chain a multiply-nested correlated subquery needs.
    fn subqueries(&self, columns: &'a dyn ColumnResolver) -> SelectSubqueries<'a> {
        SelectSubqueries {
            tables: self.tables,
            operators: self.operators,
            functions: self.functions,
            outer: columns,
        }
    }
}

/// Lowers a nested `SELECT` (scalar subquery, `EXISTS`, `IN`, ...) by
/// recursing back into [`lower_select_with_outer`] with the same resolvers
/// plus `outer` (the enclosing clause's own resolver) — this is what makes
/// `lower::expr`'s `SubqueryLowerer` seam real rather than a mock, for
/// anything that reaches this crate through a full statement, and what lets
/// a correlated reference inside the subquery resolve at all.
struct SelectSubqueries<'a> {
    tables: &'a dyn TableResolver,
    operators: &'a dyn OperatorResolver,
    functions: &'a dyn FunctionResolver,
    outer: &'a dyn ColumnResolver,
}

impl<'a> SubqueryLowerer for SelectSubqueries<'a> {
    fn lower(&self, subselect: &Node) -> Result<LogicalPlan, LowerError> {
        lower_select_with_outer(
            subselect,
            self.tables,
            self.operators,
            self.functions,
            Some(self.outer),
        )
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
///
/// `outer`, when present, is tried only after `scope` itself has already
/// failed to resolve the name — an inner name always shadows an outer one of
/// the same spelling, exactly as Postgres resolves a correlated reference.
/// A hit through `outer` is re-tagged [`OUTER_REF`] rather than returned
/// as-is: whatever relation `outer` itself used is an artifact of *its own*
/// scope (`0` for an ordinary local column, or already `OUTER_REF` if
/// `outer` itself fell through to a further-enclosing query), and from this
/// subquery's point of view every one of those is equally "reaches outside
/// my own `FROM`" — the one bit `opt::decorrelate`'s convention has room to
/// express. See that module's docs for why a deeper chain collapsing to the
/// same tag is an accepted, pre-existing limit of the convention rather than
/// a bug introduced here.
struct ScopeResolver<'a> {
    scope: &'a Scope,
    outer: Option<&'a dyn ColumnResolver>,
}

impl<'a> ScopeResolver<'a> {
    fn new(scope: &'a Scope, outer: Option<&'a dyn ColumnResolver>) -> Self {
        Self { scope, outer }
    }
}

impl ColumnResolver for ScopeResolver<'_> {
    fn resolve(&self, parts: &[String]) -> Option<ColumnRef> {
        if let Some(cr) = self.scope.resolve(parts) {
            return Some(cr);
        }
        let cr = self.outer?.resolve(parts)?;
        Some(ColumnRef {
            relation: OUTER_REF,
            index: cr.index,
            name: cr.name,
        })
    }
}

fn expr_ctx<'a>(res: &'a Resolvers<'a>, columns: &'a dyn ColumnResolver) -> LowerCtxOwned<'a> {
    expr_ctx_windows(res, columns, &[])
}

/// [`expr_ctx`] for the one clause that may contain a window function and so
/// needs the statement's `WINDOW` list in scope — the `SELECT` list (and,
/// through it, `ORDER BY`, which is lowered against the same context).
fn expr_ctx_windows<'a>(
    res: &'a Resolvers<'a>,
    columns: &'a dyn ColumnResolver,
    named_windows: &'a [Node],
) -> LowerCtxOwned<'a> {
    LowerCtxOwned {
        subqueries: res.subqueries(columns),
        columns,
        operators: res.operators,
        functions: res.functions,
        named_windows,
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
    /// The statement's `WINDOW w AS (...)` list — see
    /// [`LowerCtx::named_windows`]. Empty for every clause that cannot
    /// contain a window function in the first place (`WHERE`, a join
    /// condition, `LIMIT`/`OFFSET`, `VALUES` — Postgres rejects a window
    /// call in all four), which is why [`expr_ctx`] takes it explicitly
    /// rather than reading it off a statement it would then have to be
    /// handed anyway.
    named_windows: &'a [Node],
}

impl<'a> LowerCtxOwned<'a> {
    fn ctx(&self) -> LowerCtx<'_> {
        LowerCtx {
            columns: self.columns,
            operators: self.operators,
            functions: self.functions,
            subqueries: &self.subqueries,
            named_windows: self.named_windows,
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
            lower_recursive_cte(cte_stmt, res, &ctes, id, &cte.ctename, &cte.aliascolnames)?
        } else {
            let (body, schema) = lower_select_stmt_ctx(cte_stmt, res, &ctes)?;
            let schema =
                apply_column_alias_list(schema, &cte.aliascolnames, "WITH query", &cte.ctename)?;
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
    aliascolnames: &[Node],
) -> Result<(LogicalPlan, Schema, bool), LowerError> {
    let op_kind = SetOperation::try_from(stmt.op).unwrap_or(SetOperation::Undefined);
    if op_kind != SetOperation::SetopUnion {
        let (body, schema) = lower_select_stmt_ctx(stmt, res, ctes)?;
        let schema = apply_column_alias_list(schema, aliascolnames, "WITH query", name)?;
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
    // The alias list is applied to the ANCHOR's schema before the recursive
    // term is lowered, not after the whole CTE is built: the recursive term's
    // own `FROM <name>` (its self-reference) resolves columns against
    // whatever `CteBinding::schema` says right now, so `r.n` only resolves at
    // all because this renames the anchor's raw `?column?` to `n` first.
    // Verified live: `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1
    // FROM r WHERE n < 5) SELECT n FROM r` — the recursive term references
    // `n`, which does not exist on the anchor (`SELECT 1`) under any other
    // name.
    let anchor_schema = apply_column_alias_list(anchor_schema, aliascolnames, "WITH query", name)?;
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

/// Apply a `WITH x(a, b) AS (...)` / `FROM (...) AS v(a, b)` column-alias
/// list to a body's own output schema, renaming positionally. `what`/`label`
/// only affect the error message's wording (`"WITH query"`/CTE name vs.
/// `"table"`/relation alias — Postgres phrases the two differently, checked
/// live below), not the rule itself, which is identical for both.
///
/// Verified on a live PostgreSQL 18.2 server, because the obvious guess here
/// is wrong: `WITH x(a) AS (SELECT 1 AS one, 2 AS two) SELECT * FROM x`
/// returns columns `a, two` — FEWER aliases than the body has columns is
/// *not* an error, it renames only the leading columns positionally and
/// leaves the rest under their own names. Only MORE aliases than columns is
/// rejected:
/// ```text
/// WITH x(a,b,c) AS (SELECT 1, 'hi') SELECT * FROM x;
/// ERROR:  WITH query "x" has 2 columns available but 3 columns specified
/// SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(i,s,extra);
/// ERROR:  table "v" has 2 columns available but 3 columns specified
/// ```
/// and once a name is aliased, the body's own original name for that column
/// is hidden (`WITH x(a) AS (SELECT 1 AS orig) SELECT orig FROM x` fails with
/// `column "orig" does not exist`) — which falls out for free here since this
/// overwrites the name rather than adding an alternate one.
fn apply_column_alias_list(
    mut schema: Schema,
    aliascolnames: &[Node],
    what: &str,
    label: &str,
) -> Result<Schema, LowerError> {
    if aliascolnames.is_empty() {
        return Ok(schema);
    }
    if aliascolnames.len() > schema.len() {
        return Err(LowerError::Unsupported(format!(
            "{what} \"{label}\" has {} columns available but {} columns specified",
            schema.len(),
            aliascolnames.len()
        )));
    }
    for (slot, node) in schema.iter_mut().zip(aliascolnames) {
        let Some(NodeEnum::String(s)) = node.node.as_ref() else {
            return Err(LowerError::Malformed(
                "column-alias list entry is not a name",
            ));
        };
        slot.0 = s.sval.clone();
    }
    Ok(schema)
}

fn lower_select_stmt_body(
    stmt: &SelectStmt,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<(LogicalPlan, Schema), LowerError> {
    // `WINDOW w AS (...)` itself builds nothing: it only names a window
    // definition for an `OVER w` in the SELECT list to reference, which
    // `lower::expr`'s `lower_window_def` resolves out of
    // `LowerCtx::named_windows` (threaded in below via `expr_ctx_windows`).
    // The definitions are checked to be well-formed there, at the point of
    // use — an unreferenced `WINDOW` entry is legal and simply never looked
    // at, matching a live server.
    let named_windows: &[Node] = &stmt.window_clause;
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
    let is_distinct = !stmt.distinct_clause.is_empty();
    let distinct_on_raw: Option<&[Node]> = (is_distinct
        && stmt.distinct_clause.iter().any(|n| n.node.is_some()))
    .then_some(stmt.distinct_clause.as_slice());

    if !stmt.values_lists.is_empty() {
        return lower_values(stmt, res);
    }

    let from = build_from_clause(&stmt.from_clause, res, ctes)?;
    let (base_plan, scope) = apply_where(from, stmt, res)?;

    let resolver = ScopeResolver::new(&scope, res.outer);
    let lctx = expr_ctx_windows(res, &resolver, named_windows);
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

    // A live server DOES allow a set-returning call alongside `GROUP BY`/an
    // aggregate (it expands rows AFTER aggregation finishes, over the
    // aggregated output), but that needs `apply_project_set` to run on top
    // of `having_applied` instead of `base_plan`, with its own width fixed
    // up accordingly — a second, parallel wiring this increment does not
    // add. Refusing outright keeps the refusal honest (a named
    // `Unsupported`) rather than reaching `rewrite_post_agg`, which would
    // reject a bare column inside the SRF's own arguments with a confusing
    // "must appear in GROUP BY" message that has nothing to do with the real
    // reason.
    if has_agg && raw_target.iter().any(|(e, _)| e.contains_srf()) {
        return Err(LowerError::Unsupported(
            "a set-returning function combined with GROUP BY / an aggregate function is not yet lowered".into(),
        ));
    }

    // A live server DOES allow `DISTINCT ON` alongside `GROUP BY`/an
    // aggregate (`SELECT DISTINCT ON (k) k, sum(v) FROM t GROUP BY k ORDER BY
    // k` runs fine). Supporting it needs the ON list validated against the
    // ORDER BY keys *after* `rewrite_post_agg` renumbers them (an aggregate
    // query's `ORDER BY` isn't lowered until then — see the module docs on
    // aggregation), which is extra wiring this increment does not add.
    // Refusing outright keeps the refusal honest rather than reaching
    // `materialize_distinct_on` with column indices from the wrong scope.
    if distinct_on_raw.is_some() && has_agg {
        return Err(LowerError::Unsupported(
            "DISTINCT ON combined with GROUP BY / an aggregate function is not yet lowered".into(),
        ));
    }

    let plan = if has_agg {
        let mut aggs = Vec::new();

        // `order_keys` is resolved — ordinal substitution
        // ([`lower_order_by_key`]) and the plain-`DISTINCT` membership check
        // ([`check_distinct_order_by`]) — against `raw_target` itself, the
        // SELECT list exactly as written, so both must run BEFORE
        // `raw_target` is consumed into `target` below and before either is
        // touched by `rewrite_post_agg`: Postgres checks both against the
        // pre-rewrite target list, and (for plain `DISTINCT`) reports that
        // mismatch even when the same key would independently also fail the
        // `GROUP BY` rewrite below — verified live (see
        // `check_distinct_order_by`'s own docs) that the DISTINCT error wins,
        // which only happens if this runs first.
        let raw_order_keys = stmt
            .sort_clause
            .iter()
            .map(|n| lower_order_by_key(n, &raw_target, &ctx))
            .collect::<Result<Vec<_>, _>>()?;
        if is_distinct {
            // `distinct_on_raw.is_some() && has_agg` was already refused
            // above, so a plain `is_distinct` here always means plain
            // (no-`ON`) `DISTINCT`.
            check_distinct_order_by(&raw_order_keys, &raw_target)?;
        }

        let target = raw_target
            .iter()
            .map(|(e, alias)| Ok((rewrite_post_agg(e, &group_exprs, &mut aggs)?, alias.clone())))
            .collect::<Result<Vec<_>, LowerError>>()?;
        let having = having_expr
            .map(|e| rewrite_post_agg(&e, &group_exprs, &mut aggs))
            .transpose()?;
        let mut order_keys = raw_order_keys
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

        let base_plan = materialize_agg_inputs(base_plan, &scope, &mut aggs);
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
        let (windowed, target, window_width) = apply_windows(having_applied, agg_width, target);
        // The same materialization the non-aggregate branch below needs, for
        // the same reason: an ORDER BY key that survived `rewrite_post_agg`
        // as something other than a bare column (`ORDER BY count(*) + 1`,
        // say) still is not a physical position `basin-exec`'s `sort_keys`
        // can use. `agg_width + window_width` is exactly `windowed`'s own
        // width, matching what [`apply_project_set`] uses `input_width` for.
        let windowed = materialize_order_by(windowed, agg_width + window_width, &mut order_keys);
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
        // Resolved against `raw_target` — the SELECT list exactly as
        // written — which is why this runs before `raw_target` is moved into
        // `apply_windows` below. See `lower_order_by_key`'s own docs for the
        // ordinal case (`ORDER BY 2`) this also handles.
        let mut order_keys = stmt
            .sort_clause
            .iter()
            .map(|n| lower_order_by_key(n, &raw_target, &ctx))
            .collect::<Result<Vec<_>, _>>()?;
        if sort_keys_contain_window(&order_keys) {
            return Err(LowerError::Unsupported(
                "window functions in ORDER BY are not yet lowered".into(),
            ));
        }

        // Plain (no `ON`) `DISTINCT` restricts `ORDER BY` to the SELECT
        // list's own expressions — a stricter rule than `DISTINCT ON`'s (see
        // `check_distinct_order_by`'s own docs, including why it must run
        // before this branch's own aggregate-free plan is anything but
        // `raw_target` itself). `DISTINCT ON` has no such restriction — its
        // own, different, leading-match rule is checked separately just
        // below — so this only applies when `distinct_on_raw` is absent.
        if is_distinct && distinct_on_raw.is_none() {
            check_distinct_order_by(&order_keys, &raw_target)?;
        }

        // `DISTINCT ON` is lowered against exactly the same (pre-`Project`)
        // scope `ORDER BY` itself uses — the two are required to agree, so
        // sharing a scope is what makes the structural-equality check below
        // meaningful rather than comparing apples to oranges.
        let mut distinct_on_exprs = distinct_on_raw
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|n| lower_expr(n, &ctx))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        // Verified live: `SELECT DISTINCT ON (k) k, v FROM t ORDER BY v` is
        // rejected — `ERROR: SELECT DISTINCT ON expressions must match
        // initial ORDER BY expressions` — and so is `ORDER BY v, k` (an extra
        // LEADING key also counts as a mismatch). But an ABSENT `ORDER BY`
        // is fine: `SELECT DISTINCT ON (k) k, v FROM t` runs with no error,
        // "first" then being whatever order the input happens to arrive in
        // (documented on `basin_exec::setop::Distinct` itself, and matches
        // what a live server does once its own input actually is ordered).
        if let Some(on) = &distinct_on_exprs {
            let leading_matches = on.len() <= order_keys.len()
                && on.iter().zip(order_keys.iter()).all(|(o, k)| *o == k.expr);
            if !order_keys.is_empty() && !leading_matches {
                return Err(LowerError::Unsupported(
                    "SELECT DISTINCT ON expressions must match initial ORDER BY expressions".into(),
                ));
            }
        }

        let base_width = scope.total_len();
        let (windowed, target, window_width) = apply_windows(base_plan, base_width, raw_target);
        // Strictly after `apply_windows` — see `apply_project_set`'s own
        // docs for why the order is fixed, not a choice, and why its width
        // is `base_width + window_width`, not `base_width`.
        let (srf_applied, target, srf_width) =
            apply_project_set(windowed, base_width + window_width, target)?;

        // `DISTINCT ON`'s own expressions must be plain columns by the time
        // they reach `LogicalPlan::Distinct::on` — `basin-exec`'s
        // `Distinct::new_on` keys physically, on already-resolved column
        // positions, the same requirement `Sort`'s own keys have (see
        // `materialize_distinct_on`'s own docs).
        let materialized_width = base_width + window_width + srf_width;
        let (srf_applied, distinct_on_width) = match &mut distinct_on_exprs {
            Some(on) => {
                materialize_distinct_on(srf_applied, materialized_width, on, &mut order_keys)
            }
            None => (srf_applied, 0),
        };

        // Same materialization `basin-exec`'s `sort_keys` needs — see
        // `materialize_order_by`'s own docs. Runs last (after `DISTINCT ON`'s
        // own materialization above) and against the width THAT step left
        // behind, so an `ORDER BY` key already rewritten to point at a
        // `DISTINCT ON` slot (the leading-match case) is correctly seen as
        // already a bare column and left alone, and any remaining
        // non-column key is appended past it rather than colliding with it.
        let srf_applied = materialize_order_by(
            srf_applied,
            materialized_width + distinct_on_width,
            &mut order_keys,
        );

        let sorted = if order_keys.is_empty() {
            srf_applied
        } else {
            LogicalPlan::Sort {
                input: Box::new(srf_applied),
                keys: order_keys,
            }
        };
        // `DISTINCT ON` sits BELOW the final `Project`, unlike plain
        // `DISTINCT` (see `apply_distinct`'s own docs) — its expressions may
        // reference columns that never make it into the SELECT list at all
        // (verified live: `SELECT DISTINCT ON (v) k FROM t ORDER BY v, k`
        // is legal), so it must still see the pre-`Project` scope.
        let distinct_applied = match distinct_on_exprs {
            Some(on) => LogicalPlan::Distinct {
                input: Box::new(sorted),
                on: Some(on),
            },
            None => sorted,
        };
        let projected = LogicalPlan::Project {
            input: Box::new(distinct_applied),
            exprs: target,
        };
        // Plain `DISTINCT` (no ON) is applied here, above the projection, by
        // `apply_distinct`; `DISTINCT ON` was already applied above, below
        // it, so it must not be applied a second time here.
        apply_limit(
            apply_distinct(projected, is_distinct && distinct_on_raw.is_none()),
            stmt,
            res,
        )?
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

/// Lower `<select> UNION|INTERSECT|EXCEPT [ALL] <select> [ORDER BY ...]
/// [LIMIT ...] [OFFSET ...]`.
///
/// `ORDER BY`/`LIMIT` written after a set operation belong to the WHOLE
/// result, not to the last arm — the parser hangs them on the `SelectStmt`
/// that carries the set operation itself (the arms are `larg`/`rarg`), which
/// is why they are handled here rather than by
/// [`lower_select_stmt_body`]'s ordinary path. An arm's own `ORDER
/// BY`/`LIMIT`, which is legal only when the arm is parenthesized
/// (`(SELECT ... ORDER BY x LIMIT 2) UNION ...`), stays a property of that
/// arm and is lowered by the recursion into it, underneath the
/// [`LogicalPlan::SetOp`].
///
/// See [`lower_set_op_sort_key`] for the (much stricter than an ordinary
/// `SELECT`'s) rule on what a set operation's `ORDER BY` key may be.
fn lower_set_op(
    stmt: &SelectStmt,
    op_kind: SetOperation,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<(LogicalPlan, Schema), LowerError> {
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
    let set_op = LogicalPlan::SetOp {
        left: Box::new(left),
        right: Box::new(right),
        op,
        all: stmt.all,
    };

    // `ORDER BY` sits directly on the set operation's own output, so its keys
    // are positions in `left_schema` and nothing below needs
    // `materialize_order_by`'s extra-column trick: a set operation's key can
    // never be an expression in the first place (see
    // [`lower_set_op_sort_key`]), so it is already the bare
    // `Expr::Column` position `basin-exec`'s `sort_keys` requires.
    let keys = stmt
        .sort_clause
        .iter()
        .map(|n| lower_set_op_sort_key(n, &left_schema))
        .collect::<Result<Vec<_>, _>>()?;
    let sorted = if keys.is_empty() {
        set_op
    } else {
        LogicalPlan::Sort {
            input: Box::new(set_op),
            keys,
        }
    };
    // Above the `Sort`, as everywhere else: `... UNION ... ORDER BY x LIMIT 2`
    // is the first two rows of the ordered result, not the ordering of an
    // arbitrary two rows. Verified live that a trailing `LIMIT` is legal with
    // no `ORDER BY` at all (`SELECT id FROM a UNION ALL SELECT id FROM b
    // LIMIT 2`), which is why this is not gated on `keys` being non-empty.
    Ok((apply_limit(sorted, stmt, res)?, left_schema))
}

/// Lower one `ORDER BY` entry of a set operation.
///
/// Postgres's rule here is far stricter than an ordinary `SELECT`'s (see
/// [`lower_order_by_key`], which lowers a general expression against the
/// query's `FROM` scope): a set operation has no `FROM` scope of its own —
/// its arms do — so the only things a key may name are the set operation's
/// OWN OUTPUT columns, by unqualified name or by 1-based position. Verified
/// on a live PostgreSQL 18.2 server, over `a(id int, name text)` and
/// `b(id int, name text)`:
///
/// - `SELECT id FROM a UNION SELECT id FROM b ORDER BY id` and `... ORDER BY
///   1 DESC` both sort the whole result (not the right arm).
/// - The name resolved is the SET OPERATION's output name, which is the LEFT
///   arm's: `SELECT id AS k FROM a UNION SELECT id FROM b ORDER BY k` works,
///   and `SELECT id AS k FROM a UNION SELECT id AS j FROM b ORDER BY j`
///   fails — `ERROR: column "j" does not exist`, with a DETAIL noting the
///   name exists in `"*SELECT* 2"` but "cannot be referenced from this part
///   of the query".
/// - Anything that is not a plain name or a plain integer is rejected
///   outright, however trivially it would evaluate: `ORDER BY id + 1`,
///   `ORDER BY upper(name)` and even `ORDER BY 1+0` all give
///   `ERROR: invalid UNION/INTERSECT/EXCEPT ORDER BY clause` /
///   `DETAIL: Only result column names can be used, not expressions or
///   functions.` — matched below. (`1+0` is the same `A_Expr`-is-not-an-
///   ordinal distinction [`lower_order_by_key`] documents; here it is not
///   merely "not an ordinal" but a hard error.)
/// - A QUALIFIED name is rejected differently, because the qualifier names a
///   relation that is not in scope at this level at all: `ORDER BY a.id`
///   gives `ERROR: missing FROM-clause entry for table "a"`.
/// - An unknown bare name gives `ERROR: column "name" does not exist` even
///   when that column exists in both arms' `FROM` clauses — it is not in the
///   set operation's output.
/// - An out-of-range position gives `ERROR: ORDER BY position 2 is not in
///   select list`, and so does position `0` — the same wording (and the same
///   1-based, `< 1`-inclusive bound) as the ordinary-`SELECT` ordinal path.
/// - A name matching two output columns gives `ERROR: ORDER BY "k" is
///   ambiguous` (`SELECT id AS k, name AS k FROM a UNION ... ORDER BY k`).
///
/// `ASC`/`DESC`/`NULLS FIRST`/`NULLS LAST` all apply normally, via the shared
/// [`sort_by_direction`].
fn lower_set_op_sort_key(node: &Node, schema: &Schema) -> Result<SortKey, LowerError> {
    use pg_query::protobuf::a_const::Val;

    let Some(NodeEnum::SortBy(sb)) = node.node.as_ref() else {
        return Err(LowerError::Malformed("expected a SortBy node"));
    };
    let expr_node = sb
        .node
        .as_deref()
        .ok_or(LowerError::Malformed("SortBy with no expression"))?;
    let (descending, nulls_first) = sort_by_direction(sb)?;

    let index: u16 = match expr_node.node.as_ref() {
        // `ORDER BY <n>` — a 1-based ordinal into the set operation's output.
        // Any OTHER constant is its own error, not the generic
        // "expressions or functions" one: verified live, `ORDER BY 'x'` and
        // `ORDER BY 1.0` both give `ERROR: non-integer constant in ORDER BY`.
        Some(NodeEnum::AConst(ac)) => {
            let Some(Val::Ival(i)) = ac.val.as_ref() else {
                return Err(LowerError::Unsupported(
                    "non-integer constant in ORDER BY".into(),
                ));
            };
            let pos = i.ival;
            if pos < 1 || pos as usize > schema.len() {
                return Err(LowerError::Unsupported(format!(
                    "ORDER BY position {pos} is not in select list"
                )));
            }
            (pos - 1) as u16
        }
        // `ORDER BY <name>` — an unqualified output column name.
        Some(NodeEnum::ColumnRef(cr)) => {
            let parts = cr
                .fields
                .iter()
                .map(|f| match f.node.as_ref() {
                    Some(NodeEnum::String(s)) => Ok(s.sval.clone()),
                    _ => Err(LowerError::Unsupported(
                        "invalid UNION/INTERSECT/EXCEPT ORDER BY clause: only result column \
                         names can be used, not expressions or functions"
                            .into(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let [name] = parts.as_slice() else {
                // Qualified (`a.id`) — the qualifier is a relation of an arm,
                // which is not in scope for the set operation itself.
                return Err(LowerError::UnknownName(format!(
                    "missing FROM-clause entry for table \"{}\"",
                    parts.first().map(String::as_str).unwrap_or("")
                )));
            };
            let mut found = None;
            for (i, (col, _)) in schema.iter().enumerate() {
                if col == name {
                    if found.is_some() {
                        return Err(LowerError::UnknownName(format!(
                            "ORDER BY \"{name}\" is ambiguous"
                        )));
                    }
                    found = Some(i as u16);
                }
            }
            found.ok_or_else(|| {
                LowerError::UnknownName(format!("column \"{name}\" does not exist"))
            })?
        }
        _ => {
            return Err(LowerError::Unsupported(
                "invalid UNION/INTERSECT/EXCEPT ORDER BY clause: only result column names can \
                 be used, not expressions or functions"
                    .into(),
            ))
        }
    };

    Ok(SortKey {
        expr: Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: schema[index as usize].0.clone(),
        }),
        descending,
        nulls_first,
    })
}

fn lower_values(stmt: &SelectStmt, res: &Resolvers) -> Result<(LogicalPlan, Schema), LowerError> {
    let scope = Scope::empty();
    let resolver = ScopeResolver::new(&scope, res.outer);
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
    let resolver = ScopeResolver::new(&scope, res.outer);
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
        Some(NodeEnum::RangeSubselect(rs)) => build_range_subselect(rs, res),
        Some(NodeEnum::RangeFunction(_)) => Err(LowerError::Unsupported(
            "a set-returning function in FROM is not yet lowered".into(),
        )),
        Some(_) => Err(LowerError::Unsupported(
            "this FROM item is not yet lowered".into(),
        )),
        None => Err(LowerError::Malformed("empty FROM item")),
    }
}

/// `FROM (subquery) [AS alias [(colnames)]]`. A genuine subquery (anything
/// with its own `SELECT` list, `WHERE`, etc.) stays unsupported — see the
/// module docs — but a bare `VALUES` list is a relation too, and `VALUES` is
/// already fully lowered ([`lower_values`]); the only new work here is
/// exposing it under the right name(s). Verified live: `SELECT * FROM
/// (VALUES (1,'a'),(2,'b')) AS v` (and the unaliased `FROM (VALUES ...)`)
/// both name the columns `column1`, `column2`, … — exactly
/// [`lower_values`]'s own default — and `AS v(i, s)` overrides those
/// positionally through the exact same [`apply_column_alias_list`] a CTE's
/// own column-alias list uses (confirmed live: the arity rule and even the
/// error wording differ only in saying `table "v"` instead of `WITH query
/// "x"`).
fn build_range_subselect(
    rs: &pg_query::protobuf::RangeSubselect,
    res: &Resolvers,
) -> Result<FromBuilt, LowerError> {
    if rs.lateral {
        return Err(LowerError::Unsupported("LATERAL is not yet lowered".into()));
    }
    let stmt = match rs.subquery.as_deref().and_then(|n| n.node.as_ref()) {
        Some(NodeEnum::SelectStmt(s)) => s,
        _ => {
            return Err(LowerError::Unsupported(
                "a subquery in FROM is not yet lowered".into(),
            ))
        }
    };
    let op_kind = SetOperation::try_from(stmt.op).unwrap_or(SetOperation::Undefined);
    // The exact shape `lower_values` itself lowers: a bare VALUES list, with
    // no other clause layered on top. `ORDER BY` in particular is excluded
    // rather than silently dropped — `lower_values` has no wiring for it (it
    // only ever runs as a top-level statement's own body today, where an
    // absent `stmt.sort_clause` handler was never exercised), and reaching
    // it here would silently produce a plan that ignores an `ORDER BY` the
    // user actually wrote.
    let is_plain_values = op_kind == SetOperation::SetopNone
        && !stmt.values_lists.is_empty()
        && stmt.sort_clause.is_empty()
        && stmt.with_clause.is_none();
    if !is_plain_values {
        return Err(LowerError::Unsupported(
            "a subquery in FROM is not yet lowered (a bare VALUES list is)".into(),
        ));
    }
    let (plan, schema) = lower_values(stmt, res)?;
    let (qualifier, schema) = match &rs.alias {
        Some(a) if !a.colnames.is_empty() => (
            a.aliasname.clone(),
            apply_column_alias_list(schema, &a.colnames, "table", &a.aliasname)?,
        ),
        Some(a) => (a.aliasname.clone(), schema),
        // A relation with no alias at all has no name to qualify by — an
        // empty qualifier can never match a real (non-empty) one a user
        // writes, so `v.col`-style access correctly stays unresolvable while
        // bare `col` still works. Verified live: `SELECT * FROM (VALUES
        // (1,'a'))` (no alias anywhere) runs fine.
        None => (String::new(), schema),
    };
    Ok(FromBuilt {
        plan,
        scope: Scope::single(qualifier, schema),
        top_join_left_len: None,
    })
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

    let resolver = ScopeResolver::new(&scope, res.outer);
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

    let resolver = ScopeResolver::new(&scope, res.outer);
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

        // A set-returning call is left as `Expr::SetReturning` here, same as
        // any other expression — [`apply_project_set`] is what turns it into
        // a real [`LogicalPlan::ProjectSet`], once the aggregate/window
        // shape of the rest of the statement is known (SRF expansion cannot
        // be decided per-entry: two SRFs in the same list share ONE
        // `ProjectSet`, run in lockstep, not one each — see that function's
        // docs).
        let expr = lower_expr(val, ctx)?;
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
///
/// The returned `usize` is how many columns got appended (`0` when `target`
/// had no window calls at all) — [`apply_project_set`] needs it to know
/// where ITS OWN appended columns must start, since it runs strictly after
/// this (see that function's docs for why the order is fixed, not a choice).
fn apply_windows(
    input: LogicalPlan,
    base_width: usize,
    target: Vec<(Expr, String)>,
) -> (LogicalPlan, Vec<(Expr, String)>, usize) {
    let mut collected = Vec::new();
    for (e, _) in &target {
        collect_windows(e, &mut collected);
    }
    if collected.is_empty() {
        return (input, target, 0);
    }
    let groups = group_by_window_spec(collected);
    let flat: Vec<Expr> = groups.iter().flatten().cloned().collect();
    let added = flat.len();
    let plan = stack_windows(input, groups);
    let target = target
        .into_iter()
        .map(|(e, alias)| (rewrite_post_window(&e, base_width, &flat), alias))
        .collect();
    (plan, target, added)
}

// ─── SET-RETURNING FUNCTIONS ────────────────────────────────────────────────

/// Collect every distinct (structural-equality) set-returning call inside
/// `expr` into `out`, the SRF analogue of [`collect_windows`] — but where
/// that function may treat a match as a leaf with no further checking
/// (Postgres rejects a window nested inside another window's own args at
/// parse analysis, so there is never a second, deeper one to find), a live
/// server DOES parse and run `generate_series(1, generate_series(1,3))`, so
/// silently treating `expr` as a leaf here would drop the inner call on the
/// floor rather than lowering it or refusing it. Basin's `ProjectSet`
/// operator cannot evaluate that shape regardless: `basin-exec/src/cte.rs`'s
/// `resolve_srf` requires every argument to be plain, single-row scalar
/// eval, and `basin-exec/src/eval.rs` has no case for `Expr::SetReturning`
/// at all (an aggregate/window expression hits that same "not scalar eval"
/// wall in that file, for the same structural reason). Reporting a named
/// [`LowerError::Unsupported`] here, at lowering time, is the alternative to
/// that surfacing as an executor-internal error once a plan already claims
/// to be buildable.
fn collect_srfs(expr: &Expr, out: &mut Vec<Expr>) -> Result<(), LowerError> {
    if let Expr::SetReturning { args, .. } = expr {
        if args.iter().any(|a| a.contains_srf()) {
            return Err(LowerError::Unsupported(
                "a set-returning function nested inside another set-returning \
                 function's arguments is not yet lowered"
                    .into(),
            ));
        }
        if !out.contains(expr) {
            out.push(expr.clone());
        }
        return Ok(());
    }
    let mut err = None;
    expr.for_each_child(&mut |c| {
        if err.is_none() {
            if let Err(e) = collect_srfs(c, out) {
                err = Some(e);
            }
        }
    });
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Replace every `Expr::SetReturning` inside `expr` with a `Column`
/// reference into the [`LogicalPlan::ProjectSet`] [`apply_project_set`]
/// built for `flat`, at `input_width + <its position in flat>` — the SRF
/// mirror of [`rewrite_post_window`]; see that function for why stopping at
/// the first match (rather than recursing into it) is correct once `flat`
/// is known to hold no nested SRF (`collect_srfs` already refused that
/// shape before this ever runs).
fn rewrite_post_srf(expr: &Expr, input_width: usize, flat: &[Expr]) -> Expr {
    try_transform(expr, &mut |e| {
        if matches!(e, Expr::SetReturning { .. }) {
            let pos = flat.iter().position(|s| s == e).expect(
                "every Expr::SetReturning was collected into `flat` before this rewrite ran",
            );
            return Some(Ok(Expr::Column(ColumnRef {
                relation: 0,
                index: (input_width + pos) as u16,
                name: "?column?".to_string(),
            })));
        }
        None
    })
    .expect("rewrite_post_srf's callback never returns Err")
}

/// Extract every set-returning call inside `target`'s expressions into a
/// single [`LogicalPlan::ProjectSet`] above `input`, and rewrite `target` to
/// reference its appended columns instead of carrying `Expr::SetReturning`
/// directly. All of `target`'s SRF calls go into ONE `ProjectSet` node,
/// never one each: `LogicalPlan::ProjectSet`'s own docs (and a live server —
/// `SELECT generate_series(1,2), generate_series(1,4)` is 4 rows, lockstep
/// to the longest with NULL padding, not a 2×4 cartesian product) fix that
/// as the meaning of "more than one SRF in a target list", so splitting them
/// across multiple `ProjectSet`s would compute a different, wrong answer
/// (each SRF's own natural length, cross-joined) rather than merely a
/// differently-shaped plan for the same one.
///
/// Must run strictly AFTER [`apply_windows`], never before: a live server
/// computes a window function over the PRE-expansion row set, not the
/// post-expansion one — `SELECT generate_series(1,3), rank() OVER (ORDER BY
/// 1)` gives `rank = 1` on all three expanded rows (there is only one row,
/// with one rank, before `generate_series` explodes it into three), not
/// three independently-ranked rows. `input_width` is `input`'s width AFTER
/// any window columns `apply_windows` already appended, which is exactly
/// what that function's own `usize` return value is for.
///
/// The returned `usize` mirrors [`apply_windows`]'s own (`0` when `target`
/// had no SRF calls at all) — [`materialize_distinct_on`] needs it for the
/// same reason `apply_project_set` itself needed `apply_windows`'s: to know
/// where ITS OWN appended columns must start.
fn apply_project_set(
    input: LogicalPlan,
    input_width: usize,
    target: Vec<(Expr, String)>,
) -> Result<(LogicalPlan, Vec<(Expr, String)>, usize), LowerError> {
    let mut collected = Vec::new();
    for (e, _) in &target {
        collect_srfs(e, &mut collected)?;
    }
    if collected.is_empty() {
        return Ok((input, target, 0));
    }
    let added = collected.len();
    let plan = LogicalPlan::ProjectSet {
        input: Box::new(input),
        srfs: collected.clone(),
    };
    let target = target
        .into_iter()
        .map(|(e, alias)| (rewrite_post_srf(&e, input_width, &collected), alias))
        .collect();
    Ok((plan, target, added))
}

/// Materialize any `DISTINCT ON` expression that isn't already a bare column
/// into a `Project` inserted directly below where `Sort`/[`LogicalPlan::Distinct`]
/// will read from — mirroring [`materialize_agg_inputs`]'s pattern for a
/// `GROUP BY` key: `basin-exec`'s `Distinct::new_on` (like `Sort`'s own keys,
/// via `column_index`) requires a physical column position, never a general
/// expression, so `DISTINCT ON (k % 2)` needs `k % 2` computed once, here,
/// and referenced by position everywhere after.
///
/// When a materialized slot's expression is *also* one of `order_keys`'s own
/// leading expressions — already checked structurally equal by the caller,
/// per Postgres's "DISTINCT ON expressions must match initial ORDER BY
/// expressions" rule — that key is rewritten to point at the exact same slot
/// rather than recomputing the identical expression a second time under a
/// second column.
///
/// The returned `usize` mirrors [`apply_windows`]/[`apply_project_set`]'s own
/// (`0` when every `on_exprs` entry was already a bare column) —
/// [`materialize_order_by`] needs it for the same reason `apply_project_set`
/// itself needed `apply_windows`'s: an `ORDER BY` key materialized *after*
/// this runs must append past whatever this already added, not overwrite it.
fn materialize_distinct_on(
    input: LogicalPlan,
    base_width: usize,
    on_exprs: &mut [Expr],
    order_keys: &mut [SortKey],
) -> (LogicalPlan, usize) {
    let mut extra: Vec<(Expr, String)> = Vec::new();
    let mut next_index = base_width as u16;
    for (i, e) in on_exprs.iter_mut().enumerate() {
        if matches!(e, Expr::Column(_)) {
            continue;
        }
        let alias = default_alias(e);
        extra.push((e.clone(), alias.clone()));
        let materialized = Expr::Column(ColumnRef {
            relation: 0,
            index: next_index,
            name: alias,
        });
        next_index += 1;
        if let Some(k) = order_keys.get_mut(i) {
            k.expr = materialized.clone();
        }
        *e = materialized;
    }
    if extra.is_empty() {
        return (input, 0);
    }
    // A plain positional identity pass-through of every existing column —
    // names are irrelevant here (this `Project` is never what exposes a
    // query's or CTE's final output schema; the real, outer `Project` built
    // from `target` is), unlike `materialize_agg_inputs`'s identity list,
    // which does need real names for that reason.
    let identity = (0..base_width as u16).map(|i| {
        (
            Expr::Column(ColumnRef {
                relation: 0,
                index: i,
                name: String::new(),
            }),
            String::new(),
        )
    });
    let added = extra.len();
    (
        LogicalPlan::Project {
            input: Box::new(input),
            exprs: identity.chain(extra).collect(),
        },
        added,
    )
}

fn sort_keys_contain_window(keys: &[SortKey]) -> bool {
    keys.iter().any(|k| contains_window(&k.expr))
}

/// Lower one `ORDER BY` entry, the same job [`lower_sort_by`] does, plus one
/// case that function cannot handle on its own: a bare integer literal
/// (`ORDER BY 2`) is a 1-based ORDINAL into the SELECT list, not a constant
/// expression to lower and sort by — Postgres's `transformSortClause` /
/// `transformNumericSortGroupClause` resolve it against the target list
/// before anything else runs, which needs `raw_target` (this statement's own
/// SELECT list, as written), something `lower::expr::lower_sort_by` has no
/// way to see. Verified live on a PostgreSQL 18.2 server:
///
/// - `SELECT id, name FROM t ORDER BY 1` sorts by `id` (position 1).
/// - `SELECT id FROM t ORDER BY 1 + 0` does NOT — `EXPLAIN` shows no `Sort`
///   node at all, because `1 + 0` is a constant expression, not an ordinal,
///   and Postgres's own grammar only folds a PLAIN integer constant into the
///   ordinal path (an `A_Expr`, which `1 + 0` is, never does, however
///   trivially it happens to evaluate to an integer).
/// - `SELECT id FROM t ORDER BY -1` IS treated as an ordinal (and rejected,
///   `ERROR: ORDER BY position -1 is not in select list`) — Postgres's own
///   grammar folds a leading `-` directly onto an adjacent numeric literal
///   into a single negative constant (`doNegate`, the same
///   `SignedIconst`-folding that makes `SELECT -1` a `Const`, never
///   `Unary(-, Const(1))`), so this is still the plain-`A_Const` case, not a
///   unary expression wrapping one — matched here for the same reason:
///   `pg_query`'s parse tree already reflects that fold before this ever
///   runs.
/// - `SELECT id FROM t ORDER BY 2` (only one SELECT-list column) and `ORDER
///   BY 0` are both rejected with `ERROR: ORDER BY position N is not in
///   select list`, exact wording matched below.
///
/// The substituted expression is `raw_target`'s own (already lowered, but
/// not yet aggregate/window/SRF-rewritten) entry — from here on an ordinal
/// ORDER BY key is indistinguishable from the caller having written that
/// exact expression out by hand, which is also why a position naming a
/// window-function entry still hits the ordinary "window functions in ORDER
/// BY are not yet lowered" refusal downstream rather than a special case
/// here (verified live that a live server DOES allow that one case; out of
/// scope for the same reason `Expr::Window` in `ORDER BY` generally is).
fn lower_order_by_key(
    node: &Node,
    raw_target: &[(Expr, String)],
    ctx: &LowerCtx,
) -> Result<SortKey, LowerError> {
    use pg_query::protobuf::a_const::Val;

    let Some(NodeEnum::SortBy(sb)) = node.node.as_ref() else {
        return Err(LowerError::Malformed("expected a SortBy node"));
    };
    let expr_node = sb
        .node
        .as_deref()
        .ok_or(LowerError::Malformed("SortBy with no expression"))?;

    if let Some(NodeEnum::AConst(ac)) = expr_node.node.as_ref() {
        if let Some(Val::Ival(i)) = ac.val.as_ref() {
            let pos = i.ival;
            if pos < 1 || pos as usize > raw_target.len() {
                return Err(LowerError::Unsupported(format!(
                    "ORDER BY position {pos} is not in select list"
                )));
            }
            let expr = raw_target[(pos - 1) as usize].0.clone();
            let (descending, nulls_first) = sort_by_direction(sb)?;
            return Ok(SortKey {
                expr,
                descending,
                nulls_first,
            });
        }
    }

    lower_sort_by(node, ctx)
}

/// A `SortBy` node's own direction/null-placement, independent of its
/// expression — duplicated from `lower::expr::lower_sort_by`'s own logic
/// (rather than calling it) because [`lower_order_by_key`]'s ordinal branch
/// needs exactly this half WITHOUT also lowering `sb.node` as a scalar
/// expression: a bare `2` naming a select-list position is not itself a
/// value to evaluate once it is known to be an ordinal.
fn sort_by_direction(sb: &pg_query::protobuf::SortBy) -> Result<(bool, bool), LowerError> {
    let descending = match SortByDir::try_from(sb.sortby_dir).unwrap_or(SortByDir::Undefined) {
        SortByDir::Undefined | SortByDir::SortbyDefault | SortByDir::SortbyAsc => false,
        SortByDir::SortbyDesc => true,
        SortByDir::SortbyUsing => {
            return Err(LowerError::Unsupported(
                "ORDER BY ... USING <custom operator> is not yet lowered".into(),
            ))
        }
    };
    let nulls_first = match SortByNulls::try_from(sb.sortby_nulls).unwrap_or(SortByNulls::Undefined)
    {
        SortByNulls::Undefined | SortByNulls::SortbyNullsDefault => descending,
        SortByNulls::SortbyNullsFirst => true,
        SortByNulls::SortbyNullsLast => false,
    };
    Ok((descending, nulls_first))
}

/// Reject an `ORDER BY` key that is not one of `raw_target`'s own
/// expressions, for a plain (no `ON`) `DISTINCT`. Postgres's rule is
/// stricter than "every column the key reads is projected" — it requires the
/// key to structurally match a SELECT-list entry, checked (like
/// [`materialize_distinct_on`]'s own leading-match check) by `Expr`'s own
/// (derived) structural equality, against `raw_target` — the SELECT list
/// exactly as written, before any aggregate/window/SRF rewrite — the same
/// list Postgres's own `transformSortClause` matches against, and it runs
/// before that function ever looks at `GROUP BY` validity.
///
/// Verified live on a PostgreSQL 18.2 server:
/// - `SELECT DISTINCT id, amt FROM t ORDER BY amt / 2` is rejected even
///   though `amt` alone (not `amt / 2`) is in the select list —
///   `ERROR:  for SELECT DISTINCT, ORDER BY expressions must appear in
///   select list`, matched verbatim below.
/// - `SELECT DISTINCT id FROM t ORDER BY t.id` (same column, differently
///   qualified) is fine — it resolves to the identical underlying column,
///   which is exactly what makes the two `Expr::Column`s compare equal here.
/// - `SELECT DISTINCT name, count(*) FROM t GROUP BY name ORDER BY amt / 2`
///   reports THIS error, not "amt must appear in the GROUP BY clause" (both
///   are independently true of that query) — confirming the ordering this
///   function's docs describe, which is why callers run this check before
///   `rewrite_post_agg`, against `raw_target`, not after.
fn check_distinct_order_by(
    order_keys: &[SortKey],
    raw_target: &[(Expr, String)],
) -> Result<(), LowerError> {
    for key in order_keys {
        if !raw_target.iter().any(|(e, _)| *e == key.expr) {
            return Err(LowerError::Unsupported(
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list".into(),
            ));
        }
    }
    Ok(())
}

/// Materialize any `ORDER BY` key expression that isn't already a bare
/// column into a `Project` inserted directly below `Sort` — the same pattern
/// [`materialize_distinct_on`] uses for `DISTINCT ON`'s own expressions, for
/// the identical underlying reason: `basin-exec/src/build.rs`'s `sort_keys`
/// requires every key to already be a plain `Expr::Column` position
/// (`column_index(...).ok_or(BuildError::NonColumnKey("ORDER BY"))`), never
/// a general expression, so `ORDER BY amt / 2` needs `amt / 2` computed
/// once, here, and referenced by position everywhere after.
///
/// This is what makes `SELECT id FROM t ORDER BY amt / 2` legal at all:
/// verified live, an `ORDER BY` expression that is not itself in the SELECT
/// list is allowed for a plain `SELECT` (`amt` need not be projected — see
/// [`check_distinct_order_by`]'s docs for why plain `DISTINCT` is the one
/// case that forbids it, checked and refused separately, upstream of this
/// function ever running), and the extra column must never reach the
/// query's own output. That drop falls out for free here rather than
/// needing its own step: both call sites build their final `Project` from
/// `target`, a list fixed before this function ever runs, so it simply never
/// references the positions appended below.
fn materialize_order_by(
    input: LogicalPlan,
    base_width: usize,
    order_keys: &mut [SortKey],
) -> LogicalPlan {
    let mut extra: Vec<(Expr, String)> = Vec::new();
    let mut next_index = base_width as u16;
    for key in order_keys.iter_mut() {
        if matches!(key.expr, Expr::Column(_)) {
            continue;
        }
        let alias = default_alias(&key.expr);
        extra.push((key.expr.clone(), alias.clone()));
        key.expr = Expr::Column(ColumnRef {
            relation: 0,
            index: next_index,
            name: alias,
        });
        next_index += 1;
    }
    if extra.is_empty() {
        return input;
    }
    // A plain positional identity pass-through, same as
    // `materialize_distinct_on`'s own (names are irrelevant: this `Project`
    // is never what exposes the query's final output schema).
    let identity = (0..base_width as u16).map(|i| {
        (
            Expr::Column(ColumnRef {
                relation: 0,
                index: i,
                name: String::new(),
            }),
            String::new(),
        )
    });
    LogicalPlan::Project {
        input: Box::new(input),
        exprs: identity.chain(extra).collect(),
    }
}

// ─── Aggregate input materialization ───────────────────────────────────────

/// Materialize any non-column `Expr::Aggregate` argument or `FILTER (WHERE
/// …)` predicate into a `Project` inserted directly under the `Aggregate`,
/// rewriting `aggs` in place to reference the new column instead.
///
/// `basin-exec/src/build.rs`'s `agg_spec` requires both a bare `sum(amt)`'s
/// `args[0]` and a `FILTER`'s predicate to already be a plain `Expr::Column`
/// into the `Aggregate` node's own `input` — `column_index(...).ok_or(
/// BuildError::NonColumnKey(...))`, and `basin-exec/src/aggregate.rs`'s own
/// module docs confirm this is deliberate: "`FILTER (WHERE …)` predicates
/// are all pre-resolved to column positions." A bare-column argument
/// (`sum(amt)`) and no `FILTER` already satisfy that trivially, which is why
/// this was never needed until now; `sum(amt) FILTER (WHERE id > 1)`'s
/// predicate is a full comparison, not a column, and `sum(a + b)`'s argument
/// has the exact same shape of problem. Both get the same fix a `GROUP BY`
/// key's own position already gets post-aggregation ([`rewrite_post_agg`]):
/// compute it below, reference it by position above.
///
/// Only inserts the `Project` when at least one aggregate actually needs
/// it — the common "every arg and filter is already a column" case costs
/// nothing extra and returns `input` unchanged.
fn materialize_agg_inputs(input: LogicalPlan, scope: &Scope, aggs: &mut [Expr]) -> LogicalPlan {
    let base_width = scope.total_len();
    let mut extra: Vec<(Expr, String)> = Vec::new();
    let mut next_index = base_width as u16;

    let mut materialize = |e: &mut Expr| {
        if matches!(e, Expr::Column(_)) {
            return;
        }
        let alias = default_alias(e);
        extra.push((e.clone(), alias.clone()));
        *e = Expr::Column(ColumnRef {
            relation: 0,
            index: next_index,
            name: alias,
        });
        next_index += 1;
    };

    for agg in aggs.iter_mut() {
        let Expr::Aggregate { args, filter, .. } = agg else {
            continue;
        };
        for a in args.iter_mut() {
            materialize(a);
        }
        if let Some(f) = filter.as_deref_mut() {
            materialize(f);
        }
    }

    if extra.is_empty() {
        return input;
    }

    let identity = scope.star_columns(None).into_iter().map(|(name, index)| {
        (
            Expr::Column(ColumnRef {
                relation: 0,
                index,
                name: name.clone(),
            }),
            name,
        )
    });

    LogicalPlan::Project {
        input: Box::new(input),
        exprs: identity.chain(extra).collect(),
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
    use crate::{SnapshotId, SubqueryKind};
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

    #[test]
    fn a_correlated_exists_subquery_resolves_the_outer_reference() {
        // `u.t_id` resolves against the subquery's own `FROM u`; `t.id` does
        // not (only "u" is in scope there) and must fall through to the
        // enclosing query's own scope instead of `LowerError::UnknownName`,
        // tagged `OUTER_REF` per `opt::decorrelate`'s documented convention
        // — see `ScopeResolver` and `SelectSubqueries::lower`.
        let plan =
            lower("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = t.id)").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { predicate, .. } = *input else {
            panic!("expected Filter under Project");
        };
        let Expr::Subquery {
            kind,
            subplan,
            operand,
        } = predicate
        else {
            panic!("expected a Subquery predicate, got {predicate:?}");
        };
        assert_eq!(kind, SubqueryKind::Exists);
        assert!(operand.is_none());

        let LogicalPlan::Project { input, .. } = *subplan else {
            panic!("expected a Project (the subquery's own SELECT list)");
        };
        let LogicalPlan::Filter { input, predicate } = *input else {
            panic!("expected Filter under the subquery's own Project");
        };
        let Expr::Binary { lhs, rhs, .. } = predicate else {
            panic!("expected a Binary predicate");
        };
        // `u.t_id`: local to the subquery's own scope (u: id, t_id, c).
        assert_eq!(*lhs, col(1, "t_id"));
        // `t.id`: the enclosing query's own scope (t: id, a, b), reached
        // through `ScopeResolver`'s `outer` fallback and tagged OUTER_REF.
        assert_eq!(
            *rhs,
            Expr::Column(ColumnRef {
                relation: OUTER_REF,
                index: 0,
                name: "id".to_string(),
            })
        );
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn an_uncorrelated_exists_subquery_still_lowers_with_no_outer_reference() {
        // No column in the subquery's WHERE reaches outside its own FROM —
        // `ScopeResolver`'s `outer` fallback must never fire when the local
        // scope already resolves the name.
        let plan =
            lower("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.t_id = u.id)").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { predicate, .. } = *input else {
            panic!("expected Filter under Project");
        };
        let Expr::Subquery { subplan, .. } = predicate else {
            panic!("expected a Subquery predicate, got {predicate:?}");
        };
        let LogicalPlan::Project { input, .. } = *subplan else {
            panic!("expected a Project (the subquery's own SELECT list)");
        };
        let LogicalPlan::Filter { predicate, .. } = *input else {
            panic!("expected Filter under the subquery's own Project");
        };
        let Expr::Binary { lhs, rhs, .. } = predicate else {
            panic!("expected a Binary predicate");
        };
        assert_eq!(*lhs, col(1, "t_id"));
        assert_eq!(*rhs, col(0, "id"));
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
    fn aggregate_filter_materializes_the_predicate_into_a_project_below() {
        // `basin-exec/src/build.rs`'s `agg_spec` requires `Expr::Aggregate`'s
        // `filter` to already be a bare `Expr::Column` into the `Aggregate`
        // node's own input — see `materialize_agg_inputs`'s doc comment.
        // `id > 1` is not a column, so it must be computed in a `Project`
        // inserted between the scan and the `Aggregate`, and the aggregate's
        // `filter` must end up pointing at that new column.
        let plan = lower("SELECT sum(a) FILTER (WHERE id > 1) FROM t").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Aggregate { input, aggs, .. } = *input else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggs.len(), 1);
        let Expr::Aggregate { args, filter, .. } = &aggs[0] else {
            panic!("expected an Aggregate call");
        };
        // `a` was already a bare column — untouched, still index 1 (t's own
        // schema: id, a, b).
        assert_eq!(args[0], col(1, "a"));
        // The FILTER predicate is now a reference to the materialized
        // column appended after t's own 3 columns (id, a, b), at index 3.
        let filter = filter.as_deref().expect("FILTER must still be present");
        assert_eq!(*filter, col(3, "?column?"));

        let LogicalPlan::Project { input: scan, exprs } = *input else {
            panic!("expected a materializing Project directly under the Aggregate");
        };
        assert!(matches!(*scan, LogicalPlan::Scan { .. }));
        // t's own 3 columns pass through unchanged, plus the materialized
        // `id > 1` as a 4th.
        assert_eq!(exprs.len(), 4);
        assert_eq!(exprs[0], (col(0, "id"), "id".to_string()));
        assert_eq!(exprs[1], (col(1, "a"), "a".to_string()));
        assert_eq!(exprs[2], (col(2, "b"), "b".to_string()));
        let Expr::Binary { lhs, rhs, .. } = &exprs[3].0 else {
            panic!("expected the materialized FILTER predicate");
        };
        assert_eq!(**lhs, col(0, "id"));
        assert_eq!(**rhs, int_lit(1));
    }

    #[test]
    fn an_ordinary_aggregate_needs_no_materializing_project() {
        // The common case — every arg and filter already a bare column (no
        // FILTER at all here) — must cost nothing extra: no `Project`
        // inserted between the scan and the `Aggregate`. Already covered by
        // `group_by_and_a_bare_aggregate_build_an_aggregate_node`'s own
        // `matches!(*input, LogicalPlan::Scan { .. })` assertion; this test
        // names that property directly against `materialize_agg_inputs`.
        let plan = lower("SELECT count(id) FROM t").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Aggregate { input, .. } = *input else {
            panic!("expected Aggregate");
        };
        assert!(
            matches!(*input, LogicalPlan::Scan { .. }),
            "an aggregate with no non-column args/filter must not get a \
             materializing Project — got {input:?}"
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

    /// The regression this increment fixes: `ORDER BY a / 2` is neither a
    /// bare column nor in the SELECT list at all (only `id` is projected).
    /// Verified live: this is legal for a plain `SELECT` — Postgres
    /// materializes the sort key and never exposes it — and `basin-exec`'s
    /// `sort_keys` (`build.rs`) requires a physical `Expr::Column` position
    /// for ANY `Sort` key, so `a / 2` must be computed once, below `Sort`,
    /// and referenced by position; the query's own final `Project` (built
    /// from `id` alone) must not reference that appended slot.
    #[test]
    fn order_by_expression_not_in_select_list_materializes_and_is_dropped() {
        let plan = lower("SELECT id FROM t ORDER BY a / 2").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project at the top, got {plan:?}");
        };
        // The final projection is exactly what was written — the
        // materialized sort key must never reach the query's own output.
        assert_eq!(exprs, vec![(col(0, "id"), "id".to_string())]);
        let LogicalPlan::Sort { keys, input } = *input else {
            panic!("expected Sort under Project, got {input:?}");
        };
        // The materialized slot sits right after t's own 3 columns.
        assert_eq!(keys[0].expr, col(3, "?column?"));
        let LogicalPlan::Project { exprs, input } = *input else {
            panic!("expected a materializing Project under Sort, got {input:?}");
        };
        assert_eq!(exprs.len(), 4, "t's 3 columns plus the materialized a / 2");
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    /// The same materialization, on the aggregate side: `count(id) + 1`
    /// survives `rewrite_post_agg` as `Binary(Column, Literal)`, still not a
    /// bare column — `sort_keys` needs a physical position regardless of
    /// whether the query has a `GROUP BY`.
    #[test]
    fn order_by_after_aggregation_materializes_a_non_column_expression() {
        let plan =
            lower("SELECT a, count(id) FROM t GROUP BY a ORDER BY count(id) + 1").expect("lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Sort { keys, input } = *input else {
            panic!("expected Sort under Project, got {input:?}");
        };
        // The materialized slot sits right after the group key (0) and the
        // aggregate's own output (1).
        assert_eq!(keys[0].expr, col(2, "?column?"));
        let LogicalPlan::Project { exprs, input } = *input else {
            panic!("expected a materializing Project under Sort, got {input:?}");
        };
        assert_eq!(
            exprs.len(),
            3,
            "the group key and agg output, plus the materialized count(id) + 1"
        );
        assert!(matches!(*input, LogicalPlan::Aggregate { .. }));
    }

    /// A bare integer literal in `ORDER BY` is a 1-based ORDINAL into the
    /// SELECT list, not a constant to sort by — verified live. Here it names
    /// an already-projected bare column, so no materialization is needed:
    /// `Sort` sits directly on the `Scan`.
    #[test]
    fn order_by_ordinal_resolves_to_the_select_list_position() {
        let plan = lower("SELECT id, a FROM t ORDER BY 2").expect("lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Sort { keys, input } = *input else {
            panic!("expected Sort under Project, got {input:?}");
        };
        assert_eq!(
            keys[0].expr,
            col(1, "a"),
            "ORDER BY 2 must mean the 2nd select-list entry (a), not the literal 2"
        );
        assert!(
            matches!(*input, LogicalPlan::Scan { .. }),
            "an ordinal naming an already-bare-column entry needs no materializing Project"
        );
    }

    /// Verified live: `SELECT id FROM t ORDER BY 2` (only one select-list
    /// column) is rejected with `ERROR: ORDER BY position 2 is not in
    /// select list`, matched verbatim here.
    #[test]
    fn order_by_ordinal_out_of_range_is_an_error() {
        let err = lower("SELECT id FROM t ORDER BY 2").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert_eq!(msg, "ORDER BY position 2 is not in select list");
    }

    /// `1 + 0` is a general expression that merely evaluates to a constant —
    /// NOT a plain integer literal — so it must NOT be treated as an
    /// ordinal. Verified live: `SELECT id FROM t ORDER BY 1 + 0` runs with
    /// no error (in particular it is not rejected as an out-of-range
    /// ordinal the way `ORDER BY 2` above is) and produces no defined order
    /// (a live `EXPLAIN` shows no `Sort` node at all, since the key
    /// references no column) — this only requires that it lowers and
    /// materializes like any other non-column key, not that Basin also
    /// perform that no-op-sort optimization.
    #[test]
    fn order_by_a_constant_expression_is_not_an_ordinal() {
        let plan = lower("SELECT id FROM t ORDER BY 1 + 0").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs, vec![(col(0, "id"), "id".to_string())]);
        let LogicalPlan::Sort { keys, input } = *input else {
            panic!("expected Sort under Project, got {input:?}");
        };
        // Materialized past t's 3 columns — NOT resolved to `col(0, "id")`,
        // which is what treating `1` as an ordinal position would produce.
        assert_eq!(keys[0].expr, col(3, "?column?"));
        assert!(matches!(*input, LogicalPlan::Project { .. }));
    }

    /// Postgres's plain (no `ON`) `DISTINCT` is stricter than an ordinary
    /// `SELECT`: an `ORDER BY` expression must appear in the select list,
    /// not merely reference columns that are projected. Verified live:
    /// `ERROR:  for SELECT DISTINCT, ORDER BY expressions must appear in
    /// select list`, matched verbatim here.
    #[test]
    fn plain_distinct_rejects_an_order_by_expression_not_in_the_select_list() {
        let err = lower("SELECT DISTINCT id FROM t ORDER BY a / 2").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert_eq!(
            msg,
            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
        );
    }

    /// The rule is stricter than "every column it reads is projected" —
    /// verified live: `SELECT DISTINCT id, amt FROM t ORDER BY amt / 2` is
    /// STILL rejected even though `amt` alone is in the select list; only
    /// `amt` itself (not `amt / 2`) would be a legal `ORDER BY` key here.
    #[test]
    fn plain_distinct_rejects_even_when_every_column_it_reads_is_projected() {
        let err = lower("SELECT DISTINCT id, a FROM t ORDER BY a / 2").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("for SELECT DISTINCT"),
            "message should be precise: {msg}"
        );
    }

    /// The positive case: an `ORDER BY` expression that DOES structurally
    /// match a select-list entry is fine under plain `DISTINCT` — verified
    /// live (`SELECT DISTINCT a FROM t ORDER BY a` runs with no error).
    #[test]
    fn plain_distinct_allows_an_order_by_expression_that_matches_the_select_list() {
        let plan = lower("SELECT DISTINCT a FROM t ORDER BY a").expect("lowers");
        assert!(matches!(plan, LogicalPlan::Distinct { .. }));
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

    // --- 9b: ORDER BY / LIMIT on a set operation's own result ----------------
    //
    // Every rule asserted here was checked against a live PostgreSQL 18.2
    // server first — see `lower_set_op_sort_key`'s own docs for the exact
    // statements and the exact server responses.

    /// The `Sort` must sit ABOVE the `SetOp`, not inside its right arm:
    /// `ORDER BY` after a set operation orders the WHOLE result.
    #[test]
    fn union_order_by_an_output_name_sorts_above_the_set_op() {
        let plan = lower("SELECT a FROM t UNION SELECT t_id FROM u ORDER BY a").unwrap();
        let LogicalPlan::Sort { input, keys } = plan else {
            panic!("expected a Sort above the SetOp, got {plan:?}");
        };
        assert!(matches!(*input, LogicalPlan::SetOp { .. }));
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].expr, col(0, "a"));
        assert!(!keys[0].descending);
        assert!(!keys[0].nulls_first);
    }

    /// `ORDER BY <n>` is a 1-based position in the set operation's output,
    /// and DESC flips the null default the same way it does anywhere else.
    #[test]
    fn union_order_by_a_position_resolves_against_the_output() {
        let plan = lower("SELECT a, b FROM t UNION SELECT t_id, c FROM u ORDER BY 2 DESC").unwrap();
        let LogicalPlan::Sort { keys, .. } = plan else {
            panic!("expected Sort");
        };
        assert_eq!(keys[0].expr, col(1, "b"));
        assert!(keys[0].descending);
        assert!(keys[0].nulls_first);
    }

    /// The name resolved is the SET OPERATION's own output name — which is
    /// the LEFT arm's, alias included.
    #[test]
    fn union_order_by_resolves_the_left_arms_alias() {
        let plan = lower("SELECT a AS k FROM t UNION SELECT t_id FROM u ORDER BY k").unwrap();
        let LogicalPlan::Sort { keys, .. } = plan else {
            panic!("expected Sort");
        };
        assert_eq!(keys[0].expr, col(0, "k"));
    }

    /// ... and the RIGHT arm's alias is not in scope at all.
    #[test]
    fn union_order_by_a_right_arm_alias_does_not_resolve() {
        let err =
            lower("SELECT a AS k FROM t UNION SELECT t_id AS j FROM u ORDER BY j").unwrap_err();
        let LowerError::UnknownName(msg) = err else {
            panic!("expected UnknownName, got {err:?}");
        };
        assert_eq!(msg, "column \"j\" does not exist");
    }

    /// A column that exists in both arms' FROM clauses but not in the set
    /// operation's OUTPUT is still not a legal key.
    #[test]
    fn union_order_by_a_non_output_column_does_not_resolve() {
        let err = lower("SELECT a FROM t UNION SELECT t_id FROM u ORDER BY b").unwrap_err();
        assert!(matches!(err, LowerError::UnknownName(_)), "got {err:?}");
    }

    /// Only names and positions — never an expression, however trivially it
    /// would evaluate.
    #[test]
    fn union_order_by_an_expression_is_refused() {
        for sql in [
            "SELECT a FROM t UNION SELECT t_id FROM u ORDER BY a + 1",
            "SELECT a FROM t UNION SELECT t_id FROM u ORDER BY upper(b)",
            "SELECT a FROM t UNION SELECT t_id FROM u ORDER BY 1 + 0",
        ] {
            let err = lower(sql).unwrap_err();
            let LowerError::Unsupported(msg) = err else {
                panic!("expected Unsupported for `{sql}`, got {err:?}");
            };
            assert!(
                msg.starts_with("invalid UNION/INTERSECT/EXCEPT ORDER BY clause"),
                "unexpected message for `{sql}`: {msg}"
            );
        }
    }

    /// A non-integer constant gets its own, different message — matching the
    /// live server, which distinguishes the two.
    #[test]
    fn union_order_by_a_non_integer_constant_is_refused() {
        let err = lower("SELECT a FROM t UNION SELECT t_id FROM u ORDER BY 'x'").unwrap_err();
        assert_eq!(
            err,
            LowerError::Unsupported("non-integer constant in ORDER BY".into())
        );
    }

    /// A qualified key names a relation belonging to an ARM, which the set
    /// operation itself cannot see.
    #[test]
    fn union_order_by_a_qualified_name_is_refused() {
        let err = lower("SELECT a FROM t UNION SELECT t_id FROM u ORDER BY t.a").unwrap_err();
        let LowerError::UnknownName(msg) = err else {
            panic!("expected UnknownName, got {err:?}");
        };
        assert_eq!(msg, "missing FROM-clause entry for table \"t\"");
    }

    #[test]
    fn union_order_by_an_out_of_range_position_is_refused() {
        for (sql, want) in [
            (
                "SELECT a FROM t UNION SELECT t_id FROM u ORDER BY 2",
                "ORDER BY position 2 is not in select list",
            ),
            (
                "SELECT a FROM t UNION SELECT t_id FROM u ORDER BY 0",
                "ORDER BY position 0 is not in select list",
            ),
        ] {
            let err = lower(sql).unwrap_err();
            assert_eq!(err, LowerError::Unsupported(want.into()), "for `{sql}`");
        }
    }

    #[test]
    fn union_order_by_an_ambiguous_output_name_is_refused() {
        let err = lower("SELECT a AS k, b AS k FROM t UNION SELECT t_id, c FROM u ORDER BY k")
            .unwrap_err();
        let LowerError::UnknownName(msg) = err else {
            panic!("expected UnknownName, got {err:?}");
        };
        assert_eq!(msg, "ORDER BY \"k\" is ambiguous");
    }

    /// `LIMIT` after a set operation bounds the ORDERED result, so it must
    /// sit above the `Sort`, not below it.
    #[test]
    fn union_limit_sits_above_the_sort_which_sits_above_the_set_op() {
        let plan =
            lower("SELECT a FROM t UNION ALL SELECT t_id FROM u ORDER BY a LIMIT 3").unwrap();
        let LogicalPlan::Limit { input, fetch, .. } = plan else {
            panic!("expected Limit at the root, got {plan:?}");
        };
        assert_eq!(fetch, Some(int_lit(3)));
        let LogicalPlan::Sort { input, .. } = *input else {
            panic!("expected Sort under the Limit");
        };
        assert!(matches!(*input, LogicalPlan::SetOp { all: true, .. }));
    }

    /// A trailing `LIMIT` with no `ORDER BY` is legal, and must not
    /// gratuitously introduce a `Sort`.
    #[test]
    fn union_limit_without_an_order_by_wraps_the_set_op_directly() {
        let plan = lower("SELECT a FROM t UNION ALL SELECT t_id FROM u LIMIT 2").unwrap();
        let LogicalPlan::Limit { input, .. } = plan else {
            panic!("expected Limit, got {plan:?}");
        };
        assert!(matches!(*input, LogicalPlan::SetOp { .. }));
    }

    #[test]
    fn intersect_and_except_take_the_same_order_by_path() {
        for sql in [
            "SELECT a FROM t INTERSECT SELECT t_id FROM u ORDER BY 1",
            "SELECT a FROM t EXCEPT SELECT t_id FROM u ORDER BY a DESC",
        ] {
            let plan = lower(sql).unwrap();
            let LogicalPlan::Sort { input, keys } = plan else {
                panic!("expected Sort for `{sql}`");
            };
            assert_eq!(keys[0].expr, col(0, "a"));
            assert!(matches!(*input, LogicalPlan::SetOp { .. }));
        }
    }

    /// An arm's OWN `ORDER BY`/`LIMIT` (legal only parenthesized) stays a
    /// property of that arm, underneath the `SetOp` — it is not hoisted to
    /// the whole result. This shape already lowered before set-operation-level
    /// `ORDER BY` existed; it is asserted here so that the two cannot be
    /// confused for one another.
    #[test]
    fn a_parenthesized_arms_own_order_by_stays_inside_that_arm() {
        let plan = lower(
            "(SELECT a FROM t ORDER BY a LIMIT 2) UNION (SELECT t_id FROM u ORDER BY t_id LIMIT 2)",
        )
        .unwrap();
        let LogicalPlan::SetOp { left, right, .. } = plan else {
            panic!("expected a bare SetOp at the root, got {plan:?}");
        };
        assert!(matches!(*left, LogicalPlan::Limit { .. }));
        assert!(matches!(*right, LogicalPlan::Limit { .. }));
    }

    /// A three-arm chain parses as `SetOp(SetOp(a, b), c)` with the `ORDER
    /// BY` on the outermost node, and the output names it resolves against
    /// are still the LEFTMOST arm's.
    #[test]
    fn order_by_on_a_chained_set_op_resolves_against_the_leftmost_arm() {
        let plan =
            lower("SELECT a FROM t UNION SELECT t_id FROM u UNION SELECT id FROM t ORDER BY a")
                .unwrap();
        let LogicalPlan::Sort { input, keys } = plan else {
            panic!("expected Sort, got {plan:?}");
        };
        assert_eq!(keys[0].expr, col(0, "a"));
        let LogicalPlan::SetOp { left, .. } = *input else {
            panic!("expected SetOp under the Sort");
        };
        assert!(matches!(*left, LogicalPlan::SetOp { .. }));
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

    /// `OVER w` is exactly the named definition spelled out inline —
    /// verified live (see `lower::expr`'s `lower_window_def` docs) — so the
    /// two forms must lower to the identical plan.
    #[test]
    fn a_named_window_clause_lowers_the_same_as_writing_it_inline() {
        let named = lower("SELECT rank() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)")
            .expect("a named WINDOW clause lowers");
        let inline = lower("SELECT rank() OVER (PARTITION BY a ORDER BY b) FROM t")
            .expect("the inline form lowers");
        assert_eq!(named, inline);
    }

    /// Two calls may share one named window; each lowers to its own
    /// `Expr::Window`, so nothing here depends on the name surviving.
    #[test]
    fn one_named_window_may_be_referenced_by_several_calls() {
        let plan = lower("SELECT rank() OVER w, count(*) OVER w FROM t WINDOW w AS (ORDER BY a)")
            .expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs.len(), 2);
        let LogicalPlan::Window { windows, .. } = *input else {
            panic!("expected Window under the Project");
        };
        assert_eq!(windows.len(), 2);
    }

    /// An unreferenced `WINDOW` entry is legal and simply never resolved —
    /// so it must not make an otherwise-ordinary query fail.
    #[test]
    fn an_unreferenced_named_window_is_ignored() {
        let plan = lower("SELECT id FROM t WINDOW w AS (ORDER BY a)").expect("lowers");
        let inline = lower("SELECT id FROM t").expect("lowers");
        assert_eq!(plan, inline);
    }

    #[test]
    fn over_an_undefined_window_name_is_an_unknown_name() {
        let err = lower("SELECT rank() OVER nope FROM t WINDOW w AS (ORDER BY a)").unwrap_err();
        let LowerError::UnknownName(msg) = err else {
            panic!("expected UnknownName, got {err:?}");
        };
        assert_eq!(msg, "window \"nope\" does not exist");
    }

    /// `OVER (w ...)` — copying a named window and adding to it — is a
    /// different (and legal, live) construct with its own merge rules, and
    /// is still refused rather than silently treated as a bare `OVER w`.
    #[test]
    fn extending_a_named_window_with_extra_clauses_is_still_unsupported() {
        let err = lower(
            "SELECT rank() OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM t WINDOW w AS (ORDER BY a)",
        )
        .unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("named WINDOW"), "unexpected message: {msg}");
    }

    // --- 11: Set-returning functions -> ProjectSet --------------------------

    #[test]
    fn a_single_srf_lowers_to_a_project_set_above_an_empty_input() {
        let plan = lower("SELECT generate_series(1, 3)").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs.len(), 1);
        assert_eq!(exprs[0].0, col(0, "?column?"));

        let LogicalPlan::ProjectSet { input, srfs } = *input else {
            panic!("expected ProjectSet directly under Project, got {input:?}");
        };
        assert_eq!(srfs.len(), 1);
        let Expr::SetReturning { func, args } = &srfs[0] else {
            panic!("expected an Expr::SetReturning, got {:?}", srfs[0]);
        };
        assert_eq!(*func, crate::FuncId(Oid(1066)));
        assert_eq!(*args, vec![int_lit(1), int_lit(3)]);
        assert!(matches!(
            *input,
            LogicalPlan::Empty {
                produce_one_row: true,
                ..
            }
        ));
    }

    #[test]
    fn two_srfs_of_different_lengths_share_one_project_set_not_a_cartesian_product() {
        // A separate ProjectSet per SRF (or a cartesian product of their
        // lengths) would silently compute the pre-10 rule. The fix is that
        // BOTH calls land in ONE ProjectSet node, which is what makes the
        // executor's lockstep-to-the-longest/NULL-pad rule (verified
        // against a live PostgreSQL 18 server — see `LogicalPlan::
        // ProjectSet`'s own docs) the meaning of this plan, rather than a
        // 2x4 cross join.
        let plan = lower("SELECT generate_series(1,2), generate_series(1,4)").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0].0, col(0, "?column?"));
        assert_eq!(exprs[1].0, col(1, "?column?"));

        let LogicalPlan::ProjectSet { srfs, .. } = *input else {
            panic!("expected a single ProjectSet under Project");
        };
        assert_eq!(
            srfs.len(),
            2,
            "both SRFs belong to the SAME ProjectSet node, not one each"
        );
    }

    #[test]
    fn an_srf_alongside_a_plain_column_leaves_the_column_untouched_and_appends_the_srf() {
        let plan = lower("SELECT a, generate_series(1,3) FROM t").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0], (col(1, "a"), "a".to_string()));
        // `t` is 3 columns wide (id, a, b); the SRF's own output column is
        // appended right after it, exactly like a window function's.
        assert_eq!(exprs[1].0, col(3, "?column?"));

        let LogicalPlan::ProjectSet { input, srfs } = *input else {
            panic!("expected ProjectSet directly under Project");
        };
        assert_eq!(srfs.len(), 1);
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn an_srf_nested_inside_another_srfs_arguments_is_rejected() {
        // A live PostgreSQL server parses AND RUNS
        // `generate_series(1, generate_series(1,3))` — this is not invalid
        // SQL. It is invalid for Basin specifically: `ProjectSet::
        // resolve_srf` (`basin-exec/src/cte.rs`) requires every argument to
        // be plain scalar eval, and `eval::eval` has no case for
        // `Expr::SetReturning` at all — see `collect_srfs`'s docs. Refusing
        // here, by name, is the alternative to that surfacing as an
        // executor-internal error once a plan already claims to be
        // buildable.
        let err = lower("SELECT generate_series(1, generate_series(1,3))").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("nested"),
            "message should call out the nesting: {msg}"
        );
    }

    #[test]
    fn an_srf_combined_with_an_aggregate_is_rejected_with_the_real_reason() {
        // Before this rejection existed, such a query would have reached
        // `rewrite_post_agg` and failed with "column must appear in the
        // GROUP BY clause" — technically true of a bare column inside the
        // SRF's own arguments, but not the actual reason this doesn't lower,
        // and confusing for anyone hitting it. This asserts the real reason
        // is what gets reported.
        let err = lower("SELECT sum(id), generate_series(1,3) FROM t").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("GROUP BY") || msg.contains("aggregate"),
            "message should name the real reason: {msg}"
        );
    }

    #[test]
    fn an_srf_sits_above_a_window_node_since_windows_see_pre_expansion_rows() {
        // Verified against a live server: `SELECT generate_series(1,3),
        // rank() OVER (ORDER BY 1)` gives `rank = 1` on all three expanded
        // rows — the window function saw the single pre-expansion row, not
        // the three post-expansion ones. `ProjectSet` must therefore sit
        // ABOVE `Window`, not below it, and its own appended column must
        // land after the window's.
        let plan =
            lower("SELECT generate_series(1,3), rank() OVER (ORDER BY a) FROM t").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        // `t` is 3 columns wide; the window's own column lands at 3, and the
        // SRF's own column is appended after THAT, at 4.
        assert_eq!(exprs[0].0, col(4, "?column?"));
        assert_eq!(exprs[1].0, col(3, "?column?"));

        let LogicalPlan::ProjectSet { input, srfs } = *input else {
            panic!("expected ProjectSet directly under Project, got {input:?}");
        };
        assert_eq!(srfs.len(), 1);
        assert!(
            matches!(*input, LogicalPlan::Window { .. }),
            "ProjectSet must sit directly above Window, got {input:?}"
        );
    }

    // --- 12: WITH / CTE -> Cte / CteRef -------------------------------------

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

    /// A CTE's own column-alias list renames its exposed output positionally
    /// — `WITH x(n) AS (SELECT 1) SELECT n FROM x` only type-checks because
    /// `n` is what `x` exposes, not the anchor's own `?column?`. Verified
    /// live: `WITH x(a, b) AS (SELECT 1, 'hi') SELECT a, b FROM x` returns
    /// `a=1, b='hi'`.
    #[test]
    fn a_ctes_own_column_alias_list_renames_its_output() {
        let plan = lower("WITH x(n) AS (SELECT 1) SELECT n FROM x").expect("lowers");
        let LogicalPlan::Cte { input, .. } = plan else {
            panic!("expected a Cte node, got {plan:?}");
        };
        let LogicalPlan::Project { input, exprs } = *input else {
            panic!("expected the main query's own Project, got {input:?}");
        };
        assert_eq!(exprs, vec![(col(0, "n"), "n".to_string())]);
        let LogicalPlan::CteRef { schema, .. } = *input else {
            panic!("expected a CteRef, got {input:?}");
        };
        assert_eq!(schema, vec![("n".to_string(), PgType::INT4)]);
    }

    /// Fewer aliases than the body has columns is NOT an error — only the
    /// leading columns are renamed, the rest keep their own name. Verified
    /// live: `WITH x(a) AS (SELECT 1 AS one, 2 AS two) SELECT * FROM x`
    /// returns columns `a, two`.
    #[test]
    fn a_ctes_column_alias_list_may_be_shorter_than_the_body() {
        let plan = lower("WITH x(a) AS (SELECT id, b FROM t) SELECT * FROM x").expect("lowers");
        let LogicalPlan::Cte { input, .. } = plan else {
            panic!("expected a Cte node, got {plan:?}");
        };
        let LogicalPlan::Project { exprs, .. } = *input else {
            panic!("expected the main query's own Project, got {input:?}");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "a"), "a".to_string()),
                (col(1, "b"), "b".to_string()),
            ]
        );
    }

    /// MORE aliases than the body has columns IS an error. Verified live:
    /// `WITH x(a,b,c) AS (SELECT 1, 'hi') SELECT * FROM x` fails with `WITH
    /// query "x" has 2 columns available but 3 columns specified`.
    #[test]
    fn a_ctes_column_alias_list_longer_than_the_body_is_an_error() {
        let err = lower("WITH x(a,b,c) AS (SELECT id, b FROM t) SELECT * FROM x").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("2 columns available") && msg.contains("3 columns specified"),
            "message should be precise: {msg}"
        );
    }

    /// Once a CTE has its own column-alias list, the body's original column
    /// name is hidden — `n` is the only name in scope for `x`'s first column,
    /// not whatever the body itself called it. Verified live: `WITH x(a) AS
    /// (SELECT 1 AS orig) SELECT orig FROM x` fails with `column "orig" does
    /// not exist`.
    #[test]
    fn a_ctes_column_alias_list_hides_the_bodys_own_name() {
        let err = lower("WITH x(a) AS (SELECT id AS orig FROM t) SELECT orig FROM x").unwrap_err();
        assert!(matches!(err, LowerError::UnknownName(_)));
    }

    /// `WITH RECURSIVE`'s alias list must apply to the anchor's schema before
    /// the recursive term is lowered, not after: the recursive term's own
    /// `FROM r` self-reference is what resolves `n`. Verified live: `WITH
    /// RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5)
    /// SELECT n FROM r` returns 1..5 — this was the confirmed blocker this
    /// increment exists to fix.
    #[test]
    fn a_recursive_ctes_column_alias_list_is_visible_to_its_own_recursive_term() {
        let plan = lower(
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) \
             SELECT n FROM r",
        )
        .expect("lowers");
        let LogicalPlan::Cte {
            recursive, body, ..
        } = plan
        else {
            panic!("expected a Cte node, got {plan:?}");
        };
        assert!(recursive);
        assert!(matches!(*body, LogicalPlan::SetOp { .. }));
    }

    #[test]
    fn a_data_modifying_cte_is_unsupported() {
        let err = lower("WITH x AS (INSERT INTO t (id) VALUES (1) RETURNING id) SELECT id FROM x")
            .unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

    // --- Unsupported constructs ------------------------------------------------

    /// No `ORDER BY` at all is legal for `DISTINCT ON` — verified live:
    /// `SELECT DISTINCT ON (k) k, v FROM t` runs with no error, "first"
    /// being whatever order the input arrives in (documented on
    /// `basin_exec::setop::Distinct` itself). `Distinct` must sit BELOW the
    /// final `Project`, not above it like plain `DISTINCT` — its own
    /// expression may not even be in the SELECT list.
    #[test]
    fn distinct_on_with_no_order_by_lowers_below_the_projection() {
        let plan = lower("SELECT DISTINCT ON (a) a, b FROM t").expect("lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project at the top, got {plan:?}");
        };
        let LogicalPlan::Distinct { input, on } = *input else {
            panic!("expected Distinct under the Project, got {input:?}");
        };
        assert_eq!(on, Some(vec![col(1, "a")]));
        assert!(
            matches!(*input, LogicalPlan::Scan { .. }),
            "with no ORDER BY, Distinct sits directly on the FROM scan: {input:?}"
        );
    }

    /// A matching leading `ORDER BY` sorts before `Distinct` picks the first
    /// row of each group — verified live: `SELECT DISTINCT ON (k) k, v FROM t
    /// ORDER BY k, v DESC` keeps the row with the largest `v` per `k`.
    #[test]
    fn distinct_on_with_a_matching_order_by_sorts_before_deduping() {
        let plan = lower("SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b DESC").expect("lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project at the top, got {plan:?}");
        };
        let LogicalPlan::Distinct { input, on } = *input else {
            panic!("expected Distinct under the Project, got {input:?}");
        };
        assert_eq!(on, Some(vec![col(1, "a")]));
        let LogicalPlan::Sort { keys, input } = *input else {
            panic!("expected Sort under Distinct, got {input:?}");
        };
        assert_eq!(keys.len(), 2);
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    /// Verified live: `SELECT DISTINCT ON (k) k, v FROM t ORDER BY v` fails
    /// with `SELECT DISTINCT ON expressions must match initial ORDER BY
    /// expressions`.
    #[test]
    fn distinct_on_with_a_non_matching_order_by_is_an_error() {
        let err = lower("SELECT DISTINCT ON (a) a, b FROM t ORDER BY b").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("DISTINCT ON expressions must match"),
            "message should be precise: {msg}"
        );
    }

    /// An extra LEADING `ORDER BY` key ahead of the `DISTINCT ON` expression
    /// is also a mismatch — verified live: `... ORDER BY v, k` (with
    /// `DISTINCT ON (k)`) is rejected the same as `ORDER BY v` alone.
    #[test]
    fn distinct_on_with_an_extra_leading_order_by_key_is_an_error() {
        let err = lower("SELECT DISTINCT ON (a) a, b FROM t ORDER BY b, a").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

    /// A `DISTINCT ON` expression that isn't a bare column (`a + 1`, say) is
    /// computed once and referenced by position — `basin-exec`'s
    /// `Distinct::new_on` requires a physical column, the same requirement
    /// `Sort`'s own keys already have.
    #[test]
    fn distinct_on_materializes_a_non_column_expression() {
        let plan =
            lower("SELECT DISTINCT ON (a + 1) a, b FROM t ORDER BY a + 1, b").expect("lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project at the top, got {plan:?}");
        };
        let LogicalPlan::Distinct { input, on } = *input else {
            panic!("expected Distinct under the Project, got {input:?}");
        };
        // The materialized slot sits right after `t`'s own 3 columns.
        assert_eq!(on, Some(vec![col(3, "?column?")]));
        let LogicalPlan::Sort { keys, input } = *input else {
            panic!("expected Sort under Distinct, got {input:?}");
        };
        // Both the DISTINCT ON slot and the matching ORDER BY key point at
        // the SAME materialized column, not two separate computations of
        // `a + 1`.
        assert_eq!(keys[0].expr, col(3, "?column?"));
        let LogicalPlan::Project { exprs, input } = *input else {
            panic!("expected a materializing Project under Sort, got {input:?}");
        };
        assert_eq!(exprs.len(), 4, "t's 3 columns plus the materialized a + 1");
        assert!(matches!(*input, LogicalPlan::Scan { .. }));
    }

    /// Combining `DISTINCT ON` with `GROUP BY`/an aggregate is a live-server
    /// legal combination (`SELECT DISTINCT ON (k) k, sum(v) FROM t GROUP BY k
    /// ORDER BY k` runs), but is out of scope for this increment — see the
    /// guard's own comment for why. It must be refused, not silently
    /// mishandled.
    #[test]
    fn distinct_on_combined_with_group_by_is_unsupported() {
        let err =
            lower("SELECT DISTINCT ON (a) a, sum(id) FROM t GROUP BY a ORDER BY a").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("DISTINCT ON") && msg.contains("GROUP BY"),
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

    // --- VALUES in FROM ------------------------------------------------------

    /// Verified live: `SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v` names the
    /// columns `column1`, `column2` — the exact same default `lower_values`
    /// itself already uses for a top-level `VALUES` statement.
    #[test]
    fn a_values_list_in_from_uses_the_default_column_names() {
        let plan = lower("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v").expect("lowers");
        let LogicalPlan::Project { exprs, input } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "column1"), "column1".to_string()),
                (col(1, "column2"), "column2".to_string()),
            ]
        );
        let LogicalPlan::Values { rows, .. } = *input else {
            panic!("expected Values under Project, got {input:?}");
        };
        assert_eq!(rows.len(), 2);
    }

    /// Verified live: an alias with no column list still works, and even a
    /// bare `FROM (VALUES ...)` with no alias at all does too — the relation
    /// simply has no name to qualify by.
    #[test]
    fn a_values_list_in_from_needs_no_alias() {
        let plan = lower("SELECT * FROM (VALUES (1,'a'))").expect("lowers");
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs[0].1, "column1");
    }

    /// Verified live: `SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(i, s)`
    /// renames both columns positionally, same rule a CTE's own column-alias
    /// list uses.
    #[test]
    fn a_values_list_in_from_may_be_column_aliased() {
        let plan = lower("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(i, s)").expect("lowers");
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "i"), "i".to_string()),
                (col(1, "s"), "s".to_string()),
            ]
        );
    }

    /// Fewer aliases than columns renames only the leading ones, same as a
    /// CTE's own column-alias list — verified live.
    #[test]
    fn a_values_list_in_from_column_alias_list_may_be_shorter() {
        let plan = lower("SELECT * FROM (VALUES (1,'a')) AS v(i)").expect("lowers");
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "i"), "i".to_string()),
                (col(1, "column2"), "column2".to_string()),
            ]
        );
    }

    /// More aliases than columns is an error. Verified live: `SELECT * FROM
    /// (VALUES (1,'a'),(2,'b')) AS v(i,s,extra)` fails with `table "v" has 2
    /// columns available but 3 columns specified`.
    #[test]
    fn a_values_list_in_from_column_alias_list_longer_than_the_body_is_an_error() {
        let err = lower("SELECT * FROM (VALUES (1,'a')) AS v(i, s, extra)").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("table \"v\"")
                && msg.contains("2 columns available")
                && msg.contains("3 columns specified"),
            "message should be precise: {msg}"
        );
    }

    /// A `VALUES` relation in `FROM` joins like any other — exercising it
    /// alongside a real table catches an off-by-one in the scope's flat
    /// column offsets that a `VALUES`-only query never would.
    #[test]
    fn a_values_list_in_from_joins_against_a_real_table() {
        let plan = lower("SELECT t.id, s.x FROM t JOIN (VALUES (1), (2)) AS s(x) ON t.id = s.x")
            .expect("lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        let LogicalPlan::Join { on, right, .. } = *input else {
            panic!("expected Join under Project, got {input:?}");
        };
        assert_eq!(on.len(), 1);
        assert!(matches!(*right, LogicalPlan::Values { .. }));
    }

    // --- A correlated subquery in the SELECT list, end to end -------------
    //
    // `opt::decorrelate` scopes all four of its transforms to a subquery
    // sitting in a `Filter` predicate and says so ("a subquery anywhere else
    // (a `Project` target list, ...) is left untouched"). Something
    // downstream nevertheless assumed the opposite: `basin_exec::build`'s
    // `materialize_scalar_subquery` carried a doc comment asserting that
    // decorrelation GUARANTEED no correlated subquery could reach it, and
    // evaluated every scalar subquery once for the whole statement on the
    // strength of that. The result was a silently wrong answer, and these two
    // tests pin down the two plan-level facts the physical layer now relies
    // on instead of on that assumption.

    /// After the FULL default pipeline, a correlated scalar subquery in the
    /// target list is still a correlated scalar subquery: nothing turned it
    /// into a join, and its `OUTER_REF` reference is intact. A physical layer
    /// that assumes otherwise produces wrong values, not errors.
    #[test]
    fn optimization_leaves_a_target_list_correlated_subquery_correlated() {
        let plan = lower("SELECT id, (SELECT count(*) FROM t x WHERE x.id = t.id) FROM t").unwrap();
        let (opt, _passes) = crate::opt::optimize_default(plan);
        let LogicalPlan::Project { exprs, .. } = &opt else {
            panic!("expected a Project at the root, got {opt:?}");
        };
        let Expr::Subquery { kind, subplan, .. } = &exprs[1].0 else {
            panic!("expected the target list's second entry to still be a subquery");
        };
        assert_eq!(*kind, SubqueryKind::Scalar);
        assert!(
            crate::opt::decorrelate::references_outer_row(subplan),
            "the subquery still reads the enclosing row — decorrelation did              not (and documents that it does not) reach a Project target list"
        );
    }

    /// And the column that correlation reads is still there to be read.
    /// `SELECT a, (SELECT ... WHERE x.id = t.id) FROM t` needs only `a`
    /// locally, so projection pruning used to shrink the outer scan to
    /// `[a]` — while the subquery went on saying "outer column 0", which was
    /// `t.id` before the pruning and `a` after it. Measured, not theorised:
    /// that is what the optimizer produced before `ProjectionPruning` learned
    /// to leave correlated plans alone.
    #[test]
    fn projection_pruning_keeps_the_column_a_correlated_subquery_reads() {
        let plan = lower("SELECT a, (SELECT count(*) FROM t x WHERE x.id = t.id) FROM t").unwrap();
        let (opt, _passes) = crate::opt::optimize_default(plan);
        let LogicalPlan::Project { input, exprs } = &opt else {
            panic!("expected a Project at the root, got {opt:?}");
        };
        let LogicalPlan::Scan { projection, .. } = input.as_ref() else {
            panic!("expected a Scan under the Project, got {input:?}");
        };
        assert!(
            projection.contains(&ColId(0)),
            "the outer scan must still read t.id (ColId(0)) — the correlated \
             subquery reads it by position; got {projection:?}"
        );
        // And the position it reads by is still t.id's own: the subquery
        // says outer column 0, and `id` is column 0 of the scan's output.
        assert_eq!(projection[0], ColId(0));
        let Expr::Subquery { subplan, .. } = &exprs[1].0 else {
            panic!("expected a subquery in the target list");
        };
        let mut outer_indices = Vec::new();
        collect_outer_indices(subplan, &mut outer_indices);
        assert_eq!(
            outer_indices,
            vec![0],
            "the correlation reads outer column 0, which is where t.id is"
        );
    }

    /// Every `ColumnRef { relation: 1 }` index reachable in `plan`'s own
    /// expressions — the outer-row positions a correlated subplan reads.
    fn collect_outer_indices(plan: &LogicalPlan, out: &mut Vec<u16>) {
        plan.for_each_expr(&mut |e| {
            e.any(&mut |x| {
                if let Expr::Column(c) = x {
                    if c.relation == 1 {
                        out.push(c.index);
                    }
                }
                false
            });
        });
        plan.for_each_input(&mut |c| collect_outer_indices(c, out));
    }
}
