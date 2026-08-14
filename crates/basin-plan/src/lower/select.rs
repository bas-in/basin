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
//! BY` / `HAVING` — including `ROLLUP` / `CUBE` / `GROUPING SETS`, which
//! [`lower_group_by`] expands and [`build_grouping_sets_union`] turns into a
//! `UNION ALL` of ordinary aggregates rather than a `grouping_sets`-bearing
//! [`LogicalPlan::Aggregate`], for the reason spelled out there —
//! `ORDER BY`, `LIMIT` / `OFFSET`, `VALUES`, a `FROM`-less
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
//! [`lower_set_op_sort_key`]. A set-returning function in `FROM` —
//! `generate_series`/`unnest`, plain or `LATERAL`-correlated, under
//! PostgreSQL's own output-column naming rule — is lowered by
//! [`build_range_function`], and a correlated one is combined into a
//! [`LogicalPlan::LateralJoin`] by [`combine_from_items`] /
//! [`build_lateral_join_expr`]. Everything else — `OVER (w ...)` extending a
//! named window, a `LATERAL` *subquery* (as opposed to a `LATERAL`
//! function), a genuine subquery in `FROM` (anything but a bare `VALUES`
//! list), `WITH ORDINALITY`, `ROWS FROM (...)`, a self-naming or
//! composite-returning set-returning function in `FROM` (see
//! [`SELF_NAMING_SRFS`]),
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
    node::Node as NodeEnum, AExprKind, BoolExprType, GroupingSetKind, JoinType, LimitOption, Node,
    SelectStmt, SetOperation, SortByDir, SortByNulls,
};

use basin_pgtype::PgType;

use crate::lower::colname::figure_colname_or_unnamed;
use crate::lower::expr::{
    best_effort_type, lower_expr, lower_sort_by, ColumnResolver, FunctionResolver, LowerCtx,
    OperatorResolver, SubqueryLowerer,
};
use crate::lower::LowerError;
use crate::{
    ColId, ColumnRef, CteId, Datum, Expr, FrameBound, JoinKind, LogicalPlan, Schema, SetOpKind,
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
    lower_select_with_outer(node, tables, operators, functions, None, &[])
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
/// `ctes` is the second such seam, and closes the same kind of gap for names
/// that `outer` closes for columns: the `WITH`-list entries visible where the
/// subquery was written. Postgres scopes a CTE lexically over the whole
/// statement, so `WITH a AS (...) SELECT (SELECT count(*) FROM a)` is legal
/// there; this argument is what makes it legal here, where the recursion used
/// to restart from an empty list and report `UnknownName("a")`.
fn lower_select_with_outer(
    node: &Node,
    tables: &dyn TableResolver,
    operators: &dyn OperatorResolver,
    functions: &dyn FunctionResolver,
    outer: Option<&dyn ColumnResolver>,
    ctes: &[CteBinding],
) -> Result<LogicalPlan, LowerError> {
    let Some(NodeEnum::SelectStmt(stmt)) = node.node.as_ref() else {
        return Err(LowerError::Malformed("expected a SelectStmt node"));
    };
    let res = Resolvers {
        tables,
        operators,
        functions,
        outer,
        ctes,
    };
    let (plan, _schema) = lower_select_stmt_ctx(stmt, &res, ctes)?;
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
    /// The `WITH`-list entries in scope for this statement, so that a
    /// subquery lowered out of any clause can still see them.
    ///
    /// This duplicates the `ctes: &[CteBinding]` argument already threaded
    /// down the `FROM`-building path, and does so deliberately: that argument
    /// reaches [`build_range_var`], which is what makes `FROM a` resolve, but
    /// it never reaches [`SelectSubqueries`], which is what makes `(SELECT
    /// ... FROM a)` resolve. Rather than add the argument to the six clause
    /// helpers that sit between them, [`lower_select_stmt_body`] republishes
    /// it here once, on entry — see there for why that keeps the two copies
    /// from drifting.
    ctes: &'a [CteBinding],
}

impl<'a> Resolvers<'a> {
    /// `columns` is the calling clause's own resolver (its `Scope`, wrapped
    /// — already falling back to `self.outer` itself, if any) — handed to a
    /// nested subquery as *its* `outer`, one level further in. This is the
    /// single seam that turns a fixed, statement-wide `outer` into the
    /// correctly-nested chain a multiply-nested correlated subquery needs.
    ///
    /// `ctes` rides along unchanged, and unlike `outer` needs no re-tagging:
    /// a CTE name is lexically scoped, so the enclosing statement's list is
    /// exactly the list the subquery should see.
    fn subqueries(&self, columns: &'a dyn ColumnResolver) -> SelectSubqueries<'a> {
        SelectSubqueries {
            tables: self.tables,
            operators: self.operators,
            functions: self.functions,
            outer: columns,
            ctes: self.ctes,
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
    /// The `WITH`-list entries visible where the subquery was written — see
    /// [`Resolvers::ctes`].
    ctes: &'a [CteBinding],
}

impl<'a> SubqueryLowerer for SelectSubqueries<'a> {
    fn lower(&self, subselect: &Node) -> Result<LogicalPlan, LowerError> {
        lower_select_with_outer(
            subselect,
            self.tables,
            self.operators,
            self.functions,
            Some(self.outer),
            self.ctes,
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

/// A column that `JOIN … USING (c)` / `NATURAL JOIN` merged: the *one* column
/// named `c` that such a join produces, in place of the two the inputs
/// contributed.
///
/// All three of Postgres's rules for it are measured against a live
/// PostgreSQL 18.2 server rather than recalled, by reading back what the
/// server deparses for a view over each join kind:
///
/// - `ut JOIN uu USING (id)` and `ut LEFT JOIN uu USING (id)` deparse the
///   merged column as `ut.id` — the LEFT input's column. Sound because an
///   inner join matched both sides and a left join null-extends only the
///   right.
/// - `ut RIGHT JOIN uu USING (id)` deparses it as `uu.id` — the RIGHT
///   input's, by the mirror argument.
/// - `ut FULL JOIN uu USING (id)` deparses it as a bare `id` that belongs to
///   neither input: it is `COALESCE(ut.id, uu.id)`, and measurably so — with
///   `ut(id)` = 10, 20 and `uu(id)` = 20, 30, `SELECT id FROM ut FULL JOIN uu
///   USING (id)` returns 10, 20, 30 while `ut.id` returns 10, 20, NULL. This
///   is the case where picking a side is silently wrong rather than an error,
///   so it is the one [`build_join_expr`] materializes through a real
///   [`LogicalPlan::Project`] instead of pointing at an input column.
///
/// The merged column is also *unqualified*: `SELECT id` finds it, while
/// `SELECT ut.id` still reaches the underlying column past it. Hence
/// [`Scope::resolve`] consulting `merged` only on the unqualified path.
#[derive(Debug, Clone)]
struct MergedColumn {
    name: String,
    /// Flat index, in the enclosing [`Scope`], of the column whose value *is*
    /// the merged value.
    index: u16,
    /// Flat indices `*` must not expand — the underlying column of each side,
    /// plus anything an earlier `USING` on the same name already hid.
    /// `index` itself is excluded by [`Scope::star_columns`] separately, since
    /// for every kind but `FULL` it is one of these.
    hidden: Vec<u16>,
}

/// The ordered, concatenated column list a clause resolves column references
/// against — built up as `FROM` items are processed, and unchanged by
/// `WHERE`/`Filter` (a predicate narrows rows, never columns).
#[derive(Debug, Clone, Default)]
struct Scope {
    relations: Vec<ScopeRelation>,
    /// Columns merged away by `JOIN … USING` / `NATURAL JOIN`, in the order
    /// `*` must emit them — which is *before* every remaining column of
    /// either side. Measured: with `nt(a, id, b, k)` and `nu(c, k, id, d)`,
    /// `SELECT * FROM nt JOIN nu USING (k, id)` yields `k, id, a, b, c, d` —
    /// so the merged block leads, in the order the `USING` clause wrote them,
    /// not in either table's order. `NATURAL JOIN` instead orders them by the
    /// left input (`nt NATURAL JOIN nu` yields `id, k, a, b, c, d`), which
    /// [`natural_using_columns`] produces by walking the left scope.
    merged: Vec<MergedColumn>,
}

impl Scope {
    fn empty() -> Self {
        Scope::default()
    }

    fn single(qualifier: String, schema: Schema) -> Self {
        Scope {
            relations: vec![ScopeRelation { qualifier, schema }],
            merged: Vec::new(),
        }
    }

    /// Place `other`'s columns after this scope's own.
    ///
    /// `other`'s merged columns come along, re-addressed into the combined
    /// flat space: a `USING` join nested on the right of another join keeps
    /// exactly one visible copy of its own merged column, and forgetting to
    /// shift it here would silently point at one of *this* scope's columns.
    fn concat(mut self, mut other: Scope) -> Self {
        let shift = self.total_len() as u16;
        self.relations.append(&mut other.relations);
        for mut m in other.merged {
            m.index += shift;
            for h in &mut m.hidden {
                *h += shift;
            }
            self.merged.push(m);
        }
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
        // A `USING`/`NATURAL` merged column REPLACES the two columns it
        // merged, so it enters the ambiguity count as one candidate and they
        // enter as none. That is what makes `SELECT id FROM ut JOIN uu USING
        // (id)` legal while `SELECT id FROM ut JOIN uu USING (id), uv` — a
        // third relation also carrying `id` — is still "column reference
        // \"id\" is ambiguous", both measured on PostgreSQL 18.2.
        let mut found = None;
        for m in &self.merged {
            if &m.name == name {
                if found.is_some() {
                    return None; // ambiguous
                }
                found = Some(m.index as usize);
            }
        }
        let mut offset = 0usize;
        for rel in &self.relations {
            if let Some(pos) = rel.schema.iter().position(|(n, _)| n == name) {
                let flat = offset + pos;
                if !self.is_merged_away(flat as u16) {
                    if found.is_some() {
                        return None; // ambiguous
                    }
                    found = Some(flat);
                }
            }
            offset += rel.schema.len();
        }
        found.map(|idx| ColumnRef {
            relation: 0,
            index: idx as u16,
            name: name.clone(),
        })
    }

    /// Whether the flat index `i` is one a `USING`/`NATURAL` join subsumed —
    /// either an underlying column of a merged pair, or the visible
    /// representative itself (which for every kind but `FULL` is one of that
    /// pair). Such a column is unreachable unqualified and absent from `*`,
    /// but still reachable as `t.c`.
    fn is_merged_away(&self, i: u16) -> bool {
        self.merged
            .iter()
            .any(|m| m.index == i || m.hidden.contains(&i))
    }

    /// The `(name, type)` at a given flat column index, if any — the whole
    /// of what makes a column's real catalog type reachable during lowering,
    /// without needing a second, catalog-free type inference pass
    /// (`crate::schema::expr_type` cannot help here: it itself needs an
    /// input schema, which for anything rooted at a `Scan` is exactly what
    /// that module's own docs say it cannot produce without a catalog — this
    /// `Scope` already carries the real one).
    ///
    /// The name rides along because [`ScopeResolver::column_type`]
    /// cross-checks a correlated reference against it — see there.
    fn flat_entry(&self, index: u16) -> Option<(&str, PgType)> {
        let mut offset = 0u16;
        for rel in &self.relations {
            let len = rel.schema.len() as u16;
            if index < offset + len {
                return rel
                    .schema
                    .get((index - offset) as usize)
                    .map(|(n, ty)| (n.as_str(), *ty));
            }
            offset += len;
        }
        None
    }

    /// Every `(name, flat index)` pair `*` or `t.*` expands to.
    ///
    /// Unqualified `*` leads with the `USING`/`NATURAL` merged columns and
    /// then skips the columns they subsumed — measured, not inferred: with
    /// `nt(a, id, b, k)` and `nu(c, k, id, d)`, PostgreSQL 18.2 gives
    /// `SELECT * FROM nt JOIN nu USING (k, id)` the columns `k, id, a, b, c,
    /// d`. So the merged block leads, in `USING`-clause order, and neither
    /// `nt.k`/`nt.id` nor `nu.k`/`nu.id` appears again behind it.
    ///
    /// Qualified `t.*` is deliberately NOT merged: the same server gives
    /// `SELECT ut.* FROM ut JOIN uu USING (id)` the columns `a, id, b` — `ut`'s
    /// own three, in `ut`'s own order, with `id` in its natural position
    /// rather than hoisted or dropped. Hence `qualifier.is_some()` taking the
    /// plain path.
    fn star_columns(&self, qualifier: Option<&str>) -> Vec<(String, u16)> {
        let mut out = Vec::new();
        if qualifier.is_none() {
            for m in &self.merged {
                out.push((m.name.clone(), m.index));
            }
        }
        let mut offset = 0usize;
        for rel in &self.relations {
            let matches = qualifier.is_none_or(|q| rel.qualifier == q);
            if matches {
                for (i, (name, _)) in rel.schema.iter().enumerate() {
                    let flat = (offset + i) as u16;
                    if qualifier.is_none() && self.is_merged_away(flat) {
                        continue;
                    }
                    out.push((name.clone(), flat));
                }
            }
            offset += rel.schema.len();
        }
        out
    }

    /// Every `(name, flat index)` in the scope, in plain flat order, with no
    /// `USING` merging applied at all.
    ///
    /// This is the *plan's* column list, not SQL's `*`. The distinction only
    /// began to matter once [`Scope::star_columns`] learned to reorder and
    /// drop columns: an identity projection built to widen a plan (see
    /// [`materialize_agg_inputs`]) must reproduce indices `0..total_len()`
    /// exactly, so appended expressions land at the indices their callers
    /// already computed. Feeding it `*` semantics under a `USING` join would
    /// silently renumber every column below.
    fn flat_columns(&self) -> Vec<(String, u16)> {
        let mut out = Vec::new();
        let mut i = 0u16;
        for rel in &self.relations {
            for (name, _) in &rel.schema {
                out.push((name.clone(), i));
                i += 1;
            }
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

    /// The type at `index` in this resolver's own [`Scope`], but only if the
    /// column there really is the one named `name`.
    ///
    /// The name cross-check is what makes [`ScopeResolver::column_type`]'s
    /// correlated case fail closed rather than wrong — see there.
    fn column_type_local(&self, index: u16, name: &str) -> PgType {
        match self.scope.flat_entry(index) {
            Some((n, ty)) if n == name => ty,
            _ => PgType::UNKNOWN,
        }
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

    /// The type of a column this resolver returned.
    ///
    /// A local hit (`relation == 0`) is a direct lookup in [`Scope`], which
    /// carries the real catalog schema of every `FROM` item.
    ///
    /// An [`OUTER_REF`] hit came from `self.outer`, and its `index` is in
    /// *that* resolver's own flat space — so the question is handed straight
    /// back to `outer` re-tagged as local, which is what it was before
    /// [`ColumnResolver::resolve`] above rewrote it. That is exact for a
    /// singly-correlated reference and, importantly, **fails closed** for a
    /// doubly-correlated one: the `OUTER_REF` tag collapses a deeper chain
    /// (see this type's own docs), so `index` could belong to a
    /// grandparent's space instead. [`ScopeResolver::column_type_local`]'s
    /// name cross-check is what catches that — a chain only forms when the
    /// parent's scope did *not* contain the name, so a parent lookup at that
    /// index cannot match the name, and the answer is `UNKNOWN` rather than
    /// some other column's type. A wrong type here would be a misresolved
    /// overload, which is the exact failure mode this method exists to
    /// remove.
    fn column_type(&self, c: &ColumnRef) -> PgType {
        if c.relation == OUTER_REF {
            let Some(outer) = self.outer else {
                return PgType::UNKNOWN;
            };
            return outer.column_type(&ColumnRef {
                relation: 0,
                index: c.index,
                name: c.name.clone(),
            });
        }
        self.column_type_local(c.index, &c.name)
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
    // Republish `ctes` onto the resolver bundle, once, at the single point
    // every clause of a statement body is lowered from. Every `expr_ctx` below
    // reads it back out through [`Resolvers::subqueries`], so a `(SELECT ...
    // FROM a)` anywhere in this statement — SELECT list, WHERE, HAVING, an
    // `IN`/`EXISTS`, a `VALUES` row, even `LIMIT` — sees the same `WITH` list
    // that `FROM a` sees.
    //
    // Done here rather than by adding a `ctes` argument to `apply_where`,
    // `lower_values`, `apply_limit` and the rest precisely so the two copies
    // cannot drift: `ctes` is the caller's authority and this line is the only
    // thing that reads it into `res`, so they are equal by construction from
    // here down. `lower_with_clause` extends the list and then calls straight
    // back into this function, which re-runs the same line with the extended
    // one.
    let res = &Resolvers { ctes, ..*res };

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

    let (group_exprs, grouping_sets) = lower_group_by(&stmt.group_clause, &ctx)?;
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

    // `grouping_sets.is_some()` is its own term rather than being folded into
    // the `group_exprs` check: `GROUP BY GROUPING SETS (())` and `GROUP BY ()`
    // name no grouping expressions at all, yet are still aggregate queries —
    // they produce exactly one row, the grand total.
    let has_agg = !group_exprs.is_empty()
        || grouping_sets.is_some()
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
        let agg_plan = match &grouping_sets {
            // `ROLLUP` / `CUBE` / `GROUPING SETS` become a `UNION ALL` of
            // ordinary aggregates rather than a `grouping_sets`-bearing
            // `Aggregate` — see [`build_grouping_sets_union`] for why that is
            // the shape that actually runs.
            Some(sets) => build_grouping_sets_union(base_plan, group_exprs, aggs, sets, &ctx),
            None => LogicalPlan::Aggregate {
                input: Box::new(base_plan),
                group: group_exprs,
                aggs,
                grouping_sets: None,
            },
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
                .map(|r| best_effort_type(&r[i], &resolver))
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
    /// This item's own expressions reference a relation to its LEFT in the
    /// same `FROM` clause — the defining property of `LATERAL`. Only a
    /// set-returning function item can report `true` today (see
    /// [`build_range_function`]); it is what makes the caller combine this
    /// item with [`LogicalPlan::LateralJoin`] rather than an ordinary
    /// [`LogicalPlan::Join`], so the inner side is re-evaluated per outer
    /// row instead of once.
    correlated: bool,
}

impl FromBuilt {
    /// The plain, uncorrelated case: no `WHERE`-foldable join at the root and
    /// nothing reaching left.
    fn leaf(plan: LogicalPlan, scope: Scope) -> Self {
        FromBuilt {
            plan,
            scope,
            top_join_left_len: None,
            correlated: false,
        }
    }
}

/// Combine an already-built left side with the item to its right.
///
/// A comma in `FROM` is a cross join — **unless** the right item reaches back
/// into the left one, which for a set-returning function needs no `LATERAL`
/// keyword at all: `FROM t, generate_series(1, t.id) g` and `FROM t, LATERAL
/// generate_series(1, t.id) g` return the identical four rows on a live
/// PostgreSQL 18.2 server (`lt(id)` = 1, 3, 0; `id = 0` contributes no row at
/// all, since `generate_series(1, 0)` is empty and an inner lateral join
/// drops an outer row whose inner side produced nothing). So the choice is
/// driven by whether the lowered item actually carries an [`OUTER_REF`], not
/// by `RangeFunction::lateral`.
///
/// [`LogicalPlan::LateralJoin`] is used only when that is the case:
/// `opt::projection` and `opt::pushdown` both treat it as an opaque barrier
/// (see their module docs), so an uncorrelated item is worth keeping as an
/// ordinary `Join` that those rules can still optimize.
fn combine_from_items(acc: FromBuilt, rhs: FromBuilt) -> FromBuilt {
    let left_len = acc.scope.total_len();
    let scope = acc.scope.concat(rhs.scope);
    if rhs.correlated {
        return FromBuilt {
            plan: LogicalPlan::LateralJoin {
                outer: Box::new(acc.plan),
                inner: Box::new(rhs.plan),
                kind: JoinKind::Inner,
            },
            scope,
            // Not a `Join` at the root, so a `WHERE` conjunct has no `on`
            // list to fold into — `apply_where` builds a plain `Filter`
            // above the lateral join instead, which is where Postgres
            // evaluates it too.
            top_join_left_len: None,
            correlated: false,
        };
    }
    FromBuilt {
        plan: LogicalPlan::Join {
            left: Box::new(acc.plan),
            right: Box::new(rhs.plan),
            kind: JoinKind::Cross,
            on: vec![],
            filter: None,
        },
        scope,
        top_join_left_len: Some(left_len),
        correlated: false,
    }
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
    let mut acc = build_from_item(first, res, ctes, None)?;
    for item in iter {
        let rhs = build_from_item(item, res, ctes, Some(&acc.scope))?;
        acc = combine_from_items(acc, rhs);
    }
    Ok(Some(acc))
}

/// `left`, when present, is everything already in scope to this item's LEFT
/// in the same `FROM` clause — what a `LATERAL` item may reference. It is
/// `None` for the first item of a `FROM` list (nothing precedes it) and for
/// a join's own left arm.
fn build_from_item(
    item: &Node,
    res: &Resolvers,
    ctes: &[CteBinding],
    left: Option<&Scope>,
) -> Result<FromBuilt, LowerError> {
    match item.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => build_range_var(rv, res, ctes),
        Some(NodeEnum::JoinExpr(je)) => build_join_expr(je, res, ctes),
        Some(NodeEnum::RangeSubselect(rs)) => build_range_subselect(rs, res, ctes),
        Some(NodeEnum::RangeFunction(rf)) => build_range_function(rf, res, left),
        Some(_) => Err(LowerError::Unsupported(
            "this FROM item is not yet lowered".into(),
        )),
        None => Err(LowerError::Malformed("empty FROM item")),
    }
}

/// Set-returning functions whose FROM-position output column is named by the
/// function itself — a single NAMED `OUT` parameter, or a composite return
/// type — rather than by the table alias. `pg_proc.proargnames` is the
/// discriminator, checked live on PostgreSQL 18.2:
///
/// ```text
/// SELECT proname, proargnames FROM pg_proc WHERE proretset AND proname IN (...);
///  jsonb_array_elements | {from_json,value}
///  jsonb_each           | {from_json,key,value}
///  unnest(tsvector)     | {tsvector,lexeme,positions,weights}
/// ```
///
/// `jsonb_array_elements(...) AS x` still yields a column named `value`, not
/// `x` — the table alias renames nothing here. [`FuncSig`] carries no
/// `proargnames` (and no composite attribute list), so Basin cannot produce
/// that name, and `jsonb_each`-style entries are multi-column relations this
/// single-column lowering could not represent even if it could name them.
/// Naming the cases explicitly is what lets the refusal say *why* instead of
/// falling through to a generic "no such function".
///
/// [`FuncSig`]: basin_pgtype::func::FuncSig
const SELF_NAMING_SRFS: &[&str] = &[
    "jsonb_array_elements",
    "jsonb_array_elements_text",
    "jsonb_each",
    "jsonb_each_text",
    "json_array_elements",
    "json_array_elements_text",
    "json_each",
    "json_each_text",
];

/// Set-returning functions with no `proargnames` at all, whose single output
/// column is therefore named by the table alias (or, with no alias, by the
/// function name). Verified live — the `proargnames` column is NULL for
/// every one of these:
///
/// ```text
/// SELECT * FROM generate_series(1,3);        -- column "generate_series"
/// SELECT * FROM generate_series(1,3) g;      -- column "g"
/// SELECT * FROM generate_series(1,3) AS g(i);-- column "i"
/// SELECT * FROM unnest(ARRAY[10,20]) u;      -- column "u"
/// ```
///
/// An allowlist rather than "anything `basin_pgtype::func` says is
/// set-returning", so that a future catalog entry for a self-naming or
/// composite-returning SRF makes [`build_range_function`] refuse rather than
/// silently name its column after the alias.
///
/// `unnest` is on both sides of that line in real Postgres: the `anyarray`
/// overload (oid 2331, the one `basin_pgtype::func` tabulates) has no
/// `proargnames`, while a separate `unnest(tsvector)` row returns a
/// four-column `record` and does. Basin's catalog carries only the former,
/// so `unnest(some_tsvector)` finds no overload and is refused by
/// [`builtin_srf_column_type`] rather than reaching this list.
const ALIAS_NAMED_SRFS: &[&str] = &[
    "generate_series",
    "generate_subscripts",
    "unnest",
    "jsonb_object_keys",
    "json_object_keys",
    "string_to_table",
];

/// The type of the single column a set-returning function contributes in
/// `FROM` position, or `None` when Basin's builtin catalog cannot say.
///
/// Resolved by name *and* argument types, not by the [`FuncId`] the
/// [`FunctionResolver`] seam returned, because one `pg_proc` oid can have
/// several result types: `unnest` is oid 2331 for every array element type,
/// so the oid alone cannot distinguish `unnest(int[]) -> int4` from
/// `unnest(text[]) -> text`. The argument types are `best_effort_type`'s —
/// a column's real catalog type, a literal/cast/parameter's own, and
/// `unknown` for anything else, which then picks the first
/// implicitly-coercible overload — the same best-effort resolution
/// `lower_func_call` itself already performs one step earlier for the oid.
fn builtin_srf_column_type(name: &str, arg_types: &[PgType]) -> Option<PgType> {
    if !ALIAS_NAMED_SRFS.contains(&name) {
        return None;
    }
    let arg_oids: Vec<basin_pgtype::Oid> = arg_types.iter().map(|t| t.oid).collect();
    let sig = basin_pgtype::func::resolve(name, &arg_oids)?;
    (sig.kind == basin_pgtype::func::FuncKind::SetReturning).then(|| PgType::new(sig.ret))
}

/// `FROM generate_series(1, 3) [AS g [(i)]]`, including the `LATERAL` form
/// whose arguments reference a relation to its left.
///
/// # The plan shape
///
/// A set-returning function in `FROM` is exactly one row of nothing expanded
/// into a relation, which is what [`LogicalPlan::ProjectSet`] over a
/// one-row [`LogicalPlan::Empty`] already means — no new IR node is needed.
/// Correlation is carried by [`OUTER_REF`] columns inside the SRF's own
/// arguments and combined by [`combine_from_items`] into a
/// [`LogicalPlan::LateralJoin`]; `basin-exec`'s builder rebuilds the inner
/// side per outer row and substitutes those columns (`bind_outer`), the same
/// mechanism a correlated scalar subquery already uses.
///
/// # Output column naming
///
/// PostgreSQL's rule, in the order it applies (each clause verified live on
/// PostgreSQL 18.2 — see [`ALIAS_NAMED_SRFS`] and [`SELF_NAMING_SRFS`]):
///
/// 1. A column-alias list wins: `AS g(i)` names the column `i`. Positional,
///    and an over-long list is an error (`table "g" has 1 columns available
///    but 2 columns specified`) — [`apply_column_alias_list`] is that rule
///    already, shared with `WITH x(a, b)` and `FROM (VALUES ...) AS v(i, s)`.
/// 2. Otherwise a single named `OUT` parameter or a composite return type
///    wins and the table alias renames nothing — refused here, see
///    [`SELF_NAMING_SRFS`].
/// 3. Otherwise the table alias names the column, and with no alias the
///    function name does. The alias is also the qualifier, so `g.i` and
///    `generate_series.generate_series` both resolve.
///
/// # Refused
///
/// `WITH ORDINALITY`, `ROWS FROM (...)`, a per-function column definition
/// list (`AS t(a int)`, which only applies to a `record`-returning function
/// Basin has none of), a nested set-returning function in the arguments
/// (which Postgres rejects outright), and a plain non-set-returning function
/// in `FROM` (`FROM abs(-1)` is legal Postgres and is a one-row relation,
/// not lowered here). Each returns [`LowerError::Unsupported`] naming the
/// construct, so the query falls back rather than returning a wrong shape.
fn build_range_function(
    rf: &pg_query::protobuf::RangeFunction,
    res: &Resolvers,
    left: Option<&Scope>,
) -> Result<FromBuilt, LowerError> {
    if rf.ordinality {
        return Err(LowerError::Unsupported(
            "WITH ORDINALITY is not yet lowered".into(),
        ));
    }
    if rf.is_rowsfrom {
        return Err(LowerError::Unsupported(
            "ROWS FROM (...) in FROM is not yet lowered".into(),
        ));
    }
    if !rf.coldeflist.is_empty() {
        return Err(LowerError::Unsupported(
            "a column definition list on a function in FROM is not yet lowered".into(),
        ));
    }
    // `is_rowsfrom` is false, so the parser produced exactly one entry; a
    // different count would be a parse tree this code has never seen.
    let [only] = rf.functions.as_slice() else {
        return Err(LowerError::Malformed(
            "a function in FROM with no single call",
        ));
    };
    // Each entry is a two-element `List`: the call itself, and that call's
    // own column definition list (`Node { node: None }` when absent —
    // confirmed by dumping the parse tree of `SELECT * FROM
    // generate_series(1,3)`).
    let Some(NodeEnum::List(l)) = only.node.as_ref() else {
        return Err(LowerError::Malformed(
            "a function in FROM is not a (call, coldeflist) pair",
        ));
    };
    let Some(call_node) = l.items.first() else {
        return Err(LowerError::Malformed("a function in FROM with no call"));
    };
    if l.items.iter().skip(1).any(|n| n.node.is_some()) {
        return Err(LowerError::Unsupported(
            "a column definition list on a function in FROM is not yet lowered".into(),
        ));
    }
    let Some(NodeEnum::FuncCall(fc)) = call_node.node.as_ref() else {
        return Err(LowerError::Unsupported(
            "this FROM item is not yet lowered".into(),
        ));
    };
    let Some(NodeEnum::String(fname)) = fc.funcname.last().and_then(|n| n.node.as_ref()) else {
        return Err(LowerError::Malformed("function name is not a name"));
    };
    let fname = fname.sval.as_str();
    if SELF_NAMING_SRFS.contains(&fname) {
        return Err(LowerError::Unsupported(format!(
            "`{fname}` in FROM names its own output column(s) from its OUT parameters, which is not yet lowered"
        )));
    }

    // The arguments resolve against everything to this item's LEFT, and
    // nothing of its own — a set-returning function contributes no columns
    // its own arguments could reference. Wrapping an empty scope means every
    // hit goes through `ScopeResolver`'s `outer` fallback and comes back
    // tagged `OUTER_REF`, which is exactly the marker
    // `LogicalPlan::LateralJoin`'s inner side is defined in terms of. With
    // no left scope, `res.outer` is still consulted directly, so
    // `SELECT (SELECT sum(g) FROM generate_series(1, t.id) g) FROM t` — legal
    // and returning one sum per row on a live server — resolves `t.id` as an
    // ordinary correlated reference into the enclosing query.
    let own = Scope::empty();
    let left_resolver;
    let resolver = match left {
        Some(s) => {
            left_resolver = ScopeResolver::new(s, res.outer);
            ScopeResolver::new(&own, Some(&left_resolver))
        }
        None => ScopeResolver::new(&own, res.outer),
    };
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();
    let expr = lower_expr(call_node, &ctx)?;

    let Expr::SetReturning { args, .. } = &expr else {
        return Err(LowerError::Unsupported(format!(
            "`{fname}` in FROM is not a set-returning function, which is not yet lowered"
        )));
    };
    if args.iter().any(|a| a.contains_srf()) {
        return Err(LowerError::Unsupported(
            "a set-returning function nested in another one's arguments is not yet lowered".into(),
        ));
    }
    // `unnest(a, b)` is not an ordinary call with two arguments: Postgres
    // expands it to one column PER argument, in lockstep, padding the
    // shorter with NULL — verified live, `SELECT * FROM unnest(ARRAY[1,2],
    // ARRAY['a','b'])` returns two columns both named `unnest`. That is a
    // multi-column relation this single-column lowering cannot represent, so
    // it is refused by name rather than falling out of "no such overload".
    if fname == "unnest" && args.len() > 1 {
        return Err(LowerError::Unsupported(
            "multi-argument `unnest(a, b)` in FROM expands to one column per argument and is not yet lowered".into(),
        ));
    }
    let arg_types: Vec<PgType> = args
        .iter()
        .map(|a| best_effort_type(a, ctx.columns))
        .collect();
    let ty = builtin_srf_column_type(fname, &arg_types).ok_or_else(|| {
        LowerError::Unsupported(format!(
            "a set-returning function in FROM with no single-column entry in Basin's builtin catalog (`{fname}` at these argument types) is not yet lowered"
        ))
    })?;

    // A reference reaching left makes this a lateral item. An argument
    // containing a subquery counts too, conservatively: `Expr::any`
    // deliberately does not descend into a subquery's own plan (see its
    // docs), so a correlation hidden inside one would otherwise be missed —
    // and a `LateralJoin` over an uncorrelated inner side is still correct,
    // merely unoptimized.
    let correlated = left.is_some()
        && expr.any(&mut |e| {
            matches!(e, Expr::Column(c) if c.relation == OUTER_REF)
                || matches!(e, Expr::Subquery { .. })
        });

    let default_name = fname.to_string();
    let (qualifier, schema) = match &rf.alias {
        Some(a) if !a.colnames.is_empty() => (
            a.aliasname.clone(),
            apply_column_alias_list(
                vec![(a.aliasname.clone(), ty)],
                &a.colnames,
                "table",
                &a.aliasname,
            )?,
        ),
        Some(a) => (a.aliasname.clone(), vec![(a.aliasname.clone(), ty)]),
        None => (default_name.clone(), vec![(default_name, ty)]),
    };

    let plan = LogicalPlan::ProjectSet {
        input: Box::new(LogicalPlan::Empty {
            produce_one_row: true,
            schema: vec![],
        }),
        srfs: vec![expr],
    };
    Ok(FromBuilt {
        plan,
        scope: Scope::single(qualifier, schema),
        top_join_left_len: None,
        correlated,
    })
}

/// `FROM (subquery) [AS alias [(colnames)]]` — a derived table.
///
/// The body is lowered by the same [`lower_select_stmt_ctx`] that lowers a
/// `WITH` entry's body or a `UNION` arm, so a derived table gets the whole
/// `SELECT` surface (its own `WHERE`, `GROUP BY`, window functions, set
/// operations, a nested `WITH`) for free rather than a second, narrower
/// lowering path. Its output schema becomes exactly one [`ScopeRelation`],
/// which is what makes `x.col` resolve against the alias and the flat column
/// indices line up with the derived plan's own output.
///
/// The enclosing statement's `ctes` are passed straight through: a derived
/// table may reference a `WITH` entry of the query it sits in (`WITH a AS
/// (...) SELECT * FROM (SELECT * FROM a) s` runs on a live server).
///
/// `res.outer` is likewise passed through unchanged, which is the correct
/// scope rule and not an oversight: a non-`LATERAL` subquery in `FROM` may
/// not see the *sibling* `FROM` items of its own query level (that is what
/// `LATERAL` is for, still refused below), but it may see an enclosing query
/// LEVEL's columns — and `res.outer` is precisely the latter, since
/// [`build_from_clause`] never puts a sibling item into it.
///
/// A bare `VALUES` list keeps its own short path. Verified live: `SELECT *
/// FROM (VALUES (1,'a'),(2,'b')) AS v` (and the unaliased `FROM (VALUES
/// ...)`) both name the columns `column1`, `column2`, … — exactly
/// [`lower_values`]'s own default — and `AS v(i, s)` overrides those
/// positionally through the exact same [`apply_column_alias_list`] a CTE's
/// own column-alias list uses (confirmed live: the arity rule and even the
/// error wording differ only in saying `table "v"` instead of `WITH query
/// "x"`).
fn build_range_subselect(
    rs: &pg_query::protobuf::RangeSubselect,
    res: &Resolvers,
    ctes: &[CteBinding],
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
    // A `VALUES` list carrying its own `ORDER BY` is refused rather than
    // lowered, and that refusal has to live HERE rather than being left to
    // `lower_select_stmt_ctx`: that function dispatches a non-empty
    // `values_lists` straight to `lower_values`, which has no `sort_clause`
    // wiring and would silently drop an `ORDER BY` the user actually wrote,
    // answering an ordered question with an unordered relation.
    //
    // `LIMIT`/`OFFSET` are deliberately NOT refused alongside it: unlike
    // `ORDER BY`, `lower_values` does handle them (it ends in `apply_limit`),
    // so refusing them would give up reach this path already had. Every other
    // shape goes through the general path below.
    let is_values = op_kind == SetOperation::SetopNone && !stmt.values_lists.is_empty();
    if is_values && !stmt.sort_clause.is_empty() {
        return Err(LowerError::Unsupported(
            "a VALUES list in FROM with its own ORDER BY is not yet lowered".into(),
        ));
    }
    let (plan, schema) = lower_select_stmt_ctx(stmt, res, ctes)?;
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
    Ok(FromBuilt::leaf(plan, Scope::single(qualifier, schema)))
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
            return Ok(FromBuilt::leaf(plan, scope));
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
    Ok(FromBuilt::leaf(plan, scope))
}

fn build_join_expr(
    je: &pg_query::protobuf::JoinExpr,
    res: &Resolvers,
    ctes: &[CteBinding],
) -> Result<FromBuilt, LowerError> {
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
    let left = build_from_item(larg, res, ctes, None)?;
    let right = build_from_item(rarg, res, ctes, Some(&left.scope))?;

    if right.correlated {
        if je.is_natural || !je.using_clause.is_empty() {
            return Err(LowerError::Unsupported(
                "a LATERAL right side under a JOIN ... USING or NATURAL JOIN is not yet lowered"
                    .into(),
            ));
        }
        return build_lateral_join_expr(je, kind, left, right);
    }

    // `USING`/`NATURAL` names the join columns instead of writing a condition,
    // and merges each pair into one column — a different enough shape to get
    // its own builder. `NATURAL` with no column in common is NOT an error and
    // not handled there: it falls through to the ordinary no-`quals` path
    // below, which produces the `JoinKind::Cross` that Postgres itself
    // produces (`nt NATURAL JOIN nx`, no shared name, deparses back as
    // `nt CROSS JOIN nx` on PostgreSQL 18.2).
    let using = if je.is_natural {
        natural_using_columns(&left.scope, &right.scope)?
    } else {
        using_clause_columns(&je.using_clause)?
    };
    if !using.is_empty() {
        if je.join_using_alias.is_some() {
            return Err(LowerError::Unsupported(
                "JOIN ... USING (...) AS alias is not yet lowered".into(),
            ));
        }
        return build_using_join(kind, left, right, &using);
    }

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
            (on, and_together(leftover, &ctx)?, kind)
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
        correlated: false,
    })
}

/// The column names in a `USING (a, b, c)` clause, in the order written.
///
/// That order is the one `*` uses, and it is the clause's, not either table's:
/// with `nt(a, id, b, k)` and `nu(c, k, id, d)`, PostgreSQL 18.2 deparses
/// `SELECT * FROM nt JOIN nu USING (k, id)` as `SELECT nt.k, nt.id, nt.a,
/// nt.b, nu.c, nu.d` — `k` before `id`, matching the clause.
fn using_clause_columns(clause: &[Node]) -> Result<Vec<String>, LowerError> {
    clause
        .iter()
        .map(|n| match n.node.as_ref() {
            Some(NodeEnum::String(s)) => Ok(s.sval.clone()),
            _ => Err(LowerError::Malformed("USING clause entry is not a name")),
        })
        .collect()
}

/// The implicit `USING` list of a `NATURAL JOIN`: every column name the two
/// sides have in common, **in the order the LEFT side presents them**.
///
/// Measured rather than assumed: `nt(a, id, b, k) NATURAL JOIN nu(c, k, id,
/// d)` deparses on PostgreSQL 18.2 as `nt JOIN nu USING (id, k)` and yields
/// `id, k, a, b, c, d` — `id` first because `nt` lists it first, even though
/// `nu` lists `k` first. Hence walking the left scope, not the right and not
/// the intersection in some canonical order.
///
/// Both sides are read through [`Scope::star_columns`], so a `NATURAL JOIN`
/// over an already-`USING`-merged side sees the merged column once rather
/// than the two it subsumed.
///
/// An empty result is a legitimate answer, not a failure: Postgres turns a
/// `NATURAL JOIN` with nothing in common into a `CROSS JOIN` (`nt NATURAL
/// JOIN nx` deparses as `nt CROSS JOIN nx` and returns all 2×1 rows), which
/// is what [`build_join_expr`] does with it.
fn natural_using_columns(left: &Scope, right: &Scope) -> Result<Vec<String>, LowerError> {
    let right_names: Vec<String> = right
        .star_columns(None)
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    let left_names: Vec<String> = left.star_columns(None).into_iter().map(|(n, _)| n).collect();
    let mut out: Vec<String> = Vec::new();
    for name in &left_names {
        if !right_names.contains(name) || out.contains(name) {
            continue;
        }
        // Postgres refuses rather than picking one, and says so precisely;
        // the same wording is reproduced here because a `NATURAL JOIN` that
        // quietly chose a side would be wrong in a way nothing downstream
        // could detect. Measured: `(ut JOIN uv ON true) NATURAL JOIN uu`,
        // where both `ut` and `uv` have an `id`, gives exactly this.
        if left_names.iter().filter(|n| *n == name).count() > 1 {
            return Err(LowerError::Unsupported(format!(
                "common column name \"{name}\" appears more than once in left table"
            )));
        }
        if right_names.iter().filter(|n| *n == name).count() > 1 {
            return Err(LowerError::Unsupported(format!(
                "common column name \"{name}\" appears more than once in right table"
            )));
        }
        out.push(name.clone());
    }
    Ok(out)
}

/// `t JOIN u USING (c, …)` and its `NATURAL` spelling: an equijoin on the
/// named columns whose result carries **one** column per name instead of two.
///
/// Two halves, and the second is the one that is easy to get silently wrong.
///
/// **The join itself** is ordinary: one `on` pair per name. `LogicalPlan::Join`
/// stores the right side of each pair already rebased into the right input's
/// own index space (see [`split_equijoin_conjuncts`], which calls
/// [`rebase_columns`] for exactly this), and `right.scope`'s indices are in
/// that space already, so they are used as-is.
///
/// **The merged column** is whichever column actually holds the merged value,
/// which depends on the join kind — read off what PostgreSQL 18.2 deparses
/// for a view over each:
///
/// | kind    | `pg_get_viewdef` renders the merged column as |
/// |---------|-----------------------------------------------|
/// | `INNER` | `ut.id`                                       |
/// | `LEFT`  | `ut.id`                                       |
/// | `RIGHT` | `uu.id`                                       |
/// | `FULL`  | a bare `id` belonging to neither input        |
///
/// For the first three, one input's column *is* the answer: an inner join
/// matched both sides, a left join null-extends only the right, a right join
/// only the left. For `FULL` neither is — with `ut(id)` = 10, 20 and `uu(id)`
/// = 20, 30, `SELECT id` returns 10, 20, 30 while `ut.id` returns 10, 20,
/// NULL and `uu.id` returns NULL, 20, 30. So `FULL` (and only `FULL`)
/// materializes `COALESCE(ut.id, uu.id)` through a real
/// [`LogicalPlan::Project`], and the merged column points at that.
///
/// Picking a side there instead would be a *wrong answer* rather than an
/// error, visible only on unmatched rows — which is why it is a `Project`
/// here and not a comment saying it is close enough.
fn build_using_join(
    kind: JoinKind,
    left: FromBuilt,
    right: FromBuilt,
    names: &[String],
) -> Result<FromBuilt, LowerError> {
    let left_len = left.scope.total_len() as u16;

    // Resolve each name on both sides FIRST, while the two scopes are still
    // separate: a name is looked up in one side or the other, never in the
    // combined scope, where it would be ambiguous by construction.
    let mut cols: Vec<(String, u16, u16, PgType)> = Vec::new();
    for name in names {
        let key = std::slice::from_ref(name);
        let l = left.scope.resolve(key).ok_or_else(|| {
            LowerError::UnknownName(format!(
                "column \"{name}\" specified in USING clause does not exist in left table"
            ))
        })?;
        let r = right.scope.resolve(key).ok_or_else(|| {
            LowerError::UnknownName(format!(
                "column \"{name}\" specified in USING clause does not exist in right table"
            ))
        })?;
        // The merged column's type is the left side's; `USING` requires the
        // two to be comparable, and Basin has no common-supertype resolution
        // to run here. Falling back to the right side's when the left reads
        // `UNKNOWN` keeps a subquery-derived side from erasing a real type.
        let lty = left
            .scope
            .flat_entry(l.index)
            .map(|(_, t)| t)
            .unwrap_or(PgType::UNKNOWN);
        let ty = if lty == PgType::UNKNOWN {
            right
                .scope
                .flat_entry(r.index)
                .map(|(_, t)| t)
                .unwrap_or(PgType::UNKNOWN)
        } else {
            lty
        };
        cols.push((name.clone(), l.index, r.index, ty));
    }

    let on: Vec<(Expr, Expr)> = cols
        .iter()
        .map(|(name, l, r, _)| {
            (
                Expr::Column(ColumnRef {
                    relation: 0,
                    index: *l,
                    name: name.clone(),
                }),
                Expr::Column(ColumnRef {
                    relation: 0,
                    index: *r,
                    name: name.clone(),
                }),
            )
        })
        .collect();

    let mut scope = left.scope.concat(right.scope);
    let joined_len = scope.total_len() as u16;
    let join = LogicalPlan::Join {
        left: Box::new(left.plan),
        right: Box::new(right.plan),
        kind,
        on,
        filter: None,
    };

    let is_full = kind == JoinKind::Full;
    let plan = if is_full {
        let identity = scope.flat_columns().into_iter().map(|(name, index)| {
            (
                Expr::Column(ColumnRef {
                    relation: 0,
                    index,
                    name: name.clone(),
                }),
                name,
            )
        });
        let coalesced = cols.iter().map(|(name, l, r, _)| {
            (
                Expr::Coalesce(vec![
                    Expr::Column(ColumnRef {
                        relation: 0,
                        index: *l,
                        name: name.clone(),
                    }),
                    Expr::Column(ColumnRef {
                        relation: 0,
                        index: left_len + *r,
                        name: name.clone(),
                    }),
                ]),
                name.clone(),
            )
        });
        LogicalPlan::Project {
            input: Box::new(join),
            exprs: identity.chain(coalesced).collect(),
        }
    } else {
        join
    };

    if is_full {
        // A relation qualified by `""`, which no SQL text can name: a
        // zero-length delimited identifier is a *parse* error in Postgres
        // (`SELECT ""."id"` → `zero-length delimited identifier`), so these
        // columns are reachable only through `Scope::merged`. Registering them
        // as a relation rather than as loose indices is what keeps
        // `total_len` and `flat_entry` agreeing with the plan's real width.
        scope.relations.push(ScopeRelation {
            qualifier: String::new(),
            schema: cols
                .iter()
                .map(|(name, _, _, ty)| (name.clone(), *ty))
                .collect(),
        });
    }

    for (i, (name, l, r, _)) in cols.iter().enumerate() {
        let mut hidden = vec![*l, left_len + *r];
        // Chained `USING` on one name — `ut JOIN uu USING (id) JOIN uv USING
        // (id)` — merges the already-merged column again. Postgres still shows
        // exactly one `id` there (measured: `id, a, b, c, d, e`), so the older
        // entry is absorbed rather than left beside the new one.
        scope.merged.retain(|m| {
            if &m.name != name {
                return true;
            }
            hidden.push(m.index);
            hidden.extend(m.hidden.iter().copied());
            false
        });
        let index = match kind {
            JoinKind::Right => left_len + *r,
            _ if is_full => joined_len + i as u16,
            _ => *l,
        };
        scope.merged.push(MergedColumn {
            name: name.clone(),
            index,
            hidden,
        });
    }

    Ok(FromBuilt {
        plan,
        scope,
        // `Some` only for a plain inner `USING` join, and then only because
        // the root really is a `LogicalPlan::Join` — the `FULL` case has a
        // `Project` on top, and folding a `WHERE` conjunct into a `FULL`
        // join's `on` would be unsound regardless (see [`FromBuilt`]).
        top_join_left_len: (kind == JoinKind::Inner).then_some(left_len as usize),
        correlated: false,
    })
}

/// An explicit `JOIN LATERAL` whose right side reaches into the left —
/// `t CROSS JOIN LATERAL generate_series(1, t.id) g`, `t LEFT JOIN LATERAL
/// generate_series(1, t.id) g ON true`.
///
/// [`LogicalPlan::LateralJoin`] carries no `on`/`filter` of its own (unlike
/// [`LogicalPlan::Join`]), which decides what is expressible:
///
/// - `CROSS JOIN LATERAL` (no `ON` at all) and `... JOIN LATERAL ... ON true`
///   become a `LateralJoin` of the corresponding kind. `ON true` is how a
///   `LEFT JOIN LATERAL` is always written, and it is the form that matters:
///   verified live, with `lt(id)` = 1, 3, 0, the inner form returns 4 rows
///   and `LEFT JOIN LATERAL generate_series(1, lt.id) g ON true` returns 5,
///   keeping `id = 0` with a NULL — the whole reason to write it.
/// - Any other `ON` condition is refused rather than approximated. For an
///   inner join a [`LogicalPlan::Filter`] above would be equivalent, but for
///   an outer one it emphatically is not (a filter runs *after*
///   null-extension and would delete the very rows the outer join was
///   written to keep), and refusing one shape while silently rewriting the
///   other invites exactly that mistake later.
/// - `RIGHT`/`FULL JOIN LATERAL` is refused; Postgres itself rejects a
///   lateral reference to the left of a right or full join.
fn build_lateral_join_expr(
    je: &pg_query::protobuf::JoinExpr,
    kind: JoinKind,
    left: FromBuilt,
    right: FromBuilt,
) -> Result<FromBuilt, LowerError> {
    let scope = left.scope.concat(right.scope);
    let plan = LogicalPlan::LateralJoin {
        outer: Box::new(left.plan),
        inner: Box::new(right.plan),
        kind: match kind {
            JoinKind::Inner => JoinKind::Inner,
            JoinKind::Left => JoinKind::Left,
            other => {
                return Err(LowerError::Unsupported(format!(
                    "a LATERAL right side under a {other:?} join is not yet lowered"
                )))
            }
        },
    };
    match je.quals.as_deref() {
        None => {}
        Some(q) if is_literal_true(q) => {}
        Some(_) => {
            return Err(LowerError::Unsupported(
                "a JOIN LATERAL with an ON condition other than `true` is not yet lowered".into(),
            ))
        }
    }
    Ok(FromBuilt {
        plan,
        scope,
        top_join_left_len: None,
        correlated: false,
    })
}

/// Whether a join condition is the literal `TRUE` — the `ON true` that a
/// `LEFT JOIN LATERAL` is conventionally written with, since SQL requires
/// *some* condition there and there is nothing to condition on.
fn is_literal_true(node: &Node) -> bool {
    let Some(NodeEnum::AConst(c)) = node.node.as_ref() else {
        return false;
    };
    matches!(
        c.val.as_ref(),
        Some(pg_query::protobuf::a_const::Val::Boolval(b)) if b.boolval && !c.isnull
    )
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
            let filter = and_together(leftover, &ctx)?;
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
            let predicate = and_together(exprs, &ctx)?
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
fn and_together(exprs: Vec<Expr>, ctx: &LowerCtx) -> Result<Option<Expr>, LowerError> {
    let mut iter = exprs.into_iter();
    let Some(mut acc) = iter.next() else {
        return Ok(None);
    };
    for next in iter {
        let op = ctx
            .operators
            .resolve(
                "AND",
                Some(best_effort_type(&acc, ctx.columns)),
                best_effort_type(&next, ctx.columns),
            )
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

/// One grouping set, as positions into the flattened grouping-expression list
/// `lower_group_by` returns alongside it.
type GroupingSetIndices = Vec<u16>;

/// PostgreSQL 18.2 refuses `CUBE` past 12 elements ("CUBE is limited to 12
/// elements", measured live). The same ceiling is applied to `ROLLUP`, whose
/// expansion is linear rather than exponential but whose combination with
/// other clause items is not.
const MAX_CUBE_ELEMENTS: usize = 12;

/// PostgreSQL caps a clause's total grouping sets at 4096. The cap matters
/// here for a reason it does not have on a real server: this lowering emits
/// one `Aggregate` branch PER SET, so an unbounded product would build an
/// unbounded plan rather than merely a slow one.
const MAX_GROUPING_SETS: usize = 4096;

/// Lower a `GROUP BY` clause.
///
/// Returns the flattened, de-duplicated list of grouping expressions and —
/// only when `ROLLUP` / `CUBE` / `GROUPING SETS` was written — the grouping
/// sets those expand to, as index lists into that flat list. A plain `GROUP
/// BY a, b` returns `None` for the second element: it is a single implicit
/// set, and saying so keeps every plain aggregate plan byte-for-byte what it
/// was before grouping sets existed.
///
/// The flat list's ORDER is the output column order of the aggregate, which
/// is what [`rewrite_post_agg`] rewrites the SELECT list, `HAVING` and `ORDER
/// BY` against — once, against the full list, regardless of how many sets
/// there are. That is what lets [`build_grouping_sets_union`] vary the plan
/// underneath without anything above it knowing.
fn lower_group_by(
    group_clause: &[Node],
    ctx: &LowerCtx,
) -> Result<(Vec<Expr>, Option<Vec<GroupingSetIndices>>), LowerError> {
    let uses_sets = group_clause
        .iter()
        .any(|n| matches!(n.node.as_ref(), Some(NodeEnum::GroupingSet(_))));

    if !uses_sets {
        let exprs = group_clause
            .iter()
            .map(|n| lower_group_expr(n, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((exprs, None));
    }

    // Each top-level clause item contributes a FACTOR — a list of alternative
    // grouping sets — and the clause's sets are the CARTESIAN PRODUCT of the
    // factors, each combination concatenated. Measured on PostgreSQL 18.2
    // rather than recalled: `GROUP BY a, ROLLUP (b)` returns the 6 rows of
    // `(a,b)` plus `(a)`, and `GROUP BY GROUPING SETS ((a),()), GROUPING SETS
    // ((b),())` returns the same 10 rows as `CUBE (a,b)` — i.e. all four of
    // `(a,b)`, `(a)`, `(b)`, `()`. A plain expression is just a factor with
    // one alternative, which is why mixing the two forms needs no special
    // case here.
    let mut exprs: Vec<Expr> = Vec::new();
    let mut sets: Vec<GroupingSetIndices> = vec![Vec::new()];
    for item in group_clause {
        let factor = expand_grouping_element(item, &mut exprs, ctx)?;
        if sets.len().saturating_mul(factor.len()) > MAX_GROUPING_SETS {
            return Err(LowerError::Unsupported(format!(
                "GROUP BY expands to more than {MAX_GROUPING_SETS} grouping sets"
            )));
        }
        let mut next = Vec::with_capacity(sets.len() * factor.len());
        for base in &sets {
            for alt in &factor {
                let mut combined = base.clone();
                merge_indices(&mut combined, alt);
                next.push(combined);
            }
        }
        sets = next;
    }

    // Unreachable through the grammar — PostgreSQL rejects a `GROUPING SETS
    // ()` with no sets at all as a syntax error, so `pg_query` never hands
    // one over. Checked anyway so that `build_grouping_sets_union` can rely
    // on having at least one branch to build instead of panicking on an
    // empty fold.
    if sets.is_empty() {
        return Err(LowerError::Malformed(
            "GROUP BY expanded to no grouping sets at all",
        ));
    }

    Ok((exprs, Some(sets)))
}

/// Append `extra`'s positions to `set`, skipping any already present.
///
/// Grouping by the same expression twice within ONE set is grouping by it
/// once (`GROUP BY a, ROLLUP (a)` has a set that mentions `a` from both
/// items). This deliberately does NOT deduplicate whole SETS against each
/// other: PostgreSQL emits `GROUPING SETS ((a),(a))` twice — measured, 4 rows
/// over the 2-group fixture — and the union built below reproduces that only
/// because duplicates survive to here.
fn merge_indices(set: &mut GroupingSetIndices, extra: &[u16]) {
    for i in extra {
        if !set.contains(i) {
            set.push(*i);
        }
    }
}

/// Lower one ordinary (non-grouping-set) `GROUP BY` expression.
fn lower_group_expr(node: &Node, ctx: &LowerCtx) -> Result<Expr, LowerError> {
    let e = lower_expr(node, ctx)?;
    if contains_window(&e) {
        return Err(LowerError::Unsupported(
            "window functions are not allowed in GROUP BY".into(),
        ));
    }
    Ok(e)
}

/// Intern a grouping expression into `exprs`, returning its position. Equal
/// expressions share a position, so `CUBE (a, a)` grows one output column and
/// not two.
fn intern_group_expr(
    node: &Node,
    exprs: &mut Vec<Expr>,
    ctx: &LowerCtx,
) -> Result<u16, LowerError> {
    let e = lower_group_expr(node, ctx)?;
    if let Some(pos) = exprs.iter().position(|x| *x == e) {
        return Ok(pos as u16);
    }
    if exprs.len() >= u16::MAX as usize {
        return Err(LowerError::Unsupported(
            "too many GROUP BY expressions".into(),
        ));
    }
    exprs.push(e);
    Ok((exprs.len() - 1) as u16)
}

/// Expand one `GROUP BY` element into the alternative grouping sets it stands
/// for, interning its expressions into `exprs`.
fn expand_grouping_element(
    node: &Node,
    exprs: &mut Vec<Expr>,
    ctx: &LowerCtx,
) -> Result<Vec<GroupingSetIndices>, LowerError> {
    let Some(NodeEnum::GroupingSet(gs)) = node.node.as_ref() else {
        // An ordinary expression, or a parenthesised group — either way, one
        // set. See [`intern_term`] for why the group is not an `Expr::RowLit`.
        return Ok(vec![intern_term(node, exprs, ctx)?]);
    };
    match GroupingSetKind::try_from(gs.kind).unwrap_or(GroupingSetKind::Undefined) {
        // `()` — one set, holding nothing. Over an empty table this still
        // emits a row (measured: `GROUP BY GROUPING SETS (())` on an empty
        // table returns one row, `count` 0 and `sum` NULL), which the union
        // below gets for free from an aggregate with no group keys.
        GroupingSetKind::GroupingSetEmpty => Ok(vec![Vec::new()]),
        // A parenthesised group — `((a, b))` inside `GROUPING SETS` — is ONE
        // set holding all of its columns.
        GroupingSetKind::GroupingSetSimple => {
            let mut one = Vec::new();
            merge_indices(&mut one, &grouping_term(&gs.content, exprs, ctx)?);
            Ok(vec![one])
        }
        // `ROLLUP (a, b)` -> `(a,b)`, `(a)`, `()`: every prefix of the term
        // list, longest first.
        GroupingSetKind::GroupingSetRollup => {
            let terms = grouping_terms(&gs.content, exprs, ctx)?;
            if terms.len() > MAX_CUBE_ELEMENTS {
                return Err(LowerError::Unsupported(format!(
                    "ROLLUP is limited to {MAX_CUBE_ELEMENTS} elements"
                )));
            }
            Ok((0..=terms.len())
                .rev()
                .map(|take| {
                    let mut set = Vec::new();
                    for t in &terms[..take] {
                        merge_indices(&mut set, t);
                    }
                    set
                })
                .collect())
        }
        // `CUBE (a, b)` -> all four subsets.
        GroupingSetKind::GroupingSetCube => {
            let terms = grouping_terms(&gs.content, exprs, ctx)?;
            if terms.len() > MAX_CUBE_ELEMENTS {
                return Err(LowerError::Unsupported(format!(
                    "CUBE is limited to {MAX_CUBE_ELEMENTS} elements"
                )));
            }
            // Descending mask order so the all-columns set comes first and the
            // grand total last, matching `ROLLUP`'s longest-first shape. Row
            // order is not guaranteed by PostgreSQL without an `ORDER BY`
            // (measured: the grand total came back FIRST under a
            // `MixedAggregate`), so this is for plan readability, not
            // compatibility.
            Ok((0..(1usize << terms.len()))
                .rev()
                .map(|mask| {
                    let mut set = Vec::new();
                    for (bit, t) in terms.iter().enumerate() {
                        if mask & (1 << bit) != 0 {
                            merge_indices(&mut set, t);
                        }
                    }
                    set
                })
                .collect())
        }
        // `GROUPING SETS (...)` — each element expands on its own terms and
        // the results concatenate, so a nested `ROLLUP` inside is handled by
        // the same recursion.
        GroupingSetKind::GroupingSetSets => {
            let mut out = Vec::new();
            for c in &gs.content {
                out.extend(expand_grouping_element(c, exprs, ctx)?);
            }
            Ok(out)
        }
        GroupingSetKind::Undefined => {
            Err(LowerError::Malformed("GROUP BY grouping set with no kind"))
        }
    }
}

/// The term list of a `ROLLUP`/`CUBE`: each element is either a plain
/// expression or a parenthesised group that counts as ONE element
/// (`ROLLUP ((a, b), c)` has two elements, not three).
fn grouping_terms(
    content: &[Node],
    exprs: &mut Vec<Expr>,
    ctx: &LowerCtx,
) -> Result<Vec<GroupingSetIndices>, LowerError> {
    content
        .iter()
        .map(|c| match c.node.as_ref() {
            // `ROLLUP (a, GROUPING SETS (b))` and friends. PostgreSQL's own
            // grammar rejects `ROLLUP (a, ROLLUP (b))` outright (it parses the
            // inner one as a function call and fails to resolve it), so this
            // is reachable only for the forms it does accept and has no
            // expansion here yet.
            Some(NodeEnum::GroupingSet(_)) => Err(LowerError::Unsupported(
                "a nested ROLLUP / CUBE / GROUPING SETS inside ROLLUP or CUBE is not yet lowered"
                    .into(),
            )),
            _ => intern_term(c, exprs, ctx),
        })
        .collect()
}

/// Intern one *term* — one element of a `ROLLUP`/`CUBE` list, or one member of
/// a `GROUPING SETS` list — into the positions it covers.
///
/// The subtlety is the parenthesised group. `ROLLUP ((a, b), c)` has TWO
/// elements, not three (measured: `ROLLUP ((a,b))` returns 5 rows — the sets
/// `(a,b)` and `()` — rather than the 7 that `ROLLUP (a,b)` gives). The raw
/// parse tree does NOT mark `(a, b)` as a `GroupingSetSimple`; that node only
/// appears after PostgreSQL's own parse analysis. `pg_query` hands over a
/// **`RowExpr`**, which `lower::expr` would happily lower to an
/// `Expr::RowLit` — a single opaque row value. Grouping by that row value is
/// not grouping by its columns: the SELECT list's `a` would then match no
/// grouping expression and be rejected with "column \"a\" must appear in the
/// GROUP BY clause", which is exactly what happened before this flattened it.
fn intern_term(
    node: &Node,
    exprs: &mut Vec<Expr>,
    ctx: &LowerCtx,
) -> Result<GroupingSetIndices, LowerError> {
    match node.node.as_ref() {
        Some(NodeEnum::RowExpr(row)) => grouping_term(&row.args, exprs, ctx),
        Some(NodeEnum::GroupingSet(gs))
            if GroupingSetKind::try_from(gs.kind).unwrap_or(GroupingSetKind::Undefined)
                == GroupingSetKind::GroupingSetSimple =>
        {
            grouping_term(&gs.content, exprs, ctx)
        }
        _ => Ok(vec![intern_group_expr(node, exprs, ctx)?]),
    }
}

/// Flatten one parenthesised group's members into the positions it holds.
fn grouping_term(
    content: &[Node],
    exprs: &mut Vec<Expr>,
    ctx: &LowerCtx,
) -> Result<GroupingSetIndices, LowerError> {
    let mut out = Vec::new();
    for c in content {
        let term = intern_term(c, exprs, ctx)?;
        merge_indices(&mut out, &term);
    }
    Ok(out)
}

/// Build the aggregate for a query whose `GROUP BY` used `ROLLUP` / `CUBE` /
/// `GROUPING SETS`, as a `UNION ALL` of ordinary aggregates.
///
/// # Why a union rather than the `grouping_sets` field
///
/// [`LogicalPlan::Aggregate`] has a `grouping_sets` field, and filling it in
/// is the obvious lowering. It is also a worse one *today*: `basin-exec`
/// refuses a non-`None` `grouping_sets` outright (`build.rs`), so filling it
/// in would convert an honest lowering-time `Unsupported` into a runtime
/// `BuildError` — the engine trying and failing — with no query gaining an
/// answer. `basin-exec` already builds `LogicalPlan::SetOp` and
/// `LogicalPlan::Project`, so the union shape RUNS.
///
/// # The shape
///
/// One branch per grouping set. Each branch aggregates over the same input by
/// that set's subset of the grouping keys, then projects back out to the full
/// `group.len() + aggs.len()` width, substituting a typed `NULL` for every
/// grouping column the set left out:
///
/// ```text
/// SetOp UNION ALL
///   Project [c0, NULL::text, c1]        <- set (a): pads b
///     Aggregate group=[a] aggs=[count]
///   Project [NULL::int, NULL::text, c0] <- set (): pads both
///     Aggregate group=[] aggs=[count]
/// ```
///
/// Every branch therefore has the identical width and column meaning the
/// single `Aggregate` would have had, which is precisely why `HAVING`, the
/// window rewrite, `ORDER BY` and the final projection above this need no
/// changes at all.
///
/// # Why the padded NULL is correct rather than a shortcut
///
/// The grand-total row genuinely has NULL in every grouped column, and that
/// NULL is genuinely indistinguishable from a real NULL in the data —
/// measured on 18.2, `ROLLUP (a,b)` over a row with a real NULL `b` returns
/// both `y|NULL|8` (real) and `y|NULL|12` (padding). Being unable to tell
/// them apart is the reason `GROUPING()` exists, not a defect of this
/// lowering.
///
/// # Why aggregates stay correct
///
/// Each branch is an INDEPENDENT aggregate over the WHOLE input, so a
/// rolled-up group's aggregate sees every row in that group rather than a sum
/// of sub-aggregates. The cost is that the input is scanned once per set.
///
/// # Why `HAVING` above this is safe
///
/// `opt::pushdown` treats `LogicalPlan::SetOp` as an opaque barrier, so a
/// `HAVING` filter sitting above the union is never pushed into a branch.
/// That is load-bearing: pushed through a branch's padding `Project` a
/// grouping-key predicate would fold against a constant NULL, and pushed
/// below that branch's `Aggregate` it would delete the single row an empty
/// grouping set must still emit over an empty input.
fn build_grouping_sets_union(
    input: LogicalPlan,
    group: Vec<Expr>,
    aggs: Vec<Expr>,
    sets: &[GroupingSetIndices],
    ctx: &LowerCtx,
) -> LogicalPlan {
    debug_assert!(!sets.is_empty(), "a GROUP BY always has at least one set");

    let null_for =
        |i: usize| -> Expr { Expr::Literal(Datum::Null, best_effort_type(&group[i], ctx.columns)) };

    let branch = |set: &GroupingSetIndices| -> LogicalPlan {
        let branch_group: Vec<Expr> = set.iter().map(|i| group[*i as usize].clone()).collect();
        // Column `j` of the full grouping list is at `position(j)` in this
        // branch's aggregate output; anything not in the set is padded.
        let mut exprs: Vec<(Expr, String)> = (0..group.len())
            .map(|j| {
                let name = default_alias(&group[j]);
                let e = match set.iter().position(|i| *i as usize == j) {
                    Some(pos) => Expr::Column(ColumnRef {
                        relation: 0,
                        index: pos as u16,
                        name: name.clone(),
                    }),
                    None => null_for(j),
                };
                (e, name)
            })
            .collect();
        for k in 0..aggs.len() {
            exprs.push((
                Expr::Column(ColumnRef {
                    relation: 0,
                    index: (set.len() + k) as u16,
                    name: "?column?".to_string(),
                }),
                "?column?".to_string(),
            ));
        }
        LogicalPlan::Project {
            input: Box::new(LogicalPlan::Aggregate {
                input: Box::new(input.clone()),
                group: branch_group,
                aggs: aggs.clone(),
                grouping_sets: None,
            }),
            exprs,
        }
    };

    let mut iter = sets.iter();
    let first = branch(iter.next().expect("at least one grouping set"));
    iter.fold(first, |acc, set| LogicalPlan::SetOp {
        left: Box::new(acc),
        right: Box::new(branch(set)),
        op: SetOpKind::Union,
        all: true,
    })
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
        // Named from the RAW parse node, not from `expr` — that is where
        // Postgres computes it (`FigureColname` in `transformTargetEntry`)
        // and the only place a function's *name* still exists: lowering has
        // already replaced `count` with an OID. See `lower::colname`.
        let alias = if !rt.name.is_empty() {
            rt.name.clone()
        } else {
            figure_colname_or_unnamed(val)
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

/// A label for an **internal, non-user-visible** column: the materialized
/// slots this module inserts for a `DISTINCT ON` key, a sort key, or an
/// aggregate's argument, none of which a client ever sees in
/// `RowDescription` (a final `Project` carrying the real target-list names
/// always sits above them).
///
/// This is deliberately NOT Postgres's target-list naming rule — that is
/// [`crate::lower::colname::figure_colname`], which runs on the raw parse
/// tree because a function's name does not survive lowering. Using it here
/// would be wrong in the other direction: these slots are addressed by
/// position, and giving two different aggregate arguments the same borrowed
/// name would make the plan harder to read for no gain.
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
/// [`lower_values`]'s own schema does — see [`Scope::flat_entry`]'s docs for
/// why this doesn't reuse `crate::schema`'s inference instead.
///
/// The bare-column case is no longer spelled out here: `best_effort_type`
/// asks the [`ColumnResolver`] for a column's type, and a [`ScopeResolver`]
/// over this same `scope` answers with exactly the lookup this function used
/// to do inline. `outer` is `None` on purpose — an outer reference's `index`
/// is not in *this* scope's space, and the resolver's own name cross-check
/// (see [`ScopeResolver::column_type`]) turns that into `UNKNOWN` instead of
/// the wrong column's type, which the previous inline lookup did not.
fn select_output_schema(target: &[(Expr, String)], scope: &Scope) -> Schema {
    let resolver = ScopeResolver::new(scope, None);
    target
        .iter()
        .map(|(e, alias)| (alias.clone(), best_effort_type(e, &resolver)))
        .collect()
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

    // `flat_columns`, not `star_columns`: this projection exists to keep every
    // input column at the index it already had while appending computed ones
    // after it. `*` semantics would hoist a `USING` merged column to the front
    // and drop the columns it subsumed, renumbering everything the caller
    // already resolved. See [`Scope::flat_columns`].
    let identity = scope.flat_columns().into_iter().map(|(name, index)| {
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
                // Oid 2331 is `unnest(anyarray)`, the same single oid every
                // element type shares — which is exactly why
                // `builtin_srf_column_type` resolves a FROM-position SRF's
                // result type by name and argument types rather than by this
                // oid. Deliberately arg-blind here, like every other arm, so
                // a test can drive the `unnest(a, b)` refusal past this
                // resolver and into the lowering rule that owns it.
                Some("unnest") => Some((crate::FuncId(Oid(2331)), FuncKind::SetReturning)),
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

    /// A relation sharing no column name with `t` or `u` — what a `NATURAL
    /// JOIN` with nothing in common needs, so that case can be tested as the
    /// `CROSS JOIN` Postgres makes it rather than assumed.
    fn w_schema() -> Vec<(&'static str, PgType)> {
        vec![("x", PgType::INT4), ("y", PgType::TEXT)]
    }

    fn tables() -> MockTables {
        MockTables::default()
            .with("t", 100, &t_schema())
            .with("u", 200, &u_schema())
            .with("w", 300, &w_schema())
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

    /// The output names a client actually reads back in `RowDescription`.
    ///
    /// Before this was wired to [`crate::lower::colname`], every one of these
    /// came out `?column?`, because the alias was derived from the *lowered*
    /// `Expr` — by which point `count` is only an oid. The names below are
    /// PostgreSQL 18.2's, read off the server; note in particular that the
    /// window functions are named plainly, with no frame or ORDER BY
    /// decoration (the incumbent's `row_number() ORDER BY [...]` labels are a
    /// DataFusion-ism PostgreSQL never emits, so they are not the target).
    #[test]
    fn a_target_list_entry_is_named_the_way_postgres_names_it() {
        for (sql, expected) in [
            ("SELECT count(*) FROM t", "count"),
            ("SELECT count(a) FROM t", "count"),
            ("SELECT sum(a) FROM t", "sum"),
            ("SELECT upper(b) FROM t", "upper"),
            ("SELECT rank() OVER (ORDER BY a) FROM t", "rank"),
            ("SELECT sum(a) OVER (PARTITION BY b) FROM t", "sum"),
            ("SELECT coalesce(a, 1) FROM t", "coalesce"),
            ("SELECT CASE WHEN a > 0 THEN 1 ELSE 2 END FROM t", "case"),
            ("SELECT a::text FROM t", "a"),
            ("SELECT (a + 1)::text FROM t", "text"),
            ("SELECT a FROM t", "a"),
            ("SELECT count(*) AS n FROM t", "n"),
        ] {
            let plan = lower(sql).unwrap();
            let LogicalPlan::Project { exprs, .. } = plan else {
                panic!("expected Project for {sql}");
            };
            assert_eq!(
                exprs.last().expect("one target entry").1,
                expected,
                "for {sql}"
            );
        }
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

    // --- 4b: ROLLUP / CUBE / GROUPING SETS -> UNION ALL of aggregates -------
    //
    // Every expansion below was measured against a live PostgreSQL 18.2
    // server before being written down; the fixture is
    // `gs(a,b,v)` = ('x','p',1),('x','q',2),('y','p',4),('y',NULL,8) and the
    // row counts quoted in the comments are that server's.

    /// The branches of a left-deep `UNION ALL` chain, left to right. A single
    /// grouping set produces no `SetOp` at all, which this reports as one
    /// branch.
    fn union_all_branches(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
        match plan {
            LogicalPlan::SetOp {
                left,
                right,
                op: SetOpKind::Union,
                all: true,
            } => {
                let mut v = union_all_branches(left);
                v.push(right);
                v
            }
            other => vec![other],
        }
    }

    /// One branch's `(grouping keys, projected output row)`.
    fn branch_shape(b: &LogicalPlan) -> (Vec<Expr>, Vec<Expr>) {
        let LogicalPlan::Project { input, exprs } = b else {
            panic!("expected a padding Project per branch, got {b:?}");
        };
        let LogicalPlan::Aggregate {
            group,
            grouping_sets,
            ..
        } = &**input
        else {
            panic!("expected an Aggregate under each branch's Project, got {input:?}");
        };
        assert!(
            grouping_sets.is_none(),
            "each branch must be a PLAIN aggregate: basin-exec refuses a \
             grouping_sets-bearing Aggregate outright, so emitting one would \
             trade a lowering-time refusal for a runtime failure"
        );
        (
            group.clone(),
            exprs.iter().map(|(e, _)| e.clone()).collect(),
        )
    }

    /// Just the grouping-key lists, one per branch, in branch order.
    fn set_groups(plan: &LogicalPlan) -> Vec<Vec<Expr>> {
        union_all_branches(plan)
            .into_iter()
            .map(|b| branch_shape(b).0)
            .collect()
    }

    #[test]
    fn rollup_lowers_to_a_union_all_of_plain_aggregates_with_null_padding() {
        let plan = lower("SELECT a, sum(id) FROM t GROUP BY ROLLUP (a)").unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        // The SELECT list is rewritten against the FULL grouping list exactly
        // as it is for a plain `GROUP BY` — the union underneath is invisible
        // to everything above it, which is the whole point of the shape.
        assert_eq!(exprs[0].0, col(0, "a"));
        assert_eq!(exprs[1].0, col(1, "?column?"));

        let branches = union_all_branches(&input);
        assert_eq!(branches.len(), 2, "ROLLUP (a) is the two sets (a) and ()");

        let (g0, p0) = branch_shape(branches[0]);
        assert_eq!(g0, vec![col(1, "a")], "\"a\" is base-scope index 1");
        assert_eq!(p0, vec![col(0, "a"), col(1, "?column?")]);

        let (g1, p1) = branch_shape(branches[1]);
        assert!(g1.is_empty(), "the grand-total branch groups by nothing");
        assert_eq!(
            p1[0],
            Expr::Literal(Datum::Null, PgType::INT4),
            "the grand total's grouped column is a NULL of that column's own type"
        );
        assert_eq!(
            p1[1],
            col(0, "?column?"),
            "the aggregate follows the padding"
        );
    }

    #[test]
    fn rollup_of_two_columns_is_the_three_prefixes_longest_first() {
        // Measured: `ROLLUP (a,b)` returns 7 rows — the sets (a,b), (a), ().
        let plan = lower("SELECT a, b, count(*) FROM t GROUP BY ROLLUP (a, b)").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![vec![col(1, "a"), col(2, "b")], vec![col(1, "a")], vec![]]
        );
    }

    #[test]
    fn each_padded_null_carries_its_own_columns_type() {
        let plan = lower("SELECT a, b, count(*) FROM t GROUP BY ROLLUP (a, b)").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let branches = union_all_branches(&input);
        // set (a) pads only b, which is TEXT
        assert_eq!(
            branch_shape(branches[1]).1[1],
            Expr::Literal(Datum::Null, PgType::TEXT)
        );
        // the grand total pads both, each with its own type
        let (_, p) = branch_shape(branches[2]);
        assert_eq!(p[0], Expr::Literal(Datum::Null, PgType::INT4));
        assert_eq!(p[1], Expr::Literal(Datum::Null, PgType::TEXT));
    }

    #[test]
    fn cube_expands_to_every_subset() {
        // Measured: `CUBE (a,b)` returns 10 rows — all four subsets.
        let plan = lower("SELECT a, b, count(*) FROM t GROUP BY CUBE (a, b)").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![
                vec![col(1, "a"), col(2, "b")],
                vec![col(2, "b")],
                vec![col(1, "a")],
                vec![],
            ]
        );
    }

    #[test]
    fn grouping_sets_expands_to_exactly_the_sets_written() {
        // Measured: `GROUPING SETS ((a),(b),())` returns 6 rows, those three
        // sets and nothing else — no rollup of them is implied.
        let plan =
            lower("SELECT a, b, count(*) FROM t GROUP BY GROUPING SETS ((a), (b), ())").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![vec![col(1, "a")], vec![col(2, "b")], vec![]]
        );
    }

    #[test]
    fn duplicate_grouping_sets_are_not_deduplicated() {
        // Measured: `GROUPING SETS ((a),(a))` returns 4 rows over the 2-group
        // fixture — each group TWICE. `UNION ALL` (not `UNION`) is what keeps
        // that true.
        let plan = lower("SELECT a, count(*) FROM t GROUP BY GROUPING SETS ((a), (a))").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let groups = set_groups(&input);
        assert_eq!(
            groups.len(),
            2,
            "the repeated set must survive as two branches"
        );
        assert_eq!(groups[0], groups[1]);
        assert!(
            matches!(*input, LogicalPlan::SetOp { all: true, .. }),
            "UNION (not ALL) would collapse the duplicate rows PostgreSQL emits"
        );
    }

    #[test]
    fn a_parenthesised_group_counts_as_one_rollup_element() {
        // Measured: `ROLLUP ((a,b))` returns 5 rows — the sets (a,b) and (),
        // NOT the three that `ROLLUP (a,b)` gives.
        let plan = lower("SELECT a, b, count(*) FROM t GROUP BY ROLLUP ((a, b))").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![vec![col(1, "a"), col(2, "b")], vec![]]
        );
    }

    #[test]
    fn a_parenthesised_group_inside_grouping_sets_is_one_set_of_two_columns() {
        // Measured: `GROUPING SETS ((a,b),())` returns 5 rows — one set of
        // both columns plus the grand total. The raw parse tree calls that
        // `(a,b)` a `RowExpr`, and lowering it as one would make it an
        // `Expr::RowLit` that the SELECT list's own `a` could never match.
        let plan =
            lower("SELECT a, b, count(*) FROM t GROUP BY GROUPING SETS ((a, b), ())").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![vec![col(1, "a"), col(2, "b")], vec![]]
        );
    }

    #[test]
    fn a_plain_group_by_item_crosses_with_a_rollup() {
        // Measured: `GROUP BY a, ROLLUP (b)` returns 6 rows — the sets (a,b)
        // and (a). Clause items multiply; they do not concatenate.
        let plan = lower("SELECT a, b, count(*) FROM t GROUP BY a, ROLLUP (b)").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![vec![col(1, "a"), col(2, "b")], vec![col(1, "a")]]
        );
    }

    #[test]
    fn two_grouping_sets_clauses_cross_into_all_four_sets() {
        // Measured: this returns the same 10 rows as `CUBE (a,b)`.
        let plan = lower(
            "SELECT a, b, count(*) FROM t \
             GROUP BY GROUPING SETS ((a), ()), GROUPING SETS ((b), ())",
        )
        .unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![
                vec![col(1, "a"), col(2, "b")],
                vec![col(1, "a")],
                vec![col(2, "b")],
                vec![],
            ]
        );
    }

    #[test]
    fn a_rollup_nested_inside_grouping_sets_expands_in_place() {
        // Measured: `GROUPING SETS (ROLLUP(a,b))` returns the same 7 rows as
        // `ROLLUP (a,b)` — a nested element expands on its own terms.
        let plan =
            lower("SELECT a, b, count(*) FROM t GROUP BY GROUPING SETS (ROLLUP (a, b))").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            set_groups(&input),
            vec![vec![col(1, "a"), col(2, "b")], vec![col(1, "a")], vec![]]
        );
    }

    #[test]
    fn a_lone_empty_grouping_set_is_still_an_aggregate_query() {
        // `GROUP BY GROUPING SETS (())` names no grouping expression at all,
        // so the `!group_exprs.is_empty()` test alone would not see it as an
        // aggregate query. Measured: it returns exactly one row — and over an
        // EMPTY table it STILL returns one row, which is what an aggregate
        // with no group keys already does.
        let plan = lower("SELECT count(*) FROM t GROUP BY GROUPING SETS (())").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let branches = union_all_branches(&input);
        assert_eq!(branches.len(), 1, "one set means no SetOp is needed at all");
        let (g, p) = branch_shape(branches[0]);
        assert!(g.is_empty());
        assert_eq!(p, vec![col(0, "?column?")]);
    }

    #[test]
    fn having_filters_above_the_union_not_inside_each_branch() {
        // Load-bearing rather than cosmetic: `opt::pushdown` treats `SetOp` as
        // an opaque barrier, so a HAVING left HERE is never pushed into a
        // branch — where it would fold against a padded constant NULL and,
        // below that branch's Aggregate, could delete the single row an empty
        // grouping set must still emit.
        let plan =
            lower("SELECT a, count(*) FROM t GROUP BY ROLLUP (a) HAVING count(*) > 1").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { input, .. } = *input else {
            panic!("expected the HAVING Filter directly above the union");
        };
        assert!(
            matches!(*input, LogicalPlan::SetOp { all: true, .. }),
            "the HAVING must sit above the whole union"
        );
    }

    #[test]
    fn order_by_sorts_the_whole_union_rather_than_a_branch() {
        let plan =
            lower("SELECT a, count(*) FROM t GROUP BY ROLLUP (a) ORDER BY a NULLS LAST").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Sort { input, keys } = *input else {
            panic!("expected Sort above the union");
        };
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].expr, col(0, "a"));
        assert!(matches!(*input, LogicalPlan::SetOp { all: true, .. }));
    }

    #[test]
    fn cube_past_twelve_elements_is_refused_like_postgres_refuses_it() {
        // PostgreSQL 18.2, measured: "CUBE is limited to 12 elements".
        let cols = ["a"; 13].join(", ");
        let err = lower(&format!("SELECT count(*) FROM t GROUP BY CUBE ({cols})")).unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("12 elements"), "message should explain: {msg}");
    }

    #[test]
    fn a_window_function_is_still_rejected_inside_a_grouping_set() {
        // Expanding grouping sets must not smuggle a window function past the
        // check every other GROUP BY expression goes through. PostgreSQL 18.2
        // rejects this with exactly this sentence, measured live.
        let err = lower("SELECT count(*) FROM t GROUP BY ROLLUP (rank() OVER ())").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert_eq!(msg, "window functions are not allowed in GROUP BY");
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

    // --- 7b: JOIN ... USING / NATURAL JOIN ----------------------------------
    //
    // Every expectation below is the answer a live PostgreSQL 18.2 server
    // gives for the same shape, read back from `pg_get_viewdef` over a view
    // on the join — see [`MergedColumn`] and [`build_using_join`] for the
    // measurements themselves.

    /// `t(id, a, b)` and `u(id, t_id, c)` have exactly `id` in common, so
    /// `USING (id)` and `NATURAL JOIN` are the same join here — and `*` must
    /// give `id, a, b, t_id, c`: the merged column first, then the rest of
    /// the left, then the rest of the right, with `u.id` (flat index 3) gone.
    fn star_names(plan: &LogicalPlan) -> Vec<(String, u16)> {
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project");
        };
        exprs
            .iter()
            .map(|(e, name)| match e {
                Expr::Column(c) => (name.clone(), c.index),
                other => panic!("expected a column in the star projection, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn join_using_merges_the_column_and_puts_it_first_in_star() {
        let plan = lower("SELECT * FROM t JOIN u USING (id)").unwrap();
        assert_eq!(
            star_names(&plan),
            vec![
                ("id".to_string(), 0),
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("t_id".to_string(), 4),
                ("c".to_string(), 5),
            ]
        );
        let LogicalPlan::Project { input, .. } = plan else {
            unreachable!();
        };
        let LogicalPlan::Join {
            kind, on, filter, ..
        } = *input
        else {
            panic!("expected Join");
        };
        assert_eq!(kind, JoinKind::Inner);
        // The right side of an `on` pair is addressed in the RIGHT input's
        // own index space, so `u.id` is 0 there, not 3.
        assert_eq!(on, vec![(col(0, "id"), col(0, "id"))]);
        assert!(filter.is_none());
    }

    #[test]
    fn natural_join_is_using_over_the_common_columns() {
        let natural = lower("SELECT * FROM t NATURAL JOIN u").unwrap();
        let using = lower("SELECT * FROM t JOIN u USING (id)").unwrap();
        assert_eq!(natural, using);
    }

    #[test]
    fn natural_join_with_no_common_columns_is_a_cross_join() {
        // `w(x, y)` shares no name with `t(id, a, b)`. Postgres deparses this
        // exact shape back as `t CROSS JOIN w` and returns the full product,
        // rather than raising — so nothing is merged and `*` is the plain
        // concatenation.
        let plan = lower("SELECT * FROM t NATURAL JOIN w").unwrap();
        assert_eq!(
            star_names(&plan),
            vec![
                ("id".to_string(), 0),
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("x".to_string(), 3),
                ("y".to_string(), 4),
            ]
        );
        let LogicalPlan::Project { input, .. } = plan else {
            unreachable!();
        };
        let LogicalPlan::Join { kind, on, .. } = *input else {
            panic!("expected Join");
        };
        assert_eq!(kind, JoinKind::Cross);
        assert!(on.is_empty());
    }

    #[test]
    fn a_using_merged_column_is_unqualified_while_a_qualified_one_reaches_past_it() {
        let plan = lower("SELECT id, t.id, u.id FROM t JOIN u USING (id)").unwrap();
        assert_eq!(
            star_names(&plan),
            vec![
                ("id".to_string(), 0),
                ("id".to_string(), 0),
                ("id".to_string(), 3),
            ]
        );
    }

    #[test]
    fn right_join_using_takes_the_right_sides_column_as_the_merged_one() {
        // A RIGHT join null-extends the LEFT, so the left column is NULL on
        // an unmatched row and the right one is the answer. Postgres deparses
        // it as `uu.id` for exactly this reason.
        let plan = lower("SELECT id FROM t RIGHT JOIN u USING (id)").unwrap();
        assert_eq!(star_names(&plan), vec![("id".to_string(), 3)]);
    }

    #[test]
    fn left_join_using_takes_the_left_sides_column_as_the_merged_one() {
        let plan = lower("SELECT id FROM t LEFT JOIN u USING (id)").unwrap();
        assert_eq!(star_names(&plan), vec![("id".to_string(), 0)]);
    }

    /// The case that is silently wrong if a side is picked: on a FULL join
    /// neither input's column is the merged value, so one is materialized as
    /// `COALESCE(t.id, u.id)` and the merged column points at it.
    #[test]
    fn full_join_using_materializes_coalesce_rather_than_picking_a_side() {
        let plan = lower("SELECT id FROM t FULL JOIN u USING (id)").unwrap();
        // The merged column is the appended one at index 6, past the join's
        // own six columns — not index 0 (the left) and not index 3 (the
        // right).
        assert_eq!(star_names(&plan), vec![("id".to_string(), 6)]);

        let LogicalPlan::Project { input, .. } = plan else {
            unreachable!();
        };
        let LogicalPlan::Project { input, exprs } = *input else {
            panic!("expected the COALESCE Project under the select-list Project");
        };
        assert_eq!(exprs.len(), 7);
        // The first six re-emit the join's own columns at their own indices,
        // so nothing below is renumbered.
        for (i, (e, name)) in exprs.iter().take(6).enumerate() {
            assert_eq!(*e, col(i as u16, name));
        }
        assert_eq!(
            exprs[6],
            (
                Expr::Coalesce(vec![col(0, "id"), col(3, "id")]),
                "id".to_string()
            )
        );
        let LogicalPlan::Join { kind, on, .. } = *input else {
            panic!("expected Join under the COALESCE Project");
        };
        assert_eq!(kind, JoinKind::Full);
        assert_eq!(on, vec![(col(0, "id"), col(0, "id"))]);
    }

    #[test]
    fn using_a_column_absent_from_one_side_is_rejected_naming_that_side() {
        // Postgres checks the left side first: `USING (zzz)`, absent from
        // both, reports the LEFT table.
        let Err(LowerError::UnknownName(msg)) = lower("SELECT * FROM t JOIN u USING (zzz)") else {
            panic!("expected UnknownName");
        };
        assert!(msg.contains("does not exist in left table"), "{msg}");
        // `a` exists on `t` only.
        let Err(LowerError::UnknownName(msg)) = lower("SELECT * FROM t JOIN u USING (a)") else {
            panic!("expected UnknownName");
        };
        assert!(msg.contains("does not exist in right table"), "{msg}");
    }

    #[test]
    fn a_using_merged_column_is_still_ambiguous_against_a_third_relation() {
        // `SELECT id FROM t JOIN u USING (id)` is legal, but adding another
        // relation that also has `id` makes the name ambiguous again — the
        // merged column counts as one candidate, not zero.
        assert!(lower("SELECT id FROM t JOIN u USING (id)").is_ok());
        assert!(matches!(
            lower("SELECT id FROM t JOIN u USING (id), t AS t2"),
            Err(LowerError::UnknownName(_))
        ));
    }

    #[test]
    fn using_still_hides_the_merged_column_from_star_when_chained() {
        // `t JOIN u USING (id) JOIN t2 USING (id)` shows one `id`, not three.
        let plan = lower("SELECT * FROM t JOIN u USING (id) JOIN t AS t2 USING (id)").unwrap();
        let names: Vec<String> = star_names(&plan).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["id", "a", "b", "t_id", "c", "a", "b"]);
    }

    #[test]
    fn a_qualified_star_under_using_is_not_merged() {
        // `SELECT ut.* FROM ut JOIN uu USING (id)` gives `ut`'s own columns in
        // `ut`'s own order on a live server — `id` stays in position, neither
        // hoisted nor dropped.
        let plan = lower("SELECT t.* FROM t JOIN u USING (id)").unwrap();
        assert_eq!(
            star_names(&plan),
            vec![
                ("id".to_string(), 0),
                ("a".to_string(), 1),
                ("b".to_string(), 2),
            ]
        );
    }

    #[test]
    fn a_using_join_alias_is_refused_rather_than_ignored() {
        assert!(matches!(
            lower("SELECT * FROM t JOIN u USING (id) AS j"),
            Err(LowerError::Unsupported(_))
        ));
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

    /// `LATERAL` in `FROM` is no longer refused wholesale — a `LATERAL`
    /// *function* now lowers (see the `SRF in FROM` section below). What
    /// stays refused is a `LATERAL` *subquery*, which needs the general
    /// subquery-in-FROM lowering that does not exist yet, so this test now
    /// pins that narrower refusal rather than a blanket one — and pins the
    /// message, so a future change that makes the whole shape work has to
    /// come here and say so.
    #[test]
    fn a_lateral_subquery_in_from_is_still_unsupported() {
        let err = lower("SELECT * FROM t, LATERAL (SELECT t.a) sub").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("LATERAL"),
            "message should name the construct: {msg}"
        );
    }

    // --- Derived tables (subquery in FROM) ----------------------------------
    //
    // This shape used to be refused outright, and a test here pinned that
    // refusal. It is now lowered, so that test became a statement of the
    // opposite of the truth and was rewritten into the positive assertions
    // below rather than removed — the refusal it guarded is exactly what the
    // rest of this section now has to keep honest.

    /// The shape the old refusal test used, now lowering: the derived table's
    /// own plan is nested under the outer `SELECT`'s projection, and the alias
    /// puts its SELECT-list name in scope.
    #[test]
    fn a_subquery_in_from_lowers_to_its_own_plan_under_the_outer_projection() {
        let plan = lower("SELECT * FROM (SELECT 1 AS a) sub").unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs, vec![(col(0, "a"), "a".to_string())]);
        let LogicalPlan::Project { exprs: inner, .. } = *input else {
            panic!("expected the derived table's own Project");
        };
        assert_eq!(inner, vec![(int_lit(1), "a".to_string())]);
    }

    /// A derived table is a relation like any other: its columns resolve both
    /// bare and qualified by the alias, and the flat indices are positions in
    /// the derived plan's OUTPUT, not in the underlying table.
    #[test]
    fn a_derived_tables_columns_resolve_by_alias_at_their_output_positions() {
        // `b` is `t`'s column 2; through the derived table it is column 1,
        // because the derived SELECT list only exposes two columns.
        let plan = lower("SELECT sub.b, sub.id FROM (SELECT id, b FROM t WHERE id > 1) AS sub")
            .unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            exprs,
            vec![
                (col(1, "b"), "b".to_string()),
                (col(0, "id"), "id".to_string()),
            ]
        );
        // The derived body kept its own WHERE.
        let LogicalPlan::Project { input: body, .. } = *input else {
            panic!("expected the derived table's Project");
        };
        assert!(
            matches!(*body, LogicalPlan::Filter { .. }),
            "the derived table's own WHERE must survive, got {body:?}"
        );
    }

    /// A column the derived table does not expose is out of scope, exactly as
    /// on a live server (`column "a" does not exist`). Without this the alias
    /// would be decorative and an inner-scope leak would go unnoticed.
    #[test]
    fn a_column_the_derived_table_does_not_expose_does_not_resolve() {
        let err = lower("SELECT a FROM (SELECT id FROM t) AS sub").unwrap_err();
        assert!(
            matches!(err, LowerError::UnknownName(ref n) if n == "a"),
            "expected UnknownName(\"a\"), got {err:?}"
        );
    }

    /// `AS sub(x, y)` renames positionally and HIDES the body's own names —
    /// the same rule (and the same helper) a CTE's column-alias list uses.
    #[test]
    fn a_derived_tables_column_alias_list_renames_positionally() {
        let plan = lower("SELECT x, y FROM (SELECT id, b FROM t) AS sub(x, y)").unwrap();
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "x"), "x".to_string()),
                (col(1, "y"), "y".to_string()),
            ]
        );
        assert!(
            lower("SELECT id FROM (SELECT id, b FROM t) AS sub(x, y)").is_err(),
            "an aliased-away name must not still resolve"
        );
    }

    /// A derived table may reference an enclosing `WITH` entry — `ctes` is
    /// threaded into its body rather than dropped at the FROM-item boundary.
    #[test]
    fn a_derived_table_can_reference_an_enclosing_cte() {
        let plan =
            lower("WITH a AS (SELECT id FROM t) SELECT * FROM (SELECT id FROM a) AS s").unwrap();
        let LogicalPlan::Cte { input, .. } = plan else {
            panic!("expected Cte at the root, got {plan:?}");
        };
        let mut saw_cte_ref = false;
        fn walk(p: &LogicalPlan, saw: &mut bool) {
            if matches!(p, LogicalPlan::CteRef { .. }) {
                *saw = true;
            }
            p.for_each_input(&mut |c| walk(c, saw));
        }
        walk(&input, &mut saw_cte_ref);
        assert!(saw_cte_ref, "the derived table's body must read the CTE");
    }

    /// A `WITH` entry is scoped over the whole statement, not just its `FROM`
    /// clause. These are the positions that used to restart CTE resolution
    /// from an empty list — [`SelectSubqueries`] re-entered lowering without
    /// carrying `ctes`, so every one of them reported `UnknownName("a")`
    /// while `SELECT * FROM a` in the same statement worked. A live
    /// PostgreSQL 18.2 server accepts all four.
    #[test]
    fn a_cte_is_visible_inside_a_subquery_in_every_clause() {
        // A subquery's plan hangs off an `Expr::Subquery`, not off
        // `for_each_input` — which is exactly why this walk descends through
        // expressions as well. Walking only plan inputs would report "no
        // CteRef" for a plan that has one, which is the failure mode this
        // whole test exists to rule out.
        fn walk_expr(e: &Expr, saw: &mut bool) {
            if let Expr::Subquery { subplan, .. } = e {
                walk(subplan, saw);
            }
            e.for_each_child(&mut |c| walk_expr(c, saw));
        }
        fn walk(p: &LogicalPlan, saw: &mut bool) {
            if matches!(p, LogicalPlan::CteRef { .. }) {
                *saw = true;
            }
            p.for_each_expr(&mut |e| walk_expr(e, saw));
            p.for_each_input(&mut |c| walk(c, saw));
        }

        fn reads_the_cte(sql: &str) {
            let plan = lower(sql).unwrap_or_else(|e| panic!("{sql} failed to lower: {e:?}"));
            let mut saw = false;
            walk(&plan, &mut saw);
            assert!(saw, "{sql} lowered without ever reading the CTE");
        }

        // The reported case: a scalar subquery in the SELECT list.
        reads_the_cte("WITH a AS (SELECT id FROM t) SELECT (SELECT count(*) FROM a)");
        // The same gap in WHERE, which the select-list fix must not leave open.
        reads_the_cte("WITH a AS (SELECT id FROM t) SELECT id FROM t WHERE id IN (SELECT id FROM a)");
        reads_the_cte(
            "WITH a AS (SELECT id FROM t) SELECT id FROM t WHERE EXISTS (SELECT 1 FROM a)",
        );
        // HAVING, reached through the same per-clause `expr_ctx`.
        reads_the_cte(
            "WITH a AS (SELECT id FROM t) SELECT id FROM t GROUP BY id \
             HAVING count(*) > (SELECT count(*) FROM a)",
        );
    }

    /// The CTE list a subquery sees is the one in scope where it was written,
    /// so a name that is NOT a CTE must still fail rather than silently
    /// resolving against some other statement's list.
    #[test]
    fn a_subquery_still_rejects_a_name_that_is_not_a_cte() {
        assert!(matches!(
            lower("SELECT (SELECT count(*) FROM nosuch)"),
            Err(LowerError::UnknownName(_))
        ));
    }

    /// A derived table carrying a window function — the shape that motivates
    /// derived tables at all, since a window result cannot be filtered in the
    /// same query level that computes it (`WHERE rn = 1` is evaluated before
    /// `row_number()`; a live server rejects it with "window functions are not
    /// allowed in WHERE").
    #[test]
    fn a_derived_table_may_carry_a_window_function_the_outer_query_filters_on() {
        let plan =
            lower("SELECT id FROM (SELECT id, rank() OVER (ORDER BY id) rn FROM t) x WHERE rn = 1")
                .unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { input, .. } = *input else {
            panic!("expected the outer WHERE as a Filter, got {input:?}");
        };
        let LogicalPlan::Project { input: win, .. } = *input else {
            panic!("expected the derived table's Project");
        };
        assert!(
            matches!(*win, LogicalPlan::Window { .. }),
            "the window must be computed inside the derived table, got {win:?}"
        );
    }

    /// A derived table joined to a real table: the combined scope must put
    /// the derived side's columns AFTER the real side's, so the derived
    /// column's flat index counts the real table's full width first. Getting
    /// this wrong reads a neighbouring column and returns a plausible wrong
    /// answer rather than failing, which is why it is asserted by index here
    /// and not by rendered name.
    ///
    /// Row values verified against a live PostgreSQL 18.2 (tables shaped as
    /// this file's `t`/`u`): `SELECT t.id, s.x FROM t JOIN (SELECT t_id AS x
    /// FROM u) s ON s.x = t.id` returns `(1,1), (1,1), (2,2)`.
    #[test]
    fn a_derived_table_joined_to_a_real_table_keeps_the_scope_offsets_straight() {
        let plan =
            lower("SELECT t.id, s.x FROM t JOIN (SELECT t_id AS x FROM u) s ON s.x = t.id")
                .unwrap();
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project");
        };
        // `t` is 3 columns wide, so the derived table's single column `x` is
        // flat index 3 — not 0, and not 1.
        assert_eq!(
            exprs,
            vec![
                (col(0, "id"), "id".to_string()),
                (col(3, "x"), "x".to_string()),
            ]
        );
        let LogicalPlan::Join { on, .. } = *input else {
            panic!("expected a Join, got {input:?}");
        };
        // Inside a Join's `on`, each side is numbered against its OWN
        // relation, not against the combined scope (the tuple position is
        // what says which side it is) — the same convention
        // `split_equijoin_conjuncts` already produces for two real tables.
        // So the derived table's `x` is index 0 here, its first and only
        // output column, even though it is flat index 3 above.
        assert_eq!(on, vec![(col(0, "id"), col(0, "x"))]);
    }

    /// `SELECT *` across a real table and a derived table expands to the real
    /// table's columns followed by the derived table's, at their combined flat
    /// indices. Verified live: the same query returns columns `id, a, b, x`
    /// in that order.
    #[test]
    fn star_expands_across_a_real_table_then_a_derived_one() {
        let plan =
            lower("SELECT * FROM t JOIN (SELECT t_id AS x FROM u) s ON s.x = t.id").unwrap();
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(
            exprs,
            vec![
                (col(0, "id"), "id".to_string()),
                (col(1, "a"), "a".to_string()),
                (col(2, "b"), "b".to_string()),
                (col(3, "x"), "x".to_string()),
            ]
        );
    }

    /// A non-`LATERAL` derived table must NOT see its SIBLING `FROM` items —
    /// that is exactly what `LATERAL` exists to permit, and it is still
    /// refused (see `a_lateral_subquery_in_from_is_still_unsupported`). This
    /// falls out of `build_range_subselect` being handed only the enclosing
    /// query LEVEL's resolver and never the `left` scope
    /// [`build_range_function`] receives, but it falls out silently, so it is
    /// pinned here: if it ever regressed, the derived table would resolve a
    /// sibling column against its own indices and read the wrong column.
    ///
    /// Matches a live PostgreSQL 18.2 exactly, which rejects this with
    /// `invalid reference to FROM-clause entry for table "t"` / "There is an
    /// entry for table "t", but it cannot be referenced from this part of the
    /// query."
    #[test]
    fn a_non_lateral_derived_table_cannot_see_a_sibling_from_item() {
        let err = lower("SELECT * FROM t, (SELECT t.id) s").unwrap_err();
        assert!(
            matches!(err, LowerError::UnknownName(ref n) if n == "t.id"),
            "expected the sibling reference not to resolve, got {err:?}"
        );
    }

    /// A `VALUES` list in `FROM` still takes the short path, and one carrying
    /// its own `ORDER BY` is still refused by name rather than silently
    /// dropping it — `lower_values` has no wiring for that clause, so lowering
    /// it would produce an unordered answer to an ordered question.
    #[test]
    fn a_values_list_in_from_with_its_own_order_by_is_refused_by_name() {
        let err = lower("SELECT * FROM (VALUES (2),(1) ORDER BY 1) AS v").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("ORDER BY"),
            "message should name the dropped clause: {msg}"
        );
    }

    /// ...but a `VALUES` list with `LIMIT`/`OFFSET` and no `ORDER BY` still
    /// lowers, because `lower_values` genuinely applies them. This is the
    /// other half of the refusal above: the guard has to be narrow enough not
    /// to give up reach this path already had. Verified live on PostgreSQL
    /// 18.2: `SELECT * FROM (VALUES (1),(2),(3) LIMIT 2) AS v` returns 1, 2.
    #[test]
    fn a_values_list_in_from_with_only_a_limit_still_lowers() {
        let plan = lower("SELECT * FROM (VALUES (1),(2),(3) LIMIT 2) AS v").unwrap();
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        assert!(
            matches!(*input, LogicalPlan::Limit { .. }),
            "the VALUES list's own LIMIT must survive, got {input:?}"
        );
    }

    // --- Set-returning functions in FROM ------------------------------------
    //
    // Every row value asserted here was read off a live PostgreSQL 18.2
    // server (`postgres://…:5432/postgres`), not from memory. The naming
    // rule these tests pin is the one commit `2f62a334` worked out for the
    // DataFusion path, implemented natively here:
    //
    //   SELECT * FROM generate_series(1,3);          -- column "generate_series"
    //   SELECT * FROM generate_series(1,3) g;        -- column "g"
    //   SELECT * FROM generate_series(1,3) AS g(i);  -- column "i"
    //   SELECT * FROM generate_series(1,3) AS g(i,j);
    //     ERROR:  table "g" has 1 columns available but 2 columns specified
    //   SELECT * FROM unnest(ARRAY[10,20]) u;        -- column "u", rows 10, 20
    //
    // and with `lt(id)` = 1, 3, 0:
    //
    //   SELECT lt.id, g FROM lt, LATERAL generate_series(1, lt.id) g;
    //     -> (1,1) (3,1) (3,2) (3,3)          -- 4 rows; id=0 drops out
    //   SELECT lt.id, g FROM lt, generate_series(1, lt.id) g;
    //     -> identical 4 rows                 -- no LATERAL keyword needed
    //   SELECT lt.id, g FROM lt LEFT JOIN LATERAL generate_series(1, lt.id) g ON true;
    //     -> the same 4 plus (0, NULL)        -- 5 rows

    /// The base shape: one row of nothing, expanded. No new IR node — a
    /// `ProjectSet` over a one-row `Empty` is what "a set-returning function
    /// IS the relation" already means, and `basin-exec`'s builder already
    /// runs exactly this plan (`a_set_returning_function_builds_and_expands`).
    #[test]
    fn a_set_returning_function_in_from_lowers_to_a_project_set() {
        let plan = lower("SELECT * FROM generate_series(1, 3)").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        // Rule 3, no alias: the FUNCTION NAME names the column.
        assert_eq!(
            exprs,
            vec![(col(0, "generate_series"), "generate_series".to_string())]
        );
        let LogicalPlan::ProjectSet { input, srfs } = *input else {
            panic!("expected ProjectSet under Project, got {input:?}");
        };
        let [Expr::SetReturning { args, .. }] = srfs.as_slice() else {
            panic!("expected exactly one SetReturning, got {srfs:?}");
        };
        assert_eq!(*args, vec![int_lit(1), int_lit(3)]);
        assert!(
            matches!(
                *input,
                LogicalPlan::Empty {
                    produce_one_row: true,
                    ..
                }
            ),
            "the SRF must expand ONE row, not zero: {input:?}"
        );
    }

    /// Rule 3 with an alias: the table alias names the single column, and is
    /// also the qualifier — `SELECT g.g FROM generate_series(1,3) g` resolves
    /// on a live server, as does the unaliased
    /// `SELECT generate_series.generate_series FROM generate_series(1,3)`.
    #[test]
    fn a_table_alias_names_the_srf_column_and_qualifies_it() {
        let plan = lower("SELECT g.g FROM generate_series(1, 3) g").expect("lowers");
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs, vec![(col(0, "g"), "g".to_string())]);

        let plan = lower("SELECT generate_series.generate_series FROM generate_series(1, 3)")
            .expect("the function name is the qualifier when there is no alias");
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project");
        };
        assert_eq!(exprs[0].1, "generate_series");
    }

    /// Rule 1: a column-alias list beats the table alias.
    #[test]
    fn a_column_alias_list_wins_over_the_table_alias() {
        let plan = lower("SELECT i FROM generate_series(1, 3) AS g(i)").expect("lowers");
        let LogicalPlan::Project { exprs, .. } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs, vec![(col(0, "i"), "i".to_string())]);
        // …and once renamed, the old name is gone, same as for `WITH x(a)`.
        assert!(matches!(
            lower("SELECT g FROM generate_series(1, 3) AS g(i)"),
            Err(LowerError::UnknownName(_))
        ));
    }

    /// An over-long column-alias list is an error, with the same wording a
    /// live server uses for `FROM (VALUES ...) AS v(...)`:
    /// `table "g" has 1 columns available but 2 columns specified`.
    #[test]
    fn an_over_long_column_alias_list_on_an_srf_is_rejected() {
        let err = lower("SELECT * FROM generate_series(1, 3) AS g(i, j)").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("table \"g\" has 1 columns available but 2 columns specified"),
            "message should match the server's: {msg}"
        );
    }

    /// The three-argument (explicit step) form and the `int8` overload both
    /// lower, and the column's TYPE follows the overload — `int4` for the
    /// plain integer forms, `int8` for the bigint one. The type is what a
    /// `FROM`-position SRF contributes to an enclosing CTE's schema, so
    /// getting it from the argument types rather than from the (shared) oid
    /// is what makes `WITH x AS (SELECT * FROM generate_series(...))` report
    /// a real type instead of `unknown`.
    #[test]
    fn the_step_and_bigint_generate_series_overloads_lower_with_their_own_types() {
        let plan = lower("WITH x AS (SELECT * FROM generate_series(1, 10, 3)) SELECT * FROM x")
            .expect("three-argument generate_series lowers");
        assert_eq!(
            cte_ref_schema(&plan),
            vec![("generate_series".to_string(), PgType::INT4)]
        );

        let plan =
            lower("WITH x AS (SELECT * FROM generate_series(1::int8, 3::int8)) SELECT * FROM x")
                .expect("the bigint overload lowers");
        assert_eq!(
            cte_ref_schema(&plan),
            vec![("generate_series".to_string(), PgType::INT8)]
        );
    }

    /// `unnest` in `FROM` goes through the identical path — it is a
    /// set-returning function with no `proargnames`, so it is alias-named
    /// too.
    #[test]
    fn unnest_in_from_lowers_and_is_alias_named() {
        let plan = lower("SELECT * FROM unnest(ARRAY[10, 20]) u").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs, vec![(col(0, "u"), "u".to_string())]);
        assert!(
            matches!(*input, LogicalPlan::ProjectSet { .. }),
            "expected ProjectSet, got {input:?}"
        );
    }

    /// An UNCORRELATED function item is a plain cross join, not a lateral
    /// one: `opt::projection` and `opt::pushdown` both treat
    /// `LogicalPlan::LateralJoin` as an opaque barrier, so using one where a
    /// `Join` would do costs every optimization below it for nothing.
    #[test]
    fn an_uncorrelated_srf_in_from_is_a_cross_join_not_a_lateral_one() {
        let plan = lower("SELECT * FROM t, generate_series(1, 3) g").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs.len(), 4, "t's three columns plus g");
        assert_eq!(exprs[3].1, "g");
        let LogicalPlan::Join { kind, right, .. } = *input else {
            panic!("expected a plain Join, got {input:?}");
        };
        assert_eq!(kind, JoinKind::Cross);
        assert!(matches!(*right, LogicalPlan::ProjectSet { .. }));
    }

    /// The valuable case. `FROM t, LATERAL generate_series(1, t.id) g`
    /// expands per outer row, so the argument must lower to an `OUTER_REF`
    /// column — the exact marker `basin-exec`'s `bind_outer` substitutes per
    /// row when it rebuilds a `LateralJoin`'s inner side, the same mechanism
    /// a correlated scalar subquery already uses.
    #[test]
    fn a_correlated_lateral_srf_lowers_to_a_lateral_join() {
        let plan = lower("SELECT * FROM t, LATERAL generate_series(1, t.id) g").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert_eq!(exprs.len(), 4, "t's three columns plus g");
        assert_eq!(exprs[3], (col(3, "g"), "g".to_string()));

        let LogicalPlan::LateralJoin { outer, inner, kind } = *input else {
            panic!("a correlated function item must be a LateralJoin, got {input:?}");
        };
        assert_eq!(kind, JoinKind::Inner);
        assert!(matches!(*outer, LogicalPlan::Scan { .. }));
        let LogicalPlan::ProjectSet { srfs, .. } = *inner else {
            panic!("expected ProjectSet as the lateral inner side, got {inner:?}");
        };
        let [Expr::SetReturning { args, .. }] = srfs.as_slice() else {
            panic!("expected one SetReturning, got {srfs:?}");
        };
        assert_eq!(
            args[1],
            Expr::Column(ColumnRef {
                relation: OUTER_REF,
                index: 0,
                name: "id".into(),
            }),
            "t.id must be tagged OUTER_REF and indexed against the OUTER row"
        );
    }

    /// The correlation must survive the OPTIMIZER, not just lowering. A
    /// `LateralJoin`'s inner side reads its outer row by POSITION, so a rule
    /// that pruned or renumbered the outer scan's projection underneath it
    /// would leave the `OUTER_REF` pointing at a different column — a wrong
    /// answer, not an error. `opt::projection` leaves `LateralJoin` alone
    /// entirely and `opt::pushdown` treats it as a barrier, both by design;
    /// this pins that the assembled default pipeline actually behaves that
    /// way for a plan this lowering now produces.
    #[test]
    fn optimization_leaves_a_lateral_srfs_correlation_intact() {
        let plan = lower("SELECT * FROM t, LATERAL generate_series(1, t.id) g").unwrap();
        let (optimized, _passes) = crate::opt::optimize_default(plan);
        let LogicalPlan::Project { input, .. } = optimized else {
            panic!("expected Project, got {optimized:?}");
        };
        let LogicalPlan::LateralJoin { outer, inner, .. } = *input else {
            panic!("the LateralJoin must survive optimization, got {input:?}");
        };
        let LogicalPlan::Scan { projection, .. } = *outer else {
            panic!("expected the outer Scan to survive, got {outer:?}");
        };
        assert_eq!(
            projection,
            vec![ColId(0), ColId(1), ColId(2)],
            "pruning t's projection under a LateralJoin would move the column \
             the correlation reads"
        );
        let LogicalPlan::ProjectSet { srfs, .. } = *inner else {
            panic!("expected ProjectSet, got {inner:?}");
        };
        let [Expr::SetReturning { args, .. }] = srfs.as_slice() else {
            panic!("expected one SetReturning, got {srfs:?}");
        };
        assert_eq!(
            args[1],
            Expr::Column(ColumnRef {
                relation: OUTER_REF,
                index: 0,
                name: "id".into(),
            })
        );
    }

    /// The `LATERAL` keyword is optional for a function item — Postgres
    /// treats one as implicitly lateral, and both spellings return the same
    /// four rows live. So the choice of plan node follows the actual
    /// correlation, not the keyword.
    #[test]
    fn a_function_item_is_implicitly_lateral_without_the_keyword() {
        let with_keyword = lower("SELECT * FROM t, LATERAL generate_series(1, t.id) g").unwrap();
        let without = lower("SELECT * FROM t, generate_series(1, t.id) g").unwrap();
        assert_eq!(with_keyword, without);
    }

    /// `CROSS JOIN LATERAL` and `LEFT JOIN LATERAL ... ON true` — the two
    /// explicit-join spellings. The `LEFT` one is the whole reason to write
    /// an outer lateral join: live, it keeps `id = 0` (whose
    /// `generate_series(1, 0)` is empty) with a NULL, where the inner form
    /// drops that row entirely.
    #[test]
    fn explicit_join_lateral_lowers_for_cross_and_left() {
        let plan = lower("SELECT * FROM t CROSS JOIN LATERAL generate_series(1, t.id) g")
            .expect("CROSS JOIN LATERAL lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::LateralJoin { kind, .. } = *input else {
            panic!("expected LateralJoin, got {input:?}");
        };
        assert_eq!(kind, JoinKind::Inner);

        let plan = lower("SELECT * FROM t LEFT JOIN LATERAL generate_series(1, t.id) g ON true")
            .expect("LEFT JOIN LATERAL ... ON true lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::LateralJoin { kind, .. } = *input else {
            panic!("expected LateralJoin, got {input:?}");
        };
        assert_eq!(
            kind,
            JoinKind::Left,
            "an outer lateral join must NOT degrade to an inner one — that silently drops rows"
        );
    }

    /// `LogicalPlan::LateralJoin` carries no `on`/`filter`, so a real `ON`
    /// condition has nowhere to go. Refused rather than approximated with a
    /// `Filter` above, which would be wrong for an outer join.
    #[test]
    fn a_join_lateral_with_a_real_on_condition_is_unsupported() {
        let err = lower("SELECT * FROM t LEFT JOIN LATERAL generate_series(1, t.id) g ON g > 1")
            .unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("ON condition"), "unexpected message: {msg}");
    }

    /// A `WHERE` clause over a lateral join becomes a plain `Filter` above
    /// it. It must NOT be folded into a join's `on` list the way
    /// `FROM a, b WHERE a.x = b.y` is — a `LateralJoin` has no `on` list, and
    /// the equijoin-extraction path is only wired for `LogicalPlan::Join`.
    #[test]
    fn a_where_clause_above_a_lateral_join_stays_a_filter() {
        let plan = lower("SELECT * FROM t, LATERAL generate_series(1, t.id) g WHERE g > 1")
            .expect("lowers");
        let LogicalPlan::Project { input, .. } = plan else {
            panic!("expected Project");
        };
        let LogicalPlan::Filter { input, .. } = *input else {
            panic!("expected Filter above the lateral join, got {input:?}");
        };
        assert!(matches!(*input, LogicalPlan::LateralJoin { .. }));
    }

    /// With nothing to this item's LEFT, a reference reaching outside is an
    /// ordinary correlated reference into the ENCLOSING QUERY, not a lateral
    /// join — `SELECT id, (SELECT sum(g) FROM generate_series(1, lt2.id) g)
    /// FROM lt2` is legal and returns one sum per row live (1 -> 1, 3 -> 6,
    /// 0 -> NULL). The subquery machinery already handles that shape; a
    /// `LateralJoin` here would be a second, wrong answer to the same
    /// question.
    #[test]
    fn an_srf_in_a_subquerys_from_correlates_to_the_outer_query_not_a_lateral_join() {
        let plan =
            lower("SELECT (SELECT sum(g) FROM generate_series(1, t.id) g) FROM t").expect("lowers");
        let LogicalPlan::Project { input, exprs } = plan else {
            panic!("expected Project, got {plan:?}");
        };
        assert!(
            matches!(*input, LogicalPlan::Scan { .. }),
            "no LateralJoin belongs here: {input:?}"
        );
        let Expr::Subquery { subplan, .. } = &exprs[0].0 else {
            panic!("expected a scalar subquery, got {:?}", exprs[0].0);
        };
        let mut found_outer_ref = false;
        walk_plan(subplan, &mut |p| {
            p.for_each_expr(&mut |e| {
                if e.any(&mut |x| matches!(x, Expr::Column(c) if c.relation == OUTER_REF)) {
                    found_outer_ref = true;
                }
            });
        });
        assert!(
            found_outer_ref,
            "t.id inside the subquery's FROM must be tagged OUTER_REF: {subplan:?}"
        );
    }

    // --- SRF in FROM: the honest refusals ------------------------------------

    /// `WITH ORDINALITY` adds a second, bigint column numbering the rows
    /// (`SELECT * FROM generate_series(1,3) WITH ORDINALITY` returns
    /// `generate_series | ordinality`). This lowering produces exactly one
    /// column, so the feature is refused by name rather than silently
    /// dropped — a dropped `WITH ORDINALITY` is a missing column, which is a
    /// wrong answer, not a slow one.
    #[test]
    fn with_ordinality_is_unsupported() {
        let err = lower("SELECT * FROM generate_series(1, 3) WITH ORDINALITY").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("ORDINALITY"), "unexpected message: {msg}");
    }

    /// `ROWS FROM (a(), b())` runs several functions in lockstep, one column
    /// each, padding the shorter with NULL — live, `ROWS FROM
    /// (generate_series(1,2), generate_series(1,3))` returns three rows and
    /// two columns, the last `(NULL, 3)`.
    #[test]
    fn rows_from_is_unsupported() {
        let err = lower("SELECT * FROM ROWS FROM (generate_series(1, 2), generate_series(1, 3))")
            .unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("ROWS FROM"), "unexpected message: {msg}");
    }

    /// Multi-argument `unnest` is the same multi-column shape by another
    /// spelling: live, `unnest(ARRAY[1,2], ARRAY['a','b'])` returns two
    /// columns, both named `unnest`.
    #[test]
    fn multi_argument_unnest_in_from_is_unsupported() {
        let err = lower("SELECT * FROM unnest(ARRAY[1, 2], ARRAY[3, 4])").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("unnest"), "unexpected message: {msg}");
    }

    /// Rule 2 — the case Basin's catalog cannot answer. A function with a
    /// named `OUT` parameter names its own output column and the table alias
    /// renames NOTHING: `jsonb_array_elements(...) AS x` still yields a
    /// column called `value` (its `pg_proc.proargnames` is
    /// `{from_json,value}`). `basin_pgtype::func::FuncSig` carries no
    /// `proargnames`, so this is refused with a message that says which rule
    /// it is, rather than being alias-named and quietly wrong.
    #[test]
    fn a_self_naming_srf_in_from_is_unsupported() {
        let err = lower("SELECT * FROM jsonb_array_elements('[1,2]') AS x").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("OUT parameters"),
            "message should name the rule: {msg}"
        );
    }

    /// A plain (non-set-returning) function in `FROM` is legal Postgres —
    /// `SELECT * FROM abs(-1)` is a one-row relation — and is a different
    /// lowering (a `Project`, not a `ProjectSet`). Refused rather than run
    /// through the SRF path.
    #[test]
    fn a_non_set_returning_function_in_from_is_unsupported() {
        let err = lower("SELECT * FROM upper('x')").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(
            msg.contains("not a set-returning function"),
            "unexpected message: {msg}"
        );
    }

    /// A set-returning function Basin's builtin catalog has no entry for
    /// cannot be given an output column at all — its arity, its type and
    /// whether it self-names are all unknown. Refused, so it falls back
    /// rather than being guessed at.
    #[test]
    fn an_unknown_set_returning_function_in_from_is_unsupported() {
        // `unnest` is in `MockFunctions` (so it resolves to a `FuncId`) but
        // there is no `unnest()` zero-argument overload in the builtin
        // catalog, which is what this exercises.
        let err = lower("SELECT * FROM unnest()").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("builtin catalog"), "unexpected message: {msg}");
    }

    /// Every `FuncKind::SetReturning` entry in `basin_pgtype::func::FUNCS`
    /// must be one this file knows the naming rule for. This is the guard
    /// that keeps [`ALIAS_NAMED_SRFS`] honest as that catalog grows: adding
    /// `jsonb_array_elements` there would otherwise make `FROM
    /// jsonb_array_elements(...) AS x` silently name its column `x` instead
    /// of `value`.
    #[test]
    fn every_builtin_srf_has_a_known_from_position_naming_rule() {
        for sig in basin_pgtype::func::FUNCS
            .iter()
            .filter(|f| f.kind == basin_pgtype::func::FuncKind::SetReturning)
        {
            assert!(
                ALIAS_NAMED_SRFS.contains(&sig.name) || SELF_NAMING_SRFS.contains(&sig.name),
                "`{}` (oid {:?}) is a builtin SRF with no FROM-position naming rule in \
                 lower/select.rs — check its pg_proc.proargnames on a live server and add it \
                 to ALIAS_NAMED_SRFS or SELF_NAMING_SRFS",
                sig.name,
                sig.oid
            );
        }
    }

    /// Walk `plan` and every plan nested below it, including subquery
    /// subplans — `LogicalPlan::for_each_input` alone stops at the node's own
    /// children.
    fn walk_plan(plan: &LogicalPlan, f: &mut impl FnMut(&LogicalPlan)) {
        f(plan);
        plan.for_each_input(&mut |c| walk_plan(c, f));
    }

    /// The schema a `WITH x AS (...) SELECT * FROM x` plan's `CteRef` exposes
    /// — how a `FROM`-position column's TYPE becomes observable in a lowered
    /// plan at all (`lower_select` itself returns only a `LogicalPlan`).
    fn cte_ref_schema(plan: &LogicalPlan) -> Schema {
        let mut found = None;
        walk_plan(plan, &mut |p| {
            if let LogicalPlan::CteRef { schema, .. } = p {
                found = Some(schema.clone());
            }
        });
        found.expect("no CteRef in the plan")
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

    // --- A column's catalog type reaches overload resolution -----------------
    //
    // Two different things are asserted below, and keeping them apart matters:
    //
    //   1. **What the seam delivers.** [`SpyFunctions`] records the argument
    //      types the `FunctionResolver` was actually asked about. That is the
    //      seam's output and this module's whole responsibility — before
    //      `ColumnResolver::column_type` existed, every bare column arrived
    //      there as `unknown`.
    //   2. **Which overload gets picked**, end to end, against the real
    //      `pg_proc` (`basin_pgtype::func`) and `pg_operator` tables. That is
    //      the seam's *consumer*, and it is one step further out: a right
    //      answer needs both a right input and a resolver that ranks
    //      candidates the way Postgres does. See
    //      `date_part_on_a_date_column_now_offers_the_date_type` for the one
    //      family where the input is now right and the ranking is not.
    //
    // A resolver that ignores its argument types (like `MockFunctions` above,
    // deliberately arg-blind for the plan-shape tests) cannot show either.

    /// `pg_proc` itself, so overload choice is the real one.
    struct CatalogFunctions;

    impl FunctionResolver for CatalogFunctions {
        fn resolve(
            &self,
            name: &[String],
            args: &[PgType],
        ) -> Option<(crate::FuncId, crate::lower::expr::FuncKind)> {
            use crate::lower::expr::FuncKind;
            let arg_oids: Vec<Oid> = args.iter().map(|t| t.oid).collect();
            let sig = basin_pgtype::func::resolve(name.last()?, &arg_oids)?;
            let kind = match sig.kind {
                basin_pgtype::func::FuncKind::Scalar => FuncKind::Scalar,
                basin_pgtype::func::FuncKind::Aggregate => FuncKind::Aggregate,
                basin_pgtype::func::FuncKind::Window => FuncKind::Window,
                basin_pgtype::func::FuncKind::SetReturning => FuncKind::SetReturning,
            };
            Some((crate::FuncId(sig.oid), kind))
        }
    }

    /// A temporal table plus a `bigint` column — the two shapes whose
    /// overload choice used to be decided by `unknown`.
    fn typed_tables() -> MockTables {
        tables().with(
            "d",
            300,
            &[
                ("id", PgType::INT8),
                ("day", PgType::DATE),
                ("ts", PgType::TIMESTAMP),
                ("tz", PgType::TIMESTAMPTZ),
            ],
        )
    }

    /// Records every `(name, argument types)` the lowering pass asks about,
    /// then answers exactly as [`CatalogFunctions`] would. This is how a test
    /// sees the *seam's* output rather than its consumer's verdict.
    #[derive(Default)]
    struct SpyFunctions(std::cell::RefCell<Vec<(String, Vec<PgType>)>>);

    impl FunctionResolver for SpyFunctions {
        fn resolve(
            &self,
            name: &[String],
            args: &[PgType],
        ) -> Option<(crate::FuncId, crate::lower::expr::FuncKind)> {
            self.0
                .borrow_mut()
                .push((name.last()?.clone(), args.to_vec()));
            CatalogFunctions.resolve(name, args)
        }
    }

    fn lower_with(sql: &str, funcs: &dyn FunctionResolver) -> Result<LogicalPlan, LowerError> {
        let result = pg_query::parse(sql).expect("parse failed");
        let raw = result.protobuf.stmts.first().expect("no stmt").clone();
        let node = *raw.stmt.expect("no stmt node");
        lower_select(&node, &typed_tables(), &CatalogOperators, funcs)
    }

    fn lower_typed(sql: &str) -> Result<LogicalPlan, LowerError> {
        lower_with(sql, &CatalogFunctions)
    }

    /// The argument types the resolver was asked about for `name`.
    fn arg_types_seen(sql: &str, name: &str) -> Vec<PgType> {
        let spy = SpyFunctions::default();
        lower_with(sql, &spy).expect("should lower");
        let seen = spy.0.borrow();
        seen.iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("`{name}` was never resolved while lowering `{sql}`"))
            .1
            .clone()
    }

    /// The first `ScalarFn`/`Aggregate`/`Window` oid reachable in `plan`,
    /// descending into a subquery's own plan (which `Expr::any` deliberately
    /// does not — see its docs).
    fn first_func_oid(plan: &LogicalPlan) -> Option<u32> {
        fn in_expr(e: &Expr, out: &mut Option<u32>) {
            match e {
                Expr::ScalarFn { func, .. }
                | Expr::Aggregate { func, .. }
                | Expr::Window { func, .. } => {
                    out.get_or_insert(func.0 .0);
                }
                Expr::Subquery { subplan, .. } => {
                    if out.is_none() {
                        *out = first_func_oid(subplan);
                    }
                }
                _ => {}
            }
            e.for_each_child(&mut |c| in_expr(c, out));
        }
        let mut found = None;
        plan.for_each_expr(&mut |e| in_expr(e, &mut found));
        if found.is_none() {
            plan.for_each_input(&mut |c| {
                if found.is_none() {
                    found = first_func_oid(c);
                }
            });
        }
        found
    }

    /// **The seam does its job here, and the misresolution survives one step
    /// further out.**
    ///
    /// `day` now reaches `pg_proc` resolution as `date` — that is what this
    /// module owns, and it is asserted directly. The overload actually
    /// chosen is still 1171, `date_part(text, timestamptz)`, where a live
    /// PostgreSQL 18.2 picks 1384, `date_part(text, date)`.
    ///
    /// The reason is *not* this seam: it is `basin_pgtype::func::resolve`'s
    /// documented "first-in-table match wins" rule. `'month'` is a bare
    /// string literal, so it arrives as `unknown` (Postgres types it that way
    /// too — `transformAConst`); that makes the all-arguments-exact pass fail
    /// as a whole, and the implicit-coercion pass then takes whichever row
    /// comes first, `timestamptz` being ahead of `date` in the table.
    /// Postgres instead discards candidates with fewer exact matches on the
    /// arguments whose type IS known (`func_select_candidate`), which leaves
    /// only `date_part(text, date)`. That preference pass is the remaining
    /// fix, and it belongs in `basin_pgtype::func::resolve`, not here —
    /// `resolve`'s own doc comment claims the simplification "is not an
    /// observable difference" for the tabulated functions, which this case
    /// disproves.
    #[test]
    fn date_part_on_a_date_column_now_offers_the_date_type() {
        assert_eq!(
            arg_types_seen("SELECT date_part('month', day) FROM d", "date_part"),
            vec![PgType::UNKNOWN, PgType::DATE],
            "the column's catalog type reaches overload resolution; before \
             `ColumnResolver::column_type` both arguments were `unknown`"
        );
    }

    #[test]
    fn extract_on_a_date_column_now_offers_the_date_type() {
        assert_eq!(
            arg_types_seen("SELECT extract(YEAR FROM day) FROM d", "extract"),
            vec![PgType::UNKNOWN, PgType::DATE],
        );
    }

    #[test]
    fn date_part_on_a_timestamptz_column_still_picks_the_timestamptz_overload() {
        let plan = lower_typed("SELECT date_part('hour', tz) FROM d").unwrap();
        assert_eq!(
            first_func_oid(&plan),
            Some(1171),
            "date_part(text, timestamptz) — the one case where the old \
             `unknown` answer happened to be right, and must stay right"
        );
    }

    /// Both arguments are self-typed (a column and an explicit cast), so
    /// `func::resolve`'s all-exact pass hits and the overload is the one
    /// PostgreSQL picks. Contrast `date_part_on_a_date_column_now_offers_the_date_type`,
    /// where one argument is an `unknown` literal.
    #[test]
    fn age_of_two_timestamps_picks_the_timestamp_overload() {
        let plan = lower_typed("SELECT age(ts, '2024-01-01'::timestamp) FROM d").unwrap();
        assert_eq!(
            first_func_oid(&plan),
            Some(2058),
            "age(timestamp, timestamp); 1199 is the timestamptz pair, which \
             depends on a session timezone Basin's physical timestamptz has no \
             room for"
        );
    }

    #[test]
    fn extract_on_a_timestamptz_column_still_picks_the_timestamptz_overload() {
        let plan = lower_typed("SELECT extract(EPOCH FROM tz) FROM d").unwrap();
        assert_eq!(first_func_oid(&plan), Some(6203));
    }

    /// `id ^ 2` — the operator side of the same widening. `^` has exactly one
    /// `pg_operator` row (`float8 ^ float8`), so the oid cannot move; what
    /// moves is what the resolver was *told*, and an operand type is not
    /// separately observable in the plan. Asserted through the function seam
    /// instead, on the same column: `abs(id)` sees `int8`.
    #[test]
    fn a_bigint_column_reaches_resolution_as_int8_not_unknown() {
        assert_eq!(
            arg_types_seen("SELECT abs(id) FROM d", "abs"),
            vec![PgType::INT8]
        );
    }

    /// The regression an earlier attempt at this widening measured and
    /// reverted on: `array_agg`/`lag` were monomorphized at `int4`/`text`
    /// only, so a `bigint` column resolved *only* while it arrived as
    /// `unknown` and coerced into the `int4` row. `basin_pgtype::func` now
    /// carries `int8`/`float8`/`numeric` rows for both — this is the test
    /// that says so from the planner's side.
    #[test]
    fn a_bigint_column_still_resolves_the_polymorphic_aggregates_and_windows() {
        for sql in [
            "SELECT array_agg(id) FROM d",
            "SELECT lag(id) OVER (ORDER BY id) FROM d",
            "SELECT lead(id) OVER (ORDER BY id) FROM d",
            "SELECT first_value(id) OVER (ORDER BY id) FROM d",
            "SELECT last_value(id) OVER (ORDER BY id) FROM d",
            "SELECT nth_value(id, 2) OVER (ORDER BY id) FROM d",
        ] {
            assert!(
                lower_typed(sql).is_ok(),
                "{sql} must still resolve now that `id` arrives as int8"
            );
        }
    }

    /// A correlated reference's `index` belongs to the *enclosing* scope, so
    /// reading it against the inner one would report some unrelated column's
    /// type. `ScopeResolver::column_type` hands it back to `outer` instead —
    /// and `t.b` really is `text`, not the `date` sitting at index 1 of `d`.
    #[test]
    fn a_correlated_column_gets_the_enclosing_scopes_type_not_the_inner_one() {
        let plan = lower_typed("SELECT (SELECT upper(t.b) FROM d) FROM t").expect("should lower");
        assert_eq!(
            first_func_oid(&plan),
            Some(871),
            "upper(text) — resolving `t.b` against `d`'s index 1 would have \
             typed it `date` and found no `upper` at all"
        );
    }
}
