//! Lowering `pg_query`'s `InsertStmt` / `UpdateStmt` / `DeleteStmt` into
//! Basin's [`LogicalPlan::Insert`] / [`Update`](LogicalPlan::Update) /
//! [`Delete`](LogicalPlan::Delete).
//!
//! # Why this file exists
//!
//! `crate` module docs and `LogicalPlan`'s own doc comment both point at the
//! same gap: DataFusion cannot plan DML as a relation (`executor.rs:3128`,
//! `:13038`), so `WITH x AS (INSERT ... RETURNING ...) SELECT ... FROM x` —
//! an ordinary Postgres idiom — does not work today. `LogicalPlan::Insert` /
//! `Update` / `Delete` already carry `returning` and already nest inside
//! `LogicalPlan::Cte` (see `lib.rs`'s own `data_modifying_ctes_are_expressible`
//! test); this module is what actually produces those nodes from a parsed
//! statement, which is what makes that shape reachable.
//!
//! # Reuse, not a parallel design
//!
//! This module deliberately does not invent a second column/table/operator/
//! function resolution mechanism:
//!
//! - [`crate::lower::select::TableResolver`] is reused as-is — a caller wires
//!   one resolver for both `SELECT` and DML lowering.
//! - [`crate::lower::expr::ColumnResolver`] / [`OperatorResolver`] /
//!   [`FunctionResolver`] / [`SubqueryLowerer`] / [`lower_expr`] are reused
//!   exactly as `lower::select` reuses them.
//! - `INSERT INTO t SELECT ...`, and the `FROM`/`USING` relation of an
//!   `UPDATE`/`DELETE`, are lowered by handing a synthetic `SELECT` back to
//!   [`crate::lower::select::lower_select`] rather than re-implementing `FROM`
//!   list / `JOIN` lowering here — that logic (equijoin extraction, join
//!   kinds, `NATURAL`/`USING` rejection, subquery-in-`FROM` rejection, ...)
//!   already exists there and should not exist twice.
//!
//! What genuinely cannot be reused is `lower::select`'s `Scope` type: it is
//! private to that module (by design — see its own doc comment: "this
//! module's job is to build one \[...\], not to define a second
//! column-resolution mechanism", scoped to *that* module's clauses). DML has
//! its own clauses with their own scoping rules (a `SET` target is always a
//! bare column of the table being modified; `RETURNING`/`WHERE`/`SET`'s value
//! sees the modified table plus whatever `FROM`/`USING` contributes;
//! `ON CONFLICT DO UPDATE` sees the conflicting row and `excluded` — two
//! copies of the *same* schema, so a plain "ambiguous on any repeated name"
//! scope is actively wrong there), so this module builds its own small scope
//! types for those rules — [`FlatScope`] and [`ConflictColumnResolver`] below.
//!
//! # Column numbering
//!
//! Every column reference this module produces uses `relation: 0` with a
//! flat, concatenated index — exactly the convention `lower::select`'s
//! `Scope` already establishes for `WHERE`/`SELECT list`/`JOIN` filters (see
//! that module's doc comment on equijoin extraction). `ON CONFLICT DO
//! UPDATE`'s two same-shaped rows (existing vs. `excluded`) are told apart by
//! *index range* (`[0, n)` for the existing row, `[n, 2n)` for `excluded`),
//! the same flat-index idea applied to two copies of one schema instead of
//! two different ones.
//!
//! # What "the modified table's own schema" is inside `Update`/`Delete`
//!
//! `crate::schema::output_schema` cannot report `Update`/`Delete`'s own
//! target-table shape — documented there as a known limitation, revisited
//! "once `Update`/`Delete` carry (or the catalog can supply) the table's own
//! schema alongside `from`/`using`". That limitation is about schema
//! *inference* on an already-built plan with no catalog in hand. Lowering, by
//! contrast, always has a catalog (a [`TableResolver`] is a required
//! argument), so `SET`/`WHERE`/`RETURNING` can and do resolve target-table
//! columns correctly here; `output_schema`'s gap is unaffected either way and
//! not this module's to close.

use pg_query::protobuf::{
    node::Node as NodeEnum, DeleteStmt, InferClause, InsertStmt, Node, OnConflictAction,
    OnConflictClause, OverridingKind, RangeVar, UpdateStmt,
};

use crate::lower::expr::{
    lower_expr, ColumnResolver, FunctionResolver, LowerCtx, OperatorResolver, SubqueryLowerer,
};
use crate::lower::select::{self, TableResolver};
use crate::lower::LowerError;
use crate::{ColId, ColumnRef, Expr, LogicalPlan, OnConflict, Schema, SnapshotId, TableId};

/// Lower a top-level `InsertStmt` / `UpdateStmt` / `DeleteStmt` parse-tree
/// node (as produced by `pg_query::parse`) into a [`LogicalPlan`].
///
/// `MERGE` and anything else are [`LowerError::Unsupported`] /
/// [`LowerError::Malformed`] — DML statements, unlike a `SELECT`, are never
/// found nested inside another node, so there is no analogue of
/// `lower_select`'s "or found as a `SubLink::subselect`" caveat.
pub fn lower_dml(
    node: &Node,
    tables: &dyn TableResolver,
    operators: &dyn OperatorResolver,
    functions: &dyn FunctionResolver,
) -> Result<LogicalPlan, LowerError> {
    let res = Resolvers {
        tables,
        operators,
        functions,
    };
    match node.node.as_ref() {
        Some(NodeEnum::InsertStmt(stmt)) => lower_insert(stmt, &res),
        Some(NodeEnum::UpdateStmt(stmt)) => lower_update(stmt, &res),
        Some(NodeEnum::DeleteStmt(stmt)) => lower_delete(stmt, &res),
        Some(NodeEnum::MergeStmt(_)) => {
            Err(LowerError::Unsupported("MERGE is not yet lowered".into()))
        }
        Some(_) => Err(LowerError::Malformed(
            "expected an INSERT, UPDATE or DELETE statement",
        )),
        None => Err(LowerError::Malformed("empty statement node")),
    }
}

/// The three resolver seams a DML lowering pass needs — the same bundle
/// `lower::select::Resolvers` is, reused as a concept (not literally: that
/// one is private to its module) because the shape of the problem is
/// identical.
struct Resolvers<'a> {
    tables: &'a dyn TableResolver,
    operators: &'a dyn OperatorResolver,
    functions: &'a dyn FunctionResolver,
}

impl<'a> Resolvers<'a> {
    fn subqueries(&self) -> DmlSubqueries<'a> {
        DmlSubqueries {
            tables: self.tables,
            operators: self.operators,
            functions: self.functions,
        }
    }
}

/// Lowers a nested `SELECT` (a scalar/`EXISTS`/`IN` subquery inside a `SET`
/// value, a `WHERE`, or `RETURNING`) by recursing into
/// [`select::lower_select`] with the same resolvers.
struct DmlSubqueries<'a> {
    tables: &'a dyn TableResolver,
    operators: &'a dyn OperatorResolver,
    functions: &'a dyn FunctionResolver,
}

impl<'a> SubqueryLowerer for DmlSubqueries<'a> {
    fn lower(&self, subselect: &Node) -> Result<LogicalPlan, LowerError> {
        select::lower_select(subselect, self.tables, self.operators, self.functions)
    }
}

/// Owns the [`SubqueryLowerer`] (built fresh per clause) so a borrowed
/// [`LowerCtx`] can point at it without every call site juggling an extra
/// named local — the same pattern `lower::select`'s own `LowerCtxOwned` uses.
struct LowerCtxOwned<'a> {
    subqueries: DmlSubqueries<'a>,
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
            // No DML clause may contain a window function, so there is never
            // an `OVER w` here to resolve — see `LowerCtx::named_windows`.
            named_windows: &[],
        }
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

// ─── Scope: a flat, concatenated list of (qualifier, schema) relations ────

/// A small stand-in for `lower::select::Scope`, restricted to what DML needs:
/// resolve `t.col` / `col` against an ordered, concatenated list of named
/// relations, refusing to guess on an unqualified ambiguous match. Cannot
/// reuse the original directly — it is private to `lower::select` by that
/// module's own design (see this module's doc comment) — so this is a
/// from-scratch, DML-scoped rebuild of the same idea, not a copy-paste.
struct FlatScope {
    relations: Vec<(String, Schema)>,
}

impl FlatScope {
    fn resolve(&self, parts: &[String]) -> Option<ColumnRef> {
        if parts.len() >= 2 {
            let qualifier = &parts[parts.len() - 2];
            let name = parts.last()?;
            let mut offset = 0u16;
            for (q, schema) in &self.relations {
                if q == qualifier {
                    let pos = schema.iter().position(|(n, _)| n == name)?;
                    return Some(ColumnRef {
                        relation: 0,
                        index: offset + pos as u16,
                        name: name.clone(),
                    });
                }
                offset += schema.len() as u16;
            }
            return None;
        }
        let name = parts.first()?;
        let mut offset = 0u16;
        let mut found = None;
        for (_, schema) in &self.relations {
            if let Some(pos) = schema.iter().position(|(n, _)| n == name) {
                if found.is_some() {
                    return None; // ambiguous
                }
                found = Some(offset + pos as u16);
            }
            offset += schema.len() as u16;
        }
        found.map(|idx| ColumnRef {
            relation: 0,
            index: idx,
            name: name.clone(),
        })
    }

    /// Every `(name, flat index)` pair `*` or `t.*` expands to.
    fn star_columns(&self, qualifier: Option<&str>) -> Vec<(String, u16)> {
        let mut out = Vec::new();
        let mut offset = 0u16;
        for (q, schema) in &self.relations {
            if qualifier.is_none_or(|want| want == q) {
                for (i, (name, _)) in schema.iter().enumerate() {
                    out.push((name.clone(), offset + i as u16));
                }
            }
            offset += schema.len() as u16;
        }
        out
    }
}

struct FlatScopeResolver<'a>(&'a FlatScope);

impl ColumnResolver for FlatScopeResolver<'_> {
    fn resolve(&self, parts: &[String]) -> Option<ColumnRef> {
        self.0.resolve(parts)
    }
}

/// The `ON CONFLICT DO UPDATE` scope: the conflicting row (bare `col` or
/// `<table-or-alias>.col`) and `excluded` (the row that would have been
/// inserted), told apart by index range over one shared table-shaped schema
/// rather than by a second, differently-shaped relation.
///
/// This cannot be a [`FlatScope`] of two `(qualifier, schema)` entries: a
/// `FlatScope` refuses an unqualified name that matches more than one
/// relation, but Postgres's own rule here is that a bare column name always
/// means the *existing* row, never ambiguous with `excluded` — matching
/// `postgres/src/backend/parser/parse_clause.c`'s `transformOnConflictClause`,
/// which resolves the unqualified `SET`/`WHERE` namespace against the target
/// relation only, with `excluded` reachable exclusively by its own name.
struct ConflictColumnResolver<'a> {
    table_qualifier: &'a str,
    schema: &'a Schema,
}

impl ColumnResolver for ConflictColumnResolver<'_> {
    fn resolve(&self, parts: &[String]) -> Option<ColumnRef> {
        if parts.len() >= 2 {
            let qualifier = &parts[parts.len() - 2];
            let name = parts.last()?;
            if qualifier == "excluded" {
                let pos = self.schema.iter().position(|(n, _)| n == name)?;
                return Some(ColumnRef {
                    relation: 0,
                    index: self.schema.len() as u16 + pos as u16,
                    name: name.clone(),
                });
            }
            if qualifier == self.table_qualifier {
                let pos = self.schema.iter().position(|(n, _)| n == name)?;
                return Some(ColumnRef {
                    relation: 0,
                    index: pos as u16,
                    name: name.clone(),
                });
            }
            return None;
        }
        let name = parts.first()?;
        let pos = self.schema.iter().position(|(n, _)| n == name)?;
        Some(ColumnRef {
            relation: 0,
            index: pos as u16,
            name: name.clone(),
        })
    }
}

// ─── Shared helpers ─────────────────────────────────────────────────────

/// Resolve a DML statement's target relation via the shared [`TableResolver`]
/// seam.
fn resolve_relation(
    rv: &RangeVar,
    tables: &dyn TableResolver,
) -> Result<(TableId, Schema), LowerError> {
    let mut parts = Vec::new();
    if !rv.schemaname.is_empty() {
        parts.push(rv.schemaname.clone());
    }
    parts.push(rv.relname.clone());
    tables
        .resolve_table(&parts)
        .ok_or_else(|| LowerError::UnknownName(parts.join(".")))
}

/// The name a DML statement's target table is addressed by within its own
/// clauses: `AS alias` if given (Postgres hides the real name once an alias
/// is present, same rule `lower::select`'s `ScopeRelation` documents),
/// otherwise the table's own name.
fn target_qualifier(rv: &RangeVar) -> String {
    rv.alias
        .as_ref()
        .map(|a| a.aliasname.clone())
        .unwrap_or_else(|| rv.relname.clone())
}

/// `Some(None)` for a bare `*`, `Some(Some(qualifier))` for `t.*`, `None` for
/// anything that isn't a star reference — the `RETURNING *` analogue of
/// `lower::select`'s identically-named (and, again, private) helper.
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

/// Postgres's own default column name for an unaliased `RETURNING` entry —
/// the DML analogue of `lower::select`'s identically-behaved (and again,
/// private) `default_alias`.
fn default_alias(expr: &Expr) -> String {
    match expr {
        Expr::Column(cr) => cr.name.clone(),
        Expr::Cast { arg, .. } => default_alias(arg),
        _ => "?column?".to_string(),
    }
}

/// Lower a `RETURNING` list against `scope`, expanding a bare `*` / `t.*`
/// the same way a `SELECT` list does. `None` for no `RETURNING` clause at
/// all — distinct from `Some(vec![])`, which cannot actually occur (an empty
/// `RETURNING` is not valid syntax) but is kept distinguishable on principle.
fn lower_returning(
    list: &[Node],
    scope: &FlatScope,
    ctx: &LowerCtx,
) -> Result<Option<Vec<(Expr, String)>>, LowerError> {
    if list.is_empty() {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        let Some(NodeEnum::ResTarget(rt)) = item.node.as_ref() else {
            return Err(LowerError::Malformed(
                "RETURNING list entry is not a ResTarget",
            ));
        };
        let val = rt.val.as_deref().ok_or(LowerError::Malformed(
            "RETURNING list entry with no expression",
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
        let alias = if !rt.name.is_empty() {
            rt.name.clone()
        } else {
            default_alias(&expr)
        };
        out.push((expr, alias));
    }
    Ok(Some(out))
}

/// `SET c = expr [, ...]` (an `UPDATE`'s own `target_list`, or an
/// `ON CONFLICT DO UPDATE`'s `target_list` — the same node shape in both:
/// a `ResTarget` per assignment, `name` the bare target column, `val` the
/// value expression).
///
/// `target_schema` is the table being assigned into, addressed positionally
/// — a `SET` target is never qualified (`SET t.c = ...` is a syntax error in
/// Postgres), so this looks `rt.name` up directly rather than through a
/// [`ColumnResolver`].
fn lower_set_list(
    items: &[Node],
    target_schema: &Schema,
    ctx: &LowerCtx,
) -> Result<Vec<(ColId, Expr)>, LowerError> {
    let mut seen = Vec::with_capacity(items.len());
    let mut out = Vec::with_capacity(items.len());
    for node in items {
        let Some(NodeEnum::ResTarget(rt)) = node.node.as_ref() else {
            return Err(LowerError::Malformed("SET target is not a ResTarget"));
        };
        if !rt.indirection.is_empty() {
            return Err(LowerError::Unsupported(
                "a subscript or field SET target (e.g. `SET a[1] = ...`) is not yet lowered".into(),
            ));
        }
        let pos = target_schema
            .iter()
            .position(|(n, _)| n == &rt.name)
            .ok_or_else(|| LowerError::UnknownName(rt.name.clone()))?;
        if seen.contains(&pos) {
            return Err(LowerError::Unsupported(format!(
                "column \"{}\" set more than once in the same clause",
                rt.name
            )));
        }
        seen.push(pos);

        let val_node = rt
            .val
            .as_deref()
            .ok_or(LowerError::Malformed("SET target with no expression"))?;
        if matches!(val_node.node.as_ref(), Some(NodeEnum::MultiAssignRef(_))) {
            return Err(LowerError::Unsupported(
                "multi-column assignment (`SET (a, b) = ...`) is not yet lowered".into(),
            ));
        }
        // `SET a = DEFAULT` is valid Postgres (confirmed on live Postgres
        // 18.2: it resets the column to its own default expression, same as
        // an omitted INSERT column), and `DEFAULT` here is its own node
        // (`SetToDefault`), not a literal. `lower::expr::lower_expr` has no
        // named case for it, so left alone this falls through to that
        // module's generic "this expression node kind is not yet lowered" —
        // accurate but doesn't name `DEFAULT`. This crate has no
        // column-default catalog to substitute a real expression for it
        // (same gap documented on this module's INSERT `RETURNING` handling
        // below), so it is rejected here, by name, instead.
        if matches!(val_node.node.as_ref(), Some(NodeEnum::SetToDefault(_))) {
            return Err(LowerError::Unsupported(format!(
                "SET \"{}\" = DEFAULT is not yet lowered (no column-default catalog available to lowering)",
                rt.name
            )));
        }
        let expr = lower_expr(val_node, ctx)?;
        out.push((ColId(pos as u16), expr));
    }
    Ok(out)
}

/// `WHERE CURRENT OF cursor_name` — a positioned update/delete tied to an
/// open cursor. There is no cursor machinery in this increment (or a
/// physical executor for any DML at all yet), and this shape needs its own
/// plan representation (a cursor handle, not an expression), so it is
/// rejected here, before `lower_expr`, with a message naming the actual
/// construct rather than `lower::expr`'s generic "this expression node kind"
/// fallback (`CurrentOfExpr` is not one of that function's named cases).
fn reject_current_of(node: &Node) -> Result<(), LowerError> {
    if matches!(node.node.as_ref(), Some(NodeEnum::CurrentOfExpr(_))) {
        return Err(LowerError::Unsupported(
            "WHERE CURRENT OF is not yet lowered".into(),
        ));
    }
    Ok(())
}

/// A `SELECT *` target-list entry, built directly rather than parsed — what
/// [`lower_from_relation`] uses to ask `lower::select` for the plan of a
/// `FROM`/`USING` list without writing a second `FROM`/`JOIN` lowering pass.
fn star_target() -> Node {
    Node {
        node: Some(NodeEnum::ResTarget(Box::new(
            pg_query::protobuf::ResTarget {
                name: String::new(),
                indirection: vec![],
                val: Some(Box::new(Node {
                    node: Some(NodeEnum::ColumnRef(pg_query::protobuf::ColumnRef {
                        fields: vec![Node {
                            node: Some(NodeEnum::AStar(pg_query::protobuf::AStar {})),
                        }],
                        location: -1,
                    })),
                })),
                location: -1,
            },
        ))),
    }
}

/// The per-item `(qualifier, schema)` list a `FROM`/`USING` clause
/// contributes, in the same flat left-to-right order `lower_from_relation`'s
/// plan produces its columns in.
type FromRelations = Vec<(String, Schema)>;

/// Lower an `UPDATE ... FROM <items>` / `DELETE ... USING <items>` relation
/// list by handing `SELECT * FROM <items>` to [`select::lower_select`] —
/// reusing its `FROM`/`JOIN` lowering (equijoin extraction, join kinds,
/// `NATURAL`/`USING` rejection, and everything else that module already does)
/// rather than re-implementing any of it here. `None` for an empty list (no
/// `FROM`/`USING` clause at all).
///
/// Returns the plan alongside the same `(qualifier, schema)` list
/// [`collect_from_relations`] independently derives from the same `items` —
/// needed because `lower::select::lower_select`'s return value is only a
/// `LogicalPlan`, with no schema attached, and `crate::schema::output_schema`
/// cannot supply one either (it bottoms out at the `Scan`s this same `items`
/// list produces, which need a catalog it does not have — see this module's
/// doc comment). Both walks visit `items` in the same left-to-right,
/// left-then-right-of-`JOIN` order, so the returned schema's flat column
/// order matches the plan's (`SELECT *`'s own expansion order) exactly.
fn lower_from_relation(
    items: &[Node],
    res: &Resolvers,
) -> Result<Option<(LogicalPlan, FromRelations)>, LowerError> {
    if items.is_empty() {
        return Ok(None);
    }
    let select_stmt = pg_query::protobuf::SelectStmt {
        distinct_clause: vec![],
        into_clause: None,
        target_list: vec![star_target()],
        from_clause: items.to_vec(),
        where_clause: None,
        group_clause: vec![],
        group_distinct: false,
        having_clause: None,
        window_clause: vec![],
        values_lists: vec![],
        sort_clause: vec![],
        limit_offset: None,
        limit_count: None,
        limit_option: 0,
        locking_clause: vec![],
        with_clause: None,
        // `SetOperation::SetopNone` (1), not 0 (`Undefined`) — `0` would make
        // `lower_select_stmt` treat this as a malformed set operation instead
        // of a plain `SELECT`, since `Undefined != SetopNone`.
        op: pg_query::protobuf::SetOperation::SetopNone as i32,
        all: false,
        larg: None,
        rarg: None,
    };
    let node = Node {
        node: Some(NodeEnum::SelectStmt(Box::new(select_stmt))),
    };
    let plan = select::lower_select(&node, res.tables, res.operators, res.functions)?;
    let relations = collect_from_relations(items, res.tables)?;
    Ok(Some((plan, relations)))
}

/// The schema-only half of `lower_from_relation` — see its doc comment for
/// why this exists alongside a real `lower::select::lower_select` call rather
/// than being derived from the plan it returns.
fn collect_from_relations(
    items: &[Node],
    tables: &dyn TableResolver,
) -> Result<FromRelations, LowerError> {
    let mut out = Vec::new();
    for item in items {
        collect_from_item(item, tables, &mut out)?;
    }
    Ok(out)
}

fn collect_from_item(
    item: &Node,
    tables: &dyn TableResolver,
    out: &mut FromRelations,
) -> Result<(), LowerError> {
    match item.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => {
            let (_, schema) = resolve_relation(rv, tables)?;
            out.push((target_qualifier(rv), schema));
            Ok(())
        }
        Some(NodeEnum::JoinExpr(je)) => {
            let larg = je
                .larg
                .as_deref()
                .ok_or(LowerError::Malformed("JOIN with no left side"))?;
            let rarg = je
                .rarg
                .as_deref()
                .ok_or(LowerError::Malformed("JOIN with no right side"))?;
            collect_from_item(larg, tables, out)?;
            collect_from_item(rarg, tables, out)
        }
        // `lower_from_relation` always calls `select::lower_select` on the
        // very same `items` first, so an arm here only ever runs for a shape
        // that call ACCEPTS — which used to be none of these, and is now the
        // `RangeFunction` one: `select::build_range_function` lowers
        // `generate_series`/`unnest` in a `SELECT`'s `FROM`, but this
        // function is the schema-only half (see [`collect_from_relations`])
        // and has no `FromBuilt` to read a function relation's name and
        // column off. So `UPDATE ... FROM generate_series(...)` stays
        // refused, one layer later than the rest. Each arm names its
        // construct precisely rather than falling through to a generic
        // message.
        Some(NodeEnum::RangeSubselect(_)) => Err(LowerError::Unsupported(
            "a subquery in FROM is not yet lowered".into(),
        )),
        Some(NodeEnum::RangeFunction(_)) => Err(LowerError::Unsupported(
            "a set-returning function in an UPDATE's FROM / DELETE's USING is not yet lowered"
                .into(),
        )),
        Some(_) => Err(LowerError::Unsupported(
            "this FROM item is not yet lowered".into(),
        )),
        None => Err(LowerError::Malformed("empty FROM item")),
    }
}

/// The number of columns `plan`'s output has, where staticly knowable without
/// a catalog — every shape `select::lower_select` can actually return.
/// `None` for anything else (defensively; every real return value of
/// `lower_select` matches one of these).
fn plan_width(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::Project { exprs, .. } => Some(exprs.len()),
        LogicalPlan::Values { schema, .. }
        | LogicalPlan::Empty { schema, .. }
        | LogicalPlan::CteRef { schema, .. } => Some(schema.len()),
        LogicalPlan::Scan { projection, .. } => Some(projection.len()),
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::ProjectSet { input, .. }
        | LogicalPlan::Cte { input, .. } => plan_width(input),
        LogicalPlan::SetOp { left, .. } => plan_width(left),
        LogicalPlan::Aggregate { group, aggs, .. } => Some(group.len() + aggs.len()),
        LogicalPlan::Join { .. }
        | LogicalPlan::LateralJoin { .. }
        | LogicalPlan::Insert { .. }
        | LogicalPlan::Update { .. }
        | LogicalPlan::Delete { .. } => None,
    }
}

// ─── INSERT ─────────────────────────────────────────────────────────────

/// `cols` (`INSERT INTO t (a, b) ...`) resolved to positions in the target
/// table's schema, or every column in table order if no list was written —
/// Postgres's own rule for a column-list-less `INSERT`.
fn resolve_insert_columns(cols: &[Node], table_schema: &Schema) -> Result<Vec<ColId>, LowerError> {
    if cols.is_empty() {
        return Ok((0..table_schema.len() as u16).map(ColId).collect());
    }
    let mut seen = Vec::with_capacity(cols.len());
    let mut out = Vec::with_capacity(cols.len());
    for node in cols {
        let Some(NodeEnum::ResTarget(rt)) = node.node.as_ref() else {
            return Err(LowerError::Malformed(
                "INSERT column list entry is not a ResTarget",
            ));
        };
        if !rt.indirection.is_empty() {
            return Err(LowerError::Unsupported(
                "a subscript or field target in the INSERT column list (e.g. `a[1]`) is not yet lowered".into(),
            ));
        }
        let pos = table_schema
            .iter()
            .position(|(n, _)| n == &rt.name)
            .ok_or_else(|| LowerError::UnknownName(rt.name.clone()))?;
        if seen.contains(&pos) {
            return Err(LowerError::Unsupported(format!(
                "column \"{}\" specified more than once in the INSERT column list",
                rt.name
            )));
        }
        seen.push(pos);
        out.push(ColId(pos as u16));
    }
    Ok(out)
}

/// `ON CONFLICT (target) DO NOTHING|UPDATE`. `None` for no `ON CONFLICT`
/// clause at all.
fn lower_on_conflict(
    clause: Option<&OnConflictClause>,
    table_schema: &Schema,
    table_qualifier: &str,
    res: &Resolvers,
) -> Result<Option<OnConflict>, LowerError> {
    let Some(clause) = clause else {
        return Ok(None);
    };
    let target = lower_conflict_target(clause.infer.as_deref(), table_schema)?;
    let action = OnConflictAction::try_from(clause.action).unwrap_or(OnConflictAction::Undefined);
    match action {
        OnConflictAction::OnconflictNothing => Ok(Some(OnConflict::DoNothing { target })),
        OnConflictAction::OnconflictUpdate => {
            let resolver = ConflictColumnResolver {
                table_qualifier,
                schema: table_schema,
            };
            let lctx = expr_ctx(res, &resolver);
            let ctx = lctx.ctx();
            let set = lower_set_list(&clause.target_list, table_schema, &ctx)?;
            let predicate = clause
                .where_clause
                .as_deref()
                .map(|n| lower_expr(n, &ctx))
                .transpose()?;
            Ok(Some(OnConflict::DoUpdate {
                target,
                set,
                predicate,
            }))
        }
        OnConflictAction::OnconflictNone | OnConflictAction::Undefined => Err(
            LowerError::Malformed("ON CONFLICT clause with an unknown action"),
        ),
    }
}

/// `ON CONFLICT (col, ...)` / `ON CONFLICT ON CONSTRAINT ...` /
/// `ON CONFLICT (...) WHERE ...` — only the plain-column-list form is
/// lowered; the other two need, respectively, a constraint catalog and a
/// representation for a partial index's predicate that
/// [`OnConflict::DoNothing`]/[`DoUpdate`](OnConflict::DoUpdate) do not carry.
fn lower_conflict_target(
    infer: Option<&InferClause>,
    table_schema: &Schema,
) -> Result<Vec<ColId>, LowerError> {
    let Some(infer) = infer else {
        return Ok(Vec::new());
    };
    if infer.where_clause.is_some() {
        return Err(LowerError::Unsupported(
            "ON CONFLICT (...) WHERE <index predicate> is not yet lowered".into(),
        ));
    }
    if !infer.conname.is_empty() {
        return Err(LowerError::Unsupported(
            "ON CONFLICT ON CONSTRAINT is not yet lowered".into(),
        ));
    }
    infer
        .index_elems
        .iter()
        .map(|n| {
            let Some(NodeEnum::IndexElem(ie)) = n.node.as_ref() else {
                return Err(LowerError::Malformed(
                    "ON CONFLICT target is not an IndexElem",
                ));
            };
            if ie.expr.is_some() {
                return Err(LowerError::Unsupported(
                    "an expression ON CONFLICT target (not a plain column) is not yet lowered"
                        .into(),
                ));
            }
            if !ie.collation.is_empty() || !ie.opclass.is_empty() {
                return Err(LowerError::Unsupported(
                    "an ON CONFLICT target with COLLATE or an opclass is not yet lowered".into(),
                ));
            }
            table_schema
                .iter()
                .position(|(n, _)| n == &ie.name)
                .map(|p| ColId(p as u16))
                .ok_or_else(|| LowerError::UnknownName(ie.name.clone()))
        })
        .collect()
}

/// `INSERT INTO t VALUES (DEFAULT, ...)`: `DEFAULT` used as a value
/// expression is its own parse-tree node (`SetToDefault`), distinct from a
/// literal — confirmed on live Postgres 18.2 (`transformInsertRow`
/// substitutes the column's own default, or NULL, for it at execution time).
/// This crate has no default-expression catalog to draw that substitution
/// from (see this module's doc comment on the identical limitation for
/// `RETURNING`), and `lower::expr::lower_expr` has no named case for
/// `SetToDefault` — left alone, this fails deep inside
/// `select::lower_select` with that module's generic "this expression node
/// kind is not yet lowered" fallback, which is accurate but does not name
/// `DEFAULT`. This checks for it up front, over the same `values_lists` this
/// module's own doc comment says `select::lower_select` is deliberately left
/// to lower, so the message does name it.
fn reject_default_in_values(select_node: &Node) -> Result<(), LowerError> {
    let Some(NodeEnum::SelectStmt(select)) = select_node.node.as_ref() else {
        return Ok(());
    };
    for row in &select.values_lists {
        let Some(NodeEnum::List(l)) = row.node.as_ref() else {
            continue;
        };
        for item in &l.items {
            if matches!(item.node.as_ref(), Some(NodeEnum::SetToDefault(_))) {
                return Err(LowerError::Unsupported(
                    "DEFAULT as a value expression (e.g. `VALUES (DEFAULT, ...)`) is not yet lowered"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

/// The target table's column names this INSERT's own column list leaves
/// out. Postgres fills each from its DEFAULT (or NULL, or errors if NOT NULL
/// with none — all three confirmed on live Postgres 18.2) at write time, a
/// substitution this crate cannot perform during lowering: `TableResolver`
/// exposes only `(TableId, Schema)`, no default-expression catalog.
fn omitted_insert_columns(columns: &[ColId], table_schema: &Schema) -> Vec<String> {
    table_schema
        .iter()
        .enumerate()
        .filter(|(i, _)| !columns.iter().any(|c| c.0 as usize == *i))
        .map(|(_, (name, _))| name.clone())
        .collect()
}

fn lower_insert(stmt: &InsertStmt, res: &Resolvers) -> Result<LogicalPlan, LowerError> {
    if stmt.with_clause.is_some() {
        return Err(LowerError::Unsupported(
            "WITH / common table expressions on INSERT are not yet lowered".into(),
        ));
    }
    let override_kind =
        OverridingKind::try_from(stmt.r#override).unwrap_or(OverridingKind::Undefined);
    if !matches!(
        override_kind,
        OverridingKind::Undefined | OverridingKind::OverridingNotSet
    ) {
        return Err(LowerError::Unsupported(
            "INSERT ... OVERRIDING SYSTEM/USER VALUE is not yet lowered".into(),
        ));
    }

    let relation = stmt
        .relation
        .as_ref()
        .ok_or(LowerError::Malformed("INSERT with no target relation"))?;
    let (table, table_schema) = resolve_relation(relation, res.tables)?;
    let columns = resolve_insert_columns(&stmt.cols, &table_schema)?;

    let Some(select_node) = stmt.select_stmt.as_deref() else {
        return Err(LowerError::Unsupported(
            "INSERT ... DEFAULT VALUES is not yet lowered".into(),
        ));
    };
    reject_default_in_values(select_node)?;
    let input = select::lower_select(select_node, res.tables, res.operators, res.functions)?;
    if let Some(width) = plan_width(&input) {
        if width != columns.len() {
            return Err(LowerError::Unsupported(format!(
                "INSERT has {width} expression(s) for {} target column(s)",
                columns.len()
            )));
        }
    }

    let qualifier = target_qualifier(relation);
    let omitted = omitted_insert_columns(&columns, &table_schema);

    // `RETURNING *`/`<qualifier>.*` expands against whatever scope it is
    // given (`lower_returning`'s `star_columns`), so if that scope were the
    // column-list-restricted one built below, a partial column list would
    // make `*` silently expand to *fewer* columns than Postgres's real
    // `RETURNING *` returns (which sees the full post-write row — defaults
    // included, confirmed on live Postgres 18.2) — wrong output with no
    // error at all. Caught here, before that scope is even built, with a
    // message that says why, rather than left to silently under-return.
    if !omitted.is_empty() {
        for item in &stmt.returning_list {
            let Some(NodeEnum::ResTarget(rt)) = item.node.as_ref() else {
                continue;
            };
            let Some(NodeEnum::ColumnRef(cr)) = rt.val.as_deref().and_then(|v| v.node.as_ref())
            else {
                continue;
            };
            if star_marker(cr)?.is_some() {
                return Err(LowerError::Unsupported(format!(
                    "RETURNING * is not yet lowered for this INSERT: column(s) {} were not given a value, and their DEFAULT/NULL is not available to lowering",
                    omitted.join(", ")
                )));
            }
        }
    }

    // RETURNING sees exactly `input`'s columns, under the target table's real
    // names rather than VALUES's synthetic `column1`/`column2` — see this
    // module's doc comment on why that (not the full table schema) is the
    // right scope: it is what `crate::schema::output_schema`'s existing
    // `Insert` handling (`dml_schema(returning, || output_schema(input))`)
    // already resolves RETURNING against, and it is also the honest answer —
    // a column omitted from the INSERT's own column list has no expression
    // here to name its value, so RETURNING cannot reference it without a
    // default-value catalog this crate does not have yet.
    let returning_schema: Schema = columns
        .iter()
        .map(|c| table_schema[c.0 as usize].clone())
        .collect();
    let scope = FlatScope {
        relations: vec![(qualifier.clone(), returning_schema)],
    };
    let resolver = FlatScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();
    let returning = match lower_returning(&stmt.returning_list, &scope, &ctx) {
        Ok(r) => r,
        // A name that *is* a real column of the target table, just not one
        // this INSERT gave a value to, is not "unknown" in any useful sense
        // — it exists but is unavailable, for the same default-catalog
        // reason `*` is rejected above. Naming that reason (rather than
        // `UnknownName`, which reads identically for a column that plain
        // does not exist) keeps the refusal honest instead of misleading.
        Err(LowerError::UnknownName(name)) if omitted.iter().any(|o| o == &name) => {
            return Err(LowerError::Unsupported(format!(
                "RETURNING \"{name}\" is not yet lowered: \"{name}\" was not given a value by this INSERT, and its DEFAULT/NULL is not available to lowering"
            )));
        }
        Err(e) => return Err(e),
    };

    let on_conflict = lower_on_conflict(
        stmt.on_conflict_clause.as_deref(),
        &table_schema,
        &qualifier,
        res,
    )?;

    Ok(LogicalPlan::Insert {
        table,
        input: Box::new(input),
        columns,
        on_conflict,
        returning,
    })
}

// ─── UPDATE ─────────────────────────────────────────────────────────────

fn lower_update(stmt: &UpdateStmt, res: &Resolvers) -> Result<LogicalPlan, LowerError> {
    if stmt.with_clause.is_some() {
        return Err(LowerError::Unsupported(
            "WITH / common table expressions on UPDATE are not yet lowered".into(),
        ));
    }
    let relation = stmt
        .relation
        .as_ref()
        .ok_or(LowerError::Malformed("UPDATE with no target relation"))?;
    let (table, table_schema) = resolve_relation(relation, res.tables)?;
    let qualifier = target_qualifier(relation);

    let (from_plan, from_relations) = match lower_from_relation(&stmt.from_clause, res)? {
        Some((plan, relations)) => (Some(plan), relations),
        None => (None, Vec::new()),
    };

    let mut relations = vec![(qualifier, table_schema.clone())];
    relations.extend(from_relations);
    let scope = FlatScope { relations };
    let resolver = FlatScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();

    let set = lower_set_list(&stmt.target_list, &table_schema, &ctx)?;

    let predicate = match stmt.where_clause.as_deref() {
        Some(n) => {
            reject_current_of(n)?;
            Some(lower_expr(n, &ctx)?)
        }
        None => None,
    };

    let returning = lower_returning(&stmt.returning_list, &scope, &ctx)?;

    Ok(LogicalPlan::Update {
        table,
        set,
        from: from_plan.map(Box::new),
        predicate,
        returning,
        snapshot: SnapshotId(0),
    })
}

// ─── DELETE ─────────────────────────────────────────────────────────────

fn lower_delete(stmt: &DeleteStmt, res: &Resolvers) -> Result<LogicalPlan, LowerError> {
    if stmt.with_clause.is_some() {
        return Err(LowerError::Unsupported(
            "WITH / common table expressions on DELETE are not yet lowered".into(),
        ));
    }
    let relation = stmt
        .relation
        .as_ref()
        .ok_or(LowerError::Malformed("DELETE with no target relation"))?;
    let (table, table_schema) = resolve_relation(relation, res.tables)?;
    let qualifier = target_qualifier(relation);

    let (using_plan, using_relations) = match lower_from_relation(&stmt.using_clause, res)? {
        Some((plan, relations)) => (Some(plan), relations),
        None => (None, Vec::new()),
    };

    let mut relations = vec![(qualifier, table_schema)];
    relations.extend(using_relations);
    let scope = FlatScope { relations };
    let resolver = FlatScopeResolver(&scope);
    let lctx = expr_ctx(res, &resolver);
    let ctx = lctx.ctx();

    let predicate = match stmt.where_clause.as_deref() {
        Some(n) => {
            reject_current_of(n)?;
            Some(lower_expr(n, &ctx)?)
        }
        None => None,
    };

    let returning = lower_returning(&stmt.returning_list, &scope, &ctx)?;

    Ok(LogicalPlan::Delete {
        table,
        using: using_plan.map(Box::new),
        predicate,
        returning,
        snapshot: SnapshotId(0),
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Datum;
    use basin_pgtype::{Oid, PgType};
    use std::collections::HashMap;

    // --- Mock resolvers (mirroring `lower::select`'s own test mocks; that
    // module's are `#[cfg(test)]`-private, so this is its own copy of the
    // same idea, not a shared import) ------------------------------------

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
                Some("lower") => Some((crate::FuncId(Oid(870)), FuncKind::Scalar)),
                Some("now") => Some((crate::FuncId(Oid(1299)), FuncKind::Scalar)),
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
        lower_dml(&node, &tables(), &CatalogOperators, &MockFunctions)
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

    // ─── 1: INSERT ... VALUES ───────────────────────────────────────────

    #[test]
    fn insert_values_lowers_to_insert_over_values() {
        let plan = lower("INSERT INTO t (id, a, b) VALUES (1, 2, 'x')").unwrap();
        let LogicalPlan::Insert {
            table,
            input,
            columns,
            on_conflict,
            returning,
        } = plan
        else {
            panic!("expected Insert");
        };
        assert_eq!(table, TableId(100));
        assert_eq!(columns, vec![ColId(0), ColId(1), ColId(2)]);
        assert_eq!(on_conflict, None);
        assert_eq!(returning, None);
        let LogicalPlan::Values { rows, .. } = *input else {
            panic!("expected Values under Insert");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
    }

    #[test]
    fn insert_with_no_column_list_targets_every_column_in_table_order() {
        let plan = lower("INSERT INTO t VALUES (1, 2, 'x')").unwrap();
        let LogicalPlan::Insert { columns, .. } = plan else {
            panic!("expected Insert");
        };
        assert_eq!(columns, vec![ColId(0), ColId(1), ColId(2)]);
    }

    #[test]
    fn insert_values_arity_mismatch_is_rejected() {
        let err = lower("INSERT INTO t (id, a) VALUES (1, 2, 3)").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn insert_multi_row_values_lowers_every_row() {
        let plan = lower("INSERT INTO t (id, a, b) VALUES (1, 2, 'x'), (3, 4, 'y')").unwrap();
        let LogicalPlan::Insert { input, .. } = plan else {
            panic!("expected Insert");
        };
        let LogicalPlan::Values { rows, .. } = *input else {
            panic!("expected Values under Insert");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 3);
        assert_eq!(rows[1].len(), 3);
    }

    #[test]
    fn insert_unknown_target_column_is_rejected() {
        let err = lower("INSERT INTO t (nope) VALUES (1)").unwrap_err();
        assert_eq!(err, LowerError::UnknownName("nope".into()));
    }

    #[test]
    fn insert_duplicate_target_column_is_rejected() {
        let err = lower("INSERT INTO t (a, a) VALUES (1, 2)").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    // ─── 2: INSERT ... SELECT ───────────────────────────────────────────

    #[test]
    fn insert_select_lowers_over_the_selects_own_plan() {
        let plan = lower("INSERT INTO t (id, a, b) SELECT id, t_id, c FROM u").unwrap();
        let LogicalPlan::Insert {
            table,
            input,
            columns,
            ..
        } = plan
        else {
            panic!("expected Insert");
        };
        assert_eq!(table, TableId(100));
        assert_eq!(columns, vec![ColId(0), ColId(1), ColId(2)]);
        let LogicalPlan::Project { input: scan, exprs } = *input else {
            panic!("expected Project (SELECT) under Insert");
        };
        assert_eq!(exprs.len(), 3);
        assert!(matches!(*scan, LogicalPlan::Scan { table, .. } if table == TableId(200)));
    }

    #[test]
    fn insert_select_arity_mismatch_is_rejected() {
        let err = lower("INSERT INTO t (id, a) SELECT id, t_id, c FROM u").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    // ─── 3: RETURNING ───────────────────────────────────────────────────

    #[test]
    fn insert_returning_resolves_by_real_target_column_names() {
        let plan = lower("INSERT INTO t (id, a) VALUES (1, 2) RETURNING id, a AS renamed").unwrap();
        let LogicalPlan::Insert { returning, .. } = plan else {
            panic!("expected Insert");
        };
        let returning = returning.unwrap();
        assert_eq!(
            returning,
            vec![(col(0, "id"), "id".into()), (col(1, "a"), "renamed".into())]
        );
    }

    #[test]
    fn insert_returning_a_column_not_in_the_column_list_is_unresolvable() {
        // `b` was never listed, so there is no expression carrying its value
        // for RETURNING to name — see the module doc comment. This must be
        // `Unsupported`, naming `b`, not `UnknownName` — `b` is a real
        // column of `t`, so `UnknownName` (which reads identically for an
        // actually-nonexistent column) would be misleading.
        let err = lower("INSERT INTO t (id, a) VALUES (1, 2) RETURNING b").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains('b'), "{msg}");
    }

    #[test]
    fn insert_returning_star_with_a_partial_column_list_is_rejected() {
        // Postgres's real `RETURNING *` returns the full post-write row
        // (defaults included — confirmed on live Postgres 18.2). Silently
        // expanding `*` against only the given columns would return *fewer*
        // columns with no error at all, so this must refuse outright.
        let err = lower("INSERT INTO t (id, a) VALUES (1, 2) RETURNING *").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains('b'), "{msg}");
    }

    #[test]
    fn insert_returning_star_with_every_column_given_a_value_works() {
        // No column is omitted here (the column list covers the whole
        // table), so `*` is exactly `input`'s columns — no silent gap, no
        // refusal needed.
        let plan = lower("INSERT INTO t (id, a, b) VALUES (1, 2, 'x') RETURNING *").unwrap();
        let LogicalPlan::Insert { returning, .. } = plan else {
            panic!("expected Insert");
        };
        assert_eq!(
            returning.unwrap(),
            vec![
                (col(0, "id"), "id".into()),
                (col(1, "a"), "a".into()),
                (col(2, "b"), "b".into()),
            ]
        );
    }

    #[test]
    fn insert_returning_star_with_no_column_list_works() {
        // No column list at all defaults to every column in table order
        // (Postgres's own rule), so again nothing is omitted.
        let plan = lower("INSERT INTO t VALUES (1, 2, 'x') RETURNING *").unwrap();
        let LogicalPlan::Insert { returning, .. } = plan else {
            panic!("expected Insert");
        };
        assert_eq!(returning.unwrap().len(), 3);
    }

    #[test]
    fn update_returning_star_expands_target_and_from_columns() {
        let plan = lower("UPDATE t SET a = 1 FROM u WHERE t.id = u.t_id RETURNING *").unwrap();
        let LogicalPlan::Update { returning, .. } = plan else {
            panic!("expected Update");
        };
        let returning = returning.unwrap();
        assert_eq!(
            returning,
            vec![
                (col(0, "id"), "id".into()),
                (col(1, "a"), "a".into()),
                (col(2, "b"), "b".into()),
                (col(3, "id"), "id".into()),
                (col(4, "t_id"), "t_id".into()),
                (col(5, "c"), "c".into()),
            ]
        );
    }

    #[test]
    fn delete_returning_resolves_against_the_target_table() {
        let plan = lower("DELETE FROM t WHERE id = 1 RETURNING id, b").unwrap();
        let LogicalPlan::Delete { returning, .. } = plan else {
            panic!("expected Delete");
        };
        assert_eq!(
            returning.unwrap(),
            vec![(col(0, "id"), "id".into()), (col(2, "b"), "b".into())]
        );
    }

    // ─── 4: UPDATE ... SET / FROM ───────────────────────────────────────

    #[test]
    fn update_set_lowers_target_and_value() {
        let plan = lower("UPDATE t SET a = 5 WHERE id = 1").unwrap();
        let LogicalPlan::Update {
            table,
            set,
            from,
            predicate,
            returning,
            ..
        } = plan
        else {
            panic!("expected Update");
        };
        assert_eq!(table, TableId(100));
        assert_eq!(set, vec![(ColId(1), int_lit(5))]);
        assert!(from.is_none());
        assert!(predicate.is_some());
        assert_eq!(returning, None);
    }

    #[test]
    fn update_set_can_reference_the_row_being_updated() {
        // `SET a = a + 1`: the RHS's `a` must resolve to the *target* table's
        // own column, not error as unresolvable.
        let plan = lower("UPDATE t SET a = a + 1").unwrap();
        let LogicalPlan::Update { set, .. } = plan else {
            panic!("expected Update");
        };
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].0, ColId(1));
        assert!(matches!(&set[0].1, Expr::Binary { .. }));
    }

    #[test]
    fn update_from_join_resolves_qualified_columns_from_both_sides() {
        let plan = lower("UPDATE t SET b = u.c FROM u WHERE t.id = u.t_id").unwrap();
        let LogicalPlan::Update {
            set,
            from,
            predicate,
            ..
        } = plan
        else {
            panic!("expected Update");
        };
        // `u.c` is FROM's column, flat-indexed after t's own 3 columns.
        assert_eq!(set, vec![(ColId(2), col(5, "c"))]);
        assert!(from.is_some());
        let LogicalPlan::Project { input, .. } = from.unwrap().as_ref().clone() else {
            panic!("expected Project (SELECT *) as the FROM plan");
        };
        assert!(matches!(*input, LogicalPlan::Scan { table, .. } if table == TableId(200)));
        assert!(predicate.is_some());
    }

    #[test]
    fn update_set_target_not_a_table_column_is_rejected() {
        let err = lower("UPDATE t SET nope = 1").unwrap_err();
        assert_eq!(err, LowerError::UnknownName("nope".into()));
    }

    #[test]
    fn update_set_multi_column_assignment_is_unsupported() {
        // `SET (a, b) = (1, 2)`: both targets share one `MultiAssignRef`
        // value node pointing at the same RowExpr `source` (told apart by
        // `colno`) — confirmed via `pg_query::parse`'s own output for this
        // statement.
        let err = lower("UPDATE t SET (a, b) = (1, 2)").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("multi-column"), "{msg}");
    }

    #[test]
    fn update_set_duplicate_target_is_rejected() {
        let err = lower("UPDATE t SET a = 1, a = 2").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn update_with_no_set_list_and_no_where_lowers_with_empty_set_and_no_predicate() {
        // Degenerate but syntactically valid: `UPDATE t SET a = a` is the
        // usual way to write "no-op", but a bare `WHERE`-less UPDATE with a
        // real SET is exactly this shape minus the predicate.
        let plan = lower("UPDATE t SET a = 1").unwrap();
        let LogicalPlan::Update { predicate, .. } = plan else {
            panic!("expected Update");
        };
        assert!(predicate.is_none());
    }

    // ─── 5: DELETE ... WHERE / USING ────────────────────────────────────

    #[test]
    fn delete_where_lowers_predicate_against_the_target_table() {
        let plan = lower("DELETE FROM t WHERE a = 5").unwrap();
        let LogicalPlan::Delete {
            table,
            using,
            predicate,
            returning,
            ..
        } = plan
        else {
            panic!("expected Delete");
        };
        assert_eq!(table, TableId(100));
        assert!(using.is_none());
        assert!(predicate.is_some());
        assert_eq!(returning, None);
    }

    #[test]
    fn delete_using_resolves_qualified_columns_from_both_sides() {
        let plan = lower("DELETE FROM t USING u WHERE t.id = u.t_id").unwrap();
        let LogicalPlan::Delete {
            using, predicate, ..
        } = plan
        else {
            panic!("expected Delete");
        };
        assert!(using.is_some());
        assert!(predicate.is_some());
    }

    #[test]
    fn delete_with_no_where_has_no_predicate() {
        let plan = lower("DELETE FROM t").unwrap();
        let LogicalPlan::Delete { predicate, .. } = plan else {
            panic!("expected Delete");
        };
        assert!(predicate.is_none());
    }

    // ─── 6: ON CONFLICT ─────────────────────────────────────────────────

    #[test]
    fn on_conflict_do_nothing_with_no_target() {
        let plan = lower("INSERT INTO t (id) VALUES (1) ON CONFLICT DO NOTHING").unwrap();
        let LogicalPlan::Insert { on_conflict, .. } = plan else {
            panic!("expected Insert");
        };
        assert_eq!(on_conflict, Some(OnConflict::DoNothing { target: vec![] }));
    }

    #[test]
    fn on_conflict_do_nothing_with_a_column_target() {
        let plan = lower("INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING").unwrap();
        let LogicalPlan::Insert { on_conflict, .. } = plan else {
            panic!("expected Insert");
        };
        assert_eq!(
            on_conflict,
            Some(OnConflict::DoNothing {
                target: vec![ColId(0)]
            })
        );
    }

    #[test]
    fn on_conflict_do_update_set_references_excluded_and_target() {
        let plan = lower(
            "INSERT INTO t (id, a) VALUES (1, 2) \
             ON CONFLICT (id) DO UPDATE SET a = excluded.a + t.a",
        )
        .unwrap();
        let LogicalPlan::Insert { on_conflict, .. } = plan else {
            panic!("expected Insert");
        };
        let Some(OnConflict::DoUpdate {
            target,
            set,
            predicate,
        }) = on_conflict
        else {
            panic!("expected DoUpdate");
        };
        assert_eq!(target, vec![ColId(0)]);
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].0, ColId(1));
        // excluded.a -> index 3 (table width 3, + position 1); t.a -> index 1.
        let Expr::Binary { lhs, rhs, .. } = &set[0].1 else {
            panic!("expected a Binary expr");
        };
        assert_eq!(**lhs, col(4, "a"));
        assert_eq!(**rhs, col(1, "a"));
        assert!(predicate.is_none());
    }

    #[test]
    fn on_conflict_do_update_unqualified_column_defaults_to_the_target_row() {
        let plan = lower(
            "INSERT INTO t (id, a) VALUES (1, 2) \
             ON CONFLICT (id) DO UPDATE SET a = a + 1",
        )
        .unwrap();
        let LogicalPlan::Insert { on_conflict, .. } = plan else {
            panic!("expected Insert");
        };
        let Some(OnConflict::DoUpdate { set, .. }) = on_conflict else {
            panic!("expected DoUpdate");
        };
        let Expr::Binary { lhs, .. } = &set[0].1 else {
            panic!("expected a Binary expr");
        };
        // Bare `a` on the RHS is the *existing* row, index 1 — not
        // ambiguous with `excluded`, per Postgres's own rule.
        assert_eq!(**lhs, col(1, "a"));
    }

    #[test]
    fn on_conflict_do_update_excluded_can_reference_a_column_omitted_from_the_insert_list() {
        // Unlike plain `RETURNING` (restricted to columns the INSERT itself
        // gave a value, above): `excluded` here resolves against the *full*
        // target-table schema even though `b` was never listed. This is
        // deliberate, not an oversight — confirmed on live Postgres 18.2,
        // `excluded.<omitted column>` reads that column's real DEFAULT (`b`
        // there had `default 42`, and `excluded.b` read `42`). Nothing
        // downstream type-checks `on_conflict`'s `set`/`predicate` against
        // `input`'s (narrower) schema the way `crate::schema::output_schema`
        // does for `RETURNING` (see this module's doc comment on that
        // coupling), so there is no silent-truncation risk in exposing the
        // full row here — only in restricting it to match RETURNING would
        // be *wrong*, refusing valid, idiomatic upsert SQL for no reason.
        let plan = lower(
            "INSERT INTO t (id) VALUES (1) \
             ON CONFLICT (id) DO UPDATE SET b = excluded.b",
        )
        .unwrap();
        let LogicalPlan::Insert { on_conflict, .. } = plan else {
            panic!("expected Insert");
        };
        let Some(OnConflict::DoUpdate { set, .. }) = on_conflict else {
            panic!("expected DoUpdate");
        };
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].0, ColId(2)); // b's position in t
                                         // excluded.b -> table width 3 + position 2 = index 5.
        assert_eq!(set[0].1, col(5, "b"));
    }

    #[test]
    fn on_conflict_do_update_with_where_clause() {
        let plan = lower(
            "INSERT INTO t (id, a) VALUES (1, 2) \
             ON CONFLICT (id) DO UPDATE SET a = excluded.a WHERE t.a < excluded.a",
        )
        .unwrap();
        let LogicalPlan::Insert { on_conflict, .. } = plan else {
            panic!("expected Insert");
        };
        let Some(OnConflict::DoUpdate { predicate, .. }) = on_conflict else {
            panic!("expected DoUpdate");
        };
        assert!(predicate.is_some());
    }

    // ─── Unsupported: at least three unhandled constructs ───────────────

    #[test]
    fn insert_default_values_is_unsupported() {
        let err = lower("INSERT INTO t DEFAULT VALUES").unwrap_err();
        assert_eq!(
            err,
            LowerError::Unsupported("INSERT ... DEFAULT VALUES is not yet lowered".into())
        );
    }

    #[test]
    fn merge_is_unsupported() {
        let err = lower(
            "MERGE INTO t USING u ON t.id = u.t_id \
             WHEN MATCHED THEN UPDATE SET a = u.t_id",
        )
        .unwrap_err();
        assert_eq!(
            err,
            LowerError::Unsupported("MERGE is not yet lowered".into())
        );
    }

    #[test]
    fn insert_with_a_cte_is_unsupported() {
        let err =
            lower("WITH x AS (SELECT 1 AS id) INSERT INTO t (id) SELECT id FROM x").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn update_with_a_cte_is_unsupported() {
        let err = lower("WITH x AS (SELECT 1 AS v) UPDATE t SET a = 1").unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn on_conflict_on_constraint_is_unsupported() {
        let err = lower(
            "INSERT INTO t (id) VALUES (1) \
             ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING",
        )
        .unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn on_conflict_with_an_index_predicate_is_unsupported() {
        let err = lower(
            "INSERT INTO t (id) VALUES (1) \
             ON CONFLICT (id) WHERE id > 0 DO NOTHING",
        )
        .unwrap_err();
        assert!(matches!(err, LowerError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn insert_values_default_is_unsupported() {
        // `DEFAULT` in a VALUES row is its own parse node (`SetToDefault`),
        // not a literal — confirmed on live Postgres 18.2. No
        // default-expression catalog exists to lower it, and the message
        // must name `DEFAULT`, not fall back to `lower::expr`'s generic
        // "this expression node kind" wording.
        let err = lower("INSERT INTO t (id, a) VALUES (DEFAULT, 1)").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("DEFAULT"), "{msg}");
    }

    #[test]
    fn insert_values_default_in_a_later_row_is_still_caught() {
        let err = lower("INSERT INTO t (id, a) VALUES (1, 2), (3, DEFAULT)").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("DEFAULT"), "{msg}");
    }

    #[test]
    fn update_set_default_is_unsupported() {
        // `SET a = DEFAULT` is valid Postgres (confirmed on live Postgres
        // 18.2: resets the column to its own default), but needs the same
        // unavailable default-expression catalog.
        let err = lower("UPDATE t SET a = DEFAULT").unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("DEFAULT") && msg.contains('a'), "{msg}");
    }

    #[test]
    fn on_conflict_do_update_set_default_is_unsupported() {
        let err = lower(
            "INSERT INTO t (id, a) VALUES (1, 2) \
             ON CONFLICT (id) DO UPDATE SET a = DEFAULT",
        )
        .unwrap_err();
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(msg.contains("DEFAULT"), "{msg}");
    }

    // ─── Insert column list shorter than the table ──────────────────────

    #[test]
    fn insert_with_a_shorter_column_list_still_builds_a_plan() {
        // Postgres fills unlisted columns from their own DEFAULT (or NULL) at
        // execution time — a storage-layer concern this lowering pass does
        // not need to represent: the Insert node just carries the columns
        // that were actually given values.
        let plan = lower("INSERT INTO t (a) VALUES (1)").unwrap();
        let LogicalPlan::Insert { columns, input, .. } = plan else {
            panic!("expected Insert");
        };
        assert_eq!(columns, vec![ColId(1)]);
        let LogicalPlan::Values { schema, .. } = *input else {
            panic!("expected Values");
        };
        assert_eq!(schema.len(), 1);
    }
}
