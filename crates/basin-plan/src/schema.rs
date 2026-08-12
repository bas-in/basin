//! Output-schema inference for a [`LogicalPlan`].
//!
//! Every rewrite the optimizer performs has to preserve a plan's output
//! schema, so schema inference is the contract the optimizer is checked
//! against rather than a convenience: [`output_schema`] is what a rule can be
//! tested against before-and-after to prove it did not change what a query
//! returns.
//!
//! # What this module cannot do yet
//!
//! A handful of shapes genuinely require a catalog that does not exist yet
//! (see `crate::lib` module docs — "nothing consumes this crate yet"):
//! function/operator/aggregate/window return types (`pg_proc` /
//! `pg_operator`), a `Scan`'s column names and types (`pg_attribute`), and a
//! composite type's field names and types (also `pg_attribute`) for anything
//! but a literal `ROW(...)`. Those return [`SchemaError::Unimplemented`]
//! rather than a guess — see the comments at each site for why guessing would
//! be worse than an honest error.

use crate::{Expr, JoinKind, LogicalPlan, Schema, SubqueryKind, Subscript};
use basin_pgtype::{oid, PgType};

/// Why a plan's schema could not be inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// A column reference points outside its input's schema.
    ColumnOutOfRange { relation: u16, index: u16 },
    /// The two sides of a set operation have incompatible schemas.
    SetOpMismatch,
    /// Not yet implemented for this plan shape.
    Unimplemented(&'static str),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::ColumnOutOfRange { relation, index } => write!(
                f,
                "column reference (relation {relation}, index {index}) is out of range"
            ),
            SchemaError::SetOpMismatch => {
                write!(
                    f,
                    "set operation arms do not have the same number of columns"
                )
            }
            SchemaError::Unimplemented(what) => {
                write!(f, "schema inference not yet implemented: {what}")
            }
        }
    }
}

impl std::error::Error for SchemaError {}

/// Infer the output schema of `plan`.
pub fn output_schema(plan: &LogicalPlan) -> Result<Schema, SchemaError> {
    match plan {
        // A `Scan`'s columns are `ColId`s — opaque catalog references with no
        // name or type attached to the plan itself. Resolving them needs
        // `pg_attribute`, which does not exist yet; inventing a name/type
        // here would be indistinguishable from a bug that silently returns
        // wrong data.
        LogicalPlan::Scan { .. } => Err(SchemaError::Unimplemented(
            "Scan output schema (needs the catalog for column names/types)",
        )),

        LogicalPlan::Values { schema, .. }
        | LogicalPlan::Empty { schema, .. }
        | LogicalPlan::CteRef { schema, .. } => Ok(schema.clone()),

        LogicalPlan::Project { input, exprs } => {
            let input_schema = output_schema(input)?;
            exprs
                .iter()
                .map(|(expr, alias)| Ok((alias.clone(), expr_type(expr, &input_schema)?)))
                .collect()
        }

        // A predicate narrows rows, never columns.
        LogicalPlan::Filter { input, .. } => output_schema(input),

        // Postgres's own target-list order for a GROUP BY query: the grouping
        // columns first, in the order written, then the aggregates.
        LogicalPlan::Aggregate {
            input, group, aggs, ..
        } => {
            let input_schema = output_schema(input)?;
            group
                .iter()
                .chain(aggs.iter())
                .map(|expr| Ok((display_name(expr), expr_type(expr, &input_schema)?)))
                .collect()
        }

        // Sorting and limiting narrow/reorder rows, never columns.
        LogicalPlan::Sort { input, .. } => output_schema(input),
        LogicalPlan::Limit { input, .. } => output_schema(input),

        LogicalPlan::Join {
            left, right, kind, ..
        } => join_schema(left, right, *kind),
        LogicalPlan::LateralJoin { outer, inner, kind } => join_schema(outer, inner, *kind),

        LogicalPlan::SetOp { left, right, .. } => {
            let left_schema = output_schema(left)?;
            let right_schema = output_schema(right)?;
            // `SELECT a, b FROM x UNION SELECT c, d FROM y` names its output
            // `a, b` — Postgres always takes the column names of the first
            // (leftmost) SELECT, `parse_clause.c`'s `transformSetOperationTree`.
            // Types are assumed already unified by lowering (the same
            // simplification `expr_type`'s CASE/COALESCE handling makes — see
            // there), so taking the left side for types too is not a
            // shortcut, it is the whole answer once that assumption holds.
            if left_schema.len() != right_schema.len() {
                return Err(SchemaError::SetOpMismatch);
            }
            Ok(left_schema)
        }

        // DISTINCT and DISTINCT ON filter rows to one per group; the columns
        // returned are exactly the input's.
        LogicalPlan::Distinct { input, .. } => output_schema(input),

        // A window node adds computed columns alongside the input's, the way
        // an aggregate replaces them — the input rows are still visible
        // (`SELECT *, rank() OVER (...) FROM t` keeps every column of `t`).
        LogicalPlan::Window { input, windows } => {
            let input_schema = output_schema(input)?;
            let mut out = input_schema.clone();
            for w in windows {
                out.push((display_name(w), expr_type(w, &input_schema)?));
            }
            Ok(out)
        }

        // Expanding a set-returning function widens the target list, it does
        // not replace it — `SELECT x, generate_series(1,3) FROM t` still has
        // `x` in every output row.
        LogicalPlan::ProjectSet { input, srfs } => {
            let input_schema = output_schema(input)?;
            let mut out = input_schema.clone();
            for srf in srfs {
                out.push((display_name(srf), expr_type(srf, &input_schema)?));
            }
            Ok(out)
        }

        // The CTE node's own output is whatever the query *using* the CTE
        // produces; `body` defines what `CteRef` sees, it is not itself
        // consumed here.
        LogicalPlan::Cte { input, .. } => output_schema(input),

        LogicalPlan::Insert {
            input, returning, ..
        } => dml_schema(returning, || output_schema(input)),

        // Unlike `Insert`, `Update`/`Delete` carry no plan node that produces
        // the target table's own row shape — there is no catalog yet to ask
        // "what does this table's row look like" the way `Insert`'s `input`
        // already answers it for the inserted row. A `RETURNING` expression
        // that only touches the optional `FROM`/`USING` relation resolves
        // correctly; one that references a bare target-table column reports
        // `ColumnOutOfRange` against an empty base schema rather than
        // guessing. Revisit once `Update`/`Delete` carry (or the catalog can
        // supply) the table's own schema alongside `from`/`using`.
        LogicalPlan::Update {
            from, returning, ..
        } => dml_schema(returning, || match from {
            Some(f) => output_schema(f),
            None => Ok(Vec::new()),
        }),
        LogicalPlan::Delete {
            using, returning, ..
        } => dml_schema(returning, || match using {
            Some(u) => output_schema(u),
            None => Ok(Vec::new()),
        }),
    }
}

/// The shared shape of `Join` and `LateralJoin`: left/outer ++ right/inner,
/// except `LeftSemi`/`LeftAnti`, which filter the left side rather than widen
/// it — `WHERE EXISTS (...)` never adds a column, however many the subquery
/// selects. Getting this backwards is the classic join-schema bug: a semi/anti
/// join that (wrongly) reports the right side's columns produces a schema
/// that does not match what the join actually emits.
///
/// KNOWN LIMITATION: `Left`/`Right`/`Full` make the non-preserved side's
/// columns nullable at runtime (an unmatched row still needs to appear, with
/// NULLs where the other side would be), but [`Schema`] carries no
/// nullability — it is `Vec<(String, PgType)>`. Adding a field for this is
/// deliberately left to whoever consumes this to decide the shape of; not
/// invented here.
fn join_schema(
    left: &LogicalPlan,
    right: &LogicalPlan,
    kind: JoinKind,
) -> Result<Schema, SchemaError> {
    let left_schema = output_schema(left)?;
    match kind {
        JoinKind::LeftSemi | JoinKind::LeftAnti => Ok(left_schema),
        JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Cross => {
            let right_schema = output_schema(right)?;
            Ok(left_schema.into_iter().chain(right_schema).collect())
        }
    }
}

/// The shared shape of `Insert`/`Update`/`Delete`: the output schema is the
/// `RETURNING` list if there is one, and empty otherwise — a DML statement
/// without `RETURNING` sends back no rows for a client to read (`CommandComplete`,
/// not `RowDescription` + rows).
///
/// `base_schema` is computed lazily: when there is no `RETURNING` list, the
/// (possibly `Unimplemented`) schema of the DML's own input is irrelevant and
/// must not be forced.
fn dml_schema(
    returning: &Option<Vec<(Expr, String)>>,
    base_schema: impl FnOnce() -> Result<Schema, SchemaError>,
) -> Result<Schema, SchemaError> {
    let Some(items) = returning else {
        return Ok(Vec::new());
    };
    let base = base_schema()?;
    items
        .iter()
        .map(|(expr, alias)| Ok((alias.clone(), expr_type(expr, &base)?)))
        .collect()
}

/// Postgres's default column name for an unaliased target-list entry —
/// `FigureColname` in `parse_target.c`, simplified to what is derivable
/// without a catalog: a bare column reference keeps its name, a cast passes
/// through the name of what it wraps, and everything else (a function call
/// most of all — `count`, `sum`, `row_number`, …) is `?column?`. Naming a
/// function call by its actual name needs `pg_proc`, which does not exist
/// yet; `?column?` is what Postgres itself falls back to for any target-list
/// entry it cannot name more specifically, so this is a real Postgres
/// behaviour applied more often than Postgres needs to, not an invented one.
fn display_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(cr) => cr.name.clone(),
        Expr::Cast { arg, .. } => display_name(arg),
        _ => "?column?".to_string(),
    }
}

/// Infer the type of a scalar expression evaluated over rows of shape
/// `input`.
///
/// Column references resolve by `index` alone, positionally into `input` —
/// this function does not interpret [`crate::expr::ColumnRef::relation`],
/// because by the time an expression reaches here it has already been placed
/// against the one schema its `relation` refers to (a join's `on`/`filter`
/// combine two schemas and are evaluated before this function is relevant to
/// them; every other node produces or consumes exactly one).
pub fn expr_type(expr: &Expr, input: &Schema) -> Result<PgType, SchemaError> {
    match expr {
        Expr::Column(cr) => {
            input
                .get(cr.index as usize)
                .map(|(_, ty)| *ty)
                .ok_or(SchemaError::ColumnOutOfRange {
                    relation: cr.relation,
                    index: cr.index,
                })
        }

        Expr::Literal(_, ty) => Ok(*ty),
        Expr::Parameter { ty, .. } => Ok(*ty),

        // An operator's result type lives in `pg_operator`, keyed by `OpId`
        // and resolved against its operand types — the same shape of problem
        // as a function call below, and the same answer: no catalog yet, so
        // no guess.
        Expr::Unary { .. } | Expr::Binary { .. } => {
            Err(SchemaError::Unimplemented("operator return type"))
        }

        Expr::Cast { to, .. } => Ok(*to),

        Expr::Case { whens, else_, .. } => {
            // Postgres unifies every WHEN/ELSE branch to one common type
            // during parse analysis (`select_common_type`) before a CASE
            // expression is even valid; lowering is expected to have already
            // inserted whatever casts make that true. So the first branch's
            // type is authoritative — this function does not re-derive the
            // common type itself, it trusts that lowering already settled it.
            if let Some((_, then)) = whens.first() {
                expr_type(then, input)
            } else if let Some(e) = else_ {
                expr_type(e, input)
            } else {
                // Not valid SQL (CASE requires at least one WHEN), so this is
                // a malformed plan rather than a real query shape.
                Err(SchemaError::Unimplemented("CASE with no WHEN and no ELSE"))
            }
        }

        Expr::Coalesce(xs) => match xs.first() {
            // Same assumption as CASE above: lowering already unified every
            // argument's type, so the first is authoritative.
            Some(e) => expr_type(e, input),
            None => Err(SchemaError::Unimplemented("COALESCE with no arguments")),
        },

        // Three-valued-logic tests and membership/pattern predicates are
        // bool-valued regardless of their operands' types.
        Expr::IsNull { .. }
        | Expr::BoolTest { .. }
        | Expr::DistinctFrom { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::Like { .. } => Ok(PgType::BOOL),

        // Function return types — scalar, aggregate, window and
        // set-returning alike — live in `pg_proc`, keyed by `FuncId` and
        // resolved against the call's argument types. There is no catalog
        // yet, so this cannot be more than "not yet"; a later increment
        // threads a catalog lookup through here instead of guessing from the
        // call site.
        Expr::ScalarFn { .. }
        | Expr::Aggregate { .. }
        | Expr::Window { .. }
        | Expr::SetReturning { .. } => Err(SchemaError::Unimplemented("function return type")),

        Expr::Subquery { kind, subplan, .. } => match kind {
            SubqueryKind::Exists
            | SubqueryKind::NotExists
            | SubqueryKind::In
            | SubqueryKind::NotIn
            | SubqueryKind::Any(_)
            | SubqueryKind::All(_) => Ok(PgType::BOOL),
            // `(SELECT ...)` used as a value takes the type of its single
            // output column — parse analysis already rejected any subplan
            // that does not have exactly one, so the first column is the
            // only one that matters here.
            SubqueryKind::Scalar => {
                let sub_schema = output_schema(subplan)?;
                sub_schema
                    .first()
                    .map(|(_, ty)| *ty)
                    .ok_or(SchemaError::Unimplemented(
                        "scalar subquery with no output columns",
                    ))
            }
        },

        Expr::ArrayLit(xs) => {
            let Some(first) = xs.first() else {
                // An empty `ARRAY[]` has no element to infer a type from. In
                // real Postgres this is resolved from context (`::int[]`, an
                // assignment target, ...); with none available the constant
                // folder still has to pick *something* (`text[]`). This
                // function does not have that context, so it does not guess.
                return Err(SchemaError::Unimplemented("empty array literal type"));
            };
            let elem_ty = expr_type(first, input)?;
            match oid::array_of(elem_ty.oid) {
                Some(array_oid) => Ok(PgType::new(array_oid)),
                None => Err(SchemaError::Unimplemented(
                    "array literal of a non-builtin element type",
                )),
            }
        }

        // `ROW(...)` is Postgres's anonymous composite type (`record`).
        // There is no catalog entry to hang field names/types on for the
        // expression's type *alone* — `FieldSelect` below recovers field
        // types by looking directly at a literal `ROW(...)` rather than
        // through this type.
        Expr::RowLit(_) => Ok(PgType::new(oid::RECORD)),

        Expr::Subscript { arg, indices } => {
            let arg_ty = expr_type(arg, input)?;
            // A slice (`a[i:j]`) is still an array of the same type. A plain
            // index subscript peels to the element type, and it does so
            // regardless of how many indices are chained: Postgres does not
            // encode dimensionality in the *type* (a 2-D `int4[]` is still
            // OID `_int4`, per `basin_pgtype::oid`'s own module docs), so
            // `a[1][2]` and `a[1]` resolve to `int4` the same way once every
            // subscript is a plain index.
            let any_slice = indices.iter().any(|s| matches!(s, Subscript::Slice { .. }));
            if any_slice {
                Ok(arg_ty)
            } else if let Some(elem) = oid::element_of(arg_ty.oid) {
                Ok(PgType::new(elem))
            } else if arg_ty.oid == oid::JSONB {
                // jsonb supports subscripting since Postgres 14 and stays
                // jsonb — it is not in `oid::element_of`'s array table because
                // it is not an array type.
                Ok(arg_ty)
            } else {
                Err(SchemaError::Unimplemented(
                    "subscript of a non-array, non-jsonb type",
                ))
            }
        }

        Expr::FieldSelect { arg, field } => match arg.as_ref() {
            // The one composite whose field types are visible without a
            // catalog: a literal `ROW(...)` carries its field expressions
            // right here. Any other composite (a table's row type, a
            // function's OUT parameters) needs `pg_attribute`, which does not
            // exist yet.
            Expr::RowLit(fields) => fields
                .get(*field as usize)
                .ok_or(SchemaError::ColumnOutOfRange {
                    relation: 0,
                    index: *field,
                })
                .and_then(|e| expr_type(e, input)),
            _ => Err(SchemaError::Unimplemented(
                "composite field type (needs the catalog)",
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{ColumnRef, Datum};
    use crate::{
        CteId, FuncId, GroupingSets, JoinKind, OnConflict, SetOpKind, SnapshotId, TableId,
    };
    use basin_pgtype::oid;

    fn col(index: u16, name: &str, ty: PgType) -> Expr {
        // A column whose *declared* type does not matter for resolution —
        // `expr_type` reads the type out of the schema at `index`, never off
        // the `ColumnRef` itself. `ty` here is only used to build schemas for
        // fixtures below.
        let _ = ty;
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: name.to_string(),
        })
    }

    fn lit_int(v: i64) -> Expr {
        Expr::Literal(Datum::Int64(v), PgType::INT8)
    }

    fn schema(cols: &[(&str, PgType)]) -> Schema {
        cols.iter().map(|(n, t)| (n.to_string(), *t)).collect()
    }

    fn values(cols: &[(&str, PgType)]) -> LogicalPlan {
        LogicalPlan::Values {
            rows: vec![],
            schema: schema(cols),
        }
    }

    // ─── passthrough leaves ─────────────────────────────────────────────

    #[test]
    fn values_empty_and_cteref_report_their_own_schema() {
        for plan in [
            LogicalPlan::Values {
                rows: vec![],
                schema: schema(&[("a", PgType::INT4)]),
            },
            LogicalPlan::Empty {
                produce_one_row: true,
                schema: schema(&[("a", PgType::INT4)]),
            },
            LogicalPlan::CteRef {
                name: CteId(0),
                schema: schema(&[("a", PgType::INT4)]),
            },
        ] {
            assert_eq!(
                output_schema(&plan).unwrap(),
                schema(&[("a", PgType::INT4)])
            );
        }
    }

    /// A Scan cannot report a schema without a catalog to resolve its
    /// `ColId`s against — this is an honest gap, not a missing match arm.
    #[test]
    fn scan_is_unimplemented_without_a_catalog() {
        let plan = LogicalPlan::Scan {
            table: TableId(1),
            projection: vec![],
            filters: vec![],
            snapshot: SnapshotId(0),
        };
        assert!(matches!(
            output_schema(&plan),
            Err(SchemaError::Unimplemented(_))
        ));
    }

    // ─── Project / Filter / Sort / Limit / Distinct ────────────────────

    #[test]
    fn project_takes_types_from_exprs_and_names_from_aliases() {
        let input = values(&[("a", PgType::INT4), ("b", PgType::TEXT)]);
        let plan = LogicalPlan::Project {
            input: Box::new(input),
            exprs: vec![
                (col(1, "b", PgType::TEXT), "renamed".to_string()),
                (
                    Expr::Cast {
                        arg: Box::new(col(0, "a", PgType::INT4)),
                        to: PgType::INT8,
                        kind: basin_pgtype::cast::CastKind::Implicit,
                    },
                    "widened".to_string(),
                ),
            ],
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("renamed", PgType::TEXT), ("widened", PgType::INT8)])
        );
    }

    #[test]
    fn project_propagates_a_column_out_of_range() {
        let input = values(&[("a", PgType::INT4)]);
        let plan = LogicalPlan::Project {
            input: Box::new(input),
            exprs: vec![(col(5, "nope", PgType::INT4), "x".to_string())],
        };
        assert_eq!(
            output_schema(&plan),
            Err(SchemaError::ColumnOutOfRange {
                relation: 0,
                index: 5
            })
        );
    }

    #[test]
    fn filter_sort_limit_and_distinct_pass_the_input_schema_through_unchanged() {
        let base = schema(&[("a", PgType::INT4), ("b", PgType::TEXT)]);
        let input = || {
            Box::new(LogicalPlan::Values {
                rows: vec![],
                schema: base.clone(),
            })
        };

        let filter = LogicalPlan::Filter {
            input: input(),
            predicate: Expr::Literal(Datum::Bool(true), PgType::BOOL),
        };
        let sort = LogicalPlan::Sort {
            input: input(),
            keys: vec![],
        };
        let limit = LogicalPlan::Limit {
            input: input(),
            skip: None,
            fetch: None,
            with_ties: false,
        };
        let distinct = LogicalPlan::Distinct {
            input: input(),
            on: None,
        };

        for plan in [filter, sort, limit, distinct] {
            assert_eq!(output_schema(&plan).unwrap(), base);
        }
    }

    // ─── Aggregate ──────────────────────────────────────────────────────

    #[test]
    fn aggregate_orders_group_columns_before_aggs() {
        let input = values(&[("k", PgType::INT4), ("v", PgType::INT8)]);
        let plan = LogicalPlan::Aggregate {
            input: Box::new(input),
            group: vec![col(0, "k", PgType::INT4)],
            // Standing in for a real aggregate call without needing the
            // catalog: what matters here is ordering, not the value.
            aggs: vec![col(1, "v", PgType::INT8)],
            grouping_sets: None,
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("k", PgType::INT4), ("v", PgType::INT8)])
        );
    }

    #[test]
    fn a_real_aggregate_call_is_unimplemented_pending_the_catalog() {
        let input = values(&[("v", PgType::INT8)]);
        let plan = LogicalPlan::Aggregate {
            input: Box::new(input),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: FuncId(oid::Oid(2108)), // sum(int8)
                args: vec![col(0, "v", PgType::INT8)],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        };
        assert_eq!(
            output_schema(&plan),
            Err(SchemaError::Unimplemented("function return type"))
        );
    }

    #[test]
    fn grouping_sets_do_not_change_the_output_schema_shape() {
        // GroupingSets only says which combinations of `group` are evaluated
        // at runtime; the *columns* are still exactly `group` — the schema
        // must not depend on which grouping set is present.
        let input = values(&[("k", PgType::INT4)]);
        let plan = LogicalPlan::Aggregate {
            input: Box::new(input),
            group: vec![col(0, "k", PgType::INT4)],
            aggs: vec![],
            grouping_sets: Some(GroupingSets(vec![vec![0], vec![]])),
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("k", PgType::INT4)])
        );
    }

    // ─── Join / LateralJoin — the semi/anti bug ─────────────────────────

    fn join_inputs() -> (LogicalPlan, LogicalPlan) {
        (
            values(&[("a", PgType::INT4), ("b", PgType::TEXT)]),
            values(&[("c", PgType::BOOL)]),
        )
    }

    #[test]
    fn inner_left_right_full_and_cross_joins_concatenate_both_sides() {
        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
            JoinKind::Cross,
        ] {
            let (left, right) = join_inputs();
            let plan = LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                on: vec![],
                filter: None,
            };
            assert_eq!(
                output_schema(&plan).unwrap(),
                schema(&[
                    ("a", PgType::INT4),
                    ("b", PgType::TEXT),
                    ("c", PgType::BOOL)
                ]),
                "{kind:?} should widen to left ++ right"
            );
        }
    }

    /// The bug this task exists to catch: a semi/anti join filters rows of
    /// the left side, it does not add the right side's columns. A join
    /// schema that (wrongly) reports left ++ right here would claim a column
    /// `c` that no row of a `WHERE EXISTS (...)` plan actually produces.
    #[test]
    fn semi_and_anti_joins_report_only_the_left_schema() {
        for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
            let (left, right) = join_inputs();
            let plan = LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind,
                on: vec![],
                filter: None,
            };
            assert_eq!(
                output_schema(&plan).unwrap(),
                schema(&[("a", PgType::INT4), ("b", PgType::TEXT)]),
                "{kind:?} must not widen the schema with the right side's columns"
            );
        }
    }

    #[test]
    fn lateral_join_follows_the_same_semi_anti_rule_as_join() {
        let (outer, inner) = join_inputs();
        let widens = LogicalPlan::LateralJoin {
            outer: Box::new(outer),
            inner: Box::new(inner),
            kind: JoinKind::Inner,
        };
        assert_eq!(
            output_schema(&widens).unwrap(),
            schema(&[
                ("a", PgType::INT4),
                ("b", PgType::TEXT),
                ("c", PgType::BOOL)
            ])
        );

        let (outer, inner) = join_inputs();
        let narrows = LogicalPlan::LateralJoin {
            outer: Box::new(outer),
            inner: Box::new(inner),
            kind: JoinKind::LeftAnti,
        };
        assert_eq!(
            output_schema(&narrows).unwrap(),
            schema(&[("a", PgType::INT4), ("b", PgType::TEXT)])
        );
    }

    // ─── SetOp ──────────────────────────────────────────────────────────

    #[test]
    fn set_op_takes_column_names_from_the_left_side() {
        let left = values(&[("left_name", PgType::INT4)]);
        let right = values(&[("right_name", PgType::INT4)]);
        let plan = LogicalPlan::SetOp {
            left: Box::new(left),
            right: Box::new(right),
            op: SetOpKind::Union,
            all: false,
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("left_name", PgType::INT4)]),
            "UNION must report the left side's column name, per Postgres"
        );
    }

    #[test]
    fn set_op_with_mismatched_arity_is_an_error() {
        for op in [SetOpKind::Union, SetOpKind::Intersect, SetOpKind::Except] {
            let plan = LogicalPlan::SetOp {
                left: Box::new(LogicalPlan::Values {
                    rows: vec![],
                    schema: schema(&[("a", PgType::INT4), ("b", PgType::INT4)]),
                }),
                right: Box::new(LogicalPlan::Values {
                    rows: vec![],
                    schema: schema(&[("c", PgType::INT4)]),
                }),
                op,
                all: true,
            };
            assert_eq!(output_schema(&plan), Err(SchemaError::SetOpMismatch));
        }
        // Sanity: matching arity really does succeed (the mismatch above
        // isn't accidentally always erroring).
        let plan = LogicalPlan::SetOp {
            left: Box::new(values(&[("a", PgType::INT4), ("b", PgType::INT4)])),
            right: Box::new(values(&[("c", PgType::INT4), ("d", PgType::INT4)])),
            op: SetOpKind::Union,
            all: true,
        };
        let _ = output_schema(&plan).unwrap();
    }

    // ─── Window / ProjectSet ────────────────────────────────────────────

    #[test]
    fn window_with_no_window_exprs_reports_the_input_schema() {
        let input = values(&[("a", PgType::INT4)]);
        let plan = LogicalPlan::Window {
            input: Box::new(input),
            windows: vec![],
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("a", PgType::INT4)])
        );
    }

    #[test]
    fn window_expr_return_type_is_unimplemented_pending_the_catalog() {
        let input = values(&[("a", PgType::INT4)]);
        let plan = LogicalPlan::Window {
            input: Box::new(input),
            windows: vec![Expr::Window {
                func: FuncId(oid::Oid(3100)), // row_number()
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                frame: crate::WindowFrame {
                    units: crate::FrameUnits::Rows,
                    start: crate::FrameBound::UnboundedPreceding,
                    end: crate::FrameBound::CurrentRow,
                },
            }],
        };
        assert_eq!(
            output_schema(&plan),
            Err(SchemaError::Unimplemented("function return type"))
        );
    }

    #[test]
    fn project_set_appends_srf_columns_after_the_input_schema() {
        // The input schema survives; an empty SRF list is the "plus zero"
        // sanity check that the input side of the concatenation is right.
        let input = values(&[("a", PgType::INT4)]);
        let plan = LogicalPlan::ProjectSet {
            input: Box::new(input),
            srfs: vec![],
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("a", PgType::INT4)])
        );
    }

    #[test]
    fn project_set_srf_return_type_is_unimplemented_pending_the_catalog() {
        let input = values(&[("a", PgType::INT4)]);
        let plan = LogicalPlan::ProjectSet {
            input: Box::new(input),
            srfs: vec![Expr::SetReturning {
                func: FuncId(oid::Oid(1066)), // generate_series
                args: vec![lit_int(1), lit_int(10)],
            }],
        };
        assert_eq!(
            output_schema(&plan),
            Err(SchemaError::Unimplemented("function return type"))
        );
    }

    // ─── Cte ────────────────────────────────────────────────────────────

    #[test]
    fn cte_reports_the_schema_of_the_query_that_uses_it_not_the_body() {
        let plan = LogicalPlan::Cte {
            name: CteId(0),
            recursive: false,
            body: Box::new(values(&[("body_col", PgType::INT4)])),
            input: Box::new(LogicalPlan::CteRef {
                name: CteId(0),
                schema: schema(&[("used_col", PgType::TEXT)]),
            }),
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("used_col", PgType::TEXT)])
        );
    }

    // ─── Insert / Update / Delete ───────────────────────────────────────

    #[test]
    fn insert_without_returning_has_an_empty_schema() {
        let plan = LogicalPlan::Insert {
            table: TableId(1),
            input: Box::new(values(&[("a", PgType::INT4)])),
            columns: vec![],
            on_conflict: None,
            returning: None,
        };
        assert_eq!(output_schema(&plan).unwrap(), Schema::new());
    }

    #[test]
    fn insert_returning_resolves_against_its_own_input_schema() {
        let plan = LogicalPlan::Insert {
            table: TableId(1),
            input: Box::new(values(&[("id", PgType::INT8)])),
            columns: vec![],
            on_conflict: Some(OnConflict::DoNothing { target: vec![] }),
            returning: Some(vec![(col(0, "id", PgType::INT8), "id".to_string())]),
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("id", PgType::INT8)])
        );
    }

    #[test]
    fn update_without_returning_has_an_empty_schema() {
        let plan = LogicalPlan::Update {
            table: TableId(1),
            set: vec![],
            from: None,
            predicate: None,
            returning: None,
            snapshot: SnapshotId(0),
        };
        assert_eq!(output_schema(&plan).unwrap(), Schema::new());
    }

    #[test]
    fn update_returning_a_literal_needs_no_table_schema() {
        // RETURNING that never references a column works even though Update
        // carries no representation of the target table's row shape.
        let plan = LogicalPlan::Update {
            table: TableId(1),
            set: vec![],
            from: None,
            predicate: None,
            returning: Some(vec![(lit_int(1), "one".to_string())]),
            snapshot: SnapshotId(0),
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("one", PgType::INT8)])
        );
    }

    #[test]
    fn update_returning_a_bare_table_column_without_from_is_out_of_range() {
        // Documents the known limitation: without a catalog, Update has no
        // schema to resolve a target-table column reference against.
        let plan = LogicalPlan::Update {
            table: TableId(1),
            set: vec![],
            from: None,
            predicate: None,
            returning: Some(vec![(col(0, "id", PgType::INT8), "id".to_string())]),
            snapshot: SnapshotId(0),
        };
        assert_eq!(
            output_schema(&plan),
            Err(SchemaError::ColumnOutOfRange {
                relation: 0,
                index: 0
            })
        );
    }

    #[test]
    fn update_returning_resolves_columns_from_the_from_clause() {
        let plan = LogicalPlan::Update {
            table: TableId(1),
            set: vec![],
            from: Some(Box::new(values(&[("other", PgType::TEXT)]))),
            predicate: None,
            returning: Some(vec![(col(0, "other", PgType::TEXT), "other".to_string())]),
            snapshot: SnapshotId(0),
        };
        assert_eq!(
            output_schema(&plan).unwrap(),
            schema(&[("other", PgType::TEXT)])
        );
    }

    #[test]
    fn delete_without_returning_has_an_empty_schema_and_using_resolves_returning() {
        let no_returning = LogicalPlan::Delete {
            table: TableId(1),
            using: None,
            predicate: None,
            returning: None,
            snapshot: SnapshotId(0),
        };
        assert_eq!(output_schema(&no_returning).unwrap(), Schema::new());

        let with_using = LogicalPlan::Delete {
            table: TableId(1),
            using: Some(Box::new(values(&[("other", PgType::BOOL)]))),
            predicate: None,
            returning: Some(vec![(col(0, "other", PgType::BOOL), "other".to_string())]),
            snapshot: SnapshotId(0),
        };
        assert_eq!(
            output_schema(&with_using).unwrap(),
            schema(&[("other", PgType::BOOL)])
        );
    }

    // ─── expr_type ──────────────────────────────────────────────────────

    #[test]
    fn expr_type_resolves_columns_by_index_and_rejects_out_of_range() {
        let s = schema(&[("a", PgType::INT4), ("b", PgType::TEXT)]);
        assert_eq!(
            expr_type(&col(1, "b", PgType::TEXT), &s).unwrap(),
            PgType::TEXT
        );
        assert_eq!(
            expr_type(&col(9, "z", PgType::TEXT), &s),
            Err(SchemaError::ColumnOutOfRange {
                relation: 0,
                index: 9
            })
        );
    }

    #[test]
    fn expr_type_literal_and_parameter_carry_their_own_type() {
        let s = Schema::new();
        assert_eq!(expr_type(&lit_int(1), &s).unwrap(), PgType::INT8);
        assert_eq!(
            expr_type(
                &Expr::Parameter {
                    index: 1,
                    ty: PgType::BOOL
                },
                &s
            )
            .unwrap(),
            PgType::BOOL
        );
    }

    #[test]
    fn expr_type_cast_yields_its_target_type() {
        let s = schema(&[("a", PgType::INT4)]);
        let e = Expr::Cast {
            arg: Box::new(col(0, "a", PgType::INT4)),
            to: PgType::TEXT,
            kind: basin_pgtype::cast::CastKind::Assignment,
        };
        assert_eq!(expr_type(&e, &s).unwrap(), PgType::TEXT);
    }

    #[test]
    fn expr_type_boolean_tests_are_always_bool() {
        let s = schema(&[("a", PgType::INT4)]);
        let a = || Box::new(col(0, "a", PgType::INT4));
        let exprs = [
            Expr::IsNull {
                arg: a(),
                negated: false,
            },
            Expr::BoolTest {
                arg: a(),
                test: crate::BoolTest::IsTrue,
            },
            Expr::DistinctFrom {
                lhs: a(),
                rhs: a(),
                negated: false,
            },
            Expr::InList {
                arg: a(),
                list: vec![lit_int(1)],
                negated: false,
            },
            Expr::Between {
                arg: a(),
                low: Box::new(lit_int(0)),
                high: Box::new(lit_int(9)),
                symmetric: false,
                negated: false,
            },
            Expr::Like {
                arg: a(),
                pattern: Box::new(Expr::Literal(Datum::Utf8("%x%".into()), PgType::TEXT)),
                escape: None,
                case_insensitive: false,
                negated: false,
            },
        ];
        for e in exprs {
            assert_eq!(expr_type(&e, &s).unwrap(), PgType::BOOL);
        }
    }

    #[test]
    fn expr_type_case_and_coalesce_take_the_first_branchs_type() {
        let s = Schema::new();
        let case = Expr::Case {
            operand: None,
            whens: vec![(Expr::Literal(Datum::Bool(true), PgType::BOOL), lit_int(1))],
            else_: Some(Box::new(lit_int(2))),
        };
        assert_eq!(expr_type(&case, &s).unwrap(), PgType::INT8);

        let case_else_only = Expr::Case {
            operand: None,
            whens: vec![],
            else_: Some(Box::new(Expr::Literal(
                Datum::Utf8("x".into()),
                PgType::TEXT,
            ))),
        };
        assert_eq!(expr_type(&case_else_only, &s).unwrap(), PgType::TEXT);

        let coalesce = Expr::Coalesce(vec![lit_int(1), lit_int(2)]);
        assert_eq!(expr_type(&coalesce, &s).unwrap(), PgType::INT8);

        assert_eq!(
            expr_type(&Expr::Coalesce(vec![]), &s),
            Err(SchemaError::Unimplemented("COALESCE with no arguments"))
        );
    }

    #[test]
    fn expr_type_function_shapes_are_unimplemented_pending_the_catalog() {
        let s = Schema::new();
        let scalar_fn = Expr::ScalarFn {
            func: FuncId(oid::Oid(1)),
            args: vec![],
        };
        let agg = Expr::Aggregate {
            func: FuncId(oid::Oid(2108)),
            args: vec![],
            distinct: false,
            filter: None,
            order_by: vec![],
        };
        assert_eq!(
            expr_type(&scalar_fn, &s),
            Err(SchemaError::Unimplemented("function return type"))
        );
        assert_eq!(
            expr_type(&agg, &s),
            Err(SchemaError::Unimplemented("function return type"))
        );
    }

    #[test]
    fn expr_type_subquery_kinds_other_than_scalar_are_bool() {
        let s = Schema::new();
        let subplan = || Box::new(values(&[("x", PgType::INT4)]));
        for kind in [
            SubqueryKind::Exists,
            SubqueryKind::NotExists,
            SubqueryKind::In,
            SubqueryKind::NotIn,
            SubqueryKind::Any(crate::OpId(oid::Oid(96))),
            SubqueryKind::All(crate::OpId(oid::Oid(96))),
        ] {
            let e = Expr::Subquery {
                kind,
                subplan: subplan(),
                operand: None,
            };
            assert_eq!(expr_type(&e, &s).unwrap(), PgType::BOOL);
        }
    }

    #[test]
    fn expr_type_scalar_subquery_takes_its_single_columns_type() {
        let s = Schema::new();
        let e = Expr::Subquery {
            kind: SubqueryKind::Scalar,
            subplan: Box::new(values(&[("x", PgType::INT4)])),
            operand: None,
        };
        assert_eq!(expr_type(&e, &s).unwrap(), PgType::INT4);
    }

    #[test]
    fn expr_type_array_literal_takes_the_array_of_the_first_elements_type() {
        let s = Schema::new();
        let e = Expr::ArrayLit(vec![lit_int(1), lit_int(2)]);
        assert_eq!(expr_type(&e, &s).unwrap(), PgType::new(oid::INT8_ARRAY));

        assert_eq!(
            expr_type(&Expr::ArrayLit(vec![]), &s),
            Err(SchemaError::Unimplemented("empty array literal type"))
        );
    }

    #[test]
    fn expr_type_row_literal_is_record() {
        let s = Schema::new();
        let e = Expr::RowLit(vec![
            lit_int(1),
            Expr::Literal(Datum::Utf8("x".into()), PgType::TEXT),
        ]);
        assert_eq!(expr_type(&e, &s).unwrap(), PgType::new(oid::RECORD));
    }

    #[test]
    fn expr_type_field_select_reads_a_row_literals_field_type() {
        let s = Schema::new();
        let row = Expr::RowLit(vec![
            lit_int(1),
            Expr::Literal(Datum::Utf8("x".into()), PgType::TEXT),
        ]);
        let select_1 = Expr::FieldSelect {
            arg: Box::new(row.clone()),
            field: 1,
        };
        assert_eq!(expr_type(&select_1, &s).unwrap(), PgType::TEXT);

        let out_of_range = Expr::FieldSelect {
            arg: Box::new(row),
            field: 5,
        };
        assert_eq!(
            expr_type(&out_of_range, &s),
            Err(SchemaError::ColumnOutOfRange {
                relation: 0,
                index: 5
            })
        );
    }

    #[test]
    fn expr_type_field_select_on_a_non_literal_composite_is_unimplemented() {
        let s = schema(&[("composite_col", PgType::new(oid::RECORD))]);
        let e = Expr::FieldSelect {
            arg: Box::new(col(0, "composite_col", PgType::new(oid::RECORD))),
            field: 0,
        };
        assert_eq!(
            expr_type(&e, &s),
            Err(SchemaError::Unimplemented(
                "composite field type (needs the catalog)"
            ))
        );
    }

    #[test]
    fn expr_type_subscript_index_peels_to_the_element_type_and_slice_keeps_the_array_type() {
        let s = schema(&[("arr", PgType::new(oid::INT4_ARRAY))]);
        let index = Expr::Subscript {
            arg: Box::new(col(0, "arr", PgType::new(oid::INT4_ARRAY))),
            indices: vec![Subscript::Index(lit_int(1))],
        };
        assert_eq!(expr_type(&index, &s).unwrap(), PgType::INT4);

        let slice = Expr::Subscript {
            arg: Box::new(col(0, "arr", PgType::new(oid::INT4_ARRAY))),
            indices: vec![Subscript::Slice {
                lower: Some(lit_int(1)),
                upper: Some(lit_int(2)),
            }],
        };
        assert_eq!(expr_type(&slice, &s).unwrap(), PgType::new(oid::INT4_ARRAY));

        // Multiple chained index subscripts still just peel one level, since
        // Postgres does not encode array dimensionality in the type.
        let double_index = Expr::Subscript {
            arg: Box::new(col(0, "arr", PgType::new(oid::INT4_ARRAY))),
            indices: vec![Subscript::Index(lit_int(1)), Subscript::Index(lit_int(2))],
        };
        assert_eq!(expr_type(&double_index, &s).unwrap(), PgType::INT4);
    }

    #[test]
    fn expr_type_subscript_of_jsonb_stays_jsonb() {
        let s = schema(&[("j", PgType::JSONB)]);
        let e = Expr::Subscript {
            arg: Box::new(col(0, "j", PgType::JSONB)),
            indices: vec![Subscript::Index(Expr::Literal(
                Datum::Utf8("key".into()),
                PgType::TEXT,
            ))],
        };
        assert_eq!(expr_type(&e, &s).unwrap(), PgType::JSONB);
    }

    #[test]
    fn expr_type_subscript_of_a_non_array_non_jsonb_type_is_unimplemented() {
        let s = schema(&[("n", PgType::INT4)]);
        let e = Expr::Subscript {
            arg: Box::new(col(0, "n", PgType::INT4)),
            indices: vec![Subscript::Index(lit_int(0))],
        };
        assert_eq!(
            expr_type(&e, &s),
            Err(SchemaError::Unimplemented(
                "subscript of a non-array, non-jsonb type"
            ))
        );
    }

    #[test]
    fn expr_type_unary_and_binary_operators_are_unimplemented_pending_the_catalog() {
        let s = schema(&[("a", PgType::INT4)]);
        let unary = Expr::Unary {
            op: crate::OpId(oid::Oid(97)), // NOT
            arg: Box::new(col(0, "a", PgType::INT4)),
        };
        let binary = Expr::Binary {
            op: crate::OpId(oid::Oid(551)), // int4 + int4
            lhs: Box::new(col(0, "a", PgType::INT4)),
            rhs: Box::new(lit_int(1)),
        };
        assert_eq!(
            expr_type(&unary, &s),
            Err(SchemaError::Unimplemented("operator return type"))
        );
        assert_eq!(
            expr_type(&binary, &s),
            Err(SchemaError::Unimplemented("operator return type"))
        );
    }

    #[test]
    fn display_name_prefers_column_and_cast_names_over_column_underscore() {
        assert_eq!(display_name(&col(0, "a", PgType::INT4)), "a");
        let cast = Expr::Cast {
            arg: Box::new(col(0, "a", PgType::INT4)),
            to: PgType::INT8,
            kind: basin_pgtype::cast::CastKind::Implicit,
        };
        assert_eq!(display_name(&cast), "a");
        assert_eq!(display_name(&lit_int(1)), "?column?");
    }
}
