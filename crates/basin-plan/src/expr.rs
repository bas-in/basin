//! Scalar expressions.
//!
//! Scoped from the **SQL surface Basin commits to supporting**, not from a
//! survey of what the current engine happens to construct. That distinction is
//! load-bearing: every production query today reaches the planner through
//! `SessionContext::sql(&str)` (38 call sites, zero programmatic plan
//! construction — see `docs/migration/df-removal/02-logical-ir-surface.md`), so
//! any expression form reachable from SQL text is reachable, whether or not
//! Basin names it in Rust.

use basin_pgtype::{Oid, PgType};

/// A reference to a function or operator in `pg_proc` / `pg_operator`.
///
/// Deliberately an OID rather than a name or an enum variant. That is what
/// makes user-defined operators, `CREATE EXTENSION`-registered functions and
/// builtins all resolve through one path — and it is what replaces the
/// 9,546-line string-rewriting layer in `pg_operators.rs`, where `~` and `@>`
/// are matched textually because there is no catalog to look them up in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FuncId(pub Oid);

/// A reference to an operator in `pg_operator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpId(pub Oid);

/// A column reference, resolved to a position in the input's schema.
///
/// Resolution happens during lowering, so by the time a plan exists there are
/// no unresolved names. `relation` distinguishes same-named columns from
/// different inputs of a join.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Index of the input relation this column comes from — 0 for a single
    /// input, 0/1 for the sides of a join.
    pub relation: u16,
    /// Ordinal position within that relation's output schema.
    pub index: u16,
    /// The column's name, carried for error messages and `RowDescription`.
    /// Never used for resolution — that already happened.
    pub name: String,
}

/// A literal value.
///
/// Not an owned Arrow scalar: a literal in a *plan* is a small immediate, and
/// forcing it through Arrow's scalar machinery would drag array allocation into
/// planning. Bytes are the escape hatch for everything wider (numeric, uuid,
/// jsonb, arrays) and are interpreted according to the accompanying [`PgType`].
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    /// Text in the widest sense — `text`, `varchar`, `name`, `char`.
    Utf8(String),
    /// Everything whose in-memory form is bytes: `bytea`, `uuid`, binary
    /// `jsonb` (ADR 0027), `numeric` beyond an i64, array literals.
    Bytes(Vec<u8>),
}

/// Which of Postgres's boolean tests is being applied.
///
/// These are distinct from `= TRUE` because they are defined on three-valued
/// logic: `NULL IS NOT TRUE` is `true`, whereas `NULL = TRUE` is `NULL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BoolTest {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    IsUnknown,
    IsNotUnknown,
}

/// How a subquery is being used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubqueryKind {
    /// `(SELECT …)` yielding one row and one column.
    Scalar,
    /// `EXISTS (SELECT …)`.
    Exists,
    /// `NOT EXISTS (SELECT …)`.
    NotExists,
    /// `x IN (SELECT …)`.
    In,
    /// `x NOT IN (SELECT …)`.
    ///
    /// Kept distinct from `NOT (x IN …)` on purpose. Under three-valued logic
    /// they are not interchangeable: if the subquery yields any NULL, `NOT IN`
    /// is NULL rather than true, which is the single most commonly mis-handled
    /// corner of SQL semantics.
    NotIn,
    /// `x op ANY (SELECT …)` / `x op SOME (SELECT …)`.
    Any(OpId),
    /// `x op ALL (SELECT …)`.
    All(OpId),
}

/// Sort direction and null placement for one key.
#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub descending: bool,
    /// Postgres's default depends on direction: `NULLS LAST` for `ASC`,
    /// `NULLS FIRST` for `DESC`. Lowering resolves the default, so this is
    /// always explicit by the time it reaches a plan.
    pub nulls_first: bool,
}

/// A window frame.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub units: FrameUnits,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameUnits {
    Rows,
    Range,
    /// `GROUPS` counts peer groups rather than rows. Basin's window tests
    /// already exercise `GROUPS BETWEEN 1 PRECEDING`, so this is not
    /// speculative surface.
    Groups,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    /// `n PRECEDING`. For `RANGE` this is an offset in the ORDER BY column's
    /// own units — including an `INTERVAL` against a timestamp, which the
    /// existing tests cover.
    Preceding(Box<Expr>),
    CurrentRow,
    Following(Box<Expr>),
    UnboundedFollowing,
}

/// A scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(ColumnRef),
    Literal(Datum, PgType),

    /// A bind parameter, `$1`.
    ///
    /// Retained even though the current engine substitutes parameters at the
    /// AST level before planning. Correct extended-query support requires
    /// `Describe` to report *inferred parameter types* before any value
    /// arrives, which is impossible if parameters have already been replaced by
    /// literals.
    Parameter {
        index: u16,
        ty: PgType,
    },

    Unary {
        op: OpId,
        arg: Box<Expr>,
    },
    Binary {
        op: OpId,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },

    /// An explicit or implicit cast. `kind` records which of Postgres's three
    /// contexts licensed it, which matters for error messages and for
    /// `EXPLAIN` output.
    Cast {
        arg: Box<Expr>,
        to: PgType,
        kind: basin_pgtype::cast::CastKind,
    },

    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
    Coalesce(Vec<Expr>),

    IsNull {
        arg: Box<Expr>,
        negated: bool,
    },
    BoolTest {
        arg: Box<Expr>,
        test: BoolTest,
    },
    /// `a IS DISTINCT FROM b`. Null-safe equality — never returns NULL.
    DistinctFrom {
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        negated: bool,
    },

    InList {
        arg: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Between {
        arg: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        symmetric: bool,
        negated: bool,
    },
    Like {
        arg: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        case_insensitive: bool,
        negated: bool,
    },

    ScalarFn {
        func: FuncId,
        args: Vec<Expr>,
    },

    Aggregate {
        func: FuncId,
        args: Vec<Expr>,
        distinct: bool,
        /// `FILTER (WHERE …)`.
        filter: Option<Box<Expr>>,
        /// `ORDER BY` inside the aggregate, as `array_agg(x ORDER BY y)` and
        /// `percentile_cont(…) WITHIN GROUP (ORDER BY …)` require.
        order_by: Vec<SortKey>,
    },

    Window {
        func: FuncId,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<SortKey>,
        frame: WindowFrame,
    },

    /// A set-returning function in the target list — `generate_series`,
    /// `unnest`, `jsonb_array_elements`.
    ///
    /// This variant is the reason the IR exists in this shape.
    /// `jsonb_udf.rs:16` records that these "cannot be true SRFs inside
    /// DataFusion", so today they are approximated. Multiple SRFs in one target
    /// list expand to the LCM of their lengths, which only works if the planner
    /// can see them as SRFs rather than as ordinary functions.
    SetReturning {
        func: FuncId,
        args: Vec<Expr>,
    },

    /// A subquery. Boxed through `Box<super::LogicalPlan>` so `Expr` and
    /// `LogicalPlan` can be mutually recursive without either owning the other.
    Subquery {
        kind: SubqueryKind,
        subplan: Box<crate::LogicalPlan>,
        /// The expression on the left of `IN` / `ANY` / `ALL`. `None` for
        /// `Scalar` and `Exists`.
        operand: Option<Box<Expr>>,
    },

    /// `ARRAY[…]`.
    ArrayLit(Vec<Expr>),
    /// `ROW(…)`.
    RowLit(Vec<Expr>),

    /// `a[i]` or `a[i:j]`. Postgres subscripts are **1-based**, and a slice is
    /// not the same node as an element access.
    Subscript {
        arg: Box<Expr>,
        indices: Vec<Subscript>,
    },

    /// `(composite).field`.
    FieldSelect {
        arg: Box<Expr>,
        field: u16,
    },
}

/// One subscript position — either a single index or a slice.
#[derive(Debug, Clone, PartialEq)]
pub enum Subscript {
    Index(Expr),
    Slice {
        lower: Option<Expr>,
        upper: Option<Expr>,
    },
}

impl Expr {
    /// A `NULL` literal of unknown type — what a bare `NULL` in SQL starts as
    /// before context resolves it.
    pub fn null_unknown() -> Self {
        Expr::Literal(Datum::Null, PgType::UNKNOWN)
    }

    /// Whether this expression contains an aggregate anywhere beneath it.
    /// Used during lowering to reject aggregates in `WHERE`, which Postgres
    /// disallows.
    pub fn contains_aggregate(&self) -> bool {
        self.any(&mut |e| matches!(e, Expr::Aggregate { .. }))
    }

    /// Whether this expression contains a set-returning function.
    pub fn contains_srf(&self) -> bool {
        self.any(&mut |e| matches!(e, Expr::SetReturning { .. }))
    }

    /// Depth-first search for any node satisfying `pred`, including `self`.
    ///
    /// Deliberately does NOT descend into [`Expr::Subquery`]: a subquery is a
    /// separate query level, and an aggregate inside one does not make the
    /// outer expression aggregated. Getting this wrong is how a planner starts
    /// rejecting valid SQL.
    pub fn any(&self, pred: &mut impl FnMut(&Expr) -> bool) -> bool {
        if pred(self) {
            return true;
        }
        let mut found = false;
        self.for_each_child(&mut |c| {
            if !found && c.any(pred) {
                found = true;
            }
        });
        found
    }

    /// Apply `f` to each immediate child expression.
    ///
    /// Subquery *plans* are not traversed; the operand of an `IN`/`ANY` is,
    /// because it belongs to this query level.
    pub fn for_each_child(&self, f: &mut impl FnMut(&Expr)) {
        match self {
            Expr::Column(_) | Expr::Literal(..) | Expr::Parameter { .. } => {}
            Expr::Unary { arg, .. }
            | Expr::Cast { arg, .. }
            | Expr::IsNull { arg, .. }
            | Expr::BoolTest { arg, .. }
            | Expr::FieldSelect { arg, .. } => f(arg),
            Expr::Binary { lhs, rhs, .. } | Expr::DistinctFrom { lhs, rhs, .. } => {
                f(lhs);
                f(rhs);
            }
            Expr::Case {
                operand,
                whens,
                else_,
            } => {
                if let Some(o) = operand {
                    f(o);
                }
                for (w, t) in whens {
                    f(w);
                    f(t);
                }
                if let Some(e) = else_ {
                    f(e);
                }
            }
            Expr::Coalesce(xs) | Expr::ArrayLit(xs) | Expr::RowLit(xs) => xs.iter().for_each(f),
            Expr::InList { arg, list, .. } => {
                f(arg);
                list.iter().for_each(f);
            }
            Expr::Between { arg, low, high, .. } => {
                f(arg);
                f(low);
                f(high);
            }
            Expr::Like {
                arg,
                pattern,
                escape,
                ..
            } => {
                f(arg);
                f(pattern);
                if let Some(e) = escape {
                    f(e);
                }
            }
            Expr::ScalarFn { args, .. } | Expr::SetReturning { args, .. } => {
                args.iter().for_each(f)
            }
            Expr::Aggregate {
                args,
                filter,
                order_by,
                ..
            } => {
                args.iter().for_each(&mut *f);
                if let Some(x) = filter {
                    f(x);
                }
                for k in order_by {
                    f(&k.expr);
                }
            }
            Expr::Window {
                args,
                partition_by,
                order_by,
                frame,
                ..
            } => {
                args.iter().for_each(&mut *f);
                partition_by.iter().for_each(&mut *f);
                for k in order_by {
                    f(&k.expr);
                }
                for b in [&frame.start, &frame.end] {
                    if let FrameBound::Preceding(e) | FrameBound::Following(e) = b {
                        f(e);
                    }
                }
            }
            Expr::Subquery { operand, .. } => {
                if let Some(o) = operand {
                    f(o);
                }
            }
            Expr::Subscript { arg, indices } => {
                f(arg);
                for i in indices {
                    match i {
                        Subscript::Index(e) => f(e),
                        Subscript::Slice { lower, upper } => {
                            if let Some(l) = lower {
                                f(l);
                            }
                            if let Some(u) = upper {
                                f(u);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_pgtype::oid;

    fn col(i: u16) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index: i,
            name: format!("c{i}"),
        })
    }

    fn int(v: i64) -> Expr {
        Expr::Literal(Datum::Int64(v), PgType::INT8)
    }

    #[test]
    fn bare_null_starts_as_unknown() {
        // This is what makes `SELECT 'x' = col` resolve the literal from the
        // column rather than the other way round.
        let Expr::Literal(_, ty) = Expr::null_unknown() else {
            panic!("expected literal");
        };
        assert!(ty.is_unknown());
    }

    #[test]
    fn aggregates_are_found_through_nesting() {
        let agg = Expr::Aggregate {
            func: FuncId(oid::Oid(2108)), // sum(int8)
            args: vec![col(0)],
            distinct: false,
            filter: None,
            order_by: vec![],
        };
        let wrapped = Expr::Binary {
            op: OpId(oid::Oid(684)),
            lhs: Box::new(agg),
            rhs: Box::new(int(1)),
        };
        assert!(wrapped.contains_aggregate());
        assert!(!int(1).contains_aggregate());
    }

    /// An aggregate inside a subquery belongs to that subquery's level. If the
    /// traversal descended into subplans, `WHERE x IN (SELECT sum(y) …)` would
    /// be wrongly rejected as "aggregate in WHERE".
    #[test]
    fn traversal_stops_at_a_subquery_boundary() {
        let inner_agg = Expr::Aggregate {
            func: FuncId(oid::Oid(2108)),
            args: vec![col(0)],
            distinct: false,
            filter: None,
            order_by: vec![],
        };
        let subplan = crate::LogicalPlan::Values {
            rows: vec![vec![inner_agg]],
            schema: vec![("s".to_string(), PgType::INT8)],
        };
        let e = Expr::Subquery {
            kind: SubqueryKind::In,
            subplan: Box::new(subplan),
            operand: Some(Box::new(col(1))),
        };
        assert!(
            !e.contains_aggregate(),
            "an aggregate inside a subquery is not an aggregate at this level"
        );
    }

    #[test]
    fn set_returning_functions_are_detectable_in_a_target_list() {
        // The thing DataFusion structurally cannot represent.
        let srf = Expr::SetReturning {
            func: FuncId(oid::Oid(1066)), // generate_series
            args: vec![int(1), int(10)],
        };
        assert!(srf.contains_srf());
        assert!(!col(0).contains_srf());
    }

    /// NOT IN is its own variant rather than NOT(IN) because three-valued
    /// logic makes them different: with a NULL in the subquery, NOT IN yields
    /// NULL, not true.
    #[test]
    fn not_in_is_distinct_from_negated_in() {
        assert_ne!(SubqueryKind::NotIn, SubqueryKind::In);
    }

    #[test]
    fn window_frame_bounds_are_traversed() {
        let e = Expr::Window {
            func: FuncId(oid::Oid(3100)),
            args: vec![col(0)],
            partition_by: vec![col(1)],
            order_by: vec![SortKey {
                expr: col(2),
                descending: false,
                nulls_first: false,
            }],
            frame: WindowFrame {
                units: FrameUnits::Rows,
                start: FrameBound::Preceding(Box::new(int(5))),
                end: FrameBound::CurrentRow,
            },
        };
        let mut seen = 0;
        e.for_each_child(&mut |_| seen += 1);
        // arg, partition_by, order_by key, and the PRECEDING offset.
        assert_eq!(seen, 4, "the frame offset expression must be traversed");
    }
}
