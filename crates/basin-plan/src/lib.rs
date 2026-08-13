//! Basin's logical query IR.
//!
//! The plan representation the owned engine builds from a `pg_query` parse tree
//! and hands to the optimizer and physical layer. It replaces DataFusion's
//! `LogicalPlan`/`Expr` — see
//! `docs/decisions/0030-own-query-engine-remove-datafusion.md`.
//!
//! # Two shapes DataFusion could not represent
//!
//! Most of this enum is unsurprising. Two variants exist because the current
//! engine structurally cannot express them, and they are the reason this crate
//! is being written rather than a DataFusion adapter:
//!
//! - **DML is a relation.** [`LogicalPlan::Insert`], [`Update`](LogicalPlan::Update)
//!   and [`Delete`](LogicalPlan::Delete) carry `returning` and nest inside
//!   [`LogicalPlan::Cte`], so `WITH x AS (INSERT … RETURNING …) SELECT … FROM x`
//!   is expressible. `executor.rs:3128` and `:13038` record that DataFusion
//!   cannot plan DML as a relation.
//! - **Set-returning functions in the target list** — see
//!   [`Expr::SetReturning`] and [`LogicalPlan::ProjectSet`].
//!
//! # Status
//!
//! Types only. Lowering from `pg_query`, the optimizer, and execution are
//! separate increments; nothing consumes this crate yet.

pub mod display;
pub mod expr;
pub mod lower;
pub mod opt;
pub mod schema;

pub use expr::{
    BoolTest, ColumnRef, Datum, Expr, FrameBound, FrameUnits, FuncId, OpId, SortKey, SubqueryKind,
    Subscript, WindowFrame,
};

use basin_pgtype::PgType;

/// A relation's output schema: ordered `(name, type)` pairs.
///
/// Not `arrow_schema::Schema`, because a logical schema carries Postgres types
/// — `varchar(10)` and `text` are different here and identical in Arrow. The
/// Arrow schema is derived at the physical boundary via
/// `basin_pgtype::physical`.
pub type Schema = Vec<(String, PgType)>;

/// Identifies a table in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub u32);

/// Identifies a column within a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColId(pub u16);

/// Identifies a CTE within a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CteId(pub u16);

/// An MVCC snapshot. Carried on every [`LogicalPlan::Scan`] so visibility is a
/// property of the plan rather than ambient state inferred by storage.
///
/// Today there is exactly one snapshot per statement and this is close to a
/// constant. It is threaded through now because retrofitting a snapshot into
/// every scan later — once `BEGIN`/`COMMIT` isolation levels and
/// `SELECT … FOR UPDATE` need it — is far more expensive than carrying an
/// unused field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    /// `EXISTS` / `IN` after decorrelation.
    LeftSemi,
    /// `NOT EXISTS` / `NOT IN` after decorrelation.
    LeftAnti,
    Cross,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

/// Grouping sets, covering `GROUP BY`, `ROLLUP`, `CUBE` and
/// `GROUPING SETS` in one representation: a list of grouping-column index
/// lists, evaluated as a union.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingSets(pub Vec<Vec<u16>>);

/// `ON CONFLICT` behaviour for [`LogicalPlan::Insert`].
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflict {
    DoNothing {
        target: Vec<ColId>,
    },
    DoUpdate {
        target: Vec<ColId>,
        set: Vec<(ColId, Expr)>,
        predicate: Option<Expr>,
    },
}

/// A logical query plan.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan {
        table: TableId,
        /// Columns to read. Empty means "no columns" (as in `COUNT(*)`), not
        /// "all columns" — projection pruning is the single most load-bearing
        /// optimization in the published benchmark set, so an ambiguous
        /// encoding here would be expensive.
        projection: Vec<ColId>,
        /// Predicates the scan may evaluate itself. Pushing these down is what
        /// feeds row-group and bloom pruning.
        ///
        /// **Their `Expr::Column` indices are positions within `projection`,
        /// not within the table.** `projection`'s entries are `ColId`s (table
        /// columns); a filter's `ColumnRef { index }` is an offset into that
        /// list. The two coincide only while `projection` is the identity
        /// `0..n`, which is what lowering emits and therefore what every
        /// hand-built plan and every `SELECT *` looks like — so code that reads
        /// a filter's index as a table column passes its tests and is wrong on
        /// real queries. `opt::projection` renumbers these in step with the
        /// projection it shrinks; `opt::pushdown` lands predicates here already
        /// written against the scan's output. A consumer that needs a table
        /// column must go through `projection` to get it.
        filters: Vec<Expr>,
        snapshot: SnapshotId,
    },

    Values {
        rows: Vec<Vec<Expr>>,
        schema: Schema,
    },

    /// The empty relation. `produce_one_row` distinguishes `SELECT 1` (one row,
    /// no columns underneath) from a genuinely empty input.
    Empty {
        produce_one_row: bool,
        schema: Schema,
    },

    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<(Expr, String)>,
    },

    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    Aggregate {
        input: Box<LogicalPlan>,
        group: Vec<Expr>,
        aggs: Vec<Expr>,
        grouping_sets: Option<GroupingSets>,
    },

    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },

    Limit {
        input: Box<LogicalPlan>,
        skip: Option<Expr>,
        fetch: Option<Expr>,
        /// `FETCH FIRST n ROWS WITH TIES`. Currently degraded to `ONLY`
        /// (`select_advanced.rs:455`) because DataFusion cannot express it.
        with_ties: bool,
    },

    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        kind: JoinKind,
        /// Equijoin conjuncts, separated from `filter` because extracting them
        /// is what lets a join plan as a hash join rather than a nested loop.
        on: Vec<(Expr, Expr)>,
        filter: Option<Expr>,
    },

    /// `LATERAL` — the right side may reference columns of the left.
    ///
    /// A distinct node rather than a `Join` with correlated references, because
    /// the correlation is the defining property and a planner that loses track
    /// of it produces wrong answers rather than slow ones.
    LateralJoin {
        outer: Box<LogicalPlan>,
        inner: Box<LogicalPlan>,
        kind: JoinKind,
    },

    SetOp {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        op: SetOpKind,
        /// `UNION ALL` vs `UNION`.
        all: bool,
    },

    Distinct {
        input: Box<LogicalPlan>,
        /// `DISTINCT ON (…)`. `None` is plain `DISTINCT`.
        on: Option<Vec<Expr>>,
    },

    Window {
        input: Box<LogicalPlan>,
        windows: Vec<Expr>,
    },

    /// Expands set-returning functions in a target list.
    ///
    /// Multiple SRFs in one list run in LOCKSTEP to the LONGEST, with shorter
    /// ones padded with NULL — Postgres's rule since 10.
    /// `SELECT generate_series(1,2), generate_series(1,3)` yields three rows.
    ///
    /// It is emphatically NOT the least common multiple with shorter ones
    /// cycling; that was the pre-10 behaviour and is what most references, and
    /// most recollections, still describe. This comment said the wrong thing
    /// until 2026-08-12, having been written from memory; the operator in
    /// `basin-exec` was correct because its author checked a live server.
    ProjectSet {
        input: Box<LogicalPlan>,
        srfs: Vec<Expr>,
    },

    Cte {
        name: CteId,
        recursive: bool,
        body: Box<LogicalPlan>,
        input: Box<LogicalPlan>,
    },

    /// A reference to a CTE from within the query that defines it.
    CteRef { name: CteId, schema: Schema },

    Insert {
        table: TableId,
        input: Box<LogicalPlan>,
        columns: Vec<ColId>,
        on_conflict: Option<OnConflict>,
        returning: Option<Vec<(Expr, String)>>,
    },

    Update {
        table: TableId,
        set: Vec<(ColId, Expr)>,
        from: Option<Box<LogicalPlan>>,
        predicate: Option<Expr>,
        returning: Option<Vec<(Expr, String)>>,
        snapshot: SnapshotId,
    },

    Delete {
        table: TableId,
        using: Option<Box<LogicalPlan>>,
        predicate: Option<Expr>,
        returning: Option<Vec<(Expr, String)>>,
        snapshot: SnapshotId,
    },
}

impl LogicalPlan {
    /// Whether this plan modifies data at any depth.
    ///
    /// Used to reject a data-modifying statement where a read is required, and
    /// to force the single-writer path. Because DML is a relation here, a
    /// mutation can be arbitrarily deep inside a CTE, so this cannot be a
    /// shallow check on the root.
    pub fn is_mutating(&self) -> bool {
        if matches!(
            self,
            LogicalPlan::Insert { .. } | LogicalPlan::Update { .. } | LogicalPlan::Delete { .. }
        ) {
            return true;
        }
        let mut found = false;
        self.for_each_input(&mut |c| {
            if !found && c.is_mutating() {
                found = true;
            }
        });
        found
    }

    /// Apply `f` to each immediate child plan.
    pub fn for_each_input(&self, f: &mut impl FnMut(&LogicalPlan)) {
        match self {
            LogicalPlan::Scan { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Empty { .. }
            | LogicalPlan::CteRef { .. } => {}
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::ProjectSet { input, .. }
            | LogicalPlan::Insert { input, .. } => f(input),
            LogicalPlan::Join { left, right, .. } | LogicalPlan::SetOp { left, right, .. } => {
                f(left);
                f(right);
            }
            LogicalPlan::LateralJoin { outer, inner, .. } => {
                f(outer);
                f(inner);
            }
            LogicalPlan::Cte { body, input, .. } => {
                f(body);
                f(input);
            }
            LogicalPlan::Update { from, .. } => {
                if let Some(p) = from {
                    f(p);
                }
            }
            LogicalPlan::Delete { using, .. } => {
                if let Some(p) = using {
                    f(p);
                }
            }
        }
    }

    /// Apply `f` to each expression **this node itself owns** — not its
    /// children's, and not a subquery's `subplan` (an [`Expr::Subquery`] is
    /// handed to `f` whole; whether to descend into its separate query level
    /// is the caller's decision, exactly as [`Expr::for_each_child`] already
    /// leaves it).
    ///
    /// The complement of [`for_each_input`](Self::for_each_input): that one
    /// walks the plan tree, this one walks a single node's expression
    /// payload, and the two together reach every `Expr` in a plan. Written as
    /// an exhaustive match for the same reason `for_each_input` is — a
    /// variant added later and left off this list is a compile error rather
    /// than an expression that silently stops being visited.
    pub fn for_each_expr(&self, f: &mut impl FnMut(&Expr)) {
        match self {
            LogicalPlan::Scan { filters, .. } => {
                for e in filters {
                    f(e);
                }
            }
            LogicalPlan::Values { rows, .. } => {
                for row in rows {
                    for e in row {
                        f(e);
                    }
                }
            }
            LogicalPlan::Empty { .. }
            | LogicalPlan::CteRef { .. }
            | LogicalPlan::Cte { .. }
            | LogicalPlan::LateralJoin { .. }
            | LogicalPlan::SetOp { .. } => {}
            LogicalPlan::Project { exprs, .. } => {
                for (e, _) in exprs {
                    f(e);
                }
            }
            LogicalPlan::Filter { predicate, .. } => f(predicate),
            LogicalPlan::Aggregate { group, aggs, .. } => {
                for e in group.iter().chain(aggs) {
                    f(e);
                }
            }
            LogicalPlan::Sort { keys, .. } => {
                for k in keys {
                    f(&k.expr);
                }
            }
            LogicalPlan::Limit { skip, fetch, .. } => {
                for e in skip.iter().chain(fetch.iter()) {
                    f(e);
                }
            }
            LogicalPlan::Join { on, filter, .. } => {
                for (l, r) in on {
                    f(l);
                    f(r);
                }
                if let Some(e) = filter {
                    f(e);
                }
            }
            LogicalPlan::Distinct { on, .. } => {
                for e in on.iter().flatten() {
                    f(e);
                }
            }
            LogicalPlan::Window { windows, .. } => {
                for e in windows {
                    f(e);
                }
            }
            LogicalPlan::ProjectSet { srfs, .. } => {
                for e in srfs {
                    f(e);
                }
            }
            LogicalPlan::Insert {
                on_conflict,
                returning,
                ..
            } => {
                match on_conflict {
                    Some(OnConflict::DoUpdate { set, predicate, .. }) => {
                        for (_, e) in set {
                            f(e);
                        }
                        if let Some(e) = predicate {
                            f(e);
                        }
                    }
                    Some(OnConflict::DoNothing { .. }) | None => {}
                }
                for (e, _) in returning.iter().flatten() {
                    f(e);
                }
            }
            LogicalPlan::Update {
                set,
                predicate,
                returning,
                ..
            } => {
                for (_, e) in set {
                    f(e);
                }
                if let Some(e) = predicate {
                    f(e);
                }
                for (e, _) in returning.iter().flatten() {
                    f(e);
                }
            }
            LogicalPlan::Delete {
                predicate,
                returning,
                ..
            } => {
                if let Some(e) = predicate {
                    f(e);
                }
                for (e, _) in returning.iter().flatten() {
                    f(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(16384),
            projection: vec![ColId(0)],
            filters: vec![],
            snapshot: SnapshotId(1),
        }
    }

    /// The shape DataFusion cannot represent: a data-modifying CTE.
    /// `WITH x AS (INSERT … RETURNING …) SELECT … FROM x`.
    #[test]
    fn data_modifying_ctes_are_expressible() {
        let plan = LogicalPlan::Cte {
            name: CteId(0),
            recursive: false,
            body: Box::new(LogicalPlan::Insert {
                table: TableId(16385),
                input: Box::new(scan()),
                columns: vec![ColId(0)],
                on_conflict: None,
                returning: Some(vec![(
                    Expr::Column(ColumnRef {
                        relation: 0,
                        index: 0,
                        name: "id".into(),
                    }),
                    "id".into(),
                )]),
            }),
            input: Box::new(LogicalPlan::CteRef {
                name: CteId(0),
                schema: vec![("id".into(), basin_pgtype::PgType::INT8)],
            }),
        };
        assert!(
            plan.is_mutating(),
            "a mutation nested in a CTE still mutates"
        );
    }

    #[test]
    fn a_pure_read_does_not_report_as_mutating() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan()),
            predicate: Expr::Literal(Datum::Bool(true), basin_pgtype::PgType::BOOL),
        };
        assert!(!plan.is_mutating());
    }

    /// Empty projection means "no columns", not "all columns". Projection
    /// pruning is the highest-leverage optimization in the benchmark set, so
    /// the encoding must not be ambiguous.
    #[test]
    fn empty_projection_is_not_a_wildcard() {
        let LogicalPlan::Scan { projection, .. } = (LogicalPlan::Scan {
            table: TableId(1),
            projection: vec![],
            filters: vec![],
            snapshot: SnapshotId(0),
        }) else {
            unreachable!()
        };
        assert!(projection.is_empty());
    }

    #[test]
    fn scans_carry_a_snapshot() {
        let LogicalPlan::Scan { snapshot, .. } = scan() else {
            unreachable!()
        };
        assert_eq!(snapshot, SnapshotId(1));
    }
}
