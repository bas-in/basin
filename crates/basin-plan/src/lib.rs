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
    /// Multiple SRFs in one list expand to the least common multiple of their
    /// row counts, with shorter ones cycling — Postgres's rule since 10.
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
