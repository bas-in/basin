//! Turning a [`LogicalPlan`] into a tree of [`Operator`]s.
//!
//! This is the join between the two halves of the owned engine. Everything up
//! to here produces or consumes plans; everything below here moves batches.
//! Without it the operators are a pile of correct components that cannot run a
//! query, which is exactly what they were until this module existed.
//!
//! # What a table is, here
//!
//! The builder does not know about object storage, Vortex or Parquet. It asks a
//! [`TableResolver`] for a [`BatchSource`] and gets batches back. That keeps
//! `basin-exec` free of a storage dependency, lets an entire plan be executed
//! against in-memory tables in a unit test, and means the real storage layer
//! plugs in by implementing one trait rather than by editing this file.
//!
//! # What is not built yet
//!
//! [`LogicalPlan`] has more variants than there are operators. Anything without
//! one returns [`BuildError::Unsupported`] naming the construct. That is a
//! deliberate, visible gap rather than a silent fallback — a planner that
//! quietly does something else when it meets an unimplemented node produces
//! wrong answers instead of errors.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use basin_pgtype::PgType;
use basin_plan::opt::decorrelate::{contains_correlated_subquery, references_outer_row};
use basin_plan::{
    ColId, ColumnRef, CteId, Expr, LogicalPlan, OnConflict, SortKey as PlanSortKey, TableId,
};

use crate::aggregate::{AggFunc, AggregateSpec, HashAggregate, RegrKind, VarKind};
use crate::correlated::{CorrelatedKind, CorrelatedScalar, CorrelatedSubquery, QuantifiedDecider};
use crate::cte::{CteBuffer, ProjectSet};
use crate::dml::{ConflictAction, Delete, Insert, MemoryRowSink, RowSink, Update};
use crate::join::HashJoin;
use crate::lateral::{InnerFactory, LateralJoin};
use crate::limit::Limit;
use crate::operator::{default_session, ExecError, Operator, SessionRef};
use crate::project::{Filter, Project};
use crate::recursive::{RecursiveCte, RecursiveTermFactory};
use crate::scan::{BatchSource, Scan};
use crate::setop::{Distinct, Empty, SetOp, Values};
use crate::sort::{LimitWithTies, Sort, SortKey, TopK};
use crate::window::{OrderKey, WindowAgg, WindowFunc, WindowSpec};

/// The current outer row a correlated rebuild is bound to — `Some` only
/// while [`build_inner`] is constructing a `LateralJoin`'s inner side (once
/// per outer row) or a `WITH RECURSIVE` recursive term (once per
/// iteration); `None` everywhere else, including the entire ordinary build
/// path. See [`bind_outer`].
type Outer<'a> = Option<(&'a RecordBatch, usize)>;

/// Every [`CteBuffer`] registered so far in this build, shared (not copied)
/// into the `'static` closures a `LateralJoin`/`RecursiveCte` factory
/// captures — see [`SnapshotResolver`]'s doc comment for why those closures
/// cannot simply borrow anything from the top-level call instead.
type CteRegistry = Rc<RefCell<HashMap<CteId, CteBuffer>>>;

/// Why a plan could not be turned into operators.
#[derive(Debug, Clone, PartialEq)]
pub enum BuildError {
    /// A plan shape with no operator behind it yet.
    Unsupported(String),
    /// A table the resolver does not know.
    UnknownTable(TableId),
    /// A `CteRef` naming a `CteId` no enclosing `Cte` node registered. This
    /// is a planner bug, not a user error — every `CteRef` a correct
    /// planner emits is inside the scope of the `Cte` that defines it — and
    /// it is reported rather than built as an empty relation precisely
    /// because an empty relation would look like a valid, if surprising,
    /// answer instead of the broken plan it actually is.
    UnknownCte(CteId),
    /// A sort or group key that is not a plain column reference. Physical
    /// operators key on column positions, so anything else has to be
    /// materialised by a `Project` below them first — a job for the optimizer,
    /// not for this builder to improvise.
    NonColumnKey(&'static str),
    /// Building the operator itself failed.
    Exec(ExecError),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::Unsupported(what) => write!(f, "not yet executable: {what}"),
            BuildError::UnknownTable(t) => write!(f, "unknown table {t:?}"),
            BuildError::UnknownCte(id) => write!(
                f,
                "CTE reference to {id:?}, which nothing registered — a planner bug"
            ),
            BuildError::NonColumnKey(w) => {
                write!(f, "{w} key must be a column reference at this stage")
            }
            BuildError::Exec(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BuildError {}

impl From<ExecError> for BuildError {
    fn from(e: ExecError) -> Self {
        BuildError::Exec(e)
    }
}

/// Supplies the batches behind a table.
///
/// One method, so the storage layer implements exactly this and nothing else.
pub trait TableResolver {
    /// Open a source for `table`, giving it the chance to apply `projection`
    /// and `filters` itself.
    ///
    /// Both are **requests, not guarantees**. A resolver that ignores them is
    /// still correct — [`ScanPushdown::accepted`] reports what it actually did,
    /// and the builder applies whatever was declined, Arrow-side.
    ///
    /// Passing them matters: reading only the projected columns and pruning
    /// files by predicate is Basin's whole scan advantage over Postgres's heap
    /// tuples. A resolver that opens the full table and lets the engine filter
    /// afterwards throws that away, and throws it away *silently* — the answers
    /// stay right and only the I/O gets worse, which surfaces months later as a
    /// benchmark regression nobody can attribute.
    fn open(
        &self,
        table: TableId,
        projection: &[usize],
        filters: &[Expr],
    ) -> Option<(Box<dyn BatchSource>, ScanPushdown)>;
}

/// What a [`TableResolver`] actually applied, of what it was offered.
///
/// Defaults to accepting nothing, so a resolver that does not opt in stays
/// correct by construction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanPushdown {
    /// The source already applied the projection, and its batches carry only
    /// the requested columns, in the requested order. The scan must NOT project
    /// again — the indices it holds address the *table*, not the narrowed
    /// batch, so re-applying them would read the wrong columns.
    pub projection_applied: bool,
    /// The source already applied every filter it was given. The scan may skip
    /// its own filtering. Partial application is deliberately not
    /// representable: a source that can only honour some predicates should
    /// report `false` and let the scan re-apply all of them, which is cheap and
    /// always correct, rather than track which survived.
    pub filters_applied: bool,
}

/// A resolver backed by in-memory sources, for tests and for executing against
/// literal relations.
#[derive(Default)]
pub struct MemTableResolver {
    tables: HashMap<u32, Vec<(arrow_schema::SchemaRef, Vec<arrow_array::RecordBatch>)>>,
}

impl MemTableResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        table: TableId,
        schema: arrow_schema::SchemaRef,
        batches: Vec<arrow_array::RecordBatch>,
    ) {
        self.tables.insert(table.0, vec![(schema, batches)]);
    }
}

impl TableResolver for MemTableResolver {
    /// Accepts nothing. In-memory tables have no I/O to save, so pushing down
    /// would add code without buying anything; the scan filters and projects.
    fn open(
        &self,
        table: TableId,
        _projection: &[usize],
        _filters: &[Expr],
    ) -> Option<(Box<dyn BatchSource>, ScanPushdown)> {
        let (schema, batches) = self.tables.get(&table.0)?.first()?;
        Some((
            Box::new(crate::scan::VecBatchSource::new(
                schema.clone(),
                batches.clone(),
            )),
            ScanPushdown::default(),
        ))
    }
}

/// Where `INSERT`/`UPDATE`/`DELETE` write. The write-side mirror of
/// [`TableResolver`]: `build.rs` asks for a [`RowSink`] and stays free of a
/// storage dependency the same way the read side does via `BatchSource`.
///
/// Unlike a `Scan`'s projection, a write has no partial-column shape to
/// negotiate — `RowSink::insert`'s contract already fixes the batch as the
/// *full* write schema (see `dml.rs`'s module docs) — so this trait hands
/// back that schema and the row-identity columns within it (what
/// `UPDATE`/`DELETE` match on, and `INSERT ... ON CONFLICT`'s default
/// target when the statement does not name one) alongside the sink itself,
/// rather than asking for them piecemeal the way `TableResolver::open`
/// negotiates a projection.
pub trait DmlResolver {
    /// Open a sink for `table`, together with its full write schema (every
    /// column, in table-column order) and the positions within it that
    /// uniquely identify a row.
    fn open(&self, table: TableId) -> Option<(Box<dyn RowSink>, SchemaRef, Vec<usize>)>;
}

/// A [`RowSink`] that writes through a shared, `Rc`-backed
/// [`MemoryRowSink`], so a [`MemDmlResolver`]'s caller can keep its own
/// handle to inspect what was written after a build+execute — `RowSink`
/// itself is consumed by value into the operator tree, so nothing else can
/// reach it there. Not `Rc<RefCell<MemoryRowSink>>` implementing `RowSink`
/// directly, because `RowSink`'s methods take `&mut self` and `Rc` alone
/// does not give interior mutability.
struct SharedMemoryRowSink(Rc<RefCell<MemoryRowSink>>);

impl RowSink for SharedMemoryRowSink {
    fn insert(&mut self, batch: &RecordBatch) -> Result<u64, ExecError> {
        self.0.borrow_mut().insert(batch)
    }

    fn delete(&mut self, keys: &RecordBatch) -> Result<u64, ExecError> {
        self.0.borrow_mut().delete(keys)
    }
}

/// One registered table's write-side state, as [`MemDmlResolver`] tracks it.
type MemDmlTable = (Rc<RefCell<MemoryRowSink>>, SchemaRef, Vec<usize>);

/// A [`DmlResolver`] backed by in-memory [`MemoryRowSink`]s, for tests —
/// the write-side counterpart of [`MemTableResolver`].
#[derive(Default)]
pub struct MemDmlResolver {
    tables: HashMap<u32, MemDmlTable>,
}

impl MemDmlResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `table` as writable, with `key_cols` (positions within
    /// `schema`) as its uniqueness key. Returns a handle the caller can use
    /// to inspect the sink's contents after running a built plan — see
    /// [`SharedMemoryRowSink`]'s doc comment for why a plain `RowSink`
    /// cannot serve that purpose once it has been handed to `Insert`.
    pub fn insert_table(
        &mut self,
        table: TableId,
        schema: SchemaRef,
        key_cols: Vec<usize>,
    ) -> Rc<RefCell<MemoryRowSink>> {
        let sink = Rc::new(RefCell::new(MemoryRowSink::new(
            schema.clone(),
            key_cols.clone(),
        )));
        self.tables
            .insert(table.0, (Rc::clone(&sink), schema, key_cols));
        sink
    }
}

impl DmlResolver for MemDmlResolver {
    fn open(&self, table: TableId) -> Option<(Box<dyn RowSink>, SchemaRef, Vec<usize>)> {
        let (sink, schema, key_cols) = self.tables.get(&table.0)?;
        Some((
            Box::new(SharedMemoryRowSink(Rc::clone(sink))),
            schema.clone(),
            key_cols.clone(),
        ))
    }
}

/// Default memory budget for a buffering operator, in bytes.
///
/// Deliberately a constant here rather than a policy: the real budget is a
/// per-query slice of the bounded pool that already exists in the engine, and
/// wiring that through is a separate increment. Picking a number and hiding it
/// would be worse than picking one and saying so.
pub const DEFAULT_OPERATOR_BUDGET: usize = 256 * 1024 * 1024;

/// Build an operator tree for `plan`. No `INSERT`/`UPDATE`/`DELETE` in
/// `plan` can build through this entry point — see [`build_with_dml`] — so
/// this signature never changes shape no matter what else this module
/// learns to build; existing callers are never asked for more than a
/// [`TableResolver`].
pub fn build(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
) -> Result<Box<dyn Operator>, BuildError> {
    build_with_budget(plan, tables, DEFAULT_OPERATOR_BUDGET)
}

/// Build an operator tree that evaluates in `session` — the statement's
/// `TimeZone` GUC and its transaction/statement clocks — rather than in
/// [`EvalSession::DEFAULT`](crate::eval::EvalSession::DEFAULT)'s UTC with no
/// clock.
///
/// This is the entry point the engine uses for a real statement. The three
/// entry points above stay exactly as they were, defaulting to
/// `EvalSession::DEFAULT`, for the same reason [`crate::eval::eval`] still
/// exists beside [`crate::eval::eval_with`]: every existing caller and every
/// read-only test battery keeps compiling and keeps its previous answers,
/// and a session is something a caller opts into rather than something the
/// signature forces it to invent.
///
/// `dml` is optional on the same terms as [`build_with_dml`] versus
/// [`build`]: `None` refuses data-modifying plans.
pub fn build_in_session(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    session: &SessionRef,
) -> Result<Box<dyn Operator>, BuildError> {
    let ctes: CteRegistry = Rc::new(RefCell::new(HashMap::new()));
    build_inner(plan, tables, dml, budget, &ctes, session, None)
}

/// Build an operator tree with an explicit memory budget for buffering
/// operators. Like [`build`], data-modifying statements are refused — see
/// [`build_with_dml`].
pub fn build_with_budget(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    budget: usize,
) -> Result<Box<dyn Operator>, BuildError> {
    let ctes: CteRegistry = Rc::new(RefCell::new(HashMap::new()));
    build_inner(plan, tables, None, budget, &ctes, &default_session(), None)
}

/// Build an operator tree that may contain `INSERT`/`UPDATE`/`DELETE`,
/// resolving their write side through `dml`. A separate entry point rather
/// than a new parameter on [`build`]/[`build_with_budget`]: those two stay
/// exactly as they were before DML existed, so nothing that already calls
/// them needs to acquire a write resolver it does not use.
pub fn build_with_dml(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: &dyn DmlResolver,
    budget: usize,
) -> Result<Box<dyn Operator>, BuildError> {
    let ctes: CteRegistry = Rc::new(RefCell::new(HashMap::new()));
    build_inner(
        plan,
        tables,
        Some(dml),
        budget,
        &ctes,
        &default_session(),
        None,
    )
}

/// The actual recursive builder. `ctes` is threaded (not created fresh per
/// call) so a `CteRef` anywhere under the plan — including inside a
/// `LateralJoin`'s inner side or a `WITH RECURSIVE` recursive term's
/// rebuilt subplan — can see every `Cte` registered by an ancestor. `outer`
/// is `None` for the entire ordinary build; see [`Outer`].
fn build_inner(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
    outer: Outer<'_>,
) -> Result<Box<dyn Operator>, BuildError> {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            ..
        } => {
            let cols: Vec<usize> = projection.iter().map(|c| c.0 as usize).collect();
            let filters: Vec<Expr> = filters
                .iter()
                .map(|f| bind_outer(f, outer, tables, dml, budget, ctes, session))
                .collect::<Result<_, _>>()?;
            let (source, pushed) = tables
                .open(*table, &cols, &filters)
                .ok_or(BuildError::UnknownTable(*table))?;

            // Whatever the source declined, the scan still does. When the
            // source applied the projection its batches are already narrowed,
            // so the scan's indices — which address the full table — become
            // identity over the narrowed batch rather than being re-applied
            // against it, which would read the wrong columns.
            let scan_cols: Vec<usize> = if pushed.projection_applied {
                (0..cols.len()).collect()
            } else {
                cols.clone()
            };
            // `Scan.filters`' column indices are positions within
            // `Scan.projection` — the scan's own OUTPUT — not positions in the
            // table. That is what the logical layer produces: filter pushdown
            // moves a `Filter`'s predicate, whose indices address its input's
            // output, into the scan, and projection pruning renumbers those
            // indices in step with the projection it shrinks (see
            // `basin_plan::opt::projection`'s `Scan` arm).
            //
            // The physical `Scan`, by contrast, evaluates filters against the
            // source's UNPROJECTED batch (`scan.rs`), so the two index spaces
            // only coincide when the source narrowed itself to the projection.
            // When it did not, every filter index has to be translated back
            // through `cols` first. Skipping that translation is not a slow
            // query, it is a wrong answer: the predicate silently reads
            // whatever column happens to sit at that position in the table.
            let scan_filters = if pushed.filters_applied {
                Vec::new()
            } else if pushed.projection_applied {
                filters
            } else {
                filters_to_source_positions(filters, &cols)
            };
            Ok(Box::new(
                Scan::new(source, scan_cols, scan_filters)?.in_session(SessionRef::clone(session)),
            ))
        }

        // `Filter` and `Project` are the two nodes that can host a per-row
        // correlated scalar subquery (see [`CorrSink`]): each binds its
        // expressions with a sink, and if anything landed in it, the child
        // gains a `CorrelatedScalar` underneath supplying one column per
        // subquery. Every other node refuses a correlated scalar subquery
        // rather than evaluating it once and pretending — `bind_outer`.
        LogicalPlan::Filter { input, predicate } => {
            let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            let child_schema = child.schema();
            let sink = CorrSink {
                base_width: child_schema.fields().len() as u16,
                subplans: RefCell::new(Vec::new()),
            };
            let predicate =
                bind_outer_collecting(predicate, outer, &sink, tables, dml, budget, ctes, session)?;
            let subplans = sink.subplans.into_inner();
            if subplans.is_empty() {
                return Ok(Box::new(
                    Filter::new(child, predicate).in_session(SessionRef::clone(session)),
                ));
            }
            // A `Filter` must not change its input's schema, so the columns
            // the subqueries added are projected back off above it — the
            // predicate has already been rewritten to read them, and
            // nothing above this node knows they existed.
            let width = child_schema.fields().len();
            let child = build_correlated_scalars(
                child,
                subplans,
                width as u16,
                tables,
                budget,
                ctes,
                session,
            )?;
            let filtered: Box<dyn Operator> =
                Box::new(Filter::new(child, predicate).in_session(SessionRef::clone(session)));
            let trim: Vec<(Expr, String)> = child_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    (
                        Expr::Column(basin_plan::ColumnRef {
                            relation: 0,
                            index: i as u16,
                            name: f.name().clone(),
                        }),
                        f.name().clone(),
                    )
                })
                .collect();
            Ok(Box::new(
                Project::new(filtered, trim)?.in_session(SessionRef::clone(session)),
            ))
        }

        LogicalPlan::Project { input, exprs } => {
            let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            let base_width = child.schema().fields().len() as u16;
            let sink = CorrSink {
                base_width,
                subplans: RefCell::new(Vec::new()),
            };
            let exprs: Vec<(Expr, String)> = exprs
                .iter()
                .map(|(e, n)| {
                    Ok((
                        bind_outer_collecting(e, outer, &sink, tables, dml, budget, ctes, session)?,
                        n.clone(),
                    ))
                })
                .collect::<Result<_, BuildError>>()?;
            let subplans = sink.subplans.into_inner();
            let child = if subplans.is_empty() {
                child
            } else {
                build_correlated_scalars(
                    child, subplans, base_width, tables, budget, ctes, session,
                )?
            };
            Ok(Box::new(
                Project::new(child, exprs)?.in_session(SessionRef::clone(session)),
            ))
        }

        // `Limit` with a fetch and a sorted input is the top-K shape. Basin's
        // published ORDER BY … LIMIT numbers depend on early termination, so
        // recognising it here rather than sorting everything and truncating is
        // load-bearing, not a micro-optimisation.
        LogicalPlan::Limit {
            input,
            skip,
            fetch,
            with_ties,
        } => {
            let fetch = fetch
                .as_ref()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes, session))
                .transpose()?;
            let skip_n = match skip
                .as_ref()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes, session))
                .transpose()?
            {
                Some(e) if is_null_literal(&e) => None,
                Some(e) => Some(
                    const_usize(&e)
                        .ok_or_else(|| BuildError::Unsupported("non-constant OFFSET".into()))?,
                ),
                None => None,
            };
            let fetch_n = match &fetch {
                Some(e) if is_null_literal(e) => None,
                Some(e) => Some(
                    const_usize(e)
                        .ok_or_else(|| BuildError::Unsupported("non-constant LIMIT".into()))?,
                ),
                None => None,
            };
            // `WITH TIES` needs the ORDER BY key to decide what ties, so it is
            // matched before the shapes below — none of which carry one.
            // Postgres itself rejects `FETCH … WITH TIES` without an `ORDER
            // BY` ("WITH TIES cannot be specified without ORDER BY clause"),
            // so the only legal input plan is a Limit directly over a Sort;
            // anything else is refused rather than silently degraded to
            // `ONLY`, which is exactly the wrong answer the incumbent path
            // gives (see `LimitWithTies`).
            if *with_ties {
                let (LogicalPlan::Sort { input: si, keys }, None, Some(k)) =
                    (input.as_ref(), skip_n, fetch_n)
                else {
                    return Err(BuildError::Unsupported(
                        "FETCH … WITH TIES without a plain ORDER BY".into(),
                    ));
                };
                let child = build_inner(si, tables, dml, budget, ctes, session, outer)?;
                let keys = bind_sort_keys(keys, outer, tables, dml, budget, ctes, session)?;
                let sorted = Box::new(Sort::new(child, sort_keys(&keys)?, budget));
                return Ok(Box::new(LimitWithTies::new(sorted, sort_keys(&keys)?, k)));
            }

            if fetch_n.is_none() && skip_n.is_none() {
                return build_inner(input, tables, dml, budget, ctes, session, outer);
            }

            // `ORDER BY … LIMIT` with no offset fuses into a bounded heap, which
            // is what makes the published numbers for that shape depend on early
            // termination rather than a full sort. Every other combination —
            // including an offset, which the heap cannot express — becomes a
            // streaming Limit over whatever the input already is.
            match (input.as_ref(), skip_n, fetch_n) {
                (LogicalPlan::Sort { input: si, keys }, None, Some(k)) => {
                    let child = build_inner(si, tables, dml, budget, ctes, session, outer)?;
                    let keys = bind_sort_keys(keys, outer, tables, dml, budget, ctes, session)?;
                    Ok(Box::new(TopK::new(child, sort_keys(&keys)?, k)))
                }
                _ => {
                    let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
                    Ok(Box::new(Limit::new(child, skip_n.unwrap_or(0), fetch_n)))
                }
            }
        }

        LogicalPlan::Sort { input, keys } => {
            let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            let keys = bind_sort_keys(keys, outer, tables, dml, budget, ctes, session)?;
            Ok(Box::new(Sort::new(child, sort_keys(&keys)?, budget)))
        }

        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } => {
            if grouping_sets.is_some() {
                return Err(BuildError::Unsupported(
                    "GROUPING SETS / ROLLUP / CUBE".into(),
                ));
            }
            let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            let group: Vec<Expr> = group
                .iter()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes, session))
                .collect::<Result<_, _>>()?;
            let group_cols = group
                .iter()
                .map(|e| column_index(e).ok_or(BuildError::NonColumnKey("GROUP BY")))
                .collect::<Result<Vec<_>, _>>()?;
            let aggs: Vec<Expr> = aggs
                .iter()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes, session))
                .collect::<Result<_, _>>()?;
            let specs = aggs
                .iter()
                .enumerate()
                .map(|(i, a)| agg_spec(a, &format!("agg{i}")))
                .collect::<Result<Vec<_>, BuildError>>()?;
            Ok(Box::new(HashAggregate::new(
                child, group_cols, specs, budget,
            )?))
        }

        LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } => {
            // `Left`/`Right`/`Full` need the residual to survive a failed
            // pair as an unmatched, NULL-extended row rather than drop it —
            // `HashJoin` has no path for that (see its module docs' own
            // "Residual filter" section), so this is refused here, by name,
            // rather than built into something that runs and returns wrong
            // rows. `Inner`/`Cross`/`LeftSemi`/`LeftAnti` are the kinds
            // `basin-plan`'s decorrelation of `EXISTS`/`IN` actually
            // produces a residual for (`opt/decorrelate.rs`'s transforms
            // 1-3); an outer join carrying one would have to come from a
            // hand-written `ON`-clause residual on an explicit outer join,
            // which is real SQL but not the case this task exists for.
            if filter.is_some()
                && matches!(
                    kind,
                    basin_plan::JoinKind::Left
                        | basin_plan::JoinKind::Right
                        | basin_plan::JoinKind::Full
                )
            {
                return Err(BuildError::Unsupported(format!(
                    "{kind:?} join with a non-equi residual condition — a pair that fails the \
                     residual must survive as an unmatched, NULL-extended row, and there is no \
                     physical operator that implements that today"
                )));
            }
            let l = build_inner(left, tables, dml, budget, ctes, session, outer)?;
            let r = build_inner(right, tables, dml, budget, ctes, session, outer)?;
            let mut lk = Vec::with_capacity(on.len());
            let mut rk = Vec::with_capacity(on.len());
            for (a, b) in on {
                let a = bind_outer(a, outer, tables, dml, budget, ctes, session)?;
                let b = bind_outer(b, outer, tables, dml, budget, ctes, session)?;
                lk.push(column_index(&a).ok_or(BuildError::NonColumnKey("join"))?);
                rk.push(column_index(&b).ok_or(BuildError::NonColumnKey("join"))?);
            }
            // `filter` mixes relation-0 (left, sometimes flat over
            // left++right — see `join.rs`'s `flatten_filter`) and
            // relation-1 (right, decorrelation's own convention) column
            // refs; `bind_outer` only ever substitutes `relation ==
            // OUTER_REF` (1) columns when this join sits inside a LATERAL's
            // per-row closure (`outer` is `Some`). A decorrelated filter's
            // own relation-1 tags are a different thing entirely (the
            // join's own right side, not the enclosing LATERAL row) that
            // happens to reuse the same tag value — see `on`'s existing
            // `bind_outer(b, ...)` call just above, which has carried this
            // same ambiguity for `on`'s right-hand pairs since before this
            // change. Not resolved here — this join arm is not the place to
            // fix a relation-tag collision that predates it — but real: a
            // decorrelated Semi/Anti nested inside a LATERAL would mis-bind.
            let filter = filter
                .as_ref()
                .map(|f| bind_outer(f, outer, tables, dml, budget, ctes, session))
                .transpose()?;
            Ok(Box::new(
                HashJoin::with_filter(l, r, *kind, lk, rk, filter, budget)?
                    .in_session(SessionRef::clone(session)),
            ))
        }

        // `LATERAL` — the inner side is rebuilt fresh per outer row via a
        // factory, because its predicates may reference the outer row's own
        // columns. See `lateral.rs`'s module docs for why a factory rather
        // than a fixed operator, and [`bind_outer`]/[`SnapshotResolver`] for
        // how this builder resolves the correlation and satisfies the
        // factory's `'static` bound.
        LogicalPlan::LateralJoin {
            outer: outer_plan,
            inner,
            kind,
        } => {
            if !matches!(
                kind,
                basin_plan::JoinKind::Inner
                    | basin_plan::JoinKind::Cross
                    | basin_plan::JoinKind::Left
            ) {
                return Err(BuildError::Unsupported(format!(
                    "LATERAL join of kind {kind:?}"
                )));
            }
            if inner.is_mutating() {
                return Err(BuildError::Unsupported(
                    "data-modifying statement inside a LATERAL subquery".into(),
                ));
            }
            let outer_op = build_inner(outer_plan, tables, dml, budget, ctes, session, outer)?;

            let inner_plan = inner.as_ref().clone();
            let mut snapshot = SnapshotResolver::default();
            snapshot_scans(&inner_plan, tables, &mut snapshot)?;
            let snapshot = Rc::new(snapshot);

            // The inner side's schema is needed up front (the operator's own
            // output schema depends on it) but no real outer row exists yet.
            // A single all-NULL probe row gives every `OUTER_REF` column a
            // literal of the right TYPE (derived from the outer schema's
            // Arrow type, independent of the value) without needing one —
            // see `outer_literal`'s NULL handling.
            let probe_batch = null_probe_row(&outer_op.schema())?;
            let inner_schema = build_inner(
                &inner_plan,
                snapshot.as_ref(),
                None,
                budget,
                ctes,
                session,
                Some((&probe_batch, 0)),
            )?
            .schema();

            let snapshot_for_factory = Rc::clone(&snapshot);
            let ctes_for_factory = Rc::clone(ctes);
            // The inner plan is rebuilt per outer row, so the factory has to
            // own a handle to the session rather than borrow one — an `Rc`
            // clone, so every rebuild lands in the same session as the outer
            // side rather than silently reverting to the UTC default.
            let session_for_factory = SessionRef::clone(session);
            let inner_plan_for_factory = inner_plan.clone();
            let make_inner: InnerFactory = Box::new(move |row_batch: &RecordBatch, idx: usize| {
                build_inner(
                    &inner_plan_for_factory,
                    snapshot_for_factory.as_ref(),
                    None,
                    budget,
                    &ctes_for_factory,
                    &session_for_factory,
                    Some((row_batch, idx)),
                )
                .map_err(build_error_to_exec)
            });
            Ok(Box::new(LateralJoin::new(
                outer_op,
                inner_schema,
                make_inner,
                *kind,
            )))
        }

        LogicalPlan::Values { rows, schema } => {
            let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
            let rows: Vec<Vec<Expr>> = rows
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|e| bind_outer(e, outer, tables, dml, budget, ctes, session))
                        .collect::<Result<_, _>>()
                })
                .collect::<Result<_, BuildError>>()?;
            Ok(Box::new(
                Values::new(rows, names)?.in_session(SessionRef::clone(session)),
            ))
        }

        LogicalPlan::Empty {
            produce_one_row,
            schema,
        } => {
            let fields = schema
                .iter()
                .map(|(n, t)| {
                    basin_pgtype::physical(*t)
                        .map(|dt| arrow_schema::Field::new(n, dt, true))
                        .map_err(|e| BuildError::Unsupported(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let arrow_schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
            Ok(Box::new(Empty::new(arrow_schema, *produce_one_row)))
        }

        LogicalPlan::Distinct { input, on } => {
            let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            match on {
                None => Ok(Box::new(Distinct::new(child, budget))),
                Some(exprs) => {
                    let exprs: Vec<Expr> = exprs
                        .iter()
                        .map(|e| bind_outer(e, outer, tables, dml, budget, ctes, session))
                        .collect::<Result<_, _>>()?;
                    let cols = exprs
                        .iter()
                        .map(|e| column_index(e).ok_or(BuildError::NonColumnKey("DISTINCT ON")))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Box::new(Distinct::new_on(child, cols, budget)))
                }
            }
        }

        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => {
            let l = build_inner(left, tables, dml, budget, ctes, session, outer)?;
            let r = build_inner(right, tables, dml, budget, ctes, session, outer)?;
            Ok(Box::new(SetOp::new(l, r, *op, *all, budget)?))
        }

        // Every window expression in one node shares a PARTITION BY / ORDER BY,
        // because the planner groups them that way — one operator per distinct
        // window, not per expression. The operator requires its input already
        // sorted by those keys and never re-sorts, so an unsorted input is a
        // planner bug it will not paper over.
        LogicalPlan::Window { input, windows } => {
            let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            let mut windows: Vec<Expr> = windows
                .iter()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes, session))
                .collect::<Result<_, _>>()?;
            let (partition_by, order_by) = window_keys(&windows)?;
            let (child, trim) = materialize_window_args(child, &mut windows, session)?;
            let specs = windows
                .iter()
                .enumerate()
                .map(|(i, w)| window_spec(w, &format!("window{i}")))
                .collect::<Result<Vec<_>, BuildError>>()?;
            let agg: Box<dyn Operator> = Box::new(WindowAgg::new(
                child,
                partition_by,
                order_by,
                specs,
                budget,
            )?);
            match trim {
                None => Ok(agg),
                Some(t) => Ok(Box::new(trim_materialized_window_args(agg, t)?)),
            }
        }

        // Set-returning functions in the target list — the shape
        // `jsonb_udf.rs:16` records as impossible inside DataFusion, which is
        // why `generate_series` and `unnest` in a SELECT list do not work on
        // the current engine at all.
        //
        // Note the LCM trap: multiple SRFs in one list do NOT produce the least
        // common multiple of their lengths. Since Postgres 10 they run in
        // lockstep to the LONGEST, padding the shorter with NULL. The operator
        // implements the modern rule; this comment exists because the older one
        // is what most references and most recollections still describe.
        LogicalPlan::ProjectSet { input, srfs } => {
            let child = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            let named: Vec<(Expr, String)> = srfs
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    Ok((
                        bind_outer(e, outer, tables, dml, budget, ctes, session)?,
                        format!("srf{i}"),
                    ))
                })
                .collect::<Result<_, BuildError>>()?;
            Ok(Box::new(
                ProjectSet::new(child, named)?.in_session(SessionRef::clone(session)),
            ))
        }

        // A `WITH` body is built once into a `CteBuffer` and registered by
        // `CteId`; every `CteRef` to that id — however many there are —
        // takes its own `CteReader` off the same buffer (see `cte.rs`'s
        // module docs on why materialize-once/replay-many is the right
        // shape, not just a convenient one). `recursive` selects whether the
        // body is built as an ordinary subplan or as a `RecursiveCte`
        // fixpoint loop (see `build_recursive_cte`); either way, the RESULT
        // is wrapped in the same `CteBuffer`, so a recursive CTE referenced
        // twice also replays in full both times, not just a non-recursive
        // one.
        LogicalPlan::Cte {
            name,
            recursive,
            body,
            input,
        } => {
            let body_op: Box<dyn Operator> = if *recursive {
                build_recursive_cte(*name, body, tables, budget, ctes, session, outer)?
            } else {
                build_inner(body, tables, dml, budget, ctes, session, outer)?
            };
            let buffer = CteBuffer::new(body_op, budget);
            ctes.borrow_mut().insert(*name, buffer);
            build_inner(input, tables, dml, budget, ctes, session, outer)
        }

        // A planner bug, not a user error, if `name` was never registered —
        // see `BuildError::UnknownCte`'s doc comment.
        LogicalPlan::CteRef { name, .. } => {
            let reader = {
                let registered = ctes.borrow();
                let buffer = registered.get(name).ok_or(BuildError::UnknownCte(*name))?;
                buffer.reader()
            };
            Ok(Box::new(reader))
        }

        LogicalPlan::Insert {
            table,
            input,
            columns: _,
            on_conflict,
            returning,
        } => {
            let dml_resolver = dml.ok_or_else(|| {
                BuildError::Unsupported("INSERT (no write resolver configured)".into())
            })?;
            let (sink, write_schema, key_cols) = dml_resolver
                .open(*table)
                .ok_or(BuildError::UnknownTable(*table))?;
            let input_op = build_inner(input, tables, dml, budget, ctes, session, outer)?;
            if input_op.schema() != write_schema {
                return Err(BuildError::Unsupported(format!(
                    "INSERT input schema does not match {table:?}'s write schema — expected \
                     {write_schema:?}, got {:?} (defaults/column order are assumed already \
                     resolved upstream, per dml.rs's module docs)",
                    input_op.schema()
                )));
            }
            let action = bind_on_conflict(on_conflict, &write_schema, &key_cols)?;
            let want_returning = returning.is_some();
            let dml_op: Box<dyn Operator> =
                Box::new(Insert::new(input_op, sink, action, want_returning));
            wrap_returning(dml_op, returning, outer, tables, dml, budget, ctes, session)
        }

        // `Update`/`Delete` carry no explicit input plan (unlike `Insert`) —
        // this builder synthesises `Scan(table) [+ Filter(predicate)]`
        // itself. `UPDATE … FROM` / `DELETE … USING` are refused rather than
        // improvised as a cross join with no declared join condition to
        // narrow it; see the module docs' "What is not built yet" posture.
        LogicalPlan::Update {
            table,
            set,
            from,
            predicate,
            returning,
            snapshot,
        } => {
            if from.is_some() {
                return Err(BuildError::Unsupported("UPDATE … FROM".into()));
            }
            let dml_resolver = dml.ok_or_else(|| {
                BuildError::Unsupported("UPDATE (no write resolver configured)".into())
            })?;
            let (sink, write_schema, key_cols) = dml_resolver
                .open(*table)
                .ok_or(BuildError::UnknownTable(*table))?;
            let n = write_schema.fields().len();
            let scan = LogicalPlan::Scan {
                table: *table,
                projection: (0..n as u16).map(ColId).collect(),
                filters: Vec::new(),
                snapshot: *snapshot,
            };
            let scanned = build_inner(&scan, tables, dml, budget, ctes, session, outer)?;
            let matched: Box<dyn Operator> = match predicate {
                Some(p) => Box::new(
                    Filter::new(
                        scanned,
                        bind_outer(p, outer, tables, dml, budget, ctes, session)?,
                    )
                    .in_session(SessionRef::clone(session)),
                ),
                None => scanned,
            };
            let mut set_map: HashMap<usize, Expr> = HashMap::new();
            for (c, e) in set {
                set_map.insert(
                    c.0 as usize,
                    bind_outer(e, outer, tables, dml, budget, ctes, session)?,
                );
            }
            let exprs: Vec<(Expr, String)> = (0..n)
                .map(|i| {
                    let name = write_schema.field(i).name().clone();
                    let e = set_map.remove(&i).unwrap_or_else(|| {
                        Expr::Column(basin_plan::ColumnRef {
                            relation: 0,
                            index: i as u16,
                            name: name.clone(),
                        })
                    });
                    (e, name)
                })
                .collect();
            let new_rows = Project::new(matched, exprs)?.in_session(SessionRef::clone(session));
            let want_returning = returning.is_some();
            let dml_op: Box<dyn Operator> = Box::new(Update::new(
                Box::new(new_rows),
                sink,
                key_cols,
                want_returning,
            ));
            wrap_returning(dml_op, returning, outer, tables, dml, budget, ctes, session)
        }

        LogicalPlan::Delete {
            table,
            using,
            predicate,
            returning,
            snapshot,
        } => {
            if using.is_some() {
                return Err(BuildError::Unsupported("DELETE … USING".into()));
            }
            let dml_resolver = dml.ok_or_else(|| {
                BuildError::Unsupported("DELETE (no write resolver configured)".into())
            })?;
            let (sink, write_schema, key_cols) = dml_resolver
                .open(*table)
                .ok_or(BuildError::UnknownTable(*table))?;
            let n = write_schema.fields().len();
            let scan = LogicalPlan::Scan {
                table: *table,
                projection: (0..n as u16).map(ColId).collect(),
                filters: Vec::new(),
                snapshot: *snapshot,
            };
            let scanned = build_inner(&scan, tables, dml, budget, ctes, session, outer)?;
            let matched: Box<dyn Operator> = match predicate {
                Some(p) => Box::new(
                    Filter::new(
                        scanned,
                        bind_outer(p, outer, tables, dml, budget, ctes, session)?,
                    )
                    .in_session(SessionRef::clone(session)),
                ),
                None => scanned,
            };
            let want_returning = returning.is_some();
            let dml_op: Box<dyn Operator> =
                Box::new(Delete::new(matched, sink, key_cols, want_returning));
            wrap_returning(dml_op, returning, outer, tables, dml, budget, ctes, session)
        }
    }
}

/// Translate `Insert`'s `on_conflict` into the physical [`ConflictAction`].
/// `ON CONFLICT ... WHERE` and a `SET` list narrower than the full row are
/// refused rather than improvised — see `dml.rs`'s module docs on why
/// `ConflictAction::DoUpdate` can only mean "replace the row wholesale".
fn bind_on_conflict(
    on_conflict: &Option<OnConflict>,
    write_schema: &SchemaRef,
    resolver_key_cols: &[usize],
) -> Result<Option<ConflictAction>, BuildError> {
    match on_conflict {
        None => Ok(None),
        Some(OnConflict::DoNothing { .. }) => Ok(Some(ConflictAction::DoNothing)),
        Some(OnConflict::DoUpdate {
            target,
            set,
            predicate,
        }) => {
            if predicate.is_some() {
                return Err(BuildError::Unsupported(
                    "ON CONFLICT ... DO UPDATE ... WHERE".into(),
                ));
            }
            if set.len() != write_schema.fields().len() {
                return Err(BuildError::Unsupported(
                    "ON CONFLICT DO UPDATE with a SET list narrower than the full row".into(),
                ));
            }
            let key_cols = if target.is_empty() {
                resolver_key_cols.to_vec()
            } else {
                target.iter().map(|c| c.0 as usize).collect()
            };
            Ok(Some(ConflictAction::DoUpdate { key_cols }))
        }
    }
}

/// Wrap a DML operator's output with the `RETURNING` projection, if any —
/// `Insert`/`Update`/`Delete` themselves only gate WHETHER any rows come
/// out (`want_returning`), always at full row width; picking out (and
/// computing) the actual `RETURNING` list is this builder's job, the same
/// way `dml.rs`'s module docs describe "a `Project` to pick out the
/// `RETURNING` list" sitting above the DML node.
fn wrap_returning(
    dml_op: Box<dyn Operator>,
    returning: &Option<Vec<(Expr, String)>>,
    outer: Outer<'_>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Box<dyn Operator>, BuildError> {
    match returning {
        None => Ok(dml_op),
        Some(ret) => {
            let exprs: Vec<(Expr, String)> = ret
                .iter()
                .map(|(e, n)| {
                    Ok((
                        bind_outer(e, outer, tables, dml, budget, ctes, session)?,
                        n.clone(),
                    ))
                })
                .collect::<Result<_, BuildError>>()?;
            Ok(Box::new(
                Project::new(dml_op, exprs)?.in_session(SessionRef::clone(session)),
            ))
        }
    }
}

/// Upper bound on `WITH RECURSIVE` iterations — see `recursive.rs`'s module
/// docs item 4 on why this crate needs one at all (no independent timer to
/// let `statement_timeout` interrupt a non-terminating recursive term).
/// Generous rather than tight: a genuinely converging query at this scale
/// is already well past anything reasonable to run synchronously.
const DEFAULT_RECURSION_LIMIT: usize = 10_000;

/// Build a `RecursiveCte` for `body`, which must be `anchor UNION [ALL]
/// recursive_term` — the only shape SQL's `WITH RECURSIVE` has. `dml` is
/// deliberately not threaded into the recursive term's rebuilds: it runs
/// inside a `'static` factory closure (see [`SnapshotResolver`]'s doc
/// comment for why), and a data-modifying statement re-run once per
/// iteration is refused up front instead, same as `LateralJoin`'s inner
/// side.
fn build_recursive_cte(
    name: CteId,
    body: &LogicalPlan,
    tables: &dyn TableResolver,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
    outer: Outer<'_>,
) -> Result<Box<dyn Operator>, BuildError> {
    let LogicalPlan::SetOp {
        left,
        right,
        op: basin_plan::SetOpKind::Union,
        all,
    } = body
    else {
        return Err(BuildError::Unsupported(
            "WITH RECURSIVE body must be UNION [ALL] of an anchor and a recursive term".into(),
        ));
    };
    if right.is_mutating() {
        return Err(BuildError::Unsupported(
            "data-modifying statement inside a WITH RECURSIVE recursive term".into(),
        ));
    }

    let anchor_op = build_inner(left, tables, None, budget, ctes, session, outer)?;
    let anchor_schema = anchor_op.schema();

    let recursive_plan = right.as_ref().clone();
    let mut snapshot = SnapshotResolver::default();
    snapshot_scans(&recursive_plan, tables, &mut snapshot)?;
    let snapshot = Rc::new(snapshot);
    let ctes_captured = Rc::clone(ctes);
    // Owned by the factory for the same reason as the lateral one above:
    // every iteration of the recursive term rebuilds the plan, and each
    // rebuild must land in this statement's session.
    let session_captured = SessionRef::clone(session);
    let schema_for_feed = Arc::clone(&anchor_schema);
    let outer_owned: Option<(RecordBatch, usize)> = outer.map(|(b, i)| (b.clone(), i));

    let recursive_term: RecursiveTermFactory = Box::new(move |working_table: Vec<RecordBatch>| {
        // Bind `name` — this CTE's own name, as it appears inside its
        // recursive term — to a plain one-shot replay of ONLY the working
        // table just finished, never the shared, materialize-once buffer
        // the enclosing `Cte` node is about to register `name` under (that
        // buffer does not even exist while this factory runs, and would be
        // the wrong semantics regardless — recursive.rs's module docs item
        // 2). Re-inserting on every call is deliberate: each iteration's
        // working table replaces the previous one under the same key.
        let feed: Box<dyn Operator> =
            Box::new(VecFeed::new(Arc::clone(&schema_for_feed), working_table));
        ctes_captured
            .borrow_mut()
            .insert(name, CteBuffer::new(feed, budget));
        let bound_outer = outer_owned.as_ref().map(|(b, i)| (b, *i));
        build_inner(
            &recursive_plan,
            snapshot.as_ref(),
            None,
            budget,
            &ctes_captured,
            &session_captured,
            bound_outer,
        )
        .map_err(build_error_to_exec)
    });

    Ok(Box::new(RecursiveCte::new(
        anchor_op,
        recursive_term,
        *all,
        budget,
        DEFAULT_RECURSION_LIMIT,
    )))
}

/// A `'static`-safe, one-shot replay of an already-materialized set of
/// batches — the recursive term's view of "the previous iteration's working
/// table" (recursive.rs's module docs item 2), and the anchor half of the
/// `Feed` shape every operator file's own test module defines locally.
struct VecFeed {
    schema: SchemaRef,
    batches: std::collections::VecDeque<RecordBatch>,
}

impl VecFeed {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into(),
        }
    }
}

impl Operator for VecFeed {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        Ok(self.batches.pop_front())
    }
}

/// A fully self-contained (owned, no borrowed lifetime) [`TableResolver`]
/// covering exactly the `(table, projection)` pairs a subplan's `Scan`
/// nodes asked for, built once by [`snapshot_scans`] before a
/// `LateralJoin`'s inner side or a `WITH RECURSIVE` recursive term is
/// handed to its factory closure.
///
/// # Why this exists
///
/// `lateral.rs`'s `InnerFactory` and `recursive.rs`'s `RecursiveTermFactory`
/// are both `Box<dyn FnMut(...) -> ...>` with no lifetime parameter, which
/// Rust defaults to `'static` for a boxed trait object with none written.
/// The factory this builder installs has to call [`build_inner`] again on
/// every invocation — once per outer row, or once per iteration — to
/// realise the documented "re-lower and re-bind, then build a fresh
/// physical operator" design (`lateral.rs`'s module docs), which means it
/// needs a `TableResolver` to hand to that call. The one the top-level
/// caller passed to [`build_with_budget`]/[`build_with_dml`] is only
/// `&dyn TableResolver` for the duration of that one call — it cannot
/// satisfy a `'static` closure no matter how long the referent actually
/// lives, because the borrow checker has no way to know that from the type
/// alone. Rather than widen every public entry point's signature to demand
/// an owned, `'static`-safe handle that ordinary (non-correlated,
/// non-recursive) queries never need, this builder eagerly drains exactly
/// the base tables the correlated/recursive subplan touches — ONCE, while
/// the real resolver is still in scope — into a private, fully owned
/// snapshot, and gives the closure that instead.
///
/// This trades pushdown for the correlated/recursive fallback path only:
/// every row/iteration rebuilds against the SAME in-memory snapshot rather
/// than re-querying storage. The always-correct general LATERAL/`WITH
/// RECURSIVE` path this exists to provide (see `lateral.rs`'s module docs
/// on the textual rewrite it backstops) was never meant to match the fast
/// path's I/O profile; only correctness is this path's contract.
#[derive(Default)]
struct SnapshotResolver {
    tables: HashMap<(u32, Vec<usize>), (SchemaRef, Vec<RecordBatch>)>,
}

impl TableResolver for SnapshotResolver {
    fn open(
        &self,
        table: TableId,
        projection: &[usize],
        _filters: &[Expr],
    ) -> Option<(Box<dyn BatchSource>, ScanPushdown)> {
        let (schema, batches) = self.tables.get(&(table.0, projection.to_vec()))?;
        Some((
            Box::new(crate::scan::VecBatchSource::new(
                schema.clone(),
                batches.clone(),
            )),
            // The columns stored are EXACTLY the ones requested, in the
            // requested order — `projection_applied: true` is what tells
            // the `Scan` arm not to re-index them as if they addressed the
            // full, un-projected table.
            ScanPushdown {
                projection_applied: true,
                filters_applied: false,
            },
        ))
    }
}

/// Populate `into` with every `Scan` node's `(table, projection)` reachable
/// from `plan`, draining each through the real `tables` resolver exactly
/// once per distinct pair. See [`SnapshotResolver`]'s doc comment.
fn snapshot_scans(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    into: &mut SnapshotResolver,
) -> Result<(), BuildError> {
    if let LogicalPlan::Scan {
        table, projection, ..
    } = plan
    {
        let cols: Vec<usize> = projection.iter().map(|c| c.0 as usize).collect();
        let key = (table.0, cols.clone());
        if let std::collections::hash_map::Entry::Vacant(e) = into.tables.entry(key) {
            let (mut source, _pushed) = tables
                .open(*table, &cols, &[])
                .ok_or(BuildError::UnknownTable(*table))?;
            let schema = source.schema();
            let mut batches = Vec::new();
            while let Some(b) = source.next_batch()? {
                batches.push(b);
            }
            e.insert((schema, batches));
        }
    }
    // A subquery inside one of this node's expressions is a separate query
    // level, but its scans are still scans, and the per-row rebuild will
    // resolve them against this same snapshot — so they have to be in it.
    // `for_each_input` cannot reach them (a subplan hangs off an `Expr`, not
    // off the plan tree), which is why this second walk exists.
    //
    // Cloned rather than borrowed: `Expr::any`'s visitor sees each node
    // under a higher-ranked lifetime that cannot outlive the closure, and a
    // subplan clone at build time is cheap next to the scan it is about to
    // drain.
    let mut nested: Vec<LogicalPlan> = Vec::new();
    plan.for_each_expr(&mut |e| {
        e.any(&mut |x| {
            if let Expr::Subquery { subplan, .. } = x {
                nested.push(subplan.as_ref().clone());
            }
            false
        });
    });
    for subplan in &nested {
        snapshot_scans(subplan, tables, into)?;
    }

    let mut result = Ok(());
    plan.for_each_input(&mut |child| {
        if result.is_ok() {
            result = snapshot_scans(child, tables, into);
        }
    });
    result
}

fn build_error_to_exec(e: BuildError) -> ExecError {
    match e {
        BuildError::Exec(inner) => inner,
        other => ExecError::Internal(other.to_string()),
    }
}

/// Following the correlation convention `basin_plan::opt::decorrelate`
/// already establishes for correlated subqueries (`ColumnRef::relation ==
/// 1` marks a reference reaching the enclosing query's row — see that
/// module's docs), this builder adopts the same rule for
/// `LogicalPlan::LateralJoin`'s inner side: `Expr` has no dedicated "outer
/// reference" variant of its own, and a LATERAL subplan's correlation is
/// structurally the same "0 is my own scope, 1 is the enclosing one"
/// relationship a join's `on`/`filter` already uses.
const OUTER_REF: u16 = 1;

/// `AND` and `OR` have no `pg_operator` row — in PostgreSQL they are grammar,
/// not operators — so every file in this workspace that needs to *build* one
/// uses the same sentinel `OpId`s: `u32::MAX` for `AND`, one below it for
/// `OR`. See `basin_plan::opt::pushdown`'s `AND_OP`, `opt::simplify`'s
/// `AND_OP`/`OR_OP`/`NOT_OP`, `opt::decorrelate`'s `AND_OP`, and — the one
/// that actually *evaluates* what this file emits — [`crate::eval`]'s
/// `AND_OP`/`OR_OP`. Redefined here rather than imported because each is that
/// file's own private convention, but bit-for-bit identical on purpose.
///
/// The reason this file needs them at all is [`quantified_expr`]: `x op ANY
/// (subquery)` is a Kleene `OR` over one comparison per subquery row, and `x
/// op ALL (…)` the Kleene `AND`. `eval` implements both with arrow's
/// `or_kleene`/`and_kleene`, which is exactly SQL's three-valued rule — so
/// emitting these two sentinels is what makes `x > ANY (…, NULL)` come out
/// `NULL` rather than `false`. `and_or_sentinels_still_evaluate_kleene` pins
/// that, because if `eval`'s private copies ever moved, these would silently
/// become "unknown operator oid" — or, far worse, some real operator.
const AND_OP: basin_plan::OpId = basin_plan::OpId(basin_pgtype::Oid(u32::MAX));
/// See [`AND_OP`].
const OR_OP: basin_plan::OpId = basin_plan::OpId(basin_pgtype::Oid(u32::MAX - 1));

/// How many rows a quantified subquery (`IN`/`NOT IN`/`ANY`/`ALL`) may
/// produce before this builder declines it.
///
/// The strategy [`quantified_expr`] uses is to turn each of the subquery's
/// rows into a `Literal` and fold them into one expression — an `InList` for
/// `IN`/`NOT IN`, a Kleene `AND`/`OR` chain for `ANY`/`ALL`. That is exactly
/// right for the row counts real queries put on the right of an `IN`, and
/// exactly wrong for a million: the expression tree grows with the subquery,
/// and evaluating it costs one kernel call per element per batch. A set-based
/// operator is the answer at that scale; until one exists, declining is
/// honest and falling back to the other engine returns the right answer,
/// whereas silently building a million-node expression would not.
const MAX_QUANTIFIED_SUBQUERY_ROWS: usize = 10_000;

/// One correlated subquery collected by [`CorrSink`], and what the appended
/// column it becomes has to hold.
enum CorrSubplan {
    /// A correlated scalar subquery: the appended column is its value.
    Scalar(LogicalPlan),
    /// A correlated `IN`/`NOT IN`/`ANY`/`ALL`: the appended column is the
    /// three-valued boolean `decide` computes from the subquery's rows. The
    /// operand is already captured inside `decide` — it belongs to the
    /// ENCLOSING query level, not to `subplan`.
    Quantified {
        subplan: LogicalPlan,
        decide: QuantifiedDecider,
    },
}

/// Where a correlated subquery goes when the node being built can evaluate
/// one per row: [`bind_outer_rec`] replaces the subquery expression with a
/// reference to a column that does not exist yet, and pushes its `subplan`
/// here for the caller to turn into a [`CorrelatedScalar`] under the node.
/// `base_width` is that operator's input width, so the `k`th subquery
/// collected lands at output position `base_width + k` — the same position
/// the `Column` left behind names.
///
/// A `RefCell` rather than a `&mut Vec` because [`bind_outer_rec`]'s
/// traversal hands the same sink to several closures at once, which a
/// mutable borrow cannot express.
struct CorrSink {
    base_width: u16,
    subplans: RefCell<Vec<CorrSubplan>>,
}

/// Bind `expr` to a specific outer row for a `LateralJoin`'s per-row
/// rebuild or a `WITH RECURSIVE` recursive term's per-iteration rebuild
/// (module docs: "constant-folding the correlated column references into
/// literals, then building a fresh physical operator from that") — `outer`
/// is `None` everywhere except while rebuilding one of those two shapes —
/// AND fold every uncorrelated scalar subquery `expr` contains into its
/// (once-evaluated) result, regardless of `outer`; see
/// [`materialize_scalar_subquery`]. No longer a no-op clone when `outer` is
/// `None`: that used to mean "nothing to do", but a plain, non-LATERAL
/// `SELECT ... WHERE x = (SELECT ...)` reaches this exact path with `outer:
/// None` and still has a subquery to fold.
///
/// This entry point cannot evaluate a *correlated* scalar subquery, and
/// says so: one surviving the walk is [`BuildError::Unsupported`], never a
/// value folded in as if the correlation were not there. Only a caller that
/// has somewhere to put a per-row evaluation — [`bind_outer_collecting`]'s
/// callers, `Project` and `Filter` — can accept one.
fn bind_outer(
    expr: &Expr,
    outer: Outer<'_>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Expr, BuildError> {
    bind_outer_rec(expr, outer, None, tables, dml, budget, ctes, session)
}

/// [`bind_outer`] for the two nodes that can host a per-row evaluation:
/// every correlated scalar subquery found is pushed into `sink` and replaced
/// by a reference to the column [`CorrelatedScalar`] will produce for it.
fn bind_outer_collecting(
    expr: &Expr,
    outer: Outer<'_>,
    sink: &CorrSink,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Expr, BuildError> {
    bind_outer_rec(expr, outer, Some(sink), tables, dml, budget, ctes, session)
}

fn bind_sort_keys(
    keys: &[PlanSortKey],
    outer: Outer<'_>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Vec<PlanSortKey>, BuildError> {
    keys.iter()
        .map(|k| {
            Ok(PlanSortKey {
                expr: bind_outer(&k.expr, outer, tables, dml, budget, ctes, session)?,
                descending: k.descending,
                nulls_first: k.nulls_first,
            })
        })
        .collect()
}

/// The recursive worker behind [`bind_outer`]. Walks every `Expr` variant —
/// mirroring `basin_plan::Expr::for_each_child`'s own exhaustive match —
/// doing two unrelated rewrites in the same pass because both need the same
/// exhaustive traversal and neither cares about the other:
///
/// 1. Replacing `Column(relation == OUTER_REF)` with the corresponding
///    literal from `outer`'s row, when `outer` is `Some`.
/// 2. Resolving a scalar subquery (`Subquery { kind: Scalar, operand: None,
///    .. }`), by one of two routes depending on whether it is correlated —
///    see the Subquery arm below. This runs regardless of `outer`.
///
/// `Subquery`'s own `subplan` is otherwise left untouched by (1) — its
/// `operand`, which belongs to THIS query level, is not — the same "a
/// subquery is a separate query level" rule `Expr::for_each_child` already
/// states for exactly this reason; (2) is the one deliberate exception,
/// because resolving one IS building and running that separate query level.
///
/// Aggregate and window `ORDER BY` lists are left unbound by (1): a
/// correlated ordering inside an aggregate/window is a corner this builder
/// does not reach today. That is narrower than the general case rather than
/// silently wrong only because [`crate::eval`] refuses a `Column` whose
/// `relation` is not 0 — it reads column *positions*, and until that refusal
/// existed an unbound `OUTER_REF` was read as the local column at the same
/// index, which is a wrong answer wearing the right shape. This comment
/// claimed eval already errored before that was true; it is true now, and
/// `eval_column` is where.
fn bind_outer_rec(
    expr: &Expr,
    outer: Outer<'_>,
    corr: Option<&CorrSink>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Expr, BuildError> {
    let b = |e: &Expr| -> Result<Box<Expr>, BuildError> {
        Ok(Box::new(bind_outer_rec(
            e, outer, corr, tables, dml, budget, ctes, session,
        )?))
    };
    let v = |es: &[Expr]| -> Result<Vec<Expr>, BuildError> {
        es.iter()
            .map(|e| bind_outer_rec(e, outer, corr, tables, dml, budget, ctes, session))
            .collect()
    };
    let ob = |o: &Option<Box<Expr>>| -> Result<Option<Box<Expr>>, BuildError> {
        o.as_deref().map(b).transpose()
    };
    Ok(match expr {
        Expr::Column(c) if c.relation == OUTER_REF => match outer {
            Some((batch, row)) => outer_literal(batch.column(c.index as usize).as_ref(), row)?,
            None => expr.clone(),
        },
        Expr::Column(_) | Expr::Literal(..) | Expr::Parameter { .. } => expr.clone(),
        Expr::Unary { op, arg } => Expr::Unary {
            op: *op,
            arg: b(arg)?,
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op: *op,
            lhs: b(lhs)?,
            rhs: b(rhs)?,
        },
        Expr::Cast { arg, to, kind } => Expr::Cast {
            arg: b(arg)?,
            to: *to,
            kind: *kind,
        },
        Expr::Case {
            operand,
            whens,
            else_,
        } => Expr::Case {
            operand: ob(operand)?,
            whens: whens
                .iter()
                .map(|(w, t)| {
                    Ok((
                        bind_outer_rec(w, outer, corr, tables, dml, budget, ctes, session)?,
                        bind_outer_rec(t, outer, corr, tables, dml, budget, ctes, session)?,
                    ))
                })
                .collect::<Result<_, BuildError>>()?,
            else_: ob(else_)?,
        },
        Expr::Coalesce(xs) => Expr::Coalesce(v(xs)?),
        Expr::IsNull { arg, negated } => Expr::IsNull {
            arg: b(arg)?,
            negated: *negated,
        },
        Expr::BoolTest { arg, test } => Expr::BoolTest {
            arg: b(arg)?,
            test: *test,
        },
        Expr::DistinctFrom { lhs, rhs, negated } => Expr::DistinctFrom {
            lhs: b(lhs)?,
            rhs: b(rhs)?,
            negated: *negated,
        },
        Expr::InList { arg, list, negated } => Expr::InList {
            arg: b(arg)?,
            list: v(list)?,
            negated: *negated,
        },
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => Expr::Between {
            arg: b(arg)?,
            low: b(low)?,
            high: b(high)?,
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
            arg: b(arg)?,
            pattern: b(pattern)?,
            escape: ob(escape)?,
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expr::ScalarFn { func, args } => Expr::ScalarFn {
            func: *func,
            args: v(args)?,
        },
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => Expr::Aggregate {
            func: *func,
            args: v(args)?,
            distinct: *distinct,
            filter: ob(filter)?,
            order_by: order_by.clone(),
        },
        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func: *func,
            args: v(args)?,
            partition_by: v(partition_by)?,
            order_by: order_by.clone(),
            frame: frame.clone(),
        },
        Expr::SetReturning { func, args } => Expr::SetReturning {
            func: *func,
            args: v(args)?,
        },
        Expr::Subquery {
            kind,
            subplan,
            operand,
        } => {
            use basin_plan::SubqueryKind as K;
            // `IN`/`NOT IN`/`ANY`/`ALL` — a value tested against a whole
            // relation rather than folded to one. Handled before the scalar
            // fork below because they are the only kinds that carry an
            // `operand`, and because the operand has to be bound WITHOUT the
            // correlated sink (see inside).
            if let (K::In | K::NotIn | K::Any(_) | K::All(_), Some(raw_operand)) =
                (kind, operand.as_deref())
            {
                return bind_quantified_subquery(
                    *kind,
                    subplan,
                    raw_operand,
                    outer,
                    corr,
                    tables,
                    dml,
                    budget,
                    ctes,
                    session,
                );
            }
            let operand = ob(operand)?;
            if *kind == basin_plan::SubqueryKind::Scalar && operand.is_none() {
                // A correlated subquery ANYWHERE inside `subplan` is refused
                // before either route below is chosen, correlated outer
                // subquery or not. `OUTER_REF` says "outside my own FROM"
                // and nothing more: lowering collapses one level up and two
                // levels up onto the same tag, and documents the collapse
                // (`lower/select.rs`'s `ScopeResolver`). So a correlated
                // subquery two levels down may be reaching for this
                // subquery's row or for the row of the query holding it, and
                // whichever this builder picked it would be right by luck.
                // Refusing costs a fallback on a rare shape; guessing costs
                // a wrong answer on it.
                if contains_correlated_subquery(subplan) {
                    return Err(BuildError::Unsupported(
                        "correlated subquery nested inside another subquery".into(),
                    ));
                }
                // The fork this whole file used to get wrong. An
                // UNCORRELATED scalar subquery is a constant for the
                // statement and is folded once. A CORRELATED one is a
                // different value for every row and cannot be folded at
                // all — it goes to the sink, if the node being built has
                // one, and is otherwise refused outright.
                if !references_outer_row(subplan) {
                    materialize_scalar_subquery(subplan, tables, dml, budget, ctes, session)?
                } else {
                    let sink = match corr {
                        // `outer.is_some()` means we are already inside a
                        // per-row rebuild (a LATERAL inner side, a
                        // recursive term, or another correlated subquery),
                        // where the same tag collapse applies to THIS
                        // subquery's own correlation.
                        Some(sink) if outer.is_none() => sink,
                        _ => {
                            return Err(BuildError::Unsupported(
                                "correlated scalar subquery in this position".into(),
                            ))
                        }
                    };
                    let mut collected = sink.subplans.borrow_mut();
                    let index = sink.base_width + collected.len() as u16;
                    collected.push(CorrSubplan::Scalar(subplan.as_ref().clone()));
                    Expr::Column(basin_plan::ColumnRef {
                        relation: 0,
                        index,
                        name: format!("?correlated{index}?"),
                    })
                }
            } else {
                Expr::Subquery {
                    kind: *kind,
                    subplan: subplan.clone(),
                    operand,
                }
            }
        }
        Expr::ArrayLit(xs) => Expr::ArrayLit(v(xs)?),
        Expr::RowLit(xs) => Expr::RowLit(v(xs)?),
        Expr::Subscript { arg, indices } => Expr::Subscript {
            arg: b(arg)?,
            indices: indices
                .iter()
                .map(|s| {
                    Ok(match s {
                        basin_plan::Subscript::Index(e) => basin_plan::Subscript::Index(
                            bind_outer_rec(e, outer, corr, tables, dml, budget, ctes, session)?,
                        ),
                        basin_plan::Subscript::Slice { lower, upper } => {
                            basin_plan::Subscript::Slice {
                                lower: lower
                                    .as_ref()
                                    .map(|e| {
                                        bind_outer_rec(
                                            e, outer, corr, tables, dml, budget, ctes, session,
                                        )
                                    })
                                    .transpose()?,
                                upper: upper
                                    .as_ref()
                                    .map(|e| {
                                        bind_outer_rec(
                                            e, outer, corr, tables, dml, budget, ctes, session,
                                        )
                                    })
                                    .transpose()?,
                            }
                        }
                    })
                })
                .collect::<Result<_, BuildError>>()?,
        },
        Expr::FieldSelect { arg, field } => Expr::FieldSelect {
            arg: b(arg)?,
            field: *field,
        },
    })
}

/// Build and run `subplan` exactly once, folding its single-row,
/// single-column result into a `Literal` — Postgres's InitPlan: a subquery
/// with no correlated reference to the enclosing query is evaluated once
/// per statement, not once per row (the only version worth having, since
/// nothing about its result can vary row to row).
///
/// # `subplan` is uncorrelated, and here is what enforces that
///
/// This comment used to assert that `basin_plan::opt::decorrelate`
/// guaranteed no `Column(relation == OUTER_REF)` could reach here — "a
/// correlated scalar subquery it CAN decorrelate becomes a join, and
/// anything it declines has no correlation predicate to lean on." **That
/// was false**, and it made `SELECT id, (SELECT count(*) FROM t x WHERE
/// x.id = t.id) FROM t` answer `3, 3, 3` where Postgres answers `1, 1, 1`:
/// decorrelation's own docs scope all four of its transforms to a subquery
/// sitting in a `Filter` predicate and say a `Project` target list is left
/// untouched, so a correlated subquery in a `SELECT` list arrived here
/// still correlated and its correlation was silently dropped by building
/// with `outer: None`.
///
/// The guarantee now holds because [`bind_outer_rec`]'s `Subquery` arm — the
/// only caller — tests it directly with
/// [`basin_plan::opt::decorrelate::references_outer_row`] before calling
/// here, and routes a correlated subplan to a per-row
/// [`CorrelatedScalar`] (or refuses it) instead. That is the enforcement
/// point; this function assumes nothing on its own behalf beyond it, which
/// is why it can still build with `outer: None` and why any subquery nested
/// inside `subplan` gets the exact same one-shot treatment when
/// `build_inner` reaches it in turn.
///
/// Two Postgres rules for a scalar subquery, both enforced here: zero rows
/// is `NULL` — not an error, and not an empty result for the query this
/// subquery is embedded in, since this only ever produces a `Literal` and
/// never short-circuits anything above it — and more than one row is
/// SQLSTATE 21000 `cardinality_violation` ([`ExecError::CardinalityViolation`]),
/// not a silently-picked first row.
fn materialize_scalar_subquery(
    subplan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Expr, BuildError> {
    let mut op = build_inner(subplan, tables, dml, budget, ctes, session, None)?;
    let schema = op.schema();
    if schema.fields().len() != 1 {
        return Err(BuildError::Exec(ExecError::Internal(format!(
            "scalar subquery must return exactly one column, got {} — a planner bug",
            schema.fields().len()
        ))));
    }
    let ty = pg_type_for_arrow(schema.field(0).data_type())?;

    let mut result: Option<Expr> = None;
    while let Some(batch) = op.next_batch().map_err(BuildError::Exec)? {
        let col = batch.column(0).as_ref();
        for row in 0..batch.num_rows() {
            if result.is_some() {
                return Err(BuildError::Exec(ExecError::CardinalityViolation(
                    "more than one row returned by a subquery used as an expression".into(),
                )));
            }
            result = Some(outer_literal(col, row)?);
        }
    }
    Ok(result.unwrap_or(Expr::Literal(basin_plan::Datum::Null, ty)))
}

/// `x IN (SELECT …)`, `x NOT IN (…)`, `x op ANY (…)`, `x op ALL (…)`.
///
/// The same uncorrelated/correlated fork [`bind_outer_rec`]'s scalar arm
/// takes, for the same reason: an uncorrelated subquery is one relation for
/// the whole statement and is materialised once; a correlated one is a
/// different relation per outer row and goes to the sink, or is refused.
///
/// # The operand is bound WITHOUT the sink, on purpose
///
/// `operand` belongs to the enclosing query level, not to `subplan`, so it is
/// bound here — but through [`bind_outer`], not [`bind_outer_collecting`].
/// Were it bound with the sink, a correlated *scalar* subquery inside it
/// (`(SELECT max(n) FROM u WHERE u.tid = t.id) IN (SELECT …)`) would be
/// rewritten into a reference to a column that [`CorrelatedScalar`] appends
/// — a column that does not exist in the input batch the quantified
/// decision is evaluated against (`correlated.rs`'s `eval_one` slices the
/// operator's INPUT, before its own appends). It would read the wrong column
/// or run off the end. Binding without the sink turns that shape into a clean
/// `Unsupported` refusal instead.
#[allow(clippy::too_many_arguments)]
fn bind_quantified_subquery(
    kind: basin_plan::SubqueryKind,
    subplan: &LogicalPlan,
    raw_operand: &Expr,
    outer: Outer<'_>,
    corr: Option<&CorrSink>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Expr, BuildError> {
    let operand = bind_outer(raw_operand, outer, tables, dml, budget, ctes, session)?;

    // Same refusal, and the same reason, as the scalar arm's: `OUTER_REF`
    // says "outside my own FROM" and no more, so a correlated subquery nested
    // two levels down could be reaching for either level and this builder
    // would be right only by luck.
    if contains_correlated_subquery(subplan) {
        return Err(BuildError::Unsupported(
            "correlated subquery nested inside a quantified (IN/ANY/ALL) subquery".into(),
        ));
    }

    if !references_outer_row(subplan) {
        let values = materialize_subquery_column(subplan, tables, dml, budget, ctes, session)?;
        return quantified_expr(&kind, &operand, values);
    }

    let sink = match corr {
        Some(sink) if outer.is_none() => sink,
        _ => {
            return Err(BuildError::Unsupported(
                "correlated IN/NOT IN/ANY/ALL subquery in this position".into(),
            ))
        }
    };
    let mut collected = sink.subplans.borrow_mut();
    let index = sink.base_width + collected.len() as u16;
    collected.push(CorrSubplan::Quantified {
        subplan: subplan.clone(),
        decide: quantified_decider(kind, operand),
    });
    Ok(Expr::Column(basin_plan::ColumnRef {
        relation: 0,
        index,
        name: format!("?correlated{index}?"),
    }))
}

/// Build and run `subplan` once, folding its single column's rows into
/// `Literal`s — the quantified counterpart of
/// [`materialize_scalar_subquery`]'s InitPlan, differing only in that ANY row
/// count is legal here (there is no 21000 for `x IN (SELECT …)`) and that the
/// result is a list rather than a value.
fn materialize_subquery_column(
    subplan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Vec<Expr>, BuildError> {
    let mut op = build_inner(subplan, tables, dml, budget, ctes, session, None)?;
    let schema = op.schema();
    if schema.fields().len() != 1 {
        return Err(BuildError::Exec(ExecError::Internal(format!(
            "a subquery on the right of IN/ANY/ALL must return exactly one column, got {} — a \
             planner bug",
            schema.fields().len()
        ))));
    }
    let mut values = Vec::new();
    while let Some(batch) = op.next_batch().map_err(BuildError::Exec)? {
        let col = batch.column(0).as_ref();
        for row in 0..batch.num_rows() {
            if values.len() >= MAX_QUANTIFIED_SUBQUERY_ROWS {
                return Err(BuildError::Unsupported(format!(
                    "an IN/ANY/ALL subquery returning more than {MAX_QUANTIFIED_SUBQUERY_ROWS} \
                     rows"
                )));
            }
            values.push(outer_literal(col, row)?);
        }
    }
    Ok(values)
}

/// The correlated path's bridge back to [`quantified_expr`]: capture `kind`
/// and the already-bound `operand`, and hand `correlated.rs` a closure that
/// turns one outer row's subquery result into the deciding expression. See
/// `correlated.rs`'s "Quantified subqueries share this operator" for why the
/// two paths deliberately meet in the same function.
fn quantified_decider(kind: basin_plan::SubqueryKind, operand: Expr) -> QuantifiedDecider {
    Box::new(move |chunks: &[arrow_array::ArrayRef]| {
        let mut values = Vec::new();
        for chunk in chunks {
            for row in 0..chunk.len() {
                if values.len() >= MAX_QUANTIFIED_SUBQUERY_ROWS {
                    return Err(ExecError::Internal(format!(
                        "a correlated IN/ANY/ALL subquery returned more than \
                         {MAX_QUANTIFIED_SUBQUERY_ROWS} rows for one outer row"
                    )));
                }
                values.push(outer_literal(chunk.as_ref(), row).map_err(build_error_to_exec)?);
            }
        }
        quantified_expr(&kind, &operand, values).map_err(build_error_to_exec)
    })
}

/// **The three-valued logic, in one place.** `operand` against `values` —
/// the subquery's rows, already folded to literals — under `kind`.
///
/// Every rule below was read off a live PostgreSQL 18.2, not recalled; the
/// tests in this module carry the server's own output for each.
///
/// # A non-empty subquery
///
/// `IN`/`NOT IN` become [`Expr::InList`], which `eval` already implements
/// with `or_kleene` over `eq` (and, for `NOT IN`, the De Morgan dual:
/// `and_kleene` over `neq` — *not* a `not` wrapped around the positive form,
/// which is the same thing under Kleene logic but is the shape `eval` chose).
/// That gives, for free and without a second implementation:
///
/// - `3 IN (1, NULL)` → `or_kleene(false, NULL)` = **NULL**, not false.
/// - `3 NOT IN (1, NULL)` → `and_kleene(true, NULL)` = **NULL**, not true.
///   This is the footgun `opt::decorrelate` refuses `NotIn` over (its trap
///   1): an anti-join would answer `true` here. Nothing about that changes;
///   this path is not a join and does not have to prove the column
///   non-nullable, it simply computes the right answer.
/// - `1 NOT IN (1, NULL)` → `and_kleene(false, NULL)` = **false**. A match
///   still wins over the NULL, which is why `NOT IN` is not "NULL whenever a
///   NULL is present" either.
/// - `NULL IN (1, 2)` → **NULL**, from `eq`'s own null propagation.
///
/// `ANY`/`ALL` become a Kleene `OR`/`AND` fold of one `operand op value`
/// comparison per row, which is the same computation for the same reason —
/// `x = ANY (…)` *is* `IN`, `x <> ALL (…)` *is* `NOT IN`, and every other
/// operator follows the identical NULL rule (`3 > ANY (1, NULL)` is true,
/// because a true beats the NULL; `1 > ANY (1, NULL)` is NULL, because it
/// does not).
///
/// # An EMPTY subquery
///
/// The case that surprises people, and the one an `InList` cannot express at
/// all (`eval_in_list` rejects an empty list as a planner bug, since SQL's
/// grammar cannot produce one). PostgreSQL folds it to a constant:
/// `IN`/`ANY` → **false**, `NOT IN`/`ALL` → **true**, *including when the
/// operand is NULL* — `NULL NOT IN (SELECT … no rows)` is `true`, not
/// `NULL`. That is not an inconsistency: with no rows there is nothing to
/// compare against, so no unknown ever enters the fold, and an empty `OR` is
/// false while an empty `AND` is true.
fn quantified_expr(
    kind: &basin_plan::SubqueryKind,
    operand: &Expr,
    values: Vec<Expr>,
) -> Result<Expr, BuildError> {
    use basin_plan::SubqueryKind as K;

    if values.len() > MAX_QUANTIFIED_SUBQUERY_ROWS {
        return Err(BuildError::Unsupported(format!(
            "an IN/ANY/ALL subquery returning more than {MAX_QUANTIFIED_SUBQUERY_ROWS} rows"
        )));
    }

    let empty_answer = match kind {
        K::In | K::Any(_) => false,
        K::NotIn | K::All(_) => true,
        other => {
            return Err(BuildError::Exec(ExecError::Internal(format!(
                "{other:?} is not a quantified subquery — a builder bug"
            ))))
        }
    };
    if values.is_empty() {
        return Ok(Expr::Literal(
            basin_plan::Datum::Bool(empty_answer),
            PgType::BOOL,
        ));
    }

    Ok(match kind {
        K::In | K::NotIn => Expr::InList {
            arg: Box::new(operand.clone()),
            list: values,
            negated: matches!(kind, K::NotIn),
        },
        K::Any(op) | K::All(op) => {
            let connective = if matches!(kind, K::Any(_)) {
                OR_OP
            } else {
                AND_OP
            };
            let mut tests = values.into_iter().map(|v| Expr::Binary {
                op: *op,
                lhs: Box::new(operand.clone()),
                rhs: Box::new(v),
            });
            // `values` is non-empty (checked above), so `next` is `Some`.
            let first = tests
                .next()
                .expect("a non-empty value list yields a first comparison");
            tests.fold(first, |acc, test| Expr::Binary {
                op: connective,
                lhs: Box::new(acc),
                rhs: Box::new(test),
            })
        }
        // Unreachable: `empty_answer` above already rejected every other
        // kind, and it is computed before this match precisely so that the
        // two cannot drift.
        other => {
            return Err(BuildError::Exec(ExecError::Internal(format!(
                "{other:?} is not a quantified subquery — a builder bug"
            ))))
        }
    })
}

/// The uncorrelated case's opposite number: wrap `child` in a
/// [`CorrelatedScalar`] that evaluates each collected `subplan` once per
/// row, appending one column each at positions `base_width..`.
///
/// The per-row factory is built exactly the way [`LogicalPlan::LateralJoin`]'s
/// is — a [`SnapshotResolver`] to satisfy the `'static` closure, a
/// single all-NULL probe row to learn the subquery's output type before any
/// real row exists — because it is the same problem: a plan that has to be
/// rebuilt against a row that does not exist yet at build time. See
/// [`SnapshotResolver`]'s docs for what that trades away (pushdown, on this
/// path only).
fn build_correlated_scalars(
    child: Box<dyn Operator>,
    subplans: Vec<CorrSubplan>,
    base_width: u16,
    tables: &dyn TableResolver,
    budget: usize,
    ctes: &CteRegistry,
    session: &SessionRef,
) -> Result<Box<dyn Operator>, BuildError> {
    let probe = null_probe_row(&child.schema())?;

    let mut subqueries = Vec::with_capacity(subplans.len());
    for (k, collected) in subplans.into_iter().enumerate() {
        let (subplan, kind) = match collected {
            CorrSubplan::Scalar(subplan) => (subplan, CorrelatedKind::Scalar),
            CorrSubplan::Quantified { subplan, decide } => {
                (subplan, CorrelatedKind::Quantified(decide))
            }
        };
        if subplan.is_mutating() {
            return Err(BuildError::Unsupported(
                "data-modifying statement inside a subquery".into(),
            ));
        }
        let mut snapshot = SnapshotResolver::default();
        snapshot_scans(&subplan, tables, &mut snapshot)?;
        let snapshot = Rc::new(snapshot);

        let schema = build_inner(
            &subplan,
            snapshot.as_ref(),
            None,
            budget,
            ctes,
            session,
            Some((&probe, 0)),
        )?
        .schema();
        if schema.fields().len() != 1 {
            return Err(BuildError::Exec(ExecError::Internal(format!(
                "a subquery used as an expression must return exactly one column, got {} — a \
                 planner bug",
                schema.fields().len()
            ))));
        }
        // A quantified subquery contributes the three-valued BOOLEAN its
        // decider computes, not a value of the subquery's own column type —
        // so the probe build above is run for its schema CHECK and for the
        // errors it surfaces early, and its column type is then deliberately
        // discarded.
        let data_type = match &kind {
            CorrelatedKind::Scalar => schema.field(0).data_type().clone(),
            CorrelatedKind::Quantified(_) => arrow_schema::DataType::Boolean,
        };

        let snapshot_for_factory = Rc::clone(&snapshot);
        let ctes_for_factory = Rc::clone(ctes);
        // See the lateral factory: per-row rebuilds must stay in-session.
        let session_for_factory = SessionRef::clone(session);
        let plan_for_factory = subplan;
        let factory: InnerFactory = Box::new(move |row_batch: &RecordBatch, idx: usize| {
            build_inner(
                &plan_for_factory,
                snapshot_for_factory.as_ref(),
                None,
                budget,
                &ctes_for_factory,
                &session_for_factory,
                Some((row_batch, idx)),
            )
            .map_err(build_error_to_exec)
        });
        subqueries.push(CorrelatedSubquery {
            factory,
            data_type,
            // Matches the name `bind_outer_rec` gave the `Column` that
            // reads this position, so a schema dump and an expression dump
            // agree with each other.
            name: format!("?correlated{}?", base_width as usize + k),
            kind,
        });
    }
    Ok(Box::new(
        CorrelatedScalar::new(child, subqueries).in_session(SessionRef::clone(session)),
    ))
}

/// A one-row, all-NULL batch shaped like `schema` — the probe a correlated
/// rebuild is handed to learn its subplan's output type before any real outer
/// row exists. Used by the `LateralJoin` arm and by
/// [`build_correlated_scalars`], which have the identical problem.
///
/// # Every field is forced NULLABLE, and that is the whole point
///
/// `RecordBatch::try_new` validates nullability, so building this batch
/// against the child's own schema failed outright the moment that schema
/// carried a `NOT NULL` column — and `CREATE TABLE t (id BIGINT NOT NULL,
/// …)` is about as ordinary as SQL gets. The error was
/// `Invalid argument error: Column 'id' is declared as non-nullable but
/// contains null values`, raised while *building*, so EVERY correlated
/// subquery over such a table declined. Measured against the probe corpus,
/// not assumed: it is what both
/// `SELECT id FROM t WHERE amt > ALL (SELECT n FROM u WHERE u.tid = t.id)`
/// and `SELECT id, (SELECT count(*) FROM u WHERE u.tid = t.id) FROM t` were
/// failing on, the second of which is the very query `correlated.rs` exists
/// for.
///
/// Relaxing nullability here is sound because no VALUE in this batch is ever
/// observed: [`bind_outer_rec`] turns each referenced column into a typed
/// NULL `Literal` via [`outer_literal`] — which reads the array's DATA TYPE
/// and its null bit, nothing else — and the operator built from it is thrown
/// away as soon as its schema has been read. Only the types have to be real,
/// and forcing nullability does not change a type.
fn null_probe_row(schema: &SchemaRef) -> Result<RecordBatch, BuildError> {
    let fields: Vec<arrow_schema::Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    let nullable: SchemaRef = Arc::new(arrow_schema::Schema::new(fields));
    let cols: Vec<arrow_array::ArrayRef> = nullable
        .fields()
        .iter()
        .map(|f| arrow_array::new_null_array(f.data_type(), 1))
        .collect();
    RecordBatch::try_new(nullable, cols)
        .map_err(|e| BuildError::Exec(ExecError::Internal(e.to_string())))
}

/// Read one value out of an Arrow array as an [`Expr::Literal`]. Shared by
/// [`bind_outer_rec`]'s `LateralJoin`/`WITH RECURSIVE` outer-row binding and
/// [`materialize_scalar_subquery`]'s InitPlan result — both need "one Arrow
/// value, at a known row, folded into a literal". Only the handful of
/// primitive types either caller is expected to see — matching
/// [`pg_type_for_arrow`]'s coverage — are supported; anything else is
/// refused rather than guessed at.
fn outer_literal(col: &dyn arrow_array::Array, row: usize) -> Result<Expr, BuildError> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    };
    use basin_plan::Datum;

    let ty = pg_type_for_arrow(col.data_type())?;
    if col.is_null(row) {
        return Ok(Expr::Literal(Datum::Null, ty));
    }
    let datum = match col.data_type() {
        arrow_schema::DataType::Boolean => Datum::Bool(
            col.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Int16 => Datum::Int16(
            col.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Int32 => Datum::Int32(
            col.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Int64 => Datum::Int64(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Float32 => Datum::Float32(
            col.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Float64 => Datum::Float64(
            col.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Utf8 => Datum::Utf8(
            col.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        other => {
            return Err(BuildError::Unsupported(format!(
                "a correlated reference or scalar subquery result of Arrow type {other:?}"
            )))
        }
    };
    Ok(Expr::Literal(datum, ty))
}

/// The [`PgType`] whose [`basin_pgtype::physical`] round-trips to `dt` —
/// the reverse of the direction `eval.rs`'s `eval_literal` normally needs,
/// required here because [`outer_literal`]'s source column only carries an
/// Arrow type, and the literal replacing it must carry a `PgType` that maps
/// back to that exact Arrow type or `eval` will build the wrong kind of
/// array for it.
fn pg_type_for_arrow(dt: &arrow_schema::DataType) -> Result<PgType, BuildError> {
    use arrow_schema::DataType;
    Ok(match dt {
        DataType::Boolean => PgType::BOOL,
        DataType::Int16 => PgType::INT2,
        DataType::Int32 => PgType::INT4,
        DataType::Int64 => PgType::INT8,
        DataType::Float32 => PgType::FLOAT4,
        DataType::Float64 => PgType::FLOAT8,
        DataType::Utf8 => PgType::TEXT,
        other => {
            return Err(BuildError::Unsupported(format!(
                "a correlated reference or scalar subquery result of Arrow type {other:?}"
            )))
        }
    })
}

/// Translate a scan's filters from projection-relative column positions — the
/// logical convention, see the `LogicalPlan::Scan` arm of [`build_inner`] —
/// into positions in the source's own schema, for a source that declined the
/// projection and so hands back full-table batches.
///
/// An identity projection needs no translation at all, which covers every plan
/// that has not been through projection pruning: lowering always emits
/// `0..n`, and the two index spaces coincide exactly then.
///
/// A filter index with no entry in `cols` cannot be translated. That is a
/// malformed plan rather than a shape to support, and it is left untouched
/// rather than guessed at — `Scan` will then either read whatever that index
/// names in the source schema (unchanged from before this translation
/// existed) or reject it as out of range, both of which are better than
/// inventing a mapping.
fn filters_to_source_positions(filters: Vec<Expr>, cols: &[usize]) -> Vec<Expr> {
    if cols.iter().enumerate().all(|(pos, &c)| pos == c) {
        return filters;
    }
    let remap: Vec<Option<u16>> = cols.iter().map(|&c| Some(c as u16)).collect();
    filters
        .into_iter()
        .map(|f| {
            let untranslatable = f.any(&mut |e| {
                matches!(e, Expr::Column(c) if c.relation == 0 && c.index as usize >= remap.len())
            });
            if untranslatable {
                f
            } else {
                basin_plan::opt::projection::remap_expr(&f, 0, &remap)
            }
        })
        .collect()
}

/// The column position an expression refers to, if it is a plain column.
fn column_index(e: &Expr) -> Option<usize> {
    match e {
        Expr::Column(c) => Some(c.index as usize),
        _ => None,
    }
}

/// A literal SQL `NULL`, for the `LIMIT`/`OFFSET` bounds.
///
/// `LIMIT ALL` is not a distinct parse-tree shape: Postgres lowers it to a
/// `LIMIT` whose count is a NULL constant, which arrives here as
/// `Literal(Datum::Null, _)`. `const_usize` cannot read a count out of that
/// and the builder refused the whole statement as a "non-constant LIMIT" —
/// so `SELECT id FROM t LIMIT ALL`, which is ordinary SQL, fell back.
///
/// The bound is genuinely absent, not zero: measured live on PostgreSQL
/// 18.2, `LIMIT ALL`, `LIMIT NULL` and `OFFSET NULL` over a five-row input
/// all return all five rows. The same spelling covers all three because
/// Postgres itself does not distinguish them past the parser.
fn is_null_literal(e: &Expr) -> bool {
    matches!(e, Expr::Literal(basin_plan::Datum::Null, _))
}

/// A constant non-negative integer, for `LIMIT`.
fn const_usize(e: &Expr) -> Option<usize> {
    match e {
        Expr::Literal(basin_plan::Datum::Int64(v), _) if *v >= 0 => Some(*v as usize),
        Expr::Literal(basin_plan::Datum::Int32(v), _) if *v >= 0 => Some(*v as usize),
        _ => None,
    }
}

fn sort_keys(keys: &[PlanSortKey]) -> Result<Vec<SortKey>, BuildError> {
    keys.iter()
        .map(|k| {
            Ok(SortKey {
                column: column_index(&k.expr).ok_or(BuildError::NonColumnKey("ORDER BY"))?,
                descending: k.descending,
                nulls_first: k.nulls_first,
            })
        })
        .collect()
}

/// Map a Postgres aggregate function OID to the physical accumulator.
///
/// These are real `pg_proc` OIDs, read from a live PostgreSQL 18 rather than
/// invented — `count(*)` is 2803, and each of sum/min/max/avg has a distinct
/// OID per input type because Postgres resolves aggregates by signature. The
/// physical layer only cares which accumulator to run, so many OIDs collapse to
/// one variant here; the type-specific behaviour lives in the accumulator.
///
/// `manual_range_patterns` is allowed rather than applied. Clippy is right
/// that `2148 | 2149 | … | 2153` *is* `2148..=2153`; the point is that the
/// two say different things to a reader. The enumerated form is a list of
/// oids that were each read back from `pg_proc` by name; the range asserts
/// that nothing else was ever assigned inside it, which is a claim about
/// PostgreSQL's oid allocation that this table has no way to check and that
/// the next person cannot verify without a server. This file has already
/// been wrong about exactly that once — see `window_func_of`.
#[allow(clippy::manual_range_patterns)]
fn agg_func_of(oid: u32) -> Option<AggFunc> {
    Some(match oid {
        2803 => AggFunc::CountStar,
        2107 | 2108 | 2111 | 2114 => AggFunc::Sum,
        2115 | 2116 | 2120 | 2130 => AggFunc::Max,
        2131 | 2132 | 2136 | 2146 => AggFunc::Min,
        2100 | 2101 | 2103 | 2105 => AggFunc::Avg,
        // `array_agg(anynonarray)` and `array_agg(anyarray)` — read
        // individually from a live PostgreSQL 18.2 `pg_proc`, not
        // transcribed as a block (see `window_func_of`'s doc for why that
        // burned this file before: 3108/3110/3111/3112 were assigned to
        // the wrong window functions because lag/lead's overloads
        // interleave in the real table).
        2335 | 4053 => AggFunc::ArrayAgg,
        // `string_agg(text, text)` is 3538, `string_agg(bytea, bytea)` is
        // 3545 — not adjacent to array_agg's OIDs or to each other. The
        // `delim_col` placeholder here is always overwritten in `agg_spec`
        // once the call's second argument (the delimiter, resolved to a
        // column per row) is known; `agg_func_of` only sees the OID, not
        // the argument list.
        3538 | 3545 => AggFunc::StringAgg { delim_col: 0 },

        // ─── The variance family ─────────────────────────────────────────
        //
        // Six overloads each — int8/int4/int2/float4/float8/numeric, in that
        // declaration order — and each block happens to be contiguous in
        // `pg_proc`. They are spelled out one oid at a time anyway, rather
        // than as `2148..=2153`, for the reason recorded above `lag`/`lead`
        // in `window_func_of`: a range asserts that nothing else was ever
        // assigned inside it, which is a claim about oids this table cannot
        // check. Every oid below was read back individually from a live
        // PostgreSQL 18.2 with `proname` and `pg_get_function_arguments`.
        //
        // `variance` is Postgres's own alias for `var_samp` and `stddev` for
        // `stddev_samp` — separate oids, separate `pg_proc` rows, identical
        // accumulator and finalizer. See `VarKind`.
        2148 | 2149 | 2150 | 2151 | 2152 | 2153 => AggFunc::Variance(VarKind::VarSamp), // variance
        2641 | 2642 | 2643 | 2644 | 2645 | 2646 => AggFunc::Variance(VarKind::VarSamp), // var_samp
        2718 | 2719 | 2720 | 2721 | 2722 | 2723 => AggFunc::Variance(VarKind::VarPop),  // var_pop
        2154 | 2155 | 2156 | 2157 | 2158 | 2159 => AggFunc::Variance(VarKind::StddevSamp), // stddev
        2712 | 2713 | 2714 | 2715 | 2716 | 2717 => AggFunc::Variance(VarKind::StddevSamp), // stddev_samp
        2724 | 2725 | 2726 | 2727 | 2728 | 2729 => AggFunc::Variance(VarKind::StddevPop), // stddev_pop

        // ─── Bitwise and boolean ─────────────────────────────────────────
        //
        // Four overloads each: int2/int4/int8/bit. The `bit` ones — 2242,
        // 2243 and 6167 — are deliberately absent: Basin has no `bit` type,
        // so resolving them here would map a call the rest of the stack
        // cannot represent. Falling back is the right answer for those.
        //
        // Note `bit_xor` is 6164..6166, nowhere near `bit_and`/`bit_or`'s
        // 2236..2241 — it was added long after them (PG 14). Assuming
        // adjacency here would have mapped it onto `int2vectorout`'s
        // neighbourhood.
        2236 | 2238 | 2240 => AggFunc::BitAnd,
        2237 | 2239 | 2241 => AggFunc::BitOr,
        6164 | 6165 | 6166 => AggFunc::BitXor,
        // 2519 is `every(boolean)` — the SQL-standard spelling of `bool_and`,
        // a separate `pg_proc` row with the same transition function. Wired
        // alongside it because the alias relationship is real (checked live:
        // `every` is `prokind = 'a'`, one boolean argument, oid 2519) and
        // because leaving it out would make `every(flag)` fall back while
        // `bool_and(flag)` served, for no reason a user could see. There is
        // no `every`-shaped `bool_or`; the standard spells that one `some`,
        // which PostgreSQL does not define as an aggregate at all.
        2517 | 2519 => AggFunc::BoolAnd,
        2518 => AggFunc::BoolOr,

        // ─── The two-argument statistical family ─────────────────────────
        //
        // One oid each — all take `(float8, float8)`, so there are no
        // overloads to interleave. Postgres's argument order is `f(Y, X)`,
        // dependent variable first; the `x_col: 0` placeholder here is
        // always overwritten in `agg_spec` from `args[1]`, exactly as
        // `StringAgg`'s `delim_col` is, because `agg_func_of` sees only the
        // oid and not the argument list.
        //
        // The oids run in `pg_proc` order, which is *not* alphabetical and
        // not grouped by what the function computes: `regr_r2` (2824) sits
        // between `regr_avgy` and `regr_slope`, and `corr` (2829) after the
        // two `covar_*`. Each was read back by name from the live server.
        2818 => AggFunc::Regr {
            kind: RegrKind::Count,
            x_col: 0,
        }, // regr_count
        2819 => AggFunc::Regr {
            kind: RegrKind::Sxx,
            x_col: 0,
        }, // regr_sxx
        2820 => AggFunc::Regr {
            kind: RegrKind::Syy,
            x_col: 0,
        }, // regr_syy
        2821 => AggFunc::Regr {
            kind: RegrKind::Sxy,
            x_col: 0,
        }, // regr_sxy
        2822 => AggFunc::Regr {
            kind: RegrKind::AvgX,
            x_col: 0,
        }, // regr_avgx
        2823 => AggFunc::Regr {
            kind: RegrKind::AvgY,
            x_col: 0,
        }, // regr_avgy
        2824 => AggFunc::Regr {
            kind: RegrKind::R2,
            x_col: 0,
        }, // regr_r2
        2825 => AggFunc::Regr {
            kind: RegrKind::Slope,
            x_col: 0,
        }, // regr_slope
        2826 => AggFunc::Regr {
            kind: RegrKind::Intercept,
            x_col: 0,
        }, // regr_intercept
        2827 => AggFunc::Regr {
            kind: RegrKind::CovarPop,
            x_col: 0,
        }, // covar_pop
        2828 => AggFunc::Regr {
            kind: RegrKind::CovarSamp,
            x_col: 0,
        }, // covar_samp
        2829 => AggFunc::Regr {
            kind: RegrKind::Corr,
            x_col: 0,
        }, // corr

        // 3988 `percent_rank(VARIADIC "any" ORDER BY VARIADIC "any")` and
        // 3990 `cume_dist(...)` are NOT mapped here, and must not be. They
        // share a name with the window functions wired in `window_func_of`
        // (3103, 3104) but they are hypothetical-set aggregates: they answer
        // "where would this hypothetical row rank", taking the probe row as
        // arguments and a `WITHIN GROUP (ORDER BY …)` clause. Wiring them to
        // the window implementations would answer a different question with
        // the same column name. Same reason `percentile_cont` is absent —
        // ordered-set aggregates have no representation in `AggregateSpec`.
        _ => return None,
    })
}

/// Translate a logical aggregate into the physical operator's spec.
///
/// The physical aggregate keys on column positions rather than expressions —
/// an argument more complex than a column reference has to be materialised by a
/// `Project` beneath the aggregate. That is the optimizer's job, so an
/// unexpected shape is reported rather than improvised around.
fn agg_spec(e: &Expr, alias: &str) -> Result<AggregateSpec, BuildError> {
    match e {
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => {
            if !order_by.is_empty() {
                return Err(BuildError::Unsupported(
                    "ORDER BY inside an aggregate".into(),
                ));
            }
            let mut f = agg_func_of(func.0.get()).ok_or_else(|| {
                BuildError::Unsupported(format!("aggregate function with OID {}", func.0.get()))
            })?;
            let input_col = match args.first() {
                None => None,
                Some(a) => {
                    // `count(x)` counts non-null values; `count(*)` counts
                    // rows. They share no OID, but a Count with an argument
                    // must not be run as CountStar or nulls would be counted.
                    if f == AggFunc::CountStar {
                        f = AggFunc::Count;
                    }
                    Some(column_index(a).ok_or(BuildError::NonColumnKey("aggregate"))?)
                }
            };
            // `string_agg(value, delimiter)`'s delimiter is read per row
            // from a resolved column (`AggFunc::StringAgg::delim_col`'s own
            // doc), not carried as a constant — a literal delimiter still
            // arrives here as a column reference because the optimizer
            // materialises any non-column aggregate argument beneath the
            // aggregate first (see `agg_spec`'s own doc). `agg_func_of`
            // cannot fill this in itself since it only sees the OID, not
            // the argument list.
            if let AggFunc::StringAgg { .. } = f {
                let delim_expr = args.get(1).ok_or_else(|| {
                    BuildError::Unsupported("string_agg without a delimiter argument".into())
                })?;
                let delim_col = column_index(delim_expr)
                    .ok_or(BuildError::NonColumnKey("string_agg delimiter"))?;
                f = AggFunc::StringAgg { delim_col };
            }
            // `regr_slope(y, x)` and its eleven relatives read a *second*
            // column per row, carried on the variant for the same reason
            // `string_agg`'s delimiter is (`AggFunc::Regr`'s doc). `input_col`
            // above already holds `args[0]`, which is Y — Postgres puts the
            // dependent variable first — so the column resolved here is X.
            // Getting the two the wrong way round is not a fallback but a
            // wrong answer: `regr_slope` and `regr_avgx` are both defined for
            // the swapped arguments and both return a different number.
            if let AggFunc::Regr { kind, .. } = f {
                let x_expr = args.get(1).ok_or_else(|| {
                    BuildError::Unsupported("regr_*/corr/covar_* without a second argument".into())
                })?;
                let x_col = column_index(x_expr)
                    .ok_or(BuildError::NonColumnKey("regr_* second argument"))?;
                f = AggFunc::Regr { kind, x_col };
            }
            let filter_col = match filter {
                None => None,
                Some(x) => {
                    Some(column_index(x).ok_or(BuildError::NonColumnKey("aggregate FILTER"))?)
                }
            };
            Ok(AggregateSpec {
                func: f,
                input_col,
                distinct: *distinct,
                filter_col,
                alias: alias.to_string(),
            })
        }
        _ => Err(BuildError::Unsupported(
            "non-aggregate expression in an aggregate list".into(),
        )),
    }
}

/// Postgres OIDs for the window functions, from `pg_proc` on a live server.
///
/// Ranking and offset functions are true window functions with their own
/// OIDs; sum/count/min/max/avg are ordinary aggregates used in a window
/// context and reuse the aggregate OIDs, which is why this falls through to
/// the aggregate mapping rather than duplicating it.
fn window_func_of(oid: u32) -> Option<WindowFunc> {
    Some(match oid {
        3100 => WindowFunc::RowNumber,
        3101 => WindowFunc::Rank,
        3102 => WindowFunc::DenseRank,
        // 3103/3104/3105, read back from the live server with `prokind` in
        // hand: all three are `prokind = 'w'`, true window functions, and
        // 3103/3104 take no arguments at all.
        //
        // `percent_rank` and `cume_dist` have a SECOND spelling in pg_proc —
        // 3988 and 3990 — which is `prokind = 'a'` with `pg_aggregate.aggkind
        // = 'h'`: the hypothetical-set aggregates `percent_rank(VARIADIC
        // "any") WITHIN GROUP (ORDER BY VARIADIC "any")`. Same names, same
        // return type, entirely different question — they rank a hypothetical
        // row supplied as an argument against the group, rather than ranking
        // each actual row within its partition. Mapping them onto these
        // implementations would produce a plausible float per group and be
        // wrong; `basin-pgtype` omits them on purpose and pins the omission
        // with a test, so they cannot arrive here anyway. Recorded because
        // the adjacency of the names, not of the oids, is the trap.
        3103 => WindowFunc::PercentRank,
        3104 => WindowFunc::CumeDist,
        // `ntile(integer)`'s bucket count rides on the variant rather than in
        // `WindowSpec::arg_col` — same convention as `AggFunc::StringAgg`'s
        // `delim_col`, and the operator actively rejects a spec that puts it
        // in `arg_col`. The placeholder `0` here is always overwritten in
        // `window_spec`, which is the only place the argument list is visible.
        3105 => WindowFunc::Ntile { buckets_col: 0 },
        // The oids below are the real ones, dumped from PostgreSQL 18.2:
        //
        //   3106 lag(anyelement)                    3109 lead(anyelement)
        //   3107 lag(anyelement, integer)           3110 lead(anyelement, integer)
        //   3108 lag(anycompatible, int, anycompat) 3111 lead(anycompatible, ...)
        //   3112 first_value   3113 last_value      3114 nth_value
        //
        // This table previously read 3108 as lead, 3110 as first_value, 3111 as
        // last_value and 3112 as nth_value — every one of them off by the size
        // of lag's and lead's overload blocks. `basin-pgtype`'s func.rs had the
        // right oids all along, so the two tables disagreed, and the visible
        // symptom was only that `first_value(x)` fell back: it resolved to 3112,
        // arrived here as NthValue, and failed the arity check.
        //
        // The invisible symptom was the dangerous one. `lead(x, n)` resolves to
        // 3110, which this table called FirstValue — an arity the builder
        // accepts. That is a wrong answer, not a fallback.
        3106 | 3107 => WindowFunc::Lag,
        3109 | 3110 => WindowFunc::Lead,
        3112 => WindowFunc::FirstValue,
        3113 => WindowFunc::LastValue,
        3114 => WindowFunc::NthValue,
        // 3108 and 3111 — lag/lead's THREE-argument form, which supplies a
        // default for rows with no peer at the offset — are deliberately absent.
        // `window_spec` reads args[0] and args[1] and has nowhere to put a
        // default, so mapping them here would silently drop it and return NULL
        // where Postgres returns the default. Falling back is the correct
        // answer until the operator can carry one.
        2803 => WindowFunc::CountStar,
        2107 | 2108 | 2111 | 2114 => WindowFunc::Sum,
        2115 | 2116 | 2120 | 2130 => WindowFunc::Max,
        2131 | 2132 | 2136 | 2146 => WindowFunc::Min,
        // 2102 is avg(smallint), which was missing — the others are
        // avg(bigint)/avg(integer)/avg(numeric)/avg(float8).
        2100 | 2101 | 2102 | 2103 | 2105 => WindowFunc::Avg,
        _ => return None,
    })
}

/// The PARTITION BY / ORDER BY shared by every window expression in one node.
///
/// They must agree: the operator sorts nothing and computes one partitioning,
/// so two windows with different keys in one node would silently produce wrong
/// answers for one of them. Disagreement is reported rather than resolved.
fn window_keys(windows: &[Expr]) -> Result<(Vec<usize>, Vec<OrderKey>), BuildError> {
    let mut part: Option<Vec<usize>> = None;
    let mut ord: Option<Vec<OrderKey>> = None;
    for w in windows {
        let Expr::Window {
            partition_by,
            order_by,
            ..
        } = w
        else {
            return Err(BuildError::Unsupported(
                "non-window expression in a window list".into(),
            ));
        };
        let p = partition_by
            .iter()
            .map(|e| column_index(e).ok_or(BuildError::NonColumnKey("PARTITION BY")))
            .collect::<Result<Vec<_>, _>>()?;
        let o = order_by
            .iter()
            .map(|k| {
                Ok(OrderKey {
                    column: column_index(&k.expr)
                        .ok_or(BuildError::NonColumnKey("window ORDER BY"))?,
                    descending: k.descending,
                    nulls_first: k.nulls_first,
                })
            })
            .collect::<Result<Vec<_>, BuildError>>()?;
        if let Some(prev) = &part {
            if *prev != p {
                return Err(BuildError::Unsupported(
                    "window expressions with differing PARTITION BY in one node".into(),
                ));
            }
        }
        if let Some(prev) = &ord {
            let same = prev.len() == o.len()
                && prev
                    .iter()
                    .zip(&o)
                    .all(|(a, b)| a.column == b.column && a.descending == b.descending);
            if !same {
                return Err(BuildError::Unsupported(
                    "window expressions with differing ORDER BY in one node".into(),
                ));
            }
        }
        part = Some(p);
        ord = Some(o);
    }
    Ok((part.unwrap_or_default(), ord.unwrap_or_default()))
}

fn window_spec(e: &Expr, alias: &str) -> Result<WindowSpec, BuildError> {
    let Expr::Window {
        func, args, frame, ..
    } = e
    else {
        return Err(BuildError::Unsupported(
            "non-window expression in a window list".into(),
        ));
    };
    let mut f = window_func_of(func.0.get()).ok_or_else(|| {
        BuildError::Unsupported(format!("window function with OID {}", func.0.get()))
    })?;
    // count(x) counts non-null values; count(*) counts rows. Same OID split as
    // the aggregate path, and the same consequence if conflated.
    let arg_col = match args.first() {
        None => None,
        Some(a) => {
            if f == WindowFunc::CountStar {
                f = WindowFunc::Count;
            }
            Some(column_index(a).ok_or(BuildError::NonColumnKey("window argument"))?)
        }
    };
    // `ntile(n)` reads `n` from a resolved column carried on the WindowFunc
    // variant, not from `arg_col` — `window.rs`'s `resolve_window` returns a
    // TypeMismatch if `arg_col` is set for an Ntile, so this move is required
    // rather than cosmetic. The operator reads that column ONCE, at the
    // partition's first row (verified live against a per-row-varying `n`), so
    // it is a per-partition constant even though it is materialized per row.
    let (f, arg_col) = match f {
        WindowFunc::Ntile { .. } => {
            let buckets_col = arg_col.ok_or_else(|| {
                BuildError::Unsupported("ntile() without a bucket-count argument".into())
            })?;
            (WindowFunc::Ntile { buckets_col }, None)
        }
        other => (other, arg_col),
    };
    let offset_col = match args.get(1) {
        None => None,
        Some(a) => Some(column_index(a).ok_or(BuildError::NonColumnKey("window offset"))?),
    };
    let default_col = match args.get(2) {
        None => None,
        Some(a) => Some(column_index(a).ok_or(BuildError::NonColumnKey("window default"))?),
    };
    Ok(WindowSpec {
        func: f,
        arg_col,
        offset_col,
        default_col,
        nth_col: if f == WindowFunc::NthValue {
            offset_col
        } else {
            None
        },
        frame: frame_of(frame)?,
        alias: alias.to_string(),
    })
}

/// What a materializing `Project` beneath a `WindowAgg` added, and therefore
/// what has to be taken back off above it — see
/// [`materialize_window_args`].
struct WindowArgTrim {
    /// Width of the `WindowAgg`'s input *before* materialization.
    base_width: usize,
    /// How many argument columns were appended.
    extra: usize,
    /// How many window functions the node computes.
    windows: usize,
}

/// One column of `schema` as a `(Expr::Column, name)` pair for a `Project`.
fn passthrough(schema: &SchemaRef, index: usize) -> Result<(Expr, String), BuildError> {
    let name = schema.field(index).name().clone();
    let index = u16::try_from(index)
        .map_err(|_| BuildError::Unsupported("projection wider than u16 columns".into()))?;
    Ok((
        Expr::Column(ColumnRef {
            relation: 0,
            index,
            name: name.clone(),
        }),
        name,
    ))
}

/// Materialize any non-column window *argument* into a `Project` beneath the
/// `WindowAgg`, rewriting `windows` in place to reference the new column.
///
/// [`window_spec`] resolves every argument with `column_index(...).ok_or(
/// NonColumnKey)`, exactly as `agg_spec` does — and for aggregates,
/// `basin-plan`'s `lower/select.rs::materialize_agg_inputs` already satisfies
/// that contract by inserting a `Project` under the `Aggregate`. There is no
/// companion pass for windows, so before this function existed EVERY window
/// call with a constant argument failed the same way and fell back:
/// `lag(x, 1)`, `lead(x, 1)`, `nth_value(x, 2)` and `ntile(3)` alike —
/// measured, not assumed, against a live server through the owned-engine
/// bridge. Only `lag(x)`/`lead(x)` with the offset omitted entirely ever
/// reached the operator.
///
/// Doing it here rather than in the plan layer keeps the requirement and its
/// satisfaction in the one file that imposes it. The cost is that the
/// `Project` widens the operator's input, which would shift the window
/// outputs' positions — the logical `Window` node's schema is `input columns
/// ++ one column per window`, and the `Project` above it reads those by
/// index. [`WindowArgTrim`] carries what
/// [`trim_materialized_window_args`] needs to restore that layout, so the
/// widening is invisible to the parent.
///
/// Returns `(child, None)` untouched when every argument is already a column,
/// which is the overwhelmingly common case — a query that works today gets no
/// extra operator, no extra copy, and no change in nullability from
/// `Project::new`'s all-nullable output schema.
fn materialize_window_args(
    child: Box<dyn Operator>,
    windows: &mut [Expr],
    session: &SessionRef,
) -> Result<(Box<dyn Operator>, Option<WindowArgTrim>), BuildError> {
    let schema = child.schema();
    let base_width = schema.fields().len();
    let mut extra: Vec<(Expr, String)> = Vec::new();

    for w in windows.iter_mut() {
        let Expr::Window { args, .. } = w else {
            continue;
        };
        for a in args.iter_mut() {
            if matches!(a, Expr::Column(_)) {
                continue;
            }
            let name = format!("windowarg{}", extra.len());
            let index = u16::try_from(base_width + extra.len())
                .map_err(|_| BuildError::Unsupported("projection wider than u16 columns".into()))?;
            extra.push((a.clone(), name.clone()));
            *a = Expr::Column(ColumnRef {
                relation: 0,
                index,
                name,
            });
        }
    }

    if extra.is_empty() {
        return Ok((child, None));
    }

    let trim = WindowArgTrim {
        base_width,
        extra: extra.len(),
        windows: windows.len(),
    };
    let mut exprs = Vec::with_capacity(base_width + extra.len());
    for i in 0..base_width {
        exprs.push(passthrough(&schema, i)?);
    }
    exprs.extend(extra);
    // The materialized columns are the window arguments themselves — real
    // expressions lifted out of the `Expr::Window`, not passthroughs — so this
    // Project evaluates user SQL and needs the session like any other.
    Ok((
        Box::new(Project::new(child, exprs)?.in_session(SessionRef::clone(session))),
        Some(trim),
    ))
}

/// Drop the columns [`materialize_window_args`] appended, restoring the
/// `input columns ++ one per window` schema the parent `Project` indexes
/// into. The window outputs sit *after* the materialized arguments in the
/// `WindowAgg`'s schema, which is the whole reason this is not a no-op.
fn trim_materialized_window_args(
    agg: Box<dyn Operator>,
    trim: WindowArgTrim,
) -> Result<Project, BuildError> {
    let schema = agg.schema();
    let mut exprs = Vec::with_capacity(trim.base_width + trim.windows);
    for i in 0..trim.base_width {
        exprs.push(passthrough(&schema, i)?);
    }
    for i in 0..trim.windows {
        exprs.push(passthrough(&schema, trim.base_width + trim.extra + i)?);
    }
    Ok(Project::new(agg, exprs)?)
}

/// Carry the plan's frame across. `None` means the SQL had no explicit frame,
/// which the operator resolves to Postgres's default — NOT "the whole
/// partition". With an ORDER BY the default is RANGE UNBOUNDED PRECEDING TO
/// CURRENT ROW, which is why `last_value(x) OVER (ORDER BY y)` returns the
/// current row's value rather than the partition's last.
fn frame_of(f: &basin_plan::WindowFrame) -> Result<Option<crate::window::WindowFrame>, BuildError> {
    use crate::window::{FrameBound as XB, FrameUnits as XU, WindowFrame as XF};
    use basin_plan::{FrameBound as PB, FrameUnits as PU};

    let units = match f.units {
        PU::Rows => XU::Rows,
        PU::Range => XU::Range,
        PU::Groups => XU::Groups,
    };

    // A frame offset is a plan-time constant here, matching how sort keys are
    // resolved rather than evaluated. ROWS and GROUPS count rows and peer
    // groups; RANGE's offset is in the ORDER BY column's own unit, so an
    // INTERVAL bound against a timestamp is not representable as an f64 and is
    // refused rather than silently coerced into a wrong window.
    let bound = |b: &PB| -> Result<XB, BuildError> {
        Ok(match b {
            PB::UnboundedPreceding => XB::UnboundedPreceding,
            PB::CurrentRow => XB::CurrentRow,
            PB::UnboundedFollowing => XB::UnboundedFollowing,
            PB::Preceding(e) => XB::Preceding(offset_of(e, units)?),
            PB::Following(e) => XB::Following(offset_of(e, units)?),
        })
    };

    Ok(Some(XF {
        units,
        start: bound(&f.start)?,
        end: bound(&f.end)?,
    }))
}

fn offset_of(
    e: &Expr,
    units: crate::window::FrameUnits,
) -> Result<crate::window::FrameOffset, BuildError> {
    use crate::window::{FrameOffset, FrameUnits};
    let n = match e {
        Expr::Literal(basin_plan::Datum::Int64(v), _) if *v >= 0 => *v as f64,
        Expr::Literal(basin_plan::Datum::Int32(v), _) if *v >= 0 => *v as f64,
        Expr::Literal(basin_plan::Datum::Float64(v), _) if *v >= 0.0 => *v,
        _ => {
            return Err(BuildError::Unsupported(
                "window frame offset must be a non-negative constant (INTERVAL bounds not yet \
                 representable)"
                    .into(),
            ))
        }
    };
    Ok(match units {
        FrameUnits::Rows | FrameUnits::Groups => FrameOffset::Count(n as u64),
        FrameUnits::Range => FrameOffset::Range(n),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_pgtype::PgType;
    use basin_plan::{ColId, ColumnRef, Datum, SnapshotId};
    use std::sync::Arc;

    /// Pins every window oid against the real `pg_proc` block, because this
    /// table was wrong by exactly the width of lag's and lead's overload runs
    /// and nothing caught it. Two agents investigated "why does `first_value`
    /// fall back" without finding it, because the fallback was a symptom of a
    /// mapping error three oids away.
    ///
    /// Dumped from PostgreSQL 18.2:
    ///   3100 row_number  3101 rank       3102 dense_rank
    ///   3106 lag(any)    3107 lag(any,int)   3108 lag(any,int,any)
    ///   3109 lead(any)   3110 lead(any,int)  3111 lead(any,int,any)
    ///   3112 first_value 3113 last_value 3114 nth_value
    #[test]
    fn window_oids_match_the_real_pg_proc_block() {
        assert_eq!(window_func_of(3100), Some(WindowFunc::RowNumber));
        assert_eq!(window_func_of(3101), Some(WindowFunc::Rank));
        assert_eq!(window_func_of(3102), Some(WindowFunc::DenseRank));
        assert_eq!(window_func_of(3106), Some(WindowFunc::Lag));
        assert_eq!(window_func_of(3107), Some(WindowFunc::Lag));
        assert_eq!(window_func_of(3109), Some(WindowFunc::Lead));
        assert_eq!(window_func_of(3110), Some(WindowFunc::Lead));
        assert_eq!(window_func_of(3112), Some(WindowFunc::FirstValue));
        assert_eq!(window_func_of(3113), Some(WindowFunc::LastValue));
        assert_eq!(window_func_of(3114), Some(WindowFunc::NthValue));
    }

    /// `lead(x, n)` is oid 3110. This table used to call that `FirstValue` —
    /// an arity the builder ACCEPTS, so the query ran and returned the wrong
    /// column. Every other error in the block produced a fallback; this one
    /// produced an answer. Pinned separately so its significance is not lost
    /// among the rest.
    #[test]
    fn lead_with_an_offset_is_lead_and_not_first_value() {
        assert_eq!(window_func_of(3110), Some(WindowFunc::Lead));
        assert_ne!(window_func_of(3110), Some(WindowFunc::FirstValue));
    }

    /// lag/lead's three-argument form carries a DEFAULT for rows with no peer
    /// at the offset. `window_spec` reads only args[0] and args[1], so mapping
    /// these would silently drop the default and return NULL where Postgres
    /// returns it. Absent on purpose; falling back is the right answer.
    #[test]
    fn the_three_argument_lag_and_lead_forms_are_refused_not_silently_truncated() {
        assert_eq!(window_func_of(3108), None);
        assert_eq!(window_func_of(3111), None);
    }

    /// The variance family, one oid at a time. Read back from a live
    /// PostgreSQL 18.2 with `proname` and `pg_get_function_arguments`, six
    /// overloads each in declaration order int8/int4/int2/float4/float8/
    /// numeric.
    ///
    /// `variance` and `stddev` are Postgres's own aliases for `var_samp` and
    /// `stddev_samp` — DIFFERENT oids, same accumulator and finalizer — so
    /// the pairs below must land on the same [`VarKind`], and `var_pop`/
    /// `stddev_pop` must not.
    #[test]
    fn variance_family_oids_match_the_real_pg_proc_rows() {
        for (oid, want) in [
            // variance(int8/int4/int2/float4/float8/numeric)
            (2148, VarKind::VarSamp),
            (2149, VarKind::VarSamp),
            (2150, VarKind::VarSamp),
            (2151, VarKind::VarSamp),
            (2152, VarKind::VarSamp),
            (2153, VarKind::VarSamp),
            // var_samp
            (2641, VarKind::VarSamp),
            (2646, VarKind::VarSamp),
            // var_pop
            (2718, VarKind::VarPop),
            (2723, VarKind::VarPop),
            // stddev
            (2154, VarKind::StddevSamp),
            (2159, VarKind::StddevSamp),
            // stddev_samp
            (2712, VarKind::StddevSamp),
            (2717, VarKind::StddevSamp),
            // stddev_pop
            (2724, VarKind::StddevPop),
            (2729, VarKind::StddevPop),
        ] {
            assert_eq!(agg_func_of(oid), Some(AggFunc::Variance(want)), "oid {oid}");
        }
        // The blocks must not have run into each other: one past each end is
        // either a different function or nothing at all, never the same kind.
        assert_ne!(agg_func_of(2147), Some(AggFunc::Variance(VarKind::VarSamp)));
        assert_ne!(
            agg_func_of(2160),
            Some(AggFunc::Variance(VarKind::StddevSamp))
        );
    }

    /// `bit_xor` is oid 6164..6166 — added in PostgreSQL 14, nowhere near
    /// `bit_and`/`bit_or`'s 2236..2241. The `bit`-typed fourth overload of
    /// each (2242, 2243, 6167) is deliberately unmapped: Basin has no `bit`
    /// type, so falling back is the honest answer rather than running the
    /// integer accumulator over something that is not an integer.
    #[test]
    fn bitwise_and_boolean_aggregate_oids() {
        for oid in [2236, 2238, 2240] {
            assert_eq!(agg_func_of(oid), Some(AggFunc::BitAnd), "oid {oid}");
        }
        for oid in [2237, 2239, 2241] {
            assert_eq!(agg_func_of(oid), Some(AggFunc::BitOr), "oid {oid}");
        }
        for oid in [6164, 6165, 6166] {
            assert_eq!(agg_func_of(oid), Some(AggFunc::BitXor), "oid {oid}");
        }
        assert_eq!(agg_func_of(2517), Some(AggFunc::BoolAnd));
        assert_eq!(agg_func_of(2518), Some(AggFunc::BoolOr));
        // `every(boolean)`, the SQL-standard spelling of `bool_and`.
        assert_eq!(agg_func_of(2519), Some(AggFunc::BoolAnd));
        for bit_typed in [2242, 2243, 6167] {
            assert_eq!(agg_func_of(bit_typed), None, "oid {bit_typed} is bit-typed");
        }
    }

    /// The twelve two-argument statistical aggregates. `pg_proc` orders them
    /// neither alphabetically nor by what they compute — `regr_r2` (2824)
    /// sits between `regr_avgy` and `regr_slope` — so an off-by-one here maps
    /// a call onto a function that is *also* defined for the same arguments
    /// and returns a different number without erroring.
    #[test]
    fn regression_family_oids_match_the_real_pg_proc_rows() {
        for (oid, kind) in [
            (2818, RegrKind::Count),
            (2819, RegrKind::Sxx),
            (2820, RegrKind::Syy),
            (2821, RegrKind::Sxy),
            (2822, RegrKind::AvgX),
            (2823, RegrKind::AvgY),
            (2824, RegrKind::R2),
            (2825, RegrKind::Slope),
            (2826, RegrKind::Intercept),
            (2827, RegrKind::CovarPop),
            (2828, RegrKind::CovarSamp),
            (2829, RegrKind::Corr),
        ] {
            assert_eq!(
                agg_func_of(oid),
                Some(AggFunc::Regr { kind, x_col: 0 }),
                "oid {oid}"
            );
        }
    }

    /// `percent_rank`/`cume_dist` exist TWICE in `pg_proc` under the same
    /// name: 3103/3104 are `prokind = 'w'` window functions, 3988/3990 are
    /// `prokind = 'a'` hypothetical-set aggregates (`pg_aggregate.aggkind =
    /// 'h'`) taking the probe row as arguments plus a `WITHIN GROUP` clause.
    /// Verified live, both directions. Mapping the aggregate spellings onto
    /// these window implementations would answer a different question under
    /// the same column name, so both tables must refuse them.
    #[test]
    fn the_hypothetical_set_spellings_are_not_the_window_ones() {
        assert_eq!(window_func_of(3103), Some(WindowFunc::PercentRank));
        assert_eq!(window_func_of(3104), Some(WindowFunc::CumeDist));
        assert_eq!(
            window_func_of(3105),
            Some(WindowFunc::Ntile { buckets_col: 0 })
        );
        for hypothetical in [3988, 3990] {
            assert_eq!(window_func_of(hypothetical), None, "oid {hypothetical}");
            assert_eq!(agg_func_of(hypothetical), None, "oid {hypothetical}");
        }
    }

    /// The ordered-set aggregates are deliberately not wired to anything:
    /// they take a `WITHIN GROUP (ORDER BY …)` clause that `AggregateSpec`
    /// has no representation for, and `agg_spec` refuses a non-empty
    /// `order_by` unconditionally anyway. Read live: `percentile_cont` is
    /// 3974/3976/3980/3982, `percentile_disc` 3972/3978, `mode` 3984 — the
    /// float8 and float8[] forms are NOT adjacent, they interleave with
    /// `percentile_disc`. Falling back is the right answer for all of them;
    /// mapping `percentile_cont` onto some two-argument aggregate that
    /// merely accepts the arity would not be.
    #[test]
    fn the_ordered_set_aggregates_are_not_wired_to_anything() {
        for oid in [3972, 3974, 3976, 3978, 3980, 3982, 3984] {
            assert_eq!(agg_func_of(oid), None, "oid {oid}");
        }
    }

    /// `ntile(3)`'s argument is a LITERAL, and nothing in the plan layer
    /// materializes non-column window arguments the way
    /// `materialize_agg_inputs` does for aggregates — so before
    /// [`materialize_window_args`] this failed `NonColumnKey` and the whole
    /// query fell back. Measured, live: `ntile(3)`, `lag(x, 1)`,
    /// `lead(x, 1)` and `nth_value(x, 2)` were all unreachable for that one
    /// reason.
    ///
    /// The assertion that matters as much as the values is the WIDTH. The
    /// materializing `Project` widens the operator's input, which pushes the
    /// window outputs to the right; the logical `Window` node's schema is
    /// `input columns ++ one per window` and the parent `Project` reads those
    /// by index. If the trim above the `WindowAgg` were missing, the values
    /// below would still be correct and every caller would read the wrong
    /// column.
    ///
    /// `ntile(3)` over these four rows is `1,1,2,3` — read from a live
    /// PostgreSQL 18.2, not derived. Buckets are as-equal-as-possible with
    /// the LARGER ones first, so the extra row lands in bucket 1, not 3.
    #[test]
    fn ntile_with_a_literal_argument_builds_and_keeps_the_output_width() {
        let plan = LogicalPlan::Window {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            windows: vec![Expr::Window {
                func: basin_plan::FuncId(basin_pgtype::Oid(3105)),
                args: vec![Expr::Literal(Datum::Int32(3), PgType::INT4)],
                partition_by: vec![],
                order_by: vec![basin_plan::SortKey {
                    expr: col(0, "id"),
                    descending: false,
                    nulls_first: false,
                }],
                frame: basin_plan::WindowFrame {
                    units: basin_plan::FrameUnits::Range,
                    start: basin_plan::FrameBound::UnboundedPreceding,
                    end: basin_plan::FrameBound::CurrentRow,
                },
            }],
        };

        let batches = drain(build(&plan, &resolver()).unwrap());
        assert_eq!(
            batches[0].num_columns(),
            3,
            "id, v and the one window output — the materialized `3` must not \
             survive into the node's schema"
        );
        let got: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                let a = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
                (0..b.num_rows()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec![1, 1, 2, 3]);
    }

    /// The gated half of [`materialize_window_args`]: a window whose every
    /// argument is already a column gets no extra operators at all, so the
    /// queries that worked before it existed are byte-for-byte unaffected —
    /// including `Project::new`'s all-nullable output schema, which would
    /// otherwise be imposed on every windowed query in the system.
    #[test]
    fn a_window_whose_arguments_are_all_columns_gets_no_extra_projections() {
        let mut windows = vec![Expr::Window {
            func: basin_plan::FuncId(basin_pgtype::Oid(3106)),
            args: vec![col(1, "v")],
            partition_by: vec![],
            order_by: vec![],
            frame: basin_plan::WindowFrame {
                units: basin_plan::FrameUnits::Range,
                start: basin_plan::FrameBound::UnboundedPreceding,
                end: basin_plan::FrameBound::CurrentRow,
            },
        }];
        let child = build(&scan_plan(vec![ColId(0), ColId(1)], vec![]), &resolver()).unwrap();
        let before = child.schema();
        let (child, trim) =
            materialize_window_args(child, &mut windows, &crate::operator::default_session())
                .unwrap();
        assert!(trim.is_none(), "nothing needed materializing");
        assert_eq!(child.schema(), before, "and nothing was wrapped");
    }

    fn table() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn resolver() -> MemTableResolver {
        let (schema, batch) = table();
        let mut r = MemTableResolver::new();
        r.insert(TableId(1), schema, vec![batch]);
        r
    }

    /// Three columns whose values are pairwise distinguishable under the same
    /// predicate, so a filter that reads the WRONG one cannot come out right
    /// by coincidence: `uid` passes `> 7` for every row, `k` passes for none,
    /// and `n` passes for exactly the last two.
    fn three_col_table() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("uid", DataType::Int64, true),
            Field::new("k", DataType::Int32, true),
            Field::new("n", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10i64, 11, 12])),
                Arc::new(Int32Array::from(vec![0, 0, 0])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    /// A scan filter's column index is a position within the scan's OWN
    /// projection, not within the table — see the `LogicalPlan::Scan` arm.
    /// `MemTableResolver` declines the projection, so its batches carry the
    /// whole table and the builder has to translate those indices before
    /// handing them to `Scan`, which filters the unprojected batch.
    ///
    /// This is the plan `SELECT n FROM u WHERE n > 7` optimizes to: pruning
    /// drops `uid` and `k`, leaving `projection=[ColId(2)]` and renumbering
    /// the predicate's column to position 0. Untranslated, position 0 of the
    /// full batch is `uid` — every value of which passes `> 7`, so the query
    /// came back completely unfiltered rather than erroring.
    #[test]
    fn a_pruned_scans_filter_reads_the_projected_column_not_the_tables_first() {
        let (schema, batch) = three_col_table();
        let mut r = MemTableResolver::new();
        r.insert(TableId(1), schema, vec![batch]);

        let plan = scan_plan(
            vec![ColId(2)],
            // position 0 of `projection` == table column 2 == `n`.
            // OID 521 is int4 `>`, from pg_operator on a live server.
            vec![Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(521)),
                lhs: Box::new(col(0, "n")),
                rhs: Box::new(Expr::Literal(Datum::Int32(7), PgType::INT4)),
            }],
        );

        let batches = drain(build(&plan, &r).unwrap());
        let got: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                (0..b.num_rows()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            got,
            vec![8, 9],
            "the predicate must be `n > 7`; `uid > 7` would keep all three rows \
             and `k > 7` none, so the VALUES here are what tells the three apart"
        );
    }

    /// The same translation, with the projected column no longer at position
    /// 0 of the table: `SELECT uid FROM u WHERE n > 7` prunes to
    /// `projection=[uid, n]` and renumbers the predicate to position 1.
    /// Untranslated, position 1 of the full batch is `k`, whose every value
    /// fails `> 7` — so this direction returned NOTHING. Same bug, opposite
    /// symptom, which is why row COUNTS alone cannot pin it: only the values
    /// distinguish "filtered correctly" from "filtered on a different column".
    #[test]
    fn a_pruned_scans_filter_survives_the_projection_keeping_two_columns() {
        let (schema, batch) = three_col_table();
        let mut r = MemTableResolver::new();
        r.insert(TableId(1), schema, vec![batch]);

        let plan = scan_plan(
            vec![ColId(0), ColId(2)],
            vec![Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(521)),
                lhs: Box::new(col(1, "n")),
                rhs: Box::new(Expr::Literal(Datum::Int32(7), PgType::INT4)),
            }],
        );

        let batches = drain(build(&plan, &r).unwrap());
        let got: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..b.num_rows()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec![11i64, 12], "uid of the rows where n > 7");
    }

    fn col(i: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index: i,
            name: name.into(),
        })
    }

    fn scan_plan(projection: Vec<ColId>, filters: Vec<Expr>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(1),
            projection,
            filters,
            snapshot: SnapshotId(0),
        }
    }

    fn drain(mut op: Box<dyn Operator>) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            out.push(b);
        }
        out
    }

    /// The whole point of this module: a plan becomes operators and produces
    /// rows. Before this existed, every operator was individually correct and
    /// no query could run.
    #[test]
    fn a_scan_plan_executes_end_to_end() {
        let batches =
            drain(build(&scan_plan(vec![ColId(0), ColId(1)], vec![]), &resolver()).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        assert_eq!(batches[0].num_columns(), 2);
    }

    /// Filter over scan — the predicate reaches the evaluator through the
    /// operator tree, which is the seam this module exists to close.
    #[test]
    fn filter_over_scan_executes_and_reduces_rows() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            // id > 2 — OID 521 is int4 '>', verified against pg_operator.
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(521)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Literal(Datum::Int32(2), PgType::INT4)),
            },
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2, "ids 3 and 4 survive `id > 2`");
    }

    /// `ORDER BY … LIMIT` must become TopK, not a full Sort followed by a
    /// truncation. Basin's published numbers for this shape depend on early
    /// termination, so the recognition is load-bearing.
    #[test]
    fn order_by_limit_becomes_top_k() {
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                keys: vec![PlanSortKey {
                    expr: col(0, "id"),
                    descending: true,
                    nulls_first: false,
                }],
            }),
            skip: None,
            fetch: Some(Expr::Literal(Datum::Int64(2), PgType::INT8)),
            with_ties: false,
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2);
    }

    // ── array_agg / string_agg OID WIRING ───────────────────────────────
    //
    // `aggregate.rs` (commit 2d4d481f) implements both accumulators and is
    // unit-tested against them directly via `AggregateSpec` literals — see
    // `array_agg_spec`/`string_agg_spec` in that file's own test module.
    // What was missing was `agg_func_of` ever mapping `array_agg`'s and
    // `string_agg`'s real `pg_proc` OIDs to those variants, so every call
    // hit the `ok_or_else` in `agg_spec` and fell back. These tests pin the
    // OIDs (dumped from a live PostgreSQL 18.2 — see the query in
    // `agg_func_of`'s own comment) and exercise the full `LogicalPlan ->
    // build() -> RecordBatch` path, not just the accumulator in isolation.

    /// A table with a `Utf8` pair, for `string_agg` — `table()`'s two
    /// `Int32` columns can't stand in for a value/delimiter pair.
    fn table_text() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                // Every row's delimiter is the same literal here, but it is
                // still a resolved COLUMN, not a constant folded into the
                // spec — `string_agg`'s delimiter is read per row (see
                // `AggFunc::StringAgg::delim_col`'s doc), which is exactly
                // what distinguishes it from every other aggregate's single
                // `input_col`.
                Arc::new(StringArray::from(vec![",", ",", "|"])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn resolver_text() -> MemTableResolver {
        let (schema, batch) = table_text();
        let mut r = MemTableResolver::new();
        r.insert(TableId(1), schema, vec![batch]);
        r
    }

    fn agg_expr(oid: u32, args: Vec<Expr>) -> Expr {
        Expr::Aggregate {
            func: basin_plan::FuncId(basin_pgtype::Oid(oid)),
            args,
            distinct: false,
            filter: None,
            order_by: vec![],
        }
    }

    /// `array_agg(anynonarray)` is oid 2335 on a live PostgreSQL 18.2 —
    /// `array_agg(id)` over an `int4` column resolves to this overload, not
    /// 4053 (`array_agg(anyarray)`, i.e. `array_agg` of an *array-typed*
    /// column, which `id` is not). Ungrouped, so the whole table collapses
    /// to one output row holding every `id` in scan order.
    #[test]
    fn array_agg_oid_2335_reaches_the_array_agg_accumulator() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![],
            aggs: vec![agg_expr(2335, vec![col(0, "id")])],
            grouping_sets: None,
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1, "ungrouped: one output row");
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert!(!list.is_null(0));
        let elems = list
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!(
            (0..elems.len()).map(|i| elems.value(i)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4],
            "array_agg(id) with no ORDER BY still preserves scan order"
        );
    }

    /// `string_agg(text, text)` is oid 3538. Grouped by `grp`, with a
    /// per-row delimiter column (see `table_text`'s doc) — this is the shape
    /// that specifically exercises `agg_spec` filling in `delim_col` from
    /// `args[1]` rather than `agg_func_of`'s placeholder `0`.
    #[test]
    fn string_agg_oid_3538_reaches_the_string_agg_accumulator_with_its_delimiter_column() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table: TableId(1),
                projection: vec![ColId(0), ColId(1), ColId(2)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            group: vec![col(0, "grp")],
            aggs: vec![agg_expr(3538, vec![col(1, "name"), col(2, "delim")])],
            grouping_sets: None,
        };
        let batches = drain(build(&plan, &resolver_text()).unwrap());
        let mut rows: Vec<(i32, Option<String>)> = Vec::new();
        for b in &batches {
            let grp = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let sa = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((
                    grp.value(i),
                    (!sa.is_null(i)).then(|| sa.value(i).to_string()),
                ));
            }
        }
        rows.sort_unstable();
        assert_eq!(
            rows,
            vec![(1, Some("a,b".into())), (2, Some("c".into()))],
            "group 1 joins on its own rows' delimiter ','; group 2's row uses '|'"
        );
    }

    /// `array_agg(x ORDER BY y)` is real syntax, but `AggregateSpec` has no
    /// field to carry the ordering — the refusal a few lines above (`if
    /// !order_by.is_empty() { ... }`) is unconditional, for every aggregate,
    /// and must survive wiring `array_agg`'s OID in. Silently dropping the
    /// `ORDER BY` would return an array in scan order while claiming to
    /// honour the query's requested order — wrong data, not a missing
    /// feature.
    #[test]
    fn array_agg_order_by_still_refuses_rather_than_silently_reordering() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2335)),
                args: vec![col(0, "id")],
                distinct: false,
                filter: None,
                order_by: vec![PlanSortKey {
                    expr: col(1, "v"),
                    descending: false,
                    nulls_first: false,
                }],
            }],
            grouping_sets: None,
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("array_agg(x ORDER BY y) has nowhere to carry the ordering"),
        };
        assert_eq!(
            err,
            BuildError::Unsupported("ORDER BY inside an aggregate".into())
        );
    }

    // ── VALUES IN FROM ──────────────────────────────────────────────────
    //
    // `basin-plan` commit 8a87750d lowers `SELECT * FROM (VALUES ...) AS
    // v(i, s)` — confirmed by that crate's own
    // `a_values_list_in_from_may_be_column_aliased` test — to a `Project`
    // (renaming to the alias list) over a `LogicalPlan::Values` (still
    // carrying its own default `column1`/`column2` names internally; the
    // alias list only renames the outer `Scope`'s copy of the schema, per
    // `build_range_subselect`'s doc, not the `Values` node's own). Both
    // `LogicalPlan::Project` and `LogicalPlan::Values` already had
    // operators and builder arms *before* that lowering commit (`Values`
    // since c14120ea) — so despite that commit's own message saying "VALUES
    // in FROM still falls back", nothing here was actually missing; this
    // test exists to nail that down rather than take it on faith.
    #[test]
    fn values_in_from_with_a_column_alias_list_builds_end_to_end() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![
                        Expr::Literal(Datum::Int32(1), PgType::INT4),
                        Expr::Literal(Datum::Utf8("a".into()), PgType::TEXT),
                    ],
                    vec![
                        Expr::Literal(Datum::Int32(2), PgType::INT4),
                        Expr::Literal(Datum::Utf8("b".into()), PgType::TEXT),
                    ],
                ],
                schema: vec![
                    ("column1".into(), PgType::INT4),
                    ("column2".into(), PgType::TEXT),
                ],
            }),
            exprs: vec![(col(0, "i"), "i".into()), (col(1, "s"), "s".into())],
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        assert_eq!(
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["i", "s"],
            "the alias list renames the output columns, not just the Scope"
        );
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect();
        let ss: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(ss, vec!["a".to_string(), "b".to_string()]);
    }

    /// An unimplemented plan shape must say so by name rather than silently
    /// doing something else. A builder that guesses produces wrong answers
    /// instead of errors.
    #[test]
    fn an_unbuildable_plan_names_the_construct() {
        // This test exists to go stale. DISTINCT was the example, then window
        // functions, then CTEs — all three build now (and so do LATERAL,
        // WITH RECURSIVE, and INSERT/UPDATE/DELETE). GROUPING SETS / ROLLUP /
        // CUBE is the current frontier — when this fails again, that is the
        // good outcome and the next unimplemented shape takes its place.
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![col(0, "id")],
            aggs: vec![],
            grouping_sets: Some(basin_plan::GroupingSets(vec![vec![0]])),
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("GROUPING SETS has no operator yet and must not build"),
        };
        assert_eq!(
            err,
            BuildError::Unsupported("GROUPING SETS / ROLLUP / CUBE".into())
        );
    }

    #[test]
    fn an_unknown_table_is_reported_not_papered_over() {
        let plan = LogicalPlan::Scan {
            table: TableId(999),
            projection: vec![ColId(0)],
            filters: vec![],
            snapshot: SnapshotId(0),
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("an unknown table must not build"),
        };
        assert_eq!(err, BuildError::UnknownTable(TableId(999)));
    }

    /// `WITH TIES` degrades to `ONLY` in the DataFusion path
    /// (select_advanced.rs:455), silently returning fewer rows than Postgres.
    /// The owned builder now SERVES the legal shape — see
    /// `with_ties_over_an_order_by_builds_the_ties_operator` — but this
    /// plan is not it: there is no `ORDER BY` for a tie to be defined
    /// against, and Postgres rejects that spelling outright ("WITH TIES
    /// cannot be specified without ORDER BY clause"). It must still be
    /// refused rather than quietly answered as `ONLY`, which is the wrong
    /// row count this test has always existed to prevent.
    #[test]
    fn with_ties_is_refused_rather_than_silently_degraded() {
        let plan = LogicalPlan::Limit {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            skip: None,
            fetch: Some(Expr::Literal(Datum::Int64(1), PgType::INT8)),
            with_ties: true,
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("WITH TIES must be refused, not silently degraded"),
        };
        assert!(matches!(err, BuildError::Unsupported(ref s) if s.contains("WITH TIES")));
    }

    /// The legal `WITH TIES` shape — a `Limit` directly over a `Sort` — now
    /// builds, and builds something ties-aware rather than a plain
    /// `Limit`/`TopK` that would truncate at `fetch`. Measured live on
    /// PostgreSQL 18.2 over `(1,'a'),(2,'b'),(3,'c'),(100,NULL),(101,'a')`:
    /// `ORDER BY name FETCH FIRST 1 ROW WITH TIES` returns TWO rows, `(1,'a')`
    /// and `(101,'a')` — more than `fetch`.
    ///
    /// The assertion is on BEHAVIOUR, not on the operator's name: `dyn
    /// Operator` is not `Debug`, and a name check would pass for a
    /// correctly-named operator that still truncated. The sort key here is
    /// `three_col_table`'s `k`, which is `0` in every row, so `FETCH FIRST 1
    /// ROW WITH TIES` must return all THREE. A key with distinct values would
    /// have passed against a truncating operator too, which is the whole
    /// thing this has to rule out.
    #[test]
    fn with_ties_over_an_order_by_returns_the_whole_tie_group() {
        let (schema, batch) = three_col_table();
        let mut r = MemTableResolver::new();
        r.insert(TableId(1), schema, vec![batch]);

        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                // position 0 of `projection` == table column 1 == `k`.
                input: Box::new(scan_plan(vec![ColId(1)], vec![])),
                keys: vec![basin_plan::SortKey {
                    expr: col(0, "k"),
                    descending: false,
                    nulls_first: false,
                }],
            }),
            skip: None,
            fetch: Some(Expr::Literal(Datum::Int64(1), PgType::INT8)),
            with_ties: true,
        };
        let op = build(&plan, &r).expect("WITH TIES over ORDER BY must build");
        let rows: usize = drain(op).iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 3,
            "every row ties with the first on `k`, so all three come back — \
             a truncating operator would return 1"
        );
    }

    /// `LIMIT ALL` is not a distinct parse-tree shape — Postgres lowers it to
    /// a `LIMIT` whose count is a NULL constant. `const_usize` cannot read a
    /// count out of that, so the builder refused the whole statement as a
    /// "non-constant LIMIT" and `SELECT id FROM t LIMIT ALL`, which is
    /// ordinary SQL, fell back. The bound is genuinely ABSENT, not zero:
    /// live PostgreSQL 18.2 returns all five rows of a five-row input for
    /// `LIMIT ALL`, `LIMIT NULL` and `OFFSET NULL` alike.
    #[test]
    fn limit_all_is_no_limit_rather_than_a_non_constant_one() {
        for bound in ["fetch", "skip"] {
            let null = Some(Expr::Literal(Datum::Null, PgType::UNKNOWN));
            let plan = LogicalPlan::Limit {
                input: Box::new(scan_plan(vec![ColId(0)], vec![])),
                skip: if bound == "skip" { null.clone() } else { None },
                fetch: if bound == "fetch" { null } else { None },
                with_ties: false,
            };
            build(&plan, &resolver())
                .unwrap_or_else(|e| panic!("a NULL {bound} bound means no bound, got {e:?}"));
        }
    }
    /// A set-returning function reaches the operator through the builder.
    /// `generate_series` in a target list is one of the two shapes that
    /// motivated replacing DataFusion at all, so this is the first time it is
    /// reachable from a plan rather than only from a hand-built operator.
    #[test]
    fn a_set_returning_function_builds_and_expands() {
        let plan = LogicalPlan::ProjectSet {
            input: Box::new(LogicalPlan::Empty {
                produce_one_row: true,
                schema: vec![],
            }),
            // generate_series(1, 3) — OID 1067 is the two-argument int form,
            // from pg_proc on a live server.
            srfs: vec![Expr::SetReturning {
                func: basin_plan::FuncId(basin_pgtype::Oid(1067)),
                args: vec![
                    Expr::Literal(Datum::Int32(1), PgType::INT4),
                    Expr::Literal(Datum::Int32(3), PgType::INT4),
                ],
            }],
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            rows, 3,
            "generate_series(1,3) expands one input row to three"
        );
    }

    // ── JOIN ─────────────────────────────────────────────────────────────
    //
    // Reached from a real `SELECT ... JOIN ... ON ...` these build/exec-error
    // out (see `fallback_histogram`'s per-query attribution and
    // `docs/migration/df-removal` session notes for `165cd438`/`3cb16d97`,
    // the panic fix and the projection-pruning fix that preceded this). This
    // module had ZERO coverage of `LogicalPlan::Join` -> `HashJoin` before
    // these tests — only `LateralJoin` (a different node) was exercised.
    //
    // Root-caused here (in `basin-exec`, not `basin-plan`): with the
    // asymmetric-width fixtures below, driving a real
    // `lower_select`+`optimize_default` plan through `build()` succeeds
    // end-to-end (self-join, 3-row result) — `column_index()` correctly
    // reads a join key's `ColumnRef::index` as-is because
    // `lower/select.rs::split_equijoin_conjuncts` already rebases the
    // right-hand operand of an `on` pair to be relative to the RIGHT input's
    // own schema (see that function's doc comment) before `Join::on` ever
    // reaches this builder — there is no flat-vs-per-side confusion left in
    // `build.rs` for `on` to trip over. The four SQL shapes in the histogram
    // still fall back, but for reasons outside this file:
    //
    // - Both joins: `basin-plan/src/opt/projection.rs`'s `Join` arm of
    //   `prune()` (`collect_both_conventions`) applies the FLAT-position
    //   interpretation to `on`'s right-hand element too. That element is
    //   already right-schema-relative (rebased, per the paragraph above),
    //   so treating it as an unrebased flat position across the
    //   concatenated scope misattributes it to `left_required` whenever its
    //   (right-relative) index is `< left_width` — which prunes the join
    //   key clean out of the right scan's projection. Confirmed directly:
    //   building the SAME plan with `optimize_default` skipped succeeds;
    //   running it through `optimize_default` first leaves the right scan
    //   with `projection: []` and `build()` reports exactly
    //   `join key index 0 is out of range for the right side's 0-column
    //   schema`. Fix belongs in that `Join` arm: `r`'s columns should be
    //   attributed to `right_required` directly (offset by 0, not
    //   `left_width`), not run through the same flat/relation-1 dispatch as
    //   `l`.
    // - `WITH x AS (SELECT id FROM t) SELECT id FROM x`: builds and executes
    //   fine here (see the CTE section below, and `CteRegistry` in this
    //   file) — the plan itself never gets a chance to run. The bridge in
    //   `basin-engine/src/owned_engine.rs`'s `collect_tables_stmt` walks
    //   only `stmt.from_clause` (plus `larg`/`rarg` for a set op) and never
    //   `stmt.with_clause`, so a CTE's real table references (here, `t`
    //   inside the CTE body) are never prefetched into the resolver, and
    //   the CTE's own name (`x`) is looked up as if it were a catalog table
    //   and fails — `Fallback::Ineligible("table not found in the
    //   catalog")`, before `lower_select` is ever called.
    // - `SELECT generate_series(1,3)`: `ProjectSet` builds and expands fine
    //   here (see `a_set_returning_function_builds_and_expands` above) —
    //   again, the plan never gets that far. `basin-plan/src/lower/select.rs`'s
    //   `lower_target_list` hard-refuses with `LowerError::Unsupported`
    //   ("set-returning functions in the SELECT list are not yet lowered")
    //   the moment it sees `expr.contains_srf()`, rather than building the
    //   `LogicalPlan::ProjectSet` this file already knows how to run.
    //
    // None of the three bullets above are in `crates/basin-exec/**`.

    /// A left side and a right side with DIFFERENT widths and the join key
    /// at a DIFFERENT position on each side (left index 2, right index 0) —
    /// deliberately not the "both sides are column 0 of equal-width
    /// schemas" shape that a flat-vs-per-side index mixup could pass by
    /// coincidence. If `build()` ever started treating a join key as a flat
    /// position across the concatenated left+right schema (rather than the
    /// per-side position `HashJoin::new` documents), the right key here
    /// would land out of bounds (right schema is 1 column wide) and this
    /// would fail loudly rather than quietly return the wrong rows.
    fn asymmetric_join_resolver() -> MemTableResolver {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
            Field::new("id", DataType::Int32, true),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100, 100, 100])),
                Arc::new(Int32Array::from(vec![200, 201, 202])),
                Arc::new(Int32Array::from(vec![1, 2, 5])),
            ],
        )
        .unwrap();

        let right_schema = Arc::new(Schema::new(vec![Field::new("rid", DataType::Int32, true)]));
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut r = MemTableResolver::new();
        r.insert(TableId(1), left_schema, vec![left_batch]);
        r.insert(TableId(2), right_schema, vec![right_batch]);
        r
    }

    fn asymmetric_join_plan(kind: basin_plan::JoinKind) -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table: TableId(1),
                projection: vec![ColId(0), ColId(1), ColId(2)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            right: Box::new(LogicalPlan::Scan {
                table: TableId(2),
                projection: vec![ColId(0)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            kind,
            on: vec![(col(2, "id"), col(0, "rid"))],
            filter: None,
        }
    }

    /// Inner join, asymmetric widths: only `id`s 1 and 2 are on both sides
    /// (5 has no match on the right, 3 has no match on the left), so an
    /// inner join keeps exactly those two rows — and, crucially, keeps them
    /// with the RIGHT `id`/`rid` values lined up with the correct row, which
    /// a scrambled key index would not reliably do.
    #[test]
    fn an_inner_join_with_asymmetric_widths_matches_the_right_rows() {
        let plan = asymmetric_join_plan(basin_plan::JoinKind::Inner);
        let batches = drain(build(&plan, &asymmetric_join_resolver()).unwrap());
        let mut pairs: Vec<(i32, i32)> = Vec::new();
        for b in &batches {
            let id = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
            let rid = b.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((id.value(i), rid.value(i)));
            }
        }
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![(1, 1), (2, 2)],
            "only id=1 and id=2 match on both sides, each id lined up with the SAME rid"
        );
    }

    /// Left join, asymmetric widths: every left row survives (1, 2, 5), with
    /// `rid` NULL for the unmatched `id=5` — proving the join key comparison
    /// used the right side's OWN column 0, not some flat position that would
    /// either miss real matches or fabricate ones.
    #[test]
    fn a_left_join_with_asymmetric_widths_keeps_every_left_row() {
        let plan = asymmetric_join_plan(basin_plan::JoinKind::Left);
        let batches = drain(build(&plan, &asymmetric_join_resolver()).unwrap());
        let mut pairs: Vec<(i32, Option<i32>)> = Vec::new();
        for b in &batches {
            let id = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
            let rid = b.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((
                    id.value(i),
                    if rid.is_null(i) {
                        None
                    } else {
                        Some(rid.value(i))
                    },
                ));
            }
        }
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![(1, Some(1)), (2, Some(2)), (5, None)],
            "every left row survives; only id=5 (no right match) gets a NULL rid"
        );
    }

    // ── SCALAR SUBQUERY (InitPlan) ──────────────────────────────────────
    //
    // `SELECT id FROM t WHERE id = (SELECT max(id) FROM t)` fell back with
    // `eval.rs`'s `Expr::Subquery { .. } => Err(Internal("subqueries must be
    // decorrelated into a join (or a scalar materialized elsewhere) before
    // scalar eval sees them"))` — the "materialized elsewhere" the comment
    // promised did not exist. `basin_plan::opt::decorrelate` correctly
    // leaves this one alone (uncorrelated: no correlation predicate to join
    // on, better run once than turned into a join), so it reached `build()`
    // still shaped as `Expr::Subquery`. `materialize_scalar_subquery`
    // (called from `bind_outer_rec`, reached from every `bind_outer` call
    // site including `Filter`'s predicate) now builds and runs it exactly
    // once — an InitPlan — and folds the result into a `Literal` before
    // `eval` ever sees it.

    /// `max(id)` — an aggregate with no GROUP BY over the whole table,
    /// exactly the shape `SELECT max(id) FROM t` lowers to.
    fn max_id_subplan() -> LogicalPlan {
        LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![],
            // `max(int4)` is pg_proc oid 2116 — see `agg_func_of`.
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2116)),
                args: vec![col(0, "id")],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        }
    }

    /// The happy path: exactly one row (id=4) equals the table's max id.
    #[test]
    fn a_scalar_subquery_materializes_once_and_filters_correctly() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            // id = (SELECT max(id) FROM t) — OID 96 is int4 '='.
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(max_id_subplan()),
                    operand: None,
                }),
            },
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "only id=4 equals max(id)=4");
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![4]);
    }

    /// Zero rows from the subquery is Postgres's NULL rule, not an error and
    /// not an empty result for the whole query — `id = NULL` is never true
    /// under three-valued logic, so every row is filtered out, but the
    /// STATEMENT still succeeds.
    #[test]
    fn a_scalar_subquery_with_zero_rows_is_null_not_an_error() {
        let empty_subplan = LogicalPlan::Aggregate {
            // `WHERE false` empties the input before the aggregate ever
            // sees a row — `max` over zero rows is the classic "NULL, not
            // zero" aggregate case.
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Literal(Datum::Bool(false), PgType::BOOL),
            }),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2116)),
                args: vec![col(0, "id")],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(empty_subplan),
                    operand: None,
                }),
            },
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 0,
            "id = NULL is never true, so every row is filtered — but this is \
             success (Ok with 0 rows), not an error"
        );
    }

    /// More than one row from a scalar subquery is SQLSTATE 21000
    /// `cardinality_violation` in Postgres — a real runtime error, not a
    /// silently-picked first row.
    #[test]
    fn a_scalar_subquery_with_more_than_one_row_is_a_cardinality_violation() {
        let multi_row_subplan = scan_plan(vec![ColId(0)], vec![]); // 4 rows, not 1
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(multi_row_subplan),
                    operand: None,
                }),
            },
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("a multi-row scalar subquery must be refused, not built"),
        };
        assert!(
            matches!(err, BuildError::Exec(ExecError::CardinalityViolation(_))),
            "got {err:?}, expected a CardinalityViolation"
        );
    }

    // ── CORRELATED scalar subqueries ─────────────────────────────────────
    //
    // Everything above this line is an UNCORRELATED scalar subquery: one
    // value for the whole statement. These are the correlated ones, which
    // have a different value per row and which this builder answered wrongly
    // — not slowly, not with an error, but with plausible values — until
    // `CorrelatedScalar` existed. Every test here asserts VALUES: the bug
    // produced the right row count, the right column count and the right
    // types, so anything less than a value check passes on the broken build.

    /// `ColumnRef { relation: 1 }` inside the subplan — `opt::decorrelate`'s
    /// `OUTER_REF`, one column of the row this subquery is being evaluated
    /// for.
    fn outer_col(i: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: OUTER_REF,
            index: i,
            name: name.into(),
        })
    }

    /// `(SELECT count(*) FROM t x WHERE x.id = <outer>.id)` — the reported
    /// bug's own subquery. OID 96 is int4 `=`; 2803 is `count(*)`.
    fn correlated_count_subplan() -> LogicalPlan {
        LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(outer_col(0, "id")),
                },
            }),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2803)),
                args: vec![],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        }
    }

    fn i64_column(batches: &[RecordBatch], idx: usize) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(idx)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("int8 column")
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// THE BUG. `SELECT id, (SELECT count(*) FROM t x WHERE x.id = t.id)
    /// FROM t` over `id = 1,2,3,4` is `1,1,1,1` — each row's subquery counts
    /// only its own matching row. Before `CorrelatedScalar`,
    /// `materialize_scalar_subquery` ran the subplan once with `outer: None`,
    /// the unbound `OUTER_REF` was read as the LOCAL column at the same
    /// index (`x.id = x.id`, always true), and every row got `4` — the full
    /// table count, four times, with exactly the right shape.
    #[test]
    fn a_correlated_count_in_the_target_list_counts_per_row_not_per_statement() {
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![
                (col(0, "id"), "id".into()),
                (
                    Expr::Subquery {
                        kind: basin_plan::SubqueryKind::Scalar,
                        subplan: Box::new(correlated_count_subplan()),
                        operand: None,
                    },
                    "c".into(),
                ),
            ],
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        assert_eq!(
            i64_column(&batches, 1),
            vec![Some(1), Some(1), Some(1), Some(1)],
            "each row counts only the rows correlated to it — 4,4,4,4 is the \
             pre-fix wrong answer"
        );
    }

    /// The same shape with a subquery that returns a DIFFERENT value per row
    /// rather than the same one four times, so a fix that merely evaluated
    /// the subquery per row but bound the correlation to the wrong row (or
    /// to a fixed row) cannot pass either: `(SELECT x.v FROM t x WHERE x.id =
    /// t.id)` is `10, 20, 30, 40`.
    #[test]
    fn a_correlated_scalar_in_the_target_list_gets_a_different_value_per_row() {
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(outer_col(0, "id")),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![
                (col(0, "id"), "id".into()),
                (
                    Expr::Subquery {
                        kind: basin_plan::SubqueryKind::Scalar,
                        subplan: Box::new(subplan),
                        operand: None,
                    },
                    "v".into(),
                ),
            ],
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let vs: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(vs, vec![Some(10), Some(20), Some(30), Some(40)]);
    }

    /// Postgres's NULL rule, per row: a row whose correlated subquery
    /// matches nothing gets NULL and is still returned. `x.id = t.id + 100`
    /// matches for no row here (OID 551 is int4 `+`), so the column is all
    /// NULL and all four rows survive — checked against PostgreSQL 18.2,
    /// which answers exactly that.
    #[test]
    fn a_correlated_subquery_matching_no_rows_is_null_for_that_row() {
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(Expr::Binary {
                        op: basin_plan::OpId(basin_pgtype::Oid(551)),
                        lhs: Box::new(outer_col(0, "id")),
                        rhs: Box::new(Expr::Literal(Datum::Int32(100), PgType::INT4)),
                    }),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![
                (col(0, "id"), "id".into()),
                (
                    Expr::Subquery {
                        kind: basin_plan::SubqueryKind::Scalar,
                        subplan: Box::new(subplan),
                        operand: None,
                    },
                    "v".into(),
                ),
            ],
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let vs: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(vs, vec![None, None, None, None], "no match is NULL, not 0");
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            4,
            "and the outer row is kept, not dropped"
        );
    }

    /// A correlated subquery matching MORE than one row is Postgres's
    /// SQLSTATE 21000, raised at execution (the rows before the offending
    /// one may already have been emitted) rather than at build time — the
    /// uncorrelated case can be refused during build because it runs there,
    /// this one cannot. Live PostgreSQL 18.2 on the same shape:
    /// `ERROR: more than one row returned by a subquery used as an
    /// expression`.
    #[test]
    fn a_correlated_subquery_returning_two_rows_is_a_cardinality_violation() {
        // `x.id > t.id` matches 3 rows for id=1 — no equality, no unique
        // key, exactly the shape `opt::decorrelate` refuses to turn into a
        // join for this very reason (its trap 3).
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(97)),
                    lhs: Box::new(outer_col(0, "id")),
                    rhs: Box::new(col(0, "id")),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(subplan),
                    operand: None,
                },
                "v".into(),
            )],
        };
        let mut op = build(&plan, &resolver()).expect("builds — the violation is a runtime one");
        match op.next_batch() {
            Err(ExecError::CardinalityViolation(m)) => assert_eq!(
                m, "more than one row returned by a subquery used as an expression",
                "PostgreSQL 18.2's own wording for this error"
            ),
            other => panic!("expected a cardinality violation, got {other:?}"),
        }
    }

    /// An UNCORRELATED scalar subquery in the target list must still be
    /// folded once and reused — the fix narrows what gets materialized, and
    /// this is the half that must not change. `(SELECT max(id) FROM t)` is
    /// 4 for every row.
    #[test]
    fn an_uncorrelated_scalar_in_the_target_list_still_folds_to_one_value() {
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![
                (col(0, "id"), "id".into()),
                (
                    Expr::Subquery {
                        kind: basin_plan::SubqueryKind::Scalar,
                        subplan: Box::new(max_id_subplan()),
                        operand: None,
                    },
                    "m".into(),
                ),
            ],
        };
        let built = build(&plan, &resolver()).unwrap();
        let batches = drain(built);
        let ms: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ms, vec![Some(4), Some(4), Some(4), Some(4)]);
    }

    /// `WHERE` position, where `opt::decorrelate` leaves a correlated scalar
    /// subquery it cannot prove caps at one row (its trap 3). The same
    /// per-row evaluation applies, and the `Filter` must still hand its
    /// caller its INPUT's schema — the column `CorrelatedScalar` appended is
    /// projected back off above it. `t.v = (SELECT x.v FROM t x WHERE x.id =
    /// t.id)` is true for every row.
    #[test]
    fn a_correlated_scalar_in_a_where_clause_is_evaluated_per_row() {
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(outer_col(0, "id")),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(1, "v")),
                rhs: Box::new(Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(subplan),
                    operand: None,
                }),
            },
        };
        let built = build(&plan, &resolver()).unwrap();
        assert_eq!(
            built
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["id", "v"],
            "a Filter never widens its input's schema, subquery or not"
        );
        let batches = drain(built);
        let ids: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            ids,
            vec![Some(1), Some(2), Some(3), Some(4)],
            "every row's own v equals its own correlated subquery's v"
        );
    }

    /// Inside a `CASE` branch in the target list — one of the positions
    /// `opt::decorrelate` names as out of its reach. Nothing special is
    /// needed for it (the sink is threaded through every `Expr` variant, not
    /// through a list of blessed ones), and that is worth pinning down:
    /// `CASE WHEN id < 3 THEN (SELECT x.v FROM t x WHERE x.id = t.id) ELSE
    /// -1 END` is `10, 20, -1, -1`. Note the subquery is evaluated for every
    /// row, including the rows whose branch discards it — Basin's `Project`
    /// evaluates both arms of a `CASE` and selects, so a subquery whose
    /// per-row evaluation ERRORS would error on rows Postgres never
    /// evaluates it for. That is a pre-existing property of `CASE` here, not
    /// something this operator introduces, and it is not exercised by this
    /// test.
    #[test]
    fn a_correlated_scalar_inside_a_case_branch_is_evaluated_per_row() {
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(outer_col(0, "id")),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![(
                Expr::Case {
                    operand: None,
                    whens: vec![(
                        Expr::Binary {
                            // int4 <
                            op: basin_plan::OpId(basin_pgtype::Oid(97)),
                            lhs: Box::new(col(0, "id")),
                            rhs: Box::new(Expr::Literal(Datum::Int32(3), PgType::INT4)),
                        },
                        Expr::Subquery {
                            kind: basin_plan::SubqueryKind::Scalar,
                            subplan: Box::new(subplan),
                            operand: None,
                        },
                    )],
                    else_: Some(Box::new(Expr::Literal(Datum::Int32(-1), PgType::INT4))),
                },
                "v".into(),
            )],
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let vs: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(vs, vec![Some(10), Some(20), Some(-1), Some(-1)]);
    }

    /// A correlated scalar subquery in a position with nowhere to hang a
    /// per-row evaluation is REFUSED, not folded once. `Sort` is such a
    /// position; the refusal is a `BuildError`, which the engine bridge
    /// turns into a fallback — a clean "this engine cannot serve it",
    /// which is worth strictly more than a plausible wrong answer.
    #[test]
    fn a_correlated_scalar_somewhere_unsupported_is_refused_not_folded() {
        let plan = LogicalPlan::Sort {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            keys: vec![PlanSortKey {
                expr: Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(correlated_count_subplan()),
                    operand: None,
                },
                descending: false,
                nulls_first: false,
            }],
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("a correlated scalar subquery in a Sort key must be refused"),
        };
        assert_eq!(
            err,
            BuildError::Unsupported("correlated scalar subquery in this position".into())
        );
    }

    /// Two levels of correlation are refused rather than guessed at:
    /// lowering tags "one level up" and "two levels up" with the same
    /// `OUTER_REF`, so a subquery correlated inside a correlated subquery
    /// cannot be resolved to a specific row. See `lower/select.rs`'s
    /// `ScopeResolver`, which documents the collapse.
    #[test]
    fn correlation_nested_inside_correlation_is_refused() {
        let inner = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(outer_col(0, "id")),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        // The OUTER subquery is correlated too, and its target list holds
        // the inner correlated one.
        let outer_sub = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(outer_col(0, "id")),
                },
            }),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(inner),
                    operand: None,
                },
                "v".into(),
            )],
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(outer_sub),
                    operand: None,
                },
                "v".into(),
            )],
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("two levels of correlation must be refused"),
        };
        assert_eq!(
            err,
            BuildError::Unsupported("correlated subquery nested inside another subquery".into())
        );
    }

    /// The same refusal from the other direction, and the reason it cannot
    /// be narrowed to "correlated subquery inside a CORRELATED subquery":
    /// here the middle subquery is UNCORRELATED (it would be folded once for
    /// the statement) and the innermost one is correlated — to the middle
    /// level, or to the outermost, with nothing in the plan able to say
    /// which. Folding the middle would carry the ambiguity inside; the
    /// refusal is what stops that.
    #[test]
    fn correlation_inside_an_uncorrelated_subquery_is_refused_too() {
        let inner = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "id")),
                    rhs: Box::new(outer_col(0, "id")),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        let middle = LogicalPlan::Project {
            // No `OUTER_REF` of its own — this subquery is uncorrelated.
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(inner),
                    operand: None,
                },
                "v".into(),
            )],
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(middle),
                    operand: None,
                },
                "v".into(),
            )],
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("an ambiguous nested correlation must be refused"),
        };
        assert_eq!(
            err,
            BuildError::Unsupported("correlated subquery nested inside another subquery".into())
        );
    }

    /// An UNCORRELATED subquery nested inside a correlated one is fine — it
    /// folds once per per-row rebuild, and the tables it scans are in the
    /// snapshot the factory resolves against (which is why `snapshot_scans`
    /// walks expressions as well as plan children). `(SELECT x.v FROM t x
    /// WHERE x.id = t.id AND x.id <= (SELECT max(id) FROM t))` is every
    /// row's own `v`, since `max(id)` is 4.
    #[test]
    fn an_uncorrelated_subquery_nested_inside_a_correlated_one_still_resolves() {
        let subplan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Binary {
                    // AND — `eval`'s own conjunction operator id.
                    op: basin_plan::OpId(basin_pgtype::Oid(u32::MAX)),
                    lhs: Box::new(Expr::Binary {
                        op: basin_plan::OpId(basin_pgtype::Oid(96)),
                        lhs: Box::new(col(0, "id")),
                        rhs: Box::new(outer_col(0, "id")),
                    }),
                    rhs: Box::new(Expr::Binary {
                        // int4 <=
                        op: basin_plan::OpId(basin_pgtype::Oid(523)),
                        lhs: Box::new(col(0, "id")),
                        rhs: Box::new(Expr::Subquery {
                            kind: basin_plan::SubqueryKind::Scalar,
                            subplan: Box::new(max_id_subplan()),
                            operand: None,
                        }),
                    }),
                },
            }),
            exprs: vec![(col(1, "v"), "v".into())],
        };
        let plan = LogicalPlan::Project {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(subplan),
                    operand: None,
                },
                "v".into(),
            )],
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let vs: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(vs, vec![Some(10), Some(20), Some(30), Some(40)]);
    }

    // ── CTE ──────────────────────────────────────────────────────────────

    /// A `WITH x AS (...) SELECT * FROM x` plan reaches the
    /// `CteBuffer`/`CteReader` operators through the builder — the whole
    /// point of wiring `LogicalPlan::Cte`/`CteRef` at all.
    #[test]
    fn a_non_recursive_cte_executes_end_to_end() {
        let plan = LogicalPlan::Cte {
            name: basin_plan::CteId(0),
            recursive: false,
            body: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            input: Box::new(LogicalPlan::CteRef {
                name: basin_plan::CteId(0),
                schema: vec![
                    ("id".into(), basin_pgtype::PgType::INT4),
                    ("v".into(), basin_pgtype::PgType::INT4),
                ],
            }),
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 4);
    }

    /// A CTE referenced TWICE must return the FULL result both times — a
    /// replay that drained the body on the first reference would silently
    /// leave the second with zero rows. `cte.rs`'s own
    /// `cte_referenced_twice_returns_full_results_both_times` proves this
    /// one layer down (`CteBuffer`/`CteReader` directly); this proves the
    /// BUILDER actually wires two `CteRef`s to two independent
    /// `CteReader`s off one `CteBuffer`, rather than, say, re-running the
    /// body twice (which would also pass a row-count check but violate the
    /// "materialize once" contract `cte.rs`'s module docs describe).
    #[test]
    fn a_cte_referenced_twice_via_the_builder_returns_full_results_both_times() {
        let cte_ref = || LogicalPlan::CteRef {
            name: basin_plan::CteId(0),
            schema: vec![
                ("id".into(), basin_pgtype::PgType::INT4),
                ("v".into(), basin_pgtype::PgType::INT4),
            ],
        };
        let plan = LogicalPlan::Cte {
            name: basin_plan::CteId(0),
            recursive: false,
            body: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            input: Box::new(LogicalPlan::SetOp {
                left: Box::new(cte_ref()),
                right: Box::new(cte_ref()),
                op: basin_plan::SetOpKind::Union,
                all: true,
            }),
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            rows, 8,
            "4 rows from each of the two references to the same CTE, not 4+0"
        );
    }

    /// A `CteRef` to a `CteId` nothing registered is a planner bug, not a
    /// user error, and must be REPORTED — not silently built as an empty
    /// relation, which would look like a valid (if surprising) answer
    /// instead of the broken plan it actually is.
    #[test]
    fn an_unregistered_cte_ref_is_reported_not_emptied() {
        let plan = LogicalPlan::CteRef {
            name: basin_plan::CteId(7),
            schema: vec![("x".into(), basin_pgtype::PgType::INT8)],
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("an unregistered CteRef must not silently build as empty"),
        };
        assert_eq!(err, BuildError::UnknownCte(basin_plan::CteId(7)));
    }

    // ── WITH RECURSIVE ──────────────────────────────────────────────────

    /// `WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE
    /// n < 5) SELECT n FROM t` — the classic bounded counter (verified live
    /// against Postgres in `recursive.rs`'s module docs), reaching
    /// `RecursiveCte` through the builder for the first time. Previously
    /// this operator was only exercised directly, with no planner path to
    /// it at all.
    #[test]
    fn with_recursive_bounded_counter_executes_end_to_end() {
        let cte_ref = LogicalPlan::CteRef {
            name: basin_plan::CteId(0),
            schema: vec![("n".into(), PgType::INT4)],
        };
        let anchor = LogicalPlan::Values {
            rows: vec![vec![Expr::Literal(Datum::Int32(1), PgType::INT4)]],
            schema: vec![("n".into(), PgType::INT4)],
        };
        let recursive_term = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(cte_ref.clone()),
                // n < 5 — OID 97 is int4 '<', verified against pg_operator.
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(97)),
                    lhs: Box::new(col(0, "n")),
                    rhs: Box::new(Expr::Literal(Datum::Int32(5), PgType::INT4)),
                },
            }),
            exprs: vec![(
                // n + 1 — OID 551 is int4 '+'.
                Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(551)),
                    lhs: Box::new(col(0, "n")),
                    rhs: Box::new(Expr::Literal(Datum::Int32(1), PgType::INT4)),
                },
                "n".into(),
            )],
        };
        let plan = LogicalPlan::Cte {
            name: basin_plan::CteId(0),
            recursive: true,
            body: Box::new(LogicalPlan::SetOp {
                left: Box::new(anchor),
                right: Box::new(recursive_term),
                op: basin_plan::SetOpKind::Union,
                all: true,
            }),
            input: Box::new(cte_ref),
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let mut values: Vec<i32> = batches.iter().flat_map(|b| col_i32(b, 0)).collect();
        values.sort();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    fn col_i32(batch: &RecordBatch, i: usize) -> Vec<i32> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect()
    }

    // ── LATERAL ──────────────────────────────────────────────────────────

    fn two_table_resolver() -> MemTableResolver {
        let mut r = MemTableResolver::new();
        let outer_schema = Arc::new(Schema::new(vec![Field::new("o", DataType::Int32, true)]));
        let outer_batch = RecordBatch::try_new(
            outer_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        r.insert(TableId(1), outer_schema, vec![outer_batch]);

        let inner_schema = Arc::new(Schema::new(vec![
            Field::new("fk", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let inner_batch = RecordBatch::try_new(
            inner_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 3])),
                Arc::new(Int32Array::from(vec![100, 101, 300])),
            ],
        )
        .unwrap();
        r.insert(TableId(2), inner_schema, vec![inner_batch]);
        r
    }

    /// `SELECT * FROM o CROSS JOIN LATERAL (SELECT * FROM t2 WHERE t2.fk =
    /// o.o)` — the correlated fallback path `LateralJoin` exists for,
    /// reached through the builder for the first time. `o`=2 has no
    /// matching `t2` rows and must be dropped entirely (Inner LATERAL
    /// semantics — see `lateral.rs`'s own
    /// `inner_lateral_drops_outer_row_with_zero_inner_rows`); `o`=1
    /// multiplies into two rows, `o`=3 into one.
    #[test]
    fn a_lateral_join_executes_end_to_end_with_correlation() {
        let outer_plan = LogicalPlan::Scan {
            table: TableId(1),
            projection: vec![ColId(0)],
            filters: vec![],
            snapshot: SnapshotId(0),
        };
        let inner_plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: TableId(2),
                projection: vec![ColId(0), ColId(1)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            // t2.fk = o.o — OID 96 is int4 '='. `relation: 1` marks the
            // outer reference — see `OUTER_REF`.
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "fk")),
                rhs: Box::new(Expr::Column(ColumnRef {
                    relation: 1,
                    index: 0,
                    name: "o".into(),
                })),
            },
        };
        let plan = LogicalPlan::LateralJoin {
            outer: Box::new(outer_plan),
            inner: Box::new(inner_plan),
            kind: basin_plan::JoinKind::Inner,
        };
        let batches = drain(build(&plan, &two_table_resolver()).unwrap());
        let mut pairs: Vec<(i32, i32)> = batches
            .iter()
            .flat_map(|b| {
                let o = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .clone();
                let fk = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .clone();
                (0..b.num_rows()).map(move |i| (o.value(i), fk.value(i)))
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![(1, 1), (1, 1), (3, 3)],
            "o=2 has no matching t2 rows and is dropped under Inner LATERAL"
        );
    }

    // ── DML ──────────────────────────────────────────────────────────────

    fn dml_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    /// `INSERT INTO t (id, val) VALUES (...) RETURNING id, val` reaches the
    /// `Insert` operator through the builder for the first time —
    /// previously it was only exercised directly in `dml.rs`, with no
    /// planner path to it.
    #[test]
    fn an_insert_executes_end_to_end_with_returning() {
        let schema = dml_schema();
        let mut dml = MemDmlResolver::new();
        let sink = dml.insert_table(TableId(1), schema.clone(), vec![0]);

        let plan = LogicalPlan::Insert {
            table: TableId(1),
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![
                        Expr::Literal(Datum::Int64(1), PgType::INT8),
                        Expr::Literal(Datum::Utf8("a".into()), PgType::TEXT),
                    ],
                    vec![
                        Expr::Literal(Datum::Int64(2), PgType::INT8),
                        Expr::Literal(Datum::Utf8("b".into()), PgType::TEXT),
                    ],
                ],
                schema: vec![("id".into(), PgType::INT8), ("val".into(), PgType::TEXT)],
            }),
            columns: vec![ColId(0), ColId(1)],
            on_conflict: None,
            returning: Some(vec![
                (col(0, "id"), "id".into()),
                (col(1, "val"), "val".into()),
            ]),
        };
        let batches =
            drain(build_with_dml(&plan, &resolver(), &dml, DEFAULT_OPERATOR_BUDGET).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "RETURNING yields both inserted rows");
        assert_eq!(sink.borrow().len(), 2);
    }

    /// `INSERT` reached through the STABLE, 2-argument `build()` (no write
    /// resolver available) must be refused cleanly, not panic or silently
    /// drop the write — `build()`'s signature never changed to require a
    /// `DmlResolver` every caller would otherwise have to acquire.
    #[test]
    fn insert_via_build_without_a_write_resolver_is_refused() {
        let plan = LogicalPlan::Insert {
            table: TableId(1),
            input: Box::new(LogicalPlan::Values {
                rows: vec![],
                schema: vec![("id".into(), PgType::INT8)],
            }),
            columns: vec![ColId(0)],
            on_conflict: None,
            returning: None,
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("INSERT must not build without a write resolver"),
        };
        assert!(matches!(err, BuildError::Unsupported(ref s) if s.contains("INSERT")));
    }

    /// `UPDATE t SET val = 'new' WHERE id = 1 RETURNING id, val` — `Update`
    /// carries no explicit input plan (unlike `Insert`), so this proves the
    /// builder's own `Scan(table) + Filter(predicate) + Project(new
    /// values)` synthesis, not just the `Update` operator underneath it.
    #[test]
    fn an_update_executes_end_to_end_with_returning() {
        let schema = dml_schema();
        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["old1", "old2"])),
            ],
        )
        .unwrap();

        // The scan side (what UPDATE reads to find rows to change) and the
        // write side (what it writes through) are two independent mocks
        // here, the same way `TableResolver`/`DmlResolver` are two
        // independent traits — pre-populate both consistently, as a real
        // Storage-backed pair already would agree by construction.
        let mut tables = MemTableResolver::new();
        tables.insert(TableId(1), schema.clone(), vec![existing.clone()]);
        let mut dml = MemDmlResolver::new();
        let sink = dml.insert_table(TableId(1), schema.clone(), vec![0]);
        sink.borrow_mut().insert(&existing).unwrap();

        let plan = LogicalPlan::Update {
            table: TableId(1),
            set: vec![(
                ColId(1),
                Expr::Literal(Datum::Utf8("new".into()), PgType::TEXT),
            )],
            from: None,
            // id = 1 — OID 410 is int8 '='.
            predicate: Some(Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(410)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Literal(Datum::Int64(1), PgType::INT8)),
            }),
            returning: Some(vec![
                (col(0, "id"), "id".into()),
                (col(1, "val"), "val".into()),
            ]),
            snapshot: SnapshotId(0),
        };
        let batches = drain(build_with_dml(&plan, &tables, &dml, DEFAULT_OPERATOR_BUDGET).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "only id=1 matched the predicate");
        let vals: Vec<String> = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(vals, vec!["new"]);
        assert_eq!(
            sink.borrow().len(),
            2,
            "id=2 untouched, id=1 rewritten in place"
        );
    }

    /// `DELETE FROM t WHERE id = 2 RETURNING id` — same "no explicit
    /// input" synthesis as `UPDATE`, minus the new-value `Project`.
    #[test]
    fn a_delete_executes_end_to_end_with_returning() {
        let schema = dml_schema();
        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let mut tables = MemTableResolver::new();
        tables.insert(TableId(1), schema.clone(), vec![existing.clone()]);
        let mut dml = MemDmlResolver::new();
        let sink = dml.insert_table(TableId(1), schema.clone(), vec![0]);
        sink.borrow_mut().insert(&existing).unwrap();

        let plan = LogicalPlan::Delete {
            table: TableId(1),
            using: None,
            // id = 2 — OID 410 is int8 '='.
            predicate: Some(Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(410)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Literal(Datum::Int64(2), PgType::INT8)),
            }),
            returning: Some(vec![(col(0, "id"), "id".into())]),
            snapshot: SnapshotId(0),
        };
        let batches = drain(build_with_dml(&plan, &tables, &dml, DEFAULT_OPERATOR_BUDGET).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
        assert_eq!(sink.borrow().len(), 1, "id=2 removed, id=1 remains");
    }

    // ── QUANTIFIED SUBQUERIES: IN / NOT IN / ANY / ALL ───────────────────
    //
    // Before these, `bind_outer_rec` folded only `SubqueryKind::Scalar`;
    // `In`, `NotIn`, `Any` and `All` were rebuilt as `Expr::Subquery` and
    // reached `eval`, which answers every one of them with
    //   internal: subqueries must be decorrelated into a join (or a scalar
    //   materialized elsewhere) before scalar eval sees them
    // and the engine bridge turns that into a fallback. `opt::decorrelate`
    // is not the gap: it declines `NotIn` deliberately (its trap 1 — an
    // anti-join answers `true` where SQL says NULL), declines an
    // UNCORRELATED `IN` deliberately (its trap 2 — nothing to join on), and
    // says outright that it does not handle `ANY`/`ALL` at all. Every one of
    // those declines was correct; what was missing was somewhere for the
    // declined shape to go.
    //
    // EVERY expected value below was read off a live PostgreSQL 18.2
    // (`postgres://…/postgres`, `SELECT version()` → 18.2) against the exact
    // tables these fixtures reproduce, and is quoted in each test. None of it
    // is derived from reasoning about what three-valued logic ought to say.

    /// The uncorrelated fixture, transcribed from the live server:
    ///
    /// ```sql
    /// CREATE TABLE outer_t(x int); INSERT INTO outer_t VALUES (1),(3),(NULL);
    /// CREATE TABLE s_full(v int);  INSERT INTO s_full  VALUES (1),(2);
    /// CREATE TABLE s_null(v int);  INSERT INTO s_null  VALUES (1),(NULL);
    /// CREATE TABLE s_empty(v int); -- no rows
    /// ```
    ///
    /// `x` covers the three cases that matter for every form: a value the
    /// subquery contains, a value it does not, and NULL.
    fn quantified_resolver() -> MemTableResolver {
        fn one_int_col(name: &str, values: Vec<Option<i32>>) -> (Arc<Schema>, RecordBatch) {
            let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(values)) as arrow_array::ArrayRef],
            )
            .unwrap();
            (schema, batch)
        }
        let mut r = MemTableResolver::new();
        for (id, name, values) in [
            (OUTER_T, "x", vec![Some(1), Some(3), None]),
            (S_FULL, "v", vec![Some(1), Some(2)]),
            (S_NULL, "v", vec![Some(1), None]),
            (S_EMPTY, "v", vec![]),
        ] {
            let (schema, batch) = one_int_col(name, values);
            r.insert(id, schema, vec![batch]);
        }
        r
    }

    const OUTER_T: TableId = TableId(1);
    const S_FULL: TableId = TableId(2);
    const S_NULL: TableId = TableId(3);
    const S_EMPTY: TableId = TableId(4);

    fn one_col_scan(table: TableId) -> LogicalPlan {
        LogicalPlan::Scan {
            table,
            projection: vec![ColId(0)],
            filters: vec![],
            snapshot: SnapshotId(0),
        }
    }

    /// Read a boolean column back as `Option<bool>` — the only shape that can
    /// tell SQL's three values apart. A `Vec<bool>` would collapse NULL onto
    /// false, which is precisely the bug this whole section is about.
    fn three_valued(batches: &[RecordBatch], column: usize) -> Vec<Option<bool>> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(column)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("a quantified subquery decides to a boolean")
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// `SELECT (x <kind> (SELECT v FROM <set>)) FROM outer_t`, as the three
    /// values for `x` = 1, 3, NULL.
    fn quantified_answers(kind: basin_plan::SubqueryKind, set: TableId) -> Vec<Option<bool>> {
        let plan = LogicalPlan::Project {
            input: Box::new(one_col_scan(OUTER_T)),
            exprs: vec![(
                Expr::Subquery {
                    kind,
                    subplan: Box::new(one_col_scan(set)),
                    operand: Some(Box::new(col(0, "x"))),
                },
                "r".into(),
            )],
        };
        let batches = drain(build(&plan, &quantified_resolver()).unwrap());
        three_valued(&batches, 0)
    }

    /// ```text
    /// SELECT x, x IN (SELECT v FROM s_full), x IN (SELECT v FROM s_null),
    ///           x IN (SELECT v FROM s_empty) FROM outer_t;
    ///  x | in_full | in_null | in_empty
    /// ---+---------+---------+----------
    ///  1 | t       | t       | f
    ///  3 | f       |         | f
    ///    |         |         | f
    /// ```
    ///
    /// The middle column is the one worth staring at: `3 IN (1, NULL)` is
    /// **NULL**, not false — there is a value in the subquery that might have
    /// matched and the engine cannot say it did not.
    #[test]
    fn in_a_subquery_matches_postgres_three_valued_answers() {
        use basin_plan::SubqueryKind::In;
        assert_eq!(
            quantified_answers(In, S_FULL),
            vec![Some(true), Some(false), None],
            "x IN (1, 2)"
        );
        assert_eq!(
            quantified_answers(In, S_NULL),
            vec![Some(true), None, None],
            "x IN (1, NULL) — no match plus a NULL is NULL, not false"
        );
        assert_eq!(
            quantified_answers(In, S_EMPTY),
            vec![Some(false), Some(false), Some(false)],
            "x IN (empty) is false for every x, NULL included"
        );
    }

    /// ```text
    /// SELECT x, x NOT IN (SELECT v FROM s_full), x NOT IN (SELECT v FROM s_null),
    ///           x NOT IN (SELECT v FROM s_empty) FROM outer_t;
    ///  x | notin_full | notin_null | notin_empty
    /// ---+------------+------------+-------------
    ///  1 | f          | f          | t
    ///  3 | t          |            | t
    ///    |            |            | t
    /// ```
    ///
    /// The classic footgun, and the reason `opt::decorrelate` refuses to turn
    /// `NOT IN` into an anti-join at all: with a NULL in the subquery the
    /// answer is **never true**. An anti-join would have returned row `x = 3`.
    ///
    /// Note `1 NOT IN (1, NULL)` is `false`, not NULL — an actual match still
    /// wins over the unknown, so "any NULL makes the whole thing NULL" is
    /// itself the wrong rule. Only rows with no match go NULL.
    #[test]
    fn not_in_a_subquery_is_never_true_when_the_subquery_has_a_null() {
        use basin_plan::SubqueryKind::NotIn;
        assert_eq!(
            quantified_answers(NotIn, S_FULL),
            vec![Some(false), Some(true), None],
            "x NOT IN (1, 2)"
        );
        assert_eq!(
            quantified_answers(NotIn, S_NULL),
            vec![Some(false), None, None],
            "x NOT IN (1, NULL) — never true; an anti-join would say true for x=3"
        );
        assert_eq!(
            quantified_answers(NotIn, S_EMPTY),
            vec![Some(true), Some(true), Some(true)],
            "x NOT IN (empty) is TRUE for every x — NULL included"
        );
    }

    /// `x = ANY (…)` is `IN` and `x <> ALL (…)` is `NOT IN`. Same server, same
    /// rows, same answers — asserted against the *other* form's expectations
    /// so the two implementations (an `InList`, versus a Kleene `OR`/`AND`
    /// fold of `=`/`<>`) cannot drift apart.
    ///
    /// ```text
    ///  x | eq_any_full | eq_any_null | eq_any_empty | ne_all_full | ne_all_null | ne_all_empty
    /// ---+-------------+-------------+--------------+-------------+-------------+--------------
    ///  1 | t           | t           | f            | f           | f           | t
    ///  3 | f           |             | f            | t           |             | t
    ///    |             |             | f            |             |             | t
    /// ```
    #[test]
    fn eq_any_is_in_and_ne_all_is_not_in() {
        use basin_plan::SubqueryKind::{All, Any, In, NotIn};
        // int4 '=' is oid 96, int4 '<>' is 518 — read from pg_operator.
        let eq = basin_plan::OpId(basin_pgtype::Oid(96));
        let ne = basin_plan::OpId(basin_pgtype::Oid(518));
        for set in [S_FULL, S_NULL, S_EMPTY] {
            assert_eq!(
                quantified_answers(Any(eq), set),
                quantified_answers(In, set),
                "x = ANY (…) must equal x IN (…) for {set:?}"
            );
            assert_eq!(
                quantified_answers(All(ne), set),
                quantified_answers(NotIn, set),
                "x <> ALL (…) must equal x NOT IN (…) for {set:?}"
            );
        }
        assert_eq!(
            quantified_answers(Any(eq), S_NULL),
            vec![Some(true), None, None]
        );
        assert_eq!(
            quantified_answers(All(ne), S_EMPTY),
            vec![Some(true), Some(true), Some(true)]
        );
    }

    /// A comparison other than `=`/`<>` follows the identical NULL rule, and
    /// `ANY`/`ALL` differ in exactly the way an `OR` fold differs from an
    /// `AND` one.
    ///
    /// ```text
    ///  x | gt_any_full | gt_any_null | gt_any_empty | gt_all_full | gt_all_null | gt_all_empty
    /// ---+-------------+-------------+--------------+-------------+-------------+--------------
    ///  1 | f           |             | f            | f           | f           | t
    ///  3 | t           | t           | f            | t           |             | t
    ///    |             |             | f            |             |             | t
    /// ```
    ///
    /// `1 > ANY (1, NULL)` is NULL (false OR unknown) while `3 > ANY (1,
    /// NULL)` is true (true OR unknown) — a true beats the unknown, a false
    /// does not. `ALL` is the mirror image: `1 > ALL (1, NULL)` is false
    /// (false AND unknown), `3 > ALL (1, NULL)` is NULL.
    #[test]
    fn gt_any_and_gt_all_follow_the_same_null_rule() {
        use basin_plan::SubqueryKind::{All, Any};
        // int4 '>' is oid 521 — read from pg_operator.
        let gt = basin_plan::OpId(basin_pgtype::Oid(521));
        assert_eq!(
            quantified_answers(Any(gt), S_FULL),
            vec![Some(false), Some(true), None]
        );
        assert_eq!(
            quantified_answers(Any(gt), S_NULL),
            vec![None, Some(true), None],
            "1 > ANY (1, NULL) is NULL; 3 > ANY (1, NULL) is true"
        );
        assert_eq!(
            quantified_answers(Any(gt), S_EMPTY),
            vec![Some(false), Some(false), Some(false)],
            "ANY over an empty subquery is false, NULL operand included"
        );
        assert_eq!(
            quantified_answers(All(gt), S_FULL),
            vec![Some(false), Some(true), None]
        );
        assert_eq!(
            quantified_answers(All(gt), S_NULL),
            vec![Some(false), None, None],
            "1 > ALL (1, NULL) is false; 3 > ALL (1, NULL) is NULL"
        );
        assert_eq!(
            quantified_answers(All(gt), S_EMPTY),
            vec![Some(true), Some(true), Some(true)],
            "ALL over an empty subquery is true, NULL operand included"
        );
    }

    /// `SELECT x FROM outer_t WHERE x IN (SELECT v FROM s_null)` — the probe's
    /// own shape, in a `WHERE` rather than a target list. Live server:
    ///
    /// ```text
    ///  x
    /// ---
    ///  1
    /// ```
    ///
    /// One row, not two: `WHERE` keeps only TRUE, and both the NULL rows are
    /// NULL rather than false — indistinguishable here, which is exactly why
    /// the target-list tests above exist as well.
    #[test]
    fn in_a_subquery_in_a_where_clause_keeps_only_the_true_rows() {
        let plan = LogicalPlan::Filter {
            input: Box::new(one_col_scan(OUTER_T)),
            predicate: Expr::Subquery {
                kind: basin_plan::SubqueryKind::In,
                subplan: Box::new(one_col_scan(S_NULL)),
                operand: Some(Box::new(col(0, "x"))),
            },
        };
        let batches = drain(build(&plan, &quantified_resolver()).unwrap());
        let xs: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(xs, vec![Some(1)]);
    }

    // ── CORRELATED quantified subqueries ─────────────────────────────────
    //
    // The uncorrelated cases above materialize the subquery once. These have
    // a different subquery result per outer row, so they go through
    // `CorrelatedScalar`'s per-row factory with a `CorrelatedKind::Quantified`
    // decider — which calls back into the SAME `quantified_expr` the
    // uncorrelated path uses, so there is one three-valued implementation and
    // not two.

    const CORR_T: TableId = TableId(1);
    const CORR_U: TableId = TableId(2);

    /// The correlated fixture, transcribed from the live server:
    ///
    /// ```sql
    /// CREATE TABLE t(id int, amt int);
    /// INSERT INTO t VALUES (1,5),(2,5),(3,5),(4,NULL);
    /// CREATE TABLE u(tid int, n int);
    /// INSERT INTO u VALUES (1,1),(1,2),(2,1),(2,NULL),(4,9);
    /// ```
    ///
    /// Per outer row the subquery `SELECT n FROM u WHERE u.tid = t.id` is a
    /// different relation: `{1,2}` for id=1, `{1,NULL}` for id=2, **empty**
    /// for id=3, `{9}` for id=4 (whose `amt` is itself NULL). One row for
    /// each of the four cases that matter.
    fn correlated_quantified_resolver() -> MemTableResolver {
        let t_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("amt", DataType::Int32, true),
        ]));
        let t = RecordBatch::try_new(
            t_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![Some(5), Some(5), Some(5), None])),
            ],
        )
        .unwrap();
        let u_schema = Arc::new(Schema::new(vec![
            Field::new("tid", DataType::Int32, true),
            Field::new("n", DataType::Int32, true),
        ]));
        let u = RecordBatch::try_new(
            u_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 4])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(1),
                    None,
                    Some(9),
                ])),
            ],
        )
        .unwrap();
        let mut r = MemTableResolver::new();
        r.insert(CORR_T, t_schema, vec![t]);
        r.insert(CORR_U, u_schema, vec![u]);
        r
    }

    /// `SELECT n FROM u WHERE u.tid = t.id` — correlated on the enclosing
    /// row's column 0 via `opt::decorrelate`'s `OUTER_REF` convention.
    fn correlated_n_subplan() -> LogicalPlan {
        LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: CORR_U,
                    projection: vec![ColId(0), ColId(1)],
                    filters: vec![],
                    snapshot: SnapshotId(0),
                }),
                // u.tid = <outer>.id — OID 96 is int4 '='.
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "tid")),
                    rhs: Box::new(Expr::Column(ColumnRef {
                        relation: OUTER_REF,
                        index: 0,
                        name: "id".into(),
                    })),
                },
            }),
            exprs: vec![(col(1, "n"), "n".into())],
        }
    }

    /// `SELECT amt <kind> (SELECT n FROM u WHERE u.tid = t.id) FROM t`, as the
    /// four values for id = 1, 2, 3, 4.
    fn correlated_quantified_answers(kind: basin_plan::SubqueryKind) -> Vec<Option<bool>> {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: CORR_T,
                projection: vec![ColId(0), ColId(1)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            exprs: vec![(
                Expr::Subquery {
                    kind,
                    subplan: Box::new(correlated_n_subplan()),
                    operand: Some(Box::new(col(1, "amt"))),
                },
                "r".into(),
            )],
        };
        let batches = drain(build(&plan, &correlated_quantified_resolver()).unwrap());
        three_valued(&batches, 0)
    }

    /// ```text
    /// SELECT id, amt,
    ///   amt > ALL (SELECT n FROM u WHERE u.tid = t.id),
    ///   amt > ANY (SELECT n FROM u WHERE u.tid = t.id),
    ///   amt IN     (SELECT n FROM u WHERE u.tid = t.id),
    ///   amt NOT IN (SELECT n FROM u WHERE u.tid = t.id)
    /// FROM t ORDER BY id;
    ///  id | amt | gt_all | gt_any | in_corr | notin_corr
    /// ----+-----+--------+--------+---------+------------
    ///   1 |   5 | t      | t      | f       | t
    ///   2 |   5 |        | t      |         |
    ///   3 |   5 | t      | f      | f       | t
    ///   4 |     |        |        |         |
    /// ```
    ///
    /// Row `id = 3` is the empty-subquery case *per row*: `ALL` is true and
    /// `ANY` is false for that row while every other row's subquery is
    /// non-empty — which a once-per-statement evaluation could not produce at
    /// all, and a "NULL when the subquery is empty" shortcut would get wrong
    /// in both directions.
    #[test]
    fn a_correlated_quantified_subquery_is_decided_per_outer_row() {
        use basin_plan::SubqueryKind::{All, Any, In, NotIn};
        let gt = basin_plan::OpId(basin_pgtype::Oid(521));
        assert_eq!(
            correlated_quantified_answers(All(gt)),
            vec![Some(true), None, Some(true), None],
            "amt > ALL (…): {{1,2}} → t, {{1,NULL}} → NULL, empty → t, NULL > {{9}} → NULL"
        );
        assert_eq!(
            correlated_quantified_answers(Any(gt)),
            vec![Some(true), Some(true), Some(false), None],
            "amt > ANY (…): the empty row is FALSE, not true and not NULL"
        );
        assert_eq!(
            correlated_quantified_answers(In),
            vec![Some(false), None, Some(false), None]
        );
        assert_eq!(
            correlated_quantified_answers(NotIn),
            vec![Some(true), None, Some(true), None],
            "5 NOT IN {{1,NULL}} is NULL, and NULL NOT IN {{9}} is NULL"
        );
    }

    /// The probe's own query, `SELECT id FROM t WHERE amt > ALL (SELECT n FROM
    /// u WHERE u.tid = t.id)`. Live server:
    ///
    /// ```text
    ///  id
    /// ----
    ///   1
    ///   3
    /// ```
    ///
    /// Two rows. `id = 2` and `id = 4` are NULL, not true, and `WHERE` drops
    /// both; `id = 3`'s empty subquery is TRUE and must survive.
    #[test]
    fn gt_all_a_correlated_subquery_in_a_where_clause() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: CORR_T,
                projection: vec![ColId(0), ColId(1)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            predicate: Expr::Subquery {
                kind: basin_plan::SubqueryKind::All(basin_plan::OpId(basin_pgtype::Oid(521))),
                subplan: Box::new(correlated_n_subplan()),
                operand: Some(Box::new(col(1, "amt"))),
            },
        };
        let op = build(&plan, &correlated_quantified_resolver()).unwrap();
        assert_eq!(
            op.schema().fields().len(),
            2,
            "the boolean column the decider added is projected back off — a \
             Filter must not change its input's schema"
        );
        let batches = drain(op);
        let ids: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, vec![Some(1), Some(3)]);
    }

    /// A correlated quantified subquery in a position with nowhere to hang a
    /// per-row evaluation is REFUSED — the same posture the correlated
    /// *scalar* case takes, and for the same reason: a clean fallback beats a
    /// plausible wrong answer. `Sort` is such a position.
    #[test]
    fn a_correlated_quantified_subquery_with_nowhere_to_go_is_refused() {
        let plan = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                table: CORR_T,
                projection: vec![ColId(0), ColId(1)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            keys: vec![PlanSortKey {
                expr: Expr::Subquery {
                    kind: basin_plan::SubqueryKind::In,
                    subplan: Box::new(correlated_n_subplan()),
                    operand: Some(Box::new(col(1, "amt"))),
                },
                descending: false,
                nulls_first: false,
            }],
        };
        match build(&plan, &correlated_quantified_resolver()) {
            Err(BuildError::Unsupported(m)) => assert!(
                m.contains("correlated IN/NOT IN/ANY/ALL subquery"),
                "got {m:?}"
            ),
            Err(other) => panic!("expected an Unsupported refusal, got {other:?}"),
            Ok(_) => panic!("a correlated quantified subquery under Sort must not build"),
        }
    }

    /// `eval` recognizes `AND`/`OR` only by the sentinel oids this file
    /// redefines, and implements both with arrow's Kleene kernels. If either
    /// half of that ever moved, `ANY`/`ALL` would stop being three-valued
    /// while still compiling — so it is pinned directly rather than only
    /// through the query-level tests above.
    ///
    /// The four rows are SQL's, checked on the live server:
    /// `SELECT true OR NULL, false OR NULL, true AND NULL, false AND NULL`
    /// → `t, NULL, NULL, f`.
    #[test]
    fn and_or_sentinels_still_evaluate_kleene() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef],
        )
        .unwrap();
        let lit = |b: Option<bool>| match b {
            Some(v) => Expr::Literal(Datum::Bool(v), PgType::BOOL),
            None => Expr::Literal(Datum::Null, PgType::BOOL),
        };
        for (op, lhs, want) in [
            (OR_OP, Some(true), Some(true)),
            (OR_OP, Some(false), None),
            (AND_OP, Some(true), None),
            (AND_OP, Some(false), Some(false)),
        ] {
            let e = Expr::Binary {
                op,
                lhs: Box::new(lit(lhs)),
                rhs: Box::new(lit(None)),
            };
            let got = crate::eval::eval(&e, &batch).expect("the sentinel still evaluates");
            let got = got
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("a boolean")
                .iter()
                .next()
                .unwrap();
            assert_eq!(got, want, "{lhs:?} {op:?} NULL");
        }
    }

    /// A `NOT NULL` column in the OUTER relation must not stop a correlated
    /// subquery from building.
    ///
    /// The type-probe row every correlated rebuild starts from is a one-row,
    /// all-NULL batch, and `RecordBatch::try_new` validates nullability — so
    /// building it against the child's own schema raised
    /// `Column 'id' is declared as non-nullable but contains null values`
    /// and declined the query, for both correlated *scalar* and correlated
    /// *quantified* subqueries. `CREATE TABLE t (id BIGINT NOT NULL, …)` is
    /// the probe corpus's own `t`, so this was not an edge case: it is why
    /// `SELECT id FROM t WHERE amt > ALL (SELECT n FROM u WHERE u.tid =
    /// t.id)` and `SELECT id, (SELECT count(*) FROM u WHERE u.tid = t.id)
    /// FROM t` both fell back. See [`null_probe_row`].
    ///
    /// Same data and same expected answers as
    /// [`a_correlated_quantified_subquery_is_decided_per_outer_row`] — only
    /// `t.id`'s nullability differs — so a failure here is unambiguously
    /// about nullability and nothing else.
    #[test]
    fn a_correlated_subquery_builds_over_a_not_null_outer_column() {
        let t_schema = Arc::new(Schema::new(vec![
            // NOT NULL — the whole point of the test.
            Field::new("id", DataType::Int32, false),
            Field::new("amt", DataType::Int32, true),
        ]));
        let t = RecordBatch::try_new(
            t_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![Some(5), Some(5), Some(5), None])),
            ],
        )
        .unwrap();
        let mut resolver = correlated_quantified_resolver();
        resolver.insert(CORR_T, t_schema, vec![t]);

        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: CORR_T,
                projection: vec![ColId(0), ColId(1)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::All(basin_plan::OpId(basin_pgtype::Oid(521))),
                    subplan: Box::new(correlated_n_subplan()),
                    operand: Some(Box::new(col(1, "amt"))),
                },
                "r".into(),
            )],
        };
        let op = build(&plan, &resolver)
            .expect("a NOT NULL outer column must not block the correlated type probe");
        assert_eq!(
            three_valued(&drain(op), 0),
            vec![Some(true), None, Some(true), None]
        );
    }

    /// The same nullability trap, on the correlated *scalar* path — the older
    /// of the two, and the one `correlated.rs` was written for. `SELECT amt,
    /// (SELECT max(n) FROM u WHERE u.tid = t.id) FROM t` over a `NOT NULL`
    /// `t.id`. Live server, same fixture:
    ///
    /// ```text
    ///  id | max
    /// ----+-----
    ///   1 |   2
    ///   2 |   1
    ///   3 |
    ///   4 |   9
    /// ```
    #[test]
    fn a_correlated_scalar_subquery_builds_over_a_not_null_outer_column() {
        let t_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amt", DataType::Int32, true),
        ]));
        let t = RecordBatch::try_new(
            t_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![Some(5), Some(5), Some(5), None])),
            ],
        )
        .unwrap();
        let mut resolver = correlated_quantified_resolver();
        resolver.insert(CORR_T, t_schema, vec![t]);

        // max(int4) is pg_proc oid 2116 — see `agg_func_of`.
        let max_n = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: CORR_U,
                    projection: vec![ColId(0), ColId(1)],
                    filters: vec![],
                    snapshot: SnapshotId(0),
                }),
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(96)),
                    lhs: Box::new(col(0, "tid")),
                    rhs: Box::new(Expr::Column(ColumnRef {
                        relation: OUTER_REF,
                        index: 0,
                        name: "id".into(),
                    })),
                },
            }),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2116)),
                args: vec![col(1, "n")],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        };
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Scan {
                table: CORR_T,
                projection: vec![ColId(0), ColId(1)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            exprs: vec![(
                Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(max_n),
                    operand: None,
                },
                "m".into(),
            )],
        };
        let op = build(&plan, &resolver)
            .expect("a NOT NULL outer column must not block the correlated type probe");
        let got: Vec<Option<i32>> = drain(op)
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec![Some(2), Some(1), None, Some(9)]);
    }

    /// A subquery bigger than [`MAX_QUANTIFIED_SUBQUERY_ROWS`] is declined,
    /// not folded into an expression tree of that size. A decline is a
    /// fallback; the alternative is a build that succeeds and then evaluates
    /// one kernel call per element per batch.
    #[test]
    fn an_oversized_quantified_subquery_is_declined_rather_than_unrolled() {
        let values: Vec<Expr> = (0..MAX_QUANTIFIED_SUBQUERY_ROWS + 1)
            .map(|i| Expr::Literal(Datum::Int32(i as i32), PgType::INT4))
            .collect();
        match quantified_expr(&basin_plan::SubqueryKind::In, &col(0, "x"), values) {
            Err(BuildError::Unsupported(m)) => assert!(m.contains("more than"), "got {m:?}"),
            other => panic!("expected a decline, got {other:?}"),
        }
    }
}
