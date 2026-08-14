//! `RecursiveCte` — `WITH RECURSIVE`, the standard SQL fixpoint iteration.
//!
//! # Why this file exists, and why it's separate from `cte.rs`
//!
//! `cte.rs`'s module docs already explain why non-recursive `WITH` bodies get
//! a materialize-once/replay-many shape (`CteBuffer`/`CteReader`) and say in
//! so many words that `WITH RECURSIVE` needs "an actual fixed-point loop
//! (repeatedly re-running the body against its own prior output until it
//! stops growing), which is a different operator shape than 'materialize
//! once, replay many', not a variant of it." This module is that different
//! shape. Per `docs/migration/df-removal/03-physical-operators.md`, `WITH
//! RECURSIVE` was, before this file, 100% DataFusion (`RecursiveQueryExec`)
//! with zero Basin-side implementation, despite being exercised by three
//! integration test suites — entirely greenfield.
//!
//! # The algorithm
//!
//! Standard SQL fixpoint evaluation (Postgres's own description of `WITH
//! RECURSIVE` in its documentation matches this exactly):
//!
//! ```text
//! working_table := anchor term
//! emit working_table
//! while working_table is not empty:
//!     working_table := recursive term, evaluated with the CTE's own name
//!                      bound to ONLY the previous working_table (not the
//!                      accumulated result so far)
//!     if UNION (not ALL): drop rows already seen in any earlier iteration
//!     emit working_table
//! ```
//!
//! [`RecursiveCte::new`] takes the anchor as a plain child [`Operator`] and
//! the recursive term as a *factory* — `FnMut(Vec<RecordBatch>) ->
//! Result<Box<dyn Operator>, ExecError>` — invoked once per iteration with
//! that iteration's input working table, rather than as a single
//! pre-built operator tree. Two reasons: the recursive term physically reads
//! a different working table every iteration (the previous iteration's
//! output, not a fixed input — see item 2 below), so it cannot be built
//! once, and taking a plain closure means this operator is fully testable
//! with hand-written test doubles, with no `basin-plan`/planner dependency
//! at all — exactly what the brief for this operator asked for.
//!
//! # Semantics, each verified against a live server (see `psql -U postgres`
//! # queries recorded below and in the tests) rather than recalled — this
//! # project's `cte.rs`/`setop.rs` module docs both note recall has been
//! # wrong here before, and it was wrong again while writing THIS file: the
//! # first draft assumed `UNION`'s dedup was per-iteration, and item 1 below
//! # is exactly the live query that proved that assumption wrong.
//!
//! 1. **`UNION` deduplicates against every row seen in ANY earlier
//!    iteration, not just within the iteration currently running** — and
//!    critically, this also applies to duplicates the ANCHOR term itself
//!    produces, not just to the recursive term. Verified live:
//!    ```sql
//!    WITH RECURSIVE t(n) AS (
//!      SELECT x FROM (VALUES (1),(1)) AS v(x)   -- anchor with a duplicate
//!      UNION
//!      SELECT n+1 FROM t WHERE n < 3
//!    )
//!    SELECT n FROM t ORDER BY n;
//!    -- n
//!    -- ---
//!    --  1
//!    --  2
//!    --  3
//!    -- (3 rows) — NOT 1,1,2,2,3,3, proving the anchor's own two `1`s
//!    -- collapse to one row before anything is fed to the recursive term.
//!    ```
//!    A dedup that only looked within one iteration's own batch would let
//!    the same row resurface every time a cycle revisits it, and on cyclic
//!    input (a graph with a cycle, a graph traversal that can revisit a
//!    node) that never converges to an empty working table — it loops
//!    forever. `UNION ALL` performs NO deduplication anywhere, ever. See
//!    [`union_dedups_across_the_whole_history_not_just_one_iteration_or_a_cycle_loops_forever`].
//! 2. **The recursive term sees ONLY the immediately preceding iteration's
//!    working table, never the full accumulated result.** Feeding it
//!    everything computed so far would recompute (and re-emit, modulo
//!    dedup) every prior iteration's work on every subsequent iteration —
//!    exponentially many duplicate rows before dedup even gets a chance to
//!    collapse them. See
//!    [`recursive_term_sees_only_the_previous_iterations_working_table_not_the_accumulated_result`].
//! 3. **Termination is "the working table came back empty", never a fixed
//!    iteration count.** A recursive term that converges in 2 iterations
//!    must stop at 2, not run on to some hard-coded number. See
//!    [`termination_is_an_empty_working_set_not_a_fixed_iteration_count`].
//! 4. **An iteration cap exists and fails LOUDLY (`ExecError::Internal`,
//!    naming the cap) rather than hanging.** Postgres itself has no such
//!    limit on `WITH RECURSIVE` — it relies entirely on `statement_timeout`
//!    to bound a runaway recursive query (see `basin-engine/src/session.rs`'s
//!    `DEFAULT_STATEMENT_TIMEOUT_MS` doc comment, which names exactly this
//!    threat: "an ill-bounded recursive CTE pinning a … worker thread").
//!    This crate's physical operators are pulled synchronously by whatever
//!    drives the tree, with no independent timer thread, so a genuinely
//!    non-terminating recursive term (a bug in the query, e.g. `UNION ALL`
//!    over a cycle) would otherwise hang the calling thread forever with no
//!    way for `statement_timeout` to intervene between pulls — a hang is a
//!    strictly worse failure mode than a clear error naming the cap. See
//!    [`a_non_terminating_recursive_term_hits_the_iteration_cap_and_errors_loudly`].
//! 5. **An empty anchor yields zero rows and the recursive term is never
//!    invoked at all** — not even once. Verified live:
//!    ```sql
//!    WITH RECURSIVE t(n) AS (
//!      SELECT 1 WHERE FALSE
//!      UNION ALL
//!      SELECT n+1 FROM t WHERE n < 5
//!    )
//!    SELECT n FROM t;
//!    -- (0 rows)
//!    ```
//!    See [`empty_anchor_yields_zero_rows_and_never_invokes_the_recursive_term`].
//! 6. **The recursive term's own output column names (and nullability) need
//!    not match the anchor's.** The CTE's exposed column names come from the
//!    anchor term (or an explicit `WITH x(a, b) AS (...)` column list),
//!    never from the recursive term — whose `SELECT` list is routinely left
//!    unaliased. Only the column TYPES, in position, must line up. See
//!    [`SchemaRebind`] and
//!    [`a_recursive_terms_unaliased_column_need_not_match_the_anchors_name`].
//! 7. **A `NOT NULL` anchor does not make the CTE's column `NOT NULL`** — the
//!    recursive term can contribute a NULL, and live Postgres serves exactly
//!    that (`SELECT 1 UNION ALL SELECT NULL::int FROM r WHERE n IS NOT NULL`
//!    returns 1 and a NULL). A recursive CTE is a `UNION`, and a `UNION`'s
//!    nullability is `left || right` — `setop.rs`'s rule, which this file
//!    shares rather than re-deciding. See [`recursive_cte_output_schema`] for
//!    the rule, why the `right` half is assumed rather than consulted here,
//!    and what it costs; and
//!    [`a_null_from_the_recursive_term_under_a_not_null_anchor`] for the query
//!    that failed without it.
//!
//! # The classic shapes, verified live against PostgreSQL 18.2
//!
//! ```sql
//! -- A bounded counter.
//! WITH RECURSIVE t(n) AS (
//!   SELECT 1
//!   UNION ALL
//!   SELECT n+1 FROM t WHERE n < 5
//! )
//! SELECT n FROM t;
//! --  n
//! -- ---
//! --  1
//! --  2
//! --  3
//! --  4
//! --  5
//! -- (5 rows)
//! ```
//! See [`the_classic_bounded_counter_union_all`].
//!
//! ```sql
//! -- A cyclic graph: UNION terminates via dedup; UNION ALL over the same
//! -- graph would never terminate (a cycle 1->2->3->1 keeps re-deriving
//! -- node 1 forever with no dedup to stop it) — this file cannot actually
//! -- run that forever in a test, so [`a_non_terminating_recursive_term_hits_the_iteration_cap_and_errors_loudly`]
//! -- demonstrates the same "keeps producing rows, never converges" shape
//! -- and asserts it hits the iteration cap rather than hanging.
//! CREATE TEMP TABLE edges(src int, dst int);
//! INSERT INTO edges VALUES (1,2),(2,3),(3,1),(3,4);
//! WITH RECURSIVE reach(node) AS (
//!   SELECT 1
//!   UNION
//!   SELECT e.dst FROM edges e JOIN reach r ON e.src = r.node
//! )
//! SELECT node FROM reach ORDER BY node;
//! --  node
//! -- ------
//! --     1
//! --     2
//! --     3
//! --     4
//! -- (4 rows)
//! ```
//! See [`cyclic_graph_reachability_terminates_via_union_dedup`], and for the
//! intra-iteration-duplicate variant (two edges into the same node within
//! ONE iteration, verified live to also collapse to a single row — `edges2`
//! adds `(1,3)` and `(3,4)` so node 4 is reachable via two different paths
//! landing in the same iteration) see
//! [`union_dedups_across_the_whole_history_not_just_one_iteration_or_a_cycle_loops_forever`].
//!
//! # The memory contract
//!
//! [`RecursiveCte`] holds, at any moment, at most ONE iteration's worth of
//! working-table rows (dropped and replaced the moment the next iteration's
//! recursive term is instantiated — see `next_batch`'s phase-transition
//! logic), plus — for `UNION` only, never for `UNION ALL` — the
//! ever-growing set of row keys already seen, the same `RowConverter`-backed
//! `HashSet<OwnedRow>` [`crate::setop::SetOp`] and [`crate::cte::Distinct`]
//! already use for the same "NULL equals NULL for set membership" purpose
//! (see `setop.rs`'s module docs for why that rule is the opposite of a join
//! key). It does NOT hold the full history of every iteration's raw batches
//! — an implementation that did would grow its memory footprint every
//! iteration even under `UNION ALL`, where nothing is ever deduplicated
//! away. See [`memory_used_reflects_only_the_current_working_table_not_the_accumulated_history`].

use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::take;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};

use crate::operator::{ExecError, Operator};

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

/// Reorder every column of `batch` by `indices`, keeping `batch`'s own
/// schema. Duplicated rather than shared with `setop.rs`'s identical helper
/// per that file's own precedent (see its doc comment on `take_batch`): each
/// operator file in this crate keeps its own copy.
fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch, ExecError> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices, None).map_err(arrow_err))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(arrow_err)
}

/// Whether two schemas have the same column TYPES in the same positions —
/// deliberately ignoring field names, nullability and metadata, all three of
/// which a `WITH RECURSIVE` recursive term is free to disagree with the
/// anchor on (see [`SchemaRebind`]'s doc comment). A real mismatch — wrong
/// arity, or a type that doesn't line up positionally — is still rejected by
/// the caller; this only widens what counts as compatible, never narrows it.
fn schema_types_match(a: &SchemaRef, b: &SchemaRef) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields()
            .iter()
            .zip(b.fields().iter())
            .all(|(x, y)| x.data_type() == y.data_type())
}

/// The schema a [`RecursiveCte`] over this `anchor` reports: the anchor's
/// column names, types and metadata, with every column's nullability widened
/// to nullable.
///
/// # Why the anchor's nullability is not the CTE's
///
/// `WITH RECURSIVE x AS (anchor UNION [ALL] recursive_term)` is a `UNION`, so
/// its nullability rule is `setop.rs`'s [`crate::setop`] `Union` arm — an
/// output column can hold a NULL if EITHER branch's can:
///
/// ```text
/// UNION      left || right        <- this operator, always
/// INTERSECT  left && right
/// EXCEPT     left
/// ```
///
/// Taking the anchor's nullability whole was `setop.rs`'s bug in exactly the
/// shape fixed in 02251a4c, one branch later: every batch this operator yields
/// — the anchor's own and every iteration's — is declared under ONE schema,
/// and `RecordBatch::try_new` rejects a `NOT NULL` field holding a NULL. With
/// a `NOT NULL` anchor and a recursive term that produces a NULL, the rebind
/// in [`SchemaRebind::next_batch`] failed with `Column 'n' is declared as
/// non-nullable but contains null values`, which the owned-engine bridge turns
/// into a silent fallback (and, once DataFusion is gone, an error). Live
/// PostgreSQL 18.2 serves that shape and returns the NULL:
///
/// ```sql
/// WITH RECURSIVE r(n) AS (
///   SELECT 1 UNION ALL SELECT NULL::int FROM r WHERE n IS NOT NULL)
/// SELECT n FROM r;
/// --  n
/// -- ---
/// --  1
/// --          <- NULL
/// -- (2 rows)
/// ```
/// See [`a_null_from_the_recursive_term_under_a_not_null_anchor`].
///
/// # Why `right` is assumed nullable rather than consulted
///
/// `SetOp` has both branches in hand at construction and evaluates `left ||
/// right` for real. This operator does not and cannot: the recursive term is a
/// [`RecursiveTermFactory`] that is only invoked once the anchor's working
/// table exists (module docs item 2), and its schema therefore does not exist
/// while `Operator::schema` — which the parent operators read at BUILD time,
/// before a single batch is pulled — is already being answered. Nor may this
/// operator invoke the factory early to find out: module docs item 5 requires
/// that an empty anchor never invoke the recursive term at all (see
/// [`empty_anchor_yields_zero_rows_and_never_invokes_the_recursive_term`]),
/// and building the term is not free of failure or side effects.
///
/// So `right` is unknown, and the only sound value for an unknown branch in
/// `left || right` is `true`. The rule is unchanged; it is evaluated with the
/// only information available. That is a genuine loss of precision — DataFusion
/// declares `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r
/// WHERE n < 5)`'s `n` as `NOT NULL` and is right about the data — and it is
/// the safe direction: a column declared nullable that never holds a NULL
/// costs precision, while a column declared `NOT NULL` that does hold one
/// costs the query. Postgres itself does not make the precise claim either —
/// its planner keeps a `WHERE n IS NOT NULL` filter over a recursive CTE scan
/// even when the values are provably non-NULL, where it DELETES that filter
/// over a `UNION ALL` of two `NOT NULL` branches (`EXPLAIN (COSTS OFF)`,
/// PostgreSQL 18.2).
///
/// Measured, not assumed: the golden corpus records that query's `n` as
/// `notnull`, so this widening adds a `NULLABILITY: expected false, actual
/// true` line to `base/CTEs #0154` — a query that ALREADY diverges there on
/// type (`expected Int64, actual Int32`). Diverging queries stayed at 75, the
/// CTE area at 1/10, and the owned engine still served 273 of 440 statements,
/// before and after. The cost is one line of detail on an existing divergence,
/// not a query.
///
/// Recovering the precision needs the recursive term's schema at construction,
/// which means building the term once in `build.rs`'s `build_recursive_cte`
/// and passing it in — a change to a file this operator does not reach into.
/// `pub` so that when that happens there is ONE definition of the CTE's output
/// schema rather than a second copy of the rule over there: `build.rs` also
/// declares the working-table `VecFeed` the recursive term reads (its
/// `schema_for_feed`) and today gives it the anchor's schema, which is this
/// same over-claim one level down.
pub fn recursive_cte_output_schema(anchor: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = anchor
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Arc::new(Schema::new_with_metadata(fields, anchor.metadata().clone()))
}

/// Wraps an operator whose column TYPES match `target` positionally but
/// whose field names/nullability/metadata may not, and rebinds every batch
/// it yields to `target` verbatim — the underlying column data is untouched,
/// only the reported schema changes.
///
/// # Why this exists
///
/// Postgres does not require a `WITH RECURSIVE` recursive term's own output
/// column names to match the anchor term's. The CTE's exposed column names
/// come from the anchor (or an explicit `WITH x(a, b) AS (...)` column
/// list), never from the recursive term — and the recursive term's `SELECT`
/// list is routinely left unaliased, which gets Postgres's own synthetic
/// `?column?` name. Verified live against PostgreSQL 18.2:
/// ```sql
/// WITH RECURSIVE r AS (
///   SELECT 1 AS n
///   UNION ALL
///   SELECT n+1 FROM r WHERE n < 5   -- no `AS n` here
/// )
/// SELECT n FROM r;
/// --  n
/// -- ---
/// --  1
/// --  2
/// --  3
/// --  4
/// --  5
/// -- (5 rows) -- succeeds; the recursive term's own column is unnamed.
/// ```
/// Before this wrapper existed, `RecursiveCte::next_batch`'s schema check
/// compared the recursive term's `Schema` to the anchor's with plain `!=` —
/// full `arrow_schema::Schema` equality, which includes field names — so
/// this exact live-legal query was rejected as `ExecError::TypeMismatch`,
/// which the caller (`basin-engine`'s owned-engine bridge) turns into a
/// silent fallback to DataFusion rather than a served query. Renaming the
/// batch's schema here (rather than merely relaxing the equality check to
/// let mismatched names through unrenamed) keeps this operator's own
/// contract intact: [`Operator::schema`] documents "the Arrow schema of the
/// batches this operator yields", so every batch `RecursiveCte` actually
/// yields — anchor or any iteration — must carry exactly `self.schema`, not
/// merely something type-compatible with it.
///
/// The rebind CAN fail — `RecordBatch::try_new` rejects a `NOT NULL` field
/// holding a NULL — and did, for a `NOT NULL` anchor and a recursive term
/// that produced a NULL. `RecursiveCte` never hands it a target that can fail
/// that way any more: the only targets it passes are
/// [`recursive_cte_output_schema`]'s, which declares every column nullable.
/// The check is kept rather than swapped for an unchecked `RecordBatch::new`
/// so a future caller with a narrower target gets an error rather than an
/// invalid batch.
struct SchemaRebind {
    inner: Box<dyn Operator>,
    target: SchemaRef,
}

impl SchemaRebind {
    fn new(inner: Box<dyn Operator>, target: SchemaRef) -> Self {
        Self { inner, target }
    }
}

impl Operator for SchemaRebind {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.target)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        match self.inner.next_batch()? {
            // Column data is reused as-is (`schema_types_match` already
            // proved the data types line up positionally) — only the
            // `Schema` attached to the batch changes.
            Some(batch) => Ok(Some(
                RecordBatch::try_new(Arc::clone(&self.target), batch.columns().to_vec())
                    .map_err(arrow_err)?,
            )),
            None => Ok(None),
        }
    }

    fn memory_used(&self) -> usize {
        self.inner.memory_used()
    }
}

/// The recursive term, re-instantiated once per iteration against that
/// iteration's input working table (see the module docs' item 2). Returning
/// `Result` lets a factory reject an iteration's working table (e.g. a
/// planner-level `build()` failing to bind it) without this operator having
/// to invent a placeholder error variant on its behalf.
pub type RecursiveTermFactory =
    Box<dyn FnMut(Vec<RecordBatch>) -> Result<Box<dyn Operator>, ExecError>>;

/// Which term is currently being pulled from. `Done` is terminal: once
/// reached, every subsequent `next_batch` call returns `Ok(None)` without
/// touching anything else.
enum Stage {
    /// Pulling from the anchor term (conceptually "iteration 0" — see
    /// `next_batch`'s doc comment on why the anchor and every recursive
    /// iteration share one code path).
    Anchor(Box<dyn Operator>),
    /// Pulling from the recursive term's operator for one iteration.
    Iteration(Box<dyn Operator>),
    Done,
}

/// `WITH RECURSIVE t(...) AS (anchor UNION [ALL] recursive_term) SELECT
/// ... FROM t` — see the module docs for the algorithm, the semantics each
/// test pins down, and the live-Postgres-verified classic shapes.
pub struct RecursiveCte {
    schema: SchemaRef,
    recursive_term: RecursiveTermFactory,
    all: bool,
    /// `None` when `all` is true: `UNION ALL` never dedups, so there is
    /// nothing to convert rows into a hashable key for.
    row_converter: Option<RowConverter>,
    /// Every row key ever kept, across every iteration including the
    /// anchor. Empty (and never touched) when `all` is true.
    seen: HashSet<OwnedRow>,
    seen_bytes: usize,
    /// The CURRENTLY RUNNING phase's surviving rows so far — becomes the
    /// working table handed to the next iteration's recursive term once
    /// this phase's operator is exhausted, then is reset to empty. This is
    /// deliberately never more than one iteration's worth — see the module
    /// docs' "memory contract" section.
    working_set: Vec<RecordBatch>,
    working_set_rows: usize,
    working_set_bytes: usize,
    budget: usize,
    /// Number of recursive-term invocations made so far. The anchor does
    /// not count — it is not a recursive-term invocation at all.
    iteration: usize,
    max_iterations: usize,
    stage: Stage,
}

impl RecursiveCte {
    /// `anchor` is the non-recursive term's already-built operator.
    /// `recursive_term` is invoked once per iteration, given that
    /// iteration's input working table (a plain `Vec<RecordBatch>`, in
    /// whatever batch boundaries the previous phase happened to produce —
    /// no boundary significance is implied); it must return an operator
    /// whose schema matches `anchor`'s, checked at each invocation (see
    /// `next_batch`), not assumed.
    ///
    /// `all` selects `UNION ALL` (`true`, no dedup at all) vs. `UNION`
    /// (`false`, dedup against every row seen in any earlier iteration —
    /// module docs item 1). `memory_budget` bounds `seen_bytes +
    /// working_set_bytes` (module docs' "memory contract"). `max_iterations`
    /// is the loud-failure cap of module docs item 4 — Postgres has none and
    /// relies on `statement_timeout` instead; this crate's operators are
    /// pulled synchronously with no independent timer, so without a cap a
    /// non-terminating recursive term would hang the calling thread forever
    /// rather than erroring.
    pub fn new(
        anchor: Box<dyn Operator>,
        recursive_term: RecursiveTermFactory,
        all: bool,
        memory_budget: usize,
        max_iterations: usize,
    ) -> Self {
        let anchor_schema = anchor.schema();
        // NOT the anchor's schema: see `recursive_cte_output_schema` for the
        // `UNION` rule this operator's output obeys and why the anchor's own
        // nullability is not it.
        let schema = recursive_cte_output_schema(&anchor_schema);
        // The anchor's batches are yielded under that schema too, so they get
        // the same rebind an iteration's do — `Operator::schema` promises
        // EVERY batch carries exactly the reported schema, and the anchor is
        // conceptually just iteration 0 (see `next_batch`). Widening a `NOT
        // NULL` column to nullable can never fail: it claims less about data
        // that is already there, the opposite direction from the failure
        // being fixed.
        let anchor: Box<dyn Operator> = if anchor_schema == schema {
            anchor
        } else {
            Box::new(SchemaRebind::new(anchor, Arc::clone(&schema)))
        };
        let row_converter = if all {
            None
        } else {
            let fields: Vec<SortField> = schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect();
            Some(
                RowConverter::new(fields)
                    .expect("WITH RECURSIVE's output columns support row encoding"),
            )
        };
        Self {
            schema,
            recursive_term,
            all,
            row_converter,
            seen: HashSet::new(),
            seen_bytes: 0,
            working_set: Vec::new(),
            working_set_rows: 0,
            working_set_bytes: 0,
            budget: memory_budget,
            iteration: 0,
            max_iterations,
            stage: Stage::Anchor(anchor),
        }
    }

    /// Filter `batch` down to rows not already in `self.seen` (module docs
    /// item 1), inserting the survivors' keys as it goes — a no-op
    /// passthrough when `all` is true. Charges `seen_bytes` and checks the
    /// combined budget per newly-seen row, mirroring `Distinct::next_batch`'s
    /// per-row check in `cte.rs`.
    fn dedup_against_history(&mut self, batch: RecordBatch) -> Result<RecordBatch, ExecError> {
        let Some(converter) = &self.row_converter else {
            return Ok(batch);
        };
        let rows = converter
            .convert_columns(batch.columns())
            .map_err(arrow_err)?;
        let mut keep: Vec<u32> = Vec::new();
        for i in 0..batch.num_rows() {
            let key = rows.row(i).owned();
            // NULL equals NULL here, same rule and same reasoning as
            // `setop.rs`'s `SetOp`/`Distinct` — set membership, not a join
            // key.
            if self.seen.insert(key) {
                self.seen_bytes += std::mem::size_of::<OwnedRow>();
                if self.seen_bytes + self.working_set_bytes > self.budget {
                    return Err(ExecError::OutOfMemory {
                        requested: self.seen_bytes + self.working_set_bytes,
                        budget: self.budget,
                    });
                }
                keep.push(i as u32);
            }
        }
        take_batch(&batch, &UInt32Array::from(keep))
    }

    /// Accumulate `batch` into the current phase's working table, charging
    /// and checking the combined memory budget.
    fn buffer_into_working_set(&mut self, batch: &RecordBatch) -> Result<(), ExecError> {
        // The batch itself must be retained, not just measured: the next
        // iteration's recursive term is handed exactly this working table.
        // Counting the rows without keeping them leaves `working_set` empty,
        // `mem::take` yields nothing, and the recursion emits the anchor and
        // stops — which looks like correct termination rather than a bug.
        self.working_set.push(batch.clone());
        self.working_set_rows += batch.num_rows();
        self.working_set_bytes += batch.get_array_memory_size();
        if self.seen_bytes + self.working_set_bytes > self.budget {
            return Err(ExecError::OutOfMemory {
                requested: self.seen_bytes + self.working_set_bytes,
                budget: self.budget,
            });
        }
        Ok(())
    }
}

impl Operator for RecursiveCte {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// One bounded unit of work per call — never an internal loop over many
    /// iterations — so the caller regains control between pulls and
    /// `statement_timeout` cancellation checks stay meaningful (see
    /// `operator.rs`'s trait doc on why an operator that loops internally
    /// until exhaustion is a bug, and module docs item 4 for why this
    /// operator ALSO has an explicit iteration cap on top of that
    /// discipline).
    ///
    /// The anchor and every recursive iteration share this one code path —
    /// `Stage::Anchor` and `Stage::Iteration` are both "pull one batch from
    /// the currently active operator, dedup it against the whole history,
    /// buffer it as this phase's working table, emit it". Unifying them is
    /// what makes module docs item 5 (an empty anchor never invokes the
    /// recursive term at all) fall out for free, rather than needing a
    /// special case: "the just-finished phase's working table came back
    /// empty" is checked identically whether that phase was the anchor or
    /// iteration N, and the recursive term is only ever instantiated from
    /// that ONE check failing to be empty.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        loop {
            let op = match &mut self.stage {
                Stage::Anchor(op) | Stage::Iteration(op) => op,
                Stage::Done => return Ok(None),
            };

            match op.next_batch()? {
                Some(batch) => {
                    // Item 1: dedup against the ENTIRE history so far
                    // (every earlier iteration, including the anchor), not
                    // just this batch or this iteration.
                    let survivors = self.dedup_against_history(batch)?;
                    self.buffer_into_working_set(&survivors)?;
                    // Mirrors `Distinct`'s contract in `cte.rs`: a batch
                    // that survived dedup down to zero rows is still
                    // `Some(empty)`, not `None` — there may be more batches
                    // behind it in this same phase.
                    return Ok(Some(survivors));
                }
                None => {
                    // This phase (the anchor, or one recursive iteration)
                    // is exhausted. Item 3: termination is decided PURELY
                    // by whether its working table came back empty — never
                    // by any iteration count.
                    if self.working_set_rows == 0 {
                        self.stage = Stage::Done;
                        return Ok(None);
                    }

                    // Item 4: the loud-failure cap. Checked BEFORE
                    // instantiating another recursive-term operator, so a
                    // capped query never runs one iteration more than
                    // `max_iterations` regardless of how expensive building
                    // that operator would have been.
                    self.iteration += 1;
                    if self.iteration > self.max_iterations {
                        return Err(ExecError::Internal(format!(
                            "WITH RECURSIVE exceeded its iteration cap of {} without reaching \
                             a fixpoint (an empty working table) — this is very likely a \
                             non-terminating recursive term. PostgreSQL itself has no such \
                             cap and instead relies on statement_timeout to bound a runaway \
                             recursive query; this operator adds one because it is pulled \
                             synchronously with no independent timer, so an uncapped \
                             non-terminating term would hang the calling thread forever \
                             rather than erroring.",
                            self.max_iterations
                        )));
                    }

                    // Item 2: the recursive term is handed ONLY the working
                    // table just finished — the `Vec` taken here — never
                    // anything from an earlier iteration. Resetting
                    // `working_set_bytes` to 0 here (rather than continuing
                    // to accumulate it) is what keeps this operator's
                    // memory contract "at most one iteration's worth", not
                    // "the whole history" — see the module docs.
                    let working_table = std::mem::take(&mut self.working_set);
                    self.working_set_rows = 0;
                    self.working_set_bytes = 0;

                    let next_op = (self.recursive_term)(working_table)?;
                    let next_schema = next_op.schema();
                    self.stage = if next_schema == self.schema {
                        Stage::Iteration(next_op)
                    } else if schema_types_match(&next_schema, &self.schema) {
                        // Field names (and nullability/metadata) differ but
                        // the column TYPES line up positionally — legal in
                        // Postgres, which takes the CTE's exposed column
                        // names from the anchor term (or an explicit `WITH
                        // x(a, b) AS` list), never from the recursive term.
                        // A recursive term's `SELECT` list is routinely
                        // unaliased (`SELECT n+1 FROM r ...`, no `AS n`),
                        // which basin-plan names `?column?` — see
                        // `SchemaRebind`'s doc comment for the live-verified
                        // query this exact shape rejected before this fix.
                        Stage::Iteration(Box::new(SchemaRebind::new(
                            next_op,
                            Arc::clone(&self.schema),
                        )))
                    } else {
                        return Err(ExecError::TypeMismatch(format!(
                            "WITH RECURSIVE's recursive term must return the same column types \
                             as the anchor term: expected {:?}, got {:?}",
                            self.schema, next_schema
                        )));
                    };
                    // Bounded continuation: at most one more factory call
                    // and one more pull happen before this call returns —
                    // see the doc comment above for why this can never
                    // chain into an unbounded internal loop.
                }
            }
        }
    }

    fn memory_used(&self) -> usize {
        self.seen_bytes + self.working_set_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    /// The same test double every other file in this crate uses — see
    /// `cte.rs`'s copy for why it is duplicated per-module rather than
    /// shared.
    struct Feed {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Feed {
        fn boxed(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
            Box::new(Feed {
                schema,
                batches: batches.into(),
            })
        }
    }

    impl Operator for Feed {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
            Ok(self.batches.pop_front())
        }
    }

    fn schema_1i32(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]))
    }

    fn schema_2i32(a: &str, b: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(a, DataType::Int32, true),
            Field::new(b, DataType::Int32, true),
        ]))
    }

    fn batch_i32(schema: &SchemaRef, values: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn batch_2i32(schema: &SchemaRef, a: Vec<Option<i32>>, b: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
        )
        .unwrap()
    }

    fn col_i32(batch: &RecordBatch, i: usize) -> Vec<Option<i32>> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn drain_values(mut op: RecursiveCte) -> Vec<i32> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            out.extend(col_i32(&b, 0).into_iter().flatten());
        }
        out
    }

    // ── The classic shapes, verified live (see module docs) ─────────────

    // `WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE
    // n < 5) SELECT n FROM t;` — verified live against PostgreSQL 18.2 to be
    // exactly 1,2,3,4,5 (5 rows). The recursive term here is a plain
    // closure: given the previous working table, emit `n+1` for every row
    // with `n < 5`, or nothing once every row is `>= 5` — a hand-built
    // stand-in for `SELECT n+1 FROM t WHERE n < 5` with no planner involved.
    #[test]
    fn the_classic_bounded_counter_union_all() {
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(1)])]);
        let s = schema.clone();
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            let mut next: Vec<Option<i32>> = Vec::new();
            for batch in &working_table {
                for n in col_i32(batch, 0).into_iter().flatten() {
                    if n < 5 {
                        next.push(Some(n + 1));
                    }
                }
            }
            Ok(Feed::boxed(s.clone(), vec![batch_i32(&s, next)]))
        });
        let op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        let mut out = drain_values(op);
        out.sort();
        assert_eq!(out, vec![1, 2, 3, 4, 5]);
    }

    // `edges(1,2),(2,3),(3,1),(3,4)`, reachability from node 1 via `UNION` —
    // verified live to be exactly {1,2,3,4}, 4 rows, even though the graph
    // has a cycle (3 -> 1) that would re-derive node 1 forever without
    // dedup. This is the "UNION terminates on cyclic data" half of the
    // brief's required cyclic-graph test.
    #[test]
    fn cyclic_graph_reachability_terminates_via_union_dedup() {
        let edges: &[(i32, i32)] = &[(1, 2), (2, 3), (3, 1), (3, 4)];
        let out = reachability_from_1(edges, false, 100);
        let mut sorted = out.unwrap();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![1, 2, 3, 4],
            "must match the live Postgres result exactly, despite the 3->1 cycle"
        );
    }

    // Builds a `RecursiveCte` computing single-source reachability over a
    // fixed edge list, starting from node 1: `anchor = {1}`, `recursive_term
    // (working_table) = {e.dst | e in edges, e.src in working_table}` — the
    // hand-built equivalent of `edges e JOIN reach r ON e.src = r.node`.
    fn reachability_from_1(
        edges: &'static [(i32, i32)],
        all: bool,
        max_iterations: usize,
    ) -> Result<Vec<i32>, ExecError> {
        let schema = schema_1i32("node");
        let anchor = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(1)])]);
        let s = schema.clone();
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            let mut next: Vec<Option<i32>> = Vec::new();
            for batch in &working_table {
                for src in col_i32(batch, 0).into_iter().flatten() {
                    for &(e_src, e_dst) in edges {
                        if e_src == src {
                            next.push(Some(e_dst));
                        }
                    }
                }
            }
            Ok(Feed::boxed(s.clone(), vec![batch_i32(&s, next)]))
        });
        let mut op = RecursiveCte::new(anchor, recursive_term, all, 1 << 30, max_iterations);
        let mut out = Vec::new();
        loop {
            match op.next_batch()? {
                Some(b) => out.extend(col_i32(&b, 0).into_iter().flatten()),
                None => break,
            }
        }
        Ok(out)
    }

    // ── Item 1: UNION dedups across the WHOLE history, not one iteration ──

    // `edges2` adds `(1,3)` and `(3,4)` to the previous graph, so node 4 is
    // reachable via TWO different paths (2->4 and 3->4) landing in the SAME
    // iteration (both 2 and 3 are derived together, from the anchor's {1}
    // via 1->2 and 1->3) — an intra-iteration duplicate. Node 1 also
    // resurfaces later via 4->1 — an inter-iteration duplicate, since node 1
    // was already emitted by the anchor. Verified live to still be exactly
    // {1,2,3,4}, 4 rows: `WITH RECURSIVE reach(node) AS (SELECT 1 UNION
    // SELECT e.dst FROM edges2 e JOIN reach r ON e.src = r.node) SELECT node
    // FROM reach ORDER BY node;` with `edges2 = (1,2),(1,3),(2,4),(3,4),
    // (4,1)`.
    //
    // The wrong answer this prevents: a dedup that only looked within the
    // CURRENT iteration's own batch would still collapse the intra-iteration
    // duplicate (both copies of 4 arrive together), but would let node 1
    // reappear via 4->1 since the anchor's earlier `1` is in a different
    // iteration's batch — and on a graph where the cycle keeps re-deriving a
    // row every iteration (rather than converging), the working table would
    // never become empty and the query would loop forever instead of
    // terminating in 3 iterations the way live Postgres does.
    #[test]
    fn union_dedups_across_the_whole_history_not_just_one_iteration_or_a_cycle_loops_forever() {
        let edges: &[(i32, i32)] = &[(1, 2), (1, 3), (2, 4), (3, 4), (4, 1)];
        // A generous cap: the correct algorithm converges in 3 iterations
        // (derive {2,3}, then {4} deduped from two paths, then {} once 4->1
        // is dropped as already-seen). A per-iteration-only dedup would
        // still be going at iteration 100.
        let out = reachability_from_1(edges, false, 100).unwrap();
        let mut sorted = out;
        sorted.sort();
        assert_eq!(
            sorted,
            vec![1, 2, 3, 4],
            "must match live Postgres exactly: the intra-iteration duplicate 4 collapses to \
             one row, and the inter-iteration duplicate 1 (via 4->1) never reappears"
        );
    }

    // `UNION ALL` performs no deduplication anywhere: over the SAME cyclic
    // graph, node 1 (and everything reachable from it) is re-derived every
    // single iteration forever. This file cannot actually let that run
    // forever, so it demonstrates the same "never converges" shape by
    // asserting it hits the iteration cap and errors — the other half of
    // module docs item 4 and the "a cyclic graph where UNION terminates and
    // UNION ALL would not" requirement.
    #[test]
    fn union_all_performs_no_dedup_so_the_same_cyclic_graph_never_converges() {
        let edges: &[(i32, i32)] = &[(1, 2), (2, 3), (3, 1), (3, 4)];
        let err = reachability_from_1(edges, true, 20).unwrap_err();
        match err {
            ExecError::Internal(msg) => {
                assert!(
                    msg.contains("iteration cap") && msg.contains("20"),
                    "error must name the cap it hit: {msg}"
                );
            }
            other => panic!("expected ExecError::Internal naming the iteration cap, got {other:?}"),
        }
    }

    // ── Item 2: the recursive term sees only the PREVIOUS working table ──

    // The implementation was correct; the test's expected invocation count
    // was not. A missing `working_set.push` was found and fixed elsewhere
    // (it counted rows without keeping them, so the recursive term got an
    // empty table and the recursion stopped after the anchor, looking like
    // correct termination) — after that fix this test still failed, but the
    // trace it printed (`[[1],[2],[3],[4]]`) is exactly what standard
    // fixpoint evaluation produces: discovering the working table is finally
    // empty REQUIRES invoking the recursive term one more time on the last
    // non-empty working table and observing it yields nothing. Verified live
    // against PostgreSQL 18.2 with a volatile marker function substituted for
    // `r.node` in the recursive term's `WHERE` clause, over this exact
    // `edges` graph:
    // ```sql
    // WITH RECURSIVE reach(node) AS (
    //   SELECT 1
    //   UNION
    //   SELECT e.dst FROM edges e JOIN reach r ON e.src = r.node
    //                              WHERE mark(r.node) -- logs (n, order)
    // )
    // SELECT node FROM reach ORDER BY node;
    // -- log: (1,1), (2,2), (3,3), (4,4) -- four rounds, each fed exactly the
    // -- single node the PREVIOUS round emitted, node 3 included. The old
    // -- expectation `[[1],[2],[4]]` silently skipped node 3's own round.
    // ```
    #[test]
    fn recursive_term_sees_only_the_previous_iterations_working_table_not_the_accumulated_result() {
        let edges: &[(i32, i32)] = &[(1, 2), (2, 3), (3, 1), (3, 4)];
        let schema = schema_1i32("node");
        let anchor = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(1)])]);
        let s = schema.clone();
        // Log exactly which rows each recursive-term invocation was handed.
        let seen_working_tables: Rc<RefCell<Vec<Vec<i32>>>> = Rc::new(RefCell::new(Vec::new()));
        let log = Rc::clone(&seen_working_tables);
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            let mut given: Vec<i32> = working_table
                .iter()
                .flat_map(|b| col_i32(b, 0))
                .flatten()
                .collect();
            given.sort();
            log.borrow_mut().push(given.clone());

            let mut next: Vec<Option<i32>> = Vec::new();
            for src in given {
                for &(e_src, e_dst) in edges {
                    if e_src == src {
                        next.push(Some(e_dst));
                    }
                }
            }
            Ok(Feed::boxed(s.clone(), vec![batch_i32(&s, next)]))
        });
        let op = RecursiveCte::new(anchor, recursive_term, false, 1 << 30, 100);
        let mut out = drain_values(op);
        out.sort();
        assert_eq!(out, vec![1, 2, 3, 4]);

        // Iteration 1 must see ONLY {1} (the anchor's output), never {1}
        // plus anything derived later. Iteration 2 must see ONLY {2} — not
        // {1,2}. Iteration 3 must see ONLY {3} — not {1,2,3}. And iteration 4
        // must see ONLY {4} — not {1,2,3,4} — and is invoked at all only
        // because a working table (namely {4}) still needs to be tried
        // before the empty result that ends the recursion can be observed.
        // If this operator instead fed the recursive term the FULL
        // accumulated result so far, every entry below would be a growing
        // superset instead of exactly one node.
        assert_eq!(
            *seen_working_tables.borrow(),
            vec![vec![1], vec![2], vec![3], vec![4]],
            "each invocation must see ONLY the immediately preceding iteration's rows"
        );
    }

    // ── Item 3: termination is an empty working set, not a fixed count ──

    // The implementation was correct; the test's expected iteration count
    // was not. This test previously expected termination after 2 recursive-
    // term invocations, but discovering the working table is finally empty
    // REQUIRES invoking the recursive term one more time on the last
    // non-empty working table ({3}) and observing it produces nothing — that
    // is a 3rd invocation, not a stray extra one. Verified live against
    // PostgreSQL 18.2 with a volatile marker function wrapped around `n` in
    // the recursive term's `WHERE` clause:
    // ```sql
    // WITH RECURSIVE t(n) AS (
    //   SELECT 1
    //   UNION ALL
    //   SELECT n+1 FROM t WHERE mark(n) < 3 -- mark() logs (n, order)
    // )
    // SELECT n FROM t ORDER BY n;
    // -- log: (1,1), (2,2), (3,3) -- three rounds. The old expectation of 2
    // -- rounds skipped the round that runs on {3} and discovers it's < 3
    // -- is false, which is exactly what proves the working set went empty.
    // ```
    #[test]
    fn termination_is_an_empty_working_set_not_a_fixed_iteration_count() {
        // A recursive term that converges after exactly 3 invocations:
        // n=1 -> n=2 (invocation 1), n=2 -> n=3 (invocation 2), then
        // invocation 3 is handed {3}, finds `3 < 3` false, and produces
        // nothing — that 3rd invocation is what discovers the working table
        // is finally empty. The cap is set far higher (100) to prove
        // termination happens because the working set went empty, not
        // because a cap was hit.
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(1)])]);
        let s = schema.clone();
        let iterations_run = Rc::new(RefCell::new(0usize));
        let counter = Rc::clone(&iterations_run);
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            *counter.borrow_mut() += 1;
            let mut next: Vec<Option<i32>> = Vec::new();
            for batch in &working_table {
                for n in col_i32(batch, 0).into_iter().flatten() {
                    if n < 3 {
                        next.push(Some(n + 1));
                    }
                }
            }
            Ok(Feed::boxed(s.clone(), vec![batch_i32(&s, next)]))
        });
        let op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        let mut out = drain_values(op);
        out.sort();
        assert_eq!(out, vec![1, 2, 3]);
        assert_eq!(
            *iterations_run.borrow(),
            3,
            "must stop the moment the working set is empty (after deriving 2, then 3, then \
             invoking once more on {{3}} to discover it produces nothing), not continue \
             toward the 100-iteration cap"
        );
    }

    // ── Item 4: the iteration cap fails loudly, naming itself ──

    #[test]
    fn a_non_terminating_recursive_term_hits_the_iteration_cap_and_errors_loudly() {
        // `UNION ALL` (no dedup) with a recursive term that always produces
        // exactly one row, forever: genuinely non-terminating.
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(1)])]);
        let s = schema.clone();
        let recursive_term: RecursiveTermFactory = Box::new(move |_working_table| {
            Ok(Feed::boxed(s.clone(), vec![batch_i32(&s, vec![Some(1)])]))
        });
        let op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 7);
        let mut op = op;
        let err = loop {
            match op.next_batch() {
                Ok(Some(_)) => continue,
                Ok(None) => {
                    panic!("a genuinely non-terminating recursive term must never reach None")
                }
                Err(e) => break e,
            }
        };
        match err {
            ExecError::Internal(msg) => {
                assert!(
                    msg.contains("iteration cap") && msg.contains('7'),
                    "the error must name the cap it hit (7): {msg}"
                );
                assert!(
                    msg.to_lowercase().contains("statement_timeout"),
                    "must document why Postgres needs no such cap (statement_timeout instead): {msg}"
                );
            }
            other => panic!("expected ExecError::Internal, got {other:?}"),
        }
    }

    // ── Item 5: an empty anchor short-circuits entirely ──

    #[test]
    fn empty_anchor_yields_zero_rows_and_never_invokes_the_recursive_term() {
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(schema.clone(), vec![]);
        let invocations = Rc::new(RefCell::new(0usize));
        let counter = Rc::clone(&invocations);
        let recursive_term: RecursiveTermFactory = Box::new(move |_working_table| {
            *counter.borrow_mut() += 1;
            Ok(Feed::boxed(schema_1i32("n"), vec![]))
        });
        let mut op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        assert!(op.next_batch().unwrap().is_none());
        assert_eq!(
            *invocations.borrow(),
            0,
            "the recursive term must never be invoked when the anchor is empty"
        );
    }

    // An anchor that yields an explicit zero-row batch (as opposed to no
    // batches at all) must behave identically — the "empty" check is on row
    // count, not on whether the anchor operator returned `Some` at least
    // once.
    #[test]
    fn anchor_yielding_only_an_explicit_zero_row_batch_also_short_circuits() {
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![])]);
        let invocations = Rc::new(RefCell::new(0usize));
        let counter = Rc::clone(&invocations);
        let recursive_term: RecursiveTermFactory = Box::new(move |_working_table| {
            *counter.borrow_mut() += 1;
            Ok(Feed::boxed(schema_1i32("n"), vec![]))
        });
        let mut op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        // The zero-row batch itself is still a legitimate `Some` (mirrors
        // `Distinct`'s "empty batch, not end of stream" contract) before the
        // anchor operator is exhausted.
        assert_eq!(op.next_batch().unwrap().unwrap().num_rows(), 0);
        assert!(op.next_batch().unwrap().is_none());
        assert_eq!(*invocations.borrow(), 0);
    }

    // ── Multi-column working sets ──

    // `WITH RECURSIVE t(n, m) AS (SELECT 1, 100 UNION ALL SELECT n+1, m+10
    // FROM t WHERE n < 4) SELECT n, m FROM t;` — verified live to be
    // (1,100),(2,110),(3,120),(4,130). Proves the working table (and the
    // dedup key, in the UNION case exercised by the next test) spans every
    // column jointly, not just the first.
    #[test]
    fn multi_column_working_set_carries_every_column_through_each_iteration() {
        let schema = schema_2i32("n", "m");
        let anchor = Feed::boxed(
            schema.clone(),
            vec![batch_2i32(&schema, vec![Some(1)], vec![Some(100)])],
        );
        let s = schema.clone();
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            let mut ns = Vec::new();
            let mut ms = Vec::new();
            for batch in &working_table {
                let n_col = col_i32(batch, 0);
                let m_col = col_i32(batch, 1);
                for (n, m) in n_col.into_iter().zip(m_col) {
                    if let (Some(n), Some(m)) = (n, m) {
                        if n < 4 {
                            ns.push(Some(n + 1));
                            ms.push(Some(m + 10));
                        }
                    }
                }
            }
            Ok(Feed::boxed(s.clone(), vec![batch_2i32(&s, ns, ms)]))
        });
        let mut op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        let mut rows: Vec<(i32, i32)> = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            let n = col_i32(&b, 0);
            let m = col_i32(&b, 1);
            rows.extend(n.into_iter().zip(m).map(|(a, b)| (a.unwrap(), b.unwrap())));
        }
        rows.sort();
        assert_eq!(rows, vec![(1, 100), (2, 110), (3, 120), (4, 130)]);
    }

    // Two rows sharing the first column's value but differing in the second
    // must NOT be treated as the same key for `UNION` dedup — the wrong
    // answer this prevents is keying only on column 0 and incorrectly
    // dropping (1,2) as "already seen (1,1)".
    #[test]
    fn union_dedup_keys_on_every_column_jointly_not_just_the_first() {
        let schema = schema_2i32("a", "b");
        // Anchor emits (1,1) and (1,2) — same `a`, different `b`.
        let anchor = Feed::boxed(
            schema.clone(),
            vec![batch_2i32(
                &schema,
                vec![Some(1), Some(1)],
                vec![Some(1), Some(2)],
            )],
        );
        let s = schema.clone();
        // Recursive term never produces anything — this test only cares
        // about the anchor's own dedup pass.
        let recursive_term: RecursiveTermFactory =
            Box::new(move |_working_table| Ok(Feed::boxed(s.clone(), vec![])));
        let mut op = RecursiveCte::new(anchor, recursive_term, false, 1 << 30, 100);
        let mut rows: Vec<(i32, i32)> = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            let a = col_i32(&b, 0);
            let b_col = col_i32(&b, 1);
            rows.extend(
                a.into_iter()
                    .zip(b_col)
                    .map(|(x, y)| (x.unwrap(), y.unwrap())),
            );
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, 1), (1, 2)],
            "(1,1) and (1,2) differ in column b and must both survive, not collapse to one row"
        );
    }

    // A genuine duplicate — the SAME pair repeated — must still collapse to
    // one row under `UNION`, proving the multi-column key really is doing
    // deduplication, not just "always keep everything".
    #[test]
    fn union_dedup_still_collapses_a_genuine_multi_column_duplicate() {
        let schema = schema_2i32("a", "b");
        let anchor = Feed::boxed(
            schema.clone(),
            vec![batch_2i32(
                &schema,
                vec![Some(1), Some(1)],
                vec![Some(1), Some(1)],
            )],
        );
        let s = schema.clone();
        let recursive_term: RecursiveTermFactory =
            Box::new(move |_working_table| Ok(Feed::boxed(s.clone(), vec![])));
        let mut op = RecursiveCte::new(anchor, recursive_term, false, 1 << 30, 100);
        let mut rows = 0;
        while let Some(b) = op.next_batch().unwrap() {
            rows += b.num_rows();
        }
        assert_eq!(rows, 1, "the two identical (1,1) rows must collapse to one");
    }

    // ── The memory contract ──

    #[test]
    fn memory_used_is_zero_before_the_first_pull() {
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(schema.clone(), vec![]);
        let recursive_term: RecursiveTermFactory =
            Box::new(move |_working_table| Ok(Feed::boxed(schema_1i32("n"), vec![])));
        let op = RecursiveCte::new(anchor, recursive_term, false, 1 << 30, 100);
        assert_eq!(op.memory_used(), 0);
    }

    // `memory_used` must grow as `UNION`'s `seen` set grows (mirrors
    // `Distinct::memory_used`'s test of the same name/shape in `cte.rs`).
    #[test]
    fn memory_used_grows_with_the_union_seen_set() {
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(
            schema.clone(),
            vec![batch_i32(&schema, vec![Some(1), Some(2)])],
        );
        let s = schema.clone();
        let recursive_term: RecursiveTermFactory =
            Box::new(move |_working_table| Ok(Feed::boxed(s.clone(), vec![])));
        let mut op = RecursiveCte::new(anchor, recursive_term, false, 1 << 30, 100);
        op.next_batch().unwrap();
        assert!(op.memory_used() > 0, "two distinct keys were seen");
    }

    // The headline memory-contract claim: a budget that comfortably fits ONE
    // iteration's working table, but would be blown through many times over
    // by the FULL accumulated history of a many-iteration `UNION ALL` query,
    // still succeeds — proving this operator really does drop each
    // iteration's rows once handed to the next one, rather than retaining
    // every iteration's batches for the life of the query. `UNION ALL` is
    // used deliberately (no `seen` growth at all) so the ONLY thing that
    // could blow the budget is an implementation mistake that keeps
    // accumulating `working_set` across iterations instead of resetting it.
    #[test]
    fn memory_used_reflects_only_the_current_working_table_not_the_accumulated_history() {
        let schema = schema_1i32("n");
        let one_row_batch = batch_i32(&schema, vec![Some(1)]);
        let one_row_bytes = one_row_batch.get_array_memory_size();
        let anchor = Feed::boxed(schema.clone(), vec![one_row_batch]);
        let s = schema.clone();
        // 40 iterations, each producing exactly one row (as long as
        // fewer than 40 have run) — chosen so that 40x one row's bytes
        // would exceed a budget sized for only ~3x one row's bytes, if
        // history were ever allowed to accumulate.
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            let count: i32 = working_table
                .iter()
                .flat_map(|b| col_i32(b, 0))
                .flatten()
                .count() as i32;
            let _ = count;
            static ITER: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);
            let i = ITER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let next = if i < 39 { vec![Some(1)] } else { vec![] };
            Ok(Feed::boxed(s.clone(), vec![batch_i32(&s, next)]))
        });
        // Budget: enough for a handful of one-row batches at once (this
        // operator briefly holds the just-produced batch plus its running
        // working-set accumulation within one phase), but nowhere near 40x.
        let budget = one_row_bytes * 5;
        let mut op = RecursiveCte::new(anchor, recursive_term, true, budget, 100);
        let mut total_rows = 0;
        loop {
            match op.next_batch() {
                Ok(Some(b)) => total_rows += b.num_rows(),
                Ok(None) => break,
                Err(e) => panic!(
                    "must not run out of memory: history must not accumulate across \
                     iterations, got {e:?}"
                ),
            }
        }
        assert_eq!(
            total_rows, 40,
            "anchor's 1 row + 39 more from the recursive term"
        );
    }

    #[test]
    fn exceeding_the_budget_is_out_of_memory_not_a_hang() {
        let schema = schema_1i32("n");
        let big = batch_i32(&schema, (0..1000).map(Some).collect());
        let size = big.get_array_memory_size();
        let anchor = Feed::boxed(schema.clone(), vec![big]);
        let s = schema.clone();
        let recursive_term: RecursiveTermFactory =
            Box::new(move |_working_table| Ok(Feed::boxed(s.clone(), vec![])));
        let mut op = RecursiveCte::new(anchor, recursive_term, false, size - 1, 100);
        let err = op.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::OutOfMemory { .. }), "{err:?}");
    }

    // ── Schema / miscellaneous ──

    #[test]
    fn schema_matches_the_anchors_schema() {
        let schema = schema_1i32("value");
        let anchor = Feed::boxed(schema.clone(), vec![]);
        let recursive_term: RecursiveTermFactory =
            Box::new(move |_working_table| Ok(Feed::boxed(schema_1i32("value"), vec![])));
        let op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        assert_eq!(op.schema(), schema);
    }

    // ── Item 7: a NOT NULL anchor does not make the CTE NOT NULL ──

    // The regression test for the whole class: a `NOT NULL` anchor and a
    // recursive term that actually produces a NULL. Without
    // `recursive_cte_output_schema` this fails with
    //   Internal("Invalid argument error: Column 'n' is declared as
    //            non-nullable but contains null values")
    // from `SchemaRebind`'s `RecordBatch::try_new` — the same rejection, in
    // the same place, that 02251a4c fixed for `SetOp`.
    //
    // Live PostgreSQL 18.2:
    //   WITH RECURSIVE r(n) AS (
    //     SELECT 1 UNION ALL SELECT NULL::int FROM r WHERE n IS NOT NULL)
    //   SELECT n FROM r;
    //    n
    //   ---
    //    1
    //         <- NULL
    //   (2 rows)
    #[test]
    fn a_null_from_the_recursive_term_under_a_not_null_anchor() {
        let notnull = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let nullable = schema_1i32("n");
        let anchor = Feed::boxed(notnull.clone(), vec![batch_i32(&notnull, vec![Some(1)])]);
        let s = nullable.clone();
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            // `SELECT NULL::int FROM r WHERE n IS NOT NULL` — one NULL per
            // non-NULL input row, so the second iteration produces nothing
            // and the fixpoint is reached.
            let mut next: Vec<Option<i32>> = Vec::new();
            for batch in &working_table {
                for v in col_i32(batch, 0) {
                    if v.is_some() {
                        next.push(None);
                    }
                }
            }
            Ok(Feed::boxed(s.clone(), vec![batch_i32(&s, next)]))
        });
        let mut op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        assert_eq!(
            op.schema(),
            nullable,
            "a UNION's column is nullable if EITHER branch's is, and the recursive term's \
             half is not knowable here — see recursive_cte_output_schema"
        );
        let declared = op.schema();
        let mut out: Vec<Option<i32>> = Vec::new();
        loop {
            match op.next_batch() {
                Ok(Some(b)) => {
                    // Including the ANCHOR's own batch, which arrives under
                    // the NOT NULL schema and is rebound like any iteration's.
                    assert_eq!(
                        b.schema(),
                        declared,
                        "every yielded batch carries exactly the reported schema"
                    );
                    out.extend(col_i32(&b, 0));
                }
                Ok(None) => break,
                Err(e) => panic!("live Postgres returns 1 and NULL here, got {e:?}"),
            }
        }
        assert_eq!(out, vec![Some(1), None]);
    }

    #[test]
    fn a_recursive_term_returning_a_mismatched_schema_is_a_type_mismatch() {
        let schema = schema_1i32("n");
        let anchor = Feed::boxed(schema.clone(), vec![batch_i32(&schema, vec![Some(1)])]);
        let recursive_term: RecursiveTermFactory =
            Box::new(move |_working_table| Ok(Feed::boxed(schema_2i32("n", "extra"), vec![])));
        let mut op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        op.next_batch().unwrap(); // the anchor's own row, fine
        let err = op.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::TypeMismatch(_)), "{err:?}");
    }

    // `WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE
    // n < 5) SELECT n FROM r;` — the recursive term's own output column has
    // NO alias (`n+1`, not `n+1 AS n`), which basin-plan (like Postgres
    // itself) names `?column?`, not `n`. Verified live against PostgreSQL
    // 18.2 to still succeed, 1..=5 (5 rows): Postgres takes the CTE's
    // exposed column names from the ANCHOR, never from the recursive term.
    //
    // Before `SchemaRebind` existed, the plain `Schema` `!=` check in
    // `next_batch` treated this as `ExecError::TypeMismatch` — which
    // `basin-engine`'s owned-engine bridge turns into a silent fallback to
    // DataFusion — even though the column TYPES agree and only the NAME
    // differs. This is exactly the shape that made
    // `WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE
    // n < 5) SELECT n FROM r` fall back before this fix (confirmed via
    // `RUST_LOG=basin_engine::owned_engine=debug` against a live `Engine`:
    // `reason=execution failed: type mismatch: ... expected Schema { ...,
    // name: "n", ... }, got Schema { ..., name: "?column?", ... }`).
    #[test]
    fn a_recursive_terms_unaliased_column_need_not_match_the_anchors_name() {
        let anchor_schema = schema_1i32("n");
        let anchor = Feed::boxed(
            anchor_schema.clone(),
            vec![batch_i32(&anchor_schema, vec![Some(1)])],
        );
        // The recursive term's own schema names its column `?column?`
        // (basin-plan's synthetic name for an unaliased expression, the
        // same convention Postgres uses) — deliberately NOT `n`.
        let recursive_schema = schema_1i32("?column?");
        let recursive_term: RecursiveTermFactory = Box::new(move |working_table| {
            let mut next: Vec<Option<i32>> = Vec::new();
            for batch in &working_table {
                for n in col_i32(batch, 0).into_iter().flatten() {
                    if n < 5 {
                        next.push(Some(n + 1));
                    }
                }
            }
            Ok(Feed::boxed(
                recursive_schema.clone(),
                vec![batch_i32(&recursive_schema, next)],
            ))
        });
        let mut op = RecursiveCte::new(anchor, recursive_term, true, 1 << 30, 100);
        let mut out = Vec::new();
        loop {
            match op.next_batch().unwrap() {
                Some(b) => {
                    // The Operator contract (`operator.rs`: "the Arrow
                    // schema of the batches this operator yields") must
                    // hold for EVERY batch, anchor or iteration — not just
                    // ones that happened to already agree with the anchor's
                    // naming.
                    assert_eq!(
                        b.schema(),
                        anchor_schema,
                        "every yielded batch must carry the CTE's own (anchor) schema, \
                         never the recursive term's own field name"
                    );
                    out.extend(col_i32(&b, 0).into_iter().flatten());
                }
                None => break,
            }
        }
        out.sort();
        assert_eq!(
            out,
            vec![1, 2, 3, 4, 5],
            "must match live Postgres exactly, despite the recursive term's unaliased column"
        );
        assert_eq!(
            op.schema(),
            anchor_schema,
            "the operator's advertised schema is always the anchor's, unaffected by any \
             iteration's own (possibly differently-named) recursive term"
        );
    }
}
