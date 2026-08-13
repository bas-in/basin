//! Bridges a real client `SELECT` into Basin's owned query pipeline —
//! `basin-plan` (lowering + IR + optimizer) and `basin-exec` (operators,
//! reading real Vortex/Parquet files) — behind the `BASIN_OWNED_ENGINE` env
//! flag (default OFF, matching `docs/decisions/0014-pg-query-as-canonical-parser.md`'s
//! `BASIN_PG_QUERY` staged-migration pattern exactly: a new pipeline sits
//! beside the incumbent one, gated by an env var that defaults to "keep
//! today's behaviour", flipped on deliberately, and removed once the ratio
//! this module tracks reaches zero).
//!
//! Before this module, `basin-plan` and `basin-exec` were a complete,
//! independently-tested pipeline that no client query had ever reached —
//! every SELECT still went to DataFusion via [`crate::executor::execute`].
//! This module is the first real caller: [`try_execute`] is invoked from
//! `execute()` for a single-statement `SELECT`, and either returns the owned
//! engine's answer or reports that it could not, in which case the caller
//! falls through to the unchanged DataFusion path exactly as if this module
//! did not exist.
//!
//! # Why "any error" is not the only fallback trigger
//!
//! The task this module exists for is explicit that ANY error from lowering
//! ([`basin_plan::lower::LowerError`]), building ([`basin_exec::build::BuildError`])
//! or executing ([`basin_exec::operator::ExecError`]) must fall back rather
//! than surface to the client — the owned engine covers a fraction of SQL,
//! and a hard failure there would make turning this flag on strictly worse
//! than leaving it off.
//!
//! That is necessary but not sufficient. [`basin_exec::storage_source::StorageTableResolver`]
//! reads a table's *committed cold files* directly — it has no notion of the
//! DataFusion-side machinery this crate has grown around a scan: RLS
//! predicate injection ([`crate::rls`]), the hot-tier / tombstone overlay
//! ([`basin_hottier::MemTableRegistry`], consulted by
//! `tombstone_cold_scan.rs` and friends), view expansion
//! ([`crate::view_ddl`]), and promoted-JSONB shadow columns
//! ([`crate::promoted_columns`]). None of those produce an `Err` if skipped
//! — they would silently produce the *wrong rows*, which "fall back on
//! error" cannot catch because there is no error. So [`build_resolver`]
//! checks each referenced table against exactly those conditions before
//! lowering ever runs, and reports [`Fallback::Ineligible`] (not a real
//! error, just as unactionable to the caller) when one applies. See that
//! function's body for the precise list.
//!
//! The same class of silent-wrong-rows failure applies to WHICH FILES the
//! scan reads, and that one is not a check but a choice made in
//! [`build_resolver`]: the file set comes from the catalog's
//! `live_data_files()`, never from an object-store LIST. A LIST answers "what
//! physically exists", and superseded files are deliberately retained for
//! `BASIN_SUPERSEDED_DELETE_GRACE_SECS` (300 s) after every compaction, so a
//! LIST-sourced scan returns the pre- and post-UPDATE image of the same row
//! side by side and inflates every aggregate over it. The DataFusion path has
//! sourced its `ListingTable` from `live_data_files()` since bug #41; this
//! path now matches it exactly. See `build_resolver`'s comment at the
//! `live_files` binding, and `tests/owned_scan_liveness.rs`.
//!
//! What is intentionally *not* checked, because it costs performance and not
//! correctness — the owned path returns right answers, just without the
//! index-assisted pruning the DataFusion path has accumulated (secondary
//! B-tree, GIN, R-tree, trigram): those are consulted only to prune files
//! before decode, never to change which rows a query returns.
//!
//! # Reporting why, not just how many
//!
//! [`crate::Engine::owned_engine_served_count`]/[`crate::Engine::owned_engine_fallback_count`]
//! say *how far* this migration has gotten; they cannot say what to build
//! next. [`Fallback::reason_kind`] buckets every decline into one of
//! [`FallbackReasonKind`]'s five categories — an ineligible table, a
//! construct neither lowering nor building implements yet, some other
//! lowering failure, some other build failure, or a runtime execution error
//! — and [`crate::Engine::owned_engine_fallback_reason_counts`] exposes the tally.
//! `Unsupported` is the one to watch: it is exactly "resolved fine, but
//! nothing downstream builds it yet", which is a to-do list, not noise.
//!
//! # Resolving against the real catalogs, safely
//!
//! [`RealOperators`] and [`RealFunctions`] resolve against
//! `basin_pgtype::operator::resolve` and `basin_pgtype::func::resolve` — the
//! real `pg_operator`/`pg_proc` tables — rather than a hand-picked handful of
//! names, so a construct is only ever unreachable because lowering/building
//! don't implement it yet, not because this bridge under-resolved it first.
//! This is safe to do broadly, not just for the names `basin-exec::eval`
//! happens to implement: every one of that crate's scalar-function and
//! aggregate paths downcasts the real Arrow array it receives to a concrete
//! type before touching it, so resolving to an oid whose declared argument
//! type doesn't match what a column actually holds fails loudly
//! (`ExecError::TypeMismatch`/`Internal`) rather than computing a wrong
//! value — the same "any error still falls back" guarantee the rest of this
//! module leans on. `count`/`sum`/`avg`/`min`/`max` are the one deliberate
//! exception: see [`RealFunctions`] for why they stay pinned to a single
//! representative oid per name instead of the argument-typed one
//! `basin_pgtype::func::resolve` would pick.
//!
//! # Table resolution: real catalog, minted `TableId`s, no async in `lower_select`
//!
//! [`basin_plan::lower::select::TableResolver::resolve_table`] and
//! [`basin_exec::build::TableResolver::open`] are both synchronous, but
//! resolving a table name needs an async catalog round-trip
//! (`basin_catalog::Catalog::load_table`). [`build_resolver`] resolves this
//! by walking the parse tree *before* lowering starts — mirroring exactly
//! the `FROM`-clause shapes `basin-plan/src/lower/select.rs`'s own
//! `build_from_clause`/`build_from_item`/`build_join_expr` already handle
//! (a plain table, a comma list, `JOIN ... ON`) plus `UNION`/`INTERSECT`/
//! `EXCEPT` arms — and prefetching each referenced table's metadata with the
//! ordinary `await`ed catalog call. [`CatalogTableResolver`] is then a pure
//! in-memory lookup for both traits, no blocking-on-async trick required.
//!
//! Subqueries embedded in expressions (`SubLink` — `EXISTS`, `NOT EXISTS`,
//! `IN`, `NOT IN`, `ANY`/`ALL`, and a scalar `(SELECT ...)`) are walked too,
//! by [`collect_expr`], wherever an expression can appear at a statement
//! level: the target list, `WHERE`, `HAVING`, `GROUP BY`, `ORDER BY`, and a
//! `JOIN ... ON` qual. That used to be skipped, on the grounds that the cost
//! of under-prefetching is only a safe fallback (`resolve_table` returns
//! `None` for an unprefetched name, `lower_select` reports
//! `LowerError::UnknownName`) and never a wrong answer. True, but the cost
//! was being paid constantly: `basin-plan` lowers correlated `SubLink`s
//! (`SelectSubqueries`) and `opt::decorrelate` rewrites them, so every
//! `WHERE ... IN (SELECT ... FROM other_table)` was dying in this bridge,
//! several steps before the machinery built to serve it. Only a subquery
//! naming a table the enclosing statement does not otherwise mention was
//! ever affected — which is why the aliased `EXISTS (SELECT 1 FROM t u
//! WHERE u.id = t.id)` shape served all along.
//!
//! Two things that walk is careful about. The enclosing statement's CTE
//! scope is threaded into the subquery unchanged, so `WHERE id IN (SELECT
//! ... FROM cte)` still excludes `cte` instead of asking the catalog for a
//! table by that name — which would turn a servable query into an
//! `Ineligible` fallback, strictly worse than not walking at all. A subquery
//! in `FROM` (`RangeSubselect`, LATERAL included) IS walked, and the same CTE
//! scope is threaded into it. That reverses an earlier decision, correctly:
//! while `lower/select.rs` refused a subquery in `FROM` as `Unsupported`,
//! collecting its tables could only downgrade a clean verdict — but it now
//! lowers derived tables through the full `SELECT` surface, so leaving them
//! unwalked strands the inner `FROM` at `UnknownName` instead.
//!
//! `basin_plan::TableId` has no catalog-side counterpart (see
//! `basin-catalog`'s `TableMetadata`, keyed only by `(ProjectId,
//! TableName)`) — [`build_resolver`] mints one per referenced table, scoped
//! to the single resolver instance built for one statement.
//!
//! # Shadow-compare: a free differential oracle, behind `BASIN_OWNED_ENGINE_SHADOW_COMPARE`
//!
//! Every time [`try_execute`] serves a `SELECT`, both engines are already
//! reachable in the same process against the same committed data — the
//! owned engine just answered, and [`crate::executor::exec_select`] is the
//! unchanged DataFusion path this bridge falls back to for everything else.
//! Behind a second, independent flag ([`shadow_compare_enabled`]), served
//! queries are ALSO run through that path — via
//! [`crate::executor::exec_select_reference`], which applies the same
//! string-rewrite pipeline the normal statement path applies before it
//! reaches `exec_select`, so the oracle is comparing like with like — and
//! the two results diffed by
//! [`shadow_compare`] — turning every test in this crate that exercises real
//! SQL into a differential-equivalence check against the incumbent, with no
//! second oracle to install and no fixture to maintain. Four design
//! decisions this mode had to make, in the order the task asked for them:
//!
//! 1. **Row order.** A statement with no top-level `ORDER BY` has no
//!    guaranteed row order (Postgres itself makes no promise, and Basin
//!    inherits that), so two independent physical engines are not going to
//!    coincidentally agree on one. [`compare_results`] checks the parsed
//!    `SelectStmt::sort_clause` and compares POSITIONALLY only when it is
//!    non-empty; otherwise it sorts both row lists by an identical canonical
//!    key ([`row_sort_key`]) and compares the sorted lists position for
//!    position. A naive unconditional positional diff would report a false
//!    divergence on nearly every `GROUP BY`/join/no-`ORDER BY` query in the
//!    corpus, drowning the real findings in noise.
//! 2. **Floating point.** Cell comparison ([`cell_eq`]/[`float_eq`]) uses a
//!    tolerance — `1e-9` absolute or `1e-9` relative to the larger operand,
//!    whichever is looser — not exact bit equality. Two engines that sum,
//!    average, or otherwise accumulate the same `FLOAT4`/`FLOAT8` values in
//!    a different order (different join order, different hash-group
//!    iteration order, ...) will legitimately land on different but equally
//!    correct IEEE-754 results at the ULP level; exact equality would flag
//!    that as a divergence on nearly every aggregate query, which is not
//!    what this mode exists to find. `NUMERIC`/`DECIMAL` and every other
//!    type are compared EXACTLY (via their rendered text — see
//!    [`display_cell`]): decimal arithmetic has no equivalent
//!    summation-order slack, so exactness there is a real signal, not noise.
//! 3. **Side effects — safe by construction, not by convention.** DML must
//!    never run twice (a second `exec_select`-equivalent for an INSERT would
//!    double-write). [`shadow_compare`] enforces this structurally, as its
//!    very first act and before anything is executed: [`shadow_target`]
//!    `match`es `stmt_node` against `NodeEnum::SelectStmt` and yields
//!    `None` for anything else, on which `shadow_compare` returns
//!    immediately. (That guard is a named function purely so it can be
//!    unit-tested directly against a parsed `INSERT`/`UPDATE`/`DELETE`
//!    node — see this file's tests.) This does not lean on today's single
//!    call site only ever reaching [`try_execute`] for `StmtKind::Select`
//!    (see `executor.rs`) — `try_execute_inner` already lowers `INSERT`/
//!    `UPDATE`/`DELETE` via `basin_plan::lower::dml::lower_dml` for exactly
//!    this reason, so a future call site wiring DML through this same bridge
//!    must not silently start double-executing writes just because this flag
//!    happens to be on. The guard is the enforcement; nothing upstream has
//!    to remember to keep it safe.
//!
//!    Matching the outermost node kind is NOT on its own enough, which is
//!    worth stating because the obvious one-line version of this guard is
//!    unsafe: `WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT *
//!    FROM x` is a perfectly ordinary data-modifying CTE, and `pg_query`
//!    roots it at a `SelectStmt` whose `with_clause` holds the `InsertStmt`.
//!    [`shadow_target`] therefore also requires [`is_side_effect_free`],
//!    which walks the `WITH` list (and each set-operation arm's own) and
//!    insists every CTE body is itself a side-effect-free `SelectStmt`.
//! 4. **Cost.** This mode runs every served query twice end to end. That is
//!    the whole point (this is a diagnostic/measurement mode, not a
//!    performance-sensitive one — see `tests/shadow_compare.rs` and
//!    `tests/fallback_histogram.rs`, which it is modelled on) and it is
//!    exactly why the flag defaults OFF and is independent of
//!    `BASIN_OWNED_ENGINE` itself.
//!
//! One case the four decisions above do not cover, because it is not a
//! difference between two answers: the incumbent path *erroring* on a
//! statement the owned engine served. That is recorded as a divergence too
//! — the engines disagreed about whether the statement is even executable,
//! which is exactly the kind of thing this mode exists to surface.
//!
//! Divergences are counted and logged (capped at
//! [`MAX_RECORDED_DIVERGENCES`] entries — a diagnostic aid, not an
//! unbounded audit log) behind [`crate::Engine::owned_engine_shadow_compare_count`],
//! [`crate::Engine::owned_engine_shadow_compare_divergence_count`], and
//! [`crate::Engine::owned_engine_shadow_compare_divergences`] — mirroring
//! the shape [`crate::Engine::owned_engine_served_count`] /
//! [`crate::Engine::owned_engine_fallback_reason_counts`] already
//! established for the fallback ratio.

use std::collections::{HashMap, HashSet};

use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field};
use pg_query::protobuf::{node::Node as NodeEnum, Node, SelectStmt, SetOperation, WithClause};

use basin_common::{ProjectId, TableName};
use basin_exec::build::{BuildError, ScanPushdown, TableResolver as ExecTableResolver};
use basin_exec::operator::ExecError;
use basin_exec::scan::BatchSource;
use basin_exec::storage_source::StorageTableResolver;
use basin_pgtype::{Oid, PgType};
use basin_plan::lower::expr::{FuncKind, OperatorResolver};
use basin_plan::lower::select::{lower_select, TableResolver as PlanTableResolver};
use basin_plan::lower::LowerError;
use basin_plan::{Expr as PlanExpr, FuncId, OpId, Schema as PlanSchema, TableId};

use crate::{ExecResult, ProjectSession};

/// Whether the owned-engine bridge is enabled. Reads the env var on every
/// call — the same convention every other `BASIN_*` runtime flag in this
/// crate follows (e.g. `dml_mutate`'s `BASIN_HOTTIER_FASTPATH_DISABLE`);
/// absent, empty, or anything but exactly `"1"` means OFF, so today's
/// behaviour is unchanged byte for byte with the flag unset.
pub(crate) fn enabled() -> bool {
    std::env::var("BASIN_OWNED_ENGINE").as_deref() == Ok("1")
}

/// Whether shadow-compare is enabled — see the module docs' "Shadow-compare"
/// section. Same convention as [`enabled`]: absent, empty, or anything but
/// exactly `"1"` means OFF. Independent of `BASIN_OWNED_ENGINE` itself as an
/// env var (a caller could in principle set this without that one), but
/// inert unless it is also `"1"`: [`try_execute`] returns before ever
/// checking this when the owned engine itself is off, so there is nothing to
/// shadow-compare against.
pub(crate) fn shadow_compare_enabled() -> bool {
    std::env::var("BASIN_OWNED_ENGINE_SHADOW_COMPARE").as_deref() == Ok("1")
}

/// Attempt to serve `stmt_node` (a single `SELECT` statement's already-parsed
/// `pg_query` node, as classified by the caller) through the owned pipeline.
///
/// `Some(result)` only on genuine success; `None` in every other case
/// (disabled, ineligible, or any lowering/build/exec failure) — the caller
/// treats `None` identically to "this module was never called" and falls
/// through to the existing DataFusion path unchanged. Every `None` is logged
/// at debug with its reason so the served-vs-fallback ratio (the counters
/// this bumps) can be explained, not just observed.
///
/// `sql` is the original statement text, needed only for the shadow-compare
/// mode (see the module docs) to re-run the same statement through
/// [`crate::executor::exec_select`] — it is otherwise unused, since every
/// other part of this bridge works from the already-parsed `stmt_node`.
pub(crate) async fn try_execute(
    sess: &ProjectSession,
    stmt_node: &Node,
    sql: &str,
) -> Option<ExecResult> {
    if !enabled() {
        return None;
    }

    // A transaction may hold this session's own uncommitted writes; the
    // owned path's `StorageTableResolver` only ever sees committed cold
    // files, so it cannot see them. Declining here (rather than relying on
    // the per-table hot-tier check in `build_resolver`) is what keeps this
    // correct for a table the *current* transaction wrote to but that has no
    // hot-tier footprint yet (e.g. a fresh `CREATE TABLE` + `INSERT` still
    // inside the same `BEGIN`).
    if crate::session::tx_is_active(&sess.state) {
        record_fallback(
            sess,
            &Fallback::Ineligible("inside an explicit transaction"),
        );
        return None;
    }

    match try_execute_inner(sess, stmt_node).await {
        Ok(result) => {
            sess.engine.note_owned_engine_served();
            tracing::debug!(target: "basin_engine::owned_engine", "owned engine served a SELECT");
            if shadow_compare_enabled() {
                shadow_compare(sess, stmt_node, sql, &result).await;
            }
            Some(result)
        }
        Err(reason) => {
            record_fallback(sess, &reason);
            None
        }
    }
}

/// Bump both the flat fallback counter and `reason`'s bucket in the
/// per-reason histogram, and log the reason at debug — the single place
/// every non-served path (the transaction guard included) funnels through,
/// so the two counters can never drift apart and every decline is logged
/// exactly once. See the module docs' "Reporting why, not just how many".
fn record_fallback(sess: &ProjectSession, reason: &Fallback) {
    sess.engine.note_owned_engine_fallback();
    sess.engine
        .note_owned_engine_fallback_reason(reason.reason_kind());
    tracing::debug!(
        target: "basin_engine::owned_engine",
        reason = %reason,
        category = ?reason.reason_kind(),
        "owned engine fell back to DataFusion"
    );
}

/// Why the owned path did not serve the statement. Distinct from a single
/// error type so the debug log (and a future metrics label) can say exactly
/// which stage declined, rather than collapsing everything into one string.
#[derive(Debug)]
enum Fallback {
    /// A precondition this module itself checks failed — not a
    /// `LowerError`/`BuildError`/`ExecError`, because nothing in
    /// `basin-plan`/`basin-exec` was even asked to run. See the module docs'
    /// "any error is not the only fallback trigger" section.
    Ineligible(&'static str),
    Lower(LowerError),
    Build(BuildError),
    Exec(ExecError),
}

impl std::fmt::Display for Fallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fallback::Ineligible(why) => write!(f, "ineligible: {why}"),
            Fallback::Lower(e) => write!(f, "lowering failed: {e:?}"),
            Fallback::Build(e) => write!(f, "build failed: {e}"),
            Fallback::Exec(e) => write!(f, "execution failed: {e}"),
        }
    }
}

impl Fallback {
    /// Which histogram bucket this decline is filed under. See the module
    /// docs' "Reporting why, not just how many".
    ///
    /// `LowerError::Unsupported`/`NoMatchingOperator` and
    /// `BuildError::Unsupported` all collapse to
    /// [`FallbackReasonKind::Unsupported`] deliberately, not just the literal
    /// `Unsupported` variants: a `NoMatchingOperator` from this bridge's own
    /// widened resolvers (see [`RealOperators`]/[`RealFunctions`]) means
    /// "the real `pg_operator`/`pg_proc` catalog has no such entry", which is
    /// exactly as actionable — and exactly as much "go build this" rather
    /// than "something is broken" — as an explicit `Unsupported`. Everything
    /// else in `LowerError`/`BuildError` (`UnknownName`, `Malformed`,
    /// `UnknownTable`, `UnknownCte`, `NonColumnKey`) reflects a real failure
    /// worth its own bucket instead.
    fn reason_kind(&self) -> FallbackReasonKind {
        match self {
            Fallback::Ineligible(_) => FallbackReasonKind::Ineligible,
            Fallback::Lower(LowerError::Unsupported(_) | LowerError::NoMatchingOperator(_)) => {
                FallbackReasonKind::Unsupported
            }
            Fallback::Lower(_) => FallbackReasonKind::LoweringError,
            Fallback::Build(BuildError::Unsupported(_)) => FallbackReasonKind::Unsupported,
            Fallback::Build(BuildError::Exec(_)) => FallbackReasonKind::ExecError,
            Fallback::Build(_) => FallbackReasonKind::BuildError,
            Fallback::Exec(_) => FallbackReasonKind::ExecError,
        }
    }
}

/// The coarse bucket a [`Fallback`] is filed under for
/// [`crate::Engine::owned_engine_fallback_reason_counts`]. Five categories
/// rather than one string per error message: a message is precise but
/// uncountable (every `NoMatchingOperator` names different argument types),
/// while this is coarse enough to sum, trend, and alert on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FallbackReasonKind {
    /// A per-table safety gate declined before lowering ever ran — RLS, a
    /// pending hot-tier/tombstone footprint, a view, promoted JSONB shadow
    /// columns, a name that isn't a valid/known table, or (see
    /// [`try_execute`]) an in-progress transaction. See [`build_resolver`].
    Ineligible,
    /// Lowering or building reached a construct neither implements yet —
    /// `LowerError::Unsupported`, `LowerError::NoMatchingOperator`, or
    /// `BuildError::Unsupported`. The one bucket that is a to-do list: an
    /// entry here names precisely what the owned pipeline should grow next.
    Unsupported,
    /// Lowering failed for a reason other than "not supported yet" —
    /// `LowerError::UnknownName` (a name [`build_resolver`] didn't
    /// prefetch, e.g. one reached only through a `FROM` shape
    /// [`collect_from_item`] does not walk — see the module docs) or
    /// `LowerError::Malformed` (a parse-tree shape assumption broken).
    LoweringError,
    /// Building the physical plan failed for a reason other than "not
    /// supported yet" — an unknown `TableId`/`CteId` or a sort/group key
    /// that isn't a plain column, both of which point at a planner/bridge
    /// bug rather than an unimplemented construct.
    BuildError,
    /// The physical plan built but erroring while it ran —
    /// `ExecError`, including one already wrapped inside
    /// `BuildError::Exec`. Covers, among other things, a resolved function
    /// or operator oid `basin-exec::eval` does not implement yet, or a
    /// resolved-but-mismatched argument type (see [`RealFunctions`]'s docs
    /// on why that is safe rather than a wrong-answer risk).
    ExecError,
}

/// Per-reason tally behind
/// [`crate::Engine::owned_engine_fallback_reason_counts`]. One `AtomicU64`
/// per [`FallbackReasonKind`] bucket, `Relaxed` throughout — this is an
/// approximate operational counter like every other one in this crate (see
/// `secondary_index::IndexSkipCounter`, `pk_row_cache::PkRowCacheCounters`),
/// not a consistency-critical value.
#[derive(Debug, Default)]
pub(crate) struct FallbackReasonCounters {
    ineligible: std::sync::atomic::AtomicU64,
    unsupported: std::sync::atomic::AtomicU64,
    lowering_error: std::sync::atomic::AtomicU64,
    build_error: std::sync::atomic::AtomicU64,
    exec_error: std::sync::atomic::AtomicU64,
}

impl FallbackReasonCounters {
    pub(crate) const fn new() -> Self {
        Self {
            ineligible: std::sync::atomic::AtomicU64::new(0),
            unsupported: std::sync::atomic::AtomicU64::new(0),
            lowering_error: std::sync::atomic::AtomicU64::new(0),
            build_error: std::sync::atomic::AtomicU64::new(0),
            exec_error: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(crate) fn record(&self, kind: FallbackReasonKind) {
        let counter = match kind {
            FallbackReasonKind::Ineligible => &self.ineligible,
            FallbackReasonKind::Unsupported => &self.unsupported,
            FallbackReasonKind::LoweringError => &self.lowering_error,
            FallbackReasonKind::BuildError => &self.build_error,
            FallbackReasonKind::ExecError => &self.exec_error,
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> FallbackReasonCountersSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        FallbackReasonCountersSnapshot {
            ineligible: self.ineligible.load(Relaxed),
            unsupported: self.unsupported.load(Relaxed),
            lowering_error: self.lowering_error.load(Relaxed),
            build_error: self.build_error.load(Relaxed),
            exec_error: self.exec_error.load(Relaxed),
        }
    }
}

/// A point-in-time, plain-`u64` snapshot of [`FallbackReasonCounters`] —
/// see [`crate::Engine::owned_engine_fallback_reason_counts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FallbackReasonCountersSnapshot {
    /// See [`FallbackReasonKind::Ineligible`].
    pub ineligible: u64,
    /// See [`FallbackReasonKind::Unsupported`].
    pub unsupported: u64,
    /// See [`FallbackReasonKind::LoweringError`].
    pub lowering_error: u64,
    /// See [`FallbackReasonKind::BuildError`].
    pub build_error: u64,
    /// See [`FallbackReasonKind::ExecError`].
    pub exec_error: u64,
}

impl FallbackReasonCountersSnapshot {
    /// Sum of every bucket. Equal to [`crate::Engine::owned_engine_fallback_count`]
    /// at any point no attempt is concurrently in flight — a test pins this
    /// invariant so the histogram can never silently drift from the total
    /// count it is supposed to add up to.
    pub fn total(&self) -> u64 {
        self.ineligible
            + self.unsupported
            + self.lowering_error
            + self.build_error
            + self.exec_error
    }
}

// ─── Shadow-compare ─────────────────────────────────────────────────────

/// How many divergence descriptions
/// [`crate::Engine::owned_engine_shadow_compare_divergences`] retains before
/// it stops recording new ones. The *count*
/// ([`crate::Engine::owned_engine_shadow_compare_divergence_count`]) keeps
/// rising past this — only the descriptions are capped. A run that diverges
/// on every one of thousands of queries would otherwise accumulate an
/// unbounded `Vec<String>` inside a long-lived `Engine`, and the first
/// handful of examples is what actually gets read; the counter is what gets
/// tracked.
pub(crate) const MAX_RECORDED_DIVERGENCES: usize = 64;

/// Both halves of the float tolerance from the module docs' decision 2: a
/// difference is ignored when it is within `1e-9` absolutely, OR within
/// `1e-9` relative to the larger operand — whichever admits more.
const FLOAT_TOLERANCE: f64 = 1e-9;

/// One recorded disagreement between the owned engine's answer and the
/// incumbent DataFusion path's answer for the same statement. See the module
/// docs' "Shadow-compare" section and
/// [`crate::Engine::owned_engine_shadow_compare_divergences`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowDivergence {
    /// The statement text both engines were given.
    pub sql: String,
    /// What differed, rendered for a human reading the log — the FIRST
    /// disagreement found, not an exhaustive diff: one concrete
    /// "row 3 column 1: owned 4, DataFusion 5" is what identifies the bug,
    /// and a full diff of a million-row result is not readable anyway.
    pub detail: String,
}

/// Tally + bounded sample behind
/// [`crate::Engine::owned_engine_shadow_compare_count`],
/// [`crate::Engine::owned_engine_shadow_compare_divergence_count`] and
/// [`crate::Engine::owned_engine_shadow_compare_divergences`]. Same shape
/// (and same `Relaxed`, approximate-operational-counter rationale) as
/// [`FallbackReasonCounters`] above; the one addition is the sample `Vec`,
/// which needs a real lock because it is not a single word.
#[derive(Debug, Default)]
pub(crate) struct ShadowCompareCounters {
    compared: std::sync::atomic::AtomicU64,
    diverged: std::sync::atomic::AtomicU64,
    /// Capped at [`MAX_RECORDED_DIVERGENCES`]. `std::sync::Mutex` rather
    /// than an async lock: every critical section here is a `push` or a
    /// `clone`, never an `.await`.
    divergences: std::sync::Mutex<Vec<ShadowDivergence>>,
}

impl ShadowCompareCounters {
    pub(crate) const fn new() -> Self {
        Self {
            compared: std::sync::atomic::AtomicU64::new(0),
            diverged: std::sync::atomic::AtomicU64::new(0),
            divergences: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn note_compared(&self) {
        self.compared
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn note_divergence(&self, sql: &str, detail: String) {
        self.diverged
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Recover from poison rather than propagate: this is a diagnostic
        // sample list, so a panic elsewhere while holding the lock cannot
        // have left an invariant broken that matters here — and turning a
        // diagnostic into a second panic would be strictly worse.
        let mut recorded = self
            .divergences
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if recorded.len() < MAX_RECORDED_DIVERGENCES {
            recorded.push(ShadowDivergence {
                sql: sql.to_string(),
                detail,
            });
        }
    }

    pub(crate) fn compared(&self) -> u64 {
        self.compared.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn diverged(&self) -> u64 {
        self.diverged.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn divergences(&self) -> Vec<ShadowDivergence> {
        self.divergences
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

/// The structural side-effect guard from the module docs' decision 3: the
/// ONLY thing shadow-compare will ever re-execute is a `SelectStmt` that is
/// side-effect free all the way down ([`is_side_effect_free`]).
///
/// Factored out of [`shadow_compare`] purely so it can be unit-tested
/// directly against a parsed `INSERT`/`UPDATE`/`DELETE` node — the guard is
/// what stops a future DML-carrying call site from double-writing, so it
/// needs a test that does not depend on today's call site only ever handing
/// this module SELECTs.
fn shadow_target(stmt_node: &Node) -> Option<&SelectStmt> {
    match stmt_node.node.as_ref() {
        Some(NodeEnum::SelectStmt(select)) if is_side_effect_free(select) => Some(select.as_ref()),
        _ => None,
    }
}

/// Whether re-running this `SelectStmt` is guaranteed to write nothing.
///
/// "It parsed as a `SelectStmt`" is NOT that guarantee, which is the whole
/// reason this function exists: `WITH x AS (INSERT INTO t VALUES (1)
/// RETURNING id) SELECT * FROM x` — a data-modifying CTE, ordinary Postgres
/// — roots at a `SelectStmt` whose `with_clause` holds the `InsertStmt`.
/// A guard that only inspected the outermost node kind would wave that
/// through and double-write on every execution with the flag on. This walks
/// the `WITH` list (and each set-operation arm, which carries its own) and
/// requires every CTE body to itself be a side-effect-free `SelectStmt`.
///
/// Deliberately conservative: any CTE body this cannot positively identify
/// as a `SelectStmt` — including node kinds that do not exist yet — is
/// treated as writable. Being wrong in that direction costs one skipped
/// comparison; being wrong the other way corrupts the user's data.
fn is_side_effect_free(select: &SelectStmt) -> bool {
    if let Some(with) = select.with_clause.as_ref() {
        for cte in &with.ctes {
            let Some(NodeEnum::CommonTableExpr(cte)) = cte.node.as_ref() else {
                return false;
            };
            match cte.ctequery.as_ref().and_then(|q| q.node.as_ref()) {
                Some(NodeEnum::SelectStmt(body)) => {
                    if !is_side_effect_free(body) {
                        return false;
                    }
                }
                // INSERT/UPDATE/DELETE/MERGE — or anything unrecognised.
                _ => return false,
            }
        }
    }
    // `a UNION b` puts each arm in `larg`/`rarg`, and an arm can carry its
    // own `WITH`.
    for arm in [select.larg.as_ref(), select.rarg.as_ref()]
        .into_iter()
        .flatten()
    {
        if !is_side_effect_free(arm) {
            return false;
        }
    }
    true
}

/// Re-run `sql` through the incumbent DataFusion path and diff its answer
/// against the one the owned engine just produced. See the module docs'
/// "Shadow-compare" section for the four decisions this encodes.
///
/// Never returns an error and never affects what the client sees: a failure
/// on the incumbent side is itself recorded as a divergence (the two paths
/// disagreed on whether the statement even succeeds, which is exactly the
/// kind of finding this mode exists for) and execution continues.
async fn shadow_compare(sess: &ProjectSession, stmt_node: &Node, sql: &str, owned: &ExecResult) {
    // Decision 3. Structural, first thing, before anything is executed.
    let Some(select) = shadow_target(stmt_node) else {
        tracing::debug!(
            target: "basin_engine::owned_engine",
            "shadow-compare declined a non-SELECT node — re-executing it could double-write"
        );
        return;
    };

    let reference = match crate::executor::exec_select_reference(sess, sql).await {
        Ok(reference) => reference,
        Err(e) => {
            record_shadow_result(
                sess,
                sql,
                Some(format!(
                    "the owned engine served this statement but the incumbent \
                     DataFusion path errored on it: {e}"
                )),
            );
            return;
        }
    };

    let detail = compare_results(select, owned, &reference);
    record_shadow_result(sess, sql, detail);
}

/// Bump the comparison counter and, when `detail` is `Some`, the divergence
/// counter + bounded sample, and log. The single place every shadow-compare
/// outcome funnels through, so the two counters cannot drift apart — the
/// same reason [`record_fallback`] exists.
fn record_shadow_result(sess: &ProjectSession, sql: &str, detail: Option<String>) {
    sess.engine.note_owned_engine_shadow_compare();
    if let Some(detail) = detail {
        tracing::warn!(
            target: "basin_engine::owned_engine",
            sql = %sql,
            detail = %detail,
            "shadow-compare divergence: the owned engine and DataFusion disagree"
        );
        sess.engine
            .note_owned_engine_shadow_compare_divergence(sql, detail);
    } else {
        tracing::debug!(
            target: "basin_engine::owned_engine",
            sql = %sql,
            "shadow-compare agreed"
        );
    }
}

/// One cell of one result row, normalised so the two engines' answers are
/// comparable without depending on either one's physical types.
///
/// `Float` exists as its own variant rather than being rendered to text like
/// everything else because it is the one type compared with a tolerance
/// (module docs, decision 2) — a rendered `"0.30000000000000004"` vs
/// `"0.3"` cannot be compared with one, and both engines rendering the same
/// `f64` is not guaranteed either.
#[derive(Debug, Clone, PartialEq)]
enum Cell {
    Null,
    /// A `FLOAT4` (widened to `f64`) or `FLOAT8` value.
    Float(f64),
    /// Every other type — including `NUMERIC`/`DECIMAL` — as its exactly
    /// rendered text. See [`display_cell`].
    Text(String),
}

/// Render one non-float cell to the exact text it is compared by, via
/// Arrow's own display formatter — which is type-aware (a `Decimal128` keeps
/// its scale, a `Date32` renders as a date, a list renders its elements)
/// rather than a `Debug` dump of the physical representation, so two engines
/// that answer with the same *value* in different but equivalent physical
/// encodings still compare equal.
///
/// Builds a formatter per cell rather than per column: this whole mode
/// already runs every query twice end to end (decision 4), so the constant
/// factor here is not the thing worth optimising, and per-cell keeps the
/// call sites trivial.
fn display_cell(array: &dyn Array, row: usize) -> String {
    match ArrayFormatter::try_new(array, &FormatOptions::default()) {
        Ok(formatter) => formatter.value(row).to_string(),
        // A type Arrow itself declines to format. Fall back to something
        // stable and comparable rather than skipping the cell: two identical
        // unformattable values still produce the same string, so this can
        // only ever under-report, never invent a divergence.
        Err(_) => format!("<unformattable {}>", array.data_type()),
    }
}

/// Flatten batches into plain rows, so a comparison never depends on how
/// either engine happened to chunk its output.
fn rows_of(batches: &[RecordBatch]) -> Vec<Vec<Cell>> {
    let mut out: Vec<Vec<Cell>> = Vec::new();
    for batch in batches {
        let base = out.len();
        out.resize_with(base + batch.num_rows(), Vec::new);
        for column in batch.columns() {
            for row in 0..batch.num_rows() {
                let cell = if column.is_null(row) {
                    Cell::Null
                } else {
                    match column.data_type() {
                        DataType::Float64 => {
                            Cell::Float(column.as_primitive::<Float64Type>().value(row))
                        }
                        DataType::Float32 => {
                            Cell::Float(column.as_primitive::<Float32Type>().value(row) as f64)
                        }
                        _ => Cell::Text(display_cell(column.as_ref(), row)),
                    }
                };
                out[base + row].push(cell);
            }
        }
    }
    out
}

/// Whether two floats agree within the module docs' decision-2 tolerance.
///
/// `NaN` is treated as equal to `NaN` deliberately: IEEE-754 says otherwise,
/// but two engines both answering "not a number" AGREE, and reporting that
/// as a divergence would fire on every `0/0`, every `avg()` of an empty
/// float group, and every `'NaN'::float8` in the corpus. Infinities compare
/// by exact equality (`+inf == +inf`, `+inf != -inf`) — no tolerance is
/// meaningful there, and `inf - inf` is `NaN`, which would otherwise make
/// the subtraction below silently declare them unequal.
fn float_eq(a: f64, b: f64) -> bool {
    if a.is_nan() || b.is_nan() {
        return a.is_nan() && b.is_nan();
    }
    if a == b {
        return true;
    }
    if a.is_infinite() || b.is_infinite() {
        return false;
    }
    let diff = (a - b).abs();
    diff <= FLOAT_TOLERANCE || diff <= FLOAT_TOLERANCE * a.abs().max(b.abs())
}

/// Whether two cells agree — tolerant for floats, exact for everything else.
/// See the module docs' decision 2.
fn cell_eq(a: &Cell, b: &Cell) -> bool {
    match (a, b) {
        (Cell::Null, Cell::Null) => true,
        (Cell::Float(x), Cell::Float(y)) => float_eq(*x, *y),
        (Cell::Text(x), Cell::Text(y)) => x == y,
        // One engine typed the column as FLOAT and the other did not (a
        // literal `1.5` lowered to `Decimal128` on one side and `Float64` on
        // the other, say). That is a *type* difference, not a value one, and
        // this mode is looking for wrong answers: if the non-float side
        // parses back to the same number within tolerance, the answers
        // agree. If it does not parse at all, they genuinely differ.
        (Cell::Float(x), Cell::Text(y)) | (Cell::Text(y), Cell::Float(x)) => {
            y.parse::<f64>().map(|y| float_eq(*x, y)).unwrap_or(false)
        }
        // NULL against a value is always a divergence — the one case where
        // being forgiving would hide a real bug.
        _ => false,
    }
}

/// The canonical key both sides are sorted by when the statement has no
/// `ORDER BY` (module docs, decision 1). Identical on both sides by
/// construction — it is derived only from the normalised [`Cell`]s, never
/// from either engine's physical types or batch layout.
///
/// Floats are keyed by a fixed 9-significant-digit rendering rather than
/// their full precision, so a pair of values the tolerance would call equal
/// almost always lands in the same key and therefore the same sorted
/// position. "Almost always" is the honest word: two tolerance-equal values
/// straddling a rounding boundary would sort apart and be reported as a
/// divergence. That is a false positive in a diagnostic mode, not a wrong
/// answer to a client, and it needs a divergence-free duplicate float key to
/// even arise.
fn row_sort_key(row: &[Cell]) -> String {
    let mut key = String::new();
    for cell in row {
        match cell {
            // NUL sorts before every printable rendering, so NULLs cluster
            // at one end on both sides — and is distinct from the empty
            // string, which contributes no bytes before the separator.
            Cell::Null => key.push('\u{0}'),
            Cell::Float(v) => key.push_str(&format!("{v:+.9e}")),
            Cell::Text(t) => key.push_str(t),
        }
        key.push('\u{1}');
    }
    key
}

/// How a cell is named in a divergence message.
fn describe_cell(cell: &Cell) -> String {
    match cell {
        Cell::Null => "NULL".to_string(),
        Cell::Float(v) => format!("{v}"),
        Cell::Text(t) => format!("{t:?}"),
    }
}

/// Diff the owned engine's answer against the incumbent's, returning `None`
/// when they agree and `Some(detail)` describing the first disagreement
/// otherwise.
///
/// `select` is consulted for exactly one thing: whether the statement has a
/// top-level `ORDER BY`, which decides positional vs. canonically-sorted
/// comparison (module docs, decision 1).
///
/// Column *names* are deliberately not compared. The owned engine derives
/// output names from `basin-plan`'s lowering and DataFusion from its own
/// planner; a differing label on an unaliased expression column is a
/// cosmetic difference, and this mode is looking for wrong values.
fn compare_results(
    select: &SelectStmt,
    owned: &ExecResult,
    reference: &ExecResult,
) -> Option<String> {
    let (owned_schema, owned_batches, ref_schema, ref_batches) = match (owned, reference) {
        (
            ExecResult::Rows {
                schema: owned_schema,
                batches: owned_batches,
            },
            ExecResult::Rows {
                schema: ref_schema,
                batches: ref_batches,
            },
        ) => (owned_schema, owned_batches, ref_schema, ref_batches),
        (ExecResult::Empty { tag: owned_tag }, ExecResult::Empty { tag: ref_tag }) => {
            return (owned_tag != ref_tag).then(|| {
                format!("command tag differs: owned {owned_tag:?}, DataFusion {ref_tag:?}")
            });
        }
        (ExecResult::Rows { .. }, ExecResult::Empty { tag }) => {
            return Some(format!(
                "owned engine returned a result set, DataFusion returned the bare tag {tag:?}"
            ));
        }
        (ExecResult::Empty { tag }, ExecResult::Rows { .. }) => {
            return Some(format!(
                "owned engine returned the bare tag {tag:?}, DataFusion returned a result set"
            ));
        }
    };

    if owned_schema.fields().len() != ref_schema.fields().len() {
        return Some(format!(
            "column count differs: owned {}, DataFusion {}",
            owned_schema.fields().len(),
            ref_schema.fields().len()
        ));
    }

    let mut owned_rows = rows_of(owned_batches);
    let mut ref_rows = rows_of(ref_batches);

    if owned_rows.len() != ref_rows.len() {
        return Some(format!(
            "row count differs: owned {}, DataFusion {}",
            owned_rows.len(),
            ref_rows.len()
        ));
    }

    // Decision 1: positional only when the statement actually asked for an
    // order; otherwise both sides are put in the same canonical order first.
    let ordered = !select.sort_clause.is_empty();
    if !ordered {
        owned_rows.sort_by_cached_key(|row| row_sort_key(row));
        ref_rows.sort_by_cached_key(|row| row_sort_key(row));
    }
    let how = if ordered {
        "positional compare — the statement has ORDER BY"
    } else {
        "compared after a canonical sort — the statement has no ORDER BY"
    };

    for (r, (owned_row, ref_row)) in owned_rows.iter().zip(ref_rows.iter()).enumerate() {
        for (c, (owned_cell, ref_cell)) in owned_row.iter().zip(ref_row.iter()).enumerate() {
            if !cell_eq(owned_cell, ref_cell) {
                return Some(format!(
                    "row {r} column {c} differs ({how}): owned {}, DataFusion {}",
                    describe_cell(owned_cell),
                    describe_cell(ref_cell)
                ));
            }
        }
    }

    None
}

/// Apply shadow-compare's rules to two answers to `sql` that the caller
/// already has — the same [`compare_results`] the flag-on path uses, minus
/// the flag and minus the second execution.
///
/// This exists because the flag-on path can only ever compare two answers
/// that *agree*: both engines read the same committed files, so a passing
/// end-to-end run proves the oracle ran, not that it can tell a divergence
/// apart from a benign difference. Given two results the caller obtained
/// deliberately (the same rows in a different order, one value changed, a
/// float perturbed by a ULP) this pins the rules themselves. See
/// `tests/shadow_compare.rs`.
///
/// Returns `None` when the two answers agree, `Some(detail)` otherwise —
/// including when `sql` is not a single, side-effect-free `SELECT`, since
/// there is then no statement whose `ORDER BY` could decide positional
/// versus canonical comparison.
pub fn compare_shadow_results(
    sql: &str,
    owned: &ExecResult,
    reference: &ExecResult,
) -> Option<String> {
    let parsed = match pg_query::parse(sql) {
        Ok(parsed) => parsed,
        Err(e) => return Some(format!("{sql:?} did not parse: {e}")),
    };
    let node = match parsed.protobuf.stmts.as_slice() {
        [only] => only.stmt.clone(),
        stmts => {
            return Some(format!(
                "expected exactly one statement in {sql:?}, got {}",
                stmts.len()
            ))
        }
    };
    let Some(node) = node else {
        return Some(format!("{sql:?} parsed to an empty statement"));
    };
    let Some(select) = shadow_target(&node) else {
        return Some(format!(
            "{sql:?} is not a side-effect-free SELECT, so it has no shadow comparison"
        ));
    };
    compare_results(select, owned, reference)
}

async fn try_execute_inner(
    sess: &ProjectSession,
    stmt_node: &Node,
) -> Result<ExecResult, Fallback> {
    let resolver = build_resolver(sess, stmt_node).await?;

    // `lower_dml` has existed in basin-plan the whole time and nothing here
    // called it, so every INSERT, UPDATE and DELETE fell back regardless of
    // whether the engine could serve it. DML is lowered as a relation like any
    // other plan, so everything downstream — optimize, build, execute — is
    // unchanged; only the entry point differs.
    let plan = match stmt_node.node.as_ref() {
        Some(NodeEnum::InsertStmt(_))
        | Some(NodeEnum::UpdateStmt(_))
        | Some(NodeEnum::DeleteStmt(_)) => {
            basin_plan::lower::dml::lower_dml(stmt_node, &resolver, &RealOperators, &RealFunctions)
        }
        _ => lower_select(stmt_node, &resolver, &RealOperators, &RealFunctions),
    }
    .map_err(Fallback::Lower)?;

    // `optimize_default` is the full assembled pipeline — decorrelation,
    // filter pushdown, limit pushdown and projection pruning, run to a
    // fixpoint (see `basin_plan::opt`'s module docs for why the order
    // matters) — not the two-rule hand-picked subset this bridge used to
    // call directly. Every lowered plan that reaches this point goes
    // through every rule basin-plan has, not just the two that happened to
    // be wired up first.
    let (plan, passes) = basin_plan::opt::optimize_default(plan);
    sess.engine.note_owned_engine_optimizer_passes(passes);

    let mut op = basin_exec::build::build(&plan, &resolver).map_err(Fallback::Build)?;

    // A plain synchronous drain, deliberately with no `.await` in the loop:
    // `Box<dyn Operator>` (and the resolver's `StorageTableResolver`,
    // holding an `mpsc::Receiver`) are not required to be `Send` by their
    // trait definitions, and this whole async fn is reached from spawn
    // sites elsewhere in the engine that require the containing future to
    // stay `Send` — which only constrains values *live across a suspend
    // point*, not ordinary local work. `Operator::next_batch` is itself
    // documented as bounded work per call (see `basin-exec`'s crate docs on
    // cancellation), so this stays a bounded loop even without yielding.
    let schema = op.schema();
    let mut batches = Vec::new();
    loop {
        match op.next_batch() {
            Ok(Some(batch)) => batches.push(batch),
            Ok(None) => break,
            Err(e) => return Err(Fallback::Exec(e)),
        }
    }

    Ok(ExecResult::Rows { schema, batches })
}

// ─── Table resolution ───────────────────────────────────────────────────

/// A [`PlanTableResolver`] and [`ExecTableResolver`] backed by Basin's real
/// catalog and storage, built fresh for one statement. See the module docs
/// for why table resolution happens up front (async) rather than inside the
/// synchronous trait methods.
struct CatalogTableResolver {
    /// Keyed by the lowercased last name segment, mirroring how
    /// `resolve_table` is asked to resolve a (possibly schema-qualified)
    /// name — see `lower/select.rs`'s own `MockTables` test double, which
    /// uses the identical convention.
    plan_tables: HashMap<String, (TableId, PlanSchema)>,
    exec: StorageTableResolver,
}

impl CatalogTableResolver {
    fn new(storage: basin_storage::Storage) -> Self {
        Self {
            plan_tables: HashMap::new(),
            exec: StorageTableResolver::new(storage),
        }
    }

    /// `live_files` is the catalog's `live_data_files()` for this table, taken
    /// at resolution time and pinned for the whole statement. See
    /// [`basin_exec::storage_source::StorageTableResolver::register`] for why
    /// a file SET rather than a table name, and `build_resolver` below for
    /// where it comes from.
    fn register(
        &mut self,
        key: String,
        table_id: TableId,
        plan_schema: PlanSchema,
        project: ProjectId,
        arrow_schema: arrow_schema::SchemaRef,
        live_files: Vec<basin_storage::DataFile>,
    ) {
        self.plan_tables.insert(key, (table_id, plan_schema));
        self.exec
            .register(table_id, project, arrow_schema, live_files);
    }
}

impl PlanTableResolver for CatalogTableResolver {
    fn resolve_table(&self, name: &[String]) -> Option<(TableId, PlanSchema)> {
        let key = name.last()?.to_ascii_lowercase();
        self.plan_tables.get(&key).cloned()
    }
}

impl ExecTableResolver for CatalogTableResolver {
    fn open(
        &self,
        table: TableId,
        projection: &[usize],
        filters: &[PlanExpr],
    ) -> Option<(Box<dyn BatchSource>, ScanPushdown)> {
        self.exec.open(table, projection, filters)
    }
}

/// Walk `stmt_node`'s `FROM` clause(s) — including `UNION`/`INTERSECT`/
/// `EXCEPT` arms, which `lower_select_stmt` also recurses into — for every
/// referenced table name, resolve each one against the real catalog exactly
/// once, and reject (via [`Fallback::Ineligible`]) any table this bridge is
/// not yet safe to read directly. See the module docs for the full
/// rationale of each check below.
async fn build_resolver(
    sess: &ProjectSession,
    stmt_node: &Node,
) -> Result<CatalogTableResolver, Fallback> {
    let mut wanted = Vec::new();
    collect_tables(stmt_node, &mut wanted);

    let storage = sess.engine.config().storage.clone();
    let mut resolver = CatalogTableResolver::new(storage);
    let mut seen = HashSet::new();
    let mut next_id: u32 = 1;

    for parts in wanted {
        let Some(last) = parts.last() else { continue };
        let key = last.to_ascii_lowercase();
        if !seen.insert(key.clone()) {
            continue; // already resolved (self-join, repeated reference, ...)
        }

        let table_name = TableName::new(last.as_str())
            .map_err(|_| Fallback::Ineligible("not a valid table identifier"))?;

        let (meta, view_present) = crate::session::load_table_meta_cached(sess, &table_name)
            .await
            .ok_or(Fallback::Ineligible("table not found in the catalog"))?;

        if view_present {
            return Err(Fallback::Ineligible(
                "name resolves to a view, not a base table",
            ));
        }
        if meta.rls_enabled {
            return Err(Fallback::Ineligible(
                "row-level security is enabled on this table",
            ));
        }
        if meta.schema.fields().iter().any(|f| {
            f.name()
                .starts_with(crate::promoted_columns::SHADOW_COL_PREFIX)
        }) {
            return Err(Fallback::Ineligible(
                "table carries promoted JSONB shadow columns",
            ));
        }
        // Any hot-tier footprint (unflushed inserts, or update/delete
        // tombstones — `tombstone_cold_scan.rs`'s own doc comment confirms
        // DELETE tombstones live in this same registry, not a separate
        // cold-tier mechanism) means the committed cold files this bridge
        // reads are not the whole story for this table.
        if let Some(entry) = sess
            .engine
            .memtable_registry()
            .get(&sess.project, &table_name)
        {
            if entry.memtable.total_count() != 0 {
                return Err(Fallback::Ineligible(
                    "table has pending hot-tier rows or tombstones",
                ));
            }
        }

        let table_id = TableId(next_id);
        next_id += 1;
        let plan_schema: PlanSchema = meta
            .schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), pgtype_of(f)))
            .collect();
        // THE file set this statement's scans read, taken from the catalog —
        // not re-derived by LIST'ing the table prefix down in storage.
        //
        // A LIST answers "what physically exists", and existence is not
        // liveness: a file superseded by a copy-on-write UPDATE/DELETE, a
        // compaction or a stripe merge stays on the object store long after it
        // leaves the table. `basin-shard` retains superseded compaction inputs
        // for `BASIN_SUPERSEDED_DELETE_GRACE_SECS` — 300 seconds by default —
        // so that in-flight scans do not 404, and `dml_mutate`'s own cleanup
        // runs from a detached task. Scanning the LIST inside that window
        // returns each affected row once per physically present copy, at its
        // pre- AND post-update values both: measured 6, then 9, then 12 rows
        // where the truth was 3, and `count(*)` = 6 where the truth was 3.
        //
        // `live_data_files()` is what the DataFusion path has always used
        // (`session::refresh_table_inner`, bug #41). It is safe HERE, and only
        // here, because the engine commits every write before making it
        // visible; pushing the same catalog lookup down into `Storage::read`
        // is the bc57fa48 regression, where a flushed-but-uncommitted file
        // (`note_uncommitted_file`'s window) goes invisible and reads LOSE
        // rows.
        //
        // Pinning the set here also gives the statement a stable file set for
        // its whole execution — every scan of the same table in one statement
        // (self-join, repeated reference) reads exactly the same files.
        //
        // The per-file `column_stats` ride along because the resolver still
        // has to prune files by predicate before opening them, and that prune
        // used to live inside `Storage::read` (which LISTed and read footers
        // to get the same numbers). The catalog has carried them since Phase
        // 5.7 A4 for exactly this purpose, so the prune is now free.
        let live_files: Vec<basin_storage::DataFile> = meta
            .live_data_files()
            .into_iter()
            .map(|f| basin_storage::DataFile {
                path: object_store::path::Path::from(f.path.as_str()),
                tier: basin_storage::Tier::from_path(&f.path),
                size_bytes: f.size_bytes,
                row_count: f.row_count,
                column_stats: f.column_stats,
                bloom_filters: f.bloom_filters,
                hll_sketches: f.hll_sketches,
                tdigest_sketches: f.tdigest_sketches,
            })
            .collect();
        resolver.register(
            key,
            table_id,
            plan_schema,
            sess.project,
            meta.schema.clone(),
            live_files,
        );
    }

    Ok(resolver)
}

/// Collect every table name a `SELECT` statement references — its `FROM`
/// clause(s), `UNION`/`INTERSECT`/`EXCEPT` arms (exactly the way
/// `lower_select_stmt` recurses), any `WITH` clause's CTE bodies, and any
/// `SubLink` subquery reachable from an expression clause (see
/// [`collect_expr`]).
fn collect_tables(node: &Node, out: &mut Vec<Vec<String>>) {
    let empty = HashSet::new();
    match node.node.as_ref() {
        Some(NodeEnum::SelectStmt(stmt)) => collect_tables_stmt(stmt, &empty, out),
        // DML's target relation has to be prefetched exactly like a FROM
        // entry, and so does anything it reads: INSERT ... SELECT's source,
        // UPDATE ... FROM's extra relations, DELETE ... USING's. Without this
        // the resolver is handed nothing and every INSERT, UPDATE and DELETE
        // falls back — not because the engine cannot run them (basin-plan has
        // `lower_dml`) but because the bridge never told it what to load.
        // The WITH clause is walked FIRST in each arm below, not last. It has
        // to be: it both contributes tables (the CTE bodies) and establishes
        // the names that must NOT be collected, so anything walked before it
        // is walked without knowing what the CTE names are. Collecting the
        // target and the source first sent the CTE's own name to the resolver
        // as if it were a table.
        Some(NodeEnum::InsertStmt(stmt)) => {
            let mut scope = HashSet::new();
            if let Some(with) = stmt.with_clause.as_ref() {
                collect_with_clause(with, &mut scope, out);
            }
            if let Some(rel) = stmt.relation.as_ref() {
                collect_range_var(rel, &scope, out);
            }
            if let Some(NodeEnum::SelectStmt(sel)) =
                stmt.select_stmt.as_deref().and_then(|n| n.node.as_ref())
            {
                collect_tables_stmt(sel, &scope, out);
            }
            collect_exprs(&stmt.returning_list, &scope, out);
        }
        Some(NodeEnum::UpdateStmt(stmt)) => {
            let mut scope = HashSet::new();
            if let Some(with) = stmt.with_clause.as_ref() {
                collect_with_clause(with, &mut scope, out);
            }
            if let Some(rel) = stmt.relation.as_ref() {
                collect_range_var(rel, &scope, out);
            }
            for item in &stmt.from_clause {
                collect_from_item(item, &scope, out);
            }
            // A DML statement's own clauses hold subqueries exactly the way a
            // `SELECT`'s do — `WHERE id IN (SELECT ... FROM u)`, `SET c =
            // (SELECT ... FROM u)`, `RETURNING (SELECT ... FROM u)`. Walking
            // only `WITH`/target/`FROM` left `u` unprefetched, so lowering
            // died on `UnknownName("u")` for a shape it can otherwise handle.
            // `target_list` here is the `SET` list (`ResTarget`s), which
            // `collect_expr` already unwraps to its value expression.
            collect_exprs(&stmt.target_list, &scope, out);
            collect_opt_expr(stmt.where_clause.as_deref(), &scope, out);
            collect_exprs(&stmt.returning_list, &scope, out);
        }
        Some(NodeEnum::DeleteStmt(stmt)) => {
            let mut scope = HashSet::new();
            if let Some(with) = stmt.with_clause.as_ref() {
                collect_with_clause(with, &mut scope, out);
            }
            if let Some(rel) = stmt.relation.as_ref() {
                collect_range_var(rel, &scope, out);
            }
            for item in &stmt.using_clause {
                collect_from_item(item, &scope, out);
            }
            collect_opt_expr(stmt.where_clause.as_deref(), &scope, out);
            collect_exprs(&stmt.returning_list, &scope, out);
        }
        _ => {}
    }
}

/// `cte_scope` is the set of (lowercased, unqualified) CTE names visible to
/// `stmt` — names `lower_select` will resolve against its own CTE
/// environment rather than the catalog. A bare `FROM x` where `x` is in
/// scope must not end up in `out`: `build_resolver` would then ask the real
/// catalog for a table named `x`, which does not exist, turning a query that
/// `lower_select` can otherwise serve into an `Ineligible` fallback instead.
fn collect_tables_stmt(stmt: &SelectStmt, cte_scope: &HashSet<String>, out: &mut Vec<Vec<String>>) {
    let mut scope = cte_scope.clone();
    if let Some(with) = stmt.with_clause.as_ref() {
        collect_with_clause(with, &mut scope, out);
    }

    let op_kind = SetOperation::try_from(stmt.op).unwrap_or(SetOperation::Undefined);
    if op_kind != SetOperation::SetopNone {
        if let Some(l) = stmt.larg.as_deref() {
            collect_tables_stmt(l, &scope, out);
        }
        if let Some(r) = stmt.rarg.as_deref() {
            collect_tables_stmt(r, &scope, out);
        }
        // A set-op node carries no target list or `WHERE` of its own — the
        // arms do — but `ORDER BY` on the whole result hangs here.
        collect_exprs(&stmt.sort_clause, &scope, out);
        return;
    }
    for item in &stmt.from_clause {
        collect_from_item(item, &scope, out);
    }
    collect_exprs(&stmt.target_list, &scope, out);
    collect_opt_expr(stmt.where_clause.as_deref(), &scope, out);
    collect_opt_expr(stmt.having_clause.as_deref(), &scope, out);
    collect_exprs(&stmt.group_clause, &scope, out);
    collect_exprs(&stmt.sort_clause, &scope, out);
}

/// Every expression node kind that can *contain* a [`SubLink`], walked purely
/// to reach the subquery bodies inside — nothing here inspects a `ColumnRef`,
/// so no alias, correlated or otherwise, can reach `out`; only
/// [`collect_range_var`] ever pushes a name.
///
/// `cte_scope` is passed down unchanged, including into a `SubLink`'s body: a
/// subquery sees exactly the CTE names its enclosing statement level sees, so
/// `WHERE id IN (SELECT ... FROM cte)` keeps excluding `cte` rather than
/// sending it to the catalog as a table that does not exist.
///
/// Anything unlisted falls to `_ => {}` on the same terms as the rest of this
/// module: under-collecting costs a fallback, never a wrong answer. A subquery
/// in `FROM` is not reached from here — it is a `RangeSubselect`, handled by
/// [`collect_from_item`], which walks it.
fn collect_expr(node: &Node, cte_scope: &HashSet<String>, out: &mut Vec<Vec<String>>) {
    match node.node.as_ref() {
        // The whole reason this function exists.
        Some(NodeEnum::SubLink(sl)) => {
            collect_opt_expr(sl.testexpr.as_deref(), cte_scope, out);
            if let Some(NodeEnum::SelectStmt(inner)) =
                sl.subselect.as_deref().and_then(|n| n.node.as_ref())
            {
                collect_tables_stmt(inner, cte_scope, out);
            }
        }
        Some(NodeEnum::BoolExpr(e)) => collect_exprs(&e.args, cte_scope, out),
        Some(NodeEnum::AExpr(e)) => {
            collect_opt_expr(e.lexpr.as_deref(), cte_scope, out);
            collect_opt_expr(e.rexpr.as_deref(), cte_scope, out);
        }
        Some(NodeEnum::List(l)) => collect_exprs(&l.items, cte_scope, out),
        Some(NodeEnum::ResTarget(rt)) => collect_opt_expr(rt.val.as_deref(), cte_scope, out),
        Some(NodeEnum::SortBy(sb)) => collect_opt_expr(sb.node.as_deref(), cte_scope, out),
        Some(NodeEnum::FuncCall(fc)) => {
            collect_exprs(&fc.args, cte_scope, out);
            collect_exprs(&fc.agg_order, cte_scope, out);
            collect_opt_expr(fc.agg_filter.as_deref(), cte_scope, out);
            if let Some(w) = fc.over.as_deref() {
                collect_exprs(&w.partition_clause, cte_scope, out);
                collect_exprs(&w.order_clause, cte_scope, out);
                collect_opt_expr(w.start_offset.as_deref(), cte_scope, out);
                collect_opt_expr(w.end_offset.as_deref(), cte_scope, out);
            }
        }
        Some(NodeEnum::CaseExpr(c)) => {
            collect_opt_expr(c.arg.as_deref(), cte_scope, out);
            collect_exprs(&c.args, cte_scope, out);
            collect_opt_expr(c.defresult.as_deref(), cte_scope, out);
        }
        Some(NodeEnum::CaseWhen(c)) => {
            collect_opt_expr(c.expr.as_deref(), cte_scope, out);
            collect_opt_expr(c.result.as_deref(), cte_scope, out);
        }
        Some(NodeEnum::CoalesceExpr(c)) => collect_exprs(&c.args, cte_scope, out),
        Some(NodeEnum::MinMaxExpr(m)) => collect_exprs(&m.args, cte_scope, out),
        Some(NodeEnum::RowExpr(r)) => collect_exprs(&r.args, cte_scope, out),
        Some(NodeEnum::AArrayExpr(a)) => collect_exprs(&a.elements, cte_scope, out),
        Some(NodeEnum::TypeCast(tc)) => collect_opt_expr(tc.arg.as_deref(), cte_scope, out),
        Some(NodeEnum::CollateClause(cc)) => collect_opt_expr(cc.arg.as_deref(), cte_scope, out),
        Some(NodeEnum::NullTest(nt)) => collect_opt_expr(nt.arg.as_deref(), cte_scope, out),
        Some(NodeEnum::BooleanTest(bt)) => collect_opt_expr(bt.arg.as_deref(), cte_scope, out),
        Some(NodeEnum::NamedArgExpr(na)) => collect_opt_expr(na.arg.as_deref(), cte_scope, out),
        Some(NodeEnum::AIndirection(ai)) => {
            collect_opt_expr(ai.arg.as_deref(), cte_scope, out);
            collect_exprs(&ai.indirection, cte_scope, out);
        }
        Some(NodeEnum::AIndices(ai)) => {
            collect_opt_expr(ai.lidx.as_deref(), cte_scope, out);
            collect_opt_expr(ai.uidx.as_deref(), cte_scope, out);
        }
        Some(NodeEnum::GroupingSet(gs)) => collect_exprs(&gs.content, cte_scope, out),
        _ => {}
    }
}

fn collect_exprs(nodes: &[Node], cte_scope: &HashSet<String>, out: &mut Vec<Vec<String>>) {
    for n in nodes {
        collect_expr(n, cte_scope, out);
    }
}

fn collect_opt_expr(node: Option<&Node>, cte_scope: &HashSet<String>, out: &mut Vec<Vec<String>>) {
    if let Some(n) = node {
        collect_expr(n, cte_scope, out);
    }
}

/// Walk one `WITH` list, collecting each CTE body's own table references
/// into `out` and growing `scope` with each CTE's name as it goes — mirrors
/// `parse_cte.c`'s visibility rules close enough for this bridge's purposes:
///
/// * A later CTE (and the statement's own `FROM`) can see every earlier CTE
///   in the same list — `scope` gains `ctename` only *after* that CTE's own
///   body has been walked, so an earlier CTE never sees a later one.
/// * `WITH RECURSIVE` (`with.recursive`, the grammar-level flag set on the
///   whole list — `pg_query` never populates the per-CTE `cterecursive`,
///   that is a parse-analysis output this crate never runs) additionally
///   lets a CTE see its own name while its own body is walked, so `FROM
///   self` inside a recursive CTE's body is excluded exactly like a
///   reference to any other CTE, not sent to the catalog as a nonexistent
///   table.
///
/// A nested `WITH` inside a CTE's body (`WITH a AS (WITH b AS (...) SELECT
/// ...) SELECT ...`) needs no special case here: `collect_tables_stmt`
/// clones the incoming scope and folds in its own `with_clause` the same way
/// for every statement it is called on, so recursing into `cte.ctequery`
/// below already walks it correctly.
fn collect_with_clause(with: &WithClause, scope: &mut HashSet<String>, out: &mut Vec<Vec<String>>) {
    for cte_node in &with.ctes {
        let Some(NodeEnum::CommonTableExpr(cte)) = cte_node.node.as_ref() else {
            continue;
        };
        let name = cte.ctename.to_ascii_lowercase();

        let mut body_scope = scope.clone();
        if with.recursive {
            body_scope.insert(name.clone());
        }

        if let Some(query) = cte.ctequery.as_deref() {
            if let Some(NodeEnum::SelectStmt(inner)) = query.node.as_ref() {
                collect_tables_stmt(inner, &body_scope, out);
            }
            // A data-modifying CTE body (INSERT/UPDATE/DELETE ... RETURNING)
            // is not a SelectStmt; nothing to walk, and lowering will reject
            // it as Unsupported regardless of what this collects.
        }

        scope.insert(name);
    }
}

/// Mirrors `lower/select.rs`'s `build_from_item`/`build_join_expr` shape
/// (`RangeVar`, and `JoinExpr` recursing into both sides plus its `ON`
/// qual, which is an expression like any other). Anything else
/// (a subquery or set-returning function in `FROM`) is already
/// `LowerError::Unsupported` at lowering time regardless of what this
/// collects, so there is nothing to gain by recognising it here too.
/// A `RangeVar` is the one shape that names a real relation, and it is reached
/// from two directions: a `FROM` entry (a `Node`) and DML's target relation (a
/// bare `RangeVar` field). Shared so the CTE-shadowing rule cannot be applied
/// in one place and forgotten in the other.
fn collect_range_var(
    rv: &pg_query::protobuf::RangeVar,
    cte_scope: &HashSet<String>,
    out: &mut Vec<Vec<String>>,
) {
    // Only an *unqualified* name can be a CTE reference — Postgres never lets
    // a schema-qualified name (`public.x`) resolve to a CTE, so `cte_scope` is
    // not consulted when `schemaname` is set.
    if rv.schemaname.is_empty() && cte_scope.contains(&rv.relname.to_ascii_lowercase()) {
        return;
    }
    let mut parts = Vec::new();
    if !rv.schemaname.is_empty() {
        parts.push(rv.schemaname.clone());
    }
    parts.push(rv.relname.clone());
    out.push(parts);
}

fn collect_from_item(item: &Node, cte_scope: &HashSet<String>, out: &mut Vec<Vec<String>>) {
    match item.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => collect_range_var(rv, cte_scope, out),
        Some(NodeEnum::JoinExpr(je)) => {
            if let Some(l) = je.larg.as_deref() {
                collect_from_item(l, cte_scope, out);
            }
            if let Some(r) = je.rarg.as_deref() {
                collect_from_item(r, cte_scope, out);
            }
            // `ON` is an ordinary expression, and can hold a `SubLink`.
            collect_opt_expr(je.quals.as_deref(), cte_scope, out);
        }
        // A derived table — `FROM (SELECT ...) s`. Walked as of the commit
        // that added this arm, and NOT before: `basin-plan` used to refuse a
        // subquery in `FROM` outright, so prefetching its tables would have
        // converted a clean `Unsupported` into a possible `Ineligible` and
        // made the histogram worse. `lower/select.rs::build_range_subselect`
        // now lowers the body through the full `SELECT` surface, so the inner
        // `FROM` needs its tables prefetched like any other.
        //
        // `cte_scope` is threaded unchanged, for the same reason
        // `collect_expr` threads it into a `SubLink`: a derived table sees
        // exactly the CTE names its enclosing level sees, so
        // `FROM (SELECT ... FROM cte) s` keeps excluding `cte` rather than
        // sending it to the catalog as a table that does not exist.
        //
        // LATERAL is the same `RangeSubselect` node with `lateral` set; it is
        // walked identically, since a correlated reference resolves against
        // the outer scope and never names a new table.
        Some(NodeEnum::RangeSubselect(rs)) => {
            if let Some(NodeEnum::SelectStmt(inner)) =
                rs.subquery.as_deref().and_then(|n| n.node.as_ref())
            {
                collect_tables_stmt(inner, cte_scope, out);
            }
        }
        _ => {}
    }
}

/// Best-effort `arrow_schema::DataType -> basin_pgtype::PgType`, the inverse
/// of `basin_pgtype::physical`, which no direction of the workspace defines
/// (`basin_pgtype::physical` only goes `PgType -> DataType`; the catalog
/// stores only the Arrow-side schema). This is safe to be lossy: a plan's
/// per-column `PgType` is not consumed by `basin-plan`'s expression lowering
/// for a bare `Expr::Column` today — `lower::expr::best_effort_type` only
/// trusts a literal, cast, or parameter's own type (see that function's
/// docs: "Column types are not available in this increment") — so an
/// imprecise mapping here narrows nothing a real query depends on yet; it
/// only feeds `*`-expansion column naming and `EXPLAIN`-shaped tooling.
fn pgtype_of(field: &Field) -> PgType {
    match field.data_type() {
        DataType::Boolean => PgType::BOOL,
        DataType::Int16 | DataType::Int8 | DataType::UInt8 => PgType::INT2,
        DataType::Int32 | DataType::UInt16 => PgType::INT4,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => PgType::INT8,
        DataType::Float32 => PgType::FLOAT4,
        DataType::Float64 => PgType::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => PgType::TEXT,
        DataType::Binary | DataType::LargeBinary => PgType::BYTEA,
        DataType::Date32 | DataType::Date64 => PgType::DATE,
        DataType::Timestamp(_, Some(_)) => PgType::TIMESTAMPTZ,
        DataType::Timestamp(_, None) => PgType::TIMESTAMP,
        DataType::Decimal128(p, s) => PgType::numeric(*p as i32, *s as i32),
        _ => PgType::UNKNOWN,
    }
}

// ─── Operators / functions ─────────────────────────────────────────────

/// The real `pg_operator` table (`basin_pgtype::operator::resolve`), plus
/// the synthetic `AND`/`OR`/`NOT` sentinels `basin-exec::eval` and
/// `basin-plan::opt::pushdown` already agree on (see those modules' docs:
/// none of the three has a `pg_operator` row, because Postgres parses them
/// as a `BoolExpr`, not an `OpExpr`). All three are resolved: `eval_unary`
/// implements `NOT` now (see [`NOT_OP`]), so there is no wasted-build reason
/// left to leave it unresolved.
struct RealOperators;

/// Same sentinel values as `basin_exec::eval::{AND_OP, OR_OP, NOT_OP}` and
/// `basin_plan::opt::pushdown::AND_OP` — the largest real `pg_operator` oid
/// is in the low thousands, so `u32::MAX` / `u32::MAX - 1` / `u32::MAX - 2`
/// cannot alias one.
const AND_OP: OpId = OpId(Oid(u32::MAX));
const OR_OP: OpId = OpId(Oid(u32::MAX - 1));
/// `basin_exec::eval::eval_unary` has implemented `NOT` (`arrow`'s
/// `boolean::not`, which already gives `NOT NULL = NULL` for free) since
/// this resolver was first written — that comment describing `NOT` as
/// deliberately unresolved is stale; this is exactly the widening the task
/// that added it called out by name.
const NOT_OP: OpId = OpId(Oid(u32::MAX - 2));

impl OperatorResolver for RealOperators {
    fn resolve(&self, name: &str, left: Option<PgType>, right: PgType) -> Option<OpId> {
        match name {
            "AND" => Some(AND_OP),
            "OR" => Some(OR_OP),
            "NOT" => Some(NOT_OP),
            _ => {
                let left_oid = left.map(|t| t.oid);
                basin_pgtype::operator::resolve(name, left_oid, right.oid).map(|sig| OpId(sig.oid))
            }
        }
    }
}

/// Resolves against the real `pg_proc` table (`basin_pgtype::func::resolve`,
/// ~95 rows covering string/math/date-time/aggregate/window/set-returning
/// functions), with one deliberate, narrow exception: see the `count` /
/// `sum` / `avg` / `min` / `max` special case below.
///
/// Everything else — scalar functions (`lower`, `substr`, `abs`, `round`,
/// ...), `array_agg`/`string_agg`, window functions (`row_number`, `lag`,
/// ...) and set-returning functions (`generate_series`, `unnest`) — resolves
/// through the real catalog. Resolving one is not a promise that it runs
/// end to end: `basin-exec::eval`/`build.rs` only implement a subset (see
/// the module docs on why resolving the rest anyway is still safe), and
/// `basin-plan/src/lower/select.rs` still rejects any window function in a
/// target list outright (`contains_window`) regardless of what resolves —
/// but a `LowerError`/`BuildError`/`ExecError` past this point is exactly
/// the "any error falls back" contract the rest of this module already
/// relies on, now with an accurate reason attached (see
/// [`Fallback::reason_kind`]) instead of a blanket "no such function".
struct RealFunctions;

/// `count`/`sum`/`avg`/`min`/`max` stay pinned to one representative
/// `pg_proc` oid per name — the same behaviour this resolver had before it
/// was widened — rather than the argument-typed oid `basin_pgtype::func`
/// would pick. This is deliberate, not an oversight: `basin-exec/src/build.rs`'s
/// `agg_func_of` only recognises a handful of oids per name (int4/int8/
/// float8/numeric widths, roughly), missing real rows `basin_pgtype::func`
/// *does* have (`sum(int2)`, `min(text)`, `min(timestamptz)`, `avg(interval)`,
/// ...). `aggregate.rs`'s physical accumulators dispatch on the *actual*
/// input column's Arrow type at build time (see `AggFunc::Min`/`Max`/`Sum`/
/// `Avg` in `basin-exec/src/aggregate.rs`), not on the resolved oid beyond
/// picking which accumulator family to use — so one representative oid per
/// name is exactly as capable as the type-correct one would be, and strictly
/// more capable than `agg_func_of`'s own narrower oid list. Widening this to
/// the argument-typed oid would only ever *lose* coverage (e.g. `sum(int2)`
/// would resolve to a real oid `agg_func_of` doesn't recognise and fall back,
/// where today it is served). `count(x)` follows the same reasoning: its
/// real oid (2147, distinct from `count(*)`'s 2803 — see
/// `basin_pgtype::func`'s own module docs on why those must never collapse)
/// is not one `agg_func_of` recognises either, so this reports 2803 for
/// both and lets `agg_spec`'s own `args.first()` check recover the
/// `Count`/`CountStar` distinction, exactly as it already does.
const AGGREGATE_REPRESENTATIVE_OID: &[(&str, u32)] = &[
    ("count", 2803),
    ("sum", 2108),
    ("avg", 2101),
    ("min", 2132),
    ("max", 2116),
];

impl basin_plan::lower::expr::FunctionResolver for RealFunctions {
    fn resolve(&self, name: &[String], args: &[PgType]) -> Option<(FuncId, FuncKind)> {
        let name = name.last()?.as_str();

        if let Some((_, oid)) = AGGREGATE_REPRESENTATIVE_OID
            .iter()
            .find(|(n, _)| *n == name)
        {
            return Some((FuncId(Oid(*oid)), FuncKind::Aggregate));
        }

        let arg_oids: Vec<basin_pgtype::Oid> = args.iter().map(|t| t.oid).collect();
        let sig = basin_pgtype::func::resolve(name, &arg_oids)?;
        Some((FuncId(sig.oid), map_func_kind(sig.kind)))
    }
}

/// `basin_pgtype::func::FuncKind` -> `basin_plan::lower::expr::FuncKind`:
/// the same four cases, defined in two crates because `basin-pgtype` (the
/// catalog) and `basin-plan` (the resolver seam it plugs into) don't depend
/// on each other — see those crates' own docs on the trait-seam pattern this
/// mirrors for [`OperatorResolver`].
fn map_func_kind(kind: basin_pgtype::func::FuncKind) -> FuncKind {
    match kind {
        basin_pgtype::func::FuncKind::Scalar => FuncKind::Scalar,
        basin_pgtype::func::FuncKind::Aggregate => FuncKind::Aggregate,
        basin_pgtype::func::FuncKind::Window => FuncKind::Window,
        basin_pgtype::func::FuncKind::SetReturning => FuncKind::SetReturning,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_plan::lower::expr::FunctionResolver;
    use basin_plan::LogicalPlan;

    // ── Fallback::reason_kind ────────────────────────────────────────────

    #[test]
    fn unsupported_bucket_covers_both_lower_and_build_unsupported_and_no_matching_operator() {
        assert_eq!(
            Fallback::Lower(LowerError::Unsupported("x".into())).reason_kind(),
            FallbackReasonKind::Unsupported
        );
        assert_eq!(
            Fallback::Lower(LowerError::NoMatchingOperator("x".into())).reason_kind(),
            FallbackReasonKind::Unsupported,
            "a resolver miss is exactly as actionable as an explicit Unsupported \
             — see reason_kind's doc comment"
        );
        assert_eq!(
            Fallback::Build(BuildError::Unsupported("x".into())).reason_kind(),
            FallbackReasonKind::Unsupported
        );
    }

    #[test]
    fn every_other_fallback_variant_lands_in_its_own_bucket() {
        assert_eq!(
            Fallback::Ineligible("x").reason_kind(),
            FallbackReasonKind::Ineligible
        );
        assert_eq!(
            Fallback::Lower(LowerError::UnknownName("x".into())).reason_kind(),
            FallbackReasonKind::LoweringError
        );
        assert_eq!(
            Fallback::Lower(LowerError::Malformed("x")).reason_kind(),
            FallbackReasonKind::LoweringError
        );
        assert_eq!(
            Fallback::Build(BuildError::UnknownTable(TableId(1))).reason_kind(),
            FallbackReasonKind::BuildError
        );
        assert_eq!(
            Fallback::Build(BuildError::NonColumnKey("x")).reason_kind(),
            FallbackReasonKind::BuildError
        );
        assert_eq!(
            Fallback::Build(BuildError::Exec(ExecError::Cancelled)).reason_kind(),
            FallbackReasonKind::ExecError,
            "a BuildError wrapping an ExecError must file under ExecError, not BuildError"
        );
        assert_eq!(
            Fallback::Exec(ExecError::Cancelled).reason_kind(),
            FallbackReasonKind::ExecError
        );
    }

    // ── FallbackReasonCounters ───────────────────────────────────────────

    #[test]
    fn counters_snapshot_matches_what_was_recorded_and_totals_correctly() {
        let counters = FallbackReasonCounters::new();
        counters.record(FallbackReasonKind::Ineligible);
        counters.record(FallbackReasonKind::Unsupported);
        counters.record(FallbackReasonKind::Unsupported);
        counters.record(FallbackReasonKind::LoweringError);
        counters.record(FallbackReasonKind::BuildError);
        counters.record(FallbackReasonKind::ExecError);

        let snap = counters.snapshot();
        assert_eq!(snap.ineligible, 1);
        assert_eq!(snap.unsupported, 2);
        assert_eq!(snap.lowering_error, 1);
        assert_eq!(snap.build_error, 1);
        assert_eq!(snap.exec_error, 1);
        assert_eq!(
            snap.total(),
            6,
            "total() must equal the number of record() calls"
        );
    }

    #[test]
    fn fresh_counters_snapshot_to_all_zero() {
        let snap = FallbackReasonCounters::new().snapshot();
        assert_eq!(snap, FallbackReasonCountersSnapshot::default());
        assert_eq!(snap.total(), 0);
    }

    // ── RealOperators ─────────────────────────────────────────────────────

    #[test]
    fn not_resolves_to_its_own_sentinel_distinct_from_and_and_or() {
        let resolved = RealOperators.resolve("NOT", None, PgType::BOOL);
        assert_eq!(
            resolved,
            Some(NOT_OP),
            "NOT must resolve now — eval_unary implements it"
        );
        assert_ne!(NOT_OP, AND_OP);
        assert_ne!(NOT_OP, OR_OP);
    }

    #[test]
    fn and_or_still_resolve_to_their_original_sentinels() {
        assert_eq!(
            RealOperators.resolve("AND", None, PgType::BOOL),
            Some(AND_OP)
        );
        assert_eq!(RealOperators.resolve("OR", None, PgType::BOOL), Some(OR_OP));
    }

    #[test]
    fn a_real_pg_operator_still_resolves_through_the_catalog() {
        // `>` over two int4s — exercises the fallthrough to
        // `basin_pgtype::operator::resolve`, unchanged by this widening.
        let resolved = RealOperators.resolve(">", Some(PgType::INT4), PgType::INT4);
        assert!(resolved.is_some());
        assert_ne!(resolved, Some(AND_OP));
        assert_ne!(resolved, Some(NOT_OP));
    }

    // ── RealFunctions ────────────────────────────────────────────────────

    #[test]
    fn scalar_functions_beyond_the_original_five_aggregates_now_resolve() {
        let (id, kind) = RealFunctions
            .resolve(&["lower".to_string()], &[PgType::TEXT])
            .expect("lower(text) must resolve against the real pg_proc table");
        assert_eq!(kind, FuncKind::Scalar);
        assert_eq!(id, FuncId(Oid(870)));

        let (id, kind) = RealFunctions
            .resolve(&["abs".to_string()], &[PgType::INT4])
            .expect("abs(int4) must resolve");
        assert_eq!(kind, FuncKind::Scalar);
        assert_eq!(id, FuncId(Oid(1397)));
    }

    #[test]
    fn window_and_set_returning_functions_now_resolve_with_the_right_kind() {
        let (_, kind) = RealFunctions
            .resolve(&["row_number".to_string()], &[])
            .expect("row_number() must resolve");
        assert_eq!(kind, FuncKind::Window);

        let (_, kind) = RealFunctions
            .resolve(
                &["generate_series".to_string()],
                &[PgType::INT4, PgType::INT4],
            )
            .expect("generate_series(int4, int4) must resolve");
        assert_eq!(kind, FuncKind::SetReturning);
    }

    #[test]
    fn unknown_function_name_still_fails_to_resolve() {
        assert!(RealFunctions
            .resolve(&["frobnicate".to_string()], &[PgType::TEXT])
            .is_none());
    }

    #[test]
    fn count_star_and_count_of_a_column_resolve_to_the_same_representative_oid() {
        // Real `pg_proc` gives these two different oids (2803 vs 2147) — see
        // `basin_pgtype::func`'s own module docs on why they must never
        // collapse there. This resolver deliberately does collapse them, to
        // match what `basin-exec::build::agg_func_of` actually recognises —
        // see `AGGREGATE_REPRESENTATIVE_OID`'s doc comment.
        let (star, _) = RealFunctions.resolve(&["count".to_string()], &[]).unwrap();
        let (of_col, _) = RealFunctions
            .resolve(&["count".to_string()], &[PgType::INT4])
            .unwrap();
        assert_eq!(star, FuncId(Oid(2803)));
        assert_eq!(of_col, FuncId(Oid(2803)));
    }

    #[test]
    fn min_max_sum_avg_resolve_regardless_of_argument_type() {
        // `min(text)`/`min(timestamptz)` etc. have no row in
        // `basin_pgtype::func`'s table that `agg_func_of` recognises — the
        // representative-oid pin is what keeps these servable at all.
        for (name, oid, ty) in [
            ("min", 2132u32, PgType::TEXT),
            ("max", 2116, PgType::TIMESTAMPTZ),
            ("sum", 2108, PgType::INT2),
            ("avg", 2101, PgType::new(basin_pgtype::oid::INTERVAL)),
        ] {
            let (id, kind) = RealFunctions
                .resolve(&[name.to_string()], &[ty])
                .unwrap_or_else(|| panic!("{name} must resolve regardless of argument type"));
            assert_eq!(
                id,
                FuncId(Oid(oid)),
                "{name} must stay pinned to its representative oid"
            );
            assert_eq!(kind, FuncKind::Aggregate);
        }
    }

    // ── optimize_default is actually wired between lowering and build ──────
    //
    // These exercise exactly `try_execute_inner`'s own lowering call
    // (`lower_select` with this bridge's own `RealOperators`/`RealFunctions`)
    // followed by `basin_plan::opt::optimize_default` — the same two calls
    // in the same order — without needing a real catalog/storage session,
    // since neither depends on one. A test that only checked the returned
    // rows would pass whether or not `optimize_default` ever ran (the rules
    // are answer-preserving by design); these instead pin the *shape* of the
    // plan the optimizer hands to `basin_exec::build::build`.

    /// A table resolver over an in-memory schema, standing in for
    /// [`CatalogTableResolver`] — that type needs a real `basin_storage::Storage`
    /// it has no use for here, since these tests never reach `basin-exec`.
    struct PlanOnlyTables(HashMap<String, (TableId, PlanSchema)>);

    impl PlanTableResolver for PlanOnlyTables {
        fn resolve_table(&self, name: &[String]) -> Option<(TableId, PlanSchema)> {
            let last = name.last()?;
            self.0.get(last).cloned()
        }
    }

    fn lower_over_t(sql: &str) -> basin_plan::LogicalPlan {
        let schema: PlanSchema = [
            ("id".to_string(), PgType::INT8),
            ("name".to_string(), PgType::TEXT),
            ("extra".to_string(), PgType::TEXT),
        ]
        .into_iter()
        .collect();
        let mut tables = HashMap::new();
        tables.insert("t".to_string(), (TableId(1), schema));
        let resolver = PlanOnlyTables(tables);

        let result = pg_query::parse(sql).expect("parse failed");
        let raw = result.protobuf.stmts.first().expect("no stmt").clone();
        let node = *raw.stmt.expect("no stmt node");
        lower_select(&node, &resolver, &RealOperators, &RealFunctions).expect("lower failed")
    }

    /// Before optimization, lowering always scans every column of `t` (see
    /// `select.rs`'s `build_range_var`) and leaves the `WHERE` predicate
    /// sitting in its own `Filter` node above the `Scan` — `Scan::filters`
    /// starts empty no matter what the query's `WHERE` clause says.
    #[test]
    fn lowering_alone_leaves_a_full_projection_and_an_unpushed_filter() {
        let plan = lower_over_t("SELECT id FROM t WHERE id > 2");
        let LogicalPlan::Project { input, .. } = &plan else {
            panic!("expected Project at the top, got {plan:?}");
        };
        let LogicalPlan::Filter { input, .. } = input.as_ref() else {
            panic!("expected an un-pushed Filter under Project, got {input:?}");
        };
        let LogicalPlan::Scan {
            projection,
            filters,
            ..
        } = input.as_ref()
        else {
            panic!("expected Scan under Filter");
        };
        assert_eq!(
            projection.len(),
            3,
            "lowering scans every column of t (id, name, extra) before optimizing"
        );
        assert!(
            filters.is_empty(),
            "lowering never puts a WHERE predicate into Scan::filters itself"
        );
    }

    /// The behavior this whole task is about: `optimize_default`, called
    /// exactly the way `try_execute_inner` now calls it, must (a) push `id >
    /// 2` all the way into `Scan::filters` — eliminating the `Filter` node
    /// entirely — and (b) prune `Scan::projection` down to just `id`, the
    /// only column either the output or the predicate references, dropping
    /// `name` and `extra`. Before this bridge called `optimize_default`
    /// (previously it called nothing, then a hand-picked 2-rule subset),
    /// every scan through this path read all 3 columns and filtered
    /// Arrow-side after decode; this test would fail against that plan.
    #[test]
    fn optimize_default_prunes_the_projection_and_pushes_the_filter_into_the_scan() {
        let plan = lower_over_t("SELECT id FROM t WHERE id > 2");
        let (optimized, passes) = basin_plan::opt::optimize_default(plan);

        assert!(
            passes > 0,
            "this plan has a filter to push and two unused columns to prune — \
             the pipeline must have made at least one productive pass"
        );

        let LogicalPlan::Project { input, exprs } = &optimized else {
            panic!("expected Project to survive at the top, got {optimized:?}");
        };
        assert_eq!(exprs.len(), 1, "the output list is still just `id`");
        let LogicalPlan::Scan {
            projection,
            filters,
            ..
        } = input.as_ref()
        else {
            panic!(
                "expected a bare Scan directly under Project — filter pushdown should have \
                 eaten the Filter node entirely, got {input:?}"
            );
        };
        assert_eq!(
            projection.len(),
            1,
            "projection pruning must narrow the scan to just `id`, got {projection:?}"
        );
        assert_eq!(
            filters.len(),
            1,
            "the `id > 2` predicate must have been pushed into the scan, got {filters:?}"
        );
    }

    /// The inverse control: a query with nothing to prune or push (every
    /// column used, no predicate at all) must converge in zero passes. This
    /// is what the task's counters would show as "the rules are firing on
    /// nothing" if it happened on real queries that *do* have work to do —
    /// pinning it here on a query that genuinely has none is what makes the
    /// non-zero pass count above meaningful rather than a driver artifact.
    #[test]
    fn optimize_default_converges_in_zero_passes_on_an_already_minimal_query() {
        let plan = lower_over_t("SELECT id, name, extra FROM t");
        let (_optimized, passes) = basin_plan::opt::optimize_default(plan);
        assert_eq!(
            passes, 0,
            "a full-projection, no-filter query has nothing for any rule to do"
        );
    }

    // ── collect_tables / WITH clauses ───────────────────────────────────

    fn collected(sql: &str) -> Vec<String> {
        let result = pg_query::parse(sql).expect("parse failed");
        let raw = result.protobuf.stmts.first().expect("no stmt").clone();
        let node = *raw.stmt.expect("no stmt node");
        let mut out = Vec::new();
        collect_tables(&node, &mut out);
        out.into_iter().map(|parts| parts.join(".")).collect()
    }

    /// DML collects its TARGET relation, which a `SELECT`-only walk never
    /// looked at. Until this existed the resolver was handed an empty table
    /// list for every INSERT/UPDATE/DELETE, so the bridge could not have
    /// served one even with the executor gate open.
    #[test]
    fn dml_collects_its_target_relation() {
        assert_eq!(collected("INSERT INTO t VALUES (1)"), vec!["t".to_string()]);
        assert_eq!(
            collected("UPDATE t SET a = 1 WHERE id = 2"),
            vec!["t".to_string()]
        );
        assert_eq!(
            collected("DELETE FROM t WHERE id = 2"),
            vec!["t".to_string()]
        );
    }

    /// DML reads as well as writes, and every relation it reads needs
    /// prefetching too — the source of an `INSERT ... SELECT`, the extra
    /// relations of `UPDATE ... FROM` and `DELETE ... USING`. Collecting only
    /// the target would leave the read side unresolvable.
    #[test]
    fn dml_collects_the_relations_it_reads_as_well_as_its_target() {
        assert_eq!(
            collected("INSERT INTO t SELECT id FROM u"),
            vec!["t".to_string(), "u".to_string()]
        );
        assert_eq!(
            collected("UPDATE t SET a = u.n FROM u WHERE u.tid = t.id"),
            vec!["t".to_string(), "u".to_string()]
        );
        assert_eq!(
            collected("DELETE FROM t USING u WHERE u.tid = t.id"),
            vec!["t".to_string(), "u".to_string()]
        );
    }

    /// A CTE name shadows a table for DML exactly as it does for `SELECT`.
    /// The scoping rule lives in one shared helper precisely so it cannot be
    /// applied on one path and forgotten on the other.
    #[test]
    fn a_dml_source_referencing_a_cte_does_not_collect_the_cte_name() {
        assert_eq!(
            collected("WITH x AS (SELECT id FROM u) INSERT INTO t SELECT id FROM x"),
            vec!["u".to_string(), "t".to_string()]
        );
    }

    #[test]
    fn cte_body_tables_are_collected() {
        let names = collected("WITH x AS (SELECT id FROM t) SELECT id FROM x");
        assert_eq!(
            names,
            vec!["t".to_string()],
            "the table referenced inside the CTE body must be prefetched"
        );
    }

    #[test]
    fn a_bare_reference_to_the_ctes_own_name_is_not_collected_as_a_table() {
        let names = collected("WITH x AS (SELECT id FROM t) SELECT id FROM x");
        assert!(
            !names.iter().any(|n| n == "x"),
            "`FROM x` in the outer query names the CTE, not a catalog table — \
             collecting it would send build_resolver looking for a table that \
             does not exist, got {names:?}"
        );
    }

    #[test]
    fn nested_with_inside_a_cte_body_is_walked() {
        let names =
            collected("WITH a AS (WITH b AS (SELECT id FROM t) SELECT id FROM b) SELECT id FROM a");
        assert_eq!(
            names,
            vec!["t".to_string()],
            "the innermost table must surface, and neither CTE name (a or b) should, \
             got {names:?}"
        );
    }

    #[test]
    fn a_cte_referencing_an_earlier_cte_does_not_collect_that_earlier_name() {
        let names =
            collected("WITH a AS (SELECT id FROM t), b AS (SELECT id FROM a) SELECT id FROM b");
        assert_eq!(
            names,
            vec!["t".to_string()],
            "`b`'s body references `a`, an earlier CTE in the same WITH list, not a \
             table — only `t` (from `a`'s own body) should be collected, got {names:?}"
        );
    }

    #[test]
    fn a_recursive_cte_referencing_itself_does_not_collect_its_own_name() {
        let names = collected(
            "WITH RECURSIVE r AS (SELECT id FROM t UNION ALL SELECT id FROM r) \
             SELECT id FROM r",
        );
        assert_eq!(
            names,
            vec!["t".to_string()],
            "the recursive self-reference `FROM r` inside r's own body must be \
             excluded, not sent to the catalog as a nonexistent table, got {names:?}"
        );
    }

    // ── collect_tables / SubLink subqueries ──────────────────────────────

    /// The shapes that used to die at `LowerError::UnknownName` in this
    /// bridge: each names `u` only inside a subquery, so before `collect_expr`
    /// existed the resolver was never told to load it. `EXISTS (SELECT 1 FROM
    /// t u ...)` served all along because aliasing `t` as `u` names no new
    /// table — which is exactly why that one shape hid the gap.
    #[test]
    fn a_table_named_only_inside_a_sublink_is_collected() {
        for sql in [
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id AND u.n > 7)",
            "SELECT id FROM t WHERE id IN (SELECT tid FROM u)",
            "SELECT id FROM t WHERE id NOT IN (SELECT tid FROM u WHERE tid IS NOT NULL)",
            "SELECT id, (SELECT count(*) FROM u WHERE u.tid = t.id) FROM t",
            "SELECT id FROM t WHERE id = ANY (SELECT tid FROM u)",
            "SELECT a.id FROM t a JOIN t b ON a.id = b.id AND a.id IN (SELECT tid FROM u)",
            "SELECT count(*) FROM t GROUP BY name HAVING count(*) > (SELECT count(*) FROM u)",
            "SELECT id FROM t ORDER BY (SELECT count(*) FROM u WHERE u.tid = t.id)",
        ] {
            let names = collected(sql);
            assert!(
                names.iter().any(|n| n == "u"),
                "`u` is named only inside the subquery of `{sql}`, and must still be \
                 prefetched, got {names:?}"
            );
        }
    }

    /// The same `SubLink` walk, in a DML statement's own clauses. `UPDATE t
    /// SET name = upper(name) WHERE id IN (SELECT tid FROM u)` is the shape
    /// the fallback histogram's DML section found: the `UpdateStmt` arm
    /// walked `WITH`, the target relation and `FROM`, but never the `WHERE`,
    /// so `u` was never prefetched and lowering died on `UnknownName("u")` —
    /// a genuine gap, not the executor's write gate, and the only one of the
    /// 15 DML shapes that failed before reaching `build`.
    #[test]
    fn a_table_named_only_inside_a_dml_clause_subquery_is_collected() {
        for sql in [
            "UPDATE t SET name = upper(name) WHERE id IN (SELECT tid FROM u)",
            "UPDATE t SET name = (SELECT tag FROM u WHERE u.tid = t.id) WHERE id = 1",
            "UPDATE t SET name = 'x' WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
            "UPDATE t SET name = 'x' RETURNING (SELECT count(*) FROM u)",
            "DELETE FROM t WHERE id IN (SELECT tid FROM u)",
            "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)",
            "DELETE FROM t WHERE id = 1 RETURNING (SELECT count(*) FROM u)",
            "INSERT INTO t VALUES (1) RETURNING (SELECT count(*) FROM u)",
        ] {
            let names = collected(sql);
            assert!(
                names.iter().any(|n| n == "u"),
                "`u` is named only inside a subquery of `{sql}`, and must still be \
                 prefetched, got {names:?}"
            );
            assert!(
                names.iter().any(|n| n == "t"),
                "the DML target `t` must still be collected for `{sql}`, got {names:?}"
            );
        }
    }

    /// The CTE-scope trap, in the DML clauses this walk just learned to
    /// enter. A `WITH` name referenced from an `UPDATE`'s `WHERE` subquery
    /// must not be sent to the catalog — same rule the `SELECT` arm already
    /// obeys, and the reason `scope` is threaded rather than a fresh set.
    #[test]
    fn a_dml_clause_subquery_reading_a_cte_does_not_collect_the_cte_name() {
        let names = collected(
            "WITH c AS (SELECT tid FROM u) UPDATE t SET a = 1 WHERE id IN (SELECT tid FROM c)",
        );
        assert!(
            names.iter().all(|n| n != "c"),
            "`c` is the CTE, not a catalog table, got {names:?}"
        );
        assert!(
            names.iter().any(|n| n == "u") && names.iter().any(|n| n == "t"),
            "the CTE body's `u` and the target `t` are both real tables, got {names:?}"
        );
    }

    /// The trap this walk had to avoid: threading the enclosing statement's
    /// CTE scope into the subquery. A `SubLink` body reading a CTE must not
    /// send that name to the catalog — doing so would turn a query lowering
    /// can serve into `Ineligible("table not found in the catalog")`, strictly
    /// worse than never walking the subquery at all.
    #[test]
    fn a_sublink_reading_a_cte_does_not_collect_the_cte_name() {
        let names = collected(
            "WITH c AS (SELECT tid FROM u) SELECT id FROM t WHERE id IN (SELECT tid FROM c)",
        );
        assert_eq!(
            names,
            vec!["u".to_string(), "t".to_string()],
            "`c` inside the IN-subquery is the CTE, not a catalog table, got {names:?}"
        );
    }

    /// A subquery in `FROM` stays unwalked on purpose — `lower/select.rs`
    /// rejects that shape as `Unsupported` whatever this collects, and
    /// collecting its tables could only turn that clean verdict into an
    /// `Ineligible` one.
    ///
    /// THAT IS NO LONGER TRUE. `lower/select.rs::build_range_subselect` now
    /// lowers a derived table's body through the full `SELECT` surface, so
    /// leaving it unwalked strands the inner `FROM` at `UnknownName` — the
    /// refusal became a capability, and the omission that used to be a
    /// decision became a bug. These replace the test that pinned it.
    #[test]
    fn a_subquery_in_from_is_walked() {
        let names = collected("SELECT s.tid FROM (SELECT tid FROM u) s");
        assert_eq!(
            names,
            vec!["u".to_string()],
            "a derived table's inner FROM must be prefetched, got {names:?}"
        );
    }

    #[test]
    fn a_lateral_subquery_in_from_is_walked() {
        let names = collected(
            "SELECT t.id FROM t, LATERAL (SELECT tid FROM u WHERE u.tid = t.id) s",
        );
        assert!(
            names.contains(&"u".to_string()) && names.contains(&"t".to_string()),
            "LATERAL is the same RangeSubselect shape and is walked identically, got {names:?}"
        );
    }

    /// The CTE trap, in the derived-table position rather than the `SubLink`
    /// one: `cte_scope` must be threaded through `RangeSubselect` too, or a
    /// derived table reading a CTE sends that name to the catalog and turns a
    /// servable query into `Ineligible` — the exact failure the old omission
    /// was protecting against, now handled rather than avoided.
    #[test]
    fn a_derived_table_reading_a_cte_does_not_collect_the_cte_name() {
        let names =
            collected("WITH c AS (SELECT tid FROM u) SELECT s.tid FROM (SELECT tid FROM c) s");
        assert_eq!(
            names,
            vec!["u".to_string()],
            "`c` inside the derived table is the CTE, not a catalog table, got {names:?}"
        );
    }

    // ── Shadow-compare ──────────────────────────────────────────────────
    //
    // The end-to-end behaviour (both flags on, real tables, real DataFusion)
    // lives in `tests/shadow_compare.rs`. What is here is what that test
    // cannot reach: the structural DML guard, exercised against parse nodes
    // the executor's SELECT-only call site never hands this module today,
    // and the comparison primitives, exercised against hand-built results
    // whose divergence is known by construction.

    use arrow_array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray};
    use arrow_schema::Schema as ArrowSchema;
    use std::sync::Arc;

    fn stmt_node_of(sql: &str) -> Node {
        let result = pg_query::parse(sql).expect("parse failed");
        let raw = result.protobuf.stmts.first().expect("no stmt").clone();
        *raw.stmt.expect("no stmt node")
    }

    fn select_of(sql: &str) -> SelectStmt {
        match stmt_node_of(sql).node {
            Some(NodeEnum::SelectStmt(select)) => *select,
            other => panic!("{sql:?} did not parse to a SelectStmt: {other:?}"),
        }
    }

    /// A one-batch `ExecResult::Rows` over the given columns.
    fn result_of(columns: Vec<ArrayRef>) -> ExecResult {
        let fields: Vec<Field> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));
        let batches = vec![RecordBatch::try_new(schema.clone(), columns).expect("batch")];
        ExecResult::Rows { schema, batches }
    }

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn floats(values: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn strings(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    /// `NUMERIC(20, 12)` — twelve fractional digits, so a difference of
    /// `1e-12` is representable and the "decimals are compared exactly"
    /// claim can be tested against a difference the float tolerance would
    /// otherwise swallow.
    fn numerics(units: &[i128]) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(units.to_vec())
                .with_precision_and_scale(20, 12)
                .expect("precision/scale"),
        )
    }

    /// Decision 3, structurally. This is the guard that stops a future
    /// DML-carrying call site from double-writing; it must reject on the
    /// node kind itself, not on a convention the call site is trusted to
    /// keep.
    #[test]
    fn shadow_target_accepts_only_select_nodes() {
        assert!(
            shadow_target(&stmt_node_of("SELECT id FROM t")).is_some(),
            "a plain SELECT is the one thing shadow-compare may re-run"
        );
        assert!(
            shadow_target(&stmt_node_of(
                "SELECT id FROM t UNION SELECT tid FROM u ORDER BY 1"
            ))
            .is_some(),
            "a set operation is still a SelectStmt and still side-effect free"
        );
        for dml in [
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t SELECT id FROM u",
            "UPDATE t SET id = 2",
            "DELETE FROM t WHERE id = 1",
            "INSERT INTO t VALUES (1) RETURNING id",
            "CREATE TABLE z (id BIGINT)",
            "WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM x",
        ] {
            assert!(
                shadow_target(&stmt_node_of(dml)).is_none(),
                "{dml:?} must never be re-executed by shadow-compare — that is a double-write"
            );
        }
    }

    /// The last case above is the one that makes the outer-node-kind check
    /// insufficient, so pin the fact directly rather than leaving it implicit
    /// in the loop: `pg_query` roots a data-modifying CTE at a `SelectStmt`.
    /// A guard that stopped at the outermost node kind would re-run that
    /// INSERT on every execution.
    #[test]
    fn a_data_modifying_cte_still_roots_at_a_select_node() {
        let node = stmt_node_of("WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM x");
        assert!(
            matches!(node.node.as_ref(), Some(NodeEnum::SelectStmt(_))),
            "if this ever stops being a SelectStmt the is_side_effect_free walk \
             is still correct, but the reason it exists has changed — got {:?}",
            node.node
        );
        assert!(
            shadow_target(&node).is_none(),
            "and the guard must reject it anyway"
        );
    }

    #[test]
    fn side_effect_free_accepts_read_only_ctes_and_set_op_arms() {
        for read_only in [
            "WITH x AS (SELECT id FROM t) SELECT * FROM x",
            "WITH a AS (SELECT id FROM t), b AS (SELECT id FROM a) SELECT * FROM b",
            "WITH a AS (WITH b AS (SELECT id FROM t) SELECT * FROM b) SELECT * FROM a",
            "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT n FROM r",
            "(WITH a AS (SELECT id FROM t) SELECT * FROM a) UNION (SELECT tid FROM u)",
        ] {
            assert!(
                shadow_target(&stmt_node_of(read_only)).is_some(),
                "{read_only:?} writes nothing — refusing it would silently \
                 shrink the oracle's coverage"
            );
        }
        assert!(
            shadow_target(&stmt_node_of(
                "(WITH a AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM a) \
                 UNION (SELECT tid FROM u)"
            ))
            .is_none(),
            "a writable CTE hiding in a set-operation arm must be caught too"
        );
    }

    #[test]
    fn float_eq_admits_absolute_and_relative_slack_and_nothing_more() {
        // Absolute limb: tiny values, difference under 1e-9.
        assert!(float_eq(0.0, 5e-10));
        assert!(!float_eq(0.0, 5e-9), "5e-9 is past both limbs at this scale");
        // Relative limb: large values, difference far past 1e-9 absolute but
        // well under 1e-9 relative.
        assert!(float_eq(1.0e12, 1.0e12 + 1.0e-3));
        assert!(
            !float_eq(1.0e12, 1.0e12 + 1.0e4),
            "1e4 out of 1e12 is 1e-8 relative — past the tolerance"
        );
        // The classic accumulation-order artefact this tolerance exists for.
        assert!(float_eq(0.1 + 0.2, 0.3));
        // And a difference that is simply wrong.
        assert!(!float_eq(1.5, 2.5));
    }

    #[test]
    fn float_eq_treats_nan_as_agreement_and_keeps_infinity_signed() {
        assert!(
            float_eq(f64::NAN, f64::NAN),
            "both engines answering NaN is agreement, whatever IEEE-754 says"
        );
        assert!(!float_eq(f64::NAN, 1.0));
        assert!(!float_eq(1.0, f64::NAN));
        assert!(float_eq(f64::INFINITY, f64::INFINITY));
        assert!(float_eq(f64::NEG_INFINITY, f64::NEG_INFINITY));
        assert!(
            !float_eq(f64::INFINITY, f64::NEG_INFINITY),
            "opposite infinities differ; `inf - inf` is NaN so the subtraction \
             below must never be the thing deciding this"
        );
        assert!(!float_eq(f64::INFINITY, 1.0e308));
    }

    #[test]
    fn cell_eq_is_exact_off_the_float_path() {
        assert!(cell_eq(&Cell::Null, &Cell::Null));
        assert!(!cell_eq(&Cell::Null, &Cell::Text("".into())));
        assert!(!cell_eq(&Cell::Null, &Cell::Float(0.0)));
        assert!(cell_eq(&Cell::Text("a".into()), &Cell::Text("a".into())));
        assert!(!cell_eq(&Cell::Text("a".into()), &Cell::Text("A".into())));
        // A float on one side and its text rendering on the other is a type
        // difference, not a value one.
        assert!(cell_eq(&Cell::Float(1.5), &Cell::Text("1.5".into())));
        assert!(!cell_eq(&Cell::Float(1.5), &Cell::Text("beta".into())));
    }

    /// Decision 1, the false-positive half: a no-`ORDER BY` statement whose
    /// two engines emit the same rows in different order must NOT be
    /// reported.
    #[test]
    fn without_order_by_row_order_is_not_a_divergence() {
        let select = select_of("SELECT id, name FROM t");
        let owned = result_of(vec![ints(&[1, 2, 3]), strings(&["a", "b", "c"])]);
        let reference = result_of(vec![ints(&[3, 1, 2]), strings(&["c", "a", "b"])]);
        assert_eq!(compare_results(&select, &owned, &reference), None);
    }

    /// Decision 1, the true-positive half: the same shuffle under an
    /// `ORDER BY` IS a divergence, because the statement asked for an order
    /// and the two engines produced different ones.
    #[test]
    fn with_order_by_row_order_is_compared_positionally() {
        let select = select_of("SELECT id, name FROM t ORDER BY id");
        let owned = result_of(vec![ints(&[1, 2, 3]), strings(&["a", "b", "c"])]);
        let reference = result_of(vec![ints(&[3, 1, 2]), strings(&["c", "a", "b"])]);
        let detail = compare_results(&select, &owned, &reference)
            .expect("a differing order under ORDER BY must be reported");
        assert!(
            detail.contains("row 0 column 0") && detail.contains("positional"),
            "got {detail:?}"
        );
    }

    /// The canonical sort must not paper over a genuine value difference:
    /// same multiset size, no ORDER BY, but one cell really is different.
    #[test]
    fn a_genuine_value_difference_survives_the_canonical_sort() {
        let select = select_of("SELECT id FROM t");
        let owned = result_of(vec![ints(&[1, 2, 3])]);
        let reference = result_of(vec![ints(&[1, 2, 4])]);
        let detail = compare_results(&select, &owned, &reference)
            .expect("3 vs 4 is a real divergence, sorted or not");
        assert!(detail.contains("row 2 column 0"), "got {detail:?}");
    }

    #[test]
    fn shape_mismatches_are_reported_before_any_cell_is_read() {
        let select = select_of("SELECT id FROM t");
        let two = result_of(vec![ints(&[1, 2])]);
        let three = result_of(vec![ints(&[1, 2, 3])]);
        assert!(compare_results(&select, &two, &three)
            .expect("row counts differ")
            .contains("row count differs"));

        let wide = result_of(vec![ints(&[1, 2]), strings(&["a", "b"])]);
        assert!(compare_results(&select, &two, &wide)
            .expect("column counts differ")
            .contains("column count differs"));

        let empty = ExecResult::Empty { tag: "SELECT".into() };
        assert!(compare_results(&select, &two, &empty).is_some());
        assert!(compare_results(&select, &empty, &two).is_some());
        assert_eq!(compare_results(&select, &empty, &empty), None);
    }

    /// Decision 2: floats inside a real result get the tolerance...
    #[test]
    fn a_float_difference_within_tolerance_is_not_a_divergence() {
        let select = select_of("SELECT amt FROM t ORDER BY amt");
        let owned = result_of(vec![floats(&[0.1 + 0.2, 1.0e12])]);
        let reference = result_of(vec![floats(&[0.3, 1.0e12 + 1.0e-3])]);
        assert_eq!(
            compare_results(&select, &owned, &reference),
            None,
            "both cells differ only at the ULP/relative-1e-9 level"
        );
    }

    /// ...and a float difference past it is still reported.
    #[test]
    fn a_float_difference_outside_tolerance_is_a_divergence() {
        let select = select_of("SELECT amt FROM t ORDER BY amt");
        let owned = result_of(vec![floats(&[1.5])]);
        let reference = result_of(vec![floats(&[1.5000001])]);
        let detail = compare_results(&select, &owned, &reference)
            .expect("1e-7 relative is well past the 1e-9 tolerance");
        assert!(detail.contains("row 0 column 0"), "got {detail:?}");
    }

    /// Decision 2's other half: NUMERIC gets no tolerance at all. The
    /// difference here (1e-12 out of 1.0) is far inside the float tolerance,
    /// so this test fails the moment decimals start being compared as
    /// floats.
    #[test]
    fn numeric_is_compared_exactly_not_within_the_float_tolerance() {
        let select = select_of("SELECT n FROM e ORDER BY n");
        let owned = result_of(vec![numerics(&[1_000_000_000_000])]);
        let reference = result_of(vec![numerics(&[1_000_000_000_001])]);
        let detail = compare_results(&select, &owned, &reference)
            .expect("decimal arithmetic has no summation-order slack to forgive");
        assert!(
            detail.contains("1.000000000000") && detail.contains("1.000000000001"),
            "the message must show the exact rendered decimals, got {detail:?}"
        );
    }

    #[test]
    fn nulls_and_empty_strings_do_not_collide_in_the_sort_key() {
        assert_ne!(
            row_sort_key(&[Cell::Null]),
            row_sort_key(&[Cell::Text(String::new())])
        );
        assert_ne!(
            row_sort_key(&[Cell::Text("a".into()), Cell::Text("b".into())]),
            row_sort_key(&[Cell::Text("ab".into()), Cell::Text(String::new())]),
            "a separator-free key would make these two rows indistinguishable"
        );
    }

    /// The cap bounds the recorded sample, not the count.
    #[test]
    fn divergence_recording_is_capped_but_the_counter_is_not() {
        let counters = ShadowCompareCounters::new();
        let over = MAX_RECORDED_DIVERGENCES + 17;
        for i in 0..over {
            counters.note_compared();
            counters.note_divergence("SELECT 1", format!("detail {i}"));
        }
        assert_eq!(counters.compared(), over as u64);
        assert_eq!(
            counters.diverged(),
            over as u64,
            "the count must keep rising past the cap — only the sample is bounded"
        );
        let recorded = counters.divergences();
        assert_eq!(recorded.len(), MAX_RECORDED_DIVERGENCES);
        assert_eq!(
            recorded[0].detail, "detail 0",
            "the sample keeps the FIRST divergences, which are the ones with \
             context; later ones are usually the same bug repeating"
        );
    }
}
