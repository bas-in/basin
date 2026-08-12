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
//! This deliberately does not walk into subqueries embedded in `WHERE` /
//! `HAVING` / the target list (`SubLink`): those are a much larger node-kind
//! surface to traverse safely, and the cost of under-prefetching is exactly
//! a safe fallback (`resolve_table` returns `None` for an unprefetched name,
//! `lower_select` reports `LowerError::UnknownName`), never a wrong answer.
//! A query whose only table references live in such a subquery serves
//! narrower than it structurally could; that is the "fraction of SQL" the
//! task described, not a bug.
//!
//! `basin_plan::TableId` has no catalog-side counterpart (see
//! `basin-catalog`'s `TableMetadata`, keyed only by `(ProjectId,
//! TableName)`) — [`build_resolver`] mints one per referenced table, scoped
//! to the single resolver instance built for one statement.

use std::collections::{HashMap, HashSet};

use arrow_schema::{DataType, Field};
use pg_query::protobuf::{node::Node as NodeEnum, Node, SelectStmt, SetOperation};

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

/// Attempt to serve `stmt_node` (a single `SELECT` statement's already-parsed
/// `pg_query` node, as classified by the caller) through the owned pipeline.
///
/// `Some(result)` only on genuine success; `None` in every other case
/// (disabled, ineligible, or any lowering/build/exec failure) — the caller
/// treats `None` identically to "this module was never called" and falls
/// through to the existing DataFusion path unchanged. Every `None` is logged
/// at debug with its reason so the served-vs-fallback ratio (the counters
/// this bumps) can be explained, not just observed.
pub(crate) async fn try_execute(sess: &ProjectSession, stmt_node: &Node) -> Option<ExecResult> {
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
    /// prefetch, e.g. one hidden behind a subquery — see the module docs)
    /// or `LowerError::Malformed` (a parse-tree shape assumption broken).
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

async fn try_execute_inner(
    sess: &ProjectSession,
    stmt_node: &Node,
) -> Result<ExecResult, Fallback> {
    let resolver = build_resolver(sess, stmt_node).await?;

    let plan = lower_select(stmt_node, &resolver, &RealOperators, &RealFunctions)
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

    fn register(
        &mut self,
        key: String,
        table_id: TableId,
        plan_schema: PlanSchema,
        project: ProjectId,
        table: TableName,
        arrow_schema: arrow_schema::SchemaRef,
    ) {
        self.plan_tables.insert(key, (table_id, plan_schema));
        self.exec.register(table_id, project, table, arrow_schema);
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
        resolver.register(
            key,
            table_id,
            plan_schema,
            sess.project,
            table_name,
            meta.schema.clone(),
        );
    }

    Ok(resolver)
}

/// Collect every table name a `SELECT` statement's `FROM` clause(s)
/// reference, recursing into `UNION`/`INTERSECT`/`EXCEPT` arms exactly the
/// way `lower_select_stmt` does. See the module docs for why subqueries
/// embedded in `WHERE`/`HAVING`/the target list are deliberately not walked.
fn collect_tables(node: &Node, out: &mut Vec<Vec<String>>) {
    let Some(NodeEnum::SelectStmt(stmt)) = node.node.as_ref() else {
        return;
    };
    collect_tables_stmt(stmt, out);
}

fn collect_tables_stmt(stmt: &SelectStmt, out: &mut Vec<Vec<String>>) {
    let op_kind = SetOperation::try_from(stmt.op).unwrap_or(SetOperation::Undefined);
    if op_kind != SetOperation::SetopNone {
        if let Some(l) = stmt.larg.as_deref() {
            collect_tables_stmt(l, out);
        }
        if let Some(r) = stmt.rarg.as_deref() {
            collect_tables_stmt(r, out);
        }
        return;
    }
    for item in &stmt.from_clause {
        collect_from_item(item, out);
    }
}

/// Mirrors `lower/select.rs`'s `build_from_item`/`build_join_expr` shape
/// (`RangeVar`, and `JoinExpr` recursing into both sides). Anything else
/// (a subquery or set-returning function in `FROM`) is already
/// `LowerError::Unsupported` at lowering time regardless of what this
/// collects, so there is nothing to gain by recognising it here too.
fn collect_from_item(item: &Node, out: &mut Vec<Vec<String>>) {
    match item.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => {
            let mut parts = Vec::new();
            if !rv.schemaname.is_empty() {
                parts.push(rv.schemaname.clone());
            }
            parts.push(rv.relname.clone());
            out.push(parts);
        }
        Some(NodeEnum::JoinExpr(je)) => {
            if let Some(l) = je.larg.as_deref() {
                collect_from_item(l, out);
            }
            if let Some(r) = je.rarg.as_deref() {
                collect_from_item(r, out);
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
}
