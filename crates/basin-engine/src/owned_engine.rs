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
use basin_plan::lower::expr::{FuncKind, OperatorResolver};
use basin_plan::lower::select::{lower_select, TableResolver as PlanTableResolver};
use basin_plan::lower::LowerError;
use basin_plan::opt::OptimizerRule;
use basin_plan::{Expr as PlanExpr, FuncId, OpId, Schema as PlanSchema, TableId};
use basin_pgtype::{Oid, PgType};

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
        sess.engine.note_owned_engine_fallback();
        tracing::debug!(
            target: "basin_engine::owned_engine",
            "owned engine fell back to DataFusion: inside an explicit transaction"
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
            sess.engine.note_owned_engine_fallback();
            tracing::debug!(
                target: "basin_engine::owned_engine",
                reason = %reason,
                "owned engine fell back to DataFusion"
            );
            None
        }
    }
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

async fn try_execute_inner(
    sess: &ProjectSession,
    stmt_node: &Node,
) -> Result<ExecResult, Fallback> {
    let resolver = build_resolver(sess, stmt_node).await?;

    let plan = lower_select(stmt_node, &resolver, &RealOperators, &RealFunctions)
        .map_err(Fallback::Lower)?;

    let rules: [&dyn OptimizerRule; 2] = [
        &basin_plan::opt::pushdown::FilterPushdown,
        &basin_plan::opt::projection::ProjectionPruning,
    ];
    let (plan, _passes) = basin_plan::opt::optimize(plan, &rules);

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

    #[allow(clippy::too_many_arguments)]
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
        if meta
            .schema
            .fields()
            .iter()
            .any(|f| f.name().starts_with(crate::promoted_columns::SHADOW_COL_PREFIX))
        {
            return Err(Fallback::Ineligible(
                "table carries promoted JSONB shadow columns",
            ));
        }
        // Any hot-tier footprint (unflushed inserts, or update/delete
        // tombstones — `tombstone_cold_scan.rs`'s own doc comment confirms
        // DELETE tombstones live in this same registry, not a separate
        // cold-tier mechanism) means the committed cold files this bridge
        // reads are not the whole story for this table.
        if let Some(entry) = sess.engine.memtable_registry().get(&sess.project, &table_name) {
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
/// the synthetic `AND`/`OR` sentinels `basin-exec::eval` and
/// `basin-plan::opt::pushdown` already agree on (see those modules' docs:
/// `AND`/`OR` have no `pg_operator` row because Postgres parses them as a
/// `BoolExpr`, not an `OpExpr`). `NOT` is deliberately left unresolved
/// (`None`) rather than given a third sentinel: `basin-exec::eval::eval_unary`
/// does not implement a `NOT` case at all today, so resolving it here would
/// only move the inevitable fallback from a clean `LowerError` at lowering
/// time to a `ExecError::Internal` after a wasted build.
struct RealOperators;

/// Same sentinel values as `basin_exec::eval::{AND_OP, OR_OP}` and
/// `basin_plan::opt::pushdown::AND_OP` — the largest real `pg_operator` oid
/// is in the low thousands, so `u32::MAX` / `u32::MAX - 1` cannot alias one.
const AND_OP: OpId = OpId(Oid(u32::MAX));
const OR_OP: OpId = OpId(Oid(u32::MAX - 1));

impl OperatorResolver for RealOperators {
    fn resolve(&self, name: &str, left: Option<PgType>, right: PgType) -> Option<OpId> {
        match name {
            "AND" => Some(AND_OP),
            "OR" => Some(OR_OP),
            "NOT" => None,
            _ => {
                let left_oid = left.map(|t| t.oid);
                basin_pgtype::operator::resolve(name, left_oid, right.oid)
                    .map(|sig| OpId(sig.oid))
            }
        }
    }
}

/// A hand-written, deliberately small function table — there is no `pg_proc`
/// catalog anywhere in the workspace yet (`lower/expr.rs`'s own module docs:
/// "A column catalog and a function catalog (`pg_proc`) are not built yet").
/// Scoped to exactly the aggregates `basin-exec/src/build.rs`'s
/// `agg_func_of` implements (`sum`/`count`/`avg`/`min`/`max`) — a scalar
/// function name is deliberately left unresolved (`None`) rather than
/// resolved-then-failed: `basin-exec::eval` has no `Expr::ScalarFn` case at
/// all yet (see that module's docs), so resolving one here would only move
/// the inevitable fallback later, past a wasted lower+build.
///
/// The OIDs are real `pg_proc` values (matching `agg_func_of`'s own
/// comment: read from a live PostgreSQL 18, not invented); Postgres itself
/// has one OID per input-type overload, and `agg_func_of` already collapses
/// every overload of one function name to the same accumulator, so any one
/// representative OID per name is sufficient here.
struct RealFunctions;

impl basin_plan::lower::expr::FunctionResolver for RealFunctions {
    fn resolve(&self, name: &[String], _args: &[PgType]) -> Option<(FuncId, FuncKind)> {
        match name.last().map(String::as_str) {
            Some("count") => Some((FuncId(Oid(2803)), FuncKind::Aggregate)),
            Some("sum") => Some((FuncId(Oid(2108)), FuncKind::Aggregate)),
            Some("avg") => Some((FuncId(Oid(2101)), FuncKind::Aggregate)),
            Some("min") => Some((FuncId(Oid(2132)), FuncKind::Aggregate)),
            Some("max") => Some((FuncId(Oid(2116)), FuncKind::Aggregate)),
            _ => None,
        }
    }
}
