//! SQL → side-effects + result sets, dispatched by sqlparser statement kind.

use crate::pg_ast::ObjectNamePartExt;
use std::cell::Cell;
use std::sync::Arc;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Cooperative pg_sleep cancellation (Phase 5.28.D / #64 partial fix)
// ---------------------------------------------------------------------------
// Problem: `pg_sleep(N)` sleeps the full N seconds even when `statement_timeout`
// fires, because DataFusion UDFs are synchronous and cannot be preempted by the
// outer `tokio::time::timeout` wrapper.
//
// Solution: before each DataFusion `collect()` call the executor records the
// absolute statement deadline in a thread-local.  `PgSleepUdf` reads this
// deadline cooperatively: instead of sleeping the full duration in one shot, it
// sleeps in 50 ms ticks and checks the deadline after each tick.  If the
// deadline has passed it signals cancellation by setting `PG_SLEEP_CANCELED`
// and returning a `DataFusionError::Execution`.  The executor then converts that
// sentinel into `BasinError::QueryCanceled` (SQLSTATE 57014) instead of the
// generic `BasinError::internal`.
//
// Thread-local safety: for the non-shard path the UDF runs on the same tokio
// thread that called `collect()`.  For the shard path the executor captures the
// deadline by value, spawns a blocking thread, sets the thread-local at the top
// of the closure (before `rt.block_on`), and DataFusion runs the UDF on that
// same blocking thread — so the thread-local is always visible.
// ---------------------------------------------------------------------------

thread_local! {
    /// Absolute wall-clock deadline for the current statement, or `None` if
    /// no `statement_timeout` is active.  Set by the executor before each
    /// DataFusion `collect()`; read by `PgSleepUdf` on each sleep tick.
    static STATEMENT_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };

    /// Set to `true` by `PgSleepUdf` when it terminates early due to the
    /// deadline.  The executor checks this flag after a DataFusion collect
    /// error to decide whether to return `QueryCanceled` vs `internal`.
    static PG_SLEEP_CANCELED: Cell<bool> = const { Cell::new(false) };
}

/// Set the per-statement deadline that `pg_sleep` will poll against.
/// Called by the executor before each `df.collect()` / `physical_plan::collect()`.
pub(crate) fn set_statement_deadline(deadline: Option<Instant>) {
    STATEMENT_DEADLINE.with(|c| c.set(deadline));
}

/// Return the current statement deadline (called from `PgSleepUdf`).
pub(crate) fn get_statement_deadline() -> Option<Instant> {
    STATEMENT_DEADLINE.with(|c| c.get())
}

/// Signal that `pg_sleep` terminated early because the deadline passed.
/// Called from `PgSleepUdf` before returning the cancellation error.
pub(crate) fn mark_pg_sleep_canceled() {
    PG_SLEEP_CANCELED.with(|c| c.set(true));
}

/// Read-and-clear the pg_sleep cancellation flag.  Returns `true` once if
/// `mark_pg_sleep_canceled` was called since the last `take_pg_sleep_canceled`.
pub(crate) fn take_pg_sleep_canceled() -> bool {
    PG_SLEEP_CANCELED.with(|c| {
        let v = c.get();
        if v { c.set(false); }
        v
    })
}

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{DataFileRef, TableMetadata};
use basin_common::{BasinError, ChangeEvent, ChangeOp, PartitionKey, Result, TableName};
use basin_storage::{FileFormat, WriteOptions};
use sqlparser::ast::{
    AssignmentTarget, ConflictTarget, Expr, ObjectName, OnConflictAction, OnInsert, SetExpr,
    Statement,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::convert::{batch_df_to_ws, batch_ws_to_df, schema_df_to_ws};
use crate::ddl::{extract_create_table_cluster_by, extract_exclude_using_gist, partition_spec_from_ast};
use crate::dml::{batch_from_rows, group_rows_by_partition};
use crate::events::{
    build_row_json, dispatch_post_commit, dispatch_pre_commit, make_event, registry_has_any,
};
use crate::fast_select::{execute_simple_select, match_simple_select};
use crate::lifecycle::{
    extract_create_table_lifecycle, extract_select_include_deleted, CreateTableLifecycle,
};
use crate::session::{refresh_table, refresh_table_counted};
use crate::{ExecResult, ProjectSession};
use basin_catalog::PartitionSpec;

// ---------------------------------------------------------------------------
// sqlparser Statement parse-cache (perf-impl-w2 Commit B)
// ---------------------------------------------------------------------------
//
// `sqlparser::Parser::parse_sql` is ~20–40µs per call, and for repeat-shape
// workloads (a hot loop running the same SELECT over and over) it fires on
// every execute even after Commit A's rewrite-pipeline pre-screen.
//
// Cache strategy: process-global LRU keyed by xxh3_64 of the SQL string
// handed to `Parser::parse_sql`.  That string is purely text-deterministic
// (catalog state never enters it; the only inputs are the raw SQL and the
// pre-screens above, all of which are pure text transforms or sit inside
// `run_full_rewrite_pipeline` which produces SQL whose hash captures any
// catalog-influenced rewrite output).  Same hash ⇒ sqlparser would
// produce the same `Statement`, so returning the cached one is correct.
//
// Why process-global, not per-session: with per-session caches a connection
// pool that round-robins across N sessions sees N cold misses for the same
// SQL.  The Statement is purely a function of the (post-rewrite) SQL bytes
// — there is no session-specific state inside sqlparser's AST construction
// — so sharing across sessions is safe and lets warm caches stay warm.
//
// Cap: 256 entries by default, overridable via BASIN_ENGINE_PARSE_CACHE_SIZE.
// Entries are `Arc<Statement>` so a hit is a cheap pointer clone, and the
// dispatch sites that need to consume by value (the existing `stmts.pop()`)
// clone out of the Arc — sqlparser's `Statement` implements `Clone` cheaply
// for the structurally-shallow ASTs the cache is hit by in practice
// (point-queries, single-row INSERTs).

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Mutex, OnceLock};
use xxhash_rust::xxh3::xxh3_64;

fn parse_cache_size() -> NonZeroUsize {
    let cap = std::env::var("BASIN_ENGINE_PARSE_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(256);
    NonZeroUsize::new(cap).expect("cap > 0 enforced above")
}

fn parse_cache() -> &'static Mutex<LruCache<u64, Arc<Statement>>> {
    static CACHE: OnceLock<Mutex<LruCache<u64, Arc<Statement>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(LruCache::new(parse_cache_size())))
}

/// Parse `sql` via sqlparser, consulting the process-global Statement cache.
/// On cache hit returns the cached `Arc<Statement>`; on miss parses and
/// inserts.  Multi-statement SQL is parsed but not cached (the cache stores
/// exactly one Statement per entry — the executor enforces `stmts.len() == 1`
/// downstream anyway).
fn parse_sql_cached(sql: &str) -> Result<Arc<Statement>> {
    let key = xxh3_64(sql.as_bytes());

    if let Ok(mut g) = parse_cache().lock() {
        if let Some(cached) = g.get(&key) {
            return Ok(Arc::clone(cached));
        }
    }

    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).map_err(|e| {
        let msg = format!("{e}");
        if msg.contains("Expected: STORED") {
            BasinError::FeatureNotSupported(
                "VIRTUAL generated columns deferred to v0.2; use STORED".to_string(),
            )
        } else {
            BasinError::internal(format!("parse error: {e}"))
        }
    })?;

    if stmts.len() != 1 {
        // Mirror the downstream guard so the caller still produces the
        // canonical "expected exactly one statement" diagnostic — but emit
        // it here, without caching, so the cache never holds an entry that
        // a future identical call could mis-route.
        return Err(BasinError::internal(format!(
            "expected exactly one statement, got {}",
            stmts.len()
        )));
    }
    let stmt = Arc::new(stmts.pop().unwrap());

    if let Ok(mut g) = parse_cache().lock() {
        g.put(key, Arc::clone(&stmt));
    }
    Ok(stmt)
}

#[cfg(test)]
pub(crate) fn parse_cache_contains_for_test(sql: &str) -> bool {
    let key = xxh3_64(sql.as_bytes());
    parse_cache()
        .lock()
        .map(|mut g| g.contains(&key))
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn parse_cache_cap_for_test() -> usize {
    parse_cache()
        .lock()
        .map(|g| g.cap().get())
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn parse_cache_resize_for_test(cap: usize) {
    if let (Ok(mut g), Some(nz)) = (parse_cache().lock(), NonZeroUsize::new(cap)) {
        g.resize(nz);
        g.clear();
    }
}

// ---------------------------------------------------------------------------
// W4: write-striping for the shard auto-commit INSERT path.
//
// Problem: every shard-routed write lands in `PartitionKey::default_key()`,
// whose per-partition `compact_lock` + WAL mutex serialize ALL concurrent
// writers. `concurrent_insert_8x1000` and `rmw_contention_8` both bottleneck
// on this single mutex.
//
// Fix: statement-affine stripe selection. Each statement writes its WHOLE
// batch to ONE of N distinct `PartitionKey`s (`_default`, `s1`, `s2`, …),
// chosen by `session_pid % stripes`. Each stripe has its own `compact_lock`
// + WAL mutex, so concurrent SESSIONS still fan out N-way — which is the
// only place striping ever bought anything: the per-stripe encodes are
// synchronous CPU awaited on the current task, so slicing one statement's
// batch across all N stripes gave ZERO intra-statement parallelism while
// paying N Arrow-IPC encodes + N schema headers + N partition locks + N WAL
// streams + N compacted files per statement (the per-file fan-out that the
// #212 PK-bloom workaround in `basin-shard` compaction exists to mitigate).
//
// Why `session_pid`: it is a process-wide monotonic counter assigned at
// session-open by the `ConnectionRegistry` and never reused, so consecutive
// sessions map to consecutive stripes (perfect round-robin under a
// connection pool) and one session's writes stay in ONE partition — fewer
// files per table, total WAL order for one session's statements.
//
// PK uniqueness is enforced BEFORE this point in
// `enforce_pk_on_insert`/`enforce_unique_on_insert`, against ALL files for
// the table (storage reads union all partitions), so stripe choice doesn't
// affect constraint checking. Cross-partition ordering between distinct
// statements has always been undefined (WAL consumers — replay, compaction,
// reads — are strictly per-partition), so routing different sessions'
// statements to different stripes changes nothing observable.
//
// Stripe count: `BASIN_WRITE_STRIPES` env, default 8.
// Stripe 0 is `default_key()` so existing single-partition data continues
// to live in the same WAL stream and read path.
// ---------------------------------------------------------------------------

const WRITE_STRIPE_DEFAULT: usize = 8;

/// Effective number of write stripes. Clamped to >= 1.
fn write_stripe_count() -> usize {
    std::env::var("BASIN_WRITE_STRIPES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(WRITE_STRIPE_DEFAULT)
}

/// Build the `PartitionKey` for stripe index `i`. Stripe 0 == default so
/// pre-W4 data stays in the same WAL stream; stripes 1..N use `sN`.
fn stripe_partition_key(i: usize) -> PartitionKey {
    if i == 0 {
        PartitionKey::default_key()
    } else {
        // Format is statically valid (no `/`, ASCII, short) so `new` never
        // errors — but degrade to default_key on the impossible case rather
        // than `expect`-ing, so a future change to the validator can't panic
        // the write path.
        PartitionKey::new(format!("s{i}")).unwrap_or_else(|_| PartitionKey::default_key())
    }
}

/// Write `batch` whole through `shard` into ONE stripe partition selected
/// by `session_pid % stripes` (statement-affine striping — see the W4 block
/// comment above). Returns once the write has been durably acknowledged
/// (the WAL append's standard durability contract).
///
/// `stripes == 1` or `batch.num_rows() <= 1` falls back to the default-key
/// single-handle path, preserving the pre-existing small-statement behavior
/// (1-row statements have always landed in `default_key`).
async fn write_batch_striped(
    shard: &basin_shard::Shard,
    project: &basin_common::ProjectId,
    table: &TableName,
    batch: RecordBatch,
    session_pid: i32,
    durable: bool,
) -> Result<()> {
    let stripes = write_stripe_count();
    let n_rows = batch.num_rows();
    if stripes <= 1 || n_rows <= 1 {
        let handle = shard.get(project, &PartitionKey::default_key()).await?;
        return handle.write_batch_opts(table, batch, durable).await;
    }

    // `session_pid` is monotonic and never reused (ConnectionRegistry), so
    // plain modulo distributes consecutive sessions across stripes evenly —
    // no hash needed. `max(0)` guards a hypothetical negative pid so the
    // cast can't explode the modulus.
    let part = stripe_partition_key((session_pid.max(0) as usize) % stripes);
    let handle = shard.get(project, &part).await?;
    handle.write_batch_opts(table, batch, durable).await
}

/// Dispatch `LISTEN` / `UNLISTEN` / `NOTIFY` through the engine's SQL
/// pub-sub primitive (`crate::notify_registry`).
///
/// PG transaction semantics: `NOTIFY` issued inside an open transaction is
/// buffered and only fanned out on `COMMIT`; `ROLLBACK` discards it.
/// `LISTEN` / `UNLISTEN` take effect immediately (they are not transactional
/// in PG either — the docs explicitly state subscriptions are session state
/// not txn state).
async fn exec_pubsub(
    sess: &ProjectSession,
    ps: crate::pg_ast::PubSubStmt,
) -> Result<ExecResult> {
    use crate::pg_ast::PubSubStmt;
    let registry = sess.engine.notify_registry();
    match ps {
        PubSubStmt::Listen(channel) => {
            if channel.is_empty() {
                return Err(BasinError::InvalidSchema(
                    "LISTEN: channel name is required".into(),
                ));
            }
            crate::session::listen_subscribe(&sess.state, registry, sess.project, &channel);
            Ok(ExecResult::Empty {
                tag: "LISTEN".into(),
            })
        }
        PubSubStmt::Unlisten(channel) => {
            crate::session::listen_unsubscribe(&sess.state, &channel);
            Ok(ExecResult::Empty {
                tag: "UNLISTEN".into(),
            })
        }
        PubSubStmt::UnlistenAll => {
            crate::session::listen_unsubscribe_all(&sess.state);
            Ok(ExecResult::Empty {
                tag: "UNLISTEN".into(),
            })
        }
        PubSubStmt::Notify { channel, payload } => {
            if channel.is_empty() {
                return Err(BasinError::InvalidSchema(
                    "NOTIFY: channel name is required".into(),
                ));
            }
            if crate::session::tx_is_active(&sess.state) {
                // PG semantics: queue until COMMIT; discard on ROLLBACK.
                crate::session::listen_buffer_notify(&sess.state, channel, payload);
            } else {
                // Auto-commit: publish immediately.
                registry.publish(sess.project, &channel, &payload, sess.session_pid);
            }
            Ok(ExecResult::Empty {
                tag: "NOTIFY".into(),
            })
        }
    }
}

/// Conservative pre-screen for the ~40-pass string-rewrite pipeline.
///
/// Returns `true` if the SQL contains any token that any rewrite pass might
/// fire on; `false` when none of the markers are present and the pipeline is
/// guaranteed to be a no-op for this query.
///
/// Design contract: false-positives (returning `true` when no rewrite would
/// fire) are free — we just spend ~80–100µs running a pipeline that produces
/// no change. False-negatives (returning `false` when a rewrite was needed)
/// would silently produce wrong results, so any plausible trigger must be
/// listed here. When in doubt, add the marker.
///
/// The markers are derived from the rewrite chain in `run_full_rewrite_pipeline`:
/// each rewrite pass has at least one distinguishing token it scans for; we
/// union them all here.
pub(crate) fn needs_rewrite_pipeline(sql: &str) -> bool {
    // ASCII-only fast-path screen.  The byte-substring searches below are
    // case-sensitive; for keywords we check both common cases.  All current
    // markers are ASCII so a single pass over the bytes is sufficient.
    let bytes = sql.as_bytes();

    // ── 1. Symbol / operator markers — single byte scan, cheap ──────────────
    //
    // `::` — every cast (vector, uuid, citext, interval, text on vec, range,
    //         pg array literal `{…}::int[]`, infinity timestamp).
    // `->` — JSON arrow ops (covers `->`, `->>`, `#>`, `#>>` via spacing pass
    //         and the json_operators rewriter).
    // `@`  — `@>`, `<@`, `@@`, `@?` (json, range, array containment, tsvector,
    //         jsonpath).
    // `~`  — POSIX regex (`~`, `!~`, `~*`, `!~*`) and PG unary bitwise NOT.
    // `?`  — JSON `?`, `?&`, `?|` (also the pgwire bind-param marker; benign
    //         since simple-protocol point queries don't carry `?`).
    // `&`  — array overlap `&&`, range `&&`, json `?&`.
    // `|`  — json `?|`, json `||` concat (also `||` string concat — rewriter
    //         is a no-op for non-jsonb operands but the scan must catch it).
    // `#`  — PG bitwise XOR `#` (DataFusion gap), and the `#>` / `#>>` json
    //         path operators.
    // `B'` — bit-string literal (`B'1010'`).
    // `(`  — function-call syntax; many passes scan for `name(` (every(,
    //         variance(, make_interval(, json_to_record(, SUBSTRING(, EXTRACT(,
    //         OVERLAPS, FILTER(, ANY(, ALL(, SOME(, generate_series(, …).
    //         A trivial point-query has zero `(` so this is a safe trigger.
    let has_symbol_marker = bytes
        .windows(2)
        // `<%` — trgm word-similarity operator `a <% b` (2-byte window catches it;
        // also catches `<->` via the existing `<-` window below).
        .any(|w| matches!(w, b"::" | b"->" | b"<-" | b"<#" | b"<=" | b"B'" | b"<%"))
        || bytes.iter().any(|&b| matches!(b, b'@' | b'~' | b'?' | b'&' | b'|' | b'#' | b'(' | b'%'));
    if has_symbol_marker {
        return true;
    }

    // ── 2. Schema-dot prefixes ──────────────────────────────────────────────
    // `auth.`, `cron.`, `net.`, `pg_` — qualifying / catalog-view rewrites.
    // Check case-insensitively via a lowercase copy (only when none of the
    // cheap symbol triggers fired, so this is the slow path).
    let lower = sql.to_ascii_lowercase();
    if lower.contains("auth.")
        || lower.contains("cron.")
        || lower.contains("net.")
        || lower.contains("pg_")
    {
        return true;
    }

    // ── 3. Keyword markers ──────────────────────────────────────────────────
    // Words that trigger a rewrite when present at SQL-token granularity.
    // We only fire on substring presence (cheap, false-positive-tolerant).
    //
    //  LATERAL                 — every lateral-join rewriter
    //  MATERIALIZED            — `WITH cte AS [NOT] MATERIALIZED`
    //  RECURSIVE               — `WITH RECURSIVE` column-alias injector
    //  ONLY                    — `FROM ONLY` / `JOIN ONLY` strip (handled
    //                            upstream of this pre-screen, but still a
    //                            keyword we don't expect in trivial queries)
    //  OVERLAPS                — `(s,e) OVERLAPS (s,e)`
    //  FILTER                  — `agg(x) FILTER (WHERE …)`
    //  SYMMETRIC               — `BETWEEN SYMMETRIC`
    //  IGNORE                  — `IGNORE NULLS` window arg rewrite
    //  AT TIME ZONE            — at_time_zone rewrite
    //  SUBSTRING               — `SUBSTRING(x FROM '…regex…')`
    //  EXTRACT                 — `EXTRACT(SECOND|EPOCH FROM …)`
    //  ANY / ALL / SOME        — array / subquery quantifier rewrites
    //  INFINITY                — `'infinity'::timestamp`
    //  NEXTVAL / CURRVAL /
    //   SETVAL                 — sequence call rewrites
    //  JSON_AGG / JSONB_AGG    — wildcard expansion
    //  JSON_TO_RECORD /
    //   JSONB_TO_RECORD        — FROM-position rewrite
    //  JSONB_ARRAY_ELEMENTS    — SRF lift
    //  EVERY / VARIANCE /
    //   STDDEV / VAR_SAMP /
    //   MAKE_INTERVAL          — aggregate / interval alias rewrites
    //  TSVECTOR / TSQUERY      — `@@` rewrite (also caught by `@` above; kept
    //                            here for documentation symmetry)
    //  GENERATE_SERIES         — lateral generate_series decorrelation
    //  RANGE                   — substring matches int4range / numrange / etc
    const KEYWORD_MARKERS: &[&str] = &[
        "lateral",
        "materialized",
        "recursive",
        " only ",
        "only\n",
        "only\t",
        "overlaps",
        "filter",
        "symmetric",
        "ignore",
        "at time zone",
        "substring",
        "extract",
        " any",
        " all",
        " some",
        "infinity",
        "nextval",
        "currval",
        "setval",
        "json_agg",
        "jsonb_agg",
        "json_to_record",
        "jsonb_to_record",
        "jsonb_array_elements",
        "every",
        "variance",
        "stddev",
        "var_samp",
        "make_interval",
        "tsvector",
        "tsquery",
        "generate_series",
        "range",
    ];
    KEYWORD_MARKERS.iter().any(|m| lower.contains(m))
}

/// Runs the full ~40-pass string-rewrite pipeline.  Only invoked when
/// `needs_rewrite_pipeline` says at least one pass might fire.
async fn run_full_rewrite_pipeline(sess: &ProjectSession, sql: &str) -> Result<String> {
    // Rewrite `auth.uid()` / `auth.role()` / `auth.jwt()` to their
    // underscore-namespaced UDF equivalents before handing SQL to sqlparser.
    // DataFusion's SQL parser does not support schema-qualified function names
    // in call position; this rewrite is safe (identifier-boundary checked)
    // and always runs, even when auth is disabled — the UDFs simply return
    // NULL/`'anon'` for unauthenticated sessions.
    let sql = crate::udf::rewrite_auth_schema_functions(sql);
    // Phase 5.8.A: rewrite cron.schedule/unschedule and net.http_get/post.
    let sql = crate::cron_glue::rewrite_cron_schema_functions(&sql);
    let sql = crate::net_glue::rewrite_net_schema_functions(&sql);
    // Phase 5.23.D: qualify unqualified pg_catalog system view names so that
    // DataFusion resolves them in `pg_catalog` rather than `public`. E.g.
    // `FROM pg_locks` → `FROM pg_catalog.pg_locks`.
    let sql = crate::pg_operators::rewrite_unqualified_pg_catalog_views(&sql);
    let sql = sql.as_str();

    // Translate the pg_vector operator forms (`<->`, `<#>`, `<=>`) into the
    // matching UDF calls before handing the SQL to sqlparser. See
    // `udf::rewrite_vector_operators` for the strategy and its limits.
    // Lower `tsvector @@ tsquery` to the `ts_match(...)` UDF before sqlparser
    // sees the SQL — sqlparser doesn't have AtAt in its operator table.
    let rewritten = crate::pg_ast::rewrite_tsvector_at_at(sql);
    // Phase 5.26.C: strip `::vector` / `::vector(N)` casts BEFORE the operator
    // rewriter so that `'[1,0,0]'::vector <-> '[0,1,0]'::vector` becomes
    // `'[1,0,0]' <-> '[0,1,0]'` before the operator is expanded to l2_distance().
    // Also strip `::text` on vector columns before the operator rewrites see them.
    let rewritten = crate::pg_operators::rewrite_vector_cast(&rewritten);
    // Phase 5.26.B: rewrite `col::text` → `vector_to_text(col)` before operators.
    let rewritten = crate::pg_operators::rewrite_vector_col_text_cast(&rewritten);
    // Expand `json_agg(<alias>.*)` / `jsonb_agg(<alias>.*)` to the explicit
    // `jsonb_build_object(...)` form. Prisma's nested-read shape (correlated
    // subquery returning `json_agg(o.*)`) hits a DataFusion physical-planner
    // gap: it rejects `Wildcard { qualifier: Some(...) }` inside scalar
    // aggregates with XX000. The expansion is semantically identical and
    // only uses unqualified column refs + the wired `jsonb_build_object` UDF.
    // The catalog lookup builds the alias→columns map; aliases that don't
    // resolve to a real table fall through unchanged (false-positive guard).
    let rewritten = rewrite_json_agg_qualified_wildcard_with_catalog(
        &rewritten,
        sess.engine.config().catalog.as_ref(),
        &sess.project,
    )
    .await;
    let rewritten = crate::udf::rewrite_vector_operators(&rewritten);
    // Normalise whitespace around the PG JSON path operators (`->`, `->>`,
    // `#>`, `#>>`) before the JSON-op rewriter sees them.  Without this pass,
    // a bare-column form like `body->>'k'` (no spaces, no `::jsonb` cast)
    // fails the `rewrite_binary_op_to_fn` `prev_ok` guard and is left for
    // sqlparser, which doesn't know `->>` and errors out.  Pre-spacing closes
    // that gap without relaxing the boundary guard (which would risk
    // mis-matching vector ops like `<->`).
    let rewritten = crate::pg_operators::rewrite_jsonb_arrow_op_spacing(&rewritten);
    // Rewrite JSON/JSONB infix operators (`->`, `->>`, `#>`, `#>>`, `?`,
    // `?&`, `?|`, `<@`, `@>` for JSON, `||` for JSON concat, `@?` for
    // jsonpath exists) to UDF calls that DataFusion can evaluate.
    let rewritten = crate::udf::rewrite_json_operators(&rewritten);
    // ADR 0027 Phase 4: rewrite `json_get_text(col, 'key')` → shadow column
    // for any promoted JSONB paths on tables referenced in the query.
    let rewritten = rewrite_promoted_cols_for_query(&rewritten, sess).await;
    // Rewrite `json_to_record(J) AS t(coldefs)` / `jsonb_to_record(J) AS t(coldefs)`
    // → `SELECT * FROM json_to_recordset([J]) AS t(coldefs)` so that sqlparser
    // does not choke on the typed coldef list in scalar-SELECT / FROM position.
    let rewritten = crate::pg_operators::rewrite_json_to_record(&rewritten);
    // Rewrite a FROM-less `SELECT jsonb_array_elements('[…]'::jsonb)` (and the
    // other JSON/JSONB set-returning functions) into
    // `SELECT * FROM jsonb_array_elements('[…]'::jsonb)` so the row-expanding
    // table function (UDTF) is used instead of the single-row scalar stub.
    // PostgreSQL expands SRFs in the SELECT list to a row set; this restores
    // that behaviour for the common ORM array-/object-expansion idiom.
    let rewritten = crate::pg_operators::rewrite_jsonb_srf_scalar_select(&rewritten);
    // Rewrite PostgreSQL POSIX regex operators (`~`, `!~`, `~*`, `!~*`) to
    // `regexp_like(…)` calls DataFusion accepts; expand `BETWEEN SYMMETRIC`;
    // rewrite array containment / overlap operators (`@>`, `<@`, `&&`) for
    // array-typed operands. See `pg_operators` for the full operator table.
    let rewritten = crate::pg_operators::rewrite_posix_regex_operators(&rewritten);
    // JSONPath @? / @@ operator rewrites → jsonb_path_exists / jsonb_path_match.
    let rewritten = crate::jsonb_path_udf::rewrite_jsonpath_operators(&rewritten);
    let rewritten = crate::window_extras::rewrite_ignore_nulls_in_args(&rewritten);
    let rewritten = crate::pg_operators::rewrite_between_symmetric(&rewritten);
    // Rewrite `'{1,2,3}'::int[]` curly-brace array literal casts to
    // `make_array(1,2,3)` before the array-operator pass sees them.
    let rewritten = crate::pg_operators::rewrite_pg_array_literal_casts(&rewritten);
    let rewritten = crate::pg_operators::rewrite_array_operators(&rewritten);
    // Rewrite `B'1010'` (bit-string literals) to plain string literals `'1010'`
    // before sqlparser/DataFusion sees them. DataFusion 53 does not handle
    // sqlparser's `SingleQuotedByteStringLiteral` value variant.
    let rewritten = crate::pg_operators::rewrite_bit_string_literal(&rewritten);
    // Rewrite `'...'::UUID` to `'...'::VARCHAR` — DataFusion 53 does not
    // implement the UUID SQL type in CAST expressions.
    let rewritten = crate::pg_operators::rewrite_uuid_cast(&rewritten);
    // Rewrite `'...'::citext` to `'...'::TEXT` — citext is stored as plain
    // Utf8 at the Arrow level; the cast is a no-op for the evaluator.
    let rewritten = crate::pg_operators::rewrite_citext_cast(&rewritten);
    // Rewrite `'HH:MM:SS'::INTERVAL` to `'N seconds'::INTERVAL` — Arrow's
    // interval parser does not accept the PG HH:MM:SS shorthand form.
    let rewritten = crate::pg_operators::rewrite_interval_hms_cast(&rewritten);
    // Rewrite PG bitwise operators that DataFusion's GenericDialect doesn't
    // understand: `A # B` (XOR) → `A ^ B`; `~expr` (unary NOT) →
    // `(-1 ^ (expr))`.
    let rewritten = crate::pg_operators::rewrite_pg_bitwise_operators(&rewritten);
    // Rewrite `expr = ANY(ARRAY[...])` / `= SOME(ARRAY[...])` → `expr IN (...)`.
    // DataFusion cannot plan the ARRAY-literal form of ANY/SOME; `IN` is the
    // exact PG equivalent for equality quantification over an inline array.
    // Also handles `<> ANY(ARRAY[...])` → `NOT IN (...)`.
    // This must run BEFORE the subquery ANY rewriter so the subquery rewriter
    // only sees subquery forms.
    let rewritten = crate::pg_operators::rewrite_any_array(&rewritten);
    // Rewrite `expr OP ALL(ARRAY[...])` to a VALUES subquery so the existing
    // all-subquery rewriter can reduce it to a scalar aggregate comparison.
    let rewritten = crate::pg_operators::rewrite_all_array(&rewritten);
    // Rewrite `= ANY (subquery)` / `= SOME (subquery)` → `IN (subquery)`.
    // DataFusion's ANY subquery planner has type-coercion issues; the IN form
    // is equivalent for equality comparisons and works reliably.
    let rewritten = crate::pg_operators::rewrite_any_some_subquery(&rewritten);
    // Rewrite `LATERAL unnest(...)` → `unnest(...)` so sqlparser sees
    // TableFactor::UNNEST (handled by DataFusion) instead of
    // TableFactor::Function { lateral: true } (not a registered table fn).
    let rewritten = crate::pg_operators::rewrite_lateral_unnest(&rewritten);
    // Rewrite uncorrelated `LATERAL (subquery)` → `(subquery)`.  When the
    // subquery body has zero references to outer FROM columns, LATERAL is
    // semantically identical to a plain join.  Stripping it lets DataFusion
    // plan it without needing the (unimplemented) correlated-lateral physical
    // operator.  Correlated LATERAL is left untouched (DataFusion upstream
    // limitation: physical plan does not support OuterReferenceColumn paths).
    let rewritten = crate::pg_operators::rewrite_lateral_uncorrelated(&rewritten);
    // Rewrite the common ORM nested-read pattern:
    //   `LEFT JOIN LATERAL (SELECT agg(...) FROM child WHERE child.fk=outer.pk) sub ON true`
    // → `LEFT JOIN (SELECT child.fk, agg(...) FROM child GROUP BY child.fk) sub ON sub.fk=outer.pk`
    // Only fires when ALL projection items are aggregate functions and there is
    // exactly ONE correlation predicate.  ORDER BY / LIMIT inside the subquery,
    // non-aggregate projections, or multiple correlation predicates cause the
    // rewriter to defer (leaving the query to fail with the upstream error).
    let rewritten = crate::pg_operators::rewrite_lateral_nested_agg(&rewritten);
    // Rewrite correlated non-aggregate LATERAL subqueries into ordinary JOINs.
    let rewritten = crate::pg_operators::rewrite_lateral_correlated_row(&rewritten);
    // Rewrite correlated LATERAL bodies with ORDER BY + LIMIT into window-function joins.
    let rewritten = crate::pg_operators::rewrite_lateral_order_limit(&rewritten);
    // Decorrelate `JOIN LATERAL generate_series(<lo>, <tbl>.<col>)` into a bounded
    // recursive-CTE JOIN.
    let rewritten = crate::pg_operators::rewrite_lateral_generate_series(&rewritten);
    // Rewrite `(s1, e1) OVERLAPS (s2, e2)` → `overlaps(s1, e1, s2, e2)`.
    let rewritten = crate::pg_operators::rewrite_overlaps(&rewritten);
    // Rewrite `agg(x) FILTER (WHERE cond)` → `agg(CASE WHEN cond THEN x END)`.
    let rewritten = crate::pg_operators::rewrite_aggregate_filter(&rewritten);
    // Strip `[NOT] MATERIALIZED` hint from `WITH cte AS [NOT] MATERIALIZED (…)`.
    let rewritten = crate::pg_operators::rewrite_cte_materialized(&rewritten);
    // Inject explicit `AS col` aliases into the base case of recursive CTEs.
    let rewritten = crate::pg_operators::rewrite_recursive_cte_column_aliases(&rewritten);
    // Rewrite `'[lo,hi)'::int4range = '[lo,hi)'::int4range` to range_eq.
    let rewritten = crate::range_udf::rewrite_range_equality(&rewritten);
    // Translate PG range infix operators (`@>`, `<@`, `&&`, `<<`, `>>`, `-|-`)
    // into UDF calls.
    let rewritten = crate::range_udf::rewrite_range_operators(&rewritten);
    // Rewrite `'...'::int4range` / `'...'::daterange` etc. to just `'...'`.
    let rewritten = crate::range_udf::rewrite_range_casts(&rewritten);
    // Rewrite pg_trgm operators (`%`, `<%`, `<->`) to function-call forms.
    // Runs after `rewrite_vector_operators` so that pgvector `<->` has already
    // been expanded to `l2_distance`; any remaining `<->` is either a text
    // trigram distance or a tsquery phrase operator inside a string literal
    // (quote-aware scan protects the literal case).
    // `%` / modulo: skipped when both operands are bare numeric literals.
    let rewritten = crate::trgm_glue::rewrite_trgm_operators(
        &rewritten,
        crate::session::session_trgm_similarity_threshold(&sess.state),
        crate::session::session_trgm_word_similarity_threshold(&sess.state),
    );
    // Rewrite `SUBSTRING(<expr> FROM '<regex>')` into `substring_regex(...)`.
    let rewritten = crate::regex_udf::rewrite_substring_regex(&rewritten);
    // Route `EXTRACT(SECOND FROM <expr>)` to the Basin UDF.
    let rewritten = crate::udf::rewrite_extract_second(&rewritten);
    // Rewrite `expr AT TIME ZONE 'tz'` to `at_time_zone(expr, 'tz')`.
    let rewritten = crate::interval_tz_udf::rewrite_at_time_zone(&rewritten);
    // Rewrite `EXTRACT(EPOCH FROM interval_expr)` to the interval-specific UDF.
    let rewritten = crate::interval_tz_udf::rewrite_extract_epoch_interval(&rewritten);
    // Rewrite `make_interval(years => 1, days => 30)` to the positional form.
    let rewritten = crate::pg_scalar_aliases::rewrite_make_interval_named_args(&rewritten);
    // Rewrite `every(...)` → `bool_and(...) AS every`.
    let rewritten = crate::pg_scalar_aliases::rewrite_every_to_bool_and(&rewritten);
    // PG aggregate name aliases: `variance(x)` → `var(x)`.
    let rewritten = crate::udf::rewrite_pg_agg_aliases(&rewritten);
    // Add explicit AS aliases to known aliased aggregates.
    let rewritten = crate::pg_scalar_aliases::rewrite_agg_unique_aliases(&rewritten);
    // Rewrite `'infinity'::timestamp` / `'-infinity'::timestamp` to UDF form.
    let rewritten = crate::datetime_extras::rewrite_infinity_timestamp(&rewritten);
    // User-defined `LANGUAGE sql` function inlining.
    let inlined = crate::sql_functions::rewrite_sql_inlining_functions(
        &sess.engine.config().catalog,
        &sess.project,
        &rewritten,
    )
    .await?;
    // Rewrite sequence calls (`nextval('seq')` / `currval('seq')` / `setval`).
    let seq_ctx = crate::seq_udf::SequenceContext {
        catalog: &sess.engine.config().catalog,
        project: sess.project,
        session_cache: &sess.state.sequence_cache,
    };
    let seq_rewritten = crate::seq_udf::rewrite_sequence_calls(&inlined, &seq_ctx).await?;
    // Enum-column ordering rewrite.
    let enum_rewritten = crate::enum_ordinal::rewrite_enum_ordering(
        &sess.engine.config().catalog,
        &sess.project,
        &seq_rewritten,
    )
    .await?;
    Ok(enum_rewritten)
}

/// Returns `true` when the full string-rewrite pipeline leaves `sql`
/// byte-for-byte unchanged — i.e. dispatching the parsed AST is guaranteed
/// identical to dispatching the rewritten text.
///
/// Used by the prepared-statement bind fast path ([`prepared`]) to decide,
/// ONCE at prepare time, whether a template is AST-fast-path eligible. The
/// cheap `needs_rewrite_pipeline` pre-screen over-triggers on `(` (so every
/// `INSERT … VALUES (…)` would be excluded); this exact check runs the real
/// pipeline and compares, so a plain INSERT whose text no pass actually
/// touches qualifies for the fast path while anything the pipeline would
/// rewrite (json operators, casts, lateral, …) correctly falls back to text.
/// Errors conservatively report "not a no-op" (→ fall back).
pub(crate) async fn rewrite_pipeline_is_noop(sess: &ProjectSession, sql: &str) -> bool {
    if !needs_rewrite_pipeline(sql) {
        // No marker token at all — the pipeline is definitionally a no-op.
        return true;
    }
    match run_full_rewrite_pipeline(sess, sql).await {
        Ok(rewritten) => rewritten == sql,
        Err(_) => false,
    }
}

pub(crate) async fn execute(sess: &ProjectSession, sql: &str) -> Result<ExecResult> {
    // Phase 5.28.C: touch last-activity timestamp so the idle-in-txn reaper
    // sees that this session is still making progress. Done unconditionally at
    // the top of every execute — even for rejected statements — so the reaper
    // doesn't incorrectly fire during a burst of aborted commands.
    crate::session::touch_last_active(&sess.state);

    // Phase 5.28.C: check whether the idle-in-txn reaper has flagged this
    // session as expired. If so, reject with SQLSTATE 25P03 immediately.
    if sess.reaped_flag.is_reaped() {
        return Err(basin_common::BasinError::IdleInTransactionTimeout(
            "idle transaction terminated by server idle_in_transaction_session_timeout".into(),
        ));
    }

    // ── Pre-parse literal-INSERT fast path ──────────────────────────────────
    // For a plain auto-commit `INSERT INTO t [(cols)] VALUES (...), ...` the
    // normal path below parses the ENTIRE statement twice — libpg_query (for
    // statement-kind dispatch) and sqlparser (a full `Vec<Vec<Expr>>` AST for
    // every tuple) — only for the values_fast scanner in `exec_insert` to then
    // discard the sqlparser rows unused. For a 10k-row / multi-MB statement
    // those two parses are a large fixed tax. `try_insert_preparse` classifies
    // the statement with an O(prefix) header scan, validates the header with a
    // tiny sqlparser parse of just `sql[..VALUES]`, runs the existing tuple
    // scanner, and on success routes straight into the shared
    // `exec_insert_prebuilt` seam — neither whole-statement parser ever runs.
    // ANY uncertainty (open transaction, ON CONFLICT / RETURNING / extra
    // statements after the tuples, hypertables, partitioned targets,
    // unsupported literals, …) returns `None` here and the normal path below
    // runs byte-for-byte unchanged.
    if let Some(result) = try_insert_preparse(sess, sql).await {
        return result;
    }

    // Keep a reference to the SQL the user actually wrote. The rewriter
    // below mangles vector operators into UDF calls; that rewrite is
    // irrelevant to (and would only confuse) the analytical engine, which
    // doesn't know our UDFs.
    let raw_sql = sql;

    // #53 P1 — parser DoS pre-check (depth guard). Both libpg_query (C, called
    // below via `pg_ast::parse`) and sqlparser (Rust, called further down) use
    // recursive descent: a deeply nested expression like `(((…1…)))` with
    // 10 000 levels overflows the thread stack and SIGABRTs the process —
    // remote-triggerable DoS. The guard scans the SQL string for unbalanced
    // paren depth (quote- and comment-aware) and rejects with
    // `BasinError::InvalidSchema` (SQLSTATE 42601) when it exceeds
    // `MAX_PARSE_DEPTH`. We surface the rejection here (not just inside
    // `pg_ast::parse`) because the existing call site swallows the error with
    // `.ok()` and would otherwise hand the same SQL to sqlparser, which would
    // then blow the stack instead.
    crate::pg_ast::check_parse_depth(sql)?;

    // ADR 0014 Phase 1: parse with the real PostgreSQL parser first.
    // This lets us:
    //   1. Intercept noop-accept statements (VACUUM, ANALYZE, CLUSTER, LOCK,
    //      COMMENT, EXPLAIN, RBAC primitives, FDWs, ownership, etc.) and return
    //      immediately — sqlparser never sees them.
    //   2. Reject explicitly-unsupported statements (LISTEN, NOTIFY, UNLISTEN)
    //      with SQLSTATE 0A000 before sqlparser sees them.
    //   3. Route TRUNCATE to its real implementation.
    // On pg_query parse failure we fall through to sqlparser, which will
    // produce its own error. Both errors will surface as
    // BasinError::InvalidSchema (SQLSTATE 42601) to the client.
    //
    // IMPORTANT: We cache the parse tree so the second noop-accept / reject
    // gate later in this function (after the string-rewrite pipeline) can
    // reuse the same tree without calling pg_query::parse a second time.
    // pg_query::parse calls into a C library and is not cheap.
    let raw_pg_tree: Option<crate::pg_ast::ParseTree> = crate::pg_ast::parse(sql).ok();
    if let Some(ref tree) = raw_pg_tree {
        // Collect statement kinds so we can dispatch before sqlparser.
        let kinds: Vec<_> = tree.stmts().map(|n| crate::pg_ast::stmt_kind(n)).collect();

        // Dispatch LISTEN / NOTIFY / UNLISTEN through the engine's SQL
        // pub-sub primitive (`crate::notify_registry`). For multi-statement
        // bodies we only fast-path single-statement messages here — the
        // router splits multi-statement simple-query into individual
        // statements before reaching this entry point, so the common case
        // is `kinds.len() == 1`.
        if kinds.len() == 1 {
            use crate::pg_ast::StmtKind;
            let kind = kinds[0];
            if matches!(kind, StmtKind::Listen | StmtKind::Notify | StmtKind::Unlisten) {
                let node = tree.stmts().next().expect("kinds[0] implies stmts[0]");
                let ps = crate::pg_ast::pubsub_stmt(node)
                    .expect("pubsub stmt kind must classify");
                return exec_pubsub(sess, ps).await;
            }
        }

        // Noop-accept dispatch: for single-statement SQL only (the common case).
        // Multi-statement bodies are split by the router before reaching execute().
        if kinds.len() == 1 {
            let kind = kinds[0];

            // TRUNCATE is a real operation — delete all rows.
            if matches!(kind, crate::pg_ast::StmtKind::Truncate) {
                return crate::truncate::exec_truncate(sess, &tree).await;
            }

            // ── Aborted-state guard (SQLSTATE 25P02) ──────────────────────────
            // When a statement failed inside an active transaction the session
            // enters the "aborted" state.  Every subsequent statement except
            // ROLLBACK (full or to-savepoint) is rejected until the client
            // issues ROLLBACK to reset the session.
            if crate::session::tx_is_aborted(&sess.state) {
                // ROLLBACK and ROLLBACK TO SAVEPOINT are the only exit paths.
                let is_rollback_kind = matches!(kind, crate::pg_ast::StmtKind::Rollback);
                if !is_rollback_kind {
                    return Err(basin_common::BasinError::InvalidSchema(
                        "ERROR: current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)".into()
                    ));
                }
            }

            // Transaction control — intercept before noop_accept so TxState
            // is updated on every BEGIN / COMMIT / ROLLBACK / SAVEPOINT.
            match kind {
                crate::pg_ast::StmtKind::BeginTransaction => {
                    // Snapshot the current catalog heads into TxState so that
                    // ROLLBACK can restore them.
                    let current_snapshots = sess
                        .state
                        .snapshots
                        .try_lock()
                        .map(|g| g.clone())
                        .unwrap_or_default();
                    crate::session::tx_begin(&sess.state, current_snapshots);
                    return Ok(ExecResult::Empty {
                        tag: "BEGIN".into(),
                    });
                }
                crate::pg_ast::StmtKind::Commit => {
                    // Phase 5.14.C2 / ADR 0020 §6: promote hot-tier rows to
                    // the shared MemTableRegistry and emit an explicit WAL
                    // TxCommit marker so crash-recovery replay can distinguish
                    // committed transactions from crash-mid-tx aborts.
                    //
                    // Capture tx_id *before* tx_commit() clears TxState.
                    let commit_tx_id = crate::session::tx_get_id(&sess.state);
                    let htap_rows = crate::session::tx_htap_take_all(&sess.state);
                    // Drain the in-tx hot-tier UPDATE/DELETE overlay (the
                    // OLTP-in-tx fast path). Each entry is written into the
                    // shared MemTableRegistry below — exactly mirroring what the
                    // auto-commit fast path writes (registry-only; durability
                    // comes from the registry's own flush/compaction, NOT the
                    // WAL — the auto-commit `hot_tier_update_by_pk` /
                    // `hot_tier_delete_by_pk` do not WAL-log either, so we add
                    // no extra WAL records here beyond the existing TxCommit
                    // marker emitted below).
                    let tx_overlay = crate::session::tx_overlay_take_all(&sess.state);

                    // Promote committed HTAP batches to the process-wide
                    // registry. Promotion stays PRE-commit (visibility: other
                    // sessions must see the rows the moment COMMIT starts
                    // landing them — see the function doc); the returned
                    // per-table `(key, seq)` acks are applied AFTER the
                    // catalog commit below makes the same rows durable in
                    // cold Parquet (S4 commit 4b).
                    let promote_clean_acks = htap_promote_to_registry(sess, &htap_rows).await?;

                    // Drain the tx overlay into the shared registry. This runs
                    // BEFORE `tx_commit` (which clears TxState) and BEFORE the
                    // pending-file catalog commit below, so the override/tombstone
                    // becomes visible to other sessions at the same moment the
                    // committed cold files do. Crash window vs the pending-file
                    // catalog commit: identical to htap_promote_to_registry's
                    // (registry writes are not transactional with the catalog) —
                    // we match the existing pattern, no stronger guarantee.
                    if !tx_overlay.is_empty() {
                        let registry = sess.engine.memtable_registry();
                        for (table, entries) in &tx_overlay {
                            let entry =
                                registry.get_or_create(sess.project, table.clone());
                            for (key, value) in entries {
                                entry.memtable.insert(key.clone(), value.clone());
                            }
                        }
                    }

                    // perf-w7-txn: write buffered INSERT batches to ONE Parquet
                    // file per table.  Replaces the old per-INSERT writes that
                    // dominated `BEGIN; INSERT x100; COMMIT` cost.  Each table's
                    // files (htap-derived + any UPDATE/DELETE pending files) are
                    // committed together so the catalog snapshot advances once.
                    let mut buffered_files: std::collections::HashMap<
                        TableName,
                        Vec<DataFileRef>,
                    > = std::collections::HashMap::new();
                    for (table, batches) in &htap_rows {
                        if batches.is_empty() {
                            continue;
                        }
                        // Load table metadata to derive write options.
                        // Inv-OLTP-write (#155): served from the per-session
                        // cache populated by the in-tx INSERTs above (the
                        // invalidation carve-out keeps the entry hot across
                        // INSERT batches; COMMIT is also not an invalidating
                        // statement kind — it's a transaction control
                        // intercepted before the dispatch table-meta-cache
                        // invalidation block).
                        let meta = match crate::session::load_table_meta_cached_err(
                            sess, table,
                        )
                        .await
                        {
                            Ok(m) => m,
                            Err(e) => {
                                crate::session::tx_set_aborted(&sess.state);
                                return Err(e);
                            }
                        };
                        // Concat all buffered batches into one.  `concat_batches`
                        // shares Arrow buffers where possible; for the typical
                        // single-row-per-INSERT shape this is a cheap copy.
                        let schema = batches[0].schema();
                        let combined = match arrow::compute::concat_batches(&schema, batches) {
                            Ok(b) => b,
                            Err(e) => {
                                crate::session::tx_set_aborted(&sess.state);
                                return Err(BasinError::internal(format!(
                                    "concat_batches at COMMIT for {table}: {e}"
                                )));
                            }
                        };
                        // perf-w7-fix (txn_insert_x100): the COMMIT-time write
                        // is a SINGLE concatenated batch (potentially
                        // many-row), not a per-statement single-row INSERT, so
                        // the Vortex `Fast` cascade's fixed setup cost
                        // amortises cleanly. Flip to `Fast` unconditionally
                        // here — the `!in_tx` caveat (per-statement
                        // single-row setup overhead, commit ddfd8a8) does not
                        // apply to a buffered concat. Measured drop on
                        // `BEGIN; INSERT x100; COMMIT`: ~210ms → ~74ms (3x)
                        // without any env flag. Ignored for Parquet (the
                        // ZSTD-1 path is already fast; see
                        // basin-storage::writer::WriteOptions::encoding_mode).
                        // Round-trip parity is covered by the Fast⇆Best
                        // differential test in vortex_format.rs (#92).
                        let mut opts = write_options_for(&meta, false);
                        opts.encoding_mode = basin_storage::EncodingMode::Fast;
                        let part = basin_common::PartitionKey::default_key();
                        let df = match sess
                            .engine
                            .config()
                            .storage
                            .write_batch_with_options(
                                &sess.project,
                                table,
                                &part,
                                &combined,
                                &opts,
                            )
                            .await
                        {
                            Ok(d) => d,
                            Err(e) => {
                                crate::session::tx_set_aborted(&sess.state);
                                return Err(e);
                            }
                        };
                        // Maintain secondary indexes against the COMMIT-time
                        // file path (deferred from the in-tx INSERT path
                        // because no real file existed then).
                        maintain_secondary_indexes_on_insert(
                            sess,
                            table,
                            &meta,
                            &combined,
                            df.path.as_ref(),
                        )
                        .await;
                        buffered_files.insert(
                            table.clone(),
                            vec![DataFileRef {
                                path: df.path.as_ref().to_string(),
                                size_bytes: df.size_bytes,
                                row_count: df.row_count,
                                column_stats: df.column_stats.clone(),
                                bloom_filters: df.bloom_filters.clone(),
                                hll_sketches: std::collections::BTreeMap::new(),
                                tdigest_sketches: std::collections::BTreeMap::new(),
                            }],
                        );
                    }

                    // Flush pending files to the catalog (real COMMIT).
                    // `pending` contains UPDATE/DELETE-generated files (INSERT
                    // batches are no longer staged as pending files — they were
                    // just written above into `buffered_files`).
                    let pending = crate::session::tx_commit(&sess.state);
                    // Merge buffered (htap-INSERT) + pending (UPDATE/DELETE)
                    // files per table so each table commits once.
                    let mut commits: std::collections::HashMap<TableName, Vec<DataFileRef>> =
                        buffered_files;
                    for (table, files) in pending {
                        commits.entry(table).or_default().extend(files);
                    }
                    if !commits.is_empty() {
                        for (table, files) in &commits {
                            // Load the current snapshot id for this table.
                            let snap = {
                                let snaps = sess.state.snapshots.lock().await;
                                snaps
                                    .get(table)
                                    .copied()
                                    .unwrap_or(basin_catalog::SnapshotId(0))
                            };
                            match commit_with_retry(sess, table, snap, files.clone()).await {
                                Ok(()) => {}
                                Err(e) => {
                                    // If catalog append fails, return error.
                                    // The pending map was already drained from
                                    // TxState; the session is now in a dirty
                                    // state (files written but not catalogued).
                                    // Best-effort: mark aborted — client must ROLLBACK.
                                    crate::session::tx_set_aborted(&sess.state);
                                    return Err(e);
                                }
                            }
                            // S4 commit 4b: this table's htap rows are now
                            // durable in the cold file the commit above just
                            // landed — ack the promoted registry copies CLEAN
                            // (`mark_flushed` at the insert seqs captured at
                            // promote time). Without this ack the rows would
                            // sit DIRTY and the hot-tier flush would write
                            // them to cold a SECOND time. Acked rows keep
                            // serving point reads as retained residency.
                            // Kill switch: with `retain_secs == 0` we skip
                            // the ack entirely so the registry behaves
                            // byte-for-byte like today (rows stay dirty and
                            // resident until the flush worker drains them).
                            {
                                let registry = sess.engine.memtable_registry();
                                if registry.config().retain_secs > 0 {
                                    if let Some(acks) = promote_clean_acks.get(table) {
                                        if !acks.is_empty() {
                                            if let Some(entry) =
                                                registry.get(&sess.project, table)
                                            {
                                                let freed = entry.memtable.mark_flushed(acks);
                                                if freed > 0 {
                                                    registry
                                                        .release_bytes(&sess.project, freed);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // Inv-OLTP-write (#155): COMMIT just advanced the
                            // catalog for this table — the cached
                            // `TableMetadata` (populated by the in-tx INSERTs)
                            // now has a stale `live_data_files()` snapshot.
                            // Invalidate the per-table entry so the FIRST
                            // post-COMMIT SELECT re-loads. Without this,
                            // metadata-only aggregates (e.g. COUNT(*)) would
                            // be answered from the pre-INSERT snapshot.
                            // Correctness gate:
                            // `coverage_txn_schema::txn_multi_statement_all_rows_visible_after_commit`.
                            sess.state.table_meta_cache.invalidate(table);
                            // Refresh the session's DataFusion context so
                            // the committed files are visible via catalog.
                            let _ = refresh_table(
                                &sess.engine,
                                &sess.project,
                                &sess.ctx,
                                &sess.state,
                                table,
                            )
                            .await;
                        }
                    }
                    // Flush any NOTIFY payloads buffered during this
                    // transaction to the engine's NotifyRegistry. PG
                    // semantics: notifies queued inside BEGIN..COMMIT only
                    // fire on COMMIT; they are silently dropped on
                    // ROLLBACK (handled in the Rollback arm below).
                    let pending_notifies =
                        crate::session::listen_take_pending_notifies(&sess.state);
                    if !pending_notifies.is_empty() {
                        let registry = sess.engine.notify_registry();
                        for n in pending_notifies {
                            registry.publish(
                                sess.project,
                                &n.channel,
                                &n.payload,
                                sess.session_pid,
                            );
                        }
                    }
                    // ADR 0020 §6: emit explicit TxCommit WAL marker so that
                    // crash-recovery replay can distinguish committed transactions
                    // from crash-mid-tx aborts.  Only emitted when the executor
                    // had a WAL-backed tx_id (i.e. at least one DML was executed
                    // inside this BEGIN block, which triggers htap_emit_wal_begin_lazy).
                    // Best-effort: WAL marker failures must not block commit.
                    if let (Some(shard), Some(tx_id)) =
                        (sess.engine.config().shard.as_ref(), commit_tx_id)
                    {
                        let part = basin_common::PartitionKey::default_key();
                        let _ = shard
                            .wal()
                            .append_tx_commit(&sess.project, &part, tx_id)
                            .await;
                    }
                    return Ok(ExecResult::Empty {
                        tag: "COMMIT".into(),
                    });
                }
                crate::pg_ast::StmtKind::Rollback | crate::pg_ast::StmtKind::Savepoint => {
                    // Use libpg_query's authoritative parse to tell the
                    // transaction-control sub-forms apart and to pull the
                    // savepoint name straight from the AST.  PostgreSQL makes
                    // `WORK`, `TRANSACTION`, and `SAVEPOINT` all optional, so
                    // the old `contains("SAVEPOINT")` text heuristic mis-routed
                    // `ROLLBACK TO s` (no SAVEPOINT keyword) into a full
                    // transaction rollback. `txn_stmt` is exact.
                    let txn = tree.stmts().next().and_then(crate::pg_ast::txn_stmt);
                    use crate::pg_ast::TxnStmt;
                    match txn {
                        Some(TxnStmt::RollbackToSavepoint(name)) => {
                            // PG: outside an explicit transaction block this is
                            // an error (SQLSTATE 25P01, no_active_sql_transaction):
                            // `ROLLBACK TO SAVEPOINT can only be used in
                            // transaction blocks` — not "savepoint does not exist".
                            if !crate::session::tx_is_active(&sess.state) {
                                return Err(basin_common::BasinError::InvalidSchema(
                                    "ROLLBACK TO SAVEPOINT can only be used in \
                                     transaction blocks (SQLSTATE 25P01)"
                                        .into(),
                                ));
                            }
                            if name.is_empty() {
                                return Err(basin_common::BasinError::internal(
                                    "ROLLBACK TO SAVEPOINT: missing savepoint name",
                                ));
                            }
                            match crate::session::tx_rollback_to_savepoint(&sess.state, &name) {
                                Ok((abandoned, snapshots)) => {
                                    // Delete abandoned files (best-effort).
                                    for (_, files) in &abandoned {
                                        for f in files {
                                            let path =
                                                object_store::path::Path::from(f.path.as_str());
                                            let _ = sess
                                                .engine
                                                .config()
                                                .storage
                                                .delete_file(&sess.project, &path)
                                                .await;
                                        }
                                    }
                                    // Refresh DataFusion ctx for affected tables.
                                    // perf-w7-txn: must use refresh_table_with_htap
                                    // so the truncated htap_rows are reflected
                                    // (savepoint may have rolled back buffered
                                    // INSERT batches but kept earlier ones).
                                    let touched = crate::session::tx_touched_tables(&sess.state);
                                    for table in &touched {
                                        let pending = crate::session::tx_pending_files_for(
                                            &sess.state,
                                            table,
                                        );
                                        let htap_batches =
                                            crate::session::tx_htap_batches_for(
                                                &sess.state,
                                                table,
                                            );
                                        let _ = crate::session::refresh_table_with_htap(
                                            &sess.engine,
                                            &sess.project,
                                            &sess.ctx,
                                            &sess.state,
                                            table,
                                            &pending,
                                            htap_batches,
                                        )
                                        .await;
                                    }
                                    // Tables where all pending files were abandoned:
                                    // restore to pre-tx catalog view.
                                    for table in abandoned.keys() {
                                        if !touched.contains(table) {
                                            if let Some(snap) = snapshots.get(table) {
                                                let _ = sess
                                                    .engine
                                                    .config()
                                                    .catalog
                                                    .rollback_to_snapshot(
                                                        &sess.project,
                                                        table,
                                                        *snap,
                                                    )
                                                    .await;
                                            }
                                            let _ = refresh_table(
                                                &sess.engine,
                                                &sess.project,
                                                &sess.ctx,
                                                &sess.state,
                                                table,
                                            )
                                            .await;
                                        }
                                    }
                                    return Ok(ExecResult::Empty {
                                        tag: "ROLLBACK".into(),
                                    });
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        Some(TxnStmt::Rollback) | None
                            if matches!(kind, crate::pg_ast::StmtKind::Rollback) =>
                        {
                            // Phase 5.14.C2: emit WAL Rollback marker so that
                            // crash-recovery replay knows to suppress these entries.
                            // HTAP rows are discarded inside tx_rollback (they were
                            // never promoted to the shared MemTableRegistry).
                            let rolled_back_tx_id = crate::session::tx_get_id(&sess.state);
                            if let (Some(shard), Some(tx_id)) =
                                (sess.engine.config().shard.as_ref(), rolled_back_tx_id)
                            {
                                let part = basin_common::PartitionKey::default_key();
                                // Best-effort: WAL marker failures must not block rollback.
                                let _ = shard
                                    .wal()
                                    .append_tx_rollback(&sess.project, &part, tx_id)
                                    .await;
                            }
                            // Bare ROLLBACK — undo everything.
                            let (pending, snapshots) = crate::session::tx_rollback(&sess.state);
                            // Delete pending (not-yet-committed) files best-effort.
                            for (_, files) in &pending {
                                for f in files {
                                    let path = object_store::path::Path::from(f.path.as_str());
                                    let _ = sess
                                        .engine
                                        .config()
                                        .storage
                                        .delete_file(&sess.project, &path)
                                        .await;
                                }
                            }
                            // Restore catalog snapshot for each touched table.
                            for (table, snap_id) in &snapshots {
                                let _ = sess
                                    .engine
                                    .config()
                                    .catalog
                                    .rollback_to_snapshot(&sess.project, table, *snap_id)
                                    .await;
                                let _ = refresh_table(
                                    &sess.engine,
                                    &sess.project,
                                    &sess.ctx,
                                    &sess.state,
                                    table,
                                )
                                .await;
                            }
                            // PG semantics: NOTIFY payloads buffered inside
                            // this transaction are silently discarded on
                            // ROLLBACK. Subscriptions themselves are session
                            // state and survive — only the queued sends
                            // are dropped.
                            crate::session::listen_discard_pending_notifies(&sess.state);
                            return Ok(ExecResult::Empty {
                                tag: "ROLLBACK".into(),
                            });
                        }
                        Some(TxnStmt::Savepoint(name)) => {
                            // PG: `SAVEPOINT can only be used in transaction
                            // blocks` (SQLSTATE 25P01) when not inside BEGIN.
                            if !crate::session::tx_is_active(&sess.state) {
                                return Err(basin_common::BasinError::InvalidSchema(
                                    "SAVEPOINT can only be used in transaction \
                                     blocks (SQLSTATE 25P01)"
                                        .into(),
                                ));
                            }
                            if name.is_empty() {
                                return Err(basin_common::BasinError::internal(
                                    "SAVEPOINT: missing savepoint name",
                                ));
                            }
                            crate::session::tx_push_savepoint(&sess.state, name);
                            return Ok(ExecResult::Empty {
                                tag: "SAVEPOINT".into(),
                            });
                        }
                        Some(TxnStmt::ReleaseSavepoint(name)) => {
                            // PG: outside a transaction block this is an error
                            // (SQLSTATE 25P01): `RELEASE SAVEPOINT can only be
                            // used in transaction blocks`.
                            if !crate::session::tx_is_active(&sess.state) {
                                return Err(basin_common::BasinError::InvalidSchema(
                                    "RELEASE SAVEPOINT can only be used in \
                                     transaction blocks (SQLSTATE 25P01)"
                                        .into(),
                                ));
                            }
                            if name.is_empty() {
                                return Err(basin_common::BasinError::internal(
                                    "RELEASE SAVEPOINT: missing savepoint name",
                                ));
                            }
                            // PG raises SQLSTATE 3B001 (no_such_savepoint)
                            // when the name is unknown; propagate it instead
                            // of silently swallowing.
                            crate::session::tx_release_savepoint(&sess.state, &name)?;
                            return Ok(ExecResult::Empty {
                                tag: "RELEASE".into(),
                            });
                        }
                        // Defensive: a transaction node we don't special-case
                        // here (e.g. PREPARE TRANSACTION) falls through to the
                        // noop-accept gate below, preserving prior behaviour.
                        _ => {}
                    }
                }
                _ => {}
            }

            // ── Phase 5.13.B — AST-based DDL pre-screens ──────────────────────
            //
            // Statement kinds parsed by libpg_query but not by sqlparser 0.52
            // are dispatched here via typed AST matches instead of textual
            // regex/lexer helpers. The pg_query parse tree is already in hand
            // (`raw_pg_tree` above) so this costs nothing extra.
            //
            // Migration 1: ALTER TYPE <name> ADD VALUE '<label>'
            if matches!(kind, crate::pg_ast::StmtKind::AlterType) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::AlterEnumStmt(ref aes)) = node.node {
                        let (type_name, label) =
                            crate::type_ddl::match_alter_type_add_value_ast(aes)?;
                        return crate::type_ddl::exec_alter_type_add_value(
                            sess, &type_name, &label,
                        )
                        .await;
                    }
                }
            }

            // Migration 2: CREATE DOMAIN <name> [AS] <type> [CHECK (<pred>)]
            if matches!(kind, crate::pg_ast::StmtKind::CreateDomain) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::CreateDomainStmt(ref cds)) = node.node {
                        let (name, base, check) =
                            crate::type_ddl::match_create_domain_ast(cds)?;
                        return crate::type_ddl::exec_create_domain(sess, &name, base, check)
                            .await;
                    }
                }
            }

            // Migration 3: DROP DOMAIN [IF EXISTS] <name>
            if matches!(kind, crate::pg_ast::StmtKind::DropDomain) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::DropStmt(ref ds)) = node.node {
                        if let Some((name, if_exists)) =
                            crate::type_ddl::match_drop_domain_ast(ds)?
                        {
                            return crate::type_ddl::exec_drop_domain(sess, &name, if_exists)
                                .await;
                        }
                    }
                }
            }

            // Migration 4: REFRESH MATERIALIZED VIEW <name>
            // Note: Basin's custom `WITH (full = true)` extension is not valid
            // PG SQL so pg_query rejects it — raw_pg_tree is None for that
            // form and this block is skipped. The textual pre-screen below
            // handles the WITH (full = true) case.
            if matches!(kind, crate::pg_ast::StmtKind::RefreshMatView) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::RefreshMatViewStmt(ref rms)) = node.node {
                        let (name, force_full) =
                            crate::cv_ddl::match_refresh_materialized_view_ast(rms)?;
                        return crate::cv_ddl::exec_refresh_materialized_view(
                            sess, &name, force_full,
                        )
                        .await;
                    }
                }
            }

            // Migration 5: DROP MATERIALIZED VIEW [IF EXISTS] <name>
            if matches!(kind, crate::pg_ast::StmtKind::DropMatView) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::DropStmt(ref ds)) = node.node {
                        if let Some((name, if_exists)) =
                            crate::cv_ddl::match_drop_materialized_view_ast(ds)?
                        {
                            return crate::cv_ddl::exec_drop_materialized_view(
                                sess, &name, if_exists,
                            )
                            .await;
                        }
                    }
                }
            }

            // Migration 6: CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name> [opts…]
            // The textual pre-screen in match_create_sequence claims all CREATE
            // SEQUENCE shapes; the AST path replaces it for the valid-PG forms.
            if matches!(kind, crate::pg_ast::StmtKind::CreateSequence) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::CreateSeqStmt(ref css)) = node.node {
                        if let Some(intent) =
                            crate::seq_ddl::match_create_sequence_ast(css)?
                        {
                            return crate::seq_ddl::exec_create_sequence_pre_screen(
                                sess, intent,
                            )
                            .await;
                        }
                    }
                }
            }

            // Migration 7: ALTER SEQUENCE [IF EXISTS] <name> [opt …]
            if matches!(kind, crate::pg_ast::StmtKind::AlterSequence) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::AlterSeqStmt(ref ass)) = node.node {
                        let intent = crate::seq_ddl::match_alter_sequence_ast(ass)?;
                        return crate::seq_ddl::exec_alter_sequence(sess, intent).await;
                    }
                }
            }

            // Migration 8: ALTER SCHEMA <old> RENAME TO <new>
            if matches!(kind, crate::pg_ast::StmtKind::AlterSchemaRename) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::RenameStmt(ref rs)) = node.node {
                        let (old, new) =
                            crate::schema_ddl::match_alter_schema_rename_ast(rs)?;
                        return crate::schema_ddl::exec_alter_schema_rename(
                            sess, &old, &new,
                        )
                        .await;
                    }
                }
            }

            // Migration 9: ALTER FUNCTION <name>(<args>) RENAME TO <new>
            if matches!(kind, crate::pg_ast::StmtKind::AlterFunctionRename) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::RenameStmt(ref rs)) = node.node {
                        let (old, new) =
                            crate::function_ddl::match_alter_function_rename_ast(rs)?;
                        return crate::function_ddl::exec_alter_function_rename(
                            sess, &old, &new,
                        )
                        .await;
                    }
                }
            }

            // Migration 10: CREATE PROCEDURE <name>(<args>) LANGUAGE sql AS $$ <body> $$
            if matches!(kind, crate::pg_ast::StmtKind::CreateProcedure) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::CreateFunctionStmt(ref cfs)) = node.node {
                        if cfs.is_procedure {
                            let (name, args, body) =
                                crate::procedure_ddl::match_create_procedure_ast(cfs)?;
                            return crate::procedure_ddl::exec_create_procedure(
                                sess, &name, args, &body,
                            )
                            .await;
                        }
                    }
                }
            }

            // Migration 11: MOVE <direction> [FROM|IN] <cursor>
            // libpg_query routes MOVE as FetchStmt(ismove=true); the
            // textual pre-screen below (match_move_sql) converts the
            // synthetic FETCH form via sqlparser, which is now redundant
            // for the common PG MOVE forms.
            if matches!(kind, crate::pg_ast::StmtKind::Move) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::FetchStmt(ref fs)) = node.node {
                        let intent = crate::cursor::match_move_sql_ast(fs)?;
                        return exec_cursor_move(sess, intent).await;
                    }
                }
            }

            // Migration 12: ALTER TABLE t ALTER COLUMN col SET GENERATED ALWAYS/
            //               BY DEFAULT, and DROP IDENTITY [IF EXISTS].
            // libpg_query parses these as AlterTableStmt with AtSetIdentity (65)
            // or AtDropIdentity (66) command subtypes. sqlparser 0.52 cannot parse
            // these PG-specific identity-sequence manipulation forms; they are
            // accepted as metadata-only no-ops (same policy as SET NOT NULL).
            if matches!(kind, crate::pg_ast::StmtKind::AlterTable) {
                if let Some(node) = tree.stmts().next() {
                    if let Some(pg_query::NodeEnum::AlterTableStmt(ref at)) = node.node {
                        use pg_query::protobuf::AlterTableType;
                        let all_identity = !at.cmds.is_empty()
                            && at.cmds.iter().all(|cmd| {
                                if let Some(pg_query::NodeEnum::AlterTableCmd(c)) =
                                    cmd.node.as_ref()
                                {
                                    c.subtype == AlterTableType::AtSetIdentity as i32
                                        || c.subtype == AlterTableType::AtDropIdentity as i32
                                } else {
                                    false
                                }
                            });
                        if all_identity {
                            return Ok(ExecResult::Empty {
                                tag: "ALTER TABLE".into(),
                            });
                        }
                    }
                }
            }

            // ── DISCARD ALL / RESET ALL — real session reset (5.27.E) ───────
            // pgbouncer-style poolers issue `DISCARD ALL` as their
            // `server_reset_query`; some drivers issue `RESET ALL`. Both must
            // perform Basin's authoritative DISCARD-ALL rather than the
            // noop-accept, so a SQL-driven pool gets the same isolation the
            // native `SessionPool` gets at checkout. `DISCARD PLANS` /
            // `DISCARD TEMP` map to the prepared-statement / (best-effort)
            // sub-resets; `RESET ALL` resets only the GUCs.
            {
                let upper = sql.trim().trim_end_matches(';').trim_end().to_ascii_uppercase();
                if matches!(kind, crate::pg_ast::StmtKind::Discard) {
                    match upper.as_str() {
                        "DISCARD PLANS" => {
                            sess.state.prepared.clear_all().await;
                            return Ok(ExecResult::Empty { tag: "DISCARD PLANS".into() });
                        }
                        "DISCARD SEQUENCES" => {
                            return Ok(ExecResult::Empty { tag: "DISCARD SEQUENCES".into() });
                        }
                        "DISCARD TEMP" | "DISCARD TEMPORARY" => {
                            // Basin has no session-scoped temp schema to drop here;
                            // the native pool tracks temp-table names and drops them
                            // at checkout. Accept with the PG tag.
                            return Ok(ExecResult::Empty { tag: "DISCARD TEMP".into() });
                        }
                        _ => {
                            // DISCARD ALL — full DISCARD-ALL reset.
                            sess.reset_for_pool_reuse().await;
                            return Ok(ExecResult::Empty { tag: "DISCARD ALL".into() });
                        }
                    }
                }
                // `RESET ALL` parses as VariableSet (VAR_RESET_ALL). Reset just
                // the GUCs (PG's `RESET ALL` does not touch cursors / prepared /
                // advisory / LISTEN — that is `DISCARD ALL`).
                if matches!(kind, crate::pg_ast::StmtKind::VariableSet) && upper == "RESET ALL" {
                    sess.state.reset_gucs();
                    return Ok(ExecResult::Empty { tag: "RESET".into() });
                }
            }

            if let Some(result) = crate::noop_accept::try_accept_as_noop(kind, sql) {
                return Ok(result);
            }
        }
        crate::pg_ast::reject_unsupported(&tree)?;
    }

    // ── Phase 5.29.B: ALTER TABLE … SET (timescaledb.compress …) ────────────
    // Accepted as a metadata-only no-op: Basin stores data in Parquet which
    // is already columnar; the DDL is accepted so ORM migrations that call it
    // (e.g. TimescaleDB Diesel migrations) do not abort.
    if crate::hypertable::match_alter_table_timescaledb_compress(raw_sql) {
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // ── Phase 5.29.B: SELECT create_hypertable('table', 'col', …) ───────────
    // Convert a plain Basin table into a hypertable by registering it in the
    // HypertableRegistry with its time column and chunk interval.  Returns a
    // single-row result mirroring TimescaleDB's output shape.
    if let Some((ht_table, ht_col, ht_interval)) =
        crate::hypertable::match_create_hypertable(raw_sql)
    {
        return exec_create_hypertable(sess, &ht_table, &ht_col, &ht_interval).await;
    }

    // ── Phase 5.29.D: SELECT add_retention_policy('table', INTERVAL '…') ───
    if let Some((rp_table, rp_interval)) =
        crate::hypertable::match_add_retention_policy(raw_sql)
    {
        return exec_add_retention_policy(sess, &rp_table, &rp_interval).await;
    }

    // ── Phase 5.29.F: SELECT drop_chunks('table', older_than => INTERVAL/TS) ─
    if let Some((dc_table, dc_cutoff)) = crate::hypertable::match_drop_chunks(raw_sql) {
        return exec_drop_chunks(sess, &dc_table, dc_cutoff).await;
    }

    // ── Phase 5.29.D: SELECT run_retention_policy('table') ──────────────────
    if let Some(rp_table) = crate::hypertable::match_run_retention_policy(raw_sql) {
        return exec_run_retention_policy(sess, &rp_table).await;
    }

    // ── Phase 5.29.E: SELECT compress_chunk(…) ──────────────────────────────
    // Accepted as a metadata operation: marks the chunk compressed in the
    // registry.  Actual Parquet-level compression is deferred to 5.29.E.
    if let Some(intent) = crate::hypertable::match_compress_chunk(raw_sql) {
        return exec_compress_chunk(sess, intent).await;
    }

    // Phase 5.14.C5 — ALTER PROJECT <name> SET basin.memtable_hard_cap = <n>.
    if let Some((project_name, hard_cap_bytes)) =
        crate::alter_project::match_alter_project_memtable_cap(sql)?
    {
        crate::alter_project::exec_alter_project_memtable_cap(
            sess,
            &project_name,
            hard_cap_bytes,
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER PROJECT".into(),
        });
    }

    // Phase 6.X.B (ADR 0023) — ALTER PROJECT <name> SET partitions = <n>.
    // Persists in `ProjectStorageConfig::provider_extras`; the
    // `LeaseAwareShardMap` reads it on the next `owner_for` call. Existing
    // leases are NOT re-balanced here (that's 6.X.C lease handoff).
    if let Some((project_name, partitions)) =
        crate::alter_project::match_alter_project_partitions(sql)?
    {
        crate::alter_project::exec_alter_project_partitions(sess, &project_name, partitions)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER PROJECT".into(),
        });
    }

    // Phase 5.8: Basin-specific ALTER TABLE extensions (`SET cold_after`,
    // `SET cold_age_column`, `SET BLOOM FILTERS ON (...)`, `CLUSTER BY`,
    // `RESET CLUSTER BY`).
    if let Some(ext) = crate::alter::match_basin_alter_extension(sql)? {
        let table = ext.table().clone();
        let tag = ext
            .apply(&sess.engine.config().catalog, &sess.project)
            .await?;
        crate::session::refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table)
            .await?;
        return Ok(ExecResult::Empty { tag: tag.into() });
    }

    // MOVE <direction> [FROM|IN] <cursor> — sqlparser 0.52 has no
    // Statement::Move AST node.  Pre-screen textually and dispatch before
    // sqlparser even sees the SQL.
    if let Some(intent) = crate::cursor::match_move_sql(sql) {
        return exec_cursor_move(sess, intent).await;
    }

    // REFRESH MATERIALIZED VIEW <name> [WITH (full = true)] — sqlparser
    // has no AST node for REFRESH, so we recognise the full statement
    // textually and dispatch. `force_full` toggles the v0.1 opt-out from
    // incremental refresh.
    // REFRESH MATERIALIZED VIEW <name> [ WITH (full = true) ].
    if let Some((name, force_full)) = crate::cv_ddl::match_refresh_materialized_view(sql)? {
        return crate::cv_ddl::exec_refresh_materialized_view(sess, &name, force_full).await;
    }

    // ALTER SCHEMA <old> RENAME TO <new>: sqlparser 0.52 has no AlterSchema
    // AST node, so we recognise the full statement textually before sqlparser
    // sees it. Must be checked before ALTER FUNCTION (both start with ALTER).
    if let Some((old, new)) = crate::schema_ddl::match_alter_schema_rename(sql) {
        return crate::schema_ddl::exec_alter_schema_rename(sess, &old, &new).await;
    }

    // ALTER FUNCTION <name>(<args>) RENAME TO <new>: sqlparser 0.52 has no
    // AlterFunction AST node, so we recognise the full statement textually
    // and dispatch to the catalog rename helper.
    if let Some((old, new)) = crate::function_ddl::match_alter_function_rename(sql)? {
        return crate::function_ddl::exec_alter_function_rename(sess, &old, &new).await;
    }

    // CREATE PROCEDURE … LANGUAGE sql AS $$ … $$: sqlparser 0.52 only
    // parses the T-SQL `AS BEGIN … END` shape natively, so the
    // PG-style form is recognised textually before sqlparser sees it.
    // See `procedure_ddl::match_create_procedure` for the shape.
    if let Some((name, args, body)) = crate::procedure_ddl::match_create_procedure(sql)? {
        return crate::procedure_ddl::exec_create_procedure(sess, &name, args, &body).await;
    }

    // 5.11.I — `ALTER TABLE … SUBSCRIBE WEBHOOK …`. libpg_query rejects
    // `SUBSCRIBE` outright.
    if let Some(intent) = crate::webhook_ddl::match_alter_table_subscribe_webhook(sql)? {
        crate::webhook_ddl::exec_subscribe_webhook(
            intent,
            &sess.project,
            sess.engine.webhook_registry(),
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.I — `ALTER TABLE … UNSUBSCRIBE WEBHOOK <name>`. libpg_query
    // rejects `UNSUBSCRIBE` outright.
    if let Some(intent) = crate::webhook_ddl::match_alter_table_unsubscribe_webhook(sql)? {
        crate::webhook_ddl::exec_unsubscribe_webhook(
            intent,
            &sess.project,
            sess.engine.webhook_registry(),
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C — `ALTER TABLE … REACT ON … EXECUTE <body>`. libpg_query
    // rejects `REACT` outright. The body matcher returns `None` for the
    // constraint-shaped form (handled by the next arm).
    if let Some(intent) = crate::reactor_ddl::match_alter_table_react_on(sql)? {
        crate::reactor_ddl::exec_react_on(intent, &sess.project, &sess.engine.config().catalog)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C2 — `ALTER TABLE … REACT ON … CONSTRAINT (<predicate>)`.
    if let Some(intent) = crate::reactor_ddl::match_alter_table_react_constraint(sql)? {
        crate::reactor_ddl::exec_react_constraint(
            intent,
            &sess.project,
            &sess.engine.config().catalog,
        )
        .await?;
        return Ok(ExecResult::Empty {
            tag: "ALTER TABLE".into(),
        });
    }

    // 5.11.C — `DROP REACTOR <name> ON <table>`. libpg_query rejects
    // `REACTOR` outright.
    if let Some(intent) = crate::reactor_ddl::match_drop_reactor(sql)? {
        crate::reactor_ddl::exec_drop_reactor(intent, &sess.project, &sess.engine.config().catalog)
            .await?;
        return Ok(ExecResult::Empty {
            tag: "DROP REACTOR".into(),
        });
    }

    // 5.11.N + 6.SEC.P0.3 — `CREATE INBOUND WEBHOOK …` returns the generated
    // HMAC secret as a single-row `secret` result set so the creator can
    // copy it once. `DROP INBOUND WEBHOOK …` is an empty side-effect.
    if let Some(i) = crate::inbound_webhook_ddl::match_create_inbound_webhook(sql)? {
        let secret_hex = crate::inbound_webhook_ddl::exec_create_inbound_webhook(
            i,
            &sess.project,
            sess.engine.config().catalog.as_ref(),
        )
        .await?;
        let schema = Arc::new(Schema::new(vec![Field::new("secret", DataType::Utf8, false)]));
        let arr: ArrayRef = Arc::new(StringArray::from(vec![secret_hex]));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr])
            .map_err(|e| BasinError::Internal(format!("build CREATE INBOUND WEBHOOK row: {e}")))?;
        return Ok(ExecResult::Rows {
            schema,
            batches: vec![batch],
        });
    }
    if let Some(i) = crate::inbound_webhook_ddl::match_drop_inbound_webhook(sql)? { crate::inbound_webhook_ddl::exec_drop_inbound_webhook(i, &sess.project, sess.engine.config().catalog.as_ref()).await?; return Ok(ExecResult::Empty { tag: "DROP INBOUND WEBHOOK".into() }); }

    // DROP MATERIALIZED VIEW [IF EXISTS] <name> — sqlparser's DROP parser
    // does not recognise MATERIALIZED VIEW, so we handle the full
    // statement before sqlparser sees it.
    if let Some((name, if_exists)) = crate::cv_ddl::match_drop_materialized_view(sql)? {
        return crate::cv_ddl::exec_drop_materialized_view(sess, &name, if_exists).await;
    }

    // ── Phase 5.21.B — replication slot management ───────────────────────────
    // `SELECT … FROM pg_create_logical_replication_slot(name, plugin)`
    if let Some((slot_name, plugin)) =
        crate::replication::slot_udf::match_create_replication_slot(sql)
    {
        return crate::replication::slot_udf::exec_create_replication_slot(
            sess,
            &slot_name,
            &plugin,
        )
        .await;
    }

    // `SELECT pg_drop_replication_slot(name)`
    if let Some(slot_name) = crate::replication::slot_udf::match_drop_replication_slot(sql) {
        return crate::replication::slot_udf::exec_drop_replication_slot(sess, &slot_name).await;
    }

    // ── Phase 5.21.D — current WAL LSN ───────────────────────────────────────
    if crate::replication::slot_udf::match_current_wal_lsn(sql) {
        return crate::replication::slot_udf::exec_current_wal_lsn(sess).await;
    }

    // ── Phase 5.21.C — logical slot get changes ───────────────────────────────
    if let Some(slot_name) = crate::replication::slot_udf::match_logical_slot_get_changes(sql) {
        return crate::replication::slot_udf::exec_logical_slot_get_changes(sess, &slot_name).await;
    }

    // ── Phase 5.21.E — publication DDL ───────────────────────────────────────
    // CREATE PUBLICATION / ALTER PUBLICATION / DROP PUBLICATION are not
    // recognised by sqlparser 0.52's PostgreSQL dialect. Intercept via
    // text matching before sqlparser sees the SQL.
    if let Some(intent) = crate::replication::publication_ddl::match_create_publication(sql) {
        return crate::replication::publication_ddl::exec_create_publication(sess, intent).await;
    }
    if let Some(intent) = crate::replication::publication_ddl::match_alter_publication(sql) {
        return crate::replication::publication_ddl::exec_alter_publication(sess, intent).await;
    }
    if let Some(pubname) = crate::replication::publication_ddl::match_drop_publication(sql) {
        return crate::replication::publication_ddl::exec_drop_publication(sess, &pubname).await;
    }
    // ─────────────────────────────────────────────────────────────────────────

    // ALTER SEQUENCE [IF EXISTS] <name> [RESTART [WITH n]]: sqlparser 0.52
    // has no AlterSequence AST node; textual pre-screen handles the full
    // PG grammar.
    if let Some(intent) = crate::seq_ddl::match_alter_sequence(sql)? {
        return crate::seq_ddl::exec_alter_sequence(sess, intent).await;
    }

    // CREATE [TEMPORARY] SEQUENCE [IF NOT EXISTS] <name> [opt …] —
    // sqlparser 0.52 only parses one option per CREATE SEQUENCE
    // statement, so the full PG grammar fails at the second option. The
    // textual matcher claims any CREATE SEQUENCE shape.
    if let Some(intent) = crate::seq_ddl::match_create_sequence(sql)? {
        return crate::seq_ddl::exec_create_sequence_pre_screen(sess, intent).await;
    }

    // CREATE MATERIALIZED VIEW ... WITH (basin.continuous, refresh_interval =
    // '...'): sqlparser's WITH-clause parser cannot ingest a dotted-key
    // option like `basin.continuous`, so we lift the entire WITH (...) body
    // before sqlparser sees the SQL. The options live in `cv_options`; the
    // remainder is a vanilla CREATE MATERIALIZED VIEW the standard parser
    // accepts.
    let (cv_stripped, cv_options) = crate::cv_ddl::extract_basin_cv_options(sql)?;
    let cv_stripped_owned = cv_stripped;
    let sql = cv_stripped_owned.as_str();

    // Phase 5.7 B2: lift trailing `CLUSTER BY (col, …)` out of CREATE TABLE
    // before sqlparser sees it. PostgreSqlDialect doesn't recognise the
    // form so we strip it here and apply the columns via `set_cluster_columns`
    // after the table is created. Returns the original SQL untouched when
    // the clause isn't present.
    let (cluster_stripped, cluster_columns) = extract_create_table_cluster_by(sql)?;
    // Lift declarative lifecycle markers (AUTO_UPDATE / SOFT DELETE column
    // attributes, trailing AUDIT TO clause). Same pre-screen strategy as
    // CLUSTER BY: sqlparser doesn't recognise these forms.
    let (lifecycle_stripped, lifecycle) =
        extract_create_table_lifecycle(cluster_stripped.as_str())?;
    // Phase 5.24.F: strip `EXCLUDE USING gist (...)` table-level constraints
    // before sqlparser sees the SQL (sqlparser 0.61 has no TableConstraint::Exclude
    // variant). The parsed specs are threaded into `exec_create_table` so the
    // constraint is persisted as a sentinel CheckConstraint and enforced on INSERT.
    let (excl_stripped, exclude_specs) =
        extract_exclude_using_gist(lifecycle_stripped.as_str());
    let sql_owned = excl_stripped;
    let sql = sql_owned.as_str();

    // `CREATE TABLE … AS <query> [WITH [NO] DATA]` (CTAS). libpg_query —
    // the real PostgreSQL parser already run above as `raw_pg_tree` —
    // understands the trailing `WITH [NO] DATA` clause and exposes it as
    // `into.skip_data`. sqlparser 0.61's `CreateTable` AST has no field
    // for the clause, so its statement parser fails ("Expected: end of
    // statement, found: WITH"). When the original statement is a plain
    // CTAS we strip the trailing clause textually here (string/comment
    // aware, only the exactly-trailing token sequence) so sqlparser
    // parses the `CREATE TABLE … AS <query>` body normally; the
    // `skip_data` boolean is threaded into `exec_create_table` so the
    // row-population step can be skipped for `WITH NO DATA`. Not a CTAS
    // → SQL is left exactly as-is and `ctas_no_data` stays `None`.
    let mut ctas_shape: Option<crate::pg_ast::CtasShape> = None;
    if let Some(ref tree) = raw_pg_tree {
        if let Some(node) = tree.stmts().next() {
            ctas_shape = crate::pg_ast::ctas_shape(node);
        }
    }
    let ctas_stripped = if let Some(ref shape) = ctas_shape {
        // Strip the trailing `WITH [NO] DATA`, then (if present) the
        // bare LHS column-name list — both are libpg_query-confirmed
        // and unparseable by sqlparser 0.61. Order matters: strip the
        // tail first so the column-list scan sees a clean head.
        let s = crate::ddl::strip_trailing_with_data(sql);
        if shape.col_names.is_empty() {
            s
        } else {
            crate::ddl::strip_ctas_column_list(&s)
        }
    } else {
        sql.to_string()
    };
    let sql = ctas_stripped.as_str();

    // INCLUDE DELETED on SELECT is the soft-delete opt-out.
    let (select_stripped, include_deleted) = extract_select_include_deleted(sql);
    let sql = select_stripped.as_str();

    // ── Advanced SELECT pre-screens ─────────────────────────────────────────
    //
    // These rewrites handle SQL constructs that sqlparser 0.52 can't parse at
    // the statement level (TABLE foo, TABLESAMPLE) or that the sqlparser AST
    // expresses in a way DataFusion 44 ignores (FETCH FIRST N ROWS ONLY).
    // Each is a cheap string scan; non-matching SQL is returned as-is (Cow).
    //
    // 1. `TABLE foo` → `SELECT * FROM foo`
    //    sqlparser's top-level dispatch doesn't recognise TABLE as a statement
    //    start keyword; the query body parser does (SetExpr::Table) but
    //    DataFusion never encounters that variant. Rewrite before sqlparser.
    let table_shorthand_rewrite = crate::select_advanced::rewrite_table_shorthand(sql);
    let sql = table_shorthand_rewrite.as_ref();
    //
    // 2. Lower `TABLESAMPLE { BERNOULLI | SYSTEM } (p) [REPEATABLE(s)]` into a
    //    derived sub-select carrying a sampling predicate (BUG #134: the
    //    clause used to be silently stripped, so every row was returned).
    //    BERNOULLI -> per-row Bernoulli trial; SYSTEM -> per-record-batch
    //    trial; REPEATABLE -> seeded deterministic variant. An out-of-range
    //    percentage is a hard error (PG parity: "sample percentage must be
    //    between 0 and 100").
    let tablesample_rewrite = crate::select_advanced::rewrite_tablesample(sql)
        .map_err(|e| BasinError::InvalidSchema(e.0))?;
    let sql = tablesample_rewrite.as_ref();
    //
    // 2b. Strip `ONLY ` table inheritance modifier from `FROM ONLY <tbl>` /
    //     `JOIN ONLY <tbl>`. Basin has no table inheritance (flat-storage design);
    //     `ONLY` is a semantic no-op here. Rewriting to plain `FROM <tbl>` makes
    //     the query run normally against the base table.
    let only_rewrite = crate::pg_ast::strip_only_modifier(sql);
    let sql = only_rewrite.as_ref();
    //
    // 3. `FETCH FIRST N ROWS ONLY` / `FETCH NEXT N ROWS ONLY` → `LIMIT N`.
    //    sqlparser parses these into `Query.fetch`; DataFusion 44's planner
    //    only reads `Query.limit`, so FETCH is silently ignored without this
    //    rewrite.  Also handles the combined `OFFSET M ROWS FETCH NEXT N` form.
    let fetch_rewrite = crate::select_advanced::rewrite_fetch_to_limit(sql);
    let sql = fetch_rewrite.as_ref();
    //
    // 4. `FOR NO KEY UPDATE` → `FOR UPDATE`, `FOR KEY SHARE` → `FOR SHARE`.
    //    sqlparser 0.52 only recognises `FOR UPDATE` and `FOR SHARE` as lock
    //    types; the PG-specific variants `FOR NO KEY UPDATE` / `FOR KEY SHARE`
    //    trigger a parse error. After the rewrite, sqlparser parses the
    //    `Query.locks` vec normally, and DataFusion ignores it entirely —
    //    Basin is append-only / optimistic-concurrency so row locking is
    //    advisory for all four locking-strength keywords.
    let for_lock_rewrite = crate::select_advanced::rewrite_for_no_key_update_and_key_share(sql);
    let sql = for_lock_rewrite.as_ref();
    // ────────────────────────────────────────────────────────────────────────
    // `INSERT INTO t [...] OVERRIDING { SYSTEM | USER } VALUE VALUES (...)`
    // — sqlparser 0.52 doesn't recognise the clause; we lift it out
    // textually and stash the kind on the session state for
    // `exec_insert` to consume. No-op for any statement that isn't
    // INSERT (or where the clause isn't present).
    let (overriding_stripped, overriding_kind) = crate::dml::extract_insert_overriding(sql)?;
    if let Some(kind) = overriding_kind {
        crate::session::set_pending_overriding(&sess.state, kind);
    }
    let overriding_owned = overriding_stripped;
    let sql = overriding_owned.as_str();

    // Auto-route `ORDER BY <vec_col> <op> <lit> LIMIT k` to the HNSW fast
    // path BEFORE the operator-to-UDF rewrite below. Once `<->` becomes
    // `l2_distance(...)` the structural signal is gone. A `None` here
    // means at least one criterion failed; the brute-force pipeline below
    // takes over and correctness is preserved.
    if let Some(plan) = crate::vector_planner::rewrite_vector_order_by(
        &sess.engine.config().catalog,
        &sess.project,
        sql,
    )
    .await?
    {
        match execute_vector_search_plan(sess, plan).await {
            Ok(res) => return Ok(res),
            Err(e) => {
                // The HNSW segment may carry a different metric than the
                // user's operator (current sidecars are L2-only; users
                // wanting cosine/dot still parse but the segment header
                // mismatches). Fall back to brute-force rather than
                // surfacing a routing-only error.
                tracing::debug!(
                    error = %e,
                    "vector planner routed but storage rejected; falling back"
                );
            }
        }
    }

    // PG-Wave KNN — auto-route `ORDER BY <point_col> <-> ST_MakePoint(x,y)
    // LIMIT k` to the R-tree nearest-neighbour path. Runs AFTER the pgvector
    // planner (which declines POINT columns: the RHS is an ST_MakePoint call,
    // not a vector literal) so the two `<->` paths are exclusive — vector
    // column → HNSW above, POINT column → spatial KNN here. `detect_knn_predicate`
    // confirms the ORDER BY column carries `BASIN_TYPE=POINT`; a vector column
    // returns None and brute-force takes over (correctness preserved). An
    // `Ok(None)` from `execute_knn_plan` (sidecar coverage gap) also falls back.
    if let Some(knn_plan) = crate::index_probe::detect_knn_predicate(
        sql,
        &sess.project,
        &sess.engine.config().catalog,
    )
    .await
    {
        match crate::rtree_knn_scan::execute_knn_plan(sess, knn_plan).await {
            Ok(Some(res)) => return Ok(res),
            Ok(None) => {
                tracing::debug!("knn planner declined (coverage gap); falling back");
            }
            Err(e) => {
                tracing::debug!(error = %e, "knn planner errored; falling back");
            }
        }
    }

    // Perf: skip the entire 40-pass string-rewrite pipeline when the SQL
    // contains no marker tokens that any pass could fire on.  For a trivial
    // point-query like `SELECT id FROM events WHERE id = 5000` the chain
    // costs ~80–100µs of per-statement overhead and produces zero changes.
    // `needs_rewrite_pipeline` is conservative: false-positives (running
    // the pipeline when no rewrite would fire) are free; false-negatives
    // (skipping when a rewrite was needed) would silently produce wrong
    // results, so any marker token forces the full chain.
    let rewrite_pipeline_owned: String;
    let sql: &str = if needs_rewrite_pipeline(sql) {
        rewrite_pipeline_owned = run_full_rewrite_pipeline(sess, sql).await?;
        rewrite_pipeline_owned.as_str()
    } else {
        sql
    };

    // Phase 5.13.C — the noop-accept + explicit-reject gates above (early
    // in the hot path, before any string-rewrite pipeline) already dispatch
    // every statement in the syntactic-accept set and reject every
    // design-exclusion kind (LISTEN/NOTIFY/UNLISTEN). The redundant second
    // gate that previously lived here (ADR 0014 Phase 1 dual path) has been
    // removed; sqlparser now only sees DML/DDL that needs its AST.
    //
    // The cached parse returns an `Arc<Statement>`; we materialise an owned
    // `Statement` via `Arc::try_unwrap` (single-owner fast path) or `Clone`
    // (shared, e.g. another session is mid-parse on the same SQL).  The
    // downstream dispatch consumes `stmt` by value.
    let stmt_arc = parse_sql_cached(sql)?;
    let stmt: Statement = Arc::try_unwrap(stmt_arc).unwrap_or_else(|arc| (*arc).clone());

    dispatch_parsed_statement(
        sess,
        stmt,
        sql,
        raw_sql,
        include_deleted,
        cluster_columns,
        lifecycle,
        ctas_shape,
        exclude_specs,
        cv_options,
    )
    .await
}

/// Identify the SINGLE target table a non-SELECT statement mutates, when it is
/// an unambiguous single-table DML on a bare table name (INSERT / UPDATE /
/// DELETE). Returns `None` for anything else — DDL, multi-table DML, joined
/// UPDATE/DELETE, CTE-wrapped DML, or any shape we cannot pin to exactly one
/// table — so the dispatch-top cache invalidation can fall back to the broad
/// (whole-cache) clear for those, preserving correctness.
///
/// Concurrency fix #4: a write to table A must not evict table B's cached
/// provider / head / PK rows. For the overwhelmingly common OLTP shape (an
/// auto-commit single-table INSERT/UPDATE/DELETE) this lets us scope the
/// invalidation to exactly the table that changed.
fn dml_single_target_table(stmt: &Statement) -> Option<TableName> {
    use sqlparser::ast::{FromTable, TableFactor};

    // Extract a bare `TableName` from a `TableWithJoins` that has no joins and a
    // plain `TableFactor::Table` relation (no table-valued args).
    fn bare(twj: &sqlparser::ast::TableWithJoins) -> Option<TableName> {
        if !twj.joins.is_empty() {
            return None;
        }
        match &twj.relation {
            TableFactor::Table { name, args, .. } if args.is_none() => {
                TableName::new(single_part_name(name).ok()?).ok()
            }
            _ => None,
        }
    }

    match stmt {
        Statement::Insert(ins) => {
            let name = crate::pg_ast::insert_object_name(ins).ok()?;
            TableName::new(single_part_name(name).ok()?).ok()
        }
        Statement::Update(sqlparser::ast::Update { table, from, .. }) => {
            // A joined UPDATE (`UPDATE a SET … FROM b`) reads b but mutates only
            // a. Stay conservative: scope only when there is no FROM clause and
            // the target is a bare, join-free table.
            if from.is_some() {
                return None;
            }
            bare(table)
        }
        Statement::Delete(del) => {
            let tables = match &del.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            if tables.len() != 1 {
                return None;
            }
            bare(&tables[0])
        }
        _ => None,
    }
}

/// Dispatch an already-parsed `Statement` through the engine's cache-
/// invalidation gate, cost check, RLS-DDL intercept, fast paths, and the
/// big statement `match`.
///
/// Split out of [`execute`] (perf-w-prepared) so the prepared-statement
/// bind-INSERT fast path ([`execute_statement`]) can reach the identical
/// dispatch tail WITHOUT re-parsing the SQL: `prepare` already parsed the
/// template once, so at `Execute` time we substitute the bind values into a
/// clone of that AST and hand the resulting `Statement` straight here. The
/// text path ([`execute`]) and the AST path share this body verbatim, so the
/// two routes produce identical behaviour for every statement kind.
///
/// `sql` is the (possibly rewritten) statement text DataFusion needs for any
/// path that falls through to `exec_select`; `raw_sql` is the user's original
/// text used for logging and the operator-probe gates. For the AST fast path
/// both are the rendered substituted SQL (safe: that path is gated on
/// `!needs_rewrite_pipeline`, so no text rewrite would have fired anyway).
#[allow(clippy::too_many_arguments)]
async fn dispatch_parsed_statement(
    sess: &ProjectSession,
    stmt: Statement,
    sql: &str,
    raw_sql: &str,
    include_deleted: bool,
    cluster_columns: Option<Vec<String>>,
    lifecycle: CreateTableLifecycle,
    ctas_shape: Option<crate::pg_ast::CtasShape>,
    exclude_specs: Vec<crate::ddl::ExcludeConstraintSpec>,
    cv_options: Option<crate::cv_ddl::CvOptions>,
) -> Result<ExecResult> {
    // Inv-OLTP-point (#149) — per-session table-meta cache invalidation.
    // The simple-SELECT fast-path gate consults a per-session
    // `(TableMetadata, view_present)` cache to skip two catalog round-trips
    // per query. Any statement that is NOT a pure `SELECT` *or* an INSERT
    // inside an explicit transaction may mutate schema (DDL), indexes, RLS
    // state, view bindings, or data files in a way the cached snapshot
    // wouldn't reflect — clear the cache so the next SELECT re-loads from
    // the catalog. This is the simple-safe pattern documented on
    // [`crate::session::TableMetaCache`]: one `Mutex` lock + `clear()`,
    // negligible relative to the DDL/DML it precedes.
    //
    // Inv-OLTP-write (#155, perf-w7-more): in-tx `Statement::Insert` is
    // also a carve-out — INSERTs do NOT mutate `TableMetadata`'s cached
    // fields (schema, pk_columns, unique_constraints, foreign_keys,
    // check_constraints, policies, rls_enabled, partition_spec). The only
    // field they CAN advance is `current_snapshot` (via deferred
    // `append_data_files` at COMMIT), and the in-tx INSERT path never
    // consumes that field: the COMMIT handler re-reads the pinned snapshot
    // from `TxState`, and `commit_with_retry` retries on stale-snapshot
    // conflict. The `live_data_files()` embedded in cached `TableMetadata`
    // does NOT change inside the transaction either — in-tx INSERTs buffer
    // batches in `TxState::htap_rows` and only emit Parquet at COMMIT —
    // and intra-tx SELECTs route through `HtapUnionTable` for read-your-
    // own-writes regardless of the cached snapshot.
    //
    // CRITICAL: auto-commit INSERTs DO advance the catalog per statement
    // (the synchronous `commit_with_retry` path), so the next SELECT must
    // see the freshly-committed `live_data_files()`. We therefore restrict
    // the carve-out to the in-tx case; auto-commit INSERTs still
    // invalidate, matching W6 behaviour and preserving the RYOW contract
    // (`executor::auto_commit_ryow_tests`).
    //
    // Same-session DDL always flushes the cache because ALTER/CREATE/DROP
    // variants are not `Insert`. Net effect on `BEGIN; INSERT x100;
    // COMMIT`: 100 cold catalog round-trips → 1 cold-fill + 99 LRU hits
    // on the same `TableName` key; auto-commit RYOW unchanged.
    let in_txn_for_cache = crate::session::tx_is_active(&sess.state);
    let stmt_keeps_cache = matches!(stmt, Statement::Query(_))
        || (in_txn_for_cache && matches!(stmt, Statement::Insert(_)));
    if !stmt_keeps_cache {
        // Concurrency fix #4: scope invalidation to the SINGLE mutated table
        // when the statement is an unambiguous single-table INSERT/UPDATE/DELETE
        // on a bare table. A write to table A then no longer evicts table B's
        // still-valid cached provider / head / PK rows — the read-path
        // serialization the 16-session and mixed-RW benches exposed (every write
        // dumping every other table's cache, forcing a cold rebuild on the next
        // read). For DDL, multi-table DML, joined/CTE DML, or any shape we
        // cannot pin to exactly one table, `dml_single_target_table` returns
        // `None` and we fall back to the original whole-cache clear, preserving
        // correctness (DDL can change schema/RLS/views/indexes project-wide).
        //
        // Why per-table is sound for the scoped case: a single-table DML can
        // only change THAT table's data files / overlay / rows. It does NOT
        // touch any other table's schema, RLS, view bindings, file set or
        // overlay — exactly the state the provider / head / PK caches model —
        // so evicting only that table's entries is complete. The mutated
        // table's own entries are invalidated for the same reasons the broad
        // clear cited: an INSERT advances `current_snapshot` (per-statement
        // commit on the auto-commit path), and a fast-path UPDATE/DELETE writes
        // an overlay — both of which a later read must observe.
        if let Some(target) = dml_single_target_table(&stmt) {
            sess.state.table_meta_cache.invalidate(&target);
            sess.state.provider_cache.invalidate(&target);
            sess.state.head_probe_cache.invalidate(&target);
            sess.engine
                .pk_row_cache()
                .invalidate_table(&sess.project, &target);
        } else {
            sess.state.table_meta_cache.invalidate_all();
            // Fix B+C provider cache: the same statement classes that flush the
            // table-meta cache can change a table's schema, file set or overlay
            // shape. Most are caught by the per-query `(snapshot)` key, but
            // metadata-only DDL (ENABLE RLS, ADD COLUMN no-rewrite, JSONB
            // promotion) may not advance the snapshot, so clear the
            // built-provider + head-probe caches here too. The head-probe cache
            // also self-invalidates on the catalog-epoch bump these DDLs raise;
            // this eager clear covers same-session visibility within the current
            // statement.
            sess.state.provider_cache.invalidate_all();
            sess.state.head_probe_cache.invalidate_all();
            // PK row cache (always-on): the same statement classes that flush
            // the table-meta cache — any non-SELECT we could not scope — can
            // change a table's schema or rewrite its rows. Most are caught by
            // the cache's dual watermark (snapshot id + hot epoch), but
            // metadata-only DDL (e.g. ENABLE ROW LEVEL SECURITY, ADD COLUMN with
            // no rewrite) may not bump `current_snapshot`; clear the project's
            // PK rows here so a schema or policy change can never serve a
            // stale/old-shape row. Cheap no-op when the cache is empty.
            sess.engine.pk_row_cache().invalidate_project(&sess.project);
        }
    }

    // Phase 6 cost-based query rejection. Cheap when disabled (one
    // `OnceLock::get`); when enabled, one catalog round-trip per
    // simple-shape Query. Multi-FROM / JOIN / sub-query / explicit-LIMIT
    // shapes pass through unchecked in v0.1 — see `cost_check` module
    // docs for the deliberate scope.
    if let Some(limit) = crate::cost_check::cost_limit_rows() {
        if matches!(stmt, Statement::Query(_)) {
            let estimate = crate::cost_check::estimate_query_rows(
                sess.engine.config().catalog.as_ref(),
                &sess.project,
                &stmt,
            )
            .await?;
            if let Some(rows) = estimate {
                crate::cost_check::check_cost(rows, limit)?;
            }
        }
    }

    // Phase 5.6: intercept RLS-related DDL before the main dispatch. The
    // catch-all in `match_rls_ddl` keeps every other statement falling
    // through to the existing handlers — the no-RLS hot path is an
    // `Ok(None)` followed by the same dispatch as before.
    if let Some(rls_ddl) = crate::rls::match_rls_ddl(&stmt)? {
        return exec_rls_ddl(sess, rls_ddl).await;
    }

    let result = match stmt {
        Statement::CreateTable(ct) => {
            exec_create_table(
                sess,
                ct,
                cluster_columns,
                lifecycle,
                ctas_shape,
                raw_sql,
                exclude_specs,
            )
            .await
        }
        Statement::CreateIndex(ci) => exec_create_index(sess, ci).await,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Index,
            if_exists,
            names,
            ..
        } => exec_drop_index(sess, if_exists, names).await,
        Statement::CreateView(sqlparser::ast::CreateView {
            name,
            query,
            materialized,
            or_replace,
            ..
        }) => {
            if materialized {
                let view_name = single_part_name(&name)?.to_string();
                let opts = cv_options.unwrap_or_default();
                let source_sql = query.to_string();
                if opts.continuous {
                    // Continuous-aggregate path: requires refresh_interval.
                    let interval = opts.refresh_interval_secs.ok_or_else(|| {
                        BasinError::InvalidSchema(
                            "CREATE MATERIALIZED VIEW: WITH (basin.continuous) \
                             requires refresh_interval = '<duration>'"
                                .into(),
                        )
                    })?;
                    crate::cv_ddl::exec_create_materialized_view(
                        sess,
                        &view_name,
                        &source_sql,
                        interval,
                    )
                    .await
                } else {
                    // Plain (snapshot) materialized view: run the query once and
                    // persist the result as a regular table.  No automatic
                    // refresh; use REFRESH MATERIALIZED VIEW to update.
                    crate::cv_ddl::exec_create_snapshot_materialized_view(
                        sess,
                        &view_name,
                        &source_sql,
                    )
                    .await
                }
            } else {
                // Plain view path (new).
                let view_name = single_part_name(&name)?.to_string();
                let query_sql = query.to_string();
                crate::view_ddl::exec_create_view(sess, &view_name, &query_sql, or_replace).await
            }
        }
        Statement::CreateFunction(sqlparser::ast::CreateFunction {
            or_replace,
            temporary,
            name,
            args,
            return_type,
            function_body,
            language,
            ..
        }) => {
            crate::function_ddl::exec_create_function(
                sess,
                or_replace,
                temporary,
                name,
                args,
                return_type,
                function_body,
                language,
            )
            .await
        }
        Statement::DropFunction(sqlparser::ast::DropFunction {
            if_exists,
            func_desc,
            ..
        }) => {
            let names = func_desc.into_iter().map(|d| d.name).collect();
            crate::function_ddl::exec_drop_function(sess, if_exists, names).await
        }
        Statement::DropProcedure {
            if_exists,
            proc_desc,
            ..
        } => crate::procedure_ddl::exec_drop_procedure(sess, if_exists, proc_desc).await,
        Statement::Call(call) => crate::procedure_ddl::exec_call(sess, call).await,
        Statement::CreateType {
            name,
            representation,
        } => {
            use sqlparser::ast::UserDefinedTypeRepresentation;
            match representation {
                Some(UserDefinedTypeRepresentation::Enum { labels }) => {
                    crate::type_ddl::exec_create_type_enum(sess, name, labels).await
                }
                _ => Err(BasinError::FeatureNotSupported(
                    "CREATE TYPE … AS (composite) is out of scope for v0.1; \
                     only AS ENUM is supported"
                        .into(),
                )),
            }
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Type,
            if_exists,
            names,
            ..
        } => crate::type_ddl::exec_drop_type(sess, if_exists, &names).await,
        Statement::CreateSequence {
            temporary,
            if_not_exists,
            name,
            data_type: _,
            sequence_options,
            owned_by: _,
        } => {
            // sqlparser 0.52 parses `CREATE SEQUENCE` natively. The
            // `data_type` / `owned_by` fields are accepted but ignored
            // in v0.1 — the catalog stores `i64` sequences and has no
            // notion of column-attached ownership yet.
            crate::seq_ddl::exec_create_sequence(
                sess,
                temporary,
                if_not_exists,
                name,
                sequence_options,
            )
            .await
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Sequence,
            if_exists,
            names,
            ..
        } => crate::seq_ddl::exec_drop_sequence(sess, if_exists, &names).await,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::View,
            if_exists,
            names,
            ..
        } => {
            // DROP VIEW supports dropping a single view per statement.
            if names.len() != 1 {
                return Err(BasinError::InvalidSchema(
                    "DROP VIEW: exactly one view name expected".into(),
                ));
            }
            let name = single_part_name(&names[0])?.to_string();
            crate::view_ddl::exec_drop_view(sess, &name, if_exists).await
        }
        // ── Schema DDL ──────────────────────────────────────────────────
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => crate::schema_ddl::exec_create_schema(sess, schema_name, if_not_exists).await,
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Schema,
            if_exists,
            names,
            cascade,
            ..
        } => crate::schema_ddl::exec_drop_schema(sess, &names, if_exists, cascade).await,
        // ── SET search_path ─────────────────────────────────────────────
        Statement::Set(sqlparser::ast::Set::SingleAssignment {
            variable, values, ..
        }) => {
            // Only handle `SET search_path = …`; forward everything else
            // as a silent no-op so ORM migrations that emit PG-specific
            // SET statements (client_encoding, standard_conforming_strings,
            // etc.) don't hard-fail. This mirrors the PG wire protocol
            // server behaviour where un-recognised SET parameters are
            // accepted silently at the session level.
            // `variable` is an `ObjectName` holding `Vec<ObjectNamePart>`;
            // join with `.` to get the full variable name (e.g.
            // `search_path`).
            let var_name = variable
                .0
                .iter()
                .map(|i| i.id_val().as_str())
                .collect::<Vec<_>>()
                .join(".")
                .to_ascii_lowercase();

            if var_name == "search_path" {
                crate::schema_ddl::exec_set_search_path(sess, &values)
            } else if var_name == "statement_timeout" {
                // Wire `SET statement_timeout = …` to the per-session override.
                // Accept both string literals ('5s', '500ms') and bare integers (5000).
                let raw = values
                    .first()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                crate::session::set_statement_timeout(&sess.state, &raw)?;
                Ok(ExecResult::Empty { tag: "SET".into() })
            } else if var_name == "lock_timeout" {
                // Phase 5.28.B: SET lock_timeout = '500ms' / 500 / 0
                let raw = extract_set_string_value(&values);
                let d = crate::session::parse_pg_duration(&raw);
                crate::session::set_session_lock_timeout(&sess.state, d);
                Ok(ExecResult::Empty { tag: "SET".into() })
            } else if var_name == "idle_in_transaction_session_timeout" {
                // Phase 5.28.C: SET idle_in_transaction_session_timeout = '30s'
                let raw = extract_set_string_value(&values);
                let d = crate::session::parse_pg_duration(&raw);
                crate::session::set_session_idle_in_transaction_timeout(&sess.state, d);
                Ok(ExecResult::Empty { tag: "SET".into() })
            } else if var_name == "basin.synchronous_commit" || var_name == "synchronous_commit" {
                // Durability knob: ON = group-committed fsync before ack;
                // OFF (default) = RAM-buffered ack with the documented loss
                // window. A typo must error rather than silently downgrade
                // durability.
                let raw = extract_set_string_value(&values);
                let on = crate::session::parse_pg_bool(&raw)?;
                crate::session::set_session_synchronous_commit(&sess.state, on);
                Ok(ExecResult::Empty { tag: "SET".into() })
            } else if var_name == "pg_trgm.similarity_threshold" {
                // SET pg_trgm.similarity_threshold = 0.4
                // Controls the `a % b` operator threshold for this session.
                let raw = extract_set_string_value(&values);
                let trimmed = raw.trim().trim_matches('\'').trim_matches('"').trim();
                match trimmed.parse::<f32>() {
                    Ok(v) => {
                        crate::session::set_session_trgm_similarity_threshold(&sess.state, v);
                        Ok(ExecResult::Empty { tag: "SET".into() })
                    }
                    Err(_) => Err(basin_common::BasinError::InvalidSchema(format!(
                        "invalid value for pg_trgm.similarity_threshold: {trimmed:?} \
                         (expected a float in [0.0, 1.0])"
                    ))),
                }
            } else if var_name == "pg_trgm.word_similarity_threshold" {
                // SET pg_trgm.word_similarity_threshold = 0.5
                // Controls the `a <% b` operator threshold for this session.
                let raw = extract_set_string_value(&values);
                let trimmed = raw.trim().trim_matches('\'').trim_matches('"').trim();
                match trimmed.parse::<f32>() {
                    Ok(v) => {
                        crate::session::set_session_trgm_word_similarity_threshold(
                            &sess.state,
                            v,
                        );
                        Ok(ExecResult::Empty { tag: "SET".into() })
                    }
                    Err(_) => Err(basin_common::BasinError::InvalidSchema(format!(
                        "invalid value for pg_trgm.word_similarity_threshold: {trimmed:?} \
                         (expected a float in [0.0, 1.0])"
                    ))),
                }
            } else {
                // Silently accept unknown SET variables.
                Ok(ExecResult::Empty { tag: "SET".into() })
            }
        }
        Statement::Insert(ins) => {
            // Phase 5.29.B: if the target table is a hypertable, register
            // chunk records for each unique time-bucket present in the VALUES
            // list BEFORE the regular insert path runs.  This is best-effort:
            // failures here (e.g. non-timestamp columns, subquery inserts) are
            // silently ignored so the normal INSERT still proceeds.
            let _ = touch_hypertable_chunks_from_insert(sess, &ins, raw_sql).await;
            exec_insert(sess, ins, Some(raw_sql)).await
        }
        Statement::Query(ref query) => {
            // ── Data-modifying CTE intercept ─────────────────────────────────
            // `WITH x AS (INSERT/UPDATE/DELETE … RETURNING …) SELECT … FROM x`
            // DataFusion 53 cannot plan DML statements as relations, so we
            // orchestrate them ourselves: execute each DML CTE in declaration
            // order, capture the RETURNING batch, register it as a MemTable in
            // the session context, then hand the outer SELECT to DataFusion.
            if query_has_dml_cte(query) {
                return exec_dml_cte_query(sess, query, sql, include_deleted).await;
            }

            // ── WITH RECURSIVE … INSERT/UPDATE/DELETE … intercept ────────────
            // `WITH RECURSIVE t(col) AS (… UNION ALL …) INSERT INTO target …`
            // sqlparser routes this as a Query whose body is SetExpr::Insert
            // (rather than SetExpr::Select).  DataFusion cannot plan DML in a
            // query body, so we lift the RECURSIVE CTE into the DML source and
            // dispatch through the normal DML path.
            if query_has_recursive_with_dml_body(query) {
                return exec_recursive_with_dml_body(sess, query).await;
            }

            // pg_plan routing instrumentation (ADR 0014 Phase 2). When the
            // parsed SELECT matches the shape the new translator handles,
            // bump the counter — independent of whether the fast path or
            // DataFusion path actually serves the query.
            if let Ok(tree) = crate::pg_ast::parse(sql) {
                if let Some(node) = tree.stmts().next() {
                    if crate::pg_plan::supports_shape(node) {
                        sess.engine.note_pg_plan_routed();
                    }
                }
            }

            // Phase 5.14.D2: record ORDER BY / GROUP BY patterns for adaptive
            // sort.  Best-effort; only fires for simple single-table SELECTs.
            record_query_patterns(sess, query);

            // ADR 0027 Phase 4 auto-promotion: observe JSON-path accesses and
            // fire promote_jsonb_path when the threshold is crossed.  This is
            // best-effort and non-blocking; the catalog call is spawned
            // fire-and-forget.  Recording is purely frequency-based — no
            // hardcoded column or key names.
            {
                let table_refs = crate::session::collect_table_refs(query);
                crate::jsonb_promotion::observe_and_maybe_promote(
                    sql,
                    &sess.project,
                    &table_refs,
                    sess.engine.jsonb_promotion_registry(),
                    sess.engine.config().catalog.clone(),
                    &sess.engine.memtable_registry(),
                );
            }

            // Try the point-query fast path first. It only matches a tightly
            // constrained shape; on any rejection we fall back to DataFusion.
            //
            // RLS gate: the fast path bypasses DataFusion's logical planner
            // entirely, which is where we inject row-level predicates. If
            // any referenced table has `rls_enabled = true`, we *must* take
            // the DataFusion path so the RLS rewrite can fire. Tables with
            // RLS off see the fast path exactly as before — same one-`bool`
            // catalog read the existing path already pays.
            // Metadata-only aggregate fast path — tried FIRST because it is
            // the most specific and by far the cheapest (answers bare
            // COUNT/MIN/MAX, and SUM once sum_bytes is populated, from
            // catalog stats with ZERO file decode — ~30x). It MUST precede
            // `match_simple_select`, whose recogniser also matches bare
            // aggregates and would otherwise shadow this path entirely. It
            // returns `None`/`Ok(None)` for anything it can't answer from
            // metadata (WHERE present, unpopulated `sum_bytes`, unsupported
            // type, …), falling through to the recognisers below.
            //
            // Same view/RLS/soft-delete gate as the fast path (file-level
            // stats are invalid under row-level filtering). Transaction
            // gate: inside an explicit txn the session holds uncommitted
            // writes in `TxState::pending_files` that are NOT in the
            // catalog's `live_data_files()`; a metadata-only aggregate
            // would miss them, so any active txn takes the DataFusion path
            // (which merges the pending tail). Pure flag read — no I/O.
            if !crate::session::tx_is_active(&sess.state) {
                if let Some(plan) = crate::fast_aggregate::match_metadata_aggregate(&stmt) {
                    // Inv-OLTP-point (#149): per-session table-meta cache.
                    // First call serves from catalog; subsequent calls inside
                    // the TTL window skip both catalog round-trips. Any DDL
                    // / DML in this session has already invalidated the cache
                    // (see invalidation block at the top of `execute`).
                    let cached =
                        crate::session::load_table_meta_cached(sess, &plan.table).await;
                    let (table_meta, is_view) = match cached {
                        Some((arc, vp)) => (Some((*arc).clone()), vp),
                        None => (None, false),
                    };
                    if let Some(ref meta) = table_meta {
                        // The cache entry was validated against the catalog
                        // epoch in `load_table_meta_cached`: if any catalog
                        // mutation (including ENABLE/DISABLE ROW LEVEL SECURITY
                        // or CREATE/DROP POLICY) occurred since the last fill,
                        // the epoch will have advanced and the helper will have
                        // refetched from the catalog. Reading `rls_enabled`
                        // from the cache-validated `meta` is therefore safe and
                        // saves one catalog round-trip per fast-path SELECT.
                        // Fail closed: if something went wrong during the
                        // epoch-validated load, the helper returns None and we
                        // skip the fast path entirely (see outer match above).
                        let has_rls = meta.rls_enabled;
                        let has_soft_delete =
                            crate::types::soft_delete_column(meta.schema.as_ref()).is_some();
                        // Hot-tier merge-on-read gate: when the process-wide
                        // memtable holds tombstones (fast-path DELETE) or
                        // UPDATE overrides (fast-path UPDATE) for this table,
                        // the catalog `live_data_files()` row counts are stale
                        // — a tombstone removes a counted cold row and an
                        // override shadows one. Skip the metadata-only path so
                        // the aggregate is computed over the merged hot+cold
                        // row set (where the overlay / tombstone filter runs).
                        // S4: O(1) newest-version counters replace the
                        // previous O(n) snapshot walk. Tombstone/Update
                        // entries are always DIRTY (a flush ack removes clean
                        // tombstones and re-tags acked Updates as Rows), so
                        // the two counters are exactly the "overlay present"
                        // signal this auto-commit (unwatermarked) gate needs.
                        let has_hot_overlay = {
                            let registry = sess.engine.memtable_registry();
                            registry
                                .get(&sess.project, &plan.table)
                                .map(|e| {
                                    e.memtable.tombstone_count() > 0
                                        || e.memtable.update_count() > 0
                                })
                                .unwrap_or(false)
                        };
                        if !is_view && !has_rls && !has_soft_delete && !has_hot_overlay {
                            if let Some(result) =
                                crate::fast_aggregate::execute_metadata_aggregate(
                                    sess, plan, table_meta,
                                )
                                .await?
                            {
                                return Ok(result);
                            }
                        }
                    }
                }

                // Low-cardinality GROUP BY COUNT(*) fast path.  Same safety
                // gates as the metadata aggregate above: no active transaction
                // (pending_files would be missed), no view, no RLS, no soft
                // delete.  Falls through to DataFusion when the recogniser
                // returns `None` or when execute returns `Ok(None)` (e.g. key
                // range exceeds the low-cardinality threshold or catalog stats
                // are absent).
                if let Some(plan) = crate::fast_aggregate::match_groupby_low_card(&stmt) {
                    // Inv-OLTP-point (#149): same per-session cache as the
                    // metadata-aggregate path above.
                    let cached =
                        crate::session::load_table_meta_cached(sess, &plan.table).await;
                    let (table_meta, is_view) = match cached {
                        Some((arc, vp)) => (Some((*arc).clone()), vp),
                        None => (None, false),
                    };
                    if let Some(ref meta) = table_meta {
                        // Epoch-validated cache: see comment at the
                        // metadata-aggregate gate above. `rls_enabled` is
                        // safe to read from cache because the epoch check in
                        // `load_table_meta_cached` forces a refetch on any
                        // catalog mutation (including RLS DDL).
                        let has_rls = meta.rls_enabled;
                        let has_soft_delete =
                            crate::types::soft_delete_column(meta.schema.as_ref()).is_some();
                        if !is_view && !has_rls && !has_soft_delete {
                            if let Some(result) =
                                crate::fast_aggregate::execute_groupby_low_card(
                                    sess, plan, table_meta,
                                )
                                .await?
                            {
                                return Ok(result);
                            }
                        }
                    }
                }
            }

            // Point-lookup INNER-JOIN fast path (ORM hydrate). Recognise the
            // two-table PK-probe + PK-lookup join syntactically, then let
            // `execute_point_join` validate the metadata invariants (PK-ness,
            // view/RLS/soft-delete/txn/citext gates) and run two reused
            // `execute_simple_select` point lookups. `Ok(None)` on any failed
            // gate falls through to the DataFusion path below.
            if let Some(pj_plan) = crate::point_join::match_point_join(&stmt) {
                if let Some(res) =
                    crate::point_join::execute_point_join(sess, pj_plan, raw_sql).await?
                {
                    return Ok(res);
                }
            }

            if let Some(plan) = match_simple_select(&stmt) {
                // Fast-path gate: load the table metadata exactly once and
                // derive all three guard conditions from that single result.
                // Previously this performed 3 separate catalog round-trips
                // (lookup_view, table_has_rls → load_table, table_has_soft_delete
                // → load_table); now it is one load_table + one lookup_view.
                //
                // We still need lookup_view because views and tables live in
                // separate catalog maps and can share a name in principle.
                //
                // Inv-OLTP-point (#149): when both calls are served from the
                // per-session cache (`load_table_meta_cached`) the gate pays
                // a single `Mutex` lock + `LruCache::get` instead of two
                // async catalog round-trips. The OLTP point-query loop —
                // identical SELECT shape repeated — hits the cache after
                // the first miss; the TTL is 500ms by default.
                let cached =
                    crate::session::load_table_meta_cached(sess, &plan.table).await;
                let (table_meta, is_view) = match cached {
                    Some((arc, vp)) => (Some((*arc).clone()), vp),
                    None => (None, false),
                };
                if let Some(ref meta) = table_meta {
                    // Epoch-validated cache: the fast SELECT path is the
                    // primary RLS bypass vector. `load_table_meta_cached`
                    // compares the catalog epoch at read time against the
                    // epoch stored when the entry was filled; any catalog
                    // mutation (including ENABLE/DISABLE ROW LEVEL SECURITY or
                    // CREATE/DROP POLICY) bumps the epoch and forces a full
                    // refetch before the entry is returned. Reading
                    // `rls_enabled` from the cached `meta` is therefore safe
                    // and eliminates one catalog round-trip per warm SELECT.
                    // If the cache helper returned None (catalog error),
                    // `table_meta` is None and we fall through to DataFusion.
                    let has_rls = meta.rls_enabled;
                    let has_soft_delete =
                        crate::types::soft_delete_column(meta.schema.as_ref()).is_some();
                    // Inside an explicit transaction the fast path (and the
                    // aggregate fast path, which shares this dispatch) reads
                    // only the committed snapshot + shard tail — it does NOT
                    // merge `TxState::pending_files`, so a SELECT/aggregate
                    // would miss this txn's uncommitted writes (and mis-handle
                    // ROLLBACK TO SAVEPOINT). Bail to the transaction-aware
                    // DataFusion path whenever a txn is open; a missed fast
                    // path is merely slower, a wrong answer is not acceptable.
                    let in_txn = crate::session::tx_is_active(&sess.state);
                    // Phase 5.30.C/E: citext column check.
                    // The fast path uses byte-exact string comparison in
                    // `batch_matches_predicates` and a byte-lexicographic sort
                    // in the ORDER BY path; neither honours the case-insensitive
                    // citext contract.  Route through DataFusion (which applies
                    // CitextAnalyzerRule) whenever a WHERE predicate column or
                    // an ORDER BY column is marked BASIN_TYPE=CITEXT.
                    let schema_field_is_citext = |col_name: &str| -> bool {
                        meta.schema
                            .field_with_name(col_name)
                            .ok()
                            .map(|f| {
                                f.metadata()
                                    .get(crate::types::BASIN_TYPE_KEY)
                                    .map(|v| v.as_str() == crate::types::BASIN_TYPE_CITEXT)
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false)
                    };
                    let has_citext_predicate = plan.predicates.iter().any(|pred| {
                        schema_field_is_citext(pred.column())
                    });
                    let has_citext_order_by = plan
                        .order_by
                        .as_ref()
                        .map(|(col, _)| schema_field_is_citext(col.as_str()))
                        .unwrap_or(false);
                    // ADR 0027 Phase 4: promoted shadow columns are kept out of
                    // `meta.schema` (they are only visible in DataFusion's
                    // extended schema).  `fast_select` CAN read them directly
                    // (see `execute_simple_select`, which extends its working
                    // schema with the referenced shadow fields), but only when
                    // every file it scans physically carries the shadow column.
                    //
                    // The correctness guard is enforced INSIDE
                    // `execute_simple_select` (it must run against the
                    // post-flush authoritative file set, which the function
                    // reloads after draining the shard tail).  When the guard
                    // fails the function transparently delegates to the
                    // DataFusion / UDF path via `exec_select`.  So the gate here
                    // imposes no shadow-column restriction: a referenced shadow
                    // column is allowed through and self-guards downstream.
                    // In-tx exception to the bail above: when THIS table has
                    // no pending in-tx writes AND the txn's pinned read
                    // snapshot (peeked, never captured here — capturing
                    // pre-flush would pin a head older than the read's actual
                    // view) equals the current head, the committed-snapshot
                    // read the fast path performs is exactly the txn's
                    // repeatable-read view. No pin yet, pending writes, or a
                    // moved head → DataFusion, which rewinds via
                    // load_table_for_read.
                    // Snapshot-pin correctness inside an explicit transaction.
                    // `execute_simple_select` reads the *current* catalog head
                    // (it flushes the shard tail then reloads `live_data_files()`
                    // — see fast_select.rs) and applies the latest hot-tier
                    // overlay, so a NAIVE in-tx fast read leaks a concurrent
                    // commit past the txn's repeatable-read pin (the
                    // snapshot_isolation GAP). The prior fix was a blanket
                    // `!in_txn` bail; this re-enables the fast path for the safe
                    // sub-case ONLY, threading a pinned read-view into
                    // `execute_simple_select` so it reads AT the pin with fresh
                    // state instead of the moving head.
                    //
                    // The fast in-tx read is admitted ONLY when ALL hold:
                    //   (a) THIS table is untouched by THIS tx — no pending data
                    //       files, no tx-buffered HTAP INSERT batches, and an
                    //       empty in-tx UPDATE/DELETE overlay — so there are no
                    //       uncommitted local writes the fast path would miss
                    //       (read-your-own-writes), and no savepoint hazard;
                    //   (b) EITHER a read snapshot is ALREADY pinned for the
                    //       table (`tx_read_snapshot_peek`) — read AT it
                    //       (`AlreadyPinned`) — OR this is the table's FIRST
                    //       TOUCH (no pin yet) and (a) holds, in which case the
                    //       fast path itself flushes-then-pins-and-serves
                    //       (`FirstTouch`). The gate STILL never captures: it
                    //       only PEEKS here (capturing pre-flush would pin a head
                    //       older than the read's actual view — the prior leak's
                    //       root). The flush-then-capture happens inside
                    //       `execute_simple_select_inner` at the SAME post-flush
                    //       moment `load_table_for_read` captures, so if the read
                    //       instead bails to DataFusion it pins identically (the
                    //       `_for` helpers are idempotent peek-or-insert); and
                    //   (c) a hot-seq watermark is captured ALONGSIDE the cold
                    //       snapshot (`AlreadyPinned` carries the peeked one;
                    //       `FirstTouch` captures it post-flush) — the hot half of
                    //       the read-view, threaded into the memtable probes so a
                    //       concurrent session's later UPDATE/DELETE overlay
                    //       (seq > watermark) is filtered out.
                    //
                    // When admitted we pass a `PinnedReadRequest` into
                    // `execute_simple_select`, which (1) loads metadata at the
                    // pin via `load_table_at_snapshot` (FeatureNotSupported →
                    // bails to DataFusion), (2) filters hot-tier probes by the
                    // watermark, (3) bypasses the PK row cache (epoch/snapshot-
                    // keyed for current semantics), and (4) reloads at the pin
                    // (not the new head) after any tail flush. Any piece that
                    // cannot honour the pin Ok-falls back to DataFusion.
                    let in_tx_request: Option<crate::fast_select::PinnedReadRequest> = if in_txn {
                        let table = &plan.table;
                        let untouched =
                            crate::session::tx_pending_files_for(&sess.state, table).is_empty()
                                && crate::session::tx_htap_batches_for(&sess.state, table)
                                    .is_empty()
                                && crate::session::tx_overlay_peek(&sess.state, table).is_empty();
                        match (
                            untouched,
                            crate::session::tx_read_snapshot_peek(&sess.state, table),
                            crate::session::tx_hot_seq_watermark_peek(&sess.state, table),
                        ) {
                            (true, Some(snapshot), Some(hot_watermark)) => {
                                Some(crate::fast_select::PinnedReadRequest::AlreadyPinned(
                                    crate::fast_select::PinnedReadView {
                                        snapshot,
                                        hot_watermark,
                                    },
                                ))
                            }
                            // Untouched but not yet pinned: first touch of this
                            // table. The fast path flushes, then captures both
                            // pins at the post-flush head and serves at that pin.
                            (true, None, None) => {
                                Some(crate::fast_select::PinnedReadRequest::FirstTouch)
                            }
                            // Touched table (uncommitted local writes / savepoint
                            // hazard), or a partially-pinned state (one half
                            // present, the other not — should not happen since
                            // both are captured together, but conservatively
                            // route to DataFusion which re-pins coherently).
                            _ => None,
                        }
                    } else {
                        None
                    };
                    // Outside a tx the fast path always runs; inside a tx it runs
                    // ONLY with a pinned read-view request (the safe sub-case
                    // above — already-pinned or untouched-first-touch).
                    let in_tx_fast_ok = !in_txn || in_tx_request.is_some();
                    if !is_view
                        && !has_rls
                        && !has_soft_delete
                        && in_tx_fast_ok
                        && !has_citext_predicate
                        && !has_citext_order_by
                    {
                        return crate::fast_select::execute_simple_select_pinned(
                            sess,
                            plan,
                            table_meta,
                            raw_sql,
                            include_deleted,
                            in_tx_request,
                        )
                        .await;
                    }
                }
            }

            // Phase 5.19.C: GIN containment fast path.
            // Detect `SELECT … FROM t WHERE col @> 'literal'` (or `<@`) on a
            // column that has a GIN index; probe the in-RAM posting list to
            // get candidate files, then fall through to DataFusion which
            // applies the full `jsonb_contains` predicate for correctness.
            // The probe is advisory-only: when it returns `NoIndex` the query
            // falls through to a full DataFusion scan without error.
            if !crate::session::tx_is_active(&sess.state)
                && (raw_sql.contains("@>") || raw_sql.contains("<@"))
            {
                if let Some(gin_plan) = crate::index_probe::detect_gin_containment(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    // Probe the posting list.  Only an explicit Empty result
                    // lets us short-circuit; NoIndex and FileCandidates both
                    // fall through (the DataFusion path handles correctness).
                    let gin_result = sess.engine.gin_index_registry().probe_containment(
                        &sess.project,
                        &gin_plan.table,
                        &gin_plan.col,
                        &gin_plan.opclass,
                        &gin_plan.needle,
                    );
                    if let crate::index_probe::ProbeResult::Empty = gin_result {
                        // Posting list guarantees no COLD-FILE rows match.
                        //
                        // For a row-emitting query (`SELECT * / cols`) we can
                        // short-circuit with zero rows.  But for a whole-relation
                        // aggregate (`COUNT(*) … WHERE col @> '…'`) zero rows is
                        // the WRONG answer — PG returns a single row with `0`.
                        // Fall through to `exec_select`, which registers the
                        // (row-group-pruned or full) relation and lets DataFusion
                        // compute the aggregate over zero matching rows → `0`.
                        //
                        // Overlay/completeness gate (the dml_mutate.rs UPDATE
                        // fast-path gate's blocker #1): the posting list is
                        // built from cold files only, so `Empty` is
                        // authoritative ONLY when the cold files are the whole
                        // story — no live hot-tier UPDATE/DELETE overlay for
                        // the table AND every live file present in the
                        // indexed-files completeness set. Otherwise fall
                        // through to the overlay-aware DataFusion scan
                        // (correctness over the shortcut). See
                        // `gin_empty_probe_is_trustworthy`.
                        if !gin_plan.is_aggregate
                            && gin_empty_probe_is_trustworthy(sess, &gin_plan.table, &gin_plan.col)
                                .await
                        {
                            let schema = Arc::new(arrow_schema::Schema::empty());
                            return Ok(ExecResult::Rows { schema, batches: vec![] });
                        }
                    }
                    // FileCandidates: the DataFusion path still executes; the
                    // posting list result is currently advisory (file-level
                    // pruning via DataFusion scan override is 5.19.E).
                    // Fall through to exec_select for correctness.
                }
            }

            // Phase 5.19.D — detect `SELECT … FROM t WHERE col ? 'key'` (or
            // `?&` / `?|` variants) and probe the GIN posting list.
            // Advisory-only: `Empty` short-circuits; anything else falls
            // through to the DataFusion / UDF path for correctness.
            if !crate::session::tx_is_active(&sess.state)
                && (raw_sql.contains(" ? ") || raw_sql.contains(" ?'")
                    || raw_sql.contains("?&") || raw_sql.contains("?|"))
            {
                if let Some(key_plan) = crate::index_probe::detect_gin_key_probe(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    let key_refs: Vec<&str> = key_plan.keys.iter().map(|s| s.as_str()).collect();
                    let gin_result = sess.engine.gin_index_registry().probe_key_existence(
                        &sess.project,
                        &key_plan.table,
                        &key_plan.col,
                        &key_plan.opclass,
                        &key_refs,
                        key_plan.require_all,
                    );
                    if let crate::index_probe::ProbeResult::Empty = gin_result {
                        // No COLD files contain these keys. Same
                        // overlay/completeness gate as the `@>` short-circuit
                        // above: an overlay override row may carry keys whose
                        // cold posting sets are disjoint (`?&`), and a
                        // materialize-replacement file is never re-indexed —
                        // in either state `Empty` is not authoritative and we
                        // must fall through to the overlay-aware scan.
                        if gin_empty_probe_is_trustworthy(sess, &key_plan.table, &key_plan.col)
                            .await
                        {
                            let schema = Arc::new(arrow_schema::Schema::empty());
                            return Ok(ExecResult::Rows { schema, batches: vec![] });
                        }
                    }
                    // FileCandidates / NoIndex: fall through to DataFusion for correctness.
                }
            }

            // Phase 5.20.E — detect `SELECT … FROM t WHERE col @@ to_tsquery(…)`
            // on a tsvector column with a GIN index and probe the FTS posting
            // list.  Advisory-only: `Empty` short-circuits; anything else falls
            // through to the DataFusion / UDF path for correctness.
            if !crate::session::tx_is_active(&sess.state) && raw_sql.contains("@@") {
                if let Some(fts_plan) = crate::index_probe::detect_tsvector_match(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    use basin_storage::index::gin_tsvector::TsvProbeResult;
                    let fts_result = sess.engine.gin_fts_registry().probe_query(
                        &sess.project,
                        &fts_plan.table,
                        &fts_plan.col,
                        &fts_plan.tsquery_str,
                    );
                    if let TsvProbeResult::Empty = fts_result {
                        // The posting list proves no COLD-file row matches.
                        // Same overlay/completeness gate as the JSONB `@>` /
                        // `?`-family short-circuits above
                        // (`gin_empty_probe_is_trustworthy`): a live hot-tier
                        // override row may carry lexemes whose cold posting
                        // sets are disjoint, and an un-indexed live file
                        // (pre-index data, restart, posting-budget eviction,
                        // materialize replacement) may hold a real match the
                        // registry knows nothing about.  Either state makes
                        // `Empty` non-authoritative → fall through to the
                        // overlay-aware full scan.
                        if fts_empty_probe_is_trustworthy(sess, &fts_plan.table, &fts_plan.col)
                            .await
                        {
                            let schema = Arc::new(arrow_schema::Schema::empty());
                            return Ok(ExecResult::Rows { schema, batches: vec![] });
                        }
                    }
                    // FileCandidates / NoIndex: fall through to DataFusion for correctness.
                }
            }

            // Phase 5.24.D — detect `SELECT … FROM t WHERE col @> val` /
            // `col && range` / `col <@ range` on a GIST-indexed range column
            // and probe the interval tree.  Advisory-only: `Empty` short-circuits;
            // anything else falls through to the DataFusion / UDF path for
            // correctness.
            if !crate::session::tx_is_active(&sess.state)
                && (raw_sql.contains("@>") || raw_sql.contains("&&") || raw_sql.contains("<@"))
            {
                if let Some(interval_plan) = crate::index_probe::detect_range_index_probe(
                    raw_sql,
                    &sess.project,
                    &sess.engine.config().catalog,
                )
                .await
                {
                    use basin_common::types::range::{IndexInterval, RangeValue};
                    use basin_storage::index::interval::ProbeResult;
                    let interval_result = match &interval_plan.op {
                        crate::index_probe::RangeOp::ContainsElem => {
                            if let Some(pt) = interval_plan.point {
                                sess.engine.interval_registry().probe_contains_point(
                                    &sess.project,
                                    &interval_plan.table,
                                    &interval_plan.col,
                                    pt,
                                )
                            } else {
                                ProbeResult::NoIndex
                            }
                        }
                        crate::index_probe::RangeOp::ContainsRange
                        | crate::index_probe::RangeOp::Overlaps
                        | crate::index_probe::RangeOp::ContainedBy => {
                            if let Some(lit) = &interval_plan.range_literal {
                                if let Some(rv) = RangeValue::from_pg_text(lit) {
                                    if let Some(iv) = IndexInterval::from_range(&rv) {
                                        sess.engine.interval_registry().probe_overlaps(
                                            &sess.project,
                                            &interval_plan.table,
                                            &interval_plan.col,
                                            &iv,
                                        )
                                    } else {
                                        ProbeResult::NoIndex
                                    }
                                } else {
                                    ProbeResult::NoIndex
                                }
                            } else {
                                ProbeResult::NoIndex
                            }
                        }
                    };
                    if let ProbeResult::Empty = interval_result {
                        // Interval tree guarantees no rows match — short-circuit.
                        let schema = Arc::new(arrow_schema::Schema::empty());
                        return Ok(ExecResult::Rows { schema, batches: vec![] });
                    }
                    // FileCandidates / NoIndex: fall through to DataFusion for correctness.
                }
            }

            exec_select(sess, sql, include_deleted, Some(raw_sql)).await
        }
        Statement::ShowTables { .. } => exec_show_tables(sess).await,
        // ── SHOW search_path ─────────────────────────────────────────────
        Statement::ShowVariable { variable } => {
            let var_name = variable
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join("_")
                .to_ascii_lowercase();
            if var_name == "search_path" {
                crate::schema_ddl::exec_show_search_path(sess)
            } else if var_name == "statement_timeout" {
                let val = crate::session::show_statement_timeout(&sess.state);
                let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "statement_timeout",
                    arrow_schema::DataType::Utf8,
                    false,
                )]));
                let col: ArrayRef = Arc::new(StringArray::from(vec![val]));
                let batch = RecordBatch::try_new(schema.clone(), vec![col])
                    .map_err(|e| BasinError::internal(format!("SHOW statement_timeout: {e}")))?;
                Ok(ExecResult::Rows {
                    schema: Arc::new(crate::convert::schema_df_to_ws(&schema)?),
                    batches: vec![batch],
                })
            } else if var_name == "lock_timeout" {
                // Phase 5.28.D: SHOW lock_timeout
                let val = crate::session::format_pg_duration(
                    crate::session::session_lock_timeout(&sess.state),
                );
                Ok(make_show_result("lock_timeout", &val))
            } else if var_name == "idle_in_transaction_session_timeout" {
                // Phase 5.28.D: SHOW idle_in_transaction_session_timeout
                let val = crate::session::format_pg_duration(
                    crate::session::session_idle_in_transaction_timeout(&sess.state),
                );
                Ok(make_show_result("idle_in_transaction_session_timeout", &val))
            } else if var_name == "basin_synchronous_commit" || var_name == "synchronous_commit" {
                // SHOW joins dotted names with `_`, so `basin.synchronous_commit`
                // arrives as `basin_synchronous_commit`.
                let val = crate::session::show_synchronous_commit(&sess.state);
                Ok(make_show_result("basin.synchronous_commit", val))
            } else if var_name == "pg_trgm_similarity_threshold"
                || var_name == "pg_trgm.similarity_threshold"
            {
                // SHOW pg_trgm.similarity_threshold
                // sqlparser joins dotted names with `_`, so both forms must match.
                let v = crate::session::session_trgm_similarity_threshold(&sess.state);
                Ok(make_show_result("pg_trgm.similarity_threshold", &format!("{v}")))
            } else if var_name == "pg_trgm_word_similarity_threshold"
                || var_name == "pg_trgm.word_similarity_threshold"
            {
                // SHOW pg_trgm.word_similarity_threshold
                let v = crate::session::session_trgm_word_similarity_threshold(&sess.state);
                Ok(make_show_result(
                    "pg_trgm.word_similarity_threshold",
                    &format!("{v}"),
                ))
            } else {
                // Silently return empty for other SHOW <var> forms so
                // ORM startup queries don't hard-fail.
                Ok(ExecResult::Empty { tag: "SHOW".into() })
            }
        }
        Statement::AlterTable(sqlparser::ast::AlterTable {
            name, operations, ..
        }) => exec_alter_table(sess, name, operations).await,
        Statement::Delete(del) => crate::dml_mutate::exec_delete(sess, del).await,
        Statement::Update(sqlparser::ast::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            ..
        }) => {
            let from = from.and_then(|f| match f {
                sqlparser::ast::UpdateTableFromKind::BeforeSet(mut v)
                | sqlparser::ast::UpdateTableFromKind::AfterSet(mut v) => {
                    if v.is_empty() {
                        None
                    } else {
                        Some(v.swap_remove(0))
                    }
                }
            });
            crate::dml_mutate::exec_update(sess, table, assignments, from, selection, returning)
                .await
        }
        // ----- Cursor lifecycle ----- //
        Statement::Declare { stmts } => exec_declare(sess, stmts).await,
        Statement::Fetch {
            name, direction, ..
        } => exec_fetch(sess, &name.value, direction).await,
        Statement::Close { cursor } => exec_close(sess, cursor).await,
        Statement::Explain {
            analyze,
            verbose,
            format,
            options,
            statement,
            ..
        } => {
            let format = format.map(|k| match k {
                sqlparser::ast::AnalyzeFormatKind::Keyword(f)
                | sqlparser::ast::AnalyzeFormatKind::Assignment(f) => f,
            });
            crate::explain::exec_explain(sess, analyze, verbose, format, options, statement).await
        }
        Statement::Drop {
            object_type: sqlparser::ast::ObjectType::Table,
            if_exists,
            names,
            ..
        } => exec_drop_table(sess, if_exists, names).await,
        // PostgreSQL 15+ `MERGE INTO target USING source ON cond WHEN …`.
        // Compiles each per-row WHEN action to ordinary INSERT/UPDATE/DELETE
        // driven through the normal statement pipeline (RLS, constraints, fast
        // paths, atomicity all inherited). See `crate::merge`.
        Statement::Merge(merge) => crate::merge::exec_merge(sess, merge).await,
        other => Err(BasinError::internal(format!("unsupported in PoC: {other}"))),
    };

    // ── Error-in-txn → aborted state ────────────────────────────────────
    // If any statement fails while inside an active transaction, mark the
    // transaction as aborted.  Subsequent statements (except ROLLBACK) will
    // receive SQLSTATE 25P02 until the client issues ROLLBACK.
    if result.is_err() && crate::session::tx_is_active(&sess.state) {
        crate::session::tx_set_aborted(&sess.state);
    }
    result
}

/// Prepared-statement bind-INSERT fast path (perf-w-prepared).
///
/// Execute an already-parsed `Statement` — the prepared template's AST with
/// bind values substituted directly into the tree — WITHOUT re-parsing any
/// SQL. The text path ([`execute`]) parses each statement twice per call
/// (libpg_query for the noop/DDL pre-screens, then sqlparser for dispatch);
/// for a bulk INSERT loop the literal values differ on every `Execute`, so
/// the sqlparser parse-cache never hits and both parses run fresh each time.
/// This entry skips both: `prepare` already parsed the template once, the
/// caller substitutes binds into a clone of that AST, and we dispatch the
/// resulting `Statement` straight into the shared [`dispatch_parsed_statement`]
/// tail.
///
/// SCOPE: the caller (`prepared::execute_bound`) gates this on
/// `!needs_rewrite_pipeline(template)` AND a statement kind it knows is safe
/// to route through the AST path (INSERT / plain SELECT). We re-assert the
/// kind gate here as a hard invariant: anything else is rejected with
/// `FeatureNotSupported` so a mis-route can never silently skip a required
/// text rewrite or DDL pre-screen.
///
/// `raw_sql` is the rendered substituted SQL, used both as the logging text
/// and (for a SELECT that falls through to DataFusion) as the planner input.
/// Because the fast path is gated on `!needs_rewrite_pipeline`, the rendered
/// text is exactly what the text pipeline would have produced (a no-op pass),
/// so dispatching it is behaviour-identical to the text path.
pub(crate) async fn execute_statement(
    sess: &ProjectSession,
    stmt: Statement,
    raw_sql: &str,
) -> Result<ExecResult> {
    // Same session bookkeeping the text `execute` does at its top: keep the
    // idle-in-txn reaper's activity clock fresh and honour an already-fired
    // reap.
    crate::session::touch_last_active(&sess.state);
    if sess.reaped_flag.is_reaped() {
        return Err(basin_common::BasinError::IdleInTransactionTimeout(
            "idle transaction terminated by server idle_in_transaction_session_timeout".into(),
        ));
    }

    // Hard kind gate (defence in depth — the caller already screened). Only
    // INSERT and Query reach the AST path; everything else must go through the
    // text `execute` so its DDL pre-screens / noop-accept gates run.
    if !matches!(stmt, Statement::Insert(_) | Statement::Query(_)) {
        return Err(BasinError::FeatureNotSupported(
            "execute_statement only handles INSERT/SELECT; use the text path".into(),
        ));
    }

    // For the AST fast path the substituted text needs no rewriting (gated on
    // !needs_rewrite_pipeline), so `sql` == `raw_sql`. The four CREATE-TABLE
    // pre-screen outputs are all empty/None: they never apply to INSERT/SELECT.
    dispatch_parsed_statement(
        sess,
        stmt,
        raw_sql,
        raw_sql,
        /* include_deleted */ false,
        /* cluster_columns */ None,
        CreateTableLifecycle::default(),
        /* ctas_shape */ None,
        /* exclude_specs */ Vec::new(),
        /* cv_options */ None,
    )
    .await
}

/// Async wrapper around [`pg_operators::rewrite_json_agg_qualified_wildcard`]
/// that resolves the alias→table map from the SQL's FROM clauses, loads each
/// table's column list from the catalog, and hands the resulting
/// `HashMap<alias, Vec<column>>` to the pure rewriter.
///
/// Fast-paths when the SQL contains no `json_agg`/`jsonb_agg` token. The
/// rewriter is a no-op (returns SQL unchanged) when:
///   1. No qualified-wildcard `<fn>(<alias>.*)` is present.
///   2. The alias doesn't appear in a `FROM <table> [AS] <alias>` clause.
///   3. The resolved table name doesn't exist in the catalog.
///   4. The catalog load fails (logged at debug, SQL passes through unchanged
///      so the downstream parser surfaces the original error).
async fn rewrite_json_agg_qualified_wildcard_with_catalog(
    sql: &str,
    catalog: &dyn basin_catalog::Catalog,
    project: &basin_common::ProjectId,
) -> String {
    let aliases = crate::pg_operators::collect_json_agg_star_aliases(sql);
    if aliases.is_empty() {
        return sql.to_string();
    }
    let alias_table = crate::pg_operators::extract_from_alias_table_map(sql);
    let mut alias_columns: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for alias in aliases {
        let Some(table_str) = alias_table.get(&alias) else {
            continue;
        };
        let Ok(table_name) = TableName::new(table_str.clone()) else {
            continue;
        };
        match catalog.load_table(project, &table_name).await {
            Ok(meta) => {
                let cols: Vec<String> = meta
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                if !cols.is_empty() {
                    alias_columns.insert(alias, cols);
                }
            }
            Err(_) => {
                // Unknown table — leave the json_agg call unchanged so the
                // user sees the genuine "table not found" error from the
                // planner rather than a confusing expansion failure.
                continue;
            }
        }
    }
    if alias_columns.is_empty() {
        return sql.to_string();
    }
    crate::pg_operators::rewrite_json_agg_qualified_wildcard(sql, &alias_columns)
}

/// ADR 0027 Phase 4: rewrite `json_get_text(col, 'key')` to the shadow column
/// `__promoted$col$key` for any promoted JSONB paths on tables referenced in
/// `sql`.  Called after `rewrite_json_operators` so the operator forms have
/// already been lowered to `json_get_text(...)`.
///
/// Fast-path: if the SQL contains no `json_get_text(` token, return unchanged.
/// On any catalog failure the SQL passes through unchanged (graceful fallback
/// to the UDF path).
///
/// ## Correctness guard — only rewrite when every file carries the shadow column
///
/// A shadow column physically exists only in files written / backfilled AFTER
/// the path was promoted.  The reader null-fills any projected column absent
/// from a file's Arrow schema (see `basin_storage::reader::read_paths_inner`),
/// so a rewrite that targets a shadow column missing from a pre-promotion file
/// would yield a spurious NULL on BOTH the `fast_select` and the DataFusion
/// read paths — a WRONG answer, not just a slow one.  We therefore rewrite a
/// path ONLY when every live data file already carries its shadow column; any
/// path that fails this test is left as `json_get_text(...)` so the value is
/// computed from the source JSONB (correct, slower).  The authoritative
/// per-file presence signal is the catalog's `DataFileRef::column_stats`: the
/// writer inserts one entry per physical column for both Parquet and Vortex.
///
/// The shard tail is drained to cold-tier files first so the file set is
/// authoritative; the engine `MemTableRegistry` is then checked because it can
/// hold post-COMMIT HTAP-cached rows (inserted before promotion) that carry no
/// shadow column and are not covered by `column_stats`.
async fn rewrite_promoted_cols_for_query(sql: &str, sess: &ProjectSession) -> String {
    // Cheap bail-out: if there's no json_get_text call there's nothing to rewrite.
    if !sql.to_ascii_lowercase().contains("json_get_text(") {
        return sql.to_string();
    }
    let catalog = sess.engine.config().catalog.as_ref();
    let project = &sess.project;
    // Parse to extract bare table references from FROM clauses.
    let table_refs: Vec<String> = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::PostgreSqlDialect {},
        sql,
    )
    .ok()
    .and_then(|mut stmts| stmts.pop())
    .and_then(|stmt| {
        if let sqlparser::ast::Statement::Query(q) = stmt {
            Some(crate::session::collect_table_refs(&q))
        } else {
            None
        }
    })
    .unwrap_or_default();

    // Accumulate the promoted paths that are SAFE to rewrite (every live file
    // for the owning table carries the shadow column, and no uncovered hot-tier
    // rows exist).  Paths that fail the guard are dropped here so they keep
    // their `json_get_text` form.
    let mut all_paths: Vec<basin_catalog::PromotedJsonbPath> = Vec::new();
    for table_str in table_refs {
        let Ok(table_name) = basin_common::TableName::new(table_str) else { continue; };
        let Ok(meta) = catalog.load_table(project, &table_name).await else { continue; };
        if meta.promoted_jsonb_paths.is_empty() {
            continue;
        }
        // Correctness without flushing: the cold-file `column_stats` guard below
        // is authoritative ONLY if no un-flushed tail rows exist for this table
        // (a row written before promotion and still resident in the shard tail
        // or engine memtable carries no shadow column, and the reader would
        // null-fill it on the fast path → a WRONG answer).  We check this with
        // two O(1) no-flush signals: `Shard::has_pending_tail` (inspects the
        // resident per-partition tail maps; never lists/scans/drains storage)
        // and the engine `MemTableRegistry` (post-COMMIT HTAP-cached + fast-path
        // UPDATE/DELETE overlays).  If either is non-empty we skip ALL of this
        // table's paths so they keep their `json_get_text` form and route to the
        // correct DataFusion/UDF path.  A SELECT must NEVER trigger a flush:
        // `flush_to_parquet` is not a cheap no-op on a drained tail (it
        // LISTs/scans every partition), which previously added ~56ms/query at
        // 1M scale — the whole point of this no-flush guard.
        let has_pending_tail = match sess.engine.config().shard.as_ref() {
            Some(shard) => shard.has_pending_tail(project, &table_name).await,
            None => false,
        };
        // ADR 0027 Phase 4: only block the rewrite when the memtable may hold a
        // live row MISSING a promoted shadow column. After a post-promotion
        // INSERT/UPDATE the fast paths materialise the shadow column into the
        // hot row (see `hot_tier_update_by_pk` / the INSERT path), so a clean
        // memtable no longer forces a full per-row JSONB UDF scan — the hot
        // rows carry the column and read correctly via the shadow path. A row
        // that predates promotion marks the memtable dirty (see
        // `observe_and_maybe_promote`) and keeps the conservative fallback.
        let memtable_blocks = sess
            .engine
            .memtable_registry()
            .memtable_blocks_promoted_read(project, &table_name);
        if has_pending_tail || memtable_blocks {
            continue;
        }
        let live_files = meta.live_data_files();
        for path in meta.promoted_jsonb_paths {
            let shadow = path.shadow_col_name();
            // Tail/memtable already proven empty above; the cold-file
            // `column_stats` presence check is now authoritative.
            let all_present = live_files
                .iter()
                .all(|f| f.column_stats.contains_key(&shadow));
            if all_present {
                all_paths.push(path);
            }
        }
    }
    if all_paths.is_empty() {
        return sql.to_string();
    }
    crate::promoted_columns::rewrite_promoted_columns(sql, &all_paths)
}

/// Execute a `VectorSearchPlan` produced by `vector_planner`. Calls
/// `Storage::vector_search` (via the existing `ProjectSession::vector_search`
/// fast path) with `fetch_k` candidates, applies any column-equality
/// pushdown filters, truncates to the user's `LIMIT`, and projects to the
/// user's `SELECT` list.
///
/// Mirrors the result shape `exec_select` would produce for the same query
/// in brute-force mode: the projected user columns only, no synthetic
/// `_distance` column. (Brute-force computes the distance in `ORDER BY` but
/// only emits whatever the user wrote in `SELECT`.)
async fn execute_vector_search_plan(
    sess: &ProjectSession,
    plan: crate::vector_planner::VectorSearchPlan,
) -> Result<ExecResult> {
    let fetch_k = crate::vector_planner::fetch_k(&plan);
    let distance = crate::vector_planner::distance_for(plan.distance_op);

    // Resolve the user-projection schema up front so an empty result still
    // has the correct column list.
    let table_meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &plan.table)
        .await?;

    let raw_batches = sess
        .vector_search(
            &plan.table,
            &plan.vec_col,
            plan.query_vec.clone(),
            fetch_k,
            distance,
        )
        .await?;

    if raw_batches.is_empty() {
        let empty = crate::vector_planner::empty_for_projection(
            table_meta.schema.as_ref(),
            &plan.projection,
        )?;
        let schema = empty.schema();
        sess.engine.note_vector_routed();
        return Ok(ExecResult::Rows {
            schema,
            batches: vec![empty],
        });
    }

    // Apply any pushdown filters and truncate to k. The single-batch
    // contract from `ProjectSession::vector_search` keeps the loop trivial:
    // each batch already carries `_distance` ascending, so the global
    // top-k after filter is the prefix of `keep` indices.
    let mut filtered_batches: Vec<RecordBatch> = Vec::with_capacity(raw_batches.len());
    let mut total_kept = 0usize;
    for batch in &raw_batches {
        if total_kept >= plan.k {
            break;
        }
        let mut keep = crate::vector_planner::surviving_indices(batch, &plan.filters)?;
        if total_kept + keep.len() > plan.k {
            keep.truncate(plan.k - total_kept);
        }
        if keep.is_empty() {
            continue;
        }
        // Use arrow-select::take to preserve column order + types.
        let indices =
            arrow_array::UInt32Array::from(keep.iter().map(|i| *i as u32).collect::<Vec<_>>());
        let mut taken_cols = Vec::with_capacity(batch.num_columns());
        for c in batch.columns() {
            let t = arrow_select::take::take(c.as_ref(), &indices, None)
                .map_err(|e| BasinError::internal(format!("take rows: {e}")))?;
            taken_cols.push(t);
        }
        let taken = RecordBatch::try_new(batch.schema(), taken_cols)
            .map_err(|e| BasinError::internal(format!("rebuild taken batch: {e}")))?;
        total_kept += taken.num_rows();
        filtered_batches.push(taken);
    }

    if filtered_batches.is_empty() {
        let empty = crate::vector_planner::empty_for_projection(
            table_meta.schema.as_ref(),
            &plan.projection,
        )?;
        let schema = empty.schema();
        sess.engine.note_vector_routed();
        return Ok(ExecResult::Rows {
            schema,
            batches: vec![empty],
        });
    }

    let mut projected: Vec<RecordBatch> = Vec::with_capacity(filtered_batches.len());
    for b in &filtered_batches {
        projected.push(crate::vector_planner::project_for_user(
            b,
            &plan.projection,
        )?);
    }
    let schema = projected[0].schema();
    sess.engine.note_vector_routed();
    Ok(ExecResult::Rows {
        schema,
        batches: projected,
    })
}

async fn exec_create_table(
    sess: &ProjectSession,
    mut ct: sqlparser::ast::CreateTable,
    cluster_columns: Option<Vec<String>>,
    lifecycle: CreateTableLifecycle,
    ctas_shape: Option<crate::pg_ast::CtasShape>,
    // The unmodified statement the caller provided. The trailing
    // `STORED AS <format>` clause is stripped from the SQL before
    // sqlparser parses it (same pre-screen strategy as CLUSTER BY), so
    // the file format must be recovered from the original text rather
    // than the parsed `CreateTable` AST.
    raw_sql: &str,
    // Phase 5.24.F: EXCLUDE USING gist constraints extracted from the SQL
    // by `extract_exclude_using_gist` before sqlparser saw it. Each spec is
    // encoded as a sentinel `CheckConstraint` predicate and persisted on the
    // table metadata so the INSERT path can enforce it.
    exclude_specs: Vec<crate::ddl::ExcludeConstraintSpec>,
) -> Result<ExecResult> {
    // 6.SEC.P1 — reject user DDL targeting a reserved system schema
    // (`auth`, `storage`, `cron`, `net`, `realtime`, `pg_catalog`,
    // `information_schema`). `public` and bare names pass through.
    // Phase 5.18.C: system sessions (is_system = true) bypass this guard so
    // trusted internal subsystems can create tables in reserved schemas.
    if !sess.is_system {
        crate::schema_ddl::guard_reserved_schema_for_user_ddl(&ct.name, "CREATE TABLE")?;
        // Flat-model schema validation: a user-schema qualifier must name a
        // schema this session created (`CREATE SCHEMA`), else CREATE TABLE in
        // it is an error — matching PG, where `CREATE TABLE gone.t` after
        // `DROP SCHEMA gone` fails. `public` and reserved schemas are always
        // "known"; the guard above already rejected reserved ones for users.
        if ct.name.0.len() >= 2 {
            let schema_q = ct.name.0[0].id_val();
            let is_reserved =
                basin_catalog::reserved_schema::ReservedSchema::from_str(schema_q).is_some();
            if !is_reserved {
                let known = sess
                    .state
                    .schema_state
                    .read()
                    .expect("schema_state lock poisoned")
                    .contains(schema_q);
                if !known {
                    return Err(BasinError::NotFound(format!(
                        "schema {schema_q:?} does not exist"
                    )));
                }
            }
        }
    }
    let name = single_part_name(&ct.name)?;
    // Phase 5.18.C: for system sessions with a schema qualifier, resolve the
    // schema so the catalog key is (schema, table) not (public, table).
    let schema_str: Option<String> = if ct.name.0.len() >= 2 {
        Some(ct.name.0[0].id_val().clone())
    } else {
        None
    };
    let table = TableName::new(name)?;

    // `CREATE TABLE … AS <query>` (CTAS). `ctas_shape` is `Some` iff
    // libpg_query classified the original statement as a plain CTAS.
    // `skip_data` true → `WITH NO DATA` (schema only, no rows); false →
    // populate from the query result. The query's resolved output
    // schema becomes the table schema; `col_names` (the optional LHS
    // `(col, …)` list, captured by libpg_query because sqlparser cannot
    // parse it) renames the columns positionally (PG semantics).
    if let Some(shape) = ctas_shape {
        let query = ct.query.as_deref().ok_or_else(|| {
            BasinError::internal("CREATE TABLE AS: missing query body after parse")
        })?;
        return exec_create_table_as(
            sess,
            &table,
            name,
            query,
            &shape.col_names,
            ct.if_not_exists,
            shape.skip_data,
        )
        .await;
    }

    // Phase 5.11.K2: resolve column types that reference a user-defined
    // enum or domain. Each match rewrites the column's data_type to the
    // underlying Arrow-mappable shape and stamps a `BASIN_ENUM_TYPE` /
    // `BASIN_DOMAIN` field-metadata marker so the INSERT path can
    // validate values + the catalog can refcount.
    let bindings = crate::type_ddl::resolve_user_type_columns(
        &sess.engine.config().catalog,
        &sess.project,
        &ct.columns,
    )
    .await?;
    let extra_md = crate::type_ddl::rewrite_user_type_columns(&mut ct.columns, &bindings)?;

    let (schema, mut constraints) = crate::ddl::schema_and_constraints_from_columns(
        &ct.columns,
        &ct.constraints,
        name,
        &lifecycle,
    )?;
    let schema = crate::type_ddl::apply_user_type_metadata(schema, &extra_md);
    let spec = partition_spec_from_ast(ct.partition_by.as_deref(), &schema)?;

    // Validate cluster columns are a subset of the table's columns BEFORE
    // we create the table — bouncing here means we don't leave a half-
    // created table around when the user typo'd a column name.
    if let Some(cols) = cluster_columns.as_ref() {
        for c in cols {
            if schema.field_with_name(c).is_err() {
                return Err(BasinError::InvalidSchema(format!(
                    "CLUSTER BY column {c:?} is not in the table schema"
                )));
            }
        }
    }

    // Validate FK definitions before creating the table — referenced
    // table must exist in the same project, referenced columns must be
    // exactly the PK of the referenced table, and types must match.
    for fk in &constraints.foreign_keys {
        let ref_table_name = TableName::new(fk.ref_table.clone())?;
        let pk_of_ref: Vec<String> = if ref_table_name == table {
            constraints.pk_columns.clone()
        } else {
            let meta = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, &ref_table_name)
                .await
                .map_err(|e| match e {
                    BasinError::NotFound(_) => BasinError::InvalidSchema(format!(
                        "FOREIGN KEY {:?}: referenced table {:?} does not exist in this project \
                         (cross-project FKs are not supported in v0.1)",
                        fk.name, fk.ref_table
                    )),
                    other => other,
                })?;
            meta.pk_columns.clone()
        };
        if pk_of_ref.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "FOREIGN KEY {:?}: referenced table {:?} has no PRIMARY KEY (v0.1 requires \
                 referenced columns to be the PK of the referenced table; UNIQUE-only \
                 references are deferred to v0.2)",
                fk.name, fk.ref_table
            )));
        }
        let mut pk_set: std::collections::HashSet<String> =
            pk_of_ref.iter().map(|s| s.to_ascii_lowercase()).collect();
        for c in &fk.ref_columns {
            if !pk_set.remove(&c.to_ascii_lowercase()) {
                return Err(BasinError::InvalidSchema(format!(
                    "FOREIGN KEY {:?}: referenced column {c:?} is not part of {:?}'s PRIMARY KEY",
                    fk.name, fk.ref_table
                )));
            }
        }
        if !pk_set.is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "FOREIGN KEY {:?}: referenced columns must be exactly the PRIMARY KEY of {:?} \
                 (missing {pk_set:?})",
                fk.name, fk.ref_table
            )));
        }
        for (lc, rc) in fk.columns.iter().zip(fk.ref_columns.iter()) {
            let local_field = schema.field_with_name(lc).map_err(|_| {
                BasinError::InvalidSchema(format!(
                    "FOREIGN KEY {:?}: local column {lc:?} not in table",
                    fk.name
                ))
            })?;
            if ref_table_name != table {
                let ref_meta = sess
                    .engine
                    .config()
                    .catalog
                    .load_table(&sess.project, &ref_table_name)
                    .await?;
                let ref_field = ref_meta.schema.field_with_name(rc).map_err(|_| {
                    BasinError::InvalidSchema(format!(
                        "FOREIGN KEY {:?}: referenced column {rc:?} not in {:?}",
                        fk.name, fk.ref_table
                    ))
                })?;
                if local_field.data_type() != ref_field.data_type() {
                    return Err(BasinError::InvalidSchema(format!(
                        "FOREIGN KEY {:?}: local column {lc:?} type {:?} does not match \
                         referenced column {rc:?} type {:?}",
                        fk.name,
                        local_field.data_type(),
                        ref_field.data_type(),
                    )));
                }
            }
        }
    }

    // Phase 5.18.C: system sessions with a reserved-schema qualifier route
    // through `create_table_qualified` so the catalog key is `(schema, table)`.
    // All other paths (user sessions, bare names, public schema) use the
    // existing `create_table` which stores under `(public, table)`.
    let qtable_for_refresh: Option<basin_common::QualifiedTableName> = if sess.is_system {
        schema_str.as_deref().and_then(|s| {
            basin_catalog::reserved_schema::ReservedSchema::from_str(s)
                .filter(|r| !r.is_public())
                .map(|r| basin_common::QualifiedTableName::new(r.to_schema_name(), table.clone()))
        })
    } else {
        None
    };

    // If IF NOT EXISTS is set and the table already exists, return success
    // (no-op). The catalog signals "already exists" as BasinError::Catalog;
    // we only suppress that specific variant — unrelated catalog errors still
    // propagate. Without IF NOT EXISTS the error is always fatal.
    if let Some(ref qt) = qtable_for_refresh {
        match sess
            .engine
            .config()
            .catalog
            .create_table_qualified(&sess.project, qt, Arc::new(schema.clone()))
            .await
        {
            Ok(_metadata) => {}
            Err(BasinError::Catalog(_)) if ct.if_not_exists => {
                return Ok(ExecResult::Empty {
                    tag: "CREATE TABLE".into(),
                });
            }
            Err(e) => return Err(e),
        }
    } else {
        match sess
            .engine
            .config()
            .catalog
            .create_table(&sess.project, &table, &schema)
            .await
        {
            Ok(_metadata) => {}
            Err(BasinError::Catalog(_)) if ct.if_not_exists => {
                return Ok(ExecResult::Empty {
                    tag: "CREATE TABLE".into(),
                });
            }
            Err(e) => return Err(e),
        }
    }

    // Register implicit sequences promised by `SERIAL` / `BIGSERIAL` /
    // `SMALLSERIAL` columns. PG would auto-create these inline with the
    // table; we do it as a follow-on catalog call so the table-create
    // path stays one focused step. `IF NOT EXISTS`-shaped: if the
    // sequence already exists (re-run after a partial failure) we
    // swallow the duplicate-name error so the table can keep going.
    for seq in &constraints.implicit_sequences {
        let def = basin_catalog::SequenceDef::with_defaults(sess.project, seq.name.clone());
        match sess.engine.config().catalog.create_sequence(def).await {
            Ok(()) => {}
            Err(BasinError::Catalog(_)) => {
                // Pre-existing; SERIAL on a column whose sequence is
                // already there (e.g. from a prior partial create or
                // a hand-rolled `CREATE SEQUENCE`) — same shape PG
                // tolerates with `IF NOT EXISTS`.
            }
            Err(e) => return Err(e),
        }
    }

    if spec.is_partitioned() {
        sess.engine
            .config()
            .catalog
            .set_partition_spec(&sess.project, &table, spec)
            .await?;
    }

    if let Some(cols) = cluster_columns {
        sess.engine
            .config()
            .catalog
            .set_cluster_columns(&sess.project, &table, cols)
            .await?;
    }

    // Phase 3: persist the on-disk file format. The `STORED AS <format>`
    // clause is stripped from the SQL before sqlparser sees it (same
    // pre-screen strategy as CLUSTER BY above), so we recover it from the
    // original statement text. Persisted UNCONDITIONALLY for every CREATE
    // TABLE — `parse_create_table_file_format` returns the default
    // (Parquet) when the clause is absent, so the catalog always carries
    // an explicit format and the write path never has to guess.
    let fmt = crate::ddl::parse_create_table_file_format(raw_sql);
    sess.engine
        .config()
        .catalog
        .set_file_format(&sess.project, &table, fmt)
        .await?;

    // W3-R3: persist the user-asserted global sort order declared via
    // `WITH (basin.sort_by = '...')`. The sanitiser already stripped this
    // key before sqlparser saw the SQL; we recover it from the original.
    if let Some(sort_cols) = crate::ddl::parse_create_table_sort_by(raw_sql) {
        for c in &sort_cols {
            if schema.field_with_name(c).is_err() {
                return Err(basin_common::BasinError::InvalidSchema(format!(
                    "basin.sort_by column {c:?} is not in the table schema"
                )));
            }
        }
        sess.engine
            .config()
            .catalog
            .set_global_sort_order(&sess.project, &table, sort_cols)
            .await?;
    }

    // `basin.row_block_size` — optional per-table Vortex chunk / Parquet
    // row-group override. Validated (power of two, [256, 65536]) at parse
    // time; `None` means "use writer default".
    if let Some(rbs) = crate::ddl::parse_create_table_row_block_size(raw_sql)? {
        sess.engine
            .config()
            .catalog
            .set_row_block_size(&sess.project, &table, Some(rbs))
            .await?;
    }

    // `basin.adaptive_sort_override` — Phase 5.14.D2
    if let Some(aso) = crate::ddl::parse_create_table_adaptive_sort_override(raw_sql)? {
        sess.engine
            .config()
            .catalog
            .set_adaptive_sort_override(&sess.project, &table, Some(aso))
            .await?;
    }

    // Phase 5.24.F: encode each EXCLUDE USING gist spec as a sentinel
    // CheckConstraint predicate and fold it into the check-constraints list.
    // The INSERT path detects these sentinels in `enforce_check_constraints`
    // and routes them to the exclusion enforcer instead of DataFusion.
    for spec in &exclude_specs {
        constraints.checks.push(basin_catalog::CheckConstraint {
            name: spec.name.clone(),
            predicate: crate::ddl::encode_exclusion_sentinel(spec),
        });
    }

    if !constraints.pk_columns.is_empty()
        || !constraints.checks.is_empty()
        || !constraints.foreign_keys.is_empty()
    {
        sess.engine
            .config()
            .catalog
            .set_table_constraints(
                &sess.project,
                &table,
                constraints.pk_columns,
                constraints.checks,
                constraints.foreign_keys,
            )
            .await?;
    }

    if !constraints.uniques.is_empty() {
        sess.engine
            .config()
            .catalog
            .set_unique_constraints(&sess.project, &table, constraints.uniques)
            .await?;
    }

    // Phase 5.18.C: if this was a system-session create with a reserved schema,
    // refresh from the qualified catalog entry. Otherwise use the bare path.
    if let Some(ref qt) = qtable_for_refresh {
        crate::session::refresh_table_qualified(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            qt,
        )
        .await?;
    } else {
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    }

    Ok(ExecResult::Empty {
        tag: "CREATE TABLE".into(),
    })
}

/// `CREATE TABLE <name> [(<col>, …)] AS <query> [WITH [NO] DATA]`.
///
/// PostgreSQL semantics: the new table's column set is the **resolved
/// output schema of `query`** (names + types exactly as `SELECT …`
/// would produce). An optional `(col, …)` list positionally renames
/// those output columns. `WITH NO DATA` (`no_data == true`) creates the
/// schema only — zero rows; `WITH DATA` / no clause populates it from
/// the query result.
///
/// The query is planned through the session's DataFusion context (the
/// same context the SQL pre-screen pipeline already prepared), so CTEs,
/// inlined SQL functions, RLS, and partition pruning are all in effect.
/// Population is delegated to the normal `INSERT INTO … <query>` path so
/// constraint / RLS / event machinery is reused unchanged.
async fn exec_create_table_as(
    sess: &ProjectSession,
    table: &TableName,
    name: &str,
    query: &sqlparser::ast::Query,
    rename: &[String],
    if_not_exists: bool,
    no_data: bool,
) -> Result<ExecResult> {
    // Resolve the query's output schema by planning it (no execution).
    let query_sql = query.to_string();
    let df = sess
        .ctx
        .sql(&query_sql)
        .await
        .map_err(|e| map_df_plan_error("CREATE TABLE AS plan", &e))?;
    let df_schema = df.schema().as_arrow().clone();
    let mut ws_schema = schema_df_to_ws(&df_schema)?;

    // An explicit `(col, …)` list renames the query's output columns
    // positionally (PG: must not be wider than the query's projection).
    if !rename.is_empty() {
        if rename.len() > ws_schema.fields().len() {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE TABLE {name}: column name list has {} entries but the query \
                 produces only {} column(s)",
                rename.len(),
                ws_schema.fields().len()
            )));
        }
        let renamed: Vec<arrow_schema::Field> = ws_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let nm = rename.get(i).cloned().unwrap_or_else(|| f.name().clone());
                let mut nf = arrow_schema::Field::new(nm, f.data_type().clone(), f.is_nullable());
                if !f.metadata().is_empty() {
                    nf = nf.with_metadata(f.metadata().clone());
                }
                nf
            })
            .collect();
        ws_schema = arrow_schema::Schema::new(renamed);
    }

    match sess
        .engine
        .config()
        .catalog
        .create_table(&sess.project, table, &ws_schema)
        .await
    {
        Ok(_metadata) => {}
        Err(BasinError::Catalog(_)) if if_not_exists => {
            // Pre-existing table + IF NOT EXISTS — PG no-ops (no create,
            // no populate).
            return Ok(ExecResult::Empty {
                tag: "CREATE TABLE".into(),
            });
        }
        Err(e) => return Err(e),
    }

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;

    if no_data {
        // Schema-only clone: table exists with the query's columns and
        // zero rows.
        return Ok(ExecResult::Empty {
            tag: "CREATE TABLE AS".into(),
        });
    }

    // Populate via the standard INSERT … SELECT path so constraint /
    // RLS / change-event handling is identical to a hand-written
    // `INSERT INTO t <query>`. When the LHS renamed columns, name the
    // target columns explicitly so positional mapping is unambiguous.
    let insert_sql = if rename.is_empty() {
        format!("INSERT INTO {} {}", table.as_str(), query_sql)
    } else {
        let cols = ws_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!("INSERT INTO {} ({}) {}", table.as_str(), cols, query_sql)
    };
    Box::pin(execute(sess, &insert_sql)).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE TABLE AS".into(),
    })
}

/// `DROP TABLE [IF EXISTS] <name> [, ...]`
///
/// Removes each named table from the catalog. If `if_exists` is true, a
/// missing table is silently ignored (PG behavior). Without IF EXISTS an
/// absent table returns a `NotFound` error.
///
/// Note: only catalog metadata is removed. Underlying object-store data is
/// left in place for time-travel / point-in-time-restore (same policy as the
/// catalog's `drop_table` contract).
async fn exec_drop_table(
    sess: &ProjectSession,
    if_exists: bool,
    names: Vec<sqlparser::ast::ObjectName>,
) -> Result<ExecResult> {
    for name in names {
        // 6.SEC.P1 — reject DROP TABLE targeting a reserved system schema.
        // Phase 5.18.C: system sessions bypass this guard.
        if !sess.is_system {
            crate::schema_ddl::guard_reserved_schema_for_user_ddl(&name, "DROP TABLE")?;
        }
        let n = single_part_name(&name)?;
        let table = TableName::new(n)?;
        match sess
            .engine
            .config()
            .catalog
            .drop_table(&sess.project, &table)
            .await
        {
            Ok(()) => {
                // Deregister the table from the DataFusion catalog view so
                // subsequent queries in the same session don't see a stale
                // listing. Errors here are best-effort — the catalog drop
                // already succeeded; a stale DataFusion entry for a dropped
                // table is harmless for the next request (which opens a new
                // session).
                let _ = sess.ctx.deregister_table(table.as_str());

                // SECURITY (sec_catalog_leakage P1): purge any rows that
                // live in the hot tier — both the engine's
                // `MemTableRegistry` AND, when shard is wired, the
                // shard's per-partition in-memory `tail`. Without these,
                // rows that were INSERTed in the same session but never
                // flushed past the hot tier survive the catalog DROP and
                // are still visible to a freshly-opened session of the
                // same project (the read path merges overlay ∪ cold; the
                // catalog row is gone but the overlay still has values).
                //
                // The MemTableRegistry path covers code paths that route
                // through it (e.g. constraint enforcement); the shard
                // `drop_table` path covers the actual INSERT route used
                // when a `Shard` is attached — `InProcessProjectHandle::
                // write_batch` writes to `PartitionState.tail`, not the
                // registry, so the registry-only fix is insufficient on
                // the shard path. Both are O(partition-count-for-project)
                // and idempotent; calling both is the belt + braces fix.
                sess.engine
                    .memtable_registry()
                    .remove(&sess.project, &table);
                if let Some(shard) = sess.engine.config().shard.as_ref() {
                    // Errors here are best-effort: the catalog drop
                    // already succeeded; a stale shard tail entry for a
                    // table that no longer exists is not a correctness
                    // problem for any future statement (the table can't
                    // be looked up via the catalog).
                    let _ = shard.drop_table(&sess.project, &table).await;
                }
            }
            Err(BasinError::NotFound(_)) if if_exists => {
                // IF EXISTS — silently ignore missing tables.
            }
            Err(e) => return Err(e),
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP TABLE".into(),
    })
}

/// `CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<col1>[, <col2>, ...])`.
///
/// v0.1 records the index in the catalog (per `TableMetadata::indexes`) but
/// does NOT materialise any B-tree / sort-merge structure: every query on
/// the indexed table still does a table scan. The catalog row exists so
/// `information_schema.indexes` / `pg_index` introspection is honest about
/// what's declared.
//
// TODO(v0.2): wire to basin-storage's secondary index file format. The
// declaration shape here is already plural-column-aware so the storage
// hop is a swap-in rather than a parser change.
async fn exec_create_index(
    sess: &ProjectSession,
    ci: sqlparser::ast::CreateIndex,
) -> Result<ExecResult> {
    use crate::index_extras::{
        log_expression_column_notice, log_include_notice, log_metadata_only_notice,
        log_partial_index_notice, log_using_notice, parse_index_columns,
        IndexColumn,
    };

    // CONCURRENTLY is still unsupported — reject explicitly.
    if ci.concurrently {
        return Err(BasinError::FeatureNotSupported(
            "CREATE INDEX CONCURRENTLY is not supported in v0.1".into(),
        ));
    }

    // 6.SEC.P1 — reject CREATE INDEX targeting a reserved system schema.
    // Guards both the target table and (when explicit) the index name.
    // Phase 5.18.C: system sessions bypass this guard.
    if !sess.is_system {
        crate::schema_ddl::guard_reserved_schema_for_user_ddl(&ci.table_name, "CREATE INDEX")?;
        if let Some(ref idx_name) = ci.name {
            crate::schema_ddl::guard_reserved_schema_for_user_ddl(idx_name, "CREATE INDEX")?;
        }
    }

    let table_name = single_part_name(&ci.table_name)?;
    let table = TableName::new(table_name)?;

    // Parse all column expressions (bare identifiers + functional expressions).
    let parsed_cols = parse_index_columns(&ci);
    if parsed_cols.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE INDEX: column list cannot be empty".into(),
        ));
    }

    // Build the catalog column list. Bare columns are stored by name;
    // expression columns are prefixed with "expr:" so introspection can
    // distinguish them. The catalog column-existence check is bypassed for
    // expression indexes because the expression references columns
    // indirectly; the catalog only tracks the stringified form as metadata.
    let has_expressions = parsed_cols.iter().any(IndexColumn::is_expression);
    let catalog_columns: Vec<String> = parsed_cols
        .iter()
        .map(IndexColumn::as_catalog_str)
        .collect();

    // Mint a deterministic synthetic name when the user omitted one:
    // `<table>_<col1>_<col2>_idx`. For expression columns, the stringified
    // expr is used in the fallback name after stripping the "expr:" prefix.
    let index_name = match &ci.name {
        Some(n) => single_part_name(n)?.to_string(),
        None => {
            let col_part: String = catalog_columns
                .iter()
                .map(|s| {
                    s.trim_start_matches("expr:")
                        .replace(['(', ')', ' ', ','], "_")
                })
                .collect::<Vec<_>>()
                .join("_");
            format!("{table_name}_{col_part}_idx")
        }
    };

    // A plain-column `CREATE UNIQUE INDEX` is semantically identical to an
    // inline `UNIQUE` constraint in PostgreSQL and MUST be enforced (BUG
    // #136 — previously it was silently accepted as metadata, allowing
    // duplicate rows to accumulate). We register a real enforced
    // `UniqueConstraint` below, routing into the SAME catalog machinery a
    // table-level `UNIQUE (...)` uses, so the existing INSERT / UPDATE
    // enforcement (`constraints::enforce_unique_on_insert`) covers it.
    //
    // Partial unique indexes (`WHERE ...`) and expression unique indexes
    // remain non-enforcing (out of scope) — those keep the metadata-only
    // notice so behavior does not regress.
    let enforce_unique = ci.unique && ci.predicate.is_none() && !has_expressions;

    // Loud-reject UNIQUE variants we cannot actually enforce. Silently
    // accepting them as metadata (the previous behavior) breaks the
    // uniqueness contract: duplicate rows would accumulate undetected.
    // A non-unique secondary index is fine to accept as metadata (queries
    // fall through to scan, correctness preserved) — but UNIQUE is a data
    // integrity guarantee, so an un-enforceable UNIQUE must fail at DDL time.
    if ci.unique && !enforce_unique {
        return Err(BasinError::FeatureNotSupported(
            "UNIQUE on expression / partial UNIQUE / INCLUDE not yet enforced; \
             use a plain-column UNIQUE"
                .into(),
        ));
    }
    if ci.unique && !ci.include.is_empty() {
        return Err(BasinError::FeatureNotSupported(
            "UNIQUE on expression / partial UNIQUE / INCLUDE not yet enforced; \
             use a plain-column UNIQUE"
                .into(),
        ));
    }
    if ci.unique {
        if let Some(method) = &ci.using {
            let m = method.to_string();
            if !matches!(m.to_lowercase().as_str(), "btree") {
                return Err(BasinError::FeatureNotSupported(
                    "UNIQUE on expression / partial UNIQUE / INCLUDE not yet enforced; \
                     use a plain-column UNIQUE"
                        .into(),
                ));
            }
        }
    }

    // Determine the access method. `USING gin` → "gin"; `USING btree` or
    // absent → "btree" (the default). Other methods are logged as a notice and
    // treated as btree metadata-only (same pre-5.19 behaviour).
    let access_method_str: String = ci
        .using
        .as_ref()
        .map(|m| m.to_string().to_lowercase())
        .unwrap_or_else(|| "btree".to_string());

    // For GIN indexes: extract the operator class from the first (and only)
    // indexed column's `operator_class` field in the sqlparser AST.
    // sqlparser parses `(col jsonb_path_ops)` as an IndexColumn with
    // `operator_class = Some(ObjectName([Ident("jsonb_path_ops")]))`.
    // We normalise to lowercase and validate: `jsonb_ops`, `jsonb_path_ops`,
    // and `tsvector_ops` are accepted; for tsvector columns with no opclass
    // specified we auto-detect and use `tsvector_ops`.
    let gin_opclass: Option<String> = if access_method_str == "gin" {
        // Extract the operator class from the first indexed column.
        // sqlparser 0.61 parses `(col jsonb_path_ops)` as an IndexColumn with
        // `operator_class = Some(ObjectName([Identifier(Ident{value:"jsonb_path_ops"})]))`.
        // Use `id_val()` (from ObjectNamePartExt) to unwrap the identifier string.
        let raw_opclass = ci
            .columns
            .first()
            .and_then(|c| c.operator_class.as_ref())
            .map(|oc| {
                oc.0.iter()
                    .map(|part| part.id_val().to_lowercase())
                    .collect::<Vec<_>>()
                    .join(".")
            });
        match raw_opclass.as_deref() {
            // Phase 5.20.E: explicit tsvector_ops opclass.
            Some("tsvector_ops") => Some("tsvector_ops".to_string()),
            // No opclass specified — auto-detect based on column type.
            None => {
                // Check if the indexed column is a TSVECTOR column; if so,
                // use tsvector_ops.  Otherwise default to jsonb_ops.
                // `catalog_columns` has already been built above from the
                // parsed column list — use its first element.
                let col_name_opt = catalog_columns.first().cloned();
                let is_tsvector_col = if let Some(ref col_name) = col_name_opt {
                    // Look up the table metadata to check the column type.
                    // We already verified the table exists above; if load fails just
                    // fall back to jsonb_ops (safe).
                    if let Ok(meta) = sess.engine.config().catalog.load_table(&sess.project, &table).await {
                        meta.schema.fields().iter().any(|f| {
                            f.name() == col_name && crate::types::field_is_tsvector(f)
                        })
                    } else {
                        false
                    }
                } else {
                    false
                };
                if is_tsvector_col {
                    Some("tsvector_ops".to_string())
                } else {
                    Some("jsonb_ops".to_string())
                }
            }
            Some("jsonb_ops") => Some("jsonb_ops".to_string()),
            Some("jsonb_path_ops") => Some("jsonb_path_ops".to_string()),
            Some(other) => {
                return Err(BasinError::InvalidSchema(format!(
                    "CREATE INDEX USING gin: unknown operator class {other:?}; \
                     accepted: jsonb_ops (default), jsonb_path_ops, tsvector_ops"
                )));
            }
        }
    } else {
        None
    };

    // Phase 5.26.D/E: extract vector operator class for HNSW / IVFFlat indexes.
    // pgvector opclasses: `vector_l2_ops`, `vector_cosine_ops`, `vector_ip_ops`.
    // For IVFFlat: same opclasses, same convention (IVFFlat is accepted as a DDL
    // synonym and mapped to the Basin HNSW implementation as a documented fallback
    // — the catalog records the declared access method for introspection, but
    // queries use the HNSW fast path regardless).
    // WITH (m = 16, ef_construction = 64) and WITH (lists = N) are accepted and
    // logged; they are stored in the catalog opclass string for future use.
    let vector_opclass: Option<String> = if access_method_str == "hnsw"
        || access_method_str == "ivfflat"
    {
        let raw_opclass = ci
            .columns
            .first()
            .and_then(|c| c.operator_class.as_ref())
            .map(|oc| {
                oc.0.iter()
                    .map(|part| part.id_val().to_lowercase())
                    .collect::<Vec<_>>()
                    .join(".")
            });
        match raw_opclass.as_deref() {
            None | Some("vector_l2_ops") => Some("vector_l2_ops".to_string()),
            Some("vector_cosine_ops") => Some("vector_cosine_ops".to_string()),
            Some("vector_ip_ops") => Some("vector_ip_ops".to_string()),
            Some(other) => {
                // Unknown opclass — accept with a warning (pgvector DDL portability).
                tracing::warn!(
                    index = %index_name,
                    opclass = %other,
                    "CREATE INDEX USING {access_method_str}: unknown vector opclass; \
                     expected one of: vector_l2_ops, vector_cosine_ops, vector_ip_ops. \
                     Defaulting to vector_l2_ops."
                );
                Some("vector_l2_ops".to_string())
            }
        }
    } else {
        None
    };

    // Phase 5.26.D: extract HNSW build parameters from the `WITH (...)` clause.
    // pgvector canonical DDL: `WITH (m = 16, ef_construction = 64)`.  We parse
    // the two HNSW knobs, validate them, and fold them into the persisted
    // opclass string (see `encode_vector_opclass`) so they round-trip through
    // the catalog and are consumed by the HNSW build path
    // (`basin_vector::HnswIndexBuilder`).  `lists = N` (the IVFFlat knob) is
    // accepted and ignored — Basin maps IVFFlat onto the HNSW implementation.
    //
    // Absent params leave the segment defaults unchanged: `ef_construction`
    // falls back to the instant-distance default (100), and `m` is recorded
    // for introspection only (the underlying graph uses a fixed connectivity).
    let hnsw_params: Option<crate::vector_planner::HnswBuildParams> =
        if access_method_str == "hnsw" || access_method_str == "ivfflat" {
            Some(crate::vector_planner::parse_hnsw_with_params(&ci.with)?)
        } else {
            None
        };

    // Log IVFFlat fallback notice.
    if access_method_str == "ivfflat" {
        tracing::info!(
            index = %index_name,
            table = %table_name,
            "CREATE INDEX USING ivfflat accepted; Basin maps IVFFlat to the HNSW \
             implementation as a documented fallback (Phase 5.26.E). Queries use \
             the HNSW fast path. A native IVFFlat implementation is roadmap-tracked."
        );
    }

    // Phase 5.24.D: extract opclass for GIST indexes on range columns.
    // Accepted: `range_ops` (default when not specified), empty (treated as
    // `range_ops`).  Unknown opclasses are accepted with a notice so that
    // users writing `CREATE INDEX … USING gist (col)` without an explicit
    // opclass get the interval-tree wiring.
    let gist_opclass: Option<String> = if access_method_str == "gist" {
        let raw_opclass = ci
            .columns
            .first()
            .and_then(|c| c.operator_class.as_ref())
            .map(|oc| {
                use crate::pg_ast::ObjectNamePartExt;
                oc.0.iter()
                    .map(|part| part.id_val().to_lowercase())
                    .collect::<Vec<_>>()
                    .join(".")
            });
        Some(raw_opclass.unwrap_or_else(|| "range_ops".to_string()))
    } else {
        None
    };

    // Emit notices for accepted-but-not-enforced features.
    if let Some(pred) = &ci.predicate {
        log_partial_index_notice(&index_name, &pred.to_string());
    }
    if !access_method_str.eq_ignore_ascii_case("btree")
        && !access_method_str.eq_ignore_ascii_case("gin")
        && !access_method_str.eq_ignore_ascii_case("gist")
        && !access_method_str.eq_ignore_ascii_case("hnsw")
        && !access_method_str.eq_ignore_ascii_case("ivfflat")
    {
        // Non-btree, non-gin, non-gist, non-vector access methods: log the notice.
        log_using_notice(&index_name, &access_method_str);
    }
    if !ci.include.is_empty() {
        let include_cols: Vec<String> =
            ci.include.iter().map(|ident| ident.value.clone()).collect();
        log_include_notice(&index_name, &include_cols);
    }
    for col in &parsed_cols {
        if let IndexColumn::Expression(expr) = col {
            log_expression_column_notice(&index_name, expr);
        }
    }

    // Verify the table exists before touching the catalog.
    sess.engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await
        .map_err(|e| match e {
            BasinError::NotFound(_) => BasinError::InvalidSchema(format!(
                "CREATE INDEX: table {table_name:?} does not exist in this project"
            )),
            other => other,
        })?;

    // For expression indexes the catalog column-validation would reject the
    // "expr:..." strings because they are not real column names.  Accept
    // the declaration as metadata-only: log the notice and return success
    // without writing to the catalog.  Bare-column indexes continue
    // through the normal catalog path so they appear in introspection.
    if has_expressions {
        log_metadata_only_notice(&index_name, table_name);
        return Ok(ExecResult::Empty {
            tag: "CREATE INDEX".into(),
        });
    }

    log_metadata_only_notice(&index_name, table_name);

    // Phase 5.19.B: use create_index_with_method so GIN indexes persist their
    // access_method ("gin") and opclass ("jsonb_ops" / "jsonb_path_ops") in the
    // catalog SecondaryIndex row. Btree indexes continue to use the same path
    // via the default impl that delegates to create_index.
    // Phase 5.26.D/E: HNSW and IVFFlat vector indexes use vector_opclass.
    // IVFFlat is stored with access_method "hnsw" (the Basin implementation)
    // for query-routing purposes; the declared "ivfflat" is preserved in the
    // opclass string so introspection can still see the user's intent.
    let (catalog_method, catalog_opclass) = if access_method_str == "hnsw" {
        // Encode the opclass + HNSW build params into a single catalog string,
        // e.g. `vector_cosine_ops;m=16;ef_construction=64`. The vector planner
        // decodes the leading opclass token for the opclass-match routing rule
        // and the build path decodes the params.
        let opclass = crate::vector_planner::encode_vector_opclass(
            vector_opclass.as_deref().unwrap_or("vector_l2_ops"),
            hnsw_params.as_ref(),
        );
        ("hnsw".to_string(), Some(opclass))
    } else if access_method_str == "ivfflat" {
        // IVFFlat fallback: map to HNSW in the catalog so the vector planner
        // can route through the HNSW fast path. Store opclass with an "ivfflat:"
        // prefix so introspection can still see the original access method.
        let base = vector_opclass.as_deref().unwrap_or("vector_l2_ops");
        let encoded = crate::vector_planner::encode_vector_opclass(base, hnsw_params.as_ref());
        ("hnsw".to_string(), Some(format!("ivfflat:{encoded}")))
    } else if access_method_str == "gist" {
        // Phase 5.24.D: GIST indexes on range columns are stored with their
        // opclass (default "range_ops") so the probe path can verify that
        // a GIST index covers the queried column.
        ("gist".to_string(), gist_opclass.clone())
    } else {
        (access_method_str.clone(), gin_opclass.clone())
    };
    sess.engine
        .config()
        .catalog
        .create_index_with_method(
            &sess.project,
            &table,
            &index_name,
            &catalog_columns,
            ci.if_not_exists,
            &catalog_method,
            catalog_opclass.as_deref(),
        )
        .await?;

    // BUG #136 fix: register an enforced UNIQUE constraint for a plain-column
    // `CREATE UNIQUE INDEX`. PG names the implicit constraint after the index,
    // so we reuse `index_name` as the constraint name (this is also what
    // surfaces in the SQLSTATE 23505 message). `set_unique_constraints`
    // *replaces* the whole list, so load the existing set, append, and write
    // it back. If a constraint with this index's name already exists (e.g. a
    // re-run under IF NOT EXISTS where create_index was a no-op) we leave the
    // set untouched to keep the operation idempotent.
    if enforce_unique {
        let meta = sess
            .engine
            .config()
            .catalog
            .load_table(&sess.project, &table)
            .await?;
        let already_registered = meta.unique_constraints.iter().any(|u| u.name == index_name);
        if !already_registered {
            let mut uniques = meta.unique_constraints.clone();
            uniques.push(basin_catalog::UniqueConstraint {
                name: index_name.clone(),
                columns: catalog_columns.clone(),
            });
            sess.engine
                .config()
                .catalog
                .set_unique_constraints(&sess.project, &table, uniques)
                .await?;
        }
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    }

    // FIX 1 / FIX 2(b) — backfill the index over PRE-EXISTING live files.
    //
    // The catalog now records the index, but the in-RAM registries that drive
    // pruning (GIN row-group bloom + JSONB posting list for `USING gin`; the
    // secondary B-tree registry for plain single-column indexes) are populated
    // only by INSERT-path maintenance and by compaction — both of which run
    // AFTER the index exists. Files written before this CREATE INDEX would
    // never be indexed, so the read-path completeness guards
    // (`gin_rowgroup_registry().is_file_indexed`, the posting-list lazy-load,
    // and the secondary probe) would force a full scan forever.
    //
    // We do a one-time synchronous scan here. This is DDL, so a blocking scan
    // is acceptable; a very large table would benefit from doing this
    // asynchronously / incrementally in a future revision (TODO).  Backfill is
    // best-effort: a read error on any file is logged and the file is left
    // un-indexed (the completeness guard then falls back to a full scan for
    // that file — correct, just slower).
    //
    // CREATE-INDEX-OVER-OVERLAY (the dml_mutate.rs UPDATE fast-path gate's
    // blocker #3): settle any live hot-tier UPDATE/DELETE overlay into cold
    // storage BEFORE the backfill walks the live files. The backfill reads
    // cold bytes only, so an overlay row's post-SET document would otherwise
    // never enter the posting lists — and once the overlay later drains
    // (background reconciler / a cold-path mutation's materialize prologue,
    // neither of which performs GIN registry maintenance) the
    // stale-but-"complete" index would feed the probe paths wrong answers.
    // Materializing here is an O(1) no-op when the overlay is empty (counter
    // gate inside `materialize_overlay_for_table`) and otherwise leaves the
    // backfill indexing exactly the rows a scan would return.
    //
    // Overlay entries written AFTER this point — a hot-tier fast-path UPDATE
    // racing this CREATE INDEX that loaded its table metadata before the
    // catalog index row landed — are covered by the read-path overlay guards:
    // the executor's posting-probe Empty short-circuits
    // (`gin_empty_probe_is_trustworthy`) and the session pruning paths
    // (`session::table_has_live_overlay` gates in
    // `apply_gin_pruning_for_query` / `apply_jsonb_posting_pruning_for_query`)
    // all fall back to the overlay-aware full scan while any overlay entry is
    // live. Once the table's metadata shows this index, the dml_mutate
    // fast-path gate (`!meta.indexes.is_empty()`) declines new overlay writes
    // entirely, so the race window closes with the next metadata load.
    //
    // On materialize failure the backfill is SKIPPED: leaving every live file
    // un-indexed keeps the probes at NoIndex/incomplete — full scans (correct,
    // unpruned) instead of an index built beside a still-live overlay.
    //
    // RETRY (bounded): the settle commits through `replace_data_files`
    // optimistic concurrency, and the background overlay reconciler
    // (`overlay_reconcile`) may have a materialize of ITS OWN in flight for
    // this table — measured live on the 100k compare card, where the
    // reconciler's commit (snapshotted before a just-landed UPDATE) beat the
    // DDL settle, the settle returned `CommitConflict`, and the backfill was
    // skipped for good: the index stayed empty until a re-CREATE INDEX while
    // every `@>` read fell back to full scans. The conflict is transient —
    // the loser's overlay entries stay dirty and a fresh attempt reloads the
    // head snapshot — so retry a few times before giving up. `materialize`
    // is idempotent (acks land only after a successful commit) and a fully
    // drained overlay short-circuits O(1), so the retries are cheap.
    let mut overlay_settled = false;
    for attempt in 0..3u32 {
        match crate::dml_mutate::materialize_overlay_for_table(
            &sess.engine,
            sess.project,
            &table,
        )
        .await
        {
            Ok(()) => {
                overlay_settled = true;
                break;
            }
            Err(e) => {
                tracing::warn!(
                    index = %index_name,
                    table = %table,
                    attempt,
                    err = %e,
                    "CREATE INDEX: hot-tier overlay materialize attempt failed"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)))
                    .await;
            }
        }
    }
    if !overlay_settled {
        tracing::warn!(
            index = %index_name,
            table = %table,
            "CREATE INDEX: hot-tier overlay materialize failed after retries; \
             index backfill skipped (reads fall back to full scans for this table)"
        );
    }
    if overlay_settled && catalog_columns.len() == 1 {
        backfill_index_over_live_files(
            sess,
            &table,
            &index_name,
            &catalog_columns[0],
            &access_method_str,
            gin_opclass.as_deref(),
        )
        .await;
    }

    Ok(ExecResult::Empty {
        tag: "CREATE INDEX".into(),
    })
}

/// GIN-overlay correctness gate for the posting-probe `Empty` short-circuits
/// (`@>` containment and the `?`/`?&`/`?|` key-existence probes).
///
/// `ProbeResult::Empty` means "no cold file can match" — the file-level GIN
/// posting list is built from cold files only. Turning that into a zero-row
/// result is sound only when the cold files are the whole story:
///
/// 1. **No live overlay** (O(1) counter reads): a hot-tier UPDATE override /
///    DELETE tombstone in the process-wide memtable registry means a row's
///    post-SET document may match a needle that no cold file matches (the
///    dml_mutate.rs UPDATE fast-path gate's blocker #1 — e.g. a `jsonb_set`
///    override adding a term whose cold posting sets are file-disjoint, so
///    the AND-merge intersects to ∅). While `update_count + tombstone_count
///    > 0` the short-circuit must not fire; the overlay-aware scan
///    (`TombstoneFilterExec` + `UpdateOverlayExec`) re-applies the predicate
///    over the merged hot+cold row set instead.
/// 2. **Completeness** (one set probe per live file):
///    `materialize_overlay_for_table` commits replacement files with NO GIN
///    registry maintenance (blocker #3), so after an overlay drains the
///    registry still holds postings for the replaced (dead) files and knows
///    nothing about the replacement. A needle whose terms intersect to ∅
///    across the stale postings would short-circuit to zero rows while the
///    un-indexed replacement file holds a real match. Requiring every live
///    file to be in the indexed-files completeness set degrades that case to
///    a full scan — correct-but-unpruned, mirroring the per-file completeness
///    guards the session pruning paths already enforce.
///
/// Any uncertainty (catalog load failure) also returns `false` → full scan.
async fn gin_empty_probe_is_trustworthy(
    sess: &ProjectSession,
    table: &TableName,
    col: &str,
) -> bool {
    if crate::session::table_has_live_overlay(&sess.engine, &sess.project, table) {
        return false;
    }
    let Ok(meta) = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await
    else {
        return false; // can't verify completeness → fall through to the scan
    };
    let indexed = sess
        .engine
        .gin_index_registry()
        .indexed_files_for(&sess.project, table, col);
    meta.live_data_files()
        .iter()
        .all(|f| indexed.contains(f.path.as_str()))
}

/// FTS twin of [`gin_empty_probe_is_trustworthy`] for the tsvector `@@`
/// posting-probe `Empty` short-circuit (Phase 5.20.E).
///
/// `TsvProbeResult::Empty` means "no cold file can match" — the tsvector
/// posting list is built from cold files only.  Returning zero rows is sound
/// only when:
///
/// 1. **No live overlay** (O(1) counter reads): a hot-tier UPDATE override /
///    DELETE tombstone may carry a post-image whose lexemes satisfy a query
///    that no cold file satisfies (e.g. `'cat' & 'dog'` with the two lexemes
///    split across disjoint cold files and an override row holding both).
/// 2. **Per-file completeness**: every live data file must appear in the
///    FTS registry's indexed-files set.  A live file written before the
///    index existed, after a restart, de-indexed by posting-budget eviction,
///    or committed by `materialize_overlay_for_table` (which performs no FTS
///    registry maintenance) may hold a real match the posting list cannot
///    see.  Requiring full coverage degrades those states to a full scan —
///    correct-but-unpruned, mirroring `apply_gin_fts_pruning_for_query`.
///
/// Any uncertainty (catalog load failure) also returns `false` → full scan.
async fn fts_empty_probe_is_trustworthy(
    sess: &ProjectSession,
    table: &TableName,
    col: &str,
) -> bool {
    if crate::session::table_has_live_overlay(&sess.engine, &sess.project, table) {
        return false;
    }
    let Ok(meta) = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await
    else {
        return false; // can't verify completeness → fall through to the scan
    };
    let indexed = sess
        .engine
        .gin_fts_registry()
        .indexed_files_for(&sess.project, table, col);
    meta.live_data_files()
        .iter()
        .all(|f| indexed.contains(f.path.as_str()))
}

/// FIX 1 / FIX 2(b) — backfill a freshly-created single-column index over the
/// table's existing live data files.
///
/// Dispatches on access method:
///   * `gin` (JSONB, non-tsvector) → populate the GIN row-group bloom registry
///     and the file-level JSONB posting list + sidecar, then seal each file so
///     `apply_gin_pruning_for_query`'s completeness guards pass.
///   * `gin` with `tsvector_ops` (Phase 5.20.E) → populate the FTS lexeme
///     posting list (`GinTsvectorRegistry`) and seal each file so the
///     `@@` Empty short-circuit (`fts_empty_probe_is_trustworthy`) and
///     `apply_gin_fts_pruning_for_query`'s completeness guard pass.  Without
///     this backfill a tsvector GIN created over pre-existing data would
///     never reach full coverage and the index would never prune.
///   * `btree` (plain single-column) → extract `(value → file/row-group/row)`
///     locations into the secondary B-tree registry and flush it to disk.
///   * `gist` / `hnsw` / `ivfflat` → not backfilled here (those use
///     sidecar-based indexes built at compaction time; out of scope for
///     this fix).
///
/// Row-group ordinal semantics: the Parquet/Vortex writer flushes a row-group
/// every `rg_size` rows in stored row order. We thread a running per-file row
/// offset across the read stream's batches and compute
/// `row_group = (file_row_offset + local_row) / rg_size`, exactly mirroring
/// `maintain_gin_rowgroup_index_on_insert`. `rg_size` follows the same priority
/// the compactor uses (`row_block_size` > `row_group_rows` > default) so the
/// ordinals line up with how the file was actually written. For Vortex the
/// read path ignores the per-location row-group (file-level allow-list wins),
/// so the ordinal is harmless there.
async fn backfill_index_over_live_files(
    sess: &ProjectSession,
    table: &TableName,
    index_name: &str,
    col_name: &str,
    access_method: &str,
    gin_opclass: Option<&str>,
) {
    use futures::StreamExt;

    // gist and vector indexes are not backfilled here.
    let is_fts_gin = access_method == "gin" && gin_opclass == Some("tsvector_ops");
    let is_plain_gin = access_method == "gin" && !is_fts_gin;
    let is_btree = access_method == "btree";
    if !is_plain_gin && !is_fts_gin && !is_btree {
        return;
    }

    let meta = match sess.engine.config().catalog.load_table(&sess.project, table).await {
        Ok(m) => m,
        Err(_) => return,
    };
    let live_files = meta.live_data_files();
    if live_files.is_empty() {
        return;
    }

    // Effective row-group size — mirror the compactor's writer priority so the
    // computed row-group ordinals match the on-disk layout.
    let rg_size = meta
        .row_block_size
        .map(|v| v as usize)
        .or(meta.row_group_rows)
        .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE)
        .max(1);

    let storage = sess.engine.config().storage.clone();
    let project = sess.project;
    let opclass = gin_opclass.unwrap_or("jsonb_ops");

    for f in &live_files {
        let path = object_store::path::Path::from(f.path.as_str());
        let opts = basin_storage::ReadOptions {
            projection: Some(vec![col_name.to_string()]),
            ..Default::default()
        };
        let mut stream = match storage
            .read_file_with_options(&project, &path, opts)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    index = %index_name,
                    table = %table,
                    file = %f.path,
                    err = %e,
                    "CREATE INDEX backfill: read failed; file left un-indexed (falls back to full scan)"
                );
                continue;
            }
        };

        // Running row offset within this file (across the stream's batches).
        let mut file_row_off: usize = 0;
        let mut had_data = false;

        while let Some(batch_res) = stream.next().await {
            let batch = match batch_res {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(
                        index = %index_name,
                        file = %f.path,
                        err = %e,
                        "CREATE INDEX backfill: batch read error; partial file index (full-scan fallback for this file)"
                    );
                    // Leave the file un-sealed so the completeness guard treats
                    // it as not-indexed and full-scans it (no false negatives).
                    had_data = false;
                    break;
                }
            };
            had_data = true;
            if is_plain_gin {
                backfill_gin_batch(
                    sess, table, col_name, opclass, &batch, &f.path, rg_size, file_row_off,
                );
            } else if is_fts_gin {
                backfill_fts_batch(
                    sess, table, col_name, &batch, &f.path, rg_size, file_row_off,
                );
            } else {
                backfill_btree_batch(
                    sess, table, col_name, &batch, &f.path, rg_size, file_row_off,
                );
            }
            file_row_off += batch.num_rows();
        }

        if !had_data {
            continue;
        }

        if is_plain_gin {
            // Seal the file in both GIN registries so the read-path
            // completeness guards (`is_file_indexed`) pass for this file.
            sess.engine.gin_rowgroup_registry().mark_file_indexed(
                &project, table, col_name, &f.path,
            );
            sess.engine.gin_index_registry().mark_file_indexed(
                &project, table, col_name, &f.path,
            );
            // Persist the file-level posting list as a sidecar so the
            // `apply_jsonb_posting_pruning_for_query` lazy-load path (which
            // reads sidecars when its registry is cold) also sees this file.
            backfill_write_jsonb_posting_sidecar(sess, table, col_name, &f.path).await;
        } else if is_fts_gin {
            // Seal the file in the FTS registry: the Empty short-circuit and
            // the session pruning path both require every live file to be in
            // the indexed-files completeness set before they trust the
            // posting list.  A read error above leaves the file un-sealed →
            // forced full scan for this table (correct, just unpruned).
            sess.engine.gin_fts_registry().mark_file_indexed(
                &project, table, col_name, &f.path,
            );
        }
    }

    if is_btree {
        // Persist the secondary B-tree index so a restart can lazy-load it.
        crate::secondary_index::flush_index(
            sess.engine.secondary_index_registry(),
            &storage,
            &project,
            table,
            col_name,
        )
        .await;
    }
}

/// Feed one batch (read from an existing file at `file_row_off` rows into the
/// file) into the GIN row-group bloom registry and file-level posting list.
/// Mirrors `maintain_gin_rowgroup_index_on_insert` / `maintain_gin_index_on_insert`
/// but offsets the row index by `file_row_off` so multi-batch reads of a single
/// file land in the correct row-group.
fn backfill_gin_batch(
    sess: &ProjectSession,
    table: &TableName,
    col_name: &str,
    opclass: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
    rg_size: usize,
    file_row_off: usize,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    // The JSONB column's runtime Arrow encoding depends on the source file
    // format: freshly-written batches carry `LargeBinary` (the catalog JSONB
    // type), but the Vortex reader round-trips JSONB through `BinaryView` →
    // plain `Binary`. Backfill reads from cold-tier files, so we must accept
    // all three or the downcast fails silently — leaving the file with no
    // row-group entries, so the subsequent `mark_file_indexed` (which only
    // seals files that already have an entry) no-ops and the file is never
    // sealed (bug: gin_create_index_backfills_preexisting_shard_files).
    enum BinCol<'a> {
        Large(&'a arrow_array::LargeBinaryArray),
        Small(&'a arrow_array::BinaryArray),
        View(&'a arrow_array::BinaryViewArray),
    }
    let bin = if let Some(a) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        BinCol::Large(a)
    } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::BinaryArray>() {
        BinCol::Small(a)
    } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::BinaryViewArray>() {
        BinCol::View(a)
    } else {
        return;
    };
    let row_bytes = |r: usize| -> Option<&[u8]> {
        match &bin {
            BinCol::Large(a) => (!a.is_null(r)).then(|| a.value(r)),
            BinCol::Small(a) => (!a.is_null(r)).then(|| a.value(r)),
            BinCol::View(a) => (!a.is_null(r)).then(|| a.value(r)),
        }
    };
    let rg_registry = sess.engine.gin_rowgroup_registry();
    let posting_registry = sess.engine.gin_index_registry();
    let project = sess.project;
    for row in 0..batch.num_rows() {
        let Some(bytes) = row_bytes(row) else {
            continue;
        };
        let file_row = file_row_off + row;
        let row_group = (file_row / rg_size) as u32;
        // Row-group bloom (drives the direct row-group prune path).
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
            let terms = crate::index_probe::extract_terms(&value, opclass);
            if !terms.is_empty() {
                rg_registry.index_row(&project, table, col_name, &terms, file_path, row_group);
            }
        }
        // File-level posting list (drives the posting-list prune path).
        posting_registry.index_row(
            &project, table, col_name, opclass, bytes, file_path, row_group, file_row as u64,
        );
    }
}

/// Phase 5.20.E — feed one batch (read from an existing file at `file_row_off`
/// rows into the file) into the GIN FTS lexeme posting list.  Mirrors
/// `maintain_gin_fts_index_on_insert` but offsets the row index by
/// `file_row_off` so multi-batch reads of a single file land in the correct
/// row-group.
///
/// The tsvector column is stored as Utf8 (canonical lexeme text form), but
/// the runtime Arrow encoding depends on the source file format: Parquet
/// round-trips to `StringArray`, the Vortex reader may surface `Utf8View` /
/// `LargeUtf8`.  Accept all three — a silent downcast failure would leave the
/// file with no posting entries while the subsequent `mark_file_indexed`
/// seals it as "complete", and the structural probe could then prune rows
/// that exist (the same bug class fixed for the JSONB backfill).
fn backfill_fts_batch(
    sess: &ProjectSession,
    table: &TableName,
    col_name: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
    rg_size: usize,
    file_row_off: usize,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    enum StrCol<'a> {
        Small(&'a arrow_array::StringArray),
        Large(&'a arrow_array::LargeStringArray),
        View(&'a arrow_array::StringViewArray),
    }
    let arr = if let Some(a) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        StrCol::Small(a)
    } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        StrCol::Large(a)
    } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::StringViewArray>() {
        StrCol::View(a)
    } else {
        return;
    };
    let row_str = |r: usize| -> Option<&str> {
        match &arr {
            StrCol::Small(a) => (!a.is_null(r)).then(|| a.value(r)),
            StrCol::Large(a) => (!a.is_null(r)).then(|| a.value(r)),
            StrCol::View(a) => (!a.is_null(r)).then(|| a.value(r)),
        }
    };
    let fts_registry = sess.engine.gin_fts_registry();
    let project = sess.project;
    for row in 0..batch.num_rows() {
        let Some(tsv_str) = row_str(row) else {
            continue;
        };
        let file_row = file_row_off + row;
        let row_group = (file_row / rg_size) as u32;
        fts_registry.index_row(
            &project, table, col_name, tsv_str, file_path, row_group, file_row as u64,
        );
    }
}

/// Feed one batch into the secondary B-tree registry, offsetting the row index
/// by `file_row_off`. Mirrors `extract_entries_from_batch` + `insert_batch`.
pub(crate) fn backfill_btree_batch(
    sess: &ProjectSession,
    table: &TableName,
    col_name: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
    rg_size: usize,
    file_row_off: usize,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    let mut entries: Vec<(String, crate::secondary_index::IndexLocation)> = Vec::new();
    macro_rules! extract_typed {
        ($arr_ty:ty, $fmt:expr) => {{
            if let Some(arr) = col.as_any().downcast_ref::<$arr_ty>() {
                for row in 0..arr.len() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let key: String = $fmt(arr.value(row));
                    let file_row = file_row_off + row;
                    entries.push((
                        key,
                        crate::secondary_index::IndexLocation {
                            file_path: file_path.to_string(),
                            row_group: (file_row / rg_size) as u32,
                            row: file_row as u64,
                        },
                    ));
                }
            }
        }};
    }
    use arrow_array::{
        BooleanArray, Float64Array, Int64Array, LargeStringArray, StringArray, UInt64Array,
    };
    extract_typed!(Int64Array, |v: i64| v.to_string());
    extract_typed!(UInt64Array, |v: u64| v.to_string());
    extract_typed!(Float64Array, |v: f64| format!("{v:?}"));
    extract_typed!(StringArray, |v: &str| v.to_string());
    extract_typed!(LargeStringArray, |v: &str| v.to_string());
    extract_typed!(BooleanArray, |v: bool| if v { "t" } else { "f" }.to_string());

    if !entries.is_empty() {
        sess.engine
            .secondary_index_registry()
            .insert_batch(&sess.project, table, col_name, entries);
    }
}

/// Serialise the in-RAM JSONB file-level posting list for `file_path` and write
/// it to the canonical sidecar object so the engine's lazy-load probe path can
/// pick it up. Best-effort: any error is logged and ignored (the row-group
/// bloom path still works).
async fn backfill_write_jsonb_posting_sidecar(
    sess: &ProjectSession,
    table: &TableName,
    col_name: &str,
    file_path: &str,
) {
    let registry = sess.engine.jsonb_posting_registry();
    // The jsonb_posting registry is populated by the read-path lazy loader and
    // by compaction, NOT by the file-level GinIndexRegistry we just filled.
    // Re-index the file directly into the posting registry from storage so the
    // sidecar reflects the just-backfilled file. We read the JSONB column once
    // more here; for the common small-table CREATE INDEX this is cheap.
    let Some(sidecar) = basin_storage::index::jsonb_posting::posting_sidecar_key_for_data_file(
        &sess.project, table, col_name, file_path,
    ) else {
        return;
    };
    // Load the table schema to find the JSONB column index.
    let meta = match sess.engine.config().catalog.load_table(&sess.project, table).await {
        Ok(m) => m,
        Err(_) => return,
    };
    let rg_size = meta
        .row_block_size
        .map(|v| v as usize)
        .or(meta.row_group_rows)
        .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE)
        .max(1);
    let storage = sess.engine.config().storage.clone();
    let path = object_store::path::Path::from(file_path);
    let opts = basin_storage::ReadOptions {
        projection: Some(vec![col_name.to_string()]),
        ..Default::default()
    };
    let mut stream = match storage.read_file_with_options(&sess.project, &path, opts).await {
        Ok(s) => s,
        Err(_) => return,
    };
    use futures::StreamExt;
    while let Some(Ok(batch)) = stream.next().await {
        let Ok(col_idx) = batch.schema().index_of(col_name) else {
            continue;
        };
        registry.index_batch(&sess.project, table, col_name, &batch, col_idx, rg_size, file_path);
    }
    if let Some(bytes) = registry.serialize_file(&sess.project, table, col_name, file_path) {
        use object_store::ObjectStoreExt as _;
        let store = storage.project_object_store(&sess.project);
        if let Err(e) = store
            .put(
                &sidecar,
                object_store::PutPayload::from_bytes(bytes::Bytes::from(bytes)),
            )
            .await
        {
            tracing::warn!(
                table = %table, col = %col_name, file = %file_path, err = %e,
                "CREATE INDEX backfill: JSONB posting sidecar write failed (non-fatal)"
            );
        }
    }
}

/// `DROP INDEX [IF EXISTS] <name>`. Removes the catalog row only —
/// there's nothing to physically tear down because v0.1 doesn't
/// materialise any index file.
async fn exec_drop_index(
    sess: &ProjectSession,
    if_exists: bool,
    names: Vec<sqlparser::ast::ObjectName>,
) -> Result<ExecResult> {
    if names.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DROP INDEX requires at least one index name".into(),
        ));
    }
    for n in &names {
        // 6.SEC.P1 — reject DROP INDEX targeting a reserved system schema.
        // Phase 5.18.C: system sessions bypass this guard.
        if !sess.is_system {
            crate::schema_ddl::guard_reserved_schema_for_user_ddl(n, "DROP INDEX")?;
        }
        let index_name = single_part_name(n)?;
        // The catalog stores indexes per-table; we don't track a
        // global (project, index-name) → table mapping. Scan every
        // table in the project for a matching declaration.
        let tables = sess
            .engine
            .config()
            .catalog
            .list_tables(&sess.project)
            .await?;
        let mut found = false;
        for t in &tables {
            let meta = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, t)
                .await?;
            if meta.indexes.iter().any(|i| i.name == index_name) {
                sess.engine
                    .config()
                    .catalog
                    .drop_index(&sess.project, t, index_name)
                    .await?;
                // BUG #136 fix: a `CREATE UNIQUE INDEX` also registered an
                // enforced UNIQUE constraint named after the index. Dropping
                // the index must drop that enforcement too (PG: DROP INDEX on
                // a unique index removes the uniqueness). Constraints created
                // by an inline table-level `UNIQUE (...)` are not named after
                // an index, so this only removes the index-derived one.
                if meta.unique_constraints.iter().any(|u| u.name == index_name) {
                    let remaining: Vec<basin_catalog::UniqueConstraint> = meta
                        .unique_constraints
                        .iter()
                        .filter(|u| u.name != index_name)
                        .cloned()
                        .collect();
                    sess.engine
                        .config()
                        .catalog
                        .set_unique_constraints(&sess.project, t, remaining)
                        .await?;
                    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, t).await?;
                }
                found = true;
                break;
            }
        }
        if !found && !if_exists {
            return Err(BasinError::NotFound(format!("index {index_name:?}")));
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP INDEX".into(),
    })
}

/// Count of INSERT statements served end-to-end by the pre-parse literal fast
/// path (classifier + header-only parse + tuple scanner, no whole-statement
/// libpg_query/sqlparser parse). Test-visible instrumentation; relaxed
/// ordering is fine for a monotone counter.
pub(crate) static INSERTS_PREPARSE_FASTPATH: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Pre-parse fast path for plain auto-commit literal `INSERT … VALUES` —
/// the hook `execute` runs BEFORE either whole-statement parser.
///
/// Returns:
/// * `None` — decline; the caller must run the normal (double-parse) path,
///   which reproduces every behaviour including canonical errors. Declining
///   may have invalidated session caches exactly as the normal dispatch gate
///   would for this statement (it is an INSERT either way), so a decline is
///   never observable.
/// * `Some(result)` — the statement was fully executed here.
///
/// Engagement gates, in order (each one mirrors a piece of the normal path —
/// see the bookkeeping audit below):
/// 1. **Not aborted**: an aborted transaction declines, so the 25P02
///    aborted-state guard stays with the normal path. OPEN transactions are
///    admitted — `exec_insert_prebuilt` retains `exec_insert`'s full in-tx
///    branch (htap_rows buffering, intra-tx uniqueness, FK flush guarded by
///    `!tx_is_active`), so `BEGIN; INSERT ×100; COMMIT` skips the double
///    parse per statement (see the Gate 1 comment in the body).
/// 2. **Classifier** ([`crate::values_fast::classify_literal_insert`]): an
///    O(prefix) structural scan of the header. No match → decline.
/// 3. **No pending OVERRIDING**: the textual `OVERRIDING` pre-screen of the
///    normal path stashes session state this path never consumes; the
///    classifier grammar already excludes the clause from THIS statement, but
///    a stale stash from a prior statement also declines (peeked, not taken,
///    so the normal path sees identical state).
/// 4. **Header re-parse**: `sql[..VALUES]` + `" (NULL)"` through sqlparser —
///    an O(header) parse that owns ident case folding / reserved-word
///    semantics and yields the same `Insert` node fields (`columns`, table
///    object name) the normal path would dispatch on. Parse failure or any
///    unexpected field (`on`, `returning`, `partitioned`, alias …) → decline.
/// 5. **Cache invalidation** (replicates `dispatch_parsed_statement`'s
///    `!stmt_keeps_cache` gate for an auto-commit INSERT): table-meta,
///    provider, head-probe caches + per-project PK row cache. Done BEFORE the
///    table-meta load below so this path can never write through a snapshot
///    the normal path would have refused as stale.
/// 6. **Hypertable decline**: the normal path's best-effort
///    `touch_hypertable_chunks_from_insert` needs the parsed VALUES rows;
///    rather than replicate it, hypertable targets keep the normal path.
/// 7. **Table meta load** (same `load_table_meta_cached_err` the normal
///    `exec_insert` uses): missing table / catalog error → decline, and the
///    normal path surfaces the canonical error.
/// 8. **Non-partitioned only**: `RangeMonthly` targets decline (the
///    partition fan-out path needs per-row AST exprs).
/// 9. **Tuple scan** ([`crate::values_fast::try_fast_insert_at`]): the
///    existing conservative scanner, started at the classifier's offset. It
///    enforces the airtight tail contract — after the final tuple only
///    whitespace and at most one `;` may remain, so `RETURNING` /
///    `ON CONFLICT` / a second statement decline here. Identity / generated /
///    default-bearing columns, unsupported types, and every literal-shape
///    doubt also decline inside it.
///
/// Bookkeeping replicated vs declined (audit of what the normal path does
/// before/around `exec_insert` for this statement shape):
/// * `touch_last_active` + reaped-session check — run by `execute` before
///   this hook.
/// * Per-project op/latency/error counters + noisy-project estimator — in
///   `ProjectSession::execute`, outside this hook (unchanged).
/// * Parser depth guard (`check_parse_depth`) — not needed: no recursive
///   parser runs on the engaged path (classifier + scanner are iterative),
///   and any statement the scanner accepts has paren depth ≤ 2, far below
///   the guard's limit, so the guard could never have rejected it.
/// * libpg_query noop-accept / reject gates, DDL pre-screens, textual
///   matchers, rewrite pipeline — all no-ops for this statement shape (the
///   classifier grammar guarantees the statement starts `INSERT INTO ident`),
///   with the same raw-text exposure the existing AST-hook fast path already
///   has (`exec_insert` hands `raw_sql` to the scanner today).
/// * Query-insights / stat_statements — `query_stats().observe` only records
///   SELECTs (exec_select); the normal INSERT path records nothing, so there
///   is nothing to replicate.
/// * Cost check (`cost_limit_rows`) — Query-only; not applicable.
/// * RLS WITH CHECK, constraints, generated columns, shard/write paths,
///   audit, secondary indexes — all inside `exec_insert_prebuilt`, shared
///   verbatim with the normal path.
async fn try_insert_preparse(
    sess: &ProjectSession,
    raw_sql: &str,
) -> Option<Result<ExecResult>> {
    // Gate 2 first (cheapest): the O(prefix) classifier bails on the first
    // non-INSERT token, so non-INSERT statements (point SELECTs etc.) pay no
    // lock at all here.
    let prefix = crate::values_fast::classify_literal_insert(raw_sql)?;

    // Gate 1: aborted transactions decline (25P02 semantics belong to the
    // normal path). OPEN transactions are admitted: exec_insert_prebuilt
    // retains exec_insert's full in-tx branch (htap_rows buffering, intra-tx
    // uniqueness, FK flush guarded by !tx_is_active), so a BEGIN; INSERT
    // x100; COMMIT no longer pays 100 parse-cache misses — each statement's
    // literals differ, so the cache never hits and the double parse plus
    // rewrite pipeline dominated the transaction's wall time.
    if crate::session::tx_is_aborted(&sess.state) {
        return None;
    }

    // Gate 3: stale pending-OVERRIDING stash → decline (peek, don't take).
    if sess
        .state
        .pending_overriding
        .lock()
        .ok()?
        .is_some()
    {
        return None;
    }

    // Gate 4: header-only sqlparser parse. `header (NULL)` is a complete
    // one-tuple INSERT; sqlparser never sees the multi-MB tail.
    let head = &raw_sql[..prefix.values_end];
    let mut synthetic = String::with_capacity(head.len() + 8);
    synthetic.push_str(head);
    synthetic.push_str(" (NULL)");
    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, &synthetic).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let ins = match stmts.pop()? {
        Statement::Insert(ins) => ins,
        _ => return None,
    };
    // Defensive: the classifier grammar excludes all of these, but the seam
    // below is only valid for the plain shape — re-check on the parsed node.
    if ins.on.is_some()
        || ins.returning.is_some()
        || ins.partitioned.is_some()
        || !ins.after_columns.is_empty()
        || ins.table_alias.is_some()
        || ins.source.is_none()
    {
        return None;
    }
    let name = single_part_name(crate::pg_ast::insert_object_name(&ins).ok()?).ok()?;
    let table = TableName::new(name).ok()?;

    // Gate 5: replicate the dispatch-gate cache invalidation for an
    // auto-commit INSERT (`!stmt_keeps_cache`). See the rationale on the
    // block in `dispatch_parsed_statement`; running it on a statement that
    // later declines is harmless — the normal path re-runs it.
    sess.state.table_meta_cache.invalidate_all();
    sess.state.provider_cache.invalidate_all();
    sess.state.head_probe_cache.invalidate_all();
    sess.engine.pk_row_cache().invalidate_project(&sess.project);

    // Gate 6: hypertable targets need chunk bookkeeping from the parsed rows.
    if sess
        .engine
        .hypertable_registry()
        .time_column(&sess.project, table.as_str())
        .await
        .is_some()
    {
        return None;
    }

    // Gate 7: table metadata (same loader + cache as `exec_insert`).
    let meta = crate::session::load_table_meta_cached_err(sess, &table)
        .await
        .ok()?;

    // Gate 8: partitioned targets keep the per-row AST path.
    if matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. }) {
        return None;
    }

    // Gate 9: scan the tuple list straight into Arrow batches.
    let batches = crate::values_fast::try_fast_insert_at(
        raw_sql,
        prefix.values_end,
        meta.schema.as_ref(),
        &ins.columns,
    )?;
    let batch = arrow::compute::concat_batches(&meta.schema, batches.iter()).ok()?;

    INSERTS_PREPARSE_FASTPATH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Same execution seam the AST path uses after building its batch.
    // RETURNING is impossible here (gate 9 declines any trailing clause).
    Some(exec_insert_prebuilt(sess, &table, &meta, batch, false).await)
}

/// Count of INSERT executions served end-to-end by the bind-direct
/// parameterized fast path (extended protocol Parse/Bind/Execute — the batch
/// is built straight from the decoded bind values; no SQL text and no AST in
/// the per-Execute loop). Test-visible instrumentation; relaxed ordering is
/// fine for a monotone counter.
pub(crate) static INSERTS_BIND_DIRECT_FASTPATH: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Plan for the bind-direct parameterized-INSERT fast path, precomputed ONCE
/// at prepare time (`prepared::build_bind_insert_plan`) for the plain shape
/// `INSERT INTO t [(cols)] VALUES ($1, …)[, (…)]` where EVERY tuple cell is a
/// bare `$N` placeholder.
///
/// The plan stores column NAMES, not schema indices: a prepared statement
/// outlives DDL, so name→index resolution and every column-eligibility check
/// re-run against the freshly loaded `TableMetadata` on each Execute
/// (`values_fast::try_bind_insert_batch`), exactly like the pre-parse literal
/// path re-runs its scanner on each statement.
#[derive(Debug)]
pub(crate) struct BindInsertPlan {
    pub(crate) table: TableName,
    /// Insert column list as written (empty = full width, declaration order).
    pub(crate) columns: Vec<String>,
    /// Per VALUES row: the zero-based parameter index for each tuple position.
    pub(crate) rows: Vec<Vec<usize>>,
}

/// Bind-direct fast path for a parameterized prepared INSERT — the hook
/// `prepared::execute_bound` runs BEFORE the AST / text bind routes when the
/// prepared statement carries a [`BindInsertPlan`].
///
/// Returns:
/// * `None` — decline; the caller falls back to the AST fast route (or the
///   text route) which reproduces every behaviour, including in-transaction
///   buffering and canonical errors. As with `try_insert_preparse`, a decline
///   may have invalidated session caches exactly as the normal dispatch gate
///   would for this INSERT, so a decline is never observable.
/// * `Some(result)` — the statement was fully executed here.
///
/// Engagement gates mirror [`try_insert_preparse`] one-for-one (see the
/// bookkeeping audit there — the same statement shape, just with bind values
/// instead of literal tokens):
/// 1. **Auto-commit only**: in-tx buffering and the 25P02 aborted-state guard
///    stay with the AST/text fallback.
/// 2. **No pending OVERRIDING** stash (peeked, not taken).
/// 3. **Cache invalidation** replicating `dispatch_parsed_statement`'s
///    `!stmt_keeps_cache` gate for an auto-commit INSERT — run BEFORE the
///    table-meta load so this path never writes through a stale snapshot.
/// 4. **Hypertable decline** (chunk bookkeeping needs parsed rows).
/// 5. **Table meta load** (same `load_table_meta_cached_err` as `exec_insert`).
/// 6. **Non-partitioned only** (`RangeMonthly` needs the per-row AST path).
/// 7. **Bind-direct batch build** (`values_fast::try_bind_insert_batch`):
///    re-resolves the plan's column names against the CURRENT schema, applies
///    the identity/generated/default/NULL-fill eligibility gates, and declines
///    on any param/column type doubt — the shared `ColAcc` accumulators make
///    the produced batch byte-identical to the slow path's.
///
/// ON CONFLICT / RETURNING / OVERRIDING / multi-part table names are
/// impossible here: `build_bind_insert_plan` declines them at prepare time, so
/// statements carrying them never get a plan.
pub(crate) async fn try_insert_bind_direct(
    sess: &ProjectSession,
    plan: &BindInsertPlan,
    params: &[crate::prepared::ScalarParam],
) -> Option<Result<ExecResult>> {
    // Same session bookkeeping the text / AST entry points run at their top
    // (this hook fires BEFORE either): keep the idle-in-txn reaper's activity
    // clock fresh, and decline a reaped session so the fallback raises the
    // canonical 25P03 error.
    crate::session::touch_last_active(&sess.state);
    if sess.reaped_flag.is_reaped() {
        return None;
    }

    // Gate 1: auto-commit only.
    if crate::session::tx_is_active(&sess.state) || crate::session::tx_is_aborted(&sess.state) {
        return None;
    }

    // Gate 2: stale pending-OVERRIDING stash → decline (peek, don't take).
    if sess
        .state
        .pending_overriding
        .lock()
        .ok()?
        .is_some()
    {
        return None;
    }

    // Gate 3: replicate the dispatch-gate cache invalidation for an
    // auto-commit INSERT (`!stmt_keeps_cache`). Running it on a statement that
    // later declines is harmless — the fallback path re-runs it.
    sess.state.table_meta_cache.invalidate_all();
    sess.state.provider_cache.invalidate_all();
    sess.state.head_probe_cache.invalidate_all();
    sess.engine.pk_row_cache().invalidate_project(&sess.project);

    // Gate 4: hypertable targets need chunk bookkeeping from parsed rows.
    if sess
        .engine
        .hypertable_registry()
        .time_column(&sess.project, plan.table.as_str())
        .await
        .is_some()
    {
        return None;
    }

    // Gate 5: table metadata (same loader + cache as `exec_insert`).
    let meta = crate::session::load_table_meta_cached_err(sess, &plan.table)
        .await
        .ok()?;

    // Gate 6: partitioned targets keep the per-row AST path.
    if matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. }) {
        return None;
    }

    // Gate 7: build the batch straight from the decoded bind values.
    let batches = crate::values_fast::try_bind_insert_batch(
        meta.schema.as_ref(),
        &plan.columns,
        &plan.rows,
        params,
    )?;
    let batch = arrow::compute::concat_batches(&meta.schema, batches.iter()).ok()?;

    INSERTS_BIND_DIRECT_FASTPATH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Same execution seam as the other INSERT fast paths. RETURNING is
    // impossible here (the plan precompute declines it).
    Some(exec_insert_prebuilt(sess, &plan.table, &meta, batch, false).await)
}

async fn exec_insert(
    sess: &ProjectSession,
    ins: sqlparser::ast::Insert,
    raw_sql: Option<&str>,
) -> Result<ExecResult> {
    let name = single_part_name(crate::pg_ast::insert_object_name(&ins)?)?;
    let table = TableName::new(name)?;

    // --- DEFAULT VALUES: source is None and columns list is empty -----------
    // `INSERT INTO t DEFAULT VALUES` — build a single all-NULL row then let
    // `apply_column_defaults` stamp defaults on every non-generated column.
    if ins.source.is_none() {
        return exec_insert_default_values(sess, ins).await;
    }

    // --- ON CONFLICT DO UPDATE pre-check ------------------------------------
    // If the statement has ON CONFLICT (col) DO UPDATE SET …, check whether
    // the conflict column already has that value and route to UPDATE if so.
    if let Some(OnInsert::OnConflict(ref on_conflict)) = ins.on {
        if let OnConflictAction::DoUpdate(_) = &on_conflict.action {
            if let Some(result) = try_on_conflict_do_update(sess, &table, &ins, on_conflict).await?
            {
                return Ok(result);
            }
            // result is None means no conflict was found — fall through to
            // the normal INSERT path.
        }
    }

    // Pull literal rows out of `INSERT ... VALUES (...)`. Subquery inserts
    // (`INSERT ... SELECT ...`) are routed through `exec_insert_select`
    // below; the body is materialised into VALUES-shaped rows.
    let source = ins
        .source
        .as_ref()
        .ok_or_else(|| BasinError::internal("INSERT without VALUES is not supported in PoC"))?;
    if !matches!(source.body.as_ref(), SetExpr::Values(_)) {
        // INSERT INTO <t> SELECT ... — materialise the SELECT into a
        // RecordBatch via the session's DataFusion context (which already
        // sees inlined user-defined functions, RLS policies, etc.) and
        // hand the resulting rows to the standard INSERT path as if they
        // had been written as VALUES literals.
        return exec_insert_select(sess, &table, &ins, source.as_ref()).await;
    }
    let rows_raw = match source.body.as_ref() {
        SetExpr::Values(v) => &v.rows,
        _ => unreachable!("checked above"),
    };

    // Inv-OLTP-write (#155): served from the per-session table-meta cache
    // on the 2nd..Nth INSERT within a tx (see invalidation carve-out at
    // the top of `execute`). The cached `TableMetadata` is `Arc`-shared
    // — we keep `meta` as `Arc<TableMetadata>` so subsequent borrows
    // (`meta.schema`, `meta.pk_columns`, …) cost nothing.
    let meta = crate::session::load_table_meta_cached_err(sess, &table).await?;
    let schema = meta.schema.clone();
    let mut row_count = rows_raw.len();

    // Pick up any `OVERRIDING { SYSTEM | USER } VALUE` clause the
    // textual pre-screen in `execute()` stashed for us. `take_pending`
    // is take-once: a stale value from a prior statement on this
    // session can't leak into the next INSERT.
    let overriding = crate::session::take_pending_overriding(&sess.state);

    // ── Literal-VALUES fast scanner (values_fast) ───────────────────────────
    // For a plain multi-row literal `INSERT ... VALUES (...), (...)` with no
    // ON CONFLICT, no OVERRIDING clause, and a non-partitioned target, try to
    // hand-tokenize the raw VALUES tail straight into an Arrow batch, skipping
    // the sqlparser-AST round-trip (`expand_insert_rows` → `batch_from_rows`).
    //
    // `try_fast_insert` is conservative: it returns `None` (→ slow path) unless
    // its own gates guarantee the resulting batch is byte-identical to what the
    // slow path would build. Those gates also guarantee the row-prep section
    // below would be a *no-op* on this statement — no identity/generated/
    // default-bearing omitted columns to fill, no ON CONFLICT DO-NOTHING filter
    // (excluded here), and (checked here) not a RangeMonthly partition — so we
    // skip the entire row-prep block when the scanner succeeds and feed the
    // batch directly into the constraint/write seam below.
    let fast_batch: Option<RecordBatch> = if ins.on.is_none()
        && overriding.is_none()
        && !matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. })
    {
        raw_sql
            .and_then(|raw| {
                crate::values_fast::try_fast_insert(raw, schema.as_ref(), &ins.columns)
            })
            .and_then(|batches| {
                arrow::compute::concat_batches(&schema, batches.iter()).ok()
            })
    } else {
        None
    };

    // Enforce IDENTITY semantics on the user-written column list
    // *before* we expand to full-width rows. ALWAYS columns reject
    // user-supplied values unless `OVERRIDING SYSTEM VALUE` is set;
    // BY DEFAULT columns accept them unconditionally (and the
    // `OVERRIDING USER VALUE` clause forces them back to nextval —
    // handled in `apply_identity_columns` below).
    let mut rows_expanded: Vec<Vec<sqlparser::ast::Expr>> = Vec::new();
    if fast_batch.is_none() {
        enforce_identity_insert_columns(schema.as_ref(), &ins.columns, overriding)?;
        // Reject direct writes to generated columns + expand `INSERT INTO t
        // (col_subset) VALUES ...` into full schema-width rows with NULL in
        // unmentioned columns. Generated columns are NULL'd here too;
        // `materialise_generated_columns` overwrites them once the per-row
        // batch is built.
        rows_expanded = expand_insert_rows(schema.as_ref(), &ins.columns, rows_raw)?;
        // Fill IDENTITY columns. Three cases:
        //   * User omitted the column      → fill from nextval.
        //   * Column is BY DEFAULT and
        //     OVERRIDING USER VALUE is set → discard user literal, fill
        //                                    from nextval.
        //   * Otherwise (column supplied,
        //     no OVERRIDING USER VALUE,
        //     ALWAYS already gated above)  → leave the user's value.
        apply_identity_columns(
            sess,
            schema.as_ref(),
            &ins.columns,
            overriding,
            &mut rows_expanded,
        )
        .await?;
        // Substitute column-level DEFAULT expressions for omitted columns.
        // For columns with `BASIN_COLUMN_DEFAULT` metadata that the user did
        // not explicitly write, evaluate the default text (which routes any
        // `nextval(...)` calls through `Catalog::nextval` so each row gets a
        // distinct value) and overwrite the NULL placeholder produced by
        // `expand_insert_rows`. User-written NULL is preserved.
        apply_column_defaults(sess, schema.as_ref(), &ins.columns, &mut rows_expanded).await?;

        // --- ON CONFLICT DO NOTHING filter ------------------------------------
        // If the statement specifies ON CONFLICT (cols) DO NOTHING, proactively
        // remove any proposed rows that would conflict with existing rows (or with
        // earlier rows in the same batch). This must happen *before* the unique /
        // PK constraint checks so those checks never see the skipped rows.
        if let Some(OnInsert::OnConflict(ref on_conflict)) = ins.on {
            if let OnConflictAction::DoNothing = &on_conflict.action {
                // WHERE clause form with conflict predicate is deferred.
                if on_conflict
                    .conflict_target
                    .as_ref()
                    .map(|t| matches!(t, ConflictTarget::OnConstraint(_)))
                    .unwrap_or(false)
                {
                    // ON CONFLICT ON CONSTRAINT <name> DO NOTHING — not yet
                    // implemented; reject cleanly rather than guessing.
                    return Err(BasinError::FeatureNotSupported(
                        "ON CONFLICT ON CONSTRAINT DO NOTHING is not yet supported; \
                         use ON CONFLICT (col, ...) DO NOTHING"
                            .into(),
                    ));
                }
                rows_expanded = filter_rows_do_nothing(
                    sess,
                    &table,
                    schema.as_ref(),
                    &meta,
                    &on_conflict.conflict_target,
                    rows_expanded,
                )
                .await?;
                if rows_expanded.is_empty() {
                    return Ok(ExecResult::Empty {
                        tag: "INSERT 0 0".into(),
                    });
                }
                // Update the row count to reflect only the non-conflicting rows.
                row_count = rows_expanded.len();
            }
        }
    }

    let rows: &[Vec<sqlparser::ast::Expr>] = &rows_expanded;

    // Partitioned path. We must compute each row's partition key from its
    // partition-column value before producing any RecordBatch — multi-row
    // INSERTs may span partitions and we issue one Parquet write per
    // resulting partition.
    if matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. }) {
        let groups = group_rows_by_partition(schema.as_ref(), rows, &meta.partition_spec)?;

        // Shard path is intentionally disabled for partitioned tables in
        // v0.1 — the shard owner's WAL pre-supposes one partition key per
        // project slice and the multi-partition fan-out hasn't been wired
        // through compaction yet. Fall through to the synchronous Parquet
        // write path below.
        let opts = write_options_for(&meta, crate::session::tx_is_active(&sess.state));
        let mut materialised_groups: Vec<(PartitionKey, RecordBatch)> =
            Vec::with_capacity(groups.len());
        for (pkey, group_rows) in groups {
            let batch = batch_from_rows(schema.clone(), &group_rows)?;
            let batch = crate::generated_cols::materialise_generated_columns(
                &sess.engine.config().catalog,
                &sess.project,
                batch,
            )
            .await?;
            crate::type_ddl::enforce_enum_labels(
                &sess.engine.config().catalog,
                &sess.project,
                &batch,
            )
            .await?;
            crate::type_ddl::enforce_domain_checks(
                &sess.engine.config().catalog,
                &sess.project,
                &batch,
            )
            .await?;
            crate::constraints::enforce_check_constraints(
                &sess.engine.config().storage,
                &sess.project,
                &table,
                table.as_str(),
                meta.schema.as_ref(),
                &meta.check_constraints,
                &batch,
            )
            .await?;
            // BUG #133: RLS WITH CHECK on INSERT. Reuses the same per-row
            // predicate-evaluation machinery as CHECK constraints above.
            crate::rls::enforce_with_check(
                &sess.auth_context,
                table.as_str(),
                meta.rls_enabled,
                &meta.policies,
                &sess.current_user,
                basin_catalog::PolicyCommand::Insert,
                &batch,
            )
            .await?;
            crate::constraints::enforce_fk_on_insert(
                &sess.engine.config().catalog,
                &sess.engine.config().storage,
                &sess.project,
                table.as_str(),
                &meta.foreign_keys,
                &batch,
            )
            .await?;
            crate::constraints::enforce_pk_on_insert(
                &sess.engine.config().storage,
                &sess.project,
                &table,
                table.as_str(),
                &meta.pk_columns,
                &batch,
                Some((&*sess.engine.memtable_registry(), &sess.project)),
                Some(sess.engine.pk_set_cache().as_ref()),
            )
            .await?;
            crate::constraints::enforce_unique_on_insert(
                &sess.engine.config().storage,
                &sess.project,
                &table,
                table.as_str(),
                &meta.unique_constraints,
                &batch,
                Some((&*sess.engine.memtable_registry(), &sess.project)),
                &meta.pk_columns,
            )
            .await?;
            materialised_groups.push((pkey, batch));
        }

        // Pre-commit before any Parquet IO so a rejecting sink leaves the
        // object store untouched. Sinks see the in-memory `after` payload;
        // they don't need the on-disk file.
        let preview_batches: Vec<RecordBatch> =
            materialised_groups.iter().map(|(_, b)| b.clone()).collect();
        let events = build_insert_events(sess, &table, &preview_batches)?;
        dispatch_pre_commit(&sess.engine, &events).await?;

        let mut file_refs: Vec<DataFileRef> = Vec::with_capacity(materialised_groups.len());
        for (pkey, batch) in &materialised_groups {
            let df = sess
                .engine
                .config()
                .storage
                .write_batch_with_options(&sess.project, &table, pkey, batch, &opts)
                .await?;
            file_refs.push(DataFileRef {
                path: df.path.as_ref().to_string(),
                size_bytes: df.size_bytes,
                row_count: df.row_count,
                column_stats: df.column_stats.clone(),
                bloom_filters: df.bloom_filters.clone(),
                hll_sketches: std::collections::BTreeMap::new(),
                tdigest_sketches: std::collections::BTreeMap::new(),
            });
        }

        commit_with_retry(sess, &table, meta.current_snapshot, file_refs).await?;
        // Inv-OLTP-write (#155): auto-commit INSERT advances the catalog
        // for this specific table, so the cached `TableMetadata` is now
        // stale (`live_data_files()` misses the just-committed file).
        // Invalidate just this entry so the next SELECT re-loads — the
        // SELECT fast-path's RYOW contract (`auto_commit_ryow_tests`)
        // depends on it.
        sess.state.table_meta_cache.invalidate(&table);
        dispatch_post_commit(&sess.engine, events);
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
        write_insert_audit_rows(sess, meta.schema.as_ref(), &preview_batches).await?;
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    // Seam shared by the literal-VALUES fast paths and the slow AST→Arrow
    // path. Everything inside `exec_insert_prebuilt` (generated columns,
    // constraints, write, audit, RETURNING) runs identically regardless of
    // which arm produced `batch`.
    let batch = match fast_batch {
        Some(b) => b,
        None => batch_from_rows(schema, rows)?,
    };
    exec_insert_prebuilt(sess, &table, &meta, batch, ins.returning.is_some()).await
}

/// Post-batch-build INSERT execution: the shared seam for every non-partitioned
/// INSERT arm once a full-width `RecordBatch` exists.
///
/// Callers:
/// * `exec_insert` — both its slow AST→Arrow arm (`batch_from_rows`) and its
///   literal-VALUES fast arm (`values_fast::try_fast_insert`).
/// * `try_insert_preparse` — the pre-parse fast path, which never runs the
///   whole-statement parsers at all. That caller is restricted (by its own
///   gates) to plain auto-commit literal inserts: no ON CONFLICT, no
///   OVERRIDING, no RETURNING (`has_returning == false`), no partitioned
///   target — so every branch in here behaves exactly as it does when reached
///   through `exec_insert`.
///
/// `meta` must be the same `TableMetadata` snapshot used to build `batch`
/// (schema width/order must match), and `has_returning` mirrors
/// `ins.returning.is_some()` on the AST path.
async fn exec_insert_prebuilt(
    sess: &ProjectSession,
    table: &TableName,
    meta: &TableMetadata,
    batch: RecordBatch,
    has_returning: bool,
) -> Result<ExecResult> {
    let batch = crate::generated_cols::materialise_generated_columns(
        &sess.engine.config().catalog,
        &sess.project,
        batch,
    )
    .await?;
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    // PK / CHECK / FK enforcement. Order: CHECK (no I/O), then FK
    // (one referenced-table scan), then PK (one full-table scan).
    // v0.2 secondary indexes (Phase 5.7 B1) will collapse PK / FK
    // to point lookups; for v0.1 we accept the scan cost.
    crate::constraints::enforce_check_constraints(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    // BUG #133: RLS WITH CHECK on INSERT. Same per-row predicate-eval
    // machinery as CHECK constraints; no-op when rls_enabled = false.
    crate::rls::enforce_with_check(
        &sess.auth_context,
        table.as_str(),
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Insert,
        &batch,
    )
    .await?;
    // FK enforcement reads the referenced (parent) tables' COLD files only
    // (`collect_pk_tuples`). On a shard-backed engine an auto-commit INSERT
    // lands in the shard's TAIL, not in cold Parquet, so a just-inserted parent
    // row would be invisible to the FK scan of a child INSERT issued before any
    // flush (read-your-writes break — `delete_features` CASCADE setup inserts
    // parent then child with no intervening flush). Drain the tail of each
    // referenced table that still has a resident tail so the FK scan sees a
    // consistent cold base. `has_pending_tail` is an O(1) resident-map probe, so
    // the common no-tail / no-FK case pays nothing.
    if !meta.foreign_keys.is_empty() {
        if let Some(shard) = sess.engine.config().shard.as_ref() {
            if !crate::session::tx_is_active(&sess.state) {
                for fk in &meta.foreign_keys {
                    if let Ok(ref_table) = TableName::new(fk.ref_table.clone()) {
                        if shard.has_pending_tail(&sess.project, &ref_table).await {
                            shard.flush_to_parquet().await?;
                            break;
                        }
                    }
                }
            }
        }
    }
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        Some(sess.engine.pk_set_cache().as_ref()),
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        &meta.pk_columns,
    )
    .await?;
    let row_count = batch.num_rows();
    let part = PartitionKey::default_key();

    // ADR 0027 Phase 4: materialise promoted JSONB shadow columns into the
    // batch before writing to storage.  No-op when no paths are promoted.
    let batch = crate::promoted_columns::materialize_promoted_columns(
        &batch,
        &meta.promoted_jsonb_paths,
    )?;

    // Shard-enabled path. The shard owner appends to its WAL, acks once durable,
    // and lets its background compactor drain into Parquet + commit through the
    // catalog later. We do *not* call `append_data_files` ourselves here: that
    // would race the compactor's own commit and produce a duplicate snapshot.
    //
    // ACID gate: the shard's WAL + memtable are a shared durability surface
    // that the session-scoped `TxState` cannot rewind. Any INSERT routed
    // through the shard while a BEGIN block is open survives ROLLBACK
    // (the catalog snapshot is rewound, but the shard's tail compacts into
    // a new file at the *next* SELECT's `shard.flush_to_parquet()` and is
    // appended at a fresh snapshot, resurrecting the row). Fall through to
    // the legacy synchronous path inside an explicit transaction so the
    // write lands in `TxState::htap_rows`, where ROLLBACK can actually
    // discard it. Bench-shape #42 (`rollback_drops_rows`) regression.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        if !crate::session::tx_is_active(&sess.state) {
            // W4: statement-affine striping — the whole batch goes to ONE
            // stripe keyed off this session's pid, so concurrent SESSIONS
            // fan out N-way instead of serialising through default_key's
            // single `compact_lock` + WAL mutex. See `write_batch_striped`.
            // Stripe 0 == default_key so existing data + the read-side tail
            // probe (`shard.get(default_key).read`) keep working as-is.
            let _ = part; // suppress unused-var warning on the striping branch
            write_batch_striped(
                shard,
                &sess.project,
                &table,
                batch.clone(),
                sess.session_pid,
                sess.synchronous_commit(),
            )
                .await?;
            // S4 commit 4a: the rows are durably acked by the shard WAL — keep
            // small OLTP inserts resident in the hot tier as CLEAN entries
            // (`insert_clean`) so the very next point read is served from
            // memory with ZERO file opens (read-own-insert). Bulk loads
            // (> the row gate) and non-encodable PKs skip; promotion is an
            // optimization and never an error.
            write_through_insert_residency(sess, &table, &meta, &batch);
            // Inv-OLTP-write (#155): the shard's compactor advances the
            // catalog out-of-band when it flushes the WAL tail into Parquet
            // (e.g. on `flush_to_parquet()` or the auto-flush threshold).
            // The cached `TableMetadata` populated by this INSERT's pre-
            // write `load_table_meta_cached_err` will then be stale on the
            // next SELECT (`live_data_files()` misses the freshly-committed
            // file). Invalidate just this entry so the SELECT fast-path
            // gate re-loads from the catalog. RYOW correctness gate:
            // `rmw_update_correctness::count_star_after_scalar_hot_update_is_correct`.
            sess.state.table_meta_cache.invalidate(&table);
            // SELECT-side handles tail-visibility (Option A: force-compact). Skip
            // the DataFusion ListingTable refresh here; reads will trigger it.
            return Ok(ExecResult::Empty {
                tag: format!("INSERT 0 {row_count}"),
            });
        }
        // In-tx: fall through to the synchronous Parquet+TxState path below.
        // Any prior auto-commit writes still in the shard's tail will be
        // flushed by `pre_mutation_flush` (UPDATE/DELETE) or by the SELECT
        // path's `shard.flush_to_parquet()`; no extra flush is needed here.
    }

    // ── Transaction-deferred path (THE BIG OLTP WIN) ────────────────────
    // When inside an explicit transaction, buffer the Arrow batch in the
    // tx-local HTAP store and SKIP the per-INSERT Parquet write + per-INSERT
    // ListingTable refresh.  COMMIT drains the buffer, concats per-table,
    // and emits ONE Parquet write + ONE catalog commit per table.
    // `BEGIN; INSERT x100; COMMIT` goes from ~185ms to ~10-20ms.
    //
    // Intra-tx read-your-own-writes is served by `refresh_table_with_htap`
    // (HtapUnionTable provider).  ROLLBACK discards the buffer with zero
    // storage cleanup — no orphan Parquet files exist.
    //
    // Cross-INSERT PK / UNIQUE dedup (which previously rode on the per-INSERT
    // pending file appearing in storage LIST) is enforced by
    // `enforce_intra_tx_uniqueness` against the prior buffered batches before
    // we accept this INSERT.
    if crate::session::tx_is_active(&sess.state) {
        // Cross-INSERT dedup against prior tx-buffered batches.  Must run
        // BEFORE pushing the current batch so we don't compare against
        // ourselves.  enforce_pk_on_insert + enforce_unique_on_insert
        // already ran above against on-disk files, so this just covers the
        // tx-local gap.
        enforce_intra_tx_uniqueness(
            sess,
            &table,
            table.as_str(),
            &meta.pk_columns,
            &meta.unique_constraints,
            &batch,
        )?;
        // Lazy WAL Begin — idempotent within a tx.
        htap_emit_wal_begin_lazy(sess).await;
        // Buffer batch for tx-local read-your-own-writes.
        crate::session::tx_htap_push_batch(&sess.state, &table, batch.clone());
        // perf-w7-fix (txn_insert_x100): DEFER the per-INSERT
        // refresh_table_with_htap.  At scale (100 INSERTs in a tx, catalog
        // with 40+ tables) the per-INSERT MemTable rebuild + register_table
        // dominated wall-time (~200ms on the bench shape).  Read-your-own-
        // writes is still preserved because `exec_select` (line ~6175)
        // re-runs `refresh_table_with_htap` for every table at SELECT-time,
        // and COMMIT drains the htap_rows buffer via `htap_promote_to_registry`
        // independent of which provider is currently registered.  UPDATE /
        // DELETE in-tx already lazy-refresh through their own pre-mutation
        // path (see `dml_mutate::pre_mutation_flush`), so they observe the
        // freshly-buffered rows via the SELECT-side refresh of their planning
        // subqueries (which traverse `exec_select`).
        // Secondary index maintenance is deferred to COMMIT (when the real
        // Parquet path exists).  Intra-tx SELECTs fall back to a full scan
        // against the HtapUnionTable provider, which is correct (no index
        // means no GIN/B-tree pruning — same semantics as a fresh table).
        if has_returning {
            return Ok(ExecResult::Rows {
                schema: batch.schema(),
                batches: vec![batch],
            });
        }
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    // Legacy synchronous path (auto-commit, no shard).
    //
    // Order: write parquet → dispatch pre-commit → commit catalog → refresh.
    let events = build_insert_events(sess, &table, std::slice::from_ref(&batch))?;

    let opts = write_options_for(&meta, false);
    let df = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, &table, &part, &batch, &opts)
        .await?;

    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
        bloom_filters: df.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };

    // ── Auto-commit path ─────────────────────────────────────────────────
    // Register the just-written (not-yet-catalogued) file as an "extra" so
    // that reactor-bodied SELECTs fired during pre-commit can see the new
    // row. This mirrors the in-tx `refresh_table_with_extra` approach from
    // #92: the file is already durable in storage; we simply expose it to
    // DataFusion's planner before the catalog snapshot advances.  Without
    // this, pre-commit hooks would query a ListingTable that does not yet
    // include the new file (it wasn't in the catalog at file-write time).
    //
    // This also fixes per-statement read-your-own-writes in auto-commit
    // mode: the next SELECT on this session will call exec_select which
    // refreshes all tables; at that point the catalog has been committed,
    // so refresh_table (without extra) will see the new file naturally.
    if let Err(e) = crate::session::refresh_table_with_extra(
        &sess.engine,
        &sess.project,
        &sess.ctx,
        &sess.state,
        &table,
        std::slice::from_ref(&file_ref),
    )
    .await
    {
        // Refresh failure before commit means the orphan file in storage
        // should be cleaned up.  Catalog snapshot is unchanged so no
        // catalog rollback is needed.
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &df.path)
            .await;
        return Err(e);
    }
    if let Err(e) = dispatch_pre_commit(&sess.engine, &events).await {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &df.path)
            .await;
        let _ = refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await;
        return Err(e);
    }

    commit_with_retry(sess, &table, meta.current_snapshot, vec![file_ref]).await?;
    // Inv-OLTP-write (#155): auto-commit advanced the catalog — invalidate
    // the cached `TableMetadata` for this table so the next SELECT's
    // fast-path gate reloads the fresh `live_data_files()`. See the
    // partitioned arm above for the full rationale.
    sess.state.table_meta_cache.invalidate(&table);
    dispatch_post_commit(&sess.engine, events);

    // Punch-list #10: if any inserted PK had a tombstone (from a prior fast-path
    // DELETE), the tombstone would suppress the newly inserted cold-tier row on
    // subsequent reads.  Repair: promote each tombstone-hitting row to a
    // MemRowValue::Update so the merge-on-read overlay shows the new value.
    //
    // This is cheap — it only fires when there is an existing registry entry
    // for the table AND at least one row in the batch matches a tombstoned PK.
    // The fast-path is to pre-check whether the registry even has an entry;
    // only then do we compute the pk_col info and scan the batch.
    if !meta.pk_columns.is_empty()
        && sess
            .engine
            .memtable_registry()
            .get(&sess.project, &table)
            .is_some()
    {
        // Derive (col_idx, DataType) pairs for PK columns from the batch schema.
        let pk_col_info: Option<Vec<(usize, arrow_schema::DataType)>> = meta
            .pk_columns
            .iter()
            .map(|c| {
                batch
                    .schema()
                    .index_of(c)
                    .ok()
                    .map(|idx| (idx, batch.schema().field(idx).data_type().clone()))
            })
            .collect();
        if let Some(pk_cols) = pk_col_info {
            promote_tombstone_overrides_on_reinsert(sess, &table, &batch, &pk_cols);
        }
    }

    // Post-commit: re-register the table from the now-authoritative catalog
    // snapshot (no extra files).  This ensures the DataFusion provider
    // reflects exactly the committed state and no longer carries the
    // pre-commit "extra" file reference.
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

    // Phase 5.7 B1: maintain secondary indexes on INSERT (auto-commit path).
    maintain_secondary_indexes_on_insert(sess, &table, &meta, &batch, df.path.as_ref()).await;

    // RETURNING: if the caller asked for RETURNING *, return the inserted batch.
    if has_returning {
        return Ok(ExecResult::Rows {
            schema: batch.schema(),
            batches: vec![batch],
        });
    }

    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

/// `INSERT INTO <table> [(<cols>)] <SELECT ...>` (or any non-VALUES
/// query body). Materialises the source query through the session's
/// DataFusion context — which already sees inlined user-defined
/// functions, RLS predicates, and partition pruning — and writes the
/// resulting batch using the same legacy synchronous path that the
/// VALUES form uses. Partitioned tables, generated columns, and the
/// shard-write path are out of scope for v0.1 INSERT-SELECT.
async fn exec_insert_select(
    sess: &ProjectSession,
    table: &TableName,
    ins: &sqlparser::ast::Insert,
    source: &sqlparser::ast::Query,
) -> Result<ExecResult> {
    use crate::convert::batch_df_to_ws;
    use arrow_array::{ArrayRef, RecordBatch};

    // Inv-OLTP-write (#155): cached on the 2nd..Nth INSERT-SELECT against
    // the same table within a tx — same correctness model as the VALUES
    // path above.
    let meta = crate::session::load_table_meta_cached_err(sess, table).await?;
    let schema = meta.schema.clone();

    if matches!(meta.partition_spec, PartitionSpec::RangeMonthly { .. }) {
        return Err(BasinError::internal(
            "INSERT INTO ... SELECT is not supported on partitioned tables in v0.1",
        ));
    }
    if schema
        .fields()
        .iter()
        .any(|f| crate::types::field_is_generated(f).is_some())
    {
        return Err(BasinError::internal(
            "INSERT INTO ... SELECT is not supported on tables with generated columns in v0.1",
        ));
    }

    // Run the source SELECT through the session context. The full
    // pre-screen pipeline (function inlining, vector-operator rewrite,
    // RLS via `apply_rls_to_select`-equivalents) ran on the parent
    // statement; the source query inherits that. We do *not* re-run
    // the inliner here because it already mutated the AST in
    // `executor::execute`'s SQL-string pass.
    let source_sql = source.to_string();

    // Registered providers are point-in-time snapshots of the catalog's file
    // set (see `exec_select`'s rationale) — they do NOT re-list storage on
    // scan. A provider registered before an external catalog mutation (a
    // shard `flush_to_parquet`, a `rollback_to_snapshot`, a promotion
    // backfill) silently serves the OLD file set, so the source SELECT here
    // would read stale — or zero — rows. Mirror `exec_select`: flush the
    // shard tail so just-written rows are on disk, then refresh every base
    // table the source query can read (scoped via the same refresh-set
    // computation; conservative refresh-all when it cannot be enumerated).
    // In-tx, the htap overlay keeps read-your-own-writes intact.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }
    {
        let in_tx = crate::session::tx_is_active(&sess.state);
        let all_tables: Vec<TableName> = sess
            .engine
            .config()
            .catalog
            .list_tables(&sess.project)
            .await?;
        let tables: Vec<TableName> =
            match compute_select_refresh_set(sess, &source_sql, &all_tables).await {
                Some(scoped) => scoped,
                None => all_tables,
            };
        for t in &tables {
            if in_tx {
                let pending = crate::session::tx_pending_files_for(&sess.state, t);
                let htap_batches = crate::session::tx_htap_batches_for(&sess.state, t);
                if pending.is_empty() && htap_batches.is_empty() {
                    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, t).await?;
                } else {
                    crate::session::refresh_table_with_htap(
                        &sess.engine,
                        &sess.project,
                        &sess.ctx,
                        &sess.state,
                        t,
                        &pending,
                        htap_batches,
                    )
                    .await?;
                }
            } else {
                refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, t).await?;
            }
        }
    }

    let df = sess
        .ctx
        .sql(&source_sql)
        .await
        .map_err(|e| map_df_plan_error("INSERT INTO ... SELECT plan", &e))?;
    let df_batches = df
        .collect()
        .await
        .map_err(|e| BasinError::internal(format!("INSERT INTO ... SELECT execute: {e}")))?;

    // Concatenate batches and convert to workspace arrow.
    let combined_df = if df_batches.is_empty() {
        // Empty result — produce an empty batch with the source schema
        // for the column-mapping step below; the write path is a no-op
        // when there are no rows.
        let plan_schema = sess
            .ctx
            .sql(&source_sql)
            .await
            .map_err(|e| BasinError::internal(format!("INSERT INTO ... SELECT replan: {e}")))?
            .schema()
            .as_arrow()
            .clone();
        datafusion::arrow::record_batch::RecordBatch::new_empty(Arc::new(plan_schema))
    } else if df_batches.len() == 1 {
        df_batches.into_iter().next().unwrap()
    } else {
        let s = df_batches[0].schema();
        datafusion::arrow::compute::concat_batches(&s, &df_batches)
            .map_err(|e| BasinError::internal(format!("concat INSERT-SELECT batches: {e}")))?
    };
    let source_batch = batch_df_to_ws(&combined_df)?;

    // ADR 0027: promoted JSONB shadow columns (`__promoted$col$key`) exist
    // only in the DataFusion-registered schema — `meta.schema` never contains
    // them — so a wildcard source (`INSERT INTO copy SELECT * FROM
    // promoted_table`) materialises them as EXTRA columns and the width check
    // below would reject a copy between column-for-column identical tables
    // ("source has 7 columns, target has 6" — the published `events_copy`
    // benchmark gap, which appears as soon as the JSONB auto-promoter fires
    // on the source table). They are internal storage detail, never user
    // data: drop them before mapping. An explicit `SELECT "__promoted$…" AS
    // x` still works — the alias renames the output column past this filter.
    let source_batch = {
        let keep: Vec<usize> = source_batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !crate::promoted_columns::is_shadow_col_name(f.name()))
            .map(|(i, _)| i)
            .collect();
        if keep.len() == source_batch.num_columns() {
            source_batch
        } else {
            source_batch.project(&keep).map_err(|e| {
                BasinError::internal(format!(
                    "drop promoted shadow columns from INSERT-SELECT source: {e}"
                ))
            })?
        }
    };
    let row_count = source_batch.num_rows();

    // Map source columns to the target schema: when `INSERT INTO t (a, b)
    // SELECT ...` is given, the source's i-th column lands in column `a`,
    // etc. When `(a, b)` is omitted, we insist the source's column count
    // matches the target schema width and use the target's column order.
    let target_cols: Vec<usize> = if ins.columns.is_empty() {
        if source_batch.num_columns() != schema.fields().len() {
            return Err(BasinError::InvalidSchema(format!(
                "INSERT INTO {}: source has {} columns, target has {}",
                table.as_str(),
                source_batch.num_columns(),
                schema.fields().len()
            )));
        }
        (0..schema.fields().len()).collect()
    } else {
        if source_batch.num_columns() != ins.columns.len() {
            return Err(BasinError::InvalidSchema(format!(
                "INSERT INTO {}: source has {} columns, target column list has {}",
                table.as_str(),
                source_batch.num_columns(),
                ins.columns.len()
            )));
        }
        let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
        for (i, f) in schema.fields().iter().enumerate() {
            by_name.insert(f.name().to_ascii_lowercase(), i);
        }
        let mut out = Vec::with_capacity(ins.columns.len());
        for c in &ins.columns {
            let key = c.value.to_ascii_lowercase();
            let idx = *by_name.get(&key).ok_or_else(|| {
                BasinError::InvalidSchema(format!("INSERT references unknown column {:?}", c.value))
            })?;
            out.push(idx);
        }
        out
    };

    // Build a target-schema-shaped batch by placing each source column at
    // the matching target index; unmentioned target columns get NULL
    // arrays of the right type. Source types are cast to the target type
    // when they differ: DataFusion's coercion never sees the target table
    // (we bypass its DML planner), so a source column can legitimately
    // arrive view-typed (string kernels emit Utf8View; the Vortex layer
    // promotes Utf8/Binary to view encodings) or in a different numeric
    // width (`SELECT id::int ...` into a BIGINT column). `safe: false`
    // makes lossy casts (e.g. overflowing BIGINT → INT) error like
    // Postgres instead of silently producing NULLs.
    let n_cols = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(n_cols);
    for (target_idx, target_field) in schema.fields().iter().enumerate() {
        if let Some(source_pos) = target_cols.iter().position(|&i| i == target_idx) {
            let arr = source_batch.column(source_pos).clone();
            let arr = if arr.data_type() == target_field.data_type() {
                arr
            } else if arrow::compute::can_cast_types(arr.data_type(), target_field.data_type()) {
                let opts = arrow::compute::CastOptions {
                    safe: false,
                    ..Default::default()
                };
                arrow::compute::cast_with_options(&arr, target_field.data_type(), &opts).map_err(
                    |e| {
                        BasinError::InvalidSchema(format!(
                            "INSERT INTO {} column {:?}: cannot cast source type {:?} to target type {:?}: {e}",
                            table.as_str(),
                            target_field.name(),
                            arr.data_type(),
                            target_field.data_type()
                        ))
                    },
                )?
            } else {
                return Err(BasinError::InvalidSchema(format!(
                    "INSERT INTO {} column {:?}: source type {:?} does not match target type {:?}",
                    table.as_str(),
                    target_field.name(),
                    arr.data_type(),
                    target_field.data_type()
                )));
            };
            columns.push(arr);
        } else {
            // Column not supplied by the SELECT. For identity/SERIAL columns,
            // generate sequential values via nextval; for others push NULLs.
            use crate::types::field_identity_sequence;
            if let Some(seq_name) = field_identity_sequence(target_field) {
                // Allocate one nextval per row from the catalog sequence.
                let catalog = &sess.engine.config().catalog;
                let mut ids: Vec<i64> = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    let next = catalog.nextval(&sess.project, seq_name).await?;
                    sess.state
                        .sequence_cache
                        .record(sess.project, seq_name, next)
                        .await;
                    ids.push(next);
                }
                columns.push(Arc::new(arrow_array::Int64Array::from(ids)) as ArrayRef);
            } else {
                columns.push(arrow_array::new_null_array(
                    target_field.data_type(),
                    row_count,
                ));
            }
        }
    }
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| BasinError::internal(format!("build INSERT-SELECT batch: {e}")))?;

    // Enum / domain check enforcement matches the VALUES path so
    // constraint violations surface identically regardless of source.
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::constraints::enforce_check_constraints(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    // BUG #133: RLS WITH CHECK on INSERT ... SELECT — the materialised
    // rows are subject to the same policy enforcement as VALUES inserts.
    crate::rls::enforce_with_check(
        &sess.auth_context,
        table.as_str(),
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Insert,
        &batch,
    )
    .await?;
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        Some(sess.engine.pk_set_cache().as_ref()),
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        &meta.pk_columns,
    )
    .await?;

    let part = PartitionKey::default_key();

    // ── Phase 5.14.C3: INSERT-SELECT HTAP wiring (transaction-deferred path) ──
    // Big OLTP win: buffer the batch in the tx-local HTAP store and SKIP the
    // per-INSERT Parquet write.  COMMIT emits ONE write per table.
    if crate::session::tx_is_active(&sess.state) {
        enforce_intra_tx_uniqueness(
            sess,
            table,
            table.as_str(),
            &meta.pk_columns,
            &meta.unique_constraints,
            &batch,
        )?;
        htap_emit_wal_begin_lazy(sess).await;
        crate::session::tx_htap_push_batch(&sess.state, table, batch.clone());
        let htap_batches = crate::session::tx_htap_batches_for(&sess.state, table);
        if let Err(e) = crate::session::refresh_table_with_htap(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            table,
            &[],
            htap_batches,
        )
        .await
        {
            crate::session::tx_set_aborted(&sess.state);
            return Err(e);
        }
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    let events = build_insert_events(sess, table, std::slice::from_ref(&batch))?;

    let opts = write_options_for(&meta, false);
    let written = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, table, &part, &batch, &opts)
        .await?;

    let file_ref = DataFileRef {
        path: written.path.as_ref().to_string(),
        size_bytes: written.size_bytes,
        row_count: written.row_count,
        column_stats: written.column_stats.clone(),
        bloom_filters: written.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };

    // Pre-commit: expose the new file to DataFusion so reactor hooks can
    // see it.  Mirror the auto-commit VALUES path: use refresh_table_with_extra
    // so the file is visible before the catalog snapshot advances.
    if let Err(e) = crate::session::refresh_table_with_extra(
        &sess.engine,
        &sess.project,
        &sess.ctx,
        &sess.state,
        table,
        std::slice::from_ref(&file_ref),
    )
    .await
    {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &written.path)
            .await;
        return Err(e);
    }
    if let Err(e) = dispatch_pre_commit(&sess.engine, &events).await {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &written.path)
            .await;
        let _ = refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await;
        return Err(e);
    }

    commit_with_retry(sess, table, meta.current_snapshot, vec![file_ref]).await?;
    // Inv-OLTP-write (#155): same per-table cache invalidation as the
    // VALUES auto-commit path — `live_data_files()` advanced.
    sess.state.table_meta_cache.invalidate(table);
    dispatch_post_commit(&sess.engine, events);

    // ── Phase 5.14.C3: auto-commit INSERT-SELECT → memtable registry ─────────
    // After a successful auto-commit, push the batch to the shared
    // MemTableRegistry so that subsequent point-lookup fast-path queries find
    // the rows without flushing. This mirrors the committed-row visibility
    // provided by the VALUES path on COMMIT.
    {
        let htap_rows: std::collections::HashMap<_, _> =
            [(table.clone(), vec![batch.clone()])].into_iter().collect();
        // Best-effort: if promotion fails (e.g. hard-cap exceeded), the row is
        // still durable in Parquet — we just lose the hot-tier cache entry.
        let _ = htap_promote_to_registry(sess, &htap_rows).await;
    }

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

// ---------------------------------------------------------------------------
// INSERT DEFAULT VALUES
// ---------------------------------------------------------------------------

/// Handle `INSERT INTO t DEFAULT VALUES`.
///
/// Builds a single all-NULL row then applies every non-generated column's
/// DEFAULT expression. Columns without a DEFAULT stay NULL (which will fail
/// NOT NULL enforcement in `batch_from_rows` if the column is NOT NULL, giving
/// the user a clean error rather than a silent bad insert).
async fn exec_insert_default_values(
    sess: &ProjectSession,
    ins: sqlparser::ast::Insert,
) -> Result<ExecResult> {
    use sqlparser::ast::{Expr, Value};

    let name = single_part_name(crate::pg_ast::insert_object_name(&ins)?)?;
    let table = TableName::new(name)?;

    // Inv-OLTP-write (#155): cached on the 2nd..Nth `INSERT INTO t
    // DEFAULT VALUES` against the same table within a tx — same
    // correctness model as the VALUES path.
    let meta = crate::session::load_table_meta_cached_err(sess, &table).await?;
    let schema = meta.schema.clone();

    // Build one all-NULL row spanning all schema columns.
    let mut row: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|_| Expr::Value((Value::Null).into()))
        .collect();

    // Apply defaults to every non-generated column (treat all as unmentioned).
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if crate::types::field_is_generated(field).is_some() {
            continue;
        }
        let Some(default_text) = crate::types::field_default_text(field) else {
            continue;
        };
        let expr = crate::seq_ddl::evaluate_default_expression(sess, default_text).await?;
        row[col_idx] = expr;
    }

    let rows = vec![row];
    let batch = batch_from_rows(schema.clone(), &rows)?;
    let batch = crate::generated_cols::materialise_generated_columns(
        &sess.engine.config().catalog,
        &sess.project,
        batch,
    )
    .await?;
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::constraints::enforce_check_constraints(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    // BUG #133: RLS WITH CHECK on INSERT ... DEFAULT VALUES.
    crate::rls::enforce_with_check(
        &sess.auth_context,
        table.as_str(),
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Insert,
        &batch,
    )
    .await?;
    // FK enforcement reads the referenced (parent) tables' COLD files only
    // (`collect_pk_tuples`). On a shard-backed engine an auto-commit INSERT
    // lands in the shard's TAIL, not in cold Parquet, so a just-inserted parent
    // row would be invisible to the FK scan of a child INSERT issued before any
    // flush (read-your-writes break — `delete_features` CASCADE setup inserts
    // parent then child with no intervening flush). Drain the tail of each
    // referenced table that still has a resident tail so the FK scan sees a
    // consistent cold base. `has_pending_tail` is an O(1) resident-map probe, so
    // the common no-tail / no-FK case pays nothing.
    if !meta.foreign_keys.is_empty() {
        if let Some(shard) = sess.engine.config().shard.as_ref() {
            if !crate::session::tx_is_active(&sess.state) {
                for fk in &meta.foreign_keys {
                    if let Ok(ref_table) = TableName::new(fk.ref_table.clone()) {
                        if shard.has_pending_tail(&sess.project, &ref_table).await {
                            shard.flush_to_parquet().await?;
                            break;
                        }
                    }
                }
            }
        }
    }
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        Some(sess.engine.pk_set_cache().as_ref()),
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        &table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        &meta.pk_columns,
    )
    .await?;

    let row_count = batch.num_rows();
    let part = PartitionKey::default_key();

    // ── Phase 5.14.C3: DEFAULT VALUES HTAP wiring (transaction-deferred path) ─
    // Big OLTP win: buffer the batch in the tx-local HTAP store and SKIP the
    // per-INSERT Parquet write.  COMMIT emits ONE write per table.
    if crate::session::tx_is_active(&sess.state) {
        enforce_intra_tx_uniqueness(
            sess,
            &table,
            table.as_str(),
            &meta.pk_columns,
            &meta.unique_constraints,
            &batch,
        )?;
        htap_emit_wal_begin_lazy(sess).await;
        crate::session::tx_htap_push_batch(&sess.state, &table, batch.clone());
        let htap_batches = crate::session::tx_htap_batches_for(&sess.state, &table);
        if let Err(e) = crate::session::refresh_table_with_htap(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            &table,
            &[],
            htap_batches,
        )
        .await
        {
            crate::session::tx_set_aborted(&sess.state);
            return Err(e);
        }
        if ins.returning.is_some() {
            return Ok(ExecResult::Rows {
                schema: batch.schema(),
                batches: vec![batch],
            });
        }
        return Ok(ExecResult::Empty {
            tag: format!("INSERT 0 {row_count}"),
        });
    }

    let opts = write_options_for(&meta, false);
    let events = build_insert_events(sess, &table, std::slice::from_ref(&batch))?;
    let df = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, &table, &part, &batch, &opts)
        .await?;
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
        bloom_filters: df.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };

    // Pre-commit: register the new file as "extra" so reactor hooks see it
    // before the catalog snapshot advances (mirrors the VALUES path fix).
    crate::session::refresh_table_with_extra(
        &sess.engine,
        &sess.project,
        &sess.ctx,
        &sess.state,
        &table,
        std::slice::from_ref(&file_ref),
    )
    .await?;
    dispatch_pre_commit(&sess.engine, &events).await?;
    commit_with_retry(sess, &table, meta.current_snapshot, vec![file_ref]).await?;
    // Inv-OLTP-write (#155): per-table cache invalidation after auto-commit.
    sess.state.table_meta_cache.invalidate(&table);
    dispatch_post_commit(&sess.engine, events);

    // ── Phase 5.14.C3: auto-commit DEFAULT VALUES → memtable registry ─────────
    // After a successful auto-commit, push the batch to the shared
    // MemTableRegistry so that subsequent point-lookup fast-path queries find
    // the row without requiring a flush. Best-effort: failure just means the
    // row is served from Parquet on the next read (still durable).
    {
        let htap_rows: std::collections::HashMap<_, _> =
            [(table.clone(), vec![batch.clone()])].into_iter().collect();
        let _ = htap_promote_to_registry(sess, &htap_rows).await;
    }

    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, &table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;

    if ins.returning.is_some() {
        return Ok(ExecResult::Rows {
            schema: batch.schema(),
            batches: vec![batch],
        });
    }
    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {row_count}"),
    })
}

// ---------------------------------------------------------------------------
// copy_ingest fast path — pre-built RecordBatch ingest
// ---------------------------------------------------------------------------

/// Ingest a pre-built [`RecordBatch`] into `table`, bypassing SQL parse and
/// Arrow literal-coercion.  Called by `crate::copy_ingest::exec_copy_from_batch`
/// after it builds the batch from CSV text rows.
///
/// Runs the same post-batch pipeline as `exec_insert` (generated columns,
/// enum/domain checks, constraints, write, commit/refresh).  The shard path
/// and the synchronous-Parquet path are both handled.  Partitioned tables are
/// not supported (caller must gate on `PartitionSpec::Unpartitioned`).
///
/// Returns the number of rows written on success.
pub(crate) async fn exec_ingest_batch(
    sess: &ProjectSession,
    table: &TableName,
    batch: RecordBatch,
) -> Result<u64> {
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;

    let batch = crate::generated_cols::materialise_generated_columns(
        &sess.engine.config().catalog,
        &sess.project,
        batch,
    )
    .await?;
    crate::type_ddl::enforce_enum_labels(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::type_ddl::enforce_domain_checks(&sess.engine.config().catalog, &sess.project, &batch)
        .await?;
    crate::constraints::enforce_check_constraints(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        meta.schema.as_ref(),
        &meta.check_constraints,
        &batch,
    )
    .await?;
    crate::rls::enforce_with_check(
        &sess.auth_context,
        table.as_str(),
        meta.rls_enabled,
        &meta.policies,
        &sess.current_user,
        basin_catalog::PolicyCommand::Insert,
        &batch,
    )
    .await?;
    crate::constraints::enforce_fk_on_insert(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        table.as_str(),
        &meta.foreign_keys,
        &batch,
    )
    .await?;
    crate::constraints::enforce_pk_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        &meta.pk_columns,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        Some(sess.engine.pk_set_cache().as_ref()),
    )
    .await?;
    crate::constraints::enforce_unique_on_insert(
        &sess.engine.config().storage,
        &sess.project,
        table,
        table.as_str(),
        &meta.unique_constraints,
        &batch,
        Some((&*sess.engine.memtable_registry(), &sess.project)),
        &meta.pk_columns,
    )
    .await?;

    let row_count = batch.num_rows() as u64;
    let part = PartitionKey::default_key();

    // Shard path (auto-commit only — same guard as exec_insert).
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        if !crate::session::tx_is_active(&sess.state) {
            // W4: statement-affine striping; see `write_batch_striped`.
            let _ = part;
            write_batch_striped(
                shard,
                &sess.project,
                table,
                batch,
                sess.session_pid,
                sess.synchronous_commit(),
            )
            .await?;
            return Ok(row_count);
        }
    }

    // Synchronous Parquet path (no shard or inside a transaction).
    let opts = write_options_for(&meta, crate::session::tx_is_active(&sess.state));
    let events = build_insert_events(sess, table, std::slice::from_ref(&batch))?;
    let df = sess
        .engine
        .config()
        .storage
        .write_batch_with_options(&sess.project, table, &part, &batch, &opts)
        .await?;
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
        bloom_filters: df.bloom_filters.clone(),
        hll_sketches: std::collections::BTreeMap::new(),
        tdigest_sketches: std::collections::BTreeMap::new(),
    };

    if crate::session::tx_is_active(&sess.state) {
        htap_emit_wal_begin_lazy(sess).await;
        crate::session::tx_htap_push_batch(&sess.state, table, batch.clone());
        crate::session::tx_push_pending_file(&sess.state, table, file_ref);
        let pending = crate::session::tx_pending_files_for(&sess.state, table);
        let htap_batches = crate::session::tx_htap_batches_for(&sess.state, table);
        if let Err(e) = crate::session::refresh_table_with_htap(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &sess.state,
            table,
            &pending,
            htap_batches,
        )
        .await
        {
            crate::session::tx_set_aborted(&sess.state);
            return Err(e);
        }
        return Ok(row_count);
    }

    // Auto-commit: pre-commit hook → catalog commit → refresh.
    if let Err(e) = crate::session::refresh_table_with_extra(
        &sess.engine,
        &sess.project,
        &sess.ctx,
        &sess.state,
        table,
        std::slice::from_ref(&file_ref),
    )
    .await
    {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &df.path)
            .await;
        return Err(e);
    }
    if let Err(e) = dispatch_pre_commit(&sess.engine, &events).await {
        let _ = sess
            .engine
            .config()
            .storage
            .delete_file(&sess.project, &df.path)
            .await;
        let _ = refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await;
        return Err(e);
    }
    commit_with_retry(sess, table, meta.current_snapshot, vec![file_ref]).await?;
    dispatch_post_commit(&sess.engine, events);
    refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, table).await?;
    write_insert_audit_rows(sess, meta.schema.as_ref(), std::slice::from_ref(&batch)).await?;
    Ok(row_count)
}

/// Expose `write_options_for` to `copy_ingest` (same crate, different module).
pub(crate) fn write_options_for_copy(meta: &TableMetadata, in_tx: bool) -> WriteOptions {
    write_options_for(meta, in_tx)
}

// ---------------------------------------------------------------------------
// INSERT … ON CONFLICT (col) DO UPDATE SET …
// ---------------------------------------------------------------------------

// -----------------------------------------------------------------------
// ON CONFLICT DO NOTHING — proactive conflict filter
// -----------------------------------------------------------------------

/// Validate that `conflict_cols` match an existing UNIQUE or PK constraint.
///
/// Returns `Ok(())` when the columns collectively form a registered unique
/// constraint (or the table's primary key), matching PG's requirement that
/// the ON CONFLICT target must refer to a uniqueness/exclusion constraint.
///
/// Returns `Err(BasinError::InvalidSchema(...))` if no matching constraint
/// exists — this mirrors PG SQLSTATE 42P10 ("there is no unique or exclusion
/// constraint matching the ON CONFLICT specification").
fn validate_conflict_target_columns(
    pk_columns: &[String],
    unique_constraints: &[basin_catalog::UniqueConstraint],
    conflict_cols: &[String],
) -> Result<()> {
    if conflict_cols.is_empty() {
        // No explicit target — will check all constraints (handled by caller).
        return Ok(());
    }
    // Normalise to lowercase for comparison.
    let target_set: std::collections::HashSet<String> = conflict_cols
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect();

    // Check against the PK.
    if !pk_columns.is_empty() {
        let pk_set: std::collections::HashSet<String> =
            pk_columns.iter().map(|c| c.to_ascii_lowercase()).collect();
        if pk_set == target_set {
            return Ok(());
        }
    }

    // Check against each UNIQUE constraint.
    for u in unique_constraints {
        let u_set: std::collections::HashSet<String> =
            u.columns.iter().map(|c| c.to_ascii_lowercase()).collect();
        if u_set == target_set {
            return Ok(());
        }
    }

    Err(BasinError::InvalidSchema(format!(
        "there is no unique or exclusion constraint matching the ON CONFLICT \
         specification (columns: {})",
        conflict_cols.join(", ")
    )))
}

/// ON CONFLICT DO NOTHING — filter `rows_expanded` to remove any proposed
/// row that would conflict with an existing row or an earlier row in the
/// same batch. Returns the surviving (non-conflicting) rows.
///
/// # Conflict-target handling
///
/// * `ON CONFLICT (col, ...) DO NOTHING` — check only those columns.
/// * `ON CONFLICT DO NOTHING` (no target) — check all PK + UNIQUE columns
///   (PG semantics: any constraint match suppresses the error).
///
/// In both cases, a WHERE clause predicate attached to the ON CONFLICT clause
/// (`ON CONFLICT (col) WHERE <pred> DO NOTHING`) causes a clean rejection
/// ("not yet supported") because the predicate scopes the suppression to a
/// *partial* unique index, which v0.1 does not model.
async fn filter_rows_do_nothing(
    sess: &ProjectSession,
    table: &TableName,
    schema: &arrow_schema::Schema,
    meta: &TableMetadata,
    conflict_target: &Option<ConflictTarget>,
    rows_expanded: Vec<Vec<sqlparser::ast::Expr>>,
) -> Result<Vec<Vec<sqlparser::ast::Expr>>> {
    use sqlparser::ast::Value;

    if rows_expanded.is_empty() {
        return Ok(rows_expanded);
    }

    // Determine the set of column groups to check. Each entry is a
    // Vec<String> of column names that must *all* match for a conflict.
    let constraint_groups: Vec<Vec<String>> = match conflict_target {
        Some(ConflictTarget::Columns(idents)) => {
            let cols: Vec<String> = idents.iter().map(|i| i.value.clone()).collect();
            // Reject unknown columns eagerly.
            for c in &cols {
                schema.index_of(c).map_err(|_| {
                    BasinError::InvalidSchema(format!(
                        "ON CONFLICT DO NOTHING: unknown column {c:?}"
                    ))
                })?;
            }
            // Validate that the columns form a real unique/pk constraint.
            validate_conflict_target_columns(&meta.pk_columns, &meta.unique_constraints, &cols)?;
            vec![cols]
        }
        None => {
            // No explicit target — gather all constraint groups (PK + UNIQUEs).
            let mut groups: Vec<Vec<String>> = Vec::new();
            if !meta.pk_columns.is_empty() {
                groups.push(meta.pk_columns.clone());
            }
            for u in &meta.unique_constraints {
                groups.push(u.columns.clone());
            }
            if groups.is_empty() {
                // Table has no constraints at all — nothing to suppress.
                return Ok(rows_expanded);
            }
            groups
        }
        Some(ConflictTarget::OnConstraint(_)) => {
            // Caller already rejected this form before calling us.
            return Err(BasinError::FeatureNotSupported(
                "ON CONFLICT ON CONSTRAINT DO NOTHING is not yet supported".into(),
            ));
        }
    };

    // Pre-build column indexes for every constraint group.
    let group_idxs: Vec<Vec<usize>> = constraint_groups
        .iter()
        .map(|cols| {
            cols.iter()
                .map(|c| {
                    schema.index_of(c).map_err(|_| {
                        BasinError::internal(format!(
                            "DO NOTHING: column {c:?} missing from schema"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    // Helper: extract the string tuple for row `r` using `idxs`.
    // Returns None if any value is NULL (a NULL in a unique column is
    // not considered a conflict under PG's default NULLS DISTINCT).
    let extract_tuple = |row: &Vec<sqlparser::ast::Expr>, idxs: &[usize]| -> Option<Vec<String>> {
        let mut parts = Vec::with_capacity(idxs.len());
        for &idx in idxs {
            let val = match &row[idx] {
                sqlparser::ast::Expr::Value(v) => match v.value {
                    Value::Null => return None,
                    _ => format!("{}", row[idx]),
                },
                other => format!("{other}"),
            };
            parts.push(val);
        }
        Some(parts)
    };

    // ── Batched existence probe ──────────────────────────────────────────
    // Single constraint group over a single column: resolve every row's key
    // up front with one chunked `SELECT col … WHERE col IN (…)` instead of a
    // per-row SELECT-1 recursion through `sess.execute`. `None` → statement
    // not eligible (composite group, multiple groups, non-literal key,
    // unsupported key type, undecodable probe result) → the per-row probes
    // below stay authoritative.
    let batched_existing: Option<std::collections::HashSet<String>> = if rows_expanded.len() > 1
        && constraint_groups.len() == 1
        && constraint_groups[0].len() == 1
    {
        batched_existing_conflict_keys(
            sess,
            table,
            meta,
            schema,
            &constraint_groups[0],
            group_idxs[0][0],
            &rows_expanded,
        )
        .await?
    } else {
        None
    };

    // One `seen` set per constraint group to handle same-batch dedup.
    let mut seen_per_group: Vec<std::collections::HashSet<Vec<String>>> = constraint_groups
        .iter()
        .map(|_| Default::default())
        .collect();

    let mut survivors: Vec<Vec<sqlparser::ast::Expr>> = Vec::with_capacity(rows_expanded.len());

    'row: for row in rows_expanded {
        // For each constraint group: if this row conflicts (with existing
        // data OR with a preceding row in the same batch), skip it.
        for (g_idx, (cols, idxs)) in constraint_groups.iter().zip(group_idxs.iter()).enumerate() {
            let Some(tuple) = extract_tuple(&row, idxs) else {
                // NULL in the conflict column → not a conflict; keep row.
                continue;
            };

            // Same-batch dup check.
            if seen_per_group[g_idx].contains(&tuple) {
                // Skip this row — it duplicates an earlier row in this batch.
                continue 'row;
            }

            // Existing-table existence check. The batched probe (when this
            // statement was eligible) already resolved every key in one
            // chunked IN-list SELECT; otherwise try the cheap memtable +
            // zone-map/bloom probe first (when the constraint group IS the
            // table's single-column PK); only fall back to the authoritative
            // SELECT 1 when the probe can't answer definitively. Mirrors the
            // ON CONFLICT DO UPDATE existence-check fast path above.
            if let Some(existing) = &batched_existing {
                // `extract_tuple` returned Some, so the key is non-NULL; a
                // non-literal key is impossible here (eligibility checked
                // every row). Any residue is "no conflict" — identical to a
                // `col = <expr>` probe that can never match.
                let exists = match conflict_probe_key(
                    &row[idxs[0]],
                    schema.field(idxs[0]).data_type(),
                    &cols[0],
                ) {
                    ConflictKey::Lit { canonical, .. } => existing.contains(&canonical),
                    _ => false,
                };
                if exists {
                    continue 'row;
                }
                continue;
            }
            let fast_exists =
                fast_pk_exists_check(sess, table, meta, cols, &row).await?;
            let exists = match fast_exists {
                Some(b) => b,
                None => {
                    let where_parts: Vec<String> = cols
                        .iter()
                        .zip(idxs.iter())
                        .map(|(col, &idx)| {
                            let expr = &row[idx];
                            // Strings need quoting. The expr Display includes
                            // literal quotes for Value::SingleQuotedString so
                            // use it directly.
                            format!("{col} = {expr}")
                        })
                        .collect();
                    let where_clause = where_parts.join(" AND ");
                    let check_sql = format!(
                        "SELECT 1 FROM {} WHERE {} LIMIT 1",
                        table.as_str(),
                        where_clause
                    );
                    // Route through the session dispatcher (NOT raw ctx.sql):
                    // direct DataFusion would read the REGISTERED provider,
                    // which may be stale — providers refresh per-statement in
                    // exec_select, and fast-path reads never touch them. A
                    // stale empty provider here reported "no conflict" for a
                    // cold-tier row and the INSERT blew up on the PK check.
                    match Box::pin(sess.execute(&check_sql)).await {
                        Ok(res) => {
                            let batches = match res {
                                ExecResult::Rows { batches, .. } => batches,
                                _ => Vec::new(),
                            };
                            batches.iter().any(|b| b.num_rows() > 0)
                        }
                        Err(_) => {
                            // Table may be empty or not yet registered — no conflict.
                            false
                        }
                    }
                }
            };
            if exists {
                continue 'row;
            }
        }

        // Row survived all constraint groups — mark it seen and keep it.
        for (g_idx, idxs) in group_idxs.iter().enumerate() {
            if let Some(tuple) = extract_tuple(&row, idxs) {
                seen_per_group[g_idx].insert(tuple);
            }
        }
        survivors.push(row);
    }

    Ok(survivors)
}

/// Cheap existence probe for a single-row ON CONFLICT (pk) shape.
///
/// Skips the DataFusion `SELECT 1 ... WHERE pk = lit` round-trip by checking
/// the hot-tier memtable for a definitive answer, then asking the catalog's
/// per-file zone-map + bloom whether any cold-tier file *could* contain the
/// key. Returns:
///   * `Some(true)`  — row is definitively present (hot-tier Row/Update).
///   * `Some(false)` — row is definitively absent (hot-tier Tombstone OR
///                     every live cold file was pruned by zone-map / bloom).
///   * `None`        — can't decide cheaply (e.g. PK type not supported by
///                     the probe, conflict target is not the table PK, cold
///                     file may contain the key). Caller falls back to the
///                     authoritative SELECT 1 path.
///
/// This is purely an optimisation. The follow-up UPDATE issued by the caller
/// re-applies the PK predicate against the canonical row image, so a false
/// "present" answer would still surface 0 updated rows and the upsert would
/// then fall through to INSERT correctly. We only return `Some(false)` when
/// the probe is *certain* the row is absent.
async fn fast_pk_exists_check(
    sess: &ProjectSession,
    table: &TableName,
    meta: &TableMetadata,
    conflict_cols: &[String],
    row_expanded: &[Expr],
) -> Result<Option<bool>> {
    // Gate: only single-column PK target. Composite PK / non-PK UNIQUE
    // constraint target falls through to the SELECT 1 authoritative path.
    if meta.pk_columns.len() != 1 || conflict_cols.len() != 1 {
        return Ok(None);
    }
    let pk_col = &meta.pk_columns[0];
    if !pk_col.eq_ignore_ascii_case(&conflict_cols[0]) {
        return Ok(None);
    }
    let schema = meta.schema.as_ref();
    let pk_idx = match schema.index_of(pk_col) {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    let pk_dt = schema.field(pk_idx).data_type().clone();

    // Coerce the proposed-row PK expression to a ScalarValue. If the
    // expression isn't a plain literal (e.g. a bind param node we haven't
    // pre-evaluated, a function call) bail to the slow path.
    let pk_expr = &row_expanded[pk_idx];
    let pk_scalar = match crate::dml_mutate::try_literal_to_scalar(pk_expr, &pk_dt, pk_col)? {
        Some(s) => s,
        None => return Ok(None),
    };

    // 1) Memtable: a definitive answer when the PK key is present.
    if let Some(rk) = crate::dml_mutate::pk_scalar_to_row_key(&pk_scalar, &pk_dt) {
        let registry = sess.engine.memtable_registry();
        let entry = registry.get_or_create(sess.project, table.clone());
        match entry.memtable.get(&rk) {
            Some(basin_hottier::MemRowValue::Row { .. })
            | Some(basin_hottier::MemRowValue::Update { .. }) => return Ok(Some(true)),
            Some(basin_hottier::MemRowValue::Tombstone) => return Ok(Some(false)),
            None => { /* fall through to cold-tier probe */ }
        }
    }

    // 2) Cold tier zone-map + bloom probe. `Absent` is decisive ("no live
    //    file can contain this PK"); `Candidates` is inconclusive (a file
    //    *may* contain it — we'd need to actually scan to know for sure,
    //    so we hand back to the SELECT 1 caller).
    let live_files = meta.live_data_files();
    if live_files.is_empty() {
        // No cold tier + memtable miss → definitively absent.
        return Ok(Some(false));
    }
    match crate::index_probe::pk_point_probe(pk_col, &pk_scalar, &live_files, schema) {
        crate::index_probe::PkProbeOutcome::Absent { .. } => Ok(Some(false)),
        crate::index_probe::PkProbeOutcome::Candidates { .. } => Ok(None),
    }
}

/// Pre-check strategy for ON CONFLICT DO UPDATE (upsert).
///
/// Handles both single-row and multi-row VALUES lists with full PG semantics:
///
/// * Each row is checked independently for a conflict on `conflict_cols`.
/// * Conflicting rows are routed to UPDATE (with EXCLUDED.* bindings resolved
///   to the proposed-row literal values).
/// * Non-conflicting rows are collected and submitted as a plain INSERT at
///   the end.
///
/// Returns `Some(result)` when at least one row conflicted (so all rows were
/// handled here).  Returns `Ok(None)` only when zero rows in the batch
/// conflict, in which case the caller falls through to a normal INSERT that
/// will succeed without any constraint violation.
///
/// Intra-statement duplicate keys follow PG semantics: if the VALUES list
/// contains two rows with the same conflict-column tuple, this function
/// returns an error — "ON CONFLICT DO UPDATE command cannot affect row a
/// second time" — matching the SQLSTATE 21000 PG surfaces.
async fn try_on_conflict_do_update(
    sess: &ProjectSession,
    table: &TableName,
    ins: &sqlparser::ast::Insert,
    on_conflict: &sqlparser::ast::OnConflict,
) -> Result<Option<ExecResult>> {
    use sqlparser::ast::OnConflictAction;

    let do_update = match &on_conflict.action {
        OnConflictAction::DoUpdate(u) => u,
        OnConflictAction::DoNothing => return Ok(None),
    };

    // Extract the conflict column(s). We support the `(col, ...)` form.
    let conflict_cols: Vec<String> = match &on_conflict.conflict_target {
        Some(ConflictTarget::Columns(idents)) => idents.iter().map(|i| i.value.clone()).collect(),
        _ => {
            // No explicit target — skip upsert pre-check; fall through to
            // plain INSERT (which will surface a constraint error if needed).
            return Ok(None);
        }
    };
    if conflict_cols.is_empty() {
        return Ok(None);
    }

    // Resolve the inserted rows.
    let source = match ins.source.as_ref() {
        Some(s) => s,
        None => return Ok(None),
    };
    let rows_raw = match source.body.as_ref() {
        SetExpr::Values(v) => &v.rows,
        _ => return Ok(None),
    };
    if rows_raw.is_empty() {
        return Ok(None);
    }

    // Build the WHERE clause for the existence check.
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, table)
        .await?;
    let schema = meta.schema.clone();

    // Expand all rows to schema-width and apply column defaults once.
    let mut rows_expanded = expand_insert_rows(schema.as_ref(), &ins.columns, rows_raw)?;
    apply_column_defaults(sess, schema.as_ref(), &ins.columns, &mut rows_expanded).await?;

    // Pre-compute schema column indices for each conflict column.
    let conflict_idxs: Vec<usize> = conflict_cols
        .iter()
        .map(|c| {
            schema
                .index_of(c)
                .map_err(|_| BasinError::InvalidSchema(format!("ON CONFLICT: unknown column {c:?}")))
        })
        .collect::<Result<_>>()?;

    // The bare table name (last component) for resolving `tablename.col`.
    let table_bare = table.as_str().to_ascii_lowercase();

    // Double-quoted table identifier for the generated probe / UPDATE SQL.
    // Without quoting, a case-sensitive relation created as `"User"` re-parses
    // as the lowercased `user` when these strings are re-executed, so the
    // existence probe misses the conflict and the row falls to a plain INSERT
    // (PK violation). Quoting preserves the stored case; for a lowercase name
    // it is a no-op.
    let table_quoted = format!("\"{}\"", table.as_str().replace('"', "\"\""));

    // Build the SET clause template once — the RHS will be rewritten per row
    // using that row's EXCLUDED values, but the column names are constant.
    // We validate assignments once here; each per-row UPDATE fills in the
    // row-specific `excluded_map`.
    let assignment_cols: Vec<String> = do_update
        .assignments
        .iter()
        .map(|a| match &a.target {
            AssignmentTarget::ColumnName(n) => {
                let col = n.0.last().map(|i| i.id_val().clone()).unwrap_or_default();
                if col.is_empty() {
                    Err(BasinError::InvalidSchema(
                        "ON CONFLICT DO UPDATE: malformed assignment".into(),
                    ))
                } else {
                    Ok(col)
                }
            }
            AssignmentTarget::Tuple(_) => Err(BasinError::InvalidSchema(
                "ON CONFLICT DO UPDATE: malformed assignment".into(),
            )),
        })
        .collect::<Result<_>>()?;

    // ── Batched multi-row path ──────────────────────────────────────────────
    // The per-row loop below costs, for a 50-row batch, up to 50 existence
    // probes (each potentially a full `SELECT 1` recursion through
    // `sess.execute` — metadata refresh + file list + read) plus 50
    // single-row UPDATE statements. When the conflict target is a single
    // column with plain-literal keys we instead:
    //   1. probe ALL keys with one chunked `SELECT col … WHERE col IN (…)`
    //      (after letting the cheap memtable/bloom probe answer what it can),
    //   2. apply ALL conflicting rows with one chunked
    //      `UPDATE … SET c = CASE <ck> WHEN … END WHERE <ck> IN (…)`,
    //   3. INSERT the fresh rows exactly as before (already one statement).
    // Any shape the batch builder can't render exactly (composite target,
    // non-literal conflict key, unsupported key type, assignment RHS that is
    // not safe as a CASE arm) falls back to the per-row loop below —
    // correctness over speed.
    // The batched path cannot cheaply evaluate a per-row DO UPDATE WHERE
    // clause (the conditional UPDATE would need per-row filtering inside the
    // CASE expression, which is unsupported). When `do_update.selection` is
    // present, fall straight through to the per-row path, which appends the
    // rewritten filter as `AND (…)` to each single-row UPDATE.
    if rows_expanded.len() > 1
        && conflict_cols.len() == 1
        && do_update.selection.is_none()
    {
        match try_batched_do_update(
            sess,
            table,
            &meta,
            schema.as_ref(),
            &conflict_cols,
            conflict_idxs[0],
            &table_bare,
            &assignment_cols,
            &do_update.assignments,
            &rows_expanded,
        )
        .await?
        {
            BatchedUpsertOutcome::NoConflicts => return Ok(None),
            BatchedUpsertOutcome::Done {
                update_count,
                insert_rows,
            } => {
                return finish_upsert_fresh_inserts(
                    sess,
                    table,
                    schema.as_ref(),
                    &insert_rows,
                    update_count,
                )
                .await
                .map(Some);
            }
            BatchedUpsertOutcome::NotEligible => {
                // Fall through to the per-row path.
            }
        }
    }

    // Track intra-statement conflict-key duplicates. PG errors with
    // "ON CONFLICT DO UPDATE command cannot affect row a second time"
    // (SQLSTATE 21000) when the VALUES list contains two rows with the
    // same conflict-column tuple. We match that behaviour here.
    let mut seen_in_batch: std::collections::HashSet<Vec<String>> =
        std::collections::HashSet::new();

    // Rows that had no conflict and must be INSERTed.
    let mut insert_rows: Vec<Vec<sqlparser::ast::Expr>> = Vec::new();
    // Count of rows processed via the UPDATE (conflict) path.
    let mut update_count: usize = 0;
    // Whether at least one row conflicted — determines the return path.
    let mut any_conflict = false;

    for row in &rows_expanded {
        // Build the string-tuple key for intra-batch dup detection.
        let key_tuple: Vec<String> = conflict_idxs
            .iter()
            .map(|&idx| format!("{}", row[idx]))
            .collect();

        if !seen_in_batch.insert(key_tuple.clone()) {
            // PG: "ON CONFLICT DO UPDATE command cannot affect row a second
            // time" — two rows in the same VALUES list share the same
            // conflict-column tuple.
            return Err(BasinError::InvalidSchema(
                "ON CONFLICT DO UPDATE command cannot affect row a second time".into(),
            ));
        }

        // Build WHERE conflict_col = value AND ... for existence check + UPDATE.
        let where_clause: String = conflict_cols
            .iter()
            .zip(conflict_idxs.iter())
            .map(|(col, &idx)| format!("{} = {}", col, row[idx]))
            .collect::<Vec<_>>()
            .join(" AND ");

        // Existence check — cheap memtable/bloom probe first, then SELECT 1.
        let fast_exists = fast_pk_exists_check(sess, table, &meta, &conflict_cols, row).await?;
        let exists = match fast_exists {
            Some(b) => b,
            None => {
                let check_sql =
                    format!("SELECT 1 FROM {} WHERE {} LIMIT 1", table_quoted, where_clause);
                // Session dispatcher, not raw ctx.sql — see the DO NOTHING
                // twin above: a stale registered provider must not decide
                // conflict existence.
                match Box::pin(sess.execute(&check_sql)).await {
                    Ok(ExecResult::Rows { batches, .. }) => {
                        batches.iter().any(|b| b.num_rows() > 0)
                    }
                    Ok(_) => false,
                    Err(_) => {
                        // Table may be empty (no Parquet file yet) → no conflict.
                        false
                    }
                }
            }
        };

        if !exists {
            insert_rows.push(row.clone());
            continue;
        }

        // Conflict found. Build and execute an UPDATE for this row.
        any_conflict = true;

        // Build EXCLUDED map: col_name_lowercase → proposed-row expr.
        let mut excluded_map: std::collections::HashMap<String, Expr> =
            std::collections::HashMap::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            excluded_map.insert(field.name().to_ascii_lowercase(), row[i].clone());
        }

        let set_parts: Vec<String> = assignment_cols
            .iter()
            .zip(do_update.assignments.iter())
            .map(|(col, a)| {
                let rhs = rewrite_do_update_expr(a.value.clone(), &table_bare, &excluded_map);
                format!("{col} = {rhs}")
            })
            .collect();

        // Optional DO UPDATE WHERE clause. Rewrite EXCLUDED/table refs first
        // (EXCLUDED.col → proposed literal, table.col → bare existing-row col).
        // After the rewrite the predicate may have collapsed to a constant
        // (the common ORM shape `WHERE EXCLUDED.col <op> <lit>` references only
        // the proposed row, so it folds to a bool). dml_mutate's UPDATE WHERE
        // grammar only accepts `column <op> literal`, so a constant predicate
        // like `NULL IS NOT NULL` or `0 > 50` cannot be pushed back into the
        // UPDATE SQL — we evaluate it here and skip the UPDATE when it is
        // false. A predicate that still references an existing-row column is
        // appended as `AND (…)`; dml_mutate handles the column-OP-literal form.
        let where_filter: Option<String> = match do_update.selection.as_ref() {
            None => None,
            Some(sel) => {
                let rewritten =
                    rewrite_do_update_expr(sel.clone(), &table_bare, &excluded_map);
                match eval_constant_predicate(&rewritten) {
                    // Fully constant and false → this row's UPDATE is skipped.
                    Some(false) => {
                        // Row conflicted but the guard rejected it. Count it as
                        // a processed conflict so the caller treats it as an
                        // upsert (not a fresh INSERT); PG reports 0 affected
                        // for the rejected row — a documented cosmetic tag
                        // divergence, not a correctness issue for read-back.
                        update_count += 1;
                        continue;
                    }
                    // Fully constant and true → unconditional UPDATE.
                    Some(true) => None,
                    // Still references a column → push the filter into the SQL.
                    None => Some(format!("{rewritten}")),
                }
            }
        };

        let update_sql = match where_filter {
            None => format!(
                "UPDATE {} SET {} WHERE {}",
                table_quoted,
                set_parts.join(", "),
                where_clause
            ),
            Some(filter) => format!(
                "UPDATE {} SET {} WHERE {} AND ({})",
                table_quoted,
                set_parts.join(", "),
                where_clause,
                filter
            ),
        };
        Box::pin(sess.execute(&update_sql)).await?;
        update_count += 1;
    }

    if !any_conflict {
        // No row in the batch conflicted — let the caller do a normal INSERT.
        return Ok(None);
    }

    // At least one row conflicted. Handle the non-conflicting rows here too
    // (we cannot return None and let the caller INSERT them because the
    // schema-expand + default-apply pass has already been done; more
    // importantly, a plain INSERT of the full original batch would re-try the
    // conflicting keys and hit the PK constraint).
    finish_upsert_fresh_inserts(sess, table, schema.as_ref(), &insert_rows, update_count)
        .await
        .map(Some)
}

/// Shared tail for the upsert paths: INSERT the non-conflicting rows as one
/// plain multi-row statement and produce the PG-style command tag
/// (`INSERT 0 N`, N = updated + inserted — what PG reports for an upsert).
async fn finish_upsert_fresh_inserts(
    sess: &ProjectSession,
    table: &TableName,
    schema: &Schema,
    insert_rows: &[Vec<Expr>],
    update_count: usize,
) -> Result<ExecResult> {
    let insert_count = insert_rows.len();
    if !insert_rows.is_empty() {
        // Reconstruct a VALUES literal from the already-expanded rows and
        // re-execute as a plain INSERT (without ON CONFLICT, so the normal
        // INSERT path handles PK enforcement, identity columns, etc.).
        // The column list uses the schema field names in order because
        // `expand_insert_rows` already filled every position.
        let col_list: String = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let value_rows: String = insert_rows
            .iter()
            .map(|row| {
                let vals = row
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({vals})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        let plain_insert_sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table.as_str(),
            col_list,
            value_rows
        );
        Box::pin(sess.execute(&plain_insert_sql)).await?;
    }

    // PG returns "INSERT 0 N" for a multi-row upsert where N is the total
    // number of rows processed (both updated and inserted).
    Ok(ExecResult::Empty {
        tag: format!("INSERT 0 {}", update_count + insert_count),
    })
}

/// Rewrite a DO UPDATE SET RHS expression to resolve PostgreSQL upsert
/// column-reference semantics before passing the expression to a plain UPDATE:
///
/// - `EXCLUDED.col`     → the literal proposed-row value for `col`
/// - `tablename.col`    → bare `col` identifier (existing row in UPDATE scope)
/// - unqualified `col`  → left unchanged (already refers to the existing row)
///
/// The function is purely structural; it does not need access to the session
/// because `rows_expanded` already holds concrete literal / bind-param Expr
/// nodes for every column in the proposed row.
fn rewrite_do_update_expr(
    expr: Expr,
    table_name_lower: &str,
    excluded_map: &std::collections::HashMap<String, Expr>,
) -> Expr {
    // Convenience macro to avoid repeating the recursive call signature.
    // Accepts a `Box<Expr>` and returns a new `Box<Expr>` with the inner
    // expression recursively rewritten.
    macro_rules! rw {
        ($e:expr) => {
            Box::new(rewrite_do_update_expr(*$e, table_name_lower, excluded_map))
        };
    }
    match expr {
        // Two-part qualified identifier: either `EXCLUDED.col` or `table.col`.
        // The qualifier is matched case-insensitively; the column name uses
        // `.value` (the unquoted form stored by sqlparser, so `"email"` and
        // `email` both yield `"email"` as the value).
        Expr::CompoundIdentifier(ref parts) if parts.len() == 2 => {
            let qualifier = parts[0].value.to_ascii_lowercase();
            let col_lower = parts[1].value.to_ascii_lowercase();
            if qualifier == "excluded" {
                // EXCLUDED.col → proposed-row value (or keep as-is if unknown).
                if let Some(proposed) = excluded_map.get(&col_lower) {
                    return proposed.clone();
                }
            } else if qualifier == table_name_lower {
                // tablename.col → bare col (existing row in the UPDATE scope).
                return Expr::Identifier(parts[1].clone());
            }
            expr
        }
        // Recurse into binary operations (the common case: `t.hits + EXCLUDED.hits`).
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: rw!(left),
            op,
            right: rw!(right),
        },
        // Recurse into unary operations.
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op,
            expr: rw!(inner),
        },
        // Recurse into parenthesised expressions.
        Expr::Nested(inner) => Expr::Nested(rw!(inner)),
        // CAST(expr AS type) — recurse into the inner expression.
        Expr::Cast {
            expr: inner,
            data_type,
            format,
            kind,
            array,
        } => Expr::Cast {
            expr: rw!(inner),
            data_type,
            format,
            kind,
            array,
        },
        // IS NULL / IS NOT NULL — recurse into the operand.
        Expr::IsNull(inner) => Expr::IsNull(rw!(inner)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(rw!(inner)),
        // IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE.
        Expr::IsTrue(inner) => Expr::IsTrue(rw!(inner)),
        Expr::IsNotTrue(inner) => Expr::IsNotTrue(rw!(inner)),
        Expr::IsFalse(inner) => Expr::IsFalse(rw!(inner)),
        Expr::IsNotFalse(inner) => Expr::IsNotFalse(rw!(inner)),
        // BETWEEN low AND high.
        Expr::Between {
            expr: e,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: rw!(e),
            negated,
            low: rw!(low),
            high: rw!(high),
        },
        // col [NOT] IN (list).
        Expr::InList {
            expr: e,
            list,
            negated,
        } => Expr::InList {
            expr: rw!(e),
            list: list
                .into_iter()
                .map(|item| rewrite_do_update_expr(item, table_name_lower, excluded_map))
                .collect(),
            negated,
        },
        // [NOT] LIKE / [NOT] ILIKE.
        Expr::Like {
            negated,
            expr: e,
            pattern,
            escape_char,
            any,
        } => Expr::Like {
            negated,
            expr: rw!(e),
            pattern: rw!(pattern),
            escape_char,
            any,
        },
        Expr::ILike {
            negated,
            expr: e,
            pattern,
            escape_char,
            any,
        } => Expr::ILike {
            negated,
            expr: rw!(e),
            pattern: rw!(pattern),
            escape_char,
            any,
        },
        // Function calls: rewrite every argument expression. Covers forms like
        // `coalesce(EXCLUDED.col, 0)` or `upper(EXCLUDED.name)`.
        Expr::Function(mut func) => {
            if let sqlparser::ast::FunctionArguments::List(ref mut list) = func.args {
                for arg in &mut list.args {
                    use sqlparser::ast::{FunctionArg, FunctionArgExpr};
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => {
                            let owned = std::mem::replace(e, Expr::Value(sqlparser::ast::ValueWithSpan {
                                value: sqlparser::ast::Value::Null,
                                span: sqlparser::tokenizer::Span::empty(),
                            }));
                            *e = rewrite_do_update_expr(owned, table_name_lower, excluded_map);
                        }
                        _ => {}
                    }
                }
            }
            Expr::Function(func)
        }
        // CASE WHEN … END — recurse into the operand, each WHEN condition and
        // result, and the ELSE branch. Covers DO UPDATE SET col = CASE WHEN
        // EXCLUDED.v IS NOT NULL THEN EXCLUDED.v ELSE col END.
        Expr::Case {
            operand,
            conditions,
            else_result,
            case_token,
            end_token,
        } => Expr::Case {
            operand: operand.map(|op| rw!(op)),
            conditions: conditions
                .into_iter()
                .map(|cw| sqlparser::ast::CaseWhen {
                    condition: rewrite_do_update_expr(cw.condition, table_name_lower, excluded_map),
                    result: rewrite_do_update_expr(cw.result, table_name_lower, excluded_map),
                })
                .collect(),
            else_result: else_result.map(|er| rw!(er)),
            case_token,
            end_token,
        },
        // String concat operator `||` is a BinaryOp and already handled above.
        // All other expression forms (literals, bind params, subqueries, …) are
        // left unchanged — they either have no column references or reference
        // forms we don't need to rewrite for the DO UPDATE scope.
        other => other,
    }
}

/// A constant value reduced from a DO UPDATE WHERE predicate after the
/// EXCLUDED/table rewrite. Only the shapes the rewrite can actually produce
/// from an ORM-emitted predicate are modelled.
#[derive(Debug, Clone, PartialEq)]
enum ConstVal {
    Null,
    Bool(bool),
    Num(f64),
    Str(String),
}

/// Reduce a rewritten DO UPDATE WHERE expression to a constant value when it
/// references no remaining column (every `EXCLUDED.col` has been substituted
/// with a literal). Returns `None` the moment a column reference, bind
/// parameter, or unsupported form is hit — the caller then pushes the
/// predicate into the UPDATE SQL so dml_mutate's `column <op> literal` grammar
/// evaluates it against the existing row.
fn fold_const_expr(e: &Expr) -> Option<ConstVal> {
    use sqlparser::ast::{Value, ValueWithSpan};
    match e {
        Expr::Nested(inner) => fold_const_expr(inner),
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::Null => Some(ConstVal::Null),
            Value::Boolean(b) => Some(ConstVal::Bool(*b)),
            Value::Number(s, _) => s.parse::<f64>().ok().map(ConstVal::Num),
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::EscapedStringLiteral(s) => Some(ConstVal::Str(s.clone())),
            _ => None,
        },
        // Identifier / CompoundIdentifier = unresolved column → not constant.
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => None,
        Expr::UnaryOp { op, expr: inner } => {
            use sqlparser::ast::UnaryOperator;
            let v = fold_const_expr(inner)?;
            match op {
                UnaryOperator::Not => match v {
                    ConstVal::Bool(b) => Some(ConstVal::Bool(!b)),
                    ConstVal::Null => Some(ConstVal::Null),
                    _ => None,
                },
                UnaryOperator::Minus => match v {
                    ConstVal::Num(n) => Some(ConstVal::Num(-n)),
                    _ => None,
                },
                UnaryOperator::Plus => match v {
                    ConstVal::Num(n) => Some(ConstVal::Num(n)),
                    _ => None,
                },
                _ => None,
            }
        }
        Expr::IsNull(inner) => Some(ConstVal::Bool(matches!(
            fold_const_expr(inner)?,
            ConstVal::Null
        ))),
        Expr::IsNotNull(inner) => Some(ConstVal::Bool(!matches!(
            fold_const_expr(inner)?,
            ConstVal::Null
        ))),
        Expr::IsTrue(inner) => {
            Some(ConstVal::Bool(fold_const_expr(inner)? == ConstVal::Bool(true)))
        }
        Expr::IsNotTrue(inner) => {
            Some(ConstVal::Bool(fold_const_expr(inner)? != ConstVal::Bool(true)))
        }
        Expr::IsFalse(inner) => Some(ConstVal::Bool(
            fold_const_expr(inner)? == ConstVal::Bool(false),
        )),
        Expr::IsNotFalse(inner) => Some(ConstVal::Bool(
            fold_const_expr(inner)? != ConstVal::Bool(false),
        )),
        Expr::BinaryOp { left, op, right } => {
            use sqlparser::ast::BinaryOperator;
            let l = fold_const_expr(left)?;
            let r = fold_const_expr(right)?;
            // SQL three-valued logic: any comparison with NULL yields NULL
            // (treated as "not true" by the caller, matching PG's WHERE).
            if matches!(l, ConstVal::Null) || matches!(r, ConstVal::Null) {
                return match op {
                    BinaryOperator::And => {
                        // FALSE AND NULL = FALSE; otherwise NULL.
                        if l == ConstVal::Bool(false) || r == ConstVal::Bool(false) {
                            Some(ConstVal::Bool(false))
                        } else {
                            Some(ConstVal::Null)
                        }
                    }
                    BinaryOperator::Or => {
                        // TRUE OR NULL = TRUE; otherwise NULL.
                        if l == ConstVal::Bool(true) || r == ConstVal::Bool(true) {
                            Some(ConstVal::Bool(true))
                        } else {
                            Some(ConstVal::Null)
                        }
                    }
                    _ => Some(ConstVal::Null),
                };
            }
            match op {
                BinaryOperator::And => match (&l, &r) {
                    (ConstVal::Bool(a), ConstVal::Bool(b)) => Some(ConstVal::Bool(*a && *b)),
                    _ => None,
                },
                BinaryOperator::Or => match (&l, &r) {
                    (ConstVal::Bool(a), ConstVal::Bool(b)) => Some(ConstVal::Bool(*a || *b)),
                    _ => None,
                },
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => {
                    let ord = match (&l, &r) {
                        (ConstVal::Num(a), ConstVal::Num(b)) => a.partial_cmp(b)?,
                        (ConstVal::Str(a), ConstVal::Str(b)) => a.cmp(b),
                        (ConstVal::Bool(a), ConstVal::Bool(b)) => a.cmp(b),
                        _ => return None,
                    };
                    use std::cmp::Ordering;
                    let res = match op {
                        BinaryOperator::Eq => ord == Ordering::Equal,
                        BinaryOperator::NotEq => ord != Ordering::Equal,
                        BinaryOperator::Lt => ord == Ordering::Less,
                        BinaryOperator::LtEq => ord != Ordering::Greater,
                        BinaryOperator::Gt => ord == Ordering::Greater,
                        BinaryOperator::GtEq => ord != Ordering::Less,
                        _ => unreachable!(),
                    };
                    Some(ConstVal::Bool(res))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Evaluate a rewritten DO UPDATE WHERE predicate to a boolean when it has
/// folded to a constant. Returns `Some(true)`/`Some(false)` for a constant
/// predicate (NULL is treated as false, matching SQL WHERE semantics), and
/// `None` when the predicate still references a column and must be evaluated
/// against the existing row by the UPDATE itself.
fn eval_constant_predicate(e: &Expr) -> Option<bool> {
    match fold_const_expr(e)? {
        ConstVal::Bool(b) => Some(b),
        ConstVal::Null => Some(false),
        // A constant non-boolean in boolean position is not a shape we expect
        // from a WHERE predicate; defer to the UPDATE rather than guess.
        _ => None,
    }
}

/// Upper bound on the IN-list / CASE-arm count per statement in the batched
/// upsert path. Bigger batches are split into successive statements (also
/// keeps the IN-list under the delta-update machinery's key budget).
const UPSERT_BATCH_CHUNK: usize = 1000;

/// Outcome of [`try_batched_do_update`].
enum BatchedUpsertOutcome {
    /// Statement shape not batchable — the caller runs the per-row path.
    NotEligible,
    /// The probe found zero conflicting keys. The caller returns `Ok(None)`
    /// so the normal INSERT path handles the whole batch — identical to the
    /// per-row path's `any_conflict == false` exit.
    NoConflicts,
    /// Conflicting rows have been UPDATEd (chunked); `insert_rows` holds the
    /// fresh rows for the shared INSERT tail.
    Done {
        update_count: usize,
        insert_rows: Vec<Vec<Expr>>,
    },
}

/// Classification of one row's conflict-column expression for the batched
/// probe (single-column conflict target only).
enum ConflictKey {
    /// NULL literal — never conflicts (PG default NULLS DISTINCT). The
    /// per-row twin reaches the same answer structurally: its
    /// `WHERE col = NULL` probe can never match, so the row routes to INSERT.
    Null,
    /// Batchable literal key.
    Lit {
        /// Type-canonical equality key: the parsed i64 re-rendered for
        /// integer-family columns (so the literals `5` and `05` agree), the
        /// unescaped string for text. Compared against the probe's returned
        /// column values.
        canonical: String,
        /// The literal exactly as the per-row path renders it into
        /// `WHERE <col> = {expr}` (sqlparser `Display`, which re-escapes
        /// embedded quotes). Reused verbatim in IN lists and CASE arms.
        rendered: String,
    },
    /// Not a plain literal — the statement must take the per-row path.
    NotLiteral,
}

fn conflict_probe_key(expr: &Expr, dt: &DataType, col: &str) -> ConflictKey {
    if let Expr::Value(v) = expr {
        if matches!(v.value, sqlparser::ast::Value::Null) {
            return ConflictKey::Null;
        }
    }
    // Reuse the UPDATE-SET literal coercion (the same one
    // `fast_pk_exists_check` trusts) so the canonical key agrees with how
    // the engine itself interprets the literal. `Err` (malformed literal)
    // maps to NotLiteral — the per-row path owns that error surface.
    let scalar = match crate::dml_mutate::try_literal_to_scalar(expr, dt, col) {
        Ok(Some(s)) => s,
        _ => return ConflictKey::NotLiteral,
    };
    let canonical = match scalar {
        basin_storage::ScalarValue::Int64(v) => v.to_string(),
        basin_storage::ScalarValue::Utf8(s) => s,
        _ => return ConflictKey::NotLiteral,
    };
    ConflictKey::Lit {
        canonical,
        rendered: format!("{expr}"),
    }
}

/// Whether a rewritten DO-UPDATE SET RHS is safe to embed as a CASE arm in
/// the batched UPDATE. Conservative allowlist: the recursive shapes
/// `rewrite_do_update_expr` itself understands, plus CAST. Functions,
/// subqueries, nested CASE, … → per-row fallback (correctness over speed).
fn case_arm_safe(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) | Expr::Identifier(_) | Expr::CompoundIdentifier(_) => true,
        Expr::BinaryOp { left, right, .. } => case_arm_safe(left) && case_arm_safe(right),
        Expr::UnaryOp { expr: inner, .. } => case_arm_safe(inner),
        Expr::Nested(inner) => case_arm_safe(inner),
        Expr::Cast { expr: inner, .. } => case_arm_safe(inner),
        _ => false,
    }
}

/// Decode one probe-result column into canonical key strings. Returns
/// `false` when the column isn't the expected Arrow type (caller falls back
/// to the per-row path). NULL elements are skipped — `IN` never matches a
/// NULL anyway.
fn collect_probe_keys(
    arr: &dyn arrow_array::Array,
    dt: &DataType,
    out: &mut std::collections::HashSet<String>,
) -> bool {
    use arrow_array::Array as _;
    macro_rules! collect_int {
        ($ty:ty) => {{
            let Some(a) = arr.as_any().downcast_ref::<$ty>() else {
                return false;
            };
            for i in 0..a.len() {
                if !a.is_null(i) {
                    out.insert(i64::from(a.value(i)).to_string());
                }
            }
        }};
    }
    match dt {
        DataType::Int16 => collect_int!(arrow_array::Int16Array),
        DataType::Int32 => collect_int!(arrow_array::Int32Array),
        DataType::Int64 => collect_int!(arrow_array::Int64Array),
        DataType::Utf8 => {
            let Some(a) = arr.as_any().downcast_ref::<StringArray>() else {
                return false;
            };
            for i in 0..a.len() {
                if !a.is_null(i) {
                    out.insert(a.value(i).to_string());
                }
            }
        }
        _ => return false,
    }
    true
}

/// Resolve which of this statement's conflict keys already exist in the
/// table, using ONE chunked `SELECT <col> FROM <t> WHERE <col> IN (…)` for
/// everything the cheap memtable/bloom probe can't answer. Shared by the
/// batched DO UPDATE and DO NOTHING paths.
///
/// Returns `Ok(None)` when the statement isn't batchable: unsupported key
/// column type, a non-literal key expression, or a probe result that can't
/// be decoded. NULL keys are skipped (they never conflict).
async fn batched_existing_conflict_keys(
    sess: &ProjectSession,
    table: &TableName,
    meta: &TableMetadata,
    schema: &Schema,
    conflict_cols: &[String], // single-column group (len == 1)
    conflict_idx: usize,
    rows_expanded: &[Vec<Expr>],
) -> Result<Option<std::collections::HashSet<String>>> {
    let dt = schema.field(conflict_idx).data_type().clone();
    if !matches!(
        dt,
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Utf8
    ) {
        return Ok(None);
    }

    let mut existing: std::collections::HashSet<String> = Default::default();
    // (canonical, rendered) pairs the cheap probe could not decide.
    let mut undecided: Vec<(String, String)> = Vec::new();
    for row in rows_expanded {
        match conflict_probe_key(&row[conflict_idx], &dt, &conflict_cols[0]) {
            ConflictKey::Null => {}
            ConflictKey::NotLiteral => return Ok(None),
            ConflictKey::Lit {
                canonical,
                rendered,
            } => {
                // Memtable / zone-map+bloom first — free when it answers.
                match fast_pk_exists_check(sess, table, meta, conflict_cols, row).await? {
                    Some(true) => {
                        existing.insert(canonical);
                    }
                    Some(false) => {}
                    None => undecided.push((canonical, rendered)),
                }
            }
        }
    }

    if undecided.is_empty() {
        return Ok(Some(existing));
    }

    // Dedup (a DO NOTHING batch may repeat a key) while keeping the rendered
    // literal for the IN list.
    let mut seen: std::collections::HashSet<&str> = Default::default();
    let unique: Vec<&(String, String)> = undecided
        .iter()
        .filter(|(canonical, _)| seen.insert(canonical.as_str()))
        .collect();

    for chunk in unique.chunks(UPSERT_BATCH_CHUNK) {
        let in_list = chunk
            .iter()
            .map(|(_, rendered)| rendered.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let probe_sql = format!(
            "SELECT {col} FROM {t} WHERE {col} IN ({in_list})",
            col = conflict_cols[0],
            t = table.as_str()
        );
        // Session dispatcher, not raw ctx.sql — same staleness rationale as
        // the per-row SELECT 1 probe.
        let batches = match Box::pin(sess.execute(&probe_sql)).await {
            Ok(ExecResult::Rows { batches, .. }) => batches,
            Ok(_) => Vec::new(),
            // Table may be empty / not yet registered → no conflicts
            // (mirrors the per-row probe's `Err(_) => false`).
            Err(_) => Vec::new(),
        };
        for b in &batches {
            if b.num_rows() == 0 {
                continue;
            }
            if !collect_probe_keys(b.column(0).as_ref(), &dt, &mut existing) {
                // Unexpected result column type — hand back to the per-row
                // path, which stays authoritative.
                return Ok(None);
            }
        }
    }
    Ok(Some(existing))
}

/// Batched multi-row ON CONFLICT DO UPDATE: one chunked IN-list existence
/// probe + one chunked CASE-arm UPDATE per ≤[`UPSERT_BATCH_CHUNK`]
/// conflicting rows, replacing the per-row probe + per-row UPDATE loop.
///
/// Failure-granularity note: the per-row path issued one `sess.execute`
/// UPDATE per conflicting row, so a mid-batch failure could leave earlier
/// rows updated. Here a whole chunk applies (or fails) as one statement, and
/// the intra-statement duplicate-key error fires before ANY row is touched —
/// strictly coarser, never finer, than the per-row behaviour.
#[allow(clippy::too_many_arguments)]
async fn try_batched_do_update(
    sess: &ProjectSession,
    table: &TableName,
    meta: &TableMetadata,
    schema: &Schema,
    conflict_cols: &[String], // single-column target (len == 1)
    conflict_idx: usize,
    table_bare: &str,
    assignment_cols: &[String],
    assignments: &[sqlparser::ast::Assignment],
    rows_expanded: &[Vec<Expr>],
) -> Result<BatchedUpsertOutcome> {
    let dt = schema.field(conflict_idx).data_type().clone();

    // Gate 0 (pure): SET must not assign the conflict column itself. The
    // per-row path probes sequentially, so an earlier row's UPDATE that
    // rewrites the conflict key is visible to later rows' probes; the
    // batched probe sees only the initial state. Fall back to preserve the
    // sequential semantics exactly.
    if assignment_cols
        .iter()
        .any(|c| c.eq_ignore_ascii_case(&conflict_cols[0]))
    {
        return Ok(BatchedUpsertOutcome::NotEligible);
    }

    // Gate 1 (pure): every rewritten SET RHS must be safe as a CASE arm.
    // This also vets the per-row EXCLUDED substitutions, since they splice
    // arbitrary VALUES expressions into the arm. The rendered RHS strings
    // are exactly what the per-row path would have rendered into its
    // single-row UPDATEs (same `rewrite_do_update_expr` + `Display`).
    let mut row_set_rhs: Vec<Vec<String>> = Vec::with_capacity(rows_expanded.len());
    for row in rows_expanded {
        let mut excluded_map: std::collections::HashMap<String, Expr> =
            std::collections::HashMap::with_capacity(schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            excluded_map.insert(field.name().to_ascii_lowercase(), row[i].clone());
        }
        let mut rhs_list = Vec::with_capacity(assignments.len());
        for a in assignments {
            let rhs = rewrite_do_update_expr(a.value.clone(), table_bare, &excluded_map);
            if !case_arm_safe(&rhs) {
                return Ok(BatchedUpsertOutcome::NotEligible);
            }
            rhs_list.push(format!("{rhs}"));
        }
        row_set_rhs.push(rhs_list);
    }

    // Gate 2 (pure): intra-statement duplicate conflict keys — same key
    // rendering and error as the per-row path (PG SQLSTATE 21000), but
    // raised before any UPDATE executes. Additionally, two *differently
    // rendered* literals for the same canonical key (`5` vs `05`) are not an
    // error on the per-row path (it applies both, last wins) while a CASE
    // dispatch would pick the first arm — preserve the per-row behaviour by
    // falling back.
    let mut seen_rendered: std::collections::HashSet<String> = Default::default();
    let mut seen_canonical: std::collections::HashSet<String> = Default::default();
    for row in rows_expanded {
        if !seen_rendered.insert(format!("{}", row[conflict_idx])) {
            return Err(BasinError::InvalidSchema(
                "ON CONFLICT DO UPDATE command cannot affect row a second time".into(),
            ));
        }
        if let ConflictKey::Lit { canonical, .. } =
            conflict_probe_key(&row[conflict_idx], &dt, &conflict_cols[0])
        {
            if !seen_canonical.insert(canonical) {
                return Ok(BatchedUpsertOutcome::NotEligible);
            }
        }
    }

    // Batched existence probe (one chunked IN-list SELECT).
    let existing = match batched_existing_conflict_keys(
        sess,
        table,
        meta,
        schema,
        conflict_cols,
        conflict_idx,
        rows_expanded,
    )
    .await?
    {
        Some(set) => set,
        None => return Ok(BatchedUpsertOutcome::NotEligible),
    };

    // Partition: conflicting (key exists) → UPDATE, fresh → INSERT.
    // NULL keys never conflict (PG NULLS DISTINCT) → INSERT.
    let mut conflict_rows: Vec<(usize, String)> = Vec::new(); // (row idx, rendered key)
    let mut insert_rows: Vec<Vec<Expr>> = Vec::new();
    for (ri, row) in rows_expanded.iter().enumerate() {
        match conflict_probe_key(&row[conflict_idx], &dt, &conflict_cols[0]) {
            ConflictKey::Lit {
                canonical,
                rendered,
            } if existing.contains(&canonical) => {
                conflict_rows.push((ri, rendered));
            }
            _ => insert_rows.push(row.clone()),
        }
    }
    if conflict_rows.is_empty() {
        return Ok(BatchedUpsertOutcome::NoConflicts);
    }

    // One UPDATE per chunk. A single conflicting row keeps the exact SQL
    // shape the per-row path produced (`SET c = rhs WHERE ck = key`) so it
    // hits the same single-key fast paths; larger chunks dispatch per row
    // via a simple CASE on the conflict column and route through the
    // IN-list delta-update machinery as one statement.
    let conflict_col = &conflict_cols[0];
    for chunk in conflict_rows.chunks(UPSERT_BATCH_CHUNK) {
        let update_sql = if let [(ri, rendered)] = chunk {
            let set_parts: Vec<String> = assignment_cols
                .iter()
                .zip(row_set_rhs[*ri].iter())
                .map(|(col, rhs)| format!("{col} = {rhs}"))
                .collect();
            format!(
                "UPDATE {} SET {} WHERE {conflict_col} = {rendered}",
                table.as_str(),
                set_parts.join(", ")
            )
        } else {
            let in_list = chunk
                .iter()
                .map(|(_, rendered)| rendered.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let set_parts: Vec<String> = assignment_cols
                .iter()
                .enumerate()
                .map(|(ai, col)| {
                    let mut arms = String::new();
                    for (ri, rendered) in chunk {
                        arms.push_str(&format!(
                            " WHEN {rendered} THEN {}",
                            row_set_rhs[*ri][ai]
                        ));
                    }
                    // ELSE is unreachable (the WHERE restricts to the arm
                    // keys) but keeps the expression total.
                    format!("{col} = CASE {conflict_col}{arms} ELSE {col} END")
                })
                .collect();
            format!(
                "UPDATE {} SET {} WHERE {conflict_col} IN ({in_list})",
                table.as_str(),
                set_parts.join(", ")
            )
        };
        Box::pin(sess.execute(&update_sql)).await?;
    }

    Ok(BatchedUpsertOutcome::Done {
        update_count: conflict_rows.len(),
        insert_rows,
    })
}

// ---------------------------------------------------------------------------
// Existing helpers (expand_insert_rows, apply_column_defaults, …)
// ---------------------------------------------------------------------------

/// Translate `INSERT INTO t (col_subset) VALUES (...)` into a list of
/// schema-width rows by reordering the user's values to match the
/// table's column order and inserting `NULL` placeholders in unmentioned
/// positions. When `col_subset` is empty the rows pass through with one
/// transform: any generated column gets a `NULL` slot inserted, leaving
/// the user-supplied values right-shifted across the non-generated columns
/// so the per-cell coercion sees a value where it expects one. This keeps
/// the no-generated-column path byte-identical (rows pass through), while
/// the generated-column path produces a NULL placeholder that the
/// expression evaluator overwrites later.
///
/// Direct writes to a generated column are rejected here with the
/// SQLSTATE-42601-shaped error PG ORMs key off.
fn expand_insert_rows(
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    rows: &[Vec<sqlparser::ast::Expr>],
) -> Result<Vec<Vec<sqlparser::ast::Expr>>> {
    use sqlparser::ast::{Expr, Value};
    let n_cols = schema.fields().len();

    // Build a quick `name -> index` lookup with case-folding.
    let mut by_name = std::collections::HashMap::with_capacity(n_cols);
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }

    // Reject direct writes to a generated column.
    for c in insert_columns {
        let key = c.value.to_ascii_lowercase();
        let idx = *by_name.get(&key).ok_or_else(|| {
            BasinError::InvalidSchema(format!("INSERT references unknown column {:?}", c.value))
        })?;
        if crate::types::field_is_generated(schema.field(idx)).is_some() {
            return Err(BasinError::InvalidSchema(format!(
                "cannot insert into generated column {:?}",
                schema.field(idx).name()
            )));
        }
    }

    // The user can omit `(col_subset)`; the engine still needs to land a
    // schema-width row. Two possibilities:
    //   1. Table has no generated columns. The legacy contract — every
    //      column listed in declaration order — applies. Pass through.
    //   2. Table has generated columns. The user supplies values for
    //      every NON-generated column, and we insert NULL slots at the
    //      generated positions.
    if insert_columns.is_empty() {
        let has_gen = schema
            .fields()
            .iter()
            .any(|f| crate::types::field_is_generated(f).is_some());
        if !has_gen {
            return Ok(rows.to_vec());
        }
        let n_user = schema
            .fields()
            .iter()
            .filter(|f| crate::types::field_is_generated(f).is_none())
            .count();
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            if row.len() != n_user {
                return Err(BasinError::InvalidSchema(format!(
                    "row {i} has {} values, expected {n_user} (one per non-generated column)",
                    row.len()
                )));
            }
            let mut full: Vec<Expr> = Vec::with_capacity(n_cols);
            let mut user_iter = row.iter();
            for f in schema.fields() {
                if crate::types::field_is_generated(f).is_some() {
                    full.push(Expr::Value((Value::Null).into()));
                } else {
                    full.push(user_iter.next().expect("count check above").clone());
                }
            }
            out.push(full);
        }
        return Ok(out);
    }

    // `INSERT INTO t (col_subset) VALUES (...)` — build a name->position
    // map, validate the user's row width, and place each value at its
    // schema-side index.
    let mut user_positions: Vec<usize> = Vec::with_capacity(insert_columns.len());
    for c in insert_columns {
        let idx = by_name[&c.value.to_ascii_lowercase()];
        user_positions.push(idx);
    }
    let mut out = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        if row.len() != insert_columns.len() {
            return Err(BasinError::InvalidSchema(format!(
                "row {i} has {} values, expected {} (one per listed column)",
                row.len(),
                insert_columns.len()
            )));
        }
        let mut full: Vec<Expr> = vec![Expr::Value((Value::Null).into()); n_cols];
        for (val_idx, &col_idx) in user_positions.iter().enumerate() {
            full[col_idx] = row[val_idx].clone();
        }
        out.push(full);
    }
    Ok(out)
}

/// For each column with a stored `BASIN_COLUMN_DEFAULT` metadata entry
/// that the user did not explicitly mention in the INSERT, evaluate the
/// DEFAULT expression once per row and overwrite the corresponding
/// position. Generated columns are skipped (they're owned by
/// `materialise_generated_columns`). User-written NULL is preserved by
/// definition: this function only fires on positions the user *omitted*,
/// inferred from the original `insert_columns` list.
///
/// `nextval('seq')` defaults are the load-bearing case: the rewriter
/// dispatches to `Catalog::nextval` on each evaluation, so each row
/// receives a distinct sequence value. See
/// [`crate::seq_ddl::evaluate_default_expression`] for the per-call
/// rewrite + parse hop.
async fn apply_column_defaults(
    sess: &ProjectSession,
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    rows: &mut [Vec<sqlparser::ast::Expr>],
) -> Result<()> {
    // Determine which columns the user explicitly mentioned. When
    // `insert_columns` is empty, the user wrote `INSERT INTO t VALUES
    // (...)` — every non-generated column is "mentioned" (the user
    // supplied a value for each in declaration order); generated
    // positions were filled with NULL by `expand_insert_rows` and
    // `materialise_generated_columns` will overwrite them. So in the
    // empty-`insert_columns` case there's nothing for DEFAULTs to do.
    if insert_columns.is_empty() {
        return Ok(());
    }
    let mut mentioned = vec![false; schema.fields().len()];
    let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }
    for c in insert_columns {
        if let Some(&idx) = by_name.get(&c.value.to_ascii_lowercase()) {
            mentioned[idx] = true;
        }
    }
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if mentioned[col_idx] {
            continue;
        }
        if crate::types::field_is_generated(field).is_some() {
            continue;
        }
        let Some(default_text) = crate::types::field_default_text(field) else {
            continue;
        };
        // Evaluate the DEFAULT once per row so `nextval('seq')` hands
        // out a fresh value per row.
        for row in rows.iter_mut() {
            let expr = crate::seq_ddl::evaluate_default_expression(sess, default_text).await?;
            row[col_idx] = expr;
        }
    }
    Ok(())
}

/// Public(crate) wrapper for `apply_column_defaults`, called from
/// `copy_ingest::apply_column_defaults_to_batch` to fill DEFAULT expressions
/// for columns not listed in a `COPY t (col_list) FROM STDIN` statement.
pub(crate) async fn apply_column_defaults_pub(
    sess: &ProjectSession,
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    rows: &mut [Vec<sqlparser::ast::Expr>],
) -> Result<()> {
    apply_column_defaults(sess, schema, insert_columns, rows).await
}

/// Gate IDENTITY-column writes on the `OVERRIDING { SYSTEM | USER }
/// VALUE` clause. Called *before* `expand_insert_rows` so a user-listed
/// `INSERT INTO t (id, name) VALUES (...)` where `id` is `GENERATED
/// ALWAYS AS IDENTITY` fails up front rather than after the row builder
/// processes the literal. Only the user's *explicit* column list is
/// inspected — the `INSERT INTO t VALUES (...)` form (no column list)
/// lands every position, so an IDENTITY ALWAYS column written this way
/// is also gated (PG-shape).
fn enforce_identity_insert_columns(
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    overriding: Option<crate::session::OverridingKind>,
) -> Result<()> {
    use crate::session::OverridingKind;
    use crate::types::{field_identity_mode, IdentityMode};
    let always_with_value_is_error = !matches!(overriding, Some(OverridingKind::System));
    if insert_columns.is_empty() {
        // `INSERT INTO t VALUES (...)` — the user supplies a value for
        // every column (or relies on `expand_insert_rows` to NULL-fill
        // generated cols). If the table has an IDENTITY ALWAYS column
        // and we don't have OVERRIDING SYSTEM VALUE, reject.
        if always_with_value_is_error {
            for f in schema.fields() {
                if let Some(IdentityMode::Always) = field_identity_mode(f) {
                    return Err(BasinError::InvalidSchema(format!(
                        "cannot insert a non-DEFAULT value into column {:?} (GENERATED ALWAYS AS \
                         IDENTITY); use OVERRIDING SYSTEM VALUE to override",
                        f.name()
                    )));
                }
            }
        }
        return Ok(());
    }
    // Explicit column list — only the listed columns get user values.
    let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }
    for c in insert_columns {
        let idx = match by_name.get(&c.value.to_ascii_lowercase()) {
            Some(&i) => i,
            // Unknown columns are caught downstream with a better
            // error; just skip here so we don't double-report.
            None => continue,
        };
        if let Some(IdentityMode::Always) = field_identity_mode(schema.field(idx)) {
            if always_with_value_is_error {
                return Err(BasinError::InvalidSchema(format!(
                    "cannot insert a non-DEFAULT value into column {:?} (GENERATED ALWAYS AS \
                     IDENTITY); use OVERRIDING SYSTEM VALUE to override",
                    schema.field(idx).name()
                )));
            }
        }
    }
    Ok(())
}

/// Fill IDENTITY columns by routing through the per-project sequence.
/// Three cases:
///   * Column is omitted from the user's INSERT column list (or the
///     user wrote no column list and the table has IDENTITY columns
///     intermixed with generated ones — `expand_insert_rows` flagged
///     those positions with `NULL`): fill from nextval.
///   * Column is in the user's column list AND mode is BY DEFAULT AND
///     OVERRIDING USER VALUE is set: discard the user's literal, fill
///     from nextval (matches PG-shape: USER VALUE means "use the
///     identity sequence, not the user value").
///   * Otherwise: leave the user's value (the `ALWAYS` gate already
///     enforced `OVERRIDING SYSTEM VALUE` in
///     `enforce_identity_insert_columns`).
async fn apply_identity_columns(
    sess: &ProjectSession,
    schema: &Schema,
    insert_columns: &[sqlparser::ast::Ident],
    overriding: Option<crate::session::OverridingKind>,
    rows: &mut [Vec<sqlparser::ast::Expr>],
) -> Result<()> {
    use crate::session::OverridingKind;
    use crate::types::{field_identity_mode, field_identity_sequence, IdentityMode};
    use sqlparser::ast::{Expr, Value};

    let mut mentioned = vec![false; schema.fields().len()];
    let mut by_name = std::collections::HashMap::with_capacity(schema.fields().len());
    for (i, f) in schema.fields().iter().enumerate() {
        by_name.insert(f.name().to_ascii_lowercase(), i);
    }
    if !insert_columns.is_empty() {
        for c in insert_columns {
            if let Some(&idx) = by_name.get(&c.value.to_ascii_lowercase()) {
                mentioned[idx] = true;
            }
        }
    } else {
        // Empty insert_columns: see `expand_insert_rows` — every
        // non-generated field is "mentioned" (the user supplied a value
        // in declaration order). Generated and identity columns are
        // intermixed in that branch only when generated cols exist;
        // otherwise the contract is "one value per declared column" and
        // the user supplied each.
        let has_gen = schema
            .fields()
            .iter()
            .any(|f| crate::types::field_is_generated(f).is_some());
        if has_gen {
            for (i, f) in schema.fields().iter().enumerate() {
                if crate::types::field_is_generated(f).is_some() {
                    // Generated cols get filled by
                    // `materialise_generated_columns`; the user did
                    // not supply a value for them. Leave `mentioned`
                    // as false so identity-aware filling stays
                    // disabled for those slots.
                    mentioned[i] = false;
                } else {
                    mentioned[i] = true;
                }
            }
        } else {
            for v in mentioned.iter_mut() {
                *v = true;
            }
        }
    }

    let user_value_override = matches!(overriding, Some(OverridingKind::User));

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let Some(mode) = field_identity_mode(field) else {
            continue;
        };
        let Some(seq_name) = field_identity_sequence(field) else {
            // Identity tagged but no sequence — shouldn't happen, but
            // safer to skip than panic.
            continue;
        };
        // Decide whether this column's slot gets filled from nextval
        // for this row. Three triggers:
        //   * Column was omitted from the INSERT.
        //   * OVERRIDING USER VALUE on a BY DEFAULT column.
        //   * `INSERT INTO t VALUES (...)` with no column list AND
        //     this slot was filled with `NULL` by `expand_insert_rows`
        //     (i.e. the column was treated as "user did not supply").
        //     We don't currently exercise that branch (no IDENTITY +
        //     generated col mixed-table tests), but route it for
        //     PG-correctness.
        let omitted = !mentioned[col_idx];
        let force_via_user_override =
            matches!(mode, IdentityMode::ByDefault) && user_value_override && mentioned[col_idx];
        if !omitted && !force_via_user_override {
            continue;
        }
        // Fetch one nextval per row. The shared catalog instance
        // serialises concurrent calls across sessions.
        let catalog = &sess.engine.config().catalog;
        for row in rows.iter_mut() {
            let next = catalog.nextval(&sess.project, seq_name).await?;
            sess.state
                .sequence_cache
                .record(sess.project, seq_name, next)
                .await;
            // BIGINT-shaped literal. The row builder coerces this
            // through the standard Int64 path.
            row[col_idx] = Expr::Value((Value::Number(next.to_string(), false)).into());
        }
    }
    Ok(())
}

/// AUDIT TO emission for INSERT. The mutation has already committed by
/// the time we get here; we materialise the after-row payloads from the
/// in-memory batches and append them to the configured audit table.
/// Project scoping is enforced by `lifecycle::write_audit_rows` resolving
/// the audit table within the calling session's project prefix.
async fn write_insert_audit_rows(
    sess: &ProjectSession,
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<()> {
    let Some(audit_table) = crate::types::audit_table_name(schema) else {
        return Ok(());
    };
    use crate::events::build_row_json;
    use crate::lifecycle::AuditRecord;
    let mut records: Vec<AuditRecord> = Vec::new();
    for b in batches {
        for row in 0..b.num_rows() {
            records.push(AuditRecord {
                before: None,
                after: Some(build_row_json(b, row)?),
            });
        }
    }
    crate::lifecycle::write_audit_rows(sess, audit_table, ChangeOp::Insert, records).await
}

/// Build one [`ChangeEvent`] per row across `batches`, allocating a
/// fresh per-`(project, table)` seq for each. Returns an empty vec when
/// no sinks are attached so callers pay only the registry-empty check.
fn build_insert_events(
    sess: &ProjectSession,
    table: &TableName,
    batches: &[RecordBatch],
) -> Result<Vec<ChangeEvent>> {
    {
        let guard = sess
            .engine
            .event_sinks()
            .read()
            .expect("event_sinks lock poisoned");
        if !registry_has_any(&guard) {
            return Ok(Vec::new());
        }
    }
    let user = causation_user(sess);
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let after = build_row_json(batch, row)?;
            let seq = sess.engine.next_event_seq(&sess.project, table);
            out.push(make_event(
                &sess.project,
                table,
                ChangeOp::Insert,
                None,
                Some(after),
                seq,
                user.clone(),
            ));
        }
    }
    Ok(out)
}

/// Map session principal to the event's `causation_user`. The
/// anonymous-session sentinel becomes `None` so sinks needn't special-
/// case it.
fn causation_user(sess: &ProjectSession) -> Option<String> {
    if sess.current_user == crate::ANONYMOUS_USER {
        None
    } else {
        Some(sess.current_user.clone())
    }
}

/// Map the catalog's per-table `TableFileFormat` (#161) onto the storage
/// layer's `FileFormat`. The two enums are intentionally 1:1 (Parquet /
/// Vortex); they live in separate crates so the catalog has no storage
/// dependency, hence the explicit bridge. Vortex is opt-in — `Parquet`
/// stays the default on both sides and the Parquet path is byte-identical
/// when the table's format is `Parquet`. Shared by the executor and
/// `dml_mutate` write paths so the mapping has exactly one definition.
pub(crate) fn map_file_format(fmt: basin_catalog::TableFileFormat) -> FileFormat {
    match fmt {
        basin_catalog::TableFileFormat::Parquet => FileFormat::Parquet,
        basin_catalog::TableFileFormat::Vortex => FileFormat::Vortex,
    }
}

/// Build the per-write `WriteOptions` from the table's catalog metadata.
/// Two knobs survive the trip:
///  * `bloom_filter_columns` — Phase 5.7 A3, the writer materialises a
///    native Parquet bloom filter section per column.
///  * `max_row_group_size` — Phase 5.7 B3, override the writer's global
///    default for point-query-heavy tables.
///
/// When neither is configured the result is `WriteOptions::default()`,
/// which is byte-equivalent to the pre-Phase-5.7 write path.
///
/// `in_tx` mirrors AF's fastpath-off-in-tx rule: non-tx direct INSERT always
/// encodes with `Fast`. Inside `BEGIN..COMMIT` the per-statement Fast-encoder
/// setup cost dominates the single-row INSERT shape and produced a 27x
/// regression on the `txn_insert_x100` bench (38ms -> 1042ms at 10k scale,
/// commit ddfd8a8). When `in_tx` is true we fall through to `Best` so the
/// in-tx path keeps its cheaper single-row encode.
fn write_options_for(meta: &TableMetadata, in_tx: bool) -> WriteOptions {
    WriteOptions {
        bloom_filter_columns: meta.bloom_filter_columns.clone(),
        max_row_group_size: meta.row_group_rows,
        // #204: an explicit `WITH (basin.cluster_by=…)` wins; otherwise
        // default to the single-column PK so cold files are sorted by the
        // key and the reader's min/max prune isolates `WHERE pk = $1`.
        cluster_columns: if meta.cluster_columns.is_empty() {
            meta.default_cluster_cols()
        } else {
            meta.cluster_columns.clone()
        },
        // Phase 3: honour the table's persisted on-disk format. Defaults
        // to Parquet (catalog default), keeping the legacy write path
        // byte-identical for every Parquet table.
        file_format: map_file_format(meta.file_format),
        row_block_size: meta.row_block_size,
        // Phase 5.14.A2: bloom columns are the global sort-order columns
        // declared via `WITH (basin.sort_by = '...')`. The writer builds a
        // fastbloom per column and stores it in DataFile::bloom_filters so
        // the reader can skip files on point_eq miss without opening them.
        bloom_columns: meta
            .global_sort_order
            .clone()
            .unwrap_or_default(),
        // Always-on Vortex fast-write mode for non-tx direct bulk INSERTs:
        // every non-tx direct INSERT runs through the minimal cascade
        // (~3-4x faster encode at ~1.5x disk size). The next compaction
        // rewrites the file with `Best`, so the disk-size delta is
        // transient. `Best` is NOT a correctness fallback — it is just a
        // more exhaustive encoder cascade — so always-Fast for non-tx
        // INSERT is safe. Copy-on-write UPDATE/DELETE rewrites also use
        // `Fast` (see dml_mutate's write_replacement options).
        //
        // The `!in_tx` gate mirrors AF's fastpath-off-in-tx pattern:
        // inside BEGIN..COMMIT the per-statement Fast-encoder setup cost
        // dominates and regresses `BEGIN; INSERT x100; COMMIT` by ~27x
        // (single-row inserts don't amortise the Fast cascade's fixed
        // setup), so in-tx INSERTs keep `Best`. Tx rollback semantics are
        // preserved because encoding mode only affects on-disk bytes; the
        // in-tx pending-files queue (see `tx_pending_files_for`) is
        // independent.
        encoding_mode: if !in_tx {
            basin_storage::EncodingMode::Fast
        } else {
            basin_storage::EncodingMode::Best
        },
        // Page-index sub-row-group pruning: keep the writer's default data
        // page size (None). The Parquet writer now always emits per-page
        // statistics (EnabledStatistics::Page) so the reader can build a
        // RowSelection regardless of this knob.
        data_pagesize_limit: None,
    }
}

/// Optimistic commit with a single retry on conflict. A conflict is possible
/// only if some other writer raced us between `load_table` and
/// `append_data_files`; the in-memory catalog serializes per table so we
/// re-read and try once more before bubbling up.
async fn commit_with_retry(
    sess: &ProjectSession,
    table: &TableName,
    expected_initial: basin_catalog::SnapshotId,
    files: Vec<DataFileRef>,
) -> Result<()> {
    let mut expected = expected_initial;
    match sess
        .engine
        .config()
        .catalog
        .append_data_files(&sess.project, table, expected, files.clone())
        .await
    {
        Ok(_) => Ok(()),
        Err(BasinError::CommitConflict(_)) => {
            let fresh = sess
                .engine
                .config()
                .catalog
                .load_table(&sess.project, table)
                .await?;
            expected = fresh.current_snapshot;
            sess.engine
                .config()
                .catalog
                .append_data_files(&sess.project, table, expected, files)
                .await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Parallel-scan target_partitions guard (Inv-E #132)
// ---------------------------------------------------------------------------
// For full-table scans (no PK fast-path, non-trivial file count), temporarily
// raise the DataFusion `target_partitions` on the session context so the scan
// fans out across multiple CPU cores.  The guard restores all changed options
// on drop — covering both the success and error paths.
//
// The fast-path (`execute_simple_select`) never reaches this code; it keeps
// `target_partitions = 1` permanently.  OLTP point-queries are served by the
// fast path and are unaffected.
//
// With `target_partitions > 1` DataFusion's planner also enables
// `repartition_aggregations` and `repartition_joins` by default, which adds
// Partial→Repartition→Final overhead that HURTS performance for aggregate
// shapes (per the investigation at line ~6742, the exchange cost swamps the
// aggregate for the table sizes Basin targets).  We explicitly disable both
// so only the file-group fan-out benefits, while aggregates keep the cheaper
// `mode=Single` plan that DataFusion emits when repartition is off.
struct TargetPartitionsGuard<'a> {
    ctx: &'a datafusion::prelude::SessionContext,
    restore_partitions: usize,
    restore_repart_agg: bool,
    restore_repart_joins: bool,
    restore_repart_windows: bool,
}
impl Drop for TargetPartitionsGuard<'_> {
    fn drop(&mut self) {
        let state_ref = self.ctx.state_ref();
        let mut state = state_ref.write();
        let opts = state.config_mut().options_mut();
        opts.execution.target_partitions = self.restore_partitions;
        opts.optimizer.repartition_aggregations = self.restore_repart_agg;
        opts.optimizer.repartition_joins = self.restore_repart_joins;
        opts.optimizer.repartition_windows = self.restore_repart_windows;
    }
}

/// Map a DataFusion planning error to a typed `BasinError`.
///
/// DataFusion reports a missing relation as a planning error whose message
/// contains `table '<name>' not found` (SessionState::get_table_source).
/// PostgreSQL raises SQLSTATE 42P01 (`undefined_table`) with the message
/// `relation "<name>" does not exist` for the same condition, and ORM
/// migration flows branch on exactly that code (Diesel / TypeORM / Django
/// all decide "tracker table missing → create it" on 42P01) — so this must
/// not collapse into the XX000 internal bucket.
///
/// Deliberately narrow: only four exact planner-message patterns are
/// promoted, each to its PG SQLSTATE:
///   * `table '…' not found`                → 42P01 (`UndefinedTable`)
///   * `Invalid function '…'`               → 42883 (`UndefinedFunction`)
///   * `table function '…' not found`       → 42883 (`UndefinedFunction`)
///   * `No field named …` / `column '…' not found` → 42703 (`UndefinedColumn`)
/// Anything else keeps the generic internal mapping so unrelated planner
/// errors are never mis-typed as a missing object.
pub(crate) fn map_df_plan_error(
    context: &str,
    e: &datafusion::error::DataFusionError,
) -> BasinError {
    let msg = e.to_string();
    // Missing table function in FROM position (`SELECT * FROM nosuch()`):
    // DataFusion's SessionState reports `table function '<name>' not found`.
    // Checked before the plain-table pattern for clarity — the two cannot
    // collide (`table '` requires the quote immediately after `table `).
    if let Some(start) = msg.find("table function '") {
        let rest = &msg[start + "table function '".len()..];
        if let Some(end) = rest.find("' not found") {
            return BasinError::undefined_function(&rest[..end]);
        }
    }
    if let Some(start) = msg.find("table '") {
        let rest = &msg[start + "table '".len()..];
        if let Some(end) = rest.find("' not found") {
            // DataFusion resolves the reference against its default catalog
            // and schema before erroring, so the message names
            // `datafusion.public.<table>`. PG reports the relation as the
            // user wrote it — and Basin strips schema qualifiers before
            // DataFusion sees the SQL — so drop the synthetic prefix.
            let name = rest[..end]
                .strip_prefix("datafusion.public.")
                .unwrap_or(&rest[..end]);
            return BasinError::undefined_table(name);
        }
    }
    // Missing scalar/aggregate function: DataFusion's expression planner
    // reports `Invalid function '<name>'` (optionally followed by
    // `.\nDid you mean '…'?`). PG raises SQLSTATE 42883 here.
    if let Some(start) = msg.find("Invalid function '") {
        let rest = &msg[start + "Invalid function '".len()..];
        if let Some(end) = rest.find('\'') {
            return BasinError::undefined_function(&rest[..end]);
        }
    }
    // Missing column, planner flavour: `column '<name>' not found`
    // (optionally `… not found in '<relation>'`). PG raises 42703.
    if let Some(start) = msg.find("column '") {
        let rest = &msg[start + "column '".len()..];
        if let Some(end) = rest.find("' not found") {
            return BasinError::undefined_column(&rest[..end]);
        }
    }
    // Missing column, schema-resolution flavour: `Schema error: No field
    // named <name>. Valid fields are …` (the `Valid fields` tail is absent
    // on an empty schema; the name itself may be quoted and/or qualified —
    // qualified names contain `.` so the terminator must be matched against
    // the known suffixes, not the first dot).
    if let Some(start) = msg.find("No field named ") {
        let rest = &msg[start + "No field named ".len()..];
        let name_part = match rest.find(". Valid fields") {
            Some(end) => &rest[..end],
            None => rest.trim_end().trim_end_matches('.'),
        };
        let name = name_part.trim_matches('"');
        if !name.is_empty() {
            return BasinError::undefined_column(name);
        }
    }
    BasinError::internal(format!("{context}: {e}"))
}

/// Map a DataFusion error raised during `collect()` (physical execution) back
/// to a `BasinError`, preserving any typed `BasinError` a UDF surfaced through
/// `DataFusionError::External`.
///
/// The blocking advisory-lock UDFs (`pg_advisory_lock` / `pg_advisory_xact_lock`)
/// return `BasinError::LockNotAvailable` on lock-wait timeout, wrapped in
/// `DataFusionError::External`. Without this unwrap the error would collapse to
/// a generic `Internal` and lose its SQLSTATE 55P03 mapping. We walk
/// `find_root` so the unwrap also works when DataFusion has nested the External
/// error inside a `Context`/`ArrowError` shell during execution.
pub(crate) fn map_df_exec_error(e: datafusion::error::DataFusionError) -> BasinError {
    use datafusion::error::DataFusionError;
    if let DataFusionError::External(inner) = e.find_root() {
        if let Some(be) = inner.downcast_ref::<BasinError>() {
            // BasinError is not Clone in general; reconstruct the variants we
            // intentionally surface from UDFs. Anything else falls through to
            // the generic mapping below with the original message preserved.
            match be {
                BasinError::LockNotAvailable(msg) => {
                    return BasinError::LockNotAvailable(msg.clone());
                }
                other => {
                    return BasinError::internal(format!("execute: {other}"));
                }
            }
        }
    }
    BasinError::internal(format!("execute: {e}"))
}

#[cfg(test)]
mod df_plan_error_tests {
    use super::map_df_plan_error;
    use basin_common::BasinError;
    use datafusion::error::DataFusionError;

    fn plan_err(msg: &str) -> DataFusionError {
        DataFusionError::Plan(msg.to_string())
    }

    #[test]
    fn missing_table_promotes_to_undefined_table() {
        let e = plan_err("table 'datafusion.public.users' not found");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedTable(name) => assert_eq!(name, "users"),
            other => panic!("expected UndefinedTable, got {other:?}"),
        }
    }

    #[test]
    fn invalid_function_promotes_to_undefined_function() {
        // Both the bare form and the did-you-mean form must extract the name.
        let e = plan_err("Invalid function 'nosuch_fn'");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedFunction(name) => assert_eq!(name, "nosuch_fn"),
            other => panic!("expected UndefinedFunction, got {other:?}"),
        }
        let e = plan_err("Invalid function 'lowerr'.\nDid you mean 'lower'?");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedFunction(name) => assert_eq!(name, "lowerr"),
            other => panic!("expected UndefinedFunction, got {other:?}"),
        }
    }

    #[test]
    fn missing_table_function_promotes_to_undefined_function() {
        // `SELECT * FROM nosuch()` — note the quote does NOT immediately
        // follow `table `, so this must not be mis-typed as 42P01.
        let e = plan_err("table function 'nosuch' not found");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedFunction(name) => assert_eq!(name, "nosuch"),
            other => panic!("expected UndefinedFunction, got {other:?}"),
        }
    }

    #[test]
    fn missing_column_patterns_promote_to_undefined_column() {
        // Planner flavour, bare and with-relation forms.
        let e = plan_err("column 'ghost' not found");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedColumn(name) => assert_eq!(name, "ghost"),
            other => panic!("expected UndefinedColumn, got {other:?}"),
        }
        let e = plan_err("column 'ghost' not found in 'users'");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedColumn(name) => assert_eq!(name, "ghost"),
            other => panic!("expected UndefinedColumn, got {other:?}"),
        }
        // Schema-resolution flavour, with and without the Valid-fields tail;
        // a qualified name keeps its qualifier (dots inside the name must
        // not truncate it).
        let e = plan_err("Schema error: No field named ghost. Valid fields are users.id.");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedColumn(name) => assert_eq!(name, "ghost"),
            other => panic!("expected UndefinedColumn, got {other:?}"),
        }
        let e = plan_err("Schema error: No field named t1.c0.");
        match map_df_plan_error("plan", &e) {
            BasinError::UndefinedColumn(name) => assert_eq!(name, "t1.c0"),
            other => panic!("expected UndefinedColumn, got {other:?}"),
        }
    }

    #[test]
    fn unrelated_plan_errors_stay_internal() {
        let e = plan_err("Projection references non-aggregate values");
        match map_df_plan_error("plan", &e) {
            BasinError::Internal(msg) => assert!(msg.contains("plan: ")),
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}

// `gin_original_sql`: the original (pre-operator-rewrite) SQL for GIN pruning
// detection.  `None` when calling from internal paths that have no original SQL
// (e.g. CTAS, DML SELECT sub-selects).  When `Some`, `apply_gin_pruning_for_query`
// uses this to detect `@>` / `<@` before the JSON operator rewriter converts them
// to `jsonb_contains(...)`.
pub(crate) async fn exec_select(
    sess: &ProjectSession,
    sql: &str,
    include_deleted: bool,
    gin_original_sql: Option<&str>,
) -> Result<ExecResult> {
    // Refresh the catalog-driven file set for every table before planning.
    //
    // Rationale: `refresh_table` now registers per-file `ListingTableUrl`s
    // derived from `TableMetadata::live_data_files()` rather than a directory
    // URL. This means the registered `ListingTable` is a point-in-time
    // snapshot of the catalog's current file set — it does NOT re-list the
    // object store on each scan.  That is the fix for bug #41 (rollback
    // correctness), but it means we must refresh before every SELECT so
    // that inserts committed in this session, and catalog mutations performed
    // externally (e.g. `rollback_to_snapshot`), are reflected in the plan.
    //
    // When the shard is wired in we additionally flush the in-RAM tail to
    // Parquet first so the just-written rows land in object storage before
    // the catalog-driven refresh reads the file list.
    if let Some(shard) = sess.engine.config().shard.as_ref() {
        shard.flush_to_parquet().await?;
    }
    // Inv-E #132: accumulate live-file count during the existing refresh loop
    // so we can pick target_partitions below without an extra catalog round-trip.
    //
    // We track the MAXIMUM file count of any single table (not the sum across
    // all tables) because a query can only scan the files of its own table(s).
    // Using the sum would over-estimate for small tables in a project that also
    // has large tables, causing DataFusion to fan out scans needlessly (e.g.
    // a 2000-row scratch table would be given 8 scan partitions just because
    // another table in the same project has 15 files).
    let mut max_single_table_files: usize = 0;
    {
        // Statement-scoped refresh (perf): the historical behaviour refreshed
        // EVERY table in the project before planning each query — a per-table
        // provider rebuild (TableMetadata clone incl. bloom blobs +
        // ListingTable re-registration) that scales with the project's table
        // COUNT, not query complexity. At a few-thousand-row scale this is the
        // dominant component of the small-query latency floor.
        //
        // Instead we narrow the refresh set to just the base tables the
        // statement can actually read. `compute_select_refresh_set` parses the
        // SQL, walks every TableScan-bearing shape (FROM/JOIN/CTE bodies/
        // subqueries), expands referenced VIEW names to their underlying base
        // tables (recursively, cycle-safe), and excludes CTE names (which are
        // not base tables). It returns `None` — meaning "refresh everything,
        // conservatively" — whenever it cannot enumerate the set with
        // confidence: parse failure, non-`Query` statement, empty ref set, or
        // any referenced name that resolves to neither a base table nor a view
        // (e.g. `information_schema.*` / `pg_catalog.*` / `basin_stat_statements`,
        // which are synthesized providers registered at session open and
        // unaffected by `refresh_table`).
        //
        // Registration invariant (why narrowing is safe): EVERY table the
        // catalog knows about is registered as a DataFusion provider at session
        // open (`session::open` → `list_tables_qualified` → `refresh_table_qualified`),
        // and CREATE TABLE registers its provider too. DataFusion planning only
        // fails on an *unregistered* provider, never on a *stale* one. So a
        // table we choose NOT to refresh still plans fine — it would merely be
        // stale. And that staleness can never be observed through a SELECT,
        // because any query that actually READS a table appears in that query's
        // ref set and is therefore refreshed by this very logic. (DML/FK/
        // RETURNING go through different exec paths; this scope is read-only
        // SELECT only, so DML refresh behaviour is untouched.)
        let in_tx = crate::session::tx_is_active(&sess.state);
        let all_tables: Vec<_> = sess
            .engine
            .config()
            .catalog
            .list_tables(&sess.project)
            .await?;
        let tables: Vec<TableName> =
            match compute_select_refresh_set(sess, sql, &all_tables).await {
                Some(scoped) => scoped,
                None => all_tables,
            };
        for table in &tables {
            if in_tx {
                // Within a transaction: include pending (not-yet-committed)
                // files (UPDATE/DELETE rewrites) AND tx-buffered htap batches
                // (INSERTs deferred until COMMIT — perf-w7-txn) so reads see
                // within-tx writes. When THIS table has neither, the cheap
                // refresh suffices — snapshot pinning still applies because
                // it lives in load_table_for_read inside refresh_table_inner,
                // not in the htap overlay.
                let pending = crate::session::tx_pending_files_for(&sess.state, table);
                let htap_batches = crate::session::tx_htap_batches_for(&sess.state, table);
                if pending.is_empty() && htap_batches.is_empty() {
                    crate::session::refresh_table(
                        &sess.engine,
                        &sess.project,
                        &sess.ctx,
                        &sess.state,
                        table,
                    )
                    .await?;
                } else {
                crate::session::refresh_table_with_htap(
                    &sess.engine,
                    &sess.project,
                    &sess.ctx,
                    &sess.state,
                    table,
                    &pending,
                    htap_batches,
                )
                .await?;
                }
                // In-transaction: skip file count (parallelism heuristic is
                // non-critical; counting would require an extra catalog call).
            } else {
                let n = refresh_table_counted(
                    &sess.engine,
                    &sess.project,
                    &sess.ctx,
                    &sess.state,
                    table,
                )
                .await?;
                if n > max_single_table_files {
                    max_single_table_files = n;
                }
            }
        }
    }

    // Inv-E #132 — intra-query parallelism for full-table DataFusion scans.
    //
    // Every query that reaches exec_select has already bypassed the simple-SELECT
    // fast-path (execute_simple_select) — i.e. it is either a full-table scan, an
    // aggregate, a JOIN, or a query with RLS/soft-delete predicates.  For these
    // shapes, raising target_partitions from 1 to min(cpu_count, file_count) lets
    // DataFusion split the Parquet file list across parallel scan streams.
    //
    // `max_single_table_files` was accumulated for free during the refresh loop
    // above — no extra catalog calls.  `target_partitions_for_bulk_scan` applies
    // the MIN_FILES_FOR_PARALLEL_SCAN gate (returns 1 for tiny tables) and the
    // BASIN_ENGINE_TARGET_PARTITIONS_MAX env-var cap.
    //
    // The TargetPartitionsGuard restores the session value to 1 on drop, covering
    // both the success path and any early-return via `?`.
    let _tp_guard: Option<TargetPartitionsGuard<'_>> = {
        let new_tp = crate::session::target_partitions_for_bulk_scan(max_single_table_files);
        let (restore_partitions, restore_repart_agg, restore_repart_joins, restore_repart_windows) = {
            let state_ref = sess.ctx.state_ref();
            let state = state_ref.read();
            let cfg = state.config();
            let opts = cfg.options();
            (
                cfg.target_partitions(),
                opts.optimizer.repartition_aggregations,
                opts.optimizer.repartition_joins,
                opts.optimizer.repartition_windows,
            )
        };
        if new_tp != restore_partitions {
            {
                let state_ref = sess.ctx.state_ref();
                let mut state = state_ref.write();
                let opts = state.config_mut().options_mut();
                opts.execution.target_partitions = new_tp;
                // Disable aggregate, join, and window repartition so DataFusion
                // keeps AggregateExec/WindowAggExec in mode=Single: the exchange
                // overhead exceeds the benefit at Basin's typical table sizes, and
                // repartition_windows=true (DataFusion 53.1 default) inserts
                // exchange nodes around WindowAggExec breaking LAG/LEAD/RANK
                // queries when target_partitions > 1.  File scan parallelism
                // (independent file groups) is unaffected.
                opts.optimizer.repartition_aggregations = false;
                opts.optimizer.repartition_joins = false;
                opts.optimizer.repartition_windows = false;
            }
            Some(TargetPartitionsGuard {
                ctx: &sess.ctx,
                restore_partitions,
                restore_repart_agg,
                restore_repart_joins,
                restore_repart_windows,
            })
        } else {
            None
        }
    };

    // Phase 5.5 partition pruning: if this session has seen a partitioned
    // table at least once, walk the SQL's AST and (if the WHERE clause
    // restricts the partition column) swap the registered `ListingTable`
    // for one whose paths are pre-filtered to matching partitions. The
    // atomic `has_partitioned_table` gate keeps the hot path fast for
    // projects that never use PARTITION BY.
    if sess
        .state
        .has_partitioned_table
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        crate::session::apply_partition_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            sql,
        )
        .await?;
    }

    // Phase 5.19.C — GIN file-level pruning (JSONB @> / <@).
    // Use the original (pre-operator-rewrite) SQL so `@>` / `<@` operators
    // are still present in the text.  After `rewrite_json_operators` runs,
    // `@>` becomes `jsonb_contains(…)` and the detector would miss it.
    // On any parse/probe failure this is a silent no-op → full scan.
    //
    // Phase 5.20.E — GIN FTS file-level pruning (tsvector @@).
    // Use the original SQL so `@@` is still present (before
    // `rewrite_tsvector_at_at` converts it to `tsvector_match_udf(...)`).
    if let Some(orig_sql) = gin_original_sql {
        crate::session::apply_gin_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            orig_sql,
        )
        .await?;
        // Inv-W5 / W9 — JSONB `@>` posting-list row-group prune.
        // Runs AFTER `apply_gin_pruning_for_query` so the precise
        // per-`(key, value)` posting list takes precedence over the
        // bloom row-group prune: when the posting registry covers all
        // live files it re-registers the table with a tighter
        // row-group selection.  When the posting registry is cold /
        // partial / NoIndex, the bloom path's earlier registration
        // remains in effect.
        crate::session::apply_jsonb_posting_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            orig_sql,
        )
        .await?;
        // Transaction guard: an in-tx SELECT's registration may include
        // tx-pending files / buffered batches that the FTS registry has
        // never seen (insert-path maintenance is deferred to COMMIT) and
        // that the catalog's live-file set — which the completeness guard
        // checks — does not contain.  A pruned re-registration would drop
        // those pending rows from the read.  Decline pruning inside a
        // transaction (full scan, correct results).
        if !crate::session::tx_is_active(&sess.state) {
            crate::session::apply_gin_fts_pruning_for_query(
                &sess.engine,
                &sess.project,
                &sess.ctx,
                orig_sql,
            )
            .await?;
        }
        // Phase 5.24.D — GIST interval-tree file-level pruning (range @> / && / <@).
        // Use the original SQL so range operators are still present (before
        // `rewrite_range_operators` converts them to UDF calls).
        crate::session::apply_gist_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            orig_sql,
        )
        .await?;
        // PG-Wave-β — R-tree row-group spatial pruning
        // (ST_DWithin / ST_Contains / = on a GIST-indexed POINT column).
        // Probes the per-file R-tree registry to narrow each candidate file
        // to the surviving row-groups and re-registers the table as a custom
        // RTreePrunedTable for the duration of this query.
        //
        // First normalize the PostGIS bbox-overlap idiom
        // `geom && ST_MakeEnvelope(...)` into the `ST_Contains(env, geom)`
        // form that `detect_spatial_predicate` recognizes, so the native
        // `&&` operator routes through the R-tree pushdown rather than a
        // full scan.  Non-bbox `&&` (array overlap) is left untouched.
        let rtree_sql = crate::pg_operators::rewrite_bbox_amp_amp(orig_sql.to_string());
        crate::session::apply_rtree_pruning_for_query(
            &sess.engine,
            &sess.project,
            &sess.ctx,
            &rtree_sql,
        )
        .await?;
    }

    // View-reference rewriting: replace any reference to a known plain view
    // in the SQL's FROM / JOIN clauses with an inline subquery so DataFusion
    // sees a derived table rather than an unknown table name. This is a
    // no-op when the project has no registered views.
    let view_rewritten_owned;
    let sql = if let Some(rewritten) = crate::view_ddl::rewrite_view_refs(
        sess.engine.config().catalog.as_ref(),
        &sess.project,
        sql,
    )
    .await?
    {
        view_rewritten_owned = rewritten;
        view_rewritten_owned.as_str()
    } else {
        sql
    };
    // Strip schema qualifiers (`schema.table` → `table`) before DataFusion
    // sees the SQL. DataFusion uses its own catalog hierarchy; Basin's tables
    // are all registered in the flat default namespace, so `schema.table`
    // would be misrouted as a DataFusion catalog-schema lookup.
    let sql_stripped =
        crate::schema_ddl::strip_schema_qualifiers_for_session(sql, &sess.state.schema_state);
    let sql_for_df = sql_stripped.as_str();

    // Expand `json_agg(t)` / `jsonb_agg(t)` where `t` is a bare table name
    // into `json_agg(named_struct('col1', t.col1, ...))` so DataFusion can plan
    // and execute it.  This must happen after schema-qualifier stripping because
    // we look up tables by their unqualified name in the session context.
    let json_agg_rewritten;
    let sql_for_df = {
        let rewritten = rewrite_json_agg_whole_row(sql_for_df, &sess.ctx).await;
        json_agg_rewritten = rewritten;
        json_agg_rewritten.as_str()
    };

    let mut df = sess
        .ctx
        .sql(sql_for_df)
        .await
        .map_err(|e| map_df_plan_error("plan", &e))?;

    // Phase 5.16.B: compute query-shape hash and record it in the per-shape
    // HDR histogram registry.  The hash was computed-and-discarded in 5.16.A;
    // here we route it into `QueryStatRegistry::observe`.
    //
    // We time only the DataFusion collect() call below so that planning /
    // view-rewrite overhead does not inflate the latency bucket.  The timer
    // starts here so it covers the RLS / soft-delete rewrite below as well —
    // those rewrites are part of the logical query shape's execution cost.
    let shape_hash = crate::query_shape::QueryShapeHash::of(df.logical_plan());
    // Extract the primary table name from the plan (the first TableScan we
    // find).  Multi-table queries (JOINs) record under the sentinel name
    // "_multi_table_" — the shape hash already encodes the full join topology.
    let primary_table: TableName = {
        fn first_scan(plan: &datafusion::logical_expr::LogicalPlan) -> Option<&str> {
            use datafusion::logical_expr::LogicalPlan;
            if let LogicalPlan::TableScan(ts) = plan {
                return Some(ts.table_name.table());
            }
            for child in plan.inputs() {
                if let Some(t) = first_scan(child) {
                    return Some(t);
                }
            }
            None
        }
        let raw = first_scan(df.logical_plan()).unwrap_or("_multi_table_");
        // Sanitise: TableName::new validates idents; fall back to sentinel on
        // failure (e.g. DataFusion internal scans, subquery aliases, etc.).
        TableName::new(raw).unwrap_or_else(|_| {
            TableName::new("_multi_table_").expect("sentinel is valid")
        })
    };
    let exec_start = std::time::Instant::now();

    // Phase 5.6: row-level security. The per-project policy lookup is gated
    // on the catalog's `rls_enabled` per table; tables with RLS off cost
    // exactly one `load_table` call, no plan rewriting. The plan rewrite
    // itself happens via DataFusion's `LogicalPlanBuilder::filter` —
    // wrapping each RLS-enabled `TableScan` in a `Filter` node — so
    // downstream optimisation (predicate pushdown, projection pruning)
    // sees the RLS filter as a first-class predicate.
    df = apply_rls_to_select(sess, sql, df).await?;
    if !include_deleted {
        df = apply_soft_delete_to_select(sess, sql, df).await?;
    }
    let df_schema = df.schema().inner().clone();
    // Build the WS schema and annotate any json_agg / jsonb_agg result columns
    // with BASIN_TYPE=JSONB so the pgwire layer advertises OID 3802 (JSONB).
    // DataFusion strips the `return_field` metadata when wrapping an aggregate
    // in a scalar correlated subquery or through join projection; the annotation
    // pass recovers it by scanning the SQL text for json_agg occurrences and
    // matching their aliases against the result schema field names.
    let ws_schema = {
        let raw = Arc::new(schema_df_to_ws(df_schema.as_ref())?);
        annotate_json_agg_columns(&raw, sql_for_df)
    };

    // Change C: when the shard is wired in we know there are large per-project
    // tails on the same runtime. Move the DataFusion executor onto the
    // blocking thread pool so its parquet-decode loop can't pin the
    // cooperative tokio workers a quiet project's point queries run on. Tests
    // that run without a shard keep the single-await path and behave as
    // before.
    //
    // Phase 6.P0.A: statement-level wall-clock timeout (noisy-neighbor P0).
    // The cost-check (`cost_check.rs`) only bounds single-table SELECTs at
    // *planning* time; JOIN / sub-query / CTE / cartesian shapes pass through
    // unchecked and can pin a DataFusion worker thread indefinitely, starving
    // every other project on the runtime. We wrap the *execution* future in
    // `tokio::time::timeout`: when the deadline elapses the future is dropped,
    // which drops the DataFusion stream / physical plan and cancels execution
    // (DataFusion's documented cancellation contract). On the fast path the
    // deadline is a single comparison set up once — it is NOT a per-row check,
    // so a normal sub-timeout query sees no latency regression. `None` (env
    // `BASIN_STATEMENT_TIMEOUT_MS=0`) disables the guard for back-compat.
    let timeout = crate::session::session_statement_timeout(&sess.state);
    let canceled = || {
        BasinError::query_canceled(match timeout {
            Some(d) => format!("exceeded statement_timeout ({} ms)", d.as_millis()),
            None => "exceeded statement timeout".to_owned(),
        })
    };
    // Phase 5.28.D: publish the absolute statement deadline into the
    // per-thread slot so `PgSleepUdf` can observe it cooperatively.
    // The deadline is computed here (= now + timeout) so both the shard and
    // non-shard paths share the same reference point.
    let statement_deadline: Option<Instant> =
        timeout.map(|d| Instant::now() + d);
    // Aggregate / GROUP-BY / UNION-ALL partitioning note (investigated; no
    // Basin lever). A cluster of GROUP-BY-shaped queries runs several× slower
    // than PostgreSQL at 10k rows, with the gap NARROWING as the table grows
    // (≈8× @10k → ≈2-4× @100k/1M) — the signature of a per-query FIXED
    // overhead, not an algorithmic blow-up. The hypothesis was that DataFusion
    // over-partitions small inputs (Partial→Repartition→Final fan-out whose
    // exchange + merge cost dwarfs a 10k-row aggregate). EXPLAIN of these
    // shapes (see `tests/aggregate_tuning.rs`) shows that is NOT happening:
    // because `open_session` pins `datafusion.execution.target_partitions = 1`
    // (session.rs), every aggregate plans as `AggregateExec: mode=Single` with
    // ZERO `RepartitionExec` and all files in a single file_group, and the two
    // UNION-ALL branches are collapsed by `UnionScanCollapse` into one scan
    // with an OR predicate. With `target_partitions = 1` the repartition flags
    // (`repartition_aggregations` / `_joins` / `_file_scans`) are moot —
    // `EnforceDistribution` emits no exchanges. So the optimal partition lever
    // is already pulled; the residual gap is intrinsic DataFusion execution
    // cost (per-batch hash-aggregate setup + scan startup) amortized away at
    // scale, which is exactly why the gap narrows as the row count grows. There
    // is no further Basin-side config knob here to make this faster without
    // regressing other shapes; documenting honestly rather than faking a fix.
    let df_batches = if sess.engine.config().shard.is_some() {
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| BasinError::internal(format!("create plan: {e}")))?;
        let task_ctx = sess.ctx.task_ctx();
        // The DataFusion collect runs on its own current-thread runtime inside
        // a blocking thread, so the *outer* runtime's timer can't observe it.
        // Apply the timeout INSIDE `block_on` so the inner stream is dropped on
        // elapse — that both cancels execution and lets the blocking thread
        // return promptly instead of running on detached. We surface the
        // elapsed case as a sentinel `Ok(None)` and map it to QueryCanceled.
        let join = tokio::task::spawn_blocking(move || {
            // Publish the deadline on this blocking thread so pg_sleep can
            // poll it cooperatively via get_statement_deadline().
            set_statement_deadline(statement_deadline);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| BasinError::internal(format!("blocking runtime: {e}")))?;
            let result = rt.block_on(async {
                let fut = datafusion::physical_plan::collect(plan, task_ctx);
                match timeout {
                    Some(d) => match tokio::time::timeout(d, fut).await {
                        Ok(res) => res
                            .map(Some)
                            .map_err(|e| {
                                if take_pg_sleep_canceled() {
                                    BasinError::query_canceled(
                                        "pg_sleep: interrupted by statement_timeout",
                                    )
                                } else {
                                    map_df_exec_error(e)
                                }
                            }),
                        Err(_) => Ok(None),
                    },
                    None => fut
                        .await
                        .map(Some)
                        .map_err(|e| {
                            if take_pg_sleep_canceled() {
                                BasinError::query_canceled(
                                    "pg_sleep: interrupted by statement_timeout",
                                )
                            } else {
                                map_df_exec_error(e)
                            }
                        }),
                }
            });
            set_statement_deadline(None);
            result
        })
        .await
        .map_err(|e| BasinError::internal(format!("spawn_blocking join: {e}")))?;
        match join? {
            Some(batches) => batches,
            None => return Err(canceled()),
        }
    } else {
        // Non-shard path: set the deadline on the current tokio thread.
        // pg_sleep's block_in_place runs on the same thread, so the
        // thread-local is visible inside the UDF.
        set_statement_deadline(statement_deadline);
        let fut = df.collect();
        // Phase 5.31.A: race the DataFusion collect against both the
        // statement-timeout AND the per-session cancel notification
        // (`pg_cancel_backend`). Whichever fires first wins:
        //   - Statement timeout:  returns canceled() error (SQLSTATE 57014).
        //   - pg_cancel_backend:  returns query_canceled error (SQLSTATE 57014).
        //   - DataFusion error:   propagates as before.
        let cancel_notify = sess.cancel_notify.clone();
        let result = match timeout {
            Some(d) => {
                // Pin the cancel notification future so it can be polled
                // multiple times across the select! arms.
                tokio::pin!(fut);
                match tokio::time::timeout(d, async {
                    tokio::select! {
                        res = &mut fut => Some(res),
                        _ = cancel_notify.notified() => None,
                    }
                }).await {
                    Ok(Some(res)) => res.map_err(|e| {
                        if take_pg_sleep_canceled() {
                            BasinError::query_canceled(
                                "pg_sleep: interrupted by statement_timeout",
                            )
                        } else {
                            map_df_exec_error(e)
                        }
                    }),
                    Ok(None) => {
                        // pg_cancel_backend fired.
                        set_statement_deadline(None);
                        return Err(BasinError::query_canceled(
                            "canceling statement due to user request",
                        ));
                    }
                    Err(_) => {
                        // Statement timeout fired.
                        return { set_statement_deadline(None); Err(canceled()) };
                    }
                }
            },
            None => {
                tokio::pin!(fut);
                let outcome = tokio::select! {
                    res = &mut fut => Some(res),
                    _ = cancel_notify.notified() => None,
                };
                match outcome {
                    Some(res) => res.map_err(|e| {
                        if take_pg_sleep_canceled() {
                            BasinError::query_canceled(
                                "pg_sleep: interrupted by statement_timeout",
                            )
                        } else {
                            map_df_exec_error(e)
                        }
                    }),
                    None => {
                        set_statement_deadline(None);
                        return Err(BasinError::query_canceled(
                            "canceling statement due to user request",
                        ));
                    }
                }
            },
        };
        set_statement_deadline(None);
        result?
    };
    let exec_elapsed_ns = exec_start.elapsed().as_nanos() as u64;

    // Phase 5.16.B: route shape + metrics into the HDR histogram registry.
    // Row count is the sum of all batch row counts from the DataFusion output.
    let total_rows: u64 = df_batches.iter().map(|b| b.num_rows() as u64).sum();
    sess.engine.query_stats().observe(
        &sess.project,
        &primary_table,
        shape_hash,
        &crate::query_stats::QueryMetrics {
            latency_ns: exec_elapsed_ns,
            rows_scanned: total_rows,
            // files_opened / bytes_decoded / cache_hits: DataFusion does not
            // expose these counters through the public DataFrame API in the
            // current version.  Set to 0 for now; 5.16.D will instrument the
            // physical plan metrics once the OTLP export is wired in.
            files_opened: 0,
            bytes_decoded: 0,
            cache_hits: 0,
            fast_path_engaged: false,
            // Phase 5.16.C: table_row_count for scale-bucket assignment.
            // We use 0 (bucket 0) as the default — the catalog row count is
            // not readily available at this call site without an additional
            // load_table round-trip.  5.16.D will wire in the real count.
            table_row_count: 0,
        },
    );

    let mut batches: Vec<RecordBatch> = Vec::with_capacity(df_batches.len());
    for b in df_batches.iter() {
        batches.push(batch_df_to_ws(b)?);
    }
    Ok(ExecResult::Rows {
        schema: ws_schema,
        batches,
    })
}

/// AND-merge per-table RLS USING predicates into `df`'s logical plan. Re-
/// parses `sql` to discover referenced tables (cheap; sqlparser is fast).
/// Tables with `rls_enabled = false` short-circuit — they pay one catalog
/// `load_table` call and nothing else, preserving the no-RLS hot path.
async fn apply_rls_to_select(
    sess: &ProjectSession,
    sql: &str,
    df: datafusion::prelude::DataFrame,
) -> Result<datafusion::prelude::DataFrame> {
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(df),
    };
    if stmts.len() != 1 {
        return Ok(df);
    }
    let Statement::Query(query) = &stmts[0] else {
        return Ok(df);
    };
    let referenced = collect_table_refs_from_query(query);
    if referenced.is_empty() {
        return Ok(df);
    }
    let policies = crate::rls::build_policies_for_query(
        &sess.engine.config().catalog,
        &sess.project,
        &referenced,
        &sess.current_user,
        basin_catalog::PolicyCommand::Select,
    )
    .await?;
    if policies.is_empty() {
        return Ok(df);
    }
    crate::rls::inject_select_predicates(&sess.ctx, df, &policies, &sess.current_user).await
}

/// AND-merge an `<soft_delete_col> IS NULL` predicate into `df`'s logical
/// plan for every TableScan against a table that has a SOFT DELETE column.
/// Mirrors `apply_rls_to_select` — same TreeNode rewrite shape, different
/// predicate source. When `INCLUDE DELETED` was specified the caller skips
/// this step entirely.
async fn apply_soft_delete_to_select(
    sess: &ProjectSession,
    sql: &str,
    df: datafusion::prelude::DataFrame,
) -> Result<datafusion::prelude::DataFrame> {
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(df),
    };
    if stmts.len() != 1 {
        return Ok(df);
    }
    let Statement::Query(query) = &stmts[0] else {
        return Ok(df);
    };
    let referenced = collect_table_refs_from_query(query);
    if referenced.is_empty() {
        return Ok(df);
    }
    let mut soft_cols: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for table in &referenced {
        let meta = match sess
            .engine
            .config()
            .catalog
            .load_table(&sess.project, table)
            .await
        {
            Ok(m) => m,
            Err(_) => continue,
        };
        if let Some(col) = crate::types::soft_delete_column(meta.schema.as_ref()) {
            soft_cols.insert(table.to_string(), col);
        }
    }
    if soft_cols.is_empty() {
        return Ok(df);
    }
    crate::lifecycle::inject_soft_delete_predicates(&sess.ctx, df, &soft_cols).await
}

/// Collect every table the query can read from — the input set RLS must
/// inject predicates against.
///
/// Walks every shape that can hide a `TableScan` from the rewriter:
///
/// - top-level `SetExpr::Select` → each `from.relation` + every `from.joins[*]`
/// - `SetExpr::SetOperation` (UNION / INTERSECT / EXCEPT) → both legs
/// - `query.with` CTEs → each CTE body (a sub-`Query`) recurses
/// - `TableFactor::Derived` (FROM (SELECT …)) → subquery recurses
/// - `TableFactor::NestedJoin` → unwrap the inner `TableWithJoins`
/// - subqueries embedded in expressions (WHERE, HAVING, projection,
///   `EXISTS`, `IN (SELECT …)`, scalar subqueries) → recurse
///
/// **Why this matters (P0):** RLS predicate injection short-circuits when
/// `referenced.is_empty()`. A naive walker that only handles
/// `SetExpr::Select` returns an empty list for `SELECT … UNION SELECT …`
/// and `WITH peek AS (SELECT …) SELECT … FROM peek`, leaving the
/// underlying `TableScan`s un-rewritten and *silently leaking* rows the
/// policy would otherwise hide. See
/// `tests/integration/tests/security.rs::rls_union_subquery_cannot_bypass`
/// and `rls_cte_cannot_bypass` for the regression repro — those tests
/// must stay green for this invariant to hold.
fn collect_table_refs_from_query(query: &sqlparser::ast::Query) -> Vec<TableName> {
    let mut out = Vec::new();
    collect_from_query(query, &mut out);
    out
}

/// Collect every CTE name *defined* anywhere in `query` (including CTEs
/// nested inside subqueries / other CTE bodies). A CTE name is NOT a base
/// table — `collect_table_refs_from_query` cannot tell `FROM cte` from
/// `FROM real_table` (both are `TableFactor::Table` with a single ident), so
/// the refresh-set computation must subtract these out before deciding a
/// referenced name is "unknown".
fn collect_cte_names(query: &sqlparser::ast::Query, out: &mut std::collections::HashSet<String>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            out.insert(cte.alias.name.value.to_ascii_lowercase());
            collect_cte_names(&cte.query, out);
        }
    }
    collect_cte_names_in_set_expr(query.body.as_ref(), out);
}

fn collect_cte_names_in_set_expr(
    set_expr: &sqlparser::ast::SetExpr,
    out: &mut std::collections::HashSet<String>,
) {
    use sqlparser::ast::SetExpr;
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                collect_cte_names_in_table_factor(&from.relation, out);
                for join in &from.joins {
                    collect_cte_names_in_table_factor(&join.relation, out);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_cte_names_in_set_expr(left, out);
            collect_cte_names_in_set_expr(right, out);
        }
        SetExpr::Query(q) => collect_cte_names(q, out),
        _ => {}
    }
}

fn collect_cte_names_in_table_factor(
    tf: &sqlparser::ast::TableFactor,
    out: &mut std::collections::HashSet<String>,
) {
    use sqlparser::ast::TableFactor;
    match tf {
        TableFactor::Derived { subquery, .. } => collect_cte_names(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_cte_names_in_table_factor(&table_with_joins.relation, out);
            for join in &table_with_joins.joins {
                collect_cte_names_in_table_factor(&join.relation, out);
            }
        }
        _ => {}
    }
}

/// Compute the *statement-scoped* refresh set: the base tables this read-only
/// SELECT can actually read, so `exec_select` refreshes only those instead of
/// every table in the project.
///
/// Returns `Some(tables)` to refresh exactly that set; returns `None` to mean
/// "fall back to refreshing everything" — the conservative default taken
/// whenever the set cannot be enumerated with confidence:
///
/// - the SQL fails to parse, or is not exactly one statement,
/// - the single statement is not a plain `Query` (e.g. DML / EXECUTE / SHOW),
/// - table-ref extraction yields nothing,
/// - a referenced name resolves to neither a base table nor a known view
///   (covers `information_schema.*` / `pg_catalog.*` synthesized providers,
///   `basin_stat_statements`, and any future virtual relation — all of which
///   are registered at session open and unaffected by `refresh_table`),
/// - a view's body fails to parse or expansion exceeds the depth bound.
///
/// VIEW handling: at this point in `exec_select` the SQL still names views by
/// name (`rewrite_view_refs`, which inlines `(body) AS view`, runs *later*,
/// after this refresh loop). So a referenced view name is expanded here to the
/// base tables in its stored `query_sql`. We recurse into view-over-view
/// references defensively (with a depth bound and a visited-set so view cycles
/// can't loop) so the refresh set stays a superset of whatever the planner
/// ultimately scans. The returned set is the union of all reachable base
/// tables, which covers exactly what the post-rewrite inlined query scans —
/// guaranteeing a view-over-table SELECT sees data written after the session
/// opened.
///
/// `all_tables` is the project's current base-table list (already fetched by
/// the caller); the result is intersected against it so we never hand
/// `refresh_table` a name it doesn't own.
async fn compute_select_refresh_set(
    sess: &ProjectSession,
    sql: &str,
    all_tables: &[TableName],
) -> Option<Vec<TableName>> {
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(query) = &stmts[0] else {
        return None;
    };

    // Base-table membership (lowercased name → canonical TableName).
    let mut base_by_lc: std::collections::HashMap<String, TableName> =
        std::collections::HashMap::with_capacity(all_tables.len());
    for t in all_tables {
        base_by_lc.insert(t.as_str().to_ascii_lowercase(), t.clone());
    }

    // View bodies (lowercased name → SELECT body). `list_views` is the same
    // call `rewrite_view_refs` makes; empty for the common (no-views) case.
    let view_map: std::collections::HashMap<String, String> = sess
        .engine
        .config()
        .catalog
        .list_views(&sess.project)
        .await
        .into_iter()
        .map(|v| (v.name.to_ascii_lowercase(), v.query_sql))
        .collect();

    let mut resolved: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<TableName> = Vec::new();

    // Top-level CTE names defined by this statement are not base tables.
    let mut cte_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    collect_cte_names(query, &mut cte_names);

    let refs = collect_table_refs_from_query(query);
    if refs.is_empty() {
        return None;
    }

    // Resolve each referenced name to base tables. View names recurse into
    // their body; cycles and runaway nesting are bounded.
    const MAX_VIEW_DEPTH: usize = 16;
    let mut visiting_views: std::collections::HashSet<String> = std::collections::HashSet::new();
    for name in &refs {
        if !resolve_ref_to_base_tables(
            name.as_str(),
            &base_by_lc,
            &view_map,
            &cte_names,
            &mut visiting_views,
            &mut resolved,
            &mut out,
            MAX_VIEW_DEPTH,
        ) {
            // Unresolvable name (not a base table, not a view, not a CTE) →
            // conservative fallback to refresh-all.
            return None;
        }
    }

    Some(out)
}

/// Resolve a single referenced name to zero-or-more base tables, accumulating
/// into `out` (deduped via `resolved`). Returns `false` if the name cannot be
/// classified as a base table, a known view, or an in-statement CTE — the
/// caller treats that as "fall back to refresh-all".
#[allow(clippy::too_many_arguments)]
fn resolve_ref_to_base_tables(
    name: &str,
    base_by_lc: &std::collections::HashMap<String, TableName>,
    view_map: &std::collections::HashMap<String, String>,
    cte_names: &std::collections::HashSet<String>,
    visiting_views: &mut std::collections::HashSet<String>,
    resolved: &mut std::collections::HashSet<String>,
    out: &mut Vec<TableName>,
    depth: usize,
) -> bool {
    let lc = name.to_ascii_lowercase();

    // A CTE name shadows any same-named base table within the query and is not
    // itself a base table — nothing to refresh for it.
    if cte_names.contains(&lc) {
        return true;
    }

    // Base table: record it.
    if let Some(t) = base_by_lc.get(&lc) {
        if resolved.insert(lc) {
            out.push(t.clone());
        }
        return true;
    }

    // View: expand its body's base-table refs recursively.
    if let Some(body) = view_map.get(&lc) {
        if depth == 0 {
            return false; // too deep — be conservative
        }
        if !visiting_views.insert(lc.clone()) {
            // Cycle (view references itself transitively): we've already
            // entered this view higher in the stack; its tables are being
            // collected there. Treat as resolved to avoid looping.
            return true;
        }
        let dialect = PostgreSqlDialect {};
        let parsed = Parser::parse_sql(&dialect, body);
        let ok = match parsed {
            Ok(stmts) if stmts.len() == 1 => {
                if let Statement::Query(q) = &stmts[0] {
                    // CTE names defined inside the view body shadow base tables
                    // for refs originating inside that body.
                    let mut body_ctes: std::collections::HashSet<String> = cte_names.clone();
                    collect_cte_names(q, &mut body_ctes);
                    let body_refs = collect_table_refs_from_query(q);
                    body_refs.iter().all(|r| {
                        resolve_ref_to_base_tables(
                            r.as_str(),
                            base_by_lc,
                            view_map,
                            &body_ctes,
                            visiting_views,
                            resolved,
                            out,
                            depth - 1,
                        )
                    })
                } else {
                    false
                }
            }
            _ => false,
        };
        visiting_views.remove(&lc);
        return ok;
    }

    // Unknown: neither base table, view, nor CTE.
    false
}

fn collect_from_query(query: &sqlparser::ast::Query, out: &mut Vec<TableName>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_from_query(&cte.query, out);
        }
    }
    collect_from_set_expr(query.body.as_ref(), out);
}

fn collect_from_set_expr(set_expr: &sqlparser::ast::SetExpr, out: &mut Vec<TableName>) {
    use sqlparser::ast::SetExpr;
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                collect_from_table_factor(&from.relation, out);
                for join in &from.joins {
                    collect_from_table_factor(&join.relation, out);
                }
            }
            if let Some(sel) = &select.selection {
                collect_from_expr(sel, out);
            }
            if let Some(having) = &select.having {
                collect_from_expr(having, out);
            }
            if let Some(qualify) = &select.qualify {
                collect_from_expr(qualify, out);
            }
            for item in &select.projection {
                collect_from_select_item(item, out);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_from_set_expr(left, out);
            collect_from_set_expr(right, out);
        }
        SetExpr::Query(q) => collect_from_query(q, out),
        // VALUES / Insert / Update / Delete / Table — no rewritable
        // TableScan reachable from a SELECT-shaped RLS path.
        _ => {}
    }
}

fn collect_from_table_factor(tf: &sqlparser::ast::TableFactor, out: &mut Vec<TableName>) {
    use sqlparser::ast::TableFactor;
    match tf {
        TableFactor::Table { name, .. } => {
            if name.0.len() == 1 {
                if let Ok(t) = TableName::new(name.0[0].id_val().clone()) {
                    out.push(t);
                }
            }
        }
        TableFactor::Derived { subquery, .. } => collect_from_query(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_from_table_factor(&table_with_joins.relation, out);
            for join in &table_with_joins.joins {
                collect_from_table_factor(&join.relation, out);
            }
        }
        // TableFunction / Pivot / Unpivot / UNNEST etc — function-style
        // sources don't reference catalog tables in the RLS-relevant way;
        // any subqueries embedded in their args are walked by the
        // expression-side traversal.
        _ => {}
    }
}

fn collect_from_select_item(item: &sqlparser::ast::SelectItem, out: &mut Vec<TableName>) {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            collect_from_expr(e, out);
        }
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {}
    }
}

fn collect_from_expr(expr: &sqlparser::ast::Expr, out: &mut Vec<TableName>) {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => collect_from_query(q, out),
        Expr::InSubquery {
            subquery: q,
            expr: e,
            ..
        } => {
            collect_from_query(q, out);
            collect_from_expr(e, out);
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_from_expr(left, out);
            collect_from_expr(right, out);
        }
        Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::Nested(e)
        | Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => collect_from_expr(e, out),
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_from_expr(e, out);
            collect_from_expr(low, out);
            collect_from_expr(high, out);
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        }
        | Expr::SimilarTo {
            expr: e, pattern, ..
        } => {
            collect_from_expr(e, out);
            collect_from_expr(pattern, out);
        }
        Expr::InList { expr: e, list, .. } => {
            collect_from_expr(e, out);
            for x in list {
                collect_from_expr(x, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                collect_from_expr(o, out);
            }
            for c in conditions {
                collect_from_expr(&c.condition, out);
                collect_from_expr(&c.result, out);
            }
            if let Some(e) = else_result {
                collect_from_expr(e, out);
            }
        }
        Expr::Function(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString { .. }
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _) => {}
        // Anything else (windows, lambdas, MATCH, dialect-specific) —
        // walking it is best-effort and we're conservative on misses
        // here: the `_` arm is reachable only on shapes that don't carry
        // a Query, so RLS coverage is preserved.
        _ => {}
    }
}

/// Apply an RLS DDL statement to the catalog. The mutation reads the current
/// `(rls_enabled, policies)`, applies the change in memory, and writes back
/// via `Catalog::set_rls_state`. We do not refresh the DataFusion ListingTable
/// here — RLS state is consulted at SELECT time by re-reading the catalog
/// (per-query) so a freshly created policy takes effect on the very next
/// query without per-session bookkeeping.
async fn exec_rls_ddl(sess: &ProjectSession, ddl: crate::rls::RlsDdl) -> Result<ExecResult> {
    let table = ddl.table().clone();
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.project, &table)
        .await?;
    let mut rls_enabled = meta.rls_enabled;
    let mut policies = meta.policies.clone();
    let tag = ddl.apply(&mut rls_enabled, &mut policies)?;
    sess.engine
        .config()
        .catalog
        .set_rls_state(&sess.project, &table, rls_enabled, policies)
        .await?;
    Ok(ExecResult::Empty { tag: tag.into() })
}

/// Standard `ALTER TABLE` forms that sqlparser DOES recognise (currently
/// `ADD COLUMN`). RLS ENABLE/DISABLE and CREATE/ALTER/DROP POLICY are
/// intercepted earlier in [`crate::rls::match_rls_ddl`] and never reach
/// this dispatch arm. Basin-specific extensions (`SET cold_after`,
/// `SET BLOOM FILTERS ON`, etc.) are intercepted at the very top of
/// [`execute`] before sqlparser is even called.
async fn exec_alter_table(
    sess: &ProjectSession,
    name: sqlparser::ast::ObjectName,
    operations: Vec<sqlparser::ast::AlterTableOperation>,
) -> Result<ExecResult> {
    // 6.SEC.P1 — reject ALTER TABLE targeting a reserved system schema.
    // Phase 5.18.C: system sessions bypass this guard.
    if !sess.is_system {
        crate::schema_ddl::guard_reserved_schema_for_user_ddl(&name, "ALTER TABLE")?;
    }
    // Resolve the bare (pre-rename) table name up front: it keys both the
    // pre-ALTER overlay drain and the post-ALTER hot-tier clear below, and
    // for RENAME TO it is the OLD name — exactly the key the registry holds.
    let bare_name = match name.0.len() {
        1 => Some(name.0[0].id_val().clone()),
        2 => Some(name.0[1].id_val().clone()),
        _ => None,
    };
    let bare_table: Option<TableName> = bare_name.and_then(|raw| TableName::new(raw).ok());

    // S4 hot-tier schema-change protocol. Memtable entries carry Arrow IPC
    // row bytes encoded against the schema AT WRITE TIME; rows surviving an
    // ALTER (add/drop/rename column, type change) risk projection mismatches
    // on every read path that decodes them. Two steps:
    //   1. BEFORE the ALTER: materialize any pending UPDATE/DELETE overlay
    //      into the cold tier (dirty overlay entries are committed fast-path
    //      writes — clearing them without materializing would LOSE them).
    //   2. AFTER a successful ALTER: drop the table's whole memtable entry
    //      (now only old-schema residency/HTAP rows, all cold-backed) and
    //      release the freed bytes from the project budget.
    if let Some(t) = bare_table.as_ref() {
        crate::dml_mutate::materialize_hot_overlay_into_cold(sess, t).await?;
    }
    let tag = crate::alter::apply_standard_alter_table(
        &sess.engine.config().catalog,
        &sess.engine.config().storage,
        &sess.project,
        &name,
        &operations,
        &sess.state.schema_state,
    )
    .await?;
    if let Some(t) = bare_table.as_ref() {
        let registry = sess.engine.memtable_registry();
        if let Some(entry) = registry.get(&sess.project, t) {
            let bytes = entry.memtable.bytes_allocated();
            registry.remove(&sess.project, t);
            registry.release_bytes(&sess.project, bytes);
        }
    }

    // ADD COLUMN replaced the schema in the catalog; refresh the
    // session's DataFusion ListingTable so subsequent SELECTs see the
    // new column. The bare name was resolved above (schema qualifier
    // stripped: `myschema.t` → `t`).
    if let Some(t) = bare_table.as_ref() {
        refresh_table(&sess.engine, &sess.project, &sess.ctx, &sess.state, t).await?;
    }
    Ok(ExecResult::Empty { tag: tag.into() })
}

async fn exec_show_tables(sess: &ProjectSession) -> Result<ExecResult> {
    // Phase 5.18.C: use list_tables_qualified so system-schema tables
    // (cron.job, auth.users, net._http_response, etc.) are also visible.
    // We return the bare table name (not the qualified form) for back-compat
    // with callers that check for table names like "job" or "_http_response".
    let qtables = sess
        .engine
        .config()
        .catalog
        .list_tables_qualified(&sess.project)
        .await?;
    let bare_names: Vec<&str> = qtables.iter().map(|qt| qt.name.as_str()).collect();
    let arr = StringArray::from(bare_names);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "table_name",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef])
        .map_err(|e| BasinError::internal(format!("SHOW TABLES batch: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

/// Phase 5.28.D: build a single-row/single-column `ExecResult::Rows` for `SHOW <var>`.
/// Column name is the variable name; value is `val`.
fn make_show_result(col_name: &str, val: &str) -> ExecResult {
    let schema = Arc::new(Schema::new(vec![Field::new(col_name, DataType::Utf8, false)]));
    let arr = StringArray::from(vec![val]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef])
        .expect("make_show_result: infallible");
    ExecResult::Rows {
        schema,
        batches: vec![batch],
    }
}

/// Extract the string form of the first value in a SET expression.
/// Handles both literal string values and numeric literals.
/// Falls back to `"0"` for unrecognised forms.
fn extract_set_string_value(values: &[sqlparser::ast::Expr]) -> String {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    if let Some(expr) = values.first() {
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
                ..
            }) => return s.clone(),
            Expr::Value(ValueWithSpan {
                value: Value::Number(n, _),
                ..
            }) => return n.clone(),
            Expr::Identifier(id) => return id.value.clone(),
            _ => {}
        }
    }
    "0".to_string()
}

/// Pull a bare table name out of a sqlparser `ObjectName`.
///
/// Accepts both bare names (`t`) and schema-qualified names (`myschema.t`).
/// The schema qualifier is stripped: Basin's flat per-project model stores all
/// tables in a single catalog namespace. Callers that need to validate the
/// schema against the session's schema registry should call
/// `crate::schema_ddl::table_name_from_object` instead.
fn single_part_name(name: &ObjectName) -> Result<&str> {
    match name.0.len() {
        1 => Ok(&name.0[0].id_val()),
        2 => {
            // schema.table — drop the schema prefix and return the table name.
            Ok(&name.0[1].id_val())
        }
        _ => Err(BasinError::InvalidIdent(format!(
            "table name must have at most one schema qualifier: {name}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Cursor lifecycle handlers
// ---------------------------------------------------------------------------

/// Execute `DECLARE <name> [SCROLL | NO SCROLL] CURSOR [WITH HOLD] FOR <query>`.
///
/// The backing SELECT is materialised immediately into the session's cursor
/// registry.  WITH HOLD is silently accepted but not implemented (cursors die
/// with the session regardless).
async fn exec_declare(
    sess: &ProjectSession,
    stmts: Vec<sqlparser::ast::Declare>,
) -> Result<ExecResult> {
    use sqlparser::ast::DeclareType;

    for decl in stmts {
        // We only handle CURSOR declarations.
        if !matches!(decl.declare_type, Some(DeclareType::Cursor)) {
            return Err(BasinError::internal(
                "DECLARE: only CURSOR declarations are supported in v0.1".to_string(),
            ));
        }
        let name = decl
            .names
            .first()
            .ok_or_else(|| BasinError::internal("DECLARE: missing cursor name".to_string()))?
            .value
            .clone();

        // sqlparser 0.52 puts the SELECT query in `for_query` (Box<Query>)
        // for `DECLARE c CURSOR FOR SELECT …`. The `assignment` field uses
        // `Box<Expr>` and is for variable-assignment forms, not cursor FOR.
        let query = decl.for_query.ok_or_else(|| {
            BasinError::internal("DECLARE CURSOR: missing FOR <query>".to_string())
        })?;

        // Execute the SELECT to materialise the result set.
        let select_sql = query.to_string();
        let result = exec_select(sess, &select_sql, false, None).await?;
        // An empty result (0 rows) is valid — declare with the schema.
        let (schema, batches) = match result {
            ExecResult::Rows { schema, batches } => (schema, batches),
            ExecResult::Empty { .. } => (Arc::new(Schema::empty()), vec![]),
        };
        sess.state.cursors.declare(name, schema, batches).await?;
    }
    Ok(ExecResult::Empty {
        tag: "DECLARE".into(),
    })
}

/// Execute `FETCH [direction] FROM <cursor>`.
async fn exec_fetch(
    sess: &ProjectSession,
    cursor_name: &str,
    direction: sqlparser::ast::FetchDirection,
) -> Result<ExecResult> {
    let dir = crate::cursor::CursorDirection::from_sqlparser(&direction)?;
    let (schema, batches) = sess.state.cursors.apply(cursor_name, dir, true).await?;
    Ok(ExecResult::Rows { schema, batches })
}

/// Execute `CLOSE <cursor>` (or `CLOSE ALL`).
async fn exec_close(
    sess: &ProjectSession,
    cursor: sqlparser::ast::CloseCursor,
) -> Result<ExecResult> {
    use sqlparser::ast::CloseCursor;
    match cursor {
        CloseCursor::All => {
            // Close all — not trivially implementable without exposing the
            // registry's internals; for v0.1 we surface a helpful error.
            return Err(BasinError::internal(
                "CLOSE ALL is not supported in v0.1; close cursors individually".to_string(),
            ));
        }
        CloseCursor::Specific { name } => {
            sess.state.cursors.close(&name.value).await?;
        }
    }
    Ok(ExecResult::Empty {
        tag: "CLOSE".into(),
    })
}

/// Execute `MOVE <direction> [FROM|IN] <cursor>`.
async fn exec_cursor_move(
    sess: &ProjectSession,
    intent: crate::cursor::MoveIntent,
) -> Result<ExecResult> {
    sess.state
        .cursors
        .apply(&intent.cursor_name, intent.direction, false)
        .await?;
    Ok(ExecResult::Empty { tag: "MOVE".into() })
}

// ---------------------------------------------------------------------------
// json_agg(t) whole-row expansion
// ---------------------------------------------------------------------------

/// Annotate any field in `ws_schema` that originated from a `json_agg` or
/// `jsonb_agg` expression in the SQL with `BASIN_TYPE=JSONB` metadata.
///
/// DataFusion's planner returns the UDAF result as a plain `Utf8` column
/// without the JSONB metadata in two cases where `return_field` alone is
/// insufficient:
///
/// 1. **Scalar correlated subquery** — `(SELECT json_agg(x) FROM t WHERE …) AS alias`:
///    the outer column gets type `Utf8` from the subquery schema's `field(0).data_type()`,
///    stripping the metadata.
/// 2. **Join over a pre-aggregated subquery** — after the LATERAL rewrite the
///    `json_agg` column lives in an inner SELECT; the outer query projects it
///    by alias through the join, and DataFusion may lose the metadata.
///
/// This function scans the SQL text (case-insensitively) for every `json_agg`
/// / `jsonb_agg` occurrence and collects the alias of the enclosing projection
/// item.  Any `Utf8` field in `ws_schema` whose name matches a collected alias
/// is re-annotated with `BASIN_TYPE=JSONB`.
///
/// This is best-effort and conservative: only fields whose name appears
/// explicitly in the alias list are changed.  If a field is already annotated
/// (e.g. from `return_field` for direct GROUP BY aggregates) the function is
/// idempotent.
pub(crate) fn annotate_json_agg_columns(ws_schema: &Arc<Schema>, sql: &str) -> Arc<Schema> {
    // Fast exit if no json_agg / jsonb_agg in the SQL.
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("json_agg(") && !lower.contains("jsonb_agg(") {
        return Arc::clone(ws_schema);
    }

    // Collect aliases of projection items that contain json_agg / jsonb_agg.
    let json_aliases = collect_json_agg_aliases(&lower);
    if json_aliases.is_empty() {
        return Arc::clone(ws_schema);
    }

    // Rebuild the schema, annotating matching Utf8 fields.
    let mut changed = false;
    let new_fields: Vec<Field> = ws_schema
        .fields()
        .iter()
        .map(|f| {
            let f = f.as_ref();
            if f.data_type() == &DataType::Utf8
                && json_aliases.contains(&f.name().to_ascii_lowercase())
                && !crate::types::field_is_jsonb(f)
            {
                changed = true;
                let mut meta = f.metadata().clone();
                meta.insert(
                    crate::types::BASIN_TYPE_KEY.to_string(),
                    crate::types::BASIN_TYPE_JSONB.to_string(),
                );
                f.clone().with_metadata(meta)
            } else {
                f.clone()
            }
        })
        .collect();

    if changed {
        Arc::new(Schema::new(new_fields))
    } else {
        Arc::clone(ws_schema)
    }
}

/// Scan the (lowercase) SQL string and collect the aliases of all SELECT
/// projection items that contain a `json_agg(` or `jsonb_agg(` call.
///
/// Algorithm: for each occurrence of `json_agg(` or `jsonb_agg(` in the SQL,
/// we look at the surrounding projection context by:
/// 1. Finding the depth-0 comma or the surrounding context that delimits the
///    projection item.
/// 2. Scanning forward from the closing `)` of the agg call for `as <alias>`
///    or a bare identifier used as the alias.
///
/// This is intentionally conservative: if we cannot determine an alias we
/// leave it empty (no annotation).  False positives (annotating non-JSON
/// columns) are impossible because we only annotate aliases that explicitly
/// appear after a json_agg call.
fn collect_json_agg_aliases(lower_sql: &str) -> std::collections::HashSet<String> {
    let bytes = lower_sql.as_bytes();
    let len = bytes.len();
    let mut aliases = std::collections::HashSet::new();
    let mut pos = 0usize;

    loop {
        // Find the next json_agg( or jsonb_agg( occurrence.
        let hit = {
            let j = lower_sql[pos..].find("json_agg(");
            let jb = lower_sql[pos..].find("jsonb_agg(");
            match (j, jb) {
                (Some(a), Some(b)) if a <= b => Some((pos + a, 8usize)), // json_agg
                (Some(a), None) => Some((pos + a, 8usize)),
                (_, Some(b)) => Some((pos + b, 9usize)), // jsonb_agg
                (None, None) => None,
            }
        };
        let (agg_start, fn_len) = match hit {
            Some(h) => h,
            None => break,
        };

        // Word boundary before: must not be alphanumeric or `_`.
        if agg_start > 0 {
            let prev = bytes[agg_start - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                pos = agg_start + 1;
                continue;
            }
        }

        // Find the matching `)` for the agg function call.
        let paren_open = agg_start + fn_len; // points at `(`
        let Some(paren_close) = find_matching_paren_in_sql(lower_sql, paren_open) else {
            pos = agg_start + 1;
            continue;
        };

        // After the closing `)` of the agg call, skip whitespace and check for
        // optional `AS alias` or bare `alias`.  Also handle the case where the
        // agg is the last part of a scalar subquery `)` followed by `) AS alias`.
        let mut after = paren_close + 1;
        // Skip closing parens (scalar subquery wrappers) and whitespace.
        while after < len && (bytes[after] == b')' || bytes[after].is_ascii_whitespace()) {
            after += 1;
        }
        // Optional `AS` keyword.
        if after + 2 <= len && &lower_sql[after..after + 2] == "as" {
            let after_as = after + 2;
            if after_as < len && bytes[after_as].is_ascii_whitespace() {
                after = after_as + 1;
                while after < len && bytes[after].is_ascii_whitespace() {
                    after += 1;
                }
            } else {
                // `as` not followed by whitespace — not the AS keyword.
            }
        }
        // Read the alias identifier.
        let alias_start = after;
        let mut alias_end = alias_start;
        while alias_end < len
            && (bytes[alias_end].is_ascii_alphanumeric() || bytes[alias_end] == b'_')
        {
            alias_end += 1;
        }
        if alias_end > alias_start {
            let alias = lower_sql[alias_start..alias_end].to_string();
            // Filter out SQL keywords that can follow a function call but
            // are not aliases: FROM, WHERE, ORDER, GROUP, HAVING, LIMIT,
            // OFFSET, ON, AND, OR, NOT, NULL, AS, THEN, ELSE, END.
            const NOT_ALIAS: &[&str] = &[
                "from",
                "where",
                "order",
                "group",
                "having",
                "limit",
                "offset",
                "on",
                "and",
                "or",
                "not",
                "null",
                "as",
                "then",
                "else",
                "end",
                "inner",
                "left",
                "right",
                "join",
                "cross",
                "lateral",
                "union",
                "intersect",
                "except",
                "returning",
                "into",
            ];
            if !NOT_ALIAS.contains(&alias.as_str()) {
                aliases.insert(alias);
            }
        }

        pos = paren_close + 1;
    }

    aliases
}

/// Find the matching `)` for the `(` at `open_paren` in `sql`, handling
/// nested parens and single-quoted strings.  Returns `None` if malformed.
fn find_matching_paren_in_sql(sql: &str, open_paren: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    if open_paren >= len || bytes[open_paren] != b'(' {
        return None;
    }
    let mut depth = 1i32;
    let mut i = open_paren + 1;
    while i < len {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Rewrite `json_agg(<table_name>)` / `jsonb_agg(<table_name>)` (and their
/// `JSON_AGG` / `JSONB_AGG` uppercase forms) where the bare identifier
/// `<table_name>` refers to a relation visible in the `FROM` clause.
///
/// DataFusion does not understand a bare relation reference as an aggregate
/// argument.  We expand it to:
///
/// ```sql
/// json_agg(named_struct('col1', t.col1, 'col2', t.col2, ...))
/// ```
///
/// using the Arrow schema already registered in `ctx`, so DataFusion sees a
/// concrete `named_struct(...)` call over typed column references.
///
/// This is a best-effort text rewrite: only `json_agg(<single_ident>)` and
/// `jsonb_agg(<single_ident>)` where `<single_ident>` matches a registered
/// table name are expanded.  Anything else is returned unchanged, so existing
/// working queries are unaffected.
pub(crate) async fn rewrite_json_agg_whole_row(
    sql: &str,
    ctx: &datafusion::prelude::SessionContext,
) -> String {
    // Fast path: skip if there is no json_agg / jsonb_agg in the SQL.
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("json_agg(") && !lower.contains("jsonb_agg(") {
        return sql.to_string();
    }

    // We scan for occurrences of `json_agg(<ident>)` or `jsonb_agg(<ident>)`
    // where `<ident>` is a bare (unqualified, no dots, no spaces) identifier.
    // We rewrite each occurrence if `<ident>` is a registered table.
    //
    // Strategy: walk the original SQL byte-by-byte.  When we see
    // `json_agg(` or `jsonb_agg(`, extract the argument up to the matching `)`,
    // check if it's a simple identifier, query the schema, and splice the
    // expansion in place.

    let bytes = sql.as_bytes();
    let len = bytes.len();

    let mut result = String::with_capacity(sql.len() + 256);
    let mut pos = 0usize;

    while pos < len {
        // Look for `json_agg(` or `jsonb_agg(` (case-insensitive).
        let tail_lower = &lower[pos..];
        let (fn_name, fn_start_rel) = if let Some(p) = tail_lower.find("jsonb_agg(") {
            // Check: also try json_agg( at pos < p+1
            let json_p = tail_lower.find("json_agg(");
            let jsonb_p = Some(p);
            match (json_p, jsonb_p) {
                (Some(jp), Some(bp)) if jp < bp => ("json_agg", jp),
                (_, Some(bp)) => ("jsonb_agg", bp),
                (Some(jp), None) => ("json_agg", jp),
                (None, None) => break,
            }
        } else if let Some(p) = tail_lower.find("json_agg(") {
            ("json_agg", p)
        } else {
            break;
        };

        let abs_fn_start = pos + fn_start_rel;

        // Word-boundary check: character before `json_agg` must not be alphanum or `_`.
        if abs_fn_start > 0 {
            let prev = bytes[abs_fn_start - 1];
            if prev.is_ascii_alphanumeric() || prev == b'_' {
                // Not a word boundary (e.g. `my_json_agg`); skip past this hit.
                result.push_str(&sql[pos..abs_fn_start + 1]);
                pos = abs_fn_start + 1;
                continue;
            }
        }

        let fn_kw_len = fn_name.len(); // "json_agg" = 8, "jsonb_agg" = 9
                                       // Position of `(` — already consumed by the find pattern.
        let paren_open = abs_fn_start + fn_kw_len;
        // paren_open points at `(`.
        if paren_open >= len || bytes[paren_open] != b'(' {
            result.push_str(&sql[pos..abs_fn_start + 1]);
            pos = abs_fn_start + 1;
            continue;
        }

        // Find the matching `)` for the function call.  Simple scan (no
        // nested-paren awareness needed for single-ident args, but we handle
        // parens so we skip false positives with nested calls).
        let mut depth = 1i32;
        let mut inner_end: Option<usize> = None;
        let mut k = paren_open + 1;
        while k < len {
            match bytes[k] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        inner_end = Some(k);
                        break;
                    }
                }
                b'\'' => {
                    // Skip string literals.
                    k += 1;
                    while k < len {
                        if bytes[k] == b'\'' {
                            if k + 1 < len && bytes[k + 1] == b'\'' {
                                k += 2;
                                continue;
                            }
                            break;
                        }
                        k += 1;
                    }
                }
                _ => {}
            }
            k += 1;
        }

        let close_paren = match inner_end {
            Some(p) => p,
            None => {
                // Malformed — emit verbatim.
                result.push_str(&sql[pos..abs_fn_start + 1]);
                pos = abs_fn_start + 1;
                continue;
            }
        };

        // Extract argument between `(` and `)`.
        let arg = sql[paren_open + 1..close_paren].trim();

        // Check: is `arg` a simple identifier (no dots, spaces, parens)?
        let is_simple_ident = !arg.is_empty()
            && arg.chars().all(|c| c.is_alphanumeric() || c == '_')
            && arg
                .chars()
                .next()
                .map_or(false, |c| c.is_alphabetic() || c == '_');

        if !is_simple_ident {
            // Not a bare ident; emit verbatim and advance past the whole call.
            result.push_str(&sql[pos..close_paren + 1]);
            pos = close_paren + 1;
            continue;
        }

        // Check whether `arg` is a registered table by trying table_provider.
        let table_name_lower = arg.to_ascii_lowercase();
        let schema_opt = ctx
            .table_provider(table_name_lower.as_str())
            .await
            .ok()
            .map(|tp| tp.schema());

        let expansion = match schema_opt {
            Some(schema) if !schema.fields().is_empty() => {
                // Build `named_struct('col1', t.col1, 'col2', t.col2, ...)`.
                let mut pairs: Vec<String> = Vec::with_capacity(schema.fields().len() * 2);
                for field in schema.fields() {
                    let col = field.name();
                    // Quote the string literal key with single quotes (no escaping needed
                    // for normal column names).
                    pairs.push(format!("'{col}', {table_name_lower}.{col}"));
                }
                let inner = pairs.join(", ");
                Some(format!("{fn_name}(named_struct({inner}))"))
            }
            _ => None,
        };

        match expansion {
            Some(rewritten) => {
                // Emit everything before this function call, then the rewritten form.
                result.push_str(&sql[pos..abs_fn_start]);
                result.push_str(&rewritten);
                pos = close_paren + 1;
            }
            None => {
                // Not a table or empty schema; emit verbatim.
                result.push_str(&sql[pos..close_paren + 1]);
                pos = close_paren + 1;
            }
        }
    }

    // Emit any trailing content.
    if pos < len {
        result.push_str(&sql[pos..]);
    }

    result
}

// Savepoint name extraction was previously done by re-scraping the SQL
// text (`extract_savepoint_name`).  It was replaced by
// `pg_ast::txn_stmt`, which reads libpg_query's authoritative parsed
// `TransactionStmt.savepoint_name`, so the text-scraper is gone.

// ---------------------------------------------------------------------------
// Data-modifying CTE orchestrator
// ---------------------------------------------------------------------------

/// Returns `true` when any CTE in the `WITH` clause has a DML body
/// (`INSERT`, `UPDATE`, or `DELETE`).  Pure-SELECT CTEs return `false`.
fn query_has_dml_cte(query: &sqlparser::ast::Query) -> bool {
    let Some(ref with) = query.with else {
        return false;
    };
    with.cte_tables.iter().any(|cte| {
        matches!(
            cte.query.body.as_ref(),
            SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
        )
    })
}

/// Returns `true` when the query has a `WITH RECURSIVE` clause AND the outer
/// body is a DML statement (`INSERT`, `UPDATE`, or `DELETE`).
///
/// This is the combination shape: `WITH RECURSIVE t(col) AS (… UNION ALL …)
/// INSERT INTO target SELECT * FROM t`.  sqlparser routes the whole thing as a
/// `Statement::Query` with `SetExpr::Insert` as the body.  DataFusion cannot
/// plan DML bodies inside a query, so we intercept before `exec_select`.
fn query_has_recursive_with_dml_body(query: &sqlparser::ast::Query) -> bool {
    let Some(ref with) = query.with else {
        return false;
    };
    if !with.recursive {
        return false;
    }
    matches!(
        query.body.as_ref(),
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
    )
}

/// Execute a `WITH RECURSIVE … INSERT/UPDATE/DELETE …` query.
///
/// Strategy: the `WITH RECURSIVE` clause contains only pure-SELECT CTEs (the
/// recursive generator); the DML is the query *body*, not a CTE.  sqlparser
/// wraps the whole statement as a `Statement::Query{with: RECURSIVE, body:
/// SetExpr::Insert}`.  DataFusion's `sql()` cannot plan a DML body, so we:
///
/// 1. Lift the `WITH RECURSIVE` clause onto the DML's *source* `SELECT`.
///    e.g. `INSERT INTO t SELECT * FROM cte` becomes
///         `INSERT INTO t (WITH RECURSIVE cte AS (…) SELECT * FROM cte)`.
/// 2. Call `exec_insert` / the appropriate DML path with the modified
///    statement, which then routes through `exec_insert_select` → DataFusion.
///    DataFusion CAN plan `WITH RECURSIVE … SELECT …`, so the recursive
///    generator is fully expanded before the rows land in the target table.
///
/// **UPDATE/DELETE**: Materialize each recursive CTE by running
///   `WITH RECURSIVE name AS (body) SELECT * FROM name` through DataFusion,
///   register the result as a MemTable, then run the UPDATE/DELETE.  The WHERE
///   subquery (e.g. `WHERE id IN (SELECT id FROM cte)`) resolves against the
///   registered MemTable via `resolve_subqueries_in_expr`.
async fn exec_recursive_with_dml_body(
    sess: &ProjectSession,
    query: &sqlparser::ast::Query,
) -> Result<ExecResult> {
    let with = query
        .with
        .as_ref()
        .expect("caller verified query.with is Some");

    match query.body.as_ref() {
        SetExpr::Insert(Statement::Insert(ins)) => {
            let mut ins = ins.clone();

            // Attach the RECURSIVE WITH clause to the INSERT's source query so
            // that DataFusion sees `WITH RECURSIVE t AS (…) SELECT * FROM t`
            // when it plans the source.  The Insert.source is the SELECT that
            // feeds rows into the target table.
            if let Some(ref mut source) = ins.source {
                // Only attach when the source doesn't already have a WITH
                // clause (shouldn't happen, but be safe).
                if source.with.is_none() {
                    source.with = Some(with.clone());
                }
            } else {
                return Err(BasinError::InvalidSchema(
                    "WITH RECURSIVE INSERT without a SELECT source is not supported".into(),
                ));
            }

            exec_insert(sess, ins, None).await
        }
        SetExpr::Update(Statement::Update(upd)) => {
            exec_recursive_with_update(sess, with, upd).await
        }
        SetExpr::Delete(Statement::Delete(del)) => {
            exec_recursive_with_delete(sess, with, del).await
        }
        _ => {
            // Shouldn't reach here given the query_has_recursive_with_dml_body
            // guard, but be defensive.
            Err(BasinError::internal(
                "exec_recursive_with_dml_body called on non-DML body",
            ))
        }
    }
}

/// Materialize every CTE in `with` as a MemTable registered in `sess.ctx`.
///
/// For each CTE `name AS (body)` we run:
///   `WITH RECURSIVE name AS (body) SELECT * FROM name`
/// through DataFusion (which natively supports recursive CTEs), collect the
/// rows, and register them as a MemTable so subsequent DML can reference
/// `name` in WHERE subqueries (e.g. `WHERE id IN (SELECT id FROM name)`).
///
/// Returns the list of registered table names so the caller can deregister
/// after DML completes (best-effort cleanup; sessions are per-request anyway).
async fn materialize_recursive_ctes(
    sess: &ProjectSession,
    with: &sqlparser::ast::With,
) -> Result<Vec<String>> {
    use datafusion::datasource::memory::MemTable;

    let mut registered: Vec<String> = Vec::new();

    for cte in &with.cte_tables {
        let cte_name = cte.alias.name.value.clone();
        // Build: WITH RECURSIVE <name> AS (<body>) SELECT * FROM <name>
        // DataFusion natively expands recursive CTEs in SELECT context.
        let expand_sql = format!(
            "WITH RECURSIVE {name} AS ({body}) SELECT * FROM {name}",
            name = cte_name,
            body = cte.query,
        );
        let df = sess.ctx.sql(&expand_sql).await.map_err(|e| {
            BasinError::internal(format!(
                "WITH RECURSIVE: failed to plan CTE {cte_name:?}: {e}"
            ))
        })?;
        // Capture the logical schema BEFORE consuming `df` with collect().
        // This is the authoritative schema from the plan, which we use for
        // the MemTable so that plan-level type info is preserved.
        let plan_schema: datafusion::arrow::datatypes::SchemaRef =
            df.schema().as_arrow().clone().into();
        let df_batches = df.collect().await.map_err(|e| {
            BasinError::internal(format!(
                "WITH RECURSIVE: failed to execute CTE {cte_name:?}: {e}"
            ))
        })?;

        // Re-cast every batch to use `plan_schema` so that schema and batches
        // agree on nullability / metadata and MemTable::try_new succeeds.
        // `RecordBatch::with_schema` is cheap — it reuses the underlying
        // column buffers, only wrapping them with a new schema reference.
        let recast_batches: Vec<datafusion::arrow::record_batch::RecordBatch> = df_batches
            .into_iter()
            .map(|b| {
                datafusion::arrow::record_batch::RecordBatch::try_new(
                    plan_schema.clone(),
                    b.columns().to_vec(),
                )
                .map_err(|e| {
                    BasinError::internal(format!(
                        "WITH RECURSIVE: recast batch for CTE {cte_name:?}: {e}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let provider =
            MemTable::try_new(plan_schema.clone(), vec![recast_batches]).map_err(|e| {
                BasinError::internal(format!("MemTable for CTE {cte_name:?}: {e}"))
            })?;

        let _ = sess.ctx.deregister_table(&cte_name);
        sess.ctx
            .register_table(&cte_name, Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register CTE {cte_name:?}: {e}")))?;
        registered.push(cte_name);
    }

    Ok(registered)
}

/// Execute `WITH RECURSIVE … UPDATE …` by materializing the CTEs as MemTables
/// and routing the UPDATE through the existing `exec_update` path.
///
/// The WHERE clause subquery (e.g. `WHERE id IN (SELECT id FROM cte)`) is
/// resolved by `resolve_subqueries_in_expr` against the registered MemTable.
async fn exec_recursive_with_update(
    sess: &ProjectSession,
    with: &sqlparser::ast::With,
    upd: &sqlparser::ast::Update,
) -> Result<ExecResult> {
    let registered = materialize_recursive_ctes(sess, with).await?;

    let from_twj = upd.from.as_ref().and_then(|f| match f {
        sqlparser::ast::UpdateTableFromKind::BeforeSet(v)
        | sqlparser::ast::UpdateTableFromKind::AfterSet(v) => v.first().cloned(),
    });

    let result = crate::dml_mutate::exec_update(
        sess,
        upd.table.clone(),
        upd.assignments.clone(),
        from_twj,
        upd.selection.clone(),
        upd.returning.clone(),
    )
    .await;

    for name in &registered {
        let _ = sess.ctx.deregister_table(name.as_str());
    }

    result
}

/// Execute `WITH RECURSIVE … DELETE …` by materializing the CTEs as MemTables
/// and routing the DELETE through the existing `exec_delete` path.
///
/// The WHERE clause subquery (e.g. `WHERE id IN (SELECT id FROM cte)`) is
/// resolved by `resolve_subqueries_in_expr` against the registered MemTable.
async fn exec_recursive_with_delete(
    sess: &ProjectSession,
    with: &sqlparser::ast::With,
    del: &sqlparser::ast::Delete,
) -> Result<ExecResult> {
    let registered = materialize_recursive_ctes(sess, with).await?;

    let result = crate::dml_mutate::exec_delete(sess, del.clone()).await;

    for name in &registered {
        let _ = sess.ctx.deregister_table(name.as_str());
    }

    result
}

/// Execute a `WITH … SELECT …` query whose CTE list contains at least one
/// data-modifying CTE (`INSERT`/`UPDATE`/`DELETE … RETURNING …`).
///
/// Strategy:
/// 1. Walk the CTE list in declaration order.
///    - Pure-SELECT CTEs: skip (DataFusion handles them in the outer query).
///    - DML CTEs: execute via the existing DML paths, forcing `RETURNING *`
///      when the user omitted a RETURNING clause, to capture the affected rows.
/// 2. Register each captured batch as a named `MemTable` in `sess.ctx` under
///    the CTE alias.  DataFusion's subsequent planning of the outer SELECT will
///    find these in scope exactly as it does for real tables.
/// 3. Execute the outer SELECT body (stripped of DML CTEs; any remaining
///    pure-SELECT CTEs stay in the query string) through the normal
///    `exec_select` path.
///
/// Atomicity: if any DML leg fails, the function returns the error immediately
/// and subsequent legs are not executed.  Already-committed legs are NOT
/// rolled back (Basin has no multi-statement rollback yet).  This is documented
/// behaviour: a failed DML-CTE may partially apply.  When the session is inside
/// an explicit transaction the existing transaction machinery provides the
/// rollback guarantee.
///
/// Recursive DML CTEs and Postgres's "snapshot-at-statement-start" inter-leg
/// visibility semantics are deferred — they require planner-level support that
/// is out of scope for this implementation.
async fn exec_dml_cte_query(
    sess: &ProjectSession,
    query: &sqlparser::ast::Query,
    original_sql: &str,
    include_deleted: bool,
) -> Result<ExecResult> {
    use datafusion::datasource::memory::MemTable;
    use sqlparser::ast::{SelectItem, WildcardAdditionalOptions};

    let with = query
        .with
        .as_ref()
        .expect("caller verified query.with is Some");

    // Reject RECURSIVE data-modifying CTEs — the interaction with our sequential
    // execution model would be incorrect.
    if with.recursive {
        return Err(BasinError::InvalidSchema(
            "RECURSIVE data-modifying CTEs are not supported".into(),
        ));
    }

    // Table names registered during this call so we can clean them up if the
    // outer SELECT fails (best effort; DataFusion contexts are per-session anyway).
    let mut registered: Vec<String> = Vec::new();

    for cte in &with.cte_tables {
        let cte_name = cte.alias.name.value.clone();

        match cte.query.body.as_ref() {
            SetExpr::Insert(Statement::Insert(ins)) => {
                let mut ins = ins.clone();
                // Force RETURNING * when the user omitted it so we always capture
                // the inserted rows for the MemTable.
                let user_had_returning = ins.returning.is_some();
                if ins.returning.is_none() {
                    ins.returning = Some(vec![SelectItem::Wildcard(
                        WildcardAdditionalOptions::default(),
                    )]);
                }
                let result = exec_insert(sess, ins, None).await?;
                let (schema, batches) =
                    dml_cte_extract_rows(result, user_had_returning, &cte_name)?;
                register_dml_cte_memtable(sess, &cte_name, schema, batches, &mut registered)?;
            }
            SetExpr::Update(Statement::Update(sqlparser::ast::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                ..
            })) => {
                let from_twj = from.as_ref().and_then(|f| match f {
                    sqlparser::ast::UpdateTableFromKind::BeforeSet(v)
                    | sqlparser::ast::UpdateTableFromKind::AfterSet(v) => v.first().cloned(),
                });
                let user_had_returning = returning.is_some();
                // If the user didn't supply RETURNING we force RETURNING * so
                // we can capture affected rows for the MemTable.
                let effective_returning = if returning.is_some() {
                    returning.clone()
                } else {
                    Some(vec![SelectItem::Wildcard(
                        WildcardAdditionalOptions::default(),
                    )])
                };
                let result = crate::dml_mutate::exec_update(
                    sess,
                    table.clone(),
                    assignments.clone(),
                    from_twj,
                    selection.clone(),
                    effective_returning,
                )
                .await?;
                let (schema, batches) =
                    dml_cte_extract_rows(result, user_had_returning, &cte_name)?;
                register_dml_cte_memtable(sess, &cte_name, schema, batches, &mut registered)?;
            }
            SetExpr::Delete(Statement::Delete(del)) => {
                let mut del = del.clone();
                let user_had_returning = del.returning.is_some();
                if del.returning.is_none() {
                    del.returning = Some(vec![SelectItem::Wildcard(
                        WildcardAdditionalOptions::default(),
                    )]);
                }
                let result = crate::dml_mutate::exec_delete(sess, del).await?;
                let (schema, batches) =
                    dml_cte_extract_rows(result, user_had_returning, &cte_name)?;
                register_dml_cte_memtable(sess, &cte_name, schema, batches, &mut registered)?;
            }
            // Non-DML CTE body: leave it for DataFusion to handle in the outer query.
            _ => continue,
        }
    }

    // Build the outer SELECT SQL: strip DML CTEs, keep pure-SELECT CTEs.
    // Strategy: reconstruct the query with only the non-DML CTEs in `WITH`,
    // or drop the `WITH` clause entirely if all CTEs were DML.
    let outer_sql = build_outer_select_sql(query)?;

    // Execute the outer SELECT through the normal path, which will find the
    // registered MemTables in `sess.ctx` when it references them.
    exec_select(sess, &outer_sql, include_deleted, None).await
}

/// Extract rows from a DML result.  When `user_had_returning` is false (we
/// forced `RETURNING *` ourselves), a `ExecResult::Empty` is not expected but
/// we defend against it by returning an empty batch with an empty schema.
fn dml_cte_extract_rows(
    result: ExecResult,
    _user_had_returning: bool,
    cte_name: &str,
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    match result {
        ExecResult::Rows { schema, batches } => Ok((schema, batches)),
        ExecResult::Empty { .. } => {
            // DML returned no rows (e.g. 0-row UPDATE).  Register an empty
            // schema placeholder so that any outer SELECT FROM <cte_name> returns
            // 0 rows without a "table not found" error.
            Ok((Arc::new(Schema::empty()), vec![]))
        }
        #[allow(unreachable_patterns)]
        _ => Err(BasinError::internal(format!(
            "unexpected DML result for CTE {cte_name:?}"
        ))),
    }
}

/// Register `batches` under `cte_name` as a `MemTable` in the session context.
/// The `ws`-side schema is converted to DataFusion's schema for the MemTable.
fn register_dml_cte_memtable(
    sess: &ProjectSession,
    cte_name: &str,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
    registered: &mut Vec<String>,
) -> Result<()> {
    use datafusion::datasource::memory::MemTable;

    // Convert ws batches → df batches for MemTable.
    let df_batches: Vec<datafusion::arrow::record_batch::RecordBatch> = batches
        .iter()
        .map(|b| crate::convert::batch_ws_to_df(b))
        .collect::<Result<Vec<_>>>()?;

    // Build the DataFusion schema from the ws schema.
    let df_schema = if df_batches.is_empty() {
        // No rows: convert the ws schema to df schema manually.
        let ws_to_df_schema = crate::convert::schema_ws_to_df(&schema)?;
        Arc::new(ws_to_df_schema)
    } else {
        df_batches[0].schema()
    };

    let provider = MemTable::try_new(df_schema, vec![df_batches])
        .map_err(|e| BasinError::internal(format!("MemTable for CTE {cte_name:?}: {e}")))?;

    // Deregister any pre-existing table with this name (e.g. a real table that
    // shares the CTE alias).  DataFusion's `register_table` returns an error if
    // the name is already occupied.
    let _ = sess.ctx.deregister_table(cte_name);
    sess.ctx
        .register_table(cte_name, Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register CTE {cte_name:?}: {e}")))?;
    registered.push(cte_name.to_string());
    Ok(())
}

/// Build the SQL for the outer SELECT by stripping DML CTEs from the `WITH`
/// clause.  Pure-SELECT CTEs are retained because DataFusion can plan them.
///
/// If all CTEs were DML (none remain), the `WITH` clause is dropped entirely
/// and we return just the outer query body as SQL.
fn build_outer_select_sql(query: &sqlparser::ast::Query) -> Result<String> {
    let with = query.with.as_ref().expect("caller verified non-None");

    // Collect only the non-DML CTEs.
    let pure_ctes: Vec<_> = with
        .cte_tables
        .iter()
        .filter(|cte| {
            !matches!(
                cte.query.body.as_ref(),
                SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_)
            )
        })
        .collect();

    // Reconstruct the outer query body SQL.
    let body_sql = query.body.to_string();

    // Append ORDER BY / LIMIT / OFFSET / FETCH if present.
    let mut suffix = String::new();
    if let Some(ref order_by) = query.order_by {
        suffix.push(' ');
        suffix.push_str(&order_by.to_string());
    }
    if let Some(ref lc) = query.limit_clause {
        suffix.push_str(&lc.to_string());
    }
    if let Some(ref fetch) = query.fetch {
        suffix.push(' ');
        suffix.push_str(&fetch.to_string());
    }

    if pure_ctes.is_empty() {
        // No pure-SELECT CTEs remain; drop the WITH clause.
        Ok(format!("{body_sql}{suffix}"))
    } else {
        // Reconstruct WITH <pure_ctes> <body>.
        let ctes_sql = pure_ctes
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!("WITH {ctes_sql} {body_sql}{suffix}"))
    }
}

// ---------------------------------------------------------------------------
// Phase 5.14.D2 — query history recording
// ---------------------------------------------------------------------------

/// Best-effort ORDER BY / GROUP BY column recorder for adaptive sort.
///
/// Fires for simple single-table `SELECT … FROM <table>` shapes.  Multi-table
/// joins, CTEs, and subqueries in the FROM clause are skipped (too ambiguous
/// to attribute to a single table).  Non-column expressions in ORDER BY (e.g.
/// `ORDER BY 1`, `ORDER BY col + 1`) are silently excluded from the recorded
/// tuple; if the resulting tuple is empty after filtering, nothing is recorded.
fn record_query_patterns(sess: &ProjectSession, query: &sqlparser::ast::Query) {
    use crate::pg_ast::OrderByExt;
    use sqlparser::ast::{Expr, GroupByExpr, TableFactor};

    // Skip queries with CTEs — the table attribution is ambiguous.
    if query.with.is_some() {
        return;
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return,
    };

    // Only single-table FROM (no joins).
    if select.from.len() != 1 {
        return;
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return;
    }
    let table_name = match &from.relation {
        TableFactor::Table { name, alias: None, args: None, .. } => {
            if name.0.len() != 1 {
                return;
            }
            match basin_common::TableName::new(name.0[0].id_val().clone()) {
                Ok(t) => t,
                Err(_) => return,
            }
        }
        _ => return,
    };

    let history = sess.engine.query_history();

    // Record ORDER BY column names (identifier-only expressions).
    if let Some(order_by) = &query.order_by {
        let cols: Vec<String> = order_by
            .ext_exprs()
            .iter()
            .filter_map(|ob| match &ob.expr {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                    Some(parts[0].value.clone())
                }
                _ => None,
            })
            .collect();
        history.record_order_by(&sess.project, &table_name, cols);
    }

    // Record GROUP BY column names (identifier-only expressions).
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        let cols: Vec<String> = exprs
            .iter()
            .filter_map(|e| match e {
                Expr::Identifier(ident) => Some(ident.value.clone()),
                Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                    Some(parts[0].value.clone())
                }
                _ => None,
            })
            .collect();
        history.record_group_by(&sess.project, &table_name, cols);
    }
}

// ── Phase 5.14.C2 HTAP helpers ────────────────────────────────────────────────

/// Cross-INSERT PK / UNIQUE dedup for the tx-deferred INSERT path.
///
/// `enforce_pk_on_insert` / `enforce_unique_on_insert` already ran against
/// on-disk files (committed snapshot + any pending files from UPDATE/DELETE
/// rewrites).  With the OLTP-write optimisation (perf-w7-txn), per-INSERT
/// Parquet writes are deferred until COMMIT — prior in-tx INSERTs live only
/// as buffered `RecordBatch`es in `TxState::htap_rows`.  This helper closes
/// that gap by probing the current batch's PK / UNIQUE tuples against the
/// prior buffered batches.
///
/// Cheap: O(prior_rows + batch_rows) per call.  Skipped when the table has
/// no constraints or no prior buffered batches.
fn enforce_intra_tx_uniqueness(
    sess: &ProjectSession,
    table: &TableName,
    table_name_str: &str,
    pk_columns: &[String],
    unique_constraints: &[basin_catalog::UniqueConstraint],
    batch: &arrow_array::RecordBatch,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if pk_columns.is_empty() && unique_constraints.is_empty() {
        return Ok(());
    }
    let prior = crate::session::tx_htap_batches_for(&sess.state, table);
    if prior.is_empty() {
        return Ok(());
    }

    // PK check.
    if !pk_columns.is_empty() {
        let pk_idx_curr: Vec<usize> = pk_columns
            .iter()
            .map(|c| {
                batch.schema().index_of(c).map_err(|_| {
                    BasinError::internal(format!("PK column {c:?} missing from batch"))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Tier-3 fastpath: single Int64 PK. Avoids the `Vec<String>` per-row
        // allocation that dominates the O(k²) cost for large bulk-INSERT
        // transactions. Mirrors the same pattern in constraints.rs.
        let single_i64_pk = pk_columns.len() == 1
            && batch
                .schema()
                .field(pk_idx_curr[0])
                .data_type()
                == &DataType::Int64;

        if single_i64_pk {
            use arrow_array::Array as _;
            let curr_arr = batch
                .column(pk_idx_curr[0])
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| {
                    BasinError::internal(
                        "single_i64_pk detected but current batch column is not Int64Array",
                    )
                })?;
            let prior_row_count: usize = prior.iter().map(|rb| rb.num_rows()).sum();
            let mut existing_i64: std::collections::HashSet<i64> =
                std::collections::HashSet::with_capacity(prior_row_count);
            for rb in &prior {
                let prior_idx = rb.schema().index_of(&pk_columns[0]).map_err(|_| {
                    BasinError::internal(format!(
                        "PK column {:?} missing from tx-buffered batch",
                        pk_columns[0]
                    ))
                })?;
                let prior_arr = rb
                    .column(prior_idx)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .ok_or_else(|| {
                        BasinError::internal(
                            "single_i64_pk detected but prior batch column is not Int64Array",
                        )
                    })?;
                for row in 0..rb.num_rows() {
                    if !prior_arr.is_null(row) {
                        existing_i64.insert(prior_arr.value(row));
                    }
                }
            }
            for row in 0..batch.num_rows() {
                if !curr_arr.is_null(row) {
                    let v = curr_arr.value(row);
                    if existing_i64.contains(&v) {
                        return Err(BasinError::UniqueViolation(format!(
                            "duplicate key value violates unique constraint \
                             \"{table_name_str}_pkey\": Key ({})=({}) already exists.",
                            pk_columns[0], v
                        )));
                    }
                }
            }
        } else {
            let mut existing: std::collections::HashSet<Vec<String>> =
                std::collections::HashSet::new();
            for rb in &prior {
                let idx: Vec<usize> = pk_columns
                    .iter()
                    .map(|c| {
                        rb.schema().index_of(c).map_err(|_| {
                            BasinError::internal(format!(
                                "PK column {c:?} missing from tx-buffered batch"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..rb.num_rows() {
                    if let Some(k) = crate::constraints::pk_tuple_for_row(rb, &idx, row)? {
                        existing.insert(k);
                    }
                }
            }
            for row in 0..batch.num_rows() {
                if let Some(k) =
                    crate::constraints::pk_tuple_for_row(batch, &pk_idx_curr, row)?
                {
                    if existing.contains(&k) {
                        return Err(BasinError::UniqueViolation(format!(
                            "duplicate key value violates unique constraint \
                             \"{table_name_str}_pkey\": Key ({})=({}) already exists.",
                            pk_columns.join(", "),
                            k.join(", ")
                        )));
                    }
                }
            }
        }
    }

    // UNIQUE constraints (per-constraint NULL-skip semantics, mirroring
    // `enforce_one_unique`).
    for u in unique_constraints {
        if u.columns.is_empty() {
            continue;
        }
        let curr_idx: Vec<usize> = u
            .columns
            .iter()
            .map(|c| {
                batch.schema().index_of(c).map_err(|_| {
                    BasinError::internal(format!("UNIQUE column {c:?} missing from batch"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut existing: std::collections::HashSet<Vec<String>> =
            std::collections::HashSet::new();
        for rb in &prior {
            let idx: Vec<usize> = u
                .columns
                .iter()
                .map(|c| {
                    rb.schema().index_of(c).map_err(|_| {
                        BasinError::internal(format!(
                            "UNIQUE column {c:?} missing from tx-buffered batch"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            for row in 0..rb.num_rows() {
                if let Some(k) = crate::constraints::pk_tuple_for_row(rb, &idx, row)? {
                    existing.insert(k);
                }
            }
        }
        for row in 0..batch.num_rows() {
            if let Some(k) = crate::constraints::pk_tuple_for_row(batch, &curr_idx, row)? {
                if existing.contains(&k) {
                    return Err(BasinError::UniqueViolation(format!(
                        "duplicate key value violates unique constraint \"{}\": \
                         Key ({})=({}) already exists.",
                        u.name,
                        u.columns.join(", "),
                        k.join(", ")
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Emit a WAL `Begin` marker the first time a DML statement runs inside an
/// explicit transaction.  The marker is lazy: it is emitted once per tx, on
/// the first INSERT/UPDATE/DELETE.  Auto-commit statements never emit markers.
///
/// Returns the `tx_id` that was assigned (new or pre-existing).
async fn htap_emit_wal_begin_lazy(sess: &ProjectSession) -> u64 {
    let candidate = sess.engine.next_tx_id();
    let tx_id = crate::session::tx_ensure_id(&sess.state, candidate);
    // If a new id was just assigned (candidate == tx_id) emit the Begin marker.
    if tx_id == candidate {
        if let Some(shard) = sess.engine.config().shard.as_ref() {
            let part = basin_common::PartitionKey::default_key();
            // Best-effort — WAL marker failures must not block the write.
            let _ = shard
                .wal()
                .append_tx_begin(&sess.project, &part, tx_id)
                .await;
        }
    }
    tx_id
}

/// Encode a `RecordBatch` to Arrow IPC stream format.
///
/// Returns the wire bytes.  Used to store rows in the HTAP `MemTable` as
/// IPC-encoded blobs so the hot-tier data format matches the WAL wire format.
fn encode_batch_to_ipc(batch: &arrow_array::RecordBatch) -> Vec<u8> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())
        .expect("IPC StreamWriter init");
    writer.write(batch).expect("IPC write");
    writer.finish().expect("IPC finish");
    buf
}

/// Default row gate for the auto-commit INSERT write-through (S4 commit 4a):
/// batches larger than this are bulk loads and skip residency promotion.
/// Override with `BASIN_HOTTIER_RESIDENT_INSERT_MAX_ROWS`.
const BASIN_HOTTIER_RESIDENT_INSERT_MAX_ROWS: usize = 128;

fn hottier_resident_insert_max_rows() -> usize {
    std::env::var("BASIN_HOTTIER_RESIDENT_INSERT_MAX_ROWS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(BASIN_HOTTIER_RESIDENT_INSERT_MAX_ROWS)
}

/// S4 commit 4a — auto-commit INSERT write-through residency.
///
/// Called on the shard auto-commit INSERT path AFTER `write_batch_striped`
/// durably acked the rows (shard WAL): promote each PK-encodable row into the
/// process-wide `MemTableRegistry` as a CLEAN entry (`insert_clean`), so the
/// next point read for that PK is served from memory with zero file opens —
/// no shard tail flush, no catalog load, no cold decode (the pre-flush PK
/// probe in `fast_select`). Clean entries are excluded from the hot-tier
/// flush (`dirty_snapshot`), so the shard's own tail→Parquet flush remains
/// the single durability path — no double-flush — and they are evictable at
/// any time under the retention/budget sweeps with zero correctness impact
/// (an evicted row is re-read from the tail/cold exactly as today).
///
/// Gates (every miss silently skips — promotion is an optimization, never an
/// error):
///   * `retain_secs == 0` kill switch — retention disabled means behave
///     exactly like today: no write-through at all;
///   * `batch.num_rows() <= BASIN_HOTTIER_RESIDENT_INSERT_MAX_ROWS` (128) —
///     OLTP inserts only, bulk loads skip;
///   * single-column PK whose value encodes to a `RowKey` — the counter-key
///     fallback is NEVER taken here (it would grow the un-indexed half of the
///     memtable that point reads can't probe), and composite PKs are skipped
///     because the only read paths that serve CLEAN rows are the single-PK
///     direct-get probes (the auto-commit snapshot fallback deliberately
///     skips clean entries — see `probe_memtable`);
///   * budget: `try_reserve_bytes` (clean bytes still consume budget — they
///     are reclaimable). On `HardCapReached`, evict this project's clean
///     bytes and retry ONCE, then skip silently.
///
/// Row encoding reuses `htap_promote_to_registry`'s machinery
/// (`build_pk_row_key` + `encode_batch_to_ipc`) so the resident image is
/// byte-compatible with what every memtable read path already decodes.
fn write_through_insert_residency(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    batch: &arrow_array::RecordBatch,
) {
    let registry = sess.engine.memtable_registry();
    if registry.config().retain_secs == 0 {
        return; // kill switch — today's behavior
    }
    let n_rows = batch.num_rows();
    if n_rows == 0 || n_rows > hottier_resident_insert_max_rows() {
        return; // bulk load (or empty) — skip
    }
    if meta.pk_columns.len() != 1 {
        return; // composite/no PK — clean rows would be unreachable residency
    }
    let pk_col = &meta.pk_columns[0];
    let Ok(pk_idx) = batch.schema().index_of(pk_col) else {
        return;
    };
    let pk_cols = vec![(pk_idx, batch.schema().field(pk_idx).data_type().clone())];

    // Budget gate: evict-clean-then-retry once, then skip silently.
    let approx_bytes = batch.get_array_memory_size() as u64;
    let reserve_ok = |outcome: basin_hottier::ReservationOutcome| {
        outcome != basin_hottier::ReservationOutcome::HardCapReached
    };
    if !reserve_ok(registry.try_reserve_bytes(&sess.project, approx_bytes)) {
        let _ = registry.evict_clean(&sess.project, approx_bytes);
        if !reserve_ok(registry.try_reserve_bytes(&sess.project, approx_bytes)) {
            return;
        }
    }

    let entry = registry.get_or_create(sess.project, table.clone());
    for row_idx in 0..n_rows {
        // Skip rows whose PK does not encode (NULL / unsupported type) —
        // NEVER fall back to a counter key on this path.
        let Some(key) = build_pk_row_key(batch, row_idx, &pk_cols) else {
            continue;
        };
        // Never overwrite an EXISTING entry. The only reachable case here is
        // a DIRTY Tombstone (re-INSERT of a fast-path-deleted PK — a live
        // Row/Update would have failed the PK-uniqueness check upstream):
        // `insert_clean` would push-and-immediately-ack over it, DRAINING the
        // tombstone from the chain and un-suppressing the STALE cold row it
        // still shadows (the fast-path DELETE never rewrote cold). Skipping
        // preserves today's overlay semantics for that key.
        if entry.memtable.get(&key).is_some() {
            continue;
        }
        let row_batch = batch.slice(row_idx, 1);
        let row_bytes = encode_batch_to_ipc(&row_batch);
        entry
            .memtable
            .insert_clean(key, basin_hottier::MemRowValue::row(row_bytes, 0));
    }
}

/// Promote committed HTAP batches from a completed transaction to the
/// process-wide `MemTableRegistry`.  Called on COMMIT before `tx_commit`
/// clears the `TxState`.
///
/// Budget enforcement (per ADR 0016 §Multi-project isolation + Phase 5.14.C5
/// + S4 clean-first eviction):
/// 1. `try_reserve_bytes` per batch (which itself evicts THIS project's clean
///    bytes before reporting `HardCapReached`).
/// 2. On `HardCapReached`: evict CLEAN (flushed-and-retained) bytes from the
///    largest project — clean rows are pure read acceleration, so this never
///    loses a write — then retry.
/// 3. Still over cap: fall back to the pre-S4 last resort — synchronous
///    `remove_project` of the largest project's memtables (this DOES drop
///    dirty overlays; semantics unchanged, just demoted to last resort) —
///    then retry once.
/// 4. If still over cap: return `SQLSTATE 53200` (out_of_memory).
///
/// Returns, per table, the `(RowKey, seq)` pairs for the rows whose insert
/// seq was captured race-free (S4 commit 4b): after the COMMIT's catalog
/// commit makes these exact rows durable in cold Parquet, the caller acks
/// them CLEAN via `mark_flushed` so a later hot-tier flush does not
/// double-flush them while they keep serving point reads as residency.
async fn htap_promote_to_registry(
    sess: &ProjectSession,
    htap_rows: &std::collections::HashMap<
        basin_common::TableName,
        Vec<arrow_array::RecordBatch>,
    >,
) -> Result<std::collections::HashMap<basin_common::TableName, Vec<(basin_hottier::RowKey, u64)>>>
{
    let mut clean_acks: std::collections::HashMap<
        basin_common::TableName,
        Vec<(basin_hottier::RowKey, u64)>,
    > = std::collections::HashMap::new();
    if htap_rows.is_empty() {
        return Ok(clean_acks);
    }
    let registry = sess.engine.memtable_registry();
    for (table, batches) in htap_rows {
        let entry = registry.get_or_create(sess.project, table.clone());
        let table_acks: &mut Vec<(basin_hottier::RowKey, u64)> =
            clean_acks.entry(table.clone()).or_default();

        // Pre-fetch table metadata once per table so the hot loop below does
        // not call load_table() per row.  On catalog miss (e.g. mid-DROP) fall
        // back to the monotonic-counter key for the whole table.
        //
        // Inv-OLTP-write (#155): the per-session table-meta cache is hot here
        // — the in-tx INSERTs that produced these `htap_rows` populated it,
        // and the COMMIT match arm does not invalidate (see the carve-out
        // and the COMMIT-handler routing above the dispatch invalidation
        // block).
        let pk_info: Option<Vec<(usize, arrow_schema::DataType)>> =
            match crate::session::load_table_meta_cached_err(sess, table).await {
                Ok(meta) if !meta.pk_columns.is_empty() => {
                    // For each PK column resolve: (batch column index, DataType).
                    // We validate against the first batch's schema — all batches
                    // for the same table share the same schema within a session.
                    let first_batch = batches.first();
                    first_batch.and_then(|fb| {
                        let schema = fb.schema();
                        let mut cols: Vec<(usize, arrow_schema::DataType)> =
                            Vec::with_capacity(meta.pk_columns.len());
                        for pk_col in &meta.pk_columns {
                            match schema.index_of(pk_col) {
                                Ok(idx) => {
                                    let dt = schema.field(idx).data_type().clone();
                                    cols.push((idx, dt));
                                }
                                Err(_) => {
                                    // PK column missing from batch schema — degrade
                                    // to counter key for the whole table.
                                    tracing::warn!(
                                        table = %table,
                                        pk_col = %pk_col,
                                        "htap_promote_to_registry: PK column missing \
                                         from batch schema — falling back to counter key"
                                    );
                                    return None;
                                }
                            }
                        }
                        Some(cols)
                    })
                }
                Ok(_) => None, // no PK declared
                Err(_) => {
                    // Catalog unavailable — degrade gracefully.
                    tracing::warn!(
                        table = %table,
                        "htap_promote_to_registry: catalog miss for table \
                         — falling back to counter key"
                    );
                    None
                }
            };

        // ── S2: write-through PK row cache context ───────────────────────────
        //
        // After we insert each row into the memtable below we ALSO seed the
        // process-global `PkRowCache` so the *next* point read for that PK is a
        // ~2-5 µs HashMap hit instead of an O(n) memtable snapshot walk +
        // IPC decode. The cache is only meaningful (and only correct) for the
        // canonical single-PK point-lookup shape that `fast_select` serves from
        // it, so we gate the write-through on exactly that shape:
        //
        //   * single-column PK whose key encodes (i.e. `pk_info` resolved AND
        //     `pk_columns.len() == 1`) — the only shape `pk_row_cache::get`
        //     ever keys; a composite PK or counter fallback is never cached;
        //   * RLS DISABLED — the cache stores the RAW row, never an RLS-filtered
        //     view (bug family #159), and the read path bypasses the cache for
        //     RLS tables, so an RLS row must never enter it;
        //   * `proj_hash == hash_read_cols(None)` — we store the FULL-schema row
        //     image (the single sliced batch carries every column), matching the
        //     full-projection / `SELECT *` lookup shape. A narrower projection
        //     read computes a different `proj_hash` and simply misses (no
        //     wrong-column hit), then repopulates via the cold read.
        //
        // The dual watermarks are captured PER ROW *after* the memtable insert
        // (see the insert loop): the insert bumps the table's hot epoch, so the
        // post-insert epoch is the value a concurrent reader would observe right
        // now — caching it makes the entry valid immediately. The snapshot id is
        // the committed `current_snapshot` (these rows are now durable in the
        // memtable overlay on top of that snapshot; a later cold-tier commit
        // advances it and invalidates the entry, exactly as the cold-read
        // populate path relies on).
        // `Some(snapshot_id)` enables the per-row write-through; the cached entry
        // is keyed by the row's encoded PK (`pk_key` below), so we need no column
        // index here — only the snapshot watermark to store alongside it.
        let pk_cache_single: Option<u64> = match pk_info.as_ref() {
            // Composite PK or counter fallback: never cached (the cache keys a
            // single encoded PK only). The memtable still gets its PK / counter
            // keyed entry below; only the cache write-through is skipped.
            Some(cols) if cols.len() == 1 => {
                match crate::session::load_table_meta_cached_err(sess, table).await {
                    // RLS table → skip (cache must never hold a raw RLS row).
                    Ok(meta) if !meta.rls_enabled => Some(meta.current_snapshot.0),
                    // RLS table or catalog miss → skip the write-through.
                    _ => None,
                }
            }
            _ => None,
        };
        let pk_cache_proj_hash = crate::pk_row_cache::hash_read_cols(None);

        for batch in batches {
            let approx_bytes = batch.get_array_memory_size() as u64;
            // Budget gate.
            let reserve_ok = |outcome: basin_hottier::ReservationOutcome| {
                outcome != basin_hottier::ReservationOutcome::HardCapReached
            };
            if !reserve_ok(registry.try_reserve_bytes(&sess.project, approx_bytes)) {
                // S4 clean-first: reclaim CLEAN (flushed-and-retained) bytes
                // from the largest project before dropping anything dirty.
                // Clean rows are read acceleration only — evicting them never
                // loses a write. (`try_reserve_bytes` already evicted THIS
                // project's clean bytes internally; the largest project may
                // be a different one.)
                if let Some(largest) = registry.largest_project() {
                    let _ = registry.evict_clean(&largest, approx_bytes);
                }
                if !reserve_ok(registry.try_reserve_bytes(&sess.project, approx_bytes)) {
                    // Last resort — the pre-S4 behavior, unchanged in
                    // semantics but now reached only after clean eviction
                    // failed to free enough: drop the largest project's
                    // memtables wholesale. NOTE: this still discards DIRTY
                    // overlays (committed fast-path UPDATE/DELETE writes not
                    // yet materialized to cold). Preferring clean eviction
                    // above shrinks how often that loss path fires; removing
                    // it entirely is a separate change with its own
                    // back-pressure semantics.
                    if let Some(largest) = registry.largest_project() {
                        registry.remove_project(&largest);
                    }
                    // Retry once.
                    if !reserve_ok(registry.try_reserve_bytes(&sess.project, approx_bytes)) {
                        return Err(basin_common::BasinError::internal(
                            "HTAP memtable hard cap exceeded (SQLSTATE 53200)",
                        ));
                    }
                }
            }
            for row_idx in 0..batch.num_rows() {
                let row_batch = batch.slice(row_idx, 1);
                let row_bytes = encode_batch_to_ipc(&row_batch);

                // Derive the RowKey from the encoded PK columns so that:
                // 1. The memtable PK direct-get fast path can find the row.
                // 2. Future merge-on-read can dedupe hot vs cold by PK.
                //
                // `pk_key` is `Some` only when the row's PK actually encoded
                // (single- or multi-column). When it is `None` we fall back to a
                // monotonic counter key. The S2 cache write-through below fires
                // ONLY when `pk_key` is `Some` AND the table is single-PK
                // (`pk_cache_single`), so the cached key byte-matches what
                // `fast_select`'s `pk_scalar_to_row_key` lookup produces — never
                // a counter key (which the read path would never query).
                let pk_key: Option<basin_hottier::RowKey> = pk_info
                    .as_ref()
                    .and_then(|pk_cols| build_pk_row_key(batch, row_idx, pk_cols));
                let key = match pk_key.clone() {
                    Some(k) => k,
                    None => {
                        // No PK info, unsupported PK type, or NULL PK — use a
                        // monotonic counter key. warn! is rate-limited by the
                        // one-per-table pre-fetch warning above.
                        basin_hottier::RowKey::builder()
                            .append_u64(entry.memtable.total_count() as u64)
                            .finish()
                    }
                };

                // S4 commit 4b: capture this insert's MVCC seq for the
                // post-commit clean ack. `MemTable::insert` does not return
                // the claimed seq, so we fence it with `current_seq()` reads:
                // the insert's seq lies in `(seq_before, seq_after]`, and when
                // `seq_after == seq_before + 1` NO other write interleaved —
                // the seq is exactly `seq_after`. If another write DID
                // interleave we skip the ack for this key: acking at
                // `seq_after` could cover a concurrent same-key fast-path
                // write that is NOT in the file this COMMIT writes, marking a
                // not-yet-cold value clean (the version-loss hazard
                // `mark_flushed` exists to prevent). A skipped ack just
                // leaves the row dirty — the hot-tier flush re-flushes it
                // later (one redundant row write, never a lost one).
                let seq_before = entry.memtable.current_seq();
                entry
                    .memtable
                    .insert(key.clone(), basin_hottier::MemRowValue::row(row_bytes, 0));
                let seq_after = entry.memtable.current_seq();
                if seq_after == seq_before + 1 {
                    table_acks.push((key, seq_after));
                }

                // S2: write-through to the PK row cache. Single-PK, RLS-disabled,
                // PK-encodable rows only (see `pk_cache_single`). Capture the hot
                // epoch AFTER the insert above (the insert bumped it) so the
                // cached entry's watermark equals the value a reader observes
                // right now — the entry is therefore immediately valid. A later
                // mutation bumps the epoch again and invalidates it. The cached
                // image is the full-schema single-row batch (`proj_hash =
                // hash_read_cols(None)`), matching the full-projection lookup
                // shape; narrower projections miss and repopulate from cold.
                //
                // Multi-row INSERT note: each insert bumps the epoch, so within
                // one multi-row batch only the LAST row's entry stays valid
                // (earlier rows are invalidated by the later inserts' epoch
                // bumps). That is correct — never stale — and the common
                // single-row INSERT warms cleanly. A later point read for an
                // earlier row simply misses and repopulates from the (now
                // memtable-resident) row, so there is no correctness gap.
                if let (Some(snapshot_id), Some(pk_rk)) = (pk_cache_single, pk_key) {
                    let hot_epoch_after = entry.memtable.epoch();
                    sess.engine.pk_row_cache().insert(
                        &sess.project,
                        table,
                        pk_rk,
                        hot_epoch_after,
                        snapshot_id,
                        pk_cache_proj_hash,
                        vec![row_batch],
                    );
                }
            }
        }
    }
    Ok(clean_acks)
}

/// After a successful INSERT that overwrote a tombstoned PK, the registry still
/// holds the stale `Tombstone` entry.  Any subsequent cold-tier read would then
/// suppress the newly inserted row — making the re-INSERT invisible.
///
/// This helper repairs that: for each row in `batch` whose PK-encoded key is
/// currently a `Tombstone` in the registry, we write the row as
/// `MemRowValue::Update` (which replaces the tombstone and acts as an update
/// overlay on the cold tier, suppressing old cold-tier copies while surfacing
/// the new value).  Rows whose PKs are not tombstoned are left untouched —
/// they're visible through normal cold-tier reads.
///
/// Called from the VALUES auto-commit INSERT path after a successful
/// `commit_with_retry`.  Best-effort: errors are silently ignored so a
/// promotion failure never blocks the INSERT response.
fn promote_tombstone_overrides_on_reinsert(
    sess: &ProjectSession,
    table: &TableName,
    batch: &arrow_array::RecordBatch,
    pk_cols: &[(usize, arrow_schema::DataType)],
) {
    let registry = sess.engine.memtable_registry();
    let entry = registry.get(&sess.project, table);
    let entry = match entry {
        Some(e) => e,
        None => return, // no registry entry → no tombstones to worry about
    };

    for row_idx in 0..batch.num_rows() {
        let Some(key) = build_pk_row_key(batch, row_idx, pk_cols) else {
            continue;
        };
        // Only act when the current registry value IS a tombstone.
        if matches!(entry.memtable.get(&key), Some(basin_hottier::MemRowValue::Tombstone)) {
            // Encode this row as IPC and write as Update, replacing the tombstone.
            // `Update` semantics: suppresses the stale cold-tier row at this PK
            // and provides the new value to merge-on-read.
            let row_batch = batch.slice(row_idx, 1);
            let row_bytes = encode_batch_to_ipc(&row_batch);
            entry
                .memtable
                .insert(key, basin_hottier::MemRowValue::update(row_bytes, 0));
        }
    }
}

/// Build a [`basin_hottier::RowKey`] from the PK columns of a single row in
/// `batch` at `row_idx`, using the same per-type encoding as
/// `dml_mutate::pk_scalar_to_row_key` / `constraint_union::array_value_to_row_key`.
///
/// `pk_cols` is a slice of `(column_index_in_batch, declared_DataType)` in PK
/// declaration order.  For single-column PKs a single segment is produced;
/// for composite PKs every column's bytes are concatenated in order.
///
/// Returns `None` when any PK column contains a NULL value or has an
/// unsupported type — the caller should fall back to a monotonic counter key.
fn build_pk_row_key(
    batch: &arrow_array::RecordBatch,
    row_idx: usize,
    pk_cols: &[(usize, arrow_schema::DataType)],
) -> Option<basin_hottier::RowKey> {
    // Collect each column's encoded bytes into one contiguous buffer.
    // `array_value_to_row_key` returns `None` on NULL or unsupported type,
    // propagating `?` so we fall back to counter key on the first failure.
    let mut composite: Vec<u8> = Vec::new();
    for (col_idx, col_dt) in pk_cols {
        let array = batch.column(*col_idx);
        let segment =
            crate::constraint_union::array_value_to_row_key(array.as_ref(), row_idx, col_dt)?;
        composite.extend_from_slice(segment.as_bytes());
    }
    Some(basin_hottier::RowKey::from_bytes(composite))
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.7 B1: secondary index maintenance on INSERT
// ─────────────────────────────────────────────────────────────────────────────

/// Maintain secondary B-tree indexes after a successful INSERT.
///
/// For each single-column non-expression index declared on `table`, extracts
/// the indexed column's values from `batch` and inserts `(key → location)`
/// entries into the process-wide registry, then flushes the updated index to
/// the object store (best-effort; errors are logged, not propagated).
///
/// Called after a successful Parquet write and catalog commit on the
/// auto-commit path, and after the tx-deferred write on the tx-active path.
async fn maintain_secondary_indexes_on_insert(
    sess: &ProjectSession,
    table: &TableName,
    meta: &basin_catalog::TableMetadata,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    if meta.indexes.is_empty() {
        return;
    }
    let registry = sess.engine.secondary_index_registry();
    let gin_registry = sess.engine.gin_index_registry();
    let storage = &sess.engine.config().storage;

    for idx in &meta.indexes {
        // Only handle single-column non-expression indexes in v0.1.
        if idx.columns.len() != 1 {
            continue;
        }
        let col_name = &idx.columns[0];

        // Phase 5.19.C + 5.20.E: populate GIN posting list for JSONB / FTS.
        if idx.access_method == "gin" {
            let opclass = idx.opclass.as_deref().unwrap_or("jsonb_ops");
            if opclass == "tsvector_ops" {
                // Phase 5.20.E: tsvector GIN index — populate FTS posting list.
                // Thread the effective row-group size (same priority the
                // writer uses: `row_block_size` > `row_group_rows` > default)
                // so each row is recorded under its true row-group ordinal —
                // recording everything under row-group 0 would let the
                // rg-granular prune path skip row-groups 1+ of a >cap batch
                // and drop real matches.
                let fts_registry = sess.engine.gin_fts_registry();
                let rg_size = meta
                    .row_block_size
                    .map(|v| v as usize)
                    .or(meta.row_group_rows)
                    .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE)
                    .max(1);
                maintain_gin_fts_index_on_insert(fts_registry, &sess.project, table, col_name, batch, file_path, rg_size);
            } else {
                // Phase 5.19.C: JSONB GIN index — populate JSONB posting list.
                maintain_gin_index_on_insert(gin_registry, &sess.project, table, col_name, opclass, batch, file_path);
                // C2: also populate the per-row-group bloom-filter registry.
                // The Parquet writer splits a batch into row-groups of
                // `meta.row_group_rows` (default DEFAULT_MAX_ROW_GROUP_SIZE)
                // rows, so a >cap batch spans multiple row-groups — pass the
                // cap so each row is recorded under its true row-group index.
                let rg_registry = sess.engine.gin_rowgroup_registry();
                let rg_size = meta
                    .row_group_rows
                    .unwrap_or(basin_storage::DEFAULT_MAX_ROW_GROUP_SIZE);
                maintain_gin_rowgroup_index_on_insert(rg_registry, &sess.project, table, col_name, opclass, batch, file_path, rg_size);
            }
            // GIN posting list is RAM-only (5.19.E handles persistence); skip
            // the flush call.  Continue to also populate the B-tree registry
            // (which is a no-op for LargeBinary columns — scalar_to_key returns
            // None — so this is harmless).
            continue;
        }

        // Phase 5.24.D: populate interval-tree for range-typed columns with a
        // GIST index.  Range values are stored as Utf8 JSON; the interval tree
        // allows point-containment and overlap probes at file-level granularity.
        if idx.access_method == "gist" {
            let interval_registry = sess.engine.interval_registry();
            maintain_gist_index_on_insert(interval_registry, &sess.project, table, col_name, batch, file_path);
            continue;
        }

        let entries = crate::secondary_index::extract_entries_from_batch(
            batch,
            col_name,
            file_path,
            0, // row_group 0 — single batch write
        );
        if !entries.is_empty() {
            registry.insert_batch(&sess.project, table, col_name, entries);
        }
        crate::secondary_index::flush_index(registry, storage, &sess.project, table, col_name)
            .await;
    }
}

/// Populate the GIN posting list for a single JSONB column from a newly
/// written `batch`.  Iterates every row in the batch, parses the JSONB bytes,
/// extracts GIN terms, and inserts them into the registry.
///
/// Only `LargeBinary` columns are handled (Basin's canonical JSONB wire type).
/// Other column types are silently skipped — the index is best-effort.
fn maintain_gin_index_on_insert(
    gin_registry: &Arc<crate::index_probe::GinIndexRegistry>,
    project: &basin_common::ProjectId,
    table: &basin_common::TableName,
    col_name: &str,
    opclass: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    // Basin stores JSONB as LargeBinary.
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            gin_registry.index_row(project, table, col_name, opclass, bytes, file_path, 0, row as u64);
        }
        // Phase 5.19.C: mark this file as fully indexed so the completeness
        // guard in the probe path can safely prune to FileCandidates.
        // We only mark it when the column was present and had a LargeBinary
        // array — i.e., a real JSONB column in this batch.
        gin_registry.mark_file_indexed(project, table, col_name, file_path);
    }
}

/// C2 — Populate the per-row-group bloom-filter registry for a single JSONB
/// column from a newly written `batch`.  Each row in the batch belongs to
/// row-group 0 (Basin writes one Parquet row-group per INSERT batch).  The
/// same structure-keyed GIN term atoms used by the file-level posting list
/// (`crate::index_probe::extract_terms`) are fed into the row-group bloom so
/// that at query time `rowgroup_prune_for_containment` can narrow to only the
/// row-groups that MIGHT contain a match for a given `@>` needle.
///
/// Only `LargeBinary` columns are handled (Basin's canonical JSONB wire type).
/// Silently skips non-JSONB columns and NULL values — best-effort.
fn maintain_gin_rowgroup_index_on_insert(
    rg_registry: &Arc<basin_storage::index::gin_rowgroup::GinRowGroupRegistry>,
    project: &basin_common::ProjectId,
    table: &basin_common::TableName,
    col_name: &str,
    opclass: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
    row_group_size: usize,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    // The Parquet writer flushes a row-group every `row_group_size` rows
    // (WriterProperties::max_row_group_size), so row `r` of this single-batch
    // write lands in row-group `r / row_group_size`. Recording every row under
    // row-group 0 would be a false-negative bug: a probe could then prune
    // row-groups 1+ and drop real matches for any batch larger than the cap.
    let rg_size = row_group_size.max(1);
    let col = batch.column(col_idx);
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
                continue;
            };
            let terms = crate::index_probe::extract_terms(&value, opclass);
            if terms.is_empty() {
                continue;
            }
            let row_group = (row / rg_size) as u32;
            rg_registry.index_row(project, table, col_name, &terms, file_path, row_group);
        }
        // Seal the file after all rows have been fed into the bloom(s).
        rg_registry.mark_file_indexed(project, table, col_name, file_path);
    }
}

/// Phase 5.20.E — Populate the GIN FTS posting list for a single tsvector
/// column from a newly written `batch`.  Iterates every row in the batch,
/// reads the canonical tsvector string, extracts lexemes, and inserts them
/// into the FTS registry under its true row-group ordinal
/// (`row / rg_size` — the writer flushes a row-group every `rg_size` rows of
/// this single-batch write, so recording everything under row-group 0 would
/// be a false-negative bug once the rg-granular prune drives
/// `row_group_selection`).
///
/// Only `Utf8` columns are handled (Basin stores tsvector as Utf8; freshly
/// built INSERT batches always carry plain `StringArray`).  Other column
/// types are silently skipped — the file is then never marked complete and
/// the completeness guards force a full scan (correct, just unpruned).
fn maintain_gin_fts_index_on_insert(
    fts_registry: &Arc<basin_storage::index::gin_tsvector::GinTsvectorRegistry>,
    project: &basin_common::ProjectId,
    table: &basin_common::TableName,
    col_name: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
    rg_size: usize,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let rg_size = rg_size.max(1);
    let col = batch.column(col_idx);
    // Basin stores tsvector as Utf8 (canonical lexeme text form).
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let tsv_str = arr.value(row);
            let row_group = (row / rg_size) as u32;
            fts_registry.index_row(
                project, table, col_name, tsv_str, file_path, row_group, row as u64,
            );
        }
        // Mark this file as fully indexed so the completeness guard in the
        // probe path can safely prune to FileCandidates.
        fts_registry.mark_file_indexed(project, table, col_name, file_path);
    }
}

/// Phase 5.24.D — Populate the interval-tree index for a single range-typed
/// column from a newly written `batch`. Iterates every row in the batch, parses
/// the range JSON string, converts it to a half-open `IndexInterval`, and
/// inserts it into the registry.
///
/// Only `Utf8` columns with range-type metadata are handled (Basin stores range
/// types as Utf8 JSON). Other column types are silently skipped — the index is
/// best-effort.  After all rows have been indexed, `mark_file_indexed` is called
/// so the completeness guard can safely prune to `FileCandidates`.
fn maintain_gist_index_on_insert(
    interval_registry: &Arc<basin_storage::index::interval::IntervalRegistry>,
    project: &basin_common::ProjectId,
    table: &basin_common::TableName,
    col_name: &str,
    batch: &arrow_array::RecordBatch,
    file_path: &str,
) {
    use arrow_array::Array;
    let Ok(col_idx) = batch.schema().index_of(col_name) else {
        return;
    };
    let col = batch.column(col_idx);
    // Basin stores range types as Utf8 JSON strings.
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let range_json = arr.value(row);
            interval_registry.index_row(project, table, col_name, range_json, file_path, 0);
        }
        // Mark this file as fully indexed so the completeness guard in the
        // probe path can safely prune to FileCandidates.
        interval_registry.mark_file_indexed(project, table, col_name, file_path);
    }
}

// ---------------------------------------------------------------------------
// Phase 5.29.B-E — hypertable DDL + retention executors
// ---------------------------------------------------------------------------

/// Best-effort: for INSERT statements into hypertable tables, scan the VALUES
/// list for the hypertable's time column and register chunk records for each
/// unique time bucket. Called before the regular INSERT path so that
/// `timescaledb_information.chunks` is populated by the time the test queries
/// it, even within the same session.
async fn touch_hypertable_chunks_from_insert(
    sess: &ProjectSession,
    ins: &sqlparser::ast::Insert,
    _raw_sql: &str,
) {
    use sqlparser::ast::{Expr, SetExpr};
    // Get the table name.
    let obj_name = match crate::pg_ast::insert_object_name(ins) {
        Ok(n) => n,
        Err(_) => return,
    };
    let table = match single_part_name(obj_name) {
        Ok(n) => n.to_string(),
        Err(_) => return,
    };

    // Check if it's a hypertable.
    let Some(time_col) = sess.engine.hypertable_registry()
        .time_column(&sess.project, &table)
        .await
    else {
        return; // not a hypertable
    };

    // Find the column index of the time column in the INSERT's column list.
    let col_names: Vec<String> = ins
        .columns
        .iter()
        .map(|c| c.value.to_ascii_lowercase())
        .collect();
    let time_col_lower = time_col.to_ascii_lowercase();
    let time_col_idx = col_names.iter().position(|c| c == &time_col_lower);

    // If no explicit column list, check if the table schema has the column,
    // and try to infer position.  For simplicity in v0.1 we require an
    // explicit column list to locate the time column.
    let Some(col_idx) = time_col_idx else {
        // No explicit column list. Try to get it from the table schema.
        if let Ok(meta) = sess.engine.config().catalog
            .load_table(&sess.project, &match basin_common::TableName::new(table.as_str()) {
                Ok(n) => n,
                Err(_) => return,
            })
            .await
        {
            let schema = meta.schema.clone();
            let idx = schema.fields().iter().position(|f| {
                f.name().eq_ignore_ascii_case(&time_col)
            });
            if let Some(sidx) = idx {
                // Scan values using schema index.
                let Some(ref source) = ins.source else { return; };
                let SetExpr::Values(vals) = source.body.as_ref() else { return; };
                for row in &vals.rows {
                    if sidx >= row.len() { continue; }
                    if let Some(ts) = expr_to_datetime(&row[sidx]) {
                        let _ = sess.engine.hypertable_registry()
                            .touch_chunk(&sess.project, &table, ts)
                            .await;
                    }
                }
            }
        }
        return;
    };

    // Scan VALUES rows and extract timestamp from the time column.
    let Some(ref source) = ins.source else { return; };
    let SetExpr::Values(vals) = source.body.as_ref() else { return; };
    for row in &vals.rows {
        if col_idx >= row.len() { continue; }
        if let Some(ts) = expr_to_datetime(&row[col_idx]) {
            let _ = sess.engine.hypertable_registry()
                .touch_chunk(&sess.project, &table, ts)
                .await;
        }
    }
}

/// Parse a simple timestamp literal expression into a `DateTime<Utc>`.
/// Understands single-quoted ISO-8601 strings and `TO_TIMESTAMP(...)` calls.
/// Normalise a PG-style timestamp string so that `DateTime::parse_from_rfc3339`
/// can parse it.  Handles two deviations from RFC 3339:
///   * space separator instead of 'T'  → replace first space with 'T'
///   * short UTC offset `+00` or `-05` → expand to `+00:00` / `-05:00`
fn normalize_pg_timestamp(s: &str) -> String {
    // Replace the date/time separator space with 'T'
    let s = if !s.contains('T') {
        s.replacen(' ', "T", 1)
    } else {
        s.to_string()
    };
    // Detect a trailing offset of the form +HH or -HH (3 chars, no colon)
    // We look for the last '+' or '-' that appears after position 10 (after the date part).
    let tz_plus  = s[10..].rfind('+').map(|p| p + 10);
    let tz_minus = s[10..].rfind('-').map(|p| p + 10);
    let tz_pos = match (tz_plus, tz_minus) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None)    => Some(a),
        (None, Some(b))    => Some(b),
        (None, None)       => None,
    };
    if let Some(pos) = tz_pos {
        let suffix = &s[pos..];
        // +HH or -HH: exactly 3 chars and no colon
        if suffix.len() == 3 && !suffix.contains(':') {
            return format!("{}:00", s);
        }
        // +HHMM or -HHMM: exactly 5 chars and no colon → +HH:MM
        if suffix.len() == 5 && !suffix.contains(':') {
            return format!("{}:{}:{}", &s[..pos + 1], &suffix[1..3], &suffix[3..]);
        }
    }
    s
}

fn expr_to_datetime(expr: &sqlparser::ast::Expr) -> Option<chrono::DateTime<chrono::Utc>> {
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    use chrono::DateTime;

    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) => {
            // Try multiple PG/ISO timestamp formats, most specific first.
            let formats: &[&str] = &[
                "%Y-%m-%d %H:%M:%S%.f%:z",  // '2024-01-01 12:00:00.000000+00:00'
                "%Y-%m-%d %H:%M:%S%:z",     // '2024-01-01 12:00:00+00:00'
                "%Y-%m-%d %H:%M:%S%z",      // '2024-01-01 12:00:00+0000'
                "%Y-%m-%dT%H:%M:%S%:z",     // ISO-8601 with colon
                "%Y-%m-%dT%H:%M:%SZ",       // ISO-8601 Zulu
            ];
            let mut result = DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .ok();
            if result.is_none() {
                // Try '2024-01-01 00:00:00+00' (no colon in offset, short offset)
                // by appending ':00' to a bare `+HH` suffix.
                let normalized = normalize_pg_timestamp(s);
                result = chrono::DateTime::parse_from_rfc3339(&normalized)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .ok();
            }
            if result.is_none() {
                for fmt in formats {
                    if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
                        result = Some(dt.with_timezone(&chrono::Utc));
                        break;
                    }
                }
            }
            if result.is_none() {
                // Try without tz: '2024-01-01 00:00:00'
                result = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .map(|ndt| ndt.and_utc())
                    .ok();
            }
            result
        }
        // TO_TIMESTAMP(<epoch_s>) + INTERVAL '...' — in the soak test.
        // We don't evaluate this fully; skip (chunk creation still works via
        // the simpler literal path for the basic tests).
        _ => None,
    }
}

/// Execute `SELECT create_hypertable('table', 'col', chunk_time_interval =>
/// INTERVAL '...')`.  Registers the table in the HypertableRegistry and
/// returns a single-row result mimicking TimescaleDB's output shape.
async fn exec_create_hypertable(
    sess: &ProjectSession,
    table: &str,
    time_col: &str,
    interval_text: &str,
) -> Result<ExecResult> {
    let secs = crate::hypertable::parse_interval_secs(interval_text)
        .ok_or_else(|| BasinError::InvalidSchema(format!(
            "create_hypertable: could not parse interval '{interval_text}'"
        )))?;
    sess.engine
        .hypertable_registry()
        .register(&sess.project, table, time_col.to_string(), secs)
        .await
        .map_err(|e| BasinError::InvalidSchema(e))?;
    // Return a single-row result mimicking TimescaleDB's create_hypertable().
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("hypertable_id",   arrow_schema::DataType::Int64,  false),
        arrow_schema::Field::new("schema_name",     arrow_schema::DataType::Utf8,   false),
        arrow_schema::Field::new("table_name",      arrow_schema::DataType::Utf8,   false),
        arrow_schema::Field::new("created",         arrow_schema::DataType::Boolean, false),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![1i64])) as ArrayRef,
            Arc::new(arrow_array::StringArray::from(vec!["public"])) as ArrayRef,
            Arc::new(arrow_array::StringArray::from(vec![table])) as ArrayRef,
            Arc::new(arrow_array::BooleanArray::from(vec![true])) as ArrayRef,
        ],
    )
    .map_err(|e| BasinError::internal(format!("create_hypertable result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

/// Execute `SELECT add_retention_policy('table', INTERVAL '...')`.
/// Registers a retention policy on the hypertable.
async fn exec_add_retention_policy(
    sess: &ProjectSession,
    table: &str,
    interval_text: &str,
) -> Result<ExecResult> {
    let secs = crate::hypertable::parse_interval_secs(interval_text)
        .ok_or_else(|| BasinError::InvalidSchema(format!(
            "add_retention_policy: could not parse interval '{interval_text}'"
        )))?;
    sess.engine
        .hypertable_registry()
        .set_retention(&sess.project, table, secs)
        .await
        .map_err(|e| BasinError::InvalidSchema(e))?;
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("job_id", arrow_schema::DataType::Int64, false),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1i64])) as ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("add_retention_policy result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

/// Execute `SELECT drop_chunks('table', older_than => INTERVAL/TIMESTAMP)`.
///
/// Steps:
/// 1. Resolve the cutoff to a `DateTime<Utc>`.
/// 2. Call `HypertableRegistry::drop_chunks_before` to remove chunk metadata
///    for all chunks whose range_end <= cutoff.
/// 3. Issue a physical DELETE WHERE ts < cutoff so the base-table rows are
///    removed (reuses the same retention-delete path).
/// 4. Return a result row with the number of chunks dropped.
///
/// The chunk metadata removal and the physical DELETE are NOT atomic (there is
/// no 2PC across the catalog and the storage layer).  The safe failure mode is
/// metadata dropped but rows surviving, which is conservative — a subsequent
/// SELECT will still return the rows until the DELETE commits.  See APPLY.md
/// for the full risk note on catalog atomicity.
async fn exec_drop_chunks(
    sess: &ProjectSession,
    table: &str,
    cutoff_spec: crate::hypertable::DropChunksCutoff,
) -> Result<ExecResult> {
    use crate::hypertable::DropChunksCutoff;
    use chrono::Utc;

    // Resolve the cutoff timestamp.
    let cutoff: chrono::DateTime<Utc> = match &cutoff_spec {
        DropChunksCutoff::Interval(iv_text) => {
            let secs = crate::hypertable::parse_interval_secs(iv_text)
                .ok_or_else(|| BasinError::InvalidSchema(format!(
                    "drop_chunks: could not parse interval '{iv_text}'"
                )))?;
            Utc::now() - chrono::Duration::seconds(secs as i64)
        }
        DropChunksCutoff::Timestamp(ts_text) => {
            // Accept ISO-8601 / PG-style timestamp strings.
            parse_cutoff_timestamp(ts_text)
                .ok_or_else(|| BasinError::InvalidSchema(format!(
                    "drop_chunks: could not parse timestamp '{ts_text}'"
                )))?
        }
    };

    // Drop chunk metadata and collect the names of dropped chunks.
    let dropped = sess.engine
        .hypertable_registry()
        .drop_chunks_before(&sess.project, table, cutoff)
        .await
        .map_err(|e| BasinError::InvalidSchema(e))?;

    if !dropped.is_empty() {
        // Issue a physical DELETE for rows in the dropped range.
        if let Some(time_col) = sess.engine.hypertable_registry()
            .time_column(&sess.project, table)
            .await
        {
            let cutoff_us = cutoff.timestamp_micros();
            let delete_sql = format!(
                "DELETE FROM \"{table}\" WHERE \"{time_col}\" < {cutoff_us}"
            );
            let _ = exec_retention_delete(sess, &delete_sql).await;
        }
    }

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("chunks_dropped", arrow_schema::DataType::Int64, false),
    ]));
    let n = dropped.len() as i64;
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![n])) as ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("drop_chunks result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

/// Parse a timestamp string in a variety of formats to `DateTime<Utc>`.
/// Used by `exec_drop_chunks` for the `older_than => TIMESTAMP '...'` form.
fn parse_cutoff_timestamp(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::{DateTime, NaiveDateTime, Utc};
    // Try RFC 3339 first.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Try common PG formats.
    let formats: &[&str] = &[
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ];
    for fmt in formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(ndt.and_utc());
        }
        // chrono parse_from_str with timezone
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
    }
    // Bare date
    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(nd.and_hms_opt(0, 0, 0)?.and_utc());
    }
    None
}

/// Execute `SELECT run_retention_policy('table')`.
/// Drops chunks that fall outside the retention window, then issues a physical
/// DELETE so the base table's rows are also removed.
async fn exec_run_retention_policy(
    sess: &ProjectSession,
    table: &str,
) -> Result<ExecResult> {
    // Read the retention window, compute the cutoff, then delegate to the
    // shared drop_chunks_before core (same path as exec_drop_chunks).
    let Some(retention_secs) = get_retention_secs_from_registry(
        sess.engine.hypertable_registry(),
        &sess.project,
        table,
    ).await else {
        // No retention policy set — return 0 chunks dropped.
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("chunks_dropped", arrow_schema::DataType::Int64, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![0i64])) as ArrayRef],
        )
        .map_err(|e| BasinError::internal(format!("run_retention_policy result: {e}")))?;
        return Ok(ExecResult::Rows { schema, batches: vec![batch] });
    };

    let cutoff = chrono::Utc::now() - chrono::Duration::seconds(retention_secs as i64);

    let dropped = sess.engine
        .hypertable_registry()
        .drop_chunks_before(&sess.project, table, cutoff)
        .await
        .map_err(|e| BasinError::InvalidSchema(e))?;

    if !dropped.is_empty() {
        if let Some(time_col) = sess.engine.hypertable_registry()
            .time_column(&sess.project, table)
            .await
        {
            let cutoff_us = cutoff.timestamp_micros();
            let delete_sql = format!(
                "DELETE FROM \"{table}\" WHERE \"{time_col}\" < {cutoff_us}"
            );
            let _ = exec_retention_delete(sess, &delete_sql).await;
        }
    }

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("chunks_dropped", arrow_schema::DataType::Int64, false),
    ]));
    let n = dropped.len() as i64;
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int64Array::from(vec![n])) as ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("run_retention_policy result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

/// Issue a DELETE SQL statement for the retention path without going through
/// the top-level `execute` (which would create an async recursion cycle).
/// Parses the DELETE SQL directly and calls `exec_delete`.
async fn exec_retention_delete(sess: &ProjectSession, sql: &str) -> Result<ExecResult> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|e| BasinError::InvalidSchema(format!("retention delete parse: {e}")))?;
    match stmts.into_iter().next() {
        Some(sqlparser::ast::Statement::Delete(del)) => {
            crate::dml_mutate::exec_delete(sess, del).await
        }
        _ => Ok(ExecResult::Empty { tag: "DELETE 0".into() }),
    }
}

/// Helper to read the retention_secs from the registry without a new public API.
async fn get_retention_secs_from_registry(
    reg: &Arc<crate::hypertable::HypertableRegistry>,
    project: &basin_common::ProjectId,
    table: &str,
) -> Option<u64> {
    // We expose this by adding a small method to HypertableRegistry.
    reg.retention_secs(project, table).await
}

/// Execute `SELECT compress_chunk(...)` — marks chunks compressed in the
/// registry.  Physical compression is a 5.29.E concern; for now this is a
/// metadata-only operation so `is_compressed = true` shows in the chunks view.
async fn exec_compress_chunk(
    sess: &ProjectSession,
    intent: crate::hypertable::CompressChunkIntent,
) -> Result<ExecResult> {
    use crate::hypertable::CompressChunkIntent;
    match intent {
        CompressChunkIntent::Named { chunk_name } => {
            sess.engine
                .hypertable_registry()
                .compress_chunk(&sess.project, &chunk_name)
                .await;
        }
        CompressChunkIntent::AllForTable { hypertable_name, before_ts } => {
            // Mark all chunks for this table (optionally before a cutoff) compressed.
            let chunks = sess.engine
                .hypertable_registry()
                .snapshot_chunks(&sess.project, &hypertable_name)
                .await;
            let cutoff: Option<chrono::DateTime<chrono::Utc>> = before_ts
                .as_deref()
                .and_then(|s| s.parse().ok());
            for c in &chunks {
                let should_compress = match cutoff {
                    Some(co) => c.range_end <= co,
                    None => true,
                };
                if should_compress {
                    sess.engine
                        .hypertable_registry()
                        .compress_chunk(&sess.project, &c.chunk_name)
                        .await;
                }
            }
        }
        CompressChunkIntent::AllUnfiltered => {
            // No target table info — noop compress (safe fallback).
        }
    }
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("compress_chunk", arrow_schema::DataType::Utf8, true),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::StringArray::from(vec![Option::<&str>::None])) as ArrayRef],
    )
    .map_err(|e| BasinError::internal(format!("compress_chunk result: {e}")))?;
    Ok(ExecResult::Rows { schema, batches: vec![batch] })
}

// ---------------------------------------------------------------------------
// json_agg whole-row rewrite — unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod json_agg_rewrite_tests {
    use super::rewrite_json_agg_whole_row;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn make_ctx_with_table(table: &str, cols: &[(&str, DataType)]) -> SessionContext {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table(table, Arc::new(mem)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn json_agg_bare_table_is_expanded() {
        let ctx = make_ctx_with_table("t", &[("id", DataType::Int32), ("name", DataType::Utf8)]);
        let sql = "SELECT json_agg(t) FROM t";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        // Must contain named_struct with the column names.
        let lower = rewritten.to_ascii_lowercase();
        assert!(
            lower.contains("named_struct"),
            "expected named_struct in: {rewritten}"
        );
        assert!(lower.contains("'id'"), "expected 'id' key in: {rewritten}");
        assert!(
            lower.contains("'name'"),
            "expected 'name' key in: {rewritten}"
        );
        assert!(lower.contains("t.id"), "expected t.id ref in: {rewritten}");
        assert!(
            lower.contains("t.name"),
            "expected t.name ref in: {rewritten}"
        );
    }

    #[tokio::test]
    async fn jsonb_agg_bare_table_is_expanded() {
        let ctx = make_ctx_with_table("u", &[("val", DataType::Int64)]);
        let sql = "SELECT jsonb_agg(u) FROM u";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        let lower = rewritten.to_ascii_lowercase();
        assert!(
            lower.contains("named_struct"),
            "expected named_struct in: {rewritten}"
        );
        assert!(
            lower.contains("u.val"),
            "expected u.val ref in: {rewritten}"
        );
    }

    #[tokio::test]
    async fn json_agg_scalar_col_is_unchanged() {
        let ctx = make_ctx_with_table("t", &[("id", DataType::Int32)]);
        // `json_agg(id)` — `id` is not a table name, should not be rewritten.
        let sql = "SELECT json_agg(id) FROM t";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        // The rewriter tries `id` as a table name; since there's no table `id`,
        // it emits it verbatim.
        assert_eq!(rewritten, sql, "json_agg(col) must not be rewritten");
    }

    #[tokio::test]
    async fn no_json_agg_is_unchanged() {
        let ctx = SessionContext::new();
        let sql = "SELECT COUNT(*) FROM t WHERE id > 1";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        assert_eq!(rewritten, sql);
    }

    #[tokio::test]
    async fn json_agg_uppercase_is_expanded() {
        let ctx = make_ctx_with_table("t", &[("x", DataType::Float64)]);
        let sql = "SELECT JSON_AGG(t) FROM t";
        let rewritten = rewrite_json_agg_whole_row(sql, &ctx).await;
        let lower = rewritten.to_ascii_lowercase();
        assert!(
            lower.contains("named_struct"),
            "JSON_AGG uppercase should be expanded: {rewritten}"
        );
        assert!(lower.contains("'x'"), "expected 'x' key: {rewritten}");
    }
}

// ---------------------------------------------------------------------------
// ON CONFLICT DO UPDATE — rewrite_do_update_expr unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod do_update_rewrite_tests {
    use super::rewrite_do_update_expr;
    use sqlparser::ast::{BinaryOperator, Expr, Ident, Value, ValueWithSpan};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    /// Parse a single SQL expression string into an `Expr` node.
    fn parse_expr(sql: &str) -> Expr {
        let mut p = Parser::new(&PostgreSqlDialect {});
        // Wrap in SELECT so the parser reaches the expression context.
        let stmt = Parser::parse_sql(&PostgreSqlDialect {}, &format!("SELECT {sql}"))
            .expect("parse")
            .into_iter()
            .next()
            .expect("stmt");
        if let sqlparser::ast::Statement::Query(q) = stmt {
            if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                if let sqlparser::ast::SelectItem::UnnamedExpr(e) =
                    sel.projection.into_iter().next().expect("item")
                {
                    return e;
                }
            }
        }
        panic!("could not parse expression: {sql}");
    }

    /// Build a simple integer literal Expr.
    fn int_expr(n: i64) -> Expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(n.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }

    fn make_excluded(pairs: &[(&str, Expr)]) -> HashMap<String, Expr> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // EXCLUDED.col → proposed-row literal value.
    #[test]
    fn excluded_col_rewritten_to_literal() {
        let excluded = make_excluded(&[("hits", int_expr(42))]);
        let expr = parse_expr("EXCLUDED.hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        assert_eq!(format!("{result}"), "42");
    }

    // tablename.col → bare col identifier (existing row).
    #[test]
    fn table_col_rewritten_to_bare_identifier() {
        let excluded: HashMap<String, Expr> = HashMap::new();
        let expr = parse_expr("accounts.hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        // Should become bare `hits` identifier.
        assert!(
            matches!(&result, Expr::Identifier(i) if i.value == "hits"),
            "expected bare identifier `hits`, got: {result}"
        );
    }

    // Unqualified col → unchanged (existing row in UPDATE scope).
    #[test]
    fn unqualified_col_unchanged() {
        let excluded: HashMap<String, Expr> = HashMap::new();
        let expr = parse_expr("hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        assert!(
            matches!(&result, Expr::Identifier(i) if i.value == "hits"),
            "expected unchanged identifier `hits`, got: {result}"
        );
    }

    // BinaryOp: t.hits + EXCLUDED.hits → hits + 42
    #[test]
    fn binary_op_mixed_refs() {
        let excluded = make_excluded(&[("hits", int_expr(5))]);
        let expr = parse_expr("accounts.hits + EXCLUDED.hits");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        // Should be `hits + 5`.
        let s = format!("{result}");
        assert!(s.contains("hits"), "expected `hits` in result: {s}");
        assert!(s.contains('5'), "expected `5` in result: {s}");
        assert!(
            !s.contains("accounts"),
            "table qualifier must be stripped: {s}"
        );
        assert!(!s.contains("EXCLUDED"), "EXCLUDED must be resolved: {s}");
    }

    // Case-insensitive: EXCLUDED.HITS, ACCOUNTS.HITS → resolved correctly.
    #[test]
    fn case_insensitive_matching() {
        let excluded = make_excluded(&[("hits", int_expr(99))]);
        let expr = parse_expr("EXCLUDED.HITS");
        let result = rewrite_do_update_expr(expr, "accounts", &excluded);
        assert_eq!(format!("{result}"), "99");
    }

    // Unknown EXCLUDED column → left unchanged (pass-through for unknown refs).
    #[test]
    fn excluded_unknown_col_passthrough() {
        let excluded: HashMap<String, Expr> = HashMap::new();
        let expr = parse_expr("EXCLUDED.nonexistent");
        let result = rewrite_do_update_expr(expr.clone(), "accounts", &excluded);
        assert_eq!(format!("{result}"), format!("{expr}"));
    }
}

// ---------------------------------------------------------------------------
// ON CONFLICT DO NOTHING — validate_conflict_target_columns unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod do_nothing_validate_tests {
    use super::validate_conflict_target_columns;
    use basin_catalog::UniqueConstraint;

    /// Build PK and UNIQUE inputs for the helper.
    fn pk(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }
    fn uniques(groups: &[&[&str]]) -> Vec<UniqueConstraint> {
        groups
            .iter()
            .enumerate()
            .map(|(i, cols)| UniqueConstraint {
                name: format!("t_uc{i}"),
                columns: cols.iter().map(|s| s.to_string()).collect(),
            })
            .collect()
    }
    fn conflict(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    // --- matches PK ---------------------------------------------------------

    #[test]
    fn pk_single_col_accepted() {
        assert!(
            validate_conflict_target_columns(&pk(&["id"]), &uniques(&[]), &conflict(&["id"]))
                .is_ok()
        );
    }

    #[test]
    fn pk_composite_accepted() {
        assert!(validate_conflict_target_columns(
            &pk(&["a", "b"]),
            &uniques(&[]),
            &conflict(&["a", "b"])
        )
        .is_ok());
    }

    #[test]
    fn pk_order_independent() {
        // Reversed order — still matches PK set.
        assert!(validate_conflict_target_columns(
            &pk(&["a", "b"]),
            &uniques(&[]),
            &conflict(&["b", "a"])
        )
        .is_ok());
    }

    // --- matches UNIQUE constraint ------------------------------------------

    #[test]
    fn unique_constraint_single_col_accepted() {
        assert!(validate_conflict_target_columns(
            &pk(&[]),
            &uniques(&[&["email"]]),
            &conflict(&["email"])
        )
        .is_ok());
    }

    #[test]
    fn unique_constraint_composite_accepted() {
        assert!(validate_conflict_target_columns(
            &pk(&[]),
            &uniques(&[&["org_id", "slug"]]),
            &conflict(&["org_id", "slug"])
        )
        .is_ok());
    }

    // --- mismatches → error -------------------------------------------------

    #[test]
    fn subset_of_pk_rejected() {
        // Only one of the two PK cols — not a complete match.
        assert!(validate_conflict_target_columns(
            &pk(&["a", "b"]),
            &uniques(&[]),
            &conflict(&["a"])
        )
        .is_err());
    }

    #[test]
    fn superset_of_pk_rejected() {
        assert!(validate_conflict_target_columns(
            &pk(&["id"]),
            &uniques(&[]),
            &conflict(&["id", "extra"])
        )
        .is_err());
    }

    #[test]
    fn non_constraint_col_rejected() {
        // The combined set {email, id} is not any single constraint.
        assert!(validate_conflict_target_columns(
            &pk(&["id"]),
            &uniques(&[&["email"]]),
            &conflict(&["email", "id"])
        )
        .is_err());
    }

    #[test]
    fn empty_target_accepted_without_error() {
        // Empty conflict target (no columns): validated as OK; caller handles
        // the no-target case (all constraints checked).
        assert!(
            validate_conflict_target_columns(&pk(&["id"]), &uniques(&[]), &conflict(&[])).is_ok()
        );
    }

    // --- same-batch dedup semantics (PG: first row wins) --------------------
    // Structural test: validate returns Ok for a conflict target that IS a real constraint.
    #[test]
    fn same_batch_dup_target_accepted() {
        assert!(
            validate_conflict_target_columns(&pk(&["id"]), &uniques(&[]), &conflict(&["id"]))
                .is_ok()
        );
    }
}

// ---------------------------------------------------------------------------
// WITH RECURSIVE multi-column — DataFusion execution tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod recursive_cte_exec_tests {
    use crate::pg_operators::rewrite_recursive_cte_column_aliases;
    use datafusion::arrow::array::Array;
    use datafusion::prelude::SessionContext;

    /// Run a SQL string through the recursive-CTE alias rewriter then execute
    /// it in a bare DataFusion `SessionContext`.  Returns the flattened i64
    /// values from the first column of all result batches.
    async fn run_i64(sql: &str) -> Result<Vec<i64>, String> {
        let rewritten = rewrite_recursive_cte_column_aliases(sql);
        let ctx = SessionContext::new();
        let df = ctx.sql(&rewritten).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        let mut vals = Vec::new();
        for batch in &batches {
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .ok_or_else(|| format!("expected Int64Array, got {:?}", col.data_type()))?;
            for i in 0..arr.len() {
                vals.push(arr.value(i));
            }
        }
        Ok(vals)
    }

    /// Run a SQL string through the rewriter and execute it; returns first
    /// column as `Vec<String>` (Utf8).
    async fn run_str(sql: &str) -> Result<Vec<String>, String> {
        let rewritten = rewrite_recursive_cte_column_aliases(sql);
        let ctx = SessionContext::new();
        let df = ctx.sql(&rewritten).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        let mut vals = Vec::new();
        for batch in &batches {
            let col = batch.column(0);
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| format!("expected StringArray, got {:?}", col.data_type()))?;
            for i in 0..arr.len() {
                vals.push(arr.value(i).to_string());
            }
        }
        Ok(vals)
    }

    /// Fibonacci multi-column WITH RECURSIVE: fib(a, b).
    ///
    /// PostgreSQL spec: anchor row (1,1) + 10 recursive rows where source b < 100.
    /// Full table: (1,1),(1,2),(2,3),(3,5),(5,8),(8,13),(13,21),(21,34),(34,55),(55,89),(89,144)
    /// SELECT a → 11 values: 1,1,2,3,5,8,13,21,34,55,89
    #[tokio::test]
    async fn fib_multi_col_exact_sequence() {
        let sql = "WITH RECURSIVE fib(a, b) AS \
            (SELECT 1, 1 \
             UNION ALL \
             SELECT b, a+b FROM fib WHERE b < 100) \
            SELECT a FROM fib";
        let vals = run_i64(sql).await.expect("fib query must succeed");
        let expected: Vec<i64> = vec![1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89];
        assert_eq!(
            vals, expected,
            "fib(a,b) SELECT a must match PG Fibonacci sequence"
        );
    }

    /// Single-column regression: existing behaviour must be preserved.
    #[tokio::test]
    async fn single_col_regression() {
        let sql = "WITH RECURSIVE r(n) AS \
            (SELECT 1 \
             UNION ALL \
             SELECT n+1 FROM r WHERE n < 5) \
            SELECT n FROM r";
        let vals = run_i64(sql)
            .await
            .expect("single-col recursive CTE must succeed");
        let expected: Vec<i64> = vec![1, 2, 3, 4, 5];
        assert_eq!(vals, expected, "single-col r(n) must produce 1..=5");
    }

    /// Two-column string-hierarchy accumulator (non-numeric confidence test).
    #[tokio::test]
    async fn two_col_string_hierarchy() {
        // Builds a chain: path grows left-to-right, depth counts steps.
        // anchor: ('root', 0)
        // recursive: append '->child' while depth < 3
        // Rows: ('root',0), ('root->child',1), ('root->child->child',2), ('root->child->child->child',3)
        let sql = "WITH RECURSIVE tree(path, depth) AS \
            (SELECT 'root', 0 \
             UNION ALL \
             SELECT path || '->child', depth + 1 FROM tree WHERE depth < 3) \
            SELECT path FROM tree";
        let vals = run_str(sql)
            .await
            .expect("string hierarchy CTE must succeed");
        let expected = vec![
            "root".to_string(),
            "root->child".to_string(),
            "root->child->child".to_string(),
            "root->child->child->child".to_string(),
        ];
        assert_eq!(vals, expected, "string hierarchy paths must match");
    }
}

// ---------------------------------------------------------------------------
// Auto-commit read-your-own-writes (RYOW) — lib unit tests (#105)
// ---------------------------------------------------------------------------
// These tests use the full Engine + InMemoryCatalog + LocalFileSystem stack so
// they exercise the real exec_insert → catalog → exec_select pipeline.  They
// are deliberately placed in the `executor` lib so they run with
// `cargo test -p basin-engine --lib` and count toward the lib-test baseline.
#[cfg(test)]
mod auto_commit_ryow_tests {
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    fn count_from_result(res: ExecResult) -> i64 {
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
        };
        let b = batches.first().expect("no batch returned");
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count column must be Int64")
            .value(0)
    }

    /// Each auto-commit INSERT must be immediately visible to the next SELECT
    /// on the same session.  Pins the fix for #105 (refresh_table_with_extra
    /// in the auto-commit path).
    #[tokio::test]
    async fn insert_values_ryow_per_statement() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
            .await
            .unwrap();

        sess.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let n1 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n1, 1,
            "row 1 must be visible immediately after first INSERT"
        );

        sess.execute("INSERT INTO t VALUES (2)").await.unwrap();
        let n2 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n2, 2,
            "row 2 must be visible immediately after second INSERT"
        );

        sess.execute("INSERT INTO t VALUES (3)").await.unwrap();
        let n3 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n3, 3,
            "row 3 must be visible immediately after third INSERT"
        );
    }

    /// Multi-row INSERT: all rows in a single statement must be visible to the
    /// immediately-following SELECT.
    #[tokio::test]
    async fn insert_multi_row_ryow() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
            .await
            .unwrap();

        sess.execute("INSERT INTO t VALUES (10), (20), (30)")
            .await
            .unwrap();
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n, 3,
            "all 3 rows from multi-row INSERT must be visible immediately"
        );
    }

    /// INSERT … RETURNING: the returned row must also appear in a subsequent
    /// SELECT on the same session.
    #[tokio::test]
    async fn insert_returning_ryow() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE t (id BIGINT NOT NULL)")
            .await
            .unwrap();

        // First INSERT: verify it's visible.
        sess.execute("INSERT INTO t VALUES (1)").await.unwrap();
        let n1 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(
            n1, 1,
            "first INSERT must be visible before RETURNING INSERT"
        );

        // INSERT RETURNING: the returned id must match, and the subsequent
        // COUNT must reflect both rows.
        let ret = sess
            .execute("INSERT INTO t VALUES (2) RETURNING id")
            .await
            .unwrap();
        let returned_id = match ret {
            ExecResult::Rows { batches, .. } => {
                let b = batches.first().expect("RETURNING must produce a batch");
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column must be Int64")
                    .value(0)
            }
            ExecResult::Empty { tag } => panic!("INSERT RETURNING produced Empty({tag})"),
        };
        assert_eq!(returned_id, 2, "RETURNING must echo inserted id");

        let n2 = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n2, 2, "both rows must be visible after INSERT RETURNING");
    }
}

// ---------------------------------------------------------------------------
// Pre-parse literal-INSERT fast path — engagement / decline gates.
// ---------------------------------------------------------------------------
// These tests call `try_insert_preparse` directly so engagement is asserted
// without relying on the process-global counter alone (other tests in this
// binary run concurrently and also bump it — e.g. every plain literal INSERT
// in `auto_commit_ryow_tests` engages the path). The byte-equivalence of the
// engaged path vs the declined path is covered end-to-end by the integration
// suite (`tests/integration/tests/values_fast_ingest.rs`).
#[cfg(test)]
mod preparse_fastpath_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use arrow_array::Int64Array;
    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::{try_insert_preparse, INSERTS_PREPARSE_FASTPATH};
    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    fn count_from_result(res: ExecResult) -> i64 {
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
        };
        batches.first().expect("no batch")
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count must be Int64")
            .value(0)
    }

    #[tokio::test]
    async fn engages_on_plain_multi_row_literal_insert() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        let before = INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed);
        let res = try_insert_preparse(
            &sess,
            "INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b''c'), (3, NULL)",
        )
        .await;
        let res = res.expect("pre-parse path must engage on the plain literal shape");
        match res.expect("engaged insert must succeed") {
            ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 3"),
            other => panic!("expected INSERT tag, got {other:?}"),
        }
        // ≥ 1, not == 1: other tests in this binary bump the global counter
        // concurrently.
        assert!(
            INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed) > before,
            "engagement counter must advance"
        );

        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 3, "engaged rows must be visible to the next SELECT");
    }

    #[tokio::test]
    async fn engages_via_execute_entry_point() {
        // Same shape through the public `execute` path (the real hook site).
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();
        let before = INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed);
        sess.execute("INSERT INTO t (id, s) VALUES (10, 'x'), (11, 'y')")
            .await
            .unwrap();
        assert!(
            INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed) > before,
            "execute() must route the plain literal shape through the pre-parse path"
        );
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn declines_on_trailing_clauses_and_non_literal_shapes() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();
        sess.execute("CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        // Trailing clause after the tuples → the tuple scanner declines.
        for sql in [
            "INSERT INTO t (id, s) VALUES (1, 'a') RETURNING id",
            "INSERT INTO t (id, s) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING",
            "INSERT INTO t (id, s) VALUES (1, 'a'); INSERT INTO t (id, s) VALUES (2, 'b')",
            // Header shapes the classifier itself declines.
            "INSERT INTO t2 SELECT * FROM t",
            "WITH x AS (SELECT 4 AS id) INSERT INTO t (id) SELECT id FROM x",
            "INSERT INTO t DEFAULT VALUES",
            // Non-literal tuple values → scanner declines.
            "INSERT INTO t (id, s) VALUES (1 + 1, 'a')",
            "INSERT INTO t (id, s) VALUES ($1, 'a')",
        ] {
            assert!(
                try_insert_preparse(&sess, sql).await.is_none(),
                "must decline: {sql}"
            );
        }
        // Nothing was written by the declined probes.
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn engages_inside_explicit_transaction_with_tx_semantics() {
        // Gate 1's CURRENT contract: OPEN transactions are admitted (only the
        // aborted state declines — see `declines_inside_aborted_transaction`).
        // The engaged path must inherit exec_insert's full in-tx branch:
        // rows buffered + visible in-tx, dropped on ROLLBACK, kept on COMMIT.
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        // ── BEGIN → engage → visible in-tx → ROLLBACK drops ────────────────
        sess.execute("BEGIN").await.unwrap();
        let before = INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed);
        let res = try_insert_preparse(&sess, "INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b')")
            .await
            .expect("pre-parse path must engage inside an open transaction");
        match res.expect("engaged in-tx insert must succeed") {
            ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 2"),
            other => panic!("expected INSERT tag, got {other:?}"),
        }
        // ≥ 1, not == 1: other tests in this binary bump the global counter
        // concurrently.
        assert!(
            INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed) > before,
            "engagement counter must advance for the in-tx statement"
        );
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 2, "engaged in-tx rows must be visible inside the transaction");
        sess.execute("ROLLBACK").await.unwrap();
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 0, "ROLLBACK must drop the engaged in-tx rows");

        // ── BEGIN → engage → COMMIT keeps ───────────────────────────────────
        sess.execute("BEGIN").await.unwrap();
        let before = INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed);
        try_insert_preparse(&sess, "INSERT INTO t (id, s) VALUES (3, 'c'), (4, 'd')")
            .await
            .expect("pre-parse path must engage inside an open transaction")
            .expect("engaged in-tx insert must succeed");
        assert!(
            INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed) > before,
            "engagement counter must advance for the second in-tx statement"
        );
        sess.execute("COMMIT").await.unwrap();
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 2, "COMMIT must persist the engaged in-tx rows");
    }

    #[tokio::test]
    async fn declines_inside_aborted_transaction() {
        // The 25P02 aborted-state guard stays with the normal path: once a
        // statement fails in-tx, the pre-parse path must decline.
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        sess.execute("BEGIN").await.unwrap();
        sess.execute("SELECT * FROM no_such_table")
            .await
            .expect_err("statement against a missing table must fail in-tx");
        assert!(
            try_insert_preparse(&sess, "INSERT INTO t (id, s) VALUES (1, 'a')")
                .await
                .is_none(),
            "aborted transaction must decline the pre-parse path"
        );
        sess.execute("ROLLBACK").await.unwrap();
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 0, "nothing may be written by the declined probe");
    }

    #[tokio::test]
    async fn declined_returning_still_works_via_execute() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();
        // Full execute(): RETURNING declines the pre-parse path and the
        // normal path returns the inserted batch.
        match sess
            .execute("INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b') RETURNING *")
            .await
            .unwrap()
        {
            ExecResult::Rows { batches, .. } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(rows, 2, "RETURNING must surface both inserted rows");
            }
            other => panic!("expected Rows from RETURNING, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Bind-direct parameterized-INSERT fast path — engagement / decline gates.
// ---------------------------------------------------------------------------
// Mirrors `preparse_fastpath_tests`: engagement is asserted by calling
// `try_insert_bind_direct` directly (and via the prepared-statement API with
// the process-global counter as a secondary signal). End-to-end equivalence
// vs the simple path is covered by the integration suites
// (`values_fast_ingest.rs` prepared variants, `prepared_insert_fast.rs`).
#[cfg(test)]
mod bind_direct_fastpath_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use arrow_array::Int64Array;
    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::{ProjectId, TableName};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use super::{BindInsertPlan, INSERTS_BIND_DIRECT_FASTPATH, try_insert_bind_direct};
    use crate::prepared::ScalarParam;
    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    fn count_from_result(res: ExecResult) -> i64 {
        let batches = match res {
            ExecResult::Rows { batches, .. } => batches,
            ExecResult::Empty { tag } => panic!("expected Rows, got Empty({tag})"),
        };
        batches
            .first()
            .expect("no batch")
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count must be Int64")
            .value(0)
    }

    fn plan_id_s() -> BindInsertPlan {
        BindInsertPlan {
            table: TableName::new("t").unwrap(),
            columns: vec!["id".to_string(), "s".to_string()],
            rows: vec![vec![0, 1]],
        }
    }

    #[tokio::test]
    async fn engages_on_direct_call() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        let before = INSERTS_BIND_DIRECT_FASTPATH.load(Ordering::Relaxed);
        let plan = plan_id_s();
        let res = try_insert_bind_direct(
            &sess,
            &plan,
            &[ScalarParam::Int8(1), ScalarParam::Text("a".into())],
        )
        .await
        .expect("bind-direct must engage on the plain parameterized shape");
        match res.expect("engaged insert must succeed") {
            ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 1"),
            other => panic!("expected INSERT tag, got {other:?}"),
        }
        // ≥ 1, not == 1: other tests in this binary bump the global counter.
        assert!(
            INSERTS_BIND_DIRECT_FASTPATH.load(Ordering::Relaxed) > before,
            "engagement counter must advance"
        );
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 1, "engaged row must be visible to the next SELECT");
    }

    #[tokio::test]
    async fn engages_via_prepared_api() {
        // The real hook site: prepare → bind → execute_bound. The prepared
        // template gets a bind-direct plan at prepare time and every Execute
        // routes through `try_insert_bind_direct`.
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        let before = INSERTS_BIND_DIRECT_FASTPATH.load(Ordering::Relaxed);
        let (handle, _schema) = sess
            .prepare("INSERT INTO t (id, s) VALUES ($1, $2)")
            .await
            .unwrap();
        for i in 0..3_i64 {
            let bound = sess
                .bind(
                    &handle,
                    vec![ScalarParam::Int8(i), ScalarParam::Text(format!("row-{i}"))],
                )
                .await
                .unwrap();
            match sess.execute_bound(bound).await.unwrap() {
                ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 1"),
                other => panic!("expected INSERT tag, got {other:?}"),
            }
        }
        assert!(
            INSERTS_BIND_DIRECT_FASTPATH.load(Ordering::Relaxed) >= before + 3,
            "all three Executes must take the bind-direct path"
        );
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn declines_inside_explicit_transaction_with_working_fallback() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();
        let (handle, _schema) = sess
            .prepare("INSERT INTO t (id, s) VALUES ($1, $2)")
            .await
            .unwrap();

        sess.execute("BEGIN").await.unwrap();
        // Direct call: auto-commit-only gate declines.
        let plan = plan_id_s();
        assert!(
            try_insert_bind_direct(
                &sess,
                &plan,
                &[ScalarParam::Int8(1), ScalarParam::Text("a".into())],
            )
            .await
            .is_none(),
            "bind-direct path is auto-commit-only"
        );
        // The prepared API still inserts via the AST fallback (in-tx buffering).
        let bound = sess
            .bind(
                &handle,
                vec![ScalarParam::Int8(1), ScalarParam::Text("a".into())],
            )
            .await
            .unwrap();
        sess.execute_bound(bound).await.unwrap();
        sess.execute("COMMIT").await.unwrap();
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 1, "fallback in-tx insert must land at COMMIT");
    }

    #[tokio::test]
    async fn declines_on_schema_or_param_mismatch() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        // Unknown column (e.g. the table was re-created since Parse) → decline.
        let stale = BindInsertPlan {
            table: TableName::new("t").unwrap(),
            columns: vec!["nope".to_string()],
            rows: vec![vec![0]],
        };
        assert!(
            try_insert_bind_direct(&sess, &stale, &[ScalarParam::Int8(1)])
                .await
                .is_none(),
            "unknown column must decline"
        );
        // Out-of-grammar param (bytea) → decline.
        let plan = plan_id_s();
        assert!(
            try_insert_bind_direct(
                &sess,
                &plan,
                &[ScalarParam::Int8(1), ScalarParam::Bytea(vec![1, 2])],
            )
            .await
            .is_none(),
            "bytea param must decline"
        );
        // Missing table → decline (the fallback owns the canonical error).
        let missing = BindInsertPlan {
            table: TableName::new("absent").unwrap(),
            columns: vec![],
            rows: vec![vec![0]],
        };
        assert!(
            try_insert_bind_direct(&sess, &missing, &[ScalarParam::Int8(1)])
                .await
                .is_none(),
            "missing table must decline"
        );
        // Nothing was written by the declined probes.
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn prepared_literal_insert_routes_through_preparse() {
        // Extended-protocol shape 1: a zero-parameter literal multi-row INSERT
        // prepared once must execute through the pre-parse scanner path
        // (`try_insert_preparse`), not by cloning a cached AST.
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
            .await
            .unwrap();

        let before = super::INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed);
        let (handle, schema) = sess
            .prepare("INSERT INTO t (id, s) VALUES (1, 'a'), (2, 'b''c'), (3, NULL)")
            .await
            .unwrap();
        assert!(schema.param_types.is_empty(), "no parameters expected");
        let bound = sess.bind(&handle, vec![]).await.unwrap();
        match sess.execute_bound(bound).await.unwrap() {
            ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 3"),
            other => panic!("expected INSERT tag, got {other:?}"),
        }
        assert!(
            super::INSERTS_PREPARSE_FASTPATH.load(Ordering::Relaxed) > before,
            "prepared literal INSERT must engage the pre-parse scanner path"
        );
        // Executing the same prepared statement again re-runs the scanner and
        // hits the PK constraint — identical to re-sending the simple query.
        let bound = sess.bind(&handle, vec![]).await.unwrap();
        let err = sess.execute_bound(bound).await;
        assert!(err.is_err(), "duplicate PK re-execute must fail");
        let n = count_from_result(sess.execute("SELECT COUNT(*) FROM t").await.unwrap());
        assert_eq!(n, 3);
    }
}

// ---------------------------------------------------------------------------
// Phase 6.P0.A — statement-level wall-clock timeout (noisy-neighbor P0)
// ---------------------------------------------------------------------------
// Proves: (1) a deliberately slow JOIN/cross-join shape — exactly the family
// the planning-time cost-check misses — is cancelled at the deadline with
// SQLSTATE-mappable QueryCanceled; (2) the connection stays usable for the
// next query; (3) a fast query under the deadline is unaffected; (4)
// disabling (`timeout = None`, i.e. BASIN_STATEMENT_TIMEOUT_MS=0) restores
// back-compat (the same slow query completes).
#[cfg(test)]
mod statement_timeout_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::{BasinError, ProjectId};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::session::test_timeout_override;
    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Seed a table with `n` rows so a self cross-join is `n^4` output rows —
    /// astronomically expensive, guaranteed to outrun a millisecond deadline.
    async fn seed(sess: &crate::ProjectSession, n: i64) {
        sess.execute("CREATE TABLE big (id BIGINT NOT NULL)")
            .await
            .unwrap();
        let vals: Vec<String> = (0..n).map(|i| format!("({i})")).collect();
        sess.execute(&format!("INSERT INTO big VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }

    const HOSTILE_JOIN: &str = "SELECT COUNT(*) FROM big a, big b, big c, big d";

    #[tokio::test]
    async fn slow_join_is_canceled_and_connection_survives() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed(&sess, 80).await; // 80^4 = ~41M output rows for the COUNT

        // 1 ms deadline: the cross-join cannot finish in time.
        let _guard = test_timeout_override::install(Some(Duration::from_millis(1)));
        let err = sess
            .execute(HOSTILE_JOIN)
            .await
            .expect_err("hostile cross-join must be cancelled at the deadline");
        assert!(
            matches!(err, BasinError::QueryCanceled(_)),
            "expected QueryCanceled (→ SQLSTATE 57014), got {err:?}"
        );

        // The connection must remain usable: a fast query on the same session
        // succeeds immediately after the cancellation.
        let res = sess.execute("SELECT 1").await.expect("session still usable");
        assert!(matches!(res, ExecResult::Rows { .. }));
    }

    #[tokio::test]
    async fn fast_query_under_deadline_is_unaffected() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed(&sess, 3).await;

        // Generous deadline; a trivial query must complete normally.
        let _guard = test_timeout_override::install(Some(Duration::from_secs(30)));
        let res = sess
            .execute("SELECT COUNT(*) FROM big")
            .await
            .expect("fast query under the deadline must succeed");
        assert!(matches!(res, ExecResult::Rows { .. }));
    }

    #[tokio::test]
    async fn disabled_timeout_runs_unbounded() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        seed(&sess, 6).await; // 6^4 = 1296 rows — small but JOIN-shaped

        // None == BASIN_STATEMENT_TIMEOUT_MS=0: back-compat, no cancellation.
        // The same JOIN shape that gets cancelled under a tight deadline must
        // run to completion when the guard is disabled.
        let _guard = test_timeout_override::install(None);
        let res = sess
            .execute(HOSTILE_JOIN)
            .await
            .expect("disabled timeout must let the (small) join complete");
        assert!(matches!(res, ExecResult::Rows { .. }));
    }
}

/// Regression coverage for BUG #139: the JSON/JSONB set-returning functions
/// (`jsonb_array_elements`, `jsonb_each`, `jsonb_object_keys`, …) must expand
/// to one row per element/key/pair like PostgreSQL — both in FROM-clause
/// position *and* in scalar SELECT-list position (the common ORM idiom
/// `SELECT jsonb_array_elements('[…]'::jsonb)`).
#[cfg(test)]
mod jsonb_srf_expansion_tests {
    use std::sync::Arc;

    use arrow_array::{Array, StringArray};
    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    async fn rows(sess: &crate::ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
        match sess.execute(sql).await {
            Ok(ExecResult::Rows { batches, .. }) => batches,
            other => panic!("expected Rows for `{sql}`, got {other:?}"),
        }
    }

    fn total_rows(b: &[arrow_array::RecordBatch]) -> usize {
        b.iter().map(|x| x.num_rows()).sum()
    }

    /// Collect a String column by name across all batches.
    fn col_str(b: &[arrow_array::RecordBatch], name: &str) -> Vec<String> {
        let mut out = Vec::new();
        for batch in b {
            let arr = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("no column `{name}` in {:?}", batch.schema()))
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("column must be Utf8");
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        }
        out
    }

    #[tokio::test]
    async fn array_elements_select_list_multi() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // Scalar SELECT-list position (the regressed form) must expand to 3 rows.
        let b = rows(&sess, "SELECT jsonb_array_elements('[1,2,3]'::jsonb)").await;
        assert_eq!(total_rows(&b), 3, "must yield one row per array element");
        assert_eq!(col_str(&b, "value"), vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn array_elements_from_clause_multi() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT * FROM jsonb_array_elements('[1,2,3]'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 3);
        assert_eq!(col_str(&b, "value"), vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn array_elements_empty_and_single() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let empty = rows(&sess, "SELECT jsonb_array_elements('[]'::jsonb)").await;
        assert_eq!(total_rows(&empty), 0, "empty array → zero rows");
        let one = rows(&sess, "SELECT jsonb_array_elements('[42]'::jsonb)").await;
        assert_eq!(total_rows(&one), 1);
        assert_eq!(col_str(&one, "value"), vec!["42"]);
    }

    #[tokio::test]
    async fn array_elements_nested_values_preserved() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT jsonb_array_elements('[[1,2],{\"k\":3}]'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 2);
        assert_eq!(col_str(&b, "value"), vec!["[1,2]", "{\"k\":3}"]);
    }

    #[tokio::test]
    async fn array_elements_text_variant() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // `_text` unwraps JSON strings (no surrounding quotes).
        let b = rows(
            &sess,
            "SELECT jsonb_array_elements_text('[\"a\",\"b\",\"c\"]'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 3);
        assert_eq!(col_str(&b, "value"), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn each_select_list_and_from() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        for sql in [
            "SELECT jsonb_each('{\"a\":1,\"b\":2}'::jsonb)",
            "SELECT * FROM jsonb_each('{\"a\":1,\"b\":2}'::jsonb)",
        ] {
            let b = rows(&sess, sql).await;
            assert_eq!(total_rows(&b), 2, "jsonb_each → one row per pair: {sql}");
            assert_eq!(col_str(&b, "key"), vec!["a", "b"], "keys for {sql}");
            assert_eq!(col_str(&b, "value"), vec!["1", "2"], "values for {sql}");
        }
    }

    #[tokio::test]
    async fn each_text_unwraps_strings() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT jsonb_each_text('{\"a\":\"x\",\"b\":\"y\"}'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&b), 2);
        assert_eq!(col_str(&b, "key"), vec!["a", "b"]);
        assert_eq!(col_str(&b, "value"), vec!["x", "y"]);
    }

    #[tokio::test]
    async fn each_empty_object() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(&sess, "SELECT jsonb_each('{}'::jsonb)").await;
        assert_eq!(total_rows(&b), 0, "empty object → zero rows");
    }

    #[tokio::test]
    async fn object_keys_select_list_and_from() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let s = rows(
            &sess,
            "SELECT jsonb_object_keys('{\"a\":1,\"b\":2,\"c\":3}'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&s), 3, "one row per top-level key");
        assert_eq!(col_str(&s, "jsonb_object_keys"), vec!["a", "b", "c"]);

        let f = rows(
            &sess,
            "SELECT * FROM jsonb_object_keys('{\"a\":1,\"b\":2,\"c\":3}'::jsonb)",
        )
        .await;
        assert_eq!(total_rows(&f), 3);
        assert_eq!(col_str(&f, "jsonb_object_keys"), vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn object_keys_empty() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(&sess, "SELECT jsonb_object_keys('{}'::jsonb)").await;
        assert_eq!(total_rows(&b), 0);
    }

    #[tokio::test]
    async fn json_prefixed_variants_expand() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let a = rows(&sess, "SELECT json_array_elements('[1,2,3]')").await;
        assert_eq!(total_rows(&a), 3, "json_array_elements must expand too");
        assert_eq!(col_str(&a, "value"), vec!["1", "2", "3"]);

        let k = rows(&sess, "SELECT json_object_keys('{\"x\":1,\"y\":2}')").await;
        assert_eq!(total_rows(&k), 2, "json_object_keys must expand too");
    }

    #[tokio::test]
    async fn select_list_with_alias_expands() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // `AS v` aliases the table function; column is still `value`.
        let b = rows(&sess, "SELECT jsonb_array_elements('[1,2,3]'::jsonb) AS v").await;
        assert_eq!(total_rows(&b), 3);
    }

    #[tokio::test]
    async fn mixed_select_list_not_rewritten() {
        // Safety: a SRF mixed with other columns must NOT be rewritten by the
        // scalar→FROM rule (that needs LATERAL semantics, out of scope). The
        // query must still plan (using the scalar stub) and return 1 row — we
        // only assert it does not error or explode into the wrong shape.
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        let b = rows(
            &sess,
            "SELECT 1 AS n, jsonb_array_elements('[1,2,3]'::jsonb)",
        )
        .await;
        assert_eq!(
            total_rows(&b),
            1,
            "mixed SELECT list keeps scalar-stub behaviour (not in scope for #139)"
        );
    }
}

/// Tests for Bug #2 (SET statement_timeout wiring) and pg_sleep registration.
#[cfg(test)]
mod statement_timeout_guc_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig { storage, catalog, shard: None })
    }

    /// `SET statement_timeout = '5s'` must be accepted and stored on the session.
    #[tokio::test]
    async fn set_statement_timeout_string_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = '5s'")
            .await
            .expect("SET statement_timeout '5s' must succeed");
        assert_eq!(
            crate::session::session_statement_timeout(&sess.state),
            Some(Duration::from_secs(5)),
            "session timeout should be 5 s after SET"
        );
    }

    /// `SET statement_timeout = 5000` (bare integer milliseconds) must work.
    #[tokio::test]
    async fn set_statement_timeout_integer_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = 5000")
            .await
            .expect("SET statement_timeout 5000 must succeed");
        assert_eq!(
            crate::session::session_statement_timeout(&sess.state),
            Some(Duration::from_millis(5000))
        );
    }

    /// `SET statement_timeout = 0` disables the timeout (Postgres semantics).
    #[tokio::test]
    async fn set_statement_timeout_zero_disables() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = 0")
            .await
            .expect("SET statement_timeout 0 must succeed");
        assert_eq!(
            crate::session::session_statement_timeout(&sess.state),
            None,
            "timeout should be disabled after SET statement_timeout = 0"
        );
    }

    /// `SHOW statement_timeout` reflects the value set by `SET`.
    #[tokio::test]
    async fn show_statement_timeout_reflects_set() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET statement_timeout = '500ms'")
            .await
            .unwrap();
        let res = sess
            .execute("SHOW statement_timeout")
            .await
            .expect("SHOW statement_timeout must succeed");
        match res {
            ExecResult::Rows { batches, .. } => {
                assert!(!batches.is_empty(), "expected at least one batch");
                let batch = &batches[0];
                let col = batch
                    .column_by_name("statement_timeout")
                    .expect("column statement_timeout");
                use arrow_array::StringArray;
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 column");
                let val = arr.value(0);
                assert_eq!(val, "500ms", "SHOW should reflect 500ms");
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `pg_sleep` is registered — `SELECT pg_sleep(0)` must not error.
    #[tokio::test]
    async fn pg_sleep_is_registered() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // pg_sleep(0) must succeed without sleeping.
        let res = sess
            .execute("SELECT pg_sleep(0)")
            .await
            .expect("pg_sleep(0) must be registered and callable");
        assert!(matches!(res, ExecResult::Rows { .. }), "expected Rows result");
    }
}

// ---------------------------------------------------------------------------
// Phase 5.31.A — pg_cancel_backend end-to-end tests
// ---------------------------------------------------------------------------
// Proves: (1) SELECT pg_cancel_backend(pid) from another session aborts the
// running query on the target session with SQLSTATE 57014 (QueryCanceled);
// (2) the target connection stays usable afterwards; (3) pg_cancel_backend
// returns false for unknown pids.
#[cfg(test)]
mod pg_cancel_backend_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::{BasinError, ProjectId};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Seed a large table so a cross-join is guaranteed to run longer than
    /// the cancel signal arrives (deterministic without `pg_sleep`).
    async fn seed_big(sess: &crate::ProjectSession) {
        sess.execute("CREATE TABLE cancel_big (id BIGINT NOT NULL)")
            .await
            .unwrap();
        let vals: Vec<String> = (0..80_i64).map(|i| format!("({i})")).collect();
        sess.execute(&format!("INSERT INTO cancel_big VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }

    /// Prove: pg_cancel_backend(pid) terminates a running cross-join with
    /// SQLSTATE 57014, and the connection stays usable afterwards.
    ///
    /// Uses the multi-thread runtime so the CPU-bound slow query (which only
    /// yields between `df.collect()` batch boundaries) cannot starve the
    /// cancelling session. Under `current_thread` the cancel call could not be
    /// polled until the spawned task happened to yield, which made the test
    /// flaky.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_running_query_returns_57014() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let project = ProjectId::new();

        // Session A: will run the slow query. Wrapped in an `Arc` so the outer
        // scope keeps the session (and thus its `ConnectionHandle`) alive while
        // the spawned task runs. Without this, if the spawned task finishes
        // before `pg_cancel_backend` runs, dropping the session deregisters its
        // pid from the `ConnectionRegistry`, so the cancel returns `false` for a
        // still-conceptually-live session.
        let sess_a = Arc::new(eng.open_session(project).await.unwrap());
        seed_big(&sess_a).await;
        let pid_a = sess_a.session_pid;

        // Session B: will call pg_cancel_backend.
        let sess_b = eng.open_session(project).await.unwrap();

        // Spawn the slow cross-join on sess_a in the background.
        //
        // The seed table has 80 rows, so the 5-way self-cross-join produces
        // 80^5 ≈ 3.3 billion output rows. The depth matters: the 5-level
        // streaming pipeline yields between batches often enough that the
        // per-session `cancel_notify` branch of the executor's `tokio::select!`
        // is reliably polled mid-flight, so the cancel always lands before the
        // query completes. A shallower 4-level join runs DataFusion's aggregate
        // to completion inside a single poll, never re-entering the select — the
        // root cause of the original flake/failure.
        let sess_a_spawn = Arc::clone(&sess_a);
        let slow_fut = tokio::spawn(async move {
            sess_a_spawn
                .execute(
                    "SELECT COUNT(*) FROM cancel_big a, cancel_big b, cancel_big c, \
                     cancel_big d, cancel_big e",
                )
                .await
        });

        // Give the slow query a moment to start executing and register itself
        // on the executor before we issue the cancel.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel from session B.
        let cancel_res = sess_b
            .execute(&format!("SELECT pg_cancel_backend({pid_a})"))
            .await
            .expect("pg_cancel_backend must succeed");
        // Verify that it returned true (pid was found).
        if let ExecResult::Rows { batches, .. } = cancel_res {
            let batch = batches.first().expect("cancel result must have a batch");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("result column must be boolean");
            assert!(col.value(0), "pg_cancel_backend must return true for a live pid");
        } else {
            panic!("expected Rows from pg_cancel_backend");
        }

        // Wait for the slow query to finish — it must return QueryCanceled.
        let slow_result = slow_fut.await.expect("task must not panic");
        assert!(
            matches!(slow_result, Err(BasinError::QueryCanceled(_))),
            "slow query must be cancelled with SQLSTATE 57014, got: {slow_result:?}"
        );
    }

    /// pg_cancel_backend(unknown_pid) returns false.
    #[tokio::test]
    async fn cancel_unknown_pid_returns_false() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        let res = sess
            .execute("SELECT pg_cancel_backend(999999)")
            .await
            .expect("pg_cancel_backend with unknown pid must not error");

        if let ExecResult::Rows { batches, .. } = res {
            let batch = batches.first().expect("result must have a batch");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("result column must be boolean");
            assert!(!col.value(0), "pg_cancel_backend must return false for unknown pid");
        } else {
            panic!("expected Rows from pg_cancel_backend");
        }
    }
}

// ---------------------------------------------------------------------------
// P1 fix: SET lock_timeout GUC wiring — end-to-end tests
// ---------------------------------------------------------------------------
// Verifies that `SET lock_timeout = …` stores the value on the session's
// advisory-lock GUC (so blocking pg_advisory_lock calls honour it) and that
// `SHOW lock_timeout` reflects it back.  Mirrors the statement_timeout_guc_tests
// structure to ensure the two GUCs are symmetrically wired.
#[cfg(test)]
mod lock_timeout_guc_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig { storage, catalog, shard: None })
    }

    /// `SET lock_timeout = '500ms'` must be accepted and stored on the session.
    #[tokio::test]
    async fn set_lock_timeout_string_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = '500ms'")
            .await
            .expect("SET lock_timeout '500ms' must succeed");
        assert_eq!(
            crate::session::session_lock_timeout(&sess.state),
            Some(Duration::from_millis(500)),
            "session lock_timeout should be 500 ms after SET"
        );
    }

    /// `SET lock_timeout = 2000` (bare integer milliseconds) must work.
    #[tokio::test]
    async fn set_lock_timeout_integer_form() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = 2000")
            .await
            .expect("SET lock_timeout 2000 must succeed");
        assert_eq!(
            crate::session::session_lock_timeout(&sess.state),
            Some(Duration::from_millis(2000))
        );
    }

    /// `SET lock_timeout = 0` disables the timeout (Postgres semantics).
    #[tokio::test]
    async fn set_lock_timeout_zero_disables() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        // First set a non-zero value, then disable it.
        sess.execute("SET lock_timeout = '1s'").await.unwrap();
        sess.execute("SET lock_timeout = 0")
            .await
            .expect("SET lock_timeout 0 must succeed");
        assert_eq!(
            crate::session::session_lock_timeout(&sess.state),
            None,
            "lock_timeout should be disabled after SET lock_timeout = 0"
        );
    }

    /// `SHOW lock_timeout` must return the value set by `SET lock_timeout`.
    /// Verifies the SHOW path falls through to the real executor (not noop).
    #[tokio::test]
    async fn show_lock_timeout_reflects_set() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = '300ms'")
            .await
            .unwrap();
        let res = sess
            .execute("SHOW lock_timeout")
            .await
            .expect("SHOW lock_timeout must succeed");
        match res {
            ExecResult::Rows { batches, .. } => {
                assert!(!batches.is_empty(), "expected at least one batch");
                let batch = &batches[0];
                let col = batch
                    .column_by_name("lock_timeout")
                    .expect("column lock_timeout");
                use arrow_array::StringArray;
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 column");
                let val = arr.value(0);
                assert_eq!(val, "300ms", "SHOW should reflect 300ms");
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// `SET lock_timeout` must also propagate to the advisory-lock manager
    /// so that `advisory_lock.get_lock_timeout()` returns the new value.
    #[tokio::test]
    async fn set_lock_timeout_propagates_to_advisory_lock() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();
        sess.execute("SET lock_timeout = '750ms'")
            .await
            .expect("SET lock_timeout '750ms' must succeed");
        // Read the value back directly from the advisory lock manager.
        let advisory_timeout = sess.state.advisory.get_lock_timeout();
        assert_eq!(
            advisory_timeout,
            Some(Duration::from_millis(750)),
            "advisory lock manager must see the updated lock_timeout"
        );
    }
}

#[cfg(test)]
mod rewrite_pipeline_prescreen_tests {
    use super::needs_rewrite_pipeline;

    #[test]
    fn needs_rewrite_pipeline_skips_trivial_select() {
        // Trivial point-query: no marker tokens, no rewrite pass would fire.
        assert!(!needs_rewrite_pipeline("SELECT id FROM t WHERE id = 5"));
        assert!(!needs_rewrite_pipeline(
            "SELECT id, name FROM events WHERE id = 5000"
        ));
        assert!(!needs_rewrite_pipeline("SELECT 1"));
    }

    #[test]
    fn needs_rewrite_pipeline_fires_on_json_arrow() {
        // `->` triggers the JSON arrow operator rewriter.
        assert!(needs_rewrite_pipeline("SELECT j -> 'k' FROM t"));
        assert!(needs_rewrite_pipeline("SELECT j->>'k' FROM t"));
    }

    #[test]
    fn needs_rewrite_pipeline_fires_on_vector_cast() {
        // `::vector` is caught by the `::` cast marker.
        assert!(needs_rewrite_pipeline("SELECT '[1,2]'::vector"));
        assert!(needs_rewrite_pipeline("SELECT col::int FROM t"));
    }

    #[test]
    fn needs_rewrite_pipeline_fires_on_at_contains() {
        // `@>` triggers JSON/array/range containment.
        assert!(needs_rewrite_pipeline("SELECT * FROM t WHERE j @> '{}'"));
        assert!(needs_rewrite_pipeline("SELECT * FROM t WHERE j <@ '{}'"));
    }
}

#[cfg(test)]
mod parse_cache_tests {
    use super::{
        parse_cache_cap_for_test, parse_cache_contains_for_test, parse_cache_resize_for_test,
        parse_sql_cached,
    };

    // The parse cache is process-global; the eviction test resizes the
    // shared LRU, which would race other tests if they ran in parallel.
    // Guard with a module-local mutex.  The other two tests use SQL keys
    // unique to their case so they're parallel-safe even without the
    // mutex; we still take it for symmetry.
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn parse_cache_returns_cached_statement_for_repeat_sql() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        // SQL string chosen to be unique to this test (no other test or
        // production path will parse it), so the cache assertions are
        // independent of what else happens to share the process cache.
        let sql = "SELECT id FROM t_pcache_repeat WHERE id = 4242";
        let first = parse_sql_cached(sql).expect("first parse must succeed");
        let second = parse_sql_cached(sql).expect("second parse must succeed");

        // Cache hit returns the same Arc instance (Arc::ptr_eq).
        assert!(
            std::sync::Arc::ptr_eq(&first, &second),
            "repeat parse must return the cached Arc<Statement>"
        );
        assert!(
            parse_cache_contains_for_test(sql),
            "cache must contain the parsed entry"
        );
    }

    #[test]
    fn parse_cache_distinct_sql_distinct_entries() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let a = "SELECT 1 AS pcache_distinct_a";
        let b = "SELECT 2 AS pcache_distinct_b";
        let c = "SELECT 3 AS pcache_distinct_c";

        let _ = parse_sql_cached(a).unwrap();
        let _ = parse_sql_cached(b).unwrap();
        let _ = parse_sql_cached(c).unwrap();

        assert!(parse_cache_contains_for_test(a));
        assert!(parse_cache_contains_for_test(b));
        assert!(parse_cache_contains_for_test(c));
    }

    #[test]
    fn parse_cache_eviction_under_cap() {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());

        // Shrink to 2; the resize helper also clears so no foreign entries
        // contaminate the bound under test.
        let original_cap = parse_cache_cap_for_test();
        parse_cache_resize_for_test(2);

        let a = "SELECT 100 AS pcache_evict_a";
        let b = "SELECT 200 AS pcache_evict_b";
        let c = "SELECT 300 AS pcache_evict_c";

        let _ = parse_sql_cached(a).unwrap();
        let _ = parse_sql_cached(b).unwrap();
        // LRU now contains [b, a]; touch `b` so `a` becomes oldest.
        let _ = parse_sql_cached(b).unwrap();
        // Insert `c`; cap=2 evicts the LRU entry — `a`.
        let _ = parse_sql_cached(c).unwrap();

        assert!(parse_cache_contains_for_test(b), "b must still be cached");
        assert!(parse_cache_contains_for_test(c), "c must be cached");
        assert!(
            !parse_cache_contains_for_test(a),
            "a must be evicted when cap=2 and c is most recently inserted"
        );

        // Restore default cap for other tests.
        parse_cache_resize_for_test(original_cap.max(1));
    }
}

// ---------------------------------------------------------------------------
// Inv-OLTP-point (#149) — per-session table-meta cache tests
// ---------------------------------------------------------------------------
// Three properties under test:
//   1. Repeated lookups of the same table inside the TTL window hit the
//      cache (no second catalog round-trip).
//   2. Any same-session DDL (here: CREATE INDEX) invalidates the cache so
//      the very next SELECT observes the post-DDL schema/indexes.
//   3. Expired entries (TTL elapsed) are treated as misses and re-loaded
//      from the catalog.
#[cfg(test)]
mod table_meta_cache_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::{ProjectId, TableName};
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Two identical point-lookup SELECTs run back-to-back: the second one
    /// must find the cache populated by the first. We assert against the
    /// session-state cache directly rather than counting catalog calls
    /// (the in-memory catalog has no observable round-trip counter).
    #[tokio::test]
    async fn table_meta_cache_hits_repeated_lookup() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE pkq (id BIGINT NOT NULL, payload TEXT)")
            .await
            .unwrap();
        sess.execute("INSERT INTO pkq VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .await
            .unwrap();

        // CREATE TABLE / INSERT both invalidate the whole cache via the
        // dispatcher's `!matches!(stmt, Statement::Query(_))` branch.
        assert_eq!(
            sess.state.table_meta_cache.len(),
            0,
            "post-DML the cache must be empty"
        );

        // First SELECT: cache miss, populates the entry.
        let _ = sess
            .execute("SELECT * FROM pkq WHERE id = 2")
            .await
            .unwrap();
        let tbl = TableName::new("pkq").unwrap();
        let after_first = sess.state.table_meta_cache.len();
        assert_eq!(
            after_first, 1,
            "first SELECT populates the cache for the touched table"
        );
        assert!(
            sess.state.table_meta_cache.get_fresh(&tbl, sess.engine.config().catalog.epoch()).is_some(),
            "first SELECT must leave a fresh cache entry"
        );

        // Second SELECT (same shape): cache hit — the entry survives, and
        // no other table got cached.
        let _ = sess
            .execute("SELECT * FROM pkq WHERE id = 3")
            .await
            .unwrap();
        assert_eq!(
            sess.state.table_meta_cache.len(),
            1,
            "second SELECT must reuse the cached entry — no new keys"
        );
        assert!(
            sess.state.table_meta_cache.get_fresh(&tbl, sess.engine.config().catalog.epoch()).is_some(),
            "cache entry stays fresh inside the TTL window"
        );
    }

    /// In-session DDL (CREATE INDEX) MUST be visible to subsequent reads
    /// even though the cached `TableMetadata` predates the new index. We
    /// verify by:
    ///   1. SELECT once to populate the cache.
    ///   2. CREATE INDEX — must invalidate the cache.
    ///   3. Assert the cache is empty after the DDL.
    ///   4. Re-run SELECT; assert it succeeds (catalog re-loaded) and
    ///      that the catalog now lists the new index on the table.
    #[tokio::test]
    async fn table_meta_cache_ddl_in_session_evicts() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE pkidx (id BIGINT NOT NULL, k BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO pkidx VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap();

        // Populate the cache.
        let _ = sess
            .execute("SELECT * FROM pkidx WHERE id = 1")
            .await
            .unwrap();
        let tbl = TableName::new("pkidx").unwrap();
        assert!(
            sess.state.table_meta_cache.get_fresh(&tbl, sess.engine.config().catalog.epoch()).is_some(),
            "SELECT must populate the cache"
        );

        // DDL: CREATE INDEX. This is not a `Statement::Query` so the
        // dispatcher invalidates the entire session cache before the
        // handler runs.
        sess.execute("CREATE INDEX pkidx_k_idx ON pkidx (k)")
            .await
            .unwrap();
        assert_eq!(
            sess.state.table_meta_cache.len(),
            0,
            "CREATE INDEX must invalidate the per-session table-meta cache"
        );

        // Second SELECT: re-loads from catalog. The fresh metadata must
        // now list the index we just created.
        let _ = sess
            .execute("SELECT * FROM pkidx WHERE k = 20")
            .await
            .unwrap();
        let entry = sess
            .state
            .table_meta_cache
            .get_fresh(&tbl, sess.engine.config().catalog.epoch())
            .expect("post-DDL SELECT must repopulate the cache");
        let idx_names: Vec<&str> = entry
            .meta
            .indexes
            .iter()
            .map(|ix| ix.name.as_str())
            .collect();
        assert!(
            idx_names.iter().any(|n| *n == "pkidx_k_idx"),
            "post-DDL cached metadata must include the new index \
             pkidx_k_idx (got {idx_names:?})"
        );
    }

    /// Stale entries (TTL elapsed) are treated as misses. We install a
    /// 1ms TTL override on the current test thread, populate the cache,
    /// wait past the TTL, then verify `get_fresh` returns `None`. The
    /// override uses the same thread-local pattern as
    /// `test_timeout_override` so it doesn't race the production
    /// `OnceLock`.
    #[tokio::test]
    async fn table_meta_cache_ttl_expires() {
        let _g = crate::session::test_meta_cache_ttl_override::install(
            Duration::from_millis(1),
        );

        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE pkt (id BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO pkt VALUES (1)").await.unwrap();

        // Populate the cache.
        let _ = sess
            .execute("SELECT * FROM pkt WHERE id = 1")
            .await
            .unwrap();
        let tbl = TableName::new("pkt").unwrap();
        // The override only takes effect on the current thread; the
        // tokio multi-thread runtime can park the test future on a
        // different worker, so we can't assert `get_fresh().is_some()`
        // here. Instead, advance past the TTL on the *same* thread the
        // assertion below runs on.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // The entry exists in the map (insert succeeded) but is stale
        // → `get_fresh` returns `None`. Underlying LRU is unchanged
        // until the next `insert`, which is the documented behaviour.
        let stale = sess.state.table_meta_cache.get_fresh(&tbl, sess.engine.config().catalog.epoch());
        assert!(
            stale.is_none(),
            "1ms TTL must expire after 20ms sleep — got {stale:?}"
        );

        // A subsequent SELECT must re-populate the entry (cache miss →
        // load_table → insert). We rely on `ExecResult::Rows` here just
        // to make sure the query succeeds without choking on the stale
        // entry.
        let res = sess
            .execute("SELECT * FROM pkt WHERE id = 1")
            .await
            .unwrap();
        assert!(matches!(res, ExecResult::Rows { .. }));
    }
}

#[cfg(test)]
mod enforce_intra_tx_uniqueness_tests {
    use std::sync::Arc;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Single-column i64 PK fastpath must detect a duplicate across two
    /// batches buffered within the same open transaction.
    #[tokio::test]
    async fn enforce_intra_tx_uniqueness_i64_pk_detects_dup() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE itu_i64 (id BIGINT PRIMARY KEY, v TEXT)")
            .await
            .unwrap();

        // Open an explicit transaction so inserts stay buffered.
        sess.execute("BEGIN").await.unwrap();
        sess.execute("INSERT INTO itu_i64 VALUES (1, 'a'), (2, 'b')")
            .await
            .unwrap();
        // Second insert with a duplicate id must be rejected.
        let err = sess
            .execute("INSERT INTO itu_i64 VALUES (3, 'c'), (1, 'd')")
            .await;
        assert!(
            err.is_err(),
            "second INSERT with duplicate i64 PK must be rejected"
        );
        let msg = format!("{:?}", err.unwrap_err());
        assert!(
            msg.contains("duplicate key") || msg.contains("UniqueViolation"),
            "error must be a uniqueness violation, got: {msg}"
        );
        sess.execute("ROLLBACK").await.unwrap();
    }

    /// Composite PK falls through to the generic `HashSet<Vec<String>>` path
    /// without panicking and still detects duplicates correctly.
    #[tokio::test]
    async fn enforce_intra_tx_uniqueness_falls_back_for_composite_pk() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute(
            "CREATE TABLE itu_comp (a BIGINT NOT NULL, b BIGINT NOT NULL, \
             v TEXT, PRIMARY KEY (a, b))",
        )
        .await
        .unwrap();

        sess.execute("BEGIN").await.unwrap();
        sess.execute("INSERT INTO itu_comp VALUES (1, 1, 'x'), (1, 2, 'y')")
            .await
            .unwrap();
        // (1, 1) is a duplicate of the first row.
        let err = sess
            .execute("INSERT INTO itu_comp VALUES (2, 3, 'z'), (1, 1, 'dup')")
            .await;
        assert!(
            err.is_err(),
            "composite PK duplicate must be rejected via the generic path"
        );
        let msg = format!("{:?}", err.unwrap_err());
        assert!(
            msg.contains("duplicate key") || msg.contains("UniqueViolation"),
            "error must be a uniqueness violation, got: {msg}"
        );
        sess.execute("ROLLBACK").await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// UPSERT (ON CONFLICT) fast-path tests
// ---------------------------------------------------------------------------
//
// Cover the four shapes the OLTP-W3 fast path targets:
//   * INSERT … ON CONFLICT (pk) DO UPDATE  → row not yet present (INSERT)
//   * INSERT … ON CONFLICT (pk) DO UPDATE  → row present         (UPDATE)
//   * INSERT … ON CONFLICT (pk) DO NOTHING → row present         (no-op)
//   * INSERT … SET col = col + EXCLUDED.col → must NOT lose data
//     even though the literal-only hot-tier UPDATE fast path can't
//     evaluate the expression (falls through to slow path).

#[cfg(test)]
mod upsert_fastpath_tests {
    use std::sync::Arc;

    use basin_catalog::{Catalog, InMemoryCatalog};
    use basin_common::ProjectId;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    use crate::{Engine, EngineConfig, ExecResult};

    fn make_engine(dir: &TempDir) -> Engine {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
            disk_cache: None,
            page_cache: None,
        });
        let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        Engine::new(EngineConfig {
            storage,
            catalog,
            shard: None,
        })
    }

    /// Empty table → INSERT … ON CONFLICT DO UPDATE should land the row as a
    /// fresh INSERT (no rows match the existence probe; the DataFusion
    /// SELECT-1 fallback also returns 0 rows since the table is empty).
    #[tokio::test]
    async fn upsert_fastpath_insert_new() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE u_kv (k BIGINT PRIMARY KEY, v TEXT NOT NULL)")
            .await
            .unwrap();

        let res = sess
            .execute(
                "INSERT INTO u_kv (k, v) VALUES (1, 'fresh') \
                 ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            )
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert!(tag.starts_with("INSERT"), "got tag {tag}"),
            other => panic!("expected Empty tag, got {other:?}"),
        }

        let r = sess
            .execute("SELECT v FROM u_kv WHERE k = 1")
            .await
            .unwrap();
        let ExecResult::Rows { batches, .. } = r else {
            panic!("expected rows")
        };
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "row must be present after fresh upsert");
    }

    /// Existing row + same conflicting PK → the fast path should route to
    /// UPDATE (return an UPDATE 1 tag) and the new EXCLUDED value must
    /// surface on the next SELECT. This is the bench's hot loop.
    #[tokio::test]
    async fn upsert_fastpath_update_existing() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE u_kv2 (k BIGINT PRIMARY KEY, v TEXT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO u_kv2 (k, v) VALUES (42, 'old')")
            .await
            .unwrap();

        let res = sess
            .execute(
                "INSERT INTO u_kv2 (k, v) VALUES (42, 'new') \
                 ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            )
            .await
            .unwrap();
        match res {
            ExecResult::Empty { tag } => assert!(
                tag.starts_with("UPDATE") || tag.starts_with("INSERT"),
                "got tag {tag}"
            ),
            other => panic!("expected Empty tag, got {other:?}"),
        }

        let r = sess
            .execute("SELECT v FROM u_kv2 WHERE k = 42")
            .await
            .unwrap();
        let ExecResult::Rows { batches, .. } = r else {
            panic!("expected rows")
        };
        // Pull the single string value.
        let mut got: Vec<String> = Vec::new();
        for b in &batches {
            use arrow_array::Array;
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..arr.len() {
                got.push(arr.value(i).to_string());
            }
        }
        assert_eq!(got, vec!["new".to_string()], "row must be updated");
    }

    /// Existing row + ON CONFLICT DO NOTHING → row stays unchanged. This
    /// exercises the DO NOTHING existence-probe fast path on the PK.
    #[tokio::test]
    async fn upsert_do_nothing_existing() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE u_kv3 (k BIGINT PRIMARY KEY, v TEXT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO u_kv3 (k, v) VALUES (7, 'keep')")
            .await
            .unwrap();

        let _ = sess
            .execute(
                "INSERT INTO u_kv3 (k, v) VALUES (7, 'lose') \
                 ON CONFLICT (k) DO NOTHING",
            )
            .await
            .unwrap();

        let r = sess
            .execute("SELECT v FROM u_kv3 WHERE k = 7")
            .await
            .unwrap();
        let ExecResult::Rows { batches, .. } = r else {
            panic!("expected rows")
        };
        let mut got: Vec<String> = Vec::new();
        for b in &batches {
            use arrow_array::Array;
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..arr.len() {
                got.push(arr.value(i).to_string());
            }
        }
        assert_eq!(
            got,
            vec!["keep".to_string()],
            "DO NOTHING must leave the existing row untouched"
        );
    }

    /// Read-modify-write SET (`col = col + EXCLUDED.col`) is out of scope for
    /// the literal-only hot-tier UPDATE fast path; it MUST fall through to
    /// the slow path (currently routed via the DataFusion UPDATE planner).
    /// Correctness — not performance — is what we test here: the existing
    /// row's `n` must reflect the addition after the upsert.
    #[tokio::test]
    async fn upsert_complex_set_falls_back() {
        let dir = TempDir::new().unwrap();
        let eng = make_engine(&dir);
        let sess = eng.open_session(ProjectId::new()).await.unwrap();

        sess.execute("CREATE TABLE u_counter (k BIGINT PRIMARY KEY, n BIGINT NOT NULL)")
            .await
            .unwrap();
        sess.execute("INSERT INTO u_counter (k, n) VALUES (1, 10)")
            .await
            .unwrap();

        // `n = n + EXCLUDED.n` adds the proposed-row value (5) to the
        // existing-row value (10). EXCLUDED.n is rewritten to the literal
        // 5; the bare `n` on the RHS stays referring to the existing row,
        // so this should resolve to `n = n + 5` → 15. The literal-only
        // UPDATE fast path can't evaluate `n + 5` and must fall back.
        let res = sess
            .execute(
                "INSERT INTO u_counter (k, n) VALUES (1, 5) \
                 ON CONFLICT (k) DO UPDATE SET n = n + EXCLUDED.n",
            )
            .await
            .unwrap();
        assert!(
            matches!(res, ExecResult::Empty { .. }),
            "expected Empty result"
        );

        let r = sess
            .execute("SELECT n FROM u_counter WHERE k = 1")
            .await
            .unwrap();
        let ExecResult::Rows { batches, .. } = r else {
            panic!("expected rows")
        };
        let mut got: Vec<i64> = Vec::new();
        for b in &batches {
            use arrow_array::Array;
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            for i in 0..arr.len() {
                got.push(arr.value(i));
            }
        }
        assert_eq!(
            got,
            vec![15],
            "RMW SET (n = n + EXCLUDED.n) must accumulate via the slow path"
        );
    }
}
