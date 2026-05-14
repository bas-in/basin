//! Per-tenant DataFusion session.
//!
//! ## URL convention for the object store
//!
//! DataFusion routes each `ListingTable`'s I/O through whatever
//! `ObjectStore` is registered on the [`SessionContext`]'s `RuntimeEnv`
//! under the URL's `scheme://host` pair. We pick a synthetic scheme,
//! `basin://engine/`, and register `Storage`'s underlying store there once
//! per session.
//!
//! That gives us one URL convention that works identically against
//! `LocalFileSystem` for the PoC and against `AmazonS3` / `GoogleCloudStorage`
//! for production: only the registered store changes; the listing-table URLs
//! the engine constructs do not. The path component carries the configured
//! `root_prefix` (if any) followed by the standard
//! `tenants/{tenant}/tables/{table}/data/` layout.
//!
//! Production note: when this crate moves to native S3 listing, swap the
//! registered store to one whose semantics match `s3://` and then either keep
//! the `basin://` synthetic scheme (simplest) or change paths to `s3://...`.
//! The `register_object_store` call is the single switch point.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::AuthContext;

use basin_catalog::{PartitionSpec, SnapshotId};
use basin_common::{BasinError, Result, TableName, TenantId};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use object_store::path::Path as ObjectPath;
use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio::sync::Mutex;
use tracing::instrument;
use url::Url;

use crate::convert::schema_ws_to_df;
use crate::{Engine, TenantSession};

/// Synthetic URL we register the storage `ObjectStore` under. The scheme is
/// purely an internal protocol between `basin-engine` and DataFusion; it is
/// never exposed to clients.
pub(crate) const BASIN_URL_BASE: &str = "basin://engine/";

/// `OVERRIDING { SYSTEM | USER } VALUE` clause on INSERT. Tracked
/// per-pending-statement because sqlparser 0.52 doesn't recognise the
/// clause; the executor pre-screens it textually and stashes the kind
/// for the INSERT path to consume. See `session::take_pending_overriding`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OverridingKind {
    /// `OVERRIDING SYSTEM VALUE`: bypasses IDENTITY ALWAYS rejection;
    /// user-supplied values are accepted. No effect on BY DEFAULT.
    System,
    /// `OVERRIDING USER VALUE`: discards user-supplied values on BY
    /// DEFAULT IDENTITY columns and fills from nextval instead. No
    /// effect on ALWAYS.
    User,
}

/// Per-session mutable state. The `SessionContext` itself is `Send + Sync`
/// and DataFusion handles concurrency on it; we only need the snapshot cache
/// behind a mutex.
pub(crate) struct SessionState {
    /// Latest snapshot id we've observed for each table this session has
    /// touched. Used to feed the `expected_snapshot` argument of
    /// `append_data_files` without an extra catalog load on every INSERT.
    pub(crate) snapshots: Mutex<HashMap<TableName, SnapshotId>>,
    /// Per-session prepared-statement registry. See `prepared.rs`.
    pub(crate) prepared: crate::prepared::PreparedRegistry,
    /// Tracks whether this tenant has *any* table with a non-trivial
    /// partition spec. The SELECT path uses this as an O(1) gate to skip
    /// the partition-pruning AST walk on every query for tenants that
    /// never used `PARTITION BY`. Initialised at session-open time and
    /// flipped to `true` by `refresh_table` when it sees a partitioned
    /// table.
    pub(crate) has_partitioned_table: std::sync::atomic::AtomicBool,
    /// Per-session sequence "last nextval" cache. PG's `currval`
    /// semantics require this to be per-session: every session has its
    /// own view of "what value was most recently handed out for each
    /// sequence". Empty until the session's first `nextval`; consulted
    /// by the SQL-string sequence rewriter on every `currval` call.
    pub(crate) sequence_cache: Arc<crate::seq_udf::SessionSequenceCache>,
    /// Per-session open-cursor registry. `DECLARE … CURSOR FOR …`
    /// materialises the SELECT result and stores it here under the cursor
    /// name; `FETCH` / `MOVE` advance the position; `CLOSE` removes the
    /// entry.  All cursors are destroyed when the session is dropped
    /// (`CursorRegistry` holds no external state).
    pub(crate) cursors: crate::cursor::CursorRegistry,
    /// Per-session schema registry and search_path. `"public"` is always
    /// present. Updated by `CREATE SCHEMA` / `DROP SCHEMA` /
    /// `SET search_path`. See `crate::schema_ddl::SchemaState`.
    pub(crate) schema_state: Arc<RwLock<crate::schema_ddl::SchemaState>>,
    /// Pending `OVERRIDING { SYSTEM | USER } VALUE` clause stripped from
    /// the current INSERT statement before sqlparser sees it. Set by
    /// `execute()`'s pre-screen, consumed (taken) by `exec_insert`.
    /// `std::sync::Mutex` (not `tokio::sync::Mutex`) so the consumer can
    /// `.take()` synchronously from inside the INSERT path without
    /// crossing an `await`.
    pub(crate) pending_overriding: std::sync::Mutex<Option<OverridingKind>>,
}

impl SessionState {
    fn new() -> Self {
        Self {
            snapshots: Mutex::new(HashMap::new()),
            prepared: crate::prepared::PreparedRegistry::new(),
            has_partitioned_table: std::sync::atomic::AtomicBool::new(false),
            sequence_cache: Arc::new(crate::seq_udf::SessionSequenceCache::default()),
            cursors: crate::cursor::CursorRegistry::new(),
            schema_state: Arc::new(RwLock::new(crate::schema_ddl::SchemaState::default())),
            pending_overriding: std::sync::Mutex::new(None),
        }
    }
}

/// Stash the OVERRIDING kind extracted from the current INSERT
/// statement's source text. Called by the executor pre-screen.
pub(crate) fn set_pending_overriding(state: &SessionState, kind: OverridingKind) {
    *state
        .pending_overriding
        .lock()
        .expect("pending_overriding lock poisoned") = Some(kind);
}

/// Atomically consume any pending OVERRIDING kind. Returns `None`
/// when the user didn't write the clause.
pub(crate) fn take_pending_overriding(state: &SessionState) -> Option<OverridingKind> {
    state
        .pending_overriding
        .lock()
        .expect("pending_overriding lock poisoned")
        .take()
}

#[instrument(skip(engine, current_user, auth_context), fields(tenant = %tenant))]
pub(crate) async fn open(
    engine: Engine,
    tenant: TenantId,
    current_user: String,
    auth_context: Arc<AuthContext>,
) -> Result<TenantSession> {
    // 1. Idempotent namespace.
    engine.config().catalog.create_namespace(&tenant).await?;

    // 2. SessionContext + register the storage's object store under our
    //    synthetic scheme. Recursively descend into the date-and-partition
    //    subdirectories `basin-storage` writes (otherwise DataFusion's
    //    default `listing_table_ignore_subdirectory = true` would skip them).
    //
    //    Noisy-tenant downshift: when this tenant's recent query rate is
    //    over the threshold (see `crate::noisy_detector`), pin
    //    `target_partitions = 1` so its bulk scans stop fanning out
    //    parallel range reads at full strength. This is a cooperative hint
    //    that lets a heavy tenant self-cap; the storage layer's fair-share
    //    scheduler is the real fairness mechanism. We only consult the
    //    detector here (at session-open time): a tenant that becomes noisy
    //    mid-session keeps its current partition count until the next
    //    `open_session`, which is the natural granularity for this kind of
    //    soft throttle.
    // Pin target_partitions=1 by default so per-query Parquet fan-out
    // is bounded. Each query reads its files sequentially via one stream
    // instead of issuing 4–8 concurrent range reads per file. Combined
    // with the storage scheduler's small global budget (default 4),
    // this lets quiet tenants always find a free permit slot within
    // ~one in-flight RPC's duration. Noisy tenants pay a per-query
    // throughput cost (single-threaded reads) — that's the right
    // tradeoff for fairness on bounded-concurrency backends. On AWS S3
    // with effectively unbounded server-side concurrency, raising this
    // back to `num_cpus` is fine; surface as a per-deployment knob in
    // a v0.3 catalog field. The noisy detector still applies — it
    // would catch a hypothetical per-deployment override that bumps
    // partitions back up for a tenant that abuses it.
    let cfg = datafusion::execution::config::SessionConfig::new()
        .set_str(
            "datafusion.execution.listing_table_ignore_subdirectory",
            "false",
        )
        .with_target_partitions(1);
    if engine.is_noisy(&tenant) {
        // Already pinned to 1; keep the log so noisy detection is
        // observable in tracing.
        tracing::info!(
            tenant = %tenant,
            "noisy tenant detected (target_partitions already pinned to 1)"
        );
    }
    let ctx = SessionContext::new_with_config(cfg);
    let url = Url::parse(BASIN_URL_BASE)
        .map_err(|e| BasinError::internal(format!("bad basin url: {e}")))?;
    // Register the *tenant-scoped* store so every range read DataFusion
    // drives for this session counts against the tenant's per-tenant
    // concurrency budget. This is the load-bearing call for in-process
    // tenant fairness on shared object-store backends (real S3 in
    // particular, where the shared reqwest pool would otherwise be
    // saturated by one heavy tenant).
    let store = engine.config().storage.tenant_object_store(&tenant);
    ctx.register_object_store(&url, store);

    // Register the vector distance UDFs once per session. They're stateless
    // and don't depend on per-tenant data; registering on every session is
    // cheap and keeps the UDFs visible to any SQL the session executes.
    crate::udf::register_distance_udfs(&ctx);
    // pgcrypto + uuid-ossp shaped UDFs (gen_random_uuid, digest, encode,
    // decode, crypt, gen_salt). Same per-session shape as the distance
    // UDFs; registration is `Arc<ScalarUDF>::clone` under the hood.
    crate::udf::register_pg_udfs(&ctx);
    // Phase 5.11.A: PG-compat scalar functions — `mod`, `age`, plus
    // `to_char`/`to_timestamp` overrides that accept PG-style format strings
    // (`YYYY-MM-DD HH24:MI:SS`) on top of chrono syntax.
    crate::udf::register_pg_compat_udfs(&ctx);
    // String + datetime gap-fillers: `split_part`, `reverse`, `format`,
    // `quote_ident`, `quote_literal`, `quote_nullable`, `regexp_match`,
    // `regexp_split_to_array`, `chr`, `ascii`, `translate`, `btrim`/`ltrim`/`rtrim`,
    // `convert_from`, `convert_to`, `isfinite`.
    crate::string_dt_udf::register_string_dt_udfs(&ctx);
    // Full-text search stub UDFs: to_tsvector, to_tsquery, ts_rank, ts_headline,
    // setweight, strip, tsvector_length, numnode, querytree, and
    // tsvector_match_udf (the `@@`-operator substitute). All are stubs that
    // return identity/zero/false values; real FTS is deferred to basin-fts.
    crate::fts_udf::register_fts_udfs(&ctx);
    // Auth session functions: `auth_uid()`, `auth_role()`, `auth_jwt()`.
    // These read the per-session AuthContext stamped at open time so RLS
    // policies can reference the authenticated user without re-verifying
    // the JWT on every query. Must be registered after pg_compat to ensure
    // we don't overwrite anything (names are distinct).
    crate::udf::register_auth_udfs(&ctx, auth_context.clone());
<<<<<<< HEAD
<<<<<<< HEAD
    // JSONB scalar UDFs: jsonb_typeof, jsonb_pretty, jsonb_set, jsonb_insert,
    // jsonb_strip_nulls, jsonb_path_query/exists/match, jsonb_object_keys,
    // jsonb_each, jsonb_each_text, jsonb_array_elements[_text],
    // jsonb_build_object, jsonb_build_array, to_jsonb, row_to_json,
    // array_to_json, and aggregate stubs (jsonb_agg, jsonb_object_agg).
<<<<<<< HEAD
    // Extended JSON path, JSON conversion, and JSON operator UDFs.
    crate::jsonb_udf::register_jsonb_udfs(&ctx);
    // Interval arithmetic + time-zone shim UDFs (Phase bulk-interval-tz).
    crate::interval_tz_udf::register_interval_tz_udfs(&ctx);
    // Phase 5.11.B: extended PG scalar inventory — `ceiling`/`sign` aliases,
    // `clock_timestamp`/`transaction_timestamp`/`statement_timestamp`/
    // `localtime`/`localtimestamp` stubs, `make_time`/`make_timestamp`/
    // `make_interval`, `width_bucket`, `to_number`.
    crate::pg_scalar_aliases::register_pg_scalar_aliases(&ctx);
    // Gap-filler string + datetime UDFs: `split_part`, `format`, `quote_ident`,
    // `quote_literal`, `regexp_match`, `regexp_split_to_array`, `chr`, `ascii`,
    // `translate`, `btrim`, `ltrim`, `rtrim`, `convert_from`/`to`, `isfinite`.
    crate::string_dt_udf::register_string_dt_udfs(&ctx);
    // pg_catalog stub UDFs for psql `\dt` / `\d`-family meta-commands.
    crate::pg_catalog_udf::register_pg_catalog_udfs(&ctx);
    // Full-text search stub UDFs: `to_tsvector`, `to_tsquery`, `ts_rank`, etc.
    crate::fts_udf::register_fts_udfs(&ctx);
    // Phase 5.11.AGG: PG JSON aggregate UDAFs — json_agg, jsonb_agg,
    // json_object_agg, jsonb_object_agg.
    crate::pg_agg_udf::register_json_agg_udafs(&ctx);
    crate::jsonb_udf::register_jsonb_udfs(&ctx);
    // Range type constructors + accessors + predicates. Registered per-session
    // like the other UDFs; cost is O(Arc clones).
    crate::range_udf::register_range_udfs(&ctx);
=======
    // Phase 5.11.N: pg_catalog scalar stubs — pg_table_is_visible,
    // pg_get_userbyid, format_type, current_schema(), etc.  These are the
    // functions psql's \dt / \d-family meta-queries probe; registering them
    // as constant-returning stubs is enough for psql to plan and execute
    // without "Invalid function" errors.
    crate::pg_catalog_udf::register_pg_catalog_udfs(&ctx);
>>>>>>> worktree-agent-a64ea2ae9a44cffbc
=======
    // P10: supplemental datetime + array UDFs — overlaps, cast_infinity_timestamp,
    // array_dims. Registered after pg_compat so name collisions resolve in
    // favour of the more-specific implementations above.
    crate::datetime_extras::register_datetime_extras(&ctx);
>>>>>>> worktree-agent-a675cd26807f9dc6b
=======
    // FTS stub UDFs: to_tsvector, to_tsquery, plainto_tsquery,
    // phraseto_tsquery, websearch_to_tsquery, ts_rank, ts_rank_cd,
    // ts_headline, setweight, strip, tsvector_length, numnode, querytree.
    // These are stubs only — no inverted index; rank functions return 0.0,
    // text-conversion functions echo their text argument.
    crate::fts_udf::register_fts_udfs(&ctx);
>>>>>>> worktree-agent-af0b8d7333b8c0693

    // Phase 5.11.M: route `information_schema.tables` and
    // `pg_catalog.pg_class` SELECTs to the tenant-scoped catalog
    // snapshot. The providers hold `Arc<dyn Catalog>` + `TenantId` only;
    // the heavy resource (the catalog handle) is shared across every
    // session, so per-tenant cost is O(bytes).
    crate::info_schema_provider::register_info_schema_providers(
        &ctx,
        engine.config().catalog.clone(),
        tenant,
    )
    .map_err(|e| BasinError::internal(format!("info_schema providers: {e}")))?;

    let state = Arc::new(SessionState::new());

    // 3. Pre-register every table the catalog already knows about. This makes
    //    SELECT work immediately without a per-query refresh.
    let tables = engine.config().catalog.list_tables(&tenant).await?;
    for table in tables {
        refresh_table(&engine, &tenant, &ctx, &state, &table).await?;
    }

    Ok(TenantSession {
        engine,
        tenant,
        current_user,
        auth_context,
        ctx,
        state,
    })
}

/// Re-load a table's catalog metadata and (re-)register it with the
/// `SessionContext`. Called after CREATE / INSERT so subsequent queries see
/// the new state.
///
/// `ListingTable` caches the file list it discovers when it's constructed,
/// which is exactly why we have to throw it away and build a fresh one after
/// every commit.
pub(crate) async fn refresh_table(
    engine: &Engine,
    tenant: &TenantId,
    ctx: &SessionContext,
    state: &Arc<SessionState>,
    table: &TableName,
) -> Result<()> {
    let meta = engine.config().catalog.load_table(tenant, table).await?;
    // The catalog hands us a workspace-version schema; convert into the
    // version DataFusion's `register_listing_table` expects.
    let df_schema = Arc::new(schema_ws_to_df(meta.schema.as_ref())?);

    // Build the URL for this table's hot prefix. Tables with an active
    // tier policy *also* register the cold URL so the tiering compactor's
    // migrations remain transparent to readers; tables without a policy
    // skip that — the cold prefix can never receive files for them, and
    // an extra empty-directory list per query is wasted work on real
    // object stores.
    let root = engine.config().storage.root_prefix_handle();
    let hot_url = build_table_tier_url(&root, tenant, table, "data");
    let has_tier_policy = meta.cold_after_seconds.is_some();
    let cold_url = if has_tier_policy {
        Some(build_table_tier_url(&root, tenant, table, "cold"))
    } else {
        None
    };

    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

    // Drop any stale registration before re-registering. `deregister_table`
    // returns Ok(None) for the first-time path, which is exactly what we want.
    let _ = ctx.deregister_table(table.as_str());

    if let Some(cold_url) = cold_url {
        let urls = vec![
            ListingTableUrl::parse(&hot_url)
                .map_err(|e| BasinError::internal(format!("listing url parse {hot_url}: {e}")))?,
            ListingTableUrl::parse(&cold_url)
                .map_err(|e| BasinError::internal(format!("listing url parse {cold_url}: {e}")))?,
        ];
        let cfg = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(df_schema);
        let provider = ListingTable::try_new(cfg)
            .map_err(|e| BasinError::internal(format!("ListingTable::try_new {table}: {e}")))?;
        ctx.register_table(table.as_str(), Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("register_table {table}: {e}")))?;
    } else {
        ctx.register_listing_table(
            table.as_str(),
            &hot_url,
            listing_options,
            Some(df_schema),
            None,
        )
        .await
        .map_err(|e| BasinError::internal(format!("register_listing_table {table}: {e}")))?;
    }

    // Cache the snapshot id for this session's INSERT path.
    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), meta.current_snapshot);

    if meta.partition_spec.is_partitioned() {
        state
            .has_partitioned_table
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    Ok(())
}

/// Build `basin://engine/<root?>/tenants/<tenant>/tables/<table>/<tier>/`,
/// where `<tier>` is `data` (hot) or `cold`. The trailing slash is critical
/// — DataFusion uses it to distinguish a directory listing from a single-file
/// reference. Tier-aware so `refresh_table` can register both URLs and
/// have the cold-prefix migration land transparently.
fn build_table_tier_url(
    root: &Option<ObjectPath>,
    tenant: &TenantId,
    table: &TableName,
    tier_segment: &str,
) -> String {
    let mut url = String::from(BASIN_URL_BASE);
    if let Some(r) = root {
        let s = r.as_ref();
        if !s.is_empty() {
            url.push_str(s);
            if !url.ends_with('/') {
                url.push('/');
            }
        }
    }
    url.push_str("tenants/");
    url.push_str(&tenant.as_prefix());
    url.push_str("/tables/");
    url.push_str(table.as_str());
    url.push('/');
    url.push_str(tier_segment);
    url.push('/');
    url
}

/// Inspect `sql` for tables with [`PartitionSpec::RangeMonthly`] and, if the
/// query's WHERE clause restricts the partition column to a sub-range of the
/// table's data, replace the registered `ListingTable` with one whose file
/// list is pre-filtered to matching partitions only. On any failure (parse
/// error, no narrowing predicate) we silently fall back to the un-pruned
/// listing — DataFusion's per-file stats pruning still catches what we miss.
pub(crate) async fn apply_partition_pruning_for_query(
    engine: &Engine,
    tenant: &TenantId,
    ctx: &SessionContext,
    sql: &str,
) -> Result<()> {
    let dialect = PostgreSqlDialect {};
    let stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    if stmts.len() != 1 {
        return Ok(());
    }
    let Statement::Query(query) = &stmts[0] else {
        return Ok(());
    };

    let referenced = collect_table_refs(query);
    for table_name in referenced {
        let table = match TableName::new(table_name.clone()) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let meta = match engine.config().catalog.load_table(tenant, &table).await {
            Ok(m) => m,
            Err(_) => continue,
        };
        let column = match &meta.partition_spec {
            PartitionSpec::RangeMonthly { column } => column.clone(),
            _ => continue,
        };
        let Some(range) = extract_range_predicate(query, &column) else {
            continue;
        };

        let storage = engine.config().storage.clone();
        let files = match storage.list_data_files(tenant, &table).await {
            Ok(f) => f,
            Err(_) => continue,
        };
        let matching: Vec<String> = files
            .iter()
            .filter_map(|f| {
                let p = f.path.as_ref();
                let (year, month) = parse_partition_segments(p)?;
                if range.overlaps_year_month(year, month) {
                    Some(p.to_string())
                } else {
                    None
                }
            })
            .collect();
        if matching.is_empty() || matching.len() == files.len() {
            // Either nothing matches (let DF return empty) or everything
            // matches (no point re-registering). Skip the swap.
            continue;
        }
        let _ = register_pruned_listing_table(engine, ctx, &table, &meta.schema, &matching).await;
    }
    Ok(())
}

/// Re-register `table_name` as a `ListingTable` whose `table_paths` is a
/// per-file URL list of `paths`. This bypasses DataFusion's directory scan
/// entirely, so no footer GET is issued for files we already proved
/// irrelevant.
async fn register_pruned_listing_table(
    _engine: &Engine,
    ctx: &SessionContext,
    table: &TableName,
    schema: &arrow_schema::Schema,
    paths: &[String],
) -> Result<()> {
    let df_schema = Arc::new(schema_ws_to_df(schema)?);
    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");

    let mut urls: Vec<ListingTableUrl> = Vec::with_capacity(paths.len());
    for p in paths {
        // Each path comes from `Storage::list_data_files`, which forwards
        // the object_store's `meta.location` — i.e. a *full* bucket-relative
        // key that already includes any configured `root_prefix`. Compose
        // it with the engine's synthetic scheme; do NOT re-prepend the
        // root prefix or it'll appear twice in the resulting URL.
        let mut s = String::from(BASIN_URL_BASE);
        s.push_str(p);
        let url = ListingTableUrl::parse(&s)
            .map_err(|e| BasinError::internal(format!("listing url parse {s}: {e}")))?;
        urls.push(url);
    }

    let cfg = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(listing_options)
        .with_schema(df_schema);
    let provider = ListingTable::try_new(cfg)
        .map_err(|e| BasinError::internal(format!("ListingTable::try_new (pruned): {e}")))?;

    let _ = ctx.deregister_table(table.as_str());
    ctx.register_table(table.as_str(), Arc::new(provider))
        .map_err(|e| BasinError::internal(format!("register_table pruned: {e}")))?;
    Ok(())
}

/// Inclusive month range covering the rows a query may need.
#[derive(Copy, Clone, Debug)]
struct YearMonthRange {
    /// Lowest year-month any matching row may carry, encoded as `year * 12
    /// + month - 1` so comparisons are a single integer compare.
    lo: i32,
    hi: i32,
}

impl YearMonthRange {
    fn from_micros(min_micros: Option<i64>, max_micros: Option<i64>) -> Option<Self> {
        let lo = min_micros.map(year_month_of_micros).unwrap_or(i32::MIN);
        let hi = max_micros.map(year_month_of_micros).unwrap_or(i32::MAX);
        if lo > hi {
            return None;
        }
        Some(Self { lo, hi })
    }

    fn overlaps_year_month(&self, year: i32, month: u32) -> bool {
        let target = encode_year_month(year, month as i32);
        self.lo <= target && target <= self.hi
    }
}

fn encode_year_month(year: i32, month: i32) -> i32 {
    year.saturating_mul(12).saturating_add(month - 1)
}

fn year_month_of_micros(micros: i64) -> i32 {
    let dt = micros_to_datetime(micros);
    encode_year_month(dt.year(), dt.month() as i32)
}

fn micros_to_datetime(micros: i64) -> DateTime<Utc> {
    let secs = micros.div_euclid(1_000_000);
    let sub_us = micros.rem_euclid(1_000_000) as u32;
    Utc.timestamp_opt(secs, sub_us * 1000)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap())
}

/// Parse `…/data/year=YYYY/month=MM/…/x.parquet` to extract the `(year,
/// month)` of a data file. Returns `None` if the path doesn't carry both
/// segments — the engine tolerates mixed partitioning shapes.
fn parse_partition_segments(path: &str) -> Option<(i32, u32)> {
    let mut year: Option<i32> = None;
    let mut month: Option<u32> = None;
    for seg in path.split('/') {
        if let Some(rest) = seg.strip_prefix("year=") {
            year = rest.parse().ok();
        } else if let Some(rest) = seg.strip_prefix("month=") {
            month = rest.parse().ok();
        }
    }
    Some((year?, month?))
}

/// Walk `query`'s top-level FROM list and return each referenced bare table
/// name. We don't follow CTEs, subqueries, or schema-qualified names — the
/// PoC has none of those so the simple form covers our cases.
fn collect_table_refs(query: &Query) -> Vec<String> {
    let mut out = Vec::new();
    if let SetExpr::Select(select) = query.body.as_ref() {
        for from in &select.from {
            if let TableFactor::Table { name, .. } = &from.relation {
                if name.0.len() == 1 {
                    out.push(name.0[0].value.clone());
                }
            }
        }
    }
    out
}

/// Walk the top-level WHERE clause and pull out the tightest min/max range
/// the predicate places on `column`, expressed in microseconds-since-epoch.
/// Returns `None` if the clause leaves the column unconstrained or carries
/// disjunctions / negations we can't reason about safely (for those we
/// fall through to no-pruning).
fn extract_range_predicate(query: &Query, column: &str) -> Option<YearMonthRange> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let where_expr = select.selection.as_ref()?;
    let (min, max) = walk_predicate(where_expr, column);
    YearMonthRange::from_micros(min, max)
}

/// Recursively walk a WHERE expression, ANDing together each leaf's
/// contribution to the running (min, max) bounds. Returns `(None, None)`
/// when no useful information can be extracted; the caller treats that as
/// "skip pruning".
fn walk_predicate(expr: &Expr, column: &str) -> (Option<i64>, Option<i64>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (lmin, lmax) = walk_predicate(left, column);
            let (rmin, rmax) = walk_predicate(right, column);
            (
                merge_min(lmin, rmin, |a, b| a.max(b)),
                merge_min(lmax, rmax, |a, b| a.min(b)),
            )
        }
        Expr::BinaryOp { left, op, right } => {
            // Identify whether one side is the partition column and the
            // other is a literal we can decode.
            let (col_side_left, lit_expr) = if matches_column(left, column) {
                (true, right.as_ref())
            } else if matches_column(right, column) {
                (false, left.as_ref())
            } else {
                return (None, None);
            };
            let Some(micros) = literal_to_micros(lit_expr) else {
                return (None, None);
            };
            // Normalise: rewrite `lit OP col` as `col REVERSE_OP lit`.
            let logical_op = if col_side_left {
                op.clone()
            } else {
                reverse_op(op.clone())
            };
            match logical_op {
                BinaryOperator::Eq => (Some(micros), Some(micros)),
                BinaryOperator::Gt => (Some(micros + 1), None),
                BinaryOperator::GtEq => (Some(micros), None),
                BinaryOperator::Lt => (None, Some(micros - 1)),
                BinaryOperator::LtEq => (None, Some(micros)),
                _ => (None, None),
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            if !matches_column(expr, column) {
                return (None, None);
            }
            let (Some(lo), Some(hi)) = (literal_to_micros(low), literal_to_micros(high)) else {
                return (None, None);
            };
            (Some(lo), Some(hi))
        }
        Expr::Nested(inner) => walk_predicate(inner, column),
        _ => (None, None),
    }
}

fn merge_min(a: Option<i64>, b: Option<i64>, f: impl FnOnce(i64, i64) -> i64) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(f(x, y)),
        (s, None) | (None, s) => s,
    }
}

fn reverse_op(op: BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        other => other,
    }
}

fn matches_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(i) => i.value == column,
        Expr::CompoundIdentifier(parts) => parts.last().is_some_and(|p| p.value == column),
        Expr::Nested(inner) => matches_column(inner, column),
        _ => false,
    }
}

/// Decode the literal forms our partition pruner cares about into
/// microseconds-since-epoch UTC. Mirrors `dml::coerce_timestamp_micros`'s
/// surface but uses a smaller subset (we only need read-side parsing here).
fn literal_to_micros(expr: &Expr) -> Option<i64> {
    let inner: &Expr = match expr {
        Expr::Cast { expr, .. } => expr.as_ref(),
        Expr::Nested(inner) => inner.as_ref(),
        other => other,
    };
    match inner {
        Expr::Value(Value::SingleQuotedString(s))
        | Expr::Value(Value::DoubleQuotedString(s))
        | Expr::Value(Value::EscapedStringLiteral(s))
        | Expr::Value(Value::NationalStringLiteral(s)) => parse_timestamp_string_for_pruning(s),
        Expr::Value(Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    }
}

fn parse_timestamp_string_for_pruning(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&Utc).timestamp_micros());
    }
    let zoned = [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%#z",
    ];
    for fmt in zoned {
        if let Ok(dt) = DateTime::parse_from_str(trimmed, fmt) {
            return Some(dt.with_timezone(&Utc).timestamp_micros());
        }
    }
    let naive = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"];
    for fmt in naive {
        if let Ok(n) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            return Some(DateTime::<Utc>::from_naive_utc_and_offset(n, Utc).timestamp_micros());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let n = d.and_hms_opt(0, 0, 0)?;
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(n, Utc).timestamp_micros());
    }
    None
}
