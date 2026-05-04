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
use std::sync::Arc;

use basin_catalog::SnapshotId;
use basin_common::{BasinError, Result, TableName, TenantId};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::SessionContext;
use object_store::path::Path as ObjectPath;
use tokio::sync::Mutex;
use tracing::instrument;
use url::Url;

use crate::convert::schema_ws_to_df;
use crate::{Engine, TenantSession};

/// Synthetic URL we register the storage `ObjectStore` under. The scheme is
/// purely an internal protocol between `basin-engine` and DataFusion; it is
/// never exposed to clients.
pub(crate) const BASIN_URL_BASE: &str = "basin://engine/";

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
}

impl SessionState {
    fn new() -> Self {
        Self {
            snapshots: Mutex::new(HashMap::new()),
            prepared: crate::prepared::PreparedRegistry::new(),
        }
    }
}

#[instrument(skip(engine), fields(tenant = %tenant))]
pub(crate) async fn open(engine: Engine, tenant: TenantId) -> Result<TenantSession> {
    // 1. Idempotent namespace.
    engine
        .config()
        .catalog
        .create_namespace(&tenant)
        .await?;

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
    let mut cfg = datafusion::execution::config::SessionConfig::new()
        .set_str("datafusion.execution.listing_table_ignore_subdirectory", "false")
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

    // Build the URL for this table's data prefix under the engine's
    // synthetic scheme. This must end in `/` so DataFusion treats it as a
    // directory rather than a single file.
    let table_url = build_table_url(&engine.config().storage.root_prefix_handle(), tenant, table);

    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet");

    // Drop any stale registration before re-registering. `deregister_table`
    // returns Ok(None) for the first-time path, which is exactly what we want.
    let _ = ctx.deregister_table(table.as_str());
    ctx.register_listing_table(
        table.as_str(),
        &table_url,
        listing_options,
        Some(df_schema),
        None,
    )
    .await
    .map_err(|e| BasinError::internal(format!("register_listing_table {table}: {e}")))?;

    // Cache the snapshot id for this session's INSERT path.
    state
        .snapshots
        .lock()
        .await
        .insert(table.clone(), meta.current_snapshot);

    Ok(())
}

/// Build `basin://engine/<root?>/tenants/<tenant>/tables/<table>/data/`.
fn build_table_url(
    root: &Option<ObjectPath>,
    tenant: &TenantId,
    table: &TableName,
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
    url.push_str("/data/");
    url
}
