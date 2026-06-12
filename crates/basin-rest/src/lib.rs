//! `basin-rest` — PostgREST-compatible HTTP layer for Basin.
//!
//! See [ADR 0006](../../../docs/decisions/0006-rest-api-layer.md) for
//! the full design. Depends on `basin-auth` for JWT verification —
//! REST without auth is a public dump of every project's data, not a
//! feature.
//!
//! ## v1 endpoints
//!
//! ```text
//! GET    /rest/v1/<table>?<filters>&<order>&<select>&<limit>&<offset>
//! POST   /rest/v1/<table>           (body: JSON object or array)
//! PATCH  /rest/v1/<table>?<filter>  (body: JSON column updates) — gated on UPDATE
//! DELETE /rest/v1/<table>?<filter>                              — gated on DELETE
//! ```
//!
//! ## Filters
//!
//! - `<col>=eq.<value>`        — equality
//! - `<col>=neq.<value>`       — inequality
//! - `<col>=gt.<value>`        — greater-than
//! - `<col>=gte.<value>`       — greater-than-or-equal
//! - `<col>=lt.<value>`        — less-than
//! - `<col>=lte.<value>`       — less-than-or-equal
//! - `<col>=in.(a,b,c)`        — membership
//! - `<col>=is.null` /
//!   `<col>=is.notnull`        — null check
//! - `order=col.desc,c2.asc`   — ordering
//! - `limit=N` / `offset=N`    — pagination
//! - `select=col1,col2`        — column projection
//!
//! ## Auth
//!
//! Every `/rest/v1/*` request carries `Authorization: Bearer <jwt>`. The JWT
//! is verified via [`basin_auth::AuthService::verify_jwt`]; the resulting
//! [`basin_auth::Claims::project_id`] feeds [`basin_engine::Engine::open_session`]
//! for the request's lifetime. RLS policies (when implemented) consume the
//! `roles[]` claim.
//!
//! `/auth/v1/{signup,signin,refresh,verify-email,reset-password,magic-link}`
//! are thin wrappers over the existing `AuthService` methods so a customer
//! can stand up auth + REST on a single port.
//!
//! ## Pagination + streaming (Phase 5.10 ✅)
//!
//! - `GET /rest/v1/<table>?cursor=<token>&limit=<n>` — opaque base64url JSON
//!   cursor `{"after_id": <last_seen_id>}`, joined with the existing filter
//!   set as `WHERE id > <after_id> ORDER BY id ASC LIMIT <n>`. Limit clamps
//!   to `max_page_size`. When `cursor` or `limit` is supplied the response
//!   wraps as `{"rows":[…],"next_cursor":"…"}`; absent both, the bare-array
//!   shape ships unchanged for back-compat. **v0.1 caveat:** cursor only
//!   works for tables whose first column is named `id` and is `BIGINT`.
//! - `?stream=true` (or > 1 MiB / 10_000 rows) switches the response to
//!   `application/x-ndjson` chunked transfer; the cursor token (if any) is
//!   the final NDJSON line keyed `_basin_next_cursor`.
//!
//! ## Arrow IPC transport
//!
//! Clients that send `Accept: application/vnd.apache.arrow.stream` on GET
//! `/rest/v1/<table>` or POST `/rest/v1/rpc/:fn` receive an Arrow IPC stream
//! instead of JSON. The schema is preserved exactly (no JSON round-trip losses
//! for i64 extremes, timestamps, or binary types). Pagination state is in
//! response headers — see [`arrow_ipc`] for the header contract.
//!
//! ## What's not implemented yet
//!
//! - Embedded resources, stored functions, realtime, GraphQL, and
//!   aggregates are out of v1 (see ADR 0006).
//! - OpenAPI: `GET /rest/v1/_openapi.json` ships a per-project 3.0.3
//!   spec generated from the live catalog. Deferred for follow-up:
//!   security schemes (the bearer-JWT requirement), per-operation
//!   query / filter parameter docs, request examples, and a multi-
//!   version `?spec=v0.2` switch.
//!
//! ## What's wired now
//!
//! - **PATCH** is live: `build_update_sql` produces `UPDATE table SET col=val
//!   WHERE …` against the engine's Iceberg copy-on-write `UPDATE` path,
//!   exercised by the `patch_round_trip` integration test.
//!
//! ## Hard requirements honored
//!
//! - `#![forbid(unsafe_code)]`.
//! - Every endpoint goes through JWT auth before reaching the engine.
//! - Per-project rate limit via `governor`.
//! - Body size capped via `axum::extract::DefaultBodyLimit`.
//! - CORS allowlist is explicit; the bare `*` origin is never emitted.
//! - All errors come back as `{ "code": "...", "message": "..." }` JSON.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::{BasinError, Result};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

mod arrow_ipc;
mod errors;
mod json;
mod parser;
mod routes;
mod server;

#[cfg(test)]
mod tests;

pub use errors::ErrorCode;

// Phase 5.11.W2: HTTP-handler function shape. Public re-exports so callers
// (integration tests for W2; W6's catalog-backed runtime later) can install
// a [`FunctionInvoker`] without taking a `pub(crate)` path into `routes`.
pub use routes::fn_handler::{
    set_global_invoker, FunctionInvoker, InvokeRequest, InvokeResponse,
    NoopFunctionInvoker,
};

// Feature 2: realtime co-mount public surface.
#[cfg(feature = "realtime")]
pub use server::RealtimeCoMount;

// #55: per-function log buffer + CPU counter. Public so FunctionInvoker
// implementations (integration tests, W6 catalog-backed runtime) can call
// `append_log` / `add_cpu_ms` after each invocation.
// Feature 4: also re-export invocation log + version history types.
pub use routes::admin_functions::{
    FunctionRegistry, InvocationRecord, LogEntry, VersionRecord, INVOCATION_LOG_CAP,
    LOG_BUFFER_CAP, VERSION_HISTORY_CAP,
};

/// REST server configuration.
///
/// `engine` and `auth` are required and non-optional by design — a REST
/// stack without auth is the largest data-leak class we know how to ship,
/// so the type system refuses to let you do it.
#[derive(Clone)]
pub struct RestConfig {
    /// Address to bind. Use `127.0.0.1:0` in tests to get an ephemeral port.
    pub bind_addr: SocketAddr,
    /// Engine handle. Cheap to clone (`Arc` inside).
    pub engine: basin_engine::Engine,
    /// Auth service. Verifies the bearer JWT on every request.
    pub auth: Arc<basin_auth::AuthService>,
    /// Maximum request body size in bytes. Anything larger gets a 413.
    pub max_body_bytes: usize,
    /// Default `?limit=` for unbounded GET requests.
    pub default_page_size: usize,
    /// Hard cap on `?limit=`. Any larger value is silently clamped.
    pub max_page_size: usize,
    /// Allowed CORS origins. **Never `*`.**
    pub cors_origins: Vec<String>,
    /// Per-project rate limit, requests per second. Defaults to 100.
    pub rate_limit_per_sec: u32,
}

impl RestConfig {
    /// Defaults sized for a wedge customer, not a busy public API. Tweak as
    /// load actually arrives.
    pub fn new(
        bind_addr: SocketAddr,
        engine: basin_engine::Engine,
        auth: Arc<basin_auth::AuthService>,
    ) -> Self {
        Self {
            bind_addr,
            engine,
            auth,
            max_body_bytes: 1 << 20, // 1 MiB
            default_page_size: 100,
            max_page_size: 1000,
            cors_origins: Vec::new(),
            rate_limit_per_sec: 100,
        }
    }
}

/// REST server handle.
///
/// Cheap to clone — internally an [`Arc`].
#[derive(Clone)]
pub struct RestService {
    inner: Arc<server::Inner>,
}

impl RestService {
    /// Construct a new service.
    ///
    /// **Auth is mandatory.** [`RestConfig`] takes a non-optional
    /// `Arc<AuthService>`; the type system enforces the same invariant the
    /// ADR mandates ("BIND requires AUTH at config time"). If you find
    /// yourself wanting a `None` here, the ADR is the right place to start
    /// the conversation, not the type signature.
    pub fn new(cfg: RestConfig) -> Self {
        Self {
            inner: Arc::new(server::Inner::from_config(cfg)),
        }
    }

    /// Construct a new service, selecting the blob catalog backend from the
    /// environment.
    ///
    /// When `BASIN_BLOB_PG_URL` is set, the blob store is backed by a durable,
    /// multi-replica-safe Postgres catalog ([`basin_blob::PostgresBlobCatalog`]).
    /// Otherwise it falls back to the in-memory catalog (the same backend
    /// [`RestService::new`] always uses), which is per-process and lost on
    /// restart — fine for tests and single-process dev only.
    ///
    /// Prefer this over [`RestService::new`] for any deployment that runs more
    /// than one replica (closes #54 P0). Returns an error if the Postgres URL
    /// is set but the connection or migration fails.
    pub async fn new_async(cfg: RestConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(server::Inner::from_config_async(cfg).await?),
        })
    }

    /// Bind, listen, accept until the process is killed. Mirrors
    /// [`basin_router::run`].
    pub async fn run(self) -> Result<()> {
        let bound = self.run_until_bound().await?;
        match bound.join.await {
            Ok(res) => res,
            Err(e) => Err(BasinError::internal(format!("rest accept-loop join: {e}"))),
        }
    }

    /// Return a shared reference to the blob store so callers can register
    /// RLS policies (Phase 5.17.C) or inspect object metadata in tests.
    ///
    /// The blob store is shared behind the same `Arc<Inner>` as the router.
    pub fn blob_store(
        &self,
    ) -> &basin_blob::store::BlobStore<std::sync::Arc<dyn basin_blob::store::BlobCatalog>> {
        &self.inner.blob_store
    }

    /// Feature 2: attach realtime SSE + WS handlers so clients can reach
    /// `/realtime/v1/sse/:project/:table` and `/realtime/v1/ws/:project`
    /// on the same REST port.
    ///
    /// Must be called **before** [`Self::run_until_bound`]. The returned
    /// `&mut Self` allows chaining: `svc.attach_realtime(…).run_until_bound()`.
    ///
    /// When the `realtime` feature is not compiled in, this method is absent
    /// and callers behind `#[cfg(feature = "realtime")]` compile cleanly.
    #[cfg(feature = "realtime")]
    pub fn attach_realtime(&mut self, mount: RealtimeCoMount) -> &mut Self {
        // `Inner` is behind `Arc`; we need to replace it with a clone that has
        // the realtime field set.  The `Arc::make_mut` would work if `Inner`
        // were `Clone`, but it's not worth adding that.  Instead we use
        // `Arc::try_unwrap` / `get_mut` (guaranteed to succeed if we're the
        // only holder at this point, which is always true pre-`run`).
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.realtime = Some(mount);
        }
        self
    }

    /// Return a clone of the in-process [`FunctionRegistry`].
    ///
    /// Cheap: the registry is an `Arc`-wrapped pair of maps. The clone shares
    /// the same underlying data as the server's copy, so writes via
    /// [`FunctionRegistry::append_log`] / [`FunctionRegistry::add_cpu_ms`]
    /// are immediately visible to the `/admin/v1/functions/:name/logs` and
    /// `/cpu-ms` endpoints.
    ///
    /// Intended for [`FunctionInvoker`] implementations that need to record
    /// per-invocation instrumentation without depending on
    /// `basin-rest`-internal types.
    pub fn function_registry(&self) -> FunctionRegistry {
        self.inner.function_registry.clone()
    }

    /// Bind synchronously (so callers can read `local_addr`), then spawn the
    /// server on a background task. Mirrors [`basin_router::run_until_bound`].
    pub async fn run_until_bound(self) -> Result<RunningRest> {
        let listener = TcpListener::bind(self.inner.cfg.bind_addr)
            .await
            .map_err(|e| {
                BasinError::Internal(format!(
                    "rest bind {} failed: {e}",
                    self.inner.cfg.bind_addr
                ))
            })?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| BasinError::Internal(format!("rest local_addr: {e}")))?;
        let (tx, rx) = oneshot::channel();
        let inner = self.inner.clone();
        let join = tokio::spawn(async move { server::serve(inner, listener, rx).await });
        Ok(RunningRest {
            local_addr,
            shutdown: tx,
            join,
        })
    }
}

/// Handle returned by [`RestService::run_until_bound`]. Drop or send `()`
/// through `shutdown` to ask the server to stop accepting connections; await
/// `join` to learn the final exit status.
pub struct RunningRest {
    pub local_addr: SocketAddr,
    pub shutdown: oneshot::Sender<()>,
    pub join: tokio::task::JoinHandle<Result<()>>,
}
