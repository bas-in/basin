//! `basin-rest` — PostgREST-compatible HTTP layer for Basin.
//!
//! See [ADR 0006](../../../docs/decisions/0006-rest-api-layer.md) for
//! the full design. Depends on `basin-auth` for JWT verification —
//! REST without auth is a public dump of every tenant's data, not a
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
//! [`basin_auth::Claims::tenant_id`] feeds [`basin_engine::Engine::open_session`]
//! for the request's lifetime. RLS policies (when implemented) consume the
//! `roles[]` claim.
//!
//! `/auth/v1/{signup,signin,refresh,verify-email,reset-password,magic-link}`
//! are thin wrappers over the existing `AuthService` methods so a customer
//! can stand up auth + REST on a single port.
//!
//! ## What's not implemented yet
//!
//! - **PATCH** returns `501 Not Implemented` with code `E_ENGINE_UNSUPPORTED`
//!   until `basin-engine` grows `UPDATE` support.
//! - Embedded resources, stored functions, realtime, GraphQL, and
//!   aggregates are out of v1 (see ADR 0006).
//!
//! ## Hard requirements honored
//!
//! - `#![forbid(unsafe_code)]`.
//! - Every endpoint goes through JWT auth before reaching the engine.
//! - Per-tenant rate limit via `governor`.
//! - Body size capped via `axum::extract::DefaultBodyLimit`.
//! - CORS allowlist is explicit; the bare `*` origin is never emitted.
//! - All errors come back as `{ "code": "...", "message": "..." }` JSON.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::{BasinError, Result};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

mod errors;
mod json;
mod parser;
mod routes;
mod server;

#[cfg(test)]
mod tests;

pub use errors::ErrorCode;

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
    /// Per-tenant rate limit, requests per second. Defaults to 100.
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

    /// Bind, listen, accept until the process is killed. Mirrors
    /// [`basin_router::run`].
    pub async fn run(self) -> Result<()> {
        let bound = self.run_until_bound().await?;
        match bound.join.await {
            Ok(res) => res,
            Err(e) => Err(BasinError::internal(format!("rest accept-loop join: {e}"))),
        }
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
