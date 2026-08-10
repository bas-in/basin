//! [`FunctionHost`] and [`FunctionCallContext`] — host-side implementation of
//! the four `basin:functions` WIT imports.
//!
//! `FunctionHost` is the `Store` data type; it implements all four generated
//! `Host` traits and carries a `FunctionCallContext` so every import gets
//! access to the query executor, the tracing span, and future context (project
//! id, identity) added in subsequent phases.

use wasmtime::component::HasSelf;
use wasmtime::ResourceLimiter;

use crate::engine::InvocationContext;
use crate::governance::MemoryLimiter;

// Pull the generated host-trait bindings.
use crate::harness::basin::functions::{http, log, query, secret};

// ---------------------------------------------------------------------------
// FunctionCallContext
// ---------------------------------------------------------------------------

/// Per-invocation context threaded into every host-import call.
///
/// Wraps the `InvocationContext` (executor + secrets) and any additional
/// call-scoped fields added in later phases (e.g. project id, caller identity
/// for RLS).
pub struct FunctionCallContext {
    pub invocation: InvocationContext,
}

impl FunctionCallContext {
    pub fn new(invocation: InvocationContext) -> Self {
        Self { invocation }
    }
}

// ---------------------------------------------------------------------------
// FunctionHost — store data that implements all four Host traits
// ---------------------------------------------------------------------------

/// The value stored in every `wasmtime::Store` used by this harness.
///
/// One instance per function invocation (Stores are single-use in W1). The
/// optional `memory_limiter` slot is populated when the harness is built
/// with a [`crate::governance::FunctionGovernance`] — wasmtime's
/// [`wasmtime::Store::limiter`] requires the [`ResourceLimiter`] to live
/// *inside* the store-data type, so the limiter rides along on
/// [`FunctionHost`] and is reached via [`FunctionHost::limiter_mut`].
pub struct FunctionHost {
    pub ctx: FunctionCallContext,
    /// Per-Store linear-memory cap. `None` when the host is built without a
    /// governance attached (legacy path) — in that case the engine-level
    /// `memory_reservation` is the only memory backstop.
    pub memory_limiter: Option<MemoryLimiter>,
}

impl FunctionHost {
    /// Build a host with no per-Store memory limiter. Engine-level
    /// `memory_reservation` is the only memory cap.
    pub fn new(ctx: FunctionCallContext) -> Self {
        Self {
            ctx,
            memory_limiter: None,
        }
    }

    /// Build a host carrying a [`MemoryLimiter`] for the per-Store cap.
    /// Used by [`crate::ComponentHarness`] / [`crate::HandlerHarness`] when
    /// they are constructed with governance.
    pub fn with_limiter(ctx: FunctionCallContext, limiter: MemoryLimiter) -> Self {
        Self {
            ctx,
            memory_limiter: Some(limiter),
        }
    }

    /// Closure-style accessor used by [`wasmtime::Store::limiter`]. Returns
    /// a `&mut dyn ResourceLimiter` to the embedded [`MemoryLimiter`]; if
    /// the host was built without a limiter, an unbounded one is lazily
    /// installed so the closure signature stays satisfied (the engine-level
    /// reservation still caps memory in that case).
    pub fn limiter_mut(&mut self) -> &mut dyn ResourceLimiter {
        if self.memory_limiter.is_none() {
            self.memory_limiter = Some(MemoryLimiter::new(usize::MAX));
        }
        self.memory_limiter.as_mut().expect("just set")
    }
}

// ---------------------------------------------------------------------------
// Trait implementations
// ---------------------------------------------------------------------------

/// `basin:functions/query` — fully wired.
///
/// Calls `InvocationContext::query.exec_sql` and maps the result into the
/// WIT `row` type. RLS is applied inside the executor (which uses the real
/// engine session when wired to basin-engine).
impl query::Host for FunctionHost {
    fn exec(&mut self, sql: String) -> Result<Vec<query::Row>, String> {
        tracing::debug!(sql = %sql, "basin:fn/query exec");
        self.ctx.invocation.query.exec_sql(&sql).map(|rows| {
            rows.into_iter()
                .map(|r| query::Row { columns: r.columns })
                .collect()
        })
    }
}

/// `basin:functions/http` — wired via `InvocationContext::http` (`HttpSend`).
///
/// Marshals the WIT `request` record into `FnHttpRequest`, delegates to the
/// injected `HttpSend` implementation (real: basin_net adapter; test: mock),
/// and marshals the `FnHttpResponse` back to the WIT `response` record.
///
/// The allowlist + rate-limit + body-cap + timeout guardrails live in the
/// `HttpSend` implementation — the host side has zero safety-critical logic.
impl http::Host for FunctionHost {
    fn fetch(&mut self, req: http::Request) -> Result<http::Response, String> {
        use crate::engine::FnHttpRequest;

        tracing::debug!(
            url = %req.url,
            method = %req.method,
            "basin:fn/http fetch"
        );

        let fn_req = FnHttpRequest {
            url: req.url,
            method: req.method,
            headers: req.headers,
            body: req.body,
        };

        let fn_resp = self.ctx.invocation.http.send(&fn_req)?;

        Ok(http::Response {
            status: fn_resp.status,
            headers: fn_resp.headers,
            body: fn_resp.body,
        })
    }
}

/// `basin:functions/log` — fully wired.
///
/// Maps the WIT `level` enum to the corresponding `tracing` macro so logs
/// appear in the normal structured-log stream.
impl log::Host for FunctionHost {
    fn emit(&mut self, lvl: log::Level, msg: String) {
        match lvl {
            log::Level::Trace => tracing::trace!(target: "basin_fn::guest", "{}", msg),
            log::Level::Debug => tracing::debug!(target: "basin_fn::guest", "{}", msg),
            log::Level::Info => tracing::info!(target: "basin_fn::guest", "{}", msg),
            log::Level::Warn => tracing::warn!(target: "basin_fn::guest", "{}", msg),
            log::Level::Error => tracing::error!(target: "basin_fn::guest", "{}", msg),
        }
    }
}

/// `basin:functions/secret` — wired via `InvocationContext::secrets` (`SecretStore`).
///
/// Delegates to the injected `SecretStore` implementation. In production the
/// caller constructs an `InvocationContext` whose `secrets` field wraps a
/// catalog-backed store that decrypts via `basin_auth::oauth::EncryptionProvider`
/// (AES-256-GCM). Tests inject `MockSecretStore` with pre-seeded key-value
/// pairs so the happy path can be exercised without a live catalog.
///
/// Only secrets scoped to the calling project are accessible — the
/// `SecretStore` implementation is responsible for the project boundary
/// (it receives a project-scoped handle from `InvocationContext`).
impl secret::Host for FunctionHost {
    fn get(&mut self, name: String) -> Result<String, String> {
        tracing::debug!(name = %name, "basin:fn/secret get");
        self.ctx.invocation.secrets.get_secret(&name)
    }
}

// ---------------------------------------------------------------------------
// add_to_linker helper
// ---------------------------------------------------------------------------

/// Wire all four host-import interfaces into `linker` for a store whose data
/// type is `FunctionHost`.
///
/// Uses `HasSelf<FunctionHost>` as the `D` type parameter so the generated
/// `add_to_linker` signature is satisfied without a redundant wrapper type.
pub fn add_host_to_linker(
    linker: &mut wasmtime::component::Linker<FunctionHost>,
) -> anyhow::Result<()> {
    query::add_to_linker::<FunctionHost, HasSelf<FunctionHost>>(linker, |s| s)?;
    http::add_to_linker::<FunctionHost, HasSelf<FunctionHost>>(linker, |s| s)?;
    log::add_to_linker::<FunctionHost, HasSelf<FunctionHost>>(linker, |s| s)?;
    secret::add_to_linker::<FunctionHost, HasSelf<FunctionHost>>(linker, |s| s)?;
    Ok(())
}
