//! [`FunctionHost`] and [`FunctionCallContext`] — host-side implementation of
//! the four `basin:functions` WIT imports.
//!
//! `FunctionHost` is the `Store` data type; it implements all four generated
//! `Host` traits and carries a `FunctionCallContext` so every import gets
//! access to the query executor, the tracing span, and future context (project
//! id, identity) added in subsequent phases.

use wasmtime::component::HasSelf;

use crate::engine::InvocationContext;

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
/// One instance per function invocation (Stores are single-use in W1).
pub struct FunctionHost {
    pub ctx: FunctionCallContext,
}

impl FunctionHost {
    pub fn new(ctx: FunctionCallContext) -> Self {
        Self { ctx }
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
        self.ctx
            .invocation
            .query
            .exec_sql(&sql)
            .map(|rows| {
                rows.into_iter()
                    .map(|r| query::Row { columns: r.columns })
                    .collect()
            })
    }
}

/// `basin:functions/http` — stubbed (TODO W1-followup).
///
/// In a follow-up pass this calls `basin_net::HttpClient::send` using the
/// project's allowlist + rate-limit + body-cap + timeout. The basin-net crate
/// already implements all those guards; wiring here is deliberately deferred
/// so this PR commits a solid foundation first.
impl http::Host for FunctionHost {
    fn fetch(&mut self, req: http::Request) -> Result<http::Response, String> {
        // TODO W1-followup: call basin_net::HttpClient::send(project, &req).
        // The allowlist + rate-limit + body-cap + timeout are already
        // implemented in basin-net; this stub is the only gap.
        tracing::warn!(
            url = %req.url,
            method = %req.method,
            "basin:fn/http fetch called — stubbed in W1; returning error"
        );
        Err(format!(
            "basin:fn/http not yet wired \
             (TODO W1-followup: call basin_net::HttpClient for url={})",
            req.url
        ))
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
            log::Level::Info  => tracing::info!(target: "basin_fn::guest", "{}", msg),
            log::Level::Warn  => tracing::warn!(target: "basin_fn::guest", "{}", msg),
            log::Level::Error => tracing::error!(target: "basin_fn::guest", "{}", msg),
        }
    }
}

/// `basin:functions/secret` — stubbed (TODO W1-followup).
///
/// In a follow-up pass this calls
/// `basin_storage::encryption::EncryptionProvider::unwrap_key` to decrypt the
/// project secret. The trait is already defined and stable; wiring it here is
/// deferred for the same reason as http.
impl secret::Host for FunctionHost {
    fn get(&mut self, name: String) -> Result<String, String> {
        // TODO W1-followup: decrypt via EncryptionProvider.
        // basin-storage::encryption::EncryptionProvider::unwrap_key is the
        // entry point; the encrypted secret bytes are stored per-project in
        // the catalog.
        tracing::warn!(
            name = %name,
            "basin:fn/secret get called — stubbed in W1; returning error"
        );
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
