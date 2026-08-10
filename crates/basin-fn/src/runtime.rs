//! [`FunctionRuntime`] — catalog-aware registry that resolves a function
//! name to a compiled [`HandlerHarness`] and invokes it. Phase 5.11.W6.
//!
//! ## What this module owns
//!
//! `FunctionRuntime` is the call surface W2's HTTP mount uses to run a
//! function by name. It sits between the catalog (which knows the
//! compiled Wasm component bytes + revision counter) and W2's
//! [`HandlerHarness`] (which knows how to instantiate and call into a
//! component implementing the `basin:functions/handler` world). The
//! runtime caches a compiled harness per `(project, name, version)`
//! tuple so a redeploy (which bumps `version` in the catalog) atomically
//! invalidates the cached entry without callers needing to evict.
//!
//! ## Decoupling from the engine
//!
//! The runtime depends on a tiny [`FunctionStore`] trait — *not* on
//! `basin-catalog` or `basin-engine` directly. This keeps `basin-fn`
//! free of a circular dependency with `basin-engine` (which depends on
//! `basin-catalog`, which would otherwise pull `basin-fn` back in
//! transitively for the runtime). Tests inject [`InMemoryFunctionStore`];
//! production wires a thin adapter over `Arc<dyn basin_catalog::Catalog>`.
//!
//! ## Distinct from [`crate::handler::FunctionStore`]
//!
//! W2's [`crate::handler::FunctionStore`] returns a *pre-compiled*
//! `HandlerHarness` along with the per-invocation `InvocationContext`.
//! That trait is the right surface when the REST layer pre-warms a
//! handler at startup (one `.wasm` blob per process). W6's
//! [`FunctionStore`] here is one level lower: it returns the **raw
//! catalog bytes** and lets the runtime cache + recompile on redeploy.
//! Both can coexist — the W2 path remains for cron / always-on routes;
//! the W6 path is for catalog-driven dynamic functions.
//!
//! ## Caller identity / RLS-in-function
//!
//! The `basin:functions/query` host call runs against an
//! [`InvocationContext::query`] that the caller (W2's mount) builds for
//! the JWT-authenticated session. Because the executor is a per-request
//! object built from a `ProjectSession` opened with the caller's
//! `AuthContext`, every `auth.uid()` / `auth.role()` / `auth.aal()`
//! evaluation inside the function resolves to the caller's identity.
//! The runtime itself stays identity-agnostic: it owns the harness
//! cache, not the request context.
//!
//! ## Governance hook
//!
//! Phase 5.11.W5 introduces a `FunctionGovernance` trait (rate limit,
//! quota, allowlist). The runtime accepts an
//! `Option<Arc<dyn FunctionGovernance>>` so the gate is wired without
//! creating a hard dep on W5's not-yet-landed module. W5's governance
//! impl lives in `crates/basin-fn/src/governance.rs` (created by W5,
//! not this PR — see scope discipline in the task).

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use basin_common::ids::ProjectId;
use tokio::sync::RwLock;

use crate::engine::InvocationContext;
use crate::governance::FunctionGovernance as W5Governance;
use crate::handler::{HandlerHarness, HandlerRequest, HandlerResponse};

// ---------------------------------------------------------------------------
// FunctionStore trait — catalog lookup decoupled from basin-catalog
// ---------------------------------------------------------------------------

/// Resolved function record returned by [`FunctionStore::lookup`].
///
/// `body` is the **raw Wasm component bytes** (already base64-decoded);
/// the runtime hands these straight to `HandlerHarness::new` without
/// re-parsing the encoding wrapper. `version` is the catalog's monotonic
/// revision counter — used as part of the cache key so a redeploy bumps
/// the version, the cache miss recompiles, and the previous harness
/// drops when no in-flight call holds it.
#[derive(Clone, Debug)]
pub struct FunctionRecord {
    /// Project that owns the function. Stored for the defensive
    /// `(project, name)`-matches-record invariant check at invoke time.
    pub project: String,
    pub name: String,
    /// Raw Wasm component bytes (base64-decoded by the store adapter).
    /// Handed straight to `HandlerHarness::new`.
    pub body: Vec<u8>,
    /// Catalog revision counter. Bumped on every `CREATE OR REPLACE`;
    /// part of the harness-cache key.
    pub version: i32,
}

/// Boxed future returned by `FunctionStore::lookup`.
///
/// We hand-roll the boxing (rather than use `async_trait`) so this crate
/// doesn't grow a new dev-dep: `Pin<Box<dyn Future + Send>>` is exactly
/// what `async_trait` expands to, just spelled out.
pub type LookupFuture<'a> = Pin<Box<dyn Future<Output = Option<FunctionRecord>> + Send + 'a>>;

/// Trait the runtime calls to find a function by `(project, name)`.
///
/// Object-safe (`Arc<dyn FunctionStore>`) so it can be stored inside a
/// `FunctionRuntime` without generics. Async because the production
/// implementation (a thin wrapper over `basin_catalog::Catalog`) is async.
pub trait FunctionStore: Send + Sync {
    /// Return the function record for `(project, name)`, or `None` if no
    /// such function exists for that project.
    fn lookup<'a>(&'a self, project: &'a str, name: &'a str) -> LookupFuture<'a>;
}

// ---------------------------------------------------------------------------
// Governance hook — wired by W5 if available, no-op otherwise
// ---------------------------------------------------------------------------

/// Boxed future returned by `FunctionGovernance::check`.
pub type GovernanceFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Per-invocation governance gate (Phase 5.11.W5).
///
/// W5 lands the concrete implementation (quotas, rate limits, project
/// allowlist) in `governance.rs`. The runtime accepts an
/// `Option<Arc<dyn FunctionGovernance>>` so it works with or without W5
/// merged. When `Some`, [`FunctionGovernance::check`] is called before
/// every invocation; an `Err` short-circuits the call.
pub trait FunctionGovernance: Send + Sync {
    /// Check whether `(project, name)` may be invoked right now. Returns
    /// `Ok(())` to proceed, `Err(reason)` to reject. The reason is
    /// surfaced verbatim to the caller as the response error.
    fn check<'a>(&'a self, project: &'a str, name: &'a str) -> GovernanceFuture<'a>;
}

// ---------------------------------------------------------------------------
// FunctionRuntime — the actual registry
// ---------------------------------------------------------------------------

/// Outcome of [`FunctionRuntime::invoke`].
#[derive(Debug)]
pub enum InvokeResult {
    /// The function ran to completion. Carries the HTTP-style response
    /// shape the guest returned.
    Ok(HandlerResponse),
    /// The function is not registered for this project.
    NotFound,
    /// Governance rejected the call (rate limit, quota, allowlist).
    Rejected(String),
    /// The component itself trapped, failed to instantiate, or otherwise
    /// errored at the wasmtime layer.
    RuntimeError(String),
}

/// Catalog-aware harness registry.
///
/// One `FunctionRuntime` per process (or per HTTP service handle). The
/// cache is keyed by `(project, name, version)` so concurrent invocations
/// of the same function share a compiled harness, and a redeploy (which
/// bumps `version`) inserts a fresh entry without invalidating in-flight
/// calls.
pub struct FunctionRuntime {
    store: Arc<dyn FunctionStore>,
    governance: Option<Arc<dyn FunctionGovernance>>,
    /// Phase 6.P0.C: W5 resource governance (CPU / memory / wall / project
    /// semaphore / dedicated wasm runtime). When present, every cached
    /// harness is built via [`HandlerHarness::with_governance`] and every
    /// invoke goes through [`HandlerHarness::handle_with`] — that's how the
    /// W5 caps actually fire on the W2 / W6 invoke path.
    caps_governance: Option<Arc<W5Governance>>,
    /// `(project, name, version)` → pre-compiled handler harness. `Arc`
    /// so a concurrent invocation can hold the harness past a cache
    /// eviction.
    cache: RwLock<HashMap<CacheKey, Arc<HandlerHarness>>>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct CacheKey {
    project: String,
    name: String,
    version: i32,
}

impl FunctionRuntime {
    /// Build a runtime backed by `store`, with no governance (always-allow).
    pub fn new(store: Arc<dyn FunctionStore>) -> Self {
        Self {
            store,
            governance: None,
            caps_governance: None,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Build a runtime with both a store and a governance gate (admit/deny).
    pub fn with_governance(
        store: Arc<dyn FunctionStore>,
        governance: Arc<dyn FunctionGovernance>,
    ) -> Self {
        Self {
            store,
            governance: Some(governance),
            caps_governance: None,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Build a runtime that runs every invocation through the W5
    /// [`W5Governance`] (CPU + memory + wall + per-project semaphore +
    /// dedicated wasm runtime). The cached harnesses are constructed via
    /// [`HandlerHarness::with_governance`] so the caps fire on every
    /// `invoke`.
    ///
    /// Phase 6.P0.C: this is the recommended constructor for production —
    /// the older [`Self::new`] / [`Self::with_governance`] paths leave the
    /// caps unenforced on the HTTP handler shape.
    pub fn with_caps_governance(
        store: Arc<dyn FunctionStore>,
        caps_governance: Arc<W5Governance>,
    ) -> Self {
        Self {
            store,
            governance: None,
            caps_governance: Some(caps_governance),
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Invoke `name` in `project` with the provided invocation context
    /// and HTTP-style request.
    ///
    /// The runtime:
    /// 1. Runs the governance check (if configured) and aborts on rejection.
    /// 2. Looks up the function record (the cached harness, if version
    ///    matches; otherwise recompiles).
    /// 3. Calls `handler.handle(ctx, req)` and maps the result.
    ///
    /// `ctx` is the per-call context — built by the caller (W2's mount)
    /// from the request's JWT. The caller's identity flows into
    /// `basin:functions/query` via `ctx.query`, so RLS-gated SQL inside
    /// the function resolves `auth.uid()` etc. to the calling user.
    pub async fn invoke(
        &self,
        project: &str,
        name: &str,
        ctx: InvocationContext,
        req: HandlerRequest,
    ) -> InvokeResult {
        if let Some(gov) = &self.governance {
            if let Err(reason) = gov.check(project, name).await {
                return InvokeResult::Rejected(reason);
            }
        }

        let record = match self.store.lookup(project, name).await {
            Some(r) => r,
            None => return InvokeResult::NotFound,
        };
        // Defensive: the store contract says "lookup by project" but a
        // misimplemented adapter could return a record from a different
        // project. Reject loudly rather than executing the wrong code —
        // this is the load-bearing cross-project isolation check.
        if record.project != project || record.name != name {
            return InvokeResult::RuntimeError(format!(
                "function-store invariant violated: requested ({project},{name}) but \
                 got ({},{})",
                record.project, record.name
            ));
        }

        let harness = match self.harness_for(&record).await {
            Ok(h) => h,
            Err(e) => return InvokeResult::RuntimeError(e),
        };

        // Phase 6.P0.C: when W5 governance is attached, every cap (CPU,
        // memory, wall, per-project semaphore, dedicated wasm runtime) fires
        // through `HandlerHarness::handle_with`. The semaphore key is the
        // typed `ProjectId`. The legacy path (`handle` + the global
        // `spawn_blocking`) is retained only for runtimes constructed via
        // `FunctionRuntime::new` / `with_governance` without caps.
        if self.caps_governance.is_some() {
            let project_id = match project.parse::<ProjectId>() {
                Ok(id) => id,
                Err(e) => {
                    return InvokeResult::RuntimeError(format!(
                        "invalid project id {project:?}: {e}"
                    ))
                }
            };
            return match harness.handle_with(project_id, ctx, req).await {
                Ok(resp) => InvokeResult::Ok(resp),
                Err(e) => InvokeResult::RuntimeError(format!("{e:#}")),
            };
        }

        // `HandlerHarness::handle` is synchronous (the WIT `handle` import
        // is not async in W2's shape). Wrap in `spawn_blocking` so we
        // don't stall the tokio reactor on a CPU-bound guest. Per-call
        // epoch deadline (5 epochs ≈ 5 × BASIN_WASM_EPOCH_MS) is already
        // enforced inside the harness.
        let harness_arc = harness.clone();
        let join = tokio::task::spawn_blocking(move || harness_arc.handle(ctx, req)).await;

        match join {
            Ok(Ok(resp)) => InvokeResult::Ok(resp),
            Ok(Err(e)) => InvokeResult::RuntimeError(format!("{e:#}")),
            Err(join_err) => {
                InvokeResult::RuntimeError(format!("function-runtime join failure: {join_err}"))
            }
        }
    }

    /// Return the cached harness for `record`, compiling on miss.
    ///
    /// Uses a read-lock fast-path; on miss, takes the write-lock and
    /// re-checks (in case a concurrent caller already compiled it).
    async fn harness_for(&self, record: &FunctionRecord) -> Result<Arc<HandlerHarness>, String> {
        let key = CacheKey {
            project: record.project.clone(),
            name: record.name.clone(),
            version: record.version,
        };
        {
            let cache = self.cache.read().await;
            if let Some(h) = cache.get(&key) {
                return Ok(h.clone());
            }
        }
        // Compile outside the lock — `Component::new` is expensive and
        // we don't want to serialise compilation across the cache. When
        // W5 caps governance is attached the harness is compiled against
        // the governance's engine so the per-Engine memory reservation +
        // epoch ticker apply to every invocation.
        let compiled = match &self.caps_governance {
            Some(gov) => HandlerHarness::with_governance(&record.body, gov.clone()),
            None => HandlerHarness::new(&record.body),
        }
        .map_err(|e| format!("compile component for {}: {e:#}", record.name))?;
        let arc = Arc::new(compiled);

        let mut cache = self.cache.write().await;
        // Another caller may have compiled while we were outside the lock.
        if let Some(existing) = cache.get(&key) {
            return Ok(existing.clone());
        }
        // On insert: drop any older-version entry for the same
        // (project, name). The cache otherwise leaks one entry per
        // redeploy. We scan because the map is keyed by version too;
        // for the cache sizes the runtime handles (≪ 10⁴ entries per
        // process) this is fine.
        cache.retain(|k, _| !(k.project == key.project && k.name == key.name));
        cache.insert(key, arc.clone());
        Ok(arc)
    }

    /// Test/debug helper: number of cached harness entries.
    pub async fn cache_size(&self) -> usize {
        self.cache.read().await.len()
    }
}

// ---------------------------------------------------------------------------
// In-memory store — for tests
// ---------------------------------------------------------------------------

/// Trivial in-memory [`FunctionStore`] for tests. Production wires a
/// `basin_catalog::Catalog`-backed adapter.
pub struct InMemoryFunctionStore {
    inner: RwLock<HashMap<(String, String), FunctionRecord>>,
}

impl Default for InMemoryFunctionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryFunctionStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Register / replace a function. Returns the prior record if any.
    pub async fn put(&self, record: FunctionRecord) -> Option<FunctionRecord> {
        let mut map = self.inner.write().await;
        map.insert((record.project.clone(), record.name.clone()), record)
    }

    /// Bump version + replace body for `(project, name)`. Returns the new
    /// version, or `None` if the function did not exist.
    pub async fn redeploy(&self, project: &str, name: &str, body: Vec<u8>) -> Option<i32> {
        let mut map = self.inner.write().await;
        let entry = map.get_mut(&(project.to_string(), name.to_string()))?;
        entry.version = entry.version.saturating_add(1);
        entry.body = body;
        Some(entry.version)
    }
}

impl FunctionStore for InMemoryFunctionStore {
    fn lookup<'a>(&'a self, project: &'a str, name: &'a str) -> LookupFuture<'a> {
        Box::pin(async move {
            let map = self.inner.read().await;
            map.get(&(project.to_string(), name.to_string())).cloned()
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid Wasm **component** (no exports). Enough for
    /// `Component::new` to succeed and for cache-eviction tests that
    /// never actually call `handle`. Full handle-roundtrip tests live in
    /// `component_host_test.rs` with a real WIT-compiled component.
    fn empty_component_bytes() -> Vec<u8> {
        wat::parse_str("(component)").expect("WAT compilation of empty component")
    }

    fn request() -> HandlerRequest {
        HandlerRequest {
            method: "POST".into(),
            path: "/fn/v1/x".into(),
            headers: vec![],
            body: vec![],
        }
    }

    #[tokio::test]
    async fn not_found_when_function_absent() {
        let store: Arc<dyn FunctionStore> = Arc::new(InMemoryFunctionStore::new());
        let rt = FunctionRuntime::new(store);
        let result = rt
            .invoke("p1", "missing", InvocationContext::mock(), request())
            .await;
        assert!(
            matches!(result, InvokeResult::NotFound),
            "expected NotFound, got {result:?}"
        );
    }

    #[tokio::test]
    async fn governance_can_reject() {
        struct DenyAll;
        impl FunctionGovernance for DenyAll {
            fn check<'a>(&'a self, _p: &'a str, _n: &'a str) -> GovernanceFuture<'a> {
                Box::pin(async { Err("over quota".to_string()) })
            }
        }
        let store: Arc<dyn FunctionStore> = Arc::new(InMemoryFunctionStore::new());
        let rt = FunctionRuntime::with_governance(store, Arc::new(DenyAll));
        let result = rt
            .invoke("p1", "anything", InvocationContext::mock(), request())
            .await;
        match result {
            InvokeResult::Rejected(msg) => assert_eq!(msg, "over quota"),
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn project_isolation_invariant_check() {
        // Misbehaving store returns a record for a different project.
        struct WrongProjectStore;
        impl FunctionStore for WrongProjectStore {
            fn lookup<'a>(&'a self, _project: &'a str, name: &'a str) -> LookupFuture<'a> {
                let name = name.to_string();
                Box::pin(async move {
                    Some(FunctionRecord {
                        project: "OTHER".to_string(),
                        name,
                        body: empty_component_bytes(),
                        version: 1,
                    })
                })
            }
        }
        let rt = FunctionRuntime::new(Arc::new(WrongProjectStore));
        let result = rt
            .invoke("p1", "f", InvocationContext::mock(), request())
            .await;
        match result {
            InvokeResult::RuntimeError(msg) => {
                assert!(
                    msg.contains("invariant"),
                    "expected invariant error, got: {msg}"
                );
            }
            other => panic!("expected RuntimeError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn redeploy_bumps_version_and_evicts_cache() {
        let store = Arc::new(InMemoryFunctionStore::new());
        let body_v1 = empty_component_bytes();
        store
            .put(FunctionRecord {
                project: "p1".to_string(),
                name: "f".to_string(),
                body: body_v1.clone(),
                version: 1,
            })
            .await;
        let rt = FunctionRuntime::new(store.clone());

        // First lookup: compile + cache.
        let _ = rt
            .harness_for(&store.lookup("p1", "f").await.unwrap())
            .await
            .expect("compile v1");
        assert_eq!(rt.cache_size().await, 1);

        // Redeploy: version bumps to 2; cache eviction happens on next
        // `harness_for` (lazy), which inserts version=2 and removes
        // version=1.
        let new_version = store
            .redeploy("p1", "f", empty_component_bytes())
            .await
            .unwrap();
        assert_eq!(new_version, 2);

        let _ = rt
            .harness_for(&store.lookup("p1", "f").await.unwrap())
            .await
            .expect("compile v2");
        // Cache size stays at 1: old version evicted, new one inserted.
        assert_eq!(
            rt.cache_size().await,
            1,
            "redeploy must evict the old cached harness"
        );
    }

    #[tokio::test]
    async fn cache_hit_returns_same_arc() {
        let store = Arc::new(InMemoryFunctionStore::new());
        store
            .put(FunctionRecord {
                project: "p1".to_string(),
                name: "f".to_string(),
                body: empty_component_bytes(),
                version: 1,
            })
            .await;
        let rt = FunctionRuntime::new(store.clone());
        let rec = store.lookup("p1", "f").await.unwrap();
        let h1 = rt.harness_for(&rec).await.expect("compile");
        let h2 = rt.harness_for(&rec).await.expect("cache hit");
        assert!(
            Arc::ptr_eq(&h1, &h2),
            "second lookup of same (project,name,version) must reuse the Arc"
        );
    }
}
