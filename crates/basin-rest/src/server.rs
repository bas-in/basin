//! Axum router + middleware stack + accept loop.
//!
//! ## Layer order
//!
//! Outer-most → inner-most: TraceLayer → CorsLayer → DefaultBodyLimit →
//! handler. The body limit lives inside the CORS layer so a preflight
//! `OPTIONS` doesn't trip the limit unnecessarily.
//!
//! Per-project rate limiting is enforced *inside* the handler (via
//! [`authorize`]) because the rate-limit key is `claims.project_id`, which
//! we don't have until after JWT verification — and JWT verification is the
//! cheapest reliable way to identify the request's owner.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::http::header::{HeaderName, AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Method};
use axum::routing::{any, get, post, Router};
use basin_auth::Claims;
use basin_blob::signing::BlobSigningSecret;
use basin_blob::store::{BlobCatalog, BlobStore, InMemoryBlobCatalog};
use basin_blob::PostgresBlobCatalog;
use basin_catalog::Catalog;
use basin_common::Result;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

// Feature-gated realtime co-mount types. The `RealimeCoMount` struct holds
// the ChannelRegistry + AuthService + optional replay rings needed to build
// the SSE and WS sub-routers that are merged into the main axum app when the
// `realtime` feature is on.
#[cfg(feature = "realtime")]
use basin_realtime::{ChannelRegistry, ReplayRingRegistry};

// ADR 0028 Phase 1: the durable CDC SSE stream is co-mounted on the REST port
// alongside realtime. Gated on the same `realtime` feature (both consume the
// post-commit change-event seam).
#[cfg(feature = "realtime")]
use basin_cdc::CdcSseState;

use crate::errors::ApiError;
use crate::routes::{
    admin as admin_routes, admin_functions as admin_fn_routes,
    admin_projects as admin_projects_routes, auth as auth_routes, data as data_routes,
    fn_handler as fn_handler_routes, inbound as inbound_routes,
    openapi as openapi_routes, rpc as rpc_routes,
    storage as storage_routes, storage_sign as storage_sign_routes,
};
use crate::RestConfig;

/// Shared inner state. Cheap to wrap in `Arc` and pass around handlers.
pub(crate) struct Inner {
    pub(crate) cfg: RestConfig,
    pub(crate) rate_limiter: basin_auth::rate_limit::PerKey,
    /// Blob store. The catalog backend is chosen at construction time:
    ///
    /// - **Postgres** (`PostgresBlobCatalog`) when `BASIN_BLOB_PG_URL` is set
    ///   (or via [`RestService::new_async`]). This is the only correct backend
    ///   for multi-replica deployments — metadata is durable across restarts
    ///   and shared by every replica (closes #54 P0).
    /// - **In-memory** (`InMemoryBlobCatalog`) otherwise — tests and
    ///   single-process dev only. Per-process, lost on restart.
    ///
    /// Held as `Arc<dyn BlobCatalog>` so the runtime choice doesn't leak into
    /// every call site as a generic parameter.
    pub(crate) blob_store: BlobStore<Arc<dyn BlobCatalog>>,
    /// Phase 6.X.D — optional coordinator-handed slice gate for the
    /// per-project `RestQps` cap (ADR 0023). When attached, every authed
    /// request consumes one token from the slice for
    /// `(claims.project_id, RestQps)`. Exhausted slice → 429.
    pub(crate) slice_gate: Option<basin_catalog::SliceGate>,
    /// T-078/079/080: in-process per-project function catalog driving
    /// `/admin/v1/functions/*`. Opaque Wasm-bytes registry — basin-rest does
    /// not parse the bytes; a downstream `FunctionInvoker` impl is what
    /// hands them to `basin_fn::HandlerHarness::new`.
    pub(crate) function_registry: admin_fn_routes::FunctionRegistry,
    /// fn-persist: durable catalog for handler functions (LANGUAGE wasm /
    /// javascript). When `Some`, deploy writes through to the catalog before
    /// updating the in-process registry, and list / exists checks read from
    /// the catalog as source of truth. `None` (tests, catalog-less dev)
    /// preserves the original in-memory-only behaviour.
    pub(crate) fn_catalog: Option<Arc<dyn Catalog>>,
    /// P2-1: dedicated rotatable HMAC-SHA256 secret for blob signed-URL
    /// tokens. Kept separate from the JWT secret so each can be rotated
    /// independently. Seeded from the JWT secret at startup; rotated via
    /// `POST /admin/v1/storage/signing-key/rotate`.
    pub(crate) blob_signing_secret: Arc<BlobSigningSecret>,
    /// Feature 2: optional realtime co-mount (SSE + WS on the REST port).
    /// When set, the SSE router is merged at `/realtime/v1/sse/:project/:table`
    /// and the WS router at `/realtime/v1/ws/:project`.  Absent → those paths
    /// return 404 (same behaviour as before this feature landed).
    #[cfg(feature = "realtime")]
    pub(crate) realtime: Option<RealtimeCoMount>,
    /// ADR 0028 Phase 1: optional durable CDC SSE co-mount. When set, the CDC
    /// stream router is merged at `GET /v1/cdc/:project/stream`. Absent → that
    /// path 404s (same posture as the realtime co-mount).
    #[cfg(feature = "realtime")]
    pub(crate) cdc: Option<CdcCoMount>,
}

/// Feature 2: data needed to build the realtime SSE + WS sub-routers that
/// are merged into the REST app when the `realtime` Cargo feature is on.
/// Constructed by [`RestService::attach_realtime`] and stored in [`Inner`].
#[cfg(feature = "realtime")]
pub struct RealtimeCoMount {
    pub registry: ChannelRegistry,
    pub auth: Arc<basin_auth::AuthService>,
    pub replay_rings: Option<ReplayRingRegistry>,
}

/// ADR 0028 Phase 1: data needed to build the durable CDC SSE sub-router that
/// is merged into the REST app. Constructed by [`RestService::attach_cdc`] and
/// stored in [`Inner`]. Carries the same handles the [`basin_cdc::CdcRingWriter`]
/// sink uses — the engine's [`basin_storage::Storage`] (durable replay), the
/// in-process live-tail registry (fast path), and the auth service.
#[cfg(feature = "realtime")]
pub struct CdcCoMount {
    pub storage: basin_storage::Storage,
    pub live: basin_cdc::LiveRegistry,
    pub auth: Arc<basin_auth::AuthService>,
}

/// Env var holding the Postgres connection string for the durable blob
/// catalog. When set, [`Inner::from_config_async`] backs the blob store with
/// [`PostgresBlobCatalog`]; otherwise it falls back to the in-memory catalog
/// (tests / single-process dev only).
pub(crate) const BLOB_PG_URL_ENV: &str = "BASIN_BLOB_PG_URL";
/// Env var overriding the Postgres schema for blob catalog tables.
pub(crate) const BLOB_PG_SCHEMA_ENV: &str = "BASIN_BLOB_PG_SCHEMA";

impl Inner {
    /// Synchronous constructor — always backs the blob store with the
    /// in-memory catalog. Suitable for tests and single-process dev. For
    /// multi-replica deployments use [`Inner::from_config_async`], which honors
    /// `BASIN_BLOB_PG_URL` and connects a durable Postgres catalog.
    pub(crate) fn from_config(cfg: RestConfig) -> Self {
        let catalog: Arc<dyn BlobCatalog> = InMemoryBlobCatalog::arc();
        Self::build(cfg, catalog)
    }

    /// Async constructor — selects the catalog backend from the environment.
    ///
    /// If `BASIN_BLOB_PG_URL` is set, connects a [`PostgresBlobCatalog`]
    /// (durable, multi-replica safe; closes #54 P0). Otherwise falls back to
    /// the in-memory catalog. Connection / migration failures are surfaced as
    /// an error so a misconfigured deployment fails loudly rather than
    /// silently degrading to per-process metadata.
    pub(crate) async fn from_config_async(cfg: RestConfig) -> Result<Self> {
        let catalog: Arc<dyn BlobCatalog> = match std::env::var(BLOB_PG_URL_ENV) {
            Ok(url) if !url.trim().is_empty() => {
                let pg = match std::env::var(BLOB_PG_SCHEMA_ENV) {
                    Ok(schema) if !schema.trim().is_empty() => {
                        PostgresBlobCatalog::connect_with_schema(&url, &schema).await
                    }
                    _ => PostgresBlobCatalog::connect(&url).await,
                }
                .map_err(|e| {
                    basin_common::BasinError::Internal(format!(
                        "blob catalog connect ({BLOB_PG_URL_ENV}): {e}"
                    ))
                })?;
                tracing::info!("blob catalog: Postgres-backed (durable, multi-replica)");
                Arc::new(pg)
            }
            _ => {
                tracing::warn!(
                    "blob catalog: in-memory (per-process, lost on restart). \
                     Set {BLOB_PG_URL_ENV} for a durable multi-replica catalog."
                );
                InMemoryBlobCatalog::arc()
            }
        };
        Ok(Self::build(cfg, catalog))
    }

    fn build(cfg: RestConfig, blob_catalog: Arc<dyn BlobCatalog>) -> Self {
        // governor's `Quota::per_minute` is the closest fit; we want
        // requests-per-second to translate to "burst N then refill". 60×N per
        // minute matches the requested rate.
        let rate = cfg.rate_limit_per_sec.saturating_mul(60).max(1);

        // Build the blob store from the engine's object_store so blob bytes
        // land in the same backend as engine table data.
        let blob_obj_store = cfg.engine.config().storage.object_store_handle();
        let blob_store = BlobStore::new(Arc::new(blob_catalog), blob_obj_store, None);

        // P2-1: seed the blob signing secret from the JWT secret. This
        // gives a non-trivial initial key without requiring a new config
        // field. The key is immediately rotatable via the admin endpoint.
        let blob_signing_secret = Arc::new(BlobSigningSecret::new(
            cfg.auth.signing_secret(),
        ));

        Self {
            rate_limiter: basin_auth::rate_limit::PerKey::per_minute(rate, "rest_per_project"),
            blob_store,
            cfg,
            slice_gate: None,
            fn_catalog: None,
            function_registry: admin_fn_routes::FunctionRegistry::new(),
            blob_signing_secret,
            #[cfg(feature = "realtime")]
            realtime: None,
            #[cfg(feature = "realtime")]
            cdc: None,
        }
    }

    /// Phase 6.X.D — attach a heartbeat-reconciled slice gate for the
    /// per-project REST QPS cap.
    #[allow(dead_code)]
    pub(crate) fn with_slice_gate(mut self, gate: basin_catalog::SliceGate) -> Self {
        self.slice_gate = Some(gate);
        self
    }

    /// fn-persist: wire the durable function catalog. After this call, deploy
    /// and delete go through the catalog before touching the in-process cache,
    /// and list reads from the catalog as the source of truth.
    pub(crate) fn with_fn_catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.fn_catalog = Some(catalog);
        self
    }
}

pub(crate) async fn serve(
    inner: Arc<Inner>,
    listener: TcpListener,
    shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    let app = router(inner);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.await;
        })
        .await
        .map_err(|e| basin_common::BasinError::Internal(format!("rest serve: {e}")))
}

pub(crate) fn router(inner: Arc<Inner>) -> Router {
    let cors = build_cors(&inner.cfg.cors_origins);
    let body_limit = DefaultBodyLimit::max(inner.cfg.max_body_bytes);

    // Feature 2: build realtime sub-routers before consuming `inner`.
    // Each sub-router has its own `State` type (SseState / WsState) that is
    // independent of `Arc<Inner>`, so `Router::merge` works without any
    // state-type gymnastics. The routers are built here (before `inner` is
    // moved into `.with_state(inner)`) so we can borrow from `inner.realtime`.
    #[cfg(feature = "realtime")]
    let realtime_sse: Option<Router> = inner.realtime.as_ref().map(|rt| {
        match &rt.replay_rings {
            Some(rings) => basin_realtime::sse_router_with_rings(
                rt.registry.clone(),
                rt.auth.clone(),
                rings.clone(),
            ),
            None => basin_realtime::sse_router(rt.registry.clone(), rt.auth.clone()),
        }
    });
    #[cfg(feature = "realtime")]
    let realtime_ws: Option<Router> = inner.realtime.as_ref().map(|rt| {
        match &rt.replay_rings {
            Some(rings) => basin_realtime::ws_router_with_rings(
                rt.registry.clone(),
                rt.auth.clone(),
                rings.clone(),
            ),
            None => basin_realtime::ws_router(rt.registry.clone(), rt.auth.clone()),
        }
    });

    // ADR 0028 Phase 1: build the durable CDC SSE sub-router (own State, like
    // the realtime routers, so `Router::merge` needs no Arc<Inner> threading).
    #[cfg(feature = "realtime")]
    let cdc_router: Option<Router> = inner.cdc.as_ref().map(|c| {
        basin_cdc::cdc_router(CdcSseState {
            storage: c.storage.clone(),
            live: c.live.clone(),
            auth: c.auth.clone(),
        })
    });

    let app = Router::new()
        .route("/rest/v1/_openapi.json", get(openapi_routes::openapi))
        .route("/rest/v1/rpc/:fn_name", post(rpc_routes::post_rpc))
        // Phase 5.11.W2: HTTP-handler function shape (ANY method).
        .route("/fn/v1/:name", any(fn_handler_routes::any_fn_handler))
        .route(
            "/in/:project_id/:name",
            post(inbound_routes::post_inbound_webhook),
        )
        .route(
            "/rest/v1/:table",
            get(data_routes::get_table)
                .post(data_routes::post_table)
                .patch(data_routes::patch_table)
                .delete(data_routes::delete_table),
        )
        .route("/auth/v1/signup", post(auth_routes::signup))
        .route("/auth/v1/signin", post(auth_routes::signin))
        .route("/auth/v1/refresh", post(auth_routes::refresh))
        .route("/auth/v1/signout", post(auth_routes::signout))
        .route("/auth/v1/verify-email", post(auth_routes::verify_email))
        .route("/auth/v1/reset-password", post(auth_routes::reset_password))
        .route(
            "/auth/v1/request-password-reset",
            post(auth_routes::request_password_reset),
        )
        // Project-agnostic email-link login. POST /auth/v1/magic-link body
        // `{email}` → 204; POST /auth/v1/magic-link/consume body `{token}` →
        // tokens. The legacy per-project flow lives in `AuthService` for now.
        .route("/auth/v1/magic-link", post(auth_routes::request_email_link))
        .route(
            "/auth/v1/magic-link/consume",
            post(auth_routes::consume_email_link),
        )
        .route(
            "/auth/v1/api-keys",
            post(auth_routes::create_api_key).get(auth_routes::list_api_keys),
        )
        .route(
            "/auth/v1/api-keys/:id",
            axum::routing::delete(auth_routes::delete_api_key),
        )
        // --- OAuth routes (Phase 5.10.O) ------------------------------------
        .route(
            "/auth/v1/oauth/:provider/authorize",
            get(auth_routes::oauth_authorize),
        )
        .route(
            "/auth/v1/oauth/:provider/callback",
            get(auth_routes::oauth_callback),
        )
        // --- MFA routes (Phase 5.10.M) --------------------------------------
        .route(
            "/auth/v1/factors",
            post(auth_routes::enroll_factor).get(auth_routes::list_factors),
        )
        .route(
            "/auth/v1/factors/:id/verify",
            post(auth_routes::verify_factor),
        )
        .route(
            "/auth/v1/factors/:id/challenge",
            post(auth_routes::begin_challenge),
        )
        .route(
            "/auth/v1/factors/:id/challenge/verify",
            post(auth_routes::verify_challenge),
        )
        .route(
            "/auth/v1/factors/:id",
            axum::routing::delete(auth_routes::unenroll_factor),
        )
        // Operator-grade endpoints: provision per-project pgwire credentials
        // and rotate them. All gated on `claims.is_admin == true` (see
        // `admin_routes::*`).
        .route("/admin/v1/projects", post(admin_routes::provision_project))
        .route(
            "/admin/v1/projects/:pgwire_user/rotate",
            post(admin_routes::rotate_project),
        )
        .route(
            "/admin/v1/projects/:project_id/credentials",
            get(admin_routes::list_project_credentials),
        )
        // T-049 engine-side: cloud-handed BYO bucket registration.
        .route(
            "/admin/v1/projects/:project_id/byo-bucket",
            post(admin_routes::register_byo_bucket),
        )
        // issue #28b: per-project pgwire connection ceiling.
        // POST sets the ceiling (persists + live); GET reads it.
        .route(
            "/admin/v1/projects/:project_id/max-connections",
            post(admin_projects_routes::set_project_max_connections)
                .get(admin_projects_routes::get_project_max_connections),
        )
        // Feature 3 (5.22.D): pg_dump-compatible project export.
        .route(
            "/admin/v1/projects/:project_id/dump",
            get(admin_routes::dump_project),
        )
        // P2-1: blob signing-key rotation (admin-gated).
        .route(
            "/admin/v1/storage/signing-key/rotate",
            post(admin_routes::rotate_blob_signing_key),
        )
        // T-078 / T-079 / T-080 / T-081: basin-fn control API. The Wasm
        // bytes flow through this surface as opaque blobs; an out-of-process
        // wiring (W6's catalog-backed FunctionInvoker) compiles + runs them.
        .route(
            "/admin/v1/functions/deploy",
            post(admin_fn_routes::deploy_function),
        )
        .route(
            "/admin/v1/functions",
            get(admin_fn_routes::list_functions),
        )
        .route(
            "/admin/v1/functions/:name",
            axum::routing::delete(admin_fn_routes::delete_function),
        )
        .route(
            "/admin/v1/functions/:name/logs",
            get(admin_fn_routes::function_logs),
        )
        .route(
            "/admin/v1/functions/:name/cpu-ms",
            get(admin_fn_routes::function_cpu_ms),
        )
        // Feature 4 (5.11.W): invocation log, version history, rollback.
        .route(
            "/admin/v1/functions/:name/invocations",
            get(admin_fn_routes::function_invocations),
        )
        .route(
            "/admin/v1/functions/:name/versions",
            get(admin_fn_routes::function_versions),
        )
        .route(
            "/admin/v1/functions/:name/rollback",
            post(admin_fn_routes::function_rollback),
        )
        .route("/health", get(health))
        // --- Phase 5.17.B: object-storage HTTP surface (ADR 0021) -----------
        // TODO 5.17.B-featuregate: wrap in #[cfg(feature = "storage")] once
        //      ADR 0018 adds the `storage` Cargo feature to basin-server.
        //      For now mounted unconditionally alongside the REST + auth routes.
        .route(
            "/storage/v1/bucket",
            post(storage_routes::create_bucket),
        )
        .route(
            "/storage/v1/bucket/:name",
            get(storage_routes::get_bucket)
                .delete(storage_routes::delete_bucket),
        )
        // Public (no JWT) — must be registered before the wildcard :bucket
        // route so axum's router matches the literal `public` segment first.
        .route(
            "/storage/v1/object/public/:project_id/:bucket/*path",
            get(storage_routes::download_public_object),
        )
        // Phase 5.17.D: signed-URL mint (JWT-gated).
        // Uses the `upload` literal segment to avoid an axum route-param
        // ambiguity with the download path (fixes #55 — :bucket/*path vs
        // :project/:bucket/*path both starting from `sign/` were
        // indistinguishable to axum's router).
        // BREAKING: POST path changed from
        //   /storage/v1/object/sign/:bucket/*path
        // to
        //   /storage/v1/object/sign/upload/:bucket/*path
        .route(
            "/storage/v1/object/sign/upload/:bucket/*path",
            post(storage_sign_routes::sign_object),
        )
        // Phase 5.17.D: signed-URL download (no JWT) — project ID in path so
        // the handler can identify the project without a bearer token.
        // Path is server-minted by sign_object, so no client breakage here.
        .route(
            "/storage/v1/object/sign/:project/:bucket/*path",
            get(storage_sign_routes::download_signed_object),
        )
        // Authenticated single-object routes.
        .route(
            "/storage/v1/object/:bucket/*path",
            post(storage_routes::upload_object)
                .get(storage_routes::download_object)
                .delete(storage_routes::delete_object),
        )
        // List (POST with body) and bulk-delete (DELETE with body) share :bucket
        // but differ in method — axum resolves by method, not path conflict.
        .route(
            "/storage/v1/object/list/:bucket",
            post(storage_routes::list_objects),
        )
        .route(
            "/storage/v1/object/:bucket",
            axum::routing::delete(storage_routes::bulk_delete_objects),
        )
        .layer(body_limit)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(inner);

    // Feature 2: merge the realtime SSE + WS sub-routers (if configured).
    // The sub-routers carry their own `State` (SseState / WsState) so they
    // integrate cleanly via `Router::merge` — no `Arc<Inner>` threading needed.
    #[cfg(feature = "realtime")]
    let app = {
        let app = if let Some(sse) = realtime_sse {
            app.merge(sse)
        } else {
            app
        };
        let app = if let Some(ws) = realtime_ws {
            app.merge(ws)
        } else {
            app
        };
        // ADR 0028 Phase 1: merge the durable CDC SSE router.
        if let Some(cdc) = cdc_router {
            app.merge(cdc)
        } else {
            app
        }
    };

    app
}

async fn health() -> &'static str {
    "ok"
}

/// Build the CORS layer from the configured allowlist.
///
/// We deliberately never emit `*`. An empty allowlist disables CORS — the
/// browser will refuse cross-origin requests, which is the safer default
/// when the operator hasn't said anything.
fn build_cors(origins: &[String]) -> CorsLayer {
    let methods = [
        Method::GET,
        Method::POST,
        Method::PATCH,
        Method::DELETE,
        Method::OPTIONS,
    ];
    let headers = [
        AUTHORIZATION,
        CONTENT_TYPE,
        HeaderName::from_static("prefer"),
    ];

    if origins.is_empty() {
        return CorsLayer::new()
            .allow_origin(AllowOrigin::list(Vec::<HeaderValue>::new()))
            .allow_methods(methods)
            .allow_headers(headers);
    }

    let parsed: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| HeaderValue::from_str(o).ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed))
        .allow_methods(methods)
        .allow_headers(headers)
}

/// Verify the bearer token, run the per-project rate-limit check, and return
/// the parsed claims. All `/rest/v1/*` handlers call this as the first step;
/// it owns the auth + limiter stack.
///
/// The bearer can be a JWT (preferred path) or a long-lived API key. JWTs
/// resolve in-memory; API-key validation hits the auth DB. API-key claims
/// carry an empty roles list and the literal email `<api-key>` so engine
/// code that reads `claims.roles` doesn't see a stale identity.
pub(crate) async fn authorize(
    state: &Arc<Inner>,
    headers: &HeaderMap,
) -> std::result::Result<Claims, ApiError> {
    let auth = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::unauthenticated("missing Authorization header"))?
        .to_str()
        .map_err(|_| ApiError::unauthenticated("Authorization header is not ASCII"))?;
    let token = auth
        .strip_prefix("Bearer ")
        .or_else(|| auth.strip_prefix("bearer "))
        .ok_or_else(|| ApiError::unauthenticated("Authorization must be `Bearer <token>`"))?
        .trim();
    if token.is_empty() {
        return Err(ApiError::unauthenticated("empty bearer token"));
    }
    let claims = match state.cfg.auth.verify_jwt(token) {
        Ok(c) => c,
        // JWT verification failed — fall through to API-key lookup. We
        // don't surface the JWT error here so a caller using an API key
        // doesn't get a misleading "invalid jwt" message.
        Err(_) => match state.cfg.auth.validate_api_key(token).await {
            Ok((project, user)) => Claims {
                project_id: project,
                user_id: user,
                email: "<api-key>".to_string(),
                roles: Vec::new(),
                exp: 0,
                iat: 0,
                is_admin: false,
                aal: basin_auth::jwt::Aal::Aal1,
                amr: Vec::new(),
            },
            Err(e) => {
                return Err(ApiError::unauthenticated(format!(
                    "invalid bearer token: {e}"
                )));
            }
        },
    };

    // Per-project rate limit.
    state
        .rate_limiter
        .check(&claims.project_id.to_string())
        .map_err(|_| ApiError::rate_limited("per-project rate limit exceeded"))?;

    // Phase 6.X.D — heartbeat-reconciled slice gate for RestQps. No-op when
    // slice_gate is None (back-compat) or when the slice has never been set
    // (coordinator silent / project uncapped → `PerKey` is the cap).
    if let Some(gate) = state.slice_gate.as_ref() {
        if gate
            .try_consume(claims.project_id, basin_catalog::CapKind::RestQps, 1)
            .await
            .is_err()
        {
            return Err(ApiError::rate_limited(
                "per-project rate limit exceeded (coordinator-handed slice)",
            ));
        }
    }

    Ok(claims)
}
