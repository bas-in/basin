//! Axum router + middleware stack + accept loop.
//!
//! ## Layer order
//!
//! Outer-most → inner-most: TraceLayer → CorsLayer → DefaultBodyLimit →
//! handler. The body limit lives inside the CORS layer so a preflight
//! `OPTIONS` doesn't trip the limit unnecessarily.
//!
//! Per-tenant rate limiting is enforced *inside* the handler (via
//! [`authorize`]) because the rate-limit key is `claims.tenant_id`, which
//! we don't have until after JWT verification — and JWT verification is the
//! cheapest reliable way to identify the request's owner.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::http::header::{HeaderName, AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Method};
use axum::routing::{get, post, Router};
use basin_auth::Claims;
use basin_common::Result;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::errors::ApiError;
use crate::routes::{
    admin as admin_routes, auth as auth_routes, data as data_routes, openapi as openapi_routes,
};
use crate::RestConfig;

/// Shared inner state. Cheap to wrap in `Arc` and pass around handlers.
pub(crate) struct Inner {
    pub(crate) cfg: RestConfig,
    pub(crate) rate_limiter: basin_auth::rate_limit::PerKey,
}

impl Inner {
    pub(crate) fn from_config(cfg: RestConfig) -> Self {
        // governor's `Quota::per_minute` is the closest fit; we want
        // requests-per-second to translate to "burst N then refill". 60×N per
        // minute matches the requested rate.
        let rate = cfg.rate_limit_per_sec.saturating_mul(60).max(1);
        Self {
            rate_limiter: basin_auth::rate_limit::PerKey::per_minute(rate, "rest_per_tenant"),
            cfg,
        }
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

    Router::new()
        .route("/rest/v1/_openapi.json", get(openapi_routes::openapi))
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
        .route("/auth/v1/verify-email", post(auth_routes::verify_email))
        .route(
            "/auth/v1/reset-password",
            post(auth_routes::reset_password),
        )
        .route(
            "/auth/v1/request-password-reset",
            post(auth_routes::request_password_reset),
        )
        // Tenant-agnostic email-link login. POST /auth/v1/magic-link body
        // `{email}` → 204; POST /auth/v1/magic-link/consume body `{token}` →
        // tokens. The legacy per-tenant flow lives in `AuthService` for now.
        .route(
            "/auth/v1/magic-link",
            post(auth_routes::request_email_link),
        )
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
        // Operator-grade endpoints: provision per-tenant pgwire credentials
        // and rotate them. All gated on `claims.is_admin == true` (see
        // `admin_routes::*`).
        .route("/admin/v1/tenants", post(admin_routes::provision_tenant))
        .route(
            "/admin/v1/tenants/:pgwire_user/rotate",
            post(admin_routes::rotate_tenant),
        )
        .route(
            "/admin/v1/tenants/:tenant_id/credentials",
            get(admin_routes::list_tenant_credentials),
        )
        .route("/health", get(health))
        .layer(body_limit)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(inner)
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

/// Verify the bearer token, run the per-tenant rate-limit check, and return
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
            Ok((tenant, user)) => Claims {
                tenant_id: tenant,
                user_id: user,
                email: "<api-key>".to_string(),
                roles: Vec::new(),
                exp: 0,
                iat: 0,
                is_admin: false,
            },
            Err(e) => {
                return Err(ApiError::unauthenticated(format!(
                    "invalid bearer token: {e}"
                )));
            }
        },
    };

    // Per-tenant rate limit.
    state
        .rate_limiter
        .check(&claims.tenant_id.to_string())
        .map_err(|_| ApiError::rate_limited("per-tenant rate limit exceeded"))?;

    Ok(claims)
}
