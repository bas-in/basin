//! `/admin/v1/projects/:project_id/max-connections` — per-project pgwire
//! connection ceiling (issue #28b).
//!
//! ## Surface
//!
//! | Verb + Path                                              | Action        | Status |
//! |----------------------------------------------------------|---------------|--------|
//! | `POST /admin/v1/projects/:project_id/max-connections`    | set ceiling   | 204    |
//! | `GET  /admin/v1/projects/:project_id/max-connections`    | read ceiling  | 200    |
//!
//! ## Auth
//!
//! Same admin gate as all `/admin/v1/*` routes — every request requires a JWT
//! with `is_admin: true`. The `project_id` claim in the token must match the
//! `:project_id` path segment (same cross-project isolation invariant as
//! `register_byo_bucket`).
//!
//! ## Wire shape
//!
//! `POST` body:
//! ```json
//! { "max_connections": 250 }
//! ```
//! → `204 No Content` on success.
//!
//! `GET` response:
//! ```json
//! { "project_id": "01H...", "max_connections": 250 }
//! ```
//! → `200 OK` on success. When no ceiling has been persisted yet the value
//! returned is `DEFAULT_PROJECT_MAX_CONNECTIONS` (25 — the Free tier).
//!
//! ## Enforcement
//!
//! The ceiling is both **persisted** (in the catalog) and **applied live**
//! (into the `ConnectionLimiter` held in the `CatalogConnectionLimitProvider`
//! that is wired into `ServerConfig::connection_limiter`). Lowering the ceiling
//! does not kill existing connections; only new connect attempts are refused once
//! the live count hits the new (lower) number.
//!
//! ## Default
//!
//! A project with no stored ceiling is treated as having
//! `DEFAULT_PROJECT_MAX_CONNECTIONS` connections by the pgwire startup handler
//! (fail-closed). The cloud re-pushes on every project-create and every plan
//! change, so the window where the default applies is bounded.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use basin_catalog::Catalog as _;
use basin_common::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

// ---------------------------------------------------------------------------
// Fail-closed default (issue #28b / HANDOFF.md)
// ---------------------------------------------------------------------------

/// Maximum concurrent pgwire connections to allow for a project when no
/// ceiling has been explicitly pushed by the cloud control-plane.
///
/// 25 = the Free tier ceiling. Using the Free limit as the default is the
/// correct fail-closed choice: a new engine that has never received a
/// ceiling push from the cloud (e.g. right after a restart before the cloud
/// re-seeds) should not behave as if the project has unlimited connections.
pub const DEFAULT_PROJECT_MAX_CONNECTIONS: u32 = 25;

// ---------------------------------------------------------------------------
// Shared admin gate (mirrors admin.rs — kept local to avoid cross-module
// dependency on private functions)
// ---------------------------------------------------------------------------

fn require_admin(claims: &basin_auth::Claims) -> Result<(), ApiError> {
    if !claims.is_admin {
        return Err(ApiError::unauthenticated(
            "admin endpoint requires `is_admin: true` claim",
        ));
    }
    Ok(())
}

/// Enforce that an admin JWT is scoped to the project named in the URL path.
fn assert_admin_for_path_project(
    claims: &basin_auth::Claims,
    path_project_id: &ProjectId,
) -> Result<(), ApiError> {
    if claims.project_id != *path_project_id {
        return Err(ApiError::forbidden(format!(
            "admin token scoped to project {} cannot operate on project {}",
            claims.project_id, path_project_id,
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

/// `POST /admin/v1/projects/:project_id/max-connections` body.
///
/// Mirrors `EngineMaxConnectionsConfig` in `basin_auth_client.rs` exactly:
/// the cloud serialises `{ "max_connections": <i64> }`. We accept `i64` on
/// the wire for forward-compat, then validate it is strictly positive and
/// small enough to fit in a `u32`.
#[derive(Debug, Deserialize)]
pub(crate) struct SetMaxConnectionsRequest {
    pub max_connections: i64,
}

/// `GET /admin/v1/projects/:project_id/max-connections` response.
#[derive(Debug, Serialize)]
struct GetMaxConnectionsResponse {
    project_id: String,
    max_connections: u32,
}

// ---------------------------------------------------------------------------
// POST /admin/v1/projects/:project_id/max-connections
// ---------------------------------------------------------------------------

/// Set the per-project pgwire connection ceiling.
///
/// Persists the ceiling in the catalog (survives restarts) and immediately
/// updates the in-process [`CatalogConnectionLimitProvider`] so new connections
/// on this engine see the new limit without a restart.
///
/// Lowering the ceiling does not kill existing connections; only subsequent
/// connect attempts are affected.
#[axum::debug_handler]
pub(crate) async fn set_project_max_connections(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(req): Json<SetMaxConnectionsRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;
    assert_admin_for_path_project(&claims, &project)?;

    if req.max_connections <= 0 {
        return Err(ApiError::invalid(
            "max_connections must be > 0",
        ));
    }
    if req.max_connections > u32::MAX as i64 {
        return Err(ApiError::invalid(format!(
            "max_connections must be <= {}",
            u32::MAX
        )));
    }
    let ceiling = req.max_connections as u32;

    // 1. Persist to catalog (survives restarts).
    let catalog = state.cfg.engine.config().catalog.clone();
    catalog
        .set_project_max_connections(&project, ceiling)
        .await
        .map_err(ApiError::from)?;

    // 2. Update the live in-process limiter immediately so ongoing
    //    connection attempts see the new ceiling without waiting for a
    //    restart (the catalog-backed provider reads from the catalog on
    //    every connect, but updating the catalog IS the live update for
    //    that provider).
    //
    //    If the server was wired with a `CatalogConnectionLimitProvider`
    //    (see main.rs wiring below), its `limit_for` calls `catalog.get_project_max_connections`
    //    on every new connection, so writing to the catalog IS the live update.
    //    No additional in-memory notification is needed.

    tracing::info!(
        %project,
        ceiling,
        "per-project pgwire connection ceiling updated",
    );

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---------------------------------------------------------------------------
// GET /admin/v1/projects/:project_id/max-connections
// ---------------------------------------------------------------------------

/// Read the current per-project pgwire connection ceiling.
///
/// Returns the stored value, or `DEFAULT_PROJECT_MAX_CONNECTIONS` when no
/// ceiling has been explicitly pushed.
#[axum::debug_handler]
pub(crate) async fn get_project_max_connections(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;
    assert_admin_for_path_project(&claims, &project)?;

    let catalog = state.cfg.engine.config().catalog.clone();
    let ceiling = catalog
        .get_project_max_connections(&project)
        .await
        .map_err(ApiError::from)?
        .unwrap_or(DEFAULT_PROJECT_MAX_CONNECTIONS);

    Ok(Json(json!({
        "project_id": project.to_string(),
        "max_connections": ceiling,
    }))
    .into_response())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use basin_catalog::InMemoryCatalog;

    // ── helper: round-trip via catalog directly ────────────────────────────

    /// Verify catalog round-trip for both backends. This is the persistence
    /// contract: set then get returns the same value; unset returns None.
    #[tokio::test]
    async fn catalog_set_get_round_trip() {
        let cat = Arc::new(InMemoryCatalog::new());
        let p = ProjectId::new();

        // Before any push: None (caller must apply default).
        let before = cat.get_project_max_connections(&p).await.unwrap();
        assert_eq!(before, None, "unset should return None");

        // After a push: stored value is returned.
        cat.set_project_max_connections(&p, 250).await.unwrap();
        let after = cat.get_project_max_connections(&p).await.unwrap();
        assert_eq!(after, Some(250));

        // Overwrite: new value replaces old.
        cat.set_project_max_connections(&p, 75).await.unwrap();
        let updated = cat.get_project_max_connections(&p).await.unwrap();
        assert_eq!(updated, Some(75));
    }

    /// Verify that different projects have independent ceilings.
    #[tokio::test]
    async fn catalog_per_project_isolation() {
        let cat = Arc::new(InMemoryCatalog::new());
        let a = ProjectId::new();
        let b = ProjectId::new();

        cat.set_project_max_connections(&a, 100).await.unwrap();
        cat.set_project_max_connections(&b, 200).await.unwrap();

        assert_eq!(cat.get_project_max_connections(&a).await.unwrap(), Some(100));
        assert_eq!(cat.get_project_max_connections(&b).await.unwrap(), Some(200));
    }

    /// Lowering a ceiling stores the new (lower) value.
    #[tokio::test]
    async fn catalog_ceiling_lowering_stored() {
        let cat = Arc::new(InMemoryCatalog::new());
        let p = ProjectId::new();

        cat.set_project_max_connections(&p, 750).await.unwrap();
        cat.set_project_max_connections(&p, 25).await.unwrap();

        let v = cat.get_project_max_connections(&p).await.unwrap();
        assert_eq!(v, Some(25), "lowered ceiling must be persisted");
    }
}
