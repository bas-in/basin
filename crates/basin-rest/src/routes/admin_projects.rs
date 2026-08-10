//! Admin project-scoped control routes.
//!
//! ## Surface
//!
//! | Verb + Path                                              | Action           | Status |
//! |----------------------------------------------------------|------------------|--------|
//! | `POST /admin/v1/projects/:project_id/max-connections`    | set conn ceiling | 204    |
//! | `GET  /admin/v1/projects/:project_id/max-connections`    | read ceiling     | 200    |
//! | `POST /admin/v1/projects/:project_id/placement`          | set home region  | 204    |
//! | `GET  /admin/v1/projects/:project_id/placement`          | read home region | 200    |
//!
//! ## Auth
//!
//! Same admin gate as all `/admin/v1/*` routes — every request requires a JWT
//! with `is_admin: true`. The `project_id` claim in the token must match the
//! `:project_id` path segment (same cross-project isolation invariant as
//! `register_byo_bucket`).
//!
//! ## max-connections wire shape
//!
//! `POST` body:
//! ```json
//! { "max_connections": 250 }
//! ```
//! -> `204 No Content` on success.
//!
//! `GET` response:
//! ```json
//! { "project_id": "01H...", "max_connections": 250 }
//! ```
//! -> `200 OK` on success. When no ceiling has been persisted yet the value
//! returned is `DEFAULT_PROJECT_MAX_CONNECTIONS` (25 -- the Free tier).
//!
//! ## Enforcement (max-connections)
//!
//! The ceiling is both **persisted** (in the catalog) and **applied live**
//! (into the `ConnectionLimiter` held in the `CatalogConnectionLimitProvider`
//! that is wired into `ServerConfig::connection_limiter`). Lowering the ceiling
//! does not kill existing connections; only new connect attempts are refused once
//! the live count hits the new (lower) number.
//!
//! ## Default (max-connections)
//!
//! A project with no stored ceiling is treated as having
//! `DEFAULT_PROJECT_MAX_CONNECTIONS` connections by the pgwire startup handler
//! (fail-closed). The cloud re-pushes on every project-create and every plan
//! change, so the window where the default applies is bounded.
//!
//! ## placement wire shape
//!
//! `POST` body (mirrors `basin_auth_client::set_project_home_region`):
//! ```json
//! { "home_region": "jnb" }
//! ```
//! -> `204 No Content` on success.
//!
//! `GET` response:
//! ```json
//! { "project_id": "01H...", "home_region": "jnb" }
//! ```
//! -> `200 OK` on success. When no home region has been set the response is:
//! ```json
//! { "project_id": "01H...", "home_region": null }
//! ```
//!
//! ## Note -- unified with ProjectMetadata (ADR 0009)
//!
//! The per-project `home_region` here is a *project-level placement pin*
//! persisted as the `home_region` field of [`ProjectMetadata`] (alongside
//! `byo_bucket`). This is the single source of truth the multi-region
//! WrongRegion write gate reads (`basin-engine` `region.rs` via
//! `get_project_metadata`), so setting it here actually gates writes. The
//! `POST` handler does a read-modify-write so persisting the placement pin does
//! not clobber a previously registered BYO-bucket config. It is distinct from
//! the per-table `home_region` field on `TableMetadata` (which pins individual
//! tables).

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
// Shared admin gate (mirrors admin.rs -- kept local to avoid cross-module
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
    // The control-plane super-admin token — scoped to INTERNAL_AUTH_PROJECT_ID —
    // may operate on any project. That is the single deploy-time `is_admin`
    // token the cloud control plane mints to provision and administer every
    // project (see `basin_auth::Claims::is_admin` docs); without this it could
    // create projects (the unscoped `POST /admin/v1/projects` route) but not
    // set their connection ceiling or placement, which are path-scoped.
    if claims.project_id.to_string() == basin_auth::INTERNAL_AUTH_PROJECT_ID {
        return Ok(());
    }
    if claims.project_id != *path_project_id {
        return Err(ApiError::forbidden(format!(
            "admin token scoped to project {} cannot operate on project {}",
            claims.project_id, path_project_id,
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// max-connections: request / response types
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
        return Err(ApiError::invalid("max_connections must be > 0"));
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

    // 2. The in-process `CatalogConnectionLimitProvider` reads from the catalog
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
// Rate limit: POST/GET /admin/v1/projects/:project_id/rate-limit
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub(crate) struct SetRateLimitRequest {
    /// Sustained pgwire statements/sec for this project (per-tier cap). Burst
    /// is 3× this. The control plane pushes it per tier.
    pub qps: i64,
}

/// `POST /admin/v1/projects/:project_id/rate-limit` — set the per-project pgwire
/// request rate cap. Persisted to the catalog; the router resolves it lazily per
/// project at connection time (cloud-pushed per tier), mirroring max-connections.
pub(crate) async fn set_project_rate_limit(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(req): Json<SetRateLimitRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;
    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;
    assert_admin_for_path_project(&claims, &project)?;
    if req.qps <= 0 || req.qps > u32::MAX as i64 {
        return Err(ApiError::invalid(format!(
            "qps must be in 1..={}",
            u32::MAX
        )));
    }
    let qps = req.qps as u32;
    state
        .cfg
        .engine
        .config()
        .catalog
        .clone()
        .set_project_rate_limit_qps(&project, qps)
        .await
        .map_err(ApiError::from)?;
    tracing::info!(%project, qps, "per-project pgwire rate limit updated");
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `GET /admin/v1/projects/:project_id/rate-limit` — read the per-project cap
/// (`null` qps when none is set → project uses the global default limiter).
pub(crate) async fn get_project_rate_limit(
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
    let qps = state
        .cfg
        .engine
        .config()
        .catalog
        .clone()
        .get_project_rate_limit_qps(&project)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "project_id": project.to_string(), "qps": qps })).into_response())
}

// ---------------------------------------------------------------------------
// Placement: POST /admin/v1/projects/:project_id/placement
//            GET  /admin/v1/projects/:project_id/placement
// ---------------------------------------------------------------------------

/// `POST /admin/v1/projects/:project_id/placement` body.
///
/// Mirrors `basin_auth_client::set_project_home_region` wire shape exactly:
/// the cloud serialises `{ "home_region": "<fly_region_code>" }`. The value
/// is a Fly.io region code (e.g. `"jnb"`, `"iad"`, `"fra"`).
#[derive(Debug, Deserialize)]
pub(crate) struct SetPlacementRequest {
    pub home_region: String,
}

/// Set the per-project home region (placement pin).
///
/// Persists the region code in the catalog (survives restarts). v0.1 records
/// the value only; the multi-region router that acts on it is future work
/// (ADR 0009 / lane F4). The cloud calls this at project-create and on a
/// placement-move admin action.
#[axum::debug_handler]
pub(crate) async fn set_project_placement(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(req): Json<SetPlacementRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;
    assert_admin_for_path_project(&claims, &project)?;

    if req.home_region.is_empty() {
        return Err(ApiError::invalid("home_region must not be empty"));
    }

    let catalog = state.cfg.engine.config().catalog.clone();

    // Read-modify-write: the home region is one field of the unified
    // `ProjectMetadata` (alongside `byo_bucket`). Load the current metadata
    // first so setting the placement pin does NOT clobber a previously
    // registered BYO-bucket config. The WrongRegion write gate reads
    // `ProjectMetadata::home_region` (see `basin-engine` region.rs), so this is
    // the single source of truth the gate actually consults.
    let mut meta = catalog
        .get_project_metadata(&project)
        .await
        .map_err(ApiError::from)?;
    meta.home_region = Some(req.home_region.clone());
    catalog
        .set_project_metadata(&project, meta)
        .await
        .map_err(ApiError::from)?;

    tracing::info!(
        %project,
        home_region = %req.home_region,
        "per-project home region updated",
    );

    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Read the current per-project home region.
///
/// Returns the stored value, or `null` in the `home_region` field when no
/// placement pin has been pushed.
#[axum::debug_handler]
pub(crate) async fn get_project_placement(
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
    let region = catalog
        .get_project_metadata(&project)
        .await
        .map_err(ApiError::from)?
        .home_region;

    Ok(Json(json!({
        "project_id": project.to_string(),
        "home_region": region,
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

    fn claims_for(project_id: ProjectId, is_admin: bool) -> basin_auth::Claims {
        basin_auth::Claims {
            project_id,
            user_id: Default::default(),
            email: "admin@test".to_string(),
            roles: Vec::new(),
            exp: 0,
            iat: 0,
            is_admin,
            aal: basin_auth::jwt::Aal::Aal1,
            amr: Vec::new(),
        }
    }

    /// The control-plane token (scoped to INTERNAL_AUTH_PROJECT_ID) is a
    /// super-admin: it may operate on any path project. This is what lets the
    /// cloud's single deploy-time admin JWT set per-project ceilings/placement.
    #[test]
    fn internal_project_token_is_super_admin() {
        let internal: ProjectId = basin_auth::INTERNAL_AUTH_PROJECT_ID.parse().unwrap();
        let some_other = ProjectId::new();
        let claims = claims_for(internal, true);
        assert!(assert_admin_for_path_project(&claims, &some_other).is_ok());
    }

    /// A token scoped to a concrete project may only operate on that project.
    #[test]
    fn project_scoped_token_cannot_cross_projects() {
        let a = ProjectId::new();
        let b = ProjectId::new();
        let claims = claims_for(a.clone(), true);
        assert!(assert_admin_for_path_project(&claims, &a).is_ok());
        assert!(assert_admin_for_path_project(&claims, &b).is_err());
    }

    // -- max-connections: catalog round-trips ---------------------------------

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

        assert_eq!(
            cat.get_project_max_connections(&a).await.unwrap(),
            Some(100)
        );
        assert_eq!(
            cat.get_project_max_connections(&b).await.unwrap(),
            Some(200)
        );
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

    // -- placement: catalog round-trips ---------------------------------------
    //
    // The placement pin is the `home_region` field of the unified
    // `ProjectMetadata` (the single source of truth the WrongRegion gate
    // reads). The route does a read-modify-write so it preserves `byo_bucket`.

    /// Before any push, the metadata home_region is None.
    #[tokio::test]
    async fn placement_unset_returns_none() {
        let cat = Arc::new(InMemoryCatalog::new());
        let p = ProjectId::new();
        let v = cat.get_project_metadata(&p).await.unwrap().home_region;
        assert_eq!(v, None, "unset home_region should return None");
    }

    /// Set then get returns the pushed value; overwrite replaces it.
    #[tokio::test]
    async fn placement_set_get_round_trip() {
        let cat = Arc::new(InMemoryCatalog::new());
        let p = ProjectId::new();

        let mut meta = cat.get_project_metadata(&p).await.unwrap();
        meta.home_region = Some("jnb".to_owned());
        cat.set_project_metadata(&p, meta).await.unwrap();
        let v = cat.get_project_metadata(&p).await.unwrap().home_region;
        assert_eq!(v, Some("jnb".to_owned()));

        // Overwrite with a different region.
        let mut meta = cat.get_project_metadata(&p).await.unwrap();
        meta.home_region = Some("iad".to_owned());
        cat.set_project_metadata(&p, meta).await.unwrap();
        let v2 = cat.get_project_metadata(&p).await.unwrap().home_region;
        assert_eq!(v2, Some("iad".to_owned()), "overwrite must persist");
    }

    /// Different projects maintain independent home regions.
    #[tokio::test]
    async fn placement_per_project_isolation() {
        let cat = Arc::new(InMemoryCatalog::new());
        let a = ProjectId::new();
        let b = ProjectId::new();

        let mut meta = cat.get_project_metadata(&a).await.unwrap();
        meta.home_region = Some("fra".to_owned());
        cat.set_project_metadata(&a, meta).await.unwrap();
        // b receives no push.
        assert_eq!(
            cat.get_project_metadata(&a).await.unwrap().home_region,
            Some("fra".to_owned())
        );
        assert_eq!(
            cat.get_project_metadata(&b).await.unwrap().home_region,
            None,
            "project b should have no pin"
        );
    }

    /// drop_namespace clears the home region.
    #[tokio::test]
    async fn placement_cleared_on_drop_namespace() {
        let cat = Arc::new(InMemoryCatalog::new());
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();
        let mut meta = cat.get_project_metadata(&p).await.unwrap();
        meta.home_region = Some("sin".to_owned());
        cat.set_project_metadata(&p, meta).await.unwrap();
        cat.drop_namespace(&p).await.unwrap();
        let v = cat.get_project_metadata(&p).await.unwrap().home_region;
        assert_eq!(v, None, "home_region must be cleared after drop_namespace");
    }

    /// Setting the placement pin via read-modify-write must NOT clobber a
    /// previously registered BYO-bucket config (the unification invariant).
    #[tokio::test]
    async fn placement_rmw_preserves_byo_bucket() {
        use basin_catalog::{ProjectMetadata, S3Config};

        let cat = Arc::new(InMemoryCatalog::new());
        let p = ProjectId::new();

        // Existing BYO-bucket config (as register_byo_bucket would persist).
        let cfg = S3Config {
            endpoint: "https://fly.storage.tigris.dev".to_owned(),
            bucket: "cust-bucket".to_owned(),
            region: "auto".to_owned(),
            access_key_id: "AKIA".to_owned(),
            secret_access_key_enc: vec![1, 2, 3],
            force_path_style: false,
        };
        cat.set_project_metadata(
            &p,
            ProjectMetadata {
                byo_bucket: Some(cfg.clone()),
                home_region: None,
            },
        )
        .await
        .unwrap();

        // Now set the placement pin the way the route does (read-modify-write).
        let mut meta = cat.get_project_metadata(&p).await.unwrap();
        meta.home_region = Some("jnb".to_owned());
        cat.set_project_metadata(&p, meta).await.unwrap();

        let after = cat.get_project_metadata(&p).await.unwrap();
        assert_eq!(after.home_region, Some("jnb".to_owned()));
        assert_eq!(
            after.byo_bucket,
            Some(cfg),
            "placement RMW must preserve the BYO-bucket config"
        );
    }
}
