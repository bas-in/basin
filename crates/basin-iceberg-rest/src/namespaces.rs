//! Namespace handlers.
//!
//! Iceberg's catalog-list-namespaces semantic is "every namespace the
//! caller can see". The locked Basin `Catalog` trait is intentionally
//! project-scoped — there is no enumerate-across-projects method (see
//! the trait docstring) — so v0.1 returns a single-element list
//! containing the caller's own project. That's the smallest answer
//! consistent with Basin's isolation model: the caller is always
//! permitted to see exactly its own namespace, never anyone else's.

use axum::extract::{Path, State as AxumState};
use axum::http::HeaderMap;
use axum::Json;

use crate::auth;
use crate::error::IcebergRestError;
use crate::models::{project_to_namespace, ListNamespacesResponse};
use crate::State;

pub(crate) async fn list_namespaces(
    AxumState(state): AxumState<State>,
    Path(_prefix): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ListNamespacesResponse>, IcebergRestError> {
    let caller = auth::extract_project(&state.cfg, &headers)?;
    let namespaces = match caller {
        // Auth on: surface only the caller's project.
        Some(t) => vec![project_to_namespace(&t)],
        // Auth off (test config): empty list — there's nothing the
        // server can responsibly enumerate without a caller identity.
        None => Vec::new(),
    };
    Ok(Json(ListNamespacesResponse { namespaces }))
}
