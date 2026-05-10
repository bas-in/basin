//! Auth stub.
//!
//! v0.1 treats the `Authorization: Bearer <token>` header as the caller's
//! [`basin_common::TenantId`]: the bearer must parse as a ULID, and that
//! ULID is the tenant whose namespace the URL is allowed to address.
//! This is the smallest stub that's actually safe — it gives every
//! integration test a way to assert cross-tenant isolation without
//! wiring a real JWT issuer.
//!
//! Production wiring will replace [`extract_tenant`] with a call into
//! `basin_auth::AuthService::verify_jwt` (the same path
//! `basin_rest::server::authorize` uses) and pull `claims.tenant_id`
//! from the verified token. The handler-side contract — "the URL's
//! `:namespace` segment must equal the caller's tenant" — stays the
//! same; only the token decoder changes.
//!
//! When [`crate::IcebergRestConfig::require_bearer`] is `false` (test
//! config), the check is skipped entirely and handlers trust the URL
//! `:namespace` segment unconditionally. Useful for the read-only happy
//! path tests; the isolation test toggles it back on.

use std::str::FromStr;

use axum::http::header::AUTHORIZATION;
use axum::http::HeaderMap;
use basin_common::TenantId;

use crate::error::IcebergRestError;
use crate::IcebergRestConfig;

/// Extract the caller's tenant from the bearer token. Returns `None`
/// when auth is disabled (the caller then trusts the URL `:namespace`).
pub(crate) fn extract_tenant(
    cfg: &IcebergRestConfig,
    headers: &HeaderMap,
) -> Result<Option<TenantId>, IcebergRestError> {
    if !cfg.require_bearer {
        return Ok(None);
    }
    let auth = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| IcebergRestError::Unauthorized("missing Authorization header".into()))?
        .to_str()
        .map_err(|_| IcebergRestError::Unauthorized("Authorization not ASCII".into()))?;
    let token = auth
        .strip_prefix("Bearer ")
        .or_else(|| auth.strip_prefix("bearer "))
        .ok_or_else(|| {
            IcebergRestError::Unauthorized("Authorization must be `Bearer <token>`".into())
        })?
        .trim();
    if token.is_empty() {
        return Err(IcebergRestError::Unauthorized("empty bearer token".into()));
    }
    let tenant = TenantId::from_str(token).map_err(|e| {
        IcebergRestError::Unauthorized(format!("bearer token must be a ULID tenant id: {e}"))
    })?;
    Ok(Some(tenant))
}

/// Authorize a request whose URL `:namespace` segment must match the
/// caller's tenant. Returns the resolved tenant id either way.
pub(crate) fn authorize_namespace(
    cfg: &IcebergRestConfig,
    headers: &HeaderMap,
    url_namespace: &TenantId,
) -> Result<TenantId, IcebergRestError> {
    match extract_tenant(cfg, headers)? {
        Some(caller) => {
            if &caller != url_namespace {
                return Err(IcebergRestError::Unauthorized(
                    "caller is not authorized for this namespace".into(),
                ));
            }
            Ok(caller)
        }
        None => Ok(*url_namespace),
    }
}
