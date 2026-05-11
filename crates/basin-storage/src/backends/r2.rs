//! Cloudflare R2 (and AWS S3) object-store backend.
//!
//! R2 is S3-API-compatible: same SigV4, same `PutObject` / `GetObject` /
//! `DeleteObjects` actions. The only differences are the endpoint URL and
//! the region literal — both belong in config, not in code. As a result
//! this module is a thin wrapper around `object_store::aws::AmazonS3`.
//!
//! Why a wrapper at all?
//!
//! - One env-var schema (`BASIN_STORAGE_*`) regardless of which S3-flavoured
//!   backend is in use. The binary's startup code calls one function and
//!   doesn't care about the difference.
//! - Centralised defaults: `region = "auto"` for R2, virtual-hosted-style
//!   on, anonymous-credentials off. Easy to get wrong by hand.
//! - One place to add provider-specific tweaks (e.g. R2's IA storage class
//!   when we wire lifecycle policies through the engine instead of the
//!   bucket dashboard).
//!
//! The `object_store` workspace dep already includes the `aws` feature
//! (see root `Cargo.toml`), so no new dependency is introduced.
//!
//! See `docs/scaling/object-storage.md` for the deployment story.

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

/// Provider flavour. The two values share the same builder and the same
/// env-var surface; only the *default* region differs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    /// Cloudflare R2. Region must be `auto`; endpoint is required and
    /// follows the shape `https://<account-id>.r2.cloudflarestorage.com`.
    R2,
    /// AWS S3 proper. Region is required (e.g. `us-east-1`); endpoint is
    /// optional (defaults to the AWS regional endpoint).
    S3,
}

impl Provider {
    fn default_region(self) -> &'static str {
        match self {
            Provider::R2 => "auto",
            Provider::S3 => "us-east-1",
        }
    }
}

/// Configuration for the R2 / S3 backend. Built by [`R2Config::from_env`]
/// in the server binary; callers can also construct it literally for
/// integration tests.
#[derive(Clone, Debug)]
pub struct R2Config {
    pub provider: Provider,
    pub bucket: String,
    /// Optional. Required for `Provider::R2`. Shape:
    /// `https://<account-id>.r2.cloudflarestorage.com`. For `Provider::S3`
    /// leave `None` to use AWS's regional endpoint; set for S3-compatible
    /// stores like MinIO or Wasabi.
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Optional session token (STS). Most deployments leave this `None`.
    pub session_token: Option<String>,
}

impl R2Config {
    /// Construct from environment variables. The contract:
    ///
    /// | Env var                            | Required | Notes                                              |
    /// |------------------------------------|----------|----------------------------------------------------|
    /// | `BASIN_STORAGE_BACKEND`            | yes      | `r2` or `s3`                                       |
    /// | `BASIN_STORAGE_BUCKET`             | yes      |                                                    |
    /// | `BASIN_STORAGE_ENDPOINT`           | r2 only  | `https://<account>.r2.cloudflarestorage.com`       |
    /// | `BASIN_STORAGE_REGION`             | no       | defaults: r2=`auto`, s3=`us-east-1`                |
    /// | `BASIN_STORAGE_ACCESS_KEY_ID`      | yes      | from `fly secrets set`                             |
    /// | `BASIN_STORAGE_SECRET_ACCESS_KEY`  | yes      | from `fly secrets set`                             |
    /// | `BASIN_STORAGE_SESSION_TOKEN`      | no       | STS deployments only                               |
    ///
    /// The function is library-agnostic about the error type so it can be
    /// called from `anyhow`-using binaries; we return a `String` and let
    /// the caller wrap it.
    pub fn from_env() -> Result<Self, String> {
        let backend = std::env::var("BASIN_STORAGE_BACKEND")
            .map_err(|_| "BASIN_STORAGE_BACKEND not set".to_string())?;
        let provider = match backend.as_str() {
            "r2" => Provider::R2,
            "s3" => Provider::S3,
            other => return Err(format!("R2Config::from_env: unsupported backend {other:?}")),
        };
        let bucket = require_env("BASIN_STORAGE_BUCKET")?;
        let endpoint = std::env::var("BASIN_STORAGE_ENDPOINT").ok().filter(|s| !s.is_empty());
        if provider == Provider::R2 && endpoint.is_none() {
            return Err("BASIN_STORAGE_BACKEND=r2 requires BASIN_STORAGE_ENDPOINT".into());
        }
        let region = std::env::var("BASIN_STORAGE_REGION")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| provider.default_region().to_string());
        let access_key_id = require_env("BASIN_STORAGE_ACCESS_KEY_ID")?;
        let secret_access_key = require_env("BASIN_STORAGE_SECRET_ACCESS_KEY")?;
        let session_token = std::env::var("BASIN_STORAGE_SESSION_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());

        Ok(Self {
            provider,
            bucket,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    /// Realise the config into an `Arc<dyn ObjectStore>` ready to drop
    /// into [`crate::StorageConfig::object_store`].
    pub fn build_object_store(&self) -> Result<Arc<dyn ObjectStore>, String> {
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            // R2 needs virtual-hosted-style (`<bucket>.<acc>.r2...`) so the
            // pre-signed URLs the engine hands out match Cloudflare's
            // routing. AWS S3 accepts both forms, so this default is safe
            // across providers.
            .with_virtual_hosted_style_request(true);

        if let Some(ep) = &self.endpoint {
            b = b.with_endpoint(ep);
        }
        if let Some(tok) = &self.session_token {
            b = b.with_token(tok);
        }
        // R2 endpoints are always HTTPS; reject any plaintext config to
        // avoid silently leaking credentials over the wire.
        if let Some(ep) = &self.endpoint {
            if !ep.starts_with("https://") {
                return Err(format!(
                    "BASIN_STORAGE_ENDPOINT must be HTTPS (got {ep:?})"
                ));
            }
        }

        let store = b
            .build()
            .map_err(|e| format!("AmazonS3Builder::build failed: {e}"))?;
        Ok(Arc::new(store))
    }
}

fn require_env(name: &str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{name} not set"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_region_for_r2_is_auto() {
        assert_eq!(Provider::R2.default_region(), "auto");
        assert_eq!(Provider::S3.default_region(), "us-east-1");
    }

    #[test]
    fn build_rejects_plaintext_endpoint() {
        let cfg = R2Config {
            provider: Provider::R2,
            bucket: "b".into(),
            endpoint: Some("http://example.com".into()),
            region: "auto".into(),
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
            session_token: None,
        };
        let err = cfg.build_object_store().unwrap_err();
        assert!(err.contains("HTTPS"), "got {err}");
    }

    #[test]
    fn build_accepts_well_formed_r2_config() {
        let cfg = R2Config {
            provider: Provider::R2,
            bucket: "basin-engine-dev".into(),
            endpoint: Some("https://abc.r2.cloudflarestorage.com".into()),
            region: "auto".into(),
            access_key_id: "AKIA...".into(),
            secret_access_key: "secret".into(),
            session_token: None,
        };
        // We don't actually issue any HTTP — just prove the builder is
        // happy with the shape.
        let _store = cfg.build_object_store().expect("build");
    }
}
