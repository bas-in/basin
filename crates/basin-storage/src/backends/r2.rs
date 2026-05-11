//! S3-compatible object-store backend (R2 / Tigris / S3 / MinIO / Wasabi).
//!
//! Cloudflare R2, Tigris (Fly's S3 surface), AWS S3, MinIO and Wasabi all
//! speak the same S3 API: SigV4, same `PutObject` / `GetObject` /
//! `DeleteObjects` actions. The only differences are the endpoint URL and
//! the region literal — both belong in config, not in code. As a result
//! this module is a thin wrapper around `object_store::aws::AmazonS3`.
//!
//! Why a wrapper at all?
//!
//! - One env-var schema (`BASIN_STORAGE_*`) regardless of which S3-flavoured
//!   backend is in use. For Fly-Tigris deployments we additionally read the
//!   `AWS_*` / `BUCKET_NAME` names that `fly storage create` sets on the
//!   app automatically — when both are present the canonical
//!   `BASIN_STORAGE_*` wins. The binary's startup code calls one function
//!   and doesn't care about the difference.
//! - Centralised defaults: `region = "auto"` for R2 and Tigris,
//!   virtual-hosted-style on, anonymous-credentials off. Easy to get wrong
//!   by hand.
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

/// Provider flavour. The values share the same builder and the same
/// env-var surface; only the *default* region and endpoint differ.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provider {
    /// Cloudflare R2. Region must be `auto`; endpoint is required and
    /// follows the shape `https://<account-id>.r2.cloudflarestorage.com`.
    R2,
    /// AWS S3 proper. Region is required (e.g. `us-east-1`); endpoint is
    /// optional (defaults to the AWS regional endpoint).
    S3,
    /// Tigris (Fly's globally distributed S3-compatible store, provisioned
    /// via `fly storage create`). Region is `auto`; endpoint defaults to
    /// `https://fly.storage.tigris.dev`.
    Tigris,
}

impl Provider {
    fn default_region(self) -> &'static str {
        match self {
            Provider::R2 => "auto",
            Provider::S3 => "us-east-1",
            Provider::Tigris => "auto",
        }
    }

    /// Provider's well-known endpoint, if one exists. `S3` has no fixed
    /// endpoint (the AWS SDK derives it from the region); R2 needs an
    /// account-scoped endpoint and so returns `None` (it must come from
    /// config); Tigris has a single global endpoint.
    fn default_endpoint(&self) -> Option<&'static str> {
        match self {
            Provider::R2 => None,
            Provider::S3 => None,
            Provider::Tigris => Some("https://fly.storage.tigris.dev"),
        }
    }
}

/// Configuration for the S3-compatible object-store backend. Built by
/// [`S3LikeConfig::from_env`] in the server binary; callers can also
/// construct it literally for integration tests.
#[derive(Clone, Debug)]
pub struct S3LikeConfig {
    pub provider: Provider,
    pub bucket: String,
    /// Optional. Required for `Provider::R2`. Shape:
    /// `https://<account-id>.r2.cloudflarestorage.com`. For `Provider::S3`
    /// leave `None` to use AWS's regional endpoint; set for S3-compatible
    /// stores like MinIO or Wasabi. For `Provider::Tigris`, defaults to
    /// `https://fly.storage.tigris.dev` if unset.
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Optional session token (STS). Most deployments leave this `None`.
    pub session_token: Option<String>,
}

/// Backward-compatibility alias for the pre-rename type name.
pub type R2Config = S3LikeConfig;

impl S3LikeConfig {
    /// Construct from environment variables. The contract:
    ///
    /// | Env var                            | Fly fallback              | Required | Notes                                              |
    /// |------------------------------------|---------------------------|----------|----------------------------------------------------|
    /// | `BASIN_STORAGE_BACKEND`            | —                         | see note | `r2`, `s3` or `tigris`; can be inferred (see note) |
    /// | `BASIN_STORAGE_BUCKET`             | `BUCKET_NAME`             | yes      |                                                    |
    /// | `BASIN_STORAGE_ENDPOINT`           | `AWS_ENDPOINT_URL_S3`     | r2 only  | tigris defaults to `https://fly.storage.tigris.dev`|
    /// | `BASIN_STORAGE_REGION`             | `AWS_REGION`              | no       | defaults: r2/tigris=`auto`, s3=`us-east-1`         |
    /// | `BASIN_STORAGE_ACCESS_KEY_ID`      | `AWS_ACCESS_KEY_ID`       | yes      |                                                    |
    /// | `BASIN_STORAGE_SECRET_ACCESS_KEY`  | `AWS_SECRET_ACCESS_KEY`   | yes      |                                                    |
    /// | `BASIN_STORAGE_SESSION_TOKEN`      | —                         | no       | STS deployments only                               |
    ///
    /// Backend inference: if `BASIN_STORAGE_BACKEND` is unset but
    /// `AWS_ENDPOINT_URL_S3` is set (Fly-Tigris case), the provider is
    /// inferred from the endpoint host — `tigris.dev` → `Tigris`,
    /// `cloudflarestorage.com` → `R2`, otherwise `S3`.
    ///
    /// The function is library-agnostic about the error type so it can be
    /// called from `anyhow`-using binaries; we return a `String` and let
    /// the caller wrap it.
    pub fn from_env() -> Result<Self, String> {
        let provider = match std::env::var("BASIN_STORAGE_BACKEND")
            .ok()
            .filter(|s| !s.is_empty())
        {
            Some(b) => match b.as_str() {
                "r2" => Provider::R2,
                "s3" => Provider::S3,
                "tigris" => Provider::Tigris,
                other => {
                    return Err(format!(
                        "S3LikeConfig::from_env: unsupported backend {other:?} (expected r2, s3, or tigris)"
                    ))
                }
            },
            None => {
                // No explicit backend — infer from the Fly-set
                // `AWS_ENDPOINT_URL_S3` if present.
                let ep = std::env::var("AWS_ENDPOINT_URL_S3")
                    .ok()
                    .filter(|s| !s.is_empty());
                match ep.as_deref() {
                    Some(e) if e.contains("tigris.dev") => Provider::Tigris,
                    Some(e) if e.contains("cloudflarestorage.com") => Provider::R2,
                    Some(_) => Provider::S3,
                    None => {
                        return Err(
                            "BASIN_STORAGE_BACKEND not set and no AWS_ENDPOINT_URL_S3 to infer from"
                                .into(),
                        )
                    }
                }
            }
        };

        let bucket = require_env_or("BASIN_STORAGE_BUCKET", "BUCKET_NAME")?;

        // Endpoint: explicit BASIN_STORAGE_ENDPOINT > Fly's
        // AWS_ENDPOINT_URL_S3 > provider's default_endpoint().
        let endpoint = std::env::var("BASIN_STORAGE_ENDPOINT")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("AWS_ENDPOINT_URL_S3")
                    .ok()
                    .filter(|s| !s.is_empty())
            })
            .or_else(|| provider.default_endpoint().map(|s| s.to_string()));
        if provider == Provider::R2 && endpoint.is_none() {
            return Err(
                "BASIN_STORAGE_BACKEND=r2 requires BASIN_STORAGE_ENDPOINT (or AWS_ENDPOINT_URL_S3)"
                    .into(),
            );
        }

        let region = std::env::var("BASIN_STORAGE_REGION")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| std::env::var("AWS_REGION").ok().filter(|s| !s.is_empty()))
            .unwrap_or_else(|| provider.default_region().to_string());

        let access_key_id = require_env_or("BASIN_STORAGE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID")?;
        let secret_access_key =
            require_env_or("BASIN_STORAGE_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")?;
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
            // routing. Tigris and AWS S3 also accept this form, so this
            // default is safe across providers.
            .with_virtual_hosted_style_request(true);

        if let Some(ep) = &self.endpoint {
            b = b.with_endpoint(ep);
        }
        if let Some(tok) = &self.session_token {
            b = b.with_token(tok);
        }
        // R2 / Tigris endpoints are always HTTPS; reject any plaintext
        // config to avoid silently leaking credentials over the wire.
        if let Some(ep) = &self.endpoint {
            if !ep.starts_with("https://") {
                return Err(format!("BASIN_STORAGE_ENDPOINT must be HTTPS (got {ep:?})"));
            }
        }

        let store = b
            .build()
            .map_err(|e| format!("AmazonS3Builder::build failed: {e}"))?;
        Ok(Arc::new(store))
    }
}

/// Read `primary` from the environment; if unset or empty, fall back to
/// `fallback`. Returns a clear error naming both keys when neither is set.
fn require_env_or(primary: &str, fallback: &str) -> Result<String, String> {
    if let Some(v) = std::env::var(primary).ok().filter(|s| !s.is_empty()) {
        return Ok(v);
    }
    if let Some(v) = std::env::var(fallback).ok().filter(|s| !s.is_empty()) {
        return Ok(v);
    }
    Err(format!("neither {primary} nor {fallback} is set"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env-var tests must not run in parallel — `std::env::set_var` mutates
    // process-global state. Serialise them through one mutex.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that snapshots and restores a set of env vars across a
    /// test body. Set the var to `None` to unset it for the duration.
    struct EnvGuard {
        original: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn new(vars: &[(&'static str, Option<&str>)]) -> Self {
            let mut original = Vec::with_capacity(vars.len());
            for (k, v) in vars {
                original.push((*k, std::env::var(k).ok()));
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
            Self { original }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.original {
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    /// Names of every env var the tests in this module touch. The guard
    /// always clears all of them and restores afterwards, so a test that
    /// only cares about a subset still gets a clean slate.
    const ALL_KEYS: &[&str] = &[
        "BASIN_STORAGE_BACKEND",
        "BASIN_STORAGE_BUCKET",
        "BASIN_STORAGE_ENDPOINT",
        "BASIN_STORAGE_REGION",
        "BASIN_STORAGE_ACCESS_KEY_ID",
        "BASIN_STORAGE_SECRET_ACCESS_KEY",
        "BASIN_STORAGE_SESSION_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "AWS_ENDPOINT_URL_S3",
        "BUCKET_NAME",
    ];

    fn clean_env_with(overrides: &[(&'static str, &str)]) -> EnvGuard {
        let mut vars: Vec<(&'static str, Option<&str>)> =
            ALL_KEYS.iter().map(|k| (*k, None)).collect();
        for (k, v) in overrides {
            // Replace the matching entry with a Some.
            if let Some(slot) = vars.iter_mut().find(|(kk, _)| kk == k) {
                slot.1 = Some(*v);
            } else {
                vars.push((*k, Some(*v)));
            }
        }
        EnvGuard::new(&vars)
    }

    #[test]
    fn default_region_for_r2_is_auto() {
        assert_eq!(Provider::R2.default_region(), "auto");
        assert_eq!(Provider::S3.default_region(), "us-east-1");
    }

    #[test]
    fn default_region_for_tigris_is_auto() {
        assert_eq!(Provider::Tigris.default_region(), "auto");
        assert_eq!(
            Provider::Tigris.default_endpoint(),
            Some("https://fly.storage.tigris.dev")
        );
        assert_eq!(Provider::R2.default_endpoint(), None);
        assert_eq!(Provider::S3.default_endpoint(), None);
    }

    #[test]
    fn build_rejects_plaintext_endpoint() {
        let cfg = S3LikeConfig {
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
        let cfg = S3LikeConfig {
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

    #[test]
    fn build_accepts_tigris_endpoint() {
        let cfg = S3LikeConfig {
            provider: Provider::Tigris,
            bucket: "basin-engine-dev".into(),
            endpoint: Some("https://fly.storage.tigris.dev".into()),
            region: "auto".into(),
            access_key_id: "tid_...".into(),
            secret_access_key: "tsec_...".into(),
            session_token: None,
        };
        let _store = cfg.build_object_store().expect("build");
    }

    #[test]
    fn from_env_picks_up_aws_endpoint_url_when_basin_unset() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = clean_env_with(&[
            // Mirror what `fly storage create -a basin-engine` would set —
            // no `BASIN_STORAGE_*` keys at all.
            ("AWS_ENDPOINT_URL_S3", "https://fly.storage.tigris.dev"),
            ("AWS_REGION", "auto"),
            ("AWS_ACCESS_KEY_ID", "tid_test"),
            ("AWS_SECRET_ACCESS_KEY", "tsec_test"),
            ("BUCKET_NAME", "basin-engine-dev"),
        ]);

        let cfg = S3LikeConfig::from_env().expect("from_env should succeed");
        assert_eq!(cfg.provider, Provider::Tigris);
        assert_eq!(cfg.bucket, "basin-engine-dev");
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://fly.storage.tigris.dev")
        );
        assert_eq!(cfg.region, "auto");
        assert_eq!(cfg.access_key_id, "tid_test");
        assert_eq!(cfg.secret_access_key, "tsec_test");
    }

    #[test]
    fn from_env_basin_keys_override_aws_fallbacks() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guard = clean_env_with(&[
            ("BASIN_STORAGE_BACKEND", "tigris"),
            ("BASIN_STORAGE_BUCKET", "explicit-bucket"),
            ("BASIN_STORAGE_ACCESS_KEY_ID", "explicit_id"),
            ("BASIN_STORAGE_SECRET_ACCESS_KEY", "explicit_secret"),
            // Fly-set fallbacks; should all be shadowed.
            ("AWS_ENDPOINT_URL_S3", "https://fly.storage.tigris.dev"),
            ("AWS_REGION", "auto"),
            ("AWS_ACCESS_KEY_ID", "fly_id"),
            ("AWS_SECRET_ACCESS_KEY", "fly_secret"),
            ("BUCKET_NAME", "fly-bucket"),
        ]);

        let cfg = S3LikeConfig::from_env().unwrap();
        assert_eq!(cfg.provider, Provider::Tigris);
        assert_eq!(cfg.bucket, "explicit-bucket");
        assert_eq!(cfg.access_key_id, "explicit_id");
        assert_eq!(cfg.secret_access_key, "explicit_secret");
        // Endpoint not explicit → falls back to AWS env, then to provider
        // default. Both happen to be the Tigris endpoint here.
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://fly.storage.tigris.dev")
        );
    }
}
