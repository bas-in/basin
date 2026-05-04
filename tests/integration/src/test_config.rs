//! Test-config loader for integration tests that need cloud credentials.
//!
//! The current consumers are the `s3_*` tests (real S3 / R2 / Tigris / etc.)
//! and the `compare_postgres` family. Both gate on the presence of the
//! relevant config section and **skip cleanly** — printing `[skip] ...`
//! and returning Ok — when their config is absent. The whole point of this
//! module is to avoid the failure mode where a CI run dies because someone
//! doesn't have AWS credentials on their machine.
//!
//! ## Lookup order
//!
//! 1. `$BASIN_TEST_CONFIG=<path>`
//! 2. `./.basin-test.toml`        (project-local, the recommended spot)
//! 3. `~/.basin/test-config.toml` (per-user, shared across projects)
//! 4. nothing — every gated test skips
//!
//! ## Never commit `.basin-test.toml`
//!
//! The file contains live cloud credentials. It is gitignored at the repo
//! root. The committable example template lives at
//! `.basin-test.example.toml` and contains no real values.

use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

/// Top-level configuration. Every section is optional.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct BasinTestConfig {
    pub s3: Option<S3Config>,
    pub postgres: Option<PostgresConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Tests append `<test_name>/<run_ulid>/` to this prefix and DELETE
    /// the resulting sub-prefix on teardown.
    #[serde(default = "default_s3_prefix")]
    pub prefix: String,

    /// Credential resolution — first set wins.
    pub profile: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,

    /// Non-AWS S3-compatible service endpoint (R2, MinIO, Tigris, Wasabi).
    pub endpoint: Option<String>,
    /// Plaintext HTTP — only true for local MinIO. Default false.
    #[serde(default)]
    pub allow_http: bool,
    /// Force HTTP/2-only on the S3 client. When true, reqwest multiplexes
    /// many concurrent streams over a single TCP socket — eliminates
    /// HTTP/1.1 head-of-line blocking when a single tenant's bulk reads
    /// would otherwise tie up every keep-alive connection. Default false:
    /// HTTP/1.1 with the connection pool. Real AWS S3 / R2 / GCS support
    /// HTTP/2 over TLS and benefit from this; local MinIO over plain HTTP
    /// may not support h2c (cleartext h2) — leave off there.
    #[serde(default)]
    pub http2_only: bool,
    /// Accept self-signed / invalid TLS certificates. Useful only for
    /// local MinIO over HTTPS where you've generated a self-signed
    /// cert. NEVER enable in production.
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

fn default_s3_prefix() -> String {
    "basin-test".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct PostgresConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_pg_dbname")]
    pub dbname: String,
}

fn default_pg_port() -> u16 {
    5432
}
fn default_pg_dbname() -> String {
    "postgres".to_string()
}

impl BasinTestConfig {
    /// Resolve config via the documented lookup order. Never errors on a
    /// missing file; that path returns `Default::default()`. Errors only on
    /// a found-but-malformed file (parse failure).
    pub fn load() -> std::io::Result<Self> {
        let path = match resolve_config_path() {
            Some(p) => p,
            None => return Ok(Self::default()),
        };
        let body = std::fs::read_to_string(&path)?;
        match toml::from_str(&body) {
            Ok(cfg) => Ok(cfg),
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("parse {}: {e}", path.display()),
            )),
        }
    }

    /// Convenience: returns the S3 config if present, else `None` and prints
    /// a `[skip]` line so test output is self-explanatory.
    pub fn s3_or_skip(&self, test_name: &str) -> Option<&S3Config> {
        match &self.s3 {
            Some(c) => Some(c),
            None => {
                println!(
                    "[skip] {test_name}: [s3] not in test config; copy .basin-test.example.toml to .basin-test.toml and fill in to run"
                );
                None
            }
        }
    }

    pub fn pg_or_skip(&self, test_name: &str) -> Option<&PostgresConfig> {
        match &self.postgres {
            Some(c) => Some(c),
            None => {
                println!("[skip] {test_name}: [postgres] not in test config");
                None
            }
        }
    }
}

fn resolve_config_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("BASIN_TEST_CONFIG") {
        let path = PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    // CWD-relative `.basin-test.toml`. When `cargo test` runs from the
    // workspace root this hits the file directly; when it runs from a
    // crate manifest dir it doesn't, so we also walk up to the workspace
    // root via CARGO_MANIFEST_DIR (set by cargo at build time).
    let project_local = PathBuf::from(".basin-test.toml");
    if project_local.exists() {
        return Some(project_local);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut dir: Option<&std::path::Path> = Some(manifest.as_path());
    while let Some(d) = dir {
        let candidate = d.join(".basin-test.toml");
        if candidate.exists() {
            return Some(candidate);
        }
        dir = d.parent();
    }
    if let Some(home) = dirs_home() {
        let user_path = home.join(".basin").join("test-config.toml");
        if user_path.exists() {
            return Some(user_path);
        }
    }
    None
}

/// Stand-in for the `dirs` crate to avoid a new workspace dep. We honor the
/// same environment variables `dirs::home_dir` does on macOS / Linux.
fn dirs_home() -> Option<PathBuf> {
    if let Ok(h) = std::env::var("HOME") {
        if !h.is_empty() {
            return Some(PathBuf::from(h));
        }
    }
    None
}

// ----------------------------------------------------------------------------
// S3 ObjectStore construction.
//
// These functions are gated behind `cfg(feature = "object_store-aws")` style
// in object_store's Cargo features, but the workspace already enables `aws`
// so we can use AmazonS3Builder unconditionally.
// ----------------------------------------------------------------------------

impl S3Config {
    /// Build an S3-backed `ObjectStore`. Returns the shared `Arc<dyn ObjectStore>`
    /// the storage layer accepts directly.
    ///
    /// Credential precedence: inline keys → default AWS credentials chain
    /// (env vars, IAM role, etc.). The `profile` config field is reserved
    /// for a future object_store version with first-class profile support;
    /// today, set `AWS_PROFILE=foo` in the environment instead.
    pub fn build_object_store(
        &self,
    ) -> Result<Arc<dyn object_store::ObjectStore>, object_store::Error> {
        use object_store::aws::AmazonS3Builder;
        use object_store::ClientOptions;

        // Bump the per-host idle pool floor well above reqwest's default
        // (~32). Real-S3 multi-tenant workloads otherwise serialise behind
        // the pool's limit and a noisy tenant's full scan starves a quiet
        // tenant's point reads. 256 is generous enough that the pool is
        // never the proximate bottleneck on the workloads in
        // `tests/integration/tests/s3_*`; tenant fairness is then
        // guaranteed by `basin-storage`'s per-tenant semaphore on top.
        let mut client_opts = ClientOptions::new().with_pool_max_idle_per_host(256);

        // HTTP/2 multiplexes many concurrent requests over a single TCP
        // socket — eliminates HTTP/1.1 head-of-line blocking that would
        // otherwise let a single tenant's bulk transfer tie up the pool.
        // See `S3Config::http2_only`.
        if self.http2_only {
            client_opts = client_opts.with_http2_only();
        }
        if self.accept_invalid_certs {
            client_opts = client_opts.with_allow_invalid_certificates(true);
        }

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_client_options(client_opts);

        if let Some(key) = &self.access_key_id {
            if !key.is_empty() {
                builder = builder.with_access_key_id(key);
                if let Some(secret) = &self.secret_access_key {
                    builder = builder.with_secret_access_key(secret);
                }
                if let Some(token) = &self.session_token {
                    if !token.is_empty() {
                        builder = builder.with_token(token);
                    }
                }
            }
        }
        // else: fall through to AWS default credentials chain
        // (object_store reads AWS_ACCESS_KEY_ID / AWS_SESSION_TOKEN /
        // ~/.aws/credentials automatically when no creds are set).

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }

        let store = builder.build()?;
        Ok(Arc::new(store))
    }

    /// Construct a fully-qualified prefix path for one test run. Tests should
    /// keep ALL writes under this prefix and pass the same value into
    /// `cleanup_prefix_on_drop` so teardown nukes only their data.
    pub fn run_prefix(&self, test_name: &str) -> String {
        let ulid = ulid::Ulid::new();
        format!("{}/{}/{}", self.prefix, test_name, ulid)
    }
}

/// RAII guard: deletes everything under `prefix` in `store` when dropped.
/// Spawns a tokio task to do the deletion so `Drop` itself is non-blocking.
/// The task runs even if the test panics.
pub struct CleanupOnDrop {
    pub store: Arc<dyn object_store::ObjectStore>,
    pub prefix: String,
}

impl Drop for CleanupOnDrop {
    fn drop(&mut self) {
        let store = self.store.clone();
        let prefix = self.prefix.clone();
        // The Drop guard works whether or not we're in a tokio runtime; it
        // deliberately uses a fresh runtime so panicking tests still clean up.
        let join = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("CleanupOnDrop: cannot build runtime: {e}");
                    return;
                }
            };
            rt.block_on(async {
                let path = object_store::path::Path::from(prefix.as_str());
                let mut stream = store.list(Some(&path));
                use futures::StreamExt as _;
                let mut to_delete = Vec::new();
                while let Some(meta) = stream.next().await {
                    match meta {
                        Ok(m) => to_delete.push(m.location),
                        Err(e) => eprintln!("CleanupOnDrop: list error: {e}"),
                    }
                }
                for loc in to_delete {
                    if let Err(e) = store.delete(&loc).await {
                        eprintln!("CleanupOnDrop: delete {loc}: {e}");
                    }
                }
            });
        });
        // Best-effort join; if it hangs, abandon — we don't want a stuck test
        // run to also have a stuck teardown.
        let _ = join.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_s3() {
        let s = r#"
            [s3]
            bucket = "b"
            region = "us-east-1"
        "#;
        let cfg: BasinTestConfig = toml::from_str(s).unwrap();
        let s3 = cfg.s3.as_ref().unwrap();
        assert_eq!(s3.bucket, "b");
        assert_eq!(s3.region, "us-east-1");
        assert_eq!(s3.prefix, "basin-test"); // default
        assert!(!s3.allow_http);
    }

    #[test]
    fn parses_inline_creds_and_endpoint() {
        let s = r#"
            [s3]
            bucket = "b"
            region = "auto"
            access_key_id = "AKIA..."
            secret_access_key = "..."
            endpoint = "https://example.com"
            allow_http = true
        "#;
        let cfg: BasinTestConfig = toml::from_str(s).unwrap();
        let s3 = cfg.s3.unwrap();
        assert_eq!(s3.access_key_id.as_deref(), Some("AKIA..."));
        assert_eq!(s3.endpoint.as_deref(), Some("https://example.com"));
        assert!(s3.allow_http);
    }

    #[test]
    fn empty_config_loads() {
        let cfg: BasinTestConfig = toml::from_str("").unwrap();
        assert!(cfg.s3.is_none());
        assert!(cfg.postgres.is_none());
    }

    #[test]
    fn run_prefix_is_unique() {
        let s3 = S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            prefix: "test".into(),
            profile: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint: None,
            allow_http: false,
            http2_only: false,
            accept_invalid_certs: false,
        };
        let p1 = s3.run_prefix("foo");
        let p2 = s3.run_prefix("foo");
        assert_ne!(p1, p2);
        assert!(p1.starts_with("test/foo/"));
    }
}
