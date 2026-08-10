//! Mutual TLS (rustls) for the raft transport.
//!
//! The transport is plaintext by default — a single private network (a VPC, a
//! fly.io 6PN mesh) is the assumed deployment, and the openraft `Vote` carried
//! in every RPC already fences stale leaders at the protocol layer. mTLS is the
//! opt-in hardening for deployments that cannot assume a trusted network: it
//! gives confidentiality (TLS) AND mutual peer authentication (both ends
//! present a certificate signed by the cluster CA), so a node that cannot prove
//! cluster membership cannot speak raft.
//!
//! ## Configuration (all three or none)
//!
//! | env | meaning |
//! |-----|---------|
//! | `BASIN_RAFT_TLS_CERT` | PEM path to this node's leaf certificate. |
//! | `BASIN_RAFT_TLS_KEY`  | PEM path to this node's private key. |
//! | `BASIN_RAFT_TLS_CA`   | PEM path to the cluster CA bundle used to verify peers. |
//! | `BASIN_RAFT_TLS_DOMAIN` | (optional) SNI / hostname the client verifies the peer cert against. Defaults to `basin-raft`. |
//!
//! Presence is all-or-nothing: setting some but not all of the three required
//! vars is a **startup error** (a half-configured TLS posture must not silently
//! fall back to plaintext). With none set, the transport is plaintext as
//! before. [`RaftTlsConfig::from_env`] enforces this.
//!
//! ## Mutual authentication
//!
//! * The **server** ([`RaftTlsConfig::server_tls`]) presents this node's leaf +
//!   key and REQUIRES a client certificate chaining to the CA
//!   (`with_client_ca_root` makes client auth mandatory in tonic 0.12). An
//!   unauthenticated dialer is rejected at the TLS handshake.
//! * The **client** ([`RaftTlsConfig::client_tls`]) presents the same leaf +
//!   key as its client identity and verifies the server's cert against the CA,
//!   pinning the expected domain. Because the same node-local cert is used for
//!   both directions, one fixture per node covers inbound and outbound auth.
//!
//! The cluster CA is operator-provisioned (a private CA, step-ca, cert-manager,
//! …). This module does NOT mint certificates in production — it only loads
//! operator-supplied PEM. The tests mint a throwaway CA + leaf certs in-process
//! (`rcgen`) so the mTLS handshake is exercised end-to-end without committing
//! key material.

use basin_common::{BasinError, Result};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// The default SNI / verification hostname when `BASIN_RAFT_TLS_DOMAIN` is
/// unset. Leaf certs minted for a cluster should carry this as a SAN.
pub const DEFAULT_RAFT_TLS_DOMAIN: &str = "basin-raft";

/// Loaded TLS material for the raft transport. Cheap to clone (PEM bytes +
/// a domain string); cloned into the client factory and the server builder.
#[derive(Clone, Debug)]
pub struct RaftTlsConfig {
    /// This node's leaf certificate chain (PEM).
    cert_pem: Vec<u8>,
    /// This node's private key (PEM).
    key_pem: Vec<u8>,
    /// The cluster CA bundle used to verify the peer (PEM).
    ca_pem: Vec<u8>,
    /// SNI / hostname the client verifies the server cert against.
    domain: String,
}

impl RaftTlsConfig {
    /// Build directly from PEM bytes (used by tests with in-memory fixtures).
    pub fn from_pem(
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
        ca_pem: impl Into<Vec<u8>>,
        domain: impl Into<String>,
    ) -> Self {
        Self {
            cert_pem: cert_pem.into(),
            key_pem: key_pem.into(),
            ca_pem: ca_pem.into(),
            domain: domain.into(),
        }
    }

    /// Resolve TLS config from the environment.
    ///
    /// Returns:
    ///   * `Ok(None)` — none of the three required vars set ⇒ plaintext.
    ///   * `Ok(Some(cfg))` — all three set and their files read.
    ///   * `Err(..)` — a partial config (some-but-not-all set) or a file that
    ///     could not be read. A half-configured TLS posture must fail loud, not
    ///     silently downgrade to plaintext.
    pub fn from_env() -> Result<Option<Self>> {
        let cert = env_opt("BASIN_RAFT_TLS_CERT");
        let key = env_opt("BASIN_RAFT_TLS_KEY");
        let ca = env_opt("BASIN_RAFT_TLS_CA");

        match (cert, key, ca) {
            (None, None, None) => Ok(None),
            (Some(cert), Some(key), Some(ca)) => {
                let cert_pem = read_pem(&cert, "BASIN_RAFT_TLS_CERT")?;
                let key_pem = read_pem(&key, "BASIN_RAFT_TLS_KEY")?;
                let ca_pem = read_pem(&ca, "BASIN_RAFT_TLS_CA")?;
                let domain = env_opt("BASIN_RAFT_TLS_DOMAIN")
                    .unwrap_or_else(|| DEFAULT_RAFT_TLS_DOMAIN.to_string());
                Ok(Some(Self {
                    cert_pem,
                    key_pem,
                    ca_pem,
                    domain,
                }))
            }
            (cert, key, ca) => {
                let missing: Vec<&str> = [
                    ("BASIN_RAFT_TLS_CERT", cert.is_none()),
                    ("BASIN_RAFT_TLS_KEY", key.is_none()),
                    ("BASIN_RAFT_TLS_CA", ca.is_none()),
                ]
                .into_iter()
                .filter_map(|(k, missing)| if missing { Some(k) } else { None })
                .collect();
                Err(BasinError::wal(format!(
                    "raft mTLS is partially configured — set ALL of \
                     BASIN_RAFT_TLS_CERT/KEY/CA or none (missing: {})",
                    missing.join(", ")
                )))
            }
        }
    }

    /// The verification / SNI domain leaf certs must carry as a SAN.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Build the tonic [`ServerTlsConfig`]: present this node's identity and
    /// REQUIRE a client cert chaining to the cluster CA (mutual auth).
    pub fn server_tls(&self) -> ServerTlsConfig {
        let identity = Identity::from_pem(&self.cert_pem, &self.key_pem);
        ServerTlsConfig::new()
            .identity(identity)
            // Mandatory client auth: a dialer that cannot present a
            // CA-signed cert is rejected at the handshake.
            .client_ca_root(Certificate::from_pem(&self.ca_pem))
    }

    /// Build the tonic [`ClientTlsConfig`]: present this node's identity as the
    /// client cert and verify the server against the cluster CA + domain.
    pub fn client_tls(&self) -> ClientTlsConfig {
        let identity = Identity::from_pem(&self.cert_pem, &self.key_pem);
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&self.ca_pem))
            .identity(identity)
            .domain_name(self.domain.clone())
    }
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn read_pem(path: &str, key: &str) -> Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| BasinError::wal(format!("raft mTLS: read {key} from '{path}': {e}")))
}
