//! TLS termination for the pgwire listener.
//!
//! Postgres' TLS dance starts with an `SSLRequest` startup message: 8-byte
//! body `0x00 0x00 0x00 0x08 0x04 0xD2 0x16 0x2F` (length + magic code
//! 80877103). The server responds with a single byte — `'S'` to accept TLS
//! or `'N'` to refuse — and on `'S'` the rest of the connection is wrapped
//! in TLS before the regular pgwire startup continues.
//!
//! pgwire 0.28's [`pgwire::tokio::process_socket`] already implements that
//! peek-and-respond dance when a [`TlsAcceptor`] is supplied; this module's
//! only job is to turn a static cert + key (PEM) into the [`TlsAcceptor`]
//! the accept loop hands in.
//!
//! ## What's in / out of scope
//!
//! In: static PEM cert + key, single end-entity cert, PKCS#8 or RSA private
//! key. Out: mTLS, OCSP stapling, cert rotation, ALPN selection, HSM-backed
//! keys. Those are v0.2 — see CAPABILITIES.md.
//!
//! ## Cert format
//!
//! - `cert_pem`: one or more PEM-encoded X.509 certificates concatenated
//!   (end-entity first, then any intermediates). The full chain is presented
//!   to the client.
//! - `key_pem`: a single PEM-encoded private key. Both PKCS#8
//!   (`-----BEGIN PRIVATE KEY-----`) and PKCS#1 RSA
//!   (`-----BEGIN RSA PRIVATE KEY-----`) and SEC1
//!   (`-----BEGIN EC PRIVATE KEY-----`) are accepted; the first key found
//!   in the file wins.

use std::io::{BufReader, Cursor};
use std::sync::Arc;

use basin_common::{BasinError, Result};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// Idempotently installs aws-lc-rs as the process-wide rustls crypto provider.
/// rustls 0.23 requires *some* provider be selected; pgwire's TLS path installs
/// one via its feature flags but only on the path that enters via
/// `process_socket` with TLS — the fresh `ServerConfig::builder()` call below
/// is reached earlier (at startup), so we install ourselves first. `set_default`
/// is a no-op if a provider is already installed, so this is safe to call from
/// multiple acceptors.
fn ensure_default_crypto_provider() {
    use tokio_rustls::rustls::crypto::{aws_lc_rs, CryptoProvider};
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Ignore the result: a provider may already be installed by another
        // crate (e.g. pgwire's secure example path) — that's fine.
        let _ = CryptoProvider::install_default(aws_lc_rs::default_provider());
    });
}

/// Static PEM-encoded TLS material. See module docs for accepted formats.
pub struct TlsConfig {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// Parse `cfg` and return a ready-to-use [`TlsAcceptor`].
///
/// Errors out on: empty cert chain, missing key, malformed PEM, or any
/// rustls config validation failure (e.g. cert/key mismatch).
pub fn build_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor> {
    ensure_default_crypto_provider();
    let mut cert_reader = BufReader::new(Cursor::new(&cfg.cert_pem));
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| BasinError::Internal(format!("TLS cert PEM parse failed: {e}")))?;
    if certs.is_empty() {
        return Err(BasinError::Internal(
            "TLS cert PEM contained no certificates".into(),
        ));
    }

    let mut key_reader = BufReader::new(Cursor::new(&cfg.key_pem));
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| BasinError::Internal(format!("TLS key PEM parse failed: {e}")))?
        .ok_or_else(|| BasinError::Internal("TLS key PEM contained no private key".into()))?;

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| BasinError::Internal(format!("rustls ServerConfig build: {e}")))?;
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal self-signed cert + matching key, generated once at runtime via
    /// `rcgen`, so the test stays hermetic and we don't ship test PEMs.
    fn self_signed() -> (Vec<u8>, Vec<u8>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        (
            cert.cert.pem().into_bytes(),
            cert.key_pair.serialize_pem().into_bytes(),
        )
    }

    #[test]
    fn acceptor_builds_from_self_signed() {
        let (cert_pem, key_pem) = self_signed();
        let acceptor = build_acceptor(&TlsConfig { cert_pem, key_pem })
            .expect("self-signed cert should produce a valid acceptor");
        // TlsAcceptor isn't comparable; just prove we got one.
        let _: TlsAcceptor = acceptor;
    }

    #[test]
    fn empty_cert_pem_is_rejected() {
        let res = build_acceptor(&TlsConfig {
            cert_pem: b"".to_vec(),
            key_pem: b"-----BEGIN PRIVATE KEY-----\n-----END PRIVATE KEY-----\n".to_vec(),
        });
        let err = match res {
            Ok(_) => panic!("expected build_acceptor to fail on empty cert PEM"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(msg.contains("no certificates") || msg.contains("PEM"), "got: {msg}");
    }

    #[test]
    fn missing_key_is_rejected() {
        let (cert_pem, _key_pem) = self_signed();
        let res = build_acceptor(&TlsConfig {
            cert_pem,
            key_pem: b"".to_vec(),
        });
        let err = match res {
            Ok(_) => panic!("expected build_acceptor to fail on empty key PEM"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(msg.contains("no private key") || msg.contains("PEM"), "got: {msg}");
    }
}
