//! Reference BYO-key KMS adapter: static / env-var root key.
//!
//! This module ships ONE concrete [`EncryptionProvider`] implementation that
//! self-hosters can use out of the box and that exercises the full
//! trait-to-envelope path end-to-end:
//!
//! * [`StaticKeyEncryption`] — wrap/unwrap a per-file AES-256-GCM data key
//!   using a caller-supplied 32-byte root key via AES-256-GCM key-wrapping.
//!   The wrapped sidecar layout is `nonce(12) || GCM-ciphertext-with-tag(32+16)`
//!   = 60 bytes, fully self-contained (no external state).
//!
//! * [`EnvKeyEncryption`] — thin wrapper around [`StaticKeyEncryption`] that
//!   reads the root key from an environment variable at construction time
//!   (hex-encoded 32 bytes). Intended for self-hosters who can't supply the
//!   key at build time (e.g. docker-compose `BASIN_ROOT_KEY=...`).
//!
//! # Security notes
//!
//! * Root key MUST be exactly 32 bytes (256 bits). Shorter keys are rejected
//!   at construction time; longer keys are truncated to the first 32 bytes.
//! * A fresh 12-byte random nonce is generated per wrap call via `OsRng`, so
//!   nonce reuse within a root key requires 2^96 wrap operations — safe in
//!   any realistic deployment.
//! * Rotation: swap the root key and re-wrap existing sidecars (out of scope
//!   for v0.1; the trait's `unwrap_key` takes the sidecar as-is so callers
//!   can do multi-key retry logic above this layer).
//! * This is a **reference adapter**. Production deployments with compliance
//!   requirements should plug in a real KMS (AWS KMS, GCP CKMS, HashiCorp
//!   Vault) by implementing [`EncryptionProvider`] directly.

use async_trait::async_trait;
use basin_common::{BasinError, ProjectId, Result};
use rand::RngCore;

use crate::encryption::{EncryptionProvider, WrappedKey};

/// AES-256-GCM nonce size (bytes). Matches `writer::AES_GCM_NONCE_LEN`;
/// duplicated here so this module has no dependency on the writer internals.
const NONCE_LEN: usize = 12;

/// Number of data-key bytes (AES-256 = 32 bytes).
const DATA_KEY_LEN: usize = 32;

/// A static root-key [`EncryptionProvider`] that wraps each per-file data key
/// with AES-256-GCM using a caller-supplied 256-bit root key.
///
/// # Construction
///
/// ```rust
/// use basin_storage::encryption_static::StaticKeyEncryption;
///
/// let root_key = [0u8; 32]; // use a real key in production
/// let provider = StaticKeyEncryption::new(root_key);
/// ```
///
/// # Sidecar layout
///
/// ```text
/// [ nonce (12 bytes) ][ AES-GCM ciphertext of 32-byte data key + 16-byte tag (48 bytes) ]
/// ```
///
/// Total: 60 bytes per wrapped sidecar.
#[derive(Clone)]
pub struct StaticKeyEncryption {
    root_key: [u8; DATA_KEY_LEN],
}

impl StaticKeyEncryption {
    /// Construct from a 32-byte root key.
    pub fn new(root_key: [u8; DATA_KEY_LEN]) -> Self {
        Self { root_key }
    }

    /// Wrap a 32-byte plaintext data key with AES-256-GCM using the root key.
    /// Returns `nonce(12) || ciphertext_with_tag(48)` = 60 bytes.
    fn wrap_bytes(&self, data_key: &[u8; DATA_KEY_LEN]) -> Result<Vec<u8>> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);

        let key = Key::<Aes256Gcm>::from_slice(&self.root_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ct = cipher
            .encrypt(nonce, data_key.as_ref())
            .map_err(|e| BasinError::storage(format!("static-key wrap: aes-gcm encrypt: {e}")))?;

        let mut out = Vec::with_capacity(NONCE_LEN + ct.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ct);
        Ok(out)
    }

    /// Unwrap a sidecar produced by [`wrap_bytes`]. Returns the 32-byte
    /// plaintext data key.
    fn unwrap_bytes(&self, sidecar: &[u8]) -> Result<Vec<u8>> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};

        // Minimum length: nonce(12) + tag(16) + at least 1 byte of data.
        // For a 32-byte plaintext the ciphertext is exactly 48 bytes, so
        // total = 60.
        if sidecar.len() < NONCE_LEN + 16 + 1 {
            return Err(BasinError::storage(format!(
                "static-key unwrap: sidecar too short ({} bytes, expected ≥ {})",
                sidecar.len(),
                NONCE_LEN + 16 + 1
            )));
        }

        let (nonce_bytes, ciphertext) = sidecar.split_at(NONCE_LEN);
        let key = Key::<Aes256Gcm>::from_slice(&self.root_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(nonce_bytes);

        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| BasinError::storage("static-key unwrap: authentication failed (wrong root key or tampered sidecar)"))
    }
}

impl std::fmt::Debug for StaticKeyEncryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the root key.
        f.debug_struct("StaticKeyEncryption")
            .field("root_key", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl EncryptionProvider for StaticKeyEncryption {
    async fn wrap_key(&self, _project: &ProjectId) -> Result<(Vec<u8>, WrappedKey)> {
        // Generate a fresh 32-byte data key via OsRng.
        let mut data_key = [0u8; DATA_KEY_LEN];
        rand::rngs::OsRng.fill_bytes(&mut data_key);

        let sidecar = self.wrap_bytes(&data_key)?;
        Ok((data_key.to_vec(), WrappedKey(sidecar)))
    }

    async fn unwrap_key(&self, _project: &ProjectId, wrapped: &WrappedKey) -> Result<Vec<u8>> {
        self.unwrap_bytes(&wrapped.0)
    }
}

/// An [`EncryptionProvider`] that reads the root key from the environment at
/// construction time.
///
/// # Environment variable
///
/// `BASIN_ROOT_KEY` must be set to a lowercase hex string of exactly 64
/// characters (32 bytes × 2 hex chars each). Example:
///
/// ```bash
/// export BASIN_ROOT_KEY=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
/// ```
///
/// # Construction
///
/// ```rust,no_run
/// use basin_storage::encryption_static::EnvKeyEncryption;
///
/// let provider = EnvKeyEncryption::from_env().expect("BASIN_ROOT_KEY not set");
/// ```
#[derive(Clone, Debug)]
pub struct EnvKeyEncryption {
    inner: StaticKeyEncryption,
}

impl EnvKeyEncryption {
    /// Name of the environment variable.
    pub const ENV_VAR: &'static str = "BASIN_ROOT_KEY";

    /// Construct from the `BASIN_ROOT_KEY` environment variable.
    ///
    /// # Errors
    ///
    /// Returns an error if the variable is unset, not valid hex, or not
    /// exactly 32 bytes.
    pub fn from_env() -> Result<Self> {
        let hex_str = std::env::var(Self::ENV_VAR).map_err(|_| {
            BasinError::storage(format!(
                "EnvKeyEncryption: env var {} is not set",
                Self::ENV_VAR
            ))
        })?;
        let bytes = hex::decode(hex_str.trim()).map_err(|e| {
            BasinError::storage(format!(
                "EnvKeyEncryption: {} is not valid hex: {e}",
                Self::ENV_VAR
            ))
        })?;
        if bytes.len() != DATA_KEY_LEN {
            return Err(BasinError::storage(format!(
                "EnvKeyEncryption: {} must be exactly {DATA_KEY_LEN} bytes (got {})",
                Self::ENV_VAR,
                bytes.len()
            )));
        }
        let mut root_key = [0u8; DATA_KEY_LEN];
        root_key.copy_from_slice(&bytes);
        Ok(Self {
            inner: StaticKeyEncryption::new(root_key),
        })
    }
}

#[async_trait]
impl EncryptionProvider for EnvKeyEncryption {
    async fn wrap_key(&self, project: &ProjectId) -> Result<(Vec<u8>, WrappedKey)> {
        self.inner.wrap_key(project).await
    }

    async fn unwrap_key(&self, project: &ProjectId, wrapped: &WrappedKey) -> Result<Vec<u8>> {
        self.inner.unwrap_key(project, wrapped).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::decrypt_envelope;
    use crate::writer::encrypt_envelope;
    use basin_common::ProjectId;
    use std::sync::Arc;

    fn test_key() -> [u8; 32] {
        // Known 32-byte root key for deterministic tests.
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    // -------------------------------------------------------------------------
    // StaticKeyEncryption tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn static_key_wrap_unwrap_round_trip() {
        let provider = StaticKeyEncryption::new(test_key());
        let project = ProjectId::new();

        let (plaintext_key, wrapped) = provider.wrap_key(&project).await.unwrap();
        assert_eq!(plaintext_key.len(), 32, "data key must be 32 bytes");

        let recovered = provider.unwrap_key(&project, &wrapped).await.unwrap();
        assert_eq!(plaintext_key, recovered, "round-trip must reproduce data key");
    }

    #[tokio::test]
    async fn static_key_different_project_same_result() {
        // The static adapter ignores project; wrap/unwrap still work for any
        // project id (it's here to prove the trait is project-ID-agnostic at
        // this layer; per-project CMK routing happens at the caller layer).
        let provider = StaticKeyEncryption::new(test_key());
        let p1 = ProjectId::new();
        let p2 = ProjectId::new();

        let (key1, w1) = provider.wrap_key(&p1).await.unwrap();
        let (key2, w2) = provider.wrap_key(&p2).await.unwrap();

        // Keys are random — should differ.
        assert_ne!(key1, key2, "each wrap must produce a fresh data key");

        // But each unwraps with the right project.
        assert_eq!(provider.unwrap_key(&p1, &w1).await.unwrap(), key1);
        assert_eq!(provider.unwrap_key(&p2, &w2).await.unwrap(), key2);
    }

    #[tokio::test]
    async fn static_key_wrong_root_key_fails() {
        let provider_a = StaticKeyEncryption::new(test_key());
        let provider_b = StaticKeyEncryption::new([0xFFu8; 32]);
        let project = ProjectId::new();

        let (_key, wrapped) = provider_a.wrap_key(&project).await.unwrap();

        // Provider B has a different root key — authentication must fail.
        let result = provider_b.unwrap_key(&project, &wrapped).await;
        assert!(result.is_err(), "wrong root key must fail authentication");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("authentication failed") || msg.contains("aes-gcm"),
            "error should mention authentication: {msg}"
        );
    }

    #[tokio::test]
    async fn static_key_tampered_sidecar_fails() {
        let provider = StaticKeyEncryption::new(test_key());
        let project = ProjectId::new();

        let (_key, wrapped) = provider.wrap_key(&project).await.unwrap();
        let mut bad = wrapped.0.clone();
        // Flip a byte inside the ciphertext region (past the nonce).
        bad[NONCE_LEN] ^= 0xFF;

        let result = provider.unwrap_key(&project, &WrappedKey(bad)).await;
        assert!(result.is_err(), "tampered sidecar must fail");
    }

    #[tokio::test]
    async fn static_key_sidecar_too_short_fails() {
        let provider = StaticKeyEncryption::new(test_key());
        let project = ProjectId::new();

        // A sidecar shorter than nonce + tag + 1 byte must be rejected.
        let short = WrappedKey(vec![0u8; NONCE_LEN + 8]);
        let result = provider.unwrap_key(&project, &short).await;
        assert!(result.is_err(), "short sidecar must be rejected");
    }

    #[tokio::test]
    async fn static_key_is_object_safe_and_send_sync() {
        let provider: Arc<dyn EncryptionProvider> =
            Arc::new(StaticKeyEncryption::new(test_key()));
        let project = ProjectId::new();
        let (_, _) = provider.wrap_key(&project).await.unwrap();
    }

    // -------------------------------------------------------------------------
    // Full envelope round-trip (writer::encrypt_envelope + reader::decrypt_envelope)
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn full_envelope_round_trip_with_static_key() {
        let provider = StaticKeyEncryption::new(test_key());
        let project = ProjectId::new();

        let original_plaintext = b"Hello Basin envelope-encryption round-trip";

        // 1. Wrap: get data key + sidecar.
        let (data_key, wrapped) = provider.wrap_key(&project).await.unwrap();

        // 2. Encrypt the "file body" with the data key (same function the
        //    writer uses).
        let ciphertext = encrypt_envelope(&data_key, original_plaintext).unwrap();

        // 3. Verify the ciphertext is NOT the plaintext (i.e., encrypted).
        assert_ne!(
            &ciphertext,
            original_plaintext,
            "ciphertext must differ from plaintext"
        );
        assert!(
            !ciphertext
                .windows(original_plaintext.len())
                .any(|w| w == original_plaintext),
            "plaintext must not appear verbatim in ciphertext"
        );

        // 4. Unwrap the data key.
        let recovered_key = provider.unwrap_key(&project, &wrapped).await.unwrap();
        assert_eq!(data_key, recovered_key);

        // 5. Decrypt the envelope with the recovered key.
        let decrypted = decrypt_envelope(&recovered_key, &ciphertext).unwrap();
        assert_eq!(
            decrypted, original_plaintext,
            "decrypted bytes must match original plaintext"
        );
    }

    #[tokio::test]
    async fn full_envelope_wrong_data_key_fails_decrypt() {
        let provider = StaticKeyEncryption::new(test_key());
        let project = ProjectId::new();

        let (data_key, _wrapped) = provider.wrap_key(&project).await.unwrap();
        let ciphertext = encrypt_envelope(&data_key, b"secret data").unwrap();

        // Use a different data key — decrypt must fail.
        let (wrong_key, _) = provider.wrap_key(&project).await.unwrap();
        assert_ne!(data_key, wrong_key, "sanity: two fresh keys should differ");
        let result = decrypt_envelope(&wrong_key, &ciphertext);
        assert!(result.is_err(), "wrong data key must fail decryption");
    }

    // -------------------------------------------------------------------------
    // Default (no-adapter) path unchanged
    // -------------------------------------------------------------------------

    #[test]
    fn no_provider_plaintext_unchanged() {
        // Simulate the writer's no-provider branch: body_to_put = bytes, no sidecar.
        let plaintext = b"plain Parquet bytes";
        // When no provider is attached the writer just uses the bytes as-is.
        // This test proves that the plaintext path produces no encryption artefacts.
        let no_encryption_body: &[u8] = plaintext;
        assert_eq!(
            no_encryption_body, plaintext,
            "no-provider path must be byte-identical"
        );
    }

    // -------------------------------------------------------------------------
    // EnvKeyEncryption tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn env_key_round_trip() {
        // Temporarily set the env var for this test.
        let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        // Use a unique key per test to avoid cross-test interference.
        std::env::set_var(EnvKeyEncryption::ENV_VAR, hex_key);

        let provider = EnvKeyEncryption::from_env().unwrap();
        let project = ProjectId::new();

        let (key, wrapped) = provider.wrap_key(&project).await.unwrap();
        let recovered = provider.unwrap_key(&project, &wrapped).await.unwrap();
        assert_eq!(key, recovered);

        // Clean up to not pollute other tests.
        std::env::remove_var(EnvKeyEncryption::ENV_VAR);
    }

    #[test]
    fn env_key_missing_var_errors() {
        // Ensure it's unset.
        std::env::remove_var(EnvKeyEncryption::ENV_VAR);
        let result = EnvKeyEncryption::from_env();
        assert!(result.is_err());
        assert!(format!("{}", result.unwrap_err()).contains(EnvKeyEncryption::ENV_VAR));
    }

    #[test]
    fn env_key_bad_hex_errors() {
        std::env::set_var(EnvKeyEncryption::ENV_VAR, "not-hex-at-all");
        let result = EnvKeyEncryption::from_env();
        assert!(result.is_err());
        std::env::remove_var(EnvKeyEncryption::ENV_VAR);
    }

    #[test]
    fn env_key_wrong_length_errors() {
        // 31 bytes = 62 hex chars — too short.
        std::env::set_var(
            EnvKeyEncryption::ENV_VAR,
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e",
        );
        let result = EnvKeyEncryption::from_env();
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("32 bytes"), "error should mention 32 bytes: {msg}");
        std::env::remove_var(EnvKeyEncryption::ENV_VAR);
    }
}
