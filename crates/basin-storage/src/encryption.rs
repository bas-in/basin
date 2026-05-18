//! Envelope-encryption hooks for BYO-key storage. The trait is additive:
//! a `Storage` with no provider set is byte-for-byte identical to today's
//! plaintext path. When set, every PUT envelope-encrypts with a fresh
//! per-file data key wrapped by the provider's CMK; every GET unwraps and
//! decrypts. The provider is responsible for the wrap/unwrap round trips
//! to the underlying KMS — basin-storage only orchestrates.

use async_trait::async_trait;
use basin_catalog::ProjectStorageConfig;
use basin_common::{BasinError, ProjectId, Result};
use bytes::Bytes;
use futures::future::FutureExt;

/// Wrapped data-encryption key — opaque to basin-storage. Adapter encodes
/// whatever metadata the underlying KMS needs (key id, version, IV) into
/// the bytes; basin-storage stores them as a sidecar alongside the
/// ciphertext file and hands them back unmodified at unwrap time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WrappedKey(pub Vec<u8>);

/// Project-scoped envelope-encryption provider. Implementations must be
/// `Send + Sync` and cheap to clone (`Arc` inside) — one provider per
/// process, looked up per project.
#[async_trait]
pub trait EncryptionProvider: Send + Sync {
    /// Generate a fresh data key for `project`, wrap it via the project's
    /// CMK, and return both the plaintext key (the caller uses it to
    /// AES-GCM the file body) and the wrapped form (basin-storage
    /// persists it as a sidecar).
    async fn wrap_key(&self, project: &ProjectId) -> Result<(Vec<u8>, WrappedKey)>;

    /// Inverse of `wrap_key`. Given the wrapped sidecar, return the
    /// plaintext data key. Used at GET time.
    async fn unwrap_key(&self, project: &ProjectId, wrapped: &WrappedKey) -> Result<Vec<u8>>;

    /// Optional: when the engine has a [`ProjectStorageConfig`] for this
    /// project, it threads it through here so the impl can route to the
    /// per-project CMK instead of its default. Default impl ignores the
    /// config and falls back to plain `wrap_key`, preserving backwards
    /// compatibility for impls written against the original two-method
    /// trait.
    async fn wrap_key_with_config(
        &self,
        project: &ProjectId,
        _config: &ProjectStorageConfig,
    ) -> Result<(Vec<u8>, WrappedKey)> {
        self.wrap_key(project).await
    }

    /// Symmetric for unwrap. Default impl forwards to plain `unwrap_key`.
    async fn unwrap_key_with_config(
        &self,
        project: &ProjectId,
        wrapped: &WrappedKey,
        _config: &ProjectStorageConfig,
    ) -> Result<Vec<u8>> {
        self.unwrap_key(project, wrapped).await
    }
}

/// AES-GCM decrypt an envelope produced by the writer. The on-disk layout
/// is `nonce(12) || ciphertext_with_tag`; this is the inverse of
/// `writer::encrypt_envelope`. Returns the recovered plaintext (the
/// Parquet bytes).
pub(crate) fn decrypt_envelope(data_key: &[u8], envelope: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::aead::Aead;
    use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

    if data_key.len() != 32 {
        return Err(BasinError::storage(format!(
            "envelope decrypt: data key is {} bytes, expected 32",
            data_key.len()
        )));
    }
    if envelope.len() < crate::writer::AES_GCM_NONCE_LEN {
        return Err(BasinError::storage(format!(
            "envelope decrypt: body too short ({} bytes)",
            envelope.len()
        )));
    }
    let key = aes_gcm::Key::<Aes256Gcm>::from_slice(data_key);
    let cipher = Aes256Gcm::new(key);
    let (nonce_bytes, ciphertext) = envelope.split_at(crate::writer::AES_GCM_NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| BasinError::storage(format!("aes-gcm decrypt: {e}")))
}

/// In-memory `AsyncFileReader` over a `Bytes` blob. Lets the reader run
/// the parquet pipeline against decrypted-into-memory file bodies without
/// faking an `ObjectStore`. Range reads slice the underlying `Bytes`
/// (zero-copy); metadata fetches parse the standard Parquet footer.
pub(crate) struct BytesFileReader {
    pub bytes: Bytes,
}

impl parquet::arrow::async_reader::AsyncFileReader for BytesFileReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let bytes = self.bytes.clone();
        async move {
            let start = range.start as usize;
            let end = range.end as usize;
            if end > bytes.len() {
                return Err(parquet::errors::ParquetError::General(format!(
                    "BytesFileReader: range {:?} out of bounds (len={})",
                    range,
                    bytes.len()
                )));
            }
            Ok(bytes.slice(start..end))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> futures::future::BoxFuture<
        'a,
        parquet::errors::Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        let bytes = self.bytes.clone();
        async move {
            let mut reader = parquet::file::metadata::ParquetMetaDataReader::new();
            reader.try_parse(&bytes)?;
            Ok(std::sync::Arc::new(reader.finish()?))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::BasinError;
    use std::sync::Arc;

    /// Test-only provider: the "wrap" is a constant XOR of the data key.
    /// Deterministic so we can assert tampered sidecars fail to unwrap.
    struct MockProvider {
        mask: u8,
    }

    #[async_trait]
    impl EncryptionProvider for MockProvider {
        async fn wrap_key(&self, _project: &ProjectId) -> Result<(Vec<u8>, WrappedKey)> {
            // Fixed 32-byte key for reproducibility; real adapters draw
            // from a CSPRNG / KMS GenerateDataKey.
            let plaintext: Vec<u8> = (0u8..32).collect();
            let wrapped: Vec<u8> = plaintext.iter().map(|b| b ^ self.mask).collect();
            // Prepend a one-byte version so a tampered sidecar can be
            // detected on unwrap.
            let mut sidecar = vec![0x01];
            sidecar.extend_from_slice(&wrapped);
            Ok((plaintext, WrappedKey(sidecar)))
        }

        async fn unwrap_key(&self, _project: &ProjectId, wrapped: &WrappedKey) -> Result<Vec<u8>> {
            let bytes = &wrapped.0;
            if bytes.first().copied() != Some(0x01) {
                return Err(BasinError::storage("mock unwrap: bad version byte"));
            }
            Ok(bytes[1..].iter().map(|b| b ^ self.mask).collect())
        }
    }

    #[tokio::test]
    async fn round_trip_wrap_unwrap() {
        let p = MockProvider { mask: 0xA5 };
        let project = ProjectId::new();
        let (plain, wrapped) = p.wrap_key(&project).await.unwrap();
        let unwrapped = p.unwrap_key(&project, &wrapped).await.unwrap();
        assert_eq!(plain, unwrapped);
    }

    #[tokio::test]
    async fn tampered_wrapped_key_errors() {
        let p = MockProvider { mask: 0xA5 };
        let project = ProjectId::new();
        let (_plain, wrapped) = p.wrap_key(&project).await.unwrap();
        let mut bad = wrapped.0.clone();
        bad[0] = 0xFF; // corrupt the version byte
        let res = p.unwrap_key(&project, &WrappedKey(bad)).await;
        assert!(res.is_err());
    }

    /// Smoke check that the trait is dyn-compatible — what `Storage`
    /// stores as `Option<Arc<dyn EncryptionProvider>>`.
    #[tokio::test]
    async fn provider_is_object_safe() {
        let p: Arc<dyn EncryptionProvider> = Arc::new(MockProvider { mask: 0x7E });
        let project = ProjectId::new();
        let _ = p.wrap_key(&project).await.unwrap();
    }
}
