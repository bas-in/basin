//! Per-project storage configuration. Carries opaque-to-OSS settings any
//! `EncryptionProvider` impl (in `basin-storage`) can read to route
//! per-project CMK lookups, plus any future per-project storage-level
//! settings that don't belong on the process-global `StorageConfig`.
//!
//! The shape is intentionally small and provider-agnostic. `kms_key_ref`
//! is a string the impl knows how to resolve (an AWS CMK ARN, a GCP KMS
//! resource path, a Vault URL, etc.); `provider_extras` is a free-form
//! key/value bag for provider-specific knobs (assume-role ARNs,
//! impersonated service accounts, regional overrides). The OSS engine
//! never inspects either field — it persists them via the [`crate::Catalog`]
//! and threads them through to the provider via the config-aware
//! variants of `EncryptionProvider`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Per-project storage configuration. Persisted by the catalog; consumed
/// by the encryption call path. Defaults to "no per-project routing" which
/// preserves the legacy `wrap_key` / `unwrap_key` behaviour.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectStorageConfig {
    /// External KMS reference. Format is provider-specific and opaque to
    /// basin-storage — typically a fully-qualified CMK identifier the
    /// caller's `EncryptionProvider` impl knows how to resolve. Examples:
    ///   "arn:aws:kms:us-east-1:123456789012:key/abcd-..."
    ///   "projects/my-project/locations/us/keyRings/r/cryptoKeys/k"
    ///   "https://my-vault.vault.azure.net/keys/key-name/version"
    /// `None` = default (no per-project KMS routing; provider falls back to
    /// its global default behaviour).
    pub kms_key_ref: Option<String>,

    /// Free-form key/value bag for provider-specific settings (e.g. AWS
    /// `assume_role_arn`, GCP impersonated SA, regional override). Opaque
    /// to basin-storage — keys + values are arbitrary strings the impl
    /// agrees on. Per-project cost discipline: keep small (< 1KB total).
    pub provider_extras: BTreeMap<String, String>,
}
