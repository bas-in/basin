//! End-to-end coverage for the per-project storage-config routing in the
//! storage encryption call path: writer should call
//! `wrap_key_with_config` when a config is present, plain `wrap_key`
//! otherwise; the reader's unwrap path mirrors. Cache invalidation on
//! `set_project_storage_config` is also asserted.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog, ProjectStorageConfig};
use basin_common::{PartitionKey, ProjectId, Result, TableName};
use basin_storage::{EncryptionProvider, Storage, StorageConfig, WrappedKey};
use object_store::memory::InMemory;

#[derive(Default)]
struct CallCounters {
    wrap_plain: AtomicUsize,
    wrap_with_cfg: AtomicUsize,
    unwrap_plain: AtomicUsize,
    unwrap_with_cfg: AtomicUsize,
    last_kms_key_ref: std::sync::Mutex<Option<String>>,
}

/// Test provider that records which method was called and (optionally)
/// derives the data key from the per-project config so we can prove the
/// config-aware path is the one decrypting.
struct RecordingProvider {
    counters: Arc<CallCounters>,
}

impl RecordingProvider {
    fn new() -> (Self, Arc<CallCounters>) {
        let c = Arc::new(CallCounters::default());
        (
            Self {
                counters: c.clone(),
            },
            c,
        )
    }

    /// Derive a deterministic 32-byte data key from the optional config
    /// so wrap and unwrap line up regardless of whether the no-config or
    /// with-config path was taken on each leg.
    fn data_key(cfg: Option<&ProjectStorageConfig>) -> Vec<u8> {
        let mut k = vec![0u8; 32];
        if let Some(cfg) = cfg {
            if let Some(r) = &cfg.kms_key_ref {
                let bytes = r.as_bytes();
                for (i, b) in bytes.iter().take(32).enumerate() {
                    k[i] = *b;
                }
            }
        }
        k
    }
}

#[async_trait]
impl EncryptionProvider for RecordingProvider {
    async fn wrap_key(&self, _project: &ProjectId) -> Result<(Vec<u8>, WrappedKey)> {
        self.counters.wrap_plain.fetch_add(1, Ordering::Relaxed);
        let key = Self::data_key(None);
        // Sidecar carries a tag identifying the path so unwrap can
        // recover the matching key. `0x00` = plain.
        let mut sidecar = vec![0x00];
        sidecar.extend_from_slice(&key);
        Ok((key, WrappedKey(sidecar)))
    }

    async fn unwrap_key(&self, _project: &ProjectId, wrapped: &WrappedKey) -> Result<Vec<u8>> {
        self.counters.unwrap_plain.fetch_add(1, Ordering::Relaxed);
        Ok(wrapped.0[1..].to_vec())
    }

    async fn wrap_key_with_config(
        &self,
        _project: &ProjectId,
        config: &ProjectStorageConfig,
    ) -> Result<(Vec<u8>, WrappedKey)> {
        self.counters.wrap_with_cfg.fetch_add(1, Ordering::Relaxed);
        *self.counters.last_kms_key_ref.lock().unwrap() = config.kms_key_ref.clone();
        let key = Self::data_key(Some(config));
        let mut sidecar = vec![0x01];
        sidecar.extend_from_slice(&key);
        Ok((key, WrappedKey(sidecar)))
    }

    async fn unwrap_key_with_config(
        &self,
        _project: &ProjectId,
        wrapped: &WrappedKey,
        _config: &ProjectStorageConfig,
    ) -> Result<Vec<u8>> {
        self.counters
            .unwrap_with_cfg
            .fetch_add(1, Ordering::Relaxed);
        Ok(wrapped.0[1..].to_vec())
    }
}

fn build_storage() -> (Storage, Arc<InMemoryCatalog>) {
    let cat = Arc::new(InMemoryCatalog::new());
    let s = Storage::new(StorageConfig {
        object_store: Arc::new(InMemory::new()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    s.attach_catalog(cat.clone() as Arc<dyn Catalog>);
    (s, cat)
}

fn small_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Int64Array = (0..16).collect();
    RecordBatch::try_new(schema, vec![Arc::new(ids)]).unwrap()
}

fn cfg_with(key: &str) -> ProjectStorageConfig {
    let mut extras = BTreeMap::new();
    extras.insert("region".into(), "us-east-1".into());
    ProjectStorageConfig {
        kms_key_ref: Some(key.into()),
        provider_extras: extras,
    }
}

#[tokio::test]
async fn wrap_key_with_config_routes_when_config_present() {
    let (storage, _cat) = build_storage();
    let (provider, counters) = RecordingProvider::new();
    storage.attach_encryption_provider(Arc::new(provider));

    let project = ProjectId::new();
    storage
        .set_project_storage_config(&project, cfg_with("arn:aws:kms:us-east-1:1:key/abc"))
        .await
        .unwrap();

    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();
    storage
        .write_batch(&project, &table, &part, &small_batch())
        .await
        .unwrap();

    assert_eq!(counters.wrap_with_cfg.load(Ordering::Relaxed), 1);
    assert_eq!(counters.wrap_plain.load(Ordering::Relaxed), 0);
    assert_eq!(
        counters.last_kms_key_ref.lock().unwrap().as_deref(),
        Some("arn:aws:kms:us-east-1:1:key/abc")
    );
}

#[tokio::test]
async fn wrap_key_falls_back_when_no_config() {
    let (storage, _cat) = build_storage();
    let (provider, counters) = RecordingProvider::new();
    storage.attach_encryption_provider(Arc::new(provider));

    // No `set_project_storage_config` call — the cached lookup returns
    // None and the writer falls back to plain `wrap_key`.
    let project = ProjectId::new();
    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();
    storage
        .write_batch(&project, &table, &part, &small_batch())
        .await
        .unwrap();

    assert_eq!(counters.wrap_plain.load(Ordering::Relaxed), 1);
    assert_eq!(counters.wrap_with_cfg.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn cache_invalidated_on_config_update() {
    let (storage, _cat) = build_storage();
    let (provider, counters) = RecordingProvider::new();
    storage.attach_encryption_provider(Arc::new(provider));

    let project = ProjectId::new();
    storage
        .set_project_storage_config(&project, cfg_with("first-cmk"))
        .await
        .unwrap();

    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();
    storage
        .write_batch(&project, &table, &part, &small_batch())
        .await
        .unwrap();
    assert_eq!(
        counters.last_kms_key_ref.lock().unwrap().as_deref(),
        Some("first-cmk")
    );

    storage
        .set_project_storage_config(&project, cfg_with("second-cmk"))
        .await
        .unwrap();
    storage
        .write_batch(&project, &table, &part, &small_batch())
        .await
        .unwrap();
    assert_eq!(
        counters.last_kms_key_ref.lock().unwrap().as_deref(),
        Some("second-cmk")
    );
    assert_eq!(counters.wrap_with_cfg.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn unwrap_path_threads_config() {
    use futures::stream::StreamExt;
    let (storage, _cat) = build_storage();
    let (provider, counters) = RecordingProvider::new();
    storage.attach_encryption_provider(Arc::new(provider));

    let project = ProjectId::new();
    storage
        .set_project_storage_config(&project, cfg_with("round-trip-cmk"))
        .await
        .unwrap();

    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();
    storage
        .write_batch(&project, &table, &part, &small_batch())
        .await
        .unwrap();

    let stream = storage
        .read(&project, &table, basin_storage::ReadOptions::default())
        .await
        .unwrap();
    let batches: Vec<_> = stream.collect().await;
    let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(total, 16);

    assert_eq!(counters.wrap_with_cfg.load(Ordering::Relaxed), 1);
    assert!(counters.unwrap_with_cfg.load(Ordering::Relaxed) >= 1);
    // The plaintext-shape methods should not have fired on either leg.
    assert_eq!(counters.wrap_plain.load(Ordering::Relaxed), 0);
    assert_eq!(counters.unwrap_plain.load(Ordering::Relaxed), 0);
}

/// Stub provider that does not override the with-config methods, to
/// confirm the default-impl forwarding keeps such impls working.
struct LegacyProvider;

#[async_trait]
impl EncryptionProvider for LegacyProvider {
    async fn wrap_key(&self, _project: &ProjectId) -> Result<(Vec<u8>, WrappedKey)> {
        Ok((vec![7u8; 32], WrappedKey(vec![0xAA; 8])))
    }
    async fn unwrap_key(&self, _project: &ProjectId, _wrapped: &WrappedKey) -> Result<Vec<u8>> {
        Ok(vec![7u8; 32])
    }
}

#[tokio::test]
async fn legacy_provider_default_impl_forwards_to_wrap_key() {
    // A provider without `wrap_key_with_config` overridden still works:
    // the default-impl forwards to `wrap_key` so the encryption call
    // path remains valid even when a per-project config is present.
    let (storage, _cat) = build_storage();
    storage.attach_encryption_provider(Arc::new(LegacyProvider));

    let project = ProjectId::new();
    storage
        .set_project_storage_config(&project, cfg_with("ignored"))
        .await
        .unwrap();

    let table = TableName::new("t").unwrap();
    let part = PartitionKey::default_key();
    storage
        .write_batch(&project, &table, &part, &small_batch())
        .await
        .unwrap();
    // No assertion needed beyond not panicking — we just verified that
    // wrap_key_with_config defaulting back to wrap_key produces a valid
    // sidecar that the writer can persist. (The reader-side default-impl
    // forwarding is structurally identical and exercised by the
    // legacy MockProvider unit test in `encryption.rs`.)
}
