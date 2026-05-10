//! Viability test: BYO-key envelope-encryption hooks.
//!
//! Card: `viability_kms_envelope`
//! Bar: with a `MockProvider` attached, written Parquet bodies are NOT
//! the same bytes as a plaintext write of the same batch (encryption is
//! actually happening), the `<key>.wrapped` sidecar is present, and a
//! read on the encrypted `Storage` returns the original rows. Detaching
//! the provider (== fresh Storage with no provider) preserves backwards
//! compatibility on plaintext files.
//!
//! This is the load-bearing integration test for the
//! `EncryptionProvider` trait shipped in basin-storage. External KMS
//! adapters plug into this same trait without trait changes.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_common::{BasinError, PartitionKey, Result, TableName, TenantId};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::encryption::{EncryptionProvider, WrappedKey};
use basin_storage::{ReadOptions, Storage, StorageConfig};
use futures::stream::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde_json::json;
use tempfile::TempDir;
use walkdir::WalkDir;

/// XOR-mask test provider: deterministic so we can round-trip the key
/// without a real KMS. Real adapters (AWS KMS GenerateDataKey,
/// GCP KMS encrypt) wrap a fresh CSPRNG-drawn key with their CMK and
/// return the ciphertext blob; basin-storage stores it verbatim.
struct MockProvider {
    mask: u8,
}

#[async_trait]
impl EncryptionProvider for MockProvider {
    async fn wrap_key(&self, _tenant: &TenantId) -> Result<(Vec<u8>, WrappedKey)> {
        // 32 bytes = AES-256-GCM key. Drawn deterministically here so
        // re-wraps are reproducible across the test; production
        // adapters draw from `OsRng`.
        let plaintext: Vec<u8> = (0u8..32).collect();
        let wrapped: Vec<u8> = plaintext.iter().map(|b| b ^ self.mask).collect();
        let mut sidecar = vec![0xC0]; // version byte
        sidecar.extend_from_slice(&wrapped);
        Ok((plaintext, WrappedKey(sidecar)))
    }

    async fn unwrap_key(&self, _tenant: &TenantId, wrapped: &WrappedKey) -> Result<Vec<u8>> {
        let bytes = &wrapped.0;
        if bytes.first().copied() != Some(0xC0) {
            return Err(BasinError::storage("mock unwrap: bad version byte"));
        }
        Ok(bytes[1..].iter().map(|b| b ^ self.mask).collect())
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let names: Vec<String> = (0..len)
        .map(|i| format!("kms-row-{:08x}", start + i as i64))
        .collect();
    let name_arr: StringArray = names.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(name_arr)]).unwrap()
}

/// Walk a bucket directory and return the first regular file ending in
/// the given suffix. Used to assert the encrypted body / sidecar layout
/// from the test, sidestepping the path-resolution logic in `paths.rs`.
fn first_file_with_suffix(root: &std::path::Path, suffix: &str) -> Option<std::path::PathBuf> {
    for e in WalkDir::new(root).into_iter().flatten() {
        if e.file_type().is_file() && e.path().to_string_lossy().ends_with(suffix) {
            return Some(e.path().to_path_buf());
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn viability_kms_envelope() {
    basin_common::telemetry::try_init_for_tests();

    let tenant = TenantId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();
    let batch = make_batch(0, 100);

    // ---- 1. Plaintext baseline. Same batch, no provider attached. We
    // capture the on-disk bytes so we can prove the encrypted run
    // produced different bytes (encryption is actually happening, not
    // just no-op'd).
    let plain_dir = TempDir::new().unwrap();
    let plain_storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(plain_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let _ = plain_storage
        .write_batch(&tenant, &table, &part, &batch)
        .await
        .expect("plain write");
    let plain_path =
        first_file_with_suffix(plain_dir.path(), ".parquet").expect("plaintext parquet exists");
    let plain_bytes = std::fs::read(&plain_path).unwrap();
    // Must start with the Parquet magic. Sanity that we wrote a real
    // Parquet file in the no-provider path.
    assert_eq!(
        &plain_bytes[..4],
        b"PAR1",
        "plaintext path lost Parquet magic — check writer is unchanged when no provider attached"
    );

    // ---- 2. Encrypted run. Same batch, provider attached. ---------
    let enc_dir = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(enc_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let provider: Arc<dyn EncryptionProvider> = Arc::new(MockProvider { mask: 0x5A });
    storage.attach_encryption_provider(provider);

    let df = storage
        .write_batch(&tenant, &table, &part, &batch)
        .await
        .expect("encrypted write");

    // Body file exists and is NOT plaintext bytes.
    let enc_path =
        first_file_with_suffix(enc_dir.path(), ".parquet").expect("encrypted parquet path exists");
    let enc_bytes = std::fs::read(&enc_path).unwrap();
    assert_ne!(
        &enc_bytes[..4.min(enc_bytes.len())],
        b"PAR1",
        "encrypted body must not start with the Parquet magic"
    );
    assert_ne!(
        enc_bytes, plain_bytes,
        "encrypted body must not match the plaintext bytes"
    );

    // Sidecar exists at <key>.wrapped.
    let sidecar_path = first_file_with_suffix(enc_dir.path(), ".parquet.wrapped")
        .expect("sidecar `<key>.parquet.wrapped` exists");
    let sidecar_bytes = std::fs::read(&sidecar_path).unwrap();
    assert!(
        !sidecar_bytes.is_empty(),
        "sidecar must contain the wrapped key"
    );

    // ---- 3. Round-trip read on the encrypted Storage returns the
    // input rows. The reader transparently fetches the sidecar,
    // unwraps, AES-GCM-decrypts, and decodes Parquet.
    let stream = storage
        .read(&tenant, &table, ReadOptions::default())
        .await
        .expect("read");
    let batches: Vec<_> = stream.collect::<Vec<_>>().await;
    let total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(total, batch.num_rows(), "row count round-trip mismatch");

    // Confirm the values, not just the row count. Pick the first batch's
    // first id+name and match against the input.
    let first = batches[0].as_ref().unwrap();
    let ids = first
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let names = first
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), 0);
    assert_eq!(names.value(0), "kms-row-00000000");

    // ---- 4. Back-compat: a fresh Storage with NO provider attached
    // can still read previously-plaintext files. Prove this by reading
    // back the plaintext bucket with no provider.
    let plain_storage_2 = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(plain_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let stream = plain_storage_2
        .read(&tenant, &table, ReadOptions::default())
        .await
        .expect("plain read");
    let batches: Vec<_> = stream.collect::<Vec<_>>().await;
    let plain_total: usize = batches.iter().map(|b| b.as_ref().unwrap().num_rows()).sum();
    assert_eq!(
        plain_total,
        batch.num_rows(),
        "plaintext bucket must remain readable without a provider"
    );

    println!(
        "[VIABILITY kms_envelope] enc_size={} plain_size={} sidecar_size={} df_path={}",
        enc_bytes.len(),
        plain_bytes.len(),
        sidecar_bytes.len(),
        df.path,
    );

    // Final dashboard signal.
    let primary = PrimaryMetric {
        label: "rows round-tripped".to_string(),
        value: total as f64,
        unit: "rows".to_string(),
        bar: BarOp::eq(batch.num_rows() as f64),
    };
    let details = json!({
        "encrypted_body_bytes": enc_bytes.len(),
        "plaintext_body_bytes": plain_bytes.len(),
        "sidecar_bytes": sidecar_bytes.len(),
        "rows_round_tripped": total,
        "data_file_path": df.path.to_string(),
    });
    report_viability(
        "kms_envelope",
        "BYO-key envelope encryption",
        "MockProvider attached: bytes != plaintext, sidecar present, read returns input rows; back-compat with provider-less files preserved.",
        primary.bar.evaluate(primary.value),
        primary,
        details,
    );

    // Sanity: the path-rooted ObjectStore *also* sees the sidecar via
    // its own listing API (paranoia: confirm we didn't side-channel
    // through walkdir).
    let raw: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(enc_dir.path()).unwrap());
    let mut found_sidecar = false;
    let mut s = raw.list(None);
    while let Some(meta) = s.next().await {
        let m = meta.unwrap();
        if m.location.as_ref().ends_with(".parquet.wrapped") {
            found_sidecar = true;
            break;
        }
    }
    assert!(
        found_sidecar,
        "ObjectStore::list did not surface the .wrapped sidecar"
    );
    let _ = ObjectPath::from(""); // type assert it imports
}
