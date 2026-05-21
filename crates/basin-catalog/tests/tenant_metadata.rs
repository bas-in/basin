//! Integration tests for TenantMetadata catalog API and S3Config serde
//! round-trips (T-048).
//!
//! In-memory cases run unconditionally. The Postgres durability case skips
//! silently if Postgres is not reachable, mirroring the pattern used by the
//! rest of the basin-catalog suite.

use basin_catalog::{Catalog, InMemoryCatalog, S3Config, TenantMetadata};
use basin_common::ProjectId;

// -------------------------------------------------------------------------
// S3Config serde round-trips
// -------------------------------------------------------------------------

#[test]
fn s3_config_round_trips_json() {
    let cfg = S3Config {
        endpoint: "https://s3.amazonaws.com".into(),
        bucket: "my-bucket".into(),
        region: "us-east-1".into(),
        access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
        secret_access_key_enc: vec![1, 2, 3, 4, 5],
        force_path_style: false,
    };
    let json = serde_json::to_string(&cfg).unwrap();
    let back: S3Config = serde_json::from_str(&json).unwrap();
    assert_eq!(cfg, back);
}

#[test]
fn s3_config_force_path_style_round_trips() {
    let cfg = S3Config {
        endpoint: "https://minio.example.com".into(),
        bucket: "test".into(),
        region: "us-east-1".into(),
        access_key_id: "minio_user".into(),
        secret_access_key_enc: vec![0xde, 0xad, 0xbe, 0xef],
        force_path_style: true,
    };
    let json = serde_json::to_string(&cfg).unwrap();
    let back: S3Config = serde_json::from_str(&json).unwrap();
    assert!(back.force_path_style);
}

// -------------------------------------------------------------------------
// TenantMetadata serde round-trips
// -------------------------------------------------------------------------

#[test]
fn tenant_metadata_default_is_none() {
    let meta = TenantMetadata::default();
    assert!(meta.byo_bucket.is_none());
}

#[test]
fn tenant_metadata_default_serialises_without_byo_bucket_key() {
    let meta = TenantMetadata::default();
    let json = serde_json::to_string(&meta).unwrap();
    // skip_serializing_if = "Option::is_none" should suppress the key entirely.
    assert!(
        !json.contains("byo_bucket"),
        "expected no byo_bucket key in JSON, got: {json}"
    );
}

#[test]
fn tenant_metadata_with_byo_bucket_round_trips() {
    let meta = TenantMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://fly.storage.tigris.dev".into(),
            bucket: "customer-bucket".into(),
            region: "auto".into(),
            access_key_id: "tid_test_key".into(),
            secret_access_key_enc: b"encrypted_blob".to_vec(),
            force_path_style: false,
        }),
    };
    let json = serde_json::to_string(&meta).unwrap();
    let back: TenantMetadata = serde_json::from_str(&json).unwrap();
    assert_eq!(meta, back);
}

#[test]
fn tenant_metadata_deserialises_from_empty_object() {
    // Back-compat: existing catalog rows without byo_bucket should
    // deserialise cleanly to the default (None).
    let meta: TenantMetadata = serde_json::from_str("{}").unwrap();
    assert!(meta.byo_bucket.is_none());
}

// -------------------------------------------------------------------------
// Catalog API — in-memory
// -------------------------------------------------------------------------

#[tokio::test]
async fn get_returns_default_when_unset() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let got = cat.get_tenant_metadata(&project).await.unwrap();
    assert!(
        got.byo_bucket.is_none(),
        "unset project should return default TenantMetadata"
    );
}

#[tokio::test]
async fn set_then_get_round_trips_in_memory() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let meta = TenantMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://fly.storage.tigris.dev".into(),
            bucket: "acme-bucket".into(),
            region: "auto".into(),
            access_key_id: "tid_key".into(),
            secret_access_key_enc: vec![0x42, 0x43],
            force_path_style: false,
        }),
    };
    cat.set_tenant_metadata(&project, meta.clone())
        .await
        .unwrap();
    let got = cat.get_tenant_metadata(&project).await.unwrap();
    assert_eq!(got, meta);
}

#[tokio::test]
async fn set_overwrites_existing_in_memory() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let first = TenantMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://first.example.com".into(),
            bucket: "b1".into(),
            region: "us-east-1".into(),
            access_key_id: "k1".into(),
            secret_access_key_enc: vec![1],
            force_path_style: false,
        }),
    };
    let second = TenantMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://second.example.com".into(),
            bucket: "b2".into(),
            region: "eu-west-1".into(),
            access_key_id: "k2".into(),
            secret_access_key_enc: vec![2],
            force_path_style: true,
        }),
    };
    cat.set_tenant_metadata(&project, first).await.unwrap();
    cat.set_tenant_metadata(&project, second.clone())
        .await
        .unwrap();
    let got = cat.get_tenant_metadata(&project).await.unwrap();
    assert_eq!(got, second);
}

#[tokio::test]
async fn set_none_byo_bucket_clears_it() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let with_byo = TenantMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key_enc: vec![],
            force_path_style: false,
        }),
    };
    cat.set_tenant_metadata(&project, with_byo).await.unwrap();

    // Overwrite with default (None) to clear the BYO bucket.
    cat.set_tenant_metadata(&project, TenantMetadata::default())
        .await
        .unwrap();
    let got = cat.get_tenant_metadata(&project).await.unwrap();
    assert!(
        got.byo_bucket.is_none(),
        "byo_bucket should be None after clearing"
    );
}

#[tokio::test]
async fn cross_project_isolation_in_memory() {
    let cat = InMemoryCatalog::new();
    let a = ProjectId::new();
    let b = ProjectId::new();
    let meta_a = TenantMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            bucket: "bucket-a".into(),
            region: "us-east-1".into(),
            access_key_id: "key-a".into(),
            secret_access_key_enc: vec![0xa],
            force_path_style: false,
        }),
    };
    cat.set_tenant_metadata(&a, meta_a.clone()).await.unwrap();

    let got_a = cat.get_tenant_metadata(&a).await.unwrap();
    let got_b = cat.get_tenant_metadata(&b).await.unwrap();
    assert_eq!(got_a, meta_a, "project A should see its own metadata");
    assert!(
        got_b.byo_bucket.is_none(),
        "project B should see default metadata"
    );
}

#[tokio::test]
async fn drop_namespace_clears_tenant_metadata() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let meta = TenantMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key_enc: vec![],
            force_path_style: false,
        }),
    };
    cat.create_namespace(&project).await.unwrap();
    cat.set_tenant_metadata(&project, meta).await.unwrap();
    cat.drop_namespace(&project).await.unwrap();
    let got = cat.get_tenant_metadata(&project).await.unwrap();
    assert!(
        got.byo_bucket.is_none(),
        "tenant metadata should be cleared after drop_namespace"
    );
}
