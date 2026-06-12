//! Integration tests for ProjectMetadata catalog API and S3Config serde
//! round-trips (T-048).
//!
//! In-memory cases run unconditionally. The Postgres durability case skips
//! silently if Postgres is not reachable, mirroring the pattern used by the
//! rest of the basin-catalog suite.

use basin_catalog::{Catalog, InMemoryCatalog, S3Config, ProjectMetadata};
use basin_catalog::usage::{spawn_usage_snapshot_task, UsageSnapshotConfig};
use basin_common::ProjectCounterRegistry;
use basin_common::ProjectId;
use std::sync::Arc;

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
// ProjectMetadata serde round-trips
// -------------------------------------------------------------------------

#[test]
fn project_metadata_default_is_none() {
    let meta = ProjectMetadata::default();
    assert!(meta.byo_bucket.is_none());
}

#[test]
fn project_metadata_default_serialises_without_byo_bucket_key() {
    let meta = ProjectMetadata::default();
    let json = serde_json::to_string(&meta).unwrap();
    // skip_serializing_if = "Option::is_none" should suppress the key entirely.
    assert!(
        !json.contains("byo_bucket"),
        "expected no byo_bucket key in JSON, got: {json}"
    );
}

#[test]
fn project_metadata_with_byo_bucket_round_trips() {
    let meta = ProjectMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://fly.storage.tigris.dev".into(),
            bucket: "customer-bucket".into(),
            region: "auto".into(),
            access_key_id: "tid_test_key".into(),
            secret_access_key_enc: b"encrypted_blob".to_vec(),
            force_path_style: false,
        }),
        home_region: None,
    };
    let json = serde_json::to_string(&meta).unwrap();
    let back: ProjectMetadata = serde_json::from_str(&json).unwrap();
    assert_eq!(meta, back);
}

#[test]
fn project_metadata_deserialises_from_empty_object() {
    // Back-compat: existing catalog rows without byo_bucket should
    // deserialise cleanly to the default (None).
    let meta: ProjectMetadata = serde_json::from_str("{}").unwrap();
    assert!(meta.byo_bucket.is_none());
}

// -------------------------------------------------------------------------
// Catalog API — in-memory
// -------------------------------------------------------------------------

#[tokio::test]
async fn get_returns_default_when_unset() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let got = cat.get_project_metadata(&project).await.unwrap();
    assert!(
        got.byo_bucket.is_none(),
        "unset project should return default ProjectMetadata"
    );
}

#[tokio::test]
async fn set_then_get_round_trips_in_memory() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let meta = ProjectMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://fly.storage.tigris.dev".into(),
            bucket: "acme-bucket".into(),
            region: "auto".into(),
            access_key_id: "tid_key".into(),
            secret_access_key_enc: vec![0x42, 0x43],
            force_path_style: false,
        }),
        home_region: None,
    };
    cat.set_project_metadata(&project, meta.clone())
        .await
        .unwrap();
    let got = cat.get_project_metadata(&project).await.unwrap();
    assert_eq!(got, meta);
}

#[tokio::test]
async fn set_overwrites_existing_in_memory() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let first = ProjectMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://first.example.com".into(),
            bucket: "b1".into(),
            region: "us-east-1".into(),
            access_key_id: "k1".into(),
            secret_access_key_enc: vec![1],
            force_path_style: false,
        }),
        home_region: None,
    };
    let second = ProjectMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://second.example.com".into(),
            bucket: "b2".into(),
            region: "eu-west-1".into(),
            access_key_id: "k2".into(),
            secret_access_key_enc: vec![2],
            force_path_style: true,
        }),
        home_region: None,
    };
    cat.set_project_metadata(&project, first).await.unwrap();
    cat.set_project_metadata(&project, second.clone())
        .await
        .unwrap();
    let got = cat.get_project_metadata(&project).await.unwrap();
    assert_eq!(got, second);
}

#[tokio::test]
async fn set_none_byo_bucket_clears_it() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let with_byo = ProjectMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key_enc: vec![],
            force_path_style: false,
        }),
        home_region: None,
    };
    cat.set_project_metadata(&project, with_byo).await.unwrap();

    // Overwrite with default (None) to clear the BYO bucket.
    cat.set_project_metadata(&project, ProjectMetadata::default())
        .await
        .unwrap();
    let got = cat.get_project_metadata(&project).await.unwrap();
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
    let meta_a = ProjectMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            bucket: "bucket-a".into(),
            region: "us-east-1".into(),
            access_key_id: "key-a".into(),
            secret_access_key_enc: vec![0xa],
            force_path_style: false,
        }),
        home_region: None,
    };
    cat.set_project_metadata(&a, meta_a.clone()).await.unwrap();

    let got_a = cat.get_project_metadata(&a).await.unwrap();
    let got_b = cat.get_project_metadata(&b).await.unwrap();
    assert_eq!(got_a, meta_a, "project A should see its own metadata");
    assert!(
        got_b.byo_bucket.is_none(),
        "project B should see default metadata"
    );
}

#[tokio::test]
async fn drop_namespace_clears_project_metadata() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();
    let meta = ProjectMetadata {
        byo_bucket: Some(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: "k".into(),
            secret_access_key_enc: vec![],
            force_path_style: false,
        }),
        home_region: None,
    };
    cat.create_namespace(&project).await.unwrap();
    cat.set_project_metadata(&project, meta).await.unwrap();
    cat.drop_namespace(&project).await.unwrap();
    let got = cat.get_project_metadata(&project).await.unwrap();
    assert!(
        got.byo_bucket.is_none(),
        "project metadata should be cleared after drop_namespace"
    );
}

// -------------------------------------------------------------------------
// T-053: metering HTTP POST body shape + URL verification
// -------------------------------------------------------------------------

/// Verifies that `spawn_usage_snapshot_task` POSTs a JSON array of
/// [`ProjectCounters`] to `{BASIN_METERING_URL}/internal/v1/metering/counters`
/// when the URL is configured.  Uses `httpmock` as a local HTTP mock server so
/// no real network traffic leaves the test process.
///
/// Body shape is verified by matching expected field values as JSON substrings
/// (e.g. `"storage_bytes":4096`).
#[tokio::test]
async fn metering_post_sends_correct_body_to_metering_endpoint() {
    use httpmock::prelude::*;

    let registry = Arc::new(ProjectCounterRegistry::new());
    let project = ProjectId::new();
    let counter = registry.for_project(&project);
    counter.record_op();
    counter.record_bytes_written(4096);
    counter.record_cpu_micros(1_000_000); // 1000 ms → cpu_ms = 1000

    // Stand up a mock server on a random local port.
    let server = MockServer::start_async().await;

    // Expect POST to the metering path; verify Content-Type and key body
    // fields are present.  httpmock 0.7 does not expose captured request bodies
    // after the fact, so we assert field presence in the `when` matcher.
    let mock = server
        .mock_async(|when, then| {
            when.method(POST)
                .path("/internal/v1/metering/counters")
                .header("content-type", "application/json")
                // Verify the body contains expected field values.
                .body_contains("\"storage_bytes\":4096")
                .body_contains("\"ops_count\":1")
                .body_contains("\"cpu_ms\":1000")
                .body_contains("\"active_hours\":1.0");
            then.status(200).body("ok");
        })
        .await;

    let config = UsageSnapshotConfig {
        // Very short interval so the first non-startup tick fires quickly.
        interval: std::time::Duration::from_millis(20),
        otlp_endpoint: None,
        metering_url: Some(server.base_url()),
    };

    let handle = spawn_usage_snapshot_task(
        Arc::clone(&registry),
        config,
        move || vec![project],
        |_rows| async {},
    );

    // Allow at least one tick to fire and the POST to complete.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    handle.abort();

    // The mock must have been hit at least once with the correct body.
    mock.assert_hits_async(1).await;
}
