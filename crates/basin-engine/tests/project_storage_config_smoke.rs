//! Engine passthrough smoke test: `Engine::set_project_storage_config` /
//! `Engine::get_project_storage_config` round-trip through the attached
//! Storage + InMemoryCatalog without exercising any KMS provider. The
//! deeper routing semantics live in `basin-storage`'s
//! `project_storage_config_routing` integration test; this one just
//! confirms the engine surface is wired.

use std::collections::BTreeMap;
use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_storage::{ProjectStorageConfig, Storage, StorageConfig};
use object_store::memory::InMemory;

#[tokio::test]
async fn engine_passthrough_set_get() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(InMemory::new()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut extras = BTreeMap::new();
    extras.insert("region".into(), "eu-west-1".into());
    let cfg = ProjectStorageConfig {
        kms_key_ref: Some("arn:aws:kms:eu-west-1:1:key/eng-passthrough".into()),
        provider_extras: extras,
    };

    engine
        .set_project_storage_config(&project, cfg.clone())
        .await
        .unwrap();
    let got = engine.get_project_storage_config(&project).await.unwrap();
    assert_eq!(got, Some(cfg));

    // Unknown project returns None.
    let other = ProjectId::new();
    let none = engine.get_project_storage_config(&other).await.unwrap();
    assert!(none.is_none());
}
