//! Integration tests for the per-tenant storage config catalog API.
//!
//! In-memory cases run unconditionally. The Postgres durability case
//! skips silently if Postgres is not reachable on the local PoC URL,
//! mirroring the harness shape used by the rest of the basin-catalog
//! suite.

use std::collections::BTreeMap;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog, PostgresCatalog, TenantStorageConfig};
use basin_common::TenantId;
use ulid::Ulid;

fn sample_config(key: &str) -> TenantStorageConfig {
    let mut extras = BTreeMap::new();
    extras.insert("assume_role_arn".into(), "arn:aws:iam::123:role/r".into());
    extras.insert("region".into(), "us-east-1".into());
    TenantStorageConfig {
        kms_key_ref: Some(key.into()),
        provider_extras: extras,
    }
}

#[tokio::test]
async fn set_then_get_round_trips_in_memory() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    let cfg = sample_config("arn:aws:kms:us-east-1:123:key/abc");
    cat.set_tenant_storage_config(&t, cfg.clone())
        .await
        .unwrap();
    let got = cat.get_tenant_storage_config(&t).await.unwrap();
    assert_eq!(got, Some(cfg));
}

#[tokio::test]
async fn set_overwrites_existing_in_memory() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.set_tenant_storage_config(&t, sample_config("first"))
        .await
        .unwrap();
    cat.set_tenant_storage_config(&t, sample_config("second"))
        .await
        .unwrap();
    let got = cat.get_tenant_storage_config(&t).await.unwrap().unwrap();
    assert_eq!(got.kms_key_ref.as_deref(), Some("second"));
}

#[tokio::test]
async fn get_returns_none_when_unset_in_memory() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    let got = cat.get_tenant_storage_config(&t).await.unwrap();
    assert!(got.is_none());
}

#[tokio::test]
async fn cross_tenant_isolation_in_memory() {
    let cat = InMemoryCatalog::new();
    let a = TenantId::new();
    let b = TenantId::new();
    cat.set_tenant_storage_config(&a, sample_config("for-a"))
        .await
        .unwrap();
    let got_a = cat.get_tenant_storage_config(&a).await.unwrap().unwrap();
    let got_b = cat.get_tenant_storage_config(&b).await.unwrap();
    assert_eq!(got_a.kms_key_ref.as_deref(), Some("for-a"));
    assert!(got_b.is_none());
}

#[tokio::test]
async fn drop_namespace_clears_config_in_memory() {
    let cat = InMemoryCatalog::new();
    let t = TenantId::new();
    cat.set_tenant_storage_config(&t, sample_config("k"))
        .await
        .unwrap();
    cat.create_namespace(&t).await.unwrap();
    cat.drop_namespace(&t).await.unwrap();
    let got = cat.get_tenant_storage_config(&t).await.unwrap();
    assert!(got.is_none());
}

// ---- Postgres durability harness ------------------------------------

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn unique_schema() -> String {
    format!(
        "basin_catalog_test_{}",
        Ulid::new().to_string().to_lowercase()
    )
}

/// Drops the schema (and every table inside) on Drop. Spawns a fresh
/// runtime on a separate thread so cleanup completes even if the test's
/// runtime is shutting down.
struct SchemaGuard {
    schema: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async {
                let connect = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(PG_URL, tokio_postgres::NoTls),
                )
                .await;
                let (client, conn) = match connect {
                    Ok(Ok(p)) => p,
                    _ => return,
                };
                let driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                let _ = client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await;
                drop(client);
                let _ = tokio::time::timeout(Duration::from_millis(200), driver).await;
            });
        })
        .join();
    }
}

async fn try_connect() -> Option<(PostgresCatalog, SchemaGuard)> {
    let schema = unique_schema();
    match tokio::time::timeout(
        Duration::from_secs(2),
        PostgresCatalog::connect_with_schema(PG_URL, &schema),
    )
    .await
    {
        Ok(Ok(cat)) => Some((cat, SchemaGuard { schema })),
        _ => {
            eprintln!("postgres unreachable, skipping test");
            None
        }
    }
}

#[tokio::test]
async fn set_then_get_round_trips_postgres() {
    let Some((cat, _guard)) = try_connect().await else {
        return;
    };
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let cfg = sample_config("arn:aws:kms:eu-west-1:99:key/xyz");
    cat.set_tenant_storage_config(&t, cfg.clone())
        .await
        .unwrap();
    let got = cat.get_tenant_storage_config(&t).await.unwrap();
    assert_eq!(got, Some(cfg));
}

#[tokio::test]
async fn set_overwrites_existing_postgres() {
    let Some((cat, _guard)) = try_connect().await else {
        return;
    };
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.set_tenant_storage_config(&t, sample_config("first"))
        .await
        .unwrap();
    cat.set_tenant_storage_config(&t, sample_config("second"))
        .await
        .unwrap();
    let got = cat.get_tenant_storage_config(&t).await.unwrap().unwrap();
    assert_eq!(got.kms_key_ref.as_deref(), Some("second"));
}

#[tokio::test]
async fn cross_tenant_isolation_postgres() {
    let Some((cat, _guard)) = try_connect().await else {
        return;
    };
    let a = TenantId::new();
    let b = TenantId::new();
    cat.create_namespace(&a).await.unwrap();
    cat.create_namespace(&b).await.unwrap();
    cat.set_tenant_storage_config(&a, sample_config("for-a"))
        .await
        .unwrap();
    let got_a = cat.get_tenant_storage_config(&a).await.unwrap().unwrap();
    let got_b = cat.get_tenant_storage_config(&b).await.unwrap();
    assert_eq!(got_a.kms_key_ref.as_deref(), Some("for-a"));
    assert!(got_b.is_none());
}

#[tokio::test]
async fn survives_simulated_restart_postgres() {
    // Build a fresh schema, write a config, drop the catalog handle,
    // reconnect into the *same* schema, and assert the config came back.
    let schema = unique_schema();
    let _guard = SchemaGuard {
        schema: schema.clone(),
    };
    let cat = match tokio::time::timeout(
        Duration::from_secs(2),
        PostgresCatalog::connect_with_schema(PG_URL, &schema),
    )
    .await
    {
        Ok(Ok(c)) => c,
        _ => {
            eprintln!("postgres unreachable, skipping test");
            return;
        }
    };
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    let cfg = sample_config("arn:aws:kms:us-east-2:1:key/persist");
    cat.set_tenant_storage_config(&t, cfg.clone())
        .await
        .unwrap();
    drop(cat);

    let cat2 = PostgresCatalog::connect_with_schema(PG_URL, &schema)
        .await
        .expect("reconnect");
    let got = cat2.get_tenant_storage_config(&t).await.unwrap();
    assert_eq!(got, Some(cfg));
}

#[tokio::test]
async fn drop_namespace_clears_config_postgres() {
    let Some((cat, _guard)) = try_connect().await else {
        return;
    };
    let t = TenantId::new();
    cat.create_namespace(&t).await.unwrap();
    cat.set_tenant_storage_config(&t, sample_config("k"))
        .await
        .unwrap();
    cat.drop_namespace(&t).await.unwrap();
    let got = cat.get_tenant_storage_config(&t).await.unwrap();
    assert!(got.is_none());
}
