//! Phase 6.X.A — partition-lease registry tests (ADR 0023).
//!
//! Covers acquire / renew / steal-on-expiry / release / owner_of round-trips
//! against the in-memory backend, plus the same invariants against Postgres
//! when a local instance is reachable (skipped otherwise, like the rest of the
//! catalog suite).

use std::time::Duration;

use basin_catalog::{InMemoryCatalog, LeaseRegistry, PostgresCatalog};
use basin_common::ProjectId;
use ulid::Ulid;

const PART: &str = "_default";

// ---- In-memory ------------------------------------------------------------

#[tokio::test]
async fn acquire_renew_release_round_trip_in_memory() {
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();

    // First grant: epoch 1.
    let lease = cat
        .acquire(&p, PART, "replica-a", Duration::from_secs(15))
        .await
        .unwrap()
        .expect("first grant succeeds");
    assert_eq!(lease.epoch, 1);
    assert_eq!(lease.holder, "replica-a");

    // owner_of reflects the live lease.
    let owner = cat.owner_of(&p, PART).await.unwrap();
    assert_eq!(owner, Some(("replica-a".to_string(), 1)));

    // Renew at the held epoch succeeds and does NOT bump the epoch.
    assert!(cat
        .renew(&p, PART, "replica-a", 1, Duration::from_secs(15))
        .await
        .unwrap());
    assert_eq!(cat.owner_of(&p, PART).await.unwrap().unwrap().1, 1);

    // Renew at a wrong epoch fails.
    assert!(!cat
        .renew(&p, PART, "replica-a", 99, Duration::from_secs(15))
        .await
        .unwrap());

    // Release drops the lease.
    assert!(cat.release(&p, PART, "replica-a").await.unwrap());
    assert_eq!(cat.owner_of(&p, PART).await.unwrap(), None);
}

#[tokio::test]
async fn two_replicas_contend_in_memory() {
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();

    // Replica A wins the first grant.
    let a = cat
        .acquire(&p, PART, "replica-a", Duration::from_secs(15))
        .await
        .unwrap();
    assert!(a.is_some());

    // Replica B loses while A's lease is live.
    let b = cat
        .acquire(&p, PART, "replica-b", Duration::from_secs(15))
        .await
        .unwrap();
    assert!(b.is_none(), "second replica must lose while lease is live");

    // owner is still A.
    assert_eq!(
        cat.owner_of(&p, PART).await.unwrap(),
        Some(("replica-a".to_string(), 1))
    );
}

#[tokio::test]
async fn steal_on_expiry_in_memory() {
    let cat = InMemoryCatalog::new();
    let p = ProjectId::new();

    // A grabs a near-zero TTL lease, then it expires.
    let a = cat
        .acquire(&p, PART, "replica-a", Duration::from_millis(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.epoch, 1);
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Expired: no live owner.
    assert_eq!(cat.owner_of(&p, PART).await.unwrap(), None);

    // B steals it; epoch increments monotonically.
    let b = cat
        .acquire(&p, PART, "replica-b", Duration::from_secs(15))
        .await
        .unwrap()
        .expect("steal after expiry succeeds");
    assert_eq!(b.epoch, 2, "stolen lease must bump the epoch");
    assert_eq!(
        cat.owner_of(&p, PART).await.unwrap(),
        Some(("replica-b".to_string(), 2))
    );

    // A can no longer renew at its stale epoch.
    assert!(!cat
        .renew(&p, PART, "replica-a", 1, Duration::from_secs(15))
        .await
        .unwrap());
}

// ---- Postgres -------------------------------------------------------------

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn unique_schema() -> String {
    format!(
        "basin_catalog_test_{}",
        Ulid::new().to_string().to_lowercase()
    )
}

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
async fn acquire_renew_steal_release_round_trip_postgres() {
    let Some((cat, _guard)) = try_connect().await else {
        return;
    };
    let p = ProjectId::new();

    // First grant.
    let lease = cat
        .acquire(&p, PART, "replica-a", Duration::from_secs(15))
        .await
        .unwrap()
        .expect("first grant");
    assert_eq!(lease.epoch, 1);
    assert_eq!(
        cat.owner_of(&p, PART).await.unwrap(),
        Some(("replica-a".to_string(), 1))
    );

    // Renew at the held epoch.
    assert!(cat
        .renew(&p, PART, "replica-a", 1, Duration::from_secs(15))
        .await
        .unwrap());
    // Renew at a stale epoch fails.
    assert!(!cat
        .renew(&p, PART, "replica-a", 99, Duration::from_secs(15))
        .await
        .unwrap());

    // A second replica loses while the lease is live.
    assert!(cat
        .acquire(&p, PART, "replica-b", Duration::from_secs(15))
        .await
        .unwrap()
        .is_none());

    // Release, then a fresh grant starts a new monotonic epoch (2).
    assert!(cat.release(&p, PART, "replica-a").await.unwrap());
    assert_eq!(cat.owner_of(&p, PART).await.unwrap(), None);
    // After release the row is gone, so the next acquire is a first-grant (1).
    let regrant = cat
        .acquire(&p, PART, "replica-b", Duration::from_secs(15))
        .await
        .unwrap()
        .expect("regrant after release");
    assert_eq!(
        regrant.epoch, 1,
        "post-release grant is a fresh first-grant"
    );
}

#[tokio::test]
async fn steal_on_expiry_postgres() {
    let Some((cat, _guard)) = try_connect().await else {
        return;
    };
    let p = ProjectId::new();

    // Near-instant TTL, let it expire.
    let a = cat
        .acquire(&p, PART, "replica-a", Duration::from_secs(0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.epoch, 1);
    // TTL of 0s means expires_at == granted_at; it's already expired.
    assert_eq!(
        cat.owner_of(&p, PART).await.unwrap(),
        None,
        "0s lease is immediately expired"
    );

    // B steals; epoch bumps to 2.
    let b = cat
        .acquire(&p, PART, "replica-b", Duration::from_secs(15))
        .await
        .unwrap()
        .expect("steal after expiry");
    assert_eq!(b.epoch, 2);
    assert_eq!(
        cat.owner_of(&p, PART).await.unwrap(),
        Some(("replica-b".to_string(), 2))
    );

    // A's stale renew fails.
    assert!(!cat
        .renew(&p, PART, "replica-a", 1, Duration::from_secs(15))
        .await
        .unwrap());
}
