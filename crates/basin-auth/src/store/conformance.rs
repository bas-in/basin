//! `AuthStore` conformance test suite.
//!
//! `test_auth_store_conformance(store)` is an async function that exercises
//! the full `AuthStore` contract against any implementation. Call it from a
//! `#[tokio::test]` block and supply an already-migrated store.
//!
//! The suite is intentionally self-contained: it generates its own tenant
//! ULIDs and UUIDs so concurrent test runs don't collide. Every sub-test is
//! documented with the invariant it verifies.
//!
//! ## Running against the Postgres implementation
//!
//! The tests marked `#[ignore]` require a live Postgres connection. Set
//! `BASIN_TEST_PG_DSN` to a `host=… user=… dbname=…` libpq connection string
//! before running:
//!
//! ```bash
//! BASIN_TEST_PG_DSN="host=127.0.0.1 port=5432 user=pc dbname=postgres" \
//!   cargo test -p basin-auth -- --include-ignored conformance
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use basin_common::{BasinError, TenantId};
use chrono::Utc;
use uuid::Uuid;

use super::{AuthStore, EmailTokenRow};
use crate::tokens;

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

/// Salt used by all credential inserts in this suite. Cost 4 is the minimum
/// the bcrypt crate accepts; it keeps the tests fast.
const TEST_BCRYPT_COST: u32 = 4;

fn bcrypt_hash(plain: &str) -> String {
    bcrypt::hash(plain, TEST_BCRYPT_COST).expect("bcrypt hash")
}

// ---------------------------------------------------------------------------
// Public conformance runner
// ---------------------------------------------------------------------------

/// Run the full conformance suite against `store`. The store must already have
/// had `migrate(schema)` called (schema migrations are out-of-scope for this
/// runner — they are tested by the `AuthService::connect_with_mailer` path in
/// `lib.rs`).
pub async fn test_auth_store_conformance(store: Arc<dyn AuthStore>) {
    user_uniqueness_per_tenant(&store).await;
    find_user_by_email_tests(&store).await;
    email_token_single_use(&store).await;
    email_token_expiry(&store).await;
    api_key_lifecycle(&store).await;
    api_key_tenant_scoping(&store).await;
    tenant_credential_uniqueness(&store).await;
    tenant_credential_self_routing(&store).await;
    session_settings_upsert(&store).await;
}

// ---------------------------------------------------------------------------
// Individual invariants
// ---------------------------------------------------------------------------

/// `(tenant, email)` pairs are unique; the same email in two different tenants
/// must succeed.
async fn user_uniqueness_per_tenant(store: &Arc<dyn AuthStore>) {
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let email = "shared@conformance.test";
    let hash = bcrypt_hash("testpassword");

    // First insert into tenant_a → Ok.
    let uid_a = Uuid::new_v4();
    store
        .create_user(&tenant_a, email, &hash, uid_a)
        .await
        .expect("first create_user must succeed");

    // Duplicate (tenant_a, email) → Err / conflict.
    let uid_dup = Uuid::new_v4();
    let dup_result = store.create_user(&tenant_a, email, &hash, uid_dup).await;
    assert!(
        dup_result.is_err(),
        "duplicate (tenant, email) must fail, got Ok"
    );

    // Same email in tenant_b → Ok (different tenant).
    let uid_b = Uuid::new_v4();
    store
        .create_user(&tenant_b, email, &hash, uid_b)
        .await
        .expect("same email in different tenant must succeed");
}

/// `find_user_by_email` returns `None` for unknown users, `Some` for known
/// ones, and `None` when queried in the wrong tenant.
async fn find_user_by_email_tests(store: &Arc<dyn AuthStore>) {
    let tenant = TenantId::new();
    let other_tenant = TenantId::new();
    let email = format!("find-{}@conformance.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");

    // Non-existent → None.
    let not_found = store
        .find_user_by_email(&tenant, &email)
        .await
        .expect("find_user_by_email must not error on miss");
    assert!(not_found.is_none(), "expected None for non-existent user");

    // Create then find → Some.
    let uid = Uuid::new_v4();
    store
        .create_user(&tenant, &email, &hash, uid)
        .await
        .expect("create_user");
    let found = store
        .find_user_by_email(&tenant, &email)
        .await
        .expect("find_user_by_email must not error")
        .expect("user must exist after create");
    assert_eq!(found.email, email);
    assert_eq!(found.user_id, uid);
    // email_verified_at starts as NULL (not set on create alone).
    assert!(
        found.email_verified_at.is_none(),
        "email_verified_at must be NULL immediately after create"
    );

    // Find in wrong tenant → None.
    let wrong = store
        .find_user_by_email(&other_tenant, &email)
        .await
        .expect("find_user_by_email in wrong tenant must not error");
    assert!(
        wrong.is_none(),
        "user must not be visible in a different tenant"
    );
}

/// An email token can be consumed exactly once; a second attempt returns 0
/// updated rows (the flow layer maps this to an error — we test the count).
async fn email_token_single_use(store: &Arc<dyn AuthStore>) {
    let tenant = TenantId::new();
    let user_id = Uuid::new_v4();
    let email = format!("token-{}@conformance.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");
    store
        .create_user(&tenant, &email, &hash, user_id)
        .await
        .expect("create_user for token test");

    let (raw, token_hash) = tokens::generate();
    let expires_at = Utc::now() + chrono::Duration::minutes(30);

    store
        .insert_email_token(&tenant, user_id, &token_hash, "verify", expires_at)
        .await
        .expect("insert_email_token");

    // First consume → 1 row updated.
    let n = store
        .consume_email_token(&tenant, &token_hash)
        .await
        .expect("consume_email_token first call");
    assert_eq!(n, 1, "first consume must update 1 row");

    // Second consume → 0 rows updated (already consumed).
    let n2 = store
        .consume_email_token(&tenant, &token_hash)
        .await
        .expect("consume_email_token second call must not error");
    assert_eq!(
        n2, 0,
        "second consume must update 0 rows (already consumed)"
    );

    // Raw token sanity: the hash produced externally must survive the round
    // trip through the store.
    let _ = raw; // suppress unused-variable lint
}

/// An email token whose `expires_at` is in the past must not be consumable.
/// We verify this by inserting with `expires_at = now() - 1s` and checking
/// the row via `find_email_token`: if the store exposes the expiry it must
/// show the past timestamp, and the flow layer's `now() > expires_at` check
/// would reject it. The conformance test verifies the *row shape* here; full
/// rejection semantics are tested in `lib.rs::expired_token_rejected`.
async fn email_token_expiry(store: &Arc<dyn AuthStore>) {
    let tenant = TenantId::new();
    let user_id = Uuid::new_v4();
    let email = format!("expiry-{}@conformance.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");
    store
        .create_user(&tenant, &email, &hash, user_id)
        .await
        .expect("create_user");

    let (_, token_hash) = tokens::generate();
    // Insert with expiry 1 second in the past.
    let past = Utc::now() - chrono::Duration::seconds(1);
    store
        .insert_email_token(&tenant, user_id, &token_hash, "reset", past)
        .await
        .expect("insert expired token");

    // The row must be findable — the store doesn't auto-delete expired rows.
    let row: Option<EmailTokenRow> = store
        .find_email_token(&tenant, &token_hash)
        .await
        .expect("find_email_token must not error");
    let row = row.expect("expired token row must exist in store");
    assert!(
        row.expires_at < Utc::now(),
        "expires_at must be in the past; got {:?}",
        row.expires_at
    );
    assert!(
        row.consumed_at.is_none(),
        "row must not be pre-consumed just because it's expired"
    );
    assert_eq!(row.user_id, user_id);
    assert_eq!(row.purpose, "reset");
}

/// API key lifecycle: insert → find-by-hash (not revoked) → revoke →
/// find-by-hash (revoked_at is set).
async fn api_key_lifecycle(store: &Arc<dyn AuthStore>) {
    let tenant = TenantId::new();
    let user_id = Uuid::new_v4();
    let email = format!("apikey-{}@conformance.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");
    store
        .create_user(&tenant, &user_id.to_string(), &hash, user_id)
        .await
        .ok(); // user existence is needed only for FK in Postgres; memory store may not care

    // Use a stable fake key so the sha256 hash is predictable.
    let raw_key = format!("basin_{tenant}_testkey{}", Uuid::new_v4());
    let key_sha = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(raw_key.as_bytes()))
    };
    let key_bcrypt = bcrypt_hash(&raw_key);

    let (id, created_at) = store
        .insert_api_key(&tenant, user_id, "conformance-key", &key_sha, &key_bcrypt)
        .await
        .expect("insert_api_key");
    assert!(id > 0, "api key id must be positive");
    assert!(
        (Utc::now() - created_at).num_seconds() < 5,
        "created_at must be recent"
    );

    // find_api_keys_by_hash → non-empty, not revoked.
    let rows = store
        .find_api_keys_by_hash(&key_sha)
        .await
        .expect("find_api_keys_by_hash");
    assert!(
        !rows.is_empty(),
        "find_api_keys_by_hash must return the inserted row"
    );
    let row = rows.iter().find(|r| r.id == id).expect("row by id");
    assert!(
        row.revoked_at.is_none(),
        "freshly inserted key must not be revoked"
    );
    assert_eq!(row.user_id, user_id);

    // Revoke.
    store
        .revoke_api_key(&tenant, id)
        .await
        .expect("revoke_api_key");

    // find again → revoked_at is set.
    let rows_after = store
        .find_api_keys_by_hash(&key_sha)
        .await
        .expect("find_api_keys_by_hash after revoke");
    let row_after = rows_after.iter().find(|r| r.id == id).expect("row by id");
    assert!(
        row_after.revoked_at.is_some(),
        "revoked_at must be set after revoke_api_key"
    );

    // Revoke in a different tenant → NotFound.
    let other_tenant = TenantId::new();
    let err = store
        .revoke_api_key(&other_tenant, id)
        .await
        .expect_err("revoking a key belonging to another tenant must fail");
    assert!(
        matches!(err, BasinError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
}

/// `list_api_keys(tenant_a, user)` must not return keys belonging to
/// `tenant_b`.
async fn api_key_tenant_scoping(store: &Arc<dyn AuthStore>) {
    let tenant_a = TenantId::new();
    let tenant_b = TenantId::new();
    let user = Uuid::new_v4();

    let insert = |tenant: &TenantId, name: &str| {
        let raw_key = format!("basin_{tenant}_{name}");
        let key_sha = {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(raw_key.as_bytes()))
        };
        let key_bcrypt = bcrypt_hash(&raw_key);
        (key_sha, key_bcrypt)
    };

    let (sha_a, bcrypt_a) = insert(&tenant_a, "key-a");
    let (sha_b, bcrypt_b) = insert(&tenant_b, "key-b");
    store
        .insert_api_key(&tenant_a, user, "key-a", &sha_a, &bcrypt_a)
        .await
        .expect("insert key-a");
    store
        .insert_api_key(&tenant_b, user, "key-b", &sha_b, &bcrypt_b)
        .await
        .expect("insert key-b");

    let list_a = store
        .list_api_keys(&tenant_a, user)
        .await
        .expect("list_api_keys tenant_a");
    assert_eq!(list_a.len(), 1, "tenant_a must have exactly 1 key");
    assert_eq!(list_a[0].name, "key-a");

    let list_b = store
        .list_api_keys(&tenant_b, user)
        .await
        .expect("list_api_keys tenant_b");
    assert_eq!(list_b.len(), 1, "tenant_b must have exactly 1 key");
    assert_eq!(list_b[0].name, "key-b");
}

/// Inserting the same `pgwire_user` twice must return `false` the second time
/// (the row already exists).
async fn tenant_credential_uniqueness(store: &Arc<dyn AuthStore>) {
    let tenant = TenantId::new();
    let pgwire_user = format!("{}_conformance", tenant.to_string());
    let hash = bcrypt_hash("somepgpassword");

    let first = store
        .insert_tenant_credential(&tenant, &pgwire_user, &hash, "basin")
        .await
        .expect("first insert_tenant_credential");
    assert!(first, "first insert must return true (inserted)");

    let second = store
        .insert_tenant_credential(&tenant, &pgwire_user, &hash, "basin")
        .await
        .expect("second insert_tenant_credential must not error");
    assert!(
        !second,
        "second insert with same pgwire_user must return false (conflict)"
    );
}

/// A pgwire_user generated by the production format carries the tenant_id,
/// and `parse_tenant_from_pgwire_user` must recover it.
async fn tenant_credential_self_routing(store: &Arc<dyn AuthStore>) {
    use crate::tenant_credentials::parse_tenant_from_pgwire_user;

    let tenant = TenantId::new();
    // Generate a user in the production format: {26-char-ulid}_{8-hex}.
    let pgwire_user = format!("{}_{:08x}", tenant, 0xdeadbeef_u32);
    let hash = bcrypt_hash("routingpwd");

    store
        .insert_tenant_credential(&tenant, &pgwire_user, &hash, "basin")
        .await
        .expect("insert for self-routing test");

    let parsed = parse_tenant_from_pgwire_user(&pgwire_user)
        .expect("generated pgwire_user must parse tenant from username");
    assert_eq!(
        parsed,
        tenant.to_string().as_str(),
        "self-routing: parsed tenant must match original; got {parsed:?}"
    );
}

/// Upserting the same key twice must not create duplicate rows — only the
/// value is updated. The session-settings map must have exactly one entry.
async fn session_settings_upsert(store: &Arc<dyn AuthStore>) {
    let tenant = TenantId::new();
    let user = Uuid::new_v4();

    // Initial upsert.
    store
        .upsert_session_setting(&tenant, user, "timezone", "UTC")
        .await
        .expect("first upsert");

    let m1: HashMap<String, String> = store
        .list_session_settings(&tenant, user)
        .await
        .expect("list after first upsert");
    assert_eq!(
        m1.get("timezone").map(|s| s.as_str()),
        Some("UTC"),
        "timezone must be UTC after first upsert"
    );
    assert_eq!(m1.len(), 1, "must have exactly one entry");

    // Update the same key.
    store
        .upsert_session_setting(&tenant, user, "timezone", "America/New_York")
        .await
        .expect("second upsert");

    let m2: HashMap<String, String> = store
        .list_session_settings(&tenant, user)
        .await
        .expect("list after second upsert");
    assert_eq!(
        m2.get("timezone").map(|s| s.as_str()),
        Some("America/New_York"),
        "timezone must be updated to America/New_York"
    );
    assert_eq!(
        m2.len(),
        1,
        "upsert must not duplicate — still exactly one entry"
    );

    // Add a second key to prove it's additive, not a full replace.
    store
        .upsert_session_setting(&tenant, user, "language", "en")
        .await
        .expect("upsert language");
    let m3: HashMap<String, String> = store
        .list_session_settings(&tenant, user)
        .await
        .expect("list after third upsert");
    assert_eq!(m3.len(), 2, "two distinct keys must produce two entries");
    assert_eq!(
        m3.get("timezone").map(|s| s.as_str()),
        Some("America/New_York")
    );
    assert_eq!(m3.get("language").map(|s| s.as_str()), Some("en"));
}

// ---------------------------------------------------------------------------
// PostgresAuthStore conformance (requires live Postgres)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod pg_conformance_tests {
    //! Tests that run the full conformance suite against `PostgresAuthStore`.
    //!
    //! These are `#[ignore]` by default because they require a live Postgres
    //! instance. Set `BASIN_TEST_PG_DSN` to opt in:
    //!
    //! ```bash
    //! BASIN_TEST_PG_DSN="host=127.0.0.1 port=5432 user=pc dbname=postgres" \
    //!   cargo test -p basin-auth -- --include-ignored pg_conformance
    //! ```

    use std::sync::Arc;
    use std::time::Duration;

    use basin_common::TenantId;
    use tokio_postgres::NoTls;
    use ulid::Ulid;

    use crate::store::postgres::PostgresAuthStore;
    use crate::store::AuthStore;

    fn test_dsn() -> Option<String> {
        std::env::var("BASIN_TEST_PG_DSN").ok()
    }

    fn unique_schema() -> String {
        format!("basin_conform_{}", Ulid::new().to_string().to_lowercase())
    }

    struct SchemaGuard {
        schema: String,
        dsn: String,
    }

    impl Drop for SchemaGuard {
        fn drop(&mut self) {
            let schema = self.schema.clone();
            let dsn = self.dsn.clone();
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
                        tokio_postgres::connect(&dsn, NoTls),
                    )
                    .await;
                    let (client, conn) = match connect {
                        Ok(Ok(pair)) => pair,
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

    /// Returns `(store, schema_guard)` or `None` if Postgres is unreachable
    /// or `BASIN_TEST_PG_DSN` is unset.
    async fn try_connect() -> Option<(Arc<dyn AuthStore>, SchemaGuard)> {
        let dsn = test_dsn()?;
        let schema = unique_schema();
        let connect =
            tokio::time::timeout(Duration::from_secs(3), tokio_postgres::connect(&dsn, NoTls))
                .await;
        let (client, conn) = match connect {
            Ok(Ok(pair)) => pair,
            _ => {
                eprintln!("conformance: postgres unreachable ({dsn}), skipping");
                return None;
            }
        };
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let store = Arc::new(PostgresAuthStore::new(client, schema.clone()));
        // Run migrations so the tables exist.
        store
            .migrate(&schema)
            .await
            .expect("conformance migration must succeed");
        let guard = SchemaGuard { schema, dsn };
        Some((store, guard))
    }

    /// Run the full conformance suite against `PostgresAuthStore`.
    ///
    /// Requires `BASIN_TEST_PG_DSN`. Skip gracefully when Postgres is absent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires BASIN_TEST_PG_DSN"]
    async fn pg_conformance() {
        let Some((store, _guard)) = try_connect().await else {
            return;
        };
        super::test_auth_store_conformance(store).await;
    }
}
