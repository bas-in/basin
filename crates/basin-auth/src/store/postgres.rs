//! `PostgresAuthStore` — [`AuthStore`] impl backed by a `tokio_postgres::Client`.
//!
//! This is the external-Postgres path, active when `BASIN_AUTH_CATALOG_DSN` is
//! set in the environment. Auth state is stored in a separate Postgres instance
//! (or managed PG service such as Neon / RDS) in the schema named by
//! `BASIN_AUTH_CATALOG_SCHEMA` (default `basin_auth`). The table names use the
//! flat `{schema}_<table>` convention described in `schema.rs` so the same SQL
//! works against both this implementation and `EngineAuthStore`.
//!
//! The client is wrapped in a `Mutex` so it can be shared across async tasks
//! without a full connection pool in v0.1.

use std::collections::HashMap;

use async_trait::async_trait;
use basin_common::{BasinError, Result, TenantId};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tokio_postgres::Client;
use uuid::Uuid;

use super::{
    ApiKeyRow, AuthMagicLinkRow, AuthStore, AuthUser, EmailTokenRow, MagicLinkEmailTokenRow,
    RefreshRevocationRow, TenantCredentialRow,
};
use crate::{api_keys::ApiKeyDescriptor, schema, tenant_credentials::TenantCredentialDescriptor, UserId};

/// Postgres-backed auth store. Wraps a `tokio_postgres::Client` in a `Mutex`
/// so it can be shared across async tasks without needing a connection pool
/// for v0.1.
///
/// The `schema` field is the table-name prefix (e.g. `basin_auth`). All SQL
/// emitted here uses `{schema}_<table>` as the flat table name, matching the
/// basin-engine dialect described in `schema.rs`.
pub struct PostgresAuthStore {
    client: Mutex<Client>,
    schema: String,
}

impl PostgresAuthStore {
    /// Wrap an already-connected `tokio_postgres::Client`.
    pub fn new(client: Client, schema: String) -> Self {
        Self {
            client: Mutex::new(client),
            schema,
        }
    }

    fn sch(&self) -> &str {
        &self.schema
    }
}

#[async_trait]
impl AuthStore for PostgresAuthStore {
    // --- Migration ----------------------------------------------------------

    async fn migrate(&self, schema: &str) -> Result<()> {
        let client = self.client.lock().await;
        schema::run_migrations(&client, schema).await
    }

    // --- Users --------------------------------------------------------------

    async fn create_user(
        &self,
        tenant: &TenantId,
        email: &str,
        password_hash: &str,
        user_id: UserId,
    ) -> Result<UserId> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let inserted = client
            .execute(
                &format!(
                    "INSERT INTO {sch}_users (user_id, tenant_id, email, password_hash)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (tenant_id, email) DO NOTHING",
                    sch = self.sch()
                ),
                &[&user_id, &tenant_str, &email, &password_hash],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("signup insert: {e}")))?;
        if inserted == 0 {
            return Err(BasinError::CommitConflict(format!(
                "user {email} already exists for tenant {tenant}"
            )));
        }
        Ok(user_id)
    }

    async fn find_user_by_email(
        &self,
        tenant: &TenantId,
        email: &str,
    ) -> Result<Option<AuthUser>> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT user_id, email, password_hash, email_verified_at
                     FROM {sch}_users
                     WHERE tenant_id = $1 AND email = $2",
                    sch = self.sch()
                ),
                &[&tenant_str, &email],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_user_by_email: {e}")))?;
        Ok(row.map(|r| AuthUser {
            user_id: r.get(0),
            email: r.get(1),
            password_hash: r.get(2),
            email_verified_at: r.get(3),
        }))
    }

    async fn find_user_by_id(
        &self,
        tenant: &TenantId,
        user_id: UserId,
    ) -> Result<Option<AuthUser>> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT user_id, email, password_hash, email_verified_at
                     FROM {sch}_users
                     WHERE user_id = $1 AND tenant_id = $2",
                    sch = self.sch()
                ),
                &[&user_id, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_user_by_id: {e}")))?;
        Ok(row.map(|r| AuthUser {
            user_id: r.get(0),
            email: r.get(1),
            password_hash: r.get(2),
            email_verified_at: r.get(3),
        }))
    }

    async fn any_user_by_email(&self, email: &str) -> Result<Option<()>> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT 1 FROM {sch}_users WHERE email = $1 LIMIT 1",
                    sch = self.sch()
                ),
                &[&email],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("any_user_by_email: {e}")))?;
        Ok(row.map(|_| ()))
    }

    async fn latest_user_by_email(&self, email: &str) -> Result<Option<(UserId, TenantId)>> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT user_id, tenant_id FROM {sch}_users
                     WHERE email = $1
                     ORDER BY created_at DESC
                     LIMIT 1",
                    sch = self.sch()
                ),
                &[&email],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("latest_user_by_email: {e}")))?;
        if let Some(r) = row {
            let user_id: Uuid = r.get(0);
            let tenant_str: String = r.get(1);
            let tenant: TenantId = tenant_str
                .parse()
                .map_err(|e| BasinError::internal(format!("latest_user_by_email tenant parse: {e}")))?;
            Ok(Some((user_id, tenant)))
        } else {
            Ok(None)
        }
    }

    async fn mark_email_verified(&self, tenant: &TenantId, user_id: UserId) -> Result<()> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "UPDATE {sch}_users SET email_verified_at = now()
                     WHERE user_id = $1 AND tenant_id = $2",
                    sch = self.sch()
                ),
                &[&user_id, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("mark_email_verified: {e}")))?;
        Ok(())
    }

    async fn mark_email_verified_if_null(&self, tenant: &TenantId, user_id: UserId) -> Result<()> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "UPDATE {sch}_users SET email_verified_at = COALESCE(email_verified_at, now())
                     WHERE user_id = $1 AND tenant_id = $2",
                    sch = self.sch()
                ),
                &[&user_id, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("mark_email_verified_if_null: {e}")))?;
        Ok(())
    }

    async fn update_password(
        &self,
        tenant: &TenantId,
        user_id: UserId,
        password_hash: &str,
    ) -> Result<()> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "UPDATE {sch}_users SET password_hash = $1
                     WHERE user_id = $2 AND tenant_id = $3",
                    sch = self.sch()
                ),
                &[&password_hash, &user_id, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("update_password: {e}")))?;
        Ok(())
    }

    // --- Email tokens -------------------------------------------------------

    async fn insert_email_token(
        &self,
        tenant: &TenantId,
        user_id: UserId,
        token_hash: &str,
        purpose: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}_email_tokens
                       (token_hash, user_id, tenant_id, purpose, expires_at)
                     VALUES ($1, $2, $3, $4, $5)",
                    sch = self.sch()
                ),
                &[&token_hash, &user_id, &tenant_str, &purpose, &expires_at],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_email_token: {e}")))?;
        Ok(())
    }

    async fn find_email_token(
        &self,
        tenant: &TenantId,
        token_hash: &str,
    ) -> Result<Option<EmailTokenRow>> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT user_id, expires_at, consumed_at, purpose
                     FROM {sch}_email_tokens
                     WHERE token_hash = $1 AND tenant_id = $2",
                    sch = self.sch()
                ),
                &[&token_hash, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_email_token: {e}")))?;
        Ok(row.map(|r| EmailTokenRow {
            user_id: r.get(0),
            expires_at: r.get(1),
            consumed_at: r.get(2),
            purpose: r.get(3),
        }))
    }

    async fn find_magic_link_email_token(
        &self,
        tenant: &TenantId,
        token_hash: &str,
    ) -> Result<Option<MagicLinkEmailTokenRow>> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT et.user_id, et.expires_at, et.consumed_at, et.purpose, u.email
                     FROM {sch}_email_tokens et
                     JOIN {sch}_users u
                       ON u.user_id = et.user_id AND u.tenant_id = et.tenant_id
                     WHERE et.token_hash = $1 AND et.tenant_id = $2",
                    sch = self.sch()
                ),
                &[&token_hash, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_magic_link_email_token: {e}")))?;
        Ok(row.map(|r| MagicLinkEmailTokenRow {
            user_id: r.get(0),
            expires_at: r.get(1),
            consumed_at: r.get(2),
            purpose: r.get(3),
            email: r.get(4),
        }))
    }

    async fn consume_email_token(
        &self,
        tenant: &TenantId,
        token_hash: &str,
    ) -> Result<u64> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}_email_tokens SET consumed_at = now()
                     WHERE token_hash = $1 AND tenant_id = $2 AND consumed_at IS NULL",
                    sch = self.sch()
                ),
                &[&token_hash, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("consume_email_token: {e}")))?;
        Ok(n)
    }

    // --- Revoked refresh tokens ---------------------------------------------

    async fn insert_refresh_revocation(
        &self,
        jti: &str,
        user_id: UserId,
        expires_at: DateTime<Utc>,
    ) -> Result<u64> {
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "INSERT INTO {sch}_auth_revoked_refresh_tokens
                       (token_hash, user_id, revoked_at, expires_at)
                     VALUES ($1, $2, now(), $3)
                     ON CONFLICT (token_hash) DO NOTHING",
                    sch = self.sch()
                ),
                &[&jti, &user_id, &expires_at],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_refresh_revocation: {e}")))?;
        Ok(n)
    }

    async fn upsert_refresh_revocation(
        &self,
        jti: &str,
        user_id: UserId,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}_auth_revoked_refresh_tokens
                       (token_hash, user_id, revoked_at, expires_at)
                     VALUES ($1, $2, now(), $3)
                     ON CONFLICT (token_hash) DO UPDATE SET revoked_at = now()",
                    sch = self.sch()
                ),
                &[&jti, &user_id, &expires_at],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("upsert_refresh_revocation: {e}")))?;
        Ok(())
    }

    async fn list_refresh_revocations(
        &self,
        user_id: UserId,
    ) -> Result<Vec<RefreshRevocationRow>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT token_hash, revoked_at, expires_at
                     FROM {sch}_auth_revoked_refresh_tokens
                     WHERE user_id = $1 AND expires_at > now()",
                    sch = self.sch()
                ),
                &[&user_id],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_refresh_revocations: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| RefreshRevocationRow {
                token_hash: r.get(0),
                revoked_at: r.get(1),
            })
            .collect())
    }

    async fn upsert_blanket_revocation(
        &self,
        blanket_key: &str,
        user_id: UserId,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}_auth_revoked_refresh_tokens
                       (token_hash, user_id, revoked_at, expires_at)
                     VALUES ($1, $2, now(), $3)
                     ON CONFLICT (token_hash) DO UPDATE
                       SET revoked_at = now(),
                           expires_at = EXCLUDED.expires_at",
                    sch = self.sch()
                ),
                &[&blanket_key, &user_id, &expires_at],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("upsert_blanket_revocation: {e}")))?;
        Ok(())
    }

    // --- API keys -----------------------------------------------------------

    async fn insert_api_key(
        &self,
        tenant: &TenantId,
        user_id: UserId,
        name: &str,
        key_hash: &str,
        key_bcrypt: &str,
    ) -> Result<(i64, DateTime<Utc>)> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "INSERT INTO {sch}_api_keys
                       (tenant_id, user_id, name, key_hash, key_bcrypt)
                     VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (tenant_id, user_id, name) DO NOTHING
                     RETURNING id, created_at",
                    sch = self.sch()
                ),
                &[&tenant_str, &user_id, &name, &key_hash, &key_bcrypt],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_api_key: {e}")))?;
        let Some(row) = row else {
            return Err(BasinError::CommitConflict(format!(
                "api key {name:?} already exists for user"
            )));
        };
        Ok((row.get(0), row.get(1)))
    }

    async fn find_api_keys_by_hash(&self, key_hash: &str) -> Result<Vec<ApiKeyRow>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT id, tenant_id, user_id, key_bcrypt, revoked_at
                     FROM {sch}_api_keys
                     WHERE key_hash = $1",
                    sch = self.sch()
                ),
                &[&key_hash],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_api_keys_by_hash: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| ApiKeyRow {
                id: r.get(0),
                tenant_id: r.get(1),
                user_id: r.get(2),
                key_bcrypt: r.get(3),
                revoked_at: r.get(4),
            })
            .collect())
    }

    async fn touch_api_key(&self, id: i64) -> Result<()> {
        let client = self.client.lock().await;
        let _ = client
            .execute(
                &format!(
                    "UPDATE {sch}_api_keys SET last_used_at = now()
                     WHERE id = $1",
                    sch = self.sch()
                ),
                &[&id],
            )
            .await;
        Ok(())
    }

    async fn revoke_api_key(&self, tenant: &TenantId, key_id: i64) -> Result<()> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}_api_keys SET revoked_at = now()
                     WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL",
                    sch = self.sch()
                ),
                &[&key_id, &tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("revoke_api_key: {e}")))?;
        if n == 0 {
            let exists = client
                .query_opt(
                    &format!(
                        "SELECT 1 FROM {sch}_api_keys
                         WHERE id = $1 AND tenant_id = $2",
                        sch = self.sch()
                    ),
                    &[&key_id, &tenant_str],
                )
                .await
                .map_err(|e| BasinError::catalog(format!("revoke_api_key check: {e}")))?;
            if exists.is_none() {
                return Err(BasinError::not_found(format!("api key {key_id}")));
            }
            // Already revoked — idempotent.
        }
        Ok(())
    }

    async fn list_api_keys(
        &self,
        tenant: &TenantId,
        user_id: UserId,
    ) -> Result<Vec<ApiKeyDescriptor>> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT id, name, created_at, last_used_at, revoked_at
                     FROM {sch}_api_keys
                     WHERE tenant_id = $1 AND user_id = $2
                     ORDER BY id ASC",
                    sch = self.sch()
                ),
                &[&tenant_str, &user_id],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_api_keys: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| ApiKeyDescriptor {
                id: r.get(0),
                name: r.get(1),
                created_at: r.get(2),
                last_used_at: r.get(3),
                revoked_at: r.get(4),
            })
            .collect())
    }

    // --- Session settings ---------------------------------------------------

    async fn upsert_session_setting(
        &self,
        tenant: &TenantId,
        user_id: UserId,
        key: &str,
        value: &str,
    ) -> Result<()> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}_user_session_settings
                       (tenant_id, user_id, key, value)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (tenant_id, user_id, key)
                     DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                    sch = self.sch()
                ),
                &[&tenant_str, &user_id, &key, &value],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("upsert_session_setting: {e}")))?;
        Ok(())
    }

    async fn list_session_settings(
        &self,
        tenant: &TenantId,
        user_id: UserId,
    ) -> Result<HashMap<String, String>> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT key, value
                     FROM {sch}_user_session_settings
                     WHERE tenant_id = $1 AND user_id = $2",
                    sch = self.sch()
                ),
                &[&tenant_str, &user_id],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_session_settings: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect())
    }

    // --- Tenant credentials -------------------------------------------------

    async fn insert_tenant_credential(
        &self,
        tenant: &TenantId,
        pgwire_user: &str,
        password_hash: &str,
        dbname: &str,
    ) -> Result<bool> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "INSERT INTO {sch}_auth_tenant_credentials
                       (tenant_id, pgwire_user, password_hash, dbname)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (pgwire_user) DO NOTHING
                     RETURNING id",
                    sch = self.sch()
                ),
                &[&tenant_str, &pgwire_user, &password_hash, &dbname],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_tenant_credential: {e}")))?;
        Ok(row.is_some())
    }

    async fn find_tenant_credential(
        &self,
        pgwire_user: &str,
    ) -> Result<Option<TenantCredentialRow>> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "SELECT tenant_id, password_hash, dbname
                     FROM {sch}_auth_tenant_credentials
                     WHERE pgwire_user = $1",
                    sch = self.sch()
                ),
                &[&pgwire_user],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("find_tenant_credential: {e}")))?;
        Ok(row.map(|r| TenantCredentialRow {
            tenant_id: r.get(0),
            password_hash: r.get(1),
            dbname: r.get(2),
        }))
    }

    async fn rotate_tenant_credential(
        &self,
        pgwire_user: &str,
        password_hash: &str,
    ) -> Result<Option<TenantCredentialRow>> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                &format!(
                    "UPDATE {sch}_auth_tenant_credentials
                       SET password_hash = $1, rotated_at = now()
                     WHERE pgwire_user = $2
                     RETURNING tenant_id, dbname",
                    sch = self.sch()
                ),
                &[&password_hash, &pgwire_user],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("rotate_tenant_credential: {e}")))?;
        Ok(row.map(|r| TenantCredentialRow {
            tenant_id: r.get(0),
            dbname: r.get(1),
            // password_hash not returned from UPDATE — caller doesn't need
            // the stored hash after rotation.
            password_hash: String::new(),
        }))
    }

    async fn list_tenant_credentials(
        &self,
        tenant: &TenantId,
    ) -> Result<Vec<TenantCredentialDescriptor>> {
        let tenant_str = tenant.to_string();
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT id, tenant_id, pgwire_user, dbname, created_at, rotated_at
                     FROM {sch}_auth_tenant_credentials
                     WHERE tenant_id = $1
                     ORDER BY id ASC",
                    sch = self.sch()
                ),
                &[&tenant_str],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_tenant_credentials: {e}")))?;
        rows.into_iter()
            .map(|r| {
                let t_str: String = r.get(1);
                let t: TenantId = t_str.parse().map_err(|e| {
                    BasinError::internal(format!("list_tenant_credentials tenant parse: {e}"))
                })?;
                Ok(TenantCredentialDescriptor {
                    id: r.get(0),
                    tenant_id: t,
                    pgwire_user: r.get(2),
                    dbname: r.get(3),
                    created_at: r.get(4),
                    rotated_at: r.get(5),
                })
            })
            .collect()
    }

    async fn list_legacy_tenant_credentials(&self) -> Result<Vec<(TenantId, String)>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT tenant_id, pgwire_user
                     FROM {sch}_auth_tenant_credentials
                     WHERE pgwire_user LIKE 'tenant_%'
                     ORDER BY id ASC",
                    sch = self.sch()
                ),
                &[],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_legacy_tenant_credentials: {e}")))?;
        rows.into_iter()
            .map(|r| {
                let t_str: String = r.get(0);
                let t: TenantId = t_str.parse().map_err(|e| {
                    BasinError::internal(format!(
                        "list_legacy_tenant_credentials tenant parse: {e}"
                    ))
                })?;
                Ok((t, r.get::<_, String>(1)))
            })
            .collect()
    }

    async fn delete_tenant_credential(&self, pgwire_user: &str) -> Result<()> {
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "DELETE FROM {sch}_auth_tenant_credentials
                     WHERE pgwire_user = $1",
                    sch = self.sch()
                ),
                &[&pgwire_user],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("delete_tenant_credential: {e}")))?;
        if n == 0 {
            return Err(BasinError::not_found(format!(
                "pgwire_user {pgwire_user:?} not found for deletion"
            )));
        }
        Ok(())
    }

    // --- Auth magic links (tenant-agnostic) ---------------------------------

    async fn insert_auth_magic_link(
        &self,
        email: &str,
        token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "INSERT INTO {sch}_auth_magic_links (email, token_hash, expires_at)
                     VALUES ($1, $2, $3)",
                    sch = self.sch()
                ),
                &[&email, &token_hash, &expires_at],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("insert_auth_magic_link: {e}")))?;
        Ok(())
    }

    async fn list_active_auth_magic_links(&self) -> Result<Vec<AuthMagicLinkRow>> {
        let client = self.client.lock().await;
        let rows = client
            .query(
                &format!(
                    "SELECT id, email, token_hash, expires_at, consumed_at
                     FROM {sch}_auth_magic_links
                     WHERE consumed_at IS NULL
                       AND expires_at > now()",
                    sch = self.sch()
                ),
                &[],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("list_active_auth_magic_links: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| AuthMagicLinkRow {
                id: r.get(0),
                email: r.get(1),
                token_hash: r.get(2),
            })
            .collect())
    }

    async fn consume_auth_magic_link(&self, id: i64) -> Result<u64> {
        let client = self.client.lock().await;
        let n = client
            .execute(
                &format!(
                    "UPDATE {sch}_auth_magic_links
                     SET consumed_at = now()
                     WHERE id = $1 AND consumed_at IS NULL",
                    sch = self.sch()
                ),
                &[&id],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("consume_auth_magic_link: {e}")))?;
        Ok(n)
    }

    async fn mark_email_verified_by_user_id(&self, user_id: UserId) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                &format!(
                    "UPDATE {sch}_users
                     SET email_verified_at = COALESCE(email_verified_at, now())
                     WHERE user_id = $1",
                    sch = self.sch()
                ),
                &[&user_id],
            )
            .await
            .map_err(|e| BasinError::catalog(format!("mark_email_verified_by_user_id: {e}")))?;
        Ok(())
    }
}
