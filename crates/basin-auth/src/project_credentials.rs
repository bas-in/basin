//! Per-project pgwire credentials.
//!
//! One row per project in `auth_project_credentials`. Each row maps a public
//! `pgwire_user` (the literal string a customer pastes into a Postgres URL,
//! e.g. `01JBAS1NAVTH00000000000000_a1b2c3d4`) to a project id, a
//! bcrypt-hashed password, and a `dbname`. Validation is bcrypt-only — no
//! second sha256 fast path — because the authentication boundary on a pgwire
//! connection is per-connection (cheap enough at 12-cost bcrypt for the
//! wedge).
//!
//! Plaintext passwords leave this module exactly twice: from
//! `provision_project_db` and from `rotate_pgwire_password`. After that, the
//! plaintext is permanently gone — only the bcrypt hash and the public
//! descriptor survive.
//!
//! ## pgwire_user format
//!
//! `{project_id}_{8 hex chars}` where project_id is the full 26-char ULID.
//! Example: `01JBAS1NAVTH00000000000000_a1b2c3d4`.
//! The project_id is embedded so callers can extract it without a DB round-trip
//! using [`parse_project_from_pgwire_user`].

use base64::Engine;
use basin_common::{BasinError, Result, ProjectId};
use chrono::{DateTime, Utc};
use rand::RngCore;

use crate::{password, Inner};

/// 24 random bytes -> ~32 chars of `URL_SAFE_NO_PAD` base64. The character
/// set is `A-Za-z0-9-_`, which means the password pastes cleanly into a
/// `postgres://user:password@host/db` URL without percent-encoding — that's
/// the whole point of choosing this alphabet over the standard one.
const PASSWORD_BYTES: usize = 24;

/// Public-facing descriptor for `list_project_credentials`. Carries the
/// columns an operator needs to render a credentials page; never the hash,
/// never the plaintext.
#[derive(Debug, Clone)]
pub struct ProjectCredentialDescriptor {
    pub id: i64,
    pub project_id: ProjectId,
    pub pgwire_user: String,
    pub dbname: String,
    pub created_at: DateTime<Utc>,
    pub rotated_at: Option<DateTime<Utc>>,
}

/// Returned exactly once from `provision_project_db` and `rotate_pgwire_password`.
/// The `password_secret` field is the only place the plaintext exists outside
/// the customer's clipboard; after the response goes out, it's gone.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub project_id: ProjectId,
    pub pgwire_user: String,
    pub dbname: String,
    /// Plaintext password — present only on the response from
    /// `provision_project_db` / `rotate_pgwire_password`, never returned again.
    pub password_secret: String,
    /// Convenience: a complete `postgres://user:password@host/dbname` URL
    /// the customer can paste into psql / tokio-postgres / asyncpg / JDBC.
    pub connection_url: String,
}

/// Generate a pgwire username in the new ADR-0013 format:
/// `{26-char-project-ulid}_{8 hex chars}`. The project ULID embedded in the
/// username makes it self-routing — the pgwire auth handler can call
/// [`parse_project_from_pgwire_user`] to resolve the project without a global
/// DB lookup, even before the password has been verified.
fn generate_pgwire_user(project: &ProjectId) -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("{}_{}", project.to_string(), hex::encode(buf))
}

fn generate_password() -> String {
    let mut buf = [0u8; PASSWORD_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// Returns `true` if `pgwire_user` is in the pre-ADR-0013 `project_<hex>`
/// format, which does not embed the project ULID and therefore requires a DB
/// lookup to resolve the project. Credentials in this format must be migrated
/// to the new `{26-char-ulid}_{hex}` format via
/// [`migrate_legacy_credential`] before the self-routing path can be used.
///
/// The check is deliberately conservative: only strings that start with
/// `"project_"` AND are shorter than 20 chars are treated as legacy (the old
/// format is `project_` + 8 hex chars = 15 chars; the new format is at least
/// 28 chars).
pub fn is_legacy_pgwire_user(pgwire_user: &str) -> bool {
    pgwire_user.starts_with("project_") && pgwire_user.len() < 20
}

/// Extract the 26-character project ULID from a new-format pgwire username
/// (`{26-char-ulid}_{suffix}`). This makes credential validation self-routing:
/// the pgwire handler can identify the project from the username alone without a
/// global DB lookup, enabling efficient per-project auth table access. Returns
/// `None` for usernames that don't match the format (e.g. legacy `project_<hex>`
/// credentials).
pub fn parse_project_from_pgwire_user(pgwire_user: &str) -> Option<&str> {
    // ULID is 26 chars, followed by '_'
    if pgwire_user.len() > 27 && pgwire_user.as_bytes()[26] == b'_' {
        Some(&pgwire_user[..26])
    } else {
        None
    }
}

/// Validate that a pgwire_user matches the new `{26-char-ulid}_{hex}` format.
/// Also accepts the legacy `project_{hex}` format for backwards-compat reads.
fn validate_pgwire_user_format(s: &str) -> Result<()> {
    // New format: 26-char ULID + '_' + hex suffix
    if s.len() > 27 && s.as_bytes()[26] == b'_' {
        let suffix = &s[27..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(());
        }
    }
    // Legacy format: `project_<hex>`
    if s.starts_with("project_") {
        let suffix = &s[7..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(());
        }
    }
    Err(BasinError::InvalidIdent(format!(
        "pgwire_user has invalid format: {s:?}"
    )))
}

fn validate_dbname(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(BasinError::InvalidIdent("dbname is empty".into()));
    }
    if s.len() > 63 {
        return Err(BasinError::InvalidIdent(
            "dbname longer than 63 chars".into(),
        ));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(BasinError::InvalidIdent(format!(
            "dbname has invalid char: {s:?}"
        )));
    }
    Ok(())
}

fn build_connection_url(host: &str, user: &str, password: &str, dbname: &str) -> String {
    // `user` is `{ulid}_{hex}` and the password is `URL_SAFE_NO_PAD` base64,
    // so neither needs percent-encoding. Same goes for dbname (validated
    // above as alphanumeric/_/-). If we ever broaden any of those, this
    // helper grows a real percent-encoder.
    format!("postgres://{user}:{password}@{host}/{dbname}")
}

pub(crate) async fn provision(
    inner: &Inner,
    project: &ProjectId,
    dbname: Option<&str>,
) -> Result<ConnectionInfo> {
    let dbname = dbname.unwrap_or("basin");
    validate_dbname(dbname)?;

    // Retry on the rare username collision; 32 bits of entropy is plenty
    // for v0.1, but the UNIQUE index can still kick on a duplicate.
    let mut last_err = None;
    for _ in 0..4 {
        let user = generate_pgwire_user(project);
        validate_pgwire_user_format(&user)?;
        let secret = generate_password();
        let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;
        let inserted = inner
            .store
            .insert_project_credential(project, &user, &hash, dbname)
            .await?;
        if inserted {
            let connection_url =
                build_connection_url(&inner.cfg.pgwire_public_host, &user, &secret, dbname);
            return Ok(ConnectionInfo {
                project_id: *project,
                pgwire_user: user,
                dbname: dbname.to_owned(),
                password_secret: secret,
                connection_url,
            });
        } else {
            last_err = Some(BasinError::CommitConflict(format!(
                "pgwire_user {user:?} collided"
            )));
        }
    }
    Err(last_err
        .unwrap_or_else(|| BasinError::internal("provision_project_db: exhausted user retries")))
}

pub(crate) async fn validate(inner: &Inner, user: &str, password_plain: &str) -> Result<ProjectId> {
    // Uniform error for both "no such user" and "user exists but wrong
    // password" — keeps the SQLSTATE 28P01 surface minimal and avoids
    // leaking enumeration information. bcrypt's constant-ish work factor
    // does the timing-attack hardening; we add a single error string here.
    let unknown = || BasinError::InvalidIdent("invalid pgwire credentials".into());
    if user.is_empty() {
        return Err(unknown());
    }
    let row = inner.store.find_project_credential(user).await?;
    let Some(row) = row else {
        return Err(unknown());
    };
    if !password::verify(password_plain, &row.password_hash)? {
        return Err(unknown());
    }
    let project: ProjectId = row
        .project_id
        .parse()
        .map_err(|e| BasinError::internal(format!("project_credentials project parse: {e}")))?;
    Ok(project)
}

pub(crate) async fn rotate(inner: &Inner, pgwire_user: &str) -> Result<ConnectionInfo> {
    validate_pgwire_user_format(pgwire_user)?;
    let secret = generate_password();
    let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;
    let row = inner
        .store
        .rotate_project_credential(pgwire_user, &hash)
        .await?;
    let Some(row) = row else {
        return Err(BasinError::not_found(format!(
            "pgwire_user {pgwire_user:?}"
        )));
    };
    let project: ProjectId = row
        .project_id
        .parse()
        .map_err(|e| BasinError::internal(format!("project_credentials project parse: {e}")))?;
    let connection_url = build_connection_url(
        &inner.cfg.pgwire_public_host,
        pgwire_user,
        &secret,
        &row.dbname,
    );
    Ok(ConnectionInfo {
        project_id: project,
        pgwire_user: pgwire_user.to_owned(),
        dbname: row.dbname,
        password_secret: secret,
        connection_url,
    })
}

pub(crate) async fn list(
    inner: &Inner,
    project: &ProjectId,
) -> Result<Vec<ProjectCredentialDescriptor>> {
    inner.store.list_project_credentials(project).await
}

/// Returns every credential row whose `pgwire_user` is in the old
/// `project_<hex>` format. Used by the upgrade migration to discover rows
/// that need to be rotated to the new `{project_id}_{hex}` format.
///
/// Returns a list of `(project_id, old_pgwire_user)` pairs.
pub(crate) async fn list_legacy(inner: &Inner) -> Result<Vec<(ProjectId, String)>> {
    inner.store.list_legacy_project_credentials().await
}

/// Rotates a single legacy credential in place:
///
/// 1. Generates a new pgwire_user in the new `{project_id}_{hex}` format.
/// 2. Generates a new random password.
/// 3. Inserts the new credential row.
/// 4. Deletes the old credential row (only after the insert succeeds).
///
/// Returns `(new_pgwire_user, plaintext_password)`. The plaintext password
/// is returned so the caller (e.g. basin-cloud's startup migration job) can
/// update the corresponding row in cloud's `project_pgwire_credentials` table.
///
/// The function is idempotent with respect to the *old* credential: if the
/// old row is already gone (migrated by a previous run), it returns a
/// `BasinError::NotFound` which the caller should treat as "already done".
pub(crate) async fn migrate_legacy_credential(
    inner: &Inner,
    project: &ProjectId,
    old_pgwire_user: &str,
) -> Result<(String, String)> {
    // Sanity-check: only migrate credentials that actually need it.
    if !is_legacy_pgwire_user(old_pgwire_user) {
        return Err(BasinError::InvalidIdent(format!(
            "migrate_legacy_credential: {old_pgwire_user:?} is not a legacy pgwire_user"
        )));
    }

    // Try up to 4 times in case of a username collision (same as provision).
    let mut last_err = None;
    for _ in 0..4 {
        let new_user = generate_pgwire_user(project);
        let secret = generate_password();
        let hash = password::hash(&secret, inner.cfg.bcrypt_cost)?;

        // INSERT new row first — keep the old row alive until we succeed.
        let inserted = inner
            .store
            .insert_project_credential(project, &new_user, &hash, "basin")
            .await?;

        if inserted {
            // New row is in place; now remove the old one.
            inner
                .store
                .delete_project_credential(old_pgwire_user)
                .await?;
            return Ok((new_user, secret));
        } else {
            last_err = Some(BasinError::CommitConflict(format!(
                "new pgwire_user {new_user:?} collided during migration"
            )));
        }
    }
    Err(last_err.unwrap_or_else(|| {
        BasinError::internal("migrate_legacy_credential: exhausted user retries")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;

    #[test]
    fn pgwire_user_format_round_trip() {
        let project = ProjectId::new();
        let u = generate_pgwire_user(&project);
        validate_pgwire_user_format(&u).unwrap();
        // 26-char ULID + '_' + 8 hex chars = 35 chars
        assert_eq!(u.len(), 26 + 1 + 8);
        assert_eq!(&u[..26], project.to_string().as_str());
    }

    #[test]
    fn parse_project_round_trip() {
        let project = ProjectId::new();
        let u = generate_pgwire_user(&project);
        let parsed = parse_project_from_pgwire_user(&u).expect("should parse");
        assert_eq!(parsed, project.to_string().as_str());
    }

    #[test]
    fn parse_project_rejects_short() {
        assert!(parse_project_from_pgwire_user("project_a1b2c3d4").is_none());
        assert!(parse_project_from_pgwire_user("short").is_none());
    }

    #[test]
    fn pgwire_user_format_rejects_path_injection() {
        assert!(validate_pgwire_user_format("project_../etc").is_err());
        assert!(validate_pgwire_user_format("../etc").is_err());
        assert!(validate_pgwire_user_format("project_").is_err());
    }

    #[test]
    fn dbname_validation() {
        validate_dbname("basin").unwrap();
        validate_dbname("basin_2").unwrap();
        validate_dbname("a-b").unwrap();
        assert!(validate_dbname("").is_err());
        assert!(validate_dbname("../etc").is_err());
        assert!(validate_dbname(&"a".repeat(64)).is_err());
    }

    #[test]
    fn password_is_url_safe() {
        // Generated passwords must not require percent-encoding when pasted
        // into a postgres:// URL — i.e. only [A-Za-z0-9-_].
        let p = generate_password();
        for c in p.chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '-' || c == '_',
                "password contains URL-unsafe char {c:?}"
            );
        }
    }

    #[test]
    fn connection_url_shape() {
        let project = ProjectId::new();
        let user = generate_pgwire_user(&project);
        let u = build_connection_url("db.example.com:5433", &user, "supersecret", "basin");
        assert!(u.starts_with("postgres://"));
        assert!(u.contains("supersecret@db.example.com:5433/basin"));
    }

    // --- parse_project_from_pgwire_user: new comprehensive cases ---

    #[test]
    fn parse_project_valid_new_format() {
        // Valid new format: exactly 26-char ULID + '_' + any suffix
        let project = ProjectId::new();
        let project_str = project.to_string();
        assert_eq!(project_str.len(), 26, "ProjectId must be 26 chars");
        let pgwire_user = format!("{project_str}_a1b2c3d4");
        let parsed = parse_project_from_pgwire_user(&pgwire_user).expect("must parse new format");
        assert_eq!(parsed, project_str.as_str());
    }

    #[test]
    fn parse_project_requires_more_than_27_chars() {
        // Exactly 27 chars (26 + '_' + nothing) → len == 27, not > 27 → None.
        let project = ProjectId::new();
        let user_no_suffix = format!("{}_", project);
        assert_eq!(user_no_suffix.len(), 27);
        assert!(
            parse_project_from_pgwire_user(&user_no_suffix).is_none(),
            "need at least one char after the underscore"
        );
    }

    #[test]
    fn parse_project_old_legacy_format_returns_none() {
        // Old `project_<hex>` format does not embed a 26-char ULID → None.
        assert!(parse_project_from_pgwire_user("project_a1b2c3d4").is_none());
    }

    #[test]
    fn parse_project_no_underscore_at_position_26_returns_none() {
        // 26 chars with no underscore at all.
        assert!(parse_project_from_pgwire_user("01JBAS1NAVTH00000000000000").is_none());
        // 26 chars + non-underscore separator.
        assert!(parse_project_from_pgwire_user("01JBAS1NAVTH00000000000000-suffix").is_none());
    }

    #[test]
    fn parse_project_too_short_returns_none() {
        assert!(parse_project_from_pgwire_user("abc_def").is_none());
        assert!(parse_project_from_pgwire_user("").is_none());
        assert!(parse_project_from_pgwire_user("_").is_none());
    }

    #[test]
    fn parse_project_self_routing_invariant() {
        // Every username generated by `generate_pgwire_user` must parse back
        // to the exact project_id it was generated for.
        for _ in 0..20 {
            let project = ProjectId::new();
            let u = generate_pgwire_user(&project);
            let parsed = parse_project_from_pgwire_user(&u).expect("generated user must parse");
            assert_eq!(
                parsed,
                project.to_string().as_str(),
                "self-routing invariant broken: generated={u:?} parsed={parsed:?}"
            );
        }
    }

    // --- is_legacy_pgwire_user ---

    #[test]
    fn is_legacy_true_for_project_hex_format() {
        assert!(is_legacy_pgwire_user("project_a1b2c3d4"));
        assert!(is_legacy_pgwire_user("project_deadbeef"));
        // 15 chars total (7 + 8): shorter than 20 → legacy.
        assert!(is_legacy_pgwire_user("project_00000000"));
    }

    #[test]
    fn is_legacy_false_for_new_format() {
        let project = ProjectId::new();
        let u = generate_pgwire_user(&project);
        assert!(
            !is_legacy_pgwire_user(&u),
            "new-format user must not be flagged legacy: {u:?}"
        );
    }

    #[test]
    fn is_legacy_false_for_empty_string() {
        assert!(!is_legacy_pgwire_user(""));
    }

    #[test]
    fn is_legacy_false_for_long_project_prefix() {
        // Starts with "project_" but is >= 20 chars (new-format collision unlikely
        // but the boundary is what matters for migration correctness).
        let long = "project_0000000000000"; // 7 + 13 = 20 chars
        assert!(!is_legacy_pgwire_user(long), "length 20 is NOT legacy");
    }

    #[test]
    fn is_legacy_false_for_unrelated_string() {
        assert!(!is_legacy_pgwire_user(
            "01JBAS1NAVTH00000000000000_a1b2c3d4"
        ));
        assert!(!is_legacy_pgwire_user("notaprojectatall"));
    }
}
