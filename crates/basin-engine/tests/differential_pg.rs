//! Differential PG-parity harness.
//!
//! Each case replays an identical SQL script against (a) basin's embedded
//! engine and (b) a real PostgreSQL, then asserts the two backends agree on
//! the SQLSTATE outcome of a designated "probe" statement.
//!
//! The real-PG leg only runs when `PG_DIFF_TEST_DSN` is set to a libpq DSN
//! (e.g. `postgres://user:pw@localhost/postgres`). When the env var is
//! absent the cases SKIP CLEANLY (return early, test passes) — there is no
//! `#[ignore]`, so CI without a PG instance still exercises the basin leg
//! and the harness fails honestly if basin's own behaviour regresses.
//!
//! `DifferentialRunner` is the shared driver: it owns a fresh basin
//! `ProjectSession` and (when a DSN is present) a `tokio_postgres::Client`
//! against a throwaway schema, runs the setup statements on both, then
//! compares the probe statement's success/SQLSTATE.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// --------------------------------------------------------------------------
// basin embedded engine plumbing (same shape as the other test files)
// --------------------------------------------------------------------------

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

/// Map a basin error to the PG SQLSTATE basin's router would surface, so the
/// two backends can be compared on equal footing. Only the codes the
/// differential cases below probe for are modelled.
fn basin_sqlstate(err: &BasinError) -> &'static str {
    match err {
        BasinError::UniqueViolation(_) => "23505",
        BasinError::CheckViolation(_) => "23514",
        // string_data_right_truncation — VARCHAR(n)/CHAR(n) over-length.
        BasinError::StringTooLong(_) => "22001",
        _ => "XXOTHER",
    }
}

/// The outcome of running one statement: either it succeeded, or it failed
/// with a specific SQLSTATE. Used to compare basin vs PG.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Err(String),
}

/// Shared differential driver. Holds the basin session and an optional real
/// PG client (present only when `PG_DIFF_TEST_DSN` is set).
struct DifferentialRunner {
    sess: ProjectSession,
    pg: Option<tokio_postgres::Client>,
    /// Unique throwaway schema name so concurrent test runs don't collide.
    pg_schema: String,
    _dir: TempDir,
    _eng: Engine,
}

impl DifferentialRunner {
    /// Build a runner. If `PG_DIFF_TEST_DSN` is unset, `pg` is `None` and the
    /// case will run basin-only (still a real assertion against basin).
    async fn new(case_tag: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = open(&eng).await;

        let (pg, pg_schema) = match std::env::var("PG_DIFF_TEST_DSN") {
            Ok(dsn) if !dsn.trim().is_empty() => {
                let (client, connection) =
                    tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                        .await
                        .expect("PG_DIFF_TEST_DSN connect");
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let schema = format!(
                    "basin_diff_{}_{}",
                    case_tag,
                    std::process::id()
                );
                client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS {schema} CASCADE; \
                         CREATE SCHEMA {schema}; SET search_path TO {schema};"
                    ))
                    .await
                    .expect("PG schema bootstrap");
                (Some(client), schema)
            }
            _ => (None, String::new()),
        };

        Self {
            sess,
            pg,
            pg_schema,
            _dir: dir,
            _eng: eng,
        }
    }

    /// Run a setup statement on basin and (if configured) PG. Both are
    /// expected to succeed; a mismatch here is itself a parity failure.
    async fn setup(&self, sql: &str) {
        self.sess
            .execute(sql)
            .await
            .unwrap_or_else(|e| panic!("basin setup failed for {sql:?}: {e:?}"));
        if let Some(pg) = &self.pg {
            pg.batch_execute(sql)
                .await
                .unwrap_or_else(|e| panic!("PG setup failed for {sql:?}: {e}"));
        }
    }

    /// Run the probe statement on basin and PG and return both outcomes.
    async fn probe(&self, sql: &str) -> (Outcome, Option<Outcome>) {
        let basin = match self.sess.execute(sql).await {
            Ok(_) => Outcome::Ok,
            Err(e) => Outcome::Err(basin_sqlstate(&e).to_string()),
        };
        let pg = match &self.pg {
            None => None,
            Some(pg) => Some(match pg.batch_execute(sql).await {
                Ok(_) => Outcome::Ok,
                Err(e) => {
                    let code = e
                        .as_db_error()
                        .map(|db| db.code().code().to_string())
                        .unwrap_or_else(|| "XXOTHER".to_string());
                    Outcome::Err(code)
                }
            }),
        };
        (basin, pg)
    }
}

impl Drop for DifferentialRunner {
    fn drop(&mut self) {
        // Best-effort schema cleanup; the PG client's connection task is
        // detached so this is fire-and-forget if it can't complete in time.
        if self.pg.is_some() && !self.pg_schema.is_empty() {
            let schema = self.pg_schema.clone();
            // We can't easily await in Drop; rely on DROP SCHEMA IF EXISTS in
            // the next run with the same pid+tag to clean up. Document intent.
            let _ = schema;
        }
    }
}

// --------------------------------------------------------------------------
// Cases
// --------------------------------------------------------------------------

/// BUG #136: a plain-column `CREATE UNIQUE INDEX` must reject a duplicate
/// INSERT with SQLSTATE 23505 on BOTH basin and real PG.
#[tokio::test]
async fn diff_create_unique_index_duplicate_insert() {
    let r = DifferentialRunner::new("uidx_dup").await;

    r.setup("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await;
    r.setup("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await;
    r.setup("INSERT INTO users (id, email) VALUES (1, 'ada@example.com')")
        .await;

    let (basin, pg) =
        r.probe("INSERT INTO users (id, email) VALUES (2, 'ada@example.com')")
            .await;

    // basin leg (always runs): the duplicate must be rejected with 23505.
    assert_eq!(
        basin,
        Outcome::Err("23505".to_string()),
        "basin must reject duplicate under CREATE UNIQUE INDEX with 23505"
    );

    // PG leg (only when a DSN is configured): the two must agree.
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on CREATE UNIQUE INDEX duplicate-insert outcome"
        );
    }
}

/// Distinct values under a `CREATE UNIQUE INDEX` succeed on both backends.
#[tokio::test]
async fn diff_create_unique_index_distinct_values_ok() {
    let r = DifferentialRunner::new("uidx_ok").await;

    r.setup("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await;
    r.setup("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await;
    r.setup("INSERT INTO users (id, email) VALUES (1, 'a@x.com')")
        .await;

    let (basin, pg) =
        r.probe("INSERT INTO users (id, email) VALUES (2, 'b@x.com')")
            .await;

    assert_eq!(basin, Outcome::Ok, "distinct values must succeed in basin");
    if let Some(pg) = pg {
        assert_eq!(basin, pg, "basin and PG disagree on distinct-value insert");
    }
}

/// Multi-column `CREATE UNIQUE INDEX` enforces on the tuple on both backends.
#[tokio::test]
async fn diff_create_unique_index_multicolumn_duplicate() {
    let r = DifferentialRunner::new("uidx_multi").await;

    r.setup(
        "CREATE TABLE memberships (\
             id BIGINT PRIMARY KEY, org_id BIGINT NOT NULL, user_id BIGINT NOT NULL)",
    )
    .await;
    r.setup(
        "CREATE UNIQUE INDEX memberships_org_user_uidx \
         ON memberships (org_id, user_id)",
    )
    .await;
    r.setup("INSERT INTO memberships VALUES (1, 10, 100)").await;
    r.setup("INSERT INTO memberships VALUES (2, 10, 200)").await;

    let (basin, pg) = r.probe("INSERT INTO memberships VALUES (3, 10, 100)").await;

    assert_eq!(
        basin,
        Outcome::Err("23505".to_string()),
        "basin must reject duplicate tuple under multi-col CREATE UNIQUE INDEX"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on multi-column CREATE UNIQUE INDEX duplicate"
        );
    }
}

/// BUG #140: an INSERT of a value longer than a declared `VARCHAR(n)`
/// must be rejected with SQLSTATE 22001 (`string_data_right_truncation`)
/// — the same error PG raises (`value too long for type character
/// varying(n)`) — on BOTH basin and real PG.
#[tokio::test]
async fn diff_varchar_overlength_insert_rejected() {
    let r = DifferentialRunner::new("vchar_over").await;

    r.setup("CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(5))")
        .await;
    // Exactly-n is accepted on both backends.
    r.setup("INSERT INTO t (id, code) VALUES (1, 'abcde')").await;

    let (basin, pg) = r
        .probe("INSERT INTO t (id, code) VALUES (2, 'toolong')")
        .await;

    assert_eq!(
        basin,
        Outcome::Err("22001".to_string()),
        "basin must reject over-length VARCHAR(n) INSERT with 22001"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on over-length VARCHAR(n) INSERT outcome"
        );
    }
}

/// BUG #140 (CHAR variant): over-length `CHAR(n)` INSERT must also raise
/// SQLSTATE 22001 on both backends; an at-or-under-length value is
/// accepted (PG blank-pads it to n, as does basin).
#[tokio::test]
async fn diff_char_overlength_insert_rejected() {
    let r = DifferentialRunner::new("char_over").await;

    r.setup("CREATE TABLE t (id BIGINT PRIMARY KEY, code CHAR(3))")
        .await;
    r.setup("INSERT INTO t (id, code) VALUES (1, 'ab')").await;

    let (basin, pg) = r
        .probe("INSERT INTO t (id, code) VALUES (2, 'abcd')")
        .await;

    assert_eq!(
        basin,
        Outcome::Err("22001".to_string()),
        "basin must reject over-length CHAR(n) INSERT with 22001"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on over-length CHAR(n) INSERT outcome"
        );
    }
}
