//! ORM-driver connect-sequence compatibility.
//!
//! psycopg / SQLAlchemy (and other PG drivers) run a fixed handshake of
//! introspection statements when opening a connection and FETCH a row from each
//! one. If any returns an empty result (no row) the driver raises
//! "no results to fetch" and aborts the whole connection — which cascades to
//! every test in that suite of the live-ORM harness. This pins that each
//! connect-time statement returns at least one fetchable row, independent of the
//! live-ORM harness (which needs python/node toolchains).
//!
//! Regressions guarded here, in the order the live SQLAlchemy connect issues
//! them:
//!   * `SELECT version()` / `select pg_catalog.version()`  — server banner
//!   * `select current_schema()`                           — search-path head
//!   * `show transaction isolation level`                  — isolation level
//!     (sqlparser lowers this to `SHOW <identifiers>`; the engine must return a
//!     row, not an empty `SHOW` tag).

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn open_session() -> (TempDir, Engine, ProjectSession) {
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    (dir, engine, sess)
}

/// Number of result rows for `sql`, or a panic naming the statement when it does
/// not return a row-set (the exact failure a fetching driver hits).
async fn row_count(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { tag }) => panic!(
            "connect-sequence statement [{sql}] returned an empty result (tag={tag:?}); \
             a fetching driver (psycopg/SQLAlchemy) raises \"no results to fetch\" and \
             aborts the connection"
        ),
        Err(e) => panic!("connect-sequence statement [{sql}] errored: {e:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn psycopg_connect_sequence_each_statement_returns_a_row() {
    let (_dir, _engine, sess) = open_session().await;

    // Every statement a driver fetches a value from must yield >= 1 row.
    for sql in [
        "SELECT version()",
        "select pg_catalog.version()",
        "select current_schema()",
        "show transaction isolation level",
        "SHOW transaction_isolation",
        // A genuinely unknown GUC must still be fetchable (one empty-valued row),
        // not an empty result — drivers probe assorted GUCs at connect.
        "SHOW some_unknown_guc_xyz",
    ] {
        let n = row_count(&sess, sql).await;
        assert!(n >= 1, "[{sql}] must return at least one row, got {n}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_banner_advertises_postgres_major_15() {
    let (_dir, _engine, sess) = open_session().await;
    let (schema, batches) = match sess.execute("SELECT version()").await {
        Ok(ExecResult::Rows { schema, batches }) => (schema, batches),
        other => panic!("version() must return rows, got {other:?}"),
    };
    // pgwire builds the RowDescription from the ExecResult schema (not the
    // RecordBatch schema), so THAT is the column name the client sees. typeorm's
    // PostgresQueryRunner.getVersion() reads `result[0].version` BY COLUMN NAME
    // and calls `.replace()` on it — so the output column of `SELECT version()`
    // must be named exactly "version" (as PostgreSQL names it), or typeorm
    // crashes at connect with "Cannot read properties of undefined (reading
    // 'replace')".
    assert_eq!(
        schema.field(0).name(),
        "version",
        "SELECT version() must name its column \"version\" (node-postgres/typeorm read by name)"
    );
    let b = batches.iter().find(|b| b.num_rows() > 0).expect("a row");
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("version() returns text");
    let banner = col.value(0);
    assert!(
        banner.starts_with("PostgreSQL 15"),
        "version() banner must advertise PostgreSQL 15, got {banner:?}"
    );
}
