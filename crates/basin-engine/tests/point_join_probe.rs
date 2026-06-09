//! End-to-end correctness + latency guard for the point-lookup INNER-JOIN
//! fast path (`point_join.rs` — the ORM hydrate `SELECT e.*, u.email FROM
//! events e JOIN users u ON e.user_id = u.id WHERE e.id = $1`).
//!
//! These tests run the canonical join SQL through `sess.execute` and assert
//! the result against seeded literal values. Because the dispatch hook in
//! `executor.rs` (the diff reported alongside this module) makes the fast path
//! transparent — `Ok(None)` falls through to DataFusion — every assertion here
//! is identical whether or not the fast path engaged. They therefore pin the
//! contract ("the fast path must produce exactly what DataFusion would") even
//! before the dispatch hook is wired in: with the hook absent the same SQL is
//! served by DataFusion and the assertions still hold.
//!
//! Coverage:
//!   * basic hydrate, both column orders
//!   * `e.*` wildcard expansion
//!   * `AS` alias naming
//!   * probe miss → 0 rows
//!   * lookup miss → 0 rows (INNER)
//!   * overlay: UPDATE the lookup row via the fast path → join sees new value
//!   * tombstone: DELETE the lookup row → join returns 0 rows
//!   * NULL join key → 0 rows
//!   * #[ignore] latency probe at 100k rows

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

async fn result(sess: &ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"))
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Read every output field name from a result's schema (projection order).
fn field_names(res: &ExecResult) -> Vec<String> {
    match res {
        ExecResult::Rows { schema, .. } => {
            schema.fields().iter().map(|f| f.name().clone()).collect()
        }
        _ => panic!("expected Rows"),
    }
}

/// Read an Int64 column's single-row value by name across the (≤1-row) batches.
fn i64_col(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let a = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column {name} is not Int64"));
        for r in 0..b.num_rows() {
            out.push((!a.is_null(r)).then(|| a.value(r)));
        }
    }
    out
}

/// Read a Utf8 column's single-row value by name across the (≤1-row) batches.
fn str_col(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let a = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {name} is not Utf8"));
        for r in 0..b.num_rows() {
            out.push((!a.is_null(r)).then(|| a.value(r).to_string()));
        }
    }
    out
}

/// Standard two-table schema + seed used by the correctness tests.
///   events(id PK, user_id, payload), users(id PK, email)
async fn seed(sess: &ProjectSession) {
    exec(
        sess,
        "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)",
    )
    .await;
    exec(
        sess,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT, payload TEXT)",
    )
    .await;
    exec(
        sess,
        "INSERT INTO users (id, email) VALUES \
         (10, 'alice@x.com'), (20, 'bob@x.com'), (30, 'carol@x.com')",
    )
    .await;
    exec(
        sess,
        "INSERT INTO events (id, user_id, payload) VALUES \
         (1, 10, 'p1'), (2, 20, 'p2'), (3, 30, 'p3'), (4, NULL, 'orphan')",
    )
    .await;
}

#[tokio::test]
async fn basic_hydrate_probe_then_lookup() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = result(
        &sess,
        "SELECT e.id, e.payload, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 2",
    )
    .await;
    let names = field_names(&res);
    assert_eq!(names, vec!["id", "payload", "email"]);
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        _ => unreachable!(),
    };
    assert_eq!(row_count(&batches), 1);
    assert_eq!(i64_col(&batches, "id"), vec![Some(2)]);
    assert_eq!(str_col(&batches, "payload"), vec![Some("p2".to_string())]);
    assert_eq!(str_col(&batches, "email"), vec![Some("bob@x.com".to_string())]);
}

#[tokio::test]
async fn basic_hydrate_lookup_columns_first() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // Lookup-side column projected BEFORE the probe-side column.
    let res = result(
        &sess,
        "SELECT u.email, e.id FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1",
    )
    .await;
    assert_eq!(field_names(&res), vec!["email", "id"]);
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        _ => unreachable!(),
    };
    assert_eq!(str_col(&batches, "email"), vec![Some("alice@x.com".to_string())]);
    assert_eq!(i64_col(&batches, "id"), vec![Some(1)]);
}

#[tokio::test]
async fn wildcard_expansion_probe_star() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = result(
        &sess,
        "SELECT e.*, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 3",
    )
    .await;
    // e.* expands to events' catalog column order: id, user_id, payload.
    assert_eq!(field_names(&res), vec!["id", "user_id", "payload", "email"]);
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        _ => unreachable!(),
    };
    assert_eq!(row_count(&batches), 1);
    assert_eq!(i64_col(&batches, "id"), vec![Some(3)]);
    assert_eq!(i64_col(&batches, "user_id"), vec![Some(30)]);
    assert_eq!(str_col(&batches, "payload"), vec![Some("p3".to_string())]);
    assert_eq!(str_col(&batches, "email"), vec![Some("carol@x.com".to_string())]);
}

#[tokio::test]
async fn alias_as_naming() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let res = result(
        &sess,
        "SELECT e.id AS event_id, u.email AS contact FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1",
    )
    .await;
    assert_eq!(field_names(&res), vec!["event_id", "contact"]);
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        _ => unreachable!(),
    };
    assert_eq!(i64_col(&batches, "event_id"), vec![Some(1)]);
    assert_eq!(str_col(&batches, "contact"), vec![Some("alice@x.com".to_string())]);
}

#[tokio::test]
async fn probe_miss_zero_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // No event with id=999 → empty.
    let batches = rows(
        &sess,
        "SELECT e.id, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 999",
    )
    .await;
    assert_eq!(row_count(&batches), 0);
}

#[tokio::test]
async fn lookup_miss_zero_rows_inner() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // Event id=5 points at a non-existent user — INNER JOIN drops it.
    exec(&sess, "INSERT INTO events (id, user_id, payload) VALUES (5, 999, 'dangling')").await;
    let batches = rows(
        &sess,
        "SELECT e.id, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 5",
    )
    .await;
    assert_eq!(row_count(&batches), 0, "dangling FK must drop under INNER JOIN");
}

#[tokio::test]
async fn null_join_key_zero_rows() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // Event id=4 has NULL user_id — NULL never equals any PK → empty.
    let batches = rows(
        &sess,
        "SELECT e.id, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 4",
    )
    .await;
    assert_eq!(row_count(&batches), 0, "NULL join key must yield no rows");
}

#[tokio::test]
async fn overlay_update_lookup_row_visible_in_join() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // Update the joined user's email via the fast UPDATE path (memtable overlay).
    exec(&sess, "UPDATE users SET email = 'alice2@x.com' WHERE id = 10").await;

    let batches = rows(
        &sess,
        "SELECT e.id, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 1",
    )
    .await;
    assert_eq!(row_count(&batches), 1);
    assert_eq!(
        str_col(&batches, "email"),
        vec![Some("alice2@x.com".to_string())],
        "join must observe the post-UPDATE overlay value"
    );
}

#[tokio::test]
async fn tombstone_delete_lookup_row_drops_join() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // Delete the joined user (tombstone in the overlay). The INNER JOIN must
    // then produce no row for the event that referenced it.
    exec(&sess, "DELETE FROM users WHERE id = 20").await;

    let batches = rows(
        &sess,
        "SELECT e.id, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 2",
    )
    .await;
    assert_eq!(row_count(&batches), 0, "deleted lookup row must drop the join result");
}

#[tokio::test]
async fn string_pk_join() {
    // Cover the Utf8 join-key extraction path.
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(&sess, "CREATE TABLE accts (code TEXT PRIMARY KEY, name TEXT)").await;
    exec(
        &sess,
        "CREATE TABLE txns (id BIGINT PRIMARY KEY, acct_code TEXT, amount BIGINT)",
    )
    .await;
    exec(&sess, "INSERT INTO accts (code, name) VALUES ('A', 'Anvil'), ('B', 'Bolt')").await;
    exec(
        &sess,
        "INSERT INTO txns (id, acct_code, amount) VALUES (1, 'A', 100), (2, 'B', 200)",
    )
    .await;

    let batches = rows(
        &sess,
        "SELECT t.id, a.name FROM txns t JOIN accts a ON t.acct_code = a.code WHERE t.id = 2",
    )
    .await;
    assert_eq!(row_count(&batches), 1);
    assert_eq!(i64_col(&batches, "id"), vec![Some(2)]);
    assert_eq!(str_col(&batches, "name"), vec![Some("Bolt".to_string())]);
}

/// p50 latency of the canonical ORM hydrate at 100k rows. Run with:
///   cargo test -p basin-engine --test point_join_probe latency -- --ignored --nocapture
#[ignore]
#[tokio::test]
async fn point_join_latency_100k() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)").await;
    exec(
        &sess,
        "CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT, payload TEXT)",
    )
    .await;

    const N: i64 = 100_000;
    // Batch the inserts so seeding doesn't dominate wall-clock.
    let mut u = String::from("INSERT INTO users (id, email) VALUES ");
    let mut e = String::from("INSERT INTO events (id, user_id, payload) VALUES ");
    for i in 1..=N {
        if i > 1 {
            u.push(',');
            e.push(',');
        }
        u.push_str(&format!("({i}, 'user{i}@x.com')"));
        e.push_str(&format!("({i}, {i}, 'p{i}')"));
        // Flush the statement every 10k rows to keep the SQL string bounded.
        if i % 10_000 == 0 || i == N {
            exec(&sess, &u).await;
            exec(&sess, &e).await;
            u = String::from("INSERT INTO users (id, email) VALUES ");
            e = String::from("INSERT INTO events (id, user_id, payload) VALUES ");
        }
    }

    let sql = "SELECT e.id, e.payload, u.email FROM events e JOIN users u ON e.user_id = u.id WHERE e.id = 50000";
    // Warm.
    let _ = rows(&sess, sql).await;

    let iters = 200;
    let mut samples: Vec<u128> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = std::time::Instant::now();
        let b = rows(&sess, sql).await;
        assert_eq!(row_count(&b), 1);
        samples.push(t.elapsed().as_micros());
    }
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p99 = samples[(samples.len() * 99) / 100];
    println!(
        "[point-join-latency] p50={}us p99={}us over {} iters at {}k rows",
        p50,
        p99,
        iters,
        N / 1000
    );
}
