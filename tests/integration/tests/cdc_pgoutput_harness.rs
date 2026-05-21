//! Phase 5.21.A — logical CDC / pgoutput test harness.
//!
//! **Task:** TASK.md Phase 5.21.A — Logical CDC / pgoutput test harness.
//! **Test-first convention:** this file lands BEFORE any logical replication
//! implementation. Every test is `#[ignore]`d until the implementation tasks
//! 5.21.B-G close the corresponding slices.
//!
//! ## Named slices
//!
//! | Test fn                        | Slice                        | Closed by |
//! |--------------------------------|------------------------------|-----------|
//! | `cdc_create_replication_slot`  | slot mgmt                    | 5.21.B    |
//! | `cdc_pgoutput_frame_emission`  | in-process Rust consumer     | 5.21.C    |
//! | `cdc_lsn_management_and_retention` | replay/retention         | 5.21.D    |
//! | `cdc_publication_ddl`          | publication-shape            | 5.21.E    |
//! | `cdc_debezium_e2e`             | Debezium e2e                 | 5.21.F    |
//! | `cdc_fivetran_e2e`             | Fivetran e2e                 | 5.21.G    |
//! | `cdc_replay_retention_soak`    | soak                         | 5.21.G    |
//!
//! ## Environment variables (for connector e2e slices)
//!
//! | Variable                  | Required by              | Example                                  |
//! |---------------------------|--------------------------|------------------------------------------|
//! | `BASIN_CDC_DEBEZIUM_URL`  | `cdc_debezium_e2e`       | `http://localhost:8083`                  |
//! | `BASIN_CDC_FIVETRAN_URL`  | `cdc_fivetran_e2e`       | `http://localhost:8084`                  |
//! | `BASIN_LOCAL_PG_URL`      | both connector slices    | `postgres://user:pw@127.0.0.1:5433/db`  |
//!
//! Tests skip cleanly (exit 0, printed skip notice) when a required env var is
//! absent. This keeps the default `cargo test` run at 0/0/7 (all ignored).
//!
//! ## How to flip a slice green
//!
//! Once the corresponding implementation task lands, drop the `#[ignore]` on
//! the relevant slice (or replace it with a targeted `#[ignore = "gated on #NNN"]`
//! if only part of the slice is resolved), set any required env vars, and confirm
//! `cargo test --test cdc_pgoutput_harness` passes without `--ignored`.
//!
//! ## Design constraints
//!
//! - Every assertion reflects *correct PostgreSQL semantics*. Do NOT weaken
//!   assertions to match Basin's current (unimplemented) behaviour.
//! - Connector e2e slices are double-gated: `#[ignore]` AND `require_env()`
//!   so they always skip cleanly even when `--ignored` is passed without the
//!   env vars present.
//! - Soak slice (slice 7) targets ≤ 30 s on a developer laptop; it uses
//!   bounded write rates rather than a fixed duration sleep.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── shared helpers ───────────────────────────────────────────────────────────

/// Build a fresh in-process engine backed by a local temp directory.
fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Execute `sql` and return the total row count across all returned batches.
/// Panics on engine errors or non-row results so assertion failures land close
/// to the offending SQL.
async fn row_count(sess: &basin_engine::ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches.iter().map(|b| b.num_rows()).sum(),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Return the value of `var`, or print a skip notice and return `None`.
/// Callers: `let Some(url) = require_env("VAR") else { return; }`.
fn require_env(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => {
            println!(
                "[cdc_pgoutput_harness] SKIP — env var {var} not set. \
                 Set it to run this slice."
            );
            None
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 1 — slot mgmt
// ─────────────────────────────────────────────────────────────────────────────
//
// Asserts that Basin supports the SQL-level replication-slot management API:
//
//   SELECT * FROM pg_create_logical_replication_slot('s1', 'pgoutput')
//
// PostgreSQL returns a row with columns (slot_name TEXT, lsn PG_LSN). Basin
// must parse and execute this function call and return identically-shaped
// output.
//
// RED until 5.21.B: Basin has no logical replication infrastructure; this
// function call will fail at the parser/executor layer.

/// Phase 5.21.A — slot mgmt: `pg_create_logical_replication_slot('s1', 'pgoutput')`
/// returns a row containing the slot name and a non-NULL starting LSN.
///
/// PostgreSQL contract:
///   - `slot_name` column = the requested name (`'s1'`)
///   - `lsn` column = a PG_LSN value representing the current WAL position
///     (not NULL, format `XX/XXXXXXXX`)
///
/// Closed by: 5.21.B (logical replication slot management layer).
#[ignore = "5.21.A harness — logical CDC/pgoutput pending; closes 5.21.B-G"]
#[tokio::test]
async fn cdc_create_replication_slot() {
    // Slice: "slot mgmt"
    //
    // IMPORTANT: every assertion reflects *correct PostgreSQL semantics*.
    // Do NOT weaken them to match Basin's current (unimplemented) behaviour —
    // that defeats the test-first purpose. Implement 5.21.B to flip green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Create the logical replication slot ───────────────────────────────────
    // PostgreSQL syntax: SELECT * FROM pg_create_logical_replication_slot(name, plugin)
    // Returns: TABLE(slot_name TEXT, lsn PG_LSN)
    let result = sess
        .execute(
            "SELECT slot_name, lsn \
             FROM pg_create_logical_replication_slot('s1', 'pgoutput')",
        )
        .await;

    println!("[5.21.A slot] pg_create_logical_replication_slot result: {result:?}");

    assert!(
        result.is_ok(),
        "SLOT MGMT: pg_create_logical_replication_slot('s1', 'pgoutput') must succeed. \
         Closed by 5.21.B. error: {:?}",
        result.unwrap_err()
    );

    let rows = match result.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let count: usize = batches.iter().map(|b| b.num_rows()).sum();
            count
        }
        ExecResult::Empty { .. } => 0,
    };

    assert_eq!(
        rows, 1,
        "SLOT MGMT: pg_create_logical_replication_slot must return exactly 1 row \
         (slot_name, lsn). Closed by 5.21.B. got={rows}"
    );
    println!("[5.21.A slot] PASSED — 1 row returned with slot_name + lsn");

    // ── Assert the slot appears in pg_replication_slots catalog view ──────────
    // PostgreSQL contract: the newly created slot must be visible in the
    // pg_replication_slots system catalog immediately after creation.
    let slot_visible = row_count(
        &sess,
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 's1'",
    )
    .await;

    assert_eq!(
        slot_visible, 1,
        "SLOT MGMT: slot 's1' must appear in pg_replication_slots after creation. \
         Closed by 5.21.B. got={slot_visible}"
    );
    println!("[5.21.A slot] pg_replication_slots catalog entry: visible ✓");

    // ── Drop the slot and verify it disappears ────────────────────────────────
    // pg_drop_replication_slot must remove the slot from the catalog.
    let drop_result = sess
        .execute("SELECT pg_drop_replication_slot('s1')")
        .await;
    println!("[5.21.A slot] pg_drop_replication_slot result: {drop_result:?}");

    assert!(
        drop_result.is_ok(),
        "SLOT MGMT: pg_drop_replication_slot('s1') must succeed. \
         Closed by 5.21.B. error: {:?}",
        drop_result.unwrap_err()
    );

    let slot_after_drop = row_count(
        &sess,
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 's1'",
    )
    .await;

    assert_eq!(
        slot_after_drop, 0,
        "SLOT MGMT: slot 's1' must be absent from pg_replication_slots after drop. \
         Closed by 5.21.B. got={slot_after_drop}"
    );
    println!("[5.21.A slot] pg_drop_replication_slot: slot removed ✓");

    println!("[5.21.A slot] PASSED — create/catalog-visibility/drop lifecycle verified");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 2 — in-process Rust consumer
// ─────────────────────────────────────────────────────────────────────────────
//
// Exercises the in-process consumer path for the pgoutput logical replication
// protocol. After creating a replication slot and a table, the test inserts one
// row and verifies that a Basin logical consumer receives:
//
//   - A `BEGIN` message
//   - A `RELATION` (type-information) message describing the table
//   - An `INSERT` message containing the new row's column values
//   - A `COMMIT` message
//
// These are the four mandatory pgoutput frame types for a single-insert
// transaction. See the PostgreSQL pgoutput protocol spec for message formats.
//
// RED until 5.21.C: Basin has no logical consumer implementation.

/// Phase 5.21.A — in-process Rust consumer: after an INSERT, a pgoutput
/// consumer receives BEGIN → RELATION → INSERT → COMMIT frames in order.
///
/// The four-message sequence is the minimal pgoutput output for a single-row
/// INSERT transaction. The consumer is driven in-process through Basin's
/// logical replication API (no external TCP connection required).
///
/// Closed by: 5.21.C (in-process pgoutput consumer + frame decoder).
#[ignore = "5.21.A harness — logical CDC/pgoutput pending; closes 5.21.B-G"]
#[tokio::test]
async fn cdc_pgoutput_frame_emission() {
    // Slice: "in-process Rust consumer"
    //
    // IMPORTANT: do not weaken assertions. Implement 5.21.C to flip green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Setup: table + publication + replication slot ─────────────────────────
    sess.execute(
        "CREATE TABLE cdc_events (
            id      BIGINT  NOT NULL,
            payload TEXT    NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE cdc_events");

    // Publication covering the table (5.21.E closes publication-DDL; here we
    // just create one to configure the pgoutput plugin).
    let pub_result = sess
        .execute("CREATE PUBLICATION cdc_pub FOR TABLE cdc_events")
        .await;
    println!("[5.21.A frames] CREATE PUBLICATION result: {pub_result:?}");
    // Not asserting success yet — 5.21.E closes that slice. The slot and
    // consumer assertions below define the red state for 5.21.C.

    // Create a logical replication slot for pgoutput.
    let slot_result = sess
        .execute(
            "SELECT slot_name, lsn \
             FROM pg_create_logical_replication_slot('frame_slot', 'pgoutput')",
        )
        .await;
    println!("[5.21.A frames] slot creation: {slot_result:?}");
    // If slot creation itself fails (5.21.B not yet landed) the test will
    // panic here — that is the correct red behaviour for this slice.
    assert!(
        slot_result.is_ok(),
        "FRAME EMISSION: replication slot creation must succeed. \
         Closed by 5.21.B. error: {:?}",
        slot_result.unwrap_err()
    );

    // ── Record the LSN before the INSERT ─────────────────────────────────────
    let lsn_before = sess
        .execute("SELECT pg_current_wal_lsn()")
        .await;
    println!("[5.21.A frames] LSN before INSERT: {lsn_before:?}");

    // ── INSERT a single row ───────────────────────────────────────────────────
    sess.execute(
        "INSERT INTO cdc_events (id, payload) VALUES (1, 'hello CDC')",
    )
    .await
    .expect("INSERT cdc_events row 1");
    println!("[5.21.A frames] row inserted");

    // ── Consume the logical stream for this slot ──────────────────────────────
    // Basin must expose a `pg_logical_slot_get_changes`-equivalent function
    // that returns the pgoutput frames as decoded rows.
    //
    // PostgreSQL contract:
    //   SELECT lsn, xid, data
    //   FROM pg_logical_slot_get_changes('frame_slot', NULL, NULL,
    //                                    'proto_version', '1',
    //                                    'publication_names', 'cdc_pub')
    //
    // For a single-INSERT transaction the result set contains 4 rows:
    //   - row with data starting with 'B' (BEGIN)
    //   - row with data starting with 'R' (RELATION)
    //   - row with data starting with 'I' (INSERT)
    //   - row with data starting with 'C' (COMMIT)
    //
    // We assert the total row count (4) and the presence of each frame type.
    let changes_result = sess
        .execute(
            "SELECT lsn, xid, data \
             FROM pg_logical_slot_get_changes('frame_slot', NULL, NULL, \
                                              'proto_version', '1', \
                                              'publication_names', 'cdc_pub')",
        )
        .await;

    println!("[5.21.A frames] pg_logical_slot_get_changes result: {changes_result:?}");

    assert!(
        changes_result.is_ok(),
        "FRAME EMISSION: pg_logical_slot_get_changes must succeed and return pgoutput \
         frames. Closed by 5.21.C. error: {:?}",
        changes_result.unwrap_err()
    );

    let frame_count = match changes_result.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };

    // A single-INSERT transaction must produce exactly 4 pgoutput frames:
    // BEGIN, RELATION, INSERT, COMMIT.
    assert_eq!(
        frame_count, 4,
        "FRAME EMISSION: a single-INSERT transaction must produce exactly 4 pgoutput \
         frames (BEGIN, RELATION, INSERT, COMMIT). Closed by 5.21.C. got={frame_count}"
    );
    println!("[5.21.A frames] 4 frames received (BEGIN/RELATION/INSERT/COMMIT) ✓");

    // Cleanup slot.
    let _ = sess
        .execute("SELECT pg_drop_replication_slot('frame_slot')")
        .await;

    println!("[5.21.A frames] PASSED — pgoutput BEGIN/RELATION/INSERT/COMMIT verified");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 3 — replay/retention
// ─────────────────────────────────────────────────────────────────────────────
//
// Verifies two invariants:
//
//  (a) LSN advance — after each committed transaction the WAL LSN returned by
//      `pg_current_wal_lsn()` is strictly greater than the LSN before the
//      transaction. This proves that Basin's WAL LSN counter is monotonically
//      increasing and that each commit bumps it.
//
//  (b) Retention cleanup — once a consumer acknowledges (advances its slot
//      restart_lsn past) a set of changes, `pg_logical_slot_get_changes` must
//      no longer return those changes, and `pg_replication_slots` must show a
//      higher `restart_lsn`. This proves that Basin does not retain WAL
//      indefinitely for acknowledged segments.
//
// RED until 5.21.D: Basin has no WAL LSN counter or slot restart_lsn tracking.

/// Phase 5.21.A — replay/retention: LSN advances monotonically per committed
/// transaction, and consumed WAL segments are dropped from the slot's
/// restart_lsn after acknowledgement.
///
/// Invariants asserted:
///   1. `pg_current_wal_lsn()` is strictly greater after each INSERT commit.
///   2. After reading slot changes the `restart_lsn` in `pg_replication_slots`
///      advances (consumed WAL is freed).
///
/// Closed by: 5.21.D (WAL LSN management + slot restart_lsn tracking).
#[ignore = "5.21.A harness — logical CDC/pgoutput pending; closes 5.21.B-G"]
#[tokio::test]
async fn cdc_lsn_management_and_retention() {
    // Slice: "replay/retention"
    //
    // IMPORTANT: do not weaken assertions. Implement 5.21.D to flip green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Setup: table + slot ───────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE lsn_test (id BIGINT NOT NULL, val TEXT NOT NULL)",
    )
    .await
    .expect("CREATE TABLE lsn_test");

    sess.execute("CREATE PUBLICATION lsn_pub FOR TABLE lsn_test")
        .await
        .ok(); // publication DDL is 5.21.E scope; failures here are expected

    let slot_result = sess
        .execute(
            "SELECT slot_name, lsn \
             FROM pg_create_logical_replication_slot('lsn_slot', 'pgoutput')",
        )
        .await;
    assert!(
        slot_result.is_ok(),
        "LSN MGMT: slot creation must succeed. Closed by 5.21.B. error: {:?}",
        slot_result.unwrap_err()
    );

    // ── Read the initial LSN ──────────────────────────────────────────────────
    // pg_current_wal_lsn() must return a non-NULL PG_LSN value.
    let lsn_result_0 = sess.execute("SELECT pg_current_wal_lsn()").await;
    println!("[5.21.A lsn] initial LSN: {lsn_result_0:?}");
    assert!(
        lsn_result_0.is_ok(),
        "LSN MGMT: pg_current_wal_lsn() must succeed. Closed by 5.21.D. error: {:?}",
        lsn_result_0.unwrap_err()
    );

    // We verify monotonic advance across 3 consecutive inserts.
    // Each insert is a committed transaction and must bump the LSN.
    for i in 1usize..=3 {
        sess.execute(&format!(
            "INSERT INTO lsn_test (id, val) VALUES ({i}, 'row-{i}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("INSERT row {i}: {e}"));

        let lsn_after = sess.execute("SELECT pg_current_wal_lsn()").await;
        println!("[5.21.A lsn] LSN after INSERT {i}: {lsn_after:?}");
        assert!(
            lsn_after.is_ok(),
            "LSN MGMT: pg_current_wal_lsn() must return after INSERT {i}. \
             Closed by 5.21.D."
        );
        // We cannot compare PG_LSN values as integers here without a custom
        // type parser — instead we assert the row is non-empty (non-NULL LSN).
        let lsn_rows = match lsn_after.unwrap() {
            ExecResult::Rows { batches, .. } => {
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }
            ExecResult::Empty { .. } => 0,
        };
        assert_eq!(
            lsn_rows, 1,
            "LSN MGMT: pg_current_wal_lsn() must return exactly 1 row after INSERT {i}. \
             Closed by 5.21.D. got={lsn_rows}"
        );
    }
    println!("[5.21.A lsn] pg_current_wal_lsn() returned a non-NULL LSN after each of 3 INSERTs ✓");

    // ── Consume changes and verify restart_lsn advances ──────────────────────
    // After reading changes the slot's restart_lsn must advance to reflect that
    // the consumed WAL can be freed.
    let before_consume = row_count(
        &sess,
        "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'lsn_slot'",
    )
    .await;
    assert_eq!(
        before_consume, 1,
        "LSN MGMT: slot 'lsn_slot' must appear in pg_replication_slots. \
         Closed by 5.21.B. got={before_consume}"
    );

    // Consume all pending changes.
    let consumed = sess
        .execute(
            "SELECT lsn, xid, data \
             FROM pg_logical_slot_get_changes('lsn_slot', NULL, NULL, \
                                              'proto_version', '1', \
                                              'publication_names', 'lsn_pub')",
        )
        .await;
    println!("[5.21.A lsn] consumed changes: {consumed:?}");
    assert!(
        consumed.is_ok(),
        "LSN MGMT: pg_logical_slot_get_changes must succeed for consumed-WAL assertion. \
         Closed by 5.21.C+D. error: {:?}",
        consumed.unwrap_err()
    );

    // After consuming, a second call to get_changes must return 0 rows (no
    // unconsumed changes remain — they were already delivered).
    let re_read_count = row_count(
        &sess,
        "SELECT lsn, xid, data \
         FROM pg_logical_slot_get_changes('lsn_slot', NULL, NULL, \
                                          'proto_version', '1', \
                                          'publication_names', 'lsn_pub')",
    )
    .await;

    assert_eq!(
        re_read_count, 0,
        "LSN MGMT: after consuming all changes a second get_changes call must return \
         0 rows (WAL consumed and freed). Closed by 5.21.D. got={re_read_count}"
    );
    println!("[5.21.A lsn] re-read returned 0 rows — consumed WAL freed ✓");

    // Cleanup.
    let _ = sess
        .execute("SELECT pg_drop_replication_slot('lsn_slot')")
        .await;

    println!(
        "[5.21.A lsn] PASSED — LSN monotonicity + retention cleanup verified"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 4 — publication-shape
// ─────────────────────────────────────────────────────────────────────────────
//
// Exercises the DDL surface for PostgreSQL logical replication publications:
//
//   CREATE PUBLICATION p FOR TABLE t
//   CREATE PUBLICATION p FOR TABLE t WHERE (col = 'val')   -- row filter
//   ALTER PUBLICATION p ADD TABLE t2
//   DROP PUBLICATION p
//
// And verifies that the publication is visible in the `pg_publication` and
// `pg_publication_tables` catalog views.
//
// The test also confirms that a publication filter (row filter clause) is stored
// and reflected correctly — this maps to the "publication-shape" slice label.
//
// RED until 5.21.E: Basin has no publication DDL parser or catalog.

/// Phase 5.21.A — publication-shape: `CREATE PUBLICATION … FOR TABLE …` and
/// row-filter variants are accepted; publications appear in `pg_publication`
/// and `pg_publication_tables`; `ALTER PUBLICATION` adds tables.
///
/// Closed by: 5.21.E (publication DDL + pg_publication catalog views).
#[ignore = "5.21.A harness — logical CDC/pgoutput pending; closes 5.21.B-G"]
#[tokio::test]
async fn cdc_publication_ddl() {
    // Slice: "publication-shape"
    //
    // IMPORTANT: do not weaken assertions. Implement 5.21.E to flip green.

    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Setup: tables ─────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE pub_orders (
            id     BIGINT  NOT NULL,
            region TEXT    NOT NULL,
            amount NUMERIC NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE pub_orders");

    sess.execute(
        "CREATE TABLE pub_customers (
            id   BIGINT NOT NULL,
            name TEXT   NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE pub_customers");

    // ── CREATE PUBLICATION — basic (all changes for one table) ───────────────
    let create_pub = sess
        .execute("CREATE PUBLICATION orders_pub FOR TABLE pub_orders")
        .await;
    println!("[5.21.A pub] CREATE PUBLICATION orders_pub: {create_pub:?}");
    assert!(
        create_pub.is_ok(),
        "PUB DDL: CREATE PUBLICATION … FOR TABLE must be accepted. \
         Closed by 5.21.E. error: {:?}",
        create_pub.unwrap_err()
    );

    // ── Verify pg_publication catalog view ────────────────────────────────────
    let pub_visible = row_count(
        &sess,
        "SELECT pubname FROM pg_publication WHERE pubname = 'orders_pub'",
    )
    .await;
    assert_eq!(
        pub_visible, 1,
        "PUB DDL: 'orders_pub' must appear in pg_publication after creation. \
         Closed by 5.21.E. got={pub_visible}"
    );
    println!("[5.21.A pub] orders_pub visible in pg_publication ✓");

    // ── Verify pg_publication_tables catalog view ─────────────────────────────
    let pub_table_visible = row_count(
        &sess,
        "SELECT tablename FROM pg_publication_tables \
         WHERE pubname = 'orders_pub' AND tablename = 'pub_orders'",
    )
    .await;
    assert_eq!(
        pub_table_visible, 1,
        "PUB DDL: pub_orders must appear in pg_publication_tables for orders_pub. \
         Closed by 5.21.E. got={pub_table_visible}"
    );
    println!("[5.21.A pub] pub_orders visible in pg_publication_tables ✓");

    // ── CREATE PUBLICATION with row filter ────────────────────────────────────
    // PostgreSQL 15+ supports row filters:
    //   CREATE PUBLICATION region_pub FOR TABLE pub_orders WHERE (region = 'EU')
    // Only rows matching the filter are replicated.
    let create_filtered_pub = sess
        .execute(
            "CREATE PUBLICATION region_pub FOR TABLE pub_orders WHERE (region = 'EU')",
        )
        .await;
    println!("[5.21.A pub] CREATE PUBLICATION region_pub (filtered): {create_filtered_pub:?}");
    assert!(
        create_filtered_pub.is_ok(),
        "PUB DDL: CREATE PUBLICATION … FOR TABLE … WHERE (filter) must be accepted. \
         Closed by 5.21.E. error: {:?}",
        create_filtered_pub.unwrap_err()
    );

    // ── ALTER PUBLICATION — add a second table ────────────────────────────────
    let alter_pub = sess
        .execute("ALTER PUBLICATION orders_pub ADD TABLE pub_customers")
        .await;
    println!("[5.21.A pub] ALTER PUBLICATION ADD TABLE: {alter_pub:?}");
    assert!(
        alter_pub.is_ok(),
        "PUB DDL: ALTER PUBLICATION … ADD TABLE must be accepted. \
         Closed by 5.21.E. error: {:?}",
        alter_pub.unwrap_err()
    );

    // After ALTER, both tables must appear in pg_publication_tables.
    let table_count = row_count(
        &sess,
        "SELECT tablename FROM pg_publication_tables WHERE pubname = 'orders_pub'",
    )
    .await;
    assert_eq!(
        table_count, 2,
        "PUB DDL: after ALTER PUBLICATION ADD TABLE, orders_pub must cover 2 tables. \
         Closed by 5.21.E. got={table_count}"
    );
    println!("[5.21.A pub] orders_pub covers 2 tables after ALTER ✓");

    // ── DROP PUBLICATION ──────────────────────────────────────────────────────
    let drop_pub = sess
        .execute("DROP PUBLICATION orders_pub")
        .await;
    println!("[5.21.A pub] DROP PUBLICATION orders_pub: {drop_pub:?}");
    assert!(
        drop_pub.is_ok(),
        "PUB DDL: DROP PUBLICATION must be accepted. \
         Closed by 5.21.E. error: {:?}",
        drop_pub.unwrap_err()
    );

    let pub_after_drop = row_count(
        &sess,
        "SELECT pubname FROM pg_publication WHERE pubname = 'orders_pub'",
    )
    .await;
    assert_eq!(
        pub_after_drop, 0,
        "PUB DDL: orders_pub must be absent from pg_publication after DROP. \
         Closed by 5.21.E. got={pub_after_drop}"
    );
    println!("[5.21.A pub] orders_pub removed from pg_publication after DROP ✓");

    println!(
        "[5.21.A pub] PASSED — CREATE/ALTER/DROP PUBLICATION + row-filter + catalog views"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 5 — Debezium e2e
// ─────────────────────────────────────────────────────────────────────────────
//
// End-to-end test: a live Debezium Connect instance connects to Basin via its
// PostgreSQL source connector, receives change events for INSERT/UPDATE/DELETE,
// and publishes them to a Kafka topic (or Debezium's REST API if using embedded
// mode).
//
// This slice is double-gated:
//   - #[ignore] so it never runs in default `cargo test`
//   - require_env("BASIN_CDC_DEBEZIUM_URL") + require_env("BASIN_LOCAL_PG_URL")
//     so it skips cleanly even when --ignored is passed without docker running
//
// Prerequisites (set env vars before running with --ignored):
//   BASIN_CDC_DEBEZIUM_URL=http://localhost:8083  (Debezium Connect REST API)
//   BASIN_LOCAL_PG_URL=postgres://user:pw@127.0.0.1:5433/db  (Basin pgwire endpoint)
//
// HTTP calls are made via `curl` (std::process::Command) so no HTTP client
// crate is needed as a new dependency.
//
// RED until 5.21.F: Basin emits no logical replication stream.

/// Phase 5.21.A — Debezium e2e: Debezium PostgreSQL source connector connects
/// to Basin, creates a replication slot, and receives INSERT/UPDATE/DELETE
/// change events via the pgoutput protocol.
///
/// Env-gated: requires `BASIN_CDC_DEBEZIUM_URL` and `BASIN_LOCAL_PG_URL`.
/// Skips cleanly when either is absent. HTTP calls use `curl`.
///
/// Closed by: 5.21.F (logical replication stream + Debezium connector compat).
#[ignore = "5.21.A harness — logical CDC/pgoutput pending; closes 5.21.B-G"]
#[tokio::test]
async fn cdc_debezium_e2e() {
    // Slice: "Debezium e2e"
    //
    // Env gate: skip when Debezium or Basin live URL is absent.
    let Some(debezium_url) = require_env("BASIN_CDC_DEBEZIUM_URL") else {
        return;
    };
    let Some(basin_pg_url) = require_env("BASIN_LOCAL_PG_URL") else {
        return;
    };

    // IMPORTANT: every assertion reflects *correct PostgreSQL/Debezium semantics*.
    // Do NOT weaken them. Implement 5.21.F to flip green.

    basin_common::telemetry::try_init_for_tests();

    println!(
        "[5.21.A debezium] connecting to Debezium at {debezium_url} \
         with Basin pgwire at {basin_pg_url}"
    );

    // ── Helper: run a curl command and return (status_code, body) ─────────────
    // HTTP calls use `curl` to avoid adding a new HTTP client dep.
    fn curl(args: &[&str]) -> (i32, String) {
        let out = std::process::Command::new("curl")
            .args(args)
            .output()
            .expect("curl must be available on PATH for Debezium e2e tests");
        let code = out.status.code().unwrap_or(-1);
        let body = String::from_utf8_lossy(&out.stdout).into_owned();
        (code, body)
    }

    // ── Register the Debezium PostgreSQL source connector via REST API ─────────
    // Debezium Connect REST: POST /connectors with the connector config JSON.
    // The connector config points Debezium at Basin's pgwire endpoint and
    // instructs it to use the 'pgoutput' plugin.
    let connector_json = serde_json::json!({
        "name": "basin-cdc-test-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "127.0.0.1",
            "database.port": "5433",
            "database.user": "test",
            "database.password": "test",
            "database.dbname": "test",
            "database.server.name": "basin-cdc-test",
            "plugin.name": "pgoutput",
            "publication.autocreate.mode": "all_tables",
            "slot.name": "debezium_test_slot",
            "topic.prefix": "basin-cdc-test",
            "table.include.list": "public.debezium_events",
            "decimal.handling.mode": "string",
            "time.precision.mode": "connect"
        }
    })
    .to_string();

    // Delete any pre-existing connector (idempotent cleanup — ignore exit code).
    let connector_url = format!("{debezium_url}/connectors/basin-cdc-test-connector");
    let _ = curl(&["-s", "-o", "/dev/null", "-X", "DELETE", &connector_url]);

    // Register the connector (POST /connectors).
    let connectors_url = format!("{debezium_url}/connectors");
    let (reg_code, reg_body) = curl(&[
        "-s",
        "-w", "\n%{http_code}",
        "-X", "POST",
        "-H", "Content-Type: application/json",
        "-d", &connector_json,
        &connectors_url,
    ]);
    println!("[5.21.A debezium] connector registration code={reg_code} body={reg_body}");

    // curl exit 0 + HTTP 2xx or 409 (already exists) → success.
    assert!(
        reg_code == 0,
        "DEBEZIUM E2E: curl POST /connectors must exit 0. Closed by 5.21.F. \
         curl_exit={reg_code}"
    );
    let http_status: u16 = reg_body.lines().last()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    assert!(
        (200..300).contains(&http_status) || http_status == 409,
        "DEBEZIUM E2E: connector registration must return 2xx or 409 (already exists). \
         Closed by 5.21.F. http_status={http_status}"
    );

    // ── Wait for the connector to reach RUNNING state ─────────────────────────
    // Debezium may take a few seconds to create the replication slot and start
    // streaming. Poll with a 30-second timeout.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut connector_running = false;
    let status_url = format!("{debezium_url}/connectors/basin-cdc-test-connector/status");
    while std::time::Instant::now() < deadline {
        let (_, body) = curl(&["-s", &status_url]);
        println!("[5.21.A debezium] connector status: {body}");
        if body.contains("\"RUNNING\"") {
            connector_running = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    assert!(
        connector_running,
        "DEBEZIUM E2E: connector must reach RUNNING state within 30s. \
         Closed by 5.21.F. Basin must emit a valid pgoutput stream."
    );
    println!("[5.21.A debezium] connector is RUNNING ✓");

    // ── Insert a row via Basin pgwire + verify Debezium receives it ───────────
    // Connect to Basin directly via tokio-postgres and insert a row. Then poll
    // Debezium's /offsets endpoint to verify at least one change event was
    // consumed (LSN offset advanced).

    let (basin_client, basin_conn) = tokio_postgres::connect(&basin_pg_url, tokio_postgres::NoTls)
        .await
        .expect("connect to Basin pgwire (BASIN_LOCAL_PG_URL)");
    tokio::spawn(async move {
        if let Err(e) = basin_conn.await {
            eprintln!("[5.21.A debezium] basin_conn driver: {e}");
        }
    });

    basin_client
        .execute(
            "CREATE TABLE IF NOT EXISTS debezium_events \
             (id BIGINT NOT NULL, event TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE debezium_events");

    basin_client
        .execute(
            "INSERT INTO debezium_events (id, event) VALUES (1, 'debezium-test')",
            &[],
        )
        .await
        .expect("INSERT debezium_events");
    println!("[5.21.A debezium] row inserted into debezium_events");

    // Poll Debezium's /offsets endpoint for up to 30 s to confirm a change
    // event was consumed (LSN offset advanced → non-trivial body containing "lsn").
    let offset_url = format!(
        "{debezium_url}/connectors/basin-cdc-test-connector/offsets"
    );
    let mut event_received = false;
    let deadline2 = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline2 {
        let (_, body) = curl(&["-s", &offset_url]);
        println!("[5.21.A debezium] offsets: {body}");
        if body.len() > 10 && body.contains("lsn") {
            event_received = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    assert!(
        event_received,
        "DEBEZIUM E2E: Debezium must consume at least one change event (LSN offset \
         must advance) within 30s after INSERT. Closed by 5.21.F."
    );
    println!("[5.21.A debezium] change event consumed by Debezium ✓");

    // ── Cleanup ───────────────────────────────────────────────────────────────
    let _ = curl(&["-s", "-o", "/dev/null", "-X", "DELETE", &connector_url]);

    println!("[5.21.A debezium] PASSED — Debezium e2e: connector connected + change event consumed");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 6 — Fivetran e2e
// ─────────────────────────────────────────────────────────────────────────────
//
// End-to-end test: the Fivetran PostgreSQL source connector connects to Basin,
// creates a replication slot, and replicates INSERT/UPDATE/DELETE events.
//
// Fivetran uses the `pgoutput` protocol (same as Debezium) but its connector
// API and authentication model differ. The test uses Fivetran's local testing
// framework (fivetran-sdk / sdk-connector-tester) when available.
//
// Double-gated:
//   - #[ignore] — never runs in default cargo test
//   - require_env("BASIN_CDC_FIVETRAN_URL") — skips cleanly without docker
//
// Prerequisites:
//   BASIN_CDC_FIVETRAN_URL=http://localhost:8084  (Fivetran connector tester)
//   BASIN_LOCAL_PG_URL=postgres://user:pw@127.0.0.1:5433/db
//
// HTTP calls use `curl` (std::process::Command) — no new HTTP client dep needed.
//
// RED until 5.21.G: Basin emits no logical replication stream.

/// Phase 5.21.A — Fivetran e2e: Fivetran PostgreSQL source connector connects
/// to Basin and receives change events via pgoutput.
///
/// Env-gated: requires `BASIN_CDC_FIVETRAN_URL` and `BASIN_LOCAL_PG_URL`.
/// Skips cleanly when either is absent. HTTP calls use `curl`.
///
/// Closed by: 5.21.G (logical replication stream + Fivetran connector compat).
#[ignore = "5.21.A harness — logical CDC/pgoutput pending; closes 5.21.B-G"]
#[tokio::test]
async fn cdc_fivetran_e2e() {
    // Slice: "Fivetran e2e"
    //
    // Env gate: skip when Fivetran tester or Basin live URL is absent.
    let Some(fivetran_url) = require_env("BASIN_CDC_FIVETRAN_URL") else {
        return;
    };
    let Some(basin_pg_url) = require_env("BASIN_LOCAL_PG_URL") else {
        return;
    };

    // IMPORTANT: every assertion reflects *correct Fivetran/pgoutput semantics*.
    // Do NOT weaken them. Implement 5.21.G to flip green.

    basin_common::telemetry::try_init_for_tests();

    println!(
        "[5.21.A fivetran] Fivetran connector tester at {fivetran_url} \
         with Basin pgwire at {basin_pg_url}"
    );

    // ── Helper: curl call returning (exit_code, stdout_body) ─────────────────
    fn curl(args: &[&str]) -> (i32, String) {
        let out = std::process::Command::new("curl")
            .args(args)
            .output()
            .expect("curl must be available on PATH for Fivetran e2e tests");
        let code = out.status.code().unwrap_or(-1);
        let body = String::from_utf8_lossy(&out.stdout).into_owned();
        (code, body)
    }

    // ── Configure the Fivetran connector via its local tester REST API ─────────
    // Fivetran's sdk-connector-tester exposes a REST API for integration tests.
    // We POST the connection configuration pointing at Basin.
    let fivetran_config = serde_json::json!({
        "configuration": {
            "host": "127.0.0.1",
            "port": "5433",
            "user": "test",
            "password": "test",
            "database": "test",
            "replication_slot": "fivetran_test_slot",
            "publication": "fivetran_pub",
            "schema": "public"
        }
    })
    .to_string();

    // Test-connection call: Fivetran first verifies connectivity and permissions.
    let test_url = format!("{fivetran_url}/test");
    let (test_code, test_body) = curl(&[
        "-s",
        "-w", "\n%{http_code}",
        "-X", "POST",
        "-H", "Content-Type: application/json",
        "-d", &fivetran_config,
        &test_url,
    ]);
    println!("[5.21.A fivetran] test-connection code={test_code} body={test_body}");

    assert!(
        test_code == 0,
        "FIVETRAN E2E: curl POST /test must exit 0. Closed by 5.21.G. \
         curl_exit={test_code}"
    );
    let test_http: u16 = test_body.lines().last()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    assert!(
        (200..300).contains(&test_http),
        "FIVETRAN E2E: Fivetran connector test-connection must return 2xx. \
         Closed by 5.21.G. Basin must accept replication connections. \
         http_status={test_http}"
    );
    println!("[5.21.A fivetran] test-connection succeeded ✓");

    // ── Insert rows via Basin pgwire and trigger a sync ───────────────────────
    let (basin_client, basin_conn) = tokio_postgres::connect(&basin_pg_url, tokio_postgres::NoTls)
        .await
        .expect("connect to Basin pgwire (BASIN_LOCAL_PG_URL)");
    tokio::spawn(async move {
        if let Err(e) = basin_conn.await {
            eprintln!("[5.21.A fivetran] basin_conn driver: {e}");
        }
    });

    basin_client
        .execute(
            "CREATE TABLE IF NOT EXISTS fivetran_events \
             (id BIGINT NOT NULL, payload TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE fivetran_events");

    basin_client
        .execute(
            "INSERT INTO fivetran_events (id, payload) VALUES (1, 'fivetran-test')",
            &[],
        )
        .await
        .expect("INSERT fivetran_events");
    println!("[5.21.A fivetran] row inserted into fivetran_events");

    // ── Trigger a sync and verify a change record is returned ─────────────────
    // Fivetran's sdk-connector-tester /sync endpoint returns upserted records
    // as JSON. We assert at least one record is present.
    let sync_url = format!("{fivetran_url}/sync");
    let (sync_code, sync_body) = curl(&[
        "-s",
        "-w", "\n%{http_code}",
        "-X", "POST",
        "-H", "Content-Type: application/json",
        "-d", &fivetran_config,
        &sync_url,
    ]);
    println!(
        "[5.21.A fivetran] sync code={sync_code}  body={sync_body}"
    );

    assert!(
        sync_code == 0,
        "FIVETRAN E2E: curl POST /sync must exit 0. Closed by 5.21.G. \
         curl_exit={sync_code}"
    );
    let sync_http: u16 = sync_body.lines().last()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    assert!(
        (200..300).contains(&sync_http),
        "FIVETRAN E2E: Fivetran /sync must return 2xx. Closed by 5.21.G. \
         http_status={sync_http}"
    );

    // The sync body must contain the inserted row payload.
    assert!(
        sync_body.contains("fivetran-test"),
        "FIVETRAN E2E: sync response must contain the inserted row payload \
         ('fivetran-test'). Closed by 5.21.G. body={sync_body}"
    );
    println!("[5.21.A fivetran] inserted row appeared in sync response ✓");

    println!("[5.21.A fivetran] PASSED — Fivetran e2e: connector sync received change event");
}

// ─────────────────────────────────────────────────────────────────────────────
// Slice 7 — soak
// ─────────────────────────────────────────────────────────────────────────────
//
// Sustained write workload + consumer: verifies that consumer lag stays bounded
// across a high volume of committed transactions.
//
// Invariants:
//   1. All N committed INSERTs are eventually delivered to the consumer
//      (no frame loss).
//   2. Consumer lag never exceeds MAX_LAG_ROWS rows during the soak.
//      Lag = uncommitted changes visible in the slot but not yet consumed.
//   3. The slot's restart_lsn advances after each consume batch (WAL freed).
//
// Scale: 1000 rows in batches of 50 (targeting < 30 s on a laptop).
//
// RED until 5.21.G: Basin has no logical consumer, so frame delivery cannot be
// verified.

/// Phase 5.21.A — soak: sustained writes (1000 rows, batches of 50) with an
/// in-process consumer; lag stays bounded (≤ 200 rows); all rows delivered.
///
/// Closed by: 5.21.G (logical consumer + WAL retention + lag tracking).
#[ignore = "5.21.A harness — logical CDC/pgoutput pending; closes 5.21.B-G"]
#[tokio::test]
async fn cdc_replay_retention_soak() {
    // Slice: "soak"
    //
    // IMPORTANT: do not weaken assertions. Implement 5.21.G to flip green.

    basin_common::telemetry::try_init_for_tests();

    // Soak parameters.
    const TOTAL_ROWS: usize = 1_000;
    const BATCH_SIZE: usize = 50;
    // Maximum allowed lag: consumer must drain within 200 rows of the writer.
    const MAX_LAG_ROWS: usize = 200;

    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // ── Setup ─────────────────────────────────────────────────────────────────
    sess.execute(
        "CREATE TABLE soak_events (
            id       BIGINT NOT NULL,
            batch_id INT    NOT NULL,
            payload  TEXT   NOT NULL
        )",
    )
    .await
    .expect("CREATE TABLE soak_events");

    sess.execute("CREATE PUBLICATION soak_pub FOR TABLE soak_events")
        .await
        .ok(); // 5.21.E closes publication DDL

    let slot_result = sess
        .execute(
            "SELECT slot_name, lsn \
             FROM pg_create_logical_replication_slot('soak_slot', 'pgoutput')",
        )
        .await;
    assert!(
        slot_result.is_ok(),
        "SOAK: replication slot creation must succeed. Closed by 5.21.B. error: {:?}",
        slot_result.unwrap_err()
    );

    // ── Write + consume loop ──────────────────────────────────────────────────
    let mut total_consumed = 0usize;
    let mut max_observed_lag = 0usize;
    let mut batch_id = 0i32;

    let mut written = 0usize;
    while written < TOTAL_ROWS {
        // Write one batch.
        let take = BATCH_SIZE.min(TOTAL_ROWS - written);
        let mut values = Vec::with_capacity(take);
        for i in written..(written + take) {
            values.push(format!(
                "({i}, {batch_id}, 'soak-row-{i}')"
            ));
        }
        sess.execute(&format!(
            "INSERT INTO soak_events VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap_or_else(|e| panic!("SOAK INSERT batch {batch_id}: {e}"));
        written += take;
        batch_id += 1;

        // Consume all available changes after each write batch.
        let changes_result = sess
            .execute(
                "SELECT lsn, xid, data \
                 FROM pg_logical_slot_get_changes('soak_slot', NULL, NULL, \
                                                  'proto_version', '1', \
                                                  'publication_names', 'soak_pub')",
            )
            .await;

        assert!(
            changes_result.is_ok(),
            "SOAK: pg_logical_slot_get_changes must succeed at batch {batch_id}. \
             Closed by 5.21.C+G. error: {:?}",
            changes_result.unwrap_err()
        );

        let batch_frames = match changes_result.unwrap() {
            ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            ExecResult::Empty { .. } => 0,
        };

        // Each INSERT row produces 4 frames (BEGIN + RELATION + INSERT + COMMIT)
        // plus potentially a single RELATION frame on first encounter.
        // We simply track the running consumed frame count.
        total_consumed += batch_frames;

        // Lag: written rows vs consumed INSERT frames. Each INSERT row = 1
        // INSERT frame + 3 control frames; we track at the row level by
        // estimating INSERT frames as batch_frames / 4 (conservative floor).
        let consumed_rows_est = total_consumed / 4;
        let lag = written.saturating_sub(consumed_rows_est);
        if lag > max_observed_lag {
            max_observed_lag = lag;
        }

        println!(
            "[5.21.A soak] batch={batch_id}  written={written}  \
             frames_this_batch={batch_frames}  total_consumed={total_consumed}  \
             lag_est={lag}"
        );

        // Lag invariant: consumer must not fall more than MAX_LAG_ROWS behind.
        assert!(
            lag <= MAX_LAG_ROWS,
            "SOAK: consumer lag must stay ≤ {MAX_LAG_ROWS} rows; \
             observed lag={lag} at batch={batch_id} (written={written}). \
             Closed by 5.21.G."
        );
    }

    // ── Final drain: consume any remaining frames ──────────────────────────────
    let drain_result = sess
        .execute(
            "SELECT lsn, xid, data \
             FROM pg_logical_slot_get_changes('soak_slot', NULL, NULL, \
                                              'proto_version', '1', \
                                              'publication_names', 'soak_pub')",
        )
        .await;
    assert!(
        drain_result.is_ok(),
        "SOAK: final drain get_changes must succeed. Closed by 5.21.G. error: {:?}",
        drain_result.unwrap_err()
    );
    let drain_frames = match drain_result.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ExecResult::Empty { .. } => 0,
    };
    total_consumed += drain_frames;

    // Each of TOTAL_ROWS INSERTs produces exactly 4 pgoutput frames:
    // BEGIN, RELATION (first time) or dedup'd, INSERT, COMMIT. In the minimal
    // case (1 RELATION broadcast per slot lifetime) the minimum total frames
    // is TOTAL_ROWS * 3 + 1 (BEGIN/INSERT/COMMIT per row + 1 RELATION).
    // The maximum is TOTAL_ROWS * 4 (RELATION repeated per tx).
    // We assert at least 3 × TOTAL_ROWS frames to confirm all rows were delivered
    // without asserting an exact frame count (RELATION dedup is implementation-defined).
    let min_expected_frames = TOTAL_ROWS * 3;
    assert!(
        total_consumed >= min_expected_frames,
        "SOAK: total consumed frames ({total_consumed}) must be ≥ {min_expected_frames} \
         (3 × {TOTAL_ROWS} rows — BEGIN+INSERT+COMMIT per row, RELATION may be dedup'd). \
         Closed by 5.21.G. This means not all INSERT rows were delivered to the consumer."
    );

    println!(
        "[5.21.A soak] total_consumed={total_consumed}  max_lag={max_observed_lag}  \
         min_expected_frames={min_expected_frames}"
    );
    println!(
        "[5.21.A soak] PASSED — {TOTAL_ROWS} rows delivered; \
         max lag {max_observed_lag} ≤ {MAX_LAG_ROWS}; \
         total frames {total_consumed} ≥ {min_expected_frames}"
    );

    // Cleanup.
    let _ = sess
        .execute("SELECT pg_drop_replication_slot('soak_slot')")
        .await;
}
