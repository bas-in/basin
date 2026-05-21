//! Integration tests for the numeric / network / bit-string type stubs added
//! in the bulk-extra-types pass.
//!
//! Each test covers one type family with a CREATE TABLE → INSERT → SELECT
//! round-trip exercised through the engine's in-process session surface
//! (no pgwire layer needed — the Arrow storage / retrieval path is what
//! matters here).
//!
//! Types covered:
//!   - `NUMERIC(p,s)` / `DECIMAL(p,s)` → Arrow `Decimal128(p,s)` (real)
//!   - `MONEY`                          → Arrow `Decimal128(20,2)` (real)
//!   - `INET`                           → Arrow `Utf8` + `BASIN_TYPE=INET`
//!   - `CIDR`                           → Arrow `Utf8` + `BASIN_TYPE=CIDR`
//!   - `MACADDR`                        → Arrow `Utf8` + `BASIN_TYPE=MACADDR`
//!   - `MACADDR8`                       → Arrow `Utf8` + `BASIN_TYPE=MACADDR8`
//!   - `BIT(n)`                         → Arrow `Utf8` + `BASIN_TYPE=BIT(n)`
//!   - `BIT VARYING(n)` / `VARBIT(n)`   → Arrow `Utf8` + `BASIN_TYPE=VARBIT(n)`

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Decimal128Array, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── test harness ─────────────────────────────────────────────────────────────

async fn open_engine() -> (TempDir, Engine) {
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
    (dir, engine)
}

/// Execute SQL and panic on error.
async fn exec_ok(sess: &basin_engine::ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed [{sql}]: {e}"))
}

/// Execute SQL, collect all batches.
async fn rows(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match exec_ok(sess, sql).await {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => panic!("expected rows for [{sql}], got Empty tag={tag:?}"),
    }
}

// ─── NUMERIC / DECIMAL ────────────────────────────────────────────────────────

/// NUMERIC(10,2) round-trips through Arrow Decimal128(10,2).
#[tokio::test]
async fn numeric_decimal128_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE prices (id BIGINT NOT NULL, amount NUMERIC(10,2))",
    )
    .await;

    exec_ok(
        &sess,
        "INSERT INTO prices VALUES (1, 12.50), (2, -0.01), (3, 999.99)",
    )
    .await;

    let batches = rows(&sess, "SELECT id, amount FROM prices ORDER BY id").await;
    assert!(!batches.is_empty());

    // The amount column must be Decimal128(10,2).
    let schema = batches[0].schema();
    let amount_field = schema.field_with_name("amount").expect("amount column");
    assert_eq!(
        amount_field.data_type(),
        &DataType::Decimal128(10, 2),
        "NUMERIC(10,2) must map to Decimal128(10,2)"
    );

    // Collect values.
    let mut amounts: Vec<i128> = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");
        for i in 0..arr.len() {
            assert!(!arr.is_null(i));
            amounts.push(arr.value(i));
        }
    }

    // Arrow Decimal128(10,2) stores `12.50` as `1250`, `-0.01` as `-1`, `999.99` as `99999`.
    assert_eq!(amounts, vec![1250i128, -1, 99999]);
}

/// DECIMAL is a synonym for NUMERIC; verify it maps to the same Arrow type.
#[tokio::test]
async fn decimal_synonym_accepted() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(&sess, "CREATE TABLE dec_test (v DECIMAL(6,3) NOT NULL)").await;
    exec_ok(&sess, "INSERT INTO dec_test VALUES (3.141)").await;

    let batches = rows(&sess, "SELECT v FROM dec_test").await;
    let schema = batches[0].schema();
    let field = schema.field_with_name("v").unwrap();
    assert_eq!(field.data_type(), &DataType::Decimal128(6, 3));
}

// ─── MONEY ────────────────────────────────────────────────────────────────────

/// MONEY is stored as Decimal128(20,2) with BASIN_TYPE=MONEY metadata.
///
/// Unblocked by vortex 0.71 which preserves Arrow field-level metadata
/// through the DataFusion read path (ADR-0024 BASIN_TYPE sidecar fix).
#[tokio::test]
async fn money_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE wallet (id BIGINT NOT NULL, balance MONEY)",
    )
    .await;

    // Inspect the schema through the engine's execute path.
    exec_ok(&sess, "INSERT INTO wallet VALUES (1, 9.99), (2, -3.50)").await;

    let batches = rows(&sess, "SELECT id, balance FROM wallet ORDER BY id").await;
    assert!(!batches.is_empty());

    let schema0 = batches[0].schema();
    let field = schema0.field_with_name("balance").unwrap();
    assert_eq!(
        field.data_type(),
        &DataType::Decimal128(20, 2),
        "MONEY must map to Decimal128(20,2)"
    );
    assert_eq!(
        field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("MONEY"),
        "MONEY field must carry BASIN_TYPE=MONEY marker"
    );

    let arr = batches[0]
        .column_by_name("balance")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();

    // 9.99 → 999, -3.50 → -350 at scale=2.
    assert_eq!(arr.value(0), 999i128);
    assert_eq!(arr.value(1), -350i128);

    // Also verify currency-prefixed string literals are accepted.
    exec_ok(&sess, "INSERT INTO wallet VALUES (3, '$1.00')").await;
}

// ─── INET ─────────────────────────────────────────────────────────────────────

/// INET column: CREATE + INSERT valid addresses + SELECT round-trip.
///
/// Unblocked by vortex 0.71 which preserves Arrow field-level metadata
/// through the DataFusion read path (ADR-0024 BASIN_TYPE sidecar fix).
#[tokio::test]
async fn inet_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(&sess, "CREATE TABLE hosts (id BIGINT NOT NULL, addr INET)").await;

    exec_ok(
        &sess,
        "INSERT INTO hosts VALUES \
         (1, '192.168.1.1'), \
         (2, '10.0.0.0/8'), \
         (3, '::1'), \
         (4, '2001:db8::/32')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, addr FROM hosts ORDER BY id").await;
    assert!(!batches.is_empty());

    // Arrow type check.
    let schema0 = batches[0].schema();
    let field = schema0.field_with_name("addr").unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8, "INET must be Utf8");
    assert_eq!(
        field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("INET"),
        "INET field must carry BASIN_TYPE=INET marker"
    );

    // Value check.
    let mut addrs: Vec<String> = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name("addr")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            assert!(!arr.is_null(i));
            addrs.push(arr.value(i).to_string());
        }
    }
    assert_eq!(addrs[0], "192.168.1.1");
    assert_eq!(addrs[1], "10.0.0.0/8");
    assert_eq!(addrs[2], "::1");
    assert_eq!(addrs[3], "2001:db8::/32");

    // Malformed address must be rejected.
    let bad = sess
        .execute("INSERT INTO hosts VALUES (5, 'not-an-ip')")
        .await;
    assert!(bad.is_err(), "malformed INET literal must be rejected");
}

// ─── CIDR ─────────────────────────────────────────────────────────────────────

/// CIDR column: CREATE + INSERT + SELECT round-trip.
///
/// Unblocked by vortex 0.71 which preserves Arrow field-level metadata
/// through the DataFusion read path (ADR-0024 BASIN_TYPE sidecar fix).
#[tokio::test]
async fn cidr_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(&sess, "CREATE TABLE nets (id BIGINT NOT NULL, net CIDR)").await;

    exec_ok(
        &sess,
        "INSERT INTO nets VALUES (1, '192.168.0.0/24'), (2, '10.0.0.0/8')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, net FROM nets ORDER BY id").await;
    let schema0 = batches[0].schema();
    let field = schema0.field_with_name("net").unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(
        field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("CIDR")
    );

    let arr = batches[0]
        .column_by_name("net")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "192.168.0.0/24");
    assert_eq!(arr.value(1), "10.0.0.0/8");

    // Bad CIDR.
    let bad = sess
        .execute("INSERT INTO nets VALUES (3, 'not-a-cidr')")
        .await;
    assert!(bad.is_err(), "malformed CIDR literal must be rejected");
}

// ─── MACADDR ──────────────────────────────────────────────────────────────────

/// MACADDR column: CREATE + INSERT + SELECT round-trip.
///
/// Unblocked by vortex 0.71 which preserves Arrow field-level metadata
/// through the DataFusion read path (ADR-0024 BASIN_TYPE sidecar fix).
#[tokio::test]
async fn macaddr_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(&sess, "CREATE TABLE nics (id BIGINT NOT NULL, mac MACADDR)").await;

    exec_ok(
        &sess,
        "INSERT INTO nics VALUES (1, '01:23:45:67:89:ab'), (2, '01-23-45-67-89-ab')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, mac FROM nics ORDER BY id").await;
    let schema0 = batches[0].schema();
    let field = schema0.field_with_name("mac").unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(
        field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("MACADDR")
    );

    let arr = batches[0]
        .column_by_name("mac")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Both rows should store in canonical colon-separated lowercase form.
    assert_eq!(arr.value(0), "01:23:45:67:89:ab");
    assert_eq!(arr.value(1), "01:23:45:67:89:ab");

    // Bad MAC.
    let bad = sess
        .execute("INSERT INTO nics VALUES (3, 'not-a-mac')")
        .await;
    assert!(bad.is_err(), "malformed MACADDR literal must be rejected");
}

// ─── MACADDR8 ─────────────────────────────────────────────────────────────────

/// MACADDR8 (EUI-64) column: CREATE + INSERT + SELECT round-trip.
///
/// Unblocked by vortex 0.71 which preserves Arrow field-level metadata
/// through the DataFusion read path (ADR-0024 BASIN_TYPE sidecar fix).
#[tokio::test]
async fn macaddr8_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE nics8 (id BIGINT NOT NULL, mac MACADDR8)",
    )
    .await;

    exec_ok(
        &sess,
        "INSERT INTO nics8 VALUES (1, '01:23:45:67:89:ab:cd:ef')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, mac FROM nics8 ORDER BY id").await;
    let schema0 = batches[0].schema();
    let field = schema0.field_with_name("mac").unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(
        field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("MACADDR8")
    );

    let arr = batches[0]
        .column_by_name("mac")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "01:23:45:67:89:ab:cd:ef");

    // Wrong group count for MACADDR8 (6 groups instead of 8).
    let bad = sess
        .execute("INSERT INTO nics8 VALUES (2, '01:23:45:67:89:ab')")
        .await;
    assert!(
        bad.is_err(),
        "6-group MAC must be rejected for MACADDR8 column"
    );
}

// ─── BIT(n) ───────────────────────────────────────────────────────────────────

/// BIT(8) column: CREATE + INSERT + SELECT round-trip.
///
/// Unblocked by vortex 0.71 which preserves Arrow field-level metadata
/// through the DataFusion read path (ADR-0024 BASIN_TYPE sidecar fix).
#[tokio::test]
async fn bit_fixed_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE flags (id BIGINT NOT NULL, bits BIT(8))",
    )
    .await;

    exec_ok(
        &sess,
        "INSERT INTO flags VALUES (1, '10101010'), (2, '00000000')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, bits FROM flags ORDER BY id").await;
    let schema0 = batches[0].schema();
    let field = schema0.field_with_name("bits").unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(
        field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("BIT(8)"),
        "BIT(8) field must carry BASIN_TYPE=BIT(8) marker"
    );

    let arr = batches[0]
        .column_by_name("bits")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "10101010");
    assert_eq!(arr.value(1), "00000000");

    // Too long.
    let bad = sess
        .execute("INSERT INTO flags VALUES (3, '101010101')") // 9 bits
        .await;
    assert!(bad.is_err(), "9-bit literal must be rejected for BIT(8)");

    // Too short (BIT(n) requires exact length, not max).
    let bad3 = sess
        .execute("INSERT INTO flags VALUES (5, '1010')") // 4 bits
        .await;
    assert!(bad3.is_err(), "4-bit literal must be rejected for BIT(8)");

    // Invalid character.
    let bad2 = sess
        .execute("INSERT INTO flags VALUES (4, '1010102X')")
        .await;
    assert!(bad2.is_err(), "non-bit characters must be rejected");
}

// ─── VARBIT(n) ────────────────────────────────────────────────────────────────

/// VARBIT(16) column: variable-length bit string up to 16 bits.
///
/// BASIN_TYPE metadata stripping (the 0.71 fix) is NOT the blocker here.
/// CREATE TABLE fails with "unsupported column type in PoC: VARBIT(16)" —
/// the engine schema layer does not yet map VARBIT to an Arrow type.
/// Remains blocked until that mapping is implemented.
#[tokio::test]
#[ignore = "engine bug: VARBIT column type not supported in schema layer (CREATE TABLE fails — separate from vortex metadata fix)"]
async fn varbit_round_trip() {
    let (_dir, engine) = open_engine().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    exec_ok(
        &sess,
        "CREATE TABLE vbits (id BIGINT NOT NULL, bits VARBIT(16))",
    )
    .await;

    exec_ok(
        &sess,
        "INSERT INTO vbits VALUES (1, '1'), (2, '10101010'), (3, '1111111111111111')",
    )
    .await;

    let batches = rows(&sess, "SELECT id, bits FROM vbits ORDER BY id").await;
    let schema0 = batches[0].schema();
    let field = schema0.field_with_name("bits").unwrap();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(
        field.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("VARBIT(16)")
    );

    let arr = batches[0]
        .column_by_name("bits")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "1");
    assert_eq!(arr.value(1), "10101010");
    assert_eq!(arr.value(2), "1111111111111111");

    // Exceeds max length.
    let bad = sess
        .execute("INSERT INTO vbits VALUES (4, '11111111111111111')") // 17 bits
        .await;
    assert!(
        bad.is_err(),
        "17-bit literal must be rejected for VARBIT(16)"
    );
}
