//! Integration tests for new Arrow type mappings: network addresses, money,
//! XML, and range types.
//!
//! These are DDL-acceptance and UDF smoke tests. Each test:
//!   1. Creates a table with the new column type.
//!   2. Verifies the catalog's Arrow schema carries the correct metadata marker.
//!   3. (Where applicable) exercises the range constructor / accessor UDFs.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{BooleanArray, StringArray};
use arrow_schema::DataType;
use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct Harness {
    engine: Engine,
    catalog: Arc<dyn basin_catalog::Catalog>,
    project: ProjectId,
}

impl Harness {
    fn new() -> Self {
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
            catalog: catalog.clone(),
            shard: None,
        });
        let project = ProjectId::new();
        Harness {
            engine,
            catalog,
            project,
        }
    }

    async fn session(&self) -> basin_engine::ProjectSession {
        self.engine
            .open_session(self.project.clone())
            .await
            .unwrap()
    }

    async fn exec_ok(&self, sess: &basin_engine::ProjectSession, sql: &str) -> ExecResult {
        sess.execute(sql)
            .await
            .unwrap_or_else(|e| panic!("SQL failed: {e}\nSQL: {sql}"))
    }

    async fn field_for(&self, table: &str, col: &str) -> arrow_schema::Field {
        let tn = TableName::new(table).unwrap();
        let meta = self.catalog.load_table(&self.project, &tn).await.unwrap();
        meta.schema.field_with_name(col).unwrap().clone()
    }
}

// ---------------------------------------------------------------------------
// Network address types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ddl_accepts_inet_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_inet (id BIGINT, addr INET)")
        .await;
    let f = h.field_for("t_inet", "addr").await;
    assert_eq!(f.data_type(), &DataType::Utf8, "INET must be Utf8");
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("INET")
    );
}

#[tokio::test]
async fn ddl_accepts_cidr_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_cidr (id BIGINT, net CIDR)")
        .await;
    let f = h.field_for("t_cidr", "net").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("CIDR")
    );
}

#[tokio::test]
async fn ddl_accepts_macaddr_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_mac (id BIGINT, mac MACADDR)")
        .await;
    let f = h.field_for("t_mac", "mac").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("MACADDR")
    );
}

#[tokio::test]
async fn ddl_accepts_macaddr8_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_mac8 (id BIGINT, mac MACADDR8)")
        .await;
    let f = h.field_for("t_mac8", "mac").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("MACADDR8")
    );
}

// ---------------------------------------------------------------------------
// Money type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ddl_accepts_money_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_money (id BIGINT, price MONEY)")
        .await;
    let f = h.field_for("t_money", "price").await;
    assert_eq!(
        f.data_type(),
        &DataType::Decimal128(19, 2),
        "MONEY must be Decimal128(19,2)"
    );
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("MONEY")
    );
}

// ---------------------------------------------------------------------------
// XML type
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ddl_accepts_xml_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_xml (id BIGINT, doc XML)")
        .await;
    let f = h.field_for("t_xml", "doc").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("XML")
    );
}

// ---------------------------------------------------------------------------
// Range types — DDL acceptance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ddl_accepts_int4range_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_int4r (id BIGINT, r INT4RANGE)")
        .await;
    let f = h.field_for("t_int4r", "r").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("INT4RANGE")
    );
}

#[tokio::test]
async fn ddl_accepts_int8range_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_int8r (id BIGINT, r INT8RANGE)")
        .await;
    let f = h.field_for("t_int8r", "r").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("INT8RANGE")
    );
}

#[tokio::test]
async fn ddl_accepts_numrange_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_numr (id BIGINT, r NUMRANGE)")
        .await;
    let f = h.field_for("t_numr", "r").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("NUMRANGE")
    );
}

#[tokio::test]
async fn ddl_accepts_daterange_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_dr (id BIGINT, r DATERANGE)")
        .await;
    let f = h.field_for("t_dr", "r").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("DATERANGE")
    );
}

#[tokio::test]
async fn ddl_accepts_tsrange_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_tsr (id BIGINT, r TSRANGE)")
        .await;
    let f = h.field_for("t_tsr", "r").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("TSRANGE")
    );
}

#[tokio::test]
async fn ddl_accepts_tstzrange_column() {
    let h = Harness::new();
    let sess = h.session().await;
    h.exec_ok(&sess, "CREATE TABLE t_tstzr (id BIGINT, r TSTZRANGE)")
        .await;
    let f = h.field_for("t_tstzr", "r").await;
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata().get("BASIN_TYPE").map(|s| s.as_str()),
        Some("TSTZRANGE")
    );
}

// ---------------------------------------------------------------------------
// Range UDF smoke tests via SQL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn int4range_constructor_via_sql() {
    let h = Harness::new();
    let sess = h.session().await;
    let result = h.exec_ok(&sess, "SELECT int4range(1, 10)").await;
    if let ExecResult::Rows { batches, .. } = result {
        assert!(!batches.is_empty());
        let col = batches[0].column(0);
        let sa = col.as_any().downcast_ref::<StringArray>().unwrap();
        let v: serde_json::Value = serde_json::from_str(sa.value(0)).unwrap();
        assert_eq!(v["l"], 1);
        assert_eq!(v["u"], 10);
        assert_eq!(v["li"], true);
        assert_eq!(v["ui"], false);
    } else {
        panic!("expected Rows result");
    }
}

#[tokio::test]
async fn range_lower_upper_via_sql() {
    let h = Harness::new();
    let sess = h.session().await;
    let result = h
        .exec_ok(
            &sess,
            "SELECT lower(int4range(5, 20)), upper(int4range(5, 20))",
        )
        .await;
    if let ExecResult::Rows { batches, .. } = result {
        let lo_sa = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let hi_sa = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(lo_sa.value(0), "5");
        assert_eq!(hi_sa.value(0), "20");
    } else {
        panic!("expected Rows result");
    }
}

#[tokio::test]
async fn range_lower_inc_upper_inc_via_sql() {
    let h = Harness::new();
    let sess = h.session().await;
    let result = h
        .exec_ok(
            &sess,
            "SELECT lower_inc(int4range(1, 5)), upper_inc(int4range(1, 5))",
        )
        .await;
    if let ExecResult::Rows { batches, .. } = result {
        let li_ba = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let ui_ba = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(
            li_ba.value(0),
            "lower_inc should be true for default [) range"
        );
        assert!(
            !ui_ba.value(0),
            "upper_inc should be false for default [) range"
        );
    } else {
        panic!("expected Rows result");
    }
}

#[tokio::test]
async fn range_contains_elem_via_sql() {
    let h = Harness::new();
    let sess = h.session().await;
    // 5 in [1, 10) → true
    let r1 = h
        .exec_ok(&sess, "SELECT range_contains_elem(int4range(1, 10), 5)")
        .await;
    // 10 in [1, 10) → false (upper-exclusive)
    let r2 = h
        .exec_ok(&sess, "SELECT range_contains_elem(int4range(1, 10), 10)")
        .await;
    if let (ExecResult::Rows { batches: b1, .. }, ExecResult::Rows { batches: b2, .. }) = (r1, r2) {
        let in_range = b1[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let not_in = b2[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(in_range.value(0));
        assert!(!not_in.value(0));
    } else {
        panic!("expected Rows results");
    }
}

#[tokio::test]
async fn range_overlaps_via_sql() {
    let h = Harness::new();
    let sess = h.session().await;
    // [1,10) && [5,15) → overlap
    let r1 = h
        .exec_ok(
            &sess,
            "SELECT range_overlaps(int4range(1, 10), int4range(5, 15))",
        )
        .await;
    // [1,5) && [5,10) → no overlap (upper-exclusive meets lower-inclusive)
    let r2 = h
        .exec_ok(
            &sess,
            "SELECT range_overlaps(int4range(1, 5), int4range(5, 10))",
        )
        .await;
    if let (ExecResult::Rows { batches: b1, .. }, ExecResult::Rows { batches: b2, .. }) = (r1, r2) {
        let overlaps = b1[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let no_overlap = b2[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(overlaps.value(0));
        assert!(!no_overlap.value(0));
    } else {
        panic!("expected Rows results");
    }
}
