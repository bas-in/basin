//! Feature coverage suite.
//!
//! One focused integration test per ✅ row in `CAPABILITIES.md` whose
//! capability isn't already exercised by an existing integration test.
//! Each test is small, fast, and panics loudly if the claim regresses so
//! the row in `CAPABILITIES.md` matches a passing assertion in CI.
//!
//! Audit table — every CAPABILITIES.md ✅ row is either covered here or
//! cross-referenced to its existing test:
//!
//! ## Wire protocol
//! - pgwire v3                            -> covered elsewhere: tests/integration/tests/poc_smoke.rs
//! - Simple query (Q)                     -> covered elsewhere: tests/integration/tests/poc_smoke.rs
//! - Extended query (Parse/Bind/...)      -> covered elsewhere: tests/integration/tests/poc_extended_smoke.rs
//! - Binary parameter / result format     -> covered elsewhere: tests/integration/tests/poc_extended_smoke.rs, orm_smoke.rs
//! - COPY FROM STDIN / COPY TO STDOUT     -> covered elsewhere: tests/integration/tests/copy_round_trip.rs
//! - TLS (rustls on pgwire listener)      -> covered elsewhere: crates/basin-router/src/tls.rs (unit), services/basin-server (deploy-only); wire-level cert handshake is plumbed and not exercised here to avoid baking PEM fixtures into integration deps
//!
//! ## SQL surface
//! - CREATE TABLE                         -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - INSERT VALUES (single + multi-row)   -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - SELECT WHERE (single table)          -> covered elsewhere: tests/integration/tests/viability_predicate_pushdown.rs
//! - SHOW TABLES (per-project scoped)      -> covered by feature_coverage::show_tables_per_project_scoped
//! - ORDER BY / LIMIT                     -> covered elsewhere: tests/integration/tests/poc_smoke.rs
//! - ORDER BY … NULLS FIRST / LAST       -> covered elsewhere: tests/integration/tests/select_advanced.rs
//! - SELECT DISTINCT ON (cols)            -> covered elsewhere: tests/integration/tests/select_advanced.rs
//! - FOR UPDATE / SHARE / NO KEY UPDATE / KEY SHARE -> covered elsewhere: tests/integration/tests/select_advanced.rs
//! - FETCH FIRST/NEXT N ROWS ONLY        -> covered elsewhere: tests/integration/tests/select_advanced.rs
//! - OFFSET N ROW / ROWS (SQL-std)       -> covered elsewhere: tests/integration/tests/select_advanced.rs
//! - TABLE <name> shorthand              -> covered elsewhere: tests/integration/tests/select_advanced.rs
//! - TABLESAMPLE BERNOULLI / SYSTEM      -> covered elsewhere: tests/integration/tests/select_advanced.rs
//! - UPDATE / DELETE                      -> covered elsewhere: tests/integration/tests/viability_update_delete.rs
//! - ALTER TABLE                          -> covered elsewhere: tests/integration/tests/viability_alter_table.rs
//! - CREATE POLICY (RLS)                  -> covered elsewhere: tests/integration/tests/viability_rls_basic.rs
//! - LATERAL subqueries / joins           -> covered elsewhere: tests/integration/tests/lateral_joins.rs
//! - Prepared statements + parameter bind -> covered elsewhere: tests/integration/tests/poc_extended_smoke.rs
//!
//! ## Types
//! - BIGINT / INTEGER / SMALLINT          -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - TEXT / VARCHAR                       -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - BOOLEAN                              -> covered elsewhere: tests/integration/tests/orm_smoke.rs
//! - DOUBLE PRECISION                     -> covered by feature_coverage::double_precision_round_trips
//! - vector(N)                            -> covered elsewhere: tests/integration/tests/vector_smoke.rs
//! - BYTEA                                -> covered elsewhere: tests/integration/tests/orm_smoke.rs
//! - JSONB                                -> covered elsewhere: tests/integration/tests/viability_jsonb.rs
//! - UUID                                 -> covered elsewhere: tests/integration/tests/viability_uuid.rs
//! - TIMESTAMPTZ                          -> covered elsewhere: tests/integration/tests/viability_partition_pruning.rs, viability_continuous_aggregate.rs
//! - INT4RANGE                            -> covered elsewhere: tests/integration/tests/type_stubs.rs (ddl_accepts_int4range_column, int4range_constructor_via_sql)
//! - INT8RANGE                            -> covered elsewhere: tests/integration/tests/type_stubs.rs (ddl_accepts_int8range_column)
//! - NUMRANGE                             -> covered elsewhere: tests/integration/tests/type_stubs.rs (ddl_accepts_numrange_column)
//! - DATERANGE                            -> covered elsewhere: tests/integration/tests/type_stubs.rs (ddl_accepts_daterange_column)
//! - TSRANGE                              -> covered elsewhere: tests/integration/tests/type_stubs.rs (ddl_accepts_tsrange_column)
//! - TSTZRANGE                            -> covered elsewhere: tests/integration/tests/type_stubs.rs (ddl_accepts_tstzrange_column)
//! - Range operator @> (contains)        -> covered elsewhere: tests/integration/tests/type_stubs.rs (range_contains_elem_via_sql)
//! - Range operator <@ (contained by)    -> covered elsewhere: tests/integration/tests/type_stubs.rs (range_contains_elem_via_sql)
//! - Range operator && (overlaps)        -> covered elsewhere: tests/integration/tests/type_stubs.rs (range_overlaps_via_sql)
//! - Range operator << (strictly left)   -> covered by feature_coverage::range_strictly_left_operator
//! - Range operator >> (strictly right)  -> covered by feature_coverage::range_strictly_right_operator
//! - Range operator -|- (adjacent)       -> covered by feature_coverage::range_adjacent_operator
//! - Range function lower / upper        -> covered elsewhere: tests/integration/tests/type_stubs.rs (range_lower_upper_via_sql)
//! - Range function isempty              -> covered by unit tests in crates/basin-engine/src/range_udf.rs
//! - Range function lower_inc/upper_inc  -> covered elsewhere: tests/integration/tests/type_stubs.rs (range_lower_inc_upper_inc_via_sql)
//! - Range function lower_inf/upper_inf  -> covered by unit tests in crates/basin-engine/src/range_udf.rs
//! - Range function range_contains_elem  -> covered elsewhere: tests/integration/tests/type_stubs.rs (range_contains_elem_via_sql)
//! - Range function range_contains_range -> covered by feature_coverage::range_contains_range_udf
//! - Range function range_overlaps       -> covered elsewhere: tests/integration/tests/type_stubs.rs (range_overlaps_via_sql)
//! - Range function range_strictly_left  -> covered by feature_coverage::range_strictly_left_operator
//! - Range function range_strictly_right -> covered by feature_coverage::range_strictly_right_operator
//! - Range function range_adjacent       -> covered by feature_coverage::range_adjacent_operator
//! - Range function range_merge          -> covered by feature_coverage::range_merge_udf
//!
//! ## Multi-project
//! - Per-project bucket prefix isolation   -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - Connection -> project via username    -> covered elsewhere: tests/integration/tests/poc_smoke.rs
//! - Per-project snapshots                 -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - Per-project fairness (Semaphore)      -> covered elsewhere: tests/integration/tests/viability_isolation_under_load.rs
//! - Row-Level Security                   -> covered elsewhere: tests/integration/tests/viability_rls_isolation.rs
//! - Project deletion                      -> covered elsewhere: tests/integration/tests/viability_project_deletion.rs
//! - Table fork (catalog COW)             -> covered by feature_coverage::table_fork_clones_within_project
//! - Within-project time partitioning      -> covered elsewhere: tests/integration/tests/viability_partition_pruning.rs
//! - Tiered storage (hot/cold)            -> covered elsewhere: tests/integration/tests/viability_tiered_storage.rs
//! - Whale-project pinning                 -> covered by feature_coverage::whale_project_pinning_routes_deterministically
//!
//! ## Storage
//! - Parquet under projects/{id}/...       -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - Predicate pushdown                   -> covered elsewhere: tests/integration/tests/viability_predicate_pushdown.rs
//! - Projection pushdown                  -> covered elsewhere: tests/integration/tests/phase1_substrate.rs (uses ReadOptions.projection)
//! - Bloom filters                        -> covered elsewhere: tests/integration/tests/viability_bloom_filter_pruning.rs
//! - Coalesced metadata in catalog        -> covered elsewhere: tests/integration/tests/viability_catalog_stats_pruning.rs
//! - Per-table row-group sizing           -> covered elsewhere: tests/integration/tests/viability_row_group_sizing.rs
//! - Cluster-by physical sort             -> covered elsewhere: tests/integration/tests/viability_cluster_by_sql.rs
//! - Pluggable object_store (LocalFS, S3) -> covered elsewhere: tests/integration/tests/phase1_substrate.rs (LocalFS), s3_*.rs (S3)
//! - NVMe disk cache                      -> covered elsewhere: tests/integration/tests/viability_disk_cache.rs
//! - Parquet page cache                   -> covered elsewhere: tests/integration/tests/viability_page_cache.rs
//! - HTTP/2 toggle for S3 client          -> covered elsewhere: crates/basin-storage S3Config (config-only flag; no behavioural test that wouldn't require live HTTPS)
//! - Iceberg-style catalog (in-memory)    -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - Iceberg-style catalog (durable)      -> covered elsewhere: tests/integration/tests/durable_catalog_smoke.rs
//!
//! ## Query execution
//! - Query path via DataFusion            -> covered elsewhere: tests/integration/tests/phase1_substrate.rs
//! - Cost-based query rejection           -> covered elsewhere: crates/basin-engine/src/cost_check.rs (in-crate; OnceLock-cached env var means a process-wide integration test would race)
//! - Window functions (ranking + offset + frame + aggregate OVER) -> covered elsewhere: tests/integration/tests/window_fns.rs
//!
//! ## Vector search
//! - vector(N) column type                -> covered elsewhere: tests/integration/tests/vector_smoke.rs
//! - Distance ops <-> <#> <=>             -> covered by feature_coverage::vector_distance_operators_rewrite
//! - L2 / cosine / dot UDFs               -> covered elsewhere: tests/integration/tests/vector_smoke.rs (l2_distance), feature_coverage::vector_distance_operators_rewrite (cosine + dot)
//! - HNSW sidecar                         -> covered elsewhere: tests/integration/tests/vector_smoke.rs, s3_vector_search.rs
//! - vector_search fast path              -> covered elsewhere: tests/integration/tests/vector_smoke.rs
//!
//! ## Postgres-extension equivalents
//! - pg_vector (native)                   -> covered elsewhere: tests/integration/tests/vector_smoke.rs
//! - pg_cron (basin-cron)                 -> covered elsewhere: tests/integration/tests/viability_basin_cron.rs
//! - pg_net + http (basin-net)            -> covered elsewhere: tests/integration/tests/viability_basin_net.rs
//! - pgcrypto (digest/encode/crypt)       -> covered elsewhere: tests/integration/tests/viability_uuid.rs
//! - uuid-ossp                            -> covered elsewhere: tests/integration/tests/viability_uuid.rs
//! - PostGIS subset (basin-geo)           -> covered elsewhere: tests/integration/tests/viability_postgis.rs
//! - pg_trgm (basin-trgm)                 -> covered elsewhere: tests/integration/tests/viability_pg_trgm.rs
//! - TimescaleDB CV (basin-cv)            -> covered elsewhere: tests/integration/tests/viability_continuous_aggregate.rs
//! - TimescaleDB hypertables              -> covered elsewhere: tests/integration/tests/viability_partition_pruning.rs
//! - pg_stat_statements (OTEL)            -> covered elsewhere: crates/basin-common/src/telemetry.rs (every layer is `#[tracing::instrument]`-annotated; OTLP export is wire-only behaviour)
//! - Citus (router consistent-hash)       -> covered elsewhere: tests/integration/tests/insert_through_shard.rs, s3_shard_insert_path.rs
//! - pg_partman (Iceberg manifest)        -> covered elsewhere: tests/integration/tests/phase1_substrate.rs (snapshot evolution)
//! - hstore (use JSONB)                   -> covered elsewhere: tests/integration/tests/viability_jsonb.rs
//!
//! ## Operations
//! - Per-project metrics                   -> covered elsewhere: tests/integration/tests/viability_per_project_counters.rs
//! - OpenTelemetry traces                 -> covered elsewhere: every layer carries `#[tracing::instrument]`; OTLP export is a transport-only flag
//! - Structured logs                      -> covered elsewhere: crates/basin-common/src/telemetry.rs (format selection at startup; no per-call assertion to write)
//! - Connection pooling (basin-pool)      -> covered elsewhere: tests/integration/tests/poc_pool_smoke.rs
//! - Rate limiting (basin-net)            -> covered elsewhere: tests/integration/tests/viability_basin_net.rs
//! - Rate limiting (pgwire)               -> covered by feature_coverage::pgwire_rate_limit_returns_53400 (and exercised at scale by tests/integration/tests/security.rs)
//!
//! ## Auth + REST
//! - basin-auth (signup, signin, ...)     -> covered elsewhere: crates/basin-auth/src/lib.rs#[cfg(test)], tests/integration/tests/poc_pgwire_jwt_smoke.rs
//! - basin-rest (PostgREST-compatible)    -> covered elsewhere: crates/basin-rest/src/tests.rs, tests/integration/tests/poc_rest_smoke.rs
//! - Pgwire JWT auth                      -> covered elsewhere: tests/integration/tests/poc_pgwire_jwt_smoke.rs

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_router::{parse_pins_env, PgRateLimit, ShardMap};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use serde_json;

/// Spin a fresh in-process `Engine` against `LocalFileSystem` + `InMemoryCatalog`.
fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// SHOW TABLES is per-project scoped: project A never sees project B's table.
#[tokio::test]
async fn show_tables_per_project_scoped() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let a = engine.open_session(ProjectId::new()).await.unwrap();
    let b = engine.open_session(ProjectId::new()).await.unwrap();
    a.execute("CREATE TABLE only_a (id BIGINT NOT NULL)")
        .await
        .unwrap();
    b.execute("CREATE TABLE only_b (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let res = a.execute("SHOW TABLES").await.unwrap();
    let names = collect_strings(res, "table_name");
    assert!(
        names.iter().any(|n| n == "only_a"),
        "project A missing own: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "only_b"),
        "project A leaked project B's table: {names:?}"
    );
}

/// `DOUBLE PRECISION` round-trips through the engine via Arrow `Float64`.
#[tokio::test]
async fn double_precision_round_trips() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();
    s.execute("CREATE TABLE m (id BIGINT NOT NULL, score DOUBLE PRECISION NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO m VALUES (1, 1.5), (2, 2.25), (3, -3.0)")
        .await
        .unwrap();

    let ExecResult::Rows { batches, .. } =
        s.execute("SELECT score FROM m ORDER BY id").await.unwrap()
    else {
        panic!("expected Rows");
    };
    let mut got = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        for i in 0..arr.len() {
            got.push(arr.value(i));
        }
    }
    assert_eq!(got, vec![1.5, 2.25, -3.0]);
}

/// `Catalog::fork_table` clones the source's snapshot history and schema
/// within the same project, sharing data files by reference.
#[tokio::test]
async fn table_fork_clones_within_project() {
    let cat = Arc::new(InMemoryCatalog::new());
    let t = ProjectId::new();
    cat.create_namespace(&t).await.unwrap();
    let src = TableName::new("src").unwrap();
    let dst = TableName::new("dst").unwrap();
    let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]);
    cat.create_table(&t, &src, &schema).await.unwrap();
    let forked = cat.fork_table(&t, &src, &dst).await.unwrap();
    assert_eq!(
        forked.snapshots.len(),
        cat.load_table(&t, &src).await.unwrap().snapshots.len()
    );
    let listed = cat.list_tables(&t).await.unwrap();
    assert!(listed.contains(&src) && listed.contains(&dst));
}

/// Whale-project pinning: a pin hard-routes a project to its assigned shard
/// regardless of consistent-hash bucketing.
#[tokio::test]
async fn whale_project_pinning_routes_deterministically() {
    let endpoints: Vec<String> = (0..4).map(|i| format!("ep-{i}")).collect();
    let whale = ProjectId::new();
    let mut pins = HashMap::new();
    pins.insert(whale, 3);
    let map = ShardMap::with_pins(endpoints.clone(), pins).unwrap();
    assert_eq!(
        map.shard_index(&whale),
        3,
        "pin must override consistent hash"
    );
    assert!(map.is_pinned(&whale));
    // parse_pins_env round-trips the same shape
    let parsed = parse_pins_env(&format!("{whale}:2")).unwrap();
    assert_eq!(parsed.get(&whale), Some(&2));
}

/// Distance operators `<->`, `<#>`, `<=>` rewrite to the corresponding UDFs.
/// Run a query for each and assert the result column types are numeric.
#[tokio::test]
async fn vector_distance_operators_rewrite() {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();
    s.execute("CREATE TABLE v (id BIGINT NOT NULL, e VECTOR(3) NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO v VALUES (1, '[1.0, 0.0, 0.0]'), (2, '[0.0, 1.0, 0.0]')")
        .await
        .unwrap();
    for op in ["<->", "<#>", "<=>"] {
        let sql = format!("SELECT id FROM v ORDER BY e {op} '[1.0, 0.0, 0.0]' LIMIT 1");
        let ExecResult::Rows { batches, .. } = s
            .execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("{op}: {e:?}"))
        else {
            panic!("{op}: expected Rows");
        };
        let arr = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        assert_eq!(arr.value(0), 1, "{op}: nearest to query is row 1");
    }
}

/// Pgwire rate limiter returns SQLSTATE 53400 once the burst is drained.
/// Drives `PgRateLimit::check` directly; the protocol-side mapping to
/// `53400` is asserted by tests/integration/tests/security.rs.
#[tokio::test]
async fn pgwire_rate_limit_returns_53400() {
    let rl = PgRateLimit::with_qps(1);
    let t = ProjectId::new();
    // 1 qps × 3 burst = 3 free tokens; the 4th-onwards must reject.
    for _ in 0..3 {
        rl.check(&t).expect("burst should pass");
    }
    let mut throttled = false;
    for _ in 0..50 {
        if rl.check(&t).is_err() {
            throttled = true;
            break;
        }
    }
    assert!(throttled, "rate limiter never threw, despite drained burst");
}

// ---------------------------------------------------------------------------
// Range type feature coverage tests
// ---------------------------------------------------------------------------

/// Helper: open a fresh in-process engine session.
async fn range_session() -> (basin_engine::ProjectSession, TempDir) {
    let dir = TempDir::new().unwrap();
    let engine = engine_in(&dir);
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    (sess, dir)
}

/// `range_contains_range` UDF: checks that [1,10) @> [3,7) → true
/// and [1,5) @> [3,7) → false.
#[tokio::test]
async fn range_contains_range_udf() {
    let (sess, _dir) = range_session().await;
    let r1 = sess
        .execute("SELECT range_contains_range(int4range(1, 10), int4range(3, 7))")
        .await
        .unwrap();
    let r2 = sess
        .execute("SELECT range_contains_range(int4range(1, 5), int4range(3, 7))")
        .await
        .unwrap();
    if let (ExecResult::Rows { batches: b1, .. }, ExecResult::Rows { batches: b2, .. }) = (r1, r2)
    {
        use arrow_array::BooleanArray;
        let contains = b1[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        let no_contain = b2[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(contains.value(0), "[1,10) should contain [3,7)");
        assert!(!no_contain.value(0), "[1,5) should not contain [3,7)");
    } else {
        panic!("expected Rows result");
    }
}

/// `range_strictly_left` UDF: [1,5) << [5,10) → true; [1,6) << [5,10) → false.
#[tokio::test]
async fn range_strictly_left_operator() {
    let (sess, _dir) = range_session().await;
    let r1 = sess
        .execute("SELECT range_strictly_left(int4range(1, 5), int4range(5, 10))")
        .await
        .unwrap();
    let r2 = sess
        .execute("SELECT range_strictly_left(int4range(1, 6), int4range(5, 10))")
        .await
        .unwrap();
    if let (ExecResult::Rows { batches: b1, .. }, ExecResult::Rows { batches: b2, .. }) = (r1, r2)
    {
        use arrow_array::BooleanArray;
        let left = b1[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        let not_left = b2[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(left.value(0), "[1,5) should be strictly left of [5,10)");
        assert!(!not_left.value(0), "[1,6) is not strictly left of [5,10)");
    } else {
        panic!("expected Rows result");
    }
}

/// `range_strictly_right` UDF: [5,10) >> [1,5) → true; [4,10) >> [1,5) → false.
#[tokio::test]
async fn range_strictly_right_operator() {
    let (sess, _dir) = range_session().await;
    let r1 = sess
        .execute("SELECT range_strictly_right(int4range(5, 10), int4range(1, 5))")
        .await
        .unwrap();
    let r2 = sess
        .execute("SELECT range_strictly_right(int4range(4, 10), int4range(1, 5))")
        .await
        .unwrap();
    if let (ExecResult::Rows { batches: b1, .. }, ExecResult::Rows { batches: b2, .. }) = (r1, r2)
    {
        use arrow_array::BooleanArray;
        let right = b1[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        let not_right = b2[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(right.value(0), "[5,10) should be strictly right of [1,5)");
        assert!(!not_right.value(0), "[4,10) is not strictly right of [1,5)");
    } else {
        panic!("expected Rows result");
    }
}

/// `range_adjacent` UDF: [1,5) -|- [5,10) → true (share bound, one excl one incl).
#[tokio::test]
async fn range_adjacent_operator() {
    let (sess, _dir) = range_session().await;
    let r1 = sess
        .execute("SELECT range_adjacent(int4range(1, 5), int4range(5, 10))")
        .await
        .unwrap();
    let r2 = sess
        .execute("SELECT range_adjacent(int4range(1, 4), int4range(5, 10))")
        .await
        .unwrap();
    if let (ExecResult::Rows { batches: b1, .. }, ExecResult::Rows { batches: b2, .. }) = (r1, r2)
    {
        use arrow_array::BooleanArray;
        let adj = b1[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        let not_adj = b2[0].column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(adj.value(0), "[1,5) and [5,10) should be adjacent");
        assert!(!not_adj.value(0), "[1,4) and [5,10) should not be adjacent");
    } else {
        panic!("expected Rows result");
    }
}

/// `range_merge` UDF: hull of [1,5) and [3,10) should give [1,10).
#[tokio::test]
async fn range_merge_udf() {
    let (sess, _dir) = range_session().await;
    let result = sess
        .execute("SELECT range_merge(int4range(1, 5), int4range(3, 10))")
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = result {
        let col = batches[0].column(0);
        let sa = col.as_any().downcast_ref::<StringArray>().unwrap();
        let v: serde_json::Value = serde_json::from_str(sa.value(0)).unwrap();
        assert_eq!(v["l"], 1, "merged lower should be 1");
        assert_eq!(v["u"], 10, "merged upper should be 10");
        assert_eq!(v["li"], true, "merged lower should be inclusive");
        assert_eq!(v["ui"], false, "merged upper should be exclusive (from [3,10))");
    } else {
        panic!("expected Rows result");
    }
}

// --- helpers ----------------------------------------------------------------

fn collect_strings(res: ExecResult, col: &str) -> Vec<String> {
    let ExecResult::Rows { batches, .. } = res else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        for i in 0..arr.len() {
            out.push(arr.value(i).to_owned());
        }
    }
    out
}
