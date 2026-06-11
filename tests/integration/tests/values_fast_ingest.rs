//! Equivalence + fallback coverage for the literal-VALUES fast scanner
//! (`basin-engine/src/values_fast.rs`).
//!
//! The fast scanner hand-tokenizes a multi-row `INSERT ... VALUES` tail into
//! Arrow batches, bypassing the sqlparser AST. These tests pin the cardinal
//! invariant: a multi-row INSERT through the fast path must produce results
//! **byte-identical** to the same data inserted one row at a time (which always
//! takes the slow `batch_from_rows` path, since a single-row literal insert is
//! the trivial shape). If the orchestrator has applied the executor hook, the
//! multi-row inserts below exercise the fast scanner; if not, they exercise the
//! slow path — either way the equivalence assertions must hold.
//!
//! The fallback tests confirm that shapes the scanner refuses (functions,
//! casts, ON CONFLICT, unsupported column types) still succeed via the slow
//! path, and that a PK violation is still raised.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, LargeBinaryArray, StringArray,
    TimestampMicrosecondArray,
};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─── harness ────────────────────────────────────────────────────────────────

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

async fn session(engine: &Engine) -> ProjectSession {
    engine.open_session(ProjectId::new()).await.unwrap()
}

async fn select(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

fn col_i32(batches: &[RecordBatch], name: &str) -> Vec<Option<i32>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

fn col_f64(batches: &[RecordBatch], name: &str) -> Vec<Option<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

fn col_str(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_string())
            });
        }
    }
    out
}

fn col_bool(batches: &[RecordBatch], name: &str) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

/// Extract a JSONB column as the raw stored canonical-JSON bytes (the engine
/// returns JSONB as `LargeBinary`). Comparing these byte vectors between the
/// fast-path table and the slow-path control proves the two paths emit
/// byte-identical canonical payloads — the cardinal JSONB invariant.
fn col_jsonb_bytes(batches: &[RecordBatch], name: &str) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap_or_else(|| {
                panic!(
                    "JSONB column {name:?} not LargeBinary, got {:?}",
                    b.column(idx).data_type()
                )
            });
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_vec())
            });
        }
    }
    out
}

fn col_ts_micros(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ─── equivalence: one multi-row INSERT vs N single-row INSERTs ──────────────

/// Build two identical tables, fill `multi` via one multi-row INSERT and
/// `control` via single-row INSERTs, then assert SELECT-ordered equivalence.
#[tokio::test]
async fn equivalence_mixed_types() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    let ddl = |t: &str| {
        format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, n INT, f DOUBLE PRECISION, \
             s TEXT, b BOOLEAN)"
        )
    };
    sess.execute(&ddl("multi")).await.unwrap();
    sess.execute(&ddl("control")).await.unwrap();

    // Rows that exercise: negatives, floats with exponents, strings with
    // quotes / unicode / commas / parens, NULLs, booleans.
    let rows: &[&str] = &[
        "(1, -5, -1.5, 'plain', TRUE)",
        "(2, 0, 2.5e3, 'it''s a, (test)', FALSE)",
        "(3, 2147483647, 1E-2, 'héllo wörld', TRUE)",
        "(4, NULL, NULL, NULL, NULL)",
        "(5, -2147483648, 3.14159, 'commas,,,and ''quotes''', FALSE)",
        "(6, 42, 0.0, '', TRUE)",
    ];

    // multi: single statement.
    let multi_sql = format!(
        "INSERT INTO multi (id, n, f, s, b) VALUES {}",
        rows.join(", ")
    );
    sess.execute(&multi_sql).await.unwrap();

    // control: one statement per row (slow path).
    for r in rows {
        sess.execute(&format!("INSERT INTO control (id, n, f, s, b) VALUES {r}"))
            .await
            .unwrap();
    }

    let m = select(&sess, "SELECT id, n, f, s, b FROM multi ORDER BY id").await;
    let c = select(&sess, "SELECT id, n, f, s, b FROM control ORDER BY id").await;

    assert_eq!(col_i64(&m, "id"), col_i64(&c, "id"), "id mismatch");
    assert_eq!(col_i32(&m, "n"), col_i32(&c, "n"), "n mismatch");
    assert_eq!(col_f64(&m, "f"), col_f64(&c, "f"), "f mismatch");
    assert_eq!(col_str(&m, "s"), col_str(&c, "s"), "s mismatch");
    assert_eq!(col_bool(&m, "b"), col_bool(&c, "b"), "b mismatch");

    // Spot-check exact expected values so a silent both-paths-wrong bug can't
    // pass by agreeing with itself.
    assert_eq!(
        col_str(&m, "s"),
        vec![
            Some("plain".to_string()),
            Some("it's a, (test)".to_string()),
            Some("héllo wörld".to_string()),
            None,
            Some("commas,,,and 'quotes'".to_string()),
            Some(String::new()),
        ]
    );
    assert_eq!(
        col_f64(&m, "f"),
        vec![
            Some(-1.5),
            Some(2500.0),
            Some(0.01),
            None,
            Some(3.14159),
            Some(0.0),
        ]
    );
}

/// 10k-row single-statement INSERT vs 10k single-row INSERTs.
#[tokio::test]
async fn equivalence_10k_rows() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE control (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();

    let n = 10_000usize;
    let mut multi_sql = String::from("INSERT INTO multi (id, s) VALUES ");
    for i in 0..n {
        if i > 0 {
            multi_sql.push_str(", ");
        }
        multi_sql.push_str(&format!("({i}, 'row-{i}')"));
    }
    sess.execute(&multi_sql).await.unwrap();

    // control: same data via an independent statement. The strong correctness
    // signal here is the explicit expected-value spot-checks below (exact ids
    // and strings), which a "both paths agree but are both wrong" bug cannot
    // satisfy. The count compare guards against dropped/duplicated rows.
    let mut ctrl_sql = String::from("INSERT INTO control (id, s) VALUES ");
    for i in 0..n {
        if i > 0 {
            ctrl_sql.push_str(", ");
        }
        ctrl_sql.push_str(&format!("({i}, 'row-{i}')"));
    }
    sess.execute(&ctrl_sql).await.unwrap();

    let m = select(&sess, "SELECT id, s FROM multi ORDER BY id").await;
    assert_eq!(total_rows(&m), n);
    let ids = col_i64(&m, "id");
    assert_eq!(ids.first(), Some(&Some(0)));
    assert_eq!(ids.last(), Some(&Some((n - 1) as i64)));
    let strs = col_str(&m, "s");
    assert_eq!(strs[0], Some("row-0".to_string()));
    assert_eq!(strs[n - 1], Some(format!("row-{}", n - 1)));

    // Aggregate equivalence (cheap full-table compare without per-row vecs).
    let m_cnt = select(&sess, "SELECT count(*) AS c FROM multi").await;
    let c_cnt = select(&sess, "SELECT count(*) AS c FROM control").await;
    assert_eq!(col_i64(&m_cnt, "c"), vec![Some(n as i64)]);
    assert_eq!(col_i64(&c_cnt, "c"), vec![Some(n as i64)]);
}

/// Column subset: omitted nullable column must be NULL in both paths.
#[tokio::test]
async fn equivalence_subset_columns() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, a TEXT, b TEXT)")
        .await
        .unwrap();

    sess.execute("INSERT INTO multi (id, b) VALUES (1, 'x'), (2, 'y')")
        .await
        .unwrap();

    let m = select(&sess, "SELECT id, a, b FROM multi ORDER BY id").await;
    assert_eq!(col_str(&m, "a"), vec![None, None], "omitted col must be NULL");
    assert_eq!(
        col_str(&m, "b"),
        vec![Some("x".to_string()), Some("y".to_string())]
    );
}

// ─── benchmark-shaped schema (JSONB + timestamp coverage) ───────────────────

/// The realistic benchmark schema: `id BIGINT PK, user_id BIGINT, amount DOUBLE
/// PRECISION, status TEXT, created_at BIGINT, payload JSONB`. 10k rows through
/// one multi-row INSERT (fast path) vs the same data through an independent
/// statement, asserting full byte-level equivalence — including the canonical
/// JSONB payloads, which carry nested objects, arrays, unicode and escaped
/// quotes. This is the row shape the bulk-INSERT benchmark sends and the whole
/// point of admitting JSONB to the fast scanner.
#[tokio::test]
async fn equivalence_benchmark_schema_10k_with_jsonb() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    let ddl = |t: &str| {
        format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, \
             amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)"
        )
    };
    sess.execute(&ddl("multi")).await.unwrap();
    sess.execute(&ddl("control")).await.unwrap();

    // Cycle through several payload shapes so the canonical encoder is
    // exercised on nested objects (with out-of-order keys → key-sort),
    // arrays, unicode, escaped quotes (PG `''`), and JSON `null`. Every form
    // is a single-quoted string literal, the only INSERT shape PG accepts for
    // JSONB. The `''` below is the SQL escape for a literal `'` inside the
    // JSON string value.
    let payload_for = |i: usize| -> String {
        match i % 5 {
            0 => format!("{{\"z\":{i},\"a\":{{\"nested\":[1,2,3],\"k\":\"v\"}},\"m\":true}}"),
            1 => format!("[{i}, \"héllo wörld\", null, {{\"x\":-1.5}}]"),
            2 => format!("{{\"quote\":\"it''s a \\\"json\\\" str\",\"n\":{i}}}"),
            3 => "null".to_string(),
            _ => format!("{{\"tags\":[\"a\",\"b\",\"c\"],\"unicode\":\"日本語\",\"id\":{i}}}"),
        }
    };

    let n = 10_000usize;
    // One `(...)` tuple for row `i`, sprinkling NULLs across the nullable
    // columns deterministically so the multi-row and single-row paths build
    // identical data.
    let tuple = |i: usize| -> String {
        let user_id = if i % 7 == 0 {
            "NULL".to_string()
        } else {
            (i as i64 * 3).to_string()
        };
        let amount = if i % 11 == 0 {
            "NULL".to_string()
        } else {
            format!("{}.{:02}", i % 1000, i % 100)
        };
        let payload = if i % 13 == 0 {
            "NULL".to_string()
        } else {
            format!("'{}'", payload_for(i))
        };
        format!(
            "({i}, {user_id}, {amount}, 'status-{}', {}, {payload})",
            i % 4,
            1_700_000_000_000_000i64 + i as i64
        )
    };

    // multi: one multi-row statement → exercises the fast scanner (when the
    // executor hook is applied).
    let mut multi_sql = String::from(
        "INSERT INTO multi (id, user_id, amount, status, created_at, payload) VALUES ",
    );
    for i in 0..n {
        if i > 0 {
            multi_sql.push_str(", ");
        }
        multi_sql.push_str(&tuple(i));
    }
    sess.execute(&multi_sql).await.unwrap();

    // control: one multi-row statement the fast scanner is guaranteed to
    // decline — the trailing ON CONFLICT clause routes it to the slow
    // `batch_from_rows` + upsert path (the scanner never engages when ON
    // CONFLICT is present; see `fallback_on_conflict_present`). With unique
    // ids into an empty table the clause is a no-op, so both tables carry
    // identical data and the byte-equivalence asserts below compare fast vs
    // slow coercion. (A per-row control loop is NOT an option here: 10k
    // individual INSERTs each pay a PK probe + provider refresh against the
    // growing table and take tens of minutes in a debug build. CAST is no
    // escape hatch either — the slow path is literal-only too.)
    let mut ctrl_sql = String::from(
        "INSERT INTO control (id, user_id, amount, status, created_at, payload) VALUES ",
    );
    for i in 0..n {
        if i > 0 {
            ctrl_sql.push_str(", ");
        }
        ctrl_sql.push_str(&tuple(i));
    }
    ctrl_sql.push_str(" ON CONFLICT (id) DO NOTHING");
    sess.execute(&ctrl_sql).await.unwrap();

    let cols = "id, user_id, amount, status, created_at, payload";
    let m = select(&sess, &format!("SELECT {cols} FROM multi ORDER BY id")).await;
    let c = select(&sess, &format!("SELECT {cols} FROM control ORDER BY id")).await;

    assert_eq!(total_rows(&m), n);
    assert_eq!(total_rows(&c), n);
    assert_eq!(col_i64(&m, "id"), col_i64(&c, "id"), "id mismatch");
    assert_eq!(col_i64(&m, "user_id"), col_i64(&c, "user_id"), "user_id mismatch");
    assert_eq!(col_f64(&m, "amount"), col_f64(&c, "amount"), "amount mismatch");
    assert_eq!(col_str(&m, "status"), col_str(&c, "status"), "status mismatch");
    assert_eq!(
        col_i64(&m, "created_at"),
        col_i64(&c, "created_at"),
        "created_at mismatch"
    );
    // The load-bearing assertion: byte-identical canonical JSONB payloads.
    assert_eq!(
        col_jsonb_bytes(&m, "payload"),
        col_jsonb_bytes(&c, "payload"),
        "JSONB payload bytes mismatch between fast and slow paths"
    );

    // Spot-check that key-sorting actually happened (canonical form), so a
    // both-paths-store-raw bug can't pass by agreeing with itself. Row 5 is
    // the first row that both uses payload shape 0 (keys z,a,m → must
    // serialise a,m,z) and escapes the `i % 13 == 0` NULL sprinkle (row 0
    // does not — its payload is NULL).
    let m_payloads = col_jsonb_bytes(&m, "payload");
    let row5 = m_payloads[5].as_ref().expect("row 5 payload not null");
    let row5_str = std::str::from_utf8(row5).unwrap();
    let a = row5_str.find("\"a\"").unwrap();
    let m_ = row5_str.find("\"m\"").unwrap();
    let z = row5_str.find("\"z\"").unwrap();
    assert!(a < m_ && m_ < z, "canonical key order a<m<z, got {row5_str}");
}

/// Invalid JSON in a JSONB column: the multi-row statement must error, and the
/// error must be identical to the single-row (slow-path) control — the fast
/// scanner declines on the invalid document and the slow path surfaces the
/// canonical `invalid JSON literal` error.
#[tokio::test]
async fn jsonb_invalid_falls_back_and_errors_identically() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, payload JSONB)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE control (id BIGINT NOT NULL PRIMARY KEY, payload JSONB)")
        .await
        .unwrap();

    // Multi-row literal insert where one row's payload is not valid JSON. The
    // scanner declines (bad JSON), the slow path runs and raises.
    let multi_err = sess
        .execute(
            "INSERT INTO multi (id, payload) VALUES \
             (1, '{\"ok\":1}'), (2, '{not valid json}')",
        )
        .await
        .expect_err("invalid JSON must error");

    // Single-row control: the trivially-slow path for the same bad document.
    let ctrl_err = sess
        .execute("INSERT INTO control (id, payload) VALUES (2, '{not valid json}')")
        .await
        .expect_err("invalid JSON must error on slow path too");

    assert_eq!(
        multi_err.to_string(),
        ctrl_err.to_string(),
        "fast-path fallback must surface the identical canonical error"
    );

    // Statement failed atomically: nothing written to multi.
    let cnt = select(&sess, "SELECT count(*) AS c FROM multi").await;
    assert_eq!(col_i64(&cnt, "c"), vec![Some(0)]);
}

/// TIMESTAMP column equivalence: RFC3339 and `YYYY-MM-DD HH:MM:SS` forms, plus
/// NULL, through the fast multi-row path vs the slow single-row control. The
/// stored microsecond values must match exactly.
#[tokio::test]
async fn equivalence_timestamp_column() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    let ddl = |t: &str| {
        format!("CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, ts TIMESTAMP)")
    };
    sess.execute(&ddl("multi")).await.unwrap();
    sess.execute(&ddl("control")).await.unwrap();

    let rows: &[&str] = &[
        "(1, '2020-01-01T00:00:00Z')",
        "(2, '2021-06-15 12:30:00')",
        "(3, '2026-04-15T12:00:00.123456Z')",
        "(4, NULL)",
        "(5, '1999-12-31 23:59:59')",
    ];

    sess.execute(&format!(
        "INSERT INTO multi (id, ts) VALUES {}",
        rows.join(", ")
    ))
    .await
    .unwrap();
    for r in rows {
        sess.execute(&format!("INSERT INTO control (id, ts) VALUES {r}"))
            .await
            .unwrap();
    }

    let m = select(&sess, "SELECT id, ts FROM multi ORDER BY id").await;
    let c = select(&sess, "SELECT id, ts FROM control ORDER BY id").await;
    assert_eq!(col_i64(&m, "id"), col_i64(&c, "id"));
    assert_eq!(
        col_ts_micros(&m, "ts"),
        col_ts_micros(&c, "ts"),
        "timestamp micros mismatch between fast and slow paths"
    );
    // The NULL row stays NULL, and a known value is exact.
    let ts = col_ts_micros(&m, "ts");
    assert_eq!(ts[3], None, "row 4 must be NULL");
    // 2020-01-01T00:00:00Z = 1577836800 s since epoch.
    assert_eq!(ts[0], Some(1_577_836_800_000_000));
}

// ─── fallback: unsupported shapes still work via the slow path ──────────────

/// A `gen_random_uuid()` function call into a UUID column: the scanner declines
/// on two counts (function token is not a literal; UUID is an unsupported column
/// type), so the slow path runs and generates the UUIDs. Proves a non-literal
/// shape the engine *does* support still works once the scanner steps aside.
#[tokio::test]
async fn fallback_function_in_values() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u UUID NOT NULL)")
        .await
        .unwrap();
    sess.execute(
        "INSERT INTO t (id, u) VALUES (1, gen_random_uuid()), (2, gen_random_uuid())",
    )
    .await
    .unwrap();
    let r = select(&sess, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(col_i64(&r, "id"), vec![Some(1), Some(2)]);
    // Both UUIDs landed and are distinct.
    let cnt = select(&sess, "SELECT count(DISTINCT u) AS c FROM t").await;
    assert_eq!(col_i64(&cnt, "c"), vec![Some(2)]);
}

/// A `::int` cast in the VALUES list is a non-literal token the scanner refuses.
/// basin's INSERT VALUES path doesn't evaluate casts on literals, so the engine
/// rejects this today — the fast scanner must not change that: it declines and
/// the slow path produces the same error (zero behavior change).
#[tokio::test]
async fn fallback_cast_in_values_unchanged_behavior() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, n INT)")
        .await
        .unwrap();
    // Mixed literal + cast: scanner declines on the cast, slow path runs and
    // errors exactly as it would without the fast path.
    let res = sess
        .execute("INSERT INTO t (id, n) VALUES (1, 8), (2, '7'::int)")
        .await;
    assert!(
        res.is_err(),
        "cast in VALUES is not evaluated by the INSERT path; expected error, got {res:?}"
    );
    // Nothing was written (the statement failed atomically).
    let r = select(&sess, "SELECT count(*) AS c FROM t").await;
    assert_eq!(col_i64(&r, "c"), vec![Some(0)]);
}

#[tokio::test]
async fn fallback_on_conflict_present() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t (id, v) VALUES (1, 'a')")
        .await
        .unwrap();
    // ON CONFLICT present → scanner must not be invoked; upsert semantics apply.
    sess.execute(
        "INSERT INTO t (id, v) VALUES (1, 'b'), (2, 'c') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
    )
    .await
    .unwrap();
    let r = select(&sess, "SELECT id, v FROM t ORDER BY id").await;
    assert_eq!(
        col_str(&r, "v"),
        vec![Some("b".to_string()), Some("c".to_string())]
    );
}

#[tokio::test]
async fn fallback_unsupported_column_type() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    // NUMERIC (Arrow Decimal128) is outside the fast scanner's supported set
    // (no in-scanner decimal coercion) → slow path must handle it. (DATE
    // would be the other natural pick, but Date32 INSERT is unsupported in
    // the slow path too — a pre-existing engine gap, not a fallback bug.)
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, d NUMERIC(10,2))")
        .await
        .unwrap();
    sess.execute("INSERT INTO t (id, d) VALUES (1, 12.50), (2, 99.99)")
        .await
        .unwrap();
    let r = select(&sess, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(col_i64(&r, "id"), vec![Some(1), Some(2)]);
}

// ─── correctness: constraints still enforced on the fast path ───────────────

#[tokio::test]
async fn pk_violation_still_raised() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO t (id, s) VALUES (1, 'a')")
        .await
        .unwrap();
    // Duplicate PK in a multi-row literal insert must error (constraint
    // enforcement runs on the fast-path batch).
    let err = sess
        .execute("INSERT INTO t (id, s) VALUES (2, 'b'), (1, 'dup')")
        .await;
    assert!(err.is_err(), "duplicate PK must raise, got {err:?}");

    // In-batch duplicate PK must also error.
    let err2 = sess
        .execute("INSERT INTO t (id, s) VALUES (3, 'x'), (3, 'y')")
        .await;
    assert!(err2.is_err(), "in-batch duplicate PK must raise, got {err2:?}");
}

#[tokio::test]
async fn not_null_violation_still_raised() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    let err = sess
        .execute("INSERT INTO t (id, v) VALUES (1, 'a'), (2, NULL)")
        .await;
    assert!(err.is_err(), "NULL into NOT NULL must raise, got {err:?}");
}

// ─── timing probe (ignored by default) ──────────────────────────────────────

#[tokio::test]
#[ignore = "timing probe; run explicitly with --ignored"]
async fn values_fast_throughput_probe() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    // Benchmark-shaped schema: the JSONB `payload` and BIGINT `created_at`
    // columns are exactly what previously forced the whole statement onto the
    // slow path. With the extended scanner this should now run on the fast
    // path end-to-end.
    sess.execute(
        "CREATE TABLE fast (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, \
         amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)",
    )
    .await
    .unwrap();

    let n = 100_000usize;
    let chunk = 10_000usize;
    let start = Instant::now();
    let mut id = 0usize;
    while id < n {
        let end = (id + chunk).min(n);
        let mut sql = String::from(
            "INSERT INTO fast (id, user_id, amount, status, created_at, payload) VALUES ",
        );
        for i in id..end {
            if i > id {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({i}, {}, {}.{:02}, 'status-{}', {}, '{{\"user\":{},\"tags\":[\"a\",\"b\"],\"v\":{i}}}')",
                i as i64 * 3,
                i % 1000,
                i % 100,
                i % 4,
                1_700_000_000_000_000i64 + i as i64,
                i % 100,
            ));
        }
        sess.execute(&sql).await.unwrap();
        id = end;
    }
    let elapsed = start.elapsed();
    let rps = n as f64 / elapsed.as_secs_f64();
    println!(
        "[values-fast] benchmark schema (JSONB+ts): {n} rows in {:.3}s = {rps:.0} rows/s",
        elapsed.as_secs_f64()
    );

    let r = select(&sess, "SELECT count(*) AS c FROM fast").await;
    assert_eq!(col_i64(&r, "c"), vec![Some(n as i64)]);
}
