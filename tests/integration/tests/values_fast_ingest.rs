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
//! function-style `CAST(...)`, mismatched or unrecognised `::` suffix casts,
//! ON CONFLICT, unsupported column types) still succeed (or fail identically)
//! via the slow path, and that a PK violation is still raised. The one cast
//! shape the scanner *admits* is a type-matching `::jsonb` / `::timestamp` /
//! `::timestamptz` suffix on a string literal — the exact shape the published
//! bulk-INSERT benchmark sends (`'<json>'::jsonb`) — covered by the
//! suffix-cast equivalence tests below.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    StringArray, TimestampMicrosecondArray,
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

/// Extract a DATE column as raw Arrow `Date32` values (days since the Unix
/// epoch).
fn col_date32(batches: &[RecordBatch], name: &str) -> Vec<Option<i32>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<Date32Array>()
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

/// The published benchmark's exact literal shape: every JSONB payload carries
/// a `::jsonb` suffix cast (`'<json>'::jsonb` — see the bulk-INSERT seed in
/// `compare_postgres_common.rs`). 10k rows through one multi-row INSERT (the
/// fast scanner now admits the type-matching suffix cast) vs the same data
/// WITHOUT casts through a slow-path control (ON CONFLICT forces the decline,
/// as in `equivalence_benchmark_schema_10k_with_jsonb` — and the slow path
/// peels the cast wrapper anyway, so the uncast control is the same data).
/// The JSONB column must come back byte-identical.
#[tokio::test]
async fn equivalence_benchmark_jsonb_suffix_cast_10k() {
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

    // Same payload-shape cycle as the uncast benchmark-schema test: nested
    // objects with out-of-order keys, arrays, unicode, escaped quotes, JSON
    // null — so the canonical encoder is exercised under the cast suffix too.
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
    // `cast` controls whether the payload literal carries the benchmark's
    // `::jsonb` suffix. NULL payloads (every 13th row) stay bare NULL in both
    // statements, exactly as the benchmark would send them.
    let tuple = |i: usize, cast: bool| -> String {
        let payload = if i % 13 == 0 {
            "NULL".to_string()
        } else if cast {
            format!("'{}'::jsonb", payload_for(i))
        } else {
            format!("'{}'", payload_for(i))
        };
        format!(
            "({i}, {}, {}.{:02}, 'status-{}', {}, {payload})",
            i as i64 * 3,
            i % 1000,
            i % 100,
            i % 4,
            1_700_000_000_000_000i64 + i as i64
        )
    };

    // multi: one multi-row statement, every payload suffix-cast — the
    // benchmark shape the fast scanner must now engage on.
    let mut multi_sql = String::from(
        "INSERT INTO multi (id, user_id, amount, status, created_at, payload) VALUES ",
    );
    for i in 0..n {
        if i > 0 {
            multi_sql.push_str(", ");
        }
        multi_sql.push_str(&tuple(i, true));
    }
    sess.execute(&multi_sql).await.unwrap();

    // control: identical data, NO casts, ON CONFLICT DO NOTHING so the
    // scanner is guaranteed not to engage (slow `batch_from_rows` path).
    let mut ctrl_sql = String::from(
        "INSERT INTO control (id, user_id, amount, status, created_at, payload) VALUES ",
    );
    for i in 0..n {
        if i > 0 {
            ctrl_sql.push_str(", ");
        }
        ctrl_sql.push_str(&tuple(i, false));
    }
    ctrl_sql.push_str(" ON CONFLICT (id) DO NOTHING");
    sess.execute(&ctrl_sql).await.unwrap();

    let cols = "id, user_id, amount, status, created_at, payload";
    let m = select(&sess, &format!("SELECT {cols} FROM multi ORDER BY id")).await;
    let c = select(&sess, &format!("SELECT {cols} FROM control ORDER BY id")).await;

    assert_eq!(total_rows(&m), n);
    assert_eq!(total_rows(&c), n);
    assert_eq!(col_i64(&m, "id"), col_i64(&c, "id"), "id mismatch");
    assert_eq!(col_str(&m, "status"), col_str(&c, "status"), "status mismatch");
    // The load-bearing assertion: a `::jsonb`-suffixed literal through the
    // fast path must store byte-identical canonical JSONB to the uncast
    // slow-path control.
    assert_eq!(
        col_jsonb_bytes(&m, "payload"),
        col_jsonb_bytes(&c, "payload"),
        "JSONB payload bytes mismatch between suffix-cast fast path and uncast slow path"
    );

    // Canonical-form spot check (key sort a<m<z on row 5), as in the uncast
    // benchmark-schema test, so both-paths-wrong can't self-agree.
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

    // Rows 6 and 7 carry the `::timestamp` / `::timestamptz` suffix casts the
    // scanner now admits into Timestamp(µs) columns; the single-row control
    // peels the same casts on the slow path, so both paths must agree.
    let rows: &[&str] = &[
        "(1, '2020-01-01T00:00:00Z')",
        "(2, '2021-06-15 12:30:00')",
        "(3, '2026-04-15T12:00:00.123456Z')",
        "(4, NULL)",
        "(5, '1999-12-31 23:59:59')",
        "(6, '2022-03-04T05:06:07Z'::timestamptz)",
        "(7, '2023-07-08 09:10:11'::timestamp)",
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

/// A `::int` cast in the VALUES list is outside the scanner's admitted suffix
/// set (`jsonb`/`timestamp`/`timestamptz` on strings only), so it still
/// declines. basin's INSERT VALUES path doesn't evaluate `::int` casts on
/// literals, so the engine rejects this today — the fast scanner must not
/// change that: it declines and the slow path produces the same error (zero
/// behavior change).
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

/// Function-style `CAST(... AS JSONB)` must STILL decline — only the `::`
/// suffix form on a string literal is admitted. The slow path peels `CAST`
/// wrappers for JSONB columns, so the statement succeeds end-to-end via the
/// slow path, and its stored bytes must match the suffix-cast / bare forms.
#[tokio::test]
async fn fallback_function_style_cast_still_declines() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, payload JSONB)")
        .await
        .unwrap();
    // One CAST(...) cell forces the whole statement onto the slow path; the
    // suffix-cast and bare cells ride along and must coerce identically there.
    sess.execute(
        "INSERT INTO t (id, payload) VALUES \
         (1, CAST('{\"b\":1,\"a\":2}' AS JSONB)), \
         (2, '{\"b\":1,\"a\":2}'::jsonb), \
         (3, '{\"b\":1,\"a\":2}')",
    )
    .await
    .unwrap();
    let r = select(&sess, "SELECT id, payload FROM t ORDER BY id").await;
    assert_eq!(col_i64(&r, "id"), vec![Some(1), Some(2), Some(3)]);
    let payloads = col_jsonb_bytes(&r, "payload");
    assert_eq!(
        payloads[0], payloads[1],
        "CAST(...) and ::jsonb forms must store identical canonical bytes"
    );
    assert_eq!(
        payloads[1], payloads[2],
        "::jsonb and bare-string forms must store identical canonical bytes"
    );
    // Canonical key sort happened (a before b after sorting z,a,m-style input).
    let p = std::str::from_utf8(payloads[0].as_ref().unwrap()).unwrap();
    assert!(
        p.find("\"a\"").unwrap() < p.find("\"b\"").unwrap(),
        "canonical key order a<b, got {p}"
    );
}

/// A mismatched suffix cast — `'2020-01-01'::date` into a DATE column — is
/// outside the admitted tag set, so the scanner declines (twice over: Date32
/// is also an unsupported fast-path column type) and the statement routes to
/// the slow path either way. The slow path's DATE coercion peels the cast, so
/// the INSERT succeeds and round-trips — pinning that the scanner change is
/// behavior-neutral here.
#[tokio::test]
async fn fallback_mismatched_cast_date_column() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, d DATE)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE control (id BIGINT NOT NULL PRIMARY KEY, d DATE)")
        .await
        .unwrap();

    // Multi-row with the `::date` suffix (the shape under test) …
    sess.execute(
        "INSERT INTO multi (id, d) VALUES \
         (1, '2020-01-01'::date), (2, '1999-12-31'::date), (3, NULL)",
    )
    .await
    .unwrap();
    // … and the same data uncast as the control (also slow path).
    sess.execute("INSERT INTO control (id, d) VALUES (1, '2020-01-01'), (2, '1999-12-31'), (3, NULL)")
        .await
        .unwrap();

    let m = select(&sess, "SELECT id, d FROM multi ORDER BY id").await;
    let c = select(&sess, "SELECT id, d FROM control ORDER BY id").await;
    assert_eq!(col_i64(&m, "id"), vec![Some(1), Some(2), Some(3)]);
    assert_eq!(
        col_date32(&m, "d"),
        col_date32(&c, "d"),
        "cast and uncast DATE inserts must round-trip identically"
    );
    // Exact value: 2020-01-01 is 18262 days after the Unix epoch.
    assert_eq!(col_date32(&m, "d"), vec![Some(18262), Some(10956), None]);
}

/// `'{}'::jsonb` into a plain TEXT column: the tag/column mismatch makes the
/// scanner decline, and the slow path ACCEPTS the shape (its JSONB cast
/// coercion peels the wrapper and the value lands as a string). This test
/// pins that end-to-end behavior and that the multi-row statement (scanner
/// eligible, must decline) produces exactly what the single-row control
/// does — declining must never change behavior, whatever that behavior is.
#[tokio::test]
async fn fallback_jsonb_cast_into_text_unchanged_behavior() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE control (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();

    sess.execute("INSERT INTO multi (id, s) VALUES (1, 'plain'), (2, '{}'::jsonb)")
        .await
        .expect("slow path accepts ::jsonb into TEXT");
    sess.execute("INSERT INTO control (id, s) VALUES (1, 'plain')")
        .await
        .unwrap();
    sess.execute("INSERT INTO control (id, s) VALUES (2, '{}'::jsonb)")
        .await
        .expect("single-row slow path accepts ::jsonb into TEXT");

    let m = select(&sess, "SELECT id, s FROM multi ORDER BY id").await;
    let c = select(&sess, "SELECT id, s FROM control ORDER BY id").await;
    assert_eq!(col_i64(&m, "id"), vec![Some(1), Some(2)]);
    assert_eq!(
        col_str(&m, "s"),
        col_str(&c, "s"),
        "declined multi-row statement must match the single-row slow path exactly"
    );
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

// ─── pre-parse fast path: decline shapes must behave byte-identically ───────
//
// The executor now classifies plain literal INSERTs BEFORE either
// whole-statement parser runs (the pre-parse fast path). These tests pin the
// decline contract: every shape the pre-parse classifier/scanner refuses —
// RETURNING, explicit transactions, INSERT…SELECT, data-modifying CTEs,
// multiple statements — must still run through the normal path with exactly
// the pre-existing behaviour.

/// RETURNING after the tuples: the tuple scanner declines (trailing clause),
/// the normal path runs, and the RETURNING batch + stored rows must match a
/// control table filled by the same statement WITHOUT the clause.
#[tokio::test]
async fn preparse_returning_declines_and_works() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE control (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();

    let tuples = "(1, 'a'), (2, 'b''c'), (3, NULL)";
    let ret = sess
        .execute(&format!(
            "INSERT INTO multi (id, s) VALUES {tuples} RETURNING *"
        ))
        .await
        .unwrap();
    let ret_batches = match ret {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("RETURNING must produce rows, got {other:?}"),
    };
    assert_eq!(total_rows(&ret_batches), 3);
    assert_eq!(col_i64(&ret_batches, "id"), vec![Some(1), Some(2), Some(3)]);

    sess.execute(&format!("INSERT INTO control (id, s) VALUES {tuples}"))
        .await
        .unwrap();

    let m = select(&sess, "SELECT id, s FROM multi ORDER BY id").await;
    let c = select(&sess, "SELECT id, s FROM control ORDER BY id").await;
    assert_eq!(col_i64(&m, "id"), col_i64(&c, "id"));
    assert_eq!(col_str(&m, "s"), col_str(&c, "s"));
    assert_eq!(
        col_str(&m, "s"),
        vec![Some("a".to_string()), Some("b'c".to_string()), None]
    );
}

/// Inside an explicit transaction the pre-parse path declines (auto-commit
/// only) and the in-tx buffering path runs; the committed data must match an
/// auto-commit control statement byte-for-byte.
#[tokio::test]
async fn preparse_explicit_transaction_declines_and_works() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE multi (id BIGINT NOT NULL PRIMARY KEY, s TEXT, f DOUBLE PRECISION)")
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE control (id BIGINT NOT NULL PRIMARY KEY, s TEXT, f DOUBLE PRECISION)",
    )
    .await
    .unwrap();

    let tuples = "(1, 'x', 1.5), (2, 'it''s', -2.5e2), (3, NULL, NULL)";
    sess.execute("BEGIN").await.unwrap();
    sess.execute(&format!("INSERT INTO multi (id, s, f) VALUES {tuples}"))
        .await
        .unwrap();
    sess.execute("COMMIT").await.unwrap();

    // Control: same statement auto-commit (engages the pre-parse path).
    sess.execute(&format!("INSERT INTO control (id, s, f) VALUES {tuples}"))
        .await
        .unwrap();

    let m = select(&sess, "SELECT id, s, f FROM multi ORDER BY id").await;
    let c = select(&sess, "SELECT id, s, f FROM control ORDER BY id").await;
    assert_eq!(col_i64(&m, "id"), col_i64(&c, "id"));
    assert_eq!(col_str(&m, "s"), col_str(&c, "s"));
    assert_eq!(col_f64(&m, "f"), col_f64(&c, "f"));
}

/// INSERT…SELECT has no literal VALUES — the classifier declines and the
/// materialise-through-DataFusion path runs unchanged.
#[tokio::test]
async fn preparse_insert_select_declines_and_works() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE src (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("CREATE TABLE dst (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    sess.execute("INSERT INTO src (id, s) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    sess.execute("INSERT INTO dst SELECT id, s FROM src")
        .await
        .unwrap();
    let d = select(&sess, "SELECT id, s FROM dst ORDER BY id").await;
    assert_eq!(col_i64(&d, "id"), vec![Some(1), Some(2), Some(3)]);
    assert_eq!(
        col_str(&d, "s"),
        vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string())
        ]
    );
}

/// A data-modifying CTE (`WITH ins AS (INSERT … RETURNING …) SELECT …`)
/// starts with WITH — the classifier never matches — and the DML-CTE
/// orchestrator runs unchanged.
#[tokio::test]
async fn preparse_dml_cte_declines_and_works() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    let r = select(
        &sess,
        "WITH ins AS (INSERT INTO t VALUES (7) RETURNING id) SELECT * FROM ins",
    )
    .await;
    assert_eq!(col_i64(&r, "id"), vec![Some(7)]);
    let cnt = select(&sess, "SELECT count(*) AS c FROM t").await;
    assert_eq!(col_i64(&cnt, "c"), vec![Some(1)]);
}

/// Two statements in one execute() call: the tuple scanner declines on the
/// bytes after the first `;`, and the normal path's single-statement guard
/// rejects — exactly the pre-existing behaviour. Atomic: nothing written.
#[tokio::test]
async fn preparse_multiple_statements_decline() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;
    sess.execute("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
        .await
        .unwrap();
    let res = sess
        .execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)")
        .await;
    assert!(
        res.is_err(),
        "multi-statement execute must keep erroring, got {res:?}"
    );
    let cnt = select(&sess, "SELECT count(*) AS c FROM t").await;
    assert_eq!(col_i64(&cnt, "c"), vec![Some(0)]);
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
    // slow path. The payload literal carries the `::jsonb` suffix cast so the
    // statement byte-shape mirrors the published bulk-INSERT benchmark
    // (`compare_postgres_common.rs`) exactly. With the extended scanner this
    // runs on the fast path end-to-end, and with the pre-parse classifier the
    // statement additionally skips BOTH whole-statement parses (libpg_query +
    // sqlparser) that previously ran before the scanner engaged.
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
                "({i}, {}, {}.{:02}, 'status-{}', {}, '{{\"user\":{},\"tags\":[\"a\",\"b\"],\"v\":{i}}}'::jsonb)",
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
    let per_stmt_ms = elapsed.as_secs_f64() * 1000.0 / (n as f64 / chunk as f64);
    println!(
        "[values-fast] pre-parse fast path, benchmark schema (JSONB ::jsonb suffix + ts): \
         {n} rows in {:.3}s = {rps:.0} rows/s ({per_stmt_ms:.1}ms per {chunk}-row statement)",
        elapsed.as_secs_f64()
    );

    let r = select(&sess, "SELECT count(*) AS c FROM fast").await;
    assert_eq!(col_i64(&r, "c"), vec![Some(n as i64)]);
}

// ─── prepared-statement variants (extended-protocol seam) ───────────────────
//
// Real ORMs reach the engine through Parse/Bind/Execute, not simple-query.
// These tests drive the SAME engine seam the pgwire extended path uses
// (`ProjectSession::prepare` → `bind` → `execute_bound`) and pin two shapes:
//
// * Shape 1 — prepared LITERAL multi-row INSERT (zero parameters): must equal
//   the identical statement through the simple `execute` path. At prepare
//   time the classifier verdict short-circuits AST caching; each Execute
//   re-runs the values_fast scanner on the stored SQL.
// * Shape 2 — PARAMETERIZED INSERT executed N times: must equal N simple
//   INSERTs carrying the equivalent literals (the bind-direct batch builder
//   produces byte-identical Arrow via the shared ColAcc accumulators).
//
// Decline shapes (ON CONFLICT, explicit transaction, identity columns,
// unsupported param kinds) must still work via the AST/text fallback.

use basin_engine::ScalarParam;

/// Shape 1: a 10k-row literal INSERT prepared once and Executed through the
/// extended seam equals the same statement through the simple path.
#[tokio::test]
async fn prepared_literal_10k_equals_simple_path() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    for t in ["simple_t", "prepared_t"] {
        sess.execute(&format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, val DOUBLE PRECISION, \
             name TEXT, flag BOOLEAN, payload JSONB)"
        ))
        .await
        .unwrap();
    }

    let n = 10_000usize;
    let tail = |_table: &str| {
        let mut s = String::new();
        for i in 0..n {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(&format!(
                "({i}, {}.{:02}, 'it''s-{i}', {}, '{{\"k\":{i}}}'::jsonb)",
                i % 1000,
                i % 100,
                i % 2 == 0,
            ));
        }
        s
    };

    // Simple path.
    sess.execute(&format!(
        "INSERT INTO simple_t (id, val, name, flag, payload) VALUES {}",
        tail("simple_t")
    ))
    .await
    .unwrap();

    // Prepared / extended path: prepare once, Execute once.
    let (handle, schema) = sess
        .prepare(&format!(
            "INSERT INTO prepared_t (id, val, name, flag, payload) VALUES {}",
            tail("prepared_t")
        ))
        .await
        .unwrap();
    assert!(
        schema.param_types.is_empty(),
        "literal statement must report zero parameters"
    );
    let bound = sess.bind(&handle, vec![]).await.unwrap();
    match sess.execute_bound(bound).await.unwrap() {
        ExecResult::Empty { tag } => assert_eq!(tag, format!("INSERT 0 {n}")),
        other => panic!("expected INSERT tag, got {other:?}"),
    }

    // Full equivalence, column by column.
    let a = select(
        &sess,
        "SELECT id, val, name, flag, payload FROM simple_t ORDER BY id",
    )
    .await;
    let b = select(
        &sess,
        "SELECT id, val, name, flag, payload FROM prepared_t ORDER BY id",
    )
    .await;
    assert_eq!(total_rows(&a), n);
    assert_eq!(total_rows(&b), n);
    assert_eq!(col_i64(&a, "id"), col_i64(&b, "id"));
    assert_eq!(col_f64(&a, "val"), col_f64(&b, "val"));
    assert_eq!(col_str(&a, "name"), col_str(&b, "name"));
    assert_eq!(col_bool(&a, "flag"), col_bool(&b, "flag"));
    assert_eq!(col_jsonb_bytes(&a, "payload"), col_jsonb_bytes(&b, "payload"));

    // Re-Executing the same prepared literal statement re-inserts the same
    // rows — which must hit the PK constraint, exactly like re-sending the
    // simple statement would.
    let bound = sess.bind(&handle, vec![]).await.unwrap();
    assert!(
        sess.execute_bound(bound).await.is_err(),
        "duplicate PK re-execute must fail like the simple path"
    );
    let c = select(&sess, "SELECT count(*) AS c FROM prepared_t").await;
    assert_eq!(col_i64(&c, "c"), vec![Some(n as i64)]);
}

/// Shape 2: a parameterized INSERT (the dominant ORM shape) executed N times
/// through the prepared seam equals N simple INSERTs with equivalent
/// literals — including NULLs, apostrophes, JSONB and timestamp params.
#[tokio::test]
async fn prepared_param_inserts_equal_simple_inserts() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    for t in ["simple_p", "prepared_p"] {
        sess.execute(&format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, name TEXT, \
             score DOUBLE PRECISION, ok BOOLEAN, doc JSONB, ts TIMESTAMPTZ)"
        ))
        .await
        .unwrap();
    }

    let n = 50i64;
    let ts_for = |i: i64| format!("2026-01-02 03:04:{:02}.123456", i % 60);

    // Simple path: one literal INSERT per row.
    for i in 0..n {
        let name = if i % 7 == 0 {
            "NULL".to_string()
        } else {
            format!("'it''s-{i}'")
        };
        let score = if i % 5 == 0 {
            "NULL".to_string()
        } else {
            format!("{i}.25")
        };
        sess.execute(&format!(
            "INSERT INTO simple_p (id, name, score, ok, doc, ts) VALUES \
             ({i}, {name}, {score}, {}, '{{\"k\":{i},\"s\":\"v-{i}\"}}', '{}')",
            i % 3 == 0,
            ts_for(i),
        ))
        .await
        .unwrap();
    }

    // Prepared path: prepare ONCE, bind+execute per row with typed params.
    let (handle, _schema) = sess
        .prepare("INSERT INTO prepared_p (id, name, score, ok, doc, ts) VALUES ($1, $2, $3, $4, $5, $6)")
        .await
        .unwrap();
    for i in 0..n {
        let name = if i % 7 == 0 {
            ScalarParam::Null
        } else {
            ScalarParam::Text(format!("it's-{i}"))
        };
        let score = if i % 5 == 0 {
            ScalarParam::Null
        } else {
            ScalarParam::Float8(i as f64 + 0.25)
        };
        let bound = sess
            .bind(
                &handle,
                vec![
                    ScalarParam::Int8(i),
                    name,
                    score,
                    ScalarParam::Bool(i % 3 == 0),
                    ScalarParam::Text(format!("{{\"k\":{i},\"s\":\"v-{i}\"}}")),
                    ScalarParam::Text(ts_for(i)),
                ],
            )
            .await
            .unwrap();
        match sess.execute_bound(bound).await.unwrap() {
            ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 1"),
            other => panic!("expected INSERT tag, got {other:?}"),
        }
    }

    let a = select(
        &sess,
        "SELECT id, name, score, ok, doc, ts FROM simple_p ORDER BY id",
    )
    .await;
    let b = select(
        &sess,
        "SELECT id, name, score, ok, doc, ts FROM prepared_p ORDER BY id",
    )
    .await;
    assert_eq!(total_rows(&a), n as usize);
    assert_eq!(total_rows(&b), n as usize);
    assert_eq!(col_i64(&a, "id"), col_i64(&b, "id"));
    assert_eq!(col_str(&a, "name"), col_str(&b, "name"));
    assert_eq!(col_f64(&a, "score"), col_f64(&b, "score"));
    assert_eq!(col_bool(&a, "ok"), col_bool(&b, "ok"));
    assert_eq!(col_jsonb_bytes(&a, "doc"), col_jsonb_bytes(&b, "doc"));
    assert_eq!(col_ts_micros(&a, "ts"), col_ts_micros(&b, "ts"));
}

/// Multi-row parameterized template (some ORMs batch with one statement of
/// many placeholder tuples): equivalence vs the literal simple path.
#[tokio::test]
async fn prepared_param_multi_row_template_equivalence() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    for t in ["simple_m", "prepared_m"] {
        sess.execute(&format!(
            "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, s TEXT)"
        ))
        .await
        .unwrap();
    }

    sess.execute("INSERT INTO simple_m (id, s) VALUES (1, 'a'), (2, 'b'), (3, NULL)")
        .await
        .unwrap();

    let (handle, _schema) = sess
        .prepare("INSERT INTO prepared_m (id, s) VALUES ($1, $2), ($3, $4), ($5, $6)")
        .await
        .unwrap();
    let bound = sess
        .bind(
            &handle,
            vec![
                ScalarParam::Int8(1),
                ScalarParam::Text("a".into()),
                ScalarParam::Int8(2),
                ScalarParam::Text("b".into()),
                ScalarParam::Int8(3),
                ScalarParam::Null,
            ],
        )
        .await
        .unwrap();
    match sess.execute_bound(bound).await.unwrap() {
        ExecResult::Empty { tag } => assert_eq!(tag, "INSERT 0 3"),
        other => panic!("expected INSERT tag, got {other:?}"),
    }

    let a = select(&sess, "SELECT id, s FROM simple_m ORDER BY id").await;
    let b = select(&sess, "SELECT id, s FROM prepared_m ORDER BY id").await;
    assert_eq!(col_i64(&a, "id"), col_i64(&b, "id"));
    assert_eq!(col_str(&a, "s"), col_str(&b, "s"));
}

/// Decline shapes must still work via the AST/text fallback routes:
/// ON CONFLICT (no bind-direct plan), an explicit transaction (execute-time
/// decline), and an identity column (column-eligibility decline).
#[tokio::test]
async fn prepared_param_declines_still_work() {
    let (_dir, eng) = open_engine().await;
    let sess = session(&eng).await;

    // ── ON CONFLICT DO UPDATE: upsert semantics preserved ───────────────────
    sess.execute("CREATE TABLE up (id BIGINT NOT NULL PRIMARY KEY, v TEXT NOT NULL)")
        .await
        .unwrap();
    let (h_up, _) = sess
        .prepare(
            "INSERT INTO up (id, v) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        )
        .await
        .unwrap();
    for (id, v) in [(1i64, "a"), (1, "b"), (2, "c")] {
        let bound = sess
            .bind(&h_up, vec![ScalarParam::Int8(id), ScalarParam::Text(v.into())])
            .await
            .unwrap();
        sess.execute_bound(bound).await.unwrap();
    }
    let r = select(&sess, "SELECT id, v FROM up ORDER BY id").await;
    assert_eq!(col_i64(&r, "id"), vec![Some(1), Some(2)]);
    assert_eq!(
        col_str(&r, "v"),
        vec![Some("b".to_string()), Some("c".to_string())],
        "upsert must have replaced the conflicting row"
    );

    // ── Explicit transaction: bind-direct declines, in-tx buffering applies ─
    sess.execute("CREATE TABLE txt (id BIGINT NOT NULL PRIMARY KEY, s TEXT)")
        .await
        .unwrap();
    let (h_tx, _) = sess
        .prepare("INSERT INTO txt (id, s) VALUES ($1, $2)")
        .await
        .unwrap();
    sess.execute("BEGIN").await.unwrap();
    for i in 0..3i64 {
        let bound = sess
            .bind(
                &h_tx,
                vec![ScalarParam::Int8(i), ScalarParam::Text(format!("tx-{i}"))],
            )
            .await
            .unwrap();
        sess.execute_bound(bound).await.unwrap();
    }
    sess.execute("ROLLBACK").await.unwrap();
    let r = select(&sess, "SELECT count(*) AS c FROM txt").await;
    assert_eq!(
        col_i64(&r, "c"),
        vec![Some(0)],
        "ROLLBACK must discard in-tx prepared inserts (no fast-path leak)"
    );
    sess.execute("BEGIN").await.unwrap();
    for i in 0..3i64 {
        let bound = sess
            .bind(
                &h_tx,
                vec![ScalarParam::Int8(i), ScalarParam::Text(format!("tx-{i}"))],
            )
            .await
            .unwrap();
        sess.execute_bound(bound).await.unwrap();
    }
    sess.execute("COMMIT").await.unwrap();
    let r = select(&sess, "SELECT count(*) AS c FROM txt").await;
    assert_eq!(col_i64(&r, "c"), vec![Some(3)]);

    // ── Identity column: server-side fill must still run ────────────────────
    sess.execute(
        "CREATE TABLE ident (id BIGINT GENERATED ALWAYS AS IDENTITY, name TEXT NOT NULL)",
    )
    .await
    .unwrap();
    let (h_id, _) = sess
        .prepare("INSERT INTO ident (name) VALUES ($1)")
        .await
        .unwrap();
    for name in ["x", "y"] {
        let bound = sess
            .bind(&h_id, vec![ScalarParam::Text(name.into())])
            .await
            .unwrap();
        sess.execute_bound(bound).await.unwrap();
    }
    let r = select(&sess, "SELECT id, name FROM ident ORDER BY id").await;
    assert_eq!(
        col_i64(&r, "id"),
        vec![Some(1), Some(2)],
        "identity sequence must have filled ids on the fallback path"
    );
    assert_eq!(
        col_str(&r, "name"),
        vec![Some("x".to_string()), Some("y".to_string())]
    );
}
