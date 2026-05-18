//! Vortex⇆Parquet differential correctness harness (#161).
//!
//! Builds the SAME schema + SAME data as TWO tables — one explicitly
//! Parquet (`WITH (basin.file_format='parquet')`), one Vortex (the
//! default) — seeded over MANY inserts so each table spans multiple data
//! files, then runs a battery of selective queries (point / range /
//! inequality / IS NULL / compound / aggregate / ORDER BY+LIMIT / full
//! scan) against both and asserts the result sets are byte-identical.
//!
//! This is the safety gate for any file-/chunk-pruning optimisation on
//! Vortex: a wrongly pruned file or chunk shows up immediately as a
//! row/value mismatch vs the Parquet oracle. It is fast (no Postgres) so
//! it stays in the iterative loop.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
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

/// Stringify a result set into a canonical, order-independent form: each
/// row is a `|`-joined cell list, rows sorted. NULL is the sentinel
/// `\0NULL`. Covers the column types this harness uses (Int64, Utf8,
/// Float64, Boolean) and panics loudly on anything else so a silent
/// type drift can't mask a mismatch.
fn normalized_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows: Vec<String> = Vec::new();
    for b in batches {
        for r in 0..b.num_rows() {
            let mut cells: Vec<String> = Vec::with_capacity(b.num_columns());
            for c in 0..b.num_columns() {
                let col = b.column(c);
                let cell = if col.is_null(r) {
                    "\0NULL".to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    a.value(r).to_string()
                } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                    // Stable formatting so Parquet/Vortex float repr can't
                    // diverge spuriously.
                    format!("{:.6}", a.value(r))
                } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                    a.value(r).to_string()
                } else {
                    panic!(
                        "differential harness: unsupported result column type {:?} \
                         (col {c}) — extend normalized_rows",
                        col.data_type()
                    );
                };
                cells.push(cell);
            }
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<String> {
    match sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql:?}: {e:?}"))
    {
        ExecResult::Rows { batches, .. } => normalized_rows(&batches),
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// Create the table with the given DDL suffix (format selector) and seed
/// it identically over `n_batches` separate INSERTs (→ multiple data
/// files, so file-level pruning is genuinely exercised).
async fn build_table(sess: &ProjectSession, table: &str, with_clause: &str) {
    exec(
        sess,
        &format!(
            "CREATE TABLE {table} (id BIGINT, k BIGINT, s TEXT, f DOUBLE, b BOOLEAN){with_clause}"
        ),
    )
    .await;
    // 6 batches × 50 rows = 300 rows, disjoint id ranges per batch.
    for batch in 0..6i64 {
        let mut vals = String::new();
        for i in 0..50i64 {
            let id = batch * 50 + i;
            if !vals.is_empty() {
                vals.push(',');
            }
            // Deterministic, with NULLs in s and b for IS NULL coverage.
            let s = if id % 7 == 0 {
                "NULL".to_string()
            } else {
                format!("'v{}'", id % 13)
            };
            let bb = if id % 5 == 0 {
                "NULL".to_string()
            } else if id % 2 == 0 {
                "true".to_string()
            } else {
                "false".to_string()
            };
            let f = (id as f64) * 1.5 - 10.0;
            vals.push_str(&format!("({id}, {}, {s}, {f}, {bb})", id % 17));
        }
        exec(
            sess,
            &format!("INSERT INTO {table} (id, k, s, f, b) VALUES {vals}"),
        )
        .await;
    }
}

/// The differential battery. Every query must yield byte-identical
/// normalized result sets on the Parquet table and the Vortex table.
#[tokio::test]
async fn vortex_matches_parquet_across_query_battery() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    build_table(&sess, "tp", " WITH (basin.file_format='parquet')").await;
    build_table(&sess, "tv", "").await; // default = Vortex

    // {Q} is substituted with each table name. Mix of shapes that
    // exercise file/chunk pruning, predicate eval, projection,
    // aggregation, ordering, and NULL handling.
    let queries: &[&str] = &[
        "SELECT * FROM {Q}",
        "SELECT id, s FROM {Q}",
        "SELECT * FROM {Q} WHERE id = 137",
        "SELECT * FROM {Q} WHERE id = 999999",
        "SELECT * FROM {Q} WHERE id BETWEEN 120 AND 180",
        "SELECT id, k FROM {Q} WHERE k > 10",
        "SELECT * FROM {Q} WHERE k < 3",
        "SELECT * FROM {Q} WHERE s IS NULL",
        "SELECT * FROM {Q} WHERE s = 'v3'",
        "SELECT * FROM {Q} WHERE b IS NULL",
        "SELECT * FROM {Q} WHERE id BETWEEN 50 AND 250 AND b = true",
        "SELECT COUNT(*), SUM(id), MIN(k), MAX(k) FROM {Q}",
        // Metadata-only aggregate fast path (no WHERE): COUNT(*) and a
        // MIN/MAX-only mix. These exercise `fast_aggregate` directly and
        // must stay byte-identical to the DataFusion oracle (and to each
        // other across Vortex/Parquet).
        "SELECT COUNT(*) FROM {Q}",
        "SELECT MIN(k), MAX(k) FROM {Q}",
        "SELECT COUNT(*), SUM(id) FROM {Q} WHERE id BETWEEN 75 AND 225",
        "SELECT k, COUNT(*) FROM {Q} GROUP BY k",
        "SELECT s, COUNT(*) FROM {Q} WHERE s IS NOT NULL GROUP BY s",
        "SELECT id, f FROM {Q} WHERE f < 0.0",
        "SELECT * FROM {Q} ORDER BY id LIMIT 10",
        "SELECT * FROM {Q} WHERE id >= 200 ORDER BY id DESC LIMIT 5",
    ];

    for q in queries {
        let pq = q.replace("{Q}", "tp");
        let vq = q.replace("{Q}", "tv");
        let pr = rows(&sess, &pq).await;
        let vr = rows(&sess, &vq).await;
        assert_eq!(
            vr,
            pr,
            "Vortex result diverged from Parquet for query: {q}\n\
             parquet rows ({}) != vortex rows ({})\n\
             parquet: {:?}\nvortex:  {:?}",
            pr.len(),
            vr.len(),
            pr,
            vr
        );
    }

    // Mutations must also stay identical (DELETE/UPDATE rewrite paths).
    // Basin DML WHERE atoms are `<col> OP <literal>` — use supported forms
    // (a low-id range delete + a scattered single-value delete).
    for t in ["tp", "tv"] {
        exec(&sess, &format!("DELETE FROM {t} WHERE id < 40")).await;
        exec(&sess, &format!("DELETE FROM {t} WHERE k = 5")).await;
        exec(&sess, &format!("UPDATE {t} SET k = 9999 WHERE id = 150")).await;
        exec(&sess, &format!("UPDATE {t} SET k = 8888 WHERE k = 7")).await;
    }
    for q in &[
        "SELECT COUNT(*), SUM(id), SUM(k) FROM {Q}",
        "SELECT * FROM {Q} WHERE id BETWEEN 90 AND 210",
        "SELECT * FROM {Q} WHERE k = 9999",
    ] {
        let pr = rows(&sess, &q.replace("{Q}", "tp")).await;
        let vr = rows(&sess, &q.replace("{Q}", "tv")).await;
        assert_eq!(
            vr, pr,
            "Vortex diverged from Parquet after mutations for: {q}\n\
             parquet: {:?}\nvortex: {:?}",
            pr, vr
        );
    }
}
