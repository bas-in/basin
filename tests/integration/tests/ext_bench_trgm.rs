//! EXTENSIONS BENCHMARK SUITE — `pg_trgm` family performance card.
//!
//! Deliberately SEPARATE from the `compare_postgres_*` family. Own artifact
//! (`benchmark/data/ext_bench_trgm.json`), own runner
//! (`benchmark/run/extensions-suite.sh`). It exercises ONLY the trigram
//! surface Basin actually implements today (verified against
//! `trgm_sql_conformance.rs`):
//!
//!   * `similarity(a, b)` scalar UDF
//!   * `word_similarity(a, b)` scalar UDF
//!   * `a % b`   → `similarity(a,b) >= pg_trgm.similarity_threshold`
//!   * `a <% b`  → `word_similarity(a,b) >= pg_trgm.word_similarity_threshold`
//!   * `a <-> b` → `1.0 - similarity(a,b)` (trigram distance; ORDER BY nearest)
//!   * `SET / SHOW pg_trgm.similarity_threshold`
//!
//! Basin now builds a `gin_trgm_ops` trigram GIN index, so `%` filter queries
//! (T2/T3) are index-backed: the trigram posting list prunes to candidate
//! files/rows and the `similarity()` predicate rechecks. The card records
//! `basin_uses_index` (runtime-probed against the live posting registry) so the
//! head-to-head is honest: PG WITH a `gin_trgm_ops` index vs Basin WITH its
//! trigram GIN, plus PG WITHOUT the index for the apples-to-apples scan
//! comparison. `<->` ORDER BY (T4) stays a sequential scan on Basin
//! (index-assisted trigram kNN is not implemented).
//!
//! ## Shapes (cards)
//!   T1 similarity() projection over N strings (pure scalar throughput)
//!   T2 `%` filter — rare needle   (low selectivity)
//!   T3 `%` filter — common needle (high selectivity)
//!   T4 `<->` ORDER BY LIMIT 10    (nearest-name search — THE customer shape)
//!   T5 word_similarity `<%` filter
//!   T6 threshold-GUC sweep effect on `%` (0.3 vs 0.6 selectivity + latency)
//!
//! ## Excluded as unimplemented
//!   * Trigram-GIN index on the Basin side — not built; recorded
//!     `basin_uses_index:false` in every shape. (PG gets `gin_trgm_ops` when
//!     the extension is installable.)
//!   * `show_trgm()` micro-bench — correctness-only surface, not a perf shape.
//!
//! ## PG head-to-head probe
//! `CREATE EXTENSION IF NOT EXISTS pg_trgm` inside a try. On failure we record
//! `pg_available:false` and emit Basin-only timings (NEVER fail the card).
//! When available we build BOTH a `gin_trgm_ops` GIN index table and an
//! un-indexed twin so the artifact carries the indexed AND the scan number.
//!
//! ## Env knobs
//!   * `BASIN_EXT_BENCH_ROWS`         — string count (default 100_000)
//!   * `BASIN_EXT_BENCH_TRGM_SAMPLES` — samples per shape (default 25)

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Array, Int64Array};
use basin_common::ProjectId;
use basin_engine::ExecResult;
use serde_json::json;
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, try_connect, SchemaGuard};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Atomic write to `benchmark/data/<file>` — a SEPARATE artifact family from
/// `compare_postgres*.json`. Mirrors the atomic tmp+rename emit used by
/// `fts_compare.rs::write_artifact` but lands `ext_bench_*` files.
fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    if let Ok(bytes) = serde_json::to_vec_pretty(value) {
        let _ = std::fs::write(&tmp, &bytes);
        let _ = std::fs::rename(&tmp, &path);
        eprintln!("[ext_bench_trgm] artifact written: {}", path.display());
    }
}

fn opt_ms(v: Option<f64>) -> serde_json::Value {
    match v {
        Some(ms) => json!(ms),
        None => serde_json::Value::Null,
    }
}

fn ratio(b: Option<f64>, p: Option<f64>) -> serde_json::Value {
    match (b, p) {
        (Some(bv), Some(pv)) if pv > 1e-9 => json!(bv / pv),
        _ => serde_json::Value::Null,
    }
}

fn basin_count(res: &ExecResult) -> i64 {
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut total = 0i64;
            for b in batches {
                if let Some(a) = b.column(0).as_any().downcast_ref::<Int64Array>() {
                    for i in 0..a.len() {
                        total = total.saturating_add(a.value(i));
                    }
                }
            }
            total
        }
        ExecResult::Empty { .. } => 0,
    }
}

fn basin_rows(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Deterministic synthetic "customer name". A small alphabet of first/last
/// roots is combined so the trigram space is realistic (shared trigrams across
/// neighbours) but every row is distinct. ~1/200 rows contain the rare needle
/// "zephyrine" and ~1/8 contain the common root "smith" — gives a meaningful
/// selectivity spread for T2/T3.
fn name_for(i: usize) -> String {
    const FIRST: &[&str] = &[
        "alice", "alyce", "bob", "carol", "dave", "erin", "frank", "grace", "heidi", "ivan",
        "judy", "mallory", "olivia", "peggy", "trent", "victor",
    ];
    const LAST: &[&str] = &[
        "smith", "smyth", "jones", "brown", "taylor", "wilson", "davies", "evans",
    ];
    let first = FIRST[i % FIRST.len()];
    let last = LAST[(i / FIRST.len()) % LAST.len()];
    // Inject the rare needle ~every 200th row.
    if i % 200 == 0 {
        format!("zephyrine {first} {last}{}", i)
    } else {
        format!("{first} {last}{}", i % 5000)
    }
}

const RARE_NEEDLE: &str = "zephyrine";
const COMMON_NEEDLE: &str = "smith";
const WORD_NEEDLE: &str = "smyth";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "extensions benchmark: needs idle box (+ live PG with extension for head-to-head); run via benchmark/run/extensions-suite.sh"]
async fn ext_bench_trgm() {
    let rows = env_usize("BASIN_EXT_BENCH_ROWS", 100_000);
    let samples = env_usize("BASIN_EXT_BENCH_TRGM_SAMPLES", 25);
    eprintln!("[ext_bench_trgm] config: rows={rows} samples={samples}");

    // ── Basin setup ──────────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute("CREATE TABLE people (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .unwrap();

    let batch = 5_000usize;
    let mut off = 0usize;
    eprintln!("[ext_bench_trgm] seeding {rows} names ...");
    while off < rows {
        let hi = (off + batch).min(rows);
        let mut stmt = String::with_capacity((hi - off) * 48);
        stmt.push_str("INSERT INTO people VALUES ");
        for k in off..hi {
            if k > off {
                stmt.push(',');
            }
            let nm = name_for(k).replace('\'', "''");
            stmt.push_str(&format!("({k},'{nm}')"));
        }
        sess.execute(&stmt).await.expect("basin trgm seed");
        off = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    eprintln!("[ext_bench_trgm] basin seed complete");

    // Build the trigram GIN index so `%` queries can be index-backed. CREATE
    // INDEX backfills the cold files' trigram postings (same extraction as
    // similarity()). Best-effort: a failure leaves the sequential-scan path.
    let trgm_index_built = sess
        .execute("CREATE INDEX people_trgm ON people USING gin (name gin_trgm_ops)")
        .await
        .is_ok();

    // Runtime probe: did the index actually ENGAGE? The file-level trigram
    // posting list seals every live file it backfilled into the registry's
    // indexed-files set. A non-empty set means the index is resident and the
    // `%` probe path can prune — record it per shape (PG-style `basin_uses_index`
    // truth), keeping the sequential-scan fallback honest when it is empty.
    let basin_uses_index = trgm_index_built && {
        use basin_common::TableName;
        let table = TableName::new("people").unwrap();
        !instance
            .engine
            .gin_index_registry_for_test()
            .indexed_files_for(&instance.project, &table, "name")
            .is_empty()
    };
    eprintln!("[ext_bench_trgm] trgm index built={trgm_index_built} engaged={basin_uses_index}");

    // ── SQL shapes ─────────────────────────────────────────────────────────
    let t1_sql = "SELECT count(*) FROM people WHERE similarity(name, 'alice smith') > 0.0";
    let t2_sql = format!("SELECT count(*) FROM people WHERE name % '{RARE_NEEDLE}'");
    let t3_sql = format!("SELECT count(*) FROM people WHERE name % '{COMMON_NEEDLE}'");
    let t4_sql = format!("SELECT id FROM people ORDER BY name <-> 'alyce smyth' LIMIT 10");
    let t5_sql = format!("SELECT count(*) FROM people WHERE name <% '{WORD_NEEDLE}'");

    // A small generic timed-median runner that tolerates an unsupported shape.
    async fn basin_p50(
        sess: &basin_engine::ProjectSession,
        sql: &str,
        n: usize,
    ) -> (Option<f64>, Option<ExecResult>) {
        match sess.execute(sql).await {
            Ok(warm) => {
                let mut s = Vec::with_capacity(n);
                for _ in 0..n {
                    let t = Instant::now();
                    let _ = sess.execute(sql).await.expect("basin trgm shape");
                    s.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                (Some(median(&s)), Some(warm))
            }
            Err(e) => {
                eprintln!("[ext_bench_trgm] shape unsupported: {sql} :: {e}");
                (None, None)
            }
        }
    }

    let (t1_b, t1_res) = basin_p50(&sess, t1_sql, samples).await;
    let t1_count = t1_res.as_ref().map(basin_count).unwrap_or(0);
    let (t2_b, t2_res) = basin_p50(&sess, &t2_sql, samples).await;
    let t2_count = t2_res.as_ref().map(basin_count).unwrap_or(0);
    let (t3_b, t3_res) = basin_p50(&sess, &t3_sql, samples).await;
    let t3_count = t3_res.as_ref().map(basin_count).unwrap_or(0);
    let (t4_b, t4_res) = basin_p50(&sess, &t4_sql, samples).await;
    let t4_rows = t4_res.as_ref().map(basin_rows).unwrap_or(0);
    let (t5_b, t5_res) = basin_p50(&sess, &t5_sql, samples).await;
    let t5_count = t5_res.as_ref().map(basin_count).unwrap_or(0);

    // T6 — threshold GUC sweep: at 0.3 (default) vs 0.6, on the common needle.
    sess.execute("SET pg_trgm.similarity_threshold = 0.3")
        .await
        .ok();
    let (t6_lo_b, t6_lo_res) = basin_p50(&sess, &t3_sql, samples).await;
    let t6_lo_count = t6_lo_res.as_ref().map(basin_count).unwrap_or(0);
    sess.execute("SET pg_trgm.similarity_threshold = 0.6")
        .await
        .ok();
    let (t6_hi_b, t6_hi_res) = basin_p50(&sess, &t3_sql, samples).await;
    let t6_hi_count = t6_hi_res.as_ref().map(basin_count).unwrap_or(0);
    // restore
    sess.execute("SET pg_trgm.similarity_threshold = 0.3")
        .await
        .ok();

    println!(
        "[ext_bench_trgm] Basin: T1={:?} T2(rare={t2_count})={:?} T3(common={t3_count})={:?} \
         T4(nn,rows={t4_rows})={:?} T5(word={t5_count})={:?} T6 lo({t6_lo_count})={:?} hi({t6_hi_count})={:?}",
        t1_b, t2_b, t3_b, t4_b, t5_b, t6_lo_b, t6_hi_b
    );

    // ── PG twin (gin_trgm_ops indexed + un-indexed scan) ────────────────────
    let mut pg_available = false;
    let mut pg_ext_ok = false;
    // Per-shape PG timings: (idx, scan).
    let mut pg_t2 = (None, None);
    let mut pg_t3 = (None, None);
    let mut pg_t4 = (None, None);
    let mut pg_t5 = (None, None);
    let mut pg_t1: Option<f64> = None;
    let mut pg_t2_count = 0i64;
    let mut pg_t3_count = 0i64;

    if let Some((pg, cs)) = try_connect().await {
        pg_available = true;
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_ext_trgm_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .ok();
        pg.simple_query("SET work_mem = '16MB'").await.ok();

        pg_ext_ok = pg
            .simple_query("CREATE EXTENSION IF NOT EXISTS pg_trgm")
            .await
            .is_ok();
        eprintln!(
            "[ext_bench_trgm] PG pg_trgm extension: {}",
            if pg_ext_ok { "ok" } else { "UNAVAILABLE" }
        );

        // Indexed table + un-indexed twin, same data.
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.people_idx (id BIGINT PRIMARY KEY, name TEXT NOT NULL)"
        ))
        .await
        .ok();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.people_scan (id BIGINT PRIMARY KEY, name TEXT NOT NULL)"
        ))
        .await
        .ok();

        let mut po = 0usize;
        while po < rows {
            let hi = (po + batch).min(rows);
            let mut v = String::with_capacity((hi - po) * 48);
            for k in po..hi {
                if k > po {
                    v.push(',');
                }
                let nm = name_for(k).replace('\'', "''");
                v.push_str(&format!("({k},'{nm}')"));
            }
            pg.simple_query(&format!("INSERT INTO {schema}.people_idx VALUES {v}"))
                .await
                .ok();
            pg.simple_query(&format!("INSERT INTO {schema}.people_scan VALUES {v}"))
                .await
                .ok();
            po = hi;
        }

        if pg_ext_ok {
            pg.simple_query(&format!(
                "CREATE INDEX people_trgm_gin ON {schema}.people_idx USING gin (name gin_trgm_ops)"
            ))
            .await
            .ok();
        }
        pg.simple_query(&format!("ANALYZE {schema}.people_idx"))
            .await
            .ok();
        pg.simple_query(&format!("ANALYZE {schema}.people_scan"))
            .await
            .ok();

        // Median over `samples` EXPLAIN ANALYZE runs.
        async fn pg_p50(pg: &tokio_postgres::Client, inner: &str, n: usize) -> Option<f64> {
            let _ = pg
                .simple_query(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {inner}"))
                .await;
            let mut s = Vec::with_capacity(n);
            for _ in 0..n {
                if let Ok(rs) = pg
                    .simple_query(&format!("EXPLAIN (ANALYZE, FORMAT TEXT) {inner}"))
                    .await
                {
                    for m in &rs {
                        if let SimpleQueryMessage::Row(r) = m {
                            if let Some(line) = r.get(0) {
                                if let Some(idx) = line.find("Execution Time:") {
                                    let after = line[idx + "Execution Time:".len()..].trim();
                                    if let Some(end) = after.find(' ') {
                                        if let Ok(v) = after[..end].parse::<f64>() {
                                            s.push(v);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if s.is_empty() {
                None
            } else {
                Some(median(&s))
            }
        }

        async fn pg_count(pg: &tokio_postgres::Client, sql: &str) -> i64 {
            pg.query_one(sql, &[])
                .await
                .map(|r| r.get::<usize, i64>(0))
                .unwrap_or(0)
        }

        if pg_ext_ok {
            // PG `set_limit(0.3)` to align with Basin's default similarity_threshold.
            pg.simple_query("SELECT set_limit(0.3)").await.ok();

            // Counts (correctness reference) from the indexed table.
            pg_t2_count = pg_count(
                &pg,
                &format!(
                    "SELECT count(*)::bigint FROM {schema}.people_idx WHERE name % '{RARE_NEEDLE}'"
                ),
            )
            .await;
            pg_t3_count = pg_count(&pg,
                &format!("SELECT count(*)::bigint FROM {schema}.people_idx WHERE name % '{COMMON_NEEDLE}'")).await;

            // T1 similarity projection (no index involved — pure scalar).
            pg_t1 = pg_p50(&pg,
                &format!("SELECT count(*) FROM {schema}.people_scan WHERE similarity(name, 'alice smith') > 0.0"),
                samples).await;

            // T2 rare % — indexed and scan.
            pg_t2 = (
                pg_p50(
                    &pg,
                    &format!("SELECT id FROM {schema}.people_idx WHERE name % '{RARE_NEEDLE}'"),
                    samples,
                )
                .await,
                pg_p50(
                    &pg,
                    &format!("SELECT id FROM {schema}.people_scan WHERE name % '{RARE_NEEDLE}'"),
                    samples,
                )
                .await,
            );
            // T3 common % — indexed and scan.
            pg_t3 = (
                pg_p50(
                    &pg,
                    &format!("SELECT id FROM {schema}.people_idx WHERE name % '{COMMON_NEEDLE}'"),
                    samples,
                )
                .await,
                pg_p50(
                    &pg,
                    &format!("SELECT id FROM {schema}.people_scan WHERE name % '{COMMON_NEEDLE}'"),
                    samples,
                )
                .await,
            );
            // T4 nearest-name <-> (gin_trgm_ops supports <-> ordering with the index).
            pg_t4 = (
                pg_p50(&pg, &format!("SELECT id FROM {schema}.people_idx ORDER BY name <-> 'alyce smyth' LIMIT 10"), samples).await,
                pg_p50(&pg, &format!("SELECT id FROM {schema}.people_scan ORDER BY name <-> 'alyce smyth' LIMIT 10"), samples).await,
            );
            // T5 word-similarity <%.
            pg_t5 = (
                pg_p50(
                    &pg,
                    &format!("SELECT id FROM {schema}.people_idx WHERE name <% '{WORD_NEEDLE}'"),
                    samples,
                )
                .await,
                pg_p50(
                    &pg,
                    &format!("SELECT id FROM {schema}.people_scan WHERE name <% '{WORD_NEEDLE}'"),
                    samples,
                )
                .await,
            );
        }

        let _ = pg
            .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await;
        std::mem::forget(_guard);
    } else {
        eprintln!("[ext_bench_trgm] PG unavailable — Basin-only card");
    }

    // ── Correctness cross-check (counts must match when PG available + ext) ──
    if pg_ext_ok {
        if t2_count > 0 && pg_t2_count > 0 {
            assert_eq!(
                t2_count, pg_t2_count,
                "T2 rare-needle % count mismatch: basin {t2_count} != pg {pg_t2_count}"
            );
        }
        if t3_count > 0 && pg_t3_count > 0 {
            assert_eq!(
                t3_count, pg_t3_count,
                "T3 common-needle % count mismatch: basin {t3_count} != pg {pg_t3_count}"
            );
        }
    }

    // ── Emit artifact ────────────────────────────────────────────────────────
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "ext_bench_trgm.json",
        &json!({
            "card": "ext_bench_trgm",
            "family": "pg_trgm",
            "generated_at": format!("@{ts}"),
            "pg_available": pg_available,
            "pg_extension_available": pg_ext_ok,
            "basin_uses_index": basin_uses_index,
            "config": {
                "rows": rows,
                "samples": samples,
                "rare_needle": RARE_NEEDLE,
                "common_needle": COMMON_NEEDLE,
                "word_needle": WORD_NEEDLE,
            },
            "shapes": [
                {
                    "label": "T1: similarity() projection over N strings",
                    "basin_p50_ms": opt_ms(t1_b),
                    "pg_p50_ms": opt_ms(pg_t1),
                    "basin_over_pg": ratio(t1_b, pg_t1),
                    "basin_match_count": t1_count,
                },
                {
                    "label": "T2: % filter — rare needle (low selectivity)",
                    "basin_p50_ms": opt_ms(t2_b),
                    "basin_uses_index": basin_uses_index,
                    "pg_gin_p50_ms": opt_ms(pg_t2.0),
                    "pg_scan_p50_ms": opt_ms(pg_t2.1),
                    "basin_over_pg_gin": ratio(t2_b, pg_t2.0),
                    "basin_over_pg_scan": ratio(t2_b, pg_t2.1),
                    "basin_hits": t2_count,
                    "pg_hits": pg_t2_count,
                },
                {
                    "label": "T3: % filter — common needle (high selectivity)",
                    "basin_p50_ms": opt_ms(t3_b),
                    "basin_uses_index": basin_uses_index,
                    "pg_gin_p50_ms": opt_ms(pg_t3.0),
                    "pg_scan_p50_ms": opt_ms(pg_t3.1),
                    "basin_over_pg_gin": ratio(t3_b, pg_t3.0),
                    "basin_over_pg_scan": ratio(t3_b, pg_t3.1),
                    "basin_hits": t3_count,
                    "pg_hits": pg_t3_count,
                },
                {
                    "label": "T4: <-> ORDER BY LIMIT 10 (nearest-name search)",
                    "basin_p50_ms": opt_ms(t4_b),
                    "pg_gin_p50_ms": opt_ms(pg_t4.0),
                    "pg_scan_p50_ms": opt_ms(pg_t4.1),
                    "basin_over_pg_gin": ratio(t4_b, pg_t4.0),
                    "basin_over_pg_scan": ratio(t4_b, pg_t4.1),
                    "basin_rows": t4_rows,
                },
                {
                    "label": "T5: word_similarity <% filter",
                    "basin_p50_ms": opt_ms(t5_b),
                    "pg_gin_p50_ms": opt_ms(pg_t5.0),
                    "pg_scan_p50_ms": opt_ms(pg_t5.1),
                    "basin_over_pg_gin": ratio(t5_b, pg_t5.0),
                    "basin_over_pg_scan": ratio(t5_b, pg_t5.1),
                    "basin_hits": t5_count,
                },
                {
                    "label": "T6: threshold-GUC sweep effect on % (Basin-only)",
                    "basin_thresh_0_3_p50_ms": opt_ms(t6_lo_b),
                    "basin_thresh_0_3_hits": t6_lo_count,
                    "basin_thresh_0_6_p50_ms": opt_ms(t6_hi_b),
                    "basin_thresh_0_6_hits": t6_hi_count,
                },
            ],
            "note": "Basin builds a gin_trgm_ops trigram GIN index; the % filter shapes \
                     (T2/T3) are index-backed (per-shape basin_uses_index reflects the \
                     runtime-probed posting registry). <-> ORDER BY (T4) and <% (T5) stay \
                     sequential scans on Basin. PG numbers are reported both WITH a \
                     gin_trgm_ops index and WITHOUT it (sequential scan) so the artifact \
                     carries the indexed head-to-head AND the apples-to-apples scan \
                     comparison.",
        }),
    );

    instance.wal.close().await.unwrap();
}
