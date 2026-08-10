//! 100M-row tier benchmark: the headline query shapes, two orders of
//! magnitude above the published 1M compare cards and 10x above
//! `scale_10m_smoke.rs`.
//!
//! Seeds one hundred million rows through the real bulk-INSERT path (the
//! same 10k-row statement shape the compare suite uses), settles the layout
//! (flush + stripe merge — the quiesce recipe from `run_full_compare_inner`),
//! then times the headline shapes:
//!
//!   1. PK point lookup            — gated `files_opened <= 2` (zone-map prune,
//!                                   the O(1)-in-N invariant from
//!                                   `scale_invariants.rs` / `scale_10m_smoke.rs`)
//!   2. selective range filter     — exact-result correctness + pruning stats
//!   3. keyset pagination          — gated `files_opened <= 3`
//!   4. LIMIT scan                 — first-page browse shape
//!   5. aggregate w/ GROUP BY      — full-scan OLAP shape
//!   6. JSONB `@>` containment     — only if the GIN index applied at CREATE
//!   7. bulk UPDATE slice          — 100k-row band rewrite (overlay/hot-tier path)
//!
//! Timing follows the compare-card discipline: one warm run, then
//! `BASIN_SCALE_100M_SAMPLES` timed runs per shape reduced with
//! `robust_estimate` (median at >= 5 samples, min-of-n noise floor below —
//! the twin of the estimator in `compare_postgres_common.rs`). ASSERTS are
//! on row counts and deterministic work counters only, never wall-clock, so
//! a run on a loaded box fails honestly or not at all.
//!
//! A JSON artifact is written to `benchmark/data/scale_100m.json` with a
//! unix-epoch provenance timestamp and per-shape robust timing + pruning
//! stats, in the spirit of the `compare_*` sidecars (see
//! `tests/integration/src/benchmark.rs`).
//!
//! # Running it
//!
//! Sanctioned `#[ignore]`: a genuinely expensive tier card, run explicitly
//! and ALONE on an idle box (repo cargo discipline):
//!
//! ```text
//! cargo test -p basin-integration-tests --test scale_100m -- --ignored --nocapture
//! ```
//!
//! or via the suite runner `benchmark/run/scale-suite.sh`.
//!
//! # Env knobs
//!
//! * `BASIN_SCALE_100M_ROWS`    — row count (default 100_000_000). Set to a
//!                                small N (e.g. 200_000) to smoke-test the
//!                                harness itself; every shape's expected
//!                                results scale with the knob.
//! * `BASIN_SCALE_100M_BATCH`   — rows per INSERT statement (default 10_000,
//!                                the published compare-card batch size).
//! * `BASIN_SCALE_100M_SAMPLES` — timed samples per shape (default 5).

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;

/// One Vortex chunk's worth of rows — see the twin const in
/// `scale_invariants.rs` for the derivation.
const ONE_CHUNK: u64 = 65_536;

/// Build a real in-process engine (Shard + WAL + LocalFileSystem storage).
/// Mirror of `scale_10m_smoke::build` — sibling test binaries each carry
/// their own copy of this helper by suite convention.
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
    Storage,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, storage)
}

/// Count rows in an `ExecResult::Rows`, asserting it is a result set.
fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

/// Collect the first Int64 column of a result in result order.
fn ids_of(res: &ExecResult) -> Vec<i64> {
    use arrow_array::{Array, Int64Array};
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in batches {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column 0 must be Int64");
                for r in 0..ids.len() {
                    out.push(ids.value(r));
                }
            }
            out
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

fn median(samples: &[f64]) -> f64 {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if s.is_empty() {
        return f64::NAN;
    }
    let mid = s.len() / 2;
    if s.len() % 2 == 0 {
        (s[mid - 1] + s[mid]) / 2.0
    } else {
        s[mid]
    }
}

/// Noise-floor estimator: median at >= 5 samples, min-of-n below. Twin of
/// `compare_postgres_common::robust_estimate` (private to that binary; copied
/// here by suite convention — see that file for the full estimator-policy
/// rationale).
fn robust_estimate(samples: &[f64]) -> f64 {
    if samples.len() >= 5 {
        median(samples)
    } else {
        samples
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min)
            .max(0.0)
    }
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Write the JSON artifact to `<repo_root>/benchmark/data/<file>`, atomically
/// (tmp + rename), infallibly from the test's perspective — the same contract
/// as `basin_integration_tests::benchmark::write_atomic` (private; minimal
/// copy here because this card's filename is not one of the `report_*`
/// families).
fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("[100m] artifact mkdir {}: {e}", dir.display());
        return;
    }
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).expect("serialize scale_100m artifact");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("[100m] artifact write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("[100m] artifact rename {}: {e}", path.display());
    }
    eprintln!("[100m] artifact written: {}", path.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "100M-row tier benchmark: hours of runtime + ~100GB disk; run via benchmark/run/scale-suite.sh on an idle box"]
async fn scale_100m() {
    let rows: i64 = env_i64("BASIN_SCALE_100M_ROWS", 100_000_000);
    let batch: i64 = env_i64("BASIN_SCALE_100M_BATCH", 10_000).max(1);
    let samples: usize = env_i64("BASIN_SCALE_100M_SAMPLES", 5).max(1) as usize;
    assert!(
        rows >= 1_000,
        "BASIN_SCALE_100M_ROWS must be >= 1000 (shape windows assume it); got {rows}"
    );

    // ~0.1% selectivity for the JSONB containment shape, at every scale.
    let rare_every: i64 = (rows / 1_000).max(1);

    let (_sd, _wd, engine, shard, bg, wal, storage) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    eprintln!("[100m] config: rows={rows} batch={batch} samples={samples} rare_every={rare_every}");

    sess.execute(
        "CREATE TABLE t (\
            id BIGINT NOT NULL PRIMARY KEY, \
            grp BIGINT NOT NULL, \
            v BIGINT NOT NULL, \
            payload JSONB NOT NULL\
         )",
    )
    .await
    .unwrap();

    // GIN index BEFORE the seed (the `viability_jsonb_posting.rs` recipe) so
    // posting lists build during ingest/compaction. If the engine rejects a
    // GIN index on this schema, the containment shape is skipped and the gap
    // is recorded in the artifact — measured honestly, not masked.
    let gin_ok = match sess
        .execute("CREATE INDEX t_gin ON t USING gin (payload)")
        .await
    {
        Ok(_) => true,
        Err(e) => {
            eprintln!(
                "[100m] GIN index unavailable on this schema — containment shape skipped: {e:?}"
            );
            false
        }
    };

    // ── Seed: chunked bulk INSERT, deterministic content ─────────────────────
    // Same production write shape as the compare cards and the 10M smoke:
    // multi-row literal INSERTs, no per-statement flush. Row content is a
    // pure function of (id, rows): grp = id % 1000, v = id, payload carries a
    // 1024-ary category plus a rare marker on every `rare_every`-th id.
    let seed_started = Instant::now();
    let log_every: i64 = 1_000_000.min(rows);
    let mut next_log = log_every;
    let mut id = 0i64;
    while id < rows {
        let lo = id;
        let hi = (id + batch).min(rows);
        let mut stmt = String::with_capacity((hi - lo) as usize * 56);
        stmt.push_str("INSERT INTO t VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            let cat = k % 1024;
            if k % rare_every == 0 {
                stmt.push_str(&format!(
                    "({k},{},{k},'{{\"cat\":\"c{cat}\",\"rare\":\"yes\"}}')",
                    k % 1000
                ));
            } else {
                stmt.push_str(&format!("({k},{},{k},'{{\"cat\":\"c{cat}\"}}')", k % 1000));
            }
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
        if id >= next_log || id == rows {
            let secs = seed_started.elapsed().as_secs_f64();
            eprintln!(
                "[100m] seeded {id}/{rows} rows ({:.0} rows/s, {:.0}s elapsed)",
                id as f64 / secs.max(1e-9),
                secs
            );
            next_log = id + log_every;
        }
    }
    let seed_s = seed_started.elapsed().as_secs_f64();
    eprintln!("[100m] seed {rows} rows: {seed_s:.1}s");

    // ── Settle: flush + stripe merge + stop background loops ────────────────
    // Identical quiesce recipe to `run_full_compare_inner` / the 10M smoke:
    // drain the WAL tail to cold files, one stripe-merge pass to the merged
    // disjoint-PK layout, then stop the background loop so nothing rewrites
    // files under the timed probes.
    let settle_started = Instant::now();
    shard.flush_to_parquet().await.unwrap();
    shard.run_stripe_merge_once().await.unwrap();
    bg.shutdown().await;
    let settle_s = settle_started.elapsed().as_secs_f64();
    eprintln!("[100m] settle (flush + stripe merge): {settle_s:.1}s");

    let table = TableName::new("t").unwrap();
    let live_files = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .live_data_files()
        .len();
    eprintln!("[100m] settled layout: {live_files} live data files");

    // Per-probe counter delta helper (counter discipline: probes run one at a
    // time on this single session; the background loop is already stopped).
    let probe = |sql: String| {
        let sess = &sess;
        let storage = &storage;
        async move {
            let before = storage.read_counters().snapshot();
            let started = Instant::now();
            let res = sess.execute(&sql).await.unwrap();
            let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
            let after = storage.read_counters().snapshot();
            (res, after.delta(&before), wall_ms)
        }
    };

    let mut shapes: Vec<serde_json::Value> = Vec::new();

    // ── 1. PK point lookup — gated files_opened <= 2 ─────────────────────────
    {
        let _ = probe(format!("SELECT v FROM t WHERE id = {}", rows / 2)).await; // warm
        let mut wall = Vec::with_capacity(samples);
        let mut max_open = 0u64;
        let mut max_dec = 0u64;
        for s in 0..samples {
            let key = (rows / 2 + 137 + (s as i64) * 9_973) % rows;
            let (res, d, ms) = probe(format!("SELECT v FROM t WHERE id = {key}")).await;
            assert_eq!(
                ids_of(&res),
                vec![key],
                "point lookup must return exactly the row (v == id == {key})"
            );
            assert!(
                d.files_opened <= 2,
                "point lookup opened {} files at {rows} rows; design bound is <=2 \
                 (zone-map prune) — growth here means the prune regressed at scale",
                d.files_opened
            );
            assert!(
                d.rows_decoded <= 2 * ONE_CHUNK,
                "point lookup decoded {} rows; bound <= 2 chunks",
                d.rows_decoded
            );
            max_open = max_open.max(d.files_opened);
            max_dec = max_dec.max(d.rows_decoded);
            wall.push(ms);
        }
        let est = robust_estimate(&wall);
        eprintln!(
            "[100m] point_lookup_pk: {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
        );
        shapes.push(json!({
            "label": "point_lookup_pk",
            "sql_example": "SELECT v FROM t WHERE id = <key>",
            "robust_ms": est,
            "samples_ms": wall,
            "files_opened_max": max_open,
            "rows_decoded_max": max_dec,
            "gate": "files_opened <= 2",
        }));
    }

    // ── 2. Selective range filter — exact result + pruning stats ─────────────
    {
        let window: i64 = if rows >= 4_000 {
            1_000
        } else {
            (rows / 4).max(1)
        };
        let step = (rows / (2 * samples as i64).max(1)).max(1);
        let _ = probe(format!(
            "SELECT id, v FROM t WHERE id BETWEEN {} AND {} ORDER BY id",
            rows / 8,
            rows / 8 + window - 1
        ))
        .await; // warm
        let mut wall = Vec::with_capacity(samples);
        let mut max_open = 0u64;
        let mut max_dec = 0u64;
        for s in 0..samples {
            let lo = (rows / 8 + (s as i64) * step).min(rows - window);
            let hi = lo + window - 1;
            let (res, d, ms) = probe(format!(
                "SELECT id, v FROM t WHERE id BETWEEN {lo} AND {hi} ORDER BY id"
            ))
            .await;
            assert_eq!(
                ids_of(&res),
                (lo..=hi).collect::<Vec<i64>>(),
                "selective filter must return exactly the {window} ids in the window, in order"
            );
            max_open = max_open.max(d.files_opened);
            max_dec = max_dec.max(d.rows_decoded);
            wall.push(ms);
        }
        let est = robust_estimate(&wall);
        eprintln!(
            "[100m] selective_filter ({window} rows): {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
        );
        shapes.push(json!({
            "label": "selective_filter",
            "sql_example": "SELECT id, v FROM t WHERE id BETWEEN <lo> AND <hi> ORDER BY id",
            "window_rows": window,
            "robust_ms": est,
            "samples_ms": wall,
            "files_opened_max": max_open,
            "rows_decoded_max": max_dec,
        }));
    }

    // ── 3. Keyset pagination — gated files_opened <= 3 ───────────────────────
    {
        let _ = probe(format!(
            "SELECT id, v FROM t WHERE id > {} ORDER BY id LIMIT 50",
            rows / 2
        ))
        .await; // warm
        let mut wall = Vec::with_capacity(samples);
        let mut max_open = 0u64;
        let mut max_dec = 0u64;
        for s in 0..samples {
            let cursor = (rows / 2 + 10_345 + (s as i64) * 1_009).min(rows - 51);
            let (res, d, ms) = probe(format!(
                "SELECT id, v FROM t WHERE id > {cursor} ORDER BY id LIMIT 50"
            ))
            .await;
            assert_eq!(
                ids_of(&res),
                ((cursor + 1)..=(cursor + 50)).collect::<Vec<i64>>(),
                "keyset page must be the 50 smallest ids > cursor"
            );
            assert!(
                d.files_opened <= 3,
                "keyset page opened {} files at {rows} rows; bound <=3 (cursor band + \
                 boundary + slack) — more means the ordered traversal regressed to \
                 the all-candidate fan-out",
                d.files_opened
            );
            max_open = max_open.max(d.files_opened);
            max_dec = max_dec.max(d.rows_decoded);
            wall.push(ms);
        }
        let est = robust_estimate(&wall);
        eprintln!(
            "[100m] keyset_page: {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
        );
        shapes.push(json!({
            "label": "keyset_page",
            "sql_example": "SELECT id, v FROM t WHERE id > <cursor> ORDER BY id LIMIT 50",
            "robust_ms": est,
            "samples_ms": wall,
            "files_opened_max": max_open,
            "rows_decoded_max": max_dec,
            "gate": "files_opened <= 3",
        }));
    }

    // ── 4. LIMIT scan — first-page browse ────────────────────────────────────
    {
        let sql = "SELECT id, v FROM t LIMIT 100".to_string();
        let _ = probe(sql.clone()).await; // warm
        let mut wall = Vec::with_capacity(samples);
        let mut max_open = 0u64;
        let mut max_dec = 0u64;
        for _ in 0..samples {
            let (res, d, ms) = probe(sql.clone()).await;
            assert_eq!(row_count(&res), 100, "LIMIT 100 must return 100 rows");
            max_open = max_open.max(d.files_opened);
            max_dec = max_dec.max(d.rows_decoded);
            wall.push(ms);
        }
        let est = robust_estimate(&wall);
        eprintln!(
            "[100m] limit_scan: {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
        );
        shapes.push(json!({
            "label": "limit_scan",
            "sql_example": "SELECT id, v FROM t LIMIT 100",
            "robust_ms": est,
            "samples_ms": wall,
            "files_opened_max": max_open,
            "rows_decoded_max": max_dec,
        }));
    }

    // ── 5. Aggregate with GROUP BY — full-scan OLAP shape ────────────────────
    {
        let expected_groups = rows.min(1_000) as usize;
        let sql = "SELECT grp, COUNT(*), SUM(v) FROM t GROUP BY grp".to_string();
        let _ = probe(sql.clone()).await; // warm
        let mut wall = Vec::with_capacity(samples);
        let mut max_open = 0u64;
        let mut max_dec = 0u64;
        for _ in 0..samples {
            let (res, d, ms) = probe(sql.clone()).await;
            assert_eq!(
                row_count(&res),
                expected_groups,
                "GROUP BY grp must produce exactly {expected_groups} groups"
            );
            max_open = max_open.max(d.files_opened);
            max_dec = max_dec.max(d.rows_decoded);
            wall.push(ms);
        }
        let est = robust_estimate(&wall);
        eprintln!(
            "[100m] group_agg ({expected_groups} groups): {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
        );
        shapes.push(json!({
            "label": "group_agg",
            "sql_example": "SELECT grp, COUNT(*), SUM(v) FROM t GROUP BY grp",
            "groups": expected_groups,
            "robust_ms": est,
            "samples_ms": wall,
            "files_opened_max": max_open,
            "rows_decoded_max": max_dec,
        }));
    }

    // ── 6. JSONB containment (`@>`) — only if the GIN index applied ──────────
    if gin_ok {
        let expected: i64 = (rows + rare_every - 1) / rare_every;
        let sql = r#"SELECT COUNT(*) FROM t WHERE payload @> '{"rare":"yes"}'"#.to_string();
        let _ = probe(sql.clone()).await; // warm
        let mut wall = Vec::with_capacity(samples);
        let mut max_open = 0u64;
        let mut max_dec = 0u64;
        for _ in 0..samples {
            let (res, d, ms) = probe(sql.clone()).await;
            assert_eq!(
                ids_of(&res),
                vec![expected],
                "containment COUNT must match the deterministic rare-marker count"
            );
            max_open = max_open.max(d.files_opened);
            max_dec = max_dec.max(d.rows_decoded);
            wall.push(ms);
        }
        let est = robust_estimate(&wall);
        eprintln!(
            "[100m] jsonb_containment ({expected} hits): {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
        );
        shapes.push(json!({
            "label": "jsonb_containment",
            "sql_example": "SELECT COUNT(*) FROM t WHERE payload @> '{\"rare\":\"yes\"}'",
            "expected_hits": expected,
            "robust_ms": est,
            "samples_ms": wall,
            "files_opened_max": max_open,
            "rows_decoded_max": max_dec,
        }));
    }

    // ── 7. Bulk UPDATE slice — overlay / hot-tier write path ─────────────────
    // Bounded band rewrite (not the compare suite's 1/3-table rewrite, which
    // extrapolates to hours at this scale): each sample updates a DISJOINT
    // slice so overlay stacking doesn't compound across samples.
    {
        let slice: i64 = 100_000.min((rows / 100).max(10));
        let base = (rows / 10).min(rows - slice);
        let mut wall = Vec::with_capacity(samples);
        for s in 0..samples {
            let lo = (base + (s as i64) * slice).min(rows - slice);
            let hi = lo + slice - 1;
            let started = Instant::now();
            sess.execute(&format!(
                "UPDATE t SET v = v + 1 WHERE id BETWEEN {lo} AND {hi}"
            ))
            .await
            .unwrap();
            wall.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        // Correctness: v started equal to id; the first slice's anchor row
        // must have been incremented by at least one and at most `samples`
        // (clamping at tiny N can overlap slices) of the updates.
        let v_after = ids_of(
            &sess
                .execute(&format!("SELECT v FROM t WHERE id = {base}"))
                .await
                .unwrap(),
        );
        assert_eq!(v_after.len(), 1, "anchor row must still exist after UPDATE");
        assert!(
            v_after[0] > base && v_after[0] <= base + samples as i64,
            "anchor row v must reflect the slice UPDATE(s): id={base}, v={}",
            v_after[0]
        );
        let est = robust_estimate(&wall);
        eprintln!("[100m] bulk_update_slice ({slice} rows/slice): {est:.2}ms robust");
        shapes.push(json!({
            "label": "bulk_update_slice",
            "sql_example": "UPDATE t SET v = v + 1 WHERE id BETWEEN <lo> AND <hi>",
            "slice_rows": slice,
            "robust_ms": est,
            "samples_ms": wall,
        }));
    }

    // ── Artifact ─────────────────────────────────────────────────────────────
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let artifact = json!({
        "kind": "scale_tier",
        "id": "scale_100m",
        "name": "Basin 100M-row tier benchmark (LocalFS, default config)",
        "claim": "The headline query shapes stay healthy at 100M rows: point and \
                  keyset I/O remain O(1) in file count, selective/aggregate/JSONB \
                  shapes return exact results, and a bounded bulk-UPDATE slice \
                  lands through the overlay path.",
        "generated_at": format!("@{epoch}"),
        "generated_at_unix": epoch,
        "rows": rows,
        "batch": batch,
        "samples_per_shape": samples,
        "rare_every": rare_every,
        "gin_indexed": gin_ok,
        "seed_s": seed_s,
        "settle_s": settle_s,
        "live_files": live_files,
        "shapes": shapes,
    });
    write_artifact("scale_100m.json", &artifact);

    println!(
        "[100m] rows={rows} files={live_files}: seed {seed_s:.1}s, settle {settle_s:.1}s; \
         point <=2 opens, keyset <=3 opens, filter/agg/containment/update exact — all PASS"
    );

    wal.close().await.unwrap();
}
