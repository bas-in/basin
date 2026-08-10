//! Generic row-scale TIER card — the core engine-proof, ClickBench /
//! TimescaleDB-class. One parameterized binary that runs the headline query
//! shapes at any seed size on the ladder, env-driven by `BASIN_SCALE_ROWS`
//! (no hardcoded ceiling — accepts 1_000_000_000+).
//!
//! This is the generalization of `scale_100m.rs` (the 100M point on this same
//! ladder, kept as a standalone card for its sanctioned-ignore identity) to a
//! single tier-pinned binary: pick the row count with the env knob and the
//! whole shape suite plus the pruning invariants run at that scale.
//!
//! # The tier ladder — and WHERE each tier runs
//!
//!   * ≤ 1M    — DEV / CI. Sub-second to a few seconds; the correctness asserts
//!               catch shape regressions on every change. Default for this card
//!               is 1M so a bare `--ignored` run is a meaningful (not toy) proof.
//!   * 10M     — BOX-CEILING. The largest tier that fits this dev box's RAM /
//!               disk / patience (~10 min seed). The honest top of a laptop run.
//!   * 100M    — ClickBench-class. ~100GB on disk, hours. PROVISIONED hardware
//!               only — see `scale_100m.rs` for the dedicated card, or run this
//!               binary with `BASIN_SCALE_ROWS=100000000`.
//!   * 1B      — SCALE-PROOF. Tens of GB to ~1TB, hours. PROVISIONED hardware
//!               only. The `scale_tier_1b` test below is the explicit 1B card.
//!
//! The box-ceiling-vs-provisioned distinction is enforced by the runner
//! (`benchmark/run/scale-suite.sh`, `BASIN_SCALE_MAX`) so nobody runs the 1B
//! tier on a laptop by accident.
//!
//! # THE scale-proof: pruning invariants that must hold AT 1B
//!
//! Wall-clock is never asserted (a loaded box must fail honestly or not at
//! all). The load-bearing gates are deterministic WORK COUNTERS that prove
//! point-shaped work stays BOUNDED regardless of table size — the zone-map /
//! whole-file `column_stats` prune keeps a point lookup O(1)-ish in N:
//!
//!   * PK point lookup    — `files_opened <= 2`, `rows_decoded <= 2 chunks`.
//!                          THE scale-proof: a point lookup at 1B rows must
//!                          touch the SAME bounded work as at 1M. Growth here
//!                          is a prune regression to a scan, and it fails the
//!                          card.
//!   * keyset pagination  — `files_opened <= 3` (cursor band + boundary + slack).
//!
//! These bounds are identical at every tier (see `scale_invariants.rs` /
//! `scale_10m_smoke.rs` / `scale_100m.rs`); that they hold UNCHANGED from 1M to
//! 1B is the whole point.
//!
//! # Shapes (the headline set — ClickBench/TimescaleDB-shaped)
//!
//!   1. PK point lookup            — gated `files_opened <= 2`
//!   2. selective range filter     — exact result + pruning stats
//!   3. keyset pagination          — gated `files_opened <= 3`
//!   4. GROUP BY aggregate         — full-scan OLAP shape, exact group count
//!   5. JSONB `@>` containment      — only if the GIN index applied at CREATE
//!   6. bulk slice UPDATE          — bounded band rewrite (overlay/hot-tier path)
//!
//! # Artifact
//!
//! Each run writes `benchmark/data/scale_tier_<N>.json` (N = the row count) so
//! a ladder run leaves one artifact per tier, the way the ext size-suite
//! emits size-suffixed sidecars. Provenance: unix epoch + the tier + the
//! file-count / decode-rows invariants observed.
//!
//! # Running it
//!
//! ```text
//! # dev/CI (1M default) — meaningful, fast:
//! cargo test -p basin-integration-tests --test scale_tier scale_tier_default -- --ignored --nocapture
//!
//! # any ladder point:
//! BASIN_SCALE_ROWS=10000000 cargo test -p basin-integration-tests \
//!     --test scale_tier scale_tier_default -- --ignored --nocapture
//!
//! # the 1B scale-proof (provisioned hardware only):
//! cargo test -p basin-integration-tests --test scale_tier scale_tier_1b -- --ignored --nocapture
//! ```
//!
//! or via the suite runner `benchmark/run/scale-suite.sh` (which drives the
//! whole ladder and routes each artifact).
//!
//! # Env knobs
//!
//! * `BASIN_SCALE_ROWS`    — row count for `scale_tier_default` (default
//!                           1_000_000). No ceiling — accepts 1_000_000_000+.
//!                           Set tiny (e.g. 5_000) to smoke the harness.
//! * `BASIN_SCALE_BATCH`   — rows per INSERT statement (default 10_000, the
//!                           published compare-card batch size).
//! * `BASIN_SCALE_SAMPLES` — timed samples per shape (default 5).

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
/// Mirror of `scale_100m::build` — sibling test binaries each carry their own
/// copy of this helper by suite convention.
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
/// `compare_postgres_common::robust_estimate` (copied here by suite convention).
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
/// (tmp + rename). Minimal copy of the `benchmark.rs` write contract (this
/// card's filename is not one of the `report_*` families).
fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("[scale-tier] artifact mkdir {}: {e}", dir.display());
        return;
    }
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).expect("serialize scale_tier artifact");
    if let Err(e) = std::fs::write(&tmp, &bytes) {
        eprintln!("[scale-tier] artifact write {}: {e}", tmp.display());
        return;
    }
    if let Err(e) = std::fs::rename(&tmp, &path) {
        eprintln!("[scale-tier] artifact rename {}: {e}", path.display());
    }
    eprintln!("[scale-tier] artifact written: {}", path.display());
}

/// Human label for a tier so the artifact records WHERE it runs.
fn tier_label(rows: i64) -> &'static str {
    match rows {
        r if r <= 1_000_000 => "dev/CI (<=1M)",
        r if r <= 10_000_000 => "box-ceiling (10M)",
        r if r <= 100_000_000 => "ClickBench-class (100M, provisioned)",
        _ => "scale-proof (1B+, provisioned)",
    }
}

/// The whole tier run: seed `rows`, settle, then time + gate every headline
/// shape. Shared by `scale_tier_default` (env-driven) and `scale_tier_1b`
/// (pinned at 1B). Writes `scale_tier_<rows>.json`.
async fn run_tier(rows: i64, batch: i64, samples: usize) {
    assert!(
        rows >= 1_000,
        "scale tier needs >= 1000 rows (shape windows assume it); got {rows}"
    );
    let batch = batch.max(1);
    let samples = samples.max(1);

    // ~0.1% selectivity for the JSONB containment shape, at every scale.
    let rare_every: i64 = (rows / 1_000).max(1);

    let (_sd, _wd, engine, shard, bg, wal, storage) = build().await;
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    eprintln!(
        "[scale-tier] tier={} rows={rows} batch={batch} samples={samples} rare_every={rare_every}",
        tier_label(rows)
    );

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
            eprintln!("[scale-tier] GIN index unavailable on this schema — containment shape skipped: {e:?}");
            false
        }
    };

    // ── Seed: chunked bulk INSERT, deterministic content ─────────────────────
    // Same production write shape as the compare cards and the 10M/100M cards:
    // multi-row literal INSERTs, no per-statement flush. Row content is a pure
    // function of (id, rows): grp = id % 1000, v = id, payload carries a
    // 1024-ary category plus a rare marker on every `rare_every`-th id. Progress
    // is eprintln'd with a rows/s rate so a long 1B run is peekable.
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
                "[scale-tier] seeded {id}/{rows} rows ({:.0} rows/s, {:.0}s elapsed)",
                id as f64 / secs.max(1e-9),
                secs
            );
            next_log = id + log_every;
        }
    }
    let seed_s = seed_started.elapsed().as_secs_f64();
    let seed_rps = rows as f64 / seed_s.max(1e-9);
    eprintln!("[scale-tier] seed {rows} rows: {seed_s:.1}s ({seed_rps:.0} rows/s)");

    // ── Settle: flush + stripe merge + stop background loops ────────────────
    // Identical quiesce recipe to `run_full_compare_inner` / the 10M+100M
    // cards: drain the WAL tail to cold files, one stripe-merge pass to the
    // merged disjoint-PK layout, then stop the background loop so nothing
    // rewrites files under the timed probes.
    let settle_started = Instant::now();
    shard.flush_to_parquet().await.unwrap();
    shard.run_stripe_merge_once().await.unwrap();
    bg.shutdown().await;
    let settle_s = settle_started.elapsed().as_secs_f64();
    eprintln!("[scale-tier] settle (flush + stripe merge): {settle_s:.1}s");

    let table = TableName::new("t").unwrap();
    let live_files = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap()
        .live_data_files()
        .len();
    eprintln!("[scale-tier] settled layout: {live_files} live data files");

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

    // ── 1. PK point lookup — gated files_opened <= 2 (THE scale-proof) ───────
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
                 (zone-map prune) — growth here means the prune regressed at scale. \
                 This is THE scale-proof: the bound MUST be the same at 1B as at 1M.",
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
            "[scale-tier] point_lookup_pk: {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
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
            "[scale-tier] selective_filter ({window} rows): {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
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
            "[scale-tier] keyset_page: {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
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

    // ── 4. Aggregate with GROUP BY — full-scan OLAP shape ────────────────────
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
            "[scale-tier] group_agg ({expected_groups} groups): {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
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

    // ── 5. JSONB containment (`@>`) — only if the GIN index applied ──────────
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
            "[scale-tier] jsonb_containment ({expected} hits): {est:.2}ms robust (max files_opened={max_open}, max rows_decoded={max_dec})"
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

    // ── 6. Bulk UPDATE slice — overlay / hot-tier write path ─────────────────
    // Bounded band rewrite (not a 1/3-table rewrite, which extrapolates to
    // hours at this scale): each sample updates a DISJOINT slice so overlay
    // stacking doesn't compound across samples.
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
        // Correctness: v started equal to id; the first slice's anchor row must
        // have been incremented at least once and at most `samples` times
        // (clamping at tiny N can overlap slices).
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
        eprintln!("[scale-tier] bulk_update_slice ({slice} rows/slice): {est:.2}ms robust");
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
        "id": format!("scale_tier_{rows}"),
        "name": "Basin row-scale tier (LocalFS, default config)",
        "tier": tier_label(rows),
        "claim": "The headline query shapes stay healthy at this tier: point and \
                  keyset I/O remain O(1) in file count (THE scale-proof — the bound \
                  is identical from 1M to 1B), selective/aggregate/JSONB shapes \
                  return exact results, and a bounded bulk-UPDATE slice lands \
                  through the overlay path.",
        "generated_at": format!("@{epoch}"),
        "generated_at_unix": epoch,
        "rows": rows,
        "batch": batch,
        "samples_per_shape": samples,
        "rare_every": rare_every,
        "gin_indexed": gin_ok,
        "seed_s": seed_s,
        "seed_rows_per_sec": seed_rps,
        "settle_s": settle_s,
        "live_files": live_files,
        "shapes": shapes,
    });
    write_artifact(&format!("scale_tier_{rows}.json"), &artifact);

    println!(
        "[scale-tier] tier={} rows={rows} files={live_files}: seed {seed_s:.1}s ({seed_rps:.0} rows/s), \
         settle {settle_s:.1}s; point <=2 opens, keyset <=3 opens, filter/agg/containment/update exact — all PASS",
        tier_label(rows)
    );

    wal.close().await.unwrap();
}

/// Env-driven tier (default 1M = dev/CI). Drives the whole ladder via
/// `BASIN_SCALE_ROWS`; the runner sets that per tier point. Default is a
/// meaningful 1M proof, not a toy, so a bare `--ignored` run is useful.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "row-scale tier card: default 1M (dev/CI); set BASIN_SCALE_ROWS for 10M/100M/1B — \
            10M is the box-ceiling, 100M/1B are provisioned-hardware only. \
            Run via benchmark/run/scale-suite.sh, or: \
            BASIN_SCALE_ROWS=<N> cargo test ... --test scale_tier scale_tier_default -- --ignored --nocapture"]
async fn scale_tier_default() {
    let rows = env_i64("BASIN_SCALE_ROWS", 1_000_000);
    let batch = env_i64("BASIN_SCALE_BATCH", 10_000);
    let samples = env_i64("BASIN_SCALE_SAMPLES", 5) as usize;
    run_tier(rows, batch, samples).await;
}

/// The 1B scale-proof, pinned. This is the explicit headline card: a billion
/// rows through the real bulk-INSERT + cold-tier path, proving the pruning
/// invariants hold UNCHANGED at 1B (point lookup still `files_opened <= 2`).
/// `BASIN_SCALE_ROWS` overrides the row count so this same card smokes at
/// tiny-N, but its identity is the 1B tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "1B-row scale-proof: ~tens-of-GB, hours, provisioned hardware only — \
            run via benchmark/run/scale-suite.sh BASIN_SCALE_ROWS=1000000000, \
            never on a laptop (the runner's BASIN_SCALE_MAX refuses it by default)"]
async fn scale_tier_1b() {
    // Pinned at 1B; BASIN_SCALE_ROWS still overrides so the card can be smoked
    // at tiny-N without editing the source.
    let rows = env_i64("BASIN_SCALE_ROWS", 1_000_000_000);
    let batch = env_i64("BASIN_SCALE_BATCH", 10_000);
    let samples = env_i64("BASIN_SCALE_SAMPLES", 5) as usize;
    run_tier(rows, batch, samples).await;
}
