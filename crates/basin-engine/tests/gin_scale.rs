//! Phase 5.19.F — GIN behaviour at scale under a tiny posting budget.
//!
//! The in-RAM GIN posting list evicts terms when it exceeds
//! `BASIN_GIN_POSTING_BUDGET`.  Before per-file degradation, eviction marked
//! the affected files un-indexed and the all-or-nothing completeness guard
//! then disabled pruning for the whole table — or worse, stale coverage maps
//! could prune files whose terms were gone.  These tests force the old
//! failure mode with a tiny budget over high-cardinality JSONB and assert:
//!
//! 1. results are NEVER wrong — every `@>` / `?` / `<@` query returns
//!    exactly the rows a reference re-implementation of the containment
//!    semantics predicts;
//! 2. the registry records that eviction fired (the once-per-table operator
//!    warning latch);
//! 3. nested / unicode / numeric payloads behave identically with the index
//!    in place (the lazy raw-containment path + per-file pruning).
//!
//! The budget env var is read once per process (OnceLock), so every test in
//! this binary sets the SAME tiny value before touching the engine.

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Force the tiny-budget failure mode.  Must run before the first posting
/// insert in this process (the budget is cached in a OnceLock).  All tests
/// set the same value, so ordering between tests does not matter.
fn set_tiny_posting_budget() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| std::env::set_var("BASIN_GIN_POSTING_BUDGET", "400"));
}

/// Build a shard-backed engine over tempdir-backed storage + WAL (mirrors
/// `gin_backfill_probe.rs`).
async fn shard_engine() -> (Engine, TempDir, TempDir, basin_shard::Shard) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal,
    ));
    let eng = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (eng, storage_dir, wal_dir, shard)
}

async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn count_rows(sess: &ProjectSession, sql: &str) -> usize {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

// ── Reference containment semantics (mirrors the engine's json_contains) ────

fn ref_contains(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    use serde_json::Value;
    match (left, right) {
        (Value::Object(lm), Value::Object(rm)) => rm
            .iter()
            .all(|(k, rv)| lm.get(k).map(|lv| ref_contains(lv, rv)).unwrap_or(false)),
        (Value::Array(la), Value::Array(ra)) => {
            ra.iter().all(|rv| la.iter().any(|lv| ref_contains(lv, rv)))
        }
        (l, r) => l == r,
    }
}

fn ref_has_key(doc: &serde_json::Value, key: &str) -> bool {
    match doc {
        serde_json::Value::Object(m) => m.contains_key(key),
        serde_json::Value::Array(a) => a
            .iter()
            .any(|v| matches!(v, serde_json::Value::String(s) if s == key)),
        _ => false,
    }
}

/// Count how many of `docs` contain `needle` per the reference semantics.
fn expected_contains(docs: &[serde_json::Value], needle: &str) -> usize {
    let nv: serde_json::Value = serde_json::from_str(needle).unwrap();
    docs.iter().filter(|d| ref_contains(d, &nv)).count()
}

fn expected_has_key(docs: &[serde_json::Value], key: &str) -> usize {
    docs.iter().filter(|d| ref_has_key(d, key)).count()
}

/// 50k high-cardinality rows under a 400-entry posting budget: the CREATE
/// INDEX backfill must blow the budget (eviction fires, the warn latch sets)
/// and every probe-eligible query must still return exactly the reference
/// answer — pruning degrades per file, never to wrong results.
#[tokio::test]
async fn gin_tiny_budget_eviction_never_wrong() {
    set_tiny_posting_budget();
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    let table = TableName::new("docs").unwrap();

    exec_ok(&sess, "CREATE TABLE docs (id BIGINT, body JSONB)").await;

    // 50k rows; "u" is unique per row (high cardinality → posting blow-up),
    // "kind" is low cardinality.  Two flush rounds → at least two cold files,
    // so per-file coverage is exercised (eviction de-indexes some files while
    // others stay covered).
    let mut docs: Vec<serde_json::Value> = Vec::with_capacity(50_000);
    for round in 0..2 {
        for chunk in 0..13 {
            let mut values = String::new();
            let mut wrote = 0usize;
            for i in 0..2_000 {
                let id = round * 25_000 + chunk * 2_000 + i;
                if id >= (round + 1) * 25_000 {
                    break;
                }
                let kind = if id % 2 == 0 { "a" } else { "b" };
                let body = format!("{{\"kind\":\"{kind}\",\"u\":\"u{id}\"}}");
                docs.push(serde_json::from_str(&body).unwrap());
                if wrote > 0 {
                    values.push_str(", ");
                }
                values.push_str(&format!("({id}, '{body}')"));
                wrote += 1;
            }
            if wrote > 0 {
                exec_ok(
                    &sess,
                    &format!("INSERT INTO docs (id, body) VALUES {values}"),
                )
                .await;
            }
        }
        shard.flush_to_parquet().await.unwrap();
    }
    assert_eq!(docs.len(), 50_000);
    assert_eq!(count_rows(&sess, "SELECT id FROM docs").await, 50_000);

    // Backfill the GIN index over the flushed files.  With a 400-entry
    // budget this MUST overflow (50k unique kv:u=… pairs alone).
    exec_ok(&sess, "CREATE INDEX docs_body_gin ON docs USING gin (body)").await;

    let registry = eng.gin_index_registry_for_test();
    assert!(
        registry.has_evicted(&project, &table, "body"),
        "the 400-entry budget must force eviction during a 50k-row backfill \
         (the once-per-table operator warning latch must be set)"
    );

    // Low-cardinality term — its posting pairs may or may not have survived
    // eviction; the answer must be exact either way.
    let q = "SELECT id FROM docs WHERE body @> '{\"kind\":\"a\"}'";
    assert_eq!(
        count_rows(&sess, q).await,
        expected_contains(&docs, "{\"kind\":\"a\"}"),
        "query: {q}"
    );

    // High-cardinality needles, one resident in each flush round's files.
    for probe_id in [123usize, 24_999, 25_001, 49_999] {
        let needle = format!("{{\"u\":\"u{probe_id}\"}}");
        let q = format!("SELECT id FROM docs WHERE body @> '{needle}'");
        assert_eq!(
            count_rows(&sess, &q).await,
            expected_contains(&docs, &needle),
            "query: {q}"
        );
        assert_eq!(expected_contains(&docs, &needle), 1, "self-check: {needle}");
    }

    // A needle that matches nothing (its term was likely never in the
    // surviving posting list → the probe must degrade, not fabricate).
    let q = "SELECT id FROM docs WHERE body @> '{\"u\":\"nope\"}'";
    assert_eq!(count_rows(&sess, q).await, 0, "query: {q}");

    // Compound needle.
    let q = "SELECT id FROM docs WHERE body @> '{\"kind\":\"b\",\"u\":\"u101\"}'";
    assert_eq!(
        count_rows(&sess, q).await,
        expected_contains(&docs, "{\"kind\":\"b\",\"u\":\"u101\"}"),
        "query: {q}"
    );

    // Key-existence probes.
    let q = "SELECT id FROM docs WHERE body ? 'kind'";
    assert_eq!(
        count_rows(&sess, q).await,
        expected_has_key(&docs, "kind"),
        "query: {q}"
    );
    let q = "SELECT id FROM docs WHERE body ? 'zzz'";
    assert_eq!(count_rows(&sess, q).await, 0, "query: {q}");

    // COUNT(*) aggregate over the pruned path must not be short-circuited
    // into zero rows.
    match sess
        .execute("SELECT COUNT(*) FROM docs WHERE body @> '{\"kind\":\"b\"}'")
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "COUNT(*) must return exactly one row");
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Nested objects, unicode keys/values, numeric tokens, and arrays — with a
/// GIN index in place and the tiny budget forcing partial coverage.  Every
/// answer is checked against the reference containment semantics computed
/// over the generated documents (no hand-derived counts to go stale).
#[tokio::test]
async fn gin_nested_unicode_numeric_correctness() {
    set_tiny_posting_budget();
    let (eng, _sd, _wd, shard) = shard_engine().await;
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();

    exec_ok(&sess, "CREATE TABLE t (id BIGINT, body JSONB)").await;

    let mut docs: Vec<serde_json::Value> = Vec::new();
    for chunk in 0..3 {
        let mut values = String::new();
        for i in 0..1_000 {
            let id = chunk * 1_000 + i;
            let body = match id % 4 {
                0 => format!(
                    "{{\"a\":{{\"x\":1,\"y\":{id}}},\"tag\":\"n\u{e9}{}\"}}",
                    id % 5
                ),
                1 => "{\"a\":{\"x\":1},\"v\":1.5}".to_string(),
                2 => format!("{{\"v\":1,\"arr\":[1,2,{}]}}", id % 7),
                _ => format!("{{\"sub\":{}}}", id % 3),
            };
            docs.push(serde_json::from_str(&body).unwrap());
            if i > 0 {
                values.push_str(", ");
            }
            values.push_str(&format!("({id}, '{body}')"));
        }
        exec_ok(&sess, &format!("INSERT INTO t (id, body) VALUES {values}")).await;
    }
    shard.flush_to_parquet().await.unwrap();
    exec_ok(&sess, "CREATE INDEX t_body_gin ON t USING gin (body)").await;

    // Nested-object needle: recursive subset — the superset docs
    // {"a":{"x":1,"y":N}} MUST match {"a":{"x":1}} (this guards the
    // needle_terms necessary-condition fix end-to-end: an exact kv:a={…}
    // probe term would prune every superset row).
    let cases = [
        "{\"a\":{\"x\":1}}",
        "{\"a\":{\"x\":1,\"y\":8}}",
        "{\"v\":1}",
        "{\"v\":1.5}",
        "{\"arr\":[2,1]}",
        "{\"arr\":[1,2,3]}",
        "{\"tag\":\"né3\"}",
        "{\"sub\":2}",
        "{\"a\":{}}",
        "{}",
    ];
    for needle in cases {
        let q = format!("SELECT id FROM t WHERE body @> '{needle}'");
        let expected = expected_contains(&docs, needle);
        assert_eq!(count_rows(&sess, &q).await, expected, "query: {q}");
        // Make sure the battery isn't vacuous: the nested needle must match
        // a strict superset of the exact-match rows.
        if needle == "{\"a\":{\"x\":1}}" {
            assert!(expected >= 1_000, "nested needle should match both shapes");
        }
    }

    // Key existence incl. a unicode key payload.
    for key in ["a", "arr", "tag", "missing"] {
        let q = format!("SELECT id FROM t WHERE body ? '{key}'");
        assert_eq!(
            count_rows(&sess, &q).await,
            expected_has_key(&docs, key),
            "query: {q}"
        );
    }

    // `<@` must NOT be pruned by the containment probe (a matching row may
    // be an arbitrary subset of the literal).  The {"sub":k} rows are the
    // only subsets of this literal.
    let lit = "{\"sub\":2,\"extra\":true}";
    let q = format!("SELECT id FROM t WHERE body <@ '{lit}'");
    let lit_v: serde_json::Value = serde_json::from_str(lit).unwrap();
    let expected = docs.iter().filter(|d| ref_contains(&lit_v, d)).count();
    assert_eq!(count_rows(&sess, &q).await, expected, "query: {q}");
    assert!(expected > 0, "self-check: some rows are subsets of {lit}");
}
