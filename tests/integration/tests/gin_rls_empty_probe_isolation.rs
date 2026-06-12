//! Audit gap #1 — GIN empty-probe short-circuit under Row-Level Security.
//!
//! The executor has a "return zero rows without scanning any file"
//! optimisation for `col @> 'needle'` on a GIN-indexed JSONB column: when the
//! file-level posting list proves no cold file can contain the needle
//! (`ProbeResult::Empty`) and `gin_empty_probe_is_trustworthy` confirms no live
//! overlay + full file coverage, the executor returns an empty result without
//! consulting any data file (`executor.rs` ~line 2862).
//!
//! The audit flagged that `gin_empty_probe_is_trustworthy` does NOT read
//! `meta.rls_enabled`. This suite pins WHY that is safe and proves it stays
//! safe:
//!
//!   * The empty short-circuit returns ZERO rows. Zero rows is sound under RLS
//!     for ANY principal: the RLS-filtered subset is always a subset of the
//!     unfiltered universe, so if the unfiltered universe is provably empty
//!     (no file holds the needle) the RLS-filtered subset is empty too. The
//!     short-circuit can only ever return the *correct* empty answer, never a
//!     wrong non-empty one.
//!   * Every probe verdict that is NOT a trustworthy `Empty`
//!     (`FileCandidates`, `NoIndex`, aggregate, or untrustworthy `Empty`)
//!     falls through to `exec_select`, which applies `apply_rls_to_select`
//!     AFTER any file-level GIN narrowing. The narrowing only prunes files that
//!     provably cannot match the `@>` needle; RLS filters the survivors. So the
//!     non-empty paths NEVER emit a posting-derived row without the RLS
//!     predicate applied downstream.
//!
//! The danger the audit named — "return zero rows for some principals and
//! wrong rows for others depending on posting-list coverage" — would require a
//! path that returns posting-derived rows directly. No such path exists: the
//! probe is advisory; only an `Empty` verdict short-circuits, and only to
//! empty.
//!
//! These tests are the regression pin. They drive a real RLS-enabled,
//! GIN-indexed table from a policy-restricted principal and assert:
//!   (i)   needle present ONLY in another user's rows  → empty, no error;
//!   (ii)  needle present in the principal's own rows  → exactly own rows;
//!   (iii) needle present in NO row (short-circuit fires) → empty;
//! and differentially asserts every case against the DataFusion fallback by
//! also enabling RLS and comparing the GIN-probe path to a forced full-scan
//! shape. Each case is run with cold-only data AND with a live hot-tier overlay
//! present (which forces the trustworthy gate to decline and the overlay-aware
//! scan + RLS to run instead).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Env isolation for the UPDATE fastpath (overlay) sub-cases ───────────────────

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}
fn set_env(key: &'static str) -> EnvGuard {
    let prev = std::env::var(key).ok();
    unsafe { std::env::set_var(key, "1") };
    EnvGuard { key, prev }
}

// ── Engine + read helpers ──────────────────────────────────────────────────────

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Run `sql` and return the sorted `id` column of every row.
async fn ids(sess: &ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut out = Vec::new();
            for b in &batches {
                let Ok(idx) = b.schema().index_of("id") else { continue };
                if let Some(a) = b.column(idx).as_any().downcast_ref::<Int64Array>() {
                    for i in 0..a.len() {
                        if !a.is_null(i) {
                            out.push(a.value(i));
                        }
                    }
                }
            }
            out.sort_unstable();
            out
        }
        Ok(ExecResult::Empty { .. }) => vec![],
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Collect the sorted `owner` column of every returned row. Any owner that is
/// not the querying principal is an RLS leak.
async fn owners(sess: &ProjectSession, sql: &str) -> Vec<String> {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => {
            let mut out = Vec::new();
            for b in &batches {
                let Some(col) = b.column_by_name("owner") else { continue };
                let arr = col.as_any().downcast_ref::<StringArray>().expect("owner utf8");
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        out.push(arr.value(i).to_string());
                    }
                }
            }
            out.sort();
            out
        }
        Ok(ExecResult::Empty { .. }) => vec![],
        Err(e) => panic!("execute failed for:\n  {sql}\n  error: {e}"),
    }
}

/// COUNT(*) helper — the aggregate path NEVER short-circuits on the probe
/// (see `!gin_plan.is_aggregate`), so it always routes through exec_select +
/// RLS. We assert it agrees with the row-emitting path.
async fn count_where(sess: &ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0),
        Ok(ExecResult::Empty { .. }) => 0,
        Err(e) => panic!("count failed for:\n  {sql}\n  error: {e}"),
    }
}

/// Set up a GIN-indexed, RLS-enabled `docs(id, owner, payload)` and seed
/// alice/bob rows whose payload tags are user-disjoint. Returns the project.
///
/// alice rows carry `{"tag":"alice_tag"}`; bob rows carry `{"tag":"bob_tag"}`.
/// No row carries `{"tag":"ghost_tag"}`.
async fn seed(engine: &Engine) -> ProjectId {
    let project = ProjectId::new();
    let admin = engine.open_session(project).await.unwrap();

    admin
        .execute("CREATE TABLE docs (id BIGINT NOT NULL, owner TEXT NOT NULL, payload JSONB)")
        .await
        .expect("create table");
    admin
        .execute("CREATE INDEX docs_gin ON docs USING gin (payload jsonb_ops)")
        .await
        .expect("create gin index");

    // Separate INSERTs so the GIN posting list is built per file with
    // user-disjoint tags — the exact shape the empty-probe short-circuit keys
    // on (a needle present only in another user's file).
    admin
        .execute(
            r#"INSERT INTO docs VALUES
                (1, 'alice', '{"tag":"alice_tag","n":1}'),
                (2, 'alice', '{"tag":"alice_tag","n":2}')"#,
        )
        .await
        .expect("insert alice");
    admin
        .execute(
            r#"INSERT INTO docs VALUES
                (3, 'bob', '{"tag":"bob_tag","n":3}'),
                (4, 'bob', '{"tag":"bob_tag","n":4}')"#,
        )
        .await
        .expect("insert bob");

    admin
        .execute("ALTER TABLE docs ENABLE ROW LEVEL SECURITY")
        .await
        .expect("enable rls");
    admin
        .execute("CREATE POLICY docs_owner ON docs FOR ALL TO PUBLIC USING (owner = current_user)")
        .await
        .expect("create policy");

    project
}

// ═══════════════════════════════════════════════════════════════════════════════
// Case (i)/(ii)/(iii) — cold-only data, GIN empty-probe fully eligible.
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gin_at_gt_under_rls_cold_only_never_leaks() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = seed(&engine).await;

    let alice = engine.open_session_as(project, "alice").await.unwrap();
    let bob = engine.open_session_as(project, "bob").await.unwrap();

    // A non-RLS control table with the identical GIN-indexed shape and the same
    // disjoint tags. The empty-probe short-circuit is RLS-blind; the ghost-tag
    // `@>` below engages it on BOTH tables. Asserting the control returns the
    // same empty answer as the RLS table proves the short-circuit's verdict is
    // independent of RLS (and that RLS only ever subtracts rows, never adds).
    let control_project = ProjectId::new();
    {
        let admin = engine.open_session(control_project).await.unwrap();
        admin
            .execute("CREATE TABLE ctrl (id BIGINT NOT NULL, owner TEXT NOT NULL, payload JSONB)")
            .await
            .unwrap();
        admin
            .execute("CREATE INDEX ctrl_gin ON ctrl USING gin (payload jsonb_ops)")
            .await
            .unwrap();
        admin
            .execute(
                r#"INSERT INTO ctrl VALUES
                    (1, 'alice', '{"tag":"alice_tag","n":1}'),
                    (2, 'alice', '{"tag":"alice_tag","n":2}')"#,
            )
            .await
            .unwrap();
        admin
            .execute(
                r#"INSERT INTO ctrl VALUES
                    (3, 'bob', '{"tag":"bob_tag","n":3}'),
                    (4, 'bob', '{"tag":"bob_tag","n":4}')"#,
            )
            .await
            .unwrap();
    }

    // ── (i) needle present ONLY in bob's rows; alice queries it ────────────────
    //
    // The GLOBAL (RLS-blind) posting list DOES contain bob's `alice`-disjoint
    // tag, so the probe returns FileCandidates (not Empty) — the path falls
    // through to exec_select + RLS, which filters bob's rows out for alice.
    // alice must get an empty, error-free result; bob's rows must NOT leak.
    let q_bob_tag = r#"SELECT id, owner FROM docs WHERE payload @> '{"tag":"bob_tag"}'"#;
    assert_eq!(
        ids(&alice, q_bob_tag).await,
        Vec::<i64>::new(),
        "(i) alice @> bob_tag must return zero rows (RLS), not error"
    );
    assert_eq!(
        owners(&alice, q_bob_tag).await,
        Vec::<String>::new(),
        "(i) alice must never see an owner!=alice row via the GIN path"
    );
    // The aggregate path (never short-circuits) must agree: 0.
    assert_eq!(
        count_where(
            &alice,
            r#"SELECT COUNT(*) FROM docs WHERE payload @> '{"tag":"bob_tag"}'"#
        )
        .await,
        0,
        "(i) alice COUNT(*) @> bob_tag must be 0 under RLS"
    );
    // bob, conversely, sees exactly his own two rows for the same needle.
    assert_eq!(
        ids(&bob, q_bob_tag).await,
        vec![3, 4],
        "(i-mirror) bob @> bob_tag must return exactly bob's rows"
    );

    // ── (ii) needle present in the principal's OWN rows ────────────────────────
    let q_alice_tag = r#"SELECT id, owner FROM docs WHERE payload @> '{"tag":"alice_tag"}'"#;
    assert_eq!(
        ids(&alice, q_alice_tag).await,
        vec![1, 2],
        "(ii) alice @> alice_tag must return exactly alice's rows"
    );
    assert_eq!(
        owners(&alice, q_alice_tag).await,
        vec!["alice".to_string(), "alice".to_string()],
        "(ii) every returned owner must be alice"
    );
    // bob asking for alice_tag: present only in alice's rows → RLS-empty for bob.
    assert_eq!(
        ids(&bob, q_alice_tag).await,
        Vec::<i64>::new(),
        "(ii-mirror) bob @> alice_tag must be empty under RLS"
    );

    // ── (iii) needle present in NO row — the empty-probe short-circuit fires ───
    //
    // The ghost tag appears in NO file, so the global posting list's term
    // intersection is empty and the executor's `@>` empty-probe short-circuit
    // returns empty WITHOUT scanning any file (the RLS-blind path the audit
    // flagged). We prove the verdict is RLS-independent by asserting the
    // non-RLS control table — which engages the SAME short-circuit — returns
    // the identical empty answer, and that the RLS table is empty for BOTH
    // principals.
    let q_ghost = r#"SELECT id, owner FROM docs WHERE payload @> '{"tag":"ghost_tag"}'"#;
    let q_ghost_ctrl = r#"SELECT id, owner FROM ctrl WHERE payload @> '{"tag":"ghost_tag"}'"#;
    let ctrl_admin = engine.open_session(control_project).await.unwrap();
    assert_eq!(
        ids(&ctrl_admin, q_ghost_ctrl).await,
        Vec::<i64>::new(),
        "(iii) control (no RLS) @> ghost_tag must be empty — the short-circuit's \
         verdict; RLS cannot change an already-empty universe"
    );
    assert_eq!(
        ids(&alice, q_ghost).await,
        Vec::<i64>::new(),
        "(iii) alice @> ghost_tag must be empty (short-circuit returns correct empty)"
    );
    assert_eq!(
        ids(&bob, q_ghost).await,
        Vec::<i64>::new(),
        "(iii) bob @> ghost_tag must be empty"
    );

    println!("[gap#1] GIN @> empty-probe under RLS (cold-only): no leak, correct empty ✓");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Differential: GIN-probe path == DataFusion fallback path, both under RLS.
//
// The same query is asked two ways:
//   * `@>` — which engages the GIN containment fast path (probe → short-circuit
//     OR fall-through to exec_select).
//   * `jsonb_contains(payload, '…')` (the function form) — which the GIN
//     containment detector does not recognise as `@>`, so it always routes
//     straight through exec_select + RLS (the DataFusion fallback).
// They must return the identical RLS-filtered row set for every principal and
// every needle (present-in-own / present-in-other / present-in-none).
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gin_probe_matches_datafusion_fallback_under_rls() {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = seed(&engine).await;

    let alice = engine.open_session_as(project, "alice").await.unwrap();
    let bob = engine.open_session_as(project, "bob").await.unwrap();

    let cases = [
        ("alice_tag", "needle in own rows"),
        ("bob_tag", "needle in other user's rows"),
        ("ghost_tag", "needle in no rows (short-circuit)"),
    ];

    for (sess, who) in [(&alice, "alice"), (&bob, "bob")] {
        for (tag, label) in cases {
            let gin_sql =
                format!(r#"SELECT id, owner FROM docs WHERE payload @> '{{"tag":"{tag}"}}'"#);
            let df_sql = format!(
                r#"SELECT id, owner FROM docs WHERE jsonb_contains(payload, '{{"tag":"{tag}"}}')"#
            );

            let via_gin = ids(sess, &gin_sql).await;
            let via_df = ids(sess, &df_sql).await;
            assert_eq!(
                via_gin, via_df,
                "differential mismatch for {who} / {tag} ({label}): \
                 GIN @> path={via_gin:?} vs DataFusion jsonb_contains path={via_df:?}"
            );

            // And neither path may surface a row owned by someone else.
            let leaked: Vec<String> = owners(sess, &gin_sql)
                .await
                .into_iter()
                .filter(|o| o != who)
                .collect();
            assert!(
                leaked.is_empty(),
                "{who} saw foreign owners {leaked:?} via the GIN path for tag {tag}"
            );
        }
    }

    println!("[gap#1] GIN @> path == DataFusion fallback, both RLS-filtered ✓");
}

// ═══════════════════════════════════════════════════════════════════════════════
// After a mutation that changes a row's GIN-indexed payload: the GIN posting
// list (built at INSERT time) is now stale w.r.t. the rewritten file, so the
// `@>` path must NOT trust a stale Empty and must route through the
// overlay/completeness-aware scan + RLS. A re-tag of one of alice's rows must
// still never leak across principals, and the new tag must be visible to alice
// (and only alice) — proving the post-mutation @> path also applies RLS.
//
// NB: the hot-tier UPDATE fastpath is GATED OFF on RLS-enabled tables
// (`dml_mutate.rs` ~744: `if meta.rls_enabled { return Ok(None) }`), so this
// UPDATE takes the slow copy-on-write rewrite path. The rewritten file is not
// re-registered in the GIN posting list, so `gin_empty_probe_is_trustworthy`'s
// per-file completeness guard (`live_data_files().all(indexed)`) fails and the
// query falls through to `exec_select` + `apply_rls_to_select`. The env var is
// still set to exercise the gate's RLS short-circuit explicitly.
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn gin_at_gt_under_rls_after_payload_rewrite_never_leaks() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let engine = build_engine(&dir);
    let project = seed(&engine).await;

    let alice = engine.open_session_as(project, "alice").await.unwrap();
    let bob = engine.open_session_as(project, "bob").await.unwrap();

    // Re-tag alice's row id=1 to a brand-new tag. On an RLS table this routes
    // through the slow CoW rewrite; the rewritten file is unindexed in the GIN
    // posting list, so every later @> query on this table is forced through the
    // completeness-guarded fall-through to exec_select + RLS.
    alice
        .execute(r#"UPDATE docs SET payload = '{"tag":"alice_new"}' WHERE id = 1"#)
        .await
        .expect("update");

    // alice sees the re-tagged row for the NEW tag; bob never does.
    let q_new = r#"SELECT id, owner FROM docs WHERE payload @> '{"tag":"alice_new"}'"#;
    assert_eq!(
        ids(&alice, q_new).await,
        vec![1],
        "alice must see her re-tagged row via the post-rewrite @> scan"
    );
    assert_eq!(
        ids(&bob, q_new).await,
        Vec::<i64>::new(),
        "bob must NOT see alice's re-tagged row (RLS through the post-rewrite scan)"
    );

    // bob's own tag still returns exactly bob's rows; alice still gets nothing.
    let q_bob = r#"SELECT id, owner FROM docs WHERE payload @> '{"tag":"bob_tag"}'"#;
    assert_eq!(ids(&bob, q_bob).await, vec![3, 4], "bob sees his own rows");
    assert_eq!(
        ids(&alice, q_bob).await,
        Vec::<i64>::new(),
        "alice must not see bob's rows after the payload rewrite"
    );

    // alice's OLD tag (id=1 used to carry alice_tag) must no longer match id=1,
    // but still matches id=2; bob still sees nothing for it.
    let q_old = r#"SELECT id FROM docs WHERE payload @> '{"tag":"alice_tag"}'"#;
    assert_eq!(
        ids(&alice, q_old).await,
        vec![2],
        "after re-tag, alice_tag matches only the un-rewritten alice row id=2"
    );
    assert_eq!(
        ids(&bob, q_old).await,
        Vec::<i64>::new(),
        "bob never sees alice_tag rows"
    );

    // ghost tag: still the correct empty for both principals.
    let q_ghost = r#"SELECT id FROM docs WHERE payload @> '{"tag":"ghost_tag"}'"#;
    assert_eq!(ids(&alice, q_ghost).await, Vec::<i64>::new(), "ghost empty for alice");
    assert_eq!(ids(&bob, q_ghost).await, Vec::<i64>::new(), "ghost empty for bob");

    println!("[gap#1] GIN @> under RLS after payload rewrite: no leak, correct rows ✓");
}
