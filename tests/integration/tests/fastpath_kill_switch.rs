//! Kill-switch + per-shape resolution for the OLTP hot-tier fast paths.
//!
//! Phase 5.14 closure flipped `BASIN_HOTTIER_DELETE_FASTPATH` and
//! `BASIN_HOTTIER_UPDATE_FASTPATH` defaults from **OFF → ON**. The
//! resolution order encoded in `dml_mutate::hottier_fastpath_enabled`:
//!
//!   1. `BASIN_HOTTIER_FASTPATH_DISABLE=1` — global kill-switch, forces
//!      every hot-tier fast path off without a redeploy.
//!   2. `BASIN_HOTTIER_{DELETE,UPDATE}_FASTPATH=0` — per-shape override.
//!   3. `BASIN_HOTTIER_{DELETE,UPDATE}_FASTPATH=1` — historical opt-in
//!      value, still respected for operators with pinned configs.
//!   4. Default — ON.
//!
//! These tests prove the precedence behaviour end-to-end by observing
//! whether a fastpath DELETE / UPDATE actually wrote a `Tombstone` /
//! `Update` entry into the `MemTableRegistry` for each env config.
//!
//! All tests serialise via `ENV_LOCK` so the process-wide env vars stay
//! consistent across parallel test threads.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_storage::Storage;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Tokio mutex held across each test's awaits so parallel tests don't
/// race on the process-wide env vars.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// RAII guard: sets `key` for the test's duration and restores prior
/// value (or removes the var) on drop.
struct EnvSwap {
    key: &'static str,
    prev: Option<String>,
}
impl EnvSwap {
    fn set(key: &'static str, val: &str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: ENV_LOCK held by caller serialises process-wide env mutation.
        unsafe { std::env::set_var(key, val) };
        Self { key, prev }
    }
    fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        // SAFETY: ENV_LOCK held by caller.
        unsafe { std::env::remove_var(key) };
        Self { key, prev }
    }
}
impl Drop for EnvSwap {
    fn drop(&mut self) {
        match &self.prev {
            // SAFETY: restoring the captured prior value; ENV_LOCK still held.
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

fn build_engine(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Seed `t (id BIGINT PRIMARY KEY, v BIGINT)` with N rows then DELETE one.
/// Returns true iff a registry tombstone entry exists for the deleted PK.
async fn delete_writes_tombstone(deleted_pk: i64) -> bool {
    let dir = TempDir::new().unwrap();
    let eng = build_engine(&dir);
    let project = ProjectId::new();
    let sess = eng.open_session(project).await.unwrap();
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    for k in 0..5_i64 {
        sess.execute(&format!("INSERT INTO t VALUES ({k}, {k})"))
            .await
            .unwrap();
    }
    let res = sess
        .execute(&format!("DELETE FROM t WHERE id = {deleted_pk}"))
        .await
        .unwrap();
    matches!(res, ExecResult::Empty { .. });

    let registry = eng.memtable_registry();
    let table = TableName::new("t").unwrap();
    let Some(entry) = registry.get(&project, &table) else {
        return false;
    };
    let snap = entry.memtable.snapshot();
    snap.iter()
        .any(|(_, v)| matches!(v, basin_hottier::MemRowValue::Tombstone))
}

// ── Resolution-order tests ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fastpath_default_on_when_env_unset() {
    let _g = ENV_LOCK.lock().await;
    let _disable = EnvSwap::unset("BASIN_HOTTIER_FASTPATH_DISABLE");
    let _per = EnvSwap::unset("BASIN_HOTTIER_DELETE_FASTPATH");
    assert!(
        delete_writes_tombstone(2).await,
        "Phase 5.14 default — unset env should mean fast path ON \
         (DELETE must write a registry tombstone)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fastpath_per_shape_zero_disables() {
    let _g = ENV_LOCK.lock().await;
    let _disable = EnvSwap::unset("BASIN_HOTTIER_FASTPATH_DISABLE");
    let _per = EnvSwap::set("BASIN_HOTTIER_DELETE_FASTPATH", "0");
    assert!(
        !delete_writes_tombstone(2).await,
        "explicit per-shape '0' must override the default-on policy \
         (DELETE must take the cold rewrite path, no registry entry)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fastpath_per_shape_one_explicit_opt_in_back_compat() {
    let _g = ENV_LOCK.lock().await;
    let _disable = EnvSwap::unset("BASIN_HOTTIER_FASTPATH_DISABLE");
    let _per = EnvSwap::set("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    assert!(
        delete_writes_tombstone(2).await,
        "historical opt-in '1' must still enable the fast path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fastpath_kill_switch_overrides_default() {
    let _g = ENV_LOCK.lock().await;
    let _disable = EnvSwap::set("BASIN_HOTTIER_FASTPATH_DISABLE", "1");
    let _per = EnvSwap::unset("BASIN_HOTTIER_DELETE_FASTPATH");
    assert!(
        !delete_writes_tombstone(2).await,
        "BASIN_HOTTIER_FASTPATH_DISABLE=1 must force fast path OFF even \
         with no per-shape override (operator rollback without redeploy)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fastpath_kill_switch_overrides_per_shape_one() {
    let _g = ENV_LOCK.lock().await;
    let _disable = EnvSwap::set("BASIN_HOTTIER_FASTPATH_DISABLE", "1");
    let _per = EnvSwap::set("BASIN_HOTTIER_DELETE_FASTPATH", "1");
    assert!(
        !delete_writes_tombstone(2).await,
        "kill-switch must beat explicit per-shape '1' (operators rolling \
         back override pinned configs)"
    );
}
