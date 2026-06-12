//! Background retention GC for the CDC ring (ADR 0028 §5, commit 4).
//!
//! On a tick (`BASIN_CDC_GC_INTERVAL_SECS`, default 1h) every project's ring
//! is pruned of segments older than the retention window. CDC retention is
//! age-based and **independent of WAL truncation** — the WAL truncates when
//! the hot tier flushes; the CDC ring truncates on age, decoupling a CDC
//! consumer's lag from the flush cadence.
//!
//! ## Project discovery
//!
//! The GC needs the set of projects that have a ring. Phase 1 derives this by
//! listing the `cdc/` prefix on the shared object store. (A per-project
//! enumerator from the catalog would be tighter, but the frozen
//! `basin-catalog` surface and the prompt's "Phase 1 scope only" rule make the
//! LIST-driven approach the smaller change; it is exact because a project only
//! appears under `cdc/` once it has emitted at least one event.)

use std::time::Duration;

use basin_common::ProjectId;
use chrono::Utc;
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use crate::CdcRingWriter;

/// Handle to a running retention GC task. Dropping it stops the loop.
pub struct RetentionGc {
    handle: tokio::task::JoinHandle<()>,
}

impl RetentionGc {
    pub fn abort(self) {
        self.handle.abort();
    }
}

/// Spawn the retention GC loop for `writer`. Prunes every discovered project's
/// ring every [`crate::CdcConfig::gc_interval`].
pub fn spawn_retention_gc(writer: CdcRingWriter) -> RetentionGc {
    let interval = writer.config().gc_interval;
    let retention = writer.config().retention;
    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(e) = run_once(&writer, retention).await {
                tracing::warn!(error = %e, "cdc: retention GC sweep failed");
            }
        }
    });
    RetentionGc { handle }
}

/// One GC sweep across all discovered projects. Exposed (crate-public via the
/// re-export) for the deterministic GC test that does not want to wait a tick.
pub async fn run_once(
    writer: &CdcRingWriter,
    retention: Duration,
) -> Result<usize, object_store::Error> {
    let cutoff = Utc::now()
        - chrono::Duration::from_std(retention).unwrap_or_else(|_| chrono::Duration::hours(24));
    let mut total = 0usize;
    for project in discover_projects(writer).await? {
        let ring = writer.ring_for(&project);
        match ring.prune_older_than(cutoff).await {
            Ok(n) => total += n,
            Err(e) => tracing::warn!(project = %project, error = %e, "cdc: prune failed"),
        }
    }
    Ok(total)
}

/// Enumerate projects that have a CDC ring by listing the `cdc/<project>/`
/// segments under the shared store. Uses the un-scoped object-store handle so
/// it can see across projects (the per-project scoped store would hide other
/// projects' prefixes); the only keys read are CDC keys, never table data.
async fn discover_projects(writer: &CdcRingWriter) -> Result<Vec<ProjectId>, object_store::Error> {
    let store = writer.storage().object_store_handle();
    let root = writer.storage().root_prefix_handle();
    let prefix = match &root {
        Some(r) => ObjectPath::from(format!("{}/cdc", r)),
        None => ObjectPath::from("cdc"),
    };
    let metas: Vec<object_store::ObjectMeta> = store.list(Some(&prefix)).try_collect().await?;
    let mut seen: std::collections::HashSet<ProjectId> = std::collections::HashSet::new();
    for m in metas {
        // key shape: [{root}/]cdc/{project}/...  — pull the segment right
        // after the literal `cdc`.
        let parts: Vec<&str> = m.location.as_ref().split('/').collect();
        if let Some(pos) = parts.iter().position(|p| *p == "cdc") {
            if let Some(proj) = parts.get(pos + 1) {
                if let Ok(pid) = proj.parse::<ProjectId>() {
                    seen.insert(pid);
                }
            }
        }
    }
    Ok(seen.into_iter().collect())
}
