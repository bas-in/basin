//! The durable per-project CDC ring on object storage.
//!
//! Layout (ADR 0028 §5, `cdc-design.md` §2):
//!
//! ```text
//! {root}/cdc/{project_id}/{seq_lo_hex}/{ulid}.cdc      ← NDJSON segments
//! {root}/cdc/{project_id}/_index.json                  ← min/max seq index
//! ```
//!
//! `{seq_lo_hex}` is the lower 32 bits of the segment's first `seq`,
//! left-padded to 8 hex digits, so an object-store prefix LIST returns
//! segments in seq order (within a 4-billion-event window — far beyond one
//! retention window at realistic rates).
//!
//! ## Crash-loss window (honest semantics)
//!
//! The CDC ring is the durability contract (ADR §5). The Phase-1 sink writes
//! each commit's events to the ring **synchronously inside the post-commit
//! hook**, before the spawned sink task returns. Because [`crate::CdcRingWriter`]
//! is attached as a *post-commit* sink, the commit itself has already been
//! made durable by the engine before the ring PUT is attempted: a crash
//! between catalog-commit and ring-PUT loses the CDC *record* for that commit
//! (the row is committed and visible, but absent from the stream) — the
//! at-least-once contract is "delivered at-least-once **if the ring PUT
//! succeeds**". The PUT is retried with bounded backoff to shrink this window;
//! ADR "Trigger to revisit" §3 documents the async-pre-buffer tradeoff that
//! would widen it in exchange for latency. We do NOT block the commit path on
//! the PUT (post-commit sinks must not stall the engine), so the window is
//! `commit → PUT-success`, bounded by the retry budget below. This is the
//! same shape as the realtime `BUFFER_FULL` path but with durability retries.

use std::sync::Arc;
use std::time::Duration;

use basin_common::ProjectId;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use crate::record::{decode_segment, encode_segment, CdcRecord};

/// Max records per segment before a flush is forced (`cdc-design.md` §2:
/// "up to 1 000 events or 1 MiB").
pub const SEGMENT_MAX_RECORDS: usize = 1_000;
/// Max encoded bytes per segment before a flush is forced.
pub const SEGMENT_MAX_BYTES: usize = 1 << 20; // 1 MiB
/// PUT retry budget for a single segment (durability, not the commit path).
const PUT_MAX_ATTEMPTS: u32 = 5;
const PUT_BACKOFF_BASE: Duration = Duration::from_millis(20);

/// The lightweight index object content (`cdc-design.md` §2).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CdcIndex {
    pub min_seq: u64,
    pub max_seq: u64,
    pub segment_count: u64,
    #[serde(default)]
    pub oldest_segment_key: Option<String>,
    #[serde(default)]
    pub newest_segment_key: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

/// Object-key helpers. The project-scoped store
/// (`Storage::project_object_store`) is a pass-through wrapper that does NOT
/// rewrite keys (it only meters + gates I/O), so — exactly like `Storage`'s
/// own key builders — we prepend the configured `root_prefix` ourselves. The
/// CDC tree lives at `{root}/cdc/{project}/...`, a sibling of the engine's
/// `{root}/projects/{project}/...` data tree.
pub(crate) fn cdc_prefix(root: Option<&str>, project: &ProjectId) -> String {
    match root {
        Some(r) if !r.is_empty() => format!("{}/cdc/{}", r.trim_end_matches('/'), project),
        _ => format!("cdc/{}", project),
    }
}

pub(crate) fn index_key(root: Option<&str>, project: &ProjectId) -> ObjectPath {
    ObjectPath::from(format!("{}/_index.json", cdc_prefix(root, project)))
}

fn seq_shard(first_seq: u64) -> String {
    format!("{:08x}", (first_seq & 0xFFFF_FFFF) as u32)
}

fn segment_key(root: Option<&str>, project: &ProjectId, first_seq: u64) -> (String, ObjectPath) {
    // ULID monotonic-by-time → newest-last within a shard; the shard prefix
    // (seq_lo) gives coarse seq ordering across shards.
    let ulid = ulid::Ulid::new().to_string();
    let key = format!(
        "{}/{}/{}.cdc",
        cdc_prefix(root, project),
        seq_shard(first_seq),
        ulid
    );
    (key.clone(), ObjectPath::from(key))
}

/// Durable per-project ring backed by a project-scoped [`ObjectStore`]. One
/// instance per project; the [`crate::CdcRingWriter`] sink owns a registry of
/// these keyed by project so a slow project never blocks another (ADR §6
/// fan-out isolation).
pub struct ProjectRing {
    project: ProjectId,
    store: Arc<dyn ObjectStore>,
    /// Configured `root_prefix` (string form), prepended to every key so the
    /// CDC tree honours the deployment's bucket root. `None` in tests.
    root: Option<String>,
}

impl ProjectRing {
    /// Build a ring with no root prefix (tests, or a bucket whose root is "").
    pub fn new(project: ProjectId, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            project,
            store,
            root: None,
        }
    }

    /// Build a ring honouring the configured `root_prefix`.
    pub fn with_root(
        project: ProjectId,
        store: Arc<dyn ObjectStore>,
        root: Option<String>,
    ) -> Self {
        Self {
            project,
            store,
            root,
        }
    }

    fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    /// Append a batch of records as one segment and update the index. The
    /// batch is the events of a single commit (or a slice of it if it exceeds
    /// [`SEGMENT_MAX_RECORDS`]). Records must be pre-sorted by `seq`.
    ///
    /// Returns the segment key written. Retries the PUT with bounded backoff
    /// (durability); the index update is best-effort (last-writer-wins;
    /// values are monotonic so a lost race is self-healing).
    pub async fn append(&self, records: &[CdcRecord]) -> Result<String, object_store::Error> {
        if records.is_empty() {
            return Ok(String::new());
        }
        let first_seq = records[0].seq;
        let last_seq = records[records.len() - 1].seq;
        let (key_str, key) = segment_key(self.root(), &self.project, first_seq);
        let body = encode_segment(records).map_err(|e| object_store::Error::Generic {
            store: "cdc",
            source: Box::new(e),
        })?;
        let payload = PutPayload::from_bytes(Bytes::from(body));

        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.store.put(&key, payload.clone()).await {
                Ok(_) => break,
                Err(e) if attempt < PUT_MAX_ATTEMPTS => {
                    tracing::warn!(
                        project = %self.project,
                        attempt,
                        error = %e,
                        "cdc: segment PUT failed; retrying",
                    );
                    tokio::time::sleep(PUT_BACKOFF_BASE * attempt).await;
                }
                Err(e) => {
                    tracing::error!(
                        project = %self.project,
                        error = %e,
                        "cdc: segment PUT exhausted retries; record lost from ring",
                    );
                    return Err(e);
                }
            }
        }

        self.update_index_after_write(&key_str, first_seq, last_seq)
            .await;
        Ok(key_str)
    }

    /// Read the index object, or a fresh default if none exists yet.
    pub async fn read_index(&self) -> CdcIndex {
        match self.store.get(&index_key(self.root(), &self.project)).await {
            Ok(r) => match r.bytes().await {
                Ok(b) => serde_json::from_slice(&b).unwrap_or_default(),
                Err(_) => CdcIndex::default(),
            },
            Err(_) => CdcIndex::default(),
        }
    }

    async fn write_index(&self, idx: &CdcIndex) {
        let body = match serde_json::to_vec(idx) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "cdc: index serialize failed");
                return;
            }
        };
        if let Err(e) = self
            .store
            .put(
                &index_key(self.root(), &self.project),
                PutPayload::from_bytes(Bytes::from(body)),
            )
            .await
        {
            // Index is a seek hint, not the durability contract — a failed
            // update is recoverable by a full LIST. Warn, don't fail.
            tracing::warn!(project = %self.project, error = %e, "cdc: index PUT failed");
        }
    }

    async fn update_index_after_write(&self, seg_key: &str, first_seq: u64, last_seq: u64) {
        let mut idx = self.read_index().await;
        if idx.segment_count == 0 {
            idx.min_seq = first_seq;
            idx.oldest_segment_key = Some(seg_key.to_string());
        } else {
            idx.min_seq = idx.min_seq.min(first_seq);
        }
        idx.max_seq = idx.max_seq.max(last_seq);
        idx.newest_segment_key = Some(seg_key.to_string());
        idx.segment_count += 1;
        idx.updated_at = Some(Utc::now().to_rfc3339());
        self.write_index(&idx).await;
    }

    /// List all segment object keys for this project, in prefix (≈seq) order.
    async fn list_segment_keys(&self) -> Result<Vec<ObjectPath>, object_store::Error> {
        let prefix = ObjectPath::from(cdc_prefix(self.root(), &self.project));
        let metas: Vec<object_store::ObjectMeta> =
            self.store.list(Some(&prefix)).try_collect().await?;
        let mut keys: Vec<ObjectPath> = metas
            .into_iter()
            .map(|m| m.location)
            .filter(|loc| loc.as_ref().ends_with(".cdc"))
            .collect();
        keys.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(keys)
    }

    /// Replay every record with `seq > after_seq`, in seq order, from the
    /// durable ring. Used for cursor resume.
    pub async fn replay_after(
        &self,
        after_seq: u64,
    ) -> Result<Vec<CdcRecord>, object_store::Error> {
        let keys = self.list_segment_keys().await?;
        let mut out: Vec<CdcRecord> = Vec::new();
        for key in keys {
            let body = self.store.get(&key).await?.bytes().await?;
            for rec in decode_segment(&body) {
                if rec.seq > after_seq {
                    out.push(rec);
                }
            }
        }
        out.sort_by_key(|r| r.seq);
        Ok(out)
    }

    /// The smallest `seq` still present in the ring (after retention GC). Used
    /// to decide whether a resume cursor predates the window (→ gap signal).
    /// `None` when the ring is empty.
    pub async fn min_available_seq(&self) -> Result<Option<u64>, object_store::Error> {
        let keys = self.list_segment_keys().await?;
        let mut min: Option<u64> = None;
        for key in keys {
            let body = self.store.get(&key).await?.bytes().await?;
            for rec in decode_segment(&body) {
                min = Some(min.map_or(rec.seq, |m| m.min(rec.seq)));
            }
        }
        Ok(min)
    }

    /// Delete every segment whose newest record is older than `cutoff`
    /// (retention GC). A segment is kept if ANY of its records is at/after the
    /// cutoff — we never split a segment. Returns the number of segments
    /// deleted. Rebuilds the index `min_seq`/`oldest_segment_key` afterward.
    pub async fn prune_older_than(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<usize, object_store::Error> {
        let cutoff_micros = cutoff.timestamp_micros();
        let keys = self.list_segment_keys().await?;
        let mut deleted = 0usize;
        let mut surviving_min: Option<u64> = None;
        let mut surviving_oldest: Option<String> = None;

        for key in keys {
            let body = self.store.get(&key).await?.bytes().await?;
            let recs = decode_segment(&body);
            let newest = recs.iter().map(|r| r.committed_at).max().unwrap_or(0);
            if newest < cutoff_micros && !recs.is_empty() {
                self.store.delete(&key).await?;
                deleted += 1;
            } else if let Some(seg_min) = recs.iter().map(|r| r.seq).min() {
                if surviving_min.map_or(true, |m| seg_min < m) {
                    surviving_min = Some(seg_min);
                    surviving_oldest = Some(key.as_ref().to_string());
                }
            }
        }

        if deleted > 0 {
            let mut idx = self.read_index().await;
            idx.min_seq = surviving_min.unwrap_or(idx.max_seq);
            idx.oldest_segment_key = surviving_oldest;
            idx.segment_count = idx.segment_count.saturating_sub(deleted as u64);
            idx.updated_at = Some(Utc::now().to_rfc3339());
            self.write_index(&idx).await;
        }
        Ok(deleted)
    }
}
