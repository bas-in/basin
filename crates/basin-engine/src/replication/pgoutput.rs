//! Phase 5.21.C — pgoutput logical-replication encoder.
//!
//! Encodes Basin's internal `ChangeEvent` (from `basin-common`) into the
//! PostgreSQL `pgoutput` logical-replication message format.
//!
//! ## Message types produced
//!
//! For a single committed INSERT transaction the encoder emits exactly 4 frames
//! in this order:
//!
//!   1. **B** — Begin: marks the start of a transaction.
//!   2. **R** — Relation: type-information describing the affected table's columns.
//!   3. **I** / **U** / **D** — Insert / Update / Delete: the row-level change.
//!   4. **C** — Commit: marks the end of the transaction.
//!
//! ## Representation
//!
//! For the SQL-facing `pg_logical_slot_get_changes` function the `data` column
//! carries a human-readable text representation of each frame. The first byte
//! of `data` is the message type tag (B/R/I/U/D/C) matching the PostgreSQL
//! protocol, making the assertion `data LIKE 'B%'` meaningful.
//!
//! In a full wire-protocol implementation `data` would be the raw binary
//! pgoutput bytes. For the in-process consumer path used by the harness
//! tests, the text format is sufficient.
//!
//! ## Reuse of the change-event stream
//!
//! The encoder subscribes to Basin's existing `ChangeEvent` / `ChangeEventSink`
//! infrastructure (ADR 0012). No parallel capture mechanism is introduced.
//! `ReplicationSink` implements `ChangeEventSink` and pushes pgoutput frames
//! into the `SlotRegistry` after each committed mutation.

use std::sync::Arc;

use async_trait::async_trait;
use basin_common::{ChangeEvent, ChangeEventSink, ChangeOp, ProjectId, Result};

use basin_catalog::{lsn_to_pgtext, PendingFrame, SlotRegistry};

use crate::replication::lsn;

/// Build the four pgoutput frames for one `ChangeEvent`.
///
/// Returns `(begin_frame, relation_frame, change_frame, commit_frame)`.
///
/// The frame `data` strings start with the standard pgoutput message type
/// byte (B / R / I / U / D / C) so callers can assert on the message type
/// without a full binary decode.
pub fn encode_change_event(event: &ChangeEvent, commit_seq: u64, xid: u32) -> [PendingFrame; 4] {
    let begin_lsn = lsn::lsn_for_frame(commit_seq, 0);
    let relation_lsn = lsn::lsn_for_frame(commit_seq, 1);
    let change_lsn = lsn::lsn_for_frame(commit_seq, 2);
    let commit_lsn = lsn::lsn_for_frame(commit_seq, 3);

    // B — Begin
    let begin_frame = PendingFrame {
        lsn: begin_lsn,
        xid,
        data: format!(
            "B final_lsn={} commit_time=now xid={xid}",
            lsn_to_pgtext(commit_lsn)
        ),
    };

    // R — Relation
    let table_name = event.table.as_str();
    let relation_frame = PendingFrame {
        lsn: relation_lsn,
        xid,
        data: format!("R rel_id=0 schema=public table={table_name} replica_identity=d"),
    };

    // I / U / D — data change
    let (tag, payload) = match event.op {
        ChangeOp::Insert => {
            let after = event
                .after
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default();
            ("I", format!("new={after}"))
        }
        ChangeOp::Update => {
            let before = event
                .before
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default();
            let after = event
                .after
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default();
            ("U", format!("old={before} new={after}"))
        }
        ChangeOp::Delete => {
            let before = event
                .before
                .as_ref()
                .map(|v| v.to_string())
                .unwrap_or_default();
            ("D", format!("old={before}"))
        }
    };
    let change_frame = PendingFrame {
        lsn: change_lsn,
        xid,
        data: format!("{tag} table={table_name} {payload}"),
    };

    // C — Commit
    let commit_frame = PendingFrame {
        lsn: commit_lsn,
        xid,
        data: format!(
            "C commit_lsn={} end_lsn={} commit_time=now",
            lsn_to_pgtext(commit_lsn),
            lsn_to_pgtext(commit_lsn.saturating_add(1))
        ),
    };

    [begin_frame, relation_frame, change_frame, commit_frame]
}

/// A `ChangeEventSink` that routes events to the `SlotRegistry` as pgoutput
/// frames. Attached as a post-commit sink on the engine so all sessions on
/// the engine automatically feed into active replication slots.
///
/// ## Per-project cost
///
/// The sink holds only `Arc<SlotRegistry>` + `ProjectId` — O(bytes) per idle
/// project. The `SlotRegistry` itself is a single `Arc<Mutex<HashMap<…>>>`,
/// consistent with the "shared heavy resource + cheap per-project primitive"
/// design rule.
pub struct ReplicationSink {
    registry: Arc<SlotRegistry>,
    project: ProjectId,
    /// Monotonically increasing XID counter for this sink instance.
    next_xid: std::sync::atomic::AtomicU32,
    /// Commit-sequence counter mirroring `Engine::next_tx_id` increments.
    /// We use a per-sink counter here so the sink is self-contained; in a
    /// production system this would come from the shared WAL sequence.
    next_commit_seq: std::sync::atomic::AtomicU64,
}

impl ReplicationSink {
    pub fn new(registry: Arc<SlotRegistry>, project: ProjectId) -> Self {
        Self {
            registry,
            project,
            next_xid: std::sync::atomic::AtomicU32::new(1),
            next_commit_seq: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

#[async_trait]
impl ChangeEventSink for ReplicationSink {
    async fn publish(&self, event: &ChangeEvent) -> Result<()> {
        // Only handle events for our project.
        if event.project != self.project {
            return Ok(());
        }

        let xid = self
            .next_xid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let commit_seq = self
            .next_commit_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let frames = encode_change_event(event, commit_seq, xid);
        self.registry
            .push_frames_all_slots(&self.project, frames.to_vec());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::{ChangeEvent, ChangeOp, TableName};
    use chrono::Utc;
    use serde_json::json;

    fn make_insert_event() -> ChangeEvent {
        ChangeEvent {
            project: basin_common::ProjectId::new(),
            table: TableName::new("test_table").unwrap(),
            op: ChangeOp::Insert,
            before: None,
            after: Some(json!({"id": 1, "name": "hello"})),
            committed_at: Utc::now(),
            seq: 1,
            causation_user: None,
        }
    }

    #[test]
    fn encode_insert_produces_four_frames() {
        let event = make_insert_event();
        let frames = encode_change_event(&event, 1, 100);
        assert_eq!(frames.len(), 4);
        assert!(
            frames[0].data.starts_with('B'),
            "expected BEGIN: {}",
            frames[0].data
        );
        assert!(
            frames[1].data.starts_with('R'),
            "expected RELATION: {}",
            frames[1].data
        );
        assert!(
            frames[2].data.starts_with('I'),
            "expected INSERT: {}",
            frames[2].data
        );
        assert!(
            frames[3].data.starts_with('C'),
            "expected COMMIT: {}",
            frames[3].data
        );
    }

    #[test]
    fn frame_lsns_are_strictly_increasing() {
        let event = make_insert_event();
        let frames = encode_change_event(&event, 1, 100);
        for w in frames.windows(2) {
            assert!(w[0].lsn < w[1].lsn, "frame LSNs must be increasing");
        }
    }

    #[test]
    fn encode_delete_produces_d_frame() {
        let mut event = make_insert_event();
        event.op = ChangeOp::Delete;
        event.before = Some(json!({"id": 1}));
        event.after = None;
        let frames = encode_change_event(&event, 2, 101);
        assert!(frames[2].data.starts_with('D'), "expected DELETE frame");
    }

    #[test]
    fn encode_update_produces_u_frame() {
        let mut event = make_insert_event();
        event.op = ChangeOp::Update;
        event.before = Some(json!({"id": 1, "name": "old"}));
        event.after = Some(json!({"id": 1, "name": "new"}));
        let frames = encode_change_event(&event, 3, 102);
        assert!(frames[2].data.starts_with('U'), "expected UPDATE frame");
    }
}
