//! The durable CDC record — the on-disk shape of one committed change.
//!
//! See `docs/cdc-design.md` §2. The ADR specifies CBOR for compactness;
//! Phase 1 serialises each record as one line of JSON (NDJSON) so the ring
//! has no external codec dependency (the workspace carries no CBOR crate and
//! its `Cargo.toml` is frozen for this change). The field set is identical to
//! the design's `CdcRecord`; switching the wire codec to CBOR in Phase 2 is a
//! drop-in change behind [`CdcRecord::encode_segment`] / [`decode_segment`].

use basin_common::{ChangeEvent, ChangeOp, ProjectId};
use serde::{Deserialize, Serialize};

/// Numeric op code matching `docs/cdc-design.md` §2 (1=insert, 2=update,
/// 3=delete). Stored as a small int so a future CBOR codec stays compact.
pub(crate) fn op_code(op: ChangeOp) -> u8 {
    match op {
        ChangeOp::Insert => 1,
        ChangeOp::Update => 2,
        ChangeOp::Delete => 3,
    }
}

/// One durable change record. Mirrors the [`ChangeEvent`] captured at the
/// post-commit hook, plus the internal `wal_lsn` bookmark the design reserves
/// for the Phase 5 pgoutput bridge.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdcRecord {
    /// Monotonic per-project cursor. The public resume token.
    pub seq: u64,
    /// WAL LSN at commit time. Internal; Phase 5 (pgoutput) use only. The
    /// `ChangeEvent` captured at the commit hook does not currently carry an
    /// LSN, so Phase 1 stores `0` here as an honest placeholder — see the
    /// crate docs and ADR §4 ("not exposed externally in Phases 1–4").
    #[serde(default)]
    pub wal_lsn: u64,
    pub project: ProjectId,
    pub table: String,
    /// 1=insert, 2=update, 3=delete.
    pub op: u8,
    /// Phase 3+ engine-instance tx id. Always `None` in Phase 1 (no tx
    /// grouping markers in the wire protocol per ADR Phase-1 non-goals).
    #[serde(default)]
    pub tx_id: Option<u64>,
    /// UTC unix micros at commit.
    pub committed_at: i64,
    #[serde(default)]
    pub causation_user: Option<String>,
    /// `null` for insert.
    pub before: Option<serde_json::Value>,
    /// `null` for delete.
    pub after: Option<serde_json::Value>,
}

impl CdcRecord {
    /// Build a durable record from a captured [`ChangeEvent`]. `wal_lsn` is
    /// supplied by the caller (0 when unavailable at the hook — see field doc).
    pub fn from_event(event: &ChangeEvent, wal_lsn: u64) -> Self {
        Self {
            seq: event.seq,
            wal_lsn,
            project: event.project,
            table: event.table.as_str().to_string(),
            op: op_code(event.op),
            tx_id: None,
            committed_at: event.committed_at.timestamp_micros(),
            causation_user: event.causation_user.clone(),
            before: event.before.clone(),
            after: event.after.clone(),
        }
    }

    /// The op as a lowercase string, for the SSE wire frame and filter
    /// matching (`insert` / `update` / `delete`).
    pub fn op_name(&self) -> &'static str {
        match self.op {
            1 => "insert",
            2 => "update",
            3 => "delete",
            _ => "unknown",
        }
    }
}

/// Encode a batch of records as one NDJSON segment body. One record per line;
/// trailing newline. Append-only segments are never rewritten, so a partial
/// final line can only result from a crash mid-PUT — and object-store PUTs are
/// atomic (the object either exists whole or not at all), so a reader never
/// sees a torn segment.
pub fn encode_segment(records: &[CdcRecord]) -> Result<Vec<u8>, serde_json::Error> {
    let mut buf = Vec::with_capacity(records.len() * 128);
    for r in records {
        serde_json::to_writer(&mut buf, r)?;
        buf.push(b'\n');
    }
    Ok(buf)
}

/// Decode an NDJSON segment body back into records. Blank lines are skipped;
/// a malformed line is logged and skipped (forward-compat: a future codec or
/// added field must not poison the whole segment for an old reader).
pub fn decode_segment(body: &[u8]) -> Vec<CdcRecord> {
    let mut out = Vec::new();
    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice::<CdcRecord>(line) {
            Ok(r) => out.push(r),
            Err(e) => {
                tracing::warn!(error = %e, "cdc: skipping unparseable segment line");
            }
        }
    }
    out
}

/// The SSE `data:` frame for one delivered record. `op` is the lowercase
/// string form; `before`/`after` are omitted when null (matches the realtime
/// SSE convention in `basin-realtime/src/sse.rs`).
#[derive(Serialize)]
pub struct CdcSseFrame<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub seq: u64,
    pub table: &'a str,
    pub op: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<&'a serde_json::Value>,
}

impl<'a> CdcSseFrame<'a> {
    pub fn event(rec: &'a CdcRecord) -> Self {
        Self {
            kind: "event",
            seq: rec.seq,
            table: &rec.table,
            op: match rec.op {
                1 => "insert",
                2 => "update",
                3 => "delete",
                _ => "unknown",
            },
            before: rec.before.as_ref(),
            after: rec.after.as_ref(),
        }
    }
}
