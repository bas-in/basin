//! On-disk segment format for the file-backed WAL.
//!
//! A segment is a sequence of length-prefixed bincode-encoded records. The
//! first record is always a [`SegmentHeader`]; subsequent records are
//! [`SegmentRecord::Entry`] payloads. Records are framed as:
//!
//! ```text
//! [u32 little-endian length][bincode-encoded record bytes]
//! ```
//!
//! Format is internal to this crate. Callers see [`crate::WalEntry`] only.

use basin_common::{BasinError, PartitionKey, ProjectId, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::Lsn;

/// Header record. First entry in every segment. Lets recovery identify the
/// segment without reading every entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SegmentHeader {
    pub format_version: u16,
    pub project: ProjectId,
    pub partition: PartitionKey,
    pub first_lsn: Lsn,
    pub segment_id: Ulid,
}

/// Wire-format payload entry. Mirrors [`crate::WalEntry`] but owns the bytes
/// for ser/de.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EntryRecord {
    pub project: ProjectId,
    pub partition: PartitionKey,
    pub lsn: Lsn,
    pub payload: Vec<u8>,
    pub appended_at: DateTime<Utc>,
}

/// Tagged wrapper so a segment can hold a header, a data entry, or a
/// transaction lifecycle marker. Bincode handles the discriminant.
///
/// Variants added in v0.2 (`TxBegin`, `TxRollback`) are forward-compatible:
/// a v0.1 reader that encounters an unknown bincode discriminant will return a
/// decode error (surfaced as a WAL read error — not silent data loss).
/// The replay path in [`crate::replay_wal`] handles both.
///
/// ADR 0020 §6: `TxCommit` (variant index 4, tag `0x04` in bincode enum
/// encoding) is added in v0.3. Replay now requires an explicit `TxCommit`
/// marker to honour a transaction; absence of `TxCommit` at EOF means the
/// writer crashed mid-transaction and the buffered entries are discarded.
/// Old segments (pre-v0.3) that reach EOF with an open `TxBegin` and no
/// `TxCommit` are treated as implicit commits for backwards compatibility
/// (the `legacy_implicit_commit` flag in the replay path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum SegmentRecord {
    Header(SegmentHeader),
    Entry(EntryRecord),
    /// Marks the start of a logical transaction. Must be paired with either a
    /// matching `TxCommit` (commit) or `TxRollback` (discard).
    TxBegin { tx_id: u64 },
    /// Marks that all entries since the matching `TxBegin { tx_id }` should be
    /// discarded during replay.
    TxRollback { tx_id: u64 },
    /// Phase 6.X.C (ADR 0023) — voluntary lease-handoff marker. The outgoing
    /// leaseholder writes this as the last record before releasing the lease;
    /// it records `to_holder` (the candidate replica id) and `at_epoch` (the
    /// fence under which the handoff was performed). Treated as an
    /// informational no-op by the replay path — handoff doesn't change any
    /// row state, it's purely an observability + audit hook. New owners replay
    /// data entries normally and see the marker as a recovery breadcrumb.
    Handoff { to_holder: String, at_epoch: i64 },
    /// ADR 0020 §6 — explicit commit marker. Paired with the matching
    /// `TxBegin { tx_id }` to signal that the transaction committed durably.
    /// Replay honours entries between `TxBegin(tx_id)` and `TxCommit(tx_id)`.
    /// Without this marker a transaction whose `TxBegin` is present at EOF
    /// is treated as crashed-mid-tx and its entries are discarded (new
    /// behaviour) or committed under the legacy implicit-commit rule (old
    /// segments written before this variant existed).
    TxCommit { tx_id: u64 },
}

pub(crate) const FORMAT_VERSION: u16 = 1;

/// Length-prefix one record's bincode bytes onto `buf`.
pub(crate) fn frame_into(buf: &mut Vec<u8>, record: &SegmentRecord) -> Result<()> {
    let bytes =
        bincode::serialize(record).map_err(|e| BasinError::wal(format!("segment encode: {e}")))?;
    let len: u32 = bytes
        .len()
        .try_into()
        .map_err(|_| BasinError::wal("segment record exceeds 4 GiB"))?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&bytes);
    Ok(())
}

/// A decoded record from a segment, preserving transaction markers for the
/// replay path.
#[derive(Debug, Clone)]
pub(crate) enum DecodedRecord {
    Entry(EntryRecord),
    TxBegin { tx_id: u64 },
    TxRollback { tx_id: u64 },
    /// Phase 6.X.C — handoff marker. See [`SegmentRecord::Handoff`].
    Handoff { to_holder: String, at_epoch: i64 },
    /// ADR 0020 §6 — explicit commit marker. See [`SegmentRecord::TxCommit`].
    TxCommit { tx_id: u64 },
}

/// Parse a length-prefixed segment buffer into header + decoded records.
///
/// Unlike [`decode_segment`], this variant preserves transaction marker records
/// (`TxBegin`, `TxRollback`) so the replay path can filter rolled-back entries.
/// Use this path whenever transaction-aware processing is required.
pub(crate) fn decode_segment_full(
    buf: &[u8],
) -> Result<(SegmentHeader, Vec<DecodedRecord>)> {
    let mut cursor = 0usize;
    let mut header: Option<SegmentHeader> = None;
    let mut records: Vec<DecodedRecord> = Vec::new();

    while cursor < buf.len() {
        if cursor + 4 > buf.len() {
            return Err(BasinError::wal("segment: truncated length prefix"));
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&buf[cursor..cursor + 4]);
        let len = u32::from_le_bytes(len_bytes) as usize;
        cursor += 4;
        if cursor + len > buf.len() {
            return Err(BasinError::wal("segment: truncated record body"));
        }
        let record: SegmentRecord = bincode::deserialize(&buf[cursor..cursor + len])
            .map_err(|e| BasinError::wal(format!("segment decode: {e}")))?;
        cursor += len;
        match record {
            SegmentRecord::Header(h) => {
                if header.is_some() {
                    return Err(BasinError::wal("segment: multiple headers"));
                }
                header = Some(h);
            }
            SegmentRecord::Entry(e) => records.push(DecodedRecord::Entry(e)),
            SegmentRecord::TxBegin { tx_id } => {
                records.push(DecodedRecord::TxBegin { tx_id });
            }
            SegmentRecord::TxRollback { tx_id } => {
                records.push(DecodedRecord::TxRollback { tx_id });
            }
            SegmentRecord::Handoff {
                to_holder,
                at_epoch,
            } => {
                records.push(DecodedRecord::Handoff {
                    to_holder,
                    at_epoch,
                });
            }
            SegmentRecord::TxCommit { tx_id } => {
                records.push(DecodedRecord::TxCommit { tx_id });
            }
        }
    }

    let header = header.ok_or_else(|| BasinError::wal("segment: missing header"))?;
    Ok((header, records))
}

/// Parse a length-prefixed segment buffer into header + entries. Used by
/// recovery and by `read_from`.
///
/// Transaction marker records (`TxBegin`, `TxRollback`) are silently dropped
/// by this path for back-compat. For transaction-aware replay use
/// [`decode_segment_full`].
pub(crate) fn decode_segment(buf: &[u8]) -> Result<(SegmentHeader, Vec<EntryRecord>)> {
    let (header, records) = decode_segment_full(buf)?;
    let entries = records
        .into_iter()
        .filter_map(|r| match r {
            DecodedRecord::Entry(e) => Some(e),
            DecodedRecord::TxBegin { .. }
            | DecodedRecord::TxRollback { .. }
            | DecodedRecord::TxCommit { .. }
            | DecodedRecord::Handoff { .. } => None,
        })
        .collect();
    Ok((header, entries))
}

/// Construct a `TxBegin` segment record for the given transaction id.
pub(crate) fn tx_begin_record(tx_id: u64) -> SegmentRecord {
    SegmentRecord::TxBegin { tx_id }
}

/// Construct a `TxRollback` segment record for the given transaction id.
pub(crate) fn tx_rollback_record(tx_id: u64) -> SegmentRecord {
    SegmentRecord::TxRollback { tx_id }
}

/// Construct a `TxCommit` segment record for the given transaction id.
/// ADR 0020 §6 — emitted by the executor at every explicit COMMIT.
pub(crate) fn tx_commit_record(tx_id: u64) -> SegmentRecord {
    SegmentRecord::TxCommit { tx_id }
}

/// Construct a `Handoff` segment record (Phase 6.X.C).
pub(crate) fn handoff_record(to_holder: String, at_epoch: i64) -> SegmentRecord {
    SegmentRecord::Handoff {
        to_holder,
        at_epoch,
    }
}

/// Convenience for constructing the wire entry from the public type's pieces.
pub(crate) fn entry_record(
    project: &ProjectId,
    partition: &PartitionKey,
    lsn: Lsn,
    payload: &Bytes,
    appended_at: DateTime<Utc>,
) -> EntryRecord {
    EntryRecord {
        project: *project,
        partition: partition.clone(),
        lsn,
        payload: payload.to_vec(),
        appended_at,
    }
}
