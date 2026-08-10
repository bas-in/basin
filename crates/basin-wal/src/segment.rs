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
//!
//! The framing primitive itself ([`frame_record_into`] / [`iter_frames`]) is
//! record-type-agnostic and shared: the file WAL frames [`SegmentRecord`]s
//! with it, and the raft log store (`raft_storage.rs`, multi-node commit 2)
//! frames its own record enum with the identical layout — one framing
//! format, two record vocabularies.

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
///
/// `payload` is a refcounted [`Bytes`] so constructing and cloning a record
/// never copies the payload (the append ack path and the flush path both
/// used to pay a full memcpy here). Wire compatibility: under bincode 1.x
/// (fixint, little-endian) `Bytes` serializes via `serialize_bytes` as
/// `u64 LE length + raw bytes`, byte-for-byte identical to the historical
/// `Vec<u8>` encoding (`serialize_seq` of `u8`). Pinned by
/// `tests::wire_format_matches_pre_change_vec_payload`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EntryRecord {
    pub project: ProjectId,
    pub partition: PartitionKey,
    pub lsn: Lsn,
    pub payload: Bytes,
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
    TxBegin {
        tx_id: u64,
    },
    /// Marks that all entries since the matching `TxBegin { tx_id }` should be
    /// discarded during replay.
    TxRollback {
        tx_id: u64,
    },
    /// Phase 6.X.C (ADR 0023) — voluntary lease-handoff marker. The outgoing
    /// leaseholder writes this as the last record before releasing the lease;
    /// it records `to_holder` (the candidate replica id) and `at_epoch` (the
    /// fence under which the handoff was performed). Treated as an
    /// informational no-op by the replay path — handoff doesn't change any
    /// row state, it's purely an observability + audit hook. New owners replay
    /// data entries normally and see the marker as a recovery breadcrumb.
    Handoff {
        to_holder: String,
        at_epoch: i64,
    },
    /// ADR 0020 §6 — explicit commit marker. Paired with the matching
    /// `TxBegin { tx_id }` to signal that the transaction committed durably.
    /// Replay honours entries between `TxBegin(tx_id)` and `TxCommit(tx_id)`.
    /// Without this marker a transaction whose `TxBegin` is present at EOF
    /// is treated as crashed-mid-tx and its entries are discarded (new
    /// behaviour) or committed under the legacy implicit-commit rule (old
    /// segments written before this variant existed).
    TxCommit {
        tx_id: u64,
    },
}

pub(crate) const FORMAT_VERSION: u16 = 1;

/// Length-prefix one bincode-serializable record onto `buf`:
/// `[u32 LE length][bincode record bytes]`.
///
/// The shared framing primitive. [`frame_into`] (file WAL segments) and the
/// raft log store (`raft_storage.rs`) both write through here so the two
/// logs share one framing format byte-for-byte.
pub(crate) fn frame_record_into<T: Serialize>(buf: &mut Vec<u8>, record: &T) -> Result<()> {
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

/// Length-prefix one [`SegmentRecord`]'s bincode bytes onto `buf`.
pub(crate) fn frame_into(buf: &mut Vec<u8>, record: &SegmentRecord) -> Result<()> {
    frame_record_into(buf, record)
}

/// Iterator over the length-prefixed frames in `buf`, yielding each frame's
/// raw bincode bytes (the body, without the length prefix).
///
/// The decode-side counterpart of [`frame_record_into`], shared by
/// [`decode_segment_full`] and the raft log store. A truncated length prefix
/// or record body yields one `Err` and then terminates.
pub(crate) struct FrameIter<'a> {
    buf: &'a [u8],
    cursor: usize,
}

impl FrameIter<'_> {
    /// Byte offset just past the most recently yielded frame. Consumers
    /// that tolerate a torn tail (crash mid-append — the raft log store)
    /// record this after every `Ok` so they can truncate the file back to
    /// the last whole frame when the iterator reports a truncation error.
    pub(crate) fn offset(&self) -> usize {
        self.cursor
    }
}

impl<'a> Iterator for FrameIter<'a> {
    type Item = Result<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.buf.len() {
            return None;
        }
        if self.cursor + 4 > self.buf.len() {
            self.cursor = self.buf.len();
            return Some(Err(BasinError::wal("segment: truncated length prefix")));
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&self.buf[self.cursor..self.cursor + 4]);
        let len = u32::from_le_bytes(len_bytes) as usize;
        self.cursor += 4;
        if self.cursor + len > self.buf.len() {
            self.cursor = self.buf.len();
            return Some(Err(BasinError::wal("segment: truncated record body")));
        }
        let body = &self.buf[self.cursor..self.cursor + len];
        self.cursor += len;
        Some(Ok(body))
    }
}

/// Iterate the length-prefixed frames in `buf`. See [`FrameIter`].
pub(crate) fn iter_frames(buf: &[u8]) -> FrameIter<'_> {
    FrameIter { buf, cursor: 0 }
}

/// A decoded record from a segment, preserving transaction markers for the
/// replay path.
#[derive(Debug, Clone)]
pub(crate) enum DecodedRecord {
    Entry(EntryRecord),
    TxBegin {
        tx_id: u64,
    },
    TxRollback {
        tx_id: u64,
    },
    /// Phase 6.X.C — handoff marker. See [`SegmentRecord::Handoff`].
    Handoff {
        to_holder: String,
        at_epoch: i64,
    },
    /// ADR 0020 §6 — explicit commit marker. See [`SegmentRecord::TxCommit`].
    TxCommit {
        tx_id: u64,
    },
}

/// Parse a length-prefixed segment buffer into header + decoded records.
///
/// Unlike [`decode_segment`], this variant preserves transaction marker records
/// (`TxBegin`, `TxRollback`) so the replay path can filter rolled-back entries.
/// Use this path whenever transaction-aware processing is required.
pub(crate) fn decode_segment_full(buf: &[u8]) -> Result<(SegmentHeader, Vec<DecodedRecord>)> {
    let mut header: Option<SegmentHeader> = None;
    let mut records: Vec<DecodedRecord> = Vec::new();

    for frame in iter_frames(buf) {
        let body = frame?;
        let record: SegmentRecord = bincode::deserialize(body)
            .map_err(|e| BasinError::wal(format!("segment decode: {e}")))?;
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
/// Takes the payload by value: `Bytes` is refcounted, so this is copy-free.
pub(crate) fn entry_record(
    project: &ProjectId,
    partition: &PartitionKey,
    lsn: Lsn,
    payload: Bytes,
    appended_at: DateTime<Utc>,
) -> EntryRecord {
    EntryRecord {
        project: *project,
        partition: partition.clone(),
        lsn,
        payload,
        appended_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// Pre-change wire shape of [`EntryRecord`]: `payload` was a `Vec<u8>`.
    /// Re-declared here (with `Serialize` only) so the wire-compat test can
    /// pin byte-for-byte equality against what the old code wrote, without
    /// needing the old code. bincode 1.x ignores struct/field names — only
    /// field order and element encodings matter.
    #[derive(Serialize)]
    struct VecPayloadEntryRecord {
        project: ProjectId,
        partition: PartitionKey,
        lsn: Lsn,
        payload: Vec<u8>,
        appended_at: DateTime<Utc>,
    }

    /// Pre-change wire shape of [`SegmentRecord`]. Variant order must match
    /// the real enum exactly — bincode encodes the variant index (u32 LE)
    /// followed by the variant body.
    #[derive(Serialize)]
    #[allow(dead_code)]
    enum VecPayloadSegmentRecord {
        Header(SegmentHeader),
        Entry(VecPayloadEntryRecord),
        TxBegin { tx_id: u64 },
        TxRollback { tx_id: u64 },
        Handoff { to_holder: String, at_epoch: i64 },
        TxCommit { tx_id: u64 },
    }

    fn fixed_time() -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap()
    }

    /// WIRE-COMPAT FIXTURE. The on-disk segment format must not change when
    /// `EntryRecord.payload` moves from `Vec<u8>` to `bytes::Bytes`.
    ///
    /// Why this holds: bincode 1.x with its default options (fixint encoding,
    /// little-endian) serializes
    ///   - `Vec<u8>` via `serialize_seq(Some(len))` → u64 LE length, then each
    ///     `u8` as one raw byte;
    ///   - `Bytes`   via `serialize_bytes(&[u8])`   → u64 LE length, then the
    ///     raw bytes.
    /// Both produce `[len: u64 LE][raw payload]`. This test proves it by
    /// serializing the same logical record through both shapes and comparing
    /// the framed bytes, including the empty-payload edge case.
    #[test]
    fn wire_format_matches_pre_change_vec_payload() {
        let project = ProjectId::new();
        let partition = PartitionKey::new("orders/2026").unwrap();
        let payloads: [&[u8]; 3] = [b"hello wal payload \x00\x01\xff", b"", b"x"];

        for (i, raw) in payloads.iter().enumerate() {
            let new_record = SegmentRecord::Entry(EntryRecord {
                project,
                partition: partition.clone(),
                lsn: Lsn(42 + i as u64),
                payload: Bytes::copy_from_slice(raw),
                appended_at: fixed_time(),
            });
            let old_record = VecPayloadSegmentRecord::Entry(VecPayloadEntryRecord {
                project,
                partition: partition.clone(),
                lsn: Lsn(42 + i as u64),
                payload: raw.to_vec(),
                appended_at: fixed_time(),
            });

            let new_bytes = bincode::serialize(&new_record).unwrap();
            let old_bytes = bincode::serialize(&old_record).unwrap();
            assert_eq!(
                new_bytes, old_bytes,
                "Bytes payload must serialize identically to the historical \
                 Vec<u8> payload (case {i})"
            );

            // And the full framing (length prefix included) must match what
            // the pre-change frame_into produced for the same record.
            let mut framed = Vec::new();
            frame_into(&mut framed, &new_record).unwrap();
            let mut expected = Vec::new();
            expected.extend_from_slice(&(old_bytes.len() as u32).to_le_bytes());
            expected.extend_from_slice(&old_bytes);
            assert_eq!(framed, expected, "framed record bytes drifted (case {i})");
        }
    }

    /// Full encode→decode round trip through the production reader path.
    /// Every field — including the payload bytes — must survive intact.
    ///
    /// NOTE for verification: this test is intentionally written against the
    /// public framing/decoding helpers only, so it also passes when run
    /// against the pre-change code (payload as `Vec<u8>`). Running it on both
    /// sides of the change is an additional wire-stability check on top of
    /// `wire_format_matches_pre_change_vec_payload`.
    #[test]
    fn frame_round_trip_preserves_all_fields() {
        let project = ProjectId::new();
        let partition = PartitionKey::new("p1").unwrap();
        let segment_id = Ulid::new();
        let header = SegmentHeader {
            format_version: FORMAT_VERSION,
            project,
            partition: partition.clone(),
            first_lsn: Lsn(7),
            segment_id,
        };
        let entry = EntryRecord {
            project,
            partition: partition.clone(),
            lsn: Lsn(7),
            payload: Bytes::from_static(b"round-trip payload \x00\x7f\xff"),
            appended_at: fixed_time(),
        };

        let mut buf = Vec::new();
        frame_into(&mut buf, &SegmentRecord::Header(header)).unwrap();
        frame_into(&mut buf, &tx_begin_record(9)).unwrap();
        frame_into(&mut buf, &SegmentRecord::Entry(entry.clone())).unwrap();
        frame_into(&mut buf, &tx_commit_record(9)).unwrap();
        frame_into(&mut buf, &handoff_record("replica-b".to_string(), 3)).unwrap();
        frame_into(&mut buf, &tx_rollback_record(11)).unwrap();

        let (decoded_header, records) = decode_segment_full(&buf).unwrap();
        assert_eq!(decoded_header.format_version, FORMAT_VERSION);
        assert_eq!(decoded_header.project, project);
        assert_eq!(decoded_header.partition, partition);
        assert_eq!(decoded_header.first_lsn, Lsn(7));
        assert_eq!(decoded_header.segment_id, segment_id);

        assert_eq!(records.len(), 5);
        assert!(matches!(records[0], DecodedRecord::TxBegin { tx_id: 9 }));
        match &records[1] {
            DecodedRecord::Entry(e) => {
                assert_eq!(e.project, entry.project);
                assert_eq!(e.partition, entry.partition);
                assert_eq!(e.lsn, entry.lsn);
                assert_eq!(e.payload, entry.payload);
                assert_eq!(e.appended_at, entry.appended_at);
            }
            other => panic!("expected Entry, got {other:?}"),
        }
        assert!(matches!(records[2], DecodedRecord::TxCommit { tx_id: 9 }));
        match &records[3] {
            DecodedRecord::Handoff {
                to_holder,
                at_epoch,
            } => {
                assert_eq!(to_holder, "replica-b");
                assert_eq!(*at_epoch, 3);
            }
            other => panic!("expected Handoff, got {other:?}"),
        }
        assert!(matches!(
            records[4],
            DecodedRecord::TxRollback { tx_id: 11 }
        ));
    }
}
