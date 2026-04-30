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

use basin_common::{BasinError, PartitionKey, Result, TenantId};
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
    pub tenant: TenantId,
    pub partition: PartitionKey,
    pub first_lsn: Lsn,
    pub segment_id: Ulid,
}

/// Wire-format payload entry. Mirrors [`crate::WalEntry`] but owns the bytes
/// for ser/de.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EntryRecord {
    pub tenant: TenantId,
    pub partition: PartitionKey,
    pub lsn: Lsn,
    pub payload: Vec<u8>,
    pub appended_at: DateTime<Utc>,
}

/// Tagged wrapper so a segment can hold either a header or an entry. Bincode
/// handles the discriminant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum SegmentRecord {
    Header(SegmentHeader),
    Entry(EntryRecord),
}

pub(crate) const FORMAT_VERSION: u16 = 1;

/// Length-prefix one record's bincode bytes onto `buf`.
pub(crate) fn frame_into(buf: &mut Vec<u8>, record: &SegmentRecord) -> Result<()> {
    let bytes = bincode::serialize(record)
        .map_err(|e| BasinError::wal(format!("segment encode: {e}")))?;
    let len: u32 = bytes
        .len()
        .try_into()
        .map_err(|_| BasinError::wal("segment record exceeds 4 GiB"))?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&bytes);
    Ok(())
}

/// Parse a length-prefixed segment buffer into header + entries. Used by
/// recovery and by `read_from`.
pub(crate) fn decode_segment(buf: &[u8]) -> Result<(SegmentHeader, Vec<EntryRecord>)> {
    let mut cursor = 0usize;
    let mut header: Option<SegmentHeader> = None;
    let mut entries: Vec<EntryRecord> = Vec::new();

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
            SegmentRecord::Entry(e) => entries.push(e),
        }
    }

    let header = header.ok_or_else(|| BasinError::wal("segment: missing header"))?;
    Ok((header, entries))
}

/// Convenience for constructing the wire entry from the public type's pieces.
pub(crate) fn entry_record(
    tenant: &TenantId,
    partition: &PartitionKey,
    lsn: Lsn,
    payload: &Bytes,
    appended_at: DateTime<Utc>,
) -> EntryRecord {
    EntryRecord {
        tenant: *tenant,
        partition: partition.clone(),
        lsn,
        payload: payload.to_vec(),
        appended_at,
    }
}
