//! `pgoutput` binary logical-replication message encoder + slot/cursor model
//! (ADR 0028 Phase 5).
//!
//! # What this is
//!
//! `pgoutput` is Postgres' **native binary** logical-replication output format
//! (the one `pg_logical`, AWS DMS binary mode, and Debezium's default decoder
//! consume). Unlike `wal2json`'s text JSON, `pgoutput` frames each change as a
//! tagged binary message carrying relation OIDs, a column descriptor, and
//! length-prefixed tuple values. This module implements the **message encoder**
//! for the message types a logical-replication consumer must receive:
//!
//! | Tag  | Message    | Purpose                                            |
//! |------|------------|----------------------------------------------------|
//! | `B`  | Begin      | start of a transaction (final LSN, commit ts, xid) |
//! | `R`  | Relation   | table schema descriptor (sent before first row)    |
//! | `I`  | Insert     | a new tuple                                        |
//! | `U`  | Update     | old + new tuple (old present per REPLICA IDENTITY) |
//! | `D`  | Delete     | the old (key) tuple                                |
//! | `C`  | Commit     | end of transaction (commit LSN, end LSN, ts)       |
//!
//! These messages are what ride inside the `CopyData` / `XLogData` frames of a
//! `START_REPLICATION ... LOGICAL` stream. This module produces the **payload
//! bytes** of each message (the `pgoutput` body); the pgwire transport framing
//! (`CopyData('d')` → `XLogData('w')` header → this payload) is the remaining
//! router wiring documented in [`crate::pgoutput`] §"Router integration" and in
//! `basin-router`'s flagged replication-command path.
//!
//! # The synthetic-LSN mapping (the load-bearing decision)
//!
//! Postgres logical replication is LSN-addressed: every Begin/Commit carries an
//! `XLogRecPtr` (a `u64`), and the consumer acks progress by LSN in a Standby
//! Status Update. Basin's public cursor is the per-project monotonic
//! [`CdcRecord::seq`] (a `u64`); Basin's WAL LSN (`CdcRecord::wal_lsn`) is the
//! ADR's internal bookmark but is **0 at the post-commit hook today** (the
//! `ChangeEvent` does not carry an LSN at that seam — see `record.rs`).
//!
//! So we define a **synthetic LSN** that is a pure, monotonic function of
//! `seq`: [`seq_to_lsn`](crate::pgoutput::seq_to_lsn). We map `seq` into the high bits of the 64-bit LSN so
//! a per-row LSN never collides and is trivially monotone in `seq`:
//!
//! ```text
//! lsn = (seq << LSN_SEQ_SHIFT) | record_kind_low_bits
//! ```
//!
//! With `LSN_SEQ_SHIFT = 8` we reserve the low 8 bits to give a Begin, its
//! rows, and its Commit **distinct, ordered** LSNs within one logical commit
//! (Begin < rows < Commit), which Debezium's monotonicity check requires. The
//! ADR's "TxBegin-LSN caveat" (hot-tier fast paths emit only a `TxBegin` WAL
//! marker, so the real begin-LSN is unreliable) is sidestepped entirely: we do
//! **not** use the WAL LSN at all — the synthetic LSN is derived from the
//! commit-marker `seq`, which is reliable for every path (fast-path and
//! WAL-logged alike, because both flow through the post-commit hook that
//! assigns `seq`). The synthetic mapping is documented to consumers as
//! "Basin LSNs are synthetic, monotone in commit order, and not comparable to a
//! real Postgres cluster's LSNs."
//!
//! Reverse: [`lsn_to_seq`](crate::pgoutput::lsn_to_seq) recovers the `seq` (the high bits) from an LSN a
//! consumer acked in its Standby Status Update, so the ring cursor advances to
//! `replay_after(lsn_to_seq(acked_lsn))`.
//!
//! # Type fidelity
//!
//! `pgoutput` tuples carry per-column data as either text-format bytes (`'t'`)
//! or NULL (`'n'`) (we do not emit binary-format `'b'` columns — text is always
//! valid and is what `wal2json`-style consumers tolerate). The column **type
//! OID** in the Relation message is derived best-effort from the JSON value's
//! runtime kind ([`pg_type_oid`](crate::pgoutput::pg_type_oid)); a consumer needing exact source typmod must
//! reconcile against its own schema, the same documented limitation the ADR
//! notes for the JSON `before`/`after` projection.

use std::collections::HashMap;

use basin_common::ProjectId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::record::CdcRecord;

// ---------------------------------------------------------------------------
// Synthetic LSN mapping
// ---------------------------------------------------------------------------

/// Bits reserved at the bottom of a synthetic LSN to order the messages within
/// one logical commit (Begin / rows / Commit). 8 bits ⇒ up to 254 distinct
/// intra-commit message slots before the row offset saturates (rows beyond
/// that share the penultimate slot but keep Begin<rows<Commit ordering).
pub const LSN_SEQ_SHIFT: u32 = 8;

/// Intra-commit ordering slot for the Begin message (lowest).
const SLOT_BEGIN: u64 = 0;
/// Intra-commit ordering slot for the Commit message (highest in the byte).
const SLOT_COMMIT: u64 = 0xFF;
/// Row messages occupy slots `[1 ..= 0xFE]`, offset by their position.
const SLOT_ROW_BASE: u64 = 1;
const SLOT_ROW_MAX: u64 = 0xFE;

/// Map a commit `seq` to the synthetic LSN of that commit's **row** at intra-
/// commit position `row_ix` (0-based). Monotone in `(seq, row_ix)`.
pub fn seq_to_lsn(seq: u64) -> u64 {
    row_lsn(seq, 0)
}

/// Synthetic LSN of the Begin marker for commit `seq` (lowest in the commit).
pub fn begin_lsn(seq: u64) -> u64 {
    (seq << LSN_SEQ_SHIFT) | SLOT_BEGIN
}

/// Synthetic LSN of the Commit marker for commit `seq` (highest in the commit).
pub fn commit_lsn(seq: u64) -> u64 {
    (seq << LSN_SEQ_SHIFT) | SLOT_COMMIT
}

/// Synthetic LSN of the `row_ix`-th row of commit `seq`.
pub fn row_lsn(seq: u64, row_ix: u64) -> u64 {
    let slot = (SLOT_ROW_BASE + row_ix).min(SLOT_ROW_MAX);
    (seq << LSN_SEQ_SHIFT) | slot
}

/// Recover the commit `seq` from any synthetic LSN (the high bits above the
/// reserved intra-commit slot). The inverse of the `(seq << SHIFT)` mapping.
pub fn lsn_to_seq(lsn: u64) -> u64 {
    lsn >> LSN_SEQ_SHIFT
}

/// Format an LSN as Postgres' canonical `X/Y` upper/lower-32 hex text (the form
/// `IDENTIFY_SYSTEM` and `CREATE_REPLICATION_SLOT` report on the wire).
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Parse a Postgres `X/Y` LSN text back to a `u64` (e.g. from a consumer's
/// `START_REPLICATION ... 0/0` argument). Returns `None` on malformed input.
pub fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

// ---------------------------------------------------------------------------
// Postgres type OIDs (best-effort, from JSON runtime kind)
// ---------------------------------------------------------------------------

/// `text` OID.
pub const OID_TEXT: u32 = 25;
/// `int8` (bigint) OID — used for JSON integers.
pub const OID_INT8: u32 = 20;
/// `numeric` OID — used for JSON non-integer numbers.
pub const OID_NUMERIC: u32 = 1700;
/// `bool` OID.
pub const OID_BOOL: u32 = 16;
/// `jsonb` OID — used for nested JSON objects/arrays.
pub const OID_JSONB: u32 = 3802;

/// Best-effort Postgres type OID for a JSON value's runtime kind. See the
/// module doc: Basin carries no per-column SQL type at the CDC seam.
pub fn pg_type_oid(v: &Value) -> u32 {
    match v {
        Value::Null => OID_TEXT,
        Value::Bool(_) => OID_BOOL,
        Value::Number(n) if n.is_i64() || n.is_u64() => OID_INT8,
        Value::Number(_) => OID_NUMERIC,
        Value::String(_) => OID_TEXT,
        Value::Array(_) | Value::Object(_) => OID_JSONB,
    }
}

/// Render a JSON value as the text-format bytes `pgoutput` carries in a `'t'`
/// column. Strings are emitted verbatim (no JSON quoting); scalars use their
/// canonical text; objects/arrays use compact JSON.
fn text_value(v: &Value) -> Vec<u8> {
    match v {
        Value::String(s) => s.clone().into_bytes(),
        Value::Null => Vec::new(), // never called for null (NULL uses 'n')
        Value::Bool(b) => {
            if *b {
                b"t".to_vec()
            } else {
                b"f".to_vec()
            }
        }
        Value::Number(n) => n.to_string().into_bytes(),
        other => serde_json::to_vec(other).unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Relation OID assignment
// ---------------------------------------------------------------------------

/// Assigns a stable, per-stream relation OID to each `(table)` seen, and tracks
/// which relations have already had their `Relation` descriptor sent (the
/// protocol requires a `Relation` message before the first row referencing it,
/// and again whenever the descriptor changes). One per replication session.
#[derive(Default)]
pub struct RelationRegistry {
    next_oid: u32,
    by_table: HashMap<String, u32>,
    sent: HashMap<u32, ColumnShape>,
}

/// The column shape a `Relation` message described, so we only re-send a
/// descriptor when the shape changes (added/removed/renamed columns).
#[derive(Clone, PartialEq, Eq, Default)]
struct ColumnShape {
    names: Vec<String>,
}

impl RelationRegistry {
    pub fn new() -> Self {
        Self {
            // Start OIDs above the system-catalog range so they never look like
            // a real pg_class OID to a confused consumer.
            next_oid: 16_384,
            by_table: HashMap::new(),
            sent: HashMap::new(),
        }
    }

    /// The stable OID for `table`, minting one on first sight.
    pub fn oid_for(&mut self, table: &str) -> u32 {
        if let Some(oid) = self.by_table.get(table) {
            return *oid;
        }
        let oid = self.next_oid;
        self.next_oid += 1;
        self.by_table.insert(table.to_string(), oid);
        oid
    }

    /// Returns `Some(relation_bytes)` if a `Relation` descriptor must be sent
    /// before the next row of `table` with the column shape `cols` (first sight,
    /// or shape change); `None` if the last-sent descriptor still applies.
    pub fn relation_if_needed(&mut self, table: &str, cols: &[String]) -> Option<Vec<u8>> {
        let oid = self.oid_for(table);
        let shape = ColumnShape {
            names: cols.to_vec(),
        };
        if self.sent.get(&oid) == Some(&shape) {
            return None;
        }
        self.sent.insert(oid, shape);
        Some(encode_relation(oid, table, cols))
    }
}

// ---------------------------------------------------------------------------
// Message encoders — the pgoutput body bytes (big-endian, per the protocol).
// ---------------------------------------------------------------------------

fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}
fn put_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn put_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}
fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0); // C-string NUL terminator
}

/// Postgres epoch (2000-01-01 00:00:00 UTC) in unix micros. `pgoutput`
/// timestamps are micros since the Postgres epoch, not the unix epoch.
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

/// Convert unix micros (Basin's `committed_at`) to Postgres-epoch micros.
fn pg_timestamp(unix_micros: i64) -> i64 {
    unix_micros - PG_EPOCH_UNIX_MICROS
}

/// `B` — Begin. Body: `final_lsn(u64) commit_ts(i64) xid(u32)`.
/// `final_lsn` is the synthetic commit LSN; `xid` is the low 32 bits of `seq`
/// (a synthetic transaction id, monotone within u32 wrap — documented synthetic).
pub fn encode_begin(seq: u64, commit_unix_micros: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(21);
    put_u8(&mut buf, b'B');
    put_u64(&mut buf, commit_lsn(seq)); // final LSN of the committing txn
    put_i64(&mut buf, pg_timestamp(commit_unix_micros));
    put_u32(&mut buf, seq as u32); // synthetic xid
    buf
}

/// `C` — Commit. Body: `flags(u8) commit_lsn(u64) end_lsn(u64) commit_ts(i64)`.
pub fn encode_commit(seq: u64, commit_unix_micros: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(26);
    put_u8(&mut buf, b'C');
    put_u8(&mut buf, 0); // flags (unused; 0)
    put_u64(&mut buf, commit_lsn(seq));
    put_u64(&mut buf, commit_lsn(seq)); // end LSN == commit LSN for a synthetic txn
    put_i64(&mut buf, pg_timestamp(commit_unix_micros));
    buf
}

/// `R` — Relation. Body:
/// `oid(u32) namespace(str) relname(str) replica_identity(u8) ncols(i16)`
/// then per column: `flags(u8) name(str) type_oid(u32) type_modifier(i32)`.
/// All columns are flagged key (`1`) under our whole-`before`-image identity
/// (a documented superset of Postgres default replica identity).
pub fn encode_relation(oid: u32, table: &str, columns: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u8(&mut buf, b'R');
    put_u32(&mut buf, oid);
    put_str(&mut buf, "public"); // namespace
    put_str(&mut buf, table);
    put_u8(&mut buf, b'd'); // replica identity: 'd' = default
    buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for name in columns {
        put_u8(&mut buf, 1); // flag: part of the key
        put_str(&mut buf, name);
        // We do not carry per-column type at this seam in the Relation; report
        // text OID + no modifier. The per-row TupleData carries text values.
        put_u32(&mut buf, OID_TEXT);
        put_i32(&mut buf, -1); // type modifier: none
    }
    buf
}

/// Encode a `TupleData` block: `ncols(i16)` then per column either
/// `'n'` (NULL) or `'t' len(i32) bytes` (text value). Column order follows the
/// JSON object's key order. Returns `(bytes, column_names)` so the caller can
/// emit a matching `Relation` descriptor.
fn encode_tuple(image: &Value) -> (Vec<u8>, Vec<String>) {
    let mut names = Vec::new();
    let mut cols: Vec<(&String, &Value)> = Vec::new();
    if let Value::Object(map) = image {
        for (k, v) in map {
            names.push(k.clone());
            cols.push((k, v));
        }
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(&(cols.len() as i16).to_be_bytes());
    for (_, v) in cols {
        if v.is_null() {
            put_u8(&mut buf, b'n');
        } else {
            put_u8(&mut buf, b't');
            let bytes = text_value(v);
            put_i32(&mut buf, bytes.len() as i32);
            buf.extend_from_slice(&bytes);
        }
    }
    (buf, names)
}

/// The column names of a row image, in object key order (for the Relation
/// descriptor decision). Empty for a non-object / absent image.
pub fn image_columns(image: Option<&Value>) -> Vec<String> {
    match image {
        Some(Value::Object(map)) => map.keys().cloned().collect(),
        _ => Vec::new(),
    }
}

/// `I` — Insert. Body: `relation_oid(u32) 'N' TupleData(new)`.
pub fn encode_insert(oid: u32, after: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u8(&mut buf, b'I');
    put_u32(&mut buf, oid);
    put_u8(&mut buf, b'N'); // new tuple follows
    let (tuple, _) = encode_tuple(after);
    buf.extend_from_slice(&tuple);
    buf
}

/// `U` — Update. Body: `relation_oid(u32) ['O' TupleData(old)] 'N' TupleData(new)`.
/// The `'O'` (old tuple) block is present when a `before` image exists (our
/// whole-row identity always provides one for an UPDATE).
pub fn encode_update(oid: u32, before: Option<&Value>, after: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u8(&mut buf, b'U');
    put_u32(&mut buf, oid);
    if let Some(old) = before {
        put_u8(&mut buf, b'O'); // old tuple follows (full row, key+changed)
        let (tuple, _) = encode_tuple(old);
        buf.extend_from_slice(&tuple);
    }
    put_u8(&mut buf, b'N'); // new tuple follows
    let (tuple, _) = encode_tuple(after);
    buf.extend_from_slice(&tuple);
    buf
}

/// `D` — Delete. Body: `relation_oid(u32) 'O' TupleData(old)`. We always emit
/// the full old image under the `'O'` (whole-tuple) variant rather than `'K'`
/// (key-only), since Basin's `before` is the whole prior row.
pub fn encode_delete(oid: u32, before: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u8(&mut buf, b'D');
    put_u32(&mut buf, oid);
    put_u8(&mut buf, b'O'); // old tuple follows (full row)
    let (tuple, _) = encode_tuple(before);
    buf.extend_from_slice(&tuple);
    buf
}

/// Encode one durable record as the ordered sequence of pgoutput message bodies
/// needed before it can be sent: an optional `Relation` (when the table/shape is
/// new to this session) followed by the `Insert`/`Update`/`Delete`. The caller
/// frames each returned `Vec<u8>` in its own `CopyData`/`XLogData`.
pub fn encode_record(reg: &mut RelationRegistry, rec: &CdcRecord) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    // The column shape comes from the row's `after` image (insert/update) or
    // `before` image (delete).
    let shape_image = rec.after.as_ref().or(rec.before.as_ref());
    let cols = image_columns(shape_image);
    if let Some(relmsg) = reg.relation_if_needed(&rec.table, &cols) {
        out.push(relmsg);
    }
    let oid = reg.oid_for(&rec.table);
    let body = match rec.op {
        1 => rec.after.as_ref().map(|a| encode_insert(oid, a)),
        2 => rec
            .after
            .as_ref()
            .map(|a| encode_update(oid, rec.before.as_ref(), a)),
        3 => rec.before.as_ref().map(|b| encode_delete(oid, b)),
        _ => None,
    };
    if let Some(b) = body {
        out.push(b);
    }
    out
}

// ---------------------------------------------------------------------------
// Replication slot model (catalog-shaped; the additive slot row lives in the
// catalog — see basin_catalog::cdc_slots — this is the in-crate mirror the
// encoder + the router replication path share).
// ---------------------------------------------------------------------------

/// The output plugin a slot was created with. Basin supports `pgoutput` (Phase
/// 5) and `wal2json` (Phase 4) over the same replication transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputPlugin {
    Pgoutput,
    Wal2json,
}

impl OutputPlugin {
    /// Parse the plugin token a consumer names in `CREATE_REPLICATION_SLOT`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "pgoutput" => Some(OutputPlugin::Pgoutput),
            "wal2json" => Some(OutputPlugin::Wal2json),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            OutputPlugin::Pgoutput => "pgoutput",
            OutputPlugin::Wal2json => "wal2json",
        }
    }
}

/// A logical-replication slot: the durable cursor a consumer resumes from. The
/// slot's `confirmed_seq` is the ring `seq` the consumer has acked (via Standby
/// Status Update LSN → [`lsn_to_seq`]); the worker streams `replay_after(
/// confirmed_seq)`. A slot also holds back the CDC retention GC (the ADR §5
/// slot-hold: the GC must not prune below the minimum `confirmed_seq` of any
/// active slot) — that hold is enforced by the GC reading the slot table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationSlot {
    /// Slot name (the consumer-chosen identifier in CREATE_REPLICATION_SLOT).
    pub slot_name: String,
    pub project: ProjectId,
    pub plugin: OutputPlugin,
    /// Highest ring `seq` the consumer has confirmed (acked). `0` ⇒ never
    /// confirmed; the stream starts from the slot's `restart_seq`.
    pub confirmed_seq: u64,
    /// The `seq` the consumer must restart from on reconnect (the ring is
    /// replayed from here). Advances to `confirmed_seq` as acks arrive; held
    /// back from retention GC.
    pub restart_seq: u64,
    pub active: bool,
}

impl ReplicationSlot {
    /// A fresh slot at the zero cursor.
    pub fn new(slot_name: impl Into<String>, project: ProjectId, plugin: OutputPlugin) -> Self {
        Self {
            slot_name: slot_name.into(),
            project,
            plugin,
            confirmed_seq: 0,
            restart_seq: 0,
            active: false,
        }
    }

    /// The synthetic LSN this slot would report as its confirmed-flush point
    /// (for `IDENTIFY_SYSTEM` / slot-status responses).
    pub fn confirmed_lsn(&self) -> u64 {
        commit_lsn(self.confirmed_seq)
    }

    /// Advance the confirmed cursor from a consumer-acked synthetic LSN. The
    /// cursor is monotonic (never rewinds), mirroring the Kafka/webhook ack.
    pub fn confirm_lsn(&mut self, acked_lsn: u64) {
        let acked_seq = lsn_to_seq(acked_lsn);
        self.confirmed_seq = self.confirmed_seq.max(acked_seq);
        self.restart_seq = self.restart_seq.max(self.confirmed_seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(op: u8, seq: u64, before: Option<Value>, after: Option<Value>) -> CdcRecord {
        CdcRecord {
            seq,
            wal_lsn: 0,
            project: ProjectId::new(),
            table: "t".to_string(),
            op,
            tx_id: None,
            committed_at: 0,
            causation_user: None,
            before,
            after,
        }
    }

    // ---- LSN mapping -------------------------------------------------------

    #[test]
    fn lsn_is_monotone_in_seq_and_orders_within_a_commit() {
        // Begin < first row < last row < Commit, and a later commit's Begin is
        // above the earlier commit's Commit.
        assert!(begin_lsn(5) < row_lsn(5, 0));
        assert!(row_lsn(5, 0) < row_lsn(5, 3));
        assert!(row_lsn(5, 3) < commit_lsn(5));
        assert!(commit_lsn(5) < begin_lsn(6));
    }

    #[test]
    fn lsn_round_trips_to_seq() {
        for seq in [0u64, 1, 42, 1_000_000, u32::MAX as u64] {
            assert_eq!(lsn_to_seq(begin_lsn(seq)), seq);
            assert_eq!(lsn_to_seq(commit_lsn(seq)), seq);
            assert_eq!(lsn_to_seq(row_lsn(seq, 7)), seq);
            assert_eq!(lsn_to_seq(seq_to_lsn(seq)), seq);
        }
    }

    #[test]
    fn lsn_text_format_round_trips() {
        let lsn = commit_lsn(0x1234);
        let text = format_lsn(lsn);
        assert_eq!(parse_lsn(&text), Some(lsn));
        // Canonical X/Y shape.
        assert!(text.contains('/'));
        assert_eq!(parse_lsn("0/0"), Some(0));
        assert_eq!(
            parse_lsn("16/B374D848"),
            Some((0x16u64 << 32) | 0xB374_D848)
        );
        assert_eq!(parse_lsn("nonsense"), None);
    }

    #[test]
    fn confirm_lsn_advances_monotonically() {
        let mut slot = ReplicationSlot::new("s", ProjectId::new(), OutputPlugin::Pgoutput);
        slot.confirm_lsn(commit_lsn(10));
        assert_eq!(slot.confirmed_seq, 10);
        // A stale (lower) ack never rewinds.
        slot.confirm_lsn(commit_lsn(3));
        assert_eq!(slot.confirmed_seq, 10);
        slot.confirm_lsn(commit_lsn(25));
        assert_eq!(slot.confirmed_seq, 25);
        assert_eq!(slot.restart_seq, 25);
    }

    // ---- Message bytes (hand-pinned against the protocol layout) ----------

    #[test]
    fn begin_message_layout() {
        let b = encode_begin(7, PG_EPOCH_UNIX_MICROS); // commit ts == pg epoch ⇒ 0
        assert_eq!(b[0], b'B');
        // final LSN (u64 BE) == commit_lsn(7)
        let lsn = u64::from_be_bytes(b[1..9].try_into().unwrap());
        assert_eq!(lsn, commit_lsn(7));
        // commit ts (i64 BE) == 0 (we passed pg epoch)
        let ts = i64::from_be_bytes(b[9..17].try_into().unwrap());
        assert_eq!(ts, 0);
        // xid (u32 BE) == 7
        let xid = u32::from_be_bytes(b[17..21].try_into().unwrap());
        assert_eq!(xid, 7);
        assert_eq!(b.len(), 21);
    }

    #[test]
    fn commit_message_layout() {
        let c = encode_commit(7, PG_EPOCH_UNIX_MICROS);
        assert_eq!(c[0], b'C');
        assert_eq!(c[1], 0, "flags");
        let commit = u64::from_be_bytes(c[2..10].try_into().unwrap());
        let end = u64::from_be_bytes(c[10..18].try_into().unwrap());
        assert_eq!(commit, commit_lsn(7));
        assert_eq!(end, commit_lsn(7));
        let ts = i64::from_be_bytes(c[18..26].try_into().unwrap());
        assert_eq!(ts, 0);
        assert_eq!(c.len(), 26);
    }

    #[test]
    fn relation_message_layout() {
        let r = encode_relation(16_384, "orders", &["id".into(), "name".into()]);
        assert_eq!(r[0], b'R');
        let oid = u32::from_be_bytes(r[1..5].try_into().unwrap());
        assert_eq!(oid, 16_384);
        // namespace "public\0"
        assert_eq!(&r[5..12], b"public\0");
        // relname "orders\0"
        assert_eq!(&r[12..19], b"orders\0");
        // replica identity 'd'
        assert_eq!(r[19], b'd');
        // ncols (i16 BE) == 2
        let ncols = i16::from_be_bytes(r[20..22].try_into().unwrap());
        assert_eq!(ncols, 2);
        // first column: flag(1) "id\0" oid(text) typmod(-1)
        assert_eq!(r[22], 1);
        assert_eq!(&r[23..26], b"id\0");
        let coid = u32::from_be_bytes(r[26..30].try_into().unwrap());
        assert_eq!(coid, OID_TEXT);
        let typmod = i32::from_be_bytes(r[30..34].try_into().unwrap());
        assert_eq!(typmod, -1);
    }

    #[test]
    fn insert_message_layout() {
        let i = encode_insert(16_384, &json!({"id": 1}));
        assert_eq!(i[0], b'I');
        let oid = u32::from_be_bytes(i[1..5].try_into().unwrap());
        assert_eq!(oid, 16_384);
        assert_eq!(i[5], b'N', "new-tuple marker");
        // TupleData: ncols(i16)=1
        let ncols = i16::from_be_bytes(i[6..8].try_into().unwrap());
        assert_eq!(ncols, 1);
        // col: 't' len(i32)=1 "1"
        assert_eq!(i[8], b't');
        let len = i32::from_be_bytes(i[9..13].try_into().unwrap());
        assert_eq!(len, 1);
        assert_eq!(&i[13..14], b"1");
    }

    #[test]
    fn update_message_carries_old_and_new() {
        let u = encode_update(16_384, Some(&json!({"id": 1})), &json!({"id": 2}));
        assert_eq!(u[0], b'U');
        // After oid(4) → 'O' old tuple, then 'N' new tuple.
        assert_eq!(u[5], b'O', "old-tuple marker present for update");
        // old TupleData: ncols=1, 't', len=1, "1"
        assert_eq!(&u[6..8], &1i16.to_be_bytes());
        assert_eq!(u[8], b't');
        // Find the 'N' marker after the old tuple block.
        // Layout: I/U(1) oid(4) 'O'(1@idx5) then old TupleData starting @idx6:
        // ncols(2) + ['t'(1) len(4) val(1)] = 2+6 = 8 bytes ⇒ old block is
        // idx6..=13, so 'N' is at idx 14.
        let n_pos = 6 + 8;
        assert_eq!(u[n_pos], b'N', "new-tuple marker after old block");
    }

    #[test]
    fn delete_message_carries_old_image() {
        let d = encode_delete(16_384, &json!({"id": 9}));
        assert_eq!(d[0], b'D');
        let oid = u32::from_be_bytes(d[1..5].try_into().unwrap());
        assert_eq!(oid, 16_384);
        assert_eq!(d[5], b'O', "delete carries the full old tuple");
        let ncols = i16::from_be_bytes(d[6..8].try_into().unwrap());
        assert_eq!(ncols, 1);
    }

    #[test]
    fn null_column_uses_n_marker() {
        let i = encode_insert(16_384, &json!({"id": null}));
        // I, oid(4), 'N', ncols(2)=1, then 'n'
        assert_eq!(i[6..8], 1i16.to_be_bytes());
        assert_eq!(i[8], b'n', "null column uses 'n' marker, no length/value");
        assert_eq!(i.len(), 9);
    }

    // ---- Relation registry -------------------------------------------------

    #[test]
    fn relation_sent_once_per_shape() {
        let mut reg = RelationRegistry::new();
        // First insert → Relation + Insert.
        let msgs = encode_record(&mut reg, &rec(1, 1, None, Some(json!({"id": 1}))));
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0][0], b'R');
        assert_eq!(msgs[1][0], b'I');
        // Second insert, same shape → Insert only (no re-sent Relation).
        let msgs = encode_record(&mut reg, &rec(1, 2, None, Some(json!({"id": 2}))));
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0][0], b'I');
        // Shape change (new column) → Relation re-sent.
        let msgs = encode_record(&mut reg, &rec(1, 3, None, Some(json!({"id": 3, "v": 1}))));
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0][0], b'R');
    }

    #[test]
    fn delete_record_emits_relation_from_before_image() {
        let mut reg = RelationRegistry::new();
        let msgs = encode_record(&mut reg, &rec(3, 1, Some(json!({"id": 1})), None));
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0][0], b'R', "relation derived from before-image");
        assert_eq!(msgs[1][0], b'D');
    }

    #[test]
    fn oid_is_stable_per_table() {
        let mut reg = RelationRegistry::new();
        let a1 = reg.oid_for("a");
        let b1 = reg.oid_for("b");
        let a2 = reg.oid_for("a");
        assert_eq!(a1, a2);
        assert_ne!(a1, b1);
        assert!(a1 >= 16_384, "OIDs above system-catalog range");
    }

    #[test]
    fn output_plugin_round_trips() {
        assert_eq!(
            OutputPlugin::parse("pgoutput"),
            Some(OutputPlugin::Pgoutput)
        );
        assert_eq!(
            OutputPlugin::parse("WAL2JSON"),
            Some(OutputPlugin::Wal2json)
        );
        assert_eq!(OutputPlugin::parse("bogus"), None);
        assert_eq!(OutputPlugin::Pgoutput.as_str(), "pgoutput");
    }

    #[test]
    fn pg_type_oids_from_json_kind() {
        assert_eq!(pg_type_oid(&json!(1)), OID_INT8);
        assert_eq!(pg_type_oid(&json!(1.5)), OID_NUMERIC);
        assert_eq!(pg_type_oid(&json!(true)), OID_BOOL);
        assert_eq!(pg_type_oid(&json!("x")), OID_TEXT);
        assert_eq!(pg_type_oid(&json!({"a":1})), OID_JSONB);
    }
}
