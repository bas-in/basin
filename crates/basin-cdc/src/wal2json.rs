//! `wal2json` output format for the CDC stream (ADR 0028 Phase 4).
//!
//! # What this is
//!
//! A serialiser that turns a durable-ring [`CdcRecord`] into the JSON shape the
//! Postgres [`wal2json`](https://github.com/eulalie/wal2json) logical-decoding
//! output plugin emits. Tools that already consume Postgres logical replication
//! through `wal2json` (Debezium's `wal2json` connector, Airbyte, bespoke
//! consumers) can therefore parse Basin's CDC stream without a Basin-specific
//! codec.
//!
//! # Which `wal2json` shape
//!
//! We pin the documented **format-version 1** per-change object — the `change`
//! array of `{kind, schema, table, columnnames, columntypes, columnvalues}`
//! objects, with `oldkeys: {keynames, keytypes, keyvalues}` attached to UPDATE
//! and DELETE. This is the shape `wal2json`'s README documents and the one
//! Debezium's decoder pins against, so the byte tests below are hand-pinned to
//! the README example rather than to whatever a given `wal2json` build emits.
//!
//! Transaction boundaries: `wal2json` v1 wraps a whole transaction's changes in
//! a single `{"change": [...]}` object (one object per `XLogData` callback);
//! it does NOT emit standalone `{"action":"B"}` / `{"action":"C"}` markers —
//! those belong to format-version 2 (`include-transaction`/streaming mode),
//! which we model separately via [`Wal2JsonTxn::begin`] / [`Wal2JsonTxn::commit`]
//! for the v2 streaming shape. The default ([`encode_change`] +
//! [`encode_transaction`]) is v1: a commit's rows are one `change` array.
//!
//! # Type fidelity (honest)
//!
//! The captured [`CdcRecord`] carries `before` / `after` as `serde_json::Value`
//! — Basin's canonical JSON projection of the row (the same projection the SSE
//! and Kafka sinks emit). `wal2json` normally reports Postgres column **types**
//! (`columntypes: ["integer", "text", ...]`). Basin's `ChangeEvent` does not
//! carry per-column SQL type metadata at the post-commit hook, so we derive a
//! best-effort wal2json type token from the JSON value's runtime kind
//! ([`json_type_token`]) — `"numeric"` for numbers, `"boolean"` for bools,
//! `"text"` for strings, `"json"` for nested objects/arrays, `"text"` for null.
//! This is a documented deviation: a consumer that relies on exact Postgres
//! OID/typmod fidelity must use the Phase 5 `pgoutput` path, which carries the
//! relation's column types. Column **names** and **values** are exact.

use serde::Serialize;
use serde_json::Value;

use crate::record::CdcRecord;

/// The wal2json `kind` token for an op code (1=insert, 2=update, 3=delete).
fn kind_token(op: u8) -> &'static str {
    match op {
        1 => "insert",
        2 => "update",
        3 => "delete",
        _ => "message",
    }
}

/// Best-effort wal2json column-type token from a JSON value's runtime kind.
/// See the module doc: Basin does not carry per-column SQL types at this seam.
pub fn json_type_token(v: &Value) -> &'static str {
    match v {
        Value::Null => "text",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "numeric",
        Value::String(_) => "text",
        Value::Array(_) | Value::Object(_) => "json",
    }
}

/// One row image decomposed into the parallel `columnnames` / `columntypes` /
/// `columnvalues` arrays wal2json uses. Column order follows the JSON object's
/// key order (serde_json preserves insertion order with the `preserve_order`
/// feature; otherwise BTree key order — deterministic either way).
#[derive(Serialize, Default)]
struct ColumnArrays {
    columnnames: Vec<String>,
    columntypes: Vec<String>,
    columnvalues: Vec<Value>,
}

impl ColumnArrays {
    fn from_image(image: &Value) -> Self {
        let mut out = ColumnArrays::default();
        if let Value::Object(map) = image {
            for (k, v) in map {
                out.columnnames.push(k.clone());
                out.columntypes.push(json_type_token(v).to_string());
                out.columnvalues.push(v.clone());
            }
        }
        out
    }
}

/// The `oldkeys` block wal2json attaches to UPDATE and DELETE — the row's
/// identity columns (under default `REPLICA IDENTITY` Postgres emits only the
/// primary key here; Basin's `before` image is the whole prior row, which is a
/// superset, so we emit every column of `before` as a key — a documented
/// superset of Postgres' default-replica-identity behaviour).
#[derive(Serialize)]
struct OldKeys {
    keynames: Vec<String>,
    keytypes: Vec<String>,
    keyvalues: Vec<Value>,
}

impl OldKeys {
    fn from_before(before: &Value) -> Self {
        let cols = ColumnArrays::from_image(before);
        OldKeys {
            keynames: cols.columnnames,
            keytypes: cols.columntypes,
            keyvalues: cols.columnvalues,
        }
    }
}

/// One wal2json `change` element. Serialises to the README's per-change object.
/// `columnnames`/`columntypes`/`columnvalues` are omitted for a DELETE (which
/// carries no `after` image); `oldkeys` is present for UPDATE and DELETE.
#[derive(Serialize)]
pub struct Wal2JsonChange {
    pub kind: &'static str,
    pub schema: String,
    pub table: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columnnames: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columntypes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columnvalues: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldkeys: Option<OldKeysJson>,
}

/// Public mirror of [`OldKeys`] so [`Wal2JsonChange`] can carry it in its
/// public field while keeping the parallel-array construction private.
#[derive(Serialize)]
pub struct OldKeysJson {
    pub keynames: Vec<String>,
    pub keytypes: Vec<String>,
    pub keyvalues: Vec<Value>,
}

impl From<OldKeys> for OldKeysJson {
    fn from(k: OldKeys) -> Self {
        OldKeysJson {
            keynames: k.keynames,
            keytypes: k.keytypes,
            keyvalues: k.keyvalues,
        }
    }
}

/// The schema name reported in every change. Basin has no Postgres schema
/// namespace at the CDC seam; we report the conventional `"public"` so a
/// Postgres-shaped consumer's `schema.table` qualification resolves.
pub const WAL2JSON_SCHEMA: &str = "public";

/// Build one wal2json `change` element from a durable [`CdcRecord`].
///
/// - INSERT: `kind:"insert"`, full `columnnames/types/values` from `after`,
///   no `oldkeys`.
/// - UPDATE: `kind:"update"`, new image from `after`, `oldkeys` from `before`.
/// - DELETE: `kind:"delete"`, no column arrays, `oldkeys` from `before`.
pub fn encode_change(rec: &CdcRecord) -> Wal2JsonChange {
    let kind = kind_token(rec.op);
    let after_cols = rec
        .after
        .as_ref()
        .map(ColumnArrays::from_image)
        .unwrap_or_default();
    let oldkeys = rec
        .before
        .as_ref()
        .map(|b| OldKeysJson::from(OldKeys::from_before(b)));

    // DELETE carries no new image; INSERT/UPDATE carry the `after` columns.
    let (columnnames, columntypes, columnvalues) = if rec.op == 3 {
        (Vec::new(), Vec::new(), Vec::new())
    } else {
        (after_cols.columnnames, after_cols.columntypes, after_cols.columnvalues)
    };

    Wal2JsonChange {
        kind,
        schema: WAL2JSON_SCHEMA.to_string(),
        table: rec.table.clone(),
        columnnames,
        columntypes,
        columnvalues,
        // INSERT has no oldkeys; UPDATE/DELETE do (when a `before` image exists).
        oldkeys: if rec.op == 1 { None } else { oldkeys },
    }
}

/// The wal2json format-version-1 transaction envelope: a single object with a
/// `change` array. One `XLogData` callback per transaction in real wal2json;
/// here, one object per commit-grouped batch of records.
#[derive(Serialize)]
pub struct Wal2JsonTransaction {
    pub change: Vec<Wal2JsonChange>,
}

/// Encode a commit's records (already in `seq` order) as one wal2json v1
/// transaction object: `{"change":[ ... ]}`.
pub fn encode_transaction(records: &[CdcRecord]) -> Wal2JsonTransaction {
    Wal2JsonTransaction {
        change: records.iter().map(encode_change).collect(),
    }
}

/// Serialise one commit's records to the wal2json v1 transaction JSON bytes.
pub fn encode_transaction_bytes(records: &[CdcRecord]) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&encode_transaction(records))
}

// ---------------------------------------------------------------------------
// Format-version 2 streaming markers (standalone B/C action objects).
//
// wal2json v2 (`format-version=2`) emits a separate object per action, with
// transaction boundaries as `{"action":"B"}` / `{"action":"C"}`. We expose
// this shape for streaming consumers that prefer per-row framing over the v1
// per-transaction envelope. The `action` token is `B`/`I`/`U`/`D`/`C`.
// ---------------------------------------------------------------------------

/// Map an op code to the v2 single-letter `action` token.
fn action_token(op: u8) -> &'static str {
    match op {
        1 => "I",
        2 => "U",
        3 => "D",
        _ => "M",
    }
}

/// A wal2json v2 per-action object. `B`/`C` carry only the action (+ optional
/// LSN); `I`/`U`/`D` carry the same column/oldkeys payload as v1.
#[derive(Serialize)]
pub struct Wal2JsonV2 {
    pub action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<Wal2JsonV2Column>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<Vec<Wal2JsonV2Column>>,
}

/// A v2 `columns` / `identity` element: `{name, type, value}`.
#[derive(Serialize)]
pub struct Wal2JsonV2Column {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    pub value: Value,
}

fn v2_columns(image: &Value) -> Vec<Wal2JsonV2Column> {
    match image {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| Wal2JsonV2Column {
                name: k.clone(),
                col_type: json_type_token(v).to_string(),
                value: v.clone(),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Streaming-mode (v2) helpers: BEGIN / row / COMMIT objects.
pub struct Wal2JsonTxn;

impl Wal2JsonTxn {
    /// `{"action":"B"}` — transaction begin marker (v2).
    pub fn begin() -> Wal2JsonV2 {
        Wal2JsonV2 {
            action: "B",
            schema: None,
            table: None,
            columns: Vec::new(),
            identity: None,
        }
    }

    /// `{"action":"C"}` — transaction commit marker (v2).
    pub fn commit() -> Wal2JsonV2 {
        Wal2JsonV2 {
            action: "C",
            schema: None,
            table: None,
            columns: Vec::new(),
            identity: None,
        }
    }

    /// A v2 per-row object (`I`/`U`/`D`). `U`/`D` carry the `identity` block
    /// (the prior-image key columns); `I` does not.
    pub fn row(rec: &CdcRecord) -> Wal2JsonV2 {
        let columns = if rec.op == 3 {
            Vec::new()
        } else {
            rec.after.as_ref().map(v2_columns).unwrap_or_default()
        };
        let identity = if rec.op == 1 {
            None
        } else {
            rec.before.as_ref().map(v2_columns)
        };
        Wal2JsonV2 {
            action: action_token(rec.op),
            schema: Some(WAL2JSON_SCHEMA.to_string()),
            table: Some(rec.table.clone()),
            columns,
            identity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::ProjectId;
    use serde_json::json;

    fn rec(op: u8, seq: u64, before: Option<Value>, after: Option<Value>) -> CdcRecord {
        CdcRecord {
            seq,
            wal_lsn: 0,
            project: ProjectId::new(),
            table: "orders".to_string(),
            op,
            tx_id: None,
            committed_at: 0,
            causation_user: None,
            before,
            after,
        }
    }

    #[test]
    fn insert_change_shape_matches_wal2json_readme() {
        // wal2json README INSERT example shape: kind/schema/table + parallel
        // columnnames/columntypes/columnvalues, no oldkeys.
        let r = rec(1, 1, None, Some(json!({"id": 1, "name": "alice"})));
        let change = encode_change(&r);
        let v = serde_json::to_value(&change).unwrap();
        assert_eq!(v["kind"], "insert");
        assert_eq!(v["schema"], "public");
        assert_eq!(v["table"], "orders");
        assert_eq!(v["columnnames"], json!(["id", "name"]));
        assert_eq!(v["columntypes"], json!(["numeric", "text"]));
        assert_eq!(v["columnvalues"], json!([1, "alice"]));
        assert!(v.get("oldkeys").is_none(), "insert carries no oldkeys");
    }

    #[test]
    fn update_change_carries_new_image_and_oldkeys() {
        let r = rec(
            2,
            2,
            Some(json!({"id": 1, "name": "alice"})),
            Some(json!({"id": 1, "name": "bob"})),
        );
        let v = serde_json::to_value(encode_change(&r)).unwrap();
        assert_eq!(v["kind"], "update");
        // New image in column arrays.
        assert_eq!(v["columnvalues"], json!([1, "bob"]));
        // Prior image in oldkeys.
        assert_eq!(v["oldkeys"]["keynames"], json!(["id", "name"]));
        assert_eq!(v["oldkeys"]["keyvalues"], json!([1, "alice"]));
    }

    #[test]
    fn delete_change_has_oldkeys_only() {
        let r = rec(3, 3, Some(json!({"id": 1, "name": "alice"})), None);
        let v = serde_json::to_value(encode_change(&r)).unwrap();
        assert_eq!(v["kind"], "delete");
        // No new image for a delete.
        assert!(v.get("columnnames").is_none(), "delete has no column arrays");
        assert!(v.get("columnvalues").is_none());
        // Identity in oldkeys.
        assert_eq!(v["oldkeys"]["keyvalues"], json!([1, "alice"]));
    }

    #[test]
    fn transaction_envelope_wraps_change_array() {
        // wal2json v1: one transaction object with a `change` array, in order.
        let recs = vec![
            rec(1, 1, None, Some(json!({"id": 1}))),
            rec(2, 2, Some(json!({"id": 1})), Some(json!({"id": 2}))),
            rec(3, 3, Some(json!({"id": 2})), None),
        ];
        let v = serde_json::to_value(encode_transaction(&recs)).unwrap();
        let arr = v["change"].as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["kind"], "insert");
        assert_eq!(arr[1]["kind"], "update");
        assert_eq!(arr[2]["kind"], "delete");
    }

    #[test]
    fn transaction_bytes_are_a_single_json_object() {
        let recs = vec![rec(1, 1, None, Some(json!({"id": 1})))];
        let bytes = encode_transaction_bytes(&recs).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert!(s.starts_with("{\"change\":["), "v1 envelope shape: {s}");
        let _: Value = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn v2_begin_commit_markers() {
        let b = serde_json::to_value(Wal2JsonTxn::begin()).unwrap();
        assert_eq!(b, json!({"action": "B"}));
        let c = serde_json::to_value(Wal2JsonTxn::commit()).unwrap();
        assert_eq!(c, json!({"action": "C"}));
    }

    #[test]
    fn v2_row_actions_and_identity() {
        // INSERT → action I, columns, no identity.
        let ins = serde_json::to_value(Wal2JsonTxn::row(&rec(
            1,
            1,
            None,
            Some(json!({"id": 7})),
        )))
        .unwrap();
        assert_eq!(ins["action"], "I");
        assert_eq!(ins["columns"], json!([{"name":"id","type":"numeric","value":7}]));
        assert!(ins.get("identity").is_none());

        // DELETE → action D, no columns, identity from before.
        let del = serde_json::to_value(Wal2JsonTxn::row(&rec(
            3,
            2,
            Some(json!({"id": 7})),
            None,
        )))
        .unwrap();
        assert_eq!(del["action"], "D");
        assert!(del.get("columns").is_none(), "delete v2 carries no columns");
        assert_eq!(
            del["identity"],
            json!([{"name":"id","type":"numeric","value":7}])
        );
    }

    #[test]
    fn type_tokens_cover_json_kinds() {
        assert_eq!(json_type_token(&json!(1)), "numeric");
        assert_eq!(json_type_token(&json!(1.5)), "numeric");
        assert_eq!(json_type_token(&json!(true)), "boolean");
        assert_eq!(json_type_token(&json!("x")), "text");
        assert_eq!(json_type_token(&json!(null)), "text");
        assert_eq!(json_type_token(&json!({"a":1})), "json");
        assert_eq!(json_type_token(&json!([1, 2])), "json");
    }
}
