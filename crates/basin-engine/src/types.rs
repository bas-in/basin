//! sqlparser → Arrow type bridging for the PoC.

use crate::pg_ast::ObjectNamePartExt;
use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use basin_common::{BasinError, Result};
use sqlparser::ast::ArrayElemTypeDef;
use sqlparser::ast::DataType as SqlDataType;
use sqlparser::ast::ExactNumberInfo;
use sqlparser::ast::TimezoneInfo;

/// Field-metadata key used to mark Basin-specific logical types that don't have
/// a dedicated Arrow `DataType`. Today the marked types are `JSONB` (rides on
/// `LargeBinary`; the bytes are canonical-form serialised JSON) and `UUID`
/// (rides on `FixedSizeBinary(16)`; the bytes are RFC 4122 big-endian raw).
/// Adding more logical types means a new `BASIN_TYPE_*` constant + a bit of
/// plumbing in the encoder; the storage layer is type-agnostic and needs no
/// change.
pub const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
pub const BASIN_TYPE_JSONB: &str = "JSONB";
pub const BASIN_TYPE_UUID: &str = "UUID";
/// Logical type marker for `TSVECTOR` columns. The Arrow physical type is
/// `Utf8`; this marker tells downstream layers (pgwire encoder, info_schema)
/// to advertise the appropriate PG OID rather than the plain-text OID.
pub const BASIN_TYPE_TSVECTOR: &str = "TSVECTOR";
/// Logical type marker for `TSQUERY` columns. Same physical type (`Utf8`);
/// same purpose as `BASIN_TYPE_TSVECTOR`.
pub const BASIN_TYPE_TSQUERY: &str = "TSQUERY";

// ── Network types ─────────────────────────────────────────────────────────────
/// `INET` — IPv4/IPv6 host address with optional /prefix. Stored as `Utf8`.
pub const BASIN_TYPE_INET: &str = "INET";
/// `CIDR` — IPv4/IPv6 network address (mandatory netmask). Stored as `Utf8`.
pub const BASIN_TYPE_CIDR: &str = "CIDR";
/// `MACADDR` — 6-byte Ethernet MAC address. Stored as `Utf8`.
pub const BASIN_TYPE_MACADDR: &str = "MACADDR";
/// `MACADDR8` — 8-byte EUI-64 MAC address. Stored as `Utf8`.
pub const BASIN_TYPE_MACADDR8: &str = "MACADDR8";

// ── Bit-string types ──────────────────────────────────────────────────────────
/// `BIT(n)` — fixed-length bit string (sequence of '0'/'1'). Stored as
/// `Utf8`; the metadata value is `"BIT(n)"` so the exact declared length
/// can be recovered at SELECT time.
pub const BASIN_TYPE_BIT_PREFIX: &str = "BIT(";
/// `BIT VARYING(n)` / `VARBIT(n)` — variable-length bit string up to `n`
/// bits. Stored as `Utf8`; metadata value is `"VARBIT(n)"` (or `"VARBIT"`
/// when no length was specified).
pub const BASIN_TYPE_VARBIT_PREFIX: &str = "VARBIT";

// ── MONEY ─────────────────────────────────────────────────────────────────────
/// `MONEY` — currency amount, stored as `Decimal128(20, 2)`. PG uses an
/// 8-byte signed integer scaled by 100 internally; Decimal128 is the
/// closest lossless Arrow representation for the range and scale PG
/// guarantees (`-92233720368547758.08` to `+92233720368547758.07`).
pub const BASIN_TYPE_MONEY: &str = "MONEY";

// XML — UTF-8 text with marker for OID 142.
pub const BASIN_TYPE_XML: &str = "XML";

/// `POINT` — PostGIS-style 2D point. Rides on `FixedSizeBinary(21)`; the
/// bytes are little-endian WKB (1-byte endian + 4-byte type + 8-byte X +
/// 8-byte Y), matching what `basin_geo::encode_point` emits. The marker
/// lets pgwire surface the column to clients (today rendered as WKB hex
/// in text format; binary-format clients can read the raw 21 bytes).
pub const BASIN_TYPE_POINT: &str = "POINT";

/// `CITEXT` — case-insensitive text. Stored as plain `Utf8` in Arrow; the
/// marker tells comparison operators, UNIQUE enforcement, and ORDER BY to
/// apply case-folding (lower()) before comparing values. PG OID 25 (same as
/// TEXT) — citext is a PG extension, not a wire-level distinct type, so the
/// pgwire encoder emits OID 25 for citext columns.
pub const BASIN_TYPE_CITEXT: &str = "CITEXT";

// Range types — stored as a JSON string `{"l":<lower>,"u":<upper>,"li":<bool>,"ui":<bool>}`.
// Physical Arrow type is Utf8; the marker carries the PG range sub-type so the
// pgwire encoder knows which element OID to advertise.
pub const BASIN_TYPE_INT4RANGE: &str = "INT4RANGE";
pub const BASIN_TYPE_INT8RANGE: &str = "INT8RANGE";
pub const BASIN_TYPE_NUMRANGE: &str = "NUMRANGE";
pub const BASIN_TYPE_DATERANGE: &str = "DATERANGE";
pub const BASIN_TYPE_TSRANGE: &str = "TSRANGE";
pub const BASIN_TYPE_TSTZRANGE: &str = "TSTZRANGE";

/// Per-column markers for declarative lifecycle behaviours. Stored as
/// Arrow `Field` metadata so they round-trip through the catalog's
/// schema serde without a `TableMetadata` field per behaviour.
pub const BASIN_AUTO_UPDATE_KEY: &str = "BASIN_AUTO_UPDATE";
pub const BASIN_SOFT_DELETE_KEY: &str = "BASIN_SOFT_DELETE";

/// Schema-level marker for `AUDIT TO <table>`. Stored on the source
/// table's `Schema::metadata`; the value is the audit table's bare name.
pub const BASIN_AUDIT_TABLE_KEY: &str = "BASIN_AUDIT_TABLE";

/// Per-column marker for `GENERATED ALWAYS AS (<expr>) STORED`. Value is
/// the parenthesised expression source text (without the surrounding
/// parens). Engine reads it on every INSERT/UPDATE row to materialise
/// the persisted column value.
pub const BASIN_GENERATED_AS: &str = "BASIN_GENERATED_AS";

/// Per-column marker for IDENTITY columns (`GENERATED ALWAYS AS
/// IDENTITY` / `GENERATED BY DEFAULT AS IDENTITY`, plus the `SERIAL`
/// family which expands to `BY DEFAULT` semantics). Value is one of
/// [`BASIN_IDENTITY_ALWAYS`] or [`BASIN_IDENTITY_BY_DEFAULT`].
///
/// Identity columns ride on the same per-project sequence machinery as
/// `DEFAULT nextval('seq')` — the backing sequence name is stored in
/// [`BASIN_IDENTITY_SEQ`] and the INSERT path routes through
/// `Catalog::nextval` to fill omitted slots. The `ALWAYS` variant
/// rejects user-supplied values unless the INSERT statement carries
/// `OVERRIDING SYSTEM VALUE`; the `BY DEFAULT` variant accepts them.
pub const BASIN_IDENTITY: &str = "BASIN_IDENTITY";
pub const BASIN_IDENTITY_ALWAYS: &str = "ALWAYS";
pub const BASIN_IDENTITY_BY_DEFAULT: &str = "BY_DEFAULT";

/// Per-column marker holding the name of the backing sequence for an
/// IDENTITY / SERIAL column. Set alongside [`BASIN_IDENTITY`] at CREATE
/// TABLE time; read at INSERT time when the column is omitted (or when
/// `OVERRIDING USER VALUE` discards the user's literal).
pub const BASIN_IDENTITY_SEQ: &str = "BASIN_IDENTITY_SEQ";

/// Per-column marker for `DEFAULT <expr>`. Value is the source text
/// of the DEFAULT expression as the user wrote it (e.g.
/// `nextval('my_seq')`, `0`, `'pending'`). The INSERT path reads this
/// on every omitted column and substitutes the evaluated expression
/// before row coercion. `nextval(...)` calls inside the text are
/// routed through [`crate::seq_udf::rewrite_sequence_calls`] at
/// evaluation time so each insertion handed out a distinct value.
pub const BASIN_COLUMN_DEFAULT: &str = "BASIN_COLUMN_DEFAULT";

/// Per-column marker for `CREATE TYPE … AS ENUM` columns. Value is
/// the unqualified enum type name. The Arrow physical type is
/// `Utf8` (storing the label string); the engine validates the label
/// against the catalog row at INSERT time.
pub const BASIN_ENUM_TYPE_KEY: &str = basin_catalog::BASIN_ENUM_TYPE_KEY;

/// Per-column marker for `CREATE DOMAIN` columns. Value is the
/// unqualified domain name. The Arrow physical type is the domain's
/// underlying base type; the engine evaluates the domain's CHECK
/// predicate against each row at INSERT time.
pub const BASIN_DOMAIN_KEY: &str = basin_catalog::BASIN_DOMAIN_KEY;

/// Per-column marker for a declared character-length limit on
/// `VARCHAR(n)` / `CHARACTER VARYING(n)` / `CHAR(n)` / `CHARACTER(n)`.
/// The Arrow physical type stays `Utf8` (these are just length-checked
/// text). Value format:
///   - `"varchar(n)"` — variable-length, error if char-length > n
///     (trailing spaces beyond n are truncated rather than erroring,
///     matching PG's `varchar` cast rule).
///   - `"char(n)"`    — fixed-length, error if char-length > n, and
///     blank-padded with spaces up to n on store (PG `bpchar`).
/// Unbounded `VARCHAR` / `CHARACTER VARYING` / `TEXT` carry no marker
/// (no length check). `CHAR` with no length is `CHAR(1)` per the SQL
/// standard and PG.
pub const BASIN_CHARLEN_KEY: &str = "BASIN_CHARLEN";

/// The declared character-length flavour parsed back out of a
/// [`BASIN_CHARLEN_KEY`] marker value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CharLen {
    /// `VARCHAR(n)` — length-checked, no padding.
    Varchar(u32),
    /// `CHAR(n)` — length-checked, blank-padded to exactly `n`.
    Char(u32),
}

/// Compute the [`BASIN_CHARLEN_KEY`] marker value for a column's declared
/// type, or `None` when the type carries no enforceable character-length
/// limit (`TEXT`, unbounded `VARCHAR`, non-text types).
///
/// PG semantics encoded here:
///   - `VARCHAR(n)` / `CHARACTER VARYING(n)` → `varchar(n)`
///   - bare `VARCHAR` / `CHARACTER VARYING`  → no marker (unbounded)
///   - `CHAR(n)` / `CHARACTER(n)`            → `char(n)`
///   - bare `CHAR` / `CHARACTER`             → `char(1)`
///   - `TEXT` / `STRING`                     → no marker
pub(crate) fn charlen_marker(sql: &SqlDataType) -> Option<String> {
    fn n_of(info: &Option<sqlparser::ast::CharacterLength>) -> Option<u32> {
        match info {
            Some(sqlparser::ast::CharacterLength::IntegerLength { length, .. }) => {
                u32::try_from(*length).ok()
            }
            // `CHARACTER VARYING(MAX)` (T-SQL-ism) — treat as unbounded.
            Some(sqlparser::ast::CharacterLength::Max) => None,
            None => None,
        }
    }
    match sql {
        SqlDataType::Varchar(info) | SqlDataType::CharacterVarying(info) => {
            n_of(info).map(|n| format!("varchar({n})"))
        }
        SqlDataType::Char(info) | SqlDataType::Character(info) => {
            // PG: bare CHAR ≡ CHAR(1).
            let n = n_of(info).unwrap_or(1);
            Some(format!("char({n})"))
        }
        _ => None,
    }
}

/// Parse a [`BASIN_CHARLEN_KEY`] marker value into a [`CharLen`]. Returns
/// `None` for an absent / unparseable marker (treated as "no limit").
pub(crate) fn parse_charlen(field: &arrow_schema::Field) -> Option<CharLen> {
    let v = field.metadata().get(BASIN_CHARLEN_KEY)?;
    if let Some(rest) = v.strip_prefix("varchar(") {
        let n: u32 = rest.strip_suffix(')')?.parse().ok()?;
        return Some(CharLen::Varchar(n));
    }
    if let Some(rest) = v.strip_prefix("char(") {
        let n: u32 = rest.strip_suffix(')')?.parse().ok()?;
        return Some(CharLen::Char(n));
    }
    None
}

/// Enforce a declared `VARCHAR(n)` / `CHAR(n)` limit on one text value,
/// returning the value as it should be stored.
///
/// PG parity:
///   - `VARCHAR(n)`: if `char_length(s) > n` *after* stripping trailing
///     spaces down to n, raise SQLSTATE 22001. Otherwise stored as-is
///     (the trailing-space truncation only kicks in when it lets an
///     otherwise-too-long value fit, exactly like PG's `varchar` input).
///   - `CHAR(n)`: if `char_length(s) > n` raise 22001; else blank-pad
///     with spaces to exactly `n` characters.
///
/// Length is measured in Unicode scalar values (`chars().count()`),
/// matching PG's per-character semantics rather than byte length.
pub(crate) fn enforce_charlen<'a>(
    limit: CharLen,
    s: &'a str,
    col: &str,
) -> Result<std::borrow::Cow<'a, str>> {
    use std::borrow::Cow;
    match limit {
        CharLen::Varchar(n) => {
            let n = n as usize;
            let len = s.chars().count();
            if len <= n {
                return Ok(Cow::Borrowed(s));
            }
            // PG truncates over-length *trailing spaces* for varchar input
            // instead of erroring; only non-space overflow is an error.
            let trimmed: &str = s.trim_end_matches(' ');
            let trimmed_len = trimmed.chars().count();
            if trimmed_len <= n {
                // Keep exactly n chars (trailing spaces beyond n dropped).
                let kept: String = s.chars().take(n).collect();
                return Ok(Cow::Owned(kept));
            }
            let _ = col;
            Err(BasinError::StringTooLong(format!(
                "value too long for type character varying({n})"
            )))
        }
        CharLen::Char(n) => {
            let n = n as usize;
            let len = s.chars().count();
            if len > n {
                return Err(BasinError::StringTooLong(format!(
                    "value too long for type character({n})"
                )));
            }
            if len == n {
                Ok(Cow::Borrowed(s))
            } else {
                let mut out = String::with_capacity(s.len() + (n - len));
                out.push_str(s);
                out.extend(std::iter::repeat(' ').take(n - len));
                Ok(Cow::Owned(out))
            }
        }
    }
}

/// Returns `true` if `field` carries the `JSONB` metadata marker. Cheap — just
/// a hashmap lookup on a small map.
pub(crate) fn field_is_jsonb(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_JSONB)
}

/// Returns `true` if `field` carries the `UUID` metadata marker. The
/// underlying Arrow type is `FixedSizeBinary(16)`; the marker tells the
/// pgwire encoder to emit Postgres OID 2950 (UUID) and the canonical
/// hyphenated text form, and tells the REST layer to render the bytes as a
/// hyphenated string rather than hex.
pub(crate) fn field_is_uuid(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID)
}

/// Returns `true` if `field` carries the `INET` metadata marker.
pub(crate) fn field_is_inet(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_INET)
}

/// Returns `true` if `field` carries the `CIDR` metadata marker.
pub(crate) fn field_is_cidr(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_CIDR)
}

/// Returns `true` if `field` carries the `MACADDR` metadata marker.
pub(crate) fn field_is_macaddr(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_MACADDR)
}

/// Returns `true` if `field` carries the `MACADDR8` metadata marker.
pub(crate) fn field_is_macaddr8(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_MACADDR8)
}

/// Returns `true` if `field` carries the `CITEXT` metadata marker. The
/// underlying Arrow type is `Utf8`; the marker tells comparison operators,
/// UNIQUE enforcement, and ORDER BY to apply lower()-fold before comparing.
pub(crate) fn field_is_citext(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_CITEXT)
}

/// Returns `true` if `field` carries the `MONEY` metadata marker.
pub(crate) fn field_is_money(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_MONEY)
}

/// Returns `true` if `field` carries the `TSVECTOR` metadata marker. The
/// underlying Arrow type is `Utf8` holding the canonical tsvector text form
/// (see `fts_udf`); the marker lets the INSERT path evaluate a
/// `to_tsvector(...)` expression into that canonical form.
pub(crate) fn field_is_tsvector(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_TSVECTOR)
}

/// Returns `true` if `field` carries the `TSQUERY` metadata marker.
pub(crate) fn field_is_tsquery(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_TSQUERY)
}

/// Returns `true` if `field` carries the `POINT` metadata marker
/// (Arrow physical: `FixedSizeBinary(21)`, content: WKB).
pub(crate) fn field_is_point(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_POINT)
}

/// Returns `true` if `field` carries any of the six PG range-type markers
/// (`INT4RANGE`, `INT8RANGE`, `NUMRANGE`, `DATERANGE`, `TSRANGE`, `TSTZRANGE`).
/// Range values are stored as `Utf8` JSON strings; this marker distinguishes
/// them from plain text columns.
pub(crate) fn field_is_range(field: &arrow_schema::Field) -> bool {
    matches!(
        field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()),
        Some(BASIN_TYPE_INT4RANGE)
            | Some(BASIN_TYPE_INT8RANGE)
            | Some(BASIN_TYPE_NUMRANGE)
            | Some(BASIN_TYPE_DATERANGE)
            | Some(BASIN_TYPE_TSRANGE)
            | Some(BASIN_TYPE_TSTZRANGE)
    )
}

/// Returns `true` if `field` is a BIT(n) column (fixed-length bit string).
/// Checks that the `BASIN_TYPE` value starts with `"BIT("`.
pub(crate) fn field_is_bit(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get(BASIN_TYPE_KEY)
        .map(|s| s.starts_with(BASIN_TYPE_BIT_PREFIX))
        .unwrap_or(false)
}

/// Returns `true` if `field` is a VARBIT column (variable-length bit string).
/// Checks that the `BASIN_TYPE` value starts with `"VARBIT"`.
pub(crate) fn field_is_varbit(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get(BASIN_TYPE_KEY)
        .map(|s| s.starts_with(BASIN_TYPE_VARBIT_PREFIX))
        .unwrap_or(false)
}

/// Extract the declared maximum bit length from a VARBIT field's metadata.
/// Returns `None` when no length was given (unbounded). Panics if called on
/// a non-VARBIT field (caller must gate with `field_is_varbit`).
pub(crate) fn varbit_max_len(field: &arrow_schema::Field) -> Option<u64> {
    let v = field.metadata().get(BASIN_TYPE_KEY)?;
    // Formats: "VARBIT" (unbounded) or "VARBIT(n)".
    let inner = v.strip_prefix("VARBIT(")?;
    let n_str = inner.strip_suffix(')')?;
    n_str.parse::<u64>().ok()
}

/// Extract the declared fixed bit length from a BIT(n) field's metadata.
/// Returns the `n` from `"BIT(n)"`. Panics if called on a non-BIT field.
pub(crate) fn bit_fixed_len(field: &arrow_schema::Field) -> u64 {
    let v = field
        .metadata()
        .get(BASIN_TYPE_KEY)
        .expect("BIT field must have BASIN_TYPE metadata");
    let inner = v
        .strip_prefix(BASIN_TYPE_BIT_PREFIX)
        .and_then(|s| s.strip_suffix(')'))
        .expect("BIT field BASIN_TYPE must be BIT(n)");
    inner
        .parse::<u64>()
        .expect("BIT(n) metadata must contain a valid integer")
}

/// Convenience helper to read the `BASIN_TYPE` marker from a field's metadata.
/// Used by the pgwire encoder and REST layer to distinguish logical sub-types
/// that share the same Arrow physical type (e.g. INET vs CIDR vs MACADDR all
/// on Utf8). Left `#[allow(dead_code)]` because the encoder lives outside
/// this crate; it will call through the public API.
#[allow(dead_code)]
pub(crate) fn field_type_marker(field: &arrow_schema::Field) -> Option<&str> {
    field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str())
}

pub(crate) fn field_is_auto_update(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get(BASIN_AUTO_UPDATE_KEY)
        .map(|s| s.as_str())
        == Some("1")
}

pub(crate) fn field_is_soft_delete(field: &arrow_schema::Field) -> bool {
    field
        .metadata()
        .get(BASIN_SOFT_DELETE_KEY)
        .map(|s| s.as_str())
        == Some("1")
}

/// Return the stored expression text for a `GENERATED ALWAYS AS (...)
/// STORED` column, or `None` for ordinary columns. The text is the
/// parenthesised expression with the outer parens stripped.
pub(crate) fn field_is_generated(field: &arrow_schema::Field) -> Option<&str> {
    field.metadata().get(BASIN_GENERATED_AS).map(|s| s.as_str())
}

/// Return the stored DEFAULT expression text for a column declared with
/// `DEFAULT <expr>`, or `None` for columns without an explicit default.
/// The text is the user's expression source (e.g. `nextval('s')`,
/// `'pending'`).
pub(crate) fn field_default_text(field: &arrow_schema::Field) -> Option<&str> {
    field
        .metadata()
        .get(BASIN_COLUMN_DEFAULT)
        .map(|s| s.as_str())
}

/// Identity mode for a column, derived from the `BASIN_IDENTITY` metadata
/// marker. `None` for ordinary columns; `Some(IdentityMode::Always)` for
/// `GENERATED ALWAYS AS IDENTITY`; `Some(IdentityMode::ByDefault)` for
/// `GENERATED BY DEFAULT AS IDENTITY` and the `SERIAL` family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdentityMode {
    Always,
    ByDefault,
}

/// Return the identity mode for `field`, or `None` for ordinary columns.
pub(crate) fn field_identity_mode(field: &arrow_schema::Field) -> Option<IdentityMode> {
    match field.metadata().get(BASIN_IDENTITY).map(|s| s.as_str())? {
        BASIN_IDENTITY_ALWAYS => Some(IdentityMode::Always),
        BASIN_IDENTITY_BY_DEFAULT => Some(IdentityMode::ByDefault),
        _ => None,
    }
}

/// Return the backing sequence name for an IDENTITY / SERIAL column.
pub(crate) fn field_identity_sequence(field: &arrow_schema::Field) -> Option<&str> {
    field.metadata().get(BASIN_IDENTITY_SEQ).map(|s| s.as_str())
}

/// Locate the unique soft-delete column on `schema`, if any. Returns the
/// column's name. There is at most one (enforced at CREATE TABLE).
pub(crate) fn soft_delete_column(schema: &arrow_schema::Schema) -> Option<String> {
    for f in schema.fields() {
        if field_is_soft_delete(f) {
            return Some(f.name().clone());
        }
    }
    None
}

/// Bare audit-table name when the source table was declared with
/// `AUDIT TO <name>`.
pub(crate) fn audit_table_name(schema: &arrow_schema::Schema) -> Option<&str> {
    schema
        .metadata()
        .get(BASIN_AUDIT_TABLE_KEY)
        .map(|s| s.as_str())
}

/// PG `SERIAL` family — recognised forms and the integer width each one
/// implies. `SERIAL` rides on the int4 / int8 / int2 surface; the
/// `nextval` machinery doesn't care which one, but we keep the
/// distinction so the column type matches what `psql \d` would print.
///
/// sqlparser 0.52 has no dedicated AST variant for SERIAL — every form
/// lands in `Custom` with the keyword as the identifier. We match on
/// the unparameterised identifier (no modifiers) so `serial(8)` (which
/// PG would reject anyway) stays out of this matcher.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SerialKind {
    /// `SMALLSERIAL` / `SERIAL2` → INT2-backed.
    Small,
    /// `SERIAL` / `SERIAL4` → INT4-backed (Int32, OID 23).
    Regular,
    /// `BIGSERIAL` / `SERIAL8` → INT8-backed.
    Big,
}

/// Recognise `SERIAL` / `SMALLSERIAL` / `BIGSERIAL` (+ `SERIAL2` /
/// `SERIAL4` / `SERIAL8` aliases). Returns `None` for non-SERIAL types.
///
/// SERIAL is a *pseudo-type* in PG: it expands to an integer column
/// with an auto-created sequence and a `DEFAULT nextval(...)`. The
/// expansion happens in `ddl::schema_and_constraints_from_columns`;
/// this helper exists so the type-bridge and the DDL site both agree
/// on which sqlparser shapes count as SERIAL.
pub(crate) fn serial_kind(sql: &SqlDataType) -> Option<SerialKind> {
    if let SqlDataType::Custom(name, modifiers) = sql {
        if !modifiers.is_empty() || name.0.len() != 1 {
            return None;
        }
        let kw = name.0[0].id_val().to_ascii_uppercase();
        return match kw.as_str() {
            "SMALLSERIAL" | "SERIAL2" => Some(SerialKind::Small),
            "SERIAL" | "SERIAL4" => Some(SerialKind::Regular),
            "BIGSERIAL" | "SERIAL8" => Some(SerialKind::Big),
            _ => None,
        };
    }
    None
}

/// Returns true when `sql` is one of the recognised network-address type
/// keywords that ride on `Utf8` with a `BASIN_TYPE` marker.
pub(crate) fn is_inet_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "inet")
}
pub(crate) fn is_cidr_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "cidr")
}
pub(crate) fn is_macaddr_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "macaddr")
}
pub(crate) fn is_macaddr8_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "macaddr8")
}
pub(crate) fn is_money_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "money")
}
pub(crate) fn is_xml_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "xml")
}
pub(crate) fn is_int4range_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "int4range")
}
pub(crate) fn is_int8range_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "int8range")
}
pub(crate) fn is_numrange_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "numrange")
}
pub(crate) fn is_daterange_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "daterange")
}
pub(crate) fn is_tsrange_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "tsrange")
}
pub(crate) fn is_tstzrange_sql(sql: &SqlDataType) -> bool {
    custom_type_name_eq(sql, "tstzrange")
}
pub(crate) fn is_tsvector_sql(sql: &SqlDataType) -> bool {
    matches!(sql, SqlDataType::TsVector) || custom_type_name_eq(sql, "tsvector")
}
pub(crate) fn is_tsquery_sql(sql: &SqlDataType) -> bool {
    matches!(sql, SqlDataType::TsQuery) || custom_type_name_eq(sql, "tsquery")
}

/// True when `sql` is an unparameterised `Custom` whose identifier matches
/// `keyword` case-insensitively.
fn custom_type_name_eq(sql: &SqlDataType, keyword: &str) -> bool {
    if let SqlDataType::Custom(name, modifiers) = sql {
        name.0.len() == 1
            && name.0[0].id_val().eq_ignore_ascii_case(keyword)
            && modifiers.is_empty()
    } else {
        false
    }
}

/// Map a sqlparser column type to an Arrow [`DataType`]. Only the small set
/// listed in the engine's PoC SQL contract is accepted; anything else is an
/// `InvalidSchema` error so callers see exactly which type they tripped on.
pub(crate) fn arrow_data_type(sql: &SqlDataType) -> Result<DataType> {
    // SERIAL family mapping — each width keeps its natural Arrow type so that
    // the INSERT path, pgwire RowDescription, and param-decode all agree.
    //   SMALLSERIAL / SERIAL2  → Int16 (INT2, OID 21)
    //   SERIAL / SERIAL4       → Int32 (INT4, OID 23)
    //   BIGSERIAL / SERIAL8    → Int64 (INT8, OID 20)
    // The auto-created sequence + DEFAULT lives in the DDL site, not here.
    if let Some(kind) = serial_kind(sql) {
        return match kind {
            SerialKind::Small => Ok(DataType::Int16),
            SerialKind::Regular => Ok(DataType::Int32),
            SerialKind::Big => Ok(DataType::Int64),
        };
    }
    match sql {
        // INT / INTEGER / INT4  → 32-bit (PG default integer width, OID 23).
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => Ok(DataType::Int32),

        // BIGINT / INT8 → 64-bit (OID 20). Unchanged.
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => Ok(DataType::Int64),

        // SMALLINT / INT2 → 16-bit (OID 21).
        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => Ok(DataType::Int16),

        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::String(_) => Ok(DataType::Utf8),

        SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),

        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float(_) => Ok(DataType::Float64),

        // REAL / FLOAT4 — 32-bit floating point. PG's `real` is a synonym for
        // `float4`. sqlparser has both a `Real` variant and a `Float4` variant
        // (introduced alongside `Float8`); both map to Arrow Float32.
        SqlDataType::Real | SqlDataType::Float4 => Ok(DataType::Float32),

        SqlDataType::Bytea => Ok(DataType::Binary),

        // DATE — day-resolution, no time component, no timezone. Arrow's
        // `Date32` matches PG's wire format (days since 1970-01-01).
        SqlDataType::Date => Ok(DataType::Date32),

        // TIME / TIME WITHOUT TIME ZONE → microsecond-resolution time-of-day.
        // TIMETZ is not on the v0.1 roadmap; reject so callers see the gap.
        SqlDataType::Time(_, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
            Ok(DataType::Time64(TimeUnit::Microsecond))
        }

        // JSONB / JSON. sqlparser 0.52 surfaces both as dedicated AST
        // variants. JSONB rides on Arrow `LargeBinary`, with the bytes
        // being canonical-form serialised JSON (keys sorted, no whitespace
        // — see `basin_engine::dml::coerce_jsonb`). The `BASIN_TYPE=JSONB`
        // marker on the `Field` (set in `ddl::schema_from_columns`) tells
        // the pgwire encoder to emit Postgres OID 3802 and render the
        // bytes as JSON text rather than `\x...` hex bytea. Plain `JSON`
        // is treated as a JSONB synonym for v0.1 — Postgres distinguishes
        // them (JSON keeps the user's whitespace and key order; JSONB
        // canonicalises) but supporting that distinction would mean a
        // second logical type, second metadata marker, and a second
        // INSERT path. v0.2.
        SqlDataType::JSONB | SqlDataType::JSON => Ok(DataType::LargeBinary),
        // Forward-compat: unparameterised `JSONB` typed via the `Custom`
        // catch-all (some sqlparser dialects route there for unrecognised
        // keywords). Same Arrow type, same JSONB tag downstream.
        SqlDataType::Custom(name, modifiers)
            if name.0.len() == 1
                && name.0[0].id_val().eq_ignore_ascii_case("jsonb")
                && modifiers.is_empty() =>
        {
            Ok(DataType::LargeBinary)
        }

        // UUID. sqlparser 0.52 has a dedicated `Uuid` variant; some
        // dialects also route a bare `UUID` keyword through `Custom` so we
        // accept both. The Arrow physical type is `FixedSizeBinary(16)`
        // — RFC 4122 big-endian. The `BASIN_TYPE=UUID` marker on the field
        // (set in `ddl::schema_from_columns`) is what the pgwire encoder
        // and REST layer key off to render the hyphenated canonical form.
        SqlDataType::Uuid => Ok(DataType::FixedSizeBinary(16)),
        SqlDataType::Custom(name, modifiers)
            if name.0.len() == 1
                && name.0[0].id_val().eq_ignore_ascii_case("uuid")
                && modifiers.is_empty() =>
        {
            Ok(DataType::FixedSizeBinary(16))
        }

        // TSVECTOR / TSQUERY. PG full-text search types — see the bounded
        // FTS subset in `fts_udf`.  Both are stored as plain `Utf8` holding
        // the *canonical text form* of the vector / query (e.g.
        // `'fox':2 'quick':1` for a tsvector, `'quick' & 'fox'` for a
        // tsquery).  The `BASIN_TYPE` marker (set in
        // `ddl::schema_from_columns`) distinguishes these columns from
        // ordinary TEXT so DDL round-trips faithfully and the pgwire encoder
        // can advertise OID 3614 / 3615.
        //
        // sqlparser 0.61 has dedicated `TsVector` / `TsQuery` AST variants
        // (older versions routed these through `Custom`).  We accept both the
        // dedicated variants and the legacy `Custom` form for robustness.
        SqlDataType::TsVector | SqlDataType::TsQuery => Ok(DataType::Utf8),
        SqlDataType::Custom(name, modifiers)
            if name.0.len() == 1
                && (name.0[0].id_val().eq_ignore_ascii_case("tsvector")
                    || name.0[0].id_val().eq_ignore_ascii_case("tsquery"))
                && modifiers.is_empty() =>
        {
            Ok(DataType::Utf8)
        }

        // NUMERIC / DECIMAL / DEC. PG's `numeric` rides on Arrow
        // `Decimal128(precision, scale)`. sqlparser's `Numeric`, `Decimal`,
        // and `Dec` AST variants all carry the same `ExactNumberInfo`
        // payload and are PG synonyms; we map them identically. PG accepts
        // a bare `NUMERIC` (no parens) as arbitrary-precision; Arrow can
        // only express a fixed precision, so we pick (38, 0) — Arrow's
        // `Decimal128` max precision, large enough for the typical
        // `numeric` use cases (financial sums, ID-as-numeric, etc.).
        // Validation: `1 <= p <= 38` and `0 <= s <= p`. Anything else is
        // a hard `InvalidSchema` error so the user sees the gate trip
        // explicitly rather than silently truncating to a smaller type.
        SqlDataType::Numeric(info) | SqlDataType::Decimal(info) | SqlDataType::Dec(info) => {
            decimal128_from_exact_number_info(info)
        }

        // TIMESTAMPTZ / TIMESTAMP WITH TIME ZONE → microsecond UTC.
        // Bare TIMESTAMP (no zone) → microsecond, no zone string. Both ride
        // on the same Arrow physical type; the timezone string is the only
        // distinguishing bit, and downstream layers (router OID, info_schema,
        // pgwire encoding, convert.rs bridge) already key off `Some(_)` vs
        // `None` to advertise OID 1184 vs 1114.
        SqlDataType::Timestamp(_, tz_info) => match tz_info {
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),
            _ => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        },

        // INTERVAL — PG's interval type. Stored as Arrow
        // `Interval(MonthDayNano)` which matches DataFusion's native interval
        // representation and allows arithmetic with timestamps.
        SqlDataType::Interval { .. } => {
            Ok(DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano))
        }

        // Array types: INT[], TEXT[], etc. Stored as Arrow List<element_type>.
        // Supports the `INT[]` / `TEXT[]` PG syntax (sqlparser SquareBracket
        // form). Nested arrays (e.g. INT[][]) use nested List types.
        SqlDataType::Array(ArrayElemTypeDef::SquareBracket(elem, _))
        | SqlDataType::Array(ArrayElemTypeDef::AngleBracket(elem))
        | SqlDataType::Array(ArrayElemTypeDef::Parenthesis(elem)) => {
            let elem_dt = arrow_data_type(elem)?;
            Ok(DataType::List(Arc::new(Field::new("item", elem_dt, true))))
        }
        SqlDataType::Array(ArrayElemTypeDef::None) => {
            // Bare ARRAY keyword with no element type — not representable; reject.
            Err(BasinError::InvalidSchema(
                "ARRAY type requires an element type (e.g. INT[])".into(),
            ))
        }

        // MONEY. PG's `money` type is a fixed-point 8-byte integer; we
        // represent it as Decimal128(20, 2) — enough range for any PG money
        // value, two fractional digits matching PG's default lc_monetary.
        // sqlparser surfaces `MONEY` through `Custom`; some future dialect
        // versions may add a dedicated variant. The metadata marker tells the
        // pgwire encoder to emit OID 790.
        sql if is_money_sql(sql) => Ok(DataType::Decimal128(20, 2)),

        // XML. sqlparser has a dedicated `XML` variant in some dialects and
        // also routes it through `Custom`. Physical type is Utf8.
        sql if is_xml_sql(sql) => Ok(DataType::Utf8),

        // Network address types. All stored as Utf8; metadata carries the
        // logical sub-type so the pgwire encoder can emit the right OID and
        // the REST layer can validate format on ingress.
        sql if is_inet_sql(sql) => Ok(DataType::Utf8),
        sql if is_cidr_sql(sql) => Ok(DataType::Utf8),
        sql if is_macaddr_sql(sql) => Ok(DataType::Utf8),
        sql if is_macaddr8_sql(sql) => Ok(DataType::Utf8),

        // Range types. Stored as Utf8 JSON strings of the shape
        // `{"l":<lower>,"u":<upper>,"li":<bool>,"ui":<bool>}`.
        sql if is_int4range_sql(sql) => Ok(DataType::Utf8),
        sql if is_int8range_sql(sql) => Ok(DataType::Utf8),
        sql if is_numrange_sql(sql) => Ok(DataType::Utf8),
        sql if is_daterange_sql(sql) => Ok(DataType::Utf8),
        sql if is_tsrange_sql(sql) => Ok(DataType::Utf8),
        sql if is_tstzrange_sql(sql) => Ok(DataType::Utf8),
        // (TSVECTOR / TSQUERY handled earlier via the dedicated
        // `SqlDataType::TsVector` / `TsQuery` variants + legacy `Custom`
        // fallback.)

        // sqlparser's Postgres dialect parses unknown parameterised types
        // (e.g. `vector(N)`) as `Custom`. We recognise the `vector(N)` form
        // and map it to the Arrow physical layout the rest of the engine
        // already understands: a `FixedSizeList<Float32>` of length N.
        //
        // We also handle the full set of PG network / bit-string / money
        // types here; all ride on `Utf8` (or `Decimal128` for MONEY) with a
        // `BASIN_TYPE` field metadata marker so INSERT validation and the
        // pgwire encoder can recover the logical type.
        // FTS types: TSVECTOR and TSQUERY are stored as Utf8 in v0.1 (stub
        // semantics — no real tokenisation).  The FTS UDFs in `fts_udf` all
        // accept/return Utf8, so this mapping is consistent end-to-end.
        SqlDataType::Custom(name, modifiers) => {
            let kw = name.0[0].id_val().to_ascii_uppercase();
            match kw.as_str() {
                "VECTOR" => {
                    let dim = parse_vector_dim(modifiers)?;
                    // Child field is nullable=true to match what the Arrow
                    // builder helpers produce (`FixedSizeListArray::
                    // from_iter_primitive` defaults its child to nullable). The
                    // distinction is irrelevant in practice because vector(N) at
                    // the user level never carries per-element NULLs.
                    Ok(DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        dim,
                    ))
                }
                // ── Network types ────────────────────────────────────────
                // INET / CIDR / MACADDR / MACADDR8 take no modifiers.
                // Arrow physical type: Utf8. Logical type recovered via
                // BASIN_TYPE field metadata set in ddl::schema_from_columns.
                "INET" if modifiers.is_empty() => Ok(DataType::Utf8),
                "CIDR" if modifiers.is_empty() => Ok(DataType::Utf8),
                "MACADDR" if modifiers.is_empty() => Ok(DataType::Utf8),
                "MACADDR8" if modifiers.is_empty() => Ok(DataType::Utf8),
                // ── MONEY ────────────────────────────────────────────────
                // MONEY has no user-visible precision/scale in PG. We pin at
                // Decimal128(20, 2) — large enough for PG's 8-byte signed
                // internal representation (max ±92233720368547758.07).
                "MONEY" if modifiers.is_empty() => Ok(DataType::Decimal128(20, 2)),
                // ── Bit-string types ─────────────────────────────────────
                // BIT(n): fixed-length; modifier list has exactly one entry.
                // BIT without modifier defaults to length 1 (PG semantics).
                "BIT" => {
                    // sqlparser surfaces BIT VARYING as two separate tokens;
                    // bare "BIT" with no modifiers = BIT(1).
                    if !modifiers.is_empty() {
                        let n: u64 = modifiers[0].trim().parse().map_err(|_| {
                            BasinError::InvalidSchema(format!(
                                "BIT type requires a positive integer length, got {:?}",
                                modifiers[0]
                            ))
                        })?;
                        if n == 0 {
                            return Err(BasinError::InvalidSchema("BIT(n): n must be >= 1".into()));
                        }
                    }
                    Ok(DataType::Utf8)
                }
                // VARBIT(n) / VARBIT: variable-length bit string.
                "VARBIT" => Ok(DataType::Utf8),
                // ── FTS types ────────────────────────────────────────────
                // TSVECTOR / TSQUERY stored as Utf8 (stub — no real FTS engine).
                "TSVECTOR" | "TSQUERY" if modifiers.is_empty() => Ok(DataType::Utf8),
                // ── CITEXT ───────────────────────────────────────────────
                // Case-insensitive text. Stored as plain Utf8 in Basin v0.1.
                "CITEXT" if modifiers.is_empty() => Ok(DataType::Utf8),
                // ── POINT ────────────────────────────────────────────────
                // PostGIS-style POINT — stored as 21-byte little-endian WKB
                // (1 endian + 4 type + 8 X + 8 Y) in `FixedSizeBinary(21)`.
                // The BASIN_TYPE=POINT marker (added in `basin_type_marker`)
                // tells the INSERT-coercion + pgwire-encoder paths to treat
                // these bytes as a POINT logical value rather than opaque
                // bytea. ST_MakePoint / ST_X / ST_Y UDFs in `geo_glue` round-
                // trip the same 21-byte encoding.
                "POINT" if modifiers.is_empty() => Ok(DataType::FixedSizeBinary(
                    basin_geo::POINT_WKB_LEN as i32,
                )),
                _ => Err(BasinError::InvalidSchema(format!(
                    "unsupported custom type: {name}"
                ))),
            }
        }

        // ── Native BIT / BIT VARYING / VARBIT variants ──────────────────
        // sqlparser 0.61 parses `BIT(8)` as `SqlDataType::Bit(Some(8))`,
        // `BIT VARYING(16)` as `SqlDataType::BitVarying(Some(16))`, and the
        // PostgreSQL alias `VARBIT(16)` as the distinct `SqlDataType::VarBit`.
        // The `Custom("BIT", …)` / `Custom("VARBIT", …)` branches above handle
        // bare-keyword forms that arrive as Custom AST nodes; these arms handle
        // the native AST variants so all parse paths produce `DataType::Utf8`.
        // The `BASIN_TYPE` marker (e.g. "BIT(8)" / "VARBIT(16)") is
        // attached by `ddl::schema_from_columns` via `basin_type_marker`.
        SqlDataType::Bit(len) => {
            let n = len.unwrap_or(1);
            if n == 0 {
                return Err(BasinError::InvalidSchema("BIT(n): n must be >= 1".into()));
            }
            Ok(DataType::Utf8)
        }
        // BIT VARYING(n) and the PG alias VARBIT(n) are both variable-length;
        // both map to Arrow Utf8 with a BASIN_TYPE=VARBIT(n) sidecar.
        SqlDataType::BitVarying(_) | SqlDataType::VarBit(_) => Ok(DataType::Utf8),

        other => Err(BasinError::InvalidSchema(format!(
            "unsupported column type in PoC: {other}"
        ))),
    }
}

/// Map sqlparser's `ExactNumberInfo` (the `(p, s)` payload of `NUMERIC` /
/// `DECIMAL` / `DEC`) onto an Arrow `Decimal128(precision, scale)`. PG
/// rules: bare `NUMERIC` is arbitrary-precision; we pin it at `(38, 0)`
/// — Arrow's `Decimal128` max precision, big enough for typical PG
/// `numeric` use cases. `NUMERIC(p)` defaults scale to 0, matching PG.
/// Out-of-range precision/scale is rejected up front so a user can't
/// silently end up with a smaller-than-expected column.
fn decimal128_from_exact_number_info(info: &ExactNumberInfo) -> Result<DataType> {
    let (p, s): (u8, i8) = match info {
        ExactNumberInfo::None => (38, 0),
        ExactNumberInfo::Precision(p) => (clamp_precision(*p)?, 0),
        ExactNumberInfo::PrecisionAndScale(p, s) => {
            let prec = clamp_precision(*p)?;
            if *s > i8::MAX as i64 || *s < i8::MIN as i64 {
                return Err(BasinError::InvalidSchema(format!(
                    "NUMERIC scale {s} exceeds i8 range"
                )));
            }
            let scale = *s as i8;
            if scale < 0 || (scale as i16) > (prec as i16) {
                return Err(BasinError::InvalidSchema(format!(
                    "NUMERIC scale {scale} must satisfy 0 <= scale <= precision ({prec})"
                )));
            }
            (prec, scale)
        }
    };
    Ok(DataType::Decimal128(p, s))
}

fn clamp_precision(p: u64) -> Result<u8> {
    if p == 0 || p > 38 {
        return Err(BasinError::InvalidSchema(format!(
            "NUMERIC precision {p} out of range; Arrow Decimal128 supports 1..=38"
        )));
    }
    Ok(p as u8)
}

/// Return the `BASIN_TYPE` metadata value for the new stub types handled via
/// `Custom` AST nodes: network types (`INET`, `CIDR`, `MACADDR`, `MACADDR8`),
/// bit-string types (`BIT(n)`, `VARBIT(n)`), and `MONEY`. Returns `None` for
/// all other types (including `vector`, `jsonb`, `uuid`, and all built-ins).
///
/// Called from `ddl::schema_from_columns` after `arrow_data_type` so the
/// metadata marker is attached to the resulting `Field` immediately.
pub(crate) fn basin_type_marker(sql: &SqlDataType) -> Option<String> {
    // Handle the native sqlparser AST variants for BIT / BIT VARYING before
    // the Custom fallback so both parse paths get the same metadata marker.
    match sql {
        SqlDataType::Bit(len) => {
            let n = len.unwrap_or(1);
            return Some(format!("BIT({n})"));
        }
        SqlDataType::BitVarying(len) | SqlDataType::VarBit(len) => {
            return match len {
                Some(n) => Some(format!("VARBIT({n})")),
                None => Some("VARBIT".to_string()),
            };
        }
        _ => {}
    }

    let SqlDataType::Custom(name, modifiers) = sql else {
        return None;
    };
    if name.0.len() != 1 {
        return None;
    }
    let kw = name.0[0].id_val().to_ascii_uppercase();
    match kw.as_str() {
        "INET" if modifiers.is_empty() => Some(BASIN_TYPE_INET.to_string()),
        "CIDR" if modifiers.is_empty() => Some(BASIN_TYPE_CIDR.to_string()),
        "MACADDR" if modifiers.is_empty() => Some(BASIN_TYPE_MACADDR.to_string()),
        "MACADDR8" if modifiers.is_empty() => Some(BASIN_TYPE_MACADDR8.to_string()),
        "MONEY" if modifiers.is_empty() => Some(BASIN_TYPE_MONEY.to_string()),
        "BIT" => {
            // BIT(n) → metadata value "BIT(n)"; bare BIT → "BIT(1)".
            let n: u64 = if modifiers.is_empty() {
                1
            } else {
                modifiers[0].trim().parse().unwrap_or(1)
            };
            Some(format!("BIT({n})"))
        }
        "VARBIT" => {
            // VARBIT(n) → "VARBIT(n)"; bare VARBIT → "VARBIT".
            if modifiers.is_empty() {
                Some("VARBIT".to_string())
            } else {
                Some(format!("VARBIT({})", modifiers[0].trim()))
            }
        }
        "CITEXT" if modifiers.is_empty() => Some(BASIN_TYPE_CITEXT.to_string()),
        // Range types — all stored as Utf8 JSON.
        "INT4RANGE" if modifiers.is_empty() => Some(BASIN_TYPE_INT4RANGE.to_string()),
        "INT8RANGE" if modifiers.is_empty() => Some(BASIN_TYPE_INT8RANGE.to_string()),
        "NUMRANGE" if modifiers.is_empty() => Some(BASIN_TYPE_NUMRANGE.to_string()),
        "DATERANGE" if modifiers.is_empty() => Some(BASIN_TYPE_DATERANGE.to_string()),
        "TSRANGE" if modifiers.is_empty() => Some(BASIN_TYPE_TSRANGE.to_string()),
        "TSTZRANGE" if modifiers.is_empty() => Some(BASIN_TYPE_TSTZRANGE.to_string()),
        // XML — stored as Utf8.
        "XML" if modifiers.is_empty() => Some(BASIN_TYPE_XML.to_string()),
        // POINT — PostGIS-style 2D point stored as 21-byte WKB
        // (FixedSizeBinary(21)).
        "POINT" if modifiers.is_empty() => Some(BASIN_TYPE_POINT.to_string()),
        _ => None,
    }
}

/// Pull the dimensionality out of `vector(N)`'s modifier list. sqlparser
/// stores the parenthesised modifiers as raw strings; we accept exactly one
/// positive integer.
fn parse_vector_dim(modifiers: &[String]) -> Result<i32> {
    if modifiers.len() != 1 {
        return Err(BasinError::InvalidSchema(format!(
            "vector type requires one dimension argument, got {}",
            modifiers.len()
        )));
    }
    let n: i32 = modifiers[0].trim().parse().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "vector dimension must be a positive integer, got {:?}: {e}",
            modifiers[0]
        ))
    })?;
    if n <= 0 {
        return Err(BasinError::InvalidSchema(format!(
            "vector dimension must be > 0, got {n}"
        )));
    }
    Ok(n)
}

/// Decode a `'[a, b, c]'` literal into a `Vec<f32>`. Used both at INSERT time
/// and at query time when the engine sees a vector literal in a UDF call.
pub(crate) fn parse_vector_literal(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "vector literal must be bracketed `[...]`, got {trimmed:?}"
            ))
        })?;
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for piece in inner.split(',') {
        let p = piece.trim();
        let v: f32 = p
            .parse()
            .map_err(|e| BasinError::InvalidSchema(format!("bad vector element {p:?}: {e}")))?;
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    fn parse_col_type(sql: &str) -> SqlDataType {
        let ddl = format!("CREATE TABLE t (c {sql})");
        let mut stmts = Parser::parse_sql(&PostgreSqlDialect {}, &ddl).unwrap();
        match stmts.pop().unwrap() {
            sqlparser::ast::Statement::CreateTable(ct) => ct.columns[0].data_type.clone(),
            other => panic!("not a CREATE TABLE: {other:?}"),
        }
    }

    #[test]
    fn vector_n_parses_to_fixed_size_list_float32() {
        let dt = arrow_data_type(&parse_col_type("vector(4)")).unwrap();
        match dt {
            DataType::FixedSizeList(field, n) => {
                assert_eq!(*field.data_type(), DataType::Float32);
                assert!(field.is_nullable());
                assert_eq!(n, 4);
            }
            other => panic!("expected FixedSizeList, got {other:?}"),
        }
    }

    #[test]
    fn vector_dim_must_be_positive() {
        let err = arrow_data_type(&parse_col_type("vector(0)")).unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn vector_literal_parse_basic() {
        let v = parse_vector_literal("[0.1, 0.2, -0.3]").unwrap();
        assert_eq!(v, vec![0.1f32, 0.2, -0.3]);
    }

    #[test]
    fn vector_literal_empty() {
        let v = parse_vector_literal("[]").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn vector_literal_must_be_bracketed() {
        let err = parse_vector_literal("0.1, 0.2").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    /// `JSONB` and `JSON` both round-trip to `LargeBinary` via the dedicated
    /// sqlparser variants. The metadata marker is added by ddl::schema_from_
    /// columns, not here — `arrow_data_type` only owns the Arrow datatype.
    #[test]
    fn jsonb_keyword_parses_to_large_binary() {
        let dt = arrow_data_type(&parse_col_type("JSONB")).unwrap();
        assert_eq!(dt, DataType::LargeBinary);
    }

    #[test]
    fn json_keyword_also_parses_to_large_binary() {
        // Plain `JSON` is treated as JSONB for v0.1 (see the comment in
        // `arrow_data_type`). When v0.2 splits the two, this test should
        // flip to `BASIN_TYPE=JSON` (or whatever marker we choose).
        let dt = arrow_data_type(&parse_col_type("JSON")).unwrap();
        assert_eq!(dt, DataType::LargeBinary);
    }

    /// `UUID` parses to `FixedSizeBinary(16)`. The metadata marker is
    /// added by `ddl::schema_from_columns`; here we only check the
    /// underlying Arrow physical layout.
    #[test]
    fn uuid_keyword_parses_to_fixed_size_binary_16() {
        let dt = arrow_data_type(&parse_col_type("UUID")).unwrap();
        assert_eq!(dt, DataType::FixedSizeBinary(16));
    }

    // ── Narrow integer / float type-fidelity tests (#66) ────────────────────

    /// `INTEGER` / `INT` / `INT4` → Arrow `Int32`, not `Int64`. This is the
    /// canonical PG 4-byte integer width; pgwire OID 23.
    #[test]
    fn integer_maps_to_int32() {
        assert_eq!(
            arrow_data_type(&parse_col_type("INTEGER")).unwrap(),
            DataType::Int32
        );
    }

    #[test]
    fn int_keyword_maps_to_int32() {
        assert_eq!(
            arrow_data_type(&parse_col_type("INT")).unwrap(),
            DataType::Int32
        );
    }

    #[test]
    fn int4_maps_to_int32() {
        assert_eq!(
            arrow_data_type(&parse_col_type("INT4")).unwrap(),
            DataType::Int32
        );
    }

    /// `SMALLINT` / `INT2` → Arrow `Int16` (PG OID 21).
    #[test]
    fn smallint_maps_to_int16() {
        assert_eq!(
            arrow_data_type(&parse_col_type("SMALLINT")).unwrap(),
            DataType::Int16
        );
    }

    #[test]
    fn int2_maps_to_int16() {
        assert_eq!(
            arrow_data_type(&parse_col_type("INT2")).unwrap(),
            DataType::Int16
        );
    }

    /// `BIGINT` / `INT8` stays `Int64` (PG OID 20). Regression guard.
    #[test]
    fn bigint_stays_int64() {
        assert_eq!(
            arrow_data_type(&parse_col_type("BIGINT")).unwrap(),
            DataType::Int64
        );
    }

    #[test]
    fn int8_stays_int64() {
        assert_eq!(
            arrow_data_type(&parse_col_type("INT8")).unwrap(),
            DataType::Int64
        );
    }

    /// `REAL` → Arrow `Float32` (PG OID 700).
    #[test]
    fn real_maps_to_float32() {
        assert_eq!(
            arrow_data_type(&parse_col_type("REAL")).unwrap(),
            DataType::Float32
        );
    }

    /// `FLOAT4` → Arrow `Float32` (same as REAL, PG OID 700).
    #[test]
    fn float4_maps_to_float32() {
        assert_eq!(
            arrow_data_type(&parse_col_type("FLOAT4")).unwrap(),
            DataType::Float32
        );
    }

    /// `DOUBLE PRECISION` / `FLOAT8` stays `Float64` (PG OID 701). Regression guard.
    #[test]
    fn double_precision_stays_float64() {
        assert_eq!(
            arrow_data_type(&parse_col_type("DOUBLE PRECISION")).unwrap(),
            DataType::Float64
        );
    }

    /// `SERIAL` → Arrow `Int32` (PG OID 23, INT4-backed).
    #[test]
    fn serial_maps_to_int32() {
        assert_eq!(
            arrow_data_type(&parse_col_type("SERIAL")).unwrap(),
            DataType::Int32
        );
    }

    /// `BIGSERIAL` → Arrow `Int64` (PG OID 20).
    #[test]
    fn bigserial_stays_int64() {
        assert_eq!(
            arrow_data_type(&parse_col_type("BIGSERIAL")).unwrap(),
            DataType::Int64
        );
    }
}
