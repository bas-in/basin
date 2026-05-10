//! sqlparser → Arrow type bridging for the PoC.

use std::sync::Arc;

use arrow_schema::{DataType, Field, TimeUnit};
use basin_common::{BasinError, Result};
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

pub(crate) fn field_is_auto_update(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_AUTO_UPDATE_KEY).map(|s| s.as_str()) == Some("1")
}

pub(crate) fn field_is_soft_delete(field: &arrow_schema::Field) -> bool {
    field.metadata().get(BASIN_SOFT_DELETE_KEY).map(|s| s.as_str()) == Some("1")
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
    field.metadata().get(BASIN_COLUMN_DEFAULT).map(|s| s.as_str())
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
    schema.metadata().get(BASIN_AUDIT_TABLE_KEY).map(|s| s.as_str())
}

/// Map a sqlparser column type to an Arrow [`DataType`]. Only the small set
/// listed in the engine's PoC SQL contract is accepted; anything else is an
/// `InvalidSchema` error so callers see exactly which type they tripped on.
pub(crate) fn arrow_data_type(sql: &SqlDataType) -> Result<DataType> {
    match sql {
        SqlDataType::Int(_)
        | SqlDataType::Integer(_)
        | SqlDataType::Int4(_)
        | SqlDataType::BigInt(_)
        | SqlDataType::Int8(_) => Ok(DataType::Int64),

        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => Ok(DataType::Int16),

        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::String(_) => Ok(DataType::Utf8),

        SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),

        SqlDataType::Double
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float(_) => Ok(DataType::Float64),

        SqlDataType::Bytea => Ok(DataType::Binary),

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
            if name.0.len() == 1 && name.0[0].value.eq_ignore_ascii_case("jsonb")
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
            if name.0.len() == 1 && name.0[0].value.eq_ignore_ascii_case("uuid")
                && modifiers.is_empty() =>
        {
            Ok(DataType::FixedSizeBinary(16))
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
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
            }
            _ => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        },

        // sqlparser's Postgres dialect parses unknown parameterised types
        // (e.g. `vector(N)`) as `Custom`. We recognise the `vector(N)` form
        // and map it to the Arrow physical layout the rest of the engine
        // already understands: a `FixedSizeList<Float32>` of length N.
        SqlDataType::Custom(name, modifiers) => {
            if name.0.len() == 1 && name.0[0].value.eq_ignore_ascii_case("vector") {
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
            } else {
                Err(BasinError::InvalidSchema(format!(
                    "unsupported custom type: {name}"
                )))
            }
        }

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
            if *s > i8::MAX as u64 {
                return Err(BasinError::InvalidSchema(format!(
                    "NUMERIC scale {s} exceeds i8::MAX"
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
        let v: f32 = p.parse().map_err(|e| {
            BasinError::InvalidSchema(format!(
                "bad vector element {p:?}: {e}"
            ))
        })?;
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
}
