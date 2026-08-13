//! A [`CatalogSource`] backed by Basin's real per-table schema data.
//!
//! # The boundary this file has to respect
//!
//! `crates/basin-pgcatalog/src/lib.rs` documents a deliberate constraint:
//! this crate is "`basin-pgtype` plus `arrow-array`/`arrow-schema` only, no
//! DataFusion, no `basin-storage`, no tokio". `basin_catalog::Catalog` — the
//! trait that would give us `TableMetadata` — is `#[async_trait]` end to
//! end, with no sync facade, and `basin-catalog`'s own `Cargo.toml` pulls in
//! `tokio`, `tokio-postgres`, `deadpool-postgres` and `reqwest` as ordinary
//! (non-optional, non-dev) dependencies. Adding `basin-catalog` to *this*
//! crate's `[dependencies]` — even only to name the `TableMetadata` type —
//! would drag that whole async/network/runtime stack into every consumer of
//! `basin-pgcatalog`, on every build, not only the caller that actually
//! talks to a live catalog. `cargo tree -p basin-pgcatalog` has zero `tokio`
//! in it today; that is exactly the property worth keeping.
//!
//! So [`RealCatalogSource`] does not hold a `dyn basin_catalog::Catalog` and
//! never calls one — it is not async and needs no runtime to run. It is
//! built purely from the same *plain, already-fetched* data a
//! `TableMetadata` is made of: an `Arc<arrow_schema::Schema>` — literally
//! `TableMetadata::schema`'s own type — plus the handful of primitives
//! (name, owner, kind) [`CatalogSource`] needs. The actual catalog round
//! trip (`Catalog::list_tables` + `Catalog::load_table`, both `async fn`)
//! happens in the caller, which by construction already has a runtime
//! (it is talking to a real catalog); only the resulting owned,
//! already-`.await`-ed data crosses into this crate. That caller is not
//! written here — wiring it up is the follow-up this file exists to make
//! possible, not to itself perform.
//!
//! **Verdict: the boundary holds.** `[dependencies]` in this crate's
//! `Cargo.toml` is unchanged — still just `basin-pgtype`, `arrow-array`,
//! `arrow-schema`. `basin-catalog` (plus `tokio`, `object_store`,
//! `tempfile`) appear only in `[dev-dependencies]`, used exclusively by this
//! module's own test suite to prove the conversion holds against a real
//! catalog end to end. Dev-dependencies do not propagate to downstream
//! consumers or to a plain `cargo build -p basin-pgcatalog`, so this is the
//! same trick the standard library itself relies on for doctests/tests, not
//! a loophole specific to this crate.
//!
//! # Why the columns are read off `Field` metadata, not a `Catalog` call
//!
//! Basin has no integer OIDs of its own (tables are addressed by
//! [`basin_common::TableName`]/[`basin_common::ProjectId`], not `pg_class`
//! row numbers) and, as of this writing, `basin_catalog::TableMetadata`
//! carries type modifiers (`VARCHAR(n)`, `NUMERIC(p,s)`) nowhere *except* on
//! the Arrow `Schema` it already stores — as `Field` metadata markers
//! `basin-engine`'s DDL layer stamps at `CREATE TABLE` time
//! (`crates/basin-engine/src/types.rs`). This module reads exactly those
//! markers (mirrored below as local constants — importing `basin-engine`
//! itself would pull DataFusion in, a strictly worse version of the same
//! problem `basin-catalog` would cause). Confirms
//! `crates/basin-catalog/src/info_schema.rs`'s own `pg_attribute` currently
//! hard-codes `atttypmod = -1` for every row (see its
//! `pg_attribute_emits_atttypmod_neg1` test) — i.e. today there is no other
//! source of truth for this than the `Field` metadata this module reads.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use basin_pgtype::{oid, typmod};

use crate::{
    catalog_source::{
        CatalogSource, ColumnDefaultInfo, ColumnInfo, CommentInfo, ConstraintInfo, IndexInfo,
        NamespaceInfo, TableInfo,
    },
    Oid,
};

/// Field metadata key `basin-engine` stamps on a column with a declared
/// `VARCHAR(n)`/`CHARACTER VARYING(n)`/`CHAR(n)`/`CHARACTER(n)` length
/// limit. Value format is `"varchar(n)"` or `"char(n)"`. Mirrors
/// `basin_engine::types::BASIN_CHARLEN_KEY` — see the module docs for why
/// this is duplicated rather than imported.
pub const BASIN_CHARLEN_KEY: &str = "BASIN_CHARLEN";

/// Field metadata key marking `GENERATED ALWAYS AS (<expr>) STORED`.
/// Presence (any value) is what matters; the value is the expression's
/// source text. Mirrors `basin_engine::types::BASIN_GENERATED_AS`.
pub const BASIN_GENERATED_AS_KEY: &str = "BASIN_GENERATED_AS";

/// Field metadata key for `GENERATED ALWAYS/BY DEFAULT AS IDENTITY` (and the
/// `SERIAL` family). Value is [`BASIN_IDENTITY_ALWAYS`] or
/// [`BASIN_IDENTITY_BY_DEFAULT`]. Mirrors
/// `basin_engine::types::BASIN_IDENTITY`.
pub const BASIN_IDENTITY_KEY: &str = "BASIN_IDENTITY";
/// Mirrors `basin_engine::types::BASIN_IDENTITY_ALWAYS`.
pub const BASIN_IDENTITY_ALWAYS: &str = "ALWAYS";
/// Mirrors `basin_engine::types::BASIN_IDENTITY_BY_DEFAULT`.
pub const BASIN_IDENTITY_BY_DEFAULT: &str = "BY_DEFAULT";

/// Field metadata key for a column's `DEFAULT <expr>`. Value is the
/// default's source text, exactly what [`crate::pg_attrdef`] wants for
/// `expr_src`. Mirrors `basin_engine::types::BASIN_COLUMN_DEFAULT`.
pub const BASIN_COLUMN_DEFAULT_KEY: &str = "BASIN_COLUMN_DEFAULT";

/// One real table's shape, as handed to [`RealCatalogSource::with_table`]:
/// the [`TableInfo`] row plus the table's actual Arrow schema (the same
/// `Arc<Schema>` `TableMetadata::schema` carries) and whatever index /
/// constraint rows the caller has for it.
#[derive(Debug, Clone)]
struct RealTable {
    info: TableInfo,
    schema: Arc<Schema>,
    indexes: Vec<IndexInfo>,
    constraints: Vec<ConstraintInfo>,
}

/// A [`CatalogSource`] whose rows are derived from Basin's real per-table
/// Arrow schemas rather than from [`crate::mock::MockCatalog`]'s hand-built
/// `Vec`s.
///
/// Construction is entirely synchronous and in-memory: a caller that has
/// already `.await`-ed `Catalog::list_tables`/`Catalog::load_table` (or
/// `load_table_meta`) hands the resulting `(TableName, Arc<Schema>)` pairs
/// to [`with_table`](Self::with_table). See the module docs for why the
/// `async` half of that round trip is deliberately not this module's job.
#[derive(Debug, Clone, Default)]
pub struct RealCatalogSource {
    namespaces: Vec<NamespaceInfo>,
    tables: Vec<RealTable>,
}

impl RealCatalogSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_namespace(mut self, ns: NamespaceInfo) -> Self {
        self.namespaces.push(ns);
        self
    }

    /// Register one table: its `pg_class` row plus its real Arrow schema.
    /// `pg_attribute`/`pg_attrdef` rows are derived from `schema`'s fields
    /// (in declaration order — `attnum` is `1..=schema.fields().len()`) the
    /// next time [`columns`](CatalogSource::columns) or
    /// [`column_defaults`](CatalogSource::column_defaults) is called; there
    /// is no separate registration step for them.
    pub fn with_table(mut self, info: TableInfo, schema: Arc<Schema>) -> Self {
        self.tables.push(RealTable {
            info,
            schema,
            indexes: Vec::new(),
            constraints: Vec::new(),
        });
        self
    }

    /// Attach `pg_index` rows to a table already registered via
    /// [`with_table`](Self::with_table). A no-op if `table_oid` was never
    /// registered.
    pub fn with_indexes(mut self, table_oid: Oid, indexes: Vec<IndexInfo>) -> Self {
        if let Some(t) = self.tables.iter_mut().find(|t| t.info.oid == table_oid) {
            t.indexes = indexes;
        }
        self
    }

    /// Attach `pg_constraint` rows to a table already registered via
    /// [`with_table`](Self::with_table). A no-op if `table_oid` was never
    /// registered.
    pub fn with_constraints(mut self, table_oid: Oid, constraints: Vec<ConstraintInfo>) -> Self {
        if let Some(t) = self.tables.iter_mut().find(|t| t.info.oid == table_oid) {
            t.constraints = constraints;
        }
        self
    }

    /// A deterministic surrogate OID for an object Basin identifies by name
    /// rather than by integer id (Basin has no `pg_class.oid`-equivalent of
    /// its own). `name` is expected to be something stable and unique for
    /// the object — e.g. `"<schema>.<table>"` for a table. Always lands at
    /// `16384` or above, matching Postgres's own convention that OIDs below
    /// that are reserved for system objects.
    pub fn oid_for_name(name: &str) -> Oid {
        const FIRST_USER_OID: u32 = 16384;
        let h = fnv1a(name.as_bytes());
        Oid(FIRST_USER_OID + (h % (u32::MAX - FIRST_USER_OID)))
    }
}

/// FNV-1a, 32-bit. Simple, dependency-free, and stable across runs/
/// processes — unlike `std`'s default hasher, which is randomized per
/// process and therefore useless for a surrogate id that must stay the same
/// across calls.
fn fnv1a(bytes: &[u8]) -> u32 {
    const OFFSET_BASIS: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    let mut hash = OFFSET_BASIS;
    for b in bytes {
        hash ^= u32::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// The `"varchar(n)"`/`"char(n)"` marker's `n`, or `None` if `v` doesn't
/// parse as one (defensive — a marker this module didn't write itself).
fn parse_charlen_n(v: &str) -> Option<i32> {
    let inner = v
        .strip_prefix("varchar(")
        .or_else(|| v.strip_prefix("char("))?;
    inner.strip_suffix(')')?.parse().ok()
}

/// `pg_attribute.atttypid` for one Arrow field. Numeric/character types
/// that carry a modifier are still mapped to their base OID here —
/// `NUMERIC(10,2)` is `atttypid = 1700` (`numeric`) with the precision/scale
/// living in `atttypmod`, exactly as real Postgres does it.
///
/// Falls back to `oid::TEXT` for any Arrow `DataType` this mapping doesn't
/// yet recognise (e.g. `List`, `Struct`) — a deliberate, documented
/// simplification for this increment, in the same spirit as
/// `pg_attribute`'s own `attlen: typlen(..).unwrap_or(0)` fallback.
fn arrow_type_oid(data_type: &DataType, metadata: &HashMap<String, String>) -> Oid {
    match data_type {
        DataType::Boolean => oid::BOOL,
        DataType::Int16 => oid::INT2,
        DataType::Int32 => oid::INT4,
        DataType::Int64 => oid::INT8,
        DataType::Float32 => oid::FLOAT4,
        DataType::Float64 => oid::FLOAT8,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => oid::NUMERIC,
        DataType::Date32 | DataType::Date64 => oid::DATE,
        DataType::Time32(_) | DataType::Time64(_) => oid::TIME,
        DataType::Timestamp(_, Some(_)) => oid::TIMESTAMPTZ,
        DataType::Timestamp(_, None) => oid::TIMESTAMP,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => oid::BYTEA,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            match metadata.get(BASIN_CHARLEN_KEY).map(String::as_str) {
                Some(v) if v.starts_with("char(") => oid::BPCHAR,
                Some(v) if v.starts_with("varchar(") => oid::VARCHAR,
                _ => oid::TEXT,
            }
        }
        _ => oid::TEXT,
    }
}

/// `pg_attribute.atttypmod` for one Arrow field: `NUMERIC(p,s)` encodes
/// straight from `Decimal128`'s own precision/scale (Arrow already carries
/// that natively, no marker needed); `VARCHAR(n)`/`CHAR(n)` decode the
/// [`BASIN_CHARLEN_KEY`] marker; everything else is
/// [`typmod::UNSPECIFIED`].
fn atttypmod_for(data_type: &DataType, metadata: &HashMap<String, String>) -> i32 {
    match data_type {
        DataType::Decimal128(p, s) => typmod::encode_numeric(i32::from(*p), i32::from(*s)),
        DataType::Decimal256(p, s) => typmod::encode_numeric(i32::from(*p), i32::from(*s)),
        _ => metadata
            .get(BASIN_CHARLEN_KEY)
            .and_then(|v| parse_charlen_n(v))
            .map(typmod::encode_varlena_len)
            .unwrap_or(typmod::UNSPECIFIED),
    }
}

/// One [`ColumnInfo`] from a schema `Field` plus its 1-based `attnum`.
fn column_info(table_oid: Oid, attnum: i16, field: &Field) -> ColumnInfo {
    let metadata = field.metadata();
    ColumnInfo {
        table_oid,
        name: field.name().clone(),
        attnum,
        type_oid: arrow_type_oid(field.data_type(), metadata),
        not_null: !field.is_nullable(),
        atttypmod: atttypmod_for(field.data_type(), metadata),
        // Basin's Arrow schema drops a removed column outright rather than
        // tombstoning its `Field` in place, so there is no in-band way to
        // recover "this slot used to be a column" from the schema alone —
        // matches `pg_attribute`'s own documented gap (see its module
        // docs): this crate reports `attisdropped` as given, but nothing
        // upstream of this module currently gives it `true`.
        attisdropped: false,
        attidentity: match metadata.get(BASIN_IDENTITY_KEY).map(String::as_str) {
            Some(BASIN_IDENTITY_ALWAYS) => Some('a'),
            Some(BASIN_IDENTITY_BY_DEFAULT) => Some('d'),
            _ => None,
        },
        attgenerated: metadata.contains_key(BASIN_GENERATED_AS_KEY).then_some('s'),
    }
}

impl CatalogSource for RealCatalogSource {
    fn namespaces(&self) -> Vec<NamespaceInfo> {
        self.namespaces.clone()
    }

    fn tables(&self) -> Vec<TableInfo> {
        self.tables.iter().map(|t| t.info.clone()).collect()
    }

    fn columns(&self, table_oid: Oid) -> Vec<ColumnInfo> {
        self.tables
            .iter()
            .find(|t| t.info.oid == table_oid)
            .map(|t| {
                t.schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| column_info(table_oid, (i + 1) as i16, f))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn column_defaults(&self, table_oid: Oid) -> Vec<ColumnDefaultInfo> {
        self.tables
            .iter()
            .find(|t| t.info.oid == table_oid)
            .map(|t| {
                t.schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, f)| {
                        let expr_src = f.metadata().get(BASIN_COLUMN_DEFAULT_KEY)?.clone();
                        let attnum = (i + 1) as i16;
                        Some(ColumnDefaultInfo {
                            oid: RealCatalogSource::oid_for_name(&format!(
                                "{}.attrdef.{attnum}",
                                table_oid.get()
                            )),
                            table_oid,
                            attnum,
                            expr_src,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn indexes(&self, table_oid: Oid) -> Vec<IndexInfo> {
        self.tables
            .iter()
            .find(|t| t.info.oid == table_oid)
            .map(|t| t.indexes.clone())
            .unwrap_or_default()
    }

    fn constraints(&self, table_oid: Oid) -> Vec<ConstraintInfo> {
        self.tables
            .iter()
            .find(|t| t.info.oid == table_oid)
            .map(|t| t.constraints.clone())
            .unwrap_or_default()
    }

    /// Always empty. `COMMENT ON TABLE`/`COMMENT ON COLUMN` is parsed but
    /// deliberately accepted as a silent no-op today — see
    /// `crates/basin-engine/src/pg_ast.rs`'s `reject_unsupported` test, which
    /// lists `"COMMENT ON TABLE t IS 'desc'"` among the noop-accepted
    /// statements — so there is no comment storage anywhere upstream of this
    /// module for a real table's Arrow schema to carry. Once `COMMENT ON`
    /// actually persists something (most likely as `Field` metadata, the same
    /// mechanism [`BASIN_COLUMN_DEFAULT_KEY`] already uses), this method gets
    /// a real implementation the same way [`Self::column_defaults`] did.
    fn comments(&self, _table_oid: Oid) -> Vec<CommentInfo> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::{catalog_source::RelKind, Oid};

    fn schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn oid_for_name_is_deterministic_and_in_the_user_range() {
        let a = RealCatalogSource::oid_for_name("public.widgets");
        let b = RealCatalogSource::oid_for_name("public.widgets");
        let c = RealCatalogSource::oid_for_name("public.gadgets");
        assert_eq!(a, b, "same name must hash to the same oid every time");
        assert_ne!(
            a, c,
            "different names should (overwhelmingly likely) differ"
        );
        assert!(
            a.get() >= 16384,
            "surrogate oids stay out of the system range"
        );
    }

    #[test]
    fn columns_are_scoped_to_their_table_and_1_based() {
        let widgets_oid = Oid(16385);
        let gadgets_oid = Oid(16390);
        let source = RealCatalogSource::new()
            .with_table(
                TableInfo {
                    oid: widgets_oid,
                    name: "widgets".to_string(),
                    namespace: Oid(16384),
                    owner: Oid(10),
                    kind: RelKind::OrdinaryTable,
                },
                schema(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]),
            )
            .with_table(
                TableInfo {
                    oid: gadgets_oid,
                    name: "gadgets".to_string(),
                    namespace: Oid(16384),
                    owner: Oid(10),
                    kind: RelKind::OrdinaryTable,
                },
                schema(vec![Field::new("widget_id", DataType::Int32, false)]),
            );

        let widgets_cols = source.columns(widgets_oid);
        assert_eq!(widgets_cols.len(), 2);
        assert_eq!(widgets_cols[0].attnum, 1);
        assert_eq!(widgets_cols[0].name, "id");
        assert_eq!(widgets_cols[1].attnum, 2);
        assert_eq!(widgets_cols[1].name, "name");

        let gadgets_cols = source.columns(gadgets_oid);
        assert_eq!(gadgets_cols.len(), 1);
        assert_eq!(gadgets_cols[0].name, "widget_id");
    }

    #[test]
    fn not_null_comes_from_arrow_field_nullability() {
        let oid = Oid(16385);
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("bio", DataType::Utf8, true),
            ]),
        );
        let cols = source.columns(oid);
        assert!(cols[0].not_null, "non-nullable field -> NOT NULL");
        assert!(!cols[1].not_null, "nullable field -> nullable");
    }

    /// `varchar(10)` reports `atttypmod = 14` (`10 + VARHDRSZ`) — the same
    /// value `pg_attribute`'s own mock-backed test asserts, now derived from
    /// an Arrow `Field`'s `BASIN_CHARLEN` marker instead of a hand-built
    /// `ColumnInfo`.
    #[test]
    fn varchar_10_field_reports_atttypmod_14() {
        let oid = Oid(16385);
        let mut md = HashMap::new();
        md.insert(BASIN_CHARLEN_KEY.to_string(), "varchar(10)".to_string());
        let field = Field::new("name", DataType::Utf8, false).with_metadata(md);
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![field]),
        );
        let cols = source.columns(oid);
        assert_eq!(cols[0].atttypmod, 14);
        assert_eq!(cols[0].type_oid, oid::VARCHAR);
    }

    #[test]
    fn char_5_field_reports_bpchar_and_atttypmod_9() {
        let oid = Oid(16385);
        let mut md = HashMap::new();
        md.insert(BASIN_CHARLEN_KEY.to_string(), "char(5)".to_string());
        let field = Field::new("code", DataType::Utf8, false).with_metadata(md);
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![field]),
        );
        let cols = source.columns(oid);
        assert_eq!(cols[0].type_oid, oid::BPCHAR);
        assert_eq!(cols[0].atttypmod, typmod::encode_varlena_len(5));
    }

    #[test]
    fn text_field_with_no_modifier_reports_negative_one() {
        let oid = Oid(16385);
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![Field::new("bio", DataType::Utf8, true)]),
        );
        assert_eq!(source.columns(oid)[0].atttypmod, typmod::UNSPECIFIED);
    }

    #[test]
    fn numeric_10_2_encodes_from_decimal128_natively() {
        let oid = Oid(16385);
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![Field::new(
                "amount",
                DataType::Decimal128(10, 2),
                true,
            )]),
        );
        let cols = source.columns(oid);
        assert_eq!(cols[0].type_oid, oid::NUMERIC);
        assert_eq!(cols[0].atttypmod, typmod::encode_numeric(10, 2));
    }

    #[test]
    fn generated_and_identity_markers_round_trip() {
        let oid = Oid(16385);
        let mut gen_md = HashMap::new();
        gen_md.insert(
            BASIN_GENERATED_AS_KEY.to_string(),
            "name || bio".to_string(),
        );
        let mut id_md = HashMap::new();
        id_md.insert(
            BASIN_IDENTITY_KEY.to_string(),
            BASIN_IDENTITY_ALWAYS.to_string(),
        );
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![
                Field::new("seq_id", DataType::Int64, false).with_metadata(id_md),
                Field::new("full_name", DataType::Utf8, true).with_metadata(gen_md),
                Field::new("plain", DataType::Int32, false),
            ]),
        );
        let cols = source.columns(oid);
        assert_eq!(cols[0].attidentity, Some('a'));
        assert_eq!(cols[1].attgenerated, Some('s'));
        assert_eq!(cols[2].attidentity, None);
        assert_eq!(cols[2].attgenerated, None);
    }

    #[test]
    fn column_defaults_come_from_the_basin_column_default_marker() {
        let oid = Oid(16385);
        let mut md = HashMap::new();
        md.insert(
            BASIN_COLUMN_DEFAULT_KEY.to_string(),
            "'anon'::character varying".to_string(),
        );
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true).with_metadata(md),
            ]),
        );
        let defaults = source.column_defaults(oid);
        assert_eq!(defaults.len(), 1);
        assert_eq!(defaults[0].attnum, 2);
        assert_eq!(defaults[0].expr_src, "'anon'::character varying");
    }

    #[test]
    fn unknown_table_oid_yields_empty_everything() {
        let source = RealCatalogSource::new();
        assert!(source.columns(Oid(1)).is_empty());
        assert!(source.column_defaults(Oid(1)).is_empty());
        assert!(source.indexes(Oid(1)).is_empty());
        assert!(source.constraints(Oid(1)).is_empty());
        assert!(source.comments(Oid(1)).is_empty());
    }

    /// `comments` is always empty — `COMMENT ON` has no storage upstream of
    /// this module yet. See [`RealCatalogSource::comments`]'s doc comment.
    #[test]
    fn comments_are_always_empty_until_comment_on_persists_something() {
        let oid = Oid(16385);
        let source = RealCatalogSource::new().with_table(
            TableInfo {
                oid,
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            },
            schema(vec![Field::new("id", DataType::Int32, false)]),
        );
        assert!(source.comments(oid).is_empty());
    }

    // ---------------------------------------------------------------
    // The hard requirement: prove the pushdown contract against a real
    // `basin-catalog`, not only against hand-assembled `arrow_schema::Schema`
    // values. `basin-catalog` (and the `tokio`/`object_store`/`tempfile` it
    // drags along) are dev-dependencies only — see the module docs for why
    // that keeps this crate's own `[dependencies]` (and therefore every
    // downstream consumer's build) tokio-free.
    // ---------------------------------------------------------------
    mod against_a_real_catalog {
        use std::sync::Arc as StdArc;

        use arrow_array::{Array, Int32Array, StringArray, UInt32Array};
        use basin_catalog::{Catalog, ObjectStoreCatalog};
        use basin_common::{ProjectId, TableName};
        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;

        use super::*;
        use crate::{
            pg_attribute::PgAttribute, pg_class::PgClass, predicate::Predicate, SystemView,
        };

        fn col_u32(batch: &arrow_array::RecordBatch, name: &str) -> Vec<u32> {
            batch
                .column(batch.schema().index_of(name).unwrap())
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .to_vec()
        }

        fn col_str(batch: &arrow_array::RecordBatch, name: &str) -> Vec<String> {
            batch
                .column(batch.schema().index_of(name).unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.unwrap().to_string())
                .collect()
        }

        /// Build a real, on-disk-backed `basin-catalog` in a `TempDir`,
        /// register two tables through its actual `create_table`/
        /// `load_table` API (no shortcuts, no mock), and read them back
        /// through [`PgClass`]/[`PgAttribute`] via [`RealCatalogSource`].
        /// Asserts the two things that matter most: a pushed `attrelid`
        /// predicate narrows to exactly one real table's columns, and a
        /// `varchar(10)` column read off a real table definition reports
        /// `atttypmod = 14`.
        #[tokio::test]
        async fn attrelid_pushdown_and_varchar_typmod_hold_against_real_data() {
            let dir = TempDir::new().expect("tempdir");
            let store: StdArc<dyn object_store::ObjectStore> = StdArc::new(
                LocalFileSystem::new_with_prefix(dir.path()).expect("local object store"),
            );
            let catalog = ObjectStoreCatalog::new(store);
            let project = ProjectId::new();
            catalog
                .create_namespace(&project)
                .await
                .expect("create_namespace");

            let mut name_md = HashMap::new();
            name_md.insert(BASIN_CHARLEN_KEY.to_string(), "varchar(10)".to_string());
            let widgets_schema = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false).with_metadata(name_md),
                Field::new("bio", DataType::Utf8, true),
            ]);
            let widgets_name = TableName::new("widgets").unwrap();
            catalog
                .create_table(&project, &widgets_name, &widgets_schema)
                .await
                .expect("create widgets");
            let widgets_meta = catalog
                .load_table(&project, &widgets_name)
                .await
                .expect("load widgets");

            let gadgets_schema = Schema::new(vec![Field::new("widget_id", DataType::Int32, false)]);
            let gadgets_name = TableName::new("gadgets").unwrap();
            catalog
                .create_table(&project, &gadgets_name, &gadgets_schema)
                .await
                .expect("create gadgets");
            let gadgets_meta = catalog
                .load_table(&project, &gadgets_name)
                .await
                .expect("load gadgets");

            // This is the only place real `TableMetadata` fields get pulled
            // into pgcatalog shapes — plain field reads, no further catalog
            // calls, no `.await`.
            let widgets_oid = RealCatalogSource::oid_for_name("public.widgets");
            let gadgets_oid = RealCatalogSource::oid_for_name("public.gadgets");
            let public_ns = Oid(2200);

            let source = RealCatalogSource::new()
                .with_namespace(NamespaceInfo {
                    oid: public_ns,
                    name: "public".to_string(),
                    owner: Oid(10),
                })
                .with_table(
                    TableInfo {
                        oid: widgets_oid,
                        name: widgets_meta.table.as_str().to_string(),
                        namespace: public_ns,
                        owner: Oid(10),
                        kind: RelKind::OrdinaryTable,
                    },
                    widgets_meta.schema.clone(),
                )
                .with_table(
                    TableInfo {
                        oid: gadgets_oid,
                        name: gadgets_meta.table.as_str().to_string(),
                        namespace: public_ns,
                        owner: Oid(10),
                        kind: RelKind::OrdinaryTable,
                    },
                    gadgets_meta.schema.clone(),
                );

            // pg_class sees both real tables.
            let classes = PgClass.scan(&source, &[]).unwrap();
            assert_eq!(classes.num_rows(), 2);

            // The entire point of this crate: a pushed `attrelid` predicate
            // narrows to exactly one real table's columns.
            let full = PgAttribute.scan(&source, &[]).unwrap();
            assert_eq!(full.num_rows(), 4, "sanity: 3 widgets cols + 1 gadgets col");

            let widgets_only = PgAttribute
                .scan(&source, &[Predicate::eq("attrelid", widgets_oid)])
                .unwrap();
            assert_eq!(widgets_only.num_rows(), 3);
            for relid in col_u32(&widgets_only, "attrelid") {
                assert_eq!(relid, widgets_oid.get());
            }
            let mut widgets_names = col_str(&widgets_only, "attname");
            widgets_names.sort();
            assert_eq!(widgets_names, vec!["bio", "id", "name"]);

            let gadgets_only = PgAttribute
                .scan(&source, &[Predicate::eq("attrelid", gadgets_oid)])
                .unwrap();
            assert_eq!(gadgets_only.num_rows(), 1);
            assert_eq!(col_str(&gadgets_only, "attname"), vec!["widget_id"]);

            // `atttypmod` is 14 for the real `varchar(10)` column.
            let name_row = PgAttribute
                .scan(
                    &source,
                    &[
                        Predicate::eq("attrelid", widgets_oid),
                        Predicate::eq("attname", "name"),
                    ],
                )
                .unwrap();
            assert_eq!(name_row.num_rows(), 1);
            let atttypmods = name_row
                .column(name_row.schema().index_of("atttypmod").unwrap())
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(atttypmods.value(0), 14);
        }
    }
}
