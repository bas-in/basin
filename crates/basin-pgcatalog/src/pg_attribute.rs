//! `pg_catalog.pg_attribute` — one row per table column.
//!
//! This is the relation `docs/migration/df-removal/11-pg-catalog-fidelity.md`
//! calls the largest user-visible win for the least code, alongside
//! [`crate::pg_attrdef`]: ORM introspection (Prisma, Drizzle, SQLAlchemy,
//! ActiveRecord) reads column widths off `atttypmod`, and until this
//! relation existed Basin had nowhere to report the typmods
//! `basin_pgtype::typmod` already encodes correctly.
//!
//! # Where these values come from
//!
//! Checked against a live PostgreSQL 18, not recalled from memory — this
//! project has repeatedly found recall wrong and the server right (see
//! `crates/basin-pgtype/src/operator.rs`'s module docs for the precedent).
//! The queries:
//!
//! ```sql
//! \d pg_attribute
//!
//! CREATE TABLE attr_test (
//!   id serial PRIMARY KEY,
//!   name varchar(10) NOT NULL DEFAULT 'anon',
//!   bio text,
//!   amount numeric(10,2) DEFAULT 0,
//!   created_at timestamp DEFAULT now(),
//!   seq_id bigint GENERATED ALWAYS AS IDENTITY,
//!   full_name text GENERATED ALWAYS AS (name || bio) STORED
//! );
//!
//! SELECT attrelid, attname, atttypid, attlen, attnum, atttypmod, attnotnull,
//!        atthasdef, attisdropped, attidentity, attgenerated
//!   FROM pg_attribute
//!  WHERE attrelid = 'attr_test'::regclass
//!  ORDER BY attnum;
//! ```
//!
//! Confirmed live:
//! - `name varchar(10)` reports `atttypmod = 14` (`10 + VARHDRSZ`), matching
//!   `basin_pgtype::typmod::encode_varlena_len(10)` exactly.
//! - `bio text` (no modifier) reports `atttypmod = -1`.
//! - `amount numeric(10,2)` reports `atttypmod = 655366`, matching
//!   `basin_pgtype::typmod::encode_numeric(10, 2)` exactly.
//! - `id` (has a `DEFAULT nextval(...)`) reports `atthasdef = t`; `bio` (no
//!   default) reports `atthasdef = f`.
//! - `name` is declared `NOT NULL`, and reports `attnotnull = t`; `bio` is
//!   nullable and reports `attnotnull = f`.
//! - `attnum` is 1-based for real columns (`id` is `1`, `name` is `2`, ...).
//! - `seq_id GENERATED ALWAYS AS IDENTITY` reports `attidentity = 'a'`; every
//!   other column reports `attidentity` as the *empty string* (confirmed via
//!   `SELECT ''::"char" = attidentity ...` and `length(attidentity::text)` =
//!   `0` on an ordinary column — the "unset" `"char"` value round-trips to
//!   empty, not to a literal NUL byte, when compared or cast the way
//!   introspection queries do). This crate models that as `Option<char>`
//!   (`None` → empty string) on [`crate::catalog_source::ColumnInfo`] rather
//!   than a `'\0'` sentinel.
//! - `full_name GENERATED ALWAYS AS (...) STORED` reports `attgenerated =
//!   's'`; every other column reports the empty string, same rule as
//!   `attidentity`.
//! - Dropped columns keep their `pg_attribute` row rather than losing it:
//!   `ALTER TABLE attr_test DROP COLUMN created_at` leaves a row with
//!   `attisdropped = t`, `atttypid = 0`, `atttypmod = -1`, and a mangled name
//!   (`........pg.dropped.5........`). This crate reports `attisdropped` as
//!   given by [`crate::CatalogSource`] but does not attempt the name-mangling
//!   or `atttypid`-zeroing — out of scope for this increment.
//! - System columns (`tableoid`, `cmax`, `xmax`, `cmin`, `xmin`, `ctid`) have
//!   negative `attnum` (`-6` through `-1`) and are the same across every
//!   relation. This crate does not report them yet — [`crate::CatalogSource`]
//!   has no notion of them — which is why `attnum` being negative for system
//!   columns is a documented fact about real Postgres, not a case this
//!   relation's tests exercise.
//! - **Column order**, re-verified against PostgreSQL 18.2's real `attnum`
//!   sequence (`attrelid` 1, `attname` 2, `atttypid` 3, `attlen` 4, `attnum`
//!   5, `atttypmod` 6, `attnotnull` 12, `atthasdef` 13, `attidentity` 15,
//!   `attgenerated` 16, `attisdropped` 17): `attidentity`/`attgenerated`
//!   precede `attisdropped` in real Postgres. A prior version of this module
//!   had `attisdropped` before `attidentity`/`attgenerated` in the
//!   `arrow_schema()` field order — wrong relative to `attnum` — fixed by
//!   this audit; see `schema_matches_live_postgres_column_order_and_types`
//!   below, which pins it.
//!
//! # The rest of the 25 columns
//!
//! This relation reports all 25 real columns. The eleven above were the
//! original set; the other fourteen fall into three groups, all checked
//! against a live PostgreSQL 18.2 (`CREATE TABLE attrs (id int primary key,
//! name text, tags text[], m numeric(10,2), arr2 int[][])`, then `SELECT
//! attndims, attbyval, attalign, attstorage, attcompression, atthasmissing,
//! attislocal, attinhcount, attcollation, attstattarget, attacl, attoptions,
//! attfdwoptions, attmissingval FROM pg_attribute WHERE attrelid =
//! 'attrs'::regclass AND attnum > 0`):
//!
//! **Copied from `pg_type`, exactly as real Postgres does at `CREATE TABLE`
//! time** — `attlen` already was, and `attbyval` (8), `attalign` (9),
//! `attstorage` (10) and `attcollation` (20) now join it, all via
//! [`crate::pg_type::attribute_type_info`]. These are not defaults: they are
//! the column's own type's `typbyval`/`typalign`/`typstorage`/`typcollation`,
//! which [`crate::pg_type`] carries as live-verified per-type data. The probe
//! above confirms the copy: `name text` reports `attstorage = 'x'` and
//! `attcollation = 100` (text's `typstorage`/`typcollation`), while `m
//! numeric(10,2)` reports `'m'` and `0` (numeric's). A column whose
//! `atttypid` has no [`crate::pg_type`] row at all (a user-defined type —
//! out of scope for this increment) falls back to the same `0`/`false`/
//! `'i'`/`'p'`/`0` this relation already used for `attlen`.
//!
//! **Derived from the catalog** — `attndims` (7) is `1` for a column whose
//! type is an array type and `0` otherwise, which is what the probe reports
//! for `tags text[]` (1) versus `name text` (0). Basin's array types are
//! one-dimensional, so the `arr2 int[][]` case (real value `2`) is not
//! representable in [`crate::catalog_source::ColumnInfo`] and cannot arise. `attislocal` (18) and
//! `attinhcount` (19) are derived from [`CatalogSource::inheritance`]: a
//! column whose name also appears in one of the table's inheritance parents
//! is inherited (`attislocal = false`, `attinhcount` = the number of parents
//! declaring it), otherwise it is local (`true`, `0`) — the same name-based
//! merge real Postgres performs. The one case this cannot distinguish is a
//! column redeclared in *both* child and parent, where real Postgres reports
//! `attislocal = true` *and* `attinhcount = 1`;
//! [`crate::catalog_source::ColumnInfo`] has no field
//! recording that a column was written out in the child's own `CREATE TABLE`.
//!
//! **Features Basin does not have, reported the way a real server reports
//! their absence** — `attcompression` (11) is the *empty string*, which is
//! what the probe returns for every ordinary column (`length(attcompression::
//! text) = 0`), meaning "the default compression method"; Basin does not
//! TOAST-compress at all, so the default is the only honest answer, and it is
//! the same `"char"` unset convention `attidentity`/`attgenerated` already
//! use. `atthasmissing` (14) is `false` — it is only ever `true` after an
//! `ALTER TABLE ADD COLUMN ... DEFAULT` fast path, which Basin has no
//! equivalent of, and the probe reports `false` for every column.
//! `attstattarget` (21) is `NULL`, which since PostgreSQL 17 is how "use the
//! system default statistics target" is spelled (the probe confirms `NULL`,
//! not `-1`); Basin collects no column statistics, so it has no per-column
//! target to report. `attacl` (22), `attoptions` (23), `attfdwoptions` (24)
//! and `attmissingval` (25) are all `NULL`, which is what the probe reports
//! for an ordinary column: no privileges granted away from the default, no
//! per-attribute storage options, no foreign-data-wrapper options (Basin has
//! no FDWs), and no missing-value (which follows from `atthasmissing =
//! false`).
//!
//! `attmissingval` is Postgres's `anyarray`, a pseudo-type whose element type
//! is whatever the column's type is. Since every value here is `NULL`, no
//! element ever materializes; it is declared `List<Utf8>` to match this
//! crate's convention for array columns whose element type is not statically
//! known (the same one `pg_class.relacl` and `pg_proc.proacl` use).

use std::sync::Arc;

use arrow_array::{
    builder::{ListBuilder, StringBuilder},
    BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::CatalogSource,
    error::Error,
    pg_type,
    predicate::{Predicate, Value},
    Oid,
};

/// `pg_attribute.attcompression` for every column Basin reports — the empty
/// string, meaning "the default compression method". Basin does not
/// TOAST-compress; live-verified that an ordinary Postgres column reports the
/// empty `"char"` here too (see the module docs).
const ATTCOMPRESSION_DEFAULT: &str = "";

/// One row of `pg_attribute`, assembled from a table's
/// [`crate::catalog_source::ColumnInfo`], its
/// [`crate::catalog_source::ColumnDefaultInfo`]s (for `atthasdef`), the
/// column type's own [`crate::pg_type`] row (for the physical-representation
/// columns), and the table's inheritance parents (for `attislocal`/
/// `attinhcount`).
struct AttributeRow {
    attrelid: Oid,
    attname: String,
    atttypid: Oid,
    attlen: i16,
    attnum: i16,
    atttypmod: i32,
    attndims: i16,
    attbyval: bool,
    attalign: char,
    attstorage: char,
    attnotnull: bool,
    atthasdef: bool,
    attidentity: Option<char>,
    attgenerated: Option<char>,
    attisdropped: bool,
    attislocal: bool,
    attinhcount: i16,
    attcollation: Oid,
}

impl AttributeRow {
    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's scalar-`Value`-representable columns. The always-`NULL`
    /// columns (`attstattarget`, `attacl`, `attoptions`, `attfdwoptions`,
    /// `attmissingval`) have no scalar value and are handled by the caller.
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            "attrelid" => Value::Oid(self.attrelid),
            "attname" => Value::Text(self.attname.clone()),
            "atttypid" => Value::Oid(self.atttypid),
            "attlen" => Value::Int(self.attlen as i64),
            "attnum" => Value::Int(self.attnum as i64),
            "atttypmod" => Value::Int(self.atttypmod as i64),
            "attndims" => Value::Int(self.attndims as i64),
            "attbyval" => Value::Bool(self.attbyval),
            "attalign" => Value::Text(self.attalign.to_string()),
            "attstorage" => Value::Text(self.attstorage.to_string()),
            "attcompression" => Value::Text(ATTCOMPRESSION_DEFAULT.to_string()),
            "attnotnull" => Value::Bool(self.attnotnull),
            "atthasdef" => Value::Bool(self.atthasdef),
            // Only ever `true` after an `ALTER TABLE ADD COLUMN ... DEFAULT`
            // fast path, which Basin has no equivalent of — see module docs.
            "atthasmissing" => Value::Bool(false),
            // The "unset" value round-trips to the empty string, not a `'\0'`
            // byte — see the module docs.
            "attidentity" => {
                Value::Text(self.attidentity.map(|c| c.to_string()).unwrap_or_default())
            }
            "attgenerated" => {
                Value::Text(self.attgenerated.map(|c| c.to_string()).unwrap_or_default())
            }
            "attisdropped" => Value::Bool(self.attisdropped),
            "attislocal" => Value::Bool(self.attislocal),
            "attinhcount" => Value::Int(self.attinhcount as i64),
            "attcollation" => Value::Oid(self.attcollation),
            _ => return None,
        })
    }
}

/// `pg_catalog.pg_attribute`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgAttribute;

impl PgAttribute {
    fn arrow_schema() -> SchemaRef {
        let list_of_utf8 = || DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        Arc::new(Schema::new(vec![
            Field::new("attrelid", DataType::UInt32, false),
            Field::new("attname", DataType::Utf8, false),
            Field::new("atttypid", DataType::UInt32, false),
            Field::new("attlen", DataType::Int16, false),
            Field::new("attnum", DataType::Int16, false),
            Field::new("atttypmod", DataType::Int32, false),
            Field::new("attndims", DataType::Int16, false),
            Field::new("attbyval", DataType::Boolean, false),
            Field::new("attalign", DataType::Utf8, false),
            Field::new("attstorage", DataType::Utf8, false),
            Field::new("attcompression", DataType::Utf8, false),
            Field::new("attnotnull", DataType::Boolean, false),
            Field::new("atthasdef", DataType::Boolean, false),
            Field::new("atthasmissing", DataType::Boolean, false),
            Field::new("attidentity", DataType::Utf8, false),
            Field::new("attgenerated", DataType::Utf8, false),
            Field::new("attisdropped", DataType::Boolean, false),
            Field::new("attislocal", DataType::Boolean, false),
            Field::new("attinhcount", DataType::Int16, false),
            Field::new("attcollation", DataType::UInt32, false),
            // Nullable, and always `NULL` — see the module docs for what each
            // absence means and the live probe that confirms a real server
            // reports the same for an ordinary column.
            Field::new("attstattarget", DataType::Int16, true),
            Field::new("attacl", list_of_utf8(), true),
            Field::new("attoptions", list_of_utf8(), true),
            Field::new("attfdwoptions", list_of_utf8(), true),
            Field::new("attmissingval", list_of_utf8(), true),
        ]))
    }
}

impl crate::SystemView for PgAttribute {
    fn name(&self) -> &str {
        "pg_attribute"
    }

    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }

    fn scan(
        &self,
        catalog: &dyn CatalogSource,
        pushed: &[Predicate],
    ) -> Result<RecordBatch, Error> {
        let schema = Self::arrow_schema();
        for p in pushed {
            if !schema.fields().iter().any(|f| f.name() == p.column()) {
                return Err(Error::UnknownColumn {
                    relation: "pg_attribute",
                    column: p.column().to_string(),
                });
            }
        }

        let mut rows: Vec<AttributeRow> = Vec::new();
        for table in catalog.tables() {
            let defaults = catalog.column_defaults(table.oid);
            // Every column name declared by one of this table's inheritance
            // parents, with how many parents declare it — real Postgres's own
            // name-based merge, and the only thing `CatalogSource` can tell us
            // about inherited columns (see the module docs).
            let mut inherited_names: std::collections::HashMap<String, i16> =
                std::collections::HashMap::new();
            for parent in catalog.inheritance(table.oid) {
                for parent_col in catalog.columns(parent.parent_oid) {
                    *inherited_names.entry(parent_col.name).or_insert(0) += 1;
                }
            }
            for col in catalog.columns(table.oid) {
                let atthasdef = defaults.iter().any(|d| d.attnum == col.attnum);
                // Real Postgres copies these five off the column's type's
                // `pg_type` row at column-creation time — see the module docs.
                // The fallback applies only when `type_oid` names a type this
                // crate's `pg_type` has no row for; `attlen = 0` is not a real
                // `typlen` value, which is what makes that case visible.
                let ty = pg_type::attribute_type_info(col.type_oid);
                let attinhcount = inherited_names.get(&col.name).copied().unwrap_or(0);
                rows.push(AttributeRow {
                    attrelid: table.oid,
                    attname: col.name,
                    atttypid: col.type_oid,
                    attlen: ty.map(|t| t.attlen).unwrap_or(0),
                    attnum: col.attnum,
                    atttypmod: col.atttypmod,
                    attndims: i16::from(ty.is_some_and(|t| t.is_array)),
                    attbyval: ty.is_some_and(|t| t.attbyval),
                    attalign: ty.map(|t| t.attalign).unwrap_or('i'),
                    attstorage: ty.map(|t| t.attstorage).unwrap_or('p'),
                    attnotnull: col.not_null,
                    atthasdef,
                    attidentity: col.attidentity,
                    attgenerated: col.attgenerated,
                    attisdropped: col.attisdropped,
                    attislocal: attinhcount == 0,
                    attinhcount,
                    attcollation: ty.map(|t| t.attcollation).unwrap_or(Oid::INVALID),
                });
            }
        }

        let rows: Vec<AttributeRow> = rows
            .into_iter()
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(r.value(p.column()).as_ref()))
            })
            .collect();

        let n = rows.len();
        let attrelids: UInt32Array = rows.iter().map(|r| r.attrelid.get()).collect();
        let attnames: StringArray = rows.iter().map(|r| Some(r.attname.as_str())).collect();
        let atttypids: UInt32Array = rows.iter().map(|r| r.atttypid.get()).collect();
        let attlens: Int16Array = rows.iter().map(|r| r.attlen).collect();
        let attnums: Int16Array = rows.iter().map(|r| r.attnum).collect();
        let atttypmods: Int32Array = rows.iter().map(|r| r.atttypmod).collect();
        let attndimss: Int16Array = rows.iter().map(|r| r.attndims).collect();
        let attbyvals: BooleanArray = rows.iter().map(|r| r.attbyval).collect();
        let attaligns: StringArray = rows.iter().map(|r| Some(r.attalign.to_string())).collect();
        let attstorages: StringArray = rows
            .iter()
            .map(|r| Some(r.attstorage.to_string()))
            .collect();
        let attcompressions: StringArray =
            rows.iter().map(|_| Some(ATTCOMPRESSION_DEFAULT)).collect();
        let attnotnulls: BooleanArray = rows.iter().map(|r| r.attnotnull).collect();
        let atthasdefs: BooleanArray = rows.iter().map(|r| r.atthasdef).collect();
        let atthasmissings: BooleanArray = rows.iter().map(|_| false).collect();
        let attidentities: StringArray = rows
            .iter()
            .map(|r| Some(r.attidentity.map(|c| c.to_string()).unwrap_or_default()))
            .collect();
        let attgenerateds: StringArray = rows
            .iter()
            .map(|r| Some(r.attgenerated.map(|c| c.to_string()).unwrap_or_default()))
            .collect();
        let attisdroppeds: BooleanArray = rows.iter().map(|r| r.attisdropped).collect();
        let attislocals: BooleanArray = rows.iter().map(|r| r.attislocal).collect();
        let attinhcounts: Int16Array = rows.iter().map(|r| r.attinhcount).collect();
        let attcollations: UInt32Array = rows.iter().map(|r| r.attcollation.get()).collect();
        // The five always-`NULL` columns — see the module docs.
        let attstattargets = Int16Array::from(vec![None::<i16>; n]);
        let null_list = || -> ListArray {
            let mut b = ListBuilder::new(StringBuilder::new());
            for _ in 0..n {
                b.append(false);
            }
            b.finish()
        };

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(attrelids),
                Arc::new(attnames),
                Arc::new(atttypids),
                Arc::new(attlens),
                Arc::new(attnums),
                Arc::new(atttypmods),
                Arc::new(attndimss),
                Arc::new(attbyvals),
                Arc::new(attaligns),
                Arc::new(attstorages),
                Arc::new(attcompressions),
                Arc::new(attnotnulls),
                Arc::new(atthasdefs),
                Arc::new(atthasmissings),
                Arc::new(attidentities),
                Arc::new(attgenerateds),
                Arc::new(attisdroppeds),
                Arc::new(attislocals),
                Arc::new(attinhcounts),
                Arc::new(attcollations),
                Arc::new(attstattargets),
                Arc::new(null_list()),
                Arc::new(null_list()),
                Arc::new(null_list()),
                Arc::new(null_list()),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int16Array, Int32Array, StringArray, UInt32Array};
    use basin_pgtype::typmod;

    use super::*;
    use crate::{
        catalog_source::{ColumnInfo, RelKind, TableInfo},
        mock::MockCatalog,
        SystemView,
    };

    fn col_u32(batch: &RecordBatch, name: &str) -> Vec<u32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_i16(batch: &RecordBatch, name: &str) -> Vec<i16> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_i32(batch: &RecordBatch, name: &str) -> Vec<i32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn col_bool(batch: &RecordBatch, name: &str) -> Vec<bool> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap()
            .iter()
            .map(|b| b.unwrap())
            .collect()
    }

    fn col_str(batch: &RecordBatch, name: &str) -> Vec<String> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect()
    }

    /// A two-table catalog: `widgets` (a `varchar(10)` column with a default
    /// and a `NOT NULL` column) and `gadgets` (one plain, nullable column) —
    /// enough to prove `attrelid` pushdown actually scopes to one table.
    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_table(TableInfo {
                oid: Oid(16385),
                name: "widgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_table(TableInfo {
                oid: Oid(16390),
                name: "gadgets".to_string(),
                namespace: Oid(16384),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_column(ColumnInfo {
                table_oid: Oid(16385),
                name: "id".to_string(),
                attnum: 1,
                type_oid: Oid(23), // int4
                not_null: true,
                atttypmod: typmod::UNSPECIFIED,
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_column(ColumnInfo {
                table_oid: Oid(16385),
                name: "name".to_string(),
                attnum: 2,
                type_oid: Oid(1043), // varchar
                not_null: true,
                atttypmod: typmod::encode_varlena_len(10), // varchar(10)
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_column(ColumnInfo {
                table_oid: Oid(16385),
                name: "bio".to_string(),
                attnum: 3,
                type_oid: Oid(25), // text, no modifier
                not_null: false,
                atttypmod: typmod::UNSPECIFIED,
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_column(ColumnInfo {
                table_oid: Oid(16390),
                name: "widget_id".to_string(),
                attnum: 1,
                type_oid: Oid(23),
                not_null: true,
                atttypmod: typmod::UNSPECIFIED,
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_default(crate::catalog_source::ColumnDefaultInfo {
                oid: Oid(20001),
                table_oid: Oid(16385),
                attnum: 2,
                expr_src: "'anon'::character varying".to_string(),
            })
    }

    /// The entire point of this crate, applied to `pg_attribute`: a predicate
    /// on `attrelid` must narrow to exactly one table's columns.
    #[test]
    fn pushed_attrelid_predicate_narrows_to_one_tables_columns() {
        let full = PgAttribute.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 4, "sanity: two tables, four columns total");

        let filtered = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attrelid", Oid(16385))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 3, "widgets has three columns");
        for relid in col_u32(&filtered, "attrelid") {
            assert_eq!(relid, 16385);
        }

        let gadgets = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attrelid", Oid(16390))])
            .unwrap();
        assert_eq!(gadgets.num_rows(), 1, "gadgets has one column");
        assert_eq!(col_str(&gadgets, "attname"), vec!["widget_id".to_string()]);
    }

    /// `varchar(10)` must report `atttypmod = 14` (`10 + VARHDRSZ`), not `10`
    /// and not `-1` — confirmed live, see the module docs.
    #[test]
    fn varchar_10_reports_atttypmod_14() {
        let batch = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attname", "name")])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_i32(&batch, "atttypmod"), vec![14]);
    }

    /// A column with no type modifier reports `atttypmod = -1`.
    #[test]
    fn column_with_no_modifier_reports_negative_one() {
        let batch = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attname", "bio")])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_i32(&batch, "atttypmod"), vec![-1]);
    }

    /// `attnotnull` and `atthasdef` reflect exactly what the catalog source
    /// gave: `id` is `NOT NULL` with no default, `name` is `NOT NULL` with a
    /// default, `bio` is nullable with no default.
    #[test]
    fn attnotnull_and_atthasdef_reflect_the_source() {
        let batch = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attrelid", Oid(16385))])
            .unwrap();

        let by_name: std::collections::HashMap<String, (bool, bool)> = col_str(&batch, "attname")
            .into_iter()
            .zip(
                col_bool(&batch, "attnotnull")
                    .into_iter()
                    .zip(col_bool(&batch, "atthasdef")),
            )
            .collect();

        assert_eq!(by_name["id"], (true, false));
        assert_eq!(by_name["name"], (true, true));
        assert_eq!(by_name["bio"], (false, false));
    }

    /// `attnum` is 1-based for real columns.
    #[test]
    fn attnum_is_one_based() {
        let batch = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attrelid", Oid(16385))])
            .unwrap();
        let mut nums = col_i16(&batch, "attnum");
        nums.sort_unstable();
        assert_eq!(nums, vec![1, 2, 3]);
    }

    /// `attidentity`/`attgenerated` report the empty string when unset — not
    /// a `'\0'` byte — matching what real Postgres's `"char"` unset value
    /// round-trips to (see the module docs).
    #[test]
    fn identity_and_generated_default_to_empty_string() {
        let batch = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attname", "id")])
            .unwrap();
        assert_eq!(col_str(&batch, "attidentity"), vec!["".to_string()]);
        assert_eq!(col_str(&batch, "attgenerated"), vec!["".to_string()]);
    }

    /// A predicate naming a column `pg_attribute` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgAttribute
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_attribute",
                column: "nope".to_string(),
            }
        );
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgAttribute.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn name_is_pg_attribute() {
        assert_eq!(PgAttribute.name(), "pg_attribute");
    }

    /// Pins the exact column set, order, and Arrow type of `pg_attribute`
    /// against live PostgreSQL 18.2's `attnum` order, so a future edit
    /// cannot silently reorder or rename a column out from under positional
    /// readers (psql's `\d`, `pg_dump`, and ORM introspection all read this
    /// positionally as well as by name). Verified live via:
    ///
    /// ```sql
    /// SELECT attname, atttypid::regtype, attnum, attnotnull
    ///   FROM pg_attribute
    ///  WHERE attrelid = 'pg_catalog.pg_attribute'::regclass AND attnum > 0
    ///  ORDER BY attnum;
    /// ```
    ///
    /// which reports `attidentity` (15) and `attgenerated` (16) *before*
    /// `attisdropped` (17) — the order this crate had backwards until this
    /// audit — and all **25** columns in exactly this order. This relation
    /// implements every one of them, so position here is faithful as well as
    /// name.
    #[test]
    fn schema_matches_live_postgres_column_order_and_types() {
        let list_of_utf8 = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let schema = PgAttribute.schema();
        let got: Vec<(&str, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("attrelid", DataType::UInt32, false),
                ("attname", DataType::Utf8, false),
                ("atttypid", DataType::UInt32, false),
                ("attlen", DataType::Int16, false),
                ("attnum", DataType::Int16, false),
                ("atttypmod", DataType::Int32, false),
                ("attndims", DataType::Int16, false),
                ("attbyval", DataType::Boolean, false),
                ("attalign", DataType::Utf8, false),
                ("attstorage", DataType::Utf8, false),
                ("attcompression", DataType::Utf8, false),
                ("attnotnull", DataType::Boolean, false),
                ("atthasdef", DataType::Boolean, false),
                ("atthasmissing", DataType::Boolean, false),
                ("attidentity", DataType::Utf8, false),
                ("attgenerated", DataType::Utf8, false),
                ("attisdropped", DataType::Boolean, false),
                ("attislocal", DataType::Boolean, false),
                ("attinhcount", DataType::Int16, false),
                ("attcollation", DataType::UInt32, false),
                ("attstattarget", DataType::Int16, true),
                ("attacl", list_of_utf8.clone(), true),
                ("attoptions", list_of_utf8.clone(), true),
                ("attfdwoptions", list_of_utf8.clone(), true),
                ("attmissingval", list_of_utf8, true),
            ]
        );
    }

    /// `attbyval`/`attalign`/`attstorage`/`attcollation` are copies of the
    /// column type's own `pg_type` row, exactly as real Postgres makes them
    /// at `CREATE TABLE` time — live-verified per the module docs (`text`
    /// gives `'x'`/`100`, `int4` gives `'i'`/`0`).
    #[test]
    fn physical_columns_are_copied_from_the_columns_type() {
        let batch = PgAttribute
            .scan(&catalog(), &[Predicate::eq("attrelid", Oid(16385))])
            .unwrap();

        let idx = |name: &str| {
            col_str(&batch, "attname")
                .iter()
                .position(|n| n == name)
                .unwrap()
        };

        let id = idx("id"); // int4
        assert!(col_bool(&batch, "attbyval")[id], "int4 is passed by value");
        assert_eq!(col_str(&batch, "attalign")[id], "i");
        assert_eq!(col_str(&batch, "attstorage")[id], "p");
        assert_eq!(col_u32(&batch, "attcollation")[id], 0);
        assert_eq!(col_i16(&batch, "attndims")[id], 0);

        let bio = idx("bio"); // text
        assert!(!col_bool(&batch, "attbyval")[bio]);
        assert_eq!(col_str(&batch, "attalign")[bio], "i");
        assert_eq!(col_str(&batch, "attstorage")[bio], "x");
        assert_eq!(
            col_u32(&batch, "attcollation")[bio],
            100,
            "text is collatable"
        );
    }

    /// An array-typed column reports `attndims = 1`; a scalar one reports
    /// `0`. Live-verified — see the module docs' probe.
    #[test]
    fn array_typed_column_reports_one_dimension() {
        let catalog = catalog().with_column(ColumnInfo {
            table_oid: Oid(16390),
            name: "tags".to_string(),
            attnum: 2,
            type_oid: Oid(1009), // _text
            not_null: false,
            atttypmod: typmod::UNSPECIFIED,
            attisdropped: false,
            attidentity: None,
            attgenerated: None,
        });
        let batch = PgAttribute
            .scan(&catalog, &[Predicate::eq("attname", "tags")])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_i16(&batch, "attndims"), vec![1]);
        assert_eq!(
            col_u32(&batch, "attcollation"),
            vec![100],
            "_text inherits text's collation"
        );
    }

    /// `attislocal`/`attinhcount` are derived from the table's inheritance
    /// parents by column name, the same merge real Postgres performs: a
    /// column the parent also declares is inherited, one only the child
    /// declares is local. See the module docs.
    #[test]
    fn inherited_columns_are_not_local() {
        use crate::catalog_source::InheritanceInfo;

        let catalog = catalog().with_inheritance(InheritanceInfo {
            child_oid: Oid(16390),
            parent_oid: Oid(16385),
            seqno: 1,
        });

        // `gadgets`.`widget_id` is not a `widgets` column name, so it stays
        // local even though `gadgets` now inherits from `widgets`.
        let gadgets = PgAttribute
            .scan(&catalog, &[Predicate::eq("attrelid", Oid(16390))])
            .unwrap();
        assert_eq!(col_bool(&gadgets, "attislocal"), vec![true]);
        assert_eq!(col_i16(&gadgets, "attinhcount"), vec![0]);

        // A child column sharing the parent's name is reported inherited.
        let catalog = catalog.with_column(ColumnInfo {
            table_oid: Oid(16390),
            name: "bio".to_string(),
            attnum: 2,
            type_oid: Oid(25),
            not_null: false,
            atttypmod: typmod::UNSPECIFIED,
            attisdropped: false,
            attidentity: None,
            attgenerated: None,
        });
        let batch = PgAttribute
            .scan(
                &catalog,
                &[
                    Predicate::eq("attrelid", Oid(16390)),
                    Predicate::eq("attname", "bio"),
                ],
            )
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_bool(&batch, "attislocal"), vec![false]);
        assert_eq!(col_i16(&batch, "attinhcount"), vec![1]);
    }

    /// The five nullable columns are `NULL` for every row — see the module
    /// docs for why `NULL`, not a stand-in, is the correct value for each.
    #[test]
    fn the_five_nullable_columns_are_null_for_every_row() {
        let batch = PgAttribute.scan(&catalog(), &[]).unwrap();
        assert!(batch.num_rows() > 0);
        for name in [
            "attstattarget",
            "attacl",
            "attoptions",
            "attfdwoptions",
            "attmissingval",
        ] {
            let c = batch.column(batch.schema().index_of(name).unwrap());
            assert_eq!(c.null_count(), c.len(), "{name} must be NULL everywhere");
        }
    }

    /// `attcompression` is the empty string (the default method), matching
    /// the `"char"` unset convention `attidentity`/`attgenerated` use and
    /// what a live server reports for an ordinary column.
    #[test]
    fn attcompression_is_the_empty_default() {
        let batch = PgAttribute.scan(&catalog(), &[]).unwrap();
        assert!(col_str(&batch, "attcompression")
            .iter()
            .all(|s| s.is_empty()));
    }
}
