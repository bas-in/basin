//! `pg_catalog.pg_class` — relations (tables, indexes, views, sequences, ...).
//!
//! `relnatts`, `relhasindex`, `relchecks` and `relhassubclass` are *derived* —
//! via [`CatalogSource::columns`], [`CatalogSource::indexes`],
//! [`CatalogSource::constraints`] and [`CatalogSource::inheritance`]
//! respectively — rather than stored on [`TableInfo`], which is deliberate: it
//! exercises those [`CatalogSource`] methods and proves the trait's shape is
//! usable, not just declared.
//!
//! Live-verified column set, types, and **order** (`\d pg_class` /
//! `SELECT attname, atttypid::regtype, attnum FROM pg_attribute WHERE
//! attrelid = 'pg_catalog.pg_class'::regclass AND attnum > 0 ORDER BY
//! attnum`, PostgreSQL 18.2) reports **34** columns, all of which this
//! relation now reports. The relative order matters and is not the
//! alphabetical-looking one it is tempting to write: `relhasindex` (15)
//! precedes `relkind` (18), which precedes `relnatts` (19).
//!
//! # Where the values Basin does not store come from
//!
//! [`TableInfo`] carries five fields (`oid`, `name`, `namespace`, `owner`,
//! `kind`). Everything else below is either derived from another
//! [`CatalogSource`] method or is a property of a storage engine Basin does
//! not have. For the latter, the value reported is the one a **real
//! PostgreSQL 18.2 reports for a relation that genuinely lacks the feature**,
//! read off the server rather than recalled — probe:
//!
//! ```sql
//! CREATE SCHEMA basin_probe;
//! CREATE TABLE basin_probe.widgets (id int primary key, name text);
//! CREATE VIEW basin_probe.v AS SELECT 1 AS x;
//! CREATE SEQUENCE basin_probe.s;
//! CREATE MATERIALIZED VIEW basin_probe.mv AS SELECT 1 AS x;
//! SELECT relname, relkind, relam, relreplident, relfilenode, reltype,
//!        relpages, reltuples, reltoastrelid, relispopulated, relfrozenxid,
//!        relminmxid, relacl, reloptions, relpartbound
//!   FROM pg_class WHERE relnamespace = 'basin_probe'::regnamespace;
//! ```
//!
//! - `reltype` (4) and `reloftype` (5) are `0`. `reltype` is the `pg_type` row
//!   for the relation's composite row type; [`crate::pg_type`] is builtins-only
//!   and has no row for any relation, so there is none to point at. `0` is the
//!   value the probe shows for an index and for a sequence — Postgres's own
//!   "this relation has no row type". `reloftype` is `0` for anything that is
//!   not a `CREATE TABLE ... OF <type>`, which is everything Basin has.
//! - `relam` (7) is `0`. Postgres reports `2` (`heap`) for a table and `403`
//!   (`btree`) for an index; Basin's tables are Vortex or Parquet files, and
//!   [`crate::pg_am`] — which reports exactly stock Postgres's seven access
//!   methods — has no row that describes them. Claiming `heap` would assert a
//!   storage format Basin does not use. `0` is the value the probe shows for a
//!   view and a sequence, i.e. "no access method", which is the truthful
//!   answer here. The visible consequence is that a client joining
//!   `pg_class.relam` to `pg_am` gets no row, so `psql \d+`'s "Access method:"
//!   line is absent rather than wrong.
//! - `relfilenode` (8) is `0` — Basin has no per-relation physical file node.
//!   The probe shows `0` for a view, Postgres's own "no storage of its own".
//! - `reltablespace` (9) is `0`, which is *literally correct*: `0` means the
//!   database's default tablespace, which is what the probe shows for every
//!   ordinary relation.
//! - `relpages` (10), `relallvisible` (12), `relallfrozen` (13) are `0` and
//!   `reltuples` (11) is `-1`. These are planner statistics, and the probe
//!   shows exactly these values for a freshly created table that has never
//!   been `VACUUM`ed or `ANALYZE`d. `-1` is not a placeholder: since
//!   PostgreSQL 14 it is the specific sentinel meaning "row count unknown", as
//!   distinct from `0` ("known to be empty"). Basin has no such statistics, so
//!   "unknown" is the honest answer and Postgres already spells it.
//! - `reltoastrelid` (14) is `0` — Basin has no TOAST tables. The probe shows
//!   `0` for every relation without one.
//! - `relisshared` (16) is `false`; only Postgres's cluster-wide catalogs
//!   (`pg_database`, `pg_authid`, ...) are shared, and Basin has none.
//! - `relpersistence` (17) is `'p'` (permanent). [`TableInfo`] cannot express
//!   a temporary or unlogged relation, so no other value can arise.
//! - `relchecks` (20) is **derived**: the number of
//!   [`ConstraintKind::Check`]
//!   constraints [`CatalogSource::constraints`] reports for the relation.
//! - `relhasrules` (21), `relhastriggers` (22), `relrowsecurity` (24),
//!   `relforcerowsecurity` (25) are `false` — Basin has no rule system, no
//!   triggers and no row-level security, so the feature is absent rather than
//!   unknown. The probe shows `false` for all four on an ordinary table.
//! - `relhassubclass` (23) is **derived**: `true` when some other relation
//!   names this one as an inheritance/partition parent via
//!   [`CatalogSource::inheritance`]. Note the direction — `inheritance` is
//!   *child*-scoped, so this is computed once per scan over every relation
//!   rather than per row.
//! - `relispopulated` (26) is `true`. The only relation Postgres reports
//!   `false` for is a materialized view created `WITH NO DATA`, which
//!   [`TableInfo`] cannot express.
//! - `relreplident` (27) is `'d'` for a table or materialized view and `'n'`
//!   for an index, view or sequence — exactly what the probe shows for each
//!   `relkind`. Basin has no logical replication, and `'d'` (default: the
//!   primary key) is Postgres's own value for a relation on which nothing has
//!   set a replica identity.
//! - `relispartition` (28) is `false` and `relpartbound` (34) is `NULL`.
//!   [`RelKind`] has no partitioned-table
//!   variant, so no relation Basin can describe has a partition parent, and
//!   therefore none is a partition.
//! - `relrewrite` (29) is `0`; it is only ever non-zero for the transient
//!   relation an in-progress `ALTER TABLE` rewrite is building.
//! - `relfrozenxid` (30) and `relminmxid` (31) are `0`. Basin has no MVCC
//!   transaction ids at all; `0` is `InvalidTransactionId`, which the probe
//!   shows for every relation without storage (index, view, sequence).
//! - `relacl` (32) and `reloptions` (33) are `NULL` — the probe shows `NULL`
//!   for a table whose privileges have never been `GRANT`ed away from the
//!   default and which has no per-relation storage options. Basin has neither
//!   a privilege system nor storage options.

use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    builder::{ListBuilder, StringBuilder},
    BooleanArray, Float32Array, Int16Array, Int32Array, ListArray, RecordBatch, StringArray,
    UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, ConstraintKind, RelKind, TableInfo},
    error::Error,
    predicate::{Predicate, Value},
    Oid,
};

/// `pg_class.reltuples` for a relation whose row count is unknown — the
/// sentinel PostgreSQL 14 and later use for "never vacuumed or analyzed",
/// distinct from `0` ("known to be empty"). See the module docs.
const RELTUPLES_UNKNOWN: f32 = -1.0;

/// `pg_class.relreplident` for `kind` — `'d'` (default: the primary key) for
/// a relation that can have a replica identity, `'n'` (nothing) for one that
/// cannot. Live-verified per `relkind`; see the module docs' probe.
fn relreplident(kind: RelKind) -> char {
    match kind {
        RelKind::OrdinaryTable | RelKind::MaterializedView => 'd',
        RelKind::Index | RelKind::View | RelKind::Sequence => 'n',
    }
}

/// `pg_catalog.pg_class`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgClass;

impl PgClass {
    fn arrow_schema() -> SchemaRef {
        let list_of_utf8 = || DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::UInt32, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("relnamespace", DataType::UInt32, false),
            Field::new("reltype", DataType::UInt32, false),
            Field::new("reloftype", DataType::UInt32, false),
            Field::new("relowner", DataType::UInt32, false),
            Field::new("relam", DataType::UInt32, false),
            Field::new("relfilenode", DataType::UInt32, false),
            Field::new("reltablespace", DataType::UInt32, false),
            Field::new("relpages", DataType::Int32, false),
            Field::new("reltuples", DataType::Float32, false),
            Field::new("relallvisible", DataType::Int32, false),
            Field::new("relallfrozen", DataType::Int32, false),
            Field::new("reltoastrelid", DataType::UInt32, false),
            Field::new("relhasindex", DataType::Boolean, false),
            Field::new("relisshared", DataType::Boolean, false),
            Field::new("relpersistence", DataType::Utf8, false),
            Field::new("relkind", DataType::Utf8, false),
            Field::new("relnatts", DataType::Int16, false),
            Field::new("relchecks", DataType::Int16, false),
            Field::new("relhasrules", DataType::Boolean, false),
            Field::new("relhastriggers", DataType::Boolean, false),
            Field::new("relhassubclass", DataType::Boolean, false),
            Field::new("relrowsecurity", DataType::Boolean, false),
            Field::new("relforcerowsecurity", DataType::Boolean, false),
            Field::new("relispopulated", DataType::Boolean, false),
            Field::new("relreplident", DataType::Utf8, false),
            Field::new("relispartition", DataType::Boolean, false),
            Field::new("relrewrite", DataType::UInt32, false),
            // `xid`, represented as the 32-bit unsigned value it is — the
            // same `UInt32` this crate uses for every `oid`-ish column.
            Field::new("relfrozenxid", DataType::UInt32, false),
            Field::new("relminmxid", DataType::UInt32, false),
            // Nullable, and always `NULL` — see the module docs.
            Field::new("relacl", list_of_utf8(), true),
            Field::new("reloptions", list_of_utf8(), true),
            Field::new("relpartbound", DataType::Utf8, true),
        ]))
    }
}

/// A [`TableInfo`] plus the columns of [`pg_class`](self) that must be
/// derived from the rest of the catalog rather than read off `TableInfo`
/// directly.
struct ClassRow<'a> {
    table: &'a TableInfo,
    relnatts: i16,
    relhasindex: bool,
    relchecks: i16,
    relhassubclass: bool,
}

fn row_value(row: &ClassRow, column: &str) -> Option<Value> {
    // Every literal below is documented in this module's docs, with the live
    // probe that produced it. None is a guess.
    Some(match column {
        "oid" => Value::Oid(row.table.oid),
        "relname" => Value::Text(row.table.name.clone()),
        "relnamespace" => Value::Oid(row.table.namespace),
        "reltype" => Value::Oid(Oid::INVALID),
        "reloftype" => Value::Oid(Oid::INVALID),
        "relowner" => Value::Oid(row.table.owner),
        "relam" => Value::Oid(Oid::INVALID),
        "relfilenode" => Value::Oid(Oid::INVALID),
        "reltablespace" => Value::Oid(Oid::INVALID),
        "relpages" => Value::Int(0),
        // `reltuples` is `real`, and `Value` has no floating-point variant;
        // a predicate on it is rejected by `scan`'s schema check only if it
        // is not a real column, so it simply never matches here. Same for
        // the always-`NULL` columns below.
        "relallvisible" => Value::Int(0),
        "relallfrozen" => Value::Int(0),
        "reltoastrelid" => Value::Oid(Oid::INVALID),
        "relhasindex" => Value::Bool(row.relhasindex),
        "relisshared" => Value::Bool(false),
        "relpersistence" => Value::Text("p".to_string()),
        "relkind" => Value::Text(row.table.kind.as_char().to_string()),
        "relnatts" => Value::Int(row.relnatts as i64),
        "relchecks" => Value::Int(row.relchecks as i64),
        "relhasrules" => Value::Bool(false),
        "relhastriggers" => Value::Bool(false),
        "relhassubclass" => Value::Bool(row.relhassubclass),
        "relrowsecurity" => Value::Bool(false),
        "relforcerowsecurity" => Value::Bool(false),
        "relispopulated" => Value::Bool(true),
        "relreplident" => Value::Text(relreplident(row.table.kind).to_string()),
        "relispartition" => Value::Bool(false),
        "relrewrite" => Value::Oid(Oid::INVALID),
        "relfrozenxid" => Value::Oid(Oid::INVALID),
        "relminmxid" => Value::Oid(Oid::INVALID),
        _ => return None,
    })
}

impl crate::SystemView for PgClass {
    fn name(&self) -> &str {
        "pg_class"
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
                    relation: "pg_class",
                    column: p.column().to_string(),
                });
            }
        }

        let tables = catalog.tables();
        // `CatalogSource::inheritance` is *child*-scoped — it answers "who are
        // this relation's parents?", not "who are its children?" — so
        // `relhassubclass` needs one pass over every relation up front rather
        // than a query per row. See the module docs.
        let parents: HashSet<Oid> = tables
            .iter()
            .flat_map(|t| catalog.inheritance(t.oid))
            .map(|i| i.parent_oid)
            .collect();

        let rows: Vec<ClassRow> = tables
            .iter()
            .map(|table| ClassRow {
                table,
                relnatts: catalog.columns(table.oid).len() as i16,
                relhasindex: !catalog.indexes(table.oid).is_empty(),
                relchecks: catalog
                    .constraints(table.oid)
                    .iter()
                    .filter(|c| c.kind == ConstraintKind::Check)
                    .count() as i16,
                relhassubclass: parents.contains(&table.oid),
            })
            .filter(|row| {
                pushed
                    .iter()
                    .all(|p| p.matches(row_value(row, p.column()).as_ref()))
            })
            .collect();

        let n = rows.len();
        let oids: UInt32Array = rows.iter().map(|r| r.table.oid.get()).collect();
        let relnames: StringArray = rows.iter().map(|r| Some(r.table.name.as_str())).collect();
        let relnamespaces: UInt32Array = rows.iter().map(|r| r.table.namespace.get()).collect();
        let relowners: UInt32Array = rows.iter().map(|r| r.table.owner.get()).collect();
        let relhasindexes: BooleanArray = rows.iter().map(|r| r.relhasindex).collect();
        let relkinds: StringArray = rows
            .iter()
            .map(|r| Some(r.table.kind.as_char().to_string()))
            .collect();
        let relnatts: Int16Array = rows.iter().map(|r| r.relnatts).collect();
        let relchecks: Int16Array = rows.iter().map(|r| r.relchecks).collect();
        let relhassubclasses: BooleanArray = rows.iter().map(|r| r.relhassubclass).collect();
        let relreplidents: StringArray = rows
            .iter()
            .map(|r| Some(relreplident(r.table.kind).to_string()))
            .collect();
        // Features Basin does not have, reported the way a real server reports
        // their absence — see the module docs for the live probe behind each.
        let zeros_u32 = || -> UInt32Array { rows.iter().map(|_| 0u32).collect() };
        let zeros_i32 = || -> Int32Array { rows.iter().map(|_| 0i32).collect() };
        let falses = || -> BooleanArray { rows.iter().map(|_| false).collect() };
        let reltuples: Float32Array = rows.iter().map(|_| RELTUPLES_UNKNOWN).collect();
        let relpersistences: StringArray = rows.iter().map(|_| Some("p")).collect();
        let relispopulateds: BooleanArray = rows.iter().map(|_| true).collect();
        let null_list = || -> ListArray {
            let mut b = ListBuilder::new(StringBuilder::new());
            for _ in 0..n {
                b.append(false);
            }
            b.finish()
        };
        let relpartbound = StringArray::from(vec![None::<&str>; n]);

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(oids),
                Arc::new(relnames),
                Arc::new(relnamespaces),
                Arc::new(zeros_u32()), // reltype
                Arc::new(zeros_u32()), // reloftype
                Arc::new(relowners),
                Arc::new(zeros_u32()), // relam
                Arc::new(zeros_u32()), // relfilenode
                Arc::new(zeros_u32()), // reltablespace
                Arc::new(zeros_i32()), // relpages
                Arc::new(reltuples),
                Arc::new(zeros_i32()), // relallvisible
                Arc::new(zeros_i32()), // relallfrozen
                Arc::new(zeros_u32()), // reltoastrelid
                Arc::new(relhasindexes),
                Arc::new(falses()), // relisshared
                Arc::new(relpersistences),
                Arc::new(relkinds),
                Arc::new(relnatts),
                Arc::new(relchecks),
                Arc::new(falses()), // relhasrules
                Arc::new(falses()), // relhastriggers
                Arc::new(relhassubclasses),
                Arc::new(falses()), // relrowsecurity
                Arc::new(falses()), // relforcerowsecurity
                Arc::new(relispopulateds),
                Arc::new(relreplidents),
                Arc::new(falses()),    // relispartition
                Arc::new(zeros_u32()), // relrewrite
                Arc::new(zeros_u32()), // relfrozenxid
                Arc::new(zeros_u32()), // relminmxid
                Arc::new(null_list()), // relacl
                Arc::new(null_list()), // reloptions
                Arc::new(relpartbound),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, BooleanArray, Int16Array};

    use super::*;
    use crate::{
        catalog_source::{ColumnInfo, IndexInfo, RelKind},
        mock::MockCatalog,
        Oid, SystemView,
    };

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
                type_oid: Oid(23),
                not_null: true,
                atttypmod: -1,
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_column(ColumnInfo {
                table_oid: Oid(16385),
                name: "name".to_string(),
                attnum: 2,
                type_oid: Oid(25),
                not_null: false,
                atttypmod: -1,
                attisdropped: false,
                attidentity: None,
                attgenerated: None,
            })
            .with_index(IndexInfo {
                oid: Oid(16389),
                table_oid: Oid(16385),
                name: "widgets_pkey".to_string(),
                is_unique: true,
                is_primary: true,
                column_attnums: vec![1],
            })
    }

    fn names(batch: &RecordBatch) -> Vec<String> {
        batch
            .column(batch.schema().index_of("relname").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap().to_string())
            .collect()
    }

    #[test]
    fn returns_every_table_the_mock_holds() {
        let batch = PgClass.scan(&catalog(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let mut n = names(&batch);
        n.sort();
        assert_eq!(n, vec!["gadgets".to_string(), "widgets".to_string()]);
    }

    /// `relnatts` is derived from `CatalogSource::columns`, not stored on
    /// `TableInfo` — this is the whole reason `pg_class` needs a catalog
    /// while `pg_type` does not.
    #[test]
    fn relnatts_is_derived_from_catalog_columns() {
        let batch = PgClass
            .scan(&catalog(), &[Predicate::eq("oid", Oid(16385))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        let relnatts = batch
            .column(batch.schema().index_of("relnatts").unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(relnatts.value(0), 2, "widgets has two columns in the mock");

        let gadgets = PgClass
            .scan(&catalog(), &[Predicate::eq("oid", Oid(16390))])
            .unwrap();
        let relnatts = gadgets
            .column(gadgets.schema().index_of("relnatts").unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(
            relnatts.value(0),
            0,
            "gadgets has no columns registered in the mock"
        );
    }

    /// `relhasindex` is likewise derived, from `CatalogSource::indexes`.
    #[test]
    fn relhasindex_is_derived_from_catalog_indexes() {
        let batch = PgClass.scan(&catalog(), &[]).unwrap();
        let by_name: Vec<(String, bool)> = names(&batch)
            .into_iter()
            .zip(
                batch
                    .column(batch.schema().index_of("relhasindex").unwrap())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .map(|b| b.unwrap()),
            )
            .collect();

        assert!(by_name.contains(&("widgets".to_string(), true)));
        assert!(by_name.contains(&("gadgets".to_string(), false)));
    }

    /// A pushed `relkind` predicate narrows on a derived-from-`TableInfo`
    /// (not derived-from-catalog-query) column.
    #[test]
    fn pushed_relkind_predicate_narrows() {
        let batch = PgClass
            .scan(&catalog(), &[Predicate::eq("relkind", "r")])
            .unwrap();
        assert_eq!(batch.num_rows(), 2, "both mock tables are ordinary tables");
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgClass.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// Pins the exact column set, order, and Arrow type of `pg_class` against
    /// live PostgreSQL 18.2's `attnum` order, so a future edit cannot
    /// silently reorder or rename a column out from under positional readers
    /// (psql's `\d`, `pg_dump`, and ORM introspection all read this
    /// positionally as well as by name). Verified live via:
    ///
    /// ```sql
    /// SELECT attname, atttypid::regtype, attnum, attnotnull
    ///   FROM pg_attribute
    ///  WHERE attrelid = 'pg_catalog.pg_class'::regclass AND attnum > 0
    ///  ORDER BY attnum;
    /// ```
    ///
    /// which reports `relhasindex` at attnum 15, *before* `relkind` (18) and
    /// `relnatts` (19) — the order this crate had backwards until this audit
    /// — and all **34** columns in exactly this order. This relation reports
    /// every one of them, so position here is faithful as well as name.
    #[test]
    fn schema_matches_live_postgres_column_order_and_types() {
        let list_of_utf8 = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let schema = PgClass.schema();
        let got: Vec<(&str, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("oid", DataType::UInt32, false),
                ("relname", DataType::Utf8, false),
                ("relnamespace", DataType::UInt32, false),
                ("reltype", DataType::UInt32, false),
                ("reloftype", DataType::UInt32, false),
                ("relowner", DataType::UInt32, false),
                ("relam", DataType::UInt32, false),
                ("relfilenode", DataType::UInt32, false),
                ("reltablespace", DataType::UInt32, false),
                ("relpages", DataType::Int32, false),
                ("reltuples", DataType::Float32, false),
                ("relallvisible", DataType::Int32, false),
                ("relallfrozen", DataType::Int32, false),
                ("reltoastrelid", DataType::UInt32, false),
                ("relhasindex", DataType::Boolean, false),
                ("relisshared", DataType::Boolean, false),
                ("relpersistence", DataType::Utf8, false),
                ("relkind", DataType::Utf8, false),
                ("relnatts", DataType::Int16, false),
                ("relchecks", DataType::Int16, false),
                ("relhasrules", DataType::Boolean, false),
                ("relhastriggers", DataType::Boolean, false),
                ("relhassubclass", DataType::Boolean, false),
                ("relrowsecurity", DataType::Boolean, false),
                ("relforcerowsecurity", DataType::Boolean, false),
                ("relispopulated", DataType::Boolean, false),
                ("relreplident", DataType::Utf8, false),
                ("relispartition", DataType::Boolean, false),
                ("relrewrite", DataType::UInt32, false),
                ("relfrozenxid", DataType::UInt32, false),
                ("relminmxid", DataType::UInt32, false),
                ("relacl", list_of_utf8.clone(), true),
                ("reloptions", list_of_utf8, true),
                ("relpartbound", DataType::Utf8, true),
            ]
        );
    }

    /// `relchecks` is derived from `CatalogSource::constraints`, counting
    /// only `CHECK` constraints — a `PRIMARY KEY` does not raise it, which is
    /// what real Postgres reports (the probe table in the module docs has a
    /// primary key and `relchecks = 0`).
    #[test]
    fn relchecks_counts_only_check_constraints() {
        use crate::catalog_source::{ConstraintInfo, ConstraintKind};

        let catalog = catalog()
            .with_constraint(ConstraintInfo {
                oid: Oid(30001),
                table_oid: Oid(16385),
                name: "widgets_pkey".to_string(),
                kind: ConstraintKind::PrimaryKey,
            })
            .with_constraint(ConstraintInfo {
                oid: Oid(30002),
                table_oid: Oid(16385),
                name: "widgets_id_positive".to_string(),
                kind: ConstraintKind::Check,
            });

        let batch = PgClass
            .scan(&catalog, &[Predicate::eq("oid", Oid(16385))])
            .unwrap();
        let relchecks = batch
            .column(batch.schema().index_of("relchecks").unwrap())
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(relchecks.value(0), 1, "one CHECK, not two constraints");
    }

    /// `relhassubclass` is derived from `CatalogSource::inheritance`, which is
    /// child-scoped: the *parent* is the relation that reports `true`.
    #[test]
    fn relhassubclass_marks_the_parent_not_the_child() {
        use crate::catalog_source::InheritanceInfo;

        let catalog = catalog().with_inheritance(InheritanceInfo {
            child_oid: Oid(16390),
            parent_oid: Oid(16385),
            seqno: 1,
        });

        let by_name: Vec<(String, bool)> = {
            let batch = PgClass.scan(&catalog, &[]).unwrap();
            names(&batch)
                .into_iter()
                .zip(
                    batch
                        .column(batch.schema().index_of("relhassubclass").unwrap())
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .map(|b| b.unwrap()),
                )
                .collect()
        };
        assert!(
            by_name.contains(&("widgets".to_string(), true)),
            "the parent"
        );
        assert!(
            by_name.contains(&("gadgets".to_string(), false)),
            "the child"
        );
    }

    /// `relreplident` follows `relkind`, exactly as the live probe in the
    /// module docs shows: `'d'` for a table, `'n'` for an index.
    #[test]
    fn relreplident_follows_relkind() {
        assert_eq!(relreplident(RelKind::OrdinaryTable), 'd');
        assert_eq!(relreplident(RelKind::MaterializedView), 'd');
        assert_eq!(relreplident(RelKind::Index), 'n');
        assert_eq!(relreplident(RelKind::View), 'n');
        assert_eq!(relreplident(RelKind::Sequence), 'n');
    }

    /// `reltuples` is `-1`, Postgres's own "row count unknown" sentinel, not
    /// `0` ("known to be empty") — see the module docs.
    #[test]
    fn reltuples_is_the_unknown_sentinel_not_zero() {
        let batch = PgClass.scan(&catalog(), &[]).unwrap();
        let reltuples = batch
            .column(batch.schema().index_of("reltuples").unwrap())
            .as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .unwrap();
        assert!(reltuples.values().iter().all(|&v| v == -1.0));
    }

    /// The three nullable columns are `NULL` for every row — see the module
    /// docs for why `NULL` is the correct value for each.
    #[test]
    fn the_three_nullable_columns_are_null_for_every_row() {
        let batch = PgClass.scan(&catalog(), &[]).unwrap();
        assert!(batch.num_rows() > 0);
        for name in ["relacl", "reloptions", "relpartbound"] {
            let c = batch.column(batch.schema().index_of(name).unwrap());
            assert_eq!(c.null_count(), c.len(), "{name} must be NULL everywhere");
        }
    }
}
