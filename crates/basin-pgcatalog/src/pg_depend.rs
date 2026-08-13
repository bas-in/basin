//! `pg_catalog.pg_depend` — the dependency graph `pg_dump` walks to order
//! `CREATE`/`DROP` statements correctly (a column default must be created
//! after its table; a table must be created after its schema; and so on).
//!
//! # Where the column layout comes from
//!
//! Checked against a live PostgreSQL 18.2, not recalled from memory:
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull FROM pg_attribute
//!   WHERE attrelid = 'pg_catalog.pg_depend'::regclass AND attnum > 0
//!   ORDER BY attnum;
//! --   attname   | atttypid | attnum | attnotnull
//! -- classid     | oid      |      1 | t
//! -- objid       | oid      |      2 | t
//! -- objsubid    | integer  |      3 | t
//! -- refclassid  | oid      |      4 | t
//! -- refobjid    | oid      |      5 | t
//! -- refobjsubid | integer  |      6 | t
//! -- deptype     | "char"   |      7 | t
//! ```
//!
//! # Scope: only the edges this crate can report without guessing
//!
//! Real `pg_depend` is dense — creating one table with a serial primary key
//! and a `REFERENCES` column produces rows for the table's composite row
//! type, its owned sequence, its `NOT NULL` constraints (PostgreSQL 18),
//! its primary key constraint and index, its foreign key constraint, and
//! its schema, verified live:
//!
//! ```sql
//! CREATE TABLE dep_parent (id serial PRIMARY KEY, note text DEFAULT 'x');
//! CREATE TABLE dep_child (id serial PRIMARY KEY,
//!                          parent_id int REFERENCES dep_parent(id));
//! CREATE INDEX dep_child_parent_idx ON dep_child(parent_id);
//! ```
//!
//! [`CatalogSource`] does not model most of that: there is no row-type oid
//! on [`crate::catalog_source::TableInfo`], no sequence-owns-column link,
//! no per-column list on [`crate::catalog_source::ConstraintInfo`] (so a
//! multi-column `CHECK` or `FOREIGN KEY`'s exact `refobjsubid` values can't
//! be derived), and no way to tell a plain `CREATE UNIQUE INDEX` apart from
//! an index backing a named `UNIQUE` table constraint. Rather than guess at
//! any of those — a wrong `deptype` on a real dependency graph breaks
//! `pg_dump`'s ordering silently, which is worse than the edge being absent
//! — this relation reports exactly three edge kinds, each verified live and
//! each fully determined by data [`CatalogSource`] already exposes:
//!
//! 1. **Every table depends `'n'` (normal) on its schema.** Verified live:
//!    `dep_child`'s own `pg_depend` row against its namespace is `deptype =
//!    'n'`. Derived from [`crate::catalog_source::TableInfo::namespace`]
//!    directly.
//! 2. **Every column default depends `'a'` (auto) on its table's column.**
//!    Verified live: `pg_attrdef`'s row for `dep_parent.note`'s default
//!    depends on `dep_parent`, `refobjsubid = 2` (`note`'s `attnum`).
//!    Derived from [`crate::catalog_source::ColumnDefaultInfo`] directly —
//!    this is the same data [`crate::pg_attrdef`] reports, reshaped.
//! 3. **An index depends on what it indexes**, split two ways, both
//!    verified live:
//!    - A **primary key**'s index depends `'i'` (internal) on the primary
//!      key *constraint*, not on the table's columns — verified live
//!      (`dep_parent_pkey`'s only `pg_depend` row points at
//!      `pg_constraint`, not `pg_class`). This crate only emits this row
//!      when [`CatalogSource::constraints`] actually has a matching
//!      `PrimaryKey` row for the same table (real Postgres always has one
//!      for a primary key index; a catalog that doesn't supply one is
//!      inconsistent, and this relation silently omits the edge rather than
//!      invent a `refobjid` that names nothing).
//!    - Every other index depends `'a'` on each of its indexed columns —
//!      verified live for a plain `CREATE INDEX` (`dep_child_parent_idx`
//!      depends on `dep_child`, `refobjsubid = 2`). **This is also what a
//!      plain unique index reports, but a unique index backing a *named*
//!      `UNIQUE` table constraint really depends `'i'` on that constraint
//!      in real Postgres** (verified live the same way as the primary key
//!      case) — [`crate::catalog_source::IndexInfo`] has no link back to
//!      the constraint it backs, so this crate cannot tell the two cases
//!      apart and reports the constraint-backed case as `'a'` on columns
//!      instead of `'i'` on the constraint. Documented deviation, not a
//!      bug: a `CatalogSource` gap, the same shape as
//!      [`crate::pg_index`]'s documented `indisvalid`/`indnkeyatts` gaps.
//!
//! Everything else real `pg_depend` reports (composite type dependencies,
//! sequence ownership, per-column constraint dependencies, extension
//! membership, ...) is simply absent here, not reported wrongly.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, ConstraintKind},
    error::Error,
    predicate::{Predicate, Value},
    Oid,
};

/// `'pg_catalog.pg_class'::regclass::oid` — confirmed live, fixed across
/// installations.
const PG_CLASS_OID: Oid = Oid(1259);
/// `'pg_catalog.pg_namespace'::regclass::oid` — confirmed live.
const PG_NAMESPACE_OID: Oid = Oid(2615);
/// `'pg_catalog.pg_attrdef'::regclass::oid` — confirmed live.
const PG_ATTRDEF_OID: Oid = Oid(2604);
/// `'pg_catalog.pg_constraint'::regclass::oid` — confirmed live.
const PG_CONSTRAINT_OID: Oid = Oid(2606);

/// One row of `pg_depend`, already resolved to concrete oids.
struct DependRow {
    classid: Oid,
    objid: Oid,
    objsubid: i32,
    refclassid: Oid,
    refobjid: Oid,
    refobjsubid: i32,
    deptype: char,
}

impl DependRow {
    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's columns.
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            "classid" => Value::Oid(self.classid),
            "objid" => Value::Oid(self.objid),
            "objsubid" => Value::Int(self.objsubid as i64),
            "refclassid" => Value::Oid(self.refclassid),
            "refobjid" => Value::Oid(self.refobjid),
            "refobjsubid" => Value::Int(self.refobjsubid as i64),
            "deptype" => Value::Text(self.deptype.to_string()),
            _ => return None,
        })
    }
}

/// `pg_catalog.pg_depend`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgDepend;

impl PgDepend {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("classid", DataType::UInt32, false),
            Field::new("objid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("refclassid", DataType::UInt32, false),
            Field::new("refobjid", DataType::UInt32, false),
            Field::new("refobjsubid", DataType::Int32, false),
            Field::new("deptype", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgDepend {
    fn name(&self) -> &str {
        "pg_depend"
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
                    relation: "pg_depend",
                    column: p.column().to_string(),
                });
            }
        }

        let mut rows: Vec<DependRow> = Vec::new();
        for table in catalog.tables() {
            // 1. table -> namespace ('n'), verified live.
            rows.push(DependRow {
                classid: PG_CLASS_OID,
                objid: table.oid,
                objsubid: 0,
                refclassid: PG_NAMESPACE_OID,
                refobjid: table.namespace,
                refobjsubid: 0,
                deptype: 'n',
            });

            // 2. column default -> table column ('a'), verified live.
            for d in catalog.column_defaults(table.oid) {
                rows.push(DependRow {
                    classid: PG_ATTRDEF_OID,
                    objid: d.oid,
                    objsubid: 0,
                    refclassid: PG_CLASS_OID,
                    refobjid: table.oid,
                    refobjsubid: d.attnum as i32,
                    deptype: 'a',
                });
            }

            // 3. index -> {constraint 'i', or columns 'a'}, verified live.
            let constraints = catalog.constraints(table.oid);
            for idx in catalog.indexes(table.oid) {
                if idx.is_primary {
                    // Only emit when a matching PrimaryKey constraint is
                    // actually present — see module docs on why this is a
                    // silent omission rather than a guess.
                    if let Some(pk) = constraints
                        .iter()
                        .find(|c| c.kind == ConstraintKind::PrimaryKey)
                    {
                        rows.push(DependRow {
                            classid: PG_CLASS_OID,
                            objid: idx.oid,
                            objsubid: 0,
                            refclassid: PG_CONSTRAINT_OID,
                            refobjid: pk.oid,
                            refobjsubid: 0,
                            deptype: 'i',
                        });
                    }
                } else {
                    for attnum in &idx.column_attnums {
                        rows.push(DependRow {
                            classid: PG_CLASS_OID,
                            objid: idx.oid,
                            objsubid: 0,
                            refclassid: PG_CLASS_OID,
                            refobjid: table.oid,
                            refobjsubid: *attnum as i32,
                            deptype: 'a',
                        });
                    }
                }
            }
        }

        let rows: Vec<DependRow> = rows
            .into_iter()
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(r.value(p.column()).as_ref()))
            })
            .collect();

        let classids: UInt32Array = rows.iter().map(|r| r.classid.get()).collect();
        let objids: UInt32Array = rows.iter().map(|r| r.objid.get()).collect();
        let objsubids: Int32Array = rows.iter().map(|r| r.objsubid).collect();
        let refclassids: UInt32Array = rows.iter().map(|r| r.refclassid.get()).collect();
        let refobjids: UInt32Array = rows.iter().map(|r| r.refobjid.get()).collect();
        let refobjsubids: Int32Array = rows.iter().map(|r| r.refobjsubid).collect();
        let deptypes: StringArray = rows.iter().map(|r| Some(r.deptype.to_string())).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(classids),
                Arc::new(objids),
                Arc::new(objsubids),
                Arc::new(refclassids),
                Arc::new(refobjids),
                Arc::new(refobjsubids),
                Arc::new(deptypes),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::{
        catalog_source::{ColumnDefaultInfo, ConstraintInfo, IndexInfo, RelKind, TableInfo},
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

    fn col_i32(batch: &RecordBatch, name: &str) -> Vec<i32> {
        batch
            .column(batch.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
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

    /// Mirrors the live `dep_parent`/`dep_child` capture in the module
    /// docs: `dep_parent` has a primary key (`id`, backed by a named
    /// constraint) and a defaulted column (`note`); `dep_child` has a plain
    /// (non-unique) index on `parent_id`.
    fn catalog() -> MockCatalog {
        MockCatalog::new()
            .with_table(TableInfo {
                oid: Oid(16385),
                name: "dep_parent".to_string(),
                namespace: Oid(2200),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_table(TableInfo {
                oid: Oid(16390),
                name: "dep_child".to_string(),
                namespace: Oid(2200),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_default(ColumnDefaultInfo {
                oid: Oid(20001),
                table_oid: Oid(16385),
                attnum: 2,
                expr_src: "'x'::text".to_string(),
            })
            .with_index(IndexInfo {
                oid: Oid(16386),
                table_oid: Oid(16385),
                name: "dep_parent_pkey".to_string(),
                is_unique: true,
                is_primary: true,
                column_attnums: vec![1],
            })
            .with_constraint(ConstraintInfo {
                oid: Oid(16387),
                table_oid: Oid(16385),
                name: "dep_parent_pkey".to_string(),
                kind: ConstraintKind::PrimaryKey,
            })
            .with_index(IndexInfo {
                oid: Oid(16391),
                table_oid: Oid(16390),
                name: "dep_child_parent_idx".to_string(),
                is_unique: false,
                is_primary: false,
                column_attnums: vec![2],
            })
    }

    #[test]
    fn name_is_pg_depend() {
        assert_eq!(PgDepend.name(), "pg_depend");
    }

    /// The exact column layout live Postgres reports, in order.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgDepend.schema();
        let fields: Vec<(&str, &DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("classid", &DataType::UInt32, false),
                ("objid", &DataType::UInt32, false),
                ("objsubid", &DataType::Int32, false),
                ("refclassid", &DataType::UInt32, false),
                ("refobjid", &DataType::UInt32, false),
                ("refobjsubid", &DataType::Int32, false),
                ("deptype", &DataType::Utf8, false),
            ]
        );
    }

    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgDepend.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// Every table depends `'n'` on its schema — one row per table, always.
    #[test]
    fn every_table_depends_normally_on_its_namespace() {
        let batch = PgDepend
            .scan(&catalog(), &[Predicate::eq("deptype", "n")])
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        for refobjid in col_u32(&batch, "refobjid") {
            assert_eq!(refobjid, 2200);
        }
        let mut objids = col_u32(&batch, "objid");
        objids.sort_unstable();
        assert_eq!(objids, vec![16385, 16390]);
    }

    /// The column default on `dep_parent.note` depends `'a'` on
    /// `dep_parent`, `refobjsubid = 2` — exactly `note`'s `attnum`.
    #[test]
    fn column_default_depends_on_its_table_column() {
        let batch = PgDepend
            .scan(&catalog(), &[Predicate::eq("classid", PG_ATTRDEF_OID)])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_u32(&batch, "refobjid"), vec![16385]);
        assert_eq!(col_i32(&batch, "refobjsubid"), vec![2]);
        assert_eq!(col_str(&batch, "deptype"), vec!["a".to_string()]);
    }

    /// The primary key's index depends `'i'` on the primary key
    /// *constraint*, not on the table's columns — matches the live
    /// `dep_parent_pkey` capture in the module docs.
    #[test]
    fn primary_key_index_depends_internally_on_its_constraint() {
        let batch = PgDepend
            .scan(&catalog(), &[Predicate::eq("objid", Oid(16386))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_u32(&batch, "refclassid"), vec![PG_CONSTRAINT_OID.get()]);
        assert_eq!(col_u32(&batch, "refobjid"), vec![16387]);
        assert_eq!(col_str(&batch, "deptype"), vec!["i".to_string()]);
    }

    /// A plain (non-primary) index depends `'a'` on each of its indexed
    /// columns — matches the live `dep_child_parent_idx` capture.
    #[test]
    fn plain_index_depends_on_its_columns() {
        let batch = PgDepend
            .scan(&catalog(), &[Predicate::eq("objid", Oid(16391))])
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(col_u32(&batch, "refclassid"), vec![PG_CLASS_OID.get()]);
        assert_eq!(col_u32(&batch, "refobjid"), vec![16390]);
        assert_eq!(col_i32(&batch, "refobjsubid"), vec![2]);
        assert_eq!(col_str(&batch, "deptype"), vec!["a".to_string()]);
    }

    /// A primary key index with no matching `PrimaryKey` constraint in the
    /// catalog yields no dependency row for that index at all — the
    /// documented silent omission, not a fabricated `refobjid`.
    #[test]
    fn primary_index_without_matching_constraint_is_omitted() {
        let catalog = MockCatalog::new()
            .with_table(TableInfo {
                oid: Oid(16385),
                name: "widgets".to_string(),
                namespace: Oid(2200),
                owner: Oid(10),
                kind: RelKind::OrdinaryTable,
            })
            .with_index(IndexInfo {
                oid: Oid(16386),
                table_oid: Oid(16385),
                name: "widgets_pkey".to_string(),
                is_unique: true,
                is_primary: true,
                column_attnums: vec![1],
            });
        // No matching constraint row supplied.
        let batch = PgDepend
            .scan(&catalog, &[Predicate::eq("objid", Oid(16386))])
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// The entire point of this crate: a pushed `refobjid` predicate
    /// narrows to exactly the dependencies pointing at one object, applied
    /// inside `scan` rather than after it.
    #[test]
    fn pushed_refobjid_predicate_narrows_to_one_objects_dependents() {
        let full = PgDepend.scan(&catalog(), &[]).unwrap();
        assert_eq!(
            full.num_rows(),
            5,
            "sanity: 2 namespace + 1 default + 1 pk index + 1 plain index"
        );

        let filtered = PgDepend
            .scan(&catalog(), &[Predicate::eq("refobjid", Oid(16385))])
            .unwrap();
        // Only the attrdef edge. A table's NAMESPACE edge does not point at
        // the table — it points at the namespace, with the table as `objid`.
        // Confirmed on PostgreSQL 18.2 against a real table:
        //
        //   SELECT count(*) FROM pg_depend WHERE refobjid = 'dep_probe'::regclass;
        //     -> pg_type, pg_constraint, pg_constraint, pg_attrdef — no namespace
        //   SELECT count(*) FROM pg_depend
        //     WHERE objid = 'dep_probe'::regclass
        //       AND refclassid = 'pg_namespace'::regclass;   -> 1
        //
        // This test originally asserted 2 on the assumption that the namespace
        // edge counted here. It does not, and asserting it would have pinned
        // an edge direction backwards — the same class of error as reading an
        // asymmetric operator OID the wrong way round.
        assert_eq!(filtered.num_rows(), 1);
        for refobjid in col_u32(&filtered, "refobjid") {
            assert_eq!(refobjid, 16385);
        }
    }

    /// A predicate naming a column `pg_depend` does not have is an error.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgDepend
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_depend",
                column: "nope".to_string(),
            }
        );
    }
}
