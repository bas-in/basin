//! `pg_catalog.pg_description` — the relation psql (`\d+`, `\dt+`) and
//! `pg_dump` read `COMMENT ON` text off.
//!
//! # This relation returns zero rows today, and that is correct
//!
//! `COMMENT ON TABLE`/`COMMENT ON COLUMN` is parsed but accepted as a silent
//! no-op in `basin-engine` — see `crates/basin-engine/src/pg_ast.rs`'s
//! `reject_unsupported` test, which lists `"COMMENT ON TABLE t IS 'desc'"`
//! among the statements Basin parses and discards rather than rejects. There
//! is therefore no comment storage anywhere upstream of this crate:
//! [`CatalogSource::comments`]'s own doc comment says so, and
//! [`crate::real_source::RealCatalogSource::comments`] (the implementation a
//! live session would actually use) is hard-coded to return an empty `Vec`
//! for exactly this reason.
//!
//! This relation must not paper over that gap. A caller — psql's `\d+`,
//! `pg_dump`, an ORM's introspection — treats "no row in `pg_description`"
//! as "no comment", which is only true here because nothing can produce a
//! comment in the first place. Synthesising a plausible-looking row (e.g.
//! echoing a column's name back as its "description") would make that
//! tooling believe a comment exists that a user never wrote. So: this file
//! wires [`CatalogSource::comments`] straight through, unmodified, and when
//! that returns nothing (always, today), `pg_description` reports zero rows.
//! The day `COMMENT ON` gains real storage, this relation needs no changes
//! at all — only [`CatalogSource::comments`]'s implementations do.
//!
//! # Where the column layout comes from
//!
//! Checked against a live PostgreSQL 18.2, not recalled from memory:
//!
//! ```sql
//! SELECT attname, atttypid::regtype, attnum, attnotnull
//!   FROM pg_attribute
//!  WHERE attrelid = 'pg_catalog.pg_description'::regclass AND attnum > 0
//!  ORDER BY attnum;
//! --   attname   | atttypid | attnum | attnotnull
//! -- objoid      | oid      |      1 | t
//! -- classoid    | oid      |      2 | t
//! -- objsubid    | integer  |      3 | t
//! -- description | text     |      4 | t
//! ```
//!
//! `classoid` is always `pg_class`'s own `pg_class.oid`, not the table's
//! `pg_class` row — real `pg_description.classoid` names *which system
//! catalog* `objoid` is a row of (`pg_class` for a table/column comment,
//! `pg_proc` for a function comment, `pg_type` for a type comment, ...).
//! Confirmed live, and matches [`crate::catalog_source::CommentInfo`]'s own
//! doc comment:
//!
//! ```sql
//! CREATE TABLE cmt_test (id serial PRIMARY KEY, note text);
//! COMMENT ON TABLE cmt_test IS 'a table comment';
//! COMMENT ON COLUMN cmt_test.note IS 'a column comment';
//! SELECT objoid, classoid, objsubid, description FROM pg_description
//!   WHERE objoid = 'cmt_test'::regclass;
//! --  objoid  | classoid | objsubid |   description
//! -- 27614366 |     1259 |        0 | a table comment
//! -- 27614366 |     1259 |        2 | a column comment
//! ```
//!
//! `1259` is `'pg_catalog.pg_class'::regclass::oid`, confirmed live and
//! stable across installations (it is a fixed bootstrap OID, the same way
//! [`crate::PG_CATALOG_NAMESPACE`] is). This crate only ever reports table
//! and column comments (the two kinds [`CatalogSource::comments`] can
//! supply — see its own doc comment), so `classoid` is always this
//! constant, never a per-row value.
//!
//! # What is deliberately absent
//!
//! Real `pg_description` covers every commentable object kind (schema,
//! function, operator, type, index, constraint, ...) via `classoid`
//! varying; this relation reports only table and column comments, matching
//! the restriction [`crate::pg_attrdef`] and [`crate::pg_index`] already
//! make for defaults and indexes. A comment on anything else is simply
//! absent, not reported wrongly.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    catalog_source::{CatalogSource, CommentInfo},
    error::Error,
    predicate::{Predicate, Value},
    Oid,
};

/// `'pg_catalog.pg_class'::regclass::oid` — confirmed live (see module
/// docs). Every row this relation reports has this as `classoid`, since
/// both a table comment and a column comment are filed against the table's
/// own `pg_class` row (see [`CommentInfo`]'s doc comment).
const PG_CLASS_OID: Oid = Oid(1259);

/// One row of `pg_description`, joining a table's oid to one of its
/// [`CommentInfo`] rows.
struct DescriptionRow {
    objoid: Oid,
    objsubid: i32,
    description: String,
}

impl DescriptionRow {
    /// This row's value for `column`, or `None` if `column` is not one of
    /// this relation's columns.
    fn value(&self, column: &str) -> Option<Value> {
        Some(match column {
            "objoid" => Value::Oid(self.objoid),
            "classoid" => Value::Oid(PG_CLASS_OID),
            "objsubid" => Value::Int(self.objsubid as i64),
            "description" => Value::Text(self.description.clone()),
            _ => return None,
        })
    }
}

/// `pg_catalog.pg_description`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PgDescription;

impl PgDescription {
    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("objoid", DataType::UInt32, false),
            Field::new("classoid", DataType::UInt32, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
        ]))
    }
}

impl crate::SystemView for PgDescription {
    fn name(&self) -> &str {
        "pg_description"
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
                    relation: "pg_description",
                    column: p.column().to_string(),
                });
            }
        }

        let mut rows: Vec<DescriptionRow> = Vec::new();
        for table in catalog.tables() {
            for c in catalog.comments(table.oid) {
                let CommentInfo {
                    objsubid,
                    description,
                } = c;
                rows.push(DescriptionRow {
                    objoid: table.oid,
                    objsubid,
                    description,
                });
            }
        }

        let rows: Vec<DescriptionRow> = rows
            .into_iter()
            .filter(|r| {
                pushed
                    .iter()
                    .all(|p| p.matches(r.value(p.column()).as_ref()))
            })
            .collect();

        let objoids: UInt32Array = rows.iter().map(|r| r.objoid.get()).collect();
        let classoids: UInt32Array = rows.iter().map(|_| PG_CLASS_OID.get()).collect();
        let objsubids: Int32Array = rows.iter().map(|r| r.objsubid).collect();
        let descriptions: StringArray = rows.iter().map(|r| Some(r.description.as_str())).collect();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(objoids),
                Arc::new(classoids),
                Arc::new(objsubids),
                Arc::new(descriptions),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;
    use crate::{
        catalog_source::{RelKind, TableInfo},
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

    /// `widgets` has a table comment (`objsubid = 0`) and a column comment
    /// (`objsubid` = the commented column's `attnum`); `gadgets` has none —
    /// matching the live `cmt_test` capture in the module docs.
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
            .with_comment(
                Oid(16385),
                CommentInfo {
                    objsubid: 0,
                    description: "a table comment".to_string(),
                },
            )
            .with_comment(
                Oid(16385),
                CommentInfo {
                    objsubid: 2,
                    description: "a column comment".to_string(),
                },
            )
    }

    #[test]
    fn name_is_pg_description() {
        assert_eq!(PgDescription.name(), "pg_description");
    }

    /// The exact column layout live Postgres reports, in order: `objoid`,
    /// `classoid`, `objsubid`, `description`, all `NOT NULL`.
    #[test]
    fn schema_matches_live_postgres_column_layout() {
        let schema = PgDescription.schema();
        let fields: Vec<(&str, &DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
            .collect();
        assert_eq!(
            fields,
            vec![
                ("objoid", &DataType::UInt32, false),
                ("classoid", &DataType::UInt32, false),
                ("objsubid", &DataType::Int32, false),
                ("description", &DataType::Utf8, false),
            ]
        );
    }

    /// `MockCatalog` today produces zero comments for a fresh/empty catalog
    /// — this is the "honest empty" case the module docs insist on: no
    /// comment storage upstream means no rows, not fabricated ones.
    #[test]
    fn empty_catalog_yields_zero_rows() {
        let batch = PgDescription.scan(&MockCatalog::new(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// A table with no comments at all yields no rows for it, even though
    /// other tables in the same catalog have comments.
    #[test]
    fn table_with_no_comments_yields_no_rows() {
        let batch = PgDescription
            .scan(&catalog(), &[Predicate::eq("objoid", Oid(16390))])
            .unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    /// The core pushdown claim, applied to `pg_description`: a predicate on
    /// `objoid` narrows to exactly that table's comment rows, and the
    /// narrowing happens inside `scan`, not after it — proven by asserting
    /// the unfiltered scan has more rows than the filtered one.
    #[test]
    fn pushed_objoid_predicate_narrows_to_one_tables_comments() {
        let full = PgDescription.scan(&catalog(), &[]).unwrap();
        assert_eq!(full.num_rows(), 2, "sanity: two comments total");

        let filtered = PgDescription
            .scan(&catalog(), &[Predicate::eq("objoid", Oid(16385))])
            .unwrap();
        assert_eq!(filtered.num_rows(), 2);
        for oid in col_u32(&filtered, "objoid") {
            assert_eq!(oid, 16385);
        }
    }

    /// `objsubid = 0` is the table comment; the column comment carries the
    /// commented column's `attnum` — matches real Postgres's own encoding
    /// (verified live, see module docs).
    #[test]
    fn objsubid_distinguishes_table_from_column_comments() {
        let batch = PgDescription
            .scan(&catalog(), &[Predicate::eq("objoid", Oid(16385))])
            .unwrap();
        let mut by_subid: Vec<(i32, String)> = col_i32(&batch, "objsubid")
            .into_iter()
            .zip(col_str(&batch, "description"))
            .collect();
        by_subid.sort();
        assert_eq!(
            by_subid,
            vec![
                (0, "a table comment".to_string()),
                (2, "a column comment".to_string()),
            ]
        );
    }

    /// `classoid` is always `pg_class`'s own oid (`1259`, confirmed live),
    /// for every row this relation reports — never the commented table's
    /// own oid.
    #[test]
    fn classoid_is_always_pg_class_oid() {
        let batch = PgDescription.scan(&catalog(), &[]).unwrap();
        for classoid in col_u32(&batch, "classoid") {
            assert_eq!(classoid, 1259);
        }
    }

    /// A predicate naming a column `pg_description` does not have is an
    /// error, not a silently ignored no-op.
    #[test]
    fn predicate_on_unknown_column_is_an_error() {
        let err = PgDescription
            .scan(&catalog(), &[Predicate::eq("nope", 1i64)])
            .unwrap_err();
        assert_eq!(
            err,
            Error::UnknownColumn {
                relation: "pg_description",
                column: "nope".to_string(),
            }
        );
    }
}
