//! `information_schema` + `pg_catalog` views (Phase 5.11.M starter).
//!
//! Read-only projections over the live [`Catalog`] state. Results are
//! Arrow [`RecordBatch`]es shaped exactly like the Postgres / SQL-standard
//! columns the most-common introspecting clients (PostgREST, pgAdmin,
//! DataGrip, ORM auto-discovery) read on startup. Engine-side `SELECT *
//! FROM information_schema.tables` routing is intentionally deferred to a
//! follow-up agent — this module is the data structure half.
//!
//! Scope discipline (5.11.M starter):
//! - Only `information_schema.tables` and `pg_catalog.pg_class`.
//! - Read existing [`Catalog`] methods only (`list_tables`, `load_table`).
//!   No new trait methods; other agents are concurrently extending the
//!   trait and we avoid contention.
//! - Filter to the calling project's tables at query-construction time;
//!   never materialise cross-project rows.
//!
//! Oid-hashing scheme: see [`stable_oid`].

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema};
use basin_common::{BasinError, ProjectId, Result, TableName};

use crate::functions::{SqlArgType, SqlReturnType};
use crate::reserved_schema::ReservedSchema;
use crate::Catalog;

/// Read-only projections over [`Catalog`] state for the
/// `information_schema` / `pg_catalog` system views.
pub struct InfoSchemaQuery;

impl InfoSchemaQuery {
    /// Schema for `information_schema.tables` rows.
    ///
    /// | column         | type | notes                                          |
    /// |----------------|------|------------------------------------------------|
    /// | table_catalog  | TEXT | always `"basin"` for v0.1                      |
    /// | table_schema   | TEXT | always `"public"` (single-schema-per-project)   |
    /// | table_name     | TEXT | the user-visible table name                    |
    /// | table_type     | TEXT | `"BASE TABLE"` for tables and continuous       |
    /// |                |      | aggregates (PG behaviour for matviews); `"VIEW"` |
    /// |                |      | reserved for future plain views                |
    pub fn tables_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]))
    }

    /// Schema for `pg_catalog.pg_class` rows.
    ///
    /// | column          | type   | notes                                   |
    /// |-----------------|--------|-----------------------------------------|
    /// | oid             | BIGINT | stable per-(project, table) hash         |
    /// | relname         | TEXT   | table name within the project            |
    /// | relnamespace    | BIGINT | hash of `"public"` namespace            |
    /// | relkind         | TEXT   | `'r'` table / `'v'` view / `'m'` matview |
    /// | relrowsecurity  | BOOL   | RLS-enabled flag                        |
    /// | relispartition  | BOOL   | always false in v0.1                    |
    /// | reltuples       | FLOAT4 | row-count estimate (sum across snapshot)|
    pub fn pg_class_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("relnamespace", DataType::Int64, false),
            Field::new("relkind", DataType::Utf8, false),
            Field::new("relrowsecurity", DataType::Boolean, false),
            Field::new("relispartition", DataType::Boolean, false),
            Field::new("reltuples", DataType::Float32, false),
        ]))
    }

    /// Build `information_schema.tables` filtered to `project`'s tables.
    ///
    /// Phase 5.18.D: uses [`Catalog::list_tables_qualified`] so each row
    /// carries the table's real schema name (auth/storage/cron/net/…) rather
    /// than hardcoding `"public"` for every table.
    ///
    /// Cross-project leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables_qualified`] / [`Catalog::load_table`] for
    /// `project`.
    pub async fn tables(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let qtables = catalog.list_tables_qualified(project).await?;

        let mut catalogs: Vec<&str> = Vec::with_capacity(qtables.len());
        let mut schemas: Vec<String> = Vec::with_capacity(qtables.len());
        let mut table_names: Vec<String> = Vec::with_capacity(qtables.len());
        let mut table_types: Vec<&str> = Vec::with_capacity(qtables.len());

        // We need the per-table metadata to distinguish materialized
        // views from base tables. PG reports MVs as `'BASE TABLE'` in
        // `information_schema.tables` (the SQL-standard view doesn't
        // know about matviews); we mirror that for compatibility.
        for qt in &qtables {
            let _meta = catalog.load_table_qualified(project, qt).await?;
            catalogs.push(BASIN_CATALOG_NAME);
            schemas.push(qt.schema.as_str().to_string());
            table_names.push(qt.name.as_str().to_string());
            table_types.push(TABLE_TYPE_BASE_TABLE);
        }

        let schema = Self::tables_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(table_types)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.tables build: {e}")))
    }

    /// Build `pg_catalog.pg_class` filtered to `project`'s tables.
    ///
    /// Phase 5.18.D: uses [`Catalog::list_tables_qualified`] so each row's
    /// `relnamespace` points at the table's real schema oid (auth/storage/…)
    /// rather than always pointing at the public-schema oid.
    ///
    /// Cross-project leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables_qualified`] / [`Catalog::load_table`] for
    /// `project`.
    pub async fn pg_class(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let qtables = catalog.list_tables_qualified(project).await?;

        let mut oids: Vec<i64> = Vec::with_capacity(qtables.len());
        let mut relnames: Vec<String> = Vec::with_capacity(qtables.len());
        let mut namespaces: Vec<i64> = Vec::with_capacity(qtables.len());
        let mut relkinds: Vec<&str> = Vec::with_capacity(qtables.len());
        let mut rls: Vec<bool> = Vec::with_capacity(qtables.len());
        let mut partitioned: Vec<bool> = Vec::with_capacity(qtables.len());
        let mut reltuples: Vec<f32> = Vec::with_capacity(qtables.len());

        for qt in &qtables {
            let meta = catalog.load_table_qualified(project, qt).await?;
            oids.push(table_oid(project, &qt.name));
            relnames.push(qt.name.as_str().to_string());
            // Point relnamespace at the table's real schema, not always public.
            namespaces.push(namespace_oid_for(project, qt.schema.as_str()));
            // v0.1 only knows base tables and materialized views (the
            // continuous-aggregate flavour). Plain `CREATE VIEW` is not
            // shipped, so `'v'` is reserved for future use.
            relkinds.push(if meta.continuous_aggregate.is_some() {
                RELKIND_MATVIEW
            } else {
                RELKIND_TABLE
            });
            rls.push(meta.rls_enabled);
            partitioned.push(false);
            // Row-count estimate: sum row_count across the current
            // snapshot's data files. Genesis tables (no data files yet)
            // report 0.0, matching PG's "fresh table" estimate.
            let rows = meta
                .current()
                .map(|s| s.data_files.iter().map(|f| f.row_count).sum::<u64>())
                .unwrap_or(0);
            reltuples.push(rows as f32);
        }

        let schema = Self::pg_class_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(Int64Array::from(namespaces)),
            Arc::new(StringArray::from(relkinds)),
            Arc::new(BooleanArray::from(rls)),
            Arc::new(BooleanArray::from(partitioned)),
            Arc::new(Float32Array::from(reltuples)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_class build: {e}")))
    }

    /// Schema for `information_schema.columns` rows.
    ///
    /// One row per (table, column) belonging to the calling project. Column
    /// names match the SQL-standard / PG layout exactly so PostgREST,
    /// pgAdmin, and ORMs that probe `information_schema.columns` for
    /// type / nullability / ordering metadata receive what they expect.
    ///
    /// | column            | type    | notes                                  |
    /// |-------------------|---------|----------------------------------------|
    /// | table_catalog     | TEXT    | always `"basin"`                       |
    /// | table_schema      | TEXT    | always `"public"`                      |
    /// | table_name        | TEXT    | project-local table name                |
    /// | column_name       | TEXT    | column name                            |
    /// | ordinal_position  | INT     | 1-based                                |
    /// | column_default    | TEXT?   | default expression text or NULL        |
    /// | is_nullable       | TEXT    | `"YES"` / `"NO"` (PG style)            |
    /// | data_type         | TEXT    | PG type name (`"integer"`, `"text"`, …)|
    /// | udt_name          | TEXT    | underlying type (`"int4"`, `"text"`, …)|
    pub fn columns_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("udt_name", DataType::Utf8, false),
        ]))
    }

    /// Schema for `pg_catalog.pg_attribute` rows.
    ///
    /// One row per (table, column) belonging to the calling project.
    /// `attrelid` shares its hashing scheme with [`pg_class.oid`] so a
    /// JOIN between the two tables is direct.
    ///
    /// | column         | type    | notes                                   |
    /// |----------------|---------|-----------------------------------------|
    /// | attrelid       | BIGINT  | the table's pg_class oid                |
    /// | attname        | TEXT    | column name                             |
    /// | atttypid       | BIGINT  | PG type OID (23 = int4, 25 = text, …)   |
    /// | attnum         | SMALLINT| 1-based                                 |
    /// | attnotnull     | BOOL    | true if column is NOT NULL              |
    /// | atthasdef      | BOOL    | true if column has DEFAULT              |
    /// | attisdropped   | BOOL    | always false in v0.1                    |
    pub fn pg_attribute_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("attrelid", DataType::Int64, false),
            Field::new("attname", DataType::Utf8, false),
            Field::new("atttypid", DataType::Int64, false),
            Field::new("attnum", DataType::Int16, false),
            Field::new("attnotnull", DataType::Boolean, false),
            Field::new("atthasdef", DataType::Boolean, false),
            Field::new("attisdropped", DataType::Boolean, false),
        ]))
    }

    /// Schema for `pg_catalog.pg_namespace` rows.
    ///
    /// Phase 5.18.D: emits one row per reserved schema (all 8 variants of
    /// [`ReservedSchema`]) rather than only `"public"`. Stable oids are derived
    /// from `namespace_oid_for(project, schema_name)` so they are consistent
    /// with `pg_class.relnamespace`.
    ///
    /// | column     | type   | notes                                                   |
    /// |------------|--------|---------------------------------------------------------|
    /// | oid        | BIGINT | namespace oid (FNV-1a of `(project, schema_name)`)      |
    /// | nspname    | TEXT   | reserved schema name (`public`, `auth`, `storage`, …)   |
    /// | nspowner   | BIGINT | 0 (placeholder; v0.2 wires real owner)                  |
    pub fn pg_namespace_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("nspname", DataType::Utf8, false),
            Field::new("nspowner", DataType::Int64, false),
        ]))
    }

    /// Build `information_schema.columns` filtered to `project`'s tables.
    ///
    /// Cross-project leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `project`.
    pub async fn columns(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;

        let mut catalogs: Vec<&str> = Vec::new();
        let mut schemas: Vec<&str> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut column_names: Vec<String> = Vec::new();
        let mut ordinals: Vec<i32> = Vec::new();
        let mut defaults: Vec<Option<String>> = Vec::new();
        let mut is_nullable: Vec<&str> = Vec::new();
        let mut data_types: Vec<&'static str> = Vec::new();
        let mut udt_names: Vec<&'static str> = Vec::new();

        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            for (idx, field) in meta.schema.fields().iter().enumerate() {
                catalogs.push(BASIN_CATALOG_NAME);
                schemas.push(DEFAULT_SCHEMA);
                table_names.push(name.as_str().to_string());
                column_names.push(field.name().clone());
                ordinals.push((idx as i32) + 1);
                // The catalog only persists DEFAULT-expression text for
                // GENERATED columns today (in BASIN_GENERATED_AS field
                // metadata). Plain `DEFAULT 0` / `DEFAULT now()` columns
                // are not yet round-tripped through CREATE TABLE — see
                // `basin-engine::ddl`. Expose what we have; everything
                // else surfaces as NULL.
                defaults.push(
                    field
                        .metadata()
                        .get(BASIN_GENERATED_AS_KEY)
                        .map(|s| s.to_string()),
                );
                is_nullable.push(if field.is_nullable() { "YES" } else { "NO" });
                let (data_type, udt_name) = pg_type_for_field(field);
                data_types.push(data_type);
                udt_names.push(udt_name);
            }
        }

        let schema = Self::columns_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(column_names)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(StringArray::from(defaults)),
            Arc::new(StringArray::from(is_nullable)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(StringArray::from(udt_names)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.columns build: {e}")))
    }

    /// Build `pg_catalog.pg_attribute` filtered to `project`'s tables.
    ///
    /// Cross-project leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `project`.
    pub async fn pg_attribute(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;

        let mut attrelids: Vec<i64> = Vec::new();
        let mut attnames: Vec<String> = Vec::new();
        let mut atttypids: Vec<i64> = Vec::new();
        let mut attnums: Vec<i16> = Vec::new();
        let mut attnotnulls: Vec<bool> = Vec::new();
        let mut atthasdefs: Vec<bool> = Vec::new();
        let mut attisdroppeds: Vec<bool> = Vec::new();

        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let relid = table_oid(project, name);
            for (idx, field) in meta.schema.fields().iter().enumerate() {
                attrelids.push(relid);
                attnames.push(field.name().clone());
                atttypids.push(pg_type_oid_for_field(field));
                attnums.push((idx as i16) + 1);
                attnotnulls.push(!field.is_nullable());
                // v0.1 only persists default-expression text for
                // GENERATED columns (BASIN_GENERATED_AS metadata);
                // mirror what `information_schema.columns.column_default`
                // exposes.
                atthasdefs.push(field.metadata().contains_key(BASIN_GENERATED_AS_KEY));
                attisdroppeds.push(false);
            }
        }

        let schema = Self::pg_attribute_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(attrelids)),
            Arc::new(StringArray::from(attnames)),
            Arc::new(Int64Array::from(atttypids)),
            Arc::new(Int16Array::from(attnums)),
            Arc::new(BooleanArray::from(attnotnulls)),
            Arc::new(BooleanArray::from(atthasdefs)),
            Arc::new(BooleanArray::from(attisdroppeds)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_attribute build: {e}")))
    }

    /// Build `pg_catalog.pg_namespace` filtered to `project`.
    ///
    /// Phase 5.18.D: emits one row per reserved schema (all 8 variants of
    /// [`ReservedSchema`]) so PG tooling (pgAdmin, Prisma, PostgREST) sees
    /// `auth`, `storage`, `cron`, `net`, `realtime`, `pg_catalog`,
    /// `information_schema`, and `public` as distinct namespaces. Oids are
    /// stable across restarts (FNV-1a of `(project, schema_name)`).
    pub async fn pg_namespace(_catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let mut oids: Vec<i64> = Vec::with_capacity(ReservedSchema::ALL.len());
        let mut nspnames: Vec<String> = Vec::with_capacity(ReservedSchema::ALL.len());
        let mut nspowners: Vec<i64> = Vec::with_capacity(ReservedSchema::ALL.len());

        for &rs in ReservedSchema::ALL {
            oids.push(namespace_oid_for(project, rs.as_str()));
            nspnames.push(rs.as_str().to_string());
            nspowners.push(NSPOWNER_PLACEHOLDER);
        }

        let schema = Self::pg_namespace_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(nspnames)),
            Arc::new(Int64Array::from(nspowners)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_namespace build: {e}")))
    }

    /// Schema for `pg_catalog.pg_proc` rows.
    ///
    /// One row per user-defined function (`prokind = 'f'`) and procedure
    /// (`prokind = 'p'`) registered for the project.
    ///
    /// | column        | type    | notes                                     |
    /// |---------------|---------|-------------------------------------------|
    /// | oid           | BIGINT  | FNV-1a of `(project, name)` (same scheme)  |
    /// | proname       | TEXT    | function/procedure name                    |
    /// | pronamespace  | BIGINT  | `pg_namespace.oid` for `"public"`         |
    /// | prokind       | TEXT    | `'f'` for function, `'p'` for procedure    |
    /// | pronargs      | SMALLINT| argument count                            |
    /// | prorettype    | BIGINT  | return type OID; 0 for procedures          |
    /// | proargtypes   | TEXT    | space-separated arg type OIDs (oidvector) |
    /// | prosrc        | TEXT    | function/procedure body source             |
    /// | prolang       | BIGINT  | language oid (always 14 = SQL for v0.1)    |
    pub fn pg_proc_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("proname", DataType::Utf8, false),
            Field::new("pronamespace", DataType::Int64, false),
            Field::new("prokind", DataType::Utf8, false),
            Field::new("pronargs", DataType::Int16, false),
            Field::new("prorettype", DataType::Int64, false),
            Field::new("proargtypes", DataType::Utf8, false),
            Field::new("prosrc", DataType::Utf8, false),
            Field::new("prolang", DataType::Int64, false),
        ]))
    }

    /// Schema for `information_schema.routines` rows.
    ///
    /// One row per user-defined function (`routine_type = 'FUNCTION'`)
    /// and procedure (`routine_type = 'PROCEDURE'`).
    ///
    /// | column              | type      | notes                                     |
    /// |---------------------|-----------|-------------------------------------------|
    /// | specific_catalog    | TEXT      | always `"basin"`                          |
    /// | specific_schema     | TEXT      | always `"public"`                         |
    /// | specific_name       | TEXT      | name                                      |
    /// | routine_catalog     | TEXT      | always `"basin"`                          |
    /// | routine_schema      | TEXT      | always `"public"`                         |
    /// | routine_name        | TEXT      | function/procedure name                   |
    /// | routine_type        | TEXT      | `"FUNCTION"` or `"PROCEDURE"`             |
    /// | data_type           | TEXT?     | PG type name of return; NULL for procedure|
    /// | routine_body        | TEXT      | always `"SQL"`                            |
    /// | routine_definition  | TEXT      | body source                                |
    /// | external_language   | TEXT      | always `"SQL"`                            |
    /// | is_deterministic    | TEXT      | `"YES"` / `"NO"` heuristic                |
    pub fn routines_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("specific_catalog", DataType::Utf8, false),
            Field::new("specific_schema", DataType::Utf8, false),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("routine_catalog", DataType::Utf8, false),
            Field::new("routine_schema", DataType::Utf8, false),
            Field::new("routine_name", DataType::Utf8, false),
            Field::new("routine_type", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, true),
            Field::new("routine_body", DataType::Utf8, false),
            Field::new("routine_definition", DataType::Utf8, false),
            Field::new("external_language", DataType::Utf8, false),
            Field::new("is_deterministic", DataType::Utf8, false),
        ]))
    }

    /// Build `pg_catalog.pg_proc` filtered to `project`'s functions and
    /// procedures. Cross-project leak is a P0 invariant: only
    /// [`Catalog::list_sql_functions`] / [`Catalog::list_procedures`] for
    /// `project` are consulted.
    pub async fn pg_proc(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let funcs = catalog.list_sql_functions(project).await;
        let procs = catalog.list_procedures(project).await;
        let cap = funcs.len() + procs.len();

        let mut oids: Vec<i64> = Vec::with_capacity(cap);
        let mut pronames: Vec<String> = Vec::with_capacity(cap);
        let mut pronamespaces: Vec<i64> = Vec::with_capacity(cap);
        let mut prokinds: Vec<&str> = Vec::with_capacity(cap);
        let mut pronargs: Vec<i16> = Vec::with_capacity(cap);
        let mut prorettypes: Vec<i64> = Vec::with_capacity(cap);
        let mut proargtypes: Vec<String> = Vec::with_capacity(cap);
        let mut prosrcs: Vec<String> = Vec::with_capacity(cap);
        let mut prolangs: Vec<i64> = Vec::with_capacity(cap);

        let namespace_oid = namespace_oid_for(project, DEFAULT_SCHEMA);

        for f in &funcs {
            oids.push(routine_oid(project, &f.name));
            pronames.push(f.name.clone());
            pronamespaces.push(namespace_oid);
            prokinds.push(PROKIND_FUNCTION);
            pronargs.push(f.args.len() as i16);
            prorettypes.push(return_type_oid(&f.return_type));
            proargtypes.push(format_argtypes(f.args.iter().map(|a| a.data_type)));
            prosrcs.push(f.body.clone());
            prolangs.push(PROLANG_SQL);
        }
        for p in &procs {
            oids.push(routine_oid(project, &p.name));
            pronames.push(p.name.clone());
            pronamespaces.push(namespace_oid);
            prokinds.push(PROKIND_PROCEDURE);
            pronargs.push(p.args.len() as i16);
            prorettypes.push(0);
            proargtypes.push(format_argtypes(p.args.iter().map(|a| a.data_type)));
            prosrcs.push(p.body.clone());
            prolangs.push(PROLANG_SQL);
        }

        let schema = Self::pg_proc_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(pronames)),
            Arc::new(Int64Array::from(pronamespaces)),
            Arc::new(StringArray::from(prokinds)),
            Arc::new(Int16Array::from(pronargs)),
            Arc::new(Int64Array::from(prorettypes)),
            Arc::new(StringArray::from(proargtypes)),
            Arc::new(StringArray::from(prosrcs)),
            Arc::new(Int64Array::from(prolangs)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_proc build: {e}")))
    }

    /// Build `information_schema.routines` filtered to `project`'s
    /// functions and procedures. Cross-project leak is a P0 invariant:
    /// only [`Catalog::list_sql_functions`] / [`Catalog::list_procedures`]
    /// for `project` are consulted.
    pub async fn routines(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let funcs = catalog.list_sql_functions(project).await;
        let procs = catalog.list_procedures(project).await;
        let cap = funcs.len() + procs.len();

        let mut specific_catalogs: Vec<&str> = Vec::with_capacity(cap);
        let mut specific_schemas: Vec<&str> = Vec::with_capacity(cap);
        let mut specific_names: Vec<String> = Vec::with_capacity(cap);
        let mut routine_catalogs: Vec<&str> = Vec::with_capacity(cap);
        let mut routine_schemas: Vec<&str> = Vec::with_capacity(cap);
        let mut routine_names: Vec<String> = Vec::with_capacity(cap);
        let mut routine_types: Vec<&str> = Vec::with_capacity(cap);
        let mut data_types: Vec<Option<&'static str>> = Vec::with_capacity(cap);
        let mut routine_bodies: Vec<&str> = Vec::with_capacity(cap);
        let mut routine_definitions: Vec<String> = Vec::with_capacity(cap);
        let mut external_languages: Vec<&str> = Vec::with_capacity(cap);
        let mut is_deterministics: Vec<&str> = Vec::with_capacity(cap);

        for f in &funcs {
            specific_catalogs.push(BASIN_CATALOG_NAME);
            specific_schemas.push(DEFAULT_SCHEMA);
            specific_names.push(f.name.clone());
            routine_catalogs.push(BASIN_CATALOG_NAME);
            routine_schemas.push(DEFAULT_SCHEMA);
            routine_names.push(f.name.clone());
            routine_types.push(ROUTINE_TYPE_FUNCTION);
            data_types.push(Some(return_type_name(&f.return_type)));
            routine_bodies.push(ROUTINE_BODY_SQL);
            routine_definitions.push(f.body.clone());
            external_languages.push(EXTERNAL_LANGUAGE_SQL);
            is_deterministics.push(if is_deterministic_body(&f.body) {
                "YES"
            } else {
                "NO"
            });
        }
        for p in &procs {
            specific_catalogs.push(BASIN_CATALOG_NAME);
            specific_schemas.push(DEFAULT_SCHEMA);
            specific_names.push(p.name.clone());
            routine_catalogs.push(BASIN_CATALOG_NAME);
            routine_schemas.push(DEFAULT_SCHEMA);
            routine_names.push(p.name.clone());
            routine_types.push(ROUTINE_TYPE_PROCEDURE);
            data_types.push(None);
            routine_bodies.push(ROUTINE_BODY_SQL);
            routine_definitions.push(p.body.clone());
            external_languages.push(EXTERNAL_LANGUAGE_SQL);
            is_deterministics.push(if is_deterministic_body(&p.body) {
                "YES"
            } else {
                "NO"
            });
        }

        let schema = Self::routines_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(specific_catalogs)),
            Arc::new(StringArray::from(specific_schemas)),
            Arc::new(StringArray::from(specific_names)),
            Arc::new(StringArray::from(routine_catalogs)),
            Arc::new(StringArray::from(routine_schemas)),
            Arc::new(StringArray::from(routine_names)),
            Arc::new(StringArray::from(routine_types)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(StringArray::from(routine_bodies)),
            Arc::new(StringArray::from(routine_definitions)),
            Arc::new(StringArray::from(external_languages)),
            Arc::new(StringArray::from(is_deterministics)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.routines build: {e}")))
    }

    /// Schema for `pg_catalog.pg_index` rows.
    ///
    /// Basin v0.1 has no user-defined indexes (Phase 5.7 B1 secondary
    /// indexes are queued); this view always returns zero rows. The
    /// schema is shaped exactly like PG's `pg_index` so introspection
    /// clients (PostgREST, pgAdmin) that probe `pg_index` succeed and
    /// see an empty result, rather than failing on a missing relation.
    /// Once 5.7 B1 ships, the row-builder will read
    /// [`TableMetadata::indexes`] and emit one row per declared index.
    ///
    /// | column        | type     | notes                                 |
    /// |---------------|----------|---------------------------------------|
    /// | indexrelid    | BIGINT   | always 0 (no indexes in v0.1)         |
    /// | indrelid      | BIGINT   | parent table's pg_class oid           |
    /// | indnatts      | SMALLINT | column count                           |
    /// | indisunique   | BOOL     |                                        |
    /// | indisprimary  | BOOL     |                                        |
    /// | indkey        | TEXT     | space-separated column attnums         |
    pub fn pg_index_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("indexrelid", DataType::Int64, false),
            Field::new("indrelid", DataType::Int64, false),
            Field::new("indnatts", DataType::Int16, false),
            Field::new("indisunique", DataType::Boolean, false),
            Field::new("indisprimary", DataType::Boolean, false),
            Field::new("indkey", DataType::Utf8, false),
        ]))
    }

    /// Build `pg_catalog.pg_index` filtered to `project`. Always empty in
    /// v0.1 (no user-defined indexes). The `project` argument is held for
    /// signature stability against the v0.2 expansion that will read
    /// [`TableMetadata::indexes`].
    pub async fn pg_index(_catalog: &dyn Catalog, _project: &ProjectId) -> Result<RecordBatch> {
        let schema = Self::pg_index_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int16Array::from(Vec::<i16>::new())),
            Arc::new(BooleanArray::from(Vec::<bool>::new())),
            Arc::new(BooleanArray::from(Vec::<bool>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_index build: {e}")))
    }

    /// Schema for `pg_catalog.pg_constraint` rows.
    ///
    /// Basin v0.1 captures NOT NULL as Arrow column metadata only; FOREIGN
    /// KEY (queued in TASK.md), explicit PRIMARY KEY (unenforced in v0.1),
    /// CHECK, and UNIQUE constraint surfaces aren't yet shipped. This view
    /// returns zero rows for v0.1 and will expand as those surfaces land.
    ///
    /// | column        | type   | notes                                     |
    /// |---------------|--------|-------------------------------------------|
    /// | oid           | BIGINT | FNV-1a of `(project, table, conname)`      |
    /// | conname       | TEXT   | constraint name                            |
    /// | connamespace  | BIGINT | namespace oid (matches pg_namespace)       |
    /// | contype       | TEXT   | `'p'`/`'f'`/`'c'`/`'u'`/`'n'`              |
    /// | conrelid      | BIGINT | table's pg_class oid                       |
    /// | conkey        | TEXT   | space-separated column attnums              |
    /// | confrelid     | BIGINT | referenced table oid (FK only); 0 otherwise |
    /// | confkey       | TEXT   | referenced column attnums (FK only); empty  |
    pub fn pg_constraint_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("conname", DataType::Utf8, false),
            Field::new("connamespace", DataType::Int64, false),
            Field::new("contype", DataType::Utf8, false),
            Field::new("conrelid", DataType::Int64, false),
            Field::new("conkey", DataType::Utf8, false),
            Field::new("confrelid", DataType::Int64, false),
            Field::new("confkey", DataType::Utf8, false),
        ]))
    }

    /// Build `pg_catalog.pg_constraint` filtered to `project`. Emits one
    /// row per constraint declared on each project-owned table:
    ///
    /// - `contype = 'p'` for the PRIMARY KEY (one row per table that
    ///   has a PK), named `<table>_pkey`. `conkey` is the
    ///   space-separated 1-based attnums of the PK columns in order.
    /// - `contype = 'f'` for each FOREIGN KEY, named `<table>_<col>_fkey`
    ///   (or the user-supplied `CONSTRAINT <name>`). `conkey` is the
    ///   local attnums; `confrelid` is the referenced table's oid;
    ///   `confkey` is the referenced columns' attnums on that table.
    /// - `contype = 'c'` for each CHECK constraint.
    /// - `contype = 'n'` for each NOT NULL column. PG itself doesn't
    ///   surface NOT NULL in `pg_constraint` (it's a column attribute),
    ///   but Basin advertises it here so PostgREST / pgAdmin can list
    ///   every constraint surface in one view.
    ///
    /// Cross-project leak is a P0 invariant: only [`Catalog::list_tables`]
    /// / [`Catalog::load_table`] for `project`.
    pub async fn pg_constraint(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;
        let namespace_oid = namespace_oid_for(project, DEFAULT_SCHEMA);

        let mut oids: Vec<i64> = Vec::new();
        let mut connames: Vec<String> = Vec::new();
        let mut connamespaces: Vec<i64> = Vec::new();
        let mut contypes: Vec<&'static str> = Vec::new();
        let mut conrelids: Vec<i64> = Vec::new();
        let mut conkeys: Vec<String> = Vec::new();
        let mut confrelids: Vec<i64> = Vec::new();
        let mut confkeys: Vec<String> = Vec::new();

        // Pre-load metadata for every table so we can resolve FK target
        // attnums by looking the referenced table back up. The map is
        // keyed on the bare name as written in the FK; the engine has
        // already validated cross-project FKs are rejected.
        let mut metas: std::collections::HashMap<String, crate::metadata::TableMetadata> =
            std::collections::HashMap::with_capacity(names.len());
        for name in &names {
            let m = catalog.load_table(project, name).await?;
            metas.insert(name.as_str().to_string(), m);
        }

        let push = |conname: String,
                    contype: &'static str,
                    conrelid: i64,
                    conkey: String,
                    confrelid: i64,
                    confkey: String,
                    oids: &mut Vec<i64>,
                    connames: &mut Vec<String>,
                    connamespaces: &mut Vec<i64>,
                    contypes: &mut Vec<&'static str>,
                    conrelids: &mut Vec<i64>,
                    conkeys: &mut Vec<String>,
                    confrelids: &mut Vec<i64>,
                    confkeys: &mut Vec<String>| {
            let oid_key = format!("basin.pg_constraint:{project}:{}:{}", conrelid, &conname);
            oids.push(fnv1a_64_to_positive_i64(oid_key.as_bytes()));
            connames.push(conname);
            connamespaces.push(namespace_oid);
            contypes.push(contype);
            conrelids.push(conrelid);
            conkeys.push(conkey);
            confrelids.push(confrelid);
            confkeys.push(confkey);
        };

        for name in &names {
            let meta = &metas[name.as_str()];
            let relid = table_oid(project, name);

            // PRIMARY KEY (one row per table with a PK).
            if !meta.pk_columns.is_empty() {
                let conkey = meta
                    .pk_columns
                    .iter()
                    .filter_map(|c| attnum_in_schema(&meta.schema, c))
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                push(
                    pk_constraint_name(name),
                    CONTYPE_PRIMARY_KEY,
                    relid,
                    conkey,
                    0,
                    String::new(),
                    &mut oids,
                    &mut connames,
                    &mut connamespaces,
                    &mut contypes,
                    &mut conrelids,
                    &mut conkeys,
                    &mut confrelids,
                    &mut confkeys,
                );
            }

            // FOREIGN KEY (one row each).
            for fk in &meta.foreign_keys {
                let conkey = fk
                    .columns
                    .iter()
                    .filter_map(|c| attnum_in_schema(&meta.schema, c))
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                let ref_table = TableName::new(fk.ref_table.clone())
                    .map_err(|e| BasinError::internal(format!("FK ref_table {e}")))?;
                let confrelid = table_oid(project, &ref_table);
                let confkey = if let Some(ref_meta) = metas.get(&fk.ref_table) {
                    fk.ref_columns
                        .iter()
                        .filter_map(|c| attnum_in_schema(&ref_meta.schema, c))
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(" ")
                } else {
                    String::new()
                };
                push(
                    fk.name.clone(),
                    CONTYPE_FOREIGN_KEY,
                    relid,
                    conkey,
                    confrelid,
                    confkey,
                    &mut oids,
                    &mut connames,
                    &mut connamespaces,
                    &mut contypes,
                    &mut conrelids,
                    &mut conkeys,
                    &mut confrelids,
                    &mut confkeys,
                );
            }

            // CHECK (one row each). conkey is left empty — PG fills in
            // referenced attnums via parse of `consrc`, which Basin
            // doesn't track per-column for CHECK predicates yet.
            for chk in &meta.check_constraints {
                push(
                    chk.name.clone(),
                    CONTYPE_CHECK,
                    relid,
                    String::new(),
                    0,
                    String::new(),
                    &mut oids,
                    &mut connames,
                    &mut connamespaces,
                    &mut contypes,
                    &mut conrelids,
                    &mut conkeys,
                    &mut confrelids,
                    &mut confkeys,
                );
            }

            // NOT NULL (one row per non-nullable column). Optional under
            // PG's spec; emitted here so the introspection surface has
            // a single source of truth for "what constraints exist on
            // this table".
            for field in meta.schema.fields().iter() {
                if !field.is_nullable() {
                    let attnum = attnum_in_schema(&meta.schema, field.name())
                        .map(|n| n.to_string())
                        .unwrap_or_default();
                    push(
                        not_null_constraint_name(name, field.name()),
                        CONTYPE_NOT_NULL,
                        relid,
                        attnum,
                        0,
                        String::new(),
                        &mut oids,
                        &mut connames,
                        &mut connamespaces,
                        &mut contypes,
                        &mut conrelids,
                        &mut conkeys,
                        &mut confrelids,
                        &mut confkeys,
                    );
                }
            }
        }

        let schema = Self::pg_constraint_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(connames)),
            Arc::new(Int64Array::from(connamespaces)),
            Arc::new(StringArray::from(contypes)),
            Arc::new(Int64Array::from(conrelids)),
            Arc::new(StringArray::from(conkeys)),
            Arc::new(Int64Array::from(confrelids)),
            Arc::new(StringArray::from(confkeys)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_constraint build: {e}")))
    }

    /// Schema for `information_schema.views` rows.
    ///
    /// One row per Basin continuous materialized view (5.11.D2). Plain
    /// `CREATE VIEW` is not in v0.1; `CREATE MATERIALIZED VIEW` paired
    /// with the `basin.continuous` flag is the only path that registers
    /// a row here.
    ///
    /// | column             | type | notes                                  |
    /// |--------------------|------|----------------------------------------|
    /// | table_catalog      | TEXT | always `"basin"`                       |
    /// | table_schema       | TEXT | always `"public"`                      |
    /// | table_name         | TEXT | the matview name                       |
    /// | view_definition    | TEXT | the matview's stored SELECT body       |
    /// | check_option       | TEXT | always `"NONE"`                        |
    /// | is_updatable       | TEXT | always `"NO"`                          |
    /// | is_insertable_into | TEXT | always `"NO"`                          |
    pub fn views_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("view_definition", DataType::Utf8, false),
            Field::new("check_option", DataType::Utf8, false),
            Field::new("is_updatable", DataType::Utf8, false),
            Field::new("is_insertable_into", DataType::Utf8, false),
        ]))
    }

    /// Build `information_schema.views` filtered to `project`.
    ///
    /// Returns one row per:
    /// - Plain view registered via `CREATE VIEW … AS SELECT …`
    ///   ([`Catalog::list_views`]).
    /// - Continuous materialized view (`CREATE MATERIALIZED VIEW … WITH
    ///   (basin.continuous, …)`) — same as before.
    ///
    /// Cross-project leak is a P0 invariant: only APIs scoped to `project`
    /// are consulted.
    pub async fn views(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;

        let mut catalogs: Vec<&str> = Vec::new();
        let mut schemas: Vec<&str> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut definitions: Vec<String> = Vec::new();
        let mut check_options: Vec<&str> = Vec::new();
        let mut updatables: Vec<&str> = Vec::new();
        let mut insertables: Vec<&str> = Vec::new();

        // 1. Continuous materialized views (existing path).
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let Some(cv) = meta.continuous_aggregate.as_ref() else {
                continue;
            };
            catalogs.push(BASIN_CATALOG_NAME);
            schemas.push(DEFAULT_SCHEMA);
            table_names.push(name.as_str().to_string());
            definitions.push(cv.query_sql.clone());
            check_options.push(VIEW_CHECK_OPTION_NONE);
            updatables.push(VIEW_FLAG_NO);
            insertables.push(VIEW_FLAG_NO);
        }

        // 2. Plain views registered via CREATE VIEW.
        let plain_views = catalog.list_views(project).await;
        for v in plain_views {
            catalogs.push(BASIN_CATALOG_NAME);
            schemas.push(DEFAULT_SCHEMA);
            table_names.push(v.name.clone());
            definitions.push(v.query_sql.clone());
            check_options.push(VIEW_CHECK_OPTION_NONE);
            updatables.push(VIEW_FLAG_NO);
            insertables.push(VIEW_FLAG_NO);
        }

        let schema = Self::views_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(definitions)),
            Arc::new(StringArray::from(check_options)),
            Arc::new(StringArray::from(updatables)),
            Arc::new(StringArray::from(insertables)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.views build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_views
    // -----------------------------------------------------------------------

    /// Schema for `pg_catalog.pg_views` rows.
    ///
    /// | column      | type | notes                              |
    /// |-------------|------|------------------------------------|
    /// | schemaname  | TEXT | always `"public"`                  |
    /// | viewname    | TEXT | the view name                      |
    /// | viewowner   | TEXT | `""` placeholder (v0.2 wires auth) |
    /// | definition  | TEXT | the stored SELECT body             |
    pub fn pg_views_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("viewname", DataType::Utf8, false),
            Field::new("viewowner", DataType::Utf8, false),
            Field::new("definition", DataType::Utf8, false),
        ]))
    }

    /// Build `pg_catalog.pg_views` filtered to `project`. One row per plain
    /// view registered via `CREATE VIEW … AS SELECT …` plus one row per
    /// continuous materialized view.
    pub async fn pg_views(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;

        let mut schema_names: Vec<&str> = Vec::new();
        let mut view_names: Vec<String> = Vec::new();
        let mut owners: Vec<&str> = Vec::new();
        let mut definitions: Vec<String> = Vec::new();

        // Continuous materialized views.
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let Some(cv) = meta.continuous_aggregate.as_ref() else {
                continue;
            };
            schema_names.push(DEFAULT_SCHEMA);
            view_names.push(name.as_str().to_string());
            owners.push("");
            definitions.push(cv.query_sql.clone());
        }

        // Plain views.
        let plain_views = catalog.list_views(project).await;
        for v in plain_views {
            schema_names.push(DEFAULT_SCHEMA);
            view_names.push(v.name.clone());
            owners.push("");
            definitions.push(v.query_sql.clone());
        }

        let schema = Self::pg_views_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(schema_names)),
            Arc::new(StringArray::from(view_names)),
            Arc::new(StringArray::from(owners)),
            Arc::new(StringArray::from(definitions)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_views build: {e}")))
    }

    /// Schema for `information_schema.schemata` rows.
    ///
    /// Phase 5.18.D: emits one row per reserved schema (all 8 variants of
    /// [`ReservedSchema`]) so PG tooling sees the full set of system schemas.
    /// Previously emitted only `"public"`.
    ///
    /// | column                          | type  | notes                                    |
    /// |---------------------------------|-------|------------------------------------------|
    /// | catalog_name                    | TEXT  | always `"basin"`                         |
    /// | schema_name                     | TEXT  | reserved schema name (public/auth/…)     |
    /// | schema_owner                    | TEXT  | `""` placeholder (v0.2 wires)            |
    /// | default_character_set_catalog   | TEXT? | NULL                                     |
    /// | default_character_set_schema    | TEXT? | NULL                                     |
    /// | default_character_set_name      | TEXT? | NULL                                     |
    pub fn schemata_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("schema_owner", DataType::Utf8, false),
            Field::new("default_character_set_catalog", DataType::Utf8, true),
            Field::new("default_character_set_schema", DataType::Utf8, true),
            Field::new("default_character_set_name", DataType::Utf8, true),
        ]))
    }

    /// Build `information_schema.schemata` filtered to `project`.
    ///
    /// Phase 5.18.D: emits one row per reserved schema so PG tooling sees all
    /// system schemas (`public`, `auth`, `storage`, `cron`, `net`, `realtime`,
    /// `pg_catalog`, `information_schema`) in one view. Previously emitted
    /// only `"public"`.
    pub async fn schemata(_catalog: &dyn Catalog, _project: &ProjectId) -> Result<RecordBatch> {
        let n = ReservedSchema::ALL.len();
        let mut catalog_names: Vec<&str> = Vec::with_capacity(n);
        let mut schema_names: Vec<String> = Vec::with_capacity(n);
        let mut schema_owners: Vec<&str> = Vec::with_capacity(n);
        let mut charset_catalogs: Vec<Option<String>> = Vec::with_capacity(n);
        let mut charset_schemas: Vec<Option<String>> = Vec::with_capacity(n);
        let mut charset_names: Vec<Option<String>> = Vec::with_capacity(n);

        for &rs in ReservedSchema::ALL {
            catalog_names.push(BASIN_CATALOG_NAME);
            schema_names.push(rs.as_str().to_string());
            schema_owners.push(SCHEMA_OWNER_PLACEHOLDER);
            charset_catalogs.push(None);
            charset_schemas.push(None);
            charset_names.push(None);
        }

        let schema = Self::schemata_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(catalog_names)),
            Arc::new(StringArray::from(schema_names)),
            Arc::new(StringArray::from(schema_owners)),
            Arc::new(StringArray::from(charset_catalogs)),
            Arc::new(StringArray::from(charset_schemas)),
            Arc::new(StringArray::from(charset_names)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.schemata build: {e}")))
    }

    /// Schema for `information_schema.table_constraints` rows.
    ///
    /// One row per declared constraint visible to the calling project. v0.1
    /// only persists `NOT NULL` constraints (carried by Arrow field
    /// nullability); `PRIMARY KEY`, `FOREIGN KEY`, `CHECK`, and `UNIQUE`
    /// are rejected at parse time today (see `basin_engine::ddl`) and so
    /// emit zero rows. The column shape still matches PG so PostgREST /
    /// pgAdmin queries don't error on missing columns.
    ///
    /// | column            | type | notes                                              |
    /// |-------------------|------|----------------------------------------------------|
    /// | constraint_catalog| TEXT | always `"basin"`                                   |
    /// | constraint_schema | TEXT | always `"public"`                                  |
    /// | constraint_name   | TEXT | derived (`<table>_<column>_not_null` for NOT NULL) |
    /// | table_catalog     | TEXT | always `"basin"`                                   |
    /// | table_schema      | TEXT | always `"public"`                                  |
    /// | table_name        | TEXT | project-local table name                            |
    /// | constraint_type   | TEXT | `"NOT NULL"` / `"PRIMARY KEY"` / `"FOREIGN KEY"` / `"CHECK"` / `"UNIQUE"` |
    /// | is_deferrable     | TEXT | always `"NO"` in v0.1                              |
    /// | initially_deferred| TEXT | always `"NO"` in v0.1                              |
    pub fn table_constraints_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("constraint_type", DataType::Utf8, false),
            Field::new("is_deferrable", DataType::Utf8, false),
            Field::new("initially_deferred", DataType::Utf8, false),
        ]))
    }

    /// Schema for `information_schema.key_column_usage` rows.
    ///
    /// PG semantics: this view describes columns that participate in
    /// uniqueness or foreign-key constraints. NOT NULL is intentionally
    /// not represented here — that's `table_constraints` territory only.
    /// v0.1 ships zero PK / FK / UNIQUE constraints, so this view is
    /// always empty. Columns will populate once PK enforcement lands.
    ///
    /// | column                         | type   | notes                       |
    /// |--------------------------------|--------|-----------------------------|
    /// | constraint_catalog             | TEXT   | always `"basin"`            |
    /// | constraint_schema              | TEXT   | always `"public"`           |
    /// | constraint_name                | TEXT   | matches table_constraints   |
    /// | table_catalog                  | TEXT   | always `"basin"`            |
    /// | table_schema                   | TEXT   | always `"public"`           |
    /// | table_name                     | TEXT   |                             |
    /// | column_name                    | TEXT   |                             |
    /// | ordinal_position               | INT4   | 1-based within constraint   |
    /// | position_in_unique_constraint  | INT4?  | NULL for non-FK             |
    pub fn key_column_usage_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("position_in_unique_constraint", DataType::Int32, true),
        ]))
    }

    /// Schema for `information_schema.referential_constraints` rows.
    ///
    /// PG semantics: one row per FOREIGN KEY constraint. v0.1 doesn't
    /// support FOREIGN KEY (queued single-shard work) so this view is
    /// always empty; the column shape still matches PG so PostgREST
    /// auto-discovery doesn't choke.
    ///
    /// | column                       | type | notes                |
    /// |------------------------------|------|----------------------|
    /// | constraint_catalog           | TEXT | always `"basin"`     |
    /// | constraint_schema            | TEXT | always `"public"`    |
    /// | constraint_name              | TEXT |                      |
    /// | unique_constraint_catalog    | TEXT |                      |
    /// | unique_constraint_schema     | TEXT |                      |
    /// | unique_constraint_name       | TEXT |                      |
    /// | match_option                 | TEXT | always `"NONE"`      |
    /// | update_rule                  | TEXT | always `"NO ACTION"` |
    /// | delete_rule                  | TEXT | always `"NO ACTION"` |
    pub fn referential_constraints_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("unique_constraint_catalog", DataType::Utf8, false),
            Field::new("unique_constraint_schema", DataType::Utf8, false),
            Field::new("unique_constraint_name", DataType::Utf8, false),
            Field::new("match_option", DataType::Utf8, false),
            Field::new("update_rule", DataType::Utf8, false),
            Field::new("delete_rule", DataType::Utf8, false),
        ]))
    }

    /// Build `information_schema.table_constraints` filtered to `project`.
    ///
    /// v0.1 only emits `NOT NULL` constraint rows: one per non-nullable
    /// column on each project-owned table. PK / FK / CHECK / UNIQUE are
    /// queued (the parser rejects them today; see `basin_engine::ddl`)
    /// and so contribute zero rows. The constraint name follows the
    /// `<table>_<column>_not_null` convention — pgwire-introspecting
    /// clients (PostgREST, pgAdmin) only need stable names within a
    /// project, not PG-byte-identical ones.
    ///
    /// Cross-project leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `project`.
    pub async fn table_constraints(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;

        let mut constraint_catalogs: Vec<&str> = Vec::new();
        let mut constraint_schemas: Vec<&str> = Vec::new();
        let mut constraint_names: Vec<String> = Vec::new();
        let mut table_catalogs: Vec<&str> = Vec::new();
        let mut table_schemas: Vec<&str> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut constraint_types: Vec<&str> = Vec::new();
        let mut is_deferrables: Vec<&str> = Vec::new();
        let mut initially_deferreds: Vec<&str> = Vec::new();

        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let mut push = |conname: String, ctype: &'static str| {
                constraint_catalogs.push(BASIN_CATALOG_NAME);
                constraint_schemas.push(DEFAULT_SCHEMA);
                constraint_names.push(conname);
                table_catalogs.push(BASIN_CATALOG_NAME);
                table_schemas.push(DEFAULT_SCHEMA);
                table_names.push(name.as_str().to_string());
                constraint_types.push(ctype);
                is_deferrables.push(CONSTRAINT_NO);
                initially_deferreds.push(CONSTRAINT_NO);
            };

            // PRIMARY KEY (one row per table with a PK).
            if !meta.pk_columns.is_empty() {
                push(pk_constraint_name(name), CONSTRAINT_TYPE_PRIMARY_KEY);
            }

            // FOREIGN KEY (one row each).
            for fk in &meta.foreign_keys {
                push(fk.name.clone(), CONSTRAINT_TYPE_FOREIGN_KEY);
            }

            // CHECK (one row each).
            for chk in &meta.check_constraints {
                push(chk.name.clone(), CONSTRAINT_TYPE_CHECK);
            }

            // NOT NULL (one row per non-nullable column).
            for field in meta.schema.fields().iter() {
                if !field.is_nullable() {
                    push(
                        not_null_constraint_name(name, field.name()),
                        CONSTRAINT_TYPE_NOT_NULL,
                    );
                }
            }
        }

        let schema = Self::table_constraints_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(constraint_catalogs)),
            Arc::new(StringArray::from(constraint_schemas)),
            Arc::new(StringArray::from(constraint_names)),
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(constraint_types)),
            Arc::new(StringArray::from(is_deferrables)),
            Arc::new(StringArray::from(initially_deferreds)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.table_constraints build: {e}")))
    }

    /// Build `information_schema.key_column_usage` filtered to `project`.
    /// PG semantics: one row per column that participates in a UNIQUE /
    /// PRIMARY KEY / FOREIGN KEY constraint. NOT NULL is intentionally
    /// excluded — that's `table_constraints` territory.
    ///
    /// `ordinal_position` is 1-based within the constraint, not within
    /// the table; the test pin `key_column_usage_lists_pk_and_fk_columns`
    /// expects a composite PK `(order_id, item_id)` to render as
    /// positions `[1, 2]`.
    ///
    /// `position_in_unique_constraint` is set on FK rows to the matching
    /// position in the referenced table's PK; NULL elsewhere.
    ///
    /// Cross-project leak is a P0 invariant: only [`Catalog::list_tables`]
    /// / [`Catalog::load_table`] for `project`.
    pub async fn key_column_usage(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;

        let mut constraint_catalogs: Vec<&str> = Vec::new();
        let mut constraint_schemas: Vec<&str> = Vec::new();
        let mut constraint_names: Vec<String> = Vec::new();
        let mut table_catalogs: Vec<&str> = Vec::new();
        let mut table_schemas: Vec<&str> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut column_names: Vec<String> = Vec::new();
        let mut ordinal_positions: Vec<i32> = Vec::new();
        let mut position_in_unique: Vec<Option<i32>> = Vec::new();

        for name in &names {
            let meta = catalog.load_table(project, name).await?;

            // PK columns.
            if !meta.pk_columns.is_empty() {
                let pkname = pk_constraint_name(name);
                for (i, col) in meta.pk_columns.iter().enumerate() {
                    constraint_catalogs.push(BASIN_CATALOG_NAME);
                    constraint_schemas.push(DEFAULT_SCHEMA);
                    constraint_names.push(pkname.clone());
                    table_catalogs.push(BASIN_CATALOG_NAME);
                    table_schemas.push(DEFAULT_SCHEMA);
                    table_names.push(name.as_str().to_string());
                    column_names.push(col.clone());
                    ordinal_positions.push((i + 1) as i32);
                    position_in_unique.push(None);
                }
            }

            // FK local columns.
            for fk in &meta.foreign_keys {
                for (i, col) in fk.columns.iter().enumerate() {
                    constraint_catalogs.push(BASIN_CATALOG_NAME);
                    constraint_schemas.push(DEFAULT_SCHEMA);
                    constraint_names.push(fk.name.clone());
                    table_catalogs.push(BASIN_CATALOG_NAME);
                    table_schemas.push(DEFAULT_SCHEMA);
                    table_names.push(name.as_str().to_string());
                    column_names.push(col.clone());
                    ordinal_positions.push((i + 1) as i32);
                    // Position of this local column's referenced peer
                    // within the unique constraint it points at — same
                    // index since we require FK column count to match
                    // referenced PK column count.
                    position_in_unique.push(Some((i + 1) as i32));
                }
            }
        }

        let schema = Self::key_column_usage_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(constraint_catalogs)),
            Arc::new(StringArray::from(constraint_schemas)),
            Arc::new(StringArray::from(constraint_names)),
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(column_names)),
            Arc::new(Int32Array::from(ordinal_positions)),
            Arc::new(Int32Array::from(position_in_unique)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.key_column_usage build: {e}")))
    }

    /// Schema for `pg_catalog.pg_type` rows.
    ///
    /// Static catalog of the PG built-in types Basin's pgwire layer
    /// advertises. The row set is fixed (no user-defined types in v0.1 —
    /// enums/domains are tracked separately) and shared across projects;
    /// `typnamespace` is the only per-project column (FNV-1a hash of
    /// `(project, "pg_catalog")`). pgAdmin's column-detail query joins
    /// against this view to render PG type names.
    ///
    /// | column        | type    | notes                                     |
    /// |---------------|---------|-------------------------------------------|
    /// | oid           | BIGINT  | PG type OID (16=bool, 23=int4, 25=text…)  |
    /// | typname       | TEXT    | PG type name (`bool`, `int4`, `text`, …)  |
    /// | typnamespace  | BIGINT  | hash of `(project, "pg_catalog")`          |
    /// | typtype       | TEXT    | `'b'` base type (all v0.1 entries)        |
    /// | typcategory   | TEXT    | PG single-letter category (`B`/`N`/`S`/…) |
    /// | typlen        | SMALLINT| length in bytes; `-1` for variable-length |
    /// | typbyval      | BOOL    | true for fixed-size pass-by-value types   |
    pub fn pg_type_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::Int64, false),
            Field::new("typtype", DataType::Utf8, false),
            Field::new("typcategory", DataType::Utf8, false),
            Field::new("typlen", DataType::Int16, false),
            Field::new("typbyval", DataType::Boolean, false),
        ]))
    }

    /// Build `pg_catalog.pg_type`. Row set is the static [`BASIN_PG_TYPES`]
    /// table; `typnamespace` is hashed per-project against the
    /// `"pg_catalog"` schema so a JOIN against `pg_namespace` works the
    /// same way as `pg_class.relnamespace` / `pg_proc.pronamespace`.
    /// `catalog` is unused for v0.1 (no user-defined types in pg_type yet)
    /// and held for signature stability against the v0.2 expansion that
    /// will read project-defined enum / domain types.
    pub async fn pg_type(_catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let n = BASIN_PG_TYPES.len();
        let mut oids: Vec<i64> = Vec::with_capacity(n);
        let mut typnames: Vec<&'static str> = Vec::with_capacity(n);
        let mut typnamespaces: Vec<i64> = Vec::with_capacity(n);
        let mut typtypes: Vec<&'static str> = Vec::with_capacity(n);
        let mut typcategories: Vec<&'static str> = Vec::with_capacity(n);
        let mut typlens: Vec<i16> = Vec::with_capacity(n);
        let mut typbyvals: Vec<bool> = Vec::with_capacity(n);

        let nsp = namespace_oid_for(project, PG_CATALOG_SCHEMA);

        for (oid, typname, typtype, typcategory, typlen, typbyval) in BASIN_PG_TYPES {
            oids.push(*oid);
            typnames.push(typname);
            typnamespaces.push(nsp);
            typtypes.push(typtype);
            typcategories.push(typcategory);
            typlens.push(*typlen);
            typbyvals.push(*typbyval);
        }

        let schema = Self::pg_type_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(oids)),
            Arc::new(StringArray::from(typnames)),
            Arc::new(Int64Array::from(typnamespaces)),
            Arc::new(StringArray::from(typtypes)),
            Arc::new(StringArray::from(typcategories)),
            Arc::new(Int16Array::from(typlens)),
            Arc::new(BooleanArray::from(typbyvals)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_type build: {e}")))
    }

    /// Build `information_schema.referential_constraints` filtered to
    /// `project`. One row per FOREIGN KEY. `update_rule` / `delete_rule`
    /// reflect the FK's [`crate::metadata::RefAction`] mapped to PG's
    /// action strings (`"NO ACTION"` / `"CASCADE"`). `match_option` is
    /// always `"NONE"` — Basin doesn't model `MATCH PARTIAL` / `MATCH
    /// FULL`. `unique_constraint_name` points at the referenced table's
    /// PK constraint (`<ref_table>_pkey`) since v0.1 only allows FKs
    /// against PKs.
    ///
    /// Cross-project leak is a P0 invariant: only [`Catalog::list_tables`]
    /// / [`Catalog::load_table`] for `project`.
    pub async fn referential_constraints(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;

        let mut constraint_catalogs: Vec<&str> = Vec::new();
        let mut constraint_schemas: Vec<&str> = Vec::new();
        let mut constraint_names: Vec<String> = Vec::new();
        let mut unique_catalogs: Vec<&str> = Vec::new();
        let mut unique_schemas: Vec<&str> = Vec::new();
        let mut unique_names: Vec<String> = Vec::new();
        let mut match_options: Vec<&str> = Vec::new();
        let mut update_rules: Vec<&'static str> = Vec::new();
        let mut delete_rules: Vec<&'static str> = Vec::new();

        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            for fk in &meta.foreign_keys {
                let ref_table = TableName::new(fk.ref_table.clone())
                    .map_err(|e| BasinError::internal(format!("FK ref_table {e}")))?;
                constraint_catalogs.push(BASIN_CATALOG_NAME);
                constraint_schemas.push(DEFAULT_SCHEMA);
                constraint_names.push(fk.name.clone());
                unique_catalogs.push(BASIN_CATALOG_NAME);
                unique_schemas.push(DEFAULT_SCHEMA);
                unique_names.push(pk_constraint_name(&ref_table));
                match_options.push(MATCH_OPTION_NONE);
                update_rules.push(ref_action_to_pg(fk.on_update));
                delete_rules.push(ref_action_to_pg(fk.on_delete));
            }
        }

        let schema = Self::referential_constraints_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(constraint_catalogs)),
            Arc::new(StringArray::from(constraint_schemas)),
            Arc::new(StringArray::from(constraint_names)),
            Arc::new(StringArray::from(unique_catalogs)),
            Arc::new(StringArray::from(unique_schemas)),
            Arc::new(StringArray::from(unique_names)),
            Arc::new(StringArray::from(match_options)),
            Arc::new(StringArray::from(update_rules)),
            Arc::new(StringArray::from(delete_rules)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("info_schema.referential_constraints build: {e}"))
        })
    }

    /// Schema for `pg_catalog.pg_depend` rows.
    ///
    /// Records dependency edges between catalog objects. v0.1 surfaces:
    ///
    /// - Continuous matview → source-table edges (one row per CV when the
    ///   source table is owned by the same project).
    /// - Function → return type / argument type edges (one row per arg
    ///   plus one row per non-zero return type, per registered function).
    ///
    /// Procedures, indexes, constraints, and other PG dependency edges are
    /// queued — the schema shape stays stable so the v0.2 row-builder
    /// expansion is non-breaking.
    ///
    /// | column        | type   | notes                                    |
    /// |---------------|--------|------------------------------------------|
    /// | classid       | BIGINT | catalog oid of dependent (synthetic)     |
    /// | objid         | BIGINT | dependent object's oid                   |
    /// | objsubid      | INT    | sub-object id (column attnum); 0 typical |
    /// | refclassid    | BIGINT | catalog oid of referenced object         |
    /// | refobjid      | BIGINT | referenced object's oid                  |
    /// | refobjsubid   | INT    | referenced sub-object id; 0 typical      |
    /// | deptype       | TEXT   | `'n'` normal / `'a'` auto / `'i'` internal / `'p'` pin |
    pub fn pg_depend_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("classid", DataType::Int64, false),
            Field::new("objid", DataType::Int64, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("refclassid", DataType::Int64, false),
            Field::new("refobjid", DataType::Int64, false),
            Field::new("refobjsubid", DataType::Int32, false),
            Field::new("deptype", DataType::Utf8, false),
        ]))
    }

    /// Build `pg_catalog.pg_depend` filtered to `project`.
    ///
    /// Cross-project leak is a P0 invariant: only `list_tables` /
    /// `load_table` / `list_sql_functions` for `project` are consulted.
    pub async fn pg_depend(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let mut classids: Vec<i64> = Vec::new();
        let mut objids: Vec<i64> = Vec::new();
        let mut objsubids: Vec<i32> = Vec::new();
        let mut refclassids: Vec<i64> = Vec::new();
        let mut refobjids: Vec<i64> = Vec::new();
        let mut refobjsubids: Vec<i32> = Vec::new();
        let mut deptypes: Vec<&str> = Vec::new();

        let pg_class_classid = catalog_table_oid(project, "pg_class");
        let pg_proc_classid = catalog_table_oid(project, "pg_proc");
        let pg_type_classid = catalog_table_oid(project, "pg_type");

        // CV → source-table edges: walk the project's tables, surface one
        // row per matview whose `source_table` resolves to a sibling table.
        // Tables in `list_tables` are valid identifiers, so reparsing the
        // CV's `source_table` only fails for malformed metadata (would
        // never have round-tripped through DDL); we silently skip those.
        let names = catalog.list_tables(project).await?;
        let known: std::collections::HashSet<String> =
            names.iter().map(|n| n.as_str().to_string()).collect();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let Some(cv) = meta.continuous_aggregate.as_ref() else {
                continue;
            };
            if !known.contains(&cv.source_table) {
                continue;
            }
            let Ok(src) = TableName::new(cv.source_table.clone()) else {
                continue;
            };
            classids.push(pg_class_classid);
            objids.push(table_oid(project, name));
            objsubids.push(0);
            refclassids.push(pg_class_classid);
            refobjids.push(table_oid(project, &src));
            refobjsubids.push(0);
            deptypes.push(DEPTYPE_NORMAL);
        }

        // Function → return-type / arg-type edges. Procedures don't have
        // a return type and `Table` returns aren't yet round-tripped to a
        // single OID, so we restrict to `Scalar` returns.
        let funcs = catalog.list_sql_functions(project).await;
        for f in &funcs {
            let fn_oid = routine_oid(project, &f.name);
            // Return type: only emit when scalar with a positive OID.
            let ret_oid = return_type_oid(&f.return_type);
            if ret_oid > 0 {
                classids.push(pg_proc_classid);
                objids.push(fn_oid);
                objsubids.push(0);
                refclassids.push(pg_type_classid);
                refobjids.push(ret_oid);
                refobjsubids.push(0);
                deptypes.push(DEPTYPE_NORMAL);
            }
            // Argument types.
            for arg in &f.args {
                classids.push(pg_proc_classid);
                objids.push(fn_oid);
                objsubids.push(0);
                refclassids.push(pg_type_classid);
                refobjids.push(pg_type_oid_for_arg(arg.data_type));
                refobjsubids.push(0);
                deptypes.push(DEPTYPE_NORMAL);
            }
        }

        let schema = Self::pg_depend_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(classids)),
            Arc::new(Int64Array::from(objids)),
            Arc::new(Int32Array::from(objsubids)),
            Arc::new(Int64Array::from(refclassids)),
            Arc::new(Int64Array::from(refobjids)),
            Arc::new(Int32Array::from(refobjsubids)),
            Arc::new(StringArray::from(deptypes)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_depend build: {e}")))
    }

    /// Schema for `pg_catalog.pg_authid` rows.
    ///
    /// Basin uses projects, not PG-style roles. v0.1 surfaces exactly one
    /// row per project — the calling project — so admin scripts that JOIN
    /// against `pg_authid` to render an "owner" column resolve. Cross-
    /// project role enumeration is not exposed: each project only ever
    /// sees its own row.
    ///
    /// | column          | type    | notes                                  |
    /// |-----------------|---------|----------------------------------------|
    /// | oid             | BIGINT  | FNV-1a of `(project)`                   |
    /// | rolname         | TEXT    | project id rendered as text             |
    /// | rolsuper        | BOOL    | always false                           |
    /// | rolinherit      | BOOL    | always true                            |
    /// | rolcreaterole   | BOOL    | always false                           |
    /// | rolcreatedb     | BOOL    | always false                           |
    /// | rolcanlogin     | BOOL    | always true                            |
    /// | rolreplication  | BOOL    | always false                           |
    /// | rolconnlimit    | INT     | -1 (unlimited)                         |
    /// | rolpassword     | TEXT?   | always NULL — never leak credential    |
    /// | rolvaliduntil   | TEXT?   | always NULL                            |
    pub fn pg_authid_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("rolname", DataType::Utf8, false),
            Field::new("rolsuper", DataType::Boolean, false),
            Field::new("rolinherit", DataType::Boolean, false),
            Field::new("rolcreaterole", DataType::Boolean, false),
            Field::new("rolcreatedb", DataType::Boolean, false),
            Field::new("rolcanlogin", DataType::Boolean, false),
            Field::new("rolreplication", DataType::Boolean, false),
            Field::new("rolconnlimit", DataType::Int32, false),
            Field::new("rolpassword", DataType::Utf8, true),
            Field::new("rolvaliduntil", DataType::Utf8, true),
        ]))
    }

    /// Build `pg_catalog.pg_authid` filtered to `project`. Always exactly
    /// one row (the calling project); cross-project role enumeration is
    /// intentionally absent.
    pub async fn pg_authid(_catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let oid = role_oid_for(project);
        let rolname = project.to_string();
        let schema = Self::pg_authid_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![oid])),
            Arc::new(StringArray::from(vec![rolname])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(Int32Array::from(vec![-1])),
            Arc::new(StringArray::from(vec![None::<String>])),
            Arc::new(StringArray::from(vec![None::<String>])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_authid build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_database  (one row — the current project's logical db)
    // -----------------------------------------------------------------------

    pub fn pg_database_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("datdba", DataType::Int64, false),
            Field::new("encoding", DataType::Int32, false),
            Field::new("datcollate", DataType::Utf8, false),
            Field::new("datctype", DataType::Utf8, false),
            Field::new("datistemplate", DataType::Boolean, false),
            Field::new("datallowconn", DataType::Boolean, false),
            Field::new("datconnlimit", DataType::Int32, false),
        ]))
    }

    pub async fn pg_database(_catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let db_oid = fnv1a_64_to_positive_i64(format!("basin.pg_database:{project}").as_bytes());
        let dba_oid = role_oid_for(project);
        let schema = Self::pg_database_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![db_oid])),
            Arc::new(StringArray::from(vec!["basin"])),
            Arc::new(Int64Array::from(vec![dba_oid])),
            Arc::new(Int32Array::from(vec![6])), // UTF8
            Arc::new(StringArray::from(vec!["en_US.UTF-8"])),
            Arc::new(StringArray::from(vec!["en_US.UTF-8"])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int32Array::from(vec![-1])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_database build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_roles  (alias to pg_authid with public columns)
    // pg_catalog.pg_roles  (public view of pg_authid)
    // -----------------------------------------------------------------------

    pub fn pg_roles_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("rolname", DataType::Utf8, false),
            Field::new("rolsuper", DataType::Boolean, false),
            Field::new("rolinherit", DataType::Boolean, false),
            Field::new("rolcreaterole", DataType::Boolean, false),
            Field::new("rolcreatedb", DataType::Boolean, false),
            Field::new("rolcanlogin", DataType::Boolean, false),
            Field::new("rolreplication", DataType::Boolean, false),
            Field::new("rolconnlimit", DataType::Int32, false),
            Field::new("rolvaliduntil", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_roles(_catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let oid = role_oid_for(project);
        let rolname = project.to_string();
        let schema = Self::pg_roles_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![oid])),
            Arc::new(StringArray::from(vec![rolname])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(Int32Array::from(vec![-1])),
            Arc::new(StringArray::from(vec![None::<String>])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_roles build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_indexes  (denormalised index info)
    // pg_catalog.pg_indexes
    // -----------------------------------------------------------------------

    pub fn pg_indexes_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("tablename", DataType::Utf8, false),
            Field::new("indexname", DataType::Utf8, false),
            Field::new("tablespace", DataType::Utf8, true),
            Field::new("indexdef", DataType::Utf8, true),
        ]))
    }

    /// Returns one row per index in the project's tables. Covers PRIMARY KEY
    /// constraints and every `SecondaryIndex` stored in the catalog (B-tree,
    /// GIN, etc.).
    pub async fn pg_indexes(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;
        let mut schemas: Vec<&str> = Vec::new();
        let mut tablenames: Vec<String> = Vec::new();
        let mut indexnames: Vec<String> = Vec::new();
        let mut tablespaces: Vec<Option<String>> = Vec::new();
        let mut indexdefs: Vec<Option<String>> = Vec::new();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            // Primary key pseudo-index.
            if !meta.pk_columns.is_empty() {
                schemas.push(DEFAULT_SCHEMA);
                tablenames.push(name.as_str().to_string());
                indexnames.push(format!("{}_pkey", name.as_str()));
                tablespaces.push(None);
                let cols = meta.pk_columns.join(", ");
                indexdefs.push(Some(format!(
                    "CREATE UNIQUE INDEX {}_pkey ON {} ({})",
                    name.as_str(),
                    name.as_str(),
                    cols
                )));
            }
            // Secondary indexes (B-tree, GIN, …).
            for idx in &meta.indexes {
                schemas.push(DEFAULT_SCHEMA);
                tablenames.push(name.as_str().to_string());
                indexnames.push(idx.name.clone());
                tablespaces.push(None);
                let indexdef = build_indexdef(name.as_str(), idx);
                indexdefs.push(Some(indexdef));
            }
        }
        let schema = Self::pg_indexes_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(tablenames)),
            Arc::new(StringArray::from(indexnames)),
            Arc::new(StringArray::from(tablespaces)),
            Arc::new(StringArray::from(indexdefs)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_indexes build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_tables  (denormalised table info)
    // pg_catalog.pg_tables
    // -----------------------------------------------------------------------
    // (build_indexdef is a free fn below the impl block)

    pub fn pg_tables_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("tablename", DataType::Utf8, false),
            Field::new("tableowner", DataType::Utf8, false),
            Field::new("tablespace", DataType::Utf8, true),
            Field::new("hasindexes", DataType::Boolean, false),
            Field::new("hasrules", DataType::Boolean, false),
            Field::new("hastriggers", DataType::Boolean, false),
            Field::new("rowsecurity", DataType::Boolean, false),
        ]))
    }

    pub async fn pg_tables(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;
        let mut schemas: Vec<&str> = Vec::new();
        let mut tablenames: Vec<String> = Vec::new();
        let mut owners: Vec<String> = Vec::new();
        let mut tablespaces: Vec<Option<String>> = Vec::new();
        let mut hasindexes: Vec<bool> = Vec::new();
        let mut hasrules: Vec<bool> = Vec::new();
        let mut hastriggers: Vec<bool> = Vec::new();
        let mut rowsecurity: Vec<bool> = Vec::new();
        let owner = project.to_string();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            // Only base tables (not matviews) appear in pg_tables
            if meta.continuous_aggregate.is_none() {
                schemas.push(DEFAULT_SCHEMA);
                tablenames.push(name.as_str().to_string());
                owners.push(owner.clone());
                tablespaces.push(None);
                hasindexes.push(!meta.pk_columns.is_empty());
                hasrules.push(false);
                hastriggers.push(false);
                rowsecurity.push(meta.rls_enabled);
            }
        }
        let schema = Self::pg_tables_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(tablenames)),
            Arc::new(StringArray::from(owners)),
            Arc::new(StringArray::from(tablespaces)),
            Arc::new(BooleanArray::from(hasindexes)),
            Arc::new(BooleanArray::from(hasrules)),
            Arc::new(BooleanArray::from(hastriggers)),
            Arc::new(BooleanArray::from(rowsecurity)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_tables build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_settings  (GUC variables — static defaults)
    // -----------------------------------------------------------------------

    pub fn pg_settings_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("setting", DataType::Utf8, false),
            Field::new("unit", DataType::Utf8, true),
            Field::new("category", DataType::Utf8, false),
            Field::new("short_desc", DataType::Utf8, false),
            Field::new("extra_desc", DataType::Utf8, true),
            Field::new("context", DataType::Utf8, false),
            Field::new("vartype", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
            Field::new("min_val", DataType::Utf8, true),
            Field::new("max_val", DataType::Utf8, true),
            Field::new("enumvals", DataType::Utf8, true),
            Field::new("boot_val", DataType::Utf8, true),
            Field::new("reset_val", DataType::Utf8, true),
            Field::new("sourcefile", DataType::Utf8, true),
            Field::new("sourceline", DataType::Int32, true),
            Field::new("pending_restart", DataType::Boolean, false),
        ]))
    }

    pub async fn pg_settings(_catalog: &dyn Catalog, _project: &ProjectId) -> Result<RecordBatch> {
        // A minimal GUC list that ORMs and admin tools commonly probe.
        // Tuple: (name, setting, unit, category, short_desc, context, vartype, source, min, max, boot, reset)
        let rows: &[(
            &str,
            &str,
            Option<&str>,
            &str,
            &str,
            &str,
            &str,
            &str,
            Option<&str>,
            Option<&str>,
            Option<&str>,
            Option<&str>,
        )] = &[
            (
                "server_version",
                "15.0",
                None,
                "Preset Options",
                "Shows the server version.",
                "internal",
                "string",
                "default",
                None,
                None,
                Some("15.0"),
                Some("15.0"),
            ),
            (
                "server_version_num",
                "150000",
                None,
                "Preset Options",
                "Shows the server version as an integer.",
                "internal",
                "integer",
                "default",
                Some("0"),
                Some("2147483647"),
                Some("150000"),
                Some("150000"),
            ),
            (
                "max_connections",
                "100",
                None,
                "Connections and Authentication / Connection Settings",
                "Sets the maximum number of concurrent connections.",
                "postmaster",
                "integer",
                "default",
                Some("1"),
                Some("262143"),
                Some("100"),
                Some("100"),
            ),
            (
                "TimeZone",
                "UTC",
                None,
                "Client Connection Defaults / Locale and Formatting",
                "Sets the time zone for displaying and interpreting time stamps.",
                "user",
                "string",
                "default",
                None,
                None,
                Some("UTC"),
                Some("UTC"),
            ),
            (
                "client_encoding",
                "UTF8",
                None,
                "Client Connection Defaults / Locale and Formatting",
                "Sets the client's character set encoding.",
                "user",
                "string",
                "default",
                None,
                None,
                Some("UTF8"),
                Some("UTF8"),
            ),
            (
                "standard_conforming_strings",
                "on",
                None,
                "Version and Platform Compatibility / Previous PostgreSQL Versions",
                "Causes '...' strings to treat backslashes literally.",
                "user",
                "bool",
                "default",
                None,
                None,
                Some("on"),
                Some("on"),
            ),
            (
                "search_path",
                "public",
                None,
                "Client Connection Defaults / Statement Behavior",
                "Sets the schema search order for names that are not schema-qualified.",
                "user",
                "string",
                "default",
                None,
                None,
                Some("public"),
                Some("public"),
            ),
            (
                "default_transaction_isolation",
                "read committed",
                None,
                "Client Connection Defaults / Statement Behavior",
                "Sets the transaction isolation level of each new transaction.",
                "user",
                "enum",
                "default",
                None,
                None,
                Some("read committed"),
                Some("read committed"),
            ),
        ];
        let schema = Self::pg_settings_schema();
        let n = rows.len();
        let mut names = Vec::with_capacity(n);
        let mut settings = Vec::with_capacity(n);
        let mut units: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut categories = Vec::with_capacity(n);
        let mut short_descs = Vec::with_capacity(n);
        let mut extra_descs: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut contexts = Vec::with_capacity(n);
        let mut vartypes = Vec::with_capacity(n);
        let mut sources = Vec::with_capacity(n);
        let mut min_vals: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut max_vals: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut enumvals: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut boot_vals: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut reset_vals: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut sourcefiles: Vec<Option<&str>> = Vec::with_capacity(n);
        let mut sourcelines: Vec<Option<i32>> = Vec::with_capacity(n);
        let mut pending: Vec<bool> = Vec::with_capacity(n);
        for (
            name,
            setting,
            unit,
            category,
            short_desc,
            context,
            vartype,
            source,
            min,
            max,
            boot,
            reset,
        ) in rows
        {
            names.push(*name);
            settings.push(*setting);
            units.push(*unit);
            categories.push(*category);
            short_descs.push(*short_desc);
            extra_descs.push(None);
            contexts.push(*context);
            vartypes.push(*vartype);
            sources.push(*source);
            min_vals.push(*min);
            max_vals.push(*max);
            enumvals.push(None);
            boot_vals.push(*boot);
            reset_vals.push(*reset);
            sourcefiles.push(None);
            sourcelines.push(None);
            pending.push(false);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(settings)),
            Arc::new(StringArray::from(units)),
            Arc::new(StringArray::from(categories)),
            Arc::new(StringArray::from(short_descs)),
            Arc::new(StringArray::from(extra_descs)),
            Arc::new(StringArray::from(contexts)),
            Arc::new(StringArray::from(vartypes)),
            Arc::new(StringArray::from(sources)),
            Arc::new(StringArray::from(min_vals)),
            Arc::new(StringArray::from(max_vals)),
            Arc::new(StringArray::from(enumvals)),
            Arc::new(StringArray::from(boot_vals)),
            Arc::new(StringArray::from(reset_vals)),
            Arc::new(StringArray::from(sourcefiles)),
            Arc::new(Int32Array::from(sourcelines)),
            Arc::new(BooleanArray::from(pending)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_settings build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_extension  (installed extensions — empty stub)
    // pg_catalog.pg_extension  (empty stub — no extensions in basin)
    // -----------------------------------------------------------------------

    pub fn pg_extension_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("extname", DataType::Utf8, false),
            Field::new("extowner", DataType::Int64, false),
            Field::new("extnamespace", DataType::Int64, false),
            Field::new("extrelocatable", DataType::Boolean, false),
            Field::new("extversion", DataType::Utf8, false),
        ]))
    }

    pub async fn pg_extension(_catalog: &dyn Catalog, _project: &ProjectId) -> Result<RecordBatch> {
        let schema = Self::pg_extension_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(BooleanArray::from(Vec::<bool>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_extension build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_description  (object comments — empty stub)
    // -----------------------------------------------------------------------

    pub fn pg_description_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("objoid", DataType::Int64, false),
            Field::new("classoid", DataType::Int64, false),
            Field::new("objsubid", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
        ]))
    }

    pub async fn pg_description(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_description_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_description build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_user_tables  (basic row/scan count stubs)
    // pg_catalog.pg_stat_user_tables  (row-count stubs per table)
    // -----------------------------------------------------------------------

    pub fn pg_stat_user_tables_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("relid", DataType::Int64, false),
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("seq_scan", DataType::Int64, false),
            Field::new("seq_tup_read", DataType::Int64, false),
            Field::new("idx_scan", DataType::Int64, true),
            Field::new("idx_tup_fetch", DataType::Int64, true),
            Field::new("n_tup_ins", DataType::Int64, false),
            Field::new("n_tup_upd", DataType::Int64, false),
            Field::new("n_tup_del", DataType::Int64, false),
            Field::new("n_live_tup", DataType::Int64, false),
            Field::new("n_dead_tup", DataType::Int64, false),
            Field::new("last_vacuum", DataType::Utf8, true),
            Field::new("last_autovacuum", DataType::Utf8, true),
            Field::new("last_analyze", DataType::Utf8, true),
            Field::new("last_autoanalyze", DataType::Utf8, true),
            Field::new("vacuum_count", DataType::Int64, false),
            Field::new("autovacuum_count", DataType::Int64, false),
            Field::new("analyze_count", DataType::Int64, false),
            Field::new("autoanalyze_count", DataType::Int64, false),
        ]))
    }

    pub async fn pg_stat_user_tables(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;
        let mut relids: Vec<i64> = Vec::new();
        let mut schemas: Vec<&str> = Vec::new();
        let mut relnames: Vec<String> = Vec::new();
        let mut seq_scans: Vec<i64> = Vec::new();
        let mut seq_tup_reads: Vec<i64> = Vec::new();
        let mut idx_scans: Vec<Option<i64>> = Vec::new();
        let mut idx_tup_fetchs: Vec<Option<i64>> = Vec::new();
        let mut n_ins: Vec<i64> = Vec::new();
        let mut n_upd: Vec<i64> = Vec::new();
        let mut n_del: Vec<i64> = Vec::new();
        let mut n_live: Vec<i64> = Vec::new();
        let mut n_dead: Vec<i64> = Vec::new();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let row_count = meta
                .current()
                .map(|s| s.data_files.iter().map(|f| f.row_count).sum::<u64>())
                .unwrap_or(0) as i64;
            relids.push(table_oid(project, name));
            schemas.push(DEFAULT_SCHEMA);
            relnames.push(name.as_str().to_string());
            seq_scans.push(0);
            seq_tup_reads.push(0);
            idx_scans.push(Some(0));
            idx_tup_fetchs.push(Some(0));
            n_ins.push(row_count);
            n_upd.push(0);
            n_del.push(0);
            n_live.push(row_count);
            n_dead.push(0);
        }
        let n = relids.len();
        let schema = Self::pg_stat_user_tables_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(relids)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(Int64Array::from(seq_scans)),
            Arc::new(Int64Array::from(seq_tup_reads)),
            Arc::new(Int64Array::from(idx_scans)),
            Arc::new(Int64Array::from(idx_tup_fetchs)),
            Arc::new(Int64Array::from(n_ins)),
            Arc::new(Int64Array::from(n_upd)),
            Arc::new(Int64Array::from(n_del)),
            Arc::new(Int64Array::from(n_live)),
            Arc::new(Int64Array::from(n_dead)),
            // last_vacuum / last_autovacuum / last_analyze / last_autoanalyze —
            // all nullable Utf8; Basin doesn't run vacuum/analyze so always NULL.
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            // vacuum_count / autovacuum_count / analyze_count / autoanalyze_count
            Arc::new(Int64Array::from(vec![0i64; n])),
            Arc::new(Int64Array::from(vec![0i64; n])),
            Arc::new(Int64Array::from(vec![0i64; n])),
            Arc::new(Int64Array::from(vec![0i64; n])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_stat_user_tables build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_user_indexes  (scan-count stubs)
    // -----------------------------------------------------------------------

    pub fn pg_stat_user_indexes_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("relid", DataType::Int64, false),
            Field::new("indexrelid", DataType::Int64, false),
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("relname", DataType::Utf8, false),
            Field::new("indexrelname", DataType::Utf8, false),
            Field::new("idx_scan", DataType::Int64, false),
            Field::new("idx_tup_read", DataType::Int64, false),
            Field::new("idx_tup_fetch", DataType::Int64, false),
        ]))
    }

    pub async fn pg_stat_user_indexes(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;
        let mut relids: Vec<i64> = Vec::new();
        let mut indexrelids: Vec<i64> = Vec::new();
        let mut schemas: Vec<&str> = Vec::new();
        let mut relnames: Vec<String> = Vec::new();
        let mut indexrelnames: Vec<String> = Vec::new();
        let mut idx_scans: Vec<i64> = Vec::new();
        let mut idx_tup_reads: Vec<i64> = Vec::new();
        let mut idx_tup_fetchs: Vec<i64> = Vec::new();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            if !meta.pk_columns.is_empty() {
                let idx_name = format!("{}_pkey", name.as_str());
                let idx_oid = fnv1a_64_to_positive_i64(
                    format!("basin.pg_index:{project}:{idx_name}").as_bytes(),
                );
                relids.push(table_oid(project, name));
                indexrelids.push(idx_oid);
                schemas.push(DEFAULT_SCHEMA);
                relnames.push(name.as_str().to_string());
                indexrelnames.push(idx_name);
                idx_scans.push(0);
                idx_tup_reads.push(0);
                idx_tup_fetchs.push(0);
            }
        }
        let schema = Self::pg_stat_user_indexes_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(relids)),
            Arc::new(Int64Array::from(indexrelids)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(relnames)),
            Arc::new(StringArray::from(indexrelnames)),
            Arc::new(Int64Array::from(idx_scans)),
            Arc::new(Int64Array::from(idx_tup_reads)),
            Arc::new(Int64Array::from(idx_tup_fetchs)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_stat_user_indexes build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_locks  (empty — optimistic, no lock manager in basin)
    // -----------------------------------------------------------------------

    pub fn pg_locks_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("locktype", DataType::Utf8, true),
            Field::new("database", DataType::Int64, true),
            Field::new("relation", DataType::Int64, true),
            Field::new("page", DataType::Int32, true),
            Field::new("tuple", DataType::Int16, true),
            Field::new("virtualxid", DataType::Utf8, true),
            Field::new("transactionid", DataType::Int64, true),
            Field::new("classid", DataType::Int64, true),
            Field::new("objid", DataType::Int64, true),
            Field::new("objsubid", DataType::Int16, true),
            Field::new("virtualtransaction", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, true),
            Field::new("mode", DataType::Utf8, true),
            Field::new("granted", DataType::Boolean, true),
            Field::new("fastpath", DataType::Boolean, true),
        ]))
    }

    pub async fn pg_locks(_catalog: &dyn Catalog, _project: &ProjectId) -> Result<RecordBatch> {
        let schema = Self::pg_locks_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(Int16Array::from(Vec::<Option<i16>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(Int16Array::from(Vec::<Option<i16>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())),
            Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_locks build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_activity  (current sessions — just this session)
    // pg_catalog.pg_stat_activity  (single row for the current session)
    // -----------------------------------------------------------------------

    pub fn pg_stat_activity_schema() -> Arc<Schema> {
        // Canonical PG `pg_stat_activity` column order. Timestamps reported
        // as text in v0.1; once we wire real session start times to backend
        // metadata switch to Timestamp(Microsecond, Some("UTC")).
        Arc::new(Schema::new(vec![
            Field::new("datid", DataType::Int64, true),
            Field::new("datname", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, false),
            Field::new("usesysid", DataType::Int64, true),
            Field::new("usename", DataType::Utf8, true),
            Field::new("application_name", DataType::Utf8, true),
            Field::new("client_addr", DataType::Utf8, true),
            Field::new("client_hostname", DataType::Utf8, true),
            Field::new("client_port", DataType::Int32, true),
            Field::new("backend_start", DataType::Utf8, true),
            Field::new("xact_start", DataType::Utf8, true),
            Field::new("query_start", DataType::Utf8, true),
            Field::new("state_change", DataType::Utf8, true),
            Field::new("wait_event_type", DataType::Utf8, true),
            Field::new("wait_event", DataType::Utf8, true),
            Field::new("state", DataType::Utf8, true),
            Field::new("backend_xid", DataType::Int64, true),
            Field::new("backend_xmin", DataType::Int64, true),
            Field::new("query", DataType::Utf8, true),
            Field::new("backend_type", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_stat_activity(
        _catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let db_oid = fnv1a_64_to_positive_i64(format!("basin.pg_database:{project}").as_bytes());
        let role_oid = role_oid_for(project);
        let schema = Self::pg_stat_activity_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(db_oid)])),
            Arc::new(StringArray::from(vec![Some("basin")])),
            Arc::new(Int32Array::from(vec![0i32])),
            Arc::new(Int64Array::from(vec![Some(role_oid)])),
            Arc::new(StringArray::from(vec![Some(project.to_string())])),
            Arc::new(StringArray::from(vec![Some("basin")])),
            Arc::new(StringArray::from(vec![None::<String>])), // client_addr
            Arc::new(StringArray::from(vec![None::<String>])), // client_hostname
            Arc::new(Int32Array::from(vec![None::<i32>])),     // client_port
            Arc::new(StringArray::from(vec![None::<String>])), // backend_start
            Arc::new(StringArray::from(vec![None::<String>])), // xact_start
            Arc::new(StringArray::from(vec![None::<String>])), // query_start
            Arc::new(StringArray::from(vec![None::<String>])), // state_change
            Arc::new(StringArray::from(vec![None::<String>])), // wait_event_type
            Arc::new(StringArray::from(vec![None::<String>])), // wait_event
            Arc::new(StringArray::from(vec![Some("active")])), // state
            Arc::new(Int64Array::from(vec![None::<i64>])),     // backend_xid
            Arc::new(Int64Array::from(vec![None::<i64>])),     // backend_xmin
            Arc::new(StringArray::from(vec![None::<String>])), // query
            Arc::new(StringArray::from(vec![Some("client backend")])), // backend_type
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_stat_activity build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_database  (one row for the project's logical database)
    // -----------------------------------------------------------------------

    pub fn pg_stat_database_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("datid", DataType::Int64, true),
            Field::new("datname", DataType::Utf8, true),
            Field::new("numbackends", DataType::Int32, false),
            Field::new("xact_commit", DataType::Int64, false),
            Field::new("xact_rollback", DataType::Int64, false),
            Field::new("blks_read", DataType::Int64, false),
            Field::new("blks_hit", DataType::Int64, false),
            Field::new("tup_returned", DataType::Int64, false),
            Field::new("tup_fetched", DataType::Int64, false),
            Field::new("tup_inserted", DataType::Int64, false),
            Field::new("tup_updated", DataType::Int64, false),
            Field::new("tup_deleted", DataType::Int64, false),
            Field::new("conflicts", DataType::Int64, false),
            Field::new("temp_files", DataType::Int64, false),
            Field::new("temp_bytes", DataType::Int64, false),
            Field::new("deadlocks", DataType::Int64, false),
            Field::new("blk_read_time", DataType::Float32, false),
            Field::new("blk_write_time", DataType::Float32, false),
            Field::new("stats_reset", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_stat_database(
        _catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let db_oid = fnv1a_64_to_positive_i64(format!("basin.pg_database:{project}").as_bytes());
        let schema = Self::pg_stat_database_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![Some(db_oid)])),
            Arc::new(StringArray::from(vec![Some("basin")])),
            Arc::new(Int32Array::from(vec![1i32])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Float32Array::from(vec![0.0f32])),
            Arc::new(Float32Array::from(vec![0.0f32])),
            Arc::new(StringArray::from(vec![None::<String>])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_stat_database build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_bgwriter  (empty stub — no bgwriter in basin)
    // -----------------------------------------------------------------------

    pub fn pg_stat_bgwriter_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("checkpoints_timed", DataType::Int64, false),
            Field::new("checkpoints_req", DataType::Int64, false),
            Field::new("checkpoint_write_time", DataType::Float32, false),
            Field::new("checkpoint_sync_time", DataType::Float32, false),
            Field::new("buffers_checkpoint", DataType::Int64, false),
            Field::new("buffers_clean", DataType::Int64, false),
            Field::new("maxwritten_clean", DataType::Int64, false),
            Field::new("buffers_backend", DataType::Int64, false),
            Field::new("buffers_backend_fsync", DataType::Int64, false),
            Field::new("buffers_alloc", DataType::Int64, false),
            Field::new("stats_reset", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_stat_bgwriter(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        // Single all-zero row (bgwriter concept does not exist in basin)
        let schema = Self::pg_stat_bgwriter_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Float32Array::from(vec![0.0f32])),
            Arc::new(Float32Array::from(vec![0.0f32])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(StringArray::from(vec![None::<String>])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_stat_bgwriter build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_replication  (empty stub — no replication)
    // -----------------------------------------------------------------------

    pub fn pg_stat_replication_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("pid", DataType::Int32, true),
            Field::new("usesysid", DataType::Int64, true),
            Field::new("usename", DataType::Utf8, true),
            Field::new("application_name", DataType::Utf8, true),
            Field::new("client_addr", DataType::Utf8, true),
            Field::new("client_hostname", DataType::Utf8, true),
            Field::new("client_port", DataType::Int32, true),
            Field::new("backend_start", DataType::Utf8, true),
            Field::new("backend_xmin", DataType::Int64, true),
            Field::new("state", DataType::Utf8, true),
            Field::new("sent_lsn", DataType::Utf8, true),
            Field::new("write_lsn", DataType::Utf8, true),
            Field::new("flush_lsn", DataType::Utf8, true),
            Field::new("replay_lsn", DataType::Utf8, true),
            Field::new("sync_priority", DataType::Int32, true),
            Field::new("sync_state", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_stat_replication(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_replication_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_stat_replication build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_archiver  (empty stub)
    // -----------------------------------------------------------------------

    pub fn pg_stat_archiver_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("archived_count", DataType::Int64, false),
            Field::new("last_archived_wal", DataType::Utf8, true),
            Field::new("last_archived_time", DataType::Utf8, true),
            Field::new("failed_count", DataType::Int64, false),
            Field::new("last_failed_wal", DataType::Utf8, true),
            Field::new("last_failed_time", DataType::Utf8, true),
            Field::new("stats_reset", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_stat_archiver(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_archiver_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(StringArray::from(vec![None::<String>])),
            Arc::new(StringArray::from(vec![None::<String>])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(StringArray::from(vec![None::<String>])),
            Arc::new(StringArray::from(vec![None::<String>])),
            Arc::new(StringArray::from(vec![None::<String>])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_stat_activity build: {e}")))
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_stat_archiver build: {e}")))
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_wal_receiver  (empty stub — no replication)
    // -----------------------------------------------------------------------

    pub fn pg_stat_wal_receiver_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("pid", DataType::Int32, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("receive_start_lsn", DataType::Utf8, true),
            Field::new("receive_start_tli", DataType::Int32, true),
            Field::new("received_lsn", DataType::Utf8, true),
            Field::new("received_tli", DataType::Int32, true),
            Field::new("last_msg_send_time", DataType::Utf8, true),
            Field::new("last_msg_receipt_time", DataType::Utf8, true),
            Field::new("latest_end_lsn", DataType::Utf8, true),
            Field::new("latest_end_time", DataType::Utf8, true),
            Field::new("slot_name", DataType::Utf8, true),
            Field::new("conninfo", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_stat_wal_receiver(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_wal_receiver_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("pg_catalog.pg_stat_wal_receiver build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_subscription  (empty stub — no logical replication)
    // -----------------------------------------------------------------------

    pub fn pg_stat_subscription_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("subid", DataType::Int64, true),
            Field::new("subname", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, true),
            Field::new("relid", DataType::Int64, true),
            Field::new("received_lsn", DataType::Utf8, true),
            Field::new("last_msg_send_time", DataType::Utf8, true),
            Field::new("last_msg_receipt_time", DataType::Utf8, true),
            Field::new("latest_end_lsn", DataType::Utf8, true),
            Field::new("latest_end_time", DataType::Utf8, true),
        ]))
    }

    pub async fn pg_stat_subscription(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_subscription_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(Int64Array::from(Vec::<Option<i64>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("pg_catalog.pg_stat_subscription build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_user_functions  (empty stub — no tracked functions)
    // -----------------------------------------------------------------------

    pub fn pg_stat_user_functions_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("funcid", DataType::Int64, false),
            Field::new("schemaname", DataType::Utf8, false),
            Field::new("funcname", DataType::Utf8, false),
            Field::new("calls", DataType::Int64, false),
            Field::new("total_time", DataType::Float32, false),
            Field::new("self_time", DataType::Float32, false),
        ]))
    }

    pub async fn pg_stat_user_functions(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_user_functions_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Float32Array::from(Vec::<f32>::new())),
            Arc::new(Float32Array::from(Vec::<f32>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("pg_catalog.pg_stat_user_functions build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_progress_vacuum  (empty stub)
    // -----------------------------------------------------------------------

    pub fn pg_stat_progress_vacuum_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("pid", DataType::Int32, false),
            Field::new("datid", DataType::Int64, false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("relid", DataType::Int64, false),
            Field::new("phase", DataType::Utf8, false),
            Field::new("heap_blks_total", DataType::Int64, false),
            Field::new("heap_blks_scanned", DataType::Int64, false),
            Field::new("heap_blks_vacuumed", DataType::Int64, false),
            Field::new("index_vacuum_count", DataType::Int64, false),
            Field::new("max_dead_tuples", DataType::Int64, false),
            Field::new("num_dead_tuples", DataType::Int64, false),
        ]))
    }

    pub async fn pg_stat_progress_vacuum(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_progress_vacuum_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("pg_catalog.pg_stat_progress_vacuum build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_progress_create_index  (empty stub)
    // -----------------------------------------------------------------------

    pub fn pg_stat_progress_create_index_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("pid", DataType::Int32, false),
            Field::new("datid", DataType::Int64, false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("relid", DataType::Int64, false),
            Field::new("index_relid", DataType::Int64, false),
            Field::new("command", DataType::Utf8, false),
            Field::new("phase", DataType::Utf8, false),
            Field::new("blocks_done", DataType::Int64, false),
            Field::new("blocks_total", DataType::Int64, false),
            Field::new("tuples_done", DataType::Int64, false),
            Field::new("tuples_total", DataType::Int64, false),
            Field::new("partitions_done", DataType::Int64, false),
            Field::new("partitions_total", DataType::Int64, false),
        ]))
    }

    pub async fn pg_stat_progress_create_index(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_progress_create_index_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!(
                "pg_catalog.pg_stat_progress_create_index build: {e}"
            ))
        })
    }

    // -----------------------------------------------------------------------
    // pg_catalog.pg_stat_progress_analyze  (empty stub)
    // -----------------------------------------------------------------------

    pub fn pg_stat_progress_analyze_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("pid", DataType::Int32, false),
            Field::new("datid", DataType::Int64, false),
            Field::new("datname", DataType::Utf8, false),
            Field::new("relid", DataType::Int64, false),
            Field::new("phase", DataType::Utf8, false),
            Field::new("sample_blks_total", DataType::Int64, false),
            Field::new("sample_blks_scanned", DataType::Int64, false),
            Field::new("ext_stats_total", DataType::Int64, false),
            Field::new("ext_stats_computed", DataType::Int64, false),
            Field::new("child_tables_total", DataType::Int64, false),
            Field::new("child_tables_done", DataType::Int64, false),
        ]))
    }

    pub async fn pg_stat_progress_analyze(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_stat_progress_analyze_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("pg_catalog.pg_stat_progress_analyze build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.check_constraints
    // -----------------------------------------------------------------------

    pub fn check_constraints_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("constraint_catalog", DataType::Utf8, false),
            Field::new("constraint_schema", DataType::Utf8, false),
            Field::new("constraint_name", DataType::Utf8, false),
            Field::new("check_clause", DataType::Utf8, false),
        ]))
    }

    pub async fn check_constraints(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;
        let mut constraint_catalogs: Vec<&str> = Vec::new();
        let mut constraint_schemas: Vec<&str> = Vec::new();
        let mut constraint_names: Vec<String> = Vec::new();
        let mut check_clauses: Vec<String> = Vec::new();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let arrow_schema = &meta.schema;
            for field in arrow_schema.fields() {
                // Basin encodes CHECK constraints as field metadata
                if let Some(check) = field.metadata().get("BASIN_CHECK") {
                    let constraint_name = format!("{}_{}_check", name.as_str(), field.name());
                    constraint_catalogs.push(BASIN_CATALOG_NAME);
                    constraint_schemas.push(DEFAULT_SCHEMA);
                    constraint_names.push(constraint_name);
                    check_clauses.push(check.clone());
                }
            }
        }
        let schema = Self::check_constraints_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(constraint_catalogs)),
            Arc::new(StringArray::from(constraint_schemas)),
            Arc::new(StringArray::from(constraint_names)),
            Arc::new(StringArray::from(check_clauses)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.check_constraints build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.triggers  (empty — Basin doesn't execute triggers)
    // information_schema.triggers  (empty stub — no DDL triggers in basin)
    // -----------------------------------------------------------------------

    pub fn triggers_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("trigger_catalog", DataType::Utf8, false),
            Field::new("trigger_schema", DataType::Utf8, false),
            Field::new("trigger_name", DataType::Utf8, false),
            Field::new("event_manipulation", DataType::Utf8, false),
            Field::new("event_object_catalog", DataType::Utf8, false),
            Field::new("event_object_schema", DataType::Utf8, false),
            Field::new("event_object_table", DataType::Utf8, false),
            Field::new("action_order", DataType::Int64, false),
            Field::new("action_condition", DataType::Utf8, true),
            Field::new("action_statement", DataType::Utf8, false),
            Field::new("action_orientation", DataType::Utf8, false),
            Field::new("action_timing", DataType::Utf8, false),
        ]))
    }

    pub async fn triggers(_catalog: &dyn Catalog, _project: &ProjectId) -> Result<RecordBatch> {
        let schema = Self::triggers_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("information_schema.triggers build: {e}")))
    }

    // -----------------------------------------------------------------------
    // information_schema.sequences  (real, from catalog sequences)
    // -----------------------------------------------------------------------

    pub fn sequences_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("sequence_catalog", DataType::Utf8, false),
            Field::new("sequence_schema", DataType::Utf8, false),
            Field::new("sequence_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("numeric_precision", DataType::Int32, false),
            Field::new("numeric_precision_radix", DataType::Int32, false),
            Field::new("numeric_scale", DataType::Int32, false),
            Field::new("start_value", DataType::Utf8, false),
            Field::new("minimum_value", DataType::Utf8, false),
            Field::new("maximum_value", DataType::Utf8, false),
            Field::new("increment", DataType::Utf8, false),
            Field::new("cycle_option", DataType::Utf8, false),
        ]))
    }

    pub async fn sequences(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let seqs = catalog.list_sequences(project).await;
        let mut seq_catalogs: Vec<&str> = Vec::new();
        let mut seq_schemas: Vec<&str> = Vec::new();
        let mut seq_names: Vec<String> = Vec::new();
        let mut data_types: Vec<&str> = Vec::new();
        let mut num_precisions: Vec<i32> = Vec::new();
        let mut num_precision_radixes: Vec<i32> = Vec::new();
        let mut num_scales: Vec<i32> = Vec::new();
        let mut start_values: Vec<String> = Vec::new();
        let mut min_values: Vec<String> = Vec::new();
        let mut max_values: Vec<String> = Vec::new();
        let mut increments: Vec<String> = Vec::new();
        let mut cycle_options: Vec<&str> = Vec::new();
        for seq in &seqs {
            seq_catalogs.push(BASIN_CATALOG_NAME);
            seq_schemas.push(DEFAULT_SCHEMA);
            seq_names.push(seq.name.clone());
            data_types.push("bigint");
            num_precisions.push(64);
            num_precision_radixes.push(2);
            num_scales.push(0);
            start_values.push(seq.start.to_string());
            min_values.push(seq.min_value.to_string());
            max_values.push(seq.max_value.to_string());
            increments.push(seq.increment.to_string());
            cycle_options.push(if seq.cycle { "YES" } else { "NO" });
        }
        let schema = Self::sequences_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(seq_catalogs)),
            Arc::new(StringArray::from(seq_schemas)),
            Arc::new(StringArray::from(seq_names)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(Int32Array::from(num_precisions)),
            Arc::new(Int32Array::from(num_precision_radixes)),
            Arc::new(Int32Array::from(num_scales)),
            Arc::new(StringArray::from(start_values)),
            Arc::new(StringArray::from(min_values)),
            Arc::new(StringArray::from(max_values)),
            Arc::new(StringArray::from(increments)),
            Arc::new(StringArray::from(cycle_options)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("information_schema.sequences build: {e}")))
    }

    // -----------------------------------------------------------------------
    // information_schema.domains  (real, from catalog domains)
    // -----------------------------------------------------------------------

    pub fn domains_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("domain_catalog", DataType::Utf8, false),
            Field::new("domain_schema", DataType::Utf8, false),
            Field::new("domain_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::Int32, true),
            Field::new("character_octet_length", DataType::Int32, true),
            Field::new("numeric_precision", DataType::Int32, true),
            Field::new("numeric_precision_radix", DataType::Int32, true),
            Field::new("numeric_scale", DataType::Int32, true),
            Field::new("datetime_precision", DataType::Int32, true),
            Field::new("domain_default", DataType::Utf8, true),
            Field::new("udt_catalog", DataType::Utf8, false),
            Field::new("udt_schema", DataType::Utf8, false),
            Field::new("udt_name", DataType::Utf8, false),
        ]))
    }

    pub async fn domains(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let doms = catalog.list_domains(project).await;
        let mut dom_catalogs: Vec<&str> = Vec::new();
        let mut dom_schemas: Vec<&str> = Vec::new();
        let mut dom_names: Vec<String> = Vec::new();
        let mut data_types: Vec<String> = Vec::new();
        let mut char_max_lengths: Vec<Option<i32>> = Vec::new();
        let mut char_octet_lengths: Vec<Option<i32>> = Vec::new();
        let mut num_precisions: Vec<Option<i32>> = Vec::new();
        let mut num_precision_radixes: Vec<Option<i32>> = Vec::new();
        let mut num_scales: Vec<Option<i32>> = Vec::new();
        let mut datetime_precisions: Vec<Option<i32>> = Vec::new();
        let mut dom_defaults: Vec<Option<String>> = Vec::new();
        let mut udt_catalogs: Vec<&str> = Vec::new();
        let mut udt_schemas: Vec<&str> = Vec::new();
        let mut udt_names: Vec<String> = Vec::new();
        for dom in &doms {
            let type_name = pg_type_name_for_arg(dom.base_type);
            dom_catalogs.push(BASIN_CATALOG_NAME);
            dom_schemas.push(DEFAULT_SCHEMA);
            dom_names.push(dom.name.clone());
            data_types.push(type_name.to_string());
            char_max_lengths.push(None);
            char_octet_lengths.push(None);
            num_precisions.push(None);
            num_precision_radixes.push(None);
            num_scales.push(None);
            datetime_precisions.push(None);
            dom_defaults.push(None);
            udt_catalogs.push(BASIN_CATALOG_NAME);
            udt_schemas.push(PG_CATALOG_SCHEMA);
            udt_names.push(type_name.to_string());
        }
        let schema = Self::domains_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(dom_catalogs)),
            Arc::new(StringArray::from(dom_schemas)),
            Arc::new(StringArray::from(dom_names)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(Int32Array::from(char_max_lengths)),
            Arc::new(Int32Array::from(char_octet_lengths)),
            Arc::new(Int32Array::from(num_precisions)),
            Arc::new(Int32Array::from(num_precision_radixes)),
            Arc::new(Int32Array::from(num_scales)),
            Arc::new(Int32Array::from(datetime_precisions)),
            Arc::new(StringArray::from(dom_defaults)),
            Arc::new(StringArray::from(udt_catalogs)),
            Arc::new(StringArray::from(udt_schemas)),
            Arc::new(StringArray::from(udt_names)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("information_schema.domains build: {e}")))
    }

    // -----------------------------------------------------------------------
    // information_schema.parameters  (function/procedure parameters)
    // -----------------------------------------------------------------------

    pub fn parameters_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("specific_catalog", DataType::Utf8, false),
            Field::new("specific_schema", DataType::Utf8, false),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("parameter_mode", DataType::Utf8, false),
            Field::new("is_result", DataType::Utf8, false),
            Field::new("as_locator", DataType::Utf8, false),
            Field::new("parameter_name", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::Int32, true),
            Field::new("character_octet_length", DataType::Int32, true),
            Field::new("numeric_precision", DataType::Int32, true),
            Field::new("numeric_precision_radix", DataType::Int32, true),
            Field::new("numeric_scale", DataType::Int32, true),
            Field::new("datetime_precision", DataType::Int32, true),
            Field::new("udt_catalog", DataType::Utf8, true),
            Field::new("udt_schema", DataType::Utf8, true),
            Field::new("udt_name", DataType::Utf8, true),
        ]))
    }

    pub async fn parameters(catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let functions = catalog.list_sql_functions(project).await;
        let procedures = catalog.list_procedures(project).await;
        let mut specific_catalogs: Vec<&str> = Vec::new();
        let mut specific_schemas: Vec<&str> = Vec::new();
        let mut specific_names: Vec<String> = Vec::new();
        let mut ordinal_positions: Vec<i32> = Vec::new();
        let mut parameter_modes: Vec<&str> = Vec::new();
        let mut is_results: Vec<&str> = Vec::new();
        let mut as_locators: Vec<&str> = Vec::new();
        let mut parameter_names: Vec<Option<String>> = Vec::new();
        let mut data_types: Vec<String> = Vec::new();
        let mut char_max_lengths: Vec<Option<i32>> = Vec::new();
        let mut char_octet_lengths: Vec<Option<i32>> = Vec::new();
        let mut num_precisions: Vec<Option<i32>> = Vec::new();
        let mut num_precision_radixes: Vec<Option<i32>> = Vec::new();
        let mut num_scales: Vec<Option<i32>> = Vec::new();
        let mut datetime_precisions: Vec<Option<i32>> = Vec::new();
        let mut udt_catalogs: Vec<Option<&str>> = Vec::new();
        let mut udt_schemas: Vec<Option<&str>> = Vec::new();
        let mut udt_names: Vec<Option<String>> = Vec::new();

        for func in &functions {
            for (i, arg) in func.args.iter().enumerate() {
                let type_name = pg_type_name_for_arg(arg.data_type);
                specific_catalogs.push(BASIN_CATALOG_NAME);
                specific_schemas.push(DEFAULT_SCHEMA);
                specific_names.push(func.name.clone());
                ordinal_positions.push((i + 1) as i32);
                parameter_modes.push("IN");
                is_results.push("NO");
                as_locators.push("NO");
                parameter_names.push(Some(arg.name.clone()));
                data_types.push(type_name.to_string());
                char_max_lengths.push(None);
                char_octet_lengths.push(None);
                num_precisions.push(None);
                num_precision_radixes.push(None);
                num_scales.push(None);
                datetime_precisions.push(None);
                udt_catalogs.push(None);
                udt_schemas.push(None);
                udt_names.push(None);
            }
        }
        for proc in &procedures {
            for (i, arg) in proc.args.iter().enumerate() {
                let type_name = pg_type_name_for_arg(arg.data_type);
                specific_catalogs.push(BASIN_CATALOG_NAME);
                specific_schemas.push(DEFAULT_SCHEMA);
                specific_names.push(proc.name.clone());
                ordinal_positions.push((i + 1) as i32);
                parameter_modes.push("IN");
                is_results.push("NO");
                as_locators.push("NO");
                parameter_names.push(Some(arg.name.clone()));
                data_types.push(type_name.to_string());
                char_max_lengths.push(None);
                char_octet_lengths.push(None);
                num_precisions.push(None);
                num_precision_radixes.push(None);
                num_scales.push(None);
                datetime_precisions.push(None);
                udt_catalogs.push(None);
                udt_schemas.push(None);
                udt_names.push(None);
            }
        }
        let schema = Self::parameters_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(specific_catalogs)),
            Arc::new(StringArray::from(specific_schemas)),
            Arc::new(StringArray::from(specific_names)),
            Arc::new(Int32Array::from(ordinal_positions)),
            Arc::new(StringArray::from(parameter_modes)),
            Arc::new(StringArray::from(is_results)),
            Arc::new(StringArray::from(as_locators)),
            Arc::new(StringArray::from(parameter_names)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(Int32Array::from(char_max_lengths)),
            Arc::new(Int32Array::from(char_octet_lengths)),
            Arc::new(Int32Array::from(num_precisions)),
            Arc::new(Int32Array::from(num_precision_radixes)),
            Arc::new(Int32Array::from(num_scales)),
            Arc::new(Int32Array::from(datetime_precisions)),
            Arc::new(StringArray::from(udt_catalogs)),
            Arc::new(StringArray::from(udt_schemas)),
            Arc::new(StringArray::from(udt_names)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("information_schema.parameters build: {e}")))
    }

    // -----------------------------------------------------------------------
    // information_schema.role_table_grants  (empty/calling-project-only)
    // -----------------------------------------------------------------------

    pub fn role_table_grants_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("grantor", DataType::Utf8, true),
            Field::new("grantee", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("privilege_type", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
            Field::new("with_hierarchy", DataType::Utf8, false),
        ]))
    }

    // -----------------------------------------------------------------------
    // information_schema.usage_privileges  (always-allow for calling project)
    // -----------------------------------------------------------------------

    pub fn usage_privileges_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("grantor", DataType::Utf8, true),
            Field::new("grantee", DataType::Utf8, false),
            Field::new("object_catalog", DataType::Utf8, false),
            Field::new("object_schema", DataType::Utf8, false),
            Field::new("object_name", DataType::Utf8, false),
            Field::new("object_type", DataType::Utf8, false),
            Field::new("privilege_type", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
        ]))
    }

    pub async fn usage_privileges(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        // Basin does not enforce SQL GRANT; return empty (no explicit grants).
        let schema = Self::usage_privileges_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.usage_privileges build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.table_privileges
    // -----------------------------------------------------------------------

    pub fn table_privileges_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("grantor", DataType::Utf8, true),
            Field::new("grantee", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("privilege_type", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
            Field::new("with_hierarchy", DataType::Utf8, false),
        ]))
    }

    pub async fn role_table_grants(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::role_table_grants_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.role_table_grants build: {e}"))
        })
    }

    pub async fn table_privileges(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        // Return one row per privilege type per table owned by the project.
        let names = catalog.list_tables(project).await?;
        let grantee = project.to_string();
        let privs = [
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "REFERENCES",
            "TRIGGER",
        ];
        let mut grantors: Vec<Option<String>> = Vec::new();
        let mut grantees: Vec<String> = Vec::new();
        let mut catalogs: Vec<&str> = Vec::new();
        let mut schemas: Vec<&str> = Vec::new();
        let mut tnames: Vec<String> = Vec::new();
        let mut ptypes: Vec<&str> = Vec::new();
        let mut is_grantables: Vec<&str> = Vec::new();
        let mut with_hierarchies: Vec<&str> = Vec::new();
        for name in &names {
            for priv_type in &privs {
                grantors.push(None);
                grantees.push(grantee.clone());
                catalogs.push(BASIN_CATALOG_NAME);
                schemas.push(DEFAULT_SCHEMA);
                tnames.push(name.as_str().to_string());
                ptypes.push(priv_type);
                is_grantables.push("YES");
                with_hierarchies.push("YES");
            }
        }
        let schema = Self::table_privileges_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(grantors)),
            Arc::new(StringArray::from(grantees)),
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(schemas)),
            Arc::new(StringArray::from(tnames)),
            Arc::new(StringArray::from(ptypes)),
            Arc::new(StringArray::from(is_grantables)),
            Arc::new(StringArray::from(with_hierarchies)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.table_privileges build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.column_privileges
    // -----------------------------------------------------------------------

    pub fn column_privileges_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("grantor", DataType::Utf8, true),
            Field::new("grantee", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("privilege_type", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
        ]))
    }

    pub async fn column_privileges(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        // Basin does not track per-column grants.
        let schema = Self::column_privileges_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.role_table_grants build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.user_defined_types  (alias to pg_type user types)
    // -----------------------------------------------------------------------

    pub fn user_defined_types_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("user_defined_type_catalog", DataType::Utf8, false),
            Field::new("user_defined_type_schema", DataType::Utf8, false),
            Field::new("user_defined_type_name", DataType::Utf8, false),
            Field::new("user_defined_type_category", DataType::Utf8, false),
            Field::new("is_instantiable", DataType::Utf8, false),
            Field::new("is_final", DataType::Utf8, false),
        ]))
    }

    pub async fn user_defined_types(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        // Include enum types and domains as user-defined types
        let enums = catalog.list_enum_types(project).await;
        let doms = catalog.list_domains(project).await;
        let mut udt_catalogs: Vec<&str> = Vec::new();
        let mut udt_schemas: Vec<&str> = Vec::new();
        let mut udt_names: Vec<String> = Vec::new();
        let mut udt_categories: Vec<&str> = Vec::new();
        let mut is_instantiables: Vec<&str> = Vec::new();
        let mut is_finals: Vec<&str> = Vec::new();
        for e in &enums {
            udt_catalogs.push(BASIN_CATALOG_NAME);
            udt_schemas.push(DEFAULT_SCHEMA);
            udt_names.push(e.name.clone());
            udt_categories.push("ENUM");
            is_instantiables.push("YES");
            is_finals.push("YES");
        }
        for d in &doms {
            udt_catalogs.push(BASIN_CATALOG_NAME);
            udt_schemas.push(DEFAULT_SCHEMA);
            udt_names.push(d.name.clone());
            udt_categories.push("DISTINCT");
            is_instantiables.push("YES");
            is_finals.push("YES");
        }
        let schema = Self::user_defined_types_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(udt_catalogs)),
            Arc::new(StringArray::from(udt_schemas)),
            Arc::new(StringArray::from(udt_names)),
            Arc::new(StringArray::from(udt_categories)),
            Arc::new(StringArray::from(is_instantiables)),
            Arc::new(StringArray::from(is_finals)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.user_defined_types build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.column_domain_usage  (domain columns)
    // -----------------------------------------------------------------------

    pub fn column_domain_usage_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("domain_catalog", DataType::Utf8, false),
            Field::new("domain_schema", DataType::Utf8, false),
            Field::new("domain_name", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
        ]))
    }

    // -----------------------------------------------------------------------
    // information_schema.role_column_grants
    // -----------------------------------------------------------------------

    pub fn role_column_grants_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("grantor", DataType::Utf8, true),
            Field::new("grantee", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("privilege_type", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
        ]))
    }

    pub async fn column_domain_usage(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(project).await?;
        let mut domain_catalogs: Vec<&str> = Vec::new();
        let mut domain_schemas: Vec<&str> = Vec::new();
        let mut domain_names: Vec<String> = Vec::new();
        let mut table_catalogs: Vec<&str> = Vec::new();
        let mut table_schemas: Vec<&str> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut column_names: Vec<String> = Vec::new();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let arrow_schema = &meta.schema;
            for field in arrow_schema.fields() {
                if let Some(domain_name) = field.metadata().get("BASIN_DOMAIN") {
                    domain_catalogs.push(BASIN_CATALOG_NAME);
                    domain_schemas.push(DEFAULT_SCHEMA);
                    domain_names.push(domain_name.clone());
                    table_catalogs.push(BASIN_CATALOG_NAME);
                    table_schemas.push(DEFAULT_SCHEMA);
                    table_names.push(name.as_str().to_string());
                    column_names.push(field.name().clone());
                }
            }
        }
        let schema = Self::column_domain_usage_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(domain_catalogs)),
            Arc::new(StringArray::from(domain_schemas)),
            Arc::new(StringArray::from(domain_names)),
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(column_names)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.column_domain_usage build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.column_udt_usage  (columns with user-defined types)
    // -----------------------------------------------------------------------

    pub fn column_udt_usage_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("udt_catalog", DataType::Utf8, false),
            Field::new("udt_schema", DataType::Utf8, false),
            Field::new("udt_name", DataType::Utf8, false),
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
        ]))
    }

    pub async fn column_udt_usage(
        catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        // Reuse column_domain_usage data — domain columns also count as UDT usage
        let names = catalog.list_tables(project).await?;
        let mut udt_catalogs: Vec<&str> = Vec::new();
        let mut udt_schemas: Vec<&str> = Vec::new();
        let mut udt_names: Vec<String> = Vec::new();
        let mut table_catalogs: Vec<&str> = Vec::new();
        let mut table_schemas: Vec<&str> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut column_names: Vec<String> = Vec::new();
        for name in &names {
            let meta = catalog.load_table(project, name).await?;
            let arrow_schema = &meta.schema;
            for field in arrow_schema.fields() {
                if let Some(domain_name) = field.metadata().get("BASIN_DOMAIN") {
                    udt_catalogs.push(BASIN_CATALOG_NAME);
                    udt_schemas.push(DEFAULT_SCHEMA);
                    udt_names.push(domain_name.clone());
                    table_catalogs.push(BASIN_CATALOG_NAME);
                    table_schemas.push(DEFAULT_SCHEMA);
                    table_names.push(name.as_str().to_string());
                    column_names.push(field.name().clone());
                }
            }
        }
        let schema = Self::column_udt_usage_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(udt_catalogs)),
            Arc::new(StringArray::from(udt_schemas)),
            Arc::new(StringArray::from(udt_names)),
            Arc::new(StringArray::from(table_catalogs)),
            Arc::new(StringArray::from(table_schemas)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(column_names)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.column_udt_usage build: {e}"))
        })
    }

    pub async fn role_column_grants(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::role_column_grants_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.role_column_grants build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.role_routine_grants
    // -----------------------------------------------------------------------

    pub fn role_routine_grants_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("grantor", DataType::Utf8, true),
            Field::new("grantee", DataType::Utf8, false),
            Field::new("specific_catalog", DataType::Utf8, false),
            Field::new("specific_schema", DataType::Utf8, false),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("routine_catalog", DataType::Utf8, false),
            Field::new("routine_schema", DataType::Utf8, false),
            Field::new("routine_name", DataType::Utf8, false),
            Field::new("privilege_type", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
        ]))
    }

    pub async fn role_routine_grants(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::role_routine_grants_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.role_routine_grants build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.applicable_roles
    // -----------------------------------------------------------------------

    pub fn applicable_roles_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("grantee", DataType::Utf8, false),
            Field::new("role_name", DataType::Utf8, false),
            Field::new("is_grantable", DataType::Utf8, false),
        ]))
    }

    pub async fn applicable_roles(
        _catalog: &dyn Catalog,
        project: &ProjectId,
    ) -> Result<RecordBatch> {
        let rolname = project.to_string();
        let schema = Self::applicable_roles_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![rolname.as_str()])),
            Arc::new(StringArray::from(vec![rolname.as_str()])),
            Arc::new(StringArray::from(vec!["YES"])),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.applicable_roles build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema.enabled_roles
    // -----------------------------------------------------------------------

    pub fn enabled_roles_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "role_name",
            DataType::Utf8,
            false,
        )]))
    }

    pub async fn enabled_roles(_catalog: &dyn Catalog, project: &ProjectId) -> Result<RecordBatch> {
        let rolname = project.to_string();
        let schema = Self::enabled_roles_schema();
        let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![rolname.as_str()]))];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.enabled_roles build: {e}"))
        })
    }

    // -----------------------------------------------------------------------
    // information_schema FDW / Foreign tables — all empty stubs
    // -----------------------------------------------------------------------

    pub fn foreign_data_wrappers_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("foreign_data_wrapper_catalog", DataType::Utf8, false),
            Field::new("foreign_data_wrapper_name", DataType::Utf8, false),
            Field::new("authorization_identifier", DataType::Utf8, true),
            Field::new("library_name", DataType::Utf8, true),
            Field::new("foreign_data_wrapper_language", DataType::Utf8, false),
        ]))
    }

    pub async fn foreign_data_wrappers(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::foreign_data_wrappers_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!(
                "information_schema.foreign_data_wrappers build: {e}"
            ))
        })
    }

    pub fn foreign_data_wrapper_options_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("foreign_data_wrapper_catalog", DataType::Utf8, false),
            Field::new("foreign_data_wrapper_name", DataType::Utf8, false),
            Field::new("option_name", DataType::Utf8, false),
            Field::new("option_value", DataType::Utf8, true),
        ]))
    }

    pub async fn foreign_data_wrapper_options(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::foreign_data_wrapper_options_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!(
                "information_schema.foreign_data_wrapper_options build: {e}"
            ))
        })
    }

    pub fn foreign_servers_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("foreign_server_catalog", DataType::Utf8, false),
            Field::new("foreign_server_name", DataType::Utf8, false),
            Field::new("foreign_data_wrapper_catalog", DataType::Utf8, false),
            Field::new("foreign_data_wrapper_name", DataType::Utf8, false),
            Field::new("foreign_server_type", DataType::Utf8, true),
            Field::new("foreign_server_version", DataType::Utf8, true),
            Field::new("authorization_identifier", DataType::Utf8, true),
        ]))
    }

    pub async fn foreign_servers(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::foreign_servers_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.foreign_servers build: {e}"))
        })
    }

    pub fn foreign_server_options_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("foreign_server_catalog", DataType::Utf8, false),
            Field::new("foreign_server_name", DataType::Utf8, false),
            Field::new("option_name", DataType::Utf8, false),
            Field::new("option_value", DataType::Utf8, true),
        ]))
    }

    pub async fn foreign_server_options(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::foreign_server_options_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!(
                "information_schema.foreign_server_options build: {e}"
            ))
        })
    }

    pub fn foreign_tables_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("foreign_table_catalog", DataType::Utf8, false),
            Field::new("foreign_table_schema", DataType::Utf8, false),
            Field::new("foreign_table_name", DataType::Utf8, false),
            Field::new("foreign_server_catalog", DataType::Utf8, false),
            Field::new("foreign_server_name", DataType::Utf8, false),
        ]))
    }

    pub async fn foreign_tables(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::foreign_tables_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.foreign_tables build: {e}"))
        })
    }

    pub fn foreign_table_options_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("foreign_table_catalog", DataType::Utf8, false),
            Field::new("foreign_table_schema", DataType::Utf8, false),
            Field::new("foreign_table_name", DataType::Utf8, false),
            Field::new("option_name", DataType::Utf8, false),
            Field::new("option_value", DataType::Utf8, true),
        ]))
    }

    pub async fn foreign_table_options(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::foreign_table_options_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!(
                "information_schema.foreign_table_options build: {e}"
            ))
        })
    }

    pub fn user_mappings_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("authorization_identifier", DataType::Utf8, false),
            Field::new("foreign_server_catalog", DataType::Utf8, false),
            Field::new("foreign_server_name", DataType::Utf8, false),
        ]))
    }

    pub async fn user_mappings(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::user_mappings_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("information_schema.user_mappings build: {e}"))
        })
    }

    pub fn user_mapping_options_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("authorization_identifier", DataType::Utf8, false),
            Field::new("foreign_server_catalog", DataType::Utf8, false),
            Field::new("foreign_server_name", DataType::Utf8, false),
            Field::new("option_name", DataType::Utf8, false),
            Field::new("option_value", DataType::Utf8, true),
        ]))
    }

    pub async fn user_mapping_options(
        _catalog: &dyn Catalog,
        _project: &ProjectId,
    ) -> Result<RecordBatch> {
        let schema = Self::user_mapping_options_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!(
                "information_schema.user_mapping_options build: {e}"
            ))
        })
    }
}

/// Database-level `table_catalog` value reported by
/// `information_schema.tables`. PG semantics: this is the database name
/// the connection is bound to. Basin is a single logical database in v0.1.
const BASIN_CATALOG_NAME: &str = "basin";

/// Schema name returned for every Basin table. v0.1 maps a project to
/// exactly one schema, named `"public"` to match the PG default and let
/// PostgREST / pgAdmin discover tables without configuration. Multi-schema
/// per project lands in v0.2; until then `relnamespace` and `table_schema`
/// always carry this value.
const DEFAULT_SCHEMA: &str = "public";

/// Synthetic schema name used as the namespace for `pg_catalog.pg_type`
/// rows. Every project's pg_type rows nominally live in the `pg_catalog`
/// namespace; the value participates only in the per-project FNV hash and
/// never surfaces directly in any query result.
const PG_CATALOG_SCHEMA: &str = "pg_catalog";

/// Static list of PG built-in types Basin's pgwire layer advertises.
///
/// Tuple shape: `(oid, typname, typtype, typcategory, typlen, typbyval)`.
/// Cross-reference `basin-router::types::arrow_to_pg_type` for the live
/// Arrow → PG OID mapping that drives the wire layer; the OIDs here must
/// stay aligned with `pg_type_oid_for_field` so `pg_attribute.atttypid`
/// JOINs against `pg_type.oid` resolve. v0.2 will append rows for
/// user-defined enum / domain types once those carry stable OIDs.
const BASIN_PG_TYPES: &[(i64, &str, &str, &str, i16, bool)] = &[
    (16, "bool", "b", "B", 1, true),
    (17, "bytea", "b", "U", -1, false),
    (20, "int8", "b", "N", 8, true),
    (21, "int2", "b", "N", 2, true),
    (23, "int4", "b", "N", 4, true),
    (25, "text", "b", "S", -1, false),
    (700, "float4", "b", "N", 4, true),
    (701, "float8", "b", "N", 8, true),
    (1082, "date", "b", "D", 4, true),
    (1114, "timestamp", "b", "D", 8, true),
    (1184, "timestamptz", "b", "D", 8, true),
    (1186, "interval", "b", "T", 16, false),
    (1700, "numeric", "b", "N", -1, false),
    (2950, "uuid", "b", "U", 16, false),
    (3802, "jsonb", "b", "U", -1, false),
];

/// `information_schema.tables.table_type` for base tables and matviews.
/// Matviews report as `BASE TABLE` per PG behaviour (the SQL-standard
/// view predates matviews and PG keeps them in `BASE TABLE`).
const TABLE_TYPE_BASE_TABLE: &str = "BASE TABLE";

/// `pg_class.relkind` literal for ordinary tables.
const RELKIND_TABLE: &str = "r";
/// `pg_class.relkind` literal for materialized views (continuous aggregates).
const RELKIND_MATVIEW: &str = "m";

/// Field-metadata key the engine uses for `GENERATED ALWAYS AS (<expr>)
/// STORED` columns. Duplicated here as a `&str` rather than imported from
/// `basin-engine` because `basin-engine` depends on `basin-catalog`,
/// not the other way around — pulling in the engine dep here would
/// create a cycle. The value lives in `basin_engine::types::BASIN_GENERATED_AS`
/// and the two strings must stay in sync.
const BASIN_GENERATED_AS_KEY: &str = "BASIN_GENERATED_AS";

/// Field-metadata key the engine sets on `JSONB` and `UUID` columns. Same
/// duplicate-the-string-don't-import-the-crate rule as `BASIN_GENERATED_AS_KEY`
/// above. The mirror constant lives in `basin_engine::types::BASIN_TYPE_KEY`.
const BASIN_TYPE_KEY: &str = "BASIN_TYPE";
const BASIN_TYPE_JSONB: &str = "JSONB";
const BASIN_TYPE_UUID: &str = "UUID";

/// `pg_namespace.nspowner` placeholder. v0.2 will populate this from the
/// auth subsystem once owner records carry an OID.
const NSPOWNER_PLACEHOLDER: i64 = 0;

/// `pg_proc.prokind` literal for ordinary functions.
const PROKIND_FUNCTION: &str = "f";
/// `pg_proc.prokind` literal for procedures.
const PROKIND_PROCEDURE: &str = "p";
/// `pg_proc.prolang` for `LANGUAGE sql`. PG hard-codes oid 14 for the SQL
/// language entry in `pg_language`; v0.1 only ships SQL bodies.
const PROLANG_SQL: i64 = 14;

/// `information_schema.routines.routine_type` for SQL functions.
const ROUTINE_TYPE_FUNCTION: &str = "FUNCTION";
/// `information_schema.routines.routine_type` for SQL procedures.
const ROUTINE_TYPE_PROCEDURE: &str = "PROCEDURE";
/// `information_schema.routines.routine_body` value. SQL-standard:
/// `"SQL"` for SQL bodies, `"EXTERNAL"` otherwise. v0.1 only ships SQL.
const ROUTINE_BODY_SQL: &str = "SQL";
/// `information_schema.routines.external_language` value. Mirrors PG's
/// behaviour of reporting `"SQL"` for `LANGUAGE sql` routines.
const EXTERNAL_LANGUAGE_SQL: &str = "SQL";

/// `information_schema.views.check_option` value. SQL-standard: `"NONE"`
/// when no `WITH CHECK OPTION` clause was attached. Basin's continuous
/// matviews can't be inserted into, so the check-option degrades to NONE.
const VIEW_CHECK_OPTION_NONE: &str = "NONE";
/// `information_schema.views.is_updatable` / `is_insertable_into` value.
/// Basin matviews are read-only — refresh is the only mutation surface,
/// and that goes through `REFRESH MATERIALIZED VIEW`, not `UPDATE` /
/// `INSERT`. Both columns report `"NO"`.
const VIEW_FLAG_NO: &str = "NO";

/// `information_schema.schemata.schema_owner` placeholder. Empty string
/// rather than NULL because the column is non-nullable in PG and the
/// SQL standard. v0.2 will populate this from the auth subsystem.
const SCHEMA_OWNER_PLACEHOLDER: &str = "";

/// `information_schema.table_constraints.constraint_type` literals.
/// PG-style spelling (`"PRIMARY KEY"`, `"FOREIGN KEY"`, `"CHECK"`,
/// `"NOT NULL"`). PG's spec doesn't list `"NOT NULL"` here, but Basin
/// follows the pragmatic PostgREST / pgAdmin convention of including
/// it; the row carries `is_deferrable = NO` like the others.
const CONSTRAINT_TYPE_NOT_NULL: &str = "NOT NULL";
const CONSTRAINT_TYPE_PRIMARY_KEY: &str = "PRIMARY KEY";
const CONSTRAINT_TYPE_FOREIGN_KEY: &str = "FOREIGN KEY";
const CONSTRAINT_TYPE_CHECK: &str = "CHECK";

/// `pg_constraint.contype` single-letter codes per PG docs. Basin emits
/// `p`/`f`/`c`/`n`; `u` (unique-only) is reserved for v0.2 when UNIQUE
/// constraints split out from PRIMARY KEY.
const CONTYPE_PRIMARY_KEY: &str = "p";
const CONTYPE_FOREIGN_KEY: &str = "f";
const CONTYPE_CHECK: &str = "c";
const CONTYPE_NOT_NULL: &str = "n";

/// PG-style referential action string for
/// `information_schema.referential_constraints.{update,delete}_rule`.
fn ref_action_to_pg(action: crate::metadata::RefAction) -> &'static str {
    match action {
        crate::metadata::RefAction::NoAction => "NO ACTION",
        crate::metadata::RefAction::Cascade => "CASCADE",
    }
}

/// `is_deferrable` / `initially_deferred` value used across the
/// constraint-introspection views. v0.1 has no deferrable constraints;
/// these columns are non-nullable in the SQL standard so `"NO"` is the
/// only valid encoding.
const CONSTRAINT_NO: &str = "NO";

/// `information_schema.referential_constraints.match_option`. PG default
/// is `"NONE"`; Basin doesn't support `MATCH PARTIAL` / `MATCH FULL`.
const MATCH_OPTION_NONE: &str = "NONE";

/// Synthesise the PG-style PK constraint name (`<table>_pkey`).
/// Documented contract: pinned by `pk_simple_unique_enforced` (the error
/// message must mention `users_pkey`).
fn pk_constraint_name(table: &TableName) -> String {
    format!("{}_pkey", table.as_str())
}

/// 1-based column index within `schema`. Mirrors PG `pg_attribute.attnum`
/// usage in `pg_constraint.conkey` (space-separated attnums).
fn attnum_in_schema(schema: &Schema, column: &str) -> Option<i32> {
    schema
        .fields()
        .iter()
        .position(|f| f.name() == column)
        .map(|i| (i + 1) as i32)
}

/// `pg_depend.deptype` literal for a normal dependency edge. PG semantics:
/// drop the referenced object cascades to the dependent. Basin v0.1 emits
/// only normal edges (no auto / internal / pin distinctions yet).
const DEPTYPE_NORMAL: &str = "n";

/// Synthesise the `constraint_name` for a NOT NULL constraint on
/// `(table, column)`. PG invents constraint names internally too — the
/// only durable contract is that the name is stable across queries
/// against the same `(project, table, column)` and unique within the
/// project's `table_constraints` rows. The convention is documented in
/// [`InfoSchemaQuery::table_constraints`] so the engine-side test can
/// assert against it.
fn not_null_constraint_name(table: &TableName, column: &str) -> String {
    format!("{}_{column}_not_null", table.as_str())
}

/// Map an Arrow [`Field`] to the `(data_type, udt_name)` pair Postgres
/// `information_schema.columns` exposes. `data_type` is the
/// human-readable SQL standard name; `udt_name` is the underlying PG
/// type name (e.g. `int4`, `timestamptz`). Both come from the same
/// physical-type decision the pgwire encoder makes (`basin-router::types`)
/// so the OIDs the wire layer advertises stay in sync with what
/// introspection clients see in `information_schema.columns`.
fn pg_type_for_field(field: &Field) -> (&'static str, &'static str) {
    // Logical-type markers win: a JSONB / UUID column is `LargeBinary` /
    // `FixedSizeBinary(16)` at the Arrow layer but must surface as
    // `jsonb` / `uuid` to introspecting tooling.
    if field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_JSONB) {
        return ("jsonb", "jsonb");
    }
    if field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID) {
        return ("uuid", "uuid");
    }
    match field.data_type() {
        DataType::Boolean => ("boolean", "bool"),
        DataType::Int16 => ("smallint", "int2"),
        DataType::Int32 => ("integer", "int4"),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => ("bigint", "int8"),
        DataType::Float32 => ("real", "float4"),
        DataType::Float64 => ("double precision", "float8"),
        DataType::Utf8 | DataType::LargeUtf8 => ("text", "text"),
        DataType::Binary | DataType::LargeBinary => ("bytea", "bytea"),
        DataType::FixedSizeBinary(_) => ("bytea", "bytea"),
        DataType::Date32 => ("date", "date"),
        DataType::Timestamp(_, Some(_)) => ("timestamp with time zone", "timestamptz"),
        DataType::Timestamp(_, None) => ("timestamp without time zone", "timestamp"),
        DataType::Interval(IntervalUnit::MonthDayNano) => ("interval", "interval"),
        // PG `numeric` rides on Decimal128 — surface as the SQL standard
        // name in `data_type` and PG's short udt_name `numeric`.
        DataType::Decimal128(_, _) => ("numeric", "numeric"),
        // FixedSizeList<Float32> is the engine's `vector(N)` shape.
        DataType::FixedSizeList(child, _) if matches!(child.data_type(), DataType::Float32) => {
            ("USER-DEFINED", "vector")
        }
        // Everything we don't recognise falls back to `text`. Matches the
        // basin-router fallback (Type::TEXT for unmapped Arrow types).
        _ => ("text", "text"),
    }
}

/// Map an Arrow [`Field`] to the Postgres type OID `pg_catalog.pg_attribute`
/// reports in `atttypid`. Mirrors the OIDs `basin-router::types` advertises
/// in `RowDescription` so a project joining `pg_attribute` against the wire
/// layer's `RowDescription.type_id` sees consistent values.
///
/// OID reference: <https://www.postgresql.org/docs/current/catalog-pg-type.html>.
fn pg_type_oid_for_field(field: &Field) -> i64 {
    if field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_JSONB) {
        return 3802; // jsonb
    }
    if field.metadata().get(BASIN_TYPE_KEY).map(|s| s.as_str()) == Some(BASIN_TYPE_UUID) {
        return 2950; // uuid
    }
    match field.data_type() {
        DataType::Boolean => 16,
        DataType::Int16 => 21,
        DataType::Int32 => 23,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::Utf8 | DataType::LargeUtf8 => 25,
        DataType::Binary | DataType::LargeBinary => 17,
        DataType::FixedSizeBinary(_) => 17,
        DataType::Date32 => 1082,
        DataType::Timestamp(_, Some(_)) => 1184,
        DataType::Timestamp(_, None) => 1114,
        DataType::Interval(IntervalUnit::MonthDayNano) => 1186,
        // PG `numeric` (OID 1700). Mirrors the `BASIN_PG_TYPES` row.
        DataType::Decimal128(_, _) => 1700,
        // Unknown / fallback → text (25). Matches the router's fallback so
        // wire-layer and introspection-layer agree on the mismapped type.
        _ => 25,
    }
}

/// Stable 64-bit oid for a `(project, table)` pair.
///
/// Hashing scheme: FNV-1a 64-bit over the byte sequence
/// `b"basin.pg_class:" || project.to_string() || ":" || table.as_str()`,
/// then masked to 63 bits to fit a positive `i64` (PG's `oid` is
/// unsigned 32-bit; we widen to `i64` because Basin's identifier space
/// is per-project and a 32-bit hash collides too cheaply across the full
/// fleet). Properties:
///
/// - **Stable**: the same `(project, table)` always hashes to the same
///   oid across process restarts and across in-memory / Postgres backends.
/// - **Per-project disjoint by construction**: the project ULID is part of
///   the input, so two projects with identically-named tables get
///   different oids. Cross-project oid collisions are a per-table
///   birthday problem in 2^63 space (negligible at any plausible scale).
/// - **Same-project collision**: 2^63 hash space; same-project collisions
///   would surface as a `pg_class` row pair sharing an oid. Not a
///   correctness concern for the views (PostgREST doesn't dedupe by oid)
///   but worth flagging for the v0.2 catalog-side oid registry which
///   will replace this hash with a monotonic counter.
///
/// This is intentionally _not_ persistence-versioned: changing the input
/// format here changes every oid downstream clients have cached, so the
/// constant prefix (`b"basin.pg_class:"`) is load-bearing for stability.
fn table_oid(project: &ProjectId, table: &TableName) -> i64 {
    let key = format!("basin.pg_class:{project}:{}", table.as_str());
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable oid for a project-scoped namespace. v0.1 has one namespace per
/// project (`"public"`); the function takes the schema name explicitly so
/// the v0.2 multi-schema upgrade is a non-breaking signature extension.
fn namespace_oid_for(project: &ProjectId, schema: &str) -> i64 {
    let key = format!("basin.pg_namespace:{project}:{schema}");
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable 64-bit oid for a `(project, routine_name)` pair. Mirrors
/// [`table_oid`] but uses a distinct prefix so a function and a table
/// with the same name in the same project do not collide on oid.
fn routine_oid(project: &ProjectId, name: &str) -> i64 {
    let key = format!("basin.pg_proc:{project}:{name}");
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable synthetic OID for one of the system catalog tables themselves
/// (`pg_class`, `pg_proc`, `pg_type`, …) within a project's namespace.
/// Used as `classid` / `refclassid` in `pg_catalog.pg_depend` rows.
///
/// Reuses the same FNV-1a-then-positive-i64 hash family as the rest of
/// the M-starter so the resulting OIDs are stable across process restarts
/// and disjoint between projects. The catalog-table label (`"pg_class"`,
/// `"pg_proc"`, `"pg_type"`) participates in the hash so the three
/// labels never collide on OID for the same project.
fn catalog_table_oid(project: &ProjectId, table: &str) -> i64 {
    let key = format!("basin.pg_catalog_table:{project}:{table}");
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable role OID for `pg_authid`. v0.1 maps each project to exactly one
/// "role" row, so the OID is a per-project FNV-1a hash with a distinct
/// prefix from [`table_oid`] / [`routine_oid`] / [`namespace_oid_for`].
fn role_oid_for(project: &ProjectId) -> i64 {
    let key = format!("basin.pg_authid:{project}");
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Map a [`SqlArgType`] to the PG type OID used in `pg_attribute.atttypid`
/// / `pg_proc.prorettype` / `pg_proc.proargtypes`. Routes through an
/// Arrow `Field` so the OID table lives in exactly one place
/// ([`pg_type_oid_for_field`]).
fn pg_type_oid_for_arg(arg: SqlArgType) -> i64 {
    let dt = match arg {
        SqlArgType::Boolean => DataType::Boolean,
        SqlArgType::Int => DataType::Int32,
        SqlArgType::BigInt => DataType::Int64,
        SqlArgType::Double => DataType::Float64,
        SqlArgType::Text => DataType::Utf8,
        SqlArgType::Bytea => DataType::LargeBinary,
        SqlArgType::Date => DataType::Date32,
        SqlArgType::TimestampTz => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        }
        SqlArgType::Timestamp => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
    };
    pg_type_oid_for_field(&Field::new("_", dt, false))
}

/// PG type name for [`SqlArgType`], used in `routines.data_type`.
/// Matches the strings PG reports for the same types in
/// `information_schema.columns.data_type`.
fn pg_type_name_for_arg(arg: SqlArgType) -> &'static str {
    match arg {
        SqlArgType::Boolean => "boolean",
        SqlArgType::Int => "integer",
        SqlArgType::BigInt => "bigint",
        SqlArgType::Double => "double precision",
        SqlArgType::Text => "text",
        SqlArgType::Bytea => "bytea",
        SqlArgType::Date => "date",
        SqlArgType::TimestampTz => "timestamp with time zone",
        SqlArgType::Timestamp => "timestamp without time zone",
    }
}

/// Format the PG `oidvector` text rendering (`"23 25 1184"`) for a
/// procedure or function's positional argument list.
fn format_argtypes(args: impl Iterator<Item = SqlArgType>) -> String {
    let parts: Vec<String> = args.map(|a| pg_type_oid_for_arg(a).to_string()).collect();
    parts.join(" ")
}

/// PG return-type OID for a [`SqlReturnType`]. v0.1 functions only emit
/// `Scalar`; the `Table` variant is reserved for future `RETURNS TABLE`
/// (5.11.E follow-up) and reports as `0` (PG uses `RECORD = 2249`, but
/// the catalog can't yet round-trip the row shape so 0 keeps the column
/// honest until that lands).
fn return_type_oid(rt: &SqlReturnType) -> i64 {
    match rt {
        SqlReturnType::Scalar(t) => pg_type_oid_for_arg(*t),
        SqlReturnType::Table(_) => 0,
    }
}

/// PG return-type name for a [`SqlReturnType`]. `Table` returns
/// `"record"` (PG convention for set-returning functions whose row
/// shape isn't a single declared composite type).
fn return_type_name(rt: &SqlReturnType) -> &'static str {
    match rt {
        SqlReturnType::Scalar(t) => pg_type_name_for_arg(*t),
        SqlReturnType::Table(_) => "record",
    }
}

/// Heuristic for `routines.is_deterministic`. We treat a body as
/// non-deterministic if it textually mentions any of:
///
/// - `nextval(` — sequence advance, mutates server state and returns a
///   fresh value each call.
/// - `random(` — non-deterministic by definition.
/// - `now(` — wall-clock, varies between calls.
/// - `current_timestamp` — same as `now()`.
///
/// The match is case-insensitive and substring-based; we deliberately
/// do not parse the body because (a) the SQL parser is expensive to
/// invoke per introspection row, and (b) any false positive (e.g. a
/// column named `now_ts`) errs on the safe side — declaring a routine
/// non-deterministic when it isn't is harmless to query planners and
/// caches; the inverse would let a planner cache a stale value.
fn is_deterministic_body(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    !(lower.contains("nextval(")
        || lower.contains("random(")
        || lower.contains("now(")
        || lower.contains("current_timestamp"))
}

/// FNV-1a 64-bit, masked to 63 bits so the result fits a positive `i64`.
/// FNV is stable across compiler versions (unlike `std::hash::DefaultHasher`)
/// and dependency-free.
fn fnv1a_64_to_positive_i64(bytes: &[u8]) -> i64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h: u64 = FNV_OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    (h & 0x7fff_ffff_ffff_ffff) as i64
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.19.B — GIN index introspection helper
// ─────────────────────────────────────────────────────────────────────────────

/// Build the `indexdef` string for a [`crate::metadata::SecondaryIndex`] in the
/// style PostgreSQL uses for `pg_indexes.indexdef`:
///
/// - B-tree (default): `CREATE INDEX <name> ON <table> (<cols>)`
/// - GIN without opclass: `CREATE INDEX <name> ON <table> USING gin (<col>)`
/// - GIN with opclass: `CREATE INDEX <name> ON <table> USING gin (<col> jsonb_path_ops)`
///
/// The result matches what `\d <tbl>` and `psql` display, so ORM migration
/// tooling that reads `pg_indexes` to verify applied migrations sees the
/// correct DDL text.
fn build_indexdef(table_name: &str, idx: &crate::metadata::SecondaryIndex) -> String {
    let is_gin = idx.access_method.eq_ignore_ascii_case("gin");
    if is_gin {
        // GIN indexes are always single-column in Basin v0.1.
        // Include the opclass when it was explicitly declared.
        let col_part = match idx.columns.first() {
            Some(col) => match &idx.opclass {
                Some(opclass) => format!("{col} {opclass}"),
                None => col.clone(),
            },
            None => String::new(),
        };
        format!(
            "CREATE INDEX {} ON {} USING gin ({})",
            idx.name, table_name, col_part
        )
    } else {
        // B-tree (and any unrecognised access method): omit USING clause for
        // back-compatibility with the PK indexdef format already in use.
        let cols = idx.columns.join(", ");
        format!("CREATE INDEX {} ON {} ({})", idx.name, table_name, cols)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5.18.D — honest schema introspection tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests_5_18_d {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_common::{ProjectId, QualifiedTableName, SchemaName, TableName};

    use crate::reserved_schema::ReservedSchema;
    use crate::InMemoryCatalog;
    use crate::Catalog;

    use super::InfoSchemaQuery;

    fn minimal_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn qtable(schema: &str, table: &str) -> QualifiedTableName {
        QualifiedTableName::new(
            SchemaName::new(schema).unwrap(),
            TableName::new(table).unwrap(),
        )
    }

    // ── schemata ─────────────────────────────────────────────────────────────

    /// `information_schema.schemata` must list all reserved schemas.
    #[tokio::test]
    async fn schemata_lists_all_reserved_schemas() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let batch = InfoSchemaQuery::schemata(&cat, &p).await.unwrap();

        // One row per reserved schema.
        assert_eq!(
            batch.num_rows(),
            ReservedSchema::ALL.len(),
            "schemata row count should equal number of reserved schemas"
        );

        // Extract schema_name column and collect into a vec for assertion.
        let schema_names = batch
            .column(1) // schema_name is col index 1
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("schema_name must be StringArray");

        let mut names: Vec<&str> = (0..schema_names.len())
            .map(|i| schema_names.value(i))
            .collect();
        names.sort();

        let mut expected: Vec<&str> = ReservedSchema::ALL.iter().map(|r| r.as_str()).collect();
        expected.sort();

        assert_eq!(names, expected, "schemata must cover every reserved schema");
    }

    /// `information_schema.schemata` must include `"public"`.
    #[tokio::test]
    async fn schemata_includes_public() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let batch = InfoSchemaQuery::schemata(&cat, &p).await.unwrap();
        let schema_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let has_public = (0..schema_names.len()).any(|i| schema_names.value(i) == "public");
        assert!(has_public, "schemata must include 'public'");
    }

    /// `information_schema.schemata` must include `"auth"`.
    #[tokio::test]
    async fn schemata_includes_auth() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let batch = InfoSchemaQuery::schemata(&cat, &p).await.unwrap();
        let schema_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let has_auth = (0..schema_names.len()).any(|i| schema_names.value(i) == "auth");
        assert!(has_auth, "schemata must include 'auth'");
    }

    // ── information_schema.tables ─────────────────────────────────────────────

    /// A table created in `auth` schema must report `table_schema = 'auth'`.
    #[tokio::test]
    async fn tables_auth_table_reports_auth_schema() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let qt = qtable("auth", "users");
        cat.create_table_qualified(&p, &qt, minimal_schema())
            .await
            .unwrap();

        let batch = InfoSchemaQuery::tables(&cat, &p).await.unwrap();

        let table_names = batch
            .column(2) // table_name col index 2
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let table_schemas = batch
            .column(1) // table_schema col index 1
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Find the row for the `users` table.
        let idx = (0..table_names.len())
            .find(|&i| table_names.value(i) == "users")
            .expect("users table must appear in information_schema.tables");

        assert_eq!(
            table_schemas.value(idx),
            "auth",
            "auth.users must report table_schema = 'auth'"
        );
    }

    /// A table in `public` must still report `table_schema = 'public'`.
    #[tokio::test]
    async fn tables_public_table_reports_public_schema() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let qt = QualifiedTableName::in_public(TableName::new("orders").unwrap());
        cat.create_table_qualified(&p, &qt, minimal_schema())
            .await
            .unwrap();

        let batch = InfoSchemaQuery::tables(&cat, &p).await.unwrap();

        let table_names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let table_schemas = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let idx = (0..table_names.len())
            .find(|&i| table_names.value(i) == "orders")
            .expect("orders table must appear in information_schema.tables");

        assert_eq!(
            table_schemas.value(idx),
            "public",
            "public.orders must report table_schema = 'public'"
        );
    }

    /// Tables in different schemas coexist and each reports its own schema.
    #[tokio::test]
    async fn tables_multi_schema_reporting() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        cat.create_table_qualified(&p, &qtable("auth", "users"), minimal_schema())
            .await
            .unwrap();
        cat.create_table_qualified(&p, &qtable("storage", "objects"), minimal_schema())
            .await
            .unwrap();
        cat.create_table_qualified(
            &p,
            &QualifiedTableName::in_public(TableName::new("orders").unwrap()),
            minimal_schema(),
        )
        .await
        .unwrap();

        let batch = InfoSchemaQuery::tables(&cat, &p).await.unwrap();

        let table_names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let table_schemas = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut schema_by_table: std::collections::HashMap<&str, &str> =
            std::collections::HashMap::new();
        for i in 0..table_names.len() {
            schema_by_table.insert(table_names.value(i), table_schemas.value(i));
        }

        assert_eq!(schema_by_table.get("users"), Some(&"auth"));
        assert_eq!(schema_by_table.get("objects"), Some(&"storage"));
        assert_eq!(schema_by_table.get("orders"), Some(&"public"));
    }

    // ── pg_namespace ─────────────────────────────────────────────────────────

    /// `pg_catalog.pg_namespace` must have one row per reserved schema.
    #[tokio::test]
    async fn pg_namespace_lists_all_reserved_schemas() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let batch = InfoSchemaQuery::pg_namespace(&cat, &p).await.unwrap();

        assert_eq!(
            batch.num_rows(),
            ReservedSchema::ALL.len(),
            "pg_namespace row count should equal number of reserved schemas"
        );

        let nspnames = batch
            .column(1) // nspname is col index 1
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut names: Vec<&str> = (0..nspnames.len()).map(|i| nspnames.value(i)).collect();
        names.sort();

        let mut expected: Vec<&str> = ReservedSchema::ALL.iter().map(|r| r.as_str()).collect();
        expected.sort();

        assert_eq!(names, expected, "pg_namespace must cover every reserved schema");
    }

    /// `pg_namespace` oids must be stable across two calls (deterministic).
    #[tokio::test]
    async fn pg_namespace_oids_are_stable() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let batch1 = InfoSchemaQuery::pg_namespace(&cat, &p).await.unwrap();
        let batch2 = InfoSchemaQuery::pg_namespace(&cat, &p).await.unwrap();

        // Collect oid→name maps for both calls.

        let oids1 = batch1.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let oids2 = batch2.column(0).as_any().downcast_ref::<Int64Array>().unwrap();

        let oids1_vec: Vec<i64> = (0..oids1.len()).map(|i| oids1.value(i)).collect();
        let oids2_vec: Vec<i64> = (0..oids2.len()).map(|i| oids2.value(i)).collect();

        assert_eq!(oids1_vec, oids2_vec, "pg_namespace oids must be stable across calls");
    }

    // ── pg_class.relnamespace ─────────────────────────────────────────────────

    /// `pg_class.relnamespace` for an auth-schema table must equal the
    /// `pg_namespace.oid` for `"auth"`.
    #[tokio::test]
    async fn pg_class_relnamespace_matches_auth_namespace_oid() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let qt = qtable("auth", "users");
        cat.create_table_qualified(&p, &qt, minimal_schema())
            .await
            .unwrap();

        let pg_class_batch = InfoSchemaQuery::pg_class(&cat, &p).await.unwrap();
        let pg_ns_batch = InfoSchemaQuery::pg_namespace(&cat, &p).await.unwrap();

        // Find the namespace oid for "auth".

        let ns_oids = pg_ns_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ns_names = pg_ns_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let auth_oid = (0..ns_names.len())
            .find(|&i| ns_names.value(i) == "auth")
            .map(|i| ns_oids.value(i))
            .expect("pg_namespace must contain an 'auth' row");

        // Find the relnamespace for the "users" table in pg_class.
        let relnames = pg_class_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let relnamespaces = pg_class_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let users_ns = (0..relnames.len())
            .find(|&i| relnames.value(i) == "users")
            .map(|i| relnamespaces.value(i))
            .expect("pg_class must contain a 'users' row");

        assert_eq!(
            users_ns, auth_oid,
            "pg_class.relnamespace for auth.users must equal pg_namespace.oid for 'auth'"
        );
    }

    /// `pg_class.relnamespace` for a public-schema table must equal the
    /// `pg_namespace.oid` for `"public"`.
    #[tokio::test]
    async fn pg_class_relnamespace_matches_public_namespace_oid() {
        let cat = InMemoryCatalog::new();
        let p = ProjectId::new();
        cat.create_namespace(&p).await.unwrap();

        let qt = QualifiedTableName::in_public(TableName::new("orders").unwrap());
        cat.create_table_qualified(&p, &qt, minimal_schema())
            .await
            .unwrap();

        let pg_class_batch = InfoSchemaQuery::pg_class(&cat, &p).await.unwrap();
        let pg_ns_batch = InfoSchemaQuery::pg_namespace(&cat, &p).await.unwrap();


        let ns_oids = pg_ns_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ns_names = pg_ns_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let public_oid = (0..ns_names.len())
            .find(|&i| ns_names.value(i) == "public")
            .map(|i| ns_oids.value(i))
            .expect("pg_namespace must contain a 'public' row");

        let relnames = pg_class_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let relnamespaces = pg_class_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let orders_ns = (0..relnames.len())
            .find(|&i| relnames.value(i) == "orders")
            .map(|i| relnamespaces.value(i))
            .expect("pg_class must contain an 'orders' row");

        assert_eq!(
            orders_ns, public_oid,
            "pg_class.relnamespace for public.orders must equal pg_namespace.oid for 'public'"
        );
    }
}
