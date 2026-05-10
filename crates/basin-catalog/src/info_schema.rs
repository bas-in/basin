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
//! - Filter to the calling tenant's tables at query-construction time;
//!   never materialise cross-tenant rows.
//!
//! Oid-hashing scheme: see [`stable_oid`].

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Int16Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, IntervalUnit, Schema};
use basin_common::{BasinError, Result, TableName, TenantId};

use crate::functions::{SqlArgType, SqlReturnType};
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
    /// | table_schema   | TEXT | always `"public"` (single-schema-per-tenant)   |
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
    /// | oid             | BIGINT | stable per-(tenant, table) hash         |
    /// | relname         | TEXT   | table name within the tenant            |
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

    /// Build `information_schema.tables` filtered to `tenant`'s tables.
    ///
    /// Cross-tenant leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `tenant`.
    pub async fn tables(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let names = catalog.list_tables(tenant).await?;

        let mut catalogs: Vec<&str> = Vec::with_capacity(names.len());
        let mut schemas: Vec<&str> = Vec::with_capacity(names.len());
        let mut table_names: Vec<String> = Vec::with_capacity(names.len());
        let mut table_types: Vec<&str> = Vec::with_capacity(names.len());

        // We need the per-table metadata to distinguish materialized
        // views from base tables. PG reports MVs as `'BASE TABLE'` in
        // `information_schema.tables` (the SQL-standard view doesn't
        // know about matviews); we mirror that for compatibility.
        for name in &names {
            let _meta = catalog.load_table(tenant, name).await?;
            catalogs.push(BASIN_CATALOG_NAME);
            schemas.push(DEFAULT_SCHEMA);
            table_names.push(name.as_str().to_string());
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

    /// Build `pg_catalog.pg_class` filtered to `tenant`'s tables.
    ///
    /// Cross-tenant leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `tenant`.
    pub async fn pg_class(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let names = catalog.list_tables(tenant).await?;

        let mut oids: Vec<i64> = Vec::with_capacity(names.len());
        let mut relnames: Vec<String> = Vec::with_capacity(names.len());
        let mut namespaces: Vec<i64> = Vec::with_capacity(names.len());
        let mut relkinds: Vec<&str> = Vec::with_capacity(names.len());
        let mut rls: Vec<bool> = Vec::with_capacity(names.len());
        let mut partitioned: Vec<bool> = Vec::with_capacity(names.len());
        let mut reltuples: Vec<f32> = Vec::with_capacity(names.len());

        let namespace_oid = namespace_oid_for(tenant, DEFAULT_SCHEMA);

        for name in &names {
            let meta = catalog.load_table(tenant, name).await?;
            oids.push(table_oid(tenant, name));
            relnames.push(name.as_str().to_string());
            namespaces.push(namespace_oid);
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
    /// One row per (table, column) belonging to the calling tenant. Column
    /// names match the SQL-standard / PG layout exactly so PostgREST,
    /// pgAdmin, and ORMs that probe `information_schema.columns` for
    /// type / nullability / ordering metadata receive what they expect.
    ///
    /// | column            | type    | notes                                  |
    /// |-------------------|---------|----------------------------------------|
    /// | table_catalog     | TEXT    | always `"basin"`                       |
    /// | table_schema      | TEXT    | always `"public"`                      |
    /// | table_name        | TEXT    | tenant-local table name                |
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
    /// One row per (table, column) belonging to the calling tenant.
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
    /// v0.1 emits exactly one row per tenant (`"public"`); multi-schema
    /// per tenant is a v0.2 extension.
    ///
    /// | column     | type   | notes                                       |
    /// |------------|--------|---------------------------------------------|
    /// | oid        | BIGINT | namespace oid (FNV-1a of `(tenant, "public")`) |
    /// | nspname    | TEXT   | always `"public"` for v0.1                   |
    /// | nspowner   | BIGINT | 0 (placeholder; v0.2 wires real owner)       |
    pub fn pg_namespace_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("nspname", DataType::Utf8, false),
            Field::new("nspowner", DataType::Int64, false),
        ]))
    }

    /// Build `information_schema.columns` filtered to `tenant`'s tables.
    ///
    /// Cross-tenant leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `tenant`.
    pub async fn columns(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let names = catalog.list_tables(tenant).await?;

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
            let meta = catalog.load_table(tenant, name).await?;
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

    /// Build `pg_catalog.pg_attribute` filtered to `tenant`'s tables.
    ///
    /// Cross-tenant leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `tenant`.
    pub async fn pg_attribute(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let names = catalog.list_tables(tenant).await?;

        let mut attrelids: Vec<i64> = Vec::new();
        let mut attnames: Vec<String> = Vec::new();
        let mut atttypids: Vec<i64> = Vec::new();
        let mut attnums: Vec<i16> = Vec::new();
        let mut attnotnulls: Vec<bool> = Vec::new();
        let mut atthasdefs: Vec<bool> = Vec::new();
        let mut attisdroppeds: Vec<bool> = Vec::new();

        for name in &names {
            let meta = catalog.load_table(tenant, name).await?;
            let relid = table_oid(tenant, name);
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

    /// Build `pg_catalog.pg_namespace` filtered to `tenant`. v0.1 emits
    /// exactly one row (`"public"`) — single-schema-per-tenant is the
    /// invariant the rest of the views (`pg_class.relnamespace`,
    /// `information_schema.tables.table_schema`) already encode.
    pub async fn pg_namespace(_catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let oid = namespace_oid_for(tenant, DEFAULT_SCHEMA);
        let schema = Self::pg_namespace_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![oid])),
            Arc::new(StringArray::from(vec![DEFAULT_SCHEMA.to_string()])),
            Arc::new(Int64Array::from(vec![NSPOWNER_PLACEHOLDER])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("pg_catalog.pg_namespace build: {e}")))
    }

    /// Schema for `pg_catalog.pg_proc` rows.
    ///
    /// One row per user-defined function (`prokind = 'f'`) and procedure
    /// (`prokind = 'p'`) registered for the tenant.
    ///
    /// | column        | type    | notes                                     |
    /// |---------------|---------|-------------------------------------------|
    /// | oid           | BIGINT  | FNV-1a of `(tenant, name)` (same scheme)  |
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

    /// Build `pg_catalog.pg_proc` filtered to `tenant`'s functions and
    /// procedures. Cross-tenant leak is a P0 invariant: only
    /// [`Catalog::list_sql_functions`] / [`Catalog::list_procedures`] for
    /// `tenant` are consulted.
    pub async fn pg_proc(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let funcs = catalog.list_sql_functions(tenant).await;
        let procs = catalog.list_procedures(tenant).await;
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

        let namespace_oid = namespace_oid_for(tenant, DEFAULT_SCHEMA);

        for f in &funcs {
            oids.push(routine_oid(tenant, &f.name));
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
            oids.push(routine_oid(tenant, &p.name));
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

    /// Build `information_schema.routines` filtered to `tenant`'s
    /// functions and procedures. Cross-tenant leak is a P0 invariant:
    /// only [`Catalog::list_sql_functions`] / [`Catalog::list_procedures`]
    /// for `tenant` are consulted.
    pub async fn routines(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let funcs = catalog.list_sql_functions(tenant).await;
        let procs = catalog.list_procedures(tenant).await;
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

    /// Build `pg_catalog.pg_index` filtered to `tenant`. Always empty in
    /// v0.1 (no user-defined indexes). The `tenant` argument is held for
    /// signature stability against the v0.2 expansion that will read
    /// [`TableMetadata::indexes`].
    pub async fn pg_index(_catalog: &dyn Catalog, _tenant: &TenantId) -> Result<RecordBatch> {
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
    /// | oid           | BIGINT | FNV-1a of `(tenant, table, conname)`      |
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

    /// Build `pg_catalog.pg_constraint` filtered to `tenant`. Always empty
    /// in v0.1 (no FK / explicit PK / CHECK / UNIQUE surfaces). `tenant`
    /// is held for signature stability.
    pub async fn pg_constraint(
        _catalog: &dyn Catalog,
        _tenant: &TenantId,
    ) -> Result<RecordBatch> {
        let schema = Self::pg_constraint_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
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

    /// Build `information_schema.views` filtered to `tenant`'s
    /// continuous materialized views. Same `list_tables` + `load_table`
    /// pattern as [`Self::pg_class`]; we filter to tables whose
    /// `continuous_aggregate` is `Some(_)` and pull the SELECT body out
    /// of `CvDef::query_sql`.
    ///
    /// Cross-tenant leak is a P0 invariant: only [`Catalog::list_tables`]
    /// / [`Catalog::load_table`] for `tenant` are consulted.
    pub async fn views(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let names = catalog.list_tables(tenant).await?;

        let mut catalogs: Vec<&str> = Vec::new();
        let mut schemas: Vec<&str> = Vec::new();
        let mut table_names: Vec<String> = Vec::new();
        let mut definitions: Vec<String> = Vec::new();
        let mut check_options: Vec<&str> = Vec::new();
        let mut updatables: Vec<&str> = Vec::new();
        let mut insertables: Vec<&str> = Vec::new();

        for name in &names {
            let meta = catalog.load_table(tenant, name).await?;
            // Filter to matviews (continuous aggregates) — same boundary
            // `pg_class.relkind == 'm'` uses.
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

    /// Schema for `information_schema.schemata` rows.
    ///
    /// v0.1 emits exactly one row per tenant (`"public"`), matching
    /// the single-schema-per-tenant invariant the rest of the views
    /// already encode (`pg_namespace`, `pg_class.relnamespace`,
    /// `information_schema.tables.table_schema`).
    ///
    /// | column                          | type  | notes                          |
    /// |---------------------------------|-------|--------------------------------|
    /// | catalog_name                    | TEXT  | always `"basin"`               |
    /// | schema_name                     | TEXT  | always `"public"`              |
    /// | schema_owner                    | TEXT  | `""` placeholder (v0.2 wires)  |
    /// | default_character_set_catalog   | TEXT? | NULL                           |
    /// | default_character_set_schema    | TEXT? | NULL                           |
    /// | default_character_set_name      | TEXT? | NULL                           |
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

    /// Build `information_schema.schemata` filtered to `tenant`. v0.1
    /// emits exactly one row (`"public"`).
    pub async fn schemata(_catalog: &dyn Catalog, _tenant: &TenantId) -> Result<RecordBatch> {
        let schema = Self::schemata_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![BASIN_CATALOG_NAME.to_string()])),
            Arc::new(StringArray::from(vec![DEFAULT_SCHEMA.to_string()])),
            Arc::new(StringArray::from(vec![SCHEMA_OWNER_PLACEHOLDER.to_string()])),
            Arc::new(StringArray::from(vec![None::<String>])),
            Arc::new(StringArray::from(vec![None::<String>])),
            Arc::new(StringArray::from(vec![None::<String>])),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|e| BasinError::internal(format!("info_schema.schemata build: {e}")))
    }

    /// Schema for `information_schema.table_constraints` rows.
    ///
    /// One row per declared constraint visible to the calling tenant. v0.1
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
    /// | table_name        | TEXT | tenant-local table name                            |
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

    /// Build `information_schema.table_constraints` filtered to `tenant`.
    ///
    /// v0.1 only emits `NOT NULL` constraint rows: one per non-nullable
    /// column on each tenant-owned table. PK / FK / CHECK / UNIQUE are
    /// queued (the parser rejects them today; see `basin_engine::ddl`)
    /// and so contribute zero rows. The constraint name follows the
    /// `<table>_<column>_not_null` convention — pgwire-introspecting
    /// clients (PostgREST, pgAdmin) only need stable names within a
    /// tenant, not PG-byte-identical ones.
    ///
    /// Cross-tenant leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] / [`Catalog::load_table`] for `tenant`.
    pub async fn table_constraints(
        catalog: &dyn Catalog,
        tenant: &TenantId,
    ) -> Result<RecordBatch> {
        let names = catalog.list_tables(tenant).await?;

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
            let meta = catalog.load_table(tenant, name).await?;
            for field in meta.schema.fields().iter() {
                if !field.is_nullable() {
                    constraint_catalogs.push(BASIN_CATALOG_NAME);
                    constraint_schemas.push(DEFAULT_SCHEMA);
                    constraint_names.push(not_null_constraint_name(name, field.name()));
                    table_catalogs.push(BASIN_CATALOG_NAME);
                    table_schemas.push(DEFAULT_SCHEMA);
                    table_names.push(name.as_str().to_string());
                    constraint_types.push(CONSTRAINT_TYPE_NOT_NULL);
                    is_deferrables.push(CONSTRAINT_NO);
                    initially_deferreds.push(CONSTRAINT_NO);
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
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("info_schema.table_constraints build: {e}"))
        })
    }

    /// Build `information_schema.key_column_usage` filtered to `tenant`.
    ///
    /// Always empty in v0.1: PG semantics restrict this view to columns
    /// participating in PRIMARY KEY / UNIQUE / FOREIGN KEY constraints,
    /// none of which Basin tracks today (PK/UNIQUE/FK are rejected at
    /// parse time in `basin_engine::ddl`). NOT NULL constraints are
    /// surfaced in `table_constraints` only — PG does not list them
    /// here. Rows will appear once PK enforcement ships.
    ///
    /// Cross-tenant leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] for `tenant`.
    pub async fn key_column_usage(
        catalog: &dyn Catalog,
        tenant: &TenantId,
    ) -> Result<RecordBatch> {
        // Walk the tables to keep the access pattern (and therefore the
        // tenant-isolation surface) identical to the other views, even
        // though no rows are produced today.
        let _ = catalog.list_tables(tenant).await?;

        let schema = Self::key_column_usage_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!("info_schema.key_column_usage build: {e}"))
        })
    }

    /// Schema for `pg_catalog.pg_type` rows.
    ///
    /// Static catalog of the PG built-in types Basin's pgwire layer
    /// advertises. The row set is fixed (no user-defined types in v0.1 —
    /// enums/domains are tracked separately) and shared across tenants;
    /// `typnamespace` is the only per-tenant column (FNV-1a hash of
    /// `(tenant, "pg_catalog")`). pgAdmin's column-detail query joins
    /// against this view to render PG type names.
    ///
    /// | column        | type    | notes                                     |
    /// |---------------|---------|-------------------------------------------|
    /// | oid           | BIGINT  | PG type OID (16=bool, 23=int4, 25=text…)  |
    /// | typname       | TEXT    | PG type name (`bool`, `int4`, `text`, …)  |
    /// | typnamespace  | BIGINT  | hash of `(tenant, "pg_catalog")`          |
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
    /// table; `typnamespace` is hashed per-tenant against the
    /// `"pg_catalog"` schema so a JOIN against `pg_namespace` works the
    /// same way as `pg_class.relnamespace` / `pg_proc.pronamespace`.
    /// `catalog` is unused for v0.1 (no user-defined types in pg_type yet)
    /// and held for signature stability against the v0.2 expansion that
    /// will read tenant-defined enum / domain types.
    pub async fn pg_type(_catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let n = BASIN_PG_TYPES.len();
        let mut oids: Vec<i64> = Vec::with_capacity(n);
        let mut typnames: Vec<&'static str> = Vec::with_capacity(n);
        let mut typnamespaces: Vec<i64> = Vec::with_capacity(n);
        let mut typtypes: Vec<&'static str> = Vec::with_capacity(n);
        let mut typcategories: Vec<&'static str> = Vec::with_capacity(n);
        let mut typlens: Vec<i16> = Vec::with_capacity(n);
        let mut typbyvals: Vec<bool> = Vec::with_capacity(n);

        let nsp = namespace_oid_for(tenant, PG_CATALOG_SCHEMA);

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
    /// `tenant`. Always empty in v0.1 (FOREIGN KEY queued).
    ///
    /// Cross-tenant leak is a P0 invariant: this only ever calls
    /// [`Catalog::list_tables`] for `tenant`.
    pub async fn referential_constraints(
        catalog: &dyn Catalog,
        tenant: &TenantId,
    ) -> Result<RecordBatch> {
        let _ = catalog.list_tables(tenant).await?;

        let schema = Self::referential_constraints_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| {
            BasinError::internal(format!(
                "info_schema.referential_constraints build: {e}"
            ))
        })
    }

    /// Schema for `pg_catalog.pg_depend` rows.
    ///
    /// Records dependency edges between catalog objects. v0.1 surfaces:
    ///
    /// - Continuous matview → source-table edges (one row per CV when the
    ///   source table is owned by the same tenant).
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

    /// Build `pg_catalog.pg_depend` filtered to `tenant`.
    ///
    /// Cross-tenant leak is a P0 invariant: only `list_tables` /
    /// `load_table` / `list_sql_functions` for `tenant` are consulted.
    pub async fn pg_depend(catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let mut classids: Vec<i64> = Vec::new();
        let mut objids: Vec<i64> = Vec::new();
        let mut objsubids: Vec<i32> = Vec::new();
        let mut refclassids: Vec<i64> = Vec::new();
        let mut refobjids: Vec<i64> = Vec::new();
        let mut refobjsubids: Vec<i32> = Vec::new();
        let mut deptypes: Vec<&str> = Vec::new();

        let pg_class_classid = catalog_table_oid(tenant, "pg_class");
        let pg_proc_classid = catalog_table_oid(tenant, "pg_proc");
        let pg_type_classid = catalog_table_oid(tenant, "pg_type");

        // CV → source-table edges: walk the tenant's tables, surface one
        // row per matview whose `source_table` resolves to a sibling table.
        // Tables in `list_tables` are valid identifiers, so reparsing the
        // CV's `source_table` only fails for malformed metadata (would
        // never have round-tripped through DDL); we silently skip those.
        let names = catalog.list_tables(tenant).await?;
        let known: std::collections::HashSet<String> =
            names.iter().map(|n| n.as_str().to_string()).collect();
        for name in &names {
            let meta = catalog.load_table(tenant, name).await?;
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
            objids.push(table_oid(tenant, name));
            objsubids.push(0);
            refclassids.push(pg_class_classid);
            refobjids.push(table_oid(tenant, &src));
            refobjsubids.push(0);
            deptypes.push(DEPTYPE_NORMAL);
        }

        // Function → return-type / arg-type edges. Procedures don't have
        // a return type and `Table` returns aren't yet round-tripped to a
        // single OID, so we restrict to `Scalar` returns.
        let funcs = catalog.list_sql_functions(tenant).await;
        for f in &funcs {
            let fn_oid = routine_oid(tenant, &f.name);
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
    /// Basin uses tenants, not PG-style roles. v0.1 surfaces exactly one
    /// row per tenant — the calling tenant — so admin scripts that JOIN
    /// against `pg_authid` to render an "owner" column resolve. Cross-
    /// tenant role enumeration is not exposed: each tenant only ever
    /// sees its own row.
    ///
    /// | column          | type    | notes                                  |
    /// |-----------------|---------|----------------------------------------|
    /// | oid             | BIGINT  | FNV-1a of `(tenant)`                   |
    /// | rolname         | TEXT    | tenant id rendered as text             |
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

    /// Build `pg_catalog.pg_authid` filtered to `tenant`. Always exactly
    /// one row (the calling tenant); cross-tenant role enumeration is
    /// intentionally absent.
    pub async fn pg_authid(_catalog: &dyn Catalog, tenant: &TenantId) -> Result<RecordBatch> {
        let oid = role_oid_for(tenant);
        let rolname = tenant.to_string();
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
}

/// Database-level `table_catalog` value reported by
/// `information_schema.tables`. PG semantics: this is the database name
/// the connection is bound to. Basin is a single logical database in v0.1.
const BASIN_CATALOG_NAME: &str = "basin";

/// Schema name returned for every Basin table. v0.1 maps a tenant to
/// exactly one schema, named `"public"` to match the PG default and let
/// PostgREST / pgAdmin discover tables without configuration. Multi-schema
/// per tenant lands in v0.2; until then `relnamespace` and `table_schema`
/// always carry this value.
const DEFAULT_SCHEMA: &str = "public";

/// Synthetic schema name used as the namespace for `pg_catalog.pg_type`
/// rows. Every tenant's pg_type rows nominally live in the `pg_catalog`
/// namespace; the value participates only in the per-tenant FNV hash and
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

/// `information_schema.table_constraints.constraint_type` literal for a
/// NOT NULL column constraint. PG-style spelling (`"NOT NULL"`).
const CONSTRAINT_TYPE_NOT_NULL: &str = "NOT NULL";
/// `is_deferrable` / `initially_deferred` value used across the
/// constraint-introspection views. v0.1 has no deferrable constraints;
/// these columns are non-nullable in the SQL standard so `"NO"` is the
/// only valid encoding.
const CONSTRAINT_NO: &str = "NO";

/// `pg_depend.deptype` literal for a normal dependency edge. PG semantics:
/// drop the referenced object cascades to the dependent. Basin v0.1 emits
/// only normal edges (no auto / internal / pin distinctions yet).
const DEPTYPE_NORMAL: &str = "n";

/// Synthesise the `constraint_name` for a NOT NULL constraint on
/// `(table, column)`. PG invents constraint names internally too — the
/// only durable contract is that the name is stable across queries
/// against the same `(tenant, table, column)` and unique within the
/// tenant's `table_constraints` rows. The convention is documented in
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
/// in `RowDescription` so a tenant joining `pg_attribute` against the wire
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
        // Unknown / fallback → text (25). Matches the router's fallback so
        // wire-layer and introspection-layer agree on the mismapped type.
        _ => 25,
    }
}


/// Stable 64-bit oid for a `(tenant, table)` pair.
///
/// Hashing scheme: FNV-1a 64-bit over the byte sequence
/// `b"basin.pg_class:" || tenant.to_string() || ":" || table.as_str()`,
/// then masked to 63 bits to fit a positive `i64` (PG's `oid` is
/// unsigned 32-bit; we widen to `i64` because Basin's identifier space
/// is per-tenant and a 32-bit hash collides too cheaply across the full
/// fleet). Properties:
///
/// - **Stable**: the same `(tenant, table)` always hashes to the same
///   oid across process restarts and across in-memory / Postgres backends.
/// - **Per-tenant disjoint by construction**: the tenant ULID is part of
///   the input, so two tenants with identically-named tables get
///   different oids. Cross-tenant oid collisions are a per-table
///   birthday problem in 2^63 space (negligible at any plausible scale).
/// - **Same-tenant collision**: 2^63 hash space; same-tenant collisions
///   would surface as a `pg_class` row pair sharing an oid. Not a
///   correctness concern for the views (PostgREST doesn't dedupe by oid)
///   but worth flagging for the v0.2 catalog-side oid registry which
///   will replace this hash with a monotonic counter.
///
/// This is intentionally _not_ persistence-versioned: changing the input
/// format here changes every oid downstream clients have cached, so the
/// constant prefix (`b"basin.pg_class:"`) is load-bearing for stability.
fn table_oid(tenant: &TenantId, table: &TableName) -> i64 {
    let key = format!("basin.pg_class:{tenant}:{}", table.as_str());
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable oid for a tenant-scoped namespace. v0.1 has one namespace per
/// tenant (`"public"`); the function takes the schema name explicitly so
/// the v0.2 multi-schema upgrade is a non-breaking signature extension.
fn namespace_oid_for(tenant: &TenantId, schema: &str) -> i64 {
    let key = format!("basin.pg_namespace:{tenant}:{schema}");
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable 64-bit oid for a `(tenant, routine_name)` pair. Mirrors
/// [`table_oid`] but uses a distinct prefix so a function and a table
/// with the same name in the same tenant do not collide on oid.
fn routine_oid(tenant: &TenantId, name: &str) -> i64 {
    let key = format!("basin.pg_proc:{tenant}:{name}");
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable synthetic OID for one of the system catalog tables themselves
/// (`pg_class`, `pg_proc`, `pg_type`, …) within a tenant's namespace.
/// Used as `classid` / `refclassid` in `pg_catalog.pg_depend` rows.
///
/// Reuses the same FNV-1a-then-positive-i64 hash family as the rest of
/// the M-starter so the resulting OIDs are stable across process restarts
/// and disjoint between tenants. The catalog-table label (`"pg_class"`,
/// `"pg_proc"`, `"pg_type"`) participates in the hash so the three
/// labels never collide on OID for the same tenant.
fn catalog_table_oid(tenant: &TenantId, table: &str) -> i64 {
    let key = format!("basin.pg_catalog_table:{tenant}:{table}");
    fnv1a_64_to_positive_i64(key.as_bytes())
}

/// Stable role OID for `pg_authid`. v0.1 maps each tenant to exactly one
/// "role" row, so the OID is a per-tenant FNV-1a hash with a distinct
/// prefix from [`table_oid`] / [`routine_oid`] / [`namespace_oid_for`].
fn role_oid_for(tenant: &TenantId) -> i64 {
    let key = format!("basin.pg_authid:{tenant}");
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
        SqlArgType::TimestampTz => DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            Some("UTC".into()),
        ),
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
