//! `basinctl import-from-postgres` — onboarding migration from a running
//! PostgreSQL into Basin.
//!
//! Pipeline (see `translate.rs` for the pure DDL half):
//!
//! 1. Connect to the source PG, enumerate user schemas / tables / columns /
//!    constraints via `information_schema` (joined with `pg_catalog` for
//!    `format_type`, which information_schema loses: vector dims, geometry
//!    subtypes).
//! 2. Translate DDL into Basin's dialect; record every skipped object
//!    (triggers → reactors, plpgsql → WASM functions, EXCLUDE/exotic
//!    constraints → manual) loudly.
//! 3. Create enums + tables on Basin, level by level so FK parents exist
//!    first. Each `CREATE TABLE` retries down a ladder (full → no CHECKs →
//!    no CHECKs/FKs) before the table is skipped.
//! 4. Stream data per table: `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` on
//!    PG piped into `COPY t (…) FROM STDIN (FORMAT BINARY)` on Basin, with
//!    CSV as the fallback for types Basin's binary COPY rejects (decided
//!    statically per column set, plus a runtime retry if the binary COPY is
//!    refused at start). `--jobs N` tables run in parallel within each FK
//!    level. The source read runs inside one REPEATABLE READ / READ ONLY
//!    transaction per table so the row-count verification sees the same
//!    snapshot the COPY streamed.
//! 5. Verify per-table row counts (source snapshot count vs Basin
//!    `count(*)`), bump identity sequences past `MAX(col)`, and print a
//!    final imported/skipped/warnings summary. Exit non-zero if any table
//!    failed or any count mismatched.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_postgres::types::ToSql;
use tokio_postgres::SimpleQueryMessage;

use crate::translate::{
    self, CreateAttempt, EnumType, IdentityKind, Plan, Report, SourceCheck, SourceColumn, SourceFk,
    SourceTable, SourceUnique, TablePlan, Transfer,
};
use crate::{connect, ImportFormat};

/// CLI-level arguments, resolved by `main.rs`.
pub struct ImportArgs {
    pub source: String,
    pub target: String,
    /// Schema filter; empty = every non-system schema.
    pub schemas: Vec<String>,
    /// Table filter (bare or `schema.table`); empty = all.
    pub tables: Vec<String>,
    pub jobs: usize,
    pub dry_run: bool,
    pub skip_data: bool,
    pub format: ImportFormat,
}

pub async fn run(args: ImportArgs) -> Result<()> {
    let t0 = Instant::now();
    let mut report = Report::default();

    // ── 1. Introspect the source ───────────────────────────────────────
    let src = connect(&args.source)
        .await
        .context("connect to source Postgres (--source)")?;
    let schemas = pick_schemas(&src, &args.schemas).await?;
    if schemas.is_empty() {
        return Err(anyhow!("no matching schemas on the source"));
    }
    println!("source schemas: {}", schemas.join(", "));

    let mut tables = introspect_tables(&src, &schemas).await?;
    if !args.tables.is_empty() {
        tables.retain(|t| {
            args.tables
                .iter()
                .any(|f| f == &t.name || f == &t.qualified())
        });
    }
    if tables.is_empty() {
        return Err(anyhow!("no matching tables on the source"));
    }
    let enums = introspect_enums(&src, &schemas).await?;
    report_unsupported_objects(&src, &schemas, &mut report).await?;

    // ── 2. Translate ────────────────────────────────────────────────────
    let plan = translate_catalog_with_format(&tables, &enums, args.format, &mut report);
    println!(
        "plan: {} table(s) across {} FK level(s), {} enum type(s)",
        plan.tables.len(),
        plan.levels.len(),
        plan.enum_ddl.len()
    );

    if args.dry_run {
        for ddl in &plan.enum_ddl {
            println!("{ddl};");
        }
        for lvl in &plan.levels {
            for &i in lvl {
                let p = &plan.tables[i];
                println!(
                    "-- {} -> {} ({})",
                    p.source_qualified(),
                    p.basin_name,
                    p.transfer.as_str()
                );
                println!("{};", p.create_sql(CreateAttempt::Full));
            }
        }
        print_summary(&report, &[], t0);
        return Ok(());
    }

    // ── 3. Create types + tables on Basin ──────────────────────────────
    let dst = connect(&args.target)
        .await
        .context("connect to target Basin (--target)")?;
    for ddl in &plan.enum_ddl {
        dst.simple_query(ddl)
            .await
            .with_context(|| format!("create enum type: {ddl}"))?;
    }
    let mut created: Vec<bool> = vec![false; plan.tables.len()];
    for lvl in plan.levels.clone() {
        for i in lvl {
            let p = &plan.tables[i];
            match create_table_ladder(&dst, p, &mut report).await {
                Ok(()) => created[i] = true,
                Err(e) => {
                    report.skip(
                        "table",
                        p.source_qualified(),
                        format!("CREATE TABLE failed on Basin: {e:#}"),
                        None,
                    );
                }
            }
        }
    }

    // ── 4 + 5. Stream data, verify counts ──────────────────────────────
    let mut results: Vec<TableResult> = Vec::new();
    if args.skip_data {
        println!("--skip-data: DDL only, no rows copied");
    } else {
        let total: usize = created.iter().filter(|&&c| c).count();
        let mut done = 0usize;
        let source_url = Arc::new(args.source.clone());
        let target_url = Arc::new(args.target.clone());
        for lvl in &plan.levels {
            // Parallelism is confined to one FK level: parents always
            // finish loading before any child starts, so Basin's FK
            // enforcement on ingest never sees a missing parent row.
            let level_plans: Vec<Arc<TablePlan>> = lvl
                .iter()
                .filter(|&&i| created[i])
                .map(|&i| Arc::new(plan.tables[i].clone()))
                .collect();
            let mut stream = futures::stream::iter(level_plans.into_iter().map(|p| {
                let s = Arc::clone(&source_url);
                let t = Arc::clone(&target_url);
                async move { copy_table(&s, &t, &p).await }
            }))
            .buffer_unordered(args.jobs.max(1));
            while let Some(r) = stream.next().await {
                done += 1;
                match &r {
                    Ok(res) => println!(
                        "[{done}/{total}] {} -> {}: {} rows, {} in {:.1}s ({}){}",
                        res.source,
                        res.basin_name,
                        res.rows,
                        human_bytes(res.bytes),
                        res.secs,
                        res.transfer.as_str(),
                        if res.count_match {
                            ", counts match"
                        } else {
                            " -- COUNT MISMATCH"
                        }
                    ),
                    Err(e) => println!("[{done}/{total}] FAILED: {e:#}"),
                }
                results.push(match r {
                    Ok(res) => res,
                    Err(e) => TableResult::failed(format!("{e:#}")),
                });
            }
        }
    }

    // Drop the warning/skip noise into one place at the end.
    print_summary(&report, &results, t0);

    let failed = results.iter().filter(|r| r.error.is_some()).count();
    let mismatched = results
        .iter()
        .filter(|r| r.error.is_none() && !r.count_match)
        .count();
    if failed > 0 || mismatched > 0 {
        return Err(anyhow!(
            "import finished with {failed} failed table(s) and {mismatched} row-count mismatch(es)"
        ));
    }
    Ok(())
}

/// Apply the `--format` override on top of the per-table static decision.
fn translate_catalog_with_format(
    tables: &[SourceTable],
    enums: &[EnumType],
    format: ImportFormat,
    report: &mut Report,
) -> Plan {
    let mut plan = translate::translate_catalog(tables, enums, report);
    match format {
        ImportFormat::Auto => {}
        ImportFormat::Csv => {
            for t in &mut plan.tables {
                t.transfer = Transfer::Csv;
            }
        }
        ImportFormat::Binary => {
            for t in &mut plan.tables {
                if t.transfer == Transfer::Csv {
                    report.warn(format!(
                        "{}: --format binary forced, but the column set is not binary-safe; expect a 0A000 rejection",
                        t.source_qualified()
                    ));
                }
                t.transfer = Transfer::Binary;
            }
        }
    }
    plan
}

// ---------------------------------------------------------------------------
// Source introspection (information_schema + pg_catalog joins)
// ---------------------------------------------------------------------------

/// All non-system schemas, optionally filtered by `--schema`.
async fn pick_schemas(src: &tokio_postgres::Client, filter: &[String]) -> Result<Vec<String>> {
    let rows = src
        .query(
            "SELECT schema_name::text FROM information_schema.schemata \
             WHERE schema_name NOT IN ('pg_catalog', 'information_schema') \
               AND schema_name NOT LIKE 'pg\\_%' \
             ORDER BY schema_name",
            &[],
        )
        .await
        .context("enumerate schemas (information_schema.schemata)")?;
    let mut schemas: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    if !filter.is_empty() {
        schemas.retain(|s| filter.iter().any(|f| f == s));
        for f in filter {
            if !schemas.contains(f) {
                return Err(anyhow!("--schema {f:?} not found on the source"));
            }
        }
    }
    Ok(schemas)
}

async fn introspect_tables(
    src: &tokio_postgres::Client,
    schemas: &[String],
) -> Result<Vec<SourceTable>> {
    let params: [&(dyn ToSql + Sync); 1] = [&schemas];

    let table_rows = src
        .query(
            "SELECT table_schema::text, table_name::text \
             FROM information_schema.tables \
             WHERE table_type = 'BASE TABLE' AND table_schema::text = ANY($1) \
             ORDER BY table_schema, table_name",
            &params,
        )
        .await
        .context("enumerate tables (information_schema.tables)")?;
    let mut tables: Vec<SourceTable> = table_rows
        .iter()
        .map(|r| SourceTable {
            schema: r.get(0),
            name: r.get(1),
            columns: Vec::new(),
            primary_key: Vec::new(),
            uniques: Vec::new(),
            checks: Vec::new(),
            fks: Vec::new(),
        })
        .collect();
    let idx_of = |schema: &str, name: &str, tables: &[SourceTable]| {
        tables
            .iter()
            .position(|t| t.schema == schema && t.name == name)
    };

    // Columns. The pg_catalog join recovers format_type (vector dims,
    // geometry subtype) and typtype (enum / composite / domain / range);
    // every projected column is cast to a plain text/int4/bool so the
    // information_schema domain types (cardinal_number, yes_or_no, …)
    // never reach the client-side decoder.
    let col_rows = src
        .query(
            "SELECT c.table_schema::text, c.table_name::text, c.column_name::text, \
                    c.data_type::text, c.udt_name::text, \
                    pg_catalog.format_type(a.atttypid, a.atttypmod)::text, \
                    ty.typtype::text, \
                    CASE WHEN ty.typelem <> 0 AND ty.typlen = -1 \
                         THEN ltrim(et.typname, '_') END::text, \
                    et.typtype::text, \
                    c.character_maximum_length::int4, \
                    c.numeric_precision::int4, c.numeric_scale::int4, \
                    (c.is_nullable = 'NO')::bool, \
                    c.column_default::text, \
                    c.is_identity::text, c.identity_generation::text, \
                    (c.is_generated = 'ALWAYS')::bool, c.generation_expression::text, \
                    c.domain_name::text \
             FROM information_schema.columns c \
             JOIN pg_catalog.pg_namespace pn ON pn.nspname = c.table_schema \
             JOIN pg_catalog.pg_class pc \
               ON pc.relnamespace = pn.oid AND pc.relname = c.table_name \
             JOIN pg_catalog.pg_attribute a \
               ON a.attrelid = pc.oid AND a.attname = c.column_name \
             JOIN pg_catalog.pg_type ty ON ty.oid = a.atttypid \
             LEFT JOIN pg_catalog.pg_type et ON et.oid = ty.typelem \
             WHERE c.table_schema::text = ANY($1) \
             ORDER BY c.table_schema, c.table_name, c.ordinal_position",
            &params,
        )
        .await
        .context("enumerate columns (information_schema.columns)")?;
    for r in &col_rows {
        let schema: String = r.get(0);
        let name: String = r.get(1);
        let Some(i) = idx_of(&schema, &name, &tables) else {
            continue; // column of a view / non-BASE TABLE
        };
        let is_identity: String = r.get(14);
        let identity_generation: Option<String> = r.get(15);
        let identity = if is_identity == "YES" {
            match identity_generation.as_deref() {
                Some("ALWAYS") => Some(IdentityKind::Always),
                _ => Some(IdentityKind::ByDefault),
            }
        } else {
            None
        };
        let is_generated: bool = r.get(16);
        tables[i].columns.push(SourceColumn {
            name: r.get(2),
            data_type: r.get(3),
            udt_name: r.get(4),
            formatted_type: r.get(5),
            typtype: r.get(6),
            elem_udt: r.get(7),
            elem_typtype: r.get(8),
            max_len: r.get(9),
            num_precision: r.get(10),
            num_scale: r.get(11),
            not_null: r.get(12),
            default: r.get(13),
            identity,
            generated_expr: if is_generated { r.get(17) } else { None },
            domain_name: r.get(18),
        });
    }

    // PRIMARY KEY / UNIQUE.
    let key_rows = src
        .query(
            "SELECT tc.table_schema::text, tc.table_name::text, \
                    tc.constraint_name::text, tc.constraint_type::text, \
                    kcu.column_name::text \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.key_column_usage kcu \
               ON kcu.constraint_schema = tc.constraint_schema \
              AND kcu.constraint_name = tc.constraint_name \
             WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') \
               AND tc.table_schema::text = ANY($1) \
             ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, \
                      kcu.ordinal_position",
            &params,
        )
        .await
        .context("enumerate key constraints")?;
    for r in &key_rows {
        let schema: String = r.get(0);
        let name: String = r.get(1);
        let Some(i) = idx_of(&schema, &name, &tables) else {
            continue;
        };
        let cname: String = r.get(2);
        let ctype: String = r.get(3);
        let col: String = r.get(4);
        if ctype == "PRIMARY KEY" {
            tables[i].primary_key.push(col);
        } else if let Some(u) = tables[i].uniques.iter_mut().find(|u| u.name == cname) {
            u.columns.push(col);
        } else {
            tables[i].uniques.push(SourceUnique {
                name: cname,
                columns: vec![col],
            });
        }
    }

    // CHECK constraints — minus the `… IS NOT NULL` rows PG synthesises
    // for every NOT NULL column.
    let check_rows = src
        .query(
            "SELECT tc.table_schema::text, tc.table_name::text, \
                    tc.constraint_name::text, cc.check_clause::text \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.check_constraints cc \
               ON cc.constraint_schema = tc.constraint_schema \
              AND cc.constraint_name = tc.constraint_name \
             WHERE tc.constraint_type = 'CHECK' \
               AND tc.table_schema::text = ANY($1) \
               AND cc.check_clause::text NOT ILIKE '% IS NOT NULL'",
            &params,
        )
        .await
        .context("enumerate CHECK constraints")?;
    for r in &check_rows {
        let schema: String = r.get(0);
        let name: String = r.get(1);
        if let Some(i) = idx_of(&schema, &name, &tables) {
            tables[i].checks.push(SourceCheck {
                name: r.get(2),
                clause: r.get(3),
            });
        }
    }

    // FOREIGN KEYs, with referenced columns matched by position through
    // the parent's unique constraint.
    let fk_rows = src
        .query(
            "SELECT tc.table_schema::text, tc.table_name::text, \
                    tc.constraint_name::text, kcu.column_name::text, \
                    rk.table_schema::text, rk.table_name::text, rk.column_name::text, \
                    rc.delete_rule::text, rc.update_rule::text \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.referential_constraints rc \
               ON rc.constraint_schema = tc.constraint_schema \
              AND rc.constraint_name = tc.constraint_name \
             JOIN information_schema.key_column_usage kcu \
               ON kcu.constraint_schema = tc.constraint_schema \
              AND kcu.constraint_name = tc.constraint_name \
             JOIN information_schema.key_column_usage rk \
               ON rk.constraint_schema = rc.unique_constraint_schema \
              AND rk.constraint_name = rc.unique_constraint_name \
              AND rk.ordinal_position = kcu.position_in_unique_constraint \
             WHERE tc.constraint_type = 'FOREIGN KEY' \
               AND tc.table_schema::text = ANY($1) \
             ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, \
                      kcu.ordinal_position",
            &params,
        )
        .await
        .context("enumerate FOREIGN KEY constraints")?;
    for r in &fk_rows {
        let schema: String = r.get(0);
        let name: String = r.get(1);
        let Some(i) = idx_of(&schema, &name, &tables) else {
            continue;
        };
        let cname: String = r.get(2);
        let col: String = r.get(3);
        let ref_col: String = r.get(6);
        if let Some(fk) = tables[i].fks.iter_mut().find(|f| f.name == cname) {
            fk.columns.push(col);
            fk.ref_columns.push(ref_col);
        } else {
            tables[i].fks.push(SourceFk {
                name: cname,
                columns: vec![col],
                ref_schema: r.get(4),
                ref_table: r.get(5),
                ref_columns: vec![ref_col],
                on_delete: r.get(7),
                on_update: r.get(8),
            });
        }
    }

    Ok(tables)
}

async fn introspect_enums(
    src: &tokio_postgres::Client,
    schemas: &[String],
) -> Result<Vec<EnumType>> {
    let params: [&(dyn ToSql + Sync); 1] = [&schemas];
    let rows = src
        .query(
            "SELECT n.nspname::text, t.typname::text, e.enumlabel::text \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname::text = ANY($1) \
             ORDER BY n.nspname, t.typname, e.enumsortorder",
            &params,
        )
        .await
        .context("enumerate enum types (pg_enum)")?;
    let mut enums: Vec<EnumType> = Vec::new();
    for r in &rows {
        let schema: String = r.get(0);
        let name: String = r.get(1);
        let label: String = r.get(2);
        match enums
            .iter_mut()
            .find(|e| e.schema == schema && e.name == name)
        {
            Some(e) => e.labels.push(label),
            None => enums.push(EnumType {
                schema,
                name,
                labels: vec![label],
            }),
        }
    }
    Ok(enums)
}

/// Enumerate the source features Basin cannot carry over and report each
/// one loudly with its Basin-shaped migration path.
async fn report_unsupported_objects(
    src: &tokio_postgres::Client,
    schemas: &[String],
    report: &mut Report,
) -> Result<()> {
    let params: [&(dyn ToSql + Sync); 1] = [&schemas];

    // Triggers → Basin reactors.
    let rows = src
        .query(
            "SELECT DISTINCT trigger_schema::text, event_object_table::text, \
                    trigger_name::text \
             FROM information_schema.triggers \
             WHERE trigger_schema::text = ANY($1) \
             ORDER BY 1, 2, 3",
            &params,
        )
        .await
        .context("enumerate triggers")?;
    for r in &rows {
        let (s, t, n): (String, String, String) = (r.get(0), r.get(1), r.get(2));
        report.skip(
            "trigger",
            format!("{s}.{t}.{n}"),
            "triggers are not imported",
            Some("rewrite as a Basin reactor (CREATE REACTOR … ON INSERT/UPDATE/DELETE)"),
        );
    }

    // plpgsql / SQL functions → WASM functions.
    let rows = src
        .query(
            "SELECT r.routine_schema::text, r.routine_name::text, \
                    coalesce(r.external_language, 'SQL')::text \
             FROM information_schema.routines r \
             WHERE r.routine_schema::text = ANY($1) \
             ORDER BY 1, 2",
            &params,
        )
        .await
        .context("enumerate routines")?;
    for r in &rows {
        let (s, n, lang): (String, String, String) = (r.get(0), r.get(1), r.get(2));
        report.skip(
            "function",
            format!("{s}.{n}()"),
            format!("{} functions are not imported", lang.to_lowercase()),
            Some("port to a Basin WASM function — see `basinctl fn` for the toolchain roadmap"),
        );
    }

    // Views / materialized views — the SQL body is not translated.
    let rows = src
        .query(
            "SELECT table_schema::text, table_name::text \
             FROM information_schema.tables \
             WHERE table_type = 'VIEW' AND table_schema::text = ANY($1) \
             ORDER BY 1, 2",
            &params,
        )
        .await
        .context("enumerate views")?;
    for r in &rows {
        let (s, n): (String, String) = (r.get(0), r.get(1));
        report.skip(
            "view",
            format!("{s}.{n}"),
            "view bodies are not imported",
            Some("re-create with CREATE VIEW on Basin after checking the SQL against docs/sql-support.md"),
        );
    }
    let rows = src
        .query(
            "SELECT schemaname::text, matviewname::text FROM pg_catalog.pg_matviews \
             WHERE schemaname::text = ANY($1) ORDER BY 1, 2",
            &params,
        )
        .await
        .context("enumerate materialized views")?;
    for r in &rows {
        let (s, n): (String, String) = (r.get(0), r.get(1));
        report.skip(
            "matview",
            format!("{s}.{n}"),
            "materialized views are not imported",
            Some("re-create as a table + scheduled refresh, or a Basin continuous pre-aggregation"),
        );
    }

    // EXCLUDE constraints — no Basin equivalent.
    let rows = src
        .query(
            "SELECT n.nspname::text, cl.relname::text, con.conname::text \
             FROM pg_catalog.pg_constraint con \
             JOIN pg_catalog.pg_class cl ON cl.oid = con.conrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace \
             WHERE con.contype = 'x' AND n.nspname::text = ANY($1) \
             ORDER BY 1, 2, 3",
            &params,
        )
        .await
        .context("enumerate EXCLUDE constraints")?;
    for r in &rows {
        let (s, t, n): (String, String, String) = (r.get(0), r.get(1), r.get(2));
        report.skip(
            "constraint",
            format!("{s}.{t}.{n}"),
            "EXCLUDE constraints are not supported",
            Some("enforce in application code or a reactor"),
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// DDL on Basin (retry ladder)
// ---------------------------------------------------------------------------

/// Try `CREATE TABLE` full-fat, then progressively drop the optional
/// constraint tiers (CHECKs first — they carry verbatim source SQL — then
/// FKs). Whatever got dropped is recorded loudly.
async fn create_table_ladder(
    dst: &tokio_postgres::Client,
    p: &TablePlan,
    report: &mut Report,
) -> Result<()> {
    let ladder = [
        CreateAttempt::Full,
        CreateAttempt::NoChecks,
        CreateAttempt::NoChecksNoFks,
    ];
    let mut last_err: Option<tokio_postgres::Error> = None;
    for attempt in ladder {
        let sql = p.create_sql(attempt);
        // Pointless to retry identical SQL when there is nothing to drop.
        if attempt != CreateAttempt::Full && Some(&sql) == last_sql(p, attempt).as_ref() {
            continue;
        }
        match dst.simple_query(&sql).await {
            Ok(_) => {
                for dropped in p.dropped_constraints(attempt) {
                    report.skip(
                        "constraint",
                        format!("{}.{}", p.source_qualified(), dropped),
                        format!(
                            "dropped so CREATE TABLE succeeds on Basin (engine said: {})",
                            last_err.as_ref().map(|e| e.to_string()).unwrap_or_default()
                        ),
                        None,
                    );
                }
                return Ok(());
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(anyhow!(
        "{}",
        last_err.map(|e| e.to_string()).unwrap_or_default()
    ))
}

/// The SQL of the previous (less-stripped) rung, used to skip no-op retries.
fn last_sql(p: &TablePlan, attempt: CreateAttempt) -> Option<String> {
    match attempt {
        CreateAttempt::Full => None,
        CreateAttempt::NoChecks => Some(p.create_sql(CreateAttempt::Full)),
        CreateAttempt::NoChecksNoFks => Some(p.create_sql(CreateAttempt::NoChecks)),
    }
}

// ---------------------------------------------------------------------------
// Data streaming
// ---------------------------------------------------------------------------

pub struct TableResult {
    pub source: String,
    pub basin_name: String,
    pub rows: u64,
    pub bytes: u64,
    pub secs: f64,
    pub transfer: Transfer,
    pub count_match: bool,
    pub error: Option<String>,
}

impl TableResult {
    fn failed(error: String) -> Self {
        TableResult {
            source: String::new(),
            basin_name: String::new(),
            rows: 0,
            bytes: 0,
            secs: 0.0,
            transfer: Transfer::Csv,
            count_match: false,
            error: Some(error),
        }
    }
}

/// Copy one table. Opens fresh connections on both ends so `--jobs`
/// parallelism never shares a session, and reads the source inside one
/// REPEATABLE READ / READ ONLY transaction so the verification count and
/// the streamed rows come from the same snapshot.
async fn copy_table(source_url: &str, target_url: &str, p: &TablePlan) -> Result<TableResult> {
    let table = p.source_qualified();
    let src = connect(source_url)
        .await
        .with_context(|| format!("{table}: connect source"))?;
    let dst = connect(target_url)
        .await
        .with_context(|| format!("{table}: connect target"))?;

    src.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .with_context(|| format!("{table}: begin snapshot"))?;
    let count_sql = format!(
        "SELECT count(*) FROM {}.{}",
        translate::quote_ident(&p.source_schema),
        translate::quote_ident(&p.source_name)
    );
    let src_count: i64 = src
        .query_one(&count_sql, &[])
        .await
        .with_context(|| format!("{table}: source count"))?
        .get(0);

    let t0 = Instant::now();
    let (rows, bytes, transfer) = match stream_rows(&src, &dst, p, p.transfer).await {
        Ok((rows, bytes)) => (rows, bytes, p.transfer),
        // Runtime fallback: a binary COPY refused at start (e.g. Basin's
        // 0A000 "no binary codec") retries once as CSV. Mid-stream
        // failures do NOT fall back — rows may have landed.
        Err(StreamError::Start(e)) if p.transfer == Transfer::Binary => {
            eprintln!("WARN {table}: binary COPY refused ({e:#}); retrying as CSV");
            let (rows, bytes) = stream_rows(&src, &dst, p, Transfer::Csv)
                .await
                .map_err(|e| e.into_anyhow(&table))?;
            (rows, bytes, Transfer::Csv)
        }
        Err(e) => return Err(e.into_anyhow(&table)),
    };
    src.batch_execute("COMMIT")
        .await
        .with_context(|| format!("{table}: commit snapshot"))?;

    // Row-count verification: Basin count(*) vs the source snapshot count.
    let dst_count = scalar_i64(
        &dst,
        &format!(
            "SELECT count(*) FROM {}",
            translate::quote_ident(&p.basin_name)
        ),
    )
    .await
    .with_context(|| format!("{table}: target count"))?;
    let count_match = dst_count == src_count && rows == src_count as u64;
    if !count_match {
        eprintln!(
            "WARN {table}: row counts differ — source snapshot {src_count}, COPY reported {rows}, Basin count(*) {dst_count}"
        );
    }

    // Bump identity sequences past MAX(col) so post-import inserts don't
    // collide with imported ids.
    for c in p.identity_columns() {
        let max = scalar_i64(
            &dst,
            &format!(
                "SELECT coalesce(max({}), 0) FROM {}",
                translate::quote_ident(&c.name),
                translate::quote_ident(&p.basin_name)
            ),
        )
        .await
        .with_context(|| format!("{table}: max({})", c.name))?;
        if max > 0 {
            let seq = format!("{}_{}_seq", p.basin_name, c.name);
            dst.simple_query(&format!(
                "SELECT setval({}, {max})",
                translate::quote_literal(&seq)
            ))
            .await
            .with_context(|| format!("{table}: setval {seq}"))?;
        }
    }

    Ok(TableResult {
        source: table,
        basin_name: p.basin_name.clone(),
        rows,
        bytes,
        secs: t0.elapsed().as_secs_f64(),
        transfer,
        count_match,
        error: None,
    })
}

/// Distinguishes "the COPY never started" (safe to fall back to CSV) from
/// "it failed mid-stream" (rows may already be on Basin; do not retry).
enum StreamError {
    Start(anyhow::Error),
    Mid(anyhow::Error),
}

impl StreamError {
    fn into_anyhow(self, table: &str) -> anyhow::Error {
        match self {
            StreamError::Start(e) => e.context(format!("{table}: COPY start")),
            StreamError::Mid(e) => e.context(format!("{table}: COPY stream")),
        }
    }
}

/// Pump `COPY … TO STDOUT` on the source into `COPY … FROM STDIN` on Basin.
/// Returns (rows reported by Basin, bytes transferred).
async fn stream_rows(
    src: &tokio_postgres::Client,
    dst: &tokio_postgres::Client,
    p: &TablePlan,
    transfer: Transfer,
) -> std::result::Result<(u64, u64), StreamError> {
    let out_sql = p.copy_out_sql(transfer);
    let in_sql = p.copy_in_sql(transfer);
    let stream = src
        .copy_out(&out_sql)
        .await
        .map_err(|e| StreamError::Start(anyhow!("{e} (source: {out_sql})")))?;
    let sink = dst
        .copy_in::<_, Bytes>(&in_sql)
        .await
        .map_err(|e| StreamError::Start(anyhow!("{e} (target: {in_sql})")))?;
    futures::pin_mut!(stream, sink);

    let mut bytes = 0u64;
    let mut reported = 0u64;
    while let Some(chunk) = stream.next().await {
        let b = chunk.map_err(|e| StreamError::Mid(anyhow!("source read: {e}")))?;
        bytes += b.len() as u64;
        sink.send(b)
            .await
            .map_err(|e| StreamError::Mid(anyhow!("target write: {e}")))?;
        // Coarse progress for very large tables, on stderr so it never
        // interleaves with the per-table result lines on stdout.
        if bytes / (256 * 1024 * 1024) > reported {
            reported = bytes / (256 * 1024 * 1024);
            eprintln!(
                "  … {}: {} transferred",
                p.source_qualified(),
                human_bytes(bytes)
            );
        }
    }
    let rows = sink
        .as_mut()
        .finish()
        .await
        .map_err(|e| StreamError::Mid(anyhow!("target finish: {e}")))?;
    Ok((rows, bytes))
}

/// Run a single-cell query through the simple-query path (Basin renders
/// everything as text there) and parse it as i64.
async fn scalar_i64(client: &tokio_postgres::Client, sql: &str) -> Result<i64> {
    let msgs = client
        .simple_query(sql)
        .await
        .with_context(|| format!("query: {sql}"))?;
    for m in msgs {
        if let SimpleQueryMessage::Row(r) = m {
            let cell = r.get(0).ok_or_else(|| anyhow!("NULL cell from {sql}"))?;
            return cell
                .parse::<i64>()
                .with_context(|| format!("parse {cell:?} from {sql}"));
        }
    }
    Err(anyhow!("no rows from {sql}"))
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = b as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    format!("{v:.1} {}", UNITS[u])
}

fn print_summary(report: &Report, results: &[TableResult], t0: Instant) {
    let ok: Vec<&TableResult> = results
        .iter()
        .filter(|r| r.error.is_none() && r.count_match)
        .collect();
    let mismatched = results
        .iter()
        .filter(|r| r.error.is_none() && !r.count_match)
        .count();
    let failed = results.iter().filter(|r| r.error.is_some()).count();
    let rows: u64 = ok.iter().map(|r| r.rows).sum();

    println!("\n== import summary ==");
    println!(
        "imported : {} table(s), {} row(s), in {:.1}s",
        ok.len(),
        rows,
        t0.elapsed().as_secs_f64()
    );
    if mismatched > 0 {
        println!("mismatch : {mismatched} table(s) with row-count differences");
    }
    if failed > 0 {
        println!("failed   : {failed} table(s)");
    }
    println!("skipped  : {} object(s)", report.skips.len());
    println!("warnings : {}", report.warnings.len());
    if !report.skips.is_empty() {
        println!("\n-- skipped objects --");
        for s in &report.skips {
            println!("{}", s.render());
        }
    }
    if !report.warnings.is_empty() {
        println!("\n-- warnings --");
        for w in &report.warnings {
            println!("WARN {w}");
        }
    }
}
